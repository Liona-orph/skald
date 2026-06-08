package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/internal/persistence/memory"
	"github.com/skald-io/skald/internal/persistence/persistencetest"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

func TestConformance(t *testing.T) {
	persistencetest.RunSuite(t, func(t *testing.T) persistence.Store {
		s := memory.New()
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// TestNoAliasing is the driver's reason to exist. Everything else the memory
// store does, SQLite also does; what makes it a usable stand-in is that a
// caller cannot reach stored state through a value the store handed back. A
// driver that leaks an alias turns "my code mutated a shared map" into a bug
// that only reproduces in production.
func TestNoAliasing(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	// Each subtest gets its own run: they mutate the very values they handed
	// the store, so sharing one execution would make them interfere.
	seed := func(t *testing.T, id string, memo map[string]string) *persistencetest.Builder {
		t.Helper()
		b := persistencetest.NewBuilder("default", id, id+"-run", persistencetest.WithMemo(memo))
		b.WorkflowTask()
		_, err := store.CreateExecution(ctx, b.Create())
		require.NoError(t, err)
		return b
	}

	t.Run("the request cannot be mutated after the write", func(t *testing.T) {
		memo := map[string]string{"team": "payments"}
		seed(t, "req", memo)

		memo["team"] = "fraud"
		memo["leaked"] = "yes"

		got, err := store.GetExecution(ctx, "default", "req", "req-run")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"team": "payments"}, got.Memo)

		h, err := store.ReadHistory(ctx, "default", "req", "req-run", 1, 1)
		require.NoError(t, err)
		attrs := history.MustAttributes[history.WorkflowExecutionStartedAttributes](h[0])
		require.Equal(t, map[string]string{"team": "payments"}, attrs.Memo)
	})

	t.Run("a returned record cannot be mutated", func(t *testing.T) {
		seed(t, "rec", map[string]string{"team": "payments"})

		first, err := store.GetExecution(ctx, "default", "rec", "rec-run")
		require.NoError(t, err)
		first.Memo["team"] = "clobbered"
		first.Status = skald.StatusTerminated

		second, err := store.GetExecution(ctx, "default", "rec", "rec-run")
		require.NoError(t, err)
		require.Equal(t, "payments", second.Memo["team"])
		require.Equal(t, skald.StatusRunning, second.Status)
	})

	t.Run("returned events cannot be mutated", func(t *testing.T) {
		b := seed(t, "read", map[string]string{"team": "payments"})

		first, err := store.ReadHistory(ctx, "default", "read", "read-run", 1, 0)
		require.NoError(t, err)

		attrs, ok := history.AttributesAs[history.WorkflowExecutionStartedAttributes](first[0])
		require.True(t, ok)
		require.NotNil(t, attrs.Input)

		// Reach through every indirection an attribute set offers: the payload
		// pointer, its byte slice, and the maps hanging off it.
		attrs.Input.Data[0] = 'X'
		attrs.Memo["team"] = "clobbered"
		first[0].ID = 99

		second, err := store.ReadHistory(ctx, "default", "read", "read-run", 1, 0)
		require.NoError(t, err)
		require.Equal(t, b.History(), second)
		require.NoError(t, second.Validate())
	})

	t.Run("the caller's event slice cannot be mutated after the write", func(t *testing.T) {
		b := seed(t, "write", nil)

		b.Signal("approve")
		events := b.Pending()
		_, err := store.AppendHistory(ctx, persistence.AppendHistoryRequest{
			Namespace: "default", WorkflowID: "write", RunID: "write-run",
			ExpectedVersion: 1,
			Events:          events,
			Record:          b.Record(),
		})
		require.NoError(t, err)

		// The builder shares the payload pointer with the request, so this
		// mutation reaches everything the caller still holds. Only the store
		// must be untouched.
		signalled := history.MustAttributes[history.WorkflowExecutionSignaledAttributes](events[0])
		signalled.Input.Data[0] = 'X'
		events[0].ID = 4242

		got, err := store.ReadHistory(ctx, "default", "write", "write-run", 1, 0)
		require.NoError(t, err)
		require.NoError(t, got.Validate())
		last := history.MustAttributes[history.WorkflowExecutionSignaledAttributes](got[len(got)-1])
		require.Equal(t, []byte(`"sig"`), last.Input.Data)
		require.Equal(t, int64(len(got)), got.LastEventID())
	})

	t.Run("returned timers cannot be mutated", func(t *testing.T) {
		b := seed(t, "timer", nil)

		id := b.Timer(time.Minute)
		want := b.TimerFor(id, time.Date(2024, 3, 1, 13, 0, 0, 0, time.UTC))
		_, err := store.AppendHistory(ctx, b.Append(1, func(r *persistence.AppendHistoryRequest) {
			r.UpsertTimers = []persistence.TimerRecord{want}
		}))
		require.NoError(t, err)

		horizon := time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC)
		due, err := store.DueTimers(ctx, horizon, 10)
		require.NoError(t, err)
		require.Len(t, due, 1)
		due[0].TaskQueue = "clobbered"
		due[0].FireAt = time.Time{}

		again, err := store.DueTimers(ctx, horizon, 10)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{want}, again)
	})
}

// TestTimerIndexOrdering exercises the heap beyond what the conformance suite
// needs: enough entries that a bug in Fix or Remove would show up as a
// mis-ordered or resurrected timer rather than as luck.
func TestTimerIndexOrdering(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	base := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	const n = 64

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	b.WorkflowTask()
	timers := make([]persistence.TimerRecord, 0, n)
	for i := 0; i < n; i++ {
		// Insert out of order: 0, 63, 1, 62, ... so that the heap actually has
		// to sift rather than accidentally being sorted on arrival.
		offset := i / 2
		if i%2 == 1 {
			offset = n - 1 - i/2
		}
		id := b.Timer(time.Duration(offset) * time.Minute)
		timers = append(timers, b.TimerFor(id, base.Add(time.Duration(offset)*time.Minute)))
	}
	_, err := store.CreateExecution(ctx, b.Create(func(r *persistence.CreateExecutionRequest) {
		r.Timers = timers
	}))
	require.NoError(t, err)

	due, err := store.DueTimers(ctx, base.Add(n*time.Minute), n)
	require.NoError(t, err)
	require.Len(t, due, n)
	for i := 1; i < len(due); i++ {
		require.False(t, due[i].FireAt.Before(due[i-1].FireAt), "due timers must ascend")
	}

	// A limit must take a prefix of that order, not an arbitrary subset.
	head, err := store.DueTimers(ctx, base.Add(n*time.Minute), 5)
	require.NoError(t, err)
	require.Equal(t, due[:5], head)

	// The horizon must cut the tail off, not the head.
	window, err := store.DueTimers(ctx, base.Add(10*time.Minute), n)
	require.NoError(t, err)
	require.Len(t, window, 11)
	require.Equal(t, due[:11], window)

	// Deleting every other timer must not disturb the order of the rest.
	var deleted []persistence.TimerKey
	var kept []persistence.TimerRecord
	for i, tr := range due {
		if i%2 == 0 {
			deleted = append(deleted, tr.TimerKey)
		} else {
			kept = append(kept, tr)
		}
	}
	require.NoError(t, store.DeleteTimers(ctx, deleted))
	after, err := store.DueTimers(ctx, base.Add(n*time.Minute), n)
	require.NoError(t, err)
	require.Equal(t, kept, after)
	require.Equal(t, len(kept), store.Stats().Timers)
}

func TestFaultInjectionIsDeterministic(t *testing.T) {
	cfg := memory.FaultConfig{Seed: 7, ConflictRate: 0.3, ErrorRate: 0.2}

	// The same seed and the same call order must produce the same failures,
	// because that is the only thing that makes a failing simulation run
	// reproducible from its seed alone.
	first := faultTrace(t, cfg)
	second := faultTrace(t, cfg)
	require.Equal(t, first, second)

	cfg.Seed = 8
	require.NotEqual(t, first, faultTrace(t, cfg), "a different seed must explore a different schedule")
}

// faultTrace drives a fault-injecting store through a fixed call sequence and
// returns a printable record of what failed.
func faultTrace(t *testing.T, cfg memory.FaultConfig) []string {
	t.Helper()
	ctx := context.Background()
	store := memory.New(memory.WithFaults(cfg))
	t.Cleanup(func() { _ = store.Close() })

	var trace []string
	for i := 0; i < 40; i++ {
		wf := fmt.Sprintf("wf-%02d", i)
		b := persistencetest.NewBuilder("default", wf, "run-1")
		_, err := store.CreateExecution(ctx, b.Create())
		trace = append(trace, fmt.Sprintf("create %s: %v", wf, errKind(err)))
		if err != nil {
			continue
		}
		b.Signal("s")
		_, err = store.AppendHistory(ctx, b.Append(1))
		trace = append(trace, fmt.Sprintf("append %s: %v", wf, errKind(err)))

		_, err = store.GetExecution(ctx, "default", wf, "run-1")
		trace = append(trace, fmt.Sprintf("get %s: %v", wf, errKind(err)))
	}
	return trace
}

func errKind(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, persistence.ErrVersionConflict):
		return "conflict"
	case errors.Is(err, memory.ErrInjectedFault):
		return "transient"
	default:
		return "unexpected: " + err.Error()
	}
}

func TestFaultInjectionProducesRetryableErrors(t *testing.T) {
	ctx := context.Background()
	store := memory.New(memory.WithFaults(memory.FaultConfig{Seed: 1, ConflictRate: 0.5, ErrorRate: 0.5}))
	t.Cleanup(func() { _ = store.Close() })

	var conflicts, transients, successes int
	for i := 0; i < 200; i++ {
		b := persistencetest.NewBuilder("default", fmt.Sprintf("wf-%03d", i), "run-1")
		_, err := store.CreateExecution(ctx, b.Create())
		switch {
		case err == nil:
			successes++
		case errors.Is(err, persistence.ErrVersionConflict):
			require.ErrorIs(t, err, memory.ErrInjectedFault, "an injected conflict must be identifiable as injected")
			conflicts++
		case errors.Is(err, memory.ErrInjectedFault):
			transients++
		default:
			t.Fatalf("unexpected error %v", err)
		}
	}
	require.Positive(t, conflicts)
	require.Positive(t, transients)
	require.Positive(t, successes)

	// An injected failure must not leave a partial write behind: the number of
	// stored runs has to match the number of calls that returned nil.
	require.Equal(t, successes, store.Stats().Runs)
}

func TestFaultInjectionLatencyRespectsContext(t *testing.T) {
	store := memory.New(memory.WithFaults(memory.FaultConfig{
		Seed:      1,
		LatencyFn: func(string) time.Duration { return time.Hour },
	}))
	t.Cleanup(func() { _ = store.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Minute, "injected latency must not outlive its context")
}

// TestConcurrentReadersAndWriters is a race-detector target. The conformance
// suite proves the version protocol; this proves the lock discipline around it,
// including the read paths that run outside the mutex.
func TestConcurrentReadersAndWriters(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0); err != nil {
					t.Error(err)
					return
				}
				if _, err := store.ListExecutions(ctx, persistence.ListFilter{Namespace: "default"}); err != nil {
					t.Error(err)
					return
				}
				if _, err := store.DueTimers(ctx, time.Now(), 10); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	version := created.Version
	for i := 0; i < 200; i++ {
		b.Signal(fmt.Sprintf("s%d", i))
		rec, err := store.AppendHistory(ctx, b.Append(version))
		require.NoError(t, err)
		version = rec.Version
	}
	close(done)
	wg.Wait()

	h, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.NoError(t, h.Validate())
	require.Equal(t, b.History(), h)
}
