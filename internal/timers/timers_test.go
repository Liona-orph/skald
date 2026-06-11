package timers_test

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/skald-io/skald/internal/clock"
	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/internal/timers"
	"github.com/skald-io/skald/pkg/history"
)

// timerStore is a throwaway store that implements only what the timer service
// uses. Everything else returns an error so that a future call to an
// unimplemented method fails loudly instead of silently doing nothing.
type timerStore struct {
	mu      sync.Mutex
	entries []persistence.TimerRecord
	// dueErr, when set, fails the next DueTimers call and is then cleared.
	dueErr error
	// dueErrs fails the next N calls.
	dueErrs int
	// deleteErr fails every DeleteTimers call while it is set.
	deleteErr error
}

var _ persistence.Store = (*timerStore)(nil)

func (s *timerStore) add(recs ...persistence.TimerRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, recs...)
}

func (s *timerStore) remaining() []persistence.TimerRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]persistence.TimerRecord(nil), s.entries...)
}

func (s *timerStore) failDue(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dueErrs, s.dueErr = n, err
}

func (s *timerStore) failDelete(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteErr = err
}

func (s *timerStore) DueTimers(_ context.Context, now time.Time, limit int) ([]persistence.TimerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dueErrs > 0 {
		s.dueErrs--
		return nil, s.dueErr
	}
	var due []persistence.TimerRecord
	for _, e := range s.entries {
		if !e.FireAt.After(now) {
			due = append(due, e)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].FireAt.Before(due[j].FireAt) })
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (s *timerStore) DeleteTimers(_ context.Context, keys []persistence.TimerKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	kept := s.entries[:0]
	drop := make(map[persistence.TimerKey]bool, len(keys))
	for _, k := range keys {
		drop[k] = true
	}
	for _, e := range s.entries {
		if !drop[e.TimerKey] {
			kept = append(kept, e)
		}
	}
	s.entries = kept
	return nil
}

func (s *timerStore) CreateExecution(context.Context, persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	return persistence.ExecutionRecord{}, errors.New("not implemented")
}

func (s *timerStore) GetExecution(context.Context, string, string, string) (persistence.ExecutionRecord, error) {
	return persistence.ExecutionRecord{}, errors.New("not implemented")
}

func (s *timerStore) ReadHistory(context.Context, string, string, string, int64, int64) (history.History, error) {
	return nil, errors.New("not implemented")
}

func (s *timerStore) AppendHistory(context.Context, persistence.AppendHistoryRequest) (persistence.ExecutionRecord, error) {
	return persistence.ExecutionRecord{}, errors.New("not implemented")
}

func (s *timerStore) ListExecutions(context.Context, persistence.ListFilter) (persistence.ListResult, error) {
	return persistence.ListResult{}, errors.New("not implemented")
}

func (s *timerStore) OpenExecutions(context.Context, string, func(persistence.ExecutionRecord) error) error {
	return errors.New("not implemented")
}

func (s *timerStore) Close() error { return nil }

func rec(id int64, fireAt time.Time, kind persistence.TimerKind) persistence.TimerRecord {
	return persistence.TimerRecord{
		TimerKey: persistence.TimerKey{
			Namespace: "default", WorkflowID: "wf", RunID: "run", EventID: id, Kind: kind,
		},
		FireAt: fireAt,
	}
}

type collector struct {
	mu   sync.Mutex
	got  []persistence.TimerRecord
	fail error
	// gate, when non-nil, blocks each dispatch until it is closed.
	gate chan struct{}
	// entered reports that a dispatch has begun, which is what lets a test say
	// "now that work really is in flight" without guessing at a sleep.
	entered chan struct{}
}

func (c *collector) dispatch(_ context.Context, r persistence.TimerRecord) error {
	if c.entered != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
	}
	if c.gate != nil {
		<-c.gate
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, r)
	return c.fail
}

func (c *collector) seen() []persistence.TimerRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]persistence.TimerRecord(nil), c.got...)
}

func newService(t *testing.T, store persistence.Store, clk clock.Clock, d timers.Dispatch, tweak func(*timers.Config)) *timers.Service {
	t.Helper()
	cfg := timers.Config{
		Store:    store,
		Dispatch: d,
		Clock:    clk,
		Interval: time.Second,
		// Pin the jitter so a test that asserts on scan timing is exact. The
		// jitter itself is covered separately.
		Rand: func() float64 { return 0 },
	}
	if tweak != nil {
		tweak(&cfg)
	}
	svc, err := timers.New(cfg)
	if err != nil {
		t.Fatalf("timers.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svc.Stop(ctx)
	})
	return svc
}

// waitFor spins until cond holds. It is used only to observe work done on
// another goroutine after virtual time was advanced; the *schedule* is still
// asserted against the virtual clock, never against wall time.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFiresDueTimersAndDeletesThem(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	start := clk.Now()
	store.add(
		rec(1, start.Add(time.Second), persistence.TimerKindUser),
		rec(2, start.Add(10*time.Second), persistence.TimerKindActivityRetry),
	)
	c := &collector{}
	svc := newService(t, store, clk, c.dispatch, nil)
	svc.Start()

	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the first timer to fire", func() bool { return len(c.seen()) == 1 })

	if got := c.seen()[0].EventID; got != 1 {
		t.Fatalf("fired event %d, want 1", got)
	}
	if left := store.remaining(); len(left) != 1 || left[0].EventID != 2 {
		t.Fatalf("index holds %v, want only the future timer", left)
	}

	// The second timer is still in the future and must not have fired.
	if svc.Stats().Dispatched != 1 {
		t.Fatalf("Dispatched = %d, want 1", svc.Stats().Dispatched)
	}

	clk.BlockUntil(1)
	clk.Advance(10 * time.Second)
	waitFor(t, "the second timer to fire", func() bool { return len(c.seen()) == 2 })
	if left := store.remaining(); len(left) != 0 {
		t.Fatalf("index still holds %v", left)
	}
}

// TestFailedDispatchLeavesTimerForRedelivery is the at-least-once property.
func TestFailedDispatchLeavesTimerForRedelivery(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.add(rec(1, clk.Now().Add(time.Second), persistence.TimerKindUser))
	c := &collector{fail: errors.New("engine is busy")}
	svc := newService(t, store, clk, c.dispatch, nil)
	svc.Start()

	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the first dispatch", func() bool { return len(c.seen()) == 1 })

	if left := store.remaining(); len(left) != 1 {
		t.Fatalf("a failed dispatch deleted the timer: %v", left)
	}

	// The next scan redelivers it.
	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the redelivery", func() bool { return len(c.seen()) == 2 })
	waitFor(t, "the delete", func() bool { return len(store.remaining()) == 0 })

	if got := svc.Stats().DispatchErrors; got != 1 {
		t.Fatalf("DispatchErrors = %d, want 1", got)
	}
}

func TestScanIntervalIsJittered(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	// A fixed draw of 1 puts the interval at the top of the jitter window, so
	// the assertion is on the schedule rather than on a statistical property.
	svc := newService(t, store, clk, func(context.Context, persistence.TimerRecord) error { return nil },
		func(c *timers.Config) {
			c.Interval = time.Second
			c.JitterFraction = 0.5
			c.Rand = func() float64 { return 1 }
		})
	svc.Start()

	clk.BlockUntil(1)
	pending := clk.Pending()
	if want := clk.Now().Add(1500 * time.Millisecond); !pending[0].Deadline.Equal(want) {
		t.Fatalf("next scan armed for %s, want %s (interval plus full jitter)", pending[0].Deadline, want)
	}
}

func TestBackoffAfterStoreErrors(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.failDue(2, errors.New("database is down"))
	svc := newService(t, store, clk, func(context.Context, persistence.TimerRecord) error { return nil }, nil)
	svc.Start()

	// First scan fails: the next attempt is armed at 2x the interval.
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the first failure", func() bool { return svc.Stats().StoreErrors == 1 })
	clk.BlockUntil(1)
	if got, want := clk.Pending()[0].Deadline, clk.Now().Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("after one failure the next scan is at %s, want %s", got, want)
	}

	// Second failure doubles again.
	clk.Advance(2 * time.Second)
	waitFor(t, "the second failure", func() bool { return svc.Stats().StoreErrors == 2 })
	clk.BlockUntil(1)
	if got, want := clk.Pending()[0].Deadline, clk.Now().Add(4*time.Second); !got.Equal(want) {
		t.Fatalf("after two failures the next scan is at %s, want %s", got, want)
	}

	// A success resets the backoff to the plain interval.
	clk.Advance(4 * time.Second)
	waitFor(t, "a successful scan", func() bool { return svc.Stats().Scans == 1 })
	clk.BlockUntil(1)
	if got, want := clk.Pending()[0].Deadline, clk.Now().Add(time.Second); !got.Equal(want) {
		t.Fatalf("after recovery the next scan is at %s, want %s", got, want)
	}
}

func TestBackoffIsCapped(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.failDue(20, errors.New("still down"))
	svc := newService(t, store, clk, func(context.Context, persistence.TimerRecord) error { return nil },
		func(c *timers.Config) { c.MaxBackoff = 4 * time.Second })
	svc.Start()

	for i := 0; i < 5; i++ {
		clk.BlockUntil(1)
		next := clk.Pending()[0].Deadline
		clk.Advance(next.Sub(clk.Now()))
		want := uint64(i + 1)
		waitFor(t, "a failed scan", func() bool { return svc.Stats().StoreErrors >= want })
	}
	clk.BlockUntil(1)
	if got, want := clk.Pending()[0].Deadline, clk.Now().Add(4*time.Second); !got.Equal(want) {
		t.Fatalf("backoff grew past the cap: next scan at %s, want %s", got, want)
	}
}

func TestGracefulShutdownDrainsInFlightWork(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.add(rec(1, clk.Now().Add(time.Second), persistence.TimerKindUser))

	gate := make(chan struct{})
	c := &collector{gate: gate, entered: make(chan struct{}, 1)}
	svc := newService(t, store, clk, c.dispatch, nil)
	svc.Start()

	clk.BlockUntil(1)
	clk.Advance(time.Second)
	// Wait for the dispatch to actually begin. Stopping a service that is
	// merely *about* to dispatch is allowed to return at once, and this test is
	// about the other case.
	<-c.entered

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- svc.Stop(ctx)
	}()

	// Stop must not return while the callback is still running.
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v while a dispatch was in flight", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(gate)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(c.seen()) != 1 {
		t.Fatalf("the in-flight timer was not dispatched: %v", c.seen())
	}
	if left := store.remaining(); len(left) != 0 {
		t.Fatalf("the drained timer was not deleted: %v", left)
	}
}

func TestStopIsIdempotentAndSafeWithoutStart(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	svc := newService(t, &timerStore{}, clk, func(context.Context, persistence.TimerRecord) error { return nil }, nil)

	ctx := context.Background()
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop on a service that never started: %v", err)
	}
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// Starting after Stop must not resurrect the loop.
	svc.Start()
	if got := svc.Stats().Scans; got != 0 {
		t.Fatalf("a stopped service ran %d scans", got)
	}
}

func TestNoGoroutineLeakAfterStop(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()

	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.add(rec(1, clk.Now().Add(time.Second), persistence.TimerKindUser))
	c := &collector{}
	svc, err := timers.New(timers.Config{
		Store: store, Dispatch: c.dispatch, Clock: clk,
		Interval: time.Second, Rand: func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("timers.New: %v", err)
	}
	svc.Start()
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the timer to fire", func() bool { return len(c.seen()) == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d, baseline %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchSizeBoundsOneScan(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	for i := int64(1); i <= 5; i++ {
		store.add(rec(i, clk.Now().Add(time.Second), persistence.TimerKindUser))
	}
	c := &collector{}
	svc := newService(t, store, clk, c.dispatch, func(cfg *timers.Config) { cfg.BatchSize = 2 })
	svc.Start()

	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the first batch", func() bool { return len(store.remaining()) == 3 })
	if got := len(c.seen()); got != 2 {
		t.Fatalf("first scan dispatched %d timers, want 2", got)
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	if _, err := timers.New(timers.Config{Dispatch: func(context.Context, persistence.TimerRecord) error { return nil }}); !errors.Is(err, timers.ErrNotConfigured) {
		t.Fatalf("missing store returned %v", err)
	}
	if _, err := timers.New(timers.Config{Store: &timerStore{}}); !errors.Is(err, timers.ErrNotConfigured) {
		t.Fatalf("missing dispatch returned %v", err)
	}
}

// TestFailedDeleteLeavesTimerForRedelivery is the other half of at-least-once:
// the callback succeeded but the store could not forget the timer, so the timer
// comes back. The engine's handlers are idempotent precisely so that this is
// merely wasteful rather than wrong.
func TestFailedDeleteLeavesTimerForRedelivery(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	store := &timerStore{}
	store.add(rec(1, clk.Now().Add(time.Second), persistence.TimerKindActivityTimeout))
	store.failDelete(errors.New("write failed"))
	c := &collector{}
	svc := newService(t, store, clk, c.dispatch, nil)
	svc.Start()

	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the first dispatch", func() bool { return len(c.seen()) == 1 })
	if left := store.remaining(); len(left) != 1 {
		t.Fatalf("the timer vanished despite the failed delete: %v", left)
	}
	if got := svc.Stats().StoreErrors; got != 1 {
		t.Fatalf("StoreErrors = %d, want 1", got)
	}
	if got := svc.Stats().Deleted; got != 0 {
		t.Fatalf("Deleted = %d, want 0", got)
	}

	store.failDelete(nil)
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	waitFor(t, "the redelivery", func() bool { return len(store.remaining()) == 0 })
	if got := len(c.seen()); got != 2 {
		t.Fatalf("the timer was dispatched %d times, want 2", got)
	}
}
