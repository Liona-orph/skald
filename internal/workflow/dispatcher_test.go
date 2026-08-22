package workflow

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/internal/clock"
)

// newTestDispatcher returns a dispatcher and its root context. The environment
// is nil: these tests exercise the scheduler alone, with no notion of history.
func newTestDispatcher(t *testing.T) (*Dispatcher, Context) {
	t.Helper()
	d := NewDispatcher(clock.System())
	t.Cleanup(func() { _ = d.Close() })
	return d, Background(d, nil)
}

// noDeadline disables the wall-clock guard. Every test here either terminates
// or is caught by the deadlock detector, so a timeout would only add flakiness.
var noDeadline time.Time

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

// TestDispatcherInterleavingIsDeterministic is the property the entire engine
// rests on: the same workflow program must produce the same interleaving every
// single time, or replay produces different commands than the original run.
//
// A thousand iterations is not superstition. Go's scheduler is free to run the
// coroutine goroutines in any order it likes, and a hand-off that was
// accidentally racy would show up as a rare reordering rather than as a
// consistent failure -- exactly the shape of bug that survives a ten-run test
// and then corrupts one execution in ten thousand in production.
func TestDispatcherInterleavingIsDeterministic(t *testing.T) {
	t.Parallel()

	program := func() []string {
		d := NewDispatcher(clock.System())
		ctx := Background(d, nil)
		var log []string

		work := NewChannel[int](d, "work", 0)
		results := NewChannel[string](d, "results", 2)

		d.NewCoroutine(ctx, "producer", func(ctx Context) {
			for i := 0; i < 5; i++ {
				log = append(log, fmt.Sprintf("produce %d", i))
				work.Send(ctx, i)
			}
			work.Close()
		})
		for w := 0; w < 3; w++ {
			id := w
			d.NewCoroutine(ctx, fmt.Sprintf("worker-%d", id), func(ctx Context) {
				for {
					v, ok := work.Receive(ctx)
					if !ok {
						log = append(log, fmt.Sprintf("worker %d done", id))
						return
					}
					log = append(log, fmt.Sprintf("worker %d took %d", id, v))
					results.Send(ctx, fmt.Sprintf("%d:%d", id, v))
				}
			})
		}
		d.NewCoroutine(ctx, "collector", func(ctx Context) {
			for i := 0; i < 5; i++ {
				v, ok := results.Receive(ctx)
				if !ok {
					return
				}
				log = append(log, "collected "+v)
			}
		})

		require.NoError(t, d.RunUntilDone(noDeadline))
		require.NoError(t, d.Close())
		return log
	}

	want := program()
	require.NotEmpty(t, want)
	for i := 0; i < 1000; i++ {
		got := program()
		if !equalStrings(want, got) {
			t.Fatalf("run %d interleaved differently:\n first: %s\n  this: %s",
				i, strings.Join(want, " | "), strings.Join(got, " | "))
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDispatcherRunsCoroutinesInCreationOrder pins the scheduling order itself,
// so that a refactor that switched to a map or a work queue fails loudly rather
// than quietly making replay non-deterministic.
func TestDispatcherRunsCoroutinesInCreationOrder(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	var order []string
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("c%d", i)
		d.NewCoroutine(ctx, name, func(ctx Context) { order = append(order, name) })
	}
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, []string{"c0", "c1", "c2", "c3", "c4"}, order)
}

// TestDispatcherSchedulesNestedCoroutinesInTheSamePass checks that a coroutine
// spawned mid-pass runs in that pass. Deferring it to the next one would make
// the command order depend on where in the pass the spawn happened.
func TestDispatcherSchedulesNestedCoroutinesInTheSamePass(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	var order []string
	d.NewCoroutine(ctx, "outer", func(ctx Context) {
		order = append(order, "outer")
		d.NewCoroutine(ctx, "inner", func(ctx Context) {
			order = append(order, "inner")
			d.NewCoroutine(ctx, "innermost", func(ctx Context) {
				order = append(order, "innermost")
			})
		})
		order = append(order, "outer-after-spawn")
	})
	d.NewCoroutine(ctx, "sibling", func(ctx Context) { order = append(order, "sibling") })

	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, []string{"outer", "outer-after-spawn", "sibling", "inner", "innermost"}, order)
	require.Equal(t, 4, d.NumCoroutines())
}

// TestDispatcherWakesACoroutineBlockedBeforeTheWriterRan is the ordering hazard
// the "entering a new blocking call is progress" rule exists for.
//
// The awaiting coroutine is scheduled *first*, so it observes a false condition
// and blocks. The coroutine that makes the condition true runs afterwards and
// then blocks itself. Without the rule the dispatcher would call that a fixpoint
// and the workflow would stall one instruction short of finishing.
func TestDispatcherWakesACoroutineBlockedBeforeTheWriterRan(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	flag := false
	awaited := false
	gate := NewChannel[int](d, "gate", 0)

	d.NewCoroutine(ctx, "waiter", func(ctx Context) {
		co := coroutineFrom(ctx)
		d.markProgress()
		for !flag {
			co.yield("flag")
		}
		awaited = true
	})
	d.NewCoroutine(ctx, "setter", func(ctx Context) {
		flag = true
		// Block on something nobody will ever send to: the point is that
		// *entering* the wait counts as progress.
		gate.Receive(ctx)
	})

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.True(t, awaited, "the waiter never observed the flag the setter raised")
}

// ---------------------------------------------------------------------------
// Deadlock detection
// ---------------------------------------------------------------------------

func TestDispatcherDetectsDeadlockAndNamesTheCoroutines(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	left := NewChannel[int](d, "left", 0)
	right := NewChannel[int](d, "right", 0)

	d.NewCoroutine(ctx, "ping", func(ctx Context) {
		right.Receive(ctx)
		left.Send(ctx, 1)
	})
	d.NewCoroutine(ctx, "pong", func(ctx Context) {
		left.Receive(ctx)
		right.Send(ctx, 1)
	})

	err := d.RunUntilDone(noDeadline)
	require.Error(t, err)

	var deadlock *DeadlockError
	require.ErrorAs(t, err, &deadlock)
	require.Len(t, deadlock.Blocked, 2)

	msg := err.Error()
	require.Contains(t, msg, "ping")
	require.Contains(t, msg, "pong")
	require.Contains(t, msg, "receive on right")
	require.Contains(t, msg, "receive on left")
}

// TestDispatcherDeadlineProducesADiagnosableError covers the production case: a
// workflow that spins instead of blocking must be caught by the task deadline
// and reported with the same coroutine dump.
func TestDispatcherDeadlineProducesADiagnosableError(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	d.NewCoroutine(ctx, "spinner", func(ctx Context) {
		co := coroutineFrom(ctx)
		for {
			// Marking progress on every wake is what an infinite loop of
			// blocking calls looks like from the dispatcher's point of view.
			d.markProgress()
			co.yield("a wait that never ends")
		}
	})

	err := d.ExecuteUntilAllBlocked(time.Now().Add(20 * time.Millisecond))
	require.Error(t, err)
	var deadlock *DeadlockError
	require.ErrorAs(t, err, &deadlock)
	require.Contains(t, err.Error(), "spinner")
	require.Contains(t, err.Error(), "a wait that never ends")
}

// TestDispatcherAllBlockedIsNotADeadlock separates the two states that look
// alike from the outside: a workflow waiting for an activity is healthy, and
// ExecuteUntilAllBlocked must return cleanly for it.
func TestDispatcherAllBlockedIsNotADeadlock(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	_, set := NewFuture[int](ctx, "activity result")
	f, _ := NewFuture[int](ctx, "activity result")
	d.NewCoroutine(ctx, "main", func(ctx Context) { _, _ = f.Get(ctx) })

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.False(t, d.Done())
	require.Len(t, d.Blocked(), 1)
	require.Equal(t, "activity result", d.Blocked()[0].BlockedOn)
	set.SetValue(0)
}

// ---------------------------------------------------------------------------
// Panics
// ---------------------------------------------------------------------------

func TestDispatcherSurfacesPanicsWithAStack(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	d.NewCoroutine(ctx, "bad", func(ctx Context) { panic("boom") })

	err := d.ExecuteUntilAllBlocked(noDeadline)
	require.Error(t, err)
	var panicErr *CoroutinePanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "bad", panicErr.Coroutine)
	require.Equal(t, "boom", panicErr.Value)
	require.Contains(t, panicErr.Stack, "dispatcher_test.go")
}

// ---------------------------------------------------------------------------
// Forced unwinding and goroutine hygiene
// ---------------------------------------------------------------------------

// TestCloseUnwindsBlockedCoroutinesAndRunsTheirDefers proves the sentinel panic
// really does tear down a coroutine parked deep inside user code, and that user
// cleanup still runs on the way out.
func TestCloseUnwindsBlockedCoroutinesAndRunsTheirDefers(t *testing.T) {
	t.Parallel()
	d := NewDispatcher(clock.System())
	ctx := Background(d, nil)

	ch := NewChannel[int](d, "never", 0)
	cleanedUp := 0
	reached := false

	d.NewCoroutine(ctx, "blocked", func(ctx Context) {
		defer func() { cleanedUp++ }()
		func() {
			defer func() { cleanedUp++ }()
			ch.Receive(ctx)
		}()
		reached = true
	})

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.Len(t, d.Blocked(), 1)

	require.NoError(t, d.Close())
	require.Equal(t, 2, cleanedUp, "both defers must run while the stack unwinds")
	require.False(t, reached, "the coroutine must not resume past the blocking call")
	require.True(t, d.Done())
}

// TestCloseLeavesNoGoroutinesBehind is the leak test. Every coroutine is a real
// goroutine, so an executor that is dropped without being closed leaks one per
// coroutine -- invisible until a worker that has run for a week is holding a
// hundred thousand of them.
func TestCloseLeavesNoGoroutinesBehind(t *testing.T) {
	before := goroutineCount()

	for i := 0; i < 50; i++ {
		d := NewDispatcher(clock.System())
		ctx := Background(d, nil)
		ch := NewChannel[int](d, "never", 0)
		for c := 0; c < 10; c++ {
			d.NewCoroutine(ctx, fmt.Sprintf("blocked-%d", c), func(ctx Context) {
				ch.Receive(ctx)
			})
		}
		require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
		require.NoError(t, d.Close())
	}

	waitForGoroutines(t, before)
}

// TestCloseReportsACoroutineThatSwallowsTheUnwind documents the one case the
// sentinel cannot win: user code that recovers from everything. The dispatcher
// gives up after a bounded number of attempts and names the offender, because a
// silent leak is worse than a loud one.
func TestCloseReportsACoroutineThatSwallowsTheUnwind(t *testing.T) {
	t.Parallel()
	d := NewDispatcher(clock.System())
	ctx := Background(d, nil)
	ch := NewChannel[int](d, "never", 0)

	// It recovers more times than Close will try, then finally gives up, so the
	// test can drive it to completion afterwards and not leak a goroutine into
	// the rest of the suite.
	const stubbornness = maxUnwindAttempts + 2
	recovered := 0

	d.NewCoroutine(ctx, "stubborn", func(ctx Context) {
		for recovered < stubbornness {
			func() {
				defer func() {
					if r := recover(); r != nil {
						recovered++
					}
				}()
				ch.Receive(ctx)
			}()
		}
	})

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	err := d.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "stubborn")
	require.Contains(t, err.Error(), "refused to unwind")

	// Let it finish so the goroutine does not outlive the test.
	c := d.coroutines[0]
	for i := 0; i < stubbornness && !c.done; i++ {
		c.execute()
	}
	require.True(t, c.done)
}

func TestNewCoroutineAfterCloseIsIgnored(t *testing.T) {
	t.Parallel()
	d := NewDispatcher(clock.System())
	ctx := Background(d, nil)
	require.NoError(t, d.Close())

	before := goroutineCount()
	d.NewCoroutine(ctx, "late", func(ctx Context) { t.Error("a coroutine created after Close ran") })
	require.Equal(t, 0, d.NumCoroutines())
	waitForGoroutines(t, before)

	require.ErrorContains(t, d.ExecuteUntilAllBlocked(noDeadline), "closed")
}

func TestDispatcherStopConditionEndsThePass(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	stop := false
	var ran []string
	d.SetStopCondition(func() bool { return stop })
	d.NewCoroutine(ctx, "first", func(ctx Context) {
		ran = append(ran, "first")
		stop = true
	})
	d.NewCoroutine(ctx, "second", func(ctx Context) { ran = append(ran, "second") })

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.Equal(t, []string{"first"}, ran)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func goroutineCount() int {
	runtime.GC()
	return runtime.NumGoroutine()
}

// waitForGoroutines allows the runtime a moment to reap goroutines that have
// already returned; the count is observably eventual, not instantaneous.
func waitForGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		now := goroutineCount()
		if now <= before {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines leaked: %d before, %d after\n%s", before, now, buf[:n])
		}
		time.Sleep(time.Millisecond)
	}
}
