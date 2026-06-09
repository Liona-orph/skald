// Package clock decouples Skald from the operating system's notion of time.
//
// Every component that waits -- the matching long poll, the durable timer
// service, the engine's retry backoff -- reads time through this interface
// rather than through the time package. That single indirection is what makes
// the test suite both fast and honest.
//
// # Why a virtual clock beats time.Sleep in tests
//
// A test that exercises a five second retry backoff with real time has three
// bad options: sleep for five seconds (a suite that takes minutes and that
// nobody runs before pushing), shrink the production constant until the test is
// fast (now the test exercises a configuration nobody deploys), or poll until
// the condition holds (a race that passes on a fast laptop and flakes in CI
// under load).
//
// A virtual clock removes the trade-off. Time advances only when a test says
// so, so a test can assert that "nothing fires at 4.999s and exactly one thing
// fires at 5s" -- a statement about the *schedule*, which is what the code
// actually promises, rather than about how long a machine happened to take.
// Advancing an hour costs microseconds, so timeout and backoff paths become
// cheap enough to test exhaustively instead of being covered by one hopeful
// integration test.
//
// The subtlety a virtual clock introduces is that firing a timer only makes a
// goroutine *runnable*; it does not run it. Advance therefore hands control
// back before the woken goroutine has observed the tick. Tests synchronise with
// BlockUntil, which waits for a known number of timers to be registered, and
// with the assertions themselves, which retry against an in-memory condition
// rather than sleeping. See clock_test.go for the pattern.
package clock

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Timer is the subset of *time.Timer that Skald uses.
//
// It is an interface rather than a struct so that the virtual implementation
// can hand out a channel it controls. Stop and Reset keep the semantics of the
// standard library: Stop reports whether the timer was still armed, and Reset
// must only be called on a timer that has been stopped or has already fired.
type Timer interface {
	// C is the channel on which the tick is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing and reports whether it was armed.
	Stop() bool
	// Reset re-arms the timer and reports whether it was armed beforehand.
	Reset(d time.Duration) bool
}

// Clock is the source of time for everything above this package.
type Clock interface {
	// Now returns the current instant.
	Now() time.Time
	// NewTimer returns a timer that fires once after d. A non-positive d fires
	// immediately, matching time.NewTimer.
	NewTimer(d time.Duration) Timer
	// After is shorthand for NewTimer(d).C() for call sites that never cancel.
	// Prefer NewTimer plus Stop in loops: After leaks its timer until it fires.
	After(d time.Duration) <-chan time.Time
	// Sleep blocks for d.
	Sleep(d time.Duration)
}

// ---------------------------------------------------------------------------
// System clock
// ---------------------------------------------------------------------------

// System returns the production clock, backed by the time package.
//
// It is a function rather than an exported variable so that no caller can
// reassign the global clock out from under another package -- the kind of
// action-at-a-distance that makes a test suite unreproducible.
func System() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) NewTimer(d time.Duration) Timer         { return &systemTimer{t: time.NewTimer(d)} }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (systemClock) Sleep(d time.Duration)                  { time.Sleep(d) }

type systemTimer struct{ t *time.Timer }

func (s *systemTimer) C() <-chan time.Time { return s.t.C }
func (s *systemTimer) Stop() bool          { return s.t.Stop() }
func (s *systemTimer) Reset(d time.Duration) bool {
	return s.t.Reset(d)
}

// ---------------------------------------------------------------------------
// Virtual clock
// ---------------------------------------------------------------------------

// PendingTimer describes one armed virtual timer. Tests use it to assert on the
// *schedule* a component produced -- "the retry is armed 2s out, not 200ms" --
// which is a far stronger statement than observing that something eventually
// happened.
type PendingTimer struct {
	// ID is a monotonically increasing registration number. Timers armed
	// earlier have smaller IDs, which is what makes Advance's ordering total
	// even when two timers share a deadline.
	ID int64
	// Deadline is the instant at which the timer fires.
	Deadline time.Time
}

// Virtual is a Clock whose time only moves when a test moves it.
//
// It is safe for concurrent use: the component under test typically arms timers
// from its own goroutines while the test advances time from the test goroutine.
type Virtual struct {
	mu     sync.Mutex
	now    time.Time
	nextID int64
	timers map[int64]*virtualTimer
	// changed is closed and replaced whenever the timer set changes, which is
	// how BlockUntil waits without polling.
	changed chan struct{}
}

var _ Clock = (*Virtual)(nil)

// NewVirtual returns a clock stopped at start.
//
// A zero start is replaced with a fixed, arbitrary instant rather than the
// zero time, because the zero time.Time is easy to confuse with "unset" in
// assertions and because durations before it cannot be represented.
func NewVirtual(start time.Time) *Virtual {
	if start.IsZero() {
		start = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Virtual{
		now:     start,
		timers:  make(map[int64]*virtualTimer),
		changed: make(chan struct{}),
	}
}

// Now implements Clock.
func (v *Virtual) Now() time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.now
}

// Set moves the clock to an absolute instant, firing everything due in between.
// Moving time backwards is ignored: it would let a test observe a history whose
// timestamps decrease, which the history validator rejects anyway.
func (v *Virtual) Set(t time.Time) {
	v.mu.Lock()
	now := v.now
	v.mu.Unlock()
	if t.After(now) {
		v.Advance(t.Sub(now))
	}
}

// Advance moves time forward by d, firing every timer that comes due in the
// window in deadline order.
//
// Firing happens with the lock released and the clock already moved to the
// timer's own deadline, so a callback that reads Now sees the instant it was
// scheduled for rather than the end of the window. A timer armed by a woken
// goroutine during the same Advance is picked up only if it is registered
// before the loop looks again; tests that depend on that ordering should
// advance in explicit steps, which is clearer in the test anyway.
//
// A non-positive d is a no-op.
func (v *Virtual) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	v.mu.Lock()
	target := v.now.Add(d)
	v.mu.Unlock()

	for {
		v.mu.Lock()
		t := v.earliestDue(target)
		if t == nil {
			v.now = target
			v.mu.Unlock()
			return
		}
		v.now = t.deadline
		delete(v.timers, t.id)
		at := v.now
		v.broadcastLocked()
		v.mu.Unlock()

		t.deliver(at)
	}
}

// earliestDue returns the armed timer with the smallest deadline at or before
// target, breaking ties by registration order. The scan is linear because a
// test never has more than a handful of timers armed and a heap would trade
// readability for an optimisation nobody can measure.
func (v *Virtual) earliestDue(target time.Time) *virtualTimer {
	var best *virtualTimer
	for _, t := range v.timers {
		if t.deadline.After(target) {
			continue
		}
		if best == nil || t.deadline.Before(best.deadline) || (t.deadline.Equal(best.deadline) && t.id < best.id) {
			best = t
		}
	}
	return best
}

// NewTimer implements Clock.
func (v *Virtual) NewTimer(d time.Duration) Timer {
	v.mu.Lock()
	t := &virtualTimer{
		clock:    v,
		id:       v.nextID,
		ch:       make(chan time.Time, 1),
		deadline: v.now.Add(d),
	}
	v.nextID++
	if d <= 0 {
		// Match time.NewTimer: a non-positive duration fires at once. Doing it
		// inline keeps callers that use a zero timeout from hanging forever.
		now := v.now
		v.mu.Unlock()
		t.deliver(now)
		return t
	}
	v.timers[t.id] = t
	v.broadcastLocked()
	v.mu.Unlock()
	return t
}

// After implements Clock.
func (v *Virtual) After(d time.Duration) <-chan time.Time { return v.NewTimer(d).C() }

// Sleep implements Clock. It blocks until a test advances past now+d, so a
// component that sleeps in a loop is driven entirely by the test.
func (v *Virtual) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	<-v.NewTimer(d).C()
}

// Pending returns the armed timers ordered by deadline.
func (v *Virtual) Pending() []PendingTimer {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]PendingTimer, 0, len(v.timers))
	for _, t := range v.timers {
		out = append(out, PendingTimer{ID: t.id, Deadline: t.deadline})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Deadline.Equal(out[j].Deadline) {
			return out[i].ID < out[j].ID
		}
		return out[i].Deadline.Before(out[j].Deadline)
	})
	return out
}

// NumTimers returns how many timers are armed.
func (v *Virtual) NumTimers() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.timers)
}

// BlockUntil waits until at least n timers are armed.
//
// This is the synchronisation primitive that makes virtual-time tests
// deterministic: arm-then-advance is a race unless the test can wait for the
// arm. It blocks forever if the count is never reached, which surfaces as a
// test timeout with a goroutine dump naming this function -- a much better
// failure than a flaky assertion.
func (v *Virtual) BlockUntil(n int) {
	_ = v.BlockUntilContext(context.Background(), n)
}

// BlockUntilContext is BlockUntil bounded by ctx.
func (v *Virtual) BlockUntilContext(ctx context.Context, n int) error {
	for {
		v.mu.Lock()
		if len(v.timers) >= n {
			v.mu.Unlock()
			return nil
		}
		changed := v.changed
		v.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// broadcastLocked wakes every BlockUntil waiter. The caller must hold v.mu.
func (v *Virtual) broadcastLocked() {
	close(v.changed)
	v.changed = make(chan struct{})
}

type virtualTimer struct {
	clock    *Virtual
	id       int64
	ch       chan time.Time
	deadline time.Time
}

func (t *virtualTimer) C() <-chan time.Time { return t.ch }

// deliver posts the tick without blocking. The channel has room for one value
// and a timer fires once, so a full buffer means the receiver has not drained
// the previous tick -- dropping it matches time.Timer, which also never blocks
// the runtime on a slow receiver.
func (t *virtualTimer) deliver(at time.Time) {
	select {
	case t.ch <- at:
	default:
	}
}

func (t *virtualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if _, armed := t.clock.timers[t.id]; !armed {
		return false
	}
	delete(t.clock.timers, t.id)
	t.clock.broadcastLocked()
	return true
}

func (t *virtualTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	_, armed := t.clock.timers[t.id]
	t.deadline = t.clock.now.Add(d)
	now := t.clock.now
	if d <= 0 {
		delete(t.clock.timers, t.id)
	} else {
		t.clock.timers[t.id] = t
	}
	t.clock.broadcastLocked()
	t.clock.mu.Unlock()

	if d <= 0 {
		t.deliver(now)
	}
	return armed
}
