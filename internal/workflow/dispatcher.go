package workflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/skald-io/skald/internal/clock"
)

// maxUnwindAttempts bounds how many times Close will re-panic a coroutine that
// swallowed the unwind sentinel. Ten is generous: a coroutine with a bare
// recover() in a loop is a bug, and one extra attempt per loop iteration would
// let that bug hang shutdown forever.
const maxUnwindAttempts = 10

// Dispatcher owns an ordered set of coroutines and runs them to a fixpoint.
//
// It is *not* safe for concurrent use, and that is the point: everything it
// touches is workflow state, which must be mutated by exactly one thread of
// control so that replay is reproducible. The worker serialises workflow tasks
// per run before ever reaching this type.
type Dispatcher struct {
	clk clock.Clock

	// coroutines is append-only within a run. Its order is the scheduling
	// order, and it is derived purely from the order in which workflow code
	// spawned them -- which is itself deterministic. This slice is the whole
	// reason two replays interleave identically.
	coroutines []*coroutine
	names      map[string]int

	// progress is set by markProgress during a pass and cleared at its start.
	progress bool
	// closed guards against double Close and against scheduling after teardown.
	closed bool
	// stop lets the owner cut a pass short -- the workflow function returning
	// ends the execution, so no further coroutine may emit a command.
	stop func() bool

	// leaked names coroutines that refused to unwind. Reported by Close.
	leaked []string
}

// NewDispatcher returns an empty dispatcher. A nil clock means the system clock;
// it is used only for deadline enforcement, never by workflow code.
func NewDispatcher(clk clock.Clock) *Dispatcher {
	if clk == nil {
		clk = clock.System()
	}
	return &Dispatcher{clk: clk, names: map[string]int{}}
}

// SetStopCondition installs a predicate consulted between coroutines. When it
// returns true the current pass ends immediately and ExecuteUntilAllBlocked
// returns. It exists so that a workflow whose main function has returned stops
// scheduling siblings that would otherwise append commands after the closing
// one, which the server rejects outright.
func (d *Dispatcher) SetStopCondition(stop func() bool) { d.stop = stop }

// NewCoroutine schedules fn to run as a coroutine derived from ctx.
//
// Nothing runs until the next ExecuteUntilAllBlocked, so a workflow can spawn a
// fan-out and still produce one deterministic command batch.
func (d *Dispatcher) NewCoroutine(ctx Context, name string, fn func(Context)) {
	if d.closed {
		// Spawning after teardown would create a goroutine nobody will ever
		// unwind. Silently dropping it is the only option that cannot leak.
		return
	}
	if name == "" {
		name = "coroutine"
	}
	// Duplicate names are legal (a loop that spawns "worker" ten times) but the
	// deadlock report has to distinguish them, so disambiguate on collision.
	if n := d.names[name]; n > 0 {
		d.names[name] = n + 1
		name = fmt.Sprintf("%s-%d", name, n+1)
	} else {
		d.names[name] = 1
	}
	d.coroutines = append(d.coroutines, newCoroutine(d, ctx, name, fn))
	d.markProgress()
}

// markProgress records that something happened which may unblock a coroutine.
// See the package documentation for the four cases and why case 4 -- a coroutine
// entering a new blocking call -- is load-bearing.
func (d *Dispatcher) markProgress() { d.progress = true }

// NumCoroutines returns the number of coroutines ever created.
func (d *Dispatcher) NumCoroutines() int { return len(d.coroutines) }

// Blocked returns a snapshot of the coroutines that have not finished, in
// scheduling order. It is the raw material of every deadlock diagnostic.
func (d *Dispatcher) Blocked() []BlockedCoroutine {
	out := make([]BlockedCoroutine, 0, len(d.coroutines))
	for _, c := range d.coroutines {
		if c.done {
			continue
		}
		out = append(out, BlockedCoroutine{Name: c.name, BlockedOn: c.blockedOn, Yields: c.yields})
	}
	return out
}

// Done reports whether every coroutine has finished.
func (d *Dispatcher) Done() bool {
	for _, c := range d.coroutines {
		if !c.done {
			return false
		}
	}
	return true
}

// ExecuteUntilAllBlocked runs every coroutine, in creation order, until no
// coroutine can advance without new information.
//
// deadline bounds the whole call in wall-clock time. It is not a fairness knob:
// exceeding it means workflow code is either spinning or blocked on something
// the dispatcher cannot supply, and both are reported as a DeadlockError naming
// every unfinished coroutine and what it was waiting for. A zero deadline
// disables the check, which is only appropriate in tests.
func (d *Dispatcher) ExecuteUntilAllBlocked(deadline time.Time) error {
	if d.closed {
		return fmt.Errorf("workflow: dispatcher is closed")
	}
	for pass := 1; ; pass++ {
		d.progress = false
		// Index-based iteration on purpose: a coroutine that spawns another one
		// during this pass appends to the slice, and the newcomer runs in the
		// same pass. Ranging over a copy would defer it to the next pass and
		// make the command order depend on where in the pass the spawn happened.
		for i := 0; i < len(d.coroutines); i++ {
			if d.stop != nil && d.stop() {
				return nil
			}
			c := d.coroutines[i]
			if c.done {
				continue
			}
			c.execute()
			if c.panicValue != nil {
				return &CoroutinePanicError{
					Coroutine: c.name,
					Value:     c.panicValue,
					Stack:     string(c.panicStack),
				}
			}
			if c.done {
				d.markProgress()
			}
		}
		if !d.progress {
			return nil
		}
		if !deadline.IsZero() && !d.clk.Now().Before(deadline) {
			return &DeadlockError{
				Reason:  fmt.Sprintf("workflow task deadline exceeded after %d scheduling passes", pass),
				Blocked: d.Blocked(),
			}
		}
	}
}

// RunUntilDone runs to a fixpoint and requires that every coroutine finished.
//
// This is the strict form used by tests and by any caller that owns the whole
// program: "nothing runnable and nothing finished" is a real deadlock there,
// because no history event will ever arrive to break the tie. During workflow
// replay the weaker ExecuteUntilAllBlocked is correct instead, since a workflow
// blocked on an activity is healthy, not stuck.
func (d *Dispatcher) RunUntilDone(deadline time.Time) error {
	if err := d.ExecuteUntilAllBlocked(deadline); err != nil {
		return err
	}
	if d.Done() {
		return nil
	}
	return &DeadlockError{
		Reason:  "every coroutine is blocked and none can be woken by another coroutine",
		Blocked: d.Blocked(),
	}
}

// Close unwinds every unfinished coroutine and waits for it to exit.
//
// It returns an error naming any coroutine that swallowed the unwind sentinel,
// because such a coroutine is a permanently parked goroutine: a slow leak that
// will only show up as a worker that grows without bound.
func (d *Dispatcher) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	for _, c := range d.coroutines {
		if c.done {
			continue
		}
		c.unwinding = true
		for attempt := 0; attempt < maxUnwindAttempts && !c.done; attempt++ {
			c.execute()
		}
		if !c.done {
			d.leaked = append(d.leaked, c.name)
		}
	}
	if len(d.leaked) > 0 {
		return fmt.Errorf("workflow: %d coroutine(s) refused to unwind and are leaked: %s; "+
			"this happens when workflow code recovers from every panic, which swallows the "+
			"sentinel the dispatcher uses to tear a blocked coroutine down",
			len(d.leaked), strings.Join(d.leaked, ", "))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// BlockedCoroutine is one entry of a deadlock report.
type BlockedCoroutine struct {
	Name      string
	BlockedOn string
	Yields    int
}

// DeadlockError reports that the dispatcher could not make progress.
//
// The message is deliberately verbose. A workflow that hangs in production is
// diagnosed from this text alone, and "deadlock detected" without the list of
// coroutines and what each is waiting for costs hours.
type DeadlockError struct {
	Reason  string
	Blocked []BlockedCoroutine
}

func (e *DeadlockError) Error() string {
	var b strings.Builder
	b.WriteString("workflow: deadlock: ")
	b.WriteString(e.Reason)
	if len(e.Blocked) == 0 {
		b.WriteString("; no coroutine is blocked, which means the workflow finished without being noticed")
		return b.String()
	}
	fmt.Fprintf(&b, "; %d coroutine(s) blocked:", len(e.Blocked))
	for _, c := range e.Blocked {
		reason := c.BlockedOn
		if reason == "" {
			reason = "an unnamed wait"
		}
		fmt.Fprintf(&b, "\n  - %s: waiting on %s (%d yields)", c.Name, reason, c.Yields)
	}
	return b.String()
}

// CoroutinePanicError carries a panic out of workflow code with the stack that
// produced it. The worker turns it into a workflow task failure so that the
// stack reaches an operator instead of being swallowed by the scheduler.
type CoroutinePanicError struct {
	Coroutine string
	Value     any
	Stack     string
}

func (e *CoroutinePanicError) Error() string {
	return fmt.Sprintf("workflow: coroutine %s panicked: %v", e.Coroutine, e.Value)
}
