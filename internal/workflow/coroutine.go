package workflow

import (
	"fmt"
	"runtime/debug"
)

// unwindSignal is the sentinel panic value used to tear down a coroutine that
// is parked inside arbitrary user code.
//
// Why a panic is the only sane mechanism: Go has no way to stop another
// goroutine. There is no kill, no interrupt, no way to make a function return
// from the outside. A coroutine blocked in yield() is sitting several frames
// deep inside user code -- possibly inside a defer-protected critical section --
// and the only tool the language offers for unwinding an arbitrary stack while
// still running its deferred functions is a panic. So the dispatcher resumes the
// coroutine one last time with a flag set, the yield it wakes up from panics
// with this sentinel, every user defer runs on the way out, and the coroutine's
// root recovers it and exits cleanly.
//
// The alternative, runtime.Goexit, also unwinds and runs defers and cannot be
// swallowed by a user recover(). It is rejected because it terminates the
// goroutine unconditionally, which removes the one diagnostic that matters here:
// a coroutine whose user code swallows the sentinel with a bare recover() is a
// bug (it turns worker shutdown into a goroutine leak), and Skald wants to name
// it rather than paper over it. See Dispatcher.Close, which re-panics on every
// subsequent yield and ultimately reports the coroutine that refused to die.
type unwindSignal struct{ coroutine string }

func (u unwindSignal) String() string {
	return "skald: coroutine " + u.coroutine + " is being unwound by its dispatcher"
}

// coroutine is one cooperatively scheduled goroutine.
//
// Control is passed by two unbuffered channels. The dispatcher sends on resume
// and then immediately blocks receiving on paused; the coroutine receives from
// resume, runs, and sends on paused when it stops. Because both channels are
// unbuffered and the dispatcher does nothing between the send and the receive,
// exactly one side is ever executing coroutine-visible state.
//
// Every field below is therefore written by whichever side currently holds
// control and read by the other side only after a channel hand-off, which
// supplies the happens-before edge. No mutex is needed and the race detector
// agrees.
type coroutine struct {
	dispatcher *Dispatcher
	name       string
	fn         func(Context)
	ctx        Context

	resume chan struct{}
	paused chan struct{}

	// done is set by the root once fn has returned, panicked or unwound.
	done bool
	// unwound records that the coroutine exited through the sentinel rather
	// than by returning normally.
	unwound bool
	// blockedOn is the human-readable reason the coroutine last yielded. It is
	// the payload of a deadlock diagnostic, so it should read like a sentence
	// fragment: "activity act_3", "timer timer_1", "signal approval".
	blockedOn string
	// yields counts hand-offs back to the dispatcher, purely for diagnostics.
	yields int

	// panicValue and panicStack carry a user panic out to the dispatcher.
	panicValue any
	panicStack []byte

	// unwinding is set by the dispatcher before the final resume. The coroutine
	// reads it immediately after waking and converts it into the sentinel panic.
	unwinding bool
}

func newCoroutine(d *Dispatcher, ctx Context, name string, fn func(Context)) *coroutine {
	c := &coroutine{
		dispatcher: d,
		name:       name,
		fn:         fn,
		resume:     make(chan struct{}),
		paused:     make(chan struct{}),
	}
	// The coroutine must see itself in its own context so that the blocking
	// primitives it calls know which coroutine to yield. Deriving the child
	// context here -- rather than letting the caller do it -- makes it
	// impossible to spawn a coroutine that yields on behalf of its parent.
	c.ctx = withCoroutine(ctx, c)
	go c.root()
	return c
}

// root is the body of the underlying goroutine. It parks immediately: creating
// a coroutine must not run any user code, because the workflow's command batch
// depends on the order in which the dispatcher chooses to schedule it.
func (c *coroutine) root() {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(unwindSignal); ok {
				c.unwound = true
			} else {
				c.panicValue = r
				// Captured here rather than at the panic site because a panic
				// crossing frames loses its origin otherwise; this stack still
				// contains every user frame between the panic and this defer.
				c.panicStack = debug.Stack()
			}
		}
		c.done = true
		c.blockedOn = ""
		// The final hand-off. The dispatcher is blocked receiving on paused, so
		// this send always has a partner and can never leak the goroutine.
		c.paused <- struct{}{}
	}()

	<-c.resume
	if c.unwinding {
		panic(unwindSignal{coroutine: c.name})
	}
	c.fn(c.ctx)
}

// yield hands control back to the dispatcher and returns when the dispatcher
// schedules this coroutine again.
//
// reason is recorded for diagnostics only; it never affects scheduling. Blocking
// primitives call yield in a loop and re-test their condition on every wake-up,
// which is what lets the dispatcher resume everything blindly instead of
// maintaining a wait graph it would have to keep consistent.
func (c *coroutine) yield(reason string) {
	c.blockedOn = reason
	c.yields++
	c.paused <- struct{}{}
	<-c.resume
	if c.unwinding {
		panic(unwindSignal{coroutine: c.name})
	}
	c.blockedOn = ""
}

// execute gives the coroutine the CPU and blocks until it yields or finishes.
// It must only be called by the dispatcher, and never on a finished coroutine.
func (c *coroutine) execute() {
	c.resume <- struct{}{}
	<-c.paused
}

// String renders the coroutine for a deadlock report.
func (c *coroutine) String() string {
	switch {
	case c.done:
		return fmt.Sprintf("%s (finished)", c.name)
	case c.blockedOn == "":
		return fmt.Sprintf("%s (runnable)", c.name)
	default:
		return fmt.Sprintf("%s (blocked on %s)", c.name, c.blockedOn)
	}
}
