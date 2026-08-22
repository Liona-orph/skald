package workflow

import (
	"fmt"
	"time"

	"github.com/Liona-orph/skald/pkg/skald"
)

// Context is the workflow-side analogue of context.Context.
//
// It exists as a separate type for one reason: Done must not hand out a real Go
// channel. A workflow that selected on a runtime channel would park its
// goroutine where the dispatcher cannot see it, the dispatcher would declare the
// workflow blocked while it was in fact waiting, and the command batch would
// stop being a deterministic function of the history. So Done returns a workflow
// channel, and every wait in a workflow goes through the dispatcher.
//
// The context additionally carries the three things every workflow API needs:
// the coroutine that is currently running, the dispatcher, and the environment
// that turns intentions into commands.
type Context interface {
	// Deadline reports the workflow's own deadline. Skald enforces run and
	// execution timeouts server-side, so this is always (zero, false) today; the
	// method exists so that the type stays interchangeable with the standard
	// library's shape in the reader's head.
	Deadline() (time.Time, bool)
	// Done returns a channel that becomes ready when the context is cancelled.
	Done() ReceiveChannel[struct{}]
	// Err is nil until cancellation, then *skald.CanceledError.
	Err() error
	// Value implements request-scoped values, used by the SDK for activity
	// options and by user code for anything else.
	Value(key any) any
}

// CancelFunc cancels a context derived from WithCancel.
type CancelFunc func()

// contextKey namespaces the SDK's own context values so that they cannot
// collide with a user's.
type contextKey int

const (
	coroutineKey contextKey = iota
	dispatcherKey
	environmentKey
)

// ---------------------------------------------------------------------------
// Roots and values
// ---------------------------------------------------------------------------

// rootContext is the base of every workflow context chain.
type rootContext struct {
	dispatcher *Dispatcher
	env        *Environment
	// never is a channel nobody ever closes, returned by Done. Allocating one
	// per root keeps Done non-nil without special cases at every call site.
	never *channelImpl[struct{}]
}

// Background returns the root context of a workflow execution.
func Background(d *Dispatcher, env *Environment) Context {
	return &rootContext{dispatcher: d, env: env, never: newChannel[struct{}](d, "never", 0)}
}

func (*rootContext) Deadline() (time.Time, bool)      { return time.Time{}, false }
func (c *rootContext) Done() ReceiveChannel[struct{}] { return c.never }
func (*rootContext) Err() error                       { return nil }

func (c *rootContext) Value(key any) any {
	switch key {
	case dispatcherKey:
		return c.dispatcher
	case environmentKey:
		return c.env
	}
	return nil
}

// valueContext carries one key/value pair.
type valueContext struct {
	Context
	key, val any
}

// WithValue returns a copy of parent carrying key/val.
func WithValue(parent Context, key, val any) Context {
	if key == nil {
		panic("workflow: WithValue called with a nil key")
	}
	return &valueContext{Context: parent, key: key, val: val}
}

func (c *valueContext) Value(key any) any {
	if key == c.key {
		return c.val
	}
	return c.Context.Value(key)
}

func withCoroutine(parent Context, c *coroutine) Context {
	return WithValue(parent, coroutineKey, c)
}

// coroutineFrom returns the coroutine that ctx belongs to, or nil.
func coroutineFrom(ctx Context) *coroutine {
	c, _ := ctx.Value(coroutineKey).(*coroutine)
	return c
}

// mustCoroutine returns the running coroutine or panics with an explanation.
//
// Reaching this panic means a blocking workflow API was called from a plain
// goroutine -- the single most common way to break determinism, and one that
// would otherwise show up as a mysterious hang. Naming the operation makes the
// fix obvious: use workflow.Go, not go.
func mustCoroutine(ctx Context, op string) *coroutine {
	c := coroutineFrom(ctx)
	if c == nil {
		panic(fmt.Sprintf("workflow: %s was called outside a workflow coroutine; "+
			"workflow code must not start goroutines with `go` -- use workflow.Go so that the "+
			"dispatcher can schedule and unwind them deterministically", op))
	}
	return c
}

// DispatcherFrom returns the dispatcher driving ctx.
func DispatcherFrom(ctx Context) *Dispatcher {
	d, _ := ctx.Value(dispatcherKey).(*Dispatcher)
	return d
}

// EnvironmentFrom returns the workflow environment carried by ctx. It is the
// bridge pkg/workflow uses to turn a user call into a command.
func EnvironmentFrom(ctx Context) *Environment {
	env, _ := ctx.Value(environmentKey).(*Environment)
	return env
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

// cancelContext is a Context that can be cancelled, together with everything
// derived from it.
//
// Cancellation in a workflow is deterministic: it is triggered either by a
// history event (WorkflowExecutionCancelRequested) or by workflow code itself,
// both of which occur at the same logical position on every replay. That is why
// it is safe for cancellation to resolve pending futures directly rather than
// waiting for a server round trip.
type cancelContext struct {
	Context
	done     *channelImpl[struct{}]
	err      error
	children []*cancelContext
	handlers []*cancelHandler
	canceled bool
}

type cancelHandler struct {
	fn      func()
	removed bool
}

// WithCancel derives a cancellable context.
func WithCancel(parent Context) (Context, CancelFunc) {
	d := DispatcherFrom(parent)
	c := &cancelContext{Context: parent, done: newChannel[struct{}](d, "context.Done", 0)}
	if p := parentCancelContext(parent); p != nil {
		if p.canceled {
			c.cancel(p.err)
		} else {
			p.children = append(p.children, c)
		}
	}
	return c, func() { c.cancel(&skald.CanceledError{}) }
}

func (c *cancelContext) Done() ReceiveChannel[struct{}] { return c.done }

func (c *cancelContext) Err() error { return c.err }

func (c *cancelContext) cancel(err error) {
	if c.canceled {
		return
	}
	c.canceled = true
	c.err = err
	// Closing rather than sending: cancellation is broadcast, so every waiter
	// must observe it, and a closed channel is permanently ready.
	c.done.Close()
	// Children first, then handlers. A handler that resolves a pending activity
	// future should see the whole subtree already cancelled, so that anything it
	// wakes up observes a consistent state.
	for _, child := range c.children {
		child.cancel(err)
	}
	c.children = nil
	for _, h := range c.handlers {
		if !h.removed {
			h.fn()
		}
	}
	c.handlers = nil
}

// parentCancelContext walks the chain to the nearest cancellable ancestor.
//
// The walk stops at a detachedContext, which is exactly what makes
// NewDisconnectedContext work: cancellation propagates down the chain by
// registration, so a link that refuses to register is a link cancellation
// cannot cross.
func parentCancelContext(ctx Context) *cancelContext {
	for {
		switch v := ctx.(type) {
		case *cancelContext:
			return v
		case *valueContext:
			ctx = v.Context
		default:
			return nil
		}
	}
}

// detachedContext keeps a parent's values but not its cancellation.
type detachedContext struct {
	Context
	never *channelImpl[struct{}]
}

// NewDisconnectedContext returns a context that shares parent's values and
// environment but is never cancelled by it.
//
// It is what makes compensation possible. Once a workflow's context is
// cancelled, every activity scheduled from it resolves immediately with a
// CanceledError -- which is right, and also means the "undo what we did"
// activity would be cancelled before it was ever dispatched. Running the
// compensation on a disconnected context is the standard shape:
//
//	if err := workflow.Sleep(ctx, time.Hour); err != nil {
//	    cleanup := workflow.NewDisconnectedContext(ctx)
//	    _ = workflow.GetResult(cleanup, workflow.ExecuteActivity(cleanup, Refund, id), nil)
//	    return err
//	}
func NewDisconnectedContext(parent Context) Context {
	return &detachedContext{Context: parent, never: newChannel[struct{}](DispatcherFrom(parent), "never", 0)}
}

func (c *detachedContext) Done() ReceiveChannel[struct{}] { return c.never }
func (c *detachedContext) Err() error                     { return nil }

// onCancel registers fn to run when ctx is cancelled and returns a function
// that unregisters it.
//
// Removal matters: an activity that completes normally must drop its
// cancellation handler, or a later cancel of a long-lived context would walk a
// list that grows for the lifetime of the workflow.
func onCancel(ctx Context, fn func()) (remove func()) {
	p := parentCancelContext(ctx)
	if p == nil {
		return func() {}
	}
	if p.canceled {
		fn()
		return func() {}
	}
	h := &cancelHandler{fn: fn}
	p.handlers = append(p.handlers, h)
	return func() { h.removed = true }
}
