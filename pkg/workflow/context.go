// Package workflow is the API that workflow authors write against.
//
// A Skald workflow is an ordinary Go function:
//
//	func TransferMoney(ctx workflow.Context, req TransferRequest) (Receipt, error) {
//	    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
//	        StartToCloseTimeout: 30 * time.Second,
//	    })
//	    var withdrawn Amount
//	    if err := workflow.GetResult(ctx,
//	        workflow.ExecuteActivity(ctx, Withdraw, req.From, req.Amount), &withdrawn); err != nil {
//	        return Receipt{}, err
//	    }
//	    ...
//	}
//
// The function looks sequential and blocking, and it is -- but every blocking
// call yields to a cooperative scheduler rather than to the Go runtime, and the
// whole function is re-executed from the top on every workflow task. That is
// what makes it durable: the process can die at any instruction and the next
// worker reconstructs the exact same state from the history.
//
// # The rules
//
// Workflow code must be deterministic. Concretely:
//
//   - No time.Now: use workflow.Now, which returns event time.
//   - No math/rand globals: use workflow.Rand or workflow.SideEffect.
//   - No os, no network, no disk: put those in an activity.
//   - No `go`: use workflow.Go so the dispatcher can schedule and unwind it.
//   - No Go channels, mutexes or select: use workflow.Channel and
//     workflow.Selector, which are deterministic by construction.
//   - No ranging over a map when the order affects a decision.
//
// Everything genuinely non-deterministic has an escape hatch that records its
// answer in the history: SideEffect, MutableSideEffect, ExecuteLocalActivity and
// GetVersion.
//
// # A note on generics
//
// Go cannot infer a type parameter from a variadic `...any` argument list, so
// ExecuteActivity returns an untyped future and the typed result is obtained
// either with the explicit-parameter form (ExecuteActivityAs[Receipt]) or with
// GetResult, which infers T from the destination pointer. Both are provided
// because both read well in different situations; neither costs a round trip.
package workflow

import (
	internalwf "github.com/skald-io/skald/internal/workflow"
)

// Context is the workflow-side context. See internal documentation for why it
// is not context.Context: Done must hand out a workflow channel, not a runtime
// channel, or a wait would become invisible to the scheduler.
type Context = internalwf.Context

// CancelFunc cancels a context created by WithCancel.
type CancelFunc = internalwf.CancelFunc

// Info describes the run.
type Info = internalwf.Info

// Selector waits for the first of several events, in registration order.
type Selector = internalwf.Selector

// Awaitable is anything a Selector can wait on: a Future or a Channel.
type Awaitable = internalwf.Awaitable

// DefaultVersion is returned by GetVersion for a run that started before the
// change being gated was introduced.
const DefaultVersion = internalwf.DefaultVersion

// Unbounded is the buffer size of a channel that never blocks a sender.
const Unbounded = internalwf.Unbounded

// WithCancel returns a copy of ctx that can be cancelled.
//
// Cancelling a context resolves every activity future and timer future created
// under it with a *skald.CanceledError, and asks the server to stop the
// corresponding activity. That happens immediately rather than after a round
// trip, so compensation logic runs now instead of after a worker that may
// already be gone acknowledges.
func WithCancel(parent Context) (Context, CancelFunc) { return internalwf.WithCancel(parent) }

// WithValue returns a copy of ctx carrying a key/value pair, exactly like
// context.WithValue.
func WithValue(parent Context, key, val any) Context {
	return internalwf.WithValue(parent, key, val)
}

// NewDisconnectedContext returns a context that shares parent's values but is
// never cancelled by it.
//
// This is how compensation is written. Once a context is cancelled every
// activity scheduled from it resolves immediately with a *skald.CanceledError,
// which means the "undo what we did" activity would be cancelled before it was
// dispatched. Run cleanup on a disconnected context instead:
//
//	if err := workflow.Sleep(ctx, time.Hour); err != nil {
//	    cleanup := workflow.NewDisconnectedContext(ctx)
//	    _ = workflow.GetResult(cleanup, workflow.ExecuteActivity(cleanup, Refund, id), nil)
//	    return err
//	}
func NewDisconnectedContext(parent Context) Context {
	return internalwf.NewDisconnectedContext(parent)
}
