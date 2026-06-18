package workflow

import "fmt"

// Awaitable is the readiness predicate shared by futures and channels. It is the
// only thing a Selector needs, which is what lets one selector wait on a mix of
// both without any type gymnastics at the call site.
type Awaitable interface {
	IsReady() bool
}

// Future is the result of an operation that has not finished yet.
//
// Get yields to the dispatcher until the result arrives. Everything that can
// take time in a workflow -- an activity, a timer, a child coroutine's result --
// is surfaced as a Future, so "wait for these three things and continue" is
// written the same way regardless of what the three things are.
type Future[T any] interface {
	// Get blocks the calling coroutine until the future is ready.
	Get(ctx Context) (T, error)
	// IsReady reports whether Get would return without blocking.
	IsReady() bool
}

// Settable is the write half of a Future.
//
// Handing the two halves out separately is what keeps a workflow honest: the
// code that waits cannot complete its own future by accident, and the code that
// completes it -- the replayer, applying a history event -- cannot block.
type Settable[T any] interface {
	// Set resolves the future. Only the first call has any effect.
	Set(value T, err error)
	// SetValue resolves the future successfully.
	SetValue(value T)
	// SetError resolves the future with a failure.
	SetError(err error)
	// Chain resolves this future from another one when that one settles.
	Chain(source Future[T])
}

// settledNotifier is implemented by every future this package produces. It lets
// futures compose without spending a coroutine on each link in the chain.
type settledNotifier interface {
	onSettled(fn func())
}

// settableFuture is the single future implementation.
type settableFuture[T any] struct {
	dispatcher *Dispatcher
	name       string

	value     T
	err       error
	ready     bool
	callbacks []func()
}

var (
	_ Future[int]   = (*settableFuture[int])(nil)
	_ Settable[int] = (*settableFuture[int])(nil)
)

// NewFuture returns the two halves of a fresh future.
func NewFuture[T any](ctx Context, name string) (Future[T], Settable[T]) {
	f := newFuture[T](DispatcherFrom(ctx), name)
	return f, f
}

func newFuture[T any](d *Dispatcher, name string) *settableFuture[T] {
	if name == "" {
		name = "future"
	}
	return &settableFuture[T]{dispatcher: d, name: name}
}

// ReadyFuture returns a future that is already resolved. It exists so that a
// fast path -- a cached side effect, a cancelled context -- can return the same
// type as the slow path without the caller branching.
func ReadyFuture[T any](ctx Context, name string, value T, err error) Future[T] {
	f := newFuture[T](DispatcherFrom(ctx), name)
	f.Set(value, err)
	return f
}

func (f *settableFuture[T]) IsReady() bool { return f.ready }

func (f *settableFuture[T]) Get(ctx Context) (T, error) {
	co := mustCoroutine(ctx, "Future.Get")
	// See the package doc: entering a blocking call is progress, because it
	// means this coroutine advanced through user code that may have unblocked
	// a coroutine the dispatcher already visited in this pass.
	if f.dispatcher != nil {
		f.dispatcher.markProgress()
	}
	for !f.ready {
		co.yield(f.name)
	}
	return f.value, f.err
}

func (f *settableFuture[T]) Set(value T, err error) {
	if f.ready {
		// Idempotent rather than fatal. A cancelled activity whose result
		// arrives anyway is a normal race, and turning it into a panic would
		// make a benign server round trip crash the workflow.
		return
	}
	f.ready = true
	f.value = value
	f.err = err
	if f.dispatcher != nil {
		f.dispatcher.markProgress()
	}
	cbs := f.callbacks
	f.callbacks = nil
	for _, cb := range cbs {
		cb()
	}
}

func (f *settableFuture[T]) SetValue(value T) { var zero error; f.Set(value, zero) }

func (f *settableFuture[T]) SetError(err error) {
	var zero T
	f.Set(zero, err)
}

func (f *settableFuture[T]) onSettled(fn func()) {
	if f.ready {
		fn()
		return
	}
	f.callbacks = append(f.callbacks, fn)
}

func (f *settableFuture[T]) Chain(source Future[T]) {
	n, ok := source.(settledNotifier)
	if !ok {
		panic(fmt.Sprintf("workflow: cannot chain %s from a future this package did not create", f.name))
	}
	n.onSettled(func() {
		// source is ready, so this Get cannot block and the nil context is
		// never dereferenced.
		v, err := getReady(source)
		f.Set(v, err)
	})
}

// getReady reads an already-ready future without needing a coroutine.
func getReady[T any](f Future[T]) (T, error) {
	if impl, ok := f.(interface{ readyValue() (T, error) }); ok {
		return impl.readyValue()
	}
	var zero T
	return zero, fmt.Errorf("workflow: future is not readable outside a coroutine")
}

func (f *settableFuture[T]) readyValue() (T, error) { return f.value, f.err }

// ---------------------------------------------------------------------------
// Composition
// ---------------------------------------------------------------------------

// mappedFuture adapts a Future[S] into a Future[T] by transforming its result
// when it is read.
//
// The transform runs on read rather than on settle so that a decoding error is
// reported to the coroutine that actually cares about the value, with that
// coroutine's stack, instead of surfacing inside the replayer.
type mappedFuture[S, T any] struct {
	source Future[S]
	fn     func(S, error) (T, error)
}

var _ Future[string] = (*mappedFuture[int, string])(nil)

// MapFuture returns a future whose value is fn applied to source's.
func MapFuture[S, T any](source Future[S], fn func(S, error) (T, error)) Future[T] {
	return &mappedFuture[S, T]{source: source, fn: fn}
}

func (m *mappedFuture[S, T]) IsReady() bool { return m.source.IsReady() }

func (m *mappedFuture[S, T]) Get(ctx Context) (T, error) {
	v, err := m.source.Get(ctx)
	return m.fn(v, err)
}

func (m *mappedFuture[S, T]) onSettled(fn func()) {
	if n, ok := m.source.(settledNotifier); ok {
		n.onSettled(fn)
		return
	}
	panic("workflow: mapped future wraps a future this package did not create")
}

func (m *mappedFuture[S, T]) readyValue() (T, error) {
	v, err := getReady(m.source)
	return m.fn(v, err)
}

// ThenApply returns a future that resolves by applying fn to f's result.
//
// It costs no coroutine: the continuation is a callback fired by the source
// future's Set. That matters for a workflow that fans out over a thousand items
// -- a coroutine per continuation would make every dispatcher pass a thousand
// hand-offs long.
func ThenApply[S, T any](ctx Context, f Future[S], fn func(S, error) (T, error)) Future[T] {
	out := newFuture[T](DispatcherFrom(ctx), "then")
	n, ok := f.(settledNotifier)
	if !ok {
		panic("workflow: ThenApply requires a future created by this package")
	}
	n.onSettled(func() {
		v, err := getReady(f)
		out.Set(fn(v, err))
	})
	return out
}
