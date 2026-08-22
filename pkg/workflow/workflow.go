package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"time"

	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/skald"
)

// ---------------------------------------------------------------------------
// Types
//
// Future, Settable and the channel interfaces are declared here rather than
// aliased from the internal package so the public API does not expose internal
// implementation types. They are byte-for-byte the same method sets as their
// internal counterparts, so values flow between the two packages with no
// conversion and no wrapper allocation.
// ---------------------------------------------------------------------------

// Future is the result of an operation that has not finished yet.
type Future[T any] interface {
	// Get blocks the calling coroutine until the result is available.
	Get(ctx Context) (T, error)
	// IsReady reports whether Get would return immediately.
	IsReady() bool
}

// Settable is the write half of a Future, handed out by NewFuture.
type Settable[T any] interface {
	// Set resolves the future; only the first call has an effect.
	Set(value T, err error)
	// SetValue resolves the future successfully.
	SetValue(value T)
	// SetError resolves the future with a failure.
	SetError(err error)
}

// ReceiveChannel is the receiving half of a workflow channel.
type ReceiveChannel[T any] interface {
	Receive(ctx Context) (value T, ok bool)
	ReceiveAsync() (value T, ok bool)
	IsReady() bool
	Len() int
}

// SendChannel is the sending half of a workflow channel.
type SendChannel[T any] interface {
	Send(ctx Context, value T)
	SendAsync(value T) bool
	Close()
	Closed() bool
}

// Channel is a deterministic, dispatcher-aware replacement for a Go channel.
type Channel[T any] interface {
	ReceiveChannel[T]
	SendChannel[T]
}

// ---------------------------------------------------------------------------
// Concurrency primitives
// ---------------------------------------------------------------------------

// Go starts a coroutine. It is the workflow-safe replacement for `go`.
//
// The coroutine is scheduled by the workflow dispatcher, so its interleaving
// with the rest of the workflow is a deterministic function of the code, and it
// can be unwound cleanly when the worker shuts down.
func Go(ctx Context, fn func(ctx Context)) { GoNamed(ctx, "coroutine", fn) }

// GoNamed is Go with a name that appears in deadlock diagnostics. Naming
// coroutines costs nothing and is the difference between "3 coroutines blocked"
// and "the refund poller is blocked on signal cancel-request".
func GoNamed(ctx Context, name string, fn func(ctx Context)) {
	d := internalwf.DispatcherFrom(ctx)
	if d == nil {
		panic("workflow: Go called with a context that is not a workflow context")
	}
	d.NewCoroutine(ctx, name, fn)
}

// NewChannel returns an unbuffered workflow channel.
func NewChannel[T any](ctx Context) Channel[T] {
	return internalwf.NewChannel[T](internalwf.DispatcherFrom(ctx), "channel", 0)
}

// NewBufferedChannel returns a workflow channel with the given buffer size.
func NewBufferedChannel[T any](ctx Context, size int) Channel[T] {
	return internalwf.NewChannel[T](internalwf.DispatcherFrom(ctx), "channel", size)
}

// NewNamedChannel is NewChannel with a name for diagnostics.
func NewNamedChannel[T any](ctx Context, name string, size int) Channel[T] {
	return internalwf.NewChannel[T](internalwf.DispatcherFrom(ctx), name, size)
}

// NewFuture returns the two halves of a future the workflow resolves itself.
func NewFuture[T any](ctx Context) (Future[T], Settable[T]) {
	return internalwf.NewFuture[T](ctx, "future")
}

// ChainFuture resolves target from source when source settles.
//
// It is a free function rather than a method on Settable because a method whose
// parameter is a generic interface and the public API deliberately does not
// expose the internal implementation types.
func ChainFuture[T any](target Settable[T], source Future[T]) {
	s, ok := target.(internalwf.Settable[T])
	if !ok {
		panic("workflow: ChainFuture requires a Settable created by workflow.NewFuture")
	}
	f, ok := source.(internalwf.Future[T])
	if !ok {
		panic("workflow: ChainFuture requires a Future created by this package")
	}
	s.Chain(f)
}

// ThenApply returns a future that resolves by applying fn to f's result. No
// coroutine is spent: the continuation runs as a callback when f settles.
func ThenApply[S, T any](ctx Context, f Future[S], fn func(S, error) (T, error)) Future[T] {
	src, ok := f.(internalwf.Future[S])
	if !ok {
		panic("workflow: ThenApply requires a Future created by this package")
	}
	return internalwf.ThenApply(ctx, src, fn)
}

// NewSelector returns a selector that picks the first ready branch in
// registration order. Unlike Go's select it is never random, which is what makes
// a workflow that races a timer against a result replayable.
func NewSelector(ctx Context) Selector { return internalwf.NewSelector(ctx, "selector") }

// NewNamedSelector is NewSelector with a name for diagnostics.
func NewNamedSelector(ctx Context, name string) Selector { return internalwf.NewSelector(ctx, name) }

// Await blocks until cond returns true, or until ctx is cancelled.
//
// cond is re-evaluated every time the dispatcher makes progress. It must be a
// pure function of workflow state: a condition that consults the outside world
// would make the workflow's decisions unreproducible.
func Await(ctx Context, cond func() bool) error {
	return mustEnv(ctx, "Await").Await(ctx, cond)
}

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

// ExecuteActivity schedules an activity and returns an untyped future.
//
// activity is either the activity function itself -- which the compiler checks
// for you -- or its registered name as a string. Arguments are encoded with the
// worker's data converter.
//
// Use GetResult or ExecuteActivityAs to get a typed value back.
func ExecuteActivity(ctx Context, activity any, args ...any) Future[*skald.Payload] {
	env := mustEnv(ctx, "ExecuteActivity")
	name := internalwf.FunctionName(activity)
	if name == "" {
		return internalwf.ReadyFuture[*skald.Payload](ctx, "activity", nil,
			skald.NewNonRetryableError("InvalidActivity",
				"ExecuteActivity needs an activity function or a registered name, got %T", activity))
	}
	input, err := internalwf.EncodeArgs(env.Converter(), args)
	if err != nil {
		return internalwf.ReadyFuture[*skald.Payload](ctx, "activity "+name, nil,
			skald.NewNonRetryableError("EncodeError", "encoding arguments for activity %s: %v", name, err))
	}
	opts := GetActivityOptions(ctx)
	return env.ExecuteActivity(ctx, internalwf.ActivityParams{
		ActivityID:             opts.ActivityID,
		ActivityType:           name,
		TaskQueue:              opts.TaskQueue,
		Input:                  input,
		RetryPolicy:            opts.RetryPolicy,
		ScheduleToCloseTimeout: opts.ScheduleToCloseTimeout,
		ScheduleToStartTimeout: opts.ScheduleToStartTimeout,
		StartToCloseTimeout:    opts.StartToCloseTimeout,
		HeartbeatTimeout:       opts.HeartbeatTimeout,
	})
}

// ExecuteActivityAs is ExecuteActivity with the result type named explicitly:
//
//	f := workflow.ExecuteActivityAs[Receipt](ctx, Charge, req)
//	receipt, err := f.Get(ctx)
//
// The decoding happens when the future is read, so a decode failure is reported
// to the coroutine that wanted the value.
func ExecuteActivityAs[T any](ctx Context, activity any, args ...any) Future[T] {
	env := mustEnv(ctx, "ExecuteActivity")
	return decode[T](env.Converter(), ExecuteActivity(ctx, activity, args...))
}

// GetResult waits for an untyped future and decodes its payload into T.
//
//	var receipt Receipt
//	err := workflow.GetResult(ctx, f, &receipt)
//
// T is inferred from the destination pointer, which is the one place Go's
// inference does work for this shape. Use Wait when the result is not needed.
func GetResult[T any](ctx Context, f Future[*skald.Payload], out *T) error {
	env := mustEnv(ctx, "GetResult")
	p, err := f.Get(ctx)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return env.Converter().FromPayload(p, out)
}

// Wait blocks for an untyped future and reports only whether it failed. It is
// GetResult for an activity whose return value the workflow does not care about.
func Wait(ctx Context, f Future[*skald.Payload]) error {
	_, err := f.Get(ctx)
	return err
}

// ExecuteLocalActivity runs fn inside the worker process and records the result
// as a history marker, so replay reuses it instead of running fn again.
//
// fn must take a context.Context first, exactly like a registered activity. It
// runs synchronously inside the workflow task: no scheduling event, no dispatch,
// no separate worker. That makes it the right tool for a short, idempotent call
// -- a cache lookup, a signature check -- and the wrong tool for anything that
// can take seconds, because the workflow task deadline is running while it does.
func ExecuteLocalActivity(ctx Context, fn any, args ...any) Future[*skald.Payload] {
	env := mustEnv(ctx, "ExecuteLocalActivity")
	name := internalwf.FunctionName(fn)
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return internalwf.ReadyFuture[*skald.Payload](ctx, "local activity", nil,
			skald.NewNonRetryableError("InvalidActivity",
				"ExecuteLocalActivity needs a function, got %T", fn))
	}
	if err := validateActivityFunc(v.Type()); err != nil {
		return internalwf.ReadyFuture[*skald.Payload](ctx, "local activity "+name, nil,
			skald.NewNonRetryableError("InvalidActivity", "local activity %s: %v", name, err))
	}
	input, err := internalwf.EncodeArgs(env.Converter(), args)
	if err != nil {
		return internalwf.ReadyFuture[*skald.Payload](ctx, "local activity "+name, nil,
			skald.NewNonRetryableError("EncodeError", "encoding arguments for %s: %v", name, err))
	}
	return env.ExecuteLocalActivity(ctx, name, func(c context.Context) (*skald.Payload, error) {
		return internalwf.CallFunc(env.Converter(), v, reflect.ValueOf(c), input)
	})
}

// ExecuteLocalActivityAs is ExecuteLocalActivity with an explicit result type.
func ExecuteLocalActivityAs[T any](ctx Context, fn any, args ...any) Future[T] {
	env := mustEnv(ctx, "ExecuteLocalActivity")
	return decode[T](env.Converter(), ExecuteLocalActivity(ctx, fn, args...))
}

// validateActivityFunc mirrors the worker's registration check so that a local
// activity fails at the call with a clear message instead of panicking inside
// reflect.Call.
func validateActivityFunc(t reflect.Type) error {
	if t.NumIn() < 1 || t.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return fmt.Errorf("first parameter must be context.Context")
	}
	switch t.NumOut() {
	case 1:
		if t.Out(0) != internalwf.ErrorType {
			return fmt.Errorf("single return value must be error")
		}
	case 2:
		if t.Out(1) != internalwf.ErrorType {
			return fmt.Errorf("second return value must be error")
		}
	default:
		return fmt.Errorf("must return (T, error) or error, got %d values", t.NumOut())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

// NewTimer starts a durable timer.
//
// Durable means it survives the death of every process in the system: it lives
// in the store, not in a runtime timer wheel, so a workflow that sleeps for
// thirty days does not need a worker to be running for those thirty days.
func NewTimer(ctx Context, d time.Duration) Future[struct{}] {
	return mustEnv(ctx, "NewTimer").NewTimer(ctx, d)
}

// Sleep blocks the calling coroutine for d of workflow time. It returns a
// *skald.CanceledError if the context is cancelled first.
func Sleep(ctx Context, d time.Duration) error {
	_, err := NewTimer(ctx, d).Get(ctx)
	return err
}

// Now returns the workflow's current time: the timestamp of the workflow task
// being processed. It is the only clock a workflow may read, and it is the same
// on every replay.
func Now(ctx Context) time.Time { return mustEnv(ctx, "Now").Now() }

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

// GetSignalChannel returns the channel carrying signals of the given name,
// decoded into T.
//
//	approvals := workflow.GetSignalChannel[Approval](ctx, "approve")
//	approval, _ := approvals.Receive(ctx)
//
// Signals that arrived before the workflow asked for the channel are already
// buffered in it: a signal accepted by the server is never lost, including one
// delivered by signal-with-start before the workflow ran its first instruction.
func GetSignalChannel[T any](ctx Context, name string) ReceiveChannel[T] {
	env := mustEnv(ctx, "GetSignalChannel")
	return &decodedChannel[T]{src: env.GetSignalChannel(name), conv: env.Converter(), name: name}
}

// GetRawSignalChannel returns the undecoded signal channel, for a workflow that
// wants to inspect the payload itself.
func GetRawSignalChannel(ctx Context, name string) ReceiveChannel[*skald.Payload] {
	return mustEnv(ctx, "GetSignalChannel").GetSignalChannel(name)
}

// decodedChannel adapts a payload channel into a typed one.
//
// The decode happens on receive rather than on delivery so that the workflow's
// own coroutine, not the replayer, reports a malformed signal.
type decodedChannel[T any] struct {
	src  internalwf.ReceiveChannel[*skald.Payload]
	conv skald.DataConverter
	name string
}

func (c *decodedChannel[T]) Receive(ctx Context) (T, bool) {
	p, ok := c.src.Receive(ctx)
	if !ok {
		var zero T
		return zero, false
	}
	return c.decode(p), true
}

func (c *decodedChannel[T]) ReceiveAsync() (T, bool) {
	p, ok := c.src.ReceiveAsync()
	if !ok {
		var zero T
		return zero, false
	}
	return c.decode(p), true
}

func (c *decodedChannel[T]) IsReady() bool { return c.src.IsReady() }
func (c *decodedChannel[T]) Len() int      { return c.src.Len() }

func (c *decodedChannel[T]) decode(p *skald.Payload) T {
	var v T
	if err := c.conv.FromPayload(p, &v); err != nil {
		// A signal whose payload does not fit the type the workflow declared is
		// a contract violation between the sender and the workflow. Panicking
		// fails the workflow task with the reason attached, which is far easier
		// to diagnose than a silently zeroed value.
		panic(fmt.Sprintf("workflow: signal %q could not be decoded into %T: %v", c.name, v, err))
	}
	return v
}

// ---------------------------------------------------------------------------
// Side effects, versions and randomness
// ---------------------------------------------------------------------------

// SideEffect runs fn exactly once for the life of the execution and records the
// result in the history, so every replay reuses the value.
//
// It is the escape hatch for local non-determinism that does not deserve an
// activity: reading an environment-derived constant, generating an idempotency
// token. fn must not block, must not call any workflow API, and must not do
// anything the workflow could not survive doing twice -- a worker that dies
// between running fn and recording the marker will run it again.
func SideEffect[T any](ctx Context, fn func() T) (T, error) {
	env := mustEnv(ctx, "SideEffect")
	conv := env.Converter()
	p, err := env.SideEffect(func() (*skald.Payload, error) { return conv.ToPayload(fn()) })
	var out T
	if err != nil {
		return out, err
	}
	err = conv.FromPayload(p, &out)
	return out, err
}

// MutableSideEffect is SideEffect for a value that may change over a long
// execution, recording a marker only when the value actually differs.
//
// equals may be nil, in which case every call records. A workflow polling a
// feature flag once a minute for a month records one marker per change instead
// of forty thousand markers.
func MutableSideEffect[T any](ctx Context, id string, fn func() T, equals func(a, b T) bool) (T, error) {
	env := mustEnv(ctx, "MutableSideEffect")
	conv := env.Converter()
	var out T

	payloadEquals := func(a, b *skald.Payload) bool {
		if equals == nil {
			return false
		}
		var av, bv T
		if conv.FromPayload(a, &av) != nil || conv.FromPayload(b, &bv) != nil {
			return false
		}
		return equals(av, bv)
	}
	p, err := env.MutableSideEffect(id,
		func() (*skald.Payload, error) { return conv.ToPayload(fn()) },
		payloadEquals)
	if err != nil {
		return out, err
	}
	err = conv.FromPayload(p, &out)
	return out, err
}

// GetVersion is the versioning gate that makes an incompatible workflow change
// safe to deploy while executions are in flight.
//
// The first time a run reaches changeID, GetVersion picks maxSupported and
// writes it into the history. Every replay of that run afterwards reads the
// recorded number, so the branch an execution took is decided once and never
// re-decided by a newer binary. A run whose history predates the change has no
// marker and gets DefaultVersion.
//
//	v := workflow.GetVersion(ctx, "charge-v2", workflow.DefaultVersion, 1)
//	if v == workflow.DefaultVersion {
//	    err = workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ChargeV1, req), nil)
//	} else {
//	    err = workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ChargeV2, req), nil)
//	}
//
// Once every old run has drained, raise minSupported to 1 and delete the old
// branch. A straggler pinned to DefaultVersion then fails loudly with a
// non-determinism error naming the change, instead of silently taking the wrong
// path -- which is the entire reason the gate exists.
func GetVersion(ctx Context, changeID string, minSupported, maxSupported int) int {
	return mustEnv(ctx, "GetVersion").GetVersion(changeID, minSupported, maxSupported)
}

// Rand returns the run's deterministic random source, seeded from the seed the
// server drew once and recorded in event 1. Two replays draw the same numbers.
func Rand(ctx Context) *rand.Rand { return mustEnv(ctx, "Rand").Rand() }

// NewUUID returns a UUID drawn from the run's deterministic stream. It is stable
// across replays, which a UUID from the operating system would not be.
func NewUUID(ctx Context) string { return mustEnv(ctx, "NewUUID").NewUUID() }

// ---------------------------------------------------------------------------
// Execution control
// ---------------------------------------------------------------------------

// ContinueAsNew closes this run and starts a successor with a fresh history and
// the given arguments. Return its result from the workflow function:
//
//	if processed > 1000 {
//	    return workflow.ContinueAsNew(ctx, cursor)
//	}
//
// It is how a workflow runs forever without its history running out: history
// length drives replay cost linearly, so an unbounded loop must periodically
// start over with its state compressed into the successor's input.
func ContinueAsNew(ctx Context, args ...any) error {
	return ContinueAsNewWithOptions(ctx, ContinueAsNewOptions{}, args...)
}

// ContinueAsNewWithOptions is ContinueAsNew with overrides for the successor.
func ContinueAsNewWithOptions(ctx Context, opts ContinueAsNewOptions, args ...any) error {
	env := mustEnv(ctx, "ContinueAsNew")
	input, err := internalwf.EncodeArgs(env.Converter(), args)
	if err != nil {
		return skald.NewNonRetryableError("EncodeError", "encoding continue-as-new arguments: %v", err)
	}
	return env.ContinueAsNew(internalwf.ContinueAsNewParams{
		WorkflowType: opts.WorkflowType,
		TaskQueue:    opts.TaskQueue,
		Input:        input,
		RunTimeout:   opts.RunTimeout,
		TaskTimeout:  opts.TaskTimeout,
		RetryPolicy:  opts.RetryPolicy,
	})
}

// GetInfo returns the run's immutable description.
func GetInfo(ctx Context) Info { return mustEnv(ctx, "GetInfo").GetInfo() }

// GetLogger returns a logger whose output is suppressed while the workflow is
// replaying.
//
// That suppression is not cosmetic. Without it, a workflow on its fortieth task
// re-emits every log line it has ever produced, so one execution generates
// quadratic log volume -- and, far worse, a log line stops being evidence that
// anything happened. "Charging customer 42" would no longer mean a charge was
// attempted, which is exactly the confusion durable execution exists to remove.
func GetLogger(ctx Context) *slog.Logger { return mustEnv(ctx, "GetLogger").Logger() }

// IsReplaying reports whether the code running now is re-deriving a decision
// that has already been made. Use it sparingly: branching on it makes the
// workflow's behaviour depend on how many times it has crashed.
func IsReplaying(ctx Context) bool { return mustEnv(ctx, "IsReplaying").IsReplaying() }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustEnv(ctx Context, op string) *internalwf.Environment {
	env := internalwf.EnvironmentFrom(ctx)
	if env == nil {
		panic(fmt.Sprintf("workflow: %s was called with a context that is not a workflow context; "+
			"the first parameter of a workflow function is the only valid root", op))
	}
	return env
}

// decode adapts an untyped payload future into a typed one.
func decode[T any](conv skald.DataConverter, f Future[*skald.Payload]) Future[T] {
	src, ok := f.(internalwf.Future[*skald.Payload])
	if !ok {
		panic("workflow: expected a future created by this package")
	}
	return internalwf.MapFuture(src, func(p *skald.Payload, err error) (T, error) {
		var out T
		if err != nil {
			return out, err
		}
		if decodeErr := conv.FromPayload(p, &out); decodeErr != nil {
			return out, skald.NewNonRetryableError("DecodeError", "decoding activity result: %v", decodeErr)
		}
		return out, nil
	})
}
