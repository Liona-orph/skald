package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalwf "github.com/skald-io/skald/internal/workflow"
	"github.com/skald-io/skald/pkg/skald"
)

// TestPublicInterfacesAreStructurallyIdenticalToTheInternalOnes is the test that
// justifies the duplicated interface declarations in this package.
//
// Future, Settable and the channel interfaces are deliberately re-declared here
// rather than aliased from internal/workflow. That is only safe if the two
// declarations have identical method sets, in which case values cross the
// boundary with no conversion and no wrapper. This test fails to compile the
// moment they drift apart, which is exactly when someone needs to be told.
func TestPublicInterfacesAreStructurallyIdenticalToTheInternalOnes(t *testing.T) {
	t.Parallel()

	d := internalwf.NewDispatcher(nil)
	t.Cleanup(func() { _ = d.Close() })
	ctx := internalwf.Background(d, nil)

	internalFuture, internalSettable := internalwf.NewFuture[int](ctx, "f")

	// Assignment in both directions is the property that matters: values flow
	// out to user code and back into the machinery without a conversion.
	var publicFuture Future[int] = internalFuture
	var publicSettable Settable[int] = internalSettable
	var backToInternal internalwf.Future[int] = publicFuture.(internalwf.Future[int])
	require.NotNil(t, backToInternal)

	publicSettable.SetValue(7)
	require.True(t, publicFuture.IsReady())

	var publicChannel Channel[string] = internalwf.NewChannel[string](d, "ch", 1)
	require.True(t, publicChannel.SendAsync("x"))
	var receive ReceiveChannel[string] = publicChannel
	v, ok := receive.ReceiveAsync()
	require.True(t, ok)
	require.Equal(t, "x", v)

	// A public future must satisfy Awaitable so a Selector can wait on it.
	var awaitable Awaitable = publicFuture
	require.True(t, awaitable.IsReady())

	// And ctx.Done() must be usable as a public ReceiveChannel.
	var done ReceiveChannel[struct{}] = ctx.Done()
	require.False(t, done.IsReady())
}

func TestActivityOptionsRoundTripThroughTheContext(t *testing.T) {
	t.Parallel()

	d := internalwf.NewDispatcher(nil)
	t.Cleanup(func() { _ = d.Close() })
	ctx := internalwf.Background(d, nil)

	require.Equal(t, ActivityOptions{}, GetActivityOptions(ctx))

	opts := ActivityOptions{
		TaskQueue:           "billing",
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &skald.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = WithActivityOptions(ctx, opts)
	require.Equal(t, opts, GetActivityOptions(ctx))

	// Options are scoped: a nested override does not leak outwards.
	inner := WithActivityOptions(ctx, ActivityOptions{TaskQueue: "reports"})
	require.Equal(t, "reports", GetActivityOptions(inner).TaskQueue)
	require.Equal(t, "billing", GetActivityOptions(ctx).TaskQueue)

	local := LocalActivityOptions{ScheduleToCloseTimeout: time.Second}
	require.Equal(t, local, GetLocalActivityOptions(WithLocalActivityOptions(ctx, local)))
}

// TestWorkflowAPIsRefuseANonWorkflowContext checks the guard rail that turns the
// most common mistake -- calling a workflow API from a plain goroutine or from
// an activity -- into a message that names the fix.
func TestWorkflowAPIsRefuseANonWorkflowContext(t *testing.T) {
	t.Parallel()

	d := internalwf.NewDispatcher(nil)
	t.Cleanup(func() { _ = d.Close() })
	// A context with a dispatcher but no environment: structurally a workflow
	// context, but not one the SDK produced.
	ctx := internalwf.Background(d, nil)

	require.Panics(t, func() { Now(ctx) })
	require.Panics(t, func() { GetInfo(ctx) })
	require.Panics(t, func() { GetLogger(ctx) })
	require.Panics(t, func() { Rand(ctx) })
	require.Panics(t, func() { NewUUID(ctx) })
	require.Panics(t, func() { IsReplaying(ctx) })
	require.Panics(t, func() { _ = Await(ctx, func() bool { return true }) })
	require.Panics(t, func() { GetVersion(ctx, "change", DefaultVersion, 1) })
	require.Panics(t, func() { _ = ExecuteActivity(ctx, "Something") })
}

func TestGoRequiresAWorkflowContext(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { Go(nilContext{}, func(ctx Context) {}) })
}

// nilContext is a Context that carries nothing, standing in for a value a user
// might construct by mistake.
type nilContext struct{}

func (nilContext) Deadline() (time.Time, bool)               { return time.Time{}, false }
func (nilContext) Done() internalwf.ReceiveChannel[struct{}] { return nil }
func (nilContext) Err() error                                { return nil }
func (nilContext) Value(any) any                             { return nil }
