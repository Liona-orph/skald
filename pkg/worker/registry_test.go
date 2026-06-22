package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/skald"
	"github.com/skald-io/skald/pkg/workflow"
)

// The registry's job is to turn a class of 3am incidents into a startup error.
// Each case below is a mistake that would otherwise surface as a failed workflow
// task, possibly weeks after the deploy that introduced it.

func GoodWorkflow(ctx workflow.Context, in string) (string, error) { return in, nil }
func GoodWorkflowNoResult(ctx workflow.Context) error              { return nil }
func GoodActivity(ctx context.Context, a, b int) (int, error)      { return a + b, nil }
func GoodActivityNoResult(ctx context.Context) error               { return nil }

func stdContextWorkflow(ctx context.Context) error               { return nil }
func noContextWorkflow(in string) error                          { return nil }
func noErrorWorkflow(ctx workflow.Context) string                { return "" }
func threeResultWorkflow(ctx workflow.Context) (int, int, error) { return 0, 0, nil }
func channelArgWorkflow(ctx workflow.Context, ch chan int) error { return nil }
func workflowContextActivity(ctx workflow.Context) error         { return nil }
func variadicWorkflow(ctx workflow.Context, xs ...int) error     { return nil }

func TestRegistryAcceptsValidSignatures(t *testing.T) {
	t.Parallel()
	r := NewRegistry(skald.JSONConverter{})

	require.NoError(t, r.RegisterWorkflow(GoodWorkflow, RegisterOptions{}))
	require.NoError(t, r.RegisterWorkflow(GoodWorkflowNoResult, RegisterOptions{}))
	require.NoError(t, r.RegisterActivity(GoodActivity, RegisterOptions{}))
	require.NoError(t, r.RegisterActivity(GoodActivityNoResult, RegisterOptions{}))

	require.Equal(t, []string{"GoodWorkflow", "GoodWorkflowNoResult"}, r.WorkflowNames())
	require.Equal(t, []string{"GoodActivity", "GoodActivityNoResult"}, r.ActivityNames())
}

func TestRegistryRejectsBadWorkflowSignatures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   any
		want string
	}{
		{"a standard context instead of a workflow context", stdContextWorkflow, "must take workflow.Context"},
		{"no context at all", noContextWorkflow, "must take workflow.Context"},
		{"no error result", noErrorWorkflow, "must return error"},
		{"three results", threeResultWorkflow, "must return (T, error) or error"},
		{"an unserializable parameter", channelArgWorkflow, "cannot be encoded into a payload"},
		{"a variadic signature", variadicWorkflow, "variadic"},
		{"not a function", 42, "not a function"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry(nil)
			err := r.RegisterWorkflow(tc.fn, RegisterOptions{Name: "X"})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestRegistryRejectsBadActivitySignatures(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)

	err := r.RegisterActivity(workflowContextActivity, RegisterOptions{Name: "X"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must take context.Context")
	require.Contains(t, err.Error(), "cancellation")
}

// TestRegistryRejectsAnonymousFunctions covers a subtle one: a closure's runtime
// name changes when the surrounding function is edited, so a history recorded
// under it would stop matching after an unrelated refactor.
func TestRegistryRejectsAnonymousFunctions(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)

	err := r.RegisterWorkflow(func(ctx workflow.Context) error { return nil }, RegisterOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "anonymous function or closure")

	// With an explicit name it is fine.
	require.NoError(t, r.RegisterWorkflow(
		func(ctx workflow.Context) error { return nil },
		RegisterOptions{Name: "Explicit"}))
}

func TestRegistryRejectsDuplicates(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	require.NoError(t, r.RegisterWorkflow(GoodWorkflow, RegisterOptions{}))
	err := r.RegisterWorkflow(GoodWorkflow, RegisterOptions{})
	require.ErrorContains(t, err, "already registered")
}

func TestRegistryUnknownTypeNamesWhatItDoesKnow(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	require.NoError(t, r.RegisterWorkflow(GoodWorkflow, RegisterOptions{}))

	_, err := r.WorkflowFunc("Missing")
	require.ErrorIs(t, err, ErrNotRegistered)
	require.Contains(t, err.Error(), "GoodWorkflow")

	empty := NewRegistry(nil)
	_, err = empty.ActivityFunc("Missing")
	require.Contains(t, err.Error(), "its registry is empty")
}

func TestRegistryInvokesActivitiesWithDecodedArguments(t *testing.T) {
	t.Parallel()
	r := NewRegistry(skald.JSONConverter{})
	require.NoError(t, r.RegisterActivity(GoodActivity, RegisterOptions{}))

	fn, err := r.ActivityFunc("GoodActivity")
	require.NoError(t, err)

	input, err := encodeArgsForTest(2, 40)
	require.NoError(t, err)

	out, err := fn(context.Background(), input)
	require.NoError(t, err)

	var got int
	require.NoError(t, skald.JSONConverter{}.FromPayload(out, &got))
	require.Equal(t, 42, got)
}

// TestRegistryAcceptsABareSingleArgumentPayload covers a client in another
// language (or an older one) that encoded a single argument directly rather than
// as a one-element array.
func TestRegistryAcceptsABareSingleArgumentPayload(t *testing.T) {
	t.Parallel()
	r := NewRegistry(skald.JSONConverter{})
	require.NoError(t, r.RegisterWorkflow(GoodWorkflow, RegisterOptions{}))

	fn, err := r.WorkflowFunc("GoodWorkflow")
	require.NoError(t, err)
	require.NotNil(t, fn)

	// The decode path is shared with activities, so exercise it there where no
	// dispatcher is needed.
	require.NoError(t, r.RegisterActivity(EchoOne, RegisterOptions{}))
	act, err := r.ActivityFunc("EchoOne")
	require.NoError(t, err)

	out, err := act(context.Background(), skald.MustPayload("bare"))
	require.NoError(t, err)
	var got string
	require.NoError(t, skald.JSONConverter{}.FromPayload(out, &got))
	require.Equal(t, "bare", got)
}

func EchoOne(ctx context.Context, s string) (string, error) { return s, nil }

func TestWorkerRegistrationPanicsOnBadSignature(t *testing.T) {
	t.Parallel()
	w := New(nil, "queue", Options{})
	require.Panics(t, func() { w.RegisterWorkflow(stdContextWorkflow) })
	require.Panics(t, func() { w.RegisterActivity(workflowContextActivity) })
	require.NotPanics(t, func() { w.RegisterWorkflow(GoodWorkflow) })
}
