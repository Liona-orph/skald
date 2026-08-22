package workflow

import (
	"time"

	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/skald"
)

// DefaultActivityStartToCloseTimeout is applied when a caller sets neither
// StartToCloseTimeout nor ScheduleToCloseTimeout.
//
// The engine refuses an activity with no upper bound at all, because such an
// activity can wedge an execution forever. Failing the workflow task for a
// missing option would be worse than picking a conservative default, so the SDK
// picks one -- and documents it here so that it is a decision, not a surprise.
const DefaultActivityStartToCloseTimeout = internalwf.DefaultActivityStartToCloseTimeout

// optionsKey namespaces the SDK's context values.
type optionsKey int

const (
	activityOptionsKey optionsKey = iota
	localActivityOptionsKey
)

// ActivityOptions configure the activities scheduled from a context.
//
// They live on the context rather than on each call because a workflow almost
// always wants one policy for a whole region of code, and repeating four
// timeouts at every call site is how a workflow ends up with one activity that
// quietly has no retry policy.
type ActivityOptions struct {
	// TaskQueue routes the activity. Empty means the workflow's own queue,
	// which is the right default; a separate queue is for activities that need
	// different hardware.
	TaskQueue string
	// ActivityID overrides the deterministic identifier the SDK assigns. Leave
	// it empty unless an external system needs to correlate on it: a
	// hand-written ID that is not a deterministic function of the workflow's
	// own state is a non-determinism bug waiting to happen.
	ActivityID string

	// ScheduleToCloseTimeout bounds the activity across every retry.
	ScheduleToCloseTimeout time.Duration
	// ScheduleToStartTimeout bounds how long the task may wait for a worker.
	// Firing it almost always means "nobody is polling this queue".
	ScheduleToStartTimeout time.Duration
	// StartToCloseTimeout bounds a single attempt.
	StartToCloseTimeout time.Duration
	// HeartbeatTimeout makes a long activity prove it is alive. An activity
	// that sets it must call worker.RecordHeartbeat more often than this.
	HeartbeatTimeout time.Duration

	// RetryPolicy governs retries. Nil means the engine's default policy, which
	// retries; set MaximumAttempts to 1 to disable retries entirely.
	RetryPolicy *skald.RetryPolicy
}

// WithActivityOptions returns a context whose activities use opts.
func WithActivityOptions(ctx Context, opts ActivityOptions) Context {
	return WithValue(ctx, activityOptionsKey, opts)
}

// GetActivityOptions returns the options in effect for ctx.
func GetActivityOptions(ctx Context) ActivityOptions {
	opts, _ := ctx.Value(activityOptionsKey).(ActivityOptions)
	return opts
}

// LocalActivityOptions configure local activities.
type LocalActivityOptions struct {
	// ScheduleToCloseTimeout is advisory today: a local activity runs inline in
	// the workflow task, so the workflow task timeout is the real bound. The
	// field exists so that workflow code can express intent and so that a future
	// implementation that runs local activities off-task does not change the API.
	ScheduleToCloseTimeout time.Duration
}

// WithLocalActivityOptions returns a context whose local activities use opts.
func WithLocalActivityOptions(ctx Context, opts LocalActivityOptions) Context {
	return WithValue(ctx, localActivityOptionsKey, opts)
}

// GetLocalActivityOptions returns the local activity options in effect.
func GetLocalActivityOptions(ctx Context) LocalActivityOptions {
	opts, _ := ctx.Value(localActivityOptionsKey).(LocalActivityOptions)
	return opts
}

// ContinueAsNewOptions overrides what the successor run inherits. Every zero
// field keeps the current run's value.
type ContinueAsNewOptions struct {
	WorkflowType string
	TaskQueue    string
	RunTimeout   time.Duration
	TaskTimeout  time.Duration
	RetryPolicy  *skald.RetryPolicy
}
