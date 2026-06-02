package skald

import (
	"errors"
	"fmt"
	"time"
)

// TimeoutKind distinguishes the four clocks that can independently expire.
type TimeoutKind int32

const (
	// TimeoutUnspecified is the zero value and should never be persisted.
	TimeoutUnspecified TimeoutKind = iota
	// TimeoutScheduleToStart fires when a task waited in a queue for too long,
	// which almost always means "no worker is polling".
	TimeoutScheduleToStart
	// TimeoutStartToClose fires when a single attempt ran for too long.
	TimeoutStartToClose
	// TimeoutScheduleToClose bounds the total wall time across every attempt.
	TimeoutScheduleToClose
	// TimeoutHeartbeat fires when a long-running activity stops reporting
	// liveness. It is the only timeout a worker can extend from user code.
	TimeoutHeartbeat
)

var timeoutNames = map[TimeoutKind]string{
	TimeoutUnspecified:     "UNSPECIFIED",
	TimeoutScheduleToStart: "SCHEDULE_TO_START",
	TimeoutStartToClose:    "START_TO_CLOSE",
	TimeoutScheduleToClose: "SCHEDULE_TO_CLOSE",
	TimeoutHeartbeat:       "HEARTBEAT",
}

func (k TimeoutKind) String() string {
	if n, ok := timeoutNames[k]; ok {
		return n
	}
	return fmt.Sprintf("TimeoutKind(%d)", int32(k))
}

// MarshalText implements encoding.TextMarshaler.
func (k TimeoutKind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (k *TimeoutKind) UnmarshalText(b []byte) error {
	for kk, v := range timeoutNames {
		if v == string(b) {
			*k = kk
			return nil
		}
	}
	return fmt.Errorf("skald: unknown timeout kind %q", b)
}

// ApplicationError is the error type that crosses the process boundary between
// a worker and the server.
//
// Go error values cannot be serialized faithfully, so the SDK converts every
// non-nil error returned by user code into an ApplicationError, preserving the
// message, an operator-chosen Type string and an optional structured Details
// payload. Type is the field a RetryPolicy matches on, which is why it is a
// plain string rather than a Go type: the workflow that inspects the failure may
// be written in a different language than the activity that produced it.
type ApplicationError struct {
	// Type is a stable, caller-defined classification such as
	// "InsufficientFunds". Empty means "unclassified".
	Type string `json:"type,omitempty"`
	// Message is the human-readable summary. It is safe to log.
	Message string `json:"message"`
	// NonRetryable short-circuits the retry policy regardless of Type.
	NonRetryable bool `json:"non_retryable,omitempty"`
	// Details carries a structured payload for programmatic handling.
	Details *Payload `json:"details,omitempty"`
	// Cause is the underlying failure, preserved across one hop so that a
	// workflow can see why an activity failed without string matching.
	Cause *ApplicationError `json:"cause,omitempty"`
	// StackTrace is captured from the worker when panic recovery is involved.
	// It is never interpreted by the engine, only surfaced to humans.
	StackTrace string `json:"stack_trace,omitempty"`
}

// NewApplicationError builds a classified, retryable failure.
func NewApplicationError(typ, format string, args ...any) *ApplicationError {
	return &ApplicationError{Type: typ, Message: fmt.Sprintf(format, args...)}
}

// NewNonRetryableError builds a classified failure that stops retries dead.
func NewNonRetryableError(typ, format string, args ...any) *ApplicationError {
	return &ApplicationError{Type: typ, Message: fmt.Sprintf(format, args...), NonRetryable: true}
}

func (e *ApplicationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.Type != "" && e.Cause != nil:
		return fmt.Sprintf("%s: %s: %v", e.Type, e.Message, e.Cause)
	case e.Type != "":
		return e.Type + ": " + e.Message
	case e.Cause != nil:
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap lets errors.Is and errors.As walk the serialized cause chain.
func (e *ApplicationError) Unwrap() error {
	if e == nil || e.Cause == nil {
		return nil
	}
	return e.Cause
}

// WithDetails attaches a structured payload and returns the receiver, so that
// construction reads as a single expression.
func (e *ApplicationError) WithDetails(p *Payload) *ApplicationError {
	e.Details = p
	return e
}

// TimeoutError reports that one of the four timeout clocks expired.
type TimeoutError struct {
	Kind TimeoutKind `json:"kind"`
	// LastHeartbeatDetails is populated for heartbeat timeouts and lets the next
	// attempt resume from the last checkpoint instead of starting over.
	LastHeartbeatDetails *Payload `json:"last_heartbeat_details,omitempty"`
}

func (e *TimeoutError) Error() string {
	return "skald: activity timed out (" + e.Kind.String() + ")"
}

// CanceledError reports that the execution honoured a cancellation request.
type CanceledError struct {
	Details *Payload `json:"details,omitempty"`
}

func (e *CanceledError) Error() string { return "skald: canceled" }

// TerminatedError reports that an operator ended the execution forcefully.
type TerminatedError struct {
	Reason string `json:"reason,omitempty"`
}

func (e *TerminatedError) Error() string {
	if e.Reason == "" {
		return "skald: terminated"
	}
	return "skald: terminated: " + e.Reason
}

// PanicError wraps a recovered panic from user code. It is always
// non-retryable at the workflow level because a panicking workflow is a bug,
// not a transient condition: retrying it would burn the retry budget and hide
// the defect. Activity panics, by contrast, are converted to retryable
// ApplicationErrors because the activity may be talking to a flaky dependency.
type PanicError struct {
	Value      string `json:"value"`
	StackTrace string `json:"stack_trace,omitempty"`
}

func (e *PanicError) Error() string { return "skald: panic: " + e.Value }

// ContinueAsNewError is returned by workflow code to request a fresh run with a
// truncated history. It is a control-flow signal rather than a failure, and the
// SDK intercepts it before the generic error path.
type ContinueAsNewError struct {
	WorkflowType string        `json:"workflow_type"`
	Input        *Payload      `json:"input,omitempty"`
	TaskQueue    string        `json:"task_queue,omitempty"`
	RunTimeout   time.Duration `json:"run_timeout,omitempty"`
}

func (e *ContinueAsNewError) Error() string {
	return "skald: continue-as-new requested for " + e.WorkflowType
}

// UnknownExternalWorkflowError reports that a signal or cancel targeted an
// execution the server has no record of.
type UnknownExternalWorkflowError struct {
	Execution WorkflowExecution `json:"execution"`
}

func (e *UnknownExternalWorkflowError) Error() string {
	return "skald: unknown external workflow execution " + e.Execution.String()
}

// IsRetryable applies the universal rules that hold before any user-supplied
// RetryPolicy is consulted.
//
// Cancellation and termination are never retried: both are explicit operator or
// caller intent. Panics in workflow code are deterministic bugs. Everything
// else defers to the ApplicationError's own flag.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var canceled *CanceledError
	if errors.As(err, &canceled) {
		return false
	}
	var terminated *TerminatedError
	if errors.As(err, &terminated) {
		return false
	}
	var panicked *PanicError
	if errors.As(err, &panicked) {
		return false
	}
	var app *ApplicationError
	if errors.As(err, &app) {
		return !app.NonRetryable
	}
	return true
}

// ErrorType extracts the classification string used by retry policies. It
// returns the empty string when the error carries no classification.
func ErrorType(err error) string {
	var app *ApplicationError
	if errors.As(err, &app) {
		return app.Type
	}
	var timeout *TimeoutError
	if errors.As(err, &timeout) {
		return "TimeoutError:" + timeout.Kind.String()
	}
	return ""
}

// AsApplicationError converts an arbitrary Go error into the wire form. It is
// the single conversion point used by the worker before shipping a failure back
// to the server, which keeps the serialization rules in one place.
func AsApplicationError(err error) *ApplicationError {
	if err == nil {
		return nil
	}
	var app *ApplicationError
	if errors.As(err, &app) {
		return app
	}
	var panicked *PanicError
	if errors.As(err, &panicked) {
		return &ApplicationError{
			Type:         "PanicError",
			Message:      panicked.Value,
			NonRetryable: false,
			StackTrace:   panicked.StackTrace,
		}
	}
	return &ApplicationError{Message: err.Error()}
}
