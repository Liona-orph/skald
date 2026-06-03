package history

import "fmt"

// RetryState explains, after the fact, why an execution or activity stopped
// being retried. Operators read this field first during an incident, so the
// values distinguish "we gave up" from "we were told to stop".
type RetryState int32

const (
	RetryStateUnspecified RetryState = 0
	// RetryStateInProgress means another attempt is scheduled.
	RetryStateInProgress RetryState = 1
	// RetryStateNonRetryableFailure means the failure was classified as
	// non-retryable, either by the producer or by the policy's type list.
	RetryStateNonRetryableFailure RetryState = 2
	// RetryStateTimeout means a schedule-to-close budget ran out mid-retry.
	RetryStateTimeout RetryState = 3
	// RetryStateMaximumAttemptsReached means the attempt budget ran out.
	RetryStateMaximumAttemptsReached RetryState = 4
	// RetryStateRetryPolicyNotSet means retries were never configured.
	RetryStateRetryPolicyNotSet RetryState = 5
	// RetryStateCanceled means a cancellation preempted the retry loop.
	RetryStateCanceled RetryState = 6
)

var retryStateNames = map[RetryState]string{
	RetryStateUnspecified:            "Unspecified",
	RetryStateInProgress:             "InProgress",
	RetryStateNonRetryableFailure:    "NonRetryableFailure",
	RetryStateTimeout:                "Timeout",
	RetryStateMaximumAttemptsReached: "MaximumAttemptsReached",
	RetryStateRetryPolicyNotSet:      "RetryPolicyNotSet",
	RetryStateCanceled:               "Canceled",
}

func (s RetryState) String() string {
	if n, ok := retryStateNames[s]; ok {
		return n
	}
	return fmt.Sprintf("RetryState(%d)", int32(s))
}

// MarshalText implements encoding.TextMarshaler.
func (s RetryState) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *RetryState) UnmarshalText(b []byte) error {
	for k, v := range retryStateNames {
		if v == string(b) {
			*s = k
			return nil
		}
	}
	return fmt.Errorf("history: unknown retry state %q", b)
}

// WorkflowTaskFailedCause classifies why a worker could not process a workflow
// task. The distinction matters operationally: a non-determinism error is a
// deploy problem that will not fix itself, while a transient worker error will.
type WorkflowTaskFailedCause int32

const (
	WorkflowTaskFailedCauseUnspecified WorkflowTaskFailedCause = 0
	// WorkflowTaskFailedCauseNonDeterminism means the commands produced by
	// replay disagreed with the recorded history. Skald never advances an
	// execution in this state; it keeps retrying the task so that rolling back
	// the offending deploy is enough to recover, with no data loss.
	WorkflowTaskFailedCauseNonDeterminism WorkflowTaskFailedCause = 1
	// WorkflowTaskFailedCauseWorkflowPanic means user code panicked.
	WorkflowTaskFailedCauseWorkflowPanic WorkflowTaskFailedCause = 2
	// WorkflowTaskFailedCauseUnhandledCommand means the worker emitted a
	// command the server rejected.
	WorkflowTaskFailedCauseUnhandledCommand WorkflowTaskFailedCause = 3
	// WorkflowTaskFailedCauseUnregisteredType means the worker polled a queue
	// carrying a workflow type it does not know. Usually a deploy ordering bug.
	WorkflowTaskFailedCauseUnregisteredType WorkflowTaskFailedCause = 4
	// WorkflowTaskFailedCauseBadCommandAttributes means a command was
	// structurally invalid, e.g. a timer with a negative duration.
	WorkflowTaskFailedCauseBadCommandAttributes WorkflowTaskFailedCause = 5
	// WorkflowTaskFailedCauseResourceExhausted means a history or payload limit
	// was exceeded.
	WorkflowTaskFailedCauseResourceExhausted WorkflowTaskFailedCause = 6
)

var workflowTaskFailedCauseNames = map[WorkflowTaskFailedCause]string{
	WorkflowTaskFailedCauseUnspecified:          "Unspecified",
	WorkflowTaskFailedCauseNonDeterminism:       "NonDeterminism",
	WorkflowTaskFailedCauseWorkflowPanic:        "WorkflowPanic",
	WorkflowTaskFailedCauseUnhandledCommand:     "UnhandledCommand",
	WorkflowTaskFailedCauseUnregisteredType:     "UnregisteredType",
	WorkflowTaskFailedCauseBadCommandAttributes: "BadCommandAttributes",
	WorkflowTaskFailedCauseResourceExhausted:    "ResourceExhausted",
}

func (c WorkflowTaskFailedCause) String() string {
	if n, ok := workflowTaskFailedCauseNames[c]; ok {
		return n
	}
	return fmt.Sprintf("WorkflowTaskFailedCause(%d)", int32(c))
}

// Permanent reports whether retrying the same task with the same worker build
// can possibly succeed. Skald uses this to decide whether to keep the task hot
// or to back off aggressively and page.
func (c WorkflowTaskFailedCause) Permanent() bool {
	switch c {
	case WorkflowTaskFailedCauseNonDeterminism,
		WorkflowTaskFailedCauseBadCommandAttributes,
		WorkflowTaskFailedCauseResourceExhausted:
		return true
	}
	return false
}

// MarshalText implements encoding.TextMarshaler.
func (c WorkflowTaskFailedCause) MarshalText() ([]byte, error) { return []byte(c.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (c *WorkflowTaskFailedCause) UnmarshalText(b []byte) error {
	for k, v := range workflowTaskFailedCauseNames {
		if v == string(b) {
			*c = k
			return nil
		}
	}
	return fmt.Errorf("history: unknown workflow task failed cause %q", b)
}
