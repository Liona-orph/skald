package skald

import "fmt"

// WorkflowStatus is the terminal-or-not state of a workflow execution.
type WorkflowStatus int32

const (
	// StatusRunning means the execution has not reached a terminal state.
	StatusRunning WorkflowStatus = iota
	// StatusCompleted means the workflow function returned successfully.
	StatusCompleted
	// StatusFailed means the workflow function returned an error that was not
	// retryable, or exhausted its retry policy.
	StatusFailed
	// StatusCanceled means a cancellation request was accepted by the workflow.
	StatusCanceled
	// StatusTerminated means an operator forcefully ended the execution without
	// giving the workflow a chance to run cleanup logic.
	StatusTerminated
	// StatusTimedOut means the execution exceeded its run or execution timeout.
	StatusTimedOut
	// StatusContinuedAsNew means this run handed off to a fresh run, carrying a
	// truncated history. The logical workflow is still alive.
	StatusContinuedAsNew
)

var statusNames = map[WorkflowStatus]string{
	StatusRunning:        "RUNNING",
	StatusCompleted:      "COMPLETED",
	StatusFailed:         "FAILED",
	StatusCanceled:       "CANCELED",
	StatusTerminated:     "TERMINATED",
	StatusTimedOut:       "TIMED_OUT",
	StatusContinuedAsNew: "CONTINUED_AS_NEW",
}

func (s WorkflowStatus) String() string {
	if n, ok := statusNames[s]; ok {
		return n
	}
	return fmt.Sprintf("WorkflowStatus(%d)", int32(s))
}

// Terminal reports whether no further history events can be appended.
func (s WorkflowStatus) Terminal() bool { return s != StatusRunning }

// MarshalText implements encoding.TextMarshaler so that statuses appear as
// readable strings in JSON payloads and log lines.
func (s WorkflowStatus) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *WorkflowStatus) UnmarshalText(b []byte) error {
	for k, v := range statusNames {
		if v == string(b) {
			*s = k
			return nil
		}
	}
	return fmt.Errorf("skald: unknown workflow status %q", b)
}
