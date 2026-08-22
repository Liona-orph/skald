// Package api defines Skald's wire protocol: the request and response bodies of
// every endpoint, plus the error envelope shared by all of them.
//
// The protocol is JSON over HTTP/1.1 with long polling rather than gRPC
// streaming. The trade-off is recorded in docs/adr/0002-http-json-transport.md;
// the short version is that a durable execution engine's hot path is a
// low-frequency, high-latency poll, so the throughput advantage of a binary
// streaming protocol buys little, while a protocol any HTTP client can speak
// removes the SDK bootstrap problem for every language.
package api

import (
	"time"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Version is the protocol version, sent in the Skald-Protocol-Version header.
// The server accepts requests without it and rejects a version it does not
// implement, so an old client fails loudly rather than subtly.
const Version = "1"

// Error is the body of every non-2xx response.
type Error struct {
	// Code is a stable, machine-readable identifier such as "not_found".
	Code string `json:"code"`
	// Message is human-readable and safe to log, never to parse.
	Message string `json:"message"`
	// Details carries structured context, for example the conflicting version.
	Details map[string]string `json:"details,omitempty"`
	// RetryAfter, when set, tells a client how long to wait before retrying.
	RetryAfter time.Duration `json:"retry_after,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Error codes. They map onto HTTP statuses in the transport layer but are
// carried in the body as well so that a client behind a proxy that rewrites
// statuses still gets the truth.
const (
	CodeInvalidArgument    = "invalid_argument"
	CodeNotFound           = "not_found"
	CodeAlreadyExists      = "already_exists"
	CodeVersionConflict    = "version_conflict"
	CodeFailedPrecondition = "failed_precondition"
	CodeResourceExhausted  = "resource_exhausted"
	CodeUnavailable        = "unavailable"
	CodeInternal           = "internal"
	CodeDeadlineExceeded   = "deadline_exceeded"
	CodeUnauthorized       = "unauthorized"
)

// ---------------------------------------------------------------------------
// Client-facing operations
// ---------------------------------------------------------------------------

// StartWorkflowRequest starts a new execution.
type StartWorkflowRequest struct {
	Namespace    string         `json:"namespace,omitempty"`
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	TaskQueue    string         `json:"task_queue"`
	Input        *skald.Payload `json:"input,omitempty"`

	// ExecutionTimeout bounds the logical workflow across retries and
	// continue-as-new. RunTimeout bounds a single run. TaskTimeout bounds how
	// long a worker may hold one workflow task.
	ExecutionTimeout time.Duration `json:"execution_timeout,omitempty"`
	RunTimeout       time.Duration `json:"run_timeout,omitempty"`
	TaskTimeout      time.Duration `json:"task_timeout,omitempty"`

	RetryPolicy  *skald.RetryPolicy `json:"retry_policy,omitempty"`
	CronSchedule string             `json:"cron_schedule,omitempty"`

	// RequestID makes the call idempotent. Two starts with the same request ID
	// return the same run.
	RequestID   string            `json:"request_id,omitempty"`
	ReusePolicy string            `json:"id_reuse_policy,omitempty"`
	Memo        map[string]string `json:"memo,omitempty"`
	SearchAttrs map[string]string `json:"search_attributes,omitempty"`
	Identity    string            `json:"identity,omitempty"`
}

// StartWorkflowResponse identifies the run that was started or reused.
type StartWorkflowResponse struct {
	RunID string `json:"run_id"`
	// Started is false when an existing run was returned because of request ID
	// deduplication, which lets a caller distinguish "I started it" from "it
	// was already running" without a second query.
	Started bool `json:"started"`
}

// SignalWorkflowRequest delivers an asynchronous message.
type SignalWorkflowRequest struct {
	Namespace  string         `json:"namespace,omitempty"`
	WorkflowID string         `json:"workflow_id"`
	RunID      string         `json:"run_id,omitempty"`
	SignalName string         `json:"signal_name"`
	Input      *skald.Payload `json:"input,omitempty"`
	Identity   string         `json:"identity,omitempty"`
	RequestID  string         `json:"request_id,omitempty"`
}

// SignalWithStartRequest signals a running workflow, starting it first if it is
// not running. It exists because the alternative -- query, then start or signal
// -- has a race that duplicates work under concurrency.
type SignalWithStartRequest struct {
	Start       StartWorkflowRequest `json:"start"`
	SignalName  string               `json:"signal_name"`
	SignalInput *skald.Payload       `json:"signal_input,omitempty"`
}

// CancelWorkflowRequest asks a workflow to unwind.
type CancelWorkflowRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Identity   string `json:"identity,omitempty"`
}

// TerminateWorkflowRequest ends a workflow immediately.
type TerminateWorkflowRequest struct {
	Namespace  string         `json:"namespace,omitempty"`
	WorkflowID string         `json:"workflow_id"`
	RunID      string         `json:"run_id,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	Details    *skald.Payload `json:"details,omitempty"`
	Identity   string         `json:"identity,omitempty"`
}

// DescribeWorkflowResponse summarises a run.
type DescribeWorkflowResponse struct {
	Namespace           string               `json:"namespace"`
	WorkflowID          string               `json:"workflow_id"`
	RunID               string               `json:"run_id"`
	WorkflowType        string               `json:"workflow_type"`
	TaskQueue           string               `json:"task_queue"`
	Status              skald.WorkflowStatus `json:"status"`
	StartedAt           time.Time            `json:"started_at"`
	ClosedAt            *time.Time           `json:"closed_at,omitempty"`
	HistoryLength       int64                `json:"history_length"`
	FirstExecutionRunID string               `json:"first_execution_run_id,omitempty"`
	PendingActivities   []PendingActivity    `json:"pending_activities,omitempty"`
	PendingTimers       []PendingTimer       `json:"pending_timers,omitempty"`
	Memo                map[string]string    `json:"memo,omitempty"`
	SearchAttrs         map[string]string    `json:"search_attributes,omitempty"`
}

// PendingActivity is an activity that has not reached a terminal state.
type PendingActivity struct {
	ActivityID       string                  `json:"activity_id"`
	ActivityType     string                  `json:"activity_type"`
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	Attempt          int32                   `json:"attempt"`
	Started          bool                    `json:"started"`
	LastFailure      *skald.ApplicationError `json:"last_failure,omitempty"`
	ScheduledAt      time.Time               `json:"scheduled_at"`
}

// PendingTimer is a durable sleep that has not fired.
type PendingTimer struct {
	TimerID        string    `json:"timer_id"`
	StartedEventID int64     `json:"started_event_id"`
	FireAt         time.Time `json:"fire_at"`
}

// GetHistoryRequest reads a run's history, optionally waiting for more.
type GetHistoryRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
	// FromEventID is inclusive and 1-based.
	FromEventID int64 `json:"from_event_id,omitempty"`
	MaxEvents   int   `json:"max_events,omitempty"`
	// WaitForNew turns the call into a long poll that returns as soon as an
	// event beyond FromEventID exists, which is how a client awaits a result
	// without spinning.
	WaitForNew bool `json:"wait_for_new,omitempty"`
}

// GetHistoryResponse returns a slice of history.
type GetHistoryResponse struct {
	Events history.History      `json:"events"`
	Status skald.WorkflowStatus `json:"status"`
	// NextEventID is where a follow-up call should resume.
	NextEventID int64 `json:"next_event_id"`
}

// ListWorkflowsRequest queries visibility.
type ListWorkflowsRequest struct {
	Namespace    string `json:"namespace,omitempty"`
	WorkflowID   string `json:"workflow_id,omitempty"`
	WorkflowType string `json:"workflow_type,omitempty"`
	Status       string `json:"status,omitempty"`
	PageSize     int    `json:"page_size,omitempty"`
	PageToken    string `json:"page_token,omitempty"`
}

// ListWorkflowsResponse is one page of results.
type ListWorkflowsResponse struct {
	Executions    []DescribeWorkflowResponse `json:"executions"`
	NextPageToken string                     `json:"next_page_token,omitempty"`
}

// ---------------------------------------------------------------------------
// Worker-facing operations
// ---------------------------------------------------------------------------

// PollWorkflowTaskRequest is a long poll for workflow work.
type PollWorkflowTaskRequest struct {
	Namespace string `json:"namespace,omitempty"`
	TaskQueue string `json:"task_queue"`
	Identity  string `json:"identity,omitempty"`
	// RequestID lets the server recognise a retried poll and hand back the same
	// task instead of stranding it.
	RequestID string `json:"request_id,omitempty"`
}

// WorkflowTask is a unit of workflow-code execution.
type WorkflowTask struct {
	// Empty is true when the long poll expired with no work. Clients simply
	// poll again; an empty task is not an error.
	Empty bool `json:"empty,omitempty"`

	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution,omitempty"`
	WorkflowType     string                  `json:"workflow_type,omitempty"`
	TaskQueue        string                  `json:"task_queue,omitempty"`
	ScheduledEventID int64                   `json:"scheduled_event_id,omitempty"`
	StartedEventID   int64                   `json:"started_event_id,omitempty"`
	Attempt          int32                   `json:"attempt,omitempty"`
	// History is the full history up to and including the started event.
	// Skald always sends the complete history rather than a delta: sticky
	// caching is an optimisation the SDK layers on top, and correctness must
	// not depend on a cache being warm.
	History history.History `json:"history,omitempty"`
}

// RespondWorkflowTaskCompletedRequest returns the commands a workflow produced.
type RespondWorkflowTaskCompletedRequest struct {
	Namespace  string                  `json:"namespace,omitempty"`
	Execution  skald.WorkflowExecution `json:"execution"`
	Commands   []history.Command       `json:"commands"`
	Identity   string                  `json:"identity,omitempty"`
	SDKName    string                  `json:"sdk_name,omitempty"`
	SDKVersion string                  `json:"sdk_version,omitempty"`
}

// RespondWorkflowTaskFailedRequest reports that the worker could not process
// the task.
type RespondWorkflowTaskFailedRequest struct {
	Namespace string                          `json:"namespace,omitempty"`
	Execution skald.WorkflowExecution         `json:"execution"`
	Cause     history.WorkflowTaskFailedCause `json:"cause"`
	Failure   *skald.ApplicationError         `json:"failure,omitempty"`
	Identity  string                          `json:"identity,omitempty"`
}

// PollActivityTaskRequest is a long poll for activity work.
type PollActivityTaskRequest struct {
	Namespace string `json:"namespace,omitempty"`
	TaskQueue string `json:"task_queue"`
	Identity  string `json:"identity,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// ActivityTask is one activity attempt.
type ActivityTask struct {
	Empty bool `json:"empty,omitempty"`

	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution,omitempty"`
	ActivityID       string                  `json:"activity_id,omitempty"`
	ActivityType     string                  `json:"activity_type,omitempty"`
	ScheduledEventID int64                   `json:"scheduled_event_id,omitempty"`
	StartedEventID   int64                   `json:"started_event_id,omitempty"`
	Attempt          int32                   `json:"attempt,omitempty"`
	Input            *skald.Payload          `json:"input,omitempty"`

	ScheduledAt time.Time `json:"scheduled_at,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	// Deadline is the wall-clock instant at which the server will consider this
	// attempt timed out. Workers use it to cancel their own context, which
	// keeps a doomed attempt from holding resources after the server gave up.
	Deadline         time.Time     `json:"deadline,omitempty"`
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout,omitempty"`

	LastFailure          *skald.ApplicationError `json:"last_failure,omitempty"`
	LastHeartbeatDetails *skald.Payload          `json:"last_heartbeat_details,omitempty"`
	WorkflowType         string                  `json:"workflow_type,omitempty"`
}

// RespondActivityTaskCompletedRequest reports a successful activity.
type RespondActivityTaskCompletedRequest struct {
	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution"`
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	Result           *skald.Payload          `json:"result,omitempty"`
	Identity         string                  `json:"identity,omitempty"`
}

// RespondActivityTaskFailedRequest reports a failed activity.
type RespondActivityTaskFailedRequest struct {
	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution"`
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	Failure          *skald.ApplicationError `json:"failure"`
	Identity         string                  `json:"identity,omitempty"`
}

// RespondActivityTaskCanceledRequest reports that an activity honoured a
// cancellation request.
type RespondActivityTaskCanceledRequest struct {
	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution"`
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	Details          *skald.Payload          `json:"details,omitempty"`
	Identity         string                  `json:"identity,omitempty"`
}

// RecordActivityHeartbeatRequest reports liveness and a checkpoint.
type RecordActivityHeartbeatRequest struct {
	Namespace        string                  `json:"namespace,omitempty"`
	Execution        skald.WorkflowExecution `json:"execution"`
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	Details          *skald.Payload          `json:"details,omitempty"`
	Identity         string                  `json:"identity,omitempty"`
}

// RecordActivityHeartbeatResponse tells the activity whether to stop.
type RecordActivityHeartbeatResponse struct {
	CancelRequested bool `json:"cancel_requested"`
}

// Endpoint paths. They are constants so that the server, the client and the
// tests cannot drift apart.
const (
	PathStartWorkflow     = "/api/v1/workflows/start"
	PathSignalWorkflow    = "/api/v1/workflows/signal"
	PathSignalWithStart   = "/api/v1/workflows/signal-with-start"
	PathCancelWorkflow    = "/api/v1/workflows/cancel"
	PathTerminateWorkflow = "/api/v1/workflows/terminate"
	PathDescribeWorkflow  = "/api/v1/workflows/describe"
	PathGetHistory        = "/api/v1/workflows/history"
	PathListWorkflows     = "/api/v1/workflows/list"

	PathPollWorkflowTask  = "/api/v1/tasks/workflow/poll"
	PathCompleteWorkflow  = "/api/v1/tasks/workflow/completed"
	PathFailWorkflowTask  = "/api/v1/tasks/workflow/failed"
	PathPollActivityTask  = "/api/v1/tasks/activity/poll"
	PathCompleteActivity  = "/api/v1/tasks/activity/completed"
	PathFailActivity      = "/api/v1/tasks/activity/failed"
	PathCancelActivity    = "/api/v1/tasks/activity/canceled"
	PathHeartbeatActivity = "/api/v1/tasks/activity/heartbeat"

	PathHealth  = "/health"
	PathReady   = "/ready"
	PathMetrics = "/metrics"
)
