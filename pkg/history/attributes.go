package history

import (
	"time"

	"github.com/Liona-orph/skald/pkg/skald"
)

// Attributes is the payload of a history event. Every implementation is a
// plain data struct with no methods beyond Type, which keeps the persisted
// format obvious from the type definition alone.
type Attributes interface {
	// Type returns the event type this attribute set belongs to.
	Type() EventType
}

// ---------------------------------------------------------------------------
// Execution lifecycle
// ---------------------------------------------------------------------------

// WorkflowExecutionStartedAttributes is always event 1 of a history. It is the
// only place the immutable configuration of a run is written down, which means
// a replayer can reconstruct everything it needs from the first event plus the
// events that follow.
type WorkflowExecutionStartedAttributes struct {
	WorkflowType     string             `json:"workflow_type"`
	TaskQueue        string             `json:"task_queue"`
	Input            *skald.Payload     `json:"input,omitempty"`
	RunTimeout       time.Duration      `json:"run_timeout,omitempty"`
	ExecutionTimeout time.Duration      `json:"execution_timeout,omitempty"`
	TaskTimeout      time.Duration      `json:"task_timeout,omitempty"`
	RetryPolicy      *skald.RetryPolicy `json:"retry_policy,omitempty"`
	Attempt          int32              `json:"attempt"`
	// ContinuedExecutionRunID links a continued-as-new run to its predecessor,
	// forming a chain that the CLI can walk to show a logical workflow's whole
	// life across many physical runs.
	ContinuedExecutionRunID string `json:"continued_execution_run_id,omitempty"`
	// FirstExecutionRunID is the head of that chain and stays constant.
	FirstExecutionRunID string `json:"first_execution_run_id,omitempty"`
	// RandomnessSeed feeds the workflow-side deterministic RNG. Drawn once by
	// the server so that side-effect-free randomness survives replay.
	RandomnessSeed int64             `json:"randomness_seed"`
	Memo           map[string]string `json:"memo,omitempty"`
	SearchAttrs    map[string]string `json:"search_attributes,omitempty"`
	Identity       string            `json:"identity,omitempty"`
	// CronSchedule, when set, causes a new run to be scheduled after this one
	// closes.
	CronSchedule string `json:"cron_schedule,omitempty"`
}

func (WorkflowExecutionStartedAttributes) Type() EventType {
	return EventTypeWorkflowExecutionStarted
}

// WorkflowExecutionCompletedAttributes records a successful return.
type WorkflowExecutionCompletedAttributes struct {
	Result *skald.Payload `json:"result,omitempty"`
	// WorkflowTaskCompletedEventID points at the task whose command produced
	// this event. Every command-derived event carries this back-reference,
	// which is what makes a history auditable: you can always find the decision
	// that caused an effect.
	WorkflowTaskCompletedEventID int64 `json:"workflow_task_completed_event_id"`
}

func (WorkflowExecutionCompletedAttributes) Type() EventType {
	return EventTypeWorkflowExecutionCompleted
}

// WorkflowExecutionFailedAttributes records a terminal failure.
type WorkflowExecutionFailedAttributes struct {
	Failure                      *skald.ApplicationError `json:"failure"`
	WorkflowTaskCompletedEventID int64                   `json:"workflow_task_completed_event_id"`
	// RetryState explains why no further attempt was made.
	RetryState RetryState `json:"retry_state"`
}

func (WorkflowExecutionFailedAttributes) Type() EventType {
	return EventTypeWorkflowExecutionFailed
}

// WorkflowExecutionTimedOutAttributes records run or execution timeout expiry.
type WorkflowExecutionTimedOutAttributes struct {
	Kind       skald.TimeoutKind `json:"kind"`
	RetryState RetryState        `json:"retry_state"`
}

func (WorkflowExecutionTimedOutAttributes) Type() EventType {
	return EventTypeWorkflowExecutionTimedOut
}

// WorkflowExecutionCanceledAttributes records that the workflow accepted a
// cancellation request and unwound.
type WorkflowExecutionCanceledAttributes struct {
	Details                      *skald.Payload `json:"details,omitempty"`
	WorkflowTaskCompletedEventID int64          `json:"workflow_task_completed_event_id"`
}

func (WorkflowExecutionCanceledAttributes) Type() EventType {
	return EventTypeWorkflowExecutionCanceled
}

// WorkflowExecutionTerminatedAttributes records an operator kill. No workflow
// code runs in response, which is the difference between terminate and cancel.
type WorkflowExecutionTerminatedAttributes struct {
	Reason   string         `json:"reason,omitempty"`
	Details  *skald.Payload `json:"details,omitempty"`
	Identity string         `json:"identity,omitempty"`
}

func (WorkflowExecutionTerminatedAttributes) Type() EventType {
	return EventTypeWorkflowExecutionTerminated
}

// WorkflowExecutionContinuedAsNewAttributes closes this run and names its
// successor.
type WorkflowExecutionContinuedAsNewAttributes struct {
	NewRunID                     string             `json:"new_run_id"`
	WorkflowType                 string             `json:"workflow_type"`
	TaskQueue                    string             `json:"task_queue"`
	Input                        *skald.Payload     `json:"input,omitempty"`
	RunTimeout                   time.Duration      `json:"run_timeout,omitempty"`
	TaskTimeout                  time.Duration      `json:"task_timeout,omitempty"`
	RetryPolicy                  *skald.RetryPolicy `json:"retry_policy,omitempty"`
	WorkflowTaskCompletedEventID int64              `json:"workflow_task_completed_event_id"`
}

func (WorkflowExecutionContinuedAsNewAttributes) Type() EventType {
	return EventTypeWorkflowExecutionContinuedAsNew
}

// WorkflowExecutionCancelRequestedAttributes records the arrival of a cancel
// request. The workflow observes it on its next task and decides what to do.
type WorkflowExecutionCancelRequestedAttributes struct {
	Reason   string `json:"reason,omitempty"`
	Identity string `json:"identity,omitempty"`
}

func (WorkflowExecutionCancelRequestedAttributes) Type() EventType {
	return EventTypeWorkflowExecutionCancelRequested
}

// WorkflowExecutionSignaledAttributes records an asynchronous message. Signals
// are the only way information enters a running workflow from outside, and they
// are durable: a signal accepted by the server will be delivered even if every
// worker is down.
type WorkflowExecutionSignaledAttributes struct {
	SignalName string         `json:"signal_name"`
	Input      *skald.Payload `json:"input,omitempty"`
	Identity   string         `json:"identity,omitempty"`
}

func (WorkflowExecutionSignaledAttributes) Type() EventType {
	return EventTypeWorkflowExecutionSignaled
}

// ---------------------------------------------------------------------------
// Workflow tasks
// ---------------------------------------------------------------------------

// WorkflowTaskScheduledAttributes marks that new history exists for the
// workflow to react to.
type WorkflowTaskScheduledAttributes struct {
	TaskQueue          string        `json:"task_queue"`
	StartToCloseTimout time.Duration `json:"start_to_close_timeout"`
	Attempt            int32         `json:"attempt"`
}

func (WorkflowTaskScheduledAttributes) Type() EventType { return EventTypeWorkflowTaskScheduled }

// WorkflowTaskStartedAttributes records that a worker picked the task up.
type WorkflowTaskStartedAttributes struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Identity         string `json:"identity,omitempty"`
	// RequestID deduplicates a poll that was retried after a network failure.
	RequestID string `json:"request_id,omitempty"`
}

func (WorkflowTaskStartedAttributes) Type() EventType { return EventTypeWorkflowTaskStarted }

// WorkflowTaskCompletedAttributes records that the worker returned commands.
type WorkflowTaskCompletedAttributes struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	StartedEventID   int64  `json:"started_event_id"`
	Identity         string `json:"identity,omitempty"`
	// SDKName and SDKVersion are recorded so that a history can be attributed
	// to the client that produced it during an incident.
	SDKName    string `json:"sdk_name,omitempty"`
	SDKVersion string `json:"sdk_version,omitempty"`
}

func (WorkflowTaskCompletedAttributes) Type() EventType { return EventTypeWorkflowTaskCompleted }

// WorkflowTaskFailedAttributes records a worker-side failure to process the
// task, including the non-determinism detection that guards replay.
type WorkflowTaskFailedAttributes struct {
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	StartedEventID   int64                   `json:"started_event_id"`
	Cause            WorkflowTaskFailedCause `json:"cause"`
	Failure          *skald.ApplicationError `json:"failure,omitempty"`
	Identity         string                  `json:"identity,omitempty"`
}

func (WorkflowTaskFailedAttributes) Type() EventType { return EventTypeWorkflowTaskFailed }

// WorkflowTaskTimedOutAttributes records that a worker took the task and never
// came back.
type WorkflowTaskTimedOutAttributes struct {
	ScheduledEventID int64             `json:"scheduled_event_id"`
	StartedEventID   int64             `json:"started_event_id"`
	Kind             skald.TimeoutKind `json:"kind"`
}

func (WorkflowTaskTimedOutAttributes) Type() EventType { return EventTypeWorkflowTaskTimedOut }

// ---------------------------------------------------------------------------
// Activity tasks
// ---------------------------------------------------------------------------

// ActivityTaskScheduledAttributes records the intent to run an activity. It is
// written before any worker sees the task, so a crash between intent and
// dispatch loses nothing.
type ActivityTaskScheduledAttributes struct {
	ActivityID   string             `json:"activity_id"`
	ActivityType string             `json:"activity_type"`
	TaskQueue    string             `json:"task_queue"`
	Input        *skald.Payload     `json:"input,omitempty"`
	RetryPolicy  *skald.RetryPolicy `json:"retry_policy,omitempty"`

	ScheduleToCloseTimeout time.Duration `json:"schedule_to_close_timeout,omitempty"`
	ScheduleToStartTimeout time.Duration `json:"schedule_to_start_timeout,omitempty"`
	StartToCloseTimeout    time.Duration `json:"start_to_close_timeout,omitempty"`
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout,omitempty"`

	WorkflowTaskCompletedEventID int64 `json:"workflow_task_completed_event_id"`
}

func (ActivityTaskScheduledAttributes) Type() EventType { return EventTypeActivityTaskScheduled }

// ActivityTaskStartedAttributes records a dispatch to a worker.
type ActivityTaskStartedAttributes struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Identity         string `json:"identity,omitempty"`
	RequestID        string `json:"request_id,omitempty"`
	Attempt          int32  `json:"attempt"`
	// LastFailure lets the next attempt see why the previous one failed without
	// scanning the history.
	LastFailure *skald.ApplicationError `json:"last_failure,omitempty"`
	// RetryJitterSeed is the random draw used for the *next* backoff. Recording
	// it here is what makes retry timing replay-stable.
	RetryJitterSeed float64 `json:"retry_jitter_seed,omitempty"`
}

func (ActivityTaskStartedAttributes) Type() EventType { return EventTypeActivityTaskStarted }

// ActivityTaskCompletedAttributes records a successful activity result.
type ActivityTaskCompletedAttributes struct {
	ScheduledEventID int64          `json:"scheduled_event_id"`
	StartedEventID   int64          `json:"started_event_id"`
	Result           *skald.Payload `json:"result,omitempty"`
	Identity         string         `json:"identity,omitempty"`
}

func (ActivityTaskCompletedAttributes) Type() EventType { return EventTypeActivityTaskCompleted }

// ActivityTaskFailedAttributes records a terminal activity failure, i.e. one
// that the retry policy declined to retry.
type ActivityTaskFailedAttributes struct {
	ScheduledEventID int64                   `json:"scheduled_event_id"`
	StartedEventID   int64                   `json:"started_event_id"`
	Failure          *skald.ApplicationError `json:"failure"`
	RetryState       RetryState              `json:"retry_state"`
	Identity         string                  `json:"identity,omitempty"`
}

func (ActivityTaskFailedAttributes) Type() EventType { return EventTypeActivityTaskFailed }

// ActivityTaskTimedOutAttributes records timeout expiry for an activity.
type ActivityTaskTimedOutAttributes struct {
	ScheduledEventID     int64             `json:"scheduled_event_id"`
	StartedEventID       int64             `json:"started_event_id"`
	Kind                 skald.TimeoutKind `json:"kind"`
	RetryState           RetryState        `json:"retry_state"`
	LastHeartbeatDetails *skald.Payload    `json:"last_heartbeat_details,omitempty"`
}

func (ActivityTaskTimedOutAttributes) Type() EventType { return EventTypeActivityTaskTimedOut }

// ActivityTaskCancelRequestedAttributes records that the workflow asked for an
// in-flight activity to stop. Delivery is best effort and observed by the
// activity through its heartbeat.
type ActivityTaskCancelRequestedAttributes struct {
	ScheduledEventID             int64 `json:"scheduled_event_id"`
	WorkflowTaskCompletedEventID int64 `json:"workflow_task_completed_event_id"`
}

func (ActivityTaskCancelRequestedAttributes) Type() EventType {
	return EventTypeActivityTaskCancelRequested
}

// ActivityTaskCanceledAttributes records that the activity acknowledged the
// cancellation.
type ActivityTaskCanceledAttributes struct {
	ScheduledEventID int64          `json:"scheduled_event_id"`
	StartedEventID   int64          `json:"started_event_id"`
	Details          *skald.Payload `json:"details,omitempty"`
	Identity         string         `json:"identity,omitempty"`
}

func (ActivityTaskCanceledAttributes) Type() EventType { return EventTypeActivityTaskCanceled }

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// TimerStartedAttributes records a durable sleep. Durable means the timer
// survives the death of every process in the system: it lives in the store, not
// in a runtime timer wheel.
type TimerStartedAttributes struct {
	TimerID                      string        `json:"timer_id"`
	StartToFireTimeout           time.Duration `json:"start_to_fire_timeout"`
	WorkflowTaskCompletedEventID int64         `json:"workflow_task_completed_event_id"`
}

func (TimerStartedAttributes) Type() EventType { return EventTypeTimerStarted }

// TimerFiredAttributes records expiry.
type TimerFiredAttributes struct {
	TimerID        string `json:"timer_id"`
	StartedEventID int64  `json:"started_event_id"`
}

func (TimerFiredAttributes) Type() EventType { return EventTypeTimerFired }

// TimerCanceledAttributes records that the workflow cancelled a pending timer,
// which happens on every timeout race that the other branch won.
type TimerCanceledAttributes struct {
	TimerID                      string `json:"timer_id"`
	StartedEventID               int64  `json:"started_event_id"`
	WorkflowTaskCompletedEventID int64  `json:"workflow_task_completed_event_id"`
}

func (TimerCanceledAttributes) Type() EventType { return EventTypeTimerCanceled }

// ---------------------------------------------------------------------------
// Markers
// ---------------------------------------------------------------------------

// Marker names understood by the SDK. User code may record its own markers with
// any other name; the engine treats them opaquely.
const (
	// MarkerSideEffect stores the result of a non-deterministic computation
	// performed inside workflow code, so replay reuses the value instead of
	// recomputing it.
	MarkerSideEffect = "SideEffect"
	// MarkerVersion stores the branch chosen by a versioning gate, pinning
	// in-flight executions to the code path they started on.
	MarkerVersion = "Version"
	// MarkerLocalActivity stores the result of an activity executed in the
	// worker process without a server round trip.
	MarkerLocalActivity = "LocalActivity"
)

// MarkerRecordedAttributes stores a value the workflow computed itself.
type MarkerRecordedAttributes struct {
	MarkerName string `json:"marker_name"`
	// MarkerID disambiguates multiple markers of the same name. For side
	// effects it is the monotonically increasing call index.
	MarkerID                     string                  `json:"marker_id,omitempty"`
	Details                      *skald.Payload          `json:"details,omitempty"`
	Failure                      *skald.ApplicationError `json:"failure,omitempty"`
	WorkflowTaskCompletedEventID int64                   `json:"workflow_task_completed_event_id"`
}

func (MarkerRecordedAttributes) Type() EventType { return EventTypeMarkerRecorded }
