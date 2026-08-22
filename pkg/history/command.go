package history

import (
	"fmt"
	"time"

	"github.com/Liona-orph/skald/pkg/skald"
)

// CommandType identifies an instruction produced by workflow code.
//
// Commands are the only channel through which a workflow affects the world.
// They are declarative and idempotent by construction: the worker says "an
// activity named X should run", never "run activity X now". The server decides
// whether that intent is new (append a scheduling event) or already satisfied
// (a replay of an event that exists), which is what makes replay safe.
type CommandType int32

const (
	CommandTypeUnspecified CommandType = 0

	CommandTypeScheduleActivityTask      CommandType = 1
	CommandTypeRequestCancelActivityTask CommandType = 2
	CommandTypeStartTimer                CommandType = 3
	CommandTypeCancelTimer               CommandType = 4
	CommandTypeCompleteWorkflowExecution CommandType = 5
	CommandTypeFailWorkflowExecution     CommandType = 6
	CommandTypeCancelWorkflowExecution   CommandType = 7
	CommandTypeContinueAsNewWorkflow     CommandType = 8
	CommandTypeRecordMarker              CommandType = 9
)

var commandTypeNames = map[CommandType]string{
	CommandTypeUnspecified:               "Unspecified",
	CommandTypeScheduleActivityTask:      "ScheduleActivityTask",
	CommandTypeRequestCancelActivityTask: "RequestCancelActivityTask",
	CommandTypeStartTimer:                "StartTimer",
	CommandTypeCancelTimer:               "CancelTimer",
	CommandTypeCompleteWorkflowExecution: "CompleteWorkflowExecution",
	CommandTypeFailWorkflowExecution:     "FailWorkflowExecution",
	CommandTypeCancelWorkflowExecution:   "CancelWorkflowExecution",
	CommandTypeContinueAsNewWorkflow:     "ContinueAsNewWorkflow",
	CommandTypeRecordMarker:              "RecordMarker",
}

var commandTypesByName = func() map[string]CommandType {
	m := make(map[string]CommandType, len(commandTypeNames))
	for k, v := range commandTypeNames {
		m[v] = k
	}
	return m
}()

func (c CommandType) String() string {
	if n, ok := commandTypeNames[c]; ok {
		return n
	}
	return fmt.Sprintf("CommandType(%d)", int32(c))
}

// Closing reports whether the command ends the execution. At most one closing
// command may appear in a batch and it must be last; the server rejects
// anything else, because a workflow that schedules work after deciding to
// finish has a bug the engine should surface rather than paper over.
func (c CommandType) Closing() bool {
	switch c {
	case CommandTypeCompleteWorkflowExecution,
		CommandTypeFailWorkflowExecution,
		CommandTypeCancelWorkflowExecution,
		CommandTypeContinueAsNewWorkflow:
		return true
	}
	return false
}

// MarshalText implements encoding.TextMarshaler.
func (c CommandType) MarshalText() ([]byte, error) { return []byte(c.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (c *CommandType) UnmarshalText(b []byte) error {
	v, ok := commandTypesByName[string(b)]
	if !ok {
		return fmt.Errorf("history: unknown command type %q", b)
	}
	*c = v
	return nil
}

// Command is one instruction in the batch a worker returns for a workflow task.
//
// Unlike Event, Command uses a flat struct with nullable sub-structs. Commands
// are short-lived, never persisted on their own and always constructed by the
// SDK, so the extra type safety of an interface buys less than the simplicity
// of a shape that is trivial to diff against history during replay.
type Command struct {
	Type CommandType `json:"type"`

	ScheduleActivity      *ScheduleActivityCommand      `json:"schedule_activity,omitempty"`
	RequestCancelActivity *RequestCancelActivityCommand `json:"request_cancel_activity,omitempty"`
	StartTimer            *StartTimerCommand            `json:"start_timer,omitempty"`
	CancelTimer           *CancelTimerCommand           `json:"cancel_timer,omitempty"`
	CompleteWorkflow      *CompleteWorkflowCommand      `json:"complete_workflow,omitempty"`
	FailWorkflow          *FailWorkflowCommand          `json:"fail_workflow,omitempty"`
	CancelWorkflow        *CancelWorkflowCommand        `json:"cancel_workflow,omitempty"`
	ContinueAsNew         *ContinueAsNewCommand         `json:"continue_as_new,omitempty"`
	RecordMarker          *RecordMarkerCommand          `json:"record_marker,omitempty"`
}

// ScheduleActivityCommand asks the server to run an activity.
type ScheduleActivityCommand struct {
	ActivityID   string             `json:"activity_id"`
	ActivityType string             `json:"activity_type"`
	TaskQueue    string             `json:"task_queue,omitempty"`
	Input        *skald.Payload     `json:"input,omitempty"`
	RetryPolicy  *skald.RetryPolicy `json:"retry_policy,omitempty"`

	ScheduleToCloseTimeout time.Duration `json:"schedule_to_close_timeout,omitempty"`
	ScheduleToStartTimeout time.Duration `json:"schedule_to_start_timeout,omitempty"`
	StartToCloseTimeout    time.Duration `json:"start_to_close_timeout,omitempty"`
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout,omitempty"`
}

// RequestCancelActivityCommand asks the server to cancel a scheduled activity.
type RequestCancelActivityCommand struct {
	// ScheduledEventID identifies the activity by the event that created it,
	// which is stable across retries in a way the activity ID is not.
	ScheduledEventID int64 `json:"scheduled_event_id"`
}

// StartTimerCommand asks the server for a durable sleep.
type StartTimerCommand struct {
	TimerID            string        `json:"timer_id"`
	StartToFireTimeout time.Duration `json:"start_to_fire_timeout"`
}

// CancelTimerCommand cancels a pending timer.
type CancelTimerCommand struct {
	StartedEventID int64 `json:"started_event_id"`
}

// CompleteWorkflowCommand ends the execution successfully.
type CompleteWorkflowCommand struct {
	Result *skald.Payload `json:"result,omitempty"`
}

// FailWorkflowCommand ends the execution with a failure.
type FailWorkflowCommand struct {
	Failure *skald.ApplicationError `json:"failure"`
}

// CancelWorkflowCommand acknowledges a cancellation request.
type CancelWorkflowCommand struct {
	Details *skald.Payload `json:"details,omitempty"`
}

// ContinueAsNewCommand closes this run and starts a successor.
type ContinueAsNewCommand struct {
	WorkflowType string             `json:"workflow_type,omitempty"`
	TaskQueue    string             `json:"task_queue,omitempty"`
	Input        *skald.Payload     `json:"input,omitempty"`
	RunTimeout   time.Duration      `json:"run_timeout,omitempty"`
	TaskTimeout  time.Duration      `json:"task_timeout,omitempty"`
	RetryPolicy  *skald.RetryPolicy `json:"retry_policy,omitempty"`
}

// RecordMarkerCommand persists a value the workflow computed locally.
type RecordMarkerCommand struct {
	MarkerName string                  `json:"marker_name"`
	MarkerID   string                  `json:"marker_id,omitempty"`
	Details    *skald.Payload          `json:"details,omitempty"`
	Failure    *skald.ApplicationError `json:"failure,omitempty"`
}

// Validate checks that exactly the sub-struct matching Type is populated and
// that its fields are individually well formed.
//
// The server calls this before touching mutable state. Rejecting a malformed
// batch wholesale keeps the invariant that a workflow task either applies
// completely or not at all.
func (c Command) Validate() error {
	set := 0
	for _, present := range []bool{
		c.ScheduleActivity != nil, c.RequestCancelActivity != nil,
		c.StartTimer != nil, c.CancelTimer != nil,
		c.CompleteWorkflow != nil, c.FailWorkflow != nil,
		c.CancelWorkflow != nil, c.ContinueAsNew != nil, c.RecordMarker != nil,
	} {
		if present {
			set++
		}
	}
	if set != 1 {
		return fmt.Errorf("history: command %s must carry exactly one attribute set, found %d", c.Type, set)
	}

	switch c.Type {
	case CommandTypeScheduleActivityTask:
		a := c.ScheduleActivity
		if a == nil {
			return c.mismatch()
		}
		if a.ActivityID == "" {
			return fmt.Errorf("history: schedule activity command has an empty activity id")
		}
		if err := skald.ValidateTypeName(a.ActivityType); err != nil {
			return fmt.Errorf("history: schedule activity command: %w", err)
		}
		if a.ScheduleToCloseTimeout <= 0 && a.StartToCloseTimeout <= 0 {
			return fmt.Errorf("history: activity %q must set a schedule-to-close or start-to-close timeout; "+
				"an activity with no upper bound can wedge an execution forever", a.ActivityID)
		}
		for name, d := range map[string]time.Duration{
			"schedule_to_close": a.ScheduleToCloseTimeout,
			"schedule_to_start": a.ScheduleToStartTimeout,
			"start_to_close":    a.StartToCloseTimeout,
			"heartbeat":         a.HeartbeatTimeout,
		} {
			if d < 0 {
				return fmt.Errorf("history: activity %q has negative %s timeout", a.ActivityID, name)
			}
		}
		if a.HeartbeatTimeout > 0 && a.StartToCloseTimeout > 0 && a.HeartbeatTimeout > a.StartToCloseTimeout {
			return fmt.Errorf("history: activity %q heartbeat timeout exceeds its start-to-close timeout", a.ActivityID)
		}
		return a.RetryPolicy.Validate()

	case CommandTypeRequestCancelActivityTask:
		if c.RequestCancelActivity == nil {
			return c.mismatch()
		}
		if c.RequestCancelActivity.ScheduledEventID <= 0 {
			return fmt.Errorf("history: cancel activity command names no scheduled event")
		}
	case CommandTypeStartTimer:
		if c.StartTimer == nil {
			return c.mismatch()
		}
		if c.StartTimer.TimerID == "" {
			return fmt.Errorf("history: start timer command has an empty timer id")
		}
		if c.StartTimer.StartToFireTimeout < 0 {
			return fmt.Errorf("history: timer %q has a negative duration", c.StartTimer.TimerID)
		}
	case CommandTypeCancelTimer:
		if c.CancelTimer == nil {
			return c.mismatch()
		}
		if c.CancelTimer.StartedEventID <= 0 {
			return fmt.Errorf("history: cancel timer command names no started event")
		}
	case CommandTypeCompleteWorkflowExecution:
		if c.CompleteWorkflow == nil {
			return c.mismatch()
		}
	case CommandTypeFailWorkflowExecution:
		if c.FailWorkflow == nil {
			return c.mismatch()
		}
		if c.FailWorkflow.Failure == nil {
			return fmt.Errorf("history: fail workflow command carries no failure")
		}
	case CommandTypeCancelWorkflowExecution:
		if c.CancelWorkflow == nil {
			return c.mismatch()
		}
	case CommandTypeContinueAsNewWorkflow:
		if c.ContinueAsNew == nil {
			return c.mismatch()
		}
		return c.ContinueAsNew.RetryPolicy.Validate()
	case CommandTypeRecordMarker:
		if c.RecordMarker == nil {
			return c.mismatch()
		}
		if c.RecordMarker.MarkerName == "" {
			return fmt.Errorf("history: record marker command has no marker name")
		}
	default:
		return fmt.Errorf("history: unknown command type %s", c.Type)
	}
	return nil
}

func (c Command) mismatch() error {
	return fmt.Errorf("history: command declares type %s but carries a different attribute set", c.Type)
}

// ValidateBatch checks the batch-level rules: at most one closing command, and
// nothing after it.
func ValidateBatch(cmds []Command) error {
	for i, cmd := range cmds {
		if err := cmd.Validate(); err != nil {
			return fmt.Errorf("command %d: %w", i, err)
		}
		if cmd.Type.Closing() && i != len(cmds)-1 {
			return fmt.Errorf("history: closing command %s at position %d is followed by %d more commands",
				cmd.Type, i, len(cmds)-i-1)
		}
	}
	return nil
}
