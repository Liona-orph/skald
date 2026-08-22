package execution

import (
	"fmt"
	"time"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Effect describes work the engine must hand to another subsystem after the
// history write commits.
//
// Effects are returned rather than performed so that a transition stays a pure
// function of state. The caller applies them only after the store transaction
// succeeds, which is what prevents the classic "task dispatched, write rolled
// back" duplicate that plagues engines that enqueue inline.
type Effect struct {
	Kind EffectKind
	// TaskQueue and the identifiers below are populated per kind.
	TaskQueue        string
	ScheduledEventID int64
	FireAt           time.Time
	// NewRun is set for EffectStartNewRun and carries the successor's start
	// attributes.
	NewRun *NewRunRequest
}

// EffectKind enumerates the post-commit actions.
type EffectKind int

const (
	// EffectDispatchWorkflowTask makes a workflow task pollable.
	EffectDispatchWorkflowTask EffectKind = iota
	// EffectDispatchActivityTask makes an activity task pollable.
	EffectDispatchActivityTask
	// EffectArmTimer registers a durable timer with the timer service.
	EffectArmTimer
	// EffectStartNewRun creates the successor of a continue-as-new or a retry.
	EffectStartNewRun
)

// NewRunRequest carries everything needed to start a successor run.
type NewRunRequest struct {
	WorkflowID string
	Attributes history.WorkflowExecutionStartedAttributes
	// Delay defers the start, used by workflow-level retry backoff and cron.
	Delay time.Duration
}

// Start writes event 1. It is separate from AppendEvent so that the caller
// cannot accidentally start an execution twice.
func (ms *MutableState) Start(attrs history.WorkflowExecutionStartedAttributes) (history.Event, error) {
	if len(ms.events) != 0 {
		return history.Event{}, transitionErr("execution %s already started", ms.Execution)
	}
	if attrs.Attempt == 0 {
		attrs.Attempt = 1
	}
	if attrs.FirstExecutionRunID == "" {
		attrs.FirstExecutionRunID = ms.Execution.RunID
	}
	return ms.AppendEvent(attrs)
}

// ScheduleWorkflowTask appends a WorkflowTaskScheduled event unless one is
// already outstanding.
//
// The idempotence here is load-bearing: signals, activity completions and timer
// fires all want "make sure the workflow gets a turn", and they arrive
// concurrently. Collapsing them into a single pending task is what stops a
// burst of ten signals from producing ten replays of the same workflow.
func (ms *MutableState) ScheduleWorkflowTask() (*Effect, error) {
	if ms.Status.Terminal() {
		return nil, nil
	}
	if ms.workflowTask.ScheduledEventID != 0 {
		return nil, nil
	}
	timeout := ms.DefaultTaskTimeout
	if timeout <= 0 {
		timeout = DefaultWorkflowTaskTimeout
	}
	ev, err := ms.AppendEvent(history.WorkflowTaskScheduledAttributes{
		TaskQueue:          ms.TaskQueue,
		StartToCloseTimout: timeout,
		Attempt:            1,
	})
	if err != nil {
		return nil, err
	}
	return &Effect{Kind: EffectDispatchWorkflowTask, TaskQueue: ms.TaskQueue, ScheduledEventID: ev.ID}, nil
}

// DefaultWorkflowTaskTimeout bounds how long a worker may hold a workflow task.
// It is short because workflow code is supposed to be a pure, fast decision
// function; anything slow belongs in an activity.
const DefaultWorkflowTaskTimeout = 10 * time.Second

// StartWorkflowTask transitions the pending task to started and returns the
// history the worker needs.
func (ms *MutableState) StartWorkflowTask(identity, requestID string) (history.Event, error) {
	if !ms.workflowTask.Scheduled() {
		return history.Event{}, transitionErr("no workflow task available to start")
	}
	return ms.AppendEvent(history.WorkflowTaskStartedAttributes{
		ScheduledEventID: ms.workflowTask.ScheduledEventID,
		Identity:         identity,
		RequestID:        requestID,
	})
}

// CompleteWorkflowTask applies a batch of commands produced by the worker.
//
// The whole batch is applied or none of it is: a command that fails validation
// aborts before any event is written. Partial application would leave the
// workflow's in-memory view (which already ran to the end of the batch)
// disagreeing with the history, and that divergence is unrecoverable.
func (ms *MutableState) CompleteWorkflowTask(identity, sdkName, sdkVersion string, cmds []history.Command) ([]Effect, error) {
	if !ms.workflowTask.Started() {
		return nil, transitionErr("no workflow task is in flight")
	}
	if err := history.ValidateBatch(cmds); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateTransition, err)
	}
	if err := ms.precheckCommands(cmds); err != nil {
		return nil, err
	}

	completed, err := ms.AppendEvent(history.WorkflowTaskCompletedAttributes{
		ScheduledEventID: ms.workflowTask.ScheduledEventID,
		StartedEventID:   ms.workflowTask.StartedEventID,
		Identity:         identity,
		SDKName:          sdkName,
		SDKVersion:       sdkVersion,
	})
	if err != nil {
		return nil, err
	}

	var effects []Effect
	for i, cmd := range cmds {
		eff, err := ms.applyCommand(cmd, completed.ID)
		if err != nil {
			return nil, fmt.Errorf("command %d (%s): %w", i, cmd.Type, err)
		}
		effects = append(effects, eff...)
	}

	// If the workflow neither closed nor is waiting on anything, it made no
	// progress and would stall. That is a bug in user code, but the engine can
	// detect it cheaply and surface it instead of hanging forever.
	if !ms.Status.Terminal() && !ms.HasPendingWork() && len(cmds) == 0 {
		return effects, nil
	}
	return effects, nil
}

// precheckCommands validates the batch against current state before any event
// is written, so that CompleteWorkflowTask is all-or-nothing.
func (ms *MutableState) precheckCommands(cmds []history.Command) error {
	seenActivityIDs := make(map[string]struct{})
	seenTimerIDs := make(map[string]struct{})
	for i, cmd := range cmds {
		switch cmd.Type {
		case history.CommandTypeScheduleActivityTask:
			id := cmd.ScheduleActivity.ActivityID
			if _, dup := ms.activityIDs[id]; dup {
				return transitionErr("command %d schedules activity id %q, which is already pending", i, id)
			}
			if _, dup := seenActivityIDs[id]; dup {
				return transitionErr("command %d reuses activity id %q within the same batch", i, id)
			}
			seenActivityIDs[id] = struct{}{}
		case history.CommandTypeStartTimer:
			id := cmd.StartTimer.TimerID
			if _, dup := ms.timerIDs[id]; dup {
				return transitionErr("command %d starts timer id %q, which is already pending", i, id)
			}
			if _, dup := seenTimerIDs[id]; dup {
				return transitionErr("command %d reuses timer id %q within the same batch", i, id)
			}
			seenTimerIDs[id] = struct{}{}
		case history.CommandTypeCancelTimer:
			if _, ok := ms.timers[cmd.CancelTimer.StartedEventID]; !ok {
				return transitionErr("command %d cancels timer started by event %d, which is not pending",
					i, cmd.CancelTimer.StartedEventID)
			}
		case history.CommandTypeRequestCancelActivityTask:
			if _, ok := ms.activities[cmd.RequestCancelActivity.ScheduledEventID]; !ok {
				return transitionErr("command %d cancels activity scheduled by event %d, which is not pending",
					i, cmd.RequestCancelActivity.ScheduledEventID)
			}
		}
	}
	return nil
}

func (ms *MutableState) applyCommand(cmd history.Command, taskCompletedID int64) ([]Effect, error) {
	switch cmd.Type {
	case history.CommandTypeScheduleActivityTask:
		c := cmd.ScheduleActivity
		queue := c.TaskQueue
		if queue == "" {
			queue = ms.TaskQueue
		}
		policy := c.RetryPolicy.Clone()
		if policy == nil {
			policy = skald.DefaultRetryPolicy()
		}
		ev, err := ms.AppendEvent(history.ActivityTaskScheduledAttributes{
			ActivityID:                   c.ActivityID,
			ActivityType:                 c.ActivityType,
			TaskQueue:                    queue,
			Input:                        c.Input,
			RetryPolicy:                  policy,
			ScheduleToCloseTimeout:       c.ScheduleToCloseTimeout,
			ScheduleToStartTimeout:       c.ScheduleToStartTimeout,
			StartToCloseTimeout:          c.StartToCloseTimeout,
			HeartbeatTimeout:             c.HeartbeatTimeout,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		if err != nil {
			return nil, err
		}
		return []Effect{{Kind: EffectDispatchActivityTask, TaskQueue: queue, ScheduledEventID: ev.ID}}, nil

	case history.CommandTypeRequestCancelActivityTask:
		_, err := ms.AppendEvent(history.ActivityTaskCancelRequestedAttributes{
			ScheduledEventID:             cmd.RequestCancelActivity.ScheduledEventID,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		return nil, err

	case history.CommandTypeStartTimer:
		ev, err := ms.AppendEvent(history.TimerStartedAttributes{
			TimerID:                      cmd.StartTimer.TimerID,
			StartToFireTimeout:           cmd.StartTimer.StartToFireTimeout,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		if err != nil {
			return nil, err
		}
		t := ms.timers[ev.ID]
		return []Effect{{Kind: EffectArmTimer, ScheduledEventID: ev.ID, FireAt: t.FireAt}}, nil

	case history.CommandTypeCancelTimer:
		t := ms.timers[cmd.CancelTimer.StartedEventID]
		_, err := ms.AppendEvent(history.TimerCanceledAttributes{
			TimerID:                      t.TimerID,
			StartedEventID:               cmd.CancelTimer.StartedEventID,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		return nil, err

	case history.CommandTypeRecordMarker:
		c := cmd.RecordMarker
		_, err := ms.AppendEvent(history.MarkerRecordedAttributes{
			MarkerName:                   c.MarkerName,
			MarkerID:                     c.MarkerID,
			Details:                      c.Details,
			Failure:                      c.Failure,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		return nil, err

	case history.CommandTypeCompleteWorkflowExecution:
		_, err := ms.AppendEvent(history.WorkflowExecutionCompletedAttributes{
			Result:                       cmd.CompleteWorkflow.Result,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		return nil, err

	case history.CommandTypeFailWorkflowExecution:
		return ms.failExecution(cmd.FailWorkflow.Failure, taskCompletedID)

	case history.CommandTypeCancelWorkflowExecution:
		_, err := ms.AppendEvent(history.WorkflowExecutionCanceledAttributes{
			Details:                      cmd.CancelWorkflow.Details,
			WorkflowTaskCompletedEventID: taskCompletedID,
		})
		return nil, err

	case history.CommandTypeContinueAsNewWorkflow:
		return ms.continueAsNew(cmd.ContinueAsNew, taskCompletedID)
	}
	return nil, transitionErr("unhandled command type %s", cmd.Type)
}

// failExecution consults the workflow-level retry policy before closing.
func (ms *MutableState) failExecution(failure *skald.ApplicationError, taskCompletedID int64) ([]Effect, error) {
	retryState := history.RetryStateRetryPolicyNotSet
	var effects []Effect

	if ms.RetryPolicy != nil {
		seed := jitterFromSeed(ms.RandomnessSeed, int64(ms.Attempt))
		delay, ok := ms.RetryPolicy.ShouldRetry(ms.Attempt, failure, seed)
		switch {
		case ok && ms.ExecutionDeadline.IsZero(),
			ok && ms.now().Add(delay).Before(ms.ExecutionDeadline):
			retryState = history.RetryStateInProgress
			effects = append(effects, Effect{
				Kind: EffectStartNewRun,
				NewRun: &NewRunRequest{
					WorkflowID: ms.Execution.WorkflowID,
					Delay:      delay,
					Attributes: ms.successorAttributes(nil, ms.Attempt+1),
				},
			})
		case ok:
			retryState = history.RetryStateTimeout
		case ms.RetryPolicy.MaximumAttempts > 0 && ms.Attempt >= ms.RetryPolicy.MaximumAttempts:
			retryState = history.RetryStateMaximumAttemptsReached
		default:
			retryState = history.RetryStateNonRetryableFailure
		}
	}

	if _, err := ms.AppendEvent(history.WorkflowExecutionFailedAttributes{
		Failure:                      failure,
		WorkflowTaskCompletedEventID: taskCompletedID,
		RetryState:                   retryState,
	}); err != nil {
		return nil, err
	}
	return effects, nil
}

func (ms *MutableState) continueAsNew(c *history.ContinueAsNewCommand, taskCompletedID int64) ([]Effect, error) {
	wfType := c.WorkflowType
	if wfType == "" {
		wfType = ms.WorkflowType
	}
	queue := c.TaskQueue
	if queue == "" {
		queue = ms.TaskQueue
	}
	attrs := ms.successorAttributes(c.Input, 1)
	attrs.WorkflowType = wfType
	attrs.TaskQueue = queue
	attrs.RunTimeout = c.RunTimeout
	attrs.TaskTimeout = c.TaskTimeout
	if c.RetryPolicy != nil {
		attrs.RetryPolicy = c.RetryPolicy.Clone()
	}

	if _, err := ms.AppendEvent(history.WorkflowExecutionContinuedAsNewAttributes{
		// The successor's run ID is assigned by the caller, which owns ID
		// generation; it is patched into the event before the write commits.
		NewRunID:                     pendingRunIDPlaceholder,
		WorkflowType:                 wfType,
		TaskQueue:                    queue,
		Input:                        c.Input,
		RunTimeout:                   c.RunTimeout,
		TaskTimeout:                  c.TaskTimeout,
		RetryPolicy:                  attrs.RetryPolicy,
		WorkflowTaskCompletedEventID: taskCompletedID,
	}); err != nil {
		return nil, err
	}
	return []Effect{{
		Kind:   EffectStartNewRun,
		NewRun: &NewRunRequest{WorkflowID: ms.Execution.WorkflowID, Attributes: attrs},
	}}, nil
}

// pendingRunIDPlaceholder marks a run ID the caller must fill in. Using a
// recognisable sentinel rather than an empty string means a bug that forgets to
// patch it fails history validation loudly instead of writing a blank field.
const pendingRunIDPlaceholder = "<pending>"

// PatchContinuedAsNewRunID fills in the successor run ID once the caller has
// generated it.
func (ms *MutableState) PatchContinuedAsNewRunID(runID string) error {
	if len(ms.events) == 0 {
		return transitionErr("no events to patch")
	}
	last := &ms.events[len(ms.events)-1]
	attrs, ok := history.AttributesAs[history.WorkflowExecutionContinuedAsNewAttributes](*last)
	if !ok {
		return transitionErr("last event is %s, not %s", last.Type(), history.EventTypeWorkflowExecutionContinuedAsNew)
	}
	attrs.NewRunID = runID
	last.Attrs = attrs
	return nil
}

func (ms *MutableState) successorAttributes(input *skald.Payload, attempt int32) history.WorkflowExecutionStartedAttributes {
	started, _ := ms.events.StartedAttributes()
	if input == nil {
		input = started.Input
	}
	return history.WorkflowExecutionStartedAttributes{
		WorkflowType:            ms.WorkflowType,
		TaskQueue:               ms.TaskQueue,
		Input:                   input,
		RunTimeout:              started.RunTimeout,
		ExecutionTimeout:        started.ExecutionTimeout,
		TaskTimeout:             started.TaskTimeout,
		RetryPolicy:             ms.RetryPolicy.Clone(),
		Attempt:                 attempt,
		ContinuedExecutionRunID: ms.Execution.RunID,
		FirstExecutionRunID:     ms.FirstExecutionRunID,
		RandomnessSeed:          nextSeed(ms.RandomnessSeed),
		Memo:                    ms.Memo,
		SearchAttrs:             ms.SearchAttrs,
		CronSchedule:            ms.CronSchedule,
	}
}
