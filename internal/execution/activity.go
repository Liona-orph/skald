package execution

import (
	"time"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// StartActivity marks a pending activity as taken by a worker.
func (ms *MutableState) StartActivity(scheduledEventID int64, identity, requestID string) (history.Event, error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return history.Event{}, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	if act.Started() {
		// A worker that retried its poll after a timeout can legitimately ask
		// twice. Returning the existing state rather than an error keeps the
		// dispatch path idempotent.
		if ev, ok := ms.events.Get(act.StartedEventID); ok && act.RequestID == requestID {
			return ev, nil
		}
		return history.Event{}, transitionErr("activity %q is already started", act.ActivityID)
	}
	var lastFailure *skald.ApplicationError
	if act.Attempt > 1 {
		lastFailure = act.lastFailure
	}
	return ms.AppendEvent(history.ActivityTaskStartedAttributes{
		ScheduledEventID: scheduledEventID,
		Identity:         identity,
		RequestID:        requestID,
		Attempt:          act.Attempt,
		LastFailure:      lastFailure,
		RetryJitterSeed:  jitterFromSeed(ms.RandomnessSeed, scheduledEventID*1_000_003+int64(act.Attempt)),
	})
}

// CompleteActivity records a successful activity result and wakes the workflow.
func (ms *MutableState) CompleteActivity(scheduledEventID int64, result *skald.Payload, identity string) ([]Effect, error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return nil, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	if !act.Started() {
		return nil, transitionErr("activity %q completed without ever starting", act.ActivityID)
	}
	if _, err := ms.AppendEvent(history.ActivityTaskCompletedAttributes{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   act.StartedEventID,
		Result:           result,
		Identity:         identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// FailActivity applies the retry policy.
//
// Retry is implemented by *not* writing a terminal event: the activity stays
// pending, its attempt counter goes up and a fresh dispatch is scheduled after
// the backoff. The workflow sees nothing, which is exactly the point -- a
// workflow author writes the happy path and gets retries for free.
func (ms *MutableState) FailActivity(scheduledEventID int64, failure *skald.ApplicationError, identity string) ([]Effect, error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return nil, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	if !act.Started() {
		return nil, transitionErr("activity %q failed without ever starting", act.ActivityID)
	}

	startedEventID := act.StartedEventID
	jitter := ms.startedJitter(startedEventID)
	delay, retry := act.RetryPolicy.ShouldRetry(act.Attempt, failure, jitter)

	if retry {
		if deadline := act.ScheduleToCloseDeadline; !deadline.IsZero() && ms.now().Add(delay).After(deadline) {
			retry = false
		}
	}

	if retry {
		return ms.retryActivity(act, failure, delay)
	}

	state := history.RetryStateNonRetryableFailure
	if act.RetryPolicy != nil && act.RetryPolicy.MaximumAttempts > 0 && act.Attempt >= act.RetryPolicy.MaximumAttempts {
		state = history.RetryStateMaximumAttemptsReached
	}
	if _, err := ms.AppendEvent(history.ActivityTaskFailedAttributes{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Failure:          failure,
		RetryState:       state,
		Identity:         identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// retryActivity resets the activity for another attempt without touching the
// workflow. The backoff is realised as an internal timer effect rather than a
// history event, keeping retry storms out of the history entirely: an activity
// retried a thousand times still contributes two events.
func (ms *MutableState) retryActivity(act *ActivityState, failure *skald.ApplicationError, delay time.Duration) ([]Effect, error) {
	act.Attempt++
	act.StartedEventID = 0
	act.StartedAt = time.Time{}
	act.AttemptDeadline = time.Time{}
	act.HeartbeatDeadline = time.Time{}
	act.RequestID = ""
	act.lastFailure = failure
	return []Effect{{
		Kind:             EffectArmTimer,
		ScheduledEventID: act.ScheduledEventID,
		FireAt:           ms.now().Add(delay),
		TaskQueue:        act.TaskQueue,
	}}, nil
}

// TimeoutActivity records timeout expiry, retrying when the policy allows.
func (ms *MutableState) TimeoutActivity(scheduledEventID int64, kind skald.TimeoutKind) ([]Effect, error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return nil, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	timeoutErr := &skald.TimeoutError{Kind: kind, LastHeartbeatDetails: act.LastHeartbeatDetails}
	failure := &skald.ApplicationError{
		Type:    "TimeoutError:" + kind.String(),
		Message: timeoutErr.Error(),
		// A schedule-to-close expiry is the outer budget: retrying it can never
		// succeed, so it is marked non-retryable regardless of policy.
		NonRetryable: kind == skald.TimeoutScheduleToClose,
	}

	jitter := ms.startedJitter(act.StartedEventID)
	delay, retry := act.RetryPolicy.ShouldRetry(act.Attempt, failure, jitter)
	if retry {
		if deadline := act.ScheduleToCloseDeadline; !deadline.IsZero() && ms.now().Add(delay).After(deadline) {
			retry = false
		}
	}
	if retry {
		return ms.retryActivity(act, failure, delay)
	}

	if _, err := ms.AppendEvent(history.ActivityTaskTimedOutAttributes{
		ScheduledEventID:     scheduledEventID,
		StartedEventID:       act.StartedEventID,
		Kind:                 kind,
		RetryState:           history.RetryStateTimeout,
		LastHeartbeatDetails: act.LastHeartbeatDetails,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// CancelActivity records that a worker acknowledged a cancellation request.
func (ms *MutableState) CancelActivity(scheduledEventID int64, details *skald.Payload, identity string) ([]Effect, error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return nil, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	if _, err := ms.AppendEvent(history.ActivityTaskCanceledAttributes{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   act.StartedEventID,
		Details:          details,
		Identity:         identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// RecordActivityHeartbeat extends the heartbeat deadline and stores the
// checkpoint. It writes no history event: heartbeats are high frequency and
// carry no information the workflow needs, so persisting them would multiply
// history size for nothing. The checkpoint survives in mutable state and is
// handed to the next attempt.
func (ms *MutableState) RecordActivityHeartbeat(scheduledEventID int64, details *skald.Payload) (cancelRequested bool, err error) {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return false, transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	if !act.Started() {
		return false, transitionErr("activity %q heartbeat before start", act.ActivityID)
	}
	act.LastHeartbeatDetails = details
	if act.HeartbeatTimeout > 0 {
		act.HeartbeatDeadline = ms.now().Add(act.HeartbeatTimeout)
	}
	return act.CancelRequested, nil
}

// FireTimer records expiry of a durable timer and wakes the workflow.
func (ms *MutableState) FireTimer(startedEventID int64) ([]Effect, error) {
	t, ok := ms.timers[startedEventID]
	if !ok {
		// The timer was cancelled between the scan and this call. Firing a
		// cancelled timer must be a silent no-op, not an error: the race is
		// normal and the workflow already moved on.
		return nil, nil
	}
	if _, err := ms.AppendEvent(history.TimerFiredAttributes{
		TimerID:        t.TimerID,
		StartedEventID: startedEventID,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// Signal delivers an asynchronous message and wakes the workflow.
func (ms *MutableState) Signal(name string, input *skald.Payload, identity string) ([]Effect, error) {
	if _, err := ms.AppendEvent(history.WorkflowExecutionSignaledAttributes{
		SignalName: name,
		Input:      input,
		Identity:   identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// RequestCancel records a cancellation request and wakes the workflow so it can
// unwind on its own terms.
func (ms *MutableState) RequestCancel(reason, identity string) ([]Effect, error) {
	if ms.CancelRequested {
		return nil, nil
	}
	if _, err := ms.AppendEvent(history.WorkflowExecutionCancelRequestedAttributes{
		Reason:   reason,
		Identity: identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// Terminate closes the execution immediately without running workflow code.
func (ms *MutableState) Terminate(reason string, details *skald.Payload, identity string) error {
	_, err := ms.AppendEvent(history.WorkflowExecutionTerminatedAttributes{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	})
	return err
}

// TimeoutExecution closes the execution because a run or execution deadline
// passed.
func (ms *MutableState) TimeoutExecution(kind skald.TimeoutKind) error {
	_, err := ms.AppendEvent(history.WorkflowExecutionTimedOutAttributes{
		Kind:       kind,
		RetryState: history.RetryStateTimeout,
	})
	return err
}

// FailWorkflowTask records that a worker could not process the task. The task
// is rescheduled so that rolling back a bad deploy is sufficient to recover.
func (ms *MutableState) FailWorkflowTask(cause history.WorkflowTaskFailedCause, failure *skald.ApplicationError, identity string) ([]Effect, error) {
	if !ms.workflowTask.Started() {
		return nil, transitionErr("no workflow task is in flight to fail")
	}
	if _, err := ms.AppendEvent(history.WorkflowTaskFailedAttributes{
		ScheduledEventID: ms.workflowTask.ScheduledEventID,
		StartedEventID:   ms.workflowTask.StartedEventID,
		Cause:            cause,
		Failure:          failure,
		Identity:         identity,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// TimeoutWorkflowTask records that a worker took a task and vanished.
func (ms *MutableState) TimeoutWorkflowTask() ([]Effect, error) {
	if !ms.workflowTask.Started() {
		return nil, nil
	}
	if _, err := ms.AppendEvent(history.WorkflowTaskTimedOutAttributes{
		ScheduledEventID: ms.workflowTask.ScheduledEventID,
		StartedEventID:   ms.workflowTask.StartedEventID,
		Kind:             skald.TimeoutStartToClose,
	}); err != nil {
		return nil, err
	}
	return ms.wakeWorkflow()
}

// wakeWorkflow schedules a workflow task if the execution is still open.
func (ms *MutableState) wakeWorkflow() ([]Effect, error) {
	if ms.Status.Terminal() {
		return nil, nil
	}
	eff, err := ms.ScheduleWorkflowTask()
	if err != nil {
		return nil, err
	}
	if eff == nil {
		return nil, nil
	}
	return []Effect{*eff}, nil
}

// startedJitter recovers the jitter draw recorded on the activity's started
// event, so that a retry delay computed now matches one computed during replay.
func (ms *MutableState) startedJitter(startedEventID int64) float64 {
	ev, ok := ms.events.Get(startedEventID)
	if !ok {
		return 0
	}
	attrs, ok := history.AttributesAs[history.ActivityTaskStartedAttributes](ev)
	if !ok {
		return 0
	}
	return attrs.RetryJitterSeed
}
