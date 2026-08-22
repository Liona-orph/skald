package engine

import (
	"context"
	"time"

	"github.com/Liona-orph/skald/internal/execution"
	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/skald"
)

// ---------------------------------------------------------------------------
// How an activity retry is represented
//
// A retry writes no history event. That is the whole point of activity retries:
// an activity retried a thousand times contributes two events to the history,
// not two thousand, so a flaky dependency cannot blow past the history limit or
// make replay expensive. The state machine implements this by leaving the
// activity pending, bumping its attempt counter and returning a timer effect
// (see execution.retryActivity).
//
// Two consequences fall out of that, and both are handled here.
//
// First, an activity has exactly *one* ActivityTaskStarted event, written for
// its first attempt. A second one would be unreplayable -- replaying it finds
// the activity already started and rejects the history -- so later attempts
// reuse the original started event and the engine patches the attempt's clocks
// in memory instead. The event's Attempt field therefore reads 1 forever; the
// live attempt number travels with the dispatch reference and with the durable
// timer record.
//
// Second, the attempt counter between attempts exists nowhere in the history,
// so it is carried by the ActivityRetry entry in the timer index, whose Attempt
// field exists for exactly this purpose. That entry is the durable statement
// "attempt N is due at T and has not started". When it fires, the engine trusts
// it over the rebuilt state: a retry entry for attempt N is proof that the
// started event in history is stale, which is what lets a cold replica pick up
// a retry correctly.
// ---------------------------------------------------------------------------

// PollActivityTask implements api.Service.
func (e *Engine) PollActivityTask(ctx context.Context, req api.PollActivityTaskRequest) (api.ActivityTask, error) {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return api.ActivityTask{}, err
	}
	if err := skald.ValidateTaskQueue(req.TaskQueue); err != nil {
		return api.ActivityTask{}, mapError(err)
	}

	for {
		ref, ok, err := e.matcher.PollActivityTask(ctx, namespace, req.TaskQueue)
		if err != nil {
			return api.ActivityTask{}, mapError(err)
		}
		if !ok {
			return api.ActivityTask{Empty: true}, nil
		}
		task, taken, err := e.startActivityTask(ctx, ref, req)
		if err != nil {
			if apiCode(err) == api.CodeNotFound {
				continue
			}
			return api.ActivityTask{}, err
		}
		if taken {
			return task, nil
		}
		// A stale reference: the activity finished, was cancelled or was
		// dispatched twice. Keep waiting instead of returning an empty task the
		// worker would immediately re-poll on.
	}
}

// startActivityTask converts a matching reference into a started attempt.
func (e *Engine) startActivityTask(ctx context.Context, ref matching.Task, req api.PollActivityTaskRequest) (api.ActivityTask, bool, error) {
	var task api.ActivityTask
	requestID := req.RequestID
	if requestID == "" {
		requestID = e.newID()
	}

	err := e.mutate(ctx, ref.Namespace, ref.Execution.WorkflowID, ref.Execution.RunID, func(st *cachedState) (outcome, error) {
		task = api.ActivityTask{}
		ms := st.ms
		if ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		act, ok := ms.Activity(ref.ScheduledEventID)
		if !ok {
			return outcome{noop: true}, nil
		}

		now := e.clk.Now()
		switch {
		case !act.Started():
			// First attempt: this is the one dispatch that writes an event.
			if _, err := ms.StartActivity(ref.ScheduledEventID, req.Identity, requestID); err != nil {
				return outcome{}, err
			}
		case ref.Attempt > act.Attempt, ref.Attempt == act.Attempt && act.RequestID == "":
			// A retried attempt. No event is written; the attempt's clocks are
			// patched onto the state the same way the started event would have.
			e.beginRetryAttempt(act, ref.Attempt, requestID, now)
		default:
			// Some other poller already holds this attempt.
			return outcome{noop: true}, nil
		}

		task = api.ActivityTask{
			Namespace:        ms.Namespace,
			Execution:        ms.Execution,
			ActivityID:       act.ActivityID,
			ActivityType:     act.ActivityType,
			ScheduledEventID: ref.ScheduledEventID,
			StartedEventID:   act.StartedEventID,
			Attempt:          act.Attempt,
			Input:            act.Attributes.Input,
			ScheduledAt:      act.ScheduledAt,
			StartedAt:        act.StartedAt,
			// The deadline lets the worker cancel its own context when the
			// server has already given up, so a doomed attempt stops burning
			// resources instead of racing a timeout it cannot win.
			Deadline:             earliest(act.AttemptDeadline, act.ScheduleToCloseDeadline),
			HeartbeatTimeout:     act.HeartbeatTimeout,
			LastHeartbeatDetails: act.LastHeartbeatDetails,
			WorkflowType:         ms.WorkflowType,
		}

		// The attempt is now in a worker's hands, so the retry entry that was
		// holding its place is no longer needed.
		return outcome{drop: []persistence.TimerKey{activityRetryKey(ms, ref.ScheduledEventID)}}, nil
	})
	if err != nil {
		return api.ActivityTask{}, false, err
	}
	return task, task.ScheduledEventID != 0, nil
}

// beginRetryAttempt patches the clocks of an attempt that is starting without a
// history event of its own.
func (e *Engine) beginRetryAttempt(act *execution.ActivityState, attempt int32, requestID string, now time.Time) {
	if attempt > act.Attempt {
		act.Attempt = attempt
	}
	act.RequestID = requestID
	act.StartedAt = now
	act.AttemptDeadline = time.Time{}
	if d := act.Attributes.StartToCloseTimeout; d > 0 {
		act.AttemptDeadline = now.Add(d)
	}
	act.HeartbeatDeadline = time.Time{}
	if act.HeartbeatTimeout > 0 {
		act.HeartbeatDeadline = now.Add(act.HeartbeatTimeout)
	}
}

// RespondActivityTaskCompleted implements api.Service.
func (e *Engine) RespondActivityTaskCompleted(ctx context.Context, req api.RespondActivityTaskCompletedRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		if err := e.checkActivityOwnership(st, req.ScheduledEventID); err != nil {
			return outcome{}, err
		}
		effects, err := st.ms.CompleteActivity(req.ScheduledEventID, req.Result, req.Identity)
		return outcome{effects: effects}, err
	})
}

// RespondActivityTaskFailed implements api.Service.
func (e *Engine) RespondActivityTaskFailed(ctx context.Context, req api.RespondActivityTaskFailedRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	if req.Failure == nil {
		return errorf(api.CodeInvalidArgument, "engine: a failure response must carry a failure")
	}
	return e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		if err := e.checkActivityOwnership(st, req.ScheduledEventID); err != nil {
			return outcome{}, err
		}
		act, _ := st.ms.Activity(req.ScheduledEventID)
		startedEventID := act.StartedEventID

		effects, err := st.ms.FailActivity(req.ScheduledEventID, req.Failure, req.Identity)
		if err != nil {
			return outcome{}, err
		}
		restoreStartedEvent(st.ms, req.ScheduledEventID, startedEventID)
		return outcome{effects: effects}, nil
	})
}

// RespondActivityTaskCanceled implements api.Service.
func (e *Engine) RespondActivityTaskCanceled(ctx context.Context, req api.RespondActivityTaskCanceledRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		if _, ok := st.ms.Activity(req.ScheduledEventID); !ok {
			return outcome{}, errorf(api.CodeNotFound,
				"engine: activity scheduled by event %d is not pending", req.ScheduledEventID)
		}
		effects, err := st.ms.CancelActivity(req.ScheduledEventID, req.Details, req.Identity)
		return outcome{effects: effects}, err
	})
}

// RecordActivityHeartbeat implements api.Service.
//
// A heartbeat writes no history event but does write to the store, because the
// deadline it extends is durable: an engine that kept heartbeats in memory would
// time out every long activity the moment a replica restarted. The write is
// timer-index-only, so it costs one row update and no history growth. Workers
// are expected to throttle heartbeats client side; the engine deliberately does
// not silently drop them, because a dropped heartbeat looks exactly like a dead
// worker.
func (e *Engine) RecordActivityHeartbeat(ctx context.Context, req api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error) {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return api.RecordActivityHeartbeatResponse{}, err
	}
	var resp api.RecordActivityHeartbeatResponse
	err = e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		resp = api.RecordActivityHeartbeatResponse{}
		if err := e.checkActivityOwnership(st, req.ScheduledEventID); err != nil {
			return outcome{}, err
		}
		cancelRequested, err := st.ms.RecordActivityHeartbeat(req.ScheduledEventID, req.Details)
		if err != nil {
			return outcome{}, err
		}
		resp.CancelRequested = cancelRequested
		return outcome{}, nil
	})
	if err != nil {
		return api.RecordActivityHeartbeatResponse{}, err
	}
	return resp, nil
}

// checkActivityOwnership rejects a response for an attempt no worker holds.
func (e *Engine) checkActivityOwnership(st *cachedState, scheduledEventID int64) error {
	act, ok := st.ms.Activity(scheduledEventID)
	if !ok {
		return errorf(api.CodeNotFound,
			"engine: activity scheduled by event %d is not pending on %s", scheduledEventID, st.ms.Execution)
	}
	if act.RequestID == "" {
		// The attempt was timed out or retried out from under this worker.
		return errorf(api.CodeFailedPrecondition,
			"engine: activity %q attempt %d is not currently held by a worker", act.ActivityID, act.Attempt)
	}
	return nil
}

// restoreStartedEvent re-attaches an activity's original started event after a
// retry transition cleared it.
//
// execution.retryActivity zeroes StartedEventID to express "no attempt is
// running". The engine cannot let that reach the store's view of the world,
// because a completion for the next attempt must still reference a started
// event, and the history has only the first one. Re-attaching it here keeps the
// state consistent with what a rebuild from history would produce, while
// RequestID -- which the retry cleared and this does not restore -- carries the
// "no worker holds it" fact instead.
func restoreStartedEvent(ms *execution.MutableState, scheduledEventID, startedEventID int64) {
	act, ok := ms.Activity(scheduledEventID)
	if !ok || startedEventID == 0 {
		return
	}
	if act.StartedEventID == 0 {
		act.StartedEventID = startedEventID
	}
}

func activityRetryKey(ms *execution.MutableState, scheduledEventID int64) persistence.TimerKey {
	return persistence.TimerKey{
		Namespace:  ms.Namespace,
		WorkflowID: ms.Execution.WorkflowID,
		RunID:      ms.Execution.RunID,
		EventID:    scheduledEventID,
		Kind:       persistence.TimerKindActivityRetry,
	}
}
