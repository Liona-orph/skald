package execution

import (
	"errors"
	"fmt"
	"time"

	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// ErrStateTransition is the sentinel for every illegal state advance.
var ErrStateTransition = errors.New("execution: illegal state transition")

func transitionErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrStateTransition, fmt.Sprintf(format, args...))
}

// ActivityState is the engine-side record of one scheduled activity.
type ActivityState struct {
	ScheduledEventID int64
	StartedEventID   int64 // 0 until a worker takes it
	ActivityID       string
	ActivityType     string
	TaskQueue        string
	Attempt          int32
	ScheduledAt      time.Time
	StartedAt        time.Time
	// AttemptDeadline is the start-to-close deadline of the current attempt.
	AttemptDeadline time.Time
	// ScheduleToCloseDeadline bounds every attempt combined; zero means none.
	ScheduleToCloseDeadline time.Time
	// HeartbeatDeadline is refreshed by each heartbeat; zero means the activity
	// did not ask for heartbeating.
	HeartbeatDeadline    time.Time
	HeartbeatTimeout     time.Duration
	LastHeartbeatDetails *skald.Payload
	RetryPolicy          *skald.RetryPolicy
	CancelRequested      bool
	// RequestID pins the attempt to the worker poll that started it, so a
	// duplicate completion from a retried RPC is recognised and ignored.
	RequestID string
	// lastFailure is the failure of the previous attempt. It is unexported
	// because it is a scheduling detail: workflow code learns about activity
	// failures from history events, never from engine state.
	lastFailure *skald.ApplicationError
	Attributes  history.ActivityTaskScheduledAttributes
}

// Started reports whether a worker currently holds the activity.
func (a *ActivityState) Started() bool { return a.StartedEventID != 0 }

// TimerState is the engine-side record of one pending durable timer.
type TimerState struct {
	StartedEventID int64
	TimerID        string
	FireAt         time.Time
}

// WorkflowTaskState tracks the at-most-one workflow task in flight.
//
// Skald allows exactly one outstanding workflow task per execution. That single
// constraint is what makes workflow code single-threaded from the author's
// point of view and removes an entire class of concurrency bugs from user code.
type WorkflowTaskState struct {
	ScheduledEventID int64
	StartedEventID   int64
	Attempt          int32
	ScheduledAt      time.Time
	StartedAt        time.Time
	Deadline         time.Time
	RequestID        string
	TaskQueue        string
	Timeout          time.Duration
}

// Scheduled reports whether a task exists but no worker holds it.
func (t *WorkflowTaskState) Scheduled() bool { return t.ScheduledEventID != 0 && t.StartedEventID == 0 }

// Started reports whether a worker holds the task.
func (t *WorkflowTaskState) Started() bool { return t.StartedEventID != 0 }

// MutableState is the derived view of one workflow run.
//
// It is not safe for concurrent use. The engine serializes access per
// execution with a per-key lock held for the duration of a transaction, which
// is cheaper and far easier to reason about than fine-grained locking inside
// the state itself.
type MutableState struct {
	Namespace    string
	Execution    skald.WorkflowExecution
	Status       skald.WorkflowStatus
	StartedAt    time.Time
	ClosedAt     time.Time
	WorkflowType string
	TaskQueue    string

	// Version is the optimistic-concurrency token. Every successful write
	// increments it; a write against a stale version is rejected, which is how
	// two engine replicas racing on the same execution stay consistent without
	// a distributed lock.
	Version int64

	events history.History

	// Deadlines derived from the start event.
	RunDeadline        time.Time
	ExecutionDeadline  time.Time
	DefaultTaskTimeout time.Duration

	// Pending work, indexed by the event that created it.
	activities map[int64]*ActivityState
	// activityIDs maps user-visible activity IDs to their scheduled event so
	// that a duplicate ScheduleActivity command can be detected.
	activityIDs map[string]int64
	timers      map[int64]*TimerState
	timerIDs    map[string]int64

	workflowTask WorkflowTaskState

	// signalCount and pending signal buffering: signals that arrive while a
	// workflow task is in flight are appended to history immediately but only
	// become visible to workflow code on the next task, which keeps the
	// "history is the input" invariant intact.
	SignalCount int64

	CancelRequested bool
	CancelReason    string

	// Retry bookkeeping for the workflow itself.
	Attempt             int32
	RetryPolicy         *skald.RetryPolicy
	RandomnessSeed      int64
	FirstExecutionRunID string
	CronSchedule        string
	Memo                map[string]string
	SearchAttrs         map[string]string

	// now is injected so that tests and the deterministic simulator control
	// time completely. Production wires it to time.Now.
	now func() time.Time
}

// NewMutableState returns an empty state ready to receive event 1.
func NewMutableState(namespace string, exec skald.WorkflowExecution, now func() time.Time) *MutableState {
	if now == nil {
		now = time.Now
	}
	return &MutableState{
		Namespace:   namespace,
		Execution:   exec,
		Status:      skald.StatusRunning,
		activities:  make(map[int64]*ActivityState),
		activityIDs: make(map[string]int64),
		timers:      make(map[int64]*TimerState),
		timerIDs:    make(map[string]int64),
		now:         now,
	}
}

// Rebuild reconstructs state by replaying a persisted history.
//
// This is the only way a state is loaded, which means the replay path is
// exercised on every single request rather than only during recovery. A bug in
// event application therefore shows up immediately in normal operation instead
// of hiding until a failover.
func Rebuild(namespace string, exec skald.WorkflowExecution, h history.History, now func() time.Time) (*MutableState, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	ms := NewMutableState(namespace, exec, now)
	for _, ev := range h {
		if err := ms.apply(ev); err != nil {
			return nil, fmt.Errorf("execution: rebuild at event %d (%s): %w", ev.ID, ev.Type(), err)
		}
		ms.events = append(ms.events, ev)
	}
	return ms, nil
}

// Events returns the full history held by the state.
func (ms *MutableState) Events() history.History { return ms.events }

// NextEventID returns the ID the next appended event will receive.
func (ms *MutableState) NextEventID() int64 { return ms.events.NextEventID() }

// Now returns the injected clock reading.
func (ms *MutableState) Now() time.Time { return ms.now() }

// HasPendingWork reports whether anything could still advance the execution.
func (ms *MutableState) HasPendingWork() bool {
	return len(ms.activities) > 0 || len(ms.timers) > 0 || ms.workflowTask.ScheduledEventID != 0
}

// Activities returns the pending activities keyed by scheduled event ID.
func (ms *MutableState) Activities() map[int64]*ActivityState { return ms.activities }

// Activity returns the pending activity scheduled by the given event.
func (ms *MutableState) Activity(scheduledEventID int64) (*ActivityState, bool) {
	a, ok := ms.activities[scheduledEventID]
	return a, ok
}

// Timers returns the pending timers keyed by started event ID.
func (ms *MutableState) Timers() map[int64]*TimerState { return ms.timers }

// WorkflowTask returns the in-flight workflow task record.
func (ms *MutableState) WorkflowTask() WorkflowTaskState { return ms.workflowTask }

// AppendEvent stamps attrs with the next event ID and the current time, applies
// it to the state and records it.
//
// Applying before recording matters: if apply rejects the transition the
// history is left untouched, so a rejected operation cannot leave a half-written
// run behind.
func (ms *MutableState) AppendEvent(attrs history.Attributes) (history.Event, error) {
	if ms.Status.Terminal() {
		return history.Event{}, transitionErr("cannot append %s to a %s execution", attrs.Type(), ms.Status)
	}
	ev := history.Event{ID: ms.NextEventID(), Time: ms.now().UTC(), Attrs: attrs}
	if err := ms.apply(ev); err != nil {
		return history.Event{}, err
	}
	ms.events = append(ms.events, ev)
	return ev, nil
}

// apply mutates the derived state for a single event. It must remain a pure
// function of (state, event): no I/O, no clock reads, no randomness. That
// purity is what lets Rebuild and AppendEvent share the same code and produce
// byte-identical results.
func (ms *MutableState) apply(ev history.Event) error {
	switch a := ev.Attrs.(type) {
	case history.WorkflowExecutionStartedAttributes:
		return ms.applyStarted(ev, a)

	case history.WorkflowTaskScheduledAttributes:
		if ms.workflowTask.ScheduledEventID != 0 {
			return transitionErr("a workflow task is already scheduled (event %d)", ms.workflowTask.ScheduledEventID)
		}
		ms.workflowTask = WorkflowTaskState{
			ScheduledEventID: ev.ID,
			Attempt:          a.Attempt,
			ScheduledAt:      ev.Time,
			TaskQueue:        a.TaskQueue,
			Timeout:          a.StartToCloseTimout,
		}

	case history.WorkflowTaskStartedAttributes:
		if !ms.workflowTask.Scheduled() {
			return transitionErr("no workflow task is waiting to be started")
		}
		ms.workflowTask.StartedEventID = ev.ID
		ms.workflowTask.StartedAt = ev.Time
		ms.workflowTask.RequestID = a.RequestID
		ms.workflowTask.Deadline = ev.Time.Add(ms.workflowTask.Timeout)

	case history.WorkflowTaskCompletedAttributes:
		if !ms.workflowTask.Started() {
			return transitionErr("no workflow task is in flight to complete")
		}
		ms.workflowTask = WorkflowTaskState{}

	case history.WorkflowTaskFailedAttributes, history.WorkflowTaskTimedOutAttributes:
		if !ms.workflowTask.Started() {
			return transitionErr("no workflow task is in flight to fail")
		}
		ms.workflowTask = WorkflowTaskState{}

	case history.ActivityTaskScheduledAttributes:
		return ms.applyActivityScheduled(ev, a)

	case history.ActivityTaskStartedAttributes:
		act, ok := ms.activities[a.ScheduledEventID]
		if !ok {
			return transitionErr("activity scheduled by event %d is not pending", a.ScheduledEventID)
		}
		if act.Started() {
			return transitionErr("activity %q is already started", act.ActivityID)
		}
		act.StartedEventID = ev.ID
		act.StartedAt = ev.Time
		act.Attempt = a.Attempt
		act.RequestID = a.RequestID
		if act.Attributes.StartToCloseTimeout > 0 {
			act.AttemptDeadline = ev.Time.Add(act.Attributes.StartToCloseTimeout)
		}
		if act.HeartbeatTimeout > 0 {
			act.HeartbeatDeadline = ev.Time.Add(act.HeartbeatTimeout)
		}

	case history.ActivityTaskCompletedAttributes:
		return ms.closeActivity(a.ScheduledEventID)
	case history.ActivityTaskFailedAttributes:
		return ms.closeActivity(a.ScheduledEventID)
	case history.ActivityTaskTimedOutAttributes:
		return ms.closeActivity(a.ScheduledEventID)
	case history.ActivityTaskCanceledAttributes:
		return ms.closeActivity(a.ScheduledEventID)

	case history.ActivityTaskCancelRequestedAttributes:
		act, ok := ms.activities[a.ScheduledEventID]
		if !ok {
			return transitionErr("activity scheduled by event %d is not pending", a.ScheduledEventID)
		}
		act.CancelRequested = true

	case history.TimerStartedAttributes:
		if _, dup := ms.timerIDs[a.TimerID]; dup {
			return transitionErr("timer %q is already pending", a.TimerID)
		}
		ms.timers[ev.ID] = &TimerState{
			StartedEventID: ev.ID,
			TimerID:        a.TimerID,
			FireAt:         ev.Time.Add(a.StartToFireTimeout),
		}
		ms.timerIDs[a.TimerID] = ev.ID

	case history.TimerFiredAttributes:
		return ms.closeTimer(a.StartedEventID)
	case history.TimerCanceledAttributes:
		return ms.closeTimer(a.StartedEventID)

	case history.MarkerRecordedAttributes:
		// Markers carry no engine-side state; they exist purely so that replay
		// observes the same value the original execution did.

	case history.WorkflowExecutionSignaledAttributes:
		ms.SignalCount++

	case history.WorkflowExecutionCancelRequestedAttributes:
		if ms.CancelRequested {
			// A duplicate cancel request is a no-op rather than an error:
			// callers retry, and the second request carries the same intent.
			return nil
		}
		ms.CancelRequested = true
		ms.CancelReason = a.Reason

	case history.WorkflowExecutionCompletedAttributes:
		ms.close(skald.StatusCompleted, ev.Time)
	case history.WorkflowExecutionFailedAttributes:
		ms.close(skald.StatusFailed, ev.Time)
	case history.WorkflowExecutionTimedOutAttributes:
		ms.close(skald.StatusTimedOut, ev.Time)
	case history.WorkflowExecutionCanceledAttributes:
		ms.close(skald.StatusCanceled, ev.Time)
	case history.WorkflowExecutionTerminatedAttributes:
		ms.close(skald.StatusTerminated, ev.Time)
	case history.WorkflowExecutionContinuedAsNewAttributes:
		ms.close(skald.StatusContinuedAsNew, ev.Time)

	default:
		return transitionErr("no state transition defined for %s", ev.Type())
	}
	return nil
}

func (ms *MutableState) applyStarted(ev history.Event, a history.WorkflowExecutionStartedAttributes) error {
	if len(ms.events) != 0 {
		return transitionErr("execution already started")
	}
	ms.WorkflowType = a.WorkflowType
	ms.TaskQueue = a.TaskQueue
	ms.StartedAt = ev.Time
	ms.Attempt = a.Attempt
	ms.RetryPolicy = a.RetryPolicy
	ms.RandomnessSeed = a.RandomnessSeed
	ms.CronSchedule = a.CronSchedule
	ms.Memo = a.Memo
	ms.SearchAttrs = a.SearchAttrs
	ms.DefaultTaskTimeout = a.TaskTimeout
	ms.FirstExecutionRunID = a.FirstExecutionRunID
	if ms.FirstExecutionRunID == "" {
		ms.FirstExecutionRunID = ms.Execution.RunID
	}
	if a.RunTimeout > 0 {
		ms.RunDeadline = ev.Time.Add(a.RunTimeout)
	}
	if a.ExecutionTimeout > 0 {
		ms.ExecutionDeadline = ev.Time.Add(a.ExecutionTimeout)
	}
	return nil
}

func (ms *MutableState) applyActivityScheduled(ev history.Event, a history.ActivityTaskScheduledAttributes) error {
	if prev, dup := ms.activityIDs[a.ActivityID]; dup {
		return transitionErr("activity id %q is already pending as event %d", a.ActivityID, prev)
	}
	st := &ActivityState{
		ScheduledEventID: ev.ID,
		ActivityID:       a.ActivityID,
		ActivityType:     a.ActivityType,
		TaskQueue:        a.TaskQueue,
		Attempt:          1,
		ScheduledAt:      ev.Time,
		HeartbeatTimeout: a.HeartbeatTimeout,
		RetryPolicy:      a.RetryPolicy,
		Attributes:       a,
	}
	if a.ScheduleToCloseTimeout > 0 {
		st.ScheduleToCloseDeadline = ev.Time.Add(a.ScheduleToCloseTimeout)
	}
	ms.activities[ev.ID] = st
	ms.activityIDs[a.ActivityID] = ev.ID
	return nil
}

func (ms *MutableState) closeActivity(scheduledEventID int64) error {
	act, ok := ms.activities[scheduledEventID]
	if !ok {
		return transitionErr("activity scheduled by event %d is not pending", scheduledEventID)
	}
	delete(ms.activities, scheduledEventID)
	delete(ms.activityIDs, act.ActivityID)
	return nil
}

func (ms *MutableState) closeTimer(startedEventID int64) error {
	t, ok := ms.timers[startedEventID]
	if !ok {
		return transitionErr("timer started by event %d is not pending", startedEventID)
	}
	delete(ms.timers, startedEventID)
	delete(ms.timerIDs, t.TimerID)
	return nil
}

func (ms *MutableState) close(status skald.WorkflowStatus, at time.Time) {
	ms.Status = status
	ms.ClosedAt = at
	// Everything still pending is abandoned. The events that closed the
	// execution are the record; leaving stale entries here would make
	// HasPendingWork lie to the timer scanner.
	ms.activities = map[int64]*ActivityState{}
	ms.activityIDs = map[string]int64{}
	ms.timers = map[int64]*TimerState{}
	ms.timerIDs = map[string]int64{}
	ms.workflowTask = WorkflowTaskState{}
}
