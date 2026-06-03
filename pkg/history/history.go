package history

import (
	"errors"
	"fmt"
	"time"
)

// MaxHistoryEvents is the point at which the engine refuses to append more
// events to a single run. A workflow that legitimately needs more should call
// ContinueAsNew; one that does not is almost always looping.
//
// The limit exists because history length drives replay cost linearly: a worker
// that picks up a task must reconstruct state from event 1, so an unbounded
// history eventually makes every workflow task time out.
const MaxHistoryEvents = 50_000

// MaxHistoryBytes bounds the serialized size of a single run's history.
const MaxHistoryBytes = 64 << 20 // 64 MiB

// History is an ordered, dense sequence of events for one workflow run.
type History []Event

// ErrInvalidHistory is the sentinel wrapping every structural violation.
var ErrInvalidHistory = errors.New("history: invalid")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidHistory, fmt.Sprintf(format, args...))
}

// LastEventID returns the ID of the final event, or 0 for an empty history.
func (h History) LastEventID() int64 {
	if len(h) == 0 {
		return 0
	}
	return h[len(h)-1].ID
}

// NextEventID returns the ID the next appended event will take.
func (h History) NextEventID() int64 { return h.LastEventID() + 1 }

// Get returns the event with the given ID.
//
// Because IDs are dense and 1-based, this is an O(1) index rather than a scan,
// and the density invariant is enforced by Validate rather than assumed here.
func (h History) Get(id int64) (Event, bool) {
	if id < 1 || id > int64(len(h)) {
		return Event{}, false
	}
	ev := h[id-1]
	if ev.ID != id {
		return Event{}, false
	}
	return ev, true
}

// StartedAttributes returns the attributes of event 1.
func (h History) StartedAttributes() (WorkflowExecutionStartedAttributes, bool) {
	if len(h) == 0 {
		return WorkflowExecutionStartedAttributes{}, false
	}
	return AttributesAs[WorkflowExecutionStartedAttributes](h[0])
}

// Terminated reports whether the history has reached a terminal event.
func (h History) Terminated() bool {
	return len(h) > 0 && h[len(h)-1].Type().Terminal()
}

// Filter returns the events matching any of the given types, preserving order.
func (h History) Filter(types ...EventType) History {
	want := make(map[EventType]struct{}, len(types))
	for _, t := range types {
		want[t] = struct{}{}
	}
	out := make(History, 0, len(h))
	for _, ev := range h {
		if _, ok := want[ev.Type()]; ok {
			out = append(out, ev)
		}
	}
	return out
}

// Validate checks every structural invariant Skald relies on.
//
// This is deliberately thorough and deliberately cheap to call: the store runs
// it on every read in test and simulation builds, and the CLI runs it on
// exported histories, which is how corruption gets caught at the boundary
// instead of as a mysterious replay failure hours later.
func (h History) Validate() error {
	if len(h) == 0 {
		return nil
	}
	if len(h) > MaxHistoryEvents {
		return invalid("history has %d events, limit is %d", len(h), MaxHistoryEvents)
	}
	if h[0].Type() != EventTypeWorkflowExecutionStarted {
		return invalid("first event is %s, must be %s", h[0].Type(), EventTypeWorkflowExecutionStarted)
	}

	var prevTime time.Time
	// Sets of event IDs that later events are allowed to reference.
	scheduledWorkflowTasks := map[int64]bool{}
	startedWorkflowTasks := map[int64]bool{}
	completedWorkflowTasks := map[int64]bool{}
	scheduledActivities := map[int64]bool{}
	startedActivities := map[int64]bool{}
	startedTimers := map[int64]bool{}

	for i, ev := range h {
		want := int64(i + 1)
		if ev.ID != want {
			return invalid("event at index %d has ID %d, expected %d (IDs must be dense and 1-based)", i, ev.ID, want)
		}
		if ev.Attrs == nil {
			return invalid("event %d has nil attributes", ev.ID)
		}
		if !ev.Type().Known() {
			return invalid("event %d has unknown type %s", ev.ID, ev.Type())
		}
		if ev.Time.IsZero() {
			return invalid("event %d has zero timestamp", ev.ID)
		}
		if ev.Time.Before(prevTime) {
			return invalid("event %d time %s precedes event %d time %s (history time must not go backwards)",
				ev.ID, ev.Time.UTC(), ev.ID-1, prevTime.UTC())
		}
		prevTime = ev.Time

		if i > 0 && ev.Type() == EventTypeWorkflowExecutionStarted {
			return invalid("event %d is a second %s", ev.ID, EventTypeWorkflowExecutionStarted)
		}
		if ev.Type().Terminal() && i != len(h)-1 {
			return invalid("terminal event %s at position %d is not last", ev.Type(), ev.ID)
		}

		if err := h.validateReferences(ev,
			scheduledWorkflowTasks, startedWorkflowTasks, completedWorkflowTasks,
			scheduledActivities, startedActivities, startedTimers); err != nil {
			return err
		}
	}
	return nil
}

// validateReferences checks that every back-reference in ev names an event that
// exists, precedes ev and has the expected type, then records ev in the
// appropriate set for later events to reference.
func (h History) validateReferences(
	ev Event,
	scheduledWorkflowTasks, startedWorkflowTasks, completedWorkflowTasks map[int64]bool,
	scheduledActivities, startedActivities, startedTimers map[int64]bool,
) error {
	ref := func(name string, id int64, set map[int64]bool) error {
		if id <= 0 || id >= ev.ID {
			return invalid("event %d references %s %d, which is not an earlier event", ev.ID, name, id)
		}
		if !set[id] {
			return invalid("event %d references %s %d, which is not a valid %s", ev.ID, name, id, name)
		}
		return nil
	}
	// A command-derived event must name the workflow task that produced it.
	// Event 1 is exempt because nothing produced it.
	cmdRef := func(id int64) error {
		if id == 0 {
			return invalid("event %d does not name the workflow task that produced it", ev.ID)
		}
		return ref("workflow task completed event", id, completedWorkflowTasks)
	}

	switch a := ev.Attrs.(type) {
	case WorkflowTaskScheduledAttributes:
		scheduledWorkflowTasks[ev.ID] = true
	case WorkflowTaskStartedAttributes:
		if err := ref("workflow task scheduled event", a.ScheduledEventID, scheduledWorkflowTasks); err != nil {
			return err
		}
		startedWorkflowTasks[ev.ID] = true
	case WorkflowTaskCompletedAttributes:
		if err := ref("workflow task started event", a.StartedEventID, startedWorkflowTasks); err != nil {
			return err
		}
		completedWorkflowTasks[ev.ID] = true
	case WorkflowTaskFailedAttributes:
		if err := ref("workflow task started event", a.StartedEventID, startedWorkflowTasks); err != nil {
			return err
		}
	case WorkflowTaskTimedOutAttributes:
		if err := ref("workflow task started event", a.StartedEventID, startedWorkflowTasks); err != nil {
			return err
		}

	case ActivityTaskScheduledAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if a.ActivityID == "" {
			return invalid("event %d schedules an activity with an empty activity id", ev.ID)
		}
		scheduledActivities[ev.ID] = true
	case ActivityTaskStartedAttributes:
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}
		startedActivities[ev.ID] = true
	case ActivityTaskCompletedAttributes:
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}
		if err := ref("activity started event", a.StartedEventID, startedActivities); err != nil {
			return err
		}
	case ActivityTaskFailedAttributes:
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}
		if a.Failure == nil {
			return invalid("event %d is an activity failure with no failure detail", ev.ID)
		}
	case ActivityTaskTimedOutAttributes:
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}
	case ActivityTaskCancelRequestedAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}
	case ActivityTaskCanceledAttributes:
		if err := ref("activity scheduled event", a.ScheduledEventID, scheduledActivities); err != nil {
			return err
		}

	case TimerStartedAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if a.TimerID == "" {
			return invalid("event %d starts a timer with an empty timer id", ev.ID)
		}
		if a.StartToFireTimeout < 0 {
			return invalid("event %d starts a timer with a negative duration", ev.ID)
		}
		startedTimers[ev.ID] = true
	case TimerFiredAttributes:
		if err := ref("timer started event", a.StartedEventID, startedTimers); err != nil {
			return err
		}
	case TimerCanceledAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if err := ref("timer started event", a.StartedEventID, startedTimers); err != nil {
			return err
		}

	case MarkerRecordedAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if a.MarkerName == "" {
			return invalid("event %d records a marker with no name", ev.ID)
		}

	case WorkflowExecutionCompletedAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
	case WorkflowExecutionFailedAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if a.Failure == nil {
			return invalid("event %d fails the execution with no failure detail", ev.ID)
		}
	case WorkflowExecutionCanceledAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
	case WorkflowExecutionContinuedAsNewAttributes:
		if err := cmdRef(a.WorkflowTaskCompletedEventID); err != nil {
			return err
		}
		if a.NewRunID == "" {
			return invalid("event %d continues as new without naming the successor run", ev.ID)
		}

	case WorkflowExecutionStartedAttributes:
		if a.WorkflowType == "" {
			return invalid("event %d starts an execution with no workflow type", ev.ID)
		}
		if a.TaskQueue == "" {
			return invalid("event %d starts an execution with no task queue", ev.ID)
		}
	case WorkflowExecutionSignaledAttributes:
		if a.SignalName == "" {
			return invalid("event %d delivers a signal with no name", ev.ID)
		}
	}
	return nil
}
