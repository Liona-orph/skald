package history

import "fmt"

// EventType identifies the shape of a history event.
//
// The numeric values are part of Skald's persisted format. Codes are assigned
// in blocks so that a new event in an existing family stays close to its
// relatives, and gaps are left deliberately rather than filled opportunistically.
type EventType int32

const (
	EventTypeUnspecified EventType = 0

	// Execution lifecycle: 1-19.
	EventTypeWorkflowExecutionStarted         EventType = 1
	EventTypeWorkflowExecutionCompleted       EventType = 2
	EventTypeWorkflowExecutionFailed          EventType = 3
	EventTypeWorkflowExecutionTimedOut        EventType = 4
	EventTypeWorkflowExecutionCanceled        EventType = 5
	EventTypeWorkflowExecutionTerminated      EventType = 6
	EventTypeWorkflowExecutionContinuedAsNew  EventType = 7
	EventTypeWorkflowExecutionCancelRequested EventType = 8
	EventTypeWorkflowExecutionSignaled        EventType = 9

	// Workflow tasks (the unit of workflow-code execution): 20-29.
	EventTypeWorkflowTaskScheduled EventType = 20
	EventTypeWorkflowTaskStarted   EventType = 21
	EventTypeWorkflowTaskCompleted EventType = 22
	EventTypeWorkflowTaskFailed    EventType = 23
	EventTypeWorkflowTaskTimedOut  EventType = 24

	// Activity tasks: 40-59.
	EventTypeActivityTaskScheduled       EventType = 40
	EventTypeActivityTaskStarted         EventType = 41
	EventTypeActivityTaskCompleted       EventType = 42
	EventTypeActivityTaskFailed          EventType = 43
	EventTypeActivityTaskTimedOut        EventType = 44
	EventTypeActivityTaskCancelRequested EventType = 45
	EventTypeActivityTaskCanceled        EventType = 46

	// Timers: 60-69.
	EventTypeTimerStarted  EventType = 60
	EventTypeTimerFired    EventType = 61
	EventTypeTimerCanceled EventType = 62

	// Markers record a decision the workflow made outside the command protocol,
	// such as a side effect result or a version gate. 80-89.
	EventTypeMarkerRecorded EventType = 80

	// Codes 100-119 are reserved for child workflow executions. They are
	// deliberately unassigned: reserving the range now means the child workflow
	// implementation will not have to renumber anything, and a history written
	// by a future version can be rejected precisely rather than misparsed.
)

var eventTypeNames = map[EventType]string{
	EventTypeUnspecified:                      "Unspecified",
	EventTypeWorkflowExecutionStarted:         "WorkflowExecutionStarted",
	EventTypeWorkflowExecutionCompleted:       "WorkflowExecutionCompleted",
	EventTypeWorkflowExecutionFailed:          "WorkflowExecutionFailed",
	EventTypeWorkflowExecutionTimedOut:        "WorkflowExecutionTimedOut",
	EventTypeWorkflowExecutionCanceled:        "WorkflowExecutionCanceled",
	EventTypeWorkflowExecutionTerminated:      "WorkflowExecutionTerminated",
	EventTypeWorkflowExecutionContinuedAsNew:  "WorkflowExecutionContinuedAsNew",
	EventTypeWorkflowExecutionCancelRequested: "WorkflowExecutionCancelRequested",
	EventTypeWorkflowExecutionSignaled:        "WorkflowExecutionSignaled",
	EventTypeWorkflowTaskScheduled:            "WorkflowTaskScheduled",
	EventTypeWorkflowTaskStarted:              "WorkflowTaskStarted",
	EventTypeWorkflowTaskCompleted:            "WorkflowTaskCompleted",
	EventTypeWorkflowTaskFailed:               "WorkflowTaskFailed",
	EventTypeWorkflowTaskTimedOut:             "WorkflowTaskTimedOut",
	EventTypeActivityTaskScheduled:            "ActivityTaskScheduled",
	EventTypeActivityTaskStarted:              "ActivityTaskStarted",
	EventTypeActivityTaskCompleted:            "ActivityTaskCompleted",
	EventTypeActivityTaskFailed:               "ActivityTaskFailed",
	EventTypeActivityTaskTimedOut:             "ActivityTaskTimedOut",
	EventTypeActivityTaskCancelRequested:      "ActivityTaskCancelRequested",
	EventTypeActivityTaskCanceled:             "ActivityTaskCanceled",
	EventTypeTimerStarted:                     "TimerStarted",
	EventTypeTimerFired:                       "TimerFired",
	EventTypeTimerCanceled:                    "TimerCanceled",
	EventTypeMarkerRecorded:                   "MarkerRecorded",
}

var eventTypesByName = func() map[string]EventType {
	m := make(map[string]EventType, len(eventTypeNames))
	for k, v := range eventTypeNames {
		m[v] = k
	}
	return m
}()

func (t EventType) String() string {
	if n, ok := eventTypeNames[t]; ok {
		return n
	}
	return fmt.Sprintf("EventType(%d)", int32(t))
}

// Known reports whether this build understands the event type. Decoding a
// history containing an unknown type is a hard error rather than a skipped
// event: silently ignoring an event would produce a mutable state that
// disagrees with the one the writer had, which is worse than refusing to run.
func (t EventType) Known() bool {
	_, ok := eventTypeNames[t]
	return ok && t != EventTypeUnspecified
}

// Terminal reports whether the event closes the execution.
func (t EventType) Terminal() bool {
	switch t {
	case EventTypeWorkflowExecutionCompleted,
		EventTypeWorkflowExecutionFailed,
		EventTypeWorkflowExecutionTimedOut,
		EventTypeWorkflowExecutionCanceled,
		EventTypeWorkflowExecutionTerminated,
		EventTypeWorkflowExecutionContinuedAsNew:
		return true
	}
	return false
}

// MarshalText implements encoding.TextMarshaler so histories are readable in
// their JSON form without a decoder ring.
func (t EventType) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *EventType) UnmarshalText(b []byte) error {
	v, ok := eventTypesByName[string(b)]
	if !ok {
		return fmt.Errorf("history: unknown event type %q", b)
	}
	*t = v
	return nil
}
