package history

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Event is one immutable entry in a workflow's history.
//
// The struct is intentionally small: an identity triple (ID, Time, Type) plus a
// polymorphic attribute set. Storing attributes behind an interface rather than
// as twenty-odd nullable pointer fields keeps the in-memory representation
// compact and makes an impossible state -- an ActivityTaskStarted event
// carrying timer attributes -- unrepresentable.
type Event struct {
	// ID is the 1-based position of the event in its history. It is dense: a
	// history with N events has IDs 1..N with no gaps, which is what lets the
	// replayer index by ID and lets the store paginate by range.
	ID int64
	// Time is the server's wall clock at the moment the event was appended.
	// Workflow code reads time only from here, never from the OS.
	Time time.Time
	// Attrs is never nil in a valid event.
	Attrs Attributes
}

// Type returns the event's type, or EventTypeUnspecified when Attrs is nil.
func (e Event) Type() EventType {
	if e.Attrs == nil {
		return EventTypeUnspecified
	}
	return e.Attrs.Type()
}

// String renders the event compactly for logs and CLI output.
func (e Event) String() string {
	return fmt.Sprintf("%d %s %s", e.ID, e.Time.UTC().Format(time.RFC3339Nano), e.Type())
}

// attributeConstructors maps every known event type to a factory for its
// attribute struct. It is the single registry the JSON decoder consults, so
// adding an event type is a one-line change that cannot be forgotten: the
// round-trip test iterates over this map.
var attributeConstructors = map[EventType]func() Attributes{
	EventTypeWorkflowExecutionStarted:         func() Attributes { return &WorkflowExecutionStartedAttributes{} },
	EventTypeWorkflowExecutionCompleted:       func() Attributes { return &WorkflowExecutionCompletedAttributes{} },
	EventTypeWorkflowExecutionFailed:          func() Attributes { return &WorkflowExecutionFailedAttributes{} },
	EventTypeWorkflowExecutionTimedOut:        func() Attributes { return &WorkflowExecutionTimedOutAttributes{} },
	EventTypeWorkflowExecutionCanceled:        func() Attributes { return &WorkflowExecutionCanceledAttributes{} },
	EventTypeWorkflowExecutionTerminated:      func() Attributes { return &WorkflowExecutionTerminatedAttributes{} },
	EventTypeWorkflowExecutionContinuedAsNew:  func() Attributes { return &WorkflowExecutionContinuedAsNewAttributes{} },
	EventTypeWorkflowExecutionCancelRequested: func() Attributes { return &WorkflowExecutionCancelRequestedAttributes{} },
	EventTypeWorkflowExecutionSignaled:        func() Attributes { return &WorkflowExecutionSignaledAttributes{} },
	EventTypeWorkflowTaskScheduled:            func() Attributes { return &WorkflowTaskScheduledAttributes{} },
	EventTypeWorkflowTaskStarted:              func() Attributes { return &WorkflowTaskStartedAttributes{} },
	EventTypeWorkflowTaskCompleted:            func() Attributes { return &WorkflowTaskCompletedAttributes{} },
	EventTypeWorkflowTaskFailed:               func() Attributes { return &WorkflowTaskFailedAttributes{} },
	EventTypeWorkflowTaskTimedOut:             func() Attributes { return &WorkflowTaskTimedOutAttributes{} },
	EventTypeActivityTaskScheduled:            func() Attributes { return &ActivityTaskScheduledAttributes{} },
	EventTypeActivityTaskStarted:              func() Attributes { return &ActivityTaskStartedAttributes{} },
	EventTypeActivityTaskCompleted:            func() Attributes { return &ActivityTaskCompletedAttributes{} },
	EventTypeActivityTaskFailed:               func() Attributes { return &ActivityTaskFailedAttributes{} },
	EventTypeActivityTaskTimedOut:             func() Attributes { return &ActivityTaskTimedOutAttributes{} },
	EventTypeActivityTaskCancelRequested:      func() Attributes { return &ActivityTaskCancelRequestedAttributes{} },
	EventTypeActivityTaskCanceled:             func() Attributes { return &ActivityTaskCanceledAttributes{} },
	EventTypeTimerStarted:                     func() Attributes { return &TimerStartedAttributes{} },
	EventTypeTimerFired:                       func() Attributes { return &TimerFiredAttributes{} },
	EventTypeTimerCanceled:                    func() Attributes { return &TimerCanceledAttributes{} },
	EventTypeMarkerRecorded:                   func() Attributes { return &MarkerRecordedAttributes{} },
}

// NewAttributes allocates an empty attribute struct for the given type.
func NewAttributes(t EventType) (Attributes, error) {
	ctor, ok := attributeConstructors[t]
	if !ok {
		return nil, fmt.Errorf("history: no attributes registered for %s", t)
	}
	return ctor(), nil
}

// KnownEventTypes returns every event type this build can decode, in ascending
// numeric order. Tests use it to prove the registry and the name table agree.
func KnownEventTypes() []EventType {
	out := make([]EventType, 0, len(attributeConstructors))
	for t := range attributeConstructors {
		out = append(out, t)
	}
	// Insertion sort: the slice is tiny and this avoids importing sort into a
	// package that is otherwise dependency-free.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// wireEvent is the on-disk and on-the-wire shape of an Event. Keeping it
// separate from Event means the persisted format is visible in one place and
// can evolve independently of the in-memory representation.
type wireEvent struct {
	ID    int64           `json:"id"`
	Time  time.Time       `json:"time"`
	Type  EventType       `json:"type"`
	Attrs json.RawMessage `json:"attrs,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (e Event) MarshalJSON() ([]byte, error) {
	if e.Attrs == nil {
		return nil, fmt.Errorf("history: cannot marshal event %d with nil attributes", e.ID)
	}
	raw, err := json.Marshal(e.Attrs)
	if err != nil {
		return nil, fmt.Errorf("history: marshal %s attributes: %w", e.Type(), err)
	}
	return json.Marshal(wireEvent{ID: e.ID, Time: e.Time.UTC(), Type: e.Type(), Attrs: raw})
}

// UnmarshalJSON implements json.Unmarshaler.
//
// An unknown event type is a hard error. See the package documentation: a
// decoder that skips events it does not understand silently produces a
// different mutable state than the writer had.
func (e *Event) UnmarshalJSON(b []byte) error {
	var w wireEvent
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("history: decode event envelope: %w", err)
	}
	attrs, err := NewAttributes(w.Type)
	if err != nil {
		return err
	}
	if len(w.Attrs) > 0 {
		if err := json.Unmarshal(w.Attrs, attrs); err != nil {
			return fmt.Errorf("history: decode %s attributes: %w", w.Type, err)
		}
	}
	// Attributes are stored behind pointers during decoding so that
	// json.Unmarshal can write into them; dereference to the value type so that
	// Event.Attrs compares by value and cannot be mutated through an alias.
	e.ID = w.ID
	e.Time = w.Time
	e.Attrs = derefAttributes(attrs)
	return nil
}

// derefAttributes converts *T to T for any registered attribute type.
func derefAttributes(a Attributes) Attributes {
	v := reflect.ValueOf(a)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return a
	}
	elem := v.Elem().Interface()
	if attrs, ok := elem.(Attributes); ok {
		return attrs
	}
	return a
}

// AttributesAs is a generic, allocation-free accessor that reports whether the
// event carries attributes of type T and returns them.
//
//	if a, ok := history.AttributesAs[history.TimerFiredAttributes](ev); ok { ... }
//
// It replaces the long type-switch that would otherwise appear at every call
// site and, unlike a switch, fails to compile if T is not an Attributes type.
func AttributesAs[T Attributes](e Event) (T, bool) {
	var zero T
	if e.Attrs == nil {
		return zero, false
	}
	if v, ok := e.Attrs.(T); ok {
		return v, true
	}
	// Tolerate a pointer-shaped attribute produced by hand-built events in
	// tests, so that callers never have to care which form they hold.
	if p, ok := e.Attrs.(interface{ Type() EventType }); ok {
		rv := reflect.ValueOf(p)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			if v, ok := rv.Elem().Interface().(T); ok {
				return v, true
			}
		}
	}
	return zero, false
}

// MustAttributes is AttributesAs for code paths where the event type has
// already been checked and a mismatch would be a programming error.
func MustAttributes[T Attributes](e Event) T {
	v, ok := AttributesAs[T](e)
	if !ok {
		panic(fmt.Sprintf("history: event %d is %s, not %T", e.ID, e.Type(), v))
	}
	return v
}
