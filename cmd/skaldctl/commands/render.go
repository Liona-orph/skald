package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// Column widths for the history view.
//
// They are constants rather than computed from the data because `--follow`
// prints in batches: widths derived from the first batch would shift the moment
// a longer value arrived, and a log that re-aligns itself while you are reading
// it is worse than one that is slightly too wide. The event column fits
// "WorkflowExecutionCancelRequested" (32) plus the two-space indent, which is
// the longest name the protocol has.
const (
	colID    = 4
	colAge   = 11
	colDelta = 8
	colEvent = 34
)

// historyRenderer writes the human-readable history view.
//
// It is stateful because the view is: the delta column is relative to the
// previous event, and `--follow` renders in batches, so the renderer has to
// remember where the last batch ended.
type historyRenderer struct {
	p      *Printer
	header bool
	prev   time.Time
	// indentUnder is the set of workflow-task-completed event IDs seen so far.
	// An event that names one of them was produced by that task's commands and
	// is indented under it.
	indentUnder map[int64]bool
}

func newHistoryRenderer(p *Printer) *historyRenderer {
	return &historyRenderer{p: p, indentUnder: map[int64]bool{}}
}

// WriteHeader emits the column titles once.
func (r *historyRenderer) WriteHeader() {
	if r.header {
		return
	}
	r.header = true
	r.p.Printf("%s\n", r.p.bold(fmt.Sprintf("%*s  %-*s  %-*s  %-*s  %s",
		colID, "ID", colAge, "AGE", colDelta, "DELTA", colEvent, "EVENT", "DETAILS")))
}

// Write renders a batch of events.
func (r *historyRenderer) Write(events history.History) {
	if len(events) == 0 {
		return
	}
	r.WriteHeader()
	now := r.p.Now()

	for _, ev := range events {
		delta := "-"
		if !r.prev.IsZero() {
			delta = Delta(ev.Time.Sub(r.prev))
		}
		r.prev = ev.Time

		// Indent an event that a workflow task's commands produced, so the
		// causal structure is visible without reading the back-references. This
		// is the single most useful thing about the view: an incident usually
		// starts with "which decision scheduled this activity".
		name := ev.Type().String()
		if origin := commandOrigin(ev); origin > 0 && r.indentUnder[origin] {
			name = "  " + name
		}
		if ev.Type() == history.EventTypeWorkflowTaskCompleted {
			r.indentUnder[ev.ID] = true
		}

		r.p.Printf("%*d  %-*s  %-*s  %s  %s\n",
			colID, ev.ID,
			colAge, Relative(now, ev.Time),
			colDelta, delta,
			r.p.eventColor(ev.Type(), pad(name, colEvent)),
			eventDetails(ev),
		)
	}
}

// pad left-justifies s to width, without truncating: an event name that
// overflows pushes the details right on that line only, which is far less
// annoying than a truncated event name.
func pad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// eventColor paints an event name by family.
//
// The mapping is chosen for scanning, not decoration: red is only ever a
// failure, so an eye moving down a thousand-line history stops in the right
// place without reading a word.
func (p *Printer) eventColor(t history.EventType, s string) string {
	switch t {
	case history.EventTypeWorkflowExecutionFailed,
		history.EventTypeWorkflowExecutionTimedOut,
		history.EventTypeWorkflowTaskFailed,
		history.EventTypeWorkflowTaskTimedOut,
		history.EventTypeActivityTaskFailed,
		history.EventTypeActivityTaskTimedOut:
		return p.red(s)
	case history.EventTypeWorkflowExecutionCanceled,
		history.EventTypeWorkflowExecutionTerminated,
		history.EventTypeWorkflowExecutionCancelRequested,
		history.EventTypeActivityTaskCancelRequested,
		history.EventTypeActivityTaskCanceled,
		history.EventTypeTimerCanceled:
		return p.yellow(s)
	case history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowExecutionCompleted,
		history.EventTypeWorkflowExecutionContinuedAsNew,
		history.EventTypeWorkflowExecutionSignaled:
		return p.cyan(s)
	case history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted:
		return p.blue(s)
	case history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskStarted,
		history.EventTypeActivityTaskCompleted:
		return p.green(s)
	case history.EventTypeTimerStarted, history.EventTypeTimerFired:
		return p.dim(s)
	}
	return s
}

// commandOrigin returns the workflow-task-completed event whose commands
// produced ev, or zero when ev was not produced by workflow code.
func commandOrigin(ev history.Event) int64 {
	switch a := ev.Attrs.(type) {
	case history.ActivityTaskScheduledAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.ActivityTaskCancelRequestedAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.TimerStartedAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.TimerCanceledAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.MarkerRecordedAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.WorkflowExecutionCompletedAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.WorkflowExecutionFailedAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.WorkflowExecutionCanceledAttributes:
		return a.WorkflowTaskCompletedEventID
	case history.WorkflowExecutionContinuedAsNewAttributes:
		return a.WorkflowTaskCompletedEventID
	}
	return 0
}

// fields accumulates `key=value` pairs in insertion order.
type fields struct{ parts []string }

func (f *fields) add(key, value string) {
	if value == "" {
		return
	}
	f.parts = append(f.parts, key+"="+value)
}

func (f *fields) addInt(key string, v int64) {
	if v == 0 {
		return
	}
	f.parts = append(f.parts, fmt.Sprintf("%s=%d", key, v))
}

func (f *fields) addDuration(key string, d time.Duration) {
	if d <= 0 {
		return
	}
	f.parts = append(f.parts, key+"="+CompactDuration(d))
}

func (f *fields) String() string { return strings.Join(f.parts, " ") }

// eventDetails renders the fields of an event that matter during an incident.
//
// Not every field: the goal is one line that answers "what happened", and the
// back-references that only matter to a replayer are omitted in favour of the
// ones a person acts on -- the activity type, the failure message, the timeout
// that fired. `--json` prints everything for the cases where that is not enough.
func eventDetails(ev history.Event) string {
	var f fields
	switch a := ev.Attrs.(type) {
	case history.WorkflowExecutionStartedAttributes:
		f.add("type", a.WorkflowType)
		f.add("task_queue", a.TaskQueue)
		f.addInt("attempt", int64(a.Attempt))
		f.addDuration("run_timeout", a.RunTimeout)
		f.addDuration("execution_timeout", a.ExecutionTimeout)
		f.add("cron", a.CronSchedule)
		f.add("continued_from", shortID(a.ContinuedExecutionRunID))
		f.add("input", PayloadPreview(a.Input))

	case history.WorkflowExecutionCompletedAttributes:
		f.add("result", PayloadPreview(a.Result))
		f.addInt("from_task", a.WorkflowTaskCompletedEventID)

	case history.WorkflowExecutionFailedAttributes:
		f.add("failure", failureText(a.Failure))
		f.add("retry_state", retryStateText(a.RetryState))

	case history.WorkflowExecutionTimedOutAttributes:
		f.add("kind", a.Kind.String())
		f.add("retry_state", retryStateText(a.RetryState))

	case history.WorkflowExecutionCanceledAttributes:
		f.add("details", PayloadPreview(a.Details))

	case history.WorkflowExecutionTerminatedAttributes:
		f.add("reason", a.Reason)
		f.add("by", a.Identity)

	case history.WorkflowExecutionContinuedAsNewAttributes:
		f.add("new_run", shortID(a.NewRunID))
		f.add("type", a.WorkflowType)
		f.add("input", PayloadPreview(a.Input))

	case history.WorkflowExecutionCancelRequestedAttributes:
		f.add("reason", a.Reason)
		f.add("by", a.Identity)

	case history.WorkflowExecutionSignaledAttributes:
		f.add("signal", a.SignalName)
		f.add("by", a.Identity)
		f.add("input", PayloadPreview(a.Input))

	case history.WorkflowTaskScheduledAttributes:
		f.add("task_queue", a.TaskQueue)
		f.addInt("attempt", int64(a.Attempt))
		f.addDuration("start_to_close", a.StartToCloseTimout)

	case history.WorkflowTaskStartedAttributes:
		f.add("worker", a.Identity)
		f.addInt("scheduled", a.ScheduledEventID)

	case history.WorkflowTaskCompletedAttributes:
		f.add("worker", a.Identity)
		f.addInt("started", a.StartedEventID)
		f.add("sdk", sdkText(a.SDKName, a.SDKVersion))

	case history.WorkflowTaskFailedAttributes:
		f.add("cause", a.Cause.String())
		f.add("failure", failureText(a.Failure))
		f.add("worker", a.Identity)

	case history.WorkflowTaskTimedOutAttributes:
		f.add("kind", a.Kind.String())
		f.addInt("started", a.StartedEventID)

	case history.ActivityTaskScheduledAttributes:
		f.add("activity", a.ActivityType)
		f.add("id", a.ActivityID)
		f.add("task_queue", a.TaskQueue)
		f.addDuration("start_to_close", a.StartToCloseTimeout)
		f.addDuration("schedule_to_close", a.ScheduleToCloseTimeout)
		f.addDuration("heartbeat", a.HeartbeatTimeout)
		f.add("input", PayloadPreview(a.Input))

	case history.ActivityTaskStartedAttributes:
		f.addInt("scheduled", a.ScheduledEventID)
		f.addInt("attempt", int64(a.Attempt))
		f.add("worker", a.Identity)
		f.add("last_failure", failureText(a.LastFailure))

	case history.ActivityTaskCompletedAttributes:
		f.addInt("scheduled", a.ScheduledEventID)
		f.add("result", PayloadPreview(a.Result))

	case history.ActivityTaskFailedAttributes:
		f.addInt("scheduled", a.ScheduledEventID)
		f.add("failure", failureText(a.Failure))
		f.add("retry_state", retryStateText(a.RetryState))

	case history.ActivityTaskTimedOutAttributes:
		f.addInt("scheduled", a.ScheduledEventID)
		f.add("kind", a.Kind.String())
		f.add("retry_state", retryStateText(a.RetryState))
		f.add("last_heartbeat", PayloadPreview(a.LastHeartbeatDetails))

	case history.ActivityTaskCancelRequestedAttributes:
		f.addInt("scheduled", a.ScheduledEventID)

	case history.ActivityTaskCanceledAttributes:
		f.addInt("scheduled", a.ScheduledEventID)
		f.add("details", PayloadPreview(a.Details))

	case history.TimerStartedAttributes:
		f.add("timer", a.TimerID)
		f.addDuration("fires_in", a.StartToFireTimeout)

	case history.TimerFiredAttributes:
		f.add("timer", a.TimerID)
		f.addInt("started", a.StartedEventID)

	case history.TimerCanceledAttributes:
		f.add("timer", a.TimerID)
		f.addInt("started", a.StartedEventID)

	case history.MarkerRecordedAttributes:
		f.add("marker", a.MarkerName)
		f.add("id", a.MarkerID)
		f.add("details", PayloadPreview(a.Details))
		f.add("failure", failureText(a.Failure))
	}
	return f.String()
}

// failureText renders an application error compactly.
func failureText(e *skald.ApplicationError) string {
	if e == nil {
		return ""
	}
	s := e.Error()
	s = strings.Join(strings.Fields(s), " ")
	if e.NonRetryable {
		s += " (non-retryable)"
	}
	return TruncateMiddle(s, 120)
}

func retryStateText(rs history.RetryState) string {
	// The zero value is the uninteresting one; printing it on every terminal
	// event would be noise on the lines that matter most.
	if rs == 0 {
		return ""
	}
	return rs.String()
}

func sdkText(name, version string) string {
	switch {
	case name == "":
		return ""
	case version == "":
		return name
	}
	return name + "/" + version
}

// shortID abbreviates a run ID for inline display. Run IDs are UUIDs and the
// first eight characters distinguish them within any one workflow.
func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
