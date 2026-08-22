package workflow

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/Liona-orph/skald/internal/clock"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// DefaultDeadlockTimeout bounds one call into workflow code.
//
// It is shorter than the engine's workflow task timeout on purpose: the worker
// wants to report "your workflow is stuck here, on these coroutines" before the
// server reports the far less useful "a worker took the task and never came
// back".
const DefaultDeadlockTimeout = 5 * time.Second

// WorkflowFunc is a registered workflow reduced to payloads.
//
// Keeping reflection out of this package is what makes the replay machinery
// testable without a registry: a test can hand it a closure and assert on the
// commands it produces.
type WorkflowFunc func(ctx Context, input *skald.Payload) (*skald.Payload, error)

// ExecutorOptions configures one workflow instance.
type ExecutorOptions struct {
	// Fn is the workflow implementation. Required.
	Fn WorkflowFunc
	// Converter encodes and decodes payloads. Defaults to JSON.
	Converter skald.DataConverter
	// Logger receives workflow-side log output; it is wrapped so that replayed
	// statements are dropped.
	Logger *slog.Logger
	// Clock is used only to enforce DeadlockTimeout, never by workflow code.
	Clock clock.Clock
	// DeadlockTimeout bounds a single dispatcher run.
	DeadlockTimeout time.Duration
	// ReplayOnly forces replay semantics for every task, which is what an
	// offline history check needs: it must never run a side effect or a local
	// activity for real.
	ReplayOnly bool
}

// Executor drives one workflow run: it feeds history events into the workflow,
// resolves futures as the corresponding events appear, and checks every command
// the workflow produces against what the history recorded.
//
// An Executor is stateful and must be used by one goroutine at a time. The
// engine guarantees at most one workflow task per execution is outstanding, and
// the sticky cache hands an instance to exactly one task handler at a time, so
// that constraint is satisfied without locking.
type Executor struct {
	opts ExecutorOptions

	dispatcher *Dispatcher
	env        *Environment

	// next is the index of the first history event that has not been applied.
	// It is what makes a warm instance cheap: the server always sends the whole
	// history, and this cursor is how much of it we have already lived through.
	next int
	// markerScan is how far preloadMarkers has read.
	markerScan int
	// batchPos counts matched commands within the batch currently being
	// verified, purely so a diagnostic can say "command 2 of the batch".
	batchPos int
	// lastEventID is the ID of the last event applied. It lets the worker
	// recognise a warm instance that has already lived through the task it is
	// being handed -- which happens when a response never reached the server --
	// and rebuild from history instead of replaying a lie.
	lastEventID int64

	input   *skald.Payload
	started bool
	// poisoned marks an instance that hit an error mid-task. Its coroutines may
	// be in an arbitrary state, so it must never serve another task.
	poisoned bool
}

// TaskResult is what one workflow task produced.
type TaskResult struct {
	// Commands is the batch to send back to the server, in order.
	Commands []history.Command
	// Finished reports whether the batch closes the execution.
	Finished bool
}

// NewExecutor builds a cold workflow instance.
func NewExecutor(opts ExecutorOptions) (*Executor, error) {
	if opts.Fn == nil {
		return nil, errors.New("workflow: executor requires a workflow function")
	}
	if opts.Converter == nil {
		opts.Converter = skald.JSONConverter{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	if opts.DeadlockTimeout <= 0 {
		opts.DeadlockTimeout = DefaultDeadlockTimeout
	}

	d := NewDispatcher(opts.Clock)
	env := newEnvironment(d, opts.Converter, opts.Logger)
	base := Background(d, env)
	rootCtx, cancel := WithCancel(base)
	env.rootCtx = rootCtx
	env.cancelRoot = cancel
	// The workflow function returning ends the execution. Nothing after that
	// point may append a command, so the dispatcher stops scheduling siblings
	// the moment the environment closes.
	d.SetStopCondition(func() bool { return env.closed })

	return &Executor{opts: opts, dispatcher: d, env: env}, nil
}

// Close unwinds every coroutine. It must be called for every executor that is
// dropped, including on eviction from the sticky cache, or the workflow's
// goroutines stay parked forever.
func (e *Executor) Close() error { return e.dispatcher.Close() }

// Environment exposes the run's environment, for tests and diagnostics.
func (e *Executor) Environment() *Environment { return e.env }

// NextEventIndex reports how much history the instance has consumed. A test
// uses it to prove that a warm instance really did skip the prefix.
func (e *Executor) NextEventIndex() int { return e.next }

// LastEventID is the ID of the last history event this instance applied.
func (e *Executor) LastEventID() int64 { return e.lastEventID }

// Poisoned reports whether the instance must be discarded.
func (e *Executor) Poisoned() bool { return e.poisoned }

// ProcessTask feeds a workflow task's history into the workflow and returns the
// commands it produced.
//
// A task whose history the instance has already seen in part is resumed from the
// cursor; a cold instance replays from event 1. The two paths differ only in how
// much of the loop below runs, which is exactly the property that lets a test
// evict the cache between every task and still get identical results.
func (e *Executor) ProcessTask(task api.WorkflowTask) (TaskResult, error) {
	if e.poisoned {
		return TaskResult{}, errors.New("workflow: executor was poisoned by an earlier failure and cannot be reused")
	}
	h := task.History
	if len(h) == 0 {
		return TaskResult{}, errors.New("workflow: workflow task carries no history")
	}
	e.preloadMarkers(h)

	for e.next < len(h) {
		ev := h[e.next]
		e.next++
		e.lastEventID = ev.ID

		if ev.Type() == history.EventTypeWorkflowTaskStarted {
			live := task.StartedEventID != 0 && ev.ID == task.StartedEventID
			e.env.advanceTime(ev.Time)
			if !live && !e.taskCompleted(h, e.next) {
				// The task this event started never completed: the worker died,
				// the task timed out, or it was failed. Whatever commands it
				// produced were never applied, so re-running the workflow here
				// would advance past decisions that left no trace. Skipping is
				// the only choice that keeps replay faithful; the retry's own
				// WorkflowTaskStarted will run it.
				continue
			}
			e.env.replaying = !live || e.opts.ReplayOnly
			if err := e.run(); err != nil {
				e.poisoned = true
				return TaskResult{}, e.decorate(err)
			}
			if live {
				return e.result(), nil
			}
			continue
		}

		if err := e.apply(ev); err != nil {
			e.poisoned = true
			return TaskResult{}, e.decorate(err)
		}
	}

	if task.StartedEventID != 0 {
		return TaskResult{}, fmt.Errorf(
			"workflow: history ends at event %d without the started event %d this task names",
			h.LastEventID(), task.StartedEventID)
	}
	// Offline replay: every command must have been accounted for by an event.
	if len(e.env.outstanding) > 0 {
		oc := e.env.outstanding[0]
		e.poisoned = true
		return TaskResult{}, e.decorate(&NonDeterminismError{
			EventID: h.LastEventID(),
			Actual:  describeCommand(oc.cmd),
			Origin:  oc.origin,
			Detail: fmt.Sprintf("replay produced %d command(s) that the recorded history does not contain; "+
				"the first is shown above", len(e.env.outstanding)),
		})
	}
	return e.result(), nil
}

// taskCompleted reports whether the event at index idx (the one right after a
// WorkflowTaskStarted) is a successful completion.
func (e *Executor) taskCompleted(h history.History, idx int) bool {
	return idx < len(h) && h[idx].Type() == history.EventTypeWorkflowTaskCompleted
}

// run executes workflow code until every coroutine is blocked.
func (e *Executor) run() error {
	if !e.started {
		e.started = true
		input := e.input
		e.dispatcher.NewCoroutine(e.env.rootCtx, "workflow-main", func(ctx Context) {
			result, err := e.opts.Fn(ctx, input)
			e.env.complete(result, err)
		})
	}
	// Cancellations that could not name a history event when they happened are
	// emitted first, so that their position in the batch is fixed.
	e.env.flushPendingCancels()
	deadline := e.opts.Clock.Now().Add(e.opts.DeadlockTimeout)
	return e.dispatcher.ExecuteUntilAllBlocked(deadline)
}

// result snapshots the commands the workflow has produced but the server has
// not yet turned into events. At the point this is called they are exactly the
// live batch: every earlier command was consumed by a matching history event.
func (e *Executor) result() TaskResult {
	cmds := make([]history.Command, 0, len(e.env.outstanding))
	for _, oc := range e.env.outstanding {
		cmds = append(cmds, oc.cmd)
	}
	finished := len(cmds) > 0 && cmds[len(cmds)-1].Type.Closing()
	return TaskResult{Commands: cmds, Finished: finished}
}

// preloadMarkers indexes every marker in the history.
//
// Markers are read by position in the workflow's own call sequence (side effect
// 3, version gate "use-v2"), not by history position, so they must be available
// before the code that asks for them runs. Scanning is incremental: a warm
// instance only looks at the events it has not seen.
func (e *Executor) preloadMarkers(h history.History) {
	for ; e.markerScan < len(h); e.markerScan++ {
		a, ok := history.AttributesAs[history.MarkerRecordedAttributes](h[e.markerScan])
		if !ok {
			continue
		}
		e.env.markers[markerKey(a.MarkerName, a.MarkerID)] = a
	}
}

// ---------------------------------------------------------------------------
// Event application
// ---------------------------------------------------------------------------

func (e *Executor) apply(ev history.Event) error {
	// Cancellation commands raised by an event are held back until the whole
	// task's history has been applied; see Environment.applyingHistory.
	e.env.applyingHistory = true
	defer func() { e.env.applyingHistory = false }()

	// Attributes come in value form from the engine and sometimes in pointer
	// form from hand-built test histories, so every read goes through
	// history.MustAttributes rather than a direct type switch on ev.Attrs.
	switch ev.Type() {
	case history.EventTypeWorkflowExecutionStarted:
		a := history.MustAttributes[history.WorkflowExecutionStartedAttributes](ev)
		e.applyStarted(ev, a)

	// --- command-derived events: matched against what the workflow produced ---
	case history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskCancelRequested,
		history.EventTypeTimerStarted,
		history.EventTypeMarkerRecorded,
		history.EventTypeWorkflowExecutionCompleted,
		history.EventTypeWorkflowExecutionFailed,
		history.EventTypeWorkflowExecutionCanceled,
		history.EventTypeWorkflowExecutionContinuedAsNew:
		return e.matchCommand(ev)

	case history.EventTypeTimerCanceled:
		if err := e.matchCommand(ev); err != nil {
			return err
		}
		a := history.MustAttributes[history.TimerCanceledAttributes](ev)
		e.env.timerCanceledByServer(a.StartedEventID)

	// --- externally caused events: they resolve futures ---
	case history.EventTypeActivityTaskCompleted:
		a := history.MustAttributes[history.ActivityTaskCompletedAttributes](ev)
		e.env.resolveActivity(a.ScheduledEventID, a.Result, nil)
	case history.EventTypeActivityTaskFailed:
		a := history.MustAttributes[history.ActivityTaskFailedAttributes](ev)
		e.env.resolveActivity(a.ScheduledEventID, nil, activityFailure(a.Failure))
	case history.EventTypeActivityTaskTimedOut:
		a := history.MustAttributes[history.ActivityTaskTimedOutAttributes](ev)
		e.env.resolveActivity(a.ScheduledEventID, nil,
			&skald.TimeoutError{Kind: a.Kind, LastHeartbeatDetails: a.LastHeartbeatDetails})
	case history.EventTypeActivityTaskCanceled:
		a := history.MustAttributes[history.ActivityTaskCanceledAttributes](ev)
		e.env.resolveActivity(a.ScheduledEventID, nil, &skald.CanceledError{Details: a.Details})
	case history.EventTypeTimerFired:
		a := history.MustAttributes[history.TimerFiredAttributes](ev)
		e.env.fireTimer(a.StartedEventID)
	case history.EventTypeWorkflowExecutionSignaled:
		a := history.MustAttributes[history.WorkflowExecutionSignaledAttributes](ev)
		e.env.deliverSignal(a.SignalName, a.Input)
	case history.EventTypeWorkflowExecutionCancelRequested:
		e.env.cancelRoot()

	// --- events the workflow does not observe ---
	case history.EventTypeWorkflowTaskCompleted:
		// The command-derived events that follow belong to one batch, and the
		// diagnostic wants to name a position within it.
		e.batchPos = 0

	case history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskFailed,
		history.EventTypeWorkflowTaskTimedOut,
		history.EventTypeActivityTaskStarted,
		history.EventTypeWorkflowExecutionTerminated,
		history.EventTypeWorkflowExecutionTimedOut:
		// Nothing: these change server-side state only. A terminated or timed
		// out execution runs no more workflow code by definition.

	default:
		return fmt.Errorf("workflow: history event %d has type %s, which this SDK build does not understand; "+
			"refusing to replay a history it cannot fully interpret", ev.ID, ev.Type())
	}
	return nil
}

func (e *Executor) applyStarted(ev history.Event, a history.WorkflowExecutionStartedAttributes) {
	e.input = a.Input
	// Namespace and Execution come from the task, not the history, so they are
	// carried across rather than reset. Losing them would strip the identity out
	// of every diagnostic this run ever produces.
	e.env.info = Info{
		Namespace:               e.env.info.Namespace,
		Execution:               e.env.info.Execution,
		WorkflowType:            a.WorkflowType,
		TaskQueue:               a.TaskQueue,
		Attempt:                 a.Attempt,
		FirstExecutionRunID:     a.FirstExecutionRunID,
		ContinuedExecutionRunID: a.ContinuedExecutionRunID,
		CronSchedule:            a.CronSchedule,
		StartedAt:               ev.Time,
		RunTimeout:              a.RunTimeout,
		ExecutionTimeout:        a.ExecutionTimeout,
		TaskTimeout:             a.TaskTimeout,
		Memo:                    a.Memo,
		SearchAttrs:             a.SearchAttrs,
	}
	e.env.advanceTime(ev.Time)
	// Two independent streams from one recorded seed: a workflow that starts
	// drawing random numbers in v2 of its code must not shift the UUIDs v1
	// already handed out to the outside world.
	e.env.rng = newDeterministicRand(a.RandomnessSeed)
	e.env.uuidRng = newDeterministicRand(a.RandomnessSeed ^ 0x5deece66d)
}

// SetExecutionInfo fills in the identity fields that live on the task rather
// than in the history.
func (e *Executor) SetExecutionInfo(namespace string, exec skald.WorkflowExecution) {
	e.env.info.Namespace = namespace
	e.env.info.Execution = exec
}

// advanceTime moves the workflow's clock forward. It never moves backwards: a
// history whose timestamps regress would otherwise make Now non-monotonic inside
// a single execution, which no workflow author expects.
func (env *Environment) advanceTime(t time.Time) {
	if t.After(env.now) {
		env.now = t
	}
}

func activityFailure(f *skald.ApplicationError) error {
	if f == nil {
		return skald.NewApplicationError("", "activity failed without a failure detail")
	}
	return f
}

// ---------------------------------------------------------------------------
// Non-determinism detection
// ---------------------------------------------------------------------------

// matchCommand pairs one command-derived history event with the head of the
// queue of commands the workflow has produced.
func (e *Executor) matchCommand(ev history.Event) error {
	if len(e.env.outstanding) == 0 {
		return &NonDeterminismError{
			EventID:  ev.ID,
			Expected: describeEventAsCommand(ev),
			Actual:   "nothing",
			Detail: "the recorded history contains an effect that the workflow code running now " +
				"never asked for; the code has fewer steps than the code that produced this history",
		}
	}
	oc := e.env.outstanding[0]
	if detail := commandMismatch(oc.cmd, ev); detail != "" {
		return &NonDeterminismError{
			EventID:      ev.ID,
			CommandIndex: e.commandIndex(),
			Expected:     describeEventAsCommand(ev),
			Actual:       describeCommand(oc.cmd),
			Origin:       oc.origin,
			Detail:       detail,
		}
	}
	e.env.outstanding = e.env.outstanding[1:]
	e.batchPos++
	if oc.bind != nil {
		oc.bind(ev)
	}
	return nil
}

// commandIndex is the position, within the command batch being verified, of the
// command that failed to match. It is the number a reader counts to in the
// workflow function to find the offending call.
func (e *Executor) commandIndex() int { return e.batchPos }

// decorate attaches run identity to an error raised from inside workflow code.
func (e *Executor) decorate(err error) error {
	var panicErr *CoroutinePanicError
	if errors.As(err, &panicErr) {
		// A non-determinism error raised from workflow code (a version gate that
		// no longer covers the recorded branch, a side effect that vanished)
		// travels as a panic because it has to unwind user frames. Unwrap it
		// here so the caller sees the specific diagnosis rather than "panic".
		if nd, ok := panicErr.Value.(*NonDeterminismError); ok {
			nd.WorkflowType = e.env.info.WorkflowType
			nd.Execution = e.env.info.Execution
			return nd
		}
	}
	var nd *NonDeterminismError
	if errors.As(err, &nd) {
		nd.WorkflowType = e.env.info.WorkflowType
		nd.Execution = e.env.info.Execution
	}
	return err
}

// NonDeterminismError reports that replay disagreed with the recorded history.
//
// The message is the whole point of this type. A worker that says "non-
// determinism detected" and stops has told an operator nothing; a worker that
// names the event, the expected effect, the effect the code produced instead and
// the call that produced it turns a multi-hour incident into a diff.
type NonDeterminismError struct {
	WorkflowType string
	Execution    skald.WorkflowExecution
	EventID      int64
	CommandIndex int
	// Expected is the effect the history recorded.
	Expected string
	// Actual is the command the code produced in its place.
	Actual string
	// Origin names the workflow-side call that produced Actual.
	Origin string
	// Detail explains the specific disagreement.
	Detail string
}

func (e *NonDeterminismError) Error() string {
	var b strings.Builder
	b.WriteString("workflow: non-determinism detected")
	if e.WorkflowType != "" {
		fmt.Fprintf(&b, " in %s", e.WorkflowType)
	}
	if !e.Execution.IsZero() {
		fmt.Fprintf(&b, " (%s)", e.Execution)
	}
	if e.EventID > 0 {
		fmt.Fprintf(&b, " at history event %d", e.EventID)
	}
	if e.CommandIndex > 0 {
		fmt.Fprintf(&b, ", command %d of the batch", e.CommandIndex)
	}
	b.WriteString(":\n")
	if e.Expected != "" {
		fmt.Fprintf(&b, "  expected (recorded in history): %s\n", e.Expected)
	}
	if e.Actual != "" {
		fmt.Fprintf(&b, "  actual   (produced by replay):  %s\n", e.Actual)
	}
	if e.Origin != "" {
		fmt.Fprintf(&b, "  produced by: %s\n", e.Origin)
	}
	if e.Detail != "" {
		fmt.Fprintf(&b, "  %s\n", e.Detail)
	}
	b.WriteString("  The workflow code running now does not make the same decisions as the code that\n")
	b.WriteString("  wrote this history. Usual causes: an activity, timer or side effect was added,\n")
	b.WriteString("  removed or reordered; a map range, time.Now or math/rand leaked into a decision;\n")
	b.WriteString("  a behaviour change was deployed without a workflow.GetVersion gate.\n")
	b.WriteString("  Skald will not advance this execution while the disagreement stands, so rolling\n")
	b.WriteString("  the worker back is a complete fix with no data loss.")
	return b.String()
}

// commandMismatch returns "" when cmd is the command that produced ev, and a
// human explanation otherwise.
func commandMismatch(cmd history.Command, ev history.Event) string {
	want := commandTypeForEvent(ev.Type())
	if cmd.Type != want {
		return fmt.Sprintf("history event %d is a %s, which is produced by a %s command, but the workflow produced a %s command",
			ev.ID, ev.Type(), want, cmd.Type)
	}
	switch ev.Type() {
	case history.EventTypeActivityTaskScheduled:
		a := history.MustAttributes[history.ActivityTaskScheduledAttributes](ev)
		c := cmd.ScheduleActivity
		if c.ActivityID != a.ActivityID {
			return fmt.Sprintf("activity id %q was recorded, the workflow asked for %q; "+
				"activity ids are assigned by a counter, so a mismatch means the number of "+
				"activities scheduled before this point changed", a.ActivityID, c.ActivityID)
		}
		if c.ActivityType != a.ActivityType {
			return fmt.Sprintf("activity %q ran %q, the workflow now asks for %q",
				a.ActivityID, a.ActivityType, c.ActivityType)
		}
	case history.EventTypeTimerStarted:
		a := history.MustAttributes[history.TimerStartedAttributes](ev)
		c := cmd.StartTimer
		if c.TimerID != a.TimerID {
			return fmt.Sprintf("timer id %q was recorded, the workflow asked for %q", a.TimerID, c.TimerID)
		}
		if c.StartToFireTimeout != a.StartToFireTimeout {
			return fmt.Sprintf("timer %q was recorded with duration %s, the workflow asked for %s",
				a.TimerID, a.StartToFireTimeout, c.StartToFireTimeout)
		}
	case history.EventTypeMarkerRecorded:
		a := history.MustAttributes[history.MarkerRecordedAttributes](ev)
		c := cmd.RecordMarker
		if c.MarkerName != a.MarkerName {
			return fmt.Sprintf("marker %q was recorded, the workflow produced %q", a.MarkerName, c.MarkerName)
		}
		if c.MarkerID != a.MarkerID {
			return fmt.Sprintf("%s marker %q was recorded, the workflow produced %q",
				a.MarkerName, a.MarkerID, c.MarkerID)
		}
	case history.EventTypeActivityTaskCancelRequested:
		a := history.MustAttributes[history.ActivityTaskCancelRequestedAttributes](ev)
		if cmd.RequestCancelActivity.ScheduledEventID != a.ScheduledEventID {
			return fmt.Sprintf("a cancel was recorded for the activity scheduled by event %d, "+
				"the workflow cancelled the one scheduled by event %d",
				a.ScheduledEventID, cmd.RequestCancelActivity.ScheduledEventID)
		}
	case history.EventTypeTimerCanceled:
		a := history.MustAttributes[history.TimerCanceledAttributes](ev)
		if cmd.CancelTimer.StartedEventID != a.StartedEventID {
			return fmt.Sprintf("a cancel was recorded for the timer started by event %d, "+
				"the workflow cancelled the one started by event %d",
				a.StartedEventID, cmd.CancelTimer.StartedEventID)
		}
	}
	return ""
}

// commandTypeForEvent maps a command-derived event back to the command that
// must have produced it.
func commandTypeForEvent(t history.EventType) history.CommandType {
	switch t {
	case history.EventTypeActivityTaskScheduled:
		return history.CommandTypeScheduleActivityTask
	case history.EventTypeActivityTaskCancelRequested:
		return history.CommandTypeRequestCancelActivityTask
	case history.EventTypeTimerStarted:
		return history.CommandTypeStartTimer
	case history.EventTypeTimerCanceled:
		return history.CommandTypeCancelTimer
	case history.EventTypeMarkerRecorded:
		return history.CommandTypeRecordMarker
	case history.EventTypeWorkflowExecutionCompleted:
		return history.CommandTypeCompleteWorkflowExecution
	case history.EventTypeWorkflowExecutionFailed:
		return history.CommandTypeFailWorkflowExecution
	case history.EventTypeWorkflowExecutionCanceled:
		return history.CommandTypeCancelWorkflowExecution
	case history.EventTypeWorkflowExecutionContinuedAsNew:
		return history.CommandTypeContinueAsNewWorkflow
	}
	return history.CommandTypeUnspecified
}

// describeCommand renders a command the way a reader would recognise it in code.
func describeCommand(cmd history.Command) string {
	switch cmd.Type {
	case history.CommandTypeScheduleActivityTask:
		c := cmd.ScheduleActivity
		return fmt.Sprintf("ScheduleActivityTask activity_id=%q activity_type=%q", c.ActivityID, c.ActivityType)
	case history.CommandTypeRequestCancelActivityTask:
		return fmt.Sprintf("RequestCancelActivityTask scheduled_event_id=%d", cmd.RequestCancelActivity.ScheduledEventID)
	case history.CommandTypeStartTimer:
		c := cmd.StartTimer
		return fmt.Sprintf("StartTimer timer_id=%q duration=%s", c.TimerID, c.StartToFireTimeout)
	case history.CommandTypeCancelTimer:
		return fmt.Sprintf("CancelTimer started_event_id=%d", cmd.CancelTimer.StartedEventID)
	case history.CommandTypeRecordMarker:
		c := cmd.RecordMarker
		return fmt.Sprintf("RecordMarker name=%q id=%q", c.MarkerName, c.MarkerID)
	case history.CommandTypeCompleteWorkflowExecution:
		return "CompleteWorkflowExecution"
	case history.CommandTypeFailWorkflowExecution:
		return fmt.Sprintf("FailWorkflowExecution failure=%q", cmd.FailWorkflow.Failure.Error())
	case history.CommandTypeCancelWorkflowExecution:
		return "CancelWorkflowExecution"
	case history.CommandTypeContinueAsNewWorkflow:
		return fmt.Sprintf("ContinueAsNewWorkflow workflow_type=%q", cmd.ContinueAsNew.WorkflowType)
	}
	return cmd.Type.String()
}

// describeEventAsCommand renders a history event in the same vocabulary as
// describeCommand, so the two lines of a diagnostic line up column for column.
func describeEventAsCommand(ev history.Event) string {
	switch ev.Type() {
	case history.EventTypeActivityTaskScheduled:
		a := history.MustAttributes[history.ActivityTaskScheduledAttributes](ev)
		return fmt.Sprintf("ScheduleActivityTask activity_id=%q activity_type=%q", a.ActivityID, a.ActivityType)
	case history.EventTypeActivityTaskCancelRequested:
		a := history.MustAttributes[history.ActivityTaskCancelRequestedAttributes](ev)
		return fmt.Sprintf("RequestCancelActivityTask scheduled_event_id=%d", a.ScheduledEventID)
	case history.EventTypeTimerStarted:
		a := history.MustAttributes[history.TimerStartedAttributes](ev)
		return fmt.Sprintf("StartTimer timer_id=%q duration=%s", a.TimerID, a.StartToFireTimeout)
	case history.EventTypeTimerCanceled:
		a := history.MustAttributes[history.TimerCanceledAttributes](ev)
		return fmt.Sprintf("CancelTimer started_event_id=%d", a.StartedEventID)
	case history.EventTypeMarkerRecorded:
		a := history.MustAttributes[history.MarkerRecordedAttributes](ev)
		return fmt.Sprintf("RecordMarker name=%q id=%q", a.MarkerName, a.MarkerID)
	case history.EventTypeWorkflowExecutionCompleted:
		return "CompleteWorkflowExecution"
	case history.EventTypeWorkflowExecutionFailed:
		return "FailWorkflowExecution"
	case history.EventTypeWorkflowExecutionCanceled:
		return "CancelWorkflowExecution"
	case history.EventTypeWorkflowExecutionContinuedAsNew:
		return "ContinueAsNewWorkflow"
	}
	return ev.Type().String()
}

// FailureCause classifies a workflow task failure for the server.
//
// The distinction is operational, not cosmetic: NonDeterminism is permanent and
// tells an operator to roll back, while a panic is a bug that may be fixed by
// the next deploy and is worth retrying.
func FailureCause(err error) history.WorkflowTaskFailedCause {
	var nd *NonDeterminismError
	if errors.As(err, &nd) {
		return history.WorkflowTaskFailedCauseNonDeterminism
	}
	var panicErr *CoroutinePanicError
	if errors.As(err, &panicErr) {
		return history.WorkflowTaskFailedCauseWorkflowPanic
	}
	var deadlock *DeadlockError
	if errors.As(err, &deadlock) {
		return history.WorkflowTaskFailedCauseWorkflowPanic
	}
	return history.WorkflowTaskFailedCauseUnspecified
}

// FailureDetail converts an executor error into the failure the server records.
func FailureDetail(err error) *skald.ApplicationError {
	var panicErr *CoroutinePanicError
	if errors.As(err, &panicErr) {
		return &skald.ApplicationError{
			Type:       "WorkflowPanic",
			Message:    panicErr.Error(),
			StackTrace: panicErr.Stack,
		}
	}
	var nd *NonDeterminismError
	if errors.As(err, &nd) {
		return &skald.ApplicationError{Type: "NonDeterminismError", Message: nd.Error(), NonRetryable: true}
	}
	return skald.AsApplicationError(err)
}

// ---------------------------------------------------------------------------
// Sticky execution cache
// ---------------------------------------------------------------------------

// Cache keeps warm workflow instances keyed by run ID.
//
// A hit turns a task that would replay a thousand events into one that applies
// three. A miss costs a full replay and nothing else -- which is the invariant
// the whole design protects, and which the test suite proves by evicting the
// cache between every single task and asserting identical results.
//
// Instances are *taken* out of the cache while in use rather than borrowed. An
// instance evicted while a task was running would have its coroutines unwound
// underneath the goroutine using them; taking removes that possibility entirely
// instead of guarding it with a lock that would have to be held across user code.
// The LRU is used *without* an eviction callback on purpose. Removing a key
// fires that callback, so a Take -- which is a removal -- would close the very
// instance it is handing to a caller. Every place an instance genuinely dies is
// therefore explicit below, which is also the only way to tell "taken for use"
// apart from "displaced by something newer".
type Cache struct {
	mu   sync.Mutex
	size int
	lru  *lru.Cache[string, *Executor]
}

// NewCache returns a sticky cache holding at most size instances.
func NewCache(size int) (*Cache, error) {
	if size <= 0 {
		size = 1
	}
	c, err := lru.New[string, *Executor](size)
	if err != nil {
		return nil, fmt.Errorf("workflow: sticky cache: %w", err)
	}
	return &Cache{size: size, lru: c}, nil
}

// Take removes and returns the instance for runID, if one is cached. The
// instance is not closed: the caller now owns it and is expected to Put it back
// or close it.
func (c *Cache) Take(runID string) (*Executor, bool) {
	if c == nil || c.lru == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lru.Get(runID)
	if !ok {
		return nil, false
	}
	c.lru.Remove(runID)
	return e, true
}

// Put returns an instance to the cache, closing anything it displaces.
func (c *Cache) Put(runID string, e *Executor) {
	if c == nil || c.lru == nil {
		_ = e.Close()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.lru.Peek(runID); ok && prev != e {
		c.lru.Remove(runID)
		_ = prev.Close()
	}
	// Make room explicitly. Letting the LRU drop the oldest entry silently
	// would leak that instance's coroutines: a cached workflow is not a plain
	// value, it owns goroutines that only Close releases.
	for c.lru.Len() >= c.size {
		key, victim, ok := c.lru.GetOldest()
		if !ok {
			break
		}
		c.lru.Remove(key)
		_ = victim.Close()
	}
	c.lru.Add(runID, e)
}

// Evict drops and closes the instance for runID.
func (c *Cache) Evict(runID string) {
	if c == nil || c.lru == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.lru.Peek(runID); ok {
		c.lru.Remove(runID)
		_ = e.Close()
	}
}

// Len reports how many instances are cached.
func (c *Cache) Len() int {
	if c == nil || c.lru == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// Clear closes every cached instance. Called on worker shutdown so that a
// stopped worker leaves no goroutines behind.
func (c *Cache) Clear() {
	if c == nil || c.lru == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		key, e, ok := c.lru.GetOldest()
		if !ok {
			return
		}
		c.lru.Remove(key)
		_ = e.Close()
	}
}
