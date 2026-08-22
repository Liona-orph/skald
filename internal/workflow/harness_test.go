package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// The harness below is a deliberately small stand-in for the server: it applies
// a command batch to a history exactly the way internal/engine does, and nothing
// else. Two reasons it earns its keep rather than being a mock to distrust:
//
//   - It makes a replay test readable. "Run a task, complete the activity, run
//     another task" is three lines instead of thirty events built by hand.
//   - Every history it produces is passed through history.Validate, so a fixture
//     that the real engine would never write fails the test that uses it.
//
// The real engine is exercised end to end in pkg/worker's integration tests, so
// this harness is a convenience, never the only thing being trusted.

const (
	testNamespace = "default"
	testQueue     = "test-queue"
	testType      = "TestWorkflow"
)

type driver struct {
	t    *testing.T
	exec *Executor

	events history.History
	now    time.Time

	lastTaskCompletedID int64
	// activityIDs maps a user-visible activity id to its scheduling event, the
	// way the engine's mutable state does.
	activityIDs map[string]int64
	activityRun map[string]int64 // activity id -> started event id
	timerIDs    map[string]int64
	seed        int64
}

func newDriver(t *testing.T, exec *Executor, input *skald.Payload) *driver {
	t.Helper()
	d := &driver{
		t:           t,
		exec:        exec,
		now:         time.Date(2024, time.June, 1, 12, 0, 0, 0, time.UTC),
		activityIDs: map[string]int64{},
		activityRun: map[string]int64{},
		timerIDs:    map[string]int64{},
		seed:        99,
	}
	d.add(history.WorkflowExecutionStartedAttributes{
		WorkflowType:        testType,
		TaskQueue:           testQueue,
		Input:               input,
		Attempt:             1,
		RandomnessSeed:      d.seed,
		FirstExecutionRunID: "run-1",
	})
	exec.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
	return d
}

func (d *driver) add(attrs history.Attributes) history.Event {
	d.t.Helper()
	// Every event advances the clock, which keeps history.Validate's monotonic
	// timestamp rule satisfied and gives workflow.Now something to move.
	d.now = d.now.Add(time.Second)
	ev := history.Event{ID: int64(len(d.events) + 1), Time: d.now, Attrs: attrs}
	d.events = append(d.events, ev)
	return ev
}

func (d *driver) history() history.History {
	d.t.Helper()
	require.NoError(d.t, d.events.Validate(), "the harness built a history the engine would reject")
	return append(history.History(nil), d.events...)
}

// runTask schedules and starts a workflow task, hands it to the executor, and
// applies the resulting commands.
func (d *driver) runTask() TaskResult {
	d.t.Helper()
	res, err := d.runTaskErr()
	require.NoError(d.t, err)
	return res
}

func (d *driver) runTaskErr() (TaskResult, error) {
	d.t.Helper()
	d.add(history.WorkflowTaskScheduledAttributes{TaskQueue: testQueue, Attempt: 1})
	scheduled := d.events.LastEventID()
	started := d.add(history.WorkflowTaskStartedAttributes{ScheduledEventID: scheduled, Identity: "test"})

	res, err := d.exec.ProcessTask(api.WorkflowTask{
		Namespace:        testNamespace,
		Execution:        skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"},
		WorkflowType:     testType,
		TaskQueue:        testQueue,
		ScheduledEventID: scheduled,
		StartedEventID:   started.ID,
		Attempt:          1,
		History:          d.history(),
	})
	if err != nil {
		return TaskResult{}, err
	}
	d.applyCommands(scheduled, started.ID, res.Commands)
	return res, nil
}

// applyCommands mirrors execution.MutableState.CompleteWorkflowTask.
func (d *driver) applyCommands(scheduledID, startedID int64, cmds []history.Command) {
	d.t.Helper()
	require.NoError(d.t, history.ValidateBatch(cmds))
	completed := d.add(history.WorkflowTaskCompletedAttributes{
		ScheduledEventID: scheduledID, StartedEventID: startedID, Identity: "test",
	})
	d.lastTaskCompletedID = completed.ID

	for _, cmd := range cmds {
		switch cmd.Type {
		case history.CommandTypeScheduleActivityTask:
			c := cmd.ScheduleActivity
			ev := d.add(history.ActivityTaskScheduledAttributes{
				ActivityID:                   c.ActivityID,
				ActivityType:                 c.ActivityType,
				TaskQueue:                    testQueue,
				Input:                        c.Input,
				RetryPolicy:                  c.RetryPolicy,
				StartToCloseTimeout:          c.StartToCloseTimeout,
				ScheduleToCloseTimeout:       c.ScheduleToCloseTimeout,
				WorkflowTaskCompletedEventID: completed.ID,
			})
			d.activityIDs[c.ActivityID] = ev.ID
		case history.CommandTypeRequestCancelActivityTask:
			d.add(history.ActivityTaskCancelRequestedAttributes{
				ScheduledEventID:             cmd.RequestCancelActivity.ScheduledEventID,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		case history.CommandTypeStartTimer:
			c := cmd.StartTimer
			ev := d.add(history.TimerStartedAttributes{
				TimerID:                      c.TimerID,
				StartToFireTimeout:           c.StartToFireTimeout,
				WorkflowTaskCompletedEventID: completed.ID,
			})
			d.timerIDs[c.TimerID] = ev.ID
		case history.CommandTypeCancelTimer:
			startedEventID := cmd.CancelTimer.StartedEventID
			d.add(history.TimerCanceledAttributes{
				TimerID:                      d.timerName(startedEventID),
				StartedEventID:               startedEventID,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		case history.CommandTypeRecordMarker:
			c := cmd.RecordMarker
			d.add(history.MarkerRecordedAttributes{
				MarkerName:                   c.MarkerName,
				MarkerID:                     c.MarkerID,
				Details:                      c.Details,
				Failure:                      c.Failure,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		case history.CommandTypeCompleteWorkflowExecution:
			d.add(history.WorkflowExecutionCompletedAttributes{
				Result:                       cmd.CompleteWorkflow.Result,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		case history.CommandTypeFailWorkflowExecution:
			d.add(history.WorkflowExecutionFailedAttributes{
				Failure:                      cmd.FailWorkflow.Failure,
				WorkflowTaskCompletedEventID: completed.ID,
				RetryState:                   history.RetryStateRetryPolicyNotSet,
			})
		case history.CommandTypeCancelWorkflowExecution:
			d.add(history.WorkflowExecutionCanceledAttributes{
				Details:                      cmd.CancelWorkflow.Details,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		case history.CommandTypeContinueAsNewWorkflow:
			c := cmd.ContinueAsNew
			d.add(history.WorkflowExecutionContinuedAsNewAttributes{
				NewRunID:                     "run-2",
				WorkflowType:                 orDefault(c.WorkflowType, testType),
				TaskQueue:                    orDefault(c.TaskQueue, testQueue),
				Input:                        c.Input,
				WorkflowTaskCompletedEventID: completed.ID,
			})
		default:
			d.t.Fatalf("harness does not apply %s", cmd.Type)
		}
	}
}

func (d *driver) timerName(startedEventID int64) string {
	for name, id := range d.timerIDs {
		if id == startedEventID {
			return name
		}
	}
	d.t.Fatalf("no timer started by event %d", startedEventID)
	return ""
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// startActivity writes the started event the engine writes on first dispatch.
func (d *driver) startActivity(activityID string) {
	d.t.Helper()
	scheduledID, ok := d.activityIDs[activityID]
	require.Truef(d.t, ok, "activity %q was never scheduled", activityID)
	ev := d.add(history.ActivityTaskStartedAttributes{ScheduledEventID: scheduledID, Attempt: 1})
	d.activityRun[activityID] = ev.ID
}

func (d *driver) completeActivity(activityID string, result any) {
	d.t.Helper()
	if _, started := d.activityRun[activityID]; !started {
		d.startActivity(activityID)
	}
	d.add(history.ActivityTaskCompletedAttributes{
		ScheduledEventID: d.activityIDs[activityID],
		StartedEventID:   d.activityRun[activityID],
		Result:           skald.MustPayload(result),
	})
}

func (d *driver) failActivity(activityID string, failure *skald.ApplicationError) {
	d.t.Helper()
	if _, started := d.activityRun[activityID]; !started {
		d.startActivity(activityID)
	}
	d.add(history.ActivityTaskFailedAttributes{
		ScheduledEventID: d.activityIDs[activityID],
		StartedEventID:   d.activityRun[activityID],
		Failure:          failure,
		RetryState:       history.RetryStateNonRetryableFailure,
	})
}

func (d *driver) fireTimer(timerID string) {
	d.t.Helper()
	startedID, ok := d.timerIDs[timerID]
	require.Truef(d.t, ok, "timer %q was never started", timerID)
	d.add(history.TimerFiredAttributes{TimerID: timerID, StartedEventID: startedID})
}

func (d *driver) signal(name string, input any) {
	d.t.Helper()
	d.add(history.WorkflowExecutionSignaledAttributes{SignalName: name, Input: skald.MustPayload(input)})
}

func (d *driver) requestCancel() {
	d.t.Helper()
	d.add(history.WorkflowExecutionCancelRequestedAttributes{Reason: "test"})
}

// failLastTask records that the task the executor just answered never completed,
// which is what a worker crash between "produce commands" and "respond" leaves
// behind.
func (d *driver) taskTimedOut(scheduledID, startedID int64) {
	d.t.Helper()
	d.add(history.WorkflowTaskTimedOutAttributes{
		ScheduledEventID: scheduledID,
		StartedEventID:   startedID,
		Kind:             skald.TimeoutStartToClose,
	})
}

// ---------------------------------------------------------------------------
// Executor construction
// ---------------------------------------------------------------------------

func newTestExecutor(t *testing.T, fn WorkflowFunc) *Executor {
	t.Helper()
	exec, err := NewExecutor(ExecutorOptions{Fn: fn, DeadlockTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = exec.Close() })
	return exec
}

// commandTypes renders a batch for a readable assertion.
func commandTypes(cmds []history.Command) []string {
	out := make([]string, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, describeCommand(c))
	}
	return out
}

func decodeString(t *testing.T, p *skald.Payload) string {
	t.Helper()
	var s string
	require.NoError(t, skald.JSONConverter{}.FromPayload(p, &s))
	return s
}

func decodeInt(t *testing.T, p *skald.Payload) int {
	t.Helper()
	var n int
	require.NoError(t, skald.JSONConverter{}.FromPayload(p, &n))
	return n
}
