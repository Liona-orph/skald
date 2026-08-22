package engine_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Liona-orph/skald/internal/engine"
	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// TestHappyPath walks one workflow from start to completion through an activity
// and asserts the exact event sequence. The sequence is the contract: an SDK
// replays it, an operator reads it, and a change to it is a change to the
// product, so it is pinned rather than sampled.
func TestHappyPath(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	runID := h.startWorkflow("order-1", func(r *api.StartWorkflowRequest) {
		r.Input = skald.MustPayload("hello")
	})

	// Event 1 and 2 exist before any worker appears: the workflow is durable
	// from the moment start returns.
	h.assertHistory("order-1", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
	)

	task := h.pollWorkflowTask()
	if task.WorkflowType != testType || task.Attempt != 1 {
		t.Fatalf("task = %+v", task)
	}
	if len(task.History) != 3 || task.History[2].ID != task.StartedEventID {
		t.Fatalf("a workflow task must carry the whole history through its started event, got %d events", len(task.History))
	}

	h.completeWorkflowTask(task, scheduleActivity("charge", "ChargeCard"))

	activity := h.pollActivityTask()
	if activity.ActivityID != "charge" || activity.Attempt != 1 {
		t.Fatalf("activity task = %+v", activity)
	}
	if err := h.eng.RespondActivityTaskCompleted(h.ctx(), api.RespondActivityTaskCompletedRequest{
		Namespace:        testNamespace,
		Execution:        activity.Execution,
		ScheduledEventID: activity.ScheduledEventID,
		Result:           skald.MustPayload("charged"),
	}); err != nil {
		t.Fatalf("RespondActivityTaskCompleted: %v", err)
	}

	second := h.pollWorkflowTask()
	h.completeWorkflowTask(second, completeWorkflow("done"))

	h.assertHistory("order-1", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskStarted,
		history.EventTypeActivityTaskCompleted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeWorkflowExecutionCompleted,
	)
	if got := h.status("order-1", runID); got != skald.StatusCompleted {
		t.Fatalf("status = %s, want COMPLETED", got)
	}
	// A closed execution leaves nothing behind in the timer index.
	if recs := h.store.timerRecords(); len(recs) != 0 {
		t.Fatalf("timers left armed after completion: %+v", recs)
	}
}

func TestStartValidationAndDeduplication(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	bad := []struct {
		name string
		req  api.StartWorkflowRequest
	}{
		{"no workflow id", api.StartWorkflowRequest{WorkflowType: testType, TaskQueue: testQueue}},
		{"no type", api.StartWorkflowRequest{WorkflowID: "w", TaskQueue: testQueue}},
		{"no queue", api.StartWorkflowRequest{WorkflowID: "w", WorkflowType: testType}},
		{"negative timeout", api.StartWorkflowRequest{
			WorkflowID: "w", WorkflowType: testType, TaskQueue: testQueue, RunTimeout: -time.Second}},
		{"unknown reuse policy", api.StartWorkflowRequest{
			WorkflowID: "w", WorkflowType: testType, TaskQueue: testQueue, ReusePolicy: "sometimes"}},
		{"bad retry policy", api.StartWorkflowRequest{
			WorkflowID: "w", WorkflowType: testType, TaskQueue: testQueue,
			RetryPolicy: &skald.RetryPolicy{BackoffCoefficient: 0.5}}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.eng.StartWorkflow(h.ctx(), tc.req)
			assertAPIError(t, err, api.CodeInvalidArgument)
		})
	}

	first, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "dedup", WorkflowType: testType,
		TaskQueue: testQueue, RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if !first.Started {
		t.Fatal("the first start did not report Started")
	}
	second, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "dedup", WorkflowType: testType,
		TaskQueue: testQueue, RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("retried start: %v", err)
	}
	if second.RunID != first.RunID {
		t.Fatalf("a retried start created run %s, want the original %s", second.RunID, first.RunID)
	}
	if second.Started {
		t.Fatal("a deduplicated start reported Started; a caller cannot tell it apart from a real one")
	}

	// A second open run of the same workflow ID is refused whatever the policy.
	_, err = h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "dedup", WorkflowType: testType, TaskQueue: testQueue,
	})
	assertAPIError(t, err, api.CodeAlreadyExists)
}

func TestReusePolicyRejectDuplicate(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	runID := h.startWorkflow("once", func(r *api.StartWorkflowRequest) {
		r.ReusePolicy = "RejectDuplicate"
	})
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, completeWorkflow(nil))
	if got := h.status("once", runID); got != skald.StatusCompleted {
		t.Fatalf("status = %s", got)
	}

	// The workflow id is an idempotency key for all time under this policy, so
	// even a closed run blocks a second start.
	_, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "once", WorkflowType: testType,
		TaskQueue: testQueue, ReusePolicy: "RejectDuplicate",
	})
	assertAPIError(t, err, api.CodeAlreadyExists)

	// The default policy allows a fresh run once the previous one closed.
	if id := h.startWorkflow("once"); id == runID {
		t.Fatal("the default reuse policy returned the closed run")
	}
}

func TestTerminateThenRestartWithReusePolicy(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	first := h.startWorkflow("singleton")
	second, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "singleton", WorkflowType: testType,
		TaskQueue: testQueue, ReusePolicy: "terminate_if_running", Identity: "operator",
	})
	if err != nil {
		t.Fatalf("StartWorkflow with TerminateIfRunning: %v", err)
	}
	if second.RunID == first {
		t.Fatal("TerminateIfRunning reused the running run")
	}
	if got := h.status("singleton", first); got != skald.StatusTerminated {
		t.Fatalf("predecessor status = %s, want TERMINATED", got)
	}
	// The predecessor's history explains what happened to it, rather than the
	// run simply disappearing from the visibility index.
	if got := h.lastEvent("singleton", first).Type(); got != history.EventTypeWorkflowExecutionTerminated {
		t.Fatalf("predecessor's last event = %s", got)
	}
}

// TestSignalDeliveredWhileTaskInFlight covers the buffering rule: a signal that
// lands while a worker holds a task must produce a new task once that worker
// answers, or it would sit unread in the history forever.
func TestSignalDeliveredWhileTaskInFlight(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("signals")

	task := h.pollWorkflowTask()
	if err := h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "signals",
		SignalName: "approve", Input: skald.MustPayload(true),
	}); err != nil {
		t.Fatalf("SignalWorkflow: %v", err)
	}

	// The signal is durable immediately, even though no workflow code has seen
	// it: "accepted by the server" means "will be delivered".
	h.assertHistory("signals", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowExecutionSignaled,
	)

	h.completeWorkflowTask(task)

	next, ok := h.pollWorkflowTaskMaybe()
	if !ok {
		t.Fatal("no workflow task was scheduled to deliver the buffered signal")
	}
	var sawSignal bool
	for _, ev := range next.History {
		if ev.Type() == history.EventTypeWorkflowExecutionSignaled {
			sawSignal = true
		}
	}
	if !sawSignal {
		t.Fatal("the follow-up task did not carry the signal")
	}
	h.completeWorkflowTask(next, completeWorkflow("approved"))
	if got := h.status("signals", runID); got != skald.StatusCompleted {
		t.Fatalf("status = %s", got)
	}
}

func TestSignalWithStart(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	req := api.SignalWithStartRequest{
		Start: api.StartWorkflowRequest{
			Namespace: testNamespace, WorkflowID: "cart-7",
			WorkflowType: testType, TaskQueue: testQueue,
		},
		SignalName:  "add_item",
		SignalInput: skald.MustPayload("widget"),
	}
	resp, err := h.eng.SignalWithStartWorkflow(h.ctx(), req)
	if err != nil {
		t.Fatalf("SignalWithStartWorkflow: %v", err)
	}
	if !resp.Started {
		t.Fatal("the first signal-with-start did not start the workflow")
	}
	// Start and signal are one transaction, so the first workflow task already
	// carries the signal: there is no window in which the workflow exists
	// without it.
	h.assertHistory("cart-7", resp.RunID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowExecutionSignaled,
		history.EventTypeWorkflowTaskScheduled,
	)

	again, err := h.eng.SignalWithStartWorkflow(h.ctx(), req)
	if err != nil {
		t.Fatalf("second SignalWithStartWorkflow: %v", err)
	}
	if again.Started {
		t.Fatal("the second signal-with-start started a second run")
	}
	if again.RunID != resp.RunID {
		t.Fatalf("second call targeted run %s, want %s", again.RunID, resp.RunID)
	}
	h.assertHistory("cart-7", resp.RunID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowExecutionSignaled,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowExecutionSignaled,
	)
}

func TestSignalClosedWorkflowIsRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("closed")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, completeWorkflow(nil))

	err := h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "closed", SignalName: "late",
	})
	assertAPIError(t, err, api.CodeFailedPrecondition)

	err = h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "no-such-workflow", SignalName: "x",
	})
	assertAPIError(t, err, api.CodeNotFound)
}

// TestCancellationIsCooperative shows the difference between cancel and
// terminate: cancel asks, terminate tells.
func TestCancellationIsCooperative(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("cancellable")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, scheduleActivity("work", "DoWork"))

	if err := h.eng.CancelWorkflow(h.ctx(), api.CancelWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "cancellable", Reason: "user asked",
	}); err != nil {
		t.Fatalf("CancelWorkflow: %v", err)
	}
	// The workflow is still running: it has been asked, not stopped.
	if got := h.status("cancellable", runID); got != skald.StatusRunning {
		t.Fatalf("status after cancel request = %s, want RUNNING", got)
	}

	// A repeated request is a no-op rather than a second event or an error.
	appendsBefore := h.store.appendCount()
	if err := h.eng.CancelWorkflow(h.ctx(), api.CancelWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "cancellable", Reason: "user asked again",
	}); err != nil {
		t.Fatalf("second CancelWorkflow: %v", err)
	}
	if got := h.store.appendCount(); got != appendsBefore {
		t.Fatalf("a duplicate cancel wrote to the store (%d appends, want %d)", got, appendsBefore)
	}

	next := h.pollWorkflowTask()
	h.completeWorkflowTask(next, cancelWorkflowCmd())
	if got := h.status("cancellable", runID); got != skald.StatusCanceled {
		t.Fatalf("status = %s, want CANCELED", got)
	}
}

func TestTerminateStopsImmediately(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("doomed")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, scheduleActivity("work", "DoWork"))

	if err := h.eng.TerminateWorkflow(h.ctx(), api.TerminateWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "doomed",
		Reason: "operator", Identity: "sre",
	}); err != nil {
		t.Fatalf("TerminateWorkflow: %v", err)
	}
	if got := h.status("doomed", runID); got != skald.StatusTerminated {
		t.Fatalf("status = %s, want TERMINATED", got)
	}
	// No workflow code runs, so the pending activity is simply abandoned and
	// its timers are gone.
	if recs := h.store.timerRecords(); len(recs) != 0 {
		t.Fatalf("timers survived termination: %+v", recs)
	}

	err := h.eng.TerminateWorkflow(h.ctx(), api.TerminateWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "doomed",
	})
	assertAPIError(t, err, api.CodeFailedPrecondition)
}

func TestUserTimerFires(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("sleeper")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, startTimer("nap", time.Hour))

	if recs := h.store.timerRecords(); len(recs) != 1 || recs[0].Kind != persistence.TimerKindUser {
		t.Fatalf("expected one user timer in the index, got %+v", recs)
	}

	h.advance(59 * time.Minute)
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("the timer fired an hour early")
	}
	h.advance(time.Minute)

	next, ok := h.pollWorkflowTaskMaybe()
	if !ok {
		t.Fatal("the timer did not wake the workflow")
	}
	h.completeWorkflowTask(next, completeWorkflow("awake"))
	h.assertHistory("sleeper", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeTimerStarted,
		history.EventTypeTimerFired,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeWorkflowExecutionCompleted,
	)
}

func TestWorkflowTaskTimeoutReschedules(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("stalled", func(r *api.StartWorkflowRequest) {
		r.TaskTimeout = 10 * time.Second
	})
	task := h.pollWorkflowTask()

	h.advance(10 * time.Second)

	// The stalled worker's task is gone and a replacement is pollable.
	h.assertHistory("stalled", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskTimedOut,
		history.EventTypeWorkflowTaskScheduled,
	)

	// The original worker finally answers and is refused: its commands were
	// computed against history that has moved on.
	err := h.eng.RespondWorkflowTaskCompleted(h.ctx(), api.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace, Execution: task.Execution,
		Commands: []history.Command{completeWorkflow("too late")},
	})
	assertAPIError(t, err, api.CodeFailedPrecondition)

	replacement, ok := h.pollWorkflowTaskMaybe()
	if !ok {
		t.Fatal("no replacement workflow task was dispatched")
	}
	if replacement.StartedEventID == task.StartedEventID {
		t.Fatal("the replacement task reused the timed-out started event")
	}
	h.completeWorkflowTask(replacement, completeWorkflow("recovered"))
}

func TestWorkflowTaskFailedReschedules(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("bad-deploy")
	task := h.pollWorkflowTask()

	if err := h.eng.RespondWorkflowTaskFailed(h.ctx(), api.RespondWorkflowTaskFailedRequest{
		Namespace: testNamespace, Execution: task.Execution,
		Cause:   history.WorkflowTaskFailedCauseNonDeterminism,
		Failure: skald.NewApplicationError("NonDeterminism", "history divergence at event 4"),
	}); err != nil {
		t.Fatalf("RespondWorkflowTaskFailed: %v", err)
	}
	// The task is rescheduled rather than the execution being failed: rolling
	// the deploy back is then enough to recover, with no data loss.
	h.assertHistory("bad-deploy", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskFailed,
		history.EventTypeWorkflowTaskScheduled,
	)
	if got := h.status("bad-deploy", runID); got != skald.StatusRunning {
		t.Fatalf("status = %s, want RUNNING", got)
	}
	if _, ok := h.pollWorkflowTaskMaybe(); !ok {
		t.Fatal("the failed task was not re-dispatched")
	}
}

func TestExecutionTimeoutClosesTheRun(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("slowpoke", func(r *api.StartWorkflowRequest) {
		r.RunTimeout = 30 * time.Second
	})
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, scheduleActivity("forever", "Forever", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = time.Hour
	}))

	h.advance(30 * time.Second)

	if got := h.status("slowpoke", runID); got != skald.StatusTimedOut {
		t.Fatalf("status = %s, want TIMED_OUT", got)
	}
	last := h.lastEvent("slowpoke", runID)
	attrs, ok := history.AttributesAs[history.WorkflowExecutionTimedOutAttributes](last)
	if !ok {
		t.Fatalf("last event is %s, want WorkflowExecutionTimedOut", last.Type())
	}
	if attrs.Kind != skald.TimeoutStartToClose {
		t.Fatalf("timeout kind = %s, want START_TO_CLOSE for a run timeout", attrs.Kind)
	}
}

func TestContinueAsNewChainsRuns(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	firstRun := h.startWorkflow("looping", func(r *api.StartWorkflowRequest) {
		r.Input = skald.MustPayload(1)
	})

	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, continueAsNew(2))

	if got := h.status("looping", firstRun); got != skald.StatusContinuedAsNew {
		t.Fatalf("first run status = %s, want CONTINUED_AS_NEW", got)
	}
	attrs, ok := history.AttributesAs[history.WorkflowExecutionContinuedAsNewAttributes](h.lastEvent("looping", firstRun))
	if !ok {
		t.Fatal("the first run does not end with a continue-as-new event")
	}
	if attrs.NewRunID == "" || attrs.NewRunID == "<pending>" {
		t.Fatalf("successor run id was never assigned: %q", attrs.NewRunID)
	}

	secondRun := attrs.NewRunID
	h.assertHistory("looping", secondRun,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
	)
	started, _ := h.store.history(testNamespace, "looping", secondRun).StartedAttributes()
	if started.ContinuedExecutionRunID != firstRun {
		t.Fatalf("successor links back to %q, want %q", started.ContinuedExecutionRunID, firstRun)
	}
	if started.FirstExecutionRunID != firstRun {
		t.Fatalf("successor's first execution run id = %q, want %q", started.FirstExecutionRunID, firstRun)
	}
	if started.RandomnessSeed == 0 {
		t.Fatal("the successor was given no randomness seed")
	}

	// The successor is a normal run: it is pollable and can finish.
	next := h.pollWorkflowTask()
	if next.Execution.RunID != secondRun {
		t.Fatalf("polled run %s, want the successor %s", next.Execution.RunID, secondRun)
	}
	h.completeWorkflowTask(next, completeWorkflow("finally"))
	if got := h.status("looping", secondRun); got != skald.StatusCompleted {
		t.Fatalf("successor status = %s", got)
	}
	if recs := h.store.timerRecords(); len(recs) != 0 {
		t.Fatalf("the successor fallback timer was not cleaned up: %+v", recs)
	}
}

func TestWorkflowRetryStartsANewRunAfterBackoff(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	firstRun := h.startWorkflow("flaky", func(r *api.StartWorkflowRequest) {
		r.RetryPolicy = &skald.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		}
	})

	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, failWorkflow(retryable("boom")))

	if got := h.status("flaky", firstRun); got != skald.StatusFailed {
		t.Fatalf("first run status = %s, want FAILED", got)
	}
	failed, _ := history.AttributesAs[history.WorkflowExecutionFailedAttributes](h.lastEvent("flaky", firstRun))
	if failed.RetryState != history.RetryStateInProgress {
		t.Fatalf("retry state = %s, want InProgress", failed.RetryState)
	}
	// The successor waits out the backoff: nothing new exists yet.
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("the retry run started before its backoff elapsed")
	}

	// The backoff is armed durably, one-sidedly jittered: never earlier than
	// the policy's stated interval, never more than the jitter fraction later.
	fireAt := h.timerOfKind(persistence.TimerKindWorkflowRetry).FireAt
	if lower := h.clk.Now().Add(10 * time.Second); fireAt.Before(lower) {
		t.Fatalf("retry armed for %s, earlier than the policy's interval %s", fireAt, lower)
	}
	if upper := h.clk.Now().Add(12 * time.Second); fireAt.After(upper) {
		t.Fatalf("retry armed for %s, later than the interval plus jitter %s", fireAt, upper)
	}

	// The successor exists immediately -- a backoff is a run that is not yet
	// pollable, not a run that does not exist -- and its first workflow task is
	// deferred by a timer on the successor itself. Parking the backoff on the
	// closed predecessor would not survive: a closed run keeps no timers.
	pending, ok := h.store.record(testNamespace, "flaky", "")
	if !ok || pending.RunID == firstRun {
		t.Fatalf("the successor was not created up front (current = %+v)", pending)
	}
	h.assertHistory("flaky", pending.RunID, history.EventTypeWorkflowExecutionStarted)
	if got := h.timerOfKind(persistence.TimerKindWorkflowRetry); got.RunID != pending.RunID {
		t.Fatalf("the backoff timer belongs to run %s, want the successor %s", got.RunID, pending.RunID)
	}

	h.advance(13 * time.Second)

	rec, ok := h.store.record(testNamespace, "flaky", "")
	if !ok || rec.RunID == firstRun {
		t.Fatalf("no successor run was created after the backoff (current = %+v)", rec)
	}
	h.assertHistory("flaky", rec.RunID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
	)
	started, _ := h.store.history(testNamespace, "flaky", rec.RunID).StartedAttributes()
	if started.Attempt != 2 {
		t.Fatalf("successor attempt = %d, want 2", started.Attempt)
	}
	if started.ContinuedExecutionRunID != firstRun {
		t.Fatalf("successor does not link back to the failed run")
	}

	// The second attempt fails too, and the policy is exhausted.
	next := h.pollWorkflowTask()
	h.completeWorkflowTask(next, failWorkflow(retryable("boom again")))
	last, _ := history.AttributesAs[history.WorkflowExecutionFailedAttributes](h.lastEvent("flaky", rec.RunID))
	if last.RetryState != history.RetryStateMaximumAttemptsReached {
		t.Fatalf("retry state = %s, want MaximumAttemptsReached", last.RetryState)
	}
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("a third run was started past the attempt limit")
	}
}

// TestVersionConflictIsRetried injects a competing writer between the read and
// the write, which is exactly what a second engine replica looks like.
func TestVersionConflictIsRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("contended")

	var once sync.Once
	h.store.setBeforeAppend(func(req persistence.AppendHistoryRequest, s *fakeStore) error {
		once.Do(func() {
			// Someone else committed first. The engine must notice, reload and
			// re-apply rather than clobbering the winner.
			s.bumpVersion(req.Namespace, req.WorkflowID, req.RunID)
		})
		return nil
	})

	task := h.pollWorkflowTask()
	h.store.setBeforeAppend(nil)

	if got := h.status("contended", runID); got != skald.StatusRunning {
		t.Fatalf("status = %s", got)
	}
	if task.StartedEventID == 0 {
		t.Fatal("the poll did not survive the version conflict")
	}
	// The retry re-read the history, so the task carries the real event
	// sequence and not a stale one.
	h.assertHistory("contended", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
	)
}

func TestVersionConflictGivesUpAfterBoundedAttempts(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("hopeless")

	// A writer that always wins: the engine must give up rather than spin.
	h.store.setBeforeAppend(func(req persistence.AppendHistoryRequest, s *fakeStore) error {
		s.bumpVersion(req.Namespace, req.WorkflowID, req.RunID)
		return nil
	})
	defer h.store.setBeforeAppend(nil)

	err := h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "hopeless", SignalName: "x",
	})
	assertAPIError(t, err, api.CodeVersionConflict)
}

func TestGetHistoryLongPollWakesOnWrite(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("watched")

	type result struct {
		resp api.GetHistoryResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := h.eng.GetHistory(context.Background(), api.GetHistoryRequest{
			Namespace: testNamespace, WorkflowID: "watched",
			FromEventID: 3, WaitForNew: true,
		})
		done <- result{resp, err}
	}()

	// The poll is parked on the notifier, not spinning: it has one timer armed
	// for the slow re-check and nothing else.
	waitFor(t, "the long poll to park", func() bool { return h.clk.NumTimers() >= 1 })
	select {
	case r := <-done:
		t.Fatalf("GetHistory returned before event 3 existed: %+v", r)
	case <-time.After(10 * time.Millisecond):
	}

	task := h.pollWorkflowTask() // writes event 3
	r := <-done
	if r.err != nil {
		t.Fatalf("GetHistory: %v", r.err)
	}
	if len(r.resp.Events) == 0 || r.resp.Events[0].ID != 3 {
		t.Fatalf("long poll returned %d events starting at %v", len(r.resp.Events), r.resp.Events)
	}
	if r.resp.NextEventID != 4 {
		t.Fatalf("NextEventID = %d, want 4", r.resp.NextEventID)
	}
	h.completeWorkflowTask(task, completeWorkflow(nil))
}

func TestGetHistoryDoesNotWaitOnClosedExecutions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("finished")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, completeWorkflow(nil))

	// Waiting for an event that will never come must return rather than block
	// until the client's deadline.
	resp, err := h.eng.GetHistory(h.ctx(), api.GetHistoryRequest{
		Namespace: testNamespace, WorkflowID: "finished",
		FromEventID: 100, WaitForNew: true,
	})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if resp.Status != skald.StatusCompleted {
		t.Fatalf("status = %s", resp.Status)
	}
}

func TestGetHistoryRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("patient")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		waitFor(t, "the poll to park", func() bool { return h.clk.NumTimers() >= 1 })
		cancel()
	}()
	_, err := h.eng.GetHistory(ctx, api.GetHistoryRequest{
		Namespace: testNamespace, WorkflowID: "patient",
		FromEventID: 99, WaitForNew: true,
	})
	if err == nil {
		t.Fatal("a cancelled long poll returned no error")
	}
	var apiErr *api.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is %T, want *api.Error", err)
	}
}

func TestDescribeAndList(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("described", func(r *api.StartWorkflowRequest) {
		r.Memo = map[string]string{"team": "payments"}
	})
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task,
		scheduleActivity("a1", "One"),
		startTimer("t1", time.Hour),
	)

	desc, err := h.eng.DescribeWorkflow(h.ctx(), testNamespace, "described", "")
	if err != nil {
		t.Fatalf("DescribeWorkflow: %v", err)
	}
	if desc.RunID != runID || desc.Status != skald.StatusRunning {
		t.Fatalf("describe = %+v", desc)
	}
	if len(desc.PendingActivities) != 1 || desc.PendingActivities[0].ActivityID != "a1" {
		t.Fatalf("pending activities = %+v", desc.PendingActivities)
	}
	if desc.PendingActivities[0].Started {
		t.Fatal("an activity nobody has polled reports as started")
	}
	if len(desc.PendingTimers) != 1 || desc.PendingTimers[0].TimerID != "t1" {
		t.Fatalf("pending timers = %+v", desc.PendingTimers)
	}
	if desc.Memo["team"] != "payments" {
		t.Fatalf("memo = %+v", desc.Memo)
	}

	h.startWorkflow("other")
	list, err := h.eng.ListWorkflows(h.ctx(), api.ListWorkflowsRequest{Namespace: testNamespace})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(list.Executions) != 2 {
		t.Fatalf("listed %d executions, want 2", len(list.Executions))
	}

	page, err := h.eng.ListWorkflows(h.ctx(), api.ListWorkflowsRequest{Namespace: testNamespace, PageSize: 1})
	if err != nil {
		t.Fatalf("ListWorkflows paged: %v", err)
	}
	if len(page.Executions) != 1 || page.NextPageToken == "" {
		t.Fatalf("paged list = %+v", page)
	}

	if _, err := h.eng.DescribeWorkflow(h.ctx(), testNamespace, "ghost", ""); err == nil {
		t.Fatal("describing a workflow that does not exist succeeded")
	} else {
		assertAPIError(t, err, api.CodeNotFound)
	}
}

func TestPollReturnsEmptyTaskOnTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	type result struct {
		task api.WorkflowTask
		err  error
	}
	done := make(chan result, 1)
	go func() {
		task, err := h.eng.PollWorkflowTask(context.Background(), api.PollWorkflowTaskRequest{
			Namespace: testNamespace, TaskQueue: testQueue,
		})
		done <- result{task, err}
	}()

	waitFor(t, "the poller to park", func() bool { return h.clk.NumTimers() >= 2 })
	h.clk.Advance(30 * time.Second)

	r := <-done
	if r.err != nil {
		t.Fatalf("an expired poll returned an error: %v", r.err)
	}
	if !r.task.Empty {
		t.Fatalf("an expired poll returned work: %+v", r.task)
	}
}

func TestConcurrentSignalsSerialisePerExecution(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("busy")

	const signals = 32
	var wg sync.WaitGroup
	wg.Add(signals)
	for i := 0; i < signals; i++ {
		go func(i int) {
			defer wg.Done()
			if err := h.eng.SignalWorkflow(context.Background(), api.SignalWorkflowRequest{
				Namespace: testNamespace, WorkflowID: "busy",
				SignalName: "tick", Input: skald.MustPayload(i),
			}); err != nil {
				t.Errorf("SignalWorkflow: %v", err)
			}
		}(i)
	}
	wg.Wait()

	events := h.store.history(testNamespace, "busy", runID)
	if err := events.Validate(); err != nil {
		t.Fatalf("concurrent signals produced an invalid history: %v", err)
	}
	var signaled int
	for _, ev := range events {
		if ev.Type() == history.EventTypeWorkflowExecutionSignaled {
			signaled++
		}
	}
	if signaled != signals {
		t.Fatalf("recorded %d signals, want %d", signaled, signals)
	}
	// A burst of signals collapses onto a single pending workflow task rather
	// than producing one replay per signal.
	var scheduled int
	for _, ev := range events {
		if ev.Type() == history.EventTypeWorkflowTaskScheduled {
			scheduled++
		}
	}
	if scheduled != 1 {
		t.Fatalf("%d workflow tasks were scheduled for %d signals, want 1", scheduled, signals)
	}
}

func TestCloseIsIdempotentAndLeaksNothing(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()

	h := newHarness(t)
	h.startWorkflow("shutdown")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, completeWorkflow(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.eng.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.eng.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d, baseline %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStaleWorkerIsRejectedByIdentity covers the harder ownership case: the
// original task timed out, another worker started its replacement, and the
// original worker finally answered.
func TestStaleWorkerIsRejectedByIdentity(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("two-workers", func(r *api.StartWorkflowRequest) {
		r.TaskTimeout = 10 * time.Second
	})
	first := h.pollWorkflowTask()

	h.advance(10 * time.Second)

	// A different worker picks up the replacement.
	if _, err := h.eng.PollWorkflowTask(h.ctx(), api.PollWorkflowTaskRequest{
		Namespace: testNamespace, TaskQueue: testQueue, Identity: "worker-2",
	}); err != nil {
		t.Fatalf("second poll: %v", err)
	}

	err := h.eng.RespondWorkflowTaskCompleted(h.ctx(), api.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace, Execution: first.Execution,
		Commands: []history.Command{completeWorkflow("stale")},
		Identity: "worker-1",
	})
	assertAPIError(t, err, api.CodeFailedPrecondition)

	// The worker that actually holds the task is still allowed to answer.
	if err := h.eng.RespondWorkflowTaskCompleted(h.ctx(), api.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace, Execution: first.Execution,
		Commands: []history.Command{completeWorkflow("fresh")},
		Identity: "worker-2",
	}); err != nil {
		t.Fatalf("the current holder was refused: %v", err)
	}
}

func TestGetHistoryPagination(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("paged")
	task := h.pollWorkflowTask()
	h.completeWorkflowTask(task, completeWorkflow(nil))

	page, err := h.eng.GetHistory(h.ctx(), api.GetHistoryRequest{
		Namespace: testNamespace, WorkflowID: "paged", MaxEvents: 2,
	})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(page.Events) != 2 || page.Events[0].ID != 1 {
		t.Fatalf("first page = %d events starting at %d", len(page.Events), page.Events[0].ID)
	}
	if page.NextEventID != 3 {
		t.Fatalf("NextEventID = %d, want 3", page.NextEventID)
	}

	rest, err := h.eng.GetHistory(h.ctx(), api.GetHistoryRequest{
		Namespace: testNamespace, WorkflowID: "paged", FromEventID: page.NextEventID,
	})
	if err != nil {
		t.Fatalf("GetHistory page 2: %v", err)
	}
	if rest.Events[0].ID != 3 {
		t.Fatalf("second page starts at %d, want 3", rest.Events[0].ID)
	}
	if rest.Status != skald.StatusCompleted {
		t.Fatalf("status = %s", rest.Status)
	}
}

// TestManyWorkflowsProgressConcurrently is the -race workhorse for the engine:
// independent executions must proceed in parallel and never interleave their
// writes. Every history is validated by the store on every append, so a lock
// that failed to serialise one execution shows up as a rejected write rather
// than as a subtle assertion failure.
func TestManyWorkflowsProgressConcurrently(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const workflows = 24
	var wg sync.WaitGroup
	wg.Add(workflows)
	for i := 0; i < workflows; i++ {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("parallel-%d", i)
			if _, err := h.eng.StartWorkflow(context.Background(), api.StartWorkflowRequest{
				Namespace: testNamespace, WorkflowID: id,
				WorkflowType: testType, TaskQueue: testQueue,
			}); err != nil {
				t.Errorf("start %s: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// Drain every workflow through an activity and a completion, with the
	// pollers running concurrently against a shared queue.
	var workers sync.WaitGroup
	workers.Add(4)
	done := make(chan struct{})
	var completed atomic.Int64
	for w := 0; w < 4; w++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				task, err := h.eng.PollWorkflowTask(ctx, api.PollWorkflowTaskRequest{
					Namespace: testNamespace, TaskQueue: testQueue, Identity: "w",
				})
				if err != nil || task.Empty {
					activity, err := h.eng.PollActivityTask(ctx, api.PollActivityTaskRequest{
						Namespace: testNamespace, TaskQueue: testQueue, Identity: "w",
					})
					if err != nil || activity.Empty {
						runtime.Gosched()
						continue
					}
					if err := h.eng.RespondActivityTaskCompleted(context.Background(), api.RespondActivityTaskCompletedRequest{
						Namespace: testNamespace, Execution: activity.Execution,
						ScheduledEventID: activity.ScheduledEventID,
					}); err != nil {
						t.Errorf("complete activity: %v", err)
					}
					continue
				}
				var cmds []history.Command
				if len(task.History.Filter(history.EventTypeActivityTaskCompleted)) > 0 {
					cmds = []history.Command{completeWorkflow("ok")}
				} else {
					cmds = []history.Command{scheduleActivity("step", "Step")}
				}
				if err := h.eng.RespondWorkflowTaskCompleted(context.Background(), api.RespondWorkflowTaskCompletedRequest{
					Namespace: testNamespace, Execution: task.Execution, Commands: cmds, Identity: "w",
				}); err != nil {
					t.Errorf("complete workflow task: %v", err)
					continue
				}
				if cmds[0].Type == history.CommandTypeCompleteWorkflowExecution {
					completed.Add(1)
				}
			}
		}()
	}

	waitFor(t, "every workflow to complete", func() bool { return completed.Load() == workflows })
	close(done)
	workers.Wait()

	for i := 0; i < workflows; i++ {
		id := fmt.Sprintf("parallel-%d", i)
		rec, ok := h.store.record(testNamespace, id, "")
		if !ok || rec.Status != skald.StatusCompleted {
			t.Fatalf("%s ended as %+v", id, rec)
		}
	}
}

func TestNewRejectsAMissingStore(t *testing.T) {
	t.Parallel()
	if _, err := engine.New(engine.Config{}); err == nil {
		t.Fatal("an engine was built with no store")
	} else {
		assertAPIError(t, err, api.CodeInvalidArgument)
	}
}

// TestStoreFailuresSurfaceAsTypedErrors checks that an infrastructure problem
// reaches the client as something it can act on rather than as an opaque
// internal error.
func TestStoreFailuresSurfaceAsTypedErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("doomed-store")

	if err := h.store.Close(); err != nil {
		t.Fatalf("closing the store: %v", err)
	}

	_, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "after-close",
		WorkflowType: testType, TaskQueue: testQueue,
	})
	assertAPIError(t, err, api.CodeUnavailable)

	err = h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "doomed-store", SignalName: "x",
	})
	assertAPIError(t, err, api.CodeUnavailable)

	if _, err := h.eng.DescribeWorkflow(h.ctx(), testNamespace, "doomed-store", ""); err == nil {
		t.Fatal("describe succeeded against a closed store")
	} else {
		assertAPIError(t, err, api.CodeUnavailable)
	}
}

func TestUnknownNamespaceIsRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, err := h.eng.StartWorkflow(h.ctx(), api.StartWorkflowRequest{
		Namespace: "not a namespace!", WorkflowID: "w",
		WorkflowType: testType, TaskQueue: testQueue,
	})
	assertAPIError(t, err, api.CodeInvalidArgument)

	err = h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: "not a namespace!", WorkflowID: "w", SignalName: "s",
	})
	assertAPIError(t, err, api.CodeInvalidArgument)
}
