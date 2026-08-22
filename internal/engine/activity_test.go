package engine_test

import (
	"testing"
	"time"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// scheduleRetryableActivity is the shape most of these tests need: an activity
// that will be retried a bounded number of times.
func scheduleRetryableActivity(id string, maxAttempts int32, mutators ...func(*history.ScheduleActivityCommand)) history.Command {
	return scheduleActivity(id, "Flaky", append([]func(*history.ScheduleActivityCommand){
		func(c *history.ScheduleActivityCommand) {
			c.StartToCloseTimeout = 30 * time.Second
			c.ScheduleToCloseTimeout = 10 * time.Minute
			c.RetryPolicy = &skald.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumAttempts:    maxAttempts,
			}
		},
	}, mutators...)...)
}

func (h *harness) failActivity(task api.ActivityTask, failure *skald.ApplicationError) {
	h.t.Helper()
	if err := h.eng.RespondActivityTaskFailed(h.ctx(), api.RespondActivityTaskFailedRequest{
		Namespace:        testNamespace,
		Execution:        task.Execution,
		ScheduledEventID: task.ScheduledEventID,
		Failure:          failure,
		Identity:         "activity-worker",
	}); err != nil {
		h.t.Fatalf("RespondActivityTaskFailed: %v", err)
	}
}

// timerOfKind returns the single armed timer of the given kind.
func (h *harness) timerOfKind(kind persistence.TimerKind) persistence.TimerRecord {
	h.t.Helper()
	var found []persistence.TimerRecord
	for _, rec := range h.store.timerRecords() {
		if rec.Kind == kind {
			found = append(found, rec)
		}
	}
	if len(found) != 1 {
		h.t.Fatalf("expected exactly one timer of kind %d, got %+v (all: %+v)", kind, found, h.store.timerRecords())
	}
	return found[0]
}

// advanceToNextTimer moves virtual time to the earliest armed durable deadline.
func (h *harness) advanceToNextTimer() {
	h.t.Helper()
	deadline := h.store.nextTimerDeadline()
	if deadline.IsZero() {
		h.t.Fatal("no durable timer is armed")
	}
	if d := deadline.Sub(h.clk.Now()); d > 0 {
		h.advance(d)
	} else {
		h.waitForTimers()
	}
}

// TestActivityRetryWritesNoHistory is the load-bearing property of activity
// retries: the workflow sees nothing, and the history does not grow per attempt.
// An activity retried a thousand times must still cost two events.
func TestActivityRetryWritesNoHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("retries")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleRetryableActivity("flaky", 3))

	first := h.pollActivityTask()
	if first.Attempt != 1 {
		t.Fatalf("first dispatch reported attempt %d", first.Attempt)
	}
	historyAfterStart := len(h.store.history(testNamespace, "retries", runID))
	h.failActivity(first, retryable("connection reset"))

	// Nothing was written: no failure event, no scheduling event, nothing.
	if got := len(h.store.history(testNamespace, "retries", runID)); got != historyAfterStart {
		t.Fatalf("a retry wrote %d history events", got-historyAfterStart)
	}
	// The attempt counter lives in the durable timer index instead, which is
	// what lets a cold replica pick the retry up with the right number.
	retry := h.timerOfKind(persistence.TimerKindActivityRetry)
	if retry.Attempt != 2 {
		t.Fatalf("retry timer carries attempt %d, want 2", retry.Attempt)
	}
	if lower := h.clk.Now().Add(time.Second); retry.FireAt.Before(lower) {
		t.Fatalf("retry armed for %s, earlier than the policy's initial interval", retry.FireAt)
	}
	if upper := h.clk.Now().Add(1200 * time.Millisecond); retry.FireAt.After(upper) {
		t.Fatalf("retry armed for %s, later than the interval plus jitter", retry.FireAt)
	}

	h.advanceToNextTimer()

	second := h.pollActivityTask()
	if second.Attempt != 2 {
		t.Fatalf("second dispatch reported attempt %d, want 2", second.Attempt)
	}
	// Every attempt shares one started event: a second one would be
	// unreplayable, because replaying it finds the activity already started.
	if second.StartedEventID != first.StartedEventID {
		t.Fatalf("attempt 2 has started event %d, want the original %d",
			second.StartedEventID, first.StartedEventID)
	}
	if second.LastFailure != nil && second.LastFailure.Message == "" {
		t.Fatal("the retry lost the previous failure detail")
	}

	// Backoff grows: the second interval is the first times the coefficient.
	h.failActivity(second, retryable("connection reset again"))
	retry2 := h.timerOfKind(persistence.TimerKindActivityRetry)
	if retry2.Attempt != 3 {
		t.Fatalf("retry timer carries attempt %d, want 3", retry2.Attempt)
	}
	if lower := h.clk.Now().Add(2 * time.Second); retry2.FireAt.Before(lower) {
		t.Fatalf("second backoff %s is shorter than initial*coefficient", retry2.FireAt.Sub(h.clk.Now()))
	}

	h.advanceToNextTimer()
	third := h.pollActivityTask()
	if third.Attempt != 3 {
		t.Fatalf("third dispatch reported attempt %d, want 3", third.Attempt)
	}

	// The policy is exhausted, so this failure is terminal and the workflow
	// finally hears about it.
	h.failActivity(third, retryable("still broken"))
	h.assertHistory("retries", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskStarted,
		history.EventTypeActivityTaskFailed,
		history.EventTypeWorkflowTaskScheduled,
	)
	failed, _ := history.AttributesAs[history.ActivityTaskFailedAttributes](
		h.store.history(testNamespace, "retries", runID)[6])
	if failed.RetryState != history.RetryStateMaximumAttemptsReached {
		t.Fatalf("retry state = %s, want MaximumAttemptsReached", failed.RetryState)
	}
	if recs := h.store.timerRecords(); len(recs) != 0 {
		t.Fatalf("timers survived the activity: %+v", recs)
	}
}

func TestNonRetryableActivityFailureIsTerminalImmediately(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("no-retry")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleRetryableActivity("permanent", 0))

	task := h.pollActivityTask()
	h.failActivity(task, skald.NewNonRetryableError("InsufficientFunds", "declined"))

	h.assertHistory("no-retry", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskStarted,
		history.EventTypeActivityTaskFailed,
		history.EventTypeWorkflowTaskScheduled,
	)
	if _, ok := h.pollWorkflowTaskMaybe(); !ok {
		t.Fatal("the failure did not wake the workflow")
	}
}

func TestActivityStartToCloseTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("stc")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("slow", "Slow", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = 10 * time.Second
		c.RetryPolicy = &skald.RetryPolicy{MaximumAttempts: 1}
	}))
	h.pollActivityTask()

	h.advance(9 * time.Second)
	if last := h.lastEvent("stc", runID).Type(); last == history.EventTypeActivityTaskTimedOut {
		t.Fatal("the activity timed out a second early")
	}
	h.advance(time.Second)

	timedOut, ok := history.AttributesAs[history.ActivityTaskTimedOutAttributes](h.store.history(testNamespace, "stc", runID)[6])
	if !ok {
		t.Fatalf("event 7 is %s, want ActivityTaskTimedOut", h.eventTypes("stc", runID)[6])
	}
	if timedOut.Kind != skald.TimeoutStartToClose {
		t.Fatalf("timeout kind = %s, want START_TO_CLOSE", timedOut.Kind)
	}
	if _, ok := h.pollWorkflowTaskMaybe(); !ok {
		t.Fatal("the timeout did not wake the workflow")
	}
}

func TestActivityScheduleToStartTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("sts")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("unclaimed", "Unclaimed", func(c *history.ScheduleActivityCommand) {
		c.ScheduleToStartTimeout = 5 * time.Second
		c.StartToCloseTimeout = time.Minute
		c.RetryPolicy = &skald.RetryPolicy{MaximumAttempts: 1}
	}))

	// Nobody polls: a schedule-to-start expiry almost always means "no worker
	// is listening on this queue".
	h.advance(5 * time.Second)

	timedOut, ok := history.AttributesAs[history.ActivityTaskTimedOutAttributes](h.lastEvent("sts", runID))
	if !ok {
		// The wake-up task is appended after the timeout, so look one back.
		events := h.store.history(testNamespace, "sts", runID)
		timedOut, ok = history.AttributesAs[history.ActivityTaskTimedOutAttributes](events[len(events)-2])
	}
	if !ok {
		t.Fatalf("no timeout event in %v", h.eventTypes("sts", runID))
	}
	if timedOut.Kind != skald.TimeoutScheduleToStart {
		t.Fatalf("timeout kind = %s, want SCHEDULE_TO_START", timedOut.Kind)
	}
	// The task reference is stale now; a late poller must not be handed it.
	if _, ok := h.pollActivityTaskMaybe(); ok {
		t.Fatal("a timed-out activity was still dispatched to a worker")
	}
}

func TestActivityScheduleToCloseTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("stc-outer")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("budgeted", "Budgeted", func(c *history.ScheduleActivityCommand) {
		c.ScheduleToCloseTimeout = 10 * time.Second
		c.StartToCloseTimeout = time.Hour
	}))
	h.pollActivityTask()

	h.advance(10 * time.Second)

	events := h.store.history(testNamespace, "stc-outer", runID)
	timedOut, ok := history.AttributesAs[history.ActivityTaskTimedOutAttributes](events[6])
	if !ok {
		t.Fatalf("event 7 is %s, want ActivityTaskTimedOut", h.eventTypes("stc-outer", runID)[6])
	}
	// The outer budget is not retryable however generous the policy: another
	// attempt could not finish inside a window that has already closed.
	if timedOut.Kind != skald.TimeoutScheduleToClose {
		t.Fatalf("timeout kind = %s, want SCHEDULE_TO_CLOSE", timedOut.Kind)
	}
}

func TestActivityHeartbeatExtendsTheDeadline(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("heartbeats")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("long", "Long", func(c *history.ScheduleActivityCommand) {
		c.HeartbeatTimeout = 2 * time.Second
		c.StartToCloseTimeout = time.Minute
		c.RetryPolicy = &skald.RetryPolicy{MaximumAttempts: 1}
	}))
	task := h.pollActivityTask()
	if task.HeartbeatTimeout != 2*time.Second {
		t.Fatalf("the worker was not told the heartbeat timeout: %+v", task)
	}

	h.advance(time.Second)
	resp, err := h.eng.RecordActivityHeartbeat(h.ctx(), api.RecordActivityHeartbeatRequest{
		Namespace:        testNamespace,
		Execution:        task.Execution,
		ScheduledEventID: task.ScheduledEventID,
		Details:          skald.MustPayload("50%"),
	})
	if err != nil {
		t.Fatalf("RecordActivityHeartbeat: %v", err)
	}
	if resp.CancelRequested {
		t.Fatal("a heartbeat reported a cancellation nobody requested")
	}

	// The deadline moved, so the original one passing is not a timeout.
	h.advance(1500 * time.Millisecond)
	if last := h.lastEvent("heartbeats", runID).Type(); last == history.EventTypeActivityTaskTimedOut {
		t.Fatal("the heartbeat did not extend the deadline")
	}

	h.advance(time.Second)
	events := h.store.history(testNamespace, "heartbeats", runID)
	timedOut, ok := history.AttributesAs[history.ActivityTaskTimedOutAttributes](events[6])
	if !ok {
		t.Fatalf("event 7 is %s, want ActivityTaskTimedOut", h.eventTypes("heartbeats", runID)[6])
	}
	if timedOut.Kind != skald.TimeoutHeartbeat {
		t.Fatalf("timeout kind = %s, want HEARTBEAT", timedOut.Kind)
	}
	// The checkpoint survives so that the next attempt can resume from it.
	if timedOut.LastHeartbeatDetails == nil {
		t.Fatal("the heartbeat checkpoint was lost")
	}
	// Heartbeats write no history: only the two activity events plus the
	// timeout exist.
	if got := len(events); got != 8 {
		t.Fatalf("history has %d events, want 8: heartbeats must not be recorded (%v)",
			got, h.eventTypes("heartbeats", runID))
	}
}

func TestActivityCancellation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("cancel-activity")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("interruptible", "Interruptible", func(c *history.ScheduleActivityCommand) {
		c.HeartbeatTimeout = 30 * time.Second
		c.StartToCloseTimeout = 5 * time.Minute
	}))
	task := h.pollActivityTask()

	// Wake the workflow so it can issue the cancellation command.
	if err := h.eng.SignalWorkflow(h.ctx(), api.SignalWorkflowRequest{
		Namespace: testNamespace, WorkflowID: "cancel-activity", SignalName: "stop",
	}); err != nil {
		t.Fatalf("SignalWorkflow: %v", err)
	}
	second := h.pollWorkflowTask()
	h.completeWorkflowTask(second, history.Command{
		Type: history.CommandTypeRequestCancelActivityTask,
		RequestCancelActivity: &history.RequestCancelActivityCommand{
			ScheduledEventID: task.ScheduledEventID,
		},
	})

	// The activity learns about it through its heartbeat: delivery is best
	// effort by design, because the engine cannot interrupt user code.
	resp, err := h.eng.RecordActivityHeartbeat(h.ctx(), api.RecordActivityHeartbeatRequest{
		Namespace: testNamespace, Execution: task.Execution, ScheduledEventID: task.ScheduledEventID,
	})
	if err != nil {
		t.Fatalf("RecordActivityHeartbeat: %v", err)
	}
	if !resp.CancelRequested {
		t.Fatal("the heartbeat did not report the cancellation request")
	}

	if err := h.eng.RespondActivityTaskCanceled(h.ctx(), api.RespondActivityTaskCanceledRequest{
		Namespace: testNamespace, Execution: task.Execution,
		ScheduledEventID: task.ScheduledEventID, Details: skald.MustPayload("rolled back"),
	}); err != nil {
		t.Fatalf("RespondActivityTaskCanceled: %v", err)
	}

	h.assertHistory("cancel-activity", runID,
		history.EventTypeWorkflowExecutionStarted,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeActivityTaskScheduled,
		history.EventTypeActivityTaskStarted,
		history.EventTypeWorkflowExecutionSignaled,
		history.EventTypeWorkflowTaskScheduled,
		history.EventTypeWorkflowTaskStarted,
		history.EventTypeWorkflowTaskCompleted,
		history.EventTypeActivityTaskCancelRequested,
		history.EventTypeActivityTaskCanceled,
		history.EventTypeWorkflowTaskScheduled,
	)
}

// TestStaleActivityResponseIsRejected covers the worker that comes back after
// the server already gave up on its attempt.
func TestStaleActivityResponseIsRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("stale")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("slow", "Slow", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = 10 * time.Second
		c.ScheduleToCloseTimeout = time.Hour
		c.RetryPolicy = &skald.RetryPolicy{InitialInterval: time.Minute, BackoffCoefficient: 1}
	}))
	task := h.pollActivityTask()

	// The attempt times out and enters its retry backoff, so no worker holds it.
	h.advance(10 * time.Second)

	err := h.eng.RespondActivityTaskCompleted(h.ctx(), api.RespondActivityTaskCompletedRequest{
		Namespace: testNamespace, Execution: task.Execution,
		ScheduledEventID: task.ScheduledEventID, Result: skald.MustPayload("late"),
	})
	assertAPIError(t, err, api.CodeFailedPrecondition)

	// A response for an activity that never existed is a different error.
	err = h.eng.RespondActivityTaskCompleted(h.ctx(), api.RespondActivityTaskCompletedRequest{
		Namespace: testNamespace, Execution: task.Execution, ScheduledEventID: 999,
	})
	assertAPIError(t, err, api.CodeNotFound)
}

// TestActivityTimeoutRetriesWithTheDurableAttemptCount shows the timer index
// carrying the attempt across a timeout rather than the history doing it.
func TestActivityTimeoutRetriesWithTheDurableAttemptCount(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("timeout-retry")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("slow", "Slow", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = 10 * time.Second
		c.ScheduleToCloseTimeout = time.Hour
		c.RetryPolicy = &skald.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 3}
	}))
	h.pollActivityTask()

	h.advance(10 * time.Second)
	retry := h.timerOfKind(persistence.TimerKindActivityRetry)
	if retry.Attempt != 2 {
		t.Fatalf("retry after a timeout carries attempt %d, want 2", retry.Attempt)
	}

	h.advanceToNextTimer()
	second := h.pollActivityTask()
	if second.Attempt != 2 {
		t.Fatalf("re-dispatched attempt = %d, want 2", second.Attempt)
	}
	if second.Deadline.IsZero() {
		t.Fatal("the re-dispatched attempt was given no deadline")
	}
	// The second attempt's start-to-close clock restarts from its dispatch.
	if want := h.clk.Now().Add(10 * time.Second); !second.Deadline.Equal(want) {
		t.Fatalf("attempt deadline = %s, want %s", second.Deadline, want)
	}
}

func TestActivityDispatchWatchdogRecoversALostRetry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("lost-dispatch")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("slow", "Slow", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = 10 * time.Second
		c.ScheduleToCloseTimeout = time.Hour
		c.RetryPolicy = &skald.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1}
	}))
	first := h.pollActivityTask()
	h.failActivity(first, retryable("nope"))

	// The retry becomes pollable, and nobody polls it. Without a watchdog the
	// attempt would sit in matching forever, invisible to the history.
	dispatchAt := h.timerOfKind(persistence.TimerKindActivityRetry).FireAt
	h.advanceToNextTimer()
	watchdog := h.timerOfKind(persistence.TimerKindActivityTimeout)
	// The watchdog is measured from the instant the re-dispatch ran, which is
	// the first scan at or after the retry deadline -- so the assertion allows
	// one scan interval of slack and nothing more.
	if d := watchdog.FireAt.Sub(dispatchAt); d < time.Minute || d > time.Minute+5*testTimerInterval {
		t.Fatalf("dispatch watchdog armed %s after the re-dispatch, want about a minute", d)
	}

	h.advance(time.Minute)
	// The watchdog reports the attempt as never having started, which the retry
	// policy treats like any other failure: another attempt is scheduled.
	retry := h.timerOfKind(persistence.TimerKindActivityRetry)
	if retry.Attempt != 3 {
		t.Fatalf("attempt after the watchdog = %d, want 3", retry.Attempt)
	}
}
