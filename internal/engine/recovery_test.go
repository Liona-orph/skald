package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// crash throws away everything that is not durable.
//
// It stops the engine, drops the matcher entirely -- which is exactly what a
// process restart does to derived task queues -- and stands a fresh engine up
// over the same store. The new engine is deliberately left *unstarted* so that a
// test can observe the world before recovery as well as after it.
func (h *harness) crash() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.eng.Close(ctx); err != nil {
		h.t.Fatalf("closing the old engine: %v", err)
	}
	h.matcher.Close()

	h.matcher = matching.New(matching.Config{Clock: h.clk, PollTimeout: 30 * time.Second})
	h.eng = h.newEngine()
}

// restart is crash followed by the normal startup sequence.
func (h *harness) restart() {
	h.t.Helper()
	h.crash()
	if err := h.eng.Start(context.Background()); err != nil {
		h.t.Fatalf("starting the recovered engine: %v", err)
	}
}

// TestRecoverRematerialisesWorkflowTasks is the point of derived task queues: a
// lost queue costs latency, never correctness.
func TestRecoverRematerialisesWorkflowTasks(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("survivor")

	h.crash()
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("the fresh matcher already held a task; the test is not exercising recovery")
	}

	if err := h.eng.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	task, ok := h.pollWorkflowTaskMaybe()
	if !ok {
		t.Fatal("recovery did not re-dispatch the pending workflow task")
	}
	if task.Execution.RunID != runID {
		t.Fatalf("recovered a task for run %s, want %s", task.Execution.RunID, runID)
	}
	// The recovered task is a normal task: the workflow carries on as if
	// nothing happened.
	h.completeWorkflowTask(task, completeWorkflow("survived"))
	if got := h.status("survivor", runID); got != skald.StatusCompleted {
		t.Fatalf("status = %s", got)
	}
}

func TestRecoverRematerialisesActivityTasks(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.startWorkflow("activity-survivor")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("charge", "ChargeCard"))

	h.restart()

	task, ok := h.pollActivityTaskMaybe()
	if !ok {
		t.Fatal("recovery did not re-dispatch the pending activity")
	}
	if task.ActivityID != "charge" || task.Attempt != 1 {
		t.Fatalf("recovered activity task = %+v", task)
	}
	if err := h.eng.RespondActivityTaskCompleted(h.ctx(), api.RespondActivityTaskCompletedRequest{
		Namespace: testNamespace, Execution: task.Execution,
		ScheduledEventID: task.ScheduledEventID, Result: skald.MustPayload("ok"),
	}); err != nil {
		t.Fatalf("RespondActivityTaskCompleted: %v", err)
	}
	next := h.pollWorkflowTask()
	h.completeWorkflowTask(next, completeWorkflow("done"))
	h.assertHistory("activity-survivor", runID,
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
}

// TestRecoverLeavesInFlightWorkAlone pins the other half of the rule: recovery
// restores only what nothing else can. Work a worker may still be running is
// owned by the timer index, and re-dispatching it would duplicate an activity
// execution.
func TestRecoverLeavesInFlightWorkAlone(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("in-flight")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, scheduleActivity("running", "Running", func(c *history.ScheduleActivityCommand) {
		c.StartToCloseTimeout = 10 * time.Second
		c.RetryPolicy = &skald.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1}
	}))
	h.pollActivityTask() // a worker now holds attempt 1

	h.restart()

	if _, ok := h.pollActivityTaskMaybe(); ok {
		t.Fatal("recovery re-dispatched an activity a worker was already running")
	}
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("recovery dispatched a workflow task that does not exist")
	}

	// The durable timeout is what eventually reclaims it, exactly as it would
	// have without a restart.
	h.advance(10 * time.Second)
	h.advanceToNextTimer()

	recovered, ok := h.pollActivityTaskMaybe()
	if !ok {
		t.Fatal("the start-to-close timeout did not reclaim the abandoned attempt")
	}
	if recovered.Attempt != 2 {
		t.Fatalf("reclaimed attempt = %d, want 2: the attempt count must survive the restart", recovered.Attempt)
	}
}

func TestRecoverIgnoresClosedExecutions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("finished")
	wt := h.pollWorkflowTask()
	h.completeWorkflowTask(wt, completeWorkflow(nil))

	h.restart()

	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("recovery dispatched work for a closed execution")
	}
}

func TestRecoverIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.startWorkflow("twice")

	h.crash()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := h.eng.Recover(ctx); err != nil {
			t.Fatalf("Recover %d: %v", i, err)
		}
	}

	// Recovery is allowed to produce duplicate references -- matching is a
	// derived structure and cannot deduplicate -- but only one of them may
	// start the task. The rest are recognised as stale and skipped.
	first, ok := h.pollWorkflowTaskMaybe()
	if !ok {
		t.Fatal("no task after recovery")
	}
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("a second poller was handed the same workflow task")
	}
	h.completeWorkflowTask(first, completeWorkflow(nil))
}

func TestRecoverAcrossManyExecutions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const n = 25
	for i := 0; i < n; i++ {
		h.startWorkflow(workflowIDFor(i))
	}
	h.restart()

	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		task, ok := h.pollWorkflowTaskMaybe()
		if !ok {
			t.Fatalf("only %d of %d workflow tasks were recovered", len(seen), n)
		}
		if seen[task.Execution.WorkflowID] {
			t.Fatalf("%s was recovered twice", task.Execution.WorkflowID)
		}
		seen[task.Execution.WorkflowID] = true
	}
	if _, ok := h.pollWorkflowTaskMaybe(); ok {
		t.Fatal("recovery produced more tasks than there are executions")
	}
}

func workflowIDFor(i int) string {
	return "bulk-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26))
}
