package engine_test

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skald-io/skald/internal/clock"
	"github.com/skald-io/skald/internal/engine"
	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

const (
	testNamespace = "default"
	testQueue     = "test-queue"
	testType      = "TestWorkflow"

	// testTimerInterval is the timer service's scan interval in virtual time.
	testTimerInterval = time.Millisecond
)

// harness wires an engine to a fake store and a virtual clock, and gives the
// tests a vocabulary for the things they actually do: start a workflow, take a
// task, answer it, move time.
type harness struct {
	t       *testing.T
	store   *fakeStore
	clk     *clock.Virtual
	matcher *matching.Matcher
	eng     *engine.Engine
	ids     atomic.Int64
	seeds   atomic.Int64
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:     t,
		store: newFakeStore(),
		clk:   clock.NewVirtual(time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)),
	}
	h.matcher = matching.New(matching.Config{Clock: h.clk, PollTimeout: 30 * time.Second})
	h.eng = h.newEngine()
	if err := h.eng.Start(context.Background()); err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.eng.Close(ctx); err != nil {
			t.Errorf("engine.Close: %v", err)
		}
		h.matcher.Close()
	})
	return h
}

// newEngine builds an engine over the harness's store, clock and matcher. It is
// separate from newHarness so that a crash-recovery test can throw the first
// engine away and stand a fresh one up over the same durable state.
func (h *harness) newEngine() *engine.Engine {
	h.t.Helper()
	eng, err := engine.New(engine.Config{
		Store:   h.store,
		Clock:   h.clk,
		Matcher: h.matcher,
		// Deterministic identifiers make a failing assertion readable and let a
		// test name a run before it exists.
		NewID:   func() string { return fmt.Sprintf("id-%d", h.ids.Add(1)) },
		NewSeed: func() int64 { return h.seeds.Add(1) },
		// The scan interval is a millisecond of *virtual* time. Timer latency
		// is then far below the granularity of anything the tests assert on,
		// so a deadline set for "ten seconds from now" fires at ten seconds and
		// not at ten seconds plus a scan.
		TimerInterval:       testTimerInterval,
		HistoryPollInterval: time.Second,
		RedispatchInterval:  time.Minute,
		// Engine logs are the only window into the post-commit path, which
		// swallows failures by design. Routing them at the test makes a dropped
		// dispatch or a failed timer visible instead of silent.
		Logger: slog.New(slog.NewTextHandler(testWriter{h.t}, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		h.t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func (h *harness) ctx() context.Context { return context.Background() }

// testWriter routes engine logs into the test output.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("engine: %s", strings.TrimSpace(string(p)))
	return len(p), nil
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

func (h *harness) startWorkflow(workflowID string, mutators ...func(*api.StartWorkflowRequest)) string {
	h.t.Helper()
	req := api.StartWorkflowRequest{
		Namespace:    testNamespace,
		WorkflowID:   workflowID,
		WorkflowType: testType,
		TaskQueue:    testQueue,
	}
	for _, m := range mutators {
		m(&req)
	}
	resp, err := h.eng.StartWorkflow(h.ctx(), req)
	if err != nil {
		h.t.Fatalf("StartWorkflow(%s): %v", workflowID, err)
	}
	return resp.RunID
}

func (h *harness) pollWorkflowTask() api.WorkflowTask {
	h.t.Helper()
	task, err := h.eng.PollWorkflowTask(h.ctx(), api.PollWorkflowTaskRequest{
		Namespace: testNamespace, TaskQueue: testQueue, Identity: "worker-1",
	})
	if err != nil {
		h.t.Fatalf("PollWorkflowTask: %v", err)
	}
	if task.Empty {
		h.t.Fatalf("PollWorkflowTask returned an empty task; queue stats: %+v",
			h.matcher.Stats(matching.QueueKey{Namespace: testNamespace, TaskQueue: testQueue, Kind: matching.KindWorkflow}))
	}
	return task
}

func (h *harness) pollWorkflowTaskMaybe() (api.WorkflowTask, bool) {
	h.t.Helper()
	// A context that is already cancelled turns the long poll into a
	// non-blocking check: the matcher's fast path answers from the backlog, and
	// an empty queue returns at once instead of parking.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task, err := h.eng.PollWorkflowTask(ctx, api.PollWorkflowTaskRequest{
		Namespace: testNamespace, TaskQueue: testQueue, Identity: "worker-1",
	})
	if err != nil {
		h.t.Fatalf("PollWorkflowTask: %v", err)
	}
	return task, !task.Empty
}

func (h *harness) completeWorkflowTask(task api.WorkflowTask, cmds ...history.Command) {
	h.t.Helper()
	if err := h.eng.RespondWorkflowTaskCompleted(h.ctx(), api.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Execution: task.Execution,
		Commands:  cmds,
		Identity:  "worker-1",
	}); err != nil {
		h.t.Fatalf("RespondWorkflowTaskCompleted: %v", err)
	}
}

func (h *harness) pollActivityTask() api.ActivityTask {
	h.t.Helper()
	task, err := h.eng.PollActivityTask(h.ctx(), api.PollActivityTaskRequest{
		Namespace: testNamespace, TaskQueue: testQueue, Identity: "activity-worker",
	})
	if err != nil {
		h.t.Fatalf("PollActivityTask: %v", err)
	}
	if task.Empty {
		h.t.Fatalf("PollActivityTask returned an empty task; queue stats: %+v",
			h.matcher.Stats(matching.QueueKey{Namespace: testNamespace, TaskQueue: testQueue, Kind: matching.KindActivity}))
	}
	return task
}

func (h *harness) pollActivityTaskMaybe() (api.ActivityTask, bool) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task, err := h.eng.PollActivityTask(ctx, api.PollActivityTaskRequest{
		Namespace: testNamespace, TaskQueue: testQueue, Identity: "activity-worker",
	})
	if err != nil {
		h.t.Fatalf("PollActivityTask: %v", err)
	}
	return task, !task.Empty
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

func scheduleActivity(id, typ string, mutators ...func(*history.ScheduleActivityCommand)) history.Command {
	cmd := &history.ScheduleActivityCommand{
		ActivityID:          id,
		ActivityType:        typ,
		StartToCloseTimeout: 30 * time.Second,
	}
	for _, m := range mutators {
		m(cmd)
	}
	return history.Command{Type: history.CommandTypeScheduleActivityTask, ScheduleActivity: cmd}
}

func completeWorkflow(result any) history.Command {
	return history.Command{
		Type:             history.CommandTypeCompleteWorkflowExecution,
		CompleteWorkflow: &history.CompleteWorkflowCommand{Result: skald.MustPayload(result)},
	}
}

func failWorkflow(err *skald.ApplicationError) history.Command {
	return history.Command{
		Type:         history.CommandTypeFailWorkflowExecution,
		FailWorkflow: &history.FailWorkflowCommand{Failure: err},
	}
}

func startTimer(id string, d time.Duration) history.Command {
	return history.Command{
		Type:       history.CommandTypeStartTimer,
		StartTimer: &history.StartTimerCommand{TimerID: id, StartToFireTimeout: d},
	}
}

func continueAsNew(input any) history.Command {
	return history.Command{
		Type:          history.CommandTypeContinueAsNewWorkflow,
		ContinueAsNew: &history.ContinueAsNewCommand{Input: skald.MustPayload(input)},
	}
}

func cancelWorkflowCmd() history.Command {
	return history.Command{
		Type:           history.CommandTypeCancelWorkflowExecution,
		CancelWorkflow: &history.CancelWorkflowCommand{},
	}
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

// advance moves virtual time forward and lets the timer service catch up.
//
// It stops at each durable deadline in turn rather than jumping straight to the
// end, so a chain of timers -- a retry that arms a timeout that arms another
// retry -- unfolds in the order production would see it. After each stop it
// waits until the store holds nothing that is due, which is the observable form
// of "the engine has absorbed everything that came due".
func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	target := h.clk.Now().Add(d)
	for {
		next := target
		if deadline := h.store.nextTimerDeadline(); !deadline.IsZero() && deadline.Before(next) {
			next = deadline
		}
		h.clk.Set(next)
		h.waitForTimers()
		if !h.clk.Now().Before(target) {
			return
		}
	}
}

// waitForTimers blocks until every due timer has been processed.
//
// The timer service wakes on its own scan timer, so the clock has to be nudged
// onto that scan before anything happens. The nudge is bounded by two scan
// intervals -- two milliseconds of virtual time -- which is small enough that
// it cannot disturb a deadline a test is asserting on.
func (h *harness) waitForTimers() {
	h.t.Helper()
	wallDeadline := time.Now().Add(5 * time.Second)
	for h.store.dueTimerCount(h.clk.Now()) > 0 {
		if time.Now().After(wallDeadline) {
			h.t.Fatalf("the timer service never drained; still due at %s: %+v",
				h.clk.Now(), h.store.timerRecords())
		}
		runtime.Gosched()
		if pending := h.clk.Pending(); len(pending) > 0 {
			step := pending[0].Deadline
			if limit := h.clk.Now().Add(2 * testTimerInterval); step.After(limit) {
				step = limit
			}
			h.clk.Set(step)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

func (h *harness) eventTypes(workflowID, runID string) []history.EventType {
	h.t.Helper()
	events := h.store.history(testNamespace, workflowID, runID)
	out := make([]history.EventType, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type())
	}
	return out
}

func (h *harness) assertHistory(workflowID, runID string, want ...history.EventType) {
	h.t.Helper()
	got := h.eventTypes(workflowID, runID)
	if len(got) != len(want) {
		h.t.Fatalf("history for %s/%s:\n got %s\nwant %s", workflowID, runID, formatTypes(got), formatTypes(want))
	}
	for i := range want {
		if got[i] != want[i] {
			h.t.Fatalf("history for %s/%s differs at event %d:\n got %s\nwant %s",
				workflowID, runID, i+1, formatTypes(got), formatTypes(want))
		}
	}
}

func formatTypes(types []history.EventType) string {
	parts := make([]string, len(types))
	for i, t := range types {
		parts[i] = fmt.Sprintf("%d:%s", i+1, t)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func (h *harness) status(workflowID, runID string) skald.WorkflowStatus {
	h.t.Helper()
	rec, ok := h.store.record(testNamespace, workflowID, runID)
	if !ok {
		h.t.Fatalf("no record for %s/%s", workflowID, runID)
	}
	return rec.Status
}

func (h *harness) lastEvent(workflowID, runID string) history.Event {
	h.t.Helper()
	events := h.store.history(testNamespace, workflowID, runID)
	if len(events) == 0 {
		h.t.Fatalf("no history for %s/%s", workflowID, runID)
	}
	return events[len(events)-1]
}

// assertAPIError checks that err is an *api.Error with the expected code.
func assertAPIError(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with code %q, got nil", code)
	}
	apiErr, ok := err.(*api.Error)
	if !ok {
		t.Fatalf("expected *api.Error, got %T: %v", err, err)
	}
	if apiErr.Code != code {
		t.Fatalf("error code = %q, want %q (message: %s)", apiErr.Code, code, apiErr.Message)
	}
}

func retryable(msg string) *skald.ApplicationError {
	return skald.NewApplicationError("Transient", "%s", msg)
}
