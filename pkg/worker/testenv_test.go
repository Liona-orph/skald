package worker

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/internal/clock"
	"github.com/Liona-orph/skald/internal/engine"
	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/internal/persistence/memory"
	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// testEnv wires a real engine, a real store and a real worker into one process.
//
// Nothing here is a mock. The worker talks to engine.Engine through the same
// api.Service interface it would use against an HTTP server, so these tests
// exercise the actual command protocol, the actual history the engine writes and
// the actual replay path -- which is the only way to have any confidence in a
// replay engine at all. The one concession to test speed is the timer scan
// interval, and even that is a real durable timer, just scanned more often.
type testEnv struct {
	t      *testing.T
	store  *memory.Store
	match  *matching.Matcher
	engine *engine.Engine
	worker *Worker

	namespace string
	queue     string
}

const (
	testNamespace = "default"
	testQueue     = "test-queue"

	// testTaskTimeout bounds a workflow task. It is short so that a worker which
	// is stopped while holding a task is replaced quickly, which is exactly the
	// crash-recovery case one of these tests wants to observe.
	testTaskTimeout = 2 * time.Second
	// testWait is the wall-clock budget for any "eventually" assertion.
	testWait = 30 * time.Second
)

func newTestEnv(t *testing.T, mutators ...func(*Options)) *testEnv {
	t.Helper()

	store := memory.New()
	clk := clock.System()
	match := matching.New(matching.Config{Clock: clk, PollTimeout: 100 * time.Millisecond})

	eng, err := engine.New(engine.Config{
		Store:   store,
		Clock:   clk,
		Matcher: match,
		// A five millisecond scan keeps a hundred millisecond timer honest
		// without making the suite slow.
		TimerInterval:       5 * time.Millisecond,
		HistoryPollInterval: 20 * time.Millisecond,
		RedispatchInterval:  time.Second,
		Logger:              testLogger(t, slog.LevelError),
	})
	require.NoError(t, err)
	require.NoError(t, eng.Start(context.Background()))

	e := &testEnv{t: t, store: store, match: match, engine: eng, namespace: testNamespace, queue: testQueue}
	e.worker = e.newWorker(mutators...)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.worker.Stop(ctx)
		_ = eng.Close(ctx)
		match.Close()
		_ = store.Close()
	})
	return e
}

// newWorker builds a worker over the same engine. It is separate so that a test
// can throw one worker away and stand up a fresh one, which is what a crash and
// a redeploy look like from the engine's side.
func (e *testEnv) newWorker(mutators ...func(*Options)) *Worker {
	e.t.Helper()
	opts := Options{
		Namespace:                e.namespace,
		Identity:                 "test-worker",
		WorkflowTaskPollers:      1,
		ActivityTaskPollers:      1,
		DeadlockDetectionTimeout: time.Second,
		FailedTaskBackoff:        20 * time.Millisecond,
		Logger:                   testLogger(e.t, slog.LevelError),
	}
	for _, m := range mutators {
		m(&opts)
	}
	return New(e.engine, e.queue, opts)
}

func (e *testEnv) start() {
	e.t.Helper()
	require.NoError(e.t, e.worker.Start())
}

// replaceWorker stops the current worker and returns a fresh one, simulating a
// process restart: the sticky cache goes with it, so the replacement has to
// rebuild every workflow from history.
func (e *testEnv) replaceWorker(register func(*Worker)) *Worker {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(e.t, e.worker.Stop(ctx))

	w := e.newWorker()
	register(w)
	e.worker = w
	require.NoError(e.t, w.Start())
	return w
}

func testLogger(t *testing.T, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: level}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimSpace(string(p)))
	return len(p), nil
}

// ---------------------------------------------------------------------------
// Client-side operations
// ---------------------------------------------------------------------------

func (e *testEnv) startWorkflow(workflowID, workflowType string, args ...any) string {
	e.t.Helper()
	input, err := internalwf.EncodeArgs(skald.JSONConverter{}, args)
	require.NoError(e.t, err)

	resp, err := e.engine.StartWorkflow(context.Background(), api.StartWorkflowRequest{
		Namespace:    e.namespace,
		WorkflowID:   workflowID,
		WorkflowType: workflowType,
		TaskQueue:    e.queue,
		Input:        input,
		TaskTimeout:  testTaskTimeout,
	})
	require.NoError(e.t, err)
	return resp.RunID
}

func (e *testEnv) signal(workflowID, name string, arg any) {
	e.t.Helper()
	require.NoError(e.t, e.engine.SignalWorkflow(context.Background(), api.SignalWorkflowRequest{
		Namespace:  e.namespace,
		WorkflowID: workflowID,
		SignalName: name,
		Input:      skald.MustPayload(arg),
	}))
}

func (e *testEnv) cancel(workflowID string) {
	e.t.Helper()
	require.NoError(e.t, e.engine.CancelWorkflow(context.Background(), api.CancelWorkflowRequest{
		Namespace:  e.namespace,
		WorkflowID: workflowID,
		Reason:     "test",
	}))
}

func (e *testEnv) describe(workflowID, runID string) api.DescribeWorkflowResponse {
	e.t.Helper()
	resp, err := e.engine.DescribeWorkflow(context.Background(), e.namespace, workflowID, runID)
	require.NoError(e.t, err)
	return resp
}

func (e *testEnv) history(workflowID, runID string) history.History {
	e.t.Helper()
	resp, err := e.engine.GetHistory(context.Background(), api.GetHistoryRequest{
		Namespace: e.namespace, WorkflowID: workflowID, RunID: runID,
	})
	require.NoError(e.t, err)
	return resp.Events
}

// awaitClosed blocks until the run reaches a terminal state and returns it.
func (e *testEnv) awaitClosed(workflowID, runID string) api.DescribeWorkflowResponse {
	e.t.Helper()
	var last api.DescribeWorkflowResponse
	waitFor(e.t, "workflow "+workflowID+" to close", func() bool {
		last = e.describe(workflowID, runID)
		return last.Status.Terminal()
	})
	return last
}

// awaitResult waits for a successful completion and decodes its result.
//
// An empty runID means "follow the chain": a run that continued as new has not
// finished, it has handed off, so the wait moves to its successor. Any other
// terminal state fails immediately rather than burning the whole timeout.
func (e *testEnv) awaitResult(workflowID, runID string, out any) {
	e.t.Helper()
	var desc api.DescribeWorkflowResponse
	waitFor(e.t, "workflow "+workflowID+" to complete", func() bool {
		desc = e.describe(workflowID, runID)
		switch desc.Status {
		case skald.StatusCompleted:
			return true
		case skald.StatusRunning:
			return false
		case skald.StatusContinuedAsNew:
			if runID == "" {
				return false // the successor is now the current run
			}
		}
		e.t.Fatalf("workflow %s ended as %s, not completed:\n%s",
			workflowID, desc.Status, formatHistory(e.history(workflowID, desc.RunID)))
		return false
	})

	events := e.history(workflowID, desc.RunID)
	require.NotEmpty(e.t, events)
	last := events[len(events)-1]
	attrs := history.MustAttributes[history.WorkflowExecutionCompletedAttributes](last)
	if out != nil {
		require.NoError(e.t, skald.JSONConverter{}.FromPayload(attrs.Result, out))
	}
}

func formatHistory(events history.History) string {
	var b strings.Builder
	for _, ev := range events {
		b.WriteString("  " + ev.String() + "\n")
	}
	return b.String()
}

func (e *testEnv) eventTypes(workflowID, runID string) []history.EventType {
	e.t.Helper()
	events := e.history(workflowID, runID)
	out := make([]history.EventType, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type())
	}
	return out
}

func (e *testEnv) countEvents(workflowID, runID string, t history.EventType) int {
	e.t.Helper()
	n := 0
	for _, got := range e.eventTypes(workflowID, runID) {
		if got == t {
			n++
		}
	}
	return n
}

// waitFor polls cond until it holds, failing the test with a useful name if it
// never does. Polling beats a fixed sleep: the assertion is about the condition,
// not about how long a loaded CI machine happened to take.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", testWait, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
