package frontend

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/internal/engine"
	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/internal/persistence/memory"
	"github.com/skald-io/skald/internal/telemetry"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/client"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// TestEndToEnd wires the real stack -- memory store, engine, telemetry,
// frontend, HTTP client -- and drives one workflow from start to result.
//
// The unit tests above stub the service so that transport behaviour can be
// asserted in isolation. This one exists to catch the failures that only appear
// where the layers meet: a field that does not survive JSON, an endpoint the
// client calls with the wrong shape, a status the client misreads. It is the
// same code path a worker takes in production, minus the worker.
func TestEndToEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	reg := prometheus.NewRegistry()
	tel, err := telemetry.New(telemetry.Config{
		ServiceName: "skald-e2e",
		Registry:    reg,
		Logger:      slog.New(slog.NewJSONHandler(&safeBuffer{}, nil)),
	})
	require.NoError(t, err)

	matcher := matching.New(matching.Config{
		Metrics: tel.Metrics.MatchingMetrics(),
		// Short, so a poll for work that never arrives does not hold the test
		// open for the default thirty seconds.
		PollTimeout: 2 * time.Second,
	})
	t.Cleanup(matcher.Close)

	eng, err := engine.New(engine.Config{
		Store:            store,
		Matcher:          matcher,
		DefaultNamespace: skald.DefaultNamespace,
	})
	require.NoError(t, err)
	require.NoError(t, eng.Start(ctx))
	t.Cleanup(func() { _ = eng.Close(context.Background()) })

	srv, err := New(Config{
		Service:         telemetry.InstrumentService(eng, tel, skald.DefaultNamespace),
		Telemetry:       tel,
		MaxPollDuration: 5 * time.Second,
		ReadyCheck:      func(context.Context) error { return nil },
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	c, err := client.New(ts.URL, client.WithIdentity("e2e-worker"))
	require.NoError(t, err)
	t.Cleanup(c.Close)

	// --- start -------------------------------------------------------------

	handle, err := c.ExecuteWorkflow(ctx, client.WorkflowOptions{
		ID:        "order-1",
		Type:      "OrderWorkflow",
		TaskQueue: "orders",
	}, map[string]int{"total": 4200})
	require.NoError(t, err)
	require.Equal(t, "order-1", handle.WorkflowID())
	require.NotEmpty(t, handle.RunID())

	described, err := handle.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, skald.StatusRunning, described.Status)
	require.Equal(t, "OrderWorkflow", described.WorkflowType)

	// --- poll and complete the workflow task --------------------------------

	task, err := c.PollWorkflowTask(ctx, api.PollWorkflowTaskRequest{TaskQueue: "orders"})
	require.NoError(t, err)
	require.False(t, task.Empty, "the start should have made a workflow task pollable")
	require.Equal(t, "order-1", task.Execution.WorkflowID)
	require.Equal(t, "OrderWorkflow", task.WorkflowType)

	// The task carries the whole history, and the input survived the round trip
	// through JSON in both directions.
	require.GreaterOrEqual(t, len(task.History), 3)
	started, ok := task.History.StartedAttributes()
	require.True(t, ok)
	require.JSONEq(t, `{"total":4200}`, string(started.Input.Data))

	require.NoError(t, c.RespondWorkflowTaskCompleted(ctx, api.RespondWorkflowTaskCompletedRequest{
		Execution: task.Execution,
		Commands: []history.Command{{
			Type: history.CommandTypeCompleteWorkflowExecution,
			CompleteWorkflow: &history.CompleteWorkflowCommand{
				Result: skald.MustPayload(map[string]string{"status": "charged"}),
			},
		}},
	}))

	// --- result -------------------------------------------------------------

	var result map[string]string
	require.NoError(t, handle.Result(ctx, &result))
	require.Equal(t, map[string]string{"status": "charged"}, result)

	// --- the operator's view ------------------------------------------------

	events, err := handle.History(ctx)
	require.NoError(t, err)
	require.NoError(t, events.Validate(), "a history read back over HTTP must still be structurally sound")
	require.True(t, events.Terminated())

	listed, err := c.ListWorkflows(ctx, api.ListWorkflowsRequest{})
	require.NoError(t, err)
	require.Len(t, listed.Executions, 1)
	require.Equal(t, skald.StatusCompleted, listed.Executions[0].Status)

	// --- metrics ------------------------------------------------------------

	resp, err := ts.Client().Get(ts.URL + api.PathMetrics)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := readAllString(t, resp)
	require.Contains(t, body, `skald_workflow_completions_total{namespace="default",status="COMPLETED"} 1`)
	require.Contains(t, body, `skald_requests_total{code="ok",operation="StartWorkflow"} 1`)
	// A sync match means a poller was already waiting; an async one means the
	// task queued first. Either is fine here -- what matters is that the
	// matching layer's counters reached the registry at all.
	require.Contains(t, body, "skald_task_matches_total")
}

// TestEndToEndSignalAndTerminate drives the two operator paths that do not go
// through a worker at all.
func TestEndToEndSignalAndTerminate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	eng, err := engine.New(engine.Config{Store: store, DefaultNamespace: skald.DefaultNamespace})
	require.NoError(t, err)
	require.NoError(t, eng.Start(ctx))
	t.Cleanup(func() { _ = eng.Close(context.Background()) })

	h := newHarness(t, eng)
	c, err := client.New(h.http.URL)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	handle, err := c.ExecuteWorkflow(ctx, client.WorkflowOptions{
		ID: "order-2", Type: "OrderWorkflow", TaskQueue: "orders",
	}, nil)
	require.NoError(t, err)

	require.NoError(t, handle.Signal(ctx, "approve", map[string]string{"by": "alice"}))
	require.NoError(t, handle.Terminate(ctx, "operator intervention, INC-1", nil))

	// Result reconstructs the typed error from the terminal event, so a caller
	// can branch with errors.As rather than parsing a message.
	err = handle.Result(ctx, nil)
	var terminated *skald.TerminatedError
	require.ErrorAs(t, err, &terminated)
	require.Equal(t, "operator intervention, INC-1", terminated.Reason)

	events, err := handle.History(ctx)
	require.NoError(t, err)
	require.NoError(t, events.Validate())

	signals := events.Filter(history.EventTypeWorkflowExecutionSignaled)
	require.Len(t, signals, 1)
	attrs, ok := history.AttributesAs[history.WorkflowExecutionSignaledAttributes](signals[0])
	require.True(t, ok)
	require.Equal(t, "approve", attrs.SignalName)

	// Terminating an execution twice is a failed precondition, not a 500, and
	// the client must surface the code rather than a status.
	err = handle.Terminate(ctx, "again", nil)
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeFailedPrecondition, apiErr.Code)
}
