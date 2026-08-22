package frontend

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

func TestStartWorkflowHappyPath(t *testing.T) {
	t.Parallel()

	var got api.StartWorkflowRequest
	h := newHarness(t, &stubService{
		start: func(_ context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			got = req
			return api.StartWorkflowResponse{RunID: "run-1", Started: true}, nil
		},
	})

	resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{
		Namespace:    "prod",
		WorkflowID:   "order-1",
		WorkflowType: "OrderWorkflow",
		TaskQueue:    "orders",
		RunTimeout:   30 * time.Second,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out api.StartWorkflowResponse
	decodeBody(t, resp, &out)
	require.Equal(t, "run-1", out.RunID)
	require.True(t, out.Started)

	// The request must arrive at the service intact, including the duration,
	// which is the field most likely to be mangled by a JSON round trip.
	require.Equal(t, "order-1", got.WorkflowID)
	require.Equal(t, "prod", got.Namespace)
	require.Equal(t, 30*time.Second, got.RunTimeout)

	require.Equal(t, api.Version, resp.Header.Get(HeaderProtocolVersion))
	require.NotEmpty(t, resp.Header.Get(HeaderRequestID))
}

func TestVoidOperationsReturnAnObject(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	for _, path := range []string{
		api.PathSignalWorkflow,
		api.PathCancelWorkflow,
		api.PathTerminateWorkflow,
		api.PathCompleteWorkflow,
		api.PathFailWorkflowTask,
		api.PathCompleteActivity,
		api.PathFailActivity,
		api.PathCancelActivity,
	} {
		resp := h.post(t, path, map[string]any{})
		require.Equal(t, http.StatusOK, resp.StatusCode, path)

		// A JSON object, not an empty body: one decode path serves every
		// endpoint and there is room to add fields without a protocol break.
		var out map[string]any
		decodeBody(t, resp, &out)
		require.Empty(t, out, path)
	}
}

func TestDescribeAcceptsGetAndPost(t *testing.T) {
	t.Parallel()

	type call struct{ ns, wid, rid string }
	var calls []call
	h := newHarness(t, &stubService{
		describe: func(_ context.Context, ns, wid, rid string) (api.DescribeWorkflowResponse, error) {
			calls = append(calls, call{ns, wid, rid})
			return api.DescribeWorkflowResponse{WorkflowID: wid, RunID: rid, Status: skald.StatusRunning}, nil
		},
	})

	resp := h.post(t, api.PathDescribeWorkflow, map[string]string{
		"namespace": "prod", "workflow_id": "order-1", "run_id": "run-9",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req := h.request(t, http.MethodGet,
		api.PathDescribeWorkflow+"?namespace=prod&workflow_id=order-1&run_id=run-9", nil)
	resp = h.do(t, req)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, []call{{"prod", "order-1", "run-9"}, {"prod", "order-1", "run-9"}}, calls)
}

func TestGetHistoryReturnsEvents(t *testing.T) {
	t.Parallel()

	events := history.History{{
		ID:   1,
		Time: time.Now().UTC(),
		Attrs: history.WorkflowExecutionStartedAttributes{
			WorkflowType: "OrderWorkflow",
			TaskQueue:    "orders",
		},
	}}
	h := newHarness(t, &stubService{
		getHistory: func(_ context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
			require.Equal(t, int64(1), req.FromEventID)
			return api.GetHistoryResponse{Events: events, Status: skald.StatusRunning, NextEventID: 2}, nil
		},
	})

	resp := h.post(t, api.PathGetHistory, api.GetHistoryRequest{WorkflowID: "order-1", FromEventID: 1})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out api.GetHistoryResponse
	decodeBody(t, resp, &out)
	require.Len(t, out.Events, 1)
	require.Equal(t, history.EventTypeWorkflowExecutionStarted, out.Events[0].Type())
	require.Equal(t, int64(2), out.NextEventID)
}

func TestPollReturnsEmptyTaskWhenTheServerCapExpires(t *testing.T) {
	t.Parallel()

	// The service blocks until its context ends, which is what a real matcher
	// with no work does. The frontend must convert that into an empty task
	// rather than a timeout error: an idle worker is the normal state of a
	// healthy deployment and must not look like a fault.
	h := newHarness(t, &stubService{
		pollWorkflow: func(ctx context.Context, _ api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
			<-ctx.Done()
			return api.WorkflowTask{}, &api.Error{Code: api.CodeDeadlineExceeded, Message: "poll expired"}
		},
	}, func(c *Config) { c.MaxPollDuration = 50 * time.Millisecond })

	start := time.Now()
	resp := h.post(t, api.PathPollWorkflowTask, api.PollWorkflowTaskRequest{TaskQueue: "orders"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Less(t, time.Since(start), 5*time.Second)

	var task api.WorkflowTask
	decodeBody(t, resp, &task)
	require.True(t, task.Empty)
}

func TestActivityPollReturnsEmptyTaskWhenTheServerCapExpires(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		pollActivity: func(ctx context.Context, _ api.PollActivityTaskRequest) (api.ActivityTask, error) {
			<-ctx.Done()
			return api.ActivityTask{}, ctx.Err()
		},
	}, func(c *Config) { c.MaxPollDuration = 50 * time.Millisecond })

	resp := h.post(t, api.PathPollActivityTask, api.PollActivityTaskRequest{TaskQueue: "orders"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var task api.ActivityTask
	decodeBody(t, resp, &task)
	require.True(t, task.Empty)
}

func TestLongPollHonoursClientDisconnect(t *testing.T) {
	t.Parallel()

	observed := make(chan error, 1)
	h := newHarness(t, &stubService{
		pollWorkflow: func(ctx context.Context, _ api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
			<-ctx.Done()
			observed <- ctx.Err()
			return api.WorkflowTask{}, ctx.Err()
		},
	}, func(c *Config) {
		// Far longer than the test is willing to wait, so a pass proves the
		// disconnect propagated rather than that the cap fired.
		c.MaxPollDuration = 30 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	req := h.request(t, http.MethodPost, api.PathPollWorkflowTask, api.PollWorkflowTaskRequest{TaskQueue: "orders"})
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The request is cancelled mid-flight; there is no body to close.
		_, _ = h.client.Do(req)
	}()

	// Give the server a moment to park in the handler, then hang up.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-observed:
		require.Error(t, err, "the handler's context must end when the client goes away")
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not observe the client disconnect")
	}
	<-done
}

func TestGetHistoryLongPollExpiryReturnsCursor(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		getHistory: func(ctx context.Context, _ api.GetHistoryRequest) (api.GetHistoryResponse, error) {
			<-ctx.Done()
			return api.GetHistoryResponse{}, ctx.Err()
		},
	}, func(c *Config) { c.MaxPollDuration = 50 * time.Millisecond })

	resp := h.post(t, api.PathGetHistory, api.GetHistoryRequest{
		WorkflowID: "order-1", FromEventID: 7, WaitForNew: true,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The cursor comes back unchanged so the next poll resumes exactly where
	// this one started; anything else would skip or replay events.
	var out api.GetHistoryResponse
	decodeBody(t, resp, &out)
	require.Empty(t, out.Events)
	require.Equal(t, int64(7), out.NextEventID)
}

func TestHeartbeatResponseIsForwarded(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		heartbeat: func(context.Context, api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error) {
			return api.RecordActivityHeartbeatResponse{CancelRequested: true}, nil
		},
	})
	resp := h.post(t, api.PathHeartbeatActivity, api.RecordActivityHeartbeatRequest{ScheduledEventID: 5})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out api.RecordActivityHeartbeatResponse
	decodeBody(t, resp, &out)
	require.True(t, out.CancelRequested)
}

func TestMalformedBodies(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})

	cases := []struct {
		name       string
		body       string
		wantDetail string
	}{
		{name: "not json", body: `{"workflow_id":`, wantDetail: "not valid JSON"},
		{name: "wrong type", body: `{"workflow_id": 42}`, wantDetail: "expects"},
		{
			// The whole point of DisallowUnknownFields: a typo'd field silently
			// ignored is a workflow started without the setting the caller
			// believed they had passed.
			name: "unknown field", body: `{"workflow_idd":"x"}`, wantDetail: "unknown field",
		},
		{name: "two documents", body: `{"workflow_id":"a"}{"workflow_id":"b"}`, wantDetail: "more than one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.post(t, api.PathStartWorkflow, tc.body)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)

			apiErr := decodeAPIError(t, resp)
			require.Equal(t, api.CodeInvalidArgument, apiErr.Code)
			require.Contains(t, apiErr.Message, tc.wantDetail)
		})
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	t.Parallel()

	served := false
	h := newHarness(t, &stubService{
		start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			served = true
			return api.StartWorkflowResponse{}, nil
		},
	}, func(c *Config) { c.MaxRequestBytes = 128 })

	body := `{"workflow_id":"` + strings.Repeat("x", 4096) + `"}`
	resp := h.post(t, api.PathStartWorkflow, body)

	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	apiErr := decodeAPIError(t, resp)
	require.Equal(t, api.CodeInvalidArgument, apiErr.Code)
	require.Equal(t, "128", apiErr.Details["limit_bytes"])
	require.False(t, served, "an oversized body must never reach the service")
}

func TestEmptyBodyReachesTheServiceAsAZeroRequest(t *testing.T) {
	t.Parallel()

	// The service produces a far better message than the transport could
	// ("workflow id must not be empty"), so an empty body is passed through
	// rather than rejected at the boundary.
	called := false
	h := newHarness(t, &stubService{
		start: func(_ context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			called = true
			require.Empty(t, req.WorkflowID)
			return api.StartWorkflowResponse{}, nil
		},
	})
	resp := h.post(t, api.PathStartWorkflow, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, called)
}

func TestUnknownPathReturnsTheProtocolEnvelope(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	resp := h.do(t, h.request(t, http.MethodGet, "/api/v1/nope", nil))

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	apiErr := decodeAPIError(t, resp)
	require.Equal(t, api.CodeNotFound, apiErr.Code)
	require.Equal(t, "/api/v1/nope", apiErr.Details["path"])
}

func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	resp := h.do(t, h.request(t, http.MethodGet, api.PathStartWorkflow, nil))

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.Equal(t, http.MethodPost, resp.Header.Get("Allow"))
	require.Equal(t, api.CodeInvalidArgument, decodeAPIError(t, resp).Code)
}

func TestHealthIsIndependentOfTheStore(t *testing.T) {
	t.Parallel()

	// Liveness must not depend on a dependency: restarting a healthy process
	// does not fix a broken database, it only destroys in-flight work.
	h := newHarness(t, &stubService{}, func(c *Config) {
		c.ReadyCheck = func(context.Context) error { return context.DeadlineExceeded }
	})

	resp := h.do(t, h.request(t, http.MethodGet, api.PathHealth, nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = h.do(t, h.request(t, http.MethodGet, api.PathReady, nil))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, api.CodeUnavailable, decodeAPIError(t, resp).Code)
}

func TestReadyReportsHealthyWhenTheStoreAnswers(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	resp := h.do(t, h.request(t, http.MethodGet, api.PathReady, nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body healthResponse
	decodeBody(t, resp, &body)
	require.Equal(t, "ok", body.Status)
}

func TestReadinessErrorIsLoggedNotReturned(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{}, func(c *Config) {
		c.ReadyCheck = func(context.Context) error {
			return &api.Error{Code: api.CodeInternal, Message: "dsn=postgres://user:hunter2@db/skald"}
		}
	})
	resp := h.do(t, h.request(t, http.MethodGet, api.PathReady, nil))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// A readiness body is scraped by infrastructure and ends up in places a log
	// does not; store errors can carry credentials.
	require.NotContains(t, decodeAPIError(t, resp).Message, "hunter2")
	require.Contains(t, h.logs.String(), "hunter2")
}

func TestMetricsEndpointExposesSkaldSeries(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})

	resp := h.do(t, h.request(t, http.MethodGet, api.PathMetrics, nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := readAllString(t, resp)
	require.Contains(t, body, "skald_requests_total")
	require.Contains(t, body, `operation="StartWorkflow"`)
	// The cardinality rule: no metric is ever labelled by a caller-chosen id.
	require.NotContains(t, body, `workflow_id=`)
}
