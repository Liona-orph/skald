package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/internal/telemetry"
	"github.com/Liona-orph/skald/pkg/api"
)

// stubService is a programmable api.Service.
//
// Every method has a function field that defaults to "succeed with the zero
// value", so a test overrides only the one call it is about. That keeps each
// test's setup to the two or three lines that actually matter, which is what
// makes a failure readable a year later.
type stubService struct {
	start           func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error)
	signal          func(context.Context, api.SignalWorkflowRequest) error
	signalWithStart func(context.Context, api.SignalWithStartRequest) (api.StartWorkflowResponse, error)
	cancel          func(context.Context, api.CancelWorkflowRequest) error
	terminate       func(context.Context, api.TerminateWorkflowRequest) error
	describe        func(context.Context, string, string, string) (api.DescribeWorkflowResponse, error)
	getHistory      func(context.Context, api.GetHistoryRequest) (api.GetHistoryResponse, error)
	list            func(context.Context, api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error)

	pollWorkflow    func(context.Context, api.PollWorkflowTaskRequest) (api.WorkflowTask, error)
	respondWFDone   func(context.Context, api.RespondWorkflowTaskCompletedRequest) error
	respondWFFailed func(context.Context, api.RespondWorkflowTaskFailedRequest) error

	pollActivity     func(context.Context, api.PollActivityTaskRequest) (api.ActivityTask, error)
	respondActDone   func(context.Context, api.RespondActivityTaskCompletedRequest) error
	respondActFailed func(context.Context, api.RespondActivityTaskFailedRequest) error
	respondActCancel func(context.Context, api.RespondActivityTaskCanceledRequest) error
	heartbeat        func(context.Context, api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error)
}

var _ api.Service = (*stubService)(nil)

func (s *stubService) StartWorkflow(ctx context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
	if s.start != nil {
		return s.start(ctx, req)
	}
	return api.StartWorkflowResponse{}, nil
}

func (s *stubService) SignalWorkflow(ctx context.Context, req api.SignalWorkflowRequest) error {
	if s.signal != nil {
		return s.signal(ctx, req)
	}
	return nil
}

func (s *stubService) SignalWithStartWorkflow(ctx context.Context, req api.SignalWithStartRequest) (api.StartWorkflowResponse, error) {
	if s.signalWithStart != nil {
		return s.signalWithStart(ctx, req)
	}
	return api.StartWorkflowResponse{}, nil
}

func (s *stubService) CancelWorkflow(ctx context.Context, req api.CancelWorkflowRequest) error {
	if s.cancel != nil {
		return s.cancel(ctx, req)
	}
	return nil
}

func (s *stubService) TerminateWorkflow(ctx context.Context, req api.TerminateWorkflowRequest) error {
	if s.terminate != nil {
		return s.terminate(ctx, req)
	}
	return nil
}

func (s *stubService) DescribeWorkflow(ctx context.Context, ns, wid, rid string) (api.DescribeWorkflowResponse, error) {
	if s.describe != nil {
		return s.describe(ctx, ns, wid, rid)
	}
	return api.DescribeWorkflowResponse{}, nil
}

func (s *stubService) GetHistory(ctx context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
	if s.getHistory != nil {
		return s.getHistory(ctx, req)
	}
	return api.GetHistoryResponse{}, nil
}

func (s *stubService) ListWorkflows(ctx context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
	if s.list != nil {
		return s.list(ctx, req)
	}
	return api.ListWorkflowsResponse{}, nil
}

func (s *stubService) PollWorkflowTask(ctx context.Context, req api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	if s.pollWorkflow != nil {
		return s.pollWorkflow(ctx, req)
	}
	return api.WorkflowTask{Empty: true}, nil
}

func (s *stubService) RespondWorkflowTaskCompleted(ctx context.Context, req api.RespondWorkflowTaskCompletedRequest) error {
	if s.respondWFDone != nil {
		return s.respondWFDone(ctx, req)
	}
	return nil
}

func (s *stubService) RespondWorkflowTaskFailed(ctx context.Context, req api.RespondWorkflowTaskFailedRequest) error {
	if s.respondWFFailed != nil {
		return s.respondWFFailed(ctx, req)
	}
	return nil
}

func (s *stubService) PollActivityTask(ctx context.Context, req api.PollActivityTaskRequest) (api.ActivityTask, error) {
	if s.pollActivity != nil {
		return s.pollActivity(ctx, req)
	}
	return api.ActivityTask{Empty: true}, nil
}

func (s *stubService) RespondActivityTaskCompleted(ctx context.Context, req api.RespondActivityTaskCompletedRequest) error {
	if s.respondActDone != nil {
		return s.respondActDone(ctx, req)
	}
	return nil
}

func (s *stubService) RespondActivityTaskFailed(ctx context.Context, req api.RespondActivityTaskFailedRequest) error {
	if s.respondActFailed != nil {
		return s.respondActFailed(ctx, req)
	}
	return nil
}

func (s *stubService) RespondActivityTaskCanceled(ctx context.Context, req api.RespondActivityTaskCanceledRequest) error {
	if s.respondActCancel != nil {
		return s.respondActCancel(ctx, req)
	}
	return nil
}

func (s *stubService) RecordActivityHeartbeat(ctx context.Context, req api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error) {
	if s.heartbeat != nil {
		return s.heartbeat(ctx, req)
	}
	return api.RecordActivityHeartbeatResponse{}, nil
}

// safeBuffer is a bytes.Buffer that survives concurrent writers. The access log
// is written from whichever goroutine served the request, so an unsynchronised
// buffer here would fail under -race for reasons that have nothing to do with
// the code under test.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testHarness is a running frontend plus the things a test asserts against.
type testHarness struct {
	server *Server
	http   *httptest.Server
	logs   *safeBuffer
	client *http.Client
}

// newHarness builds a frontend backed by svc and serves it over loopback.
func newHarness(t *testing.T, svc api.Service, mutate ...func(*Config)) *testHarness {
	t.Helper()

	logs := &safeBuffer{}
	tel, err := telemetry.New(telemetry.Config{
		ServiceName: "skald-test",
		// A fresh registry per harness: metric registration is global to a
		// registry, so sharing one across tests makes the second one fail.
		Registry: prometheus.NewRegistry(),
		Logger:   slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)

	cfg := Config{
		Service:   svc,
		Telemetry: tel,
		Logger:    slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		ReadyCheck: func(context.Context) error {
			return nil
		},
	}
	for _, m := range mutate {
		m(&cfg)
	}

	srv, err := New(cfg)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return &testHarness{server: srv, http: ts, logs: logs, client: ts.Client()}
}

// post sends a JSON request body to path.
func (h *testHarness) post(t *testing.T, path string, body any) *http.Response {
	t.Helper()
	return h.do(t, h.request(t, http.MethodPost, path, body))
}

func (h *testHarness) request(t *testing.T, method, path string, body any) *http.Request {
	t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	case []byte:
		reader = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.http.URL+path, reader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func (h *testHarness) do(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := h.client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// decodeBody reads a JSON response into out.
func decodeBody(t *testing.T, resp *http.Response, out any) {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, out), "body was: %s", raw)
}

// decodeAPIError reads an api.Error response.
func decodeAPIError(t *testing.T, resp *http.Response) api.Error {
	t.Helper()
	var apiErr api.Error
	decodeBody(t, resp, &apiErr)
	return apiErr
}

// decodeJSONBytes decodes a raw body, used where the test unwrapped the
// transport encoding itself.
func decodeJSONBytes(raw []byte, out any) error { return json.Unmarshal(raw, out) }

// readAllString drains a response body.
func readAllString(t *testing.T, resp *http.Response) string {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(raw)
}
