package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/skald"
)

// fastRetries makes a test's retries take microseconds instead of seconds while
// keeping the exponential shape, and pins the jitter so the delays are exact.
func fastRetries(attempts int) []Option {
	return []Option{
		WithRetryPolicy(RetryPolicy{
			MaxAttempts:        attempts,
			InitialInterval:    time.Millisecond,
			MaxInterval:        5 * time.Millisecond,
			BackoffCoefficient: 2,
		}),
		WithJitter(func() float64 { return 0 }),
	}
}

func newTestClient(t *testing.T, handler http.Handler, opts ...Option) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}

func TestNewValidatesTheBaseURL(t *testing.T) {
	t.Parallel()

	_, err := New("")
	require.ErrorContains(t, err, "base URL is required")

	_, err = New("skald://localhost:7233")
	require.ErrorContains(t, err, "must use http or https")

	_, err = New("http://localhost:7233", WithNamespace(""))
	require.ErrorContains(t, err, "namespace must not be empty")
}

func TestStartWorkflowRoundTrip(t *testing.T) {
	t.Parallel()

	var got api.StartWorkflowRequest
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, api.PathStartWorkflow, r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, api.Version, r.Header.Get(headerProtocolVersion))
		require.NotEmpty(t, r.Header.Get(headerRequestID))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1", Started: true})
	}), WithNamespace("prod"), WithIdentity("tester"))

	resp, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{
		WorkflowID:   "order-1",
		WorkflowType: "OrderWorkflow",
		TaskQueue:    "orders",
		RunTimeout:   90 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "run-1", resp.RunID)

	require.Equal(t, "prod", got.Namespace, "the client's namespace fills in")
	require.Equal(t, "tester", got.Identity)
	require.Equal(t, 90*time.Second, got.RunTimeout)
	// Without a request ID a retried start can create two runs from one intent,
	// which for a payment workflow is the failure the whole system exists to
	// prevent. The client fills it in so the guarantee holds for every caller.
	require.NotEmpty(t, got.RequestID)
}

func TestIdempotencyKeysAreStableAcrossRetries(t *testing.T) {
	t.Parallel()

	var ids []string
	var attempts atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.StartWorkflowRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		ids = append(ids, req.RequestID)
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}), fastRetries(4)...)

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "order-1"})
	require.NoError(t, err)

	require.Len(t, ids, 3)
	require.Equal(t, ids[0], ids[1])
	require.Equal(t, ids[1], ids[2], "a retry must dedupe against the original, not start a second run")
}

func TestRetriesOnServerErrors(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}), fastRetries(5)...)

	resp, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	require.NoError(t, err)
	require.Equal(t, "run-1", resp.RunID)
	require.Equal(t, int32(3), attempts.Load())
}

func TestRetriesAreExhaustedAndTheLastErrorIsReturned(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writeJSONResponse(t, w, http.StatusServiceUnavailable, api.Error{
			Code: api.CodeUnavailable, Message: "draining",
		})
	}), fastRetries(3)...)

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	require.Equal(t, int32(3), attempts.Load())

	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeUnavailable, apiErr.Code)
	require.Equal(t, "draining", apiErr.Message)
}

func TestNeverRetriesClientErrors(t *testing.T) {
	t.Parallel()

	// Including 429. A client that answers "you are sending too much" by
	// sending more is how a brownout becomes an outage; the RetryAfter is
	// surfaced so the caller can decide with knowledge this layer lacks.
	for _, tc := range []struct {
		status int
		code   string
	}{
		{http.StatusBadRequest, api.CodeInvalidArgument},
		{http.StatusNotFound, api.CodeNotFound},
		{http.StatusConflict, api.CodeAlreadyExists},
		{http.StatusPreconditionFailed, api.CodeFailedPrecondition},
		{http.StatusTooManyRequests, api.CodeResourceExhausted},
		{http.StatusUnauthorized, api.CodeUnauthorized},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			var attempts atomic.Int32
			c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.Header().Set("Retry-After", "1")
				writeJSONResponse(t, w, tc.status, api.Error{Code: tc.code, Message: "no"})
			}), fastRetries(5)...)

			_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
			require.Error(t, err)
			require.Equal(t, int32(1), attempts.Load())

			var apiErr *api.Error
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tc.code, apiErr.Code)
		})
	}
}

func TestUnavailableInTheBodyIsRetriedWhateverTheStatus(t *testing.T) {
	t.Parallel()

	// The body is authoritative precisely because a proxy can rewrite the
	// status; "unavailable" is the engine saying "ask again".
	var attempts atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 2 {
			writeJSONResponse(t, w, http.StatusBadRequest, api.Error{Code: api.CodeUnavailable, Message: "shutting down"})
			return
		}
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}), fastRetries(3)...)

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	require.NoError(t, err)
	require.Equal(t, int32(2), attempts.Load())
}

func TestRetryAfterIsRespected(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var gap time.Duration
	var previous time.Time

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		if attempts.Add(1) == 1 {
			previous = now
			// Far longer than the 0.5ms the backoff would have chosen, so a
			// pass proves the header won.
			w.Header().Set("Retry-After", "1")
			writeJSONResponse(t, w, http.StatusServiceUnavailable, api.Error{Code: api.CodeUnavailable})
			return
		}
		gap = now.Sub(previous)
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}), fastRetries(3)...)

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, gap, 900*time.Millisecond, "the server's Retry-After must not be shortened")
}

func TestRetriesConnectionFailures(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close() // nothing is listening any more

	c, err := New(addr, fastRetries(3)...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	_, err = c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeUnavailable, apiErr.Code)
}

func TestCallerCancellationBeatsTheRetryPolicy(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		cancel()
		w.WriteHeader(http.StatusInternalServerError)
	}), WithRetryPolicy(RetryPolicy{
		MaxAttempts: 10, InitialInterval: time.Millisecond, MaxInterval: time.Millisecond, BackoffCoefficient: 1,
	}))

	_, err := c.StartWorkflow(ctx, api.StartWorkflowRequest{WorkflowID: "a"})
	require.Error(t, err)
	require.Equal(t, int32(1), attempts.Load(), "a cancelled caller does not want another attempt")
}

func TestNonJSONErrorBodyIsClassifiedFromTheStatus(t *testing.T) {
	t.Parallel()

	// This is what a load balancer or a captive portal returns. Without a
	// snippet of the body, "the client got a 502" is unactionable.
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html><body>502 Bad Gateway (nginx)</body></html>")
	}), fastRetries(1)...)

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeUnavailable, apiErr.Code)
	require.Contains(t, apiErr.Message, "502")
	require.Contains(t, apiErr.Message, "nginx")
}

func TestLongPollUsesItsOwnDeadline(t *testing.T) {
	t.Parallel()

	// The general request timeout is 20ms; the poll timeout is a second. A poll
	// that took 200ms must survive, or every healthy poll would look like a
	// client-side fault.
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.PathPollWorkflowTask {
			time.Sleep(200 * time.Millisecond)
			writeJSONResponse(t, w, http.StatusOK, api.WorkflowTask{Empty: true})
			return
		}
		time.Sleep(200 * time.Millisecond)
		writeJSONResponse(t, w, http.StatusOK, api.ListWorkflowsResponse{})
	}),
		WithRequestTimeout(20*time.Millisecond),
		WithPollTimeout(time.Second),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1, InitialInterval: time.Millisecond, MaxInterval: time.Millisecond, BackoffCoefficient: 1}),
	)

	task, err := c.PollWorkflowTask(context.Background(), api.PollWorkflowTaskRequest{TaskQueue: "orders"})
	require.NoError(t, err)
	require.True(t, task.Empty)

	_, err = c.ListWorkflows(context.Background(), api.ListWorkflowsRequest{})
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeDeadlineExceeded, apiErr.Code)
}

func TestGetHistoryUsesThePollDeadlineOnlyWhenWaiting(t *testing.T) {
	t.Parallel()

	var waiting []bool
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.GetHistoryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		waiting = append(waiting, req.WaitForNew)
		if req.WaitForNew {
			time.Sleep(120 * time.Millisecond)
		}
		writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{NextEventID: 1})
	}),
		WithRequestTimeout(40*time.Millisecond),
		WithPollTimeout(2*time.Second),
		fastRetries(1)[0], fastRetries(1)[1],
	)

	_, err := c.GetHistory(context.Background(), api.GetHistoryRequest{WorkflowID: "a"})
	require.NoError(t, err)

	_, err = c.GetHistory(context.Background(), api.GetHistoryRequest{WorkflowID: "a", WaitForNew: true})
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, waiting)
}

func TestAuthTokenIsSent(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer s3cret", r.Header.Get("Authorization"))
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{})
	}), WithAuthToken("s3cret"))

	_, err := c.StartWorkflow(context.Background(), api.StartWorkflowRequest{WorkflowID: "a"})
	require.NoError(t, err)
}

func TestDescribeSendsTheMirroredRequestShape(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// The field names must match internal/frontend's describeRequest; the
		// two definitions exist only because pkg/api has no request type.
		require.Equal(t, "prod", body["namespace"])
		require.Equal(t, "order-1", body["workflow_id"])
		require.Equal(t, "run-9", body["run_id"])
		writeJSONResponse(t, w, http.StatusOK, api.DescribeWorkflowResponse{
			WorkflowID: "order-1", Status: skald.StatusRunning,
		})
	}), WithNamespace("prod"))

	got, err := c.DescribeWorkflow(context.Background(), "", "order-1", "run-9")
	require.NoError(t, err)
	require.Equal(t, "order-1", got.WorkflowID)
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	c, err := New("http://example.invalid",
		WithRetryPolicy(RetryPolicy{
			MaxAttempts: 6, InitialInterval: 100 * time.Millisecond,
			MaxInterval: 500 * time.Millisecond, BackoffCoefficient: 2,
		}),
		WithJitter(func() float64 { return 0 }),
	)
	require.NoError(t, err)

	// Equal jitter: half the interval fixed, half random. With the draw pinned
	// at zero the delay is exactly half the interval, which is the floor -- the
	// property that stops a retry landing microseconds after the failure.
	require.Equal(t, 50*time.Millisecond, c.backoff(1, 0))
	require.Equal(t, 100*time.Millisecond, c.backoff(2, 0))
	require.Equal(t, 200*time.Millisecond, c.backoff(3, 0))
	require.Equal(t, 250*time.Millisecond, c.backoff(4, 0), "capped by MaxInterval")
	require.Equal(t, 250*time.Millisecond, c.backoff(9, 0))

	// The server's instruction is never shortened.
	require.Equal(t, 3*time.Second, c.backoff(1, 3*time.Second))
	// ...and never lengthens a delay that was already longer.
	require.Equal(t, 250*time.Millisecond, c.backoff(4, 10*time.Millisecond))

	full, err := New("http://example.invalid",
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2, InitialInterval: time.Second, MaxInterval: time.Second, BackoffCoefficient: 2}),
		WithJitter(func() float64 { return 0.5 }),
	)
	require.NoError(t, err)
	require.Equal(t, 750*time.Millisecond, full.backoff(1, 0))
}

func TestBackoffToleratesAHostileJitterSource(t *testing.T) {
	t.Parallel()

	for _, draw := range []float64{-1, 2, 1} {
		c, err := New("http://example.invalid",
			WithRetryPolicy(RetryPolicy{MaxAttempts: 2, InitialInterval: time.Second, MaxInterval: time.Second, BackoffCoefficient: 2}),
			WithJitter(func() float64 { return draw }),
		)
		require.NoError(t, err)
		d := c.backoff(1, 0)
		require.GreaterOrEqual(t, d, 500*time.Millisecond)
		require.LessOrEqual(t, d, time.Second)
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	t.Parallel()

	for _, p := range []RetryPolicy{
		{MaxAttempts: 0},
		{MaxAttempts: 1, InitialInterval: -1, BackoffCoefficient: 1},
		{MaxAttempts: 1, MaxInterval: -1, BackoffCoefficient: 1},
		{MaxAttempts: 1, BackoffCoefficient: 0.5},
	} {
		_, err := New("http://example.invalid", WithRetryPolicy(p))
		require.Error(t, err, fmt.Sprintf("%+v", p))
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	require.Equal(t, time.Duration(0), parseRetryAfter(""))
	require.Equal(t, 5*time.Second, parseRetryAfter("5"))
	require.Equal(t, time.Duration(0), parseRetryAfter("-5"))
	require.Equal(t, time.Duration(0), parseRetryAfter("nonsense"))

	// The HTTP-date form, which is what some proxies emit.
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	require.InDelta(t, 30*time.Second, parseRetryAfter(future), float64(2*time.Second))
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	require.Equal(t, time.Duration(0), parseRetryAfter(past))
}

func TestCodeForStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, api.CodeInvalidArgument, codeForStatus(http.StatusBadRequest))
	require.Equal(t, api.CodeUnauthorized, codeForStatus(http.StatusForbidden))
	require.Equal(t, api.CodeNotFound, codeForStatus(http.StatusNotFound))
	require.Equal(t, api.CodeResourceExhausted, codeForStatus(http.StatusTooManyRequests))
	require.Equal(t, api.CodeUnavailable, codeForStatus(http.StatusServiceUnavailable))
	require.Equal(t, api.CodeDeadlineExceeded, codeForStatus(http.StatusGatewayTimeout))
	require.Equal(t, api.CodeInternal, codeForStatus(http.StatusNotImplemented))
}

func TestEndpointURLJoining(t *testing.T) {
	t.Parallel()

	for _, base := range []string{"http://host:7233", "http://host:7233/", "http://host:7233/skald"} {
		c, err := New(base)
		require.NoError(t, err)
		require.NotContains(t, c.endpointURL(api.PathStartWorkflow), "//api")
		require.Contains(t, c.endpointURL(api.PathStartWorkflow), api.PathStartWorkflow)
	}
}

func TestReadyProbe(t *testing.T) {
	t.Parallel()

	t.Run("healthy", func(t *testing.T) {
		c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, api.PathReady, r.URL.Path)
			w.WriteHeader(http.StatusOK)
		}))
		require.NoError(t, c.Ready(context.Background()))
	})

	t.Run("draining", func(t *testing.T) {
		c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSONResponse(t, w, http.StatusServiceUnavailable, api.Error{
				Code: api.CodeUnavailable, Message: "server is shutting down",
			})
		}))
		err := c.Ready(context.Background())
		var apiErr *api.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, api.CodeUnavailable, apiErr.Code)
	})
}

func TestClientImplementsTheFullService(t *testing.T) {
	t.Parallel()

	// One interface, three implementations: the engine, the HTTP handler and
	// this. The compile-time assertion in client.go is the real check; this
	// exercises every method once so that a handler-side path change that
	// breaks one of them fails here rather than in a worker.
	var calls atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSONResponse(t, w, http.StatusOK, map[string]any{})
	}))

	ctx := context.Background()
	var svc api.Service = c
	_, err := svc.StartWorkflow(ctx, api.StartWorkflowRequest{})
	require.NoError(t, err)
	require.NoError(t, svc.SignalWorkflow(ctx, api.SignalWorkflowRequest{}))
	_, err = svc.SignalWithStartWorkflow(ctx, api.SignalWithStartRequest{})
	require.NoError(t, err)
	require.NoError(t, svc.CancelWorkflow(ctx, api.CancelWorkflowRequest{}))
	require.NoError(t, svc.TerminateWorkflow(ctx, api.TerminateWorkflowRequest{}))
	_, err = svc.DescribeWorkflow(ctx, "", "a", "")
	require.NoError(t, err)
	_, err = svc.GetHistory(ctx, api.GetHistoryRequest{})
	require.NoError(t, err)
	_, err = svc.ListWorkflows(ctx, api.ListWorkflowsRequest{})
	require.NoError(t, err)
	_, err = svc.PollWorkflowTask(ctx, api.PollWorkflowTaskRequest{})
	require.NoError(t, err)
	require.NoError(t, svc.RespondWorkflowTaskCompleted(ctx, api.RespondWorkflowTaskCompletedRequest{}))
	require.NoError(t, svc.RespondWorkflowTaskFailed(ctx, api.RespondWorkflowTaskFailedRequest{}))
	_, err = svc.PollActivityTask(ctx, api.PollActivityTaskRequest{})
	require.NoError(t, err)
	require.NoError(t, svc.RespondActivityTaskCompleted(ctx, api.RespondActivityTaskCompletedRequest{}))
	require.NoError(t, svc.RespondActivityTaskFailed(ctx, api.RespondActivityTaskFailedRequest{}))
	require.NoError(t, svc.RespondActivityTaskCanceled(ctx, api.RespondActivityTaskCanceledRequest{}))
	_, err = svc.RecordActivityHeartbeat(ctx, api.RecordActivityHeartbeatRequest{})
	require.NoError(t, err)

	require.Equal(t, int32(16), calls.Load())
}

func TestDecodeErrorPrefersTheBody(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(api.Error{Code: api.CodeVersionConflict, Message: "raced", Details: map[string]string{"v": "7"}})
	require.NoError(t, err)

	got := decodeError(raw, http.StatusConflict)
	require.Equal(t, api.CodeVersionConflict, got.Code, "the body distinguishes codes that share a status")
	require.Equal(t, "7", got.Details["v"])

	// An empty body still has to produce something a caller can branch on.
	got = decodeError(nil, http.StatusServiceUnavailable)
	require.Equal(t, api.CodeUnavailable, got.Code)
	require.Contains(t, got.Message, "Service Unavailable")
}

func TestErrorsAreUnwrappable(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusNotFound, api.Error{Code: api.CodeNotFound, Message: "no such workflow"})
	}), fastRetries(1)...)

	_, err := c.DescribeWorkflow(context.Background(), "", "missing", "")
	require.Error(t, err)

	var apiErr *api.Error
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, api.CodeNotFound, apiErr.Code)
	require.Equal(t, "not_found: no such workflow", err.Error())
}
