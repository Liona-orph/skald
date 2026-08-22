package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// historyServer serves GetHistory from a fixed set of runs, honouring the
// FromEventID cursor the way the engine does.
type historyServer struct {
	runs map[string]history.History
	// status is the status reported for a run once its history is exhausted.
	status map[string]skald.WorkflowStatus
}

func (s *historyServer) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, api.PathGetHistory, r.URL.Path)

		var req api.GetHistoryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		events := s.runs[req.RunID]
		from := req.FromEventID
		if from < 1 {
			from = 1
		}
		var out history.History
		if int(from) <= len(events) {
			out = events[from-1:]
		}

		status := s.status[req.RunID]
		next := from
		if len(out) > 0 {
			next = out[len(out)-1].ID + 1
		}
		writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{
			Events: out, Status: status, NextEventID: next,
		})
	})
}

func event(id int64, attrs history.Attributes) history.Event {
	return history.Event{ID: id, Time: time.Unix(1700000000+id, 0).UTC(), Attrs: attrs}
}

func startedEvent() history.Event {
	return event(1, history.WorkflowExecutionStartedAttributes{
		WorkflowType: "OrderWorkflow", TaskQueue: "orders",
	})
}

func TestResultDecodesACompletedWorkflow(t *testing.T) {
	t.Parallel()

	type result struct {
		Charged int    `json:"charged"`
		Ref     string `json:"ref"`
	}
	payload := skald.MustPayload(result{Charged: 4200, Ref: "ch_1"})

	srv := &historyServer{
		runs: map[string]history.History{"": {
			startedEvent(),
			event(2, history.WorkflowExecutionCompletedAttributes{Result: payload, WorkflowTaskCompletedEventID: 1}),
		}},
		status: map[string]skald.WorkflowStatus{"": skald.StatusCompleted},
	}
	c := newTestClient(t, srv.handler(t))

	var got result
	require.NoError(t, c.NewHandle("", "order-1", "").Result(context.Background(), &got))
	require.Equal(t, result{Charged: 4200, Ref: "ch_1"}, got)
}

func TestResultReturnsTypedTerminalErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		attrs  history.Attributes
		status skald.WorkflowStatus
		assert func(*testing.T, error)
	}{
		{
			name: "failed",
			attrs: history.WorkflowExecutionFailedAttributes{
				Failure: &skald.ApplicationError{
					Type: "InsufficientFunds", Message: "balance too low", NonRetryable: true,
				},
				WorkflowTaskCompletedEventID: 1,
			},
			status: skald.StatusFailed,
			assert: func(t *testing.T, err error) {
				var app *skald.ApplicationError
				require.ErrorAs(t, err, &app)
				// The classification survives the round trip, which is what
				// lets a caller branch on the failure instead of matching text.
				require.Equal(t, "InsufficientFunds", app.Type)
				require.True(t, app.NonRetryable)
			},
		},
		{
			name:   "canceled",
			attrs:  history.WorkflowExecutionCanceledAttributes{WorkflowTaskCompletedEventID: 1},
			status: skald.StatusCanceled,
			assert: func(t *testing.T, err error) {
				var canceled *skald.CanceledError
				require.ErrorAs(t, err, &canceled)
			},
		},
		{
			name:   "terminated",
			attrs:  history.WorkflowExecutionTerminatedAttributes{Reason: "stuck, INC-4471"},
			status: skald.StatusTerminated,
			assert: func(t *testing.T, err error) {
				var terminated *skald.TerminatedError
				require.ErrorAs(t, err, &terminated)
				require.Equal(t, "stuck, INC-4471", terminated.Reason)
			},
		},
		{
			name:   "timed out",
			attrs:  history.WorkflowExecutionTimedOutAttributes{Kind: skald.TimeoutStartToClose},
			status: skald.StatusTimedOut,
			assert: func(t *testing.T, err error) {
				var timeout *skald.TimeoutError
				require.ErrorAs(t, err, &timeout)
				require.Equal(t, skald.TimeoutStartToClose, timeout.Kind)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &historyServer{
				runs:   map[string]history.History{"": {startedEvent(), event(2, tc.attrs)}},
				status: map[string]skald.WorkflowStatus{"": tc.status},
			}
			c := newTestClient(t, srv.handler(t))
			err := c.NewHandle("", "order-1", "").Result(context.Background(), nil)
			require.Error(t, err)
			tc.assert(t, err)
		})
	}
}

func TestResultFollowsContinueAsNew(t *testing.T) {
	t.Parallel()

	// Continue-as-new is how a long-running workflow keeps its history bounded.
	// It is not an outcome, so "the result of this workflow" means the result
	// of the chain.
	srv := &historyServer{
		runs: map[string]history.History{
			"": {
				startedEvent(),
				event(2, history.WorkflowExecutionContinuedAsNewAttributes{
					NewRunID: "run-2", WorkflowType: "OrderWorkflow", WorkflowTaskCompletedEventID: 1,
				}),
			},
			"run-2": {
				startedEvent(),
				event(2, history.WorkflowExecutionCompletedAttributes{
					Result: skald.MustPayload("done"), WorkflowTaskCompletedEventID: 1,
				}),
			},
		},
		status: map[string]skald.WorkflowStatus{
			"":      skald.StatusContinuedAsNew,
			"run-2": skald.StatusCompleted,
		},
	}
	c := newTestClient(t, srv.handler(t))

	var got string
	require.NoError(t, c.NewHandle("", "order-1", "").Result(context.Background(), &got))
	require.Equal(t, "done", got)
}

func TestResultKeepsPollingUntilTheTerminalEventArrives(t *testing.T) {
	t.Parallel()

	// The server answers the first two polls with nothing, as a long poll that
	// reached its cap does. The client must resume from its cursor rather than
	// treating the empty answer as an end.
	var polls int
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.GetHistoryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.True(t, req.WaitForNew)

		polls++
		if polls < 3 {
			writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{
				Status: skald.StatusRunning, NextEventID: req.FromEventID,
			})
			return
		}
		writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{
			Events: history.History{
				startedEvent(),
				event(2, history.WorkflowExecutionCompletedAttributes{
					Result: skald.MustPayload(7), WorkflowTaskCompletedEventID: 1,
				}),
			},
			Status:      skald.StatusCompleted,
			NextEventID: 3,
		})
	}))

	var got int
	require.NoError(t, c.NewHandle("", "order-1", "").Result(context.Background(), &got))
	require.Equal(t, 7, got)
	require.Equal(t, 3, polls)
}

func TestResultRefusesToWaitOnAClosedRunWithNoTerminalEvent(t *testing.T) {
	t.Parallel()

	// A corrupt history, not a state to wait in. Blocking forever would look
	// exactly like a workflow that is simply slow.
	srv := &historyServer{
		runs:   map[string]history.History{"": {startedEvent()}},
		status: map[string]skald.WorkflowStatus{"": skald.StatusFailed},
	}
	c := newTestClient(t, srv.handler(t))

	err := c.NewHandle("", "order-1", "").Result(context.Background(), nil)
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeInternal, apiErr.Code)
	require.Contains(t, apiErr.Message, "no terminal event")
}

func TestResultPropagatesTransportErrors(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusNotFound, api.Error{Code: api.CodeNotFound, Message: "no such workflow"})
	}), fastRetries(1)...)

	err := c.NewHandle("", "missing", "").Result(context.Background(), nil)
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeNotFound, apiErr.Code)
}

func TestHistoryPagesToTheEnd(t *testing.T) {
	t.Parallel()

	full := history.History{
		startedEvent(),
		event(2, history.WorkflowTaskScheduledAttributes{TaskQueue: "orders", Attempt: 1}),
		event(3, history.WorkflowTaskStartedAttributes{ScheduledEventID: 2}),
	}
	// One event per response, so a client that ignores the cursor loops or
	// stops early.
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.GetHistoryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		from := req.FromEventID
		if from < 1 {
			from = 1
		}
		if int(from) > len(full) {
			writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{NextEventID: from})
			return
		}
		writeJSONResponse(t, w, http.StatusOK, api.GetHistoryResponse{
			Events: full[from-1 : from], NextEventID: from + 1,
		})
	}))

	got, err := c.NewHandle("", "order-1", "").History(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, history.EventTypeWorkflowTaskStarted, got[2].Type())
}

func TestHandleMutations(t *testing.T) {
	t.Parallel()

	type captured struct {
		path string
		body map[string]any
	}
	var calls []captured
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		calls = append(calls, captured{path: r.URL.Path, body: body})
		writeJSONResponse(t, w, http.StatusOK, map[string]any{})
	}), WithNamespace("prod"))

	h := c.NewHandle("", "order-1", "run-9")
	require.NoError(t, h.Signal(context.Background(), "approve", map[string]string{"by": "alice"}))
	require.NoError(t, h.Cancel(context.Background(), "changed mind"))
	require.NoError(t, h.Terminate(context.Background(), "wedged", nil))

	require.Len(t, calls, 3)
	require.Equal(t, api.PathSignalWorkflow, calls[0].path)
	require.Equal(t, "approve", calls[0].body["signal_name"])
	require.Equal(t, "prod", calls[0].body["namespace"])
	require.Equal(t, "run-9", calls[0].body["run_id"])
	require.Equal(t, api.PathCancelWorkflow, calls[1].path)
	require.Equal(t, "changed mind", calls[1].body["reason"])
	require.Equal(t, api.PathTerminateWorkflow, calls[2].path)
	require.Equal(t, "wedged", calls[2].body["reason"])
}

func TestSignalRequiresAName(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("the request must not leave the client")
	}))
	err := c.NewHandle("", "order-1", "").Signal(context.Background(), "", nil)
	var apiErr *api.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, api.CodeInvalidArgument, apiErr.Code)
}

func TestExecuteWorkflowValidatesAndReturnsAHandle(t *testing.T) {
	t.Parallel()

	var got api.StartWorkflowRequest
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1", Started: true})
	}), WithNamespace("prod"))

	_, err := c.ExecuteWorkflow(context.Background(), WorkflowOptions{TaskQueue: "orders"}, nil)
	require.ErrorContains(t, err, "workflow type is required")

	_, err = c.ExecuteWorkflow(context.Background(), WorkflowOptions{Type: "OrderWorkflow"}, nil)
	require.ErrorContains(t, err, "task queue is required")

	h, err := c.ExecuteWorkflow(context.Background(), WorkflowOptions{
		ID: "order-1", Type: "OrderWorkflow", TaskQueue: "orders",
	}, map[string]int{"total": 4200})
	require.NoError(t, err)
	require.Equal(t, "order-1", h.WorkflowID())
	require.Equal(t, "run-1", h.RunID())
	require.Equal(t, "prod", h.Namespace())

	require.Equal(t, skald.EncodingJSON, got.Input.Encoding)
	require.JSONEq(t, `{"total":4200}`, string(got.Input.Data))
}

func TestExecuteWorkflowGeneratesAnIDWhenOmitted(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}), WithRequestIDFunc(func() string { return "generated-id" }))

	h, err := c.ExecuteWorkflow(context.Background(), WorkflowOptions{Type: "T", TaskQueue: "q"}, nil)
	require.NoError(t, err)
	require.Equal(t, "generated-id", h.WorkflowID())
}

func TestSignalWithStart(t *testing.T) {
	t.Parallel()

	var got api.SignalWithStartRequest
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, api.PathSignalWithStart, r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		writeJSONResponse(t, w, http.StatusOK, api.StartWorkflowResponse{RunID: "run-1"})
	}))

	h, err := c.SignalWithStart(context.Background(),
		WorkflowOptions{ID: "order-1", Type: "OrderWorkflow", TaskQueue: "orders"},
		"input", "approve", map[string]string{"by": "alice"})
	require.NoError(t, err)
	require.Equal(t, "run-1", h.RunID())
	require.Equal(t, "approve", got.SignalName)
	require.Equal(t, "order-1", got.Start.WorkflowID)
	require.NotEmpty(t, got.Start.RequestID)

	_, err = c.SignalWithStart(context.Background(),
		WorkflowOptions{Type: "T", TaskQueue: "q"}, nil, "", nil)
	require.ErrorContains(t, err, "signal name is required")
}

func TestDescribeThroughHandle(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusOK, api.DescribeWorkflowResponse{
			WorkflowID: "order-1", RunID: "run-9", Status: skald.StatusRunning,
		})
	}))
	got, err := c.NewHandle("", "order-1", "run-9").Describe(context.Background())
	require.NoError(t, err)
	require.Equal(t, skald.StatusRunning, got.Status)
}

func TestResultStopsWhenTheCallerCancels(t *testing.T) {
	t.Parallel()

	blocked := make(chan struct{})
	stop := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// The body must be drained before blocking. net/http only starts the
		// background read that notices a closed connection once the request
		// body has hit EOF, so a handler that parks without reading never sees
		// the disconnect -- and neither would httptest.Server.Close.
		_, _ = io.Copy(io.Discard, r.Body)
		once.Do(func() { close(blocked) })
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	// Cleanups run last-in-first-out, so the handler is released before Close
	// waits for it.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stop) })

	c, err := New(srv.URL, WithPollTimeout(10*time.Second), fastRetries(1)[0], fastRetries(1)[1])
	require.NoError(t, err)
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.NewHandle("", "order-1", "").Result(ctx, nil) }()

	<-blocked
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Result ignored the caller's cancellation")
	}
}
