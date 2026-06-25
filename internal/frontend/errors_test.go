package frontend

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/api"
)

// TestErrorMappingOverTheWire drives every api error code through a real
// request so that the status, the body and the headers are all asserted
// together. A unit test of statusForCode alone would not catch a Retry-After
// that never reaches the response.
func TestErrorMappingOverTheWire(t *testing.T) {
	t.Parallel()

	cases := []struct {
		code            string
		wantStatus      int
		wantRetryAfter  string
		wantWWWAuth     bool
		serviceRetryHnt time.Duration
	}{
		{code: api.CodeInvalidArgument, wantStatus: http.StatusBadRequest},
		{code: api.CodeUnauthorized, wantStatus: http.StatusUnauthorized, wantWWWAuth: true},
		{code: api.CodeNotFound, wantStatus: http.StatusNotFound},
		{code: api.CodeAlreadyExists, wantStatus: http.StatusConflict},
		{code: api.CodeVersionConflict, wantStatus: http.StatusConflict},
		{code: api.CodeFailedPrecondition, wantStatus: http.StatusPreconditionFailed},
		{code: api.CodeResourceExhausted, wantStatus: http.StatusTooManyRequests, wantRetryAfter: "1"},
		{code: api.CodeUnavailable, wantStatus: http.StatusServiceUnavailable, wantRetryAfter: "1"},
		{code: api.CodeDeadlineExceeded, wantStatus: http.StatusGatewayTimeout},
		{code: api.CodeInternal, wantStatus: http.StatusInternalServerError},
		{
			// An unrecognised code means the engine invented one this build
			// does not know: a server problem, so a 500 rather than a 400.
			code: "some_future_code", wantStatus: http.StatusInternalServerError,
		},
		{
			// A server-supplied hint wins over the default.
			code: api.CodeUnavailable, wantStatus: http.StatusServiceUnavailable,
			serviceRetryHnt: 12 * time.Second, wantRetryAfter: "12",
		},
		{
			// Sub-second hints round up: Retry-After has one-second granularity
			// and a value of 0 means "retry immediately", which is the opposite
			// of what a 429 is asking for.
			code: api.CodeResourceExhausted, wantStatus: http.StatusTooManyRequests,
			serviceRetryHnt: 250 * time.Millisecond, wantRetryAfter: "1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.code+"/"+tc.wantRetryAfter, func(t *testing.T) {
			h := newHarness(t, &stubService{
				start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
					return api.StartWorkflowResponse{}, &api.Error{
						Code:       tc.code,
						Message:    "boom",
						Details:    map[string]string{"key": "value"},
						RetryAfter: tc.serviceRetryHnt,
					}
				},
			})

			resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			require.Equal(t, tc.wantRetryAfter, resp.Header.Get("Retry-After"))
			require.Equal(t, tc.wantWWWAuth, resp.Header.Get("WWW-Authenticate") != "")

			apiErr := decodeAPIError(t, resp)
			require.Equal(t, tc.code, apiErr.Code)
			require.Equal(t, "boom", apiErr.Message)
			require.Equal(t, "value", apiErr.Details["key"])
		})
	}
}

func TestRetryAfterIsOmittedWhereRetryingCannotHelp(t *testing.T) {
	t.Parallel()

	// A 404 with a Retry-After is an invitation to hammer a request that cannot
	// start working, so the hint is dropped even when the error carries one.
	h := newHarness(t, &stubService{
		start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			return api.StartWorkflowResponse{}, &api.Error{
				Code: api.CodeNotFound, Message: "gone", RetryAfter: time.Minute,
			}
		},
	})
	resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Retry-After"))
}

func TestAsAPIError(t *testing.T) {
	t.Parallel()

	t.Run("passes an api error through", func(t *testing.T) {
		want := &api.Error{Code: api.CodeNotFound, Message: "nope"}
		require.Same(t, want, asAPIError(want))
	})

	t.Run("unwraps a wrapped api error", func(t *testing.T) {
		inner := &api.Error{Code: api.CodeVersionConflict, Message: "raced"}
		got := asAPIError(errors.Join(errors.New("context"), inner))
		require.Equal(t, api.CodeVersionConflict, got.Code)
	})

	t.Run("translates context errors", func(t *testing.T) {
		require.Equal(t, api.CodeDeadlineExceeded, asAPIError(context.DeadlineExceeded).Code)
		require.Equal(t, api.CodeUnavailable, asAPIError(context.Canceled).Code)
	})

	t.Run("classifies an uncoded error as internal", func(t *testing.T) {
		got := asAPIError(errors.New("something went sideways"))
		require.Equal(t, api.CodeInternal, got.Code)
		require.Equal(t, "something went sideways", got.Message)
	})
}

func TestRetryAfterHeaderRendering(t *testing.T) {
	t.Parallel()

	require.Equal(t, "1", retryAfterHeader(time.Nanosecond))
	require.Equal(t, "1", retryAfterHeader(time.Second))
	require.Equal(t, "2", retryAfterHeader(1500*time.Millisecond))
	require.Equal(t, "90", retryAfterHeader(90*time.Second))
}
