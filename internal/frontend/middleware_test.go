package frontend

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/skald"
)

func TestPanicIsRecoveredAndNeverLeaksAStack(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			panic("the engine exploded at internal/engine/workflow.go:1234")
		},
	})

	resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	apiErr := decodeAPIError(t, resp)
	require.Equal(t, api.CodeInternal, apiErr.Code)
	require.Equal(t, "internal error", apiErr.Message)

	// The client learns nothing about the internals; the server learns
	// everything, because the stack is the only evidence a panic happened.
	logs := h.logs.String()
	require.Contains(t, logs, "panic serving request")
	require.Contains(t, logs, "internal/engine/workflow.go:1234")
	require.Contains(t, logs, "runtime/debug.Stack")
}

func TestPanicStillProducesAnAccessLogLine(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			panic("boom")
		},
	})
	h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})

	// The status in the access log must be the 500 the client got, not the 200
	// the response writer defaults to.
	logs := h.logs.String()
	require.Contains(t, logs, `"msg":"request"`)
	require.Contains(t, logs, `"status":500`)
}

func TestRequestIDIsEchoedAndSanitised(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})

	t.Run("generated when absent", func(t *testing.T) {
		resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		require.NotEmpty(t, resp.Header.Get(HeaderRequestID))
	})

	t.Run("propagated when supplied", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		req.Header.Set(HeaderRequestID, "trace-me-42")
		resp := h.do(t, req)
		require.Equal(t, "trace-me-42", resp.Header.Get(HeaderRequestID))
		require.Contains(t, h.logs.String(), "trace-me-42")
	})
}

func TestSanitizeRequestID(t *testing.T) {
	t.Parallel()

	// A client-supplied header goes straight into structured logs, so control
	// characters (log injection) and unbounded length (log storage) are handled
	// at the boundary rather than trusted.
	require.Equal(t, "abc", sanitizeRequestID("a\x00b\x1bc"))
	require.Len(t, sanitizeRequestID(strings.Repeat("x", 500)), maxRequestIDLen)
	require.Empty(t, sanitizeRequestID(""))
}

func TestProtocolVersionNegotiation(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})

	t.Run("absent is accepted", func(t *testing.T) {
		resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("unknown is refused loudly", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		req.Header.Set(HeaderProtocolVersion, "99")
		resp := h.do(t, req)

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		apiErr := decodeAPIError(t, resp)
		require.Equal(t, api.CodeInvalidArgument, apiErr.Code)
		require.Equal(t, "99", apiErr.Details["requested"])
		require.Equal(t, api.Version, apiErr.Details["supported"])
	})
}

func TestBearerAuth(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{}, func(c *Config) { c.AuthToken = "s3cret" })

	t.Run("missing token", func(t *testing.T) {
		resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Contains(t, resp.Header.Get("WWW-Authenticate"), "Bearer")
		require.Equal(t, api.CodeUnauthorized, decodeAPIError(t, resp).Code)
	})

	t.Run("wrong token", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		req.Header.Set("Authorization", "Bearer nope")
		require.Equal(t, http.StatusUnauthorized, h.do(t, req).StatusCode)
	})

	t.Run("correct token", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		req.Header.Set("Authorization", "Bearer s3cret")
		require.Equal(t, http.StatusOK, h.do(t, req).StatusCode)
	})

	t.Run("health and ready stay open", func(t *testing.T) {
		// A probe cannot be expected to hold a credential, and a liveness check
		// that fails because a token rotated would restart a healthy process.
		for _, path := range []string{api.PathHealth, api.PathReady} {
			resp := h.do(t, h.request(t, http.MethodGet, path, nil))
			require.Equal(t, http.StatusOK, resp.StatusCode, path)
		}
	})

	t.Run("metrics are protected", func(t *testing.T) {
		resp := h.do(t, h.request(t, http.MethodGet, api.PathMetrics, nil))
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

func TestGzipCompressesLargeResponsesOnly(t *testing.T) {
	t.Parallel()

	big := api.ListWorkflowsResponse{}
	for i := 0; i < 200; i++ {
		big.Executions = append(big.Executions, api.DescribeWorkflowResponse{
			WorkflowID:   "order-with-a-fairly-long-identifier-" + strings.Repeat("0", 20),
			WorkflowType: "OrderWorkflow",
			TaskQueue:    "orders",
			Status:       skald.StatusRunning,
		})
	}
	h := newHarness(t, &stubService{
		list: func(context.Context, api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
			return big, nil
		},
		start: func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			return api.StartWorkflowResponse{RunID: "r"}, nil
		},
	})

	// The transport must not decompress for us, or the assertion below is
	// vacuous: Go's http.Transport sets Accept-Encoding and unwraps silently
	// unless the header is set by hand.
	t.Run("large response is compressed", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathListWorkflows, api.ListWorkflowsRequest{})
		req.Header.Set("Accept-Encoding", "gzip")
		resp := h.do(t, req)

		require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
		require.Contains(t, resp.Header.Values("Vary"), "Accept-Encoding")

		zr, err := gzip.NewReader(resp.Body)
		require.NoError(t, err)
		raw, err := io.ReadAll(zr)
		require.NoError(t, err)

		var out api.ListWorkflowsResponse
		require.NoError(t, decodeJSONBytes(raw, &out))
		require.Len(t, out.Executions, 200)
	})

	t.Run("small response is left alone", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
		req.Header.Set("Accept-Encoding", "gzip")
		resp := h.do(t, req)

		require.Empty(t, resp.Header.Get("Content-Encoding"))
		require.Contains(t, readAllString(t, resp), `"run_id":"r"`)
	})

	t.Run("a client that refuses gzip gets plain bytes", func(t *testing.T) {
		req := h.request(t, http.MethodPost, api.PathListWorkflows, api.ListWorkflowsRequest{})
		req.Header.Set("Accept-Encoding", "gzip;q=0")
		resp := h.do(t, req)
		require.Empty(t, resp.Header.Get("Content-Encoding"))
	})
}

func TestGzipDoesNotDoubleEncode(t *testing.T) {
	t.Parallel()

	// /metrics is the real case: promhttp negotiates compression itself. A
	// doubly-gzipped response decodes exactly once in every client and then
	// fails to parse, which is a miserable bug to chase from the client side.
	h := newHarness(t, &stubService{})
	h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})

	req := h.request(t, http.MethodGet, api.PathMetrics, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := h.do(t, req)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	zr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	raw, err := io.ReadAll(zr)
	require.NoError(t, err)
	// One layer of gzip: the body is the exposition format, not more gzip.
	require.Contains(t, string(raw), "skald_requests_total")
}

func TestAcceptsGzip(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"":                         false,
		"identity":                 false,
		"gzip":                     true,
		"deflate, gzip":            true,
		"gzip;q=0":                 false,
		"gzip;q=0.0":               false,
		"gzip;q=0.1":               true,
		"br;q=1.0, gzip;q=0.8":     true,
		"GZIP":                     true,
		"identity;q=1, gzip;q=0.5": true,
	}
	for header, want := range cases {
		req, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)
		req.Header.Set("Accept-Encoding", header)
		require.Equal(t, want, acceptsGzip(req), "Accept-Encoding: %q", header)
	}
}

func TestRequestTimeoutCancelsTheHandlerContext(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{
		list: func(ctx context.Context, _ api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
			<-ctx.Done()
			return api.ListWorkflowsResponse{}, ctx.Err()
		},
	}, func(c *Config) { c.RequestTimeout = 50 * time.Millisecond })

	resp := h.post(t, api.PathListWorkflows, api.ListWorkflowsRequest{})
	require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
	require.Equal(t, api.CodeDeadlineExceeded, decodeAPIError(t, resp).Code)
}
