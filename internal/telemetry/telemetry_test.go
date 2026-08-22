package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]slog.Level{
		"":      slog.LevelInfo,
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		" warn": slog.LevelWarn,
		"error": slog.LevelError,
	} {
		got, err := ParseLevel(input)
		require.NoError(t, err, input)
		require.Equal(t, want, got, input)
	}

	_, err := ParseLevel("chatty")
	require.ErrorContains(t, err, "unknown log level")
}

func TestParseLogFormat(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]LogFormat{
		"":     LogFormatJSON,
		"json": LogFormatJSON,
		"TEXT": LogFormatText,
	} {
		got, err := ParseLogFormat(input)
		require.NoError(t, err, input)
		require.Equal(t, want, got, input)
	}

	_, err := ParseLogFormat("yaml")
	require.ErrorContains(t, err, "unknown log format")
}

func TestNewLoggerHonoursFormatAndLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := NewLogger(&buf, LogFormatJSON, slog.LevelWarn)
	log.Info("suppressed")
	log.Warn("kept", "k", "v")

	require.NotContains(t, buf.String(), "suppressed")

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	require.Equal(t, "kept", record["msg"])
	require.Equal(t, "v", record["k"])
}

func TestContextLoggerRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// The absent case returns a discarding logger, never nil, so no call site
	// has to nil-check.
	got, ok := LoggerFromContext(ctx)
	require.False(t, ok)
	require.NotNil(t, got)

	var buf bytes.Buffer
	log := NewLogger(&buf, LogFormatJSON, slog.LevelDebug)
	ctx = ContextWithLogger(ctx, log)
	got, ok = LoggerFromContext(ctx)
	require.True(t, ok)
	require.Same(t, log, got)

	ctx = ContextWithRequestID(ctx, "req-1")
	require.Equal(t, "req-1", RequestIDFrom(ctx))
}

func TestWithExecutionOmitsUnknownFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := WithExecution(NewLogger(&buf, LogFormatJSON, slog.LevelDebug), "prod", "order-1", "")
	log.Info("hello")

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	require.Equal(t, "prod", record[KeyNamespace])
	require.Equal(t, "order-1", record[KeyWorkflowID])
	// A run ID is genuinely unknown until the server picks one; an empty string
	// would make every log query need two cases.
	require.NotContains(t, record, KeyRunID)
}

func TestNopLoggerRefusesWorkAtEnabled(t *testing.T) {
	t.Parallel()

	// A discarding logger that still formats the record is not discarding much.
	require.False(t, NopLogger().Enabled(context.Background(), slog.LevelError))
}

func TestNewIsSelfContained(t *testing.T) {
	t.Parallel()

	tel, err := New(Config{ServiceName: "skald-test", Registry: prometheus.NewRegistry(), Logger: NopLogger()})
	require.NoError(t, err)

	require.NotNil(t, tel.Tracer())
	require.NotNil(t, tel.Propagator())
	require.NotNil(t, tel.Metrics)
	// Shutdown with no exporter is a no-op and is safe twice.
	require.NoError(t, tel.Shutdown(context.Background()))
	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestNilTelemetryIsUsable(t *testing.T) {
	t.Parallel()

	// Instrumentation added to a component must never be the reason that
	// component crashes.
	var tel *Telemetry
	require.NotNil(t, tel.Tracer())
	require.NotNil(t, tel.Log())
	require.NoError(t, tel.Shutdown(context.Background()))

	handler := tel.HTTPMiddleware(OpHealth)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusTeapot, rec.Code)
}

func TestHTTPMiddlewareInstallsARequestLogger(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	tel, err := New(Config{
		Registry: prometheus.NewRegistry(),
		Logger:   NewLogger(&buf, LogFormatJSON, slog.LevelDebug),
	})
	require.NoError(t, err)

	handler := tel.HTTPMiddleware(OpStartWorkflow)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		LoggerFrom(r.Context()).Info("inside")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, api.PathStartWorkflow, nil)
	req = req.WithContext(ContextWithRequestID(req.Context(), "req-7"))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	var record map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record))
	require.Equal(t, "inside", record["msg"])
	require.Equal(t, OpStartWorkflow, record[KeyOperation])
	require.Equal(t, "req-7", record[KeyRequestID])
}

func TestHTTPMiddlewareRecordsRequests(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	tel, err := New(Config{Registry: reg, Logger: NopLogger()})
	require.NoError(t, err)

	handler := tel.HTTPMiddleware(OpStartWorkflow)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetResultCode(r.Context(), api.CodeNotFound)
		w.WriteHeader(http.StatusNotFound)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/x", nil))

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_requests_total{code="not_found",operation="StartWorkflow"} 1`)
	require.Contains(t, body, `skald_request_errors_total{code="not_found",operation="StartWorkflow"} 1`)
	// The gauge must come back down: a leaked in-flight count reads as a
	// permanently overloaded server.
	require.Contains(t, body, `skald_requests_in_flight{operation="StartWorkflow"} 0`)
}

func TestMarkLongPollSelectsThePollHistogram(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	tel, err := New(Config{Registry: reg, Logger: NopLogger()})
	require.NoError(t, err)

	// GetHistory is a 2ms read or a 50s wait depending on one request field, so
	// the handler classifies it per call rather than the route classifying it
	// once.
	handler := tel.HTTPMiddleware(OpGetHistory)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		MarkLongPoll(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/x", nil))

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_poll_duration_seconds_count{operation="GetHistory"} 1`)
	require.NotContains(t, body, `skald_request_duration_seconds_count{operation="GetHistory"}`)
}

func TestResultCodeFallback(t *testing.T) {
	t.Parallel()

	require.Equal(t, api.CodeNotFound, resultCode(api.CodeNotFound, 200))
	require.Equal(t, CodeOK, resultCode("", 204))
	// Coarse but bounded: a dozen statuses at worst, versus one series per
	// distinct upstream message.
	require.Equal(t, "http_502", resultCode("", 502))
}

func TestResponseWriterWrappingIsIdempotent(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	first := WrapResponseWriter(rec)
	require.Same(t, first, WrapResponseWriter(first))

	first.WriteHeader(http.StatusTeapot)
	// A second WriteHeader is ignored, matching net/http, so the recorded
	// status stays the one the client actually saw.
	first.WriteHeader(http.StatusOK)
	n, err := first.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	require.Equal(t, http.StatusTeapot, first.Status())
	require.Equal(t, int64(5), first.BytesWritten())
	require.Same(t, rec, first.Unwrap())
}

// gatherText renders a registry in the Prometheus exposition format.
func gatherText(t *testing.T, g prometheus.Gatherer) string {
	t.Helper()
	rec := httptest.NewRecorder()
	promHandler(t, g).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

func promHandler(t *testing.T, g prometheus.Gatherer) http.Handler {
	t.Helper()
	reg, ok := g.(*prometheus.Registry)
	require.True(t, ok)
	tel := &Telemetry{Registry: reg, Logger: NopLogger()}
	return tel.MetricsHandler()
}

func TestMetricsHandlerServesExpositionFormat(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	tel, err := New(Config{Registry: reg, Logger: NopLogger()})
	require.NoError(t, err)

	tel.Metrics.ObserveWorkflowCompletion("prod", "COMPLETED")
	body := gatherText(t, reg)
	require.Contains(t, body, `skald_workflow_completions_total{namespace="prod",status="COMPLETED"} 1`)
	require.True(t, strings.HasPrefix(body, "# HELP"))
}
