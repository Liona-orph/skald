// Package telemetry wires Skald's three observability signals -- structured
// logs, Prometheus metrics and OpenTelemetry traces -- into one value that the
// process entry point builds once and hands to everything else.
//
// # Why one package and one object
//
// Each signal answers a different question and none of them answers all three:
//
//   - Metrics answer "is anything wrong?". They are cheap, aggregate and
//     bounded, and they are the only signal you can afford to keep forever.
//   - Traces answer "where did this one request spend its time?". They are
//     per-request, high cardinality and sampled.
//   - Logs answer "what exactly happened to this workflow?". They are
//     per-event, searchable by identifier and expensive at volume.
//
// Because the three are answered together during an incident they have to be
// correlatable, which means a single place has to decide that the trace ID
// appears in the log line and that the operation name is spelled the same way
// in the metric label and the span name. That place is this package.
//
// # What is deliberately absent
//
// Nothing here touches OpenTelemetry's process-global setters
// (otel.SetTracerProvider, otel.SetTextMapPropagator) or Prometheus's default
// registry. Every dependency is a field on a value the caller owns. Two servers
// in one test binary therefore do not fight over shared state, and a component
// that was not given telemetry emits none rather than silently writing to a
// stream nobody reads.
package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config parameterises New.
type Config struct {
	// ServiceName and ServiceVersion identify this process to a trace backend.
	ServiceName    string
	ServiceVersion string

	// Logger, when set, is used verbatim and LogOutput/LogFormat/LogLevel are
	// ignored. It exists so that a test can capture output and so that an
	// embedder that already has an slog.Logger does not get a second one.
	Logger *slog.Logger
	// LogOutput defaults to os.Stderr. Logs go to stderr, not stdout, because
	// stdout is where a CLI writes results a human or a pipe consumes, and
	// mixing the two makes `skaldctl ... | jq` fail in a confusing way.
	LogOutput io.Writer
	LogFormat LogFormat
	LogLevel  slog.Leveler

	// Registry receives the metric set. A fresh registry is created when nil;
	// the default global registry is never used, because a library that
	// registers into a global cannot be instantiated twice.
	Registry *prometheus.Registry
	// CollectRuntimeMetrics adds the Go runtime and process collectors. It is
	// opt-in so that a test registry contains only Skald's own series.
	CollectRuntimeMetrics bool

	// SpanExporter, when nil, selects a no-op tracer provider. See
	// newTracerProvider for why that is not the same as an SDK provider with a
	// discarding exporter.
	SpanExporter sdktrace.SpanExporter
	// Sampler overrides the default parent-based sampler.
	Sampler sdktrace.Sampler
}

// Telemetry is the wired observability stack.
type Telemetry struct {
	// Logger is the process logger. Request handlers should prefer the
	// request-scoped logger from the context, which carries correlation fields.
	Logger *slog.Logger
	// Metrics is the Prometheus instrumentation. It may be nil-safe-called.
	Metrics *Metrics
	// Registry is what /metrics serves.
	Registry *prometheus.Registry

	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	shutdown   func(context.Context) error
}

// New builds the telemetry stack.
func New(cfg Config) (*Telemetry, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "skald"
	}
	logger := cfg.Logger
	if logger == nil {
		out := cfg.LogOutput
		if out == nil {
			out = os.Stderr
		}
		level := cfg.LogLevel
		if level == nil {
			level = slog.LevelInfo
		}
		format := cfg.LogFormat
		if format == "" {
			format = LogFormatJSON
		}
		logger = NewLogger(out, format, level)
	}

	reg := cfg.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if cfg.CollectRuntimeMetrics {
		for _, c := range []prometheus.Collector{
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		} {
			if err := reg.Register(c); err != nil {
				return nil, fmt.Errorf("telemetry: register runtime collectors: %w", err)
			}
		}
	}
	metrics, err := NewMetrics(reg)
	if err != nil {
		return nil, err
	}

	tp, shutdown, err := newTracerProvider(cfg)
	if err != nil {
		return nil, err
	}

	return &Telemetry{
		Logger:     logger,
		Metrics:    metrics,
		Registry:   reg,
		tracer:     tp.Tracer(instrumentationName),
		propagator: newPropagator(),
		shutdown:   shutdown,
	}, nil
}

// Tracer returns the tracer spans are started from.
func (t *Telemetry) Tracer() trace.Tracer {
	if t == nil {
		return noopTracer()
	}
	return t.tracer
}

// Propagator returns the context propagator for the HTTP boundary.
func (t *Telemetry) Propagator() propagation.TextMapPropagator {
	if t == nil {
		return propagation.NewCompositeTextMapPropagator()
	}
	return t.propagator
}

// Log returns the process logger, never nil.
func (t *Telemetry) Log() *slog.Logger {
	if t == nil || t.Logger == nil {
		return NopLogger()
	}
	return t.Logger
}

// Shutdown flushes buffered spans. It is safe to call on a nil Telemetry and
// safe to call twice.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.shutdown == nil {
		return nil
	}
	fn := t.shutdown
	t.shutdown = nil
	return fn(ctx)
}

// MetricsHandler serves the Prometheus exposition format.
func (t *Telemetry) MetricsHandler() http.Handler {
	if t == nil || t.Registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{
		// A collector that fails must not take the whole scrape down: a partial
		// scrape still tells the operator most of what they need, whereas a 500
		// tells them nothing and looks identical to the process being dead.
		ErrorHandling: promhttp.ContinueOnError,
		ErrorLog:      promLogger{log: t.Log()},
	})
}

// promLogger adapts slog to promhttp's tiny logger interface.
type promLogger struct{ log *slog.Logger }

func (l promLogger) Println(v ...any) { l.log.Error("metrics scrape", KeyError, fmt.Sprint(v...)) }

// ---------------------------------------------------------------------------
// Per-request observation
// ---------------------------------------------------------------------------

// observation is the mutable, request-scoped state the middleware needs and the
// handler knows. It lives in the request context rather than in a field so that
// it cannot outlive the request that owns it.
type observation struct {
	mu       sync.Mutex
	longPoll bool
	code     string
}

type observationKey struct{}

func (o *observation) markLongPoll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.longPoll = true
}

func (o *observation) setCode(code string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.code = code
}

func (o *observation) read() (longPoll bool, code string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.longPoll, o.code
}

func observationFrom(ctx context.Context) *observation {
	o, _ := ctx.Value(observationKey{}).(*observation)
	return o
}

// MarkLongPoll tells the middleware that this request blocked waiting for work,
// so its latency belongs in the long-poll histogram.
//
// It exists because one endpoint -- GetHistory -- is a 2ms read or a 50s wait
// depending on a single field of the request body. Classifying it statically
// would put both in the same histogram and make the resulting quantiles
// meaningless; classifying it here costs one mutex on a path that is about to
// block for seconds anyway.
func MarkLongPoll(ctx context.Context) {
	if o := observationFrom(ctx); o != nil {
		o.markLongPoll()
	}
}

// SetResultCode records the api error code this request produced, so the
// metrics label is the protocol's own vocabulary rather than an HTTP status
// that several codes share.
func SetResultCode(ctx context.Context, code string) {
	if o := observationFrom(ctx); o != nil {
		o.setCode(code)
	}
}

// ---------------------------------------------------------------------------
// HTTP middleware
// ---------------------------------------------------------------------------

// HTTPMiddleware instruments one route.
//
// The operation is bound at wiring time rather than derived from the request,
// which is what keeps the metric label set closed (see the comment on the Op
// constants). One middleware value per route is the cost; an unbounded label
// space is the alternative.
//
// It wires all three signals in one pass:
//
//   - trace: the incoming traceparent is extracted and a server span started,
//     so a call that crosses from a worker into the engine is one trace.
//   - log: a request-scoped logger carrying the operation, the request ID and
//     the trace ID is put in the context for every handler below.
//   - metric: an in-flight gauge for the duration of the call and a
//     count/latency/error observation when it returns.
func (t *Telemetry) HTTPMiddleware(operation string) func(http.Handler) http.Handler {
	// Resolved once at wiring time so that the per-request path does not
	// re-check for a nil receiver, and so that a nil *Telemetry produces a
	// middleware that still works rather than one that panics on first use.
	var metrics *Metrics
	if t != nil {
		metrics = t.Metrics
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			ctx := t.Propagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			ctx, span := t.Tracer().Start(ctx, operation, trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()

			log := t.Log().With(KeyOperation, operation)
			if id := RequestIDFrom(r.Context()); id != "" {
				log = log.With(KeyRequestID, id)
				span.SetAttributes(AttrRequestID.String(id))
			}
			if sc := span.SpanContext(); sc.HasTraceID() {
				log = log.With(KeyTraceID, sc.TraceID().String())
			}
			ctx = ContextWithLogger(ctx, log)

			obs := &observation{}
			ctx = context.WithValue(ctx, observationKey{}, obs)

			ww := WrapResponseWriter(w)
			done := metrics.RequestStarted(operation)
			// Deferred, not sequential: RequestStarted incremented an in-flight
			// gauge, and a gauge whose decrement can be skipped by a panic
			// climbs forever and eventually reads as a permanently overloaded
			// server that nothing can be done about.
			defer func() {
				longPoll, code := obs.read()
				done(resultCode(code, ww.Status()), longPoll, time.Since(start))
			}()

			next.ServeHTTP(ww, r.WithContext(ctx))
		})
	}
}

// resultCode picks the metric label for a finished request.
//
// A handler that went through the error writer has already recorded the
// protocol code. Anything else is classified from the status, and the fallback
// is deliberately coarse -- there are a dozen statuses, so "http_502" adds a
// dozen series at worst, whereas echoing an arbitrary upstream string would
// add one per distinct message.
func resultCode(recorded string, status int) string {
	if recorded != "" {
		return recorded
	}
	if status < 400 {
		return CodeOK
	}
	return "http_" + strconv.Itoa(status)
}

// ---------------------------------------------------------------------------
// Response writer wrapper
// ---------------------------------------------------------------------------

// ResponseWriter records the status and byte count of a response.
//
// It is exported so that the access-log middleware and the metrics middleware
// share one wrapper: two independent wrappers would work, but they would report
// different byte counts the moment anything in between re-encodes the body, and
// a log line that disagrees with the metric is a bug report waiting to happen.
type ResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

// WrapResponseWriter returns w wrapped, reusing an existing wrapper when there
// already is one so that the wrapping is idempotent across middleware layers.
func WrapResponseWriter(w http.ResponseWriter) *ResponseWriter {
	if rw, ok := w.(*ResponseWriter); ok {
		return rw
	}
	return &ResponseWriter{ResponseWriter: w, status: http.StatusOK}
}

// WriteHeader implements http.ResponseWriter.
func (w *ResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write implements http.ResponseWriter.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

// Status returns the status the handler wrote, defaulting to 200.
func (w *ResponseWriter) Status() int { return w.status }

// BytesWritten returns the size of the body as it left this layer.
func (w *ResponseWriter) BytesWritten() int64 { return w.bytes }

// Unwrap lets http.ResponseController reach the underlying writer, which is how
// a handler gets at Flush or SetWriteDeadline through the wrapper.
func (w *ResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush forwards to the underlying writer when it supports flushing, so that a
// streaming response is not buffered by the wrapper.
func (w *ResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
