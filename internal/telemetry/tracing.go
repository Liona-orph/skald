package telemetry

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Liona-orph/skald/pkg/api"
)

// Span attribute keys.
//
// Unlike metric labels, span attributes *may* carry high-cardinality values:
// a span is stored once, keyed by trace ID, and never aggregated across
// executions, so a workflow ID here costs one string per span rather than one
// permanent time series per workflow. This asymmetry is the whole reason both
// signals exist, and it is why the identity of an execution belongs on the
// trace and not on the counter. See the cardinality note on Metrics.
const (
	AttrNamespace    = attribute.Key("skald.namespace")
	AttrWorkflowID   = attribute.Key("skald.workflow_id")
	AttrRunID        = attribute.Key("skald.run_id")
	AttrWorkflowType = attribute.Key("skald.workflow_type")
	AttrTaskQueue    = attribute.Key("skald.task_queue")
	AttrActivityID   = attribute.Key("skald.activity_id")
	AttrActivityType = attribute.Key("skald.activity_type")
	AttrSignalName   = attribute.Key("skald.signal_name")
	AttrIdentity     = attribute.Key("skald.identity")
	AttrAttempt      = attribute.Key("skald.attempt")
	AttrEventID      = attribute.Key("skald.scheduled_event_id")
	AttrErrorCode    = attribute.Key("skald.error_code")
	AttrRequestID    = attribute.Key("skald.request_id")
	AttrHistoryLen   = attribute.Key("skald.history_length")
	AttrEmptyPoll    = attribute.Key("skald.poll_empty")
	AttrCommands     = attribute.Key("skald.commands")
)

// instrumentationName identifies this instrumentation library to a backend.
// It is the import path by convention, which is what makes "which library
// produced this span" answerable without a lookup table.
const instrumentationName = "github.com/Liona-orph/skald/internal/telemetry"

// newTracerProvider builds the provider Skald traces into.
//
// When no exporter is configured the result is a genuine no-op provider rather
// than an SDK provider wired to a discarding exporter. The two are
// observationally identical and wildly different in cost: the SDK provider
// still samples, allocates a span, records attributes and pushes it through a
// batch processor before the exporter throws it away, which on a poll-heavy hot
// path is real allocation for no benefit. The no-op provider returns spans that
// implement the interface and do nothing, so an un-instrumented deployment pays
// approximately nothing -- and, crucially, nothing in Skald depends on a
// collector being reachable in order to serve traffic.
func newTracerProvider(cfg Config) (trace.TracerProvider, func(context.Context) error, error) {
	if cfg.SpanExporter == nil {
		return noop.NewTracerProvider(), func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
	))
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: build trace resource: %w", err)
	}

	sampler := cfg.Sampler
	if sampler == nil {
		// ParentBased(AlwaysSample) keeps a trace that a client already decided
		// to sample intact, and starts new traces sampled. Head sampling is left
		// to whatever sits in front of Skald, because only that layer knows the
		// overall trace volume budget.
		sampler = sdktrace.ParentBased(sdktrace.AlwaysSample())
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(cfg.SpanExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	return tp, tp.Shutdown, nil
}

// noopTracer is the tracer a nil *Telemetry hands out, so that instrumentation
// added to a component is never the reason that component crashes.
func noopTracer() trace.Tracer { return noop.NewTracerProvider().Tracer(instrumentationName) }

// newPropagator returns the context propagator used on the HTTP boundary.
//
// It is stored on the Telemetry value rather than installed with
// otel.SetTextMapPropagator. A package-level setter is global mutable state:
// two servers in one test binary would fight over it, and a library that
// silently reconfigures a process-wide default is impossible to reason about
// from a call site. Passing the propagator explicitly costs one field.
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// EndSpan closes a span, recording err in the form a tracing backend expects.
//
// An api.Error contributes its code as an attribute so that traces can be
// filtered by failure class without string-matching a message.
func EndSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	defer span.End()
	if err == nil {
		return
	}
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		span.SetAttributes(AttrErrorCode.String(apiErr.Code))
		span.SetStatus(codes.Error, apiErr.Code)
		// The message is recorded as an event rather than the status
		// description because it can contain caller-supplied text, and status
		// descriptions end up in dashboards where a thousand distinct strings
		// are as useless as a thousand distinct label values.
		span.RecordError(err)
		return
	}
	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
}
