package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Attribute keys. They are constants because a log field that is spelled
// "workflowID" in one file and "workflow_id" in another cannot be searched for
// during an incident, and an incident is the only time anyone reads these.
const (
	KeyNamespace  = "namespace"
	KeyWorkflowID = "workflow_id"
	KeyRunID      = "run_id"
	KeyRequestID  = "request_id"
	KeyOperation  = "operation"
	KeyTraceID    = "trace_id"
	KeyTaskQueue  = "task_queue"
	KeyError      = "error"
	KeyCode       = "code"
	KeyStatus     = "status"
)

// LogFormat selects the encoding of the log stream.
type LogFormat string

const (
	// LogFormatJSON emits one JSON object per line. It is the default because
	// the primary consumer of a server's log is a collector, not a person.
	LogFormatJSON LogFormat = "json"
	// LogFormatText emits slog's key=value form, which is what you want when
	// the primary consumer is a person with a terminal.
	LogFormatText LogFormat = "text"
)

// ParseLogFormat converts a configuration string to a LogFormat.
func ParseLogFormat(s string) (LogFormat, error) {
	switch LogFormat(strings.ToLower(strings.TrimSpace(s))) {
	case LogFormatJSON:
		return LogFormatJSON, nil
	case LogFormatText:
		return LogFormatText, nil
	case "":
		return LogFormatJSON, nil
	}
	return "", fmt.Errorf("telemetry: unknown log format %q (want %q or %q)", s, LogFormatJSON, LogFormatText)
}

// ParseLevel converts a configuration string to an slog level.
//
// slog.Level implements encoding.TextUnmarshaler, so this is a thin wrapper --
// but it exists so that a bad value produces an error naming the accepted set
// rather than slog's terser message, and so that the empty string means "the
// default" instead of an error.
func ParseLevel(s string) (slog.Level, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return slog.LevelInfo, nil
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(s))); err != nil {
		return 0, fmt.Errorf("telemetry: unknown log level %q (want debug, info, warn or error)", s)
	}
	return lvl, nil
}

// NewLogger builds the process logger.
//
// Source locations are deliberately off: they cost a runtime.Callers walk per
// record, and in a server whose log lines are already tagged with an operation
// and a request ID the file:line adds nothing an operator uses.
func NewLogger(w io.Writer, format LogFormat, level slog.Leveler) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	opts := &slog.HandlerOptions{Level: level}
	if format == LogFormatText {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// nopLogger is the value LoggerFrom returns when a context carries none.
//
// It is package level but never reassigned: an slog.Logger is immutable, so
// this is a constant in every sense except the one the language recognises.
// Returning it, rather than nil, means no call site has to nil-check a logger.
var nopLogger = slog.New(discardHandler{})

// discardHandler drops every record. slog.NewTextHandler(io.Discard) would
// work, but it still formats the record before throwing it away; this refuses
// the work at Enabled, which is the whole point of a discarding logger.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

// NopLogger returns a logger that discards everything, for tests and for
// components whose caller did not supply one.
func NopLogger() *slog.Logger { return nopLogger }

type loggerKey struct{}

type requestIDKey struct{}

// ContextWithLogger attaches a request-scoped logger.
func ContextWithLogger(ctx context.Context, log *slog.Logger) context.Context {
	if log == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey{}, log)
}

// LoggerFrom returns the request-scoped logger, or a discarding one.
//
// A discarding default rather than a global fallback is deliberate. A component
// that logs to a logger nobody configured is writing to a stream nobody reads;
// making that silent forces the wiring to be explicit at the one place that
// knows where logs should go, which is the process entry point.
func LoggerFrom(ctx context.Context) *slog.Logger {
	log, _ := LoggerFromContext(ctx)
	return log
}

// LoggerFromContext returns the request-scoped logger and whether the context
// actually carried one.
//
// The boolean matters for the outermost middleware, which runs before any
// request-scoped logger exists and must fall back to the process logger rather
// than silently discard a panic report.
func LoggerFromContext(ctx context.Context) (*slog.Logger, bool) {
	if ctx == nil {
		return nopLogger, false
	}
	if log, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && log != nil {
		return log, true
	}
	return nopLogger, false
}

// ContextWithRequestID records the correlation identifier for this request.
func ContextWithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the correlation identifier, or the empty string.
func RequestIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// WithExecution returns a logger carrying a workflow execution's identity.
//
// Empty components are omitted rather than logged as empty strings: a run ID is
// genuinely unknown until the server picks one, and a field that is sometimes
// "" and sometimes a UUID makes every log query need two cases.
func WithExecution(log *slog.Logger, namespace, workflowID, runID string) *slog.Logger {
	if log == nil {
		return nopLogger
	}
	attrs := make([]any, 0, 6)
	if namespace != "" {
		attrs = append(attrs, KeyNamespace, namespace)
	}
	if workflowID != "" {
		attrs = append(attrs, KeyWorkflowID, workflowID)
	}
	if runID != "" {
		attrs = append(attrs, KeyRunID, runID)
	}
	if len(attrs) == 0 {
		return log
	}
	return log.With(attrs...)
}
