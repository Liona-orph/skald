package telemetry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

func TestNilMetricsDiscardsEverything(t *testing.T) {
	t.Parallel()

	// A component can be wired without instrumentation and without a nil check
	// at every call site.
	var m *Metrics
	require.NotPanics(t, func() {
		m.ObserveRequest(OpStartWorkflow, CodeOK, false, time.Millisecond)
		m.ObserveHistoryAppend("prod", time.Millisecond)
		m.ObserveHistoryEvents("prod", 3)
		m.ObserveWorkflowCompletion("prod", "COMPLETED")
		m.ObserveActivityAttempt("prod", "Charge")
		m.ObserveActivityResult("prod", ActivityOutcomeFailed)
		m.ObservePollTimeout("prod", "orders", matching.KindWorkflow)
		m.RequestStarted(OpStartWorkflow)(CodeOK, false, time.Millisecond)
		require.IsType(t, matching.NopMetrics{}, m.MatchingMetrics())
	})
}

func TestRegisteringTwiceIntoOneRegistryIsAnErrorNotAPanic(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	_, err := NewMetrics(reg)
	require.NoError(t, err)

	// promauto would panic here. A wiring bug deserves an error the caller can
	// report with context, not a stack trace from an init function.
	_, err = NewMetrics(reg)
	require.ErrorContains(t, err, "register metrics")
}

func TestHistogramSelection(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	require.NoError(t, err)

	// A poll's latency belongs in the poll histogram, whose buckets bracket the
	// server's poll timeout; a start's belongs in the fast one, whose buckets
	// start at a millisecond. One bucket set cannot serve both.
	m.ObserveRequest(OpPollWorkflowTask, CodeOK, false, 30*time.Second)
	m.ObserveRequest(OpStartWorkflow, CodeOK, false, 3*time.Millisecond)

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_poll_duration_seconds_count{operation="PollWorkflowTask"} 1`)
	require.Contains(t, body, `skald_request_duration_seconds_count{operation="StartWorkflow"} 1`)
	require.Contains(t, body, `skald_poll_duration_seconds_bucket{operation="PollWorkflowTask",le="30"} 1`)
	require.Contains(t, body, `skald_request_duration_seconds_bucket{operation="StartWorkflow",le="0.005"} 1`)
}

func TestBucketsCoverTheirOperationsRange(t *testing.T) {
	t.Parallel()

	// The bucket boundaries are the resolution of the metric. A poll set that
	// topped out below the poll timeout, or an append set that started above a
	// typical append, would produce a flat quantile that reads as healthy.
	require.Greater(t, pollBuckets[len(pollBuckets)-1], 50.0, "poll buckets must extend past the server's poll cap")
	require.LessOrEqual(t, appendBuckets[0], 0.001, "append buckets must resolve sub-millisecond writes")
	require.LessOrEqual(t, fastBuckets[0], 0.001)
	require.GreaterOrEqual(t, fastBuckets[len(fastBuckets)-1], 10.0)
}

func TestEmptyLabelsGetAPlaceholder(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	require.NoError(t, err)

	m.ObserveActivityAttempt("", "")
	// An empty label reads identically to "never emitted", which is the last
	// thing anyone needs at 3am.
	require.Contains(t, gatherText(t, reg),
		`skald_activity_attempts_total{activity_type="unresolved",namespace="unresolved"} 1`)
}

func TestMatchingAdapter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	require.NoError(t, err)

	key := matching.QueueKey{Namespace: "prod", TaskQueue: "orders", Kind: matching.KindActivity}
	adapter := m.MatchingMetrics()
	adapter.TaskAdded(key, true)
	adapter.TaskAdded(key, false)
	adapter.TaskDropped(key, matching.DropBacklogFull)
	adapter.BacklogDepth(key, 17)
	adapter.PollerCount(key, 4)

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_task_matches_total{kind="activity",match="sync",namespace="prod",task_queue="orders"} 1`)
	require.Contains(t, body, `skald_task_matches_total{kind="activity",match="async",namespace="prod",task_queue="orders"} 1`)
	require.Contains(t, body, `skald_tasks_dropped_total{kind="activity",namespace="prod",reason="backlog_full",task_queue="orders"} 1`)
	require.Contains(t, body, `skald_task_queue_backlog{kind="activity",namespace="prod",task_queue="orders"} 17`)
	require.Contains(t, body, `skald_task_queue_pollers{kind="activity",namespace="prod",task_queue="orders"} 4`)
}

// TestNoUnboundedLabels is the executable form of the cardinality rule.
//
// A time series is created per distinct label combination and lives for the
// lifetime of the process plus the retention window behind it. Labelling by
// workflow ID means one series per workflow -- a million a day for a busy
// deployment -- and the failure mode is not a slow dashboard but the monitoring
// system that was supposed to report the outage going down itself.
func TestNoUnboundedLabels(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	require.NoError(t, err)

	m.ObserveRequest(OpStartWorkflow, CodeOK, false, time.Millisecond)
	m.ObserveWorkflowCompletion("prod", skald.StatusCompleted.String())
	m.ObserveActivityAttempt("prod", "ChargeCard")
	m.ObserveHistoryEvents("prod", 4)

	families, err := reg.Gather()
	require.NoError(t, err)

	banned := map[string]bool{
		"workflow_id": true, "run_id": true, "activity_id": true,
		"request_id": true, "identity": true, "scheduled_event_id": true,
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				require.False(t, banned[label.GetName()],
					"metric %s must not be labelled by %s", family.GetName(), label.GetName())
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Service instrumentation
// ---------------------------------------------------------------------------

// recordingService is a minimal api.Service that succeeds at everything.
type recordingService struct {
	api.Service
	pollWorkflow func(context.Context, api.PollWorkflowTaskRequest) (api.WorkflowTask, error)
	pollActivity func(context.Context, api.PollActivityTaskRequest) (api.ActivityTask, error)
	respondWF    func(context.Context, api.RespondWorkflowTaskCompletedRequest) error
	terminate    func(context.Context, api.TerminateWorkflowRequest) error
	start        func(context.Context, api.StartWorkflowRequest) (api.StartWorkflowResponse, error)
}

func (s *recordingService) StartWorkflow(ctx context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
	if s.start != nil {
		return s.start(ctx, req)
	}
	return api.StartWorkflowResponse{RunID: "run-1", Started: true}, nil
}

func (s *recordingService) TerminateWorkflow(ctx context.Context, req api.TerminateWorkflowRequest) error {
	if s.terminate != nil {
		return s.terminate(ctx, req)
	}
	return nil
}

func (s *recordingService) PollWorkflowTask(ctx context.Context, req api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	if s.pollWorkflow != nil {
		return s.pollWorkflow(ctx, req)
	}
	return api.WorkflowTask{Empty: true}, nil
}

func (s *recordingService) PollActivityTask(ctx context.Context, req api.PollActivityTaskRequest) (api.ActivityTask, error) {
	if s.pollActivity != nil {
		return s.pollActivity(ctx, req)
	}
	return api.ActivityTask{Empty: true}, nil
}

func (s *recordingService) RespondWorkflowTaskCompleted(ctx context.Context, req api.RespondWorkflowTaskCompletedRequest) error {
	if s.respondWF != nil {
		return s.respondWF(ctx, req)
	}
	return nil
}

func newInstrumented(t *testing.T, svc api.Service) (api.Service, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	tel, err := New(Config{Registry: reg, Logger: NopLogger()})
	require.NoError(t, err)
	return InstrumentService(svc, tel, "prod"), reg
}

func TestInstrumentedServiceCountsWorkflowCompletions(t *testing.T) {
	t.Parallel()

	svc, reg := newInstrumented(t, &recordingService{})
	require.NoError(t, svc.RespondWorkflowTaskCompleted(context.Background(), api.RespondWorkflowTaskCompletedRequest{
		Execution: skald.WorkflowExecution{WorkflowID: "order-1", RunID: "run-1"},
		Commands: []history.Command{
			{Type: history.CommandTypeRecordMarker, RecordMarker: &history.RecordMarkerCommand{MarkerName: "SideEffect"}},
			{Type: history.CommandTypeCompleteWorkflowExecution, CompleteWorkflow: &history.CompleteWorkflowCommand{}},
		},
	}))

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_workflow_completions_total{namespace="prod",status="COMPLETED"} 1`)
	// One event per command plus the task-completed event.
	require.Contains(t, body, `skald_history_events_total{namespace="prod"} 3`)
	require.Contains(t, body, `skald_history_append_duration_seconds_count 1`)
}

func TestInstrumentedServiceCountsTerminations(t *testing.T) {
	t.Parallel()

	svc, reg := newInstrumented(t, &recordingService{})
	require.NoError(t, svc.TerminateWorkflow(context.Background(), api.TerminateWorkflowRequest{WorkflowID: "order-1"}))
	require.Contains(t, gatherText(t, reg),
		`skald_workflow_completions_total{namespace="prod",status="TERMINATED"} 1`)
}

func TestInstrumentedServiceCountsPollOutcomes(t *testing.T) {
	t.Parallel()

	svc, reg := newInstrumented(t, &recordingService{
		pollActivity: func(context.Context, api.PollActivityTaskRequest) (api.ActivityTask, error) {
			return api.ActivityTask{
				Execution:    skald.WorkflowExecution{WorkflowID: "order-1", RunID: "run-1"},
				ActivityType: "ChargeCard",
				Attempt:      3,
			}, nil
		},
	})

	// An empty workflow poll is a timeout; a non-empty activity poll is an
	// attempt handed to a worker.
	_, err := svc.PollWorkflowTask(context.Background(), api.PollWorkflowTaskRequest{TaskQueue: "orders"})
	require.NoError(t, err)
	_, err = svc.PollActivityTask(context.Background(), api.PollActivityTaskRequest{TaskQueue: "orders"})
	require.NoError(t, err)

	body := gatherText(t, reg)
	require.Contains(t, body, `skald_poll_timeouts_total{kind="workflow",namespace="prod",task_queue="orders"} 1`)
	require.Contains(t, body, `skald_activity_attempts_total{activity_type="ChargeCard",namespace="prod"} 1`)
}

func TestInstrumentedServiceEnrichesTheContextLogger(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	reg := prometheus.NewRegistry()
	tel, err := New(Config{Registry: reg, Logger: NopLogger()})
	require.NoError(t, err)

	svc := InstrumentService(&recordingService{
		start: func(ctx context.Context, _ api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
			LoggerFrom(ctx).Info("deep inside")
			return api.StartWorkflowResponse{RunID: "run-1"}, nil
		},
	}, tel, "prod")

	ctx := ContextWithLogger(context.Background(), NewLogger(&buf, LogFormatJSON, nil))
	_, err = svc.StartWorkflow(ctx, api.StartWorkflowRequest{WorkflowID: "order-1"})
	require.NoError(t, err)

	// A line emitted three layers down is still attributable to a workflow
	// without every layer threading the identifiers through by hand.
	require.Contains(t, buf.String(), `"namespace":"prod"`)
	require.Contains(t, buf.String(), `"workflow_id":"order-1"`)
}

func TestAppendedEventsAndTerminalStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1, appendedEvents(nil))
	require.Equal(t, 3, appendedEvents(make([]history.Command, 2)))

	for cmd, want := range map[history.CommandType]skald.WorkflowStatus{
		history.CommandTypeCompleteWorkflowExecution: skald.StatusCompleted,
		history.CommandTypeFailWorkflowExecution:     skald.StatusFailed,
		history.CommandTypeCancelWorkflowExecution:   skald.StatusCanceled,
		history.CommandTypeContinueAsNewWorkflow:     skald.StatusContinuedAsNew,
	} {
		got, ok := terminalStatus([]history.Command{{Type: cmd}})
		require.True(t, ok, cmd)
		require.Equal(t, want, got, cmd)
	}

	_, ok := terminalStatus([]history.Command{{Type: history.CommandTypeStartTimer}})
	require.False(t, ok)
}

func TestEndSpanRecordsTheErrorCode(t *testing.T) {
	t.Parallel()

	// The default provider is a genuine no-op, so this proves only that the
	// call is safe on a non-recording span -- which is the path every
	// un-instrumented deployment takes on every request.
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "op")
	require.NotPanics(t, func() {
		EndSpan(span, &api.Error{Code: api.CodeNotFound, Message: "nope"})
		EndSpan(span, errors.New("plain"))
		EndSpan(span, nil)
		EndSpan(nil, nil)
	})
}

func TestDefaultTracerProviderNeedsNoCollector(t *testing.T) {
	t.Parallel()

	// Nothing in Skald may depend on a collector being reachable in order to
	// serve traffic, so the default provider is a no-op rather than an SDK
	// provider wired to a discarding exporter.
	tp, shutdown, err := newTracerProvider(Config{ServiceName: "skald"})
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))

	_, span := tp.Tracer("test").Start(context.Background(), "op")
	require.False(t, span.SpanContext().IsSampled())
	require.False(t, span.IsRecording())
}
