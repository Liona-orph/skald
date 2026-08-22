package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// InstrumentService wraps an api.Service with the instrumentation that only the
// domain layer can produce.
//
// # Why this is separate from the HTTP middleware
//
// The HTTP middleware sees a route, a status and a duration. It cannot see that
// this particular call completed a workflow, that that one was an activity's
// third attempt, or which workflow ID to hang a span attribute on -- all of
// that is inside a JSON body it deliberately does not parse. So the transport
// layer measures transport things and this decorator measures workflow things.
//
// The split has a second payoff: an embedded deployment that talks to the
// engine in-process, with no HTTP at all, still gets spans and workflow metrics
// by wrapping the engine here. Observability is not a property of having chosen
// the network.
//
// defaultNamespace mirrors the engine's own default so that a request which
// omits the namespace is counted under the namespace it will actually run in,
// rather than under a placeholder.
func InstrumentService(next api.Service, t *Telemetry, defaultNamespace string) api.Service {
	if defaultNamespace == "" {
		defaultNamespace = skald.DefaultNamespace
	}
	return &instrumentedService{next: next, tel: t, defaultNS: defaultNamespace}
}

type instrumentedService struct {
	next      api.Service
	tel       *Telemetry
	defaultNS string
}

var _ api.Service = (*instrumentedService)(nil)

func (s *instrumentedService) metrics() *Metrics {
	if s.tel == nil {
		return nil
	}
	return s.tel.Metrics
}

func (s *instrumentedService) namespace(ns string) string {
	if ns == "" {
		return s.defaultNS
	}
	return ns
}

// call is one instrumented operation in flight.
type call struct {
	svc       *instrumentedService
	span      trace.Span
	start     time.Time
	namespace string
	// appends records history when it succeeds, so its latency belongs in the
	// append histogram as well as in the request histogram.
	appends bool
}

// begin starts a span, enriches the context logger with the execution identity
// and returns the handle that closes both.
func (s *instrumentedService) begin(ctx context.Context, op, namespace, workflowID, runID string) (context.Context, *call) {
	ns := s.namespace(namespace)

	tracer := noopTracer()
	if s.tel != nil {
		tracer = s.tel.Tracer()
	}
	ctx, span := tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(AttrNamespace.String(ns))
	if workflowID != "" {
		span.SetAttributes(AttrWorkflowID.String(workflowID))
	}
	if runID != "" {
		span.SetAttributes(AttrRunID.String(runID))
	}

	// The logger handed downstream carries the execution identity, so a line
	// emitted three layers below this call is still attributable to a workflow
	// without every layer having to thread the identifiers through by hand.
	ctx = ContextWithLogger(ctx, WithExecution(LoggerFrom(ctx), ns, workflowID, runID))

	return ctx, &call{svc: s, span: span, start: time.Now(), namespace: ns}
}

// end closes the span and records the append histogram when appropriate.
func (c *call) end(err error) {
	if c.appends && err == nil {
		c.svc.metrics().ObserveHistoryAppend(c.namespace, time.Since(c.start))
	}
	EndSpan(c.span, err)
}

// ---------------------------------------------------------------------------
// Client-facing operations
// ---------------------------------------------------------------------------

func (s *instrumentedService) StartWorkflow(ctx context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
	ctx, c := s.begin(ctx, OpStartWorkflow, req.Namespace, req.WorkflowID, "")
	c.appends = true
	c.span.SetAttributes(AttrWorkflowType.String(req.WorkflowType), AttrTaskQueue.String(req.TaskQueue))

	resp, err := s.next.StartWorkflow(ctx, req)
	if err == nil {
		c.span.SetAttributes(AttrRunID.String(resp.RunID))
	}
	c.end(err)
	return resp, err
}

func (s *instrumentedService) SignalWorkflow(ctx context.Context, req api.SignalWorkflowRequest) error {
	ctx, c := s.begin(ctx, OpSignalWorkflow, req.Namespace, req.WorkflowID, req.RunID)
	c.appends = true
	c.span.SetAttributes(AttrSignalName.String(req.SignalName))

	err := s.next.SignalWorkflow(ctx, req)
	c.end(err)
	return err
}

func (s *instrumentedService) SignalWithStartWorkflow(ctx context.Context, req api.SignalWithStartRequest) (api.StartWorkflowResponse, error) {
	ctx, c := s.begin(ctx, OpSignalWithStartWorkflow, req.Start.Namespace, req.Start.WorkflowID, "")
	c.appends = true
	c.span.SetAttributes(
		AttrWorkflowType.String(req.Start.WorkflowType),
		AttrTaskQueue.String(req.Start.TaskQueue),
		AttrSignalName.String(req.SignalName),
	)

	resp, err := s.next.SignalWithStartWorkflow(ctx, req)
	if err == nil {
		c.span.SetAttributes(AttrRunID.String(resp.RunID))
	}
	c.end(err)
	return resp, err
}

func (s *instrumentedService) CancelWorkflow(ctx context.Context, req api.CancelWorkflowRequest) error {
	ctx, c := s.begin(ctx, OpCancelWorkflow, req.Namespace, req.WorkflowID, req.RunID)
	c.appends = true

	err := s.next.CancelWorkflow(ctx, req)
	c.end(err)
	return err
}

func (s *instrumentedService) TerminateWorkflow(ctx context.Context, req api.TerminateWorkflowRequest) error {
	ctx, c := s.begin(ctx, OpTerminateWorkflow, req.Namespace, req.WorkflowID, req.RunID)
	c.appends = true

	err := s.next.TerminateWorkflow(ctx, req)
	if err == nil {
		// Termination is the one terminal status that never passes through a
		// worker's command batch, so it is counted here. The two statuses that
		// are still invisible from the API boundary are TIMED_OUT and the
		// workflow-level retry exhaustion path, both of which the engine decides
		// on a timer with no request in flight; counting those needs an engine
		// hook that does not exist yet.
		s.metrics().ObserveWorkflowCompletion(c.namespace, skald.StatusTerminated.String())
	}
	c.end(err)
	return err
}

func (s *instrumentedService) DescribeWorkflow(ctx context.Context, namespace, workflowID, runID string) (api.DescribeWorkflowResponse, error) {
	ctx, c := s.begin(ctx, OpDescribeWorkflow, namespace, workflowID, runID)

	resp, err := s.next.DescribeWorkflow(ctx, namespace, workflowID, runID)
	if err == nil {
		c.span.SetAttributes(
			AttrRunID.String(resp.RunID),
			AttrWorkflowType.String(resp.WorkflowType),
			AttrHistoryLen.Int64(resp.HistoryLength),
		)
	}
	c.end(err)
	return resp, err
}

func (s *instrumentedService) GetHistory(ctx context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
	ctx, c := s.begin(ctx, OpGetHistory, req.Namespace, req.WorkflowID, req.RunID)
	if req.WaitForNew {
		// Tell the transport middleware to charge this call to the long-poll
		// histogram. Doing it here rather than in the HTTP handler means an
		// embedded caller gets the same classification.
		MarkLongPoll(ctx)
	}

	resp, err := s.next.GetHistory(ctx, req)
	if err == nil {
		c.span.SetAttributes(AttrHistoryLen.Int(len(resp.Events)))
	}
	c.end(err)
	return resp, err
}

func (s *instrumentedService) ListWorkflows(ctx context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
	ctx, c := s.begin(ctx, OpListWorkflows, req.Namespace, req.WorkflowID, "")

	resp, err := s.next.ListWorkflows(ctx, req)
	c.end(err)
	return resp, err
}

// ---------------------------------------------------------------------------
// Worker-facing operations
// ---------------------------------------------------------------------------

func (s *instrumentedService) PollWorkflowTask(ctx context.Context, req api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	ctx, c := s.begin(ctx, OpPollWorkflowTask, req.Namespace, "", "")
	c.span.SetAttributes(AttrTaskQueue.String(req.TaskQueue), AttrIdentity.String(req.Identity))
	MarkLongPoll(ctx)

	task, err := s.next.PollWorkflowTask(ctx, req)
	switch {
	case err != nil:
	case task.Empty:
		c.span.SetAttributes(AttrEmptyPoll.Bool(true))
		s.metrics().ObservePollTimeout(c.namespace, req.TaskQueue, matching.KindWorkflow)
	default:
		c.span.SetAttributes(
			AttrWorkflowID.String(task.Execution.WorkflowID),
			AttrRunID.String(task.Execution.RunID),
			AttrWorkflowType.String(task.WorkflowType),
			AttrAttempt.Int(int(task.Attempt)),
			AttrHistoryLen.Int(len(task.History)),
		)
	}
	c.end(err)
	return task, err
}

func (s *instrumentedService) RespondWorkflowTaskCompleted(ctx context.Context, req api.RespondWorkflowTaskCompletedRequest) error {
	ctx, c := s.begin(ctx, OpRespondWorkflowTaskDone, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.appends = true
	c.span.SetAttributes(AttrCommands.Int(len(req.Commands)))

	err := s.next.RespondWorkflowTaskCompleted(ctx, req)
	if err == nil {
		s.metrics().ObserveHistoryEvents(c.namespace, appendedEvents(req.Commands))
		if status, ok := terminalStatus(req.Commands); ok {
			s.metrics().ObserveWorkflowCompletion(c.namespace, status.String())
		}
	}
	c.end(err)
	return err
}

func (s *instrumentedService) RespondWorkflowTaskFailed(ctx context.Context, req api.RespondWorkflowTaskFailedRequest) error {
	ctx, c := s.begin(ctx, OpRespondWorkflowTaskFailed, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.appends = true

	err := s.next.RespondWorkflowTaskFailed(ctx, req)
	c.end(err)
	return err
}

func (s *instrumentedService) PollActivityTask(ctx context.Context, req api.PollActivityTaskRequest) (api.ActivityTask, error) {
	ctx, c := s.begin(ctx, OpPollActivityTask, req.Namespace, "", "")
	c.span.SetAttributes(AttrTaskQueue.String(req.TaskQueue), AttrIdentity.String(req.Identity))
	MarkLongPoll(ctx)

	task, err := s.next.PollActivityTask(ctx, req)
	switch {
	case err != nil:
	case task.Empty:
		c.span.SetAttributes(AttrEmptyPoll.Bool(true))
		s.metrics().ObservePollTimeout(c.namespace, req.TaskQueue, matching.KindActivity)
	default:
		c.span.SetAttributes(
			AttrWorkflowID.String(task.Execution.WorkflowID),
			AttrRunID.String(task.Execution.RunID),
			AttrActivityID.String(task.ActivityID),
			AttrActivityType.String(task.ActivityType),
			AttrAttempt.Int(int(task.Attempt)),
			AttrEventID.Int64(task.ScheduledEventID),
		)
		// An attempt is counted when a worker takes it, not when it finishes:
		// an attempt that never reports back is exactly the one an operator is
		// hunting for, and counting on completion would hide it.
		s.metrics().ObserveActivityAttempt(c.namespace, task.ActivityType)
	}
	c.end(err)
	return task, err
}

func (s *instrumentedService) RespondActivityTaskCompleted(ctx context.Context, req api.RespondActivityTaskCompletedRequest) error {
	ctx, c := s.begin(ctx, OpRespondActivityTaskDone, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.appends = true
	c.span.SetAttributes(AttrEventID.Int64(req.ScheduledEventID))

	err := s.next.RespondActivityTaskCompleted(ctx, req)
	if err == nil {
		s.metrics().ObserveActivityResult(c.namespace, ActivityOutcomeCompleted)
	}
	c.end(err)
	return err
}

func (s *instrumentedService) RespondActivityTaskFailed(ctx context.Context, req api.RespondActivityTaskFailedRequest) error {
	ctx, c := s.begin(ctx, OpRespondActivityTaskFailed, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.appends = true
	c.span.SetAttributes(AttrEventID.Int64(req.ScheduledEventID))

	err := s.next.RespondActivityTaskFailed(ctx, req)
	if err == nil {
		s.metrics().ObserveActivityResult(c.namespace, ActivityOutcomeFailed)
	}
	c.end(err)
	return err
}

func (s *instrumentedService) RespondActivityTaskCanceled(ctx context.Context, req api.RespondActivityTaskCanceledRequest) error {
	ctx, c := s.begin(ctx, OpRespondActivityTaskCancel, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.appends = true
	c.span.SetAttributes(AttrEventID.Int64(req.ScheduledEventID))

	err := s.next.RespondActivityTaskCanceled(ctx, req)
	if err == nil {
		s.metrics().ObserveActivityResult(c.namespace, ActivityOutcomeCanceled)
	}
	c.end(err)
	return err
}

func (s *instrumentedService) RecordActivityHeartbeat(ctx context.Context, req api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error) {
	ctx, c := s.begin(ctx, OpRecordActivityHeartbeat, req.Namespace, req.Execution.WorkflowID, req.Execution.RunID)
	c.span.SetAttributes(AttrEventID.Int64(req.ScheduledEventID))

	resp, err := s.next.RecordActivityHeartbeat(ctx, req)
	c.end(err)
	return resp, err
}

// ---------------------------------------------------------------------------
// Command batch analysis
// ---------------------------------------------------------------------------

// terminalStatus reports the workflow status a command batch closes with.
func terminalStatus(cmds []history.Command) (skald.WorkflowStatus, bool) {
	for _, cmd := range cmds {
		switch cmd.Type {
		case history.CommandTypeCompleteWorkflowExecution:
			return skald.StatusCompleted, true
		case history.CommandTypeFailWorkflowExecution:
			return skald.StatusFailed, true
		case history.CommandTypeCancelWorkflowExecution:
			return skald.StatusCanceled, true
		case history.CommandTypeContinueAsNewWorkflow:
			return skald.StatusContinuedAsNew, true
		}
	}
	return skald.StatusRunning, false
}

// appendedEvents estimates how many history events a command batch produces.
//
// Every command Skald has today expands to exactly one event, and the batch is
// always preceded by a WorkflowTaskCompleted event -- hence len+1. The engine
// may additionally schedule a follow-up workflow task, which this cannot see,
// so the value is a lower bound by at most one. That is stated here rather than
// in the metric's help text on purpose: help text is scraped into dashboards
// where nobody reads it, and the moment a command expands to two events this
// comment is what the next person needs.
func appendedEvents(cmds []history.Command) int { return len(cmds) + 1 }
