package telemetry

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Liona-orph/skald/internal/matching"
)

// Operation names. They double as the `operation` metric label and as the span
// name, so they are declared once here rather than derived from the URL path.
//
// Deriving the label from r.URL.Path would be convenient and wrong: a client
// that requests /api/v1/workflows/../../oops creates a new time series, and a
// scanner walking the address space creates thousands. A closed set of
// compile-time constants makes that class of bug unrepresentable.
const (
	OpStartWorkflow             = "StartWorkflow"
	OpSignalWorkflow            = "SignalWorkflow"
	OpSignalWithStartWorkflow   = "SignalWithStartWorkflow"
	OpCancelWorkflow            = "CancelWorkflow"
	OpTerminateWorkflow         = "TerminateWorkflow"
	OpDescribeWorkflow          = "DescribeWorkflow"
	OpGetHistory                = "GetHistory"
	OpListWorkflows             = "ListWorkflows"
	OpPollWorkflowTask          = "PollWorkflowTask"
	OpRespondWorkflowTaskDone   = "RespondWorkflowTaskCompleted"
	OpRespondWorkflowTaskFailed = "RespondWorkflowTaskFailed"
	OpPollActivityTask          = "PollActivityTask"
	OpRespondActivityTaskDone   = "RespondActivityTaskCompleted"
	OpRespondActivityTaskFailed = "RespondActivityTaskFailed"
	OpRespondActivityTaskCancel = "RespondActivityTaskCanceled"
	OpRecordActivityHeartbeat   = "RecordActivityHeartbeat"
	OpHealth                    = "Health"
	OpReady                     = "Ready"
	OpMetrics                   = "Metrics"
	OpUnknown                   = "Unknown"
)

// CodeOK is the result-code label used for a successful operation. It is not an
// api.Error code -- success is not an error -- but it shares the label so that a
// single query can compute an error ratio without knowing every error code.
const CodeOK = "ok"

// Label values that are not codes or operations.
const (
	ActivityOutcomeCompleted = "completed"
	ActivityOutcomeFailed    = "failed"
	ActivityOutcomeCanceled  = "canceled"

	matchSync  = "sync"
	matchAsync = "async"

	// unresolvedLabel stands in for a dimension whose value was not known at
	// the point of observation.
	unresolvedLabel = "unresolved"
)

// Bucket sets.
//
// One histogram cannot serve a 400 microsecond append and a 50 second poll. The
// bucket boundaries *are* the resolution of the metric: a poll observed against
// buckets that top out at 10s reports "+Inf" for every healthy poll, and an
// append observed against buckets that start at 500ms reports "le=0.5" for
// every append the store ever performs. Both produce a p99 that is a straight
// line no matter what the system does, which is worse than having no metric at
// all, because a flat line reads as "healthy".
//
// So the sets below are chosen from what the operation actually does:
//
//   - fastBuckets covers one store round trip plus JSON: hundreds of
//     microseconds when SQLite is warm, tens of milliseconds when it is not,
//     seconds only when something is wrong. It spans 1ms to 10s.
//   - pollBuckets covers a long poll, whose *expected* value is the server's
//     poll timeout. The interesting region is the left tail -- a poll that
//     returns quickly means work was waiting -- so the low buckets are fine and
//     the high ones bracket the timeout.
//   - appendBuckets covers a single store transaction and is tighter than
//     fastBuckets at the bottom: an append that takes 2ms instead of 500us is a
//     fsync regression an operator needs to see, and fastBuckets cannot show it.
var (
	fastBuckets   = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	pollBuckets   = []float64{0.005, 0.05, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90}
	appendBuckets = []float64{0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}
	// eventBuckets counts events per append. A workflow task usually produces a
	// handful; a fan-out produces hundreds. Powers of two keep the series small
	// while still separating "normal" from "this workflow is a loop".
	eventBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
)

// longPollOperations is the set whose latency is measured against pollBuckets.
//
// GetHistory is absent on purpose: the same endpoint serves a 2ms read and a
// 50s wait depending on one field of the request body, so the handler
// classifies it per call through MarkLongPoll.
var longPollOperations = map[string]bool{
	OpPollWorkflowTask: true,
	OpPollActivityTask: true,
}

// Metrics is Skald's Prometheus instrumentation.
//
// # The cardinality rule
//
// No metric in this file is ever labelled by workflow ID, run ID, activity ID,
// request ID or any other identifier chosen by a caller.
//
// The reason is that a Prometheus time series is created per distinct label
// combination and lives, in memory, for the lifetime of the process plus the
// retention window of the storage behind it. Labelling by workflow ID means one
// series per workflow: a deployment running a million workflows a day creates a
// million series a day, each with its own name, help text, sample buffer and
// index entries. The scrape grows without bound, the TSDB's inverted index
// stops fitting in memory, and the failure mode is not "the dashboard is slow"
// but "the monitoring system that was supposed to tell us the engine is down is
// itself down". This is the single most common way a well-intentioned metric
// takes out a production observability stack.
//
// The bounded dimensions are namespace, task queue, workflow type, activity
// type, operation and status. Every one of them is fixed by deployment
// configuration or by the code that is deployed, so their product is a number
// an operator can predict before turning the service on.
//
// Per-execution detail belongs in two other places: a trace, where a span is
// stored once and never aggregated, and a log line, which an index can shard.
// Both are asked for by identifier after you already know which workflow you
// care about; a metric is asked "is anything wrong", which is a question about
// aggregates.
//
// A nil *Metrics is valid and discards everything, so a component can be wired
// without instrumentation and without nil checks at every call site.
type Metrics struct {
	requests        *prometheus.CounterVec
	requestErrors   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	pollDuration    *prometheus.HistogramVec
	inFlight        *prometheus.GaugeVec

	backlog      *prometheus.GaugeVec
	pollers      *prometheus.GaugeVec
	matches      *prometheus.CounterVec
	tasksDropped *prometheus.CounterVec
	pollTimeouts *prometheus.CounterVec

	appendDuration prometheus.Histogram
	appendEvents   prometheus.Histogram
	historyEvents  *prometheus.CounterVec

	workflowCompletions *prometheus.CounterVec
	activityAttempts    *prometheus.CounterVec
	activityResults     *prometheus.CounterVec
}

// NewMetrics builds and registers the metric set.
//
// Registration failures are returned rather than panicked on: the only way to
// hit one is to build two Metrics against the same registry, which is a wiring
// bug the caller can report far more usefully than a stack trace can.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	queueLabels := []string{KeyNamespace, KeyTaskQueue, "kind"}

	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_requests_total",
			Help: "Requests handled, by operation and result code.",
		}, []string{KeyOperation, KeyCode}),
		requestErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_request_errors_total",
			Help: "Requests that failed, by operation and error code. A subset of skald_requests_total, broken out so an alert can be written without a label matcher on every success code.",
		}, []string{KeyOperation, KeyCode}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "skald_request_duration_seconds",
			Help:    "Latency of non-polling operations.",
			Buckets: fastBuckets,
		}, []string{KeyOperation}),
		pollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "skald_poll_duration_seconds",
			Help:    "Latency of long-polling operations. A distribution piled up at the server's poll timeout means no work is arriving; a left-shifted one means the queue is hot.",
			Buckets: pollBuckets,
		}, []string{KeyOperation}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "skald_requests_in_flight",
			Help: "Requests currently being served, by operation.",
		}, []string{KeyOperation}),

		backlog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "skald_task_queue_backlog",
			Help: "Task references waiting for a poller.",
		}, queueLabels),
		pollers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "skald_task_queue_pollers",
			Help: "Workers currently parked on a long poll.",
		}, queueLabels),
		matches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_task_matches_total",
			Help: "Tasks dispatched, by whether a poller was already waiting. sync/(sync+async) is the single best health signal for dispatch: it falls when workers are saturated or absent.",
		}, append(append([]string{}, queueLabels...), "match")),
		tasksDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_tasks_dropped_total",
			Help: "Task references refused or discarded by matching. A dropped task is delayed, not lost: it is recovered by the startup scan or by a schedule-to-start timeout.",
		}, append(append([]string{}, queueLabels...), "reason")),
		pollTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_poll_timeouts_total",
			Help: "Long polls that expired with no work. Expected and healthy when workers outnumber tasks; it is the ratio against skald_task_matches_total that matters.",
		}, queueLabels),

		appendDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "skald_history_append_duration_seconds",
			Help:    "Latency of the operations that append history, measured at the API boundary.",
			Buckets: appendBuckets,
		}),
		appendEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "skald_history_append_events",
			Help:    "Events written per appending operation.",
			Buckets: eventBuckets,
		}),
		historyEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_history_events_total",
			Help: "History events written, by namespace.",
		}, []string{KeyNamespace}),

		workflowCompletions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_workflow_completions_total",
			Help: "Workflow executions that reached a terminal state, by status.",
		}, []string{KeyNamespace, KeyStatus}),
		activityAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_activity_attempts_total",
			Help: "Activity attempts handed to a worker. Retries increment this, so attempts/results above one is the retry amplification of a deployment.",
		}, []string{KeyNamespace, "activity_type"}),
		activityResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skald_activity_results_total",
			Help: "Activity attempts that reported a result, by outcome.",
		}, []string{KeyNamespace, "outcome"}),
	}

	for _, c := range m.collectors() {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("telemetry: register metrics: %w", err)
		}
	}
	return m, nil
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.requests, m.requestErrors, m.requestDuration, m.pollDuration, m.inFlight,
		m.backlog, m.pollers, m.matches, m.tasksDropped, m.pollTimeouts,
		m.appendDuration, m.appendEvents, m.historyEvents,
		m.workflowCompletions, m.activityAttempts, m.activityResults,
	}
}

// RequestStarted marks an operation as in flight and returns the function that
// records its outcome. Pairing the two in one call makes it impossible to
// increment the gauge without a matching decrement.
func (m *Metrics) RequestStarted(operation string) func(code string, longPoll bool, d time.Duration) {
	if m == nil {
		return func(string, bool, time.Duration) {}
	}
	m.inFlight.WithLabelValues(operation).Inc()
	return func(code string, longPoll bool, d time.Duration) {
		m.inFlight.WithLabelValues(operation).Dec()
		m.ObserveRequest(operation, code, longPoll, d)
	}
}

// ObserveRequest records one completed operation.
//
// code is an api error code, or "ok". longPoll overrides the static
// classification for endpoints that can be either.
func (m *Metrics) ObserveRequest(operation, code string, longPoll bool, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(operation, code).Inc()
	if code != CodeOK {
		m.requestErrors.WithLabelValues(operation, code).Inc()
	}
	if longPoll || longPollOperations[operation] {
		m.pollDuration.WithLabelValues(operation).Observe(d.Seconds())
		return
	}
	m.requestDuration.WithLabelValues(operation).Observe(d.Seconds())
}

// ObserveHistoryAppend records the latency of one history-appending operation.
//
// It is measured at the API boundary rather than around the store call, so it
// includes decode, validation and state rebuild. That is the number an operator
// actually needs: "how long does it take Skald to make a change durable" is a
// question about the whole path, and a store-only timer would look healthy
// while a cold state cache made every write take a second.
func (m *Metrics) ObserveHistoryAppend(namespace string, d time.Duration) {
	if m == nil {
		return
	}
	m.appendDuration.Observe(d.Seconds())
}

// ObserveHistoryEvents records how many events one append wrote.
func (m *Metrics) ObserveHistoryEvents(namespace string, events int) {
	if m == nil || events <= 0 {
		return
	}
	m.appendEvents.Observe(float64(events))
	m.historyEvents.WithLabelValues(labelOrUnresolved(namespace)).Add(float64(events))
}

// ObserveWorkflowCompletion records a terminal workflow status.
func (m *Metrics) ObserveWorkflowCompletion(namespace, status string) {
	if m == nil {
		return
	}
	m.workflowCompletions.WithLabelValues(labelOrUnresolved(namespace), status).Inc()
}

// ObserveActivityAttempt records that an attempt was handed to a worker.
func (m *Metrics) ObserveActivityAttempt(namespace, activityType string) {
	if m == nil {
		return
	}
	m.activityAttempts.WithLabelValues(labelOrUnresolved(namespace), labelOrUnresolved(activityType)).Inc()
}

// ObserveActivityResult records the outcome an attempt reported.
func (m *Metrics) ObserveActivityResult(namespace, outcome string) {
	if m == nil {
		return
	}
	m.activityResults.WithLabelValues(labelOrUnresolved(namespace), outcome).Inc()
}

// ObservePollTimeout records a long poll that expired with no work.
func (m *Metrics) ObservePollTimeout(namespace, taskQueue string, kind matching.Kind) {
	if m == nil {
		return
	}
	m.pollTimeouts.WithLabelValues(labelOrUnresolved(namespace), labelOrUnresolved(taskQueue), kind.String()).Inc()
}

// labelOrUnresolved keeps an empty label value out of the metric.
//
// An empty label is legal in Prometheus and reads identically to "the metric
// was never emitted with this dimension", which is exactly the confusion you do
// not want at 3am. A visible placeholder says "we did emit this, we just did
// not know the value yet".
func labelOrUnresolved(v string) string {
	if v == "" {
		return unresolvedLabel
	}
	return v
}

// MatchingMetrics adapts Metrics to the matching package's callback interface.
//
// internal/matching deliberately does not import Prometheus -- it defines its
// own tiny interface so that the queue stays a data structure and the metrics
// library stays next to the registry. This is the adapter that closes the gap.
func (m *Metrics) MatchingMetrics() matching.Metrics {
	if m == nil {
		return matching.NopMetrics{}
	}
	return matchingAdapter{m: m}
}

type matchingAdapter struct{ m *Metrics }

var _ matching.Metrics = matchingAdapter{}

func (a matchingAdapter) TaskAdded(key matching.QueueKey, sync bool) {
	match := matchAsync
	if sync {
		match = matchSync
	}
	a.m.matches.WithLabelValues(labelOrUnresolved(key.Namespace), labelOrUnresolved(key.TaskQueue), key.Kind.String(), match).Inc()
}

func (a matchingAdapter) TaskDropped(key matching.QueueKey, reason string) {
	a.m.tasksDropped.WithLabelValues(labelOrUnresolved(key.Namespace), labelOrUnresolved(key.TaskQueue), key.Kind.String(), reason).Inc()
}

func (a matchingAdapter) BacklogDepth(key matching.QueueKey, depth int) {
	a.m.backlog.WithLabelValues(labelOrUnresolved(key.Namespace), labelOrUnresolved(key.TaskQueue), key.Kind.String()).Set(float64(depth))
}

func (a matchingAdapter) PollerCount(key matching.QueueKey, n int) {
	a.m.pollers.WithLabelValues(labelOrUnresolved(key.Namespace), labelOrUnresolved(key.TaskQueue), key.Kind.String()).Set(float64(n))
}
