// Package persistence defines the storage contract the engine depends on and
// the errors that contract can produce.
//
// Two rules shape the interface.
//
// First, the history is the only durable truth. Task queues are *derived*:
// after a restart the engine re-materialises pending workflow and activity
// tasks by scanning open executions, so a lost in-memory queue costs latency
// and never correctness. That choice removes an entire consistency problem --
// keeping a queue and a state machine in agreement -- at the price of a
// recovery scan, which is documented in docs/adr/0004-derived-task-queues.md.
//
// Second, every write is conditional. Callers pass the version they read; the
// store rejects the write if it changed. Two engine replicas can therefore
// process the same execution concurrently and exactly one wins, with no lease,
// no lock service and no split-brain window.
package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Sentinel errors. Callers match on these with errors.Is; drivers wrap them
// with driver-specific context.
var (
	// ErrNotFound means the requested execution or run does not exist.
	ErrNotFound = errors.New("persistence: not found")
	// ErrVersionConflict means another writer advanced the execution first.
	// It is expected under contention and callers retry by re-reading.
	ErrVersionConflict = errors.New("persistence: version conflict")
	// ErrAlreadyStarted means an open execution with the same workflow ID
	// already exists and the reuse policy forbids a second one.
	ErrAlreadyStarted = errors.New("persistence: workflow already started")
	// ErrClosed means the store has been shut down.
	ErrClosed = errors.New("persistence: store closed")
)

// ExecutionRecord is the row describing one run. It holds only what queries and
// concurrency control need; the authoritative detail lives in the history.
type ExecutionRecord struct {
	Namespace    string
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       skald.WorkflowStatus
	StartedAt    time.Time
	ClosedAt     time.Time
	// Version is the optimistic concurrency token, incremented on every write.
	Version int64
	// LastEventID is the ID of the newest persisted event, used to detect a
	// torn write between the execution row and the event log.
	LastEventID int64
	// FirstExecutionRunID chains continue-as-new runs together.
	FirstExecutionRunID string
	Memo                map[string]string
	SearchAttrs         map[string]string
}

// Open reports whether the run can still accept events.
func (r *ExecutionRecord) Open() bool { return r != nil && !r.Status.Terminal() }

// CreateExecutionRequest starts a new run.
type CreateExecutionRequest struct {
	Record ExecutionRecord
	Events history.History
	Timers []TimerRecord
	// RequestID deduplicates a retried start. A second create with the same
	// request ID returns the original run instead of an error, which makes
	// StartWorkflow safe to retry blindly.
	RequestID string
	// ReusePolicy decides what happens when a run with the same workflow ID
	// exists.
	ReusePolicy IDReusePolicy
}

// IDReusePolicy controls workflow ID collisions.
type IDReusePolicy int32

const (
	// ReuseAllowDuplicate permits a new run once the previous one closed, in
	// any state. This is the default and matches "workflow ID is a natural key
	// for the thing, not for the attempt".
	ReuseAllowDuplicate IDReusePolicy = iota
	// ReuseAllowDuplicateFailedOnly permits a new run only if the previous one
	// did not complete successfully, which makes a workflow ID an idempotency
	// key for a business operation.
	ReuseAllowDuplicateFailedOnly
	// ReuseRejectDuplicate permits exactly one run per workflow ID, ever.
	ReuseRejectDuplicate
	// ReuseTerminateIfRunning terminates an open run and starts a fresh one.
	ReuseTerminateIfRunning
)

// AppendHistoryRequest advances an existing run.
//
// It is the single mutating operation of the engine's hot path and it is
// transactional: the events, the updated execution row, the timer index and --
// when the run hands off to a successor -- the successor's whole first write
// all commit together or not at all.
type AppendHistoryRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
	// ExpectedVersion is the version the caller read. The write fails with
	// ErrVersionConflict if the stored version differs.
	ExpectedVersion int64
	Events          history.History
	// Record carries the post-write values of the mutable execution fields.
	Record ExecutionRecord
	// UpsertTimers and DeleteTimers keep the due-time index in step with the
	// pending timers implied by the new events.
	UpsertTimers []TimerRecord
	DeleteTimers []TimerKey
	// CreateSuccessor, when non-nil, opens the next run of the same workflow ID
	// inside this transaction.
	//
	// It exists for exactly one reason: continue-as-new and workflow-level retry
	// close one run and open another, and doing that as two writes leaves a
	// window in which a crash strands the whole logical workflow -- the
	// predecessor says CONTINUED_AS_NEW, the successor named by its final event
	// does not exist, and nothing is running. Making the pair atomic removes the
	// window rather than shrinking it.
	//
	// The contract a driver must implement:
	//
	//   - The successor is created after this append's own mutations, so the
	//     reuse check sees the predecessor already closed.
	//   - RequestID deduplication applies exactly as it does to CreateExecution:
	//     if a run already exists for that request ID, the successor is *not*
	//     created a second time and the append still commits. That is what makes
	//     a retried close-and-continue safe.
	//   - ReusePolicy is ignored and ReuseAllowDuplicate is assumed. A successor
	//     is by construction the continuation of the run being closed, so the
	//     only precondition is that no open run of the workflow ID survives the
	//     append.
	//   - Any other failure -- the run ID already exists, an open run remains --
	//     fails the whole call, leaving neither the events nor the successor.
	//
	// A driver that cannot honour it must return an error rather than silently
	// dropping the field: a successor that is quietly not created is the exact
	// bug this field exists to prevent.
	CreateSuccessor *CreateExecutionRequest
}

// TimerKey identifies one durable timer.
type TimerKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
	// EventID is the ID of the TimerStarted event, or the ActivityTaskScheduled
	// event for a retry backoff.
	EventID int64
	Kind    TimerKind
}

// TimerKind distinguishes the reasons the engine wants to be woken.
type TimerKind int32

const (
	// TimerKindUser is a durable sleep requested by workflow code.
	TimerKindUser TimerKind = iota
	// TimerKindActivityRetry fires when an activity's backoff elapses.
	TimerKindActivityRetry
	// TimerKindActivityTimeout fires when an activity misses a deadline.
	TimerKindActivityTimeout
	// TimerKindWorkflowTaskTimeout fires when a worker holds a workflow task
	// past its deadline.
	TimerKindWorkflowTaskTimeout
	// TimerKindExecutionTimeout fires when a run or execution deadline passes.
	TimerKindExecutionTimeout
	// TimerKindWorkflowRetry fires when a failed workflow's backoff elapses.
	TimerKindWorkflowRetry
)

// TimerRecord is one entry in the due-time index.
//
// The index is a materialised view of state that could be recomputed by
// scanning every open execution. It exists because "which timers are due in the
// next second" must be answerable in O(log n), and a full scan is O(open
// executions) -- fine at a thousand, fatal at ten million.
type TimerRecord struct {
	TimerKey
	FireAt time.Time
	// Attempt is carried so a stale retry timer can be recognised and dropped
	// after the activity moved on.
	Attempt int32
	// TaskQueue lets the timer service dispatch without a state read for the
	// common activity-retry case.
	TaskQueue string
}

// ListFilter narrows a visibility query.
type ListFilter struct {
	Namespace    string
	WorkflowID   string
	WorkflowType string
	Status       *skald.WorkflowStatus
	StartedAfter time.Time
	// PageSize is clamped by the driver; zero means the driver default.
	PageSize  int
	PageToken string
}

// ListResult is one page of visibility records.
type ListResult struct {
	Records       []ExecutionRecord
	NextPageToken string
}

// Store is the durable state interface.
//
// Implementations must be safe for concurrent use. Every method must respect
// ctx cancellation, because the frontend's long polls hold contexts open for
// tens of seconds and a driver that ignores them leaks goroutines under load.
type Store interface {
	// CreateExecution starts a run, honouring the ID reuse policy. It returns
	// the run that ended up current, which for a deduplicated retry is the
	// original.
	CreateExecution(ctx context.Context, req CreateExecutionRequest) (ExecutionRecord, error)

	// GetExecution returns one run. An empty runID means "the current run of
	// this workflow ID".
	GetExecution(ctx context.Context, namespace, workflowID, runID string) (ExecutionRecord, error)

	// ReadHistory returns events in [fromEventID, toEventID]. A toEventID of 0
	// means "to the end". Events are returned in ID order.
	ReadHistory(ctx context.Context, namespace, workflowID, runID string, fromEventID, toEventID int64) (history.History, error)

	// AppendHistory advances a run under optimistic concurrency control.
	AppendHistory(ctx context.Context, req AppendHistoryRequest) (ExecutionRecord, error)

	// ListExecutions answers visibility queries.
	ListExecutions(ctx context.Context, filter ListFilter) (ListResult, error)

	// DueTimers returns timers whose FireAt is at or before now, oldest first.
	// The engine deletes each one as it processes it; a timer that is returned
	// twice because processing crashed in between must be idempotent to
	// re-apply, which the state machine guarantees.
	DueTimers(ctx context.Context, now time.Time, limit int) ([]TimerRecord, error)

	// DeleteTimers removes entries from the due-time index.
	DeleteTimers(ctx context.Context, keys []TimerKey) error

	// OpenExecutions streams every run that is not in a terminal state. The
	// engine calls it once at startup to rebuild derived task queues.
	OpenExecutions(ctx context.Context, namespace string, fn func(ExecutionRecord) error) error

	// Close releases driver resources.
	Close() error
}
