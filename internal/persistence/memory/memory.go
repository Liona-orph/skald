// Package memory implements the persistence contract entirely in process.
//
// It has three jobs: backing the unit tests of every other package, backing the
// deterministic simulator, and backing the single-binary mode where losing
// state on exit is the expected behaviour. Two of those three are about finding
// bugs, which is why this driver is deliberately unhelpful in one specific way:
// it serialises every event on the way in and deserialises it on the way out,
// exactly as a real database would. A caller that mutates a record it read back
// therefore cannot accidentally mutate stored state and hide its own bug -- the
// class of failure that only ever reproduces against the real store.
//
// The cost is an allocation per event per read. That is the right trade for a
// driver whose purpose is fidelity; deployments that need throughput use
// SQLite.
package memory

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Query limits. They are duplicated in every driver rather than shared, because
// they are a property of a driver's storage engine and not of the contract; the
// two happen to agree today and are free to diverge.
const (
	defaultPageSize  = 100
	maxPageSize      = 1000
	defaultDueLimit  = 1000
	maxDueTimerLimit = 10000
)

// reuseTerminationReason is written into the WorkflowExecutionTerminated event
// the store synthesises for ReuseTerminateIfRunning. The engine is not involved
// -- the policy is resolved inside the same critical section that starts the
// replacement run, because a window where neither run is open would let a third
// starter slip in and win.
const reuseTerminationReason = "terminated to start a new run under the terminate-if-running id reuse policy"

// ErrInjectedFault marks an error the fault injector produced. Simulator
// assertions match on it to tell "the store misbehaved on purpose" from "the
// store has a bug".
var ErrInjectedFault = errors.New("memory: injected fault")

// FaultConfig makes the driver fail on purpose so that retry and recovery paths
// are exercised instead of merely written.
//
// Every draw comes from a seeded, store-private RNG rather than the global one:
// a simulation that cannot be replayed byte for byte from its seed is not a
// simulation, it is a flaky test.
type FaultConfig struct {
	// Seed fixes the fault sequence. Runs with the same seed and the same call
	// order fail identically.
	Seed int64
	// ConflictRate is the probability in [0,1) that a write is rejected with
	// ErrVersionConflict without touching state. It models a competing engine
	// replica, so callers must already handle it.
	ConflictRate float64
	// ErrorRate is the probability in [0,1) that any call fails with a
	// transient, retryable error of the kind a network database produces.
	ErrorRate float64
	// LatencyFn returns the delay to add before an operation runs, given the
	// operation name. A nil function means no added latency.
	LatencyFn func(op string) time.Duration
}

// enabled reports whether the config asks for anything at all, so that the
// zero-value store pays nothing for the feature.
func (c FaultConfig) enabled() bool {
	return c.ConflictRate > 0 || c.ErrorRate > 0 || c.LatencyFn != nil
}

// transientFaults are the failure modes a real store exhibits under load. The
// messages matter: an operator reading a simulation log should recognise them,
// and a caller inspecting the error should conclude "retry", not "give up".
var transientFaults = []string{
	"write i/o timeout",
	"connection reset by peer",
	"deadlock detected, transaction rolled back",
	"too many connections",
}

// Option configures a Store.
type Option func(*Store)

// WithFaults enables fault injection.
func WithFaults(cfg FaultConfig) Option {
	return func(s *Store) {
		s.faults = cfg
		s.rng = rand.New(rand.NewSource(cfg.Seed))
	}
}

// runKey identifies one stored run.
type runKey struct {
	namespace  string
	workflowID string
	runID      string
}

func (k runKey) String() string { return k.namespace + "/" + k.workflowID + "/" + k.runID }

// workflowKey identifies the chain of runs sharing a workflow ID.
type workflowKey struct {
	namespace  string
	workflowID string
}

// dedupKey identifies a start request. Scoping the request ID to the workflow
// ID rather than to the namespace means a client that reuses a request ID for a
// different workflow gets an honest rejection instead of somebody else's run.
type dedupKey struct {
	namespace  string
	workflowID string
	requestID  string
}

// run is one stored execution.
//
// Events are held encoded. That is what makes every read a deep copy without a
// hand-written clone for each of the twenty-five attribute types -- and, more
// importantly, without a clone that silently stops being deep when a new
// pointer field is added to one of them.
type run struct {
	rec       persistence.ExecutionRecord
	events    [][]byte
	lastEvent time.Time
}

// Store is an in-process persistence.Store.
type Store struct {
	faults FaultConfig
	// rngMu guards rng. It is separate from mu so that a latency or fault draw
	// never has to wait behind a write.
	rngMu sync.Mutex
	rng   *rand.Rand

	// mu guards every field below.
	//
	// One lock for the whole store, not one per execution. The critical
	// sections here are map lookups and slice appends -- hundreds of
	// nanoseconds -- so a per-execution lock would trade real contention for a
	// lock object per run, a lifetime question nobody wants to answer (when is
	// it safe to free the lock for a closed run?) and a second acquisition on
	// the read path. The engine already serialises work per execution above
	// this layer, so the contention this lock could remove mostly does not
	// exist. If profiling ever says otherwise the answer is a fixed number of
	// shards keyed by run, not a lock per key.
	mu         sync.RWMutex
	closed     bool
	runs       map[runKey]*run
	current    map[workflowKey]runKey
	dedup      map[dedupKey]runKey
	timers     *timerIndex
	timersByID map[runKey]map[persistence.TimerKey]struct{}
}

var _ persistence.Store = (*Store)(nil)

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{
		runs:       make(map[runKey]*run),
		current:    make(map[workflowKey]runKey),
		dedup:      make(map[dedupKey]runKey),
		timers:     newTimerIndex(),
		timersByID: make(map[runKey]map[persistence.TimerKey]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ---------------------------------------------------------------------------
// Preconditions and fault injection
// ---------------------------------------------------------------------------

// faultKind says whether an operation can plausibly lose a version race.
type faultKind int

const (
	faultRead faultKind = iota
	faultWrite
)

// begin runs the checks every method shares: cancellation first, then injected
// latency, then injected failure. Faults are drawn before the lock is taken so
// that a "slow store" is slow for its caller and not for everyone else, which
// is what a network round trip actually looks like.
func (s *Store) begin(ctx context.Context, op string, kind faultKind) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: %s: %w", op, err)
	}
	if !s.faults.enabled() {
		return nil
	}
	if s.faults.LatencyFn != nil {
		if d := s.faults.LatencyFn(op); d > 0 {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return fmt.Errorf("memory: %s: %w", op, ctx.Err())
			case <-timer.C:
			}
		}
	}
	return s.drawFault(op, kind)
}

func (s *Store) drawFault(op string, kind faultKind) error {
	if s.rng == nil {
		return nil
	}
	s.rngMu.Lock()
	conflict := kind == faultWrite && s.faults.ConflictRate > 0 && s.rng.Float64() < s.faults.ConflictRate
	fail := s.faults.ErrorRate > 0 && s.rng.Float64() < s.faults.ErrorRate
	which := 0
	if fail {
		which = s.rng.Intn(len(transientFaults))
	}
	s.rngMu.Unlock()

	switch {
	case conflict:
		return fmt.Errorf("memory: %s: %w: %w", op, ErrInjectedFault, persistence.ErrVersionConflict)
	case fail:
		return fmt.Errorf("memory: %s: %w: %s", op, ErrInjectedFault, transientFaults[which])
	}
	return nil
}

func closedErr(op string) error { return fmt.Errorf("memory: %s: %w", op, persistence.ErrClosed) }

func notFoundErr(op string, k runKey) error {
	return fmt.Errorf("memory: %s: %s: %w", op, k, persistence.ErrNotFound)
}

// ---------------------------------------------------------------------------
// CreateExecution
// ---------------------------------------------------------------------------

// createPlan is a validated create, held apart from the store so that every
// failure a create can produce is raised before a single map is touched.
//
// Splitting validation from application is what lets a create ride inside an
// append: the append's own mutations happen between the two halves, and both
// halves together still commit or fail as a unit.
type createPlan struct {
	rec       persistence.ExecutionRecord
	blobs     [][]byte
	timers    []persistence.TimerRecord
	lastEvent time.Time
	key       runKey
	wfKey     workflowKey
	requestID string
	reuse     persistence.IDReusePolicy
}

// planCreate does everything a create can do without seeing the store.
func planCreate(op string, req persistence.CreateExecutionRequest) (createPlan, error) {
	rec, err := validateCreate(req)
	if err != nil {
		return createPlan{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	// Encoding is pure CPU on caller-owned data, so it happens before the lock.
	blobs, err := encodeEvents(req.Events)
	if err != nil {
		return createPlan{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	timers, err := validateTimers(rec.Namespace, rec.WorkflowID, rec.RunID, req.Timers)
	if err != nil {
		return createPlan{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	return createPlan{
		rec:       rec,
		blobs:     blobs,
		timers:    timers,
		lastEvent: req.Events[len(req.Events)-1].Time.UTC(),
		key:       runKey{rec.Namespace, rec.WorkflowID, rec.RunID},
		wfKey:     workflowKey{rec.Namespace, rec.WorkflowID},
		requestID: req.RequestID,
		reuse:     req.ReusePolicy,
	}, nil
}

// resolveCreateLocked decides what a planned create means against the current
// contents of the store.
//
// It returns a non-nil record when the create must be skipped -- a deduplicated
// retry, whose original run is the answer -- and an error when it must not
// happen at all. It mutates nothing except a ReuseTerminateIfRunning
// displacement, which is why the successor path forbids that policy.
//
// closing names a run this call is about to close but has not closed yet, so
// that a successor created inside an append does not trip over a predecessor
// that is still open in the map.
func (s *Store) resolveCreateLocked(op string, p createPlan, closing *runKey) (*persistence.ExecutionRecord, error) {
	if p.requestID != "" {
		if prev, ok := s.dedup[dedupKey{p.rec.Namespace, p.rec.WorkflowID, p.requestID}]; ok {
			// A retried start is not an error and must not create a second run:
			// StartWorkflow is safe to retry blindly precisely because of this
			// branch.
			if original, ok := s.runs[prev]; ok {
				rec := cloneRecord(original.rec)
				return &rec, nil
			}
		}
	}
	if _, exists := s.runs[p.key]; exists {
		return nil, fmt.Errorf("memory: %s: run %s: %w", op, p.key, persistence.ErrAlreadyStarted)
	}

	cur, ok := s.current[p.wfKey]
	if !ok {
		return nil, nil
	}
	prev := s.runs[cur]
	// A run this call is closing counts as closed for the reuse check, because
	// by the time the create lands it will be.
	prevOpen := prev.rec.Open() && (closing == nil || cur != *closing)
	switch p.reuse {
	case persistence.ReuseRejectDuplicate:
		return nil, fmt.Errorf(
			"memory: %s: workflow %q already ran as %s: %w", op, p.rec.WorkflowID, prev.rec.RunID, persistence.ErrAlreadyStarted)
	case persistence.ReuseAllowDuplicateFailedOnly:
		if prevOpen || prev.rec.Status == skald.StatusCompleted || prev.rec.Status == skald.StatusContinuedAsNew {
			return nil, fmt.Errorf(
				"memory: %s: workflow %q last run %s is %s: %w", op, p.rec.WorkflowID, prev.rec.RunID, prev.rec.Status, persistence.ErrAlreadyStarted)
		}
	case persistence.ReuseTerminateIfRunning:
		if prevOpen {
			if err := s.terminate(prev, p.rec.StartedAt); err != nil {
				return nil, fmt.Errorf("memory: %s: %w", op, err)
			}
		}
	default: // ReuseAllowDuplicate
		if prevOpen {
			return nil, fmt.Errorf(
				"memory: %s: workflow %q is still running as %s: %w", op, p.rec.WorkflowID, prev.rec.RunID, persistence.ErrAlreadyStarted)
		}
	}
	return nil, nil
}

// applyCreateLocked writes a resolved plan. It cannot fail, which is what makes
// the enclosing transaction atomic.
func (s *Store) applyCreateLocked(p createPlan) persistence.ExecutionRecord {
	r := &run{rec: p.rec, events: p.blobs, lastEvent: p.lastEvent}
	s.runs[p.key] = r
	if cur, ok := s.current[p.wfKey]; !ok || newer(p.rec, s.runs[cur].rec) {
		s.current[p.wfKey] = p.key
	}
	if p.requestID != "" {
		s.dedup[dedupKey{p.rec.Namespace, p.rec.WorkflowID, p.requestID}] = p.key
	}
	for _, tr := range p.timers {
		s.upsertTimer(p.key, tr)
	}
	return cloneRecord(p.rec)
}

// CreateExecution implements persistence.Store.
func (s *Store) CreateExecution(ctx context.Context, req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	const op = "CreateExecution"
	if err := s.begin(ctx, op, faultWrite); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	plan, err := planCreate(op, req)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ExecutionRecord{}, closedErr(op)
	}
	existing, err := s.resolveCreateLocked(op, plan, nil)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	if existing != nil {
		return *existing, nil
	}
	return s.applyCreateLocked(plan), nil
}

// terminate closes an open run in place so that a replacement can take its
// workflow ID. The synthesised event keeps the history valid -- a run whose row
// says TERMINATED but whose history has no terminal event would fail every
// replay that touched it afterwards.
func (s *Store) terminate(r *run, at time.Time) error {
	at = at.UTC()
	if at.Before(r.lastEvent) {
		// History time must never go backwards. The replacement run may have
		// been stamped by a different clock, so clamp rather than trust it.
		at = r.lastEvent
	}
	ev := history.Event{
		ID:   r.rec.LastEventID + 1,
		Time: at,
		Attrs: history.WorkflowExecutionTerminatedAttributes{
			Reason:   reuseTerminationReason,
			Identity: "skald-store",
		},
	}
	blob, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("encode termination event for run %s: %w", r.rec.RunID, err)
	}
	r.events = append(r.events, blob)
	r.lastEvent = at
	r.rec.LastEventID = ev.ID
	r.rec.Status = skald.StatusTerminated
	r.rec.ClosedAt = at
	r.rec.Version++
	s.dropRunTimers(runKey{r.rec.Namespace, r.rec.WorkflowID, r.rec.RunID})
	return nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// GetExecution implements persistence.Store.
func (s *Store) GetExecution(ctx context.Context, namespace, workflowID, runID string) (persistence.ExecutionRecord, error) {
	const op = "GetExecution"
	if err := s.begin(ctx, op, faultRead); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return persistence.ExecutionRecord{}, closedErr(op)
	}
	r, err := s.lookup(op, namespace, workflowID, runID)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	return cloneRecord(r.rec), nil
}

// ReadHistory implements persistence.Store.
func (s *Store) ReadHistory(ctx context.Context, namespace, workflowID, runID string, fromEventID, toEventID int64) (history.History, error) {
	const op = "ReadHistory"
	if err := s.begin(ctx, op, faultRead); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, closedErr(op)
	}
	r, err := s.lookup(op, namespace, workflowID, runID)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	blobs := selectRange(r.events, fromEventID, toEventID)
	// Decoding allocates and can be slow for a long history, so it happens
	// after the lock is dropped. The slice header is a snapshot of an
	// append-only slice, and the blobs themselves are never mutated in place,
	// so reading it unlocked is safe.
	s.mu.RUnlock()

	if len(blobs) == 0 {
		return nil, nil
	}
	out := make(history.History, len(blobs))
	for i, b := range blobs {
		if err := json.Unmarshal(b, &out[i]); err != nil {
			return nil, fmt.Errorf("memory: %s: run %s/%s/%s: decode event %d: %w", op, namespace, workflowID, runID, i, err)
		}
	}
	return out, nil
}

// selectRange clamps [from, to] to the stored events and returns the matching
// slice. Out of range is empty, not an error: a poller asking for events past
// the end is asking a perfectly reasonable question.
func selectRange(events [][]byte, from, to int64) [][]byte {
	last := int64(len(events))
	if from < 1 {
		from = 1
	}
	if to <= 0 || to > last {
		to = last
	}
	if from > last || to < from {
		return nil
	}
	return events[from-1 : to]
}

// ---------------------------------------------------------------------------
// AppendHistory
// ---------------------------------------------------------------------------

// AppendHistory implements persistence.Store.
func (s *Store) AppendHistory(ctx context.Context, req persistence.AppendHistoryRequest) (persistence.ExecutionRecord, error) {
	const op = "AppendHistory"
	if err := s.begin(ctx, op, faultWrite); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	blobs, err := encodeEvents(req.Events)
	if err != nil {
		return persistence.ExecutionRecord{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	successor, err := planSuccessor(op, req)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ExecutionRecord{}, closedErr(op)
	}
	r, err := s.lookup(op, req.Namespace, req.WorkflowID, req.RunID)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	if r.rec.Version != req.ExpectedVersion {
		// Nothing has been touched at this point, which is the whole contract:
		// a losing writer must be able to re-read and retry against a store
		// that looks exactly as it did before.
		return persistence.ExecutionRecord{}, fmt.Errorf(
			"memory: %s: run %s is at version %d, caller expected %d: %w",
			op, r.rec.RunID, r.rec.Version, req.ExpectedVersion, persistence.ErrVersionConflict)
	}
	if err := validateAppend(r, req.Events); err != nil {
		return persistence.ExecutionRecord{}, fmt.Errorf("memory: %s: run %s: %w", op, r.rec.RunID, err)
	}
	key := runKey{r.rec.Namespace, r.rec.WorkflowID, r.rec.RunID}
	timers, err := validateTimers(key.namespace, key.workflowID, key.runID, req.UpsertTimers)
	if err != nil {
		return persistence.ExecutionRecord{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	for _, k := range req.DeleteTimers {
		if err := checkTimerKey(key.namespace, key.workflowID, key.runID, k); err != nil {
			return persistence.ExecutionRecord{}, fmt.Errorf("memory: %s: delete timer: %w", op, err)
		}
	}
	// The successor is resolved against the state this append is about to
	// produce, and resolved *before* any of it is applied: a successor that
	// could not be created must leave the predecessor's events unwritten too.
	skipSuccessor := false
	if successor != nil {
		existing, err := s.resolveCreateLocked(op, *successor, &key)
		if err != nil {
			return persistence.ExecutionRecord{}, err
		}
		skipSuccessor = existing != nil
	}

	// Past this point nothing can fail, which is what makes the write atomic:
	// events, execution row, timer index and successor move together or not at
	// all.
	r.events = append(r.events, blobs...)
	if n := len(req.Events); n > 0 {
		r.lastEvent = req.Events[n-1].Time.UTC()
		r.rec.LastEventID = req.Events[n-1].ID
	}
	r.rec.Version++
	applyMutableFields(&r.rec, req.Record, r.lastEvent)

	for _, k := range req.DeleteTimers {
		s.deleteTimer(key, k)
	}
	for _, tr := range timers {
		s.upsertTimer(key, tr)
	}
	if !r.rec.Open() {
		// A closed run cannot be woken again; leaving its timers behind would
		// hand the timer service work it must then learn to ignore.
		s.dropRunTimers(key)
	}
	if successor != nil && !skipSuccessor {
		s.applyCreateLocked(*successor)
	}
	return cloneRecord(r.rec), nil
}

// planSuccessor validates the create an append carries, if any.
//
// The reuse policy is deliberately overridden rather than honoured: a successor
// follows the run this append closes, so "may this workflow ID start again"
// has already been answered by the predecessor existing. See the contract on
// persistence.AppendHistoryRequest.
func planSuccessor(op string, req persistence.AppendHistoryRequest) (*createPlan, error) {
	if req.CreateSuccessor == nil {
		return nil, nil
	}
	sub := *req.CreateSuccessor
	sub.ReusePolicy = persistence.ReuseAllowDuplicate
	if sub.Record.Namespace != req.Namespace || sub.Record.WorkflowID != req.WorkflowID {
		return nil, fmt.Errorf("memory: %s: successor %s/%s does not continue %s/%s: %w",
			op, sub.Record.Namespace, sub.Record.WorkflowID, req.Namespace, req.WorkflowID,
			skald.ErrInvalidIdentifier)
	}
	plan, err := planCreate(op, sub)
	if err != nil {
		return nil, err
	}
	return &plan, nil
}

// applyMutableFields copies the fields an append is allowed to change. The
// identity of a run -- namespace, IDs, type, task queue, start time -- is fixed
// at creation, so taking it from the request would let a buggy caller rewrite
// history metadata that other rows already reference.
func applyMutableFields(dst *persistence.ExecutionRecord, src persistence.ExecutionRecord, lastEvent time.Time) {
	if !dst.Status.Terminal() {
		// Terminal is absorbing. A caller replaying an old request must not be
		// able to reopen a finished execution.
		dst.Status = src.Status
	}
	switch {
	case !dst.Status.Terminal():
		dst.ClosedAt = time.Time{}
	case !src.ClosedAt.IsZero():
		dst.ClosedAt = src.ClosedAt.UTC()
	case dst.ClosedAt.IsZero():
		dst.ClosedAt = lastEvent
	}
	// A nil map means "unchanged"; an empty one means "cleared".
	if src.Memo != nil {
		dst.Memo = cloneAttrs(src.Memo)
	}
	if src.SearchAttrs != nil {
		dst.SearchAttrs = cloneAttrs(src.SearchAttrs)
	}
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

// ListExecutions implements persistence.Store.
//
// The memory driver answers visibility queries by scanning and sorting. That is
// O(runs) per page where SQLite is O(page) on an index, and it is the right
// trade here: this driver holds a test's worth of executions, and a scan cannot
// disagree with the filter the way a hand-tuned index plan can.
func (s *Store) ListExecutions(ctx context.Context, filter persistence.ListFilter) (persistence.ListResult, error) {
	const op = "ListExecutions"
	if err := s.begin(ctx, op, faultRead); err != nil {
		return persistence.ListResult{}, err
	}
	after, err := decodePageToken(filter)
	if err != nil {
		return persistence.ListResult{}, fmt.Errorf("memory: %s: %w", op, err)
	}
	size := clampPageSize(filter.PageSize)

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return persistence.ListResult{}, closedErr(op)
	}
	matched := make([]persistence.ExecutionRecord, 0, len(s.runs))
	for _, r := range s.runs {
		if matches(r.rec, filter) {
			matched = append(matched, cloneRecord(r.rec))
		}
	}
	s.mu.RUnlock()

	slices.SortFunc(matched, byRecency)
	if after != nil {
		// Keyset, not offset: the cursor names a row, so a page is unaffected
		// by rows inserted or removed ahead of it.
		idx, _ := slices.BinarySearchFunc(matched, *after, byRecency)
		for idx < len(matched) && byRecency(matched[idx], *after) <= 0 {
			idx++
		}
		matched = matched[idx:]
	}

	var res persistence.ListResult
	if len(matched) > size {
		matched = matched[:size]
		res.NextPageToken = encodePageToken(filter, matched[len(matched)-1])
	}
	res.Records = matched
	return res, nil
}

// OpenExecutions implements persistence.Store.
func (s *Store) OpenExecutions(ctx context.Context, namespace string, fn func(persistence.ExecutionRecord) error) error {
	const op = "OpenExecutions"
	if err := s.begin(ctx, op, faultRead); err != nil {
		return err
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return closedErr(op)
	}
	open := make([]persistence.ExecutionRecord, 0, len(s.runs))
	for _, r := range s.runs {
		if r.rec.Open() && (namespace == "" || r.rec.Namespace == namespace) {
			open = append(open, cloneRecord(r.rec))
		}
	}
	s.mu.RUnlock()
	// The callback runs with no lock held. It belongs to the engine and will
	// call back into the store to read history; holding a non-reentrant lock
	// across it would deadlock the recovery scan it exists to serve.
	slices.SortFunc(open, byRecency)
	for _, rec := range open {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("memory: %s: %w", op, err)
		}
		if err := fn(rec); err != nil {
			return fmt.Errorf("memory: %s: run %s: %w", op, rec.RunID, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// DueTimers implements persistence.Store.
func (s *Store) DueTimers(ctx context.Context, now time.Time, limit int) ([]persistence.TimerRecord, error) {
	const op = "DueTimers"
	if err := s.begin(ctx, op, faultRead); err != nil {
		return nil, err
	}
	switch {
	case limit <= 0:
		limit = defaultDueLimit
	case limit > maxDueTimerLimit:
		limit = maxDueTimerLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, closedErr(op)
	}
	return s.timers.due(now.UTC(), limit), nil
}

// DeleteTimers implements persistence.Store.
func (s *Store) DeleteTimers(ctx context.Context, keys []persistence.TimerKey) error {
	const op = "DeleteTimers"
	if err := s.begin(ctx, op, faultWrite); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return closedErr(op)
	}
	for _, k := range keys {
		// Deleting a timer that is already gone is not an error: the engine
		// deletes each timer as it processes it and may be replaying a batch it
		// crashed halfway through.
		s.deleteTimer(runKey{k.Namespace, k.WorkflowID, k.RunID}, k)
	}
	return nil
}

// Close implements persistence.Store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.runs = nil
	s.current = nil
	s.dedup = nil
	s.timers = newTimerIndex()
	s.timersByID = nil
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers. All of these run with s.mu held.
// ---------------------------------------------------------------------------

// lookup resolves a run, treating an empty run ID as "the current run".
func (s *Store) lookup(op, namespace, workflowID, runID string) (*run, error) {
	key := runKey{namespace, workflowID, runID}
	if runID == "" {
		cur, ok := s.current[workflowKey{namespace, workflowID}]
		if !ok {
			return nil, notFoundErr(op, key)
		}
		key = cur
	}
	r, ok := s.runs[key]
	if !ok {
		return nil, notFoundErr(op, key)
	}
	return r, nil
}

func (s *Store) upsertTimer(key runKey, tr persistence.TimerRecord) {
	s.timers.upsert(tr)
	set, ok := s.timersByID[key]
	if !ok {
		set = make(map[persistence.TimerKey]struct{})
		s.timersByID[key] = set
	}
	set[tr.TimerKey] = struct{}{}
}

func (s *Store) deleteTimer(key runKey, tk persistence.TimerKey) {
	s.timers.remove(tk)
	if set, ok := s.timersByID[key]; ok {
		delete(set, tk)
		if len(set) == 0 {
			delete(s.timersByID, key)
		}
	}
}

// dropRunTimers retires every timer belonging to a run. The reverse index makes
// this O(timers of the run) instead of a scan of the whole due index.
func (s *Store) dropRunTimers(key runKey) {
	for tk := range s.timersByID[key] {
		s.timers.remove(tk)
	}
	delete(s.timersByID, key)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// validateCreate checks the request and returns the normalised row to store.
func validateCreate(req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	rec := req.Record
	switch {
	case rec.Namespace == "":
		return rec, fmt.Errorf("%w: namespace is empty", skald.ErrInvalidIdentifier)
	case rec.WorkflowID == "":
		return rec, fmt.Errorf("%w: workflow id is empty", skald.ErrInvalidIdentifier)
	case rec.RunID == "":
		return rec, fmt.Errorf("%w: run id is empty", skald.ErrInvalidIdentifier)
	case len(req.Events) == 0:
		return rec, fmt.Errorf("%w: a new run needs at least its %s event",
			history.ErrInvalidHistory, history.EventTypeWorkflowExecutionStarted)
	}
	// A run is created once, so full validation here costs nothing measurable
	// and rejects a malformed history at the only point where the caller still
	// has the context to fix it.
	if err := req.Events.Validate(); err != nil {
		return rec, err
	}
	if req.Events[0].ID != 1 {
		return rec, fmt.Errorf("%w: a new run must start at event 1, got %d", history.ErrInvalidHistory, req.Events[0].ID)
	}

	rec = normalizeRecord(rec)
	rec.Version = 1
	rec.LastEventID = req.Events.LastEventID()
	if rec.FirstExecutionRunID == "" {
		rec.FirstExecutionRunID = rec.RunID
	}
	if rec.StartedAt.IsZero() {
		rec.StartedAt = req.Events[0].Time.UTC()
	}
	if rec.Status.Terminal() && rec.ClosedAt.IsZero() {
		rec.ClosedAt = req.Events[len(req.Events)-1].Time.UTC()
	}
	if !rec.Status.Terminal() {
		rec.ClosedAt = time.Time{}
	}
	return rec, nil
}

// validateAppend checks that the batch is the next contiguous piece of the run.
//
// The version check has already proved the caller is not racing anyone, so a
// gap here is a caller bug rather than contention, and it is reported as the
// history-structure violation it is.
func validateAppend(r *run, events history.History) error {
	if len(events) == 0 {
		return nil
	}
	if !r.rec.Open() {
		return fmt.Errorf("%w: cannot append to a %s execution", history.ErrInvalidHistory, r.rec.Status)
	}
	want := r.rec.LastEventID + 1
	prev := r.lastEvent
	for i, ev := range events {
		if ev.ID != want+int64(i) {
			return fmt.Errorf("%w: event at index %d has ID %d, expected %d (IDs must be dense)",
				history.ErrInvalidHistory, i, ev.ID, want+int64(i))
		}
		if ev.Attrs == nil {
			return fmt.Errorf("%w: event %d has nil attributes", history.ErrInvalidHistory, ev.ID)
		}
		if ev.Time.IsZero() {
			return fmt.Errorf("%w: event %d has a zero timestamp", history.ErrInvalidHistory, ev.ID)
		}
		if ev.Time.Before(prev) {
			return fmt.Errorf("%w: event %d at %s precedes the previous event at %s",
				history.ErrInvalidHistory, ev.ID, ev.Time.UTC(), prev)
		}
		prev = ev.Time.UTC()
	}
	return nil
}

// validateTimers checks that every entry belongs to the run being written and
// returns them with their timestamps normalised.
func validateTimers(namespace, workflowID, runID string, in []persistence.TimerRecord) ([]persistence.TimerRecord, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]persistence.TimerRecord, len(in))
	for i, tr := range in {
		if err := checkTimerKey(namespace, workflowID, runID, tr.TimerKey); err != nil {
			return nil, err
		}
		if tr.FireAt.IsZero() {
			return nil, fmt.Errorf("%w: timer %d of run %s has no fire time",
				skald.ErrInvalidIdentifier, tr.EventID, runID)
		}
		tr.FireAt = tr.FireAt.UTC()
		out[i] = tr
	}
	return out, nil
}

// checkTimerKey rejects a timer aimed at a different run. Letting one slip
// through would put an entry in the due index that no execution owns, and the
// timer service would spin on it forever.
func checkTimerKey(namespace, workflowID, runID string, k persistence.TimerKey) error {
	if k.Namespace != namespace || k.WorkflowID != workflowID || k.RunID != runID {
		return fmt.Errorf("%w: timer %s/%s/%s does not belong to run %s/%s/%s",
			skald.ErrInvalidIdentifier, k.Namespace, k.WorkflowID, k.RunID, namespace, workflowID, runID)
	}
	if k.EventID <= 0 {
		return fmt.Errorf("%w: timer of run %s has event id %d", skald.ErrInvalidIdentifier, runID, k.EventID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Copying and ordering
// ---------------------------------------------------------------------------

// cloneRecord returns a record that shares nothing with the stored one. Every
// non-map field is a value, so cloning the two maps is the whole job -- but it
// is the job that has to be right, because a caller that appends to a returned
// Memo would otherwise be writing to the store.
func cloneRecord(rec persistence.ExecutionRecord) persistence.ExecutionRecord {
	rec.Memo = cloneAttrs(rec.Memo)
	rec.SearchAttrs = cloneAttrs(rec.SearchAttrs)
	return rec
}

// cloneAttrs normalises an empty map to nil so that a round trip through any
// driver -- including ones that store absence as NULL -- compares equal.
func cloneAttrs(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

func normalizeRecord(rec persistence.ExecutionRecord) persistence.ExecutionRecord {
	rec.StartedAt = rec.StartedAt.UTC()
	rec.ClosedAt = rec.ClosedAt.UTC()
	return cloneRecord(rec)
}

func encodeEvents(events history.History) ([][]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(events))
	for i, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("encode event %d (%s): %w", ev.ID, ev.Type(), err)
		}
		out[i] = b
	}
	return out, nil
}

// newer reports whether a is the more recent of two runs of the same workflow
// ID, using the same total order visibility pages by. One comparator for both
// keeps "the current run" and "the first row of an unfiltered list" the same
// thing, which is what an operator expects when they look at both.
func newer(a, b persistence.ExecutionRecord) bool { return byRecency(a, b) < 0 }

// byRecency orders visibility results newest first, breaking ties on run ID so
// that the order is total. Without a total order, keyset pagination can repeat
// or skip rows that share a start time.
func byRecency(a, b persistence.ExecutionRecord) int {
	switch {
	case a.StartedAt.After(b.StartedAt):
		return -1
	case a.StartedAt.Before(b.StartedAt):
		return 1
	case a.RunID > b.RunID:
		return -1
	case a.RunID < b.RunID:
		return 1
	}
	return 0
}

func matches(rec persistence.ExecutionRecord, f persistence.ListFilter) bool {
	switch {
	case f.Namespace != "" && rec.Namespace != f.Namespace:
		return false
	case f.WorkflowID != "" && rec.WorkflowID != f.WorkflowID:
		return false
	case f.WorkflowType != "" && rec.WorkflowType != f.WorkflowType:
		return false
	case f.Status != nil && rec.Status != *f.Status:
		return false
	case !f.StartedAfter.IsZero() && rec.StartedAt.Before(f.StartedAfter):
		return false
	}
	return true
}

func clampPageSize(n int) int {
	switch {
	case n <= 0:
		return defaultPageSize
	case n > maxPageSize:
		return maxPageSize
	}
	return n
}

// ---------------------------------------------------------------------------
// Page tokens
// ---------------------------------------------------------------------------

// pageToken names the last row of a page.
//
// It also carries a fingerprint of the filter that produced it. Without one, a
// caller that changes a filter but keeps the token gets a page from the middle
// of a result set it never asked for -- a bug that looks like data loss and is
// almost impossible to reproduce.
type pageToken struct {
	StartedAt int64  `json:"s"`
	RunID     string `json:"r"`
	Filter    uint64 `json:"f"`
}

func filterFingerprint(f persistence.ListFilter) uint64 {
	h := fnv.New64a()
	status := "any"
	if f.Status != nil {
		status = f.Status.String()
	}
	// The separator cannot appear in an identifier, so distinct filters cannot
	// collide by concatenation.
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%d",
		f.Namespace, f.WorkflowID, f.WorkflowType, status, f.StartedAfter.UnixNano())
	return h.Sum64()
}

func encodePageToken(f persistence.ListFilter, last persistence.ExecutionRecord) string {
	b, err := json.Marshal(pageToken{
		StartedAt: last.StartedAt.UnixNano(),
		RunID:     last.RunID,
		Filter:    filterFingerprint(f),
	})
	if err != nil {
		// pageToken is three scalars; json.Marshal cannot fail on it. Returning
		// an empty token degrades to "no more pages", which is wrong but safe.
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodePageToken returns the exclusive lower bound of the requested page, or
// nil for the first page.
func decodePageToken(f persistence.ListFilter) (*persistence.ExecutionRecord, error) {
	if f.PageToken == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(f.PageToken)
	if err != nil {
		return nil, fmt.Errorf("page token is not valid base64: %w", err)
	}
	var tok pageToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("page token is malformed: %w", err)
	}
	if tok.Filter != filterFingerprint(f) {
		return nil, errors.New("page token belongs to a different query")
	}
	return &persistence.ExecutionRecord{
		StartedAt: time.Unix(0, tok.StartedAt).UTC(),
		RunID:     tok.RunID,
	}, nil
}

// ---------------------------------------------------------------------------
// Timer index
// ---------------------------------------------------------------------------

// timerEntry is one element of the due-time heap. index is maintained by
// container/heap so that an upsert or delete can reach its element in O(1) and
// repair the heap in O(log n).
type timerEntry struct {
	rec   persistence.TimerRecord
	index int
}

// timerIndex is a min-heap of pending timers keyed by fire time, plus a map
// from timer key to heap element.
//
// A sorted slice would make DueTimers trivial and every upsert O(n); a plain
// map would make upsert trivial and DueTimers a full scan. "Which timers are
// due" is asked once per tick forever and "a timer moved" happens on every
// activity retry, so both have to be cheap, and a heap is the smallest
// structure that makes them so. It is not a sorted structure -- a heap only
// promises the minimum -- which is exactly enough, because the question is
// always about the earliest few.
type timerIndex struct {
	h     timerHeap
	byKey map[persistence.TimerKey]*timerEntry
}

func newTimerIndex() *timerIndex {
	return &timerIndex{byKey: make(map[persistence.TimerKey]*timerEntry)}
}

func (ti *timerIndex) upsert(rec persistence.TimerRecord) {
	if e, ok := ti.byKey[rec.TimerKey]; ok {
		e.rec = rec
		heap.Fix(&ti.h, e.index)
		return
	}
	e := &timerEntry{rec: rec}
	heap.Push(&ti.h, e)
	ti.byKey[rec.TimerKey] = e
}

func (ti *timerIndex) remove(key persistence.TimerKey) {
	e, ok := ti.byKey[key]
	if !ok {
		return
	}
	heap.Remove(&ti.h, e.index)
	delete(ti.byKey, key)
}

func (ti *timerIndex) len() int { return len(ti.h) }

// due returns up to limit timers with FireAt at or before now, earliest first.
//
// It walks the heap instead of sorting it. Because a node is never later than
// its children, the earliest entries can be found by expanding a frontier that
// starts at the root: the smallest frontier element is a lower bound on every
// node not yet visited, so the walk emits in order and can stop the moment that
// bound passes now. The cost is O(k log k) for k results, independent of how
// many timers are pending, and the index is left untouched -- the engine
// deletes what it processes, and DueTimers is not that decision.
func (ti *timerIndex) due(now time.Time, limit int) []persistence.TimerRecord {
	if len(ti.h) == 0 || limit <= 0 {
		return nil
	}
	frontier := &frontierHeap{nodes: ti.h, idx: []int{0}}
	var out []persistence.TimerRecord
	for len(out) < limit && frontier.Len() > 0 {
		i := heap.Pop(frontier).(int)
		e := ti.h[i]
		if e.rec.FireAt.After(now) {
			break
		}
		out = append(out, e.rec)
		for _, child := range [2]int{2*i + 1, 2*i + 2} {
			if child < len(ti.h) {
				heap.Push(frontier, child)
			}
		}
	}
	return out
}

// timerLess is the total order the heap is built on. Fire time first, then the
// key, so that timers sharing a millisecond still come back in a stable order
// and a test that asserts on the sequence is not asserting on map iteration.
func timerLess(a, b *timerEntry) bool {
	if !a.rec.FireAt.Equal(b.rec.FireAt) {
		return a.rec.FireAt.Before(b.rec.FireAt)
	}
	if c := strings.Compare(a.rec.Namespace, b.rec.Namespace); c != 0 {
		return c < 0
	}
	if c := strings.Compare(a.rec.WorkflowID, b.rec.WorkflowID); c != 0 {
		return c < 0
	}
	if c := strings.Compare(a.rec.RunID, b.rec.RunID); c != 0 {
		return c < 0
	}
	if a.rec.EventID != b.rec.EventID {
		return a.rec.EventID < b.rec.EventID
	}
	return a.rec.Kind < b.rec.Kind
}

type timerHeap []*timerEntry

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return timerLess(h[i], h[j]) }

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(x any) {
	e := x.(*timerEntry)
	e.index = len(*h)
	*h = append(*h, e)
}

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // release the reference so a retired timer can be collected
	*h = old[:n-1]
	e.index = -1
	return e
}

// frontierHeap orders indices into a timerHeap. It never mutates the heap it
// reads, which is why DueTimers can run under a read lock.
type frontierHeap struct {
	nodes timerHeap
	idx   []int
}

func (f *frontierHeap) Len() int           { return len(f.idx) }
func (f *frontierHeap) Less(i, j int) bool { return timerLess(f.nodes[f.idx[i]], f.nodes[f.idx[j]]) }
func (f *frontierHeap) Swap(i, j int)      { f.idx[i], f.idx[j] = f.idx[j], f.idx[i] }
func (f *frontierHeap) Push(x any)         { f.idx = append(f.idx, x.(int)) }

func (f *frontierHeap) Pop() any {
	old := f.idx
	n := len(old)
	v := old[n-1]
	f.idx = old[:n-1]
	return v
}

// Stats reports what the store is holding. It exists for the simulator's
// invariant checks and for tests that need to prove a resource was released
// rather than merely hidden.
type Stats struct {
	Runs   int
	Events int
	Timers int
}

// Stats returns a snapshot of the store's size.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{Runs: len(s.runs), Timers: s.timers.len()}
	for _, r := range s.runs {
		st.Events += len(r.events)
	}
	return st
}

// String makes a store printable in test failure output.
func (s *Store) String() string {
	st := s.Stats()
	return "memory.Store{runs=" + strconv.Itoa(st.Runs) +
		" events=" + strconv.Itoa(st.Events) +
		" timers=" + strconv.Itoa(st.Timers) + "}"
}
