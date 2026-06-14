package engine_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// fakeStore is a deliberately small, deliberately strict in-memory
// persistence.Store written for these tests alone.
//
// It is not the memory driver: it exists so that the engine's tests depend on
// the *contract* rather than on another implementation's behaviour, and so that
// they can inject failures -- a version conflict, an unavailable store -- that a
// real driver would never produce on demand.
//
// Strictness is the point. Every append re-validates the whole history, so a
// bug that produces an unreplayable event sequence fails here, in the test that
// caused it, rather than days later in a recovery path.
type fakeStore struct {
	mu       sync.Mutex
	runs     map[string]*fakeRun
	current  map[string]string
	requests map[string]string
	timers   map[persistence.TimerKey]persistence.TimerRecord
	closed   bool

	// beforeAppend runs before an append is applied and can fail it or mutate
	// the store, which is how the version-conflict path is exercised.
	beforeAppend func(req persistence.AppendHistoryRequest, s *fakeStore) error

	appends int
}

type fakeRun struct {
	rec    persistence.ExecutionRecord
	events history.History
}

var _ persistence.Store = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	return &fakeStore{
		runs:     map[string]*fakeRun{},
		current:  map[string]string{},
		requests: map[string]string{},
		timers:   map[persistence.TimerKey]persistence.TimerRecord{},
	}
}

func runKey(namespace, workflowID, runID string) string {
	return namespace + "|" + workflowID + "|" + runID
}

func workflowKey(namespace, workflowID string) string {
	return namespace + "|" + workflowID
}

func (s *fakeStore) CreateExecution(_ context.Context, req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ExecutionRecord{}, persistence.ErrClosed
	}
	return s.createLocked(req)
}

// createLocked is shared by CreateExecution and by the successor branch of
// AppendHistory, mirroring the real drivers: a successor must be created by
// exactly the same rules as an ordinary start, or the fake would hide a
// divergence the drivers do not have.
func (s *fakeStore) createLocked(req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	r := req.Record

	if req.RequestID != "" {
		if runID, ok := s.requests[runKey(r.Namespace, r.WorkflowID, req.RequestID)]; ok {
			return s.runs[runKey(r.Namespace, r.WorkflowID, runID)].rec, nil
		}
	}

	if currentID, ok := s.current[workflowKey(r.Namespace, r.WorkflowID)]; ok {
		current := s.runs[runKey(r.Namespace, r.WorkflowID, currentID)]
		switch {
		case current.rec.Open():
			return persistence.ExecutionRecord{}, fmt.Errorf("%w: %s is still running as %s",
				persistence.ErrAlreadyStarted, r.WorkflowID, currentID)
		case req.ReusePolicy == persistence.ReuseRejectDuplicate:
			return persistence.ExecutionRecord{}, fmt.Errorf("%w: %s may only run once",
				persistence.ErrAlreadyStarted, r.WorkflowID)
		case req.ReusePolicy == persistence.ReuseAllowDuplicateFailedOnly &&
			current.rec.Status == skald.StatusCompleted:
			return persistence.ExecutionRecord{}, fmt.Errorf("%w: %s already completed successfully",
				persistence.ErrAlreadyStarted, r.WorkflowID)
		}
	}

	if err := req.Events.Validate(); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	r.Version = 1
	r.LastEventID = req.Events.LastEventID()
	run := &fakeRun{rec: r, events: append(history.History(nil), req.Events...)}
	s.runs[runKey(r.Namespace, r.WorkflowID, r.RunID)] = run
	s.current[workflowKey(r.Namespace, r.WorkflowID)] = r.RunID
	if req.RequestID != "" {
		s.requests[runKey(r.Namespace, r.WorkflowID, req.RequestID)] = r.RunID
	}
	for _, t := range req.Timers {
		s.timers[t.TimerKey] = t
	}
	return r, nil
}

func (s *fakeStore) GetExecution(_ context.Context, namespace, workflowID, runID string) (persistence.ExecutionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ExecutionRecord{}, persistence.ErrClosed
	}
	run, err := s.lookupLocked(namespace, workflowID, runID)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	return run.rec, nil
}

func (s *fakeStore) lookupLocked(namespace, workflowID, runID string) (*fakeRun, error) {
	if runID == "" {
		id, ok := s.current[workflowKey(namespace, workflowID)]
		if !ok {
			return nil, fmt.Errorf("%w: %s/%s", persistence.ErrNotFound, namespace, workflowID)
		}
		runID = id
	}
	run, ok := s.runs[runKey(namespace, workflowID, runID)]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s/%s", persistence.ErrNotFound, namespace, workflowID, runID)
	}
	return run, nil
}

func (s *fakeStore) ReadHistory(_ context.Context, namespace, workflowID, runID string, from, to int64) (history.History, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, persistence.ErrClosed
	}
	run, err := s.lookupLocked(namespace, workflowID, runID)
	if err != nil {
		return nil, err
	}
	if from <= 0 {
		from = 1
	}
	out := make(history.History, 0, len(run.events))
	for _, ev := range run.events {
		if ev.ID < from || (to > 0 && ev.ID > to) {
			continue
		}
		out = append(out, ev)
	}
	return out, nil
}

func (s *fakeStore) AppendHistory(_ context.Context, req persistence.AppendHistoryRequest) (persistence.ExecutionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ExecutionRecord{}, persistence.ErrClosed
	}
	if s.beforeAppend != nil {
		hook := s.beforeAppend
		s.mu.Unlock()
		err := hook(req, s)
		s.mu.Lock()
		if err != nil {
			return persistence.ExecutionRecord{}, err
		}
	}
	run, err := s.lookupLocked(req.Namespace, req.WorkflowID, req.RunID)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	if run.rec.Version != req.ExpectedVersion {
		return persistence.ExecutionRecord{}, fmt.Errorf("%w: %s is at version %d, caller read %d",
			persistence.ErrVersionConflict, req.RunID, run.rec.Version, req.ExpectedVersion)
	}
	// Terminal is absorbing, and events must be contiguous. Both rules exist in
	// the real drivers; enforcing them here means the engine's tests fail on a
	// violation instead of a driver's tests failing later.
	if run.rec.Status.Terminal() && len(req.Events) > 0 {
		return persistence.ExecutionRecord{}, fmt.Errorf("fakestore: %s is %s and cannot accept events",
			req.RunID, run.rec.Status)
	}
	if len(req.Events) > 0 && req.Events[0].ID != run.rec.LastEventID+1 {
		return persistence.ExecutionRecord{}, fmt.Errorf(
			"fakestore: append starts at event %d, want %d", req.Events[0].ID, run.rec.LastEventID+1)
	}
	s.appends++

	merged := append(append(history.History(nil), run.events...), req.Events...)
	if err := merged.Validate(); err != nil {
		return persistence.ExecutionRecord{}, fmt.Errorf("fakestore: refusing an invalid history: %w", err)
	}
	run.events = merged

	rec := req.Record
	rec.Version = run.rec.Version + 1
	rec.LastEventID = merged.LastEventID()
	run.rec = rec

	for _, k := range req.DeleteTimers {
		delete(s.timers, k)
	}
	for _, t := range req.UpsertTimers {
		s.timers[t.TimerKey] = t
	}
	if !rec.Open() {
		// A closed run cannot be woken again, so the drivers retire its whole
		// timer set. Mirroring that here keeps the engine from relying on a
		// timer that would not survive in production -- a mistake that is
		// invisible against a permissive fake.
		for key := range s.timers {
			if key.Namespace == rec.Namespace && key.WorkflowID == rec.WorkflowID && key.RunID == rec.RunID {
				delete(s.timers, key)
			}
		}
	}
	if req.CreateSuccessor != nil {
		// Created after this append's own mutations, so the reuse check sees
		// the predecessor already closed. Any failure here fails the whole
		// call, which is exactly the atomicity the field exists to provide.
		sub := *req.CreateSuccessor
		sub.ReusePolicy = persistence.ReuseAllowDuplicate
		if _, err := s.createLocked(sub); err != nil {
			return persistence.ExecutionRecord{}, fmt.Errorf(
				"fakestore: successor of %s: %w", rec.RunID, err)
		}
	}
	return rec, nil
}

func (s *fakeStore) ListExecutions(_ context.Context, filter persistence.ListFilter) (persistence.ListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ListResult{}, persistence.ErrClosed
	}
	var out []persistence.ExecutionRecord
	for _, run := range s.runs {
		rec := run.rec
		switch {
		case filter.Namespace != "" && rec.Namespace != filter.Namespace,
			filter.WorkflowID != "" && rec.WorkflowID != filter.WorkflowID,
			filter.WorkflowType != "" && rec.WorkflowType != filter.WorkflowType,
			filter.Status != nil && rec.Status != *filter.Status:
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].RunID < out[j].RunID
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})

	offset := 0
	if filter.PageToken != "" {
		n, err := strconv.Atoi(filter.PageToken)
		if err != nil {
			return persistence.ListResult{}, fmt.Errorf("fakestore: bad page token %q", filter.PageToken)
		}
		offset = n
	}
	if offset > len(out) {
		offset = len(out)
	}
	out = out[offset:]
	res := persistence.ListResult{}
	if filter.PageSize > 0 && len(out) > filter.PageSize {
		out = out[:filter.PageSize]
		res.NextPageToken = strconv.Itoa(offset + filter.PageSize)
	}
	res.Records = out
	return res, nil
}

func (s *fakeStore) DueTimers(_ context.Context, now time.Time, limit int) ([]persistence.TimerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, persistence.ErrClosed
	}
	var due []persistence.TimerRecord
	for _, t := range s.timers {
		if !t.FireAt.After(now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].FireAt.Equal(due[j].FireAt) {
			if due[i].EventID == due[j].EventID {
				return due[i].Kind < due[j].Kind
			}
			return due[i].EventID < due[j].EventID
		}
		return due[i].FireAt.Before(due[j].FireAt)
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (s *fakeStore) DeleteTimers(_ context.Context, keys []persistence.TimerKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return persistence.ErrClosed
	}
	for _, k := range keys {
		delete(s.timers, k)
	}
	return nil
}

func (s *fakeStore) OpenExecutions(_ context.Context, namespace string, fn func(persistence.ExecutionRecord) error) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return persistence.ErrClosed
	}
	var open []persistence.ExecutionRecord
	for _, run := range s.runs {
		if namespace != "" && run.rec.Namespace != namespace {
			continue
		}
		if run.rec.Open() {
			open = append(open, run.rec)
		}
	}
	s.mu.Unlock()

	sort.Slice(open, func(i, j int) bool { return open[i].RunID < open[j].RunID })
	for _, rec := range open {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// ---------------------------------------------------------------------------
// Test-only inspection
// ---------------------------------------------------------------------------

func (s *fakeStore) history(namespace, workflowID, runID string) history.History {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, err := s.lookupLocked(namespace, workflowID, runID)
	if err != nil {
		return nil
	}
	return append(history.History(nil), run.events...)
}

func (s *fakeStore) record(namespace, workflowID, runID string) (persistence.ExecutionRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, err := s.lookupLocked(namespace, workflowID, runID)
	if err != nil {
		return persistence.ExecutionRecord{}, false
	}
	return run.rec, true
}

func (s *fakeStore) timerRecords() []persistence.TimerRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]persistence.TimerRecord, 0, len(s.timers))
	for _, t := range s.timers {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FireAt.Equal(out[j].FireAt) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].FireAt.Before(out[j].FireAt)
	})
	return out
}

func (s *fakeStore) dueTimerCount(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.timers {
		if !t.FireAt.After(now) {
			n++
		}
	}
	return n
}

// nextTimerDeadline returns the earliest armed fire time, or the zero time when
// the index is empty.
func (s *fakeStore) nextTimerDeadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best time.Time
	for _, t := range s.timers {
		if best.IsZero() || t.FireAt.Before(best) {
			best = t.FireAt
		}
	}
	return best
}

func (s *fakeStore) appendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appends
}

func (s *fakeStore) setBeforeAppend(fn func(persistence.AppendHistoryRequest, *fakeStore) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeAppend = fn
}

// bumpVersion simulates a competing writer committing between another caller's
// read and its write.
func (s *fakeStore) bumpVersion(namespace, workflowID, runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run, err := s.lookupLocked(namespace, workflowID, runID); err == nil {
		run.rec.Version++
	}
}
