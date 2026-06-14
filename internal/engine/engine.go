// Package engine implements api.Service on top of a persistence.Store, the
// matching layer and the durable timer service.
//
// # The shape of every mutating operation
//
// There is exactly one write path, and every operation is a special case of it:
//
//  1. Take the per-execution lock.
//  2. Load the execution row, then rebuild MutableState by replaying history.
//  3. Apply a state-machine transition, which appends events and returns
//     effects.
//  4. Write the new events *under the version that was read*.
//  5. Only after that write commits, apply the effects: dispatch tasks to
//     matching and create successor runs.
//
// Step 5 comes last on purpose. An engine that enqueues a task before the write
// commits produces a duplicate every time the write fails, and a duplicate
// activity execution is the exact failure durable execution exists to prevent.
// Ordering it the other way round -- write, then dispatch -- can only lose a
// dispatch, and a lost dispatch is recovered by the timer index or by the
// startup scan. One direction of failure is a bug report; the other is a
// refund.
//
// Timers are the one effect that is *not* deferred to step 5: they are written
// inside the same store transaction as the events that imply them, because a
// timer that exists only in memory after the commit is a workflow that never
// wakes up.
//
// # Concurrency
//
// Operations on the same workflow ID serialise on a striped mutex; operations
// on different workflow IDs proceed in parallel. Two engine *replicas* need no
// coordination at all: they race on the store's compare-and-set and exactly one
// wins, with the loser reloading and retrying. The lock is a latency
// optimisation within a process, never a correctness mechanism.
//
// # Known gaps
//
// Three things are deliberately incomplete, and each is called out where it
// bites rather than hidden:
//
//   - RespondWorkflowTaskCompleted carries no started event id, so a response
//     from a worker whose task was replaced can only be detected through the
//     identity recorded on the started event. Adding the field to the request
//     would make the check exact; see checkTaskOwnership.
//   - The successor of a continue-as-new or a workflow retry is created inside
//     the transaction that closes its predecessor, through
//     persistence.AppendHistoryRequest.CreateSuccessor. postCommit keeps a
//     non-atomic fallback for the paths that reach it without a plan; it should
//     never fire, and it logs loudly when it does. See successorPlan.
//   - CronSchedule is carried faithfully through starts, retries and
//     continue-as-new, but no run is scheduled from it: cron expression parsing
//     is not implemented.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/skald-io/skald/internal/clock"
	"github.com/skald-io/skald/internal/execution"
	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/internal/timers"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"

	"github.com/google/uuid"
)

// lockStripes is the size of the per-execution lock table.
//
// A stripe is a sync.Mutex, so the whole table is 8 KiB: permanently resident,
// no allocation, no bookkeeping to delete an entry when an execution goes idle.
// With S stripes and C workflow IDs being written concurrently, the chance that
// any two collide is about C^2/2S -- a few percent at a hundred concurrent
// executions -- and a collision costs only the serialisation of two
// transactions that were each going to make a store round trip anyway. The
// alternative, a map of mutexes keyed by execution, buys a slightly lower
// collision rate in exchange for an allocation, a global map lock on the hot
// path and a lifetime problem, which is a bad trade at any size.
const lockStripes = 1 << 10

// Defaults for Config.
const (
	// DefaultMaxWriteAttempts bounds the optimistic-concurrency retry loop. A
	// conflict means another writer advanced the execution, so a retry is
	// cheap and almost always succeeds on the second try; a caller that
	// consistently loses five races is contending with something pathological
	// and deserves an error rather than an unbounded loop.
	DefaultMaxWriteAttempts = 5
	// DefaultStateCacheSize bounds the number of rebuilt executions kept in
	// memory. See the cache documentation for what it is and is not for.
	DefaultStateCacheSize = 4096
	// DefaultHistoryPollInterval is how often a GetHistory long poll re-checks
	// the store when no local write woke it. It exists because a write on
	// *another* replica cannot signal this process's waiters.
	DefaultHistoryPollInterval = time.Second
	// DefaultRedispatchInterval is how long a dispatched activity retry may sit
	// in matching before the engine dispatches it again. It doubles as the
	// schedule-to-start watchdog for attempts after the first.
	DefaultRedispatchInterval = time.Minute
)

// Config parameterises an Engine.
type Config struct {
	// Store is the durable state. Required.
	Store persistence.Store
	// Matcher dispatches tasks to pollers. Created with defaults when nil.
	Matcher *matching.Matcher
	// Clock is the source of time. Defaults to clock.System.
	Clock clock.Clock
	// DefaultNamespace is used when a request omits one.
	DefaultNamespace string
	// NewID generates run IDs and request IDs. Injected so that tests get
	// stable identifiers; defaults to random UUIDs.
	NewID func() string
	// NewSeed draws the per-run randomness seed that the workflow-side RNG is
	// built from. Defaults to a cryptographically random draw.
	NewSeed func() int64
	// MaxWriteAttempts bounds the version-conflict retry loop.
	MaxWriteAttempts int
	// StateCacheSize bounds the rebuilt-state cache.
	StateCacheSize int
	// TimerInterval is the durable timer scan interval.
	TimerInterval time.Duration
	// HistoryPollInterval bounds how stale a GetHistory long poll can be when
	// the write happened on another replica.
	HistoryPollInterval time.Duration
	// RedispatchInterval is the activity retry re-dispatch watchdog.
	RedispatchInterval time.Duration
	// RecoverNamespaces lists the namespaces Recover scans. The default, a
	// single empty string, asks the store for every open execution regardless
	// of namespace; a driver that cannot answer that should be configured with
	// an explicit list.
	RecoverNamespaces []string
	// Logger receives operational events. Defaults to a discarding logger.
	Logger *slog.Logger
}

// Engine is the in-process implementation of api.Service.
type Engine struct {
	store   persistence.Store
	matcher *matching.Matcher
	timers  *timers.Service
	clk     clock.Clock
	log     *slog.Logger

	defaultNS         string
	newID             func() string
	newSeed           func() int64
	maxAttempts       int
	historyPoll       time.Duration
	redispatch        time.Duration
	recoverNamespaces []string
	locks             [lockStripes]sync.Mutex
	cache             *lru.Cache[string, *cachedState]
	notifier          notifier
	ownsMatcher       bool
	closeOnce         sync.Once
	// closed releases waiters that are parked inside the engine itself rather
	// than inside the store or the matcher.
	closed chan struct{}
}

var _ api.Service = (*Engine)(nil)

// New validates cfg and returns a stopped Engine. Call Start to begin
// processing timers and to re-materialise task queues.
func New(cfg Config) (*Engine, error) {
	if cfg.Store == nil {
		return nil, errorf(api.CodeInvalidArgument, "engine: a store is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	if cfg.DefaultNamespace == "" {
		cfg.DefaultNamespace = skald.DefaultNamespace
	}
	if cfg.NewID == nil {
		cfg.NewID = uuid.NewString
	}
	if cfg.NewSeed == nil {
		cfg.NewSeed = cryptoSeed
	}
	if cfg.MaxWriteAttempts <= 0 {
		cfg.MaxWriteAttempts = DefaultMaxWriteAttempts
	}
	if cfg.StateCacheSize <= 0 {
		cfg.StateCacheSize = DefaultStateCacheSize
	}
	if cfg.HistoryPollInterval <= 0 {
		cfg.HistoryPollInterval = DefaultHistoryPollInterval
	}
	if cfg.RedispatchInterval <= 0 {
		cfg.RedispatchInterval = DefaultRedispatchInterval
	}
	if len(cfg.RecoverNamespaces) == 0 {
		cfg.RecoverNamespaces = []string{""}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ownsMatcher := cfg.Matcher == nil
	if ownsMatcher {
		cfg.Matcher = matching.New(matching.Config{Clock: cfg.Clock})
	}
	cache, err := lru.New[string, *cachedState](cfg.StateCacheSize)
	if err != nil {
		return nil, fmt.Errorf("engine: state cache: %w", err)
	}

	e := &Engine{
		store:             cfg.Store,
		matcher:           cfg.Matcher,
		clk:               cfg.Clock,
		log:               cfg.Logger,
		defaultNS:         cfg.DefaultNamespace,
		newID:             cfg.NewID,
		newSeed:           cfg.NewSeed,
		maxAttempts:       cfg.MaxWriteAttempts,
		historyPoll:       cfg.HistoryPollInterval,
		redispatch:        cfg.RedispatchInterval,
		recoverNamespaces: cfg.RecoverNamespaces,
		cache:             cache,
		ownsMatcher:       ownsMatcher,
		closed:            make(chan struct{}),
	}
	e.notifier.init()

	e.timers, err = timers.New(timers.Config{
		Store:    cfg.Store,
		Clock:    cfg.Clock,
		Dispatch: e.onTimer,
		Interval: cfg.TimerInterval,
		Logger:   cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("engine: timer service: %w", err)
	}
	return e, nil
}

// Start recovers derived state and begins firing timers.
func (e *Engine) Start(ctx context.Context) error {
	if err := e.Recover(ctx); err != nil {
		return err
	}
	// The timer loop outlives this call by design: it is bound to the engine's
	// lifetime, not to the context that happened to start it.
	e.timers.Start() //nolint:contextcheck // see Close for the loop's lifetime
	return nil
}

// Close stops background work. It does not close the store: the caller owns
// that, because a store is frequently shared with the visibility reader and the
// CLI in the same process.
func (e *Engine) Close(ctx context.Context) error {
	var err error
	e.closeOnce.Do(func() {
		close(e.closed)
		err = e.timers.Stop(ctx)
		if e.ownsMatcher {
			e.matcher.Close()
		}
		e.notifier.close()
	})
	return err
}

// Matcher exposes the dispatch layer so that a frontend can scrape its metrics.
func (e *Engine) Matcher() *matching.Matcher { return e.matcher }

// ---------------------------------------------------------------------------
// Per-execution locking
// ---------------------------------------------------------------------------

// lockExecution serialises operations on one workflow ID and returns the
// release function.
//
// The unit is the workflow ID rather than the run ID because the runs of one
// workflow ID form a chain -- a retry or a continue-as-new closes one run and
// opens the next -- and two operations that interleave across that boundary can
// observe a workflow with either no current run or two. Runs of *different*
// workflow IDs never interact, so they are free to proceed in parallel.
func (e *Engine) lockExecution(namespace, workflowID string) func() {
	h := fnv.New64a()
	_, _ = io.WriteString(h, namespace)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, workflowID)
	mu := &e.locks[h.Sum64()%lockStripes]
	mu.Lock()
	return mu.Unlock
}

// ---------------------------------------------------------------------------
// The rebuilt-state cache
// ---------------------------------------------------------------------------

// cachedState is one execution's rebuilt view plus the engine-side bookkeeping
// that goes with it.
//
// The cache is not only an optimisation, although it is a large one: without it
// every request replays a whole history. It is also where the small amount of
// engine state that the history *cannot* express lives -- specifically which
// durable timers this engine believes it has armed, so that a transaction can
// compute the difference instead of blindly rewriting the index.
//
// Correctness never depends on a hit. An entry is used only when its version
// still matches the execution row, and it is dropped on every error, so the
// worst case is a reload. That is what makes a cold replica, a second replica
// and a restarted process all behave identically.
type cachedState struct {
	ms  *execution.MutableState
	rec persistence.ExecutionRecord
	// armed is what the engine last wrote to the due-time index for this run.
	// It is empty after a rebuild, which is safe: an unknown timer is re-armed
	// (upsert is idempotent by key) and a stale one is ignored by the handler
	// that receives it.
	armed map[persistence.TimerKey]persistence.TimerRecord
}

func cacheKey(namespace, workflowID, runID string) string {
	return namespace + "|" + workflowID + "|" + runID
}

func (e *Engine) invalidate(namespace, workflowID, runID string) {
	e.cache.Remove(cacheKey(namespace, workflowID, runID))
}

// load returns the rebuilt state for one run. The caller must hold the
// execution lock: the returned state is shared, mutable and unsynchronised.
//
// runID may be empty, meaning "the current run of this workflow ID".
func (e *Engine) load(ctx context.Context, namespace, workflowID, runID string) (*cachedState, error) {
	rec, err := e.store.GetExecution(ctx, namespace, workflowID, runID)
	if err != nil {
		return nil, err
	}
	key := cacheKey(namespace, workflowID, rec.RunID)
	if st, ok := e.cache.Get(key); ok && st.ms.Version == rec.Version {
		st.rec = rec
		return st, nil
	}

	h, err := e.store.ReadHistory(ctx, namespace, workflowID, rec.RunID, 1, 0)
	if err != nil {
		return nil, err
	}
	exec := skald.WorkflowExecution{WorkflowID: workflowID, RunID: rec.RunID}
	ms, err := execution.Rebuild(namespace, exec, h, e.clk.Now)
	if err != nil {
		return nil, err
	}
	ms.Version = rec.Version
	st := &cachedState{ms: ms, rec: rec, armed: map[persistence.TimerKey]persistence.TimerRecord{}}
	e.cache.Add(key, st)
	return st, nil
}

// ---------------------------------------------------------------------------
// The write path
// ---------------------------------------------------------------------------

// outcome is what a transition body reports back to the generic write path.
type outcome struct {
	// effects come straight from the state machine and are applied after the
	// commit.
	effects []execution.Effect
	// timers are index entries the operation wants armed whose schedule cannot
	// be derived from state: an activity retry backoff, or the deferred first
	// workflow task of a run serving a workflow-level retry.
	timers []persistence.TimerRecord
	// drop names index entries the operation wants removed even though the
	// derived set would keep them.
	drop []persistence.TimerKey
	// noop tells the write path that the transition decided there was nothing
	// to do, so no store round trip happens at all.
	noop bool
	// successor is the run that continues this one, created inside the same
	// store transaction that closes it.
	successor *createPlan
}

// transition is the body of a mutating operation. It runs with the execution
// lock held, against freshly loaded state, and may be called more than once if
// the write loses a version race.
type transition func(st *cachedState) (outcome, error)

// mutate runs fn against the named execution under the execution lock.
func (e *Engine) mutate(ctx context.Context, namespace, workflowID, runID string, fn transition) error {
	unlock := e.lockExecution(namespace, workflowID)
	defer unlock()
	return e.mutateLocked(ctx, namespace, workflowID, runID, fn)
}

// mutateLocked is mutate for callers that already hold the execution lock.
func (e *Engine) mutateLocked(ctx context.Context, namespace, workflowID, runID string, fn transition) error {
	var lastConflict error
	for attempt := 1; attempt <= e.maxAttempts; attempt++ {
		if attempt > 1 {
			// Only a *retry* checks the caller's context. The first attempt
			// runs to completion even for a caller that has already gone away,
			// because a poll that has taken a task out of matching must finish
			// starting it or the reference is lost.
			if err := ctx.Err(); err != nil {
				return mapError(err)
			}
		}
		// A conflict is retried immediately, with no backoff. That is not an
		// oversight: a version conflict is proof that another writer *made
		// progress*, so the retry re-reads a state that has genuinely changed
		// and is not a spin against a busy resource. Backing off here would add
		// latency to the common case -- two replicas racing on a hot execution
		// -- to protect against a case that cannot happen, while the bounded
		// attempt count still stops a pathological loop.
		st, err := e.load(ctx, namespace, workflowID, runID)
		if err != nil {
			return mapError(err)
		}
		before := len(st.ms.Events())

		out, err := fn(st)
		if err != nil {
			// The transition may have appended events to the in-memory state
			// before failing, so the entry is no longer a faithful copy of what
			// is durable. Dropping it is always safe and always cheap.
			e.invalidate(namespace, workflowID, st.ms.Execution.RunID)
			return mapError(err)
		}
		if out.noop {
			return nil
		}
		// Effects are prepared before the write because one of them -- naming
		// the successor of a continue-as-new -- edits an event that is about to
		// become immutable.
		extraTimers, remaining, successor, err := e.finalizeEffects(st.ms, out.effects)
		if err != nil {
			e.invalidate(namespace, workflowID, st.ms.Execution.RunID)
			return mapError(err)
		}
		out.timers = append(out.timers, extraTimers...)
		out.effects = remaining
		out.successor = successor

		committed, err := e.commit(ctx, st, before, out)
		switch {
		case errors.Is(err, persistence.ErrVersionConflict):
			e.invalidate(namespace, workflowID, st.ms.Execution.RunID)
			lastConflict = err
			continue
		case err != nil:
			e.invalidate(namespace, workflowID, st.ms.Execution.RunID)
			return mapError(err)
		}
		if committed {
			e.notifier.notify(cacheKey(namespace, workflowID, st.ms.Execution.RunID))
			if out.successor != nil {
				// The successor is durable at this point, so releasing its
				// first workflow task cannot dispatch work that does not exist.
				// Its record is not read back: caching it would cost a round
				// trip to save the first poll a load, and the first poll is
				// about to happen anyway.
				e.postCommit(ctx, out.successor.st, out.successor.effects)
				e.notifier.notify(cacheKey(namespace, workflowID, out.successor.runID))
			}
		}
		e.postCommit(ctx, st, out.effects)
		return nil
	}
	return &api.Error{
		Code:    api.CodeVersionConflict,
		Message: fmt.Sprintf("engine: %s/%s lost %d version races: %v", namespace, workflowID, e.maxAttempts, lastConflict),
		Details: map[string]string{"workflow_id": workflowID, "namespace": namespace},
	}
}

// commit writes the events appended since index `before` together with the
// timer-index changes the new state implies. It reports whether a write
// actually happened.
func (e *Engine) commit(ctx context.Context, st *cachedState, before int, out outcome) (bool, error) {
	events := st.ms.Events()
	newEvents := events[before:]

	want := e.desiredTimers(st, out)
	upserts, deletes := diffTimers(st.armed, want)
	for _, k := range out.drop {
		if _, stillWanted := want[k]; !stillWanted {
			deletes = appendUnique(deletes, k)
		}
	}

	if len(newEvents) == 0 && len(upserts) == 0 && len(deletes) == 0 && out.successor == nil {
		// A genuinely idempotent call: a duplicate cancel, a heartbeat that
		// moved no deadline. Skipping the round trip keeps retries free.
		return false, nil
	}

	rec, err := e.store.AppendHistory(ctx, persistence.AppendHistoryRequest{
		Namespace:       st.ms.Namespace,
		WorkflowID:      st.ms.Execution.WorkflowID,
		RunID:           st.ms.Execution.RunID,
		ExpectedVersion: st.ms.Version,
		Events:          newEvents,
		Record:          e.recordFor(st),
		UpsertTimers:    upserts,
		DeleteTimers:    deletes,
		CreateSuccessor: successorRequest(out.successor),
	})
	if err != nil {
		return false, err
	}

	st.rec = rec
	st.ms.Version = rec.Version
	st.armed = want
	e.cache.Add(cacheKey(st.ms.Namespace, st.ms.Execution.WorkflowID, st.ms.Execution.RunID), st)
	return true, nil
}

// recordFor projects the post-transition state onto the execution row.
func (e *Engine) recordFor(st *cachedState) persistence.ExecutionRecord {
	ms := st.ms
	rec := st.rec
	rec.Namespace = ms.Namespace
	rec.WorkflowID = ms.Execution.WorkflowID
	rec.RunID = ms.Execution.RunID
	rec.WorkflowType = ms.WorkflowType
	rec.TaskQueue = ms.TaskQueue
	rec.Status = ms.Status
	rec.StartedAt = ms.StartedAt
	rec.ClosedAt = ms.ClosedAt
	rec.LastEventID = ms.Events().LastEventID()
	rec.FirstExecutionRunID = ms.FirstExecutionRunID
	rec.Memo = ms.Memo
	rec.SearchAttrs = ms.SearchAttrs
	// The store owns version assignment; carrying the value the write will
	// produce keeps a driver that echoes the record back consistent either way.
	rec.Version = ms.Version + 1
	return rec
}

// postCommit applies the effects the transition returned. Failures here are
// logged, never returned: the write is already durable, so the caller's
// operation succeeded, and every effect is reconstructible from the store by
// the recovery scan or by a timer.
func (e *Engine) postCommit(ctx context.Context, st *cachedState, effects []execution.Effect) {
	ms := st.ms
	for _, eff := range effects {
		switch eff.Kind {
		case execution.EffectDispatchWorkflowTask:
			e.dispatchWorkflowTask(ms, eff)
		case execution.EffectDispatchActivityTask:
			e.dispatchActivityTask(ms, eff)
		case execution.EffectArmTimer:
			// Already durable: timers are written inside the transaction, not
			// after it, so there is nothing left to do here.
		case execution.EffectStartNewRun:
			if eff.NewRun != nil {
				if err := e.startSuccessor(ctx, ms, eff.NewRun); err != nil {
					e.log.Error("successor run creation failed",
						"workflow_id", ms.Execution.WorkflowID, "run_id", ms.Execution.RunID, "error", err)
				}
			}
		}
	}
}

func (e *Engine) dispatchWorkflowTask(ms *execution.MutableState, eff execution.Effect) {
	queue := eff.TaskQueue
	if queue == "" {
		queue = ms.TaskQueue
	}
	task := matching.Task{
		Namespace:        ms.Namespace,
		TaskQueue:        queue,
		Execution:        ms.Execution,
		ScheduledEventID: eff.ScheduledEventID,
		Attempt:          ms.WorkflowTask().Attempt,
		EnqueuedAt:       e.clk.Now(),
	}
	if err := e.matcher.AddWorkflowTask(task); err != nil {
		e.log.Warn("workflow task not dispatched", "execution", ms.Execution.String(),
			"scheduled_event_id", eff.ScheduledEventID, "error", err)
	}
}

// dispatchActivityTask puts an activity attempt in front of the pollers.
//
// The attempt number is read from the state rather than passed in, because the
// reference is the only place a retried attempt's number is visible to the
// poller: the history still shows the first attempt, so a reference that
// claimed attempt 1 would be rejected as stale by the poll path.
func (e *Engine) dispatchActivityTask(ms *execution.MutableState, eff execution.Effect) {
	queue := eff.TaskQueue
	attempt := int32(1)
	if act, ok := ms.Activity(eff.ScheduledEventID); ok {
		attempt = act.Attempt
		if queue == "" {
			queue = act.TaskQueue
		}
	}
	if queue == "" {
		queue = ms.TaskQueue
	}
	task := matching.Task{
		Namespace:        ms.Namespace,
		TaskQueue:        queue,
		Execution:        ms.Execution,
		ScheduledEventID: eff.ScheduledEventID,
		Attempt:          attempt,
		EnqueuedAt:       e.clk.Now(),
	}
	if err := e.matcher.AddActivityTask(task); err != nil {
		e.log.Warn("activity task not dispatched", "execution", ms.Execution.String(),
			"scheduled_event_id", eff.ScheduledEventID, "error", err)
	}
}

// ---------------------------------------------------------------------------
// The durable timer set
// ---------------------------------------------------------------------------

// desiredTimers computes the complete set of index entries this run should have
// after the transition.
//
// Deriving the whole set on every write, rather than emitting incremental edits
// per transition, means a timer can never be orphaned by a code path that
// forgot to disarm it: if the state no longer implies a deadline, the entry is
// deleted, whoever wrote it. Only two kinds cannot be derived from state -- an
// activity retry backoff and a successor-run fallback -- so those are carried
// forward explicitly.
func (e *Engine) desiredTimers(st *cachedState, out outcome) map[persistence.TimerKey]persistence.TimerRecord {
	ms := st.ms
	want := make(map[persistence.TimerKey]persistence.TimerRecord, len(st.armed)+4)
	key := func(eventID int64, kind persistence.TimerKind) persistence.TimerKey {
		return persistence.TimerKey{
			Namespace:  ms.Namespace,
			WorkflowID: ms.Execution.WorkflowID,
			RunID:      ms.Execution.RunID,
			EventID:    eventID,
			Kind:       kind,
		}
	}

	// Carry forward the entries whose schedule lives in the index rather than
	// in the state.
	for k, r := range st.armed {
		switch k.Kind {
		case persistence.TimerKindActivityRetry:
			if _, pending := ms.Activity(k.EventID); pending && !ms.Status.Terminal() {
				want[k] = r
			}
		case persistence.TimerKindWorkflowRetry:
			// Owned by the successor-creation path, which deletes it once the
			// successor exists. It deliberately outlives the closed run.
			want[k] = r
		}
	}

	if !ms.Status.Terminal() {
		if deadline := earliest(ms.RunDeadline, ms.ExecutionDeadline); !deadline.IsZero() {
			// Event 1 is the natural anchor: the deadline is a property of the
			// run as a whole, and event 1 is the only event guaranteed to exist.
			want[key(1, persistence.TimerKindExecutionTimeout)] = persistence.TimerRecord{
				TimerKey: key(1, persistence.TimerKindExecutionTimeout),
				FireAt:   deadline,
			}
		}
		if wt := ms.WorkflowTask(); wt.Started() && !wt.Deadline.IsZero() {
			k := key(wt.StartedEventID, persistence.TimerKindWorkflowTaskTimeout)
			want[k] = persistence.TimerRecord{TimerKey: k, FireAt: wt.Deadline, Attempt: wt.Attempt, TaskQueue: wt.TaskQueue}
		}
		for startedEventID, t := range ms.Timers() {
			k := key(startedEventID, persistence.TimerKindUser)
			want[k] = persistence.TimerRecord{TimerKey: k, FireAt: t.FireAt}
		}
		for scheduledEventID, act := range ms.Activities() {
			if deadline := activityDeadline(act); !deadline.IsZero() {
				k := key(scheduledEventID, persistence.TimerKindActivityTimeout)
				want[k] = persistence.TimerRecord{
					TimerKey: k, FireAt: deadline, Attempt: act.Attempt, TaskQueue: act.TaskQueue,
				}
			}
		}
	}

	for _, r := range out.timers {
		want[r.TimerKey] = r
	}
	for _, k := range out.drop {
		delete(want, k)
	}
	return want
}

// diffTimers turns "what is armed" and "what should be armed" into the two
// lists AppendHistory takes.
func diffTimers(armed, want map[persistence.TimerKey]persistence.TimerRecord) ([]persistence.TimerRecord, []persistence.TimerKey) {
	var upserts []persistence.TimerRecord
	for k, r := range want {
		prev, ok := armed[k]
		if !ok || !prev.FireAt.Equal(r.FireAt) || prev.Attempt != r.Attempt || prev.TaskQueue != r.TaskQueue {
			upserts = append(upserts, r)
		}
	}
	var deletes []persistence.TimerKey
	for k := range armed {
		if _, ok := want[k]; !ok {
			deletes = append(deletes, k)
		}
	}
	return upserts, deletes
}

func appendUnique(keys []persistence.TimerKey, k persistence.TimerKey) []persistence.TimerKey {
	for _, existing := range keys {
		if existing == k {
			return keys
		}
	}
	return append(keys, k)
}

// timerRecordsFromEffects translates the effects that carry a schedule the
// state cannot reproduce.
//
// A user timer's fire time is recoverable from MutableState, so an ArmTimer
// effect naming a pending timer needs no translation. An activity retry's
// backoff is not: nothing in the history says when the next attempt is due,
// which is exactly why the index entry carries the attempt number as well.
func (e *Engine) timerRecordsFromEffects(ms *execution.MutableState, effects []execution.Effect) []persistence.TimerRecord {
	var out []persistence.TimerRecord
	for _, eff := range effects {
		switch eff.Kind {
		case execution.EffectArmTimer:
			if _, isUserTimer := ms.Timers()[eff.ScheduledEventID]; isUserTimer {
				continue
			}
			act, ok := ms.Activity(eff.ScheduledEventID)
			if !ok {
				continue
			}
			k := persistence.TimerKey{
				Namespace:  ms.Namespace,
				WorkflowID: ms.Execution.WorkflowID,
				RunID:      ms.Execution.RunID,
				EventID:    eff.ScheduledEventID,
				Kind:       persistence.TimerKindActivityRetry,
			}
			queue := eff.TaskQueue
			if queue == "" {
				queue = act.TaskQueue
			}
			out = append(out, persistence.TimerRecord{
				TimerKey: k, FireAt: eff.FireAt, Attempt: act.Attempt, TaskQueue: queue,
			})
		}
	}
	return out
}

// activityDeadline returns the next instant at which the engine must look at an
// activity, or the zero time when nothing bounds it.
//
// Only one index entry exists per activity, so the nearest deadline wins and is
// re-armed after every transition that moves it. Which clock actually expired is
// recomputed from state when the timer fires, which keeps the index free of
// redundant rows and of the ordering questions they would raise.
func activityDeadline(act *execution.ActivityState) time.Time {
	if act.RequestID != "" {
		// A worker holds this attempt: the attempt's own clocks apply.
		return earliest(act.AttemptDeadline, act.HeartbeatDeadline, act.ScheduleToCloseDeadline)
	}
	// Waiting for a worker. Schedule-to-start is measured from the scheduling
	// event, so it only bounds the first attempt; a later attempt is bounded by
	// the dispatch watchdog the re-dispatch path writes into AttemptDeadline.
	var scheduleToStart time.Time
	if d := act.Attributes.ScheduleToStartTimeout; d > 0 && act.Attempt <= 1 {
		scheduleToStart = act.ScheduledAt.Add(d)
	}
	return earliest(scheduleToStart, act.AttemptDeadline, act.ScheduleToCloseDeadline)
}

// earliest returns the smallest non-zero instant.
func earliest(times ...time.Time) time.Time {
	var best time.Time
	for _, t := range times {
		if t.IsZero() {
			continue
		}
		if best.IsZero() || t.Before(best) {
			best = t
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Long-poll notification
// ---------------------------------------------------------------------------

// notifier wakes GetHistory long polls when an execution advances in this
// process.
//
// It is a courtesy, not a contract: a write on another replica cannot signal
// these waiters, so every waiter also re-checks the store on an interval. The
// notification exists to make the common single-replica case instant.
type notifier struct {
	mu     sync.Mutex
	chans  map[string]*waitGroup
	closed bool
}

// waitGroup is one execution's set of waiters. The reference count exists so
// that an execution nobody is watching leaves nothing behind: a map keyed by
// execution that only ever grows is a slow leak in a process that runs for
// months.
type waitGroup struct {
	ch chan struct{}
	n  int
}

func (n *notifier) init() { n.chans = make(map[string]*waitGroup) }

// subscribe returns a channel closed by the next notify for key, and the
// function that releases the subscription.
func (n *notifier) subscribe(key string) (<-chan struct{}, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	w, ok := n.chans[key]
	if !ok {
		w = &waitGroup{ch: make(chan struct{})}
		n.chans[key] = w
	}
	w.n++
	return w.ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		// A notify or a close may have replaced the entry already, in which
		// case there is nothing to release.
		if cur, ok := n.chans[key]; ok && cur == w {
			if w.n--; w.n == 0 {
				delete(n.chans, key)
			}
		}
	}
}

func (n *notifier) notify(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if w, ok := n.chans[key]; ok {
		close(w.ch)
		delete(n.chans, key)
	}
}

func (n *notifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	n.closed = true
	for key, w := range n.chans {
		close(w.ch)
		delete(n.chans, key)
	}
}

// ---------------------------------------------------------------------------
// Identifiers, randomness and errors
// ---------------------------------------------------------------------------

// cryptoSeed draws a workflow's randomness seed.
//
// The seed is written into event 1 and every derived value -- retry jitter, the
// SDK's deterministic RNG -- comes from it, so it must be unpredictable once and
// then never redrawn. A crypto source removes any question about correlated
// seeds across processes started in the same millisecond.
func cryptoSeed() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on any supported platform, but a
		// panic-free fallback is cheaper than a failure mode nobody can test.
		return time.Now().UnixNano()
	}
	return int64(binary.LittleEndian.Uint64(b[:]) & math.MaxInt64)
}

func errorf(code, format string, args ...any) *api.Error {
	return &api.Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// mapError converts an internal error into the api.Error the wire protocol
// carries. Every failure the engine can produce has exactly one code, so a
// client can branch on the code rather than on message text.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		return apiErr
	}
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return errorf(api.CodeNotFound, "%s", err)
	case errors.Is(err, persistence.ErrAlreadyStarted):
		return errorf(api.CodeAlreadyExists, "%s", err)
	case errors.Is(err, persistence.ErrVersionConflict):
		return errorf(api.CodeVersionConflict, "%s", err)
	case errors.Is(err, persistence.ErrClosed), errors.Is(err, matching.ErrClosed):
		return errorf(api.CodeUnavailable, "%s", err)
	case errors.Is(err, matching.ErrBacklogFull):
		return errorf(api.CodeResourceExhausted, "%s", err)
	case errors.Is(err, execution.ErrStateTransition):
		return errorf(api.CodeFailedPrecondition, "%s", err)
	case errors.Is(err, history.ErrInvalidHistory):
		return errorf(api.CodeInternal, "%s", err)
	case errors.Is(err, skald.ErrInvalidIdentifier), errors.Is(err, skald.ErrInvalidRetryPolicy):
		return errorf(api.CodeInvalidArgument, "%s", err)
	case errors.Is(err, context.DeadlineExceeded):
		return errorf(api.CodeDeadlineExceeded, "%s", err)
	case errors.Is(err, context.Canceled):
		return errorf(api.CodeUnavailable, "request canceled: %s", err)
	}
	return errorf(api.CodeInternal, "%s", err)
}

// successorRequest unwraps a plan for the store, or nil when there is none.
func successorRequest(plan *createPlan) *persistence.CreateExecutionRequest {
	if plan == nil {
		return nil
	}
	req := plan.req
	return &req
}
