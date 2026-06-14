package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/skald-io/skald/internal/execution"
	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// Recover re-materialises the derived task queues from durable state.
//
// Matching is deliberately not persisted, so a process that starts with an
// empty matcher has forgotten every pollable task. Recover walks the open
// executions and re-dispatches the work whose readiness is visible in the
// history: a workflow task that is scheduled but not started, and an activity
// that has never been handed to a worker.
//
// It deliberately does *not* re-dispatch work that history shows as in flight.
// A workflow task a worker already started, or an activity attempt a worker is
// running, may still be alive on that worker; re-dispatching would duplicate
// it. Those are covered by their timeout entries in the durable timer index,
// which fire and reschedule on their own. The rule is: recovery restores what
// nothing else can, and lets the timer index handle what it already owns.
func (e *Engine) Recover(ctx context.Context) error {
	for _, namespace := range e.recoverNamespaces {
		// The records are collected before any of them is processed. The
		// callback runs inside the driver's iteration, which may hold a cursor
		// or a lock, and re-entering the store from it is a deadlock waiting to
		// be discovered in production.
		var open []persistence.ExecutionRecord
		if err := e.store.OpenExecutions(ctx, namespace, func(rec persistence.ExecutionRecord) error {
			open = append(open, rec)
			return nil
		}); err != nil {
			return mapError(err)
		}

		for _, rec := range open {
			if err := ctx.Err(); err != nil {
				return mapError(err)
			}
			if err := e.recoverExecution(ctx, rec); err != nil {
				// One unreadable execution must not stop the recovery of every
				// other one: the alternative is a single corrupt history
				// keeping a whole deployment down.
				e.log.Error("recovery skipped an execution",
					"namespace", rec.Namespace, "workflow_id", rec.WorkflowID,
					"run_id", rec.RunID, "error", err)
			}
		}
	}
	return nil
}

func (e *Engine) recoverExecution(ctx context.Context, rec persistence.ExecutionRecord) error {
	unlock := e.lockExecution(rec.Namespace, rec.WorkflowID)
	defer unlock()

	st, err := e.load(ctx, rec.Namespace, rec.WorkflowID, rec.RunID)
	if err != nil {
		return mapError(err)
	}
	ms := st.ms
	if ms.Status.Terminal() {
		return nil
	}

	if wt := ms.WorkflowTask(); wt.Scheduled() {
		e.dispatchWorkflowTask(ms, execution.Effect{
			Kind:             execution.EffectDispatchWorkflowTask,
			TaskQueue:        wt.TaskQueue,
			ScheduledEventID: wt.ScheduledEventID,
		})
	}
	for scheduledEventID, act := range ms.Activities() {
		if act.Started() {
			// Either a worker is running it or a retry is pending; both are
			// owned by the timer index.
			continue
		}
		e.dispatchActivityTask(ms, execution.Effect{
			Kind:             execution.EffectDispatchActivityTask,
			TaskQueue:        act.TaskQueue,
			ScheduledEventID: scheduledEventID,
		})
	}
	return e.reconcileTimers(ctx, st)
}

// reconcileTimers re-arms every deadline the state implies.
//
// The due-time index is a materialised view of state (see the persistence
// package documentation), and like any cache it can lose an entry: a write that
// fails after its events commit, an index pruned with a run that was not
// actually closed, an operator restoring a partial backup. The consequence is
// specific and severe -- an execution whose only future wake-up was that timer
// stops forever, silently, while the console still shows it RUNNING.
//
// Ordinary writes already repair the index, because a cold load starts with an
// empty armed set and the next commit upserts the full desired set. That is no
// help to an execution that will never be written again, which is exactly the
// one that is stuck. Recovery is therefore the place to reconcile: it is the
// only moment the engine looks at every open execution.
//
// The deterministic simulator found this at seed 43, as a run whose activity
// had been started by a worker that then crashed, with no timeout timer left to
// notice.
//
// Only upserts are issued. Reconstructing deletions would need the index's
// current contents, and a stale entry is harmless: timer delivery is
// at-least-once by contract, and every transition it can drive is a no-op when
// the state has moved on.
func (e *Engine) reconcileTimers(ctx context.Context, st *cachedState) error {
	want := e.desiredTimers(st, outcome{})
	missing := make([]persistence.TimerRecord, 0, len(want))
	for key, rec := range want {
		if _, armed := st.armed[key]; !armed {
			missing = append(missing, rec)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	rec, err := e.store.AppendHistory(ctx, persistence.AppendHistoryRequest{
		Namespace:       st.ms.Namespace,
		WorkflowID:      st.ms.Execution.WorkflowID,
		RunID:           st.ms.Execution.RunID,
		ExpectedVersion: st.ms.Version,
		Record:          e.recordFor(st),
		UpsertTimers:    missing,
	})
	if err != nil {
		// A version conflict means another writer is already advancing this
		// execution, which repairs the index as a side effect. Nothing to do.
		if errors.Is(err, persistence.ErrVersionConflict) {
			e.invalidate(st.ms.Namespace, st.ms.Execution.WorkflowID, st.ms.Execution.RunID)
			return nil
		}
		return mapError(err)
	}
	e.log.Info("recovery re-armed timers",
		"namespace", st.ms.Namespace, "workflow_id", st.ms.Execution.WorkflowID,
		"run_id", st.ms.Execution.RunID, "count", len(missing))
	st.rec = rec
	st.ms.Version = rec.Version
	st.armed = want
	return nil
}

// ---------------------------------------------------------------------------
// Timer dispatch
// ---------------------------------------------------------------------------

// onTimer routes one due timer to the transition it stands for.
//
// Every branch is idempotent against the state machine rather than against the
// message, because the timer service delivers at least once. A timer that no
// longer matches the state is not an error: it is the normal outcome of a race
// the execution already won, and it returns nil so the entry is deleted.
func (e *Engine) onTimer(ctx context.Context, rec persistence.TimerRecord) error {
	switch rec.Kind {
	case persistence.TimerKindUser:
		return e.fireUserTimer(ctx, rec)
	case persistence.TimerKindActivityRetry:
		return e.redispatchActivity(ctx, rec)
	case persistence.TimerKindActivityTimeout:
		return e.timeoutActivity(ctx, rec)
	case persistence.TimerKindWorkflowTaskTimeout:
		return e.timeoutWorkflowTask(ctx, rec)
	case persistence.TimerKindExecutionTimeout:
		return e.timeoutExecution(ctx, rec)
	case persistence.TimerKindWorkflowRetry:
		return e.startDeferredRun(ctx, rec)
	}
	return fmt.Errorf("engine: no handler for timer kind %d", rec.Kind)
}

// FireTimer applies one due timer synchronously, on the caller's goroutine.
//
// In production nothing calls this: the timer service owns the scan loop and
// calls onTimer itself. It exists for the deterministic simulator, which cannot
// use that loop. A background goroutine issuing store calls interleaved with
// the simulator's own would make the sequence of operations -- and therefore
// which of them a seeded fault injector fails -- depend on the Go scheduler, and
// a run that cannot be replayed from its seed is not a simulation. Driving the
// same code path from the single scheduling goroutine removes the interleaving
// without changing what the engine does with a timer.
func (e *Engine) FireTimer(ctx context.Context, rec persistence.TimerRecord) error {
	return e.onTimer(ctx, rec)
}

// closedOrMissing reports whether a timer names a run that can no longer act on
// it, in which case the entry is simply dropped.
func (e *Engine) closedOrMissing(err error) error {
	if apiCode(err) == api.CodeNotFound {
		return nil
	}
	return err
}

func (e *Engine) fireUserTimer(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		if st.ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		// FireTimer is a documented no-op when the timer was cancelled between
		// the scan and now, so no pre-check is needed here.
		effects, err := st.ms.FireTimer(rec.EventID)
		if err != nil {
			return outcome{}, err
		}
		return outcome{effects: effects, drop: []persistence.TimerKey{rec.TimerKey}}, nil
	}))
}

// redispatchActivity makes a backed-off attempt pollable again.
func (e *Engine) redispatchActivity(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		if ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		act, ok := ms.Activity(rec.EventID)
		if !ok {
			return outcome{noop: true}, nil
		}
		if rec.Attempt < act.Attempt {
			// A newer attempt already superseded this entry.
			return outcome{noop: true}, nil
		}
		if rec.Attempt == act.Attempt && act.RequestID != "" {
			// A worker already picked this attempt up.
			return outcome{noop: true}, nil
		}

		// The index entry is the durable statement of which attempt is due, so
		// it wins over a rebuilt state whose newest started event belongs to an
		// earlier attempt.
		act.Attempt = rec.Attempt
		act.RequestID = ""
		act.HeartbeatDeadline = time.Time{}
		// Until a worker takes it, the attempt is bounded by a dispatch
		// watchdog. Without one, a dispatch lost to a full backlog or to a
		// crash between this write and the poll would strand the activity until
		// its schedule-to-close budget expired -- or forever, if it has none.
		act.AttemptDeadline = e.clk.Now().Add(e.redispatch)

		queue := rec.TaskQueue
		if queue == "" {
			queue = act.TaskQueue
		}
		return outcome{
			effects: []execution.Effect{{
				Kind:             execution.EffectDispatchActivityTask,
				TaskQueue:        queue,
				ScheduledEventID: rec.EventID,
			}},
			drop: []persistence.TimerKey{rec.TimerKey},
		}, nil
	}))
}

// timeoutActivity applies whichever activity clock expired.
func (e *Engine) timeoutActivity(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		if ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		act, ok := ms.Activity(rec.EventID)
		if !ok {
			return outcome{noop: true}, nil
		}
		if rec.Attempt < act.Attempt {
			return outcome{noop: true}, nil
		}
		if rec.Attempt > act.Attempt {
			// The entry knows about an attempt this rebuilt state does not.
			act.Attempt = rec.Attempt
		}

		now := e.clk.Now()
		kind := expiredTimeout(act, now)
		if kind == skald.TimeoutUnspecified {
			// A heartbeat moved the deadline out after this entry was written.
			// Returning without a write leaves the derived set to re-arm the
			// new deadline on the next transition; dropping the entry here and
			// re-adding it keeps the index honest in the meantime.
			return outcome{drop: []persistence.TimerKey{rec.TimerKey}}, nil
		}

		startedEventID := act.StartedEventID
		effects, err := ms.TimeoutActivity(rec.EventID, kind)
		if err != nil {
			return outcome{}, err
		}
		restoreStartedEvent(ms, rec.EventID, startedEventID)
		return outcome{effects: effects, drop: []persistence.TimerKey{rec.TimerKey}}, nil
	}))
}

// expiredTimeout decides which of an activity's four clocks ran out.
//
// The index holds one entry per activity, armed at the nearest deadline, so the
// kind is recomputed here rather than stored. Innermost clocks are checked
// first: a heartbeat or start-to-close expiry is retryable, and reporting the
// outer schedule-to-close budget instead would turn a transient stall into a
// terminal failure.
func expiredTimeout(act *execution.ActivityState, now time.Time) skald.TimeoutKind {
	expired := func(t time.Time) bool { return !t.IsZero() && !now.Before(t) }

	if act.RequestID != "" {
		if expired(act.HeartbeatDeadline) {
			return skald.TimeoutHeartbeat
		}
		if expired(act.AttemptDeadline) {
			return skald.TimeoutStartToClose
		}
	} else {
		// No worker holds the attempt, so any expiry means it never started.
		if expired(act.AttemptDeadline) {
			return skald.TimeoutScheduleToStart
		}
		if d := act.Attributes.ScheduleToStartTimeout; d > 0 && act.Attempt <= 1 && expired(act.ScheduledAt.Add(d)) {
			return skald.TimeoutScheduleToStart
		}
	}
	if expired(act.ScheduleToCloseDeadline) {
		return skald.TimeoutScheduleToClose
	}
	return skald.TimeoutUnspecified
}

// timeoutWorkflowTask reschedules a task whose worker vanished.
func (e *Engine) timeoutWorkflowTask(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		wt := ms.WorkflowTask()
		if ms.Status.Terminal() || !wt.Started() || wt.StartedEventID != rec.EventID {
			return outcome{noop: true}, nil
		}
		if now := e.clk.Now(); !wt.Deadline.IsZero() && now.Before(wt.Deadline) {
			return outcome{drop: []persistence.TimerKey{rec.TimerKey}}, nil
		}
		effects, err := ms.TimeoutWorkflowTask()
		if err != nil {
			return outcome{}, err
		}
		return outcome{effects: effects, drop: []persistence.TimerKey{rec.TimerKey}}, nil
	}))
}

// timeoutExecution closes a run whose run or execution budget ran out.
func (e *Engine) timeoutExecution(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		if ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		// The execution deadline spans every run of a retried or continued
		// workflow, so it is reported as the outer schedule-to-close budget;
		// the run deadline bounds this run alone and maps to start-to-close.
		now := e.clk.Now()
		var kind skald.TimeoutKind
		switch {
		case !ms.ExecutionDeadline.IsZero() && !now.Before(ms.ExecutionDeadline):
			kind = skald.TimeoutScheduleToClose
		case !ms.RunDeadline.IsZero() && !now.Before(ms.RunDeadline):
			kind = skald.TimeoutStartToClose
		default:
			return outcome{noop: true}, nil
		}
		if err := ms.TimeoutExecution(kind); err != nil {
			return outcome{}, err
		}
		return outcome{drop: []persistence.TimerKey{rec.TimerKey}}, nil
	}))
}

// ---------------------------------------------------------------------------
// Successor runs: continue-as-new and workflow-level retry
// ---------------------------------------------------------------------------

// successorRequestID makes successor creation idempotent.
//
// The close and the creation now share one transaction, but the pair can still
// be attempted more than once: a client retry, or two replicas that both
// observed the close and both computed a successor. Deriving the request ID
// from the predecessor's run ID means the store's own deduplication collapses
// every later attempt onto the first, with no coordination between the writers.
func successorRequestID(predecessorRunID string) string {
	return "successor-of:" + predecessorRunID
}

// startSuccessor creates the run named by a StartNewRun effect as a separate
// write.
//
// It is the fallback for a StartNewRun effect that reaches postCommit without
// having been converted into a createPlan, which finalizeEffects should always
// do. Keeping it costs little and means a future path that forgets the atomic
// route degrades to the old two-write behaviour instead of dropping the
// successor entirely -- but it is a bug when it runs, because the window
// between the two writes is exactly what CreateSuccessor exists to remove.
func (e *Engine) startSuccessor(ctx context.Context, ms *execution.MutableState, req *execution.NewRunRequest) error {
	runID := successorRunID(ms)
	if runID == "" {
		runID = e.newID()
	}
	attrs := req.Attributes
	if attrs.RandomnessSeed == 0 {
		attrs.RandomnessSeed = e.newSeed()
	}

	_, err := e.createRunLocked(ctx, createParams{
		namespace:  ms.Namespace,
		workflowID: req.WorkflowID,
		runID:      runID,
		attrs:      attrs,
		requestID:  successorRequestID(ms.Execution.RunID),
		reuse:      persistence.ReuseAllowDuplicate,
		// A workflow-level retry has to wait out its backoff. The wait is
		// expressed as a deferred first workflow task on the *successor*, not
		// as a timer on the predecessor: a closed run keeps no timers, so a
		// backoff parked on the predecessor would be dropped the moment it was
		// written. The successor therefore exists immediately and is simply not
		// pollable yet, which is also the more honest thing to show an operator.
		firstTaskDelay: req.Delay,
	})
	return err
}

// startDeferredRun schedules the first workflow task of a run that was created
// with a backoff.
func (e *Engine) startDeferredRun(ctx context.Context, rec persistence.TimerRecord) error {
	return e.closedOrMissing(e.mutate(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		if ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		// ScheduleWorkflowTask is idempotent: a signal that arrived during the
		// backoff will already have woken the run, and this becomes a no-op
		// that only clears the timer.
		eff, err := ms.ScheduleWorkflowTask()
		if err != nil {
			return outcome{}, err
		}
		var effects []execution.Effect
		if eff != nil {
			effects = append(effects, *eff)
		}
		return outcome{effects: effects, drop: []persistence.TimerKey{rec.TimerKey}}, nil
	}))
}

// successorRunID returns the run ID a continue-as-new already committed to.
// A workflow-level retry names no successor in its closing event, so its run ID
// is generated at creation time and pinned by the request ID instead.
func successorRunID(ms *execution.MutableState) string {
	events := ms.Events()
	if len(events) == 0 {
		return ""
	}
	attrs, ok := history.AttributesAs[history.WorkflowExecutionContinuedAsNewAttributes](events[len(events)-1])
	if !ok {
		return ""
	}
	return attrs.NewRunID
}

// finalizeEffects prepares effects for the write.
//
// It assigns the successor run ID while the closing event can still be patched
// -- once the event is in the store it is immutable, and a continue-as-new
// event that names no successor is an unwalkable chain -- and translates the
// effects whose schedule the state cannot reproduce into index entries.
// It also assembles the successor of a continue-as-new or a workflow-level
// retry so that it can be created *inside* the transaction that closes the
// predecessor. Doing the two as separate writes leaves a window in which a
// crash strands the whole logical workflow: the predecessor says
// CONTINUED_AS_NEW, the successor its final event names does not exist, and
// nothing is running. The deterministic simulator found exactly that, at seed
// 5, which is why the store grew persistence.AppendHistoryRequest.CreateSuccessor.
//
// The StartNewRun effects it converts are removed from the returned slice: they
// have been handled, and leaving them would make the post-commit path create
// the run a second time.
func (e *Engine) finalizeEffects(
	ms *execution.MutableState, effects []execution.Effect,
) ([]persistence.TimerRecord, []execution.Effect, *createPlan, error) {
	var plan *createPlan
	kept := effects[:0:0]
	for _, eff := range effects {
		if eff.Kind != execution.EffectStartNewRun || eff.NewRun == nil {
			kept = append(kept, eff)
			continue
		}
		if successorRunID(ms) == pendingRunIDPlaceholder {
			if err := ms.PatchContinuedAsNewRunID(e.newID()); err != nil {
				return nil, nil, nil, err
			}
		}
		if plan != nil {
			// One closing command means at most one successor. A second would
			// be a state-machine bug, and silently creating both would make the
			// chain fork.
			return nil, nil, nil, fmt.Errorf(
				"engine: run %s produced two successors", ms.Execution.RunID)
		}
		var err error
		plan, err = e.successorPlan(ms, eff.NewRun)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return e.timerRecordsFromEffects(ms, kept), kept, plan, nil
}

// successorPlan assembles the run that continues a closing one.
func (e *Engine) successorPlan(ms *execution.MutableState, req *execution.NewRunRequest) (*createPlan, error) {
	runID := successorRunID(ms)
	if runID == "" || runID == pendingRunIDPlaceholder {
		runID = e.newID()
	}
	attrs := req.Attributes
	if attrs.RandomnessSeed == 0 {
		attrs.RandomnessSeed = e.newSeed()
	}
	return e.buildCreate(createParams{
		namespace:  ms.Namespace,
		workflowID: req.WorkflowID,
		runID:      runID,
		attrs:      attrs,
		requestID:  successorRequestID(ms.Execution.RunID),
		reuse:      persistence.ReuseAllowDuplicate,
		// A workflow-level retry has to wait out its backoff. The wait is
		// expressed as a deferred first workflow task on the *successor*, not
		// as a timer on the predecessor: a closed run keeps no timers, so a
		// backoff parked on the predecessor would be dropped the moment it was
		// written. The successor therefore exists immediately and is simply not
		// pollable yet, which is also the more honest thing to show an operator.
		firstTaskDelay: req.Delay,
	})
}

// pendingRunIDPlaceholder mirrors the sentinel the state machine writes into a
// continue-as-new event until the caller assigns the successor's run ID. It is
// duplicated rather than exported from execution because it is a detail of the
// hand-off between the two packages, not part of either's API.
const pendingRunIDPlaceholder = "<pending>"
