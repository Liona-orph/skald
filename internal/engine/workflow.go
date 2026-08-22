package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Liona-orph/skald/internal/execution"
	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// ---------------------------------------------------------------------------
// Starting executions
// ---------------------------------------------------------------------------

// StartWorkflow implements api.Service.
func (e *Engine) StartWorkflow(ctx context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
	namespace, attrs, reuse, err := e.validateStart(req)
	if err != nil {
		return api.StartWorkflowResponse{}, err
	}

	unlock := e.lockExecution(namespace, req.WorkflowID)
	defer unlock()

	if reuse == persistence.ReuseTerminateIfRunning {
		// The store enforces the policy on the row, but only the engine can
		// write the Terminated event that makes the predecessor's history
		// explain what happened to it. Doing it here keeps "why did my workflow
		// stop?" answerable from the history alone.
		if err := e.terminateIfRunningLocked(ctx, namespace, req.WorkflowID, req.Identity); err != nil {
			return api.StartWorkflowResponse{}, err
		}
	}

	runID := e.newID()
	rec, err := e.createRunLocked(ctx, createParams{
		namespace:  namespace,
		workflowID: req.WorkflowID,
		runID:      runID,
		attrs:      attrs,
		requestID:  req.RequestID,
		reuse:      reuse,
	})
	if err != nil {
		return api.StartWorkflowResponse{}, err
	}
	// A run ID other than the one just generated means the store deduplicated
	// the request or reused an existing run, which the caller wants to know
	// without a second query.
	return api.StartWorkflowResponse{RunID: rec.RunID, Started: rec.RunID == runID}, nil
}

// SignalWithStartWorkflow implements api.Service.
//
// The point of the operation is atomicity: "start it if it is not running, then
// signal it" performed by a client as two calls has a window in which two
// callers both observe "not running" and both start. Here the start and the
// signal are a single CreateExecution, so the store's uniqueness check decides
// the race and the loser falls through to signalling the winner's run.
func (e *Engine) SignalWithStartWorkflow(ctx context.Context, req api.SignalWithStartRequest) (api.StartWorkflowResponse, error) {
	if req.SignalName == "" {
		return api.StartWorkflowResponse{}, errorf(api.CodeInvalidArgument, "engine: signal name must not be empty")
	}
	namespace, attrs, reuse, err := e.validateStart(req.Start)
	if err != nil {
		return api.StartWorkflowResponse{}, err
	}

	unlock := e.lockExecution(namespace, req.Start.WorkflowID)
	defer unlock()

	// If a run is already open, this is a plain signal against it.
	if rec, err := e.store.GetExecution(ctx, namespace, req.Start.WorkflowID, ""); err == nil && rec.Open() {
		err := e.mutateLocked(ctx, namespace, req.Start.WorkflowID, rec.RunID, func(st *cachedState) (outcome, error) {
			effects, err := st.ms.Signal(req.SignalName, req.SignalInput, req.Start.Identity)
			return outcome{effects: effects}, err
		})
		if err != nil {
			return api.StartWorkflowResponse{}, err
		}
		return api.StartWorkflowResponse{RunID: rec.RunID, Started: false}, nil
	}

	runID := e.newID()
	rec, err := e.createRunLocked(ctx, createParams{
		namespace:  namespace,
		workflowID: req.Start.WorkflowID,
		runID:      runID,
		attrs:      attrs,
		requestID:  req.Start.RequestID,
		reuse:      reuse,
		// The signal is applied to the same MutableState as the start, so all
		// three events -- started, signaled, workflow task scheduled -- reach
		// the store in one transaction.
		afterStart: func(ms *execution.MutableState) ([]execution.Effect, error) {
			return ms.Signal(req.SignalName, req.SignalInput, req.Start.Identity)
		},
	})
	if err != nil {
		return api.StartWorkflowResponse{}, err
	}
	if rec.RunID != runID {
		// Another writer won the create race. The workflow exists, so honour
		// the signal against the run that won.
		if err := e.SignalWorkflow(ctx, api.SignalWorkflowRequest{
			Namespace:  namespace,
			WorkflowID: req.Start.WorkflowID,
			RunID:      rec.RunID,
			SignalName: req.SignalName,
			Input:      req.SignalInput,
			Identity:   req.Start.Identity,
		}); err != nil {
			return api.StartWorkflowResponse{}, err
		}
		return api.StartWorkflowResponse{RunID: rec.RunID, Started: false}, nil
	}
	return api.StartWorkflowResponse{RunID: rec.RunID, Started: true}, nil
}

// validateStart checks a start request and turns it into event 1's attributes.
func (e *Engine) validateStart(req api.StartWorkflowRequest) (string, history.WorkflowExecutionStartedAttributes, persistence.IDReusePolicy, error) {
	var attrs history.WorkflowExecutionStartedAttributes

	namespace := req.Namespace
	if namespace == "" {
		namespace = e.defaultNS
	}
	if err := skald.ValidateNamespace(namespace); err != nil {
		return "", attrs, 0, mapError(err)
	}
	if err := skald.ValidateWorkflowID(req.WorkflowID); err != nil {
		return "", attrs, 0, mapError(err)
	}
	if err := skald.ValidateTypeName(req.WorkflowType); err != nil {
		return "", attrs, 0, mapError(err)
	}
	if err := skald.ValidateTaskQueue(req.TaskQueue); err != nil {
		return "", attrs, 0, mapError(err)
	}
	for name, d := range map[string]time.Duration{
		"execution_timeout": req.ExecutionTimeout,
		"run_timeout":       req.RunTimeout,
		"task_timeout":      req.TaskTimeout,
	} {
		if d < 0 {
			return "", attrs, 0, errorf(api.CodeInvalidArgument, "engine: %s must not be negative", name)
		}
	}
	policy := req.RetryPolicy.Clone()
	// Validate normalises in place, so every consumer downstream sees fully
	// specified values and nobody re-implements "zero means default".
	if err := policy.Validate(); err != nil {
		return "", attrs, 0, mapError(err)
	}
	reuse, err := parseReusePolicy(req.ReusePolicy)
	if err != nil {
		return "", attrs, 0, err
	}

	attrs = history.WorkflowExecutionStartedAttributes{
		WorkflowType:     req.WorkflowType,
		TaskQueue:        req.TaskQueue,
		Input:            req.Input,
		RunTimeout:       req.RunTimeout,
		ExecutionTimeout: req.ExecutionTimeout,
		TaskTimeout:      req.TaskTimeout,
		RetryPolicy:      policy,
		Attempt:          1,
		RandomnessSeed:   e.newSeed(),
		Memo:             req.Memo,
		SearchAttrs:      req.SearchAttrs,
		Identity:         req.Identity,
		CronSchedule:     req.CronSchedule,
	}
	return namespace, attrs, reuse, nil
}

// parseReusePolicy maps the wire string onto the store's enum. An unknown value
// is rejected rather than defaulted: silently downgrading RejectDuplicate to
// AllowDuplicate because of a typo would let a workflow run twice.
func parseReusePolicy(s string) (persistence.IDReusePolicy, error) {
	switch normalizeEnum(s) {
	case "", "allowduplicate":
		return persistence.ReuseAllowDuplicate, nil
	case "allowduplicatefailedonly":
		return persistence.ReuseAllowDuplicateFailedOnly, nil
	case "rejectduplicate":
		return persistence.ReuseRejectDuplicate, nil
	case "terminateifrunning":
		return persistence.ReuseTerminateIfRunning, nil
	}
	return 0, errorf(api.CodeInvalidArgument, "engine: unknown workflow id reuse policy %q", s)
}

func normalizeEnum(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "")
	return strings.ReplaceAll(s, "-", "")
}

// createParams describes a run to be created.
type createParams struct {
	namespace  string
	workflowID string
	runID      string
	attrs      history.WorkflowExecutionStartedAttributes
	requestID  string
	reuse      persistence.IDReusePolicy
	// afterStart applies extra transitions between event 1 and the first
	// workflow task, which is how signal-with-start stays a single write.
	afterStart func(*execution.MutableState) ([]execution.Effect, error)
	// firstTaskDelay defers the first workflow task by arming a timer on the
	// new run instead of scheduling the task now. It is how a workflow-level
	// retry serves its backoff.
	firstTaskDelay time.Duration
}

// createRunLocked writes a new run. The caller must hold the execution lock for
// (namespace, workflowID).
func (e *Engine) createRunLocked(ctx context.Context, p createParams) (persistence.ExecutionRecord, error) {
	plan, err := e.buildCreate(p)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	rec, err := e.store.CreateExecution(ctx, plan.req)
	if err != nil {
		return persistence.ExecutionRecord{}, mapError(err)
	}
	if rec.RunID != p.runID {
		// Deduplicated or reused: nothing of ours was written, so none of our
		// effects may be applied.
		return rec, nil
	}
	e.finishCreate(ctx, plan, rec)
	return rec, nil
}

// createPlan is a run that has been fully assembled but not yet written.
//
// Separating assembly from the write is what lets a successor run be created
// inside the transaction that closes its predecessor: the same plan can be
// handed to CreateExecution or attached to an AppendHistory as
// CreateSuccessor, with no second construction path to drift.
type createPlan struct {
	req     persistence.CreateExecutionRequest
	st      *cachedState
	effects []execution.Effect
	want    map[persistence.TimerKey]persistence.TimerRecord
	runID   string
}

// buildCreate assembles a run without touching the store.
func (e *Engine) buildCreate(p createParams) (*createPlan, error) {
	exec := skald.WorkflowExecution{WorkflowID: p.workflowID, RunID: p.runID}
	ms := execution.NewMutableState(p.namespace, exec, e.clk.Now)

	if _, err := ms.Start(p.attrs); err != nil {
		return nil, mapError(err)
	}
	var effects []execution.Effect
	if p.afterStart != nil {
		extra, err := p.afterStart(ms)
		if err != nil {
			return nil, mapError(err)
		}
		effects = append(effects, extra...)
	}
	// A new run normally needs a workflow task immediately: the first thing
	// that has to happen is workflow code running against event 1. A run
	// serving a retry backoff is the exception -- it exists, but nothing may
	// poll it until its timer fires.
	var deferred []persistence.TimerRecord
	if p.firstTaskDelay > 0 {
		key := persistence.TimerKey{
			Namespace:  p.namespace,
			WorkflowID: p.workflowID,
			RunID:      p.runID,
			EventID:    1,
			Kind:       persistence.TimerKindWorkflowRetry,
		}
		deferred = append(deferred, persistence.TimerRecord{
			TimerKey: key,
			FireAt:   e.clk.Now().Add(p.firstTaskDelay),
			Attempt:  p.attrs.Attempt,
		})
	} else if eff, err := ms.ScheduleWorkflowTask(); err != nil {
		return nil, mapError(err)
	} else if eff != nil {
		effects = append(effects, *eff)
	}

	st := &cachedState{ms: ms, armed: map[persistence.TimerKey]persistence.TimerRecord{}}
	want := e.desiredTimers(st, outcome{timers: deferred})
	timerRecs := make([]persistence.TimerRecord, 0, len(want))
	for _, r := range want {
		timerRecs = append(timerRecs, r)
	}

	req := persistence.CreateExecutionRequest{
		Record: persistence.ExecutionRecord{
			Namespace:           p.namespace,
			WorkflowID:          p.workflowID,
			RunID:               p.runID,
			WorkflowType:        ms.WorkflowType,
			TaskQueue:           ms.TaskQueue,
			Status:              skald.StatusRunning,
			StartedAt:           ms.StartedAt,
			LastEventID:         ms.Events().LastEventID(),
			FirstExecutionRunID: ms.FirstExecutionRunID,
			Memo:                ms.Memo,
			SearchAttrs:         ms.SearchAttrs,
		},
		Events:      ms.Events(),
		Timers:      timerRecs,
		RequestID:   p.requestID,
		ReusePolicy: p.reuse,
	}
	return &createPlan{req: req, st: st, effects: effects, want: want, runID: p.runID}, nil
}

// finishCreate installs a run that has just been written and releases its
// effects.
func (e *Engine) finishCreate(ctx context.Context, plan *createPlan, rec persistence.ExecutionRecord) {
	key := cacheKey(rec.Namespace, rec.WorkflowID, plan.runID)
	plan.st.ms.Version = rec.Version
	plan.st.rec = rec
	plan.st.armed = plan.want
	e.cache.Add(key, plan.st)
	e.notifier.notify(key)
	e.postCommit(ctx, plan.st, plan.effects)
}

// terminateIfRunningLocked closes any open run of the workflow ID so that a
// fresh one may start.
func (e *Engine) terminateIfRunningLocked(ctx context.Context, namespace, workflowID, identity string) error {
	rec, err := e.store.GetExecution(ctx, namespace, workflowID, "")
	if err != nil {
		if apiCode(err) == api.CodeNotFound {
			return nil
		}
		return mapError(err)
	}
	if !rec.Open() {
		return nil
	}
	return e.mutateLocked(ctx, namespace, workflowID, rec.RunID, func(st *cachedState) (outcome, error) {
		if st.ms.Status.Terminal() {
			return outcome{noop: true}, nil
		}
		return outcome{}, st.ms.Terminate("terminated by a new run with the TerminateIfRunning reuse policy", nil, identity)
	})
}

// ---------------------------------------------------------------------------
// Signal, cancel, terminate
// ---------------------------------------------------------------------------

// SignalWorkflow implements api.Service.
func (e *Engine) SignalWorkflow(ctx context.Context, req api.SignalWorkflowRequest) error {
	if req.SignalName == "" {
		return errorf(api.CodeInvalidArgument, "engine: signal name must not be empty")
	}
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.WorkflowID, req.RunID, func(st *cachedState) (outcome, error) {
		if st.ms.Status.Terminal() {
			return outcome{}, errorf(api.CodeFailedPrecondition,
				"engine: workflow %s is %s and cannot be signaled", st.ms.Execution, st.ms.Status)
		}
		effects, err := st.ms.Signal(req.SignalName, req.Input, req.Identity)
		return outcome{effects: effects}, err
	})
}

// CancelWorkflow implements api.Service.
//
// Cancellation is cooperative: it records a request and wakes the workflow,
// which decides how to unwind. A workflow that ignores the request keeps
// running, which is a feature -- a payment that must not be abandoned halfway
// is allowed to say so.
func (e *Engine) CancelWorkflow(ctx context.Context, req api.CancelWorkflowRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.WorkflowID, req.RunID, func(st *cachedState) (outcome, error) {
		if st.ms.Status.Terminal() {
			return outcome{}, errorf(api.CodeFailedPrecondition,
				"engine: workflow %s is %s and cannot be canceled", st.ms.Execution, st.ms.Status)
		}
		effects, err := st.ms.RequestCancel(req.Reason, req.Identity)
		if err != nil {
			return outcome{}, err
		}
		// A repeated cancel produces no events and no effects; the write path
		// then skips the store round trip entirely.
		return outcome{effects: effects}, nil
	})
}

// TerminateWorkflow implements api.Service. Unlike cancellation it runs no
// workflow code: the execution is closed where it stands.
func (e *Engine) TerminateWorkflow(ctx context.Context, req api.TerminateWorkflowRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.WorkflowID, req.RunID, func(st *cachedState) (outcome, error) {
		if st.ms.Status.Terminal() {
			return outcome{}, errorf(api.CodeFailedPrecondition,
				"engine: workflow %s is already %s", st.ms.Execution, st.ms.Status)
		}
		return outcome{}, st.ms.Terminate(req.Reason, req.Details, req.Identity)
	})
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// DescribeWorkflow implements api.Service.
func (e *Engine) DescribeWorkflow(ctx context.Context, namespace, workflowID, runID string) (api.DescribeWorkflowResponse, error) {
	ns, err := e.resolveNamespace(namespace)
	if err != nil {
		return api.DescribeWorkflowResponse{}, err
	}
	// The read takes the execution lock because it shares the rebuilt-state
	// cache with the write path, and an unsynchronised reader of a state a
	// writer is mutating is a data race, not merely a stale answer.
	unlock := e.lockExecution(ns, workflowID)
	defer unlock()

	st, err := e.load(ctx, ns, workflowID, runID)
	if err != nil {
		return api.DescribeWorkflowResponse{}, mapError(err)
	}
	ms := st.ms

	resp := api.DescribeWorkflowResponse{
		Namespace:           ns,
		WorkflowID:          ms.Execution.WorkflowID,
		RunID:               ms.Execution.RunID,
		WorkflowType:        ms.WorkflowType,
		TaskQueue:           ms.TaskQueue,
		Status:              ms.Status,
		StartedAt:           ms.StartedAt,
		HistoryLength:       int64(len(ms.Events())),
		FirstExecutionRunID: ms.FirstExecutionRunID,
		Memo:                ms.Memo,
		SearchAttrs:         ms.SearchAttrs,
	}
	if !ms.ClosedAt.IsZero() {
		closed := ms.ClosedAt
		resp.ClosedAt = &closed
	}
	for scheduledEventID, act := range ms.Activities() {
		resp.PendingActivities = append(resp.PendingActivities, api.PendingActivity{
			ActivityID:       act.ActivityID,
			ActivityType:     act.ActivityType,
			ScheduledEventID: scheduledEventID,
			Attempt:          act.Attempt,
			// A worker holds the attempt exactly when it has a request ID; an
			// activity between attempts still carries a started event.
			Started:     act.RequestID != "",
			ScheduledAt: act.ScheduledAt,
		})
	}
	for startedEventID, t := range ms.Timers() {
		resp.PendingTimers = append(resp.PendingTimers, api.PendingTimer{
			TimerID:        t.TimerID,
			StartedEventID: startedEventID,
			FireAt:         t.FireAt,
		})
	}
	sortPending(resp.PendingActivities, resp.PendingTimers)
	return resp, nil
}

// GetHistory implements api.Service.
//
// With WaitForNew the call blocks until an event beyond FromEventID exists. The
// wait is event driven -- a local write closes the waiter's channel -- with a
// slow re-check behind it so that a write on another replica is still noticed.
// Neither path spins.
func (e *Engine) GetHistory(ctx context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return api.GetHistoryResponse{}, err
	}
	from := req.FromEventID
	if from <= 0 {
		from = 1
	}

	for {
		done, resp, err := e.getHistoryOnce(ctx, namespace, req, from)
		if done || err != nil {
			return resp, err
		}
	}
}

// getHistoryOnce is one iteration of the long poll: read, and if there is
// nothing to report, wait for a reason to look again. It is a separate function
// so that the subscription is released on every exit path by a single defer.
func (e *Engine) getHistoryOnce(ctx context.Context, namespace string, req api.GetHistoryRequest, from int64) (bool, api.GetHistoryResponse, error) {
	rec, err := e.store.GetExecution(ctx, namespace, req.WorkflowID, req.RunID)
	if err != nil {
		return true, api.GetHistoryResponse{}, mapError(err)
	}
	// Subscribe before reading, so that a write landing between the read and
	// the wait cannot be missed.
	notified, release := e.notifier.subscribe(cacheKey(namespace, req.WorkflowID, rec.RunID))
	defer release()

	if rec.LastEventID >= from || !req.WaitForNew {
		events, err := e.store.ReadHistory(ctx, namespace, req.WorkflowID, rec.RunID, from, 0)
		if err != nil {
			return true, api.GetHistoryResponse{}, mapError(err)
		}
		if req.MaxEvents > 0 && len(events) > req.MaxEvents {
			events = events[:req.MaxEvents]
		}
		next := from
		if n := len(events); n > 0 {
			next = events[n-1].ID + 1
		}
		return true, api.GetHistoryResponse{Events: events, Status: rec.Status, NextEventID: next}, nil
	}
	if rec.Status.Terminal() {
		// Nothing more will ever be appended, so waiting would be a lie.
		return true, api.GetHistoryResponse{Status: rec.Status, NextEventID: rec.LastEventID + 1}, nil
	}

	recheck := e.clk.NewTimer(e.historyPoll)
	defer recheck.Stop()
	select {
	case <-notified:
	case <-recheck.C():
	case <-ctx.Done():
		return true, api.GetHistoryResponse{}, mapError(ctx.Err())
	case <-e.closed:
		return true, api.GetHistoryResponse{}, errorf(api.CodeUnavailable, "engine: shutting down")
	}
	return false, api.GetHistoryResponse{}, nil
}

// ListWorkflows implements api.Service.
func (e *Engine) ListWorkflows(ctx context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return api.ListWorkflowsResponse{}, err
	}
	filter := persistence.ListFilter{
		Namespace:    namespace,
		WorkflowID:   req.WorkflowID,
		WorkflowType: req.WorkflowType,
		PageSize:     req.PageSize,
		PageToken:    req.PageToken,
	}
	if req.Status != "" {
		var status skald.WorkflowStatus
		if err := status.UnmarshalText([]byte(req.Status)); err != nil {
			return api.ListWorkflowsResponse{}, errorf(api.CodeInvalidArgument, "%s", err)
		}
		filter.Status = &status
	}

	res, err := e.store.ListExecutions(ctx, filter)
	if err != nil {
		return api.ListWorkflowsResponse{}, mapError(err)
	}
	resp := api.ListWorkflowsResponse{NextPageToken: res.NextPageToken}
	for _, rec := range res.Records {
		item := api.DescribeWorkflowResponse{
			Namespace:           rec.Namespace,
			WorkflowID:          rec.WorkflowID,
			RunID:               rec.RunID,
			WorkflowType:        rec.WorkflowType,
			TaskQueue:           rec.TaskQueue,
			Status:              rec.Status,
			StartedAt:           rec.StartedAt,
			HistoryLength:       rec.LastEventID,
			FirstExecutionRunID: rec.FirstExecutionRunID,
			Memo:                rec.Memo,
			SearchAttrs:         rec.SearchAttrs,
		}
		if !rec.ClosedAt.IsZero() {
			closed := rec.ClosedAt
			item.ClosedAt = &closed
		}
		// Listing reads the visibility row only. Rebuilding every execution to
		// fill in pending activities would turn one page query into N history
		// reads, which is how a list endpoint becomes an outage.
		resp.Executions = append(resp.Executions, item)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Workflow tasks
// ---------------------------------------------------------------------------

// PollWorkflowTask implements api.Service.
func (e *Engine) PollWorkflowTask(ctx context.Context, req api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return api.WorkflowTask{}, err
	}
	if err := skald.ValidateTaskQueue(req.TaskQueue); err != nil {
		return api.WorkflowTask{}, mapError(err)
	}

	for {
		ref, ok, err := e.matcher.PollWorkflowTask(ctx, namespace, req.TaskQueue)
		if err != nil {
			return api.WorkflowTask{}, mapError(err)
		}
		if !ok {
			// An expired long poll is an empty task, never an error: an idle
			// worker is the normal state of a healthy deployment.
			return api.WorkflowTask{Empty: true}, nil
		}

		task, taken, err := e.startWorkflowTask(ctx, ref, req)
		if err != nil {
			if apiCode(err) == api.CodeNotFound {
				// The reference outlived its execution, which retention can do.
				// Dropping it is the whole point of references being derived.
				continue
			}
			return api.WorkflowTask{}, err
		}
		if taken {
			return task, nil
		}
		// The reference was stale -- the task was completed, timed out or
		// superseded before this poller got to it. Keep waiting rather than
		// handing the worker an empty response it would immediately re-poll on.
	}
}

// startWorkflowTask converts a matching reference into a started workflow task.
// It reports false when the reference no longer names a startable task.
func (e *Engine) startWorkflowTask(ctx context.Context, ref matching.Task, req api.PollWorkflowTaskRequest) (api.WorkflowTask, bool, error) {
	var task api.WorkflowTask
	requestID := req.RequestID
	if requestID == "" {
		requestID = e.newID()
	}

	err := e.mutate(ctx, ref.Namespace, ref.Execution.WorkflowID, ref.Execution.RunID, func(st *cachedState) (outcome, error) {
		// Reset on every attempt: the write path may run this body again after
		// losing a version race, and a half-filled task from the losing attempt
		// must not survive.
		task = api.WorkflowTask{}
		ms := st.ms
		wt := ms.WorkflowTask()
		if ms.Status.Terminal() || !wt.Scheduled() || wt.ScheduledEventID != ref.ScheduledEventID {
			return outcome{noop: true}, nil
		}
		started, err := ms.StartWorkflowTask(req.Identity, requestID)
		if err != nil {
			return outcome{}, err
		}
		task = api.WorkflowTask{
			Namespace:        ms.Namespace,
			Execution:        ms.Execution,
			WorkflowType:     ms.WorkflowType,
			TaskQueue:        ref.TaskQueue,
			ScheduledEventID: ref.ScheduledEventID,
			StartedEventID:   started.ID,
			Attempt:          ms.WorkflowTask().Attempt,
			// The whole history through the started event. Skald never sends a
			// delta: sticky caching is an SDK optimisation, and correctness must
			// not depend on a cache being warm.
			History: append(history.History(nil), ms.Events()...),
		}
		return outcome{}, nil
	})
	if err != nil {
		return api.WorkflowTask{}, false, err
	}
	return task, task.StartedEventID != 0, nil
}

// RespondWorkflowTaskCompleted implements api.Service.
func (e *Engine) RespondWorkflowTaskCompleted(ctx context.Context, req api.RespondWorkflowTaskCompletedRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	if req.Execution.WorkflowID == "" {
		return errorf(api.CodeInvalidArgument, "engine: response names no workflow")
	}
	return e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		ms := st.ms
		if err := e.checkTaskOwnership(ms, req.Identity); err != nil {
			return outcome{}, err
		}
		// Anything appended between the started event and the completion
		// arrived while the worker was thinking: a signal, an activity result,
		// a timer fire. Those transitions all tried to wake the workflow and
		// found a task already in flight, so they wrote their event and
		// returned no effect. Noticing the gap here is what turns that into a
		// fresh task -- without it a signal delivered mid-task would sit in the
		// history unread until something else happened to wake the workflow.
		startedEventID := ms.WorkflowTask().StartedEventID
		buffered := ms.NextEventID()-startedEventID > 1

		effects, err := ms.CompleteWorkflowTask(req.Identity, req.SDKName, req.SDKVersion, req.Commands)
		if err != nil {
			return outcome{}, err
		}
		if buffered && !ms.Status.Terminal() {
			eff, err := ms.ScheduleWorkflowTask()
			if err != nil {
				return outcome{}, err
			}
			if eff != nil {
				effects = append(effects, *eff)
			}
		}
		return outcome{effects: effects}, nil
	})
}

// RespondWorkflowTaskFailed implements api.Service.
func (e *Engine) RespondWorkflowTaskFailed(ctx context.Context, req api.RespondWorkflowTaskFailedRequest) error {
	namespace, err := e.resolveNamespace(req.Namespace)
	if err != nil {
		return err
	}
	return e.mutate(ctx, namespace, req.Execution.WorkflowID, req.Execution.RunID, func(st *cachedState) (outcome, error) {
		if err := e.checkTaskOwnership(st.ms, req.Identity); err != nil {
			return outcome{}, err
		}
		effects, err := st.ms.FailWorkflowTask(req.Cause, req.Failure, req.Identity)
		return outcome{effects: effects}, err
	})
}

// checkTaskOwnership rejects a response from a worker that no longer holds the
// task.
//
// The common cause is a worker that stalled past its task timeout: the engine
// timed the task out and scheduled a replacement, and the original worker
// finally came back with commands computed against history that has since
// moved. Applying them would corrupt the execution, so the response is refused
// with a precondition failure, which the SDK turns into "drop these commands
// and replay".
func (e *Engine) checkTaskOwnership(ms *execution.MutableState, identity string) error {
	if ms.Status.Terminal() {
		return errorf(api.CodeFailedPrecondition, "engine: workflow %s is already %s", ms.Execution, ms.Status)
	}
	wt := ms.WorkflowTask()
	if !wt.Started() {
		return errorf(api.CodeFailedPrecondition,
			"engine: no workflow task is in flight for %s; the worker no longer holds it", ms.Execution)
	}
	// Best-effort ownership check for the harder case: the original task timed
	// out, a *replacement* was started by another worker, and the original
	// worker finally answered. The request carries no started event id, so the
	// identity recorded on the started event is the only discriminator
	// available. Two workers sharing an identity defeat it, which is why the
	// check is defence in depth rather than the guarantee -- see the note in
	// the package documentation about the protocol gap.
	if identity != "" {
		if ev, ok := ms.Events().Get(wt.StartedEventID); ok {
			if attrs, ok := history.AttributesAs[history.WorkflowTaskStartedAttributes](ev); ok &&
				attrs.Identity != "" && attrs.Identity != identity {
				return errorf(api.CodeFailedPrecondition,
					"engine: workflow task %d is held by %q, not %q", wt.StartedEventID, attrs.Identity, identity)
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func (e *Engine) resolveNamespace(ns string) (string, error) {
	if ns == "" {
		ns = e.defaultNS
	}
	if err := skald.ValidateNamespace(ns); err != nil {
		return "", mapError(err)
	}
	return ns, nil
}

// apiCode extracts the api.Error code from an error, or "" when it carries none.
func apiCode(err error) string {
	var apiErr *api.Error
	if errors.As(mapError(err), &apiErr) {
		return apiErr.Code
	}
	return ""
}

// sortPending gives Describe a stable order. Maps iterate randomly, and a
// response whose field order changes between identical calls is miserable to
// diff in a CLI or a test.
func sortPending(acts []api.PendingActivity, timers []api.PendingTimer) {
	for i := 1; i < len(acts); i++ {
		for j := i; j > 0 && acts[j].ScheduledEventID < acts[j-1].ScheduledEventID; j-- {
			acts[j], acts[j-1] = acts[j-1], acts[j]
		}
	}
	for i := 1; i < len(timers); i++ {
		for j := i; j > 0 && timers[j].StartedEventID < timers[j-1].StartedEventID; j-- {
			timers[j], timers[j-1] = timers[j-1], timers[j]
		}
	}
}
