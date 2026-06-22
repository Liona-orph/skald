// Package worker hosts workflow and activity code and connects it to a Skald
// service.
//
// A worker is a long-poll loop with a scheduler behind it. It asks the service
// for work, runs the registered implementation, and reports the outcome:
//
//	w := worker.New(client, "orders", worker.Options{})
//	w.RegisterWorkflow(TransferMoney)
//	w.RegisterActivity(Withdraw)
//	w.RegisterActivity(Deposit)
//	if err := w.Run(ctx); err != nil { log.Fatal(err) }
//
// The service is an api.Service, which is implemented by the HTTP client, by the
// in-process engine and by a test double alike. Pointing a worker at an embedded
// engine gives a complete durable execution system in one process with no server
// and no second code path, which is what makes the integration tests in this
// package real tests rather than mocks.
//
// # What the worker guarantees
//
//   - Signatures are validated at registration, so a malformed workflow fails at
//     startup rather than the first time it is scheduled.
//   - Workflow tasks for one execution never run concurrently in this process.
//   - Every workflow instance is unwound cleanly on eviction and on shutdown, so
//     a stopped worker leaks no goroutines.
//   - Stop drains: in-flight tasks are given the caller's deadline to finish.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	internalwf "github.com/skald-io/skald/internal/workflow"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// Worker polls one task queue and executes the work it receives.
type Worker struct {
	service   api.Service
	taskQueue string
	opts      Options
	registry  *Registry
	cache     *internalwf.Cache
	log       *slog.Logger

	// pollCtx is cancelled first on Stop so that pollers stop taking new work
	// while running tasks finish. taskCtx is cancelled only when the drain
	// deadline expires.
	pollCtx    context.Context
	pollCancel context.CancelFunc
	taskCtx    context.Context
	taskCancel context.CancelFunc

	workflowSlots chan struct{}
	activitySlots chan struct{}

	wg sync.WaitGroup

	mu      sync.Mutex
	started bool
	stopped bool
}

// New returns a worker for one task queue. It does not start polling; call
// Start or Run.
func New(service api.Service, taskQueue string, opts Options) *Worker {
	opts.applyDefaults(defaultIdentity())
	cache, err := internalwf.NewCache(opts.StickyCacheSize)
	if err != nil {
		// NewCache only fails on a non-positive size, which applyDefaults has
		// already ruled out.
		panic(err)
	}
	return &Worker{
		service:       service,
		taskQueue:     taskQueue,
		opts:          opts,
		registry:      NewRegistry(opts.DataConverter),
		cache:         cache,
		log:           opts.Logger.With("component", "worker", "task_queue", taskQueue),
		workflowSlots: make(chan struct{}, opts.MaxConcurrentWorkflowTasks),
		activitySlots: make(chan struct{}, opts.MaxConcurrentActivityTasks),
	}
}

func defaultIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s@%d", host, os.Getpid())
}

// Registry exposes the worker's registry, mostly so that a test can swap an
// implementation between tasks.
func (w *Worker) Registry() *Registry { return w.registry }

// TaskQueue returns the queue this worker serves.
func (w *Worker) TaskQueue() string { return w.taskQueue }

// RegisterWorkflow registers a workflow implementation and panics if its
// signature is wrong.
//
// Panicking is deliberate. A registration error is a programming mistake that a
// caller cannot meaningfully handle, and returning an error invites the
// `_ = w.RegisterWorkflow(...)` that turns a startup failure into a workflow
// type that silently does not exist until the first task for it arrives.
func (w *Worker) RegisterWorkflow(fn any) {
	if err := w.registry.RegisterWorkflow(fn, RegisterOptions{}); err != nil {
		panic(err)
	}
}

// RegisterWorkflowWithOptions is RegisterWorkflow with an explicit name.
func (w *Worker) RegisterWorkflowWithOptions(fn any, opts RegisterOptions) {
	if err := w.registry.RegisterWorkflow(fn, opts); err != nil {
		panic(err)
	}
}

// RegisterActivity registers an activity implementation and panics if its
// signature is wrong.
func (w *Worker) RegisterActivity(fn any) {
	if err := w.registry.RegisterActivity(fn, RegisterOptions{}); err != nil {
		panic(err)
	}
}

// RegisterActivityWithOptions is RegisterActivity with an explicit name.
func (w *Worker) RegisterActivityWithOptions(fn any, opts RegisterOptions) {
	if err := w.registry.RegisterActivity(fn, opts); err != nil {
		panic(err)
	}
}

// Start begins polling in the background.
func (w *Worker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		// Checked before "already started" so that a caller who stopped the
		// worker gets the more specific diagnosis. A stopped worker cannot be
		// restarted: its sticky cache has been purged and its contexts are
		// cancelled, so a second Start would silently produce a worker that
		// polls with a dead context and never reports why.
		return errors.New("worker: cannot restart a stopped worker; build a new one")
	}
	if w.started {
		return errors.New("worker: already started")
	}
	if err := skald.ValidateTaskQueue(w.taskQueue); err != nil {
		return err
	}
	if w.opts.DisableWorkflowWorker && w.opts.DisableActivityWorker {
		return errors.New("worker: both the workflow and the activity worker are disabled; " +
			"this worker would do nothing")
	}
	w.started = true

	w.taskCtx, w.taskCancel = context.WithCancel(context.Background())
	w.pollCtx, w.pollCancel = context.WithCancel(w.taskCtx)

	if !w.opts.DisableWorkflowWorker {
		for i := 0; i < w.opts.WorkflowTaskPollers; i++ {
			w.wg.Add(1)
			go func(id int) {
				defer w.wg.Done()
				w.pollWorkflowTasks(id)
			}(i)
		}
	}
	if !w.opts.DisableActivityWorker {
		for i := 0; i < w.opts.ActivityTaskPollers; i++ {
			w.wg.Add(1)
			go func(id int) {
				defer w.wg.Done()
				w.pollActivityTasks(id)
			}(i)
		}
	}
	w.log.Info("worker started",
		"identity", w.opts.Identity,
		"workflows", w.registry.WorkflowNames(),
		"activities", w.registry.ActivityNames())
	return nil
}

// Run starts the worker and blocks until ctx is cancelled, then stops it.
func (w *Worker) Run(ctx context.Context) error {
	//nolint:contextcheck // Start deliberately takes no context: a worker's
	// lifetime is ended by Stop, not by whatever context started it.
	if err := w.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultStopTimeout)
	defer cancel()
	return w.Stop(stopCtx)
}

// Stop drains the worker.
//
// Polling stops immediately; tasks already running are given until ctx expires
// to finish, because a workflow task that is abandoned mid-flight becomes a
// server-side task timeout and a wasted replay. When the deadline passes the
// remaining tasks are cancelled and Stop returns an error saying so, which is
// information a deployment system can act on rather than a silent truncation.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.started || w.stopped {
		w.stopped = true
		w.mu.Unlock()
		return nil
	}
	w.stopped = true
	w.mu.Unlock()

	w.pollCancel()

	drained := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(drained)
	}()

	var err error
	select {
	case <-drained:
	case <-ctx.Done():
		w.taskCancel()
		<-drained
		err = fmt.Errorf("worker: shutdown deadline expired with tasks still running; "+
			"they were cancelled: %w", ctx.Err())
	}
	w.taskCancel()
	// Purging the sticky cache unwinds every warm workflow instance. Without it
	// a stopped worker would leave one parked goroutine per cached coroutine.
	w.cache.Clear()
	w.log.Info("worker stopped")
	return err
}

// CacheSize reports how many warm workflow instances are held. Tests use it to
// prove eviction actually happens.
func (w *Worker) CacheSize() int { return w.cache.Len() }

// EvictAll drops every warm workflow instance, unwinding their coroutines.
//
// It exists so that a test can prove the invariant the whole design rests on:
// correctness must never depend on a cache hit. Evicting between every task
// turns every task into a cold replay, and the results must be identical.
func (w *Worker) EvictAll() { w.cache.Clear() }

// ---------------------------------------------------------------------------
// Workflow tasks
// ---------------------------------------------------------------------------

func (w *Worker) handleWorkflowTask(task api.WorkflowTask) {
	runID := task.Execution.RunID
	log := w.log.With("workflow_id", task.Execution.WorkflowID, "run_id", runID,
		"workflow_type", task.WorkflowType, "started_event_id", task.StartedEventID)

	var exec *internalwf.Executor
	var warm bool
	if !w.opts.DisableStickyCache {
		exec, warm = w.cache.Take(runID)
	}
	if warm && exec.LastEventID() >= task.StartedEventID {
		// The instance has already lived through this task. That happens when a
		// previous response never reached the server and the task was retried:
		// replaying it against an instance that is already past it would be a
		// lie. Throw it away and rebuild from history.
		_ = exec.Close()
		exec, warm = nil, false
	}
	if !warm {
		var err error
		exec, err = w.newExecutor(task)
		if err != nil {
			log.Error("workflow type is not registered", "error", err)
			w.failWorkflowTask(task, history.WorkflowTaskFailedCauseUnregisteredType,
				skald.AsApplicationError(err))
			return
		}
	}

	result, err := exec.ProcessTask(task)
	if err != nil {
		_ = exec.Close()
		w.cache.Evict(runID)
		cause := internalwf.FailureCause(err)
		log.Error("workflow task failed", "cause", cause.String(), "error", err)
		w.failWorkflowTask(task, cause, internalwf.FailureDetail(err))
		return
	}

	if err := w.service.RespondWorkflowTaskCompleted(w.taskCtx, api.RespondWorkflowTaskCompletedRequest{
		Namespace:  w.opts.Namespace,
		Execution:  task.Execution,
		Commands:   result.Commands,
		Identity:   w.opts.Identity,
		SDKName:    SDKName,
		SDKVersion: SDKVersion,
	}); err != nil {
		// The server refused the batch, most often because this worker no longer
		// holds the task. The instance has advanced past state the server did not
		// accept, so it must not serve another task.
		log.Warn("workflow task response rejected", "error", err)
		_ = exec.Close()
		w.cache.Evict(runID)
		return
	}

	if result.Finished || w.opts.DisableStickyCache {
		_ = exec.Close()
		w.cache.Evict(runID)
		return
	}
	w.cache.Put(runID, exec)
}

func (w *Worker) newExecutor(task api.WorkflowTask) (*internalwf.Executor, error) {
	fn, err := w.registry.WorkflowFunc(task.WorkflowType)
	if err != nil {
		return nil, err
	}
	exec, err := internalwf.NewExecutor(internalwf.ExecutorOptions{
		Fn:              fn,
		Converter:       w.opts.DataConverter,
		Logger:          w.opts.Logger,
		DeadlockTimeout: w.opts.DeadlockDetectionTimeout,
	})
	if err != nil {
		return nil, err
	}
	exec.SetExecutionInfo(w.opts.Namespace, task.Execution)
	return exec, nil
}

func (w *Worker) failWorkflowTask(task api.WorkflowTask, cause history.WorkflowTaskFailedCause, failure *skald.ApplicationError) {
	if err := w.service.RespondWorkflowTaskFailed(w.taskCtx, api.RespondWorkflowTaskFailedRequest{
		Namespace: w.opts.Namespace,
		Execution: task.Execution,
		Cause:     cause,
		Failure:   failure,
		Identity:  w.opts.Identity,
	}); err != nil {
		w.log.Warn("could not report workflow task failure", "error", err)
	}
	// The engine re-schedules a failed task at once, which is right -- a rolled
	// back deploy must be picked up immediately -- but without a pause here a
	// permanently failing task would spin a core for as long as the bad build is
	// deployed.
	sleepCtx(w.taskCtx, w.opts.FailedTaskBackoff)
}

// ---------------------------------------------------------------------------
// Activity tasks
// ---------------------------------------------------------------------------

func (w *Worker) handleActivityTask(task api.ActivityTask) {
	log := w.log.With("workflow_id", task.Execution.WorkflowID, "run_id", task.Execution.RunID,
		"activity_id", task.ActivityID, "activity_type", task.ActivityType, "attempt", task.Attempt)

	fn, err := w.registry.ActivityFunc(task.ActivityType)
	if err != nil {
		log.Error("activity type is not registered", "error", err)
		w.respondActivityFailed(task, &skald.ApplicationError{
			Type:    "NotRegistered",
			Message: err.Error(),
			// Retryable on purpose: the usual cause is a deploy in which the
			// workflow rolled out before the worker that implements the new
			// activity, and that fixes itself minutes later.
		})
		return
	}

	ctx, cancel := w.activityContext(task)
	defer cancel()

	env := &activityEnv{
		info:                 activityInfo(w.opts.Namespace, w.taskQueue, task),
		conv:                 w.opts.DataConverter,
		lastHeartbeatDetails: task.LastHeartbeatDetails,
	}
	env.heartbeat = newHeartbeater(w.taskCtx, w, task, cancel)
	defer env.heartbeat.stop()
	ctx = withActivityEnv(ctx, env)

	result, err := executeActivity(ctx, fn, task.Input)
	switch {
	case err == nil:
		if respErr := w.service.RespondActivityTaskCompleted(w.taskCtx, api.RespondActivityTaskCompletedRequest{
			Namespace:        w.opts.Namespace,
			Execution:        task.Execution,
			ScheduledEventID: task.ScheduledEventID,
			Result:           result,
			Identity:         w.opts.Identity,
		}); respErr != nil {
			log.Warn("could not report activity completion", "error", respErr)
		}
	case isCanceled(ctx, err):
		if respErr := w.service.RespondActivityTaskCanceled(w.taskCtx, api.RespondActivityTaskCanceledRequest{
			Namespace:        w.opts.Namespace,
			Execution:        task.Execution,
			ScheduledEventID: task.ScheduledEventID,
			Details:          canceledDetails(w.opts.DataConverter, err),
			Identity:         w.opts.Identity,
		}); respErr != nil {
			log.Warn("could not report activity cancellation", "error", respErr)
		}
	default:
		log.Info("activity failed", "error", err)
		w.respondActivityFailed(task, skald.AsApplicationError(err))
	}
}

// activityContext derives the activity's context from the server's deadline.
//
// Bounding the activity by the deadline the *server* published, rather than by
// a locally configured timeout, is what stops a doomed attempt from burning
// resources after the server has already given up on it and scheduled a retry.
func (w *Worker) activityContext(task api.ActivityTask) (context.Context, context.CancelFunc) {
	if task.Deadline.IsZero() {
		return context.WithCancel(w.taskCtx)
	}
	return context.WithDeadline(w.taskCtx, task.Deadline)
}

func (w *Worker) respondActivityFailed(task api.ActivityTask, failure *skald.ApplicationError) {
	if err := w.service.RespondActivityTaskFailed(w.taskCtx, api.RespondActivityTaskFailedRequest{
		Namespace:        w.opts.Namespace,
		Execution:        task.Execution,
		ScheduledEventID: task.ScheduledEventID,
		Failure:          failure,
		Identity:         w.opts.Identity,
	}); err != nil {
		w.log.Warn("could not report activity failure", "error", err)
	}
}

func activityInfo(namespace, taskQueue string, task api.ActivityTask) ActivityInfo {
	return ActivityInfo{
		Namespace:    namespace,
		Execution:    task.Execution,
		WorkflowType: task.WorkflowType,
		ActivityID:   task.ActivityID,
		ActivityType: task.ActivityType,
		// The activity task carries no queue of its own, so the queue this
		// worker polls is by definition the one it was dispatched on.
		TaskQueue:        taskQueue,
		ScheduledEventID: task.ScheduledEventID,
		StartedEventID:   task.StartedEventID,
		Attempt:          task.Attempt,
		ScheduledAt:      task.ScheduledAt,
		StartedAt:        task.StartedAt,
		Deadline:         task.Deadline,
		HeartbeatTimeout: task.HeartbeatTimeout,
	}
}

// isCanceled reports whether an activity stopped because it was asked to.
func isCanceled(ctx context.Context, err error) bool {
	var canceled *skald.CanceledError
	if errors.As(err, &canceled) {
		return true
	}
	// A context cancellation that the activity simply propagated counts too, but
	// only when the context really was cancelled: an activity that returns
	// context.Canceled for its own reasons has failed, not been cancelled.
	return errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled)
}

func canceledDetails(conv skald.DataConverter, err error) *skald.Payload {
	var canceled *skald.CanceledError
	if errors.As(err, &canceled) && canceled.Details != nil {
		return canceled.Details
	}
	p, encErr := conv.ToPayload(err.Error())
	if encErr != nil {
		return nil
	}
	return p
}

// sleepCtx sleeps for d unless ctx is done first. It reports whether the full
// duration elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
