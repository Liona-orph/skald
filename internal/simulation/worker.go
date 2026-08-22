package simulation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
	"github.com/Liona-orph/skald/pkg/worker"
)

// simWorker is one worker process, minus its poll loop.
//
// Everything a worker does with a task once it has one is real: the registry
// that resolves the type, the sticky cache that decides warm from cold, the
// replay executor that turns history plus new events into a command batch, the
// data converter. What is missing is the part that has to be missing -- the
// goroutines that poll, the semaphores that bound concurrency, the RPC calls --
// because those are exactly where the Go scheduler would decide the interleaving
// and make a run unreproducible.
//
// The simulator supplies that part itself: it chooses which worker gets which
// task, when, and whether the response ever reaches the server. pkg/worker's own
// loop is covered by its unit tests and by test/integration; this type exists to
// remove the nondeterminism, not to avoid the code.
type simWorker struct {
	id        int
	identity  string
	namespace string
	registry  *worker.Registry
	cache     *internalwf.Cache
	conv      skald.DataConverter
	log       *slog.Logger
	deadlock  time.Duration

	// crashes counts how many times this worker has been restarted. It appears
	// in the identity so that the engine's task-ownership check can tell a
	// response from the process that took the task from one produced by its
	// replacement -- which is precisely the situation a crash creates.
	crashes int
}

// newSimWorker builds a worker with every simulation workflow and activity
// registered. Registration failures panic: they are programming errors in this
// package, and a simulator that silently ran four of its five workflows would
// be worse than useless.
func newSimWorker(id int, namespace string, conv skald.DataConverter, log *slog.Logger) *simWorker {
	reg := worker.NewRegistry(conv)
	must := func(err error) {
		if err != nil {
			panic(fmt.Sprintf("simulation: registering worker implementations: %v", err))
		}
	}
	must(reg.RegisterWorkflow(SequentialWorkflow, worker.RegisterOptions{Name: WorkflowSequential}))
	must(reg.RegisterWorkflow(FanOutWorkflow, worker.RegisterOptions{Name: WorkflowFanOut}))
	must(reg.RegisterWorkflow(TimerWorkflow, worker.RegisterOptions{Name: WorkflowTimer}))
	must(reg.RegisterWorkflow(SignalWorkflow, worker.RegisterOptions{Name: WorkflowSignal}))
	must(reg.RegisterWorkflow(ContinueAsNewWorkflow, worker.RegisterOptions{Name: WorkflowContinue}))
	must(reg.RegisterActivity(Step, worker.RegisterOptions{Name: ActivityStep}))
	must(reg.RegisterActivity(Square, worker.RegisterOptions{Name: ActivitySquare}))

	cache, err := internalwf.NewCache(8)
	if err != nil {
		panic(err)
	}
	w := &simWorker{
		id:        id,
		namespace: namespace,
		registry:  reg,
		cache:     cache,
		conv:      conv,
		log:       log,
		// Generous, because a simulated worker shares its goroutine budget with
		// the rest of the run and a race-enabled build is slow. A deadlock in
		// workflow code shows up as a hung run either way.
		deadlock: 30 * time.Second,
	}
	w.setIdentity()
	return w
}

func (w *simWorker) setIdentity() {
	w.identity = fmt.Sprintf("worker-%d/%d", w.id, w.crashes)
}

// crash discards everything the worker was holding.
//
// A crash is not a graceful stop: the sticky cache goes, every warm workflow
// instance is unwound without finishing its task, and any response the worker
// was about to send is never sent. The identity changes because the replacement
// is, from the engine's point of view, a different process -- and the engine's
// ownership check is one of the mechanisms under test.
func (w *simWorker) crash() {
	w.cache.Clear()
	w.crashes++
	w.setIdentity()
}

// workflowOutcome is what one workflow task produced, before anything is sent.
type workflowOutcome struct {
	// exec is the instance that produced the batch, still owned by the caller.
	// It is nil when the task failed.
	exec   *internalwf.Executor
	result internalwf.TaskResult

	failed  bool
	cause   history.WorkflowTaskFailedCause
	failure *skald.ApplicationError
	err     error
}

// runWorkflowTask executes one workflow task and returns the response the worker
// would send. It does not send it: the simulator does that, so that it can lose
// the response, duplicate it, or crash the worker in between.
func (w *simWorker) runWorkflowTask(task api.WorkflowTask) workflowOutcome {
	runID := task.Execution.RunID

	exec, warm := w.cache.Take(runID)
	if warm && exec.LastEventID() >= task.StartedEventID {
		// The instance already lived through this task, which happens when a
		// previous response never reached the server. Replaying against state
		// that is already past the task would be a lie; rebuild from history.
		_ = exec.Close()
		exec, warm = nil, false
	}
	if !warm {
		fn, err := w.registry.WorkflowFunc(task.WorkflowType)
		if err != nil {
			return workflowOutcome{
				failed:  true,
				cause:   history.WorkflowTaskFailedCauseUnregisteredType,
				failure: skald.AsApplicationError(err),
				err:     err,
			}
		}
		exec, err = internalwf.NewExecutor(internalwf.ExecutorOptions{
			Fn:              fn,
			Converter:       w.conv,
			Logger:          w.log,
			DeadlockTimeout: w.deadlock,
		})
		if err != nil {
			return workflowOutcome{
				failed:  true,
				cause:   history.WorkflowTaskFailedCauseUnspecified,
				failure: skald.AsApplicationError(err),
				err:     err,
			}
		}
		exec.SetExecutionInfo(w.namespace, task.Execution)
	}

	result, err := exec.ProcessTask(task)
	if err != nil {
		_ = exec.Close()
		w.cache.Evict(runID)
		return workflowOutcome{
			failed:  true,
			cause:   internalwf.FailureCause(err),
			failure: internalwf.FailureDetail(err),
			err:     err,
		}
	}
	return workflowOutcome{exec: exec, result: result}
}

// settleWorkflowTask does what the real worker does once it knows whether the
// server accepted the batch.
//
// A rejected response means the instance has advanced past state the server did
// not take, so it must never serve another task. A dropped response is the same
// situation seen from the other side and is handled identically.
func (w *simWorker) settleWorkflowTask(runID string, out workflowOutcome, accepted bool) {
	if out.exec == nil {
		return
	}
	if !accepted || out.result.Finished {
		_ = out.exec.Close()
		w.cache.Evict(runID)
		return
	}
	w.cache.Put(runID, out.exec)
}

// runActivity executes one activity attempt.
func (w *simWorker) runActivity(ctx context.Context, task api.ActivityTask) (*skald.Payload, error) {
	fn, err := w.registry.ActivityFunc(task.ActivityType)
	if err != nil {
		return nil, err
	}
	return fn(ctx, task.Input)
}

// close releases every warm instance. Skipping it would leave one parked
// goroutine per cached coroutine for the life of the test binary, which under
// -race turns a few hundred simulated seeds into a goroutine leak large enough
// to matter.
func (w *simWorker) close() { w.cache.Clear() }
