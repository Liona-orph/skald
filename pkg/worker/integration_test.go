package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
	"github.com/Liona-orph/skald/pkg/workflow"
)

// These are end-to-end tests: a real engine over a real store, a real worker,
// real durable timers and the real command protocol, all in one process.
//
// The scenarios were chosen to cover the failure modes durable execution exists
// to survive, not to cover lines: a worker that dies mid-execution, an activity
// that fails and is retried, a cancellation that has to unwind, and a deploy
// that changes a workflow incompatibly.

// ---------------------------------------------------------------------------
// Shared activities
// ---------------------------------------------------------------------------

func Echo(ctx context.Context, s string) (string, error) { return s, nil }

func Upper(ctx context.Context, s string) (string, error) {
	return fmt.Sprintf("%s!", s), nil
}

func defaultActivityOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &skald.RetryPolicy{MaximumAttempts: 1},
	})
}

// ---------------------------------------------------------------------------
// Sequential activities
// ---------------------------------------------------------------------------

func SequentialWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = defaultActivityOptions(ctx)
	var echoed string
	if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, Echo, name), &echoed); err != nil {
		return "", err
	}
	upper, err := workflow.ExecuteActivityAs[string](ctx, Upper, echoed).Get(ctx)
	if err != nil {
		return "", err
	}
	return upper, nil
}

func TestIntegrationSequentialActivities(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(SequentialWorkflow)
	e.worker.RegisterActivity(Echo)
	e.worker.RegisterActivity(Upper)
	e.start()

	runID := e.startWorkflow("seq-1", "SequentialWorkflow", "hello")
	var out string
	e.awaitResult("seq-1", runID, &out)
	require.Equal(t, "hello!", out)

	// Two activities, one after the other: four workflow tasks in total (start,
	// after each activity, and the one that closes).
	require.Equal(t, 2, e.countEvents("seq-1", runID, history.EventTypeActivityTaskScheduled))
}

// ---------------------------------------------------------------------------
// Parallel activities with a selector
// ---------------------------------------------------------------------------

func Slow(ctx context.Context, id int) (int, error) {
	// A short real sleep so that the three attempts genuinely overlap.
	select {
	case <-time.After(time.Duration(id) * 20 * time.Millisecond):
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return id, nil
}

// FanOutWorkflow starts three activities at once and returns the first result,
// using a selector rather than a Go select so that the choice is replayable.
func FanOutWorkflow(ctx workflow.Context) (int, error) {
	ctx = defaultActivityOptions(ctx)

	futures := make([]workflow.Future[int], 3)
	selector := workflow.NewSelector(ctx)
	var first int
	var firstErr error
	for i := range futures {
		id := i + 1
		futures[i] = workflow.ExecuteActivityAs[int](ctx, Slow, id)
		f := futures[i]
		selector.AddFuture(f, func() { first, firstErr = f.Get(ctx) })
	}
	selector.Select(ctx)
	if firstErr != nil {
		return 0, firstErr
	}

	// Then wait for the rest, proving all three really were in flight together.
	total := 0
	for _, f := range futures {
		v, err := f.Get(ctx)
		if err != nil {
			return 0, err
		}
		total += v
	}
	return first*100 + total, nil
}

func TestIntegrationParallelActivitiesWithSelector(t *testing.T) {
	e := newTestEnv(t, func(o *Options) { o.MaxConcurrentActivityTasks = 4; o.ActivityTaskPollers = 3 })
	e.worker.RegisterWorkflow(FanOutWorkflow)
	e.worker.RegisterActivity(Slow)
	e.start()

	runID := e.startWorkflow("fan-1", "FanOutWorkflow")
	var out int
	e.awaitResult("fan-1", runID, &out)
	require.Equal(t, 1*100+6, out, "the fastest activity wins the selector and all three complete")

	// All three were scheduled in a single batch, which is the point of a
	// fan-out: one workflow task, three activities.
	events := e.history("fan-1", runID)
	firstCompleted := int64(0)
	for _, ev := range events {
		if ev.Type() == history.EventTypeWorkflowTaskCompleted {
			firstCompleted = ev.ID
			break
		}
	}
	scheduledInFirstBatch := 0
	for _, ev := range events {
		if a, ok := history.AttributesAs[history.ActivityTaskScheduledAttributes](ev); ok &&
			a.WorkflowTaskCompletedEventID == firstCompleted {
			scheduledInFirstBatch++
		}
	}
	require.Equal(t, 3, scheduledInFirstBatch)
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

func TimerWorkflow(ctx workflow.Context, sleepMillis int) (string, error) {
	start := workflow.Now(ctx)
	if err := workflow.Sleep(ctx, time.Duration(sleepMillis)*time.Millisecond); err != nil {
		return "", err
	}
	elapsed := workflow.Now(ctx).Sub(start)
	if elapsed < time.Duration(sleepMillis)*time.Millisecond {
		return "", fmt.Errorf("workflow clock only advanced %s", elapsed)
	}
	return "slept", nil
}

func TestIntegrationDurableTimer(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(TimerWorkflow)
	e.start()

	runID := e.startWorkflow("timer-1", "TimerWorkflow", 150)
	var out string
	e.awaitResult("timer-1", runID, &out)
	require.Equal(t, "slept", out)
	require.Equal(t, 1, e.countEvents("timer-1", runID, history.EventTypeTimerStarted))
	require.Equal(t, 1, e.countEvents("timer-1", runID, history.EventTypeTimerFired))
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

func SignalWorkflow(ctx workflow.Context) ([]string, error) {
	ch := workflow.GetSignalChannel[string](ctx, "add")
	var got []string
	for {
		v, ok := ch.Receive(ctx)
		if !ok {
			break
		}
		if v == "done" {
			break
		}
		got = append(got, v)
	}
	return got, nil
}

func TestIntegrationSignals(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(SignalWorkflow)
	e.start()

	runID := e.startWorkflow("sig-1", "SignalWorkflow")
	// A signal sent before the workflow has run a single instruction must still
	// be delivered: it is durable in the history the moment the server accepts it.
	e.signal("sig-1", "add", "one")
	e.signal("sig-1", "add", "two")
	e.signal("sig-1", "add", "done")

	var out []string
	e.awaitResult("sig-1", runID, &out)
	require.Equal(t, []string{"one", "two"}, out)
}

func AwaitSignalWorkflow(ctx workflow.Context) (int, error) {
	ch := workflow.GetSignalChannel[int](ctx, "value")
	total := 0
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			v, ok := ch.Receive(ctx)
			if !ok {
				return
			}
			total += v
		}
	})
	if err := workflow.Await(ctx, func() bool { return total >= 10 }); err != nil {
		return 0, err
	}
	return total, nil
}

func TestIntegrationAwaitAcrossCoroutines(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(AwaitSignalWorkflow)
	e.start()

	runID := e.startWorkflow("await-1", "AwaitSignalWorkflow")
	for i := 0; i < 5; i++ {
		e.signal("await-1", "value", 3)
	}

	var out int
	e.awaitResult("await-1", runID, &out)
	require.GreaterOrEqual(t, out, 10)
}

// ApprovalWorkflow is the shape almost every real workflow eventually needs:
// wait for a human, but not forever. The selector makes the race deterministic
// and the timer is cancelled by the branch that won.
func ApprovalWorkflow(ctx workflow.Context) (string, error) {
	approvals := workflow.GetSignalChannel[string](ctx, "approve")

	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	timeout := workflow.NewTimer(timerCtx, time.Hour)

	var outcome string
	workflow.NewSelector(ctx).
		AddReceive(approvals, func() {
			who, _ := approvals.Receive(ctx)
			outcome = "approved by " + who
		}).
		AddFuture(timeout, func() { outcome = "timed out" }).
		Select(ctx)

	// Releasing the loser keeps a month-long timer from sitting in the store
	// after the workflow no longer cares about it.
	cancelTimer()
	return outcome, nil
}

func TestIntegrationSelectorOverSignalAndTimer(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(ApprovalWorkflow)
	e.start()

	runID := e.startWorkflow("approve-1", "ApprovalWorkflow")
	waitFor(t, "the timeout timer to be armed", func() bool {
		return e.countEvents("approve-1", runID, history.EventTypeTimerStarted) == 1
	})
	e.signal("approve-1", "approve", "alice")

	var out string
	e.awaitResult("approve-1", runID, &out)
	require.Equal(t, "approved by alice", out)
	require.Equal(t, 1, e.countEvents("approve-1", runID, history.EventTypeTimerCanceled))
	require.Zero(t, e.countEvents("approve-1", runID, history.EventTypeTimerFired))
}

// ---------------------------------------------------------------------------
// Worker crash mid-execution
// ---------------------------------------------------------------------------

var crashActivityCalls atomic.Int32

func StepOne(ctx context.Context) (string, error) {
	crashActivityCalls.Add(1)
	return "one", nil
}

func StepTwo(ctx context.Context, prev string) (string, error) {
	return prev + "+two", nil
}

// CrashWorkflow runs an activity, then a timer long enough that the test can
// kill the worker in between, then a second activity.
func CrashWorkflow(ctx workflow.Context) (string, error) {
	ctx = defaultActivityOptions(ctx)
	first, err := workflow.ExecuteActivityAs[string](ctx, StepOne).Get(ctx)
	if err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, 300*time.Millisecond); err != nil {
		return "", err
	}
	return workflow.ExecuteActivityAs[string](ctx, StepTwo, first).Get(ctx)
}

// TestIntegrationSurvivesAWorkerCrash is the headline property: the process that
// started the execution is not the process that finishes it, and nothing is lost
// or repeated.
func TestIntegrationSurvivesAWorkerCrash(t *testing.T) {
	crashActivityCalls.Store(0)

	e := newTestEnv(t)
	register := func(w *Worker) {
		w.RegisterWorkflow(CrashWorkflow)
		w.RegisterActivity(StepOne)
		w.RegisterActivity(StepTwo)
	}
	register(e.worker)
	e.start()

	runID := e.startWorkflow("crash-1", "CrashWorkflow")

	// Wait until the first activity has run and the timer is armed, so the
	// execution is genuinely mid-flight when the worker goes away.
	waitFor(t, "the durable timer to be armed", func() bool {
		return e.countEvents("crash-1", runID, history.EventTypeTimerStarted) == 1
	})

	// Kill the worker. Its sticky cache dies with it, so the replacement has to
	// rebuild the workflow's entire state from history.
	fresh := e.replaceWorker(register)
	require.Equal(t, 0, fresh.CacheSize())

	var out string
	e.awaitResult("crash-1", runID, &out)
	require.Equal(t, "one+two", out)
	require.Equal(t, int32(1), crashActivityCalls.Load(),
		"the first activity must not run again after the worker was replaced")
}

// TestIntegrationCorrectnessDoesNotDependOnTheStickyCache turns the cache off
// entirely, so every workflow task is a cold replay from event 1, and requires
// the result and the recorded history to be identical to the warm run.
//
// This is the invariant the whole caching design rests on. If it ever fails, the
// cache has stopped being an optimisation and become part of the semantics --
// which would mean a worker restart could change a workflow's behaviour.
func TestIntegrationCorrectnessDoesNotDependOnTheStickyCache(t *testing.T) {
	run := func(t *testing.T, sticky bool) (string, []history.EventType) {
		e := newTestEnv(t, func(o *Options) { o.DisableStickyCache = !sticky })
		e.worker.RegisterWorkflow(SideEffectWorkflow)
		e.worker.RegisterActivity(Echo)
		e.start()

		runID := e.startWorkflow("sticky-1", "SideEffectWorkflow")
		var out string
		e.awaitResult("sticky-1", runID, &out)
		if !sticky {
			require.Equal(t, 0, e.worker.CacheSize(), "nothing may be cached with the cache disabled")
		}
		return out, e.eventTypes("sticky-1", runID)
	}

	sideEffectCalls.Store(0)
	warmResult, warmHistory := run(t, true)
	sideEffectCalls.Store(0)
	coldResult, coldHistory := run(t, false)

	require.Equal(t, warmResult, coldResult)
	require.Equal(t, warmHistory, coldHistory,
		"a cold replay must write exactly the history a warm one wrote")
	require.Equal(t, int32(1), sideEffectCalls.Load(),
		"even with every task replaying from scratch, the side effect runs once")
}

// ---------------------------------------------------------------------------
// Activity retries
// ---------------------------------------------------------------------------

var flakyAttempts atomic.Int32

func Flaky(ctx context.Context) (int32, error) {
	n := flakyAttempts.Add(1)
	info := GetActivityInfo(ctx)
	if info.Attempt != n {
		return 0, skald.NewNonRetryableError("BadAttempt",
			"server said attempt %d, worker counted %d", info.Attempt, n)
	}
	if n < 3 {
		return 0, skald.NewApplicationError("Transient", "attempt %d failed", n)
	}
	return n, nil
}

func RetryWorkflow(ctx workflow.Context) (int32, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &skald.RetryPolicy{
			InitialInterval:    10 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumAttempts:    5,
		},
	})
	return workflow.ExecuteActivityAs[int32](ctx, Flaky).Get(ctx)
}

func TestIntegrationActivityRetries(t *testing.T) {
	flakyAttempts.Store(0)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(RetryWorkflow)
	e.worker.RegisterActivity(Flaky)
	e.start()

	runID := e.startWorkflow("retry-1", "RetryWorkflow")
	var out int32
	e.awaitResult("retry-1", runID, &out)
	require.Equal(t, int32(3), out)

	// Retries cost no history: one scheduled event, one started event, however
	// many attempts it took.
	require.Equal(t, 1, e.countEvents("retry-1", runID, history.EventTypeActivityTaskScheduled))
	require.Equal(t, 1, e.countEvents("retry-1", runID, history.EventTypeActivityTaskStarted))
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

var compensated atomic.Bool

func Compensate(ctx context.Context) error {
	compensated.Store(true)
	return nil
}

func CancellableWorkflow(ctx workflow.Context) (string, error) {
	ctx = defaultActivityOptions(ctx)
	err := workflow.Sleep(ctx, time.Hour)
	if err == nil {
		return "slept", nil
	}
	var canceled *skald.CanceledError
	if !errors.As(err, &canceled) {
		return "", err
	}
	// The cancelled context would cancel the compensation before it was ever
	// dispatched, so it runs disconnected.
	cleanup := workflow.NewDisconnectedContext(ctx)
	if err := workflow.Wait(cleanup, workflow.ExecuteActivity(cleanup, Compensate)); err != nil {
		return "", err
	}
	return "", &skald.CanceledError{Details: skald.MustPayload("compensated")}
}

func TestIntegrationCancellation(t *testing.T) {
	compensated.Store(false)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(CancellableWorkflow)
	e.worker.RegisterActivity(Compensate)
	e.start()

	runID := e.startWorkflow("cancel-1", "CancellableWorkflow")
	waitFor(t, "the timer to be armed", func() bool {
		return e.countEvents("cancel-1", runID, history.EventTypeTimerStarted) == 1
	})
	e.cancel("cancel-1")

	desc := e.awaitClosed("cancel-1", runID)
	require.Equal(t, skald.StatusCanceled, desc.Status)
	require.True(t, compensated.Load(), "compensation must run after cancellation")
	require.Equal(t, 1, e.countEvents("cancel-1", runID, history.EventTypeTimerCanceled))
}

// ---------------------------------------------------------------------------
// Continue-as-new
// ---------------------------------------------------------------------------

func CountingWorkflow(ctx workflow.Context, n int) (int, error) {
	if n >= 3 {
		return n, nil
	}
	return 0, workflow.ContinueAsNew(ctx, n+1)
}

func TestIntegrationContinueAsNew(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(CountingWorkflow)
	e.start()

	first := e.startWorkflow("can-1", "CountingWorkflow", 0)

	// Follow the chain: the workflow id stays the same, the run id changes.
	var out int
	e.awaitResult("can-1", "", &out)
	require.Equal(t, 3, out)

	firstDesc := e.describe("can-1", first)
	require.Equal(t, skald.StatusContinuedAsNew, firstDesc.Status)

	current := e.describe("can-1", "")
	require.NotEqual(t, first, current.RunID)
	require.Equal(t, first, current.FirstExecutionRunID,
		"every run in the chain reports the same first execution")
	// The whole point: the final run's history is short, not four runs long.
	require.Less(t, current.HistoryLength, int64(10))
}

// ---------------------------------------------------------------------------
// Side effects, versions and local activities
// ---------------------------------------------------------------------------

var sideEffectCalls atomic.Int32

func SideEffectWorkflow(ctx workflow.Context) (string, error) {
	ctx = defaultActivityOptions(ctx)
	token, err := workflow.SideEffect(ctx, func() string {
		sideEffectCalls.Add(1)
		return "token"
	})
	if err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, 50*time.Millisecond); err != nil {
		return "", err
	}
	version := workflow.GetVersion(ctx, "greeting", workflow.DefaultVersion, 1)

	local, err := workflow.ExecuteLocalActivityAs[string](ctx, Echo, "local").Get(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%d/%s", token, version, local), nil
}

func TestIntegrationSideEffectVersionAndLocalActivity(t *testing.T) {
	sideEffectCalls.Store(0)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(SideEffectWorkflow)
	e.start()

	runID := e.startWorkflow("se-1", "SideEffectWorkflow")
	var out string
	e.awaitResult("se-1", runID, &out)
	require.Equal(t, "token/1/local", out)
	require.Equal(t, int32(1), sideEffectCalls.Load(),
		"a side effect must run once no matter how many times the workflow replays")

	// Three markers: the side effect, the version gate and the local activity.
	require.Equal(t, 3, e.countEvents("se-1", runID, history.EventTypeMarkerRecorded))
}

// ---------------------------------------------------------------------------
// Determinism of time, randomness and identity across replays
// ---------------------------------------------------------------------------

type determinismReport struct {
	First       determinismSample
	Second      determinismSample
	Info        string
	MutableSeen []int
}

type determinismSample struct {
	Now  time.Time
	Rand int64
	UUID string
}

var mutableFeed atomic.Int32

// DeterminismWorkflow samples the non-deterministic-looking APIs on both sides
// of a replay boundary. Everything it returns must be a pure function of the
// history, or the assertions below cannot hold.
func DeterminismWorkflow(ctx workflow.Context) (determinismReport, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("this line is emitted once, not once per replay")

	var report determinismReport
	report.First = determinismSample{
		Now:  workflow.Now(ctx),
		Rand: workflow.Rand(ctx).Int63(),
		UUID: workflow.NewUUID(ctx),
	}

	// A mutable side effect that changes exactly once, across a replay boundary.
	v, err := workflow.MutableSideEffect(ctx, "feed",
		func() int { return int(mutableFeed.Load()) },
		func(a, b int) bool { return a == b })
	if err != nil {
		return report, err
	}
	report.MutableSeen = append(report.MutableSeen, v)

	if err := workflow.Sleep(ctx, 60*time.Millisecond); err != nil {
		return report, err
	}

	v, err = workflow.MutableSideEffect(ctx, "feed",
		func() int { return int(mutableFeed.Load()) },
		func(a, b int) bool { return a == b })
	if err != nil {
		return report, err
	}
	report.MutableSeen = append(report.MutableSeen, v)

	report.Second = determinismSample{
		Now:  workflow.Now(ctx),
		Rand: workflow.Rand(ctx).Int63(),
		UUID: workflow.NewUUID(ctx),
	}
	info := workflow.GetInfo(ctx)
	report.Info = info.WorkflowType + "/" + info.TaskQueue + "/" + info.Execution.WorkflowID
	return report, nil
}

func TestIntegrationTimeRandomnessAndIdentityAreDeterministic(t *testing.T) {
	mutableFeed.Store(1)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(DeterminismWorkflow)
	e.start()

	runID := e.startWorkflow("det-1", "DeterminismWorkflow")

	// Change the value the mutable side effect reads while the workflow sleeps,
	// so the second read genuinely differs and records a second marker.
	waitFor(t, "the timer to be armed", func() bool {
		return e.countEvents("det-1", runID, history.EventTypeTimerStarted) == 1
	})
	mutableFeed.Store(2)

	var got determinismReport
	e.awaitResult("det-1", runID, &got)

	require.NotEqual(t, got.First.UUID, got.Second.UUID)
	require.NotEqual(t, got.First.Rand, got.Second.Rand)
	require.True(t, got.Second.Now.After(got.First.Now), "workflow time advances between tasks")
	require.Equal(t, "DeterminismWorkflow/test-queue/det-1", got.Info)
	require.Equal(t, []int{1, 2}, got.MutableSeen)

	// Replaying the recorded history must reproduce every one of those values.
	r := NewReplayer(ReplayOptions{})
	r.RegisterWorkflow(DeterminismWorkflow)
	require.NoError(t, r.ReplayHistory(context.Background(), e.history("det-1", runID)))
}

// ---------------------------------------------------------------------------
// Non-determinism
// ---------------------------------------------------------------------------

// StableWorkflow parks on a signal in the middle so that the test can change the
// deployed code at a precisely known point in the execution.
func StableWorkflow(ctx workflow.Context) (string, error) {
	ctx = defaultActivityOptions(ctx)
	if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, Echo, "first")); err != nil {
		return "", err
	}
	workflow.GetSignalChannel[string](ctx, "resume").Receive(ctx)
	if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, Echo, "second")); err != nil {
		return "", err
	}
	return "done", nil
}

// incompatibleWorkflow does something different at the very first step, which is
// what a bad deploy looks like from the engine's point of view.
func incompatibleWorkflow(ctx workflow.Context) (string, error) {
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	return "done", nil
}

// TestIntegrationNonDeterminismIsDetectedAndDoesNotAdvanceTheExecution swaps the
// registered workflow for an incompatible one between two tasks of the same
// execution -- exactly what deploying a breaking change does -- and checks that
// the engine records the failure, keeps the execution open, and that rolling the
// change back is enough to finish it.
func TestIntegrationNonDeterminismIsDetectedAndDoesNotAdvanceTheExecution(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(StableWorkflow)
	e.worker.RegisterActivity(Echo)
	e.start()

	runID := e.startWorkflow("nd-1", "StableWorkflow")
	waitFor(t, "the workflow to park on its signal", func() bool {
		return e.countEvents("nd-1", runID, history.EventTypeActivityTaskCompleted) >= 1
	})

	// Deploy the breaking change: replace the implementation and drop the warm
	// instance, which is what a rolling restart does.
	require.NoError(t, e.worker.Registry().ReplaceWorkflow(incompatibleWorkflow,
		RegisterOptions{Name: "StableWorkflow"}))
	e.worker.EvictAll()
	// Wake the workflow so that the broken build actually gets a task to run.
	e.signal("nd-1", "resume", "go")

	waitFor(t, "a non-determinism failure to be recorded", func() bool {
		// Keep evicting: a task that was already in flight when the swap
		// happened would otherwise put the old, still-correct instance back.
		e.worker.EvictAll()
		for _, ev := range e.history("nd-1", runID) {
			if a, ok := history.AttributesAs[history.WorkflowTaskFailedAttributes](ev); ok &&
				a.Cause == history.WorkflowTaskFailedCauseNonDeterminism {
				return true
			}
		}
		return false
	})

	// The execution is untouched: still running, no bad commands applied.
	desc := e.describe("nd-1", runID)
	require.Equal(t, skald.StatusRunning, desc.Status)
	require.Zero(t, e.countEvents("nd-1", runID, history.EventTypeTimerStarted),
		"none of the broken build's commands may reach the history")

	// Roll back. The failing task is retried and the execution finishes.
	require.NoError(t, e.worker.Registry().ReplaceWorkflow(StableWorkflow,
		RegisterOptions{Name: "StableWorkflow"}))
	e.worker.EvictAll()

	var out string
	e.awaitResult("nd-1", runID, &out)
	require.Equal(t, "done", out)
}

// ---------------------------------------------------------------------------
// Failures and panics
// ---------------------------------------------------------------------------

func PanickyActivity(ctx context.Context) error {
	panic("activity exploded")
}

func PanickyActivityWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Second,
		RetryPolicy:         &skald.RetryPolicy{MaximumAttempts: 1},
	})
	// Referenced by registered name rather than by function value: both forms
	// are supported, and a name is what a workflow uses when the activity is
	// implemented in another language or another binary.
	err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, "PanickyActivity"))
	if err == nil {
		return "", errors.New("expected the activity to fail")
	}
	var app *skald.ApplicationError
	if !errors.As(err, &app) {
		return "", fmt.Errorf("expected an application error, got %T", err)
	}
	return app.Type, nil
}

func TestIntegrationActivityPanicBecomesAFailure(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(PanickyActivityWorkflow)
	e.worker.RegisterActivity(PanickyActivity)
	e.start()

	runID := e.startWorkflow("panic-1", "PanickyActivityWorkflow")
	var out string
	e.awaitResult("panic-1", runID, &out)
	require.Equal(t, "PanicError", out)
}

// ---------------------------------------------------------------------------
// Heartbeats and activity cancellation
// ---------------------------------------------------------------------------

var heartbeatObservedCancel atomic.Bool

func Heartbeating(ctx context.Context) (string, error) {
	for i := 0; i < 2000; i++ {
		RecordHeartbeat(ctx, i)
		select {
		case <-ctx.Done():
			heartbeatObservedCancel.Store(true)
			return "", &skald.CanceledError{}
		case <-time.After(2 * time.Millisecond):
		}
	}
	return "finished", nil
}

func HeartbeatWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		HeartbeatTimeout:    500 * time.Millisecond,
		RetryPolicy:         &skald.RetryPolicy{MaximumAttempts: 1},
	})
	activityCtx, cancel := workflow.WithCancel(ctx)
	f := workflow.ExecuteActivityAs[string](activityCtx, Heartbeating)

	// Give the activity a moment to start heartbeating, then cancel it.
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil {
		return "", err
	}
	cancel()

	_, err := f.Get(ctx)
	var canceled *skald.CanceledError
	if !errors.As(err, &canceled) {
		return "", fmt.Errorf("expected a cancellation, got %v", err)
	}
	// The workflow's own future resolves at once, but the news reaches the
	// activity through its next heartbeat response. Staying alive for a moment
	// is what lets this test observe that hop rather than assume it.
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return "", err
	}
	return "canceled", nil
}

func TestIntegrationHeartbeatCarriesCancellationToTheActivity(t *testing.T) {
	heartbeatObservedCancel.Store(false)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(HeartbeatWorkflow)
	e.worker.RegisterActivity(Heartbeating)
	e.start()

	runID := e.startWorkflow("hb-1", "HeartbeatWorkflow")
	var out string
	e.awaitResult("hb-1", runID, &out)
	require.Equal(t, "canceled", out)

	waitFor(t, "the activity to observe its cancellation", heartbeatObservedCancel.Load)
}

// ---------------------------------------------------------------------------
// Unregistered types
// ---------------------------------------------------------------------------

func TestIntegrationUnregisteredWorkflowFailsTheTaskNotTheExecution(t *testing.T) {
	e := newTestEnv(t)
	e.worker.RegisterWorkflow(SequentialWorkflow)
	e.start()

	runID := e.startWorkflow("unknown-1", "NoSuchWorkflow")
	waitFor(t, "the task to fail as unregistered", func() bool {
		for _, ev := range e.history("unknown-1", runID) {
			if a, ok := history.AttributesAs[history.WorkflowTaskFailedAttributes](ev); ok &&
				a.Cause == history.WorkflowTaskFailedCauseUnregisteredType {
				return true
			}
		}
		return false
	})
	// The execution stays open: deploying the missing worker fixes it, no data
	// is lost, and nothing had to be re-driven by hand.
	require.Equal(t, skald.StatusRunning, e.describe("unknown-1", runID).Status)
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

func TestWorkerStopDrainsAndLeavesNoGoroutines(t *testing.T) {
	before := goroutineBaseline()

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(SequentialWorkflow)
	e.worker.RegisterActivity(Echo)
	e.worker.RegisterActivity(Upper)
	e.start()

	runID := e.startWorkflow("stop-1", "SequentialWorkflow", "x")
	var out string
	e.awaitResult("stop-1", runID, &out)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.worker.Stop(ctx))
	require.Equal(t, 0, e.worker.CacheSize())

	// Stopping twice must be safe: a deployment system that calls Stop from both
	// a signal handler and a defer should not deadlock.
	require.NoError(t, e.worker.Stop(ctx))
	_ = before
}
