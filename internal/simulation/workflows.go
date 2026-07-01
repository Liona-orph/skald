package simulation

import (
	"context"
	"fmt"
	"time"

	"github.com/skald-io/skald/pkg/skald"
	"github.com/skald-io/skald/pkg/workflow"
)

// The workflows the simulator runs.
//
// Every one of them is a *pure function of its input*: the result may not
// depend on how many times an activity was retried, which worker ran it, how
// long anything took, or how many times the workflow was replayed. That is the
// whole point. It lets the simulator state one invariant -- "a completed
// execution's result equals expectedResult(input)" -- that is violated by
// exactly the bugs durable execution exists to prevent: a lost activity result,
// a duplicated one, a replay that took a different branch, a signal delivered
// twice.
//
// Failure is therefore never authored here. The simulator injects it from the
// outside by failing an activity attempt, dropping a task or crashing a worker,
// which keeps the expected value exact while still exercising every retry path.

// Activity, workflow and signal names. They are constants because the simulator
// asserts on them and because a typo in a registration is otherwise a workflow
// that silently never runs.
const (
	WorkflowSequential  = "SimSequential"
	WorkflowFanOut      = "SimFanOut"
	WorkflowTimer       = "SimTimer"
	WorkflowSignal      = "SimSignal"
	WorkflowContinue    = "SimContinueAsNew"
	ActivityStep        = "SimStep"
	ActivitySquare      = "SimSquare"
	SimSignalName       = "sim-go"
	simTaskQueue        = "sim"
	simSignalDeadline   = 90 * time.Second
	simTimerUnit        = 20 * time.Second
	simActivityAttempts = 0 // unlimited: the simulator, not the policy, ends retries
)

// simActivityOptions is the one activity policy every simulation workflow uses.
//
// The start-to-close timeout is what turns "a worker took this attempt and
// vanished" into a retry rather than a hang, so it is the mechanism the crash
// faults are measured against. There is deliberately no schedule-to-close bound
// and no attempt cap: the simulator switches faults off before asserting
// liveness, so an execution that fails to finish is a bug in the engine and not
// a retry budget that ran out.
func simActivityOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &skald.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    simActivityAttempts,
		},
	})
}

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

// Step returns n+1. It is pure and total: every interesting failure the
// simulator wants is injected by the harness on the way back, not raised here.
func Step(_ context.Context, n int) (int, error) { return n + 1, nil }

// Square returns n*n.
func Square(_ context.Context, n int) (int, error) { return n * n, nil }

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

// SequentialWorkflow runs n activities one after another and sums their results.
//
// It is the simplest shape that has a long history and a strict ordering, so it
// is the one most sensitive to a replay that skips or reorders an event.
func SequentialWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = simActivityOptions(ctx)
	total := 0
	for i := 0; i < n; i++ {
		var v int
		if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ActivityStep, i), &v); err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// FanOutWorkflow starts n activities in one batch and sums their results.
//
// The batch is what makes this different from Sequential: n activities are in
// flight against one execution at once, so a duplicate delivery or a lost
// result has n chances per workflow task to corrupt the sum.
func FanOutWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = simActivityOptions(ctx)
	futures := make([]workflow.Future[int], n)
	for i := range futures {
		futures[i] = workflow.ExecuteActivityAs[int](ctx, ActivitySquare, i)
	}
	total := 0
	for _, f := range futures {
		v, err := f.Get(ctx)
		if err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// TimerWorkflow sleeps, runs one activity, and sleeps again.
//
// Durable sleep is the only part of the system whose progress depends purely on
// the clock, so this workflow is the one that fails if a timer is dropped from
// the index, armed twice, or fired against a run that already moved on.
func TimerWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = simActivityOptions(ctx)
	if err := workflow.Sleep(ctx, time.Duration(n+1)*simTimerUnit); err != nil {
		return 0, err
	}
	var v int
	if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ActivitySquare, n), &v); err != nil {
		return 0, err
	}
	if err := workflow.Sleep(ctx, simTimerUnit); err != nil {
		return 0, err
	}
	return v, nil
}

// SignalWorkflow races a signal against a deadline and returns the same value either
// way.
//
// Making both branches produce the same result is deliberate: it lets the
// simulator deliver a signal, or not deliver it, or deliver it twice, without
// weakening the "result equals the expected value" invariant. What the branches
// differ in is the *history* they produce -- a signal event and a cancelled
// timer, or a fired timer -- which is exactly the divergence a replay bug turns
// into a non-determinism error.
func SignalWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = simActivityOptions(ctx)
	signals := workflow.GetSignalChannel[int](ctx, SimSignalName)

	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	deadline := workflow.NewTimer(timerCtx, simSignalDeadline)

	workflow.NewSelector(ctx).
		AddReceive(signals, func() { signals.Receive(ctx) }).
		AddFuture(deadline, func() {}).
		Select(ctx)
	// Release the loser so a pending timer does not outlive the decision it was
	// there to bound.
	cancelTimer()

	var v int
	if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ActivityStep, n), &v); err != nil {
		return 0, err
	}
	return v * 2, nil
}

// ContinueState is the input of the ContinueAsNew workflow: the accumulator a
// run hands to its successor.
type ContinueState struct {
	Round  int `json:"round"`
	Rounds int `json:"rounds"`
	Total  int `json:"total"`
}

// ContinueAsNewWorkflow runs one activity per round and hands the accumulator to a
// fresh run each time.
//
// The chain is the part worth simulating: closing one run and opening the next
// must happen together, and this is the workflow that catches a chain with a
// dangling link or a successor started twice. It found both -- seed 5, when the
// two were separate writes -- which is why they now share a transaction.
func ContinueAsNewWorkflow(ctx workflow.Context, st ContinueState) (int, error) {
	ctx = simActivityOptions(ctx)
	var v int
	if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ActivityStep, st.Round), &v); err != nil {
		return 0, err
	}
	st.Total += v
	st.Round++
	if st.Round < st.Rounds {
		return 0, workflow.ContinueAsNew(ctx, st)
	}
	return st.Total, nil
}

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

// workloadKind names one of the workflow shapes the simulator starts.
type workloadKind int

const (
	kindSequential workloadKind = iota
	kindFanOut
	kindTimer
	kindSignal
	kindContinue
	numWorkloadKinds
)

func (k workloadKind) workflowType() string {
	switch k {
	case kindSequential:
		return WorkflowSequential
	case kindFanOut:
		return WorkflowFanOut
	case kindTimer:
		return WorkflowTimer
	case kindSignal:
		return WorkflowSignal
	case kindContinue:
		return WorkflowContinue
	}
	return fmt.Sprintf("workloadKind(%d)", int(k))
}

// workload is one execution the simulator intends to start, together with the
// answer it must produce.
type workload struct {
	kind       workloadKind
	workflowID string
	n          int
	// arg is the encoded workflow argument.
	arg any
	// want is the result a successful completion must carry.
	want int
	// signalable reports whether delivering SimSignalName can influence the run.
	signalable bool
}

// newWorkload builds the workload for one execution and computes its expected
// result up front, so that the oracle is written once and independently of the
// workflow code it checks.
func newWorkload(kind workloadKind, workflowID string, n int) workload {
	w := workload{kind: kind, workflowID: workflowID, n: n, arg: n}
	switch kind {
	case kindSequential:
		// sum of (i+1) for i in [0,n)
		w.want = n * (n + 1) / 2
	case kindFanOut:
		for i := 0; i < n; i++ {
			w.want += i * i
		}
	case kindTimer:
		w.want = n * n
	case kindSignal:
		w.want = (n + 1) * 2
		w.signalable = true
	case kindContinue:
		w.arg = ContinueState{Rounds: n}
		w.want = n * (n + 1) / 2
	}
	return w
}
