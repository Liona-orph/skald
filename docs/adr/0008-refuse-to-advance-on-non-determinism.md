# 0008. Refusing to advance an execution on non-determinism

- **Status**: Accepted
- **Date**: 2026-03-02

## Context

Replay compares the commands workflow code produces now against the effects the
history recorded before. When they disagree, the deployed code does not make the
same decisions as the code that wrote the history. Causes, in rough order of
frequency:

- an activity, timer or side effect was added, removed or reordered;
- a `map` range, `time.Now()` or `math/rand` leaked into a decision;
- a behaviour change was deployed without a `workflow.GetVersion` gate;
- a version gate's `minSupported` was raised while executions pinned to the old
  branch were still in flight.

All of them mean the same thing operationally: **a deploy went out that is
incompatible with executions already running.** It usually happens to many
executions at once, minutes after a release, and it is discovered by whoever is
on call.

The engine has to decide what to do with an execution in that state. The choice
determines what the incident looks like.

## Decision

**Skald does not advance the execution.** It fails the workflow task with
`WorkflowTaskFailedCauseNonDeterminism`, appends a `WorkflowTaskFailed` event,
and **schedules the task again**.

The execution does not fail. It does not time out. No compensating action runs
and no partial state is written. It sits exactly where it is until a worker build
that agrees with the history picks it up.

Rolling back the offending deploy is therefore a complete fix, with no data loss
and no manual repair.

Supporting decisions:

- **The diagnostic is the deliverable.** `NonDeterminismError` names the workflow
  type, the execution, the history event ID, the position within the command
  batch, the effect the history recorded, the command the code produced instead,
  the workflow-side call that produced it, and the usual causes. A worker that
  says "non-determinism detected" and stops has told an operator nothing.
- **`WorkflowTaskFailedCause.Permanent()` reports `true`** for non-determinism,
  distinguishing it from a transient worker error so an operator (and an alert)
  can tell "this will not fix itself" from "this already has".
- **The failure is recorded as non-retryable** in `FailureDetail`, and the worker
  backs off `FailedTaskBackoff` (100ms) before its next poll. The engine
  reschedules immediately — a rollback must be picked up at once — and without a
  worker-side pause that would spin a core while the revert is deploying.
- **The executor is poisoned.** An instance that hit an error mid-task has
  coroutines in an arbitrary state and must never serve another task; the sticky
  cache entry is discarded and the next task replays cold.

## Consequences

### What this buys

- **Rollback is always safe.** The single most valuable property in an incident:
  whatever else is true, putting the old binary back returns the system to a
  working state. No forward-only migrations, no repair tooling, no
  "unfortunately those executions are unrecoverable".
- **The blast radius is bounded and visible.** Affected executions stop making
  progress and appear as a rising `skald_requests_total{operation="RespondWorkflowTaskFailed"}`
  and as a stalled `last_event_time`. Unaffected executions are untouched.
- **No silent corruption.** The alternative failure — applying commands computed
  against a different history — produces a run whose history no longer describes
  what happened, and there is no way to detect it afterwards.
- **Fix-forward also works.** Because the task is rescheduled indefinitely,
  deploying a *corrected* build (with a proper version gate) also recovers the
  execution, with no operator action per execution.

### What this costs

- **The execution is stalled, indefinitely, and that is a real outage for it.** A
  payment workflow that stops mid-flight is not "safe" from the customer's point
  of view. Skald converts a data-corruption incident into an availability
  incident, which is the right trade, but it is a trade.
- **It can stall forever without anyone noticing.** There is no automatic escape
  hatch, no eventual failure, and no built-in alert. An operator who does not
  watch `RespondWorkflowTaskFailed` will find out from a customer. The runbook in
  [operations.md](../operations.md) makes this alert one of the four that matter.
- **The task loop is hot.** Fail, reschedule, poll, fail. The worker's 100ms
  backoff bounds it, but a thousand wedged executions across a fleet is a
  measurable, and slightly alarming, steady request rate.
- **History grows while wedged.** Each failed attempt appends a
  `WorkflowTaskFailed` and a `WorkflowTaskScheduled`. An execution left wedged for
  a very long time can approach the history length limit, at which point the
  problem gets worse rather than better.
- **There is no operator override.** An operator cannot say "skip this command
  and continue". The only tools are roll back, fix forward, or `terminate` — and
  terminate runs no workflow code, so nothing is cleaned up.

## Alternatives considered

### Fail the execution

Write `WorkflowExecutionFailed` with the non-determinism error and close the run.

Rejected because it is irreversible and it is triggered by an operator mistake
that is trivially reversible. A bad deploy would permanently kill every workflow
that happened to take a task during the window, and rolling back would not bring
them back. The cost of the incident becomes proportional to how long it took to
notice, which is exactly the wrong incentive shape.

### Apply the commands anyway and continue

Accept whatever the new code produced and move on.

Rejected as unsafe in the specific way the system exists to prevent. If replay
produced `ScheduleActivityTask(Refund)` where the history has
`ScheduleActivityTask(Charge)`, applying it means the history now says the
workflow charged and then, on replay, decided it had refunded. Every subsequent
replay is computed against a fiction, and no later check can detect it.

### Reset the execution to the last consistent workflow task

Truncate the history to a point before the divergence and re-run from there.

Rejected for this version, and it is the most interesting rejected option. It is
a real feature in mature engines (Temporal calls it "reset") and it is the right
answer for "the new code is correct and the old history is what is wrong". It
needs history truncation, which the append-only store deliberately does not
support; a rule for which effects already issued must be compensated; and an
operator interface to choose the reset point. All three are substantial, and none
of them is needed for the common case, which is "roll back the deploy".

### Auto-pin workers to a build version

Route a task only to a worker whose build produced the history, so mismatched
code never sees it.

Rejected as solving a different problem. It requires build versioning in the
protocol, per-build task queues, and a rule for what happens when the producing
build no longer exists anywhere. It also does not remove the need for this
decision — it narrows the window in which a mismatch happens, and something still
has to be right when it does.

### Retry with backoff and eventually give up

Reschedule a bounded number of times, then fail the execution.

Rejected because "eventually" is always shorter than the time it takes to notice,
diagnose and revert a bad deploy at an inconvenient hour. Any bound that is safe
is so long that it is indistinguishable from unbounded, and any bound that is
short enough to be useful destroys executions during a normal incident response.
