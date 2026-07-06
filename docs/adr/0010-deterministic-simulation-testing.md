# 0010. Deterministic simulation testing as the primary correctness strategy

- **Status**: Accepted
- **Date**: 2026-04-07

## Context

The bugs that matter in a durable execution engine are timing bugs. Not "this
function returns the wrong value" — those are caught by any test — but:

- a worker that dies between taking a task and responding,
- a timer that fires while the workflow task it would wake is already in flight,
- two writers that read the same version and both try to commit,
- an activity that completes at the exact instant its heartbeat deadline expires,
- a store that returns a transient error on the third of five operations.

Each is a specific interleaving. Each occurs in production at some rate
proportional to load, and each is essentially impossible to reproduce from a bug
report.

Conventional testing does badly here. A test that starts goroutines and sleeps
reproduces one arbitrary interleaving out of thousands, and it reproduces a
*different* one on a loaded CI machine — which is where flakiness comes from.
Reducing production constants so tests run fast means the tests exercise a
configuration nobody deploys. Polling until a condition holds passes on a fast
laptop and fails under load.

There is also a cost problem. A test that exercises a five-second retry backoff
with real time takes five seconds. A suite with fifty of those takes four minutes,
and a suite that takes four minutes is a suite nobody runs before pushing.

## Decision

**Time is an injected dependency everywhere, and correctness is asserted against
schedules rather than against elapsed wall time.**

The building blocks, in the order they were built:

1. **`internal/clock`.** Every component that waits — the matching long poll, the
   timer service, the engine's backoff, the SDK's deadlock detector — reads time
   through a `Clock` interface. `clock.System()` in production; `clock.Virtual`
   in tests, where time moves only when a test moves it.

   `Virtual.Advance` fires timers in deadline order, with the clock already set
   to each timer's own deadline, so a callback that reads `Now` sees the instant
   it was scheduled for rather than the end of the window. `BlockUntil(n)` waits
   for a known number of timers to be armed, which is the synchronisation
   primitive that makes arm-then-advance safe instead of racy. It blocks forever
   if the count is never reached, which surfaces as a test timeout with a
   goroutine dump naming the function — a much better failure than a flaky
   assertion.

2. **Fault injection in the memory store.** `memory.WithFaults(FaultConfig{...})`
   takes a seed, a version-conflict rate, a transient-error rate and a latency
   function. Runs with the same seed and the same call order fail identically.
   The conflict rate models a competing engine replica, which callers must
   already handle ([ADR 0005](0005-optimistic-concurrency.md)).

3. **The storage conformance suite**
   (`internal/persistence/persistencetest`). One set of tests asserting the
   *contract*, run against every driver. A driver is finished when it passes.

4. **`internal/simulation`.** The harness that composes the above: a virtual
   clock, a faulty store, an engine, workers, and a scenario, driven from a seed.
   A failing seed is a complete, replayable reproduction.

5. **`worker.Replayer`.** Offline replay of a recorded history against the
   current binary, with no server, no network and no side effects. Side effects
   and local activities are served from the markers already in the history rather
   than re-executed, which is what makes running it against *production*
   histories safe.

The properties asserted are invariants of the system, not outputs of one run: the
history is always valid; an activity completes at most once per attempt; a
workflow that reaches a terminal state stays there; replay from a cold cache
produces the same commands as replay from a warm one; every timer either fires or
is disarmed.

That last one deserves emphasis. The test suite evicts the sticky cache between
**every** workflow task and asserts identical results. The entire design rests on
"correctness never depends on a cache hit", and this is the test that keeps it
true.

## Consequences

### What this buys

- **Tests assert what the code promises.** "Nothing fires at 4.999s and exactly
  one thing fires at 5.000s" is a statement about the *schedule*, which is what
  the code actually guarantees — not about how long a machine happened to take.
- **Timeout and backoff paths become cheap enough to test exhaustively.**
  Advancing an hour costs microseconds. The four timeout clocks, the retry
  backoff curve and the heartbeat deadline are covered properly rather than by
  one hopeful integration test.
- **The whole suite runs in seconds.** Which means it runs before every push,
  which is the only thing that makes a test suite valuable.
- **Flakiness is structurally excluded.** There is no `time.Sleep` in a test and
  no polling loop, so a slow CI machine changes nothing. A test that is flaky
  under this regime is reporting a real race.
- **A failure is a seed.** "Seed 42 fails at step 118" is a complete
  reproduction, on any machine, forever.
- **Fault injection exercises paths that are otherwise unreachable.** The
  version-conflict retry loop, the "task reference is stale" branch, the
  timer-redelivery path — all of them are rare in production and routine under
  injection.

### What this costs

- **Every waiting component must take a `Clock`.** That is a real API tax:
  `timers.Config`, `matching.Config`, `engine.Config` and `workflow.ExecutorOptions`
  all carry one, and a component that reaches for `time.Now()` directly silently
  opts out of the entire strategy. Reviewing for that is a permanent obligation.
- **Virtual time has a genuine subtlety.** Firing a timer makes a goroutine
  *runnable*; it does not run it. `Advance` returns before the woken goroutine
  has observed the tick, so a test that advances and immediately asserts is
  racing. `BlockUntil` and condition-based assertions are the discipline, and
  getting it wrong produces a test that passes for the wrong reason.
- **It does not cover the operating system.** A virtual clock proves nothing
  about `fsync` semantics, disk full, a partially written WAL, or the kernel
  killing the process at an inopportune moment. Those need real integration tests
  (`test/integration/`) and, honestly, production.
- **It does not cover the network.** The client's retry behaviour is tested
  against an `httptest` server, not against packet loss, connection resets
  mid-body, or a proxy that closes an idle connection at 60 seconds — the exact
  failure the 50-second poll cap exists to avoid.
- **Simulation only finds what it explores.** Coverage is a function of the
  scenarios written and the seeds run. Absence of failures over a thousand seeds
  is evidence, not proof, and there is no coverage metric for interleavings.
- **The determinism is the harness's, not the runtime's.** Go's scheduler is
  still non-deterministic. Simulation constrains *time* and *store behaviour*; two
  goroutines that genuinely race still race. The workflow dispatcher removes this
  inside workflow code ([ADR 0003](0003-cooperative-coroutine-dispatcher.md)); the
  engine relies on the race detector for the rest.

## Alternatives considered

### Real time, with shortened constants in tests

Set the retry interval to 10ms and sleep.

Rejected on both correctness and cost. The test then exercises a configuration
nobody deploys — and timing bugs are frequently *specific to the ratio* between
constants, so shrinking them all uniformly can hide the bug being looked for. It
is also slow and flaky, which are the two properties that stop a suite from being
run.

### `github.com/benbjohnson/clock` or a similar library

A well-established mock clock.

Rejected narrowly. The interface Skald needs is four methods, the virtual
implementation is 200 lines, and owning it means `BlockUntil` can have exactly
the semantics the tests need and `Advance` can fire in deadline order with the
clock set to each deadline — behaviour that off-the-shelf mock clocks differ on.
For a dependency this small and this load-bearing, the cost of owning it is lower
than the cost of matching someone else's semantics.

### Property-based testing with a shrinking generator

`gopter` or `rapid`, generating operation sequences and shrinking failures.

Not rejected — complementary, and worth adding. Shrinking is the one thing seed
replay does not give: a failing seed reproduces a 500-step scenario, and the
engineer still has to find the three steps that matter. The reason it is not the
primary strategy is that the interesting state space is *temporal* rather than
structural, and a generator that produces operation sequences without controlling
time explores far less than a virtual clock does.

### Formal specification (TLA+, Alloy)

Model the state machine and check the invariants exhaustively.

Rejected as the primary strategy, and genuinely valuable as a supplement. A TLA+
model of the workflow-task lifecycle would prove things simulation can only
sample — particularly around the two-transaction successor-creation gap. What it
would not do is catch the bug where the *implementation* diverges from the model,
which is where most real bugs live. The honest position is that a model would
have been worth writing for the state machine specifically, and it has not been.

### Chaos testing against a deployed instance

Kill processes and partition networks in a real environment.

Rejected as a substitute, endorsed as an addition. It finds things simulation
cannot — the operating system, the filesystem, real clock skew — but it is slow,
it does not reproduce, and a failure gives you a symptom rather than a seed. It
belongs after the fast suite, not instead of it.
