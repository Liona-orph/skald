# 0007. Recording retry jitter in the history

- **Status**: Accepted
- **Date**: 2026-02-17

## Context

Retry backoff needs jitter. Without it, a downstream dependency that fails a
thousand concurrent activities gets all thousand retries at exactly the same
instant, and again at the next interval, and again — a synchronised herd that
turns a partial outage into a sustained one.

Jitter is randomness. [ADR 0001](0001-event-sourcing.md) makes randomness a
problem: anything that influences a workflow's observable behaviour must produce
the same value on every replay, or two replays disagree.

The specific danger is subtle, because the jitter itself is not read by workflow
code. It determines *when* an activity's next attempt is dispatched, which
determines the timestamp of the resulting history events, which determines what
`workflow.Now()` returns, which the workflow may well branch on. A jitter drawn
freshly on each evaluation would also mean two engine replicas computing the same
retry disagree about when it is due — one arms a timer at T+1.03s, the other at
T+1.17s, and both entries exist.

There is a second, quieter version of the same problem. The retry decision itself
(`ShouldRetry`) is recomputed whenever the engine reloads state, and it must
reach the same conclusion each time.

## Decision

**The jitter value is drawn once by the server, written into the history, and
recomputed from the recorded value forever after.**

Concretely:

- `ActivityTaskStarted` carries `RetryJitterSeed float64`, the draw used for the
  *next* backoff of that activity.
- The value is not from `math/rand`. It is derived deterministically from data
  already written down:
  `jitterFromSeed(runSeed, scheduledEventID*1_000_003 + attempt)`, where
  `runSeed` is the `RandomnessSeed` in event 1 and the derivation is splitmix64.
  Any replica, at any time, derives the same value with no coordination.
- `RetryPolicy.ShouldRetry(attempt, err, jitterSeed)` takes the seed as a
  **parameter**. It never calls a global RNG. This is the whole point: the
  function's signature makes it impossible to introduce hidden non-determinism at
  a call site.
- Jitter is **one-sided**: `interval * (1 + 0.2 * seed)`, never earlier than the
  nominal backoff, so the policy's stated interval remains a lower bound an
  operator can rely on.
- `MutableState.startedJitter` recovers the recorded value from the started event
  when a failure or timeout is processed.

The same principle applies to the two other random values in the system: the
per-run `RandomnessSeed` is drawn once with `crypto/rand` and written into event
1, and the successor of a continue-as-new gets `nextSeed(previous)` — derived,
not redrawn.

## Consequences

### What this buys

- **Retry timing is reproducible.** A replayed execution arms the same timer at
  the same instant it did originally, so the history a replica would write
  matches the one that exists.
- **No coordination between replicas.** Two engines processing the same activity
  failure compute an identical delay, so their timer-index upserts are identical
  and the compare-and-set resolves the race with no ambiguity about which
  schedule won.
- **Randomness is auditable.** "Why did attempt 4 wait 2.3 seconds when the
  policy says 2?" is answered by reading the history, not by guessing at an RNG
  state that no longer exists.
- **The API cannot be misused.** Because `ShouldRetry` takes the seed as an
  argument, there is no way to call it without deciding where the randomness came
  from. A hidden `rand.Float64()` inside it would have been invisible and would
  have worked in every test.
- **splitmix64 rather than `math/rand`.** `math/rand`'s output is stable under
  Go's compatibility promise, but that is a promise about the standard library,
  not about Skald's histories. A workflow that has been running for six months
  must produce the same numbers after a Go upgrade. Owning the generator removes
  the dependency, and splitmix64 is two lines and passes BigCrush.

### What this costs

- **Eight bytes per `ActivityTaskStarted` event.** Negligible, but it is a
  permanent field in a persisted format.
- **The seed is drawn before it is needed.** `ActivityTaskStarted` records the
  jitter for the *next* backoff, which reads oddly: the event that starts attempt
  N carries the randomness for the delay before attempt N+1. It is written at
  that point because that is the last event guaranteed to exist when the failure
  arrives.
- **Only one draw per attempt is available.** The counter is
  `scheduledEventID*1_000_003 + attempt`, so all randomness for one attempt comes
  from one value. Anything needing a second independent draw would need a second
  derivation rule, and the multiplier is a magic number chosen to keep the
  counter space of different activities from colliding.
- **A retry that starts with no started event gets zero jitter.**
  `startedJitter` returns `0` when it cannot find the event or the attributes,
  which means the nominal backoff with no spread. It is a safe degradation rather
  than a correct one, and it is invisible when it happens.
- **The engine, not the policy, owns the randomness.** A user-supplied
  `RetryPolicy` cannot customise jitter — the fraction is a package constant
  (`DefaultJitterFraction = 0.2`). Making it configurable would mean recording
  the fraction too, or accepting that changing it changes replayed timing.

## Alternatives considered

### Draw jitter with `math/rand` at retry time

The obvious implementation, and what a non-replayed system would do.

Rejected because it is precisely the bug this ADR exists to prevent. It works in
every test, because tests rarely replay a retry, and fails in production as a
timing discrepancy that is almost impossible to attribute.

### No jitter at all

Deterministic by construction.

Rejected because the thundering herd is a real and severe failure mode. A
retrying fleet with no spread is a distributed denial of service against your own
dependency, and it self-synchronises: every failure at time T produces a retry at
T+1, whose failures produce retries at T+3, and the fleet stays in lockstep
indefinitely.

### Record the computed *delay* instead of the seed

Write `next_retry_delay: 2.34s` into the event.

Rejected on flexibility, though it is a defensible design. Recording the seed
means the policy's shape — the coefficient, the cap, the fraction — is applied at
evaluation time, so a fix to the backoff *formula* takes effect on in-flight
activities. Recording the delay pins the arithmetic as well as the randomness,
which sounds safer and in practice means a bug in the formula is permanent for
every activity that already has a delay written down. The chosen design pins only
the part that genuinely cannot be recomputed.

### Derive jitter from the run seed with no per-attempt counter

Simpler: use `RandomnessSeed` directly.

Rejected because every attempt of every activity in the run would then get the
same jitter, which is not jitter — it is a per-run constant offset, and a fleet
of workflows started in the same second would still synchronise within each run.

### Put the jitter in the timer index row instead of the history

The retry timer row already carries the attempt number, so it could carry the
delay.

Rejected because it makes the timer index authoritative for something a replay
needs. The index is already load-bearing for the attempt counter, which is a
known wart ([ARCHITECTURE.md](../ARCHITECTURE.md#known-limitations-and-what-it-would-take-to-fix-them));
adding a second such dependency would make the index impossible to treat as a
rebuildable structure.
