# 0001. Event sourcing as the durability model

- **Status**: Accepted
- **Date**: 2026-01-06

## Context

Skald has to answer one question after a crash: *where was this workflow, and
what has it already done to the outside world?* Everything else follows from the
answer.

The workflow is a Go function. Its state at any instant is a program counter, a
stack, and a set of local variables — none of which can be serialised in Go.
There is no way to snapshot a running goroutine and restore it in another
process.

Two further constraints:

- **Effects must not be repeated.** If the process dies after charging a card,
  the recovered execution must not charge it again. This is the entire reason
  the system exists.
- **The system must be auditable.** "Why did order 1234 refund itself?" has to
  be answerable months later, by someone who was not there, without a debugger.

## Decision

The durable state of a workflow run is an **append-only, dense, 1-based sequence
of immutable events** — its history. It is the only truth. Everything else is
derived and reconstructible.

Three rules make it work, and they are enforced structurally rather than by
convention:

1. **Every observable effect is bracketed.** An event records the intent before
   the effect happens (`ActivityTaskScheduled`) and another records the outcome
   after (`ActivityTaskCompleted`). Replay therefore never re-issues an effect
   that has already happened: it reads the outcome instead of performing the
   action.
2. **Nothing non-deterministic reaches workflow code except through an event.**
   Time, randomness and retry jitter are drawn once by the server, written to
   the history, and replayed from it.
3. **State is a fold over the history.** `execution.MutableState.apply` is a pure
   function of `(state, event)`, and `Rebuild` is that function in a loop.
   `AppendEvent` applies before it records, so a rejected transition leaves the
   history untouched.

Recovery is therefore not a special path: it is what the normal path already
does. Every request rebuilds state by replaying history, so a bug in event
application shows up on the next request rather than hiding until a failover.

## Consequences

### What this buys

- **Crash recovery is free and exact.** Not "approximately where it was" —
  exactly where it was, including which of five concurrent activities had
  returned.
- **The audit log is the system, not a copy of it.** There is no drift between
  what happened and what was logged, because they are the same bytes.
- **Determinism becomes checkable.** Replay produces a stream of commands; the
  history contains the effects those commands produced last time. Comparing them
  is a mechanical check, and it is how non-determinism is detected at all (see
  [ADR 0008](0008-refuse-to-advance-on-non-determinism.md)).
- **Time travel for debugging.** `skaldctl workflow history` and
  `worker.Replayer` re-run a production execution offline, with no server and no
  side effects.
- **The write path is one operation.** Append events, update one row, adjust the
  timer index, atomically. Every feature is a special case of it.

### What this costs

- **Replay cost is linear in history length.** A workflow task on a 10,000-event
  history replays 10,000 events. The sticky cache hides this in the common case
  and `ContinueAsNew` bounds it, but the cost is real and it is why there is a
  history length limit ([ADR 0009](0009-payload-and-history-limits.md)).
- **Workflow code must be deterministic.** This is a genuine constraint on
  authors, not a detail. It needs a versioning gate, an SDK that replaces half
  of Go's concurrency primitives, and a documented set of rules
  ([writing-workflows.md](../writing-workflows.md)).
- **Event codes are permanent.** A code that has shipped can never be reused for
  a different meaning, because histories written years ago must still decode.
  Adding an event type is cheap; changing one is not possible.
- **Payloads are copied into events.** A 2 MiB activity input is 2 MiB in the
  history, forever. Hence the payload cap and the advice to pass references to
  object storage for anything large.
- **Storage grows monotonically.** There is no retention or archival today, and
  a busy deployment accumulates history indefinitely.

## Alternatives considered

### Continuation snapshotting

Serialise the workflow's stack and resume it elsewhere. This is what a
language-level continuation or a checkpointing runtime would give.

Rejected because Go cannot do it. There is no supported way to capture a
goroutine's stack, and even a language that can (Erlang, or a CPS-transformed
JavaScript runtime) still has to answer "was the side effect issued before or
after the snapshot", which brings back the intent/outcome bracketing anyway. The
snapshot approach also makes the audit story worse: a binary blob explains
nothing about why a decision was made.

### A status column and a driver loop

`orders.state = 'charged'`, plus a cron job that finds rows stuck in a state and
pushes them forward.

Rejected on maintenance cost, not correctness — it does work. It requires the
business logic to be written twice, once as the happy path and once as the
transition table, and every new step is a schema migration plus a new branch in
the driver. It also cannot express "wait for either the approval signal or 48
hours" without another table. The failure mode is that the state machine and the
code drift, which nothing detects.

### A queue per step

Publish a message per transition; each consumer does one step and publishes the
next.

Rejected because it inverts the readable order of the program. The workflow
exists, but only as a graph you reconstruct by grepping for publishers of each
topic. Retries and timeouts become per-queue configuration rather than
per-business-step decisions, and exactly-once semantics become the queue's
problem — which it does not solve either, so the idempotency work still has to
happen.

### Command sourcing (store commands, not events)

Persist the workflow's *decisions* rather than the world's *outcomes*, and
recompute outcomes on replay.

Rejected because the outcome of an activity is not recomputable — it is a fact
about the outside world. Storing "I decided to charge the card" without storing
"the card was charged, here is the receipt" means replay has to ask the payment
provider again, which is exactly the duplicate the system exists to prevent.
Skald stores both: the command's effect is an event, and the outcome is another.
