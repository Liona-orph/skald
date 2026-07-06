# 0005. Optimistic concurrency instead of leases or a lock service

- **Status**: Accepted
- **Date**: 2026-01-27

## Context

Several things want to advance the same workflow execution at the same time: a
signal from a client, an activity completion from a worker, a timer firing in the
scanner, and a workflow task response. Each reads the execution's state, computes
a transition, and writes events.

Two writers that both read version N and both write must not both succeed — the
second would append events computed against a state that no longer exists, and
the resulting history would fail its own validation or, worse, quietly describe
something that never happened.

The standard answers are a lease (one writer owns the execution for a window and
renews it) or an external lock service (ZooKeeper, etcd, Redis). Both are what a
sharded, multi-node history service uses, and both bring the same set of
questions: what happens when the lease holder is alive but partitioned, how long
is the window, what happens to work in flight when it expires, and what happens
when the lock service itself is down.

## Decision

**Every write is a compare-and-set on a version the caller read.**

`persistence.Store.AppendHistory` takes `ExpectedVersion`. The store rejects the
write with `ErrVersionConflict` if the stored version differs. `Engine.mutate`
wraps this in a bounded retry loop:

```
load (read row, rebuild state)  ->  run transition  ->  AppendHistory(expected version)
        ^                                                       |
        +------------------- ErrVersionConflict ----------------+
```

Five attempts by default (`DefaultMaxWriteAttempts`), then a `version_conflict`
error to the caller.

**A conflict is retried immediately, with no backoff.** That is deliberate: a
version conflict is proof that another writer made progress, so the retry
re-reads a state that has genuinely changed. It is not a spin against a busy
resource, and backing off would add latency to the common case — two writers
racing on a hot execution — to protect against a case that cannot happen. The
bounded attempt count still stops a pathological loop.

Within one process, a **striped mutex** (`lockStripes = 1024`) serialises
operations on the same workflow ID. This is a latency optimisation and nothing
more: it avoids paying for conflicts that are predictable, and deleting it would
leave the system correct and slower. The stripe key is the *workflow ID*, not the
run ID, because the runs of one workflow ID form a chain and an operation that
interleaves across a continue-as-new boundary could otherwise observe a workflow
with no current run or two.

The cache that holds rebuilt state is validated against the version on every
use, and dropped on every error, so a stale entry can never be written from.

## Consequences

### What this buys

- **No lock service.** Nothing to run, nothing to be down, no dependency whose
  unavailability makes the engine unavailable.
- **No lease expiry semantics.** There is no window in which a writer believes it
  owns something it does not. The compare-and-set *is* the arbitration, and it is
  decided by the store at the instant of the write.
- **No split brain.** A partitioned engine replica cannot corrupt anything,
  because its writes are rejected by the store the moment its version is stale.
  It fails; it does not diverge.
- **Correct by default for new operations.** A new engine operation gets
  concurrency safety by being written as a `transition` function — there is no
  lock to remember to take.
- **The retry is genuinely cheap.** Rebuilding state from a warm cache and a
  short history is microseconds, and the conflict rate is a function of
  per-execution write contention, which is low by design: at most one workflow
  task is outstanding per execution.

### What this costs

- **Livelock is possible in principle.** An execution under sustained write
  pressure from many sources can exhaust five attempts and return
  `version_conflict` (409) to a caller. In practice the striped lock removes
  intra-process contention and inter-process contention does not exist yet, but
  the failure mode is real and the error code exists for it.
- **Wasted work on conflict.** The losing writer has already rebuilt state and
  computed a transition, and throws it away. This is the standard optimistic
  trade: cheap when conflicts are rare, wasteful when they are common.
- **Transitions must be re-runnable.** A `transition` function can be called more
  than once against freshly loaded state. Anything it captures from the outer
  scope must be reset on each invocation — `startWorkflowTask` explicitly
  reassigns `task = api.WorkflowTask{}` at the top for this reason, and forgetting
  it would leak a half-filled result from a losing attempt.
- **A version column on the hot table.** Every driver must implement CAS
  correctly, including the subtlety that the version increments on *every*
  successful write, including a timer-only update from a heartbeat. The
  conformance suite in `internal/persistence/persistencetest` covers it.
- **It does not solve cross-process dispatch.** Optimistic concurrency makes
  concurrent *writes* safe; it says nothing about a task reference that exists
  only in one process's memory ([ADR 0004](0004-derived-task-queues.md)). Both
  have to be solved for multi-node, and only one of them is.

## Alternatives considered

### Leases with renewal

One engine owns an execution for, say, 30 seconds, renewing while it works.

Rejected because the hard part is not acquisition, it is expiry. A lease holder
that is alive but slow, or alive but partitioned, believes it still owns the
execution while another replica has taken it. The only safe way to close that
window is to make writes conditional on the lease version — at which point the
lease is a caching layer over a compare-and-set, and the compare-and-set alone is
simpler and strictly safer. Leases pay for themselves when they avoid *reads*,
which is a sharded-cache concern Skald does not have.

### An external lock service

etcd or ZooKeeper holding a lock per execution.

Rejected on dependency cost and on failure semantics. It makes a second
distributed system a hard availability dependency of the first, and it still has
the fencing problem: a lock holder that pauses for a GC longer than its session
timeout must be fenced by a token checked at the write, which is once again a
compare-and-set.

### Pessimistic locking in the database

`SELECT ... FOR UPDATE` on the execution row for the duration of the transaction.

Rejected for two reasons. It is not portable across the drivers the interface is
meant to admit — SQLite has no row-level locking, and the whole point of
`persistence.Store` is that a driver can be written for something else. And it
holds a database transaction open across engine work, including a full state
rebuild, which turns a lock-hold time into a function of history length.

### A single-writer actor per execution

Route every operation for one execution to one goroutine, or one process, and
serialise there.

Rejected as the *primary* mechanism, though the striped mutex is a degenerate
form of it within a process. As a distributed design it requires consistent
routing, which requires a membership protocol, which is a much larger commitment
than a version column — and it still needs a compare-and-set underneath to
survive a routing change mid-write.

### No concurrency control, single writer by construction

Guarantee only one `skaldd` ever runs.

Rejected because it makes correctness depend on deployment discipline. A rolling
restart briefly has two processes; a misconfigured supervisor has two
permanently. With CAS, the second process is harmless. Without it, the second
process is a corrupted history.
