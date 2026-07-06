# 0004. Derived, non-persisted task queues

- **Status**: Accepted
- **Date**: 2026-01-22

## Context

Work has to reach workers. A workflow task becomes ready when new history exists
for a run; an activity task becomes ready when a `ScheduleActivityTask` command
is applied or a retry backoff elapses. Workers long-poll for both.

The default design is a durable queue: write the task to a queue table (or an
external broker) in the same transaction as the history, and have workers consume
from it. That is what most systems do, and it has a well-known cost — the queue
and the state machine are two representations of the same fact, and keeping them
in agreement is a permanent source of bugs. A task in the queue for an activity
that has since been cancelled. An activity marked pending with no queue entry. A
consumer that acknowledges before the state machine agrees.

There is also a size problem. An activity input can be 2 MiB. Buffering payloads
makes queue memory proportional to backlog depth times payload size, so one
workflow fanning out ten thousand activities can evict every other tenant's
queue — or, with a durable queue, doubles the write volume of the whole system.

The observation that makes a different design possible: **every pending task is
already recoverable from the history.** A workflow task is ready exactly when
`WorkflowTaskScheduled` has no matching `WorkflowTaskStarted`. An activity is
ready exactly when it is pending and has never been handed to a worker.

## Decision

Task queues are **in-memory, per-process, and never persisted**. They hold *task
references*, not payloads: the execution identity plus the scheduling event ID,
a few dozen bytes.

- Queues are partitioned by `(namespace, task queue, kind)`. One tenant's backlog
  cannot delay another's, and a workflow task can never be delivered to an
  activity poller.
- **Sync match**: if a poller is already parked, the reference goes straight to
  its buffered channel and never touches the backlog. This is the path a healthy
  deployment takes, and it makes end-to-end latency a function of the store write
  rather than a queue scan.
- **Async match**: otherwise the reference joins a FIFO backlog. Pollers are also
  FIFO, which gives round-robin: a worker that just got a task re-polls and lands
  at the back.
- Losing the entire structure costs **latency, never correctness**.
  `Engine.Recover` walks the open executions at startup and re-materialises every
  reference whose readiness is visible in the history.
- When a queue is at capacity, `Add` rejects the **newest** reference with
  `ErrBacklogFull` rather than evicting the oldest.

Recovery deliberately does **not** re-dispatch work the history shows as in
flight — a workflow task a worker already started, or an activity attempt a
worker is running. Those may still be alive on that worker, and re-dispatching
would duplicate them; they are owned by their entries in the durable timer index,
which fire and reschedule on their own. The rule is: *recovery restores what
nothing else can.*

## Consequences

### What this buys

- **One consistency problem instead of two.** There is no queue state to
  reconcile with the state machine, because there is no queue state. A reference
  that names something that is no longer pending is simply discarded on the poll
  path.
- **No write amplification.** Dispatching a task costs zero storage I/O. The
  store write per operation is exactly the history append it was always going to
  do.
- **Tiny memory footprint.** At roughly 100 bytes per reference, the default
  50,000-entry cap is a few megabytes per queue, and a megabyte-sized activity
  input is read from the store once, on the dispatch path, where it is needed.
- **No broker to operate.** No Kafka, no Redis, no queue table to vacuum. The
  matching layer is a `map[QueueKey]*queue` and about 400 lines.
- **Backpressure is honest.** A full queue rejects rather than growing, and the
  rejection is safe precisely because the reference is reconstructible.

### What this costs

- **A restart costs a recovery scan.** Every open execution is read and rebuilt
  before the server accepts traffic. That is O(open executions) at startup, and
  it is the operation that will hurt first at scale. It is measured and logged
  (`engine recovered`, with a duration).
- **The queue is process-local.** A task dispatched inside one `skaldd` can only
  be polled from that `skaldd`. This is the single largest obstacle to running
  more than one replica, and it is not a small piece of work to fix: it needs
  either a shared queue (which reintroduces the problem this ADR removes) or
  poll forwarding between replicas by consistent hash on the queue name.
- **Dispatch is best-effort after commit.** `postCommit` logs a failed `Add` and
  moves on. That is correct — the write is already durable — but it means the
  path from "committed" to "pollable" has no retry of its own and relies on the
  recovery scan and on schedule-to-start timeouts.
- **A rejected task is invisible until a timeout.** With a full backlog, an
  activity waits for its schedule-to-start deadline (or the re-dispatch watchdog)
  rather than being retried promptly. `skald_tasks_dropped_total` is the signal.
- **Fairness is per-queue, not global.** A queue with a huge backlog and a queue
  with one task are served independently; there is no global scheduler balancing
  across queues.

## Alternatives considered

### A durable queue table in the same transaction

Write task rows alongside history events, `SELECT ... FOR UPDATE SKIP LOCKED` to
consume.

Rejected because it doubles the write volume of the hottest path to store
information that is already implied by the events being written in the same
transaction, and because it recreates exactly the reconciliation problem
described above. It would, however, be the natural design if matching had to
span processes — which is why this ADR should be revisited alongside any
multi-node work rather than defended in isolation.

### An external broker (Kafka, NATS, Redis, SQS)

Push a message per ready task.

Rejected on operational cost and on semantics. It adds a second system to run,
monitor and back up, for a payload the engine can regenerate for free. Worse, it
introduces a second delivery guarantee that has to be reconciled with the state
machine's: an at-least-once broker plus an at-least-once state machine is not
composition, it is two independent sources of duplicates.

### Persist references only, not payloads, in the store

A middle path: a small `pending_tasks` table of references, written with the
history.

Rejected because it buys much less than it looks like. The recovery scan it would
avoid is only needed at startup, and the table still has to be kept in agreement
with the state machine on every transition — which is the expensive half of the
problem, not the storage.

### Evict the oldest when a queue is full

Rejected explicitly. Evicting the oldest punishes the task that has already
waited longest, turning a full queue into unbounded latency for the unlucky.
Rejecting the newest keeps the queue's waiting time bounded and pushes the
failure to the caller, which is the engine, which can log it and rely on
recovery.

### Rebuild queues lazily on first poll instead of at startup

Scan only the queues a worker actually polls.

Rejected because the poll path would then need to distinguish "this queue is
empty" from "this queue has not been scanned yet", and a worker's first poll on a
busy queue would block on a full table scan. Doing it once, before the listener
opens, keeps the poll path simple and makes the cost visible in one startup log
line.
