# 0006. Pluggable storage with SQLite as the reference driver

- **Status**: Accepted
- **Date**: 2026-02-03

## Context

The engine needs durable storage for four things: execution rows, history
events, a due-time index for timers, and enough of an index to answer visibility
queries. Everything else is derived ([ADR 0004](0004-derived-task-queues.md)).

Two pressures point in opposite directions.

The storage layer is where a durable execution engine's real behaviour lives —
transaction boundaries, the compare-and-set that arbitrates writers ([ADR
0005](0005-optimistic-concurrency.md)), the cost of an `fsync`. Coupling to one
database means the engine's design absorbs that database's assumptions, and
changing later is a rewrite.

At the same time, an abstraction wide enough to admit anything admits nothing
well. A `Store` interface with a generic query builder would let every driver
disagree about semantics, and the engine would end up coding to the weakest one.

There is also a getting-started pressure. A durable execution engine that
requires a Postgres cluster before "hello world" will not be tried.

## Decision

Storage is a **small, opinionated Go interface** — `persistence.Store`, ten
methods — with two drivers in the repository:

- **`memory`** — a map-based store for tests, examples and demos, with optional
  deterministic fault injection (`FaultConfig`: seed, conflict rate, error rate,
  latency function).
- **`sqlite`** — the reference driver, on `modernc.org/sqlite`, a pure-Go
  translation of the SQLite amalgamation.

The interface encodes the two rules that make the engine work, rather than
leaving them to each driver:

1. **The history is the only durable truth.** There is no queue to persist and no
   task table to keep in agreement.
2. **Every write is conditional.** Callers pass the version they read; the store
   rejects the write if it changed.

`AppendHistory` is transactional across all three things it touches — events, the
execution row, and the timer index — or it is nothing.

Every driver must pass `internal/persistence/persistencetest`, a shared
conformance suite that asserts the contract rather than the implementation.

### Why SQLite specifically

- `modernc.org/sqlite` is cgo-free, so a Skald binary cross-compiles and ships
  as a static executable with no C toolchain. The cost is perhaps a third of cgo
  SQLite's throughput, which is the right trade for a system whose bottleneck is
  `fsync` rather than CPU.
- WAL mode gives concurrent readers alongside a single writer, which is exactly
  the engine's access pattern.
- The whole schema is four tables and six indexes, in one file, readable in one
  sitting — which makes it a *reference* driver in the useful sense: someone
  writing a Postgres driver can read it and know what is required.
- A single-node Skald on SQLite genuinely survives process crashes, machine
  restarts and rolling upgrades. What it does not survive is the loss of the
  machine.

### Schema decisions worth restating

- **Migrations are an ordered slice compiled into the binary**, append-only, with
  no `Down`. A rollback of a released migration is a new migration. There is no
  way to deploy a build whose code and schema disagree because somebody forgot to
  ship the `.sql` files.
- **Times are unix nanoseconds as integers.** Ordering, range scans and the
  pagination cursor stay exact, where a text timestamp would depend on formatting
  and a float would lose precision above microseconds.
- **Pagination is keyset, never `OFFSET`.** The trailing columns of
  `executions_recent` are exactly the cursor, so a page is a bounded range scan.
- **`synchronous=NORMAL` in WAL mode by default.** A process crash, a `kill -9`
  or an OOM loses nothing; a power cut or kernel panic can lose the last few
  committed transactions. That is honest for a workflow engine, because Skald
  acknowledges a start only after commit returns — the transactions such a crash
  loses are ones no client was told about. `WithSynchronous(SynchronousFull)`
  buys the stronger guarantee at roughly an order of magnitude fewer writes per
  second on spinning media.
- **PRAGMAs go in the DSN, not in a statement after `Open`.** PRAGMAs are
  per-connection and `database/sql` opens connections lazily; a PRAGMA executed
  once would apply to one connection and silently not to the next.

## Consequences

### What this buys

- **Zero-dependency start.** `skaldd --store sqlite --sqlite-path ./skald.db` is
  a complete durable engine. `--store memory` is a complete ephemeral one, and it
  warns loudly at startup that state is lost on restart.
- **A new driver is a bounded piece of work.** Ten methods and a conformance
  suite that tells you when you are done, including the awkward parts — ID reuse
  policies, request-ID deduplication, timer index diffing, keyset pagination.
- **Deterministic fault injection for free.** The memory driver can fail reads,
  return version conflicts and add latency from a seeded RNG, which is what makes
  the simulation strategy in [ADR 0010](0010-deterministic-simulation-testing.md)
  possible without a network.
- **Tests run in milliseconds.** The whole suite is well under ten seconds
  because most of it never touches a disk.

### What this costs

- **The interface is a real constraint on the engine.** It has no primitive that
  closes one run and opens another atomically, which is precisely why successor
  creation (continue-as-new, workflow retry) is two transactions with a window
  in between. That gap is a direct consequence of keeping the interface small.
- **Single machine, for now.** Neither shipped driver spans hosts. The interface
  admits one; nothing implements one.
- **Visibility is deliberately weak.** `ListExecutions` supports exact-match
  filters on workflow ID, type and status with keyset pagination, and nothing
  else. Filtering by workflow type has no index of its own — it scans
  `executions_recent` and discards non-matches — because a fourth B-tree would
  slow every write to make an uncommon filter fast. A deployment that queries by
  type at scale wants a real search index, which the interface does not model.
- **Memo and search attributes are opaque.** They are stored and returned; they
  are not queryable.
- **The lowest common denominator leaks.** Because SQLite is the reference,
  features a networked database would offer — server-side cursors, partial
  indexes over JSON, listen/notify for long-poll wakeups — are not in the
  interface, and a driver that has them cannot expose them without widening it.
- **Nothing is ever deleted.** There is no retention, archival or delete
  operation in the interface. Closed executions accumulate.

## Alternatives considered

### Postgres as the only store

Rejected for the getting-started cost and because it would have coupled the
engine's design to one database's transaction semantics before the engine's
requirements were understood. It remains the most likely first networked driver,
and the interface was shaped with it in mind — `AppendHistory`'s conditional
write maps to a single `UPDATE ... WHERE version = $n` plus an insert, and the
keyset pagination maps directly.

### cgo SQLite (`mattn/go-sqlite3`)

Faster, and the more common choice.

Rejected because it makes cross-compilation require a C toolchain per target and
makes the container image need libc. For an engine whose commit path ends in an
`fsync`, the throughput difference is not where the time goes. A deployment that
measures otherwise can write a ten-line driver wrapper.

### A key-value store (BadgerDB, Pebble, bbolt)

Attractive: append-only history is a natural fit for an LSM tree.

Rejected because the visibility query — "list running workflows of this type,
newest first, paginated" — becomes a secondary index the driver has to build and
maintain by hand, and the timer index becomes a second one. SQLite gives both for
free, correctly, with a query planner. This would be a good choice for a driver
optimised purely for write throughput, and the interface admits it.

### An ORM or a generic query abstraction

Rejected outright. Every query in the driver is on a hot path and was written
against a specific index; an abstraction that hides which index is used hides the
only thing worth reviewing about it.

### No abstraction at all — a concrete `Store` struct

Simpler, and honest about there being one implementation.

Rejected because the conformance suite is worth more than the interface itself.
Having two implementations from day one — one durable, one with fault injection —
is what keeps engine code from accidentally depending on a driver detail, and it
is what makes the fault-injection tests possible at all.
