# Architecture decision records

An ADR records one decision that shaped the system: what was going on when it
was made, what was chosen, and what the choice costs. It is not documentation of
how the code works — that is [ARCHITECTURE.md](../ARCHITECTURE.md) — and it is
not a design proposal. It is a note to whoever has to change this later,
explaining why the obvious alternative was not taken.

The format is Michael Nygard's: **Title, Status, Context, Decision,
Consequences**. Skald's records add an **Alternatives considered** section,
because the alternatives are the part that is genuinely hard to reconstruct
after the fact. A consequence you can find by reading the code; a rejected
option you cannot.

## Rules

- **Numbered and immutable.** A record that has been merged is not edited except
  to change its status or add a link. If the decision changes, write a new
  record that supersedes it and mark the old one `Superseded by ADR-NNNN`.
- **One decision per record.** If it needs two headings, it is two records.
- **Written when the decision is made**, not when the code is finished. A record
  written afterwards tends to justify what was built rather than explain what
  was chosen.
- **Honest about the cost.** Every consequences section here has a negative half.
  A record with no downsides is a record that has not thought hard enough.
- **Statuses**: `Proposed`, `Accepted`, `Superseded by ADR-NNNN`, `Deprecated`.

## Index

| # | Title | Status | Date |
| --- | --- | --- | --- |
| [0001](0001-event-sourcing.md) | Event sourcing as the durability model | Accepted | 2026-01-06 |
| [0002](0002-http-json-transport.md) | HTTP/JSON transport instead of gRPC | Accepted | 2026-01-09 |
| [0003](0003-cooperative-coroutine-dispatcher.md) | A cooperative coroutine dispatcher for deterministic replay | Accepted | 2026-01-15 |
| [0004](0004-derived-task-queues.md) | Derived, non-persisted task queues | Accepted | 2026-01-22 |
| [0005](0005-optimistic-concurrency.md) | Optimistic concurrency instead of leases or a lock service | Accepted | 2026-01-27 |
| [0006](0006-pluggable-storage-sqlite-reference.md) | Pluggable storage with SQLite as the reference driver | Accepted | 2026-02-03 |
| [0007](0007-record-retry-jitter-in-history.md) | Recording retry jitter in the history | Accepted | 2026-02-17 |
| [0008](0008-refuse-to-advance-on-non-determinism.md) | Refusing to advance an execution on non-determinism | Accepted | 2026-03-02 |
| [0009](0009-payload-and-history-limits.md) | Payload size and history length limits | Accepted | 2026-03-16 |
| [0010](0010-deterministic-simulation-testing.md) | Deterministic simulation testing as the primary correctness strategy | Accepted | 2026-04-07 |

## Writing a new one

Copy the skeleton below into `docs/adr/NNNN-short-slug.md`, take the next free
number, and open a pull request. Discussion happens on the pull request; the
record is merged with status `Accepted` once the decision is actually made.

```markdown
# NNNN. Title in the imperative

- **Status**: Proposed
- **Date**: YYYY-MM-DD
- **Deciders**: who was in the room

## Context

The forces at play. What is true today, what pressure prompted the decision,
what constraints are not negotiable. Written so that someone who was not there
can tell whether the constraints still hold.

## Decision

What was chosen, stated plainly and in the present tense.

## Consequences

### What this buys

### What this costs

## Alternatives considered

### Option that was rejected

Why it was rejected, in terms of the forces above rather than in terms of taste.
```
