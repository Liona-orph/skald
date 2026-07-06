# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the major version is 0, the minor version is bumped for breaking changes
and the patch version for everything else.

Two guarantees are stronger than the version number and hold across every
release:

- **History event type codes are permanent.** A code that has shipped is never
  reused for a different meaning, because histories written years ago must still
  decode.
- **The protocol version is bumped for any breaking wire change.** See the
  [versioning policy](docs/protocol.md#versioning-policy).

## [Unreleased]

## [0.1.0] - 2026-08-09

First release. Everything below is present and tested; the limitations section
of the [README](README.md) is an equally honest account of what is not.

### Added

#### Durability model

- Append-only, dense, 1-based event history as the only durable truth, with 25
  event types across five code blocks and structural validation of every
  invariant: dense IDs, a `WorkflowExecutionStarted` first event, non-decreasing
  timestamps, a terminal event only in last position, and back-references that
  name an earlier event of the correct type.
- `execution.MutableState` as a pure fold over the history: one mutating entry
  point, no I/O, no clock reads, no randomness, so the replay path is exercised
  on every request rather than only during recovery.
- Codes 100–119 reserved for child workflows, so a future implementation does
  not have to renumber and a history from a newer version is rejected precisely
  rather than misparsed.

#### Engine

- A single write path — lock, load, transition, conditional write, then apply
  effects — with effects deliberately applied after the commit so a failed write
  can never produce a duplicate dispatch.
- Optimistic concurrency on a version token, with a bounded, backoff-free retry
  loop. No leases, no lock service.
- Striped per-workflow-ID locking (1024 stripes) as a latency optimisation.
- A rebuilt-state LRU cache, validated against the execution version on every
  use and dropped on every error.
- Durable timers derived in full on every write, so a deadline can never be
  orphaned by a code path that forgot to disarm it.
- Recovery scan at startup that re-materialises pending task references from
  open executions, and deliberately does not re-dispatch work the history shows
  as in flight.
- Workflow-level retry with the successor's first task deferred by a timer on
  the successor.
- Continue-as-new with a walkable run chain via `first_execution_run_id`.
- Four ID reuse policies: allow-duplicate, failed-only, reject, and
  terminate-if-running (which writes a `WorkflowExecutionTerminated` event
  explaining itself).
- Signal-with-start as one store transaction, so two racing callers cannot both
  start.

#### Activities

- Four independent timeout clocks — schedule-to-start, start-to-close,
  schedule-to-close, heartbeat — with one timer index row per activity, armed at
  the nearest deadline and re-classified when it fires.
- Retries that write no history event: an activity retried a thousand times
  still contributes two events.
- Exponential backoff with one-sided jitter whose seed is recorded in
  `ActivityTaskStarted`, so replay recomputes the same delay.
- Heartbeats that extend a durable deadline, carry a checkpoint to the next
  attempt, and report cancellation back to the activity — without writing a
  history event.
- Cooperative activity cancellation, observed through the heartbeat response.

#### Workflow SDK

- A cooperative coroutine dispatcher that runs workflow code to a deterministic
  fixpoint, with a documented four-case progress rule and a `DeadlockError` that
  names every blocked coroutine and what it is waiting on.
- Deterministic replacements for Go's concurrency primitives: `Go`, `Channel`,
  `Selector` (first ready branch in registration order, never random), `Future`,
  `Await`, and a `workflow.Context` whose `Done` hands out a workflow channel.
- `Now`, `Rand` and `NewUUID` driven by history: the timestamp of the current
  workflow task, and two independent deterministic streams from one server-drawn
  seed.
- `SideEffect` and `MutableSideEffect`, the latter recording a marker only when
  the value changes.
- `ExecuteLocalActivity`, run inline and recorded as a marker.
- `GetVersion`, the versioning gate that pins in-flight executions to the branch
  they started on and fails loudly when a still-needed branch has been deleted.
- `NewDisconnectedContext` for compensation that cancellation cannot cancel.
- Non-determinism detection as a single command queue matched against
  command-derived history events, with a diagnostic that names the workflow
  type, the execution, the event, the position in the batch, the expected
  effect, the actual command and the call that produced it.
- Replay-aware logging: statements emitted during replay are dropped, so a log
  line remains evidence that something happened.

#### Worker

- Registration-time signature validation, so a malformed workflow fails at
  startup rather than at its first task.
- A sticky execution cache that instances are *taken* from rather than borrowed,
  with every death path explicit, so no instance is closed while in use and none
  leaks on eviction.
- Bounded concurrency and poller counts, with pollers clamped to the
  corresponding concurrency limit.
- Draining shutdown: polling stops immediately, running tasks get the caller's
  deadline, and an expired deadline is reported rather than hidden.
- `worker.Replayer` for offline replay of recorded histories in CI, with side
  effects and local activities served from markers rather than re-executed.

#### Protocol and transport

- HTTP/JSON protocol version 1: 16 operations, stable error codes carried in the
  body, and a `Skald-Protocol-Version` header.
- Long polling with a 50-second server cap chosen against the 60-second idle
  timeout of common proxies, and validated at startup against the connection
  idle timeout.
- Strict request decoding: unknown fields rejected, trailing documents rejected,
  bodies capped.
- Response compression above 1 KiB, with double-encoding avoided for handlers
  that encode their own body.
- Panic containment placed innermost, below the compression layer, so a panic
  cannot flush an empty 200.
- Static bearer authentication with constant-time comparison, `/health` and
  `/ready` public and everything else — including `/metrics` — protected.
- Liveness and readiness as genuinely different questions, with readiness
  failing at the start of a drain so a rolling restart is invisible to clients.

#### Storage

- `persistence.Store`: ten methods, transactional appends, conditional writes,
  and a shared conformance suite every driver must pass.
- SQLite driver on `modernc.org/sqlite` (cgo-free), WAL mode, PRAGMAs in the
  DSN, append-only in-binary migrations with no `Down`, integer nanosecond
  timestamps, and keyset pagination with no `OFFSET`.
- In-memory driver with seeded fault injection: version conflicts, transient
  errors and latency.

#### Client and CLI

- `pkg/client`: `api.Service` over HTTP with retry, jitter, separate deadlines
  for long polls, and a `WorkflowHandle` that follows continue-as-new chains and
  reconstructs typed errors.
- `skaldctl`: `workflow start|signal|cancel|terminate|describe|history|list|result|replay`
  and `taskqueue describe`, with aligned terminal output, `--output json` on
  everything, and exit code 2 reserved for "the workflow failed" as distinct
  from "the command failed".

#### Observability

- Prometheus metrics with a hard cardinality rule: never labelled by workflow
  ID, run ID, activity ID or request ID.
- Separate latency histograms for polling and non-polling operations, with
  bucket sets chosen from what each operation actually does.
- OpenTelemetry tracing, wired and free, with a no-op provider until an exporter
  is supplied and no use of OpenTelemetry's process-global setters.
- Structured logging with request, trace and execution correlation.

#### Testing

- A virtual clock that makes timeout and backoff paths cheap enough to test
  exhaustively, with `BlockUntil` as the synchronisation primitive.
- The full suite runs in seconds, with no `time.Sleep` and no polling.

### Known limitations

Stated here so a reader of the changelog alone is not misled. Details in the
[README](README.md#what-skald-is-not).

- Single-node: matching is process-local and both drivers are single-machine.
- No child workflows, no cron execution (`cron_schedule` is stored and never
  acted on), no queries, no updates, no external workflow signalling.
- An activity's live attempt counter lives in the timer index, not the history.
- Workflow task ownership is checked by identity, not by a token.
- `history.MaxHistoryEvents` is enforced on rebuild rather than on append, so an
  over-long run becomes unloadable rather than failing cleanly;
  `history.MaxHistoryBytes` is not enforced at all.
- `WorkflowTaskFailedCauseResourceExhausted` is never produced.
- Local activities do not retry.
- No TLS, no authorization, no retention or archival.

[Unreleased]: https://github.com/skald-io/skald/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/skald-io/skald/releases/tag/v0.1.0
