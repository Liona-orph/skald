# 0009. Payload size and history length limits

- **Status**: Accepted
- **Date**: 2026-03-16

## Context

Two quantities in Skald are unbounded unless something bounds them, and both
degrade the system in ways that are hard to attribute after the fact.

**Payload size.** Every activity input, activity result, signal payload, workflow
input and workflow result is copied into a history event. It is then read back on
every replay, sent over the wire on every workflow task, and held in memory
twice while the response is buffered. A 50 MiB activity input is not 50 MiB of
storage; it is 50 MiB multiplied by every task that replays past it.

**History length.** Replay cost is linear in history length. A worker picking up
a task on a 200,000-event history reconstructs state from event 1, and the
workflow task deadline is 10 seconds. Past some length, every workflow task times
out, the engine reschedules, the next worker also times out, and the execution is
permanently wedged in a way that looks like a worker problem rather than a
history problem.

Both failures are gradual. Nothing breaks at 100 events or at 100 KiB; things
break somewhere, later, under load, on the execution nobody was watching.

## Decision

Explicit, named constants, enforced at the boundary where they can produce a
useful error.

| Constant | Value | Enforced |
| --- | --- | --- |
| `skald.MaxPayloadBytes` | 2 MiB | `JSONConverter.ToPayload` returns `ErrPayloadTooLarge`; `skaldctl` checks input before sending |
| `history.MaxHistoryEvents` | 50,000 | `History.Validate`, which runs on every `Rebuild` |
| `history.MaxHistoryBytes` | 64 MiB | Declared, **not enforced** |
| `frontend.DefaultMaxRequestBytes` | 8 MiB | `http.MaxBytesReader` on every request body |
| `matching.DefaultMaxBacklog` | 50,000 references | `queue.offer` returns `ErrBacklogFull` |
| `client.DefaultMaxResponseBytes` | bounded read | `io.LimitReader` on every response |

Supporting decisions:

- **The payload limit is an error at the call site, not a truncation.** A
  silently truncated payload becomes a corrupt history event discovered days
  later; an error at `ToPayload` is discovered by the person who wrote the line.
- **The request limit is deliberately larger than the payload limit.** A
  `RespondWorkflowTaskCompleted` may carry a batch of commands each with its own
  payload, so one legitimate request is several payloads plus framing. It is
  still bounded, because an unbounded body is a one-request denial of service
  against a process that must hold the decoded value in memory.
- **The guidance for large data is a reference, not a payload.** Put the object
  in blob storage and pass the key. The engine copies payloads into every event
  that mentions them, so an unbounded payload is unbounded write amplification.
- **The history limit's escape hatch is `ContinueAsNew`.** A workflow that
  legitimately needs more than 50,000 events should periodically start a
  successor run with its state compressed into the input. A workflow that does
  not is almost always looping.

## Consequences

### What this buys

- **The maximum size of a persisted row is knowable without reading code.** An
  operator sizing storage or a proxy can read the constants.
- **Failures are loud and early.** `ErrPayloadTooLarge` names the actual size and
  the limit. A 413 with a `limit_bytes` detail tells a client exactly what to
  change.
- **One workflow cannot exhaust the server.** The request cap, the backlog cap
  and the response cap each bound a distinct resource, and each has a defined
  behaviour when hit.
- **The history limit makes an unbounded loop a bug report rather than an
  outage.** A workflow that appends forever hits a stated limit instead of slowly
  making every replay more expensive until the deployment falls over.

### What this costs

- **The history limit is enforced on read, not on write.** This is the sharpest
  edge in the design. `History.Validate` runs during `Rebuild`, so an execution
  that exceeds 50,000 events keeps working from the engine's state cache and then
  becomes **unloadable** on the next cache miss, surfacing as an internal error
  rather than as a clear "this workflow is too long". The right fix is a check in
  `AppendEvent` or `commit` that fails the workflow task with
  `WorkflowTaskFailedCauseResourceExhausted` — a cause code that already exists
  and is never produced. It has not been done.
- **`MaxHistoryBytes` is decorative.** It is declared, referenced in one client
  doc comment, and enforced nowhere. A history of 50,000 events each carrying a
  2 MiB payload is 100 GiB and violates no check.
- **2 MiB is a guess.** It is large enough for any reasonable structured payload
  and small enough that the copying is bounded, but there is no measurement
  behind the specific number, and a deployment with different economics cannot
  change it without a fork — it is a `const`, not configuration.
- **The limits are not per-namespace.** Multi-tenancy is always on, but every
  tenant gets the same limits. There is no quota system and no per-namespace
  rate limiting of any kind.
- **`ContinueAsNew` is manual.** The author has to know the limit exists, know
  roughly how many events their loop generates per iteration, and remember to
  call it. Nothing warns as an execution approaches the limit.

## Alternatives considered

### No limits; let operators size their own storage

Rejected because the failure modes are not proportional to the abuse. A single
oversized payload does not fail — it makes every replay of that execution
slightly slower, forever, for everyone sharing the process. Unbounded history
does not fail — it eventually makes every workflow task time out, which presents
as a worker problem. Both are much cheaper to prevent than to diagnose.

### Configurable limits per deployment

Make all of them fields on `Config`.

Rejected for now, and this is the weakest of the rejections. The argument against
is that `MaxPayloadBytes` in particular is baked into the wire format's
expectations and into the frontend's request cap, so a deployment that raised it
would be incompatible with a client that did not — and the constants are in
`pkg/skald`, which user workflow code imports, so they are effectively public
API. The argument for is that the numbers are guesses. If this changes, the
payload limit should become a server-advertised value the client reads, not a
flag on both sides.

### Compress payloads transparently

Store a gzipped payload and decompress on read.

Rejected at the storage layer, accepted at the transport layer. `Payload` is
deliberately codec-agnostic and self-describing — the engine never inspects
`Data` — so compression belongs in a `DataConverter`, which anyone can supply
without a history migration: old events keep declaring their original encoding
and stay readable forever. On the wire, the frontend already gzips responses
above 1 KiB, which is where the real win is (history JSON compresses roughly ten
to one).

### Automatic continue-as-new when a history gets long

Have the SDK force a successor run once the history passes a threshold.

Rejected because the SDK cannot know what state to carry forward. Continue-as-new
requires compressing the workflow's meaningful state into the successor's input,
which is a domain decision. An automatic version would either lose state or have
to serialise the entire workflow, which is the thing Go cannot do and the reason
[ADR 0001](0001-event-sourcing.md) exists. A warning as the limit approaches —
in `DescribeWorkflow`, or as a metric — is the achievable version and is worth
adding.

### Truncate oversized payloads with a marker

Store the first 2 MiB plus "truncated".

Rejected outright. A truncated payload is a corrupt one that looks valid, and the
consumer is workflow code that will decode it into a struct and act on it. An
error is always better than a plausible lie.
