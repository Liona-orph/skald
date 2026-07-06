# 0003. A cooperative coroutine dispatcher for deterministic replay

- **Status**: Accepted
- **Date**: 2026-01-15

## Context

[ADR 0001](0001-event-sourcing.md) makes replay the recovery mechanism. That
imposes a hard requirement on the SDK: given the same history, workflow code
must reach the same instructions in the same order and produce the same commands
in the same sequence.

Ordinary Go defeats this in four independent ways:

- **The runtime scheduler is non-deterministic.** Two goroutines that both become
  runnable interleave differently on every run, and on a different number of
  cores they interleave differently again.
- **`select` picks uniformly at random** among ready cases. That is correct for a
  concurrent program and exactly wrong for a replayable one.
- **A blocked goroutine cannot be told to stop.** A workflow instance evicted
  from a cache while three of its goroutines are parked on channels leaks all
  three, permanently.
- **The scheduler is invisible.** Nothing in Go can answer "is any goroutine in
  this workflow still able to make progress?", which is the exact question that
  decides when a command batch is complete.

At the same time, the whole appeal of the model is that workflow code *looks
like* ordinary Go. An API that forced authors to write an explicit state machine
would give up the reason for existing.

## Decision

Workflow code runs on **coroutines**: real goroutines scheduled by
`internal/workflow.Dispatcher` rather than by the Go runtime.

- Exactly one of {dispatcher, coroutine} runs at any instant, enforced by a pair
  of unbuffered channels used as a hand-off.
- A coroutine "blocks" by yielding control back to the dispatcher, never by
  parking on a runtime primitive. The dispatcher therefore always knows the
  complete set of runnable work.
- `Dispatcher.ExecuteUntilAllBlocked` runs every coroutine, **in creation
  order**, repeating passes until a pass makes no progress. At that fixpoint the
  command batch is complete, and it is complete deterministically — there is no
  "and then a goroutine woke up a microsecond later and added one more".
- Creation order is a property of the workflow code, which is itself
  deterministic, so two replays interleave identically.

The SDK supplies dispatcher-aware replacements for the primitives an author
would otherwise reach for: `workflow.Go`, `workflow.Channel`,
`workflow.Selector`, `workflow.Future`, `workflow.Await`, and a
`workflow.Context` whose `Done` hands out a workflow channel rather than a
runtime one.

`workflow.Selector` runs the **first ready branch in registration order**. It
expresses a priority, not a race.

### The progress rule

A pass marks progress on exactly four events:

1. a coroutine completes,
2. a coroutine is created,
3. a future settles or a channel operation moves a value,
4. a coroutine **enters a new blocking call**.

Case 4 is load-bearing. Coroutine B awaits a condition that coroutine A makes
true, and B is scheduled first. Pass 1: B checks, false, yields. A runs, sets the
flag, then blocks on an activity — entering that blocking call marks progress, so
a second pass happens and B observes the flag. Without case 4 the dispatcher
stops one pass early and the workflow stalls.

Termination holds because entering a *new* blocking call requires executing user
code; a coroutine that re-checks the wait it is already parked on marks nothing.

## Consequences

### What this buys

- **Workflow code looks like Go.** `workflow.Go(ctx, func(ctx workflow.Context) {
  ... })` reads like `go func() { ... }()`, and the fan-out/fan-in patterns
  authors already know work.
- **No locks in user code, ever.** One coroutine runs at a time, so a workflow's
  shared variables need no synchronisation. This removes an entire class of bug
  from the code most likely to be written by someone who is not a concurrency
  specialist.
- **The command batch is a well-defined object.** "Everything the workflow
  decided given this history" has a precise meaning: the fixpoint.
- **Deadlocks are diagnosable.** `DeadlockError` names every unfinished
  coroutine, what each is blocked on, and how many times it has yielded. A
  workflow that hangs in production is diagnosed from that text alone.
- **Clean teardown.** `Dispatcher.Close` unwinds every parked coroutine with a
  sentinel panic and waits for it to exit, so an evicted or shut-down workflow
  instance leaves no goroutines behind. A coroutine that swallows the sentinel is
  reported by name as leaked.

### What this costs

- **Two APIs that look alike and are not.** `workflow.Context` is not
  `context.Context`; `workflow.Channel` is not `chan`. Mixing them is the most
  common authoring mistake, and it is why registration validates that a workflow's
  first parameter is a `workflow.Context` and an activity's is a
  `context.Context`.
- **The standard library is off limits inside a workflow.** `time.Sleep`,
  `sync.WaitGroup`, `errgroup` and anything else that parks a goroutine on a
  runtime primitive will block a coroutine in a way the dispatcher cannot see,
  and the workflow task will fail on the deadlock deadline rather than hanging
  forever — which is the best available outcome, not a good one.
- **A goroutine per coroutine.** The dispatcher does not multiplex; each
  coroutine is a real goroutine parked on a hand-off channel. A workflow that
  spawns ten thousand coroutines costs ten thousand goroutines in the worker.
- **The hand-off is a context switch.** Every yield is two channel operations.
  For a workflow with many coroutines and many passes this is measurable, and it
  is the reason `DeadlockDetectionTimeout` exists at all.
- **`Close` can fail.** Workflow code that recovers from every panic swallows the
  unwind sentinel, and the coroutine is leaked. The dispatcher retries ten times
  and then reports it by name.

## Alternatives considered

### A CPS transform or explicit state machine API

Force the author to write `Step1 -> Step2 -> Step3` as data, so there is no
control flow to make deterministic.

Rejected because it gives up the entire value proposition. The reason to use a
durable execution engine rather than a queue-per-step design ([ADR
0001](0001-event-sourcing.md)) is that the business process stays readable as a
program. An API that turns it back into a transition table has reinvented the
thing it replaced, with more ceremony.

### Restrict workflows to single-threaded code

No `workflow.Go`, no selectors, no concurrency at all. Determinism is then
trivial.

Rejected because the patterns that need concurrency are the ones people actually
write: fan out to N activities and wait for all, race an approval signal against
a 48-hour timer, run a compensation path while the main path unwinds. A
single-threaded API forces those into hand-rolled polling loops, which are both
uglier and easier to get wrong.

### Real goroutines with a global workflow mutex

Let the Go runtime schedule, but serialise with a mutex so only one goroutine
runs at a time.

Rejected because it does not achieve determinism. Which goroutine acquires the
mutex next is up to the runtime, so the interleaving — and therefore the command
order — still varies between replays. It also cannot answer "is anything
runnable", so there is no way to know when the batch is complete.

### Compile-time instrumentation

Rewrite workflow code at build time to yield at every blocking point, like a
generator transform.

Rejected as far too much machinery for the benefit. It requires owning a build
step, breaks `go build`, makes stack traces lie, and would still need the same
dispatcher underneath to decide scheduling order. The explicit-primitives
approach costs the author a different `Context` type and buys the same property.

### Goroutine-per-coroutine with `runtime.Gosched` cooperation

Have coroutines yield with `runtime.Gosched` instead of an explicit hand-off.

Rejected because `Gosched` is a hint, not a transfer of control. The dispatcher
would have no way to know that a yield had actually happened, and no way to
prevent two coroutines running at once on a multi-core machine.
