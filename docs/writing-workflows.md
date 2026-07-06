# Writing workflows

A workflow is a Go function that Skald can re-run from the beginning at any
time and get the same answer. Everything in this guide follows from that one
sentence.

If you read nothing else, read [the determinism
checklist](#the-determinism-checklist) and [common
mistakes](#common-mistakes-and-their-symptoms).

## Contents

- [The mental model](#the-mental-model)
- [The determinism checklist](#the-determinism-checklist)
- [Activities](#activities)
- [Timers](#timers)
- [Signals](#signals)
- [Selectors](#selectors)
- [Cancellation and compensation](#cancellation-and-compensation)
- [Side effects](#side-effects)
- [Versioning](#versioning)
- [Continue-as-new](#continue-as-new)
- [Testing](#testing)
- [Common mistakes and their symptoms](#common-mistakes-and-their-symptoms)

## The mental model

Your workflow function does not run once. It runs **once per workflow task**,
from the top, every time. On the first task it runs against a one-event history
and gets as far as its first blocking call. On the fifth task it runs against a
longer history: everything that already happened is replayed from that history
instead of being performed again, and only the tail is new.

Two consequences follow, and they are the whole of the discipline:

**1. The code must reach the same decisions given the same history.** If replay
takes a different branch than the original run did, Skald detects it and refuses
to advance the execution ([ADR
0008](adr/0008-refuse-to-advance-on-non-determinism.md)). That is a safe outcome
but a disruptive one.

**2. Everything the outside world touches goes through an activity.** A workflow
that calls an HTTP API directly will call it again on every replay. A workflow
that calls it through `ExecuteActivity` calls it once, ever, because the result
is in the history.

The signatures are:

```go
func MyWorkflow(ctx workflow.Context, args ...) (Result, error)
func MyActivity(ctx context.Context, args ...) (Result, error)
```

Note the two different `Context` types. The workflow one is not
`context.Context` — its `Done` has to hand out a workflow channel, not a runtime
channel, or a wait would be invisible to the scheduler. Registration validates
this, so getting it wrong fails at worker startup rather than at the first task.

## The determinism checklist

Run through this before merging any workflow.

**Never, inside workflow code:**

- [ ] `time.Now()`, `time.Since`, `time.Sleep`, `time.After`, `time.Tick`
- [ ] `math/rand`, `crypto/rand`, `uuid.New()`
- [ ] `go`, `chan`, `select`, `sync.Mutex`, `sync.WaitGroup`, `errgroup`
- [ ] Ranging over a `map` and letting the order affect what you do
- [ ] Network calls, file I/O, database queries, `os.Getenv` in a decision
- [ ] Reading a package-level variable that a deploy can change
- [ ] `context.Context` — the workflow gets a `workflow.Context`

**Instead:**

| Instead of | Use |
| --- | --- |
| `time.Now()` | `workflow.Now(ctx)` |
| `time.Sleep(d)` | `workflow.Sleep(ctx, d)` |
| `rand.Int()` | `workflow.Rand(ctx).Int()` |
| `uuid.New()` | `workflow.NewUUID(ctx)` |
| `go f()` | `workflow.Go(ctx, f)` |
| `make(chan T)` | `workflow.NewChannel[T](ctx)` |
| `select { ... }` | `workflow.NewSelector(ctx)` |
| A `sync.WaitGroup` | `workflow.Await(ctx, cond)` or a channel |
| Anything touching the outside world | `workflow.ExecuteActivity(ctx, Fn, args...)` |
| Something local and non-deterministic | `workflow.SideEffect(ctx, fn)` |
| `log.Printf` | `workflow.GetLogger(ctx)` |

**Also:**

- [ ] Sort before ranging over a map, or iterate a sorted key slice.
- [ ] Do not branch on `workflow.IsReplaying(ctx)`. It makes behaviour depend on
      how many times the workflow has crashed.
- [ ] Do not close over mutable state from outside the function.
- [ ] Every change to the *sequence* of activities, timers, side effects or
      markers in an already-deployed workflow needs a version gate.

## Activities

An activity is ordinary Go. It may fail, it may be slow, it may be retried.

```go
func ChargeCard(ctx context.Context, o Order) (string, error) {
    resp, err := payments.Charge(ctx, o.ID, o.Amount)
    if err != nil {
        return "", err
    }
    return resp.ChargeID, nil
}
```

Scheduling one:

```go
ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
    StartToCloseTimeout:    30 * time.Second,
    ScheduleToCloseTimeout: 10 * time.Minute,
    RetryPolicy: &skald.RetryPolicy{
        InitialInterval:        time.Second,
        BackoffCoefficient:     2,
        MaximumAttempts:        5,
        NonRetryableErrorTypes: []string{"InsufficientFunds"},
    },
})

var chargeID string
err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ChargeCard, o), &chargeID)
```

Options live on the context rather than on each call because a workflow almost
always wants one policy for a whole region of code, and repeating four timeouts
at every call site is how one activity ends up quietly with no retry policy.

Three shapes for reading a result:

```go
// Untyped future, decoded into a destination.
var chargeID string
err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ChargeCard, o), &chargeID)

// Typed future.
chargeID, err := workflow.ExecuteActivityAs[string](ctx, ChargeCard, o).Get(ctx)

// Result not needed.
err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, SendEmail, o))
```

### Fan-out

Schedule everything first, then wait. The commands all land in one batch, so
this is one workflow task and one store write rather than N of each.

```go
futures := make([]workflow.Future[*skald.Payload], 0, len(items))
for _, item := range items {
    futures = append(futures, workflow.ExecuteActivity(ctx, ProcessItem, item))
}
for _, f := range futures {
    if err := workflow.Wait(ctx, f); err != nil {
        return err
    }
}
```

### Errors that classify

`ApplicationError.Type` is what a retry policy matches on. Use it.

```go
func ChargeCard(ctx context.Context, o Order) (string, error) {
    if o.Amount <= 0 {
        return "", skald.NewNonRetryableError("InvalidAmount", "amount %d is not positive", o.Amount)
    }
    return "", skald.NewApplicationError("GatewayTimeout", "payment gateway did not respond")
}
```

In the workflow:

```go
var app *skald.ApplicationError
if errors.As(err, &app) && app.Type == "InsufficientFunds" {
    // compensate
}
```

Cancellation, termination and workflow-code panics are never retried, whatever
the policy says.

### Long activities: heartbeat

An activity that declares a `HeartbeatTimeout` must call
`worker.RecordHeartbeat` more often than that timeout, or it is killed and
retried.

```go
func ImportRows(ctx context.Context, file string) (int, error) {
    start := 0
    if worker.HasHeartbeatDetails(ctx) {
        _ = worker.GetHeartbeatDetails(ctx, &start) // resume where the last attempt stopped
    }
    for i := start; i < total; i++ {
        if err := ctx.Err(); err != nil {
            return i, err // the workflow asked us to stop
        }
        process(i)
        worker.RecordHeartbeat(ctx, i)
    }
    return total, nil
}
```

Two things happen at every heartbeat: the server's deadline is extended, and the
response says whether the workflow has asked the activity to stop — which the
worker turns into a cancellation of `ctx`. **An activity that never heartbeats
can never be cancelled.** Calls are throttled to roughly half the heartbeat
timeout, so a tight loop is safe.

### Activities must be idempotent

Skald guarantees **at-least-once** activity execution, not exactly-once. A
worker that dies after doing the work but before responding will have the
activity retried. Build idempotency on a stable key — `GetActivityInfo(ctx)`
gives you the activity ID and the scheduled event ID, both of which are stable
across attempts.

### Local activities

For a short, idempotent, low-risk call, `ExecuteLocalActivity` runs the function
inline in the workflow task and records the result as a marker. No scheduling
event, no dispatch, no separate worker: a 2ms lookup costs 2ms instead of two
round trips.

The price: it runs on the **workflow task's clock**, there is **no retry**, and
a worker crash mid-call means the work is simply redone. Anything that can take
seconds belongs in a real activity.

## Timers

`workflow.Sleep` is durable. It lives in the store, not in a runtime timer
wheel, so a workflow that sleeps for thirty days does not need a worker running
for those thirty days.

```go
if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
    return err // cancelled
}
```

`workflow.NewTimer` returns a future for use in a selector. A zero or negative
duration is a no-op that writes no history event.

Timer latency is bounded by the server's `--timer-interval` (one second by
default) plus jitter. Do not build anything that needs sub-second timer
precision.

## Signals

Signals are the only way information enters a running workflow from outside.
They are durable: a signal the server accepted will be delivered even if every
worker is down.

```go
type Approval struct {
    By string `json:"by"`
}

approvals := workflow.GetSignalChannel[Approval](ctx, "approve")
approval, ok := approvals.Receive(ctx) // blocks this coroutine
```

Signals that arrived before the workflow asked for the channel are already
buffered in it — including one delivered by signal-with-start before the
workflow ran its first instruction. The channel is unbounded, because by the
time the SDK sees a signal it is already durable in the history and refusing it
would lose data the server promised to deliver.

Signals are **not deduplicated**. A client that retries a signal delivers it
twice; the `request_id` field on the request is accepted and ignored. Carry your
own key in the payload if you need exactly-once semantics.

A common shape is a handler coroutine that drains signals for the life of the
workflow:

```go
workflow.GoNamed(ctx, "cancel-watcher", func(ctx workflow.Context) {
    ch := workflow.GetSignalChannel[string](ctx, "cancel-request")
    for {
        reason, ok := ch.Receive(ctx)
        if !ok {
            return
        }
        canceled = true
        canceledReason = reason
    }
})
```

Name your coroutines. It costs nothing and it is the difference between "3
coroutines blocked" and "the cancel-watcher is blocked on signal cancel-request"
in a deadlock report.

## Selectors

`workflow.Selector` is Go's `select` made replayable. It runs the **first ready
branch in registration order**, never a random one.

```go
approvals := workflow.GetSignalChannel[Approval](ctx, "approve")
deadline := workflow.NewTimer(ctx, 48*time.Hour)

var outcome string
sel := workflow.NewSelector(ctx)
sel.AddReceive(approvals, func() {
    a, _ := approvals.Receive(ctx)
    outcome = "approved by " + a.By
})
sel.AddFuture(deadline, func() {
    outcome = "expired"
})
sel.Select(ctx)
```

Ordering is a **priority**, not a race. A timeout registered before a result
always wins a tie; register it after and the result wins. That makes "prefer the
cached answer, fall back to the slow one" expressible, and it means a flaky test
cannot be caused by branch selection.

`AddDefault` makes the selector never block, which is how you poll for readiness
without waiting.

## Cancellation and compensation

Cancellation is cooperative. `CancelWorkflow` records a request and wakes the
workflow; the workflow decides what to do. A workflow that ignores it keeps
running — a payment that must not be abandoned halfway is allowed to say so.

When a context is cancelled, every activity future and timer future created under
it resolves immediately with a `*skald.CanceledError`, and the server is asked to
stop the corresponding activity. Resolution is immediate rather than after a
round trip, so compensation runs *now* instead of after a worker that may
already be gone acknowledges.

Which is exactly why compensation must run on a **disconnected** context:

```go
var chargeID string
if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, ChargeCard, o), &chargeID); err != nil {
    return err
}

if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
    // Cancelled. Refund on a context that cancellation cannot touch.
    cleanup := workflow.NewDisconnectedContext(ctx)
    _ = workflow.Wait(cleanup, workflow.ExecuteActivity(cleanup, Refund, chargeID))
    return err
}
```

On the plain `ctx`, the refund would be cancelled before it was ever dispatched.

`workflow.WithCancel` gives you a child context you can cancel yourself, which is
how you abandon one branch of a race without ending the workflow.

## Side effects

`workflow.SideEffect` runs a function exactly once for the life of the execution
and records the result in the history, so every replay reuses the value.

```go
region, err := workflow.SideEffect(ctx, func() string {
    return os.Getenv("AWS_REGION")
})
```

The function must not block, must not call any workflow API, and must not do
anything the workflow could not survive doing twice — a worker that dies between
running it and recording the marker will run it again.

For a value that legitimately changes over a long execution,
`MutableSideEffect` records a marker only when the value actually differs:

```go
enabled, err := workflow.MutableSideEffect(ctx, "new-pricing",
    func() bool { return featureFlags.Enabled("new-pricing") },
    func(a, b bool) bool { return a == b },
)
```

A workflow polling a flag once a minute for a month records one marker per
*change* instead of forty thousand.

Prefer an activity when the value comes from a network call. A side effect is
for something local: an environment-derived constant, an idempotency token, a
value from a library you cannot make deterministic.

## Versioning

This is the mechanism that lets you change a workflow while executions are in
flight. Use it for **any** change to the sequence of activities, timers, side
effects or markers a workflow produces.

`workflow.GetVersion(ctx, changeID, minSupported, maxSupported)`:

- The first time a run reaches `changeID`, it picks `maxSupported` and writes it
  into the history as a `Version` marker.
- Every later replay of that run reads the marker and returns the same number,
  forever.
- A run whose history predates the change has no marker and gets
  `workflow.DefaultVersion` (`-1`).

**The decision, not the code, lives in the history.** Nothing about the deployed
binary can change what an in-flight execution already decided.

### Worked example

**Before.** In production, and there are 40,000 executions mid-flight, some of
them sleeping for a week.

```go
func FulfilOrder(ctx workflow.Context, o Order) error {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
    })

    if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, ReserveInventory, o)); err != nil {
        return err
    }
    if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
        return err
    }
    return workflow.Wait(ctx, workflow.ExecuteActivity(ctx, ShipOrder, o))
}
```

**The change you want:** run a fraud check before reserving inventory.

**Wrong.** Deploying this wedges every in-flight execution. On replay, the
history's event 5 is `ScheduleActivityTask ReserveInventory` and the code now
produces `CheckFraud` — a non-determinism error, and every one of those 40,000
executions stops advancing until you roll back.

```go
// DO NOT DO THIS
if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, CheckFraud, o)); err != nil {
    return err
}
if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, ReserveInventory, o)); err != nil {
    return err
}
```

**Right.** Gate it.

```go
func FulfilOrder(ctx workflow.Context, o Order) error {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
    })

    v := workflow.GetVersion(ctx, "fraud-check-before-reserve", workflow.DefaultVersion, 1)
    if v >= 1 {
        if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, CheckFraud, o)); err != nil {
            return err
        }
    }

    if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, ReserveInventory, o)); err != nil {
        return err
    }
    if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
        return err
    }
    return workflow.Wait(ctx, workflow.ExecuteActivity(ctx, ShipOrder, o))
}
```

What each population does after the deploy:

| Population | `GetVersion` returns | Behaviour |
| --- | --- | --- |
| Started before the deploy | `DefaultVersion` (`-1`), because there is no marker | Skips `CheckFraud`, exactly as before |
| Started after the deploy | `1`, recorded as a marker on first execution | Runs `CheckFraud` |

Note that the gate itself adds a `RecordMarker` command for new runs. That is
fine — a run that predates the change never emits the marker, because emitting
one would add a command the recorded history does not have.

**Cleaning up.** Once every execution predating the change has closed — check
with `skaldctl workflow list --status RUNNING` and watch it drain — raise the
minimum and delete the old branch:

```go
_ = workflow.GetVersion(ctx, "fraud-check-before-reserve", 1, 1)
if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, CheckFraud, o)); err != nil {
    return err
}
```

A straggler that is still pinned to `DefaultVersion` now fails loudly with a
message naming the change and the version range, instead of silently taking the
wrong path. That is the entire reason the gate takes a minimum.

Do not delete the `GetVersion` call entirely until the marker itself can no
longer appear in any live history — removing it removes a command that recorded
histories still contain.

### Cheaper alternatives to versioning

- **Rename the workflow type.** New executions use `FulfilOrderV2`; old ones
  drain on the old code, which you keep registered until they are gone. No gate,
  no marker, at the cost of two implementations.
- **Additive changes need no gate.** Adding a field to an activity's input
  struct, changing an activity's *implementation*, or changing a retry policy
  does not change the command sequence.

## Testing

### Replay production histories in CI

This is the highest-value test you can write, and it is what turns a
non-determinism incident into a failed build.

```bash
skaldctl workflow history order-1234 --json > testdata/histories/fulfil-order.json
```

```go
func TestReplay(t *testing.T) {
    r := worker.NewReplayer(worker.ReplayOptions{})
    r.RegisterWorkflow(FulfilOrder)

    files, err := filepath.Glob("testdata/histories/*.json")
    if err != nil {
        t.Fatal(err)
    }
    for _, f := range files {
        if err := r.ReplayHistoryFile(context.Background(), f); err != nil {
            t.Errorf("%s: %v", f, err)
        }
    }
}
```

Replay is entirely offline: no server, no network, no side effects. Side effects
and local activities are served from the markers already in the history rather
than re-executed, which is what makes running it against production histories
safe.

Commit a handful of *representative* histories — the happy path, one with a
retry, one with a cancellation, one that continued as new — and add a new one
whenever a workflow changes shape.

### End-to-end with an embedded engine

`api.Service` is implemented by the engine as well as by the HTTP client, so a
test can run the whole system in one process with no server:

```go
store := memory.New()
eng, err := engine.New(engine.Config{Store: store})
if err != nil {
    t.Fatal(err)
}
if err := eng.Start(ctx); err != nil {
    t.Fatal(err)
}
defer eng.Close(ctx)

w := worker.New(eng, "test", worker.Options{})
w.RegisterWorkflow(FulfilOrder)
w.RegisterActivity(ChargeCard)
if err := w.Start(); err != nil {
    t.Fatal(err)
}
defer w.Stop(ctx)
```

Then start workflows through `eng` and assert on `DescribeWorkflow` and
`GetHistory`. This is what the tests in `pkg/worker` do, and it is why they are
real tests rather than mocks.

`memory.WithFaults(memory.FaultConfig{...})` adds seeded version conflicts,
transient errors and latency, which is how you exercise the retry paths that are
rare in production.

### Testing activities

Activities are ordinary Go functions taking a `context.Context`. Call them
directly. If one uses `worker.RecordHeartbeat` or `worker.GetActivityInfo`,
guard with `worker.IsActivityContext(ctx)` or exercise it through the embedded
engine.

### What to assert

Assert on the **history**, not on side effects in your test doubles. The history
is the contract: which activities were scheduled, in what order, with what
inputs. `GetHistory` gives you the exact event sequence, and it is stable in a
way "my mock was called twice" is not.

## Common mistakes and their symptoms

| Mistake | Symptom | Fix |
| --- | --- | --- |
| `time.Now()` in a workflow | Non-determinism error, or a workflow that behaves differently on replay for no visible reason | `workflow.Now(ctx)` |
| `time.Sleep` in a workflow | The workflow task deadline expires; `DeadlockError` naming the coroutine, then a task timeout | `workflow.Sleep(ctx, d)` |
| `go func()` in a workflow | The dispatcher never sees it; commands appear in a different order between replays, or the goroutine leaks after the worker evicts the instance | `workflow.Go(ctx, fn)` |
| A raw `chan` in a workflow | The coroutine blocks invisibly; `DeadlockError` after 5s | `workflow.NewChannel[T](ctx)` |
| `sync.WaitGroup` in a workflow | Same — a runtime block the dispatcher cannot see | `workflow.Await(ctx, cond)` or a workflow channel |
| Ranging a map to schedule activities | Intermittent non-determinism: it works until the map order happens to differ | Sort the keys first |
| Adding an activity without a version gate | Non-determinism at the exact event where the sequence diverges; every in-flight execution wedges | `workflow.GetVersion` |
| Removing an activity without a gate | "the recorded history contains an effect that the workflow code running now never asked for" | `workflow.GetVersion` |
| Branching on `IsReplaying` | Behaviour depends on how many times the workflow crashed; very hard to reproduce | Do not |
| An activity with no timeouts | Rejected at the command boundary: "an activity with no upper bound can wedge an execution forever" | Set `StartToCloseTimeout` or `ScheduleToCloseTimeout` |
| `HeartbeatTimeout` set, no heartbeats | The activity is killed and retried every `HeartbeatTimeout`, forever | Call `worker.RecordHeartbeat` |
| No heartbeat on a long activity | Cancellation never reaches it; it runs to completion after the workflow gave up | Add a heartbeat |
| A non-idempotent activity | Duplicate side effects after a worker crash or timeout | Make it idempotent on a stable key |
| Compensating on the cancelled context | The compensation activity resolves instantly with `CanceledError` and never runs | `workflow.NewDisconnectedContext(ctx)` |
| A workflow loop with no `ContinueAsNew` | Replay gets slower, then the run passes 50,000 events and becomes unloadable with an internal error | `workflow.ContinueAsNew(ctx, state)` |
| A 10 MiB activity input | `ErrPayloadTooLarge` at the call site | Pass an object storage key |
| An anonymous function registered as a workflow | Registration panics: a closure's runtime name is neither stable nor meaningful | `RegisterWorkflowWithOptions(fn, RegisterOptions{Name: "..."})` |
| `context.Context` as a workflow's first parameter | Registration panics at worker startup with an explanation | Use `workflow.Context` |
| Starting the client before the worker is deployed | `UnregisteredType` workflow task failures | Deploy workers first |
| Two workers sharing an `Identity` | The engine's task-ownership check cannot tell them apart; a late response from a timed-out worker may be accepted | Leave `Identity` at its default |

## Further reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — what replay actually does, and why
- [ADR 0003](adr/0003-cooperative-coroutine-dispatcher.md) — why the SDK
  replaces Go's concurrency primitives
- [ADR 0008](adr/0008-refuse-to-advance-on-non-determinism.md) — what happens
  when you get this wrong
- [operations.md](operations.md#deploying-workflow-code-safely) — the deploy
  procedure
- [`examples/`](../examples/) — runnable programs
