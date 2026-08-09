# Contributing to Skald

Thanks for taking the time. This document is what you need to get a change
merged without a round trip about process.

## Before you start

**Open an issue first** for anything that changes behaviour, adds a dependency,
or touches the persisted format. A bug fix or a documentation change needs no
preamble.

**Changes that need an ADR** before code review: anything that alters the
durability model, the wire protocol, the storage contract, or a determinism
guarantee. See [docs/adr/README.md](docs/adr/README.md) for the format. The ADR
and the code can be in the same pull request; the ADR is what gets reviewed
first.

**Changes to the persisted format are close to irreversible.** Event type codes
are permanent, attribute fields are permanent once they ship, and a history
written by an older version must still decode. Adding a new event type or a new
optional field is routine; changing or removing one is not.

## Development setup

You need **Go 1.25.12 or later** and nothing else. No C toolchain (the SQLite
driver is pure Go), no code generation, no protobuf.

```bash
git clone https://github.com/skald-io/skald.git
cd skald
make ci        # the whole gate: fmt, vet, lint, race tests, build
```

`make help` lists every target with a one-line description.

Optional tools, installed on demand by the Makefile targets that need them:

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.62.2
```

### Running things locally

```bash
# A durable server on the loopback interface.
go run ./cmd/skaldd --store sqlite --sqlite-path /tmp/skald.db --log-format text

# The CLI against it.
go run ./cmd/skaldctl workflow list

# The whole stack with metrics and a dashboard.
docker compose up -d
```

## Tests

```bash
make test        # unit tests
make test-race   # the same under -race; this is what CI gates on
make cover       # coverage profile plus a per-function summary
make bench       # benchmarks
make fuzz        # fuzz targets, 30s each by default: make fuzz FUZZTIME=5m
```

The whole suite runs in seconds. That is deliberate, and it is the property
worth protecting most: a suite nobody runs before pushing is not a suite.

### How to write tests here

**Never use `time.Sleep`, and never poll until a condition holds.** Every
component that waits takes a `clock.Clock`; tests pass a `clock.Virtual` and
advance it explicitly. See the package comment in `internal/clock` for the
reasoning and `internal/clock/clock_test.go` for the pattern.

The synchronisation primitive is `Virtual.BlockUntil(n)`, which waits until `n`
timers are armed. Arm-then-advance without it is a race. `BlockUntil` blocks
forever if the count is never reached, which surfaces as a test timeout with a
goroutine dump naming the function — a much better failure than a flaky
assertion.

**Assert on schedules, not on elapsed time.** "Nothing fires at 4.999s and
exactly one thing fires at 5.000s" is a statement about what the code promises.
"It finished within 6 seconds" is a statement about the machine.

**Test the contract, not the implementation.** A new `persistence.Store` driver
is finished when it passes `internal/persistence/persistencetest`. If you find
yourself asserting on a driver's internals, the assertion probably belongs in
the shared suite instead.

**Use the fault injector for error paths.** `memory.WithFaults(memory.FaultConfig{
Seed: 1, ConflictRate: 0.1, ErrorRate: 0.05})` exercises the optimistic-concurrency
retry loop, stale task references and timer redelivery — all rare in production
and routine under injection.

**Exercise the cold path.** Correctness must never depend on the sticky cache.
Tests that evict between every workflow task and assert identical results are
what keep that true; do not delete them to make a test faster.

**A test that is flaky under this regime is reporting a real race.** Do not add
a retry; find it.

### What a good test looks like

- One behaviour per test, named for the behaviour rather than the function.
- No mocks of things we own. The engine, the store and the worker all run for
  real in a single process; that is what `pkg/worker`'s tests do.
- Table-driven where the cases are genuinely parallel, and separate functions
  where they are not. A table with an `if tc.special` branch is two tests.
- Failure messages that say what was expected and what happened, in that order.

## Code style

`gofmt` is the formatter and `golangci-lint` is the arbiter; `make fmt` and
`make lint` are the whole style guide. Beyond that, the conventions this
codebase actually follows:

**Comments explain why, not what.** A comment that restates the code is noise. A
comment that says why a `for` loop is index-based rather than a `range` — because
a coroutine spawned during the pass must run in the same pass — is the reason
anyone can change the code safely later. The existing package comments are the
standard to match.

**Document the trade-off where it is made.** If you choose an approach with a
known cost, say so in the code, next to the choice. Skald's package comments do
this consistently and it is the single most valuable convention here.

**Errors carry the sentinel and the context.** `fmt.Errorf("%w: ...",
ErrVersionConflict, ...)` so callers can use `errors.Is`, with enough detail in
the message that a human does not need a debugger.

**Exported identifiers are documented.** Unexported ones are documented when the
reason for their existence is not obvious from the name.

**No new dependencies without discussion.** The current set is deliberately
small. A dependency is a permanent obligation: a supply-chain surface, a
compatibility constraint, and something someone has to update at an inconvenient
time.

**No `panic` in library code**, except where a caller has made an unrecoverable
programming error (`RegisterWorkflow` with a bad signature) and the panic is
documented as the contract.

## Commit conventions

Conventional Commits. The type prefix drives the changelog and the release
notes.

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`,
`chore`, `revert`.

**Scopes** are package names, without the `internal/` or `pkg/` prefix:
`engine`, `execution`, `workflow`, `worker`, `client`, `history`, `api`,
`persistence`, `sqlite`, `matching`, `timers`, `frontend`, `telemetry`,
`skaldd`, `skaldctl`, `docs`.

**Subject** is imperative, lower case, no trailing period, under 72 characters.

**Breaking changes** get a `!` after the scope and a `BREAKING CHANGE:` footer
explaining what breaks and what to do about it.

```
fix(engine): reschedule the workflow task after a mid-task signal

A signal that arrived while a worker held the task appended its event and
returned no effect, because ScheduleWorkflowTask found a task already in
flight. Nothing then woke the workflow, so the signal sat unread until some
other transition happened to schedule a task.

RespondWorkflowTaskCompleted now notices the gap between the started event and
the completion and schedules a fresh task.

Fixes #142
```

**Write the body for the person doing `git blame` in two years.** What was
wrong, why it was wrong, and what changed. A commit message that says "fix bug"
costs someone an hour later.

Keep the history linear: rebase rather than merge, and squash fixup commits
before requesting review.

## Pull requests

**Before you open one:**

```bash
make ci
```

**In the description:**

- What changes and why. Link the issue.
- What you considered and rejected, if the choice was not obvious.
- How you tested it, if it is not evident from the diff.
- Anything you are unsure about. Flagging it gets you a better review.

**Size.** A reviewable pull request is one idea. If the diff has a refactor and
a behaviour change in it, split it — the refactor first, mechanically, then the
change on top. Reviewers cannot see a subtle behaviour change inside a thousand
lines of moved code, and neither can you.

**Keep the branch rebased** on `main` while it is in review.

### What reviewers look for

In roughly this order:

1. **Is it correct under concurrency and after a crash?** For anything touching
   the engine: what happens if the process dies between these two lines? Is the
   effect applied before or after the commit, and is that the right order?
2. **Is it deterministic?** For anything touching `internal/workflow` or the
   SDK: could two replays of the same history diverge here?
3. **Does it preserve the persisted format?** New event type, new field, changed
   meaning — which is it?
4. **Is the failure mode stated?** What happens when the store is down, the
   worker is gone, the poll times out?
5. **Does it read?** Would someone unfamiliar with this package understand why
   the code is shaped this way, from the code and its comments alone?
6. **Are the tests real?** Do they fail if you break the thing they claim to
   cover? Try it.

### What to expect

One maintainer review is required. Expect a first response within a few working
days. Expect questions rather than instructions: if a reviewer asks "what happens
if the worker dies here", the useful answer is either an explanation or a test,
not a change.

Reviews are about the code. A comment on your change is not a comment on you,
and a reviewer who is wrong would like to be told so with the reasoning.

## Adding a storage driver

1. Implement `persistence.Store` in `internal/persistence/<name>/`.
2. Wire it into `internal/persistence/persistencetest` and make the suite pass.
   It covers the awkward parts: ID reuse policies, request-ID deduplication,
   version conflicts, timer index diffing, keyset pagination.
3. Add it to `openStore` in `cmd/skaldd/main.go` and to the configuration
   reference in [docs/operations.md](docs/operations.md).
4. Read `internal/persistence/sqlite/schema.go` first. The comments explain what
   each index is for and why, and a driver that ignores them will be correct and
   slow.

## Releasing

Maintainers only.

1. Update `CHANGELOG.md` — move `Unreleased` to the new version with a date.
2. Tag: `git tag -s v0.2.0 -m "v0.2.0" && git push origin v0.2.0`.
3. `.github/workflows/release.yml` runs GoReleaser, which builds multi-platform
   binaries and the container image and drafts the release notes.
4. Check the draft, then publish.

## Licence

By contributing you agree that your contributions are licensed under the
Apache 2.0 licence in [LICENSE](LICENSE).
