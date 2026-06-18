// Package workflow implements the deterministic replay machinery that lets
// ordinary-looking Go code survive process death.
//
// # The problem
//
// A durable workflow is replayed from event 1 on every workflow task. For that
// to produce the same decisions it produced the first time, two properties must
// hold:
//
//   - The code must be deterministic. Given the same history it must reach the
//     same instructions in the same order.
//   - The code must not re-issue effects that already happened. Replaying
//     "charge the customer" must not charge the customer again.
//
// Ordinary Go concurrency defeats both. The runtime schedules goroutines
// non-deterministically, `select` picks a random ready case, `time.Now` and
// `math/rand` return different values on every run, and a goroutine blocked on
// a channel cannot be told to stop.
//
// # The solution: a cooperative coroutine dispatcher
//
// Workflow code runs on coroutines: real goroutines that are scheduled by this
// package rather than by the Go runtime. Exactly one of {dispatcher, coroutine}
// runs at any instant, enforced by a pair of unbuffered channels used as a
// hand-off. A coroutine "blocks" by yielding control back to the dispatcher
// instead of parking on a runtime primitive, so the dispatcher always knows the
// complete set of runnable work.
//
// Dispatcher.ExecuteUntilAllBlocked runs every coroutine, in a fixed order, until
// the whole program reaches a fixpoint: no coroutine can advance without new
// information from the outside world. At that instant the batch of commands the
// workflow produced is complete, and it is complete deterministically -- there is
// no "and then a goroutine woke up a microsecond later and added one more".
//
// # The scheduling invariant
//
// The dispatcher repeats passes over its coroutines until a pass makes no
// progress. "Progress" is marked explicitly by exactly four kinds of event:
//
//  1. a coroutine completes,
//  2. a coroutine is created,
//  3. a future settles or a channel operation moves a value,
//  4. a coroutine *enters a new blocking call*.
//
// Case 4 is the subtle one and it is what makes the fixpoint correct. Consider
// coroutine B awaiting a condition that coroutine A makes true, where B happens
// to be scheduled first. Pass 1: B checks, false, yields. A runs, sets the flag,
// then blocks on an activity -- entering that blocking call marks progress, so a
// second pass happens and B observes the flag. Without case 4 the dispatcher
// would stop one pass too early and the workflow would stall.
//
// Termination still holds: entering a *new* blocking call requires executing
// user code, and a coroutine that merely re-checks the wait it is already parked
// on marks nothing. A workflow that genuinely spins forever is caught by the
// workflow task deadline, which is reported as a DeadlockError naming every
// blocked coroutine and what it is waiting for.
//
// # Layering
//
// This package is deliberately free of reflection and of any notion of a
// registry: it takes a WorkflowFunc that already speaks payloads. The
// user-facing API lives in pkg/workflow, which is a thin, typed skin over the
// types declared here; the polling, registration and activity machinery lives
// in pkg/worker.
package workflow
