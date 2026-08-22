package simulation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/history"
)

// ErrInjected marks a failure the simulator produced on purpose.
//
// Every assertion in this package can therefore tell "the store misbehaved
// because we told it to" from "the store has a bug", which is the difference
// between a fault injector and a source of confusing test failures.
var ErrInjected = errors.New("simulation: injected fault")

// transientFaults are the failure modes a networked database exhibits under
// load. The messages matter: a reader of a simulation trace should recognise
// them, and a caller inspecting the error should conclude "retry".
var transientFaults = []string{
	"write i/o timeout",
	"connection reset by peer",
	"deadlock detected, transaction rolled back",
	"too many connections",
}

// chaosStore wraps a persistence.Store and fails on purpose.
//
// The memory driver has its own FaultConfig, and for a driver-level test it is
// the right tool. The simulator needs two things it does not offer. First, every
// random decision in the run must come from one seeded stream, so that a seed
// reproduces not just which calls failed but the interleaving that led to them.
// Second, faults must be switchable: liveness is the statement "everything
// finishes once the faults stop", and there is no way to state it against an
// injector that cannot be turned off.
//
// The wrapper is also where the simulator observes store traffic, which is what
// makes "how many operations did this run actually make" a number in the report
// rather than a guess.
type chaosStore struct {
	inner persistence.Store

	// mu guards everything below. In practice every call arrives on the
	// simulator's single goroutine, but the engine is free to fan out internally
	// and a data race in the harness would be indistinguishable from one in the
	// system under test.
	mu      sync.Mutex
	enabled bool
	rates   FaultRates
	draw    func() float64
	// latency is the wall-clock delay a "slow store" adds. It is deliberately
	// tiny: the point is to exercise the code path that has to survive a store
	// call taking longer than the caller expected, not to make the suite slow.
	latency time.Duration

	ops    int
	faults int
}

var _ persistence.Store = (*chaosStore)(nil)

func newChaosStore(inner persistence.Store, rates FaultRates, draw func() float64) *chaosStore {
	return &chaosStore{
		inner:   inner,
		enabled: true,
		rates:   rates,
		draw:    draw,
		latency: 50 * time.Microsecond,
	}
}

// setEnabled turns fault injection on or off. The drain phase turns it off; see
// the type documentation for why that has to be possible.
func (c *chaosStore) setEnabled(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = on
}

func (c *chaosStore) counts() (ops, faults int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ops, c.faults
}

// writeOp reports whether an operation can plausibly lose a version race, which
// is the only kind that may be failed with ErrVersionConflict.
type writeOp bool

const (
	read  writeOp = false
	write writeOp = true
)

// begin draws the faults for one call and reports whether the call, if it
// succeeds, must then be reported as having failed.
//
// Ordering matters and mirrors a real driver: latency is paid first (a slow
// store is slow whether or not it eventually fails), then the conflict draw,
// then the transient-failure draw. A conflict must leave state untouched, which
// is why it is decided here rather than inside the wrapped call.
func (c *chaosStore) begin(ctx context.Context, op string, kind writeOp) (ambiguous bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("simulation: %s: %w", op, err)
	}
	c.mu.Lock()
	c.ops++
	if !c.enabled {
		c.mu.Unlock()
		return false, nil
	}
	rates := c.rates
	draw := c.draw
	latency := c.latency
	c.mu.Unlock()

	// Every draw happens unconditionally, in a fixed order, whether or not the
	// rate it belongs to is zero. A run that skipped a draw would move the whole
	// stream, so a seed would stop being a reproduction of anything the moment a
	// rate changed by a hair.
	slowRoll := draw()
	conflictRoll := draw()
	failRoll := draw()
	ambiguousRoll := draw()
	whichRoll := draw()

	slow := slowRoll < rates.StoreLatency
	conflict := kind == write && conflictRoll < rates.VersionConflict
	fail := failRoll < rates.StoreError
	ambiguous = kind == write && !conflict && !fail && ambiguousRoll < rates.AmbiguousWrite

	if slow {
		timer := time.NewTimer(latency)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("simulation: %s: %w", op, ctx.Err())
		case <-timer.C:
		}
	}
	switch {
	case conflict:
		c.recordFault()
		return false, fmt.Errorf("simulation: %s: %w: %w", op, ErrInjected, persistence.ErrVersionConflict)
	case fail:
		c.recordFault()
		which := int(whichRoll * float64(len(transientFaults)))
		if which >= len(transientFaults) {
			which = len(transientFaults) - 1
		}
		return false, fmt.Errorf("simulation: %s: %w: %s", op, ErrInjected, transientFaults[which])
	}
	return ambiguous, nil
}

// ambiguousErr is what a caller sees when the write committed and the answer did
// not come back.
//
// This is the fault a durable engine has to be correct under and the one no unit
// test writes by hand: the caller has no way to distinguish it from a write that
// never happened, so its only safe response is to re-read and reconcile.
func (c *chaosStore) ambiguousErr(op string) error {
	c.recordFault()
	return fmt.Errorf("simulation: %s: %w: connection lost after commit", op, ErrInjected)
}

func (c *chaosStore) recordFault() {
	c.mu.Lock()
	c.faults++
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// persistence.Store
// ---------------------------------------------------------------------------

func (c *chaosStore) CreateExecution(ctx context.Context, req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	const op = "CreateExecution"
	ambiguous, err := c.begin(ctx, op, write)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	rec, err := c.inner.CreateExecution(ctx, req)
	if err == nil && ambiguous {
		return persistence.ExecutionRecord{}, c.ambiguousErr(op)
	}
	return rec, err
}

func (c *chaosStore) GetExecution(ctx context.Context, namespace, workflowID, runID string) (persistence.ExecutionRecord, error) {
	if _, err := c.begin(ctx, "GetExecution", read); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	return c.inner.GetExecution(ctx, namespace, workflowID, runID)
}

func (c *chaosStore) ReadHistory(ctx context.Context, namespace, workflowID, runID string, from, to int64) (history.History, error) {
	if _, err := c.begin(ctx, "ReadHistory", read); err != nil {
		return nil, err
	}
	return c.inner.ReadHistory(ctx, namespace, workflowID, runID, from, to)
}

func (c *chaosStore) AppendHistory(ctx context.Context, req persistence.AppendHistoryRequest) (persistence.ExecutionRecord, error) {
	const op = "AppendHistory"
	ambiguous, err := c.begin(ctx, op, write)
	if err != nil {
		return persistence.ExecutionRecord{}, err
	}
	rec, err := c.inner.AppendHistory(ctx, req)
	if err == nil && ambiguous {
		return persistence.ExecutionRecord{}, c.ambiguousErr(op)
	}
	return rec, err
}

func (c *chaosStore) ListExecutions(ctx context.Context, filter persistence.ListFilter) (persistence.ListResult, error) {
	if _, err := c.begin(ctx, "ListExecutions", read); err != nil {
		return persistence.ListResult{}, err
	}
	return c.inner.ListExecutions(ctx, filter)
}

func (c *chaosStore) DueTimers(ctx context.Context, now time.Time, limit int) ([]persistence.TimerRecord, error) {
	if _, err := c.begin(ctx, "DueTimers", read); err != nil {
		return nil, err
	}
	return c.inner.DueTimers(ctx, now, limit)
}

func (c *chaosStore) DeleteTimers(ctx context.Context, keys []persistence.TimerKey) error {
	const op = "DeleteTimers"
	ambiguous, err := c.begin(ctx, op, write)
	if err != nil {
		return err
	}
	if err := c.inner.DeleteTimers(ctx, keys); err != nil {
		return err
	}
	if ambiguous {
		return c.ambiguousErr(op)
	}
	return nil
}

func (c *chaosStore) OpenExecutions(ctx context.Context, namespace string, fn func(persistence.ExecutionRecord) error) error {
	if _, err := c.begin(ctx, "OpenExecutions", read); err != nil {
		return err
	}
	return c.inner.OpenExecutions(ctx, namespace, fn)
}

// Close is not forwarded. The simulator owns the wrapped store's lifetime and
// closes it directly, so that an engine restart -- which closes what it was
// given -- cannot take the durable state with it.
func (c *chaosStore) Close() error { return nil }
