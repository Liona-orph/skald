// Package timers turns the store's due-time index into callbacks.
//
// # Why a scanner and not runtime timers
//
// A durable timer must survive the death of every process in the system, so it
// cannot live in a runtime timer wheel. It lives in the store, and something has
// to notice that it came due. That something is this package: a loop that asks
// the store "what is due now?", hands each answer to the engine, and deletes
// the entry once the engine has durably acted on it.
//
// The cost of the design is a scan interval's worth of latency on a timer fire,
// which is the right trade for a system whose timers are measured in seconds to
// months. The benefit is that a timer is exactly as durable as the history, with
// no second consistency protocol.
//
// # At-least-once, and why duplicates are harmless
//
// The service deletes a timer only after the callback returns nil, so a crash
// between the callback and the delete redelivers the timer. That is deliberate:
// the alternative -- delete first, then act -- turns a crash into a *lost*
// timer, and a workflow that never wakes up is unrecoverable, whereas a workflow
// that wakes twice is merely inefficient.
//
// Duplicates are harmless because every transition the callback can trigger is
// idempotent against the state machine, not against the message:
//
//   - Firing a user timer that already fired finds no pending timer and returns
//     a no-op (execution.MutableState.FireTimer documents exactly this race).
//   - Re-dispatching an activity retry hands matching a second reference to work
//     that is already queued; the loser of the two polls finds the attempt
//     already started and is discarded by the engine.
//   - Re-applying a timeout finds the activity or workflow task gone and does
//     nothing.
//
// In other words the timer is a *hint that state may have advanced*, never an
// instruction, and the state machine is the arbiter. That is the same principle
// that makes the whole engine safe to run in more than one replica.
package timers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Liona-orph/skald/internal/clock"
	"github.com/Liona-orph/skald/internal/persistence"
)

// Dispatch is the callback invoked for one due timer.
//
// Returning nil means "the engine has durably absorbed this timer" and licenses
// the service to delete it. Returning an error leaves the entry in the index for
// the next scan, which is the correct response to a transient store failure and
// an acceptable one to a permanent bug: the timer is retried, loudly, instead of
// vanishing.
type Dispatch func(ctx context.Context, rec persistence.TimerRecord) error

// Defaults for Config.
const (
	// DefaultInterval is how often the index is scanned. One second bounds timer
	// latency at roughly one second, which is far below the granularity anyone
	// schedules durable work at, while keeping the query rate at one per second
	// per replica.
	DefaultInterval = time.Second
	// DefaultJitterFraction spreads the scans of independent replicas.
	DefaultJitterFraction = 0.2
	// DefaultBatchSize bounds one scan. It is large enough to drain a burst in a
	// few cycles and small enough that a single scan cannot monopolise the
	// store or build a multi-second unit of work that shutdown must wait for.
	DefaultBatchSize = 100
	// DefaultMaxBackoff caps the retry delay after store errors.
	DefaultMaxBackoff = 30 * time.Second
)

// Config parameterises a Service.
type Config struct {
	// Store is the durable timer index. Required.
	Store persistence.Store
	// Dispatch receives every due timer. Required.
	Dispatch Dispatch
	// Clock drives the scan interval. Defaults to clock.System.
	Clock clock.Clock
	// Interval is the nominal time between scans.
	Interval time.Duration
	// JitterFraction spreads scans over [interval, interval*(1+fraction)].
	//
	// Without jitter, N replicas started by the same deployment tick converge on
	// the same scan instant and stay there: every scan becomes a synchronised
	// burst against the store, and every burst has N-1 replicas losing a race
	// they paid a query for. A little noise decorrelates them permanently.
	JitterFraction float64
	// BatchSize bounds how many timers one scan may claim.
	BatchSize int
	// MaxBackoff caps the delay applied after consecutive store failures.
	MaxBackoff time.Duration
	// Rand returns values in [0,1). Injected so tests can pin the jitter.
	// Defaults to a source seeded from the clock, never the global RNG: a
	// package-level RNG would make two services in one process share state and
	// make a test's jitter depend on what ran before it.
	Rand func() float64
	// Logger receives operational events. Defaults to a discarding logger.
	Logger *slog.Logger
}

// Stats reports what the service has done, for metrics and for tests that need
// to observe progress without sleeping.
type Stats struct {
	// Scans is the number of completed index scans.
	Scans uint64
	// Dispatched is the number of timers handed to the callback.
	Dispatched uint64
	// Deleted is the number of timers removed after a successful callback.
	Deleted uint64
	// DispatchErrors counts callbacks that returned an error.
	DispatchErrors uint64
	// StoreErrors counts failed reads and deletes.
	StoreErrors uint64
}

// Service scans the due-time index on an interval.
type Service struct {
	store    persistence.Store
	dispatch Dispatch
	clk      clock.Clock
	interval time.Duration
	jitter   float64
	batch    int
	maxWait  time.Duration
	rnd      func() float64
	log      *slog.Logger

	startOnce sync.Once
	stopOnce  sync.Once
	// stop asks the loop to finish the batch it is on and return.
	stop chan struct{}
	// done closes when the loop has fully drained.
	done chan struct{}
	// hardStop cancels the context handed to in-flight dispatches, used only
	// when a caller's shutdown deadline expires.
	hardStop context.CancelFunc

	scans          atomic.Uint64
	dispatched     atomic.Uint64
	deleted        atomic.Uint64
	dispatchErrors atomic.Uint64
	storeErrors    atomic.Uint64
}

// ErrNotConfigured reports a missing required dependency.
var ErrNotConfigured = errors.New("timers: service is not configured")

// New validates cfg and returns a stopped Service.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrNotConfigured)
	}
	if cfg.Dispatch == nil {
		return nil, fmt.Errorf("%w: dispatch callback is required", ErrNotConfigured)
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.JitterFraction < 0 {
		cfg.JitterFraction = 0
	} else if cfg.JitterFraction == 0 {
		cfg.JitterFraction = DefaultJitterFraction
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.Rand == nil {
		src := rand.New(rand.NewSource(cfg.Clock.Now().UnixNano()))
		cfg.Rand = src.Float64
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{
		store:    cfg.Store,
		dispatch: cfg.Dispatch,
		clk:      cfg.Clock,
		interval: cfg.Interval,
		jitter:   cfg.JitterFraction,
		batch:    cfg.BatchSize,
		maxWait:  cfg.MaxBackoff,
		rnd:      cfg.Rand,
		log:      cfg.Logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}, nil
}

// Start launches the scan loop. It is safe to call more than once; only the
// first call has an effect.
func (s *Service) Start() {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.hardStop = cancel
		go s.run(ctx)
	})
}

// Stop asks the loop to finish the scan it is on and waits for it to drain.
//
// Draining rather than cancelling matters: a timer whose callback is halfway
// through an engine transaction should be allowed to commit, because the
// alternative is a redelivery that does the same work again after restart. If
// ctx expires first the in-flight dispatch is cancelled and Stop returns
// ctx.Err(), so a shutdown deadline is still honoured.
func (s *Service) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	// A service that was never started has no loop to drain, and claiming the
	// start latch here also makes a later Start a no-op: once stopped, stopped.
	s.startOnce.Do(func() { close(s.done) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		if s.hardStop != nil {
			s.hardStop()
		}
		<-s.done
		return ctx.Err()
	}
}

// Stats returns a snapshot of the service counters.
func (s *Service) Stats() Stats {
	return Stats{
		Scans:          s.scans.Load(),
		Dispatched:     s.dispatched.Load(),
		Deleted:        s.deleted.Load(),
		DispatchErrors: s.dispatchErrors.Load(),
		StoreErrors:    s.storeErrors.Load(),
	}
}

func (s *Service) run(ctx context.Context) {
	defer close(s.done)
	// failures counts consecutive store failures and drives the backoff. It is
	// reset by any successful scan, so a single blip does not slow the service
	// down for the rest of its life.
	failures := 0
	timer := s.clk.NewTimer(s.wait(failures))
	defer timer.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ctx.Done():
			return
		case <-timer.C():
		}

		if err := s.scan(ctx); err != nil {
			failures++
			s.log.Warn("timer scan failed", "error", err, "consecutive_failures", failures)
		} else {
			failures = 0
		}
		timer.Reset(s.wait(failures))
	}
}

// wait returns the delay before the next scan: the jittered interval normally,
// and an exponentially growing delay while the store is failing so that a
// struggling database is not scanned harder for being slow.
func (s *Service) wait(failures int) time.Duration {
	if failures > 0 {
		// 2^failures capped, then jittered so recovering replicas do not all
		// return at the same instant and knock the store over again.
		backoff := float64(s.interval) * math.Pow(2, float64(failures))
		if backoff > float64(s.maxWait) || math.IsInf(backoff, 0) {
			backoff = float64(s.maxWait)
		}
		return time.Duration(backoff * (1 + s.jitter*s.rnd()))
	}
	return time.Duration(float64(s.interval) * (1 + s.jitter*s.rnd()))
}

// scan processes one page of due timers.
func (s *Service) scan(ctx context.Context) error {
	due, err := s.store.DueTimers(ctx, s.clk.Now(), s.batch)
	if err != nil {
		s.storeErrors.Add(1)
		return fmt.Errorf("timers: read due timers: %w", err)
	}
	s.scans.Add(1)

	for _, rec := range due {
		// Shutdown is checked between timers rather than mid-timer: the unit of
		// work that must not be torn is a single dispatch plus its delete.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s.dispatched.Add(1)
		if err := s.dispatch(ctx, rec); err != nil {
			s.dispatchErrors.Add(1)
			s.log.Warn("timer dispatch failed; the entry stays in the index for redelivery",
				"workflow_id", rec.WorkflowID, "run_id", rec.RunID,
				"event_id", rec.EventID, "kind", rec.Kind, "error", err)
			continue
		}
		// Deleted one at a time rather than as a batch at the end of the page.
		// Batching would save store round trips at the cost of redelivering
		// every already-processed timer in the page after a crash, and a
		// redelivery is only cheap because it is rare.
		if err := s.store.DeleteTimers(ctx, []persistence.TimerKey{rec.TimerKey}); err != nil {
			s.storeErrors.Add(1)
			s.log.Warn("timer delete failed; the timer will be redelivered",
				"workflow_id", rec.WorkflowID, "run_id", rec.RunID,
				"event_id", rec.EventID, "kind", rec.Kind, "error", err)
			continue
		}
		s.deleted.Add(1)
	}
	return nil
}
