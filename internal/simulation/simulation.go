// Package simulation is a deterministic simulation test for the whole of Skald.
//
// It stands up a complete system -- store, engine, matching, durable timers and
// several workers running real registered workflows -- inside one process, one
// goroutine and one virtual clock, and then drives it with a seeded pseudorandom
// number generator while injecting faults. Every invariant the system claims is
// checked after every step. A failure prints the seed, and the seed alone
// reproduces the run.
//
// See README.md in this directory for what deterministic simulation testing is,
// why it finds bugs ordinary tests cannot, and how to reproduce a failure.
//
// # The shape of the harness
//
// A single goroutine owns the world. It picks one action per step -- start a
// workflow, hand a task to a worker, fire a due timer, advance the clock, crash
// a worker, restart the engine -- from a weighted set of the actions that are
// currently possible, using the run's PRNG. Nothing else runs concurrently, so
// the sequence of operations the engine sees is a pure function of the seed.
//
// Three things had to be arranged for that to be true, and each is worth
// knowing about because each is a deliberate departure from production wiring:
//
//   - The durable timer service is not started. Its scan loop is a background
//     goroutine issuing store calls, and those calls interleaved with the
//     simulator's own would make the operation order depend on the Go
//     scheduler. The simulator performs the same scan itself and dispatches
//     through engine.FireTimer, which is the identical code path.
//
//   - The workers are not pkg/worker.Worker. Everything a worker does with a
//     task is real -- the registry, the sticky cache, the replay executor, the
//     converter, the command protocol -- but the poll loop and the concurrency
//     semaphores are replaced by the simulator's scheduler. See simWorker.
//
//   - Matching runs on the system clock with a microsecond poll timeout, so an
//     empty poll returns immediately instead of parking until virtual time
//     moves. Everything else -- the engine, every deadline in every history --
//     runs on the virtual clock, where advancing an hour costs nanoseconds.
//
// What is left is the real state machine, the real history, the real optimistic
// concurrency control, the real retry and timeout logic, and the real replayer.
package simulation

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/skald-io/skald/internal/clock"
	"github.com/skald-io/skald/internal/engine"
	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/internal/persistence/memory"
	internalwf "github.com/skald-io/skald/internal/workflow"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// FaultRates are the probabilities of each injected failure, in [0,1].
//
// They are probabilities per opportunity rather than per step, so raising one
// affects only the operations it applies to. The defaults are tuned to produce a
// run in which roughly a third of everything goes wrong -- high enough that the
// recovery paths are the common case, low enough that a run still makes
// progress.
type FaultRates struct {
	// StoreError fails a store call with a transient, retryable error before it
	// touches state.
	StoreError float64
	// VersionConflict rejects a write with ErrVersionConflict without touching
	// state, modelling a competing engine replica.
	VersionConflict float64
	// AmbiguousWrite commits a write and *then* reports failure, which is what a
	// database connection dropped after the commit looks like. It is the nastiest
	// store fault there is: the caller cannot tell whether its work happened.
	AmbiguousWrite float64
	// StoreLatency adds a small wall-clock delay to a call.
	StoreLatency float64
	// WorkerCrash kills the worker holding a task before it responds, dropping
	// the sticky cache and every in-flight instance with it.
	WorkerCrash float64
	// LostTask drops a task the moment it leaves matching, so the work is
	// referenced by nothing and only a timeout can recover it.
	LostTask float64
	// DuplicateDelivery sends a response a second time.
	DuplicateDelivery float64
	// ActivityFailure fails an activity attempt with a retryable error.
	ActivityFailure float64
	// ClockSkew jumps the clock forward by an arbitrary amount instead of
	// advancing to the next deadline.
	ClockSkew float64
	// EngineRestart replaces the engine with a cold one over the same store,
	// which drops the rebuilt-state cache and every in-memory task reference.
	EngineRestart float64
	// Cancel cancels a running execution.
	Cancel float64
}

// NoFaults is the fault-free configuration used by the drain phase.
func NoFaults() FaultRates { return FaultRates{} }

// DefaultFaults is the profile TestSimulation runs across many seeds.
func DefaultFaults() FaultRates {
	return FaultRates{
		StoreError:        0.06,
		VersionConflict:   0.05,
		AmbiguousWrite:    0.02,
		StoreLatency:      0.01,
		WorkerCrash:       0.08,
		LostTask:          0.06,
		DuplicateDelivery: 0.10,
		ActivityFailure:   0.15,
		ClockSkew:         0.15,
		EngineRestart:     0.02,
		Cancel:            0.02,
	}
}

// AggressiveFaults is the profile the soak run uses. Roughly one operation in
// four fails; it exists because the interesting bugs live where two independent
// failures overlap, and overlaps are quadratically rarer than single faults.
func AggressiveFaults() FaultRates {
	return FaultRates{
		StoreError:        0.15,
		VersionConflict:   0.15,
		AmbiguousWrite:    0.05,
		StoreLatency:      0.02,
		WorkerCrash:       0.20,
		LostTask:          0.15,
		DuplicateDelivery: 0.20,
		ActivityFailure:   0.30,
		ClockSkew:         0.25,
		EngineRestart:     0.06,
		Cancel:            0.05,
	}
}

// Config parameterises one simulation run. The zero value is usable except for
// Seed, which has no sensible default.
type Config struct {
	// Seed determines everything: the action sequence, the faults, the workload.
	Seed int64
	// Workers is the number of independent worker processes.
	Workers int
	// Executions is the number of workflows the run starts.
	Executions int
	// MaxSteps bounds the chaos phase.
	MaxSteps int
	// MaxDrainSteps bounds the fault-free phase that follows it. A run that
	// exhausts it has an execution that never finished, which is a liveness bug.
	MaxDrainSteps int
	// Faults is the profile for the chaos phase.
	Faults FaultRates
	// MaxBacklog bounds a matching queue.
	//
	// It is deliberately small. Rejecting a dispatch because the backlog is full
	// is a real production path (see matching.Config.MaxBacklog) and it is the
	// only way to lose a task reference that was never handed to anyone, which
	// is the case the recovery scan exists for. A large backlog would make that
	// path unreachable and quietly delete a fault from the profile.
	MaxBacklog int
	// Logger receives worker and engine output. Nil discards it.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = 3
	}
	if c.Executions <= 0 {
		c.Executions = 6
	}
	if c.MaxSteps <= 0 {
		c.MaxSteps = 900
	}
	if c.MaxDrainSteps <= 0 {
		c.MaxDrainSteps = 4000
	}
	if c.MaxBacklog <= 0 {
		c.MaxBacklog = 8
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// ---------------------------------------------------------------------------
// The simulation
// ---------------------------------------------------------------------------

const simNamespace = skald.DefaultNamespace

// simStartTime anchors the virtual clock. A fixed, arbitrary instant rather than
// time.Now keeps failure output diffable between runs of the same seed.
var simStartTime = time.Date(2024, time.March, 1, 12, 0, 0, 0, time.UTC)

// execState is what the simulator remembers about one execution.
type execState struct {
	workload
	started bool
	// runID is the first run. A continue-as-new chain moves on from it, which is
	// why every lookup that must see the whole chain filters by workflow ID.
	runID string
	// canceled records that the simulator asked for cancellation, which relaxes
	// the result oracle: a cancelled execution owes no particular answer, only a
	// terminal state.
	canceled bool
	// verified is set once a terminal run has been checked against the oracle,
	// so the check is not repeated on every step for the rest of the run.
	verified bool
}

// Simulation is one seeded run of the whole system.
type Simulation struct {
	cfg Config
	rng *prng

	clk     *clock.Virtual
	raw     *memory.Store
	store   *chaosStore
	matcher *matching.Matcher
	engine  *engine.Engine
	workers []*simWorker
	conv    skald.DataConverter

	ctx context.Context

	pending []workload
	live    map[string]*execState
	// order preserves the workflow IDs in start order, so that every iteration
	// over executions is deterministic. Ranging over the map would not be.
	order []string

	// checked remembers the newest event each run was validated at, so that a
	// per-step invariant sweep costs one list query plus the histories that
	// actually changed.
	checked map[string]int64

	ids   int64
	seeds int64
	step  int
	phase string

	trace []string
	stats Report
}

// Report summarises what a run actually did. Numbers that stay at zero across
// many seeds mean a fault is not reachable, which is a bug in the simulator and
// exactly as serious as a bug in the system.
type Report struct {
	Seed               int64
	Steps              int
	Executions         int
	Completed          int
	Canceled           int
	Runs               int
	Events             int
	WorkflowTasks      int
	ActivityTasks      int
	TimersFired        int
	Signals            int
	WorkerCrashes      int
	EngineRestarts     int
	LostTasks          int
	DuplicateResponses int
	ActivityFailures   int
	StoreOps           int
	StoreFaults        int
	VirtualElapsed     time.Duration
}

// String renders the report as one line per counter, for a test's -v output.
func (r Report) String() string {
	return fmt.Sprintf(
		"seed=%d steps=%d execs=%d completed=%d canceled=%d runs=%d events=%d "+
			"wf_tasks=%d act_tasks=%d timers=%d signals=%d crashes=%d restarts=%d "+
			"lost=%d dups=%d act_failures=%d store_ops=%d store_faults=%d virtual=%s",
		r.Seed, r.Steps, r.Executions, r.Completed, r.Canceled, r.Runs, r.Events,
		r.WorkflowTasks, r.ActivityTasks, r.TimersFired, r.Signals, r.WorkerCrashes,
		r.EngineRestarts, r.LostTasks, r.DuplicateResponses, r.ActivityFailures,
		r.StoreOps, r.StoreFaults, r.VirtualElapsed)
}

// prng is the run's single source of randomness.
//
// One stream, not one per component. Every decision -- which action, which
// worker, which call fails -- draws from it in a fixed order, which is what
// makes the seed sufficient to reproduce a run. The mutex is defensive: calls
// should only ever arrive from the simulator's goroutine, and a race detector
// hit here would be indistinguishable from one in the system under test.
type prng struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (p *prng) float() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.r.Float64()
}

func (p *prng) intn(n int) int {
	if n <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.r.Intn(n)
}

func (p *prng) int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.r.Int63n(n)
}

// New builds a simulation. Nothing runs until Run is called.
func New(cfg Config) (*Simulation, error) {
	cfg.applyDefaults()

	s := &Simulation{
		cfg:     cfg,
		rng:     &prng{r: rand.New(rand.NewSource(cfg.Seed))},
		clk:     clock.NewVirtual(simStartTime),
		conv:    skald.JSONConverter{},
		ctx:     context.Background(),
		live:    map[string]*execState{},
		checked: map[string]int64{},
		phase:   "chaos",
	}
	s.stats.Seed = cfg.Seed

	s.raw = memory.New()
	s.store = newChaosStore(s.raw, cfg.Faults, s.rng.float)
	// Matching runs on the system clock so that an empty poll returns at once
	// rather than parking until the simulator advances virtual time -- which it
	// cannot do while it is blocked inside the poll. The timeout is a
	// microsecond because in this world a poll is never a wait: the simulator
	// only polls a queue it has already seen a backlog on.
	s.matcher = matching.New(matching.Config{
		Clock:       clock.System(),
		PollTimeout: time.Microsecond,
		MaxBacklog:  cfg.MaxBacklog,
	})

	if err := s.newEngine(); err != nil {
		return nil, err
	}

	for i := 0; i < cfg.Workers; i++ {
		s.workers = append(s.workers, newSimWorker(i, simNamespace, s.conv, cfg.Logger))
	}
	s.pending = s.buildWorkload()
	s.stats.Executions = len(s.pending)
	return s, nil
}

// newEngine replaces s.engine with a cold one over the same store and matcher.
//
// This is what a server process restart looks like from the durable state's
// point of view: the rebuilt-state cache is gone, the set of timers this process
// believes it armed is gone, and the derived task queues have to be
// re-materialised from history by the recovery scan.
func (s *Simulation) newEngine() error {
	if s.engine != nil {
		if err := s.engine.Close(s.ctx); err != nil {
			return fmt.Errorf("simulation: closing engine: %w", err)
		}
	}
	eng, err := engine.New(engine.Config{
		Store:            s.store,
		Matcher:          s.matcher,
		Clock:            s.clk,
		DefaultNamespace: simNamespace,
		NewID:            s.nextID,
		NewSeed:          s.nextSeed,
		// One minute of virtual time is the watchdog on a dispatched retry that
		// nobody picked up. It has to be crossable by the clock actions, which
		// jump by at most a few minutes.
		RedispatchInterval: time.Minute,
		Logger:             s.cfg.Logger,
	})
	if err != nil {
		return fmt.Errorf("simulation: building engine: %w", err)
	}
	s.engine = eng
	// Recover, never Start: Start would also launch the timer service, whose
	// background scan is the one thing this harness cannot allow.
	//
	// A scan that fails under fault injection is not fatal, and pretending
	// otherwise would delete a real case from the profile. A process whose
	// recovery scan half completed is a process that came up with some of its
	// task queues cold, and everything downstream -- the timer index, the next
	// restart -- has to be able to finish the job.
	if err := s.engine.Recover(s.ctx); err != nil {
		s.tracef("recovery scan failed after restart, some queues stay cold: %v", err)
	}
	return nil
}

// nextID hands out run and request identifiers. They are a counter rather than
// UUIDs so that two runs of the same seed produce byte-identical histories and a
// diff of two failure dumps is readable.
func (s *Simulation) nextID() string {
	s.ids++
	return fmt.Sprintf("run-%04d", s.ids)
}

func (s *Simulation) nextSeed() int64 {
	s.seeds++
	return s.seeds * 7919
}

// buildWorkload draws the executions this run will start.
func (s *Simulation) buildWorkload() []workload {
	out := make([]workload, 0, s.cfg.Executions)
	for i := 0; i < s.cfg.Executions; i++ {
		kind := workloadKind(s.rng.intn(int(numWorkloadKinds)))
		// Small n keeps histories short enough that a failure dump is readable
		// and long enough that ordering matters.
		n := 1 + s.rng.intn(3)
		out = append(out, newWorkload(kind, fmt.Sprintf("sim-%d-%s", i, kind.workflowType()), n))
	}
	return out
}

// Close releases everything the simulation owns.
func (s *Simulation) Close() {
	for _, w := range s.workers {
		w.close()
	}
	if s.engine != nil {
		_ = s.engine.Close(s.ctx)
	}
	s.matcher.Close()
	_ = s.raw.Close()
}

// Run executes the simulation and returns the first invariant violation.
//
// It has two phases. The chaos phase injects faults and checks the safety
// invariants after every step: nothing that must not happen may happen, however
// much goes wrong. The drain phase turns the faults off and runs to quiescence
// to check the liveness invariant: everything that was started must finish.
//
// Splitting them is what makes liveness a statement at all. Under permanent
// faults "eventually" is unfalsifiable; under faults that stop, it is a bound.
func (s *Simulation) Run() error {
	if err := s.chaosPhase(); err != nil {
		return err
	}
	if err := s.drainPhase(); err != nil {
		return err
	}
	return s.finalCheck()
}

// Report returns the counters for the run so far.
func (s *Simulation) Report() Report {
	r := s.stats
	r.Steps = s.step
	r.StoreOps, r.StoreFaults = s.store.counts()
	r.VirtualElapsed = s.clk.Now().Sub(simStartTime)
	stats := s.raw.Stats()
	r.Runs, r.Events = stats.Runs, stats.Events
	for _, id := range s.order {
		st := s.live[id]
		if st.canceled {
			r.Canceled++
		}
	}
	r.Completed = s.completedCount()
	return r
}

// Trace returns the run's event log.
func (s *Simulation) Trace() []string { return s.trace }

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

func (s *Simulation) chaosPhase() error {
	for s.step = 1; s.step <= s.cfg.MaxSteps; s.step++ {
		if len(s.pending) == 0 && s.allTerminal() {
			s.tracef("every execution reached a terminal state during the chaos phase")
			return nil
		}
		s.perform(s.choose())
		if err := s.checkInvariants(); err != nil {
			return err
		}
	}
	return nil
}

// drainPhase runs the system to quiescence with no faults at all.
func (s *Simulation) drainPhase() error {
	s.phase = "drain"
	s.store.setEnabled(false)
	s.cfg.Faults = NoFaults()
	s.tracef("faults disabled; draining to quiescence")

	// A cold engine first: it re-materialises the task references that were lost
	// to a full backlog or to a restart, which is the mechanism a real
	// deployment relies on and the only thing that can recover them.
	if err := s.newEngine(); err != nil {
		return s.violation("engine restart", err.Error())
	}

	recovered := false
	for i := 0; i < s.cfg.MaxDrainSteps; i++ {
		s.step++
		if s.allTerminal() && len(s.pending) == 0 {
			s.tracef("quiescent: every execution is terminal")
			return nil
		}

		progressed := false
		if len(s.pending) > 0 {
			s.startNext()
			progressed = true
		}
		for s.backlog(matching.KindWorkflow) > 0 {
			s.doWorkflowTask()
			progressed = true
		}
		for s.backlog(matching.KindActivity) > 0 {
			s.doActivityTask()
			progressed = true
		}
		if n := s.fireDueTimers(); n > 0 {
			progressed = true
		}
		if err := s.checkInvariants(); err != nil {
			return err
		}
		if progressed {
			recovered = false
			continue
		}

		// Nothing to do at this instant. Move to the next deadline; if there is
		// none, the only remaining hope is a recovery scan, and if that changes
		// nothing the run is genuinely stuck.
		if s.advanceToNextDeadline() {
			continue
		}
		if !recovered {
			s.tracef("no work and no deadlines; running a recovery scan")
			if err := s.newEngine(); err != nil {
				return s.violation("engine restart", err.Error())
			}
			recovered = true
			continue
		}
		return s.livenessViolation("the system went quiet with executions still running")
	}
	return s.livenessViolation(fmt.Sprintf("the drain phase used all %d steps", s.cfg.MaxDrainSteps))
}

// finalCheck asserts the properties that can only be stated about a finished
// run.
func (s *Simulation) finalCheck() error {
	if err := s.checkAll(); err != nil {
		return err
	}
	if !s.allTerminal() {
		return s.livenessViolation("an execution is still running after the drain phase")
	}
	if n := s.completedCount() + s.canceledCount(); n != len(s.order) {
		return s.violation("accounting",
			fmt.Sprintf("%d executions started but %d completed or were cancelled", len(s.order), n))
	}
	return nil
}

func (s *Simulation) completedCount() int {
	n := 0
	for _, id := range s.order {
		st := s.live[id]
		if st.verified && !st.canceled {
			n++
		}
	}
	return n
}

func (s *Simulation) canceledCount() int {
	n := 0
	for _, id := range s.order {
		st := s.live[id]
		if st.verified && st.canceled {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Action selection
// ---------------------------------------------------------------------------

type action int

const (
	actStart action = iota
	actWorkflowTask
	actActivityTask
	actTimer
	actAdvance
	actSignal
	actCancel
	actCrashWorker
	actRestartEngine
)

func (a action) String() string {
	switch a {
	case actStart:
		return "start"
	case actWorkflowTask:
		return "workflow-task"
	case actActivityTask:
		return "activity-task"
	case actTimer:
		return "fire-timers"
	case actAdvance:
		return "advance-clock"
	case actSignal:
		return "signal"
	case actCancel:
		return "cancel"
	case actCrashWorker:
		return "crash-worker"
	case actRestartEngine:
		return "restart-engine"
	}
	return fmt.Sprintf("action(%d)", int(a))
}

// choose picks the next action from the ones that are currently possible,
// weighted so that the system spends most of its time doing useful work and the
// rest of it being disrupted.
//
// Weights are integers and the draw is a single uniform over their sum, which
// keeps the number of PRNG draws per step independent of how many actions
// happen to be enabled. That matters: a variable number of draws would make the
// stream position depend on state, and two runs of the same seed would diverge
// the first time they differed at all.
func (s *Simulation) choose() action {
	type option struct {
		act    action
		weight int
	}
	opts := make([]option, 0, 9)
	add := func(a action, w int, enabled bool) {
		if enabled && w > 0 {
			opts = append(opts, option{a, w})
		}
	}
	f := s.cfg.Faults

	add(actStart, 8, len(s.pending) > 0)
	add(actWorkflowTask, 34, s.backlog(matching.KindWorkflow) > 0)
	add(actActivityTask, 28, s.backlog(matching.KindActivity) > 0)
	add(actTimer, 18, s.hasDueTimers())
	add(actAdvance, 14, true)
	add(actSignal, 5, s.signalCandidate() != nil)
	add(actCancel, 2, f.Cancel > 0 && s.cancelCandidate() != nil)
	add(actCrashWorker, 3, f.WorkerCrash > 0)
	add(actRestartEngine, 2, f.EngineRestart > 0)

	total := 0
	for _, o := range opts {
		total += o.weight
	}
	pick := s.rng.intn(total)
	for _, o := range opts {
		if pick < o.weight {
			return o.act
		}
		pick -= o.weight
	}
	return actAdvance
}

func (s *Simulation) perform(a action) {
	switch a {
	case actStart:
		s.startNext()
	case actWorkflowTask:
		s.doWorkflowTask()
	case actActivityTask:
		s.doActivityTask()
	case actTimer:
		s.fireDueTimers()
	case actAdvance:
		s.advanceClock()
	case actSignal:
		s.sendSignal()
	case actCancel:
		s.cancelOne()
	case actCrashWorker:
		s.crashWorker()
	case actRestartEngine:
		if err := s.newEngine(); err != nil {
			s.tracef("engine restart failed: %v", err)
			return
		}
		s.stats.EngineRestarts++
		s.tracef("engine restarted cold over the same store")
	}
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

func (s *Simulation) startNext() {
	w := s.pending[0]
	input, err := internalwf.EncodeArgs(s.conv, []any{w.arg})
	if err != nil {
		s.tracef("encoding the input of %s failed: %v", w.workflowID, err)
		return
	}
	// Register the execution before asking for it, not after.
	//
	// A start that fails ambiguously may still have created the run, and an
	// invariant sweep that then found a run the simulator did not know about
	// would report a bug in the engine for a gap in the harness. Registering up
	// front makes the harness's view a superset of the store's, which is the only
	// direction that is safe.
	st, known := s.live[w.workflowID]
	if !known {
		st = &execState{workload: w}
		s.live[w.workflowID] = st
		s.order = append(s.order, w.workflowID)
	}

	resp, err := s.engine.StartWorkflow(s.ctx, api.StartWorkflowRequest{
		Namespace:    simNamespace,
		WorkflowID:   w.workflowID,
		WorkflowType: w.kind.workflowType(),
		TaskQueue:    simTaskQueue,
		Input:        input,
		// A workflow task timeout is what turns "the worker that held this task
		// is gone" into a rescheduled task. Without one, every crash fault would
		// be an unrecoverable hang and the run would fail liveness for the wrong
		// reason.
		TaskTimeout: 15 * time.Second,
		// A fixed request ID makes a retried start return the original run
		// rather than a second one, so the simulator can retry a start that
		// failed ambiguously without inventing a duplicate execution.
		RequestID: "start:" + w.workflowID,
	})
	if err != nil {
		s.tracef("start %s failed: %v", w.workflowID, err)
		return
	}
	s.pending = s.pending[1:]
	st.started = true
	st.runID = resp.RunID
	s.tracef("started %s (%s, n=%d, want=%d) as %s", w.workflowID, w.kind.workflowType(), w.n, w.want, resp.RunID)
}

func (s *Simulation) doWorkflowTask() {
	w := s.pickWorker()
	task, err := s.engine.PollWorkflowTask(s.ctx, api.PollWorkflowTaskRequest{
		Namespace: simNamespace,
		TaskQueue: simTaskQueue,
		Identity:  w.identity,
	})
	if err != nil {
		s.tracef("poll workflow task failed: %v", err)
		return
	}
	if task.Empty {
		return
	}
	s.stats.WorkflowTasks++

	if s.chance(s.cfg.Faults.LostTask) {
		s.stats.LostTasks++
		s.tracef("lost workflow task %s started=%d (nothing will answer it)",
			task.Execution, task.StartedEventID)
		return
	}

	out := w.runWorkflowTask(task)

	if s.chance(s.cfg.Faults.WorkerCrash) {
		if out.exec != nil {
			_ = out.exec.Close()
		}
		w.crash()
		s.stats.WorkerCrashes++
		s.tracef("%s crashed holding workflow task %s started=%d",
			w.identity, task.Execution, task.StartedEventID)
		return
	}

	accepted := true
	switch {
	case out.failed:
		s.tracef("workflow task %s failed on %s: %s (%v)",
			task.Execution, w.identity, out.cause, out.err)
		if err := s.engine.RespondWorkflowTaskFailed(s.ctx, api.RespondWorkflowTaskFailedRequest{
			Namespace: simNamespace,
			Execution: task.Execution,
			Cause:     out.cause,
			Failure:   out.failure,
			Identity:  w.identity,
		}); err != nil {
			s.tracef("reporting the workflow task failure was rejected: %v", err)
		}
		accepted = false
	default:
		req := api.RespondWorkflowTaskCompletedRequest{
			Namespace:  simNamespace,
			Execution:  task.Execution,
			Commands:   out.result.Commands,
			Identity:   w.identity,
			SDKName:    "skald-sim",
			SDKVersion: "0",
		}
		if err := s.engine.RespondWorkflowTaskCompleted(s.ctx, req); err != nil {
			s.tracef("workflow task response for %s rejected: %v", task.Execution, err)
			accepted = false
		} else {
			s.tracef("%s completed workflow task %s started=%d with %d command(s)",
				w.identity, task.Execution, task.StartedEventID, len(out.result.Commands))
			if s.chance(s.cfg.Faults.DuplicateDelivery) {
				s.stats.DuplicateResponses++
				err := s.engine.RespondWorkflowTaskCompleted(s.ctx, req)
				s.tracef("duplicate workflow task response for %s: %v", task.Execution, err)
			}
		}
	}
	w.settleWorkflowTask(task.Execution.RunID, out, accepted)
}

func (s *Simulation) doActivityTask() {
	w := s.pickWorker()
	task, err := s.engine.PollActivityTask(s.ctx, api.PollActivityTaskRequest{
		Namespace: simNamespace,
		TaskQueue: simTaskQueue,
		Identity:  w.identity,
	})
	if err != nil {
		s.tracef("poll activity task failed: %v", err)
		return
	}
	if task.Empty {
		return
	}
	s.stats.ActivityTasks++

	if s.chance(s.cfg.Faults.LostTask) {
		s.stats.LostTasks++
		s.tracef("lost activity task %s/%s attempt=%d", task.Execution, task.ActivityID, task.Attempt)
		return
	}
	if s.chance(s.cfg.Faults.WorkerCrash) {
		w.crash()
		s.stats.WorkerCrashes++
		s.tracef("%s crashed running activity %s/%s attempt=%d",
			w.identity, task.Execution, task.ActivityID, task.Attempt)
		return
	}

	if s.chance(s.cfg.Faults.ActivityFailure) {
		s.stats.ActivityFailures++
		s.tracef("activity %s/%s attempt=%d failed (injected)", task.Execution, task.ActivityID, task.Attempt)
		if err := s.engine.RespondActivityTaskFailed(s.ctx, api.RespondActivityTaskFailedRequest{
			Namespace:        simNamespace,
			Execution:        task.Execution,
			ScheduledEventID: task.ScheduledEventID,
			Failure: &skald.ApplicationError{
				Type:    "SimTransient",
				Message: "injected activity failure",
			},
			Identity: w.identity,
		}); err != nil {
			s.tracef("reporting the activity failure was rejected: %v", err)
		}
		return
	}

	result, err := w.runActivity(s.ctx, task)
	if err != nil {
		s.tracef("activity %s/%s returned an error: %v", task.Execution, task.ActivityID, err)
		if respErr := s.engine.RespondActivityTaskFailed(s.ctx, api.RespondActivityTaskFailedRequest{
			Namespace:        simNamespace,
			Execution:        task.Execution,
			ScheduledEventID: task.ScheduledEventID,
			Failure:          skald.AsApplicationError(err),
			Identity:         w.identity,
		}); respErr != nil {
			s.tracef("reporting the activity failure was rejected: %v", respErr)
		}
		return
	}

	req := api.RespondActivityTaskCompletedRequest{
		Namespace:        simNamespace,
		Execution:        task.Execution,
		ScheduledEventID: task.ScheduledEventID,
		Result:           result,
		Identity:         w.identity,
	}
	if err := s.engine.RespondActivityTaskCompleted(s.ctx, req); err != nil {
		s.tracef("activity completion for %s/%s rejected: %v", task.Execution, task.ActivityID, err)
		return
	}
	s.tracef("%s completed activity %s/%s attempt=%d", w.identity, task.Execution, task.ActivityID, task.Attempt)
	if s.chance(s.cfg.Faults.DuplicateDelivery) {
		s.stats.DuplicateResponses++
		err := s.engine.RespondActivityTaskCompleted(s.ctx, req)
		s.tracef("duplicate activity completion for %s/%s: %v", task.Execution, task.ActivityID, err)
	}
}

// fireDueTimers plays the part of the durable timer service for one scan.
//
// It is deliberately the same shape as timers.Service.scan, including the order
// that makes redelivery safe: dispatch, and only then delete. A dispatch that
// fails leaves the entry in the index, so the timer is retried rather than lost.
func (s *Simulation) fireDueTimers() int {
	due, err := s.store.DueTimers(s.ctx, s.clk.Now(), 32)
	if err != nil {
		s.tracef("reading due timers failed: %v", err)
		return 0
	}
	n := 0
	for _, rec := range due {
		if err := s.engine.FireTimer(s.ctx, rec); err != nil {
			s.tracef("timer %s/%d kind=%d dispatch failed, kept for redelivery: %v",
				rec.WorkflowID, rec.EventID, rec.Kind, err)
			continue
		}
		if err := s.store.DeleteTimers(s.ctx, []persistence.TimerKey{rec.TimerKey}); err != nil {
			s.tracef("deleting timer %s/%d failed, it will be redelivered: %v",
				rec.WorkflowID, rec.EventID, err)
			continue
		}
		n++
		s.stats.TimersFired++
		s.tracef("fired timer %s/%d kind=%d", rec.WorkflowID, rec.EventID, rec.Kind)
	}
	return n
}

// advanceClock moves virtual time forward.
//
// Two modes. Normally it advances to the next durable deadline, which is the
// cheap way to reach a timeout without simulating the seconds in between. Under
// the clock-skew fault it jumps by an arbitrary amount instead, which fires
// several deadlines at once and puts the engine in the position of an operator's
// NTP correction.
func (s *Simulation) advanceClock() {
	if s.chance(s.cfg.Faults.ClockSkew) {
		d := time.Duration(1 + s.rng.int63n(int64(5*time.Minute)))
		s.clk.Advance(d)
		s.tracef("clock skew: jumped %s to %s", d, s.clk.Now().UTC().Format(time.RFC3339))
		return
	}
	if s.advanceToNextDeadline() {
		return
	}
	s.clk.Advance(time.Second)
	s.tracef("no deadlines pending; advanced 1s to %s", s.clk.Now().UTC().Format(time.RFC3339))
}

// advanceToNextDeadline moves the clock to the earliest pending timer and
// reports whether there was one.
func (s *Simulation) advanceToNextDeadline() bool {
	next, ok := s.earliestDeadline()
	if !ok {
		return false
	}
	now := s.clk.Now()
	d := next.Sub(now)
	if d < time.Millisecond {
		// Already due, or due within the resolution the histories use. Step past
		// it so that "at or before now" is unambiguous.
		d = time.Millisecond
	}
	s.clk.Advance(d)
	s.tracef("advanced %s to the next deadline at %s", d, s.clk.Now().UTC().Format(time.RFC3339))
	return true
}

// earliestDeadline reads the soonest pending timer directly from the underlying
// store, bypassing fault injection.
//
// The simulator is the observer here, not a participant: it is deciding where to
// move time, which is a question about the world rather than an operation the
// system performs. Faulting it would only make the harness flaky.
func (s *Simulation) earliestDeadline() (time.Time, bool) {
	due, err := s.raw.DueTimers(s.ctx, time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), 1)
	if err != nil || len(due) == 0 {
		return time.Time{}, false
	}
	return due[0].FireAt, true
}

func (s *Simulation) hasDueTimers() bool {
	due, err := s.raw.DueTimers(s.ctx, s.clk.Now(), 1)
	return err == nil && len(due) > 0
}

func (s *Simulation) sendSignal() {
	st := s.signalCandidate()
	if st == nil {
		return
	}
	if err := s.engine.SignalWorkflow(s.ctx, api.SignalWorkflowRequest{
		Namespace:  simNamespace,
		WorkflowID: st.workflowID,
		SignalName: SimSignalName,
		Input:      skald.MustPayload(st.n),
	}); err != nil {
		s.tracef("signalling %s failed: %v", st.workflowID, err)
		return
	}
	s.stats.Signals++
	s.tracef("signalled %s", st.workflowID)
}

func (s *Simulation) cancelOne() {
	st := s.cancelCandidate()
	if st == nil {
		return
	}
	// Marked before the call, and left marked whatever it returns.
	//
	// A cancel that fails ambiguously may well have been recorded, so from the
	// oracle's point of view the moment the request leaves is the moment this
	// execution stops owing a particular answer. Marking it only on success
	// would make the harness assert something the client could not know.
	st.canceled = true
	if err := s.engine.CancelWorkflow(s.ctx, api.CancelWorkflowRequest{
		Namespace:  simNamespace,
		WorkflowID: st.workflowID,
		Reason:     "simulated cancellation",
	}); err != nil {
		s.tracef("cancelling %s failed (it may still have been recorded): %v", st.workflowID, err)
		return
	}
	s.tracef("cancelled %s", st.workflowID)
}

func (s *Simulation) crashWorker() {
	w := s.pickWorker()
	w.crash()
	s.stats.WorkerCrashes++
	s.tracef("crashed %s between tasks", w.identity)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Simulation) pickWorker() *simWorker { return s.workers[s.rng.intn(len(s.workers))] }

func (s *Simulation) chance(p float64) bool { return p > 0 && s.rng.float() < p }

func (s *Simulation) backlog(kind matching.Kind) int {
	return s.matcher.Stats(matching.QueueKey{
		Namespace: simNamespace,
		TaskQueue: simTaskQueue,
		Kind:      kind,
	}).Backlog
}

// signalCandidate returns the first running execution a signal can influence.
func (s *Simulation) signalCandidate() *execState {
	for _, id := range s.order {
		st := s.live[id]
		if st.signalable && !st.canceled && !s.terminal(id) {
			return st
		}
	}
	return nil
}

func (s *Simulation) cancelCandidate() *execState {
	for _, id := range s.order {
		st := s.live[id]
		if !st.canceled && !s.terminal(id) {
			return st
		}
	}
	return nil
}

// terminal reports whether the current run of a workflow ID has closed. A run
// that continued as new is *not* terminal in this sense: the execution goes on
// in its successor, which the store has already made current.
func (s *Simulation) terminal(workflowID string) bool {
	rec, err := s.raw.GetExecution(s.ctx, simNamespace, workflowID, "")
	if err != nil {
		return false
	}
	return rec.Status.Terminal() && rec.Status != skald.StatusContinuedAsNew
}

func (s *Simulation) allTerminal() bool {
	for _, id := range s.order {
		if !s.terminal(id) {
			return false
		}
	}
	return len(s.pending) == 0
}

func (s *Simulation) tracef(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf("%-5s %5d %s | %s",
		s.phase, s.step, s.clk.Now().UTC().Format("15:04:05.000"), fmt.Sprintf(format, args...)))
}

// ---------------------------------------------------------------------------
// Failure reporting
// ---------------------------------------------------------------------------

// Violation is an invariant failure. It carries everything needed to understand
// and reproduce the run: the seed first, because that is the only field a reader
// actually needs.
type Violation struct {
	Seed      int64
	Step      int
	Phase     string
	Rule      string
	Detail    string
	Report    Report
	Trace     []string
	Histories []string
}

func (v *Violation) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nsimulation: invariant %q violated at step %d of the %s phase\n", v.Rule, v.Step, v.Phase)
	fmt.Fprintf(&b, "  %s\n\n", v.Detail)
	fmt.Fprintf(&b, "reproduce with:\n  go test ./internal/simulation -run TestSimulationSeed -skald.seed=%d\n\n", v.Seed)
	fmt.Fprintf(&b, "counters:\n  %s\n\n", v.Report)
	if len(v.Histories) > 0 {
		b.WriteString("histories:\n")
		for _, h := range v.Histories {
			b.WriteString(h)
		}
		b.WriteString("\n")
	}
	b.WriteString("trace:\n")
	for _, line := range v.Trace {
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

func (s *Simulation) violation(rule, detail string) error {
	return &Violation{
		Seed:      s.cfg.Seed,
		Step:      s.step,
		Phase:     s.phase,
		Rule:      rule,
		Detail:    detail,
		Report:    s.Report(),
		Trace:     s.trace,
		Histories: s.dumpHistories(),
	}
}

func (s *Simulation) livenessViolation(detail string) error {
	var open []string
	for _, id := range s.order {
		if !s.terminal(id) {
			rec, err := s.raw.GetExecution(s.ctx, simNamespace, id, "")
			if err != nil {
				open = append(open, fmt.Sprintf("%s (unreadable: %v)", id, err))
				continue
			}
			open = append(open, fmt.Sprintf("%s run=%s status=%s events=%d",
				id, rec.RunID, rec.Status, rec.LastEventID))
		}
	}
	if len(s.pending) > 0 {
		open = append(open, fmt.Sprintf("%d execution(s) were never started", len(s.pending)))
	}
	return s.violation("liveness", detail+"\n  still open: "+strings.Join(open, "\n               "))
}

// dumpHistories renders every stored run, newest event last, for the failure
// report. It reads through the raw store so that a fault cannot hide the
// evidence.
func (s *Simulation) dumpHistories() []string {
	res, err := s.raw.ListExecutions(s.ctx, persistence.ListFilter{Namespace: simNamespace, PageSize: 1000})
	if err != nil {
		return []string{fmt.Sprintf("  (listing executions failed: %v)\n", err)}
	}
	recs := res.Records
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].WorkflowID != recs[j].WorkflowID {
			return recs[i].WorkflowID < recs[j].WorkflowID
		}
		return recs[i].RunID < recs[j].RunID
	})

	out := make([]string, 0, len(recs))
	for _, rec := range recs {
		var b strings.Builder
		fmt.Fprintf(&b, "  %s/%s  type=%s status=%s version=%d\n",
			rec.WorkflowID, rec.RunID, rec.WorkflowType, rec.Status, rec.Version)
		h, err := s.raw.ReadHistory(s.ctx, rec.Namespace, rec.WorkflowID, rec.RunID, 1, 0)
		if err != nil {
			fmt.Fprintf(&b, "    (unreadable: %v)\n", err)
			out = append(out, b.String())
			continue
		}
		for _, ev := range h {
			fmt.Fprintf(&b, "    %s\n", describeEvent(ev))
		}
		// The pending timer set is the other half of the picture. A liveness
		// violation is almost always "nothing is scheduled to wake this run",
		// and the history alone cannot show that: it records what happened, not
		// what was supposed to happen next.
		if timers := s.dumpTimers(rec); timers != "" {
			b.WriteString(timers)
		}
		out = append(out, b.String())
	}
	return out
}

// dumpTimers renders the due-time index entries a run still owns.
func (s *Simulation) dumpTimers(rec persistence.ExecutionRecord) string {
	// Far enough ahead to catch every entry: the point is to show what exists,
	// not what is due.
	due, err := s.raw.DueTimers(s.ctx, s.clk.Now().Add(1000*time.Hour), 10_000)
	if err != nil {
		return fmt.Sprintf("    (timers unreadable: %v)\n", err)
	}
	var b strings.Builder
	for _, t := range due {
		if t.Namespace != rec.Namespace || t.WorkflowID != rec.WorkflowID || t.RunID != rec.RunID {
			continue
		}
		fmt.Fprintf(&b, "    timer kind=%d event=%d attempt=%d fires=%s\n",
			t.Kind, t.EventID, t.Attempt, t.FireAt.UTC().Format("15:04:05.000"))
	}
	if b.Len() == 0 {
		return "    (no pending timers)\n"
	}
	return b.String()
}

// describeEvent renders an event with the back-references that matter when
// reading a failure, which Event.String deliberately leaves out.
func describeEvent(ev history.Event) string {
	base := fmt.Sprintf("%3d %-34s %s", ev.ID, ev.Type(), ev.Time.UTC().Format("15:04:05.000"))
	switch a := ev.Attrs.(type) {
	case history.ActivityTaskScheduledAttributes:
		return fmt.Sprintf("%s id=%s type=%s", base, a.ActivityID, a.ActivityType)
	case history.ActivityTaskStartedAttributes:
		return fmt.Sprintf("%s scheduled=%d", base, a.ScheduledEventID)
	case history.ActivityTaskCompletedAttributes:
		return fmt.Sprintf("%s scheduled=%d started=%d", base, a.ScheduledEventID, a.StartedEventID)
	case history.ActivityTaskFailedAttributes:
		return fmt.Sprintf("%s scheduled=%d failure=%s", base, a.ScheduledEventID, a.Failure.Message)
	case history.ActivityTaskTimedOutAttributes:
		return fmt.Sprintf("%s scheduled=%d", base, a.ScheduledEventID)
	case history.WorkflowTaskStartedAttributes:
		return fmt.Sprintf("%s scheduled=%d identity=%s", base, a.ScheduledEventID, a.Identity)
	case history.WorkflowTaskCompletedAttributes:
		return fmt.Sprintf("%s started=%d identity=%s", base, a.StartedEventID, a.Identity)
	case history.WorkflowTaskFailedAttributes:
		msg := ""
		if a.Failure != nil {
			msg = a.Failure.Message
		}
		return fmt.Sprintf("%s started=%d cause=%s %s", base, a.StartedEventID, a.Cause, msg)
	case history.TimerStartedAttributes:
		return fmt.Sprintf("%s id=%s after=%s", base, a.TimerID, a.StartToFireTimeout)
	case history.TimerFiredAttributes:
		return fmt.Sprintf("%s started=%d", base, a.StartedEventID)
	case history.WorkflowExecutionFailedAttributes:
		msg := ""
		if a.Failure != nil {
			msg = a.Failure.Message
		}
		return fmt.Sprintf("%s failure=%s", base, msg)
	case history.WorkflowExecutionContinuedAsNewAttributes:
		return fmt.Sprintf("%s new_run=%s", base, a.NewRunID)
	}
	return base
}
