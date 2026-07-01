package simulation

import (
	"flag"
	"fmt"
	"strings"
	"testing"
)

// Command-line control of the simulator.
//
// The flags are prefixed so that they cannot collide with the testing package's
// own, and so that `go test ./... -skald.long` is unambiguous about which suite
// it is asking for.
var (
	seedFlag = flag.Int64("skald.seed", 0,
		"replay a single simulation seed; see internal/simulation/README.md")
	longFlag = flag.Bool("skald.long", false,
		"run the extended simulation soak instead of the CI-sized sweep")
	seedsFlag = flag.Int("skald.seeds", 0,
		"override the number of seeds the sweep runs")
)

const (
	// ciSeeds is sized so that the sweep finishes in a few seconds under -race
	// on a laptop. It is the number that has to stay honest: a suite nobody runs
	// because it takes a minute finds no bugs at all.
	ciSeeds = 300
	// soakSeeds is what -skald.long runs, with the aggressive fault profile.
	soakSeeds = 5000
)

// regressionSeeds are seeds that once failed.
//
// Every entry is a bug that shipped and a bug that was fixed, and every entry
// runs on every build forever. That is the whole return on a deterministic
// simulator: a failure is not a story about a flaky afternoon, it is a number
// you can put in a table.
var regressionSeeds = []struct {
	seed int64
	what string
}{
	{
		seed: 12,
		what: "engine: a redelivered activity-timeout timer resolved an activity that a " +
			"racing completion had already resolved, so the history carried two results " +
			"for one scheduled activity",
	},
	{
		seed: 77,
		what: "engine: after an ambiguous AppendHistory the rebuilt-state cache still held " +
			"the pre-write entry, so the retry replayed commands against stale state",
	},
	{
		seed: 204,
		what: "engine: a workflow task that timed out while a signal was buffered scheduled " +
			"a replacement task without clearing the old one, briefly putting two in flight",
	},
	{
		seed: 5,
		what: "engine: closing a run and creating its continue-as-new successor were two " +
			"store writes, so a fault between them left the predecessor CONTINUED_AS_NEW " +
			"with a successor that did not exist and nothing running; the successor is " +
			"now created inside the same transaction via AppendHistoryRequest.CreateSuccessor",
	},
	{
		seed: 43,
		what: "engine: recovery re-materialised task queues but never reconciled the " +
			"due-time index, so an execution that lost its only pending timer stalled " +
			"forever while still reporting RUNNING; Recover now re-arms every deadline " +
			"the state implies",
	},
}

func baseConfig(seed int64) Config {
	return Config{
		Seed:       seed,
		Workers:    3,
		Executions: 6,
		Faults:     DefaultFaults(),
	}
}

// runSeed executes one simulation and reports the failure in full.
func runSeed(t *testing.T, cfg Config) Report {
	t.Helper()
	sim, err := New(cfg)
	if err != nil {
		t.Fatalf("building the simulation: %v", err)
	}
	defer sim.Close()

	if err := sim.Run(); err != nil {
		t.Fatalf("%v", err)
	}
	return sim.Report()
}

// TestSimulation sweeps many seeds with the default fault profile.
//
// Each seed is an independent universe: a different workload, a different
// interleaving, a different set of things that went wrong. The value is in the
// count -- no single seed is interesting, and three hundred of them cover
// orderings no hand-written test would think to write down.
func TestSimulation(t *testing.T) {
	if *seedFlag != 0 {
		t.Skipf("-skald.seed=%d was given; run TestSimulationSeed instead", *seedFlag)
	}
	seeds := ciSeeds
	faults := DefaultFaults()
	if *longFlag {
		seeds, faults = soakSeeds, AggressiveFaults()
	}
	if *seedsFlag > 0 {
		seeds = *seedsFlag
	}

	var total Report
	for seed := int64(1); seed <= int64(seeds); seed++ {
		cfg := baseConfig(seed)
		cfg.Faults = faults
		r := runSeed(t, cfg)
		total.accumulate(r)
		if t.Failed() {
			return
		}
	}
	t.Logf("%d seeds: %s", seeds, total)

	// A sweep in which nothing ever went wrong is a sweep that proves nothing.
	// These assertions are about the *simulator*, not the system: they fail when
	// a fault has become unreachable, which is the failure mode a fault injector
	// dies of.
	for _, c := range []struct {
		name string
		got  int
	}{
		{"workflow tasks", total.WorkflowTasks},
		{"activity tasks", total.ActivityTasks},
		{"timers fired", total.TimersFired},
		{"worker crashes", total.WorkerCrashes},
		{"engine restarts", total.EngineRestarts},
		{"lost tasks", total.LostTasks},
		{"duplicate responses", total.DuplicateResponses},
		{"injected activity failures", total.ActivityFailures},
		{"store faults", total.StoreFaults},
		{"signals", total.Signals},
	} {
		if c.got == 0 {
			t.Errorf("the sweep produced zero %s; that fault is unreachable and the "+
				"profile is not exercising what it claims to", c.name)
		}
	}
	if total.Completed == 0 {
		t.Error("no execution ever completed successfully")
	}
}

// TestSimulationSeed replays exactly one seed.
//
//	go test ./internal/simulation -run TestSimulationSeed -skald.seed=12345 -v
func TestSimulationSeed(t *testing.T) {
	if *seedFlag == 0 {
		t.Skip("pass -skald.seed=N to replay a single seed")
	}
	cfg := baseConfig(*seedFlag)
	if *longFlag {
		cfg.Faults = AggressiveFaults()
	}
	sim, err := New(cfg)
	if err != nil {
		t.Fatalf("building the simulation: %v", err)
	}
	defer sim.Close()

	runErr := sim.Run()
	// The trace is printed either way: a seed being replayed by hand is being
	// replayed because somebody wants to read it.
	t.Logf("report: %s", sim.Report())
	for _, line := range sim.Trace() {
		t.Log(line)
	}
	if runErr != nil {
		t.Fatalf("%v", runErr)
	}
}

// TestSimulationRegressionSeeds runs the seeds that previously found bugs.
func TestSimulationRegressionSeeds(t *testing.T) {
	for _, rs := range regressionSeeds {
		t.Run(fmt.Sprintf("seed-%d", rs.seed), func(t *testing.T) {
			t.Logf("regression: %s", rs.what)
			runSeed(t, baseConfig(rs.seed))
		})
	}
}

// TestSimulationIsDeterministic is the test that makes every other simulation
// test worth writing.
//
// If two runs of one seed can differ, then a seed is not a reproduction, a
// failure is not investigable, and the regression table above is decoration. It
// compares the traces line for line rather than only the outcome, because two
// runs that reach the same answer by different routes have already lost the
// property.
func TestSimulationIsDeterministic(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 42, 1009} {
		first := traceFor(t, seed)
		second := traceFor(t, seed)
		if len(first) != len(second) {
			t.Fatalf("seed %d produced %d trace lines and then %d", seed, len(first), len(second))
		}
		for i := range first {
			if first[i] != second[i] {
				t.Fatalf("seed %d diverged at trace line %d:\n  first:  %s\n  second: %s\n"+
					"context:\n%s", seed, i, first[i], second[i], strings.Join(first[max(0, i-8):i], "\n"))
			}
		}
	}
}

func traceFor(t *testing.T, seed int64) []string {
	t.Helper()
	sim, err := New(baseConfig(seed))
	if err != nil {
		t.Fatalf("building the simulation: %v", err)
	}
	defer sim.Close()
	if err := sim.Run(); err != nil {
		t.Fatalf("%v", err)
	}
	return sim.Trace()
}

// TestSimulationFaultFreeRunIsClean is the control experiment. With no faults at
// all every execution must complete, and it must do so without a single workflow
// task failure -- so a failure here is a bug in the workflows or in the harness,
// not in the recovery paths.
func TestSimulationFaultFreeRunIsClean(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		cfg := baseConfig(seed)
		cfg.Faults = NoFaults()
		r := runSeed(t, cfg)
		if r.Completed != r.Executions {
			t.Fatalf("seed %d: %d of %d executions completed with no faults injected",
				seed, r.Completed, r.Executions)
		}
	}
}

// accumulate sums the counters of one run into a total, for the sweep's summary.
func (r *Report) accumulate(o Report) {
	r.Executions += o.Executions
	r.Completed += o.Completed
	r.Canceled += o.Canceled
	r.Runs += o.Runs
	r.Events += o.Events
	r.Steps += o.Steps
	r.WorkflowTasks += o.WorkflowTasks
	r.ActivityTasks += o.ActivityTasks
	r.TimersFired += o.TimersFired
	r.Signals += o.Signals
	r.WorkerCrashes += o.WorkerCrashes
	r.EngineRestarts += o.EngineRestarts
	r.LostTasks += o.LostTasks
	r.DuplicateResponses += o.DuplicateResponses
	r.ActivityFailures += o.ActivityFailures
	r.StoreOps += o.StoreOps
	r.StoreFaults += o.StoreFaults
	r.VirtualElapsed += o.VirtualElapsed
}
