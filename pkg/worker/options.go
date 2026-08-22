package worker

import (
	"io"
	"log/slog"
	"time"

	"github.com/Liona-orph/skald/pkg/skald"
)

// SDK identity, recorded on every workflow task completion so that a history can
// be attributed to the client that produced it during an incident.
const (
	SDKName    = "skald-go"
	SDKVersion = "0.1.0"
)

// Defaults for Options. They are sized for a single process on a normal machine:
// generous enough that a demo works without tuning, small enough that a
// misconfigured worker cannot exhaust a server on its own.
const (
	DefaultMaxConcurrentWorkflowTasks = 64
	DefaultMaxConcurrentActivityTasks = 64
	DefaultWorkflowTaskPollers        = 2
	DefaultActivityTaskPollers        = 2
	DefaultStickyCacheSize            = 512
	DefaultDeadlockDetectionTimeout   = 5 * time.Second
	DefaultMaxPollBackoff             = 10 * time.Second
	DefaultInitialPollBackoff         = 50 * time.Millisecond
	DefaultFailedTaskBackoff          = 100 * time.Millisecond
	DefaultHeartbeatThrottle          = 5 * time.Second
	DefaultStopTimeout                = 30 * time.Second
)

// Options configure a Worker. The zero value is usable: every field has a
// documented default.
type Options struct {
	// Namespace the worker polls. Defaults to skald.DefaultNamespace.
	Namespace string
	// Identity names this worker in histories and in the engine's ownership
	// checks. Defaults to "<hostname>@<pid>", which is what an operator needs to
	// find the process that held a stuck task.
	Identity string

	// MaxConcurrentWorkflowTasks bounds workflow tasks executing at once.
	// Workflow code is supposed to be a fast decision function, so this can be
	// high; it is a memory bound, not a CPU one.
	MaxConcurrentWorkflowTasks int
	// MaxConcurrentActivityTasks bounds activities executing at once. This is
	// the number that protects a downstream dependency, so it is the one worth
	// tuning.
	MaxConcurrentActivityTasks int

	// WorkflowTaskPollers and ActivityTaskPollers are the number of long polls
	// kept open. More pollers reduce dispatch latency when a queue is bursty and
	// cost one idle connection each.
	WorkflowTaskPollers int
	ActivityTaskPollers int

	// StickyCacheSize bounds the number of warm workflow instances kept in
	// memory. A hit turns a task that would replay a thousand events into one
	// that applies three. Correctness never depends on a hit.
	StickyCacheSize int
	// DisableStickyCache makes every workflow task a cold replay from event 1.
	//
	// It is a real operational knob -- a memory-constrained worker may prefer CPU
	// to residency -- and it is also how the test suite proves the invariant the
	// whole design rests on: with the cache off, every result must be identical.
	DisableStickyCache bool

	// DataConverter encodes and decodes every payload. Defaults to JSON.
	DataConverter skald.DataConverter

	// Logger receives worker-level events. Defaults to a discarding logger:
	// a library that writes to stderr without being asked is a library that
	// corrupts somebody's structured log pipeline.
	Logger *slog.Logger

	// DeadlockDetectionTimeout bounds one call into workflow code before the
	// worker reports which coroutines are stuck. Keep it below the server's
	// workflow task timeout so the worker's diagnosis wins the race against the
	// server's far less specific "worker never came back".
	DeadlockDetectionTimeout time.Duration

	// MaxPollBackoff caps the exponential backoff applied after poll errors.
	MaxPollBackoff time.Duration
	// FailedTaskBackoff is the pause after a workflow task failure. It exists
	// because the engine re-schedules a failed task immediately -- correct, since
	// a rollback must be picked up at once -- which without a pause would spin a
	// core while a bad deploy is being reverted.
	FailedTaskBackoff time.Duration
	// HeartbeatThrottle bounds heartbeat traffic for activities that declare no
	// heartbeat timeout.
	HeartbeatThrottle time.Duration

	// DisableWorkflowWorker and DisableActivityWorker let one process serve only
	// one half of a task queue, which is how activities get their own hardware.
	DisableWorkflowWorker bool
	DisableActivityWorker bool
}

func (o *Options) applyDefaults(identity string) {
	if o.Namespace == "" {
		o.Namespace = skald.DefaultNamespace
	}
	if o.Identity == "" {
		o.Identity = identity
	}
	if o.MaxConcurrentWorkflowTasks <= 0 {
		o.MaxConcurrentWorkflowTasks = DefaultMaxConcurrentWorkflowTasks
	}
	if o.MaxConcurrentActivityTasks <= 0 {
		o.MaxConcurrentActivityTasks = DefaultMaxConcurrentActivityTasks
	}
	if o.WorkflowTaskPollers <= 0 {
		o.WorkflowTaskPollers = DefaultWorkflowTaskPollers
	}
	if o.ActivityTaskPollers <= 0 {
		o.ActivityTaskPollers = DefaultActivityTaskPollers
	}
	if o.StickyCacheSize <= 0 {
		o.StickyCacheSize = DefaultStickyCacheSize
	}
	if o.DataConverter == nil {
		o.DataConverter = skald.JSONConverter{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.DeadlockDetectionTimeout <= 0 {
		o.DeadlockDetectionTimeout = DefaultDeadlockDetectionTimeout
	}
	if o.MaxPollBackoff <= 0 {
		o.MaxPollBackoff = DefaultMaxPollBackoff
	}
	if o.FailedTaskBackoff < 0 {
		o.FailedTaskBackoff = 0
	} else if o.FailedTaskBackoff == 0 {
		o.FailedTaskBackoff = DefaultFailedTaskBackoff
	}
	if o.HeartbeatThrottle <= 0 {
		o.HeartbeatThrottle = DefaultHeartbeatThrottle
	}
	// Never poll for more work than can be executed: an unmatched poll takes a
	// task out of the queue and holds it, which looks to everyone else like a
	// stuck workflow.
	if o.WorkflowTaskPollers > o.MaxConcurrentWorkflowTasks {
		o.WorkflowTaskPollers = o.MaxConcurrentWorkflowTasks
	}
	if o.ActivityTaskPollers > o.MaxConcurrentActivityTasks {
		o.ActivityTaskPollers = o.MaxConcurrentActivityTasks
	}
}
