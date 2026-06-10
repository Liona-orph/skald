// Package matching dispatches tasks to the workers that poll for them.
//
// # Derived, not persisted
//
// A queue here holds nothing that cannot be rebuilt. Each entry is a *task
// reference* -- the execution's identity plus the history event that scheduled
// the work -- and never a payload. The durable truth is the history, and the
// engine reconstructs every pending reference at startup by scanning open
// executions (see the package documentation of internal/persistence). Losing
// this whole data structure therefore costs latency and never correctness,
// which is the property that lets it be a plain in-memory map with no
// write-ahead log, no compaction and no consistency protocol of its own.
//
// # Why references and not payloads
//
// An activity input can be a megabyte. Buffering payloads would make queue
// memory proportional to backlog size times payload size, so one workflow
// fanning out ten thousand activities could evict every other tenant's queue.
// Holding a reference makes an entry a few dozen bytes; the payload is read
// from the store on the dispatch path, where it is needed exactly once.
//
// # The two match paths
//
// A sync match happens when a poller is already parked: the task goes straight
// to that poller's channel and never touches the backlog. This is the path that
// matters, because a healthy deployment has more pollers than tasks, and it is
// what makes end-to-end latency a function of the store write rather than of a
// queue scan. An async match happens when no poller is waiting: the reference
// joins the backlog and is handed to the next poller that arrives.
package matching

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skald-io/skald/internal/clock"
	"github.com/skald-io/skald/pkg/skald"
)

// Kind separates workflow work from activity work.
//
// The two share every mechanism but must never share a queue: a worker
// registers different handlers for each, and a workflow task delivered to an
// activity poller would be a protocol violation rather than a slow path.
type Kind int32

const (
	// KindWorkflow is a workflow task: run workflow code against new history.
	KindWorkflow Kind = iota
	// KindActivity is an activity task: run one attempt of an activity.
	KindActivity
)

func (k Kind) String() string {
	switch k {
	case KindWorkflow:
		return "workflow"
	case KindActivity:
		return "activity"
	}
	return fmt.Sprintf("Kind(%d)", int32(k))
}

// QueueKey identifies one dispatch queue. Queues are partitioned by namespace
// so that one tenant's backlog cannot delay another's, and by kind so that the
// two poll paths never interfere.
type QueueKey struct {
	Namespace string
	TaskQueue string
	Kind      Kind
}

func (k QueueKey) String() string {
	return k.Namespace + "/" + k.TaskQueue + "/" + k.Kind.String()
}

// Task is a reference to work that is ready to be executed.
type Task struct {
	Namespace string
	TaskQueue string
	Execution skald.WorkflowExecution
	// ScheduledEventID is the history event that created the work. It is the
	// only identifier the engine needs to find the task again, and it is stable
	// across retries in a way an activity ID is not.
	ScheduledEventID int64
	// Attempt is the attempt this dispatch represents. It is carried on the
	// reference because the attempt counter of an activity in backoff lives in
	// the durable timer index rather than in the history; see the engine's
	// activity documentation.
	Attempt int32
	// EnqueuedAt is when the reference entered matching, used for queue latency
	// metrics and for nothing else.
	EnqueuedAt time.Time
}

// Errors returned by Add.
var (
	// ErrBacklogFull means the queue is at capacity. See Config.MaxBacklog for
	// why the caller may safely drop the reference.
	ErrBacklogFull = errors.New("matching: task queue backlog is full")
	// ErrClosed means the matcher has been shut down.
	ErrClosed = errors.New("matching: closed")
)

// Metrics receives the counters an operator needs to answer "is dispatch
// healthy?".
//
// It is an interface with no dependencies so that telemetry can be wired in
// later without this package importing a metrics library: a Prometheus adapter
// lives next to the Prometheus registry, not next to the queue. Every method
// must be cheap and non-blocking; matching calls them while holding no lock but
// on the latency-critical path.
type Metrics interface {
	// TaskAdded fires for every accepted task. sync reports whether it was
	// handed straight to a waiting poller; the ratio of sync to total is the
	// single most useful dispatch health signal there is.
	TaskAdded(key QueueKey, sync bool)
	// TaskDropped fires when a task is refused or discarded.
	TaskDropped(key QueueKey, reason string)
	// BacklogDepth reports the queue depth after a change.
	BacklogDepth(key QueueKey, depth int)
	// PollerCount reports the number of parked pollers after a change.
	PollerCount(key QueueKey, n int)
}

// NopMetrics discards everything. It is the default so that no call site has to
// nil-check.
type NopMetrics struct{}

func (NopMetrics) TaskAdded(QueueKey, bool)     {}
func (NopMetrics) TaskDropped(QueueKey, string) {}
func (NopMetrics) BacklogDepth(QueueKey, int)   {}
func (NopMetrics) PollerCount(QueueKey, int)    {}

// Drop reasons reported through Metrics.TaskDropped.
const (
	// DropBacklogFull means Add was refused because the queue was at capacity.
	DropBacklogFull = "backlog_full"
	// DropClosed means the matcher shut down with tasks still queued.
	DropClosed = "closed"
)

// Defaults for Config.
const (
	// DefaultPollTimeout is how long an unmatched long poll waits before
	// returning empty. It is under a minute because proxies and load balancers
	// routinely kill idle connections at sixty seconds, and a poll that dies in
	// transit looks like a task loss to the worker.
	DefaultPollTimeout = 30 * time.Second
	// DefaultMaxBacklog bounds one queue. At roughly 100 bytes per reference
	// this is a few megabytes per queue -- large enough that a healthy burst is
	// absorbed, small enough that a stuck queue cannot exhaust a server.
	DefaultMaxBacklog = 50_000
)

// Config parameterises a Matcher.
type Config struct {
	// Clock drives poll timeouts. Defaults to clock.System.
	Clock clock.Clock
	// PollTimeout bounds a long poll. Defaults to DefaultPollTimeout.
	PollTimeout time.Duration
	// MaxBacklog bounds the number of queued references per queue.
	//
	// The policy when full is to *reject the newest* task with ErrBacklogFull
	// rather than to evict the oldest. Two reasons. First, evicting the oldest
	// punishes the task that has already waited longest, turning a full queue
	// into unbounded latency for the unlucky. Second, the caller of Add is the
	// engine, still inside the transaction's post-commit step: it can log,
	// count and move on, knowing the reference is reconstructible from the
	// history by the recovery scan or by a schedule-to-start timeout. A
	// rejected task is delayed, never lost.
	MaxBacklog int
	// Metrics receives dispatch counters. Defaults to NopMetrics.
	Metrics Metrics
}

// Stats is a point-in-time snapshot of one queue.
type Stats struct {
	Backlog      int
	Pollers      int
	SyncMatches  uint64
	AsyncMatches uint64
	Dropped      uint64
	PollTimeouts uint64
}

// Matcher owns every task queue in the process.
type Matcher struct {
	clk         clock.Clock
	pollTimeout time.Duration
	maxBacklog  int
	metrics     Metrics

	mu     sync.RWMutex
	queues map[QueueKey]*queue

	closeOnce sync.Once
	closed    chan struct{}
}

// New returns a Matcher ready for use.
func New(cfg Config) *Matcher {
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = DefaultPollTimeout
	}
	if cfg.MaxBacklog <= 0 {
		cfg.MaxBacklog = DefaultMaxBacklog
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	return &Matcher{
		clk:         cfg.Clock,
		pollTimeout: cfg.PollTimeout,
		maxBacklog:  cfg.MaxBacklog,
		metrics:     cfg.Metrics,
		queues:      make(map[QueueKey]*queue),
		closed:      make(chan struct{}),
	}
}

// AddWorkflowTask makes a workflow task pollable.
func (m *Matcher) AddWorkflowTask(t Task) error { return m.add(KindWorkflow, t) }

// AddActivityTask makes an activity task pollable.
func (m *Matcher) AddActivityTask(t Task) error { return m.add(KindActivity, t) }

// PollWorkflowTask waits for workflow work on the given queue.
//
// The second result is false when the poll expired or ctx ended, which is not
// an error: an idle worker is the normal case, and turning it into an error
// would make every worker log noise proportional to its idleness.
func (m *Matcher) PollWorkflowTask(ctx context.Context, namespace, taskQueue string) (Task, bool, error) {
	return m.poll(ctx, QueueKey{Namespace: namespace, TaskQueue: taskQueue, Kind: KindWorkflow})
}

// PollActivityTask waits for activity work on the given queue.
func (m *Matcher) PollActivityTask(ctx context.Context, namespace, taskQueue string) (Task, bool, error) {
	return m.poll(ctx, QueueKey{Namespace: namespace, TaskQueue: taskQueue, Kind: KindActivity})
}

// Close releases every parked poller. Polls in flight return empty, and later
// calls fail with ErrClosed.
func (m *Matcher) Close() {
	m.closeOnce.Do(func() {
		close(m.closed)
		m.mu.RLock()
		defer m.mu.RUnlock()
		for key, q := range m.queues {
			n := q.drain()
			for i := 0; i < n; i++ {
				m.metrics.TaskDropped(key, DropClosed)
			}
		}
	})
}

// Stats returns a snapshot of one queue. A queue that has never been touched
// reports zeroes rather than being created, so scraping metrics does not
// allocate queues for typos.
func (m *Matcher) Stats(key QueueKey) Stats {
	m.mu.RLock()
	q := m.queues[key]
	m.mu.RUnlock()
	if q == nil {
		return Stats{}
	}
	return q.stats()
}

// Queues returns every queue key that currently exists, for metrics scraping.
func (m *Matcher) Queues() []QueueKey {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]QueueKey, 0, len(m.queues))
	for k := range m.queues {
		out = append(out, k)
	}
	return out
}

func (m *Matcher) add(kind Kind, t Task) error {
	select {
	case <-m.closed:
		return ErrClosed
	default:
	}
	if t.TaskQueue == "" {
		return fmt.Errorf("matching: task for %s has no task queue", t.Execution)
	}
	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = m.clk.Now()
	}
	key := QueueKey{Namespace: t.Namespace, TaskQueue: t.TaskQueue, Kind: kind}
	q := m.queueFor(key)

	matched, depth, err := q.offer(t, m.maxBacklog)
	if err != nil {
		m.metrics.TaskDropped(key, DropBacklogFull)
		return err
	}
	m.metrics.TaskAdded(key, matched)
	m.metrics.BacklogDepth(key, depth)
	return nil
}

func (m *Matcher) poll(ctx context.Context, key QueueKey) (Task, bool, error) {
	select {
	case <-m.closed:
		return Task{}, false, ErrClosed
	default:
	}
	if key.TaskQueue == "" {
		return Task{}, false, fmt.Errorf("matching: poll with no task queue")
	}
	q := m.queueFor(key)

	// Fast path: a backlog entry is available right now, so no waiter is ever
	// registered and no timer is ever allocated.
	if t, depth, ok := q.takeBacklog(); ok {
		m.metrics.BacklogDepth(key, depth)
		return t, true, nil
	}

	w, pollers := q.park()
	m.metrics.PollerCount(key, pollers)

	timer := m.clk.NewTimer(m.pollTimeout)
	defer timer.Stop()

	var timedOut bool
	select {
	case t := <-w.ch:
		// Delivered by a sync match. The waiter was already unlinked by the
		// producer, so there is nothing to clean up.
		m.metrics.PollerCount(key, q.pollerCount())
		return t, true, nil
	case <-timer.C():
		timedOut = true
	case <-ctx.Done():
	case <-m.closed:
	}

	// The wake-up above races with a producer that may have picked this waiter
	// a moment earlier. unpark resolves the race under the queue lock and hands
	// back the task if one was delivered, which is what makes "a task is never
	// lost on poll timeout" a property rather than a hope.
	t, delivered, pollers := q.unpark(w)
	m.metrics.PollerCount(key, pollers)
	if delivered {
		return t, true, nil
	}
	if timedOut {
		q.pollTimeouts.Add(1)
	}
	return Task{}, false, nil
}

func (m *Matcher) queueFor(key QueueKey) *queue {
	m.mu.RLock()
	q := m.queues[key]
	m.mu.RUnlock()
	if q != nil {
		return q
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if q = m.queues[key]; q != nil {
		return q
	}
	q = &queue{key: key}
	m.queues[key] = q
	return q
}

// ---------------------------------------------------------------------------
// One queue
// ---------------------------------------------------------------------------

// waiter is one parked poller.
type waiter struct {
	ch chan Task
	// elem is the waiter's position in the FIFO, kept so that removal on
	// timeout is O(1) instead of a scan.
	elem *list.Element
	// settled is true once the waiter has been taken off the FIFO, either by a
	// producer handing it a task or by the poller giving up. It is guarded by
	// the queue mutex, which is what makes the hand-off race resolvable.
	settled bool
}

type queue struct {
	key QueueKey

	mu sync.Mutex
	// backlog is FIFO. Tasks that waited longest are dispatched first, so a
	// burst does not permanently strand its earliest members.
	backlog []Task
	// waiters is FIFO over parked pollers. Serving the poller that has waited
	// longest is the round-robin property: a worker that just received a task
	// re-polls and lands at the back, so a busy worker cannot monopolise a
	// queue and an idle one cannot starve.
	waiters list.List

	syncMatches  atomic.Uint64
	asyncMatches atomic.Uint64
	dropped      atomic.Uint64
	pollTimeouts atomic.Uint64
}

// offer delivers t to a waiting poller if there is one, otherwise appends it to
// the backlog. It reports whether the delivery was a sync match and the backlog
// depth afterwards.
func (q *queue) offer(t Task, maxBacklog int) (sync bool, depth int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for e := q.waiters.Front(); e != nil; e = q.waiters.Front() {
		w := e.Value.(*waiter)
		q.waiters.Remove(e)
		if w.settled {
			// The poller gave up between selecting and taking the lock. Skip it
			// and try the next one rather than dropping the task.
			continue
		}
		w.settled = true
		// The channel is buffered with room for exactly this value, so the send
		// cannot block even though the poller has not woken yet.
		w.ch <- t
		q.syncMatches.Add(1)
		return true, len(q.backlog), nil
	}

	if len(q.backlog) >= maxBacklog {
		q.dropped.Add(1)
		return false, len(q.backlog), fmt.Errorf("%w: %s holds %d tasks", ErrBacklogFull, q.key, len(q.backlog))
	}
	q.backlog = append(q.backlog, t)
	return false, len(q.backlog), nil
}

// takeBacklog pops the oldest queued task, if any.
func (q *queue) takeBacklog() (Task, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.backlog) == 0 {
		return Task{}, 0, false
	}
	t := q.backlog[0]
	q.backlog[0] = Task{}
	q.backlog = q.backlog[1:]
	if len(q.backlog) == 0 {
		// Release the array once it drains so a one-off burst does not pin its
		// peak capacity for the life of the process.
		q.backlog = nil
	}
	q.asyncMatches.Add(1)
	return t, len(q.backlog), true
}

// park registers a poller at the back of the FIFO.
func (q *queue) park() (*waiter, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	w := &waiter{ch: make(chan Task, 1)}
	w.elem = q.waiters.PushBack(w)
	return w, q.waiters.Len()
}

// unpark removes a poller that stopped waiting, resolving the race against a
// producer that may have already handed it a task.
func (q *queue) unpark(w *waiter) (Task, bool, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if w.settled {
		// A producer took this waiter off the FIFO and sent under the same
		// mutex, so if a task was handed over it is already buffered. Returning
		// it is mandatory: the alternative loses a task that no longer exists
		// anywhere else. The receive is non-blocking because drain also settles
		// waiters, without a task, when the matcher shuts down.
		select {
		case t := <-w.ch:
			return t, true, q.waiters.Len()
		default:
			return Task{}, false, q.waiters.Len()
		}
	}
	w.settled = true
	q.waiters.Remove(w.elem)
	return Task{}, false, q.waiters.Len()
}

func (q *queue) pollerCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waiters.Len()
}

// drain discards the backlog and releases every parked poller. It reports how
// many queued tasks were discarded.
func (q *queue) drain() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.backlog)
	q.backlog = nil
	q.dropped.Add(uint64(n))
	for e := q.waiters.Front(); e != nil; e = q.waiters.Front() {
		w := e.Value.(*waiter)
		q.waiters.Remove(e)
		w.settled = true
	}
	return n
}

func (q *queue) stats() Stats {
	q.mu.Lock()
	backlog, pollers := len(q.backlog), q.waiters.Len()
	q.mu.Unlock()
	return Stats{
		Backlog:      backlog,
		Pollers:      pollers,
		SyncMatches:  q.syncMatches.Load(),
		AsyncMatches: q.asyncMatches.Load(),
		Dropped:      q.dropped.Load(),
		PollTimeouts: q.pollTimeouts.Load(),
	}
}
