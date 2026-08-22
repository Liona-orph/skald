package matching_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Liona-orph/skald/internal/clock"
	"github.com/Liona-orph/skald/internal/matching"
	"github.com/Liona-orph/skald/pkg/skald"
)

const (
	ns    = "default"
	queue = "orders"
)

func task(id int64) matching.Task {
	return matching.Task{
		Namespace:        ns,
		TaskQueue:        queue,
		Execution:        skald.WorkflowExecution{WorkflowID: "wf", RunID: "run"},
		ScheduledEventID: id,
		Attempt:          1,
	}
}

func newMatcher(t *testing.T, clk clock.Clock, backlog int) *matching.Matcher {
	t.Helper()
	m := matching.New(matching.Config{Clock: clk, MaxBacklog: backlog, PollTimeout: 30 * time.Second})
	t.Cleanup(m.Close)
	return m
}

// TestSyncMatchBypassesBacklog pins the property that matters most for
// latency: when a poller is already waiting, the task must never be enqueued.
func TestSyncMatchBypassesBacklog(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	m := newMatcher(t, clk, 0)

	polled := make(chan matching.Task, 1)
	go func() {
		got, ok, err := m.PollWorkflowTask(context.Background(), ns, queue)
		if err != nil || !ok {
			t.Errorf("poll returned (%v, %v)", ok, err)
			close(polled)
			return
		}
		polled <- got
	}()

	// Wait until the poller is genuinely parked: it registers its long-poll
	// timer with the virtual clock as its last act before blocking.
	clk.BlockUntil(1)

	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindWorkflow}
	if got := m.Stats(key).Pollers; got != 1 {
		t.Fatalf("Pollers = %d, want 1", got)
	}
	if err := m.AddWorkflowTask(task(7)); err != nil {
		t.Fatalf("AddWorkflowTask: %v", err)
	}

	got := <-polled
	if got.ScheduledEventID != 7 {
		t.Fatalf("delivered event %d, want 7", got.ScheduledEventID)
	}
	st := m.Stats(key)
	if st.SyncMatches != 1 {
		t.Fatalf("SyncMatches = %d, want 1", st.SyncMatches)
	}
	if st.AsyncMatches != 0 {
		t.Fatalf("AsyncMatches = %d, want 0: the task must never touch the backlog", st.AsyncMatches)
	}
	if st.Backlog != 0 {
		t.Fatalf("Backlog = %d, want 0", st.Backlog)
	}
}

func TestAsyncMatchFromBacklog(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	m := newMatcher(t, clk, 0)
	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindActivity}

	for i := int64(1); i <= 3; i++ {
		if err := m.AddActivityTask(task(i)); err != nil {
			t.Fatalf("AddActivityTask: %v", err)
		}
	}
	if got := m.Stats(key).Backlog; got != 3 {
		t.Fatalf("Backlog = %d, want 3", got)
	}

	// FIFO: the reference that waited longest is dispatched first.
	for i := int64(1); i <= 3; i++ {
		got, ok, err := m.PollActivityTask(context.Background(), ns, queue)
		if err != nil || !ok {
			t.Fatalf("poll %d returned (%v, %v)", i, ok, err)
		}
		if got.ScheduledEventID != i {
			t.Fatalf("poll %d delivered event %d", i, got.ScheduledEventID)
		}
	}
	st := m.Stats(key)
	if st.AsyncMatches != 3 || st.SyncMatches != 0 {
		t.Fatalf("stats = %+v, want 3 async and 0 sync matches", st)
	}
}

func TestWorkflowAndActivityQueuesAreSeparate(t *testing.T) {
	t.Parallel()
	m := newMatcher(t, clock.NewVirtual(time.Time{}), 0)

	if err := m.AddWorkflowTask(task(1)); err != nil {
		t.Fatalf("AddWorkflowTask: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, _ := m.PollActivityTask(ctx, ns, queue); ok {
		t.Fatal("an activity poll consumed a workflow task")
	}
	if _, ok, err := m.PollWorkflowTask(context.Background(), ns, queue); err != nil || !ok {
		t.Fatalf("workflow poll returned (%v, %v)", ok, err)
	}
}

// TestPollTimeoutReturnsEmptyNotError pins the wire contract: an idle worker
// gets an empty answer, never an error it would have to classify.
func TestPollTimeoutReturnsEmptyNotError(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	m := matching.New(matching.Config{Clock: clk, PollTimeout: 20 * time.Second})
	t.Cleanup(m.Close)

	type result struct {
		task matching.Task
		ok   bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		task, ok, err := m.PollWorkflowTask(context.Background(), ns, queue)
		done <- result{task, ok, err}
	}()

	clk.BlockUntil(1)
	clk.Advance(19 * time.Second)
	select {
	case r := <-done:
		t.Fatalf("poll returned early: %+v", r)
	case <-time.After(5 * time.Millisecond):
	}

	clk.Advance(time.Second)
	r := <-done
	if r.err != nil {
		t.Fatalf("expired poll returned error %v, want nil", r.err)
	}
	if r.ok {
		t.Fatal("expired poll reported a task")
	}
	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindWorkflow}
	if got := m.Stats(key).PollTimeouts; got != 1 {
		t.Fatalf("PollTimeouts = %d, want 1", got)
	}
	if got := m.Stats(key).Pollers; got != 0 {
		t.Fatalf("Pollers = %d after timeout, want 0", got)
	}
}

func TestPollRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	m := newMatcher(t, clk, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok, err := m.PollActivityTask(ctx, ns, queue); ok || err != nil {
			t.Errorf("cancelled poll returned (%v, %v)", ok, err)
		}
	}()

	clk.BlockUntil(1)
	cancel()
	<-done

	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindActivity}
	if got := m.Stats(key).Pollers; got != 0 {
		t.Fatalf("cancelled poll left %d pollers registered", got)
	}
}

// TestTaskIsNotLostWhenPollTimesOutConcurrently exercises the hand-off race:
// the producer picks a waiter at the same instant the waiter gives up. The
// contract is that exactly one of the two wins and the task is never dropped.
func TestTaskIsNotLostWhenPollTimesOutConcurrently(t *testing.T) {
	t.Parallel()
	for i := 0; i < 200; i++ {
		clk := clock.NewVirtual(time.Time{})
		m := matching.New(matching.Config{Clock: clk, PollTimeout: time.Second})

		delivered := make(chan bool, 1)
		go func() {
			_, ok, _ := m.PollWorkflowTask(context.Background(), ns, queue)
			delivered <- ok
		}()
		clk.BlockUntil(1)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); clk.Advance(time.Second) }()
		go func() { defer wg.Done(); _ = m.AddWorkflowTask(task(1)) }()
		wg.Wait()

		key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindWorkflow}
		if got := <-delivered; !got {
			// The poller gave up first, so the task must be in the backlog.
			if backlog := m.Stats(key).Backlog; backlog != 1 {
				t.Fatalf("iteration %d: poll expired and backlog = %d, want 1: task lost", i, backlog)
			}
		} else if backlog := m.Stats(key).Backlog; backlog != 0 {
			t.Fatalf("iteration %d: task delivered and also queued", i)
		}
		m.Close()
	}
}

func TestBacklogFullRejectsNewest(t *testing.T) {
	t.Parallel()
	m := newMatcher(t, clock.NewVirtual(time.Time{}), 2)

	if err := m.AddActivityTask(task(1)); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := m.AddActivityTask(task(2)); err != nil {
		t.Fatalf("second add: %v", err)
	}
	err := m.AddActivityTask(task(3))
	if !errors.Is(err, matching.ErrBacklogFull) {
		t.Fatalf("third add returned %v, want ErrBacklogFull", err)
	}

	// The oldest entries survive: the policy sheds the newest so that queued
	// work cannot be starved by a flood behind it.
	got, _, _ := m.PollActivityTask(context.Background(), ns, queue)
	if got.ScheduledEventID != 1 {
		t.Fatalf("after rejection the queue head is event %d, want 1", got.ScheduledEventID)
	}
	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindActivity}
	if st := m.Stats(key); st.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", st.Dropped)
	}
}

// TestRoundRobinAcrossPollers proves the anti-starvation property: pollers are
// served in arrival order, so a worker that just took a task goes to the back.
func TestRoundRobinAcrossPollers(t *testing.T) {
	t.Parallel()
	clk := clock.NewVirtual(time.Time{})
	m := newMatcher(t, clk, 0)

	const pollers = 4
	got := make([]chan int64, pollers)
	for i := range got {
		got[i] = make(chan int64, 1)
	}
	for i := 0; i < pollers; i++ {
		i := i
		go func() {
			task, ok, err := m.PollWorkflowTask(context.Background(), ns, queue)
			if err != nil || !ok {
				got[i] <- -1
				return
			}
			got[i] <- task.ScheduledEventID
		}()
		// Park them one at a time so the FIFO order is the loop order.
		clk.BlockUntil(i + 1)
	}

	for i := int64(0); i < pollers; i++ {
		if err := m.AddWorkflowTask(task(i)); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	for i := 0; i < pollers; i++ {
		if id := <-got[i]; id != int64(i) {
			t.Fatalf("poller %d received event %d, want %d: pollers are not served FIFO", i, id, i)
		}
	}
}

// TestConcurrentProducersAndPollers is the -race workhorse: every task must be
// delivered exactly once across many concurrent producers and consumers.
func TestConcurrentProducersAndPollers(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()

	m := matching.New(matching.Config{
		Clock:       clock.System(),
		PollTimeout: 2 * time.Second,
		MaxBacklog:  100_000,
	})

	const (
		producers      = 8
		perProducer    = 250
		pollers        = 16
		expectedTotals = producers * perProducer
	)

	var seen sync.Map
	var duplicates atomic.Int64
	var received atomic.Int64
	done := make(chan struct{})

	var pollWG sync.WaitGroup
	pollWG.Add(pollers)
	for i := 0; i < pollers; i++ {
		go func() {
			defer pollWG.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				task, ok, err := m.PollActivityTask(context.Background(), ns, queue)
				if err != nil {
					return
				}
				if !ok {
					continue
				}
				if _, dup := seen.LoadOrStore(task.ScheduledEventID, true); dup {
					duplicates.Add(1)
				}
				received.Add(1)
			}
		}()
	}

	var addWG sync.WaitGroup
	addWG.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer addWG.Done()
			for i := 0; i < perProducer; i++ {
				id := int64(p*perProducer + i + 1)
				if err := m.AddActivityTask(task(id)); err != nil {
					t.Errorf("add %d: %v", id, err)
					return
				}
			}
		}()
	}
	addWG.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for received.Load() < expectedTotals && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(done)
	m.Close()
	pollWG.Wait()

	if got := received.Load(); got != expectedTotals {
		t.Fatalf("received %d tasks, want %d", got, expectedTotals)
	}
	if got := duplicates.Load(); got != 0 {
		t.Fatalf("%d tasks were delivered more than once", got)
	}
	key := matching.QueueKey{Namespace: ns, TaskQueue: queue, Kind: matching.KindActivity}
	st := m.Stats(key)
	if st.SyncMatches+st.AsyncMatches != expectedTotals {
		t.Fatalf("match counters sum to %d, want %d", st.SyncMatches+st.AsyncMatches, expectedTotals)
	}
	assertNoGoroutineLeak(t, before)
}

func TestCloseReleasesPollers(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()
	clk := clock.NewVirtual(time.Time{})
	m := matching.New(matching.Config{Clock: clk, PollTimeout: time.Hour})

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok, _ := m.PollWorkflowTask(context.Background(), ns, queue); ok {
			t.Error("poll returned a task after close")
		}
	}()
	clk.BlockUntil(1)
	m.Close()
	<-done

	if _, _, err := m.PollWorkflowTask(context.Background(), ns, queue); !errors.Is(err, matching.ErrClosed) {
		t.Fatalf("poll after close returned %v, want ErrClosed", err)
	}
	if err := m.AddWorkflowTask(task(1)); !errors.Is(err, matching.ErrClosed) {
		t.Fatalf("add after close returned %v, want ErrClosed", err)
	}
	// Close is idempotent: the shutdown path is reached from several places.
	m.Close()
	assertNoGoroutineLeak(t, before)
}

func TestMetricsAreReported(t *testing.T) {
	t.Parallel()
	rec := &recordingMetrics{}
	clk := clock.NewVirtual(time.Time{})
	m := matching.New(matching.Config{Clock: clk, Metrics: rec, MaxBacklog: 1})
	t.Cleanup(m.Close)

	if err := m.AddWorkflowTask(task(1)); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := m.AddWorkflowTask(task(2)); !errors.Is(err, matching.ErrBacklogFull) {
		t.Fatalf("second add: %v", err)
	}
	if _, ok, _ := m.PollWorkflowTask(context.Background(), ns, queue); !ok {
		t.Fatal("poll found no task")
	}

	if got := rec.added.Load(); got != 1 {
		t.Fatalf("TaskAdded called %d times, want 1", got)
	}
	if got := rec.dropped.Load(); got != 1 {
		t.Fatalf("TaskDropped called %d times, want 1", got)
	}
	if got := rec.maxDepth.Load(); got != 1 {
		t.Fatalf("peak reported backlog depth = %d, want 1", got)
	}
	if got := rec.depth.Load(); got != 0 {
		t.Fatalf("backlog depth after the poll = %d, want 0", got)
	}
}

func TestAddWithoutTaskQueueIsRejected(t *testing.T) {
	t.Parallel()
	m := newMatcher(t, clock.NewVirtual(time.Time{}), 0)
	bad := task(1)
	bad.TaskQueue = ""
	if err := m.AddWorkflowTask(bad); err == nil {
		t.Fatal("a task with no queue was accepted")
	}
}

type recordingMetrics struct {
	added    atomic.Int64
	dropped  atomic.Int64
	depth    atomic.Int64
	maxDepth atomic.Int64
	pollers  atomic.Int64
}

func (r *recordingMetrics) TaskAdded(matching.QueueKey, bool)     { r.added.Add(1) }
func (r *recordingMetrics) TaskDropped(matching.QueueKey, string) { r.dropped.Add(1) }
func (r *recordingMetrics) BacklogDepth(_ matching.QueueKey, d int) {
	r.depth.Store(int64(d))
	for {
		peak := r.maxDepth.Load()
		if int64(d) <= peak || r.maxDepth.CompareAndSwap(peak, int64(d)) {
			return
		}
	}
}
func (r *recordingMetrics) PollerCount(_ matching.QueueKey, n int) { r.pollers.Store(int64(n)) }

// assertNoGoroutineLeak waits for the goroutine count to return to a baseline.
// It polls rather than sampling once because a goroutine that has been released
// still needs a scheduler turn to actually exit.
func assertNoGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.Gosched()
		got := runtime.NumGoroutine()
		if got <= baseline {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutine leak: %d goroutines, baseline %d\n%s", got, baseline, buf)
		}
		time.Sleep(time.Millisecond)
	}
}
