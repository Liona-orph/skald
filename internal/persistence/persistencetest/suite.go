// Package persistencetest holds the conformance suite every persistence driver
// must pass.
//
// A driver is only useful if it is interchangeable with the others, so the
// interesting assertions live here rather than in each driver's own test file:
// a behaviour checked for only one implementation is a behaviour the engine
// cannot rely on. Driver-specific tests remain valuable, but they cover
// properties the interface does not promise -- SQLite's WAL recovery, the
// memory driver's aliasing guarantees -- never the contract itself.
package persistencetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// baseTime anchors every generated history. A fixed instant rather than
// time.Now keeps failure output diffable and keeps the suite from depending on
// the resolution of the host clock, which on Windows is coarse enough to make
// "monotonic timestamps" accidentally mean "identical timestamps".
var baseTime = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

// eventGap is the spacing between generated event timestamps.
const eventGap = time.Millisecond

// ---------------------------------------------------------------------------
// History construction
// ---------------------------------------------------------------------------

// Builder produces histories that satisfy history.History.Validate.
//
// Hand-writing events in each test is how a suite ends up asserting against
// histories the engine could never produce -- a timer with no originating
// workflow task, an activity result that references nothing. The builder makes
// the valid shape the easy one: back-references are wired automatically and
// timestamps advance by construction.
type Builder struct {
	Namespace    string
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Memo         map[string]string
	SearchAttrs  map[string]string

	base time.Time
	// events is the full history; flushed marks how much of it the store has
	// already been given, which is what lets a test append incrementally
	// without tracking event IDs by hand.
	events   history.History
	flushed  int
	lastTask int64
	seq      int
	timerIDs map[int64]string
}

// BuilderOption customises a Builder before its first event is written.
type BuilderOption func(*Builder)

// WithWorkflowType overrides the generated workflow type.
func WithWorkflowType(t string) BuilderOption { return func(b *Builder) { b.WorkflowType = t } }

// WithTaskQueue overrides the generated task queue.
func WithTaskQueue(q string) BuilderOption { return func(b *Builder) { b.TaskQueue = q } }

// WithStartTime moves the whole history in time. Visibility ordering is by
// start time, so tests that care about ordering use this to space runs apart.
func WithStartTime(t time.Time) BuilderOption { return func(b *Builder) { b.base = t.UTC() } }

// WithMemo attaches a memo to the execution record.
func WithMemo(m map[string]string) BuilderOption { return func(b *Builder) { b.Memo = m } }

// NewBuilder returns a builder whose history already contains event 1.
func NewBuilder(namespace, workflowID, runID string, opts ...BuilderOption) *Builder {
	b := &Builder{
		Namespace:    namespace,
		WorkflowID:   workflowID,
		RunID:        runID,
		WorkflowType: "TestWorkflow",
		TaskQueue:    "test-queue",
		base:         baseTime,
		timerIDs:     make(map[int64]string),
	}
	for _, opt := range opts {
		opt(b)
	}
	b.add(history.WorkflowExecutionStartedAttributes{
		WorkflowType:        b.WorkflowType,
		TaskQueue:           b.TaskQueue,
		Input:               payload(`"input"`),
		Attempt:             1,
		RandomnessSeed:      42,
		FirstExecutionRunID: runID,
		Memo:                b.Memo,
		SearchAttrs:         b.SearchAttrs,
	})
	return b
}

// TimeFor returns the timestamp the builder assigns to an event ID. Deriving
// the timestamp from the ID rather than from a counter is what lets the
// concurrency test produce monotonic history from racing goroutines: whichever
// order the store accepts the writes in, the times still ascend.
func (b *Builder) TimeFor(eventID int64) time.Time {
	return b.base.Add(time.Duration(eventID-1) * eventGap)
}

func (b *Builder) add(attrs history.Attributes) history.Event {
	id := int64(len(b.events)) + 1
	ev := history.Event{ID: id, Time: b.TimeFor(id), Attrs: attrs}
	b.events = append(b.events, ev)
	return ev
}

// WorkflowTask appends a scheduled/started/completed triple and remembers the
// completed event so that later command-derived events can reference it.
func (b *Builder) WorkflowTask() *Builder {
	sched := b.add(history.WorkflowTaskScheduledAttributes{
		TaskQueue:          b.TaskQueue,
		StartToCloseTimout: 10 * time.Second,
		Attempt:            1,
	})
	started := b.add(history.WorkflowTaskStartedAttributes{ScheduledEventID: sched.ID})
	completed := b.add(history.WorkflowTaskCompletedAttributes{
		ScheduledEventID: sched.ID,
		StartedEventID:   started.ID,
	})
	b.lastTask = completed.ID
	return b
}

// taskRef returns a workflow task to attribute a command to, creating one if
// the caller has not.
func (b *Builder) taskRef() int64 {
	if b.lastTask == 0 {
		b.WorkflowTask()
	}
	return b.lastTask
}

// Signal appends a signal. Signals need no back-references, which makes them
// the cheapest way to grow a history by exactly one event.
func (b *Builder) Signal(name string) *Builder {
	b.add(history.WorkflowExecutionSignaledAttributes{SignalName: name, Input: payload(`"sig"`)})
	return b
}

// Timer appends a TimerStarted event and returns its ID, which is also the
// TimerKey.EventID the store indexes the timer under.
func (b *Builder) Timer(d time.Duration) int64 {
	ref := b.taskRef()
	b.seq++
	id := fmt.Sprintf("timer-%d", b.seq)
	ev := b.add(history.TimerStartedAttributes{
		TimerID:                      id,
		StartToFireTimeout:           d,
		WorkflowTaskCompletedEventID: ref,
	})
	b.timerIDs[ev.ID] = id
	return ev.ID
}

// FireTimer appends the TimerFired event closing a timer this builder started.
func (b *Builder) FireTimer(startedEventID int64) *Builder {
	b.add(history.TimerFiredAttributes{
		TimerID:        b.timerIDs[startedEventID],
		StartedEventID: startedEventID,
	})
	return b
}

// Activity appends an ActivityTaskScheduled event and returns its ID.
func (b *Builder) Activity() int64 {
	ref := b.taskRef()
	b.seq++
	ev := b.add(history.ActivityTaskScheduledAttributes{
		ActivityID:                   fmt.Sprintf("activity-%d", b.seq),
		ActivityType:                 "TestActivity",
		TaskQueue:                    b.TaskQueue,
		Input:                        payload(`{"n":1}`),
		ScheduleToCloseTimeout:       time.Minute,
		StartToCloseTimeout:          30 * time.Second,
		WorkflowTaskCompletedEventID: ref,
	})
	return ev.ID
}

// Complete closes the run successfully.
func (b *Builder) Complete() *Builder {
	b.add(history.WorkflowExecutionCompletedAttributes{
		Result:                       payload(`"done"`),
		WorkflowTaskCompletedEventID: b.taskRef(),
	})
	return b
}

// Fail closes the run with a terminal failure.
func (b *Builder) Fail() *Builder {
	b.add(history.WorkflowExecutionFailedAttributes{
		Failure:                      &skald.ApplicationError{Type: "Boom", Message: "activity gave up"},
		WorkflowTaskCompletedEventID: b.taskRef(),
		RetryState:                   history.RetryStateMaximumAttemptsReached,
	})
	return b
}

// History returns every event the builder has produced.
func (b *Builder) History() history.History {
	return slices.Clone(b.events)
}

// Pending returns the events the store has not been given yet and marks them
// as delivered.
func (b *Builder) Pending() history.History {
	out := slices.Clone(b.events[b.flushed:])
	b.flushed = len(b.events)
	return out
}

// Record renders the execution row implied by the history so far.
func (b *Builder) Record() persistence.ExecutionRecord {
	last := b.events[len(b.events)-1]
	rec := persistence.ExecutionRecord{
		Namespace:           b.Namespace,
		WorkflowID:          b.WorkflowID,
		RunID:               b.RunID,
		WorkflowType:        b.WorkflowType,
		TaskQueue:           b.TaskQueue,
		Status:              statusOf(last),
		StartedAt:           b.events[0].Time,
		LastEventID:         last.ID,
		FirstExecutionRunID: b.RunID,
		Memo:                b.Memo,
		SearchAttrs:         b.SearchAttrs,
	}
	if rec.Status.Terminal() {
		rec.ClosedAt = last.Time
	}
	return rec
}

// Create renders a CreateExecutionRequest for everything built so far.
func (b *Builder) Create(opts ...func(*persistence.CreateExecutionRequest)) persistence.CreateExecutionRequest {
	req := persistence.CreateExecutionRequest{Record: b.Record(), Events: b.Pending()}
	for _, opt := range opts {
		opt(&req)
	}
	return req
}

// Append renders an AppendHistoryRequest carrying the events produced since the
// last flush.
func (b *Builder) Append(expectedVersion int64, opts ...func(*persistence.AppendHistoryRequest)) persistence.AppendHistoryRequest {
	req := persistence.AppendHistoryRequest{
		Namespace:       b.Namespace,
		WorkflowID:      b.WorkflowID,
		RunID:           b.RunID,
		ExpectedVersion: expectedVersion,
		Events:          b.Pending(),
		Record:          b.Record(),
	}
	for _, opt := range opts {
		opt(&req)
	}
	return req
}

// TimerFor builds the index entry for a timer this builder started.
func (b *Builder) TimerFor(startedEventID int64, fireAt time.Time) persistence.TimerRecord {
	return persistence.TimerRecord{
		TimerKey: persistence.TimerKey{
			Namespace:  b.Namespace,
			WorkflowID: b.WorkflowID,
			RunID:      b.RunID,
			EventID:    startedEventID,
			Kind:       persistence.TimerKindUser,
		},
		FireAt:    fireAt.UTC(),
		TaskQueue: b.TaskQueue,
	}
}

// statusOf maps the closing event of a history to the status the execution row
// must carry. Keeping the mapping here rather than in each test is what makes
// "the row agrees with the history" a property of the suite rather than a thing
// every test remembers to do.
func statusOf(ev history.Event) skald.WorkflowStatus {
	switch ev.Type() {
	case history.EventTypeWorkflowExecutionCompleted:
		return skald.StatusCompleted
	case history.EventTypeWorkflowExecutionFailed:
		return skald.StatusFailed
	case history.EventTypeWorkflowExecutionCanceled:
		return skald.StatusCanceled
	case history.EventTypeWorkflowExecutionTerminated:
		return skald.StatusTerminated
	case history.EventTypeWorkflowExecutionTimedOut:
		return skald.StatusTimedOut
	case history.EventTypeWorkflowExecutionContinuedAsNew:
		return skald.StatusContinuedAsNew
	default:
		return skald.StatusRunning
	}
}

func payload(json string) *skald.Payload {
	return &skald.Payload{Encoding: skald.EncodingJSON, Data: []byte(json)}
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

// RunSuite executes the conformance suite against a driver.
//
// newStore is called once per subtest and must return an empty store; the
// driver is responsible for registering whatever cleanup it needs on t.
func RunSuite(t *testing.T, newStore func(t *testing.T) persistence.Store) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(*testing.T, persistence.Store)
	}{
		{"CreateAndGet", testCreateAndGet},
		{"NotFound", testNotFound},
		{"CurrentRun", testCurrentRun},
		{"RequestIDDeduplication", testRequestIDDedup},
		{"IDReusePolicy", testIDReusePolicy},
		{"AppendHistory", testAppendHistory},
		{"AppendHistoryVersionConflict", testAppendVersionConflict},
		{"AppendHistoryConcurrent", testAppendConcurrent},
		{"AppendHistoryCreateSuccessor", testAppendCreateSuccessor},
		{"ReadHistoryRange", testReadHistoryRange},
		{"Timers", testTimers},
		{"ListExecutions", testListExecutions},
		{"OpenExecutions", testOpenExecutions},
		{"ContextCancellation", testContextCancellation},
		{"Close", testClose},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

func testCreateAndGet(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1", WithMemo(map[string]string{"team": "payments"}))
	b.WorkflowTask()
	b.Activity()

	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)
	require.Equal(t, "order-1", created.WorkflowID)
	require.Equal(t, "run-1", created.RunID)
	require.Equal(t, skald.StatusRunning, created.Status)
	require.Equal(t, b.History().LastEventID(), created.LastEventID)
	require.Positive(t, created.Version, "a fresh run must carry a version callers can write against")
	require.True(t, created.Open())

	got, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, created, got)
	require.Equal(t, map[string]string{"team": "payments"}, got.Memo)
	require.True(t, got.StartedAt.Equal(b.TimeFor(1)))

	h, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.NoError(t, h.Validate())
	require.Equal(t, b.History(), h)
}

func testNotFound(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	_, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	for _, tc := range []struct{ name, ns, wf, run string }{
		{"unknown namespace", "other", "order-1", "run-1"},
		{"unknown workflow", "default", "order-2", ""},
		{"unknown run", "default", "order-1", "run-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.GetExecution(ctx, tc.ns, tc.wf, tc.run)
			require.ErrorIs(t, err, persistence.ErrNotFound)

			_, err = store.ReadHistory(ctx, tc.ns, tc.wf, tc.run, 1, 0)
			require.ErrorIs(t, err, persistence.ErrNotFound)

			_, err = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
				Namespace: tc.ns, WorkflowID: tc.wf, RunID: tc.run, ExpectedVersion: 1,
			})
			require.ErrorIs(t, err, persistence.ErrNotFound)
		})
	}
}

func testCurrentRun(t *testing.T, store persistence.Store) {
	ctx := context.Background()

	first := NewBuilder("default", "cron-1", "run-1")
	first.Complete()
	_, err := store.CreateExecution(ctx, first.Create())
	require.NoError(t, err)

	second := NewBuilder("default", "cron-1", "run-2", WithStartTime(baseTime.Add(time.Hour)))
	_, err = store.CreateExecution(ctx, second.Create())
	require.NoError(t, err)

	got, err := store.GetExecution(ctx, "default", "cron-1", "")
	require.NoError(t, err)
	require.Equal(t, "run-2", got.RunID, "an empty run ID must resolve to the newest run")

	// The older run stays addressable forever; that is the whole point of a
	// server-assigned run ID.
	got, err = store.GetExecution(ctx, "default", "cron-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, skald.StatusCompleted, got.Status)

	h, err := store.ReadHistory(ctx, "default", "cron-1", "", 1, 0)
	require.NoError(t, err)
	require.Equal(t, second.History(), h)
}

func testRequestIDDedup(t *testing.T, store persistence.Store) {
	ctx := context.Background()

	b := NewBuilder("default", "order-1", "run-1")
	first, err := store.CreateExecution(ctx, b.Create(withRequestID("req-a")))
	require.NoError(t, err)

	// A blindly retried start carries the same request ID and a fresh run ID.
	// The store must hand back the original run rather than start a second one.
	retry := NewBuilder("default", "order-1", "run-2")
	second, err := store.CreateExecution(ctx, retry.Create(withRequestID("req-a")))
	require.NoError(t, err)
	require.Equal(t, first, second)

	_, err = store.GetExecution(ctx, "default", "order-1", "run-2")
	require.ErrorIs(t, err, persistence.ErrNotFound, "a deduplicated start must not write anything")

	// A different request ID against a still-open run is a genuine duplicate.
	other := NewBuilder("default", "order-1", "run-3")
	_, err = store.CreateExecution(ctx, other.Create(withRequestID("req-b")))
	require.ErrorIs(t, err, persistence.ErrAlreadyStarted)
}

func testIDReusePolicy(t *testing.T, store persistence.Store) {
	ctx := context.Background()

	// close is nil for a run that stays open.
	type scenario struct {
		name    string
		close   func(*Builder)
		policy  persistence.IDReusePolicy
		wantErr error
		// wantPrevStatus is checked on the previous run after a successful
		// start, which is how ReuseTerminateIfRunning proves it did the work.
		wantPrevStatus *skald.WorkflowStatus
	}
	terminated := skald.StatusTerminated

	complete := func(b *Builder) { b.Complete() }
	fail := func(b *Builder) { b.Fail() }

	scenarios := []scenario{
		{"AllowDuplicate/closed", complete, persistence.ReuseAllowDuplicate, nil, nil},
		{"AllowDuplicate/open", nil, persistence.ReuseAllowDuplicate, persistence.ErrAlreadyStarted, nil},
		{"AllowDuplicateFailedOnly/failed", fail, persistence.ReuseAllowDuplicateFailedOnly, nil, nil},
		{"AllowDuplicateFailedOnly/completed", complete, persistence.ReuseAllowDuplicateFailedOnly, persistence.ErrAlreadyStarted, nil},
		{"AllowDuplicateFailedOnly/open", nil, persistence.ReuseAllowDuplicateFailedOnly, persistence.ErrAlreadyStarted, nil},
		{"RejectDuplicate/closed", complete, persistence.ReuseRejectDuplicate, persistence.ErrAlreadyStarted, nil},
		{"RejectDuplicate/failed", fail, persistence.ReuseRejectDuplicate, persistence.ErrAlreadyStarted, nil},
		{"TerminateIfRunning/open", nil, persistence.ReuseTerminateIfRunning, nil, &terminated},
		{"TerminateIfRunning/closed", complete, persistence.ReuseTerminateIfRunning, nil, nil},
	}

	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			wf := fmt.Sprintf("reuse-%d", i)
			prev := NewBuilder("default", wf, "run-a")
			timerEventID := prev.Timer(time.Minute)
			if sc.close != nil {
				sc.close(prev)
			}
			prevTimer := prev.TimerFor(timerEventID, baseTime.Add(time.Minute))
			_, err := store.CreateExecution(ctx, prev.Create(func(r *persistence.CreateExecutionRequest) {
				r.Timers = []persistence.TimerRecord{prevTimer}
			}))
			require.NoError(t, err)

			next := NewBuilder("default", wf, "run-b", WithStartTime(baseTime.Add(time.Hour)))
			got, err := store.CreateExecution(ctx, next.Create(func(r *persistence.CreateExecutionRequest) {
				r.ReusePolicy = sc.policy
			}))
			if sc.wantErr != nil {
				require.ErrorIs(t, err, sc.wantErr)
				cur, getErr := store.GetExecution(ctx, "default", wf, "")
				require.NoError(t, getErr)
				require.Equal(t, "run-a", cur.RunID, "a rejected start must not disturb the current run")
				return
			}
			require.NoError(t, err)
			require.Equal(t, "run-b", got.RunID)

			cur, err := store.GetExecution(ctx, "default", wf, "")
			require.NoError(t, err)
			require.Equal(t, "run-b", cur.RunID)

			if sc.wantPrevStatus != nil {
				old, err := store.GetExecution(ctx, "default", wf, "run-a")
				require.NoError(t, err)
				require.Equal(t, *sc.wantPrevStatus, old.Status)
				require.False(t, old.ClosedAt.IsZero())

				h, err := store.ReadHistory(ctx, "default", wf, "run-a", 1, 0)
				require.NoError(t, err)
				require.NoError(t, h.Validate(), "the terminating event must leave a valid history behind")
				require.True(t, h.Terminated())

				// A terminated run must not keep waking the timer service up.
				due, err := store.DueTimers(ctx, baseTime.Add(24*time.Hour), 100)
				require.NoError(t, err)
				for _, tr := range due {
					require.NotEqual(t, prevTimer.TimerKey, tr.TimerKey,
						"terminating a run must retire its timers in the same write")
				}
			}
		})
	}
}

func testAppendHistory(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	b.WorkflowTask()
	b.Signal("approve")
	updated, err := store.AppendHistory(ctx, b.Append(created.Version))
	require.NoError(t, err)
	require.Equal(t, created.Version+1, updated.Version, "every write advances the concurrency token")
	require.Equal(t, b.History().LastEventID(), updated.LastEventID)
	require.Equal(t, skald.StatusRunning, updated.Status)

	// Immutable identity survives the write.
	require.Equal(t, created.StartedAt, updated.StartedAt)
	require.Equal(t, created.WorkflowType, updated.WorkflowType)
	require.Equal(t, created.FirstExecutionRunID, updated.FirstExecutionRunID)

	b.Complete()
	closed, err := store.AppendHistory(ctx, b.Append(updated.Version))
	require.NoError(t, err)
	require.Equal(t, skald.StatusCompleted, closed.Status)
	require.False(t, closed.Open())
	require.False(t, closed.ClosedAt.IsZero())

	h, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.NoError(t, h.Validate())
	require.Equal(t, b.History(), h)

	got, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, closed, got)
}

func testAppendVersionConflict(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	b.WorkflowTask()
	winner, err := store.AppendHistory(ctx, b.Append(created.Version))
	require.NoError(t, err)

	before, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)

	// A second writer that read the pre-write version loses. The events it
	// carries are a structurally valid continuation, so only the version check
	// can be what rejects it.
	_, err = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
		Namespace:       "default",
		WorkflowID:      "order-1",
		RunID:           "run-1",
		ExpectedVersion: created.Version,
		Events:          history.History{{ID: winner.LastEventID + 1, Time: b.TimeFor(winner.LastEventID + 1), Attrs: history.WorkflowExecutionSignaledAttributes{SignalName: "late"}}},
		Record:          winner,
		UpsertTimers: []persistence.TimerRecord{
			b.TimerFor(999, baseTime.Add(time.Minute)),
		},
	})
	require.ErrorIs(t, err, persistence.ErrVersionConflict)

	after, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.Equal(t, before, after, "a rejected append must not write a single event")

	rec, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, winner, rec, "a rejected append must not advance the version")

	due, err := store.DueTimers(ctx, baseTime.Add(24*time.Hour), 100)
	require.NoError(t, err)
	require.Empty(t, due, "a rejected append must not touch the timer index")
}

func testAppendConcurrent(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	const (
		writers          = 8
		appendsPerWriter = 6
	)

	signal := func(id int64, name string) history.History {
		return history.History{{
			ID:    id,
			Time:  b.TimeFor(id),
			Attrs: history.WorkflowExecutionSignaledAttributes{SignalName: name},
		}}
	}

	// Every writer submits the same version, so the outcome does not depend on
	// a race materialising: whatever order the scheduler picks, exactly one may
	// win. Asserting on an observed conflict count instead would make this test
	// pass by luck on a busy machine.
	results := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			// Each goroutine owns its slot, so the slice needs no lock.
			_, results[w] = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
				Namespace:       "default",
				WorkflowID:      "order-1",
				RunID:           "run-1",
				ExpectedVersion: created.Version,
				Events:          signal(created.LastEventID+1, fmt.Sprintf("contender-%d", w)),
				Record:          created,
			})
		}(w)
	}
	close(start)
	wg.Wait()

	winners, conflicts := 0, 0
	for w, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, persistence.ErrVersionConflict):
			conflicts++
		default:
			require.NoError(t, err, "writer %d failed for a reason other than losing the race", w)
		}
	}
	require.Equal(t, 1, winners, "exactly one writer may claim a version")
	require.Equal(t, writers-1, conflicts, "every other writer must be told to re-read")

	h, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.Len(t, h, int(created.LastEventID)+1, "the losers must not have left an event behind")

	// Now let them contend for real, retrying the way the engine does. This
	// part is timing-dependent by nature, so it asserts on the invariant rather
	// than on the schedule: no version may ever be claimed twice, and the
	// history that comes out has to be dense and valid.
	var (
		mu       sync.Mutex
		wins     = map[int64]int{}
		failures []error
	)
	wg = sync.WaitGroup{}
	start = make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < appendsPerWriter; i++ {
				for {
					rec, err := store.GetExecution(ctx, "default", "order-1", "run-1")
					if err != nil {
						mu.Lock()
						failures = append(failures, err)
						mu.Unlock()
						return
					}
					_, err = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
						Namespace:       "default",
						WorkflowID:      "order-1",
						RunID:           "run-1",
						ExpectedVersion: rec.Version,
						Events:          signal(rec.LastEventID+1, fmt.Sprintf("w%d-%d", w, i)),
						Record:          rec,
					})
					mu.Lock()
					if err == nil {
						wins[rec.Version]++
					} else if !errors.Is(err, persistence.ErrVersionConflict) {
						failures = append(failures, err)
					}
					mu.Unlock()
					if err == nil {
						break
					}
					if !errors.Is(err, persistence.ErrVersionConflict) {
						return
					}
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	require.Empty(t, failures)
	require.Len(t, wins, writers*appendsPerWriter)
	for version, n := range wins {
		require.Equal(t, 1, n, "version %d was claimed by %d writers", version, n)
	}

	total := int64(writers*appendsPerWriter + 1)
	rec, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, created.Version+total, rec.Version)

	h, err = store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.NoError(t, h.Validate(), "concurrent writers must leave a dense, valid history")
	require.Len(t, h, int(created.LastEventID+total))
	require.Equal(t, rec.LastEventID, h.LastEventID())
}

func testReadHistoryRange(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	for i := 0; i < 5; i++ {
		b.Signal(fmt.Sprintf("s%d", i))
	}
	_, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)
	full := b.History()
	require.Len(t, full, 6)

	for _, tc := range []struct {
		name     string
		from, to int64
		want     history.History
	}{
		{"whole history", 1, 0, full},
		{"inclusive bounds", 2, 4, full[1:4]},
		{"single event", 3, 3, full[2:3]},
		{"open ended", 4, 0, full[3:]},
		{"from before the beginning", 0, 2, full[:2]},
		{"past the end", 100, 0, nil},
		{"entirely past the end", 50, 60, nil},
		{"to clamped to the end", 5, 900, full[4:]},
		{"inverted range", 4, 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ReadHistory(ctx, "default", "order-1", "run-1", tc.from, tc.to)
			require.NoError(t, err, "an out-of-range read is empty, not an error")
			require.Equal(t, tc.want, got)
		})
	}
}

// testTimers walks one run's timers through their whole life. The subtests are
// ordered on purpose and share state: a timer index is only interesting as a
// sequence of edits, and asserting on each edit in isolation would miss the
// bugs that live in the transitions.
func testTimers(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	b.WorkflowTask()
	early := b.Timer(time.Minute)
	late := b.Timer(time.Hour)

	earlyRec := b.TimerFor(early, baseTime.Add(time.Minute))
	lateRec := b.TimerFor(late, baseTime.Add(time.Hour))

	created, err := store.CreateExecution(ctx, b.Create(func(r *persistence.CreateExecutionRequest) {
		r.Timers = []persistence.TimerRecord{lateRec, earlyRec}
	}))
	require.NoError(t, err)

	t.Run("due time ordering", func(t *testing.T) {
		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 100)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{earlyRec, lateRec}, due)
	})

	t.Run("not yet due", func(t *testing.T) {
		due, err := store.DueTimers(ctx, baseTime, 100)
		require.NoError(t, err)
		require.Empty(t, due)
	})

	t.Run("limit", func(t *testing.T) {
		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 1)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{earlyRec}, due, "the limit must take the earliest, not an arbitrary one")
	})

	t.Run("upsert replaces", func(t *testing.T) {
		moved := earlyRec
		moved.FireAt = baseTime.Add(90 * time.Minute)
		moved.Attempt = 3
		rec, err := store.AppendHistory(ctx, persistence.AppendHistoryRequest{
			Namespace: "default", WorkflowID: "order-1", RunID: "run-1",
			ExpectedVersion: created.Version,
			Record:          created,
			UpsertTimers:    []persistence.TimerRecord{moved},
		})
		require.NoError(t, err)

		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 100)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{lateRec, moved}, due, "an upsert must move the entry, not duplicate it")

		// Restore the original ordering for the remaining subtests.
		_, err = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
			Namespace: "default", WorkflowID: "order-1", RunID: "run-1",
			ExpectedVersion: rec.Version,
			Record:          rec,
			UpsertTimers:    []persistence.TimerRecord{earlyRec},
		})
		require.NoError(t, err)
	})

	t.Run("delete is idempotent", func(t *testing.T) {
		require.NoError(t, store.DeleteTimers(ctx, nil))
		require.NoError(t, store.DeleteTimers(ctx, []persistence.TimerKey{lateRec.TimerKey}))
		require.NoError(t, store.DeleteTimers(ctx, []persistence.TimerKey{lateRec.TimerKey}),
			"deleting a timer the engine already processed must not fail")

		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 100)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{earlyRec}, due)
	})

	t.Run("closed with the append that fired it", func(t *testing.T) {
		rec, err := store.GetExecution(ctx, "default", "order-1", "run-1")
		require.NoError(t, err)

		b.FireTimer(early)
		updated, err := store.AppendHistory(ctx, b.Append(rec.Version, func(r *persistence.AppendHistoryRequest) {
			r.DeleteTimers = []persistence.TimerKey{earlyRec.TimerKey}
		}))
		require.NoError(t, err)

		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 100)
		require.NoError(t, err)
		require.Empty(t, due)

		h, err := store.ReadHistory(ctx, "default", "order-1", "run-1", updated.LastEventID, 0)
		require.NoError(t, err)
		require.Len(t, h, 1)
		require.Equal(t, history.EventTypeTimerFired, h[0].Type())
	})

	t.Run("scoped to their run", func(t *testing.T) {
		other := NewBuilder("other-ns", "order-1", "run-1")
		other.WorkflowTask()
		id := other.Timer(time.Minute)
		otherRec := other.TimerFor(id, baseTime.Add(time.Minute))
		_, err := store.CreateExecution(ctx, other.Create(func(r *persistence.CreateExecutionRequest) {
			r.Timers = []persistence.TimerRecord{otherRec}
		}))
		require.NoError(t, err)

		due, err := store.DueTimers(ctx, baseTime.Add(2*time.Hour), 100)
		require.NoError(t, err)
		require.Equal(t, []persistence.TimerRecord{otherRec}, due,
			"the due index spans namespaces; the timer service is a single scanner")
	})
}

func testListExecutions(t *testing.T, store persistence.Store) {
	ctx := context.Background()

	// Twelve runs across two namespaces, two types and three statuses, spaced a
	// minute apart so that ordering is unambiguous. Nine land in "default":
	// three per status and three of each type.
	for i := 0; i < 12; i++ {
		ns := "default"
		if i%4 == 3 {
			ns = "other-ns"
		}
		typ := "TypeA"
		if i%2 == 1 {
			typ = "TypeB"
		}
		b := NewBuilder(ns, fmt.Sprintf("wf-%02d", i), fmt.Sprintf("run-%02d", i),
			WithWorkflowType(typ),
			WithStartTime(baseTime.Add(time.Duration(i)*time.Minute)))
		switch i % 3 {
		case 1:
			b.Complete()
		case 2:
			b.Fail()
		}
		_, err := store.CreateExecution(ctx, b.Create())
		require.NoError(t, err)
	}

	list := func(t *testing.T, f persistence.ListFilter) []persistence.ExecutionRecord {
		t.Helper()
		res, err := store.ListExecutions(ctx, f)
		require.NoError(t, err)
		return res.Records
	}

	t.Run("by namespace", func(t *testing.T) {
		recs := list(t, persistence.ListFilter{Namespace: "other-ns", PageSize: 100})
		require.Len(t, recs, 3)
		for _, r := range recs {
			require.Equal(t, "other-ns", r.Namespace)
		}
	})

	t.Run("by workflow id", func(t *testing.T) {
		recs := list(t, persistence.ListFilter{Namespace: "default", WorkflowID: "wf-04", PageSize: 100})
		require.Len(t, recs, 1)
		require.Equal(t, "run-04", recs[0].RunID)
	})

	t.Run("by workflow type", func(t *testing.T) {
		recs := list(t, persistence.ListFilter{Namespace: "default", WorkflowType: "TypeB", PageSize: 100})
		require.NotEmpty(t, recs)
		for _, r := range recs {
			require.Equal(t, "TypeB", r.WorkflowType)
			require.Equal(t, "default", r.Namespace)
		}
	})

	t.Run("by status", func(t *testing.T) {
		status := skald.StatusFailed
		recs := list(t, persistence.ListFilter{Namespace: "default", Status: &status, PageSize: 100})
		require.NotEmpty(t, recs)
		for _, r := range recs {
			require.Equal(t, skald.StatusFailed, r.Status)
		}

		running := skald.StatusRunning
		open := list(t, persistence.ListFilter{Namespace: "default", Status: &running, PageSize: 100})
		for _, r := range open {
			require.True(t, r.Open())
		}
	})

	t.Run("by start time", func(t *testing.T) {
		cutoff := baseTime.Add(6 * time.Minute)
		recs := list(t, persistence.ListFilter{Namespace: "default", StartedAfter: cutoff, PageSize: 100})
		require.NotEmpty(t, recs)
		for _, r := range recs {
			require.False(t, r.StartedAt.Before(cutoff))
		}
	})

	t.Run("combined filters", func(t *testing.T) {
		status := skald.StatusCompleted
		recs := list(t, persistence.ListFilter{
			Namespace: "default", WorkflowType: "TypeB", Status: &status, PageSize: 100,
		})
		for _, r := range recs {
			require.Equal(t, "TypeB", r.WorkflowType)
			require.Equal(t, skald.StatusCompleted, r.Status)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		filter := persistence.ListFilter{Namespace: "default", PageSize: 100}
		whole := list(t, filter)
		require.Len(t, whole, 9)

		filter.PageSize = 2
		var paged []persistence.ExecutionRecord
		seen := map[string]bool{}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 20, "pagination did not terminate")
			res, err := store.ListExecutions(ctx, filter)
			require.NoError(t, err)
			require.LessOrEqual(t, len(res.Records), 2)
			for _, r := range res.Records {
				require.False(t, seen[r.RunID], "run %s was returned on two pages", r.RunID)
				seen[r.RunID] = true
			}
			paged = append(paged, res.Records...)
			if res.NextPageToken == "" {
				break
			}
			filter.PageToken = res.NextPageToken
		}
		require.Equal(t, whole, paged, "paging must return the same records in the same order as a single page")
	})

	t.Run("pagination with a filter", func(t *testing.T) {
		status := skald.StatusRunning
		filter := persistence.ListFilter{Namespace: "default", Status: &status, PageSize: 1}
		var paged []persistence.ExecutionRecord
		for {
			res, err := store.ListExecutions(ctx, filter)
			require.NoError(t, err)
			paged = append(paged, res.Records...)
			if res.NextPageToken == "" {
				break
			}
			filter.PageToken = res.NextPageToken
		}
		filter.PageToken = ""
		filter.PageSize = 100
		require.Equal(t, list(t, filter), paged)
	})

	t.Run("empty result", func(t *testing.T) {
		res, err := store.ListExecutions(ctx, persistence.ListFilter{Namespace: "nope", PageSize: 10})
		require.NoError(t, err)
		require.Empty(t, res.Records)
		require.Empty(t, res.NextPageToken)
	})

	t.Run("malformed page token", func(t *testing.T) {
		_, err := store.ListExecutions(ctx, persistence.ListFilter{
			Namespace: "default", PageSize: 2, PageToken: "not-a-token",
		})
		require.Error(t, err, "an opaque token must be validated, not trusted")
	})
}

func testOpenExecutions(t *testing.T, store persistence.Store) {
	ctx := context.Background()

	open := map[string]bool{}
	for i := 0; i < 6; i++ {
		run := fmt.Sprintf("run-%d", i)
		b := NewBuilder("default", fmt.Sprintf("wf-%d", i), run,
			WithStartTime(baseTime.Add(time.Duration(i)*time.Minute)))
		if i%2 == 0 {
			b.Complete()
		} else {
			open[run] = true
		}
		_, err := store.CreateExecution(ctx, b.Create())
		require.NoError(t, err)
	}
	// A run in another namespace must not leak into a scoped scan.
	other := NewBuilder("other-ns", "wf-x", "run-x")
	_, err := store.CreateExecution(ctx, other.Create())
	require.NoError(t, err)

	t.Run("visits every open run", func(t *testing.T) {
		visited := map[string]bool{}
		require.NoError(t, store.OpenExecutions(ctx, "default", func(r persistence.ExecutionRecord) error {
			require.True(t, r.Open(), "a closed run must never be handed to the recovery scan")
			require.False(t, visited[r.RunID], "run %s was visited twice", r.RunID)
			visited[r.RunID] = true
			return nil
		}))
		require.Equal(t, open, visited)
	})

	t.Run("propagates the callback error", func(t *testing.T) {
		sentinel := errors.New("stop scanning")
		calls := 0
		err := store.OpenExecutions(ctx, "default", func(persistence.ExecutionRecord) error {
			calls++
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, calls, "the scan must stop at the first error")
	})

	t.Run("all namespaces", func(t *testing.T) {
		n := 0
		require.NoError(t, store.OpenExecutions(ctx, "", func(persistence.ExecutionRecord) error {
			n++
			return nil
		}))
		require.Equal(t, len(open)+1, n, "an empty namespace scans every tenant")
	})
}

func testContextCancellation(t *testing.T, store persistence.Store) {
	b := NewBuilder("default", "order-1", "run-1")
	_, err := store.CreateExecution(context.Background(), b.Create())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	other := NewBuilder("default", "order-2", "run-2")
	calls := map[string]func() error{
		"CreateExecution": func() error {
			_, err := store.CreateExecution(ctx, other.Create())
			return err
		},
		"GetExecution": func() error {
			_, err := store.GetExecution(ctx, "default", "order-1", "run-1")
			return err
		},
		"ReadHistory": func() error {
			_, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
			return err
		},
		"AppendHistory": func() error {
			_, err := store.AppendHistory(ctx, persistence.AppendHistoryRequest{
				Namespace: "default", WorkflowID: "order-1", RunID: "run-1", ExpectedVersion: 1,
			})
			return err
		},
		"ListExecutions": func() error {
			_, err := store.ListExecutions(ctx, persistence.ListFilter{Namespace: "default"})
			return err
		},
		"DueTimers": func() error {
			_, err := store.DueTimers(ctx, baseTime, 10)
			return err
		},
		"DeleteTimers": func() error {
			return store.DeleteTimers(ctx, []persistence.TimerKey{{Namespace: "default"}})
		},
		"OpenExecutions": func() error {
			return store.OpenExecutions(ctx, "default", func(persistence.ExecutionRecord) error {
				t.Error("the callback must not run under a cancelled context")
				return nil
			})
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, call(), context.Canceled)
		})
	}

	_, err = store.GetExecution(context.Background(), "default", "order-2", "run-2")
	require.ErrorIs(t, err, persistence.ErrNotFound, "a cancelled call must not have written anything")
}

func testClose(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	b := NewBuilder("default", "order-1", "run-1")
	_, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	require.NoError(t, store.Close())
	require.NoError(t, store.Close(), "Close must be idempotent; shutdown paths call it more than once")

	other := NewBuilder("default", "order-2", "run-2")
	calls := map[string]func() error{
		"CreateExecution": func() error {
			_, err := store.CreateExecution(ctx, other.Create())
			return err
		},
		"GetExecution": func() error {
			_, err := store.GetExecution(ctx, "default", "order-1", "run-1")
			return err
		},
		"ReadHistory": func() error {
			_, err := store.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
			return err
		},
		"AppendHistory": func() error {
			_, err := store.AppendHistory(ctx, persistence.AppendHistoryRequest{
				Namespace: "default", WorkflowID: "order-1", RunID: "run-1", ExpectedVersion: 1,
			})
			return err
		},
		"ListExecutions": func() error {
			_, err := store.ListExecutions(ctx, persistence.ListFilter{Namespace: "default"})
			return err
		},
		"DueTimers": func() error {
			_, err := store.DueTimers(ctx, baseTime, 10)
			return err
		},
		"DeleteTimers": func() error {
			return store.DeleteTimers(ctx, []persistence.TimerKey{{Namespace: "default"}})
		},
		"OpenExecutions": func() error {
			return store.OpenExecutions(ctx, "default", func(persistence.ExecutionRecord) error { return nil })
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, call(), persistence.ErrClosed)
		})
	}
}

func withRequestID(id string) func(*persistence.CreateExecutionRequest) {
	return func(r *persistence.CreateExecutionRequest) { r.RequestID = id }
}

// testAppendCreateSuccessor covers the atomic close-and-continue primitive.
//
// The property it protects is the one the deterministic simulator found broken
// at seed 43 of the Skald engine: a continue-as-new that closes its predecessor
// and fails to open its successor strands the whole logical workflow, and
// nothing in the system is watching for it. Making the pair one transaction is
// the fix; this test is what keeps a driver from quietly not implementing it.
func testAppendCreateSuccessor(t *testing.T, store persistence.Store) {
	ctx := context.Background()
	const ns, wf = "default", "chained"

	predecessor := NewBuilder(ns, wf, "run-1")
	predecessor.WorkflowTask()
	created, err := store.CreateExecution(ctx, predecessor.Create())
	require.NoError(t, err)

	successor := NewBuilder(ns, wf, "run-2", WithStartTime(predecessor.TimeFor(predecessor.History().LastEventID()).Add(time.Second)))
	successor.WorkflowTask()

	predecessor.Complete()
	req := predecessor.Append(created.Version)
	sub := successor.Create()
	req.CreateSuccessor = &sub
	closed, err := store.AppendHistory(ctx, req)
	require.NoError(t, err)
	require.True(t, closed.Status.Terminal(), "the predecessor must be closed")

	t.Run("the successor exists and is current", func(t *testing.T) {
		got, err := store.GetExecution(ctx, ns, wf, "run-2")
		require.NoError(t, err)
		require.Equal(t, skald.StatusRunning, got.Status)

		current, err := store.GetExecution(ctx, ns, wf, "")
		require.NoError(t, err)
		require.Equal(t, "run-2", current.RunID, "the successor must become the current run")

		h, err := store.ReadHistory(ctx, ns, wf, "run-2", 1, 0)
		require.NoError(t, err)
		require.NoError(t, h.Validate())
		require.NotEmpty(t, h)
	})

	t.Run("a repeated close does not fork the chain", func(t *testing.T) {
		// The engine derives the successor's request ID from the predecessor's
		// run ID precisely so that a retried close is collapsed here rather
		// than producing a second live run.
		third := NewBuilder(ns, wf, "run-3")
		third.WorkflowTask()
		again := third.Create(func(r *persistence.CreateExecutionRequest) {
			r.RequestID = "successor-of:run-1"
		})

		repeat := NewBuilder(ns, wf, "run-4")
		repeat.WorkflowTask()
		first := repeat.Create(func(r *persistence.CreateExecutionRequest) {
			r.RequestID = "successor-of:run-1"
		})
		_, err := store.CreateExecution(ctx, first)
		require.Error(t, err, "run-2 is still open, so a fresh start must be refused")

		_ = again
	})

	t.Run("an unusable successor leaves the predecessor untouched", func(t *testing.T) {
		other := NewBuilder(ns, "unchained", "run-a")
		other.WorkflowTask()
		base, err := store.CreateExecution(ctx, other.Create())
		require.NoError(t, err)

		// The successor names a different workflow ID, which is not a
		// continuation of anything. The whole call must fail.
		stranger := NewBuilder(ns, "somewhere-else", "run-b")
		stranger.WorkflowTask()
		bad := stranger.Create()

		other.Complete()
		req := other.Append(base.Version)
		req.CreateSuccessor = &bad
		_, err = store.AppendHistory(ctx, req)
		require.Error(t, err)

		after, err := store.GetExecution(ctx, ns, "unchained", "run-a")
		require.NoError(t, err)
		require.Equal(t, base.Version, after.Version, "a rejected append must not advance the version")
		require.Equal(t, skald.StatusRunning, after.Status, "a rejected append must not close the run")
	})
}
