package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// DefaultVersion is the version GetVersion reports for a run that started
// before a change was introduced. See Environment.GetVersion.
const DefaultVersion = -1

// DefaultActivityStartToCloseTimeout is applied when a caller sets neither a
// start-to-close nor a schedule-to-close timeout.
//
// The engine rejects an activity with no upper bound at all, and failing the
// workflow task for a missing option would turn a small omission into an
// outage. A conservative default is the lesser evil, and it is documented on
// the public ActivityOptions so it is not a surprise.
const DefaultActivityStartToCloseTimeout = 30 * time.Second

// Info describes the run to workflow code. Every field is derived from event 1
// of the history, so it is identical on every replay.
type Info struct {
	Namespace               string
	Execution               skald.WorkflowExecution
	WorkflowType            string
	TaskQueue               string
	Attempt                 int32
	FirstExecutionRunID     string
	ContinuedExecutionRunID string
	CronSchedule            string
	StartedAt               time.Time
	RunTimeout              time.Duration
	ExecutionTimeout        time.Duration
	TaskTimeout             time.Duration
	Memo                    map[string]string
	SearchAttrs             map[string]string
}

// ActivityParams is the fully resolved description of one activity invocation.
type ActivityParams struct {
	ActivityID             string
	ActivityType           string
	TaskQueue              string
	Input                  *skald.Payload
	RetryPolicy            *skald.RetryPolicy
	ScheduleToCloseTimeout time.Duration
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
	HeartbeatTimeout       time.Duration
}

// ContinueAsNewParams describes the successor run requested by workflow code.
type ContinueAsNewParams struct {
	WorkflowType string
	TaskQueue    string
	Input        *skald.Payload
	RunTimeout   time.Duration
	TaskTimeout  time.Duration
	RetryPolicy  *skald.RetryPolicy
}

// outstandingCommand is a command the workflow has produced that has not yet
// been confirmed by a history event.
//
// The queue of these is the entire non-determinism detector. Whether a command
// was produced by replaying old history or by the live tail of the execution,
// it lands here; whether the matching event comes from the history the server
// just sent or from the batch the server is about to write, it is matched the
// same way. One mechanism, two situations, no second code path to get wrong.
type outstandingCommand struct {
	cmd  history.Command
	bind func(ev history.Event)
	// origin is a short description of the workflow-side call that produced the
	// command, used to make the non-determinism diagnostic actionable.
	origin string
}

// activityHandle tracks one scheduled activity from command to resolution.
type activityHandle struct {
	settable         Settable[*skald.Payload]
	activityID       string
	scheduledEventID int64
	// closedByServer records that a terminal activity event has arrived, which
	// makes any further cancellation command illegal.
	closedByServer bool
	canceled       bool
	removeCancel   func()
}

// timerHandle tracks one durable timer.
type timerHandle struct {
	settable       Settable[struct{}]
	timerID        string
	startedEventID int64
	closedByServer bool
	canceled       bool
	removeCancel   func()
}

// Environment is the workflow-visible side of one run: it turns workflow API
// calls into commands and history lookups, and it owns everything that must be
// identical across replays.
//
// It is single-threaded by construction. Every method runs either on a coroutine
// (with the dispatcher holding the CPU) or on the executor between coroutine
// passes, never both at once.
type Environment struct {
	dispatcher *Dispatcher
	conv       skald.DataConverter
	logger     *slog.Logger

	info    Info
	rootCtx Context
	// cancelRoot cancels rootCtx when the server records a cancellation request.
	cancelRoot CancelFunc

	// now is the event time of the workflow task being processed. Workflow code
	// reads time only from here.
	now time.Time
	// rng and uuidRng are seeded from event 1 and are separate streams so that a
	// workflow's own use of Rand cannot shift the UUIDs it generates.
	rng     *rand.Rand
	uuidRng *rand.Rand

	replaying bool

	outstanding []*outstandingCommand
	completed   bool
	closed      bool

	// Deterministic identifier counters. Never UUIDs from the OS: an activity ID
	// drawn from the operating system would differ on every replay, and the
	// engine would reject the second one as a duplicate schedule.
	activitySeq      int
	timerSeq         int
	sideEffectSeq    int
	localActivitySeq int
	mutableSeq       map[string]int

	activities map[int64]*activityHandle
	timers     map[int64]*timerHandle

	// pendingCancels are cancellations whose command must wait: either because
	// the thing being cancelled had no history event yet, or because the
	// cancellation was triggered while history was still being applied.
	pendingCancels []func()

	// applyingHistory is true while the executor is feeding recorded events into
	// the environment, as opposed to running workflow code.
	//
	// It exists because a cancellation raised by an event -- a
	// WorkflowExecutionCancelRequested cancels the root context, which resolves
	// every activity and timer under it -- must not decide *there* whether to
	// emit a cancel command. Later events in the same task may say the server
	// already resolved that activity, in which case a cancel command for it names
	// something that is no longer pending and the server rejects the whole batch.
	// Deferring the decision to flushPendingCancels, which runs once all of the
	// task's history has been applied, makes it against the final state instead
	// of a prefix. The command's position in the batch is unchanged: the flush is
	// still the first thing that happens in a run.
	applyingHistory bool

	// markers holds every MarkerRecorded event in the history, keyed by
	// name+id, so that a side effect can find its recorded value without
	// scanning.
	markers      map[string]history.MarkerRecordedAttributes
	versionCache map[string]int
	mutableCache map[string]*skald.Payload

	signalChannels map[string]*channelImpl[*skald.Payload]
}

func newEnvironment(d *Dispatcher, conv skald.DataConverter, logger *slog.Logger) *Environment {
	env := &Environment{
		dispatcher: d,
		conv:       conv,
		// Replay is the default posture. A fresh environment has seen no
		// history yet, so assuming "this has all happened before" until the
		// executor says otherwise is the safe direction to be wrong in: it
		// suppresses effects rather than duplicating them.
		replaying:      true,
		mutableSeq:     map[string]int{},
		activities:     map[int64]*activityHandle{},
		timers:         map[int64]*timerHandle{},
		markers:        map[string]history.MarkerRecordedAttributes{},
		versionCache:   map[string]int{},
		mutableCache:   map[string]*skald.Payload{},
		signalChannels: map[string]*channelImpl[*skald.Payload]{},
	}
	// The logger is wrapped so that replayed log statements are dropped. See
	// replayHandler for why that is not merely a nicety.
	env.logger = slog.New(&replayHandler{inner: logger.Handler(), replaying: env.IsReplaying})
	return env
}

// ---------------------------------------------------------------------------
// Read-only accessors used by pkg/workflow
// ---------------------------------------------------------------------------

// GetInfo returns the run's immutable description.
func (env *Environment) GetInfo() Info { return env.info }

// Now returns the workflow's notion of the current time: the timestamp of the
// workflow task being processed, never the worker's wall clock. Two replays of
// the same history therefore see the same instant at the same point in the code.
func (env *Environment) Now() time.Time { return env.now }

// Rand returns the run's deterministic random source.
func (env *Environment) Rand() *rand.Rand { return env.rng }

// NewUUID returns a version 4 UUID drawn from the run's deterministic stream.
func (env *Environment) NewUUID() string {
	id, err := uuid.NewRandomFromReader(env.uuidRng)
	if err != nil {
		// The reader is an infinite deterministic stream, so this cannot happen;
		// returning an error from a workflow API for an impossible condition is
		// worse than making the invariant explicit.
		panic(fmt.Sprintf("workflow: deterministic uuid source failed: %v", err))
	}
	return id.String()
}

// IsReplaying reports whether the code currently running is re-deriving a
// decision that has already been made and recorded.
func (env *Environment) IsReplaying() bool { return env.replaying }

// Logger returns the run's logger. Its output is suppressed during replay.
func (env *Environment) Logger() *slog.Logger { return env.logger }

// Converter returns the data converter used for payloads.
func (env *Environment) Converter() skald.DataConverter { return env.conv }

// RootContext returns the context handed to the workflow function.
func (env *Environment) RootContext() Context { return env.rootCtx }

// ---------------------------------------------------------------------------
// Command production
// ---------------------------------------------------------------------------

// enqueue appends a command to the batch and to the outstanding queue.
func (env *Environment) enqueue(origin string, cmd history.Command, bind func(history.Event)) {
	if env.closed {
		// The workflow has already produced its closing command. A sibling
		// coroutine that is still running may try to schedule more work; the
		// server rejects any command after a closing one, so dropping it here is
		// the only behaviour that keeps the batch valid. The execution is over --
		// this is not lost work, it is work that was never going to happen.
		return
	}
	env.outstanding = append(env.outstanding, &outstandingCommand{cmd: cmd, bind: bind, origin: origin})
}

// flushPendingCancels emits the cancellation commands that had to wait for
// their target to acquire a history event.
//
// It runs at the top of every workflow task, before any coroutine, so the
// resulting commands occupy a fixed position in the batch on every replay.
func (env *Environment) flushPendingCancels() {
	if len(env.pendingCancels) == 0 {
		return
	}
	pending := env.pendingCancels
	env.pendingCancels = nil
	for _, fn := range pending {
		fn()
	}
}

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

// ExecuteActivity schedules an activity and returns a future for its result.
func (env *Environment) ExecuteActivity(ctx Context, p ActivityParams) Future[*skald.Payload] {
	if p.ActivityID == "" {
		env.activitySeq++
		p.ActivityID = "act_" + strconv.Itoa(env.activitySeq)
	}
	if p.ScheduleToCloseTimeout <= 0 && p.StartToCloseTimeout <= 0 {
		p.StartToCloseTimeout = DefaultActivityStartToCloseTimeout
	}

	f, set := NewFuture[*skald.Payload](ctx, "activity "+p.ActivityID)
	h := &activityHandle{settable: set, activityID: p.ActivityID}

	if err := ctx.Err(); err != nil {
		// Already cancelled. Scheduling and then immediately cancelling would
		// cost two history events and one wasted activity dispatch to reach the
		// same answer.
		set.SetError(err)
		return f
	}

	env.enqueue("ExecuteActivity("+p.ActivityType+")", history.Command{
		Type: history.CommandTypeScheduleActivityTask,
		ScheduleActivity: &history.ScheduleActivityCommand{
			ActivityID:             p.ActivityID,
			ActivityType:           p.ActivityType,
			TaskQueue:              p.TaskQueue,
			Input:                  p.Input,
			RetryPolicy:            p.RetryPolicy,
			ScheduleToCloseTimeout: p.ScheduleToCloseTimeout,
			ScheduleToStartTimeout: p.ScheduleToStartTimeout,
			StartToCloseTimeout:    p.StartToCloseTimeout,
			HeartbeatTimeout:       p.HeartbeatTimeout,
		},
	}, func(ev history.Event) {
		h.scheduledEventID = ev.ID
		env.activities[ev.ID] = h
	})

	h.removeCancel = onCancel(ctx, func() { env.cancelActivity(h) })
	return f
}

// cancelActivity resolves an activity's future with a CanceledError and asks the
// server to stop the attempt.
//
// The future is resolved immediately rather than after the server confirms the
// cancellation. That is a deliberate semantic: a cancelled workflow should be
// able to run its compensation logic now, not after a round trip to a worker
// that may itself be gone. The server still records the request, so the activity
// worker learns to stop on its next heartbeat.
func (env *Environment) cancelActivity(h *activityHandle) {
	if h.canceled {
		return
	}
	h.canceled = true
	if h.removeCancel != nil {
		h.removeCancel()
	}
	emit := func() {
		if h.closedByServer || h.scheduledEventID == 0 {
			return
		}
		env.enqueue("cancel activity "+h.activityID, history.Command{
			Type:                  history.CommandTypeRequestCancelActivityTask,
			RequestCancelActivity: &history.RequestCancelActivityCommand{ScheduledEventID: h.scheduledEventID},
		}, nil)
	}
	if h.scheduledEventID == 0 || env.applyingHistory {
		// Either the scheduling command has not been written to history yet, so
		// there is no event id to name, or history is still being applied and a
		// later event may resolve this activity. Both are answered by deferring
		// to the flush at the top of the next run.
		env.pendingCancels = append(env.pendingCancels, emit)
	} else {
		emit()
	}
	h.settable.SetError(&skald.CanceledError{})
}

func (env *Environment) resolveActivity(scheduledEventID int64, result *skald.Payload, err error) {
	h, ok := env.activities[scheduledEventID]
	if !ok {
		return
	}
	h.closedByServer = true
	if h.removeCancel != nil {
		h.removeCancel()
	}
	h.settable.Set(result, err)
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// NewTimer starts a durable timer and returns a future that resolves when it
// fires, or with a CanceledError if the context is cancelled first.
func (env *Environment) NewTimer(ctx Context, d time.Duration) Future[struct{}] {
	env.timerSeq++
	id := "timer_" + strconv.Itoa(env.timerSeq)

	f, set := NewFuture[struct{}](ctx, "timer "+id)
	h := &timerHandle{settable: set, timerID: id}

	if err := ctx.Err(); err != nil {
		set.SetError(err)
		return f
	}
	if d <= 0 {
		// A zero sleep is a no-op, not a history event. Writing one would cost
		// three events for something the workflow can decide locally.
		set.SetValue(struct{}{})
		return f
	}

	env.enqueue("NewTimer("+d.String()+")", history.Command{
		Type:       history.CommandTypeStartTimer,
		StartTimer: &history.StartTimerCommand{TimerID: id, StartToFireTimeout: d},
	}, func(ev history.Event) {
		h.startedEventID = ev.ID
		env.timers[ev.ID] = h
	})

	h.removeCancel = onCancel(ctx, func() { env.cancelTimer(h) })
	return f
}

func (env *Environment) cancelTimer(h *timerHandle) {
	if h.canceled {
		return
	}
	h.canceled = true
	if h.removeCancel != nil {
		h.removeCancel()
	}
	emit := func() {
		if h.closedByServer || h.startedEventID == 0 {
			return
		}
		env.enqueue("cancel timer "+h.timerID, history.Command{
			Type:        history.CommandTypeCancelTimer,
			CancelTimer: &history.CancelTimerCommand{StartedEventID: h.startedEventID},
		}, nil)
	}
	// See cancelActivity: a timer the server fired later in this same task must
	// not be cancelled by a command computed from a prefix of the history.
	if h.startedEventID == 0 || env.applyingHistory {
		env.pendingCancels = append(env.pendingCancels, emit)
	} else {
		emit()
	}
	h.settable.SetError(&skald.CanceledError{})
}

func (env *Environment) fireTimer(startedEventID int64) {
	h, ok := env.timers[startedEventID]
	if !ok {
		return
	}
	h.closedByServer = true
	if h.removeCancel != nil {
		h.removeCancel()
	}
	h.settable.SetValue(struct{}{})
}

func (env *Environment) timerCanceledByServer(startedEventID int64) {
	if h, ok := env.timers[startedEventID]; ok {
		h.closedByServer = true
	}
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

// GetSignalChannel returns the channel carrying signals of the given name.
//
// The channel is unbounded on purpose: by the time the SDK sees a signal it is
// already durable in the history, so refusing it would lose data that the server
// promised to deliver.
func (env *Environment) GetSignalChannel(name string) ReceiveChannel[*skald.Payload] {
	return env.signalChannel(name)
}

func (env *Environment) signalChannel(name string) *channelImpl[*skald.Payload] {
	ch, ok := env.signalChannels[name]
	if !ok {
		ch = newChannel[*skald.Payload](env.dispatcher, "signal "+name, Unbounded)
		env.signalChannels[name] = ch
	}
	return ch
}

// deliverSignal buffers a signal for its channel.
//
// The channel is created on demand even when no workflow code has asked for it
// yet: a signal that arrives before the workflow reaches GetSignalChannel must
// still be waiting there when it does, or a signal-with-start would race the
// workflow's own first instruction.
func (env *Environment) deliverSignal(name string, input *skald.Payload) {
	env.signalChannel(name).deliver(input)
}

// ---------------------------------------------------------------------------
// Await
// ---------------------------------------------------------------------------

// Await blocks the calling coroutine until cond returns true.
func (env *Environment) Await(ctx Context, cond func() bool) error {
	co := mustCoroutine(ctx, "Await")
	env.dispatcher.markProgress()
	for !cond() {
		if err := ctx.Err(); err != nil {
			return err
		}
		co.yield("await")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Markers: side effects, versions and local activities
// ---------------------------------------------------------------------------

func markerKey(name, id string) string { return name + "\x00" + id }

func (env *Environment) marker(name, id string) (history.MarkerRecordedAttributes, bool) {
	a, ok := env.markers[markerKey(name, id)]
	return a, ok
}

func (env *Environment) recordMarker(name, id string, details *skald.Payload, failure *skald.ApplicationError) {
	env.enqueue("marker "+name+"/"+id, history.Command{
		Type: history.CommandTypeRecordMarker,
		RecordMarker: &history.RecordMarkerCommand{
			MarkerName: name,
			MarkerID:   id,
			Details:    details,
			Failure:    failure,
		},
	}, nil)
}

// SideEffect runs fn exactly once in the life of the execution and records the
// result, so that every replay reuses the value instead of recomputing it.
//
// This is the escape hatch for genuinely local non-determinism -- reading a
// config value, generating a token from an external library -- that does not
// deserve the cost of an activity.
func (env *Environment) SideEffect(fn func() (*skald.Payload, error)) (*skald.Payload, error) {
	env.sideEffectSeq++
	id := strconv.Itoa(env.sideEffectSeq)

	if a, ok := env.marker(history.MarkerSideEffect, id); ok {
		// The command is still emitted: replay has to produce the same batch it
		// produced the first time, and the marker is part of that batch.
		env.recordMarker(history.MarkerSideEffect, id, a.Details, a.Failure)
		if a.Failure != nil {
			return nil, a.Failure
		}
		return a.Details, nil
	}
	if env.replaying {
		panic(&NonDeterminismError{
			Detail: fmt.Sprintf("workflow code called SideEffect for index %s, but the recorded "+
				"history has no side-effect marker there; a side effect was added, removed or "+
				"reordered relative to the code that produced this history", id),
		})
	}
	p, err := fn()
	failure := skald.AsApplicationError(err)
	env.recordMarker(history.MarkerSideEffect, id, p, failure)
	return p, err
}

// MutableSideEffect is SideEffect for a value that may legitimately change over
// the life of a long-running workflow, recording a new marker only when it does.
//
// The saving is real: a workflow that polls a feature flag once a minute for a
// month records one marker per *change* instead of forty thousand per month.
func (env *Environment) MutableSideEffect(
	id string,
	fn func() (*skald.Payload, error),
	equals func(a, b *skald.Payload) bool,
) (*skald.Payload, error) {
	env.mutableSeq[id]++
	callIndex := env.mutableSeq[id]
	// The call index, not just the id, is part of the marker id. Whether a given
	// call recorded anything is itself a recorded fact -- it depends on whether
	// the value had changed -- so replay has to be able to ask "was there a
	// marker for call 7 of this id?" and get a straight answer. A plain side
	// effect's marker id is a bare decimal counter, so the colon keeps the two
	// namespaces from ever colliding.
	markerID := id + ":" + strconv.Itoa(callIndex)

	if a, ok := env.marker(history.MarkerSideEffect, markerID); ok {
		env.recordMarker(history.MarkerSideEffect, markerID, a.Details, a.Failure)
		env.mutableCache[id] = a.Details
		if a.Failure != nil {
			return nil, a.Failure
		}
		return a.Details, nil
	}
	if env.replaying {
		cached, ok := env.mutableCache[id]
		if !ok {
			panic(&NonDeterminismError{
				Detail: fmt.Sprintf("workflow code called MutableSideEffect(%q) call %d during replay, "+
					"but no value for it was ever recorded", id, callIndex),
			})
		}
		return cached, nil
	}

	p, err := fn()
	if err != nil {
		failure := skald.AsApplicationError(err)
		env.recordMarker(history.MarkerSideEffect, markerID, nil, failure)
		return nil, err
	}
	if cached, ok := env.mutableCache[id]; ok && equals != nil && equals(cached, p) {
		// Unchanged: no marker, no history growth.
		return cached, nil
	}
	env.mutableCache[id] = p
	env.recordMarker(history.MarkerSideEffect, markerID, p, nil)
	return p, nil
}

// GetVersion is the versioning gate that makes an incompatible workflow change
// deployable without breaking the executions already in flight.
//
// # How it works
//
// The first time a run reaches a given changeID, GetVersion picks maxSupported
// and writes it into the history as a Version marker. Every later replay of that
// run reads the marker and returns the same number, forever. A run that started
// before the change existed has no marker, and GetVersion returns DefaultVersion.
//
// # How you use it
//
// Wrap the changed region:
//
//	v := workflow.GetVersion(ctx, "use-v2-charge", workflow.DefaultVersion, 1)
//	if v == workflow.DefaultVersion {
//	    err = workflow.ExecuteActivity(ctx, ChargeV1, req).Get(ctx, nil)
//	} else {
//	    err = workflow.ExecuteActivity(ctx, ChargeV2, req).Get(ctx, nil)
//	}
//
// Deploying that is safe: in-flight runs keep taking the old branch because
// their history says DefaultVersion, and new runs take the new one. Once every
// old run has drained you raise minSupported to 1 and delete the old branch; a
// straggler that still has DefaultVersion in its history then fails loudly with
// a non-determinism error instead of silently taking the wrong path.
//
// The rule that makes all of this work is that the *decision*, not the code,
// lives in the history. Nothing about the deployed binary can change what an
// in-flight execution already decided.
func (env *Environment) GetVersion(changeID string, minSupported, maxSupported int) int {
	if v, ok := env.versionCache[changeID]; ok {
		env.checkVersionRange(changeID, v, minSupported, maxSupported)
		return v
	}
	if a, ok := env.marker(history.MarkerVersion, changeID); ok {
		var v int
		if err := env.conv.FromPayload(a.Details, &v); err != nil {
			panic(&NonDeterminismError{
				Detail: fmt.Sprintf("version marker %q has an undecodable value: %v", changeID, err),
			})
		}
		env.versionCache[changeID] = v
		env.recordMarker(history.MarkerVersion, changeID, a.Details, nil)
		env.checkVersionRange(changeID, v, minSupported, maxSupported)
		return v
	}
	if env.replaying {
		// No marker anywhere in the history: this run started before the change
		// was introduced. Emitting a marker now would add a command the recorded
		// history does not have, so the only correct answer is "the old code".
		if minSupported > DefaultVersion {
			panic(&NonDeterminismError{
				Detail: fmt.Sprintf("change %q has minimum supported version %d, but this execution "+
					"predates the change and is pinned to %d; the branch it needs has been deleted "+
					"from the workflow code",
					changeID, minSupported, DefaultVersion),
			})
		}
		env.versionCache[changeID] = DefaultVersion
		return DefaultVersion
	}
	env.versionCache[changeID] = maxSupported
	p, err := env.conv.ToPayload(maxSupported)
	if err != nil {
		panic(fmt.Sprintf("workflow: encoding a version number failed: %v", err))
	}
	env.recordMarker(history.MarkerVersion, changeID, p, nil)
	return maxSupported
}

func (env *Environment) checkVersionRange(changeID string, v, minSupported, maxSupported int) {
	if v < minSupported || v > maxSupported {
		panic(&NonDeterminismError{
			Detail: fmt.Sprintf("execution is pinned to version %d of change %q, but this build "+
				"supports only [%d, %d]; roll the deploy back or widen the range",
				v, changeID, minSupported, maxSupported),
		})
	}
}

// ExecuteLocalActivity runs fn in the worker process and records the result as a
// marker.
//
// A local activity trades durability of the *attempt* for latency: there is no
// scheduling event, no dispatch and no separate worker, so a 2ms lookup costs
// 2ms instead of two round trips. The price is that it runs inside the workflow
// task, on the workflow task's clock, and a worker crash mid-call means the work
// is simply redone. Use it for short, idempotent, low-risk calls.
func (env *Environment) ExecuteLocalActivity(ctx Context, name string, fn func(context.Context) (*skald.Payload, error)) Future[*skald.Payload] {
	env.localActivitySeq++
	id := strconv.Itoa(env.localActivitySeq)

	if a, ok := env.marker(history.MarkerLocalActivity, id); ok {
		env.recordMarker(history.MarkerLocalActivity, id, a.Details, a.Failure)
		if a.Failure != nil {
			return ReadyFuture[*skald.Payload](ctx, "local activity "+name, nil, a.Failure)
		}
		return ReadyFuture(ctx, "local activity "+name, a.Details, nil)
	}
	if env.replaying {
		panic(&NonDeterminismError{
			Detail: fmt.Sprintf("workflow code executed local activity %q (index %s) during replay, "+
				"but the recorded history has no marker for it", name, id),
		})
	}

	p, err := fn(context.Background())
	failure := skald.AsApplicationError(err)
	env.recordMarker(history.MarkerLocalActivity, id, p, failure)
	if err != nil {
		return ReadyFuture[*skald.Payload](ctx, "local activity "+name, nil, err)
	}
	return ReadyFuture(ctx, "local activity "+name, p, nil)
}

// ---------------------------------------------------------------------------
// Completion
// ---------------------------------------------------------------------------

// ContinueAsNew builds the control-flow error that closes this run and opens a
// successor with a fresh, empty history.
func (env *Environment) ContinueAsNew(p ContinueAsNewParams) error {
	typ := p.WorkflowType
	if typ == "" {
		typ = env.info.WorkflowType
	}
	queue := p.TaskQueue
	if queue == "" {
		queue = env.info.TaskQueue
	}
	return &skald.ContinueAsNewError{
		WorkflowType: typ,
		Input:        p.Input,
		TaskQueue:    queue,
		RunTimeout:   p.RunTimeout,
	}
}

// complete turns the workflow function's return into the batch's closing
// command.
func (env *Environment) complete(result *skald.Payload, err error) {
	if env.completed {
		return
	}
	env.completed = true

	var canErr *skald.ContinueAsNewError
	var cancelErr *skald.CanceledError
	switch {
	case err == nil:
		env.enqueue("workflow returned", history.Command{
			Type:             history.CommandTypeCompleteWorkflowExecution,
			CompleteWorkflow: &history.CompleteWorkflowCommand{Result: result},
		}, nil)
	case errors.As(err, &canErr):
		env.enqueue("workflow continued as new", history.Command{
			Type: history.CommandTypeContinueAsNewWorkflow,
			ContinueAsNew: &history.ContinueAsNewCommand{
				WorkflowType: canErr.WorkflowType,
				TaskQueue:    canErr.TaskQueue,
				Input:        canErr.Input,
				RunTimeout:   canErr.RunTimeout,
			},
		}, nil)
	case errors.As(err, &cancelErr):
		env.enqueue("workflow canceled", history.Command{
			Type:           history.CommandTypeCancelWorkflowExecution,
			CancelWorkflow: &history.CancelWorkflowCommand{Details: cancelErr.Details},
		}, nil)
	default:
		env.enqueue("workflow failed", history.Command{
			Type:         history.CommandTypeFailWorkflowExecution,
			FailWorkflow: &history.FailWorkflowCommand{Failure: skald.AsApplicationError(err)},
		}, nil)
	}
	// Set after the closing command is queued so that the command itself is not
	// dropped by the guard it installs.
	env.closed = true
}

// ---------------------------------------------------------------------------
// Deterministic randomness
// ---------------------------------------------------------------------------

// deterministicSource is a splitmix64 generator used as the workflow's random
// source.
//
// math/rand's own source is stable under Go's compatibility promise, but
// "stable" there is a promise about the standard library, not about Skald's
// histories: a workflow that has been running for six months must produce the
// same numbers after a Go upgrade as before it. Owning the generator removes
// that dependency entirely, and splitmix64 is two lines and passes BigCrush.
type deterministicSource struct{ state uint64 }

func (s *deterministicSource) Uint64() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (s *deterministicSource) Int63() int64 { return int64(s.Uint64() >> 1) }
func (s *deterministicSource) Seed(v int64) { s.state = uint64(v) }
func newDeterministicRand(seed int64) *rand.Rand {
	return rand.New(&deterministicSource{state: uint64(seed)})
}

// ---------------------------------------------------------------------------
// Replay-aware logging
// ---------------------------------------------------------------------------

// replayHandler drops every record produced while the workflow is replaying.
//
// Without it, a workflow that has taken forty tasks re-emits every log line it
// has ever produced on every task, so a single execution generates O(n^2) log
// volume and -- far worse -- a log line stops being evidence that anything
// happened. "Charging customer 42" appearing in the log would no longer mean a
// charge was attempted, which is precisely the confusion durable execution is
// supposed to remove.
type replayHandler struct {
	inner     slog.Handler
	replaying func() bool
}

func (h *replayHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.replaying() {
		return false
	}
	return h.inner.Enabled(ctx, level)
}

func (h *replayHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.replaying() {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *replayHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replayHandler{inner: h.inner.WithAttrs(attrs), replaying: h.replaying}
}

func (h *replayHandler) WithGroup(name string) slog.Handler {
	return &replayHandler{inner: h.inner.WithGroup(name), replaying: h.replaying}
}
