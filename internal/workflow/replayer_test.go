package workflow

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// activity is a tiny helper that schedules an activity through the environment
// the way pkg/workflow does, without dragging reflection into these tests.
func activity(ctx Context, typ string, input any) Future[*skald.Payload] {
	env := EnvironmentFrom(ctx)
	return env.ExecuteActivity(ctx, ActivityParams{
		ActivityType:        typ,
		Input:               skald.MustPayload(input),
		StartToCloseTimeout: time.Minute,
	})
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

func TestExecutorSequentialActivities(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		a, err := activity(ctx, "First", "a").Get(ctx)
		if err != nil {
			return nil, err
		}
		b, err := activity(ctx, "Second", decodeString(t, a)).Get(ctx)
		if err != nil {
			return nil, err
		}
		return skald.MustPayload(decodeString(t, a) + "+" + decodeString(t, b)), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Equal(t, []string{`ScheduleActivityTask activity_id="act_1" activity_type="First"`}, commandTypes(res.Commands))
	require.False(t, res.Finished)

	d.completeActivity("act_1", "one")
	res = d.runTask()
	require.Equal(t, []string{`ScheduleActivityTask activity_id="act_2" activity_type="Second"`}, commandTypes(res.Commands))

	d.completeActivity("act_2", "two")
	res = d.runTask()
	require.Equal(t, []string{"CompleteWorkflowExecution"}, commandTypes(res.Commands))
	require.True(t, res.Finished)
	require.Equal(t, "one+two", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
}

func TestExecutorParallelActivitiesProduceOneBatch(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		futures := make([]Future[*skald.Payload], 3)
		for i := range futures {
			futures[i] = activity(ctx, fmt.Sprintf("Task%d", i), i)
		}
		total := 0
		for _, f := range futures {
			p, err := f.Get(ctx)
			if err != nil {
				return nil, err
			}
			total += decodeInt(t, p)
		}
		return skald.MustPayload(total), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Len(t, res.Commands, 3, "a fan-out must produce one batch, not three tasks")
	require.Equal(t, "act_1", res.Commands[0].ScheduleActivity.ActivityID)
	require.Equal(t, "act_3", res.Commands[2].ScheduleActivity.ActivityID)

	// Complete out of order: the workflow must still see the right values.
	d.completeActivity("act_2", 20)
	d.completeActivity("act_3", 300)
	res = d.runTask()
	require.Empty(t, res.Commands, "the workflow is still waiting for the first activity")

	d.completeActivity("act_1", 1)
	res = d.runTask()
	require.True(t, res.Finished)
	require.Equal(t, 321, decodeInt(t, res.Commands[0].CompleteWorkflow.Result))
}

func TestExecutorSelectorRacesTimerAgainstActivity(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		work := activity(ctx, "Slow", nil)
		timeout := env.NewTimer(ctx, time.Minute)

		var outcome string
		NewSelector(ctx, "race").
			AddFuture(work, func() { outcome = "work" }).
			AddFuture(timeout, func() { outcome = "timeout" }).
			Select(ctx)
		return skald.MustPayload(outcome), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Equal(t, []string{
		`ScheduleActivityTask activity_id="act_1" activity_type="Slow"`,
		`StartTimer timer_id="timer_1" duration=1m0s`,
	}, commandTypes(res.Commands))

	d.fireTimer("timer_1")
	res = d.runTask()
	require.True(t, res.Finished)
	require.Equal(t, "timeout", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
}

func TestExecutorFailedActivityBecomesAWorkflowFailure(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		_, err := activity(ctx, "Doomed", nil).Get(ctx)
		return nil, err
	}
	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	d.runTask()
	d.failActivity("act_1", skald.NewNonRetryableError("Fatal", "no"))
	res := d.runTask()

	require.True(t, res.Finished)
	require.Equal(t, history.CommandTypeFailWorkflowExecution, res.Commands[0].Type)
	require.Equal(t, "Fatal", res.Commands[0].FailWorkflow.Failure.Type)
}

// ---------------------------------------------------------------------------
// Signals, cancellation, time and randomness
// ---------------------------------------------------------------------------

func TestExecutorDeliversSignalsIncludingOnesThatArrivedFirst(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		ch := env.GetSignalChannel("greeting")
		var got []string
		for i := 0; i < 2; i++ {
			p, ok := ch.Receive(ctx)
			if !ok {
				break
			}
			got = append(got, decodeString(t, p))
		}
		return skald.MustPayload(got), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)
	// A signal that landed before the workflow ever ran must not be lost.
	d.signal("greeting", "early")

	res := d.runTask()
	require.Empty(t, res.Commands)

	d.signal("greeting", "late")
	res = d.runTask()
	require.True(t, res.Finished)

	var got []string
	require.NoError(t, skald.JSONConverter{}.FromPayload(res.Commands[0].CompleteWorkflow.Result, &got))
	require.Equal(t, []string{"early", "late"}, got)
}

func TestExecutorCancellationResolvesActivityFuturesAndRequestsCancel(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		_, err := activity(ctx, "LongRunning", nil).Get(ctx)
		var canceled *skald.CanceledError
		if !errors.As(err, &canceled) {
			return nil, fmt.Errorf("expected a cancellation, got %v", err)
		}
		// Compensation runs on a disconnected context: on the cancelled one it
		// would be cancelled before it was ever dispatched.
		cleanup := NewDisconnectedContext(ctx)
		if _, err := activity(cleanup, "Compensate", nil).Get(cleanup); err != nil {
			return nil, err
		}
		return skald.MustPayload("compensated"), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Len(t, res.Commands, 1)

	d.requestCancel()
	res = d.runTask()
	// The cancel command comes first, then the compensating activity that the
	// workflow scheduled after observing the cancellation.
	require.Equal(t, []string{
		"RequestCancelActivityTask scheduled_event_id=5",
		`ScheduleActivityTask activity_id="act_2" activity_type="Compensate"`,
	}, commandTypes(res.Commands))

	d.completeActivity("act_2", nil)
	res = d.runTask()
	require.True(t, res.Finished)
	require.Equal(t, "compensated", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
}

func TestExecutorNowIsEventTimeAndRandomnessIsSeeded(t *testing.T) {
	t.Parallel()

	type observation struct {
		Now  time.Time
		Rand int64
		UUID string
	}
	var seen []observation

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		seen = append(seen, observation{Now: env.Now(), Rand: env.Rand().Int63(), UUID: env.NewUUID()})
		if _, err := activity(ctx, "Wait", nil).Get(ctx); err != nil {
			return nil, err
		}
		seen = append(seen, observation{Now: env.Now(), Rand: env.Rand().Int63(), UUID: env.NewUUID()})
		return skald.MustPayload("done"), nil
	}

	run := func() []observation {
		seen = nil
		exec := newTestExecutor(t, wf)
		d := newDriver(t, exec, nil)
		d.runTask()
		d.completeActivity("act_1", nil)
		d.runTask()
		return append([]observation(nil), seen...)
	}

	first := run()
	require.Len(t, first, 2)
	require.True(t, first[1].Now.After(first[0].Now), "Now must advance with the workflow task")
	require.NotEqual(t, first[0].UUID, first[1].UUID)

	second := run()
	require.Equal(t, first, second, "time, randomness and UUIDs must be identical across replays")
}

// ---------------------------------------------------------------------------
// Markers: side effects, mutable side effects and versions
// ---------------------------------------------------------------------------

func TestExecutorSideEffectRunsOnceAndReplaysFromTheMarker(t *testing.T) {
	t.Parallel()

	calls := 0
	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		p, err := env.SideEffect(func() (*skald.Payload, error) {
			calls++
			return skald.MustPayload(fmt.Sprintf("value-%d", calls)), nil
		})
		if err != nil {
			return nil, err
		}
		if _, err := activity(ctx, "Wait", nil).Get(ctx); err != nil {
			return nil, err
		}
		// Reading it again after a replay boundary must give the recorded value.
		return p, nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Equal(t, []string{
		`RecordMarker name="SideEffect" id="1"`,
		`ScheduleActivityTask activity_id="act_1" activity_type="Wait"`,
	}, commandTypes(res.Commands))
	require.Equal(t, 1, calls)

	d.completeActivity("act_1", nil)
	res = d.runTask()
	require.True(t, res.Finished)
	require.Equal(t, "value-1", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
	require.Equal(t, 1, calls, "a side effect must not run twice, warm or cold")

	// Cold replay of the whole history: still exactly one call.
	cold := newTestExecutor(t, wf)
	cold.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
	_, err := cold.ProcessTask(api.WorkflowTask{History: d.history()})
	require.NoError(t, err)
	require.Equal(t, 1, calls, "replaying the history must not re-run the side effect")
}

func TestExecutorMutableSideEffectRecordsOnlyWhenTheValueChanges(t *testing.T) {
	t.Parallel()

	values := []int{1, 1, 2}
	idx := 0
	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		equals := func(a, b *skald.Payload) bool { return decodeInt(t, a) == decodeInt(t, b) }
		read := func() (*skald.Payload, error) {
			v := values[idx]
			idx++
			return skald.MustPayload(v), nil
		}
		last := 0
		for i := 0; i < 3; i++ {
			p, err := env.MutableSideEffect("flag", read, equals)
			if err != nil {
				return nil, err
			}
			last = decodeInt(t, p)
			if i < 2 {
				if _, err := activity(ctx, "Wait", nil).Get(ctx); err != nil {
					return nil, err
				}
			}
		}
		return skald.MustPayload(last), nil
	}

	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	res := d.runTask()
	require.Equal(t, []string{
		`RecordMarker name="SideEffect" id="flag:1"`,
		`ScheduleActivityTask activity_id="act_1" activity_type="Wait"`,
	}, commandTypes(res.Commands))

	d.completeActivity("act_1", nil)
	res = d.runTask()
	// Second read produced the same value: no marker, only the next activity.
	require.Equal(t, []string{`ScheduleActivityTask activity_id="act_2" activity_type="Wait"`},
		commandTypes(res.Commands))

	d.completeActivity("act_2", nil)
	res = d.runTask()
	require.Equal(t, []string{
		`RecordMarker name="SideEffect" id="flag:3"`,
		"CompleteWorkflowExecution",
	}, commandTypes(res.Commands))
	require.Equal(t, 2, decodeInt(t, res.Commands[1].CompleteWorkflow.Result))
}

func TestExecutorGetVersionPinsTheBranchAnExecutionStartedOn(t *testing.T) {
	t.Parallel()

	// v1 of the workflow does not know about the change at all.
	v1 := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		if _, err := activity(ctx, "Old", nil).Get(ctx); err != nil {
			return nil, err
		}
		return skald.MustPayload("old"), nil
	}
	// v2 gates a new activity behind a version marker.
	v2 := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		if env.GetVersion("use-new", DefaultVersion, 1) == DefaultVersion {
			if _, err := activity(ctx, "Old", nil).Get(ctx); err != nil {
				return nil, err
			}
			return skald.MustPayload("old"), nil
		}
		if _, err := activity(ctx, "New", nil).Get(ctx); err != nil {
			return nil, err
		}
		return skald.MustPayload("new"), nil
	}

	t.Run("an in-flight execution keeps the old branch", func(t *testing.T) {
		exec := newTestExecutor(t, v1)
		d := newDriver(t, exec, nil)
		res := d.runTask()
		require.Equal(t, "Old", res.Commands[0].ScheduleActivity.ActivityType)
		d.completeActivity("act_1", nil)

		// Deploy v2 mid-execution: the same instance is replaced by a cold one
		// running the new code, exactly as a rolling deploy does.
		upgraded := newTestExecutor(t, v2)
		upgraded.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
		d.exec = upgraded

		res = d.runTask()
		require.True(t, res.Finished)
		require.Equal(t, "old", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
	})

	t.Run("a new execution takes the new branch and records the marker", func(t *testing.T) {
		exec := newTestExecutor(t, v2)
		d := newDriver(t, exec, nil)
		res := d.runTask()
		require.Equal(t, []string{
			`RecordMarker name="Version" id="use-new"`,
			`ScheduleActivityTask activity_id="act_1" activity_type="New"`,
		}, commandTypes(res.Commands))

		d.completeActivity("act_1", nil)
		res = d.runTask()
		require.Equal(t, "new", decodeString(t, res.Commands[0].CompleteWorkflow.Result))
	})

	t.Run("dropping support for the pinned branch fails loudly", func(t *testing.T) {
		exec := newTestExecutor(t, v1)
		d := newDriver(t, exec, nil)
		d.runTask()
		d.completeActivity("act_1", nil)

		// v3 has deleted the old branch: minSupported is now 1.
		v3 := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
			env := EnvironmentFrom(ctx)
			env.GetVersion("use-new", 1, 1)
			if _, err := activity(ctx, "New", nil).Get(ctx); err != nil {
				return nil, err
			}
			return skald.MustPayload("new"), nil
		}
		upgraded := newTestExecutor(t, v3)
		upgraded.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
		d.exec = upgraded

		_, err := d.runTaskErr()
		require.Error(t, err)
		var nd *NonDeterminismError
		require.ErrorAs(t, err, &nd)
		require.Contains(t, err.Error(), "use-new")
		require.Equal(t, history.WorkflowTaskFailedCauseNonDeterminism, FailureCause(err))
	})
}

// ---------------------------------------------------------------------------
// Non-determinism detection
// ---------------------------------------------------------------------------

func TestExecutorDetectsASwappedActivity(t *testing.T) {
	t.Parallel()

	original := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		if _, err := activity(ctx, "ChargeCard", nil).Get(ctx); err != nil {
			return nil, err
		}
		if _, err := activity(ctx, "SendReceipt", nil).Get(ctx); err != nil {
			return nil, err
		}
		return skald.MustPayload("ok"), nil
	}
	incompatible := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		// Someone inserted a timer before the first activity.
		if _, err := env.NewTimer(ctx, time.Second).Get(ctx); err != nil {
			return nil, err
		}
		if _, err := activity(ctx, "ChargeCard", nil).Get(ctx); err != nil {
			return nil, err
		}
		return skald.MustPayload("ok"), nil
	}

	exec := newTestExecutor(t, original)
	d := newDriver(t, exec, nil)
	d.runTask()
	d.completeActivity("act_1", nil)

	swapped := newTestExecutor(t, incompatible)
	swapped.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
	d.exec = swapped

	_, err := d.runTaskErr()
	require.Error(t, err)

	var nd *NonDeterminismError
	require.ErrorAs(t, err, &nd)
	msg := err.Error()
	// The diagnostic has to be good enough to fix the bug from the log alone.
	require.Contains(t, msg, "non-determinism detected")
	require.Contains(t, msg, testType)
	require.Contains(t, msg, "wf-1/run-1")
	require.Contains(t, msg, `expected (recorded in history): ScheduleActivityTask activity_id="act_1" activity_type="ChargeCard"`)
	require.Contains(t, msg, `actual   (produced by replay):  StartTimer timer_id="timer_1" duration=1s`)
	require.Contains(t, msg, "produced by: NewTimer(1s)")
	require.Contains(t, msg, "GetVersion")
	require.Contains(t, msg, "rolling")
	require.Equal(t, history.WorkflowTaskFailedCauseNonDeterminism, FailureCause(err))
	require.True(t, FailureDetail(err).NonRetryable)
}

func TestExecutorDetectsAMissingCommand(t *testing.T) {
	t.Parallel()

	original := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		activity(ctx, "One", nil)
		activity(ctx, "Two", nil)
		_, err := activity(ctx, "Three", nil).Get(ctx)
		return nil, err
	}
	shortened := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		activity(ctx, "One", nil)
		_, err := activity(ctx, "Two", nil).Get(ctx)
		return nil, err
	}

	exec := newTestExecutor(t, original)
	d := newDriver(t, exec, nil)
	d.runTask()

	shorter := newTestExecutor(t, shortened)
	shorter.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})

	_, err := shorter.ProcessTask(api.WorkflowTask{
		History:        d.history(),
		StartedEventID: 0,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "never asked for")
}

func TestExecutorDetectsAnExtraCommand(t *testing.T) {
	t.Parallel()

	original := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		_, err := activity(ctx, "One", nil).Get(ctx)
		return nil, err
	}
	longer := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		activity(ctx, "One", nil)
		_, err := activity(ctx, "Two", nil).Get(ctx)
		return nil, err
	}

	exec := newTestExecutor(t, original)
	d := newDriver(t, exec, nil)
	d.runTask()

	extra := newTestExecutor(t, longer)
	extra.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
	_, err := extra.ProcessTask(api.WorkflowTask{History: d.history()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not contain")
	require.Contains(t, err.Error(), `activity_type="Two"`)
}

// ---------------------------------------------------------------------------
// Warm and cold instances must agree
// ---------------------------------------------------------------------------

// TestWarmAndColdReplayAgree is the invariant the sticky cache rests on. The
// same history is driven twice: once through one long-lived instance, and once
// through a fresh instance for every task. The command batches must be
// identical, event for event.
func TestWarmAndColdReplayAgree(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		env := EnvironmentFrom(ctx)
		total := 0
		for i := 0; i < 3; i++ {
			p, err := activity(ctx, fmt.Sprintf("Step%d", i), i).Get(ctx)
			if err != nil {
				return nil, err
			}
			total += decodeInt(t, p)
			if _, err := env.NewTimer(ctx, time.Duration(i+1)*time.Second).Get(ctx); err != nil {
				return nil, err
			}
		}
		v, err := env.SideEffect(func() (*skald.Payload, error) { return skald.MustPayload("side"), nil })
		if err != nil {
			return nil, err
		}
		return skald.MustPayload(fmt.Sprintf("%d/%s", total, decodeString(t, v))), nil
	}

	// warm: one instance for the whole run.
	warmExec := newTestExecutor(t, wf)
	warm := newDriver(t, warmExec, nil)
	warmBatches := driveToCompletion(t, warm, nil)

	// cold: a brand new instance for every task, so every task is a full replay.
	coldExec := newTestExecutor(t, wf)
	cold := newDriver(t, coldExec, nil)
	coldBatches := driveToCompletion(t, cold, func() {
		fresh := newTestExecutor(t, wf)
		fresh.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
		cold.exec = fresh
	})

	require.Equal(t, warmBatches, coldBatches,
		"a cold replay must produce exactly the commands a warm instance produced")
	require.Equal(t, len(warm.events), len(cold.events))
}

// driveToCompletion runs tasks, resolving whatever the workflow is waiting on,
// until it closes. evict is called before every task when set.
func driveToCompletion(t *testing.T, d *driver, evict func()) [][]string {
	t.Helper()
	var batches [][]string
	for step := 0; step < 30; step++ {
		if evict != nil {
			evict()
		}
		res := d.runTask()
		batches = append(batches, commandTypes(res.Commands))
		if res.Finished {
			return batches
		}
		// Resolve exactly one outstanding thing, preferring activities so that
		// the two runs make the same choices.
		if !d.resolveOne() {
			t.Fatalf("step %d: the workflow produced no commands and nothing is pending", step)
		}
	}
	t.Fatal("workflow did not finish in 30 tasks")
	return nil
}

// resolveOne completes the oldest unresolved activity or fires the oldest timer.
func (d *driver) resolveOne() bool {
	d.t.Helper()
	type pending struct {
		id      string
		eventID int64
		isTimer bool
	}
	var best *pending
	consider := func(p pending) {
		if best == nil || p.eventID < best.eventID {
			cp := p
			best = &cp
		}
	}
	for id, eventID := range d.activityIDs {
		if !d.activityClosed(eventID) {
			consider(pending{id: id, eventID: eventID})
		}
	}
	for id, eventID := range d.timerIDs {
		if !d.timerClosed(eventID) {
			consider(pending{id: id, eventID: eventID, isTimer: true})
		}
	}
	if best == nil {
		return false
	}
	if best.isTimer {
		d.fireTimer(best.id)
	} else {
		d.completeActivity(best.id, 1)
	}
	return true
}

func (d *driver) activityClosed(scheduledEventID int64) bool {
	for _, ev := range d.events {
		switch a := ev.Attrs.(type) {
		case history.ActivityTaskCompletedAttributes:
			if a.ScheduledEventID == scheduledEventID {
				return true
			}
		case history.ActivityTaskFailedAttributes:
			if a.ScheduledEventID == scheduledEventID {
				return true
			}
		case history.ActivityTaskCanceledAttributes:
			if a.ScheduledEventID == scheduledEventID {
				return true
			}
		}
	}
	return false
}

func (d *driver) timerClosed(startedEventID int64) bool {
	for _, ev := range d.events {
		switch a := ev.Attrs.(type) {
		case history.TimerFiredAttributes:
			if a.StartedEventID == startedEventID {
				return true
			}
		case history.TimerCanceledAttributes:
			if a.StartedEventID == startedEventID {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Tasks that never completed
// ---------------------------------------------------------------------------

// TestExecutorSkipsWorkflowTasksThatNeverCompleted covers the crash case: a task
// whose commands were computed but never applied left no events behind, so a
// cold replay must not run the workflow for it. Running it would advance past
// decisions the server never saw.
func TestExecutorSkipsWorkflowTasksThatNeverCompleted(t *testing.T) {
	t.Parallel()

	runs := 0
	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		runs++
		_, err := activity(ctx, "Only", nil).Get(ctx)
		return skald.MustPayload("done"), err
	}

	// Build a history in which the first task started and then timed out.
	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)
	d.add(history.WorkflowTaskScheduledAttributes{TaskQueue: testQueue, Attempt: 1})
	scheduled := d.events.LastEventID()
	started := d.add(history.WorkflowTaskStartedAttributes{ScheduledEventID: scheduled})
	d.taskTimedOut(scheduled, started.ID)

	// The retry succeeds and schedules the activity.
	res := d.runTask()
	require.Len(t, res.Commands, 1)
	require.Equal(t, "act_1", res.Commands[0].ScheduleActivity.ActivityID)

	// A cold replay of the same history must reach the same place.
	cold := newTestExecutor(t, wf)
	cold.SetExecutionInfo(testNamespace, skald.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"})
	_, err := cold.ProcessTask(api.WorkflowTask{History: d.history()})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// The sticky cache
// ---------------------------------------------------------------------------

// TestCacheTakeHandsOverOwnershipWithoutClosing pins the rule that a taken
// instance is still alive.
//
// This is not hypothetical: the LRU fires its eviction callback on every removal,
// so an implementation that used one would close the very instance it just
// handed to a caller. The symptom is a workflow task failing with "dispatcher is
// closed" and then mysteriously succeeding on retry, which is exactly the kind
// of bug that gets written off as flakiness.
func TestCacheTakeHandsOverOwnershipWithoutClosing(t *testing.T) {
	t.Parallel()

	blocking := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		_, err := activity(ctx, "Wait", nil).Get(ctx)
		return skald.MustPayload("done"), err
	}

	cache, err := NewCache(2)
	require.NoError(t, err)

	exec := newTestExecutor(t, blocking)
	d := newDriver(t, exec, nil)
	d.runTask()
	cache.Put("run-1", exec)
	require.Equal(t, 1, cache.Len())

	taken, ok := cache.Take("run-1")
	require.True(t, ok)
	require.Same(t, exec, taken)
	require.Equal(t, 0, cache.Len())

	// Still usable: this is the whole point.
	d.completeActivity("act_1", nil)
	res := d.runTask()
	require.True(t, res.Finished)
}

func TestCacheEvictionUnwindsInstances(t *testing.T) {
	before := goroutineCount()

	simple := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		_, err := activity(ctx, "Wait", nil).Get(ctx)
		return nil, err
	}

	cache, err := NewCache(2)
	require.NoError(t, err)

	// Three instances into a two-slot cache: the oldest must be closed, not
	// merely forgotten, or its parked coroutine is leaked.
	for i := 0; i < 3; i++ {
		exec := newTestExecutor(t, simple)
		d := newDriver(t, exec, nil)
		d.runTask()
		cache.Put(fmt.Sprintf("run-%d", i), exec)
	}
	require.Equal(t, 2, cache.Len())

	cache.Clear()
	require.Equal(t, 0, cache.Len())
	waitForGoroutines(t, before)
}

// ---------------------------------------------------------------------------
// Panics in workflow code
// ---------------------------------------------------------------------------

func TestExecutorReportsAWorkflowPanicWithItsStack(t *testing.T) {
	t.Parallel()

	wf := func(ctx Context, input *skald.Payload) (*skald.Payload, error) {
		var m map[string]string
		m["boom"] = "yes" // assignment to a nil map
		return nil, nil
	}
	exec := newTestExecutor(t, wf)
	d := newDriver(t, exec, nil)

	_, err := d.runTaskErr()
	require.Error(t, err)
	var panicErr *CoroutinePanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, history.WorkflowTaskFailedCauseWorkflowPanic, FailureCause(err))
	require.Contains(t, FailureDetail(err).StackTrace, "replayer_test.go")
	require.True(t, exec.Poisoned())
}
