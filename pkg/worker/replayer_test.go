package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
	"github.com/Liona-orph/skald/pkg/workflow"
)

// recordProductionHistory runs a workflow for real and returns the history it
// produced. Replaying a *synthetic* history would only prove the replayer agrees
// with the test that wrote it; replaying one the engine actually recorded is the
// thing worth asserting.
func recordProductionHistory(t *testing.T) history.History {
	t.Helper()
	replaySideEffectCalls.Store(0)

	e := newTestEnv(t)
	e.worker.RegisterWorkflow(ReplayFixtureWorkflow)
	e.worker.RegisterActivity(Echo)
	e.worker.RegisterActivity(Upper)
	e.start()

	runID := e.startWorkflow("replay-1", "ReplayFixtureWorkflow", "seed")
	var out string
	e.awaitResult("replay-1", runID, &out)
	require.Equal(t, "seed!/token/1", out)

	return e.history("replay-1", runID)
}

var replaySideEffectCalls atomic.Int32

// ReplayFixtureWorkflow exercises everything a replay has to reconstruct:
// activities, a timer, a side effect and a version gate.
func ReplayFixtureWorkflow(ctx workflow.Context, seed string) (string, error) {
	ctx = defaultActivityOptions(ctx)

	var echoed string
	if err := workflow.GetResult(ctx, workflow.ExecuteActivity(ctx, Echo, seed), &echoed); err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, 20*time.Millisecond); err != nil {
		return "", err
	}
	token, err := workflow.SideEffect(ctx, func() string {
		replaySideEffectCalls.Add(1)
		return "token"
	})
	if err != nil {
		return "", err
	}
	version := workflow.GetVersion(ctx, "fixture", workflow.DefaultVersion, 1)
	upper, err := workflow.ExecuteActivityAs[string](ctx, Upper, echoed).Get(ctx)
	if err != nil {
		return "", err
	}
	return upper + "/" + token + "/" + itoa(version), nil
}

func itoa(v int) string {
	if v < 0 {
		return "-" + itoa(-v)
	}
	if v < 10 {
		return string(rune('0' + v))
	}
	return itoa(v/10) + string(rune('0'+v%10))
}

// breakingFixtureWorkflow reorders the first two steps, which is the shape of
// almost every real non-determinism incident.
func breakingFixtureWorkflow(ctx workflow.Context, seed string) (string, error) {
	ctx = defaultActivityOptions(ctx)
	if err := workflow.Sleep(ctx, 20*time.Millisecond); err != nil {
		return "", err
	}
	if err := workflow.Wait(ctx, workflow.ExecuteActivity(ctx, Echo, seed)); err != nil {
		return "", err
	}
	return "changed", nil
}

func TestReplayerAcceptsAHistoryTheCodeStillMatches(t *testing.T) {
	h := recordProductionHistory(t)
	require.Equal(t, int32(1), replaySideEffectCalls.Load())

	r := NewReplayer(ReplayOptions{})
	r.RegisterWorkflow(ReplayFixtureWorkflow)
	require.NoError(t, r.ReplayHistory(context.Background(), h))

	// The whole point of ReplayOnly: verifying a production history must never
	// re-run a side effect against production.
	require.Equal(t, int32(1), replaySideEffectCalls.Load(),
		"replaying a history must not execute its side effects again")
}

// TestReplayerRejectsABreakingChange is the check a team would run in CI on
// every pull request that touches a workflow.
func TestReplayerRejectsABreakingChange(t *testing.T) {
	h := recordProductionHistory(t)

	r := NewReplayer(ReplayOptions{})
	r.RegisterWorkflowWithOptions(breakingFixtureWorkflow, RegisterOptions{Name: "ReplayFixtureWorkflow"})

	err := r.ReplayHistory(context.Background(), h)
	require.Error(t, err)

	var nd *internalwf.NonDeterminismError
	require.ErrorAs(t, err, &nd)
	require.Contains(t, err.Error(), "non-determinism detected")
	require.Contains(t, err.Error(), "ScheduleActivityTask")
	require.Contains(t, err.Error(), "StartTimer")
}

func TestReplayerReadsAHistoryFile(t *testing.T) {
	h := recordProductionHistory(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "history.json")
	data, err := json.MarshalIndent(h, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	r := NewReplayer(ReplayOptions{})
	r.RegisterWorkflow(ReplayFixtureWorkflow)
	require.NoError(t, r.ReplayHistoryFile(context.Background(), path))

	// A history exported straight from the GetHistory endpoint is an object with
	// an events field, not a bare array. Both must work, or nobody will bother
	// wiring the CI check up.
	envelope, err := json.Marshal(api.GetHistoryResponse{
		Events: h, Status: skald.StatusCompleted, NextEventID: h.NextEventID(),
	})
	require.NoError(t, err)
	require.NoError(t, r.ReplayHistoryJSON(context.Background(), envelope))
}

func TestReplayerRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	r := NewReplayer(ReplayOptions{})
	r.RegisterWorkflow(ReplayFixtureWorkflow)
	ctx := context.Background()

	require.ErrorContains(t, r.ReplayHistoryJSON(ctx, nil), "empty")
	require.ErrorContains(t, r.ReplayHistoryJSON(ctx, []byte("{}")), "no events")
	require.ErrorContains(t, r.ReplayHistoryFile(ctx, "does-not-exist.json"), "reading history")

	// A history whose first event is not a start is structurally impossible and
	// replaying it would prove nothing.
	bad := history.History{{
		ID: 1, Time: time.Now(),
		Attrs: history.WorkflowTaskScheduledAttributes{TaskQueue: "q", Attempt: 1},
	}}
	require.ErrorContains(t, r.ReplayHistory(ctx, bad), "not well formed")
}

func TestReplayerRejectsAnUnregisteredWorkflow(t *testing.T) {
	h := recordProductionHistory(t)
	r := NewReplayer(ReplayOptions{})
	err := r.ReplayHistory(context.Background(), h)
	require.ErrorIs(t, err, ErrNotRegistered)
}

func TestDecodeHistoryJSONRoundTrip(t *testing.T) {
	h := recordProductionHistory(t)

	arrayForm, err := json.Marshal(h)
	require.NoError(t, err)
	decoded, err := DecodeHistoryJSON(arrayForm)
	require.NoError(t, err)
	require.Equal(t, len(h), len(decoded))
	require.Equal(t, h[0].Type(), decoded[0].Type())
	require.Equal(t, h.LastEventID(), decoded.LastEventID())
}
