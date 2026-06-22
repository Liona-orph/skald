package worker

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalwf "github.com/skald-io/skald/internal/workflow"
	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/skald"
)

func encodeArgsForTest(args ...any) (*skald.Payload, error) {
	return internalwf.EncodeArgs(skald.JSONConverter{}, args)
}

func goroutineBaseline() int {
	runtime.GC()
	return runtime.NumGoroutine()
}

func TestOptionsDefaults(t *testing.T) {
	t.Parallel()
	var o Options
	o.applyDefaults("identity")

	require.Equal(t, skald.DefaultNamespace, o.Namespace)
	require.Equal(t, "identity", o.Identity)
	require.Equal(t, DefaultMaxConcurrentWorkflowTasks, o.MaxConcurrentWorkflowTasks)
	require.Equal(t, DefaultStickyCacheSize, o.StickyCacheSize)
	require.NotNil(t, o.DataConverter)
	require.NotNil(t, o.Logger)
}

// TestOptionsNeverPollForMoreWorkThanCanRun guards a subtle misconfiguration: a
// poller that takes a task it has no slot for has removed it from the queue and
// made it look, to every other worker, like work already in progress.
func TestOptionsNeverPollForMoreWorkThanCanRun(t *testing.T) {
	t.Parallel()
	o := Options{MaxConcurrentActivityTasks: 2, ActivityTaskPollers: 10}
	o.applyDefaults("id")
	require.Equal(t, 2, o.ActivityTaskPollers)
}

func TestBackoffGrowsAndResets(t *testing.T) {
	t.Parallel()
	b := newBackoff(10*time.Millisecond, 100*time.Millisecond)

	require.Equal(t, 10*time.Millisecond, b.next())
	require.Equal(t, 20*time.Millisecond, b.next())
	require.Equal(t, 40*time.Millisecond, b.next())
	require.Equal(t, 80*time.Millisecond, b.next())
	require.Equal(t, 100*time.Millisecond, b.next(), "growth is capped")
	require.Equal(t, 100*time.Millisecond, b.next())

	b.reset()
	require.Equal(t, 10*time.Millisecond, b.next())
}

// failingService fails every poll, which is what a worker sees when the server
// is down.
type failingService struct {
	api.Service
	polls chan struct{}
}

func (s *failingService) PollWorkflowTask(ctx context.Context, _ api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	select {
	case s.polls <- struct{}{}:
	default:
	}
	return api.WorkflowTask{}, errors.New("server unavailable")
}

func (s *failingService) PollActivityTask(ctx context.Context, _ api.PollActivityTaskRequest) (api.ActivityTask, error) {
	<-ctx.Done()
	return api.ActivityTask{}, ctx.Err()
}

// TestWorkerBacksOffAndStopsCleanlyWhenTheServerIsDown checks that a worker
// against a dead server neither spins nor wedges its own shutdown.
func TestWorkerBacksOffAndStopsCleanlyWhenTheServerIsDown(t *testing.T) {
	svc := &failingService{polls: make(chan struct{}, 1)}
	w := New(svc, "queue", Options{
		WorkflowTaskPollers: 1,
		ActivityTaskPollers: 1,
		MaxPollBackoff:      20 * time.Millisecond,
		Logger:              testLogger(t, 100), // above every level: silence
	})
	require.NoError(t, w.Start())

	select {
	case <-svc.polls:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never polled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, w.Stop(ctx))
}

func TestWorkerRejectsRestartAndDoubleStart(t *testing.T) {
	t.Parallel()
	svc := &failingService{polls: make(chan struct{}, 1)}
	w := New(svc, "queue", Options{
		WorkflowTaskPollers:   1,
		DisableActivityWorker: true,
		Logger:                testLogger(t, 100),
	})
	require.NoError(t, w.Start())
	require.ErrorContains(t, w.Start(), "already started")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, w.Stop(ctx))
	require.ErrorContains(t, w.Start(), "cannot restart")
}

func TestWorkerRefusesToStartWithBothHalvesDisabled(t *testing.T) {
	t.Parallel()
	w := New(nil, "queue", Options{DisableWorkflowWorker: true, DisableActivityWorker: true})
	require.ErrorContains(t, w.Start(), "would do nothing")
}

func TestWorkerRejectsAnInvalidTaskQueue(t *testing.T) {
	t.Parallel()
	w := New(nil, "", Options{})
	require.ErrorIs(t, w.Start(), skald.ErrInvalidIdentifier)
}

func TestActivityHelpersRefuseNonActivityContexts(t *testing.T) {
	t.Parallel()
	require.False(t, IsActivityContext(context.Background()))
	require.Panics(t, func() { GetActivityInfo(context.Background()) })
	require.Panics(t, func() { RecordHeartbeat(context.Background()) })
	require.False(t, HasHeartbeatDetails(context.Background()))
	require.Error(t, GetHeartbeatDetails(context.Background(), nil))
}
