package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/api"
)

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	t.Run("a service is required", func(t *testing.T) {
		_, err := New(Config{})
		require.ErrorContains(t, err, "service is required")
	})

	t.Run("a poll longer than the idle timeout is refused", func(t *testing.T) {
		// Not a style preference: the connection would be torn down under an
		// in-flight request, which looks to a worker exactly like a lost task.
		_, err := New(Config{
			Service:         &stubService{},
			MaxPollDuration: 2 * time.Minute,
			IdleTimeout:     time.Minute,
		})
		require.ErrorContains(t, err, "must be below the idle timeout")
	})

	t.Run("defaults are applied", func(t *testing.T) {
		s, err := New(Config{Service: &stubService{}})
		require.NoError(t, err)
		require.Equal(t, DefaultMaxPollDuration, s.maxPoll)
		require.Equal(t, int64(DefaultMaxRequestBytes), s.maxRequestBytes)
		require.Equal(t, DefaultShutdownTimeout, s.ShutdownTimeout())
	})
}

func TestStartAndShutdown(t *testing.T) {
	t.Parallel()

	srv, err := New(Config{
		Service:    &stubService{},
		Addr:       "127.0.0.1:0",
		ReadyCheck: func(context.Context) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start())

	// Port zero means the bound address is only knowable after Start.
	require.NotEqual(t, "127.0.0.1:0", srv.Addr())

	served := make(chan error, 1)
	go func() { served <- srv.Wait() }()

	resp, err := http.Get("http://" + srv.Addr() + api.PathReady)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case err := <-served:
		require.NoError(t, err, "a graceful stop is not an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}

func TestStartReportsBindFailuresSynchronously(t *testing.T) {
	t.Parallel()

	first, err := New(Config{Service: &stubService{}, Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, first.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = first.Shutdown(ctx)
	})

	// A bind failure must reach the caller, not a goroutine: a process that
	// tells its supervisor it started and then logs "address in use" from a
	// background goroutine is a process that stays "healthy" while serving
	// nothing.
	second, err := New(Config{Service: &stubService{}, Addr: first.Addr()})
	require.NoError(t, err)
	require.ErrorContains(t, second.Start(), "listen on")
}

func TestShutdownReleasesParkedPollsBeforeDraining(t *testing.T) {
	t.Parallel()

	polling := make(chan struct{})
	released := make(chan struct{})
	srv, err := New(Config{
		Service: &stubService{
			pollWorkflow: func(ctx context.Context, _ api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
				close(polling)
				<-ctx.Done()
				close(released)
				return api.WorkflowTask{}, ctx.Err()
			},
		},
		Addr: "127.0.0.1:0",
		// Long enough that a shutdown which waited for the poll to expire would
		// blow the test's deadline.
		MaxPollDuration: 60 * time.Second,
		ReadyCheck:      func(context.Context) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start())

	go func() {
		body := strings.NewReader(`{"task_queue":"orders"}`)
		resp, err := http.Post("http://"+srv.Addr()+api.PathPollWorkflowTask, "application/json", body)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	select {
	case <-polling:
	case <-time.After(5 * time.Second):
		t.Fatal("the poll never reached the service")
	}

	// /ready must fail as soon as the drain starts, so a load balancer stops
	// routing here while the process finishes what it already accepted.
	readyDuringDrain := make(chan int, 1)
	go func() {
		<-polling
		time.Sleep(20 * time.Millisecond)
		resp, err := http.Get("http://" + srv.Addr() + api.PathReady)
		if err != nil {
			readyDuringDrain <- 0
			return
		}
		defer resp.Body.Close()
		readyDuringDrain <- resp.StatusCode
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release the parked poll")
	}
	require.Contains(t, []int{http.StatusServiceUnavailable, 0}, <-readyDuringDrain)
}

func TestReadyFailsWhenNoCheckIsConfigured(t *testing.T) {
	t.Parallel()

	// A server that cannot answer "is my store reachable" is not ready. The
	// alternative -- defaulting to healthy -- means a misconfigured deployment
	// silently reports readiness it never verified.
	h := newHarness(t, &stubService{}, func(c *Config) { c.ReadyCheck = nil })
	resp := h.do(t, h.request(t, http.MethodGet, api.PathReady, nil))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Contains(t, decodeAPIError(t, resp).Message, "no readiness check")
}

func TestEveryProtocolPathIsRouted(t *testing.T) {
	t.Parallel()

	// A path constant with no handler behind it is the kind of omission that
	// only shows up when a worker calls it in production.
	h := newHarness(t, &stubService{})
	paths := []string{
		api.PathStartWorkflow, api.PathSignalWorkflow, api.PathSignalWithStart,
		api.PathCancelWorkflow, api.PathTerminateWorkflow, api.PathDescribeWorkflow,
		api.PathGetHistory, api.PathListWorkflows,
		api.PathPollWorkflowTask, api.PathCompleteWorkflow, api.PathFailWorkflowTask,
		api.PathPollActivityTask, api.PathCompleteActivity, api.PathFailActivity,
		api.PathCancelActivity, api.PathHeartbeatActivity,
	}
	for _, path := range paths {
		resp := h.post(t, path, map[string]any{})
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"), path)
	}

	for _, path := range []string{api.PathHealth, api.PathReady, api.PathMetrics} {
		resp := h.do(t, h.request(t, http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
	}
}

func TestResponsesAreValidJSONDocuments(t *testing.T) {
	t.Parallel()

	h := newHarness(t, &stubService{})
	resp := h.post(t, api.PathStartWorkflow, api.StartWorkflowRequest{WorkflowID: "a"})
	body := readAllString(t, resp)

	require.True(t, json.Valid([]byte(body)))
	// Exactly one document, newline terminated: a client reading line by line
	// gets whole values.
	require.Equal(t, 1, strings.Count(body, "\n"))
	require.True(t, strings.HasSuffix(body, "\n"))
}
