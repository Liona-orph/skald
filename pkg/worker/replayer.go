package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	internalwf "github.com/Liona-orph/skald/internal/workflow"
	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// ReplayOptions configure a Replayer.
type ReplayOptions struct {
	// DataConverter must match the one the original worker used, or every
	// payload comparison is meaningless. Defaults to JSON.
	DataConverter skald.DataConverter
	// Logger receives replayer diagnostics. Workflow-side logging is suppressed
	// throughout, because every task of a replay is by definition a replay.
	Logger *slog.Logger
	// Namespace is recorded in workflow.GetInfo during the replay.
	Namespace string
}

// Replayer re-executes a recorded history against the workflow code in the
// current binary and reports any disagreement.
//
// This is the tool to run in CI. Export a handful of representative production
// histories, commit them, and replay them on every pull request that touches a
// workflow: a change that would break in-flight executions then fails the build
// instead of failing at 3am, halfway through a deploy, with a thousand
// executions wedged and no way forward but a rollback.
//
//	r := worker.NewReplayer(worker.ReplayOptions{})
//	r.RegisterWorkflow(TransferMoney)
//	if err := r.ReplayHistoryFile(ctx, "testdata/transfer.json"); err != nil {
//	    t.Fatal(err)
//	}
//
// Replay is entirely offline: no server, no network, no side effects. Side
// effects and local activities are served from the markers already in the
// history rather than re-executed, which is what makes running it against
// production histories safe.
type Replayer struct {
	registry *Registry
	opts     ReplayOptions
	log      *slog.Logger
}

// NewReplayer returns a replayer with no workflows registered.
func NewReplayer(opts ReplayOptions) *Replayer {
	if opts.DataConverter == nil {
		opts.DataConverter = skald.JSONConverter{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Namespace == "" {
		opts.Namespace = skald.DefaultNamespace
	}
	return &Replayer{registry: NewRegistry(opts.DataConverter), opts: opts, log: opts.Logger}
}

// RegisterWorkflow registers a workflow implementation, panicking on a bad
// signature exactly as the worker does.
func (r *Replayer) RegisterWorkflow(fn any) {
	if err := r.registry.RegisterWorkflow(fn, RegisterOptions{}); err != nil {
		panic(err)
	}
}

// RegisterWorkflowWithOptions is RegisterWorkflow with an explicit name.
func (r *Replayer) RegisterWorkflowWithOptions(fn any, opts RegisterOptions) {
	if err := r.registry.RegisterWorkflow(fn, opts); err != nil {
		panic(err)
	}
}

// ReplayHistoryFile reads a JSON history from disk and replays it.
func (r *Replayer) ReplayHistoryFile(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("worker: reading history %s: %w", path, err)
	}
	h, err := DecodeHistoryJSON(data)
	if err != nil {
		return fmt.Errorf("worker: decoding history %s: %w", path, err)
	}
	return r.ReplayHistory(ctx, h)
}

// ReplayHistoryJSON replays a history from its JSON encoding.
func (r *Replayer) ReplayHistoryJSON(ctx context.Context, data []byte) error {
	h, err := DecodeHistoryJSON(data)
	if err != nil {
		return err
	}
	return r.ReplayHistory(ctx, h)
}

// DecodeHistoryJSON accepts either a bare event array or the object returned by
// the GetHistory endpoint.
//
// Accepting both matters in practice: the array is what a hand-written fixture
// looks like, and the object is what falls out of `skald workflow show --json`
// or a curl against the API. Requiring the operator to reshape it by hand is
// exactly the friction that stops anyone from adding the CI check at all.
func DecodeHistoryJSON(data []byte) (history.History, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("worker: history is empty")
	}
	if trimmed[0] == '[' {
		var h history.History
		if err := json.Unmarshal(trimmed, &h); err != nil {
			return nil, fmt.Errorf("worker: decoding history array: %w", err)
		}
		return h, nil
	}
	var resp api.GetHistoryResponse
	if err := json.Unmarshal(trimmed, &resp); err != nil {
		return nil, fmt.Errorf("worker: decoding history object: %w", err)
	}
	if len(resp.Events) == 0 {
		return nil, errors.New("worker: history object carries no events")
	}
	return resp.Events, nil
}

// ReplayHistory re-executes h and reports any non-determinism.
//
// A nil return means every command the workflow produced during the replay
// matched, in order, the effect the history recorded -- which is the strongest
// statement available short of running the workflow again for real.
func (r *Replayer) ReplayHistory(ctx context.Context, h history.History) error {
	if err := h.Validate(); err != nil {
		return fmt.Errorf("worker: the history is not well formed, so a replay of it would prove nothing: %w", err)
	}
	started, ok := h.StartedAttributes()
	if !ok {
		return errors.New("worker: history does not begin with a WorkflowExecutionStarted event")
	}
	fn, err := r.registry.WorkflowFunc(started.WorkflowType)
	if err != nil {
		return err
	}

	exec, err := internalwf.NewExecutor(internalwf.ExecutorOptions{
		Fn:        fn,
		Converter: r.opts.DataConverter,
		Logger:    r.opts.Logger,
		// Every task in an offline replay is a replay, including the last one.
		// Without this a workflow whose final task recorded a side effect would
		// run that side effect again -- against production data, from a CI job.
		ReplayOnly: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = exec.Close() }()

	exec.SetExecutionInfo(r.opts.Namespace, executionFromHistory(h))

	// StartedEventID zero means "no task is live": every workflow task in the
	// history is verified against the events that followed it, and nothing is
	// treated as new work.
	if _, err := exec.ProcessTask(api.WorkflowTask{
		Namespace:    r.opts.Namespace,
		WorkflowType: started.WorkflowType,
		TaskQueue:    started.TaskQueue,
		History:      h,
	}); err != nil {
		return err
	}
	r.log.Info("replay succeeded",
		"workflow_type", started.WorkflowType, "events", len(h))
	return nil
}

// executionFromHistory recovers what identity it can from the history alone.
// A history file carries no workflow or run id of its own, so only the first
// execution run id -- recorded in event 1 -- is available.
func executionFromHistory(h history.History) skald.WorkflowExecution {
	started, _ := h.StartedAttributes()
	return skald.WorkflowExecution{RunID: started.FirstExecutionRunID}
}
