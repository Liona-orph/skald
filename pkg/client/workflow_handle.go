package client

import (
	"context"
	"fmt"
	"time"

	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// WorkflowOptions describes a workflow to start.
//
// It is a friendlier api.StartWorkflowRequest: the input is a Go value rather
// than a pre-encoded payload, and the fields a caller almost never sets are
// absent. Anything it cannot express is available by calling StartWorkflow
// directly.
type WorkflowOptions struct {
	// ID is the workflow's business identifier. Leaving it empty gets a UUID,
	// which is convenient for an ad-hoc start and gives up the property that
	// makes workflow IDs valuable: a natural key such as "order-1234" makes the
	// start idempotent against the *business* operation, not merely against a
	// retry of one API call.
	ID        string
	Type      string
	TaskQueue string
	Namespace string

	ExecutionTimeout time.Duration
	RunTimeout       time.Duration
	TaskTimeout      time.Duration

	RetryPolicy  *skald.RetryPolicy
	CronSchedule string
	ReusePolicy  string
	Memo         map[string]string
	SearchAttrs  map[string]string

	// RequestID deduplicates a retried start. The client generates one when it
	// is empty, so a caller only sets this to make a start idempotent across
	// process restarts as well as across retries.
	RequestID string
}

func (o WorkflowOptions) validate() error {
	if o.Type == "" {
		return &api.Error{Code: api.CodeInvalidArgument, Message: "client: workflow type is required"}
	}
	if o.TaskQueue == "" {
		return &api.Error{Code: api.CodeInvalidArgument, Message: "client: task queue is required"}
	}
	return nil
}

func (c *Client) startRequest(opts WorkflowOptions, arg any) (api.StartWorkflowRequest, error) {
	if err := opts.validate(); err != nil {
		return api.StartWorkflowRequest{}, err
	}
	id := opts.ID
	if id == "" {
		id = c.newRequestID()
	}
	input, err := c.converter.ToPayload(arg)
	if err != nil {
		return api.StartWorkflowRequest{}, &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: fmt.Sprintf("client: encode workflow input: %v", err),
		}
	}
	return api.StartWorkflowRequest{
		Namespace:        opts.Namespace,
		WorkflowID:       id,
		WorkflowType:     opts.Type,
		TaskQueue:        opts.TaskQueue,
		Input:            input,
		ExecutionTimeout: opts.ExecutionTimeout,
		RunTimeout:       opts.RunTimeout,
		TaskTimeout:      opts.TaskTimeout,
		RetryPolicy:      opts.RetryPolicy,
		CronSchedule:     opts.CronSchedule,
		RequestID:        opts.RequestID,
		ReusePolicy:      opts.ReusePolicy,
		Memo:             opts.Memo,
		SearchAttrs:      opts.SearchAttrs,
	}, nil
}

// ExecuteWorkflow starts a workflow and returns a handle to it.
//
// It returns as soon as the start is durable, not when the workflow finishes.
// Wait for the result with Handle.Result, which is a separate call precisely so
// that a caller can start work and walk away.
func (c *Client) ExecuteWorkflow(ctx context.Context, opts WorkflowOptions, arg any) (*WorkflowHandle, error) {
	req, err := c.startRequest(opts, arg)
	if err != nil {
		return nil, err
	}
	resp, err := c.StartWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.NewHandle(req.Namespace, req.WorkflowID, resp.RunID), nil
}

// SignalWithStart delivers a signal, starting the workflow first if it is not
// already running.
func (c *Client) SignalWithStart(ctx context.Context, opts WorkflowOptions, arg any, signalName string, signalArg any) (*WorkflowHandle, error) {
	start, err := c.startRequest(opts, arg)
	if err != nil {
		return nil, err
	}
	if signalName == "" {
		return nil, &api.Error{Code: api.CodeInvalidArgument, Message: "client: signal name is required"}
	}
	signalInput, err := c.converter.ToPayload(signalArg)
	if err != nil {
		return nil, &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: fmt.Sprintf("client: encode signal input: %v", err),
		}
	}
	resp, err := c.SignalWithStartWorkflow(ctx, api.SignalWithStartRequest{
		Start:       start,
		SignalName:  signalName,
		SignalInput: signalInput,
	})
	if err != nil {
		return nil, err
	}
	return c.NewHandle(start.Namespace, start.WorkflowID, resp.RunID), nil
}

// NewHandle returns a handle to an existing execution.
//
// An empty runID means "whichever run is current", which is what an operator
// wants when they know only the workflow ID. A specific run ID pins the handle
// to that run, so a continue-as-new does not silently change what Describe
// reports.
func (c *Client) NewHandle(namespace, workflowID, runID string) *WorkflowHandle {
	if namespace == "" {
		namespace = c.namespace
	}
	return &WorkflowHandle{c: c, namespace: namespace, workflowID: workflowID, runID: runID}
}

// WorkflowHandle is an ergonomic reference to one execution.
type WorkflowHandle struct {
	c         *Client
	namespace string

	workflowID string
	runID      string
}

// Namespace returns the namespace this handle addresses.
func (h *WorkflowHandle) Namespace() string { return h.namespace }

// WorkflowID returns the workflow identifier.
func (h *WorkflowHandle) WorkflowID() string { return h.workflowID }

// RunID returns the pinned run, or the empty string when the handle follows
// whichever run is current.
func (h *WorkflowHandle) RunID() string { return h.runID }

// Describe returns the current state of the execution.
func (h *WorkflowHandle) Describe(ctx context.Context) (api.DescribeWorkflowResponse, error) {
	return h.c.DescribeWorkflow(ctx, h.namespace, h.workflowID, h.runID)
}

// Signal delivers an asynchronous message to the workflow.
func (h *WorkflowHandle) Signal(ctx context.Context, name string, arg any) error {
	if name == "" {
		return &api.Error{Code: api.CodeInvalidArgument, Message: "client: signal name is required"}
	}
	input, err := h.c.converter.ToPayload(arg)
	if err != nil {
		return &api.Error{Code: api.CodeInvalidArgument, Message: fmt.Sprintf("client: encode signal input: %v", err)}
	}
	return h.c.SignalWorkflow(ctx, api.SignalWorkflowRequest{
		Namespace:  h.namespace,
		WorkflowID: h.workflowID,
		RunID:      h.runID,
		SignalName: name,
		Input:      input,
	})
}

// Cancel asks the workflow to unwind.
//
// Cancellation is cooperative: the workflow observes the request on its next
// task and decides what to do, which is what lets it release resources and
// record a compensating action. Use Terminate when you need it to stop whether
// it likes it or not.
func (h *WorkflowHandle) Cancel(ctx context.Context, reason string) error {
	return h.c.CancelWorkflow(ctx, api.CancelWorkflowRequest{
		Namespace:  h.namespace,
		WorkflowID: h.workflowID,
		RunID:      h.runID,
		Reason:     reason,
	})
}

// Terminate ends the execution immediately.
//
// No workflow code runs in response, so nothing the workflow was holding is
// cleaned up. That is the difference from Cancel and the reason this should be
// the second thing you try.
func (h *WorkflowHandle) Terminate(ctx context.Context, reason string, details any) error {
	payload, err := h.c.converter.ToPayload(details)
	if err != nil {
		return &api.Error{Code: api.CodeInvalidArgument, Message: fmt.Sprintf("client: encode details: %v", err)}
	}
	return h.c.TerminateWorkflow(ctx, api.TerminateWorkflowRequest{
		Namespace:  h.namespace,
		WorkflowID: h.workflowID,
		RunID:      h.runID,
		Reason:     reason,
		Details:    payload,
	})
}

// History returns the run's complete history.
func (h *WorkflowHandle) History(ctx context.Context) (history.History, error) {
	var all history.History
	from := int64(1)
	for {
		resp, err := h.c.GetHistory(ctx, api.GetHistoryRequest{
			Namespace:   h.namespace,
			WorkflowID:  h.workflowID,
			RunID:       h.runID,
			FromEventID: from,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.Events...)
		if len(resp.Events) == 0 || resp.NextEventID <= from {
			return all, nil
		}
		from = resp.NextEventID
	}
}

// Result waits for the execution to finish and decodes its outcome.
//
// On success the workflow's return value is decoded into out, which may be nil
// if the caller does not want it. On any other terminal state the error is the
// typed one from pkg/skald -- *skald.ApplicationError, *skald.CanceledError,
// *skald.TerminatedError or *skald.TimeoutError -- so a caller can branch with
// errors.As instead of parsing a message.
//
// The wait is a long poll against the history, not a sleep loop: the server
// holds the request open until an event beyond the cursor exists, so a workflow
// that finishes in fifty milliseconds is observed in fifty milliseconds and one
// that takes a week costs one open connection.
//
// A continue-as-new is followed automatically to the successor run. That is
// what "the result of this workflow" means to a caller: continue-as-new is an
// implementation detail of how a long-running workflow keeps its history
// bounded, not an outcome. A handle pinned to a specific run still follows the
// chain -- pinning selects where to start reading, not where to stop.
func (h *WorkflowHandle) Result(ctx context.Context, out any) error {
	runID := h.runID
	for {
		terminal, next, err := h.awaitTerminal(ctx, runID)
		if err != nil {
			return err
		}
		if next != "" {
			runID = next
			continue
		}
		return h.decodeTerminal(terminal, out)
	}
}

// awaitTerminal long-polls one run until it reaches a terminal event.
//
// It returns either the terminal event, or the run ID of the successor when the
// run continued as new.
func (h *WorkflowHandle) awaitTerminal(ctx context.Context, runID string) (history.Event, string, error) {
	from := int64(1)
	for {
		resp, err := h.c.GetHistory(ctx, api.GetHistoryRequest{
			Namespace:   h.namespace,
			WorkflowID:  h.workflowID,
			RunID:       runID,
			FromEventID: from,
			WaitForNew:  true,
		})
		if err != nil {
			return history.Event{}, "", err
		}

		for _, ev := range resp.Events {
			if !ev.Type().Terminal() {
				continue
			}
			if attrs, ok := history.AttributesAs[history.WorkflowExecutionContinuedAsNewAttributes](ev); ok {
				return history.Event{}, attrs.NewRunID, nil
			}
			return ev, "", nil
		}

		if resp.NextEventID > from {
			from = resp.NextEventID
			continue
		}
		if resp.Status.Terminal() {
			// The run is closed and we have read to the end without seeing a
			// terminal event. That is a corrupt history rather than a state to
			// wait in, so say so instead of blocking forever.
			return history.Event{}, "", &api.Error{
				Code:    api.CodeInternal,
				Message: fmt.Sprintf("client: run %s is %s but its history has no terminal event", runID, resp.Status),
			}
		}
		// The poll expired with nothing new. Loop and wait again; the server
		// caps each wait so that the connection does not outlive a proxy.
	}
}

// decodeTerminal turns a terminal event into a result or a typed error.
func (h *WorkflowHandle) decodeTerminal(ev history.Event, out any) error {
	switch attrs := ev.Attrs.(type) {
	case history.WorkflowExecutionCompletedAttributes:
		if out == nil {
			return nil
		}
		if err := h.c.converter.FromPayload(attrs.Result, out); err != nil {
			return &api.Error{
				Code:    api.CodeInvalidArgument,
				Message: fmt.Sprintf("client: decode workflow result: %v", err),
			}
		}
		return nil

	case history.WorkflowExecutionFailedAttributes:
		if attrs.Failure != nil {
			return attrs.Failure
		}
		return &skald.ApplicationError{Message: "workflow failed"}

	case history.WorkflowExecutionCanceledAttributes:
		return &skald.CanceledError{Details: attrs.Details}

	case history.WorkflowExecutionTerminatedAttributes:
		return &skald.TerminatedError{Reason: attrs.Reason}

	case history.WorkflowExecutionTimedOutAttributes:
		return &skald.TimeoutError{Kind: attrs.Kind}
	}
	return &api.Error{
		Code:    api.CodeInternal,
		Message: fmt.Sprintf("client: unexpected terminal event %s", ev.Type()),
	}
}
