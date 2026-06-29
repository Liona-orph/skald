package commands

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/skald"
)

func (r *root) newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "workflow",
		Aliases: []string{"wf"},
		Short:   "Start, inspect and control workflow executions",
		Long: `Operations on workflow executions.

Every subcommand takes the workflow ID as its first argument and an optional
--run-id. Leave --run-id off and you address whichever run is current, which is
almost always what you want; pass it to pin a specific run of a workflow that
has continued-as-new or been retried.`,
	}
	cmd.AddCommand(
		r.newWorkflowStartCommand(),
		r.newWorkflowSignalCommand(),
		r.newWorkflowCancelCommand(),
		r.newWorkflowTerminateCommand(),
		r.newWorkflowDescribeCommand(),
		r.newWorkflowListCommand(),
		r.newWorkflowResultCommand(),
		r.newWorkflowHistoryCommand(),
		r.newWorkflowReplayCommand(),
	)
	return cmd
}

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

func (r *root) newWorkflowStartCommand() *cobra.Command {
	var (
		workflowID   string
		workflowType string
		taskQueue    string
		input        string
		reusePolicy  string
		cron         string
		requestID    string
		memo         []string
		execTimeout  time.Duration
		runTimeout   time.Duration
		taskTimeout  time.Duration
		wait         bool
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a workflow execution",
		Long: `Start a workflow execution.

The workflow ID is your business key. Use the identifier of the thing the
workflow is about -- "order-1234", not a UUID -- because that is what makes a
retried start idempotent and what you will search for later. One is generated if
you omit it, which is fine for a one-off and wrong for anything a machine calls.

Input is raw JSON, passed through to the workflow untouched. Prefix it with @ to
read from a file, or use - for stdin.`,
		Example: `  # start and walk away
  skaldctl workflow start --type OrderWorkflow --task-queue orders --id order-1234 \
      --input '{"customer":"c-99","total":4200}'

  # start and block until it finishes, exiting 2 if the workflow fails
  skaldctl workflow start --type Backfill --task-queue batch --wait`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if workflowType == "" {
				return errors.New("--type is required: which workflow should run?")
			}
			if taskQueue == "" {
				return errors.New("--task-queue is required: which workers should run it?")
			}
			payload, err := parsePayloadFlag(input)
			if err != nil {
				return err
			}
			memoMap, err := parseKeyValues(memo, "--memo")
			if err != nil {
				return err
			}

			c, err := r.Client()
			if err != nil {
				return err
			}
			ctx := commandContext(cmd)

			req := api.StartWorkflowRequest{
				Namespace:        r.opts.Namespace,
				WorkflowID:       workflowID,
				WorkflowType:     workflowType,
				TaskQueue:        taskQueue,
				Input:            payload,
				ExecutionTimeout: execTimeout,
				RunTimeout:       runTimeout,
				TaskTimeout:      taskTimeout,
				CronSchedule:     cron,
				RequestID:        requestID,
				ReusePolicy:      reusePolicy,
				Memo:             memoMap,
			}
			if req.WorkflowID == "" {
				// The client would generate one, but the CLI has to know the ID
				// to report it and to wait on it.
				req.WorkflowID = "wf-" + time.Now().UTC().Format("20060102T150405.000000000")
			}

			resp, err := c.StartWorkflow(ctx, req)
			if err != nil {
				return err
			}

			out := startResult{
				Namespace:  r.opts.Namespace,
				WorkflowID: req.WorkflowID,
				RunID:      resp.RunID,
				Started:    resp.Started,
			}
			if r.opts.Format == FormatJSON {
				if err := r.printer.JSON(out); err != nil {
					return err
				}
			} else {
				verb := "started"
				if !resp.Started {
					// The request ID deduplicated: say so, because "started"
					// when nothing started is how a duplicate goes unnoticed.
					verb = "already running, reusing"
				}
				r.printer.Printf("%s %s\n  workflow_id  %s\n  run_id       %s\n",
					verb, r.printer.bold(workflowType), out.WorkflowID, out.RunID)
			}

			if !wait {
				return nil
			}
			return r.printResult(cmd, out.WorkflowID, out.RunID)
		},
	}

	f := cmd.Flags()
	f.StringVar(&workflowID, "id", "", "workflow ID (your business key; generated when omitted)")
	f.StringVar(&workflowType, "type", "", "workflow type registered by the worker (required)")
	f.StringVar(&taskQueue, "task-queue", "", "task queue the workers poll (required)")
	f.StringVar(&input, "input", "", "workflow input as JSON, @file, or - for stdin")
	f.StringVar(&reusePolicy, "id-reuse-policy", "", "AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate or TerminateIfRunning")
	f.StringVar(&cron, "cron", "", "cron schedule (recorded, not yet scheduled by the server)")
	f.StringVar(&requestID, "request-id", "", "idempotency key; reusing one returns the original run")
	f.StringArrayVar(&memo, "memo", nil, "key=value metadata, repeatable")
	f.DurationVar(&execTimeout, "execution-timeout", 0, "bound on the whole workflow, across retries and continue-as-new")
	f.DurationVar(&runTimeout, "run-timeout", 0, "bound on a single run")
	f.DurationVar(&taskTimeout, "task-timeout", 0, "bound on one workflow task held by a worker")
	f.BoolVar(&wait, "wait", false, "block until the workflow finishes and print its result")
	return cmd
}

type startResult struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Started    bool   `json:"started"`
}

// ---------------------------------------------------------------------------
// signal / cancel / terminate
// ---------------------------------------------------------------------------

func (r *root) newWorkflowSignalCommand() *cobra.Command {
	var (
		runID  string
		name   string
		input  string
		nameFl = "--name"
	)
	cmd := &cobra.Command{
		Use:   "signal <workflow-id>",
		Short: "Send a signal to a running workflow",
		Long: `Deliver an asynchronous message to a running workflow.

Signals are durable: once the server accepts one it will be delivered even if
every worker is down. They are the only way information gets into a running
workflow from outside.`,
		Example: `  skaldctl workflow signal order-1234 --name approve --input '{"by":"alice"}'`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("%s is required: which signal handler should receive this?", nameFl)
			}
			payload, err := parsePayloadFlag(input)
			if err != nil {
				return err
			}
			c, err := r.Client()
			if err != nil {
				return err
			}
			if err := c.SignalWorkflow(commandContext(cmd), api.SignalWorkflowRequest{
				Namespace:  r.opts.Namespace,
				WorkflowID: args[0],
				RunID:      runID,
				SignalName: name,
				Input:      payload,
			}); err != nil {
				return err
			}
			return r.acknowledge("signaled", args[0], map[string]string{"signal": name})
		},
	}
	f := cmd.Flags()
	f.StringVar(&runID, "run-id", "", "pin a specific run")
	f.StringVar(&name, "name", "", "signal name (required)")
	f.StringVar(&input, "input", "", "signal payload as JSON, @file, or - for stdin")
	return cmd
}

func (r *root) newWorkflowCancelCommand() *cobra.Command {
	var runID, reason string
	cmd := &cobra.Command{
		Use:   "cancel <workflow-id>",
		Short: "Ask a workflow to unwind",
		Long: `Request cancellation.

Cancellation is cooperative: the workflow sees the request on its next task and
gets to run its cleanup logic -- release the inventory hold, refund the charge,
whatever it needs. It may also decline. Reach for this first; reach for
terminate only when it does not work.`,
		Example: `  skaldctl workflow cancel order-1234 --reason "customer changed their mind"`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := r.Client()
			if err != nil {
				return err
			}
			if err := c.CancelWorkflow(commandContext(cmd), api.CancelWorkflowRequest{
				Namespace:  r.opts.Namespace,
				WorkflowID: args[0],
				RunID:      runID,
				Reason:     reason,
			}); err != nil {
				return err
			}
			return r.acknowledge("cancellation requested for", args[0], nil)
		},
	}
	cmd.Flags().StringVar(&runID, "run-id", "", "pin a specific run")
	cmd.Flags().StringVar(&reason, "reason", "", "why; recorded in the history")
	return cmd
}

func (r *root) newWorkflowTerminateCommand() *cobra.Command {
	var runID, reason, details string
	cmd := &cobra.Command{
		Use:   "terminate <workflow-id>",
		Short: "Stop a workflow immediately, without cleanup",
		Long: `Terminate an execution.

No workflow code runs in response. Nothing the workflow was holding gets
released, no compensation runs, no cleanup happens -- the execution simply stops
existing as a running thing. Use cancel unless you have already tried it or you
know the workflow is wedged.

Always pass --reason. The next person to read this history is trying to work out
why a workflow they depended on stopped, and "terminated by 3am pager, see
INC-4471" is the difference between a minute and an hour.`,
		Example: `  skaldctl workflow terminate order-1234 --reason "stuck on dead payment provider, INC-4471"`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload, err := parsePayloadFlag(details)
			if err != nil {
				return err
			}
			c, err := r.Client()
			if err != nil {
				return err
			}
			if err := c.TerminateWorkflow(commandContext(cmd), api.TerminateWorkflowRequest{
				Namespace:  r.opts.Namespace,
				WorkflowID: args[0],
				RunID:      runID,
				Reason:     reason,
				Details:    payload,
			}); err != nil {
				return err
			}
			return r.acknowledge("terminated", args[0], nil)
		},
	}
	cmd.Flags().StringVar(&runID, "run-id", "", "pin a specific run")
	cmd.Flags().StringVar(&reason, "reason", "", "why; recorded in the history")
	cmd.Flags().StringVar(&details, "details", "", "structured detail as JSON, @file, or - for stdin")
	return cmd
}

// acknowledge prints the confirmation of a mutating command.
func (r *root) acknowledge(verb, workflowID string, extra map[string]string) error {
	if r.opts.Format == FormatJSON {
		out := map[string]string{"result": strings.ReplaceAll(verb, " ", "_"), "workflow_id": workflowID}
		for k, v := range extra {
			out[k] = v
		}
		return r.printer.JSON(out)
	}
	r.printer.Printf("%s %s\n", verb, r.printer.bold(workflowID))
	return nil
}

// ---------------------------------------------------------------------------
// describe
// ---------------------------------------------------------------------------

func (r *root) newWorkflowDescribeCommand() *cobra.Command {
	var runID string
	cmd := &cobra.Command{
		Use:   "describe <workflow-id>",
		Short: "Show the current state of one execution",
		Long: `Summarise one execution: status, timings, and what it is waiting on.

The pending activities and timers are the part to read first. A workflow that
looks stuck is nearly always waiting on exactly one of them, and the attempt
count next to a pending activity tells you whether it is retrying.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := r.Client()
			if err != nil {
				return err
			}
			resp, err := c.DescribeWorkflow(commandContext(cmd), r.opts.Namespace, args[0], runID)
			if err != nil {
				return err
			}
			if r.opts.Format == FormatJSON {
				return r.printer.JSON(resp)
			}
			r.renderDescribe(resp)
			return nil
		},
	}
	cmd.Flags().StringVar(&runID, "run-id", "", "pin a specific run")
	return cmd
}

func (r *root) renderDescribe(d api.DescribeWorkflowResponse) {
	now := r.printer.Now()
	p := r.printer

	rows := [][]string{
		{"workflow_id", d.WorkflowID},
		{"run_id", d.RunID},
		{"type", d.WorkflowType},
		{"namespace", d.Namespace},
		{"task_queue", d.TaskQueue},
		{"status", p.StatusColor(d.Status)},
		{"started", fmt.Sprintf("%s (%s)", d.StartedAt.Local().Format(time.RFC3339), Relative(now, d.StartedAt))},
	}
	if d.ClosedAt != nil {
		rows = append(rows,
			[]string{"closed", fmt.Sprintf("%s (%s)", d.ClosedAt.Local().Format(time.RFC3339), Relative(now, *d.ClosedAt))},
			[]string{"duration", CompactDuration(d.ClosedAt.Sub(d.StartedAt))},
		)
	} else {
		rows = append(rows, []string{"running_for", CompactDuration(now.Sub(d.StartedAt))})
	}
	rows = append(rows, []string{"history_length", fmt.Sprintf("%d events", d.HistoryLength)})
	if d.FirstExecutionRunID != "" && d.FirstExecutionRunID != d.RunID {
		rows = append(rows, []string{"first_run_id", d.FirstExecutionRunID})
	}
	for k, v := range d.Memo {
		rows = append(rows, []string{"memo." + k, v})
	}
	_ = p.Table(nil, rows)

	if len(d.PendingActivities) > 0 {
		p.Printf("\n%s\n", p.bold("PENDING ACTIVITIES"))
		activityRows := make([][]string, 0, len(d.PendingActivities))
		for _, a := range d.PendingActivities {
			state := "queued"
			if a.Started {
				state = "running"
			}
			row := []string{
				fmt.Sprintf("%d", a.ScheduledEventID),
				a.ActivityType,
				a.ActivityID,
				state,
				fmt.Sprintf("attempt %d", a.Attempt),
				Relative(now, a.ScheduledAt),
			}
			if a.LastFailure != nil {
				row = append(row, p.red(failureText(a.LastFailure)))
			}
			activityRows = append(activityRows, row)
		}
		_ = p.Table([]string{"EVENT", "TYPE", "ID", "STATE", "ATTEMPT", "SCHEDULED", "LAST FAILURE"}, activityRows)
	}

	if len(d.PendingTimers) > 0 {
		p.Printf("\n%s\n", p.bold("PENDING TIMERS"))
		timerRows := make([][]string, 0, len(d.PendingTimers))
		for _, t := range d.PendingTimers {
			timerRows = append(timerRows, []string{
				fmt.Sprintf("%d", t.StartedEventID),
				t.TimerID,
				Relative(now, t.FireAt),
			})
		}
		_ = p.Table([]string{"EVENT", "TIMER", "FIRES"}, timerRows)
	}
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

func (r *root) newWorkflowListCommand() *cobra.Command {
	var (
		workflowType string
		status       string
		workflowID   string
		limit        int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List workflow executions",
		Long: `List executions in a namespace.

--status takes the values Skald uses in histories and metrics: RUNNING,
COMPLETED, FAILED, CANCELED, TERMINATED, TIMED_OUT, CONTINUED_AS_NEW.`,
		Example: `  skaldctl workflow list --status RUNNING
  skaldctl workflow list --type OrderWorkflow --status FAILED --limit 50`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Validated before the client is built: a typo'd status must say
			// "unknown workflow status" rather than return an empty page that
			// reads as "nothing is running".
			if status != "" {
				var parsed skald.WorkflowStatus
				if err := parsed.UnmarshalText([]byte(strings.ToUpper(status))); err != nil {
					return fmt.Errorf("--status: %w", err)
				}
				status = parsed.String()
			}

			c, err := r.Client()
			if err != nil {
				return err
			}
			ctx := commandContext(cmd)
			var (
				all   []api.DescribeWorkflowResponse
				token string
			)
			for {
				resp, err := c.ListWorkflows(ctx, api.ListWorkflowsRequest{
					Namespace:    r.opts.Namespace,
					WorkflowID:   workflowID,
					WorkflowType: workflowType,
					Status:       status,
					PageSize:     pageSizeFor(limit, len(all)),
					PageToken:    token,
				})
				if err != nil {
					return err
				}
				all = append(all, resp.Executions...)
				token = resp.NextPageToken
				if token == "" || (limit > 0 && len(all) >= limit) {
					break
				}
			}
			if limit > 0 && len(all) > limit {
				all = all[:limit]
			}

			if r.opts.Format == FormatJSON {
				return r.printer.JSON(api.ListWorkflowsResponse{Executions: all, NextPageToken: token})
			}
			r.renderList(all)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&workflowType, "type", "", "filter by workflow type")
	f.StringVar(&status, "status", "", "filter by status")
	f.StringVar(&workflowID, "id", "", "filter by workflow ID")
	f.IntVar(&limit, "limit", 100, "maximum executions to show; 0 for all")
	return cmd
}

// pageSizeFor asks for only as much as is still wanted, so that --limit 5
// against a million executions is one small query rather than a full scan.
func pageSizeFor(limit, have int) int {
	if limit <= 0 {
		return 0
	}
	if remaining := limit - have; remaining < 1000 {
		return remaining
	}
	return 1000
}

func (r *root) renderList(items []api.DescribeWorkflowResponse) {
	if len(items) == 0 {
		r.printer.Printf("no executions matched\n")
		return
	}
	now := r.printer.Now()
	rows := make([][]string, 0, len(items))
	for _, d := range items {
		age := Relative(now, d.StartedAt)
		if d.ClosedAt != nil {
			age = CompactDuration(d.ClosedAt.Sub(d.StartedAt))
		}
		rows = append(rows, []string{
			d.WorkflowID,
			d.WorkflowType,
			r.printer.StatusColor(d.Status),
			d.TaskQueue,
			age,
			shortID(d.RunID),
		})
	}
	_ = r.printer.Table([]string{"WORKFLOW ID", "TYPE", "STATUS", "TASK QUEUE", "AGE/DURATION", "RUN"}, rows)
	r.printer.Printf("\n%d execution(s)\n", len(items))
}

// ---------------------------------------------------------------------------
// result
// ---------------------------------------------------------------------------

func (r *root) newWorkflowResultCommand() *cobra.Command {
	var runID string
	cmd := &cobra.Command{
		Use:   "result <workflow-id>",
		Short: "Wait for a workflow to finish and print its result",
		Long: `Block until the execution reaches a terminal state, then print what it
produced.

This is a long poll, not a busy loop: the connection stays open and the server
answers the moment the workflow finishes, so waiting a week costs one socket.

Exit code 0 means the workflow completed. Exit code 2 means it failed, was
canceled, was terminated or timed out -- the command itself worked. Exit code 1
means the command could not do its job.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return r.printResult(cmd, args[0], runID)
		},
	}
	cmd.Flags().StringVar(&runID, "run-id", "", "pin a specific run")
	return cmd
}

// printResult waits for a terminal state and renders it.
func (r *root) printResult(cmd *cobra.Command, workflowID, runID string) error {
	c, err := r.Client()
	if err != nil {
		return err
	}
	handle := c.NewHandle(r.opts.Namespace, workflowID, runID)

	var raw json.RawMessage
	resultErr := handle.Result(commandContext(cmd), &raw)
	if resultErr == nil {
		if r.opts.Format == FormatJSON {
			return r.printer.JSON(map[string]any{
				"workflow_id": workflowID,
				"status":      skald.StatusCompleted.String(),
				"result":      rawOrNull(raw),
			})
		}
		r.printer.Printf("%s\n", r.printer.StatusColor(skald.StatusCompleted))
		if len(raw) > 0 {
			var pretty bytes.Buffer
			if json.Indent(&pretty, raw, "", "  ") == nil {
				r.printer.Printf("%s\n", pretty.String())
			} else {
				r.printer.Printf("%s\n", string(raw))
			}
		}
		return nil
	}

	// A transport failure is the command failing; a terminal workflow state is
	// the command succeeding at reporting bad news. They get different exit
	// codes and only the second one is rendered as a result.
	status, ok := terminalStatusOf(resultErr)
	if !ok {
		return resultErr
	}
	if r.opts.Format == FormatJSON {
		if err := r.printer.JSON(map[string]any{
			"workflow_id": workflowID,
			"status":      status.String(),
			"error":       resultErr.Error(),
		}); err != nil {
			return err
		}
	} else {
		r.printer.Printf("%s\n%s\n", r.printer.StatusColor(status), resultErr.Error())
	}
	return &exitError{code: ExitWorkflowFailed, err: resultErr, reported: true}
}

// terminalStatusOf maps the typed errors WorkflowHandle.Result returns back to
// the status they represent.
func terminalStatusOf(err error) (skald.WorkflowStatus, bool) {
	var canceled *skald.CanceledError
	var terminated *skald.TerminatedError
	var timedOut *skald.TimeoutError
	var app *skald.ApplicationError
	switch {
	case errors.As(err, &canceled):
		return skald.StatusCanceled, true
	case errors.As(err, &terminated):
		return skald.StatusTerminated, true
	case errors.As(err, &timedOut):
		return skald.StatusTimedOut, true
	case errors.As(err, &app):
		return skald.StatusFailed, true
	}
	return skald.StatusRunning, false
}

func rawOrNull(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

// ---------------------------------------------------------------------------
// Flag parsing helpers
// ---------------------------------------------------------------------------

// parsePayloadFlag turns an --input value into a payload.
//
// Three forms are accepted because all three come up: a literal for a quick
// signal, @file for anything with quoting in it, and - for a pipeline. The JSON
// is validated here rather than at the server so that a typo is reported before
// a workflow is started with it.
func parsePayloadFlag(raw string) (*skald.Payload, error) {
	if raw == "" {
		return nil, nil
	}
	data := []byte(raw)
	switch {
	case raw == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading input from stdin: %w", err)
		}
		data = b
	case strings.HasPrefix(raw, "@"):
		b, err := os.ReadFile(strings.TrimPrefix(raw, "@"))
		if err != nil {
			return nil, fmt.Errorf("reading input file: %w", err)
		}
		data = b
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err != nil {
		return nil, fmt.Errorf("input is not valid JSON: %w", err)
	}
	if compact.Len() > skald.MaxPayloadBytes {
		return nil, fmt.Errorf("input is %d bytes, the limit is %d", compact.Len(), skald.MaxPayloadBytes)
	}
	return &skald.Payload{Encoding: skald.EncodingJSON, Data: compact.Bytes()}, nil
}

// parseKeyValues turns repeated `k=v` flags into a map.
func parseKeyValues(values []string, flagName string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for _, v := range values {
		k, val, ok := strings.Cut(v, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("%s %q must be key=value", flagName, v)
		}
		out[k] = val
	}
	return out, nil
}
