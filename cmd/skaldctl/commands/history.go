package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

func (r *root) newWorkflowHistoryCommand() *cobra.Command {
	var (
		runID    string
		follow   bool
		asJSON   bool
		maxEvent int64
	)

	cmd := &cobra.Command{
		Use:   "history <workflow-id>",
		Short: "Print a workflow's history",
		Long: `Print the complete, ordered history of an execution.

This is the command to reach for when you do not yet know what went wrong. The
history is the only durable truth in Skald -- everything else is derived from it
-- so whatever happened is in here, in order, with timings.

Reading the view:

  ID       the event's position; dense and 1-based, so gaps are impossible
  AGE      how long ago it happened
  DELTA    time since the previous event, which is where the stalls show up
  EVENT    indented when the event was produced by the workflow task above it,
           so you can see which decision caused which effect
  DETAILS  the fields that matter; use --json for everything

--json prints the raw event array, which is exactly what "workflow replay"
reads, so you can capture a history now and validate or diff it later.`,
		Example: `  # what happened
  skaldctl workflow history order-1234

  # watch it happen
  skaldctl workflow history order-1234 --follow

  # capture it for later, then check it is structurally sound
  skaldctl workflow history order-1234 --json > order-1234.json
  skaldctl workflow replay order-1234.json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := r.Client()
			if err != nil {
				return err
			}
			asJSON = asJSON || r.opts.Format == FormatJSON
			ctx := commandContext(cmd)
			workflowID := args[0]

			if follow {
				return r.followHistory(ctx, c, workflowID, runID, asJSON)
			}

			events, status, err := fetchHistory(ctx, c, r.opts.Namespace, workflowID, runID, maxEvent)
			if err != nil {
				return err
			}
			if asJSON {
				return r.printer.JSON(events)
			}
			renderer := newHistoryRenderer(r.printer)
			renderer.Write(events)
			r.printHistoryFooter(events, status)
			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&runID, "run-id", "", "pin a specific run")
	f.BoolVarP(&follow, "follow", "f", false, "stream new events as they are appended")
	f.BoolVar(&asJSON, "json", false, "print the raw event array (same as --output json)")
	f.Int64Var(&maxEvent, "max-events", 0, "stop after this many events; 0 for all")
	return cmd
}

// historyClient is the slice of the API the history commands need.
//
// A narrow interface rather than *client.Client so that the paging and
// streaming logic -- the part with the off-by-one risk -- is testable against a
// stub with no server behind it.
type historyClient interface {
	GetHistory(ctx context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error)
}

// fetchHistory reads a whole history, following pagination.
func fetchHistory(ctx context.Context, c historyClient, namespace, workflowID, runID string, max int64) (history.History, skald.WorkflowStatus, error) {
	var (
		all    history.History
		status skald.WorkflowStatus
		from   = int64(1)
	)
	for {
		resp, err := c.GetHistory(ctx, api.GetHistoryRequest{
			Namespace:   namespace,
			WorkflowID:  workflowID,
			RunID:       runID,
			FromEventID: from,
		})
		if err != nil {
			return nil, status, err
		}
		status = resp.Status
		all = append(all, resp.Events...)
		if max > 0 && int64(len(all)) >= max {
			return all[:max], status, nil
		}
		if len(resp.Events) == 0 || resp.NextEventID <= from {
			return all, status, nil
		}
		from = resp.NextEventID
	}
}

// followHistory streams events until the execution closes or the caller stops.
func (r *root) followHistory(ctx context.Context, c historyClient, workflowID, runID string, asJSON bool) error {
	renderer := newHistoryRenderer(r.printer)
	encoder := json.NewEncoder(r.printer.Out())
	encoder.SetEscapeHTML(false)

	from := int64(1)
	for {
		resp, err := c.GetHistory(ctx, api.GetHistoryRequest{
			Namespace:   r.opts.Namespace,
			WorkflowID:  workflowID,
			RunID:       runID,
			FromEventID: from,
			WaitForNew:  true,
		})
		if err != nil {
			// A cancelled context is the operator pressing Ctrl-C, which is a
			// normal way to stop following and not a failure to report.
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil //nolint:nilerr // Ctrl-C is how you stop following; it is not a failure to report
			}
			return err
		}

		if asJSON {
			// Newline-delimited events while following: an array cannot be
			// closed until the stream ends, and a consumer that has to wait for
			// the closing bracket is not streaming. Use the non-follow form to
			// produce a file that "workflow replay" can read.
			for _, ev := range resp.Events {
				if err := encoder.Encode(ev); err != nil {
					return err
				}
			}
		} else {
			renderer.Write(resp.Events)
		}

		if resp.NextEventID > from {
			from = resp.NextEventID
		}
		if resp.Status.Terminal() && len(resp.Events) == 0 {
			if !asJSON {
				r.printer.Printf("\n%s %s\n", r.printer.dim("execution closed:"), r.printer.StatusColor(resp.Status))
			}
			return nil
		}
	}
}

// printHistoryFooter summarises what was just printed.
//
// The summary exists because the top of a long history scrolls away: after
// paging through four hundred events, "FAILED, 12m03s, 412 events" is the line
// that stops you scrolling back up to check.
func (r *root) printHistoryFooter(events history.History, status skald.WorkflowStatus) {
	if len(events) == 0 {
		r.printer.Printf("no events\n")
		return
	}
	span := events[len(events)-1].Time.Sub(events[0].Time)
	r.printer.Printf("\n%d events  %s  spanning %s\n",
		len(events), r.printer.StatusColor(status), CompactDuration(span))
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

func (r *root) newWorkflowReplayCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay <history.json>",
		Short: "Validate a history file",
		Long: `Check that a history file is structurally sound.

It verifies everything the engine relies on: that event IDs are dense and
1-based, that timestamps never go backwards, that the first event starts the
execution and the terminal one is last, and that every back-reference names an
earlier event of the expected type.

This is the check to run when a history has been through anything that could
have mangled it -- an export, a manual edit, a migration, a bug report
attachment -- before you spend an hour wondering why a replay behaves oddly.

Pass - to read from stdin.`,
		Example: `  skaldctl workflow history order-1234 --json > order-1234.json
  skaldctl workflow replay order-1234.json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, err := readHistoryFile(args[0])
			if err != nil {
				return err
			}
			var h history.History
			if err := json.Unmarshal(raw, &h); err != nil {
				return fmt.Errorf("%s is not a history: %w", args[0], err)
			}

			validationErr := h.Validate()
			if r.opts.Format == FormatJSON {
				out := map[string]any{"source": args[0], "events": len(h), "valid": validationErr == nil}
				if validationErr != nil {
					out["error"] = validationErr.Error()
				}
				if err := r.printer.JSON(out); err != nil {
					return err
				}
				if validationErr != nil {
					return &exitError{code: ExitError, err: validationErr, reported: true}
				}
				return nil
			}

			if validationErr != nil {
				r.printer.Printf("%s %s\n  %s\n", r.printer.red("INVALID"), args[0], validationErr.Error())
				return &exitError{code: ExitError, err: validationErr, reported: true}
			}
			r.printer.Printf("%s %s\n", r.printer.green("VALID"), args[0])
			r.printHistorySummary(h)
			return nil
		},
	}
	return cmd
}

func readHistoryFile(path string) ([]byte, error) {
	if path == "-" {
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading history from stdin: %w", err)
		}
		return raw, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading history file: %w", err)
	}
	return raw, nil
}

// printHistorySummary describes a validated history.
func (r *root) printHistorySummary(h history.History) {
	if len(h) == 0 {
		r.printer.Printf("  empty history\n")
		return
	}
	rows := [][]string{
		{"events", fmt.Sprintf("%d", len(h))},
		{"span", CompactDuration(h[len(h)-1].Time.Sub(h[0].Time))},
		{"first", h[0].Type().String()},
		{"last", h[len(h)-1].Type().String()},
		{"terminated", fmt.Sprintf("%t", h.Terminated())},
	}
	if attrs, ok := h.StartedAttributes(); ok {
		rows = append(rows,
			[]string{"workflow_type", attrs.WorkflowType},
			[]string{"task_queue", attrs.TaskQueue},
		)
	}
	_ = r.printer.Table(nil, rows)
}
