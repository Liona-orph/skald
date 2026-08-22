package commands

import (
	"context"
	"sort"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/skald"
)

func (r *root) newTaskQueueCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "taskqueue",
		Aliases: []string{"tq"},
		Short:   "Inspect task queues",
	}
	cmd.AddCommand(r.newTaskQueueDescribeCommand())
	return cmd
}

// TaskQueueSummary is what `taskqueue describe` reports.
//
// It is a view over the visibility store, assembled client side. See the
// command's documentation for why, and for what is missing.
type TaskQueueSummary struct {
	Namespace string `json:"namespace"`
	TaskQueue string `json:"task_queue"`

	Running int `json:"running"`
	Closed  int `json:"closed"`

	// ByStatus and ByType are the two breakdowns that answer "is this queue
	// healthy" and "what is on it".
	ByStatus map[string]int `json:"by_status"`
	ByType   map[string]int `json:"by_type"`

	// OldestRunning is the execution that has been going longest, which is the
	// one to look at when a queue is backed up.
	OldestRunning *api.DescribeWorkflowResponse `json:"oldest_running,omitempty"`

	// Sampled reports how many executions were examined, and Truncated whether
	// the answer is complete.
	Sampled   int  `json:"sampled"`
	Truncated bool `json:"truncated"`
}

func (r *root) newTaskQueueDescribeCommand() *cobra.Command {
	var sampleLimit int

	cmd := &cobra.Command{
		Use:   "describe <task-queue>",
		Short: "Summarise the executions on a task queue",
		Long: `Summarise what is on a task queue.

Use this when workflows on one queue are not progressing and you need to know
whether the problem is the queue (nothing is being picked up) or the work (one
type is failing).

What it can tell you: how many executions are running or closed on this queue,
broken down by status and workflow type, and which running execution has been
going longest.

What it cannot tell you, yet: the live backlog depth and the number of workers
currently polling. Those live in the server's matching layer, which the protocol
in pkg/api does not expose -- there is no DescribeTaskQueue endpoint. Until
there is, the server's own metrics answer it: skald_task_queue_backlog and
skald_task_queue_pollers on /metrics.

This walks the visibility store and filters client side, so it costs one query
per page. --limit bounds that.`,
		Example: `  skaldctl taskqueue describe orders
  skaldctl taskqueue describe orders --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := r.Client()
			if err != nil {
				return err
			}
			summary, err := summariseTaskQueue(commandContext(cmd), c, r.opts.Namespace, args[0], sampleLimit)
			if err != nil {
				return err
			}
			if r.opts.Format == FormatJSON {
				return r.printer.JSON(summary)
			}
			r.renderTaskQueue(summary)
			return nil
		},
	}
	cmd.Flags().IntVar(&sampleLimit, "limit", 1000, "maximum executions to examine")
	return cmd
}

// listClient is the slice of the API the summary needs.
type listClient interface {
	ListWorkflows(ctx context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error)
}

// summariseTaskQueue walks visibility and aggregates by task queue.
func summariseTaskQueue(ctx context.Context, c listClient, namespace, taskQueue string, limit int) (TaskQueueSummary, error) {
	summary := TaskQueueSummary{
		Namespace: namespace,
		TaskQueue: taskQueue,
		ByStatus:  map[string]int{},
		ByType:    map[string]int{},
	}

	var token string
	for {
		resp, err := c.ListWorkflows(ctx, api.ListWorkflowsRequest{
			Namespace: namespace,
			PageSize:  pageSizeFor(limit, summary.Sampled),
			PageToken: token,
		})
		if err != nil {
			return TaskQueueSummary{}, err
		}
		for _, exec := range resp.Executions {
			summary.Sampled++
			if exec.TaskQueue != taskQueue {
				continue
			}
			summary.ByStatus[exec.Status.String()]++
			summary.ByType[exec.WorkflowType]++
			if exec.Status == skald.StatusRunning {
				summary.Running++
				if summary.OldestRunning == nil || exec.StartedAt.Before(summary.OldestRunning.StartedAt) {
					oldest := exec
					summary.OldestRunning = &oldest
				}
			} else {
				summary.Closed++
			}
		}
		token = resp.NextPageToken
		if token == "" {
			return summary, nil
		}
		if limit > 0 && summary.Sampled >= limit {
			summary.Truncated = true
			return summary, nil
		}
	}
}

func (r *root) renderTaskQueue(s TaskQueueSummary) {
	p := r.printer
	now := p.Now()

	p.Printf("%s %s\n", p.bold("TASK QUEUE"), s.TaskQueue)
	rows := [][]string{
		{"namespace", s.Namespace},
		{"running", strconv.Itoa(s.Running)},
		{"closed", strconv.Itoa(s.Closed)},
		{"examined", strconv.Itoa(s.Sampled)},
	}
	if s.Truncated {
		rows = append(rows, []string{"note", p.yellow("sample truncated; raise --limit for a complete count")})
	}
	if s.OldestRunning != nil {
		rows = append(rows, []string{
			"oldest running",
			s.OldestRunning.WorkflowID + " (" + Relative(now, s.OldestRunning.StartedAt) + ")",
		})
	}
	_ = p.Table(nil, rows)

	if len(s.ByStatus) > 0 {
		p.Printf("\n%s\n", p.bold("BY STATUS"))
		_ = p.Table(nil, sortedCountRows(s.ByStatus))
	}
	if len(s.ByType) > 0 {
		p.Printf("\n%s\n", p.bold("BY TYPE"))
		_ = p.Table(nil, sortedCountRows(s.ByType))
	}
	if s.Running == 0 && s.Closed == 0 {
		p.Printf("\nno executions found on this task queue\n")
	}
}

// sortedCountRows renders a count map in descending order, ties broken by name
// so that repeated runs of the command produce identical output -- a diff
// between two invocations should show what changed, not what got reordered.
func sortedCountRows(counts map[string]int) [][]string {
	type entry struct {
		name  string
		count int
	}
	entries := make([]entry, 0, len(counts))
	for name, count := range counts {
		entries = append(entries, entry{name, count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].name < entries[j].name
	})
	rows := make([][]string, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, []string{e.name, strconv.Itoa(e.count)})
	}
	return rows
}
