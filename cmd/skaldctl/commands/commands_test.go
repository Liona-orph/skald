package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/client"
	"github.com/skald-io/skald/pkg/history"
	"github.com/skald-io/skald/pkg/skald"
)

// runCLI executes the command tree with a client factory that refuses to build.
//
// Everything asserted here either fails before a client is needed (argument
// validation) or never needs one (replay), so a factory that always fails makes
// "this test accidentally tried to talk to a server" a loud failure rather than
// a mysterious timeout.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := runCLIWithClient(t, func(Options) (*client.Client, error) {
		return nil, errors.New("no server configured for this test")
	}, args...)
	return out, err
}

func runCLIWithClient(t *testing.T, factory func(Options) (*client.Client, error), args ...string) (string, error) {
	t.Helper()

	var buf bytes.Buffer
	cmd := NewRootCommand(Env{
		Out:        &buf,
		Err:        &buf,
		Now:        func() time.Time { return fixedNow },
		IsTerminal: func() bool { return false },
		NewClient:  factory,
	})
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func TestGlobalFlagValidation(t *testing.T) {
	t.Parallel()

	// A bad --output must fail before the request, not after it: discovering
	// the typo only once the server has already been asked to do something is
	// the wrong order for a mutating command.
	_, err := runCLI(t, "--output", "yaml", "workflow", "list")
	require.ErrorContains(t, err, "unknown output format")

	_, err = runCLI(t, "--color", "rainbow", "workflow", "list")
	require.ErrorContains(t, err, "unknown color mode")
}

func TestArgumentValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"start without type", []string{"workflow", "start", "--task-queue", "orders"}, "--type is required"},
		{"start without task queue", []string{"workflow", "start", "--type", "T"}, "--task-queue is required"},
		{"start with bad input", []string{"workflow", "start", "--type", "T", "--task-queue", "q", "--input", "{not json"}, "not valid JSON"},
		{"start with bad memo", []string{"workflow", "start", "--type", "T", "--task-queue", "q", "--memo", "nope"}, "must be key=value"},
		{"signal without name", []string{"workflow", "signal", "order-1"}, "--name is required"},
		{"signal without id", []string{"workflow", "signal"}, "accepts 1 arg"},
		{"describe without id", []string{"workflow", "describe"}, "accepts 1 arg"},
		{"history with two ids", []string{"workflow", "history", "a", "b"}, "accepts 1 arg"},
		{"replay without file", []string{"workflow", "replay"}, "accepts 1 arg"},
		{"taskqueue without name", []string{"taskqueue", "describe"}, "accepts 1 arg"},
		{"list rejects a bad status", []string{"workflow", "list", "--status", "SLEEPY"}, "unknown workflow status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runCLI(t, tc.args...)
			require.ErrorContains(t, err, tc.wantErr)
			require.Equal(t, ExitError, ExitCodeFor(err))
		})
	}
}

func TestListStatusValidationHappensBeforeTheRequest(t *testing.T) {
	t.Parallel()

	// The status is validated client side so that a typo produces "unknown
	// workflow status" instead of an empty result set that looks like "nothing
	// is running".
	_, err := runCLI(t, "workflow", "list", "--status", "RUNNIG")
	require.ErrorContains(t, err, "unknown workflow status")
}

func TestBareInvocationPrintsHelp(t *testing.T) {
	t.Parallel()

	out, err := runCLI(t)
	require.NoError(t, err)
	require.Contains(t, out, "Start here when something is wrong")
	require.Contains(t, out, "workflow history")
}

func TestHelpTextNamesTheSituation(t *testing.T) {
	t.Parallel()

	// The help is the only documentation available to someone who has just been
	// paged, so it has to say when to use a command, not only what its flags
	// are.
	out, err := runCLI(t, "workflow", "terminate", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "No workflow code runs in response")
	require.Contains(t, out, "Always pass --reason")

	out, err = runCLI(t, "workflow", "history", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "DELTA")
	require.Contains(t, out, "workflow replay")

	out, err = runCLI(t, "taskqueue", "describe", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "What it cannot tell you")
	require.Contains(t, out, "skald_task_queue_backlog")
}

func TestVersionCommand(t *testing.T) {
	t.Parallel()

	out, err := runCLI(t, "version")
	require.NoError(t, err)
	require.Contains(t, out, "skaldctl")
	require.Contains(t, out, version)
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

func writeHistoryFile(t *testing.T, h history.History) string {
	t.Helper()
	raw, err := json.Marshal(h)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "history.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

func TestReplayValidatesAGoodHistory(t *testing.T) {
	t.Parallel()

	path := writeHistoryFile(t, sampleHistory())
	out, err := runCLI(t, "workflow", "replay", path)
	require.NoError(t, err)

	require.Contains(t, out, "VALID")
	require.Contains(t, out, "events")
	require.Contains(t, out, "workflow_type")
	require.Contains(t, out, "OrderWorkflow")
}

func TestReplayRejectsACorruptHistory(t *testing.T) {
	t.Parallel()

	// A gap in the event IDs is exactly what an export, a manual edit or a
	// truncated upload produces, and it is what history.Validate exists to
	// catch before it becomes a mysterious replay failure.
	corrupt := sampleHistory()
	corrupt[3].ID = 99

	path := writeHistoryFile(t, corrupt)
	out, err := runCLI(t, "workflow", "replay", path)
	require.Error(t, err)
	require.Equal(t, ExitError, ExitCodeFor(err))
	require.Contains(t, out, "INVALID")
	require.Contains(t, out, "dense and 1-based")
}

func TestReplayJSONOutput(t *testing.T) {
	t.Parallel()

	path := writeHistoryFile(t, sampleHistory())
	out, err := runCLI(t, "--output", "json", "workflow", "replay", path)
	require.NoError(t, err)

	var report struct {
		Source string `json:"source"`
		Events int    `json:"events"`
		Valid  bool   `json:"valid"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	require.True(t, report.Valid)
	require.Equal(t, len(sampleHistory()), report.Events)
	require.Equal(t, path, report.Source)
}

func TestReplayReportsUnreadableInput(t *testing.T) {
	t.Parallel()

	_, err := runCLI(t, "workflow", "replay", filepath.Join(t.TempDir(), "missing.json"))
	require.ErrorContains(t, err, "reading history file")

	path := filepath.Join(t.TempDir(), "garbage.json")
	require.NoError(t, os.WriteFile(path, []byte("not json at all"), 0o600))
	_, err = runCLI(t, "workflow", "replay", path)
	require.ErrorContains(t, err, "is not a history")
}

// ---------------------------------------------------------------------------
// Flag helpers
// ---------------------------------------------------------------------------

func TestParsePayloadFlag(t *testing.T) {
	t.Parallel()

	p, err := parsePayloadFlag("")
	require.NoError(t, err)
	require.Nil(t, p)

	p, err = parsePayloadFlag(`{ "a" : 1 }`)
	require.NoError(t, err)
	require.Equal(t, skald.EncodingJSON, p.Encoding)
	// Compacted, so the stored bytes are canonical whatever the shell passed.
	require.Equal(t, `{"a":1}`, string(p.Data))

	dir := t.TempDir()
	file := filepath.Join(dir, "input.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"from":"file"}`), 0o600))
	p, err = parsePayloadFlag("@" + file)
	require.NoError(t, err)
	require.JSONEq(t, `{"from":"file"}`, string(p.Data))

	_, err = parsePayloadFlag("@" + filepath.Join(dir, "missing"))
	require.ErrorContains(t, err, "reading input file")

	_, err = parsePayloadFlag("{oops")
	require.ErrorContains(t, err, "not valid JSON")

	huge := `"` + strings.Repeat("x", skald.MaxPayloadBytes+16) + `"`
	_, err = parsePayloadFlag(huge)
	require.ErrorContains(t, err, "the limit is")
}

func TestParseKeyValues(t *testing.T) {
	t.Parallel()

	got, err := parseKeyValues([]string{"a=1", "b=two=three"}, "--memo")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "1", "b": "two=three"}, got)

	_, err = parseKeyValues([]string{"=v"}, "--memo")
	require.ErrorContains(t, err, "must be key=value")
}

func TestPageSizeFor(t *testing.T) {
	t.Parallel()

	// --limit 5 against a million executions must be one small query, not a
	// full scan the client throws away.
	require.Equal(t, 0, pageSizeFor(0, 0), "no limit means the driver default")
	require.Equal(t, 5, pageSizeFor(5, 0))
	require.Equal(t, 2, pageSizeFor(5, 3))
	require.Equal(t, 1000, pageSizeFor(100000, 0))
}

func TestExitCodes(t *testing.T) {
	t.Parallel()

	require.Equal(t, ExitOK, ExitCodeFor(nil))
	require.Equal(t, ExitError, ExitCodeFor(errors.New("boom")))
	require.Equal(t, ExitWorkflowFailed, ExitCodeFor(&exitError{code: ExitWorkflowFailed, err: errors.New("failed")}))

	// A failure the command already rendered as a result must not be repeated
	// on stderr, where it would read as a second, separate problem.
	require.True(t, ShouldReport(errors.New("boom")))
	require.False(t, ShouldReport(nil))
	require.False(t, ShouldReport(&exitError{code: ExitWorkflowFailed, err: errors.New("failed"), reported: true}))
	require.True(t, ShouldReport(&exitError{code: ExitError, err: errors.New("failed")}))

	// The wrapped error stays reachable, so a caller can still inspect it.
	wrapped := &exitError{code: ExitWorkflowFailed, err: &skald.ApplicationError{Type: "X", Message: "y"}}
	var app *skald.ApplicationError
	require.ErrorAs(t, error(wrapped), &app)
	require.Equal(t, "X", app.Type)
}

func TestTerminalStatusOf(t *testing.T) {
	t.Parallel()

	// The distinction the exit code is built on: a terminal workflow state is
	// the command succeeding at reporting bad news, and anything else is the
	// command failing.
	for err, want := range map[error]skald.WorkflowStatus{
		&skald.ApplicationError{Message: "x"}: skald.StatusFailed,
		&skald.CanceledError{}:                skald.StatusCanceled,
		&skald.TerminatedError{}:              skald.StatusTerminated,
		&skald.TimeoutError{}:                 skald.StatusTimedOut,
	} {
		got, ok := terminalStatusOf(err)
		require.True(t, ok)
		require.Equal(t, want, got)
	}

	_, ok := terminalStatusOf(&api.Error{Code: api.CodeUnavailable})
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// Data plumbing
// ---------------------------------------------------------------------------

// stubHistoryClient serves a fixed history one event at a time, so a client
// that ignores the cursor loops forever or stops early.
type stubHistoryClient struct {
	events history.History
	status skald.WorkflowStatus
	calls  int
}

func (s *stubHistoryClient) GetHistory(_ context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
	s.calls++
	from := req.FromEventID
	if from < 1 {
		from = 1
	}
	if int(from) > len(s.events) {
		return api.GetHistoryResponse{Status: s.status, NextEventID: from}, nil
	}
	return api.GetHistoryResponse{
		Events:      s.events[from-1 : from],
		Status:      s.status,
		NextEventID: from + 1,
	}, nil
}

func TestFetchHistoryPages(t *testing.T) {
	t.Parallel()

	stub := &stubHistoryClient{events: sampleHistory(), status: skald.StatusFailed}
	got, status, err := fetchHistory(context.Background(), stub, "default", "order-1", "", 0)
	require.NoError(t, err)
	require.Len(t, got, len(sampleHistory()))
	require.Equal(t, skald.StatusFailed, status)
	require.Equal(t, len(sampleHistory())+1, stub.calls)
}

func TestFetchHistoryHonoursMaxEvents(t *testing.T) {
	t.Parallel()

	stub := &stubHistoryClient{events: sampleHistory(), status: skald.StatusFailed}
	got, _, err := fetchHistory(context.Background(), stub, "default", "order-1", "", 3)
	require.NoError(t, err)
	require.Len(t, got, 3)
}

// stubListClient pages a fixed set of executions.
type stubListClient struct {
	pages [][]api.DescribeWorkflowResponse
	seen  []api.ListWorkflowsRequest
}

func (s *stubListClient) ListWorkflows(_ context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
	s.seen = append(s.seen, req)
	index := len(s.seen) - 1
	if index >= len(s.pages) {
		return api.ListWorkflowsResponse{}, nil
	}
	resp := api.ListWorkflowsResponse{Executions: s.pages[index]}
	if index+1 < len(s.pages) {
		resp.NextPageToken = "next"
	}
	return resp, nil
}

func TestSummariseTaskQueue(t *testing.T) {
	t.Parallel()

	old := fixedNow.Add(-3 * time.Hour)
	recent := fixedNow.Add(-time.Minute)
	stub := &stubListClient{pages: [][]api.DescribeWorkflowResponse{
		{
			{WorkflowID: "a", WorkflowType: "OrderWorkflow", TaskQueue: "orders", Status: skald.StatusRunning, StartedAt: recent},
			{WorkflowID: "b", WorkflowType: "OrderWorkflow", TaskQueue: "orders", Status: skald.StatusRunning, StartedAt: old},
			{WorkflowID: "c", WorkflowType: "Refund", TaskQueue: "refunds", Status: skald.StatusRunning, StartedAt: recent},
		},
		{
			{WorkflowID: "d", WorkflowType: "Refund", TaskQueue: "orders", Status: skald.StatusFailed, StartedAt: old},
		},
	}}

	got, err := summariseTaskQueue(context.Background(), stub, "default", "orders", 0)
	require.NoError(t, err)

	require.Equal(t, 2, got.Running)
	require.Equal(t, 1, got.Closed)
	require.Equal(t, 4, got.Sampled, "every execution is examined; only matches are counted")
	require.Equal(t, map[string]int{"RUNNING": 2, "FAILED": 1}, got.ByStatus)
	require.Equal(t, map[string]int{"OrderWorkflow": 2, "Refund": 1}, got.ByType)
	require.NotNil(t, got.OldestRunning)
	require.Equal(t, "b", got.OldestRunning.WorkflowID, "the oldest running execution is the one to look at")
	require.False(t, got.Truncated)
}

func TestSummariseTaskQueueReportsTruncation(t *testing.T) {
	t.Parallel()

	stub := &stubListClient{pages: [][]api.DescribeWorkflowResponse{
		{{WorkflowID: "a", TaskQueue: "orders", Status: skald.StatusRunning}},
		{{WorkflowID: "b", TaskQueue: "orders", Status: skald.StatusRunning}},
	}}
	got, err := summariseTaskQueue(context.Background(), stub, "default", "orders", 1)
	require.NoError(t, err)

	// An answer that stopped early has to say so, or it reads as a complete
	// count that happens to be wrong.
	require.True(t, got.Truncated)
	require.Equal(t, 1, got.Sampled)
}

func TestSortedCountRowsIsStable(t *testing.T) {
	t.Parallel()

	rows := sortedCountRows(map[string]int{"b": 2, "a": 2, "c": 5})
	// Descending by count, ties broken by name, so a diff between two runs of
	// the command shows what changed rather than what got reordered.
	require.Equal(t, [][]string{{"c", "5"}, {"a", "2"}, {"b", "2"}}, rows)
}

func TestRenderTaskQueueSummary(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	r := &root{printer: p, opts: Options{Format: FormatTable}}
	r.renderTaskQueue(TaskQueueSummary{
		Namespace: "default",
		TaskQueue: "orders",
		Running:   2,
		Closed:    1,
		ByStatus:  map[string]int{"RUNNING": 2, "FAILED": 1},
		ByType:    map[string]int{"OrderWorkflow": 3},
		Sampled:   4,
		Truncated: true,
		OldestRunning: &api.DescribeWorkflowResponse{
			WorkflowID: "b", StartedAt: fixedNow.Add(-3 * time.Hour),
		},
	})

	out := buf.String()
	require.Contains(t, out, "TASK QUEUE orders")
	require.Contains(t, out, "running")
	require.Contains(t, out, "oldest running")
	require.Contains(t, out, "b (3h00m ago)")
	require.Contains(t, out, "sample truncated")
	require.Contains(t, out, "BY STATUS")
	require.Contains(t, out, "BY TYPE")
}

func TestRenderDescribeShowsWhatItIsWaitingOn(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	r := &root{printer: p, opts: Options{Format: FormatTable}}
	r.renderDescribe(api.DescribeWorkflowResponse{
		Namespace: "default", WorkflowID: "order-1", RunID: "run-1",
		WorkflowType: "OrderWorkflow", TaskQueue: "orders",
		Status: skald.StatusRunning, StartedAt: fixedNow.Add(-90 * time.Second),
		HistoryLength: 11,
		PendingActivities: []api.PendingActivity{{
			ActivityID: "charge", ActivityType: "ChargeCard", ScheduledEventID: 5,
			Attempt: 3, Started: false, ScheduledAt: fixedNow.Add(-30 * time.Second),
			LastFailure: &skald.ApplicationError{Type: "CardDeclined", Message: "try later"},
		}},
		PendingTimers: []api.PendingTimer{{
			TimerID: "retry", StartedEventID: 6, FireAt: fixedNow.Add(2 * time.Minute),
		}},
	})

	out := buf.String()
	require.Contains(t, out, "running_for")
	require.Contains(t, out, "1m30s")
	// The pending sections are the part to read first when a workflow looks
	// stuck, and the attempt count is what says whether it is retrying.
	require.Contains(t, out, "PENDING ACTIVITIES")
	require.Contains(t, out, "ChargeCard")
	require.Contains(t, out, "attempt 3")
	require.Contains(t, out, "queued")
	require.Contains(t, out, "CardDeclined")
	require.Contains(t, out, "PENDING TIMERS")
	require.Contains(t, out, "in 2m00s")
}

func TestRenderListSummarises(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	r := &root{printer: p, opts: Options{Format: FormatTable}}
	r.renderList([]api.DescribeWorkflowResponse{
		{WorkflowID: "a", WorkflowType: "OrderWorkflow", TaskQueue: "orders",
			Status: skald.StatusRunning, StartedAt: fixedNow.Add(-time.Hour), RunID: "0f3a1b2c-dead"},
	})

	out := buf.String()
	require.Contains(t, out, "WORKFLOW ID")
	require.Contains(t, out, "1h00m ago")
	require.Contains(t, out, "0f3a1b2c")
	require.Contains(t, out, "1 execution(s)")

	p2, buf2 := newTestPrinter(false)
	r2 := &root{printer: p2, opts: Options{Format: FormatTable}}
	r2.renderList(nil)
	require.Contains(t, buf2.String(), "no executions matched")
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()

	require.Equal(t, "a", firstNonEmpty("", "a", "b"))
	require.Equal(t, "", firstNonEmpty("", ""))
}
