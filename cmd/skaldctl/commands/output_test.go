package commands

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// fixedNow is the clock every rendering test uses, so relative timestamps are
// deterministic rather than "whatever the CI machine's clock said".
var fixedNow = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

func newTestPrinter(color bool) (*Printer, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewPrinter(&buf, FormatTable, color, func() time.Time { return fixedNow }), &buf
}

func TestParseFormat(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]Format{
		"": FormatTable, "table": FormatTable, "JSON": FormatJSON, " json ": FormatJSON,
	} {
		got, err := ParseFormat(input)
		require.NoError(t, err, input)
		require.Equal(t, want, got, input)
	}

	_, err := ParseFormat("yaml")
	require.ErrorContains(t, err, "unknown output format")
}

func TestParseColorMode(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]ColorMode{
		"": ColorAuto, "auto": ColorAuto, "ALWAYS": ColorAlways, "never": ColorNever,
	} {
		got, err := ParseColorMode(input)
		require.NoError(t, err, input)
		require.Equal(t, want, got, input)
	}

	_, err := ParseColorMode("rainbow")
	require.ErrorContains(t, err, "unknown color mode")
}

func TestCompactDuration(t *testing.T) {
	t.Parallel()

	for d, want := range map[time.Duration]string{
		0:                            "0ms",
		250 * time.Millisecond:       "250ms",
		1500 * time.Millisecond:      "1.5s",
		90 * time.Second:             "1m30s",
		2*time.Hour + 13*time.Minute: "2h13m",
		50 * time.Hour:               "2d02h",
	} {
		require.Equal(t, want, CompactDuration(d), d.String())
	}

	// Negative durations are rendered by magnitude; the sign is the caller's to
	// present, which is what Delta does with its "!" marker.
	require.Equal(t, "5.0s", CompactDuration(-5*time.Second))
}

func TestRelativeAndDelta(t *testing.T) {
	t.Parallel()

	require.Equal(t, "-", Relative(fixedNow, time.Time{}))
	require.Equal(t, "just now", Relative(fixedNow, fixedNow.Add(-10*time.Millisecond)))
	require.Equal(t, "2h13m ago", Relative(fixedNow, fixedNow.Add(-(2*time.Hour+13*time.Minute))))
	require.Equal(t, "in 30.0s", Relative(fixedNow, fixedNow.Add(30*time.Second)))

	require.Equal(t, "+1.2s", Delta(1200*time.Millisecond))
	// History time never goes backwards, so a negative delta means the file was
	// edited or corrupted and should look wrong.
	require.Equal(t, "!5ms", Delta(-5*time.Millisecond))
}

func TestPayloadPreview(t *testing.T) {
	t.Parallel()

	require.Empty(t, PayloadPreview(nil))
	require.Equal(t, "nil", PayloadPreview(&skald.Payload{Encoding: skald.EncodingNil}))
	require.Equal(t, `{"a":1}`, PayloadPreview(skald.MustPayload(map[string]int{"a": 1})))
	require.Equal(t, "<application/x-thing 3 bytes>",
		PayloadPreview(&skald.Payload{Encoding: "application/x-thing", Data: []byte("abc")}))

	// A payload can be two megabytes; printing it would bury the twenty other
	// events that supply the context.
	long := PayloadPreview(skald.MustPayload(strings.Repeat("x", 500)))
	require.True(t, strings.HasSuffix(long, "..."))
	require.LessOrEqual(t, len(long), maxPayloadPreview+3)
}

func TestTruncateMiddleKeepsBothEnds(t *testing.T) {
	t.Parallel()

	require.Equal(t, "short", TruncateMiddle("short", 10))
	// The discriminating part of an identifier is as often the suffix as the
	// prefix, so a tail-truncated list of them is a list of identical strings.
	got := TruncateMiddle("order-2024-000871", 14)
	require.Contains(t, got, "order")
	require.Contains(t, got, "871")
	require.Len(t, got, 14)
}

func TestTableAligns(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	require.NoError(t, p.Table([]string{"A", "BBBB"}, [][]string{
		{"1", "x"},
		{"longer", "y"},
	}))

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 3)
	// Every second column starts at the same offset, whatever the first
	// column's width.
	offset := strings.Index(lines[0], "BBBB")
	require.Equal(t, offset, strings.Index(lines[1], "x"))
	require.Equal(t, offset, strings.Index(lines[2], "y"))
}

func TestColourOnlyWhenEnabled(t *testing.T) {
	t.Parallel()

	plain, buf := newTestPrinter(false)
	require.NoError(t, plain.Table([]string{"A"}, [][]string{{plain.StatusColor(skald.StatusFailed)}}))
	require.NotContains(t, buf.String(), "\x1b[")
	require.Contains(t, buf.String(), "FAILED")

	colored, cbuf := newTestPrinter(true)
	require.NoError(t, colored.Table([]string{"A"}, [][]string{{colored.StatusColor(skald.StatusFailed)}}))
	require.Contains(t, cbuf.String(), ansiRed)
}

func TestJSONDoesNotEscapePayloadBytes(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	// Payload data is already-encoded JSON; HTML escaping would rewrite it and
	// make the output differ from what the server stored.
	require.NoError(t, p.JSON(map[string]string{"html": "<b>&</b>"}))
	require.Contains(t, buf.String(), "<b>&</b>")
}

// ---------------------------------------------------------------------------
// History rendering
// ---------------------------------------------------------------------------

// sampleHistory is a small but realistic run: a workflow task that schedules an
// activity, the activity failing, and the workflow failing as a result.
func sampleHistory() history.History {
	base := fixedNow.Add(-10 * time.Minute)
	at := func(offset time.Duration) time.Time { return base.Add(offset) }

	return history.History{
		{ID: 1, Time: at(0), Attrs: history.WorkflowExecutionStartedAttributes{
			WorkflowType: "OrderWorkflow", TaskQueue: "orders", Attempt: 1,
			Input: skald.MustPayload(map[string]int{"total": 4200}),
		}},
		{ID: 2, Time: at(0), Attrs: history.WorkflowTaskScheduledAttributes{TaskQueue: "orders", Attempt: 1}},
		{ID: 3, Time: at(20 * time.Millisecond), Attrs: history.WorkflowTaskStartedAttributes{
			ScheduledEventID: 2, Identity: "worker-1",
		}},
		{ID: 4, Time: at(45 * time.Millisecond), Attrs: history.WorkflowTaskCompletedAttributes{
			ScheduledEventID: 2, StartedEventID: 3, Identity: "worker-1", SDKName: "go", SDKVersion: "1.0",
		}},
		{ID: 5, Time: at(46 * time.Millisecond), Attrs: history.ActivityTaskScheduledAttributes{
			ActivityID: "charge", ActivityType: "ChargeCard", TaskQueue: "orders",
			StartToCloseTimeout: 30 * time.Second, WorkflowTaskCompletedEventID: 4,
		}},
		{ID: 6, Time: at(50 * time.Millisecond), Attrs: history.ActivityTaskStartedAttributes{
			ScheduledEventID: 5, Attempt: 1, Identity: "worker-2",
		}},
		{ID: 7, Time: at(3 * time.Second), Attrs: history.ActivityTaskFailedAttributes{
			ScheduledEventID: 5, StartedEventID: 6,
			Failure:    &skald.ApplicationError{Type: "CardDeclined", Message: "do not honour", NonRetryable: true},
			RetryState: history.RetryStateNonRetryableFailure,
		}},
		{ID: 8, Time: at(3 * time.Second), Attrs: history.WorkflowTaskScheduledAttributes{TaskQueue: "orders", Attempt: 1}},
		{ID: 9, Time: at(3010 * time.Millisecond), Attrs: history.WorkflowTaskStartedAttributes{ScheduledEventID: 8, Identity: "worker-1"}},
		{ID: 10, Time: at(3020 * time.Millisecond), Attrs: history.WorkflowTaskCompletedAttributes{
			ScheduledEventID: 8, StartedEventID: 9, Identity: "worker-1",
		}},
		{ID: 11, Time: at(3021 * time.Millisecond), Attrs: history.WorkflowExecutionFailedAttributes{
			Failure:                      &skald.ApplicationError{Type: "OrderFailed", Message: "payment declined"},
			WorkflowTaskCompletedEventID: 10,
			RetryState:                   history.RetryStateNonRetryableFailure,
		}},
	}
}

func TestRenderHistoryLayout(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	newHistoryRenderer(p).Write(sampleHistory())

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 12, "a header plus one line per event")

	require.True(t, strings.HasPrefix(lines[0], "  ID  AGE"), "header: %q", lines[0])
	require.Contains(t, lines[0], "DELTA")
	require.Contains(t, lines[0], "EVENT")
	require.Contains(t, lines[0], "DETAILS")

	// Event 1: right-aligned ID, an age relative to the injected clock and no
	// delta, because there is nothing before it.
	require.Contains(t, lines[1], "   1  10m00s ago   -")
	require.Contains(t, lines[1], "WorkflowExecutionStarted")
	require.Contains(t, lines[1], "type=OrderWorkflow")
	require.Contains(t, lines[1], "task_queue=orders")
	require.Contains(t, lines[1], `input={"total":4200}`)

	// The delta column is where stalls show up: three seconds inside an
	// activity is the interesting number on this history.
	require.Contains(t, lines[7], "+3.0s")
	require.Contains(t, lines[7], "ActivityTaskFailed")
	require.Contains(t, lines[7], "CardDeclined")
	require.Contains(t, lines[7], "non-retryable")
	require.Contains(t, lines[7], "retry_state=NonRetryableFailure")
}

func TestRenderHistoryIndentsCommandProducedEvents(t *testing.T) {
	t.Parallel()

	p, buf := newTestPrinter(false)
	newHistoryRenderer(p).Write(sampleHistory())
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")

	eventColumn := func(line string) string {
		// The event column starts after ID, AGE and DELTA plus their separators.
		start := colID + 2 + colAge + 2 + colDelta + 2
		return line[start:]
	}

	// The activity was scheduled by the commands of the workflow task completed
	// at event 4, so it is indented under it. This is the thing that makes a
	// history readable: "which decision caused this effect" is visible without
	// following back-references by eye.
	require.True(t, strings.HasPrefix(eventColumn(lines[5]), "  ActivityTaskScheduled"),
		"expected indentation, got %q", eventColumn(lines[5]))
	require.True(t, strings.HasPrefix(eventColumn(lines[11]), "  WorkflowExecutionFailed"))

	// Server-produced events are not indented: nothing decided them.
	require.True(t, strings.HasPrefix(eventColumn(lines[6]), "ActivityTaskStarted"))
	require.True(t, strings.HasPrefix(eventColumn(lines[2]), "WorkflowTaskScheduled"))
}

func TestRenderHistoryIsIncrementalForFollow(t *testing.T) {
	t.Parallel()

	full := sampleHistory()
	p, buf := newTestPrinter(false)
	r := newHistoryRenderer(p)

	r.Write(full[:4])
	r.Write(full[4:])

	// The header is written once, and the delta across the batch boundary is
	// computed against the previous batch's last event rather than restarting.
	require.Equal(t, 1, strings.Count(buf.String(), "DELTA"))
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 12)
	require.Contains(t, lines[5], "+1ms")
}

func TestEventDetailsCoverEveryEventType(t *testing.T) {
	t.Parallel()

	// A new event type with no details branch renders as a blank line, which is
	// exactly the kind of gap nobody notices until an incident.
	for _, typ := range history.KnownEventTypes() {
		attrs, err := history.NewAttributes(typ)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			eventDetails(history.Event{ID: 1, Time: fixedNow, Attrs: attrs})
		}, typ.String())
	}
}

func TestEventColorMarksFailuresRed(t *testing.T) {
	t.Parallel()

	p, _ := newTestPrinter(true)
	require.Contains(t, p.eventColor(history.EventTypeActivityTaskFailed, "x"), ansiRed)
	require.Contains(t, p.eventColor(history.EventTypeWorkflowExecutionTerminated, "x"), ansiYellow)
	require.Contains(t, p.eventColor(history.EventTypeActivityTaskCompleted, "x"), ansiGreen)
	require.NotContains(t, p.eventColor(history.EventTypeMarkerRecorded, "x"), ansiRed)
}

func TestCommandOrigin(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(4), commandOrigin(history.Event{
		Attrs: history.ActivityTaskScheduledAttributes{WorkflowTaskCompletedEventID: 4},
	}))
	require.Equal(t, int64(0), commandOrigin(history.Event{
		Attrs: history.ActivityTaskStartedAttributes{ScheduledEventID: 5},
	}))
}

func TestShortID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "abc", shortID("abc"))
	require.Equal(t, "0f3a1b2c", shortID("0f3a1b2c-dead-beef-0000-000000000000"))
}
