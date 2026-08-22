package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Liona-orph/skald/pkg/skald"
)

// Format is the value of the global --output flag.
type Format string

const (
	// FormatTable is aligned, human-readable columns.
	FormatTable Format = "table"
	// FormatJSON is the machine-readable form. It prints the server's response
	// essentially verbatim, so a script never has to parse a table that was
	// designed for a person and may be reformatted in a later release.
	FormatJSON Format = "json"
)

// ParseFormat validates the --output value.
func ParseFormat(s string) (Format, error) {
	switch Format(strings.ToLower(strings.TrimSpace(s))) {
	case FormatTable, "":
		return FormatTable, nil
	case FormatJSON:
		return FormatJSON, nil
	}
	return "", fmt.Errorf("unknown output format %q (want %q or %q)", s, FormatTable, FormatJSON)
}

// ColorMode is the value of the global --color flag.
type ColorMode string

const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

// ParseColorMode validates the --color value.
func ParseColorMode(s string) (ColorMode, error) {
	switch ColorMode(strings.ToLower(strings.TrimSpace(s))) {
	case ColorAuto, "":
		return ColorAuto, nil
	case ColorAlways:
		return ColorAlways, nil
	case ColorNever:
		return ColorNever, nil
	}
	return "", fmt.Errorf("unknown color mode %q (want auto, always or never)", s)
}

// ANSI escapes. Only the handful that survive on every terminal worth
// supporting, and never a background colour: an operator's terminal may be
// light or dark and a hard-coded background is illegible on one of them.
const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiBlue   = "\x1b[34m"
	ansiCyan   = "\x1b[36m"
)

// Printer renders command output.
//
// Colour is a property of the printer rather than of each call site so that
// there is exactly one place that can get "am I writing to a pipe" wrong. When
// colour is off, the escape helpers return their input unchanged, which means
// the rendering code has no conditionals in it and the piped output is
// byte-identical to what a human sees minus the escapes.
type Printer struct {
	out    io.Writer
	format Format
	color  bool
	now    func() time.Time
}

// NewPrinter builds a Printer.
func NewPrinter(out io.Writer, format Format, color bool, now func() time.Time) *Printer {
	if now == nil {
		now = time.Now
	}
	return &Printer{out: out, format: format, color: color, now: now}
}

// Out returns the destination writer.
func (p *Printer) Out() io.Writer { return p.out }

// Format returns the configured output format.
func (p *Printer) Format() Format { return p.format }

// Now returns the printer's clock, injected so that relative timestamps are
// deterministic in tests.
func (p *Printer) Now() time.Time { return p.now() }

// Colored reports whether escapes will be emitted.
func (p *Printer) Colored() bool { return p.color }

func (p *Printer) paint(code, s string) string {
	if !p.color || s == "" {
		return s
	}
	return code + s + ansiReset
}

func (p *Printer) bold(s string) string   { return p.paint(ansiBold, s) }
func (p *Printer) dim(s string) string    { return p.paint(ansiDim, s) }
func (p *Printer) red(s string) string    { return p.paint(ansiRed, s) }
func (p *Printer) green(s string) string  { return p.paint(ansiGreen, s) }
func (p *Printer) yellow(s string) string { return p.paint(ansiYellow, s) }
func (p *Printer) blue(s string) string   { return p.paint(ansiBlue, s) }
func (p *Printer) cyan(s string) string   { return p.paint(ansiCyan, s) }

// JSON writes v as indented JSON.
func (p *Printer) JSON(v any) error {
	enc := json.NewEncoder(p.out)
	enc.SetIndent("", "  ")
	// Payload bytes are already-encoded JSON; HTML escaping would rewrite them
	// and make the output differ from what the server stored.
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// Table writes aligned columns.
//
// tabwriter, not fixed widths: a workflow ID is a business key that can be
// eight characters or eighty, and a fixed-width column either truncates the
// long one -- destroying the only value anyone came to copy -- or wastes a
// third of the terminal on the short one.
func (p *Printer) Table(headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(p.out, 0, 0, 2, ' ', 0)
	if len(headers) > 0 {
		painted := make([]string, len(headers))
		for i, h := range headers {
			painted[i] = p.bold(h)
		}
		if _, err := fmt.Fprintln(tw, strings.Join(painted, "\t")); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(tw, strings.Join(row, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// Printf writes a formatted line to the output stream.
func (p *Printer) Printf(format string, args ...any) {
	fmt.Fprintf(p.out, format, args...)
}

// StatusColor paints a workflow status with the colour an operator expects:
// green for the one outcome that needs no attention, red for the ones that do,
// yellow for the ones that are somebody's deliberate intervention.
func (p *Printer) StatusColor(status skald.WorkflowStatus) string {
	s := status.String()
	switch status {
	case skald.StatusCompleted:
		return p.green(s)
	case skald.StatusFailed, skald.StatusTimedOut:
		return p.red(s)
	case skald.StatusCanceled, skald.StatusTerminated:
		return p.yellow(s)
	case skald.StatusRunning:
		return p.cyan(s)
	}
	return s
}

// ---------------------------------------------------------------------------
// Time formatting
// ---------------------------------------------------------------------------

// CompactDuration renders a duration in at most two units.
//
// "2h13m" rather than "2h13m41.283s". At the point someone is reading a
// history, the question is "roughly when relative to everything else", and the
// extra precision costs column width that the details need more.
func CompactDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Second:
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	case d < time.Minute:
		// One decimal below a minute: the gap between an activity scheduled and
		// started is frequently sub-second and rounding it to "0s" hides the
		// dispatch latency that is often the thing being investigated.
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + "s"
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%02dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// Relative renders when t happened with respect to now.
func Relative(now, t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	if d < 0 {
		return "in " + CompactDuration(-d)
	}
	if d < 500*time.Millisecond {
		return "just now"
	}
	return CompactDuration(d) + " ago"
}

// Delta renders the gap between consecutive events.
func Delta(d time.Duration) string {
	if d < 0 {
		// History time never goes backwards -- history.Validate enforces it --
		// so seeing this means the file was hand-edited or corrupted.
		return "!" + CompactDuration(d)
	}
	return "+" + CompactDuration(d)
}

// ---------------------------------------------------------------------------
// Payload rendering
// ---------------------------------------------------------------------------

// maxPayloadPreview bounds how much of a payload is printed inline.
//
// A payload can be two megabytes. Printing it into a table destroys the table
// and, more to the point, buries the twenty other events that provide the
// context. `--output json` prints it in full for anyone who needs it.
const maxPayloadPreview = 96

// PayloadPreview renders a payload for inline display.
func PayloadPreview(p *skald.Payload) string {
	if p == nil {
		return ""
	}
	if p.IsNil() {
		return "nil"
	}
	if p.Encoding != skald.EncodingJSON {
		return fmt.Sprintf("<%s %d bytes>", p.Encoding, len(p.Data))
	}
	s := strings.TrimSpace(string(p.Data))
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxPayloadPreview {
		return s[:maxPayloadPreview] + "..."
	}
	return s
}

// TruncateMiddle shortens s while keeping both ends.
//
// Both ends, because the discriminating part of an identifier is as often the
// suffix ("order-2024-000871") as the prefix, and a tail-truncated list of them
// is a list of identical strings.
func TruncateMiddle(s string, max int) string {
	if max < 5 || len(s) <= max {
		return s
	}
	keep := max - 3
	head := (keep + 1) / 2
	tail := keep - head
	return s[:head] + "..." + s[len(s)-tail:]
}
