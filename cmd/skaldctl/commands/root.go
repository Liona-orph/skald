// Package commands implements skaldctl, the operator CLI for Skald.
//
// Every command here is written for the person reading it during an incident,
// which drives three decisions:
//
//   - Help text names the situation the command is for, not just its flags. An
//     operator who has never used skaldctl before should be able to get from
//     `--help` to a useful invocation without a browser.
//   - Output is aligned and coloured for a terminal but plain and stable for a
//     pipe, and `--output json` always exists so nothing has to be scraped.
//   - Failure is loud and the exit code distinguishes "I could not reach the
//     server" from "the workflow itself failed", because a script that retries
//     the first and pages on the second is the normal thing to want.
package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/skald-io/skald/pkg/client"
	"github.com/skald-io/skald/pkg/skald"
)

// Environment variables the CLI reads. Flags win over them.
const (
	envAddress   = "SKALD_ADDRESS"
	envNamespace = "SKALD_NAMESPACE"
	envAuthToken = "SKALD_AUTH_TOKEN"
)

// DefaultAddress is where a local skaldd listens.
const DefaultAddress = "http://127.0.0.1:7233"

// Exit codes. They are part of the CLI's contract with whatever is scripting it.
const (
	// ExitOK is success.
	ExitOK = 0
	// ExitError is any failure of the command itself: bad arguments, an
	// unreachable server, a workflow that does not exist.
	ExitError = 1
	// ExitWorkflowFailed means the command worked perfectly and the *workflow*
	// did not. Keeping it distinct is what lets a deploy script retry a
	// transient connection failure without retrying a business failure.
	ExitWorkflowFailed = 2
)

// exitError carries the exit code a failure should produce.
type exitError struct {
	code int
	err  error
	// reported is set when the command already rendered the failure in its own
	// output. A workflow that failed is a *result*, printed as one; repeating
	// it as "skaldctl: ..." on stderr makes it look like two problems.
	reported bool
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

// ExitCodeFor returns the process exit code for an error returned by Execute.
func ExitCodeFor(err error) int {
	if err == nil {
		return ExitOK
	}
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	return ExitError
}

// ShouldReport reports whether the process still needs to print err to stderr.
func ShouldReport(err error) bool {
	if err == nil {
		return false
	}
	var ee *exitError
	if errors.As(err, &ee) {
		return !ee.reported
	}
	return true
}

// Env is the injectable environment a command tree runs in.
//
// It exists so that the whole CLI is testable without a process: a test
// supplies buffers, a fixed clock and its own client factory, and asserts on
// exact bytes.
type Env struct {
	Out io.Writer
	Err io.Writer
	// Now is the clock relative timestamps are computed against.
	Now func() time.Time
	// IsTerminal reports whether Out is a terminal, which is what `--color
	// auto` consults.
	IsTerminal func() bool
	// NewClient builds the API client from the resolved global options.
	NewClient func(Options) (*client.Client, error)
}

// Options is the resolved global configuration for one invocation.
type Options struct {
	Address   string
	Namespace string
	AuthToken string
	Timeout   time.Duration
	Format    Format
	Color     bool
}

// DefaultEnv returns the environment the real binary runs in.
func DefaultEnv() Env {
	return Env{
		Out:        os.Stdout,
		Err:        os.Stderr,
		Now:        time.Now,
		IsTerminal: func() bool { return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) },
		NewClient:  newDefaultClient,
	}
}

func newDefaultClient(o Options) (*client.Client, error) {
	opts := []client.Option{
		client.WithNamespace(o.Namespace),
		client.WithRequestTimeout(o.Timeout),
	}
	if o.AuthToken != "" {
		opts = append(opts, client.WithAuthToken(o.AuthToken))
	}
	return client.New(o.Address, opts...)
}

// root holds the state shared by every subcommand.
type root struct {
	env Env

	addressFlag   string
	namespaceFlag string
	tokenFlag     string
	timeoutFlag   time.Duration
	outputFlag    string
	colorFlag     string

	opts    Options
	printer *Printer

	client *client.Client
}

// NewRootCommand builds the skaldctl command tree.
func NewRootCommand(env Env) *cobra.Command {
	if env.Out == nil {
		env.Out = os.Stdout
	}
	if env.Err == nil {
		env.Err = os.Stderr
	}
	if env.Now == nil {
		env.Now = time.Now
	}
	if env.IsTerminal == nil {
		env.IsTerminal = func() bool { return false }
	}
	if env.NewClient == nil {
		env.NewClient = newDefaultClient
	}
	r := &root{env: env}

	cmd := &cobra.Command{
		Use:   "skaldctl",
		Short: "Inspect and control Skald workflows",
		Long: `skaldctl talks to a Skald server.

Start here when something is wrong:

  skaldctl workflow list --status RUNNING        what is still going
  skaldctl workflow describe order-1234          one execution, at a glance
  skaldctl workflow history order-1234           the whole story, in order
  skaldctl workflow history order-1234 --follow  ...as it happens

Everything accepts --output json, so nothing here has to be screen-scraped.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		// A bare `skaldctl` prints help rather than doing something surprising.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
		PersistentPreRunE: r.resolve,
	}
	cmd.SetOut(env.Out)
	cmd.SetErr(env.Err)

	flags := cmd.PersistentFlags()
	flags.StringVar(&r.addressFlag, "address", "", "Skald server URL (env "+envAddress+", default "+DefaultAddress+")")
	flags.StringVarP(&r.namespaceFlag, "namespace", "n", "", "namespace (env "+envNamespace+", default "+skald.DefaultNamespace+")")
	flags.StringVar(&r.tokenFlag, "auth-token", "", "bearer token (env "+envAuthToken+")")
	flags.DurationVar(&r.timeoutFlag, "timeout", 30*time.Second, "per-request timeout; long polls are exempt")
	flags.StringVarP(&r.outputFlag, "output", "o", string(FormatTable), "output format: table or json")
	flags.StringVar(&r.colorFlag, "color", string(ColorAuto), "colour output: auto, always or never")

	cmd.AddCommand(
		r.newWorkflowCommand(),
		r.newTaskQueueCommand(),
		r.newVersionCommand(),
	)
	return cmd
}

// resolve turns flags and environment into Options. It runs before every
// subcommand, including the ones that never build a client, so that a bad
// --output value fails immediately rather than after the request.
func (r *root) resolve(cmd *cobra.Command, _ []string) error {
	format, err := ParseFormat(r.outputFlag)
	if err != nil {
		return err
	}
	colorMode, err := ParseColorMode(r.colorFlag)
	if err != nil {
		return err
	}

	r.opts = Options{
		Address:   firstNonEmpty(r.addressFlag, os.Getenv(envAddress), DefaultAddress),
		Namespace: firstNonEmpty(r.namespaceFlag, os.Getenv(envNamespace), skald.DefaultNamespace),
		AuthToken: firstNonEmpty(r.tokenFlag, os.Getenv(envAuthToken)),
		Timeout:   r.timeoutFlag,
		Format:    format,
	}

	switch colorMode {
	case ColorAlways:
		r.opts.Color = true
	case ColorNever:
		r.opts.Color = false
	default:
		// Colour only for a terminal. Escapes in a file or a pipe corrupt
		// exactly the output someone is about to grep, and JSON with escape
		// sequences in it is not JSON.
		r.opts.Color = r.env.IsTerminal() && format == FormatTable
	}

	r.printer = NewPrinter(cmd.OutOrStdout(), format, r.opts.Color, r.env.Now)
	return nil
}

// Client returns the API client, building it on first use.
//
// Lazily, because several commands (`workflow replay`, `--help`) never touch
// the network and should not fail because a server is unreachable.
func (r *root) Client() (*client.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	c, err := r.env.NewClient(r.opts)
	if err != nil {
		return nil, err
	}
	r.client = c
	return c, nil
}

func (r *root) newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the client version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			r.printer.Printf("%s\n", VersionString())
			return nil
		},
	}
}

// Build information, injected at link time. See cmd/skaldd for the flags.
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

// VersionString renders the build information.
func VersionString() string {
	return fmt.Sprintf("skaldctl %s (commit %s, built %s)", version, commit, buildDate)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// commandContext returns the context a command's requests run under.
//
// Cancellation comes from the caller (main wires SIGINT), never from a timeout
// here: the per-request deadline lives in the client, where it can be different
// for a long poll than for a regular call. A blanket timeout at this level
// would kill `--follow` after thirty seconds.
func commandContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}
