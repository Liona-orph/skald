// Command skaldctl is the operator CLI for Skald.
//
//	skaldctl workflow list --status RUNNING
//	skaldctl workflow history order-1234 --follow
//	skaldctl taskqueue describe orders
//
// See `skaldctl --help` for the rest.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/skald-io/skald/cmd/skaldctl/commands"
)

func main() {
	// Ctrl-C has to reach a running `--follow` or a `workflow result` that is
	// parked on a long poll. Cancelling the command's context lets those return
	// cleanly instead of leaving a half-written line on the terminal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := commands.NewRootCommand(commands.DefaultEnv())
	if err := root.ExecuteContext(ctx); err != nil {
		// A command that already rendered the failure -- a workflow that
		// failed, a history that did not validate -- does not get it repeated
		// on stderr, where it would read as a second, separate problem.
		if commands.ShouldReport(err) {
			fmt.Fprintln(os.Stderr, "skaldctl: "+err.Error())
		}
		os.Exit(commands.ExitCodeFor(err))
	}
}
