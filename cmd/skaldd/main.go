// Command skaldd is the Skald durable execution server.
//
// It wires the pieces that live in separate packages -- a persistence driver,
// the engine, the matching layer, the durable timer service, telemetry and the
// HTTP frontend -- into one process, and it owns the two things a process must
// get right that a library cannot: configuration and shutdown.
//
//	skaldd --store sqlite --sqlite-path /var/lib/skald/skald.db --addr :7233
//
// Run `skaldd --help` for the configuration precedence rules.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/skald-io/skald/internal/engine"
	"github.com/skald-io/skald/internal/frontend"
	"github.com/skald-io/skald/internal/matching"
	"github.com/skald-io/skald/internal/persistence"
	"github.com/skald-io/skald/internal/persistence/memory"
	"github.com/skald-io/skald/internal/persistence/sqlite"
	"github.com/skald-io/skald/internal/telemetry"
)

// Build information, injected at link time:
//
//	go build -ldflags "\
//	  -X main.version=$(git describe --tags) \
//	  -X main.commit=$(git rev-parse --short HEAD) \
//	  -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
//
// These are package-level vars only because -X is the mechanism the toolchain
// offers; nothing writes to them after link time.
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		// flag already printed its own message for a parse error or -h.
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "skaldd: "+err.Error())
		os.Exit(1)
	}
}

// run is main's body with the process boundary factored out, so that the whole
// startup path is exercisable from a test.
func run(args []string, stdout, stderr io.Writer) error {
	cfg, showVersion, err := LoadConfig(args, os.LookupEnv, os.ReadFile, stderr)
	if err != nil {
		return err
	}
	if showVersion {
		fmt.Fprintln(stdout, versionString())
		return nil
	}

	level, err := telemetry.ParseLevel(cfg.LogLevel)
	if err != nil {
		return err
	}
	format, err := telemetry.ParseLogFormat(cfg.LogFormat)
	if err != nil {
		return err
	}
	tel, err := telemetry.New(telemetry.Config{
		ServiceName:           "skald",
		ServiceVersion:        version,
		LogOutput:             stderr,
		LogFormat:             format,
		LogLevel:              level,
		CollectRuntimeMetrics: cfg.RuntimeMetrics,
		// No span exporter: tracing is on, wired and free until an operator
		// supplies a collector. See telemetry.newTracerProvider.
	})
	if err != nil {
		return err
	}
	log := tel.Log()

	log.Info("starting skaldd",
		"version", version, "commit", commit, "build_date", buildDate,
		"go", runtime.Version(),
	)
	// The effective configuration, after every layer has been merged, is the
	// single most useful line in the log: it turns "which of the four places
	// this could be set actually won" into a lookup instead of an experiment.
	log.Info("effective configuration", "config", cfg)

	// Signals are trapped before anything is opened, so that a SIGTERM arriving
	// during a slow migration still reaches this process's handler rather than
	// killing it with the store half-initialised.
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	store, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("closing store", telemetry.KeyError, err.Error())
		}
	}()

	// The matcher is constructed here rather than left to the engine because
	// this is the only place that knows where its metrics should go. The engine
	// therefore does not own it, and closing it is this function's job.
	matcher := matching.New(matching.Config{Metrics: tel.Metrics.MatchingMetrics()})
	defer matcher.Close()

	eng, err := engine.New(engine.Config{
		Store:            store,
		Matcher:          matcher,
		DefaultNamespace: cfg.Namespace,
		TimerInterval:    cfg.TimerInterval,
		Logger:           log,
	})
	if err != nil {
		return err
	}

	// Recovery before serving, not after. Task queues are derived state (see
	// internal/persistence): until the open executions have been scanned and
	// their pending tasks re-materialised, a worker polling this process would
	// be told there is no work when there is. engine.Start runs Recover first
	// and only then arms the timer service.
	startedAt := time.Now()
	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("engine recovery: %w", err)
	}
	log.Info("engine recovered", "duration_ms", time.Since(startedAt).Milliseconds())

	srv, err := frontend.New(frontend.Config{
		Service:         telemetry.InstrumentService(eng, tel, cfg.Namespace),
		Addr:            cfg.Addr,
		Telemetry:       tel,
		Logger:          log,
		AuthToken:       cfg.AuthToken,
		ReadyCheck:      storeReadyCheck(store, cfg.Namespace),
		MaxRequestBytes: cfg.MaxRequestBytes,
		RequestTimeout:  cfg.RequestTimeout,
		MaxPollDuration: cfg.MaxPollDuration,
		ShutdownTimeout: cfg.ShutdownTimeout,
		GzipThreshold:   cfg.GzipThreshold,
	})
	if err != nil {
		return err
	}
	if err := srv.Start(); err != nil {
		return err
	}
	log.Info("listening", "addr", srv.Addr(),
		"auth", cfg.AuthToken != "", "store", cfg.Store)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Wait() }()

	select {
	case err := <-serveErr:
		// The listener died on its own. There is nothing to drain.
		if err != nil {
			return fmt.Errorf("serving: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	// Releasing the signal handlers is what makes a second SIGINT or SIGTERM
	// terminate the process immediately with its default disposition. An
	// operator who sends the signal twice has decided the drain is not working,
	// and a process that ignores its own kill signal is worse than one that
	// exits ungracefully.
	stopSignals()
	log.Info("shutting down", "grace", cfg.ShutdownTimeout)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// The frontend goes first: it stops advertising readiness and releases
	// parked long polls, so workers move to another replica while this one
	// finishes the requests it already accepted. Only then is the engine closed,
	// because a request still in flight needs it.
	shutdownErr := srv.Shutdown(shutdownCtx)
	if err := eng.Close(shutdownCtx); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("engine: %w", err))
	}
	if err := tel.Shutdown(shutdownCtx); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("telemetry: %w", err))
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	log.Info("stopped cleanly")
	return nil
}

func versionString() string {
	return fmt.Sprintf("skaldd %s (commit %s, built %s, %s)", version, commit, buildDate, runtime.Version())
}

// openStore builds the configured persistence driver.
func openStore(ctx context.Context, cfg Config, log *slog.Logger) (persistence.Store, error) {
	switch cfg.Store {
	case StoreMemory:
		// Loud, because a memory store looks identical to a durable one until
		// the process restarts, at which point every running workflow is gone.
		log.Warn("using the in-memory store: all workflow state is lost on restart")
		return memory.New(), nil
	case StoreSQLite:
		store, err := sqlite.Open(ctx, cfg.SQLitePath)
		if err != nil {
			return nil, fmt.Errorf("open sqlite store: %w", err)
		}
		return store, nil
	}
	return nil, fmt.Errorf("unknown store %q", cfg.Store)
}

// storeReadyCheck builds the readiness probe's dependency check.
//
// A one-row visibility query is used rather than a ping: it exercises the same
// connection pool, the same query path and the same schema that real traffic
// does, so it fails when the store is degraded in ways a connection-level ping
// would sail straight past.
func storeReadyCheck(store persistence.Store, namespace string) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := store.ListExecutions(ctx, persistence.ListFilter{
			Namespace: namespace,
			PageSize:  1,
		})
		return err
	}
}
