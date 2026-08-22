package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/Liona-orph/skald/internal/frontend"
	"github.com/Liona-orph/skald/internal/telemetry"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Store drivers.
const (
	StoreMemory = "memory"
	StoreSQLite = "sqlite"
)

// Environment variable names. They are listed here rather than inlined so that
// `grep SKALD_ config.go` is a complete answer to "what can I set".
const (
	envConfig          = "SKALD_CONFIG"
	envAddr            = "SKALD_ADDR"
	envNamespace       = "SKALD_NAMESPACE"
	envStore           = "SKALD_STORE"
	envSQLitePath      = "SKALD_SQLITE_PATH"
	envLogLevel        = "SKALD_LOG_LEVEL"
	envLogFormat       = "SKALD_LOG_FORMAT"
	envAuthToken       = "SKALD_AUTH_TOKEN"
	envMaxRequestBytes = "SKALD_MAX_REQUEST_BYTES"
	envRequestTimeout  = "SKALD_REQUEST_TIMEOUT"
	envMaxPoll         = "SKALD_MAX_POLL"
	envShutdownTimeout = "SKALD_SHUTDOWN_TIMEOUT"
	envTimerInterval   = "SKALD_TIMER_INTERVAL"
	envGzipThreshold   = "SKALD_GZIP_THRESHOLD"
	envRuntimeMetrics  = "SKALD_RUNTIME_METRICS"
)

// Config is the server's effective configuration.
//
// # Precedence
//
// Highest wins:
//
//  1. Command-line flags, but only the ones actually given. A flag left at its
//     default does not beat an environment variable, which is the behaviour
//     people expect and the one a naive implementation gets wrong.
//  2. Environment variables (SKALD_*). This is the layer a container runtime,
//     a systemd unit or a secret manager writes to.
//  3. The optional config file named by --config or SKALD_CONFIG.
//  4. Built-in defaults.
//
// The order is "most specific to the invocation wins". A file is a property of
// the image, the environment is a property of the deployment, and a flag is a
// property of this one run -- most often an operator overriding both at 3am,
// which is exactly when the override had better take effect.
type Config struct {
	Addr      string `json:"addr"`
	Namespace string `json:"namespace"`

	Store      string `json:"store"`
	SQLitePath string `json:"sqlite_path"`

	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// AuthToken enables bearer authentication when non-empty. It is never
	// logged; see LogValue.
	AuthToken string `json:"auth_token"`

	MaxRequestBytes int64         `json:"max_request_bytes"`
	RequestTimeout  time.Duration `json:"request_timeout"`
	MaxPollDuration time.Duration `json:"max_poll_duration"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	TimerInterval   time.Duration `json:"timer_interval"`
	GzipThreshold   int           `json:"gzip_threshold"`

	RuntimeMetrics bool `json:"runtime_metrics"`
}

// DefaultConfig returns the configuration used when nothing overrides it.
func DefaultConfig() Config {
	return Config{
		Addr:            frontend.DefaultAddr,
		Namespace:       skald.DefaultNamespace,
		Store:           StoreMemory,
		SQLitePath:      "skald.db",
		LogLevel:        "info",
		LogFormat:       string(telemetry.LogFormatJSON),
		MaxRequestBytes: frontend.DefaultMaxRequestBytes,
		RequestTimeout:  frontend.DefaultRequestTimeout,
		MaxPollDuration: frontend.DefaultMaxPollDuration,
		ShutdownTimeout: frontend.DefaultShutdownTimeout,
		TimerInterval:   time.Second,
		GzipThreshold:   frontend.DefaultGzipThreshold,
		RuntimeMetrics:  true,
	}
}

// Validate rejects a configuration the server cannot honour.
//
// Everything here fails at startup rather than at the first request. A server
// that boots with a broken configuration and starts refusing traffic is
// indistinguishable, from the outside, from a server with a bug -- and it has
// already been added to the load balancer by the time anyone finds out.
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr must not be empty")
	}
	if err := skald.ValidateNamespace(c.Namespace); err != nil {
		return err
	}
	switch c.Store {
	case StoreMemory:
	case StoreSQLite:
		if c.SQLitePath == "" {
			return fmt.Errorf("sqlite_path must be set when store is %q", StoreSQLite)
		}
	default:
		return fmt.Errorf("unknown store %q (want %q or %q)", c.Store, StoreMemory, StoreSQLite)
	}
	if _, err := telemetry.ParseLevel(c.LogLevel); err != nil {
		return err
	}
	if _, err := telemetry.ParseLogFormat(c.LogFormat); err != nil {
		return err
	}
	if c.MaxRequestBytes <= 0 {
		return fmt.Errorf("max_request_bytes must be positive")
	}
	for name, d := range map[string]time.Duration{
		"request_timeout":   c.RequestTimeout,
		"max_poll_duration": c.MaxPollDuration,
		"shutdown_timeout":  c.ShutdownTimeout,
		"timer_interval":    c.TimerInterval,
	} {
		if d <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.MaxPollDuration >= frontend.DefaultIdleTimeout {
		return fmt.Errorf("max_poll_duration %s must stay below the connection idle timeout %s",
			c.MaxPollDuration, frontend.DefaultIdleTimeout)
	}
	if c.GzipThreshold < 0 {
		return fmt.Errorf("gzip_threshold must not be negative")
	}
	return nil
}

// LogValue implements slog.LogValuer.
//
// Redaction lives here, on the type, rather than at the one call site that logs
// the config today. A future call site that logs it cannot forget: there is no
// way to render a Config through slog that reveals the token.
func (c Config) LogValue() slog.Value {
	token := "<unset>"
	if c.AuthToken != "" {
		token = "<redacted>"
	}
	return slog.GroupValue(
		slog.String("addr", c.Addr),
		slog.String("namespace", c.Namespace),
		slog.String("store", c.Store),
		slog.String("sqlite_path", c.SQLitePath),
		slog.String("log_level", c.LogLevel),
		slog.String("log_format", c.LogFormat),
		slog.String("auth_token", token),
		slog.Int64("max_request_bytes", c.MaxRequestBytes),
		slog.Duration("request_timeout", c.RequestTimeout),
		slog.Duration("max_poll_duration", c.MaxPollDuration),
		slog.Duration("shutdown_timeout", c.ShutdownTimeout),
		slog.Duration("timer_interval", c.TimerInterval),
		slog.Int("gzip_threshold", c.GzipThreshold),
		slog.Bool("runtime_metrics", c.RuntimeMetrics),
	)
}

// ---------------------------------------------------------------------------
// File format
// ---------------------------------------------------------------------------

// fileConfig is the JSON shape of a config file.
//
// Every field is a pointer so that "absent" is distinguishable from "present
// and zero". Without that, a file that does not mention auth_token would erase
// one set in the environment, which is the classic configuration-merge bug.
//
// Durations are strings ("30s"), because a config file that expresses a timeout
// as 30000000000 is a config file nobody can review.
type fileConfig struct {
	Addr            *string `json:"addr,omitempty"`
	Namespace       *string `json:"namespace,omitempty"`
	Store           *string `json:"store,omitempty"`
	SQLitePath      *string `json:"sqlite_path,omitempty"`
	LogLevel        *string `json:"log_level,omitempty"`
	LogFormat       *string `json:"log_format,omitempty"`
	AuthToken       *string `json:"auth_token,omitempty"`
	MaxRequestBytes *int64  `json:"max_request_bytes,omitempty"`
	RequestTimeout  *string `json:"request_timeout,omitempty"`
	MaxPollDuration *string `json:"max_poll_duration,omitempty"`
	ShutdownTimeout *string `json:"shutdown_timeout,omitempty"`
	TimerInterval   *string `json:"timer_interval,omitempty"`
	GzipThreshold   *int    `json:"gzip_threshold,omitempty"`
	RuntimeMetrics  *bool   `json:"runtime_metrics,omitempty"`
}

func (f fileConfig) applyTo(c *Config) error {
	assignString(&c.Addr, f.Addr)
	assignString(&c.Namespace, f.Namespace)
	assignString(&c.Store, f.Store)
	assignString(&c.SQLitePath, f.SQLitePath)
	assignString(&c.LogLevel, f.LogLevel)
	assignString(&c.LogFormat, f.LogFormat)
	assignString(&c.AuthToken, f.AuthToken)
	if f.MaxRequestBytes != nil {
		c.MaxRequestBytes = *f.MaxRequestBytes
	}
	if f.GzipThreshold != nil {
		c.GzipThreshold = *f.GzipThreshold
	}
	if f.RuntimeMetrics != nil {
		c.RuntimeMetrics = *f.RuntimeMetrics
	}
	for _, d := range []struct {
		name string
		src  *string
		dst  *time.Duration
	}{
		{"request_timeout", f.RequestTimeout, &c.RequestTimeout},
		{"max_poll_duration", f.MaxPollDuration, &c.MaxPollDuration},
		{"shutdown_timeout", f.ShutdownTimeout, &c.ShutdownTimeout},
		{"timer_interval", f.TimerInterval, &c.TimerInterval},
	} {
		if d.src == nil {
			continue
		}
		parsed, err := time.ParseDuration(*d.src)
		if err != nil {
			return fmt.Errorf("config file: %s: %w", d.name, err)
		}
		*d.dst = parsed
	}
	return nil
}

func assignString(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// LookupEnv is the shape of os.LookupEnv, injected so that configuration
// loading is testable without mutating the process environment -- which is
// global state, and global state shared with every other test in the package.
type LookupEnv func(string) (string, bool)

// ReadFile is the shape of os.ReadFile, injected for the same reason.
type ReadFile func(string) ([]byte, error)

// LoadConfig resolves the effective configuration from flags, environment and
// an optional file. See Config for the precedence rules.
//
// showVersion reports whether --version was given, in which case the caller
// should print the build information and exit without validating anything else.
func LoadConfig(args []string, env LookupEnv, read ReadFile, out io.Writer) (cfg Config, showVersion bool, err error) {
	fs := flag.NewFlagSet("skaldd", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() { usage(out, fs) }

	defaults := DefaultConfig()
	var (
		configPath string
		version    bool
	)
	fs.StringVar(&configPath, "config", "", "path to a JSON config file")
	fs.BoolVar(&version, "version", false, "print version information and exit")

	// The flag variables start at the defaults so that -h shows real values,
	// but only the flags the operator actually passed are applied below.
	flagCfg := defaults
	fs.StringVar(&flagCfg.Addr, "addr", defaults.Addr, "address to listen on")
	fs.StringVar(&flagCfg.Namespace, "namespace", defaults.Namespace, "default namespace for requests that omit one")
	fs.StringVar(&flagCfg.Store, "store", defaults.Store, "persistence driver: memory or sqlite")
	fs.StringVar(&flagCfg.SQLitePath, "sqlite-path", defaults.SQLitePath, "database file when -store=sqlite")
	fs.StringVar(&flagCfg.LogLevel, "log-level", defaults.LogLevel, "debug, info, warn or error")
	fs.StringVar(&flagCfg.LogFormat, "log-format", defaults.LogFormat, "json or text")
	fs.StringVar(&flagCfg.AuthToken, "auth-token", defaults.AuthToken, "bearer token required on API requests (prefer "+envAuthToken+")")
	fs.Int64Var(&flagCfg.MaxRequestBytes, "max-request-bytes", defaults.MaxRequestBytes, "maximum decoded request body size")
	fs.DurationVar(&flagCfg.RequestTimeout, "request-timeout", defaults.RequestTimeout, "deadline for non-polling requests")
	fs.DurationVar(&flagCfg.MaxPollDuration, "max-poll", defaults.MaxPollDuration, "server-side cap on a long poll")
	fs.DurationVar(&flagCfg.ShutdownTimeout, "shutdown-timeout", defaults.ShutdownTimeout, "how long a graceful shutdown may take")
	fs.DurationVar(&flagCfg.TimerInterval, "timer-interval", defaults.TimerInterval, "how often the durable timer index is scanned")
	fs.IntVar(&flagCfg.GzipThreshold, "gzip-threshold", defaults.GzipThreshold, "response size at which gzip is applied")
	fs.BoolVar(&flagCfg.RuntimeMetrics, "runtime-metrics", defaults.RuntimeMetrics, "export Go runtime and process metrics")

	if err := fs.Parse(args); err != nil {
		return Config{}, false, err
	}
	if version {
		return Config{}, true, nil
	}
	if fs.NArg() > 0 {
		return Config{}, false, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	cfg = defaults

	// Layer 3: the file.
	if configPath == "" {
		if v, ok := env(envConfig); ok {
			configPath = v
		}
	}
	if configPath != "" {
		raw, err := read(configPath)
		if err != nil {
			return Config{}, false, fmt.Errorf("read config file %s: %w", configPath, err)
		}
		var fc fileConfig
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&fc); err != nil {
			return Config{}, false, fmt.Errorf("parse config file %s: %w", configPath, err)
		}
		if err := fc.applyTo(&cfg); err != nil {
			return Config{}, false, err
		}
	}

	// Layer 2: the environment.
	if err := applyEnv(&cfg, env); err != nil {
		return Config{}, false, err
	}

	// Layer 1: the flags that were actually given.
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "addr":
			cfg.Addr = flagCfg.Addr
		case "namespace":
			cfg.Namespace = flagCfg.Namespace
		case "store":
			cfg.Store = flagCfg.Store
		case "sqlite-path":
			cfg.SQLitePath = flagCfg.SQLitePath
		case "log-level":
			cfg.LogLevel = flagCfg.LogLevel
		case "log-format":
			cfg.LogFormat = flagCfg.LogFormat
		case "auth-token":
			cfg.AuthToken = flagCfg.AuthToken
		case "max-request-bytes":
			cfg.MaxRequestBytes = flagCfg.MaxRequestBytes
		case "request-timeout":
			cfg.RequestTimeout = flagCfg.RequestTimeout
		case "max-poll":
			cfg.MaxPollDuration = flagCfg.MaxPollDuration
		case "shutdown-timeout":
			cfg.ShutdownTimeout = flagCfg.ShutdownTimeout
		case "timer-interval":
			cfg.TimerInterval = flagCfg.TimerInterval
		case "gzip-threshold":
			cfg.GzipThreshold = flagCfg.GzipThreshold
		case "runtime-metrics":
			cfg.RuntimeMetrics = flagCfg.RuntimeMetrics
		}
	})

	if err := cfg.Validate(); err != nil {
		return Config{}, false, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, false, nil
}

func applyEnv(cfg *Config, env LookupEnv) error {
	str := func(name string, dst *string) {
		if v, ok := env(name); ok {
			*dst = v
		}
	}
	str(envAddr, &cfg.Addr)
	str(envNamespace, &cfg.Namespace)
	str(envStore, &cfg.Store)
	str(envSQLitePath, &cfg.SQLitePath)
	str(envLogLevel, &cfg.LogLevel)
	str(envLogFormat, &cfg.LogFormat)
	str(envAuthToken, &cfg.AuthToken)

	if v, ok := env(envMaxRequestBytes); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("%s: %w", envMaxRequestBytes, err)
		}
		cfg.MaxRequestBytes = n
	}
	if v, ok := env(envGzipThreshold); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", envGzipThreshold, err)
		}
		cfg.GzipThreshold = n
	}
	if v, ok := env(envRuntimeMetrics); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s: %w", envRuntimeMetrics, err)
		}
		cfg.RuntimeMetrics = b
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{
		{envRequestTimeout, &cfg.RequestTimeout},
		{envMaxPoll, &cfg.MaxPollDuration},
		{envShutdownTimeout, &cfg.ShutdownTimeout},
		{envTimerInterval, &cfg.TimerInterval},
	} {
		v, ok := env(d.name)
		if !ok {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
		*d.dst = parsed
	}
	return nil
}

func usage(out io.Writer, fs *flag.FlagSet) {
	fmt.Fprintf(out, `skaldd - the Skald durable execution server

Usage:
  skaldd [flags]

Configuration is resolved from, highest priority first:
  1. the flags below, but only the ones you actually pass
  2. environment variables (SKALD_ADDR, SKALD_STORE, SKALD_AUTH_TOKEN, ...)
  3. the JSON file named by --config or %s
  4. built-in defaults

The effective configuration is logged at startup with the auth token redacted,
so the first line of the log answers "what is this process actually running
with" without anyone having to reconstruct it from a deployment manifest.

Flags:
`, envConfig)
	fs.PrintDefaults()
}
