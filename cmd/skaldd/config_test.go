package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// envMap turns a map into a LookupEnv, so a test never mutates the process
// environment -- which is global state shared with every other test in the
// package and with anything running in parallel.
func envMap(pairs map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		v, ok := pairs[key]
		return v, ok
	}
}

func fileMap(files map[string]string) ReadFile {
	return func(path string) ([]byte, error) {
		content, ok := files[path]
		if !ok {
			return nil, errors.New("no such file")
		}
		return []byte(content), nil
	}
}

func noFiles(string) ([]byte, error) { return nil, errors.New("no such file") }

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, version, err := LoadConfig(nil, envMap(nil), noFiles, io.Discard)
	require.NoError(t, err)
	require.False(t, version)
	require.Equal(t, DefaultConfig(), cfg)
}

func TestLoadConfigPrecedence(t *testing.T) {
	t.Parallel()

	file := `{
		"addr": "file:1",
		"store": "sqlite",
		"sqlite_path": "/from/file.db",
		"namespace": "from-file",
		"request_timeout": "11s"
	}`
	env := envMap(map[string]string{
		"SKALD_ADDR":      "env:2",
		"SKALD_NAMESPACE": "from-env",
	})
	args := []string{"--config", "skald.json", "--addr", "flag:3"}

	cfg, _, err := LoadConfig(args, env, fileMap(map[string]string{"skald.json": file}), io.Discard)
	require.NoError(t, err)

	// Flags beat the environment, the environment beats the file, and the file
	// beats the defaults. Each layer must survive where the one above it is
	// silent.
	require.Equal(t, "flag:3", cfg.Addr)
	require.Equal(t, "from-env", cfg.Namespace)
	require.Equal(t, "/from/file.db", cfg.SQLitePath)
	require.Equal(t, StoreSQLite, cfg.Store)
	require.Equal(t, 11*time.Second, cfg.RequestTimeout)
	require.Equal(t, DefaultConfig().ShutdownTimeout, cfg.ShutdownTimeout)
}

func TestAFlagLeftAtItsDefaultDoesNotBeatTheEnvironment(t *testing.T) {
	t.Parallel()

	// The bug this guards against: reading every flag variable unconditionally,
	// so a flag nobody passed silently overwrites an environment variable with
	// the built-in default.
	env := envMap(map[string]string{"SKALD_ADDR": "env:2", "SKALD_LOG_LEVEL": "debug"})
	cfg, _, err := LoadConfig([]string{"--namespace", "explicit"}, env, noFiles, io.Discard)
	require.NoError(t, err)

	require.Equal(t, "env:2", cfg.Addr)
	require.Equal(t, "debug", cfg.LogLevel)
	require.Equal(t, "explicit", cfg.Namespace)
}

func TestConfigFileCanBeNamedByEnvironment(t *testing.T) {
	t.Parallel()

	cfg, _, err := LoadConfig(nil,
		envMap(map[string]string{"SKALD_CONFIG": "/etc/skald.json"}),
		fileMap(map[string]string{"/etc/skald.json": `{"addr":"from:file"}`}),
		io.Discard)
	require.NoError(t, err)
	require.Equal(t, "from:file", cfg.Addr)
}

func TestConfigFileErrors(t *testing.T) {
	t.Parallel()

	t.Run("missing file", func(t *testing.T) {
		_, _, err := LoadConfig([]string{"--config", "nope.json"}, envMap(nil), noFiles, io.Discard)
		require.ErrorContains(t, err, "read config file")
	})

	t.Run("unknown key", func(t *testing.T) {
		// A typo'd key silently ignored is a setting the operator believes is
		// applied and is not.
		_, _, err := LoadConfig([]string{"--config", "c.json"}, envMap(nil),
			fileMap(map[string]string{"c.json": `{"adr":"x"}`}), io.Discard)
		require.ErrorContains(t, err, "parse config file")
	})

	t.Run("bad duration", func(t *testing.T) {
		_, _, err := LoadConfig([]string{"--config", "c.json"}, envMap(nil),
			fileMap(map[string]string{"c.json": `{"request_timeout":"soon"}`}), io.Discard)
		require.ErrorContains(t, err, "request_timeout")
	})

	t.Run("absent keys leave lower layers alone", func(t *testing.T) {
		// Every field in the file struct is a pointer precisely so that an
		// unmentioned key does not erase what the environment set.
		cfg, _, err := LoadConfig(nil,
			envMap(map[string]string{"SKALD_AUTH_TOKEN": "secret"}),
			fileMap(map[string]string{"c.json": `{"addr":"x:1"}`}), io.Discard)
		require.NoError(t, err)
		require.Equal(t, "secret", cfg.AuthToken)
	})
}

func TestEnvironmentParsingErrors(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]string{
		"SKALD_MAX_REQUEST_BYTES": "lots",
		"SKALD_GZIP_THRESHOLD":    "big",
		"SKALD_RUNTIME_METRICS":   "maybe",
		"SKALD_REQUEST_TIMEOUT":   "soon",
	} {
		_, _, err := LoadConfig(nil, envMap(map[string]string{name: value}), noFiles, io.Discard)
		require.ErrorContains(t, err, name)
	}
}

func TestValidation(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		mutate  func(*Config)
		wantErr string
	}{
		"empty addr":        {func(c *Config) { c.Addr = "" }, "addr must not be empty"},
		"bad namespace":     {func(c *Config) { c.Namespace = "not a namespace!" }, "namespace"},
		"unknown store":     {func(c *Config) { c.Store = "postgres" }, "unknown store"},
		"sqlite no path":    {func(c *Config) { c.Store = StoreSQLite; c.SQLitePath = "" }, "sqlite_path"},
		"bad level":         {func(c *Config) { c.LogLevel = "shouty" }, "log level"},
		"bad format":        {func(c *Config) { c.LogFormat = "yaml" }, "log format"},
		"zero request size": {func(c *Config) { c.MaxRequestBytes = 0 }, "max_request_bytes"},
		"zero timeout":      {func(c *Config) { c.RequestTimeout = 0 }, "request_timeout"},
		"poll beyond idle":  {func(c *Config) { c.MaxPollDuration = 10 * time.Minute }, "idle timeout"},
		"negative gzip":     {func(c *Config) { c.GzipThreshold = -1 }, "gzip_threshold"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.wantErr)
		})
	}

	require.NoError(t, DefaultConfig().Validate())
}

func TestInvalidConfigurationFailsAtLoad(t *testing.T) {
	t.Parallel()

	// Failing here rather than at the first request matters: a server that
	// boots broken has already been added to the load balancer.
	_, _, err := LoadConfig([]string{"--store", "postgres"}, envMap(nil), noFiles, io.Discard)
	require.ErrorContains(t, err, "invalid configuration")
}

func TestVersionFlagShortCircuits(t *testing.T) {
	t.Parallel()

	// --version must work even alongside a configuration that would not
	// validate, because "what build is this" is a question you ask about a
	// broken deployment.
	_, showVersion, err := LoadConfig([]string{"--version", "--store", "postgres"}, envMap(nil), noFiles, io.Discard)
	require.NoError(t, err)
	require.True(t, showVersion)
}

func TestUnexpectedArguments(t *testing.T) {
	t.Parallel()

	_, _, err := LoadConfig([]string{"serve"}, envMap(nil), noFiles, io.Discard)
	require.ErrorContains(t, err, `unexpected argument "serve"`)

	_, _, err = LoadConfig([]string{"--nope"}, envMap(nil), noFiles, io.Discard)
	require.Error(t, err)
}

func TestHelpMentionsPrecedence(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	_, _, err := LoadConfig([]string{"-h"}, envMap(nil), noFiles, &out)
	require.ErrorIs(t, err, flag.ErrHelp)

	help := out.String()
	require.Contains(t, help, "highest priority first")
	require.Contains(t, help, "SKALD_CONFIG")
	require.Contains(t, help, "-store")
}

func TestConfigRedactsItsSecretWhenLogged(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := DefaultConfig()
	cfg.AuthToken = "hunter2"
	log.Info("effective configuration", "config", cfg)

	// Redaction lives on the type, so no call site can forget it.
	require.NotContains(t, buf.String(), "hunter2")
	require.Contains(t, buf.String(), "<redacted>")

	buf.Reset()
	log.Info("effective configuration", "config", DefaultConfig())
	require.Contains(t, buf.String(), "<unset>")
}

func TestRunPrintsVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	require.NoError(t, run([]string{"--version"}, &stdout, io.Discard))
	require.Contains(t, stdout.String(), "skaldd")
	require.Contains(t, stdout.String(), version)
}

func TestVersionStringCarriesBuildInfo(t *testing.T) {
	t.Parallel()

	s := versionString()
	require.Contains(t, s, version)
	require.Contains(t, s, commit)
	require.Contains(t, s, buildDate)
}
