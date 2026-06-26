package client

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/skald-io/skald/pkg/skald"
)

// Defaults. Every one of them is overridable, and every one of them is a number
// somebody will eventually need to justify, so the reasoning is here rather
// than in a changelog.
const (
	// DefaultRequestTimeout bounds one attempt at a non-polling call. It is
	// generous by the standards of an RPC because a StartWorkflow behind a cold
	// SQLite page cache is a disk seek, not a memory read, and a client that
	// gives up at 2s turns a slow start into a duplicate one.
	DefaultRequestTimeout = 30 * time.Second

	// DefaultPollTimeout bounds one attempt at a long-polling call.
	//
	// It must be strictly greater than the server's max poll duration
	// (frontend.DefaultMaxPollDuration, 50s) plus the time it takes to write
	// the response. If it were not, the client would abandon every poll a
	// fraction before the server answered it: every poll would look like a
	// timeout, the worker would log an error per poll cycle, and the task the
	// server was about to hand over would be dropped. Seventy seconds leaves
	// twenty for skew and for a slow response.
	DefaultPollTimeout = 70 * time.Second

	// DefaultMaxAttempts counts the first try. Four attempts with the default
	// intervals spans roughly a second of retrying, which covers a leader
	// change or a restarting replica without turning a caller's 30s budget into
	// a 5 minute one.
	DefaultMaxAttempts = 4

	// DefaultInitialInterval is the first backoff.
	DefaultInitialInterval = 100 * time.Millisecond
	// DefaultMaxInterval caps exponential growth.
	DefaultMaxInterval = 5 * time.Second
	// DefaultBackoffCoefficient is the growth factor.
	DefaultBackoffCoefficient = 2.0

	// DefaultMaxResponseBytes bounds a decoded response. A run's history is
	// capped at history.MaxHistoryBytes (64 MiB), and a GetHistory response
	// wraps one, so the limit has to be above that; it exists to stop a
	// misconfigured endpoint from making the client allocate without bound.
	DefaultMaxResponseBytes = 96 << 20
)

// RetryPolicy configures the client's automatic retries.
//
// This is the *transport* retry policy: it decides whether to send the same
// bytes again after a failure that says nothing about the request's validity.
// It is unrelated to skald.RetryPolicy, which decides whether the engine runs an
// activity again after it failed. They are deliberately different types because
// conflating them leads to a client that retries a business failure.
type RetryPolicy struct {
	// MaxAttempts includes the first attempt. One means "never retry".
	MaxAttempts int
	// InitialInterval is the delay before the second attempt.
	InitialInterval time.Duration
	// MaxInterval caps the exponentially growing delay.
	MaxInterval time.Duration
	// BackoffCoefficient multiplies the interval after each attempt.
	BackoffCoefficient float64
}

func (p RetryPolicy) validate() error {
	switch {
	case p.MaxAttempts < 1:
		return fmt.Errorf("client: retry policy needs at least one attempt, got %d", p.MaxAttempts)
	case p.InitialInterval < 0:
		return fmt.Errorf("client: retry policy has a negative initial interval")
	case p.MaxInterval < 0:
		return fmt.Errorf("client: retry policy has a negative maximum interval")
	case p.BackoffCoefficient < 1:
		return fmt.Errorf("client: retry policy coefficient %v would shrink the interval", p.BackoffCoefficient)
	}
	return nil
}

// DefaultRetryPolicy returns the policy used when none is configured.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:        DefaultMaxAttempts,
		InitialInterval:    DefaultInitialInterval,
		MaxInterval:        DefaultMaxInterval,
		BackoffCoefficient: DefaultBackoffCoefficient,
	}
}

// options is the mutable configuration an Option writes to.
type options struct {
	httpClient       *http.Client
	namespace        string
	identity         string
	authToken        string
	requestTimeout   time.Duration
	pollTimeout      time.Duration
	retry            RetryPolicy
	jitter           func() float64
	converter        skald.DataConverter
	logger           *slog.Logger
	newRequestID     func() string
	maxResponseBytes int64
	sdkName          string
	sdkVersion       string
}

// Option customises a Client.
type Option func(*options) error

// WithHTTPClient supplies the transport.
//
// The client sets its own per-attempt deadlines through the request context, so
// a supplied http.Client should generally leave Timeout at zero: an
// http.Client.Timeout is an absolute bound on the whole exchange and would kill
// a long poll that the client intends to hold for seventy seconds.
func WithHTTPClient(hc *http.Client) Option {
	return func(o *options) error {
		if hc == nil {
			return fmt.Errorf("client: nil http client")
		}
		o.httpClient = hc
		return nil
	}
}

// WithNamespace sets the namespace applied to requests that do not name one.
func WithNamespace(ns string) Option {
	return func(o *options) error {
		if err := skald.ValidateNamespace(ns); err != nil {
			return fmt.Errorf("client: %w", err)
		}
		o.namespace = ns
		return nil
	}
}

// WithIdentity sets the identity recorded on events this client causes.
//
// It ends up in history and in the engine's task-ownership checks, so it should
// name the process rather than the human: "billing-worker-7fd4" answers "who
// completed this task" during an incident, and "alice" does not.
func WithIdentity(identity string) Option {
	return func(o *options) error {
		o.identity = identity
		return nil
	}
}

// WithAuthToken sets the bearer token sent on every request.
func WithAuthToken(token string) Option {
	return func(o *options) error {
		o.authToken = token
		return nil
	}
}

// WithRequestTimeout bounds one attempt at a non-polling call.
func WithRequestTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("client: request timeout must be positive")
		}
		o.requestTimeout = d
		return nil
	}
}

// WithPollTimeout bounds one attempt at a long-polling call. See
// DefaultPollTimeout for why it must exceed the server's poll cap.
func WithPollTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("client: poll timeout must be positive")
		}
		o.pollTimeout = d
		return nil
	}
}

// WithRetryPolicy replaces the transport retry policy.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(o *options) error {
		if err := p.validate(); err != nil {
			return err
		}
		o.retry = p
		return nil
	}
}

// WithJitter injects the randomness used to decorrelate retries.
//
// The function must return a value in [0,1). It is injectable so that a test
// can pin the backoff and assert on exact delays; production has no reason to
// touch it.
func WithJitter(fn func() float64) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("client: nil jitter source")
		}
		o.jitter = fn
		return nil
	}
}

// WithDataConverter replaces the payload codec used by the ergonomic layer.
func WithDataConverter(dc skald.DataConverter) Option {
	return func(o *options) error {
		if dc == nil {
			return fmt.Errorf("client: nil data converter")
		}
		o.converter = dc
		return nil
	}
}

// WithLogger supplies a logger for retries and other operational events.
func WithLogger(log *slog.Logger) Option {
	return func(o *options) error {
		if log == nil {
			return fmt.Errorf("client: nil logger")
		}
		o.logger = log
		return nil
	}
}

// WithRequestIDFunc replaces the generator used to fill idempotency keys.
//
// The client fills RequestID on every operation that has one, so that its own
// retries deduplicate server side rather than starting a second workflow. A
// test that wants stable identifiers replaces the generator here.
func WithRequestIDFunc(fn func() string) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("client: nil request id generator")
		}
		o.newRequestID = fn
		return nil
	}
}

// WithMaxResponseBytes bounds a decoded response body.
func WithMaxResponseBytes(n int64) Option {
	return func(o *options) error {
		if n <= 0 {
			return fmt.Errorf("client: max response bytes must be positive")
		}
		o.maxResponseBytes = n
		return nil
	}
}

// WithSDKInfo records the SDK name and version on completed workflow tasks,
// which is how a history gets attributed to the client that produced it.
func WithSDKInfo(name, version string) Option {
	return func(o *options) error {
		o.sdkName, o.sdkVersion = name, version
		return nil
	}
}
