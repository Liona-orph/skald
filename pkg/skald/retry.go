package skald

import (
	"errors"
	"math"
	"time"
)

// RetryPolicy describes how a failed activity or workflow attempt is retried.
//
// The zero value is not usable; call DefaultRetryPolicy and adjust. A nil
// *RetryPolicy means "retry with the defaults", which matches the intuition
// that forgetting to configure retries should not mean losing them.
type RetryPolicy struct {
	// InitialInterval is the delay before the second attempt.
	InitialInterval time.Duration `json:"initial_interval"`
	// BackoffCoefficient multiplies the interval after every attempt. A value
	// of 1 produces a constant delay.
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	// MaximumInterval caps the exponential growth. Zero means 100x the initial
	// interval, which keeps a misconfigured policy from sleeping for years.
	MaximumInterval time.Duration `json:"maximum_interval,omitempty"`
	// MaximumAttempts bounds the total number of attempts including the first.
	// Zero means unlimited, bounded only by ScheduleToCloseTimeout.
	MaximumAttempts int32 `json:"maximum_attempts,omitempty"`
	// NonRetryableErrorTypes lists ApplicationError.Type values that abort the
	// retry loop immediately.
	NonRetryableErrorTypes []string `json:"non_retryable_error_types,omitempty"`
}

// Retry policy defaults. They are deliberately conservative: a tight retry loop
// against a struggling dependency is how a partial outage becomes a full one.
const (
	DefaultInitialInterval    = 1 * time.Second
	DefaultBackoffCoefficient = 2.0
	DefaultMaximumInterval    = 100 * time.Second
	// DefaultJitterFraction spreads retries of a thundering herd over a window
	// proportional to the computed backoff.
	DefaultJitterFraction = 0.2
)

// DefaultRetryPolicy returns the policy applied when a caller supplies none.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		InitialInterval:    DefaultInitialInterval,
		BackoffCoefficient: DefaultBackoffCoefficient,
		MaximumInterval:    DefaultMaximumInterval,
	}
}

// ErrInvalidRetryPolicy is returned by Validate.
var ErrInvalidRetryPolicy = errors.New("skald: invalid retry policy")

// Validate normalizes the policy in place and reports unusable configurations.
//
// Normalization happens once, at the API boundary, so that every later consumer
// (scheduler, replayer, CLI renderer) sees fully specified values and no code
// downstream has to re-implement "zero means default".
func (p *RetryPolicy) Validate() error {
	if p == nil {
		return nil
	}
	if p.InitialInterval == 0 {
		p.InitialInterval = DefaultInitialInterval
	}
	if p.BackoffCoefficient == 0 {
		p.BackoffCoefficient = DefaultBackoffCoefficient
	}
	if p.MaximumInterval == 0 {
		p.MaximumInterval = time.Duration(math.Min(
			float64(100*p.InitialInterval), float64(DefaultMaximumInterval)))
	}
	switch {
	case p.InitialInterval < 0:
		return errors.Join(ErrInvalidRetryPolicy, errors.New("initial interval must not be negative"))
	case p.BackoffCoefficient < 1:
		return errors.Join(ErrInvalidRetryPolicy, errors.New("backoff coefficient must be >= 1"))
	case p.MaximumInterval < p.InitialInterval:
		return errors.Join(ErrInvalidRetryPolicy, errors.New("maximum interval must be >= initial interval"))
	case p.MaximumAttempts < 0:
		return errors.Join(ErrInvalidRetryPolicy, errors.New("maximum attempts must not be negative"))
	}
	return nil
}

// ShouldRetry reports whether another attempt is permitted and, if so, how long
// to wait before it.
//
// jitterSeed makes the decision reproducible: the server draws a random value
// once, records it in the history event, and every subsequent replay recomputes
// the same delay. Randomness that is not written down is the most common source
// of non-determinism in workflow engines, so it is a parameter rather than a
// hidden call to the global RNG.
//
// attempt is 1-based and refers to the attempt that just failed.
func (p *RetryPolicy) ShouldRetry(attempt int32, err error, jitterSeed float64) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if !IsRetryable(err) {
		return 0, false
	}
	policy := p
	if policy == nil {
		policy = DefaultRetryPolicy()
	}
	if policy.MaximumAttempts > 0 && attempt >= policy.MaximumAttempts {
		return 0, false
	}
	if t := ErrorType(err); t != "" {
		for _, nonRetryable := range policy.NonRetryableErrorTypes {
			if nonRetryable == t {
				return 0, false
			}
		}
	}
	return policy.backoff(attempt, jitterSeed), true
}

// backoff computes interval = initial * coefficient^(attempt-1), clamped to
// MaximumInterval and then spread by up to DefaultJitterFraction.
func (p *RetryPolicy) backoff(attempt int32, jitterSeed float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	interval := float64(p.InitialInterval) * math.Pow(p.BackoffCoefficient, float64(attempt-1))
	if math.IsInf(interval, 0) || interval > float64(p.MaximumInterval) {
		interval = float64(p.MaximumInterval)
	}
	// Jitter is one-sided (never earlier than the nominal backoff) so that the
	// policy's stated interval remains a lower bound an operator can rely on.
	if jitterSeed < 0 {
		jitterSeed = 0
	} else if jitterSeed > 1 {
		jitterSeed = 1
	}
	interval *= 1 + DefaultJitterFraction*jitterSeed
	if interval > float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(interval)
}

// Clone returns a deep copy, used when a policy travels from a request struct
// into long-lived mutable state.
func (p *RetryPolicy) Clone() *RetryPolicy {
	if p == nil {
		return nil
	}
	out := *p
	if p.NonRetryableErrorTypes != nil {
		out.NonRetryableErrorTypes = append([]string(nil), p.NonRetryableErrorTypes...)
	}
	return &out
}
