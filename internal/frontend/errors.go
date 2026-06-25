package frontend

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/skald-io/skald/internal/telemetry"
	"github.com/skald-io/skald/pkg/api"
)

// defaultRetryAfter is used for the two codes where a client that retries
// immediately makes the situation worse and the server did not say how long to
// wait. One second is short enough not to strand a caller and long enough to
// break a synchronised retry storm when combined with the client's jitter.
const defaultRetryAfter = time.Second

// statusForCode is the single place an api error code becomes an HTTP status.
//
// It lives in one function, and everything that produces an error response goes
// through it, because the failure mode of a scattered mapping is a client that
// branches on 409 in one place and on "version_conflict" in another and gets
// them out of step during a refactor. The body is the authoritative signal --
// api.Error carries the code precisely so a proxy that rewrites the status
// cannot lie -- and the status is the compatibility layer for everything that
// only speaks HTTP.
//
// Two choices are worth defending:
//
//   - version_conflict and already_exists both map to 409 Conflict. They are
//     genuinely the same class of thing (the request lost a race against the
//     current state) and the code in the body distinguishes them for anyone who
//     needs to.
//   - failed_precondition maps to 412 rather than the 400 that gRPC's HTTP
//     gateway would choose. 400 says "your request is malformed", which is
//     wrong and actively misleading: signalling a completed workflow is a
//     perfectly well-formed request that the state machine refuses. 412 says
//     "the request was fine, the resource was not in the required state", which
//     is exactly what happened, and it keeps 400 meaning "fix your client".
func statusForCode(code string) int {
	switch code {
	case api.CodeInvalidArgument:
		return http.StatusBadRequest
	case api.CodeUnauthorized:
		return http.StatusUnauthorized
	case api.CodeNotFound:
		return http.StatusNotFound
	case api.CodeAlreadyExists, api.CodeVersionConflict:
		return http.StatusConflict
	case api.CodeFailedPrecondition:
		return http.StatusPreconditionFailed
	case api.CodeResourceExhausted:
		return http.StatusTooManyRequests
	case api.CodeUnavailable:
		return http.StatusServiceUnavailable
	case api.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case api.CodeInternal:
		return http.StatusInternalServerError
	}
	// An unrecognised code is a server-side bug -- the engine invented a code
	// this build does not know -- so it is a 500, not a 400. Blaming the client
	// for our vocabulary drift sends the operator looking in the wrong process.
	return http.StatusInternalServerError
}

// retryAfterFor returns the Retry-After value for a response, or zero when the
// header should be omitted.
//
// The header is only meaningful where a client is expected to try again with
// the identical request: a busy server (429) and an unavailable one (503). On a
// 404 or a 409 it would be an invitation to hammer a request that cannot start
// working, so it is deliberately absent there even if the error carries a
// duration.
func retryAfterFor(code string, hint time.Duration) time.Duration {
	switch code {
	case api.CodeResourceExhausted, api.CodeUnavailable:
		if hint > 0 {
			return hint
		}
		return defaultRetryAfter
	}
	return 0
}

// retryAfterHeader renders a duration as RFC 9110 delta-seconds.
//
// Rounding up and flooring at one second is intentional: the header has
// one-second granularity, and a Retry-After of 0 tells a client to retry
// immediately, which is the opposite of what a 429 means.
func retryAfterHeader(d time.Duration) string {
	secs := int64(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}

// asAPIError coerces any error into the protocol's envelope.
//
// Context errors are translated here rather than left to the generic branch
// because the frontend produces them itself -- a request timeout, a client that
// hung up -- and they carry no api.Error to unwrap.
func asAPIError(err error) *api.Error {
	var apiErr *api.Error
	if errors.As(err, &apiErr) && apiErr != nil {
		return apiErr
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &api.Error{Code: api.CodeDeadlineExceeded, Message: "request deadline exceeded"}
	case errors.Is(err, context.Canceled):
		// The caller went away. Nothing is wrong with the server, and the
		// response almost certainly goes nowhere, but the code has to be
		// something and "unavailable" is the one a client would retry on.
		return &api.Error{Code: api.CodeUnavailable, Message: "request canceled"}
	}
	// An error with no code is a bug: every path the engine can fail on has one.
	// The message is preserved because it is the only evidence, and the engine's
	// messages are written to be safe to show.
	return &api.Error{Code: api.CodeInternal, Message: err.Error()}
}

// writeError renders an error response using the default status mapping.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	apiErr := asAPIError(err)
	writeErrorStatus(w, r, statusForCode(apiErr.Code), apiErr)
}

// writeErrorStatus renders an error response with an explicit status.
//
// The override exists for the handful of cases where HTTP has a more precise
// status than the protocol has a code: an oversized body is 413, and the
// closest code is invalid_argument, whose default status is 400.
func writeErrorStatus(w http.ResponseWriter, r *http.Request, status int, apiErr *api.Error) {
	// Record the protocol code for the metrics middleware, so the `code` label
	// is the protocol's vocabulary rather than a status several codes share.
	telemetry.SetResultCode(r.Context(), apiErr.Code)

	if d := retryAfterFor(apiErr.Code, apiErr.RetryAfter); d > 0 {
		w.Header().Set("Retry-After", retryAfterHeader(d))
	}
	if apiErr.Code == api.CodeUnauthorized {
		// RFC 9110 requires a challenge on a 401. Without it a well-behaved
		// client cannot tell "you need credentials" from "your credentials are
		// wrong" and will not prompt for them.
		w.Header().Set("WWW-Authenticate", `Bearer realm="skald"`)
	}
	writeJSON(w, r, status, apiErr)
}
