// Package client is Skald's Go client: an implementation of api.Service that
// speaks the HTTP/JSON protocol in pkg/api.
//
// # Two layers
//
// The lower layer is api.Service itself. Every method maps to exactly one
// endpoint, takes and returns the protocol's own types, and adds nothing. That
// is what lets a worker be pointed at either a *client.Client or an in-process
// engine with no code change, and it is why the engine's tests and the client's
// tests exercise the same interface.
//
// The upper layer is WorkflowHandle, which turns "poll the history until it
// reaches a terminal event, then decode the result or reconstruct the failure"
// into one method call. It is a convenience built strictly on top of the lower
// layer; nothing in it is privileged.
//
// # What the client does on your behalf
//
//   - Fills the namespace, identity and idempotency keys you did not supply, so
//     that its own retries cannot start a second workflow.
//   - Retries transport failures and server-side unavailability with
//     exponential backoff and jitter, and never retries a 4xx.
//   - Uses a separate, longer deadline for long-polling calls, so that the
//     general request timeout cannot kill a poll that is behaving correctly.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Liona-orph/skald/pkg/api"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Headers the client sets and reads. They mirror the frontend's constants; the
// two packages do not import each other because a public client should not
// depend on an internal server package.
const (
	headerRequestID       = "X-Request-Id"
	headerProtocolVersion = "Skald-Protocol-Version"
)

// Client implements api.Service over HTTP.
type Client struct {
	base *url.URL
	http *http.Client
	log  *slog.Logger

	namespace  string
	identity   string
	authToken  string
	sdkName    string
	sdkVersion string

	requestTimeout time.Duration
	pollTimeout    time.Duration
	retry          RetryPolicy
	jitter         func() float64

	converter        skald.DataConverter
	newRequestID     func() string
	maxResponseBytes int64
}

var _ api.Service = (*Client)(nil)

// New returns a Client talking to the Skald server at baseURL.
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("client: base URL is required")
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("client: parse base URL %q: %w", baseURL, err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("client: base URL %q must use http or https", baseURL)
	}

	o := options{
		namespace:        skald.DefaultNamespace,
		identity:         defaultIdentity(),
		requestTimeout:   DefaultRequestTimeout,
		pollTimeout:      DefaultPollTimeout,
		retry:            DefaultRetryPolicy(),
		jitter:           rand.Float64,
		converter:        skald.JSONConverter{},
		logger:           slog.New(discardHandler{}),
		newRequestID:     uuid.NewString,
		maxResponseBytes: DefaultMaxResponseBytes,
	}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if o.httpClient == nil {
		o.httpClient = defaultHTTPClient()
	}

	return &Client{
		base:             base,
		http:             o.httpClient,
		log:              o.logger,
		namespace:        o.namespace,
		identity:         o.identity,
		authToken:        o.authToken,
		sdkName:          o.sdkName,
		sdkVersion:       o.sdkVersion,
		requestTimeout:   o.requestTimeout,
		pollTimeout:      o.pollTimeout,
		retry:            o.retry,
		jitter:           o.jitter,
		converter:        o.converter,
		newRequestID:     o.newRequestID,
		maxResponseBytes: o.maxResponseBytes,
	}, nil
}

// defaultHTTPClient builds a transport tuned for a workflow client.
//
// The two settings that matter are the absent Timeout -- an absolute deadline
// would kill long polls, and per-attempt deadlines come from the context
// instead -- and the raised idle connection limits. A worker holds one poll per
// task queue permanently open, so the default MaxIdleConnsPerHost of two would
// make every third poll pay a fresh TCP and TLS handshake.
func defaultHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: transport}
}

// defaultIdentity names this process in the history it causes.
func defaultIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return strconv.Itoa(os.Getpid()) + "@" + host
}

// discardHandler is the logger a client uses when the caller supplied none.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

// BaseURL returns the server address this client was built for.
func (c *Client) BaseURL() string { return c.base.String() }

// Namespace returns the default namespace applied to requests that omit one.
func (c *Client) Namespace() string { return c.namespace }

// Identity returns the identity recorded on events this client causes.
func (c *Client) Identity() string { return c.identity }

// DataConverter returns the codec the ergonomic layer uses for payloads.
func (c *Client) DataConverter() skald.DataConverter { return c.converter }

// Close releases pooled connections. It is not required -- an idle connection
// times out on its own -- but a long-lived process that builds many clients
// should call it.
func (c *Client) Close() {
	if t, ok := c.http.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

// endpoint names one server operation.
type endpoint struct {
	path string
	// longPoll selects the poll deadline instead of the request deadline.
	longPoll bool
}

// attemptOutcome is what one HTTP round trip reports back to the retry loop.
type attemptOutcome struct {
	// err is nil on success.
	err *api.Error
	// retryable says whether sending the identical bytes again could plausibly
	// succeed.
	retryable bool
	// retryAfter is the server's instruction, from the header or the body.
	retryAfter time.Duration
}

// call is the single path every operation takes.
func call[Resp any](ctx context.Context, c *Client, ep endpoint, req any) (Resp, error) {
	var resp Resp
	body, err := json.Marshal(req)
	if err != nil {
		return resp, &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: fmt.Sprintf("client: encode request: %v", err),
		}
	}
	raw, err := c.send(ctx, ep, body)
	if err != nil {
		return resp, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return resp, nil
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return resp, &api.Error{
			Code:    api.CodeInternal,
			Message: fmt.Sprintf("client: decode response from %s: %v", ep.path, err),
		}
	}
	return resp, nil
}

// send runs the retry loop around attempt.
func (c *Client) send(ctx context.Context, ep endpoint, body []byte) ([]byte, error) {
	timeout := c.requestTimeout
	if ep.longPoll {
		timeout = c.pollTimeout
	}
	// One correlation ID for the whole logical call, reused across attempts, so
	// that a server-side log search for it returns every attempt of the request
	// the caller made rather than fragments that have to be stitched together.
	requestID := c.newRequestID()

	var last *api.Error
	for attempt := 1; ; attempt++ {
		raw, outcome := c.attempt(ctx, ep, body, timeout, requestID)
		if outcome.err == nil {
			return raw, nil
		}
		last = outcome.err

		// The caller's context takes precedence over any retry decision: a
		// cancelled caller does not want another attempt, however retryable the
		// failure was.
		if err := ctx.Err(); err != nil {
			return nil, contextAPIError(err)
		}
		if !outcome.retryable || attempt >= c.retry.MaxAttempts {
			break
		}

		delay := c.backoff(attempt, outcome.retryAfter)
		c.log.Debug("retrying request",
			"path", ep.path,
			"attempt", attempt,
			"delay_ms", delay.Milliseconds(),
			"code", outcome.err.Code,
			"request_id", requestID,
		)
		if err := sleep(ctx, delay); err != nil {
			return nil, contextAPIError(err)
		}
	}
	return nil, last
}

// attempt performs one HTTP round trip and classifies the result.
func (c *Client) attempt(ctx context.Context, ep endpoint, body []byte, timeout time.Duration, requestID string) ([]byte, attemptOutcome) {
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(actx, http.MethodPost, c.endpointURL(ep.path), bytes.NewReader(body))
	if err != nil {
		// Only a malformed URL gets here, which no retry can fix.
		return nil, attemptOutcome{err: &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: fmt.Sprintf("client: build request for %s: %v", ep.path, err),
		}}
	}
	c.setHeaders(httpReq, requestID)
	// Content-Length lets the server reject an oversized body before reading it.
	httpReq.ContentLength = int64(len(body))

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, c.transportOutcome(ctx, actx, ep, err)
	}
	defer func() {
		// Draining before closing is what lets the connection go back in the
		// pool. Closing an undrained body forces a new handshake on the next
		// call, which for a worker polling every fifty seconds is a real cost.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()

	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes))
	if readErr != nil {
		return nil, attemptOutcome{
			err: &api.Error{
				Code:    api.CodeUnavailable,
				Message: fmt.Sprintf("client: read response from %s: %v", ep.path, readErr),
			},
			// A truncated body is a connection that died mid-response; the
			// request may or may not have been applied, which is exactly what
			// the idempotency key is for.
			retryable: true,
		}
	}

	if resp.StatusCode/100 == 2 {
		return raw, attemptOutcome{}
	}

	apiErr := decodeError(raw, resp.StatusCode)
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	if retryAfter == 0 {
		retryAfter = apiErr.RetryAfter
	}
	return nil, attemptOutcome{
		err:        apiErr,
		retryable:  retryableResponse(resp.StatusCode, apiErr.Code),
		retryAfter: retryAfter,
	}
}

// transportOutcome classifies a failure that happened before a status arrived.
func (c *Client) transportOutcome(ctx, actx context.Context, ep endpoint, err error) attemptOutcome {
	switch {
	case ctx.Err() != nil:
		// The caller gave up. Not retryable, and the error should say so rather
		// than blaming the network.
		return attemptOutcome{err: contextAPIError(ctx.Err())}
	case actx.Err() != nil:
		// This attempt's own deadline. Retryable: the next attempt gets a fresh
		// one, and for a long poll it is simply another poll.
		return attemptOutcome{
			err: &api.Error{
				Code:    api.CodeDeadlineExceeded,
				Message: fmt.Sprintf("client: %s timed out after %s", ep.path, c.timeoutFor(ep)),
			},
			retryable: true,
		}
	default:
		// Connection refused, DNS failure, TLS handshake, reset. All transient
		// from the caller's point of view and all worth another attempt.
		return attemptOutcome{
			err: &api.Error{
				Code:    api.CodeUnavailable,
				Message: fmt.Sprintf("client: %s: %v", ep.path, err),
			},
			retryable: true,
		}
	}
}

func (c *Client) timeoutFor(ep endpoint) time.Duration {
	if ep.longPoll {
		return c.pollTimeout
	}
	return c.requestTimeout
}

func (c *Client) setHeaders(r *http.Request, requestID string) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	r.Header.Set(headerProtocolVersion, api.Version)
	r.Header.Set(headerRequestID, requestID)
	if c.authToken != "" {
		r.Header.Set("Authorization", "Bearer "+c.authToken)
	}
	// Accept-Encoding is deliberately not set: leaving it to net/http means the
	// transport advertises gzip and transparently decompresses the response,
	// which is what makes the server's compression of large histories free here.
}

// endpointURL resolves a protocol path against the base URL.
func (c *Client) endpointURL(path string) string {
	u := *c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + path
	return u.String()
}

// retryableResponse decides whether a response is worth another attempt.
//
// The rules, and why:
//
//   - api.CodeUnavailable is always retryable, whatever status carried it. The
//     body is the authoritative signal precisely because a proxy can rewrite
//     the status, and "unavailable" is the engine saying "this replica is
//     shutting down, ask again".
//   - 5xx is retryable. The server failed to answer, which says nothing about
//     whether the request was valid.
//   - 4xx is never retryable. A 4xx is a statement about *this request*, and
//     sending the same bytes again can only produce the same answer.
//
// That last rule includes 429 / resource_exhausted, which is the interesting
// case. A busy server is transient, so retrying looks right -- but a client
// that automatically retries a "you are sending too much" response is a client
// that responds to overload by generating more load, which is how a brownout
// becomes an outage. The error carries a RetryAfter and the caller can act on
// it with knowledge this layer does not have: whether the work is worth
// re-queuing, whether to shed it, whether to slow down globally.
func retryableResponse(status int, code string) bool {
	if code == api.CodeUnavailable {
		return true
	}
	return status >= 500
}

// decodeError turns an error response into an api.Error.
//
// A response from Skald carries the envelope. A response from a proxy, a load
// balancer or a captive portal carries HTML, so the status is used to
// synthesise a code and a bounded slice of the body is kept as evidence --
// without it, "the client got a 502" is unactionable.
func decodeError(raw []byte, status int) *api.Error {
	var apiErr api.Error
	if err := json.Unmarshal(raw, &apiErr); err == nil && apiErr.Code != "" {
		return &apiErr
	}
	msg := strings.TrimSpace(string(raw))
	const maxSnippet = 256
	if len(msg) > maxSnippet {
		msg = msg[:maxSnippet] + "..."
	}
	if msg == "" {
		msg = http.StatusText(status)
	}
	return &api.Error{
		Code:    codeForStatus(status),
		Message: fmt.Sprintf("client: server returned %d: %s", status, msg),
	}
}

// codeForStatus is the inverse of the frontend's status mapping, used only when
// the body did not carry a code.
//
// It is intentionally not exhaustive in the other direction: several codes share
// a status, so this picks the one a caller is most likely to branch on and the
// message preserves the rest.
func codeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType:
		return api.CodeInvalidArgument
	case http.StatusUnauthorized, http.StatusForbidden:
		return api.CodeUnauthorized
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return api.CodeNotFound
	case http.StatusConflict:
		return api.CodeAlreadyExists
	case http.StatusPreconditionFailed:
		return api.CodeFailedPrecondition
	case http.StatusTooManyRequests:
		return api.CodeResourceExhausted
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return api.CodeUnavailable
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return api.CodeDeadlineExceeded
	}
	return api.CodeInternal
}

// parseRetryAfter reads the RFC 9110 header in both of its forms.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// backoff computes the delay before the next attempt.
//
// The jitter is "equal jitter": half the interval fixed, half random. Full
// jitter -- a uniform draw over the whole interval -- decorrelates clients
// slightly better but can produce a near-zero delay, which sends a retry back
// at a server that failed microseconds ago. Keeping a floor costs a little
// synchronisation and buys the property that the interval actually grows.
//
// A server-supplied Retry-After overrides the computed delay when it is longer.
// It is never shortened: the server knows something the client does not, and
// ignoring it is how a thundering herd re-forms at the exact moment the server
// asked for room.
func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	interval := float64(c.retry.InitialInterval)
	maxInterval := float64(c.retry.MaxInterval)
	for i := 1; i < attempt; i++ {
		interval *= c.retry.BackoffCoefficient
		if maxInterval > 0 && interval >= maxInterval {
			interval = maxInterval
			break
		}
	}
	if maxInterval > 0 && interval > maxInterval {
		interval = maxInterval
	}

	j := c.jitter()
	if math.IsNaN(j) || j < 0 {
		j = 0
	} else if j >= 1 {
		j = math.Nextafter(1, 0)
	}
	delay := time.Duration(interval * (0.5 + 0.5*j))
	if retryAfter > delay {
		return retryAfter
	}
	return delay
}

// sleep waits for d unless ctx ends first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// contextAPIError renders a caller's cancellation in the protocol's envelope,
// so that a caller who branches on api.Error does not have to special-case the
// one failure the client produced by itself.
func contextAPIError(err error) *api.Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &api.Error{Code: api.CodeDeadlineExceeded, Message: "client: deadline exceeded"}
	}
	return &api.Error{Code: api.CodeUnavailable, Message: "client: request canceled"}
}

// ---------------------------------------------------------------------------
// Request defaulting
// ---------------------------------------------------------------------------

// applyStartDefaults fills in what the caller left blank.
//
// RequestID is the important one. StartWorkflow is idempotent *only* when it
// carries a request ID, so a client that retries a start without one can create
// two runs from one intent -- which for a payment workflow is the failure the
// entire system exists to prevent. Generating it here means the guarantee holds
// for every caller, including the ones who never read this comment.
func (c *Client) applyStartDefaults(req *api.StartWorkflowRequest) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	if req.RequestID == "" {
		req.RequestID = c.newRequestID()
	}
}

// ---------------------------------------------------------------------------
// api.Service: client-facing operations
// ---------------------------------------------------------------------------

// StartWorkflow implements api.Service.
func (c *Client) StartWorkflow(ctx context.Context, req api.StartWorkflowRequest) (api.StartWorkflowResponse, error) {
	c.applyStartDefaults(&req)
	return call[api.StartWorkflowResponse](ctx, c, endpoint{path: api.PathStartWorkflow}, req)
}

// SignalWorkflow implements api.Service.
func (c *Client) SignalWorkflow(ctx context.Context, req api.SignalWorkflowRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	if req.RequestID == "" {
		req.RequestID = c.newRequestID()
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathSignalWorkflow}, req)
	return err
}

// SignalWithStartWorkflow implements api.Service.
func (c *Client) SignalWithStartWorkflow(ctx context.Context, req api.SignalWithStartRequest) (api.StartWorkflowResponse, error) {
	c.applyStartDefaults(&req.Start)
	return call[api.StartWorkflowResponse](ctx, c, endpoint{path: api.PathSignalWithStart}, req)
}

// CancelWorkflow implements api.Service.
func (c *Client) CancelWorkflow(ctx context.Context, req api.CancelWorkflowRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathCancelWorkflow}, req)
	return err
}

// TerminateWorkflow implements api.Service.
func (c *Client) TerminateWorkflow(ctx context.Context, req api.TerminateWorkflowRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathTerminateWorkflow}, req)
	return err
}

// describeRequest is the JSON body of api.PathDescribeWorkflow.
//
// It mirrors the unexported type of the same name in internal/frontend, because
// pkg/api declares a Describe *response* but no request: the Service method
// takes three plain strings. Adding api.DescribeWorkflowRequest would delete
// both copies; until then, the two must change together.
type describeRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

// DescribeWorkflow implements api.Service.
func (c *Client) DescribeWorkflow(ctx context.Context, namespace, workflowID, runID string) (api.DescribeWorkflowResponse, error) {
	if namespace == "" {
		namespace = c.namespace
	}
	return call[api.DescribeWorkflowResponse](ctx, c, endpoint{path: api.PathDescribeWorkflow},
		describeRequest{Namespace: namespace, WorkflowID: workflowID, RunID: runID})
}

// GetHistory implements api.Service.
//
// The deadline depends on the request: WaitForNew turns this into a long poll,
// which must not be measured against the general request timeout.
func (c *Client) GetHistory(ctx context.Context, req api.GetHistoryRequest) (api.GetHistoryResponse, error) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	return call[api.GetHistoryResponse](ctx, c, endpoint{path: api.PathGetHistory, longPoll: req.WaitForNew}, req)
}

// ListWorkflows implements api.Service.
func (c *Client) ListWorkflows(ctx context.Context, req api.ListWorkflowsRequest) (api.ListWorkflowsResponse, error) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	return call[api.ListWorkflowsResponse](ctx, c, endpoint{path: api.PathListWorkflows}, req)
}

// ---------------------------------------------------------------------------
// api.Service: worker-facing operations
// ---------------------------------------------------------------------------

// PollWorkflowTask implements api.Service.
func (c *Client) PollWorkflowTask(ctx context.Context, req api.PollWorkflowTaskRequest) (api.WorkflowTask, error) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	if req.RequestID == "" {
		// A retried poll that reuses its request ID gets the same task back
		// instead of stranding the first one in "started" with nobody working
		// on it, which is the difference between a retry and a lost task.
		req.RequestID = c.newRequestID()
	}
	return call[api.WorkflowTask](ctx, c, endpoint{path: api.PathPollWorkflowTask, longPoll: true}, req)
}

// RespondWorkflowTaskCompleted implements api.Service.
func (c *Client) RespondWorkflowTaskCompleted(ctx context.Context, req api.RespondWorkflowTaskCompletedRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	if req.SDKName == "" {
		req.SDKName, req.SDKVersion = c.sdkName, c.sdkVersion
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathCompleteWorkflow}, req)
	return err
}

// RespondWorkflowTaskFailed implements api.Service.
func (c *Client) RespondWorkflowTaskFailed(ctx context.Context, req api.RespondWorkflowTaskFailedRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathFailWorkflowTask}, req)
	return err
}

// PollActivityTask implements api.Service.
func (c *Client) PollActivityTask(ctx context.Context, req api.PollActivityTaskRequest) (api.ActivityTask, error) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	if req.RequestID == "" {
		req.RequestID = c.newRequestID()
	}
	return call[api.ActivityTask](ctx, c, endpoint{path: api.PathPollActivityTask, longPoll: true}, req)
}

// RespondActivityTaskCompleted implements api.Service.
func (c *Client) RespondActivityTaskCompleted(ctx context.Context, req api.RespondActivityTaskCompletedRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathCompleteActivity}, req)
	return err
}

// RespondActivityTaskFailed implements api.Service.
func (c *Client) RespondActivityTaskFailed(ctx context.Context, req api.RespondActivityTaskFailedRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathFailActivity}, req)
	return err
}

// RespondActivityTaskCanceled implements api.Service.
func (c *Client) RespondActivityTaskCanceled(ctx context.Context, req api.RespondActivityTaskCanceledRequest) error {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	_, err := call[struct{}](ctx, c, endpoint{path: api.PathCancelActivity}, req)
	return err
}

// RecordActivityHeartbeat implements api.Service.
func (c *Client) RecordActivityHeartbeat(ctx context.Context, req api.RecordActivityHeartbeatRequest) (api.RecordActivityHeartbeatResponse, error) {
	if req.Namespace == "" {
		req.Namespace = c.namespace
	}
	if req.Identity == "" {
		req.Identity = c.identity
	}
	return call[api.RecordActivityHeartbeatResponse](ctx, c, endpoint{path: api.PathHeartbeatActivity}, req)
}

// ---------------------------------------------------------------------------
// Operational probes
// ---------------------------------------------------------------------------

// Ready reports whether the server is willing to serve traffic.
//
// It is a GET, so it does not go through the POST-shaped call path. The CLI
// uses it to turn "connection refused" into a sentence an operator can act on.
func (c *Client) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL(api.PathReady), nil)
	if err != nil {
		return &api.Error{Code: api.CodeInvalidArgument, Message: err.Error()}
	}
	c.setHeaders(req, c.newRequestID())

	resp, err := c.http.Do(req)
	if err != nil {
		return &api.Error{Code: api.CodeUnavailable, Message: fmt.Sprintf("client: %v", err)}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 == 2 {
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	return decodeError(raw, resp.StatusCode)
}
