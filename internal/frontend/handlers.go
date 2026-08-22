package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Liona-orph/skald/internal/telemetry"
	"github.com/Liona-orph/skald/pkg/api"
)

// describeRequest is the JSON body of api.PathDescribeWorkflow.
//
// pkg/api declares a response type for Describe but no request type, because
// api.Service.DescribeWorkflow takes three plain strings rather than a struct.
// The wire shape still has to exist, so it is declared here and mirrored in
// pkg/client.
//
// That duplication is a wart and it is deliberate rather than accidental: this
// package does not own pkg/api, and inventing a second home for a protocol type
// is worse than one honest copy with a pointer to the other. The fix is a
// DescribeWorkflowRequest in pkg/api, at which point both copies go away.
type describeRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

// ---------------------------------------------------------------------------
// Codec helpers
// ---------------------------------------------------------------------------

// decodeJSON reads a request body into T under every limit the server enforces.
//
// Strictness is the point:
//
//   - The body is capped, so a client cannot make the server allocate without
//     bound. The cap is applied by http.MaxBytesReader, which also stops the
//     connection from being read further, so an oversized upload is abandoned
//     rather than drained.
//   - Unknown fields are rejected. A client that sends `retry_polciy` has a bug,
//     and silently ignoring it produces a workflow with no retry policy and a
//     support ticket three weeks later. Failing at the boundary turns a silent
//     data-loss bug into an error at the call site.
//   - Trailing content is rejected, because `{"a":1}{"b":2}` is two documents
//     and accepting the first quietly hides whatever produced the second.
//
// It writes the error response itself and reports whether decoding succeeded,
// so handlers read as `req, ok := decodeJSON[...](...); if !ok { return }`.
func decodeJSON[T any](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, bool) {
	var out T
	if r.Body == nil {
		return out, true
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()

	switch err := dec.Decode(&out); {
	case err == nil:
	case errors.Is(err, io.EOF):
		// An empty body is a request with every field at its zero value. It is
		// almost always invalid, but the service layer produces a far better
		// message ("workflow id must not be empty") than the transport could.
		return out, true
	default:
		writeDecodeError(w, r, err, maxBytes)
		return out, false
	}

	// A second value in the stream means the client framed the request wrong.
	if dec.More() {
		writeError(w, r, &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: "request body contains more than one JSON document",
		})
		return out, false
	}
	return out, true
}

// writeDecodeError turns a json decoding failure into a useful message.
//
// "invalid character '}' looking for beginning of value" with no position is
// the kind of error that costs an engineer twenty minutes; the offset and the
// offending field are what make it a ten-second fix.
func writeDecodeError(w http.ResponseWriter, r *http.Request, err error, maxBytes int64) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		// 413 is the precise status and invalid_argument is the closest code:
		// the protocol has no "too large" code, and inventing one here would
		// mean a client had to know about a code pkg/api never declared.
		writeErrorStatus(w, r, http.StatusRequestEntityTooLarge, &api.Error{
			Code:    api.CodeInvalidArgument,
			Message: fmt.Sprintf("request body exceeds %d bytes", maxBytes),
			Details: map[string]string{"limit_bytes": strconv.FormatInt(maxBytes, 10)},
		})
		return
	}

	apiErr := &api.Error{Code: api.CodeInvalidArgument, Details: map[string]string{}}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntaxErr):
		apiErr.Message = "request body is not valid JSON"
		apiErr.Details["offset"] = strconv.FormatInt(syntaxErr.Offset, 10)
	case errors.Is(err, io.ErrUnexpectedEOF):
		// The body ended mid-value. Distinct from io.EOF (an empty body, which
		// is legal) and worth naming: a truncated request usually means the
		// client died or a proxy cut the upload, not that the JSON is wrong.
		apiErr.Message = "request body is not valid JSON: it ends mid-value"
	case errors.As(err, &typeErr):
		apiErr.Message = fmt.Sprintf("field %q expects %s", typeErr.Field, typeErr.Type)
		apiErr.Details["offset"] = strconv.FormatInt(typeErr.Offset, 10)
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		field := strings.TrimPrefix(err.Error(), "json: unknown field ")
		apiErr.Message = "request body contains an unknown field " + field
		apiErr.Details["field"] = strings.Trim(field, `"`)
	default:
		apiErr.Message = "request body could not be decoded: " + err.Error()
	}
	writeError(w, r, apiErr)
}

// writeJSON encodes a response body.
//
// The value is encoded into a buffer before any status is written. That costs
// one copy and buys the ability to fail cleanly: an encoder writing straight to
// the socket that hits an error halfway has already sent a 200 and half a
// document, and the client's only signal is a truncated body. Buffering also
// makes Content-Length available to the compression layer.
//
// The cost is real -- a full history response is held in memory twice, briefly
// -- which is why GetHistory takes a MaxEvents and clients that read large
// histories are expected to page.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Matching skald.JSONConverter: HTML escaping would rewrite payload bytes
	// that are already encoded, making the response differ from what was stored.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(body); err != nil {
		logFor(r.Context(), nil).Error("encoding response failed",
			telemetry.KeyError, err.Error(), "path", r.URL.Path)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		// A hand-written envelope, because the encoder just proved it cannot be
		// trusted with this response.
		_, _ = io.WriteString(w, `{"code":"internal","message":"failed to encode response"}`+"\n")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// writeOK is the success path: 200 plus the encoded body.
func writeOK(w http.ResponseWriter, r *http.Request, body any) {
	telemetry.SetResultCode(r.Context(), telemetry.CodeOK)
	writeJSON(w, r, http.StatusOK, body)
}

// emptyResponse is what an operation with no return value sends.
//
// A zero-length body would work but forces every client to special-case those
// endpoints; an empty JSON object lets one decode path serve all of them and
// leaves room to add fields later without a protocol break.
type emptyResponse struct{}

// ---------------------------------------------------------------------------
// Client-facing handlers
// ---------------------------------------------------------------------------

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.StartWorkflowRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	resp, err := s.svc.StartWorkflow(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

func (s *Server) handleSignalWorkflow(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.SignalWorkflowRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.SignalWorkflow(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleSignalWithStart(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.SignalWithStartRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	resp, err := s.svc.SignalWithStartWorkflow(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

func (s *Server) handleCancelWorkflow(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.CancelWorkflowRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.CancelWorkflow(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleTerminateWorkflow(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.TerminateWorkflowRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.TerminateWorkflow(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleDescribeWorkflow(w http.ResponseWriter, r *http.Request) {
	var req describeRequest
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		req = describeRequest{
			Namespace:  q.Get("namespace"),
			WorkflowID: q.Get("workflow_id"),
			RunID:      q.Get("run_id"),
		}
	} else {
		var ok bool
		if req, ok = decodeJSON[describeRequest](w, r, s.maxRequestBytes); !ok {
			return
		}
	}

	resp, err := s.svc.DescribeWorkflow(r.Context(), req.Namespace, req.WorkflowID, req.RunID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.GetHistoryRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}

	// The same endpoint is a fast read or a long poll depending on one field,
	// so the context and the metric classification are chosen per request
	// rather than per route.
	ctx := r.Context()
	if req.WaitForNew {
		var cancel context.CancelFunc
		ctx, cancel = s.pollContext(r)
		defer cancel()
		telemetry.MarkLongPoll(ctx)
	}

	resp, err := s.svc.GetHistory(ctx, req)
	if err != nil {
		// A wait that reached the server's cap is not a failure: the caller
		// gets what exists so far and polls again from NextEventID. Turning it
		// into a 504 would make every idle watcher look like an outage.
		if req.WaitForNew && pollExpired(ctx, r) {
			s.writeExpiredHistoryPoll(w, r, req)
			return
		}
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

// writeExpiredHistoryPoll answers a long poll that timed out with no new
// events. NextEventID echoes the caller's cursor so the next poll resumes
// exactly where this one started.
func (s *Server) writeExpiredHistoryPoll(w http.ResponseWriter, r *http.Request, req api.GetHistoryRequest) {
	next := req.FromEventID
	if next <= 0 {
		next = 1
	}
	writeOK(w, r, api.GetHistoryResponse{NextEventID: next})
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.ListWorkflowsRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	resp, err := s.svc.ListWorkflows(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

// ---------------------------------------------------------------------------
// Worker-facing handlers
// ---------------------------------------------------------------------------

func (s *Server) handlePollWorkflowTask(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.PollWorkflowTaskRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	ctx, cancel := s.pollContext(r)
	defer cancel()

	task, err := s.svc.PollWorkflowTask(ctx, req)
	if err != nil {
		if pollExpired(ctx, r) {
			// An expired poll is an empty task, not an error. A worker that
			// received a 504 every fifty seconds would log an error per idle
			// minute and page somebody for a healthy deployment.
			writeOK(w, r, api.WorkflowTask{Empty: true})
			return
		}
		writeError(w, r, err)
		return
	}
	writeOK(w, r, task)
}

func (s *Server) handleRespondWorkflowTaskCompleted(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RespondWorkflowTaskCompletedRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.RespondWorkflowTaskCompleted(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleRespondWorkflowTaskFailed(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RespondWorkflowTaskFailedRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.RespondWorkflowTaskFailed(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handlePollActivityTask(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.PollActivityTaskRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	ctx, cancel := s.pollContext(r)
	defer cancel()

	task, err := s.svc.PollActivityTask(ctx, req)
	if err != nil {
		if pollExpired(ctx, r) {
			writeOK(w, r, api.ActivityTask{Empty: true})
			return
		}
		writeError(w, r, err)
		return
	}
	writeOK(w, r, task)
}

func (s *Server) handleRespondActivityTaskCompleted(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RespondActivityTaskCompletedRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.RespondActivityTaskCompleted(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleRespondActivityTaskFailed(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RespondActivityTaskFailedRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.RespondActivityTaskFailed(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleRespondActivityTaskCanceled(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RespondActivityTaskCanceledRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	if err := s.svc.RespondActivityTaskCanceled(r.Context(), req); err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, emptyResponse{})
}

func (s *Server) handleRecordActivityHeartbeat(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeJSON[api.RecordActivityHeartbeatRequest](w, r, s.maxRequestBytes)
	if !ok {
		return
	}
	resp, err := s.svc.RecordActivityHeartbeat(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

// pollExpired reports whether a long poll ended because the server's cap or the
// client's own deadline elapsed, as opposed to a real failure.
//
// The client's context is checked as well as the poll context so that a caller
// who hung up is not answered with a 504 it will never read; both cases mean
// "nothing to report", and the empty response costs nothing to write.
func pollExpired(pollCtx context.Context, r *http.Request) bool {
	return pollCtx.Err() != nil || r.Context().Err() != nil
}

// ---------------------------------------------------------------------------
// Operational endpoints
// ---------------------------------------------------------------------------

// healthResponse is the body of /health and /ready.
type healthResponse struct {
	Status string `json:"status"`
	// Detail explains a non-serving state. It is omitted when everything is
	// fine so that the healthy response stays a constant string.
	Detail string `json:"detail,omitempty"`
}

// handleHealth answers the liveness question: is this process functioning?
//
// It touches nothing. Liveness exists so that a supervisor can decide whether
// *restarting the process* would help, and the honest answer to "the database
// is unreachable" is no -- restarting a healthy server because its store is
// down converts a dependency outage into an outage plus a crash loop, and
// throws away every in-flight long poll on the way.
//
// A draining server still reports live: it is running correctly, it has simply
// been asked to stop, and a supervisor that kills it during the drain defeats
// the drain.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeOK(w, r, healthResponse{Status: "ok"})
}

// handleReady answers the routing question: should this process receive traffic
// right now?
//
// It does check the store, because a server whose store is unreachable will
// fail every request it is given, and the correct response to that is for the
// load balancer to send the traffic to a replica whose store is fine -- not for
// a supervisor to restart this one. It also fails as soon as shutdown starts,
// which is what makes a rolling restart invisible: the process stops being
// routed to while it is still perfectly able to finish the work it has.
//
// The distinction is the whole reason there are two endpoints. Pointing a
// liveness probe at this handler is the single most effective way to turn a
// brief database blip into a cluster-wide restart storm.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.draining() {
		writeErrorStatus(w, r, http.StatusServiceUnavailable, &api.Error{
			Code:    api.CodeUnavailable,
			Message: "server is shutting down",
		})
		return
	}
	if s.readyCheck == nil {
		writeErrorStatus(w, r, http.StatusServiceUnavailable, &api.Error{
			Code:    api.CodeUnavailable,
			Message: "no readiness check configured",
		})
		return
	}
	if err := s.readyCheck(r.Context()); err != nil {
		logFor(r.Context(), s.log).Warn("readiness check failed", telemetry.KeyError, err.Error())
		writeErrorStatus(w, r, http.StatusServiceUnavailable, &api.Error{
			Code:    api.CodeUnavailable,
			Message: "dependency check failed",
			// The underlying error is logged, not returned: a readiness body is
			// scraped by infrastructure and frequently ends up somewhere less
			// trusted than the log, and store errors can carry connection
			// strings.
		})
		return
	}
	writeOK(w, r, healthResponse{Status: "ok"})
}

// handleMetrics serves the Prometheus exposition format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	telemetry.SetResultCode(r.Context(), telemetry.CodeOK)
	s.tel.MetricsHandler().ServeHTTP(w, r)
}

// handleNotFound answers an unrouted path in the protocol's envelope.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, &api.Error{
		Code:    api.CodeNotFound,
		Message: "no such endpoint",
		Details: map[string]string{"path": r.URL.Path},
	})
}
