// Package frontend adapts an api.Service to the HTTP/JSON wire protocol
// defined in pkg/api.
//
// # What this layer is responsible for
//
// Exactly one thing: turning bytes on a socket into Go values and back, with
// the operational concerns that only exist because there is a socket --
// authentication, request size limits, timeouts, compression, panic containment
// and the mapping between the protocol's error codes and HTTP statuses.
//
// It contains no workflow logic. Every handler decodes, calls one method of
// api.Service, and encodes. That is what makes an embedded deployment -- a
// worker talking to an in-process engine with no server at all -- exercise the
// same code as a networked one, and it is why a bug found in production can be
// reproduced in a unit test against the engine directly.
//
// # Long polls shape the design
//
// Two endpoints block for tens of seconds by design, and a third does so
// depending on one field of its request body. Everything unusual about this
// package follows from that: separate timeout treatment per route, a shutdown
// that cancels parked polls before it waits for in-flight requests, and a
// server-side poll cap chosen against the idle timeout of the proxies these
// requests travel through.
package frontend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Liona-orph/skald/internal/telemetry"
	"github.com/Liona-orph/skald/pkg/api"
)

// Defaults for Config.
const (
	// DefaultAddr binds the loopback interface. A durable execution engine with
	// no authentication configured should not be reachable from the network by
	// accident, so the default is the safe one and exposing it is a decision an
	// operator makes explicitly.
	DefaultAddr = "127.0.0.1:7233"

	// DefaultMaxRequestBytes bounds a decoded request body.
	//
	// It is deliberately larger than skald.MaxPayloadBytes (2 MiB): a
	// RespondWorkflowTaskCompleted may carry a batch of commands each with its
	// own payload, so a single legitimate request can be several payloads plus
	// framing. It is not *unbounded*, because an unbounded body is a
	// one-request denial of service against a process that has to hold the
	// decoded value in memory.
	DefaultMaxRequestBytes = 8 << 20

	// DefaultRequestTimeout bounds a non-polling request.
	DefaultRequestTimeout = 30 * time.Second

	// DefaultMaxPollDuration caps a long poll.
	//
	// Fifty seconds is not arbitrary. The idle timeouts a request crosses on
	// the way in are typically 60s: that is the default for an AWS ALB, for
	// nginx's proxy_read_timeout, for Envoy's route timeout and for most
	// ingress controllers. A poll that outlives one of those is killed *in
	// transit*, and the worker cannot tell that from a task it accepted and
	// dropped -- so the server must be the one that gives up first, and by
	// enough of a margin that clock skew and a slow response write cannot close
	// the gap. Fifty seconds leaves ten.
	//
	// The cap is also what makes shutdown bounded: a drain cannot take longer
	// than the longest poll a client is allowed to hold.
	DefaultMaxPollDuration = 50 * time.Second

	// DefaultReadyTimeout bounds the readiness probe's store check. A
	// kubelet's probe deadline is a second or two, so a readiness check with a
	// 30s timeout would be reported as a failure by the prober long before it
	// returned an answer.
	DefaultReadyTimeout = 3 * time.Second

	// DefaultShutdownTimeout bounds the graceful drain.
	DefaultShutdownTimeout = 20 * time.Second

	// DefaultGzipThreshold is the response size at which compression starts
	// paying for itself. Below roughly a kilobyte the gzip header, trailer and
	// compressor allocation cost more than the bytes saved.
	DefaultGzipThreshold = 1 << 10

	// DefaultReadHeaderTimeout bounds a client that opens a connection and
	// sends its request line slowly, which is the whole of the Slowloris attack.
	DefaultReadHeaderTimeout = 10 * time.Second

	// DefaultIdleTimeout closes a kept-alive connection nobody is using. It is
	// longer than the poll cap so that a worker's connection survives between
	// polls and does not pay a handshake per poll.
	DefaultIdleTimeout = 120 * time.Second
)

// Config parameterises a Server.
type Config struct {
	// Service is the implementation every handler delegates to. Required.
	Service api.Service

	// Addr is the listen address. Defaults to DefaultAddr.
	Addr string

	// Telemetry wires logs, metrics and traces. A private, unexported stack is
	// built when nil so that the server is usable without one.
	Telemetry *telemetry.Telemetry
	// Logger receives server-level events. Defaults to a discarding logger,
	// matching the engine: a component that logs somewhere its owner did not
	// choose is worse than one that stays quiet.
	Logger *slog.Logger

	// AuthToken, when set, requires `Authorization: Bearer <token>` on every
	// API route and on /metrics. /health and /ready stay open; see handleReady.
	AuthToken string

	// ReadyCheck is the readiness probe's dependency check. It should be cheap
	// and should actually touch the store. Readiness is always false when nil,
	// because a server that cannot say whether it is ready is not ready.
	ReadyCheck func(context.Context) error

	MaxRequestBytes int64
	RequestTimeout  time.Duration
	MaxPollDuration time.Duration
	ReadyTimeout    time.Duration
	ShutdownTimeout time.Duration
	GzipThreshold   int

	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
}

// Server is the HTTP front end.
type Server struct {
	svc api.Service
	tel *telemetry.Telemetry
	log *slog.Logger

	addr            string
	authToken       string
	readyCheck      func(context.Context) error
	maxRequestBytes int64
	requestTimeout  time.Duration
	maxPoll         time.Duration
	readyTimeout    time.Duration
	shutdownTimeout time.Duration
	gzip            *gzipCompressor

	http *http.Server

	mu       sync.Mutex
	listener net.Listener
	serveErr chan error

	// drain closes at the start of shutdown. It is what makes /ready fail and
	// what releases parked long polls before the graceful wait begins.
	drain     chan struct{}
	drainOnce sync.Once
}

// New validates cfg and returns a Server that is not yet listening.
func New(cfg Config) (*Server, error) {
	if cfg.Service == nil {
		return nil, errors.New("frontend: a service is required")
	}
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.Logger == nil {
		cfg.Logger = telemetry.NopLogger()
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxPollDuration <= 0 {
		cfg.MaxPollDuration = DefaultMaxPollDuration
	}
	if cfg.ReadyTimeout <= 0 {
		cfg.ReadyTimeout = DefaultReadyTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.GzipThreshold <= 0 {
		cfg.GzipThreshold = DefaultGzipThreshold
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = DefaultReadHeaderTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.MaxPollDuration >= cfg.IdleTimeout {
		// Not a style preference: a poll longer than the keep-alive idle
		// timeout means the connection is torn down under an in-flight request.
		return nil, fmt.Errorf("frontend: max poll duration %s must be below the idle timeout %s",
			cfg.MaxPollDuration, cfg.IdleTimeout)
	}
	if cfg.Telemetry == nil {
		t, err := telemetry.New(telemetry.Config{ServiceName: "skald", Logger: cfg.Logger})
		if err != nil {
			return nil, fmt.Errorf("frontend: telemetry: %w", err)
		}
		cfg.Telemetry = t
	}

	s := &Server{
		svc:             cfg.Service,
		tel:             cfg.Telemetry,
		log:             cfg.Logger,
		addr:            cfg.Addr,
		authToken:       cfg.AuthToken,
		readyCheck:      cfg.ReadyCheck,
		maxRequestBytes: cfg.MaxRequestBytes,
		requestTimeout:  cfg.RequestTimeout,
		maxPoll:         cfg.MaxPollDuration,
		readyTimeout:    cfg.ReadyTimeout,
		shutdownTimeout: cfg.ShutdownTimeout,
		gzip:            newGzipCompressor(cfg.GzipThreshold),
		serveErr:        make(chan error, 1),
		drain:           make(chan struct{}),
	}
	s.http = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		// WriteTimeout and ReadTimeout are deliberately unset. Both are
		// absolute deadlines on the whole exchange, and a 50 second long poll
		// would trip either one; per-request cancellation via context is the
		// mechanism that can distinguish a poll from a stalled client.
		ErrorLog: slog.NewLogLogger(cfg.Logger.Handler(), slog.LevelWarn),
	}
	return s, nil
}

// ShutdownTimeout reports the drain deadline the server was configured with, so
// that a caller's signal handler can use the same number without duplicating it.
func (s *Server) ShutdownTimeout() time.Duration { return s.shutdownTimeout }

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

// route is one endpoint's wiring.
type route struct {
	pattern string
	methods []string
	// op is the telemetry operation name: a compile-time constant, never
	// derived from the request. See the comment on telemetry's Op constants.
	op string
	// longPoll routes are exempt from the general request timeout and get the
	// poll cap instead.
	longPoll bool
	// public routes skip authentication.
	public bool
	// timeout overrides the general request timeout when non-zero.
	timeout time.Duration
	handler http.HandlerFunc
}

func (s *Server) routes() []route {
	post := []string{http.MethodPost}
	return []route{
		{pattern: api.PathStartWorkflow, methods: post, op: telemetry.OpStartWorkflow, handler: s.handleStartWorkflow},
		{pattern: api.PathSignalWorkflow, methods: post, op: telemetry.OpSignalWorkflow, handler: s.handleSignalWorkflow},
		{pattern: api.PathSignalWithStart, methods: post, op: telemetry.OpSignalWithStartWorkflow, handler: s.handleSignalWithStart},
		{pattern: api.PathCancelWorkflow, methods: post, op: telemetry.OpCancelWorkflow, handler: s.handleCancelWorkflow},
		{pattern: api.PathTerminateWorkflow, methods: post, op: telemetry.OpTerminateWorkflow, handler: s.handleTerminateWorkflow},
		{
			pattern: api.PathDescribeWorkflow,
			// Describe is a pure read with three scalar parameters, so GET is
			// offered alongside POST: it makes the endpoint usable from a
			// browser, a curl one-liner and a health dashboard without a body.
			methods: []string{http.MethodGet, http.MethodPost},
			op:      telemetry.OpDescribeWorkflow,
			handler: s.handleDescribeWorkflow,
		},
		{pattern: api.PathGetHistory, methods: post, op: telemetry.OpGetHistory, longPoll: true, handler: s.handleGetHistory},
		{pattern: api.PathListWorkflows, methods: post, op: telemetry.OpListWorkflows, handler: s.handleListWorkflows},

		{pattern: api.PathPollWorkflowTask, methods: post, op: telemetry.OpPollWorkflowTask, longPoll: true, handler: s.handlePollWorkflowTask},
		{pattern: api.PathCompleteWorkflow, methods: post, op: telemetry.OpRespondWorkflowTaskDone, handler: s.handleRespondWorkflowTaskCompleted},
		{pattern: api.PathFailWorkflowTask, methods: post, op: telemetry.OpRespondWorkflowTaskFailed, handler: s.handleRespondWorkflowTaskFailed},
		{pattern: api.PathPollActivityTask, methods: post, op: telemetry.OpPollActivityTask, longPoll: true, handler: s.handlePollActivityTask},
		{pattern: api.PathCompleteActivity, methods: post, op: telemetry.OpRespondActivityTaskDone, handler: s.handleRespondActivityTaskCompleted},
		{pattern: api.PathFailActivity, methods: post, op: telemetry.OpRespondActivityTaskFailed, handler: s.handleRespondActivityTaskFailed},
		{pattern: api.PathCancelActivity, methods: post, op: telemetry.OpRespondActivityTaskCancel, handler: s.handleRespondActivityTaskCanceled},
		{pattern: api.PathHeartbeatActivity, methods: post, op: telemetry.OpRecordActivityHeartbeat, handler: s.handleRecordActivityHeartbeat},

		{pattern: api.PathHealth, methods: []string{http.MethodGet}, op: telemetry.OpHealth, public: true, handler: s.handleHealth},
		{pattern: api.PathReady, methods: []string{http.MethodGet}, op: telemetry.OpReady, public: true, timeout: s.readyTimeout, handler: s.handleReady},
		{pattern: api.PathMetrics, methods: []string{http.MethodGet}, op: telemetry.OpMetrics, handler: s.handleMetrics},
	}
}

// Handler builds the complete request pipeline.
//
// The chain is written outermost-first, in the order a request traverses it:
//
//	request ID -> protocol version -> mux
//	  -> telemetry -> access log -> auth -> method -> gzip -> timeout
//	  -> recover -> handler
//
// Two placements are load bearing.
//
// The request ID is outermost so that every log line below it -- including the
// panic report -- carries the same correlation value the client was given back.
//
// Panic recovery is *innermost*, immediately around the handler, which is the
// opposite of the usual advice and is deliberate. The compression layer buffers
// the response and emits it from a deferred call, so a panic unwinding through
// it would flush a 200 with an empty body before an outer recovery could write
// anything; the client would see success. Recovering below gzip means the 500
// goes through the same response path as every other error, and the access log
// and the metrics see the real status. A panic in the middleware itself is not
// caught here -- it falls through to net/http, which logs it to the server's
// ErrorLog and drops the connection, which is the correct outcome for a bug in
// the layer that was supposed to handle bugs.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.Handle(rt.pattern, s.wrap(rt))
	}
	// Anything else answers in the protocol's own envelope rather than
	// net/http's plain-text 404, so a client only ever has to parse one shape.
	mux.Handle("/", s.wrap(route{
		op:      telemetry.OpUnknown,
		public:  true,
		handler: s.handleNotFound,
	}))

	return chain(mux, requestID, protocolVersion)
}

func (s *Server) wrap(rt route) http.Handler {
	timeout := rt.timeout
	if timeout == 0 && !rt.longPoll {
		timeout = s.requestTimeout
	}

	mws := []middleware{
		s.tel.HTTPMiddleware(rt.op),
		accessLog,
	}
	if !rt.public {
		mws = append(mws, bearerAuth(s.authToken))
	}
	if len(rt.methods) > 0 {
		mws = append(mws, methodGuard(rt.methods))
	}
	mws = append(mws, s.gzip.middleware, withTimeout(timeout), recoverPanics(s.log))

	return chain(rt.handler, mws...)
}

// methodGuard rejects the wrong verb with the protocol's envelope and the Allow
// header RFC 9110 requires on a 405.
func methodGuard(allowed []string) middleware {
	allow := ""
	for i, m := range allowed {
		if i > 0 {
			allow += ", "
		}
		allow += m
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, m := range allowed {
				if r.Method == m {
					next.ServeHTTP(w, r)
					return
				}
			}
			w.Header().Set("Allow", allow)
			writeErrorStatus(w, r, http.StatusMethodNotAllowed, &api.Error{
				Code:    api.CodeInvalidArgument,
				Message: fmt.Sprintf("method %s not allowed", r.Method),
				Details: map[string]string{"allow": allow},
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Start binds the listen address and begins serving in the background.
//
// Binding happens synchronously so that "address already in use" is returned to
// the caller rather than logged from a goroutine after the process has told its
// supervisor it started successfully.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("frontend: listen on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	go func() {
		err := s.http.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.serveErr <- err
	}()
	return nil
}

// Addr reports the address actually bound, which differs from the configured
// one when the port was zero. Tests rely on this; so does anyone reading logs
// after starting a second instance.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Wait blocks until the server stops serving, returning nil for a graceful stop.
func (s *Server) Wait() error { return <-s.serveErr }

// Shutdown drains the server.
//
// The order is the whole point:
//
//  1. Close the drain channel. /ready starts failing immediately, so a load
//     balancer stops sending new work while this process can still serve the
//     requests it already accepted. This is the step that makes a rolling
//     restart invisible to clients.
//  2. Release parked long polls. A worker blocked in a 50 second poll is not
//     "in flight" in any meaningful sense -- it is waiting for work that will
//     never come from this process -- and http.Server.Shutdown would sit behind
//     it for the full poll. Cancelling them turns a 50 second drain into a
//     millisecond one, and each worker simply re-polls somewhere else.
//  3. Wait for genuinely in-flight requests, bounded by ctx.
//  4. On expiry, close the remaining connections rather than hanging forever.
//     A request that has not finished in the drain window is not going to.
func (s *Server) Shutdown(ctx context.Context) error {
	s.drainOnce.Do(func() { close(s.drain) })

	err := s.http.Shutdown(ctx)
	if err != nil {
		// Shutdown only fails by deadline. Forcing the close is the honest
		// response: the alternative is a process that ignores its own SIGTERM.
		s.log.Warn("graceful shutdown deadline exceeded, closing connections",
			telemetry.KeyError, err)
		if closeErr := s.http.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return err
}

// draining reports whether shutdown has begun.
func (s *Server) draining() bool {
	select {
	case <-s.drain:
		return true
	default:
		return false
	}
}

// pollContext bounds a long poll by three independent limits.
//
// The client's own context ends the poll when the caller hangs up, which is the
// case that matters most: without it, a worker that is killed mid-poll leaves a
// goroutine parked in the matcher holding a task reference for the full poll
// duration. The cap keeps a poll inside every proxy idle timeout between here
// and the client, and the drain signal makes shutdown fast.
func (s *Server) pollContext(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), s.maxPoll)
	go func() {
		select {
		case <-s.drain:
			cancel()
		case <-ctx.Done():
			// The request finished on its own; this goroutine's only job was to
			// watch for the drain, so it exits with it.
		}
	}()
	return ctx, cancel
}
