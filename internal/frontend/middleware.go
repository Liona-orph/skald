package frontend

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Liona-orph/skald/internal/telemetry"
	"github.com/Liona-orph/skald/pkg/api"
)

// Headers Skald reads and writes.
const (
	// HeaderRequestID correlates a client's log line with a server's. It is
	// echoed back so that a caller who did not send one can still record the
	// value the server used.
	HeaderRequestID = "X-Request-Id"
	// HeaderProtocolVersion carries api.Version.
	HeaderProtocolVersion = "Skald-Protocol-Version"
)

// maxRequestIDLen bounds a client-supplied correlation ID.
//
// The value goes straight into structured logs, so it is attacker-influenced
// data in a log pipeline: unbounded length is a cheap way to blow up log
// storage, and control characters are how a forged log line gets injected into
// a text-format stream. Both are handled at the boundary rather than trusted.
const maxRequestIDLen = 128

// middleware is the shape every layer in the chain has.
type middleware func(http.Handler) http.Handler

// logFor returns the request-scoped logger when one exists and the process
// logger otherwise.
//
// The outermost middleware runs before any request-scoped logger has been
// installed, and it is the layer that reports panics -- exactly the report that
// must not be swallowed by a discarding default.
func logFor(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if log, ok := telemetry.LoggerFromContext(ctx); ok {
		return log
	}
	if fallback != nil {
		return fallback
	}
	return telemetry.NopLogger()
}

// chain applies middlewares so that the first listed is the outermost.
//
// Outermost-first reads in the order requests actually traverse the stack,
// which is the order anyone debugging a request thinks in.
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// recoverPanics turns a panic into a 500 and a log line.
//
// Two rules, both non-negotiable. The client gets a generic internal error and
// never the stack: a stack trace names internal packages, file paths and
// sometimes argument values, which is reconnaissance handed to whoever
// triggered the panic. The server gets the whole stack, because a panic is a
// bug and the stack is the only evidence that it happened at all.
//
// http.ErrAbortHandler is re-panicked: net/http uses it as a control signal to
// abandon a connection without logging, and swallowing it would turn an
// intentional abort into a bogus 500.
func recoverPanics(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				//nolint:errorlint // rec is the value recover() returned, not an
				// error being inspected: net/http panics with this exact value and
				// errors.Is on an any operand does not compile.
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				logFor(r.Context(), log).Error("panic serving request",
					"panic", rec,
					"method", r.Method,
					"path", r.URL.Path,
					"stack", string(debug.Stack()),
				)
				// The handler may have written a status already, in which case
				// this write is a no-op at the transport layer and the client
				// sees a truncated body. Nothing better is available: the bytes
				// are gone. Logging above is what makes it diagnosable.
				writeError(w, r, &api.Error{
					Code:    api.CodeInternal,
					Message: "internal error",
				})
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// requestID ensures every request has a correlation identifier.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := sanitizeRequestID(r.Header.Get(HeaderRequestID))
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(telemetry.ContextWithRequestID(r.Context(), id)))
	})
}

// sanitizeRequestID drops anything that must not reach a log line.
func sanitizeRequestID(id string) string {
	if len(id) > maxRequestIDLen {
		id = id[:maxRequestIDLen]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, id)
}

// protocolVersion rejects a client speaking a protocol this build does not
// implement, and stamps every response with the version it does.
//
// Absent means "unversioned client", which is accepted: curl is a legitimate
// client of a JSON API and should not need a header to get an answer. A version
// we do not implement is refused loudly, because the alternative is a client
// silently getting fields it cannot parse.
func protocolVersion(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderProtocolVersion, api.Version)
		if v := r.Header.Get(HeaderProtocolVersion); v != "" && v != api.Version {
			writeError(w, r, &api.Error{
				Code:    api.CodeInvalidArgument,
				Message: "unsupported protocol version",
				Details: map[string]string{"requested": v, "supported": api.Version},
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// accessLog emits one structured line per request.
//
// The level is chosen from the status: a 5xx is the server's fault and is an
// error, a 4xx is the client's and is a warning worth seeing but not paging on,
// and everything else is info. Deriving the level from the status rather than
// from the call site means a new endpoint cannot forget to log its failures.
//
// The write is deferred so that a request which panics past the recovery layer
// still produces a line. A request that vanishes from the access log is the
// worst possible failure mode for the one artefact an incident starts from.
func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := telemetry.WrapResponseWriter(w)

		defer func() {
			// The logger is fetched at the end so that it picks up the
			// operation, trace and execution fields the layers below attached.
			log := telemetry.LoggerFrom(r.Context())
			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				telemetry.KeyStatus, ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
				"remote", clientIP(r),
			}
			switch {
			case ww.Status() >= 500:
				log.Error("request", attrs...)
			case ww.Status() >= 400:
				log.Warn("request", attrs...)
			default:
				log.Info("request", attrs...)
			}
		}()

		next.ServeHTTP(ww, r)
	})
}

// clientIP returns the peer address without the port.
//
// X-Forwarded-For is deliberately ignored. It is trivially forged, and treating
// it as truth without knowing the proxy topology puts an attacker-controlled
// string in every log line. A deployment behind a trusted proxy should let that
// proxy log the client address; the server logs who it is actually talking to.
func clientIP(r *http.Request) string {
	if i := strings.LastIndex(r.RemoteAddr, ":"); i > 0 {
		return r.RemoteAddr[:i]
	}
	return r.RemoteAddr
}

// bearerAuth enforces a static bearer token.
//
// The token is a shared secret, not an identity: it answers "may you talk to
// this server" and nothing else. That is the right amount of authentication for
// a component that normally sits inside a trust boundary, and it is stated here
// so that nobody mistakes it for authorisation.
func bearerAuth(token string) middleware {
	const prefix = "Bearer "
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		if token == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if !strings.HasPrefix(header, prefix) {
				writeError(w, r, &api.Error{Code: api.CodeUnauthorized, Message: "bearer token required"})
				return
			}
			// Constant-time comparison so that the response latency does not
			// reveal how many leading bytes of a guess were right. It returns 0
			// for differing lengths, which leaks only the length -- an
			// acceptable trade for a fixed-size deployment secret.
			if subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, prefix)), want) != 1 {
				writeError(w, r, &api.Error{Code: api.CodeUnauthorized, Message: "invalid bearer token"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// withTimeout bounds a non-polling request.
//
// http.TimeoutHandler is not used: it buffers the entire response in memory so
// that it can replace it with its own error page, which would defeat the
// streaming half of the gzip layer and would produce an error body that is not
// the protocol's envelope. Cancelling the context instead lets the handler
// return the failure through the same path as every other error, and the store
// and engine both honour cancellation.
func withTimeout(d time.Duration) middleware {
	return func(next http.Handler) http.Handler {
		if d <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ---------------------------------------------------------------------------
// Response compression
// ---------------------------------------------------------------------------

// gzipCompressor compresses responses that are worth compressing.
//
// The threshold matters. A history read is hundreds of kilobytes of highly
// repetitive JSON and compresses ten to one; a StartWorkflow response is forty
// bytes and would come out *larger* after a gzip header and trailer, having
// cost a compressor allocation to get there. So small responses go out
// untouched and the switch happens the first time the body exceeds the
// threshold, which means the decision needs no Content-Length known in advance.
type gzipCompressor struct {
	threshold int
	pool      *sync.Pool
}

func newGzipCompressor(threshold int) *gzipCompressor {
	return &gzipCompressor{
		threshold: threshold,
		pool: &sync.Pool{New: func() any {
			// BestSpeed, not the default: history JSON is so redundant that the
			// extra ratio from higher levels is a few percent, while the CPU
			// cost lands on the request path of an engine whose whole job is
			// low-latency dispatch.
			w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
			return w
		}},
	}
}

func (c *gzipCompressor) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Vary is set whether or not this response ends up compressed: any cache
		// between here and the client must key on Accept-Encoding, and a
		// response that happened to be small is still one variant of two.
		w.Header().Add("Vary", "Accept-Encoding")
		if !acceptsGzip(r) {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w, compressor: c, status: http.StatusOK}
		defer gw.close()
		next.ServeHTTP(gw, r)
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, enc := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(enc), ";")
		if !strings.EqualFold(strings.TrimSpace(name), "gzip") {
			continue
		}
		return qualityAllows(params)
	}
	return false
}

// qualityAllows reports whether an Accept-Encoding parameter list permits the
// encoding. "gzip;q=0" is a client explicitly refusing gzip, which some
// debugging proxies send and which it would be rude to ignore.
func qualityAllows(params string) bool {
	for _, p := range strings.Split(params, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), "q") {
			continue
		}
		if q, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil && q <= 0 {
			return false
		}
	}
	return true
}

// gzipResponseWriter buffers until it knows whether compression is worthwhile.
type gzipResponseWriter struct {
	http.ResponseWriter
	compressor *gzipCompressor

	status      int
	wroteHeader bool
	buf         bytes.Buffer
	gz          *gzip.Writer
	// passthrough is set when the handler encoded the body itself.
	passthrough bool
	err         error
}

func (g *gzipResponseWriter) WriteHeader(code int) {
	if g.wroteHeader {
		return
	}
	g.wroteHeader = true
	g.status = code
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) {
	switch {
	case g.err != nil:
		return 0, g.err
	case g.passthrough:
		return g.ResponseWriter.Write(p)
	case g.gz != nil:
		return g.gz.Write(p)
	}

	// A handler that set Content-Encoding has already encoded its own body --
	// /metrics does, because promhttp negotiates compression itself. Wrapping
	// it again produces a doubly-gzipped response that every client decodes
	// exactly once and then fails to parse, which is a spectacularly confusing
	// bug to chase from the client side.
	if g.Header().Get("Content-Encoding") != "" {
		g.passthrough = true
		g.ResponseWriter.WriteHeader(g.status)
		if g.buf.Len() > 0 {
			if _, err := g.ResponseWriter.Write(g.buf.Bytes()); err != nil {
				g.err = err
				return 0, err
			}
			g.buf.Reset()
		}
		return g.ResponseWriter.Write(p)
	}

	g.buf.Write(p)
	if g.buf.Len() < g.compressor.threshold {
		return len(p), nil
	}
	if err := g.startCompressing(); err != nil {
		g.err = err
		return 0, err
	}
	return len(p), nil
}

// startCompressing commits to a compressed response and flushes what was
// buffered into the compressor.
func (g *gzipResponseWriter) startCompressing() error {
	h := g.Header()
	h.Set("Content-Encoding", "gzip")
	// Whatever the handler thought the length was, it described the plain body.
	// Leaving it would make the response unparseable.
	h.Del("Content-Length")
	g.ResponseWriter.WriteHeader(g.status)

	gz, _ := g.compressor.pool.Get().(*gzip.Writer)
	gz.Reset(g.ResponseWriter)
	g.gz = gz
	_, err := gz.Write(g.buf.Bytes())
	g.buf.Reset()
	return err
}

// close finishes the response, emitting the buffered body uncompressed when the
// threshold was never reached.
func (g *gzipResponseWriter) close() {
	if g.passthrough {
		// The handler owns the encoding; the headers and body are already out.
		return
	}
	if g.gz != nil {
		_ = g.gz.Close()
		g.gz.Reset(nil) // drop the reference to the connection before pooling
		g.compressor.pool.Put(g.gz)
		g.gz = nil
		return
	}
	if g.err != nil {
		return
	}
	g.Header().Set("Content-Length", strconv.Itoa(g.buf.Len()))
	g.ResponseWriter.WriteHeader(g.status)
	if g.buf.Len() > 0 {
		_, _ = g.ResponseWriter.Write(g.buf.Bytes())
	}
}

// Flush pushes buffered bytes to the client. A flush forces the compression
// decision, because a caller asking for a flush wants bytes on the wire now.
func (g *gzipResponseWriter) Flush() {
	if g.gz != nil {
		_ = g.gz.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (g *gzipResponseWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }
