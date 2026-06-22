package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/skald-io/skald/pkg/api"
	"github.com/skald-io/skald/pkg/skald"
)

// ActivityInfo describes the attempt an activity is currently serving.
//
// Everything here comes from the server, which is what makes it trustworthy:
// Attempt in particular is the server's count, so an activity that behaves
// differently on a retry is reacting to a fact rather than to a guess.
type ActivityInfo struct {
	Namespace        string
	Execution        skald.WorkflowExecution
	WorkflowType     string
	ActivityID       string
	ActivityType     string
	TaskQueue        string
	ScheduledEventID int64
	StartedEventID   int64
	Attempt          int32
	ScheduledAt      time.Time
	StartedAt        time.Time
	// Deadline is when the server will consider this attempt timed out. The
	// worker already derives the activity's context deadline from it; it is
	// exposed so that long-running code can budget its own work.
	Deadline         time.Time
	HeartbeatTimeout time.Duration
}

// activityEnvKey is the context key for the activity environment. A struct{}
// key cannot collide with anything else in the context chain.
type activityEnvKey struct{}

type activityEnv struct {
	info                 ActivityInfo
	conv                 skald.DataConverter
	heartbeat            *heartbeater
	lastHeartbeatDetails *skald.Payload
}

func withActivityEnv(ctx context.Context, env *activityEnv) context.Context {
	return context.WithValue(ctx, activityEnvKey{}, env)
}

func activityEnvFrom(ctx context.Context) (*activityEnv, bool) {
	env, ok := ctx.Value(activityEnvKey{}).(*activityEnv)
	return env, ok
}

// IsActivityContext reports whether ctx belongs to a running activity.
func IsActivityContext(ctx context.Context) bool {
	_, ok := activityEnvFrom(ctx)
	return ok
}

// GetActivityInfo returns the activity's description. It panics if ctx is not an
// activity context, because reaching that point means the caller is running
// somewhere other than where it thinks it is.
func GetActivityInfo(ctx context.Context) ActivityInfo {
	env, ok := activityEnvFrom(ctx)
	if !ok {
		panic("worker: GetActivityInfo was called with a context that is not an activity context")
	}
	return env.info
}

// RecordHeartbeat reports that the activity is alive and checkpoints details.
//
// Two things happen at once, and both matter. The server's heartbeat deadline is
// extended, so a long activity is not killed for being slow; and the response
// carries whether the workflow has asked for the activity to stop, which the
// worker turns into a cancellation of the activity's context. An activity that
// never heartbeats can therefore never be cancelled -- there is no other channel
// through which the news could reach it.
//
// details are checkpointed durably and handed to the next attempt via
// GetHeartbeatDetails, which is how a long import resumes at row 40,000 instead
// of starting over.
//
// Calls are throttled to roughly half the heartbeat timeout, so calling this in
// a tight loop is safe and costs at most one round trip per interval.
func RecordHeartbeat(ctx context.Context, details ...any) {
	env, ok := activityEnvFrom(ctx)
	if !ok {
		panic("worker: RecordHeartbeat was called with a context that is not an activity context")
	}
	if env.heartbeat == nil {
		return
	}
	p, err := encodeHeartbeat(env.conv, details)
	if err != nil {
		env.heartbeat.log.Warn("heartbeat details could not be encoded",
			"activity_type", env.info.ActivityType, "error", err)
		return
	}
	env.heartbeat.record(p)
}

// HasHeartbeatDetails reports whether a previous attempt left a checkpoint.
func HasHeartbeatDetails(ctx context.Context) bool {
	env, ok := activityEnvFrom(ctx)
	return ok && !env.lastHeartbeatDetails.IsNil()
}

// GetHeartbeatDetails decodes the checkpoint left by a previous attempt.
func GetHeartbeatDetails(ctx context.Context, out any) error {
	env, ok := activityEnvFrom(ctx)
	if !ok {
		return errors.New("worker: GetHeartbeatDetails was called outside an activity")
	}
	if env.lastHeartbeatDetails.IsNil() {
		return errors.New("worker: this attempt has no heartbeat details")
	}
	return env.conv.FromPayload(env.lastHeartbeatDetails, out)
}

func encodeHeartbeat(conv skald.DataConverter, details []any) (*skald.Payload, error) {
	switch len(details) {
	case 0:
		return nil, nil
	case 1:
		return conv.ToPayload(details[0])
	default:
		return conv.ToPayload(details)
	}
}

// ---------------------------------------------------------------------------
// Heartbeating
// ---------------------------------------------------------------------------

// heartbeater throttles heartbeat traffic and propagates cancellation.
//
// The throttle is client side because the engine deliberately does not drop
// heartbeats: a heartbeat the server silently ignored looks exactly like a dead
// worker. Rate limiting therefore has to happen where the intent is known, which
// is here.
type heartbeater struct {
	service          api.Service
	rpcCtx           context.Context
	namespace        string
	identity         string
	execution        skald.WorkflowExecution
	scheduledEventID int64
	interval         time.Duration
	cancel           context.CancelFunc
	log              *slog.Logger

	mu         sync.Mutex
	pending    *skald.Payload
	hasPending bool
	lastSent   time.Time
	closed     bool

	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

func newHeartbeater(rpcCtx context.Context, w *Worker, task api.ActivityTask, cancel context.CancelFunc) *heartbeater {
	interval := w.opts.HeartbeatThrottle
	if task.HeartbeatTimeout > 0 {
		// Half the timeout gives one full round trip of slack before the server
		// gives up, which is the smallest margin that tolerates a single lost
		// heartbeat.
		interval = task.HeartbeatTimeout / 2
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	h := &heartbeater{
		service:          w.service,
		rpcCtx:           rpcCtx,
		namespace:        w.opts.Namespace,
		identity:         w.opts.Identity,
		execution:        task.Execution,
		scheduledEventID: task.ScheduledEventID,
		interval:         interval,
		cancel:           cancel,
		log:              w.log,
		stopCh:           make(chan struct{}),
		done:             make(chan struct{}),
	}
	go h.loop()
	return h
}

// record stores details and sends immediately if the throttle allows.
func (h *heartbeater) record(p *skald.Payload) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.pending = p
	h.hasPending = true
	due := h.lastSent.IsZero() || time.Since(h.lastSent) >= h.interval
	h.mu.Unlock()
	if due {
		h.flush()
	}
}

func (h *heartbeater) flush() {
	h.mu.Lock()
	if h.closed || !h.hasPending {
		h.mu.Unlock()
		return
	}
	p := h.pending
	h.hasPending = false
	h.lastSent = time.Now()
	h.mu.Unlock()

	resp, err := h.service.RecordActivityHeartbeat(h.rpcCtx, api.RecordActivityHeartbeatRequest{
		Namespace:        h.namespace,
		Execution:        h.execution,
		ScheduledEventID: h.scheduledEventID,
		Details:          p,
		Identity:         h.identity,
	})
	if err != nil {
		// A failed heartbeat is not fatal: the next one may succeed, and the
		// server's timeout is the real safety net. It is logged because a
		// persistent failure means the activity is about to be timed out for
		// reasons that have nothing to do with the activity.
		h.log.Warn("heartbeat failed", "execution", h.execution.String(),
			"scheduled_event_id", h.scheduledEventID, "error", err)
		return
	}
	if resp.CancelRequested {
		h.log.Debug("activity cancellation requested", "execution", h.execution.String(),
			"scheduled_event_id", h.scheduledEventID)
		h.cancel()
	}
}

// loop flushes buffered details on a timer so that a checkpoint recorded just
// under the throttle is not lost when the activity goes quiet.
func (h *heartbeater) loop() {
	defer close(h.done)
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-h.rpcCtx.Done():
			return
		case <-t.C:
			h.flush()
		}
	}
}

func (h *heartbeater) stop() {
	h.stopOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()
		close(h.stopCh)
		<-h.done
	})
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

// executeActivity runs one attempt, converting panics into failures.
//
// A panic in an activity becomes a *retryable* ApplicationError, unlike a panic
// in workflow code. The asymmetry is deliberate: workflow code is a pure
// decision function, so a panic there is a deterministic bug that retrying can
// only repeat, while an activity is talking to the world and a panic may well be
// a nil dereference on a response from a dependency that was briefly broken.
func executeActivity(
	ctx context.Context,
	fn func(context.Context, *skald.Payload) (*skald.Payload, error),
	input *skald.Payload,
) (result *skald.Payload, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &skald.ApplicationError{
				Type:       "PanicError",
				Message:    fmt.Sprintf("activity panicked: %v", r),
				StackTrace: string(debug.Stack()),
			}
			result = nil
		}
	}()
	return fn(ctx, input)
}
