package worker

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Liona-orph/skald/pkg/api"
)

// backoff is the exponential retry delay used after poll failures.
//
// It exists because the failure mode it guards against is not "one poll failed"
// but "the server is down and forty workers are hammering it". Growing the delay
// turns a thundering herd into a trickle, and resetting it on the first success
// means a transient blip costs one extra millisecond rather than a minute of
// idle workers.
type backoff struct {
	initial time.Duration
	max     time.Duration
	attempt int
}

func newBackoff(initial, max time.Duration) *backoff {
	if initial <= 0 {
		initial = DefaultInitialPollBackoff
	}
	if max < initial {
		max = initial
	}
	return &backoff{initial: initial, max: max}
}

func (b *backoff) next() time.Duration {
	d := float64(b.initial) * math.Pow(2, float64(b.attempt))
	b.attempt++
	if d > float64(b.max) || math.IsInf(d, 0) {
		return b.max
	}
	return time.Duration(d)
}

func (b *backoff) reset() { b.attempt = 0 }

// pollWorkflowTasks is one workflow-task poller.
//
// The slot is acquired *before* the poll, not after it. That ordering matters:
// a poll that succeeds removes the task from the queue and starts it on the
// server, so a worker that polls without a free slot has taken work it cannot
// run and made it look, to everyone else, like a task a worker is sitting on.
func (w *Worker) pollWorkflowTasks(id int) {
	log := w.log.With("poller", "workflow", "poller_id", id)
	b := newBackoff(DefaultInitialPollBackoff, w.opts.MaxPollBackoff)

	for {
		if w.pollCtx.Err() != nil {
			return
		}
		if !acquire(w.pollCtx, w.workflowSlots) {
			return
		}
		task, err := w.service.PollWorkflowTask(w.pollCtx, api.PollWorkflowTaskRequest{
			Namespace: w.opts.Namespace,
			TaskQueue: w.taskQueue,
			Identity:  w.opts.Identity,
		})
		if err != nil {
			release(w.workflowSlots)
			if w.pollCtx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			d := b.next()
			log.Warn("workflow task poll failed", "retry_in", d, "error", err)
			if !sleepCtx(w.pollCtx, d) {
				return
			}
			continue
		}
		b.reset()
		if task.Empty {
			// An expired long poll is the normal state of an idle deployment,
			// not an error, and must not feed the backoff.
			release(w.workflowSlots)
			continue
		}

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			defer release(w.workflowSlots)
			w.handleWorkflowTask(task)
		}()
	}
}

// pollActivityTasks is one activity-task poller.
func (w *Worker) pollActivityTasks(id int) {
	log := w.log.With("poller", "activity", "poller_id", id)
	b := newBackoff(DefaultInitialPollBackoff, w.opts.MaxPollBackoff)

	for {
		if w.pollCtx.Err() != nil {
			return
		}
		if !acquire(w.pollCtx, w.activitySlots) {
			return
		}
		task, err := w.service.PollActivityTask(w.pollCtx, api.PollActivityTaskRequest{
			Namespace: w.opts.Namespace,
			TaskQueue: w.taskQueue,
			Identity:  w.opts.Identity,
		})
		if err != nil {
			release(w.activitySlots)
			if w.pollCtx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			d := b.next()
			log.Warn("activity task poll failed", "retry_in", d, "error", err)
			if !sleepCtx(w.pollCtx, d) {
				return
			}
			continue
		}
		b.reset()
		if task.Empty {
			release(w.activitySlots)
			continue
		}

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			defer release(w.activitySlots)
			w.handleActivityTask(task)
		}()
	}
}

// acquire takes a concurrency slot, reporting false if the worker is shutting
// down first.
func acquire(ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func release(slots chan struct{}) { <-slots }
