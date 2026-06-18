package workflow

import "fmt"

// Unbounded is the capacity of a channel that never blocks a sender.
//
// Signal channels use it. A signal is already durable in the history by the time
// the SDK sees it, so refusing it because a buffer is full would *lose* it; the
// real backpressure is the history size limit, which the server enforces.
const Unbounded = -1

// ReceiveChannel is the receiving half of a workflow channel.
//
// It looks like a Go channel and behaves like one, with one critical difference:
// blocking here yields to the dispatcher instead of parking on the runtime
// scheduler, so the dispatcher still knows the workflow is stuck and can tell an
// operator exactly where.
type ReceiveChannel[T any] interface {
	// Receive blocks until a value is available or the channel is closed and
	// drained. ok is false only in the latter case.
	Receive(ctx Context) (value T, ok bool)
	// ReceiveAsync takes a value if one is immediately available.
	ReceiveAsync() (value T, ok bool)
	// IsReady reports whether a Receive would return without blocking.
	IsReady() bool
	// Len is the number of values buffered plus the number of blocked senders.
	Len() int
}

// SendChannel is the sending half of a workflow channel.
type SendChannel[T any] interface {
	// Send blocks until the value is buffered or handed to a receiver.
	Send(ctx Context, value T)
	// SendAsync reports whether the value could be delivered without blocking.
	SendAsync(value T) bool
	// Close makes every subsequent Receive drain the buffer and then report
	// ok == false. Closing twice panics, exactly like a Go channel, because a
	// double close is always a bug in the workflow's own logic.
	Close()
	// Closed reports whether Close has been called.
	Closed() bool
}

// Channel is a bidirectional workflow channel.
type Channel[T any] interface {
	ReceiveChannel[T]
	SendChannel[T]
}

// pendingSend is one blocked sender's parked value.
type pendingSend[T any] struct {
	value T
	taken bool
}

// channelImpl is the single implementation behind all three interfaces.
//
// Unbuffered channels are modelled as capacity 0 with a queue of parked senders:
// a receive first drains the buffer, then takes directly from the oldest parked
// sender. That reproduces Go's rendezvous semantics without ever touching a real
// channel, which matters because a real channel would park the coroutine's
// goroutine somewhere the dispatcher cannot see.
type channelImpl[T any] struct {
	dispatcher *Dispatcher
	name       string
	capacity   int

	buffer  []T
	senders []*pendingSend[T]
	closed  bool
}

var _ Channel[int] = (*channelImpl[int])(nil)

// NewChannel returns a workflow channel with the given buffer capacity. A
// capacity of 0 is unbuffered; Unbounded never blocks a sender.
func NewChannel[T any](d *Dispatcher, name string, capacity int) Channel[T] {
	return newChannel[T](d, name, capacity)
}

func newChannel[T any](d *Dispatcher, name string, capacity int) *channelImpl[T] {
	if name == "" {
		name = "channel"
	}
	return &channelImpl[T]{dispatcher: d, name: name, capacity: capacity}
}

func (c *channelImpl[T]) markProgress() {
	if c.dispatcher != nil {
		c.dispatcher.markProgress()
	}
}

// IsReady implements ReceiveChannel and doubles as the Selector's readiness
// predicate.
func (c *channelImpl[T]) IsReady() bool {
	return len(c.buffer) > 0 || len(c.senders) > 0 || c.closed
}

func (c *channelImpl[T]) Len() int { return len(c.buffer) + len(c.senders) }

func (c *channelImpl[T]) Closed() bool { return c.closed }

func (c *channelImpl[T]) Close() {
	if c.closed {
		panic(fmt.Sprintf("workflow: %s closed twice", c.name))
	}
	c.closed = true
	// A close can unblock every parked receiver, so the dispatcher must run at
	// least one more pass.
	c.markProgress()
}

func (c *channelImpl[T]) ReceiveAsync() (T, bool) {
	var zero T
	if len(c.buffer) > 0 {
		v := c.buffer[0]
		// Clear the vacated slot so a large payload is not pinned by the
		// backing array after the value has been consumed.
		c.buffer[0] = zero
		c.buffer = c.buffer[1:]
		c.promoteSender()
		c.markProgress()
		return v, true
	}
	if len(c.senders) > 0 {
		s := c.senders[0]
		c.senders = c.senders[1:]
		s.taken = true
		c.markProgress()
		return s.value, true
	}
	return zero, false
}

// promoteSender moves one parked sender into the buffer after a receive has
// made room. Without it a buffered channel would starve its senders: they would
// stay parked while the buffer refilled from later sends.
func (c *channelImpl[T]) promoteSender() {
	if c.capacity <= 0 || len(c.senders) == 0 || len(c.buffer) >= c.capacity {
		return
	}
	s := c.senders[0]
	c.senders = c.senders[1:]
	s.taken = true
	c.buffer = append(c.buffer, s.value)
}

func (c *channelImpl[T]) Receive(ctx Context) (T, bool) {
	co := mustCoroutine(ctx, "Channel.Receive")
	// Entering a blocking call is progress even if the call does not end up
	// blocking: it means user code advanced, which may have unblocked a
	// coroutine that already yielded earlier in this pass.
	c.markProgress()
	for {
		if v, ok := c.ReceiveAsync(); ok {
			return v, true
		}
		if c.closed {
			var zero T
			return zero, false
		}
		co.yield("receive on " + c.name)
	}
}

func (c *channelImpl[T]) SendAsync(value T) bool {
	if c.closed {
		panic(fmt.Sprintf("workflow: send on closed %s", c.name))
	}
	if c.capacity == Unbounded || len(c.buffer) < c.capacity {
		c.buffer = append(c.buffer, value)
		c.markProgress()
		return true
	}
	return false
}

func (c *channelImpl[T]) Send(ctx Context, value T) {
	co := mustCoroutine(ctx, "Channel.Send")
	c.markProgress()
	if c.SendAsync(value) {
		return
	}
	// Park the value where a receiver can find it, then wait for it to be
	// taken. The token, not the channel, carries the "was it delivered" fact,
	// so two senders parked on the same channel cannot confuse each other.
	s := &pendingSend[T]{value: value}
	c.senders = append(c.senders, s)
	for !s.taken {
		if c.closed {
			panic(fmt.Sprintf("workflow: %s closed while a send was blocked on it", c.name))
		}
		co.yield("send on " + c.name)
	}
}

// deliver appends to the buffer regardless of capacity.
//
// It exists for values that arrive from outside the workflow -- signals -- where
// the value is already durable and dropping it is not an option. It is not part
// of the public interface precisely because workflow code should never be able
// to bypass a capacity it chose.
func (c *channelImpl[T]) deliver(value T) {
	if c.closed {
		return
	}
	c.buffer = append(c.buffer, value)
	c.markProgress()
}
