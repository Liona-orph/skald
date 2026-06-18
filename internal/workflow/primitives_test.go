package workflow

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/skald-io/skald/pkg/skald"
)

// ---------------------------------------------------------------------------
// Futures
// ---------------------------------------------------------------------------

func TestFutureBlocksUntilSet(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	f, set := NewFuture[int](ctx, "result")
	var got int
	var err error
	d.NewCoroutine(ctx, "reader", func(ctx Context) { got, err = f.Get(ctx) })

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.False(t, f.IsReady())
	require.Equal(t, 0, got)

	set.SetValue(42)
	require.True(t, f.IsReady())
	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.Equal(t, 42, got)
	require.NoError(t, err)
}

func TestFutureCarriesErrors(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	f, set := NewFuture[int](ctx, "result")
	var err error
	d.NewCoroutine(ctx, "reader", func(ctx Context) { _, err = f.Get(ctx) })
	set.SetError(&skald.CanceledError{})

	require.NoError(t, d.RunUntilDone(noDeadline))
	var canceled *skald.CanceledError
	require.ErrorAs(t, err, &canceled)
}

// TestFutureSetIsIdempotent covers the benign race the SDK actually hits: a
// cancelled activity whose result arrives from the server anyway.
func TestFutureSetIsIdempotent(t *testing.T) {
	t.Parallel()
	_, ctx := newTestDispatcher(t)

	f, set := NewFuture[string](ctx, "result")
	set.SetValue("first")
	set.SetValue("second")
	set.SetError(errors.New("late failure"))

	v, err := getReady(f)
	require.NoError(t, err)
	require.Equal(t, "first", v)
}

func TestFutureChainAndThenApply(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	src, srcSet := NewFuture[int](ctx, "source")
	mirror, mirrorSet := NewFuture[int](ctx, "mirror")
	mirrorSet.Chain(src)

	doubled := ThenApply(ctx, src, func(v int, err error) (string, error) {
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("v=%d", v*2), nil
	})

	require.False(t, mirror.IsReady())
	require.False(t, doubled.IsReady())

	srcSet.SetValue(21)
	require.True(t, mirror.IsReady())
	require.True(t, doubled.IsReady())

	var mirrored int
	var text string
	d.NewCoroutine(ctx, "reader", func(ctx Context) {
		mirrored, _ = mirror.Get(ctx)
		text, _ = doubled.Get(ctx)
	})
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, 21, mirrored)
	require.Equal(t, "v=42", text)
}

func TestMapFutureDecodesOnRead(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	raw, set := NewFuture[*skald.Payload](ctx, "payload")
	typed := MapFuture(raw, func(p *skald.Payload, err error) (int, error) {
		if err != nil {
			return 0, err
		}
		var v int
		return v, skald.JSONConverter{}.FromPayload(p, &v)
	})
	set.SetValue(skald.MustPayload(7))

	var got int
	d.NewCoroutine(ctx, "reader", func(ctx Context) { got, _ = typed.Get(ctx) })
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, 7, got)
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

func TestUnbufferedChannelRendezvous(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	ch := NewChannel[int](d, "hand-off", 0)
	var sent, received []int

	d.NewCoroutine(ctx, "sender", func(ctx Context) {
		for i := 0; i < 3; i++ {
			ch.Send(ctx, i)
			sent = append(sent, i)
		}
		ch.Close()
	})
	d.NewCoroutine(ctx, "receiver", func(ctx Context) {
		for {
			v, ok := ch.Receive(ctx)
			if !ok {
				return
			}
			received = append(received, v)
		}
	})

	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, []int{0, 1, 2}, sent)
	require.Equal(t, []int{0, 1, 2}, received)
}

func TestBufferedChannelDoesNotBlockUntilFull(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	ch := NewChannel[int](d, "buffered", 2)
	blocked := false

	d.NewCoroutine(ctx, "sender", func(ctx Context) {
		ch.Send(ctx, 1)
		ch.Send(ctx, 2)
		blocked = true
		ch.Send(ctx, 3) // full: must park
		blocked = false
	})

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.True(t, blocked, "the third send should still be parked")
	require.Equal(t, 3, ch.Len(), "two buffered plus one parked sender")

	v, ok := ch.ReceiveAsync()
	require.True(t, ok)
	require.Equal(t, 1, v)

	require.NoError(t, d.RunUntilDone(noDeadline))
	require.False(t, blocked)
}

func TestClosedChannelDrainsThenReportsClosed(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	ch := NewChannel[string](d, "buffered", 3)
	require.True(t, ch.SendAsync("a"))
	require.True(t, ch.SendAsync("b"))
	ch.Close()

	var got []string
	var okAfterDrain bool
	d.NewCoroutine(ctx, "reader", func(ctx Context) {
		for {
			v, ok := ch.Receive(ctx)
			if !ok {
				okAfterDrain = true
				return
			}
			got = append(got, v)
		}
	})
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, []string{"a", "b"}, got)
	require.True(t, okAfterDrain)
	require.True(t, ch.Closed())
}

func TestChannelDoubleCloseAndSendOnClosedPanic(t *testing.T) {
	t.Parallel()
	d, _ := newTestDispatcher(t)

	ch := NewChannel[int](d, "ch", 1)
	ch.Close()
	require.Panics(t, func() { ch.Close() })
	require.Panics(t, func() { ch.SendAsync(1) })
}

func TestUnboundedChannelNeverBlocksTheSender(t *testing.T) {
	t.Parallel()
	d, _ := newTestDispatcher(t)

	ch := newChannel[int](d, "signals", Unbounded)
	for i := 0; i < 1000; i++ {
		require.True(t, ch.SendAsync(i))
	}
	require.Equal(t, 1000, ch.Len())
}

func TestChannelOperationsOutsideACoroutinePanicHelpfully(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)
	ch := NewChannel[int](d, "ch", 0)

	require.PanicsWithValue(t,
		"workflow: Channel.Receive was called outside a workflow coroutine; "+
			"workflow code must not start goroutines with `go` -- use workflow.Go so that the "+
			"dispatcher can schedule and unwind them deterministically",
		func() { ch.Receive(ctx) })
}

// ---------------------------------------------------------------------------
// Selector
// ---------------------------------------------------------------------------

// TestSelectorPicksFirstReadyInRegistrationOrder is the determinism guarantee
// that Go's select cannot give: with two branches ready at once the earlier
// registration always wins, on every replay, forever.
func TestSelectorPicksFirstReadyInRegistrationOrder(t *testing.T) {
	t.Parallel()

	// Repeated because "picks at random" is precisely the failure mode a single
	// run cannot distinguish from "picks the first one".
	for i := 0; i < 200; i++ {
		d := NewDispatcher(nil)
		ctx := Background(d, nil)

		a, aSet := NewFuture[int](ctx, "a")
		b, bSet := NewFuture[int](ctx, "b")
		aSet.SetValue(1)
		bSet.SetValue(2)

		var chosen string
		d.NewCoroutine(ctx, "select", func(ctx Context) {
			NewSelector(ctx, "race").
				AddFuture(a, func() { chosen = "a" }).
				AddFuture(b, func() { chosen = "b" }).
				Select(ctx)
		})
		require.NoError(t, d.RunUntilDone(noDeadline))
		require.NoError(t, d.Close())
		require.Equal(t, "a", chosen, "the selector must never pick at random")
	}
}

func TestSelectorBlocksUntilABranchIsReady(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	slow, slowSet := NewFuture[int](ctx, "slow")
	ch := NewChannel[string](d, "signal", 1)
	var chosen string

	d.NewCoroutine(ctx, "select", func(ctx Context) {
		NewSelector(ctx, "race").
			AddFuture(slow, func() { chosen = "future" }).
			AddReceive(ch, func() {
				v, _ := ch.Receive(ctx)
				chosen = "channel:" + v
			}).
			Select(ctx)
	})

	require.NoError(t, d.ExecuteUntilAllBlocked(noDeadline))
	require.Equal(t, "", chosen)

	require.True(t, ch.SendAsync("hello"))
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, "channel:hello", chosen)
	require.False(t, slow.IsReady())
	slowSet.SetValue(0)
}

func TestSelectorDefaultBranchNeverBlocks(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	f, set := NewFuture[int](ctx, "pending")
	var chosen string

	d.NewCoroutine(ctx, "select", func(ctx Context) {
		NewSelector(ctx, "poll").
			AddFuture(f, func() { chosen = "future" }).
			AddDefault(func() { chosen = "default" }).
			Select(ctx)
	})
	require.NoError(t, d.RunUntilDone(noDeadline))
	require.Equal(t, "default", chosen)
	set.SetValue(0)
}

func TestSelectorHasPending(t *testing.T) {
	t.Parallel()
	d, ctx := newTestDispatcher(t)

	f, set := NewFuture[int](ctx, "pending")
	s := NewSelector(ctx, "poll").AddFuture(f, func() {})
	require.False(t, s.HasPending())
	set.SetValue(1)
	require.True(t, s.HasPending())
	_ = d
}
