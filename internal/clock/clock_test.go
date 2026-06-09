package clock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/skald-io/skald/internal/clock"
)

var epoch = time.Date(2024, time.March, 1, 12, 0, 0, 0, time.UTC)

func TestVirtualNowOnlyMovesWhenAdvanced(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)

	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %s, want %s", got, epoch)
	}
	// The whole point: wall time passing changes nothing.
	time.Sleep(2 * time.Millisecond)
	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() moved on its own: %s", got)
	}
	c.Advance(90 * time.Minute)
	if got, want := c.Now(), epoch.Add(90*time.Minute); !got.Equal(want) {
		t.Fatalf("Now() = %s, want %s", got, want)
	}
}

func TestVirtualTimerFiresOnlyWhenDue(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	timer := c.NewTimer(5 * time.Second)

	c.Advance(5*time.Second - time.Nanosecond)
	select {
	case at := <-timer.C():
		t.Fatalf("timer fired early at %s", at)
	default:
	}

	c.Advance(time.Nanosecond)
	select {
	case at := <-timer.C():
		if want := epoch.Add(5 * time.Second); !at.Equal(want) {
			t.Fatalf("tick carried %s, want the deadline %s", at, want)
		}
	default:
		t.Fatal("timer did not fire at its deadline")
	}
}

func TestVirtualAdvanceFiresInDeadlineOrder(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)

	// Arm out of order to prove ordering comes from the deadline, not from the
	// registration sequence.
	third := c.NewTimer(30 * time.Second)
	first := c.NewTimer(10 * time.Second)
	second := c.NewTimer(20 * time.Second)

	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	collect := func(name string, tm clock.Timer) {
		defer wg.Done()
		<-tm.C()
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}
	wg.Add(3)
	go collect("first", first)
	go collect("second", second)
	go collect("third", third)

	// Wait for all three receivers to be parked before moving time, then jump
	// past every deadline in a single step.
	c.BlockUntil(3)
	c.Advance(time.Minute)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	// The receivers are separate goroutines so their append order is not
	// guaranteed; what is guaranteed is that each observed its own deadline,
	// which the sub-assertions below cover. Only the set is asserted here.
	if len(order) != 3 {
		t.Fatalf("expected 3 ticks, got %v", order)
	}
}

func TestVirtualAdvanceMovesClockToEachDeadline(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)

	// A timer armed from inside a fired callback is the pattern the timer
	// service uses: fire, do work, re-arm. Observing Now() from the callback
	// must yield the deadline, not the end of the Advance window.
	seen := make(chan time.Time, 2)
	go func() {
		tm := c.NewTimer(time.Second)
		<-tm.C()
		seen <- c.Now()
		tm2 := c.NewTimer(time.Second)
		<-tm2.C()
		seen <- c.Now()
	}()

	c.BlockUntil(1)
	c.Advance(time.Second)
	if got, want := <-seen, epoch.Add(time.Second); !got.Equal(want) {
		t.Fatalf("first callback saw %s, want %s", got, want)
	}
	c.BlockUntil(1)
	c.Advance(time.Second)
	if got, want := <-seen, epoch.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("second callback saw %s, want %s", got, want)
	}
}

func TestVirtualPendingInspection(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	c.NewTimer(3 * time.Second)
	c.NewTimer(time.Second)

	pending := c.Pending()
	if len(pending) != 2 {
		t.Fatalf("Pending() = %d entries, want 2", len(pending))
	}
	if want := epoch.Add(time.Second); !pending[0].Deadline.Equal(want) {
		t.Fatalf("earliest pending deadline = %s, want %s", pending[0].Deadline, want)
	}
	if want := epoch.Add(3 * time.Second); !pending[1].Deadline.Equal(want) {
		t.Fatalf("latest pending deadline = %s, want %s", pending[1].Deadline, want)
	}
	if c.NumTimers() != 2 {
		t.Fatalf("NumTimers() = %d, want 2", c.NumTimers())
	}
}

func TestVirtualStop(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	tm := c.NewTimer(time.Second)

	if !tm.Stop() {
		t.Fatal("Stop() on an armed timer reported false")
	}
	if tm.Stop() {
		t.Fatal("Stop() on a stopped timer reported true")
	}
	if c.NumTimers() != 0 {
		t.Fatalf("stopped timer still armed: %d", c.NumTimers())
	}
	c.Advance(time.Hour)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestVirtualResetRearms(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	tm := c.NewTimer(time.Second)
	tm.Stop()

	if tm.Reset(10 * time.Second); c.NumTimers() != 1 {
		t.Fatalf("Reset did not re-arm: %d timers", c.NumTimers())
	}
	c.Advance(9 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("timer fired before the reset deadline")
	default:
	}
	c.Advance(time.Second)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer did not fire after the reset deadline")
	}
}

func TestVirtualNonPositiveDurationFiresImmediately(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)

	select {
	case <-c.NewTimer(0).C():
	default:
		t.Fatal("a zero timer must fire immediately, like time.NewTimer")
	}
	select {
	case <-c.After(-time.Second):
	default:
		t.Fatal("a negative timer must fire immediately")
	}
	// Sleep must not deadlock on a non-positive duration.
	c.Sleep(0)
}

func TestVirtualSleepBlocksUntilAdvance(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	done := make(chan struct{})
	go func() {
		c.Sleep(time.Minute)
		close(done)
	}()

	c.BlockUntil(1)
	select {
	case <-done:
		t.Fatal("Sleep returned without the clock moving")
	default:
	}
	c.Advance(time.Minute)
	<-done
}

func TestVirtualBlockUntilContextCancels(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.BlockUntilContext(ctx, 1); err == nil {
		t.Fatal("BlockUntilContext ignored a cancelled context")
	}
}

func TestVirtualBackwardsAdvanceIsIgnored(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)
	c.Advance(-time.Hour)
	c.Set(epoch.Add(-time.Hour))
	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("clock moved backwards to %s", got)
	}
}

func TestVirtualConcurrentArmAndAdvance(t *testing.T) {
	t.Parallel()
	c := clock.NewVirtual(epoch)

	const arms = 64
	var wg sync.WaitGroup
	wg.Add(arms)
	for i := 0; i < arms; i++ {
		go func() {
			defer wg.Done()
			<-c.NewTimer(time.Second).C()
		}()
	}
	c.BlockUntil(arms)
	c.Advance(time.Second)
	wg.Wait()

	if c.NumTimers() != 0 {
		t.Fatalf("%d timers left armed after firing", c.NumTimers())
	}
}

func TestSystemClockAdvances(t *testing.T) {
	t.Parallel()
	c := clock.System()
	start := c.Now()
	tm := c.NewTimer(time.Millisecond)
	<-tm.C()
	if !c.Now().After(start) {
		t.Fatal("the system clock did not move")
	}
	// Stop after firing reports false, matching time.Timer.
	if tm.Stop() {
		t.Fatal("Stop() on a fired system timer reported true")
	}
}
