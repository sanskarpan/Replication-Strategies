package simclock

import (
	"testing"
	"time"
)

// randSequence draws n Intn(bound) values from the given clock's RNG.
func randSequence(c Clock, n, bound int) []int {
	out := make([]int, n)
	r := c.Rand()
	for i := range out {
		out[i] = r.Intn(bound)
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSameSeedIdenticalSequence asserts two VirtualClocks with the same seed
// produce the identical stream of Rand().Intn values.
func TestSameSeedIdenticalSequence(t *testing.T) {
	a := NewVirtualClock(42)
	b := NewVirtualClock(42)

	seqA := randSequence(a, 1000, 1_000_000)
	seqB := randSequence(b, 1000, 1_000_000)

	if !equalInts(seqA, seqB) {
		t.Fatalf("same seed produced diverging random sequences")
	}
}

// TestDifferentSeedDifferentSequence asserts different seeds diverge.
func TestDifferentSeedDifferentSequence(t *testing.T) {
	a := NewVirtualClock(1)
	b := NewVirtualClock(2)

	seqA := randSequence(a, 1000, 1_000_000)
	seqB := randSequence(b, 1000, 1_000_000)

	if equalInts(seqA, seqB) {
		t.Fatalf("different seeds produced identical random sequences (astronomically unlikely -> bug)")
	}
}

// TestAdvanceFiresDueTimersInOrder registers several timers at distinct
// deadlines and asserts Advance fires the due ones in time order while leaving
// not-yet-due timers pending.
func TestAdvanceFiresDueTimersInOrder(t *testing.T) {
	c := NewVirtualClock(7)

	// Register out of deadline order to prove ordering comes from the heap.
	ch30 := c.After(30 * time.Millisecond)
	ch10 := c.After(10 * time.Millisecond)
	ch20 := c.After(20 * time.Millisecond)
	ch100 := c.After(100 * time.Millisecond)

	if got := c.Pending(); got != 4 {
		t.Fatalf("Pending() = %d, want 4", got)
	}

	// Advance to 25ms: the 10ms and 20ms timers are due; 30ms and 100ms are not.
	fired := c.Advance(25 * time.Millisecond)
	if fired != 2 {
		t.Fatalf("Advance fired %d timers, want 2", fired)
	}
	if got := c.Pending(); got != 2 {
		t.Fatalf("after first Advance Pending() = %d, want 2", got)
	}

	// The two due timers must have delivered, in ascending deadline order.
	t10 := mustRecv(t, ch10, "10ms")
	t20 := mustRecv(t, ch20, "20ms")
	if !t10.Before(t20) {
		t.Fatalf("timers fired out of order: 10ms=%v not before 20ms=%v", t10, t20)
	}
	if t10.UnixNano() != int64(10*time.Millisecond) {
		t.Fatalf("10ms timer fired at %d ns, want %d", t10.UnixNano(), int64(10*time.Millisecond))
	}
	if t20.UnixNano() != int64(20*time.Millisecond) {
		t.Fatalf("20ms timer fired at %d ns, want %d", t20.UnixNano(), int64(20*time.Millisecond))
	}

	// The 30ms and 100ms timers must still be pending (not delivered yet).
	mustNotRecv(t, ch30, "30ms")
	mustNotRecv(t, ch100, "100ms")

	// Advance past 30ms only.
	if fired := c.Advance(10 * time.Millisecond); fired != 1 { // now at 35ms
		t.Fatalf("second Advance fired %d, want 1", fired)
	}
	mustRecv(t, ch30, "30ms")
	mustNotRecv(t, ch100, "100ms")

	// Advance past the final timer.
	if fired := c.Advance(100 * time.Millisecond); fired != 1 { // now at 135ms
		t.Fatalf("final Advance fired %d, want 1", fired)
	}
	mustRecv(t, ch100, "100ms")
	if got := c.Pending(); got != 0 {
		t.Fatalf("all timers should have fired, Pending() = %d", got)
	}
}

// TestAfterDeliversExactlyWhenAdvanceCrosses asserts After(d) delivers exactly
// on the Advance that crosses d, and that Now tracks logical time.
func TestAfterDeliversExactlyWhenAdvanceCrosses(t *testing.T) {
	c := NewVirtualClock(0)
	ch := c.After(50 * time.Millisecond)

	// Just short of the deadline: nothing fires.
	c.Advance(49 * time.Millisecond)
	mustNotRecv(t, ch, "before deadline")
	if c.Now() != int64(49*time.Millisecond) {
		t.Fatalf("Now() = %d, want %d", c.Now(), int64(49*time.Millisecond))
	}

	// Crossing the deadline: fires.
	c.Advance(1 * time.Millisecond)
	got := mustRecv(t, ch, "at deadline")
	if got.UnixNano() != int64(50*time.Millisecond) {
		t.Fatalf("After delivered %d ns, want %d", got.UnixNano(), int64(50*time.Millisecond))
	}
	if c.Now() != int64(50*time.Millisecond) {
		t.Fatalf("Now() = %d, want %d", c.Now(), int64(50*time.Millisecond))
	}
}

// TestExactBoundaryFires asserts a timer whose deadline equals the new logical
// time fires (<= semantics, not <).
func TestExactBoundaryFires(t *testing.T) {
	c := NewVirtualClock(0)
	ch := c.After(10 * time.Millisecond)
	if fired := c.Advance(10 * time.Millisecond); fired != 1 {
		t.Fatalf("timer at exact boundary did not fire: fired=%d", fired)
	}
	mustRecv(t, ch, "boundary")
}

// TestSameSeedSameFiringOrder asserts the determinism guarantee across both
// randomness and timer ordering for an identical Advance sequence.
func TestSameSeedSameFiringOrder(t *testing.T) {
	build := func() ([]int, []int64) {
		c := NewVirtualClock(99)
		// Draw randomness to choose deadlines, mixing RNG and timer state.
		chans := make([]<-chan time.Time, 20)
		deadlines := make([]int64, 20)
		for i := range chans {
			d := time.Duration(c.Rand().Intn(100)+1) * time.Millisecond
			chans[i] = c.After(d)
			deadlines[i] = c.Now() + int64(d)
		}
		rands := randSequence(c, 50, 10_000)

		// Advance in fixed steps and record firing order by deadline value.
		var fireOrder []int64
		for step := 0; step < 12; step++ {
			c.Advance(10 * time.Millisecond)
			for i, ch := range chans {
				if ch == nil {
					continue
				}
				select {
				case ts := <-ch:
					fireOrder = append(fireOrder, ts.UnixNano())
					chans[i] = nil
				default:
				}
			}
		}
		return rands, fireOrder
	}

	rA, fA := build()
	rB, fB := build()

	if !equalInts(rA, rB) {
		t.Fatalf("interleaved random sequences diverged for same seed")
	}
	if len(fA) != len(fB) {
		t.Fatalf("firing counts differ: %d vs %d", len(fA), len(fB))
	}
	for i := range fA {
		if fA[i] != fB[i] {
			t.Fatalf("firing order diverged at %d: %d vs %d", i, fA[i], fB[i])
		}
	}
}

// TestRealClockNowMonotonic asserts RealClock.Now never goes backwards.
func TestRealClockNowMonotonic(t *testing.T) {
	c := NewRealClock(123)
	prev := c.Now()
	for i := 0; i < 100_000; i++ {
		cur := c.Now()
		if cur < prev {
			t.Fatalf("RealClock.Now regressed at %d: prev=%d cur=%d", i, prev, cur)
		}
		prev = cur
	}
}

// TestRealClockRandReproducible asserts RealClock randomness replays from seed.
func TestRealClockRandReproducible(t *testing.T) {
	a := NewRealClock(555)
	b := NewRealClock(555)
	if !equalInts(randSequence(a, 500, 1_000_000), randSequence(b, 500, 1_000_000)) {
		t.Fatalf("RealClock with same seed produced diverging random sequences")
	}
}

// TestSleepUnblocksOnAdvance asserts Sleep parks until Advance crosses it.
func TestSleepUnblocksOnAdvance(t *testing.T) {
	c := NewVirtualClock(0)
	done := make(chan struct{})
	go func() {
		c.Sleep(30 * time.Millisecond)
		close(done)
	}()

	// Wait until the sleeper has registered its timer.
	waitFor(t, func() bool { return c.Pending() == 1 })

	c.Advance(10 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("Sleep returned before its deadline")
	case <-time.After(20 * time.Millisecond):
	}

	c.Advance(30 * time.Millisecond)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Sleep did not return after Advance crossed its deadline")
	}
}

// --- helpers ---

func mustRecv(t *testing.T, ch <-chan time.Time, name string) time.Time {
	t.Helper()
	select {
	case v := <-ch:
		return v
	default:
		t.Fatalf("expected %s timer to have fired, but channel was empty", name)
		return time.Time{}
	}
}

func mustNotRecv(t *testing.T, ch <-chan time.Time, name string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("expected %s timer to still be pending, but it fired at %v", name, v)
	default:
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
