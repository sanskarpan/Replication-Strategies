package clock

import (
	"testing"
	"time"
)

// TestNowStrictlyMonotonic asserts that many rapid Now() calls are strictly
// increasing, even when several land within the same physical millisecond.
func TestNowStrictlyMonotonic(t *testing.T) {
	h := NewHLC()
	const n = 100_000

	prev := h.Now()
	for i := 1; i < n; i++ {
		cur := h.Now()
		if cur <= prev {
			t.Fatalf("Now() not strictly increasing at call %d: prev=%d cur=%d", i, prev, cur)
		}
		prev = cur
	}
}

// TestNowFrozenClock forces every call into the same millisecond to exercise the
// logical-increment path directly.
func TestNowFrozenClock(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	h := NewHLC()
	h.now = func() time.Time { return frozen }

	prev := h.Now()
	if Logical(prev) != 0 {
		t.Fatalf("first Now() logical = %d, want 0", Logical(prev))
	}
	for i := 1; i < 500; i++ {
		cur := h.Now()
		if cur <= prev {
			t.Fatalf("frozen-clock Now() not increasing at %d: prev=%d cur=%d", i, prev, cur)
		}
		if PhysicalMillis(cur) != PhysicalMillis(prev) {
			t.Fatalf("frozen-clock physical changed at %d: %d -> %d", i, PhysicalMillis(prev), PhysicalMillis(cur))
		}
		if Logical(cur) != Logical(prev)+1 {
			t.Fatalf("frozen-clock logical not +1 at %d: %d -> %d", i, Logical(prev), Logical(cur))
		}
		prev = cur
	}
}

// TestUpdateReturnsAtLeastRemoteAndPrior asserts the merge result dominates both
// the last local timestamp and the supplied remote timestamp across a range of
// remote inputs.
func TestUpdateReturnsAtLeastRemoteAndPrior(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	cases := []struct {
		name         string
		remoteAhead  time.Duration // how far the remote physical leads base
		remoteLogicl int64
	}{
		{"remote behind", -5 * time.Second, 3},
		{"remote equal ms", 0, 7},
		{"remote ahead", 10 * time.Second, 0},
		{"remote far ahead high logical", time.Hour, 0xFFFF},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHLC()
			h.now = func() time.Time { return base }

			prior := h.Now()
			remotePhysical := base.Add(tc.remoteAhead).UnixNano() / 1e6
			remote := (remotePhysical << logicalBits) | (tc.remoteLogicl & logicalMask)

			got := h.Update(remote)
			if got < remote {
				t.Fatalf("Update result %d < remote %d", got, remote)
			}
			if got <= prior {
				t.Fatalf("Update result %d not greater than prior Now %d", got, prior)
			}

			// A subsequent Now must still advance.
			next := h.Now()
			if next <= got {
				t.Fatalf("Now after Update did not advance: got=%d next=%d", got, next)
			}
		})
	}
}

// TestTwoHLCsCausalOrdering simulates two nodes exchanging messages and asserts
// that received-then-forwarded timestamps stay causally ordered.
func TestTwoHLCsCausalOrdering(t *testing.T) {
	a := NewHLC()
	b := NewHLC()

	last := int64(-1)
	for round := 0; round < 1000; round++ {
		// A emits, B receives and merges.
		msgA := a.Now()
		if msgA <= last {
			t.Fatalf("round %d: A timestamp %d not after chain head %d", round, msgA, last)
		}
		recvB := b.Update(msgA)
		if recvB < msgA {
			t.Fatalf("round %d: B merge %d lost causality from A %d", round, recvB, msgA)
		}

		// B emits, A receives and merges.
		msgB := b.Now()
		if msgB <= recvB {
			t.Fatalf("round %d: B send %d not after B recv %d", round, msgB, recvB)
		}
		recvA := a.Update(msgB)
		if recvA < msgB {
			t.Fatalf("round %d: A merge %d lost causality from B %d", round, recvA, msgB)
		}
		last = recvA
	}
}

// TestSetSkewMillisShiftsPhysicalNoRegression asserts that skew moves the
// physical component but Now() never goes backward, even when skew turns
// negative.
func TestSetSkewMillisShiftsPhysicalNoRegression(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	h := NewHLC()
	h.now = func() time.Time { return frozen }

	t0 := h.Now()

	// Jump the clock forward.
	h.SetSkewMillis(10_000)
	t1 := h.Now()
	if PhysicalMillis(t1) <= PhysicalMillis(t0) {
		t.Fatalf("positive skew did not raise physical: %d -> %d", PhysicalMillis(t0), PhysicalMillis(t1))
	}
	if t1 <= t0 {
		t.Fatalf("Now regressed after positive skew: %d -> %d", t0, t1)
	}

	// Now yank the clock far backward; Now must not regress.
	h.SetSkewMillis(-60_000)
	prev := t1
	for i := 0; i < 100; i++ {
		cur := h.Now()
		if cur <= prev {
			t.Fatalf("Now regressed under negative skew at %d: %d -> %d", i, prev, cur)
		}
		prev = cur
	}
}

// TestDecodeRoundTrip checks the encode/decode helpers agree.
func TestDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		physical int64
		logical  int64
	}{
		{0, 0},
		{1, 1},
		{1_700_000_000_000, 0xFFFF},
		{42, 12345},
	}
	for _, c := range cases {
		ts := encode(c.physical, c.logical)
		if got := PhysicalMillis(ts); got != c.physical {
			t.Errorf("PhysicalMillis(%d) = %d, want %d", ts, got, c.physical)
		}
		if got := Logical(ts); got != uint64(c.logical) {
			t.Errorf("Logical(%d) = %d, want %d", ts, got, c.logical)
		}
	}
}
