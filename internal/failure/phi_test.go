package failure

import "testing"

// feedRegular records `count` heartbeats spaced `interval` millis apart,
// starting at `start`, and returns the timestamp of the last heartbeat.
func feedRegular(d *Detector, node string, start, interval int64, count int) int64 {
	t := start
	for i := 0; i < count; i++ {
		d.Heartbeat(node, t)
		if i < count-1 {
			t += interval
		}
	}
	return t
}

func TestPhiLowRightAfterHeartbeat(t *testing.T) {
	d := NewDetector()
	const interval = int64(100)
	last := feedRegular(d, "n1", 1000, interval, 50)

	phi := d.Phi("n1", last) // essentially zero elapsed time
	if phi >= 1 {
		t.Fatalf("expected phi < 1 right after heartbeat, got %v", phi)
	}
	if d.Suspected("n1", last, 8) {
		t.Fatalf("node should not be suspected right after a heartbeat")
	}
}

func TestPhiRisesAfterSilence(t *testing.T) {
	d := NewDetector()
	const interval = int64(100)
	last := feedRegular(d, "n1", 1000, interval, 50)

	// Ten intervals of silence.
	now := last + 10*interval
	phi := d.Phi("n1", now)
	if phi < 8 {
		t.Fatalf("expected sharply elevated phi after long silence, got %v", phi)
	}
	if !d.Suspected("n1", now, 8) {
		t.Fatalf("node should be suspected after 10x interval of silence")
	}
}

func TestPhiMonotonicallyNonDecreasing(t *testing.T) {
	d := NewDetector()
	const interval = int64(100)
	last := feedRegular(d, "n1", 1000, interval, 50)

	prev := d.Phi("n1", last)
	for elapsed := int64(0); elapsed <= 2000; elapsed += 25 {
		cur := d.Phi("n1", last+elapsed)
		if cur < prev {
			t.Fatalf("phi decreased as elapsed grew: elapsed=%d prev=%v cur=%v", elapsed, prev, cur)
		}
		prev = cur
	}
}

func TestNoHistoryReturnsZero(t *testing.T) {
	d := NewDetector()
	if phi := d.Phi("ghost", 5000); phi != 0 {
		t.Fatalf("expected phi 0 for unknown node, got %v", phi)
	}

	// A single heartbeat establishes a baseline but no interval yet.
	d.Heartbeat("n1", 1000)
	if phi := d.Phi("n1", 1000); phi != 0 {
		t.Fatalf("expected phi 0 with no interval history, got %v", phi)
	}
}

func TestResetClearsState(t *testing.T) {
	d := NewDetector()
	const interval = int64(100)
	last := feedRegular(d, "n1", 1000, interval, 50)

	// Confirm the node is suspected before reset.
	now := last + 10*interval
	if !d.Suspected("n1", now, 8) {
		t.Fatalf("precondition: node should be suspected before reset")
	}

	d.Reset("n1")

	if phi := d.Phi("n1", now); phi != 0 {
		t.Fatalf("expected phi 0 after reset, got %v", phi)
	}
	if d.Suspected("n1", now, 8) {
		t.Fatalf("node should not be suspected after reset")
	}
}
