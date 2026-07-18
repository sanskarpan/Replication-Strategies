package transport

import (
	"math/rand"
	"testing"
)

// TestLatencyModelSample verifies the jittered/heavy-tail latency distribution:
// samples stay within [base-jitter, base+jitter] except for occasional tail spikes,
// tail spikes actually occur, and negative latency is clamped to zero.
func TestLatencyModelSample(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	m := latencyModel{base: 50, jitter: 10, tailProb: 0.1, tailMult: 5}

	tailHits := 0
	minSeen, maxSeen := 1<<30, 0
	for i := 0; i < 5000; i++ {
		v := m.sample(rng)
		if v < 0 {
			t.Fatalf("latency must never be negative, got %d", v)
		}
		// A tail spike adds base*tailMult (=250), pushing well past base+jitter (=60).
		if v > m.base+m.jitter {
			tailHits++
		} else {
			if v < minSeen {
				minSeen = v
			}
			if v > maxSeen {
				maxSeen = v
			}
		}
	}
	if tailHits == 0 {
		t.Fatal("expected some heavy-tail spikes with tailProb=0.1 over 5000 samples")
	}
	// Roughly 10% tail; guard against a wildly wrong probability.
	if tailHits > 1500 {
		t.Fatalf("far too many tail spikes: %d/5000 (expected ~500)", tailHits)
	}
	// Non-tail samples must respect the jitter band around base.
	if minSeen < m.base-m.jitter || maxSeen > m.base+m.jitter {
		t.Fatalf("non-tail samples out of jitter band: min=%d max=%d (band %d..%d)",
			minSeen, maxSeen, m.base-m.jitter, m.base+m.jitter)
	}
}

// TestLatencyModelNoJitterNoTail verifies a degenerate model returns exactly base.
func TestLatencyModelNoJitterNoTail(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	m := latencyModel{base: 30}
	for i := 0; i < 100; i++ {
		if v := m.sample(rng); v != 30 {
			t.Fatalf("expected constant 30, got %d", v)
		}
	}
}

// TestSetLatencyDistRoundTrip verifies the setter stores a model retrievable by getLatencyModel.
func TestSetLatencyDistRoundTrip(t *testing.T) {
	f := NewNetworkFabric()
	defer f.Close()
	if _, ok := f.getLatencyModel("a", "b"); ok {
		t.Fatal("expected no model before SetLatencyDist")
	}
	f.SetLatencyDist("a", "b", 40, 5, 0.2, 4)
	m, ok := f.getLatencyModel("a", "b")
	if !ok || m.base != 40 || m.jitter != 5 || m.tailProb != 0.2 || m.tailMult != 4 {
		t.Fatalf("model round-trip failed: %+v ok=%v", m, ok)
	}
	f.ClearFaults()
	if _, ok := f.getLatencyModel("a", "b"); ok {
		t.Fatal("ClearFaults should drop latency distributions")
	}
}
