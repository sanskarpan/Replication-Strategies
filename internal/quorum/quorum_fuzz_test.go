package quorum

import (
	"testing"
)

// FuzzQuorum derives an (N, W, R) config from fuzz ints, skips invalid ones via
// IsValid, and asserts the numeric invariants of the quorum model.
func FuzzQuorum(f *testing.F) {
	f.Add(3, 2, 2)
	f.Add(5, 3, 3)
	f.Add(1, 1, 1)
	f.Add(0, 0, 0)
	f.Add(100, 1, 100)
	f.Add(-4, 2, 9)

	f.Fuzz(func(t *testing.T, n, w, r int) {
		// Bound N so binomial coefficients stay in a sane range; the model is not
		// meant for astronomically large clusters and huge loops waste fuzz time.
		if n <= 0 || n > 1000 {
			t.Skip()
		}
		// Fold the raw ints into [1, N] so we exercise valid-ish configs often,
		// but still let IsValid be the authority.
		w = 1 + intMod(w, n)
		r = 1 + intMod(r, n)

		q := QuorumConfig{N: n, W: w, R: r}
		if err := q.IsValid(); err != nil {
			// Should not happen given the folding above, but honor the contract:
			// skip anything IsValid rejects rather than asserting on it.
			t.Skip()
		}

		// StaleReadProbability must be a valid probability.
		p := q.StaleReadProbability()
		if p < 0.0 || p > 1.0 || p != p { // p != p catches NaN
			t.Fatalf("StaleReadProbability out of [0,1]: %v (N=%d W=%d R=%d)", p, n, w, r)
		}

		// Strong consistency iff the write and read quorums are guaranteed to overlap.
		wantStrong := w+r > n
		if q.IsStronglyConsistent() != wantStrong {
			t.Fatalf("IsStronglyConsistent=%v want %v (W+R=%d N=%d)", q.IsStronglyConsistent(), wantStrong, w+r, n)
		}

		// Guaranteed overlap is max(0, W+R-N).
		wantOverlap := w + r - n
		if wantOverlap < 0 {
			wantOverlap = 0
		}
		if q.OverlapCount() != wantOverlap {
			t.Fatalf("OverlapCount=%d want %d (W=%d R=%d N=%d)", q.OverlapCount(), wantOverlap, w, r, n)
		}

		// Cross-check: strongly consistent implies overlap >= 1 and zero staleness.
		if q.IsStronglyConsistent() {
			if q.OverlapCount() < 1 {
				t.Fatalf("strongly consistent but OverlapCount=%d (W=%d R=%d N=%d)", q.OverlapCount(), w, r, n)
			}
			if p != 0.0 {
				t.Fatalf("strongly consistent but StaleReadProbability=%v (W=%d R=%d N=%d)", p, w, r, n)
			}
		}

		// Accessor sanity.
		if q.TotalNodes() != n || q.WriteNodes() != w || q.ReadNodes() != r {
			t.Fatalf("accessor mismatch: N=%d W=%d R=%d got (%d,%d,%d)",
				n, w, r, q.TotalNodes(), q.WriteNodes(), q.ReadNodes())
		}
	})
}

// FuzzQuorumPreset asserts every preset yields a valid config for any positive N.
func FuzzQuorumPreset(f *testing.F) {
	f.Add(3)
	f.Add(1)
	f.Add(7)

	presets := []QuorumPreset{
		PresetStrongConsistency,
		PresetQuorumConsistency,
		PresetHighAvailability,
		PresetWriteOptimized,
		PresetReadOptimized,
	}

	f.Fuzz(func(t *testing.T, n int) {
		if n <= 0 || n > 1000 {
			t.Skip()
		}
		for _, p := range presets {
			cfg := Preset(p, n)
			if err := cfg.IsValid(); err != nil {
				t.Fatalf("preset %s produced invalid config for N=%d: %v (%+v)", p, n, err, cfg)
			}
			// The quorum preset must always be strongly consistent.
			if p == PresetQuorumConsistency && !cfg.IsStronglyConsistent() {
				t.Fatalf("quorum preset not strongly consistent for N=%d: %+v", n, cfg)
			}
		}
	})
}

// intMod returns a non-negative remainder in [0, m) for any int, avoiding the
// negative results Go's % gives for negative operands.
func intMod(v, m int) int {
	r := v % m
	if r < 0 {
		r += m
	}
	return r
}
