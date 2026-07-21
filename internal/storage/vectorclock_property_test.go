package storage

import (
	"testing"
	"testing/quick"
)

// vcQuickCfg is the shared property-test configuration.
var vcQuickCfg = &quick.Config{MaxCount: 500}

// vcNodeIDs is a bounded alphabet of node identifiers. A small set makes
// overlapping clock entries (the interesting merge/order cases) likely.
var vcNodeIDs = []string{"a", "b", "c", "d", "e"}

// vcFrom builds a VectorClock from a slice of random ints. Each int selects a
// node (by index) and contributes to that node's counter, so counts stay small
// and overlapping across independently generated clocks.
func vcFrom(ticks []int) VectorClock {
	vc := NewVectorClock()
	for _, t := range ticks {
		if t < 0 {
			t = -t
		}
		vc[vcNodeIDs[t%len(vcNodeIDs)]]++
	}
	return vc
}

// mergedClone merges b into a copy of a, leaving both inputs untouched
// (VectorClock.Merge mutates its receiver in place).
func mergedClone(a, b VectorClock) VectorClock {
	return a.Clone().Merge(b)
}

func TestVectorClockMergeProperties(t *testing.T) {
	commutative := func(a, b []int) bool {
		x, y := vcFrom(a), vcFrom(b)
		return mergedClone(x, y).Equal(mergedClone(y, x))
	}
	if err := quick.Check(commutative, vcQuickCfg); err != nil {
		t.Fatalf("VectorClock merge not commutative: %v", err)
	}

	associative := func(a, b, c []int) bool {
		x, y, z := vcFrom(a), vcFrom(b), vcFrom(c)
		// (x ∨ y) ∨ z
		left := mergedClone(mergedClone(x, y), z)
		// x ∨ (y ∨ z)
		right := mergedClone(x, mergedClone(y, z))
		return left.Equal(right)
	}
	if err := quick.Check(associative, vcQuickCfg); err != nil {
		t.Fatalf("VectorClock merge not associative: %v", err)
	}

	idempotent := func(a []int) bool {
		x := vcFrom(a)
		return mergedClone(x, x).Equal(x)
	}
	if err := quick.Check(idempotent, vcQuickCfg); err != nil {
		t.Fatalf("VectorClock merge not idempotent: %v", err)
	}
}

func TestVectorClockHappensBeforeProperties(t *testing.T) {
	// Irreflexive: no clock happens-before itself.
	irreflexive := func(a []int) bool {
		x := vcFrom(a)
		return !x.HappensBefore(x)
	}
	if err := quick.Check(irreflexive, vcQuickCfg); err != nil {
		t.Fatalf("HappensBefore not irreflexive: %v", err)
	}

	// Antisymmetric: not both a<b and b<a for distinct clocks. (For equal clocks
	// HappensBefore is false by irreflexivity, so this holds vacuously there too.)
	antisymmetric := func(a, b []int) bool {
		x, y := vcFrom(a), vcFrom(b)
		return !(x.HappensBefore(y) && y.HappensBefore(x))
	}
	if err := quick.Check(antisymmetric, vcQuickCfg); err != nil {
		t.Fatalf("HappensBefore not antisymmetric: %v", err)
	}
}

// TestVectorClockLatticeUpperBound verifies the join-semilattice upper-bound
// property: Merge(a,b) must dominate (≥ component-wise) both a and b.
// The existing merge tests cover commutativity, associativity, and idempotence;
// this test makes the upper-bound guarantee explicit via Dominates().
func TestVectorClockLatticeUpperBound(t *testing.T) {
	upperBound := func(a, b []int) bool {
		x, y := vcFrom(a), vcFrom(b)
		merged := mergedClone(x, y)
		return merged.Dominates(x) && merged.Dominates(y)
	}
	if err := quick.Check(upperBound, vcQuickCfg); err != nil {
		t.Fatalf("VectorClock.Merge is not a join-semilattice upper bound: %v", err)
	}
}
