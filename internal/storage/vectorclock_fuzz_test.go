package storage

import (
	"testing"
)

// vcFromBytes deterministically derives a VectorClock from fuzz bytes. It reads
// the buffer in fixed-size records so different inputs produce a wide variety of
// clocks (varying node counts, node ids, and timestamps) without ever panicking.
func vcFromBytes(b []byte) VectorClock {
	vc := NewVectorClock()
	// Each record is 3 bytes: node id selector + a 2-byte timestamp.
	for i := 0; i+2 < len(b); i += 3 {
		// Constrain node ids to a small alphabet so overlap between the two
		// clocks is common (that is where the interesting lattice cases live).
		node := string(rune('a' + int(b[i]%8)))
		ts := uint64(b[i+1])<<8 | uint64(b[i+2])
		if ts > vc[node] {
			vc[node] = ts
		}
	}
	return vc
}

// FuzzVectorClockMerge asserts the join-semilattice laws that VectorClock.Merge
// must obey, plus the mutual consistency of HappensBefore/Concurrent/Equal.
func FuzzVectorClockMerge(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add([]byte{0, 0, 1}, []byte{0, 0, 2})
	f.Add([]byte{1, 0, 5, 2, 0, 7}, []byte{1, 0, 5, 3, 0, 1})
	f.Add([]byte{0, 1, 0, 1, 1, 0}, []byte{0, 1, 0})

	f.Fuzz(func(t *testing.T, ba, bb []byte) {
		a := vcFromBytes(ba)
		b := vcFromBytes(bb)

		// Idempotency: Merge(a, a) == a. Merge mutates the receiver, so clone.
		if got := a.Clone().Merge(a.Clone()); !got.Equal(a) {
			t.Fatalf("merge not idempotent: Merge(a,a)=%s want a=%s", got, a)
		}

		// Commutativity: Merge(a, b) == Merge(b, a).
		ab := a.Clone().Merge(b.Clone())
		ba2 := b.Clone().Merge(a.Clone())
		if !ab.Equal(ba2) {
			t.Fatalf("merge not commutative: Merge(a,b)=%s Merge(b,a)=%s", ab, ba2)
		}

		// Least-upper-bound: the merge dominates (is causally >=) both inputs.
		if !ab.Dominates(a) {
			t.Fatalf("merge does not dominate a: merge=%s a=%s", ab, a)
		}
		if !ab.Dominates(b) {
			t.Fatalf("merge does not dominate b: merge=%s b=%s", ab, b)
		}

		// Associativity is implied by max-fold but check it anyway with a third
		// clock derived by merging a and b's tails.
		c := vcFromBytes(append(append([]byte{}, ba...), bb...))
		left := a.Clone().Merge(b.Clone()).Merge(c.Clone())
		right := a.Clone().Merge(b.Clone().Merge(c.Clone()))
		if !left.Equal(right) {
			t.Fatalf("merge not associative: (a.b).c=%s a.(b.c)=%s", left, right)
		}

		// Ordering-predicate consistency. Exactly one of the four relations holds:
		// Equal, a<b, b<a, or concurrent. They must be mutually exclusive and total.
		eq := a.Equal(b)
		aBeforeB := a.HappensBefore(b)
		bBeforeA := b.HappensBefore(a)
		conc := a.Concurrent(b)

		count := 0
		for _, rel := range []bool{eq, aBeforeB, bBeforeA, conc} {
			if rel {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("relations not mutually exclusive/total: eq=%v a<b=%v b<a=%v conc=%v (a=%s b=%s)",
				eq, aBeforeB, bBeforeA, conc, a, b)
		}

		// Concurrent is symmetric.
		if a.Concurrent(b) != b.Concurrent(a) {
			t.Fatalf("Concurrent not symmetric: a.Concurrent(b)=%v b.Concurrent(a)=%v", a.Concurrent(b), b.Concurrent(a))
		}

		// If a happens-before b then b dominates a but not vice versa (strict).
		if aBeforeB {
			if !b.Dominates(a) {
				t.Fatalf("a<b but b does not dominate a (a=%s b=%s)", a, b)
			}
			if a.Dominates(b) {
				t.Fatalf("a<b but a also dominates b, contradiction (a=%s b=%s)", a, b)
			}
		}

		// Equality and HappensBefore are disjoint (HappensBefore is strict).
		if eq && (aBeforeB || bBeforeA) {
			t.Fatalf("equal clocks cannot happen-before each other (a=%s b=%s)", a, b)
		}
	})
}
