package conflict

import (
	"encoding/json"
	"testing"
)

// gcounterFromBytes deterministically builds a GCounter from fuzz bytes.
func gcounterFromBytes(b []byte) *GCounter {
	g := NewGCounter()
	for i := 0; i+1 < len(b); i += 2 {
		node := string(rune('a' + int(b[i]%6)))
		g.Counts[node] += uint64(b[i+1]) + 1 // +1 so every record advances the counter
	}
	return g
}

// pncounterFromBytes deterministically builds a PNCounter from fuzz bytes.
func pncounterFromBytes(b []byte) *PNCounter {
	p := &PNCounter{Type: PNCounterType, P: map[string]uint64{}, N: map[string]uint64{}}
	for i := 0; i+2 < len(b); i += 3 {
		node := string(rune('a' + int(b[i]%6)))
		p.P[node] += uint64(b[i+1])
		p.N[node] += uint64(b[i+2])
	}
	return p
}

// orsetFromBytes builds an ORSet from fuzz bytes: each record adds an element with
// a unique tag or removes an existing tag.
func orsetFromBytes(b []byte) *ORSet {
	s := &ORSet{Type: ORSetType, Adds: map[string][]string{}, Removes: map[string]bool{}}
	for i := 0; i+2 < len(b); i += 3 {
		el := string(rune('x' + int(b[i]%3)))
		tag := el + ":" + string(rune('0'+int(b[i+1]%10)))
		if b[i+2]%4 == 0 {
			// occasionally remove a tag we may have added
			s.Removes[tag] = true
			continue
		}
		found := false
		for _, existing := range s.Adds[el] {
			if existing == tag {
				found = true
				break
			}
		}
		if !found {
			s.Adds[el] = append(s.Adds[el], tag)
		}
	}
	return s
}

// FuzzCRDTMerge asserts the join-semilattice laws (idempotent, commutative,
// associative) for the state-based CRDTs, plus counter monotonicity: a merged
// counter is never smaller than either input.
func FuzzCRDTMerge(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add([]byte{0, 3, 1, 5}, []byte{0, 4, 2, 1})
	f.Add([]byte{0, 1, 2, 1, 2, 3}, []byte{1, 2, 0, 3, 2, 1})
	f.Add([]byte{0, 0, 1, 1, 1, 4}, []byte{2, 0, 3, 0, 0, 8})

	f.Fuzz(func(t *testing.T, ba, bb []byte) {
		// ---------- GCounter ----------
		{
			a := gcounterFromBytes(ba)
			b := gcounterFromBytes(bb)

			// Idempotent.
			if aa := a.Merge(a); aa.Value() != a.Value() {
				t.Fatalf("gcounter merge not idempotent: %d vs %d", aa.Value(), a.Value())
			}
			// Commutative (compare by value and by serialized counts map).
			ab := a.Merge(b)
			ba2 := b.Merge(a)
			if ab.Value() != ba2.Value() || !sameUintMap(ab.Counts, ba2.Counts) {
				t.Fatalf("gcounter merge not commutative: %v vs %v", ab.Counts, ba2.Counts)
			}
			// Monotonic: merged value >= each input.
			if ab.Value() < a.Value() || ab.Value() < b.Value() {
				t.Fatalf("gcounter merge not monotonic: merged=%d a=%d b=%d", ab.Value(), a.Value(), b.Value())
			}
			// Per-node values are the element-wise max (>= each side).
			for node, v := range a.Counts {
				if ab.Counts[node] < v {
					t.Fatalf("gcounter merge dropped node %q: merged=%d a=%d", node, ab.Counts[node], v)
				}
			}
			// Associative.
			c := gcounterFromBytes(append(append([]byte{}, ba...), bb...))
			left := a.Merge(b).Merge(c)
			right := a.Merge(b.Merge(c))
			if !sameUintMap(left.Counts, right.Counts) {
				t.Fatalf("gcounter merge not associative: %v vs %v", left.Counts, right.Counts)
			}
		}

		// ---------- PNCounter ----------
		{
			a := pncounterFromBytes(ba)
			b := pncounterFromBytes(bb)

			if aa := a.Merge(a); aa.Value() != a.Value() {
				t.Fatalf("pncounter merge not idempotent: %d vs %d", aa.Value(), a.Value())
			}
			ab := a.Merge(b)
			ba2 := b.Merge(a)
			if ab.Value() != ba2.Value() {
				t.Fatalf("pncounter merge not commutative: %d vs %d", ab.Value(), ba2.Value())
			}
			// The P and N components each grow monotonically (element-wise max),
			// so their sums are >= each input's component sums.
			if sumUint(ab.P) < sumUint(a.P) || sumUint(ab.P) < sumUint(b.P) {
				t.Fatalf("pncounter P not monotonic")
			}
			if sumUint(ab.N) < sumUint(a.N) || sumUint(ab.N) < sumUint(b.N) {
				t.Fatalf("pncounter N not monotonic")
			}
			c := pncounterFromBytes(append(append([]byte{}, ba...), bb...))
			left := a.Merge(b).Merge(c)
			right := a.Merge(b.Merge(c))
			if left.Value() != right.Value() {
				t.Fatalf("pncounter merge not associative: %d vs %d", left.Value(), right.Value())
			}
		}

		// ---------- ORSet ----------
		{
			a := orsetFromBytes(ba)
			b := orsetFromBytes(bb)

			// Idempotent (element set stable).
			if aa := a.Merge(a); !sameStrSlice(aa.Elements(), a.Elements()) {
				t.Fatalf("orset merge not idempotent: %v vs %v", aa.Elements(), a.Elements())
			}
			// Commutative.
			ab := a.Merge(b)
			ba2 := b.Merge(a)
			if !sameStrSlice(ab.Elements(), ba2.Elements()) {
				t.Fatalf("orset merge not commutative: %v vs %v", ab.Elements(), ba2.Elements())
			}
			// Associative.
			c := orsetFromBytes(append(append([]byte{}, ba...), bb...))
			left := a.Merge(b).Merge(c)
			right := a.Merge(b.Merge(c))
			if !sameStrSlice(left.Elements(), right.Elements()) {
				t.Fatalf("orset merge not associative: %v vs %v", left.Elements(), right.Elements())
			}
		}

		// ---------- mergeCRDT dispatch ----------
		// Exercise the resolver's dispatch path with real serialized CRDT payloads
		// and confirm two matching GCounters merge (ok=true) commutatively.
		{
			a := gcounterFromBytes(ba)
			b := gcounterFromBytes(bb)
			la, err1 := json.Marshal(a)
			rb, err2 := json.Marshal(b)
			if err1 != nil || err2 != nil {
				t.Skip()
			}
			out1, _, ok1 := mergeCRDT(la, rb)
			out2, _, ok2 := mergeCRDT(rb, la)
			if !ok1 || !ok2 {
				t.Fatalf("mergeCRDT did not recognize two gcounters: ok1=%v ok2=%v", ok1, ok2)
			}
			var g1, g2 GCounter
			if json.Unmarshal(out1, &g1) != nil || json.Unmarshal(out2, &g2) != nil {
				t.Fatalf("mergeCRDT produced unparseable output")
			}
			if g1.Value() != g2.Value() {
				t.Fatalf("mergeCRDT dispatch not commutative: %d vs %d", g1.Value(), g2.Value())
			}
			if g1.Value() < a.Value() || g1.Value() < b.Value() {
				t.Fatalf("mergeCRDT dispatch not monotonic: merged=%d a=%d b=%d", g1.Value(), a.Value(), b.Value())
			}
		}
	})
}

func sameUintMap(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sumUint(m map[string]uint64) uint64 {
	var s uint64
	for _, v := range m {
		s += v
	}
	return s
}

func sameStrSlice(a, b []string) bool {
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
