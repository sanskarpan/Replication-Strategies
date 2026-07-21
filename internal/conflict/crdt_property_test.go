package conflict

import (
	"fmt"
	"reflect"
	"replication-strategies/internal/storage"
	"sort"
	"testing"
	"testing/quick"
)

// quickCfg is the shared property-test configuration. MaxCount controls how many
// random inputs quick.Check generates per law.
var quickCfg = &quick.Config{MaxCount: 500}

// -----------------------------------------------------------------------------
// Helpers to build deterministic CRDT instances from random ints/strings so we
// do not depend on quick's default generation of unexported/pointer types.
// -----------------------------------------------------------------------------

// nodeIDs is a small fixed alphabet of node identifiers. Using a bounded set
// (rather than arbitrary strings) makes overlapping keys — and therefore the
// interesting merge cases — likely to occur.
var nodeIDs = []string{"a", "b", "c", "d"}

func nodeOf(i int) string {
	if i < 0 {
		i = -i
	}
	return nodeIDs[i%len(nodeIDs)]
}

func elemOf(i int) string {
	if i < 0 {
		i = -i
	}
	return fmt.Sprintf("el%d", i%6)
}

func tagOf(i int) string {
	if i < 0 {
		i = -i
	}
	return fmt.Sprintf("t%d", i%20)
}

// -----------------------------------------------------------------------------
// G-Counter
// -----------------------------------------------------------------------------

// gcounterFrom builds a G-Counter from a slice of random node indices; each
// entry contributes an increment to that node's count.
func gcounterFrom(incs []int) *GCounter {
	g := NewGCounter()
	for _, i := range incs {
		g.Increment(nodeOf(i))
	}
	return g
}

func TestGCounterMergeProperties(t *testing.T) {
	commutative := func(a, b []int) bool {
		x := gcounterFrom(a)
		y := gcounterFrom(b)
		return x.Merge(y).Value() == y.Merge(x).Value()
	}
	if err := quick.Check(commutative, quickCfg); err != nil {
		t.Fatalf("G-Counter merge not commutative: %v", err)
	}

	associative := func(a, b, c []int) bool {
		x, y, z := gcounterFrom(a), gcounterFrom(b), gcounterFrom(c)
		left := x.Merge(y).Merge(z).Value()
		right := x.Merge(y.Merge(z)).Value()
		return left == right
	}
	if err := quick.Check(associative, quickCfg); err != nil {
		t.Fatalf("G-Counter merge not associative: %v", err)
	}

	idempotent := func(a []int) bool {
		x := gcounterFrom(a)
		return x.Merge(x).Value() == x.Value()
	}
	if err := quick.Check(idempotent, quickCfg); err != nil {
		t.Fatalf("G-Counter merge not idempotent: %v", err)
	}
}

// -----------------------------------------------------------------------------
// PN-Counter
// -----------------------------------------------------------------------------

// pnCounterFrom builds a PN-Counter: positive indices increment P, negative
// indices increment N.
func pnCounterFrom(ops []int) *PNCounter {
	pn := &PNCounter{Type: PNCounterType, P: map[string]uint64{}, N: map[string]uint64{}}
	for _, i := range ops {
		if i%2 == 0 {
			pn.P[nodeOf(i)]++
		} else {
			pn.N[nodeOf(i)]++
		}
	}
	return pn
}

func TestPNCounterMergeProperties(t *testing.T) {
	commutative := func(a, b []int) bool {
		x, y := pnCounterFrom(a), pnCounterFrom(b)
		return x.Merge(y).Value() == y.Merge(x).Value()
	}
	if err := quick.Check(commutative, quickCfg); err != nil {
		t.Fatalf("PN-Counter merge not commutative: %v", err)
	}

	associative := func(a, b, c []int) bool {
		x, y, z := pnCounterFrom(a), pnCounterFrom(b), pnCounterFrom(c)
		left := x.Merge(y).Merge(z).Value()
		right := x.Merge(y.Merge(z)).Value()
		return left == right
	}
	if err := quick.Check(associative, quickCfg); err != nil {
		t.Fatalf("PN-Counter merge not associative: %v", err)
	}

	idempotent := func(a []int) bool {
		x := pnCounterFrom(a)
		return x.Merge(x).Value() == x.Value()
	}
	if err := quick.Check(idempotent, quickCfg); err != nil {
		t.Fatalf("PN-Counter merge not idempotent: %v", err)
	}
}

// -----------------------------------------------------------------------------
// OR-Set
// -----------------------------------------------------------------------------

// orSetFrom builds an OR-Set. adds is a list of (element, tag) pairs derived
// from paired ints; rems removes the tag derived from each int.
func orSetFrom(adds, rems []int) *ORSet {
	s := &ORSet{Type: ORSetType, Adds: map[string][]string{}, Removes: map[string]bool{}}
	for i, v := range adds {
		el := elemOf(v)
		tg := tagOf(v + i)
		s.Adds[el] = append(s.Adds[el], tg)
	}
	for _, v := range rems {
		s.Removes[tagOf(v)] = true
	}
	return s
}

func elementsEqual(a, b *ORSet) bool {
	ea, eb := a.Elements(), b.Elements()
	sort.Strings(ea)
	sort.Strings(eb)
	return reflect.DeepEqual(ea, eb)
}

func TestORSetMergeProperties(t *testing.T) {
	commutative := func(adds1, rems1, adds2, rems2 []int) bool {
		x := orSetFrom(adds1, rems1)
		y := orSetFrom(adds2, rems2)
		return elementsEqual(x.Merge(y), y.Merge(x))
	}
	if err := quick.Check(commutative, quickCfg); err != nil {
		t.Fatalf("OR-Set merge not commutative: %v", err)
	}

	associative := func(a1, r1, a2, r2, a3, r3 []int) bool {
		x := orSetFrom(a1, r1)
		y := orSetFrom(a2, r2)
		z := orSetFrom(a3, r3)
		left := x.Merge(y).Merge(z)
		right := x.Merge(y.Merge(z))
		return elementsEqual(left, right)
	}
	if err := quick.Check(associative, quickCfg); err != nil {
		t.Fatalf("OR-Set merge not associative: %v", err)
	}

	idempotent := func(adds, rems []int) bool {
		x := orSetFrom(adds, rems)
		return elementsEqual(x.Merge(x), x)
	}
	if err := quick.Check(idempotent, quickCfg); err != nil {
		t.Fatalf("OR-Set merge not idempotent: %v", err)
	}
}

// -----------------------------------------------------------------------------
// LWW-Map
// -----------------------------------------------------------------------------

// lwwMapFrom builds an LWW-Map from parallel slices of random ints; each op sets
// key elemOf(k) to a value tagged with a timestamp and node derived from the ints.
func lwwMapFrom(keys, ts, nodes []int) *LWWMap {
	m := &LWWMap{Type: LWWMapType, Entries: map[string]LWWMapEntry{}}
	n := len(keys)
	if len(ts) < n {
		n = len(ts)
	}
	if len(nodes) < n {
		n = len(nodes)
	}
	for i := 0; i < n; i++ {
		key := elemOf(keys[i])
		cand := LWWMapEntry{
			Value:     fmt.Sprintf("v%d", keys[i]),
			Timestamp: int64(ts[i] % 1000),
			NodeID:    nodeOf(nodes[i]),
		}
		cur, ok := m.Entries[key]
		if !ok || cand.Timestamp > cur.Timestamp || (cand.Timestamp == cur.Timestamp && cand.NodeID > cur.NodeID) {
			m.Entries[key] = cand
		}
	}
	return m
}

func entriesEqual(a, b *LWWMap) bool {
	return reflect.DeepEqual(a.Entries, b.Entries)
}

func TestLWWMapMergeProperties(t *testing.T) {
	commutative := func(k1, t1, n1, k2, t2, n2 []int) bool {
		x := lwwMapFrom(k1, t1, n1)
		y := lwwMapFrom(k2, t2, n2)
		return entriesEqual(x.Merge(y), y.Merge(x))
	}
	if err := quick.Check(commutative, quickCfg); err != nil {
		t.Fatalf("LWW-Map merge not commutative: %v", err)
	}

	associative := func(k1, t1, n1, k2, t2, n2, k3, t3, n3 []int) bool {
		x := lwwMapFrom(k1, t1, n1)
		y := lwwMapFrom(k2, t2, n2)
		z := lwwMapFrom(k3, t3, n3)
		left := x.Merge(y).Merge(z)
		right := x.Merge(y.Merge(z))
		return entriesEqual(left, right)
	}
	if err := quick.Check(associative, quickCfg); err != nil {
		t.Fatalf("LWW-Map merge not associative: %v", err)
	}

	idempotent := func(k, ts, n []int) bool {
		x := lwwMapFrom(k, ts, n)
		return entriesEqual(x.Merge(x), x)
	}
	if err := quick.Check(idempotent, quickCfg); err != nil {
		t.Fatalf("LWW-Map merge not idempotent: %v", err)
	}
}

// TestConflictResolutionDeterminism verifies that LWWResolver is deterministic
// (same input → same output on repeated calls) and call-order independent
// (swapping Local/Remote yields the same winning node, because the winner is
// defined by timestamp + nodeID tiebreak, not by argument position).
func TestConflictResolutionDeterminism(t *testing.T) {
	mkEntry := func(ts int64, nodeIdx int) *storage.KVEntry {
		if nodeIdx < 0 {
			nodeIdx = -nodeIdx
		}
		node := nodeIDs[nodeIdx%len(nodeIDs)]
		return &storage.KVEntry{
			Key:       "k",
			Value:     []byte(node),
			Timestamp: ts % 1000,
			NodeID:    node,
			VClock:    storage.NewVectorClock(),
		}
	}

	deterministic := func(lts, rts int64, li, ri int) bool {
		local := mkEntry(lts, li)
		remote := mkEntry(rts, ri)
		c := &Conflict{ID: "c1", Key: "k", Local: local, Remote: remote, NodeID: local.NodeID}

		r := NewLWWResolver()
		res1 := r.Resolve(c)
		res2 := r.Resolve(c)
		if res1.Winner.NodeID != res2.Winner.NodeID || res1.Winner.Timestamp != res2.Winner.Timestamp {
			return false
		}

		// Call-order independence: the same node wins regardless of local/remote assignment.
		cSwap := &Conflict{ID: "c2", Key: "k", Local: remote, Remote: local, NodeID: remote.NodeID}
		resSwap := r.Resolve(cSwap)
		return res1.Winner.NodeID == resSwap.Winner.NodeID
	}
	if err := quick.Check(deterministic, quickCfg); err != nil {
		t.Fatalf("LWWResolver not deterministic or call-order independent: %v", err)
	}
}
