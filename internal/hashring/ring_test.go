package hashring

import (
	"fmt"
	"reflect"
	"testing"
)

func newRingWith(vnodes int, nodes ...string) *Ring {
	r := NewRing(vnodes)
	for _, n := range nodes {
		r.Add(n)
	}
	return r
}

func TestPreferenceListDeterministic(t *testing.T) {
	r := newRingWith(128, "a", "b", "c", "d", "e")
	first := r.PreferenceList("some-key", 3)
	for i := 0; i < 50; i++ {
		got := r.PreferenceList("some-key", 3)
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("PreferenceList not deterministic: %v vs %v", first, got)
		}
	}
	// Rebuilding an identical ring yields the same result.
	r2 := newRingWith(128, "a", "b", "c", "d", "e")
	if got := r2.PreferenceList("some-key", 3); !reflect.DeepEqual(first, got) {
		t.Fatalf("PreferenceList not stable across identical rings: %v vs %v", first, got)
	}
}

func TestPreferenceListCountAndDistinct(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	r := newRingWith(64, nodes...)

	cases := []struct {
		n    int
		want int
	}{
		{1, 1},
		{3, 3},
		{5, 5},
		{10, 5}, // capped at len(nodes)
		{0, 0},
	}
	for _, c := range cases {
		got := r.PreferenceList("key-x", c.n)
		if len(got) != c.want {
			t.Fatalf("n=%d: got %d nodes, want %d (%v)", c.n, len(got), c.want, got)
		}
		seen := make(map[string]bool)
		for _, node := range got {
			if seen[node] {
				t.Fatalf("n=%d: duplicate node %q in %v", c.n, node, got)
			}
			seen[node] = true
		}
	}
}

func TestPreferenceListEmptyRing(t *testing.T) {
	r := NewRing(16)
	if got := r.PreferenceList("k", 3); got != nil {
		t.Fatalf("empty ring should return nil, got %v", got)
	}
}

func TestNodesSorted(t *testing.T) {
	r := newRingWith(8, "c", "a", "b")
	got := r.Nodes()
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Nodes() = %v, want %v", got, want)
	}
}

func TestAddIdempotent(t *testing.T) {
	r := NewRing(32)
	r.Add("a")
	r.mu.RLock()
	before := len(r.tokens)
	r.mu.RUnlock()

	r.Add("a") // no-op
	r.mu.RLock()
	after := len(r.tokens)
	r.mu.RUnlock()

	if before != after {
		t.Fatalf("Add not idempotent: tokens %d -> %d", before, after)
	}
	if want := []string{"a"}; !reflect.DeepEqual(r.Nodes(), want) {
		t.Fatalf("Nodes() = %v, want %v", r.Nodes(), want)
	}
}

func TestRemoveUnknownNoop(t *testing.T) {
	r := newRingWith(16, "a", "b")
	r.Remove("zzz")
	if want := []string{"a", "b"}; !reflect.DeepEqual(r.Nodes(), want) {
		t.Fatalf("Nodes() = %v, want %v", r.Nodes(), want)
	}
}

func TestKeyDistribution(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	r := newRingWith(128, nodes...)

	owners := make(map[string]int)
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		pl := r.PreferenceList(fmt.Sprintf("key-%d", i), 1)
		if len(pl) != 1 {
			t.Fatalf("expected 1 primary, got %v", pl)
		}
		owners[pl[0]]++
	}
	for _, n := range nodes {
		if owners[n] == 0 {
			t.Fatalf("node %q owns no keys; distribution: %v", n, owners)
		}
	}
}

func TestRemoveMinimalDisruption(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	r := newRingWith(128, nodes...)

	const numKeys = 10000
	primaryBefore := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		primaryBefore[i] = r.PreferenceList(fmt.Sprintf("key-%d", i), 1)[0]
	}

	removed := "c"
	r.Remove(removed)

	moved := 0
	movedOwnedByRemoved := 0
	for i := 0; i < numKeys; i++ {
		after := r.PreferenceList(fmt.Sprintf("key-%d", i), 1)[0]
		if after != primaryBefore[i] {
			moved++
			if primaryBefore[i] == removed {
				movedOwnedByRemoved++
			}
		}
	}

	// Every key that moved must previously have been owned by the removed node.
	if moved != movedOwnedByRemoved {
		t.Fatalf("keys not owned by %q were reassigned: %d moved, %d were on %q",
			removed, moved, movedOwnedByRemoved, removed)
	}

	// Disruption should be far below a naive modulo scheme, which would remap
	// roughly (n-1)/n of all keys. With consistent hashing only ~1/n move.
	naiveModuloMoved := float64(numKeys) * float64(len(nodes)-1) / float64(len(nodes))
	if float64(moved) >= naiveModuloMoved {
		t.Fatalf("disruption too high: %d keys moved, naive modulo baseline ~%.0f",
			moved, naiveModuloMoved)
	}
	// Sanity: some keys did move (the ones that were on the removed node).
	if moved == 0 {
		t.Fatalf("expected keys previously on %q to move, none did", removed)
	}
}
