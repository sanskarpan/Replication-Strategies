package conflict

import "testing"

// makeRGA builds an RGA from a slice of elements.
func makeRGA(elems ...RGAElem) *RGA {
	return &RGA{Type: RGAType, Elems: elems}
}

// TestRGAConcurrentInsertConverges checks that two replicas concurrently inserting
// distinct elements after head converge to the same deterministic order regardless
// of merge direction.
func TestRGAConcurrentInsertConverges(t *testing.T) {
	a := makeRGA(RGAElem{ID: "n1:1", Value: "A", After: ""})
	b := makeRGA(RGAElem{ID: "n2:1", Value: "B", After: ""})

	ab := a.Merge(b).Value()
	ba := b.Merge(a).Value()

	if ab != ba {
		t.Fatalf("merge not commutative: Merge(a,b)=%q Merge(b,a)=%q", ab, ba)
	}
	// Both elements must be visible.
	if len(ab) != 2 {
		t.Fatalf("expected both elements visible, got %q", ab)
	}
	// Higher ID (n2:1 = "B") wins the concurrent tiebreak after head.
	if ab != "BA" {
		t.Fatalf("expected deterministic order %q, got %q", "BA", ab)
	}
}

// TestRGADeleteTombstone checks that a delete tombstone on one replica removes the
// element after merge, with no resurrection from the other replica's live copy.
func TestRGADeleteTombstone(t *testing.T) {
	live := makeRGA(
		RGAElem{ID: "n1:1", Value: "A", After: ""},
		RGAElem{ID: "n1:2", Value: "B", After: "n1:1"},
	)
	deleted := makeRGA(
		RGAElem{ID: "n1:1", Value: "A", After: ""},
		RGAElem{ID: "n1:2", Value: "B", After: "n1:1", Deleted: true},
	)

	if got := live.Merge(deleted).Value(); got != "A" {
		t.Fatalf("expected tombstone to remove B, got %q", got)
	}
	if got := deleted.Merge(live).Value(); got != "A" {
		t.Fatalf("expected no resurrection (other direction), got %q", got)
	}
}

// TestRGAIdempotent checks that merging a replica with itself is a no-op.
func TestRGAIdempotent(t *testing.T) {
	a := makeRGA(
		RGAElem{ID: "n1:1", Value: "A", After: ""},
		RGAElem{ID: "n2:1", Value: "B", After: ""},
		RGAElem{ID: "n1:2", Value: "C", After: "n1:1", Deleted: true},
	)
	if got, want := a.Merge(a).Value(), a.Value(); got != want {
		t.Fatalf("merge not idempotent: Merge(a,a)=%q a=%q", got, want)
	}
}

// TestRGAAssociative checks associativity across three replicas.
func TestRGAAssociative(t *testing.T) {
	a := makeRGA(RGAElem{ID: "n1:1", Value: "A", After: ""})
	b := makeRGA(RGAElem{ID: "n2:1", Value: "B", After: ""})
	c := makeRGA(RGAElem{ID: "n3:1", Value: "C", After: ""})

	left := a.Merge(b).Merge(c).Value()
	right := a.Merge(b.Merge(c)).Value()
	if left != right {
		t.Fatalf("merge not associative: %q vs %q", left, right)
	}
}
