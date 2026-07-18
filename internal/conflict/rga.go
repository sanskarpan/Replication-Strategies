package conflict

import "sort"

// RGA is a Replicated Growable Array: a sequence CRDT. Each element records the ID
// of the element it was inserted after, so the visible order is recovered by walking
// the After links. It is self-describing via the crdt_type tag.

const RGAType = "rga"

// RGAElem is a single element in the sequence.
type RGAElem struct {
	ID      string `json:"id"`      // unique: nodeID + ":" + counter
	Value   string `json:"value"`   // payload
	After   string `json:"after"`   // ID of the element it was inserted after; "" = head
	Deleted bool   `json:"deleted"` // tombstone
}

// RGA is the state-based replicated growable array.
type RGA struct {
	Type  string    `json:"crdt_type"`
	Elems []RGAElem `json:"elems"`
}

// order returns the elements topologically ordered by the After links (head-first),
// breaking concurrent-insert ties deterministically by element ID (higher ID first).
func (a *RGA) order() []RGAElem {
	// Group children by their After anchor.
	children := map[string][]RGAElem{}
	for _, e := range a.Elems {
		children[e.After] = append(children[e.After], e)
	}
	// Sort each sibling group by ID descending (higher ID first) — the standard RGA
	// tiebreak for concurrent inserts after the same anchor.
	for k := range children {
		sort.Slice(children[k], func(i, j int) bool {
			return children[k][i].ID > children[k][j].ID
		})
	}

	// Depth-first walk from head, emitting each element then recursing into the
	// elements inserted after it. Guard against cycles / missing anchors.
	var out []RGAElem
	visited := map[string]bool{}
	var walk func(anchor string)
	walk = func(anchor string) {
		for _, e := range children[anchor] {
			if visited[e.ID] {
				continue
			}
			visited[e.ID] = true
			out = append(out, e)
			walk(e.ID)
		}
	}
	walk("")

	// Any elements whose After anchor is not reachable from head (dangling) are
	// appended in a deterministic order so replicas still converge.
	if len(out) != len(a.Elems) {
		var rest []RGAElem
		for _, e := range a.Elems {
			if !visited[e.ID] {
				visited[e.ID] = true
				rest = append(rest, e)
			}
		}
		sort.Slice(rest, func(i, j int) bool { return rest[i].ID > rest[j].ID })
		out = append(out, rest...)
	}
	return out
}

// Value returns the visible sequence: ordered elements, skipping tombstones,
// concatenating Value.
func (a *RGA) Value() string {
	var b []byte
	for _, e := range a.order() {
		if e.Deleted {
			continue
		}
		b = append(b, e.Value...)
	}
	return string(b)
}

// Merge unions elements by ID (an element present on either side is kept), OR-ing the
// Deleted flag so a delete on either replica wins (tombstones). The result is a new
// deterministically-ordered RGA. Merge is commutative, associative, and idempotent.
func (a *RGA) Merge(b *RGA) *RGA {
	byID := map[string]RGAElem{}
	for _, src := range [][]RGAElem{a.Elems, b.Elems} {
		for _, e := range src {
			if cur, ok := byID[e.ID]; ok {
				cur.Deleted = cur.Deleted || e.Deleted
				byID[e.ID] = cur
			} else {
				byID[e.ID] = e
			}
		}
	}
	out := &RGA{Type: RGAType, Elems: make([]RGAElem, 0, len(byID))}
	for _, e := range byID {
		out.Elems = append(out.Elems, e)
	}
	// Deterministic storage order (independent of map iteration) so identical merged
	// states serialize identically across replicas.
	sort.Slice(out.Elems, func(i, j int) bool { return out.Elems[i].ID < out.Elems[j].ID })
	return out
}
