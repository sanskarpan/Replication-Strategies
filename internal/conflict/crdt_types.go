package conflict

import "sort"

// Additional state-based (convergent) CRDTs beyond the G-Counter and LWW register.
// Every type is self-describing via the crdt_type tag so the resolver dispatches by
// type instead of guessing from payload shape.

const (
	PNCounterType = "pncounter"
	ORSetType     = "orset"
	LWWMapType    = "lwwmap"
)

// crdtTag peeks only at the crdt_type discriminator of a JSON value.
type crdtTag struct {
	Type string `json:"crdt_type"`
}

// PNCounter is a positive-negative counter: two G-Counters whose difference is the value.
type PNCounter struct {
	Type string            `json:"crdt_type"`
	P    map[string]uint64 `json:"p"`
	N    map[string]uint64 `json:"n"`
}

func (a *PNCounter) Value() int64 {
	var sum int64
	for _, v := range a.P {
		sum += int64(v)
	}
	for _, v := range a.N {
		sum -= int64(v)
	}
	return sum
}

// Merge takes the element-wise max of both P and N maps (join-semilattice).
func (a *PNCounter) Merge(b *PNCounter) *PNCounter {
	out := &PNCounter{Type: PNCounterType, P: map[string]uint64{}, N: map[string]uint64{}}
	for _, src := range []map[string]uint64{a.P, b.P} {
		for k, v := range src {
			if v > out.P[k] {
				out.P[k] = v
			}
		}
	}
	for _, src := range []map[string]uint64{a.N, b.N} {
		for k, v := range src {
			if v > out.N[k] {
				out.N[k] = v
			}
		}
	}
	return out
}

// ORSet is an observed-remove set: an element is present if it carries an add-tag that
// has not been removed. Concurrent add wins over remove (no resurrection anomaly).
type ORSet struct {
	Type    string              `json:"crdt_type"`
	Adds    map[string][]string `json:"adds"`    // element -> unique add tags
	Removes map[string]bool     `json:"removes"` // removed tags
}

// Elements returns the sorted set of currently-present elements.
func (a *ORSet) Elements() []string {
	var out []string
	for el, tags := range a.Adds {
		for _, t := range tags {
			if !a.Removes[t] {
				out = append(out, el)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// Merge unions the add-tag sets per element and unions the remove-tag sets.
func (a *ORSet) Merge(b *ORSet) *ORSet {
	out := &ORSet{Type: ORSetType, Adds: map[string][]string{}, Removes: map[string]bool{}}
	seen := map[string]map[string]bool{}
	add := func(src map[string][]string) {
		for el, tags := range src {
			if seen[el] == nil {
				seen[el] = map[string]bool{}
			}
			for _, t := range tags {
				if !seen[el][t] {
					seen[el][t] = true
					out.Adds[el] = append(out.Adds[el], t)
				}
			}
		}
	}
	add(a.Adds)
	add(b.Adds)
	for _, src := range []map[string]bool{a.Removes, b.Removes} {
		for t := range src {
			out.Removes[t] = true
		}
	}
	for el := range out.Adds {
		sort.Strings(out.Adds[el])
	}
	return out
}

// LWWMap is a map whose entries are each resolved by last-write-wins (ts, then node).
type LWWMap struct {
	Type    string                 `json:"crdt_type"`
	Entries map[string]LWWMapEntry `json:"entries"`
}

type LWWMapEntry struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	NodeID    string `json:"node_id"`
}

// Merge takes, per key, the entry with the higher (timestamp, node, value) — a full
// total order so all replicas converge. Value is the final tiebreak so the merge stays
// commutative/associative even in the degenerate case where two distinct values share
// the same timestamp AND node id (impossible under real HLC+node writes, but a merge
// that isn't a total function isn't a proper CRDT — a property test found this).
func (a *LWWMap) Merge(b *LWWMap) *LWWMap {
	out := &LWWMap{Type: LWWMapType, Entries: map[string]LWWMapEntry{}}
	for k, v := range a.Entries {
		out.Entries[k] = v
	}
	for k, v := range b.Entries {
		cur, ok := out.Entries[k]
		if !ok || lwwMapEntryWins(v, cur) {
			out.Entries[k] = v
		}
	}
	return out
}

// lwwMapEntryWins reports whether x should win over y under the (timestamp, node, value)
// total order.
func lwwMapEntryWins(x, y LWWMapEntry) bool {
	if x.Timestamp != y.Timestamp {
		return x.Timestamp > y.Timestamp
	}
	if x.NodeID != y.NodeID {
		return x.NodeID > y.NodeID
	}
	return x.Value > y.Value
}
