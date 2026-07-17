package conflict

import (
	"encoding/json"
	"replication-strategies/internal/storage"
	"time"
)

// LWWRegister is a CRDT Last-Write-Wins Register
type LWWRegister struct {
	Value     []byte `json:"value"`
	Timestamp int64  `json:"timestamp"` // UnixNano
	NodeID    string `json:"node_id"`
}

func (r *LWWRegister) Merge(other *LWWRegister) *LWWRegister {
	if other.Timestamp > r.Timestamp {
		return other
	}
	if other.Timestamp == r.Timestamp && other.NodeID > r.NodeID {
		return other
	}
	return r
}

// GCounterType is the explicit type tag a value must carry to be treated as a
// GCounter. Without it, arbitrary JSON that merely happens to contain a "counts"
// object would be silently CRDT-merged instead of resolved as an opaque value.
const GCounterType = "gcounter"

// GCounter is a grow-only counter CRDT. It is self-describing via the crdt_type tag.
type GCounter struct {
	Type   string            `json:"crdt_type"`
	Counts map[string]uint64 `json:"counts"`
}

func NewGCounter() *GCounter {
	return &GCounter{Type: GCounterType, Counts: make(map[string]uint64)}
}

func (g *GCounter) Increment(nodeID string) {
	g.Counts[nodeID]++
}

func (g *GCounter) Value() uint64 {
	var sum uint64
	for _, v := range g.Counts {
		sum += v
	}
	return sum
}

func (g *GCounter) Merge(other *GCounter) *GCounter {
	result := &GCounter{Type: GCounterType, Counts: make(map[string]uint64)}
	for k, v := range g.Counts {
		result.Counts[k] = v
	}
	for k, v := range other.Counts {
		if v > result.Counts[k] {
			result.Counts[k] = v
		}
	}
	return result
}

// CRDTResolver uses CRDT semantics to resolve conflicts
type CRDTResolver struct{}

func NewCRDTResolver() *CRDTResolver {
	return &CRDTResolver{}
}

func (r *CRDTResolver) Type() ResolverType {
	return ResolverCRDT
}

func (r *CRDTResolver) Resolve(c *Conflict) *Resolution {
	// Dispatch on the explicit crdt_type tag (only when BOTH values agree on a known
	// type). Inferring CRDT-ness from payload shape would silently corrupt ordinary JSON.
	if mergedBytes, reason, ok := mergeCRDT(c.Local.Value, c.Remote.Value); ok {
		winner := &storage.KVEntry{
			Key:       c.Local.Key,
			Value:     mergedBytes,
			VClock:    c.Local.VClock.Clone().Merge(c.Remote.VClock),
			Timestamp: max64(c.Local.Timestamp, c.Remote.Timestamp),
			// Deterministic winner identity: both nodes merging the same pair must
			// produce the same NodeID, else the merged entry's identity oscillates and
			// read-repair churns forever.
			NodeID: maxStr(c.Local.NodeID, c.Remote.NodeID),
		}
		return &Resolution{
			ConflictID:   c.ID,
			Winner:       winner,
			ResolverType: ResolverCRDT,
			Reason:       reason,
			ResolvedAt:   time.Now(),
		}
	}

	// LWW Register semantics
	localReg := &LWWRegister{Value: c.Local.Value, Timestamp: c.Local.Timestamp, NodeID: c.Local.NodeID}
	remoteReg := &LWWRegister{Value: c.Remote.Value, Timestamp: c.Remote.Timestamp, NodeID: c.Remote.NodeID}
	winner := localReg.Merge(remoteReg)
	var winnerEntry *storage.KVEntry
	reason := "lww_register_local"
	if winner == remoteReg {
		winnerEntry = c.Remote
		reason = "lww_register_remote"
	} else {
		winnerEntry = c.Local
	}
	return &Resolution{
		ConflictID:   c.ID,
		Winner:       winnerEntry,
		ResolverType: ResolverCRDT,
		Reason:       reason,
		ResolvedAt:   time.Now(),
	}
}

// mergeCRDT merges two values as a CRDT when both carry the same known crdt_type tag.
// Returns the merged JSON, a reason string, and ok=false when they are not a matching CRDT.
func mergeCRDT(localVal, remoteVal []byte) ([]byte, string, bool) {
	var lt, rt crdtTag
	if json.Unmarshal(localVal, &lt) != nil || json.Unmarshal(remoteVal, &rt) != nil {
		return nil, "", false
	}
	if lt.Type == "" || lt.Type != rt.Type {
		return nil, "", false
	}
	switch lt.Type {
	case GCounterType:
		var a, b GCounter
		if json.Unmarshal(localVal, &a) != nil || json.Unmarshal(remoteVal, &b) != nil || a.Counts == nil || b.Counts == nil {
			return nil, "", false
		}
		out, _ := json.Marshal(a.Merge(&b))
		return out, "gcounter_merge", true
	case PNCounterType:
		var a, b PNCounter
		if json.Unmarshal(localVal, &a) != nil || json.Unmarshal(remoteVal, &b) != nil {
			return nil, "", false
		}
		out, _ := json.Marshal(a.Merge(&b))
		return out, "pncounter_merge", true
	case ORSetType:
		var a, b ORSet
		if json.Unmarshal(localVal, &a) != nil || json.Unmarshal(remoteVal, &b) != nil {
			return nil, "", false
		}
		out, _ := json.Marshal(a.Merge(&b))
		return out, "orset_merge", true
	case LWWMapType:
		var a, b LWWMap
		if json.Unmarshal(localVal, &a) != nil || json.Unmarshal(remoteVal, &b) != nil {
			return nil, "", false
		}
		out, _ := json.Marshal(a.Merge(&b))
		return out, "lwwmap_merge", true
	}
	return nil, "", false
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxStr(a, b string) string {
	if a > b {
		return a
	}
	return b
}
