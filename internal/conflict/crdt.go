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
	// Only merge as a GCounter when BOTH values explicitly declare crdt_type=gcounter.
	// Inferring CRDT-ness from payload shape would silently corrupt ordinary JSON.
	var localGC, remoteGC GCounter
	localErr := json.Unmarshal(c.Local.Value, &localGC)
	remoteErr := json.Unmarshal(c.Remote.Value, &remoteGC)

	if localErr == nil && remoteErr == nil &&
		localGC.Type == GCounterType && remoteGC.Type == GCounterType &&
		localGC.Counts != nil && remoteGC.Counts != nil {
		merged := localGC.Merge(&remoteGC)
		mergedBytes, _ := json.Marshal(merged)
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
			Reason:       "gcounter_merge",
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
