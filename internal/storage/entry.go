package storage

import (
	"encoding/json"
	"fmt"
)

type OpType uint8

const (
	OpSet    OpType = 1
	OpDelete OpType = 2
)

type VectorClock map[string]uint64

func NewVectorClock() VectorClock {
	return make(VectorClock)
}

// Increment mutates the receiver in place (maps are reference types) and returns it
// for chaining. Callers that share a clock across entries must Clone() first.
func (vc VectorClock) Increment(nodeID string) VectorClock {
	vc[nodeID]++
	return vc
}

// Merge mutates the receiver in place, folding in the maximum of each entry, and
// returns it. Clone() the receiver first if the original must be preserved.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	for nodeID, ts := range other {
		if ts > vc[nodeID] {
			vc[nodeID] = ts
		}
	}
	return vc
}

func (vc VectorClock) HappensBefore(other VectorClock) bool {
	dominated := false
	for nodeID, ts := range vc {
		if ts > other[nodeID] {
			return false
		}
		if ts < other[nodeID] {
			dominated = true
		}
	}
	for nodeID, ts := range other {
		if _, exists := vc[nodeID]; !exists && ts > 0 {
			dominated = true
		}
	}
	return dominated
}

func (vc VectorClock) Concurrent(other VectorClock) bool {
	return !vc.HappensBefore(other) && !other.HappensBefore(vc) && !vc.Equal(other)
}

// Dominates reports whether vc is causally at or after other: vc[n] >= other[n] for
// every node n. Equivalent to "vc includes all of other's causal history". Used by
// the consistency guarantees, which are globally comparable across nodes (unlike the
// per-store Version counter).
func (vc VectorClock) Dominates(other VectorClock) bool {
	for nodeID, ts := range other {
		if vc[nodeID] < ts {
			return false
		}
	}
	return true
}

func (vc VectorClock) Equal(other VectorClock) bool {
	if len(vc) != len(other) {
		return false
	}
	for nodeID, ts := range vc {
		if other[nodeID] != ts {
			return false
		}
	}
	return true
}

func (vc VectorClock) Clone() VectorClock {
	clone := make(VectorClock, len(vc))
	for k, v := range vc {
		clone[k] = v
	}
	return clone
}

func (vc VectorClock) String() string {
	b, _ := json.Marshal(map[string]uint64(vc))
	return string(b)
}

// LogEntry is used in Single-Leader replication log
type LogEntry struct {
	Index     uint64      `json:"index"`
	Term      uint64      `json:"term"`
	Key       string      `json:"key"`
	Value     []byte      `json:"value"`
	Op        OpType      `json:"op"`
	Timestamp int64       `json:"timestamp"` // UnixNano
	OriginID  string      `json:"origin_id"`
	VClock    VectorClock `json:"vclock"`
}

func (e LogEntry) String() string {
	return fmt.Sprintf("LogEntry{idx=%d, key=%s, origin=%s}", e.Index, e.Key, e.OriginID)
}

// KVEntry is the stored value in the KV store
type KVEntry struct {
	Key       string      `json:"key"`
	Value     []byte      `json:"value"`
	VClock    VectorClock `json:"vclock"`
	Timestamp int64       `json:"timestamp"` // UnixNano
	NodeID    string      `json:"node_id"`
	Tombstone bool        `json:"tombstone,omitempty"`
	Version   uint64      `json:"version"`
}
