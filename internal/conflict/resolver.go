package conflict

import (
    "time"
    "replication-strategies/internal/storage"
)

type ResolverType string

const (
    ResolverLWW         ResolverType = "lww"
    ResolverVectorClock ResolverType = "vector_clock"
    ResolverCRDT        ResolverType = "crdt"
)

type Conflict struct {
    ID         string             `json:"id"`
    Key        string             `json:"key"`
    Local      *storage.KVEntry   `json:"local"`
    Remote     *storage.KVEntry   `json:"remote"`
    DetectedAt time.Time          `json:"detected_at"`
    NodeID     string             `json:"node_id"`
    ClusterID  string             `json:"cluster_id"`
}

type Resolution struct {
    ConflictID  string           `json:"conflict_id"`
    Winner      *storage.KVEntry `json:"winner"`
    ResolverType ResolverType    `json:"resolver_type"`
    Reason      string           `json:"reason"`
    ResolvedAt  time.Time        `json:"resolved_at"`
}

type ConflictResolver interface {
    Type() ResolverType
    Resolve(c *Conflict) *Resolution
}
