package conflict

import (
	"replication-strategies/internal/storage"
	"time"
)

type ResolverType string

const (
	ResolverLWW         ResolverType = "lww"
	ResolverVectorClock ResolverType = "vector_clock"
	ResolverCRDT        ResolverType = "crdt"
	ResolverManual      ResolverType = "manual"
)

// ManualResolver parks concurrent conflicts for a human to resolve (siblings). Nodes
// detect its Type() and, instead of auto-resolving, expose the conflict for a manual
// choice. Its Resolve() defaults to keeping local until a human decides.
type ManualResolver struct{}

func NewManualResolver() *ManualResolver     { return &ManualResolver{} }
func (r *ManualResolver) Type() ResolverType { return ResolverManual }
func (r *ManualResolver) Resolve(c *Conflict) *Resolution {
	return &Resolution{ConflictID: c.ID, Winner: c.Local, ResolverType: ResolverManual, Reason: "pending_manual", ResolvedAt: time.Now()}
}

type Conflict struct {
	ID         string           `json:"id"`
	Key        string           `json:"key"`
	Local      *storage.KVEntry `json:"local"`
	Remote     *storage.KVEntry `json:"remote"`
	DetectedAt time.Time        `json:"detected_at"`
	NodeID     string           `json:"node_id"`
	ClusterID  string           `json:"cluster_id"`
}

type Resolution struct {
	ConflictID   string           `json:"conflict_id"`
	Winner       *storage.KVEntry `json:"winner"`
	ResolverType ResolverType     `json:"resolver_type"`
	Reason       string           `json:"reason"`
	ResolvedAt   time.Time        `json:"resolved_at"`
}

type ConflictResolver interface {
	Type() ResolverType
	Resolve(c *Conflict) *Resolution
}
