package node

import (
	"context"

	"replication-strategies/internal/metrics"
	"replication-strategies/internal/replication"
	"replication-strategies/internal/storage"
)

// KV is a single key/value pair used in atomic multi-key batches.
type KV struct {
	Key   string
	Value []byte
}

type ReplicationStrategy string

const (
	StrategySingleLeader ReplicationStrategy = "single_leader"
	StrategyMultiLeader  ReplicationStrategy = "multi_leader"
	StrategyLeaderless   ReplicationStrategy = "leaderless"
)

type NodeRole string

const (
	RoleLeader   NodeRole = "leader"
	RoleFollower NodeRole = "follower"
	RoleReplica  NodeRole = "replica" // for leaderless
	RolePrimary  NodeRole = "primary" // for multi-leader (all primaries)
)

type NodeState string

const (
	StateOnline  NodeState = "online"
	StateOffline NodeState = "offline"
	StatePaused  NodeState = "paused"
)

type NodeStatus struct {
	ID          string              `json:"id"`
	ClusterID   string              `json:"cluster_id"`
	Strategy    ReplicationStrategy `json:"strategy"`
	Role        NodeRole            `json:"role"`
	State       NodeState           `json:"state"`
	CommitIndex uint64              `json:"commit_index"`
	LastApplied uint64              `json:"last_applied"`
	LeaderID    string              `json:"leader_id,omitempty"`
	Peers       []string            `json:"peers"`
	Lag         int64               `json:"lag"`
}

type Node interface {
	ID() string
	Strategy() ReplicationStrategy
	Role() NodeRole
	ClusterID() string

	Start(ctx context.Context)
	Stop()
	Pause()
	Resume()

	Write(key string, value []byte, clientID string) (*storage.KVEntry, error)
	Read(key string, clientID string) (*storage.KVEntry, error)
	Delete(key string, clientID string) error
	SetClockSkewMillis(ms int64)

	GetState() NodeStatus
	GetLog() *replication.ReplicationLog
	GetStore() *storage.Store
	GetMetrics() *metrics.NodeMetrics
	GetLag() int64

	AddPeer(nodeID string)
	RemovePeer(nodeID string)
	GetPeers() []string

	// Message inbox — the fabric sends messages here
	Inbox() chan interface{}
	HandleMessage(msg interface{})
}
