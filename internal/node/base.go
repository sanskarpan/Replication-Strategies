package node

import (
	"context"
	"sync"
	"time"

	"replication-strategies/internal/events"
	"replication-strategies/internal/metrics"
	"replication-strategies/internal/replication"
	"replication-strategies/internal/storage"
)

type BaseNode struct {
	mu        sync.RWMutex
	id        string
	clusterID string
	strategy  ReplicationStrategy
	role      NodeRole
	state     NodeState
	peers     []string

	store   *storage.Store
	log     *replication.ReplicationLog
	metrics *metrics.NodeMetrics
	bus     *events.EventBus

	ctx    context.Context
	cancel context.CancelFunc

	inbox chan interface{}

	commitIndex uint64
	lastApplied uint64

	leaderID string
}

func newBaseNode(id, clusterID string, strategy ReplicationStrategy, role NodeRole, bus *events.EventBus) *BaseNode {
	ctx, cancel := context.WithCancel(context.Background())
	m := metrics.NewNodeMetrics(id)
	m.IsLeader = (role == RoleLeader || role == RolePrimary)
	return &BaseNode{
		id:        id,
		clusterID: clusterID,
		strategy:  strategy,
		role:      role,
		state:     StateOnline,
		store:     storage.NewStore(),
		log:       replication.NewReplicationLog(),
		metrics:   m,
		bus:       bus,
		ctx:       ctx,
		cancel:    cancel,
		inbox:     make(chan interface{}, 512),
		peers:     make([]string, 0),
	}
}

func (b *BaseNode) ID() string                          { return b.id }
func (b *BaseNode) Strategy() ReplicationStrategy       { return b.strategy }
func (b *BaseNode) ClusterID() string                   { return b.clusterID }
func (b *BaseNode) GetLog() *replication.ReplicationLog { return b.log }
func (b *BaseNode) GetStore() *storage.Store            { return b.store }
func (b *BaseNode) GetMetrics() *metrics.NodeMetrics    { return b.metrics }
func (b *BaseNode) Inbox() chan interface{}              { return b.inbox }

func (b *BaseNode) Role() NodeRole {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.role
}

func (b *BaseNode) setRole(r NodeRole) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.role = r
	b.metrics.SetLeader(r == RoleLeader || r == RolePrimary)
}

func (b *BaseNode) Pause() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StatePaused
	b.metrics.SetOnline(false)
	b.publishStateChange()
}

func (b *BaseNode) Resume() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateOnline
	b.metrics.SetOnline(true)
	b.publishStateChange()
}

func (b *BaseNode) Stop() {
	b.cancel()
	b.mu.Lock()
	b.state = StateOffline
	b.metrics.SetOnline(false)
	b.mu.Unlock()
}

func (b *BaseNode) isPaused() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state == StatePaused || b.state == StateOffline
}

func (b *BaseNode) AddPeer(nodeID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.peers {
		if p == nodeID {
			return
		}
	}
	b.peers = append(b.peers, nodeID)
}

func (b *BaseNode) RemovePeer(nodeID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	peers := b.peers[:0]
	for _, p := range b.peers {
		if p != nodeID {
			peers = append(peers, p)
		}
	}
	b.peers = peers
}

func (b *BaseNode) GetPeers() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]string, len(b.peers))
	copy(result, b.peers)
	return result
}

func (b *BaseNode) GetState() NodeStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return NodeStatus{
		ID:          b.id,
		ClusterID:   b.clusterID,
		Strategy:    b.strategy,
		Role:        b.role,
		State:       b.state,
		CommitIndex: b.commitIndex,
		LastApplied: b.lastApplied,
		LeaderID:    b.leaderID,
		Peers:       append([]string{}, b.peers...),
		Lag:         b.metrics.Lag(),
	}
}

func (b *BaseNode) GetLag() int64 {
	return b.metrics.Lag()
}

func (b *BaseNode) publishStateChange() {
	b.bus.Publish(events.Event{
		Type:      events.EvtNodeStateChanged,
		ClusterID: b.clusterID,
		NodeID:    b.id,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"role":  string(b.role),
			"state": string(b.state),
		},
	})
}

func (b *BaseNode) publishEvent(evtType events.EventType, data map[string]interface{}) {
	b.bus.Publish(events.Event{
		Type:      evtType,
		ClusterID: b.clusterID,
		NodeID:    b.id,
		Timestamp: time.Now(),
		Data:      data,
	})
}
