package simulation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"replication-strategies/internal/conflict"
	"replication-strategies/internal/events"
	"replication-strategies/internal/failure"
	"replication-strategies/internal/metrics"
	"replication-strategies/internal/node"
	"replication-strategies/internal/quorum"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// ClusterConfig holds parameters for creating a new cluster.
type ClusterConfig struct {
	Strategy         node.ReplicationStrategy `json:"strategy"`
	NodeCount        int                      `json:"node_count"`
	ReplicationMode  node.ReplicationMode     `json:"replication_mode,omitempty"`  // single-leader
	ConflictResolver conflict.ResolverType    `json:"conflict_resolver,omitempty"` // multi-leader
	QuorumN          int                      `json:"quorum_n,omitempty"`
	QuorumW          int                      `json:"quorum_w,omitempty"`
	QuorumR          int                      `json:"quorum_r,omitempty"`
	// Geo-replication: split nodes across N regions with inter-region latency.
	Regions              int `json:"regions,omitempty"`
	InterRegionLatencyMs int `json:"inter_region_latency_ms,omitempty"`
}

// Cluster holds all runtime state for one simulated cluster.
type Cluster struct {
	mu          sync.RWMutex
	ID          string                   `json:"id"`
	Config      ClusterConfig            `json:"config"`
	Nodes       map[string]node.Node     `json:"-"`
	NodeIDs     []string                 `json:"node_ids"`
	LeaderID    string                   `json:"leader_id,omitempty"`
	Fabric      *transport.NetworkFabric `json:"-"`
	Metrics     *metrics.ClusterMetrics  `json:"-"`
	detector    *failure.Detector        `json:"-"` // phi-accrual failure detector
	NodeRegions map[string]int           `json:"-"` // nodeID -> region index (geo)
	ctx         context.Context
	cancel      context.CancelFunc
	created     time.Time
}

// Mu exposes the cluster mutex for external packages that need to lock it.
func (c *Cluster) Mu() *sync.RWMutex {
	return &c.mu
}

// GetNode returns a node by ID with a read lock held.
func (c *Cluster) GetNode(id string) (node.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.Nodes[id]
	return n, ok
}

// ClusterState is a JSON-serialisable snapshot of a Cluster.
type ClusterState struct {
	ID         string                          `json:"id"`
	Config     ClusterConfig                   `json:"config"`
	NodeIDs    []string                        `json:"node_ids"`
	LeaderID   string                          `json:"leader_id,omitempty"`
	Nodes      map[string]node.NodeStatus      `json:"nodes"`
	Metrics    metrics.ClusterSnapshot         `json:"metrics"`
	Created    time.Time                       `json:"created"`
	Partitions map[string]*transport.Partition `json:"partitions"`
	// DroppedMessages counts back-pressure drops (full queues) — otherwise invisible.
	DroppedMessages uint64         `json:"dropped_messages"`
	NodeRegions     map[string]int `json:"node_regions,omitempty"`
}

// GetState takes a consistent snapshot of the cluster.
func (c *Cluster) GetState() ClusterState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make(map[string]node.NodeStatus, len(c.Nodes))
	for id, n := range c.Nodes {
		nodes[id] = n.GetState()
	}
	return ClusterState{
		ID:              c.ID,
		Config:          c.Config,
		NodeIDs:         append([]string{}, c.NodeIDs...),
		LeaderID:        c.LeaderID,
		Nodes:           nodes,
		Metrics:         c.Metrics.Snapshot(),
		Created:         c.created,
		Partitions:      c.Fabric.GetPartitions(),
		DroppedMessages: c.Fabric.Dropped(),
		NodeRegions:     c.NodeRegions,
	}
}

// Orchestrator manages the lifecycle of one or more simulated clusters.
type Orchestrator struct {
	mu          sync.RWMutex
	clusters    map[string]*Cluster
	bus         *events.EventBus
	maxClusters int // 0 = unlimited
}

// NewOrchestrator creates a new Orchestrator backed by the given EventBus.
func NewOrchestrator(bus *events.EventBus) *Orchestrator {
	return &Orchestrator{
		clusters: make(map[string]*Cluster),
		bus:      bus,
	}
}

// SetMaxClusters bounds the number of concurrently live clusters (0 = unlimited).
// This enforces the config.yaml `max_clusters` limit and prevents unbounded
// goroutine/memory growth from repeated cluster creation.
func (o *Orchestrator) SetMaxClusters(n int) {
	o.mu.Lock()
	o.maxClusters = n
	o.mu.Unlock()
}

// CreateCluster provisions a new cluster according to cfg.
func (o *Orchestrator) CreateCluster(cfg ClusterConfig) (*Cluster, error) {
	if cfg.NodeCount < 1 {
		cfg.NodeCount = 3
	}
	if cfg.ReplicationMode == "" {
		cfg.ReplicationMode = node.ModeAsync
	}

	// Fast-fail before building/starting nodes when already at the cluster cap.
	o.mu.RLock()
	atCap := o.maxClusters > 0 && len(o.clusters) >= o.maxClusters
	max := o.maxClusters
	o.mu.RUnlock()
	if atCap {
		return nil, fmt.Errorf("cluster limit reached (max %d)", max)
	}

	clusterID := uuid.New().String()
	fabric := transport.NewNetworkFabric()
	clusterMetrics := metrics.NewClusterMetrics(clusterID, string(cfg.Strategy))
	ctx, cancel := context.WithCancel(context.Background())

	cluster := &Cluster{
		ID:       clusterID,
		Config:   cfg,
		Nodes:    make(map[string]node.Node),
		NodeIDs:  make([]string, 0, cfg.NodeCount),
		Fabric:   fabric,
		Metrics:  clusterMetrics,
		detector: failure.NewDetector(),
		ctx:      ctx,
		cancel:   cancel,
		created:  time.Now(),
	}

	// Create nodes based on strategy.
	switch cfg.Strategy {
	case node.StrategySingleLeader:
		if err := o.createSingleLeaderCluster(cluster, cfg); err != nil {
			cancel()
			return nil, err
		}
	case node.StrategyMultiLeader:
		if err := o.createMultiLeaderCluster(cluster, cfg); err != nil {
			cancel()
			return nil, err
		}
	case node.StrategyLeaderless:
		if err := o.createLeaderlessCluster(cluster, cfg); err != nil {
			cancel()
			return nil, err
		}
	default:
		cancel()
		return nil, fmt.Errorf("unknown strategy: %s", cfg.Strategy)
	}

	// Assign nodes to regions and apply inter-region latency before traffic starts.
	o.assignRegions(cluster, cfg)

	// Start all nodes.
	for _, n := range cluster.Nodes {
		n.Start(ctx)
	}
	// Feed the phi-accrual detector with heartbeats from online nodes.
	go o.runHeartbeats(cluster)

	o.mu.Lock()
	if o.maxClusters > 0 && len(o.clusters) >= o.maxClusters {
		o.mu.Unlock()
		cancel()
		for _, n := range cluster.Nodes {
			n.Stop()
		}
		return nil, fmt.Errorf("cluster limit reached (max %d)", o.maxClusters)
	}
	o.clusters[clusterID] = cluster
	o.mu.Unlock()

	o.bus.Publish(events.Event{
		Type:      events.EvtNodeStateChanged,
		ClusterID: clusterID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"action": "cluster_created",
			"config": cfg,
		},
	})

	return cluster, nil
}

func (o *Orchestrator) createSingleLeaderCluster(cluster *Cluster, cfg ClusterConfig) error {
	nodeIDs := make([]string, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%s-%d", cluster.ID[:8], i+1)
	}

	leaderID := nodeIDs[0]
	cluster.LeaderID = leaderID

	// Create leader.
	leader := node.NewSingleLeaderNode(leaderID, cluster.ID, cluster.Fabric, o.bus, cfg.ReplicationMode)
	cluster.Nodes[leaderID] = leader
	cluster.NodeIDs = append(cluster.NodeIDs, leaderID)
	cluster.Metrics.AddNode(leader.GetMetrics())

	// Create followers.
	for i := 1; i < cfg.NodeCount; i++ {
		followerID := nodeIDs[i]
		follower := node.NewFollowerNode(followerID, cluster.ID, leaderID, cluster.Fabric, o.bus)
		cluster.Nodes[followerID] = follower
		cluster.NodeIDs = append(cluster.NodeIDs, followerID)
		cluster.Metrics.AddNode(follower.GetMetrics())

		// Leader tracks all followers.
		leader.AddPeer(followerID)
	}

	return nil
}

func (o *Orchestrator) createMultiLeaderCluster(cluster *Cluster, cfg ClusterConfig) error {
	var resolver conflict.ConflictResolver
	switch cfg.ConflictResolver {
	case conflict.ResolverCRDT:
		resolver = conflict.NewCRDTResolver()
	case conflict.ResolverVectorClock:
		resolver = conflict.NewVectorClockResolver(nil)
	default:
		resolver = conflict.NewLWWResolver()
	}

	nodeIDs := make([]string, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%s-%d", cluster.ID[:8], i+1)
	}

	for i, id := range nodeIDs {
		n := node.NewMultiLeaderNode(id, cluster.ID, cluster.Fabric, o.bus, resolver)
		for j, peerID := range nodeIDs {
			if j != i {
				n.AddPeer(peerID)
			}
		}
		cluster.Nodes[id] = n
		cluster.NodeIDs = append(cluster.NodeIDs, id)
		cluster.Metrics.AddNode(n.GetMetrics())
	}

	return nil
}

func (o *Orchestrator) createLeaderlessCluster(cluster *Cluster, cfg ClusterConfig) error {
	N := cfg.NodeCount
	if N == 0 {
		N = 5
	}
	W := cfg.QuorumW
	R := cfg.QuorumR
	if W == 0 {
		W = N/2 + 1
	}
	if R == 0 {
		R = N/2 + 1
	}
	qConfig := quorum.QuorumConfig{N: N, W: W, R: R}

	nodeIDs := make([]string, N)
	for i := 0; i < N; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%s-%d", cluster.ID[:8], i+1)
	}

	for _, id := range nodeIDs {
		n := node.NewLeaderlessNode(id, cluster.ID, cluster.Fabric, o.bus, qConfig)
		n.SetAllNodes(nodeIDs)
		cluster.Nodes[id] = n
		cluster.NodeIDs = append(cluster.NodeIDs, id)
		cluster.Metrics.AddNode(n.GetMetrics())
	}

	return nil
}

// GetCluster returns the cluster with the given ID, or an error if not found.
func (o *Orchestrator) GetCluster(id string) (*Cluster, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	c, ok := o.clusters[id]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", id)
	}
	return c, nil
}

// DeleteCluster stops and removes the cluster with the given ID.
func (o *Orchestrator) DeleteCluster(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	c, ok := o.clusters[id]
	if !ok {
		return fmt.Errorf("cluster %s not found", id)
	}
	c.cancel()
	for _, n := range c.Nodes {
		n.Stop()
	}
	c.Fabric.Close() // stop link-worker goroutines to avoid leaks
	delete(o.clusters, id)
	return nil
}

// ListClusters returns all active clusters.
func (o *Orchestrator) ListClusters() []*Cluster {
	o.mu.RLock()
	defer o.mu.RUnlock()
	result := make([]*Cluster, 0, len(o.clusters))
	for _, c := range o.clusters {
		result = append(result, c)
	}
	return result
}

// AddNode adds a new node to an existing cluster.
func (o *Orchestrator) AddNode(clusterID string) (node.Node, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	newID := fmt.Sprintf("node-%s-%d", clusterID[:8], len(c.NodeIDs)+1)

	switch c.Config.Strategy {
	case node.StrategySingleLeader:
		n := node.NewFollowerNode(newID, clusterID, c.LeaderID, c.Fabric, o.bus)
		c.Nodes[newID] = n
		c.NodeIDs = append(c.NodeIDs, newID)
		c.Metrics.AddNode(n.GetMetrics())
		if leader, ok := c.Nodes[c.LeaderID]; ok {
			leader.AddPeer(newID)
		}
		n.Start(c.ctx)
		return n, nil

	case node.StrategyMultiLeader:
		resolver := conflict.NewLWWResolver()
		n := node.NewMultiLeaderNode(newID, clusterID, c.Fabric, o.bus, resolver)
		for _, peerID := range c.NodeIDs {
			n.AddPeer(peerID)
			if existing, ok := c.Nodes[peerID]; ok {
				existing.AddPeer(newID)
			}
		}
		c.Nodes[newID] = n
		c.NodeIDs = append(c.NodeIDs, newID)
		c.Metrics.AddNode(n.GetMetrics())
		n.Start(c.ctx)
		return n, nil

	case node.StrategyLeaderless:
		qConfig := quorum.QuorumConfig{
			N: c.Config.NodeCount + 1,
			W: c.Config.QuorumW,
			R: c.Config.QuorumR,
		}
		if qConfig.W == 0 {
			qConfig.W = qConfig.N/2 + 1
		}
		if qConfig.R == 0 {
			qConfig.R = qConfig.N/2 + 1
		}
		n := node.NewLeaderlessNode(newID, clusterID, c.Fabric, o.bus, qConfig)
		allNodes := append(append([]string{}, c.NodeIDs...), newID)
		n.SetAllNodes(allNodes)
		// Every existing leaderless node must learn the new membership AND the new
		// quorum config; otherwise the cluster disagrees on N/W/R after a resize.
		for _, existing := range c.Nodes {
			if ll, ok := existing.(*node.LeaderlessNode); ok {
				ll.SetAllNodes(allNodes)
				ll.UpdateQuorum(qConfig)
			}
		}
		c.Nodes[newID] = n
		c.NodeIDs = append(c.NodeIDs, newID)
		c.Config.NodeCount++ // keep NodeCount in sync so subsequent AddNode calls are correct
		c.Config.QuorumW = qConfig.W
		c.Config.QuorumR = qConfig.R
		c.Metrics.AddNode(n.GetMetrics())
		n.Start(c.ctx)
		return n, nil
	}

	return nil, fmt.Errorf("unsupported strategy: %s", c.Config.Strategy)
}

// RemoveNode stops and deregisters a node from its cluster.
func (o *Orchestrator) RemoveNode(clusterID, nodeID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.Nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found in cluster %s", nodeID, clusterID)
	}
	n.Stop()
	c.Fabric.Deregister(nodeID)
	delete(c.Nodes, nodeID)

	newIDs := make([]string, 0, len(c.NodeIDs)-1)
	for _, id := range c.NodeIDs {
		if id != nodeID {
			newIDs = append(newIDs, id)
		}
	}
	c.NodeIDs = newIDs
	if c.Config.NodeCount > 0 {
		c.Config.NodeCount--
	}

	for _, peer := range c.Nodes {
		peer.RemovePeer(nodeID)
	}
	return nil
}

// PauseNode pauses a node so it stops processing messages.
func (o *Orchestrator) PauseNode(clusterID, nodeID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	n, ok := c.Nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	n.Pause()
	return nil
}

// ResumeNode resumes a previously paused node.
func (o *Orchestrator) ResumeNode(clusterID, nodeID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	n, ok := c.Nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	n.Resume()
	return nil
}

// SetClockSkew injects a physical-clock offset (ms) on a node so LWW behavior under
// clock skew can be demonstrated. The hybrid logical clock keeps causality intact.
func (o *Orchestrator) SetClockSkew(clusterID, nodeID string, ms int64) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	n, ok := c.Nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	n.SetClockSkewMillis(ms)
	return nil
}

// InjectPartition creates a network partition between two groups of nodes.
// Returns the partition ID which can be used to heal it later.
func (o *Orchestrator) InjectPartition(clusterID string, groupA, groupB []string) (string, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return "", err
	}
	partID := uuid.New().String()
	p := &transport.Partition{
		ID:     partID,
		GroupA: make(map[string]bool),
		GroupB: make(map[string]bool),
	}
	for _, id := range groupA {
		p.GroupA[id] = true
	}
	for _, id := range groupB {
		p.GroupB[id] = true
	}
	c.Fabric.AddPartition(p)
	o.bus.Publish(events.Event{
		Type:      events.EvtPartitionCreated,
		ClusterID: clusterID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"partition_id": partID,
			"group_a":      groupA,
			"group_b":      groupB,
		},
	})
	return partID, nil
}

// HealPartition removes the named network partition.
func (o *Orchestrator) HealPartition(clusterID, partID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.Fabric.RemovePartition(partID)
	o.bus.Publish(events.Event{
		Type:      events.EvtPartitionHealed,
		ClusterID: clusterID,
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"partition_id": partID},
	})
	return nil
}

// SetLatency injects one-way artificial latency (ms) on the from→to link.
func (o *Orchestrator) SetLatency(clusterID, from, to string, ms int) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.Fabric.SetLatency(from, to, ms)
	return nil
}

// SetDropRate sets the packet-drop probability on the from→to link.
func (o *Orchestrator) SetDropRate(clusterID, from, to string, rate float64) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.Fabric.SetDropRate(from, to, rate)
	return nil
}

// ClearNetworkFaults removes all latency, drop-rate, and partition overrides.
func (o *Orchestrator) ClearNetworkFaults(clusterID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.Fabric.ClearFaults()
	return nil
}

// WriteResult is returned by Write operations.
type WriteResult struct {
	Entry  interface{} `json:"entry"`
	NodeID string      `json:"node_id"`
}

// ReadResult is returned by Read operations.
type ReadResult struct {
	Entry  interface{} `json:"entry"`
	NodeID string      `json:"node_id"`
}

// Write sends a write to the specified node (or the leader if nodeID is empty).
func (o *Orchestrator) Write(clusterID, nodeID, key string, value []byte, clientID string) (*WriteResult, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	var targetNode node.Node
	if nodeID != "" {
		targetNode = c.Nodes[nodeID]
	} else {
		if c.LeaderID != "" {
			targetNode = c.Nodes[c.LeaderID]
		} else if len(c.NodeIDs) > 0 {
			targetNode = c.Nodes[c.NodeIDs[0]]
		}
	}
	c.mu.RUnlock()

	if targetNode == nil {
		return nil, fmt.Errorf("no target node available")
	}

	entry, err := targetNode.Write(key, value, clientID)
	if err != nil {
		return nil, err
	}

	c.Metrics.IncrWrites()

	return &WriteResult{Entry: entry, NodeID: targetNode.ID()}, nil
}

// WriteBatchAtomic performs an all-or-nothing multi-key write on a single-leader
// cluster (returns an error for other strategies).
func (o *Orchestrator) WriteBatchAtomic(clusterID, nodeID string, pairs []node.KV, clientID string) ([]*storage.KVEntry, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	target := nodeID
	if target == "" {
		target = c.LeaderID
	}
	n := c.Nodes[target]
	c.mu.RUnlock()
	sl, ok := n.(*node.SingleLeaderNode)
	if !ok {
		return nil, fmt.Errorf("atomic batch requires a single-leader cluster")
	}
	entries, err := sl.WriteBatch(pairs, clientID)
	if err != nil {
		return entries, err
	}
	c.Metrics.IncrWrites()
	return entries, nil
}

// Delete sends a delete to the specified node (or the leader if nodeID is empty).
func (o *Orchestrator) Delete(clusterID, nodeID, key, clientID string) error {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	var targetNode node.Node
	if nodeID != "" {
		targetNode = c.Nodes[nodeID]
	} else if c.LeaderID != "" {
		targetNode = c.Nodes[c.LeaderID]
	} else if len(c.NodeIDs) > 0 {
		targetNode = c.Nodes[c.NodeIDs[0]]
	}
	c.mu.RUnlock()

	if targetNode == nil {
		return fmt.Errorf("no target node available")
	}
	if err := targetNode.Delete(key, clientID); err != nil {
		return err
	}
	c.Metrics.IncrWrites()
	return nil
}

// Read sends a read to the specified node (or the leader if nodeID is empty).
func (o *Orchestrator) Read(clusterID, nodeID, key, clientID string) (*ReadResult, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	var targetNode node.Node
	if nodeID != "" {
		targetNode = c.Nodes[nodeID]
	} else {
		if c.LeaderID != "" {
			targetNode = c.Nodes[c.LeaderID]
		} else if len(c.NodeIDs) > 0 {
			targetNode = c.Nodes[c.NodeIDs[0]]
		}
	}
	c.mu.RUnlock()

	if targetNode == nil {
		return nil, fmt.Errorf("no target node available")
	}

	entry, err := targetNode.Read(key, clientID)
	if err != nil {
		return nil, err
	}

	c.Metrics.IncrReads()

	return &ReadResult{Entry: entry, NodeID: targetNode.ID()}, nil
}
