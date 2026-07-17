package simulation

import (
	"fmt"
	"time"

	"replication-strategies/internal/hashring"
	"replication-strategies/internal/node"
)

// DefaultPhiThreshold is the suspicion level above which a node is treated as failed.
const DefaultPhiThreshold = 8.0

// runHeartbeats records a heartbeat for every ONLINE node on a fixed cadence, so the
// phi-accrual detector's suspicion rises for paused/removed/partitioned nodes.
func (o *Orchestrator) runHeartbeats(c *Cluster) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			c.mu.RLock()
			nodes := make(map[string]node.Node, len(c.Nodes))
			for id, n := range c.Nodes {
				nodes[id] = n
			}
			c.mu.RUnlock()
			for id, n := range nodes {
				if n.GetState().State == node.StateOnline {
					c.detector.Heartbeat(id, now)
				}
			}
		}
	}
}

// PendingConflict is a parked multi-leader conflict awaiting manual resolution.
type PendingConflict struct {
	NodeID    string `json:"node_id"`
	Key       string `json:"key"`
	LocalVal  string `json:"local_value"`
	RemoteVal string `json:"remote_value"`
}

// ListConflicts aggregates parked (manual) conflicts across all multi-leader nodes.
func (o *Orchestrator) ListConflicts(clusterID string) ([]PendingConflict, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	nodes := make(map[string]node.Node, len(c.Nodes))
	for id, n := range c.Nodes {
		nodes[id] = n
	}
	c.mu.RUnlock()
	var out []PendingConflict
	for id, n := range nodes {
		ml, ok := n.(*node.MultiLeaderNode)
		if !ok {
			continue
		}
		for _, cf := range ml.PendingConflicts() {
			out = append(out, PendingConflict{
				NodeID: id, Key: cf.Key,
				LocalVal:  string(cf.Local.Value),
				RemoteVal: string(cf.Remote.Value),
			})
		}
	}
	return out, nil
}

// ResolveConflict applies a manual conflict choice on a specific node.
func (o *Orchestrator) ResolveConflict(clusterID, nodeID, key, choice string) error {
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
	ml, ok := n.(*node.MultiLeaderNode)
	if !ok {
		return fmt.Errorf("manual conflict resolution requires a multi-leader node")
	}
	return ml.ResolveConflict(key, choice)
}

// assignRegions distributes nodes round-robin across cfg.Regions and applies
// inter-region latency on the fabric so geo-replication tradeoffs are visible.
func (o *Orchestrator) assignRegions(c *Cluster, cfg ClusterConfig) {
	c.NodeRegions = make(map[string]int, len(c.NodeIDs))
	if cfg.Regions <= 1 {
		for _, id := range c.NodeIDs {
			c.NodeRegions[id] = 0
		}
		return
	}
	lat := cfg.InterRegionLatencyMs
	if lat <= 0 {
		lat = 80
	}
	for i, id := range c.NodeIDs {
		c.NodeRegions[id] = i % cfg.Regions
	}
	for _, a := range c.NodeIDs {
		for _, b := range c.NodeIDs {
			if a != b && c.NodeRegions[a] != c.NodeRegions[b] {
				c.Fabric.SetLatency(a, b, lat)
			}
		}
	}
}

// NodeSuspicion is a node's phi-accrual suspicion level.
type NodeSuspicion struct {
	Phi       float64 `json:"phi"`
	Suspected bool    `json:"suspected"`
}

// Suspicion returns the phi-accrual suspicion for every node in the cluster.
func (o *Orchestrator) Suspicion(clusterID string, threshold float64) (map[string]NodeSuspicion, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	if threshold <= 0 {
		threshold = DefaultPhiThreshold
	}
	now := time.Now().UnixMilli()
	c.mu.RLock()
	ids := append([]string{}, c.NodeIDs...)
	c.mu.RUnlock()

	out := make(map[string]NodeSuspicion, len(ids))
	for _, id := range ids {
		out[id] = NodeSuspicion{
			Phi:       c.detector.Phi(id, now),
			Suspected: c.detector.Suspected(id, now, threshold),
		}
	}
	return out, nil
}

// Placement returns the consistent-hashing preference list (the up-to-n replicas that
// own a key) for the cluster's current membership.
func (o *Orchestrator) Placement(clusterID, key string, n int) ([]string, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}
	if n <= 0 {
		n = 3
	}
	ring := hashring.NewRing(128)
	c.mu.RLock()
	for _, id := range c.NodeIDs {
		ring.Add(id)
	}
	c.mu.RUnlock()
	return ring.PreferenceList(key, n), nil
}
