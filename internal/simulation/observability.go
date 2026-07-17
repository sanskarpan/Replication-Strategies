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
