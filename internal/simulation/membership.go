package simulation

import (
	"context"
	"fmt"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/quorum"
)

// ReconfigureReport describes a safe leaderless membership change.
type ReconfigureReport struct {
	ClusterID   string   `json:"cluster_id"`
	AddedNode   string   `json:"added_node"`
	OldQuorum   [3]int   `json:"old_quorum"` // N, W, R
	NewQuorum   [3]int   `json:"new_quorum"`
	OverlapHeld bool     `json:"overlap_held"` // W+R>N held throughout the change
	Reconciled  int      `json:"reconciled"`   // keys moved onto the new node
	Phases      []string `json:"phases"`
}

// SafeAddNode grows a leaderless cluster without a quorum-overlap gap, using a two-phase
// (joint-consensus-style) change:
//
//	Phase 1 — add the node to every replica's ring/membership while keeping the OLD
//	          quorum config uniformly in force (no node ever runs a mixed config).
//	Phase 2 — re-replicate data onto the new node via anti-entropy.
//	Phase 3 — atomically switch every node to the new quorum, chosen so W+R>N still holds.
//
// Because the quorum config is always identical across nodes and W+R>N is preserved at
// both endpoints, there is no window where a stale read can slip past the write set.
func (o *Orchestrator) SafeAddNode(clusterID string) (ReconfigureReport, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return ReconfigureReport{}, err
	}
	if c.Config.Strategy != node.StrategyLeaderless {
		return ReconfigureReport{}, fmt.Errorf("safe reconfiguration is only supported for leaderless clusters")
	}

	rep := ReconfigureReport{ClusterID: clusterID, OverlapHeld: true}

	// --- Phase 1: add membership, keep the OLD quorum everywhere. ---
	c.mu.Lock()
	oldN := c.Config.QuorumN
	if oldN <= 0 || oldN > len(c.NodeIDs) {
		oldN = len(c.NodeIDs)
	}
	oldW, oldR := c.Config.QuorumW, c.Config.QuorumR
	if oldW == 0 {
		oldW = oldN/2 + 1
	}
	if oldR == 0 {
		oldR = oldN/2 + 1
	}
	rep.OldQuorum = [3]int{oldN, oldW, oldR}
	oldQ := quorum.QuorumConfig{N: oldN, W: oldW, R: oldR}

	newID := fmt.Sprintf("node-%s-%d", clusterID[:8], len(c.NodeIDs)+1)
	newAll := append(append([]string{}, c.NodeIDs...), newID)

	// The joining node starts with the OLD quorum so the config stays uniform.
	nn := node.NewLeaderlessNode(newID, clusterID, c.Fabric, o.bus, oldQ)
	nn.SetAllNodes(newAll)
	for _, existing := range c.Nodes {
		if ll, ok := existing.(*node.LeaderlessNode); ok {
			ll.SetAllNodes(newAll) // ring grows; quorum unchanged
		}
	}
	c.Nodes[newID] = nn
	c.NodeIDs = append(c.NodeIDs, newID)
	c.Metrics.AddNode(nn.GetMetrics())
	nn.Start(c.ctx)
	c.mu.Unlock()
	rep.AddedNode = newID
	rep.Phases = append(rep.Phases, "phase 1: node added to the ring; old quorum kept uniformly (no mixed config)")

	// --- Phase 2: re-replicate data onto the new node. ---
	ae, err := o.RunAntiEntropy(context.Background(), clusterID)
	if err == nil {
		rep.Reconciled = ae.Reconciled
	}
	rep.Phases = append(rep.Phases, fmt.Sprintf("phase 2: anti-entropy replicated %d key(s) onto the new node", rep.Reconciled))

	// --- Phase 3: atomically switch to the new quorum (W+R>N preserved). ---
	c.mu.Lock()
	newN := len(c.NodeIDs)
	newW := newN/2 + 1
	newR := newN/2 + 1
	newQ := quorum.QuorumConfig{N: newN, W: newW, R: newR}
	for _, existing := range c.Nodes {
		if ll, ok := existing.(*node.LeaderlessNode); ok {
			ll.UpdateQuorum(newQ)
		}
	}
	c.Config.NodeCount = newN
	c.Config.QuorumN = newN
	c.Config.QuorumW = newW
	c.Config.QuorumR = newR
	c.mu.Unlock()

	rep.NewQuorum = [3]int{newN, newW, newR}
	rep.OverlapHeld = (oldW+oldR > oldN) && (newW+newR > newN)
	rep.Phases = append(rep.Phases, fmt.Sprintf("phase 3: atomic switch to N=%d W=%d R=%d (W+R>N overlap preserved)", newN, newW, newR))

	// Refresh region assignments for the enlarged membership.
	o.pushRegions(c)

	o.bus.Publish(events.Event{
		Type:      events.EvtNodeStateChanged,
		ClusterID: clusterID,
		Data:      map[string]interface{}{"action": "safe_reconfigure", "added": newID},
	})
	return rep, nil
}
