package simulation

import (
	"encoding/base64"

	"replication-strategies/internal/node"
)

// KeyDivergence records, for one key, the value each online replica holds.
type KeyDivergence struct {
	Key    string            `json:"key"`
	Values map[string]string `json:"values"` // nodeID -> value | "<tombstone>" | "<absent>"
}

// ConvergenceReport is the result of a cluster-wide convergence check.
type ConvergenceReport struct {
	ClusterID string          `json:"cluster_id"`
	Converged bool            `json:"converged"`
	Keys      int             `json:"keys"`
	Diverged  []KeyDivergence `json:"diverged"`
	Note      string          `json:"note,omitempty"`
}

// nodeValueRepr renders a node's view of a key for comparison.
func nodeValueRepr(n node.Node, key string) string {
	raw, ok := n.GetStore().GetRaw(key)
	if !ok {
		return "<absent>"
	}
	if raw.Tombstone {
		return "<tombstone>"
	}
	return base64.StdEncoding.EncodeToString(raw.Value)
}

// CheckConvergence reports whether every ONLINE replica holds identical state for every
// key. Paused/offline nodes are excluded (they legitimately lag). This is the invariant
// that anti-entropy / read-repair must satisfy once the cluster quiesces.
func (c *Cluster) CheckConvergence() ConvergenceReport {
	c.mu.RLock()
	online := make(map[string]node.Node)
	keys := make(map[string]struct{})
	for id, n := range c.Nodes {
		if n.GetState().State != node.StateOnline {
			continue
		}
		online[id] = n
		for _, k := range n.GetStore().Keys() {
			keys[k] = struct{}{}
		}
	}
	clusterID := c.ID
	c.mu.RUnlock()

	report := ConvergenceReport{ClusterID: clusterID, Converged: true, Keys: len(keys)}
	if len(online) < 2 {
		report.Note = "fewer than two online replicas; convergence is trivially satisfied"
		return report
	}

	for k := range keys {
		values := make(map[string]string, len(online))
		var first string
		diverged := false
		i := 0
		for id, n := range online {
			v := nodeValueRepr(n, k)
			values[id] = v
			if i == 0 {
				first = v
			} else if v != first {
				diverged = true
			}
			i++
		}
		if diverged {
			report.Converged = false
			report.Diverged = append(report.Diverged, KeyDivergence{Key: k, Values: values})
		}
	}
	return report
}

// CheckConvergence looks up a cluster and runs its convergence check.
func (o *Orchestrator) CheckConvergence(clusterID string) (ConvergenceReport, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return ConvergenceReport{}, err
	}
	return c.CheckConvergence(), nil
}
