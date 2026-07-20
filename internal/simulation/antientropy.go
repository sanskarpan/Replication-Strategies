package simulation

import (
	"context"
	"encoding/base64"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"replication-strategies/internal/antientropy"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/telemetry"
)

// AntiEntropyReport summarizes a Merkle-tree anti-entropy pass.
type AntiEntropyReport struct {
	ClusterID string `json:"cluster_id"`
	// TotalKeys is the number of distinct keys across online replicas.
	TotalKeys int `json:"total_keys"`
	// DivergentKeys are the keys the Merkle diff flagged as differing between replicas —
	// only these need to be exchanged, instead of the whole store.
	DivergentKeys []string `json:"divergent_keys"`
	// Reconciled is how many (node,key) stale copies were repaired to the newest version.
	Reconciled int `json:"reconciled"`
	// ConvergedAfter reports whether all online replicas agree once the pass completes.
	ConvergedAfter bool `json:"converged_after"`
}

// mapRepr renders a node's key->content map for Merkle hashing. The content is the
// visible value (or a tombstone marker), independent of timestamp, so replicas holding
// the same visible state hash identically.
func mapRepr(n node.Node) map[string]string {
	out := map[string]string{}
	for _, k := range n.GetStore().Keys() {
		raw, ok := n.GetStore().GetRaw(k)
		if !ok {
			continue
		}
		if raw.Tombstone {
			out[k] = "<tomb>"
		} else {
			out[k] = base64.StdEncoding.EncodeToString(raw.Value)
		}
	}
	return out
}

// newestEntry returns the winning entry for key across the given nodes using the same
// last-write-wins total order the replicas use (timestamp, then NodeID tiebreak).
func newestEntry(nodes map[string]node.Node, key string) *storage.KVEntry {
	var best *storage.KVEntry
	for _, n := range nodes {
		raw, ok := n.GetStore().GetRaw(key)
		if !ok {
			continue
		}
		if best == nil || raw.Timestamp > best.Timestamp ||
			(raw.Timestamp == best.Timestamp && raw.NodeID > best.NodeID) {
			best = raw
		}
	}
	return best
}

// RunAntiEntropy performs a Merkle-tree anti-entropy round: it builds a Merkle tree per
// online replica, uses the tree diff to find exactly the keys that differ (rather than
// re-shipping the whole store), then reconciles each divergent key to its newest version.
func (o *Orchestrator) RunAntiEntropy(ctx context.Context, clusterID string) (AntiEntropyReport, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "orchestrator.anti_entropy")
	span.SetAttributes(attribute.String("cluster_id", clusterID))
	defer span.End()
	_ = ctx

	c, err := o.GetCluster(clusterID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return AntiEntropyReport{}, err
	}

	c.mu.RLock()
	online := make(map[string]node.Node)
	for id, n := range c.Nodes {
		if n.GetState().State == node.StateOnline {
			online[id] = n
		}
	}
	c.mu.RUnlock()

	rep := AntiEntropyReport{ClusterID: clusterID}
	if len(online) < 2 {
		rep.ConvergedAfter = true
		return rep, nil
	}

	// Build a Merkle tree per replica and diff every replica against a reference to find
	// the union of divergent keys — the only keys anti-entropy must exchange.
	trees := make(map[string]*antientropy.Node, len(online))
	allKeys := map[string]struct{}{}
	for id, n := range online {
		repr := mapRepr(n)
		trees[id] = antientropy.BuildTree(repr)
		for k := range repr {
			allKeys[k] = struct{}{}
		}
	}
	rep.TotalKeys = len(allKeys)

	var refID string
	for id := range trees {
		refID = id
		break
	}
	divergent := map[string]struct{}{}
	for id, t := range trees {
		if id == refID {
			continue
		}
		for _, k := range antientropy.Diff(trees[refID], t) {
			divergent[k] = struct{}{}
		}
	}
	for k := range divergent {
		rep.DivergentKeys = append(rep.DivergentKeys, k)
	}

	// Reconcile each divergent key to the newest version across replicas.
	for k := range divergent {
		best := newestEntry(online, k)
		if best == nil {
			continue
		}
		for _, n := range online {
			raw, ok := n.GetStore().GetRaw(k)
			stale := !ok || raw.Timestamp < best.Timestamp ||
				(raw.Timestamp == best.Timestamp && raw.NodeID < best.NodeID)
			if stale {
				n.GetStore().Set(best)
				rep.Reconciled++
			}
		}
	}

	rep.ConvergedAfter = c.CheckConvergence().Converged
	span.SetAttributes(
		attribute.Int("total_keys", rep.TotalKeys),
		attribute.Int("divergent_keys", len(divergent)),
		attribute.Int("reconciled", rep.Reconciled),
		attribute.Bool("converged_after", rep.ConvergedAfter),
	)
	o.bus.Publish(events.Event{
		Type:      events.EvtReadRepair,
		ClusterID: clusterID,
		Data: map[string]interface{}{
			"anti_entropy": true,
			"divergent":    len(divergent),
			"reconciled":   rep.Reconciled,
		},
	})
	return rep, nil
}
