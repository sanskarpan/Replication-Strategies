package simulation

import (
	"context"
	"fmt"

	"replication-strategies/internal/metrics"
	"replication-strategies/internal/node"
)

// RaceResult is one strategy's outcome in a head-to-head comparison: how many writes
// landed, how many failed, the average write latency across its nodes, and whether the
// resulting cluster converged and its observed history was linearizable.
type RaceResult struct {
	Strategy          string  `json:"strategy"`
	ClusterID         string  `json:"cluster_id"`
	Writes            int     `json:"writes"`
	WriteErrors       int     `json:"write_errors"`
	AvgWriteLatencyMs float64 `json:"avg_write_latency_ms"`
	Converged         bool    `json:"converged"`
	Linearizable      bool    `json:"linearizable"`
}

// RaceReport is the aggregate of running the SAME workload against several strategies.
type RaceReport struct {
	Ops     int          `json:"ops"`
	Results []RaceResult `json:"results"`
}

// RunStrategyRace runs an identical workload against a fresh cluster of each named
// strategy and returns a side-by-side comparison. The workload writes `ops` keys
// (race-0..race-(ops-1)) then reads each one back. For every strategy it records the
// write/error counts, the average write latency across its nodes, and the outcome of
// the convergence and linearizability checks. All clusters created here are deleted
// before returning. nodeCount defaults to 3 and ops to 20 when non-positive.
func (o *Orchestrator) RunStrategyRace(strategies []string, nodeCount, ops int) (RaceReport, error) {
	if nodeCount <= 0 {
		nodeCount = 3
	}
	if ops <= 0 {
		ops = 20
	}

	report := RaceReport{Ops: ops, Results: make([]RaceResult, 0, len(strategies))}

	for _, s := range strategies {
		cfg := ClusterConfig{
			Strategy:  node.ReplicationStrategy(s),
			NodeCount: nodeCount,
		}
		cluster, err := o.CreateCluster(cfg)
		if err != nil {
			return RaceReport{}, fmt.Errorf("create cluster for strategy %q: %w", s, err)
		}

		res := RaceResult{Strategy: s, ClusterID: cluster.ID}

		clientID := fmt.Sprintf("race-%s", s)
		for i := 0; i < ops; i++ {
			key := fmt.Sprintf("race-%d", i)
			if _, werr := o.Write(context.Background(), cluster.ID, "", key, []byte(fmt.Sprintf("v-%d", i)), clientID); werr != nil {
				res.WriteErrors++
			} else {
				res.Writes++
			}
		}
		for i := 0; i < ops; i++ {
			key := fmt.Sprintf("race-%d", i)
			o.Read(context.Background(), cluster.ID, "", key, clientID) //nolint:errcheck
		}

		res.AvgWriteLatencyMs = avgWriteLatency(cluster.Metrics.Snapshot())

		if conv, cerr := o.CheckConvergence(cluster.ID); cerr == nil {
			res.Converged = conv.Converged
		}
		if lin, lerr := o.CheckLinearizable(cluster.ID); lerr == nil {
			res.Linearizable = lin.Linearizable
		}

		o.DeleteCluster(cluster.ID) //nolint:errcheck

		report.Results = append(report.Results, res)
	}

	return report, nil
}

// avgWriteLatency averages each node's mean write latency across the cluster snapshot,
// counting only nodes that actually recorded write samples.
func avgWriteLatency(snap metrics.ClusterSnapshot) float64 {
	var sum float64
	var n int
	for _, nm := range snap.NodeMetrics {
		if nm == nil {
			continue
		}
		if lat := nm.AvgWriteLatency(); lat > 0 {
			sum += lat
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
