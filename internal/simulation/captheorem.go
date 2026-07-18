package simulation

import (
	"fmt"

	"replication-strategies/internal/node"
	"replication-strategies/internal/quorum"
)

// CAPClass is the CAP-theorem / PACELC classification of a cluster configuration.
//
// CAP tells you the partition trade-off (C vs A when the network splits); PACELC
// extends it with the no-partition case (Else: Latency vs Consistency). The
// PACELC string uses the conventional form "P{A|C}/E{L|C}" — e.g. "PC/EL" means
// "on Partition favor Consistency, Else favor Latency".
type CAPClass struct {
	Type      string `json:"type"`      // CP | AP
	PACELC    string `json:"pacelc"`    // e.g. "PC/EL"
	Reasoning string `json:"reasoning"` // one-line human explanation
}

// ClassifyCAP classifies a cluster configuration under the CAP theorem and PACELC.
//
// The mapping follows the standard teaching model:
//   - single-leader + synchronous replication  => CP, PACELC "PC/EC"
//     (waits for followers before acking, so it pays latency for consistency
//     even without a partition, and refuses writes when it cannot reach them).
//   - single-leader + Raft (consensus)          => CP, PACELC "PC/EL"
//     (a majority quorum keeps it consistent under partition; when healthy the
//     leader can ack locally so it favors latency in the Else case).
//   - single-leader + asynchronous replication  => AP-leaning, PACELC "PA/EL"
//     (stays available and low-latency but followers serve stale reads).
//   - multi-leader                               => AP, PACELC "PA/EL"
//     (accepts writes on both sides of a partition, reconciles later).
//   - leaderless                                 => AP if W+R<=N (no overlap),
//     else CP-leaning; PACELC "PA/EL" for the typical tunable case, but
//     "PC/EC" when configured for strict overlap (W+R>N).
func ClassifyCAP(cfg ClusterConfig) CAPClass {
	switch cfg.Strategy {
	case node.StrategySingleLeader:
		switch cfg.ReplicationMode {
		case node.ModeSync:
			return CAPClass{
				Type:   "CP",
				PACELC: "PC/EC",
				Reasoning: "Single-leader synchronous replication blocks each write until followers " +
					"ack, choosing consistency over availability under partition and paying latency even when healthy.",
			}
		default:
			return CAPClass{
				Type:   "AP",
				PACELC: "PA/EL",
				Reasoning: "Single-leader asynchronous replication acks writes locally and stays available " +
					"and low-latency, but followers can serve stale reads (weak consistency).",
			}
		}
	case node.StrategyRaft:
		return CAPClass{
			Type:   "CP",
			PACELC: "PC/EL",
			Reasoning: "Raft uses a majority quorum, so it stays linearizable under partition (minority " +
				"side rejects writes); when healthy the leader commits quickly, favoring latency.",
		}
	case node.StrategyMultiLeader:
		return CAPClass{
			Type:   "AP",
			PACELC: "PA/EL",
			Reasoning: "Multi-leader accepts writes on every replica (both sides of a partition) and " +
				"reconciles conflicts asynchronously, favoring availability and latency over consistency.",
		}
	case node.StrategyLeaderless:
		q := quorumConfigFor(cfg)
		if q.IsStronglyConsistent() {
			return CAPClass{
				Type:   "CP",
				PACELC: "PC/EC",
				Reasoning: fmt.Sprintf("Leaderless with W+R>N (W=%d,R=%d,N=%d) guarantees read/write "+
					"overlap, leaning to consistency at the cost of availability and latency.", q.W, q.R, q.N),
			}
		}
		return CAPClass{
			Type:   "AP",
			PACELC: "PA/EL",
			Reasoning: fmt.Sprintf("Leaderless with W+R<=N (W=%d,R=%d,N=%d) has no guaranteed overlap, "+
				"so it favors availability and latency and may serve stale reads.", q.W, q.R, q.N),
		}
	default:
		return CAPClass{
			Type:      "AP",
			PACELC:    "PA/EL",
			Reasoning: fmt.Sprintf("Unknown strategy %q defaults to availability-leaning.", cfg.Strategy),
		}
	}
}

// quorumConfigFor derives the effective quorum.QuorumConfig from a cluster config,
// applying the same defaulting rules as createLeaderlessCluster (N defaults to the
// node count, W/R default to a simple majority).
func quorumConfigFor(cfg ClusterConfig) quorum.QuorumConfig {
	nodeCount := cfg.NodeCount
	if nodeCount == 0 {
		nodeCount = 5
	}
	n := cfg.QuorumN
	if n <= 0 || n > nodeCount {
		n = nodeCount
	}
	w := cfg.QuorumW
	r := cfg.QuorumR
	if w == 0 {
		w = n/2 + 1
	}
	if r == 0 {
		r = n/2 + 1
	}
	return quorum.QuorumConfig{N: n, W: w, R: r}
}

// SLA describes a service-level objective a challenge cluster must satisfy.
type SLA struct {
	MaxStaleReadProb  float64 `json:"max_stale_read_prob"`  // upper bound on P(stale read)
	MinOverlap        int     `json:"min_overlap"`          // minimum guaranteed W+R-N overlap
	MaxWriteLatencyMs int     `json:"max_write_latency_ms"` // upper bound on observed avg write latency
}

// GradeCheck is a single pass/fail assertion evaluated against an SLA.
type GradeCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// Grade is the result of grading a challenge cluster against an SLA.
type Grade struct {
	Passed bool         `json:"passed"` // true only if every check passed
	Score  int          `json:"score"`  // 0-100, proportional to checks passed
	Checks []GradeCheck `json:"checks"`
}

// GradeChallenge grades a cluster against an SLA, returning a per-check breakdown.
//
// For leaderless clusters it derives the quorum config and checks the stale-read
// probability and the read/write overlap directly from quorum math. For other
// strategies it grades the stale-read check by strategy strength: strongly
// consistent strategies (single-leader sync, Raft) pass with probability 0.
// The write-latency check always compares the observed average write latency
// (from the cluster metrics snapshot) against the SLA bound.
//
// Score is the proportion of passed checks scaled to 0-100; Passed is true only
// when every check passes.
func (o *Orchestrator) GradeChallenge(clusterID string, sla SLA) (Grade, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return Grade{}, err
	}

	cfg := c.Config
	checks := make([]GradeCheck, 0, 3)

	if cfg.Strategy == node.StrategyLeaderless {
		q := quorumConfigFor(cfg)

		staleProb := q.StaleReadProbability()
		checks = append(checks, GradeCheck{
			Name:   "stale_read_probability",
			Passed: staleProb <= sla.MaxStaleReadProb,
			Detail: fmt.Sprintf("P(stale)=%.4f <= %.4f", staleProb, sla.MaxStaleReadProb),
		})

		overlap := q.OverlapCount()
		checks = append(checks, GradeCheck{
			Name:   "quorum_overlap",
			Passed: overlap >= sla.MinOverlap,
			Detail: fmt.Sprintf("overlap(W+R-N)=%d >= %d", overlap, sla.MinOverlap),
		})
	} else {
		// Non-leaderless: grade the stale-read check by strategy strength.
		strong := isStronglyConsistent(cfg)
		staleProb := 1.0
		if strong {
			staleProb = 0.0
		}
		checks = append(checks, GradeCheck{
			Name:   "stale_read_probability",
			Passed: staleProb <= sla.MaxStaleReadProb,
			Detail: fmt.Sprintf("strategy P(stale)=%.4f <= %.4f (strong=%t)", staleProb, sla.MaxStaleReadProb, strong),
		})

		// Overlap only meaningfully applies to quorum systems; strong strategies
		// are treated as fully overlapping (MinOverlap satisfied), weak ones as 0.
		overlap := 0
		if strong {
			overlap = sla.MinOverlap
		}
		checks = append(checks, GradeCheck{
			Name:   "quorum_overlap",
			Passed: overlap >= sla.MinOverlap,
			Detail: fmt.Sprintf("effective overlap=%d >= %d (strong=%t)", overlap, sla.MinOverlap, strong),
		})
	}

	avgWriteMs := observedAvgWriteLatency(c)
	checks = append(checks, GradeCheck{
		Name:   "write_latency",
		Passed: avgWriteMs <= float64(sla.MaxWriteLatencyMs),
		Detail: fmt.Sprintf("avg write latency=%.2fms <= %dms", avgWriteMs, sla.MaxWriteLatencyMs),
	})

	passedCount := 0
	for _, ch := range checks {
		if ch.Passed {
			passedCount++
		}
	}

	grade := Grade{
		Passed: passedCount == len(checks),
		Score:  int(float64(passedCount) / float64(len(checks)) * 100.0),
		Checks: checks,
	}
	return grade, nil
}

// isStronglyConsistent reports whether a non-leaderless strategy provides strong
// (linearizable / no-stale-read) semantics: single-leader with synchronous
// replication, or Raft consensus.
func isStronglyConsistent(cfg ClusterConfig) bool {
	switch cfg.Strategy {
	case node.StrategyRaft:
		return true
	case node.StrategySingleLeader:
		return cfg.ReplicationMode == node.ModeSync
	default:
		return false
	}
}

// observedAvgWriteLatency averages the per-node average write latency from the
// cluster's current metrics snapshot. Nodes with no recorded writes contribute 0.
func observedAvgWriteLatency(c *Cluster) float64 {
	snap := c.Metrics.Snapshot()
	if len(snap.NodeMetrics) == 0 {
		return 0
	}
	var sum float64
	var count int
	for _, nm := range snap.NodeMetrics {
		if nm == nil {
			continue
		}
		sum += nm.AvgWriteLatency()
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}
