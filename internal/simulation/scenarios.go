package simulation

import (
	"fmt"
	"time"

	"replication-strategies/internal/node"
)

// Scenario describes a named demonstration scenario.
type Scenario struct {
	Name        string                   `json:"name"`
	Strategy    node.ReplicationStrategy `json:"strategy"`
	Description string                   `json:"description"`
	NodeCount   int                      `json:"node_count"`
}

// Scenarios is the catalogue of built-in demonstration scenarios.
var Scenarios = []Scenario{
	{
		Name:        "ReplicationLag",
		Strategy:    node.StrategySingleLeader,
		Description: "500ms latency on one follower; observe visible replication lag",
		NodeCount:   4,
	},
	{
		Name:        "SyncVsAsync",
		Strategy:    node.StrategySingleLeader,
		Description: "Toggle sync/async replication to compare write latency vs consistency",
		NodeCount:   3,
	},
	{
		Name:        "LeaderFailover",
		Strategy:    node.StrategySingleLeader,
		Description: "Kill leader, promote follower, observe orphaned writes",
		NodeCount:   4,
	},
	{
		Name:        "MultiLeaderConflict",
		Strategy:    node.StrategyMultiLeader,
		Description: "3-way partition then heal; compare LWW vs CRDT resolution",
		NodeCount:   3,
	},
	{
		Name:        "SplitBrain",
		Strategy:    node.StrategyMultiLeader,
		Description: "2+2 partition; diverge; reconcile on heal",
		NodeCount:   4,
	},
	{
		Name:        "QuorumTuning",
		Strategy:    node.StrategyLeaderless,
		Description: "Walk W=1/R=1 → W=3/R=3; observe stale reads demo",
		NodeCount:   5,
	},
	{
		Name:        "ReadRepair",
		Strategy:    node.StrategyLeaderless,
		Description: "Write W=3, pause 2 nodes, read quorum fires repair",
		NodeCount:   5,
	},
	{
		Name:        "NetworkPartitionHeal",
		Strategy:    node.StrategyLeaderless,
		Description: "2+3 partition; anti-entropy reconciles on heal",
		NodeCount:   5,
	},
}

// FindScenario returns the named scenario, or false if not found.
func FindScenario(name string) (*Scenario, bool) {
	for i, s := range Scenarios {
		if s.Name == name {
			return &Scenarios[i], true
		}
	}
	return nil, false
}

// RunScenario creates a cluster configured for the named scenario and performs
// its initial setup steps in the background.  Returns the new cluster ID.
func (o *Orchestrator) RunScenario(name string) (string, error) {
	s, ok := FindScenario(name)
	if !ok {
		return "", fmt.Errorf("scenario %q not found", name)
	}

	cfg := ClusterConfig{
		Strategy:  s.Strategy,
		NodeCount: s.NodeCount,
	}

	switch s.Name {
	case "ReplicationLag":
		cfg.ReplicationMode = node.ModeAsync
	case "SyncVsAsync":
		cfg.ReplicationMode = node.ModeAsync
	case "MultiLeaderConflict":
		cfg.ConflictResolver = "lww"
	case "SplitBrain":
		cfg.ConflictResolver = "lww"
	case "QuorumTuning":
		cfg.QuorumN = 5
		cfg.QuorumW = 1
		cfg.QuorumR = 1
	case "ReadRepair":
		cfg.QuorumN = 5
		cfg.QuorumW = 3
		cfg.QuorumR = 3
	case "NetworkPartitionHeal":
		cfg.QuorumN = 5
		cfg.QuorumW = 3
		cfg.QuorumR = 2
	}

	cluster, err := o.CreateCluster(cfg)
	if err != nil {
		return "", fmt.Errorf("create cluster for scenario %s: %w", name, err)
	}

	go o.setupScenario(cluster.ID, s.Name)

	return cluster.ID, nil
}

func (o *Orchestrator) setupScenario(clusterID, scenarioName string) {
	time.Sleep(200 * time.Millisecond)

	c, err := o.GetCluster(clusterID)
	if err != nil {
		return
	}

	switch scenarioName {
	case "ReplicationLag":
		// Add 500 ms latency to the last follower.
		if len(c.NodeIDs) >= 2 {
			lastFollower := c.NodeIDs[len(c.NodeIDs)-1]
			o.SetLatency(clusterID, c.LeaderID, lastFollower, 500)
			for i := 0; i < 5; i++ {
				o.Write(clusterID, c.LeaderID, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("val%d", i)), "scenario-client") //nolint:errcheck
				time.Sleep(100 * time.Millisecond)
			}
		}

	case "SyncVsAsync":
		for i := 0; i < 3; i++ {
			o.Write(clusterID, c.LeaderID, fmt.Sprintf("async-key%d", i), []byte(fmt.Sprintf("async-val%d", i)), "client-async") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
		}

	case "MultiLeaderConflict":
		if len(c.NodeIDs) >= 3 {
			o.Write(clusterID, c.NodeIDs[0], "conflict-key", []byte("initial"), "client-0") //nolint:errcheck
			time.Sleep(100 * time.Millisecond)

			// Isolate every node from every other.
			for i := 0; i < len(c.NodeIDs); i++ {
				for j := i + 1; j < len(c.NodeIDs); j++ {
					o.InjectPartition(clusterID, []string{c.NodeIDs[i]}, []string{c.NodeIDs[j]}) //nolint:errcheck
				}
			}
			time.Sleep(50 * time.Millisecond)

			// Concurrent writes to the same key.
			for i, nodeID := range c.NodeIDs {
				o.Write(clusterID, nodeID, "conflict-key", []byte(fmt.Sprintf("value-from-%s", nodeID)), fmt.Sprintf("client-%d", i)) //nolint:errcheck
				time.Sleep(20 * time.Millisecond)
			}

			// Heal all partitions after a brief delay.
			time.Sleep(500 * time.Millisecond)
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
		}

	case "SplitBrain":
		if len(c.NodeIDs) >= 4 {
			groupA := c.NodeIDs[:2]
			groupB := c.NodeIDs[2:]
			o.InjectPartition(clusterID, groupA, groupB) //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			o.Write(clusterID, groupA[0], "split-key", []byte("from-group-a"), "client-a") //nolint:errcheck
			o.Write(clusterID, groupB[0], "split-key", []byte("from-group-b"), "client-b") //nolint:errcheck

			time.Sleep(1 * time.Second)
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
		}

	case "QuorumTuning":
		for i := 0; i < 5; i++ {
			o.Write(clusterID, c.NodeIDs[0], fmt.Sprintf("quorum-key-%d", i), []byte(fmt.Sprintf("value-%d", i)), "scenario-client") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
		}

	case "ReadRepair":
		if len(c.NodeIDs) >= 3 {
			o.Write(clusterID, c.NodeIDs[0], "repair-key", []byte("repair-value"), "scenario-client") //nolint:errcheck
			time.Sleep(100 * time.Millisecond)

			o.PauseNode(clusterID, c.NodeIDs[1]) //nolint:errcheck
			o.PauseNode(clusterID, c.NodeIDs[2]) //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			// Write a new value while two nodes are paused.
			o.Write(clusterID, c.NodeIDs[0], "repair-key", []byte("updated-repair-value"), "scenario-client") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			// Resume the stale nodes.
			o.ResumeNode(clusterID, c.NodeIDs[1]) //nolint:errcheck
			o.ResumeNode(clusterID, c.NodeIDs[2]) //nolint:errcheck

			// A read now triggers repair.
			o.Read(clusterID, c.NodeIDs[0], "repair-key", "scenario-client") //nolint:errcheck
		}

	case "NetworkPartitionHeal":
		if len(c.NodeIDs) >= 5 {
			o.InjectPartition(clusterID, c.NodeIDs[:2], c.NodeIDs[2:]) //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			for i, nodeID := range c.NodeIDs {
				o.Write(clusterID, nodeID, fmt.Sprintf("partition-key-%d", i), []byte(fmt.Sprintf("partition-val-%d", i)), fmt.Sprintf("client-%d", i)) //nolint:errcheck
				time.Sleep(20 * time.Millisecond)
			}

			time.Sleep(500 * time.Millisecond)
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
		}
	}
}
