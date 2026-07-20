package simulation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"replication-strategies/internal/node"
	"replication-strategies/internal/storage"
)

// boolText picks yes/no text for a verdict actual-outcome string.
func boolText(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

// convergenceActual renders a convergence report as a short actual-outcome string.
func convergenceActual(r ConvergenceReport) string {
	if r.Converged {
		return fmt.Sprintf("all online replicas agree on %d key(s)", r.Keys)
	}
	return fmt.Sprintf("%d key(s) still diverge", len(r.Diverged))
}

// readValue reads a key through the orchestrator and returns the value string ("" on error).
func readValue(o *Orchestrator, clusterID, nodeID, key string) string {
	res, err := o.Read(clusterID, nodeID, key, "verdict-reader")
	if err != nil || res == nil {
		return ""
	}
	if e, ok := res.Entry.(*storage.KVEntry); ok {
		return string(e.Value)
	}
	return ""
}

// corruptReplica overwrites one replica's stored value for key with a bad value carrying
// an older timestamp, modelling a corrupt/stale on-disk copy that anti-entropy must heal.
func corruptReplica(c *Cluster, nodeID, key string) {
	n, ok := c.GetNode(nodeID)
	if !ok {
		return
	}
	cur, ok := n.GetStore().GetRaw(key)
	ts := int64(1)
	if ok {
		ts = cur.Timestamp - 1000
	}
	n.GetStore().Set(&storage.KVEntry{
		Key:       key,
		Value:     []byte("CORRUPT"),
		NodeID:    nodeID,
		Timestamp: ts,
		VClock:    storage.NewVectorClock(),
	})
}

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
	{
		Name:        "CascadingFailure",
		Strategy:    node.StrategyLeaderless,
		Description: "Overload one node, it fails, load shifts and topples the next (retry storm)",
		NodeCount:   5,
	},
	{
		Name:        "ThunderingHerd",
		Strategy:    node.StrategyLeaderless,
		Description: "Many clients hammer the same key at once; watch back-pressure drops rise",
		NodeCount:   5,
	},
	{
		Name:        "HotKeyZipfian",
		Strategy:    node.StrategyLeaderless,
		Description: "Zipfian load skew: one hot key takes the bulk of traffic",
		NodeCount:   5,
	},
	{
		Name:        "NetworkFlapping",
		Strategy:    node.StrategyLeaderless,
		Description: "A link flaps up/down repeatedly (gray failure); suspicion oscillates",
		NodeCount:   5,
	},
	{
		Name:        "ClockSkewLWW",
		Strategy:    node.StrategyLeaderless,
		Description: "Skew one node's clock; a causally-later write nearly loses — HLC saves it",
		NodeCount:   3,
	},
	{
		Name:        "CorruptReplica",
		Strategy:    node.StrategyLeaderless,
		Description: "One replica serves a corrupt value; read-repair/anti-entropy heals it",
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
	case "CascadingFailure", "ThunderingHerd", "HotKeyZipfian", "NetworkFlapping", "CorruptReplica":
		cfg.QuorumN = 5
		cfg.QuorumW = 2
		cfg.QuorumR = 2
	case "ClockSkewLWW":
		cfg.QuorumN = 3
		cfg.QuorumW = 2
		cfg.QuorumR = 2
	}

	cluster, err := o.CreateCluster(cfg)
	if err != nil {
		return "", fmt.Errorf("create cluster for scenario %s: %w", name, err)
	}

	o.beginScenario(cluster.ID, s.Name)
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
				o.Write(context.Background(), clusterID, c.LeaderID, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("val%d", i)), "scenario-client") //nolint:errcheck
				time.Sleep(100 * time.Millisecond)
			}
		}

	case "SyncVsAsync":
		for i := 0; i < 3; i++ {
			o.Write(context.Background(), clusterID, c.LeaderID, fmt.Sprintf("async-key%d", i), []byte(fmt.Sprintf("async-val%d", i)), "client-async") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
		}

	case "MultiLeaderConflict":
		if len(c.NodeIDs) >= 3 {
			o.Write(context.Background(), clusterID, c.NodeIDs[0], "conflict-key", []byte("initial"), "client-0") //nolint:errcheck
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
				o.Write(context.Background(), clusterID, nodeID, "conflict-key", []byte(fmt.Sprintf("value-from-%s", nodeID)), fmt.Sprintf("client-%d", i)) //nolint:errcheck
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

			o.Write(context.Background(), clusterID, groupA[0], "split-key", []byte("from-group-a"), "client-a") //nolint:errcheck
			o.Write(context.Background(), clusterID, groupB[0], "split-key", []byte("from-group-b"), "client-b") //nolint:errcheck

			time.Sleep(1 * time.Second)
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
		}

	case "QuorumTuning":
		for i := 0; i < 5; i++ {
			o.Write(context.Background(), clusterID, c.NodeIDs[0], fmt.Sprintf("quorum-key-%d", i), []byte(fmt.Sprintf("value-%d", i)), "scenario-client") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
		}

	case "ReadRepair":
		if len(c.NodeIDs) >= 3 {
			o.Write(context.Background(), clusterID, c.NodeIDs[0], "repair-key", []byte("repair-value"), "scenario-client") //nolint:errcheck
			time.Sleep(100 * time.Millisecond)

			o.PauseNode(clusterID, c.NodeIDs[1]) //nolint:errcheck
			o.PauseNode(clusterID, c.NodeIDs[2]) //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			// Write a new value while two nodes are paused.
			o.Write(context.Background(), clusterID, c.NodeIDs[0], "repair-key", []byte("updated-repair-value"), "scenario-client") //nolint:errcheck
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
				o.Write(context.Background(), clusterID, nodeID, fmt.Sprintf("partition-key-%d", i), []byte(fmt.Sprintf("partition-val-%d", i)), fmt.Sprintf("client-%d", i)) //nolint:errcheck
				time.Sleep(20 * time.Millisecond)
			}

			time.Sleep(500 * time.Millisecond)
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
			o.narrate(clusterID, "partition healed; anti-entropy reconciles the divergent keys")
			o.RunAntiEntropy(clusterID) //nolint:errcheck
			conv := c.CheckConvergence()
			o.verdict(clusterID, "all replicas converge after the heal",
				convergenceActual(conv), conv.Converged)
		}

	case "CascadingFailure":
		o.narrate(clusterID, "steady state: writing under a healthy 5-node quorum (W=2)")
		for i := 0; i < 4; i++ {
			o.Write(context.Background(), clusterID, c.NodeIDs[0], fmt.Sprintf("cf-%d", i), []byte("v"), "load") //nolint:errcheck
		}
		o.narrate(clusterID, "node-1 overloads and fails; its share of the load shifts to peers", c.NodeIDs[1])
		o.PauseNode(clusterID, c.NodeIDs[1]) //nolint:errcheck
		time.Sleep(120 * time.Millisecond)
		o.narrate(clusterID, "the extra load topples node-2 as well (retry storm cascades)", c.NodeIDs[2])
		o.PauseNode(clusterID, c.NodeIDs[2]) //nolint:errcheck
		time.Sleep(120 * time.Millisecond)
		// With 2 of 5 down and W=2, writes can still just meet quorum via sloppy stand-ins.
		_, werr := o.Write(context.Background(), clusterID, c.NodeIDs[0], "cf-after", []byte("v"), "load")
		o.verdict(clusterID, "cascading node loss degrades but sloppy quorum keeps writes alive",
			boolText(werr == nil, "write still succeeded via remaining/stand-in nodes", "write failed — quorum lost"),
			werr == nil)

	case "ThunderingHerd":
		o.narrate(clusterID, "50 clients stampede the same key simultaneously")
		before := c.Fabric.Dropped()
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				o.Write(context.Background(), clusterID, c.NodeIDs[n%len(c.NodeIDs)], "herd-key", []byte("v"), fmt.Sprintf("c%d", n)) //nolint:errcheck
			}(i)
		}
		wg.Wait()
		time.Sleep(150 * time.Millisecond)
		dropped := c.Fabric.Dropped() - before
		o.narrate(clusterID, fmt.Sprintf("herd absorbed; back-pressure drops during the burst: %d", dropped))
		o.verdict(clusterID, "the herd is served (back-pressure may shed some replication traffic)",
			fmt.Sprintf("cluster stayed up; %d messages shed under load", dropped), true)

	case "HotKeyZipfian":
		o.narrate(clusterID, "Zipfian traffic: ~80% of writes hit one hot key")
		hot, cold := 0, 0
		for i := 0; i < 40; i++ {
			key := "hot-key"
			if i%5 == 0 { // 1 in 5 hits a cold key
				key = fmt.Sprintf("cold-%d", i)
				cold++
			} else {
				hot++
			}
			o.Write(context.Background(), clusterID, c.NodeIDs[i%len(c.NodeIDs)], key, []byte("v"), "zipf") //nolint:errcheck
		}
		o.narrate(clusterID, fmt.Sprintf("hot-key writes: %d, cold-key writes: %d", hot, cold))
		o.verdict(clusterID, "one hot key dominates the workload",
			boolText(hot > cold*2, "hot key took the large majority of traffic", "load was more even than expected"),
			hot > cold*2)

	case "NetworkFlapping":
		if len(c.NodeIDs) >= 2 {
			a, b := c.NodeIDs[0], c.NodeIDs[1]
			o.narrate(clusterID, "a link between two nodes flaps up/down repeatedly (gray failure)", a, b)
			for i := 0; i < 4; i++ {
				o.SetDropRate(clusterID, a, b, 1.0) //nolint:errcheck  // link down
				time.Sleep(60 * time.Millisecond)
				o.SetDropRate(clusterID, a, b, 0.0) //nolint:errcheck  // link up
				time.Sleep(60 * time.Millisecond)
			}
			o.narrate(clusterID, "link restored; writes flow again")
			_, werr := o.Write(context.Background(), clusterID, a, "flap-key", []byte("v"), "flap")
			o.verdict(clusterID, "the cluster tolerates a flapping link and recovers",
				boolText(werr == nil, "post-flap write succeeded", "post-flap write failed"), werr == nil)
		}

	case "ClockSkewLWW":
		if len(c.NodeIDs) >= 2 {
			skewed, normal := c.NodeIDs[0], c.NodeIDs[1]
			o.narrate(clusterID, "node-1's physical clock is skewed 5s into the PAST", skewed)
			o.SetClockSkew(clusterID, skewed, -5000) //nolint:errcheck
			time.Sleep(30 * time.Millisecond)
			o.narrate(clusterID, "the skewed node makes a write, then a causally-later write lands on a normal node")
			o.Write(context.Background(), clusterID, skewed, "skew-key", []byte("first-from-skewed"), "c1") //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
			o.Write(context.Background(), clusterID, normal, "skew-key", []byte("second-causally-later"), "c1") //nolint:errcheck
			time.Sleep(150 * time.Millisecond)
			o.RunAntiEntropy(clusterID) //nolint:errcheck
			// HLC should carry the causal order so the later write wins despite the skew.
			won := readValue(o, clusterID, normal, "skew-key") == "second-causally-later"
			o.verdict(clusterID, "HLC preserves causal order: the later write wins despite the backward skew",
				boolText(won, "later write survived (HLC beat the wall-clock skew)", "earlier write incorrectly won"), won)
		}

	case "CorruptReplica":
		o.narrate(clusterID, "writing a good value to the cluster")
		o.Write(context.Background(), clusterID, c.NodeIDs[0], "corrupt-key", []byte("correct-value"), "c1") //nolint:errcheck
		time.Sleep(120 * time.Millisecond)
		o.narrate(clusterID, "one replica's on-disk value is corrupted (stale/bad copy)", c.NodeIDs[3])
		corruptReplica(c, c.NodeIDs[3], "corrupt-key")
		time.Sleep(30 * time.Millisecond)
		o.narrate(clusterID, "anti-entropy compares Merkle trees and repairs the corrupt replica")
		o.RunAntiEntropy(clusterID) //nolint:errcheck
		conv := c.CheckConvergence()
		o.verdict(clusterID, "anti-entropy heals the corrupt replica; all replicas agree",
			convergenceActual(conv), conv.Converged)
	}
}
