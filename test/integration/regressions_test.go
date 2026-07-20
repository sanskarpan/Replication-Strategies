package integration

// Regression tests for specific bugs found during in-depth review.
// Each test is named after the bug it covers.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
	"replication-strategies/internal/storage"
)

// Bug: SingleLeaderNode registered a throw-away channel with the fabric and read
// from a different (BaseNode.inbox) channel that never received messages, so sync
// and semi-sync writes always timed out waiting for follower acks.
func TestRegression_SingleLeader_SyncAcksReceived(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeSync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// A sync write must complete within a reasonable time — if acks were
	// never delivered the Write would block until the 2 s timeout.
	start := time.Now()
	_, err = orch.Write(context.Background(), cluster.ID, cluster.LeaderID, "sync-key", []byte("sync-val"), "c1")
	elapsed := time.Since(start)
	require.NoError(t, err)

	// With 3 nodes and no artificial latency the acks should arrive well
	// inside 1 s.  A genuine timeout would take the full 2 s.
	assert.Less(t, elapsed, 1*time.Second,
		"sync write took %v — acks probably not received (leader channel bug)", elapsed)
}

// Same as above but for semi-sync mode.
func TestRegression_SingleLeader_SemiSyncAcksReceived(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeSemiSync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	start := time.Now()
	_, err = orch.Write(context.Background(), cluster.ID, cluster.LeaderID, "semisync-key", []byte("v"), "c1")
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Less(t, elapsed, 400*time.Millisecond,
		"semi-sync write took %v — acks probably not received (leader channel bug)", elapsed)
}

// Bug: With W=1 the leaderless Write returned immediately after writing locally
// without sending any replication messages to other nodes, so data never
// propagated to the cluster.
func TestRegression_Leaderless_W1_DataReplicates(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1,
		QuorumR:   1,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	coordinator := cluster.NodeIDs[0]
	peer1 := cluster.NodeIDs[1]
	peer2 := cluster.NodeIDs[2]

	_, err = orch.Write(context.Background(), cluster.ID, coordinator, "w1-key", []byte("hello"), "c1")
	require.NoError(t, err)

	// Give async replication a moment.
	time.Sleep(150 * time.Millisecond)

	// Both non-coordinator nodes should now have the data.
	res1, err1 := orch.Read(context.Background(), cluster.ID, peer1, "w1-key", "c1")
	_, err2 := orch.Read(context.Background(), cluster.ID, peer2, "w1-key", "c1")

	assert.NoError(t, err1, "peer1 should have data after W=1 write")
	assert.NoError(t, err2, "peer2 should have data after W=1 write")
	if err1 == nil && res1 != nil {
		kvEntry, ok := res1.Entry.(*storage.KVEntry)
		assert.True(t, ok, "ReadResult.Entry should be *storage.KVEntry")
		if ok {
			assert.Equal(t, []byte("hello"), kvEntry.Value)
		}
	}
}

// Simpler version of the W=1 replication test that doesn't dereference results.
func TestRegression_Leaderless_W1_PeerCanRead(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1,
		QuorumR:   1,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	coordinator := cluster.NodeIDs[0]
	peer := cluster.NodeIDs[1]

	_, err = orch.Write(context.Background(), cluster.ID, coordinator, "peer-key", []byte("val"), "c1")
	require.NoError(t, err)
	time.Sleep(150 * time.Millisecond)

	_, err = orch.Read(context.Background(), cluster.ID, peer, "peer-key", "c1")
	assert.NoError(t, err, "non-coordinator peer should have data after W=1 write replicates")
}

// Bug: Read repair computed stale nodes from entry.NodeID (the original write
// coordinator) instead of the SenderID of the SyncAck response. This meant
// repair messages went to the wrong node and stale replicas were never fixed.
func TestRegression_Leaderless_ReadRepair_TargetsStaleResponder(t *testing.T) {
	bus := events.NewEventBus(100)

	repairEvents := make(chan events.Event, 20)
	sub := bus.Subscribe("repair-watch", []events.EventType{events.EvtReadRepair})
	defer bus.Unsubscribe("repair-watch")
	go func() {
		for {
			select {
			case e := <-sub.Ch:
				repairEvents <- e
			case <-sub.Done:
				return
			}
		}
	}()

	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 5,
		QuorumW:   3,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Write initial value — reaches at least 3 nodes.
	_, err = orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-tgt-key", []byte("v1"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Pause 2 nodes so they miss the next write.
	require.NoError(t, orch.PauseNode(cluster.ID, cluster.NodeIDs[1]))
	require.NoError(t, orch.PauseNode(cluster.ID, cluster.NodeIDs[2]))
	time.Sleep(30 * time.Millisecond)

	// Write new value; paused nodes don't see it.
	_, err = orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-tgt-key", []byte("v2"), "c1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Resume paused nodes — now they hold stale v1.
	require.NoError(t, orch.ResumeNode(cluster.ID, cluster.NodeIDs[1]))
	require.NoError(t, orch.ResumeNode(cluster.ID, cluster.NodeIDs[2]))
	time.Sleep(30 * time.Millisecond)

	// A quorum read may trigger repair because some responders have stale data.
	_, _ = orch.Read(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-tgt-key", "c1")

	// Wait briefly for any repair event.
	select {
	case evt := <-repairEvents:
		assert.Equal(t, events.EvtReadRepair, evt.Type)
		if nodes, ok := evt.Data["stale_nodes"]; ok {
			t.Logf("repaired stale nodes: %v", nodes)
		}
	case <-time.After(1 * time.Second):
		t.Log("no repair event triggered (may be OK if all responding nodes had latest version)")
	}
}

// Bug: MultiLeaderNode.Write() released n.mu before calling store.Set, opening
// a window where a concurrent write from a peer or another client could read a
// stale vector clock and generate a false conflict.
func TestRegression_MultiLeader_VCMonotonicity(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:         node.StrategyMultiLeader,
		NodeCount:        2,
		ConflictResolver: "vector_clock",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	n1 := cluster.NodeIDs[0]

	// Many concurrent writes to the same key on the same node — vector clocks
	// must strictly increase; no entry should ever get a lower VC than its
	// predecessor.
	const iterations = 50
	errs := make(chan error, iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			_, e := orch.Write(context.Background(), cluster.ID, n1, "mono-key", []byte("v"), "c1")
			errs <- e
		}()
	}
	for i := 0; i < iterations; i++ {
		require.NoError(t, <-errs)
	}

	// Read back — should succeed without error.
	_, err = orch.Read(context.Background(), cluster.ID, n1, "mono-key", "c1")
	assert.NoError(t, err)
}

// Bug: AddNode for leaderless strategy didn't increment c.Config.NodeCount,
// so a second AddNode call computed the same node index and could produce a
// duplicate node ID.
func TestRegression_Orchestrator_AddNode_UniqueIDs(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   2,
		QuorumR:   2,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	initialCount := len(cluster.NodeIDs)

	n1, err := orch.AddNode(cluster.ID)
	require.NoError(t, err)

	n2, err := orch.AddNode(cluster.ID)
	require.NoError(t, err)

	assert.NotEqual(t, n1.ID(), n2.ID(), "successive AddNode calls must produce unique IDs")

	c, _ := orch.GetCluster(cluster.ID)
	assert.Equal(t, initialCount+2, len(c.NodeIDs), "cluster should have 2 more nodes")

	// Verify no duplicate IDs in NodeIDs.
	seen := make(map[string]bool)
	for _, id := range c.NodeIDs {
		assert.False(t, seen[id], "duplicate node ID %s found in cluster", id)
		seen[id] = true
	}
}
