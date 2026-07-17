package integration

// Regression tests for bugs found in the deep audit (see ISSUES.md).
// Each test is named ISSUE_<n>_<summary> and fails on the pre-fix code.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// ISSUE-1: anti-entropy must not re-flag identical, already-converged entries as
// conflicts. A single write followed by an idle period should yield zero conflicts.
func TestISSUE1_AntiEntropy_NoSpuriousConflicts(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:         node.StrategyMultiLeader,
		NodeCount:        3,
		ConflictResolver: "lww",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "k", []byte("v"), "c1")
	require.NoError(t, err)

	// Several anti-entropy ticks (500ms each) with no further writes.
	time.Sleep(2500 * time.Millisecond)

	snap := cluster.Metrics.Snapshot()
	assert.Equal(t, int64(0), snap.TotalConflicts,
		"converged entries must not generate conflicts during anti-entropy")
}

// ISSUE-2: a quorum read where the coordinator lacks a local copy must not spin
// until the timeout. It should query R (not R-1) peers and return promptly.
func TestISSUE2_Leaderless_ReadLocalMiss_NoTimeout(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 5,
		QuorumW:   1,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Pause the coordinator so it misses the write entirely (guaranteed local miss).
	require.NoError(t, orch.PauseNode(cluster.ID, cluster.NodeIDs[0]))
	_, err = orch.Write(cluster.ID, cluster.NodeIDs[1], "k", []byte("v"), "c1")
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, orch.ResumeNode(cluster.ID, cluster.NodeIDs[0]))

	start := time.Now()
	res, err := orch.Read(cluster.ID, cluster.NodeIDs[0], "k", "c1")
	elapsed := time.Since(start)

	require.NoError(t, err, "read should succeed via remote quorum")
	assert.NotNil(t, res)
	assert.Less(t, elapsed, 300*time.Millisecond,
		"local-miss read must not hit the full quorum timeout (got %v)", elapsed)
}

// ISSUE-3: a leaderless write that cannot reach W acks must return an error, not a
// silent success. Pause enough replicas that W is unreachable.
func TestISSUE3_Leaderless_WriteQuorumFailure_Errors(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   3, // needs all three
		QuorumR:   1,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Pause two of the three nodes so only the coordinator remains — W=3 impossible.
	require.NoError(t, orch.PauseNode(cluster.ID, cluster.NodeIDs[1]))
	require.NoError(t, orch.PauseNode(cluster.ID, cluster.NodeIDs[2]))

	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "k", []byte("v"), "c1")
	assert.Error(t, err, "write must fail when W acks are unreachable")
}

// ISSUE-3: a healthy leaderless write that meets W must still succeed.
func TestISSUE3_Leaderless_WriteQuorumMet_Succeeds(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 5,
		QuorumW:   3,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "k", []byte("v"), "c1")
	assert.NoError(t, err, "write meeting W must succeed")
}

// ISSUE-3: a single-leader sync write that cannot reach all followers must error.
func TestISSUE3_SingleLeader_SyncReplicationIncomplete_Errors(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeSync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Pause both followers so no acks can come back.
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			require.NoError(t, orch.PauseNode(cluster.ID, id))
		}
	}

	_, err = orch.Write(cluster.ID, cluster.LeaderID, "k", []byte("v"), "c1")
	assert.Error(t, err, "sync write must fail when followers cannot ack")
}

// ISSUE-4: a write to a down replica must buffer a hinted-handoff entry that is
// delivered once the replica recovers (previously n.hints was never populated).
func TestISSUE4_Leaderless_HintedHandoff_DeliversOnRecovery(t *testing.T) {
	bus := events.NewEventBus(1000)

	hintSeen := make(chan events.Event, 10)
	sub := bus.Subscribe("hint-watch", []events.EventType{events.EvtHintedHandoff})
	defer bus.Unsubscribe("hint-watch")
	go func() {
		for {
			select {
			case e := <-sub.Ch:
				hintSeen <- e
			case <-sub.Done:
				return
			}
		}
	}()

	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1, // write still succeeds with a node down
		QuorumR:   1,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	downNode := cluster.NodeIDs[2]
	require.NoError(t, orch.PauseNode(cluster.ID, downNode))

	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "hint-key", []byte("hint-val"), "c1")
	require.NoError(t, err, "W=1 write should succeed even with one node down")

	// Let the grace window elapse so the hint is buffered, then recover the node.
	time.Sleep(400 * time.Millisecond)
	require.NoError(t, orch.ResumeNode(cluster.ID, downNode))

	// A hinted-handoff event must fire (delivery ticker runs every 2s).
	select {
	case <-hintSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("expected a hinted-handoff delivery event, got none")
	}

	// Give the delivered hint a moment to apply, then assert the recovered node
	// actually holds the value in its own local store.
	time.Sleep(150 * time.Millisecond)
	c, _ := orch.GetCluster(cluster.ID)
	n, ok := c.GetNode(downNode)
	require.True(t, ok)
	entry, present := n.GetStore().Get("hint-key")
	require.True(t, present, "recovered node should have received the hinted entry")
	assert.Equal(t, []byte("hint-val"), entry.Value)
}

// ISSUE-5: a single-leader delete must tombstone and replicate to followers so the
// key reads as not-found everywhere (previously deletes never propagated).
func TestISSUE5_SingleLeader_DeletePropagates(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.LeaderID, "del-key", []byte("v"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, orch.Delete(cluster.ID, cluster.LeaderID, "del-key", "c1"))
	time.Sleep(150 * time.Millisecond)

	// Not found on leader and every follower.
	for _, id := range cluster.NodeIDs {
		_, err := orch.Read(cluster.ID, id, "del-key", "c1")
		assert.Error(t, err, "node %s should report the deleted key as not found", id)
	}
}

// ISSUE-5: a leaderless delete tombstone must win and propagate under quorum.
func TestISSUE5_Leaderless_DeletePropagates(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   2,
		QuorumR:   2,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "del-key", []byte("v"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, orch.Delete(cluster.ID, cluster.NodeIDs[0], "del-key", "c1"))
	time.Sleep(150 * time.Millisecond)

	_, err = orch.Read(cluster.ID, cluster.NodeIDs[0], "del-key", "c1")
	assert.Error(t, err, "deleted key should read as not found after quorum delete")
}

// ISSUE-12: a follower that misses entries (all traffic dropped) must catch up via
// the periodic sync loop once the link recovers.
func TestISSUE12_Follower_RecoversDroppedEntries(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       2,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	var follower string
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			follower = id
		}
	}

	// Drop everything leader -> follower, then write two entries it will miss.
	require.NoError(t, orch.SetDropRate(cluster.ID, cluster.LeaderID, follower, 1.0))
	_, err = orch.Write(cluster.ID, cluster.LeaderID, "k1", []byte("v1"), "c1")
	require.NoError(t, err)
	_, err = orch.Write(cluster.ID, cluster.LeaderID, "k2", []byte("v2"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Confirm the follower is missing the data.
	_, missErr := orch.Read(cluster.ID, follower, "k1", "c1")
	require.Error(t, missErr, "precondition: follower should be missing k1 while link is down")

	// Heal the link; the periodic sync loop (1s) should fill the gap.
	require.NoError(t, orch.ClearNetworkFaults(cluster.ID))
	time.Sleep(2500 * time.Millisecond)

	_, err = orch.Read(cluster.ID, follower, "k1", "c1")
	assert.NoError(t, err, "follower should recover k1 via catch-up sync")
	_, err = orch.Read(cluster.ID, follower, "k2", "c1")
	assert.NoError(t, err, "follower should recover k2 via catch-up sync")
}

// ISSUE-13: messages on a single link are delivered in FIFO order even when an
// earlier message carries higher latency than a later one.
func TestISSUE13_Fabric_PreservesLinkOrder(t *testing.T) {
	fabric := transport.NewNetworkFabric()
	inbox := make(chan transport.Message, 16)
	fabric.Register("B", inbox)

	// First message on the link has high latency; later ones have none. FIFO must
	// still hold, so the receiver sees seq 1,2,3 in order.
	fabric.SetLatency("A", "B", 120)
	fabric.Send(transport.Message{Type: transport.MsgWrite, SenderID: "A", TargetID: "B", SeqNo: 1})
	fabric.SetLatency("A", "B", 0)
	fabric.Send(transport.Message{Type: transport.MsgWrite, SenderID: "A", TargetID: "B", SeqNo: 2})
	fabric.Send(transport.Message{Type: transport.MsgWrite, SenderID: "A", TargetID: "B", SeqNo: 3})

	var order []uint64
	timeout := time.After(2 * time.Second)
	for len(order) < 3 {
		select {
		case m := <-inbox:
			order = append(order, m.SeqNo)
		case <-timeout:
			t.Fatalf("only received %d/3 messages: %v", len(order), order)
		}
	}
	assert.Equal(t, []uint64{1, 2, 3}, order, "link delivery must be FIFO despite variable latency")
}

// ISSUE-14: adding a node to a leaderless cluster must keep quorum config consistent
// across all nodes (N grows, existing nodes are updated).
func TestISSUE14_AddNode_QuorumStaysConsistent(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   2,
		QuorumR:   2,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.AddNode(cluster.ID)
	require.NoError(t, err)

	// A write with the new membership should still reach quorum and succeed.
	_, err = orch.Write(cluster.ID, cluster.NodeIDs[0], "k", []byte("v"), "c1")
	assert.NoError(t, err, "write should succeed with consistent quorum after AddNode")
}

// ISSUE-6: the orchestrator must enforce the configured max-clusters cap.
func TestISSUE6_MaxClustersEnforced(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	orch.SetMaxClusters(2)

	c1, err := orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyLeaderless, NodeCount: 3})
	require.NoError(t, err)
	defer orch.DeleteCluster(c1.ID)
	c2, err := orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyLeaderless, NodeCount: 3})
	require.NoError(t, err)
	defer orch.DeleteCluster(c2.ID)

	_, err = orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyLeaderless, NodeCount: 3})
	assert.Error(t, err, "third cluster must be rejected when max is 2")
}

// ISSUE-7: PATCHing replication_mode must actually change the live leader's mode.
func TestISSUE7_ConfigPatch_ChangesLiveMode(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	c, _ := orch.GetCluster(cluster.ID)
	ln, _ := c.GetNode(cluster.LeaderID)
	sl, ok := ln.(*node.SingleLeaderNode)
	require.True(t, ok)

	// Switch to sync, then pause a follower — a sync write must now fail (proving the
	// mode change took effect on the live node, not just the stored config).
	sl.SetMode(node.ModeSync)
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			require.NoError(t, orch.PauseNode(cluster.ID, id))
		}
	}
	_, err = orch.Write(cluster.ID, cluster.LeaderID, "k", []byte("v"), "c1")
	assert.Error(t, err, "after switching to sync, a write with paused followers must fail")
}

// ISSUE-9: a read that finds a newer value on peers must also repair the
// coordinator's own stale local copy, not just the other replicas.
func TestISSUE9_Leaderless_ReadRepairsCoordinatorLocal(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	coord := cluster.NodeIDs[0]

	// v1 everywhere.
	_, err = orch.Write(cluster.ID, coord, "k", []byte("v1"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Pause coordinator, write v2 (coordinator misses it), resume -> coordinator stale.
	require.NoError(t, orch.PauseNode(cluster.ID, coord))
	_, err = orch.Write(cluster.ID, cluster.NodeIDs[1], "k", []byte("v2"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, orch.ResumeNode(cluster.ID, coord))

	// Quorum read from the coordinator sees v2 and must repair its own local copy.
	res, err := orch.Read(cluster.ID, coord, "k", "c1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), res.Entry.(*storage.KVEntry).Value)

	time.Sleep(50 * time.Millisecond)
	c, _ := orch.GetCluster(cluster.ID)
	n, _ := c.GetNode(coord)
	local, present := n.GetStore().Get("k")
	require.True(t, present)
	assert.Equal(t, []byte("v2"), local.Value,
		"coordinator's own local copy must be repaired to the newest value")
}

// ISSUE-10: concurrent writes to the same key on different multi-leader nodes must
// converge to a single agreed value (no lost-update divergence).
func TestISSUE10_MultiLeader_ConcurrentWritesConverge(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:         node.StrategyMultiLeader,
		NodeCount:        2,
		ConflictResolver: "lww",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	n0, n1 := cluster.NodeIDs[0], cluster.NodeIDs[1]
	done := make(chan struct{}, 2)
	go func() {
		for i := 0; i < 20; i++ {
			_, _ = orch.Write(cluster.ID, n0, "k", []byte("from-0"), "c0")
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 20; i++ {
			_, _ = orch.Write(cluster.ID, n1, "k", []byte("from-1"), "c1")
		}
		done <- struct{}{}
	}()
	<-done
	<-done

	// Let anti-entropy converge, then both nodes must agree on the same value.
	time.Sleep(1500 * time.Millisecond)
	r0, err0 := orch.Read(cluster.ID, n0, "k", "c")
	r1, err1 := orch.Read(cluster.ID, n1, "k", "c")
	require.NoError(t, err0)
	require.NoError(t, err1)
	assert.Equal(t, r0.Entry.(*storage.KVEntry).Value, r1.Entry.(*storage.KVEntry).Value,
		"both multi-leader nodes must converge to the same value")
}

// ISSUE-30: a delete that only partially replicates must NOT be resurrected by a
// stale live replica on read; the read must return not-found and repair the stale node.
func TestISSUE30_Leaderless_TombstoneNoResurrection(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   2,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	n0, n2 := cluster.NodeIDs[0], cluster.NodeIDs[2]

	// Write reaches all nodes.
	_, err = orch.Write(cluster.ID, n0, "k", []byte("live"), "c1")
	require.NoError(t, err)
	time.Sleep(150 * time.Millisecond)

	// Pause n2, then delete — n2 misses the tombstone but n0/n1 get it.
	require.NoError(t, orch.PauseNode(cluster.ID, n2))
	require.NoError(t, orch.Delete(cluster.ID, n0, "k", "c1"))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, orch.ResumeNode(cluster.ID, n2))

	// Quorum read from n0: n0/n1 have the tombstone (newer), n2 has the stale live value.
	// The delete must win — read returns not-found, no resurrection.
	_, err = orch.Read(cluster.ID, n0, "k", "c1")
	assert.Error(t, err, "read after delete must return not-found, not the resurrected live value")

	// n2's stale live value must be repaired to the tombstone (convergence).
	time.Sleep(150 * time.Millisecond)
	c, _ := orch.GetCluster(cluster.ID)
	nn2, _ := c.GetNode(n2)
	_, present := nn2.GetStore().Get("k")
	assert.False(t, present, "stale replica must converge to the tombstone after read-repair")
}

// ISSUE (Delete mode): a single-leader sync delete must honor the durability contract
// and error when followers can't ack, just like a sync write.
func TestISSUE31_SingleLeader_SyncDeleteIncomplete_Errors(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeSync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.LeaderID, "k", []byte("v"), "c1")
	// write itself may error (sync) but the key is committed locally; ignore.
	_ = err
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			require.NoError(t, orch.PauseNode(cluster.ID, id))
		}
	}
	err = orch.Delete(cluster.ID, cluster.LeaderID, "k", "c1")
	assert.Error(t, err, "sync delete with paused followers must fail (honors replication mode)")
}

// Caveat fix: a leaderless read that cannot gather R responses must signal quorum
// failure rather than returning possibly-stale data (symmetric with the write path).
func TestISSUE32_Leaderless_ReadQuorumNotMet(t *testing.T) {
	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	n0, n1, n2 := cluster.NodeIDs[0], cluster.NodeIDs[1], cluster.NodeIDs[2]
	_, err = orch.Write(cluster.ID, n0, "k", []byte("v"), "c1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Pause the other two replicas so R=3 responses are impossible.
	require.NoError(t, orch.PauseNode(cluster.ID, n1))
	require.NoError(t, orch.PauseNode(cluster.ID, n2))

	_, err = orch.Read(cluster.ID, n0, "k", "c1")
	require.Error(t, err, "read must fail when R responses are unreachable")
	assert.Contains(t, err.Error(), "quorum not met")
}
