package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
	"replication-strategies/internal/storage"
)

func raftLeader(c *simulation.Cluster) string {
	for id, ns := range c.GetState().Nodes {
		if ns.Role == node.RoleLeader && ns.State == node.StateOnline {
			return id
		}
	}
	return ""
}

func committedCount(c *simulation.Cluster, key, want string) int {
	count := 0
	for _, id := range c.GetState().NodeIDs {
		nn, ok := c.GetNode(id)
		if !ok {
			continue
		}
		if e, present := nn.GetStore().Get(key); present && string(e.Value) == want {
			count++
		}
	}
	return count
}

func TestRaft_ElectsLeaderReplicatesAndFailsOver(t *testing.T) {
	bus := events.NewEventBus(2000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyRaft, NodeCount: 5})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// 1. A leader is elected.
	require.Eventually(t, func() bool { return raftLeader(cluster) != "" }, 3*time.Second, 30*time.Millisecond,
		"a leader must be elected")

	// 2. A write (auto-routed to the leader) commits and is readable.
	_, err = orch.Write(cluster.ID, "", "k", []byte("v1"), "c1")
	require.NoError(t, err)
	res, err := orch.Read(cluster.ID, "", "k", "c1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), res.Entry.(*storage.KVEntry).Value)

	// 3. Committed on a majority of the 5 nodes (commit propagates to follower state
	// machines on the next heartbeat, so allow a moment).
	require.Eventually(t, func() bool { return committedCount(cluster, "k", "v1") >= 3 },
		2*time.Second, 30*time.Millisecond, "entry committed+applied on a majority")

	// 4. Kill the leader -> a new, different leader is elected.
	oldLeader := raftLeader(cluster)
	require.NotEmpty(t, oldLeader)
	require.NoError(t, orch.PauseNode(cluster.ID, oldLeader))
	require.Eventually(t, func() bool {
		l := raftLeader(cluster)
		return l != "" && l != oldLeader
	}, 3*time.Second, 30*time.Millisecond, "a new leader must take over after failover")

	// 5. Writes resume on the new leader and remain readable.
	_, err = orch.Write(cluster.ID, "", "k2", []byte("v2"), "c1")
	require.NoError(t, err, "writes resume after failover")
	res2, err := orch.Read(cluster.ID, "", "k2", "c1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), res2.Entry.(*storage.KVEntry).Value)

	// 6. The recovered old leader steps down (rejoins as a follower).
	require.NoError(t, orch.ResumeNode(cluster.ID, oldLeader))
	time.Sleep(800 * time.Millisecond)
	assert.NotEqual(t, node.RoleLeader, cluster.GetState().Nodes[oldLeader].Role,
		"the recovered old leader must not still claim leadership")
}

// A single write to a Raft leader must be present on a majority (log-matching + commit).
func TestRaft_WriteNotLeaderRejected(t *testing.T) {
	bus := events.NewEventBus(2000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyRaft, NodeCount: 3})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	require.Eventually(t, func() bool { return raftLeader(cluster) != "" }, 3*time.Second, 30*time.Millisecond)
	leader := raftLeader(cluster)
	// Writing directly to a follower must be rejected with a redirect to the leader.
	var follower string
	for _, id := range cluster.NodeIDs {
		if id != leader {
			follower = id
			break
		}
	}
	_, err = orch.Write(cluster.ID, follower, "k", []byte("v"), "c1")
	assert.Error(t, err, "a follower must reject writes and redirect to the leader")
}

// A follower that is offline while the leader compacts its log must catch up via an
// InstallSnapshot (the entries it missed no longer exist as log entries).
func TestRaft_SnapshotCatchUp(t *testing.T) {
	bus := events.NewEventBus(4000)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{Strategy: node.StrategyRaft, NodeCount: 3})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	require.Eventually(t, func() bool { return raftLeader(cluster) != "" }, 3*time.Second, 30*time.Millisecond)
	leader := raftLeader(cluster)
	var follower string
	for _, id := range cluster.NodeIDs {
		if id != leader {
			follower = id
			break
		}
	}
	// Take the follower offline, then write past the compaction threshold (30).
	require.NoError(t, orch.PauseNode(cluster.ID, follower))
	for i := 0; i < 45; i++ {
		_, err := orch.Write(cluster.ID, "", "k", []byte(fmt.Sprintf("v%d", i)), "c1")
		require.NoError(t, err)
	}

	// The leader's log must have compacted (a snapshot boundary exists).
	ln, _ := cluster.GetNode(leader)
	require.Eventually(t, func() bool {
		si, _ := ln.GetLog().SnapshotBoundary()
		return si > 0
	}, 2*time.Second, 50*time.Millisecond, "leader must compact its log")

	// Bring the follower back — it can only catch up via a snapshot.
	require.NoError(t, orch.ResumeNode(cluster.ID, follower))
	fn, _ := cluster.GetNode(follower)
	require.Eventually(t, func() bool {
		e, ok := fn.GetStore().Get("k")
		return ok && string(e.Value) == "v44"
	}, 4*time.Second, 50*time.Millisecond, "recovered follower must catch up to the latest value via snapshot")
}
