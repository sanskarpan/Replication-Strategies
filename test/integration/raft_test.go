package integration

import (
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
