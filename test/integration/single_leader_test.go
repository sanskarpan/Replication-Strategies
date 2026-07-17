package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
)

func TestSingleLeader_BasicWriteAndRead(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Write to leader
	result, err := orch.Write(cluster.ID, cluster.LeaderID, "key1", []byte("value1"), "client1")
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Read back from leader
	readResult, err := orch.Read(cluster.ID, cluster.LeaderID, "key1", "client1")
	require.NoError(t, err)
	assert.NotNil(t, readResult)
}

func TestSingleLeader_FollowerRefusesWrite(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Find a follower
	var followerID string
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			followerID = id
			break
		}
	}
	require.NotEmpty(t, followerID)

	_, err = orch.Write(cluster.ID, followerID, "key1", []byte("value1"), "client1")
	assert.Error(t, err, "follower should reject writes")
}

func TestSingleLeader_ReplicationPropagates(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(cluster.ID, cluster.LeaderID, "replicated-key", []byte("replicated-value"), "client1")
	require.NoError(t, err)

	// Give async replication time to propagate
	time.Sleep(100 * time.Millisecond)

	// Check followers got the data
	for _, id := range cluster.NodeIDs {
		if id == cluster.LeaderID {
			continue
		}
		result, err := orch.Read(cluster.ID, id, "replicated-key", "client1")
		assert.NoError(t, err, "follower %s should have replicated key", id)
		_ = result
	}
}

func TestSingleLeader_PauseAndResume(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	var followerID string
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			followerID = id
			break
		}
	}

	require.NoError(t, orch.PauseNode(cluster.ID, followerID))
	c, _ := orch.GetCluster(cluster.ID)
	n, ok := c.GetNode(followerID)
	require.True(t, ok)
	assert.Equal(t, node.StatePaused, n.GetState().State)

	require.NoError(t, orch.ResumeNode(cluster.ID, followerID))
	assert.Equal(t, node.StateOnline, n.GetState().State)
}

func TestSingleLeader_NetworkPartition(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeAsync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	followers := make([]string, 0)
	for _, id := range cluster.NodeIDs {
		if id != cluster.LeaderID {
			followers = append(followers, id)
		}
	}

	partID, err := orch.InjectPartition(cluster.ID, []string{cluster.LeaderID}, followers)
	require.NoError(t, err)
	assert.NotEmpty(t, partID)

	// Leader can still write (locally)
	_, err = orch.Write(cluster.ID, cluster.LeaderID, "partition-key", []byte("value"), "client1")
	require.NoError(t, err)

	// Heal
	require.NoError(t, orch.HealPartition(cluster.ID, partID))

	_ = context.Background()
}
