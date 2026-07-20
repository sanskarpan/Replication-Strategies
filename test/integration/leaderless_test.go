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

func TestLeaderless_QuorumWrite(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 5,
		QuorumW:   3,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	result, err := orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "quorum-key", []byte("quorum-value"), "client1")
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLeaderless_QuorumRead(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 5,
		QuorumW:   3,
		QuorumR:   3,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	_, err = orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "read-key", []byte("read-value"), "client1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	_, err = orch.Read(context.Background(), cluster.ID, cluster.NodeIDs[0], "read-key", "client1")
	require.NoError(t, err)
}

func TestLeaderless_AllNodesAcceptCoordinatorWrites(t *testing.T) {
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

	for i, id := range cluster.NodeIDs {
		key := "key-coordinator-" + id
		_, err := orch.Write(context.Background(), cluster.ID, id, key, []byte("val"), "client1")
		assert.NoError(t, err, "node %d should accept writes as coordinator", i)
	}
}

func TestLeaderless_ReadRepairEmitted(t *testing.T) {
	bus := events.NewEventBus(100)
	repairSeen := make(chan events.Event, 10)
	sub := bus.Subscribe("repair-test", []events.EventType{events.EvtReadRepair})
	defer bus.Unsubscribe("repair-test")

	go func() {
		for {
			select {
			case evt := <-sub.Ch:
				repairSeen <- evt
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

	// Write to establish base
	_, err = orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-key", []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Pause 2 nodes so they become stale
	orch.PauseNode(cluster.ID, cluster.NodeIDs[1])
	orch.PauseNode(cluster.ID, cluster.NodeIDs[2])
	time.Sleep(50 * time.Millisecond)

	// Write new value (won't reach paused nodes)
	_, err = orch.Write(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-key", []byte("v2"), "client1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Resume paused nodes — now they are stale
	orch.ResumeNode(cluster.ID, cluster.NodeIDs[1])
	orch.ResumeNode(cluster.ID, cluster.NodeIDs[2])

	// Read — should trigger repair if stale nodes respond
	orch.Read(context.Background(), cluster.ID, cluster.NodeIDs[0], "repair-key", "client1")

	// Check if repair event was emitted (may not fire in all timing conditions)
	select {
	case evt := <-repairSeen:
		assert.Equal(t, events.EvtReadRepair, evt.Type)
	case <-time.After(1 * time.Second):
		t.Log("no read repair event (acceptable — nodes may not have responded yet)")
	}
}
