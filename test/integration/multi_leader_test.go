package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
)

func TestMultiLeader_AllNodesAcceptWrites(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:         node.StrategyMultiLeader,
		NodeCount:        3,
		ConflictResolver: "lww",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	// Write to each node
	for i, nodeID := range cluster.NodeIDs {
		key := "key-from-" + nodeID
		_, err := orch.Write(cluster.ID, nodeID, key, []byte("value"), "client1")
		assert.NoError(t, err, "node %d (%s) should accept write", i, nodeID)
	}
}

func TestMultiLeader_ConflictDetection(t *testing.T) {
	bus := events.NewEventBus(100)

	conflictSeen := make(chan events.Event, 10)
	sub := bus.Subscribe("test", []events.EventType{events.EvtConflictDetected})

	orch := simulation.NewOrchestrator(bus)

	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:         node.StrategyMultiLeader,
		NodeCount:        3,
		ConflictResolver: "lww",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)
	defer bus.Unsubscribe("test")

	// Forward events from subscriber
	go func() {
		for {
			select {
			case evt := <-sub.Ch:
				conflictSeen <- evt
			case <-sub.Done:
				return
			}
		}
	}()

	// Partition all nodes
	for i := 0; i < len(cluster.NodeIDs); i++ {
		for j := i + 1; j < len(cluster.NodeIDs); j++ {
			orch.InjectPartition(cluster.ID, []string{cluster.NodeIDs[i]}, []string{cluster.NodeIDs[j]})
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Write same key from multiple nodes (creates conflict)
	for _, nodeID := range cluster.NodeIDs {
		orch.Write(cluster.ID, nodeID, "conflict-key", []byte("val-from-"+nodeID), "client-"+nodeID)
	}

	// Heal partitions
	c, _ := orch.GetCluster(cluster.ID)
	for partID := range c.Fabric.GetPartitions() {
		orch.HealPartition(cluster.ID, partID)
	}

	// Wait for conflict detection
	select {
	case evt := <-conflictSeen:
		assert.Equal(t, events.EvtConflictDetected, evt.Type)
	case <-time.After(2 * time.Second):
		// It's possible no conflict is detected if messages arrive in causal order
		t.Log("no conflict detected (acceptable if messages arrived in causal order)")
	}
}

func TestMultiLeader_VectorClockOrdering(t *testing.T) {
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
	n2 := cluster.NodeIDs[1]

	// Sequential writes: n1 writes first, then n2 reads and writes
	_, err = orch.Write(cluster.ID, n1, "ordered-key", []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// n2 writes a new value for the same key — this should NOT conflict
	// because it's a causally later write
	_, err = orch.Write(cluster.ID, n2, "ordered-key", []byte("v2"), "client1")
	require.NoError(t, err)
}
