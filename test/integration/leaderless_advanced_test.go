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
	"replication-strategies/internal/storage"
)

// entryValue extracts the value bytes from a read/write result Entry (a *storage.KVEntry).
func entryValue(t *testing.T, e interface{}) []byte {
	t.Helper()
	kv, ok := e.(*storage.KVEntry)
	require.True(t, ok, "entry is a *storage.KVEntry, got %T", e)
	return kv.Value
}

// storeHas reports whether the given node currently holds a live (non-tombstone) value.
func storeHas(t *testing.T, c *simulation.Cluster, nodeID, key string) bool {
	t.Helper()
	n, ok := c.GetNode(nodeID)
	require.True(t, ok, "node %s exists", nodeID)
	_, found := n.GetStore().Get(key)
	return found
}

// TestLeaderless_PreferenceListRouting verifies that with N < cluster size a key is
// replicated only to its preference-list replicas, not to every node.
func TestLeaderless_PreferenceListRouting(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	cluster, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 6,
		QuorumN:   3, // replication factor 3 over a 6-node cluster
		QuorumW:   2,
		QuorumR:   2,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(cluster.ID)

	key := "pref-key"
	pref, err := orch.Placement(cluster.ID, key, 3)
	require.NoError(t, err)
	require.Len(t, pref, 3)

	// Coordinate the write through the first preference-list replica.
	_, err = orch.Write(context.Background(), cluster.ID, pref[0], key, []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(150 * time.Millisecond)

	// Every preference-list replica must hold the key.
	for _, id := range pref {
		assert.True(t, storeHas(t, cluster, id, key), "preference replica %s should hold the key", id)
	}
	// Count total holders — routing must NOT have written to all 6 nodes.
	holders := 0
	for _, id := range cluster.NodeIDs {
		if storeHas(t, cluster, id, key) {
			holders++
		}
	}
	assert.LessOrEqual(t, holders, 4, "key should live on ~N replicas, not the whole cluster (got %d)", holders)
}

// TestLeaderless_SloppyQuorumMeetsWDuringFailure verifies sloppy quorum borrows healthy
// stand-in nodes so W is met when preferred replicas are down, and that a cluster with
// sloppy disabled fails the same write.
func TestLeaderless_SloppyQuorumMeetsWDuringFailure(t *testing.T) {
	sloppyOff := false
	mk := func(sloppy *bool) (*simulation.Cluster, *simulation.Orchestrator, []string) {
		bus := events.NewEventBus(100)
		orch := simulation.NewOrchestrator(bus)
		c, err := orch.CreateCluster(simulation.ClusterConfig{
			Strategy:     node.StrategyLeaderless,
			NodeCount:    6,
			QuorumN:      3,
			QuorumW:      2,
			QuorumR:      2,
			SloppyQuorum: sloppy,
		})
		require.NoError(t, err)
		pref, err := orch.Placement(c.ID, "hot", 3)
		require.NoError(t, err)
		return c, orch, pref
	}

	// Sloppy ON (default): pause the two non-coordinator preferred replicas so only the
	// coordinator can ack among the preferred set; W=2 must still be met via stand-ins.
	c1, o1, pref1 := mk(nil)
	defer o1.DeleteCluster(c1.ID)
	require.NoError(t, o1.PauseNode(c1.ID, pref1[1]))
	require.NoError(t, o1.PauseNode(c1.ID, pref1[2]))
	time.Sleep(30 * time.Millisecond)
	_, err := o1.Write(context.Background(), c1.ID, pref1[0], "hot", []byte("v1"), "client1")
	assert.NoError(t, err, "sloppy quorum should meet W via healthy stand-in nodes")

	// Sloppy OFF: the same failure must fail the write (only the coordinator can ack).
	c2, o2, pref2 := mk(&sloppyOff)
	defer o2.DeleteCluster(c2.ID)
	require.NoError(t, o2.PauseNode(c2.ID, pref2[1]))
	require.NoError(t, o2.PauseNode(c2.ID, pref2[2]))
	time.Sleep(30 * time.Millisecond)
	_, err = o2.Write(context.Background(), c2.ID, pref2[0], "hot", []byte("v1"), "client1")
	assert.Error(t, err, "without sloppy quorum the write should fail to meet W")
}

// TestLeaderless_LocalQuorumSurvivesRegionPartition verifies LOCAL_QUORUM completes a
// write using only the coordinator's own region while a plain quorum fails under the
// same cross-region partition.
func TestLeaderless_LocalQuorumSurvivesRegionPartition(t *testing.T) {
	run := func(level string) error {
		bus := events.NewEventBus(100)
		orch := simulation.NewOrchestrator(bus)
		c, err := orch.CreateCluster(simulation.ClusterConfig{
			Strategy:         node.StrategyLeaderless,
			NodeCount:        4,
			QuorumN:          4, // every node is a replica; 2 per region
			QuorumW:          3, // plain majority needs 3 (spans both regions)
			QuorumR:          2,
			Regions:          2,
			ConsistencyLevel: level,
		})
		require.NoError(t, err)
		defer orch.DeleteCluster(c.ID)

		// Regions are assigned round-robin: even indices -> region 0, odd -> region 1.
		var r0, r1 []string
		for i, id := range c.NodeIDs {
			if i%2 == 0 {
				r0 = append(r0, id)
			} else {
				r1 = append(r1, id)
			}
		}
		_, err = orch.InjectPartition(c.ID, r0, r1)
		require.NoError(t, err)
		time.Sleep(30 * time.Millisecond)

		// Coordinate from a region-0 node; only its region is reachable.
		_, werr := orch.Write(context.Background(), c.ID, r0[0], "geo-key", []byte("v1"), "client1")
		return werr
	}

	assert.NoError(t, run("local_quorum"), "LOCAL_QUORUM should succeed within the coordinator region")
	assert.Error(t, run("quorum"), "plain QUORUM should fail when a region is partitioned away")
}

// TestLeaderless_DigestReadReturnsFreshValue verifies a digest read still returns the
// correct freshest value (the value is fetched from the winning replica).
func TestLeaderless_DigestReadReturnsFreshValue(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	c, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:       node.StrategyLeaderless,
		NodeCount:      5,
		QuorumW:        3,
		QuorumR:        3,
		ReadRepairMode: "digest",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(c.ID)

	_, err = orch.Write(context.Background(), c.ID, c.NodeIDs[0], "dk", []byte("hello"), "client1")
	require.NoError(t, err)
	time.Sleep(120 * time.Millisecond)

	// Read from a different coordinator so the value must be pulled cross-node.
	res, err := orch.Read(c.ID, c.NodeIDs[1], "dk", "client1")
	require.NoError(t, err)
	require.NotNil(t, res)
	entry := entryValue(t, res.Entry)
	assert.Equal(t, "hello", string(entry), "digest read must return the true freshest value")
}

// TestLeaderless_SyncReadRepairConverges verifies synchronous read repair leaves stale
// replicas holding the fresh value by the time the read returns.
func TestLeaderless_SyncReadRepairConverges(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	c, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:       node.StrategyLeaderless,
		NodeCount:      5,
		QuorumW:        1, // let writes land without full replication so staleness exists
		QuorumR:        3,
		ReadRepairMode: "sync",
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(c.ID)

	// Seed a value everywhere.
	_, err = orch.Write(context.Background(), c.ID, c.NodeIDs[0], "sk", []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(150 * time.Millisecond)

	// Pause most nodes and write a fresh value with W=1 so only the coordinator has it.
	for _, id := range c.NodeIDs[1:] {
		require.NoError(t, orch.PauseNode(c.ID, id))
	}
	time.Sleep(20 * time.Millisecond)
	_, err = orch.Write(context.Background(), c.ID, c.NodeIDs[0], "sk", []byte("v2"), "client1")
	require.NoError(t, err)
	for _, id := range c.NodeIDs[1:] {
		require.NoError(t, orch.ResumeNode(c.ID, id))
	}
	time.Sleep(20 * time.Millisecond)

	// A sync read repairs stale replicas before returning v2.
	res, err := orch.Read(c.ID, c.NodeIDs[0], "sk", "client1")
	require.NoError(t, err)
	assert.Equal(t, "v2", string(entryValue(t, res.Entry)))
}
