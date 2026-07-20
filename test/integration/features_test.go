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

// TestLinearizability_SingleLeaderIsLinearizable verifies a normal single-leader run
// records an op history that the checker accepts as linearizable.
func TestLinearizability_SingleLeaderIsLinearizable(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	c, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:        node.StrategySingleLeader,
		NodeCount:       3,
		ReplicationMode: node.ModeSync,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(c.ID)

	for i, v := range []string{"1", "2", "3"} {
		_, err2 := orch.Write(context.Background(), c.ID, c.LeaderID, "k", []byte(v), "client1")
		require.NoError(t, err2)
		_, err2 = orch.Read(context.Background(), c.ID, c.LeaderID, "k", "client1")
		require.NoError(t, err2, "read %d", i)
	}

	rep, err := orch.CheckLinearizable(c.ID)
	require.NoError(t, err)
	assert.True(t, rep.Linearizable, "single-leader sync history must be linearizable, violation=%+v", rep.Violation)
	assert.Positive(t, rep.Ops)

	inv, err := orch.CheckInvariants(c.ID)
	require.NoError(t, err)
	assert.True(t, inv.OK, "invariants should hold: %v", inv.Violations)
}

// TestAntiEntropy_ReconcilesDivergentReplicas verifies a Merkle anti-entropy pass finds
// the divergent key and reconciles stale replicas to convergence.
func TestAntiEntropy_ReconcilesDivergentReplicas(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	c, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumW:   1, // let a write land on just the coordinator
		QuorumR:   1,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(c.ID)

	// Partition the coordinator away from the other replicas so the write is blocked at
	// send-time (never queued) and can't be delivered late — deterministic divergence.
	partID, err := orch.InjectPartition(c.ID, c.NodeIDs[:1], c.NodeIDs[1:])
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	_, err = orch.Write(context.Background(), c.ID, c.NodeIDs[0], "ae-key", []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	// Heal the partition; anti-entropy runs before the 2s hinted-handoff ticker fires.
	require.NoError(t, orch.HealPartition(c.ID, partID))

	rep, err := orch.RunAntiEntropy(context.Background(), c.ID)
	require.NoError(t, err)
	assert.Contains(t, rep.DivergentKeys, "ae-key", "Merkle diff should flag the divergent key")
	assert.Positive(t, rep.Reconciled, "stale replicas should be reconciled")
	assert.True(t, rep.ConvergedAfter, "cluster should converge after anti-entropy")
}

// TestSafeReconfigure_PreservesDataAndOverlap verifies the two-phase membership change
// keeps W+R>N, moves data onto the new node, and leaves reads working.
func TestSafeReconfigure_PreservesDataAndOverlap(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	c, err := orch.CreateCluster(simulation.ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3,
		QuorumN:   3,
		QuorumW:   2,
		QuorumR:   2,
	})
	require.NoError(t, err)
	defer orch.DeleteCluster(c.ID)

	_, err = orch.Write(context.Background(), c.ID, c.NodeIDs[0], "rk", []byte("v1"), "client1")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	rep, err := orch.SafeAddNode(c.ID)
	require.NoError(t, err)
	assert.True(t, rep.OverlapHeld, "W+R>N overlap must hold across the change")
	assert.Equal(t, rep.AddedNode, c.NodeIDs[len(c.NodeIDs)-1])
	// New quorum must still guarantee overlap.
	assert.Greater(t, rep.NewQuorum[1]+rep.NewQuorum[2], rep.NewQuorum[0], "new W+R>N")

	// The value survives the reconfiguration and remains readable.
	res, err := orch.Read(context.Background(), c.ID, rep.AddedNode, "rk", "client1")
	require.NoError(t, err)
	assert.Equal(t, "v1", string(entryValue(t, res.Entry)))
}

func TestDemo_TwoPC(t *testing.T) {
	ok := simulation.RunTwoPCDemo(false)
	assert.True(t, ok.Committed)
	assert.Equal(t, "1", ok.FinalValues["x"])
	assert.Equal(t, "2", ok.FinalValues["y"])

	crashed := simulation.RunTwoPCDemo(true)
	assert.True(t, crashed.Blocked, "participants must block after a coordinator crash")
	assert.True(t, crashed.Recovered, "recovery must finish the transaction")
	assert.True(t, crashed.Committed)
}

func TestDemo_MVCC(t *testing.T) {
	rep := simulation.RunMVCCDemo()
	assert.False(t, rep.ReadAt5Found, "read before the first version must be not-found")
	assert.Equal(t, "A", rep.ReadAt15, "snapshot @ t15 sees only A")
	assert.Equal(t, "B", rep.ReadAt25, "snapshot @ t25 sees B")
	assert.True(t, rep.SnapshotStable, "a t15 snapshot must be unaffected by the t20 write")
}

func TestDemo_WAL(t *testing.T) {
	buffered := simulation.RunWALDemo("buffered")
	assert.Positive(t, buffered.Lost, "buffered mode loses acked data on crash")
	fsync := simulation.RunWALDemo("fsync")
	assert.Zero(t, fsync.Lost, "fsync mode loses nothing on crash")
}

func TestDemo_SWIM(t *testing.T) {
	rep := simulation.RunSWIMDemo()
	assert.Contains(t, rep.FinalAlive, "n2", "n2 refuted its suspicion and is alive")
	assert.Contains(t, rep.FinalDead, "n3", "n3 timed out and is dead")
}

func TestDemo_Paxos(t *testing.T) {
	rep := simulation.RunPaxosDemo()
	assert.True(t, rep.SafetyHeld, "once chosen, the second proposer must adopt the same value")
	assert.Equal(t, rep.FirstChosen, rep.SecondFinal)
}

func TestDemo_DetSim(t *testing.T) {
	rep := simulation.RunDetSimDemo(42)
	assert.True(t, rep.Reproducible, "same seed must produce identical runs")
	assert.Equal(t, rep.Run1, rep.Run2)
	// A different seed should (almost surely) differ.
	other := simulation.RunDetSimDemo(43)
	assert.NotEqual(t, rep.Run1, other.Run1)
}
