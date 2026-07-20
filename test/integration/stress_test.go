package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
)

// TestStress_SustainedConcurrency hammers all three strategies with concurrent
// writes/reads/deletes and fault injection for a sustained period, then verifies:
//   - no panic or deadlock (test completes within the timeout),
//   - multi-leader replicas converge after faults are cleared,
//   - goroutines return to baseline after teardown (no leak).
//
// Duration defaults to 3s; set STRESS_SECONDS to run a longer soak.
func TestStress_SustainedConcurrency(t *testing.T) {
	dur := 3 * time.Second
	if v := os.Getenv("STRESS_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dur = time.Duration(n) * time.Second
		}
	}

	// Baseline goroutine count before creating anything.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	bus := events.NewEventBus(500)
	orch := simulation.NewOrchestrator(bus)

	// One cluster per strategy.
	mk := func(cfg simulation.ClusterConfig) *simulation.Cluster {
		c, err := orch.CreateCluster(cfg)
		require.NoError(t, err)
		return c
	}
	sl := mk(simulation.ClusterConfig{Strategy: node.StrategySingleLeader, NodeCount: 4, ReplicationMode: node.ModeAsync})
	ml := mk(simulation.ClusterConfig{Strategy: node.StrategyMultiLeader, NodeCount: 4, ConflictResolver: "lww"})
	ll := mk(simulation.ClusterConfig{Strategy: node.StrategyLeaderless, NodeCount: 5, QuorumW: 2, QuorumR: 2})
	clusters := []*simulation.Cluster{sl, ml, ll}

	var panics int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	worker := func(seed int64) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&panics, 1)
				t.Errorf("panic in stress worker: %v", r)
			}
		}()
		rng := rand.New(rand.NewSource(seed))
		for {
			select {
			case <-stop:
				return
			default:
			}
			c := clusters[rng.Intn(len(clusters))]
			nodeIDs := c.GetState().NodeIDs
			if len(nodeIDs) == 0 {
				continue
			}
			target := nodeIDs[rng.Intn(len(nodeIDs))]
			key := fmt.Sprintf("k%d", rng.Intn(20))
			switch rng.Intn(10) {
			case 0, 1, 2, 3:
				_, _ = orch.Write(context.Background(), c.ID, target, key, []byte(fmt.Sprintf("v%d", rng.Intn(1000))), fmt.Sprintf("c%d", seed))
			case 4, 5, 6:
				_, _ = orch.Read(context.Background(), c.ID, target, key, fmt.Sprintf("c%d", seed))
			case 7:
				_ = orch.Delete(context.Background(), c.ID, target, key, fmt.Sprintf("c%d", seed))
			case 8:
				_ = orch.PauseNode(c.ID, target)
				_ = orch.ResumeNode(c.ID, target)
			case 9:
				if len(nodeIDs) >= 2 {
					half := len(nodeIDs) / 2
					pid, err := orch.InjectPartition(c.ID, nodeIDs[:half], nodeIDs[half:])
					if err == nil {
						_ = orch.HealPartition(c.ID, pid)
					}
				}
			}
		}
	}

	const workers = 24
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(int64(i + 1))
	}

	time.Sleep(dur)
	close(stop)

	// Bounded wait for workers to finish — a deadlock would hang here and the test
	// timeout would fire, which is itself a failure signal.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("stress workers did not finish — possible deadlock")
	}
	assert.Equal(t, int64(0), atomic.LoadInt64(&panics), "no panics expected under load")

	// Convergence: clear faults, resume all nodes, write a final value to each key on
	// the multi-leader cluster, and confirm all nodes agree after anti-entropy.
	require.NoError(t, orch.ClearNetworkFaults(ml.ID))
	for _, id := range ml.GetState().NodeIDs {
		_ = orch.ResumeNode(ml.ID, id)
	}
	coord := ml.GetState().NodeIDs[0]
	for k := 0; k < 20; k++ {
		_, _ = orch.Write(context.Background(), ml.ID, coord, fmt.Sprintf("k%d", k), []byte("final"), "converger")
	}
	time.Sleep(2 * time.Second) // several anti-entropy ticks

	mlNodes := ml.GetState().NodeIDs
	diverged := 0
	for k := 0; k < 20; k++ {
		key := fmt.Sprintf("k%d", k)
		var vals []string
		for _, id := range mlNodes {
			n, ok := ml.GetNode(id)
			if !ok {
				continue
			}
			if e, present := n.GetStore().Get(key); present {
				vals = append(vals, string(e.Value))
			} else {
				vals = append(vals, "<absent>")
			}
		}
		for _, v := range vals {
			if v != vals[0] {
				diverged++
				t.Logf("key %s diverged across nodes: %v", key, vals)
				break
			}
		}
	}
	assert.Equal(t, 0, diverged, "all multi-leader replicas must converge after faults clear")

	// Teardown and leak check: after deleting clusters, goroutines should drain back
	// near baseline (node loops exit on ctx cancel; fabric link workers exit on Close).
	for _, c := range clusters {
		require.NoError(t, orch.DeleteCluster(c.ID))
	}
	// Allow short-lived goroutines (collectHints ~300ms) and workers to unwind.
	deadline := time.Now().Add(5 * time.Second)
	var final int
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		final = runtime.NumGoroutine()
		if final <= baseline+5 {
			break
		}
	}
	assert.LessOrEqualf(t, final, baseline+10,
		"goroutine leak: baseline=%d final=%d (nodes/fabric should have drained)", baseline, final)
}
