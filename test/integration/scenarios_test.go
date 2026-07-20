package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/simulation"
)

// waitForVerdict polls until the scenario for clusterID has produced a verdict.
func waitForVerdict(t *testing.T, orch *simulation.Orchestrator, clusterID string, timeout time.Duration) *simulation.ScenarioResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if res, ok := orch.ScenarioResult(clusterID); ok && res.Done {
			return res
		}
		time.Sleep(50 * time.Millisecond)
	}
	res, _ := orch.ScenarioResult(clusterID)
	return res
}

// TestScenarios_NarrationAndVerdicts runs each new teaching scenario and asserts it
// produces a narrated timeline and a passing expected-vs-actual verdict.
func TestScenarios_NarrationAndVerdicts(t *testing.T) {
	for _, name := range []string{
		"CascadingFailure", "ThunderingHerd", "HotKeyZipfian",
		"NetworkFlapping", "ClockSkewLWW", "CorruptReplica", "NetworkPartitionHeal",
	} {
		name := name
		t.Run(name, func(t *testing.T) {
			bus := events.NewEventBus(500)
			orch := simulation.NewOrchestrator(bus)
			cid, err := orch.RunScenario(name)
			require.NoError(t, err)
			defer orch.DeleteCluster(cid)

			res := waitForVerdict(t, orch, cid, 8*time.Second)
			require.NotNil(t, res, "scenario %s should produce a result", name)
			assert.True(t, res.Done, "scenario %s should complete", name)
			assert.NotEmpty(t, res.Narration, "scenario %s should narrate steps", name)
			require.NotNil(t, res.Verdict, "scenario %s should render a verdict", name)
			assert.True(t, res.Verdict.Passed,
				"scenario %s verdict should pass: expected=%q actual=%q",
				name, res.Verdict.Expected, res.Verdict.Actual)
		})
	}
}

// TestScenarios_CatalogValid asserts every cataloged scenario is runnable.
func TestScenarios_CatalogueValid(t *testing.T) {
	bus := events.NewEventBus(100)
	orch := simulation.NewOrchestrator(bus)
	for _, s := range simulation.Scenarios {
		cid, err := orch.RunScenario(s.Name)
		require.NoError(t, err, "scenario %s should start", s.Name)
		assert.NotEmpty(t, cid)
		orch.DeleteCluster(cid)
	}
}
