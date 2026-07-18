package simulation

import (
	"testing"
	"time"

	"replication-strategies/internal/events"
)

const testScenarioYAML = `
name: PartitionHealTest
strategy: leaderless
node_count: 5
quorum_n: 5
quorum_w: 3
quorum_r: 2
steps:
  - action: narrate
    narration: "writing an initial value"
  - action: write
    node: n0
    key: k
    value: v1
  - action: sleep
    ms: 50
  - action: partition
    group: [n0, n1]
    group2: [n2, n3, n4]
    narration: "split the cluster 2 + 3"
  - action: write
    node: n0
    key: k
    value: v-minority
  - action: write
    node: n2
    key: k
    value: v-majority
  - action: sleep
    ms: 50
  - action: heal
    narration: "heal the partition"
  - action: sleep
    ms: 50
  - action: anti_entropy
    narration: "anti-entropy reconciles divergence"
  - action: sleep
    ms: 50
`

func TestLoadScenarioSpec(t *testing.T) {
	spec, err := LoadScenarioSpec([]byte(testScenarioYAML))
	if err != nil {
		t.Fatalf("LoadScenarioSpec: %v", err)
	}
	if spec.Name != "PartitionHealTest" {
		t.Errorf("Name = %q, want PartitionHealTest", spec.Name)
	}
	if spec.Strategy != "leaderless" {
		t.Errorf("Strategy = %q, want leaderless", spec.Strategy)
	}
	if spec.NodeCount != 5 {
		t.Errorf("NodeCount = %d, want 5", spec.NodeCount)
	}
	if len(spec.Steps) == 0 {
		t.Fatal("expected steps, got none")
	}
}

func TestRunScenarioSpec(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)

	spec, err := LoadScenarioSpec([]byte(testScenarioYAML))
	if err != nil {
		t.Fatalf("LoadScenarioSpec: %v", err)
	}

	clusterID, err := o.RunScenarioSpec(spec)
	if err != nil {
		t.Fatalf("RunScenarioSpec: %v", err)
	}
	if clusterID == "" {
		t.Fatal("expected a non-empty cluster ID")
	}
	defer o.DeleteCluster(clusterID) //nolint:errcheck

	// Wait for the background run to finish and record its verdict.
	var result *ScenarioResult
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := o.ScenarioResult(clusterID)
		if ok && r.Done {
			result = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if result == nil {
		t.Fatal("scenario did not finish within the deadline")
	}
	if !result.Done {
		t.Fatal("expected result.Done to be true")
	}
	if len(result.Narration) == 0 {
		t.Fatal("expected non-empty narration")
	}
	if result.Verdict == nil {
		t.Fatal("expected a verdict")
	}
	if result.Verdict.Expected != "cluster converges" {
		t.Errorf("Verdict.Expected = %q, want %q", result.Verdict.Expected, "cluster converges")
	}
}
