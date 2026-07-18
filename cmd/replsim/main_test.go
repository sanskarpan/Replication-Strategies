package main

import (
	"strings"
	"testing"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
)

// TestRunCheckSingleLeader exercises the check logic in-process against a
// single-leader cluster and asserts that it reports success. A single-leader
// cluster with async replication should converge and stay linearizable after a
// simple write-then-read workload.
func TestRunCheckSingleLeader(t *testing.T) {
	orch := simulation.NewOrchestrator(events.NewEventBus(256))

	cfg := simulation.ClusterConfig{
		Strategy:  node.StrategySingleLeader,
		NodeCount: 3,
	}

	ok, report := runCheck(orch, cfg)
	if !ok {
		t.Fatalf("expected single_leader check to pass, got failure. Report:\n%s", report)
	}
	if !strings.Contains(report, "PASS") {
		t.Fatalf("expected report to contain PASS, got:\n%s", report)
	}
	if !strings.Contains(report, "converged:    true") {
		t.Errorf("expected report to show converged: true, got:\n%s", report)
	}
	if !strings.Contains(report, "linearizable: true") {
		t.Errorf("expected report to show linearizable: true, got:\n%s", report)
	}
}
