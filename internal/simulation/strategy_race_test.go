package simulation

import (
	"testing"

	"replication-strategies/internal/events"
)

func TestRunStrategyRace_SingleLeaderVsLeaderless(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)

	report, err := o.RunStrategyRace([]string{"single_leader", "leaderless"}, 3, 10)
	if err != nil {
		t.Fatalf("RunStrategyRace: %v", err)
	}
	if report.Ops != 10 {
		t.Errorf("report.Ops = %d, want 10", report.Ops)
	}
	if len(report.Results) != 2 {
		t.Fatalf("len(report.Results) = %d, want 2", len(report.Results))
	}

	for _, res := range report.Results {
		if res.Strategy == "" {
			t.Errorf("result has empty Strategy: %+v", res)
		}
		if res.ClusterID == "" {
			t.Errorf("result for %q has empty ClusterID", res.Strategy)
		}
		if res.Writes <= 0 {
			t.Errorf("result for %q has Writes = %d, want > 0", res.Strategy, res.Writes)
		}
	}

	// Clusters created during the race must be cleaned up.
	if got := len(o.ListClusters()); got != 0 {
		t.Errorf("ListClusters() len = %d after race, want 0 (clusters should be deleted)", got)
	}
}

func TestRunStrategyRace_Defaults(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)

	report, err := o.RunStrategyRace([]string{"single_leader"}, 0, 0)
	if err != nil {
		t.Fatalf("RunStrategyRace: %v", err)
	}
	if report.Ops != 20 {
		t.Errorf("report.Ops = %d, want default 20", report.Ops)
	}
	if len(report.Results) != 1 {
		t.Fatalf("len(report.Results) = %d, want 1", len(report.Results))
	}
	if report.Results[0].Writes <= 0 {
		t.Errorf("Writes = %d, want > 0", report.Results[0].Writes)
	}
}
