package simulation

import (
	"testing"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
)

func TestExportReport_PopulatedAfterWrite(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)

	c, err := o.CreateCluster(ClusterConfig{
		Strategy:  node.StrategySingleLeader,
		NodeCount: 3,
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	defer o.DeleteCluster(c.ID)

	if _, err := o.Write(c.ID, "", "k1", []byte("v1"), "client-1"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rep, err := o.ExportReport(c.ID)
	if err != nil {
		t.Fatalf("ExportReport: %v", err)
	}
	if rep == nil {
		t.Fatal("ExportReport returned nil report")
	}

	// GeneratedAt is intentionally left blank for the caller to stamp.
	if rep.GeneratedAt != "" {
		t.Errorf("GeneratedAt = %q, want empty (caller-stamped)", rep.GeneratedAt)
	}
	if rep.Config.Strategy != node.StrategySingleLeader {
		t.Errorf("Config.Strategy = %q, want %q", rep.Config.Strategy, node.StrategySingleLeader)
	}
	if rep.State.ID != c.ID {
		t.Errorf("State.ID = %q, want %q", rep.State.ID, c.ID)
	}
	if len(rep.State.NodeIDs) != 3 {
		t.Errorf("State.NodeIDs len = %d, want 3", len(rep.State.NodeIDs))
	}
	if rep.Metrics.ClusterID != c.ID {
		t.Errorf("Metrics.ClusterID = %q, want %q", rep.Metrics.ClusterID, c.ID)
	}
	if rep.Metrics.TotalWrites < 1 {
		t.Errorf("Metrics.TotalWrites = %d, want >= 1", rep.Metrics.TotalWrites)
	}
	if rep.Convergence.ClusterID != c.ID {
		t.Errorf("Convergence.ClusterID = %q, want %q", rep.Convergence.ClusterID, c.ID)
	}
	if rep.Invariants.ClusterID != c.ID {
		t.Errorf("Invariants.ClusterID = %q, want %q", rep.Invariants.ClusterID, c.ID)
	}
	// No scenario was run against this cluster.
	if rep.Scenario != nil {
		t.Errorf("Scenario = %+v, want nil (no scenario run)", rep.Scenario)
	}
}
