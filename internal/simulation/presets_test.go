package simulation

import (
	"testing"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
)

// validStrategies is the set of replication strategies the orchestrator knows
// how to build a cluster for.
var validStrategies = map[node.ReplicationStrategy]bool{
	node.StrategySingleLeader: true,
	node.StrategyMultiLeader:  true,
	node.StrategyLeaderless:   true,
	node.StrategyRaft:         true,
}

func TestPresetsAreWellFormed(t *testing.T) {
	if len(Presets) == 0 {
		t.Fatal("expected at least one preset")
	}
	for _, p := range Presets {
		if p.Name == "" {
			t.Errorf("preset with empty Name: %+v", p)
		}
		if !validStrategies[p.Config.Strategy] {
			t.Errorf("preset %q has invalid strategy %q", p.Name, p.Config.Strategy)
		}
		if p.Config.NodeCount < 1 {
			t.Errorf("preset %q has invalid node count %d", p.Name, p.Config.NodeCount)
		}
	}
}

func TestExpectedPresetsPresent(t *testing.T) {
	want := []string{"Cassandra", "DynamoDB", "PostgreSQL", "etcd", "Kafka"}
	for _, name := range want {
		if _, ok := FindPreset(name); !ok {
			t.Errorf("expected preset %q to be present", name)
		}
	}
}

func TestFindPreset(t *testing.T) {
	p, ok := FindPreset("Cassandra")
	if !ok {
		t.Fatal("Cassandra preset not found")
	}
	if p.Name != "Cassandra" {
		t.Errorf("FindPreset returned wrong preset: got %q", p.Name)
	}
	if p.Config.Strategy != node.StrategyLeaderless {
		t.Errorf("Cassandra should be leaderless, got %q", p.Config.Strategy)
	}

	if _, ok := FindPreset("does-not-exist"); ok {
		t.Error("FindPreset should return false for an unknown preset")
	}
}

func TestListPresetsIsACopy(t *testing.T) {
	got := ListPresets()
	if len(got) != len(Presets) {
		t.Fatalf("ListPresets length mismatch: got %d want %d", len(got), len(Presets))
	}
	// Mutating the returned copy must not affect the package-level slice.
	orig := Presets[0].Name
	got[0].Name = "mutated"
	if Presets[0].Name != orig {
		t.Errorf("ListPresets did not return a copy: Presets[0].Name changed to %q", Presets[0].Name)
	}
}

func TestCreateFromPreset(t *testing.T) {
	bus := events.NewEventBus(256)
	o := NewOrchestrator(bus)

	for _, p := range Presets {
		p := p
		t.Run(p.Name, func(t *testing.T) {
			cluster, err := o.CreateFromPreset(p.Name)
			if err != nil {
				t.Fatalf("CreateFromPreset(%q) failed: %v", p.Name, err)
			}
			if cluster == nil {
				t.Fatalf("CreateFromPreset(%q) returned nil cluster", p.Name)
			}
			if cluster.Config.Strategy != p.Config.Strategy {
				t.Errorf("cluster strategy = %q, want %q", cluster.Config.Strategy, p.Config.Strategy)
			}
			if len(cluster.NodeIDs) == 0 {
				t.Errorf("cluster for preset %q has no nodes", p.Name)
			}
			if err := o.DeleteCluster(cluster.ID); err != nil {
				t.Errorf("DeleteCluster(%s) failed: %v", cluster.ID, err)
			}
		})
	}
}

func TestCreateFromPresetUnknown(t *testing.T) {
	bus := events.NewEventBus(256)
	o := NewOrchestrator(bus)
	if _, err := o.CreateFromPreset("nope"); err == nil {
		t.Error("expected error for unknown preset")
	}
}
