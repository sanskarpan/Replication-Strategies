package simulation

import (
	"testing"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
)

func TestClassifyCAP(t *testing.T) {
	cases := []struct {
		name     string
		cfg      ClusterConfig
		wantType string
	}{
		{
			name:     "single_leader sync => CP",
			cfg:      ClusterConfig{Strategy: node.StrategySingleLeader, NodeCount: 3, ReplicationMode: node.ModeSync},
			wantType: "CP",
		},
		{
			name:     "raft => CP",
			cfg:      ClusterConfig{Strategy: node.StrategyRaft, NodeCount: 3},
			wantType: "CP",
		},
		{
			name:     "multi_leader => AP",
			cfg:      ClusterConfig{Strategy: node.StrategyMultiLeader, NodeCount: 3},
			wantType: "AP",
		},
		{
			name:     "single_leader async => AP-leaning",
			cfg:      ClusterConfig{Strategy: node.StrategySingleLeader, NodeCount: 3, ReplicationMode: node.ModeAsync},
			wantType: "AP",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyCAP(tc.cfg)
			if got.Type != tc.wantType {
				t.Errorf("ClassifyCAP().Type = %q, want %q", got.Type, tc.wantType)
			}
			if got.PACELC == "" {
				t.Errorf("ClassifyCAP().PACELC is empty")
			}
			if got.Reasoning == "" {
				t.Errorf("ClassifyCAP().Reasoning is empty")
			}
		})
	}
}

func TestClassifyCAP_LeaderlessConsistencyLeaning(t *testing.T) {
	// N=3, W=2, R=2 => W+R=4 > N=3 => overlap guaranteed => consistency-leaning (CP).
	strong := ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3, QuorumN: 3, QuorumW: 2, QuorumR: 2,
	}
	if got := ClassifyCAP(strong); got.Type != "CP" {
		t.Errorf("leaderless W+R>N: Type = %q, want CP (consistency-leaning)", got.Type)
	}

	// N=3, W=1, R=1 => W+R=2 <= N=3 => no overlap => AP.
	weak := ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3, QuorumN: 3, QuorumW: 1, QuorumR: 1,
	}
	if got := ClassifyCAP(weak); got.Type != "AP" {
		t.Errorf("leaderless W+R<=N: Type = %q, want AP", got.Type)
	}
}

func TestGradeChallenge_LeaderlessStrongVsWeak(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)

	// Strict SLA: no stale reads tolerated, must have real overlap, generous latency.
	sla := SLA{MaxStaleReadProb: 0.0, MinOverlap: 1, MaxWriteLatencyMs: 5000}

	// Strong leaderless config: N=3, W=2, R=2 => overlap=1, P(stale)=0.
	strong, err := o.CreateCluster(ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3, QuorumN: 3, QuorumW: 2, QuorumR: 2,
	})
	if err != nil {
		t.Fatalf("CreateCluster(strong): %v", err)
	}
	defer o.DeleteCluster(strong.ID)

	if _, err := o.Write(strong.ID, "", "k", []byte("v"), "client-1"); err != nil {
		t.Fatalf("Write(strong): %v", err)
	}

	gStrong, err := o.GradeChallenge(strong.ID, sla)
	if err != nil {
		t.Fatalf("GradeChallenge(strong): %v", err)
	}
	if !gStrong.Passed {
		t.Errorf("strong config should PASS strict SLA, got %+v", gStrong)
	}
	if gStrong.Score != 100 {
		t.Errorf("strong config Score = %d, want 100", gStrong.Score)
	}

	// Weak leaderless config: N=3, W=1, R=1 => no overlap, P(stale)>0.
	weak, err := o.CreateCluster(ClusterConfig{
		Strategy:  node.StrategyLeaderless,
		NodeCount: 3, QuorumN: 3, QuorumW: 1, QuorumR: 1,
	})
	if err != nil {
		t.Fatalf("CreateCluster(weak): %v", err)
	}
	defer o.DeleteCluster(weak.ID)

	if _, err := o.Write(weak.ID, "", "k", []byte("v"), "client-1"); err != nil {
		t.Fatalf("Write(weak): %v", err)
	}

	gWeak, err := o.GradeChallenge(weak.ID, sla)
	if err != nil {
		t.Fatalf("GradeChallenge(weak): %v", err)
	}
	if gWeak.Passed {
		t.Errorf("weak config (W=1,R=1) should FAIL strict SLA, got %+v", gWeak)
	}
}

func TestGradeChallenge_UnknownCluster(t *testing.T) {
	bus := events.NewEventBus(64)
	o := NewOrchestrator(bus)
	if _, err := o.GradeChallenge("does-not-exist", SLA{}); err == nil {
		t.Errorf("expected error for unknown cluster, got nil")
	}
}
