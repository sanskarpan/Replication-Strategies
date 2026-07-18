package simulation

import (
	"fmt"

	"replication-strategies/internal/node"
)

// Preset captures a real-world distributed system's replication configuration
// mapped onto this simulator's ClusterConfig, so users can spin up a cluster
// that mirrors how Cassandra, DynamoDB, PostgreSQL, etcd, or Kafka replicate.
type Preset struct {
	Name        string        `json:"name"`
	System      string        `json:"system"`
	Description string        `json:"description"`
	Config      ClusterConfig `json:"config"`
	Notes       string        `json:"notes"`
}

// sloppyOn is a reusable *bool literal for presets that enable sloppy quorum.
var sloppyOn = true

// Presets maps well-known real systems to the ClusterConfig that reproduces
// their replication behavior in the simulator.
var Presets = []Preset{
	{
		Name:        "Cassandra",
		System:      "Apache Cassandra",
		Description: "Tunable-consistency, leaderless, multi-datacenter wide-column store.",
		Config: ClusterConfig{
			Strategy:         node.StrategyLeaderless,
			NodeCount:        6,
			QuorumN:          3,
			QuorumW:          2,
			QuorumR:          2,
			Regions:          2,
			ConsistencyLevel: "local_quorum",
			ReadRepairMode:   "digest",
		},
		Notes: "Tunable quorum (LOCAL_QUORUM) across 2 DCs with digest-based read repair.",
	},
	{
		Name:        "DynamoDB",
		System:      "Amazon DynamoDB",
		Description: "Leaderless key-value store with sloppy quorum and hinted handoff.",
		Config: ClusterConfig{
			Strategy:     node.StrategyLeaderless,
			NodeCount:    6,
			QuorumN:      3,
			QuorumW:      2,
			QuorumR:      2,
			SloppyQuorum: &sloppyOn,
		},
		Notes: "Dynamo-style sloppy quorum + hinted handoff for high availability.",
	},
	{
		Name:        "PostgreSQL",
		System:      "PostgreSQL",
		Description: "Single-leader relational database with synchronous streaming replication.",
		Config: ClusterConfig{
			Strategy:        node.StrategySingleLeader,
			NodeCount:       3,
			ReplicationMode: node.ModeSync,
		},
		Notes: "Synchronous streaming replication: primary waits for a standby to confirm.",
	},
	{
		Name:        "etcd",
		System:      "etcd",
		Description: "Strongly consistent key-value store built on the Raft consensus protocol.",
		Config: ClusterConfig{
			Strategy:  node.StrategyRaft,
			NodeCount: 5,
		},
		Notes: "Raft consensus with a 5-node cluster (tolerates 2 failures).",
	},
	{
		Name:        "Kafka",
		System:      "Apache Kafka",
		Description: "Partitioned log with an ISR-style leader and asynchronous followers.",
		Config: ClusterConfig{
			Strategy:        node.StrategySingleLeader,
			NodeCount:       4,
			ReplicationMode: node.ModeAsync,
		},
		Notes: "ISR-style leader + follower replicas replicating asynchronously.",
	},
}

// ListPresets returns a copy of the available presets so callers cannot mutate
// the package-level slice.
func ListPresets() []Preset {
	out := make([]Preset, len(Presets))
	copy(out, Presets)
	return out
}

// FindPreset looks up a preset by its Name. The lookup is exact-match.
func FindPreset(name string) (*Preset, bool) {
	for i := range Presets {
		if Presets[i].Name == name {
			p := Presets[i]
			return &p, true
		}
	}
	return nil, false
}

// CreateFromPreset provisions a new cluster from a named real-system preset.
func (o *Orchestrator) CreateFromPreset(name string) (*Cluster, error) {
	preset, ok := FindPreset(name)
	if !ok {
		return nil, fmt.Errorf("preset %q not found", name)
	}
	return o.CreateCluster(preset.Config)
}
