package simulation

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"replication-strategies/internal/node"
)

// ScenarioSpec is a data-driven, user-authored narrated scenario. It is loaded from
// YAML so custom demonstrations can be defined without touching Go code.
type ScenarioSpec struct {
	Name      string             `yaml:"name"`
	Strategy  string             `yaml:"strategy"`
	NodeCount int                `yaml:"node_count"`
	QuorumN   int                `yaml:"quorum_n"`
	QuorumW   int                `yaml:"quorum_w"`
	QuorumR   int                `yaml:"quorum_r"`
	Steps     []ScenarioSpecStep `yaml:"steps"`
}

// ScenarioSpecStep is one action in a ScenarioSpec timeline.
//
// Supported actions:
//
//	write        — Write Value to Key on Node (empty Node targets leader/first node)
//	read         — Read Key from Node
//	pause        — pause Node
//	resume       — resume Node
//	partition    — inject a network partition between Group and Group2
//	heal         — heal all active partitions
//	skew         — set Node's clock skew to Ms milliseconds
//	sleep        — sleep for Ms milliseconds
//	anti_entropy — run one anti-entropy round across the cluster
//	narrate      — narration-only step (no side effects)
type ScenarioSpecStep struct {
	Action    string   `yaml:"action"`
	Node      string   `yaml:"node"`
	Key       string   `yaml:"key"`
	Value     string   `yaml:"value"`
	Ms        int      `yaml:"ms"`
	Group     []string `yaml:"group"`
	Group2    []string `yaml:"group2"`
	Narration string   `yaml:"narration"`
}

// LoadScenarioSpec parses a ScenarioSpec from YAML bytes.
func LoadScenarioSpec(data []byte) (*ScenarioSpec, error) {
	var spec ScenarioSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parse scenario spec: %w", err)
	}
	return &spec, nil
}

// strategyFromSpec maps a spec strategy string onto a node.ReplicationStrategy.
func strategyFromSpec(s string) (node.ReplicationStrategy, error) {
	switch s {
	case "leaderless":
		return node.StrategyLeaderless, nil
	case "single_leader", "single-leader":
		return node.StrategySingleLeader, nil
	case "multi_leader", "multi-leader":
		return node.StrategyMultiLeader, nil
	case "raft":
		return node.StrategyRaft, nil
	default:
		return "", fmt.Errorf("unknown strategy %q", s)
	}
}

// resolveNode maps a spec node reference like "n0"/"n1" to the concrete node ID,
// returning "" for an empty or out-of-range reference (so the orchestrator falls
// back to the leader / first node).
func resolveNode(c *Cluster, ref string) string {
	if ref == "" {
		return ""
	}
	var idx int
	if _, err := fmt.Sscanf(ref, "n%d", &idx); err != nil {
		return ""
	}
	if idx < 0 || idx >= len(c.NodeIDs) {
		return ""
	}
	return c.NodeIDs[idx]
}

// resolveGroup maps a slice of spec node references to concrete node IDs.
func resolveGroup(c *Cluster, refs []string) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		if id := resolveNode(c, r); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// RunScenarioSpec creates a cluster from the spec and executes its steps in the
// background, narrating each step and ending with a convergence verdict. It returns
// the new cluster ID.
func (o *Orchestrator) RunScenarioSpec(spec *ScenarioSpec) (string, error) {
	if spec == nil {
		return "", fmt.Errorf("nil scenario spec")
	}
	strategy, err := strategyFromSpec(spec.Strategy)
	if err != nil {
		return "", err
	}

	cfg := ClusterConfig{
		Strategy:  strategy,
		NodeCount: spec.NodeCount,
		QuorumN:   spec.QuorumN,
		QuorumW:   spec.QuorumW,
		QuorumR:   spec.QuorumR,
	}

	cluster, err := o.CreateCluster(cfg)
	if err != nil {
		return "", fmt.Errorf("create cluster for spec %s: %w", spec.Name, err)
	}

	name := spec.Name
	if name == "" {
		name = "CustomScenario"
	}
	o.beginScenario(cluster.ID, name)
	go o.runScenarioSpecSteps(cluster.ID, spec)

	return cluster.ID, nil
}

// runScenarioSpecSteps executes the spec's steps against the cluster.
func (o *Orchestrator) runScenarioSpecSteps(clusterID string, spec *ScenarioSpec) {
	// Give nodes a moment to come online before driving traffic.
	time.Sleep(200 * time.Millisecond)

	c, err := o.GetCluster(clusterID)
	if err != nil {
		return
	}

	for _, step := range spec.Steps {
		if step.Narration != "" {
			o.narrate(clusterID, step.Narration)
		}

		switch step.Action {
		case "write":
			o.Write(clusterID, resolveNode(c, step.Node), step.Key, []byte(step.Value), "spec-client") //nolint:errcheck
		case "read":
			o.Read(clusterID, resolveNode(c, step.Node), step.Key, "spec-client") //nolint:errcheck
		case "pause":
			o.PauseNode(clusterID, resolveNode(c, step.Node)) //nolint:errcheck
		case "resume":
			o.ResumeNode(clusterID, resolveNode(c, step.Node)) //nolint:errcheck
		case "partition":
			o.InjectPartition(clusterID, resolveGroup(c, step.Group), resolveGroup(c, step.Group2)) //nolint:errcheck
		case "heal":
			for partID := range c.Fabric.GetPartitions() {
				o.HealPartition(clusterID, partID) //nolint:errcheck
			}
		case "skew":
			o.SetClockSkew(clusterID, resolveNode(c, step.Node), int64(step.Ms)) //nolint:errcheck
		case "sleep", "pause_ms":
			time.Sleep(time.Duration(step.Ms) * time.Millisecond)
		case "anti_entropy":
			o.RunAntiEntropy(clusterID) //nolint:errcheck
		case "narrate":
			// narration already emitted above; no side effect.
		}
	}

	conv := c.CheckConvergence()
	o.verdict(clusterID, "cluster converges", convergenceActual(conv), conv.Converged)
}
