package simulation

import (
	"replication-strategies/internal/metrics"
)

// ClusterReport is a full, JSON-serialisable snapshot of a cluster: its config and
// live state, current metrics, and the outcome of the always-on correctness checks
// (convergence, invariants, linearizability) plus any scenario result attached to it.
//
// GeneratedAt is intentionally left blank here so the assembly stays deterministic and
// side-effect free; the caller (e.g. the HTTP layer) stamps it with the wall clock.
type ClusterReport struct {
	GeneratedAt string                  `json:"generated_at"`
	Config      ClusterConfig           `json:"config"`
	State       ClusterState            `json:"state"`
	Metrics     metrics.ClusterSnapshot `json:"metrics"`
	Convergence ConvergenceReport       `json:"convergence"`
	Invariants  InvariantReport         `json:"invariants"`
	Scenario    *ScenarioResult         `json:"scenario,omitempty"`
}

// ExportReport assembles a complete ClusterReport for the given cluster by combining
// its config/state, a metrics snapshot, and the convergence/invariant checks. If a
// scenario has been run against the cluster its result is attached, otherwise nil.
//
// GeneratedAt is left empty; the caller is expected to set it (see ClusterReport docs).
func (o *Orchestrator) ExportReport(clusterID string) (*ClusterReport, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return nil, err
	}

	conv, err := o.CheckConvergence(clusterID)
	if err != nil {
		return nil, err
	}
	inv, err := o.CheckInvariants(clusterID)
	if err != nil {
		return nil, err
	}

	report := &ClusterReport{
		Config:      c.Config,
		State:       c.GetState(),
		Metrics:     c.Metrics.Snapshot(),
		Convergence: conv,
		Invariants:  inv,
	}
	if scen, ok := o.ScenarioResult(clusterID); ok {
		report.Scenario = scen
	}
	return report, nil
}
