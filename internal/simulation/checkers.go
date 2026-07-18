package simulation

import (
	"fmt"

	"replication-strategies/internal/checker"
)

// LinearizabilityReport is the result of checking a cluster's recorded op history
// against a linearizable single-register model.
type LinearizabilityReport struct {
	ClusterID    string `json:"cluster_id"`
	Ops          int    `json:"ops"`
	Linearizable bool   `json:"linearizable"`
	// Violation describes the operation that could not be linearized (nil when ok).
	Violation *ViolationOp `json:"violation,omitempty"`
	Note      string       `json:"note,omitempty"`
}

// ViolationOp is a JSON-friendly view of a non-linearizable operation.
type ViolationOp struct {
	ClientID string `json:"client_id"`
	Kind     string `json:"kind"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

// CheckLinearizable runs the Porcupine-style checker over the cluster's op history and
// pinpoints a violating operation when the history is not linearizable.
func (o *Orchestrator) CheckLinearizable(clusterID string) (LinearizabilityReport, error) {
	c, err := o.GetCluster(clusterID)
	if err != nil {
		return LinearizabilityReport{}, err
	}
	ops := c.history.Ops()
	rep := LinearizabilityReport{ClusterID: clusterID, Ops: len(ops)}
	if len(ops) == 0 {
		rep.Linearizable = true
		rep.Note = "no operations recorded yet"
		return rep, nil
	}
	ok, bad := checker.CheckRegister(ops)
	rep.Linearizable = ok
	if !ok && bad != nil {
		rep.Violation = &ViolationOp{
			ClientID: bad.ClientID,
			Kind:     kindName(bad.Kind),
			Key:      bad.Key,
			Value:    bad.Value,
		}
	}
	return rep, nil
}

func kindName(k checker.OpKind) string {
	if k == checker.OpWrite {
		return "write"
	}
	return "read"
}

// InvariantReport is an always-on correctness snapshot: convergence after quiesce plus
// linearizability of the observed history. Violations lists any failed invariant.
type InvariantReport struct {
	ClusterID    string   `json:"cluster_id"`
	Converged    bool     `json:"converged"`
	Linearizable bool     `json:"linearizable"`
	OK           bool     `json:"ok"`
	Violations   []string `json:"violations,omitempty"`
}

// CheckInvariants evaluates the continuous invariants for a cluster: (1) all online
// replicas agree on every key, and (2) the observed client history is linearizable.
func (o *Orchestrator) CheckInvariants(clusterID string) (InvariantReport, error) {
	conv, err := o.CheckConvergence(clusterID)
	if err != nil {
		return InvariantReport{}, err
	}
	lin, err := o.CheckLinearizable(clusterID)
	if err != nil {
		return InvariantReport{}, err
	}
	rep := InvariantReport{
		ClusterID:    clusterID,
		Converged:    conv.Converged,
		Linearizable: lin.Linearizable,
	}
	if !conv.Converged {
		rep.Violations = append(rep.Violations, fmt.Sprintf("replicas diverge on %d key(s)", len(conv.Diverged)))
	}
	if !lin.Linearizable {
		if lin.Violation != nil {
			rep.Violations = append(rep.Violations, fmt.Sprintf("non-linearizable %s of key %q (value %q)", lin.Violation.Kind, lin.Violation.Key, lin.Violation.Value))
		} else {
			rep.Violations = append(rep.Violations, "history is not linearizable")
		}
	}
	rep.OK = len(rep.Violations) == 0
	return rep, nil
}
