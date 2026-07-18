package gateway

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"replication-strategies/internal/simulation"
)

// handleRunScenarioYAML runs a user-supplied YAML scenario spec.
func (s *Server) handleRunScenarioYAML(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	spec, err := simulation.LoadScenarioSpec(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid scenario YAML: "+err.Error())
		return
	}
	clusterID, err := s.orch.RunScenarioSpec(spec)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"cluster_id": clusterID, "scenario": spec.Name})
}

// handleExportReport returns a full JSON snapshot of a cluster (config, state, metrics,
// convergence, invariants, scenario) for reproducible sharing/reporting.
func (s *Server) handleExportReport(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	report, err := s.orch.ExportReport(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	report.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	w.Header().Set("Content-Disposition", "attachment; filename=\"cluster-report.json\"")
	writeJSON(w, http.StatusOK, report)
}

// handleStrategyRace runs the same workload into clusters of different strategies and
// returns a side-by-side comparison.
func (s *Server) handleStrategyRace(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Strategies []string `json:"strategies"`
		NodeCount  int      `json:"node_count"`
		Ops        int      `json:"ops"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Strategies) == 0 {
		req.Strategies = []string{"single_leader", "leaderless", "raft"}
	}
	report, err := s.orch.RunStrategyRace(req.Strategies, req.NodeCount, req.Ops)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// handleLinearizable checks the cluster's recorded op history against a linearizable
// register model and reports any violating operation.
func (s *Server) handleLinearizable(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rep, err := s.orch.CheckLinearizable(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// handleInvariants reports the always-on invariants (convergence + linearizability).
func (s *Server) handleInvariants(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rep, err := s.orch.CheckInvariants(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// handleAntiEntropy runs a Merkle-tree anti-entropy round and reports the divergent keys
// exchanged plus whether the cluster converged.
func (s *Server) handleAntiEntropy(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rep, err := s.orch.RunAntiEntropy(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// handleGlossary returns the DDIA-mapped glossary of distributed-systems terms.
func (s *Server) handleGlossary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.ListGlossary())
}

// handleLessons returns the guided predict-then-reveal lessons.
func (s *Server) handleLessons(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.ListLessons())
}

// handleListPresets returns the real-system configuration presets.
func (s *Server) handleListPresets(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.ListPresets())
}

// handleCreateFromPreset provisions a cluster from a named real-system preset.
func (s *Server) handleCreateFromPreset(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	cluster, err := s.orch.CreateFromPreset(name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cluster.GetState())
}

// handleCAP classifies a cluster on the CAP/PACELC spectrum.
func (s *Server) handleCAP(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, simulation.ClassifyCAP(c.Config))
}

// handleChallenge grades a cluster against a target SLA (Challenge Mode).
func (s *Server) handleChallenge(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var sla simulation.SLA
	if !decodeJSON(w, r, &sla) {
		return
	}
	grade, err := s.orch.GradeChallenge(id, sla)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, grade)
}

// handleScenarioResult returns the narrated timeline + expected-vs-actual verdict for a
// cluster's most recent scenario run.
func (s *Server) handleScenarioResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	res, ok := s.orch.ScenarioResult(id)
	if !ok {
		writeError(w, http.StatusNotFound, "no scenario has been run for this cluster")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSafeAddNode performs a safe two-phase leaderless membership change (no
// quorum-overlap gap) and reports the transition.
func (s *Server) handleSafeAddNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rep, err := s.orch.SafeAddNode(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// ---------------------------------------------------------------------------
// Standalone primitive demos (no cluster required)
// ---------------------------------------------------------------------------

func (s *Server) handleDemoTwoPC(w http.ResponseWriter, r *http.Request) {
	crash := r.URL.Query().Get("crash") == "true"
	writeJSON(w, http.StatusOK, simulation.RunTwoPCDemo(crash))
}

func (s *Server) handleDemoMVCC(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.RunMVCCDemo())
}

func (s *Server) handleDemoWAL(w http.ResponseWriter, r *http.Request) {
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "buffered"
	}
	writeJSON(w, http.StatusOK, simulation.RunWALDemo(mode))
}

func (s *Server) handleDemoSWIM(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.RunSWIMDemo())
}

func (s *Server) handleDemoPaxos(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.RunPaxosDemo())
}

func (s *Server) handleDemoDetSim(w http.ResponseWriter, r *http.Request) {
	seed := int64(42)
	if q := r.URL.Query().Get("seed"); q != "" {
		if v, err := strconv.ParseInt(q, 10, 64); err == nil {
			seed = v
		}
	}
	writeJSON(w, http.StatusOK, simulation.RunDetSimDemo(seed))
}
