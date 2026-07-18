package gateway

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"replication-strategies/internal/simulation"
)

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
