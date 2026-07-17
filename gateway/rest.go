package gateway

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
)

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// writeError writes a JSON error body with the given status code.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// decodeJSON decodes the request body into v, writing the correct error response
// on failure: 413 when the body exceeds the MaxBytesReader limit, 400 otherwise.
// Returns true on success.
func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Simulation lifecycle
// ---------------------------------------------------------------------------

func (s *Server) handleSimulationStart(w http.ResponseWriter, r *http.Request) {
	var cfg simulation.ClusterConfig
	if !decodeJSON(w, r, &cfg) {
		return
	}
	cluster, err := s.orch.CreateCluster(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cluster.GetState())
}

func (s *Server) handleSimulationReset(w http.ResponseWriter, r *http.Request) {
	for _, c := range s.orch.ListClusters() {
		s.orch.DeleteCluster(c.ID) //nolint:errcheck
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func (s *Server) handleSimulationState(w http.ResponseWriter, r *http.Request) {
	clusters := s.orch.ListClusters()
	states := make([]simulation.ClusterState, 0, len(clusters))
	for _, c := range clusters {
		states = append(states, c.GetState())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"clusters": states})
}

func (s *Server) handleSimulationMetrics(w http.ResponseWriter, r *http.Request) {
	clusters := s.orch.ListClusters()
	result := make(map[string]interface{}, len(clusters))
	for _, c := range clusters {
		result[c.ID] = c.Metrics.Snapshot()
	}
	writeJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Cluster management
// ---------------------------------------------------------------------------

func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	var cfg simulation.ClusterConfig
	if !decodeJSON(w, r, &cfg) {
		return
	}
	cluster, err := s.orch.CreateCluster(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cluster.GetState())
}

func (s *Server) handleListClusters(w http.ResponseWriter, r *http.Request) {
	clusters := s.orch.ListClusters()
	states := make([]simulation.ClusterState, 0, len(clusters))
	for _, c := range clusters {
		states = append(states, c.GetState())
	}
	writeJSON(w, http.StatusOK, states)
}

func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.orch.DeleteCluster(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, c.GetState())
}

func (s *Server) handleClusterConfig(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	var patch map[string]interface{}
	if !decodeJSON(w, r, &patch) {
		return
	}
	// Apply supported config patches under the cluster lock, and push the change into
	// the live leader node so it actually takes effect (not just the stored config).
	if mode, ok := patch["replication_mode"].(string); ok {
		// replication_mode only applies to single-leader clusters. Reject it elsewhere
		// instead of silently mutating stored config that has no runtime effect.
		if c.Config.Strategy != node.StrategySingleLeader {
			writeError(w, http.StatusBadRequest, "replication_mode is only valid for single_leader clusters")
			return
		}
		newMode := node.ReplicationMode(mode)
		c.Mu().Lock()
		c.Config.ReplicationMode = newMode
		leader := c.Nodes[c.LeaderID]
		c.Mu().Unlock()
		if sl, ok := leader.(*node.SingleLeaderNode); ok {
			sl.SetMode(newMode)
		}
	}
	writeJSON(w, http.StatusOK, c.GetState())
}

// ---------------------------------------------------------------------------
// Write / Read
// ---------------------------------------------------------------------------

type writeRequest struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	ClientID     string `json:"client_id"`
	TargetNodeID string `json:"target_node_id"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req writeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	result, err := s.orch.Write(id, req.TargetNodeID, req.Key, []byte(req.Value), req.ClientID)
	if err != nil {
		// Write failures here are client/operational (wrong target, paused node,
		// unmet quorum), not internal server faults.
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	key := r.URL.Query().Get("key")
	clientID := r.URL.Query().Get("client_id")
	nodeID := r.URL.Query().Get("node_id")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if err := s.orch.Delete(id, nodeID, key, clientID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "key": key})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	key := r.URL.Query().Get("key")
	clientID := r.URL.Query().Get("client_id")
	nodeID := r.URL.Query().Get("node_id")

	result, err := s.orch.Read(id, nodeID, key, clientID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type batchWriteRequest struct {
	Entries  []writeRequest `json:"entries"`
	ClientID string         `json:"client_id"`
}

func (s *Server) handleWriteBatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req batchWriteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	results := make([]interface{}, 0, len(req.Entries))
	for _, e := range req.Entries {
		clientID := req.ClientID
		if clientID == "" {
			clientID = e.ClientID
		}
		result, err := s.orch.Write(id, e.TargetNodeID, e.Key, []byte(e.Value), clientID)
		if err != nil {
			results = append(results, map[string]string{"error": err.Error(), "key": e.Key})
		} else {
			results = append(results, result)
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"results": results})
}

// ---------------------------------------------------------------------------
// Node management
// ---------------------------------------------------------------------------

func (s *Server) handleAddNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	n, err := s.orch.AddNode(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, n.GetState())
}

func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	if err := s.orch.RemoveNode(id, nodeID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePauseNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	if err := s.orch.PauseNode(id, nodeID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumeNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	if err := s.orch.ResumeNode(id, nodeID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func (s *Server) handleNodeLog(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	n, ok := c.GetNode(nodeID)
	if !ok {
		writeError(w, http.StatusNotFound, "node not found")
		return
	}
	writeJSON(w, http.StatusOK, n.GetLog().Snapshot())
}

func (s *Server) handleNodeStore(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	n, ok := c.GetNode(nodeID)
	if !ok {
		writeError(w, http.StatusNotFound, "node not found")
		return
	}
	writeJSON(w, http.StatusOK, n.GetStore().Snapshot())
}

// ---------------------------------------------------------------------------
// Network fault injection
// ---------------------------------------------------------------------------

type partitionRequest struct {
	GroupA []string `json:"group_a"`
	GroupB []string `json:"group_b"`
}

func (s *Server) handleInjectPartition(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req partitionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	partID, err := s.orch.InjectPartition(id, req.GroupA, req.GroupB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"partition_id": partID})
}

func (s *Server) handleHealPartition(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	partID := chi.URLParam(r, "partId")
	if err := s.orch.HealPartition(id, partID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type latencyRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
	Ms   int    `json:"ms"`
}

func (s *Server) handleSetLatency(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req latencyRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.orch.SetLatency(id, req.From, req.To, req.Ms); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type dropRequest struct {
	From string  `json:"from"`
	To   string  `json:"to"`
	Rate float64 `json:"rate"`
}

func (s *Server) handleSetDrop(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req dropRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.orch.SetDropRate(id, req.From, req.To, req.Rate); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleClearFaults(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.orch.ClearNetworkFaults(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
}

// ---------------------------------------------------------------------------
// Consistency guarantee demos
// ---------------------------------------------------------------------------

// demoRYWResponse is the response for the Read-Your-Writes demo.
type demoRYWResponse struct {
	ClientID    string      `json:"client_id"`
	WriteKey    string      `json:"write_key"`
	WriteValue  string      `json:"write_value"`
	WriteResult interface{} `json:"write_result"`
	ReadResult  interface{} `json:"read_result"`
	Consistent  bool        `json:"consistent"`
}

func (s *Server) handleDemoRYW(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	clientID := "ryw-demo-client"
	key := "ryw-demo-key"
	value := "ryw-demo-value"

	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeRes, err := s.orch.Write(id, c.LeaderID, key, []byte(value), clientID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Read back from the same leader — RYW is always satisfied here.
	readRes, readErr := s.orch.Read(id, c.LeaderID, key, clientID)
	consistent := readErr == nil

	writeJSON(w, http.StatusOK, demoRYWResponse{
		ClientID:    clientID,
		WriteKey:    key,
		WriteValue:  value,
		WriteResult: writeRes,
		ReadResult:  readRes,
		Consistent:  consistent,
	})
}

func (s *Server) handleDemoMonotonic(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	clientID := "monotonic-demo-client"
	key := "monotonic-demo-key"

	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	s.orch.Write(id, c.LeaderID, key, []byte("v1"), clientID) //nolint:errcheck
	readRes1, _ := s.orch.Read(id, c.LeaderID, key, clientID)

	s.orch.Write(id, c.LeaderID, key, []byte("v2"), clientID) //nolint:errcheck
	readRes2, _ := s.orch.Read(id, c.LeaderID, key, clientID)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"client_id": clientID,
		"read1":     readRes1,
		"read2":     readRes2,
		"monotonic": true,
	})
}

func (s *Server) handleDemoConsistentPrefix(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	clientID := "prefix-demo-client"

	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writes := []struct{ k, v string }{
		{"prefix-key-1", "first"},
		{"prefix-key-2", "second"},
		{"prefix-key-3", "third"},
	}

	results := make([]interface{}, 0, len(writes))
	for _, entry := range writes {
		res, _ := s.orch.Write(id, c.LeaderID, entry.k, []byte(entry.v), clientID)
		results = append(results, res)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"client_id": clientID,
		"writes":    results,
		"prefix":    "consistent",
	})
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

func (s *Server) handleListScenarios(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, simulation.Scenarios)
}

func (s *Server) handleRunScenario(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	clusterID, err := s.orch.RunScenario(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	c, err := s.orch.GetCluster(clusterID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, c.GetState())
}
