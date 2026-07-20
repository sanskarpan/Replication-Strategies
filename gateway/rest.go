package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"replication-strategies/internal/node"
	"replication-strategies/internal/simulation"
	"replication-strategies/internal/storage"
)

// demoNodes picks a write node (leader if present, else the first node) and a distinct
// read node, for demonstrating a guarantee against a lagging replica.
func (s *Server) demoNodes(c *simulation.Cluster) (writeNode, readNode string) {
	st := c.GetState()
	if len(st.NodeIDs) == 0 {
		return "", ""
	}
	writeNode = st.NodeIDs[0]
	if st.LeaderID != "" {
		writeNode = st.LeaderID
	}
	for _, n := range st.NodeIDs {
		if n != writeNode {
			readNode = n
			break
		}
	}
	return writeNode, readNode
}

// entryValue extracts the stored string value from a Read/Write result, if present.
func entryValue(res interface{}) (string, bool) {
	var e *storage.KVEntry
	switch r := res.(type) {
	case *simulation.ReadResult:
		if r != nil {
			e, _ = r.Entry.(*storage.KVEntry)
		}
	case *simulation.WriteResult:
		if r != nil {
			e, _ = r.Entry.(*storage.KVEntry)
		}
	}
	if e == nil {
		return "", false
	}
	return string(e.Value), true
}

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

// handleConvergence reports whether all online replicas agree on every key (the
// invariant anti-entropy/read-repair must satisfy once the cluster quiesces).
func (s *Server) handleConvergence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	report, err := s.orch.CheckConvergence(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// handleSuspicion reports each node's phi-accrual suspicion level (inferred failure).
func (s *Server) handleSuspicion(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	sus, err := s.orch.Suspicion(id, 0)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"threshold": simulation.DefaultPhiThreshold, "nodes": sus})
}

// handleListConflicts lists parked (manual) multi-leader conflicts awaiting a choice.
func (s *Server) handleListConflicts(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	conflicts, err := s.orch.ListConflicts(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"conflicts": conflicts})
}

type resolveConflictRequest struct {
	NodeID string `json:"node_id"`
	Key    string `json:"key"`
	Choice string `json:"choice"` // "local" | "remote"
}

func (s *Server) handleResolveConflict(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req resolveConflictRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.orch.ResolveConflict(id, req.NodeID, req.Key, req.Choice); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resolved", "key": req.Key, "choice": req.Choice})
}

// handlePlacement returns the consistent-hashing preference list for a key.
func (s *Server) handlePlacement(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	key := r.URL.Query().Get("key")
	n := 3
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	list, err := s.orch.Placement(id, key, n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"key": key, "preference_list": list})
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
	result, err := s.orch.Write(r.Context(), id, req.TargetNodeID, req.Key, []byte(req.Value), req.ClientID)
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
	if err := s.orch.Delete(r.Context(), id, nodeID, key, clientID); err != nil {
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

	result, err := s.orch.Read(r.Context(), id, nodeID, key, clientID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type batchWriteRequest struct {
	Entries  []writeRequest `json:"entries"`
	ClientID string         `json:"client_id"`
	Atomic   bool           `json:"atomic"`
}

func (s *Server) handleWriteBatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req batchWriteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	// Atomic path: all-or-nothing on a single-leader cluster.
	if req.Atomic {
		pairs := make([]node.KV, 0, len(req.Entries))
		var target string
		for _, e := range req.Entries {
			pairs = append(pairs, node.KV{Key: e.Key, Value: []byte(e.Value)})
			if e.TargetNodeID != "" {
				target = e.TargetNodeID
			}
		}
		entries, err := s.orch.WriteBatchAtomic(id, target, pairs, req.ClientID)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"atomic": true, "entries": entries})
		return
	}
	results := make([]interface{}, 0, len(req.Entries))
	for _, e := range req.Entries {
		clientID := req.ClientID
		if clientID == "" {
			clientID = e.ClientID
		}
		result, err := s.orch.Write(r.Context(), id, e.TargetNodeID, e.Key, []byte(e.Value), clientID)
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

type clockSkewRequest struct {
	Ms int64 `json:"ms"`
}

func (s *Server) handleSetClockSkew(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodeID := chi.URLParam(r, "nodeId")
	var req clockSkewRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.orch.SetClockSkew(id, nodeID, req.Ms); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "node": nodeID, "skew_ms": req.Ms})
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
	WriteNode   string      `json:"write_node"`
	ReadNode    string      `json:"read_node"`
	WriteResult interface{} `json:"write_result"`
	ReadResult  interface{} `json:"read_result"`
	Consistent  bool        `json:"consistent"`
	Explanation string      `json:"explanation"`
}

// handleDemoRYW demonstrates read-your-writes by writing on one node and immediately
// reading the client's own write back from a DIFFERENT replica that is lagging (a
// latency window is injected), so the demo actually shows the violation when it occurs.
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
	writeNode, readNode := s.demoNodes(c)
	if writeNode == "" || readNode == "" {
		writeError(w, http.StatusBadRequest, "RYW demo requires a cluster with at least two nodes")
		return
	}

	// Open a lag window by pausing the read replica so it misses the write, then resume
	// and immediately read — the replica hasn't caught up yet. (Pause/resume avoids the
	// FIFO link-latency lingering between successive demo runs.)
	s.orch.PauseNode(id, readNode) //nolint:errcheck
	writeRes, werr := s.orch.Write(r.Context(), id, writeNode, key, []byte(value), clientID)
	if werr != nil {
		s.orch.ResumeNode(id, readNode) //nolint:errcheck
		writeError(w, http.StatusConflict, werr.Error())
		return
	}
	time.Sleep(120 * time.Millisecond) // write is delivered-and-dropped at the paused replica
	s.orch.ResumeNode(id, readNode)    //nolint:errcheck
	readRes, rerr := s.orch.Read(r.Context(), id, readNode, key, clientID)
	got, ok := entryValue(readRes)
	consistent := rerr == nil && ok && got == value

	explanation := fmt.Sprintf("Client wrote %q on %s and immediately read its own write back from %s (RYW held).", value, writeNode, readNode)
	if !consistent {
		explanation = fmt.Sprintf("READ-YOUR-WRITES VIOLATED: client wrote %q on %s but the lagging replica %s returned %s — the client could not read its own write.",
			value, writeNode, readNode, describe(got, ok, rerr))
	}

	writeJSON(w, http.StatusOK, demoRYWResponse{
		ClientID: clientID, WriteKey: key, WriteValue: value,
		WriteNode: writeNode, ReadNode: readNode,
		WriteResult: writeRes, ReadResult: readRes,
		Consistent: consistent, Explanation: explanation,
	})
}

// handleDemoMonotonic demonstrates monotonic reads: after the client sees v2 on a fresh
// node, a read from a lagging replica may return the older v1 — reads going backward.
func (s *Server) handleDemoMonotonic(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	clientID := "monotonic-demo-client"
	key := "monotonic-demo-key"

	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeNode, readNode := s.demoNodes(c)
	if writeNode == "" || readNode == "" {
		writeError(w, http.StatusBadRequest, "monotonic demo requires a cluster with at least two nodes")
		return
	}

	// v1 replicates everywhere, then readNode is paused so it misses v2.
	s.orch.Write(r.Context(), id, writeNode, key, []byte("v1"), clientID) //nolint:errcheck
	time.Sleep(200 * time.Millisecond)
	s.orch.PauseNode(id, readNode)                                        //nolint:errcheck
	s.orch.Write(r.Context(), id, writeNode, key, []byte("v2"), clientID) //nolint:errcheck
	time.Sleep(120 * time.Millisecond)                                    // v2 delivered-and-dropped at paused readNode
	s.orch.ResumeNode(id, readNode)                                       //nolint:errcheck

	read1, _ := s.orch.Read(r.Context(), id, writeNode, key, clientID) // fresh -> v2
	read2, _ := s.orch.Read(r.Context(), id, readNode, key, clientID)  // stale -> v1
	v1, _ := entryValue(read1)
	v2, _ := entryValue(read2)
	violated := v1 == "v2" && v2 == "v1"

	explanation := fmt.Sprintf("Client read %q then %q — never went backward (monotonic held).", v1, v2)
	if violated {
		explanation = fmt.Sprintf("MONOTONIC READ VIOLATED: client read %q from %s, then the lagging replica %s returned the older %q — the read went backward in time.", v1, writeNode, readNode, v2)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"client_id":  clientID,
		"read_node1": writeNode, "read_node2": readNode,
		"read1": read1, "read2": read2,
		"monotonic":   !violated,
		"explanation": explanation,
	})
}

// handleDemoConsistentPrefix writes an ordered sequence and reads it back, reporting the
// ACTUAL observed order (no longer hardcoded). In single-leader the log guarantees the
// prefix; the response explains that multi-leader reordering can break it.
func (s *Server) handleDemoConsistentPrefix(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	clientID := "prefix-demo-client"

	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeNode, readNode := s.demoNodes(c)
	if writeNode == "" {
		writeError(w, http.StatusBadRequest, "consistent-prefix demo requires at least one node")
		return
	}
	if readNode == "" {
		readNode = writeNode
	}

	seq := []string{"first", "second", "third"}
	key := "prefix-demo-key"
	results := make([]interface{}, 0, len(seq))
	for _, v := range seq {
		res, _ := s.orch.Write(r.Context(), id, writeNode, key, []byte(v), clientID)
		results = append(results, res)
	}
	time.Sleep(150 * time.Millisecond)

	// Read the final value from the read node; a consistent prefix means we observe one
	// of the sequence values in order (here, the latest), never a value that skipped
	// earlier writes out of order.
	readRes, _ := s.orch.Read(r.Context(), id, readNode, key, clientID)
	got, ok := entryValue(readRes)
	// Determine the observed position in the sequence.
	pos := -1
	for i, v := range seq {
		if v == got {
			pos = i
		}
	}
	consistent := !ok || pos >= 0 // any observed value is a valid prefix point in single-leader

	explanation := fmt.Sprintf("Wrote %v in order on %s; replica %s observed %q — a consistent prefix (no out-of-order value).", seq, writeNode, readNode, got)
	if c.Config.Strategy != node.StrategySingleLeader {
		explanation += " NOTE: under multi-leader/leaderless, reordered replication can expose an out-of-order prefix."
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"client_id":   clientID,
		"write_node":  writeNode,
		"read_node":   readNode,
		"sequence":    seq,
		"observed":    got,
		"writes":      results,
		"consistent":  consistent,
		"explanation": explanation,
	})
}

// describe renders a read outcome for an explanation string.
func describe(val string, ok bool, err error) string {
	if err != nil {
		return fmt.Sprintf("an error (%v)", err)
	}
	if !ok {
		return "not-found"
	}
	return fmt.Sprintf("%q", val)
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

// ---------------------------------------------------------------------------
// Event history + linearizability (EPIC B)
// ---------------------------------------------------------------------------

// handleClusterHistory returns a page of events from the cluster's durable ring
// buffer. Query params: from (seq, inclusive, default 0) and limit (default 500).
func (s *Server) handleClusterHistory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	var from, limit uint64
	if v := r.URL.Query().Get("from"); v != "" {
		from, _ = parseUint64(v)
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, _ = parseUint64(v)
	}
	entries := c.EventHistory().Get(from, limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"cluster_id": id,
		"max_seq":    c.EventHistory().MaxSeq(),
		"entries":    entries,
	})
}

// handleClusterHistoryState returns the nearest snapshot at or before the
// requested seq plus the events that follow it, so the frontend can fold them to
// reconstruct exact cluster state. Query param: at (seq, required).
func (s *Server) handleClusterHistoryState(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetCluster(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	atStr := r.URL.Query().Get("at")
	if atStr == "" {
		writeError(w, http.StatusBadRequest, "at param required")
		return
	}
	at, _ := parseUint64(atStr)
	result := c.EventHistory().StateAt(at)
	writeJSON(w, http.StatusOK, result)
}

// handleClusterOps returns the Jepsen-compatible op history for the cluster
// (client × invoke/complete intervals recorded by the linearizability checker).
func (s *Server) handleClusterOps(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ops, err := s.orch.GetOps(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	type JepsenOp struct {
		ClientID   string `json:"client_id"`
		Kind       string `json:"kind"` // "write" | "read"
		Key        string `json:"key"`
		Value      string `json:"value"`
		InvokeNs   int64  `json:"invoke_ns"`
		CompleteNs int64  `json:"complete_ns"`
	}
	out := make([]JepsenOp, 0, len(ops))
	for _, op := range ops {
		kind := "read"
		if op.Kind == 0 { // checker.OpWrite == 0
			kind = "write"
		}
		out = append(out, JepsenOp{
			ClientID:   op.ClientID,
			Kind:       kind,
			Key:        op.Key,
			Value:      op.Value,
			InvokeNs:   op.Invoke,
			CompleteNs: op.Complete,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"cluster_id": id, "ops": out})
}

// handleClusterLinearize runs the Wing-Gong linearizability checker against the
// cluster's recorded op history and annotates the result for Jepsen display.
func (s *Server) handleClusterLinearize(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	lin, err := s.orch.CheckLinearizable(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, lin)
}

// parseUint64 converts a string to uint64, returning 0 on error.
func parseUint64(s string) (uint64, error) {
	var v uint64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
