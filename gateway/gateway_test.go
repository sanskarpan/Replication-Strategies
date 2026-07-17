package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/events"
	"replication-strategies/internal/simulation"
)

func newTestServer(t *testing.T) (*httptest.Server, *simulation.Orchestrator) {
	t.Helper()
	bus := events.NewEventBus(200)
	orch := simulation.NewOrchestrator(bus)
	srv := NewServer(orch, bus)
	ts := httptest.NewServer(srv.Router())
	t.Cleanup(ts.Close)
	return ts, orch
}

func doJSON(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func createCluster(t *testing.T, base, body string) string {
	t.Helper()
	code, out := doJSON(t, "POST", base+"/api/v1/clusters", body)
	require.Equal(t, http.StatusCreated, code, out)
	var st map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(out), &st))
	return st["id"].(string)
}

// #1: an over-limit request body must return 413, not 400.
func TestGateway_BodyTooLarge_Returns413(t *testing.T) {
	ts, _ := newTestServer(t)
	big := `{"key":"` + strings.Repeat("A", 2*1024*1024) + `"}`
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters", big)
	assert.Equal(t, http.StatusRequestEntityTooLarge, code)
}

// A normal small body still works (413 guard doesn't over-trigger).
func TestGateway_NormalBody_Works(t *testing.T) {
	ts, _ := newTestServer(t)
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters",
		`{"strategy":"leaderless","node_count":3}`)
	assert.Equal(t, http.StatusCreated, code)
}

// Write failures map to 409; empty key maps to 400.
func TestGateway_WriteErrors_StatusCodes(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"single_leader","node_count":3,"replication_mode":"sync"}`)

	// Empty key -> 400.
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write",
		`{"key":"","value":"v","client_id":"c"}`)
	assert.Equal(t, http.StatusBadRequest, code)

	// Write to a follower -> 409 (not 500).
	state := getState(t, ts.URL, cid)
	leader := state["leader_id"].(string)
	var follower string
	for _, n := range state["node_ids"].([]interface{}) {
		if n.(string) != leader {
			follower = n.(string)
			break
		}
	}
	code, _ = doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write",
		fmt.Sprintf(`{"key":"k","value":"v","client_id":"c","target_node_id":%q}`, follower))
	assert.Equal(t, http.StatusConflict, code)
}

// The full write -> replicate -> delete -> tombstone flow over HTTP.
func TestGateway_DeletePropagatesOverHTTP(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"single_leader","node_count":3,"replication_mode":"async"}`)
	state := getState(t, ts.URL, cid)
	leader := state["leader_id"].(string)

	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write",
		fmt.Sprintf(`{"key":"foo","value":"bar","client_id":"c","target_node_id":%q}`, leader))
	require.Equal(t, http.StatusOK, code)
	time.Sleep(150 * time.Millisecond)

	code, _ = doJSON(t, "GET", ts.URL+"/api/v1/clusters/"+cid+"/read?key=foo&client_id=c", "")
	require.Equal(t, http.StatusOK, code)

	code, _ = doJSON(t, "DELETE", ts.URL+"/api/v1/clusters/"+cid+"/kv?key=foo&client_id=c", "")
	require.Equal(t, http.StatusOK, code)
	time.Sleep(150 * time.Millisecond)

	code, _ = doJSON(t, "GET", ts.URL+"/api/v1/clusters/"+cid+"/read?key=foo&client_id=c", "")
	assert.Equal(t, http.StatusNotFound, code, "deleted key must be gone across the cluster")
}

// max_clusters is enforced at the HTTP layer.
func TestGateway_MaxClustersEnforced(t *testing.T) {
	ts, orch := newTestServer(t)
	orch.SetMaxClusters(1)
	_ = createCluster(t, ts.URL, `{"strategy":"leaderless","node_count":3}`)
	code, body := doJSON(t, "POST", ts.URL+"/api/v1/clusters", `{"strategy":"leaderless","node_count":3}`)
	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Contains(t, body, "cluster limit reached")
}

func getState(t *testing.T, base, cid string) map[string]interface{} {
	t.Helper()
	code, out := doJSON(t, "GET", base+"/api/v1/clusters/"+cid+"/state", "")
	require.Equal(t, http.StatusOK, code, out)
	var st map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(out), &st))
	return st
}

// Config PATCH of replication_mode must be rejected for non-single-leader clusters.
func TestGateway_ConfigPatchRejectsNonSingleLeader(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"leaderless","node_count":3}`)
	code, body := doJSON(t, "PATCH", ts.URL+"/api/v1/clusters/"+cid+"/config", `{"replication_mode":"sync"}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "single_leader")
}
