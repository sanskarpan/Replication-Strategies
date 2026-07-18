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
	srv := NewServer(orch, bus, nil)
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

// §0.1: the configured CORS allow-list must be honored (echo allowed origins, reject others).
func TestGateway_CORS_AllowList(t *testing.T) {
	bus := events.NewEventBus(50)
	orch := simulation.NewOrchestrator(bus)
	srv := NewServer(orch, bus, []string{"http://localhost:3001"})
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	get := func(origin string) string {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/clusters", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.Header.Get("Access-Control-Allow-Origin")
	}
	assert.Equal(t, "http://localhost:3001", get("http://localhost:3001"), "allowed origin echoed")
	assert.Equal(t, "", get("http://evil.example"), "disallowed origin gets no ACAO")

	// nil list => permissive default
	srv2 := NewServer(orch, bus, nil)
	ts2 := httptest.NewServer(srv2.Router())
	defer ts2.Close()
	req, _ := http.NewRequest("GET", ts2.URL+"/api/v1/clusters", nil)
	req.Header.Set("Origin", "http://anything")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}

// §0.5: the consistency demos must show REAL violations against a lagging replica,
// not a hardcoded success — and must work back-to-back on the same cluster.
func TestGateway_ConsistencyDemos_ShowViolations(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"single_leader","node_count":3,"replication_mode":"async"}`)

	// Read-your-writes: reading the client's own write from a lagging replica fails.
	code, body := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/demo/read-your-writes", "")
	require.Equal(t, http.StatusOK, code, body)
	var ryw map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &ryw))
	assert.Equal(t, false, ryw["consistent"], "RYW must be violated on a lagging replica")
	assert.Contains(t, ryw["explanation"], "VIOLATED")

	// Monotonic reads: the second read goes backward (v2 then v1).
	code, body = doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/demo/monotonic-reads", "")
	require.Equal(t, http.StatusOK, code, body)
	var mono map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &mono))
	assert.Equal(t, false, mono["monotonic"], "monotonic read must be violated on a lagging replica")

	// Consistent prefix reports the actual observed value (not hardcoded).
	code, body = doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/demo/consistent-prefix", "")
	require.Equal(t, http.StatusOK, code, body)
	var pfx map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &pfx))
	assert.NotNil(t, pfx["observed"])
}

// §1: the convergence endpoint reports agreement across online replicas.
func TestGateway_Convergence(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"single_leader","node_count":3,"replication_mode":"async"}`)
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write", `{"key":"k","value":"v","client_id":"c"}`)
	require.Equal(t, http.StatusOK, code)
	time.Sleep(300 * time.Millisecond)
	code, body := doJSON(t, "GET", ts.URL+"/api/v1/clusters/"+cid+"/convergence", "")
	require.Equal(t, http.StatusOK, code)
	var rep map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &rep))
	assert.Equal(t, true, rep["converged"])
}

// §1: the new correctness-checker, anti-entropy, reconfigure, and demo endpoints route.
func TestGateway_LinearizableAndInvariants(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"single_leader","node_count":3,"replication_mode":"sync"}`)
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write", `{"key":"k","value":"v","client_id":"c"}`)
	require.Equal(t, http.StatusOK, code)
	code, body := doJSON(t, "GET", ts.URL+"/api/v1/clusters/"+cid+"/linearizable", "")
	require.Equal(t, http.StatusOK, code, body)
	var lin map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &lin))
	assert.Equal(t, true, lin["linearizable"])

	code, body = doJSON(t, "GET", ts.URL+"/api/v1/clusters/"+cid+"/invariants", "")
	require.Equal(t, http.StatusOK, code, body)
	var inv map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &inv))
	assert.Equal(t, true, inv["ok"])
}

func TestGateway_AntiEntropyAndReconfigure(t *testing.T) {
	ts, _ := newTestServer(t)
	cid := createCluster(t, ts.URL, `{"strategy":"leaderless","node_count":3,"quorum_w":2,"quorum_r":2}`)
	code, _ := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/write", `{"key":"k","value":"v","client_id":"c"}`)
	require.Equal(t, http.StatusOK, code)
	time.Sleep(150 * time.Millisecond)

	code, body := doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/anti-entropy", "")
	require.Equal(t, http.StatusOK, code, body)

	code, body = doJSON(t, "POST", ts.URL+"/api/v1/clusters/"+cid+"/reconfigure/add-node", "")
	require.Equal(t, http.StatusOK, code, body)
	var rc map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &rc))
	assert.Equal(t, true, rc["overlap_held"])
}

func TestGateway_PrimitiveDemos(t *testing.T) {
	ts, _ := newTestServer(t)
	for _, path := range []string{"2pc?crash=true", "mvcc", "wal?mode=buffered", "swim", "paxos", "detsim?seed=7"} {
		code, body := doJSON(t, "GET", ts.URL+"/api/v1/demos/"+path, "")
		require.Equal(t, http.StatusOK, code, "demo %s: %s", path, body)
		assert.NotEmpty(t, body)
	}
}
