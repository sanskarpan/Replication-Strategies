package persistence

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSaveAndLoadCluster(t *testing.T) {
	s := openTestDB(t)

	cfg := map[string]any{"strategy": "single_leader", "node_count": 3}
	cfgJSON, _ := json.Marshal(cfg)
	nodeIDs := []string{"node-a", "node-b", "node-c"}

	require.NoError(t, s.SaveCluster("cluster-1", cfgJSON, nodeIDs, "node-a", 1000))

	records, err := s.LoadClusters()
	require.NoError(t, err)
	require.Len(t, records, 1)

	r := records[0]
	require.Equal(t, "cluster-1", r.ID)
	require.Equal(t, nodeIDs, r.NodeIDs)
	require.Equal(t, "node-a", r.LeaderID)
	require.Equal(t, int64(1000), r.CreatedAt)
}

func TestSaveClusterUpsert(t *testing.T) {
	s := openTestDB(t)

	cfg, _ := json.Marshal(map[string]any{"strategy": "raft"})
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1", "n2"}, "", 1))

	// Upsert with updated node list and leader.
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1", "n2", "n3"}, "n3", 1))

	records, err := s.LoadClusters()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, []string{"n1", "n2", "n3"}, records[0].NodeIDs)
	require.Equal(t, "n3", records[0].LeaderID)
}

func TestDeleteClusterCascades(t *testing.T) {
	s := openTestDB(t)

	cfg, _ := json.Marshal(map[string]any{"strategy": "leaderless"})
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1"}, "", 1))
	require.NoError(t, s.AppendHistoryEntry("c1", 1, []byte(`{"type":"foo"}`), nil))
	require.NoError(t, s.AppendHistoryEntry("c1", 2, []byte(`{"type":"bar"}`), []byte(`{"id":"c1"}`)))

	require.NoError(t, s.DeleteCluster("c1"))

	records, err := s.LoadClusters()
	require.NoError(t, err)
	require.Empty(t, records)

	// History should be gone (CASCADE).
	rows, err := s.LoadHistory("c1", 100)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestDeleteAllClusters(t *testing.T) {
	s := openTestDB(t)
	cfg, _ := json.Marshal(map[string]any{"strategy": "multi_leader"})
	for _, id := range []string{"c1", "c2", "c3"} {
		require.NoError(t, s.SaveCluster(id, cfg, []string{"n1"}, "", 1))
	}
	require.NoError(t, s.DeleteAllClusters())
	records, err := s.LoadClusters()
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestAppendAndLoadHistory(t *testing.T) {
	s := openTestDB(t)

	cfg, _ := json.Marshal(map[string]any{"strategy": "raft"})
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1"}, "", 1))

	evtJSON := []byte(`{"type":"entry_replicated","cluster_id":"c1"}`)
	snapJSON := []byte(`{"id":"c1"}`)

	require.NoError(t, s.AppendHistoryEntry("c1", 1, evtJSON, nil))
	require.NoError(t, s.AppendHistoryEntry("c1", 2, evtJSON, snapJSON))
	require.NoError(t, s.AppendHistoryEntry("c1", 3, evtJSON, nil))

	rows, err := s.LoadHistory("c1", 100)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, uint64(1), rows[0].Seq)
	require.Nil(t, rows[0].StateJSON)
	require.Equal(t, uint64(2), rows[1].Seq)
	require.Equal(t, snapJSON, rows[1].StateJSON)
}

func TestLoadHistoryLimitsTail(t *testing.T) {
	s := openTestDB(t)

	cfg, _ := json.Marshal(map[string]any{"strategy": "raft"})
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1"}, "", 1))

	evtJSON := []byte(`{"type":"x"}`)
	for i := uint64(1); i <= 20; i++ {
		require.NoError(t, s.AppendHistoryEntry("c1", i, evtJSON, nil))
	}

	// Ask for only the last 5.
	rows, err := s.LoadHistory("c1", 5)
	require.NoError(t, err)
	require.Len(t, rows, 5)
	// Should be the MOST RECENT 5, sorted ASC.
	require.Equal(t, uint64(16), rows[0].Seq)
	require.Equal(t, uint64(20), rows[4].Seq)
}

func TestDuplicateAppendIgnored(t *testing.T) {
	s := openTestDB(t)
	cfg, _ := json.Marshal(map[string]any{"strategy": "raft"})
	require.NoError(t, s.SaveCluster("c1", cfg, []string{"n1"}, "", 1))

	evt := []byte(`{"type":"x"}`)
	require.NoError(t, s.AppendHistoryEntry("c1", 1, evt, nil))
	// Second append with same seq must not error (INSERT OR IGNORE).
	require.NoError(t, s.AppendHistoryEntry("c1", 1, evt, nil))

	rows, _ := s.LoadHistory("c1", 100)
	require.Len(t, rows, 1)
}
