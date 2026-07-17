package unit

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/conflict"
	"replication-strategies/internal/storage"
)

func resolveCRDT(t *testing.T, localVal, remoteVal string) (*conflict.Resolution, string) {
	t.Helper()
	r := conflict.NewCRDTResolver()
	c := &conflict.Conflict{
		ID: "c", Key: "k",
		Local:  &storage.KVEntry{Key: "k", Value: []byte(localVal), Timestamp: 1, NodeID: "n1", VClock: storage.VectorClock{"n1": 1}},
		Remote: &storage.KVEntry{Key: "k", Value: []byte(remoteVal), Timestamp: 2, NodeID: "n2", VClock: storage.VectorClock{"n2": 1}},
	}
	res := r.Resolve(c)
	return res, string(res.Winner.Value)
}

func TestPNCounter_Merge(t *testing.T) {
	res, val := resolveCRDT(t,
		`{"crdt_type":"pncounter","p":{"n1":5},"n":{"n1":1}}`,
		`{"crdt_type":"pncounter","p":{"n1":3,"n2":4},"n":{"n2":2}}`)
	assert.Equal(t, "pncounter_merge", res.Reason)
	var pn conflict.PNCounter
	require.NoError(t, json.Unmarshal([]byte(val), &pn))
	// P = max per node {n1:5,n2:4}=9 ; N = {n1:1,n2:2}=3 -> value 6
	assert.Equal(t, int64(6), pn.Value())
}

func TestORSet_AddWinsNoResurrectionAnomaly(t *testing.T) {
	// local adds "x" with tag t1; remote removed a DIFFERENT tag but concurrently re-added x with t2.
	_, val := resolveCRDT(t,
		`{"crdt_type":"orset","adds":{"x":["t1"]},"removes":{"t1":true}}`,
		`{"crdt_type":"orset","adds":{"x":["t2"]},"removes":{}}`)
	var s conflict.ORSet
	require.NoError(t, json.Unmarshal([]byte(val), &s))
	// t1 removed but t2 add is live -> x present (concurrent add wins).
	assert.Equal(t, []string{"x"}, s.Elements())
}

func TestLWWMap_Merge(t *testing.T) {
	_, val := resolveCRDT(t,
		`{"crdt_type":"lwwmap","entries":{"a":{"value":"1","timestamp":10,"node_id":"n1"}}}`,
		`{"crdt_type":"lwwmap","entries":{"a":{"value":"2","timestamp":20,"node_id":"n2"},"b":{"value":"9","timestamp":5,"node_id":"n2"}}}`)
	var m conflict.LWWMap
	require.NoError(t, json.Unmarshal([]byte(val), &m))
	assert.Equal(t, "2", m.Entries["a"].Value, "higher-timestamp entry wins per key")
	assert.Equal(t, "9", m.Entries["b"].Value, "keys only on one side are kept")
}

// Convergence: merge must be commutative and idempotent so all replicas agree.
func TestCRDT_MergeIsCommutativeAndIdempotent(t *testing.T) {
	a := conflict.PNCounter{Type: "pncounter", P: map[string]uint64{"n1": 5, "n2": 1}, N: map[string]uint64{"n1": 2}}
	b := conflict.PNCounter{Type: "pncounter", P: map[string]uint64{"n2": 4}, N: map[string]uint64{"n2": 3}}
	ab := a.Merge(&b)
	ba := b.Merge(&a)
	assert.Equal(t, ab.Value(), ba.Value(), "commutative")
	assert.Equal(t, ab.Value(), ab.Merge(ab).Value(), "idempotent")
}

// Untagged JSON is never CRDT-merged.
func TestCRDT_UntaggedFallsBackToLWW(t *testing.T) {
	res, _ := resolveCRDT(t, `{"p":{"n1":1}}`, `{"p":{"n1":9}}`)
	assert.Contains(t, res.Reason, "lww_register")
}
