package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"replication-strategies/internal/conflict"
	"replication-strategies/internal/storage"
)

func makeConflict(localTS, remoteTS int64, localNode, remoteNode string) *conflict.Conflict {
	return &conflict.Conflict{
		ID:  "test-conflict",
		Key: "test-key",
		Local: &storage.KVEntry{
			Key: "test-key", Value: []byte("local-val"),
			Timestamp: localTS, NodeID: localNode,
			VClock: storage.VectorClock{localNode: 1},
		},
		Remote: &storage.KVEntry{
			Key: "test-key", Value: []byte("remote-val"),
			Timestamp: remoteTS, NodeID: remoteNode,
			VClock: storage.VectorClock{remoteNode: 1},
		},
		DetectedAt: time.Now(),
	}
}

func TestLWWResolver_LocalNewer(t *testing.T) {
	r := conflict.NewLWWResolver()
	c := makeConflict(200, 100, "n1", "n2")
	res := r.Resolve(c)
	assert.Equal(t, conflict.ResolverLWW, res.ResolverType)
	assert.Equal(t, "local_newer", res.Reason)
	assert.Equal(t, c.Local, res.Winner)
}

func TestLWWResolver_RemoteNewer(t *testing.T) {
	r := conflict.NewLWWResolver()
	c := makeConflict(100, 200, "n1", "n2")
	res := r.Resolve(c)
	assert.Equal(t, c.Remote, res.Winner)
	assert.Equal(t, "remote_newer", res.Reason)
}

func TestLWWResolver_TiebreakerByNodeID(t *testing.T) {
	r := conflict.NewLWWResolver()
	ts := int64(100)
	// "n2" > "n1" lexicographically → remote wins
	c := makeConflict(ts, ts, "n1", "n2")
	res := r.Resolve(c)
	assert.Equal(t, c.Remote, res.Winner)

	// "na" < "nb" → remote still wins if remote nodeID is higher
	c2 := makeConflict(ts, ts, "nb", "na")
	res2 := r.Resolve(c2)
	assert.Equal(t, c2.Local, res2.Winner, "local 'nb' > remote 'na', local should win")
}

func TestVectorClockResolver_HappensBefore(t *testing.T) {
	r := conflict.NewVectorClockResolver(nil)
	c := &conflict.Conflict{
		ID:  "vc-conflict",
		Key: "k",
		Local: &storage.KVEntry{
			Key: "k", Value: []byte("v1"),
			Timestamp: 100, NodeID: "n1",
			VClock: storage.VectorClock{"n1": 2, "n2": 1},
		},
		Remote: &storage.KVEntry{
			Key: "k", Value: []byte("v2"),
			Timestamp: 200, NodeID: "n2",
			VClock: storage.VectorClock{"n1": 1, "n2": 0},
		},
		DetectedAt: time.Now(),
	}
	// Remote VC {n1:1, n2:0} happens before local {n1:2, n2:1}
	res := r.Resolve(c)
	assert.Equal(t, c.Local, res.Winner, "local should win as remote happens-before local")
}

func TestVectorClockResolver_Concurrent_FallsBackToLWW(t *testing.T) {
	r := conflict.NewVectorClockResolver(nil)
	c := &conflict.Conflict{
		ID:  "vc-concurrent",
		Key: "k",
		Local: &storage.KVEntry{
			Key: "k", Value: []byte("v1"),
			Timestamp: 100, NodeID: "n1",
			VClock: storage.VectorClock{"n1": 2, "n2": 0},
		},
		Remote: &storage.KVEntry{
			Key: "k", Value: []byte("v2"),
			Timestamp: 200, NodeID: "n2",
			VClock: storage.VectorClock{"n1": 0, "n2": 2},
		},
		DetectedAt: time.Now(),
	}
	// Concurrent — should fall back to LWW
	res := r.Resolve(c)
	// Remote has higher timestamp (200 > 100), so LWW picks remote
	assert.Equal(t, c.Remote, res.Winner)
}

// Caveat fix: CRDT resolver must only merge values explicitly tagged crdt_type=gcounter;
// arbitrary JSON that merely contains a "counts" object is resolved as an opaque value (LWW).
func TestCRDTResolver_RequiresTypeTag(t *testing.T) {
	r := conflict.NewCRDTResolver()

	// Tagged GCounters -> merged (grow-only max per key).
	tagged := &conflict.Conflict{
		ID: "1", Key: "k",
		Local:  &storage.KVEntry{Key: "k", Value: []byte(`{"crdt_type":"gcounter","counts":{"a":2}}`), Timestamp: 100, NodeID: "n1", VClock: storage.VectorClock{"n1": 1}},
		Remote: &storage.KVEntry{Key: "k", Value: []byte(`{"crdt_type":"gcounter","counts":{"b":3}}`), Timestamp: 200, NodeID: "n2", VClock: storage.VectorClock{"n2": 1}},
	}
	res := r.Resolve(tagged)
	assert.Equal(t, "gcounter_merge", res.Reason)
	assert.Contains(t, string(res.Winner.Value), `"a":2`)
	assert.Contains(t, string(res.Winner.Value), `"b":3`)
	assert.Equal(t, "n2", res.Winner.NodeID, "merged winner NodeID must be deterministic (max)")

	// Untagged JSON that happens to have "counts" -> LWW, NOT merged (opaque value kept).
	untagged := &conflict.Conflict{
		ID: "2", Key: "k",
		Local:  &storage.KVEntry{Key: "k", Value: []byte(`{"counts":{"a":1}}`), Timestamp: 100, NodeID: "n1"},
		Remote: &storage.KVEntry{Key: "k", Value: []byte(`{"counts":{"a":9}}`), Timestamp: 200, NodeID: "n2"},
	}
	res2 := r.Resolve(untagged)
	assert.Contains(t, res2.Reason, "lww_register")
	assert.Equal(t, []byte(`{"counts":{"a":9}}`), res2.Winner.Value, "untagged value must be kept opaque, not merged")
}
