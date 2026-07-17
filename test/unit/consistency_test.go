package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/consistency"
	"replication-strategies/internal/storage"
)

func entry(key string, vc storage.VectorClock) *storage.KVEntry {
	return &storage.KVEntry{Key: key, VClock: vc}
}

// ISSUE-11: RYW is now vector-clock based, so it is correct across nodes. A read must
// causally dominate the client's last write regardless of which replica served it.
func TestRYW_VectorClock_CrossNode(t *testing.T) {
	ryw := consistency.NewReadYourWrites()
	// Client writes; the write carries vclock {A:2} (e.g. leader A's 2nd write).
	ryw.RecordWrite("c1", entry("k", storage.VectorClock{"A": 2}))

	// A read from another replica that includes {A:2} (plus its own history) is valid.
	assert.NoError(t, ryw.ValidateRead("c1", entry("k", storage.VectorClock{"A": 2, "B": 5})))

	// A stale read that predates the client's write is a violation.
	assert.Error(t, ryw.ValidateRead("c1", entry("k", storage.VectorClock{"A": 1})))

	// A concurrent read that doesn't include the client's write is a violation.
	assert.Error(t, ryw.ValidateRead("c1", entry("k", storage.VectorClock{"B": 9})))

	// Different key / different client are unaffected.
	assert.NoError(t, ryw.ValidateRead("c1", entry("other", storage.VectorClock{"A": 1})))
	assert.NoError(t, ryw.ValidateRead("c2", entry("k", storage.VectorClock{"A": 1})))
}

// ISSUE-11: monotonic reads never go causally backward.
func TestMonotonic_VectorClock(t *testing.T) {
	m := consistency.NewMonotonicReads()
	m.RecordRead("c1", entry("k", storage.VectorClock{"A": 2}))

	// Newer or concurrent-forward reads are fine.
	assert.NoError(t, m.ValidateRead("c1", entry("k", storage.VectorClock{"A": 3})))
	assert.NoError(t, m.ValidateRead("c1", entry("k", storage.VectorClock{"A": 2, "B": 1})))

	// A causally older read is a violation.
	assert.Error(t, m.ValidateRead("c1", entry("k", storage.VectorClock{"A": 1})))
}

// ISSUE-11: consistent prefix rejects causally out-of-order reads for a key.
func TestConsistentPrefix_VectorClock(t *testing.T) {
	cp := consistency.NewConsistentPrefix()
	cp.RecordWrite("c1", entry("k", storage.VectorClock{"A": 3}))

	assert.NoError(t, cp.ValidateRead("c2", entry("k", storage.VectorClock{"A": 3})))
	assert.NoError(t, cp.ValidateRead("c2", entry("k", storage.VectorClock{"A": 4})))
	assert.Error(t, cp.ValidateRead("c2", entry("k", storage.VectorClock{"A": 2})))
}

// Dominates helper sanity: causal >= componentwise.
func TestVectorClock_Dominates(t *testing.T) {
	require.True(t, storage.VectorClock{"A": 2, "B": 1}.Dominates(storage.VectorClock{"A": 2}))
	require.True(t, storage.VectorClock{"A": 2}.Dominates(storage.VectorClock{"A": 2}))
	require.False(t, storage.VectorClock{"A": 1}.Dominates(storage.VectorClock{"A": 2}))
	require.False(t, storage.VectorClock{"B": 5}.Dominates(storage.VectorClock{"A": 1}))
}
