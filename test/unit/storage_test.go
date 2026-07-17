package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/storage"
)

// ISSUE-5: Delete must be copy-on-write. A pointer obtained before the delete must
// not be mutated in place (that was a data race against unsynchronised readers).
func TestStore_DeleteIsCopyOnWrite(t *testing.T) {
	s := storage.NewStore()
	s.Set(&storage.KVEntry{Key: "k", Value: []byte("v"), Timestamp: 1})

	before, ok := s.GetRaw("k")
	require.True(t, ok)
	require.False(t, before.Tombstone)

	s.Delete("k", "n1", 2, storage.NewVectorClock())

	assert.False(t, before.Tombstone,
		"Delete must not mutate the previously shared entry in place")

	_, present := s.Get("k")
	assert.False(t, present, "deleted key must read as not found")

	raw, ok := s.GetRaw("k")
	require.True(t, ok)
	assert.True(t, raw.Tombstone, "raw entry should carry the tombstone")
}
