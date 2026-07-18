package mvcc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshot reads: a read at ts sees the newest version with Timestamp <= ts.
func TestReadAt_SnapshotResolution(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)

	v, ok := s.ReadAt("k", 15)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v, "ReadAt(15) must see v1@10, not v2@20")

	v, ok = s.ReadAt("k", 25)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v, "ReadAt(25) must see v2@20")

	// Exact-timestamp reads resolve to that version.
	v, ok = s.ReadAt("k", 10)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v)
	v, ok = s.ReadAt("k", 20)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v)

	// A read before the first version is not-found.
	_, ok = s.ReadAt("k", 5)
	assert.False(t, ok, "ReadAt(5) precedes the first version and must be not-found")
}

// snapshot isolation: a snapshot ts taken as 15 keeps returning v1 even after
// v2@20 is written later — reads at an older ts never see later writes.
func TestReadAt_SnapshotIsolationStableAcrossLaterWrites(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)

	const snapTS = int64(15)
	v, ok := s.ReadAt("k", snapTS)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v)

	// A later write must not disturb the earlier snapshot.
	s.Put("k", []byte("v2"), 20)

	v, ok = s.ReadAt("k", snapTS)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v, "snapshot@15 must still resolve to v1 after v2@20")
}

// tombstones: delete@30 makes ReadAt(35) not-found but ReadAt(25) still sees v2.
func TestDelete_TombstoneVisibility(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)
	s.Delete("k", 30)

	_, ok := s.ReadAt("k", 35)
	assert.False(t, ok, "ReadAt(35) must be not-found after delete@30")

	v, ok := s.ReadAt("k", 25)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v, "ReadAt(25) must still see v2@20 (before the delete)")

	// Latest reflects the tombstone.
	_, ok = s.Latest("k")
	assert.False(t, ok, "Latest must be not-found after a delete")
}

func TestLatest(t *testing.T) {
	s := New()
	_, ok := s.Latest("missing")
	assert.False(t, ok)

	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)
	v, ok := s.Latest("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v)
}

// Compact drops old versions but keeps snapshots at/after beforeTs resolvable.
func TestCompact_KeepsSnapshotsResolvable(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)
	s.Put("k", []byte("v3"), 30)

	// Compact everything older than 25. v1@10 is fully collectable; v2@20 is the
	// newest version <= 25 and must survive so a snapshot at 25 still sees it.
	s.Compact("k", 25)

	v, ok := s.ReadAt("k", 25)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v, "snapshot@25 must still resolve after compaction")

	v, ok = s.ReadAt("k", 35)
	require.True(t, ok)
	assert.Equal(t, []byte("v3"), v, "newer versions survive compaction")

	// v1 is gone: a read strictly between the kept boundary and the dropped
	// version now resolves to the retained boundary version, not v1.
	v, ok = s.ReadAt("k", 20)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v)

	// The chain was actually trimmed.
	s.mu.RLock()
	n := len(s.versions["k"])
	s.mu.RUnlock()
	assert.Equal(t, 2, n, "compaction must have dropped v1@10")
}

// Compacting at a boundary at/below the first version is a no-op.
func TestCompact_NoOpWhenNothingDroppable(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)

	s.Compact("k", 10)

	s.mu.RLock()
	n := len(s.versions["k"])
	s.mu.RUnlock()
	assert.Equal(t, 2, n, "no version older than 10 exists, so nothing is dropped")

	v, ok := s.ReadAt("k", 15)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v)
}

// ReadAt returns a copy: mutating the returned slice must not corrupt storage.
func TestReadAt_ReturnsCopy(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	v, ok := s.ReadAt("k", 10)
	require.True(t, ok)
	v[0] = 'X'

	again, ok := s.ReadAt("k", 10)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), again, "stored bytes must not be mutable via a returned read")
}

// Out-of-order commits are inserted in timestamp order so the chain stays sorted.
func TestPut_OutOfOrderCommit(t *testing.T) {
	s := New()
	s.Put("k", []byte("v3"), 30)
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)

	v, ok := s.ReadAt("k", 15)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v)
	v, ok = s.ReadAt("k", 25)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), v)
	v, ok = s.Latest("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v3"), v)
}

// Concurrency: the mutex must make Put/ReadAt/Compact safe under -race.
func TestStore_ConcurrentSafe(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 200; i++ {
				ts := base*1000 + i
				s.Put("k", []byte("v"), ts)
				s.ReadAt("k", ts)
				s.Latest("k")
				if i%50 == 0 {
					s.Compact("k", ts)
				}
			}
		}(int64(w))
	}
	wg.Wait()
}
