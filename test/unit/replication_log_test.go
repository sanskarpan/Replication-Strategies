package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"replication-strategies/internal/replication"
	"replication-strategies/internal/storage"
)

func TestReplicationLog_AppendAndGet(t *testing.T) {
	l := replication.NewReplicationLog()
	e := storage.LogEntry{Key: "k1", Value: []byte("v1"), Op: storage.OpSet}
	idx := l.Append(e)
	assert.Equal(t, uint64(1), idx)

	got, ok := l.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "k1", got.Key)
	assert.Equal(t, uint64(1), got.Index)
}

func TestReplicationLog_GetFrom(t *testing.T) {
	l := replication.NewReplicationLog()
	for i := 0; i < 5; i++ {
		l.Append(storage.LogEntry{Key: "k", Value: []byte("v")})
	}
	entries := l.GetFrom(3)
	assert.Len(t, entries, 3)
	assert.Equal(t, uint64(3), entries[0].Index)
}

func TestReplicationLog_CommitIndex(t *testing.T) {
	l := replication.NewReplicationLog()
	l.Append(storage.LogEntry{Key: "k"})
	l.SetCommitIndex(1)
	assert.Equal(t, uint64(1), l.CommitIndex())

	// CommitIndex should not go backwards
	l.SetCommitIndex(0)
	assert.Equal(t, uint64(1), l.CommitIndex())
}

func TestReplicationLog_TruncateFrom(t *testing.T) {
	l := replication.NewReplicationLog()
	for i := 0; i < 5; i++ {
		l.Append(storage.LogEntry{Key: "k"})
	}
	assert.Equal(t, 5, l.Len())
	l.TruncateFrom(3)
	assert.Equal(t, 2, l.Len())
	assert.Equal(t, uint64(3), l.LastIndex()+1) // next index should be 3
}

func TestReplicationLog_Snapshot(t *testing.T) {
	l := replication.NewReplicationLog()
	l.Append(storage.LogEntry{Key: "k1"})
	l.Append(storage.LogEntry{Key: "k2"})
	snap := l.Snapshot()
	assert.Len(t, snap, 2)
	snap[0].Key = "modified"
	// original should not be modified
	orig, _ := l.Get(1)
	assert.Equal(t, "k1", orig.Key)
}
