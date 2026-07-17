package replication

import (
	"sync"

	"replication-strategies/internal/storage"
)

// ReplicationLog is an append-only log of LogEntries
type ReplicationLog struct {
	mu      sync.RWMutex
	entries []storage.LogEntry
	// commitIndex is the highest committed index
	commitIndex uint64
	// nextIndex for new entries
	nextIndex uint64
}

func NewReplicationLog() *ReplicationLog {
	return &ReplicationLog{
		entries:   make([]storage.LogEntry, 0, 64),
		nextIndex: 1,
	}
}

func (l *ReplicationLog) Append(entry storage.LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.Index = l.nextIndex
	l.nextIndex++
	l.entries = append(l.entries, entry)
	return entry.Index
}

func (l *ReplicationLog) Get(index uint64) (storage.LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index >= l.nextIndex {
		return storage.LogEntry{}, false
	}
	return l.entries[index-1], true
}

func (l *ReplicationLog) GetFrom(startIndex uint64) []storage.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if startIndex >= l.nextIndex {
		return nil
	}
	start := int(startIndex) - 1
	if start < 0 {
		start = 0
	}
	result := make([]storage.LogEntry, len(l.entries)-start)
	copy(result, l.entries[start:])
	return result
}

func (l *ReplicationLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextIndex - 1
}

func (l *ReplicationLog) CommitIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.commitIndex
}

func (l *ReplicationLog) SetCommitIndex(idx uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if idx > l.commitIndex {
		l.commitIndex = idx
	}
}

func (l *ReplicationLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

func (l *ReplicationLog) Snapshot() []storage.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]storage.LogEntry, len(l.entries))
	copy(result, l.entries)
	return result
}

func (l *ReplicationLog) TruncateFrom(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == 0 || index > l.nextIndex {
		return
	}
	l.entries = l.entries[:index-1]
	l.nextIndex = index
}
