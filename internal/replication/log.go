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

// --- Raft helpers ---

// TermAt returns the term of the entry at index, or 0 for index 0 / absent.
func (l *ReplicationLog) TermAt(index uint64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index >= l.nextIndex {
		return 0
	}
	return l.entries[index-1].Term
}

// LastIndexTerm returns the index and term of the last log entry.
func (l *ReplicationLog) LastIndexTerm() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	last := l.nextIndex - 1
	if last == 0 {
		return 0, 0
	}
	return last, l.entries[last-1].Term
}

// Matches reports whether the log contains an entry at prevIndex with prevTerm (the
// Raft AppendEntries consistency check). prevIndex 0 always matches (empty prefix).
func (l *ReplicationLog) Matches(prevIndex, prevTerm uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if prevIndex == 0 {
		return true
	}
	if prevIndex >= l.nextIndex {
		return false
	}
	return l.entries[prevIndex-1].Term == prevTerm
}

// AppendAfter applies Raft AppendEntries: for each incoming entry (which carries its
// absolute Index and Term), if an existing entry at that index has a different term the
// log is truncated from there, then the entry is appended. Entries already present with
// the same term are skipped. Returns the new last index.
func (l *ReplicationLog) AppendAfter(prevIndex uint64, entries []storage.LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		idx := e.Index
		if idx < l.nextIndex {
			// existing entry at this index
			if l.entries[idx-1].Term == e.Term {
				continue // already have it
			}
			// conflict: truncate everything from idx and append
			l.entries = l.entries[:idx-1]
			l.nextIndex = idx
		}
		// append (idx should equal nextIndex now)
		if idx == l.nextIndex {
			l.entries = append(l.entries, e)
			l.nextIndex++
		}
	}
	return l.nextIndex - 1
}
