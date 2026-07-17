package replication

import (
	"sync"

	"replication-strategies/internal/storage"
)

// ReplicationLog is an append-only log of LogEntries with optional prefix compaction
// (Raft snapshots). Absolute entry index i maps to slice position i-snapshotIndex-1;
// when no snapshot has been taken snapshotIndex==0 and behavior is a plain 1-based log.
type ReplicationLog struct {
	mu      sync.RWMutex
	entries []storage.LogEntry
	// commitIndex is the highest committed index
	commitIndex uint64
	// nextIndex for new entries
	nextIndex uint64
	// snapshot boundary: entries with index <= snapshotIndex have been compacted away.
	snapshotIndex uint64
	snapshotTerm  uint64
}

func NewReplicationLog() *ReplicationLog {
	return &ReplicationLog{
		entries:   make([]storage.LogEntry, 0, 64),
		nextIndex: 1,
	}
}

// pos converts an absolute index to a slice position (may be out of range).
func (l *ReplicationLog) pos(index uint64) int { return int(index) - int(l.snapshotIndex) - 1 }

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
	if index == 0 || index <= l.snapshotIndex || index >= l.nextIndex {
		return storage.LogEntry{}, false
	}
	return l.entries[l.pos(index)], true
}

func (l *ReplicationLog) GetFrom(startIndex uint64) []storage.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if startIndex >= l.nextIndex {
		return nil
	}
	if startIndex <= l.snapshotIndex {
		startIndex = l.snapshotIndex + 1
	}
	start := l.pos(startIndex)
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
	p := l.pos(index)
	if p < 0 {
		p = 0
	}
	l.entries = l.entries[:p]
	l.nextIndex = index
}

// --- Raft helpers ---

// TermAt returns the term of the entry at index, or 0 for index 0 / absent.
func (l *ReplicationLog) TermAt(index uint64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 {
		return 0
	}
	if index == l.snapshotIndex {
		return l.snapshotTerm
	}
	if index <= l.snapshotIndex || index >= l.nextIndex {
		return 0
	}
	return l.entries[l.pos(index)].Term
}

// LastIndexTerm returns the index and term of the last log entry (or the snapshot).
func (l *ReplicationLog) LastIndexTerm() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	last := l.nextIndex - 1
	if last == 0 {
		return 0, 0
	}
	if last == l.snapshotIndex {
		return last, l.snapshotTerm
	}
	return last, l.entries[l.pos(last)].Term
}

// Matches reports whether the log contains an entry at prevIndex with prevTerm (the
// Raft AppendEntries consistency check). prevIndex 0 always matches (empty prefix).
func (l *ReplicationLog) Matches(prevIndex, prevTerm uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if prevIndex == 0 {
		return true
	}
	if prevIndex == l.snapshotIndex {
		return prevTerm == l.snapshotTerm
	}
	if prevIndex < l.snapshotIndex || prevIndex >= l.nextIndex {
		return false
	}
	return l.entries[l.pos(prevIndex)].Term == prevTerm
}

// AppendAfter applies Raft AppendEntries: for each incoming entry (carrying its absolute
// Index and Term), a conflicting existing entry truncates the log from that point, then
// the entry is appended. Entries already present with the same term are skipped.
func (l *ReplicationLog) AppendAfter(prevIndex uint64, entries []storage.LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		idx := e.Index
		if idx <= l.snapshotIndex {
			continue // already covered by the snapshot
		}
		if idx < l.nextIndex {
			p := l.pos(idx)
			if l.entries[p].Term == e.Term {
				continue // already have it
			}
			l.entries = l.entries[:p] // conflict: truncate and re-append
			l.nextIndex = idx
		}
		if idx == l.nextIndex {
			l.entries = append(l.entries, e)
			l.nextIndex++
		}
	}
	return l.nextIndex - 1
}

// SnapshotBoundary returns the last-included index and term of the current snapshot.
func (l *ReplicationLog) SnapshotBoundary() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snapshotIndex, l.snapshotTerm
}

// Compact discards log entries with index <= uptoIndex (which must be committed and
// captured in a state snapshot), recording the new snapshot boundary.
func (l *ReplicationLog) Compact(uptoIndex, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if uptoIndex <= l.snapshotIndex {
		return
	}
	if uptoIndex >= l.nextIndex {
		uptoIndex = l.nextIndex - 1
	}
	if uptoIndex <= l.snapshotIndex {
		return
	}
	drop := int(uptoIndex - l.snapshotIndex)
	if drop > len(l.entries) {
		drop = len(l.entries)
	}
	// term of the last compacted entry (before we drop it)
	l.snapshotTerm = l.entries[drop-1].Term
	if term != 0 {
		l.snapshotTerm = term
	}
	l.entries = append([]storage.LogEntry(nil), l.entries[drop:]...)
	l.snapshotIndex = uptoIndex
}

// InstallSnapshot resets the log to a snapshot boundary received from the leader.
func (l *ReplicationLog) InstallSnapshot(index, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index <= l.snapshotIndex {
		return
	}
	l.entries = nil
	l.snapshotIndex = index
	l.snapshotTerm = term
	l.nextIndex = index + 1
	if l.commitIndex < index {
		l.commitIndex = index
	}
}
