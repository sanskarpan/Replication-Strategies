// Package mvcc implements multi-version concurrency control (MVCC) with
// per-key version chains and snapshot reads. Each key holds an ordered chain
// of versions keyed by a logical commit timestamp; a read at timestamp ts sees
// the newest version whose Timestamp <= ts, giving snapshot isolation: a read
// at an older ts never observes writes committed later.
package mvcc

import (
	"sort"
	"sync"
)

// Version is a single committed value for a key at a logical commit timestamp.
// A Tombstone version marks a deletion and is treated as not-found by reads.
type Version struct {
	Value     []byte
	Timestamp int64
	Tombstone bool
}

// Store is a concurrency-safe MVCC key/value store. Versions for each key are
// held in a slice sorted by ascending Timestamp so that snapshot reads resolve
// via binary search.
type Store struct {
	mu       sync.RWMutex
	versions map[string][]Version
}

// New returns an empty Store ready for use.
func New() *Store {
	return &Store{versions: make(map[string][]Version)}
}

// Put appends a new version for key committed at ts. Timestamps are expected to
// be monotonically increasing per key; a Put at a ts <= the latest existing
// version is inserted in order so the chain stays sorted.
func (s *Store) Put(key string, value []byte, ts int64) {
	s.append(key, Version{Value: value, Timestamp: ts})
}

// Delete appends a tombstone version for key at ts. Reads at or after ts (and
// before any later Put) resolve to not-found.
func (s *Store) Delete(key string, ts int64) {
	s.append(key, Version{Timestamp: ts, Tombstone: true})
}

// append inserts v into key's chain, keeping it sorted by ascending Timestamp.
func (s *Store) append(key string, v Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chain := s.versions[key]
	// Fast path: the common case is a strictly-increasing commit timestamp,
	// which appends to the tail.
	if len(chain) == 0 || v.Timestamp >= chain[len(chain)-1].Timestamp {
		s.versions[key] = append(chain, v)
		return
	}
	// Out-of-order commit: find the insertion point to preserve ordering.
	i := sort.Search(len(chain), func(i int) bool { return chain[i].Timestamp >= v.Timestamp })
	chain = append(chain, Version{})
	copy(chain[i+1:], chain[i:])
	chain[i] = v
	s.versions[key] = chain
}

// ReadAt returns the value of the newest version whose Timestamp <= ts (a
// snapshot read). It returns ok=false if key has no version at or before ts, or
// if that version is a tombstone. Because the resolved version is fully
// determined by ts, holding ts fixed yields a stable snapshot even as later
// versions are written.
func (s *Store) ReadAt(key string, ts int64) (value []byte, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.versions[key]
	if len(chain) == 0 {
		return nil, false
	}
	// Find the first version with Timestamp > ts; the one before it is the
	// newest version visible at ts.
	i := sort.Search(len(chain), func(i int) bool { return chain[i].Timestamp > ts })
	if i == 0 {
		// ts precedes the first version: nothing visible.
		return nil, false
	}
	v := chain[i-1]
	if v.Tombstone {
		return nil, false
	}
	// Return a copy so callers cannot mutate stored bytes.
	return append([]byte(nil), v.Value...), true
}

// Latest returns the value of the most recent version for key, treating a
// tombstone as not-found. It is equivalent to a snapshot read at the highest
// possible timestamp.
func (s *Store) Latest(key string) (value []byte, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.versions[key]
	if len(chain) == 0 {
		return nil, false
	}
	v := chain[len(chain)-1]
	if v.Tombstone {
		return nil, false
	}
	return append([]byte(nil), v.Value...), true
}

// Compact garbage-collects versions of key older than beforeTs, always keeping
// the newest version with Timestamp <= beforeTs so that snapshot reads at or
// after beforeTs still resolve correctly. Versions with Timestamp >= beforeTs
// are always retained.
func (s *Store) Compact(key string, beforeTs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chain := s.versions[key]
	if len(chain) == 0 {
		return
	}
	// Index of the first version with Timestamp >= beforeTs.
	i := sort.Search(len(chain), func(i int) bool { return chain[i].Timestamp >= beforeTs })
	// The newest version strictly before beforeTs (if any) must be kept so a
	// snapshot at beforeTs still sees it. That is chain[i-1]; drop everything
	// before it.
	keepFrom := i
	if i > 0 {
		keepFrom = i - 1
	}
	if keepFrom == 0 {
		return
	}
	// Preserve ordering by copying the retained tail into a fresh slice so the
	// dropped versions become collectable.
	retained := make([]Version, len(chain)-keepFrom)
	copy(retained, chain[keepFrom:])
	s.versions[key] = retained
}
