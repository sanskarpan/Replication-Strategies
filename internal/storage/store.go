package storage

import (
	"sync"
)

type Store struct {
	mu      sync.RWMutex
	data    map[string]*KVEntry
	version uint64
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]*KVEntry),
	}
}

func (s *Store) Get(key string) (*KVEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Tombstone {
		return nil, false
	}
	return e, true
}

func (s *Store) GetRaw(key string) (*KVEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

func (s *Store) Set(entry *KVEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	// Copy to avoid mutating the caller's pointer (which may be shared across goroutines).
	cp := *entry
	// Deep-copy the vector clock: a shallow struct copy aliases the map, so the stored
	// entry would otherwise share one mutable VClock with the caller AND (via anti-entropy
	// broadcasting shared pointers) with every peer. Cloning isolates each store.
	if cp.VClock != nil {
		cp.VClock = cp.VClock.Clone()
	}
	cp.Version = s.version
	s.data[cp.Key] = &cp
}

func (s *Store) Delete(key string, nodeID string, ts int64, vc VectorClock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	if existing, ok := s.data[key]; ok {
		// Copy-on-write: never mutate an entry in place, since Get/GetRaw/Snapshot
		// hand out shared pointers that readers may still be inspecting without a lock.
		cp := *existing
		cp.Tombstone = true
		cp.Timestamp = ts
		cp.NodeID = nodeID
		cp.VClock = vc.Clone()
		cp.Version = s.version
		s.data[key] = &cp
	} else {
		s.data[key] = &KVEntry{
			Key:       key,
			NodeID:    nodeID,
			Timestamp: ts,
			VClock:    vc.Clone(),
			Tombstone: true,
			Version:   s.version,
		}
	}
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) Snapshot() map[string]*KVEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[string]*KVEntry, len(s.data))
	for k, v := range s.data {
		snap[k] = v
	}
	return snap
}

func (s *Store) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}
