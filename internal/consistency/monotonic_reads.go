package consistency

import (
	"fmt"
	"sync"

	"replication-strategies/internal/storage"
)

// MonotonicReads guarantees that once a client has seen a value, future reads never
// return a causally older value. It tracks the join of all vector clocks the client
// has observed per key; a read is a violation only if it strictly happens-before that
// frontier (i.e. it is in the past of something already seen).
type MonotonicReads struct {
	mu          sync.RWMutex
	clientReads map[string]map[string]storage.VectorClock // clientID -> key -> seen frontier
}

func NewMonotonicReads() *MonotonicReads {
	return &MonotonicReads{
		clientReads: make(map[string]map[string]storage.VectorClock),
	}
}

func (m *MonotonicReads) Name() string { return "monotonic_reads" }

func (m *MonotonicReads) ValidateRead(clientID string, proposed *storage.KVEntry) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	reads, ok := m.clientReads[clientID]
	if !ok {
		return nil
	}
	seen, ok := reads[proposed.Key]
	if !ok {
		return nil
	}
	// Violation if the proposed read does not causally dominate everything already
	// seen (i.e. it is behind OR concurrent-sideways to the seen frontier).
	if !proposed.VClock.Dominates(seen) {
		return fmt.Errorf("monotonic read violation: client %s already saw vclock %s for key %s, now got %s",
			clientID, seen, proposed.Key, proposed.VClock)
	}
	return nil
}

func (m *MonotonicReads) RecordRead(clientID string, entry *storage.KVEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.clientReads[clientID] == nil {
		m.clientReads[clientID] = make(map[string]storage.VectorClock)
	}
	cur := m.clientReads[clientID][entry.Key]
	if cur == nil {
		cur = storage.NewVectorClock()
	}
	m.clientReads[clientID][entry.Key] = cur.Clone().Merge(entry.VClock)
}

func (m *MonotonicReads) RecordWrite(clientID string, entry *storage.KVEntry) {
	// writes don't affect monotonic read tracking
}
