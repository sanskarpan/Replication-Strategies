package consistency

import (
	"fmt"
	"sync"

	"replication-strategies/internal/storage"
)

// ReadYourWrites guarantees that a client always sees its own writes. It tracks the
// vector clock of each client's last write per key and requires that any read for
// that key causally dominates it. Vector clocks are globally comparable across nodes,
// so this works regardless of which replica serves the read (unlike a per-store
// Version counter).
type ReadYourWrites struct {
	mu           sync.RWMutex
	clientWrites map[string]map[string]storage.VectorClock // clientID -> key -> write VClock
}

func NewReadYourWrites() *ReadYourWrites {
	return &ReadYourWrites{
		clientWrites: make(map[string]map[string]storage.VectorClock),
	}
}

func (r *ReadYourWrites) Name() string { return "read_your_writes" }

func (r *ReadYourWrites) ValidateRead(clientID string, proposed *storage.KVEntry) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	writes, ok := r.clientWrites[clientID]
	if !ok {
		return nil
	}
	required, ok := writes[proposed.Key]
	if !ok {
		return nil
	}
	if !proposed.VClock.Dominates(required) {
		return fmt.Errorf("RYW violation: client %s wrote vclock %s for key %s, read returned %s",
			clientID, required, proposed.Key, proposed.VClock)
	}
	return nil
}

func (r *ReadYourWrites) RecordRead(clientID string, entry *storage.KVEntry) {
	// no state needed for reads in RYW
}

func (r *ReadYourWrites) RecordWrite(clientID string, entry *storage.KVEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clientWrites[clientID] == nil {
		r.clientWrites[clientID] = make(map[string]storage.VectorClock)
	}
	// Merge so a client's requirement reflects the join of all its writes to the key.
	cur := r.clientWrites[clientID][entry.Key]
	if cur == nil {
		cur = storage.NewVectorClock()
	}
	r.clientWrites[clientID][entry.Key] = cur.Clone().Merge(entry.VClock)
}
