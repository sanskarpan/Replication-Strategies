package consistency

import (
	"fmt"
	"sync"

	"replication-strategies/internal/storage"
)

// ConsistentPrefix guarantees that reads never observe writes out of causal order.
// It tracks a global (non-client-specific) vector-clock frontier per key; a read that
// strictly happens-before the frontier would expose an out-of-order prefix and is a
// violation.
type ConsistentPrefix struct {
	mu       sync.RWMutex
	sequence map[string]storage.VectorClock // key -> frontier VClock
}

func NewConsistentPrefix() *ConsistentPrefix {
	return &ConsistentPrefix{
		sequence: make(map[string]storage.VectorClock),
	}
}

func (c *ConsistentPrefix) Name() string { return "consistent_prefix" }

func (c *ConsistentPrefix) ValidateRead(clientID string, proposed *storage.KVEntry) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	last, ok := c.sequence[proposed.Key]
	if !ok {
		return nil
	}
	if !proposed.VClock.Dominates(last) {
		return fmt.Errorf("consistent prefix violation: key %s frontier %s, proposed %s is causally earlier",
			proposed.Key, last, proposed.VClock)
	}
	return nil
}

func (c *ConsistentPrefix) RecordRead(clientID string, entry *storage.KVEntry) {
	c.advance(entry)
}

func (c *ConsistentPrefix) RecordWrite(clientID string, entry *storage.KVEntry) {
	c.advance(entry)
}

func (c *ConsistentPrefix) advance(entry *storage.KVEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.sequence[entry.Key]
	if cur == nil {
		cur = storage.NewVectorClock()
	}
	c.sequence[entry.Key] = cur.Clone().Merge(entry.VClock)
}
