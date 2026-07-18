package consistency

import (
	"fmt"
	"sync"

	"replication-strategies/internal/storage"
)

// ConsistencyCausal / ConsistencyBoundedStaleness extend the read levels beyond the
// original strong/eventual/session/monotonic set.
const (
	ConsistencyCausal           ReadConsistency = "causal"
	ConsistencyBoundedStaleness ReadConsistency = "bounded_staleness"
)

// CausalConsistency enforces session causal consistency: within a client session, every
// read must causally dominate everything that client has already observed for that key
// (its own writes AND prior reads). This combines read-your-writes with monotonic reads
// into the causal "happens-before is respected" guarantee, using globally-comparable
// vector clocks so it holds no matter which replica serves the read.
type CausalConsistency struct {
	mu       sync.RWMutex
	observed map[string]map[string]storage.VectorClock // clientID -> key -> max observed VClock
}

func NewCausalConsistency() *CausalConsistency {
	return &CausalConsistency{observed: make(map[string]map[string]storage.VectorClock)}
}

func (c *CausalConsistency) Name() string { return "causal" }

func (c *CausalConsistency) ValidateRead(clientID string, proposed *storage.KVEntry) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys, ok := c.observed[clientID]
	if !ok {
		return nil
	}
	required, ok := keys[proposed.Key]
	if !ok {
		return nil
	}
	if !proposed.VClock.Dominates(required) {
		return fmt.Errorf("causal violation: client %s already observed vclock %s for key %s, read returned older %s",
			clientID, required, proposed.Key, proposed.VClock)
	}
	return nil
}

// RecordRead advances the client's causal context (monotonic-read side of causal).
func (c *CausalConsistency) RecordRead(clientID string, entry *storage.KVEntry) {
	c.record(clientID, entry)
}

// RecordWrite advances the client's causal context (read-your-writes side of causal).
func (c *CausalConsistency) RecordWrite(clientID string, entry *storage.KVEntry) {
	c.record(clientID, entry)
}

func (c *CausalConsistency) record(clientID string, entry *storage.KVEntry) {
	if entry == nil || entry.VClock == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.observed[clientID] == nil {
		c.observed[clientID] = make(map[string]storage.VectorClock)
	}
	cur := c.observed[clientID][entry.Key]
	if cur == nil {
		cur = storage.NewVectorClock()
	}
	c.observed[clientID][entry.Key] = cur.Clone().Merge(entry.VClock)
}

// BoundedStaleness allows stale reads but only within a bound: a read whose value lags
// the freshest known write for its key by more than MaxLagMillis is a violation. This
// models the availability/latency-for-freshness tradeoff (e.g. Cosmos DB bounded
// staleness) rather than the all-or-nothing strong vs eventual choice.
type BoundedStaleness struct {
	MaxLagMillis int64
	mu           sync.RWMutex
	freshest     map[string]int64 // key -> newest write timestamp (HLC/wall nanos)
}

func NewBoundedStaleness(maxLagMillis int64) *BoundedStaleness {
	if maxLagMillis <= 0 {
		maxLagMillis = 1000
	}
	return &BoundedStaleness{MaxLagMillis: maxLagMillis, freshest: make(map[string]int64)}
}

func (b *BoundedStaleness) Name() string { return "bounded_staleness" }

func (b *BoundedStaleness) ValidateRead(clientID string, proposed *storage.KVEntry) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	newest, ok := b.freshest[proposed.Key]
	if !ok {
		return nil
	}
	lagMs := (newest - proposed.Timestamp) / 1_000_000 // ns -> ms
	if lagMs > b.MaxLagMillis {
		return fmt.Errorf("bounded-staleness violation: read of key %s lags the freshest write by %dms (bound %dms)",
			proposed.Key, lagMs, b.MaxLagMillis)
	}
	return nil
}

func (b *BoundedStaleness) RecordRead(clientID string, entry *storage.KVEntry) {}

func (b *BoundedStaleness) RecordWrite(clientID string, entry *storage.KVEntry) {
	if entry == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry.Timestamp > b.freshest[entry.Key] {
		b.freshest[entry.Key] = entry.Timestamp
	}
}

var (
	_ ConsistencyGuarantee = (*CausalConsistency)(nil)
	_ ConsistencyGuarantee = (*BoundedStaleness)(nil)
)
