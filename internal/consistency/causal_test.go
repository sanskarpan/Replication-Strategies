package consistency

import (
	"testing"

	"replication-strategies/internal/storage"
)

func vc(pairs map[string]uint64) storage.VectorClock {
	v := storage.NewVectorClock()
	for k, n := range pairs {
		for i := uint64(0); i < n; i++ {
			v = v.Increment(k)
		}
	}
	return v
}

// TestCausal_ReadMustNotGoBackwards verifies session causal consistency: once a client
// observes vclock {a:2} for a key, a later read returning an older {a:1} is a violation,
// while an equal-or-newer read is allowed.
func TestCausal_ReadMustNotGoBackwards(t *testing.T) {
	c := NewCausalConsistency()
	newer := &storage.KVEntry{Key: "k", VClock: vc(map[string]uint64{"a": 2})}
	older := &storage.KVEntry{Key: "k", VClock: vc(map[string]uint64{"a": 1})}

	// Client observes the newer version via a read.
	c.RecordRead("c1", newer)

	if err := c.ValidateRead("c1", older); err == nil {
		t.Fatal("reading an older vclock after observing a newer one must violate causal consistency")
	}
	if err := c.ValidateRead("c1", newer); err != nil {
		t.Fatalf("re-reading the same (or newer) version must be allowed, got %v", err)
	}
	// A different client with no history is unconstrained.
	if err := c.ValidateRead("c2", older); err != nil {
		t.Fatalf("a fresh client must have no causal constraint, got %v", err)
	}
}

// TestCausal_ReadYourWrites verifies the write side of causal: a client must see its own write.
func TestCausal_ReadYourWrites(t *testing.T) {
	c := NewCausalConsistency()
	w := &storage.KVEntry{Key: "k", VClock: vc(map[string]uint64{"n1": 3})}
	c.RecordWrite("c1", w)
	stale := &storage.KVEntry{Key: "k", VClock: vc(map[string]uint64{"n1": 2})}
	if err := c.ValidateRead("c1", stale); err == nil {
		t.Fatal("a read that does not dominate the client's own write must violate causal consistency")
	}
}

// TestBoundedStaleness verifies a read is rejected only when it lags the freshest write
// by more than the configured bound.
func TestBoundedStaleness(t *testing.T) {
	const ms = 1_000_000          // 1ms in ns
	b := NewBoundedStaleness(100) // 100ms bound
	fresh := &storage.KVEntry{Key: "k", Timestamp: 1000 * ms}
	b.RecordWrite("c1", fresh)

	// A read only 50ms behind is within bound.
	within := &storage.KVEntry{Key: "k", Timestamp: 950 * ms}
	if err := b.ValidateRead("c1", within); err != nil {
		t.Fatalf("50ms staleness is within the 100ms bound, got %v", err)
	}
	// A read 300ms behind exceeds the bound.
	tooOld := &storage.KVEntry{Key: "k", Timestamp: 700 * ms}
	if err := b.ValidateRead("c1", tooOld); err == nil {
		t.Fatal("300ms staleness must exceed the 100ms bound")
	}
	// An unknown key has no freshest reference and is unconstrained.
	if err := b.ValidateRead("c1", &storage.KVEntry{Key: "other", Timestamp: 0}); err != nil {
		t.Fatalf("unknown key must be unconstrained, got %v", err)
	}
}
