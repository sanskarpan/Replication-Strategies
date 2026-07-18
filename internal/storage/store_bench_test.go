package storage

import (
	"fmt"
	"testing"
)

// makeEntry builds a realistic KVEntry with a small vector clock and value.
func makeEntry(key string, node string) *KVEntry {
	vc := NewVectorClock()
	vc[node] = 1
	vc["n2"] = 3
	vc["n3"] = 7
	return &KVEntry{
		Key:       key,
		Value:     []byte("some-realistic-value-payload-abc123"),
		VClock:    vc,
		Timestamp: 1_700_000_000_000_000_000,
		NodeID:    node,
	}
}

func BenchmarkStoreSet(b *testing.B) {
	s := NewStore()
	// Precompute a rotating set of entries so the benchmark loop measures Set,
	// not allocation of inputs.
	const distinct = 1024
	entries := make([]*KVEntry, distinct)
	for i := range entries {
		entries[i] = makeEntry(fmt.Sprintf("key-%06d", i), "n1")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(entries[i%distinct])
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := NewStore()
	const distinct = 10000
	keys := make([]string, distinct)
	for i := 0; i < distinct; i++ {
		k := fmt.Sprintf("key-%06d", i)
		keys[i] = k
		s.Set(makeEntry(k, "n1"))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get(keys[i%distinct])
	}
}

func BenchmarkStoreSnapshot(b *testing.B) {
	s := NewStore()
	const size = 10000
	for i := 0; i < size; i++ {
		s.Set(makeEntry(fmt.Sprintf("key-%06d", i), "n1"))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Snapshot()
	}
}
