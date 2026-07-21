package conflict

import (
	"encoding/json"
	"replication-strategies/internal/storage"
	"testing"
	"time"
)

func makeBenchConflict(lts, rts int64, lnode, rnode string) *Conflict {
	return &Conflict{
		ID:         "bench-c",
		Key:        "k",
		DetectedAt: time.Time{},
		NodeID:     lnode,
		Local: &storage.KVEntry{
			Key: "k", Value: []byte("local-value"), Timestamp: lts, NodeID: lnode,
			VClock: storage.NewVectorClock(),
		},
		Remote: &storage.KVEntry{
			Key: "k", Value: []byte("remote-value"), Timestamp: rts, NodeID: rnode,
			VClock: storage.NewVectorClock(),
		},
	}
}

func BenchmarkLWWResolverResolve(b *testing.B) {
	r := NewLWWResolver()
	c := makeBenchConflict(100, 200, "node-a", "node-b")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve(c)
	}
}

func BenchmarkLWWResolverResolveTie(b *testing.B) {
	r := NewLWWResolver()
	c := makeBenchConflict(100, 100, "node-a", "node-b") // forces string-compare tiebreak path
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve(c)
	}
}

func BenchmarkVectorClockResolverResolve(b *testing.B) {
	r := NewVectorClockResolver(NewLWWResolver())
	local := &storage.KVEntry{
		Key: "k", Value: []byte("v"), NodeID: "node-a",
		VClock: storage.VectorClock{"node-a": 2, "node-b": 1},
	}
	remote := &storage.KVEntry{
		Key: "k", Value: []byte("v"), NodeID: "node-b",
		VClock: storage.VectorClock{"node-a": 1, "node-b": 3},
	}
	c := &Conflict{ID: "bench", Key: "k", Local: local, Remote: remote, NodeID: "node-a"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve(c)
	}
}

func BenchmarkCRDTResolverGCounter(b *testing.B) {
	r := NewCRDTResolver()
	g1 := NewGCounter()
	g2 := NewGCounter()
	for _, n := range []string{"a", "b", "c"} {
		g1.Increment(n)
		g2.Increment(n)
		g2.Increment(n)
	}
	data1, _ := json.Marshal(g1)
	data2, _ := json.Marshal(g2)
	c := &Conflict{
		ID:  "bench",
		Key: "k",
		Local: &storage.KVEntry{
			Key: "k", Value: data1, NodeID: "node-a",
			VClock: storage.NewVectorClock(),
		},
		Remote: &storage.KVEntry{
			Key: "k", Value: data2, NodeID: "node-b",
			VClock: storage.NewVectorClock(),
		},
		NodeID: "node-a",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve(c)
	}
}
