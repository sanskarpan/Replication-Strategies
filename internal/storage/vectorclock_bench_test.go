package storage

import (
	"fmt"
	"testing"
)

// makeClock builds a vector clock spanning n nodes, a realistic cluster size.
func makeClock(n int, base uint64) VectorClock {
	vc := NewVectorClock()
	for i := 0; i < n; i++ {
		vc[fmt.Sprintf("node-%02d", i)] = base + uint64(i)
	}
	return vc
}

func BenchmarkVectorClockMerge(b *testing.B) {
	const nodes = 16
	base := makeClock(nodes, 1)
	// other overlaps and extends base so Merge does real max-folding work.
	other := makeClock(nodes, 3)
	other["node-99"] = 5

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clone so the merge does not monotonically inflate the receiver and
		// so each iteration performs comparable work with fresh allocation.
		dst := base.Clone()
		dst.Merge(other)
	}
}

func BenchmarkVectorClockIncrement(b *testing.B) {
	const nodes = 16
	vc := makeClock(nodes, 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.Increment("node-07")
	}
}

func BenchmarkDominates(b *testing.B) {
	const nodes = 16
	vc := makeClock(nodes, 5)
	// other is causally at or below vc, so Dominates scans the full map.
	other := makeClock(nodes, 2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vc.Dominates(other)
	}
}
