package quorum

import (
	"fmt"
	"testing"
)

func BenchmarkIsStronglyConsistent(b *testing.B) {
	cases := []QuorumConfig{
		{N: 3, W: 2, R: 2},
		{N: 5, W: 1, R: 1},
		{N: 7, W: 4, R: 4},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink bool
	for i := 0; i < b.N; i++ {
		for _, q := range cases {
			sink = q.IsStronglyConsistent()
		}
	}
	_ = sink
}

func BenchmarkOverlapCount(b *testing.B) {
	cases := []QuorumConfig{
		{N: 3, W: 2, R: 2},
		{N: 5, W: 3, R: 3},
		{N: 5, W: 1, R: 1},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		for _, q := range cases {
			sink = q.OverlapCount()
		}
	}
	_ = sink
}

func BenchmarkStaleReadProbability(b *testing.B) {
	// Exercise a range of cluster sizes. Use a W=1, R=1 config so that
	// W+R <= N holds (no quorum overlap) and StaleReadProbability takes the
	// binomial-computation path rather than the trivial strongly-consistent
	// early return.
	for _, n := range []int{3, 5, 9, 25, 101} {
		q := QuorumConfig{N: n, W: 1, R: 1}
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var sink float64
			for i := 0; i < b.N; i++ {
				sink = q.StaleReadProbability()
			}
			_ = sink
		})
	}
}
