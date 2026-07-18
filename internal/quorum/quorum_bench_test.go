package quorum

import (
	"fmt"
	"testing"
)

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
