package antientropy

import (
	"fmt"
	"testing"
)

// makeKV builds a realistic ~n-key store.
func makeKV(n int) map[string]string {
	kv := make(map[string]string, n)
	for i := 0; i < n; i++ {
		kv[fmt.Sprintf("key-%06d", i)] = fmt.Sprintf("value-%06d-payload", i)
	}
	return kv
}

func BenchmarkBuildTree(b *testing.B) {
	const size = 1000
	kv := makeKV(size)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildTree(kv)
	}
}

func BenchmarkDiff(b *testing.B) {
	const size = 1000
	kvA := makeKV(size)

	// kvB is identical except for a handful of divergent values, the common
	// anti-entropy case: two mostly-synced replicas differing in a few keys.
	kvB := makeKV(size)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%06d", i*97%size)
		kvB[k] = "divergent-value-" + k
	}

	treeA := BuildTree(kvA)
	treeB := BuildTree(kvB)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Diff(treeA, treeB)
	}
}
