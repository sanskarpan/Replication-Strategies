package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"replication-strategies/internal/storage"
)

func TestVectorClock_Increment(t *testing.T) {
	vc := storage.NewVectorClock()
	vc = vc.Increment("node1")
	assert.Equal(t, uint64(1), vc["node1"])
	vc = vc.Increment("node1")
	assert.Equal(t, uint64(2), vc["node1"])
}

func TestVectorClock_Merge(t *testing.T) {
	a := storage.VectorClock{"n1": 3, "n2": 1}
	b := storage.VectorClock{"n1": 1, "n2": 4, "n3": 2}
	merged := a.Merge(b)
	assert.Equal(t, uint64(3), merged["n1"])
	assert.Equal(t, uint64(4), merged["n2"])
	assert.Equal(t, uint64(2), merged["n3"])
}

func TestVectorClock_HappensBefore(t *testing.T) {
	a := storage.VectorClock{"n1": 1, "n2": 0}
	b := storage.VectorClock{"n1": 2, "n2": 1}
	assert.True(t, a.HappensBefore(b), "a should happen before b")
	assert.False(t, b.HappensBefore(a), "b should not happen before a")
}

func TestVectorClock_Concurrent(t *testing.T) {
	a := storage.VectorClock{"n1": 2, "n2": 1}
	b := storage.VectorClock{"n1": 1, "n2": 2}
	assert.True(t, a.Concurrent(b), "a and b should be concurrent")
	assert.True(t, b.Concurrent(a), "b and a should be concurrent")
}

func TestVectorClock_Equal(t *testing.T) {
	a := storage.VectorClock{"n1": 1, "n2": 2}
	b := storage.VectorClock{"n1": 1, "n2": 2}
	assert.True(t, a.Equal(b))
	assert.False(t, a.Concurrent(b), "equal clocks should not be concurrent")
	assert.False(t, a.HappensBefore(b), "equal clocks should not have happens-before")
}

func TestVectorClock_Clone(t *testing.T) {
	a := storage.VectorClock{"n1": 1, "n2": 2}
	b := a.Clone()
	b["n3"] = 5
	_, exists := a["n3"]
	assert.False(t, exists, "modifying clone should not affect original")
}

func TestVectorClock_NotConcurrent_WhenIdentical(t *testing.T) {
	a := storage.VectorClock{"n1": 1}
	b := a.Clone()
	require.False(t, a.Concurrent(b))
}
