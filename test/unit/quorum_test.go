package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"replication-strategies/internal/quorum"
)

func TestQuorumConfig_IsStronglyConsistent(t *testing.T) {
	tests := []struct {
		n, w, r  int
		expected bool
	}{
		{5, 3, 3, true},  // W+R=6 > N=5
		{5, 1, 1, false}, // W+R=2 <= N=5
		{3, 2, 2, true},  // W+R=4 > N=3
		{3, 1, 3, true},  // W+R=4 > N=3
		{5, 2, 3, false}, // W+R=5 == N=5, NOT strictly greater
		{5, 3, 2, false}, // W+R=5 == N=5
		{5, 3, 3, true},  // classic quorum
	}
	for _, tt := range tests {
		q := quorum.QuorumConfig{N: tt.n, W: tt.w, R: tt.r}
		assert.Equal(t, tt.expected, q.IsStronglyConsistent(),
			"N=%d W=%d R=%d", tt.n, tt.w, tt.r)
	}
}

func TestQuorumConfig_IsValid(t *testing.T) {
	assert.NoError(t, quorum.QuorumConfig{N: 5, W: 3, R: 3}.IsValid())
	assert.Error(t, quorum.QuorumConfig{N: 5, W: 0, R: 3}.IsValid())
	assert.Error(t, quorum.QuorumConfig{N: 5, W: 3, R: 6}.IsValid())
	assert.Error(t, quorum.QuorumConfig{N: 0, W: 1, R: 1}.IsValid())
}

func TestQuorumPresets(t *testing.T) {
	q := quorum.Preset(quorum.PresetQuorumConsistency, 5)
	assert.Equal(t, 5, q.N)
	assert.Equal(t, 3, q.W)
	assert.Equal(t, 3, q.R)
	assert.True(t, q.IsStronglyConsistent())

	q2 := quorum.Preset(quorum.PresetHighAvailability, 5)
	assert.Equal(t, 1, q2.W)
	assert.Equal(t, 1, q2.R)
	assert.False(t, q2.IsStronglyConsistent())
}

func TestQuorumConfig_OverlapCount(t *testing.T) {
	q := quorum.QuorumConfig{N: 5, W: 3, R: 3}
	assert.Equal(t, 1, q.OverlapCount())

	q2 := quorum.QuorumConfig{N: 5, W: 1, R: 1}
	assert.Equal(t, 0, q2.OverlapCount())
}

func TestQuorumConfig_StaleReadProbability(t *testing.T) {
	q := quorum.QuorumConfig{N: 5, W: 3, R: 3}
	assert.Equal(t, 0.0, q.StaleReadProbability(), "strongly consistent quorum has 0 stale probability")

	q2 := quorum.QuorumConfig{N: 5, W: 1, R: 1}
	assert.Greater(t, q2.StaleReadProbability(), 0.0, "W=1,R=1 should have non-zero stale probability")
}
