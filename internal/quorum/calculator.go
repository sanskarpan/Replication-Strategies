package quorum

import (
    "fmt"
)

type QuorumConfig struct {
    N int `json:"n"` // total replicas
    W int `json:"w"` // write quorum
    R int `json:"r"` // read quorum
}

type QuorumPreset string

const (
    PresetStrongConsistency  QuorumPreset = "strong"    // W=N, R=1 or W=1, R=N
    PresetQuorumConsistency  QuorumPreset = "quorum"    // W=ceil(N/2)+1, R=ceil(N/2)+1
    PresetHighAvailability   QuorumPreset = "ha"        // W=1, R=1
    PresetWriteOptimized     QuorumPreset = "write_opt" // W=1, R=N
    PresetReadOptimized      QuorumPreset = "read_opt"  // W=N, R=1
)

func Preset(p QuorumPreset, n int) QuorumConfig {
    switch p {
    case PresetStrongConsistency:
        return QuorumConfig{N: n, W: n, R: 1}
    case PresetQuorumConsistency:
        q := n/2 + 1
        return QuorumConfig{N: n, W: q, R: q}
    case PresetHighAvailability:
        return QuorumConfig{N: n, W: 1, R: 1}
    case PresetWriteOptimized:
        return QuorumConfig{N: n, W: 1, R: n}
    case PresetReadOptimized:
        return QuorumConfig{N: n, W: n, R: 1}
    default:
        q := n/2 + 1
        return QuorumConfig{N: n, W: q, R: q}
    }
}

func (q QuorumConfig) IsValid() error {
    if q.N <= 0 {
        return fmt.Errorf("N must be > 0, got %d", q.N)
    }
    if q.W <= 0 || q.W > q.N {
        return fmt.Errorf("W must be in [1, N=%d], got %d", q.N, q.W)
    }
    if q.R <= 0 || q.R > q.N {
        return fmt.Errorf("R must be in [1, N=%d], got %d", q.N, q.R)
    }
    return nil
}

// IsStronglyConsistent returns true if W + R > N (quorum overlap guaranteed)
func (q QuorumConfig) IsStronglyConsistent() bool {
    return q.W+q.R > q.N
}

// StaleReadProbability gives a rough estimate of the probability of reading
// stale data when W+R <= N (no overlap guarantee).
// This is a simplified model: P(stale) ≈ (N-W)! / N! * R!
func (q QuorumConfig) StaleReadProbability() float64 {
    if q.IsStronglyConsistent() {
        return 0.0
    }
    // simplified: fraction of non-overlap cases
    nonWritten := q.N - q.W
    if nonWritten <= 0 {
        return 0.0
    }
    // P(all R nodes are in the nonWritten set)
    // = C(N-W, R) / C(N, R)
    denom := choose(q.N, q.R)
    if denom == 0 {
        return 0.0
    }
    return choose(nonWritten, q.R) / denom
}

// choose computes the binomial coefficient C(n, k) in float64 so large N cannot
// overflow (the previous int version overflowed for modest N).
func choose(n, k int) float64 {
    if k < 0 || k > n {
        return 0
    }
    if k == 0 || k == n {
        return 1
    }
    if k > n-k {
        k = n - k
    }
    result := 1.0
    for i := 0; i < k; i++ {
        result = result * float64(n-i) / float64(i+1)
    }
    return result
}

// WriteNodes returns the minimum required write acks for success
func (q QuorumConfig) WriteNodes() int { return q.W }

// ReadNodes returns the number of nodes to query on read
func (q QuorumConfig) ReadNodes() int { return q.R }

// TotalNodes returns N
func (q QuorumConfig) TotalNodes() int { return q.N }

// OverlapCount returns guaranteed overlap between write and read sets
func (q QuorumConfig) OverlapCount() int {
    overlap := q.W + q.R - q.N
    if overlap < 0 {
        return 0
    }
    return overlap
}
