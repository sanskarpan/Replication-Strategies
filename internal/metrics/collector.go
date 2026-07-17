package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// percentile returns the p-th percentile (nearest-rank) of the samples; 0 if empty.
func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	rank := int(math.Ceil(p/100*float64(len(s)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return s[rank]
}

type LagSample struct {
	FollowerID string    `json:"follower_id"`
	LagEntries int64     `json:"lag_entries"`
	LagMs      int64     `json:"lag_ms"`
	Timestamp  time.Time `json:"timestamp"`
}

type NodeMetrics struct {
	mu             sync.RWMutex
	NodeID         string    `json:"node_id"`
	WritesTotal    int64     `json:"writes_total"`
	ReadsTotal     int64     `json:"reads_total"`
	ConflictsTotal int64     `json:"conflicts_total"`
	ReplicaLag     int64     `json:"replica_lag"` // entries behind
	WriteLatencyMs []float64 `json:"write_latency_ms"`
	ReadLatencyMs  []float64 `json:"read_latency_ms"`
	LastUpdated    time.Time `json:"last_updated"`
	IsLeader       bool      `json:"is_leader"`
	IsOnline       bool      `json:"is_online"`
	// Latency percentiles (populated by Snapshot; averages hide tail behavior).
	WriteP50 float64 `json:"write_p50"`
	WriteP95 float64 `json:"write_p95"`
	WriteP99 float64 `json:"write_p99"`
	ReadP50  float64 `json:"read_p50"`
	ReadP95  float64 `json:"read_p95"`
	ReadP99  float64 `json:"read_p99"`
}

func NewNodeMetrics(id string) *NodeMetrics {
	return &NodeMetrics{
		NodeID:   id,
		IsOnline: true,
	}
}

func (m *NodeMetrics) RecordWrite(latencyMs float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WritesTotal++
	m.WriteLatencyMs = append(m.WriteLatencyMs, latencyMs)
	if len(m.WriteLatencyMs) > 1000 {
		m.WriteLatencyMs = m.WriteLatencyMs[1:]
	}
	m.LastUpdated = time.Now()
}

func (m *NodeMetrics) RecordRead(latencyMs float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadsTotal++
	m.ReadLatencyMs = append(m.ReadLatencyMs, latencyMs)
	if len(m.ReadLatencyMs) > 1000 {
		m.ReadLatencyMs = m.ReadLatencyMs[1:]
	}
	m.LastUpdated = time.Now()
}

func (m *NodeMetrics) RecordConflict() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ConflictsTotal++
	m.LastUpdated = time.Now()
}

func (m *NodeMetrics) SetLag(lag int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplicaLag = lag
	m.LastUpdated = time.Now()
}

// Lag returns the current replica lag under the lock (safe for concurrent readers).
func (m *NodeMetrics) Lag() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReplicaLag
}

// SetLeader/SetOnline update the corresponding flags under the lock so concurrent
// readers (Snapshot) never race with role/state transitions.
func (m *NodeMetrics) SetLeader(v bool) {
	m.mu.Lock()
	m.IsLeader = v
	m.mu.Unlock()
}

func (m *NodeMetrics) SetOnline(v bool) {
	m.mu.Lock()
	m.IsOnline = v
	m.mu.Unlock()
}

func (m *NodeMetrics) AvgWriteLatency() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.WriteLatencyMs) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range m.WriteLatencyMs {
		sum += v
	}
	return sum / float64(len(m.WriteLatencyMs))
}

func (m *NodeMetrics) AvgReadLatency() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.ReadLatencyMs) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range m.ReadLatencyMs {
		sum += v
	}
	return sum / float64(len(m.ReadLatencyMs))
}

func (m *NodeMetrics) Snapshot() NodeMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	wl := make([]float64, len(m.WriteLatencyMs))
	copy(wl, m.WriteLatencyMs)
	rl := make([]float64, len(m.ReadLatencyMs))
	copy(rl, m.ReadLatencyMs)
	return NodeMetrics{
		NodeID:         m.NodeID,
		WritesTotal:    m.WritesTotal,
		ReadsTotal:     m.ReadsTotal,
		ConflictsTotal: m.ConflictsTotal,
		ReplicaLag:     m.ReplicaLag,
		WriteLatencyMs: wl,
		ReadLatencyMs:  rl,
		LastUpdated:    m.LastUpdated,
		IsLeader:       m.IsLeader,
		IsOnline:       m.IsOnline,
		WriteP50:       percentile(wl, 50),
		WriteP95:       percentile(wl, 95),
		WriteP99:       percentile(wl, 99),
		ReadP50:        percentile(rl, 50),
		ReadP95:        percentile(rl, 95),
		ReadP99:        percentile(rl, 99),
	}
}

type ClusterMetrics struct {
	mu             sync.RWMutex
	ClusterID      string                  `json:"cluster_id"`
	Strategy       string                  `json:"strategy"`
	NodeMetrics    map[string]*NodeMetrics `json:"node_metrics"`
	TotalWrites    int64                   `json:"total_writes"`
	TotalReads     int64                   `json:"total_reads"`
	TotalConflicts int64                   `json:"total_conflicts"`
	LagSamples     []LagSample             `json:"lag_samples"`
	StartTime      time.Time               `json:"start_time"`
}

func NewClusterMetrics(clusterID, strategy string) *ClusterMetrics {
	return &ClusterMetrics{
		ClusterID:   clusterID,
		Strategy:    strategy,
		NodeMetrics: make(map[string]*NodeMetrics),
		LagSamples:  make([]LagSample, 0, 100),
		StartTime:   time.Now(),
	}
}

func (c *ClusterMetrics) AddNode(nm *NodeMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.NodeMetrics[nm.NodeID] = nm
}

func (c *ClusterMetrics) RecordLag(sample LagSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LagSamples = append(c.LagSamples, sample)
	if len(c.LagSamples) > 500 {
		c.LagSamples = c.LagSamples[1:]
	}
}

// IncrWrites atomically increments the cluster-wide write counter.
func (c *ClusterMetrics) IncrWrites() {
	c.mu.Lock()
	c.TotalWrites++
	c.mu.Unlock()
}

// IncrReads atomically increments the cluster-wide read counter.
func (c *ClusterMetrics) IncrReads() {
	c.mu.Lock()
	c.TotalReads++
	c.mu.Unlock()
}

// ClusterSnapshot is a mutex-free snapshot of ClusterMetrics.
type ClusterSnapshot struct {
	ClusterID      string                  `json:"cluster_id"`
	Strategy       string                  `json:"strategy"`
	NodeMetrics    map[string]*NodeMetrics `json:"node_metrics"`
	TotalWrites    int64                   `json:"total_writes"`
	TotalReads     int64                   `json:"total_reads"`
	TotalConflicts int64                   `json:"total_conflicts"`
	LagSamples     []LagSample             `json:"lag_samples"`
	StartTime      time.Time               `json:"start_time"`
}

func (c *ClusterMetrics) Snapshot() ClusterSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := ClusterSnapshot{
		ClusterID:   c.ClusterID,
		Strategy:    c.Strategy,
		TotalWrites: c.TotalWrites,
		TotalReads:  c.TotalReads,
		StartTime:   c.StartTime,
	}
	// Snapshot each node's metrics (acquires per-node lock internally).
	snap.NodeMetrics = make(map[string]*NodeMetrics, len(c.NodeMetrics))
	var totalConflicts int64
	for k, v := range c.NodeMetrics {
		nm := v.Snapshot()
		snap.NodeMetrics[k] = &nm
		totalConflicts += nm.ConflictsTotal
	}
	// Aggregate conflicts from node metrics — nodes track conflicts locally and
	// there is no separate cluster-level counter incremented at write time.
	snap.TotalConflicts = totalConflicts
	lags := make([]LagSample, len(c.LagSamples))
	copy(lags, c.LagSamples)
	snap.LagSamples = lags
	return snap
}
