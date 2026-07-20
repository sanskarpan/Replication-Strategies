package simulation

import (
	"sync"

	"replication-strategies/internal/events"
)

const (
	historyMaxSize   = 1000
	snapshotInterval = 50
)

// HistoryEntry is one frame in a cluster's ordered event history.
type HistoryEntry struct {
	Seq   uint64       `json:"seq"`
	Event events.Event `json:"event"`
	// State is a ClusterState snapshot; present every snapshotInterval entries and
	// whenever a structural topology change (partition, node state, leader election) occurs.
	State *ClusterState `json:"state,omitempty"`
}

// ClusterEventHistory is a bounded, ordered per-cluster event log with periodic
// ClusterState snapshots so callers can reconstruct cluster state at any seq number.
type ClusterEventHistory struct {
	mu      sync.RWMutex
	entries []HistoryEntry
	maxSize int
	seq     uint64
}

func newClusterEventHistory() *ClusterEventHistory {
	return &ClusterEventHistory{
		entries: make([]HistoryEntry, 0, historyMaxSize),
		maxSize: historyMaxSize,
	}
}

// isStructuralEvent reports whether the event type implies a topology change that
// warrants an immediate snapshot (independent of the interval counter).
func isStructuralEvent(t events.EventType) bool {
	switch t {
	case events.EvtPartitionCreated, events.EvtPartitionHealed,
		events.EvtNodeStateChanged, events.EvtLeaderElected:
		return true
	}
	return false
}

// Append records an event. getSnapshot is called (while the lock is held) when a
// snapshot should be stored alongside the entry; lock ordering is always
// history.mu → cluster.mu so there is no deadlock risk.
func (h *ClusterEventHistory) Append(evt events.Event, getSnapshot func() ClusterState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	var snap *ClusterState
	if isStructuralEvent(evt.Type) || h.seq%snapshotInterval == 0 {
		s := getSnapshot()
		snap = &s
	}
	entry := HistoryEntry{Seq: h.seq, Event: evt, State: snap}
	if len(h.entries) >= h.maxSize {
		h.entries = h.entries[1:]
	}
	h.entries = append(h.entries, entry)
}

// Get returns entries with Seq >= from, up to limit (capped at 500).
func (h *ClusterEventHistory) Get(from, limit uint64) []HistoryEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if limit == 0 || limit > 500 {
		limit = 500
	}
	result := make([]HistoryEntry, 0, limit)
	for _, e := range h.entries {
		if e.Seq >= from {
			result = append(result, e)
			if uint64(len(result)) >= limit {
				break
			}
		}
	}
	return result
}

// HistoryStateResult carries the fold base for scrubber replay.
type HistoryStateResult struct {
	BaseSeq   uint64         `json:"base_seq"`
	BaseState *ClusterState  `json:"base_state"`
	Tail      []HistoryEntry `json:"tail"`
	MaxSeq    uint64         `json:"max_seq"`
}

// StateAt returns the most recent snapshot at or before targetSeq plus all events
// from that snapshot forward up to targetSeq, so the caller can fold them to
// reconstruct exact cluster state at targetSeq.
func (h *ClusterEventHistory) StateAt(targetSeq uint64) HistoryStateResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var baseState *ClusterState
	var baseSeq uint64
	for _, e := range h.entries {
		if e.State != nil && e.Seq <= targetSeq {
			baseState = e.State
			baseSeq = e.Seq
		}
	}
	tail := make([]HistoryEntry, 0)
	for _, e := range h.entries {
		if e.Seq > baseSeq && e.Seq <= targetSeq {
			tail = append(tail, e)
		}
	}
	return HistoryStateResult{
		BaseSeq:   baseSeq,
		BaseState: baseState,
		Tail:      tail,
		MaxSeq:    h.seq,
	}
}

// MaxSeq returns the highest recorded sequence number (0 if empty).
func (h *ClusterEventHistory) MaxSeq() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.seq
}
