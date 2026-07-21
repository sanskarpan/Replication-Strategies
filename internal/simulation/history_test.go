package simulation

import (
	"testing"
	"time"

	"replication-strategies/internal/events"
)

func makeEvt(t events.EventType, clusterID string) events.Event {
	return events.Event{Type: t, ClusterID: clusterID, Timestamp: time.Now()}
}

func noSnapshot() ClusterState { return ClusterState{} }

// TestHistoryAppendAndGet verifies sequential seq assignment and range queries.
func TestHistoryAppendAndGet(t *testing.T) {
	h := newClusterEventHistory("test-cluster", nil)
	for i := 0; i < 10; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c1"), noSnapshot)
	}
	if h.MaxSeq() != 10 {
		t.Fatalf("expected MaxSeq 10, got %d", h.MaxSeq())
	}
	all := h.Get(0, 500)
	if len(all) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(all))
	}
	// from=5 should return entries 5..10
	tail := h.Get(5, 500)
	if len(tail) != 6 {
		t.Fatalf("expected 6 entries from seq 5, got %d", len(tail))
	}
	if tail[0].Seq != 5 {
		t.Fatalf("first tail entry seq expected 5, got %d", tail[0].Seq)
	}
}

// TestHistoryRingBuffer verifies that the oldest entries are evicted when the
// buffer is full and seq numbers continue to grow monotonically.
func TestHistoryRingBuffer(t *testing.T) {
	h := &ClusterEventHistory{
		entries: make([]HistoryEntry, 0, 5),
		maxSize: 5,
	}
	for i := 0; i < 8; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c2"), noSnapshot)
	}
	if h.MaxSeq() != 8 {
		t.Fatalf("MaxSeq expected 8, got %d", h.MaxSeq())
	}
	got := h.Get(0, 500)
	if len(got) != 5 {
		t.Fatalf("ring buffer should hold 5 entries, got %d", len(got))
	}
	// Oldest retained entry should be seq 4 (8-5+1).
	if got[0].Seq != 4 {
		t.Fatalf("expected oldest seq 4, got %d", got[0].Seq)
	}
}

// TestHistorySnapshotOnStructural verifies that structural events always carry a snapshot.
func TestHistorySnapshotOnStructural(t *testing.T) {
	h := newClusterEventHistory("test-cluster", nil)
	snapCalled := false
	snap := func() ClusterState {
		snapCalled = true
		return ClusterState{ID: "c3"}
	}
	h.Append(makeEvt(events.EvtPartitionCreated, "c3"), snap)
	if !snapCalled {
		t.Fatal("snapshot func not called on structural event")
	}
	entries := h.Get(0, 500)
	if entries[0].State == nil {
		t.Fatal("structural entry should have a non-nil State snapshot")
	}
	if entries[0].State.ID != "c3" {
		t.Fatalf("snapshot cluster ID expected 'c3', got '%s'", entries[0].State.ID)
	}
}

// TestHistorySnapshotInterval verifies periodic snapshots fire every 50 entries.
func TestHistorySnapshotInterval(t *testing.T) {
	h := newClusterEventHistory("test-cluster", nil)
	snapCount := 0
	snap := func() ClusterState {
		snapCount++
		return ClusterState{}
	}
	// Send 100 non-structural events — expect snapshots at seq 50 and 100.
	for i := 0; i < 100; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c4"), snap)
	}
	if snapCount != 2 {
		t.Fatalf("expected 2 periodic snapshots, got %d", snapCount)
	}
}

// TestHistoryStateAt verifies the fold-base logic: returns the most recent prior
// snapshot and the events after it up to targetSeq.
func TestHistoryStateAt(t *testing.T) {
	h := &ClusterEventHistory{
		entries: make([]HistoryEntry, 0, historyMaxSize),
		maxSize: historyMaxSize,
	}
	// First 50 events — structural at seq 1 to get a snapshot.
	h.Append(makeEvt(events.EvtPartitionCreated, "c5"), func() ClusterState { return ClusterState{ID: "snap-1"} })
	// 48 more non-structural events (seqs 2–49, none are multiples of 50 yet).
	for i := 0; i < 48; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c5"), noSnapshot)
	}
	// seq 50 is a periodic snapshot.
	h.Append(makeEvt(events.EvtWriteReceived, "c5"), func() ClusterState { return ClusterState{ID: "snap-50"} })
	// Add 5 more events: seqs 51–55.
	for i := 0; i < 5; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c5"), noSnapshot)
	}

	// Ask for state at seq 53: base should be seq 50 (snapshot), tail should be 51..53.
	res := h.StateAt(53)
	if res.BaseSeq != 50 {
		t.Fatalf("expected base_seq 50, got %d", res.BaseSeq)
	}
	if res.BaseState == nil || res.BaseState.ID != "snap-50" {
		t.Fatalf("expected base_state ID 'snap-50', got %v", res.BaseState)
	}
	if len(res.Tail) != 3 {
		t.Fatalf("expected 3 tail entries (51..53), got %d", len(res.Tail))
	}
	if res.Tail[0].Seq != 51 {
		t.Fatalf("first tail seq expected 51, got %d", res.Tail[0].Seq)
	}
	if res.MaxSeq != 55 {
		t.Fatalf("MaxSeq in result expected 55, got %d", res.MaxSeq)
	}
}

// TestHistoryGetLimit verifies the 500-entry cap.
func TestHistoryGetLimit(t *testing.T) {
	h := newClusterEventHistory("test-cluster", nil)
	for i := 0; i < 200; i++ {
		h.Append(makeEvt(events.EvtWriteReceived, "c6"), noSnapshot)
	}
	got := h.Get(0, 0) // 0 → default cap of 500
	if len(got) != 200 {
		t.Fatalf("expected 200 entries, got %d", len(got))
	}
	// Explicit limit lower than total.
	got10 := h.Get(0, 10)
	if len(got10) != 10 {
		t.Fatalf("limit=10 should return 10 entries, got %d", len(got10))
	}
}

// TestIsStructuralEvent verifies the helper covers all four structural types.
func TestIsStructuralEvent(t *testing.T) {
	structural := []events.EventType{
		events.EvtPartitionCreated,
		events.EvtPartitionHealed,
		events.EvtNodeStateChanged,
		events.EvtLeaderElected,
	}
	nonStructural := []events.EventType{
		events.EvtWriteReceived,
		events.EvtReadReceived,
		events.EvtFollowerLag,
		events.EvtConflictDetected,
	}
	for _, et := range structural {
		if !isStructuralEvent(et) {
			t.Errorf("expected %s to be structural", et)
		}
	}
	for _, et := range nonStructural {
		if isStructuralEvent(et) {
			t.Errorf("expected %s to NOT be structural", et)
		}
	}
}
