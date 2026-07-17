package node

import (
	"testing"

	"replication-strategies/internal/storage"
)

// ISSUE-8: entryWins must break timestamp ties deterministically by NodeID so all
// replicas converge (matching LWWResolver), instead of keeping whatever arrived first.
func TestEntryWins_TiebreakByNodeID(t *testing.T) {
	hi := &storage.KVEntry{Timestamp: 100, NodeID: "nb"}
	lo := &storage.KVEntry{Timestamp: 100, NodeID: "na"}

	if !entryWins(hi, lo) {
		t.Fatal("on equal timestamp the higher NodeID must win")
	}
	if entryWins(lo, hi) {
		t.Fatal("on equal timestamp the lower NodeID must not win")
	}

	newer := &storage.KVEntry{Timestamp: 200, NodeID: "na"}
	older := &storage.KVEntry{Timestamp: 100, NodeID: "nz"}
	if !entryWins(newer, older) {
		t.Fatal("higher timestamp must win regardless of NodeID")
	}
	if entryWins(older, newer) {
		t.Fatal("lower timestamp must not win")
	}
}
