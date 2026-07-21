package conflict

import (
	"replication-strategies/internal/storage"
	"testing"
)

func FuzzLWWResolver(f *testing.F) {
	// Seed corpus: different timestamps, tie (same ts), zero timestamps, fully tied.
	f.Add(int64(100), int64(200), "node-a", "node-b")
	f.Add(int64(100), int64(100), "node-a", "node-b") // tie: node-b wins lexicographically
	f.Add(int64(0), int64(0), "a", "a")               // fully tied, same node
	f.Add(int64(1), int64(0), "z", "a")               // local newer

	f.Fuzz(func(t *testing.T, lts, rts int64, lnode, rnode string) {
		if lnode == "" {
			lnode = "l"
		}
		if rnode == "" {
			rnode = "r"
		}

		local := &storage.KVEntry{Key: "k", Value: []byte("local"), Timestamp: lts, NodeID: lnode}
		remote := &storage.KVEntry{Key: "k", Value: []byte("remote"), Timestamp: rts, NodeID: rnode}
		c := &Conflict{ID: "fuzz-c", Key: "k", Local: local, Remote: remote, NodeID: lnode}

		r := NewLWWResolver()

		// Property 1: Never returns nil.
		res1 := r.Resolve(c)
		if res1 == nil || res1.Winner == nil {
			t.Fatal("LWWResolver returned nil resolution or nil winner")
		}

		// Property 2: Deterministic — second call with same input yields same winner.
		res2 := r.Resolve(c)
		if res2 == nil || res2.Winner == nil {
			t.Fatal("LWWResolver returned nil on second call")
		}
		if res1.Winner.NodeID != res2.Winner.NodeID || res1.Winner.Timestamp != res2.Winner.Timestamp {
			t.Fatalf("non-deterministic: call1 winner=%q ts=%d, call2 winner=%q ts=%d",
				res1.Winner.NodeID, res1.Winner.Timestamp,
				res2.Winner.NodeID, res2.Winner.Timestamp)
		}

		// Property 3: Winner commutativity — swapping local/remote yields the same winning node.
		// The winner is defined by (timestamp, nodeID tiebreak), not by argument position.
		cSwap := &Conflict{ID: "fuzz-swap", Key: "k", Local: remote, Remote: local, NodeID: rnode}
		resSwap := r.Resolve(cSwap)
		if resSwap == nil || resSwap.Winner == nil {
			t.Fatal("LWWResolver returned nil for swapped conflict")
		}
		if res1.Winner.NodeID != resSwap.Winner.NodeID {
			t.Fatalf("non-commutative: original winner=%q ts=%d, swapped winner=%q ts=%d (lts=%d rts=%d lnode=%q rnode=%q)",
				res1.Winner.NodeID, res1.Winner.Timestamp,
				resSwap.Winner.NodeID, resSwap.Winner.Timestamp,
				lts, rts, lnode, rnode)
		}
	})
}
