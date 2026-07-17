package consistency

import (
    "replication-strategies/internal/storage"
)

type ReadConsistency string

const (
    ConsistencyStrong    ReadConsistency = "strong"
    ConsistencyEventual  ReadConsistency = "eventual"
    ConsistencySession   ReadConsistency = "session"
    ConsistencyMonotonic ReadConsistency = "monotonic"
)

// These guarantees compare causal position using storage.KVEntry.VClock (vector
// clocks), which are globally comparable across nodes. They are therefore correct
// regardless of which replica serves a read, in any replication strategy. (They are
// currently wired into single-leader nodes, where the leader stamps a monotonic
// vector clock; leaderless/multi-leader nodes could adopt them without change.)
type ConsistencyGuarantee interface {
    Name() string
    // ValidateRead checks if the proposed read satisfies the guarantee.
    // Returns error if it would violate the guarantee.
    ValidateRead(clientID string, proposed *storage.KVEntry) error
    // RecordRead updates client state after a successful read.
    RecordRead(clientID string, entry *storage.KVEntry)
    // RecordWrite updates client state after a write.
    RecordWrite(clientID string, entry *storage.KVEntry)
}
