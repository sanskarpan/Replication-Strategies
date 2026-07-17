package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"replication-strategies/internal/consistency"
	"replication-strategies/internal/events"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// FollowerNode is a follower in a single-leader cluster.
type FollowerNode struct {
	*BaseNode
	fabric   *transport.NetworkFabric
	leaderID string
	ryw      *consistency.ReadYourWrites
	mono     *consistency.MonotonicReads
	inbox_ch chan transport.Message

	// Replication catch-up state. Entries are keyed by the leader's log index so a
	// dropped or reordered AppendEntries leaves a real gap instead of a silently
	// dense-but-wrong local log.
	applyMu     sync.Mutex
	applied     uint64                      // highest contiguous leader index applied
	pending     map[uint64]storage.LogEntry // buffered out-of-order entries
	leaderIndex uint64                      // highest leader index we know about
}

func NewFollowerNode(id, clusterID, leaderID string, fabric *transport.NetworkFabric, bus *events.EventBus) *FollowerNode {
	base := newBaseNode(id, clusterID, StrategySingleLeader, RoleFollower, bus)
	ch := make(chan transport.Message, 256)
	fabric.Register(id, ch)
	n := &FollowerNode{
		BaseNode: base,
		fabric:   fabric,
		leaderID: leaderID,
		ryw:      consistency.NewReadYourWrites(),
		mono:     consistency.NewMonotonicReads(),
		inbox_ch: ch,
		pending:  make(map[uint64]storage.LogEntry),
	}
	base.leaderID = leaderID
	return n
}

func (n *FollowerNode) Start(ctx context.Context) {
	go n.runMessageLoop()
	go n.runSyncLoop()
}

// runSyncLoop periodically asks the leader to resend any entries at or above our
// next-expected index. This recovers dropped entries — including a dropped tail
// that no later message would otherwise reveal — and is a no-op once caught up.
func (n *FollowerNode) runSyncLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if n.isPaused() {
				continue
			}
			n.applyMu.Lock()
			from := n.applied + 1
			behind := n.applied < n.leaderIndex || len(n.pending) > 0
			n.applyMu.Unlock()
			// Always probe from our next index; the leader replies with nothing when
			// we are already caught up, so steady state is quiet.
			_ = behind
			n.requestSync(from)
		}
	}
}

func (n *FollowerNode) runMessageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.inbox_ch:
			n.HandleMessage(msg)
		}
	}
}

func (n *FollowerNode) HandleMessage(raw interface{}) {
	msg, ok := raw.(transport.Message)
	if !ok {
		return
	}
	if n.isPaused() {
		return
	}
	switch msg.Type {
	case transport.MsgAppendEntries:
		n.applyEntries(msg)
	case transport.MsgReadRepair:
		if msg.Entry != nil {
			n.store.Set(msg.Entry)
		}
	}
}

func (n *FollowerNode) applyEntries(msg transport.Message) {
	n.applyMu.Lock()
	for _, entry := range msg.Entries {
		if entry.Index <= n.applied {
			continue // already applied — duplicate/resend
		}
		n.pending[entry.Index] = entry
	}
	// Apply every entry that is now contiguous from applied+1.
	for {
		next := n.applied + 1
		entry, ok := n.pending[next]
		if !ok {
			break
		}
		n.applyOne(entry)
		delete(n.pending, next)
		n.applied = next
	}
	if msg.AckIndex > n.leaderIndex {
		n.leaderIndex = msg.AckIndex
	}
	applied := n.applied
	leaderLast := n.leaderIndex
	gap := len(n.pending) > 0
	n.applyMu.Unlock()

	// send ack to leader (acks the message's seq for sync/semi-sync quorum waiters)
	ack := transport.Message{
		Type:     transport.MsgAppendAck,
		SenderID: n.id,
		TargetID: n.leaderID,
		AckIndex: msg.SeqNo,
	}
	n.fabric.Send(ack)

	// If a gap remains (missing lower indices), ask the leader to resend from our
	// next-expected index so we can fill the hole.
	if gap {
		n.requestSync(applied + 1)
	}

	// Lag is measured against the highest contiguous index actually applied.
	var lag int64
	if leaderLast > applied {
		lag = int64(leaderLast - applied)
	}
	n.metrics.SetLag(lag)

	if lag > 0 {
		n.publishEvent(events.EvtFollowerLag, map[string]interface{}{
			"follower_id":   n.id,
			"lag_entries":   lag,
			"lag_ms":        lag * 10, // rough estimate
			"leader_commit": leaderLast,
			"my_commit":     applied,
		})
	} else {
		n.publishEvent(events.EvtEntryReplicated, map[string]interface{}{
			"follower_id": n.id,
			"index":       applied,
		})
	}
}

// applyOne applies a single log entry to the store and display log. Caller holds applyMu.
func (n *FollowerNode) applyOne(entry storage.LogEntry) {
	n.log.Append(entry)
	kvEntry := &storage.KVEntry{
		Key:       entry.Key,
		Value:     entry.Value,
		VClock:    entry.VClock,
		Timestamp: entry.Timestamp,
		NodeID:    entry.OriginID,
		Version:   entry.Index,
	}
	if entry.Op == storage.OpDelete {
		kvEntry.Tombstone = true
	}
	n.store.Set(kvEntry)
	n.log.SetCommitIndex(entry.Index)
}

// requestSync asks the leader to resend log entries starting at fromIndex.
func (n *FollowerNode) requestSync(fromIndex uint64) {
	n.fabric.Send(transport.Message{
		Type:     transport.MsgSync,
		SenderID: n.id,
		TargetID: n.leaderID,
		AckIndex: fromIndex,
	})
}

func (n *FollowerNode) Write(key string, value []byte, clientID string) (*storage.KVEntry, error) {
	return nil, fmt.Errorf("follower %s: writes must go to leader %s", n.id, n.leaderID)
}

func (n *FollowerNode) Delete(key string, clientID string) error {
	return fmt.Errorf("follower %s: deletes must go to leader %s", n.id, n.leaderID)
}

func (n *FollowerNode) Read(key string, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	entry, ok := n.store.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	n.mono.RecordRead(clientID, entry)
	n.metrics.RecordRead(0)
	return entry, nil
}

// Satisfy Node interface
func (n *FollowerNode) GetPeers() []string   { return n.BaseNode.GetPeers() }
func (n *FollowerNode) AddPeer(id string)    { n.BaseNode.AddPeer(id) }
func (n *FollowerNode) RemovePeer(id string) { n.BaseNode.RemovePeer(id) }

var _ Node = (*FollowerNode)(nil)
