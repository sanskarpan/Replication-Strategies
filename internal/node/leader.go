package node

import (
	"context"
	"fmt"
	"time"

	"replication-strategies/internal/consistency"
	"replication-strategies/internal/events"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

type ReplicationMode string

const (
	ModeAsync    ReplicationMode = "async"
	ModeSync     ReplicationMode = "sync"
	ModeSemiSync ReplicationMode = "semi_sync"
)

// SingleLeaderNode is the leader in a single-leader cluster.
// There is exactly one leader; followers are separate nodes.
type SingleLeaderNode struct {
	*BaseNode
	fabric   *transport.NetworkFabric
	mode     ReplicationMode
	ryw      *consistency.ReadYourWrites
	mono     *consistency.MonotonicReads
	seqNo    uint64
	clock    uint64                 // leader's logical clock; drives the write vector clock
	ackChs   map[uint64]chan string // seqNo -> ack channel
	inbox_ch chan transport.Message // registered with the fabric
}

func NewSingleLeaderNode(id, clusterID string, fabric *transport.NetworkFabric, bus *events.EventBus, mode ReplicationMode) *SingleLeaderNode {
	base := newBaseNode(id, clusterID, StrategySingleLeader, RoleLeader, bus)
	ch := make(chan transport.Message, 256)
	fabric.Register(id, ch)
	n := &SingleLeaderNode{
		BaseNode: base,
		fabric:   fabric,
		mode:     mode,
		ryw:      consistency.NewReadYourWrites(),
		mono:     consistency.NewMonotonicReads(),
		ackChs:   make(map[uint64]chan string),
		inbox_ch: ch,
	}
	return n
}

func (n *SingleLeaderNode) Start(ctx context.Context) {
	go n.runMessageLoop()
}

// SetMode changes the replication mode at runtime (async/sync/semi-sync).
func (n *SingleLeaderNode) SetMode(m ReplicationMode) {
	n.mu.Lock()
	n.mode = m
	n.mu.Unlock()
}

func (n *SingleLeaderNode) getMode() ReplicationMode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.mode
}

// awaitReplication broadcasts msg and waits for follower acks per the replication
// mode, returning an error when the mode's durability contract was not met. Used by
// both Write and Delete so deletes honor sync/semi-sync durability too.
func (n *SingleLeaderNode) awaitReplication(mode ReplicationMode, seqNo uint64, peers []string, msg transport.Message) error {
	switch mode {
	case ModeSync:
		ackCh := make(chan string, len(peers))
		n.mu.Lock()
		n.ackChs[seqNo] = ackCh
		n.mu.Unlock()
		n.fabric.Broadcast(msg, peers)
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		received := make(map[string]bool)
	syncLoop:
		for len(received) < len(peers) {
			select {
			case senderID := <-ackCh:
				received[senderID] = true
			case <-timer.C:
				break syncLoop
			}
		}
		n.mu.Lock()
		delete(n.ackChs, seqNo)
		n.mu.Unlock()
		if len(received) < len(peers) {
			return fmt.Errorf("sync replication incomplete: %d/%d followers acked within timeout", len(received), len(peers))
		}

	case ModeSemiSync:
		ackCh := make(chan string, len(peers))
		n.mu.Lock()
		n.ackChs[seqNo] = ackCh
		n.mu.Unlock()
		n.fabric.Broadcast(msg, peers)
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		got := len(peers) == 0 // no followers => trivially satisfied
		if !got {
			select {
			case <-ackCh:
				got = true
			case <-timer.C:
			}
		}
		n.mu.Lock()
		delete(n.ackChs, seqNo)
		n.mu.Unlock()
		if !got {
			return fmt.Errorf("semi-sync replication timed out: no follower ack within 500ms")
		}

	default: // async
		go n.fabric.Broadcast(msg, peers)
	}
	return nil
}

func (n *SingleLeaderNode) runMessageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.inbox_ch:
			n.HandleMessage(msg)
		}
	}
}

func (n *SingleLeaderNode) HandleMessage(raw interface{}) {
	msg, ok := raw.(transport.Message)
	if !ok {
		return
	}
	switch msg.Type {
	case transport.MsgAppendAck:
		n.mu.Lock()
		ch, ok := n.ackChs[msg.AckIndex]
		n.mu.Unlock()
		if ok {
			select {
			case ch <- msg.SenderID:
			default:
			}
		}
	case transport.MsgSync:
		// A follower is catching up: resend log entries from the requested index.
		entries := n.log.GetFrom(msg.AckIndex)
		if len(entries) == 0 {
			return
		}
		n.fabric.Send(transport.Message{
			Type:     transport.MsgAppendEntries,
			SenderID: n.id,
			TargetID: msg.SenderID,
			Entries:  entries,
			AckIndex: n.log.LastIndex(),
		})
	}
}

func (n *SingleLeaderNode) Write(key string, value []byte, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	start := time.Now()

	ts := n.HLCNow()
	// The leader's monotonic logical clock drives the vector clock, so successive
	// writes strictly dominate one another — this is what makes the vector-clock-based
	// consistency guarantees (RYW/monotonic) work across the leader and its followers.
	n.mu.Lock()
	n.clock++
	vc := storage.VectorClock{n.id: n.clock}
	n.mu.Unlock()

	entry := storage.LogEntry{
		Key:       key,
		Value:     value,
		Op:        storage.OpSet,
		Timestamp: ts,
		OriginID:  n.id,
		VClock:    vc,
	}

	idx := n.log.Append(entry)
	entry.Index = idx // the replicated copy must carry the leader's log index
	n.log.SetCommitIndex(idx)

	kvEntry := &storage.KVEntry{
		Key:       key,
		Value:     value,
		VClock:    vc,
		Timestamp: ts,
		NodeID:    n.id,
		Version:   idx,
	}
	n.store.Set(kvEntry)

	n.metrics.RecordWrite(float64(time.Since(start).Milliseconds()))

	n.ryw.RecordWrite(clientID, kvEntry)

	peers := n.GetPeers()
	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	n.mu.Unlock()

	msg := transport.Message{
		Type:     transport.MsgAppendEntries,
		SeqNo:    seqNo,
		SenderID: n.id,
		Entries:  []storage.LogEntry{entry},
		AckIndex: idx,
	}

	// The write is always committed locally; the returned error (if any) tells the
	// client the replicas did not acknowledge in time under the current mode.
	mode := n.getMode()
	ackErr := n.awaitReplication(mode, seqNo, peers, msg)

	n.publishEvent(events.EvtWriteReceived, map[string]interface{}{
		"key":     key,
		"version": idx,
		"mode":    string(mode),
	})

	if ackErr != nil {
		return kvEntry, ackErr
	}
	return kvEntry, nil
}

// Delete tombstones a key on the leader and replicates the deletion to followers
// as an OpDelete log entry (followers already apply tombstones in applyEntries).
func (n *SingleLeaderNode) Delete(key string, clientID string) error {
	if n.isPaused() {
		return fmt.Errorf("node %s is paused/offline", n.id)
	}
	ts := n.HLCNow()
	n.mu.Lock()
	n.clock++
	vc := storage.VectorClock{n.id: n.clock}
	n.mu.Unlock()

	entry := storage.LogEntry{
		Key:       key,
		Op:        storage.OpDelete,
		Timestamp: ts,
		OriginID:  n.id,
		VClock:    vc,
	}
	idx := n.log.Append(entry)
	entry.Index = idx // the replicated copy must carry the leader's log index
	n.log.SetCommitIndex(idx)
	n.store.Delete(key, n.id, ts, vc)

	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	n.mu.Unlock()

	msg := transport.Message{
		Type:     transport.MsgAppendEntries,
		SeqNo:    seqNo,
		SenderID: n.id,
		Entries:  []storage.LogEntry{entry},
		AckIndex: idx,
	}
	// Deletes honor the replication mode just like writes (was always async before).
	ackErr := n.awaitReplication(n.getMode(), seqNo, n.GetPeers(), msg)

	n.publishEvent(events.EvtWriteReceived, map[string]interface{}{
		"key":     key,
		"version": idx,
		"op":      "delete",
	})
	return ackErr
}

func (n *SingleLeaderNode) Read(key string, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}

	entry, ok := n.store.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}

	if err := n.ryw.ValidateRead(clientID, entry); err != nil {
		return nil, err
	}
	if err := n.mono.ValidateRead(clientID, entry); err != nil {
		return nil, err
	}

	n.ryw.RecordRead(clientID, entry)
	n.mono.RecordRead(clientID, entry)

	n.metrics.RecordRead(0)
	return entry, nil
}
