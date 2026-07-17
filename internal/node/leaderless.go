package node

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"replication-strategies/internal/events"
	"replication-strategies/internal/quorum"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// readResp pairs a read response entry with the ID of the node that responded.
// This is required so read repair can target the responder, not the original writer.
type readResp struct {
	senderID string
	entry    *storage.KVEntry // nil if the node didn't have the key
}

// LeaderlessNode implements Dynamo-style quorum replication.
// All nodes are equal replicas; a "coordinator" node fans out
// writes/reads and collects responses.
type LeaderlessNode struct {
	*BaseNode
	fabric   *transport.NetworkFabric
	qConfig  quorum.QuorumConfig
	inbox_ch chan transport.Message
	allNodes []string                      // all node IDs in the cluster including self
	hints    map[string][]*storage.KVEntry // hinted handoff buffer: targetNodeID -> pending entries
	hintsMu  sync.Mutex
	// applyMu makes the store read-modify-write in applyWrite atomic, so a local
	// coordinator apply and a concurrent replicated apply for the same key cannot
	// interleave and lose the newer version (which would break LWW convergence).
	applyMu sync.Mutex
	seqNo   uint64
	// pending ack channels keyed by seqNo
	writeAcks map[uint64]chan string
	readResps map[uint64]chan readResp
}

func NewLeaderlessNode(id, clusterID string, fabric *transport.NetworkFabric, bus *events.EventBus, qConfig quorum.QuorumConfig) *LeaderlessNode {
	base := newBaseNode(id, clusterID, StrategyLeaderless, RoleReplica, bus)
	ch := make(chan transport.Message, 256)
	fabric.Register(id, ch)
	n := &LeaderlessNode{
		BaseNode:  base,
		fabric:    fabric,
		qConfig:   qConfig,
		inbox_ch:  ch,
		allNodes:  []string{id},
		hints:     make(map[string][]*storage.KVEntry),
		writeAcks: make(map[uint64]chan string),
		readResps: make(map[uint64]chan readResp),
	}
	return n
}

func (n *LeaderlessNode) Start(ctx context.Context) {
	go n.runMessageLoop()
	go n.runHintedHandoff()
}

func (n *LeaderlessNode) runMessageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.inbox_ch:
			n.HandleMessage(msg)
		}
	}
}

func (n *LeaderlessNode) runHintedHandoff() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.deliverHints()
		}
	}
}

func (n *LeaderlessNode) deliverHints() {
	n.hintsMu.Lock()
	defer n.hintsMu.Unlock()
	for targetID, entries := range n.hints {
		if len(entries) == 0 {
			continue
		}
		// Try to deliver
		for _, entry := range entries {
			msg := transport.Message{
				Type:           transport.MsgHintedHandoff,
				SenderID:       n.id,
				TargetID:       targetID,
				Entry:          entry,
				OriginalTarget: targetID,
			}
			n.fabric.Send(msg)
		}
		n.publishEvent(events.EvtHintedHandoff, map[string]interface{}{
			"target":  targetID,
			"entries": len(entries),
		})
		delete(n.hints, targetID)
	}
}

func (n *LeaderlessNode) HandleMessage(raw interface{}) {
	msg, ok := raw.(transport.Message)
	if !ok {
		return
	}
	if n.isPaused() {
		return
	}
	switch msg.Type {
	case transport.MsgWrite, transport.MsgHintedHandoff:
		if msg.Entry != nil {
			n.applyWrite(msg.Entry)
			// ack
			ack := transport.Message{
				Type:     transport.MsgWriteAck,
				SenderID: n.id,
				TargetID: msg.SenderID,
				SeqNo:    msg.SeqNo,
				Entry:    msg.Entry,
			}
			n.fabric.Send(ack)
		}
	case transport.MsgWriteAck:
		// Dispatch to any pending write quorum waiter
		n.mu.Lock()
		if ch, ok := n.writeAcks[msg.SeqNo]; ok {
			select {
			case ch <- msg.SenderID:
			default:
			}
		}
		n.mu.Unlock()
	case transport.MsgClientRead:
		// Return our version INCLUDING tombstones (GetRaw), so a delete participates in
		// read reconciliation and can win over a stale live replica.
		entry, ok := n.store.GetRaw(msg.Key)
		var resp transport.Message
		if ok {
			resp = transport.Message{
				Type:     transport.MsgSyncAck,
				SenderID: n.id,
				TargetID: msg.SenderID,
				SeqNo:    msg.SeqNo,
				Key:      msg.Key,
				Entry:    entry,
			}
		} else {
			resp = transport.Message{
				Type:     transport.MsgSyncAck,
				SenderID: n.id,
				TargetID: msg.SenderID,
				SeqNo:    msg.SeqNo,
				Key:      msg.Key,
				Error:    "not_found",
			}
		}
		n.fabric.Send(resp)
	case transport.MsgSyncAck:
		// Dispatch to any pending read quorum waiter, preserving sender identity
		// so read repair can target the correct (stale) node.
		n.mu.Lock()
		if ch, ok := n.readResps[msg.SeqNo]; ok {
			select {
			case ch <- readResp{senderID: msg.SenderID, entry: msg.Entry}:
			default:
			}
		}
		n.mu.Unlock()
	case transport.MsgReadRepair:
		if msg.Entry != nil {
			n.applyWrite(msg.Entry)
			n.publishEvent(events.EvtReadRepair, map[string]interface{}{
				"key":    msg.Entry.Key,
				"source": msg.SenderID,
			})
		}
	}
}

func (n *LeaderlessNode) applyWrite(entry *storage.KVEntry) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	existing, ok := n.store.GetRaw(entry.Key)
	if !ok {
		n.store.Set(entry)
		return
	}
	// Keep the newer version using a total order so all replicas converge even when
	// two writes share the same nanosecond timestamp. Held under applyMu so the
	// compare-and-set is atomic against concurrent applies.
	if entryWins(entry, existing) {
		n.store.Set(entry)
	}
}

// entryWins reports whether a should win over b under Last-Write-Wins with a
// deterministic NodeID tiebreak on equal timestamps. This matches LWWResolver so
// leaderless reconciliation and conflict resolution agree, guaranteeing convergence.
func entryWins(a, b *storage.KVEntry) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp > b.Timestamp
	}
	return a.NodeID > b.NodeID
}

// Write is the coordinator write: fan out to all nodes, wait for W acks
func (n *LeaderlessNode) Write(key string, value []byte, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	vc := storage.NewVectorClock().Increment(n.id)
	kvEntry := &storage.KVEntry{
		Key:       key,
		Value:     value,
		VClock:    vc,
		Timestamp: time.Now().UnixNano(),
		NodeID:    n.id,
	}
	return n.coordinate(kvEntry)
}

// Delete writes a tombstone for the key and replicates it under the same quorum
// rules as a write (tombstone with the newest timestamp wins during reconciliation).
func (n *LeaderlessNode) Delete(key string, clientID string) error {
	if n.isPaused() {
		return fmt.Errorf("node %s is paused/offline", n.id)
	}
	vc := storage.NewVectorClock().Increment(n.id)
	tomb := &storage.KVEntry{
		Key:       key,
		VClock:    vc,
		Timestamp: time.Now().UnixNano(),
		NodeID:    n.id,
		Tombstone: true,
	}
	_, err := n.coordinate(tomb)
	return err
}

// coordinate applies an entry locally then fans it out to all replicas, waiting for
// the write quorum W and buffering hinted-handoff entries for unreachable targets.
func (n *LeaderlessNode) coordinate(kvEntry *storage.KVEntry) (*storage.KVEntry, error) {
	start := time.Now()
	key := kvEntry.Key

	// Apply locally
	n.applyWrite(kvEntry)

	targets := n.getOtherNodes()

	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	// Buffer for all possible acks even if W=1 so background acks don't block.
	ackCh := make(chan string, len(targets)+1)
	n.writeAcks[seqNo] = ackCh
	n.mu.Unlock()

	msg := transport.Message{
		Type:     transport.MsgWrite,
		SenderID: n.id,
		SeqNo:    seqNo,
		Entry:    kvEntry,
	}

	// Always fan out to all other nodes so data replicates regardless of W.
	for _, targetID := range targets {
		m := msg
		m.TargetID = targetID
		n.fabric.Send(m)
	}

	// Wait for W-1 additional acks (self already counts as 1). Record which peers
	// acked so we can buffer hinted-handoff entries for the ones that didn't.
	acked := make(map[string]bool, len(targets))
	ackCount := 1 // self counts
	needMore := n.qConfig.W - 1
	if needMore > 0 {
		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()
		for ackCount-1 < needMore {
			select {
			case senderID := <-ackCh:
				if !acked[senderID] {
					acked[senderID] = true
					ackCount++
				}
			case <-timer.C:
				goto writeQuorumDone
			}
		}
	}
writeQuorumDone:
	// Hinted handoff: after the quorum decision, keep draining acks for a short grace
	// window (in the background so write latency stays at quorum latency), then buffer
	// a hint for every target that never acknowledged — presumed down/partitioned.
	go n.collectHints(seqNo, ackCh, targets, acked, kvEntry)

	if ackCount >= n.qConfig.W {
		n.publishEvent(events.EvtQuorumAchieved, map[string]interface{}{
			"key":   key,
			"w":     n.qConfig.W,
			"acked": ackCount,
		})
	} else {
		n.publishEvent(events.EvtQuorumFailed, map[string]interface{}{
			"key":     key,
			"w":       n.qConfig.W,
			"acked":   ackCount,
			"missing": n.qConfig.W - ackCount,
		})
	}

	n.metrics.RecordWrite(float64(time.Since(start).Milliseconds()))

	// The write is applied locally regardless, but the client must learn that the
	// write quorum W was not achieved rather than seeing a false success.
	if ackCount < n.qConfig.W {
		return kvEntry, fmt.Errorf("write quorum not met: %d/%d acks (W=%d)", ackCount, n.qConfig.W, n.qConfig.W)
	}
	return kvEntry, nil
}

// collectHints drains write acks for a short grace period and then buffers a
// hinted-handoff entry for every target that never acknowledged the write. Those
// hints are delivered later by runHintedHandoff once the target recovers.
func (n *LeaderlessNode) collectHints(seqNo uint64, ackCh chan string, targets []string, acked map[string]bool, entry *storage.KVEntry) {
	grace := time.NewTimer(300 * time.Millisecond)
	defer grace.Stop()
drain:
	for {
		select {
		case s := <-ackCh:
			acked[s] = true
		case <-grace.C:
			break drain
		case <-n.ctx.Done():
			break drain
		}
	}

	n.mu.Lock()
	delete(n.writeAcks, seqNo)
	n.mu.Unlock()

	n.hintsMu.Lock()
	for _, t := range targets {
		if !acked[t] {
			n.hints[t] = append(n.hints[t], entry)
		}
	}
	n.hintsMu.Unlock()
}

// Read is the coordinator read: fan out to R nodes, reconcile, repair stale
func (n *LeaderlessNode) Read(key string, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	start := time.Now()

	// Read locally first, INCLUDING a local tombstone (GetRaw), so a delete on the
	// coordinator participates in reconciliation instead of looking like a miss.
	localEntry, _ := n.store.GetRaw(key)
	localHits := 0
	if localEntry != nil {
		localHits = 1
	}
	// Number of remote responses required to reach the read quorum R. If self was a
	// miss we must query R targets, not R-1 — otherwise the collection loop can never
	// reach R and blocks until the timeout.
	remoteNeeded := n.qConfig.R - localHits
	if remoteNeeded < 0 {
		remoteNeeded = 0
	}
	targets := n.getReadTargets(remoteNeeded)

	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	respCh := make(chan readResp, len(targets)+1)
	n.readResps[seqNo] = respCh
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		delete(n.readResps, seqNo)
		n.mu.Unlock()
	}()

	msg := transport.Message{
		Type:     transport.MsgClientRead,
		SenderID: n.id,
		SeqNo:    seqNo,
		Key:      key,
	}
	for _, targetID := range targets {
		m := msg
		m.TargetID = targetID
		n.fabric.Send(m)
	}

	// Collect R responses (self already counted as one).
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	// remoteResps tracks all remote responses so we can identify stale responders.
	type nodeResp struct {
		senderID string
		entry    *storage.KVEntry
	}
	var remoteResps []nodeResp
	var responses []*storage.KVEntry
	if localEntry != nil {
		responses = append(responses, localEntry)
	}

	// Collect the required remote responses, but never wait for more than we queried.
	needed := remoteNeeded
	if needed > len(targets) {
		needed = len(targets)
	}

	for i := 0; i < needed; i++ {
		select {
		case r := <-respCh:
			remoteResps = append(remoteResps, nodeResp{senderID: r.senderID, entry: r.entry})
			if r.entry != nil {
				responses = append(responses, r.entry)
			}
		case <-timer.C:
			goto readDone
		}
	}
readDone:
	// Read quorum check: self always responds (entry or not-found), plus each remote
	// that answered. If we couldn't gather R responses the read cannot guarantee it saw
	// the latest write, so signal quorum failure rather than returning possibly-stale data
	// (symmetric with the write path, which errors when W acks aren't met).
	collected := 1 + len(remoteResps)
	if collected < n.qConfig.R {
		return nil, fmt.Errorf("read quorum not met: %d/%d responses for key %q", collected, n.qConfig.R, key)
	}

	if len(responses) == 0 {
		// Quorum reached, but no replica holds the key.
		return nil, fmt.Errorf("key %q not found", key)
	}

	// Find newest among all responses using the total order (ts, nodeID).
	best := responses[0]
	for _, r := range responses[1:] {
		if entryWins(r, best) {
			best = r
		}
	}

	// Repair our own stale local copy too — otherwise the coordinator keeps serving
	// the stale value on every subsequent local read.
	if localEntry == nil || entryWins(best, localEntry) {
		n.applyWrite(best)
	}

	// Read repair: target the responder nodes that sent stale data, not the entry's
	// original writer (entry.NodeID is the coordinator that created the write, which
	// may be a completely different node from the one that responded with stale data).
	var staleNodes []string
	for _, r := range remoteResps {
		if r.entry == nil || entryWins(best, r.entry) {
			staleNodes = append(staleNodes, r.senderID)
		}
	}
	if len(staleNodes) > 0 {
		repairMsg := transport.Message{
			Type:     transport.MsgReadRepair,
			SenderID: n.id,
			Entry:    best,
		}
		go n.fabric.Broadcast(repairMsg, staleNodes)
		n.publishEvent(events.EvtReadRepair, map[string]interface{}{
			"key":         key,
			"stale_nodes": staleNodes,
			"repaired_ts": best.Timestamp,
		})
	}

	n.metrics.RecordRead(float64(time.Since(start).Milliseconds()))

	// If the winning version is a tombstone, the key is deleted: return not-found to
	// the client (we still ran read-repair above so stale replicas converge).
	if best.Tombstone {
		return nil, fmt.Errorf("key %q not found", key)
	}
	return best, nil
}

func (n *LeaderlessNode) getOtherNodes() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make([]string, 0)
	for _, id := range n.allNodes {
		if id != n.id {
			result = append(result, id)
		}
	}
	return result
}

func (n *LeaderlessNode) getReadTargets(count int) []string {
	others := n.getOtherNodes()
	if count >= len(others) {
		return others
	}
	// Randomly sample the read set. A deterministic (sorted) selection always queried
	// the same lowest-ID nodes, so a fresh value on a high-ID replica was never read
	// and read-repair could never converge it. Random sampling reaches all nodes over time.
	rand.Shuffle(len(others), func(i, j int) { others[i], others[j] = others[j], others[i] })
	return others[:count]
}

func (n *LeaderlessNode) SetAllNodes(nodes []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.allNodes = nodes
}

func (n *LeaderlessNode) UpdateQuorum(q quorum.QuorumConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.qConfig = q
}

var _ Node = (*LeaderlessNode)(nil)
