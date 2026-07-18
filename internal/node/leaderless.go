package node

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"replication-strategies/internal/events"
	"replication-strategies/internal/hashring"
	"replication-strategies/internal/quorum"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// ReadRepairMode selects how a coordinator repairs stale replicas after a read.
type ReadRepairMode string

const (
	// RepairAsync fires read-repair in the background (default): low read latency, the
	// client's response doesn't wait for stale replicas to converge.
	RepairAsync ReadRepairMode = "async"
	// RepairSync blocks the read until stale replicas have been sent the fresh value,
	// trading read latency for a stronger monotonic-read guarantee on the next read.
	RepairSync ReadRepairMode = "sync"
	// RepairDigest first exchanges only value hashes; a full value + repair is only
	// issued when the digests disagree, saving bandwidth on the common (converged) case.
	RepairDigest ReadRepairMode = "digest"
)

// ConsistencyLevel selects region-aware quorum semantics (Cassandra-style).
type ConsistencyLevel string

const (
	// CLQuorum: a simple majority of all N replicas (the default).
	CLQuorum ConsistencyLevel = "quorum"
	// CLLocalQuorum: a quorum of the replicas in the coordinator's own region only —
	// low latency, survives remote-region partitions, weaker cross-region guarantee.
	CLLocalQuorum ConsistencyLevel = "local_quorum"
	// CLEachQuorum: a quorum in every region that holds a replica — strongest, but a
	// single partitioned region fails the write.
	CLEachQuorum ConsistencyLevel = "each_quorum"
)

// readResp pairs a read response entry with the ID of the node that responded.
// This is required so read repair can target the responder, not the original writer.
type readResp struct {
	senderID string
	entry    *storage.KVEntry // nil if the node didn't have the key
	hash     string           // content hash for digest reads ("" for full reads)
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
	// ring provides consistent-hashing placement: a key's N replicas are the next N
	// distinct nodes clockwise from the key's hash, instead of "every node".
	ring *hashring.Ring
	// regions maps nodeID -> region name for region-aware (LOCAL/EACH) quorums; selfRegion
	// is this coordinator's region. Empty when geo-replication isn't configured.
	regions    map[string]string
	selfRegion string
	// repairMode / consistencyLevel / sloppy are tunable read/write policies. Defaults
	// (async repair, plain quorum, sloppy on) preserve the original behavior.
	repairMode       ReadRepairMode
	consistencyLevel ConsistencyLevel
	sloppy           bool
	cfgMu            sync.RWMutex
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
	ring := hashring.NewRing(128)
	ring.Add(id)
	n := &LeaderlessNode{
		BaseNode:         base,
		fabric:           fabric,
		qConfig:          qConfig,
		inbox_ch:         ch,
		allNodes:         []string{id},
		hints:            make(map[string][]*storage.KVEntry),
		writeAcks:        make(map[uint64]chan string),
		readResps:        make(map[uint64]chan readResp),
		ring:             ring,
		regions:          make(map[string]string),
		repairMode:       RepairAsync,
		consistencyLevel: CLQuorum,
		sloppy:           true,
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
			// Sloppy-quorum stand-in: if this write was routed here on behalf of another
			// (unreachable) replica, hold a hint to hand it off once that node recovers.
			if msg.Type == transport.MsgWrite && msg.OriginalTarget != "" && msg.OriginalTarget != n.id {
				n.hintsMu.Lock()
				n.hints[msg.OriginalTarget] = append(n.hints[msg.OriginalTarget], msg.Entry)
				n.hintsMu.Unlock()
			}
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
		if ok && msg.Digest {
			// Digest read: reply with a lightweight metadata-only entry (no value bytes)
			// plus a content hash. The coordinator picks the winner from metadata and
			// fetches the full value only from the winning replica — the bandwidth win.
			meta := &storage.KVEntry{
				Key:       entry.Key,
				Timestamp: entry.Timestamp,
				NodeID:    entry.NodeID,
				Version:   entry.Version,
				Tombstone: entry.Tombstone,
			}
			resp = transport.Message{
				Type:     transport.MsgSyncAck,
				SenderID: n.id,
				TargetID: msg.SenderID,
				SeqNo:    msg.SeqNo,
				Key:      msg.Key,
				Entry:    meta,
				Digest:   true,
				Hash:     entryHash(entry),
			}
		} else if ok {
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
			case ch <- readResp{senderID: msg.SenderID, entry: msg.Entry, hash: msg.Hash}:
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
	n.HLCUpdate(entry.Timestamp) // merge remote clock to preserve causal order under skew
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
		Timestamp: n.HLCNow(),
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
		Timestamp: n.HLCNow(),
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

	// Apply locally (the coordinator keeps a copy even when it isn't one of the key's
	// replicas — a harmless coordinator cache that also lets local reads participate).
	n.applyWrite(kvEntry)

	// Preference-list routing: the write goes to the N replicas that own this key on
	// the consistent-hash ring, not to every node.
	replicas := n.replicasFor(key)
	selfIsReplica := false
	targets := make([]string, 0, len(replicas))
	for _, r := range replicas {
		if r == n.id {
			selfIsReplica = true
			continue
		}
		targets = append(targets, r)
	}

	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	// Buffer for all possible acks (replicas + sloppy stand-ins) so background acks
	// never block on a full channel.
	ackCh := make(chan string, len(n.allNodes)+1)
	n.writeAcks[seqNo] = ackCh
	n.mu.Unlock()

	msg := transport.Message{
		Type:     transport.MsgWrite,
		SenderID: n.id,
		SeqNo:    seqNo,
		Entry:    kvEntry,
	}
	for _, targetID := range targets {
		m := msg
		m.TargetID = targetID
		n.fabric.Send(m)
	}

	// Collect acks until the configured quorum (plain/local/each) is satisfied or we
	// time out. Self counts when it is a replica for the key.
	acked := make(map[string]bool, len(targets))
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	for !n.writeQuorumMet(acked, replicas, selfIsReplica) {
		select {
		case senderID := <-ackCh:
			acked[senderID] = true
		case <-timer.C:
			goto writeQuorumDone
		}
		if len(acked) >= len(targets) {
			break // every preferred replica responded; no more acks are coming
		}
	}
writeQuorumDone:
	met := n.writeQuorumMet(acked, replicas, selfIsReplica)

	// Sloppy quorum: if the preferred replicas couldn't form a quorum (partition/down),
	// borrow healthy nodes further along the ring so W can still be met. Each stand-in
	// stores the write tagged with OriginalTarget and hands it off on recovery.
	if !met && n.sloppyEnabled() {
		met = n.sloppyRound(key, kvEntry, seqNo, ackCh, replicas, acked, selfIsReplica)
	}

	// Snapshot the ack count BEFORE handing `acked` to the background hint collector,
	// which mutates it concurrently.
	got := len(acked)
	if selfIsReplica {
		got++
	}
	go n.collectHints(seqNo, ackCh, targets, acked, kvEntry)

	if met {
		n.publishEvent(events.EvtQuorumAchieved, map[string]interface{}{
			"key": key, "w": n.qConfig.W, "acked": got,
			"consistency": string(n.getConsistencyLevel()),
		})
	} else {
		n.publishEvent(events.EvtQuorumFailed, map[string]interface{}{
			"key": key, "w": n.qConfig.W, "acked": got,
		})
	}

	n.metrics.RecordWrite(float64(time.Since(start).Milliseconds()))

	// The write is applied locally regardless, but the client must learn that the
	// write quorum was not achieved rather than seeing a false success.
	if !met {
		return kvEntry, fmt.Errorf("write quorum not met: %d/%d acks (W=%d, %s)", got, n.qConfig.W, n.qConfig.W, n.getConsistencyLevel())
	}
	return kvEntry, nil
}

// sloppyRound sends the write to healthy stand-in nodes past the preference list when
// the preferred replicas can't form a quorum, tagging each with the OriginalTarget it
// covers so the data is handed off once that replica recovers. Returns whether the
// quorum is met after the stand-in acks. `acked` is mutated in place.
func (n *LeaderlessNode) sloppyRound(key string, entry *storage.KVEntry, seqNo uint64, ackCh chan string, replicas []string, acked map[string]bool, selfIsReplica bool) bool {
	var unacked []string
	for _, r := range replicas {
		if r != n.id && !acked[r] {
			unacked = append(unacked, r)
		}
	}
	if len(unacked) == 0 {
		return true
	}
	fallbacks := n.fallbackNodes(key, replicas, len(unacked)+2)
	sent := 0
	for i, fb := range fallbacks {
		if i >= len(unacked) {
			break
		}
		n.fabric.Send(transport.Message{
			Type:           transport.MsgWrite,
			SenderID:       n.id,
			SeqNo:          seqNo,
			Entry:          entry,
			TargetID:       fb,
			OriginalTarget: unacked[i],
		})
		sent++
	}
	if sent == 0 {
		return false
	}
	n.publishEvent(events.EvtHintedHandoff, map[string]interface{}{
		"key": key, "sloppy": true, "standins": sent,
	})
	timer := time.NewTimer(700 * time.Millisecond)
	defer timer.Stop()
	for !n.writeQuorumMet(acked, replicas, selfIsReplica) {
		select {
		case senderID := <-ackCh:
			acked[senderID] = true
		case <-timer.C:
			return n.writeQuorumMet(acked, replicas, selfIsReplica)
		}
	}
	return true
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
	// Query the key's replicas from the preference list (not random cluster-wide nodes).
	targets := n.readTargets(key, remoteNeeded)
	digest := n.getRepairMode() == RepairDigest

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
		Digest:   digest, // ask replicas for hash-only responses when in digest mode
	}
	for _, targetID := range targets {
		m := msg
		m.TargetID = targetID
		n.fabric.Send(m)
	}

	// Collect R responses (self already counted as one).
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	// remoteResps tracks all remote responses (with digest hashes) so we can identify
	// stale responders.
	var remoteResps []readResp
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
			remoteResps = append(remoteResps, r)
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

	// Find newest among all responses using the total order (ts, nodeID). This works on
	// digest (metadata-only) entries too, since entryWins only reads Timestamp+NodeID.
	best := responses[0]
	for _, r := range responses[1:] {
		if entryWins(r, best) {
			best = r
		}
	}

	// Digest read: the winner may be a metadata-only reply (no value). Fetch the full
	// value from the winning replica before returning/repairing — the whole point of a
	// digest read is that we transfer the value only once, from the freshest replica.
	if digest && best.Value == nil && !best.Tombstone {
		if full := n.fetchFull(best.NodeID, key); full != nil {
			best = full
		}
	}

	// Repair our own stale local copy too — otherwise the coordinator keeps serving
	// the stale value on every subsequent local read. Skip if best is still metadata-only.
	if (localEntry == nil || entryWins(best, localEntry)) && (best.Value != nil || best.Tombstone) {
		n.applyWrite(best)
	}

	// Read repair: target the responder nodes that sent stale data, not the entry's
	// original writer. In digest mode compare by hash; otherwise by the (ts,nodeID) order.
	bestHash := entryHash(best)
	var staleNodes []string
	for _, r := range remoteResps {
		switch {
		case r.entry == nil:
			staleNodes = append(staleNodes, r.senderID)
		case r.hash != "":
			if r.hash != bestHash {
				staleNodes = append(staleNodes, r.senderID)
			}
		case entryWins(best, r.entry):
			staleNodes = append(staleNodes, r.senderID)
		}
	}
	if len(staleNodes) > 0 && (best.Value != nil || best.Tombstone) {
		switch n.getRepairMode() {
		case RepairSync:
			// Blocking repair: wait until stale replicas acknowledge the fresh value, so
			// the next read from any of them is guaranteed monotonic. Higher read latency.
			n.repairSync(best, staleNodes)
		default:
			// Async / digest: fire-and-forget so read latency stays low.
			go n.fabric.Broadcast(transport.Message{
				Type: transport.MsgReadRepair, SenderID: n.id, Entry: best,
			}, staleNodes)
		}
		n.publishEvent(events.EvtReadRepair, map[string]interface{}{
			"key":         key,
			"stale_nodes": staleNodes,
			"repaired_ts": best.Timestamp,
			"mode":        string(n.getRepairMode()),
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

// readTargets returns up to count replicas for key (from the preference list, minus
// self), randomly sampled so read-repair reaches every replica over time.
func (n *LeaderlessNode) readTargets(key string, count int) []string {
	replicas := n.replicasFor(key)
	others := make([]string, 0, len(replicas))
	for _, r := range replicas {
		if r != n.id {
			others = append(others, r)
		}
	}
	// If the preference list can't supply enough replicas (e.g. self isn't on it), top
	// up from the rest of the membership so the read quorum is still reachable.
	if len(others) < count {
		inSet := map[string]bool{n.id: true}
		for _, r := range others {
			inSet[r] = true
		}
		for _, id := range n.getOtherNodes() {
			if !inSet[id] {
				others = append(others, id)
				inSet[id] = true
			}
		}
	}
	if count >= len(others) {
		return others
	}
	rand.Shuffle(len(others), func(i, j int) { others[i], others[j] = others[j], others[i] })
	return others[:count]
}

// entryHash is a stable content hash of a KV entry used for digest-read comparison.
func entryHash(e *storage.KVEntry) string {
	if e == nil {
		return ""
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d\x00%s\x00%t\x00%s", e.Timestamp, e.NodeID, e.Tombstone, string(e.Value))
	return strconv.FormatUint(h.Sum64(), 16)
}

// fetchFull issues a single full (non-digest) read to nodeID for key and waits briefly
// for the value, used to pull the winning value after a digest read.
func (n *LeaderlessNode) fetchFull(nodeID, key string) *storage.KVEntry {
	if nodeID == "" || nodeID == n.id {
		if e, ok := n.store.GetRaw(key); ok {
			return e
		}
		return nil
	}
	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	respCh := make(chan readResp, 2)
	n.readResps[seqNo] = respCh
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.readResps, seqNo)
		n.mu.Unlock()
	}()
	n.fabric.Send(transport.Message{
		Type: transport.MsgClientRead, SenderID: n.id, SeqNo: seqNo, Key: key,
	})
	select {
	case r := <-respCh:
		return r.entry
	case <-time.After(400 * time.Millisecond):
		return nil
	}
}

// repairSync sends the fresh value to stale replicas as writes and blocks until they
// acknowledge (or a timeout), giving a stronger monotonic-read guarantee than async.
func (n *LeaderlessNode) repairSync(best *storage.KVEntry, staleNodes []string) {
	n.mu.Lock()
	n.seqNo++
	seqNo := n.seqNo
	ackCh := make(chan string, len(staleNodes)+1)
	n.writeAcks[seqNo] = ackCh
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.writeAcks, seqNo)
		n.mu.Unlock()
	}()
	for _, id := range staleNodes {
		n.fabric.Send(transport.Message{
			Type: transport.MsgWrite, SenderID: n.id, SeqNo: seqNo, Entry: best, TargetID: id,
		})
	}
	acked := make(map[string]bool, len(staleNodes))
	timer := time.NewTimer(700 * time.Millisecond)
	defer timer.Stop()
	for len(acked) < len(staleNodes) {
		select {
		case s := <-ackCh:
			acked[s] = true
		case <-timer.C:
			return
		}
	}
}

func (n *LeaderlessNode) SetAllNodes(nodes []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.allNodes = nodes
	// Rebuild the consistent-hash ring so key placement tracks membership. This is the
	// backbone of preference-list routing, sloppy quorums, and hinted handoff.
	ring := hashring.NewRing(128)
	for _, id := range nodes {
		ring.Add(id)
	}
	n.ring = ring
}

// SetRegions records each node's region and this coordinator's own region, enabling
// LOCAL_QUORUM / EACH_QUORUM semantics.
func (n *LeaderlessNode) SetRegions(regions map[string]string) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	cp := make(map[string]string, len(regions))
	for k, v := range regions {
		cp[k] = v
	}
	n.regions = cp
	n.selfRegion = cp[n.id]
}

// SetReadRepairMode selects async/sync/digest read repair.
func (n *LeaderlessNode) SetReadRepairMode(m ReadRepairMode) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	if m == "" {
		m = RepairAsync
	}
	n.repairMode = m
}

// SetConsistencyLevel selects quorum / local_quorum / each_quorum semantics.
func (n *LeaderlessNode) SetConsistencyLevel(cl ConsistencyLevel) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	if cl == "" {
		cl = CLQuorum
	}
	n.consistencyLevel = cl
}

// SetSloppyQuorum toggles sloppy-quorum fallback (borrow healthy nodes to meet W).
func (n *LeaderlessNode) SetSloppyQuorum(on bool) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	n.sloppy = on
}

func (n *LeaderlessNode) getRepairMode() ReadRepairMode {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.repairMode
}

func (n *LeaderlessNode) getConsistencyLevel() ConsistencyLevel {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.consistencyLevel
}

func (n *LeaderlessNode) sloppyEnabled() bool {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.sloppy
}

// replicasFor returns the N nodes responsible for key per the consistent-hash ring
// (the preference list). Falls back to full membership if the ring is empty.
func (n *LeaderlessNode) replicasFor(key string) []string {
	n.mu.RLock()
	ring := n.ring
	all := append([]string{}, n.allNodes...)
	n.mu.RUnlock()
	if ring != nil {
		if pl := ring.PreferenceList(key, n.qConfig.N); len(pl) > 0 {
			return pl
		}
	}
	return all
}

// fallbackNodes returns healthy nodes just past the preference list on the ring, used
// as sloppy-quorum stand-ins when preferred replicas don't ack. It walks the ring for
// N+extra and returns the tail not already in the preference list, excluding self.
func (n *LeaderlessNode) fallbackNodes(key string, replicas []string, extra int) []string {
	n.mu.RLock()
	ring := n.ring
	n.mu.RUnlock()
	if ring == nil {
		return nil
	}
	inPref := make(map[string]bool, len(replicas))
	for _, r := range replicas {
		inPref[r] = true
	}
	walk := ring.PreferenceList(key, n.qConfig.N+extra)
	var out []string
	for _, id := range walk {
		if !inPref[id] && id != n.id {
			out = append(out, id)
		}
	}
	return out
}

// regionOf returns the region name for a node ("" if unknown / no geo config).
func (n *LeaderlessNode) regionOf(id string) string {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.regions[id]
}

// writeQuorumMet reports whether the set of acked nodes (plus self if self is a replica)
// satisfies the configured consistency level over the replica set.
func (n *LeaderlessNode) writeQuorumMet(acked map[string]bool, replicas []string, selfIsReplica bool) bool {
	// Effective acked set including self.
	total := make(map[string]bool, len(acked)+1)
	for k := range acked {
		total[k] = true
	}
	if selfIsReplica {
		total[n.id] = true
	}
	switch n.getConsistencyLevel() {
	case CLLocalQuorum:
		region := n.selfRegion
		need := regionQuorum(replicas, region, n)
		return countInRegion(total, region, n) >= need
	case CLEachQuorum:
		for _, region := range replicaRegions(replicas, n) {
			if countInRegion(total, region, n) < regionQuorum(replicas, region, n) {
				return false
			}
		}
		return true
	default: // CLQuorum
		return len(total) >= n.qConfig.W
	}
}

// replicaRegions returns the distinct regions represented in the replica set.
func replicaRegions(replicas []string, n *LeaderlessNode) []string {
	seen := make(map[string]bool)
	var out []string
	for _, r := range replicas {
		reg := n.regionOf(r)
		if !seen[reg] {
			seen[reg] = true
			out = append(out, reg)
		}
	}
	return out
}

// regionQuorum returns floor(replicasInRegion/2)+1 for a region.
func regionQuorum(replicas []string, region string, n *LeaderlessNode) int {
	count := 0
	for _, r := range replicas {
		if n.regionOf(r) == region {
			count++
		}
	}
	if count == 0 {
		return 1
	}
	return count/2 + 1
}

// countInRegion counts acked nodes belonging to region.
func countInRegion(acked map[string]bool, region string, n *LeaderlessNode) int {
	c := 0
	for id := range acked {
		if n.regionOf(id) == region {
			c++
		}
	}
	return c
}

func (n *LeaderlessNode) UpdateQuorum(q quorum.QuorumConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.qConfig = q
}

var _ Node = (*LeaderlessNode)(nil)
