package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"replication-strategies/internal/conflict"
	"replication-strategies/internal/events"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

// MultiLeaderNode accepts writes directly and replicates to peers.
// Conflicts are detected via vector clocks and resolved by the configured resolver.
type MultiLeaderNode struct {
	*BaseNode
	fabric   *transport.NetworkFabric
	resolver conflict.ConflictResolver
	inbox_ch chan transport.Message

	// applyMu serialises the per-key read-modify-write of the store across both the
	// local Write path and the remote-apply path, so a local write and an incoming
	// replicated write cannot interleave and lose an update.
	applyMu sync.Mutex

	// Track resolved conflicts for UI
	conflicts   []*conflict.Conflict
	resolutions []*conflict.Resolution

	// pending holds conflicts awaiting a manual human resolution (manual resolver).
	pendingMu sync.Mutex
	pending   map[string]*conflict.Conflict // key -> parked conflict
}

func NewMultiLeaderNode(id, clusterID string, fabric *transport.NetworkFabric, bus *events.EventBus, resolver conflict.ConflictResolver) *MultiLeaderNode {
	base := newBaseNode(id, clusterID, StrategyMultiLeader, RolePrimary, bus)
	ch := make(chan transport.Message, 256)
	fabric.Register(id, ch)
	if resolver == nil {
		resolver = conflict.NewLWWResolver()
	}
	n := &MultiLeaderNode{
		BaseNode:    base,
		fabric:      fabric,
		resolver:    resolver,
		inbox_ch:    ch,
		conflicts:   make([]*conflict.Conflict, 0),
		resolutions: make([]*conflict.Resolution, 0),
		pending:     make(map[string]*conflict.Conflict),
	}
	return n
}

func (n *MultiLeaderNode) Start(ctx context.Context) {
	go n.runMessageLoop()
	go n.runAntiEntropy()
}

func (n *MultiLeaderNode) runMessageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.inbox_ch:
			n.HandleMessage(msg)
		}
	}
}

// runAntiEntropy periodically broadcasts all local entries to all peers so
// that writes made during a partition are eventually propagated after heal.
func (n *MultiLeaderNode) runAntiEntropy() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if n.isPaused() {
				continue
			}
			entries := n.store.Snapshot()
			peers := n.GetPeers()
			for _, entry := range entries {
				msg := transport.Message{
					Type:     transport.MsgWrite,
					SenderID: n.id,
					Entry:    entry,
				}
				n.fabric.Broadcast(msg, peers)
			}
		}
	}
}

func (n *MultiLeaderNode) HandleMessage(raw interface{}) {
	msg, ok := raw.(transport.Message)
	if !ok {
		return
	}
	if n.isPaused() {
		return
	}
	switch msg.Type {
	case transport.MsgWrite:
		if msg.Entry != nil {
			n.receiveRemoteWrite(msg.Entry)
		}
	}
}

func (n *MultiLeaderNode) receiveRemoteWrite(remote *storage.KVEntry) {
	n.HLCUpdate(remote.Timestamp) // merge remote clock to preserve causal order under skew
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	local, exists := n.store.GetRaw(remote.Key)
	if !exists {
		// No local version — apply cleanly
		n.store.Set(remote)
		n.publishEvent(events.EvtEntryReplicated, map[string]interface{}{
			"key":    remote.Key,
			"origin": remote.NodeID,
		})
		return
	}

	// Compare vector clocks
	localVC := local.VClock
	remoteVC := remote.VClock

	// Already converged to the same version. Anti-entropy re-broadcasts the whole
	// store every tick, so without this check identical entries would be flagged as
	// "concurrent" conflicts forever (HappensBefore is false for equal clocks).
	if remoteVC.Equal(localVC) {
		return
	}

	if remoteVC.HappensBefore(localVC) {
		// Remote is stale — discard
		return
	}

	if localVC.HappensBefore(remoteVC) {
		// Local is stale — apply remote cleanly. This also settles any parked conflict
		// for the key (e.g. a peer's manual resolution arrived and now dominates).
		n.store.Set(remote)
		n.pendingMu.Lock()
		delete(n.pending, remote.Key)
		n.pendingMu.Unlock()
		n.publishEvent(events.EvtEntryReplicated, map[string]interface{}{
			"key":    remote.Key,
			"origin": remote.NodeID,
		})
		return
	}

	// Concurrent — conflict! In manual mode, if we've already parked a conflict for this
	// key, don't re-count it on every anti-entropy re-broadcast.
	if n.resolver.Type() == conflict.ResolverManual {
		n.pendingMu.Lock()
		_, already := n.pending[remote.Key]
		n.pendingMu.Unlock()
		if already {
			return
		}
	}

	c := &conflict.Conflict{
		ID:         uuid.New().String(),
		Key:        remote.Key,
		Local:      local,
		Remote:     remote,
		DetectedAt: time.Now(),
		NodeID:     n.id,
		ClusterID:  n.clusterID,
	}
	n.mu.Lock()
	n.conflicts = append(n.conflicts, c)
	n.mu.Unlock()
	n.metrics.RecordConflict()

	n.publishEvent(events.EvtConflictDetected, map[string]interface{}{
		"conflict_id": c.ID,
		"key":         c.Key,
		"local_ts":    local.Timestamp,
		"remote_ts":   remote.Timestamp,
		"local_node":  local.NodeID,
		"remote_node": remote.NodeID,
		"local_vc":    local.VClock,
		"remote_vc":   remote.VClock,
	})

	// Manual mode: park the conflict for a human to resolve (keep local value for now).
	if n.resolver.Type() == conflict.ResolverManual {
		n.pendingMu.Lock()
		n.pending[remote.Key] = c
		n.pendingMu.Unlock()
		return
	}

	resolution := n.resolver.Resolve(c)
	n.mu.Lock()
	n.resolutions = append(n.resolutions, resolution)
	n.mu.Unlock()

	// The resolved entry must causally dominate BOTH parents, otherwise a later write
	// concurrent with only one parent could be misclassified. Keep the winner's value
	// but merge the vector clocks so every replica converges to the same dominating clock.
	// Work on a copy — resolution.Winner may alias a stored/wire entry we must not mutate.
	winner := *resolution.Winner
	merged := local.VClock.Clone().Merge(remote.VClock)
	if resolution.Winner.VClock != nil {
		merged.Merge(resolution.Winner.VClock)
	}
	winner.VClock = merged

	n.store.Set(&winner)

	n.publishEvent(events.EvtConflictResolved, map[string]interface{}{
		"conflict_id": c.ID,
		"resolver":    string(resolution.ResolverType),
		"reason":      resolution.Reason,
		"winner_node": resolution.Winner.NodeID,
		"winner_ts":   resolution.Winner.Timestamp,
	})
}

func (n *MultiLeaderNode) Write(key string, value []byte, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	start := time.Now()

	// Hold applyMu through the read-increment-write to avoid a concurrent local
	// Write() or an incoming remote apply racing on the vector clock for the same key.
	n.applyMu.Lock()
	var vc storage.VectorClock
	if existing, ok := n.store.GetRaw(key); ok && existing.VClock != nil {
		vc = existing.VClock.Clone()
	} else {
		vc = storage.NewVectorClock()
	}
	vc = vc.Increment(n.id)
	ts := n.HLCNow()
	kvEntry := &storage.KVEntry{
		Key:       key,
		Value:     value,
		VClock:    vc,
		Timestamp: ts,
		NodeID:    n.id,
	}
	n.store.Set(kvEntry)
	n.applyMu.Unlock()

	n.metrics.RecordWrite(float64(time.Since(start).Milliseconds()))

	// broadcast to peers
	msg := transport.Message{
		Type:     transport.MsgWrite,
		SenderID: n.id,
		Entry:    kvEntry,
	}
	peers := n.GetPeers()
	go n.fabric.Broadcast(msg, peers)

	n.publishEvent(events.EvtWriteReceived, map[string]interface{}{
		"key":    key,
		"vclock": vc,
	})

	return kvEntry, nil
}

// Delete writes a tombstone for the key with an advanced vector clock and
// replicates it to peers, where receiveRemoteWrite reconciles it like any write.
func (n *MultiLeaderNode) Delete(key string, clientID string) error {
	if n.isPaused() {
		return fmt.Errorf("node %s is paused/offline", n.id)
	}

	n.applyMu.Lock()
	var vc storage.VectorClock
	if existing, ok := n.store.GetRaw(key); ok && existing.VClock != nil {
		vc = existing.VClock.Clone()
	} else {
		vc = storage.NewVectorClock()
	}
	vc = vc.Increment(n.id)
	ts := n.HLCNow()
	tomb := &storage.KVEntry{
		Key:       key,
		VClock:    vc,
		Timestamp: ts,
		NodeID:    n.id,
		Tombstone: true,
	}
	n.store.Set(tomb)
	n.applyMu.Unlock()

	msg := transport.Message{
		Type:     transport.MsgWrite,
		SenderID: n.id,
		Entry:    tomb,
	}
	go n.fabric.Broadcast(msg, n.GetPeers())

	n.publishEvent(events.EvtWriteReceived, map[string]interface{}{
		"key":    key,
		"op":     "delete",
		"vclock": vc,
	})
	return nil
}

func (n *MultiLeaderNode) Read(key string, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	entry, ok := n.store.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	n.metrics.RecordRead(0)
	return entry, nil
}

// PendingConflicts returns conflicts awaiting manual resolution.
func (n *MultiLeaderNode) PendingConflicts() []*conflict.Conflict {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	out := make([]*conflict.Conflict, 0, len(n.pending))
	for _, c := range n.pending {
		out = append(out, c)
	}
	return out
}

// ResolveConflict applies the human's choice ("local"|"remote") for a parked conflict,
// stamping a merged vector clock so the decision dominates both parents and converges
// across peers when replicated.
func (n *MultiLeaderNode) ResolveConflict(key, choice string) error {
	n.pendingMu.Lock()
	c, ok := n.pending[key]
	if ok {
		delete(n.pending, key)
	}
	n.pendingMu.Unlock()
	if !ok {
		return fmt.Errorf("no pending conflict for key %q on node %s", key, n.id)
	}
	winner := c.Local
	if choice == "remote" {
		winner = c.Remote
	}
	merged := c.Local.VClock.Clone().Merge(c.Remote.VClock).Increment(n.id)
	resolved := *winner
	resolved.VClock = merged
	resolved.Timestamp = n.HLCNow()
	resolved.NodeID = n.id

	n.applyMu.Lock()
	n.store.Set(&resolved)
	n.applyMu.Unlock()

	n.mu.Lock()
	n.resolutions = append(n.resolutions, &conflict.Resolution{
		ConflictID: c.ID, Winner: &resolved, ResolverType: conflict.ResolverManual,
		Reason: "manual_" + choice, ResolvedAt: time.Now(),
	})
	n.mu.Unlock()

	go n.fabric.Broadcast(transport.Message{Type: transport.MsgWrite, SenderID: n.id, Entry: &resolved}, n.GetPeers())
	n.publishEvent(events.EvtConflictResolved, map[string]interface{}{
		"key": key, "resolver": "manual", "choice": choice,
	})
	return nil
}

func (n *MultiLeaderNode) GetConflicts() []*conflict.Conflict {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make([]*conflict.Conflict, len(n.conflicts))
	copy(result, n.conflicts)
	return result
}

func (n *MultiLeaderNode) GetResolutions() []*conflict.Resolution {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make([]*conflict.Resolution, len(n.resolutions))
	copy(result, n.resolutions)
	return result
}

var _ Node = (*MultiLeaderNode)(nil)
