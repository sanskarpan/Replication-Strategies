package node

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"replication-strategies/internal/events"
	"replication-strategies/internal/storage"
	"replication-strategies/internal/transport"
)

type raftRole int

const (
	roleFollower raftRole = iota
	roleCandidate
	roleLeader
)

// RaftNode implements a (simplified but real) Raft consensus node: randomized-timeout
// leader election with the log-up-to-date vote check, AppendEntries with the log-matching
// consistency check, majority commit, automatic failover, and log compaction with
// InstallSnapshot catch-up for lagging followers. Membership changes are out of scope.
type RaftNode struct {
	*BaseNode
	fabric   *transport.NetworkFabric
	inbox_ch chan transport.Message

	rmu         sync.Mutex
	currentTerm uint64
	votedFor    string
	role        raftRole
	raftLeader  string
	votes       int
	lastHeard   time.Time
	timeout     time.Duration
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	rng         *rand.Rand

	// applyMu guards `applied` (touched by the message loop AND the ticker's compaction).
	applyMu sync.Mutex
	applied uint64
}

func NewRaftNode(id, clusterID string, fabric *transport.NetworkFabric, bus *events.EventBus, seed int64) *RaftNode {
	base := newBaseNode(id, clusterID, StrategyRaft, RoleFollower, bus)
	ch := make(chan transport.Message, 512)
	fabric.Register(id, ch)
	n := &RaftNode{
		BaseNode:   base,
		fabric:     fabric,
		inbox_ch:   ch,
		role:       roleFollower,
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		rng:        rand.New(rand.NewSource(seed)),
	}
	n.resetTimeout()
	n.lastHeard = time.Now()
	return n
}

// resetTimeout picks a fresh randomized election timeout (150–300ms). Caller need not
// hold rmu at construction; runtime callers hold it.
func (n *RaftNode) resetTimeout() {
	n.timeout = time.Duration(150+n.rng.Intn(150)) * time.Millisecond
}

func (n *RaftNode) touch() { n.lastHeard = time.Now() }

func (n *RaftNode) Start(ctx context.Context) {
	go n.runMessageLoop()
	go n.runTicker()
}

func (n *RaftNode) runMessageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.inbox_ch:
			n.HandleMessage(msg)
		}
	}
}

func (n *RaftNode) runTicker() {
	elect := time.NewTicker(20 * time.Millisecond)
	defer elect.Stop()
	beat := time.NewTicker(50 * time.Millisecond)
	defer beat.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-elect.C:
			if n.isPaused() {
				n.rmu.Lock()
				n.touch() // a "down" node doesn't accumulate timeout while paused
				n.rmu.Unlock()
				continue
			}
			n.maybeElect()
		case <-beat.C:
			if n.isPaused() {
				continue
			}
			n.maybeHeartbeat()
			n.maybeCompact()
		}
	}
}

func (n *RaftNode) maybeElect() {
	n.rmu.Lock()
	if n.role == roleLeader || time.Since(n.lastHeard) < n.timeout {
		n.rmu.Unlock()
		return
	}
	// become candidate for a new term
	n.currentTerm++
	n.role = roleCandidate
	n.votedFor = n.id
	n.votes = 1
	n.raftLeader = ""
	n.resetTimeout()
	n.touch()
	term := n.currentTerm
	n.rmu.Unlock()

	lastIdx, lastTerm := n.log.LastIndexTerm()
	for _, p := range n.GetPeers() {
		n.fabric.Send(transport.Message{
			Type: transport.MsgVoteRequest, SenderID: n.id, TargetID: p,
			Term: term, LastLogIndex: lastIdx, LastLogTerm: lastTerm,
		})
	}
}

func (n *RaftNode) maybeHeartbeat() {
	n.rmu.Lock()
	isLeader := n.role == roleLeader
	n.rmu.Unlock()
	if !isLeader {
		return
	}
	for _, p := range n.GetPeers() {
		n.sendAppend(p)
	}
}

// maybeCompact snapshots the state machine and truncates the applied log prefix once
// enough entries have accumulated, bounding log growth.
func (n *RaftNode) maybeCompact() {
	const threshold = 30
	snapIdx, _ := n.log.SnapshotBoundary()
	n.applyMu.Lock()
	applied := n.applied
	n.applyMu.Unlock()
	if applied <= snapIdx || applied-snapIdx <= threshold {
		return
	}
	term := n.log.TermAt(applied)
	if term == 0 {
		return
	}
	n.log.Compact(applied, term)
}

func (n *RaftNode) sendAppend(peer string) {
	n.rmu.Lock()
	if n.role != roleLeader {
		n.rmu.Unlock()
		return
	}
	term := n.currentTerm
	ni := n.nextIndex[peer]
	if ni == 0 {
		ni = 1
	}
	prevIndex := ni - 1
	n.rmu.Unlock()

	// If the follower needs an entry we've already compacted, ship a snapshot instead.
	snapIdx, _ := n.log.SnapshotBoundary()
	if prevIndex < snapIdx {
		n.sendSnapshot(peer, term)
		return
	}

	prevTerm := n.log.TermAt(prevIndex)
	entries := n.log.GetFrom(ni)
	commit := n.log.CommitIndex()
	n.fabric.Send(transport.Message{
		Type: transport.MsgAppendEntries, SenderID: n.id, TargetID: peer,
		Term: term, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: commit,
	})
}

// sendSnapshot streams the leader's compacted state to a lagging follower.
func (n *RaftNode) sendSnapshot(peer string, term uint64) {
	snapIdx, snapTerm := n.log.SnapshotBoundary()
	n.fabric.Send(transport.Message{
		Type: transport.MsgInstallSnap, SenderID: n.id, TargetID: peer,
		Term: term, PrevLogIndex: snapIdx, PrevLogTerm: snapTerm,
		Snapshot: n.store.SnapshotBytes(),
	})
}

func (n *RaftNode) handleInstallSnap(msg transport.Message) {
	n.rmu.Lock()
	if msg.Term < n.currentTerm {
		term := n.currentTerm
		n.rmu.Unlock()
		n.fabric.Send(transport.Message{Type: transport.MsgAppendAck, SenderID: n.id, TargetID: msg.SenderID, Term: term, Success: false})
		return
	}
	if msg.Term > n.currentTerm {
		n.stepDownLocked(msg.Term)
	}
	n.role = roleFollower
	if n.raftLeader != msg.SenderID {
		n.raftLeader = msg.SenderID
		n.setRole(RoleFollower)
	}
	n.touch()
	term := n.currentTerm
	n.rmu.Unlock()

	snapIdx, snapTerm := msg.PrevLogIndex, msg.PrevLogTerm
	if cur, _ := n.log.SnapshotBoundary(); snapIdx > cur {
		_ = n.store.RestoreSnapshot(msg.Snapshot)
		n.log.InstallSnapshot(snapIdx, snapTerm)
		n.applyMu.Lock()
		if n.applied < snapIdx {
			n.applied = snapIdx
		}
		n.applyMu.Unlock()
	}
	n.fabric.Send(transport.Message{
		Type: transport.MsgAppendAck, SenderID: n.id, TargetID: msg.SenderID,
		Term: term, Success: true, ConflictIndex: n.log.LastIndex(),
	})
}

func (n *RaftNode) HandleMessage(raw interface{}) {
	msg, ok := raw.(transport.Message)
	if !ok || n.isPaused() {
		return
	}
	switch msg.Type {
	case transport.MsgVoteRequest:
		n.handleVoteRequest(msg)
	case transport.MsgVoteResponse:
		n.handleVoteResponse(msg)
	case transport.MsgAppendEntries:
		n.handleAppendEntries(msg)
	case transport.MsgAppendAck:
		n.handleAppendAck(msg)
	case transport.MsgInstallSnap:
		n.handleInstallSnap(msg)
	}
}

func (n *RaftNode) stepDownLocked(newTerm uint64) {
	n.currentTerm = newTerm
	n.role = roleFollower
	n.votedFor = ""
	n.raftLeader = ""
}

func (n *RaftNode) handleVoteRequest(msg transport.Message) {
	n.rmu.Lock()
	if msg.Term > n.currentTerm {
		n.stepDownLocked(msg.Term)
	}
	grant := false
	if msg.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == msg.SenderID) {
		myIdx, myTerm := n.log.LastIndexTerm()
		upToDate := msg.LastLogTerm > myTerm || (msg.LastLogTerm == myTerm && msg.LastLogIndex >= myIdx)
		if upToDate {
			grant = true
			n.votedFor = msg.SenderID
			n.touch()
		}
	}
	term := n.currentTerm
	n.rmu.Unlock()
	n.fabric.Send(transport.Message{
		Type: transport.MsgVoteResponse, SenderID: n.id, TargetID: msg.SenderID,
		Term: term, VoteGranted: grant,
	})
}

func (n *RaftNode) handleVoteResponse(msg transport.Message) {
	n.rmu.Lock()
	defer n.rmu.Unlock()
	if msg.Term > n.currentTerm {
		n.stepDownLocked(msg.Term)
		return
	}
	if n.role != roleCandidate || msg.Term != n.currentTerm {
		return
	}
	if msg.VoteGranted {
		n.votes++
		if n.votes >= n.majority() {
			n.becomeLeaderLocked()
		}
	}
}

func (n *RaftNode) majority() int { return (len(n.GetPeers())+1)/2 + 1 }

func (n *RaftNode) becomeLeaderLocked() {
	n.role = roleLeader
	n.raftLeader = n.id
	last := n.log.LastIndex()
	for _, p := range n.GetPeers() {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.setRole(RoleLeader)
	n.publishEvent(events.EvtLeaderElected, map[string]interface{}{
		"leader": n.id, "term": n.currentTerm,
	})
	// immediate heartbeat asserts leadership
	go n.maybeHeartbeat()
}

func (n *RaftNode) handleAppendEntries(msg transport.Message) {
	n.rmu.Lock()
	if msg.Term < n.currentTerm {
		term := n.currentTerm
		n.rmu.Unlock()
		n.fabric.Send(transport.Message{Type: transport.MsgAppendAck, SenderID: n.id, TargetID: msg.SenderID, Term: term, Success: false})
		return
	}
	if msg.Term > n.currentTerm {
		n.stepDownLocked(msg.Term)
	}
	n.role = roleFollower
	if n.raftLeader != msg.SenderID {
		n.raftLeader = msg.SenderID
		n.setRole(RoleFollower)
	}
	n.touch()
	term := n.currentTerm
	n.rmu.Unlock()

	// Log-matching consistency check.
	if !n.log.Matches(msg.PrevLogIndex, msg.PrevLogTerm) {
		n.fabric.Send(transport.Message{
			Type: transport.MsgAppendAck, SenderID: n.id, TargetID: msg.SenderID,
			Term: term, Success: false, ConflictIndex: n.log.LastIndex() + 1,
		})
		return
	}
	last := n.log.AppendAfter(msg.PrevLogIndex, msg.Entries)
	if msg.LeaderCommit > n.log.CommitIndex() {
		newCommit := msg.LeaderCommit
		if last < newCommit {
			newCommit = last
		}
		n.log.SetCommitIndex(newCommit)
		n.applyCommitted(newCommit)
	}
	n.fabric.Send(transport.Message{
		Type: transport.MsgAppendAck, SenderID: n.id, TargetID: msg.SenderID,
		Term: term, Success: true, ConflictIndex: last, // ConflictIndex reused as matchIndex on success
	})
}

func (n *RaftNode) handleAppendAck(msg transport.Message) {
	n.rmu.Lock()
	defer n.rmu.Unlock()
	if msg.Term > n.currentTerm {
		n.stepDownLocked(msg.Term)
		n.setRole(RoleFollower)
		return
	}
	if n.role != roleLeader {
		return
	}
	if msg.Success {
		n.matchIndex[msg.SenderID] = msg.ConflictIndex
		n.nextIndex[msg.SenderID] = msg.ConflictIndex + 1
		n.advanceCommitLocked()
	} else if n.nextIndex[msg.SenderID] > 1 {
		n.nextIndex[msg.SenderID]--
	}
}

// advanceCommitLocked commits the highest index replicated on a majority whose term is
// the current term (Raft's commit safety rule). Caller holds rmu.
func (n *RaftNode) advanceCommitLocked() {
	last := n.log.LastIndex()
	commit := n.log.CommitIndex()
	for N := last; N > commit; N-- {
		if n.log.TermAt(N) != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range n.GetPeers() {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.majority() {
			n.log.SetCommitIndex(N)
			n.applyCommitted(N)
			break
		}
	}
}

// applyCommitted applies newly-committed log entries to the store.
func (n *RaftNode) applyCommitted(upto uint64) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	for i := n.applied + 1; i <= upto; i++ {
		e, ok := n.log.Get(i)
		if !ok {
			break
		}
		kv := &storage.KVEntry{
			Key: e.Key, Value: e.Value, VClock: e.VClock,
			Timestamp: e.Timestamp, NodeID: e.OriginID, Version: e.Index,
		}
		if e.Op == storage.OpDelete {
			kv.Tombstone = true
		}
		n.store.Set(kv)
		n.applied = i
	}
}

func (n *RaftNode) proposeAndWait(key string, value []byte, op storage.OpType) error {
	n.rmu.Lock()
	if n.role != roleLeader {
		leader := n.raftLeader
		n.rmu.Unlock()
		if leader == "" {
			return fmt.Errorf("no leader elected yet; retry")
		}
		return fmt.Errorf("not leader: writes must go to current leader %s", leader)
	}
	term := n.currentTerm
	ts := n.HLCNow()
	entry := storage.LogEntry{Key: key, Value: value, Op: op, Term: term, Timestamp: ts, OriginID: n.id}
	idx := n.log.Append(entry)
	n.matchIndex[n.id] = idx
	n.rmu.Unlock()

	// replicate immediately, then wait for the entry to commit (majority) with a timeout.
	for _, p := range n.GetPeers() {
		n.sendAppend(p)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.log.CommitIndex() >= idx {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("write not committed within timeout (lost leadership?)")
}

func (n *RaftNode) Write(key string, value []byte, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	if err := n.proposeAndWait(key, value, storage.OpSet); err != nil {
		return nil, err
	}
	e, _ := n.store.Get(key)
	n.metrics.RecordWrite(0)
	return e, nil
}

func (n *RaftNode) Delete(key string, clientID string) error {
	if n.isPaused() {
		return fmt.Errorf("node %s is paused/offline", n.id)
	}
	return n.proposeAndWait(key, nil, storage.OpDelete)
}

func (n *RaftNode) Read(key string, clientID string) (*storage.KVEntry, error) {
	if n.isPaused() {
		return nil, fmt.Errorf("node %s is paused/offline", n.id)
	}
	n.rmu.Lock()
	isLeader := n.role == roleLeader
	leader := n.raftLeader
	n.rmu.Unlock()
	if !isLeader {
		if leader == "" {
			return nil, fmt.Errorf("no leader elected yet; retry")
		}
		return nil, fmt.Errorf("not leader: reads must go to current leader %s", leader)
	}
	e, ok := n.store.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	n.metrics.RecordRead(0)
	return e, nil
}

// RaftLeader returns the node's view of the current leader (for the orchestrator router).
func (n *RaftNode) RaftLeader() (string, bool) {
	n.rmu.Lock()
	defer n.rmu.Unlock()
	return n.raftLeader, n.role == roleLeader
}

var _ Node = (*RaftNode)(nil)
