package transport

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// fabricRandMu guards the shared *rand.Rand used for packet drops.
var fabricRandMu sync.Mutex

type Partition struct {
	ID     string          `json:"id"`
	GroupA map[string]bool `json:"group_a"`
	GroupB map[string]bool `json:"group_b"`
}

func (p *Partition) Blocks(from, to string) bool {
	inA := p.GroupA[from]
	inB := p.GroupB[from]
	toInA := p.GroupA[to]
	toInB := p.GroupB[to]
	return (inA && toInB) || (inB && toInA)
}

// timedMsg is a message queued for delivery at a specific time on a link.
type timedMsg struct {
	msg Message
	at  time.Time
}

// link is a per-(source→target) ordered delivery queue. A single worker goroutine
// drains it so messages are delivered in FIFO order even when latency varies, which
// single-leader replication relies on. deliverAt is clamped monotonically so a later
// message can never overtake an earlier one on the same link.
type link struct {
	mu     sync.Mutex
	ch     chan timedMsg
	lastAt time.Time
}

type NetworkFabric struct {
	mu         sync.RWMutex
	nodes      map[string]chan Message
	latencyMs  map[string]map[string]int
	dropRate   map[string]map[string]float64
	partitions map[string]*Partition
	links      map[string]*link
	rng        *rand.Rand
	done       chan struct{} // closed by Close() to stop all link workers
	closed     bool
	dropped    atomic.Uint64 // backpressure drops (full link queue / full inbox)
}

// Dropped returns the number of messages dropped due to back-pressure (full queues),
// distinct from configured packet loss — surfaces otherwise-invisible load shedding.
func (f *NetworkFabric) Dropped() uint64 { return f.dropped.Load() }

func NewNetworkFabric() *NetworkFabric {
	return &NetworkFabric{
		nodes:      make(map[string]chan Message),
		latencyMs:  make(map[string]map[string]int),
		dropRate:   make(map[string]map[string]float64),
		partitions: make(map[string]*Partition),
		links:      make(map[string]*link),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		done:       make(chan struct{}),
	}
}

// Close stops all link-worker goroutines. Safe to call multiple times. After Close,
// Send is a no-op. This prevents per-link goroutine leaks when a cluster is deleted.
func (f *NetworkFabric) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.done)
	}
}

// getOrCreateLink returns the ordered delivery queue for from→to, starting its
// worker goroutine on first use. Returns nil if the fabric is closed.
func (f *NetworkFabric) getOrCreateLink(from, to string) *link {
	key := from + "\x00" + to
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	l, ok := f.links[key]
	if !ok {
		l = &link{ch: make(chan timedMsg, 1024)}
		f.links[key] = l
		go f.linkWorker(l)
	}
	return l
}

func (f *NetworkFabric) linkWorker(l *link) {
	for {
		select {
		case <-f.done:
			return
		case tm := <-l.ch:
			if d := time.Until(tm.at); d > 0 {
				// Wake early on shutdown instead of sleeping out the full latency.
				t := time.NewTimer(d)
				select {
				case <-f.done:
					t.Stop()
					return
				case <-t.C:
				}
			}
			f.mu.RLock()
			ch, ok := f.nodes[tm.msg.TargetID]
			f.mu.RUnlock()
			if !ok {
				continue
			}
			select {
			case ch <- tm.msg:
			default:
				f.dropped.Add(1)
				// target inbox full — drop
			}
		}
	}
}

func (f *NetworkFabric) Register(nodeID string, ch chan Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[nodeID] = ch
}

func (f *NetworkFabric) Deregister(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, nodeID)
}

func (f *NetworkFabric) Send(msg Message) {
	f.mu.RLock()
	_, ok := f.nodes[msg.TargetID]
	latency := f.getLatency(msg.SenderID, msg.TargetID)
	drop := f.getDropRate(msg.SenderID, msg.TargetID)
	blocked := f.isPartitioned(msg.SenderID, msg.TargetID)
	f.mu.RUnlock()

	if !ok || blocked {
		return
	}
	fabricRandMu.Lock()
	roll := f.rng.Float64()
	fabricRandMu.Unlock()
	if roll < drop {
		return // packet dropped
	}

	// Enqueue on the ordered per-link queue. The deliverAt is clamped to be no earlier
	// than the previous message's on this link, so delivery is strictly FIFO.
	at := time.Now().Add(time.Duration(latency) * time.Millisecond)
	l := f.getOrCreateLink(msg.SenderID, msg.TargetID)
	if l == nil {
		return // fabric closed
	}
	l.mu.Lock()
	if at.Before(l.lastAt) {
		at = l.lastAt
	}
	l.lastAt = at
	l.mu.Unlock()

	select {
	case l.ch <- timedMsg{msg: msg, at: at}:
	default:
		f.dropped.Add(1)
		// link queue full — drop (backpressure)
	}
}

func (f *NetworkFabric) Broadcast(msg Message, targetIDs []string) {
	for _, id := range targetIDs {
		m := msg
		m.TargetID = id
		f.Send(m)
	}
}

func (f *NetworkFabric) SetLatency(from, to string, ms int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.latencyMs[from] == nil {
		f.latencyMs[from] = make(map[string]int)
	}
	f.latencyMs[from][to] = ms
}

func (f *NetworkFabric) SetDropRate(from, to string, rate float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropRate[from] == nil {
		f.dropRate[from] = make(map[string]float64)
	}
	f.dropRate[from][to] = rate
}

func (f *NetworkFabric) AddPartition(p *Partition) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitions[p.ID] = p
}

func (f *NetworkFabric) RemovePartition(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.partitions, id)
}

func (f *NetworkFabric) ClearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencyMs = make(map[string]map[string]int)
	f.dropRate = make(map[string]map[string]float64)
	f.partitions = make(map[string]*Partition)
}

func (f *NetworkFabric) getLatency(from, to string) int {
	if m, ok := f.latencyMs[from]; ok {
		if l, ok := m[to]; ok {
			return l
		}
	}
	return 0
}

func (f *NetworkFabric) getDropRate(from, to string) float64 {
	if m, ok := f.dropRate[from]; ok {
		if r, ok := m[to]; ok {
			return r
		}
	}
	return 0
}

func (f *NetworkFabric) isPartitioned(from, to string) bool {
	for _, p := range f.partitions {
		if p.Blocks(from, to) {
			return true
		}
	}
	return false
}

func (f *NetworkFabric) GetPartitions() map[string]*Partition {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make(map[string]*Partition, len(f.partitions))
	for k, v := range f.partitions {
		result[k] = v
	}
	return result
}
