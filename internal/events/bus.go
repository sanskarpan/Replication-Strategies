package events

import (
	"sync"
	"time"
)

type EventType string

const (
	EvtFollowerLag      EventType = "follower_lag"
	EvtConflictDetected EventType = "conflict_detected"
	EvtConflictResolved EventType = "conflict_resolved"
	EvtEntryReplicated  EventType = "entry_replicated"
	EvtNodeStateChanged EventType = "node_state_changed"
	EvtPartitionCreated EventType = "partition_created"
	EvtPartitionHealed  EventType = "partition_healed"
	EvtReadRepair       EventType = "read_repair"
	EvtLeaderElected    EventType = "leader_elected"
	EvtHintedHandoff    EventType = "hinted_handoff"
	EvtQuorumAchieved   EventType = "quorum_achieved"
	EvtQuorumFailed     EventType = "quorum_failed"
	EvtWriteReceived    EventType = "write_received"
	EvtReadReceived     EventType = "read_received"
	// Scenario engine: narrated step + expected-vs-actual verdict.
	EvtScenarioStep    EventType = "scenario_step"
	EvtScenarioVerdict EventType = "scenario_verdict"
)

type Event struct {
	Type         EventType              `json:"type"`
	ClusterID    string                 `json:"cluster_id"`
	NodeID       string                 `json:"node_id,omitempty"`
	Timestamp    time.Time              `json:"timestamp"`
	Data         map[string]interface{} `json:"data,omitempty"`
	// TraceCarrier holds W3C traceparent/tracestate headers so spans can be
	// linked across goroutine boundaries (bus publish → subscriber).
	TraceCarrier map[string]string      `json:"-"`
}

type Subscriber struct {
	ID     string
	Filter []EventType
	Ch     chan Event
	Done   chan struct{} // closed by Unsubscribe; callers should select on this
}

type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string]*Subscriber
	buffer      []Event
	maxBuffer   int
}

func NewEventBus(bufferSize int) *EventBus {
	return &EventBus{
		subscribers: make(map[string]*Subscriber),
		buffer:      make([]Event, 0, bufferSize),
		maxBuffer:   bufferSize,
	}
}

func (b *EventBus) Subscribe(id string, filter []EventType) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub := &Subscriber{
		ID:     id,
		Filter: filter,
		Ch:     make(chan Event, 256),
		Done:   make(chan struct{}),
	}
	b.subscribers[id] = sub
	return sub
}

func (b *EventBus) Unsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subscribers[id]; ok {
		// Signal stop via Done; do NOT close sub.Ch because Publish may be
		// concurrently selecting on it (closing a channel that another goroutine
		// is sending to causes a data race).
		close(sub.Done)
		delete(b.subscribers, id)
	}
}

func (b *EventBus) Publish(evt Event) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	b.mu.Lock()
	// buffer
	if len(b.buffer) >= b.maxBuffer {
		b.buffer = b.buffer[1:]
	}
	b.buffer = append(b.buffer, evt)
	subs := make([]*Subscriber, 0, len(b.subscribers))
	for _, s := range b.subscribers {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		if matchesFilter(evt.Type, sub.Filter) {
			select {
			case sub.Ch <- evt:
			default:
				// drop if subscriber is slow
			}
		}
	}
}

func (b *EventBus) GetRecent(n int, filter []EventType) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// Walk newest-first collecting up to n matches, then reverse once to restore
	// chronological order — O(n) instead of the previous O(n^2) prepend.
	result := make([]Event, 0, n)
	for i := len(b.buffer) - 1; i >= 0 && len(result) < n; i-- {
		if matchesFilter(b.buffer[i].Type, filter) {
			result = append(result, b.buffer[i])
		}
	}
	for l, r := 0, len(result)-1; l < r; l, r = l+1, r-1 {
		result[l], result[r] = result[r], result[l]
	}
	return result
}

func matchesFilter(t EventType, filter []EventType) bool {
	if len(filter) == 0 {
		return true
	}
	for _, f := range filter {
		if f == t {
			return true
		}
	}
	return false
}
