package events

import (
	"testing"
)

func BenchmarkBusPublish(b *testing.B) {
	bus := NewEventBus(1000)
	sub := bus.Subscribe("bench-sub", nil) // nil filter = receive all events
	defer bus.Unsubscribe("bench-sub")

	// Drain the subscriber channel in the background so it never fills and
	// causes Publish to drop events (masking true publish throughput).
	go func() {
		for {
			select {
			case <-sub.Ch:
			case <-sub.Done:
				return
			}
		}
	}()

	evt := Event{Type: EvtEntryReplicated, ClusterID: "c1", NodeID: "n1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(evt)
	}
}

func BenchmarkBusPublishFanout(b *testing.B) {
	bus := NewEventBus(1000)
	const fanout = 10
	for i := 0; i < fanout; i++ {
		id := string(rune('a' + i))
		sub := bus.Subscribe(id, nil)
		go func(s *Subscriber) {
			for {
				select {
				case <-s.Ch:
				case <-s.Done:
					return
				}
			}
		}(sub)
	}
	defer func() {
		for i := 0; i < fanout; i++ {
			bus.Unsubscribe(string(rune('a' + i)))
		}
	}()

	evt := Event{Type: EvtConflictDetected, ClusterID: "c1", NodeID: "n1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(evt)
	}
}

func BenchmarkBusSubscribeUnsubscribe(b *testing.B) {
	bus := NewEventBus(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := bus.Subscribe("churn", nil)
		_ = sub
		bus.Unsubscribe("churn")
	}
}

func BenchmarkBusGetRecent(b *testing.B) {
	bus := NewEventBus(1000)
	for i := 0; i < 1000; i++ {
		bus.Publish(Event{Type: EvtEntryReplicated, ClusterID: "c1"})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bus.GetRecent(100, nil)
	}
}
