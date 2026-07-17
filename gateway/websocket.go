package gateway

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"replication-strategies/internal/events"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// handleWebSocket upgrades the connection and streams all events to the client.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade: %v", err)
		return
	}
	defer conn.Close()

	subID := uuid.New().String()
	// Subscribe to all event types (nil filter = everything).
	sub := s.bus.Subscribe(subID, nil)
	defer s.bus.Unsubscribe(subID)

	// Send recent buffered events first so the client can hydrate its state.
	recent := s.bus.GetRecent(100, nil)
	for _, evt := range recent {
		if data, err := json.Marshal(evt); err == nil {
			conn.WriteMessage(websocket.TextMessage, data) //nolint:errcheck
		}
	}

	// Stream new events until the connection closes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			// We only need to detect client disconnection; we don't process
			// any messages sent by the client.
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case evt, ok := <-sub.Ch:
			if !ok {
				return
			}
			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}
}

// wsEvent is a typed envelope sent over the WebSocket connection.
// It mirrors events.Event but is kept here to decouple the wire format.
type wsEvent struct {
	Type      events.EventType       `json:"type"`
	ClusterID string                 `json:"cluster_id"`
	NodeID    string                 `json:"node_id,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}
