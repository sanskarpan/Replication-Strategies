package transport

import (
	"replication-strategies/internal/storage"
)

type MessageType string

const (
	MsgAppendEntries MessageType = "append_entries"
	MsgAppendAck     MessageType = "append_ack"
	MsgWrite         MessageType = "write"
	MsgWriteAck      MessageType = "write_ack"
	MsgClientWrite   MessageType = "client_write"
	MsgClientRead    MessageType = "client_read"
	MsgSync          MessageType = "sync"
	MsgSyncAck       MessageType = "sync_ack"
	MsgReadRepair    MessageType = "read_repair"
	MsgHintedHandoff MessageType = "hinted_handoff"
	MsgHeartbeat     MessageType = "heartbeat"
	MsgHeartbeatAck  MessageType = "heartbeat_ack"
	MsgVoteRequest   MessageType = "vote_request"
	MsgVoteResponse  MessageType = "vote_response"
)

type QuorumConfig struct {
	N int `json:"n"` // total replicas
	W int `json:"w"` // write quorum
	R int `json:"r"` // read quorum
}

type Message struct {
	Type     MessageType         `json:"type"`
	SeqNo    uint64              `json:"seq_no"`
	SenderID string              `json:"sender_id"`
	TargetID string              `json:"target_id"`
	Entries  []storage.LogEntry  `json:"entries,omitempty"`
	Entry    *storage.KVEntry    `json:"entry,omitempty"`
	AckIndex uint64              `json:"ack_index,omitempty"`
	VClock   storage.VectorClock `json:"vclock,omitempty"`
	Quorum   *QuorumConfig       `json:"quorum,omitempty"`
	Error    string              `json:"error,omitempty"`
	Key      string              `json:"key,omitempty"`
	// For read repair
	StaleNodes []string `json:"stale_nodes,omitempty"`
	// Term for leader election
	Term uint64 `json:"term,omitempty"`
	// For hints
	OriginalTarget string `json:"original_target,omitempty"`
}
