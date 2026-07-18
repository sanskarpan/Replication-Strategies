package durability

import (
	"fmt"
	"sync"
)

// Mode selects the durability policy for a WAL. It trades write latency against
// the risk of losing acknowledged-but-not-yet-durable records on a crash.
type Mode int

const (
	// Buffered acks writes as soon as they hit the in-memory buffer. Fast, but a
	// crash loses every record that has not been explicitly Flush()ed.
	Buffered Mode = iota
	// Fsync flushes each record to the durable segment before returning. Slow,
	// but nothing acked is ever lost.
	Fsync
	// GroupCommit buffers writes and amortizes the fsync across a batch: records
	// become durable when Flush() is called or when GroupSize is reached.
	GroupCommit
)

func (m Mode) String() string {
	switch m {
	case Buffered:
		return "buffered"
	case Fsync:
		return "fsync"
	case GroupCommit:
		return "group_commit"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// WAL is an in-memory model of a write-ahead log with a tunable durability mode.
// It keeps two regions: a durable segment that survives a Crash() (models data
// that reached stable storage via fsync) and a buffer that is discarded on a
// Crash() (models data still in the OS page cache / disk write cache).
//
// The model deliberately conflates "acked to the client" with "returned from
// Append": every Append acks. Whether the record is durable at ack time depends
// on the Mode, which is the whole point — Buffered and GroupCommit can ack data
// that a crash then loses.
type WAL struct {
	mu        sync.Mutex
	mode      Mode
	groupSize int

	durable [][]byte
	buffer  [][]byte

	acked int
	lost  int
}

// New constructs a WAL in the given mode. groupSize is only consulted in
// GroupCommit mode; a value < 1 is clamped to 1 (fsync every record).
func New(mode Mode, groupSize int) *WAL {
	if groupSize < 1 {
		groupSize = 1
	}
	return &WAL{
		mode:      mode,
		groupSize: groupSize,
	}
}

// Append writes record to the log and acks it. The returned bool reports whether
// the record is durable (survives a crash) at the moment of ack:
//   - Buffered: goes to the buffer, acked, NOT durable (false).
//   - Fsync: flushed to the durable segment immediately, durable (true).
//   - GroupCommit: goes to the buffer; if that fills the group, the whole buffer
//     is flushed and this record is durable (true), otherwise it is buffered and
//     not yet durable (false).
//
// A defensive copy of record is stored so later mutation by the caller cannot
// corrupt the log.
func (w *WAL) Append(record []byte) (durable bool) {
	cp := make([]byte, len(record))
	copy(cp, record)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.acked++

	switch w.mode {
	case Fsync:
		w.durable = append(w.durable, cp)
		return true
	case GroupCommit:
		w.buffer = append(w.buffer, cp)
		if len(w.buffer) >= w.groupSize {
			w.flushLocked()
			return true
		}
		return false
	default: // Buffered
		w.buffer = append(w.buffer, cp)
		return false
	}
}

// Flush moves all buffered records to the durable segment, modeling an fsync. It
// returns the number of records made durable by this call.
func (w *WAL) Flush() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// flushLocked assumes w.mu is held.
func (w *WAL) flushLocked() int {
	n := len(w.buffer)
	if n == 0 {
		return 0
	}
	w.durable = append(w.durable, w.buffer...)
	w.buffer = nil
	return n
}

// Crash models power loss: the buffer is discarded. Every record that lived only
// in the buffer was acked to a client but is now gone, so it is counted as lost.
// Durable records survive untouched.
func (w *WAL) Crash() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lost += len(w.buffer)
	w.buffer = nil
}

// DurableRecords returns a copy of the records currently in the durable segment.
func (w *WAL) DurableRecords() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return cloneRecords(w.durable)
}

// BufferedRecords returns a copy of the records currently in the buffer (not yet
// durable, at risk on a crash).
func (w *WAL) BufferedRecords() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return cloneRecords(w.buffer)
}

// Acked returns the total number of records that were acked to clients.
func (w *WAL) Acked() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked
}

// Lost returns the number of records that were acked to a client but later lost
// in a crash.
func (w *WAL) Lost() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lost
}

// Mode returns the WAL's durability mode.
func (w *WAL) Mode() Mode {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mode
}

func cloneRecords(src [][]byte) [][]byte {
	out := make([][]byte, len(src))
	for i, r := range src {
		cp := make([]byte, len(r))
		copy(cp, r)
		out[i] = cp
	}
	return out
}
