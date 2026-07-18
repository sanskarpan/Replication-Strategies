// Package checker implements a linearizability checker for a single-register
// model in the style of Porcupine / Wing & Gong. It verifies that the
// per-key sub-histories of concurrent read/write operations are linearizable
// against a last-write-wins register whose initial value is the empty string.
package checker

import (
	"sort"
	"sync"
)

// OpKind identifies the kind of register operation.
type OpKind int

const (
	// OpWrite sets the register value.
	OpWrite OpKind = iota
	// OpRead observes the register value.
	OpRead
)

// Op is a single register operation. Invoke and Complete are monotonic
// timestamps in nanoseconds delimiting the real-time interval during which the
// operation was in flight. A Read observes Value; a Write sets Value. Key
// selects which logical register the operation applies to.
type Op struct {
	ClientID string
	Kind     OpKind
	Value    string
	Key      string
	Invoke   int64
	Complete int64
}

// History is a concurrency-safe record of operations.
type History struct {
	ops []Op
	mu  sync.Mutex
}

// Record appends an operation to the history.
func (h *History) Record(op Op) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, op)
}

// Ops returns a copy of the recorded operations.
func (h *History) Ops() []Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Op, len(h.ops))
	copy(out, h.ops)
	return out
}

// CheckRegister reports whether the per-key sub-histories are linearizable
// against a last-write-wins register with initial value "". It returns
// (true, nil) when every key's sub-history linearizes, or (false, &op)
// pointing at an operation that could not be linearized otherwise.
func CheckRegister(ops []Op) (bool, *Op) {
	// Partition operations by key, preserving nothing about global order; each
	// key is an independent register.
	byKey := make(map[string][]Op)
	keys := make([]string, 0)
	for _, op := range ops {
		if _, ok := byKey[op.Key]; !ok {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	// Deterministic iteration order over keys.
	sort.Strings(keys)

	for _, key := range keys {
		sub := byKey[key]
		if ok, bad := linearizeKey(sub); !ok {
			return false, bad
		}
	}
	return true, nil
}

// linearizeKey runs the Wing & Gong backtracking search over a single key's
// sub-history. It returns (true, nil) if the sub-history linearizes against the
// register model, or (false, &op) pointing at an operation that could not be
// placed.
func linearizeKey(sub []Op) (bool, *Op) {
	// Sort by Invoke so the search considers operations in real-time invocation
	// order. Ties are broken by Complete to keep the order deterministic.
	ops := make([]Op, len(sub))
	copy(ops, sub)
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].Invoke != ops[j].Invoke {
			return ops[i].Invoke < ops[j].Invoke
		}
		return ops[i].Complete < ops[j].Complete
	})

	remaining := make([]bool, len(ops))
	for i := range remaining {
		remaining[i] = true
	}

	// blame tracks the "deepest" operation that failed to linearize, so that on
	// total failure we can report a concrete offending op rather than nil.
	var blame *Op

	var search func(current string, count int) bool
	search = func(current string, count int) bool {
		if count == len(ops) {
			return true
		}

		// Compute the minimum Complete over the remaining operations. Any
		// operation whose Invoke is <= this minimum could legally go next in
		// real-time order (its interval either precedes or overlaps that of the
		// operation with the earliest completion).
		minComplete := int64(1<<63 - 1)
		for i := range ops {
			if remaining[i] && ops[i].Complete < minComplete {
				minComplete = ops[i].Complete
			}
		}

		for i := range ops {
			if !remaining[i] {
				continue
			}
			// Only a minimal operation (candidate that could go next in
			// real-time order) may be picked. A later-invoked op can precede an
			// earlier-completed op only when their intervals overlap, which is
			// exactly the condition Invoke <= minComplete.
			if ops[i].Invoke > minComplete {
				continue
			}

			// Apply the operation to the model register.
			var next string
			switch ops[i].Kind {
			case OpWrite:
				next = ops[i].Value
			case OpRead:
				if ops[i].Value != current {
					// This read cannot be placed here; record it as a possible
					// culprit and try another candidate.
					op := ops[i]
					blame = &op
					continue
				}
				next = current
			default:
				next = current
			}

			remaining[i] = false
			if search(next, count+1) {
				remaining[i] = true
				return true
			}
			remaining[i] = true
		}
		return false
	}

	if search("", 0) {
		return true, nil
	}
	if blame == nil {
		// No read ever mismatched (e.g. empty or pathological ordering); blame
		// the earliest remaining operation.
		for i := range ops {
			if remaining[i] {
				op := ops[i]
				return false, &op
			}
		}
	}
	return false, blame
}
