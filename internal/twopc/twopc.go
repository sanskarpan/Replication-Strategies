// Package twopc implements a Two-Phase Commit (2PC) coordinator and
// participants for atomic multi-key mini-transactions.
//
// The coordinator drives the classic two rounds: a prepare (voting) phase and
// a commit/abort (decision) phase. Participants stage writes and hold locks on
// prepared keys until they receive the coordinator's decision, exhibiting the
// blocking property that makes 2PC vulnerable to coordinator failures. A crash
// can be injected between the two phases to demonstrate blocked participants
// and recovery.
package twopc

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCoordinatorCrashed is returned by Execute when CrashAfterPrepare is set.
// The transaction has completed phase 1 (all participants prepared) but the
// coordinator "crashed" before delivering a decision, leaving participants
// blocked while holding locks on the prepared keys. Recover finishes it.
var ErrCoordinatorCrashed = errors.New("twopc: coordinator crashed after prepare, participants blocked")

// Participant is one node taking part in a 2PC transaction.
type Participant interface {
	// Prepare stages the writes for txID and votes: true means the
	// participant is ready to commit (and now holds locks on the written
	// keys); false means it cannot and the transaction must abort.
	Prepare(txID string, writes map[string][]byte) (bool, error)
	// Commit durably applies the staged writes for txID and releases locks.
	Commit(txID string) error
	// Abort discards the staged writes for txID and releases locks.
	Abort(txID string) error
}

// MemParticipant is an in-memory Participant with a staging area and per-key
// locking. It is safe for concurrent use.
type MemParticipant struct {
	// VoteNo, when set, makes Prepare vote no without staging.
	VoteNo bool
	// Down, when set, makes Prepare return an error (simulated failure).
	Down bool

	mu        sync.Mutex
	committed map[string][]byte            // durable, committed state
	staged    map[string]map[string][]byte // txID -> staged writes
	locked    map[string]string            // key -> owning txID
}

// NewMemParticipant returns an empty in-memory participant.
func NewMemParticipant() *MemParticipant {
	return &MemParticipant{
		committed: make(map[string][]byte),
		staged:    make(map[string]map[string][]byte),
		locked:    make(map[string]string),
	}
}

// Prepare stages writes and acquires locks on their keys, voting yes. It votes
// no if configured to (VoteNo), or if any target key is already locked by a
// different transaction (the blocking property). It returns an error if Down.
func (p *MemParticipant) Prepare(txID string, writes map[string][]byte) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Down {
		return false, fmt.Errorf("twopc: participant down, cannot prepare tx %s", txID)
	}
	if p.VoteNo {
		return false, nil
	}

	// A key locked by another in-flight transaction blocks us: vote no.
	for k := range writes {
		if owner, held := p.locked[k]; held && owner != txID {
			return false, nil
		}
	}

	staged := make(map[string][]byte, len(writes))
	for k, v := range writes {
		cp := make([]byte, len(v))
		copy(cp, v)
		staged[k] = cp
		p.locked[k] = txID
	}
	p.staged[txID] = staged
	return true, nil
}

// Commit moves this transaction's staged writes into committed state and
// releases its locks. Committing an unknown/already-resolved txID is a no-op.
func (p *MemParticipant) Commit(txID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	staged, ok := p.staged[txID]
	if !ok {
		return nil
	}
	for k, v := range staged {
		p.committed[k] = v
		p.releaseLocked(k, txID)
	}
	delete(p.staged, txID)
	return nil
}

// Abort discards this transaction's staged writes and releases its locks.
// Aborting an unknown/already-resolved txID is a no-op.
func (p *MemParticipant) Abort(txID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	staged, ok := p.staged[txID]
	if !ok {
		return nil
	}
	for k := range staged {
		p.releaseLocked(k, txID)
	}
	delete(p.staged, txID)
	return nil
}

// releaseLocked drops the lock on key k only if it is owned by txID.
// The caller must hold p.mu.
func (p *MemParticipant) releaseLocked(k, txID string) {
	if p.locked[k] == txID {
		delete(p.locked, k)
	}
}

// Get returns the committed value for key and whether it exists. Staged (not
// yet committed) writes are not visible.
func (p *MemParticipant) Get(key string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.committed[key]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

// IsLocked reports whether key is currently locked by a prepared transaction.
func (p *MemParticipant) IsLocked(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, held := p.locked[key]
	return held
}

// Coordinator drives 2PC transactions across a set of participants.
type Coordinator struct {
	// CrashAfterPrepare, when true, makes Execute stop after phase 1: all
	// participants have voted yes and are holding locks, but no decision is
	// delivered. Execute returns ErrCoordinatorCrashed and the participants
	// remain blocked until Recover is called.
	CrashAfterPrepare bool

	mu sync.Mutex
}

// Execute runs a 2PC transaction. perParticipant maps each Participant to the
// writes it should apply. Phase 1 sends Prepare to all; if all vote yes it
// proceeds to phase 2 and Commits all, returning committed=true. If any votes
// no or errors, it Aborts all and returns committed=false. When
// CrashAfterPrepare is set and all voted yes, it returns ErrCoordinatorCrashed
// without a decision, leaving participants blocked.
func (c *Coordinator) Execute(txID string, perParticipant map[Participant]map[string][]byte) (committed bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Fix an ordering so behavior is deterministic across the phases.
	parts := make([]Participant, 0, len(perParticipant))
	for p := range perParticipant {
		parts = append(parts, p)
	}

	// Phase 1: prepare / vote.
	allYes := true
	var voteErr error
	for _, p := range parts {
		ok, perr := p.Prepare(txID, perParticipant[p])
		if perr != nil {
			voteErr = perr
			allYes = false
			break
		}
		if !ok {
			allYes = false
			break
		}
	}

	if !allYes {
		// Phase 2 (abort): every participant must roll back staging.
		_ = c.decide(txID, parts, false)
		return false, voteErr
	}

	// Crash injection: stop before delivering the decision.
	if c.CrashAfterPrepare {
		return false, ErrCoordinatorCrashed
	}

	// Phase 2 (commit).
	if cerr := c.decide(txID, parts, true); cerr != nil {
		return false, cerr
	}
	return true, nil
}

// decide sends the phase-2 decision (commit if true, else abort) for txID to
// every participant, returning the first error encountered.
func (c *Coordinator) decide(txID string, parts []Participant, commit bool) error {
	var firstErr error
	for _, p := range parts {
		var err error
		if commit {
			err = p.Commit(txID)
		} else {
			err = p.Abort(txID)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Recover finishes a transaction that was left in-flight by a coordinator
// crash. If decision is true the transaction commits on every participant,
// otherwise it aborts; either way the participants' locks are released.
func (c *Coordinator) Recover(txID string, decision bool, participants ...Participant) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, p := range participants {
		var err error
		if decision {
			err = p.Commit(txID)
		} else {
			err = p.Abort(txID)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
