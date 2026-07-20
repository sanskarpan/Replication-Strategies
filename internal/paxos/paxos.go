// Package paxos implements single-decree Paxos and a Multi-Paxos replicated
// log. It is pure logic: message passing between proposers and acceptors is
// modeled as direct method calls over an in-memory set of acceptors, so the
// safety properties of the protocol can be studied and tested without any
// networking, timeouts, or concurrency.
package paxos

// Acceptor is a single-decree Paxos acceptor. It durably remembers the highest
// proposal number it has promised and the highest-numbered proposal it has
// accepted (together with that proposal's value).
type Acceptor struct {
	promisedN uint64      // highest proposal number promised (phase 1)
	acceptedN uint64      // proposal number of the accepted value (phase 2)
	acceptedV interface{} // the accepted value, nil if nothing accepted
}

// Prepare handles a phase-1 (prepare) request for proposal number n. The
// acceptor promises not to accept any proposal numbered lower than n iff n is
// strictly greater than any number it has already promised. On promise it
// reports back the highest-numbered proposal it has previously accepted, if
// any, so the proposer can honor the Paxos safety rule.
func (a *Acceptor) Prepare(n uint64) (promised bool, acceptedN uint64, acceptedV interface{}) {
	if n > a.promisedN {
		a.promisedN = n
		return true, a.acceptedN, a.acceptedV
	}
	return false, a.acceptedN, a.acceptedV
}

// Accept handles a phase-2 (accept) request for proposal number n with value v.
// The acceptor accepts iff n is at least as large as any number it has
// promised (n >= promisedN), which prevents it from breaking an earlier
// promise. Accepting also advances the promise, since accepting n implies
// having promised n.
func (a *Acceptor) Accept(n uint64, v interface{}) (accepted bool) {
	if n >= a.promisedN {
		a.promisedN = n
		a.acceptedN = n
		a.acceptedV = v
		return true
	}
	return false
}

// Proposer drives the two-phase Paxos protocol for a single decree against a
// set of acceptors. Each proposer must own a distinct id so that the proposal
// numbers it mints are globally unique and totally ordered.
type Proposer struct {
	id    uint64 // proposer-unique identifier, must fit in the low 16 bits
	round uint64 // monotonically increasing round counter
}

// NewProposer returns a proposer with the given unique id. The id occupies the
// low 16 bits of every proposal number, so ids must be in [0, 65535].
func NewProposer(id uint64) *Proposer {
	return &Proposer{id: id}
}

// nextProposalNumber returns a fresh, monotonically increasing proposal number
// that is unique to this proposer. Numbers are formed as round<<16 | id so
// that (a) higher rounds always outrank lower rounds and (b) two proposers in
// the same round never collide.
func (p *Proposer) nextProposalNumber() uint64 {
	p.round++
	return (p.round << 16) | (p.id & 0xFFFF)
}

// majority returns the smallest number of acceptors that constitutes a strict
// majority of n acceptors.
func majority(n int) int {
	return n/2 + 1
}

// Propose runs the full two-phase Paxos protocol to choose a value in a single
// decree against the given acceptors.
//
// Phase 1: the proposer picks a fresh proposal number n and sends Prepare(n) to
// every acceptor. If a majority promise, the proposer inspects their replies
// and adopts the value of the highest-numbered proposal any of them has already
// accepted; if none has accepted anything it is free to use its own value v.
// This adoption step is the core Paxos safety rule: it guarantees that once a
// value has been chosen it is the only value any future proposer can choose.
//
// Phase 2: the proposer sends Accept(n, chosenV) to every acceptor and succeeds
// iff a majority accept. On success it returns the value that was chosen, which
// may differ from v if a value had already been (partially or fully) accepted.
func (p *Proposer) Propose(acceptors []*Acceptor, v interface{}) (chosen interface{}, ok bool) {
	if len(acceptors) == 0 {
		return nil, false
	}
	need := majority(len(acceptors))
	n := p.nextProposalNumber()

	// Phase 1: prepare.
	promises := 0
	chosenV := v
	var highestAcceptedN uint64
	for _, a := range acceptors {
		promised, accN, accV := a.Prepare(n)
		if !promised {
			continue
		}
		promises++
		// Adopt the value with the highest acceptedN seen among promisers.
		if accV != nil && accN > highestAcceptedN {
			highestAcceptedN = accN
			chosenV = accV
		}
	}
	if promises < need {
		return nil, false
	}

	// Phase 2: accept.
	accepts := 0
	for _, a := range acceptors {
		if a.Accept(n, chosenV) {
			accepts++
		}
	}
	if accepts < need {
		return nil, false
	}
	return chosenV, true
}

// MultiPaxos runs an independent single-decree Paxos instance for each slot of
// a replicated log. Every slot has its own set of acceptors, so choosing a
// value in one slot is completely isolated from the others.
type MultiPaxos struct {
	numAcceptors int
	slots        [][]*Acceptor // per-slot acceptor sets, indexed by slot
	log          []interface{} // chosen value per slot, nil if undecided
}

// NewMultiPaxos returns a Multi-Paxos log whose every slot is replicated across
// numAcceptors acceptors.
func NewMultiPaxos(numAcceptors int) *MultiPaxos {
	if numAcceptors <= 0 {
		numAcceptors = 1
	}
	return &MultiPaxos{numAcceptors: numAcceptors}
}

// growTo ensures the log and per-slot acceptor sets cover indices up to and
// including slot, creating fresh acceptors for any newly reachable slots.
func (m *MultiPaxos) growTo(slot int) {
	for len(m.slots) <= slot {
		set := make([]*Acceptor, m.numAcceptors)
		for i := range set {
			set[i] = &Acceptor{}
		}
		m.slots = append(m.slots, set)
		m.log = append(m.log, nil)
	}
}

// Acceptors returns the acceptor set backing the given slot, creating it (and
// any earlier slots) on first use. This lets callers run multiple competing
// proposers against the same slot.
func (m *MultiPaxos) Acceptors(slot int) []*Acceptor {
	m.growTo(slot)
	return m.slots[slot]
}

// Decide runs a proposer through the full Paxos protocol for the given slot and
// records the chosen value in the log. Because Paxos is safe, if the slot was
// already decided the returned value is the previously chosen one regardless of
// v. It returns the chosen value and whether a majority accepted.
func (m *MultiPaxos) Decide(slot int, proposerID uint64, v interface{}) (chosen interface{}, ok bool) {
	m.growTo(slot)
	p := NewProposer(proposerID)
	chosen, ok = p.Propose(m.slots[slot], v)
	if ok {
		m.log[slot] = chosen
	}
	return chosen, ok
}

// Log returns the chosen value for every slot in order. Undecided slots hold a
// nil entry. The returned slice is a copy and safe to retain.
func (m *MultiPaxos) Log() []interface{} {
	out := make([]interface{}, len(m.log))
	copy(out, m.log)
	return out
}
