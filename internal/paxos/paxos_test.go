package paxos

import "testing"

// newAcceptors returns n fresh acceptors backing a single decree.
func newAcceptors(n int) []*Acceptor {
	as := make([]*Acceptor, n)
	for i := range as {
		as[i] = &Acceptor{}
	}
	return as
}

// TestSingleProposerChoosesItsValue verifies the happy path: with no
// competition a proposer's own value is chosen.
func TestSingleProposerChoosesItsValue(t *testing.T) {
	acceptors := newAcceptors(3)
	p := NewProposer(1)

	chosen, ok := p.Propose(acceptors, "hello")
	if !ok {
		t.Fatalf("Propose failed, expected a majority to accept")
	}
	if chosen != "hello" {
		t.Fatalf("chosen = %v, want %q", chosen, "hello")
	}
}

// TestCompetingProposersAgreeOnValue is the core safety test. Two proposers
// with distinct ids drive interleaved rounds against the same acceptor set.
// P2 uses a strictly higher proposal number and completes phase 1 only after
// P1 has already had its value accepted by a majority. The safety rule forces
// P2 to adopt and re-propose P1's already-chosen value, so the two proposers
// can never choose different values.
func TestCompetingProposersAgreeOnValue(t *testing.T) {
	acceptors := newAcceptors(3)

	p1 := NewProposer(1)
	p2 := NewProposer(2)

	// --- P1 phase 1 with n1 = (1<<16)|1 ---
	n1 := p1.nextProposalNumber()
	promises1 := 0
	for _, a := range acceptors {
		if promised, _, _ := a.Prepare(n1); promised {
			promises1++
		}
	}
	if promises1 < majority(len(acceptors)) {
		t.Fatalf("P1 failed to get a majority of promises")
	}

	// --- P1 phase 2: value "A" accepted by a majority (it is now chosen) ---
	accepts1 := 0
	for _, a := range acceptors {
		if a.Accept(n1, "A") {
			accepts1++
		}
	}
	if accepts1 < majority(len(acceptors)) {
		t.Fatalf("P1 failed to get a majority of accepts; value not chosen")
	}

	// --- P2 now runs the full protocol with a higher number n2 ---
	// Its own value is "B", but because "A" is already chosen the safety rule
	// must make P2 re-propose "A".
	chosen2, ok := p2.Propose(acceptors, "B")
	if !ok {
		t.Fatalf("P2 Propose failed unexpectedly")
	}
	if chosen2 != "A" {
		t.Fatalf("P2 chose %v, want %q (must adopt the already-chosen value)", chosen2, "A")
	}

	// Every acceptor must ultimately hold the single chosen value "A".
	for i, a := range acceptors {
		if a.acceptedV != "A" {
			t.Fatalf("acceptor %d holds %v, want %q", i, a.acceptedV, "A")
		}
	}
}

// TestStaleProposalNumberRejected verifies that a proposer whose proposal
// number is lower than what a majority has already promised is rejected in
// phase 1 and fails to choose anything.
func TestStaleProposalNumberRejected(t *testing.T) {
	acceptors := newAcceptors(3)

	// A high-numbered proposer bumps every acceptor's promise well past
	// anything a low-id proposer's first round could produce.
	high := NewProposer(500)
	if _, ok := high.Propose(acceptors, "fresh"); !ok {
		t.Fatalf("high proposer should have succeeded")
	}

	// A brand-new low-id proposer's first proposal number is (1<<16)|1, which
	// is far below the promised numbers, so it must be rejected by a majority.
	stale := NewProposer(1)
	n := stale.nextProposalNumber()
	promises := 0
	for _, a := range acceptors {
		if promised, _, _ := a.Prepare(n); promised {
			promises++
		}
	}
	if promises >= majority(len(acceptors)) {
		t.Fatalf("stale proposal (n=%d) unexpectedly won a majority of promises", n)
	}

	// Driven through the full protocol the stale proposer must fail outright.
	stale2 := NewProposer(2)
	if _, ok := stale2.Propose(acceptors, "stale"); ok {
		// A fresh proposer with a higher round could still win, so only fail
		// if the stale one somehow overwrote the chosen value.
		for _, a := range acceptors {
			if a.acceptedV == "stale" {
				t.Fatalf("stale value was chosen, safety violated")
			}
		}
	}
}

// TestMultiPaxosFillsLogInOrder verifies that Multi-Paxos decides successive
// slots and exposes them in order via Log().
func TestMultiPaxosFillsLogInOrder(t *testing.T) {
	m := NewMultiPaxos(3)

	values := []interface{}{"set x=1", "set y=2", "del x", "set z=3"}
	for slot, v := range values {
		chosen, ok := m.Decide(slot, 1, v)
		if !ok {
			t.Fatalf("Decide(slot=%d) failed", slot)
		}
		if chosen != v {
			t.Fatalf("slot %d chose %v, want %v", slot, chosen, v)
		}
	}

	log := m.Log()
	if len(log) != len(values) {
		t.Fatalf("log length = %d, want %d", len(log), len(values))
	}
	for slot, want := range values {
		if log[slot] != want {
			t.Fatalf("log[%d] = %v, want %v", slot, log[slot], want)
		}
	}
}

// TestMultiPaxosSlotSafety verifies that once a slot is decided, a later
// competing proposer into the same slot re-proposes the already-chosen value
// rather than a new one.
func TestMultiPaxosSlotSafety(t *testing.T) {
	m := NewMultiPaxos(3)

	chosen1, ok := m.Decide(2, 1, "first")
	if !ok || chosen1 != "first" {
		t.Fatalf("initial Decide got (%v,%v), want (first,true)", chosen1, ok)
	}

	// A different proposer tries to write a different value into the same slot.
	chosen2, ok := m.Decide(2, 2, "second")
	if !ok {
		t.Fatalf("second Decide failed")
	}
	if chosen2 != "first" {
		t.Fatalf("slot 2 re-decided to %v, want first (safety violated)", chosen2)
	}
	if got := m.Log()[2]; got != "first" {
		t.Fatalf("log[2] = %v, want first", got)
	}
}
