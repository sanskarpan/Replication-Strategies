package swim

import (
	"reflect"
	"testing"
)

func TestSuspectSameIncarnationBecomesSuspect(t *testing.T) {
	m := NewMembership("self")
	m.Alive("n1", 1)
	m.Suspect("n1", 1, 100)

	if got := m.SuspectMembers(); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("expected n1 suspect, got suspects=%v", got)
	}
	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"self"}) {
		t.Fatalf("expected only self alive, got alive=%v", got)
	}

	// An Alive at the same incarnation must NOT refute a suspicion.
	m.Alive("n1", 1)
	if got := m.SuspectMembers(); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("same-incarnation Alive should not refute suspicion, suspects=%v", got)
	}
}

func TestRefutationViaHigherIncarnation(t *testing.T) {
	m := NewMembership("self")
	m.Alive("n1", 1)
	m.Suspect("n1", 1, 100)
	if got := m.SuspectMembers(); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("precondition: n1 should be suspect, got %v", got)
	}

	// A strictly higher incarnation Alive refutes the suspicion.
	m.Alive("n1", 2)

	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"n1", "self"}) {
		t.Fatalf("higher-incarnation Alive should refute, alive=%v", got)
	}
	if got := m.SuspectMembers(); len(got) != 0 {
		t.Fatalf("expected no suspects after refutation, got %v", got)
	}
	mem, _ := m.Get("n1")
	if mem.Incarnation != 2 {
		t.Fatalf("expected incarnation bumped to 2, got %d", mem.Incarnation)
	}
}

func TestTickSuspicionsPromotesOldSuspectToDead(t *testing.T) {
	m := NewMembership("self")
	m.Alive("old", 1)
	m.Alive("fresh", 1)

	m.Suspect("old", 1, 100)   // suspected at t=100
	m.Suspect("fresh", 1, 500) // suspected at t=500

	// Deadline 400: "old" (100 < 400) promoted, "fresh" (500 !< 400) not.
	promoted := m.TickSuspicions(400)
	if !reflect.DeepEqual(promoted, []string{"old"}) {
		t.Fatalf("expected only 'old' promoted, got %v", promoted)
	}
	if got := m.DeadMembers(); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("expected old dead, got dead=%v", got)
	}
	if got := m.SuspectMembers(); !reflect.DeepEqual(got, []string{"fresh"}) {
		t.Fatalf("expected fresh still suspect, got %v", got)
	}
}

func TestDeadIsTerminal(t *testing.T) {
	m := NewMembership("self")
	m.Alive("n1", 5)
	m.Dead("n1")

	// Neither an equal nor a higher incarnation Alive revives a Dead member.
	m.Alive("n1", 6)
	if got := m.DeadMembers(); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("Dead must be terminal, dead=%v", got)
	}
	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"self"}) {
		t.Fatalf("expected only self alive, got %v", got)
	}
}

func TestMergePicksHigherIncarnationState(t *testing.T) {
	m := NewMembership("self")
	m.Alive("n1", 1)

	// Peer gossips n1 Suspect at a higher incarnation: higher wins.
	m.Merge([]Member{{ID: "n1", State: Suspect, Incarnation: 3}})
	mem, _ := m.Get("n1")
	if mem.State != Suspect || mem.Incarnation != 3 {
		t.Fatalf("higher incarnation should win, got %+v", mem)
	}

	// Peer gossips n1 Alive at a LOWER incarnation: ignored.
	m.Merge([]Member{{ID: "n1", State: Alive, Incarnation: 2}})
	mem, _ = m.Get("n1")
	if mem.State != Suspect || mem.Incarnation != 3 {
		t.Fatalf("lower incarnation must not override, got %+v", mem)
	}

	// Peer gossips n1 Alive at a strictly higher incarnation: refutes.
	m.Merge([]Member{{ID: "n1", State: Alive, Incarnation: 4}})
	mem, _ = m.Get("n1")
	if mem.State != Alive || mem.Incarnation != 4 {
		t.Fatalf("higher-incarnation Alive should refute, got %+v", mem)
	}
}

func TestMergeEqualIncarnationDeadBeatsAlive(t *testing.T) {
	m := NewMembership("self")
	m.Alive("n1", 7)

	// Same incarnation, but gossip says Dead. Dead > Alive at equal incarnation.
	m.Merge([]Member{{ID: "n1", State: Dead, Incarnation: 7}})
	if got := m.DeadMembers(); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("equal-incarnation Dead must beat Alive, dead=%v", got)
	}

	// Also verify equal-incarnation Suspect beats Alive.
	m2 := NewMembership("self")
	m2.Alive("n2", 7)
	m2.Merge([]Member{{ID: "n2", State: Suspect, Incarnation: 7}})
	if got := m2.SuspectMembers(); !reflect.DeepEqual(got, []string{"n2"}) {
		t.Fatalf("equal-incarnation Suspect must beat Alive, suspects=%v", got)
	}
}

func TestMergeUnknownMemberAdded(t *testing.T) {
	m := NewMembership("self")
	m.Merge([]Member{{ID: "n1", State: Alive, Incarnation: 2}})
	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"n1", "self"}) {
		t.Fatalf("unknown member from gossip should be added, alive=%v", got)
	}
}

func TestSelfSuspicionRefutedByBumpingIncarnation(t *testing.T) {
	m := NewMembership("self")
	start := m.SelfIncarnation()

	// A suspicion about self is auto-refuted.
	m.Suspect("self", start, 100)
	if got := m.SuspectMembers(); len(got) != 0 {
		t.Fatalf("self must never be Suspect, got %v", got)
	}
	if m.SelfIncarnation() <= start {
		t.Fatalf("self incarnation should have been bumped, was %d now %d", start, m.SelfIncarnation())
	}
	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"self"}) {
		t.Fatalf("self should remain alive, alive=%v", got)
	}
}

func TestMergeSelfDeathIsRefuted(t *testing.T) {
	m := NewMembership("self")
	start := m.SelfIncarnation()

	// Peer wrongly gossips that we are Dead at our current incarnation.
	m.Merge([]Member{{ID: "self", State: Dead, Incarnation: start}})

	if got := m.AliveMembers(); !reflect.DeepEqual(got, []string{"self"}) {
		t.Fatalf("self death must be refuted, alive=%v", got)
	}
	if m.SelfIncarnation() <= start {
		t.Fatalf("self incarnation should out-pace the death claim, was %d now %d", start, m.SelfIncarnation())
	}
}

func TestRefuteSuspicionBumpsIncarnation(t *testing.T) {
	m := NewMembership("self")
	before := m.SelfIncarnation()
	m.RefuteSuspicion()
	if m.SelfIncarnation() != before+1 {
		t.Fatalf("RefuteSuspicion should increment self incarnation by 1, was %d now %d", before, m.SelfIncarnation())
	}
}
