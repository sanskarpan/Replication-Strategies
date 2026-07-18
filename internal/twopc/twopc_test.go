package twopc

import (
	"bytes"
	"errors"
	"testing"
)

func TestExecute_AllYesCommitsAtomically(t *testing.T) {
	p1 := NewMemParticipant()
	p2 := NewMemParticipant()
	c := &Coordinator{}

	committed, err := c.Execute("tx1", map[Participant]map[string][]byte{
		p1: {"a": []byte("1")},
		p2: {"b": []byte("2")},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if !committed {
		t.Fatalf("expected committed=true")
	}

	if v, ok := p1.Get("a"); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("p1[a] = %q,%v; want 1,true", v, ok)
	}
	if v, ok := p2.Get("b"); !ok || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("p2[b] = %q,%v; want 2,true", v, ok)
	}

	// Locks released after commit.
	if p1.IsLocked("a") || p2.IsLocked("b") {
		t.Fatalf("expected locks released after commit")
	}
}

func TestExecute_OneNoVoteAbortsAll(t *testing.T) {
	p1 := NewMemParticipant()
	p2 := NewMemParticipant()
	p2.VoteNo = true
	c := &Coordinator{}

	committed, err := c.Execute("tx1", map[Participant]map[string][]byte{
		p1: {"a": []byte("1")},
		p2: {"b": []byte("2")},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if committed {
		t.Fatalf("expected committed=false on a no-vote")
	}

	// No participant committed anything.
	if _, ok := p1.Get("a"); ok {
		t.Fatalf("p1 committed despite abort")
	}
	if _, ok := p2.Get("b"); ok {
		t.Fatalf("p2 committed despite abort")
	}
	// No lingering locks after abort.
	if p1.IsLocked("a") || p2.IsLocked("b") {
		t.Fatalf("expected locks released after abort")
	}
}

func TestExecute_DownParticipantAbortsAll(t *testing.T) {
	p1 := NewMemParticipant()
	p2 := NewMemParticipant()
	p2.Down = true
	c := &Coordinator{}

	committed, err := c.Execute("tx1", map[Participant]map[string][]byte{
		p1: {"a": []byte("1")},
		p2: {"b": []byte("2")},
	})
	if err == nil {
		t.Fatalf("expected error from down participant")
	}
	if committed {
		t.Fatalf("expected committed=false")
	}
	if _, ok := p1.Get("a"); ok {
		t.Fatalf("p1 committed despite down peer")
	}
	if p1.IsLocked("a") {
		t.Fatalf("expected p1 lock released after abort")
	}
}

func TestCrashAfterPrepare_LocksHeldUntilRecover(t *testing.T) {
	p1 := NewMemParticipant()
	p2 := NewMemParticipant()
	c := &Coordinator{CrashAfterPrepare: true}

	committed, err := c.Execute("tx1", map[Participant]map[string][]byte{
		p1: {"a": []byte("1")},
		p2: {"b": []byte("2")},
	})
	if !errors.Is(err, ErrCoordinatorCrashed) {
		t.Fatalf("err = %v; want ErrCoordinatorCrashed", err)
	}
	if committed {
		t.Fatalf("expected committed=false on crash")
	}

	// Participants are blocked: keys are locked and not yet visible.
	if !p1.IsLocked("a") || !p2.IsLocked("b") {
		t.Fatalf("expected keys to remain locked after crash")
	}
	if _, ok := p1.Get("a"); ok {
		t.Fatalf("crashed tx must not be visible before recovery")
	}

	// A competing transaction touching a locked key must be blocked: the
	// participant votes no, so the whole competing tx aborts.
	c2 := &Coordinator{}
	committed2, err2 := c2.Execute("tx2", map[Participant]map[string][]byte{
		p1: {"a": []byte("99")},
	})
	if err2 != nil {
		t.Fatalf("competing Execute error: %v", err2)
	}
	if committed2 {
		t.Fatalf("competing tx on a locked key should not commit")
	}
	if v, ok := p1.Get("a"); ok {
		t.Fatalf("competing tx overwrote locked key: %q", v)
	}

	// Recovery with commit decision unblocks and applies the crashed tx.
	if err := c.Recover("tx1", true, p1, p2); err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if v, ok := p1.Get("a"); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("p1[a] = %q,%v after recover; want 1,true", v, ok)
	}
	if v, ok := p2.Get("b"); !ok || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("p2[b] = %q,%v after recover; want 2,true", v, ok)
	}
	if p1.IsLocked("a") || p2.IsLocked("b") {
		t.Fatalf("expected locks released after recovery")
	}
}

func TestRecover_AbortDiscardsAndUnblocks(t *testing.T) {
	p1 := NewMemParticipant()
	c := &Coordinator{CrashAfterPrepare: true}

	_, err := c.Execute("tx1", map[Participant]map[string][]byte{
		p1: {"a": []byte("1")},
	})
	if !errors.Is(err, ErrCoordinatorCrashed) {
		t.Fatalf("err = %v; want ErrCoordinatorCrashed", err)
	}
	if !p1.IsLocked("a") {
		t.Fatalf("expected key locked after crash")
	}

	if err := c.Recover("tx1", false, p1); err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if _, ok := p1.Get("a"); ok {
		t.Fatalf("aborted recovery must not make write visible")
	}
	if p1.IsLocked("a") {
		t.Fatalf("expected lock released after abort recovery")
	}
}
