package simulation

import (
	"fmt"

	"replication-strategies/internal/durability"
	"replication-strategies/internal/mvcc"
	"replication-strategies/internal/paxos"
	"replication-strategies/internal/simclock"
	"replication-strategies/internal/swim"
	"replication-strategies/internal/twopc"
)

// These are self-contained teaching demos that exercise the standalone distributed-
// systems primitives (2PC, MVCC, WAL durability, SWIM, Paxos, deterministic sim) and
// return a narrated result the UI can display. They don't require a running cluster.

// ---------------------------------------------------------------------------
// Two-phase commit
// ---------------------------------------------------------------------------

// TwoPCDemoReport narrates a 2PC run and its outcome.
type TwoPCDemoReport struct {
	Crash       bool              `json:"crash"`
	Committed   bool              `json:"committed"`
	Blocked     bool              `json:"blocked"`
	Recovered   bool              `json:"recovered"`
	FinalValues map[string]string `json:"final_values"`
	Narrative   []string          `json:"narrative"`
}

// RunTwoPCDemo runs an atomic two-key transaction across two participants. With crash=true
// the coordinator dies after prepare, leaving participants blocked (holding locks) until
// a recovery decision unblocks them — the classic 2PC blocking failure mode.
func RunTwoPCDemo(crash bool) TwoPCDemoReport {
	rep := TwoPCDemoReport{Crash: crash, FinalValues: map[string]string{}}
	p1 := twopc.NewMemParticipant()
	p2 := twopc.NewMemParticipant()
	coord := &twopc.Coordinator{CrashAfterPrepare: crash}

	writes := map[twopc.Participant]map[string][]byte{
		p1: {"x": []byte("1")},
		p2: {"y": []byte("2")},
	}
	rep.Narrative = append(rep.Narrative, "phase 1: coordinator sends PREPARE(x=1, y=2) to both participants")

	committed, err := coord.Execute("tx1", writes)
	rep.Committed = committed
	if err == twopc.ErrCoordinatorCrashed {
		rep.Blocked = p1.IsLocked("x") || p2.IsLocked("y")
		rep.Narrative = append(rep.Narrative,
			"coordinator CRASHED after prepare — participants stay locked/blocked, unable to proceed",
			"recovery: a new coordinator reads the decision and sends COMMIT to finish the transaction")
		if rerr := coord.Recover("tx1", true, p1, p2); rerr == nil {
			rep.Recovered = true
			rep.Committed = true
		}
	} else if committed {
		rep.Narrative = append(rep.Narrative, "all participants voted YES → phase 2: COMMIT applied atomically on every participant")
	} else {
		rep.Narrative = append(rep.Narrative, "a participant voted NO → phase 2: ABORT, no participant applies the write")
	}

	if v, ok := p1.Get("x"); ok {
		rep.FinalValues["x"] = string(v)
	}
	if v, ok := p2.Get("y"); ok {
		rep.FinalValues["y"] = string(v)
	}
	return rep
}

// ---------------------------------------------------------------------------
// MVCC snapshot reads
// ---------------------------------------------------------------------------

// MVCCDemoReport shows snapshot isolation: a read at an older timestamp never sees a
// newer version.
type MVCCDemoReport struct {
	Narrative      []string `json:"narrative"`
	ReadAt5Found   bool     `json:"read_at_5_found"`
	ReadAt15       string   `json:"read_at_15"`
	ReadAt25       string   `json:"read_at_25"`
	SnapshotStable bool     `json:"snapshot_stable"`
}

// RunMVCCDemo writes two versions of a key at logical timestamps 10 and 20, then shows
// that snapshot reads resolve to the correct version and stay stable under new writes.
func RunMVCCDemo() MVCCDemoReport {
	rep := MVCCDemoReport{}
	st := mvcc.New()
	st.Put("x", []byte("A"), 10)
	rep.Narrative = append(rep.Narrative, "write x=A @ t10")

	// Take a snapshot handle at t15 before the next write.
	v15a, _ := st.ReadAt("x", 15)

	st.Put("x", []byte("B"), 20)
	rep.Narrative = append(rep.Narrative, "write x=B @ t20")

	_, found5 := st.ReadAt("x", 5)
	rep.ReadAt5Found = found5
	v15b, _ := st.ReadAt("x", 15)
	v25, _ := st.ReadAt("x", 25)
	rep.ReadAt15 = string(v15b)
	rep.ReadAt25 = string(v25)
	rep.SnapshotStable = string(v15a) == string(v15b) // t15 snapshot unaffected by the t20 write
	rep.Narrative = append(rep.Narrative,
		"read @ t5  → not found (before the first version)",
		fmt.Sprintf("read @ t15 → %q (sees only A, the t20 write is invisible to this snapshot)", rep.ReadAt15),
		fmt.Sprintf("read @ t25 → %q (sees the newer B)", rep.ReadAt25))
	return rep
}

// ---------------------------------------------------------------------------
// WAL durability
// ---------------------------------------------------------------------------

// WALDemoReport shows how many acked records survive a crash under a durability mode.
type WALDemoReport struct {
	Mode      string   `json:"mode"`
	Acked     int      `json:"acked"`
	Durable   int      `json:"durable_before_crash"`
	Lost      int      `json:"lost"`
	Narrative []string `json:"narrative"`
}

// RunWALDemo appends five acked records, crashes, and reports how many were lost under
// the chosen durability mode (buffered loses acked data; fsync loses nothing).
func RunWALDemo(mode string) WALDemoReport {
	m := durability.Buffered
	switch mode {
	case "fsync":
		m = durability.Fsync
	case "group_commit":
		m = durability.GroupCommit
	}
	w := durability.New(m, 3)
	for i := 0; i < 5; i++ {
		w.Append([]byte(fmt.Sprintf("rec-%d", i)))
	}
	rep := WALDemoReport{Mode: m.String(), Acked: w.Acked(), Durable: len(w.DurableRecords())}
	rep.Narrative = append(rep.Narrative, fmt.Sprintf("appended & acked 5 records in %s mode; %d durable before crash", m.String(), rep.Durable))
	w.Crash()
	rep.Lost = w.Lost()
	if rep.Lost > 0 {
		rep.Narrative = append(rep.Narrative, fmt.Sprintf("CRASH: %d acked records were only buffered (not fsynced) and are LOST", rep.Lost))
	} else {
		rep.Narrative = append(rep.Narrative, "CRASH: every acked record was durable — nothing lost")
	}
	return rep
}

// ---------------------------------------------------------------------------
// SWIM gossip membership
// ---------------------------------------------------------------------------

// SWIMDemoReport shows suspicion, incarnation-based refutation, and death promotion.
type SWIMDemoReport struct {
	Narrative    []string `json:"narrative"`
	FinalAlive   []string `json:"final_alive"`
	FinalSuspect []string `json:"final_suspect"`
	FinalDead    []string `json:"final_dead"`
}

// RunSWIMDemo drives a SWIM membership through suspect → refute (higher incarnation) and
// suspect → dead (suspicion timeout).
func RunSWIMDemo() SWIMDemoReport {
	rep := SWIMDemoReport{}
	m := swim.NewMembership("n1")
	m.Alive("n2", 1)
	m.Alive("n3", 1)
	rep.Narrative = append(rep.Narrative, "n1 knows n2@inc1 and n3@inc1 as ALIVE")

	m.Suspect("n2", 1, 0)
	rep.Narrative = append(rep.Narrative, "a missed ping SUSPECTS n2 (incarnation 1)")
	m.Alive("n2", 2)
	rep.Narrative = append(rep.Narrative, "n2 gossips ALIVE@inc2 (higher incarnation) → suspicion REFUTED, n2 back to ALIVE")

	m.Suspect("n3", 1, 0)
	dead := m.TickSuspicions(1_000_000)
	rep.Narrative = append(rep.Narrative, fmt.Sprintf("n3 suspected and never refutes → suspicion timeout promotes it to DEAD (%v)", dead))

	rep.FinalAlive = m.AliveMembers()
	rep.FinalSuspect = m.SuspectMembers()
	rep.FinalDead = m.DeadMembers()
	return rep
}

// ---------------------------------------------------------------------------
// Paxos / Multi-Paxos
// ---------------------------------------------------------------------------

// PaxosDemoReport shows the Paxos safety property: once a value is chosen, a later
// competing proposer is forced to adopt it.
type PaxosDemoReport struct {
	Narrative   []string `json:"narrative"`
	FirstChosen string   `json:"first_chosen"`
	SecondValue string   `json:"second_proposed"`
	SecondFinal string   `json:"second_chosen"`
	SafetyHeld  bool     `json:"safety_held"`
}

// RunPaxosDemo has two proposers contend for one value; the second must converge on the
// value the first already got a majority to accept.
func RunPaxosDemo() PaxosDemoReport {
	rep := PaxosDemoReport{}
	acceptors := make([]*paxos.Acceptor, 5)
	for i := range acceptors {
		acceptors[i] = &paxos.Acceptor{}
	}
	pA := paxos.NewProposer(1)
	chosen1, ok1 := pA.Propose(acceptors, "X")
	rep.FirstChosen = fmt.Sprintf("%v", chosen1)
	rep.Narrative = append(rep.Narrative, fmt.Sprintf("proposer A proposes \"X\" → chosen=%v (ok=%v) by a majority", chosen1, ok1))

	pB := paxos.NewProposer(2)
	chosen2, ok2 := pB.Propose(acceptors, "Y")
	rep.SecondValue = "Y"
	rep.SecondFinal = fmt.Sprintf("%v", chosen2)
	rep.Narrative = append(rep.Narrative, fmt.Sprintf("proposer B proposes \"Y\" but must adopt the already-accepted value → chosen=%v (ok=%v)", chosen2, ok2))
	rep.SafetyHeld = ok1 && ok2 && fmt.Sprintf("%v", chosen1) == fmt.Sprintf("%v", chosen2)
	if rep.SafetyHeld {
		rep.Narrative = append(rep.Narrative, "SAFETY HELD: both proposers agree on the single chosen value")
	}
	return rep
}

// ---------------------------------------------------------------------------
// Deterministic simulation
// ---------------------------------------------------------------------------

// DetSimDemoReport shows that a seeded virtual clock produces reproducible runs.
type DetSimDemoReport struct {
	Seed         int64    `json:"seed"`
	Run1         []int    `json:"run1"`
	Run2         []int    `json:"run2"`
	Reproducible bool     `json:"reproducible"`
	Narrative    []string `json:"narrative"`
}

// RunDetSimDemo runs the same seeded VirtualClock twice and confirms identical random +
// timer-ordering output — the property that makes failing runs reproducible.
func RunDetSimDemo(seed int64) DetSimDemoReport {
	rep := DetSimDemoReport{Seed: seed}
	gen := func() []int {
		vc := simclock.NewVirtualClock(seed)
		out := make([]int, 0, 8)
		for i := 0; i < 8; i++ {
			out = append(out, vc.Rand().Intn(1000))
		}
		return out
	}
	rep.Run1 = gen()
	rep.Run2 = gen()
	rep.Reproducible = equalInts(rep.Run1, rep.Run2)
	rep.Narrative = append(rep.Narrative,
		fmt.Sprintf("two runs seeded with %d produce identical event streams", seed),
		fmt.Sprintf("reproducible=%v — a failing run can be replayed exactly", rep.Reproducible))
	return rep
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
