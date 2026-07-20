package simulation

// This file holds the pedagogical content layer: a glossary mapped to DDIA chapters and
// guided lessons (predict-then-reveal) built on the existing scenario engine. The content
// is served as JSON so the UI can render tooltips, a glossary panel, and lesson flows.

// GlossaryTerm defines a distributed-systems term and where to read more (DDIA chapter).
type GlossaryTerm struct {
	Term       string `json:"term"`
	Definition string `json:"definition"`
	DDIA       string `json:"ddia"` // Designing Data-Intensive Applications chapter reference
}

// Glossary is the built-in term catalog.
var Glossary = []GlossaryTerm{
	{"Replication lag", "The delay between a write committing on the leader and a follower applying it. Async replication trades durability/consistency for lower write latency.", "DDIA Ch.5 — Leaders and Followers"},
	{"Quorum (W+R>N)", "A read and write quorum overlap when W+R>N, guaranteeing a read sees the latest acknowledged write. Otherwise stale reads are possible.", "DDIA Ch.5 — Quorums for reading and writing"},
	{"Sloppy quorum", "During a partition, writes go to the first N healthy nodes on the ring (not the 'home' replicas), with hinted handoff delivering them home on recovery — availability over strict placement.", "DDIA Ch.5 — Sloppy Quorums and Hinted Handoff"},
	{"Read repair", "When a quorum read sees a stale replica, the coordinator writes the fresh value back to it. Async keeps latency low; sync blocks for a stronger guarantee.", "DDIA Ch.5 — Read repair and anti-entropy"},
	{"Anti-entropy", "A background process that compares replicas (often via Merkle trees) and copies missing/newer data, so replicas converge even without reads.", "DDIA Ch.5 — Read repair and anti-entropy"},
	{"Vector clock", "A per-node counter map capturing causal history, so concurrent writes (neither happens-before the other) can be detected instead of silently lost.", "DDIA Ch.5 — Detecting Concurrent Writes"},
	{"Hybrid Logical Clock", "Combines physical time with a logical counter to give a single monotonic, causality-respecting timestamp — fixing LWW under clock skew.", "DDIA Ch.8 — Unreliable Clocks"},
	{"Last-Write-Wins", "A conflict resolution that keeps the write with the highest timestamp. Simple but silently discards concurrent writes and is fragile under clock skew.", "DDIA Ch.5 — Last write wins"},
	{"CRDT", "A Conflict-free Replicated Data Type whose merge is commutative, associative, and idempotent, so replicas converge without coordination.", "DDIA Ch.5 — Custom conflict resolution"},
	{"Linearizability", "The strongest single-object consistency: operations appear to take effect atomically at some point between invocation and completion, consistent with real time.", "DDIA Ch.9 — Linearizability"},
	{"Raft", "A consensus algorithm with a strong leader, replicated log, and majority commit; provides linearizable operations and automatic failover.", "DDIA Ch.9 — Consensus"},
	{"CAP / PACELC", "Under a Partition, choose Consistency or Availability; Else (normal operation) trade Latency vs Consistency. PACELC extends CAP with the no-partition case.", "DDIA Ch.9 — The cost of linearizability"},
}

// LessonStep is one predict-then-reveal beat of a guided lesson.
type LessonStep struct {
	Prompt string `json:"prompt"` // what to predict before running
	Reveal string `json:"reveal"` // the explanation shown after
}

// Lesson is a guided, scenario-backed walkthrough.
type Lesson struct {
	Title    string       `json:"title"`
	Summary  string       `json:"summary"`
	Scenario string       `json:"scenario"` // a Scenarios[] name to run alongside
	Steps    []LessonStep `json:"steps"`
}

// Lessons is the built-in guided-lesson catalog (predict-then-reveal).
var Lessons = []Lesson{
	{
		Title:    "Why async replication can lose your write",
		Summary:  "Watch a follower lag behind the leader and reason about the durability tradeoff.",
		Scenario: "ReplicationLag",
		Steps: []LessonStep{
			{"If the leader acks a write and then crashes before a follower copies it, is the write safe?", "No — with async replication the ack does not mean the write is durable on any follower. A leader crash can lose acknowledged writes. Sync replication avoids this at the cost of write latency."},
			{"Which follower will be furthest behind?", "The one with the highest replication latency — here the link with 500ms delay. Its lag is visible in the lag panel."},
		},
	},
	{
		Title:    "Quorum overlap and stale reads",
		Summary:  "Tune N/W/R and predict when a read can miss the latest write.",
		Scenario: "QuorumTuning",
		Steps: []LessonStep{
			{"With N=5, W=1, R=1, can a read miss the latest write?", "Yes. W+R=2 ≤ N=5, so the read set may not overlap the write set — stale reads are possible. The stale-read probability panel shows it is > 0."},
			{"What is the smallest W=R that guarantees fresh reads at N=5?", "W=R=3, because 3+3=6 > 5. Now every read set overlaps every write set by at least one node."},
		},
	},
	{
		Title:    "How a CRDT beats last-write-wins",
		Summary:  "Concurrent writes under a partition: LWW discards one, a CRDT keeps both.",
		Scenario: "MultiLeaderConflict",
		Steps: []LessonStep{
			{"Two nodes write different values to the same key while partitioned. Under LWW, what happens on heal?", "One write silently wins by timestamp and the other is lost — a lost update."},
			{"Under a CRDT (e.g. OR-Set / counter), what happens instead?", "The merge combines both writes deterministically (union / element-wise max), so no update is lost and all replicas converge to the same value."},
		},
	},
}

// ListGlossary returns a copy of the glossary.
func ListGlossary() []GlossaryTerm { return append([]GlossaryTerm{}, Glossary...) }

// ListLessons returns a copy of the lessons.
func ListLessons() []Lesson { return append([]Lesson{}, Lessons...) }
