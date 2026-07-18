// Package swim implements the state-machine core of the SWIM gossip-based
// failure detector (Scalable Weakly-consistent Infection-style process group
// Membership). This is logic only: there is no real network, timers, or I/O.
// Callers drive it by invoking the transition methods, and dissemination is
// modelled by exchanging member lists through Merge.
//
// Membership state is reconciled using incarnation numbers. Each member owns a
// monotonically increasing incarnation counter that it bumps to refute stale
// gossip about itself. The reconciliation rule is:
//
//   - A higher incarnation always wins and can override Suspect or (revive from
//     the perspective of a fresh join) an older belief.
//   - For an equal incarnation, the "worse" state wins with the precedence
//     Dead > Suspect > Alive.
//   - Dead is terminal in this model: once a member is Dead it stays Dead
//     regardless of later gossip.
package swim

import (
	"sort"
	"sync"
)

// MemberState is the believed liveness of a member.
type MemberState int

const (
	// Alive means the member is believed healthy.
	Alive MemberState = iota
	// Suspect means the member is suspected of having failed, pending
	// confirmation or refutation.
	Suspect
	// Dead means the member is confirmed failed. Terminal in this model.
	Dead
)

// String renders a MemberState for diagnostics.
func (s MemberState) String() string {
	switch s {
	case Alive:
		return "Alive"
	case Suspect:
		return "Suspect"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// precedence orders states for the equal-incarnation conflict rule:
// Dead > Suspect > Alive.
func precedence(s MemberState) int {
	switch s {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Dead:
		return 2
	default:
		return -1
	}
}

// Member is a snapshot of one member's state.
type Member struct {
	ID          string
	State       MemberState
	Incarnation uint64

	// suspectedAt records the deadline-clock value at which this member
	// entered the Suspect state, used by TickSuspicions to promote stale
	// suspicions to Dead. It is unexported so it does not travel in gossip
	// snapshots returned to callers.
	suspectedAt int64
}

// Membership is a deterministic SWIM state machine for a single node's view of
// the cluster. All methods are safe for concurrent use.
type Membership struct {
	selfID  string
	members map[string]*Member
	mu      sync.Mutex
}

// NewMembership creates a Membership whose local node is selfID. The self
// member starts Alive at incarnation 0.
func NewMembership(selfID string) *Membership {
	m := &Membership{
		selfID:  selfID,
		members: make(map[string]*Member),
	}
	m.members[selfID] = &Member{ID: selfID, State: Alive, Incarnation: 0}
	return m
}

// SelfID returns the local node's ID.
func (m *Membership) SelfID() string { return m.selfID }

// SelfIncarnation returns the local node's current incarnation number.
func (m *Membership) SelfIncarnation() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.members[m.selfID].Incarnation
}

// Get returns a copy of the member with the given ID and whether it is known.
func (m *Membership) Get(id string) (Member, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mem, ok := m.members[id]
	if !ok {
		return Member{}, false
	}
	return *mem, true
}

// Alive (re)marks id as Alive if the incoming incarnation is at least the known
// one. A strictly higher incarnation overrides Suspect (refutation) but never
// revives a Dead member, which is terminal in this model.
func (m *Membership) Alive(id string, incarnation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.members[id]
	if !ok {
		m.members[id] = &Member{ID: id, State: Alive, Incarnation: incarnation}
		return
	}

	// Dead is terminal.
	if cur.State == Dead {
		return
	}

	// Stale gossip about a member we already believe at a higher incarnation.
	if incarnation < cur.Incarnation {
		return
	}

	// Equal incarnation cannot downgrade Suspect back to Alive; only a strictly
	// higher incarnation refutes a suspicion.
	if incarnation == cur.Incarnation && cur.State == Suspect {
		return
	}

	cur.State = Alive
	cur.Incarnation = incarnation
	cur.suspectedAt = 0
}

// Suspect marks id as Suspect if the incoming incarnation is at least the known
// one and the member is not already Dead. Suspecting the local node is refuted
// automatically by bumping the self incarnation and re-asserting Alive.
//
// now is the deadline-clock value recorded as the suspicion's start time and is
// later compared against the deadline passed to TickSuspicions.
func (m *Membership) Suspect(id string, incarnation uint64, now int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id == m.selfID {
		m.refuteSuspicionLocked()
		return
	}

	cur, ok := m.members[id]
	if !ok {
		mem := &Member{ID: id, State: Suspect, Incarnation: incarnation, suspectedAt: now}
		m.members[id] = mem
		return
	}

	if cur.State == Dead {
		return
	}
	if incarnation < cur.Incarnation {
		return
	}

	// If we already suspect this member at this incarnation, keep the original
	// suspicion start time so the timeout is measured from the first suspicion.
	if cur.State == Suspect && incarnation == cur.Incarnation {
		return
	}

	cur.State = Suspect
	cur.Incarnation = incarnation
	cur.suspectedAt = now
}

// RefuteSuspicion increments the local node's incarnation and re-asserts it as
// Alive, the SWIM mechanism by which a node refutes a suspicion about itself.
func (m *Membership) RefuteSuspicion() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refuteSuspicionLocked()
}

func (m *Membership) refuteSuspicionLocked() {
	self := m.members[m.selfID]
	self.Incarnation++
	self.State = Alive
	self.suspectedAt = 0
}

// Dead confirms id as failed. Dead is terminal; the member's incarnation is
// left unchanged so later equal-or-lower gossip cannot revive it.
func (m *Membership) Dead(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.members[id]
	if !ok {
		m.members[id] = &Member{ID: id, State: Dead}
		return
	}
	cur.State = Dead
	cur.suspectedAt = 0
}

// Confirm is an alias for Dead, reflecting the SWIM "confirmation" terminology.
func (m *Membership) Confirm(id string) { m.Dead(id) }

// TickSuspicions promotes every Suspect whose suspicion started strictly before
// deadline to Dead. It models the suspicion timeout: a member suspected long
// enough without refutation is confirmed failed. It returns the IDs promoted,
// sorted.
func (m *Membership) TickSuspicions(deadline int64) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var promoted []string
	for _, mem := range m.members {
		if mem.State == Suspect && mem.suspectedAt < deadline {
			mem.State = Dead
			mem.suspectedAt = 0
			promoted = append(promoted, mem.ID)
		}
	}
	sort.Strings(promoted)
	return promoted
}

// Merge folds a peer's gossiped member list into the local view using the
// incarnation-number conflict rule (higher incarnation wins; for equal
// incarnation, Dead > Suspect > Alive). The local self member is never
// downgraded by a peer: a suspicion or death claim about self at the current
// incarnation is refuted by bumping the self incarnation.
func (m *Membership) Merge(gossip []Member) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, g := range gossip {
		if g.ID == m.selfID {
			m.mergeSelfLocked(g)
			continue
		}

		cur, ok := m.members[g.ID]
		if !ok {
			m.members[g.ID] = &Member{ID: g.ID, State: g.State, Incarnation: g.Incarnation}
			continue
		}

		// Dead is terminal locally.
		if cur.State == Dead {
			continue
		}

		if m.gossipWinsLocked(cur, g) {
			cur.State = g.State
			cur.Incarnation = g.Incarnation
			// Reset the suspicion clock: any timing must be re-established by a
			// local Suspect call, since gossip carries no local deadline.
			cur.suspectedAt = 0
		}
	}
}

// gossipWinsLocked reports whether gossip g should override the current member
// cur under the incarnation-number conflict rule.
func (m *Membership) gossipWinsLocked(cur *Member, g Member) bool {
	if g.Incarnation > cur.Incarnation {
		return true
	}
	if g.Incarnation < cur.Incarnation {
		return false
	}
	// Equal incarnation: the higher-precedence (worse) state wins.
	return precedence(g.State) > precedence(cur.State)
}

// mergeSelfLocked applies gossip about the local node. Any Suspect or Dead
// claim about self at our current-or-lower incarnation is refuted by bumping
// our incarnation and re-asserting Alive.
func (m *Membership) mergeSelfLocked(g Member) {
	self := m.members[m.selfID]

	if g.State == Alive {
		if g.Incarnation > self.Incarnation {
			self.Incarnation = g.Incarnation
		}
		return
	}

	// A Suspect/Dead claim about self. If it is based on an incarnation we have
	// already superseded, ignore it; otherwise refute by out-incarnating it.
	if g.Incarnation < self.Incarnation {
		return
	}
	self.Incarnation = g.Incarnation + 1
	self.State = Alive
	self.suspectedAt = 0
}

// membersByState returns the sorted IDs of members in the given state.
func (m *Membership) membersByState(state MemberState) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ids []string
	for id, mem := range m.members {
		if mem.State == state {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// AliveMembers returns the sorted IDs of members believed Alive.
func (m *Membership) AliveMembers() []string { return m.membersByState(Alive) }

// SuspectMembers returns the sorted IDs of members believed Suspect.
func (m *Membership) SuspectMembers() []string { return m.membersByState(Suspect) }

// DeadMembers returns the sorted IDs of members believed Dead.
func (m *Membership) DeadMembers() []string { return m.membersByState(Dead) }

// Snapshot returns a copy of every known member, sorted by ID, suitable for
// gossiping to a peer via Merge.
func (m *Membership) Snapshot() []Member {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, Member{ID: mem.ID, State: mem.State, Incarnation: mem.Incarnation})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
