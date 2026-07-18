// Package simclock provides a deterministic Clock seam together with a seeded
// pseudo-random number generator so that simulations are fully reproducible.
//
// Two implementations are offered:
//
//   - RealClock drives time from the wall clock and is intended for production
//     or manual runs. Its randomness is still seeded, so a run can be repeated
//     by reusing the same seed even though timing itself is not controlled.
//   - VirtualClock is a fully deterministic clock backed by a logical
//     nanosecond counter and a min-heap scheduler. Time only moves when
//     Advance is called, at which point any timers whose deadline has been
//     crossed fire in deadline order. Combined with a seeded RNG, this makes an
//     entire simulation (message drops, election timeouts, ...) reproducible
//     from a single seed.
package simclock

import (
	"container/heap"
	"math/rand"
	"sync"
	"time"
)

// Clock is the seam through which simulation code reads time, sleeps, and draws
// randomness. Swapping RealClock for VirtualClock turns a live run into a
// deterministic, replayable one without touching call sites.
type Clock interface {
	// Now returns the current time as Unix nanoseconds. For VirtualClock this
	// is the logical time; for RealClock it is the wall clock.
	Now() int64

	// Sleep blocks until the clock has advanced by at least d.
	Sleep(d time.Duration)

	// After returns a channel that receives the (logical or wall) time once the
	// clock has advanced by at least d.
	After(d time.Duration) <-chan time.Time

	// Rand returns the seeded random source. All simulation randomness should be
	// drawn from here so that it is reproducible from the seed.
	Rand() *rand.Rand
}

// RealClock implements Clock over the real wall clock while still sourcing its
// randomness from a single seeded RNG constructed at build time.
type RealClock struct {
	rng *rand.Rand
	mu  sync.Mutex // guards rng: *rand.Rand is not safe for concurrent use
}

// NewRealClock returns a RealClock whose RNG is seeded with seed.
func NewRealClock(seed int64) *RealClock {
	return &RealClock{rng: rand.New(rand.NewSource(seed))}
}

// Now returns the current wall-clock time in Unix nanoseconds.
func (c *RealClock) Now() int64 { return time.Now().UnixNano() }

// Sleep blocks the calling goroutine for d using the real scheduler.
func (c *RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// After delegates to time.After.
func (c *RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Rand returns the seeded RNG. The returned *rand.Rand is shared, so callers on
// multiple goroutines must serialise access themselves; the guard here only
// protects the pointer handoff.
func (c *RealClock) Rand() *rand.Rand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rng
}

// virtualTimer is a single pending Sleep/After registration on a VirtualClock.
type virtualTimer struct {
	deadline int64          // logical Unix-nanosecond deadline
	seq      uint64         // tie-breaker giving a stable, deterministic order
	ch       chan time.Time // buffered (cap 1) delivery channel
	index    int            // heap index maintained by container/heap
}

// timerHeap is a min-heap of pending timers ordered by (deadline, seq). The seq
// tie-breaker makes firing order deterministic when two timers share a
// deadline, which is what the determinism guarantee relies on.
type timerHeap []*virtualTimer

func (h timerHeap) Len() int { return len(h) }

func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	return h[i].seq < h[j].seq
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(x any) {
	t := x.(*virtualTimer)
	t.index = len(*h)
	*h = append(*h, t)
}

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}

// VirtualClock is a fully deterministic clock driven by a logical nanosecond
// counter. Time never moves on its own: Advance moves it forward and fires any
// timers whose deadline has been crossed, in deadline order. A seeded RNG backs
// Rand so that all randomness replays identically from the seed.
//
// All shared state is guarded by mu, so VirtualClock is safe for concurrent
// use; note however that Sleep parks the caller until some other goroutine
// calls Advance far enough.
type VirtualClock struct {
	mu     sync.Mutex
	now    int64 // current logical time, Unix nanoseconds
	timers timerHeap
	seq    uint64 // monotonically increasing timer sequence counter
	rng    *rand.Rand
}

// NewVirtualClock returns a VirtualClock starting at logical time 0 with its RNG
// seeded by seed.
func NewVirtualClock(seed int64) *VirtualClock {
	c := &VirtualClock{rng: rand.New(rand.NewSource(seed))}
	heap.Init(&c.timers)
	return c
}

// NewVirtualClockAt is like NewVirtualClock but starts the logical clock at
// start (Unix nanoseconds).
func NewVirtualClockAt(seed, start int64) *VirtualClock {
	c := NewVirtualClock(seed)
	c.now = start
	return c
}

// Now returns the current logical time in Unix nanoseconds.
func (c *VirtualClock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Rand returns the seeded RNG. It is guarded by the same mutex as the rest of
// the clock state so concurrent draws are serialised deterministically.
func (c *VirtualClock) Rand() *rand.Rand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rng
}

// After registers a timer that fires once logical time has advanced by at least
// d. A non-positive d fires on the next Advance-driven check; to keep the
// contract that time only moves via Advance, the channel is delivered the
// moment Advance crosses (or has already crossed) the deadline. The returned
// channel is buffered so a fire never blocks.
func (c *VirtualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.after(d)
}

// after registers a timer; the caller must hold c.mu.
func (c *VirtualClock) after(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	deadline := c.now + int64(d)
	t := &virtualTimer{deadline: deadline, seq: c.seq, ch: ch}
	c.seq++
	// If the deadline is already in the past (e.g. d <= 0), fire immediately.
	if deadline <= c.now {
		ch <- time.Unix(0, c.now)
		close(ch)
		return ch
	}
	heap.Push(&c.timers, t)
	return ch
}

// Sleep blocks until logical time has advanced by at least d. It is implemented
// on top of After, so it too only unblocks when some goroutine calls Advance
// past the deadline. Sleeping for a non-positive duration returns immediately.
func (c *VirtualClock) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	ch := c.after(d)
	c.mu.Unlock()
	<-ch
}

// Advance moves logical time forward by d and fires every timer whose deadline
// is now at or before the new logical time, in (deadline, registration-order)
// order. A non-positive d is a no-op. Advance returns the number of timers
// fired, which is handy in tests.
func (c *VirtualClock) Advance(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now += int64(d)
	fired := 0
	for c.timers.Len() > 0 && c.timers[0].deadline <= c.now {
		t := heap.Pop(&c.timers).(*virtualTimer)
		t.ch <- time.Unix(0, t.deadline)
		close(t.ch)
		fired++
	}
	return fired
}

// Pending reports how many timers are still waiting to fire. It exists mainly
// for tests asserting that not-yet-due timers stay pending.
func (c *VirtualClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers.Len()
}

// Compile-time assertions that both clocks satisfy Clock.
var (
	_ Clock = (*RealClock)(nil)
	_ Clock = (*VirtualClock)(nil)
)
