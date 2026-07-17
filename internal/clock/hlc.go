// Package clock implements a Hybrid Logical Clock (HLC).
//
// An HLC combines physical (wall-clock) time with a bounded logical counter so
// that timestamps are both close to real time and strictly ordered with respect
// to causality. Timestamps are encoded into a single int64 as:
//
//	(physicalMillis << 16) | (logical & 0xFFFF)
//
// The low 16 bits hold the logical counter and the remaining high bits hold the
// physical time in milliseconds. This layout keeps timestamps comparable with a
// plain integer compare while allowing up to 65536 events within a single
// millisecond before the physical component must advance.
package clock

import (
	"sync"
	"time"
)

// logicalBits is the number of low-order bits reserved for the logical counter.
const logicalBits = 16

// logicalMask masks off the logical portion of an encoded timestamp.
const logicalMask int64 = (1 << logicalBits) - 1

// HLC is a thread-safe Hybrid Logical Clock.
//
// The zero value is not usable; construct instances with NewHLC.
type HLC struct {
	mu sync.Mutex

	// now is the physical-time source. It is a field so tests (and skew
	// injection) can reason about it; it defaults to time.Now.
	now func() time.Time

	// lastPhysical is the most recent physical-millis component emitted.
	lastPhysical int64

	// logical is the current logical counter (0..0xFFFF).
	logical int64

	// skewNanos is an artificial offset (in nanoseconds) added to the wall
	// clock, used for skew-injection demos. May be negative.
	skewNanos int64
}

// NewHLC returns a new HLC backed by the system wall clock.
func NewHLC() *HLC {
	return &HLC{
		now: time.Now,
	}
}

// wallPhysical returns the current wall-derived physical time in milliseconds,
// including any injected skew. Callers must hold h.mu.
func (h *HLC) wallPhysical() int64 {
	return (h.now().UnixNano() + h.skewNanos) / 1e6
}

// encode packs a physical-millis and logical counter into a single timestamp.
func encode(physical, logical int64) int64 {
	return (physical << logicalBits) | (logical & logicalMask)
}

// Now returns a monotonically non-decreasing encoded timestamp.
//
// On each call, if the wall-derived physical time has advanced past the last
// emitted physical time, the logical counter resets to 0. Otherwise the physical
// component is held at its previous value and the logical counter is incremented
// to preserve strict monotonicity within the same (or a lagging) millisecond.
func (h *HLC) Now() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	wall := h.wallPhysical()
	if wall > h.lastPhysical {
		h.lastPhysical = wall
		h.logical = 0
	} else {
		// Wall did not advance beyond what we've already emitted; keep the
		// physical component and bump logical to stay strictly increasing.
		h.logical++
		// Carry into the physical component if the logical counter overflows
		// its 16-bit budget, preserving strict monotonicity.
		for h.logical > logicalMask {
			h.lastPhysical++
			h.logical -= (logicalMask + 1)
		}
	}
	return encode(h.lastPhysical, h.logical)
}

// Update merges a received remote timestamp into this clock following the
// standard HLC update rule and returns the resulting encoded Now.
//
// The new physical component is the maximum of the local physical time, the
// remote physical time, and the current wall-derived physical time. The logical
// counter is chosen so that the returned timestamp is strictly greater than both
// the prior local timestamp and the supplied remote timestamp.
func (h *HLC) Update(remote int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	remotePhysical := remote >> logicalBits
	remoteLogical := remote & logicalMask

	wall := h.wallPhysical()

	// Highest physical component among wall, local, and remote.
	newPhysical := h.lastPhysical
	if wall > newPhysical {
		newPhysical = wall
	}
	if remotePhysical > newPhysical {
		newPhysical = remotePhysical
	}

	switch {
	case newPhysical == h.lastPhysical && newPhysical == remotePhysical:
		// Local and remote share the winning physical time: take the larger
		// logical and advance past it.
		if remoteLogical > h.logical {
			h.logical = remoteLogical
		}
		h.logical++
	case newPhysical == h.lastPhysical:
		// Local physical wins (>= remote and >= wall): keep advancing locally.
		h.logical++
	case newPhysical == remotePhysical:
		// Remote physical wins: adopt remote logical and advance past it.
		h.logical = remoteLogical + 1
	default:
		// Wall physical strictly dominates: fresh millisecond, reset logical.
		h.logical = 0
	}

	// If the logical counter overflowed its 16-bit budget, carry into the
	// physical component so the encoded timestamp still strictly increases.
	for h.logical > logicalMask {
		newPhysical++
		h.logical -= (logicalMask + 1)
	}

	h.lastPhysical = newPhysical
	return encode(h.lastPhysical, h.logical)
}

// SetSkewMillis sets an artificial clock offset in milliseconds. The offset is
// added to the wall clock on every subsequent read. Negative values are allowed
// (simulating a lagging clock). Monotonicity of Now is preserved regardless of
// skew because Now never lowers the physical component below what it has already
// emitted.
func (h *HLC) SetSkewMillis(ms int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skewNanos = ms * 1e6
}

// PhysicalMillis decodes the physical-time (milliseconds) component of an
// encoded HLC timestamp.
func PhysicalMillis(ts int64) int64 {
	return ts >> logicalBits
}

// Logical decodes the logical-counter component of an encoded HLC timestamp.
func Logical(ts int64) uint64 {
	return uint64(ts & logicalMask)
}
