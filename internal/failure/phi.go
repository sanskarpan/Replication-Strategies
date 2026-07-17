// Package failure implements a phi-accrual failure detector in the style of
// Cassandra and Akka. Instead of producing a boolean "up/down" verdict, the
// detector emits a continuously-valued suspicion level (phi) derived from the
// statistical distribution of observed heartbeat inter-arrival times. Callers
// choose their own threshold to trade off between detection speed and the risk
// of false positives.
package failure

import (
	"math"
	"sync"
)

// maxSamples bounds the sliding window of inter-arrival intervals retained per
// node. Older samples are evicted so the detector adapts to changing network
// conditions rather than being anchored to ancient history.
const maxSamples = 100

// minStdDevMillis is a floor applied to the sample standard deviation. Without
// it, a run of perfectly regular heartbeats would drive the variance to zero
// and make phi explode (or divide-by-zero) the instant an interval deviates
// even slightly. This mirrors the guard used in Akka's implementation.
const minStdDevMillis = 1.0

// nodeState holds the per-node heartbeat history.
type nodeState struct {
	// intervals is the sliding window of inter-arrival gaps in milliseconds.
	intervals []float64
	// lastArrival is the timestamp (millis) of the most recent heartbeat.
	lastArrival int64
	// seen reports whether at least one heartbeat has been recorded, so we can
	// distinguish "never heard from" from "arrived at time 0".
	seen bool
}

// Detector is a thread-safe phi-accrual failure detector tracking many nodes.
type Detector struct {
	mu    sync.Mutex
	nodes map[string]*nodeState
}

// NewDetector returns an empty Detector ready to accept heartbeats.
func NewDetector() *Detector {
	return &Detector{
		nodes: make(map[string]*nodeState),
	}
}

// Heartbeat records the arrival of a heartbeat from nodeID at nowMillis. The
// inter-arrival gap since the previous heartbeat is appended to the node's
// sliding window (the very first heartbeat only establishes a baseline and
// contributes no interval).
func (d *Detector) Heartbeat(nodeID string, nowMillis int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ns, ok := d.nodes[nodeID]
	if !ok {
		ns = &nodeState{}
		d.nodes[nodeID] = ns
	}

	if ns.seen {
		gap := float64(nowMillis - ns.lastArrival)
		if gap < 0 {
			gap = 0
		}
		ns.intervals = append(ns.intervals, gap)
		if len(ns.intervals) > maxSamples {
			// Drop the oldest sample to keep the window bounded.
			ns.intervals = ns.intervals[len(ns.intervals)-maxSamples:]
		}
	}

	ns.lastArrival = nowMillis
	ns.seen = true
}

// Phi returns the current suspicion level for nodeID at nowMillis.
//
// It fits a normal distribution to the observed inter-arrival intervals (using
// their sample mean and standard deviation) and computes P(later): the
// probability that the next heartbeat arrives later than the currently elapsed
// time. The returned value is phi = -log10(P(later)); larger phi means the
// node is more likely to have failed.
//
// If no interval history exists yet, Phi returns 0.
func (d *Detector) Phi(nodeID string, nowMillis int64) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	ns, ok := d.nodes[nodeID]
	if !ok || !ns.seen || len(ns.intervals) == 0 {
		return 0
	}

	mean, stdDev := meanStdDev(ns.intervals)
	if stdDev < minStdDevMillis {
		stdDev = minStdDevMillis
	}

	elapsed := float64(nowMillis - ns.lastArrival)
	if elapsed < 0 {
		elapsed = 0
	}

	// P(later) is the survival function of the fitted normal at `elapsed`:
	// the probability the next heartbeat still hasn't been superseded.
	pLater := survival(elapsed, mean, stdDev)

	// Clamp to avoid -Inf / NaN when pLater underflows to zero far in the tail.
	if pLater < math.SmallestNonzeroFloat64 {
		pLater = math.SmallestNonzeroFloat64
	}

	return -math.Log10(pLater)
}

// Suspected reports whether nodeID's current phi meets or exceeds threshold.
func (d *Detector) Suspected(nodeID string, nowMillis int64, threshold float64) bool {
	return d.Phi(nodeID, nowMillis) >= threshold
}

// Reset clears all history for nodeID, e.g. after the node is observed to
// recover. Subsequent Phi calls return 0 until new heartbeats accumulate.
func (d *Detector) Reset(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.nodes, nodeID)
}

// meanStdDev computes the sample mean and (population) standard deviation of
// the provided samples. Callers guarantee len(samples) > 0.
func meanStdDev(samples []float64) (mean, stdDev float64) {
	n := float64(len(samples))
	var sum float64
	for _, v := range samples {
		sum += v
	}
	mean = sum / n

	var variance float64
	for _, v := range samples {
		diff := v - mean
		variance += diff * diff
	}
	variance /= n
	stdDev = math.Sqrt(variance)
	return mean, stdDev
}

// survival returns P(X > x) for a normal distribution N(mean, stdDev^2), i.e.
// the complementary CDF. This is the probability that an inter-arrival drawn
// from the fitted distribution would exceed the elapsed time x.
func survival(x, mean, stdDev float64) float64 {
	z := (x - mean) / stdDev
	// Survival of the standard normal: 0.5 * erfc(z / sqrt(2)).
	return 0.5 * math.Erfc(z/math.Sqrt2)
}
