package simulation

import (
	"sync"
	"time"

	"replication-strategies/internal/events"
)

// ScenarioVerdict is the expected-vs-actual outcome of a scenario — the difference
// between "watch something happen" and "understand whether it did what it should".
type ScenarioVerdict struct {
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Passed   bool   `json:"passed"`
}

// ScenarioResult is the narrated timeline + verdict for one scenario run.
type ScenarioResult struct {
	Scenario  string           `json:"scenario"`
	ClusterID string           `json:"cluster_id"`
	Narration []string         `json:"narration"`
	Verdict   *ScenarioVerdict `json:"verdict,omitempty"`
	Done      bool             `json:"done"`
}

// scenarioTracker holds live scenario results keyed by cluster ID.
type scenarioTracker struct {
	mu      sync.Mutex
	results map[string]*ScenarioResult
}

func newScenarioTracker() *scenarioTracker {
	return &scenarioTracker{results: make(map[string]*ScenarioResult)}
}

// begin registers a fresh result for a scenario run.
func (o *Orchestrator) beginScenario(clusterID, name string) {
	o.scenarios.mu.Lock()
	o.scenarios.results[clusterID] = &ScenarioResult{Scenario: name, ClusterID: clusterID}
	o.scenarios.mu.Unlock()
}

// narrate appends a narration step to the scenario timeline and emits it as an event so
// the UI can show the story as it unfolds.
func (o *Orchestrator) narrate(clusterID, text string, highlight ...string) {
	o.scenarios.mu.Lock()
	if r, ok := o.scenarios.results[clusterID]; ok {
		r.Narration = append(r.Narration, text)
	}
	o.scenarios.mu.Unlock()
	o.bus.Publish(events.Event{
		Type:      events.EvtScenarioStep,
		ClusterID: clusterID,
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"narration": text, "highlight": highlight},
	})
}

// verdict records the expected-vs-actual outcome and emits it, closing the scenario.
func (o *Orchestrator) verdict(clusterID, expected, actual string, passed bool) {
	o.scenarios.mu.Lock()
	if r, ok := o.scenarios.results[clusterID]; ok {
		r.Verdict = &ScenarioVerdict{Expected: expected, Actual: actual, Passed: passed}
		r.Done = true
	}
	o.scenarios.mu.Unlock()
	o.bus.Publish(events.Event{
		Type:      events.EvtScenarioVerdict,
		ClusterID: clusterID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"expected": expected, "actual": actual, "passed": passed,
		},
	})
}

// ScenarioResult returns the current narrated result for a cluster's scenario run.
func (o *Orchestrator) ScenarioResult(clusterID string) (*ScenarioResult, bool) {
	o.scenarios.mu.Lock()
	defer o.scenarios.mu.Unlock()
	r, ok := o.scenarios.results[clusterID]
	if !ok {
		return nil, false
	}
	// Return a copy so callers can't mutate the live result.
	cp := *r
	cp.Narration = append([]string{}, r.Narration...)
	if r.Verdict != nil {
		v := *r.Verdict
		cp.Verdict = &v
	}
	return &cp, true
}
