package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// BuildInfo carries version metadata surfaced by /version and logs.
type BuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// SetBuildInfo records the binary's build metadata (called from main).
func (s *Server) SetBuildInfo(version, commit, date string) {
	s.build = BuildInfo{Version: version, Commit: commit, Date: date}
}

// handleHealthz is a liveness probe: the process is up and serving.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is a readiness probe: the orchestrator is initialised and can serve.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil {
		writeError(w, http.StatusServiceUnavailable, "orchestrator not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "ready",
		"clusters": len(s.orch.ListClusters()),
	})
}

// handleVersion returns build metadata.
func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.build)
}

// handleMetrics exposes the in-app metrics in Prometheus text exposition format so a
// Prometheus scraper / Grafana can chart them, without pulling in the client library.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	help := func(name, typ, doc string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, doc, name, typ)
	}
	clusters := s.orch.ListClusters()

	help("replsim_clusters", "gauge", "Number of live clusters.")
	fmt.Fprintf(&b, "replsim_clusters %d\n", len(clusters))

	help("replsim_writes_total", "counter", "Total client writes per cluster.")
	help("replsim_reads_total", "counter", "Total client reads per cluster.")
	help("replsim_conflicts_total", "counter", "Total detected conflicts per cluster.")
	help("replsim_dropped_messages", "counter", "Back-pressure message drops per cluster.")
	help("replsim_node_replica_lag", "gauge", "Replica lag in entries per node.")
	help("replsim_node_online", "gauge", "1 if the node is online, else 0.")
	help("replsim_node_leader", "gauge", "1 if the node is the leader, else 0.")
	help("replsim_node_write_latency_ms", "gauge", "Write latency percentiles (ms) per node.")
	help("replsim_node_read_latency_ms", "gauge", "Read latency percentiles (ms) per node.")

	for _, c := range clusters {
		snap := c.Metrics.Snapshot()
		cl := escapeLabel(c.ID)
		strat := escapeLabel(snap.Strategy)
		fmt.Fprintf(&b, "replsim_writes_total{cluster=%q,strategy=%q} %d\n", cl, strat, snap.TotalWrites)
		fmt.Fprintf(&b, "replsim_reads_total{cluster=%q,strategy=%q} %d\n", cl, strat, snap.TotalReads)
		fmt.Fprintf(&b, "replsim_conflicts_total{cluster=%q,strategy=%q} %d\n", cl, strat, snap.TotalConflicts)
		fmt.Fprintf(&b, "replsim_dropped_messages{cluster=%q} %d\n", cl, c.GetState().DroppedMessages)
		for id, nm := range snap.NodeMetrics {
			node := escapeLabel(id)
			fmt.Fprintf(&b, "replsim_node_replica_lag{cluster=%q,node=%q} %d\n", cl, node, nm.ReplicaLag)
			fmt.Fprintf(&b, "replsim_node_online{cluster=%q,node=%q} %d\n", cl, node, b2i(nm.IsOnline))
			fmt.Fprintf(&b, "replsim_node_leader{cluster=%q,node=%q} %d\n", cl, node, b2i(nm.IsLeader))
			fmt.Fprintf(&b, "replsim_node_write_latency_ms{cluster=%q,node=%q,quantile=\"0.5\"} %g\n", cl, node, nm.WriteP50)
			fmt.Fprintf(&b, "replsim_node_write_latency_ms{cluster=%q,node=%q,quantile=\"0.95\"} %g\n", cl, node, nm.WriteP95)
			fmt.Fprintf(&b, "replsim_node_write_latency_ms{cluster=%q,node=%q,quantile=\"0.99\"} %g\n", cl, node, nm.WriteP99)
			fmt.Fprintf(&b, "replsim_node_read_latency_ms{cluster=%q,node=%q,quantile=\"0.5\"} %g\n", cl, node, nm.ReadP50)
			fmt.Fprintf(&b, "replsim_node_read_latency_ms{cluster=%q,node=%q,quantile=\"0.95\"} %g\n", cl, node, nm.ReadP95)
			fmt.Fprintf(&b, "replsim_node_read_latency_ms{cluster=%q,node=%q,quantile=\"0.99\"} %g\n", cl, node, nm.ReadP99)
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}

func b2i(v bool) int {
	if v {
		return 1
	}
	return 0
}

// escapeLabel escapes a Prometheus label value (backslash, quote, newline).
func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// slogMiddleware logs each request with structured fields via log/slog.
func slogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		slog.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"bytes", sw.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

// statusWriter captures the response status + byte count for structured logging.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}
