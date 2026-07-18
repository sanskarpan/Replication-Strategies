package gateway

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"replication-strategies/internal/events"
	"replication-strategies/internal/simulation"
)

// Server is the HTTP gateway that wraps the Orchestrator.
type Server struct {
	orch        *simulation.Orchestrator
	bus         *events.EventBus
	corsOrigins []string // allow-list; empty or containing "*" => allow any
	build       BuildInfo
}

// NewServer creates a new Server. corsOrigins is the allowed CORS origin list from
// config; pass nil/empty for the permissive default.
func NewServer(orch *simulation.Orchestrator, bus *events.EventBus, corsOrigins []string) *Server {
	return &Server{orch: orch, bus: bus, corsOrigins: corsOrigins}
}

// Router builds and returns the HTTP handler tree.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	r.Use(slogMiddleware) // structured (slog) access logs
	r.Use(middleware.Recoverer)
	// Cap request bodies at 1 MiB to prevent unbounded-memory (DoS) via large payloads.
	r.Use(middleware.RequestSize(1 << 20))
	r.Use(s.corsMiddleware)

	// Operational endpoints (top-level, unversioned): liveness, readiness, Prometheus
	// metrics, build version, and self-hosted API docs (Swagger UI + OpenAPI spec).
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/metrics", s.handleMetrics)
	r.Get("/version", s.handleVersion)
	r.Get("/docs", s.DocsHandler())
	r.Get("/openapi.yaml", s.OpenAPIHandler())

	// REST API
	r.Route("/api/v1", func(r chi.Router) {
		// Simulation lifecycle
		r.Post("/simulation/start", s.handleSimulationStart)
		r.Post("/simulation/reset", s.handleSimulationReset)
		r.Get("/simulation/state", s.handleSimulationState)
		r.Get("/simulation/metrics", s.handleSimulationMetrics)

		// Clusters
		r.Post("/clusters", s.handleCreateCluster)
		r.Get("/clusters", s.handleListClusters)
		r.Delete("/clusters/{id}", s.handleDeleteCluster)
		r.Get("/clusters/{id}/state", s.handleClusterState)
		r.Get("/clusters/{id}/convergence", s.handleConvergence)
		r.Get("/clusters/{id}/suspicion", s.handleSuspicion)
		r.Get("/clusters/{id}/placement", s.handlePlacement)
		r.Get("/clusters/{id}/conflicts", s.handleListConflicts)
		r.Post("/clusters/{id}/conflicts/resolve", s.handleResolveConflict)
		r.Patch("/clusters/{id}/config", s.handleClusterConfig)
		// Correctness checkers + anti-entropy
		r.Get("/clusters/{id}/linearizable", s.handleLinearizable)
		r.Get("/clusters/{id}/invariants", s.handleInvariants)
		r.Post("/clusters/{id}/anti-entropy", s.handleAntiEntropy)
		r.Post("/clusters/{id}/reconfigure/add-node", s.handleSafeAddNode)
		r.Get("/clusters/{id}/scenario", s.handleScenarioResult)
		r.Get("/clusters/{id}/cap", s.handleCAP)
		r.Post("/clusters/{id}/challenge", s.handleChallenge)

		// Real-system presets (Cassandra/DynamoDB/Postgres/etcd/Kafka)
		r.Get("/presets", s.handleListPresets)
		r.Post("/presets/{name}/create", s.handleCreateFromPreset)

		// Learning content: DDIA-mapped glossary + guided lessons
		r.Get("/glossary", s.handleGlossary)
		r.Get("/lessons", s.handleLessons)

		// Custom YAML scenarios, report export, and side-by-side strategy race
		r.Post("/scenarios/yaml", s.handleRunScenarioYAML)
		r.Get("/clusters/{id}/export", s.handleExportReport)
		r.Post("/race", s.handleStrategyRace)

		// Writes & reads
		r.Post("/clusters/{id}/write", s.handleWrite)
		r.Get("/clusters/{id}/read", s.handleRead)
		r.Delete("/clusters/{id}/kv", s.handleDelete)
		r.Post("/clusters/{id}/write-batch", s.handleWriteBatch)

		// Nodes
		r.Post("/clusters/{id}/nodes", s.handleAddNode)
		r.Delete("/clusters/{id}/nodes/{nodeId}", s.handleRemoveNode)
		r.Post("/clusters/{id}/nodes/{nodeId}/pause", s.handlePauseNode)
		r.Post("/clusters/{id}/nodes/{nodeId}/resume", s.handleResumeNode)
		r.Post("/clusters/{id}/nodes/{nodeId}/clock-skew", s.handleSetClockSkew)
		r.Get("/clusters/{id}/nodes/{nodeId}/log", s.handleNodeLog)
		r.Get("/clusters/{id}/nodes/{nodeId}/store", s.handleNodeStore)

		// Network fault injection
		r.Post("/clusters/{id}/network/partition", s.handleInjectPartition)
		r.Delete("/clusters/{id}/network/partition/{partId}", s.handleHealPartition)
		r.Post("/clusters/{id}/network/latency", s.handleSetLatency)
		r.Post("/clusters/{id}/network/drop", s.handleSetDrop)
		r.Delete("/clusters/{id}/network/faults", s.handleClearFaults)

		// Consistency guarantee demos
		r.Post("/clusters/{id}/demo/read-your-writes", s.handleDemoRYW)
		r.Post("/clusters/{id}/demo/monotonic-reads", s.handleDemoMonotonic)
		r.Post("/clusters/{id}/demo/consistent-prefix", s.handleDemoConsistentPrefix)

		// Scenarios
		r.Get("/scenarios", s.handleListScenarios)
		r.Post("/scenarios/{name}/run", s.handleRunScenario)

		// Standalone primitive demos (2PC, MVCC, WAL, SWIM, Paxos, deterministic sim)
		r.Get("/demos/2pc", s.handleDemoTwoPC)
		r.Get("/demos/mvcc", s.handleDemoMVCC)
		r.Get("/demos/wal", s.handleDemoWAL)
		r.Get("/demos/swim", s.handleDemoSWIM)
		r.Get("/demos/paxos", s.handleDemoPaxos)
		r.Get("/demos/detsim", s.handleDemoDetSim)
	})

	// WebSocket event stream
	r.Get("/ws", s.handleWebSocket)

	return r
}

// corsMiddleware applies the configured CORS allow-list. When the list is empty (or
// contains "*") it stays permissive; otherwise it echoes only allowed origins.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allow := s.allowedOrigin(r.Header.Get("Origin")); allow != "" {
			w.Header().Set("Access-Control-Allow-Origin", allow)
			if allow != "*" {
				w.Header().Set("Vary", "Origin")
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allowedOrigin returns the value to send in Access-Control-Allow-Origin, or "" if
// the origin is not permitted.
func (s *Server) allowedOrigin(origin string) string {
	if len(s.corsOrigins) == 0 {
		return "*" // permissive default (dev)
	}
	for _, o := range s.corsOrigins {
		if o == "*" {
			return "*"
		}
		if o == origin {
			return origin
		}
	}
	// No match: fall back to the first configured origin so preflights still get a
	// concrete header (the browser will block a mismatched Origin anyway).
	if origin == "" {
		return s.corsOrigins[0]
	}
	return ""
}
