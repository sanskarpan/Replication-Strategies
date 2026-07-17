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
	orch *simulation.Orchestrator
	bus  *events.EventBus
}

// NewServer creates a new Server.
func NewServer(orch *simulation.Orchestrator, bus *events.EventBus) *Server {
	return &Server{orch: orch, bus: bus}
}

// Router builds and returns the HTTP handler tree.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	// Cap request bodies at 1 MiB to prevent unbounded-memory (DoS) via large payloads.
	r.Use(middleware.RequestSize(1 << 20))
	r.Use(corsMiddleware)

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
		r.Patch("/clusters/{id}/config", s.handleClusterConfig)

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
	})

	// WebSocket event stream
	r.Get("/ws", s.handleWebSocket)

	return r
}

// corsMiddleware adds permissive CORS headers for development use.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
