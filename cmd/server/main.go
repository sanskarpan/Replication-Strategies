package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"replication-strategies/gateway"
	"replication-strategies/internal/config"
	"replication-strategies/internal/events"
	"replication-strategies/internal/simulation"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Printf("config load warning (using defaults where needed): %v", err)
	}

	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	orch.SetMaxClusters(cfg.Simulation.MaxClusters)

	srv := gateway.NewServer(orch, bus, cfg.Server.CORSOrigins)
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: srv.Router(),
	}

	go func() {
		log.Printf("Server listening on %s (max_clusters=%d)", addr, cfg.Simulation.MaxClusters)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
	log.Println("Server stopped")
}
