package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on the default mux (guarded below)
	"os"
	"os/signal"
	"syscall"
	"time"

	"replication-strategies/gateway"
	"replication-strategies/internal/config"
	"replication-strategies/internal/events"
	"replication-strategies/internal/simulation"
	"replication-strategies/internal/telemetry"
)

// Build metadata, overridable at link time:
//
//	go build -ldflags "-X main.version=v1.2.3 -X main.commit=abc -X main.date=..."
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()
	if *showVersion {
		fmt.Printf("replsim %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	// Structured JSON logging via log/slog; level from LOG_LEVEL (default info).
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(logger)

	// OpenTelemetry tracing — no-op when OTEL_ENABLED != "true".
	otelShutdown, err := telemetry.Init(context.Background(), "replsim", version)
	if err != nil {
		slog.Error("OTel init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if serr := otelShutdown(context.Background()); serr != nil {
			slog.Warn("OTel shutdown error", "error", serr)
		}
	}()
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Warn("config load warning (using defaults where needed)", "error", err)
	}
	cfg.ApplyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	orch.SetMaxClusters(cfg.Simulation.MaxClusters)

	srv := gateway.NewServer(orch, bus, cfg.Server.CORSOrigins)
	srv.SetBuildInfo(version, commit, date)
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: srv.Router(),
	}

	// Guarded pprof: only exposed on a separate loopback port when PPROF_ADDR is set
	// (e.g. PPROF_ADDR=localhost:6060), never on the public API surface.
	if pprofAddr := os.Getenv("PPROF_ADDR"); pprofAddr != "" {
		go func() {
			slog.Info("pprof listening", "addr", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				slog.Warn("pprof server stopped", "error", err)
			}
		}()
	}

	go func() {
		slog.Info("server listening", "addr", addr, "max_clusters", cfg.Simulation.MaxClusters, "version", version)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
	slog.Info("server stopped")
}

// logLevel reads LOG_LEVEL (debug|info|warn|error), defaulting to info.
func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
