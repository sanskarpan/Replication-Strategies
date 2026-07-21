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
	"replication-strategies/internal/persistence"
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

	if err := run(*configPath); err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
}

// run contains all startup logic so that deferred cleanup (OTel, DB) always
// executes on return — os.Exit in main() fires before any defers are registered.
func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Warn("config load warning (using defaults where needed)", "error", err)
	}
	cfg.ApplyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// OpenTelemetry tracing — no-op when OTEL_ENABLED != "true".
	otelShutdown, otelErr := telemetry.Init(context.Background(), "replsim", version)
	if otelErr != nil {
		return fmt.Errorf("OTel init: %w", otelErr)
	}
	defer func() {
		if serr := otelShutdown(context.Background()); serr != nil {
			slog.Warn("OTel shutdown error", "error", serr)
		}
	}()

	bus := events.NewEventBus(1000)
	orch := simulation.NewOrchestrator(bus)
	orch.SetMaxClusters(cfg.Simulation.MaxClusters)

	// Attach SQLite persistence when a path is configured.
	if sqlitePath := cfg.Persistence.SQLitePath; sqlitePath != "" {
		db, dbErr := persistence.Open(sqlitePath)
		if dbErr != nil {
			return fmt.Errorf("persistence: open %s: %w", sqlitePath, dbErr)
		}
		defer func() {
			if cerr := db.Close(); cerr != nil {
				slog.Warn("persistence: close error", "error", cerr)
			}
		}()
		orch.WithPersistence(db)
		if restoreErr := orch.Restore(); restoreErr != nil {
			return fmt.Errorf("persistence: restore: %w", restoreErr)
		}
		slog.Info("persistence: SQLite attached", "path", sqlitePath)
	}

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
	return nil
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
