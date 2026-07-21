# Quick Start

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Go](https://go.dev/dl/) | 1.23+ | Backend simulator and gateway |
| [Bun](https://bun.sh/) | 1.3+ | Frontend BFF, bundler, dev server |
| [Node.js](https://nodejs.org/) | 18+ | Playwright E2E runner |

No external database or services required — everything runs in-process.

---

## Run locally

### 1. Clone

```bash
git clone https://github.com/sanskarpan/Replication-Strategies.git
cd Replication-Strategies
```

### 2. Start the backend

```bash
go run ./cmd/server
# → listening on :8080
```

The server reads `config.yaml` from the working directory. Override the port with
`PORT=9000` or point at a different config file with `-config path/to/config.yaml`.

### 3. Start the frontend BFF

```bash
cd frontend
bun server/bff.ts
# → serving the UI on :3001, proxying /api and /ws to :8080
```

### 4. Open the simulator

```
http://localhost:3001
```

Create a cluster, pick a strategy, start writing keys, inject partitions, and watch
the topology react in real time.

---

## Docker Compose

The repo ships two compose files:

=== "Full stack"
    ```bash
    docker compose up
    # backend on :8080  ·  frontend on :3001
    ```

=== "With observability"
    ```bash
    docker compose -f docker-compose.yml -f docker-compose.observability.yml up
    # adds OTel Collector, Prometheus, Grafana, Jaeger
    # Grafana → http://localhost:3000
    # Jaeger  → http://localhost:16686
    ```

---

## Configuration

`config.yaml` controls all runtime behaviour. The full file with defaults:

```yaml
server:
  port: 8080
  cors_origins:
    - "http://localhost:3001"

simulation:
  default_lag_threshold_ms: 100
  heartbeat_interval_ms: 50
  max_clusters: 10

persistence:
  # SQLite database path.  Leave empty ("") to disable persistence.
  # Use ":memory:" for an ephemeral in-process database (tests/CI).
  sqlite_path: "./data/simulation.db"
```

### Environment overrides

| Variable | Equivalent config key | Example |
|----------|----------------------|---------|
| `PORT` | `server.port` | `PORT=9090` |
| `MAX_CLUSTERS` | `simulation.max_clusters` | `MAX_CLUSTERS=5` |
| `SQLITE_PATH` | `persistence.sqlite_path` | `SQLITE_PATH=:memory:` |
| `CORS_ORIGINS` | `server.cors_origins` | `CORS_ORIGINS=https://example.com` |
| `LOG_LEVEL` | *(not in yaml)* | `LOG_LEVEL=debug` |
| `OTEL_ENABLED` | *(not in yaml)* | `OTEL_ENABLED=true` |

---

## Running tests

```bash
# Unit + integration (Go)
go test ./...

# With race detector (recommended before committing)
go test -race ./...

# Soak / stress test
STRESS_SECONDS=20 go test -race -run TestStress_ ./test/integration/

# Browser E2E (backend + BFF must be running)
cd frontend && node e2e-run.mjs
```

---

## Using the CLI without a server

`replsim` embeds the orchestrator in-process — no HTTP server, no ports:

```bash
go run ./cmd/replsim list-scenarios
go run ./cmd/replsim run -scenario ReplicationLag
go run ./cmd/replsim check -strategy leaderless -nodes 5 -w 3 -r 3
```

See the [CLI Reference](CLI.md) for all subcommands and flags.
