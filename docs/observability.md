# Observability

The simulator ships built-in support for **OpenTelemetry tracing**, **Prometheus
metrics**, and a **Grafana dashboard** — all wired together with a single Docker Compose
overlay.

---

## OpenTelemetry tracing

Tracing is **disabled by default** and adds zero overhead when off. Enable it with:

```bash
OTEL_ENABLED=true go run ./cmd/server
```

When enabled, the server exports OTLP traces to the configured collector endpoint
(default `localhost:4317`). Every HTTP request gets a span; write paths through the
network fabric propagate trace context across simulated node boundaries.

### Span coverage

| Layer | Spans |
|-------|-------|
| HTTP gateway | One root span per request |
| Orchestrator | `create_cluster`, `delete_cluster`, `write`, `read` |
| Node write path | `node.write`, `node.replicate`, `await_acks` |
| Network fabric | `fabric.send`, `fabric.deliver` |

### Jaeger

Traces are visible in Jaeger at `http://localhost:16686` when running with the
observability compose overlay.

---

## Prometheus metrics

The server exposes a `/metrics` endpoint (Prometheus format) with per-cluster and
per-node gauges and counters:

| Metric | Labels | Description |
|--------|--------|-------------|
| `replsim_writes_total` | `cluster_id`, `node_id`, `strategy` | Cumulative write count |
| `replsim_reads_total` | `cluster_id`, `node_id` | Cumulative read count |
| `replsim_replication_lag_ms` | `cluster_id`, `follower_id` | Current follower lag |
| `replsim_conflicts_total` | `cluster_id`, `resolver` | Cumulative detected conflicts |
| `replsim_quorum_failures_total` | `cluster_id` | Write failures due to quorum unavailability |
| `replsim_dropped_messages_total` | `cluster_id` | Back-pressure drops (full queues) |
| `replsim_partition_active` | `cluster_id` | 1 when a partition is active |

---

## Grafana dashboard

A pre-built Grafana dashboard is included at
`observability/grafana/dashboards/replication.json`. It shows:

- Write and read throughput per cluster and strategy
- Replication lag per follower (single-leader)
- Quorum failures over time (leaderless)
- Conflict rate (multi-leader)
- Network partition status timeline
- Dropped message back-pressure

---

## Starting the observability stack

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up
```

This starts:

| Service | URL |
|---------|-----|
| Backend | http://localhost:8080 |
| Frontend | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Jaeger | http://localhost:16686 |
| OTel Collector | grpc:4317, http:4318 |

---

## Load testing

A [k6](https://k6.io) smoke test is included at `load-test/k6-smoke.js`:

```bash
k6 run load-test/k6-smoke.js
```

It exercises `POST /write`, `GET /read`, and `DELETE /kv` against a running cluster
and outputs p50/p95/p99 latency and error-rate summaries.

---

## pprof profiling

The server exposes Go pprof handlers on a **separate loopback port** when
`PPROF_ADDR` is set — never on the public API surface:

```bash
PPROF_ADDR=localhost:6060 go run ./cmd/server
go tool pprof http://localhost:6060/debug/pprof/profile
```

This gives you CPU profiles, heap snapshots, goroutine traces, and mutex contention
data without exposing them to the network.
