# Replication Strategies

An interactive **distributed-systems replication simulator**. It implements and
visualizes the three canonical replication strategies side by side, with real
network-fault injection, so you can *see* the tradeoffs between consistency,
availability, and latency.

- **Single-leader** — one leader, many followers; async / semi-sync / sync replication,
  follower lag, and catch-up after dropped entries.
- **Multi-leader** — every node accepts writes; conflicts detected via vector clocks and
  resolved by LWW, vector-clock, or CRDT resolvers; anti-entropy reconciles after a heal.
- **Leaderless (Dynamo-style)** — tunable `N/W/R` quorums, read repair, hinted handoff,
  and the `W + R > N` overlap guarantee.

## Architecture

```
Go backend                         Frontend (Bun + D3)
┌───────────────────────────┐      ┌──────────────────────────┐
│ cmd/server  → gateway     │      │ BFF (proxy /api + /ws,    │
│ gateway     → orchestrator│  ◄── │      bundles the client)  │
│ internal/                 │  ws  │ D3 topology + panels      │
│   node (strategies)       │ ───► │ (quorum, consistency,     │
│   storage / transport     │      │  conflicts, lag, events)  │
│   conflict / quorum       │      └──────────────────────────┘
│   consistency / metrics   │
│   simulation / events     │
└───────────────────────────┘
```

## Run it

```bash
# backend (port from config.yaml, default 8080)
go run ./cmd/server

# frontend BFF (serves the UI on :3001, proxies to the backend)
cd frontend && bun server/bff.ts   # set BACKEND=http://localhost:PORT if not 8080
```

Open <http://localhost:3001>, create a cluster, and start writing/reading, injecting
partitions, and running the built-in scenarios.

## Test

```bash
go test ./...              # unit + integration
go test -race ./...        # with the race detector
STRESS_SECONDS=20 go test -race -run TestStress_ ./test/integration/   # soak
cd frontend && node e2e-run.mjs   # Playwright browser E2E (needs backend + BFF up)
```

## Project layout

| Path | What |
|------|------|
| `internal/storage` | KV store, vector clocks |
| `internal/transport` | network fabric (latency/drop/partition), messages |
| `internal/node` | single-leader / multi-leader / leaderless nodes |
| `internal/conflict` | LWW / vector-clock / CRDT resolvers |
| `internal/quorum` | N/W/R math and presets |
| `internal/consistency` | read-your-writes, monotonic reads, consistent prefix |
| `internal/simulation` | cluster orchestrator + scenarios |
| `internal/{events,metrics}` | event bus, metrics |
| `gateway` | REST + WebSocket API |
| `frontend` | Bun BFF + D3 visualization |

See [`ISSUES.md`](ISSUES.md) for the engineering/audit log.
