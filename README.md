# Replication Strategies

An interactive **distributed-systems replication simulator**. It implements and
visualizes the three canonical replication strategies side by side, with real
network-fault injection, so you can *see* the tradeoffs between consistency,
availability, and latency.

- **Single-leader** — one leader, many followers; async / semi-sync / sync replication,
  follower lag, and catch-up after dropped entries.
- **Multi-leader** — every node accepts writes; conflicts detected via vector clocks and
  resolved by LWW, vector-clock, or CRDT resolvers; anti-entropy reconciles after a heal.
- **Leaderless (Dynamo-style)** — tunable `N/W/R` quorums, consistent-hash **preference-list
  routing**, **sloppy quorums** with hinted handoff, **async/sync/digest read repair**,
  **region-aware quorums** (`LOCAL_QUORUM`/`EACH_QUORUM`), and the `W + R > N` guarantee.
- **Raft (consensus)** — real leader election, log replication with log-matching,
  majority commit, automatic failover, and log compaction + snapshots.

**Correctness checkers & anti-entropy:** a Porcupine-style **linearizability checker**
(`/linearizable`), a continuous **invariant + convergence checker** (`/invariants`), and
**Merkle-tree anti-entropy** (`/anti-entropy`) that syncs only the divergent keys.

**Interactive primitive demos** (`/api/v1/demos/…`): **2PC** (with a coordinator crash),
**MVCC** snapshot reads, tunable **WAL durability** (buffered/fsync/group-commit), **SWIM**
gossip membership, **Paxos**/Multi-Paxos, and a seeded **deterministic-simulation** clock.
Plus safe two-phase **membership reconfiguration** (`/reconfigure/add-node`).

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
| `internal/transport` | network fabric (latency/jitter/drop/partition), messages |
| `internal/node` | single-leader / multi-leader / leaderless / raft nodes |
| `internal/conflict` | LWW / vector-clock / CRDT resolvers (incl. RGA) |
| `internal/quorum` | N/W/R math and presets |
| `internal/consistency` | read-your-writes, monotonic, causal, bounded-staleness |
| `internal/hashring` | consistent hashing + preference lists |
| `internal/{checker,antientropy}` | linearizability checker, Merkle anti-entropy |
| `internal/{twopc,mvcc,durability,swim,paxos,simclock}` | 2PC, MVCC, WAL, SWIM, Paxos, deterministic clock |
| `internal/simulation` | cluster orchestrator + scenarios + checkers + demos |
| `internal/{events,metrics,clock,failure}` | event bus, metrics, HLC, phi-accrual |
| `gateway` | REST + WebSocket API |
| `frontend` | Bun BFF + D3 visualization |

See [`ISSUES.md`](ISSUES.md) for the engineering/audit log.
