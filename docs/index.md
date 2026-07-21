# Replication Strategies

An interactive **distributed-systems replication simulator**. It implements and
visualizes the four canonical replication strategies side by side — with real
network-fault injection — so you can *see* the trade-offs between consistency,
availability, and latency play out live.

---

## What it simulates

=== "Single-Leader"
    One leader accepts all writes and replicates an append-entries log to followers.
    Toggle between **async**, **semi-sync**, and **sync** durability, observe follower
    lag, and watch catch-up after dropped entries.

=== "Multi-Leader"
    Every node accepts writes simultaneously. Concurrent writes are detected via
    **vector clocks** and reconciled by a pluggable resolver:
    **LWW**, **vector-clock**, **CRDT (G-Counter / RGA)**, or **manual**.
    Anti-entropy reconciles diverged state after a partition heals.

=== "Leaderless (Dynamo)"
    Tunable `N/W/R` quorums with **consistent-hash preference-list routing**,
    **sloppy quorums** with hinted handoff, **async/sync/digest read repair**,
    and region-aware consistency levels (`LOCAL_QUORUM` / `EACH_QUORUM`).
    The `W + R > N` guarantee is enforced and visualized.

=== "Raft (Consensus)"
    Real **leader election**, log replication with **log-matching**, **majority
    commit**, automatic failover on leader crash, and **log compaction** with
    snapshots. Pause a node and watch a new leader get elected in real time.

---

## Correctness tooling

| Checker | Endpoint | What it verifies |
|---------|----------|-----------------|
| Linearizability | `GET /clusters/{id}/linearizable` | Porcupine-style history check |
| Invariants | `GET /clusters/{id}/invariants` | Always-on convergence + quorum overlap |
| Convergence | `GET /clusters/{id}/convergence` | All online replicas agree on every key |
| Merkle anti-entropy | `POST /clusters/{id}/anti-entropy` | Syncs only the divergent key-set |

---

## Interactive demos

Beyond the four strategies, the CLI and API expose primitive demos you can drive
without a UI:

- **2PC** — two-phase commit with a simulated coordinator crash
- **MVCC** — snapshot-isolated reads across concurrent writers
- **WAL durability** — buffered / fsync / group-commit write modes
- **SWIM gossip** — failure detection and membership propagation
- **Paxos / Multi-Paxos** — single-decree and multi-decree consensus
- **Deterministic clock** — seeded simulation clock for reproducible replays

---

## Get started

```bash
# 1 — run the backend (default port 8080)
go run ./cmd/server

# 2 — run the frontend BFF (serves the UI on :3001)
cd frontend && bun server/bff.ts

# 3 — open the simulator
open http://localhost:3001
```

Or use Docker Compose:

```bash
docker compose up
```

See the [Quick Start](quickstart.md) for full details including Docker and config options.

---

## Navigation

- [Quick Start](quickstart.md) — run locally, Docker, and config reference
- [Architecture](architecture.md) — component map, data-flow diagrams
- [Strategies](strategies/index.md) — deep dives on each replication model
- [API Reference](API.md) — full REST endpoint catalogue
- [CLI Reference](CLI.md) — `replsim` subcommands and flags
- [ADRs](adr/index.md) — architecture decision records
