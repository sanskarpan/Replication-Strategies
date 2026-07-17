# Enhancement Roadmap

An in-depth, prioritized catalogue of possible enhancements, additions, and follow-up
work for the Replication-Strategies simulator, produced from a four-track deep audit
(distributed-systems, frontend/visualization, engineering/infra, product/pedagogy).

Effort key: **S** ≤ half-day · **M** 1–2 days · **L** 3–5 days · **XL** 1–2 weeks.

---

## 0. Bugs / drift surfaced during this audit (fix first — cheap)

- **`cors_origins` config is dead** — `gateway/server.go` hardcodes `Access-Control-Allow-Origin: *` while `config.yaml`/`config.go` carry `cors_origins` that are never read. Wire the config into the CORS middleware. **(S)**
- **`frontend/package.json` pins `elysia`/`cors`/playwright to `"latest"`** — non-reproducible builds; replace with semver ranges and commit the resolved lockfile. **(S)**
- **BFF bundle cache never invalidates** — `server/bff.ts` memoizes the first `Bun.build` result forever, so client edits don't appear without a server restart (fine for prod, a foot-gun in dev). Add dev-mode invalidation / HMR. **(S)**
- **15 Go files fail `gofmt -l`** — space/alignment drift (pre-existing). One-time `gofmt -w .` + enforce in CI. **(S)**
- **Consistency demo endpoints always report success** — `handleDemoRYW/Monotonic/ConsistentPrefix` read from the leader and hardcode `consistent: true`; they never show the *violation* that makes the guarantee interesting. Target lagging followers instead. **(M)**

---

## 1. Distributed-systems features & correctness

### Consensus & leader election (biggest advertised gap)
- **Real Raft-style leader election** ⭐ *(XL)* — the wire types (`MsgVoteRequest/Response`, `Term`, `EvtLeaderElected`) exist but are unused; `leaderID` is write-once and `LeaderFailover` promotes nothing. Add candidate/leader states, randomized election timeouts, `RequestVote` with last-log check, heartbeats, and re-route `Orchestrator.Write` on election.
- **Log terms + AppendEntries consistency check (log matching)** *(L)* — `LogEntry.Term` is always 0; followers never validate `prevLogIndex/prevLogTerm`, so post-election divergence can't be detected. Adds Raft's core safety property.
- **Log compaction & snapshots (InstallSnapshot)** *(L)* — `ReplicationLog.entries` grows unbounded; snapshot state + truncate + catch up a rejoining follower via a snapshot.
- **Paxos / Multi-Paxos comparison mode** *(XL)* — contrast Raft's strong-leader log with per-slot Paxos rounds as a 4th strategy.

### Leaderless: hashing, anti-entropy, sloppy quorums
- **Consistent hashing + virtual nodes + preference lists** ⭐ *(L)* — leaderless currently fans out to *all* nodes (`getOtherNodes`) with no key→replica placement. Add a hash ring so N means "next N on the ring"; unlocks the items below.
- **Merkle-tree anti-entropy** *(L)* — replace the O(store) full rebroadcast (multi-leader + hints) with range-diff sync; visualize the tree comparison narrowing to the differing leaf.
- **True sloppy quorums with correct hinted handoff** *(M)* — write to the next healthy node on the ring (tagged with `OriginalTarget`, a field that already exists) so W is met during a partition, then hand off on recovery.
- **Read-repair strategy options (sync / async / digest reads)** *(M)* — add blocking repair and digest (hash-only) reads to teach the bandwidth/latency tradeoff.

### Failure detection & membership
- **Phi-accrual failure detector** *(M)* — replace manual `Pause()` with inferred suspicion from missed heartbeats (`MsgHeartbeat` types already exist); feeds election + handoff.
- **Gossip-based membership (SWIM)** *(L)* — ping / ping-req / suspect / dead with incarnation numbers, instead of the orchestrator pushing the full member list.
- **Dynamic membership reconfiguration (joint consensus / ring rebalancing)** *(L)* — `AddNode` currently rewrites leaderless N/W/R live, briefly risking quorum overlap; do it safely.

### Clocks & ordering
- **Hybrid Logical Clocks (HLC)** *(M)* — LWW mixes wall-clock with vector clocks; HLC gives a single causality-respecting monotonic timestamp (CockroachDB) and fixes LWW under skew.
- **Bounded clock-skew model + skew injection** *(S)* — per-node clock offset so LWW can be *shown* losing a causally-later write (then HLC fixing it).
- **Bounded-staleness & causal read levels** *(M)* — add explicit causal + bounded-staleness consistency levels; wire the existing guarantees into multi-leader/leaderless (they're single-leader-only today).

### Correctness checkers (huge credibility payoff)
- **Linearizability checker (Porcupine/Knossos-style)** ⭐ *(L)* — record op histories and *prove* that single-leader-sync is linearizable and leaderless `W+R≤N` is not, pinpointing the violating op.
- **Continuous invariant + convergence checker** *(M)* — always-on checks (no lost update, monotonic per client, all replicas agree after quiesce); red banner on violation.
- **Deterministic simulation mode (seeded scheduler / virtual clock)** *(XL)* — FoundationDB/TigerBeetle-style reproducible runs; abstract `time` behind a `Clock` seam.

### More CRDTs, conflict UX, transactions, durability
- **Expanded CRDT library: PN-Counter, OR-Set, LWW-Map, RGA** *(L)* — only G-Counter + LWW register exist; each teaches a distinct merge idea (all self-describing via the `crdt_type` tag already added).
- **Manual / interactive conflict resolution + Riak-style siblings** *(M)* — return concurrent versions to the client instead of silently LWW-picking; let a merge write reconcile them.
- **Atomic multi-key writes / mini-transactions + 2PC** *(M–L)* — everything is single-key today; add all-or-nothing batches and a 2PC demo (crash between prepare/commit shows blocking).
- **MVCC snapshot reads** *(L)* — per-key version chains + read-at-timestamp for snapshot isolation.
- **Tunable storage durability / WAL & fsync modeling** *(M)* — model buffered vs fsync vs group-commit and a crash that loses un-fsynced-but-acked data.

### Realism
- **Multi-region / geo-replication topology** *(M)* — region abstraction + inter-region latency matrix + region-aware quorums (LOCAL_QUORUM/EACH_QUORUM).
- **Back-pressure & load-shedding modeling** *(M)* — the fabric silently drops on full queues; make it visible (queue depth, shed counts, admission control).
- **Realistic latency distributions & tail latency** *(M)* — replace fixed per-link latency with jitter/heavy-tail; record p50/p99 histograms (metrics only compute averages today).

---

## 2. Frontend / visualization / UX

### Make replication *visible* (the central gap)
- **Animated message-passing packets on the topology** ⭐ *(L)* — spawn tweened packets along links per `entry_replicated`/`read_repair`/`hinted_handoff` event; fade+shake on drops. The flagship visual.
- **Vector-clock & causal-history visualization** *(L)* — render the happens-before lattice (concurrent forks → merge) instead of raw `JSON.stringify`; per-node VC chips.
- **"Diverged state" diff view across replicas** *(M)* — key×node matrix colored by agreement (green/amber/red) with a live convergence gauge after a heal.
- **Per-node inspector (store + log)** *(M)* — `getNodeLog`/`getNodeStore` APIs already exist and are unused; a slide-out drawer showing committed vs uncommitted tail.
- **Real-time consistency-violation highlighting** *(M)* — flash the offending node/link when an RYW/monotonic/quorum violation occurs organically.

### Time & history
- **Timeline scrubber to replay events** *(L)* — scrub back/forth over partition→diverge→heal; reconstruct state by folding the event log.
- **Jepsen-style history viewer** *(L)* — per-client op swimlanes with invoke→complete intervals and anomaly brackets.
- **Latency & throughput charts** *(M)* — the per-node latency arrays are collapsed to a single average; show p50/p95/p99 + rolling throughput.
- **CAP-theorem visualization during partitions** *(M)* — split the topology into hulls and light a CP/AP dial based on strategy + quorum.

### Interactivity & configuration
- **Interactive quorum slider with live stale-read probability** *(M)* — the `.slider-row` CSS is already stubbed; drag N/W/R and animate the Venn + probability bar.
- **Side-by-side strategy comparison** *(L)* — same workload+faults into 2–3 clusters simultaneously.
- **Save/load configs + shareable permalinks + export** *(M)* — serialize `{config, faults, workload, seed}` to URL/file for reproducibility.
- **Better error toasts + WS connection status pill** *(S)* — replace `alert()`; show live/reconnecting/down.
- **Keyboard shortcuts + command palette** *(S–M)*.

### Framework & quality
- **Componentize the 860-line `main.ts` monolith with a reactive layer** *(XL)* — every feature above is painful without it; the `topoSig`/`consistencySig` hacks are symptoms.
- **Proper build + HMR dev server** *(M)* — fixes the "edits don't appear" cache foot-gun.
- **Vitest/`bun:test` units + real Playwright `@playwright/test` suite + Storybook/gallery** *(M each)* — only one smoke script exists today.
- **Theme tokens + dark/light toggle + accessibility pass + responsive/resizable layout** *(S–M each)* — CSS vars already centralize color; add `ResizeObserver`, ARIA, colorblind-safe channels, `prefers-reduced-motion`.
- **Node drag-to-pin, zoom/pan, layout presets, event-log filtering/search, workload generator panel** *(S–M each)*.

---

## 3. Engineering / infrastructure / testing / observability

### CI/CD & hygiene (nothing exists today)
- **GitHub Actions CI** ⭐ *(L)* — build, vet, `-race` tests + coverage, gofmt-check, golangci-lint, frontend typecheck, Playwright E2E with artifacts; branch protection.
- **golangci-lint config** *(M)* — Makefile references it but there's no `.golangci.yml`.
- **gofmt/goimports normalization** *(S)* — do first; unblocks the CI fmt gate.
- **Pre-commit hooks, Makefile overhaul, EditorConfig, CODEOWNERS, PR/issue templates** *(S each)*.
- **Dependency automation (Renovate/Dependabot) + govulncheck + gosec + gitleaks** *(S each)*.

### Containerization & parity
- **Multi-stage Dockerfile (backend, distroless) + frontend Dockerfile + docker-compose** *(S–M)* — one-command run; `depends_on` health conditions.

### Testing depth (correctness-critical simulator)
- **Fuzz targets for VClock / quorum / CRDT primitives** ⭐ *(L)* — pure, adversarial-shaped functions and the exact site of past bugs; assert lattice laws.
- **Property-based tests (`rapid`/`testing/quick`)** *(M)* — merge commutativity/associativity/idempotence.
- **Benchmarks + `benchstat` regression tracking** *(M)* — none exist; guard hot paths (store, VClock merge, anti-entropy).
- **Code coverage reporting + frontend `typecheck` script** *(S)*.
- **Linearizability checker in CI (Porcupine)** *(XL)* — validate observed histories under fault injection; the flagship credibility feature (overlaps §1).
- **Contract testing (OpenAPI → generated TS types) + load testing (k6/vegeta)** *(M each)*.

### Observability (rich metrics exist, nothing is exported)
- **Prometheus `/metrics` + provisioned Grafana** ⭐ *(L)* — bridge the in-app metrics to scrapeable time-series.
- **Health/readiness endpoints** *(S)* — `main.go` has graceful shutdown but no `/healthz`/`/readyz`.
- **Structured logging (`slog`) + OpenTelemetry tracing + guarded pprof** *(M–L)* — a replication flow is a textbook distributed trace.

### API, config, release, docs
- **OpenAPI/Swagger spec + docs UI** *(M)* — 25+ undocumented endpoints; enables contract tests + TS generation.
- **Config via env + validation (and fix the CORS drift)** *(M)*.
- **goreleaser (multi-arch binaries + images) + SBOM (syft/grype) + `--version` flag** *(M)*.
- **Architecture docs, ADRs, CONTRIBUTING, Mermaid/C4 diagrams, SECURITY.md** *(M)* — `ISSUES.md` shows discipline worth documenting.

---

## 4. Product / pedagogy / content / portfolio

### Scenario engine (keystone — unblocks most content)
- **Declarative narrated scenario timeline** ⭐ *(L)* — scenarios are fire-and-forget goroutines with no narration/verdict today. Structured `Steps[]` with narration + highlights, emitted as events.
- **"Expected vs Actual" verdicts + assertions** *(M)* — turn watching into understanding; backbone of Challenge Mode.
- **Deterministic mode (seeded RNG + logical clock)** *(L)* — reproducible runs for permalinks, reports, and demo videos.
- **Scenario replay / scrub / timeline** *(L)*.

### New teaching scenarios (mostly choreography on the existing engine)
- **Consistency-*violation* demos** ⭐ *(M)* — RYW/monotonic/consistent-prefix that actually show the violation (read a lagging follower).
- **Leader failover with a real election** *(XL, engine)*, **split-brain fencing/STONITH** *(L)*, **cascading failure / retry-storm** *(M)*, **thundering herd** *(M)*, **hot-key/load-skew (Zipfian)** *(M)*, **network flapping/gray failure** *(S)*, **clock-skew LWW loss** *(M)*, **corrupt-replica fault** *(M)*, **Merkle anti-entropy viz** *(L)*.

### Lessons, challenges, dashboards
- **Guided lesson mode: narration + predict-then-reveal quizzes** ⭐ *(L)* — highest-retention pattern; turns the toy into a course.
- **DDIA chapter mapping / learning path + "explain this event" tooltips + glossary** *(S)*.
- **Challenge Mode: "configure a cluster to meet an SLA"** ⭐ *(L)* — a distributed-systems puzzle game; grades against `StaleReadProbability`/overlap/latency.
- **Free-play sandbox with fault palette + load generator; "break the guarantee" puzzles** *(M)*.
- **Consistency-vs-availability-vs-latency (CAP/PACELC) tradeoff explorer** ⭐ *(M)*, **side-by-side strategy race** *(M)*, **live p50/p99 + stale-read% + convergence-time dashboard** *(M)*.

### Presets, programmatic surface, sharing
- **Real-system presets (Cassandra/DynamoDB/Postgres/etcd/Kafka) + region latency profiles** *(M)*.
- **CLI (`replsim`) to script & assert simulations + scenario-as-YAML + REST playground** *(M each)*.
- **Shareable permalinks + exportable reports (JSON/PNG/PDF) + embeddable iframe widget** *(M each)*.

### Portfolio presentation
- **Landing page + 60–90s demo video** ⭐ *(M)*, **docs site (Astro/Docusaurus)** *(L)*, **one-command hosted live deploy (Fly/Render)** *(M)*.
- **DDIA-mapped explainer blog series + "how I audited my own distributed system" case study from `ISSUES.md`** *(L/M)* — the single biggest portfolio lever per unit effort.
- **Accessibility for the D3 viz, i18n, opt-in learning-outcome telemetry** *(M each)*.

---

## Suggested first wave (max value per effort)

1. **§0 quick fixes** (CORS drift, `"latest"` pins, gofmt, dev bundle cache) — hours, unblocks CI.
2. **§3 CI + golangci-lint + fuzzing the VClock/quorum/CRDT primitives** — protects the correctness-critical core.
3. **§4 declarative narrated scenarios + consistency-*violation* demos** — the biggest "toy → teaches" jump.
4. **§2 packet animation + node inspector + diff view** — makes replication finally *visible* (APIs already exist for the inspector).
5. **§1 real Raft election** (flagship engine gap) and **§1/§3 linearizability checker** (flagship credibility feature).
6. **§4 landing page + demo video + audit case-study** — surface the substantial work already done.

> This is a backlog, not a commitment. Items are independently valuable; the ⭐ items are the highest-leverage in each track.
