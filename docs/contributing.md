# Contributing

Thanks for your interest in improving the **Replication Strategies** simulator.

---

## Prerequisites

| Tool | Version | Used for |
|------|---------|----------|
| [Go](https://go.dev/dl/) | 1.23+ | Backend simulator, gateway, tests |
| [Bun](https://bun.sh/) | 1.3+ | Frontend BFF, bundling, dev server |
| [Node.js](https://nodejs.org/) | 18+ | Playwright E2E runner |

No database or external services required — everything runs in-process.

---

## Running everything locally

```bash
# Backend (port 8080)
go run ./cmd/server

# Frontend BFF (port 3001)
cd frontend && bun server/bff.ts
```

Open <http://localhost:3001>.

---

## Test suite

```bash
# Go unit + integration
go test ./...
go test -race ./...                                         # with race detector

# Stress test (20-second soak)
STRESS_SECONDS=20 go test -race -run TestStress_ ./test/integration/

# Browser E2E (requires backend + BFF running)
cd frontend && node e2e-run.mjs

# Go benchmarks
go test -bench=. -benchmem ./...
```

---

## Code style

```bash
gofmt -w .                # Go formatting
golangci-lint run         # linting (config in .golangci.yml)
cd frontend && bun run typecheck   # TypeScript
```

Pre-commit hooks run `gofmt` and `golangci-lint` automatically if you install them:

```bash
pip install pre-commit && pre-commit install
```

---

## Commit conventions

One file per commit where possible. Commit messages follow the format:

```
type(scope/#issue): short description
```

Types: `feat`, `fix`, `test`, `style`, `refactor`, `docs`, `ci`, `chore`.

Examples:
```
feat(leaderless): add region-aware quorum levels
fix(raft): prevent split-brain during network heal
test(e2e): add conflict inspector Playwright test
```

---

## Pull request checklist

- [ ] `go test -race ./...` passes
- [ ] `gofmt -l .` outputs nothing
- [ ] `golangci-lint run` passes
- [ ] Frontend: `bun run typecheck` passes
- [ ] E2E: 54/54 pass (`node e2e-run.mjs`)
- [ ] New behaviour is covered by a test

---

## Project layout

| Path | Responsibility |
|------|---------------|
| `cmd/server` | HTTP server entrypoint |
| `cmd/replsim` | In-process CLI |
| `gateway` | REST + WebSocket API |
| `internal/simulation` | Orchestrator, cluster lifecycle |
| `internal/node` | Strategy node types |
| `internal/transport` | Network fabric |
| `internal/storage` | KV store, vector clocks |
| `internal/conflict` | Conflict resolvers |
| `internal/quorum` | N/W/R math |
| `internal/hashring` | Consistent hash ring |
| `internal/checker` | Linearizability checker |
| `internal/antientropy` | Merkle anti-entropy |
| `internal/persistence` | SQLite store |
| `frontend/src` | Browser UI (TypeScript + D3) |
| `frontend/server` | BFF (Elysia/Bun proxy + bundler) |
| `test/integration` | Cross-package integration tests |
| `docs/` | This documentation |
| `observability/` | Grafana dashboard, OTel Collector, Prometheus config |
| `load-test/` | k6 smoke test |

---

## Adding a new feature

1. Open an issue describing what you want to build.
2. Write a failing test first (TDD).
3. Implement the feature.
4. Update `docs/` if the feature is user-visible.
5. Open a PR — CI runs the full suite automatically.

See the [Architecture](architecture.md) for a component map, and the
[ADRs](adr/index.md) for the reasoning behind key design decisions.
