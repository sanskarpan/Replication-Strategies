# Contributing

Thanks for your interest in improving the **Replication Strategies** simulator. This
document describes how to set up a development environment, run the backend and
frontend, run the test suites, and the conventions we follow for commits and pull
requests.

## Prerequisites

| Tool | Version | Used for |
|------|---------|----------|
| [Go](https://go.dev/dl/) | 1.23+ (module targets `go 1.23.0`; Go 1.22 also works for most builds) | Backend simulator, gateway, tests |
| [Bun](https://bun.sh/) | 1.3+ | Frontend BFF, bundling, dev server |
| [Node.js](https://nodejs.org/) | 18+ | Playwright end-to-end runner (`e2e-run.mjs`) |

No database or external services are required — everything runs in-process.

## Repository layout

```
cmd/server        # backend entrypoint (loads config.yaml, starts the HTTP server)
gateway           # REST + WebSocket API (chi router)
internal/         # simulation engine: nodes, transport, storage, quorum, ...
frontend/         # Bun + TypeScript client and BFF (Elysia proxy + D3 UI)
test/integration  # cross-package integration / stress tests
```

See the [README](README.md) for a full package-by-package breakdown and
[`docs/architecture.md`](docs/architecture.md) for the component and data-flow overview.

## Running the backend

The server reads `config.yaml` from the working directory (default port `8080`,
`max_clusters: 10`).

```bash
go run ./cmd/server
```

The process serves the REST API under `/api/v1/…` and a WebSocket event stream at
`/ws`. It shuts down cleanly on `SIGINT` / `SIGTERM`.

## Running the frontend

The frontend is a Bun BFF that bundles the browser client, serves it on port `3001`,
and proxies `/api/*` and `/ws` to the Go backend.

```bash
cd frontend
bun install
bun server/bff.ts          # or: bun run bff
```

If the backend is not on the default port, point the BFF at it:

```bash
BACKEND=http://localhost:9090 bun server/bff.ts
```

Then open <http://localhost:3001>. In development (`NODE_ENV` unset), the BFF
re-bundles the client whenever a file under `frontend/src/` changes, so edits appear
on refresh without restarting the server.

## Running the tests

### Backend

```bash
go test ./...                 # unit + integration
go test -race ./...           # with the race detector (required before opening a PR)
go vet ./...                  # static checks
```

Because the simulator is heavily concurrent (per-link fabric workers, node message
loops, quorum waiters), **all changes must pass `go test -race ./...`.** A change that
races under the detector is considered broken even if the plain test run is green.

Longer soak/stress runs:

```bash
STRESS_SECONDS=20 go test -race -run TestStress_ ./test/integration/
```

### Frontend

```bash
cd frontend
bun run typecheck             # tsc --noEmit
node e2e-run.mjs              # Playwright browser smoke test (needs backend + BFF up)
```

The `e2e-run.mjs` script drives a real browser against a running stack, so start the
backend (`go run ./cmd/server`) and the BFF (`bun server/bff.ts`) first.

## Formatting and linting

- **Go:** code must be `gofmt`-clean. Run `gofmt -l .` (or `go fmt ./...`) and fix any
  reported files before committing. Keep `go vet ./...` clean.
- **TypeScript:** keep `bun run typecheck` passing. Match the existing style in
  `frontend/src` (2-space indent, ES modules, no default-exported God objects).
- Prefer small, focused functions and document non-obvious concurrency or protocol
  decisions with a short comment explaining *why*, in the style already used across
  `internal/` (see `internal/transport/fabric.go` and `internal/node/leaderless.go`).

## Commit and pull-request conventions

- **Branch off `main`.** Do your work on a descriptive feature branch
  (e.g. `feat/region-aware-read-repair`, `fix/sloppy-quorum-hint-leak`). Do not push
  directly to `main`.
- **Write meaningful commit messages.** Use a concise imperative subject line
  (`Fix hinted-handoff buffer leak on node removal`) and a body that explains the
  motivation and any tradeoffs when the change is non-trivial. Group related changes
  into logical commits rather than one giant blob.
- **Keep PRs scoped.** One conceptual change per pull request. Describe what changed,
  why, and how you verified it (which tests you ran, whether `-race` is clean).
- **Tests travel with code.** New behavior needs new tests; bug fixes should include a
  regression test that fails before the fix.
- **Green CI is required.** A PR should not be merged while `go test -race ./...`,
  `go vet ./...`, `gofmt`, or the frontend `typecheck` are failing.

## Reporting bugs and requesting features

Open an issue describing the observed behavior, the expected behavior, and the steps
to reproduce (strategy, node count, quorum settings, and any injected faults). For a
security-relevant report, follow [SECURITY.md](SECURITY.md) instead of opening a
public issue.
