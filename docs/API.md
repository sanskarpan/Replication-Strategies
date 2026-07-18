# REST API

The gateway exposes a JSON REST API under `/api/v1`, backed by the simulation
orchestrator. The full machine-readable contract lives in
[`openapi.yaml`](./openapi.yaml) (OpenAPI 3.0.3) — this page is a human-readable
index into it.

- **Base URL:** `http://localhost:8080`
- **Content type:** `application/json` (request and response)
- **Errors:** `{ "error": "..." }` with an appropriate HTTP status code
- **Live events:** a WebSocket stream at `GET /ws` (not part of the OpenAPI spec)

To browse the spec interactively, open `openapi.yaml` in
[Swagger Editor](https://editor.swagger.io) or run a viewer such as
`npx @redocly/cli preview-docs docs/openapi.yaml`.

## Simulation

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/simulation/start` | Start the simulation by creating a cluster |
| POST | `/api/v1/simulation/reset` | Delete every active cluster |
| GET | `/api/v1/simulation/state` | State of all clusters |
| GET | `/api/v1/simulation/metrics` | Per-cluster metrics |

## Clusters

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/clusters` | Create a cluster |
| GET | `/api/v1/clusters` | List all clusters |
| DELETE | `/api/v1/clusters/{id}` | Delete a cluster |
| GET | `/api/v1/clusters/{id}/state` | Get a cluster's state |
| GET | `/api/v1/clusters/{id}/placement` | Preference list for a key |
| GET | `/api/v1/clusters/{id}/conflicts` | List pending conflicts |
| POST | `/api/v1/clusters/{id}/conflicts/resolve` | Resolve a pending conflict |
| PATCH | `/api/v1/clusters/{id}/config` | Patch cluster configuration |

## Correctness

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/v1/clusters/{id}/convergence` | Check replica convergence |
| GET | `/api/v1/clusters/{id}/suspicion` | Phi-accrual suspicion levels |
| GET | `/api/v1/clusters/{id}/linearizable` | Check linearizability |
| GET | `/api/v1/clusters/{id}/invariants` | Check always-on invariants |
| POST | `/api/v1/clusters/{id}/anti-entropy` | Run a Merkle anti-entropy round |
| POST | `/api/v1/clusters/{id}/reconfigure/add-node` | Safe two-phase add-node |

## Data (writes / reads)

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/clusters/{id}/write` | Write a key |
| GET | `/api/v1/clusters/{id}/read` | Read a key |
| DELETE | `/api/v1/clusters/{id}/kv` | Delete a key |
| POST | `/api/v1/clusters/{id}/write-batch` | Write a batch (optionally atomic) |

## Nodes

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/clusters/{id}/nodes` | Add a node |
| DELETE | `/api/v1/clusters/{id}/nodes/{nodeId}` | Remove a node |
| POST | `/api/v1/clusters/{id}/nodes/{nodeId}/pause` | Pause a node |
| POST | `/api/v1/clusters/{id}/nodes/{nodeId}/resume` | Resume a node |
| POST | `/api/v1/clusters/{id}/nodes/{nodeId}/clock-skew` | Set clock skew (ms) |
| GET | `/api/v1/clusters/{id}/nodes/{nodeId}/log` | Get a node's replication log |
| GET | `/api/v1/clusters/{id}/nodes/{nodeId}/store` | Get a node's key-value store |

## Network fault injection

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/clusters/{id}/network/partition` | Inject a partition |
| DELETE | `/api/v1/clusters/{id}/network/partition/{partId}` | Heal a partition |
| POST | `/api/v1/clusters/{id}/network/latency` | Set one-way link latency |
| POST | `/api/v1/clusters/{id}/network/drop` | Set packet-drop rate on a link |
| DELETE | `/api/v1/clusters/{id}/network/faults` | Clear all network faults |

## Consistency demos

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/clusters/{id}/demo/read-your-writes` | Read-your-writes demo |
| POST | `/api/v1/clusters/{id}/demo/monotonic-reads` | Monotonic-reads demo |
| POST | `/api/v1/clusters/{id}/demo/consistent-prefix` | Consistent-prefix demo |

## Scenarios

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/v1/scenarios` | List built-in scenarios |
| POST | `/api/v1/scenarios/{name}/run` | Run a scenario |

## Primitive demos (no cluster required)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/v1/demos/2pc` | Two-phase commit (`?crash=true`) |
| GET | `/api/v1/demos/mvcc` | MVCC snapshot reads |
| GET | `/api/v1/demos/wal` | Write-ahead-log durability (`?mode=`) |
| GET | `/api/v1/demos/swim` | SWIM membership |
| GET | `/api/v1/demos/paxos` | Paxos single-decree |
| GET | `/api/v1/demos/detsim` | Deterministic simulation (`?seed=`) |

## Reusable schemas

The spec defines reusable schemas including
[`ClusterConfig`](./openapi.yaml), `ClusterState`, `NodeStatus`, `KVEntry`,
`WriteRequest`, `WriteResult`/`ReadResult`, and the report types
(`ConvergenceReport`, `LinearizabilityReport`, `InvariantReport`,
`AntiEntropyReport`, `ReconfigureReport`, and the consistency- and
primitive-demo reports). See the `components.schemas` section of
[`openapi.yaml`](./openapi.yaml) for the full list.
