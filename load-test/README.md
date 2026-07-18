# Load testing

Smoke / load scripts for the replication-strategies backend. The backend must be
running and reachable (default `http://localhost:8080`).

Start the backend first, e.g.:

```bash
go run ./cmd/server
# or
docker compose up -d
```

## k6 (primary)

[k6](https://k6.io) drives a full cluster lifecycle: it creates a leaderless
cluster, runs N writes + reads per iteration, and checks that every request is
2xx. It deletes the cluster on teardown.

```bash
# defaults: BASE_URL=http://localhost:8080, 1 VU, 10s, 20 writes/iter
k6 run load-test/k6-smoke.js

# parametrized
BASE_URL=http://localhost:8080 VUS=10 DURATION=30s WRITES=50 k6 run load-test/k6-smoke.js
```

Environment variables:

| Var        | Default                  | Meaning                          |
| ---------- | ------------------------ | -------------------------------- |
| `BASE_URL` | `http://localhost:8080`  | Backend base URL                 |
| `VUS`      | `1`                      | Concurrent virtual users         |
| `DURATION` | `10s`                    | Test duration                    |
| `WRITES`   | `20`                     | Writes (and reads) per iteration |

The run fails (non-zero exit) if `checks` drop below 99% or `http_req_failed`
exceeds 1%, so it doubles as a CI smoke gate.

## vegeta (alternative one-liner)

[vegeta](https://github.com/tsenart/vegeta) is handy for a quick fixed-rate
hammer against a single endpoint. First create a cluster and capture its id:

```bash
CID=$(curl -s -XPOST http://localhost:8080/api/v1/clusters \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"leaderless","node_count":3,"quorum_n":3,"quorum_w":2,"quorum_r":2}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

# 50 writes/sec for 15s against that cluster
echo "POST http://localhost:8080/api/v1/clusters/${CID}/write
Content-Type: application/json
@/dev/stdin" | \
  vegeta attack -rate=50 -duration=15s \
    -body <(echo '{"key":"vk","value":"vv","client_id":"vegeta"}') | \
  vegeta report
```

Or the simplest read-only smoke:

```bash
echo "GET http://localhost:8080/healthz" | vegeta attack -rate=100 -duration=10s | vegeta report
```
