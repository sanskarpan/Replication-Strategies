# Correctness Checking

The simulator ships several built-in correctness checkers you can run against any live
cluster. They give you a rigorous answer to "is this cluster behaving correctly?" beyond
what the UI can show.

---

## Linearizability

**Linearizability** is the strongest single-object consistency model. An execution is
linearizable if every operation appears to take effect atomically at some point between
its invocation and its response, and the ordering of those points is consistent with
real time.

The simulator uses a **Porcupine-style** WGL (Wing-Gong-Leuschel) linearizability
checker over the client operation history.

```bash
curl localhost:8080/api/v1/clusters/{id}/linearizable
```

```json
{
  "linearizable": true,
  "ops": 42,
  "duration_ms": 3
}
```

!!! tip "When to run it"
    Run after a scenario that involves concurrent writes and partition-then-heal cycles.
    A `false` result means the cluster produced an anomaly: a read observed a value that
    could not have been the result of any legal sequential execution.

!!! note "Raft is always linearizable"
    Raft's majority-commit guarantee makes anomalies structurally impossible.
    Single-leader **sync** mode is also linearizable. Async single-leader and leaderless
    with `W + R ≤ N` are not.

---

## Invariants

The invariants checker runs a battery of always-on assertions:

| Invariant | What it checks |
|-----------|---------------|
| `quorum_overlap` | For leaderless: `W + R > N` guaranteed overlap |
| `no_byzantine` | No node has a value with a future timestamp |
| `monotonic_seq` | Event sequence numbers are strictly increasing |
| `leader_uniqueness` | At most one node believes it is leader per term (Raft) |
| `log_matching` | Raft: no two nodes have conflicting entries at the same index+term |

```bash
curl localhost:8080/api/v1/clusters/{id}/invariants
```

```json
{
  "ok": true,
  "checks": {
    "quorum_overlap": "pass",
    "no_byzantine": "pass",
    "monotonic_seq": "pass"
  }
}
```

---

## Convergence

Checks that all **online** (non-paused, non-partitioned) replicas agree on every key's
current value. After a partition heals, convergence may take a few hundred milliseconds
while anti-entropy propagates.

```bash
curl localhost:8080/api/v1/clusters/{id}/convergence
```

```json
{
  "converged": true,
  "diverged_keys": [],
  "checked_nodes": 5,
  "skipped_offline": 1
}
```

---

## Merkle anti-entropy

The **Merkle tree anti-entropy** round is both a convergence tool and a correctness
demonstration. It:

1. Builds a Merkle hash tree over each node's key set.
2. Exchanges root hashes between node pairs.
3. Walks disagreeing subtrees to find the minimal divergent key set.
4. Exchanges only the differing key-value pairs.

This models exactly what Cassandra's anti-entropy repair does.

```bash
curl -X POST localhost:8080/api/v1/clusters/{id}/anti-entropy
```

```json
{
  "rounds": 1,
  "synced_keys": 3,
  "duration_ms": 12
}
```

---

## Event history and timeline scrubber

Every cluster event (write, partition, leader election, conflict) is stored in a
durable **ring-buffer event log** with periodic state snapshots. The timeline scrubber
in the UI lets you:

- **Seek** to any past event by sequence number.
- **Step** forward/backward through the history.
- **Play** a recorded scenario from the beginning.

The history API:

```bash
# Get recent events
curl localhost:8080/api/v1/clusters/{id}/history

# Reconstruct state at a specific sequence number
curl "localhost:8080/api/v1/clusters/{id}/history/state?at=42"
```

---

## Jepsen-style operation swimlane

The **op-history** endpoint returns the full client operation log in a format compatible
with Jepsen analysis:

```bash
curl localhost:8080/api/v1/clusters/{id}/ops
```

The UI renders this as a swimlane — one lane per client, operations drawn as
horizontal bars from invocation to response, with anomaly brackets where the
linearizability checker found violations.

---

## The CLI `check` command

Run a self-contained correctness check from the terminal without a running server:

```bash
replsim check -strategy leaderless -nodes 5 -w 3 -r 3
# → PASS: invariants hold, quorum overlap = 1
```

```bash
replsim check -strategy leaderless -nodes 5 -w 2 -r 2
# → PASS: invariants hold (eventual consistency — stale-read probability ~20%)
```

Exit code `0` = pass, `1` = invariant violation.
