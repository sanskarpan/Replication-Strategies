# Multi-Leader Replication

Every node accepts writes simultaneously. The system maximizes write availability —
even a total partition between two halves of the cluster still allows both halves to
accept writes — but requires explicit conflict detection and resolution.

---

## Conflict detection with vector clocks

Each write is stamped with the node's **hybrid logical clock (HLC)** and its
**vector clock** (a per-node counter map). When a node receives a replicated write for
a key it already holds, it compares the two vector clocks:

- **Causal (one dominates)** — the later write wins without conflict; the earlier is discarded.
- **Concurrent (neither dominates)** — a genuine conflict: both writes happened
  independently on different nodes without causal knowledge of each other.

Concurrent writes are handed to the configured `ConflictResolver`.

---

## Conflict resolvers

| Resolver | Algorithm | Trade-off |
|----------|-----------|-----------|
| `lww` | Last-Write-Wins — highest wall-clock timestamp wins | Simple; can silently discard data under clock skew |
| `vector_clock` | Surface the conflict; keep both values until manually resolved | Correct; requires application-level resolution |
| `crdt` | CRDT G-Counter / RGA — commutative merge, no conflicts possible | Only applicable to specific data types |
| `manual` | Pause until the API caller resolves via `POST /conflicts/resolve` | Full control; blocks writes on that key |

Switch the resolver live:

```bash
curl -X PATCH localhost:8080/api/v1/clusters/{id}/config \
  -H 'Content-Type: application/json' \
  -d '{"conflict_resolver": "vector_clock"}'
```

---

## Anti-entropy after a heal

After a network partition heals, nodes that diverged during the split need to reconcile.
The simulator runs a **Merkle-tree anti-entropy** round (`POST /anti-entropy`) that
computes a hash tree over all keys, walks the tree to find divergent subtrees, and
exchanges only the keys that differ.

The **Convergence** checker (`GET /convergence`) confirms that all online replicas
agree on every key's value after the heal.

---

## Vector Clock Inspector

The UI's **Conflicts** panel shows all pending concurrent conflicts. Click any conflict
entry to open the **Vector Clock Inspector** — a side-by-side diff of:

- The two conflicting values
- Each node's vector clock at the time of each write
- A `LEAD / LAG / EQUAL` annotation per node entry showing which write was causally later

---

## What to try in the simulator

1. Create a `multi_leader` cluster with 3 nodes and resolver `lww`.
2. Write the same key from two different nodes rapidly. Observe the conflict in the
   Conflicts panel.
3. Switch to `vector_clock` resolver. Inject a partition between nodes 1 and 2.
4. Write key `x = "from-node-1"` on node 1 and `x = "from-node-2"` on node 2.
5. Heal the partition. Open the Vector Clock Inspector on the resulting conflict and
   inspect the clock divergence.
6. Resolve via `POST /conflicts/resolve` or switch to `crdt` and watch the merge.

---

## Real-world analogues

| System | Approach |
|--------|----------|
| CouchDB | MVCC + application-level conflict resolution |
| DynamoDB Global Tables | Last-write-wins across regions |
| Cassandra multi-DC | LWW with per-column timestamps |
| Git | Three-way merge (a form of CRDT for text) |
