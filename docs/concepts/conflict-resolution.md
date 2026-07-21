# Conflict Resolution

In multi-leader replication, two nodes can independently accept writes to the same key
at the same time. The **conflict resolver** decides which value survives.

---

## When does a conflict occur?

Conflicts arise from **concurrent writes**: two writes where neither causally precedes
the other. The simulator detects this with **vector clocks**.

A vector clock is a map of `nodeID → counter`. A write on node A increments A's entry.
When node B receives A's write and compares it with its own version:

- **A dominates B** (`∀n: A[n] ≥ B[n]`) → A is causally later; no conflict.
- **B dominates A** (`∀n: B[n] ≥ A[n]`) → B is causally later; no conflict.
- **Neither dominates** → concurrent; the resolver is called.

---

## Resolver: LWW (Last-Write-Wins)

```
winner = argmax(write.timestamp)
```

The write with the highest wall-clock timestamp wins. Ties are broken by node ID.

**Pros:** zero coordination, always resolves, simple to reason about.

**Cons:** data loss is invisible — the losing write is silently discarded. Under clock
skew, the causally-later write can lose.

**Best for:** metrics, counters, any data where losing an occasional write is acceptable
and the freshest value wins by definition.

---

## Resolver: Vector Clock

Instead of picking a winner, the resolver **surfaces the conflict**. Both values are
retained until a client explicitly resolves them via:

```bash
curl -X POST localhost:8080/api/v1/clusters/{id}/conflicts/resolve \
  -H 'Content-Type: application/json' \
  -d '{"key": "my-key", "winning_node_id": "node-xxx-1"}'
```

The UI's **Vector Clock Inspector** shows both conflicting values and the causal
diff between their vector clocks — `LEAD`, `LAG`, or `EQUAL` per node entry.

**Pros:** no data loss; application has full control.

**Cons:** requires application-level resolution logic; can pile up unresolved conflicts.

---

## Resolver: CRDT

**Conflict-free Replicated Data Types** use algebraic properties to merge any two
values without coordination. The simulator ships two CRDT types:

=== "G-Counter"
    A **grow-only counter**: each node increments its own slot; the merge is a
    per-slot maximum.

    ```
    node1: {A: 3, B: 1}
    node2: {A: 2, B: 4}
    merged: {A: 3, B: 4}   ← per-slot max
    total: 7
    ```

    Useful for: page views, event counts, likes.

=== "RGA (Replicated Growable Array)"
    An ordered sequence CRDT. Insertions carry a unique causal timestamp;
    the merge order is determined by the timestamp total order, so concurrent
    inserts from different nodes always resolve to the same final sequence.

    Useful for: collaborative text editing, ordered lists.

**Pros:** converges without any coordination; no conflicts possible.

**Cons:** only applies to specific data types; merge can be expensive for large
structures.

---

## Resolver: Manual

Writes that conflict are **blocked** until the API caller resolves them. The cluster
queues the write and returns a `409 Conflict` with the conflict ID.

```json
{
  "error": "conflict",
  "conflict_id": "abc123",
  "key": "my-key"
}
```

Resolve it:

```bash
curl -X POST localhost:8080/api/v1/clusters/{id}/conflicts/resolve \
  -d '{"conflict_id": "abc123", "winning_node_id": "node-xxx-2"}'
```

**Best for:** teaching/demo scenarios where you want to examine every conflict before
resolving.

---

## Choosing a resolver

| Scenario | Recommended resolver |
|----------|---------------------|
| Metrics / counters | `crdt` (G-Counter) |
| Last-seen-value semantics | `lww` |
| Shopping carts, collaborative docs | `crdt` (application-specific CRDT) |
| User profile updates | `vector_clock` + application merge |
| Demo / education | `manual` |
| Don't care about lost writes | `lww` |

---

## Per-node conflict counter

The **Inspector** drawer (click a node in the topology) shows a **Conflict Stats**
table with a live count of detected conflicts per node since the cluster was created.
Nodes with active conflicts show an amber highlight.
