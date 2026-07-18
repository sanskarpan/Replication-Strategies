# 2. Node interface, per-strategy node types, and a FIFO-per-link transport fabric

Date: 2025-07-18

## Status

Accepted

## Context

The simulator must run four fundamentally different replication strategies —
single-leader, multi-leader, leaderless (Dynamo-style), and Raft — side by side, and
let the orchestrator drive them uniformly (create, start, pause, write, read, inspect)
while each strategy runs its own protocol logic.

The strategies also share a lot: identity and peers, a KV store, a replication log,
metrics, a hybrid logical clock, and pause/resume semantics. Duplicating that across
four implementations would be error-prone.

Finally, all strategies talk over a *simulated* network. Single-leader log replication
in particular relies on messages arriving in the order they were sent: if a later
`AppendEntries` could overtake an earlier one, a follower's log would diverge in ways
that don't happen on a real TCP link. So the transport layer's ordering guarantees are
not an incidental detail — they are part of the correctness contract.

## Decision

**A single `Node` interface** ([`internal/node/node.go`](../../internal/node/node.go))
defines everything the orchestrator and gateway depend on: `Write`/`Read`/`Delete`,
lifecycle (`Start`/`Stop`/`Pause`/`Resume`), state/log/store/metrics accessors, peer
management, and the message inbox (`Inbox`/`HandleMessage`). The orchestrator programs
against this interface and never against a concrete strategy.

**A shared `BaseNode`** ([`internal/node/base.go`](../../internal/node/base.go)) is
embedded by every strategy and provides the common state: id/cluster/role, peer list,
`storage.Store`, `replication.ReplicationLog`, `metrics.NodeMetrics`, the HLC, and
pause/offline handling. Each strategy type — `SingleLeaderNode` (+ `FollowerNode`),
`MultiLeaderNode`, `LeaderlessNode`, `RaftNode` — embeds `BaseNode` and adds only its
protocol-specific fields and its own goroutine message loop that drains messages the
fabric delivers to its inbox.

**A FIFO-per-link transport fabric**
([`internal/transport/fabric.go`](../../internal/transport/fabric.go)). The
`NetworkFabric` gives each `(source → target)` pair its own ordered queue, drained by a
single dedicated worker goroutine. Delivery timestamps are clamped monotonically
(`if at.Before(l.lastAt) { at = l.lastAt }`), so even with per-message jitter or a
heavy-tail latency spike, a later message on a link can never be delivered before an
earlier one. On top of this ordering the fabric layers configurable latency
distributions, packet-drop probability, and partitions, and it counts back-pressure
drops when a queue or inbox is full.

## Consequences

- The orchestrator, gateway, and scenario engine are strategy-agnostic: adding a new
  strategy means implementing `Node` and a constructor, not touching the callers.
- Common concerns (store, log, HLC, metrics, pause) are implemented once, so behavior
  like clock-skew injection or pause-means-drop is consistent across strategies.
- FIFO-per-link ordering makes single-leader and Raft log replication behave like a
  real ordered transport, so the correctness checkers observe realistic histories.
- The design costs one goroutine per active link and one per node message loop. This is
  fine at simulator scale, but link workers must be torn down on cluster deletion
  (`Fabric.Close`) to avoid goroutine leaks — which the orchestrator does.
- Ordering is guaranteed *per link only*. There is deliberately no global ordering
  across links, matching real networks; strategies must not assume cross-link ordering.
