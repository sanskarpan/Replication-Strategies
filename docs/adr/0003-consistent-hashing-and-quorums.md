# 3. Consistent hashing, preference lists, sloppy quorums, and region-aware quorums

Date: 2025-07-18

## Status

Accepted

## Context

The leaderless (Dynamo-style) strategy needs to decide *where* each key lives and *how
many* replicas must respond for a read or write to count. A naive "every node is a
replica, wait for W of them" model is easy but wrong in the ways that matter
pedagogically: it doesn't show how keys are placed, how availability survives node
failure, or how multi-region deployments trade latency for cross-region durability.

We want the simulator to demonstrate the real Dynamo/Cassandra behaviors:

- keys distributed across a subset of nodes, rebalancing smoothly on membership change;
- writes that still succeed during partitions when possible;
- and tunable consistency, including region-aware quorums.

## Decision

**Consistent hashing with virtual nodes**
([`internal/hashring/ring.go`](../../internal/hashring/ring.go)). Each physical node
owns 128 virtual tokens on a hash ring. `PreferenceList(key, N)` walks the ring
clockwise from the key's hash and returns the first `N` *distinct* physical nodes —
this is the key's replica set. Virtual nodes keep placement roughly balanced and make
add/remove disturb only a fraction of keys. The `LeaderlessNode` rebuilds the ring on
every membership change (`SetAllNodes`).

**Preference-list routing.** A write coordinator applies the entry locally, then fans
it out only to the key's `N` preferred replicas
([`replicasFor`](../../internal/node/leaderless.go)), not to the whole cluster. Reads
query the key's replicas the same way. This makes cluster size and replication factor
`N` genuinely distinct.

**Sloppy quorums with hinted handoff.** If the preferred replicas can't form the write
quorum `W` (down/partitioned), `sloppyRound` borrows healthy **fallback nodes** further
along the ring, tagging each stand-in write with the `OriginalTarget` it covers. A
background loop (`runHintedHandoff` → `deliverHints`) hands those entries off to the
intended replica once it recovers. This keeps writes available during faults while
preserving eventual placement. Sloppy quorum is on by default and toggleable.

**Region-aware quorums.** With geo-replication configured, each node knows every node's
region. `writeQuorumMet` implements three consistency levels:
`QUORUM` (a simple majority of `N`), `LOCAL_QUORUM` (a majority within the
coordinator's own region — low latency, survives remote-region partitions), and
`EACH_QUORUM` (a majority in *every* region that holds a replica — strongest, but a
single partitioned region fails the write). Reads apply the symmetric `R` requirement.

Because `W + R > N` is enforced by the defaults (`W = R = N/2 + 1`), any read quorum
intersects the most recent write quorum, so a read is guaranteed to observe the latest
acknowledged write; read repair then converges the stale replicas it saw.

## Consequences

- Key placement, rebalancing on resize, and per-key replica sets are visible and
  correct, matching how Dynamo/Cassandra actually behave.
- Writes stay available under partial failure via sloppy quorums, and data eventually
  lands on its home replicas via hinted handoff — at the cost of temporary
  over-replication on stand-in nodes and a background handoff loop.
- Region-aware levels let the simulator demonstrate the latency-vs-durability tradeoff
  of multi-region quorums explicitly.
- The `W + R > N` invariant is the guarantee the UI teaches; changing `N/W/R` at
  runtime must keep all leaderless nodes in agreement (`UpdateQuorum` + `SetAllNodes`
  on resize), otherwise nodes would disagree on the quorum math.
- Placement depends on the FNV hash of node IDs and vnode indices; it is deterministic
  for a given membership but is not intended to match any specific real cluster's ring.
