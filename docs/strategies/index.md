# Replication Strategies — Overview

The simulator implements four replication strategies that cover the main design space
of distributed databases. Each runs as an independent in-process cluster you can create,
fault-inject, and inspect side by side.

---

## Strategy comparison

| | Single-Leader | Multi-Leader | Leaderless | Raft |
|--|:---:|:---:|:---:|:---:|
| **Write target** | Leader only | Any node | Coordinator (any) | Leader only |
| **Write latency** | Low (async) / High (sync) | Low | Tunable (W) | Medium |
| **Read staleness** | Possible | Possible | Tunable (R) | None (linearizable) |
| **Conflict resolution** | None needed | LWW / VC / CRDT | Last-write-wins | None needed |
| **Fault tolerance** | Leader failover (manual) | Partition-tolerant | `N − W + 1` failures | Automatic re-election |
| **Consistency model** | Sequential (async) / Linearizable (sync) | Eventual | Eventual / Strong (W+R>N) | Linearizable |
| **Real-world analogues** | PostgreSQL streaming, MySQL replication, Kafka | CouchDB, Cassandra multi-DC, DynamoDB Global Tables | Cassandra, DynamoDB, Riak | etcd, CockroachDB, TiKV |

---

## CAP positioning

```
         Consistency
              │
         Raft ●
    Single-   │
    Leader ●  │
              │
──────────────┼──────────────
              │          Availability
              │
   Leaderless ●─────────────●  Multi-Leader
   (strong W+R>N)     (W+R≤N or
                       multi-leader)
```

- **Raft** and **sync single-leader** sit at the CP corner: any partition that cuts
  off the leader stalls writes.
- **Async single-leader** relaxes consistency slightly in exchange for lower write
  latency but still loses availability on leader failure.
- **Leaderless with W+R>N** provides tunable strong consistency while surviving up
  to `N − W + 1` simultaneous failures.
- **Multi-leader** and **leaderless with W+R≤N** maximize availability at the cost
  of eventual consistency and the need for conflict resolution.

---

## Deep dives

- [Single-Leader](single-leader.md) — leader/follower log replication, async/sync/semi-sync durability
- [Multi-Leader](multi-leader.md) — vector clocks, conflict detection, LWW/VC/CRDT resolvers
- [Leaderless (Dynamo)](leaderless.md) — consistent hashing, sloppy quorums, hinted handoff, read repair
- [Raft](raft.md) — leader election, log matching, majority commit, snapshots
