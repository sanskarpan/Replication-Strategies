# Fault Injection

The simulator's network fabric lets you inject realistic failure conditions without
touching application code. All faults are live — apply and remove them while the
cluster is running.

---

## Network partitions

A partition cuts the network link between two sets of nodes. Messages between the
groups are silently dropped (no error returned to the sender — just silence, as in
real network failures).

**Via the UI:** drag the partition handles in the topology panel to select two groups
of nodes, then click **Partition**.

**Via the API:**

```bash
# Create a partition: nodes 1–2 can't talk to nodes 3–5
curl -X POST localhost:8080/api/v1/clusters/{id}/network/partition \
  -H 'Content-Type: application/json' \
  -d '{
    "group_a": ["node-xxx-1", "node-xxx-2"],
    "group_b": ["node-xxx-3", "node-xxx-4", "node-xxx-5"]
  }'

# Heal all partitions
curl -X DELETE localhost:8080/api/v1/clusters/{id}/network/partition
```

---

## Latency and jitter

The fabric applies per-link latency to every message. You can configure:

- **Fixed latency** — every message takes exactly `N ms`.
- **Jittered latency** — messages sample from a heavy-tail distribution, modelling
  real-network variance (a minority of messages take much longer than average).

Configure via the **Network** panel or `PATCH /clusters/{id}/config`:

```json
{
  "latency_ms": 50,
  "jitter_ms": 20
}
```

---

## Packet loss

Set a per-cluster drop probability (0–1). Each message delivery rolls against this
probability independently. Drop rates above ~20% will cause visible follower lag in
single-leader mode and quorum timeouts in leaderless mode.

---

## Pausing nodes

Pause a node to simulate a crash or a stop-the-world GC pause. A paused node:

- Stops processing its inbox (messages queue in the fabric).
- Stops sending heartbeats (triggers failure detection and potentially a leader election
  in Raft).
- Can be resumed at any time, after which it drains its inbox and catches up.

**Via the UI:** click a node circle → **Pause** / **Resume**.

**Via the API:**

```bash
curl -X POST localhost:8080/api/v1/clusters/{id}/nodes/{nodeId}/pause
curl -X POST localhost:8080/api/v1/clusters/{id}/nodes/{nodeId}/resume
```

---

## Failure detection (phi-accrual)

Each node runs a **phi-accrual failure detector** that maintains a sliding window of
heartbeat inter-arrival times. It outputs a continuous `φ` (phi) suspicion value rather
than a binary up/down state:

- `φ < 1` — node is almost certainly up
- `φ > 8` — node is almost certainly down (threshold configurable)
- Values in between represent proportional uncertainty

The **Suspicion** panel shows the current phi value for each node. Pausing a node
causes its phi to climb; the phi-accrual detector is adaptive — it adjusts to network
jitter so the threshold is meaningful even under variable latency.

```bash
# Get suspicion levels
curl localhost:8080/api/v1/clusters/{id}/suspicion
```

---

## Clock skew

Inject artificial wall-clock skew on a per-node basis:

```bash
curl -X POST localhost:8080/api/v1/clusters/{id}/nodes/{nodeId}/clock-skew \
  -H 'Content-Type: application/json' \
  -d '{"skew_ms": 500}'
```

Positive skew makes the node's clock run ahead; negative skew makes it run behind.
Combined with `lww` conflict resolution in multi-leader mode, this demonstrates how
LWW can silently discard the causally-later write when clocks aren't synchronized.

---

## What each fault reveals

| Fault | Strategy | What you observe |
|-------|----------|-----------------|
| Partition leader from followers | Single-leader | Followers go stale; writes stall |
| Partition minority | Raft | Majority keeps committing; minority stalls |
| Partition majority | Raft | All writes stall (CAP: C over A) |
| Partition between two halves | Multi-leader | Both halves accept writes; conflicts on heal |
| Pause 2 of 5 preferred replicas | Leaderless (W=3) | Sloppy quorum engages; hints stored |
| High packet loss | Any | Visible queue buildup in back-pressure metrics |
| Clock skew + LWW | Multi-leader | Older write silently wins over newer one |
