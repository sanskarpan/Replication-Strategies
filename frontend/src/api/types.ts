import type { components } from "./generated";

// ─── Enum types from OpenAPI spec ────────────────────────────────────────────

export type ReplicationStrategy = components["schemas"]["ReplicationStrategy"];
export type ReplicationMode = components["schemas"]["ReplicationMode"];
export type ConflictResolverType = components["schemas"]["ConflictResolver"];

// ─── Schema types from OpenAPI spec ──────────────────────────────────────────

export type KVEntry = components["schemas"]["KVEntry"];
export type NodeStatus = components["schemas"]["NodeStatus"];
export type Partition = components["schemas"]["Partition"];
export type ClusterState = components["schemas"]["ClusterState"];
export type Scenario = components["schemas"]["Scenario"];
export type KeyDivergence = components["schemas"]["KeyDivergence"];
export type ConvergenceReport = components["schemas"]["ConvergenceReport"];
export type WriteResult = components["schemas"]["WriteResult"];
export type ReadResult = components["schemas"]["ReadResult"];

// Demo report aliases (backend schema names mapped to frontend API surface)
export type DemoRYWResult = components["schemas"]["ReadYourWritesReport"];
export type DemoMonotonicResult = components["schemas"]["MonotonicReadsReport"];
export type DemoPrefixResult = components["schemas"]["ConsistentPrefixReport"];

export type LinearizeResponse = components["schemas"]["LinearizabilityReport"];

// ─── Frontend-only types (no corresponding OpenAPI schema) ───────────────────

export type NodeRole = "leader" | "follower" | "replica" | "primary";
export type NodeState = "online" | "offline" | "paused";

export type EventType =
  | "follower_lag"
  | "conflict_detected"
  | "conflict_resolved"
  | "entry_replicated"
  | "node_state_changed"
  | "partition_created"
  | "partition_healed"
  | "read_repair"
  | "leader_elected"
  | "hinted_handoff"
  | "quorum_achieved"
  | "quorum_failed"
  | "write_received"
  | "read_received";

export interface VectorClock {
  [nodeId: string]: number;
}

// NodeMetrics and LagSample are not part of the OpenAPI spec — the backend
// returns ClusterMetrics as an opaque map; these types describe the known shape.
export interface NodeMetrics {
  node_id: string;
  writes_total: number;
  reads_total: number;
  conflicts_total: number;
  replica_lag: number;
  write_latency_ms: number[];
  read_latency_ms: number[];
  is_leader: boolean;
  is_online: boolean;
  write_p50?: number;
  write_p95?: number;
  write_p99?: number;
  read_p50?: number;
  read_p95?: number;
  read_p99?: number;
}

export interface LagSample {
  follower_id: string;
  lag_entries: number;
  lag_ms: number;
  timestamp: string;
}

// The spec describes ClusterMetrics as an open map; this interface captures the
// known fields without conflicting with the generated opaque type.
export interface ClusterMetrics {
  cluster_id: string;
  strategy: string;
  node_metrics: Record<string, NodeMetrics>;
  total_writes: number;
  total_reads: number;
  total_conflicts: number;
  lag_samples: LagSample[];
  start_time: string;
}

export interface SimEvent {
  type: EventType;
  cluster_id: string;
  node_id?: string;
  timestamp: string;
  data?: Record<string, unknown>;
}

// Store snapshot returned by GET .../nodes/{nodeId}/store — map keyed by key.
export type NodeStoreSnapshot = Record<string, KVEntry>;

// Log snapshot returned by GET .../nodes/{nodeId}/log.
// The spec describes LogEntry as an open map; this interface captures the known fields.
export interface LogEntry {
  index: number;
  term: number;
  key: string;
  value: string; // base64
  op: number | string;
  timestamp: number;
  origin_id: string;
  vclock?: VectorClock;
}

// ─── EPIC B: event history + Jepsen swimlane ────────────────────────────────

export interface HistoryEntry {
  seq: number;
  event: SimEvent;
  state?: ClusterState;
}

export interface HistoryResponse {
  cluster_id: string;
  max_seq: number;
  entries: HistoryEntry[];
}

export interface HistoryStateResponse {
  base_seq: number;
  base_state: ClusterState | null;
  tail: HistoryEntry[];
  max_seq: number;
}

export type JepsenOpKind = "write" | "read";

export interface JepsenOp {
  client_id: string;
  kind: JepsenOpKind;
  key: string;
  value: string;
  invoke_ns: number;
  complete_ns: number;
}

export interface JepsenOpsResponse {
  cluster_id: string;
  ops: JepsenOp[];
}
