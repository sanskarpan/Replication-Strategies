export type ReplicationStrategy = "single_leader" | "multi_leader" | "leaderless" | "raft";
export type NodeRole = "leader" | "follower" | "replica" | "primary";
export type NodeState = "online" | "offline" | "paused";
export type ReplicationMode = "async" | "sync" | "semi_sync";
export type ConflictResolverType = "lww" | "vector_clock" | "crdt";
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

export interface KVEntry {
  key: string;
  value: string | Uint8Array;
  vclock?: VectorClock;
  timestamp: number;
  node_id: string;
  tombstone?: boolean;
  version: number;
}

export interface NodeStatus {
  id: string;
  cluster_id: string;
  strategy: ReplicationStrategy;
  role: NodeRole;
  state: NodeState;
  commit_index: number;
  last_applied: number;
  leader_id?: string;
  peers: string[];
  lag: number;
}

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
  // Percentiles populated server-side (averages hide tail behaviour).
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

export interface Partition {
  id: string;
  group_a: Record<string, boolean>;
  group_b: Record<string, boolean>;
}

export interface ClusterState {
  id: string;
  config: {
    strategy: ReplicationStrategy;
    node_count: number;
    replication_mode?: ReplicationMode;
    conflict_resolver?: ConflictResolverType;
    quorum_n?: number;
    quorum_w?: number;
    quorum_r?: number;
  };
  node_ids: string[];
  leader_id?: string;
  nodes: Record<string, NodeStatus>;
  metrics: ClusterMetrics;
  created: string;
  partitions: Record<string, Partition>;
  dropped_messages?: number;
}

export interface SimEvent {
  type: EventType;
  cluster_id: string;
  node_id?: string;
  timestamp: string;
  data?: Record<string, unknown>;
}

export interface Scenario {
  name: string;
  strategy: ReplicationStrategy;
  description: string;
  node_count: number;
}

export interface KeyDivergence {
  key: string;
  values: Record<string, string>; // nodeID -> base64 value | "<tombstone>" | "<absent>"
}

export interface ConvergenceReport {
  cluster_id: string;
  converged: boolean;
  keys: number;
  diverged?: KeyDivergence[];
  note?: string;
}

// Store snapshot returned by GET .../nodes/{nodeId}/store — map keyed by key.
export type NodeStoreSnapshot = Record<string, KVEntry>;

// Log snapshot returned by GET .../nodes/{nodeId}/log.
export interface LogEntry {
  index: number;
  term: number;
  key: string;
  value: string; // base64
  op: number | string; // numeric op enum from the backend
  timestamp: number;
  origin_id: string;
  vclock?: VectorClock;
}

export interface WriteResult {
  entry: KVEntry;
  node_id: string;
}

export interface ReadResult {
  entry: KVEntry;
  node_id: string;
}

export interface DemoRYWResult {
  client_id: string;
  write_key: string;
  write_value: string;
  write_result: WriteResult;
  read_result: ReadResult;
  consistent: boolean;
}

export interface DemoMonotonicResult {
  client_id: string;
  read1: ReadResult;
  read2: ReadResult;
  monotonic: boolean;
}

export interface DemoPrefixResult {
  client_id: string;
  writes: WriteResult[];
  prefix: string;
}

// ─── EPIC B: event history + Jepsen swimlane ────────────────────────────────

export interface HistoryEntry {
  seq: number;
  event: SimEvent;
  // Present at periodic snapshot points or after structural events.
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

export interface LinearizeResponse {
  cluster_id: string;
  ops: number;
  linearizable: boolean;
  violation?: {
    client_id: string;
    kind: string;
    key: string;
    value: string;
  };
  note?: string;
}
