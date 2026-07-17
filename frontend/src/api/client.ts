import type {
  ClusterState,
  Scenario,
  WriteResult,
  ReadResult,
  DemoRYWResult,
  DemoMonotonicResult,
  DemoPrefixResult,
} from "./types";

const BASE = "/api/v1";

async function request<T>(
  method: string,
  path: string,
  body?: unknown
): Promise<T> {
  const opts: RequestInit = { method, headers: { "Content-Type": "application/json" } };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(BASE + path, opts);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`${method} ${path}: ${res.status} ${err}`);
  }
  const text = await res.text();
  return (text ? JSON.parse(text) : null) as T;
}

export const api = {
  // Clusters
  createCluster: (cfg: object) => request<ClusterState>("POST", "/clusters", cfg),
  listClusters: () => request<ClusterState[]>("GET", "/clusters"),
  getCluster: (id: string) => request<ClusterState>("GET", `/clusters/${id}/state`),
  deleteCluster: (id: string) => request<void>("DELETE", `/clusters/${id}`),

  // Simulation
  startSimulation: (cfg: object) => request<ClusterState>("POST", "/simulation/start", cfg),
  resetSimulation: () => request<void>("POST", "/simulation/reset"),
  getSimulationState: () => request<{ clusters: ClusterState[] }>("GET", "/simulation/state"),
  getSimulationMetrics: () => request<Record<string, unknown>>("GET", "/simulation/metrics"),
  updateClusterConfig: (id: string, patch: Record<string, unknown>) =>
    request<ClusterState>("PATCH", `/clusters/${id}/config`, patch),

  // Writes/reads
  write: (id: string, key: string, value: string, clientId: string, targetNodeId?: string) =>
    request<WriteResult>("POST", `/clusters/${id}/write`, { key, value, client_id: clientId, target_node_id: targetNodeId }),
  read: (id: string, key: string, clientId: string, nodeId?: string) => {
    const q = new URLSearchParams({ key, client_id: clientId });
    if (nodeId) q.set("node_id", nodeId);
    return request<ReadResult>("GET", `/clusters/${id}/read?${q.toString()}`);
  },
  writeBatch: (id: string, entries: { key: string; value: string }[], clientId: string) =>
    // The backend returns { results: [...] } where each element is a WriteResult or
    // an { error, key } object — mirror that shape rather than a non-existent errors[].
    request<{ results: Array<WriteResult | { error: string; key: string }> }>(
      "POST", `/clusters/${id}/write-batch`, { entries, client_id: clientId }),
  deleteKey: (id: string, key: string, clientId: string, nodeId?: string) => {
    const q = new URLSearchParams({ key, client_id: clientId });
    if (nodeId) q.set("node_id", nodeId);
    return request<{ status: string; key: string }>("DELETE", `/clusters/${id}/kv?${q.toString()}`);
  },

  // Nodes
  addNode: (id: string) => request("POST", `/clusters/${id}/nodes`),
  removeNode: (id: string, nodeId: string) => request("DELETE", `/clusters/${id}/nodes/${nodeId}`),
  pauseNode: (id: string, nodeId: string) => request("POST", `/clusters/${id}/nodes/${nodeId}/pause`),
  resumeNode: (id: string, nodeId: string) => request("POST", `/clusters/${id}/nodes/${nodeId}/resume`),
  getNodeLog: (id: string, nodeId: string) => request("GET", `/clusters/${id}/nodes/${nodeId}/log`),
  getNodeStore: (id: string, nodeId: string) => request("GET", `/clusters/${id}/nodes/${nodeId}/store`),

  // Network
  injectPartition: (id: string, groupA: string[], groupB: string[]) =>
    request("POST", `/clusters/${id}/network/partition`, { group_a: groupA, group_b: groupB }),
  healPartition: (id: string, partId: string) =>
    request("DELETE", `/clusters/${id}/network/partition/${partId}`),
  setLatency: (id: string, from: string, to: string, ms: number) =>
    request("POST", `/clusters/${id}/network/latency`, { from, to, ms }),
  setDropRate: (id: string, from: string, to: string, rate: number) =>
    request<{ status: string }>("POST", `/clusters/${id}/network/drop`, { from, to, rate }),
  clearFaults: (id: string) => request<{ status: string }>("DELETE", `/clusters/${id}/network/faults`),

  // Consistency demos
  demoReadYourWrites: (id: string) =>
    request<DemoRYWResult>("POST", `/clusters/${id}/demo/read-your-writes`),
  demoMonotonicReads: (id: string) =>
    request<DemoMonotonicResult>("POST", `/clusters/${id}/demo/monotonic-reads`),
  demoConsistentPrefix: (id: string) =>
    request<DemoPrefixResult>("POST", `/clusters/${id}/demo/consistent-prefix`),

  // Scenarios
  listScenarios: () => request<Scenario[]>("GET", "/scenarios"),
  runScenario: (name: string) => request<ClusterState>("POST", `/scenarios/${name}/run`),
};
