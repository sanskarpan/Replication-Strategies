import type { AppState, Component } from "../core/component";
import type { ClusterState } from "../api/types";
import { req } from "../core/dom";
import { avgLatency, pctl } from "../core/format";

// ─── Metric Cards ─────────────────────────────────────────────────────────────
function renderMetrics(cluster: ClusterState) {
  const el = req("metric-cards");
  const m = cluster.metrics;

  // Aggregate avg latency across all nodes
  const allWriteLat = Object.values(m.node_metrics || {}).flatMap((nm) => nm.write_latency_ms || []);
  const allReadLat = Object.values(m.node_metrics || {}).flatMap((nm) => nm.read_latency_ms || []);

  el.innerHTML = `
    <div class="metric-card">
      <div class="metric-value">${m.total_writes}</div>
      <div class="metric-label">Writes</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${m.total_reads}</div>
      <div class="metric-label">Reads</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${m.total_conflicts}</div>
      <div class="metric-label">Conflicts</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${avgLatency(allWriteLat)}</div>
      <div class="metric-label">Avg Write Lat</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${avgLatency(allReadLat)}</div>
      <div class="metric-label">Avg Read Lat</div>
    </div>
    <div class="metric-card" title="99th-percentile write latency (tail)">
      <div class="metric-value">${pctl(allWriteLat, 99)}</div>
      <div class="metric-label">p99 Write Lat</div>
    </div>
    <div class="metric-card" title="Messages dropped due to back-pressure (full queues)">
      <div class="metric-value">${cluster.dropped_messages ?? 0}</div>
      <div class="metric-label">Dropped</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${cluster.node_ids.length}</div>
      <div class="metric-label">Nodes</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">${Object.keys(cluster.partitions || {}).length}</div>
      <div class="metric-label">Partitions</div>
    </div>
  `;
}

export const metrics: Component = {
  id: "metrics",
  render(state: AppState) {
    if (state.active) renderMetrics(state.active);
  },
};
