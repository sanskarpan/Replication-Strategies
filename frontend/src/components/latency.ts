import type { AppState, Component } from "../core/component";
import type { ClusterState } from "../api/types";
import { req } from "../core/dom";
import { fmtChartMs } from "../core/format";

// ─── Latency percentile charts ──────────────────────────────────────────────────
function renderLatency(cluster: ClusterState) {
  const el = req("latency-chart");
  const nm = Object.values(cluster.metrics.node_metrics || {});
  // Prefer server-computed percentiles; fall back to computing from raw samples.
  const gather = (kind: "write" | "read", p: 50 | 95 | 99): number => {
    const field = `${kind}_p${p}` as keyof typeof nm[number];
    const server = nm.map((m) => (m[field] as number) || 0).filter((v) => v > 0);
    if (server.length) return Math.max(...server);
    const all = nm.flatMap((m) => (kind === "write" ? m.write_latency_ms : m.read_latency_ms) || []);
    if (!all.length) return 0;
    const s = [...all].sort((a, b) => a - b);
    return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))];
  };
  const block = (kind: "write" | "read") => {
    const p50 = gather(kind, 50), p95 = gather(kind, 95), p99 = gather(kind, 99);
    const max = Math.max(p99, 1);
    const bar = (p: "p50" | "p95" | "p99", v: number) => `
      <div class="lat-row">
        <span class="lat-label">${p}</span>
        <span class="lat-bar-track"><span class="lat-bar ${p}" style="width:${Math.min(100, (v / max) * 100)}%"></span></span>
        <span class="lat-val">${fmtChartMs(v)}</span>
      </div>`;
    return `<div class="lat-block">
      <div class="lat-title">${kind} latency</div>
      ${bar("p50", p50)}${bar("p95", p95)}${bar("p99", p99)}
    </div>`;
  };
  el.innerHTML = block("write") + block("read");
}

export const latency: Component = {
  id: "latency",
  render(state: AppState) {
    if (state.active) renderLatency(state.active);
  },
};
