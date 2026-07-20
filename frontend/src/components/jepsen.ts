import type { Component, AppState } from "../core/component";
import type { JepsenOp, LinearizeResponse } from "../api/types";
import { store } from "../core/store";
import { api } from "../api/client";
import { byId, req } from "../core/dom";
import * as d3 from "d3";
import { renderGuard } from "../core/component";

// ─── Jepsen Swimlane Component ───────────────────────────────────────────────
// Renders a D3 SVG swimlane (one row per client) showing concurrent read/write
// operations as horizontal bars. A red marker annotates the violating op when
// the linearizability checker detects an anomaly.

const MARGIN = { top: 24, right: 16, bottom: 32, left: 100 };
const ROW_H = 28;
const MIN_H = 120;

const jepsenGuard = renderGuard();
let lastOpsKey = "";

async function refreshJepsen(clusterId: string) {
  let ops: JepsenOp[] = [];
  let lin: LinearizeResponse | null = null;
  try {
    const [opsRes, linRes] = await Promise.all([
      api.getOps(clusterId),
      api.getLinearize(clusterId),
    ]);
    ops = opsRes.ops;
    lin = linRes;
  } catch {
    return; // transient
  }

  const opsKey = ops.length + ":" + (lin?.linearizable ? "ok" : "viol");
  if (!jepsenGuard(opsKey)) return;
  lastOpsKey = opsKey;
  renderSwimlane(ops, lin);
}

function renderSwimlane(ops: JepsenOp[], lin: LinearizeResponse | null) {
  const container = byId("jepsen-body");
  if (!container) return;

  if (!ops.length) {
    container.innerHTML = `<div class="jepsen-empty">No operations recorded yet — run a workload to populate.</div>`;
    return;
  }

  // Gather clients in stable insertion order.
  const clients: string[] = [];
  const seen = new Set<string>();
  for (const op of ops) {
    if (!seen.has(op.client_id)) { seen.add(op.client_id); clients.push(op.client_id); }
  }

  const svgW = container.clientWidth || 700;
  const svgH = Math.max(MIN_H, MARGIN.top + clients.length * ROW_H + MARGIN.bottom);
  const innerW = svgW - MARGIN.left - MARGIN.right;
  const innerH = svgH - MARGIN.top - MARGIN.bottom;

  const minT = d3.min(ops, (d) => d.invoke_ns)!;
  const maxT = d3.max(ops, (d) => d.complete_ns)!;

  const xScale = d3.scaleLinear().domain([minT, maxT]).range([0, innerW]);
  const yScale = d3.scaleBand().domain(clients).range([0, innerH]).padding(0.25);

  // Identify violating op client_id + key for annotation.
  const violKey = lin?.violation ? lin.violation.client_id + "|" + lin.violation.key : null;

  container.innerHTML = "";
  const svg = d3
    .select(container)
    .append("svg")
    .attr("width", svgW)
    .attr("height", svgH)
    .attr("aria-label", "Jepsen operation history swimlane");

  const g = svg
    .append("g")
    .attr("transform", `translate(${MARGIN.left},${MARGIN.top})`);

  // Grid lines.
  g.append("g")
    .attr("class", "jepsen-grid")
    .call(
      d3.axisBottom(xScale)
        .ticks(6)
        .tickFormat((d) => `${Math.round((+d - minT) / 1e6)}ms`)
    )
    .attr("transform", `translate(0,${innerH})`);

  // Client labels (y-axis).
  g.selectAll(".client-label")
    .data(clients)
    .join("text")
    .attr("class", "client-label")
    .attr("x", -8)
    .attr("y", (d) => (yScale(d) ?? 0) + yScale.bandwidth() / 2)
    .attr("dy", "0.35em")
    .attr("text-anchor", "end")
    .attr("fill", "var(--text-dim)")
    .attr("font-size", 11)
    .text((d) => d.length > 12 ? "…" + d.slice(-10) : d);

  // Bars.
  g.selectAll(".op-bar")
    .data(ops)
    .join("rect")
    .attr("class", (d) => `op-bar op-${d.kind}`)
    .attr("x", (d) => xScale(d.invoke_ns))
    .attr("y", (d) => (yScale(d.client_id) ?? 0) + yScale.bandwidth() * 0.1)
    .attr("width", (d) => Math.max(2, xScale(d.complete_ns) - xScale(d.invoke_ns)))
    .attr("height", yScale.bandwidth() * 0.8)
    .attr("rx", 3)
    .attr("fill", (d) => d.kind === "write" ? "var(--accent)" : "var(--accent2)")
    .attr("opacity", (d) => {
      const k = d.client_id + "|" + d.key;
      return violKey && k === violKey ? 1 : 0.75;
    })
    .attr("stroke", (d) => {
      const k = d.client_id + "|" + d.key;
      return violKey && k === violKey ? "var(--danger)" : "none";
    })
    .attr("stroke-width", 2)
    .append("title")
    .text((d) => `${d.kind.toUpperCase()} key=${d.key} val=${d.value || "—"}\nclient=${d.client_id}\n${Math.round((d.complete_ns - d.invoke_ns) / 1e6)}ms`);

  // Violation marker.
  if (violKey && lin?.violation) {
    const vOps = ops.filter((o) => o.client_id + "|" + o.key === violKey);
    for (const vo of vOps) {
      const x = xScale(vo.invoke_ns);
      const y = (yScale(vo.client_id) ?? 0) + yScale.bandwidth() * 0.9 + 2;
      g.append("line")
        .attr("x1", x).attr("y1", y)
        .attr("x2", xScale(vo.complete_ns)).attr("y2", y)
        .attr("stroke", "var(--danger)").attr("stroke-width", 2)
        .attr("stroke-dasharray", "4,2");
      g.append("text")
        .attr("x", x).attr("y", y + 12)
        .attr("fill", "var(--danger)").attr("font-size", 10)
        .text("anomaly");
    }
  }

  // Legend.
  const legend = svg.append("g").attr("transform", `translate(${MARGIN.left},10)`);
  const items = [
    { label: "write", fill: "var(--accent)" },
    { label: "read", fill: "var(--accent2)" },
  ];
  if (violKey) items.push({ label: "anomaly", fill: "var(--danger)" });
  items.forEach((item, i) => {
    legend.append("rect")
      .attr("x", i * 80).attr("y", 0)
      .attr("width", 10).attr("height", 10).attr("rx", 2)
      .attr("fill", item.fill);
    legend.append("text")
      .attr("x", i * 80 + 14).attr("y", 9)
      .attr("fill", "var(--text-dim)").attr("font-size", 10)
      .text(item.label);
  });

  // Linearizability badge.
  const linBadge = byId("jepsen-lin-badge");
  if (linBadge && lin) {
    linBadge.textContent = lin.linearizable ? "✓ linearizable" : "✗ anomaly detected";
    linBadge.className = "jepsen-lin-badge " + (lin.linearizable ? "ok" : "viol");
  }
}

export const jepsen: Component = {
  id: "jepsen",

  mount() {
    byId("jepsen-refresh-btn")?.addEventListener("click", () => {
      const id = store.getActive()?.id;
      if (id) refreshJepsen(id);
    });
  },

  render(state: AppState) {
    const panel = byId("jepsen-panel");
    if (!panel) return;
    if (!state.active) return;
    // Refresh automatically when not replaying (live data), or on first load.
    if (!state.replay) {
      refreshJepsen(state.active.id);
    }
  },
};
