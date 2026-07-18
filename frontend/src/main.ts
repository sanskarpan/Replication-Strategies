import * as d3 from "d3";
import { api } from "./api/client";
import type { ClusterState, SimEvent, Scenario, VectorClock, NodeStoreSnapshot } from "./api/types";
import { WSClient } from "./ws/client";
import type { WSStatus } from "./ws/client";
import { store } from "./store/simulation";

const reduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;

// ─── Toasts (replace alert()) ──────────────────────────────────────────────────
type ToastKind = "info" | "error" | "success" | "warn";
function toast(message: string, kind: ToastKind = "info", title?: string) {
  const container = document.getElementById("toast-container");
  if (!container) { console.warn(message); return; }
  const el = document.createElement("div");
  el.className = `toast ${kind === "info" ? "" : kind}`.trim();
  el.innerHTML = title ? `<div class="toast-title">${esc(title)}</div>${esc(message)}` : esc(message);
  container.appendChild(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transition = "opacity 0.3s"; setTimeout(() => el.remove(), 300); }, 4000);
}
function esc(s: string): string {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]!));
}
// Central place to surface an operation error as a toast (used by callers that
// previously relied on alert() / silent failure).
function reportError(context: string, e: unknown) {
  const msg = e instanceof Error ? e.message : String(e);
  toast(msg, "error", context);
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
const ws = new WSClient();
ws.on("*", (evt) => {
  store.handleEvent(evt);
  appendEvent(evt);
});
ws.onStatus(renderWSStatus);
ws.connect();

// ─── Real-time consistency-violation banner ─────────────────────────────────
let violationTimer: ReturnType<typeof setTimeout> | null = null;
function flashViolation(message: string, detail = "", nodes: string[] = []) {
  const banner = document.getElementById("violation-banner");
  if (banner) {
    banner.innerHTML = `<span class="vb-icon">⚠</span><span class="vb-msg">${esc(message)}</span>` +
      (detail ? `<span class="vb-detail">${esc(detail)}</span>` : "");
    banner.classList.add("show");
    banner.setAttribute("aria-hidden", "false");
    if (violationTimer) clearTimeout(violationTimer);
    violationTimer = setTimeout(() => {
      banner.classList.remove("show");
      banner.setAttribute("aria-hidden", "true");
    }, 5000);
  }
  // Pulse the offending node circles, if any are on screen.
  if (topoSvg && nodes.length) {
    topoSvg.select(".nodes").selectAll<SVGGElement, TopoNode>("g.node-g")
      .select<SVGCircleElement>("circle")
      .filter((d) => nodes.includes(d.id))
      .classed("violation", true)
      .each(function () {
        const c = this;
        setTimeout(() => c.classList.remove("violation"), 2000);
      });
  }
}

function renderWSStatus(status: WSStatus) {
  const pill = document.getElementById("ws-status");
  if (!pill) return;
  pill.dataset.state = status;
  const label = pill.querySelector(".ws-label");
  if (label) label.textContent = status === "live" ? "live" : status;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
// Go JSON-encodes []byte as base64. Decode "value" fields for display.
function displayResult(result: unknown): string {
  return JSON.stringify(result, (_key, val) => {
    if (_key === "value" && typeof val === "string") {
      try { return atob(val); } catch { return val; }
    }
    return val;
  }, 2);
}

function decodeB64(v: string): string {
  try { return atob(v); } catch { return v; }
}

// Render a vector clock as compact chips instead of raw JSON.
function vcChipsHTML(vc: VectorClock | undefined): string {
  const entries = Object.entries(vc || {}).filter(([, n]) => n > 0);
  if (entries.length === 0) return `<span class="vc-chips empty">∅</span>`;
  return `<span class="vc-chips">` + entries
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, n]) => `<span class="vc-chip"><b>${esc(id.split("-").slice(-1)[0])}</b><span>:${n}</span></span>`)
    .join("") + `</span>`;
}

// ─── Event Log (with filter/search + timeline strip + rolling rate) ──────────
const eventLog = document.getElementById("event-log")!;
let eventCount = 0;

// Filter state, driven by the toolbar text input + type dropdown.
const eventFilter = { text: "", type: "" };
const seenEventTypes = new Set<string>();

// Timeline strip: a rolling list of coloured ticks for recent events.
const timelineEl = document.getElementById("event-timeline");
function timelineClass(type: string): string {
  if (type === "conflict_detected" || type === "conflict_resolved" || type === "quorum_failed") return "conflict";
  if (type === "read_repair" || type === "hinted_handoff") return "repair";
  if (type === "quorum_achieved") return "quorum";
  if (type.startsWith("entry_replicated") || type === "follower_lag") return "replication";
  if (type.startsWith("node_") || type.startsWith("partition_") || type === "leader_elected") return "node";
  return "";
}

// Rolling event-rate: keep timestamps in a 5s window, recompute ev/s on a timer.
const rateWindow: number[] = [];
function pushRate() {
  rateWindow.push(Date.now());
}
function renderEventRate() {
  const now = Date.now();
  while (rateWindow.length && now - rateWindow[0] > 5000) rateWindow.shift();
  const rate = rateWindow.length / 5;
  const el = document.getElementById("event-rate");
  if (!el) return;
  const val = el.querySelector(".event-rate-val");
  if (val) val.textContent = rate.toFixed(1);
  el.classList.toggle("active", rate > 0);
}
setInterval(renderEventRate, 1000);

function matchesFilter(type: string, dataStr: string): boolean {
  if (eventFilter.type && type !== eventFilter.type) return false;
  if (eventFilter.text) {
    const hay = (type + " " + dataStr).toLowerCase();
    if (!hay.includes(eventFilter.text.toLowerCase())) return false;
  }
  return true;
}

function applyEventFilter() {
  eventLog.querySelectorAll<HTMLLIElement>("li").forEach((li) => {
    const type = li.dataset.etype || "";
    const dataStr = li.dataset.edata || "";
    li.classList.toggle("filtered-out", !matchesFilter(type, dataStr));
  });
}

function registerEventType(type: string) {
  if (seenEventTypes.has(type)) return;
  seenEventTypes.add(type);
  const sel = document.getElementById("event-type-filter") as HTMLSelectElement | null;
  if (!sel) return;
  const opt = document.createElement("option");
  opt.value = type;
  opt.textContent = type;
  sel.appendChild(opt);
}

function pushTimelineTick(type: string) {
  if (!timelineEl) return;
  const tick = document.createElement("div");
  tick.className = `tick ${timelineClass(type)}${reduceMotion ? "" : " new"}`.trim();
  tick.title = type;
  timelineEl.appendChild(tick);
  while (timelineEl.children.length > 80) timelineEl.firstChild?.remove();
}

function appendEvent(evt: SimEvent) {
  eventCount++;
  pushRate();
  registerEventType(evt.type);
  pushTimelineTick(evt.type);
  const li = document.createElement("li");
  const ts = new Date(evt.timestamp).toLocaleTimeString();
  const dataStr = evt.data ? JSON.stringify(evt.data).slice(0, 80) : "";
  li.dataset.etype = evt.type;
  li.dataset.edata = dataStr;
  li.innerHTML = `<span class="event-type">${evt.type}</span><span class="event-time">${ts}</span><span class="event-data">${esc(dataStr)}</span>`;
  if (!matchesFilter(evt.type, dataStr)) li.classList.add("filtered-out");
  eventLog.prepend(li);
  // Keep at most 100 entries in the DOM
  while (eventLog.children.length > 100) eventLog.lastChild?.remove();
}

document.getElementById("clear-events-btn")!.addEventListener("click", () => {
  eventLog.innerHTML = "";
  if (timelineEl) timelineEl.innerHTML = "";
});

document.getElementById("event-filter")?.addEventListener("input", (e) => {
  eventFilter.text = (e.target as HTMLInputElement).value;
  applyEventFilter();
});
document.getElementById("event-type-filter")?.addEventListener("change", (e) => {
  eventFilter.type = (e.target as HTMLSelectElement).value;
  applyEventFilter();
});

// ─── D3 Topology ─────────────────────────────────────────────────────────────
interface TopoNode extends d3.SimulationNodeDatum { id: string; role: string; state: string; lag: number; }
interface TopoLink extends d3.SimulationLinkDatum<TopoNode> { lagged?: boolean; }

let topoSim: d3.Simulation<TopoNode, TopoLink> | null = null;
let topoSvg: d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown> | null = null;
// Signature of the current topology *structure* (strategy + node set). The force
// simulation is only rebuilt when this changes; state/lag/partition changes are
// applied in place so the layout settles and nodes stay clickable (no perpetual jiggle).
let topoSig = "";
// Signature of the consistency panel's *shape*. It holds interactive controls
// (the replication-mode dropdown, demo buttons), so we only rebuild it when the
// shape changes — otherwise the 2s poll would wipe an open dropdown / demo result.
let consistencySig = "";

function partitionSet(cluster: ClusterState): Set<string> {
  const partitioned = new Set<string>();
  for (const part of Object.values(cluster.partitions || {})) {
    for (const a of Object.keys(part.group_a)) {
      for (const b of Object.keys(part.group_b)) {
        partitioned.add(`${a}-${b}`);
        partitioned.add(`${b}-${a}`);
      }
    }
  }
  return partitioned;
}

// updateTopoVisuals refreshes node colours, lag labels and link classes without
// touching the running/settled force simulation.
function updateTopoVisuals(cluster: ClusterState) {
  if (!topoSvg) return;
  const partitioned = partitionSet(cluster);
  topoSvg.select(".nodes").selectAll<SVGGElement, TopoNode>("g.node-g").select<SVGCircleElement>("circle")
    .attr("class", (d) => {
      const st = cluster.nodes[d.id];
      const role = st?.role || d.role;
      return `node-circle ${role}${st?.state === "paused" ? " paused" : ""}`;
    });
  topoSvg.select(".nodes").selectAll<SVGGElement, TopoNode>("g.node-g").select<SVGTextElement>("text.node-label + text")
    .text((d) => {
      const lag = cluster.nodes[d.id]?.lag || 0;
      return lag > 0 ? `lag:${lag}` : (cluster.nodes[d.id]?.role || d.role);
    });
  topoSvg.select(".links").selectAll<SVGLineElement, TopoLink>("line")
    .attr("class", (d) => {
      const src = (d.source as TopoNode).id;
      const tgt = (d.target as TopoNode).id;
      const tgtLag = cluster.nodes[tgt]?.lag || 0;
      return `link-line${tgtLag > 2 ? " lagged" : ""}${partitioned.has(`${src}-${tgt}`) ? " partitioned" : " active"}`;
    });
}

function renderTopology(cluster: ClusterState) {
  const container = document.getElementById("topology-body")!;
  const W = container.clientWidth || 300;
  const H = container.clientHeight || 240;

  if (!topoSvg) {
    topoSvg = d3.select("#topology-body").append("svg")
      .attr("width", "100%").attr("height", "100%")
      .attr("viewBox", `0 0 ${W} ${H}`);
    topoSvg.append("g").attr("class", "links");
    topoSvg.append("g").attr("class", "nodes");
  }

  const svg = topoSvg;

  // Only rebuild the simulation when the node structure changes. Otherwise just
  // refresh visuals so the layout settles instead of re-animating every poll.
  const sig = cluster.config.strategy + "|" + [...cluster.node_ids].sort().join(",");
  if (sig === topoSig) {
    updateTopoVisuals(cluster);
    return;
  }
  topoSig = sig;

  const nodes: TopoNode[] = cluster.node_ids.map((id) => ({
    id,
    role: cluster.nodes[id]?.role || "replica",
    state: cluster.nodes[id]?.state || "online",
    lag: cluster.nodes[id]?.lag || 0,
  }));

  const links: TopoLink[] = [];
  const strategy = cluster.config.strategy;

  if (strategy === "single_leader" || strategy === "raft") {
    const leader = nodes.find((n) => n.role === "leader");
    if (leader) {
      nodes.filter((n) => n.role !== "leader").forEach((f) => {
        links.push({ source: leader.id, target: f.id, lagged: (f.lag || 0) > 2 });
      });
    }
  } else if (strategy === "multi_leader") {
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        links.push({ source: nodes[i].id, target: nodes[j].id });
      }
    }
  } else {
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        links.push({ source: nodes[i].id, target: nodes[j].id });
      }
    }
  }

  // Partition overlay
  const partitioned = partitionSet(cluster);

  if (topoSim) topoSim.stop();

  topoSim = d3.forceSimulation(nodes)
    .force("link", d3.forceLink<TopoNode, TopoLink>(links).id((d) => d.id).distance(80))
    .force("charge", d3.forceManyBody().strength(-200))
    .force("center", d3.forceCenter(W / 2, H / 2))
    .force("collision", d3.forceCollide(30));

  const linkG = svg.select<SVGGElement>(".links");
  const nodeG = svg.select<SVGGElement>(".nodes");

  const linkSel = linkG.selectAll<SVGLineElement, TopoLink>("line")
    .data(links, (d) => `${(d.source as TopoNode).id}-${(d.target as TopoNode).id}`)
    .join("line")
    .attr("class", (d) => {
      const src = (d.source as TopoNode).id;
      const tgt = (d.target as TopoNode).id;
      const isPartitioned = partitioned.has(`${src}-${tgt}`);
      return `link-line${d.lagged ? " lagged" : ""}${isPartitioned ? " partitioned" : " active"}`;
    });

  const nodeSel = nodeG.selectAll<SVGGElement, TopoNode>("g.node-g")
    .data(nodes, (d) => d.id)
    .join((enter) => {
      const g = enter.append("g").attr("class", "node-g");
      g.append("circle").attr("r", 20).attr("class", (d) => `node-circle ${d.role}${d.state === "paused" ? " paused" : ""}`);
      g.append("text").attr("class", "node-label").attr("dy", "0.35em");
      g.append("text").attr("class", "node-label").attr("dy", "2.2em").attr("fill", "var(--text-dim)").style("font-size", "9px");
      // Inspect affordance: a small badge top-right of the node. Clicking it opens
      // the inspector drawer WITHOUT toggling pause (which stays on the node body).
      const insp = g.append("g").attr("class", "node-inspect-btn").attr("transform", "translate(15,-15)");
      insp.append("circle").attr("r", 8).attr("fill", "var(--surface2)").attr("stroke", "var(--border)").attr("stroke-width", 1);
      insp.append("text").attr("class", "node-label").attr("dy", "0.32em").style("font-size", "9px").style("pointer-events", "none").text("🔍");
      return g;
    });

  // Inspect button opens the drawer; stopPropagation keeps pause-on-node-click intact.
  nodeSel.select<SVGGElement>("g.node-inspect-btn").on("click", (event, d) => {
    event.stopPropagation();
    openInspector(cluster.id, d.id);
  });

  nodeSel.select<SVGCircleElement>("circle")
    .attr("class", (d) => `node-circle ${d.role}${d.state === "paused" ? " paused" : ""}`);
  nodeSel.select<SVGTextElement>("text.node-label:first-of-type")
    .text((d) => d.id.split("-").slice(-1)[0]);
  nodeSel.select<SVGTextElement>("text.node-label + text")
    .text((d) => d.lag > 0 ? `lag:${d.lag}` : d.role);

  // Click on node to pause/resume
  nodeSel.on("click", (event, d) => {
    const clusterId = cluster.id;
    if (d.state === "paused") {
      api.resumeNode(clusterId, d.id).then(() => store.refreshCluster(clusterId));
    } else {
      api.pauseNode(clusterId, d.id).then(() => store.refreshCluster(clusterId));
    }
  });

  topoSim.on("tick", () => {
    linkSel
      .attr("x1", (d) => (d.source as TopoNode).x!)
      .attr("y1", (d) => (d.source as TopoNode).y!)
      .attr("x2", (d) => (d.target as TopoNode).x!)
      .attr("y2", (d) => (d.target as TopoNode).y!);
    nodeSel.attr("transform", (d) => `translate(${d.x!},${d.y!})`);
    // Keep a live position lookup for packet animation.
    nodePos.clear();
    for (const n of nodes) if (n.x != null && n.y != null) nodePos.set(n.id, { x: n.x, y: n.y });
  });
}

// ─── Animated message-passing packets ──────────────────────────────────────────
// Live node positions from the force sim, used to tween dots along links.
const nodePos = new Map<string, { x: number; y: number }>();

function packetColor(type: string): string {
  if (type === "read_repair") return "var(--accent2)";
  if (type === "hinted_handoff") return "var(--warn)";
  return "var(--accent)"; // replication
}

// Animate a dot from `from` to `to`. If `dropped`, fade it out partway.
function animatePacket(from: string, to: string, type: string, dropped = false) {
  if (reduceMotion || !topoSvg) return;
  const a = nodePos.get(from);
  const b = nodePos.get(to);
  if (!a || !b) return;
  const layer = topoSvg.select<SVGGElement>(".nodes"); // draw above links
  const dot = layer.append("circle")
    .attr("class", "packet")
    .attr("r", 4)
    .attr("fill", packetColor(type))
    .attr("cx", a.x).attr("cy", a.y)
    .attr("opacity", 0.95);
  const t = dot.transition().duration(700).ease(d3.easeLinear);
  if (dropped) {
    t.attr("cx", (a.x + b.x) / 2).attr("cy", (a.y + b.y) / 2).attr("opacity", 0)
      .on("end", () => dot.remove());
  } else {
    t.attr("cx", b.x).attr("cy", b.y)
      .transition().duration(120).attr("opacity", 0)
      .on("end", () => dot.remove());
  }
}

// Resolve source/target node ids for a replication-ish event, then animate.
function packetForEvent(evt: SimEvent) {
  const cluster = store.getActive();
  if (!cluster || evt.cluster_id !== cluster.id) return;
  const d = evt.data || {};
  const from = (d.from as string) || (d.leader_id as string) || cluster.leader_id || evt.node_id;
  const to = (d.to as string) || (d.follower_id as string) || (d.target as string) || (d.node_id as string);
  const dropped = (evt.type as string) === "message_dropped" || d.dropped === true;
  if (evt.type === "read_repair" && Array.isArray(d.stale_nodes)) {
    for (const sn of d.stale_nodes as string[]) if (from) animatePacket(from, sn, "read_repair", dropped);
    return;
  }
  if (from && to) animatePacket(from, to, evt.type, dropped);
  else if (from) {
    // Broadcast to all followers when target is unspecified.
    for (const nid of cluster.node_ids) if (nid !== from) animatePacket(from, nid, evt.type, dropped);
  }
}

// ─── Lag Timeline ─────────────────────────────────────────────────────────────
interface LagDataPoint { time: Date; follower: string; lag: number; }

const lagData: LagDataPoint[] = [];
let lagSvg: d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown> | null = null;

ws.on("follower_lag", (evt) => {
  lagData.push({
    time: new Date(evt.timestamp),
    follower: (evt.data?.follower_id as string) || evt.node_id || "?",
    lag: (evt.data?.lag_entries as number) || 0,
  });
  if (lagData.length > 200) lagData.shift();
  renderLagTimeline();
});

function renderLagTimeline() {
  const container = document.getElementById("lag-body")!;
  const W = container.clientWidth || 300;
  const H = container.clientHeight || 200;

  if (!lagSvg) {
    lagSvg = d3.select("#lag-body").append("svg")
      .attr("width", "100%").attr("height", "100%")
      .attr("viewBox", `0 0 ${W} ${H}`);
  }

  const margin = { top: 10, right: 10, bottom: 20, left: 35 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const followers = [...new Set(lagData.map((d) => d.follower))];
  const color = d3.scaleOrdinal(d3.schemeTableau10).domain(followers);

  const xExtent = d3.extent(lagData, (d) => d.time) as [Date, Date];
  const x = d3.scaleTime().domain(xExtent.every(Boolean) ? xExtent : [new Date(Date.now() - 60000), new Date()]).range([0, w]);
  const y = d3.scaleLinear().domain([0, d3.max(lagData, (d) => d.lag) || 10]).range([h, 0]);

  lagSvg.selectAll("*").remove();
  const g = lagSvg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  g.append("g").attr("transform", `translate(0,${h})`).call(d3.axisBottom(x).ticks(5).tickFormat(d3.timeFormat("%H:%M:%S") as (d: d3.AxisDomain) => string));
  g.append("g").call(d3.axisLeft(y).ticks(4));

  const line = d3.line<LagDataPoint>().x((d) => x(d.time)).y((d) => y(d.lag)).curve(d3.curveMonotoneX);

  followers.forEach((f) => {
    const fData = lagData.filter((d) => d.follower === f);
    g.append("path").datum(fData).attr("fill", "none")
      .attr("stroke", color(f)).attr("stroke-width", 2)
      .attr("d", line);
  });

  // Legend
  const legend = document.getElementById("lag-legend")!;
  legend.innerHTML = followers.map((f) => `
    <div class="lag-legend-item">
      <div class="lag-legend-dot" style="background:${color(f)}"></div>
      <span>${f.split("-").slice(-1)[0]}</span>
    </div>
  `).join("");
}

// ─── Conflict Log ─────────────────────────────────────────────────────────────
const conflictsBody = document.getElementById("conflicts-body")!;
const conflictCount = document.getElementById("conflict-count")!;
let totalConflicts = 0;

ws.on("conflict_detected", (evt) => {
  totalConflicts++;
  conflictCount.textContent = `${totalConflicts} conflicts`;
  const d = evt.data || {};
  const div = document.createElement("div");
  div.className = "conflict-entry";
  div.innerHTML = `
    <div class="key">${d.key || "?"}</div>
    <div class="vclock">local: ${JSON.stringify(d.local_vc || {})} | remote: ${JSON.stringify(d.remote_vc || {})}</div>
    <div style="color:var(--text-dim);font-size:10px">${new Date(evt.timestamp).toLocaleTimeString()}</div>
  `;
  conflictsBody.prepend(div);
  if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
});

ws.on("conflict_resolved", (evt) => {
  const d = evt.data || {};
  const first = conflictsBody.firstElementChild;
  if (first) {
    const res = document.createElement("div");
    res.className = "resolver";
    res.style.cssText = "color:var(--accent2);font-size:10px;margin-top:2px";
    res.textContent = `resolved: ${d.resolver} — ${d.reason || ""}`;
    first.appendChild(res);
  }
});

// ─── Quorum / Read-Repair events ─────────────────────────────────────────────
ws.on("quorum_failed", (evt) => {
  const d = evt.data || {};
  const div = document.createElement("div");
  div.className = "conflict-entry";
  div.style.cssText = "border-left:3px solid var(--danger)";
  div.innerHTML = `<div class="key" style="color:var(--danger)">Quorum FAILED: ${d.key || "?"}</div>
    <div style="font-size:10px;color:var(--text-dim)">acked ${d.acked}/${d.w} required</div>`;
  conflictsBody.prepend(div);
  if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
  // Surface as a real-time consistency-violation banner.
  const nodes = (evt.node_id ? [evt.node_id] : []).concat(
    Array.isArray(d.nodes) ? (d.nodes as string[]) : []);
  flashViolation(
    `Quorum failed for "${d.key ?? "?"}"`,
    d.acked != null && d.w != null ? `acked ${d.acked}/${d.w} required` : "",
    nodes,
  );
});

// Any event that carries an explicit invariant/violation payload also flashes the banner.
ws.on("*", (evt) => {
  const d = evt.data || {};
  const viol = d.violations ?? d.violation;
  const hasViol = (Array.isArray(viol) && viol.length > 0) || (typeof viol === "string" && viol) ||
    (d.consistent === false) || (d.invariant_violated === true);
  if (!hasViol || evt.type === "quorum_failed") return;
  const msg = Array.isArray(viol) ? `${viol.length} invariant violation(s)` :
    typeof viol === "string" ? viol : "Consistency invariant violated";
  const nodes = evt.node_id ? [evt.node_id] : (Array.isArray(d.nodes) ? (d.nodes as string[]) : []);
  flashViolation(msg, String(evt.type), nodes);
});

ws.on("read_repair", (evt) => {
  const d = evt.data || {};
  if (!d.stale_nodes) return; // receiving end; skip
  const div = document.createElement("div");
  div.className = "conflict-entry";
  div.style.cssText = "border-left:3px solid var(--accent2)";
  div.innerHTML = `<div class="key" style="color:var(--accent2)">Read Repair: ${d.key || "?"}</div>
    <div style="font-size:10px;color:var(--text-dim)">stale: ${JSON.stringify(d.stale_nodes)}</div>`;
  conflictsBody.prepend(div);
  if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
});

// Animate packets for replication / repair / handoff events on the WS stream.
for (const t of ["entry_replicated", "read_repair", "hinted_handoff"] as const) {
  ws.on(t, (evt) => packetForEvent(evt));
}

// ─── Quorum Visualizer ────────────────────────────────────────────────────────
function renderQuorum(cluster: ClusterState) {
  const el = document.getElementById("quorum-content")!;
  if (cluster.config.strategy !== "leaderless") {
    el.innerHTML = `<div style="color:var(--text-dim);padding:16px;text-align:center">Quorum only for leaderless strategy</div>`;
    return;
  }
  const N = cluster.config.quorum_n || 5;
  const W = cluster.config.quorum_w || 3;
  const R = cluster.config.quorum_r || 3;
  const strong = (W + R) > N;
  const overlap = Math.max(0, W + R - N);

  el.innerHTML = `
    <div style="display:flex;gap:12px;align-items:center;padding:8px">
      <svg width="160" height="120" viewBox="0 0 160 120">
        <!-- Write circle -->
        <circle cx="55" cy="60" r="45" fill="rgba(88,166,255,0.15)" stroke="var(--accent)" stroke-width="1.5"/>
        <!-- Read circle -->
        <circle cx="105" cy="60" r="45" fill="rgba(63,185,80,0.15)" stroke="var(--accent2)" stroke-width="1.5"/>
        <text x="35" y="63" fill="var(--accent)" font-size="12" font-weight="700">W=${W}</text>
        <text x="108" y="63" fill="var(--accent2)" font-size="12" font-weight="700">R=${R}</text>
        <text x="75" y="63" fill="var(--text)" font-size="11">${overlap}</text>
        <text x="75" y="75" fill="var(--text-dim)" font-size="9">overlap</text>
        <text x="80" y="108" fill="var(--text-dim)" font-size="9" text-anchor="middle">N=${N}</text>
      </svg>
      <div style="display:flex;flex-direction:column;gap:6px">
        <div><span style="color:var(--text-dim)">N = </span><strong>${N}</strong></div>
        <div><span style="color:var(--text-dim)">W = </span><strong style="color:var(--accent)">${W}</strong></div>
        <div><span style="color:var(--text-dim)">R = </span><strong style="color:var(--accent2)">${R}</strong></div>
        <div><span style="color:var(--text-dim)">W+R = </span><strong>${W+R}</strong></div>
        <div><span class="consistency-badge ${strong ? 'strong' : 'eventual'}">${strong ? 'Strong' : 'Eventual'}</span></div>
      </div>
    </div>
    <div style="padding:0 12px;font-size:10px;color:var(--text-dim)">
      ${strong ? `W+R=${W+R} > N=${N}: guaranteed overlap of ${overlap} node(s)` : `W+R=${W+R} <= N=${N}: stale reads possible`}
    </div>
    <div class="quorum-sliders">
      <div class="slider-row"><span class="slider-label">N</span>
        <input type="range" id="quorum-slider-n" min="1" max="9" value="${N}" />
        <span class="slider-val" id="quorum-val-n">${N}</span></div>
      <div class="slider-row"><span class="slider-label">W</span>
        <input type="range" id="quorum-slider-w" min="1" max="9" value="${W}" />
        <span class="slider-val" id="quorum-val-w">${W}</span></div>
      <div class="slider-row"><span class="slider-label">R</span>
        <input type="range" id="quorum-slider-r" min="1" max="9" value="${R}" />
        <span class="slider-val" id="quorum-val-r">${R}</span></div>
      <div class="quorum-verdict" id="quorum-verdict"></div>
    </div>
  `;

  // Wire the interactive sliders. Config PATCH only accepts replication_mode
  // server-side, so these visualize live overlap + estimated stale-read probability
  // without mutating backend state.
  const sn = document.getElementById("quorum-slider-n") as HTMLInputElement;
  const sw = document.getElementById("quorum-slider-w") as HTMLInputElement;
  const sr = document.getElementById("quorum-slider-r") as HTMLInputElement;
  const updateVerdict = () => {
    const n = +sn.value, w = +sw.value, r = +sr.value;
    (document.getElementById("quorum-val-n")!).textContent = String(n);
    (document.getElementById("quorum-val-w")!).textContent = String(w);
    (document.getElementById("quorum-val-r")!).textContent = String(r);
    const strongNow = (w + r) > n;
    // Simple stale-read estimate: probability a read set misses the newest write set
    // = fraction of read placements that fall entirely outside the W most-recent
    // replicas. Approximated as max(0, (n-w)/n)^(effective read shortfall).
    const staleP = strongNow ? 0 : Math.max(0, (n - w) / n) * Math.max(0, (n - r + 1) / n);
    const verdict = document.getElementById("quorum-verdict")!;
    verdict.className = `quorum-verdict ${strongNow ? "strong" : "eventual"}`;
    verdict.innerHTML = strongNow
      ? `W+R=${w + r} &gt; N=${n} → strongly consistent · stale-read ≈ 0%`
      : `W+R=${w + r} ≤ N=${n} → eventual · stale-read ≈ ${(staleP * 100).toFixed(0)}%`;
  };
  [sn, sw, sr].forEach((s) => s.addEventListener("input", updateVerdict));
  updateVerdict();
}

// ─── Consistency Panel ────────────────────────────────────────────────────────
function renderConsistency(cluster: ClusterState) {
  const el = document.getElementById("consistency-body")!;
  const strategy = cluster.config.strategy;

  // Only rebuild when the panel shape changes (strategy/mode/resolver/quorum). This
  // keeps interactive controls and demo results alive across the 2s poll.
  const sig = [cluster.id, strategy, cluster.config.replication_mode, cluster.config.conflict_resolver,
    cluster.config.quorum_n, cluster.config.quorum_w, cluster.config.quorum_r].join("|");
  if (sig === consistencySig && el.querySelector(".consistency-result")) return;
  consistencySig = sig;

  // Don't wipe the result area if it already exists (user may have clicked a demo)
  const existing = el.querySelector(".consistency-result") as HTMLElement | null;

  let html = `<div style="padding:8px;font-size:11px">`;

  if (strategy === "single_leader") {
    const mode = cluster.config.replication_mode || "async";
    html += `
      <div style="margin-bottom:6px;font-weight:600;color:var(--accent)">Single-Leader Guarantees</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <div style="display:flex;justify-content:space-between"><span>Read-Your-Writes</span><span class="consistency-badge strong">✓ Leader reads</span></div>
        <div style="display:flex;justify-content:space-between"><span>Monotonic Reads</span><span class="consistency-badge strong">✓ Tracked</span></div>
        <div style="display:flex;justify-content:space-between"><span>Consistent Prefix</span><span class="consistency-badge strong">✓ Log order</span></div>
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span>Replication Mode</span>
          <select id="repl-mode-live" style="font-size:10px;padding:1px 4px;background:var(--surface);color:var(--text);border:1px solid var(--border)">
            <option value="async" ${mode==="async"?"selected":""}>async</option>
            <option value="sync" ${mode==="sync"?"selected":""}>sync</option>
            <option value="semi_sync" ${mode==="semi_sync"?"selected":""}>semi-sync</option>
          </select>
        </div>
      </div>
      <div style="margin-top:8px;display:flex;gap:4px;flex-wrap:wrap">
        <button class="btn btn-sm" id="demo-ryw-btn">RYW Demo</button>
        <button class="btn btn-sm" id="demo-mono-btn">Monotonic Demo</button>
        <button class="btn btn-sm" id="demo-prefix-btn">Prefix Demo</button>
      </div>
    `;
  } else if (strategy === "multi_leader") {
    html += `
      <div style="margin-bottom:6px;font-weight:600;color:var(--primary)">Multi-Leader Guarantees</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <div style="display:flex;justify-content:space-between"><span>Read-Your-Writes</span><span class="consistency-badge eventual">Per-node only</span></div>
        <div style="display:flex;justify-content:space-between"><span>Monotonic Reads</span><span class="consistency-badge eventual">Not guaranteed</span></div>
        <div style="display:flex;justify-content:space-between"><span>Conflict Resolver</span><span style="color:var(--accent)">${cluster.config.conflict_resolver || "lww"}</span></div>
      </div>
      <div style="margin-top:8px;display:flex;gap:4px">
        <button class="btn btn-sm" id="demo-ryw-btn">RYW Demo</button>
      </div>
    `;
  } else {
    const N = cluster.config.quorum_n || 5;
    const W = cluster.config.quorum_w || 3;
    const R = cluster.config.quorum_r || 3;
    const strong = (W + R) > N;
    html += `
      <div style="margin-bottom:6px;font-weight:600;color:var(--replica)">Leaderless Guarantees</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <div style="display:flex;justify-content:space-between"><span>Strong Consistency</span><span class="consistency-badge ${strong ? 'strong' : 'eventual'}">${strong ? '✓ W+R>N' : '✗ W+R≤N'}</span></div>
        <div style="display:flex;justify-content:space-between"><span>Read Repair</span><span class="consistency-badge strong">✓ Active</span></div>
        <div style="display:flex;justify-content:space-between"><span>Hinted Handoff</span><span class="consistency-badge strong">✓ Active</span></div>
      </div>
    `;
  }

  html += `<div class="consistency-result" style="margin-top:6px;font-size:10px;font-family:monospace;color:var(--text-dim);word-break:break-all;max-height:80px;overflow-y:auto"></div>`;
  html += `</div>`;
  el.innerHTML = html;

  // Re-attach result content if it was there before
  if (existing?.textContent) {
    (el.querySelector(".consistency-result") as HTMLElement).textContent = existing.textContent;
  }

  const resultEl = () => el.querySelector(".consistency-result") as HTMLElement;

  // Live replication mode toggle (single-leader only)
  const replModeSel = document.getElementById("repl-mode-live") as HTMLSelectElement | null;
  replModeSel?.addEventListener("change", async () => {
    const newMode = replModeSel.value;
    await api.updateClusterConfig(cluster.id, { replication_mode: newMode });
    store.refreshCluster(cluster.id);
  });

  // Demo buttons
  document.getElementById("demo-ryw-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoReadYourWrites(cluster.id);
      resultEl().textContent = `RYW: wrote "${res.write_value}" → read consistent=${res.consistent}`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });

  document.getElementById("demo-mono-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoMonotonicReads(cluster.id);
      const v1 = res.read1?.entry?.value ? atob(res.read1.entry.value as string) : "?";
      const v2 = res.read2?.entry?.value ? atob(res.read2.entry.value as string) : "?";
      resultEl().textContent = `Monotonic: read1="${v1}" → read2="${v2}" monotonic=${res.monotonic}`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });

  document.getElementById("demo-prefix-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoConsistentPrefix(cluster.id);
      resultEl().textContent = `Prefix: wrote ${res.writes.length} keys in order, consistency="${res.prefix}"`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });
}

// ─── Metric Cards ─────────────────────────────────────────────────────────────
function avgLatency(samples: number[]): string {
  if (!samples || samples.length === 0) return "—";
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  return avg < 1 ? "<1ms" : `${avg.toFixed(1)}ms`;
}

function pctl(samples: number[], p: number): string {
  if (!samples || samples.length === 0) return "—";
  const s = [...samples].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil((p / 100) * s.length) - 1);
  const v = s[Math.min(rank, s.length - 1)];
  return v < 1 ? "<1ms" : `${v.toFixed(1)}ms`;
}

function renderMetrics(cluster: ClusterState) {
  const el = document.getElementById("metric-cards")!;
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

// ─── Control Panel ────────────────────────────────────────────────────────────
function renderControl() {
  const el = document.getElementById("control-body")!;

  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:8px;padding:4px 0">
      <!-- Create cluster -->
      <div class="form-row">
        <div class="form-group">
          <label>Strategy</label>
          <select id="strategy-select">
            <option value="single_leader">Single-Leader</option>
            <option value="raft">Raft (consensus)</option>
            <option value="multi_leader">Multi-Leader</option>
            <option value="leaderless">Leaderless</option>
          </select>
        </div>
        <div class="form-group">
          <label>Nodes</label>
          <input type="number" id="node-count-input" value="4" min="2" max="10" style="width:60px" />
        </div>
        <div class="form-group" id="repl-mode-group">
          <label>Repl. Mode</label>
          <select id="repl-mode-select">
            <option value="async">Async</option>
            <option value="sync">Sync</option>
            <option value="semi_sync">Semi-Sync</option>
          </select>
        </div>
        <div class="form-group" id="resolver-group" style="display:none">
          <label>Conflict Resolver</label>
          <select id="resolver-select">
            <option value="lww">LWW</option>
            <option value="vector_clock">Vector Clock</option>
            <option value="crdt">CRDT</option>
          </select>
        </div>
        <div class="form-group" id="quorum-group" style="display:none">
          <label>W / R</label>
          <div style="display:flex;gap:4px">
            <input type="number" id="quorum-w" value="3" min="1" max="5" style="width:45px" />
            <input type="number" id="quorum-r" value="3" min="1" max="5" style="width:45px" />
          </div>
        </div>
        <button class="btn btn-primary" id="create-cluster-btn">Create Cluster</button>
        <button class="btn" id="add-node-btn" title="Add a node to the active cluster">Add Node</button>
        <button class="btn btn-danger" id="remove-node-btn" title="Remove the last node from the active cluster">Remove Node</button>
        <button class="btn btn-danger" id="reset-btn">Reset All</button>
      </div>
    </div>

    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px">
      <!-- Write / Read -->
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="form-row">
          <div class="form-group">
            <label>Key</label>
            <input type="text" id="write-key" value="mykey" style="width:80px" />
          </div>
          <div class="form-group">
            <label>Value</label>
            <input type="text" id="write-value" value="hello" style="width:80px" />
          </div>
          <div class="form-group">
            <label>Client ID</label>
            <input type="text" id="client-id" value="client1" style="width:70px" />
          </div>
          <button class="btn" id="write-btn">Write</button>
          <button class="btn" id="read-btn">Read</button>
          <button class="btn btn-danger" id="delete-btn">Delete</button>
        </div>
        <div id="rw-result" style="font-size:11px;color:var(--text-dim);font-family:monospace;max-width:400px;word-break:break-all"></div>
      </div>

      <!-- Network faults -->
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="form-row">
          <div class="form-group">
            <label>Latency (ms)</label>
            <input type="number" id="latency-ms" value="500" style="width:60px" />
          </div>
          <div class="form-group">
            <label>Drop %</label>
            <input type="number" id="drop-rate" value="30" min="0" max="100" style="width:52px" />
          </div>
          <button class="btn" id="add-latency-btn" title="Add latency to last follower/node">Add Lag</button>
          <button class="btn" id="add-drop-btn" title="Add packet drop rate to last follower/node">Add Drop</button>
          <button class="btn" id="partition-btn" title="Partition first half vs second half">Partition</button>
          <button class="btn" id="clear-faults-btn">Clear Faults</button>
        </div>
        <div id="partition-list" style="font-size:10px;color:var(--text-dim);display:flex;gap:6px;flex-wrap:wrap"></div>
      </div>

      <!-- Scenarios -->
      <div>
        <select id="scenario-select" style="width:180px">
          <option value="">Select scenario...</option>
        </select>
        <button class="btn btn-primary" id="run-scenario-btn" style="margin-left:4px">Run</button>
      </div>
    </div>
  `;

  // Load scenarios
  api.listScenarios().then((scenarios: Scenario[]) => {
    const sel = document.getElementById("scenario-select") as HTMLSelectElement;
    for (const s of scenarios) {
      const opt = document.createElement("option");
      opt.value = s.name;
      opt.textContent = `${s.name} (${s.strategy})`;
      sel.appendChild(opt);
    }
  });

  // Strategy select -> show/hide fields
  const stratSel = document.getElementById("strategy-select") as HTMLSelectElement;
  stratSel.addEventListener("change", () => {
    const v = stratSel.value;
    (document.getElementById("repl-mode-group") as HTMLElement).style.display = v === "single_leader" ? "" : "none";
    (document.getElementById("resolver-group") as HTMLElement).style.display = v === "multi_leader" ? "" : "none";
    (document.getElementById("quorum-group") as HTMLElement).style.display = v === "leaderless" ? "" : "none";
  });

  // Create cluster
  document.getElementById("create-cluster-btn")!.addEventListener("click", async () => {
    const strategy = stratSel.value;
    const nodeCount = parseInt((document.getElementById("node-count-input") as HTMLInputElement).value);
    const cfg: Record<string, unknown> = { strategy, node_count: nodeCount };
    if (strategy === "single_leader") {
      cfg.replication_mode = (document.getElementById("repl-mode-select") as HTMLSelectElement).value;
    } else if (strategy === "multi_leader") {
      cfg.conflict_resolver = (document.getElementById("resolver-select") as HTMLSelectElement).value;
    } else {
      cfg.quorum_n = nodeCount;
      cfg.quorum_w = parseInt((document.getElementById("quorum-w") as HTMLInputElement).value);
      cfg.quorum_r = parseInt((document.getElementById("quorum-r") as HTMLInputElement).value);
    }
    try {
      const cluster = await api.startSimulation(cfg);
      store.clusters.set(cluster.id, cluster);
      store.activeClusterId = cluster.id;
      store.notify();
      refresh();
      toast(`${strategy} cluster created (${nodeCount} nodes)`, "success");
    } catch (e) {
      reportError("Create cluster", e);
    }
  });

  // Add node
  document.getElementById("add-node-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    await api.addNode(cluster.id);
    await store.refreshCluster(cluster.id);
    refresh();
  });

  // Remove node
  document.getElementById("remove-node-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    if (!lastNode) return;
    await api.removeNode(cluster.id, lastNode);
    await store.refreshCluster(cluster.id);
    refresh();
  });

  // Reset
  document.getElementById("reset-btn")!.addEventListener("click", async () => {
    await api.resetSimulation();
    store.clusters.clear();
    store.activeClusterId = null;
    store.notify();
    refresh();
  });

  // Write
  document.getElementById("write-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (document.getElementById("write-key") as HTMLInputElement).value;
    const value = (document.getElementById("write-value") as HTMLInputElement).value;
    const clientId = (document.getElementById("client-id") as HTMLInputElement).value;
    const rwResult = document.getElementById("rw-result")!;
    try {
      const result = await api.write(cluster.id, key, value, clientId);
      rwResult.textContent = displayResult(result);
      store.refreshCluster(cluster.id);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Read
  document.getElementById("read-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (document.getElementById("write-key") as HTMLInputElement).value;
    const clientId = (document.getElementById("client-id") as HTMLInputElement).value;
    const rwResult = document.getElementById("rw-result")!;
    try {
      const result = await api.read(cluster.id, key, clientId);
      rwResult.textContent = displayResult(result);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Delete
  document.getElementById("delete-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (document.getElementById("write-key") as HTMLInputElement).value;
    const clientId = (document.getElementById("client-id") as HTMLInputElement).value;
    const rwResult = document.getElementById("rw-result")!;
    try {
      const result = await api.deleteKey(cluster.id, key, clientId);
      rwResult.textContent = displayResult(result);
      store.refreshCluster(cluster.id);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Add latency
  document.getElementById("add-latency-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster || cluster.node_ids.length < 2) return;
    const ms = parseInt((document.getElementById("latency-ms") as HTMLInputElement).value);
    const src = cluster.leader_id || cluster.node_ids[0];
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    await api.setLatency(cluster.id, src, lastNode, ms);
  });

  // Add drop rate
  document.getElementById("add-drop-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster || cluster.node_ids.length < 2) return;
    const pct = parseInt((document.getElementById("drop-rate") as HTMLInputElement).value);
    const rate = Math.max(0, Math.min(1, pct / 100));
    const src = cluster.leader_id || cluster.node_ids[0];
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    await api.setDropRate(cluster.id, src, lastNode, rate);
  });

  // Partition
  document.getElementById("partition-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return;
    const half = Math.floor(cluster.node_ids.length / 2);
    const groupA = cluster.node_ids.slice(0, half);
    const groupB = cluster.node_ids.slice(half);
    await api.injectPartition(cluster.id, groupA, groupB);
    store.refreshCluster(cluster.id);
    renderPartitionList(cluster.id);
  });

  // Clear faults
  document.getElementById("clear-faults-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return;
    await api.clearFaults(cluster.id);
    store.refreshCluster(cluster.id);
    const pl = document.getElementById("partition-list");
    if (pl) pl.innerHTML = "";
  });

  // Run scenario
  document.getElementById("run-scenario-btn")!.addEventListener("click", async () => {
    const name = (document.getElementById("scenario-select") as HTMLSelectElement).value;
    if (!name) return;
    const cluster = await api.runScenario(name);
    store.clusters.set(cluster.id, cluster);
    store.activeClusterId = cluster.id;
    store.notify();
    refresh();
  });
}

// ─── Partition List ───────────────────────────────────────────────────────────
function renderPartitionList(clusterId: string) {
  const cluster = store.getActive();
  const el = document.getElementById("partition-list");
  if (!el || !cluster) return;
  const parts = Object.values(cluster.partitions || {});
  if (parts.length === 0) { el.innerHTML = ""; return; }
  el.innerHTML = parts.map((p) =>
    `<span style="background:var(--danger);opacity:0.7;padding:1px 6px;border-radius:3px;cursor:pointer" data-pid="${p.id}" title="Heal partition">
      ✗ [${Object.keys(p.group_a).map(id => id.split("-").pop()).join(",")}] | [${Object.keys(p.group_b).map(id => id.split("-").pop()).join(",")}]
    </span>`
  ).join("");
  el.querySelectorAll("span[data-pid]").forEach((span) => {
    span.addEventListener("click", async () => {
      const pid = (span as HTMLElement).dataset.pid!;
      await api.healPartition(clusterId, pid);
      store.refreshCluster(clusterId);
      renderPartitionList(clusterId);
    });
  });
}

// ─── Per-node inspector drawer ──────────────────────────────────────────────────
let inspectorNode: { clusterId: string; nodeId: string } | null = null;

async function openInspector(clusterId: string, nodeId: string) {
  inspectorNode = { clusterId, nodeId };
  const drawer = document.getElementById("node-inspector")!;
  const title = document.getElementById("inspector-title")!;
  const body = document.getElementById("inspector-body")!;
  drawer.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
  title.textContent = `Node ${nodeId.split("-").slice(-1)[0]}`;
  body.innerHTML = `<div class="drawer-empty">Loading…</div>`;
  try {
    const [storeSnap, log] = await Promise.all([
      api.getNodeStore(clusterId, nodeId),
      api.getNodeLog(clusterId, nodeId),
    ]);
    const storeRows = Object.values(storeSnap || {})
      .sort((a, b) => a.key.localeCompare(b.key))
      .map((e) => `
        <tr class="${e.tombstone ? "tombstone" : ""}">
          <td>${esc(e.key)}</td>
          <td>${e.tombstone ? "<i>deleted</i>" : esc(decodeB64(String(e.value)))}</td>
          <td>${e.version ?? "—"}</td>
          <td>${vcChipsHTML(e.vclock)}</td>
        </tr>`).join("");
    // op arrives as a numeric enum (0=put,1=..,2=delete). Label it for readability.
    const opLabel = (op: unknown): string => {
      const n = Number(op);
      if (n === 2) return "delete";
      if (n === 0 || n === 1) return "put";
      return String(op);
    };
    const logRows = (log || []).slice(-40).reverse().map((l) => `
        <tr>
          <td>${l.index}</td>
          <td>${l.term}</td>
          <td>${esc(l.key)}</td>
          <td>${esc(opLabel(l.op))}</td>
          <td>${esc((l.origin_id || "").split("-").slice(-1)[0])}</td>
        </tr>`).join("");
    body.innerHTML = `
      <div class="drawer-section-title">Store (${Object.keys(storeSnap || {}).length} keys)</div>
      ${storeRows ? `<table class="drawer-table" id="inspector-store">
        <thead><tr><th>key</th><th>value</th><th>ver</th><th>vclock</th></tr></thead>
        <tbody>${storeRows}</tbody></table>` : `<div class="drawer-empty">Store is empty</div>`}
      <div class="drawer-section-title">Replication Log (${(log || []).length} entries)</div>
      ${logRows ? `<table class="drawer-table" id="inspector-log">
        <thead><tr><th>idx</th><th>term</th><th>key</th><th>op</th><th>origin</th></tr></thead>
        <tbody>${logRows}</tbody></table>` : `<div class="drawer-empty">Log is empty</div>`}
    `;
  } catch (e) {
    body.innerHTML = `<div class="drawer-empty">Failed to load node state</div>`;
    reportError("Inspector", e);
  }
}

function closeInspector() {
  inspectorNode = null;
  const drawer = document.getElementById("node-inspector")!;
  drawer.classList.remove("open");
  drawer.setAttribute("aria-hidden", "true");
}

document.getElementById("inspector-close")?.addEventListener("click", closeInspector);

// ─── Diverged-state diff view ───────────────────────────────────────────────────
async function renderDiff(cluster: ClusterState) {
  const el = document.getElementById("diff-matrix")!;
  const badge = document.getElementById("convergence-badge")!;
  try {
    const report = await api.getConvergence(cluster.id);
    badge.textContent = report.converged ? "✓ converged" : `✗ ${report.diverged?.length || 0} diverged`;
    badge.className = `consistency-badge ${report.converged ? "strong" : "eventual"}`;

    // Build a key × online-node matrix. Diverged keys come with per-node values;
    // for agreeing keys we still want the grid, so pull each online node's store.
    const onlineIds = cluster.node_ids.filter((id) => cluster.nodes[id]?.state === "online");
    if (onlineIds.length < 2) {
      el.innerHTML = `<div class="diff-note">${esc(report.note || "Need ≥2 online replicas to compare")}</div>`;
      return;
    }
    const stores = await Promise.all(onlineIds.map((id) =>
      api.getNodeStore(cluster.id, id).catch((): NodeStoreSnapshot => ({}))));
    const nodeStore: Record<string, Record<string, string>> = {};
    const allKeys = new Set<string>();
    onlineIds.forEach((id, i) => {
      const snap = stores[i] || {};
      const m: Record<string, string> = {};
      for (const [k, e] of Object.entries(snap)) {
        m[k] = e.tombstone ? "<tombstone>" : String(e.value);
        allKeys.add(k);
      }
      nodeStore[id] = m;
    });
    if (allKeys.size === 0) {
      el.innerHTML = `<div class="diff-note">No keys written yet</div>`;
      return;
    }
    const shortId = (id: string) => id.split("-").slice(-1)[0];
    const header = `<tr><th class="diff-key">key</th>${onlineIds.map((id) => `<th>${esc(shortId(id))}</th>`).join("")}</tr>`;
    const rows = [...allKeys].sort().map((k) => {
      const vals = onlineIds.map((id) => nodeStore[id][k]);
      const present = vals.filter((v) => v !== undefined);
      const agree = present.length > 0 && present.every((v) => v === present[0]) && present.length === onlineIds.length;
      const cells = onlineIds.map((id) => {
        const v = nodeStore[id][k];
        if (v === undefined) return `<td class="absent" title="absent">∅</td>`;
        if (v === "<tombstone>") return `<td class="tomb" title="tombstone">⌫</td>`;
        const cls = agree ? "agree" : "diverge";
        return `<td class="${cls}" title="${esc(decodeB64(v))}">${esc(decodeB64(v).slice(0, 6))}</td>`;
      }).join("");
      return `<tr><td class="diff-key">${esc(k)}</td>${cells}</tr>`;
    }).join("");
    el.innerHTML = `<table class="diff-matrix"><thead>${header}</thead><tbody>${rows}</tbody></table>`;
  } catch (e) {
    el.innerHTML = `<div class="diff-note">Convergence unavailable</div>`;
  }
}

// ─── Latency percentile charts ──────────────────────────────────────────────────
function renderLatency(cluster: ClusterState) {
  const el = document.getElementById("latency-chart")!;
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
  const fmt = (v: number) => v === 0 ? "—" : v < 1 ? "<1ms" : `${v.toFixed(1)}ms`;
  const block = (kind: "write" | "read") => {
    const p50 = gather(kind, 50), p95 = gather(kind, 95), p99 = gather(kind, 99);
    const max = Math.max(p99, 1);
    const bar = (p: "p50" | "p95" | "p99", v: number) => `
      <div class="lat-row">
        <span class="lat-label">${p}</span>
        <span class="lat-bar-track"><span class="lat-bar ${p}" style="width:${Math.min(100, (v / max) * 100)}%"></span></span>
        <span class="lat-val">${fmt(v)}</span>
      </div>`;
    return `<div class="lat-block">
      <div class="lat-title">${kind} latency</div>
      ${bar("p50", p50)}${bar("p95", p95)}${bar("p99", p99)}
    </div>`;
  };
  el.innerHTML = block("write") + block("read");
}

// ─── CAP dial ───────────────────────────────────────────────────────────────────
function renderCAP(cluster: ClusterState) {
  const dial = document.getElementById("cap-dial");
  if (!dial) return;
  const strat = cluster.config.strategy;
  const partitioned = Object.keys(cluster.partitions || {}).length > 0;
  // CP: single-leader (sync) and raft favour consistency; AP: multi-leader and
  // leaderless (unless W+R>N makes it strongly consistent) favour availability.
  let cap: "CP" | "AP";
  if (strat === "raft" || strat === "single_leader") cap = "CP";
  else if (strat === "leaderless") {
    const N = cluster.config.quorum_n || cluster.node_ids.length;
    const W = cluster.config.quorum_w || 0, R = cluster.config.quorum_r || 0;
    cap = (W + R) > N ? "CP" : "AP";
  } else cap = "AP";
  dial.dataset.cap = cap;
  dial.classList.toggle("partitioned", partitioned);
  const text = dial.querySelector(".cap-text");
  if (text) text.textContent = partitioned ? `${cap} · partitioned` : cap;
  dial.setAttribute("title", partitioned
    ? `Partition active — ${cap === "CP" ? "minority side rejects to stay consistent" : "both sides stay available, may diverge"}`
    : `${cap} posture given ${strat}`);
}

// ─── Main render loop ─────────────────────────────────────────────────────────
function refresh() {
  const cluster = store.getActive();
  const badge = document.getElementById("strategy-badge")!;
  if (!cluster) {
    badge.textContent = "no cluster";
    return;
  }
  badge.textContent = `${cluster.config.strategy} | id: ${cluster.id.slice(0, 8)}`;
  renderTopology(cluster);
  renderQuorum(cluster);
  renderConsistency(cluster);
  renderMetrics(cluster);
  renderPartitionList(cluster.id);
  renderDiff(cluster);
  renderLatency(cluster);
  renderCAP(cluster);
  syncPermalink(cluster);
}

// ─── Theme toggle ───────────────────────────────────────────────────────────────
const THEME_KEY = "replsim-theme";
function applyTheme(theme: "dark" | "light") {
  document.documentElement.setAttribute("data-theme", theme);
  try { localStorage.setItem(THEME_KEY, theme); } catch {}
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
  applyTheme(cur === "light" ? "dark" : "light");
}
// Restore saved theme (default dark).
applyTheme(((): "dark" | "light" => {
  try { return localStorage.getItem(THEME_KEY) === "light" ? "light" : "dark"; } catch { return "dark"; }
})());
document.getElementById("theme-toggle")?.addEventListener("click", toggleTheme);

// ─── Help modal ─────────────────────────────────────────────────────────────────
function toggleHelp(force?: boolean) {
  const m = document.getElementById("help-modal")!;
  const open = force ?? !m.classList.contains("open");
  m.classList.toggle("open", open);
  m.setAttribute("aria-hidden", String(!open));
}
document.getElementById("help-btn")?.addEventListener("click", () => toggleHelp());
document.getElementById("help-close")?.addEventListener("click", () => toggleHelp(false));

// ─── Config export / import ─────────────────────────────────────────────────
function currentConfigSnapshot(): { config: Record<string, unknown>; faults: Record<string, unknown> } {
  const cluster = store.getActive();
  const config = cluster ? { ...cluster.config, node_count: cluster.node_ids.length } : {};
  const faults = cluster
    ? {
        partitions: Object.values(cluster.partitions || {}).map((p) => ({
          group_a: Object.keys(p.group_a),
          group_b: Object.keys(p.group_b),
        })),
        dropped_messages: cluster.dropped_messages ?? 0,
      }
    : {};
  return { config, faults };
}

function exportConfig() {
  const snap = currentConfigSnapshot();
  const blob = new Blob([JSON.stringify(snap, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `replsim-config-${(snap.config.strategy as string) || "empty"}.json`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  toast("Config downloaded", "success");
}
document.getElementById("export-config-btn")?.addEventListener("click", exportConfig);

async function importConfig(file: File) {
  try {
    const text = await file.text();
    const parsed = JSON.parse(text) as { config?: Record<string, unknown> };
    const cfg = parsed.config;
    if (!cfg || !cfg.strategy) throw new Error("Missing config.strategy in file");
    const startCfg: Record<string, unknown> = {
      strategy: cfg.strategy,
      node_count: cfg.node_count ?? 4,
    };
    if (cfg.replication_mode) startCfg.replication_mode = cfg.replication_mode;
    if (cfg.conflict_resolver) startCfg.conflict_resolver = cfg.conflict_resolver;
    if (cfg.quorum_n) startCfg.quorum_n = cfg.quorum_n;
    if (cfg.quorum_w) startCfg.quorum_w = cfg.quorum_w;
    if (cfg.quorum_r) startCfg.quorum_r = cfg.quorum_r;
    const cluster = await api.startSimulation(startCfg);
    store.clusters.set(cluster.id, cluster);
    store.activeClusterId = cluster.id;
    store.notify();
    refresh();
    toast(`Imported ${cfg.strategy} cluster`, "success");
  } catch (e) {
    reportError("Import config", e);
  }
}
document.getElementById("import-config")?.addEventListener("change", (e) => {
  const input = e.target as HTMLInputElement;
  const file = input.files?.[0];
  if (file) importConfig(file);
  input.value = ""; // allow re-importing the same file
});

// ─── Workload generator ─────────────────────────────────────────────────────
let workloadRunning = false;
async function runWorkload() {
  if (workloadRunning) return;
  const cluster = store.getActive();
  const summary = document.getElementById("workload-summary")!;
  if (!cluster) { toast("No active cluster", "warn"); return; }
  const ops = Math.max(1, Math.min(500, parseInt((document.getElementById("workload-ops") as HTMLInputElement).value) || 20));
  const readPct = Math.max(0, Math.min(100, parseInt((document.getElementById("workload-ratio") as HTMLInputElement).value) || 50));
  workloadRunning = true;
  const btn = document.getElementById("run-workload-btn") as HTMLButtonElement;
  btn.disabled = true;
  let writes = 0, reads = 0, errors = 0;
  const clientId = "workload";
  const knownKeys: string[] = [];
  summary.innerHTML = `running… <div class="workload-progress"><span id="workload-bar"></span></div>`;
  const bar = document.getElementById("workload-bar") as HTMLElement | null;
  for (let i = 0; i < ops; i++) {
    const doRead = knownKeys.length > 0 && Math.random() * 100 < readPct;
    try {
      if (doRead) {
        const key = knownKeys[Math.floor(Math.random() * knownKeys.length)];
        await api.read(cluster.id, key, clientId);
        reads++;
      } else {
        const key = `wl-${Math.floor(Math.random() * Math.max(3, ops / 4))}`;
        await api.write(cluster.id, key, `v${i}-${Math.random().toString(36).slice(2, 6)}`, clientId);
        if (!knownKeys.includes(key)) knownKeys.push(key);
        writes++;
      }
    } catch {
      errors++;
    }
    if (bar) bar.style.width = `${((i + 1) / ops) * 100}%`;
  }
  workloadRunning = false;
  btn.disabled = false;
  summary.textContent = `done: ${writes} writes · ${reads} reads · ${errors} err`;
  await store.refreshCluster(cluster.id);
  refresh();
}
document.getElementById("run-workload-btn")?.addEventListener("click", runWorkload);
document.getElementById("workload-toggle")?.addEventListener("click", () => {
  document.getElementById("workload-panel")?.classList.toggle("collapsed");
});

// ─── Command palette (Cmd/Ctrl+K) ───────────────────────────────────────────
interface PaletteCommand { id: string; label: string; icon: string; hint?: string; run: () => void; }
const paletteCommands: PaletteCommand[] = [
  { id: "create", label: "Create cluster", icon: "＋", hint: "form", run: () => document.getElementById("create-cluster-btn")?.click() },
  { id: "write", label: "Write a key", icon: "✎", hint: "w", run: () => { (document.getElementById("write-key") as HTMLInputElement)?.focus(); } },
  { id: "read", label: "Read current key", icon: "↺", hint: "r", run: () => document.getElementById("read-btn")?.click() },
  { id: "workload", label: "Run workload", icon: "⚡", run: () => runWorkload() },
  { id: "scenario", label: "Run selected scenario", icon: "▶", run: () => document.getElementById("run-scenario-btn")?.click() },
  { id: "theme", label: "Toggle theme", icon: "◐", hint: "t", run: () => toggleTheme() },
  { id: "inspect", label: "Open node inspector", icon: "🔍", hint: "i", run: () => { const c = store.getActive(); if (c?.node_ids.length) openInspector(c.id, c.node_ids[0]); } },
  { id: "export", label: "Export config", icon: "⬇", run: () => exportConfig() },
  { id: "partition", label: "Inject partition", icon: "✂", run: () => document.getElementById("partition-btn")?.click() },
  { id: "clearfaults", label: "Clear faults", icon: "✓", run: () => document.getElementById("clear-faults-btn")?.click() },
  { id: "reset", label: "Reset all clusters", icon: "⟲", run: () => document.getElementById("reset-btn")?.click() },
  { id: "help", label: "Keyboard shortcuts", icon: "?", hint: "?", run: () => toggleHelp(true) },
];
let paletteActive = 0;
let paletteFiltered: PaletteCommand[] = paletteCommands;

// Tiny subsequence fuzzy match — every query char must appear in order.
function fuzzyMatch(query: string, text: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase(), t = text.toLowerCase();
  let i = 0;
  for (const ch of t) { if (ch === q[i]) i++; if (i === q.length) return true; }
  return i === q.length;
}

function renderPalette() {
  const list = document.getElementById("palette-list")!;
  if (paletteFiltered.length === 0) {
    list.innerHTML = `<li class="palette-empty" role="option">No matching commands</li>`;
    return;
  }
  list.innerHTML = paletteFiltered.map((c, i) => `
    <li role="option" data-idx="${i}" aria-selected="${i === paletteActive}" class="${i === paletteActive ? "active" : ""}">
      <span class="pal-icon">${c.icon}</span><span>${esc(c.label)}</span>${c.hint ? `<span class="pal-hint">${esc(c.hint)}</span>` : ""}
    </li>`).join("");
  list.querySelectorAll<HTMLLIElement>("li[data-idx]").forEach((li) => {
    li.addEventListener("click", () => { paletteActive = +li.dataset.idx!; runPaletteSelection(); });
  });
}

function filterPalette(query: string) {
  paletteFiltered = paletteCommands.filter((c) => fuzzyMatch(query, c.label));
  paletteActive = 0;
  renderPalette();
}

function togglePalette(force?: boolean) {
  const p = document.getElementById("command-palette")!;
  const open = force ?? !p.classList.contains("open");
  p.classList.toggle("open", open);
  p.setAttribute("aria-hidden", String(!open));
  if (open) {
    const input = document.getElementById("palette-input") as HTMLInputElement;
    input.value = "";
    filterPalette("");
    setTimeout(() => input.focus(), 0);
  }
}

function runPaletteSelection() {
  const cmd = paletteFiltered[paletteActive];
  togglePalette(false);
  if (cmd) setTimeout(() => cmd.run(), 0);
}

document.getElementById("palette-btn")?.addEventListener("click", () => togglePalette(true));
document.getElementById("command-palette")?.addEventListener("click", (e) => {
  if ((e.target as HTMLElement).id === "command-palette") togglePalette(false);
});
document.getElementById("palette-input")?.addEventListener("input", (e) => {
  filterPalette((e.target as HTMLInputElement).value);
});
document.getElementById("palette-input")?.addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown") { e.preventDefault(); paletteActive = Math.min(paletteFiltered.length - 1, paletteActive + 1); renderPalette(); }
  else if (e.key === "ArrowUp") { e.preventDefault(); paletteActive = Math.max(0, paletteActive - 1); renderPalette(); }
  else if (e.key === "Enter") { e.preventDefault(); runPaletteSelection(); }
  else if (e.key === "Escape") { e.preventDefault(); togglePalette(false); }
});

// ─── Keyboard shortcuts ───────────────────────────────────────────────────────────
document.addEventListener("keydown", (e) => {
  // Cmd/Ctrl+K opens the command palette from anywhere (even while typing).
  if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
    e.preventDefault();
    togglePalette();
    return;
  }
  // Don't hijack typing in inputs (except Escape).
  const tag = (e.target as HTMLElement)?.tagName;
  const typing = tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
  if (e.key === "Escape") { closeInspector(); toggleHelp(false); togglePalette(false); return; }
  if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
  switch (e.key) {
    case "w": e.preventDefault(); (document.getElementById("write-key") as HTMLInputElement)?.focus(); break;
    case "r": (document.getElementById("read-btn") as HTMLElement)?.click(); break;
    case "t": toggleTheme(); break;
    case "?": toggleHelp(); break;
    case "i": {
      const c = store.getActive();
      if (c?.node_ids.length) openInspector(c.id, c.node_ids[0]);
      break;
    }
  }
});

// ─── Save/load permalink (cluster config in URL hash) ───────────────────────────────
let lastPermalink = "";
function syncPermalink(cluster: ClusterState) {
  const cfg = cluster.config;
  const params = new URLSearchParams();
  params.set("strategy", cfg.strategy);
  params.set("nodes", String(cluster.node_ids.length));
  if (cfg.replication_mode) params.set("mode", cfg.replication_mode);
  if (cfg.conflict_resolver) params.set("resolver", cfg.conflict_resolver);
  if (cfg.quorum_n) params.set("n", String(cfg.quorum_n));
  if (cfg.quorum_w) params.set("w", String(cfg.quorum_w));
  if (cfg.quorum_r) params.set("r", String(cfg.quorum_r));
  const hash = "#" + params.toString();
  if (hash !== lastPermalink) {
    lastPermalink = hash;
    history.replaceState(null, "", hash);
  }
}

// Restore a shared config from the URL hash by creating that cluster on load.
async function restoreFromPermalink(): Promise<boolean> {
  if (!location.hash || location.hash.length < 2) return false;
  const p = new URLSearchParams(location.hash.slice(1));
  const strategy = p.get("strategy");
  if (!strategy) return false;
  const nodeCount = parseInt(p.get("nodes") || "4");
  const cfg: Record<string, unknown> = { strategy, node_count: nodeCount };
  if (p.get("mode")) cfg.replication_mode = p.get("mode");
  if (p.get("resolver")) cfg.conflict_resolver = p.get("resolver");
  if (strategy === "leaderless") {
    cfg.quorum_n = parseInt(p.get("n") || String(nodeCount));
    cfg.quorum_w = parseInt(p.get("w") || "3");
    cfg.quorum_r = parseInt(p.get("r") || "3");
  }
  try {
    const cluster = await api.startSimulation(cfg);
    store.clusters.set(cluster.id, cluster);
    store.activeClusterId = cluster.id;
    store.notify();
    refresh();
    toast("Restored cluster from shared link", "success");
    return true;
  } catch (e) {
    reportError("Restore from link", e);
    return false;
  }
}

// Subscribe to store changes
store.subscribe(refresh);

// Poll for cluster state every 2 seconds
setInterval(async () => {
  const cluster = store.getActive();
  if (cluster) {
    await store.refreshCluster(cluster.id);
  }
  // Keep an open inspector drawer live.
  if (inspectorNode) openInspector(inspectorNode.clusterId, inspectorNode.nodeId);
}, 2000);

// Initial load
renderControl();
store.loadClusters().then(async () => {
  // If no cluster exists yet and a permalink is present, restore it.
  if (!store.getActive()) await restoreFromPermalink();
  refresh();
});
