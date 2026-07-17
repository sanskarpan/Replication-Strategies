import * as d3 from "d3";
import { api } from "./api/client";
import type { ClusterState, SimEvent, Scenario, DemoRYWResult, DemoMonotonicResult, DemoPrefixResult } from "./api/types";
import { WSClient } from "./ws/client";
import { store } from "./store/simulation";

// ─── WebSocket ────────────────────────────────────────────────────────────────
const ws = new WSClient();
ws.on("*", (evt) => {
  store.handleEvent(evt);
  appendEvent(evt);
});
ws.connect();

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

// ─── Event Log ───────────────────────────────────────────────────────────────
const eventLog = document.getElementById("event-log")!;
let eventCount = 0;

function appendEvent(evt: SimEvent) {
  eventCount++;
  const li = document.createElement("li");
  const ts = new Date(evt.timestamp).toLocaleTimeString();
  const dataStr = evt.data ? JSON.stringify(evt.data).slice(0, 80) : "";
  li.innerHTML = `<span class="event-type">${evt.type}</span><span class="event-time">${ts}</span><span class="event-data">${dataStr}</span>`;
  eventLog.prepend(li);
  // Keep at most 100 entries in the DOM
  while (eventLog.children.length > 100) eventLog.lastChild?.remove();
}

document.getElementById("clear-events-btn")!.addEventListener("click", () => {
  eventLog.innerHTML = "";
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

  if (strategy === "single_leader") {
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
      return g;
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
  });
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
  `;
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
    const cluster = await api.startSimulation(cfg);
    store.clusters.set(cluster.id, cluster);
    store.activeClusterId = cluster.id;
    store.notify();
    refresh();
  });

  // Add node
  document.getElementById("add-node-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return alert("No active cluster");
    await api.addNode(cluster.id);
    await store.refreshCluster(cluster.id);
    refresh();
  });

  // Remove node
  document.getElementById("remove-node-btn")!.addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return alert("No active cluster");
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
    if (!cluster) return alert("No active cluster");
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
    if (!cluster) return alert("No active cluster");
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
    if (!cluster) return alert("No active cluster");
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
}

// Subscribe to store changes
store.subscribe(refresh);

// Poll for cluster state every 2 seconds
setInterval(async () => {
  const cluster = store.getActive();
  if (cluster) {
    await store.refreshCluster(cluster.id);
  }
}, 2000);

// Initial load
renderControl();
store.loadClusters().then(refresh);
