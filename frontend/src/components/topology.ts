import * as d3 from "d3";
import type { AppState, Component } from "../core/component";
import { renderGuard } from "../core/component";
import type { ClusterState, SimEvent } from "../api/types";
import { api } from "../api/client";
import { store } from "../core/store";
import { bus } from "../core/bus";
import { reduceMotion, req, shortId } from "../core/dom";
import { openInspector } from "./inspector";

// ─── D3 Topology ─────────────────────────────────────────────────────────────
interface TopoNode extends d3.SimulationNodeDatum { id: string; role: string; state: string; lag: number; }
interface TopoLink extends d3.SimulationLinkDatum<TopoNode> { lagged?: boolean; }

let topoSim: d3.Simulation<TopoNode, TopoLink> | null = null;
let topoSvg: d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown> | null = null;
// Guard over the topology *structure* (strategy + node set). The force simulation is only
// rebuilt when this changes; state/lag/partition changes are applied in place so the layout
// settles and nodes stay clickable (no perpetual jiggle).
const topoGuard = renderGuard();

// Live node positions from the force sim, used to tween dots along links.
const nodePos = new Map<string, { x: number; y: number }>();

// ─── Per-link throughput tracking ────────────────────────────────────────────
interface LinkStat { count: number; windowStart: number; rate: number; }
// Key: "sourceId→targetId" (directed, matching D3 link direction)
const linkStats = new Map<string, LinkStat>();
const RATE_WINDOW_MS = 2000;

function recordPacket(from: string, to: string) {
  const key = `${from}→${to}`;
  const now = Date.now();
  const stat = linkStats.get(key) ?? { count: 0, windowStart: now, rate: 0 };
  stat.count++;
  const elapsed = now - stat.windowStart;
  if (elapsed >= RATE_WINDOW_MS) {
    stat.rate = Math.round((stat.count / elapsed) * 1000);
    stat.count = 0;
    stat.windowStart = now;
  }
  linkStats.set(key, stat);
}

function partitionSet(cluster: ClusterState): Set<string> {
  const partitioned = new Set<string>();
  for (const part of Object.values(cluster.partitions || {})) {
    for (const a of Object.keys(part.group_a ?? {})) {
      for (const b of Object.keys(part.group_b ?? {})) {
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
  const container = req("topology-body");
  const W = container.clientWidth || 300;
  const H = container.clientHeight || 240;

  if (!topoSvg) {
    topoSvg = d3.select("#topology-body").append("svg")
      .attr("width", "100%").attr("height", "100%")
      .attr("viewBox", `0 0 ${W} ${H}`);
    topoSvg.append("g").attr("class", "links");
    topoSvg.append("g").attr("class", "throughput-labels");
    topoSvg.append("g").attr("class", "nodes");
  }

  const svg = topoSvg;

  // Only rebuild the simulation when the node structure changes. Otherwise just
  // refresh visuals so the layout settles instead of re-animating every poll.
  const sig = cluster.config.strategy + "|" + [...cluster.node_ids].sort().join(",");
  if (!topoGuard(sig)) {
    updateTopoVisuals(cluster);
    return;
  }

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
    .text((d) => shortId(d.id));
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
    nodePos.clear();
    for (const n of nodes) if (n.x != null && n.y != null) nodePos.set(n.id, { x: n.x, y: n.y });

    // Render per-link throughput labels at the midpoint of each directed link.
    svg.select<SVGGElement>(".throughput-labels")
      .selectAll<SVGTextElement, TopoLink>("text.link-throughput")
      .data(links, (d) => `${(d.source as TopoNode).id}→${(d.target as TopoNode).id}`)
      .join("text")
      .attr("class", "link-throughput")
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("fill", "var(--text-dim)")
      .attr("opacity", 0.8)
      .style("font-size", "9px")
      .style("pointer-events", "none")
      .attr("x", (d) => ((d.source as TopoNode).x! + (d.target as TopoNode).x!) / 2)
      .attr("y", (d) => ((d.source as TopoNode).y! + (d.target as TopoNode).y!) / 2 - 8)
      .text((d) => {
        const src = (d.source as TopoNode).id;
        const tgt = (d.target as TopoNode).id;
        const stat = linkStats.get(`${src}→${tgt}`);
        if (!stat || stat.count === 0) return "";
        return stat.rate > 0 ? `${stat.count} (${stat.rate}/s)` : `${stat.count}`;
      });
  });
}

// ─── Animated message-passing packets ──────────────────────────────────────────
function packetColor(type: string): string {
  if (type === "read_repair") return "var(--accent2)";
  if (type === "hinted_handoff") return "var(--warn)";
  return "var(--accent)"; // replication
}

// Animate a dot from `from` to `to`. If `dropped`, fade it out partway.
export function animatePacket(from: string, to: string, type: string, dropped = false) {
  if (reduceMotion || !topoSvg) return;
  const a = nodePos.get(from);
  const b = nodePos.get(to);
  if (!a || !b) return;
  recordPacket(from, to);
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

// pulseNodes flashes the offending node circles (used by the violation banner).
export function pulseNodes(ids: string[]) {
  if (!topoSvg || !ids.length) return;
  topoSvg.select(".nodes").selectAll<SVGGElement, TopoNode>("g.node-g")
    .select<SVGCircleElement>("circle")
    .filter((d) => ids.includes(d.id))
    .classed("violation", true)
    .each(function () {
      const c = this;
      setTimeout(() => c.classList.remove("violation"), 2000);
    });
}

export const topology: Component = {
  id: "topology",
  mount() {
    // Animate packets for replication / repair / handoff events on the WS stream.
    for (const t of ["entry_replicated", "read_repair", "hinted_handoff"] as const) {
      bus.on(t, (evt) => packetForEvent(evt));
    }
  },
  render(state: AppState) {
    if (state.active) renderTopology(state.active);
  },
};
