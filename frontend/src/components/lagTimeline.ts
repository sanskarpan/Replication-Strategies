import * as d3 from "d3";
import type { Component } from "../core/component";
import { bus } from "../core/bus";
import { req, shortId } from "../core/dom";

// ─── Lag Timeline ─────────────────────────────────────────────────────────────
interface LagDataPoint { time: Date; follower: string; lag: number; }

const lagData: LagDataPoint[] = [];
let lagSvg: d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown> | null = null;

function renderLagTimeline() {
  const container = req("lag-body");
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
  const legend = req("lag-legend");
  legend.innerHTML = followers.map((f) => `
    <div class="lag-legend-item">
      <div class="lag-legend-dot" style="background:${color(f)}"></div>
      <span>${shortId(f)}</span>
    </div>
  `).join("");
}

export const lagTimeline: Component = {
  id: "lagTimeline",
  mount() {
    bus.on("follower_lag", (evt) => {
      lagData.push({
        time: new Date(evt.timestamp),
        follower: (evt.data?.follower_id as string) || evt.node_id || "?",
        lag: (evt.data?.lag_entries as number) || 0,
      });
      if (lagData.length > 200) lagData.shift();
      renderLagTimeline();
    });
  },
};
