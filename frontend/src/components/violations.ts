import type { Component } from "../core/component";
import { bus } from "../core/bus";
import { esc, byId } from "../core/dom";
import { pulseNodes } from "./topology";

// ─── Real-time consistency-violation banner ─────────────────────────────────
let violationTimer: ReturnType<typeof setTimeout> | null = null;
export function flashViolation(message: string, detail = "", nodes: string[] = []) {
  const banner = byId("violation-banner");
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
  pulseNodes(nodes);
}

export const violations: Component = {
  id: "violations",
  mount() {
    bus.on("quorum_failed", (evt) => {
      const d = evt.data || {};
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
    bus.on("*", (evt) => {
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
  },
};
