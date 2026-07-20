import type { Component } from "../core/component";
import { bus } from "../core/bus";
import { req } from "../core/dom";

// ─── Conflict Log ─────────────────────────────────────────────────────────────
const conflictsBody = req("conflicts-body");
const conflictCount = req("conflict-count");
let totalConflicts = 0;

export const conflicts: Component = {
  id: "conflicts",
  mount() {
    bus.on("conflict_detected", (evt) => {
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

    bus.on("conflict_resolved", (evt) => {
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

    // ─── Quorum / Read-Repair events ─────────────────────────────────────────
    bus.on("quorum_failed", (evt) => {
      const d = evt.data || {};
      const div = document.createElement("div");
      div.className = "conflict-entry";
      div.style.cssText = "border-left:3px solid var(--danger)";
      div.innerHTML = `<div class="key" style="color:var(--danger)">Quorum FAILED: ${d.key || "?"}</div>
        <div style="font-size:10px;color:var(--text-dim)">acked ${d.acked}/${d.w} required</div>`;
      conflictsBody.prepend(div);
      if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
    });

    bus.on("read_repair", (evt) => {
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
  },
};
