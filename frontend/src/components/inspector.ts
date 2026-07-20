import type { Component } from "../core/component";
import { api } from "../api/client";
import { esc, decodeB64, byId, req, shortId } from "../core/dom";
import { vcChipsHTML } from "../core/format";
import { reportError } from "../core/toast";

// ─── Per-node inspector drawer ──────────────────────────────────────────────────
let inspectorNode: { clusterId: string; nodeId: string } | null = null;

export async function openInspector(clusterId: string, nodeId: string) {
  inspectorNode = { clusterId, nodeId };
  const drawer = req("node-inspector");
  const title = req("inspector-title");
  const body = req("inspector-body");
  drawer.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
  title.textContent = `Node ${shortId(nodeId)}`;
  body.innerHTML = `<div class="drawer-empty">Loading…</div>`;
  try {
    const [storeSnap, log] = await Promise.all([
      api.getNodeStore(clusterId, nodeId),
      api.getNodeLog(clusterId, nodeId),
    ]);
    const storeRows = Object.values(storeSnap || {})
      .sort((a, b) => (a.key ?? "").localeCompare(b.key ?? ""))
      .map((e) => `
        <tr class="${e.tombstone ? "tombstone" : ""}">
          <td>${esc(e.key ?? "")}</td>
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
          <td>${esc(shortId(l.origin_id || ""))}</td>
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

export function closeInspector() {
  inspectorNode = null;
  const drawer = req("node-inspector");
  drawer.classList.remove("open");
  drawer.setAttribute("aria-hidden", "true");
}

// refreshIfOpen re-opens the drawer if a node is currently being inspected — the
// bootstrap's 2s poll calls this to keep the drawer live.
export function refreshIfOpen() {
  if (inspectorNode) openInspector(inspectorNode.clusterId, inspectorNode.nodeId);
}

export const inspector: Component & { refreshIfOpen(): void } = {
  id: "inspector",
  mount() {
    byId("inspector-close")?.addEventListener("click", closeInspector);
  },
  refreshIfOpen,
};
