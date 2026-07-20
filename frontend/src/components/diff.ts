import type { AppState, Component } from "../core/component";
import type { ClusterState, NodeStoreSnapshot } from "../api/types";
import { api } from "../api/client";
import { esc, decodeB64, req, shortId } from "../core/dom";

// ─── Diverged-state diff view ───────────────────────────────────────────────────
async function renderDiff(cluster: ClusterState) {
  const el = req("diff-matrix");
  const badge = req("convergence-badge");
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

export const diff: Component = {
  id: "diff",
  render(state: AppState) {
    if (state.active) renderDiff(state.active);
  },
};
