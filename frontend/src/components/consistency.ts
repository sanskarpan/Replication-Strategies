import type { AppState, Component } from "../core/component";
import { renderGuard } from "../core/component";
import type { ClusterState } from "../api/types";
import { api } from "../api/client";
import { store } from "../core/store";
import { req, byId } from "../core/dom";

// ─── Consistency Panel ────────────────────────────────────────────────────────
// Guard over the consistency panel's *shape*. It holds interactive controls (the
// replication-mode dropdown, demo buttons), so we only rebuild it when the shape
// changes — otherwise the 2s poll would wipe an open dropdown / demo result.
const consistencyGuard = renderGuard();

function renderConsistency(cluster: ClusterState) {
  const el = req("consistency-body");
  const strategy = cluster.config.strategy;

  // Only rebuild when the panel shape changes (strategy/mode/resolver/quorum). This
  // keeps interactive controls and demo results alive across the 2s poll.
  const sig = [cluster.id, strategy, cluster.config.replication_mode, cluster.config.conflict_resolver,
    cluster.config.quorum_n, cluster.config.quorum_w, cluster.config.quorum_r].join("|");
  if (!consistencyGuard(sig) && el.querySelector(".consistency-result")) return;

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
  const replModeSel = byId<HTMLSelectElement>("repl-mode-live");
  replModeSel?.addEventListener("change", async () => {
    const newMode = replModeSel.value;
    await api.updateClusterConfig(cluster.id, { replication_mode: newMode });
    store.refreshCluster(cluster.id);
  });

  // Demo buttons
  byId("demo-ryw-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoReadYourWrites(cluster.id);
      resultEl().textContent = `RYW: wrote "${res.write_value}" → read consistent=${res.consistent}`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });

  byId("demo-mono-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoMonotonicReads(cluster.id);
      const v1 = res.read1?.entry?.value ? atob(res.read1.entry.value as string) : "?";
      const v2 = res.read2?.entry?.value ? atob(res.read2.entry.value as string) : "?";
      resultEl().textContent = `Monotonic: read1="${v1}" → read2="${v2}" monotonic=${res.monotonic}`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });

  byId("demo-prefix-btn")?.addEventListener("click", async () => {
    try {
      const res = await api.demoConsistentPrefix(cluster.id);
      resultEl().textContent = `Prefix: wrote ${res.writes.length} keys in order, consistency="${res.prefix}"`;
    } catch (e: unknown) {
      resultEl().textContent = e instanceof Error ? e.message : String(e);
    }
  });
}

export const consistency: Component = {
  id: "consistency",
  render(state: AppState) {
    if (state.active) renderConsistency(state.active);
  },
};
