import type { AppState, Component } from "../core/component";
import type { ClusterState } from "../api/types";
import { req } from "../core/dom";

// ─── Quorum Visualizer ────────────────────────────────────────────────────────
function renderQuorum(cluster: ClusterState) {
  const el = req("quorum-content");
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
  const sn = req<HTMLInputElement>("quorum-slider-n");
  const sw = req<HTMLInputElement>("quorum-slider-w");
  const sr = req<HTMLInputElement>("quorum-slider-r");
  const updateVerdict = () => {
    const n = +sn.value, w = +sw.value, r = +sr.value;
    (req("quorum-val-n")).textContent = String(n);
    (req("quorum-val-w")).textContent = String(w);
    (req("quorum-val-r")).textContent = String(r);
    const strongNow = (w + r) > n;
    // Simple stale-read estimate: probability a read set misses the newest write set
    // = fraction of read placements that fall entirely outside the W most-recent
    // replicas. Approximated as max(0, (n-w)/n)^(effective read shortfall).
    const staleP = strongNow ? 0 : Math.max(0, (n - w) / n) * Math.max(0, (n - r + 1) / n);
    const verdict = req("quorum-verdict");
    verdict.className = `quorum-verdict ${strongNow ? "strong" : "eventual"}`;
    verdict.innerHTML = strongNow
      ? `W+R=${w + r} &gt; N=${n} → strongly consistent · stale-read ≈ 0%`
      : `W+R=${w + r} ≤ N=${n} → eventual · stale-read ≈ ${(staleP * 100).toFixed(0)}%`;
  };
  [sn, sw, sr].forEach((s) => s.addEventListener("input", updateVerdict));
  updateVerdict();
}

export const quorum: Component = {
  id: "quorum",
  render(state: AppState) {
    if (state.active) renderQuorum(state.active);
  },
};
