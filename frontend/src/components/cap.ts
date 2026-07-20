import type { AppState, Component } from "../core/component";
import type { ClusterState } from "../api/types";
import { byId } from "../core/dom";

// ─── CAP dial ───────────────────────────────────────────────────────────────────
function renderCAP(cluster: ClusterState) {
  const dial = byId("cap-dial");
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

export const cap: Component = {
  id: "cap",
  render(state: AppState) {
    if (state.active) renderCAP(state.active);
  },
};
