import type { AppState, Component } from "../core/component";
import { api } from "../api/client";
import { store } from "../core/store";
import { byId } from "../core/dom";

// ─── Partition List ───────────────────────────────────────────────────────────
export function renderPartitionList(clusterId: string) {
  const cluster = store.getActive();
  const el = byId("partition-list");
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

export const partitionList: Component = {
  id: "partitionList",
  render(state: AppState) {
    if (state.active) renderPartitionList(state.active.id);
  },
};
