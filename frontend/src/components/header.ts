import type { AppState, Component } from "../core/component";
import { req } from "../core/dom";

// ─── Header strategy badge ─────────────────────────────────────────────────────
export const header: Component = {
  id: "header",
  render(state: AppState) {
    const badge = req("strategy-badge");
    const cluster = state.active;
    if (!cluster) {
      badge.textContent = "no cluster";
      return;
    }
    badge.textContent = `${cluster.config.strategy} | id: ${cluster.id.slice(0, 8)}`;
  },
};
