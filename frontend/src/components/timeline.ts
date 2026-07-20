import type { Component, AppState } from "../core/component";
import type { HistoryEntry, HistoryResponse } from "../api/types";
import { store } from "../core/store";
import { api } from "../api/client";
import { byId, req } from "../core/dom";
import { reportError } from "../core/toast";

// ─── Timeline Scrubber Component ─────────────────────────────────────────────
// Renders a sticky strip at the bottom of the page with a range-input scrubber,
// play/pause/step controls, and a coloured event-tick canvas.
// When the thumb moves, the store's replay state is set so all 16 components
// transparently render the historical ClusterState at that sequence number.

let entries: HistoryEntry[] = [];
let maxSeq = 0;
let playing = false;
let playTimer: ReturnType<typeof setInterval> | null = null;
let pollTimer: ReturnType<typeof setInterval> | null = null;

function tickColor(type: string): string {
  if (type === "partition_created" || type === "partition_healed") return "var(--danger)";
  if (type === "node_state_changed" || type === "leader_elected") return "var(--warn)";
  if (type === "conflict_detected" || type === "quorum_failed") return "var(--replica)";
  if (type === "write_received") return "var(--accent)";
  if (type === "read_received") return "var(--accent2)";
  return "var(--text-dim)";
}

function renderTicks() {
  const canvas = byId<HTMLCanvasElement>("timeline-canvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w;
  canvas.height = h;
  ctx.clearRect(0, 0, w, h);
  if (!entries.length || !maxSeq) return;
  for (const e of entries) {
    const x = ((e.seq - 1) / maxSeq) * w;
    ctx.fillStyle = tickColor(e.event.type);
    ctx.fillRect(Math.round(x), 0, 2, h);
  }
}

async function seekTo(seq: number) {
  const clusterId = store.getActive()?.id ?? store.activeClusterId;
  if (!clusterId) return;
  try {
    const res = await api.getHistoryState(clusterId, seq);
    if (res.base_state) {
      store.setReplay(res.base_state, seq);
    }
    // Update scrubber thumb position.
    const thumb = byId<HTMLInputElement>("timeline-thumb");
    if (thumb) thumb.value = String(seq);
    updateSeqLabel(seq);
  } catch (e) {
    reportError("timeline seek", e);
  }
}

function updateSeqLabel(seq: number) {
  const label = byId("timeline-seq-label");
  if (label) label.textContent = `seq ${seq} / ${maxSeq}`;
}

function stepForward() {
  const thumb = byId<HTMLInputElement>("timeline-thumb");
  if (!thumb) return;
  const cur = parseInt(thumb.value) || 0;
  const next = Math.min(cur + 1, maxSeq);
  if (next !== cur) seekTo(next);
}

function stepBack() {
  const thumb = byId<HTMLInputElement>("timeline-thumb");
  if (!thumb) return;
  const cur = parseInt(thumb.value) || 0;
  const prev = Math.max(cur - 1, 1);
  if (prev !== cur) seekTo(prev);
}

function startPlay() {
  if (playing) return;
  playing = true;
  const playBtn = byId("timeline-play-btn");
  if (playBtn) playBtn.textContent = "⏸";
  playTimer = setInterval(() => {
    const thumb = byId<HTMLInputElement>("timeline-thumb");
    if (!thumb) return;
    const cur = parseInt(thumb.value) || 0;
    if (cur >= maxSeq) {
      stopPlay();
      return;
    }
    seekTo(cur + 1);
  }, 300);
}

function stopPlay() {
  playing = false;
  if (playTimer) { clearInterval(playTimer); playTimer = null; }
  const playBtn = byId("timeline-play-btn");
  if (playBtn) playBtn.textContent = "▶";
}

function goLive() {
  stopPlay();
  store.clearReplay();
  const thumb = byId<HTMLInputElement>("timeline-thumb");
  if (thumb) thumb.value = String(maxSeq);
  const label = byId("timeline-seq-label");
  if (label) label.textContent = "live";
}

async function refreshHistory() {
  const clusterId = store.getActive()?.id ?? store.activeClusterId;
  if (!clusterId) return;
  try {
    const res: HistoryResponse = await api.getHistory(clusterId, 0, 500);
    entries = res.entries;
    maxSeq = res.max_seq;
    const thumb = byId<HTMLInputElement>("timeline-thumb");
    if (thumb) {
      thumb.max = String(maxSeq);
      // Only advance thumb to live end when NOT in replay mode.
      if (!store.isReplaying()) thumb.value = String(maxSeq);
    }
    renderTicks();
    if (!store.isReplaying()) {
      updateSeqLabel(maxSeq);
    }
  } catch {
    // transient — ignore
  }
}

export const timeline: Component = {
  id: "timeline",

  mount() {
    const panel = byId("timeline-panel");
    if (!panel) return;

    // Play/Pause
    byId("timeline-play-btn")?.addEventListener("click", () => {
      if (playing) stopPlay();
      else startPlay();
    });
    // Step back / forward
    byId("timeline-back-btn")?.addEventListener("click", stepBack);
    byId("timeline-fwd-btn")?.addEventListener("click", stepForward);
    // Live
    byId("timeline-live-btn")?.addEventListener("click", goLive);

    // Scrubber drag
    const thumb = byId<HTMLInputElement>("timeline-thumb");
    if (thumb) {
      thumb.addEventListener("input", () => {
        stopPlay();
        const seq = parseInt(thumb.value);
        if (!isNaN(seq)) seekTo(seq);
      });
    }

    // Redraw ticks on resize.
    new ResizeObserver(renderTicks).observe(req("timeline-canvas"));

    // Poll history every 3 seconds.
    pollTimer = setInterval(refreshHistory, 3000);
  },

  render(state: AppState) {
    const panel = byId("timeline-panel");
    if (!panel) return;

    const hasCluster = !!state.active;
    panel.classList.toggle("has-cluster", hasCluster);

    // Show replay badge in topbar if replaying.
    const badge = byId("replay-badge");
    if (badge) {
      badge.hidden = !state.replay;
      if (state.replay) badge.textContent = `⏪ replay @ seq ${state.replaySeq}`;
    }

    if (hasCluster) refreshHistory();
  },
};
