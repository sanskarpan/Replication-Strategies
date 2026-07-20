import type { Component } from "../core/component";
import type { SimEvent } from "../api/types";
import { bus } from "../core/bus";
import { reduceMotion, esc, req, byId } from "../core/dom";

// ─── Event Log (with filter/search + timeline strip + rolling rate) ──────────
const eventLog = req("event-log");
let eventCount = 0;

// Filter state, driven by the toolbar text input + type dropdown.
const eventFilter = { text: "", type: "" };
const seenEventTypes = new Set<string>();

// Timeline strip: a rolling list of coloured ticks for recent events.
const timelineEl = byId("event-timeline");
function timelineClass(type: string): string {
  if (type === "conflict_detected" || type === "conflict_resolved" || type === "quorum_failed") return "conflict";
  if (type === "read_repair" || type === "hinted_handoff") return "repair";
  if (type === "quorum_achieved") return "quorum";
  if (type.startsWith("entry_replicated") || type === "follower_lag") return "replication";
  if (type.startsWith("node_") || type.startsWith("partition_") || type === "leader_elected") return "node";
  return "";
}

// Rolling event-rate: keep timestamps in a 5s window, recompute ev/s on a timer.
const rateWindow: number[] = [];
function pushRate() {
  rateWindow.push(Date.now());
}
function renderEventRate() {
  const now = Date.now();
  while (rateWindow.length && now - rateWindow[0] > 5000) rateWindow.shift();
  const rate = rateWindow.length / 5;
  const el = byId("event-rate");
  if (!el) return;
  const val = el.querySelector(".event-rate-val");
  if (val) val.textContent = rate.toFixed(1);
  el.classList.toggle("active", rate > 0);
}

function matchesFilter(type: string, dataStr: string): boolean {
  if (eventFilter.type && type !== eventFilter.type) return false;
  if (eventFilter.text) {
    const hay = (type + " " + dataStr).toLowerCase();
    if (!hay.includes(eventFilter.text.toLowerCase())) return false;
  }
  return true;
}

function applyEventFilter() {
  eventLog.querySelectorAll<HTMLLIElement>("li").forEach((li) => {
    const type = li.dataset.etype || "";
    const dataStr = li.dataset.edata || "";
    li.classList.toggle("filtered-out", !matchesFilter(type, dataStr));
  });
}

function registerEventType(type: string) {
  if (seenEventTypes.has(type)) return;
  seenEventTypes.add(type);
  const sel = byId<HTMLSelectElement>("event-type-filter");
  if (!sel) return;
  const opt = document.createElement("option");
  opt.value = type;
  opt.textContent = type;
  sel.appendChild(opt);
}

function pushTimelineTick(type: string) {
  if (!timelineEl) return;
  const tick = document.createElement("div");
  tick.className = `tick ${timelineClass(type)}${reduceMotion ? "" : " new"}`.trim();
  tick.title = type;
  timelineEl.appendChild(tick);
  while (timelineEl.children.length > 80) timelineEl.firstChild?.remove();
}

function appendEvent(evt: SimEvent) {
  eventCount++;
  pushRate();
  registerEventType(evt.type);
  pushTimelineTick(evt.type);
  const li = document.createElement("li");
  const ts = new Date(evt.timestamp).toLocaleTimeString();
  const dataStr = evt.data ? JSON.stringify(evt.data).slice(0, 80) : "";
  li.dataset.etype = evt.type;
  li.dataset.edata = dataStr;
  li.innerHTML = `<span class="event-type">${evt.type}</span><span class="event-time">${ts}</span><span class="event-data">${esc(dataStr)}</span>`;
  if (!matchesFilter(evt.type, dataStr)) li.classList.add("filtered-out");
  eventLog.prepend(li);
  // Keep at most 100 entries in the DOM
  while (eventLog.children.length > 100) eventLog.lastChild?.remove();
}

export const events: Component = {
  id: "events",
  mount() {
    bus.on("*", (evt) => appendEvent(evt));

    req("clear-events-btn").addEventListener("click", () => {
      eventLog.innerHTML = "";
      if (timelineEl) timelineEl.innerHTML = "";
    });

    byId("event-filter")?.addEventListener("input", (e) => {
      eventFilter.text = (e.target as HTMLInputElement).value;
      applyEventFilter();
    });
    byId("event-type-filter")?.addEventListener("change", (e) => {
      eventFilter.type = (e.target as HTMLSelectElement).value;
      applyEventFilter();
    });

    setInterval(renderEventRate, 1000);
  },
};
