import type { AppState, Component } from "../core/component";
import type { ClusterState } from "../api/types";
import type { WSStatus } from "../ws/client";
import { api } from "../api/client";
import { store } from "../core/store";
import { esc, byId } from "../core/dom";
import { toast, reportError } from "../core/toast";
import { openInspector, closeInspector } from "./inspector";

// ─── WebSocket status pill ────────────────────────────────────────────────────
export function renderWSStatus(status: WSStatus) {
  const pill = byId("ws-status");
  if (!pill) return;
  pill.dataset.state = status;
  const label = pill.querySelector(".ws-label");
  if (label) label.textContent = status === "live" ? "live" : status;
}

// ─── Theme toggle ───────────────────────────────────────────────────────────────
const THEME_KEY = "replsim-theme";
function applyTheme(theme: "dark" | "light") {
  document.documentElement.setAttribute("data-theme", theme);
  try { localStorage.setItem(THEME_KEY, theme); } catch {}
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
  applyTheme(cur === "light" ? "dark" : "light");
}

// ─── Help modal ─────────────────────────────────────────────────────────────────
function toggleHelp(force?: boolean) {
  const m = byId("help-modal");
  if (!m) return;
  const open = force ?? !m.classList.contains("open");
  m.classList.toggle("open", open);
  m.setAttribute("aria-hidden", String(!open));
}

// ─── Config export / import ─────────────────────────────────────────────────
function currentConfigSnapshot(): { config: Record<string, unknown>; faults: Record<string, unknown> } {
  const cluster = store.getActive();
  const config = cluster ? { ...cluster.config, node_count: cluster.node_ids.length } : {};
  const faults = cluster
    ? {
        partitions: Object.values(cluster.partitions || {}).map((p) => ({
          group_a: Object.keys(p.group_a),
          group_b: Object.keys(p.group_b),
        })),
        dropped_messages: cluster.dropped_messages ?? 0,
      }
    : {};
  return { config, faults };
}

function exportConfig() {
  const snap = currentConfigSnapshot();
  const blob = new Blob([JSON.stringify(snap, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `replsim-config-${(snap.config.strategy as string) || "empty"}.json`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  toast("Config downloaded", "success");
}

async function importConfig(file: File) {
  try {
    const text = await file.text();
    const parsed = JSON.parse(text) as { config?: Record<string, unknown> };
    const cfg = parsed.config;
    if (!cfg || !cfg.strategy) throw new Error("Missing config.strategy in file");
    const startCfg: Record<string, unknown> = {
      strategy: cfg.strategy,
      node_count: cfg.node_count ?? 4,
    };
    if (cfg.replication_mode) startCfg.replication_mode = cfg.replication_mode;
    if (cfg.conflict_resolver) startCfg.conflict_resolver = cfg.conflict_resolver;
    if (cfg.quorum_n) startCfg.quorum_n = cfg.quorum_n;
    if (cfg.quorum_w) startCfg.quorum_w = cfg.quorum_w;
    if (cfg.quorum_r) startCfg.quorum_r = cfg.quorum_r;
    const cluster = await api.startSimulation(startCfg);
    store.adopt(cluster);
    toast(`Imported ${cfg.strategy} cluster`, "success");
  } catch (e) {
    reportError("Import config", e);
  }
}

// ─── Workload generator ─────────────────────────────────────────────────────
let workloadRunning = false;
async function runWorkload() {
  if (workloadRunning) return;
  const cluster = store.getActive();
  const summary = byId("workload-summary");
  if (!summary) return;
  if (!cluster) { toast("No active cluster", "warn"); return; }
  const ops = Math.max(1, Math.min(500, parseInt((byId<HTMLInputElement>("workload-ops"))!.value) || 20));
  const readPct = Math.max(0, Math.min(100, parseInt((byId<HTMLInputElement>("workload-ratio"))!.value) || 50));
  workloadRunning = true;
  const btn = byId<HTMLButtonElement>("run-workload-btn")!;
  btn.disabled = true;
  let writes = 0, reads = 0, errors = 0;
  const clientId = "workload";
  const knownKeys: string[] = [];
  summary.innerHTML = `running… <div class="workload-progress"><span id="workload-bar"></span></div>`;
  const bar = byId("workload-bar");
  for (let i = 0; i < ops; i++) {
    const doRead = knownKeys.length > 0 && Math.random() * 100 < readPct;
    try {
      if (doRead) {
        const key = knownKeys[Math.floor(Math.random() * knownKeys.length)];
        await api.read(cluster.id, key, clientId);
        reads++;
      } else {
        const key = `wl-${Math.floor(Math.random() * Math.max(3, ops / 4))}`;
        await api.write(cluster.id, key, `v${i}-${Math.random().toString(36).slice(2, 6)}`, clientId);
        if (!knownKeys.includes(key)) knownKeys.push(key);
        writes++;
      }
    } catch {
      errors++;
    }
    if (bar) bar.style.width = `${((i + 1) / ops) * 100}%`;
  }
  workloadRunning = false;
  btn.disabled = false;
  summary.textContent = `done: ${writes} writes · ${reads} reads · ${errors} err`;
  await store.refreshCluster(cluster.id);
}

// ─── Command palette (Cmd/Ctrl+K) ───────────────────────────────────────────
interface PaletteCommand { id: string; label: string; icon: string; hint?: string; run: () => void; }
const paletteCommands: PaletteCommand[] = [
  { id: "create", label: "Create cluster", icon: "＋", hint: "form", run: () => byId("create-cluster-btn")?.click() },
  { id: "write", label: "Write a key", icon: "✎", hint: "w", run: () => { (byId<HTMLInputElement>("write-key"))?.focus(); } },
  { id: "read", label: "Read current key", icon: "↺", hint: "r", run: () => byId("read-btn")?.click() },
  { id: "workload", label: "Run workload", icon: "⚡", run: () => runWorkload() },
  { id: "scenario", label: "Run selected scenario", icon: "▶", run: () => byId("run-scenario-btn")?.click() },
  { id: "theme", label: "Toggle theme", icon: "◐", hint: "t", run: () => toggleTheme() },
  { id: "inspect", label: "Open node inspector", icon: "🔍", hint: "i", run: () => { const c = store.getActive(); if (c?.node_ids.length) openInspector(c.id, c.node_ids[0]); } },
  { id: "export", label: "Export config", icon: "⬇", run: () => exportConfig() },
  { id: "partition", label: "Inject partition", icon: "✂", run: () => byId("partition-btn")?.click() },
  { id: "clearfaults", label: "Clear faults", icon: "✓", run: () => byId("clear-faults-btn")?.click() },
  { id: "reset", label: "Reset all clusters", icon: "⟲", run: () => byId("reset-btn")?.click() },
  { id: "help", label: "Keyboard shortcuts", icon: "?", hint: "?", run: () => toggleHelp(true) },
];
let paletteActive = 0;
let paletteFiltered: PaletteCommand[] = paletteCommands;

// Tiny subsequence fuzzy match — every query char must appear in order.
function fuzzyMatch(query: string, text: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase(), t = text.toLowerCase();
  let i = 0;
  for (const ch of t) { if (ch === q[i]) i++; if (i === q.length) return true; }
  return i === q.length;
}

function renderPalette() {
  const list = byId("palette-list");
  if (!list) return;
  if (paletteFiltered.length === 0) {
    list.innerHTML = `<li class="palette-empty" role="option">No matching commands</li>`;
    return;
  }
  list.innerHTML = paletteFiltered.map((c, i) => `
    <li role="option" data-idx="${i}" aria-selected="${i === paletteActive}" class="${i === paletteActive ? "active" : ""}">
      <span class="pal-icon">${c.icon}</span><span>${esc(c.label)}</span>${c.hint ? `<span class="pal-hint">${esc(c.hint)}</span>` : ""}
    </li>`).join("");
  list.querySelectorAll<HTMLLIElement>("li[data-idx]").forEach((li) => {
    li.addEventListener("click", () => { paletteActive = +li.dataset.idx!; runPaletteSelection(); });
  });
}

function filterPalette(query: string) {
  paletteFiltered = paletteCommands.filter((c) => fuzzyMatch(query, c.label));
  paletteActive = 0;
  renderPalette();
}

function togglePalette(force?: boolean) {
  const p = byId("command-palette");
  if (!p) return;
  const open = force ?? !p.classList.contains("open");
  p.classList.toggle("open", open);
  p.setAttribute("aria-hidden", String(!open));
  if (open) {
    const input = byId<HTMLInputElement>("palette-input")!;
    input.value = "";
    filterPalette("");
    setTimeout(() => input.focus(), 0);
  }
}

function runPaletteSelection() {
  const cmd = paletteFiltered[paletteActive];
  togglePalette(false);
  if (cmd) setTimeout(() => cmd.run(), 0);
}

// ─── Save/load permalink (cluster config in URL hash) ───────────────────────────────
let lastPermalink = "";
function syncPermalink(cluster: ClusterState) {
  const cfg = cluster.config;
  const params = new URLSearchParams();
  params.set("strategy", cfg.strategy);
  params.set("nodes", String(cluster.node_ids.length));
  if (cfg.replication_mode) params.set("mode", cfg.replication_mode);
  if (cfg.conflict_resolver) params.set("resolver", cfg.conflict_resolver);
  if (cfg.quorum_n) params.set("n", String(cfg.quorum_n));
  if (cfg.quorum_w) params.set("w", String(cfg.quorum_w));
  if (cfg.quorum_r) params.set("r", String(cfg.quorum_r));
  const hash = "#" + params.toString();
  if (hash !== lastPermalink) {
    lastPermalink = hash;
    history.replaceState(null, "", hash);
  }
}

// Restore a shared config from the URL hash by creating that cluster on load.
export async function restoreFromPermalink(): Promise<boolean> {
  if (!location.hash || location.hash.length < 2) return false;
  const p = new URLSearchParams(location.hash.slice(1));
  const strategy = p.get("strategy");
  if (!strategy) return false;
  const nodeCount = parseInt(p.get("nodes") || "4");
  const cfg: Record<string, unknown> = { strategy, node_count: nodeCount };
  if (p.get("mode")) cfg.replication_mode = p.get("mode");
  if (p.get("resolver")) cfg.conflict_resolver = p.get("resolver");
  if (strategy === "leaderless") {
    cfg.quorum_n = parseInt(p.get("n") || String(nodeCount));
    cfg.quorum_w = parseInt(p.get("w") || "3");
    cfg.quorum_r = parseInt(p.get("r") || "3");
  }
  try {
    const cluster = await api.startSimulation(cfg);
    store.adopt(cluster);
    toast("Restored cluster from shared link", "success");
    return true;
  } catch (e) {
    reportError("Restore from link", e);
    return false;
  }
}

export const shell: Component = {
  id: "shell",
  mount() {
    // Restore saved theme (default dark).
    applyTheme(((): "dark" | "light" => {
      try { return localStorage.getItem(THEME_KEY) === "light" ? "light" : "dark"; } catch { return "dark"; }
    })());
    byId("theme-toggle")?.addEventListener("click", toggleTheme);

    // Help modal
    byId("help-btn")?.addEventListener("click", () => toggleHelp());
    byId("help-close")?.addEventListener("click", () => toggleHelp(false));

    // Config export / import
    byId("export-config-btn")?.addEventListener("click", exportConfig);
    byId("import-config")?.addEventListener("change", (e) => {
      const input = e.target as HTMLInputElement;
      const file = input.files?.[0];
      if (file) importConfig(file);
      input.value = ""; // allow re-importing the same file
    });

    // Workload generator
    byId("run-workload-btn")?.addEventListener("click", runWorkload);
    byId("workload-toggle")?.addEventListener("click", () => {
      byId("workload-panel")?.classList.toggle("collapsed");
    });

    // Command palette
    byId("palette-btn")?.addEventListener("click", () => togglePalette(true));
    byId("command-palette")?.addEventListener("click", (e) => {
      if ((e.target as HTMLElement).id === "command-palette") togglePalette(false);
    });
    byId("palette-input")?.addEventListener("input", (e) => {
      filterPalette((e.target as HTMLInputElement).value);
    });
    byId("palette-input")?.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown") { e.preventDefault(); paletteActive = Math.min(paletteFiltered.length - 1, paletteActive + 1); renderPalette(); }
      else if (e.key === "ArrowUp") { e.preventDefault(); paletteActive = Math.max(0, paletteActive - 1); renderPalette(); }
      else if (e.key === "Enter") { e.preventDefault(); runPaletteSelection(); }
      else if (e.key === "Escape") { e.preventDefault(); togglePalette(false); }
    });

    // Keyboard shortcuts
    document.addEventListener("keydown", (e) => {
      // Cmd/Ctrl+K opens the command palette from anywhere (even while typing).
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        togglePalette();
        return;
      }
      // Don't hijack typing in inputs (except Escape).
      const tag = (e.target as HTMLElement)?.tagName;
      const typing = tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
      if (e.key === "Escape") { closeInspector(); toggleHelp(false); togglePalette(false); return; }
      if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
      switch (e.key) {
        case "w": e.preventDefault(); (byId<HTMLInputElement>("write-key"))?.focus(); break;
        case "r": (byId("read-btn"))?.click(); break;
        case "t": toggleTheme(); break;
        case "?": toggleHelp(); break;
        case "i": {
          const c = store.getActive();
          if (c?.node_ids.length) openInspector(c.id, c.node_ids[0]);
          break;
        }
      }
    });
  },
  render(state: AppState) {
    if (state.active) syncPermalink(state.active);
  },
};
