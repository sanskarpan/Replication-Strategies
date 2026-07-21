import type { Component } from "../core/component";
import type { VectorClock } from "../api/types";
import { bus } from "../core/bus";
import { req, esc } from "../core/dom";

// ─── Conflict Log ─────────────────────────────────────────────────────────────
const conflictsBody = req("conflicts-body");
const conflictCount = req("conflict-count");
let totalConflicts = 0;

// ─── VClock Inspector Dialog ──────────────────────────────────────────────────
interface ConflictPayload {
  key: string;
  local_vc: VectorClock;
  remote_vc: VectorClock;
  resolver?: string;
  reason?: string;
}

function ensureDialog(): HTMLDialogElement {
  let dlg = document.getElementById("vclock-inspector-dialog") as HTMLDialogElement | null;
  if (dlg) return dlg;
  dlg = document.createElement("dialog");
  dlg.id = "vclock-inspector-dialog";
  dlg.className = "vci-dialog";
  dlg.innerHTML = `
    <div class="vci-header">
      <span class="vci-key"></span>
      <button class="vci-close" aria-label="Close">×</button>
    </div>
    <div class="vci-body">
      <div class="vci-col" id="vci-local">
        <div class="vci-colhead">Local</div>
        <div class="vci-entries"></div>
      </div>
      <div class="vci-col" id="vci-remote">
        <div class="vci-colhead">Remote</div>
        <div class="vci-entries"></div>
      </div>
    </div>
    <div class="vci-verdict"></div>
  `;
  document.body.appendChild(dlg);
  dlg.querySelector(".vci-close")!.addEventListener("click", () => dlg!.close());
  dlg.addEventListener("click", (e) => { if (e.target === dlg) dlg!.close(); });
  return dlg;
}

function vcDiffClass(myVal: number, otherVal: number): string {
  if (myVal > otherVal) return "vci-lead";
  if (myVal < otherVal) return "vci-lag";
  return "vci-equal";
}

function renderVCColumn(container: Element, vc: VectorClock, otherVc: VectorClock) {
  const allNodes = new Set([...Object.keys(vc), ...Object.keys(otherVc)]);
  const sorted = [...allNodes].sort();
  container.innerHTML = sorted.map((node) => {
    const myVal = vc[node] ?? 0;
    const otherVal = otherVc[node] ?? 0;
    const cls = vcDiffClass(myVal, otherVal);
    return `<div class="vci-entry ${cls}"><span class="vci-node">${esc(node)}</span><span class="vci-val">${myVal}</span></div>`;
  }).join("");
}

function openVClockInspector(payload: ConflictPayload) {
  const dlg = ensureDialog();
  const localVc = payload.local_vc ?? {};
  const remoteVc = payload.remote_vc ?? {};

  (dlg.querySelector(".vci-key") as HTMLElement).textContent = `Key: "${payload.key}"`;
  renderVCColumn(dlg.querySelector("#vci-local .vci-entries")!, localVc, remoteVc);
  renderVCColumn(dlg.querySelector("#vci-remote .vci-entries")!, remoteVc, localVc);

  const verdict = dlg.querySelector(".vci-verdict") as HTMLElement;
  const resolver = payload.resolver ?? "pending";
  const reason = payload.reason ?? "";
  verdict.textContent = reason ? `Resolved by ${resolver} — ${reason}` : `Resolver: ${resolver}`;

  dlg.showModal();
}

export const conflicts: Component = {
  id: "conflicts",
  mount() {
    bus.on("conflict_detected", (evt) => {
      totalConflicts++;
      conflictCount.textContent = `${totalConflicts} conflicts`;
      const d = evt.data || {};
      const payload: ConflictPayload = {
        key: String(d.key ?? "?"),
        local_vc: (d.local_vc as VectorClock) ?? {},
        remote_vc: (d.remote_vc as VectorClock) ?? {},
      };
      const div = document.createElement("div");
      div.className = "conflict-entry";
      div.dataset.payload = JSON.stringify(payload);
      div.title = "Click to inspect vector clock diff";
      div.style.cursor = "pointer";
      div.innerHTML = `
        <div class="key">${esc(String(d.key || "?"))} <span style="color:var(--text-dim);font-size:9px">[click to inspect]</span></div>
        <div class="vclock">local: ${esc(JSON.stringify(d.local_vc || {}))} | remote: ${esc(JSON.stringify(d.remote_vc || {}))}</div>
        <div style="color:var(--text-dim);font-size:10px">${new Date(evt.timestamp).toLocaleTimeString()}</div>
      `;
      div.addEventListener("click", () => {
        const p = JSON.parse(div.dataset.payload || "{}") as ConflictPayload;
        openVClockInspector(p);
      });
      conflictsBody.prepend(div);
      if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
    });

    bus.on("conflict_resolved", (evt) => {
      const d = evt.data || {};
      const resolvedKey = String(d.key ?? "");
      // Find matching entry by key; fall back to most-recent if server omits key.
      let target: HTMLElement | null = null;
      if (resolvedKey) {
        for (const child of Array.from(conflictsBody.children)) {
          try {
            const p = JSON.parse((child as HTMLElement).dataset.payload || "{}") as ConflictPayload;
            if (p.key === resolvedKey) { target = child as HTMLElement; break; }
          } catch { /* skip unparseable entries */ }
        }
      }
      if (!target) target = conflictsBody.firstElementChild as HTMLElement | null;
      if (!target) return;

      try {
        const p = JSON.parse(target.dataset.payload || "{}") as ConflictPayload;
        p.resolver = String(d.resolver ?? "");
        p.reason = String(d.reason ?? "");
        target.dataset.payload = JSON.stringify(p);
      } catch { /* ignore malformed payload */ }

      const res = document.createElement("div");
      res.className = "resolver";
      res.style.cssText = "color:var(--accent2);font-size:10px;margin-top:2px";
      res.textContent = `resolved: ${d.resolver} — ${d.reason || ""}`;
      target.appendChild(res);
    });

    // ─── Quorum / Read-Repair events ─────────────────────────────────────────
    bus.on("quorum_failed", (evt) => {
      const d = evt.data || {};
      const div = document.createElement("div");
      div.className = "conflict-entry";
      div.style.cssText = "border-left:3px solid var(--danger)";
      div.innerHTML = `<div class="key" style="color:var(--danger)">Quorum FAILED: ${esc(String(d.key || "?"))}</div>
        <div style="font-size:10px;color:var(--text-dim)">acked ${Number(d.acked)}/${Number(d.w)} required</div>`;
      conflictsBody.prepend(div);
      if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
    });

    bus.on("read_repair", (evt) => {
      const d = evt.data || {};
      if (!d.stale_nodes) return;
      const div = document.createElement("div");
      div.className = "conflict-entry";
      div.style.cssText = "border-left:3px solid var(--accent2)";
      div.innerHTML = `<div class="key" style="color:var(--accent2)">Read Repair: ${esc(String(d.key || "?"))}</div>
        <div style="font-size:10px;color:var(--text-dim)">stale: ${esc(JSON.stringify(d.stale_nodes))}</div>`;
      conflictsBody.prepend(div);
      if (conflictsBody.children.length > 50) conflictsBody.lastChild?.remove();
    });
  },
};
