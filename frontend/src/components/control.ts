import type { Component } from "../core/component";
import type { Scenario } from "../api/types";
import { api } from "../api/client";
import { store } from "../core/store";
import { req } from "../core/dom";
import { displayResult } from "../core/format";
import { toast, reportError } from "../core/toast";
import { renderPartitionList } from "./partitionList";

// ─── Control Panel ────────────────────────────────────────────────────────────
function renderControl() {
  const el = req("control-body");

  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:8px;padding:4px 0">
      <!-- Create cluster -->
      <div class="form-row">
        <div class="form-group">
          <label>Strategy</label>
          <select id="strategy-select">
            <option value="single_leader">Single-Leader</option>
            <option value="raft">Raft (consensus)</option>
            <option value="multi_leader">Multi-Leader</option>
            <option value="leaderless">Leaderless</option>
          </select>
        </div>
        <div class="form-group">
          <label>Nodes</label>
          <input type="number" id="node-count-input" value="4" min="2" max="10" style="width:60px" />
        </div>
        <div class="form-group" id="repl-mode-group">
          <label>Repl. Mode</label>
          <select id="repl-mode-select">
            <option value="async">Async</option>
            <option value="sync">Sync</option>
            <option value="semi_sync">Semi-Sync</option>
          </select>
        </div>
        <div class="form-group" id="resolver-group" style="display:none">
          <label>Conflict Resolver</label>
          <select id="resolver-select">
            <option value="lww">LWW</option>
            <option value="vector_clock">Vector Clock</option>
            <option value="crdt">CRDT</option>
          </select>
        </div>
        <div class="form-group" id="quorum-group" style="display:none">
          <label>W / R</label>
          <div style="display:flex;gap:4px">
            <input type="number" id="quorum-w" value="3" min="1" max="5" style="width:45px" />
            <input type="number" id="quorum-r" value="3" min="1" max="5" style="width:45px" />
          </div>
        </div>
        <button class="btn btn-primary" id="create-cluster-btn">Create Cluster</button>
        <button class="btn" id="add-node-btn" title="Add a node to the active cluster">Add Node</button>
        <button class="btn btn-danger" id="remove-node-btn" title="Remove the last node from the active cluster">Remove Node</button>
        <button class="btn btn-danger" id="reset-btn">Reset All</button>
      </div>
    </div>

    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px">
      <!-- Write / Read -->
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="form-row">
          <div class="form-group">
            <label>Key</label>
            <input type="text" id="write-key" value="mykey" style="width:80px" />
          </div>
          <div class="form-group">
            <label>Value</label>
            <input type="text" id="write-value" value="hello" style="width:80px" />
          </div>
          <div class="form-group">
            <label>Client ID</label>
            <input type="text" id="client-id" value="client1" style="width:70px" />
          </div>
          <button class="btn" id="write-btn">Write</button>
          <button class="btn" id="read-btn">Read</button>
          <button class="btn btn-danger" id="delete-btn">Delete</button>
        </div>
        <div id="rw-result" style="font-size:11px;color:var(--text-dim);font-family:monospace;max-width:400px;word-break:break-all"></div>
      </div>

      <!-- Network faults -->
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="form-row">
          <div class="form-group">
            <label>Latency (ms)</label>
            <input type="number" id="latency-ms" value="500" style="width:60px" />
          </div>
          <div class="form-group">
            <label>Drop %</label>
            <input type="number" id="drop-rate" value="30" min="0" max="100" style="width:52px" />
          </div>
          <button class="btn" id="add-latency-btn" title="Add latency to last follower/node">Add Lag</button>
          <button class="btn" id="add-drop-btn" title="Add packet drop rate to last follower/node">Add Drop</button>
          <button class="btn" id="partition-btn" title="Partition first half vs second half">Partition</button>
          <button class="btn" id="clear-faults-btn">Clear Faults</button>
        </div>
        <div id="partition-list" style="font-size:10px;color:var(--text-dim);display:flex;gap:6px;flex-wrap:wrap"></div>
      </div>

      <!-- Scenarios -->
      <div>
        <select id="scenario-select" style="width:180px">
          <option value="">Select scenario...</option>
        </select>
        <button class="btn btn-primary" id="run-scenario-btn" style="margin-left:4px">Run</button>
      </div>
    </div>
  `;

  // Load scenarios
  api.listScenarios().then((scenarios: Scenario[]) => {
    const sel = req<HTMLSelectElement>("scenario-select");
    for (const s of scenarios) {
      const opt = document.createElement("option");
      opt.value = s.name ?? "";
      opt.textContent = `${s.name} (${s.strategy})`;
      sel.appendChild(opt);
    }
  });

  // Strategy select -> show/hide fields
  const stratSel = req<HTMLSelectElement>("strategy-select");
  stratSel.addEventListener("change", () => {
    const v = stratSel.value;
    (req("repl-mode-group")).style.display = v === "single_leader" ? "" : "none";
    (req("resolver-group")).style.display = v === "multi_leader" ? "" : "none";
    (req("quorum-group")).style.display = v === "leaderless" ? "" : "none";
  });

  // Create cluster
  req("create-cluster-btn").addEventListener("click", async () => {
    const strategy = stratSel.value;
    const nodeCount = parseInt((req<HTMLInputElement>("node-count-input")).value);
    const cfg: Record<string, unknown> = { strategy, node_count: nodeCount };
    if (strategy === "single_leader") {
      cfg.replication_mode = (req<HTMLSelectElement>("repl-mode-select")).value;
    } else if (strategy === "multi_leader") {
      cfg.conflict_resolver = (req<HTMLSelectElement>("resolver-select")).value;
    } else {
      cfg.quorum_n = nodeCount;
      cfg.quorum_w = parseInt((req<HTMLInputElement>("quorum-w")).value);
      cfg.quorum_r = parseInt((req<HTMLInputElement>("quorum-r")).value);
    }
    try {
      const cluster = await api.startSimulation(cfg);
      store.adopt(cluster);
      toast(`${strategy} cluster created (${nodeCount} nodes)`, "success");
    } catch (e) {
      reportError("Create cluster", e);
    }
  });

  // Add node
  req("add-node-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    await api.addNode(cluster.id);
    await store.refreshCluster(cluster.id);
  });

  // Remove node
  req("remove-node-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    if (!lastNode) return;
    await api.removeNode(cluster.id, lastNode);
    await store.refreshCluster(cluster.id);
  });

  // Reset
  req("reset-btn").addEventListener("click", async () => {
    await api.resetSimulation();
    store.clear();
  });

  // Write
  req("write-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (req<HTMLInputElement>("write-key")).value;
    const value = (req<HTMLInputElement>("write-value")).value;
    const clientId = (req<HTMLInputElement>("client-id")).value;
    const rwResult = req("rw-result");
    try {
      const result = await api.write(cluster.id, key, value, clientId);
      rwResult.textContent = displayResult(result);
      store.refreshCluster(cluster.id);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Read
  req("read-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (req<HTMLInputElement>("write-key")).value;
    const clientId = (req<HTMLInputElement>("client-id")).value;
    const rwResult = req("rw-result");
    try {
      const result = await api.read(cluster.id, key, clientId);
      rwResult.textContent = displayResult(result);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Delete
  req("delete-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return toast("No active cluster", "warn");
    const key = (req<HTMLInputElement>("write-key")).value;
    const clientId = (req<HTMLInputElement>("client-id")).value;
    const rwResult = req("rw-result");
    try {
      const result = await api.deleteKey(cluster.id, key, clientId);
      rwResult.textContent = displayResult(result);
      store.refreshCluster(cluster.id);
    } catch (e: unknown) {
      rwResult.textContent = e instanceof Error ? e.message : String(e);
    }
  });

  // Add latency
  req("add-latency-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster || cluster.node_ids.length < 2) return;
    const ms = parseInt((req<HTMLInputElement>("latency-ms")).value);
    const src = cluster.leader_id || cluster.node_ids[0];
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    await api.setLatency(cluster.id, src, lastNode, ms);
  });

  // Add drop rate
  req("add-drop-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster || cluster.node_ids.length < 2) return;
    const pct = parseInt((req<HTMLInputElement>("drop-rate")).value);
    const rate = Math.max(0, Math.min(1, pct / 100));
    const src = cluster.leader_id || cluster.node_ids[0];
    const lastNode = cluster.node_ids[cluster.node_ids.length - 1];
    await api.setDropRate(cluster.id, src, lastNode, rate);
  });

  // Partition
  req("partition-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return;
    const half = Math.floor(cluster.node_ids.length / 2);
    const groupA = cluster.node_ids.slice(0, half);
    const groupB = cluster.node_ids.slice(half);
    await api.injectPartition(cluster.id, groupA, groupB);
    store.refreshCluster(cluster.id);
    renderPartitionList(cluster.id);
  });

  // Clear faults
  req("clear-faults-btn").addEventListener("click", async () => {
    const cluster = store.getActive();
    if (!cluster) return;
    await api.clearFaults(cluster.id);
    store.refreshCluster(cluster.id);
    const pl = req("partition-list");
    if (pl) pl.innerHTML = "";
  });

  // Run scenario
  req("run-scenario-btn").addEventListener("click", async () => {
    const name = (req<HTMLSelectElement>("scenario-select")).value;
    if (!name) return;
    const cluster = await api.runScenario(name);
    store.adopt(cluster);
  });
}

export const control: Component = {
  id: "control",
  mount() {
    renderControl();
  },
};
