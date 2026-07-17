import type { ClusterState, SimEvent } from "../api/types";
import { api } from "../api/client";

type Listener = () => void;

class SimulationStore {
  clusters: Map<string, ClusterState> = new Map();
  activeClusterId: string | null = null;
  events: SimEvent[] = [];
  maxEvents = 500;
  private listeners: Set<Listener> = new Set();

  subscribe(fn: Listener) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify() {
    this.listeners.forEach((fn) => fn());
  }

  handleEvent(evt: SimEvent) {
    this.events.unshift(evt);
    if (this.events.length > this.maxEvents) {
      this.events = this.events.slice(0, this.maxEvents);
    }
    // Refresh cluster state on relevant events
    if (evt.cluster_id && this.clusters.has(evt.cluster_id)) {
      this.refreshCluster(evt.cluster_id);
    }
    this.notify();
  }

  async refreshCluster(id: string) {
    try {
      const state = (await api.getCluster(id)) as ClusterState;
      this.clusters.set(id, state);
      this.notify();
    } catch {}
  }

  async loadClusters() {
    try {
      const data = (await api.getSimulationState()) as { clusters: ClusterState[] };
      this.clusters.clear();
      for (const c of data.clusters || []) {
        this.clusters.set(c.id, c);
      }
      if (this.clusters.size > 0 && !this.activeClusterId) {
        this.activeClusterId = [...this.clusters.keys()][0];
      }
      this.notify();
    } catch {}
  }

  setActiveCluster(id: string) {
    this.activeClusterId = id;
    this.notify();
  }

  getActive(): ClusterState | undefined {
    return this.activeClusterId ? this.clusters.get(this.activeClusterId) : undefined;
  }
}

export const store = new SimulationStore();
