import type { ClusterState, SimEvent } from "../api/types";
import { api } from "../api/client";
import type { AppState } from "./component";

type Listener = () => void;

// AppStore is the single source of truth for the UI. It owns the set of clusters, which
// one is active, and the recent event stream, and notifies subscribers on any change.
// Components read an immutable AppState snapshot via state(); they never mutate the store
// directly except through its intent methods (setActive, refreshCluster, …).
class AppStore {
  clusters: Map<string, ClusterState> = new Map();
  activeClusterId: string | null = null;
  events: SimEvent[] = [];
  maxEvents = 500;
  private listeners: Set<Listener> = new Set();
  private replayState: ClusterState | null = null;
  private replaySeq: number = 0;

  subscribe(fn: Listener): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify(): void {
    this.listeners.forEach((fn) => fn());
  }

  // state projects the store into the immutable snapshot components render from.
  // When a replay state is set (scrubber active), all components see the historical
  // cluster state transparently — no component needs to know the difference.
  state(): AppState {
    return {
      active: this.replayState ?? this.getActive(),
      activeId: this.activeClusterId,
      clusterCount: this.clusters.size,
      replay: this.replayState !== null,
      replaySeq: this.replaySeq,
    };
  }

  // setReplay pins the rendered state to a historical ClusterState. Polling is
  // not paused — the scrubber component owns that lifecycle.
  setReplay(state: ClusterState, seq: number): void {
    this.replayState = state;
    this.replaySeq = seq;
    this.notify();
  }

  clearReplay(): void {
    this.replayState = null;
    this.replaySeq = 0;
    this.notify();
  }

  isReplaying(): boolean {
    return this.replayState !== null;
  }

  handleEvent(evt: SimEvent): void {
    this.events.unshift(evt);
    if (this.events.length > this.maxEvents) this.events = this.events.slice(0, this.maxEvents);
    // Refresh cluster state on any event that names a cluster we track.
    if (evt.cluster_id && this.clusters.has(evt.cluster_id)) {
      this.refreshCluster(evt.cluster_id);
    }
    this.notify();
  }

  async refreshCluster(id: string): Promise<void> {
    try {
      const state = (await api.getCluster(id)) as ClusterState;
      this.clusters.set(id, state);
      this.notify();
    } catch {
      /* transient fetch failure — keep last-known state */
    }
  }

  async loadClusters(): Promise<void> {
    try {
      const data = (await api.getSimulationState()) as { clusters: ClusterState[] };
      this.clusters.clear();
      for (const c of data.clusters || []) this.clusters.set(c.id, c);
      if (this.clusters.size > 0 && !this.activeClusterId) {
        this.activeClusterId = [...this.clusters.keys()][0];
      }
      this.notify();
    } catch {
      /* backend not up yet — the poll loop retries */
    }
  }

  // adopt records a freshly-created cluster and makes it active (create/import/restore).
  adopt(cluster: ClusterState): void {
    this.clusters.set(cluster.id, cluster);
    this.activeClusterId = cluster.id;
    this.notify();
  }

  clear(): void {
    this.clusters.clear();
    this.activeClusterId = null;
    this.notify();
  }

  setActiveCluster(id: string): void {
    this.activeClusterId = id;
    this.notify();
  }

  getActive(): ClusterState | undefined {
    return this.activeClusterId ? this.clusters.get(this.activeClusterId) : undefined;
  }
}

export const store = new AppStore();
