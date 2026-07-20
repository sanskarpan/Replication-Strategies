import { store } from "./core/store";
import { bus } from "./core/bus";
import { mountAll } from "./core/component";
import { header } from "./components/header";
import { topology } from "./components/topology";
import { lagTimeline } from "./components/lagTimeline";
import { events } from "./components/events";
import { conflicts } from "./components/conflicts";
import { violations } from "./components/violations";
import { quorum } from "./components/quorum";
import { consistency } from "./components/consistency";
import { metrics } from "./components/metrics";
import { latency } from "./components/latency";
import { cap } from "./components/cap";
import { control } from "./components/control";
import { partitionList } from "./components/partitionList";
import { inspector } from "./components/inspector";
import { diff } from "./components/diff";
import { shell, restoreFromPermalink, renderWSStatus } from "./components/shell";

const components = [header, topology, lagTimeline, events, conflicts, violations, quorum,
  consistency, metrics, latency, cap, control, partitionList, inspector, diff, shell];

const run = mountAll(components);

bus.on("*", (evt) => store.handleEvent(evt));
bus.onStatus(renderWSStatus);
bus.connect();

store.subscribe(() => run(store.state()));

// Poll for cluster state every 2 seconds; keep an open inspector drawer live.
setInterval(async () => {
  const c = store.getActive();
  if (c) await store.refreshCluster(c.id);
  inspector.refreshIfOpen?.();
}, 2000);

// Initial load: adopt existing clusters, else restore a permalink, then render.
store.loadClusters().then(async () => {
  if (!store.getActive()) await restoreFromPermalink();
  store.notify();
});
