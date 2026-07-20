import { test, expect } from "bun:test";
import { esc, decodeB64, shortId } from "./dom";
import { displayResult, vcChipsHTML, avgLatency, pctl, pctlValue, fmtChartMs } from "./format";
import { renderGuard, mountAll } from "./component";
import type { Component, AppState } from "./component";
import { store } from "./store";
import type { SimEvent } from "../api/types";

// ─── dom helpers ────────────────────────────────────────────────────────────
test("esc escapes HTML metacharacters", () => {
  expect(esc(`<a href="x">&`)).toBe("&lt;a href=&quot;x&quot;&gt;&amp;");
});

test("decodeB64 decodes base64 and passes through invalid input", () => {
  expect(decodeB64(btoa("hello"))).toBe("hello");
  expect(decodeB64("!!not-base64!!")).toBe("!!not-base64!!");
});

test("shortId returns the trailing node segment", () => {
  expect(shortId("node-abc123-2")).toBe("2");
  expect(shortId("plain")).toBe("plain");
});

// ─── format ─────────────────────────────────────────────────────────────────
test("displayResult decodes base64 value fields", () => {
  const out = displayResult({ value: btoa("hi"), key: "k" });
  expect(out).toContain('"value": "hi"');
  expect(out).toContain('"key": "k"');
});

test("vcChipsHTML renders chips sorted by node, empty clock as ∅", () => {
  expect(vcChipsHTML({})).toContain("∅");
  const html = vcChipsHTML({ "node-b": 2, "node-a": 1 });
  // node-a (chip a:1) must appear before node-b (chip b:2)
  expect(html.indexOf(">a<")).toBeLessThan(html.indexOf(">b<"));
  expect(html).toContain(":1");
  expect(html).toContain(":2");
});

test("avgLatency / pctl formatting conventions", () => {
  expect(avgLatency([])).toBe("—");
  expect(avgLatency([0, 0])).toBe("<1ms"); // sub-1ms mean -> <1ms (not —)
  expect(avgLatency([10, 20])).toBe("15.0ms");
  expect(pctl([], 99)).toBe("—");
  expect(pctlValue([1, 2, 3, 4], 50)).toBe(2); // nearest-rank
  expect(pctlValue([1, 2, 3, 4], 100)).toBe(4);
  expect(fmtChartMs(0)).toBe("—"); // chart uses — for zero
  expect(fmtChartMs(0.5)).toBe("<1ms");
  expect(fmtChartMs(12)).toBe("12.0ms");
});

// ─── component runtime ──────────────────────────────────────────────────────
test("renderGuard fires only when the signature changes", () => {
  const g = renderGuard();
  expect(g("a")).toBe(true);
  expect(g("a")).toBe(false);
  expect(g("b")).toBe(true);
  expect(g("b")).toBe(false);
});

test("mountAll mounts once, renders on demand, and isolates failures", () => {
  const calls: string[] = [];
  const good: Component = {
    id: "good",
    mount() { calls.push("good:mount"); },
    render() { calls.push("good:render"); },
  };
  const bad: Component = {
    id: "bad",
    mount() { calls.push("bad:mount"); },
    render() { throw new Error("boom"); },
  };
  const state: AppState = { active: undefined, activeId: null, clusterCount: 0 };
  const run = mountAll([good, bad]);
  expect(calls).toEqual(["good:mount", "bad:mount"]);
  // A throwing render must not prevent the other components from rendering.
  run(state);
  expect(calls).toContain("good:render");
});

// ─── store (single source of truth) ─────────────────────────────────────────
function fakeCluster(id: string): any {
  return { id, node_ids: [], nodes: {}, config: { strategy: "leaderless" }, metrics: {}, partitions: {} };
}

test("store adopt/getActive/clear + notify", () => {
  store.clear();
  let notified = 0;
  const unsub = store.subscribe(() => notified++);
  store.adopt(fakeCluster("c1") as any);
  expect(store.getActive()?.id).toBe("c1");
  expect(store.state().activeId).toBe("c1");
  expect(store.state().clusterCount).toBe(1);
  store.clear();
  expect(store.getActive()).toBeUndefined();
  expect(notified).toBeGreaterThanOrEqual(2);
  unsub();
});

test("store.handleEvent buffers events and caps at maxEvents", () => {
  store.clear();
  store.maxEvents = 3;
  for (let i = 0; i < 5; i++) {
    // untracked cluster id -> no api refresh, just event buffering
    store.handleEvent({ type: "write_received", cluster_id: "none", timestamp: new Date().toISOString() } as SimEvent);
  }
  expect(store.events.length).toBe(3);
  store.maxEvents = 500;
});
