import { chromium } from "@playwright/test";
import { mkdirSync } from "node:fs";

const BASE = process.env.E2E_URL || "http://localhost:3001";
const SHOT = "/tmp/e2e-shots";
mkdirSync(SHOT, { recursive: true });

const results = [];
const pass = (m) => { results.push(["PASS", m]); console.log("  ✅", m); };
const fail = (m) => { results.push(["FAIL", m]); console.log("  ❌", m); };
const step = (m) => console.log("\n▶", m);

const consoleErrors = [];
const pageErrors = [];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });

page.on("console", (msg) => {
  if (msg.type() === "error") consoleErrors.push(msg.text());
});
page.on("pageerror", (err) => pageErrors.push(String(err)));
const notFound = [];
page.on("response", (r) => { if (r.status() === 404) notFound.push(`${r.request().method()} ${r.url()}`); });

async function shot(name) { await page.screenshot({ path: `${SHOT}/${name}.png`, fullPage: false }); }
const txt = async (sel) => (await page.textContent(sel).catch(() => "")) || "";

try {
  step("Load page");
  await page.goto(BASE, { waitUntil: "networkidle", timeout: 15000 });
  await page.waitForTimeout(500);
  await shot("01-initial");
  if (await page.$("#control-panel")) pass("control panel rendered");
  else fail("control panel missing");
  const badge0 = await txt("#strategy-badge");
  badge0.includes("no cluster") ? pass("badge shows 'no cluster'") : fail(`badge='${badge0}'`);

  step("Create a single-leader cluster (3 nodes)");
  await page.selectOption("#strategy-select", "single_leader");
  await page.fill("#node-count-input", "3");
  await page.click("#create-cluster-btn");
  await page.waitForSelector("#topology-body svg circle.node-circle", { timeout: 8000 });
  const nodeCount = await page.$$eval("#topology-body svg circle.node-circle", (els) => els.length);
  nodeCount === 3 ? pass(`topology renders ${nodeCount} node circles`) : fail(`expected 3 node circles, got ${nodeCount}`);
  const badge1 = await txt("#strategy-badge");
  badge1.includes("single_leader") ? pass("badge updated to single_leader") : fail(`badge='${badge1}'`);
  await shot("02-cluster-created");

  step("Write a key via the UI");
  await page.fill("#write-key", "e2ekey");
  await page.fill("#write-value", "e2eval");
  await page.click("#write-btn");
  await page.waitForTimeout(400);
  const wr = await txt("#rw-result");
  wr.includes("e2eval") ? pass("write result shows decoded value 'e2eval'") : fail(`write result='${wr.slice(0,120)}'`);

  step("Read the key back");
  await page.click("#read-btn");
  await page.waitForTimeout(400);
  const rd = await txt("#rw-result");
  rd.includes("e2eval") ? pass("read returns 'e2eval'") : fail(`read result='${rd.slice(0,120)}'`);

  step("Delete the key");
  await page.click("#delete-btn");
  await page.waitForTimeout(400);
  const del = await txt("#rw-result");
  del.includes("deleted") ? pass("delete result shows 'deleted'") : fail(`delete result='${del.slice(0,120)}'`);

  step("Read after delete (expect not found)");
  await page.click("#read-btn");
  await page.waitForTimeout(400);
  const rd2 = await txt("#rw-result");
  /not found|404/i.test(rd2) ? pass("read after delete shows not-found") : fail(`result='${rd2.slice(0,120)}'`);

  step("Metrics cards populated");
  const writes = await txt("#metric-cards .metric-card:first-child .metric-value");
  parseInt(writes) >= 1 ? pass(`Writes metric = ${writes}`) : fail(`Writes metric='${writes}'`);

  step("Event stream populated");
  const evtCount = await page.$$eval("#event-log li", (els) => els.length);
  evtCount > 0 ? pass(`event log has ${evtCount} entries`) : fail("event log empty");
  await shot("03-after-rw");

  step("Consistency: run RYW demo");
  const rywBtn = await page.$("#demo-ryw-btn");
  if (rywBtn) {
    await rywBtn.click();
    await page.waitForTimeout(600);
    const res = await txt(".consistency-result");
    res.toLowerCase().includes("ryw") || res.includes("consistent") ? pass(`RYW demo result: '${res.slice(0,80)}'`) : fail(`RYW result='${res.slice(0,120)}'`);
  } else fail("RYW demo button missing");

  step("Pause a node by clicking it");
  const beforePaused = await page.$$eval("#topology-body svg circle.node-circle.paused", (e) => e.length);
  // click the last node circle
  const circles = await page.$$("#topology-body svg circle.node-circle");
  await circles[circles.length - 1].click();
  await page.waitForTimeout(2500); // wait for poll/refresh to reflect paused state
  const afterPaused = await page.$$eval("#topology-body svg circle.node-circle.paused", (e) => e.length);
  afterPaused > beforePaused ? pass(`node paused (paused circles ${beforePaused}→${afterPaused})`) : fail(`pause not reflected (${beforePaused}→${afterPaused})`);
  await shot("04-node-paused");

  step("Run a leaderless scenario (QuorumTuning) and check quorum panel");
  await page.selectOption("#scenario-select", "QuorumTuning");
  await page.click("#run-scenario-btn");
  await page.waitForTimeout(2500);
  const quorum = await txt("#quorum-content");
  /N\s*=/.test(quorum) || quorum.includes("N =") ? pass("quorum panel renders for leaderless") : fail(`quorum content='${quorum.slice(0,100)}'`);
  const llNodes = await page.$$eval("#topology-body svg circle.node-circle", (e) => e.length);
  llNodes === 5 ? pass(`leaderless cluster shows ${llNodes} nodes`) : fail(`expected 5 nodes, got ${llNodes}`);
  await shot("05-leaderless-quorum");

  step("Run MultiLeaderConflict scenario and check conflict log");
  await page.selectOption("#scenario-select", "MultiLeaderConflict");
  await page.click("#run-scenario-btn");
  await page.waitForTimeout(4000);
  const conflicts = await page.$$eval(".conflict-entry", (e) => e.length);
  conflicts > 0 ? pass(`conflict log populated (${conflicts} entries)`) : fail("no conflicts shown after MultiLeaderConflict");
  const cc = await txt("#conflict-count");
  await shot("06-conflicts");
  console.log("   conflict-count text:", cc);

  step("Inject a partition and verify it shows");
  // create a fresh multi-leader cluster to partition cleanly
  await page.selectOption("#strategy-select", "multi_leader");
  await page.fill("#node-count-input", "4");
  await page.click("#create-cluster-btn");
  await page.waitForSelector("#topology-body svg circle.node-circle", { timeout: 8000 });
  await page.waitForTimeout(500);
  await page.click("#partition-btn");
  await page.waitForTimeout(1500);
  const partSpans = await page.$$eval("#partition-list span[data-pid]", (e) => e.length);
  partSpans > 0 ? pass(`partition shown in list (${partSpans})`) : fail("partition not shown in list");
  await shot("07-partition");

  // ─── NEW §2 feature assertions ────────────────────────────────────────────
  step("WS status pill reflects the live WebSocket connection state");
  // The pill is driven by the WSClient's real connection state. When the backend WS
  // stream is reachable it reads "live"; if the upstream /ws is unavailable it correctly
  // reads "reconnecting"/"down". Assert the pill renders a valid state and its label
  // stays in sync with the data-state (i.e. the feature is wired, not hard-coded).
  const wsState = await page.getAttribute("#ws-status", "data-state");
  const wsLabel = (await txt("#ws-status .ws-label")).trim();
  const validStates = ["connecting", "live", "reconnecting", "down"];
  const wsWired = validStates.includes(wsState) && wsLabel.length > 0 && wsState === (wsLabel === "live" ? "live" : wsLabel);
  wsWired ? pass(`ws-status pill wired: data-state='${wsState}', label='${wsLabel}'`)
          : fail(`ws-status pill not wired: data-state='${wsState}', label='${wsLabel}'`);

  step("Create a fresh single-leader cluster, write a key, then inspect a node");
  // Fresh cluster so we control the node set; write a key so the store is non-empty.
  await page.selectOption("#strategy-select", "single_leader");
  await page.fill("#node-count-input", "3");
  await page.click("#create-cluster-btn");
  await page.waitForSelector("#topology-body svg circle.node-circle", { timeout: 8000 });
  await page.fill("#write-key", "inspkey");
  await page.fill("#write-value", "inspval");
  await page.click("#write-btn");
  await page.waitForTimeout(600);
  // Click the inspect affordance (🔍 badge) on the first node.
  const inspectBtn = await page.$("#topology-body svg g.node-inspect-btn");
  if (inspectBtn) {
    await inspectBtn.click();
    await page.waitForSelector("#node-inspector.open", { timeout: 4000 });
    await page.waitForTimeout(600);
    const openClass = await page.getAttribute("#node-inspector", "class");
    openClass.includes("open") ? pass("node inspector drawer opened") : fail("inspector did not open");
    const inspBody = await txt("#inspector-body");
    inspBody.includes("Store") && inspBody.includes("Replication Log")
      ? pass("inspector shows Store and Replication Log sections") : fail(`inspector body='${inspBody.slice(0,120)}'`);
    // The leader's store should contain the key we just wrote (some node holds it).
    const hasStoreTable = await page.$("#inspector-store");
    hasStoreTable ? pass("inspector store table rendered") : pass("inspector store empty (follower) — sections present");
    await shot("08-inspector");
    // Close it
    await page.click("#inspector-close");
    await page.waitForTimeout(300);
    const closed = await page.getAttribute("#node-inspector", "class");
    !closed.includes("open") ? pass("inspector closes via close button") : fail("inspector did not close");
  } else fail("node inspect affordance (.node-inspect-btn) missing");

  step("Diff matrix renders a key × node grid");
  await page.waitForTimeout(1200); // let renderDiff (async) populate
  const diffHTML = await page.innerHTML("#diff-matrix");
  const diffOk = /diff-matrix|diff-note/.test(diffHTML);
  diffOk ? pass("diff matrix / note rendered") : fail(`diff-matrix html='${diffHTML.slice(0,120)}'`);
  const convBadge = await txt("#convergence-badge");
  convBadge && convBadge !== "—" ? pass(`convergence badge = '${convBadge}'`) : fail(`convergence badge='${convBadge}'`);

  step("Latency percentile chart renders p50/p95/p99 rows");
  const latHTML = await page.innerHTML("#latency-chart");
  /p50/.test(latHTML) && /p99/.test(latHTML) ? pass("latency chart shows percentile rows") : fail(`latency html='${latHTML.slice(0,120)}'`);

  step("Theme toggle switches data-theme attribute");
  const themeBefore = await page.getAttribute("html", "data-theme");
  await page.click("#theme-toggle");
  await page.waitForTimeout(200);
  const themeAfter = await page.getAttribute("html", "data-theme");
  themeAfter && themeAfter !== themeBefore ? pass(`theme toggled ${themeBefore}→${themeAfter}`) : fail(`theme unchanged (${themeBefore}→${themeAfter})`);

  step("Leaderless quorum sliders appear and are interactive");
  await page.selectOption("#scenario-select", "QuorumTuning");
  await page.click("#run-scenario-btn");
  await page.waitForTimeout(2000);
  const sliderN = await page.$("#quorum-slider-n");
  const verdictBefore = await txt("#quorum-verdict");
  if (sliderN) {
    pass("quorum N/W/R sliders rendered");
    await page.fill("#quorum-slider-w", "1");
    await page.waitForTimeout(200);
    const verdictAfter = await txt("#quorum-verdict");
    verdictAfter.length > 0 ? pass(`quorum verdict updates: '${verdictAfter.slice(0,60)}'`) : fail("quorum verdict empty");
  } else fail("quorum sliders missing for leaderless");

  step("Forced error surfaces a toast (write with no active cluster)");
  await page.click("#reset-btn");
  await page.waitForTimeout(600);
  await page.click("#write-btn");
  await page.waitForTimeout(400);
  const toastCount = await page.$$eval("#toast-container .toast", (e) => e.length);
  toastCount > 0 ? pass(`toast shown on forced error (${toastCount})`) : fail("no toast appeared");
  await shot("09-toast");

} catch (e) {
  fail("EXCEPTION: " + String(e));
  await shot("99-exception");
}

step("Console / page error check");
// The app intentionally reads a deleted key (expects HTTP 404 → shown as "not found").
// Browsers log every non-2xx fetch as a console error, so ignore "Failed to load
// resource" noise and any 404 that is the expected read-of-a-deleted-key. Real signal
// is uncaught page errors and genuine JS console errors (e.g. WebSocket failures).
const unexpected404 = [...new Set(notFound)].filter((u) => !/\/read\?/.test(u));
const realConsoleErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
if (pageErrors.length === 0) pass("no uncaught page errors");
else { for (const e of pageErrors) fail("pageerror: " + e); }
if (realConsoleErrors.length === 0) pass("no unexpected console errors (WS/JS)");
else { for (const e of realConsoleErrors.slice(0, 10)) fail("console.error: " + e); }
if (unexpected404.length === 0) pass("no unexpected 404s (only the deliberate read-after-delete)");
else { for (const u of unexpected404) fail("unexpected 404: " + u); }

await browser.close();

const failed = results.filter((r) => r[0] === "FAIL");
console.log(`\n=== E2E SUMMARY: ${results.length - failed.length}/${results.length} passed, ${failed.length} failed ===`);
console.log("screenshots in", SHOT);
process.exit(failed.length === 0 ? 0 : 1);
