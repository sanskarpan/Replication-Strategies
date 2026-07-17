# Known Issues — Replication-Strategies

Audit of the Go replication simulator + Bun/TS frontend. Severity ranked by blast
radius on correctness/pedagogy (this is a local, unauthenticated dev tool with no
sensitive data). Each issue records status, location, root cause, and fix.

Legend: `[ ]` open · `[x]` fixed (with regression test)

---

## Critical

### ISSUE-1 — Anti-entropy manufactures conflicts on identical/converged entries
- **Status:** [x] fixed — equal-VClock short-circuit in `receiveRemoteWrite`; test `TestISSUE1_AntiEntropy_NoSpuriousConflicts`
- **Category:** Logic
- **Location:** `internal/node/multileader.go` (`runAntiEntropy`, `receiveRemoteWrite`), `internal/storage/entry.go` (`HappensBefore`)
- **Root cause:** Every 500ms each node re-broadcasts its whole store. A peer that already
  holds an identical entry has an *equal* vector clock. `HappensBefore` returns false for
  equal clocks, so `receiveRemoteWrite` falls through to the "concurrent → conflict" branch.
  Every converged key generates a spurious conflict+resolution on every tick, forever.
- **Evidence:** 2-node cluster, one write, idle → 8 conflicts recorded (expected 0).
- **Fix:** Short-circuit when clocks are equal (and/or same value+timestamp); ideally make
  anti-entropy diff-based instead of blasting the full store.

## High

### ISSUE-2 — Leaderless read with a local miss always hits the full timeout
- **Status:** [x] fixed — derive remoteNeeded from actual local hits; test `TestISSUE2_Leaderless_ReadLocalMiss_NoTimeout`
- **Category:** Logic
- **Location:** `internal/node/leaderless.go` (`Read`)
- **Root cause:** Always queries `R-1` peers assuming self supplies one response. When the
  coordinator has no local copy, `needed` becomes `R` but only `R-1` nodes were contacted,
  so the collection loop can never reach `R` → blocks until the 500ms timeout.
- **Evidence:** N=5,R=3; pause node0, write via node1, resume node0, read node0 → 501ms.
- **Fix:** Query enough targets to satisfy the number of responses actually required;
  decouple "responses needed" from "self counts as one".

### ISSUE-3 — Writes report success even when quorum/acks were never met
- **Status:** [x] fixed — leaderless returns error when ackCount<W; leader sync/semi-sync return error on incomplete acks; tests `TestISSUE3_*`
- **Category:** Logic/Contract
- **Location:** `internal/node/leaderless.go` (`Write`), `internal/node/leader.go` (`Write` sync/semi-sync)
- **Root cause:** Leaderless returns `(entry, nil)` after publishing `EvtQuorumFailed`;
  single-leader sync/semi-sync return `nil` on ack timeout. Callers see success for writes
  that did not meet their durability contract.
- **Fix:** Return an error when `ackCount < W` / sync acks incomplete; surface as HTTP 5xx/409.

### ISSUE-4 — Hinted handoff is a fake feature (dead code)
- **Status:** [x] fixed — `collectHints` buffers hints for unacked targets after a background grace window; delivered by `runHintedHandoff`; test `TestISSUE4_Leaderless_HintedHandoff_DeliversOnRecovery`
- **Category:** Logic/Contract
- **Location:** `internal/node/leaderless.go` (`hints`, `runHintedHandoff`, `deliverHints`)
- **Root cause:** `n.hints` is never written to, so no hints are ever stored/delivered even
  though the delivery loop, events, and message handler exist.
- **Fix:** Populate `n.hints[target]` in `Write` when a target is unreachable (partitioned/
  paused/unacked), then deliver on heal.

### ISSUE-5 — `Store.Delete` mutates shared entries in place + deletes never propagate
- **Status:** [x] fixed — Delete is now copy-on-write; added end-to-end `Delete` (interface + all node types + orchestrator + `DELETE /clusters/{id}/kv`); tests `TestStore_DeleteIsCopyOnWrite`, `TestISSUE5_*`
- **Category:** Race/Data
- **Location:** `internal/storage/store.go` (`Delete`)
- **Root cause:** `Set` is copy-on-write (safe) but `Delete` mutates the existing `*KVEntry`
  in place while readers hold shared pointers → data race if exercised. Also `Store.Delete`
  is never called and `OpDelete` never propagates as a tombstone.
- **Fix:** Make `Delete` copy-on-write; wire real tombstone replication (at least single-leader
  via `OpDelete` log entries).

### ISSUE-6 — `config.yaml` is never loaded; documented limits unenforced
- **Status:** [x] fixed — config package loads config.yaml; `SetMaxClusters` enforced in CreateCluster; test `TestISSUE6_MaxClustersEnforced`
- **Category:** Contract/Security
- **Location:** `cmd/server/main.go` vs `config.yaml`
- **Root cause:** No YAML parsing anywhere. `max_clusters`, `cors_origins`, and all defaults
  are decorative. No cap on cluster creation → unbounded goroutine/memory growth.
- **Fix:** Load and honor config (at minimum enforce `max_clusters` in `CreateCluster`).

### ISSUE-7 — `PATCH /config` races on cluster state and has no effect
- **Status:** [x] fixed — PATCH locks c.mu and calls `SetMode` on the live leader; test `TestISSUE7_ConfigPatch_ChangesLiveMode`
- **Category:** Race/Contract
- **Location:** `gateway/rest.go` (`handleClusterConfig`), `internal/node/leader.go`
- **Root cause:** Writes `c.Config` without `c.mu` (races with `GetState`), and
  `SingleLeaderNode.mode` is captured at construction and never re-read, so the knob is inert.
- **Fix:** Lock the write, and push the new mode into the live node via a setter.

## Medium

### ISSUE-8 — Leaderless reconciliation has no tiebreak → permanent divergence
- **Status:** [x] fixed — `entryWins` (ts,NodeID) total order in applyWrite + reconcile; test `TestEntryWins_TiebreakByNodeID`
- **Category:** Data
- **Location:** `internal/node/leaderless.go` (`applyWrite`, `Read` reconcile)
- **Root cause:** Uses strict `>` on timestamp only; equal timestamps never converge and
  read-repair can't pick a winner. Inconsistent with `LWWResolver`'s `(ts, nodeID)` order.
- **Fix:** Apply `(Timestamp, NodeID)` total order in both apply and reconcile.

### ISSUE-9 — Read-repair never repairs the coordinator's own stale copy
- **Status:** [x] fixed — coordinator repairs its own stale copy in Read; test `TestISSUE9_Leaderless_ReadRepairsCoordinatorLocal`
- **Category:** Data
- **Location:** `internal/node/leaderless.go` (`Read`)
- **Root cause:** Stale set computed only from remote responses; a stale local copy is left
  unrepaired.
- **Fix:** If local is older than `best`, apply `best` locally too.

### ISSUE-10 — Multi-leader lost-update race: remote apply bypasses the write lock
- **Status:** [x] fixed — dedicated `applyMu` serialises local write vs remote apply; test `TestISSUE10_MultiLeader_ConcurrentWritesConverge`
- **Category:** Race
- **Location:** `internal/node/multileader.go` (`Write` holds `n.mu`, `receiveRemoteWrite` doesn't)
- **Root cause:** The VClock read-modify-write in `Write` is guarded by `n.mu`, but
  `receiveRemoteWrite` mutates the same key without it → interleaving lost update.
- **Fix:** Serialize per-key mutations on both local-write and remote-apply paths.

### ISSUE-11 — Consistency guarantees compare non-comparable per-store `Version`
- **Status:** [x] fixed — **reworked to vector clocks**: guarantees now compare `VClock.Dominates`/`HappensBefore` (globally comparable across nodes); leader stamps a monotonic vector clock; tests `TestRYW_VectorClock_CrossNode`, `TestMonotonic_VectorClock`, `TestConsistentPrefix_VectorClock`, `TestVectorClock_Dominates`
- **Category:** Logic
- **Location:** `internal/consistency/*.go`, `internal/storage/store.go`
- **Root cause:** For leaderless/multi-leader, `Version` is a per-store counter, not globally
  comparable, so cross-node RYW/monotonic/consistent-prefix checks are meaningless.
- **Fix:** Track causal position via vector clock (or a globally meaningful version) for
  cross-node guarantees; document single-leader (leader log index) as the supported case.

### ISSUE-12 — Followers silently lose entries on drop; lag metric under-reports
- **Status:** [x] fixed — follower keeps leader indices, buffers gaps, periodic `runSyncLoop` catch-up + leader `MsgSync` resend (also fixed leader stamping entry.Index); test `TestISSUE12_Follower_RecoversDroppedEntries`
- **Category:** Data
- **Location:** `internal/node/follower.go` (`applyEntries`), `internal/replication/log.go`
- **Root cause:** Follower re-indexes locally, so a dropped `AppendEntries` leaves a hole but
  the log looks dense; lag (count-based) understates the gap and there's no catch-up.
- **Fix:** Preserve leader indices, detect gaps, implement catch-up (`MsgSync`).

### ISSUE-13 — Fabric reorders in-order messages
- **Status:** [x] fixed — per-link FIFO delivery with monotonic deliverAt in fabric; test `TestISSUE13_Fabric_PreservesLinkOrder`
- **Category:** Race/Logic
- **Location:** `internal/transport/fabric.go` (`Send`)
- **Root cause:** Each delayed message is delivered from its own goroutine, so messages to the
  same target race on wake-up, breaking FIFO that single-leader assumes.
- **Fix:** Per-target ordered delivery queue.

### ISSUE-14 — `AddNode` (leaderless) leaves quorum config inconsistent
- **Status:** [x] fixed — AddNode updates all leaderless nodes' quorum config; test `TestISSUE14_AddNode_QuorumStaysConsistent`
- **Category:** Logic
- **Location:** `internal/simulation/orchestrator.go` (`AddNode`)
- **Root cause:** New node gets `N+1`; existing nodes keep old `qConfig` and are never updated.
- **Fix:** Recompute and `UpdateQuorum` on all existing leaderless nodes after add.

### ISSUE-15 — No request body size limit (memory DoS)
- **Status:** [x] fixed — chi `middleware.RequestSize(1MiB)` caps request bodies; `decodeJSON` returns **413** on overflow (400 for malformed); test `TestGateway_BodyTooLarge_Returns413`
- **Category:** Security
- **Location:** `gateway/rest.go` (all `json.NewDecoder(r.Body)`)
- **Fix:** Wrap with `http.MaxBytesReader`; cap batch length.

### ISSUE-16 — Frontend query params not URL-encoded
- **Status:** [x] fixed — `read()` builds query via URLSearchParams (URL-encoded)
- **Category:** Logic
- **Location:** `frontend/src/api/client.ts` (`read`)
- **Fix:** `encodeURIComponent` each value (or `URLSearchParams`).

## Low / cleanup

### ISSUE-17 — `quorum.choose()` int overflow for large N
- **Status:** [x] fixed — `choose` now computes in float64 (no overflow); quorum tests green · **Location:** `internal/quorum/calculator.go`
- **Fix:** Guard N range or use float/`math/big`.

### ISSUE-18 — Dead code: `SingleLeaderEngine`, `FollowerNode.updateLag`
- **Status:** [x] fixed — removed dead `SingleLeaderEngine` (deleted single_leader.go) and `FollowerNode.updateLag` · **Location:** `internal/replication/single_leader.go`, `internal/node/follower.go`
- **Fix:** Remove unused code.

### ISSUE-19 — `handleWrite` returns HTTP 500 for user errors
- **Status:** [x] fixed — `handleWrite` returns 409 for write failures and 400 for missing key (was 500) · **Location:** `gateway/rest.go` (`handleWrite`)
- **Fix:** Map client errors (write to follower, not found) to 4xx.

### ISSUE-20 — `EventBus.GetRecent` O(n²); buffer eviction leaks backing array
- **Status:** [x] fixed — `GetRecent` collects newest-first then reverses once — O(n) · **Location:** `internal/events/bus.go`
- **Fix:** Build slice forward then reverse; use ring buffer or reslice+copy.

### ISSUE-21 — `handleRunScenario` ignores `GetCluster` error → nil deref
- **Status:** [x] fixed — `handleRunScenario` now checks the GetCluster error (no nil deref) · **Location:** `gateway/rest.go` (`handleRunScenario`)
- **Fix:** Check the error and return 500 if the cluster vanished.

### ISSUE-22 — Contract drift: `writeBatch` response shape; BFF forwards `Host`
- **Status:** [x] fixed — client `writeBatch` type matches backend `{results}`; added `deleteKey`; BFF forwards only safe headers (no Host) · **Location:** `frontend/src/api/client.ts`, `frontend/server/bff.ts`
- **Fix:** Align the TS type with the backend `{results}` shape; strip hop-by-hop headers.

### ISSUE-23 — VClock `Increment`/`Merge` mutate the receiver in place (aliasing footgun)
- **Status:** [x] fixed — documented in-place mutation on VectorClock `Increment`/`Merge` (Clone before sharing) · **Location:** `internal/storage/entry.go`
- **Fix:** Prefer non-mutating semantics or document/clone at call sites that share the map.

## Found during E2E verification (post-audit)

### ISSUE-24 — BFF served raw TypeScript; browser could not load the app
- **Status:** [x] fixed — BFF now bundles `main.ts` via `Bun.build` before serving `/main.js`; also made backend URL env-configurable (`BACKEND`) and added a Delete button wired to the new endpoint.
- **Category:** Contract
- **Location:** `frontend/server/bff.ts` (`/main.js` route), `frontend/src/index.html`
- **Root cause:** `index.html` loads `<script type="module" src="/main.js">`, but the BFF served the raw `src/main.ts` file, which contains bare/extensionless imports (`import * as d3 from "d3"`, `./api/client`). Browsers cannot resolve those, so the page rendered nothing. API tests could not catch it because they never load the page.
- **Fix:** Bundle the entrypoint (d3 + local modules inlined) and serve the resolvable output; verified `/main.js` now contains 0 bare imports and the full write/read/delete cycle works through the proxy.

### ISSUE-25 — Data race on NodeMetrics flags (found by stress test)
- **Status:** [x] fixed — added locked `Lag()`/`SetLeader()`/`SetOnline()` accessors; `GetState`/`GetLag`/`setRole`/`Pause`/`Resume`/`Stop` now use them. `ReplicaLag`/`IsLeader`/`IsOnline` were written under `BaseNode.mu` but read under `metrics.mu` (different locks). Caught by `TestStress_SustainedConcurrency` under `-race`.

### ISSUE-26 — Fabric link-worker goroutine leak (regression from ISSUE-13 fix)
- **Status:** [x] fixed — `NetworkFabric.Close()` stops all link workers (select on `done`); called from `DeleteCluster`. The per-link FIFO workers ran `for range l.ch` forever and leaked one goroutine per link after cluster deletion. Verified by the goroutine-baseline assertion in `TestStress_SustainedConcurrency`.

### ISSUE-27 — WebSocket hardcoded to :8080; live events broken (found by Playwright)
- **Status:** [x] fixed — `WSClient` now connects same-origin (`ws(s)://<host>/ws`) and the BFF proxies `/ws` to the backend. Previously it hardcoded `ws://<host>:8080/ws`, which bypassed the BFF and failed for any non-8080 backend → the event stream was empty and no live updates arrived. Verified in-browser: event log populates (17+ entries), conflicts/partitions stream live.
- **Category:** Contract
- **Location:** `frontend/src/ws/client.ts`, `frontend/server/bff.ts`

### ISSUE-28 — D3 topology re-animated every poll; nodes never settled (found by Playwright)
- **Status:** [x] fixed — the force simulation is now rebuilt only when the node *structure* changes; state/lag/partition changes update in place via `updateTopoVisuals`. Previously `renderTopology` recreated the simulation on every 2s poll, so nodes jiggled perpetually and could not be clicked (Playwright: "element is not stable"). Verified: node click-to-pause now works.
- **Category:** Logic (UI)
- **Location:** `frontend/src/main.ts`

### ISSUE-29 — Missing favicon route caused console 404 (found by Playwright)
- **Status:** [x] fixed — BFF answers `/favicon.ico` with 204. Cosmetic; removes a console 404.
- **Location:** `frontend/server/bff.ts`

## Second deep-audit pass (4 parallel review agents + adversarial re-review)

Real bugs found and fixed (agents also raised ~15 false-positives/design-nitpicks that were
verified as non-issues — see notes at end).

### ISSUE-30 — Leaderless: partially-replicated delete resurrected by a stale replica
- **Status:** [x] fixed — `MsgClientRead` and the coordinator read now use `GetRaw` so tombstones participate in reconciliation; a tombstone winner returns not-found (after repairing stale replicas). **Severity: High.** Test `TestISSUE30_Leaderless_TombstoneNoResurrection`.
- **Location:** `internal/node/leaderless.go` (`HandleMessage` MsgClientRead, `Read`)

### ISSUE-31 — Leaderless: `applyWrite` compare-and-set was not atomic (lost update / non-convergence)
- **Status:** [x] fixed — added `applyMu` held across the GetRaw→entryWins→Set. A logical (not memory) race that `-race` can't catch since each store op is individually locked. **Severity: High.**
- **Location:** `internal/node/leaderless.go` (`applyWrite`)

### ISSUE-32 — Store shallow-copied entries, sharing the mutable VClock map across stores/peers
- **Status:** [x] fixed — `Set`/`Delete` now deep-copy `VClock`. Latent (everyone currently clones before mutating) but a real aliasing footgun via anti-entropy broadcasting shared pointers. **Severity: Medium.**
- **Location:** `internal/storage/store.go`

### ISSUE-33 — `PATCH /config` mutated stored config for non-single-leader clusters (no effect, returned 200)
- **Status:** [x] fixed — rejected with 400 for non-single-leader. Test `TestGateway_ConfigPatchRejectsNonSingleLeader`. **Severity: Medium.**
- **Location:** `gateway/rest.go`

### ISSUE-34 — Multi-leader: resolved conflict winner didn't dominate both parents
- **Status:** [x] fixed — the stored winner keeps its value but merges both parents' vector clocks, so all replicas converge to one dominating clock and future conflicts aren't hidden. Also removed dead `mergeVClock` no-op. **Severity: Medium.**
- **Location:** `internal/node/multileader.go`

### ISSUE-35 — Leaderless read set was deterministic (lowest IDs), so read-repair couldn't converge a high-ID replica
- **Status:** [x] fixed — `getReadTargets` now randomly samples, so every replica is eventually read and repaired. **Severity: Medium.**
- **Location:** `internal/node/leaderless.go`

### ISSUE-36 — Single-leader `Delete` ignored replication mode (always async)
- **Status:** [x] fixed — extracted `awaitReplication`; deletes now honor sync/semi-sync durability like writes. Test `TestISSUE31_SingleLeader_SyncDeleteIncomplete_Errors`. **Severity: Medium.**
- **Location:** `internal/node/leader.go`

### ISSUE-37 — Monotonic/consistent-prefix used strict HappensBefore (missed concurrent regressions)
- **Status:** [x] fixed — now flag `!Dominates(seen)` (behind OR sideways). Identical for the single-leader total order, more correct generally. **Severity: Low.**
- **Location:** `internal/consistency/monotonic_reads.go`, `consistent_prefix.go`

### ISSUE-38 — CRDT GCounter merge produced a non-deterministic winner NodeID; config didn't re-default all fields
- **Status:** [x] fixed — merged NodeID is now `max(local,remote)` (deterministic across nodes); `config.Load` re-defaults all numeric fields. **Severity: Low.**
- **Location:** `internal/conflict/crdt.go`, `internal/config/config.go`

### ISSUE-39 — Frontend consistency panel rebuilt every 2s, wiping the mode dropdown / demo results
- **Status:** [x] fixed — added a shape-signature guard (incl. cluster id) like the topology one. **Severity: Medium (UX).**
- **Location:** `frontend/src/main.ts`

### Verified NON-issues / deliberate design choices (not changed)
- Agent-claimed data races on `leaderID` (write-once before goroutines start), the `acked` map handed to `collectHints` (goroutine-start happens-before), and `seqNo` reuse (uint64, retracted by the agent) — all confirmed safe; `-race` suite + soak stay clean.
- `max_clusters` TOCTOU — the write-lock recheck makes it sound (only a transient build-then-reject cost under burst).
- Fabric drops on a full inbox / sticky per-link latency — intentional simulator behavior.
- Leaderless read returns best-effort below R (Dynamo sloppy-quorum) — deliberate; writes are strict because reporting non-durable writes as success is clearly wrong, reads returning the freshest-available value is not.
- Follower replication *log* uses local indices for display (store Version uses the leader index and is correct; followers never serve their log) — cosmetic, left as-is.
- CRDT resolver inferring GCounter from payload shape (`{"counts":...}`) — known limitation of the demo resolver; documented, not changed.

## Caveat fixes (previously deliberate limitations, now resolved)

### ISSUE-40 — CRDT resolver inferred counter-ness from payload shape
- **Status:** [x] fixed — GCounter now carries an explicit `crdt_type: "gcounter"` tag; `CRDTResolver` only merges when BOTH values are tagged, otherwise resolves the value opaquely via LWW. Arbitrary JSON containing a `"counts"` object is no longer silently CRDT-merged. Also the merged winner NodeID is deterministic (`max`). Test `TestCRDTResolver_RequiresTypeTag`.
- **Location:** `internal/conflict/crdt.go`

### ISSUE-41 — Leaderless reads returned best-effort below R (sloppy quorum)
- **Status:** [x] fixed — a read now counts self plus each remote responder and, if it cannot gather R responses, returns a `read quorum not met` error instead of possibly-stale data — symmetric with the strict write path (which errors when W acks aren't met). Test `TestISSUE32_Leaderless_ReadQuorumNotMet`.
- **Location:** `internal/node/leaderless.go` (`Read`)
