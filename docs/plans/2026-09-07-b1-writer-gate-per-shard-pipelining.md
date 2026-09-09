# B1 — Writer-gate per-shard pipelining (rev 3)

Date: 2026-09-07 (rev 2: 2026-09-08; rev 3: 2026-09-08)
Status: **executed** — Tasks 1–5 and hotfix 4b carried out 2026-09-08/09 on
branch `perf/b1-writer-gate-pipelining` (work left uncommitted for review).
Remaining: the Task 6 sanitizer gate (ASan ×3, TSan ×3) and the human-run
measurement — see §Task 6 for the exact commands. Execution findings and
their dispositions are recorded in `PLAN_NOTES.md` and folded into the
tasks below (anchor corrections in Edits 1.4/2.5, the Task 3 red-reason
correction, and new Task 4b).
Follows: 2026-09-05-request-level-commit-batching.md (two-epoch coordinator,
request-wide writer gates).

## Execution record (2026-09-08/09)

| step | result |
|---|---|
| Task 1 (G1 instrumentation + scenario 8) | red proof: `not ok 89 — G1: at most one writer gate held per thread` (counter at 8); suite otherwise green (13034/1) |
| Task 2 (seam retire/rename + scenario 9 + multi-shard crash test) | red proofs: scenario 9 (`B completes shard 1…` rf_wait_flag timeout), crash test (`request pauses on shard 1 before finalize` — pause never fires on the wave tree); suite 13048/4, exactly the known-red set |
| Task 3 (scenarios 10/11 first → per-shard flush helpers) | base-red proof for 10+11; after production edits: scenario 11 green, scenario 10 narrowed to `shard 0 hard-failed` (`ins[0].rc == 0`, the wave fold gap); suite 13067/5 |
| Task 4 (pipeline loop + revert audit) | all four scenarios green; full suite **13083/0**; mandatory revert→red→reapply→green audit passed (4 expected old-behavior failures, then green again) |
| Task 4b (hotfix, found during verification) | `test-parallel-index-integrity` flake → root-caused (see Task 4b) → fixed test-first; case 8/8 green; two consecutive full suites **13088/0** |
| Task 5 (docs sync) | applied; zero-match verify passed (also after rewording two new-text phrases that naively matched `whole request`) |
| Task 6 | fresh build + suite green (13088/0); sanitizer gate handed to the human |

## Measurement verdict (2026-09-09): **GATE FAILED — stop**

Controlled comparison on the same host, same `SHARD_TEST_TMPDIR=./db`,
same commands, 5 runs each (baseline = B1 stashed, main tree; medians):

| median, M rows/s | baseline (waves) | B1 (pipelines) | delta |
|---|---|---|---|
| single JSON | 1.02 | 0.44 | −57% |
| single CSV | 1.30 | 0.45 | −65% |
| parallel JSON | 0.70 | 0.43 | −39% |
| parallel CSV | 0.77 | 0.50 | −35% |

Every stop/go target missed (parallel ≥ 1.2 M required — actual is below
its own baseline; single-conn tolerance ±5%). The baseline runs reproduce
the 2026-09-07 measurement, ruling out environment drift. Diagnosis (from
the captured commit-phase metrics): the caller-serial pipeline replaces
wave-overlapped durability syncs with a per-request fsync-latency chain —
5 concurrent chains overlap but contend on the shared stream files /
marker dir, so aggregate throughput is bounded by one chain's duration
(parallel wall ≈ single-conn wall). The design hypothesis "cross-request
pipelining + per-file fan-out outweighs within-request shard-wave
parallelism" is falsified at this shape. Per the stop rule: no tuning
without a new approved plan step; disposition of the working tree is the
human's call (full revert / keep wave-compatible pieces only / redesign
plan).

## Revision summary — review findings → resolutions

| # | Review finding (2026-09-08) | Resolution in this revision |
|---|---|---|
| 1 | Critical: pool-task scheduler contradicted "one gate per request" (I1) | Scheduler replaced: the request's **caller thread** loops over touched shards ascending and runs one complete pipeline per shard. No pool tasks, no request-level scheduler, no parked workers. G1 holds by construction. Nested per-file I/O still fans out via `parallel_for_io` because the caller is never a parallel.c pool worker (documented exception: nested callers — §Nested callers). |
| 2 | High: "any failure → EINPROGRESS" conflates pre-M and post-M failures | Per-shard **outcome table** (§Failure semantics): pre-marker failures (stage, P sync, pre-M publish abort) are ordinary hard errors for that shard only; post-publication failures (M dir fsync, finalize, commit barriers, clear) retain markers → `EINPROGRESS`/degraded/`rc = -2`. Matches today's fold in `slotcask_bulk_request_execute` (`pending` vs `failed`). |
| 3 | High: request-wide `SHARD_TEST_PHASE_REQ_PUBLISHED` cannot survive | Seam explicitly **retired and replaced** by per-shard `SHARD_TEST_PHASE_SHARD_PUBLISHED` and shard-qualified `"shard-published-%03x"` pauses (between the shard's marker dir fsync and its finalize), with every consumer updated, and a TEST_BUILD gate-hold counter added for deterministic G1 enforcement. All other seams re-verified one by one (§Test seams). |
| 4 | High: cleanup must precede gate release | New `slotcask_bulk_shard_release`: every window's terminal hook (`commit_done` already fired in-step; `release_window` for retained windows; `abort_window` fired pre-M at abort time) and every per-shard free runs **inside** the pipeline while the gate is still held, immediately before `writer_gate_unlock`. Preserves today's property (release hooks fire before any gate unlock — `slotcask_bulk_request_end` comment, slotcask.c). |
| 5 | Medium: FIFO fairness claim unsupported | Removed. Writer gates are default `pthread_mutex_t` (`pthread_mutex_init(&db->writer_gates[s], NULL)`, slotcask.c:870); no fairness is claimed anywhere (§Deadlock and starvation). |
| 6 | CORE-PROCESS: not executable (no anchors, no code blocks, not test-first, no red/green proof, no inventory, no execution rules, wrong bench assignment, vague sanitizer gate) | This revision: every edit located by quoted anchor text; every changed function/struct/test is a complete code block; five new regression scenarios with explicit red→green proof procedure; full call-site/consumer inventory (§Inventory); embedded execution rules with exact build/test commands and the PLAN_NOTES halt rule; benches assigned to the human (§Measurement); sanitizer gate spelled out exactly (§Verification). |

### Rev 3 re-review corrections

| # | Re-review finding | Rev 3 correction |
|---|---|---|
| 1 | Critical use-after-free in the proposed public driver | Snapshot request aggregate fields before `slotcask_bulk_request_end`; also snapshot shard outcome scalars before per-shard cleanup. |
| 2 | Missing mandatory regression revert/prove/reapply | Task 4 now temporarily reverses all six production edits, proves the focused regressions red for the expected reasons, reapplies them, and proves green, with pasted output required. |
| 3 | Task 3's red-test ordering was impossible | Scenario tests are now Edit 3.1 and execute before production Edits 3.2, 3.2a, and 3.3–3.8, with separate red and green commands. |
| 4 | Incomplete/non-unique edit anchors | The dead wave block, public driver, and public comment are complete preimage/replacement blocks; all five seam consumers now have distinct context-bearing anchors. |
| 5 | Commands violated the repository's RTK rule | Every executable shell command is RTK-prefixed, including builds, tests, sanitizers, searches, and human benchmark handoff commands. |
| 6 | Intermediate `run-all` was mislabeled green despite known-red tests | Tasks 1–3 explicitly require the expected nonzero suite and distinguish only the named new failures from previously-green regressions. |
| 7 | Failure/errno/docs/crash coverage gaps | Corrected the post-link outcome row; capture the first errno explicitly; update every discovered live comment/doc including configuration; add the three-state multi-shard crash/recovery regression. |
| 8 | Invalid focused test and benchmark commands | Use registered durability cases; benchmark commands include `run`, explicit split counts, five repeats, baseline comparison, and numeric stop/go thresholds. |

## Problem (measured)

Concurrent bulk requests serialize almost end-to-end. At production shape
(1M kv, splits=8, disk-backed dir, 2026-09-07 numbers):

| shape | throughput |
|---|---|
| single-conn JSON | 1.09 M rows/s |
| single-conn CSV  | 1.28 M rows/s |
| 5-conn aggregate | 0.72–0.75 M rows/s |

Five concurrent requests should approach 5× the per-conn ceiling minus
shared-device costs; they reach ~0.7× of ONE connection instead.

**Root cause.** `slotcask_bulk_request_begin` (slotcask.c) acquires the
writer gate of *every touched shard* up front (ascending) and holds all of
them until `slotcask_bulk_request_end`, after the final commit barrier and
marker clears. Any large request hashes across every shard, so N concurrent
large requests mutually exclude almost completely: request B cannot stage a
single record until A has fully committed, cleared markers, and released
all 8 gates. Only request parsing overlaps. At splits=128 the same
mechanism plus device contention produced the historical 5× collapse
(12–16 s for 5×200k).

Single-record writers hold one gate around one mutation, so mixed
workloads also suffer: a 1-key upsert queues behind every shard of an
in-flight bulk request.

## Design

### Scheduler: caller-thread per-shard pipeline loop

A bulk request stops holding a gate set. `slotcask_bulk_request_execute`
sorts its inputs into ascending shard order and the **caller thread**
runs, for each touched shard, that shard's complete commit pipeline
before moving to the next:

```
for s in touched (ascending, on the caller thread):
  writer_gate_lock(s)
    gate replay of retained markers on s (unchanged, per shard)
    stage s (plan + segment writes; folded pre-grow)
    P sync of s's payload bytes                        [commit-input barrier]
    publish s's windows (marker link per window)       [commit point, unchanged]
    M dir fsync (s's markers durable)                  [commit point, unchanged]
    [SHARD_PUBLISHED seam]
    finalize s (A/I/K apply per window, syncs deferred)
    commit s (I sync, K msync, A/T syncs, marker clears for s + dir fsync,
      reclaim, commit_done)
    per-shard outcome fold (retained / step_failed / rc)
    terminal release (release_window for retained windows + frees)
  writer_gate_unlock(s)
```

Concurrent requests therefore pipeline across shards: request B runs
shard 5's pipeline while request A is still mid-request on shard 2.
Access to one shard serializes on that shard's gate exactly as it does
for a single-record writer today. Inside one pipeline, per-file I/O
(index sync fan-out via `index_sync_path_set`, segment passes) reuses
today's helpers unchanged.

**Why the caller thread and not one IO-pool task per shard** (rev 1's
model, rejected):

1. Pool tasks running shard pipelines concurrently would each hold a
   different shard's gate simultaneously for the *same request* — a
   direct violation of one-gate-per-request, and enforcing ordering
   inside them would need a request-level scheduler that parks pool
   workers.
2. `parallel_for_io` runs **inline** when the calling thread is a pool
   worker (src/db/parallel.c:362, the `t_in_pool_worker` guard). A shard
   pipeline dispatched as a pool task would serialize every nested
   `parallel_for_io` inside it (payload/segment passes, index sync
   issuer) — losing the fan-out the waves have today.
3. The request's caller is always a non-pool thread: the server command
   thread (server.c `worker_thread`, created via `db_thread_create`, not
   a parallel.c pool worker) or a test thread. So a caller-thread
   pipeline keeps the existing *per-file flush* fan-out.

This scheduler deliberately gives up a different kind of parallelism:
today `req_run_phase` fans stage, publish, and finalize out across all
touched shards, while B1 runs those shard bodies sequentially inside one
request. `bulk_stage_one_shard` / `bulk_phase3_stage_pending` and the
finalize loop do not contain compensating nested shard fan-out. The
performance hypothesis is therefore narrower: cross-request shard
pipelining plus retained per-file flush fan-out must outweigh the loss of
within-request shard-wave parallelism. This is a real design tradeoff, not
assumed free. Task 6 gives the human exact repeatable measurements and
numeric stop/go thresholds; missing either throughput target halts the
work without further tuning.

**Nested callers.** A request issued from *inside* a pool task (the
existing scenario 7a of test-request-flush-batching: nested
`parallel_for_io` task, 8 shard inputs, 2-worker pool) runs its
pipelines with nested per-file I/O inline — the same rule the wave
coordinator already lives with (`parallel_for_io` inline path). Scenario
7a must keep passing unchanged; no new behavior is introduced for it.

### Rejected variants (unchanged from rev 1)

- **Conservative per-shard release (release-early, acquire-all-up-front)
  rejected** — preserves full-span serialization; only fixes partial
  overlap.
- **Wave-scoped gates rejected** — dropping gates between waves lets a
  second request's stage gate-scan retained markers of the first
  request's published-but-uncommitted windows and force-converge its
  in-flight work; hook-state ownership becomes cross-request and
  replay's fail-closed identity checks were never designed for it.
  Gates must span plan→clear for a shard; that is the pipeline.
- **Pool-task pipelines rejected** — see above; this is the rev-1 model.

### Pipelining invariants

Numbered **G1–G5** to avoid colliding with the durability invariants
I1–I5 documented in docs/concepts/concurrency.md. The durability
invariants I1–I5 (index-durable-before-clear, payload-before-marker,
new-file-dirsync, marker-durable-before-mutation, per-shard exclusive
reservation) are per-window properties of the phase helpers and are
**unchanged**; each pipeline step maps onto them exactly as the wave it
replaces did (the step comments in §Code carry the [I*] tags).

- **G1 — At most one writer gate held per thread at any instant.** A
  request never holds gate X while waiting for gate Y: it releases X
  before acquiring Y. Enforced by construction (caller-thread loop) and
  asserted by the TEST_BUILD gate-hold counter (Task 1).
- **G2 — Clear before release.** Shard s's gate is released only after
  every window marker of s is durably cleared (unlink + dir fsync) and
  s's commit barriers returned success — or, on post-M failure, after
  the retained markers' terminal ownership has been transferred
  (release_window) and the per-shard state freed (G4). This preserves
  today's invariant "a gate holder sees no retained markers owned by the
  previous holder": retained markers always belong to unacked work whose
  hook state has been released. Replay stays order-insensitive;
  **D0 (durable per-shard marker sequence) remains not required** for B1
  (same argument as rev 1; marker-retaining async convergence stays
  parked).
- **G3 — Ascending, no re-acquisition.** Shards are pipelined in
  ascending order; a released shard is never re-entered by the same
  request. (A later concurrent request may hold a released shard's gate;
  it serialized on the gate — same as a single-record writer.)
- **G4 — Terminal cleanup before gate release.** Every window of shard s
  reaches exactly one terminal hook before s's gate is unlocked:
  `commit_done` (fired in the commit step after the dir-durable clear),
  `abort_window` (fired pre-marker at abort time, unchanged from
  `req_window_abort_pre_marker`), or `release_window` (retained
  windows, fired in `slotcask_bulk_shard_release` while the gate is held).
  Today this property is global ("Same coordinator thread that acquired
  the gates releases them after terminal hook and request-state cleanup
  has finished", slotcask.c `slotcask_bulk_request_end`); B1 makes it
  per-shard.
- **G5 — Single-record writers unchanged.** They take their shard's gate
  around one mutation; they now queue behind at most one shard's
  pipeline instead of a whole request.

### Deadlock and starvation

Wait-for graph: edges go *request → gate* only. A request waits for a
gate only while holding zero gates (it releases shard s's gate before
acquiring s+1's — G1/G3). Gate holders never wait on gates: a pipeline
runs to completion holding its own gate, and every lock it takes inside
(kf rwlock via `kf_shard_acquire`, bt/bm/segcache locks) is taken
*inside* the gate exactly as today's waves and single-record writers do
— gate-outermost ordering is unchanged, so no new lock-order edges and
no cycles. Pre-grow stays folded into stage under the request's own
gate (the circular-wait fix documented at slotcask.c
`slotcask_bulk_stage_shard`), and `slotcask_pregrow_kf` remains a
standalone gate-holder only for non-request callers.

**Starvation:** writer gates are default `pthread_mutex_t` — no FIFO
fairness is guaranteed and none is claimed. A unlucky writer can in
theory wait arbitrarily long, exactly as on the base branch (same mutex
type, same acquisition discipline); the practical bound improves because
each hold is one shard's pipeline instead of a whole request. No
fairness mechanism is introduced.

### Known costs (accepted, measured after — by the human)

- **P/A/T/I dedup loss across shards.** Streams and index files are
  shared across shards; per-shard syncs fdatasync the same file once per
  touching shard instead of once per request. At splits=8 that is ≤8× on
  a handful of files; the syncs of *different requests* still overlap,
  and within one request they are sequential per shard. Marker dir
  fsync becomes per-shard (8 instead of 1) — negligible. The old
  request-wide P pass was ~130–150 ms total at the measured shape.
- **Pipeline granularity.** One shard's pipeline can run for hundreds of
  ms on the caller thread; the caller thread is a per-request command
  thread (not a shared pool), so nothing else queues behind it except
  that same request's later shards.
- **Long holds per shard** are shorter than today's whole-request holds;
  reader impact improves.

### What stays request-wide vs per-shard

| today (request-wide) | B1 |
|---|---|
| gate set held request-wide (`slotcask_bulk_request_begin`) | per-shard gate around one pipeline (G1) |
| REQ_STAGE wave (`req_run_phase`) | stage call inside pipeline, per shard |
| `slotcask_bulk_request_flush_payloads` (merged dedup) | `slotcask_bulk_shard_flush_payloads` (per-shard dedup) |
| REQ_PUBLISH wave | publish loop inside pipeline, per shard |
| `slotcask_bulk_request_flush_marker_dir` (one fsync) | `slotcask_bulk_shard_flush_marker_dir` (per shard) |
| `SHARD_TEST_PHASE_REQ_PUBLISHED` / `"req-published"` | `SHARD_TEST_PHASE_SHARD_PUBLISHED` / `"shard-published-%03x"` (per shard) |
| REQ_FINALIZE wave | finalize loop inside pipeline, per shard |
| `slotcask_bulk_request_flush_commit` (merged I, `req_flush_kf`, merged A/T, batched clear) | `slotcask_bulk_shard_flush_commit` (per-shard I/K/A/T/clear) |
| `slotcask_bulk_request_end` (hooks+frees, then all gates) | `slotcask_bulk_shard_release` (hooks+frees under the gate) + container free |
| request-wide rc fold (`payload_rc`…`commit_rc`, `any_published`, `any_failed`) | per-shard fold into `ReqShard` + aggregate `any_pending`/`any_failed`/`saved_errno` |
| response assembly, `ReqShard`/`ReqWindow` structures, `res_map`, two-epoch admission concept, marker format, hook contract | unchanged (the two-epoch concept becomes "one epoch per shard, sequenced by the pipeline loop") |

## Failure semantics (per-shard outcome table)

Today's fold distinguishes pre-marker hard errors from published-but-
unresolved work (`pending` vs `failed`, slotcask.c
`slotcask_bulk_request_execute`: `int saved = pending ? EINPROGRESS :
(failed ? (errno ? errno : EIO) : 0);`). B1 keeps the distinction and
makes it per-shard. For touched shard s, with `in` = its
`SlotcaskBulkShardInput`:

| failure point (shard s) | markers on s | window hooks | in->rc | out_durability_degraded | request errno |
|---|---|---|---|---|---|
| stage: gate replay EILSEQ (corrupt/v1 marker) | none published (fail closed before staging) | none staged | -1 | 0 | EILSEQ preserved (not masked — today's rule kept) |
| stage: alloc/IO | none | none staged yet (window hooks are prepared during publish) | -1 | 0 | failing errno (hard) |
| P sync (`slotcask_bulk_shard_flush_payloads`) | none — publish skipped via `rs->payload_failed` | n/a | -1; **s's records status = -1** | 0 | failing errno (hard) |
| publish, window pre-M abort (`bulk_publish_one_kf_window` failure) | unpublished windows only; later windows still publish | `abort_window` per aborted window | -1 | 0 | failing errno (hard) |
| publish: one window fails before M after sibling windows linked successfully | failed window has no marker; published siblings continue | `abort_window` for the failed window; published siblings later reach `commit_done` if clear succeeds | -1 unless a separate published window remains unresolved | 0 unless a marker remains unresolved | hard failing errno unless a separate pending window makes EINPROGRESS win |
| M dir fsync (`marker_dir_failed`) | published, **not finalized** (finalize skipped) | `release_window` | -2 | 1 | EINPROGRESS |
| finalize (both idempotent attempts fail → `rs->failed`) | published, unconverged | `release_window` | -2 | 1 | EINPROGRESS |
| commit step (I / K / A / T sync) | converged but not cleared | `release_window` | -2 | 1 | EINPROGRESS |
| commit step (unlink failed, or dir fsync failed after unlink) | unlink-succeeded windows not dir-durable → retained | `release_window` | -2 | 1 | EINPROGRESS |
| **sibling shards** | — | **isolated: their pipelines completed independently; committed windows fired commit_done and the gate was released** | 0 | 0 | — |

Request-level return: `any_pending` → `-1, errno=EINPROGRESS` (pending
wins over hard errors, today's rule); else `any_failed` → `-1,
errno=saved_errno||EIO`; else 0. A request that had a hard failure on
shard 0 and completed shards 1..7 returns -1 with shards 1..7 committed
and released — same as today's sibling-window behavior, extended to
sibling shards.

Deliberate improvement over today (noted, not a regression): today a
request-wide P-flush failure marks **all** shards' records -1
(slotcask.c `slotcask_bulk_request_execute` payload_rc fold); B1 marks
only the failing shard's. Scenario 10 proves the new isolation.

## Test seams — full inventory and fate

| seam | site(s) | fate under B1 |
|---|---|---|
| P pause / note-sync | `bulk_stage_one_shard` (SHARD_TEST_PHASE_PAUSE(P), slotcask.c); note-sync in flush_payloads | pause unchanged (fires per shard inside pipeline stage). note-sync moves into `slotcask_bulk_shard_flush_payloads` — **occurrence semantics change** from one-per-request to one-per-shard. All existing injections use single-shard requests (verified below) → unaffected. |
| M pause / note-sync | `bulk_publish_one_kf_window` (pause per window, unchanged); note-sync in flush_marker_dir | same treatment as P. |
| A/I/K/T/C pauses | per-window helpers (`bulk_activate_new_payloads_locked`, `bulk_apply_and_sync_indexes_locked`, `bulk_apply_and_sync_kf_locked`, `bulk_tombstone_old_payloads_locked`) — unchanged call sites, fire per window inside pipeline finalize | unchanged. |
| A/I/T/C/K note-syncs | flush_commit / req_flush_kf | move into the per-shard commit helpers — occurrence semantics per shard (same verification as P/M). |
| `SHARD_TEST_PHASE_REQ_PUBLISHED` + `"req-published"` | slotcask.c (one site); shard_test_ctl.h; test-request-flush-batching (6 references) | **retired**. Replaced by in-process `SHARD_TEST_PHASE_SHARD_PUBLISHED` and cross-process `"shard-published-%03x"`, fired per shard between that shard's M dir fsync and finalize. Header and all consumers updated (Task 2); the shard-qualified string enables the new multi-shard crash test. |
| cross-process `durability_test_pause` strings `bulk-window-prepared` / `bulk-window-applied` / `bulk-window-cleared` | slotcask.c (per-window helpers; commit step) | strings and sites unchanged; `bulk-window-cleared` fires once per shard instead of once per request. Verified: all three cross-process consumers pin **single-shard batches** via `pick_same_shard_keys(8, 0, ...)` (test-durability-ordering.c: `test_durability_bulk_window_prepared_recovers` line ~905, `..._applied_recovers`, `test_durability_bulk_window_boundary` line ~1076), so the pause fires exactly once per request for them — observationally identical. |
| `g_shard_test_bulk_lookup_gap`, `g_shard_test_count_gap`, `g_shard_test_find_flush_gate` hooks | slotcask.c / query.c | untouched. |
| `shard_test_note_sync` concurrency comment ("concurrent Kf windows on different shards note-sync in parallel") | shard_test_ctl.h | still true across concurrent requests; no change. |
| **new:** `g_shard_test_gate_held_max` | shard_test_ctl.h + writer_gate_lock/unlock | TEST_BUILD running max of simultaneously held writer gates per thread — deterministic G1 enforcement (Task 1). |

## Call-site / consumer inventory

Everything the change touches, verified by search (no external consumers
exist; the daemon is a single static binary):

- `slotcask_bulk_request_execute` — public entry, signature **unchanged**.
  Declaration: src/db/slotcask.h:824. Callers: query_bulk.c
  `bulk_execute_prepared` (query_bulk.c:1074-1080) called from
  query_bulk.c:1784, 2542, 3082, 3848, 4389, 5470, 5900 (all six bulk
  command forms); tests: test_marker_v2.c:333,
  test_request_flush_batching.c:256, 601, 653. **Zero caller edits.**
- `slotcask_bulk_request_begin` / `slotcask_bulk_request_end` — file-
  static; single call sites at slotcask.c:7431 / 7484 (inside the
  replaced execute body). begin loses its gate loop; end shrinks to
  container frees.
- Deleted statics (single-consumer, all inside slotcask.c):
  `req_run_phase`, `req_phase_worker`, `ReqPhaseArg`,
  `REQ_STAGE/REQ_PUBLISH/REQ_FINALIZE` (only `req_run_phase`/execute),
  `slotcask_bulk_request_flush_payloads`, `slotcask_bulk_request_flush_marker_dir`,
  `req_flush_kf`, `req_kf_sync_worker`, `ReqKfSyncArg`,
  `slotcask_bulk_request_flush_commit`, `req_mark_durability_degraded`.
  Replacements carry the step bodies (§Tasks).
- Kept statics reused by per-shard helpers: `req_bitmap_file_path`,
  `fdatasync_path`, `req_window_abort_pre_marker`,
  `req_window_capture_hooks`, `req_window_append`, `bulk_window_plan_move`,
  `bulk_publish_one_kf_window`, `bulk_finalize_one_kf_window`,
  `slotcask_bulk_stage_shard`, `slotcask_bulk_publish_shard`,
  `slotcask_bulk_finalize_shard` (last two: one-line gating change each),
  `bulk_seg_apply_and_sync`, `index_sync_path_set`, `radix_sort_sizes`,
  `size_cmp`, `idx_touch_cmp`, `segloc_cmp`, `segloc_is_sorted`,
  `split_data_dir`, `idx_shard_for_hash`, `tg_build_path`, `build_idx_path`,
  `kfm2_unlink_by_path`, `fsync_dir`, `bulk_reclaim_old_payloads_locked`,
  `kfcache_sync_slots_locked`, `kf_shard_acquire/release`,
  `size_add_checked`, `size_vec_append` (still used by
  `bulk_apply_and_sync_kf_locked`).
- `writer_gate_lock` / `writer_gate_unlock` — file-static; call sites:
  slotcask.c:2171/2176/2190 (pregrow worker), 6762/6802 (request
  begin/end — removed by this change), 7516/7524
  (`slotcask_bulk_mutation_transaction`). Task 1 adds TEST_BUILD
  instrumentation inside the two functions; all callers unaffected.
- `SlotcaskBulkRequest`, `ReqShard`, `ReqWindow` — file-static structs
  (slotcask.c). Field changes in §Tasks 3–4; no external consumers.
- `SHARD_TEST_PHASE_REQ_PUBLISHED` — consumers: shard_test_ctl.h:28-30
  (definition), slotcask.c:7456-7457 (fire site),
  test_request_flush_batching.c lines 5, 395, 633, 663, 717, 762.
- Documentation and live-comment consumers: docs/concepts/concurrency.md
  bulk section plus durability invariant I4; AGENTS.md "Indexed-write
  crash safety"; docs/reference/changelog.md Unreleased;
  docs/getting-started/configuration.md's `BULK_COMMIT_WINDOW` row; the
  `SlotcaskDb.writer_gates` and deferred-request comments in slotcask.h;
  and slotcask.c's writer-gate, deferred-state, deferred-index-flush, and
  folded-pre-grow and fallback-allocation comments. All are updated in
  Task 5.
- Metrics: `g_bulk_stage_us_total` written only at slotcask.c:7437 —
  the timing moves inside the pipeline around the stage call so the
  metric keeps meaning "bulk stage time".
- `slotcask.h` public comment block "Deferred bulk request" (describes
  gate-set semantics) — updated in Task 4.

## Tasks

Execution rules binding every task are in §Execution rules at the end —
read them first. Tasks are ordered; do not reorder. Tasks 1–3 each
leave **known-red new tests** in the tree with the expected failures
pasted in the task log (scenario 11 goes green within Task 3; at the
end of Task 3 the known-red set is scenarios 8, 9, 10 and the
multi-shard crash case); the suite is fully green again at Task 4
(plus the previously-green tests stay green at every task). This is
the TDD loop required by CORE-PROCESS, not a broken intermediate
state. If any *previously-green* test goes red at any task, stop and
report.

### Task 1 — G1 instrumentation + scenario 8 (gate-hold assertion)

**Test-first.** Add the seam (code below), then add scenario 8 to
test_request_flush_batching.c, build, and run the case: scenario 8
asserts `g_shard_test_gate_held_max <= 1` and on the wave coordinator it
observes 8 (begin holds all touched gates) — **red for the expected
reason**. Paste the failure. It stays red until Task 4.

**Edit 1.1** — src/db/shard_test_ctl.h, declare the counter. Anchor:
the block starting `/* Count-worker pass-1 gap hook (docs/plans/2026-08-27-shard-count-worker-`
— insert immediately before it:

```c
/* B1 G1 instrumentation: running max, per thread, of simultaneously
 * held writer gates. writer_gate_lock/unlock (slotcask.c) maintain a
 * TLS count under TEST_BUILD and fold it into this atomic; a thread
 * respecting G1 never drives it above 1. */
extern _Atomic long g_shard_test_gate_held_max;
```

**Edit 1.2** — src/db/shard_test_ctl.h, reset it. Anchor:

```c
    atomic_store(&g_shard_test_count_gap_hit, 0);
}
```

replace with:

```c
    atomic_store(&g_shard_test_count_gap_hit, 0);
    atomic_store(&g_shard_test_gate_held_max, 0);
}
```

**Edit 1.3** — src/db/slotcask.c, define it in the TEST_BUILD globals
block. Anchor (line ~50, under the existing `#ifdef TEST_BUILD` guard
with shard_test_ctl.h's include at ~49):

```c
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
```

insert immediately after:

```c
_Atomic long g_shard_test_gate_held_max;
```

**Edit 1.4** — src/db/slotcask.c, instrument the gate pair. Anchor:

```c
static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
}
static void writer_gate_unlock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_unlock(&db->writer_gates[kf_shard]);
}
```

replace with:

```c
#ifdef TEST_BUILD
/* G1 instrumentation: TLS held-gate count folded into shard_test_ctl's
 * running max. Production builds keep the plain lock (block compiled
 * out; no atomics on the hot path). */
static __thread int t_writer_gates_held;
#endif

static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
#ifdef TEST_BUILD
    t_writer_gates_held++;
    long cur = atomic_load(&g_shard_test_gate_held_max);
    while (t_writer_gates_held > cur &&
           !atomic_compare_exchange_weak(&g_shard_test_gate_held_max, &cur,
                                         (long)t_writer_gates_held)) {}
#endif
}

static void writer_gate_unlock(SlotcaskDb *db, int kf_shard) {
#ifdef TEST_BUILD
    t_writer_gates_held--;
#endif
    pthread_mutex_unlock(&db->writer_gates[kf_shard]);
}
```

**Edit 1.5** — test file, new scenario 8. Anchor (end of the test
function, after scenario 7's block):

```c
        parallel_pool_shutdown();
    }

    rf_db_close(&w);
```

replace with:

```c
        parallel_pool_shutdown();
    }

    /* ── Scenario 8: G1 — a request never holds two writer gates at
     * once. Red until B1's pipeline loop (Task 4): the wave coordinator
     * holds every touched gate simultaneously, so the counter observes
     * RF_SPLITS on the multi-shard batch. ── */
    {
        static RfBatch b;
        static char keys[64][24], vals[64][24];
        static SlotcaskBulkRec recs[64];
        static SlotcaskBulkRec perm20[64];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm20;
        rf_batch_fill(&b, 20, 64, "v20", -1);   /* 8 records × 8 shards */
        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);
        shard_test_ctl_reset();
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), 0,
                      "8-shard request for G1 measurement");
        ASSERT_TRUE(rf_record_visible(&w, b.keys[63], b.vals[63]),
                    "G1 request committed");
        ASSERT_TRUE(atomic_load(&g_shard_test_gate_held_max) <= 1,
                    "G1: at most one writer gate held per thread "
                    "(wave coordinator fails this: holds all touched)");
    }

    rf_db_close(&w);
```

**Verify:**

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
```

Paste scenario 8's red output (expected: `G1: at most one writer gate
held per thread` fails with the counter at 8). The registered case and
therefore `run-all` must exit nonzero while this known-red assertion is
present. Run the full suite and paste its complete nonzero result; require
that scenario 8 is the only new failure and every pre-existing case stays
green:

```bash
rtk proxy ./build/bin/shard-db-test run-all
```

### Task 2 — Retire REQ_PUBLISHED, add the per-shard seam, scenario 9 (the pipelining property)

**Edit 2.1** — src/db/shard_test_ctl.h. Anchor:

```c
    SHARD_TEST_PHASE_C,       /* marker clear barrier */
    SHARD_TEST_PHASE_REQ_PUBLISHED, /* request-level: every window's marker
                                       published + kf dir fsync done, before
                                       the finalize wave */
    SHARD_TEST_PHASE_COUNT
```

replace with:

```c
    SHARD_TEST_PHASE_C,       /* marker clear barrier */
    SHARD_TEST_PHASE_SHARD_PUBLISHED, /* per shard: this shard's windows'
                                       markers published + kf dir fsync
                                       done, before this shard's finalize
                                       (B1 pipeline; supersedes the
                                       request-wide REQ_PUBLISHED seam) */
    SHARD_TEST_PHASE_COUNT
```

**Edit 2.2** — src/db/slotcask.c, rename the fire site (it stays
request-wide until Task 4 moves it into the pipeline). Anchor:

```c
    if (req->any_published) {
        SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_REQ_PUBLISHED);
        durability_test_pause(req->db->data_dir, "req-published");
    }
```

replace with:

```c
    if (req->any_published) {
        SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_SHARD_PUBLISHED);
        durability_test_pause(req->db->data_dir, "shard-published");
    }
```

**Edit 2.3** — test file: make the seam rename using only the exact
quoted anchors below.

```c
 * SHARD_TEST_PHASE_REQ_PUBLISHED / the state-bearing hook signatures do
```

replace with:

```c
 * SHARD_TEST_PHASE_SHARD_PUBLISHED / the state-bearing hook signatures do
```

```c
 *   1. multi-window retention via the req-published pause (marker files
```

replace with:

```c
 *   1. multi-window retention via the shard-published pause (marker files
```

```c
                                 parked at the req-published pause */
```

replace with:

```c
                                 parked at the shard-published pause */
```

```c
    /* ── Scenario 1: multi-window retention via the req-published pause ── */
```

replace with:

```c
    /* ── Scenario 1: multi-window retention via the shard-published pause ── */
```

Replace each of the five phase assignments through the following distinct,
context-bearing hunks. They are deliberately listed separately: the fifth
site has no adjacent `shard_test_ctl_reset()`, and the admission-table site
has different indentation, so a repeated three-line substitution is not a
complete execution instruction.

Scenario 1 anchor:

```c
        uint64_t w0 = __atomic_load_n(&g_db->commit_windows_total,
                                      __ATOMIC_RELAXED);
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

replace with:

```c
        uint64_t w0 = __atomic_load_n(&g_db->commit_windows_total,
                                      __ATOMIC_RELAXED);
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

Scenario 6a anchor:

```c
        /* 6a: request B on disjoint shard 1 completes while A is paused. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

replace with:

```c
        /* 6a: request B on disjoint shard 1 completes while A is paused. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

Scenario 6b anchor:

```c
        /* 6b: a request sharing shard 0 serializes on the gate. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

replace with:

```c
        /* 6b: a request sharing shard 0 serializes on the gate. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

Admission-table anchor:

```c
            }
            shard_test_ctl_reset();
            g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
            g_shard_test_pause_occurrence = 1;
            RfReqThr ta3 = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
```

replace with:

```c
            }
            shard_test_ctl_reset();
            g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
            g_shard_test_pause_occurrence = 1;
            RfReqThr ta3 = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
```

Reader-visibility anchor:

```c
        bd.recs[0].key = bd.keys[0]; bd.recs[0].klen = strlen(bd.keys[0]);
        bd.recs[0].value = bd.vals[0]; bd.recs[0].vlen = strlen(bd.vals[0]);
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

replace with:

```c
        bd.recs[0].key = bd.keys[0]; bd.recs[0].klen = strlen(bd.keys[0]);
        bd.recs[0].value = bd.vals[0]; bd.recs[0].vlen = strlen(bd.vals[0]);
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
```

Finally replace:

```c
                    "req-published pause hit");
```

with:

```c
                    "shard-published pause hit");
```

**Edit 2.4** — test file, new scenario 9 (insert immediately after
scenario 8's block, before `rf_db_close(&w);`):

```c
    /* ── Scenario 9: pipelining — B completes a disjoint shard while A
     * is paused mid-pipeline on its first shard. The property B1 exists
     * to deliver. Red until Task 4: the wave coordinator holds every
     * touched gate at the pause, so B blocks in begin. ── */
    {
        static RfBatch ba, bb;
        static char keysa[64][24], valsa[64][24];
        static SlotcaskBulkRec recsa[64];
        static SlotcaskBulkRec perma[64];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        rf_batch_fill(&ba, 21, 64, "v21", -1);   /* all 8 shards */
        static char keysb[16][24], valsb[16][24];
        static SlotcaskBulkRec recsb[16];
        static SlotcaskBulkRec permb[16];
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&bb, 22, 16, "v22", 1);    /* shard 1 only */

        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;

        RfReqThr ta = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
        pthread_t tha;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &ta), 0,
                      "spawn A (shards 0-7)");
        rf_wait_pause_hit();
        ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                    "A parked after its first shard published");

        RfReqThr tb = { .w = &w, .b = &bb, .done = 0, .rc = 0 };
        pthread_t thb;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_req_thread, &tb), 0,
                      "spawn B (shard 1)");
        ASSERT_TRUE(rf_wait_flag(&tb.done, 5000),
                    "B completes shard 1 while A is paused mid-pipeline");
        ASSERT_EQ_INT(tb.rc, 0, "B rc 0");
        ASSERT_TRUE(rf_record_visible(&w, bb.keys[0], "v22-0000"),
                    "B's record readable while A paused");

        rf_release_pause();
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        ASSERT_EQ_INT(ta.rc, 0, "A converges after release");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "all markers cleared");
        ASSERT_TRUE(rf_record_visible(&w, ba.keys[0], "v21-0000"),
                    "A's shard-0 record visible after join");
    }
```

**Verify (red expected):**

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
```

Paste scenario 9's red output (expected: `B completes shard 1 while A is
paused mid-pipeline` fails — rf_wait_flag times out at 5 s because B
is parked in `slotcask_bulk_request_begin` on shard 1's gate). Confirm
every assertion except the known-red scenarios 8 and 9 is green. The full
suite below is expected to exit nonzero for those assertions; paste its
complete output and stop if any previously-green case fails:

```bash
rtk proxy ./build/bin/shard-db-test run-all
```

**Edit 2.5 — test-first multi-shard crash state.** Add the complete test
below to `src/test/cases/test_durability_ordering.c` immediately before the
anchor comment `/* 5) Window-boundary: route 257 indexed records (single shard, so they span`
(so the comment stays attached to `test_durability_bulk_window_boundary`
directly below it). It arms
the future shard-qualified pause for shard 1. On the wave tree and after
Task 2 it is red because `"shard-published-001"` is not emitted; Task 4's
pipeline fire site turns it green.

First update the helper contract that this test broadens. Anchor:

```c
/* Forks a child that issues one bulk-insert of `count` records against
   `object`, using the given pre-picked keys (all routed to the same kf
   shard by the caller via pick_same_shard_keys), each with score = its
   index in `keys` so range/count assertions have a stable handle. Returns
   the child pid, or -1 on fork failure. */
```

replace with:

```c
/* Forks a child that issues one bulk-insert of `count` records against
   `object`, using the caller's pre-picked keys. Keys may span shards;
   existing single-shard callers still use pick_same_shard_keys. Each
   record's score is its index in `keys`, giving range/count assertions a
   stable handle. Returns the child pid, or -1 on fork failure. */
```

```c
/* B1 crash regression: shard 0 has committed and cleared, shard 1 has a
   directory-durable marker but has not finalized, and shard 2 has not
   started. After SIGKILL, startup must retain shard 0, replay shard 1, and
   leave shard 2 absent. This three-category state cannot occur under the
   request-wide wave coordinator. */
static int test_durability_bulk_pipeline_multishard_crash(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    const char *object = "durabpipe3";
    ASSERT_EQ_INT(create_indexed_object_default_splits(&env, object), 0,
                  "create multi-shard pipeline crash fixture");

    char keys[6][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_same_shard_keys(8, 0, &next_candidate, keys, 2), 0,
                  "pick two shard-0 keys");
    ASSERT_EQ_INT(pick_same_shard_keys(8, 1, &next_candidate, keys + 2, 2), 0,
                  "pick two shard-1 keys");
    ASSERT_EQ_INT(pick_same_shard_keys(8, 2, &next_candidate, keys + 4, 2), 0,
                  "pick two shard-2 keys");
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(append_durability_pause_config(
                      saved_db_root, "shard-published-001"),
                  0, "pause shard 1 after marker-dir durability");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with shard-1 pipeline pause");
    if (env.daemon_pid <= 0) return 1;

    pid_t bulk_pid = trigger_bulk_insert(&env, object, keys, 6);
    ASSERT_TRUE(bulk_pid > 0, "spawn three-shard bulk request");

    char pause_marker[PATH_MAX];
    snprintf(pause_marker, sizeof(pause_marker),
             "%s/default/%s/.durability-test-shard-published-001.active",
             saved_db_root, object);
    int pause_rc = wait_for_path(pause_marker, 20000);
    ASSERT_EQ_INT(pause_rc, 0, "request pauses on shard 1 before finalize");
    if (pause_rc != 0) {
        test_env_kill(&env);
        if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);
        return t_ctx->failed > 0 ? 1 : 0;
    }
    ASSERT_EQ_INT(request_count(&env, object), 2,
                  "only cleared shard 0 is visible before crash");
    char mpaths_before[8][PATH_MAX];
    ASSERT_EQ_INT(scan_kf_markers(saved_db_root, object, mpaths_before, 8), 1,
                  "shard 1 has exactly one durable marker");

    test_env_kill(&env);
    unlink(pause_marker);
    if (bulk_pid > 0) waitpid(bulk_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart recovers the interrupted shard pipeline");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_marker_recovery_ran(&env), 1,
                      "startup replay ran for shard 1");
        ASSERT_EQ_INT(request_count(&env, object), 4,
                      "shards 0 and 1 present; untouched shard 2 absent");
        char mpaths[8][PATH_MAX];
        ASSERT_EQ_INT(scan_kf_markers(saved_db_root, object, mpaths, 8), 0,
                      "replayed shard-1 marker cleared");

        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect after multi-shard recovery");
        if (tc) {
            char req[512], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                "\"criteria\":[{\"field\":\"score\",\"op\":\"lte\",\"value\":\"3\"}]}",
                object);
            ASSERT_EQ_INT(tc_request(tc, req, &resp), 0,
                          "query recovered shard-0/1 index entries");
            ASSERT_EQ_INT(tu_parse_count(resp), 4,
                          "all committed/replayed rows are indexed");
            free(resp); resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"4\"}]}",
                object);
            ASSERT_EQ_INT(tc_request(tc, req, &resp), 0,
                          "query untouched shard-2 score range");
            ASSERT_EQ_INT(tu_parse_count(resp), 0,
                          "later shard remained untouched across crash");
            free(resp);
            tc_close(tc);
        }
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Update the registration block using this complete anchor:

```c
TEST_REGISTER("test-durability-bulk-window-prepared-recovers", test_durability_bulk_window_prepared_recovers)
TEST_REGISTER("test-durability-bulk-window-applied-recovers", test_durability_bulk_window_applied_recovers)
TEST_REGISTER("test-durability-bulk-window-boundary", test_durability_bulk_window_boundary)
TEST_REGISTER("test-durability-bulk-window-boundary-mixed-indexes", test_durability_bulk_window_boundary_mixed_indexes)
```

replace with:

```c
TEST_REGISTER("test-durability-bulk-window-prepared-recovers", test_durability_bulk_window_prepared_recovers)
TEST_REGISTER("test-durability-bulk-window-applied-recovers", test_durability_bulk_window_applied_recovers)
TEST_REGISTER("test-durability-bulk-pipeline-multishard-crash", test_durability_bulk_pipeline_multishard_crash)
TEST_REGISTER("test-durability-bulk-window-boundary", test_durability_bulk_window_boundary)
TEST_REGISTER("test-durability-bulk-window-boundary-mixed-indexes", test_durability_bulk_window_boundary_mixed_indexes)
```

Build and paste the expected red separately; the failure must be the
missing shard-qualified pause, not a compile or fixture failure:

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-pipeline-multishard-crash
```

### Task 3 — Per-shard flush helpers + per-shard gating fields (wave driver still drives)

Pure refactor step: the step bodies move from request-wide passes into
per-shard functions; the wave driver calls them per touched shard in
ascending order. Everything is still under the full gate set, so the
concurrency behavior is unchanged and the pre-existing suite stays
green. Scenarios 10 and 11 are first added and proved **red** on the
unmodified wave implementation. The per-shard commit helper makes scenario
11 green within this task; scenario 10 remains red for the narrower
request-wide outcome-fold reason until Task 4.

**Edit 3.2** — slotcask.c, `ReqShard` fields. Anchor:

```c
    SegLoc    *p_locs; size_t np, cap_p;      /* payload bytes (epoch 1)   */
    ReqWindow *windows; size_t nwindows, cap_windows;
    ReqSlotRes *res_map; size_t res_cap;     /* cross-window planned slots */
    int        failed;                 /* EINPROGRESS / unreplayed         */
} ReqShard;
```

replace with:

```c
    SegLoc    *p_locs; size_t np, cap_p;      /* payload bytes (epoch 1)   */
    ReqWindow *windows; size_t nwindows, cap_windows;
    ReqSlotRes *res_map; size_t res_cap;     /* cross-window planned slots */
    int        failed;                 /* EINPROGRESS / unreplayed         */
    /* B1 per-shard pipeline outcome (folded before this shard's gate
       release; supersedes the request-wide payload_rc / publish_rc /
       marker_dir_rc / finalize_rc / commit_rc fold). */
    int        payload_failed;         /* this shard's P barrier failed    */
    int        marker_dir_failed;      /* post-publish kf-dir fsync failed */
    int        step_failed;            /* any pipeline step errored (hard) */
    int        step_errno;             /* first failing step's errno       */
    int        retained;               /* a window published && !cleared   */
} ReqShard;
```

**Edit 3.2a** — slotcask.c, make a stage failure return a deterministic
errno instead of whatever ambient value survives the inline stage leaf.
Anchor (the tail of `slotcask_bulk_stage_shard`):

```c
    BulkStageWork work = { .txn = &rs->txn, .shard_idx = 0 };
    bulk_stage_one_shard(&work);
    int rc = atomic_load_explicit(&rs->txn.cancelled,
                                  memory_order_acquire) || rs->shard.rc != 0
           ? -1 : 0;
    if (rc != 0) {
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++)
            if (recs[i].status == 0) recs[i].status = -1;
    }
    return rc;
}
```

replace with:

```c
    BulkStageWork work = { .txn = &rs->txn, .shard_idx = 0 };
    errno = 0;
    bulk_stage_one_shard(&work);
    int rc = atomic_load_explicit(&rs->txn.cancelled,
                                  memory_order_acquire) || rs->shard.rc != 0
           ? -1 : 0;
    if (rc != 0) {
        int saved_errno = errno ? errno : EIO;
        rs->stage_failed = 1;
        for (size_t i = 0; i < n; i++)
            if (recs[i].status == 0) recs[i].status = -1;
        errno = saved_errno;
    }
    return rc;
}
```

**Edit 3.3** — slotcask.c, replace the complete publish helper below.
Besides changing P gating to `rs->payload_failed`, the replacement captures
the first failing window's errno before later windows continue, so the
per-shard outcome fold never reads clobbered ambient errno. Anchor:

```c
static int slotcask_bulk_publish_shard(SlotcaskBulkRequest *req,
                                       int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || rs->stage_failed || req->payload_rc != 0) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0;
    while (rs->shard.cursor < rs->shard.nrecs) {
        size_t begin = rs->shard.cursor;
        size_t end = begin + rs->txn.window_cap;
        if (end > rs->shard.nrecs) end = rs->shard.nrecs;
        ReqWindow *rw = req_window_append(rs);
        if (!rw) { rc = -1; break; }
        if (bulk_publish_one_kf_window(&rs->txn, &rs->shard, &kh,
                                       begin, end, rw) != 0)
            rc = -1; /* this window aborted pre-M; later windows may proceed */
        rs->shard.cursor = end;
    }
    kfcache_release(&kh);
    return rc;
}
```

replace with:

```c
static int slotcask_bulk_publish_shard(SlotcaskBulkRequest *req,
                                       int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || rs->stage_failed || rs->payload_failed) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0, saved_errno = 0;
    while (rs->shard.cursor < rs->shard.nrecs) {
        size_t begin = rs->shard.cursor;
        size_t end = begin + rs->txn.window_cap;
        if (end > rs->shard.nrecs) end = rs->shard.nrecs;
        ReqWindow *rw = req_window_append(rs);
        if (!rw) {
            rc = -1;
            saved_errno = errno ? errno : ENOMEM;
            break;
        }
        if (bulk_publish_one_kf_window(&rs->txn, &rs->shard, &kh,
                                       begin, end, rw) != 0) {
            rc = -1; /* this window aborted pre-M; later windows may proceed */
            if (!saved_errno) saved_errno = errno ? errno : EIO;
        }
        rs->shard.cursor = end;
    }
    kfcache_release(&kh);
    if (rc != 0) errno = saved_errno ? saved_errno : EIO;
    return rc;
}
```

**Edit 3.4** — slotcask.c, replace the complete finalize helper. Besides
making the marker-dir gate per shard, preserve the first terminal retry's
errno across later windows and `kfcache_release`. Anchor:

```c
static int slotcask_bulk_finalize_shard(SlotcaskBulkRequest *req,
                                        int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || req->marker_dir_rc != 0) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->published) continue;
        if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                        &kh, rw) != 0) {
            /* Forward retry remains deferred. It may make mutations
               idempotently, but it never clears before the common barrier. */
            if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                            &kh, rw) != 0) {
                rs->failed = 1;
                rc = -1;
            }
        }
    }
    kfcache_release(&kh);
    return rc;
}
```

replace with:

```c
static int slotcask_bulk_finalize_shard(SlotcaskBulkRequest *req,
                                        int kf_shard_id) {
    if (!req || kf_shard_id < 0 || kf_shard_id >= req->num_shards ||
        !request_owns_shard(req, kf_shard_id)) {
        errno = EINVAL;
        return -1;
    }
    ReqShard *rs = &req->shards[kf_shard_id];
    if (!rs->has_txn || rs->marker_dir_failed) return 0;
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) return -1;
    int rc = 0, saved_errno = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->published) continue;
        if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                        &kh, rw) != 0) {
            /* Forward retry remains deferred. It may make mutations
               idempotently, but it never clears before the common barrier. */
            if (bulk_finalize_one_kf_window(&rs->txn, &rs->shard,
                                            &kh, rw) != 0) {
                rs->failed = 1;
                rc = -1;
                if (!saved_errno) saved_errno = errno ? errno : EIO;
            }
        }
    }
    kfcache_release(&kh);
    if (rc != 0) errno = saved_errno ? saved_errno : EIO;
    return rc;
}
```

**Edit 3.5** — slotcask.c, replace the request-wide P and M flushes.
Anchor (the full function pair, from the `/* ── Request flushes ── */`
banner through the end of `slotcask_bulk_request_flush_marker_dir`):

```c
/* ── Request flushes ── */

static int slotcask_bulk_request_flush_payloads(SlotcaskBulkRequest *req) {
    /* Merge every shard's payload locations request-wide, dedupe, and
       flush in ONE pass — shared stream files are written once. */
    size_t total = 0;
    for (int s = 0; s < req->num_shards; s++)
        if (size_add_checked(&total, req->shards[s].np) != 0) return -1;
    if (total == 0) return 0;
    if (total > SIZE_MAX / sizeof(SegLoc)) { errno = EOVERFLOW; return -1; }
    SegLoc *all = malloc(total * sizeof(*all));
    if (!all) return -1;
    size_t n = 0;
    for (int s = 0; s < req->num_shards; s++) {
        if (req->shards[s].np)
            memcpy(all + n, req->shards[s].p_locs,
                   req->shards[s].np * sizeof(*all));
        n += req->shards[s].np;
    }
    if (n > 1 && !segloc_is_sorted(all, n))
        qsort(all, n, sizeof(*all), segloc_cmp);
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0) all[w++] = all[i];
    uint64_t t0 = now_us();
    int rc = bulk_seg_apply_and_sync(req->db, all, w, 0, 0);
    commit_phase_us_record(&g_commit_segment_sync_us_total, t0);
    commit_phase_us_record(&g_commit_segment_p_us_total, t0);
    free(all);
    for (int s = 0; s < req->num_shards; s++) req->shards[s].np = 0;
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) rc = -1;
    return rc;
}

static int slotcask_bulk_request_flush_marker_dir(SlotcaskBulkRequest *req) {
    if (!req->any_published) return 0;
    int rc = fsync_dir(req->kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) rc = -1;
    return rc;
}
```

replace with:

```c
/* ── Per-shard pipeline flushes (B1) ─────────────────────────────────
   Each helper runs inside one shard's pipeline, under that shard's
   writer gate, on the request's caller thread. They replace the
   request-wide passes: dedup is per shard, so a stream or index file
   shared by several shards may be synced once per touching shard
   instead of once per request — the accepted B1 cost. */

/* P barrier for shard kf_shard_id: sync this shard's staged payload
   bytes once. On failure the caller marks this shard's records -1
   (only this shard — sibling shards are unaffected). */
static int slotcask_bulk_shard_flush_payloads(SlotcaskBulkRequest *req,
                                              int kf_shard_id) {
    ReqShard *rs = &req->shards[kf_shard_id];
    if (rs->np == 0) return 0;
    SegLoc *all = malloc(rs->np * sizeof(*all));
    if (!all) return -1;
    memcpy(all, rs->p_locs, rs->np * sizeof(*all));
    size_t n = rs->np;
    if (n > 1 && !segloc_is_sorted(all, n))
        qsort(all, n, sizeof(*all), segloc_cmp);
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0) all[w++] = all[i];
    uint64_t t0 = now_us();
    int rc = bulk_seg_apply_and_sync(req->db, all, w, 0, 0);
    int saved_errno = rc != 0 ? (errno ? errno : EIO) : 0;
    commit_phase_us_record(&g_commit_segment_sync_us_total, t0);
    commit_phase_us_record(&g_commit_segment_p_us_total, t0);
    free(all);
    rs->np = 0;
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) {
        errno = EIO;
        rc = -1;
        saved_errno = EIO;
    }
    if (rc != 0) errno = saved_errno ? saved_errno : EIO;
    return rc;
}

/* M barrier for shard kf_shard_id: ONE fsync(data/kf dir) after this
   shard's windows' markers are link-published. 0 when nothing
   published. */
static int slotcask_bulk_shard_flush_marker_dir(SlotcaskBulkRequest *req,
                                                int kf_shard_id) {
    ReqShard *rs = &req->shards[kf_shard_id];
    int any = 0;
    for (size_t i = 0; i < rs->nwindows; i++)
        any |= rs->windows[i].published;
    if (!any) return 0;
    int rc = fsync_dir(req->kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) {
        errno = EIO;
        rc = -1;
    }
    return rc;
}
```

**Edit 3.6** — slotcask.c, replace the request-wide K barrier. Anchor
(the full block from `typedef struct {` / `ReqKfSyncArg` through the end
of `req_flush_kf`):

```c
typedef struct {
    SlotcaskBulkRequest *req;
    int shard_id;
    int rc;
    int err;
} ReqKfSyncArg;
```

…through…

```c
static int req_flush_kf(SlotcaskBulkRequest *req) {
    ReqKfSyncArg *args = calloc(req->ntouched, sizeof(*args));
    if (!args) return -1;
    for (size_t i = 0; i < req->ntouched; i++) {
        args[i].req = req;
        args[i].shard_id = req->touched[i];
    }
    parallel_for_io(req_kf_sync_worker, args, (int)req->ntouched,
                    sizeof(*args));
    int rc = 0, saved = 0;
    for (size_t i = 0; i < req->ntouched; i++) {
        if (args[i].rc == 0) continue;
        rc = -1;
        if (!saved) saved = args[i].err;
    }
    free(args);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) rc = -1;
    if (rc != 0) errno = saved ? saved : EIO;
    return rc;
}
```

(Delete `ReqKfSyncArg`, `req_kf_sync_worker`, and `req_flush_kf`
entirely — the whole region between the `ReqKfSyncArg` typedef and the
end of `req_flush_kf` — and replace with:)

```c
/* K barrier for shard kf_shard_id: one mmap durability wait for the
   shard's merged kf slot vector. Body of the former req_kf_sync_worker,
   invoked inline by the pipeline instead of via parallel_for_io. */
static int slotcask_bulk_shard_flush_kf(SlotcaskBulkRequest *req,
                                        int kf_shard_id) {
    ReqShard *rs = &req->shards[kf_shard_id];
    size_t total = 0;
    int header_changed = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->converged) continue;
        if (size_add_checked(&total, rw->nkf) != 0) return -1;
        header_changed |= rw->kf_header_changed;
    }
    if (total == 0 && !header_changed) return 0;
    if (total > SIZE_MAX / sizeof(size_t)) { errno = EOVERFLOW; return -1; }
    size_t *slots = total ? malloc(total * sizeof(*slots)) : NULL;
    if (total && !slots) { errno = ENOMEM; return -1; }
    size_t n = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->converged) continue;
        if (rw->nkf > 0) {
            memcpy(slots + n, rw->kf_slots, rw->nkf * sizeof(*slots));
            n += rw->nkf;
        }
    }
    if (n > 1) {
        size_t *scratch = malloc(n * sizeof(*scratch));
        if (scratch) {
            radix_sort_sizes(slots, scratch, n);
            free(scratch);
        } else {
            qsort(slots, n, sizeof(*slots), size_cmp);
        }
    }
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || slots[w - 1] != slots[i]) slots[w++] = slots[i];
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, req->db, kf_shard_id, 1) != 0) {
        free(slots);
        return -1;
    }
    int rc = kfcache_sync_slots_locked(&kh, slots, w, header_changed);
    kfcache_release(&kh);
    free(slots);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) {
        errno = EIO;
        rc = -1;
    }
    return rc;
}
```

**Edit 3.7** — slotcask.c, replace `slotcask_bulk_request_flush_commit`
with the per-shard commit step. Anchor: the entire function from

```c
static int slotcask_bulk_request_flush_commit(SlotcaskBulkRequest *req) {
```

through its final

```c
    if (unlink_rc != 0) errno = unlink_errno ? unlink_errno : EIO;
    return unlink_rc;
}
```

Delete it and insert in its place:

```c
/* Per-shard commit barriers + clear for shard kf_shard_id (the
   pipeline's last durability step, before the shard's terminal cleanup
   and gate release): merged index flush [I1] → K sync → A/T segment
   syncs → batched marker unlink + ONE fsync(data/kf dir) → reclaim +
   commit_done. Per-shard predicates make ineligible windows no-ops, so
   the helper is safe to run after any earlier pipeline failure. */
static int slotcask_bulk_shard_flush_commit(SlotcaskBulkRequest *req,
                                            int kf_shard_id) {
    ReqShard *rs = &req->shards[kf_shard_id];

    /* 1. Index flush: merge this shard's converged windows' touch sets,
          dedupe, issue through the parallel issuer; bitmaps serial. */
    size_t total = 0;
    for (size_t i = 0; i < rs->nwindows; i++)
        if (rs->windows[i].converged)
            if (size_add_checked(&total, rs->windows[i].plan.touch.n) != 0)
                return -1;
    if (total > 0) {
        uint64_t t0i = now_us();
        if (total > SIZE_MAX / sizeof(IdxTouch)) {
            errno = EOVERFLOW;
            return -1;
        }
        IdxTouch *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        char eff_root[PATH_MAX], object[256];
        split_data_dir(req->db->data_dir, eff_root, sizeof(eff_root),
                       object, sizeof(object));
        for (size_t i = 0; i < rs->nwindows; i++) {
            ReqWindow *rw = &rs->windows[i];
            if (!rw->converged) continue;
            if (rw->plan.touch.n)
                memcpy(all + n, rw->plan.touch.v,
                       rw->plan.touch.n * sizeof(*all));
            n += rw->plan.touch.n;
        }
        qsort(all, n, sizeof(*all), idx_touch_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || idx_touch_cmp(&all[w - 1], &all[i]) != 0)
                all[w++] = all[i];
        n = w;   /* deduplicated count drives everything below */
        size_t npaths = 0;
        for (size_t i = 0; i < n; i++)
            if (all[i].type != IT_BITMAP) npaths++;
        if (npaths > 0) {
            char *path_buf = malloc(npaths * PATH_MAX);
            const char **paths = malloc(npaths * sizeof(*paths));
            if (!path_buf || !paths) {
                free(all); free(path_buf); free(paths);
                return -1;
            }
            size_t w2 = 0;
            for (size_t i = 0; i < n; i++) {
                if (all[i].type == IT_BITMAP) continue;
                int shard = idx_shard_for_hash(all[i].hash16,
                                               req->num_shards);
                if (all[i].type == IT_TRIGRAM)
                    tg_build_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                  eff_root, object, all[i].field, shard);
                else
                    build_idx_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                   eff_root, object, all[i].field, shard);
                paths[w2] = path_buf + w2 * PATH_MAX;
                w2++;
            }
            int frc = index_sync_path_set(paths, npaths);
            free(path_buf); free(paths);
            if (frc != 0) { free(all); return -1; }
            __atomic_add_fetch(&g_commit_index_sync_ops_total,
                               (uint64_t)npaths, __ATOMIC_RELAXED);
        }
        for (size_t i = 0; i < n; i++) {
            if (all[i].type != IT_BITMAP) continue;
            /* Plain-fd fdatasync: the dirty bitmap pages belong to the
               inode (written via the bm cache's mmap), so syncing any fd
               of the file is equivalent — without re-entering the bm
               cache, whose entry wrlocks are parked by the request's own
               prepare-staged handles until finalize. A missing file means
               nothing was ever written — not an error. */
            char bpath[1024];
            req_bitmap_file_path(bpath, sizeof(bpath), eff_root, object,
                                 all[i].field, all[i].idx_shard);
            if (fdatasync_path(bpath) != 0) {
                if (errno != ENOENT) { free(all); return -1; }
                continue;
            }
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        free(all);
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }

    /* 2. K barrier: one mmap durability wait for this dirty kf shard. */
    if (slotcask_bulk_shard_flush_kf(req, kf_shard_id) != 0) return -1;

    /* 3. Segment barriers: activation + tombstone bytes, this shard's
          windows only, one pass each. */
    for (int pass = 0; pass < 2; pass++) {
        size_t total = 0;
        for (size_t i = 0; i < rs->nwindows; i++)
            if (size_add_checked(&total,
                    pass == 0 ? rs->windows[i].na : rs->windows[i].nt) != 0)
                return -1;
        if (total == 0) continue;
        if (total > SIZE_MAX / sizeof(SegLoc)) {
            errno = EOVERFLOW;
            return -1;
        }
        SegLoc *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        for (size_t i = 0; i < rs->nwindows; i++) {
            ReqWindow *rw = &rs->windows[i];
            if (pass == 0) {
                if (rw->na) memcpy(all + n, rw->a_locs,
                                   rw->na * sizeof(*all));
                n += rw->na;
            } else {
                if (rw->nt) memcpy(all + n, rw->t_locs,
                                   rw->nt * sizeof(*all));
                n += rw->nt;
            }
        }
        if (n > 1 && !segloc_is_sorted(all, n))
            qsort(all, n, sizeof(*all), segloc_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0)
                all[w++] = all[i];
        uint64_t t0s = now_us();
        int rc = bulk_seg_apply_and_sync(req->db, all, w, 0, 0);
        int saved_errno = rc != 0 ? (errno ? errno : EIO) : 0;
        commit_phase_us_record(&g_commit_segment_sync_us_total, t0s);
        commit_phase_us_record(&g_commit_segment_post_us_total, t0s);
        free(all);
        if (rc != 0) {
            errno = saved_errno;
            return -1;
        }
    }
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_A)) {
        errno = EIO;
        return -1;
    }
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_T)) {
        errno = EIO;
        return -1;
    }

    /* 4. Batched clear: unlink every converged window's marker of this
          shard via its preserved path, then ONE dir fsync. Failed
          (non-converged) windows stay retained. */
    int any_unlinked = 0;
    int unlink_rc = 0;
    int unlink_errno = 0;
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->published || !rw->converged || rw->cleared) continue;
        if (kfm2_unlink_by_path(rw->marker_path) != 0) {
            if (!unlink_errno) unlink_errno = errno;
            unlink_rc = -1;
            continue; /* still fsync every successful unlink */
        }
        rw->unlink_succeeded = 1;
        any_unlinked = 1;
    }
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked && fsync_dir(req->kf_dir) != 0) dir_rc = -1;
    if (any_unlinked)
        commit_phase_us_record(&g_commit_marker_clear_us_total, t0c);
    if (any_unlinked && dir_rc == 0 &&
        SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) {
        errno = EIO;
        dir_rc = -1;
    }
    if (dir_rc != 0) return -1; /* no reclaim: a marker may survive crash */

    /* 5. Directory-durable clears: reclaim OLD capacity, then transfer
          terminal ownership. This is the only deferred commit_done site. */
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        if (!rw->unlink_succeeded || rw->cleared) continue;
        rw->cleared = 1;
        bulk_reclaim_old_payloads_locked(&rs->txn, &rw->plan);
        if (rw->hooks_staged && rw->hooks.commit_done)
            rw->hooks.commit_done(rw->hooks.ctx, rw->hook_state);
        rw->hooks_staged = 0;
        rw->hook_state = NULL;
    }
    /* Deterministic pause surface for cross-process crash tests (per
       shard: the same point the request-wide pass offered, scoped to
       this shard's cleared windows). */
    durability_test_pause(req->db->data_dir, "bulk-window-cleared");
    if (unlink_rc != 0) errno = unlink_errno ? unlink_errno : EIO;
    return unlink_rc;
}
```

**Edit 3.8** — slotcask.c, adapt the wave driver in
`slotcask_bulk_request_execute` to the per-shard helpers (still one
request-wide fold — Task 4 replaces it). Anchor:

```c
    uint64_t t0st = now_us();
    int stage_rc = req_run_phase(req, inputs, ninputs, REQ_STAGE);
    commit_phase_us_record(&g_bulk_stage_us_total, t0st);
    req->payload_rc = slotcask_bulk_request_flush_payloads(req);
```

replace with:

```c
    uint64_t t0st = now_us();
    int stage_rc = req_run_phase(req, inputs, ninputs, REQ_STAGE);
    commit_phase_us_record(&g_bulk_stage_us_total, t0st);
    req->payload_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++) {
        int s = req->touched[k];
        if (slotcask_bulk_shard_flush_payloads(req, s) != 0) {
            req->shards[s].payload_failed = 1;
            req->payload_rc = -1;
        }
    }
```

Anchor:

```c
    req->marker_dir_rc = slotcask_bulk_request_flush_marker_dir(req);
```

replace with:

```c
    req->marker_dir_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++) {
        int s = req->touched[k];
        if (slotcask_bulk_shard_flush_marker_dir(req, s) != 0) {
            req->shards[s].marker_dir_failed = 1;
            req->marker_dir_rc = -1;
        }
    }
```

Anchor:

```c
    req->commit_rc = slotcask_bulk_request_flush_commit(req);
```

replace with:

```c
    req->commit_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++)
        if (slotcask_bulk_shard_flush_commit(req, req->touched[k]) != 0)
            req->commit_rc = -1;
```

**Edit 3.1 (execute first)** — test file, scenarios 10 and 11 (insert after scenario 9's
block, before `rf_db_close(&w);`). Direct-input form (like scenario 6a)
because both need per-input `rc` visibility:

```c
    /* ── Scenario 10: pre-M failure isolation — the P barrier fails for
     * the first pipelined shard only; the sibling shard commits. Red
     * until Task 4: the wave fold marks every shard's records -1. ── */
    {
        static RfBatch b0, b1;
        static char k0[16][24], v0[16][24];
        static SlotcaskBulkRec r0[16], p0[16];
        static char k1[16][24], v1[16][24];
        static SlotcaskBulkRec r1[16], p1[16];
        b0.keys = k0; b0.vals = v0; b0.recs = r0; b0.perm = p0;
        b1.keys = k1; b1.vals = v1; b1.recs = r1; b1.perm = p1;
        rf_batch_fill(&b0, 30, 16, "vA", 0);
        rf_batch_fill(&b1, 31, 16, "vB", 1);
        SlotcaskBulkShardInput ins[2];
        SlotcaskBulkOpts o0, o1;
        int deg0 = 0, deg1 = 0;
        rf_fill_opts(&o0, NULL); o0.out_durability_degraded = &deg0;
        rf_fill_opts(&o1, NULL); o1.out_durability_degraded = &deg1;
        memset(ins, 0, sizeof(ins));
        ins[0].kf_shard_id = 0; ins[0].recs = r0; ins[0].nrecs = b0.n;
        ins[0].kind = SLOTCASK_BULK_INPUT_UPSERT; ins[0].opts.upsert = o0;
        ins[1].kf_shard_id = 1; ins[1].recs = r1; ins[1].nrecs = b1.n;
        ins[1].kind = SLOTCASK_BULK_INPUT_UPSERT; ins[1].opts.upsert = o1;

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_P;
        g_shard_test_fail_occurrence = 1;
        errno = 0;   /* the injected failure leaves errno untouched; a
                        stale EINPROGRESS from an earlier scenario would
                        otherwise surface through the hard-failure fold */
        ASSERT_EQ_INT(slotcask_bulk_request_execute(&w.db, ins, 2), -1,
                      "P failure yields hard rc -1");
        ASSERT_TRUE(errno != EINPROGRESS,
                    "pre-M failure is not EINPROGRESS");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        ASSERT_EQ_INT(ins[0].rc, -1, "shard 0 hard-failed");
        ASSERT_EQ_INT(ins[1].rc, 0, "shard 1 unaffected");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "no markers retained");
        ASSERT_EQ_INT(deg0, 0, "shard 0 not degraded (pre-M)");
        ASSERT_EQ_INT(deg1, 0, "shard 1 not degraded");
        int s0bad = 0, s1ok = 1;
        for (size_t i = 0; i < b0.n; i++) s0bad |= r0[i].status != -1;
        for (size_t i = 0; i < b1.n; i++) s1ok &= r1[i].status == 0;
        ASSERT_TRUE(s0bad == 0, "shard 0 records all -1");
        ASSERT_TRUE(s1ok, "shard 1 records all 0");
        ASSERT_TRUE(rf_record_visible(&w, b1.keys[0], "vB-0000"),
                    "shard 1 record visible after sibling P failure");
    }

    /* ── Scenario 11: post-M failure isolation — the marker-clear dir
     * fsync fails for the first pipelined shard; its marker is retained
     * (EINPROGRESS, degraded, rc -2) while the sibling shard commits
     * clean. Red on the wave coordinator (its single batched clear
     * fails atomically for the whole request); the fix is Task 3's
     * per-shard commit step, so this goes green within Task 3. ── */
    {
        static RfBatch b0, b1;
        static char k0[16][24], v0[16][24];
        static SlotcaskBulkRec r0[16], p0[16];
        static char k1[16][24], v1[16][24];
        static SlotcaskBulkRec r1[16], p1[16];
        b0.keys = k0; b0.vals = v0; b0.recs = r0; b0.perm = p0;
        b1.keys = k1; b1.vals = v1; b1.recs = r1; b1.perm = p1;
        rf_batch_fill(&b0, 40, 16, "vC", 0);
        rf_batch_fill(&b1, 41, 16, "vD", 1);
        SlotcaskBulkShardInput ins[2];
        SlotcaskBulkOpts o0, o1;
        int deg0 = 0, deg1 = 0;
        rf_fill_opts(&o0, NULL); o0.out_durability_degraded = &deg0;
        rf_fill_opts(&o1, NULL); o1.out_durability_degraded = &deg1;
        memset(ins, 0, sizeof(ins));
        ins[0].kf_shard_id = 0; ins[0].recs = r0; ins[0].nrecs = b0.n;
        ins[0].kind = SLOTCASK_BULK_INPUT_UPSERT; ins[0].opts.upsert = o0;
        ins[1].kf_shard_id = 1; ins[1].recs = r1; ins[1].nrecs = b1.n;
        ins[1].kind = SLOTCASK_BULK_INPUT_UPSERT; ins[1].opts.upsert = o1;

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_C;
        g_shard_test_fail_occurrence = 1;
        ASSERT_EQ_INT(slotcask_bulk_request_execute(&w.db, ins, 2), -1,
                      "C failure rc -1");
        ASSERT_EQ_INT(errno, EINPROGRESS, "post-M failure errno");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        ASSERT_EQ_INT(ins[0].rc, -2, "shard 0 retained (rc -2)");
        ASSERT_EQ_INT(deg0, 1, "shard 0 degraded");
        ASSERT_EQ_INT(ins[1].rc, 0, "shard 1 unaffected");
        ASSERT_EQ_INT(deg1, 0, "shard 1 not degraded");
        ASSERT_TRUE(rf_record_visible(&w, b0.keys[0], "vC-0000"),
                    "shard 0 records visible (K applied before clear)");
        ASSERT_TRUE(rf_record_visible(&w, b1.keys[0], "vD-0000"),
                    "shard 1 records visible");
        /* Golden follow-up single write on the retained shard: the gate
           replays (or finds already-applied) and the state is clean. */
        char fk[24], fv[24];
        rf_key_shard0("rf-follow-11", 0, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow11");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "follow-up write succeeds on retained shard");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "markers clean at end");
    }
```

Although the helper replacements are described above first so their
source context stays together, edit numbers are the execution order:
apply **Edit 3.1 only**, then run the red proof below. Do not apply Edits
3.2, 3.2a, or 3.3–3.8 until that proof has been pasted.

**Verify after Edit 3.1 only (red/red; this is the base-red proof for both
new tests — paste both):**

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
```

Expected reds on the wave coordinator:
- Scenario 10: `shard 1 records all 0` fails (the request-wide P fold
  sets every shard's records -1) and `shard 1 record visible after
  sibling P failure` fails.
- Scenario 11: `shard 1 unaffected` fails — the wave's single batched
  clear fails atomically for the whole request, so shard 1's windows
  are retained too (`ins[1].rc == -2`) — and `shard 1 not degraded`
  fails (`deg1 == 1`).

Then apply Edits 3.2, 3.2a, and 3.3–3.8 and verify (paste again):

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
rtk proxy ./build/bin/shard-db-test run-all
```

Expected after Edits 3.1, 3.2, 3.2a, and 3.3–3.8:
- **Scenario 11 green** — the per-shard commit step isolates the clear
  failure (this test's fix lands here, in Task 3).
- **Scenario 10 still red, narrowed to `shard 0 hard-failed`
  (`ins[0].rc == 0`)** — the wave driver has no per-input rc for a
  payload-barrier failure. The plan originally predicted the status
  asserts (`shard 1 records all 0` / `shard 1 record visible`) would
  also stay red; measurement showed otherwise: `bulk_plan_window_locked`
  (the publish-phase planner) re-initialises each record's `status` to
  0 when it plans a window, so the still-publishing sibling shard's
  records lose the request-wide fold's -1 marks during its own publish.
  The red that remains is the genuine wave-driver isolation gap Task 4
  fixes. (Task 4's pipeline is unaffected by the planner reset: the
  P-failed shard's publish is skipped, so its -1 marks stand.)
- Scenarios 8 and 9, plus Task 2's multi-shard crash case, remain red for
  their already-proved gate-set/wave and missing qualified-seam reasons;
  `run-all` therefore exits nonzero only in `test-request-flush-batching`
  (scenarios 8, 9, and 10) and
  `test-durability-bulk-pipeline-multishard-crash`. Every pre-existing
  assertion and every other registered case stays green.

Note the note-sync occurrence semantics are already per-shard after
this task (helpers fire once per touched shard, ascending); scenarios
2/3's single-shard injections are unaffected — confirmed by the suite
staying green on them.

### Task 4 — The pipeline loop (B1 proper)

Replaces the wave driver with the caller-thread per-shard pipeline,
shrinks begin/end, adds the per-shard release and outcome fold, deletes
the dead request-wide pieces. Scenarios 8, 9, and 10 turn green here;
scenario 11 must remain green.

**Edit 4.1** — slotcask.c, begin loses its gate loop. Anchor:

```c
    /* Acquire touched writer gates in ascending order. Ordinary writers
       take exactly one gate, so no acquisition cycle is possible. */
    for (size_t k = 0; k < req->ntouched; k++)
        writer_gate_lock(db, req->touched[k]);
    return req;
}
```

replace with:

```c
    /* Gates are acquired per shard inside the pipeline loop
       (slotcask_bulk_shard_pipeline): at most one held per request at
       any instant. */
    return req;
}
```

**Edit 4.2** — slotcask.c, request_end shrinks to container frees.
Anchor (the entire function):

```c
static void slotcask_bulk_request_end(SlotcaskBulkRequest *req) {
    if (!req) return;
    SlotcaskDb *db = req->db;
    for (int s = 0; s < req->num_shards; s++) {
        ReqShard *rs = &req->shards[s];
        if (rs->has_txn && rs->shard.st) {
            for (size_t i = 0; i < rs->shard.nrecs; i++) {
                free(rs->shard.st[i].old_buf);
                rs->shard.st[i].old_buf = NULL;
            }
        }
        if (rs->has_txn)
            bulk_mutation_txn_free_state(&rs->txn);
        for (size_t i = 0; i < rs->nwindows; i++) {
            ReqWindow *rw = &rs->windows[i];
            /* Retained (EINPROGRESS) windows keep their marker for
               gate/startup replay; their opaque hook state is released
               through the path's release hook. */
            if (rw->hooks_staged) {
                if (rw->hooks.release_window)
                    rw->hooks.release_window(rw->hooks.ctx,
                                             rw->hook_state);
                rw->hooks_staged = 0;
                rw->hook_state = NULL;
            }
            bulk_window_plan_destroy(&rw->plan);
            free(rw->a_locs); free(rw->t_locs); free(rw->kf_slots);
        }
        free(rs->windows);
        free(rs->p_locs);
        free(rs->res_map);
        rs->res_map = NULL;
    }
    /* Same coordinator thread that acquired the gates releases them after
       terminal hook and request-state cleanup has finished. */
    for (size_t k = req->ntouched; k > 0; k--)
        writer_gate_unlock(db, req->touched[k - 1]);
    free(req->shards);
    free(req->touched);
    free(req);
}
```

replace with:

```c
/* Terminal per-shard cleanup — runs inside the pipeline while shard
   kf_shard_id's writer gate is still held, before release (G4):
   retained (published, uncleared) windows fire release_window so the
   next gate holder never encounters hook state owned by this request,
   and every per-shard resource (windows, plans, loc vectors, txn state)
   is freed here. Per-shard body of the former
   slotcask_bulk_request_end. */
static void slotcask_bulk_shard_release(SlotcaskBulkRequest *req,
                                        int kf_shard_id) {
    ReqShard *rs = &req->shards[kf_shard_id];
    if (rs->has_txn && rs->shard.st) {
        for (size_t i = 0; i < rs->shard.nrecs; i++) {
            free(rs->shard.st[i].old_buf);
            rs->shard.st[i].old_buf = NULL;
        }
    }
    if (rs->has_txn)
        bulk_mutation_txn_free_state(&rs->txn);
    for (size_t i = 0; i < rs->nwindows; i++) {
        ReqWindow *rw = &rs->windows[i];
        /* Retained (EINPROGRESS) windows keep their marker for
           gate/startup replay; their opaque hook state is released
           through the path's release hook. */
        if (rw->hooks_staged) {
            if (rw->hooks.release_window)
                rw->hooks.release_window(rw->hooks.ctx, rw->hook_state);
            rw->hooks_staged = 0;
            rw->hook_state = NULL;
        }
        bulk_window_plan_destroy(&rw->plan);
        free(rw->a_locs); free(rw->t_locs); free(rw->kf_slots);
    }
    free(rs->windows);
    rs->windows = NULL; rs->nwindows = 0; rs->cap_windows = 0;
    free(rs->p_locs);
    rs->p_locs = NULL; rs->np = 0; rs->cap_p = 0;
    free(rs->res_map);
    rs->res_map = NULL;
}

/* Frees the request container. Since B1 every shard's terminal cleanup
   and gate release happen inside its own pipeline (under its gate), so
   nothing per-shard is left here. */
static void slotcask_bulk_request_end(SlotcaskBulkRequest *req) {
    if (!req) return;
    free(req->shards);
    free(req->touched);
    free(req);
}
```

**Edit 4.3** — slotcask.c, delete the dead wave machinery. Anchor the
complete contiguous block below and delete it (replace it with nothing):

```c
/* ── Phase dispatch + public coordinator ── */

typedef struct {
    SlotcaskBulkRequest   *req;
    SlotcaskBulkShardInput *in;
    int phase;
    int rc;
    int err;
} ReqPhaseArg;

enum { REQ_STAGE = 1, REQ_PUBLISH = 2, REQ_FINALIZE = 3 };

static void *req_phase_worker(void *raw) {
    ReqPhaseArg *a = raw;
    SlotcaskBulkShardInput *in = a->in;
    if (a->phase == REQ_STAGE) {
        const SlotcaskBulkOpts *up =
            in->kind == SLOTCASK_BULK_INPUT_UPSERT ? &in->opts.upsert : NULL;
        const SlotcaskBulkDeleteOpts *del =
            in->kind == SLOTCASK_BULK_INPUT_DELETE ? &in->opts.delete_ : NULL;
        a->rc = slotcask_bulk_stage_shard(a->req, in->kf_shard_id,
                                           in->recs, in->nrecs, up, del);
    } else if (a->phase == REQ_PUBLISH) {
        a->rc = slotcask_bulk_publish_shard(a->req, in->kf_shard_id);
    } else {
        a->rc = slotcask_bulk_finalize_shard(a->req, in->kf_shard_id);
    }
    a->err = a->rc == 0 ? 0 : errno;
    return NULL;
}

static int req_run_phase(SlotcaskBulkRequest *req,
                         SlotcaskBulkShardInput *inputs, size_t ninputs,
                         int phase) {
    ReqPhaseArg *args = calloc(ninputs, sizeof(*args));
    if (!args) return -1;
    for (size_t i = 0; i < ninputs; i++) {
        args[i].req = req;
        args[i].in = &inputs[i];
        args[i].phase = phase;
    }
    parallel_for_io(req_phase_worker, args, (int)ninputs, sizeof(*args));
    int rc = 0, saved = 0;
    for (size_t i = 0; i < ninputs; i++) {
        if (args[i].rc != 0) {
            inputs[i].rc = -1;
            rc = -1;
            if (!saved) saved = args[i].err;
        }
    }
    free(args);
    if (rc != 0) errno = saved ? saved : EIO;
    return rc;
}

static void req_mark_durability_degraded(SlotcaskBulkRequest *req) {
    for (size_t i = 0; i < req->ntouched; i++) {
        ReqShard *rs = &req->shards[req->touched[i]];
        int retained = 0;
        for (size_t w = 0; w < rs->nwindows; w++)
            retained |= rs->windows[w].published && !rs->windows[w].cleared;
        if (!retained) continue;
        int *out = rs->kind == BULK_MUTATION_UPSERT
                 ? rs->opts.upsert.out_durability_degraded
                 : rs->opts.delete_.out_durability_degraded;
        if (out) *out = 1;
    }
}
```

**Edit 4.4** — slotcask.c, `SlotcaskBulkRequest` struct. Anchor:

```c
    int        *touched;               /* [ntouched] ascending, deduped    */
    size_t      ntouched;
    int         any_published;
    int         payload_rc, publish_rc, marker_dir_rc, finalize_rc, commit_rc;
    int         any_failed;            /* any retained/unreplayed window   */
};
```

replace with:

```c
    int        *touched;               /* [ntouched] ascending, deduped    */
    size_t      ntouched;
    /* B1: per-shard outcomes are folded into ReqShard before each gate
       release; the request keeps only the aggregate for the final
       errno. */
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
};
```

**Edit 4.5** — slotcask.c, replace the driver. After Task 3, anchor the
complete function below and replace it in full:

```c
int slotcask_bulk_request_execute(SlotcaskDb *db,
                                  SlotcaskBulkShardInput *inputs,
                                  size_t ninputs) {
    if (!db || !inputs || ninputs == 0 || ninputs > (size_t)INT_MAX) {
        errno = EINVAL;
        return -1;
    }
    int *touched = malloc(ninputs * sizeof(*touched));
    if (!touched) return -1;
    for (size_t i = 0; i < ninputs; i++) {
        if (inputs[i].kf_shard_id < 0 ||
            inputs[i].kf_shard_id >= db->num_shards ||
            !inputs[i].recs || inputs[i].nrecs == 0 ||
            (inputs[i].kind != SLOTCASK_BULK_INPUT_UPSERT &&
             inputs[i].kind != SLOTCASK_BULK_INPUT_DELETE)) {
            free(touched);
            errno = EINVAL;
            return -1;
        }
        inputs[i].rc = 0;
        int *degraded = inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT
                      ? inputs[i].opts.upsert.out_durability_degraded
                      : inputs[i].opts.delete_.out_durability_degraded;
        if (degraded) *degraded = 0;
        touched[i] = inputs[i].kf_shard_id;
        for (size_t j = 0; j < i; j++) {
            if (inputs[j].kf_shard_id == inputs[i].kf_shard_id) {
                free(touched);
                errno = EINVAL;
                return -1;
            }
        }
    }
    SlotcaskBulkRequest *req =
        slotcask_bulk_request_begin(db, touched, ninputs);
    free(touched);
    if (!req) return -1;

    uint64_t t0st = now_us();
    int stage_rc = req_run_phase(req, inputs, ninputs, REQ_STAGE);
    commit_phase_us_record(&g_bulk_stage_us_total, t0st);
    req->payload_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++) {
        int s = req->touched[k];
        if (slotcask_bulk_shard_flush_payloads(req, s) != 0) {
            req->shards[s].payload_failed = 1;
            req->payload_rc = -1;
        }
    }
    if (req->payload_rc != 0) {
        for (size_t si = 0; si < ninputs; si++)
            for (size_t ri = 0; ri < inputs[si].nrecs; ri++)
                if (inputs[si].recs[ri].status == 0)
                    inputs[si].recs[ri].status = -1;
    }
    /* Every phase is joined even after failure. The phase helpers enforce
       the shard/window predicates and become no-ops when ineligible. */
    req->publish_rc = req_run_phase(req, inputs, ninputs, REQ_PUBLISH);
    req->any_published = 0;
    for (size_t si = 0; si < req->ntouched; si++) {
        ReqShard *rs = &req->shards[req->touched[si]];
        for (size_t wi = 0; wi < rs->nwindows; wi++)
            req->any_published |= rs->windows[wi].published;
    }
    req->marker_dir_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++) {
        int s = req->touched[k];
        if (slotcask_bulk_shard_flush_marker_dir(req, s) != 0) {
            req->shards[s].marker_dir_failed = 1;
            req->marker_dir_rc = -1;
        }
    }
    if (req->any_published) {
        SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_SHARD_PUBLISHED);
        durability_test_pause(req->db->data_dir, "shard-published");
    }
    req->finalize_rc = req_run_phase(req, inputs, ninputs, REQ_FINALIZE);
    req->any_failed = 0;
    for (size_t si = 0; si < req->ntouched; si++)
        req->any_failed |= req->shards[req->touched[si]].failed;
    req->commit_rc = 0;
    for (size_t k = 0; k < req->ntouched; k++)
        if (slotcask_bulk_shard_flush_commit(req, req->touched[k]) != 0)
            req->commit_rc = -1;

    int pending = req->any_failed ||
                  (req->any_published &&
                   (req->marker_dir_rc != 0 || req->finalize_rc != 0 ||
                    req->commit_rc != 0));
    int failed = stage_rc != 0 || req->payload_rc != 0 ||
                 req->publish_rc != 0 || req->marker_dir_rc != 0 ||
                 req->finalize_rc != 0 || req->commit_rc != 0;
    int saved = pending ? EINPROGRESS : (failed ? (errno ? errno : EIO) : 0);
    if (pending) {
        req_mark_durability_degraded(req);
        for (size_t i = 0; i < ninputs; i++) {
            ReqShard *rs = &req->shards[inputs[i].kf_shard_id];
            for (size_t w = 0; w < rs->nwindows; w++)
                if (rs->windows[w].published && !rs->windows[w].cleared) {
                    inputs[i].rc = -2;
                    break;
                }
        }
    }
    slotcask_bulk_request_end(req);
    if (failed) { errno = saved; return -1; }
    return 0;
}
```

Replace it with:

```c
/* B1 comparator: order input pointers by kf_shard_id so the pipeline
   loop walks req->touched's ascending order. */
static int bulk_input_shard_cmp(const void *ap, const void *bp) {
    const SlotcaskBulkShardInput *a = *(SlotcaskBulkShardInput *const *)ap;
    const SlotcaskBulkShardInput *b = *(SlotcaskBulkShardInput *const *)bp;
    return (a->kf_shard_id > b->kf_shard_id) -
           (a->kf_shard_id < b->kf_shard_id);
}

/* One shard's full commit pipeline, run by the request's caller thread
   while holding exactly this shard's writer gate (G1, G3):
   gate → replay+stage → P → publish (M) → [shard-published seam] →
   finalize (A/I/K/T) → commit barriers (I/K/A/T, clear + dir fsync,
   reclaim, commit_done) → per-shard outcome fold → terminal release →
   gate release. Every step runs even after an earlier step failed; the
   per-shard predicates in the phase helpers make ineligible steps
   no-ops (the same rule the wave coordinator applied via its phase
   joins). */
static void slotcask_bulk_shard_pipeline(SlotcaskBulkRequest *req,
                                         SlotcaskBulkShardInput *in) {
    SlotcaskDb *db = req->db;
    int s = in->kf_shard_id;
    ReqShard *rs = &req->shards[s];

    writer_gate_lock(db, s);

    /* Stage (gate replay + folded pre-grow + staging). A stage failure
       has already marked every record -1 inside
       slotcask_bulk_stage_shard. */
    uint64_t t0st = now_us();
    if (slotcask_bulk_stage_shard(req, s, in->recs, in->nrecs,
            in->kind == SLOTCASK_BULK_INPUT_UPSERT ? &in->opts.upsert : NULL,
            in->kind == SLOTCASK_BULK_INPUT_DELETE ? &in->opts.delete_
                                                   : NULL) != 0) {
        rs->step_failed = 1;
        if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
    }
    commit_phase_us_record(&g_bulk_stage_us_total, t0st);

    /* P barrier for this shard only. Failure is a hard error for THIS
       shard: publish is skipped for it (rs->payload_failed) and its
       records report -1; sibling shards are untouched. */
    if (!rs->stage_failed) {
        if (slotcask_bulk_shard_flush_payloads(req, s) != 0) {
            rs->payload_failed = 1;
            rs->step_failed = 1;
            if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
        }
    }
    if (rs->stage_failed || rs->payload_failed)
        for (size_t i = 0; i < in->nrecs; i++)
            if (in->recs[i].status == 0) in->recs[i].status = -1;

    /* Publish windows (skipped when stage/P failed), then this shard's
       M dir fsync. */
    if (!rs->stage_failed && !rs->payload_failed) {
        if (slotcask_bulk_publish_shard(req, s) != 0) {
            rs->step_failed = 1;
            if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
        }
    }
    if (slotcask_bulk_shard_flush_marker_dir(req, s) != 0) {
        rs->marker_dir_failed = 1;
        rs->step_failed = 1;
        if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
    }

    /* Per-shard published seam: this shard's markers are published and
       dir-durable; its finalize has not run. Supersedes the
       request-wide REQ_PUBLISHED / "req-published" seam. */
    {
        int any_pub = 0;
        for (size_t i = 0; i < rs->nwindows; i++)
            any_pub |= rs->windows[i].published;
        if (any_pub && !rs->marker_dir_failed) {
            char pause_phase[64];
            snprintf(pause_phase, sizeof(pause_phase),
                     "shard-published-%03x", (unsigned)s);
            SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_SHARD_PUBLISHED);
            durability_test_pause(req->db->data_dir, pause_phase);
        }
    }

    /* Finalize (A/I/K/T apply per window, syncs deferred into the
       commit step; the idempotent forward retry of a failed window
       runs inside). */
    if (slotcask_bulk_finalize_shard(req, s) != 0) {
        rs->step_failed = 1;
        if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
    }

    /* Commit barriers + clear for this shard. */
    if (slotcask_bulk_shard_flush_commit(req, s) != 0) {
        rs->step_failed = 1;
        if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
    }

    /* Per-shard outcome fold, BEFORE the gate release: retained windows
       own the terminal state (degraded flag, rc -2); hard-failed shards
       report rc -1; clean shards rc 0. */
    rs->retained = 0;
    for (size_t i = 0; i < rs->nwindows; i++)
        rs->retained |= rs->windows[i].published && !rs->windows[i].cleared;
    if (rs->retained) {
        int *out = rs->kind == BULK_MUTATION_UPSERT
                 ? rs->opts.upsert.out_durability_degraded
                 : rs->opts.delete_.out_durability_degraded;
        if (out) *out = 1;
        in->rc = -2;
    } else if (rs->step_failed) {
        in->rc = -1;
    }

    /* Snapshot the fold before per-shard release invalidates/frees its
       owned state. */
    int retained = rs->retained;
    int step_failed = rs->step_failed;
    int step_errno = rs->step_errno;

    /* Terminal hooks + per-shard frees UNDER the gate (G4), then
       release: the next gate holder can never observe this request's
       hook state or staged resources. */
    slotcask_bulk_shard_release(req, s);
    writer_gate_unlock(db, s);

    if (retained) req->any_pending = 1;
    if (step_failed || retained) req->any_failed = 1;
    if (step_failed && !req->saved_errno && step_errno)
        req->saved_errno = step_errno;
}

int slotcask_bulk_request_execute(SlotcaskDb *db,
                                  SlotcaskBulkShardInput *inputs,
                                  size_t ninputs) {
    if (!db || !inputs || ninputs == 0 || ninputs > (size_t)INT_MAX) {
        errno = EINVAL;
        return -1;
    }
    int *touched = malloc(ninputs * sizeof(*touched));
    if (!touched) return -1;
    for (size_t i = 0; i < ninputs; i++) {
        if (inputs[i].kf_shard_id < 0 ||
            inputs[i].kf_shard_id >= db->num_shards ||
            !inputs[i].recs || inputs[i].nrecs == 0 ||
            (inputs[i].kind != SLOTCASK_BULK_INPUT_UPSERT &&
             inputs[i].kind != SLOTCASK_BULK_INPUT_DELETE)) {
            free(touched);
            errno = EINVAL;
            return -1;
        }
        inputs[i].rc = 0;
        int *degraded = inputs[i].kind == SLOTCASK_BULK_INPUT_UPSERT
                      ? inputs[i].opts.upsert.out_durability_degraded
                      : inputs[i].opts.delete_.out_durability_degraded;
        if (degraded) *degraded = 0;
        touched[i] = inputs[i].kf_shard_id;
        for (size_t j = 0; j < i; j++) {
            if (inputs[j].kf_shard_id == inputs[i].kf_shard_id) {
                free(touched);
                errno = EINVAL;
                return -1;
            }
        }
    }
    SlotcaskBulkRequest *req =
        slotcask_bulk_request_begin(db, touched, ninputs);
    free(touched);
    if (!req) return -1;

    /* Pipelined execution (B1): the caller thread runs each touched
       shard's complete pipeline in ascending order, holding at most one
       writer gate at any instant. Concurrent requests therefore
       pipeline across shards; access to one shard serializes on its
       gate exactly as a single-record writer does. A request issued
       from inside a pool task runs its per-file I/O inline — the same
       rule parallel_for_io already applied to the wave coordinator. */
    SlotcaskBulkShardInput **order = malloc(ninputs * sizeof(*order));
    if (!order) {
        slotcask_bulk_request_end(req);
        errno = ENOMEM;
        return -1;
    }
    for (size_t i = 0; i < ninputs; i++) order[i] = &inputs[i];
    qsort(order, ninputs, sizeof(*order), bulk_input_shard_cmp);

    /* execute() validated the inputs unique, so order[] matches
       req->touched[k] exactly. */
    for (size_t k = 0; k < req->ntouched; k++)
        slotcask_bulk_shard_pipeline(req, order[k]);
    free(order);

    /* Snapshot the aggregate before request_end frees req. */
    int any_pending = req->any_pending;
    int any_failed = req->any_failed;
    int saved_errno = req->saved_errno;
    slotcask_bulk_request_end(req);
    if (any_pending) { errno = EINPROGRESS; return -1; }
    if (any_failed) {
        errno = saved_errno ? saved_errno : EIO;
        return -1;
    }
    return 0;
}
```

**Edit 4.6** — slotcask.h, replace the complete public comment block so
its heading and concurrency claims stop documenting request-level batching
and gate-set semantics. Anchor:

```c
/* ============================================================ Deferred
 * bulk request (request-level commit batching).
 *
 * One synchronous request spans an entire cmd_bulk_* call: the caller
 * bucket-ises its records per kf shard and hands over one input per
 * touched shard. The coordinator acquires every touched shard's writer
 * gate (ascending) for the whole request, runs the two-epoch wave
 * protocol (stage → payload flush → publish → one kf-dir fsync →
 * finalize → commit flush), and releases the gates before returning.
 * Requests on disjoint shards run concurrently; requests sharing a shard
 * serialize on that shard's gate; readers never take the gate.
 *
 * Ownership: input records, option pointer targets, hook contexts, and
 * arenas remain owned by the caller and MUST stay valid until this call
 * returns (the per-window hook states flow through it opaquely).
 * Duplicate kf_shard_id inputs are rejected. */
```

replace with:

```c
/* ============================================================ Deferred
 * bulk request (per-shard commit pipelines).
 *
 * One synchronous request spans an entire cmd_bulk_* call: the caller
 * bucket-ises its records per kf shard and hands over one input per
 * touched shard. The coordinator pipelines the touched shards in
 * ascending order on the calling thread: per shard it takes that
 * shard's writer gate and runs its full commit pipeline (gate replay →
 * stage → payload flush → publish → kf-dir fsync → finalize → commit
 * barriers → marker clear → terminal cleanup) before releasing the
 * gate — at most one gate held per request at any instant, so
 * concurrent requests pipeline across shards.
 * Requests sharing a shard serialize only while each request runs that
 * shard's pipeline; readers never take the gate.
 *
 * Ownership: input records, option pointer targets, hook contexts, and
 * arenas remain owned by the caller and MUST stay valid until this call
 * returns (the per-window hook states flow through it opaquely).
 * Duplicate kf_shard_id inputs are rejected. */
```

**Verify (green expected — paste; scenario 11 has been green since
Task 3, scenarios 8/9/10 turn green here):**

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
```

Scenario 8 (G1 ≤ 1), scenario 9 (B completes while A paused), and
scenario 10 (pre-M isolation) must pass — paste the output. Limited-
pool behavior stays covered by the existing scenario 7 (7a: nested
pool-task caller, 8 inputs, inline fallback; 7b: 2-worker pool,
8-shard request) — with B1 a pipeline never enqueues pool tasks, so
7a/7b passing unchanged is itself part of the claim. Then the full
suite plus the crash set:

```bash
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy ./build/bin/shard-db-test run test-marker-v2
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-window-boundary
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-window-boundary-mixed-indexes
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-window-prepared-recovers
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-window-applied-recovers
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-pipeline-multishard-crash
```

**Mandatory regression-proof audit (CORE-PROCESS).** After the green run,
temporarily reverse every Task 4 replacement using the complete quoted
preimage/replacement blocks in Edits 4.1–4.6, without touching Tasks 1–3.
Rebuild and run the two regression cases below. Paste the real output and
confirm the expected old-behavior failures: scenario 8 observes more than
one gate, scenario 9 cannot make progress, scenario 10 loses sibling-shard
isolation, and the multi-shard crash test cannot reach its shard-qualified
pause.

```bash
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-request-flush-batching
rtk proxy ./build/bin/shard-db-test run test-durability-bulk-pipeline-multishard-crash
```

Then reapply Edits 4.1–4.6 exactly, rebuild, rerun both commands, and paste
their green output. If the reverse or reapply anchor does not match, the
global PLAN_NOTES halt rule applies; do not approximate the revert.

### Task 4b — Hotfix: the gate-replay temp sweep must be shard-scoped

**Found during execution** (2026-09-08): with the pipeline live,
`test-parallel-index-integrity` flaked (2/5 on B1, 6/6 green on base):
five concurrent wire bulk-inserts, one chunk responded
`{"inserted":19687,...,"errors":312,"error":"some_records_dropped"}` —
exactly one shard-input. Instrumented evidence:
`slotcask_bulk_publish_shard` → `bulk_publish_one_kf_window` failed with
**ENOENT** for whole windows. Root cause: `marker_refs_scan`'s temp
sweep (armed `cleanup_temps=1` from `kf_batch_marker_gate_refs` during
another request's stage) unlinked **every** shard's recognized
`*.tmp.*` publication temp in the shared `data/kf` dir — including a
temp mid-flight under a DIFFERENT shard's gate; its `link()` then
failed ENOENT and the window's records were dropped pre-marker. The
wave coordinator masked this: whole-request gate exclusivity meant a
scan could never overlap another request's publication. Under B1 it
can.

**Test-first.** Scenario 12 in test-request-flush-batching.c: create a
valid publication temp for shard 0 and for shard 7, run one shard-0-only
request, and assert the shard-0 temp was swept while the shard-7 temp
survives. Red on the pre-fix tree (`other-shard publication temp left
for its gate holder` fails); green after.

**Edit 4b.1** — test file, scenario 12 (insert after scenario 11's block,
before `rf_db_close(&w);`):

```c
    /* ── Scenario 12: the gate-replay temp sweep is shard-scoped — a
     * request on shard 0 sweeps only shard 0's publication temporaries.
     * Red until the B1 hotfix: the sweep unlinked every shard's temps,
     * which under per-shard pipelining races another request's in-flight
     * temp on a different shard (its link() fails ENOENT and the whole
     * window's records are dropped pre-marker). ── */
    {
        static RfBatch b;
        static char keys[16][24], vals[16][24];
        static SlotcaskBulkRec recs[16];
        static SlotcaskBulkRec perm50[16];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm50;
        rf_batch_fill(&b, 50, 16, "v50", 0);      /* shard 0 only */

        char kdir[PATH_MAX];
        snprintf(kdir, sizeof(kdir), "%s/data/kf", w.base);
        char tmp0[PATH_MAX], tmp7[PATH_MAX];
        snprintf(tmp0, sizeof(tmp0),
                 "%s/000_batch_0_00000000000000aa_marker.dat.tmp.%d.1",
                 kdir, (int)getpid());
        snprintf(tmp7, sizeof(tmp7),
                 "%s/007_batch_0_00000000000000bb_marker.dat.tmp.%d.2",
                 kdir, (int)getpid());
        FILE *f0 = fopen(tmp0, "w");
        ASSERT_NOT_NULL(f0, "create shard-0 publication temp");
        if (f0) fclose(f0);
        FILE *f7 = fopen(tmp7, "w");
        ASSERT_NOT_NULL(f7, "create shard-7 publication temp");
        if (f7) fclose(f7);

        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), 0,
                      "shard-0 request for sweep check");

        ASSERT_EQ_INT(access(tmp0, F_OK), -1,
                      "own-shard publication temp swept by gate replay");
        ASSERT_EQ_INT(access(tmp7, F_OK), 0,
                      "other-shard publication temp left for its gate holder");
        unlink(tmp7);
    }

    rf_db_close(&w);
```

**Edit 4b.2** — slotcask.c, `marker_tmp_name_valid` reports the temp's
shard. Anchor:

```c
static int marker_tmp_name_valid(const char *name) {
    int used = 0;
    unsigned shard = 0, batch = 0, pid = 0;
    unsigned long long nonce = 0, tmpnonce = 0;
    if (sscanf(name, "%x_batch_%u_%16llx_marker.dat.tmp.%u.%llu%n",
               &shard, &batch, &nonce, &pid, &tmpnonce, &used) == 5 &&
        used == (int)strlen(name))
        return 1;
    used = 0;
    return sscanf(name, "%x_batch_%u_marker.dat.tmp.%u.%llu%n",
                  &shard, &batch, &pid, &tmpnonce, &used) == 4 &&
           used == (int)strlen(name);
}
```

replace with:

```c
static int marker_tmp_name_valid(const char *name, int *out_shard) {
    int used = 0;
    unsigned shard = 0, batch = 0, pid = 0;
    unsigned long long nonce = 0, tmpnonce = 0;
    if (sscanf(name, "%x_batch_%u_%16llx_marker.dat.tmp.%u.%llu%n",
               &shard, &batch, &nonce, &pid, &tmpnonce, &used) == 5 &&
        used == (int)strlen(name)) {
        if (out_shard) *out_shard = (int)shard;
        return 1;
    }
    used = 0;
    if (sscanf(name, "%x_batch_%u_marker.dat.tmp.%u.%llu%n",
               &shard, &batch, &pid, &tmpnonce, &used) == 4 &&
        used == (int)strlen(name)) {
        if (out_shard) *out_shard = (int)shard;
        return 1;
    }
    return 0;
}
```

**Edit 4b.3** — slotcask.c, `marker_refs_scan` sweeps only its own
shard's temps. Anchor (the function's comment through the sweep branch):

```c
/* Scan kf_dir for final marker files. cleanup_temps also removes
 * recognised publication temporaries (inert pre-M debris); an unlink
 * error fails the scan. Unrecognised marker-namespace files fail closed
 * with EILSEQ. Duplicate identity fails closed. */
```

through

```c
            if (marker_tmp_name_valid(de->d_name)) {
                if (!cleanup_temps) continue;
                char tmp_path[PATH_MAX];
                int tn = snprintf(tmp_path, sizeof(tmp_path), "%s/%s",
                                  kf_dir, de->d_name);
                if (tn < 0 || (size_t)tn >= sizeof(tmp_path)) {
                    errno = ENAMETOOLONG;
                    goto out;
                }
                if (unlink(tmp_path) != 0 && errno != ENOENT) goto out;
                continue;
            }
```

replace with:

```c
/* Scan kf_dir for final marker files. cleanup_temps also removes
 * recognised publication temporaries (inert pre-M debris) — but only for
 * wanted_shard (or all shards when wanted_shard < 0, i.e. startup): under
 * per-shard pipelining another request may hold a different shard's gate
 * with a publication temp in flight, and sweeping across shards would
 * unlink a live temp and fail its link() with ENOENT. An unlink error
 * fails the scan. Unrecognised marker-namespace files fail closed with
 * EILSEQ. Duplicate identity fails closed. */
```

and

```c
            int tmp_shard = -1;
            if (marker_tmp_name_valid(de->d_name, &tmp_shard)) {
                if (!cleanup_temps) continue;
                if (wanted_shard >= 0 && tmp_shard != wanted_shard)
                    continue;
                char tmp_path[PATH_MAX];
                int tn = snprintf(tmp_path, sizeof(tmp_path), "%s/%s",
                                  kf_dir, de->d_name);
                if (tn < 0 || (size_t)tn >= sizeof(tmp_path)) {
                    errno = ENAMETOOLONG;
                    goto out;
                }
                if (unlink(tmp_path) != 0 && errno != ENOENT) goto out;
                continue;
            }
```

**Safety note:** a shard-s temp's publisher must hold gate s, so while
this request holds gate s no live publisher of s exists — own-shard
temps are pure debris and sweeping them is safe. Other shards' temps
belong to their live gate holders (or crashed requests, swept by their
own shard's next holder; the startup global sweep at
`marker_refs_scan(kf_dir, -1, 1, ...)` is unchanged).

**Verify:** scenario 12 green; `test-parallel-index-integrity` 8
consecutive green runs (was 2/5 failing); two consecutive full
`run-all`s fully green (13088/0 each, measured 2026-09-08).

### Task 5 — Docs sync

**Edit 5.1** — docs/concepts/concurrency.md. Anchor: the section from
the heading `## Bulk mutations: two-epoch request batching and per-shard
writer gates` through the paragraph ending "startup recovery replays via
the same exact-path\n(`MarkerRef`) machinery." Replace the whole section
body with:

```markdown
## Bulk mutations: two-epoch pipelining and per-shard writer gates

Bulk mutations (all six `bulk-insert` / `bulk-update` / `bulk-delete` forms)
execute as **one deferred request** spanning the whole command. The request's
caller thread pipelines its touched shards: for each touched kf shard, in
ascending order, it takes that shard's **writer gate** — a plain per-shard
admission mutex (`SlotcaskDb.writer_gates`) — and runs that shard's complete
commit pipeline before releasing the gate and moving to the next shard. The
request therefore holds **at most one** writer gate at any instant;
concurrent bulk requests pipeline across shards, while shards they share
serialize on the gate. Ordinary mutating writers (single
insert/update/delete, legacy bulk entries, pre-grow) take exactly one gate
around their mutation, via the central legacy transaction
(`slotcask_bulk_mutation_transaction`). Readers never touch the gates. So:

- requests on disjoint shards of one object run concurrently, and requests
  overlapping only partially pipeline: one request's untouched shards
  proceed while another is still mid-request;
- requests sharing a shard serialize on that shard's gate, for that shard's
  pipeline only;
- a single write to a shard stalls at most for that shard's current
  pipeline; a write to an untouched shard proceeds;
- the kf rwlock stays **step-local**: acquired and released inside the same
  pipeline step, so readers only ever see coherent states (old committed
  record before finalize, new committed record after).

Per shard the pipeline runs, in order (durability invariants I1–I5 below
are per-window properties and are unchanged):

```
gate          : writer gate of shard s (ascending across the request)
replay+stage  : gate replay of retained markers → folded pre-grow → stage
barrier P     : this shard's staged payload bytes, deduped, one sync   [I2]
publish       : per window: plan → D5 fallback sync → no-replace link
barrier M     : ONE fsync(data/kf dir) — this shard's markers durable  [I4]
finalize      : per published window: A → I(apply) → K → T (syncs deferred)
commit        : merged index flush [I1] + one kf sync + A/T segment sync
                + batched marker unlink + ONE fsync(data/kf dir)
release       : terminal hooks (commit_done / release_window) and frees,
                then the writer gate
```

Failure stays shard- and window-scoped: a stage or payload-flush failure is
a hard error for that shard only (records report -1, no markers); one
window's publish failure doesn't stop its siblings; anything failing after a
window's marker publication (dir fsync, finalize, commit barriers, clear)
retains that window's marker and reports `EINPROGRESS` with
`out_durability_degraded` / per-shard `rc = -2`; sibling shards that already
completed stay committed. The next writer's gate replays any retained marker
before planning; startup recovery replays via the same exact-path
(`MarkerRef`) machinery.
```

**Edit 5.2** — AGENTS.md "Indexed-write crash safety" bullet. Anchor:

```
Bulk commands run as deferred requests (two-epoch waves, per-kf-shard writer admission gates held request-wide — see `docs/concepts/concurrency.md`); single writes keep per-window immediate durability.
```

replace with:

```
Bulk commands run as deferred requests (per-shard pipelines under per-kf-shard writer gates — at most one gate held per request at any instant — see `docs/concepts/concurrency.md`); single writes keep per-window immediate durability.
```

**Edit 5.3** — src/db/slotcask.h, update the `SlotcaskDb.writer_gates`
field comment. Anchor:

```c
    /* Per-kf-shard writer admission gates (request-level commit batching).
       Writers only: the deferred request's coordinator holds every touched
       shard's gate for the whole request (ascending acquire, reverse
       release); ordinary writers hold exactly one around their mutation.
       Readers never take it. */
```

replace with:

```c
    /* Per-kf-shard writer admission gates. Deferred requests take one gate
       at a time around that shard's complete pipeline; ordinary writers
       take exactly one around their mutation. Readers never take them. */
```

**Edit 5.4** — src/db/slotcask.c, update all five surviving internal
design comments using these complete anchors and replacements.

```c
/* ── Per-kf-shard writer admission gates (request-level batching) ──
   The deferred request's coordinator holds every touched shard's gate for
   the whole request; ordinary mutating writers lock the gate for their
   target shard around their mutation; readers never take it. Gates exist
   from the moment slotcask_open installs them, so no runtime writer can
   touch an uninitialised one. */
```

replace with:

```c
/* ── Per-kf-shard writer admission gates ────────────────────────────
   A deferred request takes one touched shard's gate around that shard's
   complete pipeline, releases it, then advances in ascending order.
   Ordinary mutating writers lock one target-shard gate around their
   mutation; readers never take it. Gates exist from the moment
   slotcask_open installs them, so no runtime writer can touch an
   uninitialised one. */
```

```c
/* ── Deferred request state (request-level commit batching) ──────────
 * One deferred bulk request spans an entire cmd_bulk_* call. The
 * coordinator holds the writer gate of every touched shard for the whole
 * request (ascending acquire, reverse release); ordinary writers to those
 * shards block on the gate; readers never touch it. Per-shard transaction
 * state persists across the coordinator's phase joins. */
```

replace with:

```c
/* ── Deferred request state (per-shard commit pipelines) ─────────────
 * One deferred bulk request spans an entire cmd_bulk_* call. Its caller
 * processes touched shards in ascending order, holding only the current
 * shard's writer gate through replay, stage, publish, finalize, commit,
 * and terminal cleanup. Readers never take the gate. */
```

```c
        /* Allocation failure fails this window's fallback records
           cleanly; the request-level durability-degraded path owns
           recovery, same as any other OOM inside a commit. */
```

replace with:

```c
        /* Allocation failure fails this window's fallback records
           cleanly. The shard pipeline reports a hard pre-marker failure;
           its publish path aborts any staged hook state. */
```

```c
        /* Deferred request: the touch set stays in plan->touch for the
           request-wide merged flush; no per-window sync here. */
```

replace with:

```c
        /* Deferred request: the touch set stays in plan->touch for this
           shard pipeline's merged commit flush; no per-window sync here. */
```

```c
    /* Pre-grow folded into the stage wave. The coordinator already holds
       this shard's writer gate for the whole request, so the resplit is
       exclusive against every writer by construction — and no pool task
       ever blocks on a gate. (A parallel pre-grow outside the request,
       as the original plan had it, let gate-blocked pregrow tasks from
       concurrent requests occupy every IO-pool worker; the running
       request's own waves then queued behind them and could never join —
       a circular wait.) The 75% load trigger matches kf_put_new's inline
       check; resplit in a loop for the same reason slotcask_pregrow_kf
       does. */
```

replace with:

```c
    /* Pre-grow is folded into this shard's pipeline stage. The caller
       already holds this shard's writer gate, so resplit is exclusive
       against every writer and no gate-blocked pre-grow task occupies an
       IO-pool worker. The 75% trigger matches kf_put_new's inline check;
       loop because one doubling may still be insufficient. */
```

```c
/* Wave 1 per shard: gate replay + stage; P sync deferred into p_locs. */
```

replace with:

```c
/* Pipeline stage step for one shard: gate replay + stage; the P sync
   follows inside the same shard pipeline. */
```

```c
    /* This function already runs in the coordinator's parallel stage wave.
       Do not call bulk_stage_payload_wave (that would nest the executor).
       Allocate the one shard's state and invoke its leaf worker inline. */
```

replace with:

```c
    /* This function runs inline on the request's caller thread inside the
       shard pipeline. Do not call bulk_stage_payload_wave (that would
       nest the executor). Allocate the one shard's state and invoke its
       leaf worker directly. */
```

**Edit 5.5** — docs/concepts/concurrency.md, update durability invariant
I4 below the replaced bulk section. Anchor:

```markdown
- **I4** — marker directory entries are durable (one `fsync(data/kf dir)`
  after the publish wave) before any window mutation (A/I/K/T) runs.
```

replace with:

```markdown
- **I4** — each shard pipeline makes that shard's marker directory entries
  durable with `fsync(data/kf dir)` before any of its window mutations
  (A/I/K/T) run.
```

**Edit 5.6** — docs/reference/changelog.md, keep the Unreleased entry
accurate. Anchor:

```markdown
**Request-level commit batching + marker V2 (2026.09).** Indexed bulk
insert/update/delete now execute as one deferred request with two-epoch
waves (stage → payload flush → publish → one marker-dir fsync → finalize →
commit flush), collapsing per-window durability barrier groups into three
request-level flush passes (~10× fewer durable ops on a 1M-record kv
insert, ~5–10× on a 100k × 14-index insert). Per-kf-shard **writer
admission gates** are held request-wide by the coordinator: requests on
disjoint shards run concurrently, single writes to a touched shard stall
for the request span, readers never block, and the kf rwlock stays
phase-local (readers between waves see only coherent old/new records).
```

replace with:

```markdown
**Per-shard bulk commit pipelines + marker V2 (2026.09).** Indexed bulk
insert/update/delete execute as one deferred request whose caller processes
touched shards in ascending order. For each shard it takes that shard's
writer gate and runs replay → stage → payload flush → marker publish and
directory sync → finalize → commit barriers → marker clear → terminal
cleanup before releasing the gate. A request therefore holds at most one
writer gate at a time: concurrent requests pipeline across shards, while
ordinary writes wait only for the current pipeline on their target shard.
Per-shard flushing sacrifices request-wide cross-shard deduplication and
within-request shard-wave parallelism in exchange for this concurrency;
the release measurement gate records the resulting throughput.
```

**Edit 5.7** — docs/getting-started/configuration.md, update the
`BULK_COMMIT_WINDOW` row. Anchor:

```markdown
| `BULK_COMMIT_WINDOW` | `16384` | Record count per marker window during bulk-insert/bulk-update/bulk-delete. Integer in `[16, 16384]` — the ceiling is the KFM2 marker entry-count limit. With request-level commit batching, the window bounds the marker file size (`16 + window × 32` bytes on disk) and the replay granularity after a crash — it no longer sets durability-barrier boundaries (barriers are per request: payload flush, marker-dir fsync, commit flush). Smaller windows shrink replay work; larger windows shrink per-record marker overhead. Shard writer-gate hold per request grows with total request size, not with the window. |
```

replace with:

```markdown
| `BULK_COMMIT_WINDOW` | `16384` | Record count per marker window during bulk-insert/bulk-update/bulk-delete. Integer in `[16, 16384]` — the ceiling is the KFM2 marker entry-count limit. With per-shard commit pipelines, the window bounds the marker file size (`16 + window × 32` bytes on disk) and the replay granularity after a crash — it does not set durability-barrier boundaries (payload, marker-directory, and commit barriers run once per touched shard). Smaller windows shrink replay work; larger windows shrink per-record marker overhead. A shard writer-gate hold grows with that shard's share of the request, never the request's full span. |
```

**Verify:** no build needed; the docs render correctly and no source or
documentation sentence still claims request-wide gate holds:

```bash
rtk rg -n "request-level commit batching|held request-wide|holds every touched|whole request|publish wave|request-wide merged flush|barriers are per request|Wave 1|stage wave" AGENTS.md docs/concepts/concurrency.md docs/getting-started/configuration.md docs/reference/changelog.md src/db/slotcask.c src/db/slotcask.h
```

The command must print no matches. Historical plans are deliberately
excluded from this live-documentation check.

### Task 6 — Gates, sanitizers, measurement handoff

1. Full suite, fresh:

   ```bash
   rtk proxy env SKIP_TESTS=1 ./build.sh
   rtk proxy ./build/bin/shard-db-test run-all
   ```

2. Sanitizer gate (repo standing rule — three consecutive fresh runs
   each, no `halt_on_error`, no suppressions, no `--jobs`):

   ```bash
   rtk proxy env BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
   rtk proxy ./build/bin/shard-db-test run-all
   rtk proxy ./build/bin/shard-db-test run-all
   rtk proxy ./build/bin/shard-db-test run-all
   rtk proxy env BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
   rtk proxy env TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
   rtk proxy env TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
   rtk proxy env TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
   ```

   TSan is the primary reviewer of this change (gates held across
   pipeline steps on a new thread interleaving). Any finding:
   root-cause and fix, then rerun the full three-run gate.

3. **Measurement is run by the human, never the executor** (repo rule:
   "The user runs benches; do not run them to validate perf"). The
   executor hands back the branch with these commands for the user:

   ```bash
   # Repeat each command five times on the same idle, disk-backed host.
   rtk proxy env SHARD_BENCH_COUNT=1000000 SHARD_BENCH_SPLITS=8 ./build/bin/shard-db-bench run bench-kv
   rtk proxy env SHARD_BENCH_TOTAL=1000000 SHARD_BENCH_CHUNK=200000 SHARD_BENCH_SPLITS=8 ./build/bin/shard-db-bench run bench-kv-parallel
   rtk proxy env SHARD_BENCH_TOTAL=1000000 SHARD_BENCH_CHUNK=200000 SHARD_BENCH_SPLITS=128 ./build/bin/shard-db-bench run bench-kv-parallel
   ```

   Record all five throughput results for every shape and compare medians
   with five fresh runs of the 2026-09-07 baseline on the same host.
   Acceptance targets: splits=8 five-connection aggregate median ≥ 1.2 M
   rows/s; single-connection JSON and CSV medians each no worse than 5%
   below baseline; splits=128 five-connection median no worse than 5%
   below baseline. If any target is missed, report and stop; do not tune
   further without a new approved plan step.

## Execution rules (bind the whole run)

- **Branch:** work on a fresh branch `perf/b1-writer-gate-pipelining`
  created off `main`. Do not commit: this repo's execution mode is
  **leave work uncommitted** — the reviewing agent + human review the
  raw `git diff` first.
- **Order:** do Tasks 1–6 in order; never skip or reorder. A task is
  done only when its verify commands have been run and their real
  output pasted into the task log.
- **Build/test commands:** every shell command uses RTK. Build with
  `rtk proxy env SKIP_TESTS=1 ./build.sh`; run the full suite with
  `rtk proxy ./build/bin/shard-db-test run-all`; run a single case with
  `rtk proxy ./build/bin/shard-db-test run <name>`.
- **Anchors:** every edit locates its site by the quoted anchor text.
  If a quoted anchor isn't found exactly, write `PLAN_NOTES.md`
  describing the mismatch and halt the entire execution run immediately
  — do not guess, reinterpret, or continue to any further task, even an
  unrelated one. Resuming requires the human (or the planning model,
  re-engaged) to read `PLAN_NOTES.md` and hand back a patched or fresh
  plan — execution never resumes on its own initiative.
- **Unknowns:** if you hit a decision this plan doesn't cover, stop and
  ask — do not improvise.
- **Benches:** the executor must never run `./build/bin/shard-db-bench`
  or the `bench/` scripts. Measurement belongs to the human (Task 6).
- **Honesty:** never claim a step passed without pasting the real
  command output; never weaken a test to make a failure disappear; a
  test that fails and passes on rerun is a confirmed bug until
  root-caused.

## Out of scope (parked)

- Durable per-shard marker sequencing (D0) — required only for the
  parked async-convergence design, not for B1 (see G2).
- Marker aggregation (window merge) — ~2% at window=16384; revisit only
  if B1's measurement shifts it.
- Residual wire-read/collect_inputs CPU — diffuse per the 2026-09-07
  profile; no concentrated target.
- Fairness mechanisms for writer gates (FIFO ticket locks etc.) — no
  starvation mechanism on base, none added.
