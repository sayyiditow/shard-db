# B2 — Per-shard pipeline tasks + coalesced durability syncs

Date: 2026-09-09
Status: **executed** (Tasks 1–4 + docs; work uncommitted on
`perf/b1-writer-gate-pipelining`; see Execution record below).
Follows: 2026-09-07-b1-writer-gate-per-shard-pipelining.md (B1: per-shard
pipelines — admission redesign that **failed its measurement gate**; the B1
tree is parked uncommitted on `perf/b1-writer-gate-pipelining` and B2
builds directly on it).

## Execution record (2026-09-09)

| Task | Proof |
|---|---|
| 1 (scenario 9 rework, red-first) | red exactly as predicted: `not ok 96 — exactly one of A's pipelines completed while the other is parked` (both sides false under caller-serial dispatch); 123 passed / 1 failed |
| 2 (pool-task dispatch) | scenario 9 green; focused case 124/0; full suite 13086/3 — the 3 failures are precisely the anticipated crash-test assertions Task 3 reworks; no other case affected |
| 3 (determinism rework) | focused cases green; full suite ×2 = 13091/0 across 448; crash set green; `test-parallel-index-integrity` ×4 green |
| 4 (coalesced syncs) | full suite ×2 = 13093/0 across 448; crash set green; integrity ×4 green |
| docs (D.1/D.2 + slotcask.h + AGENTS.md) | stale-wording sweep: no matches for the B1 caller-serial forms in live docs |
| final | rebuild clean (no warnings), full suite 13093/0 across 448 |

Execution amendments (all recorded in PLAN_NOTES.md):

1. **Scenario 9 IO-pool gap (plan-uncovered fact).** The test runner's
   process DB deliberately starts no thread pools, so `parallel_for_io`
   ran both A pipelines inline and the overlap proof degenerated to
   caller-serial even after Task 2. Scenario 9 now brackets itself with
   `parallel_io_pool_init(0)` / `parallel_io_pool_shutdown()`, mirroring
   scenario 7's CPU-pool pattern.
2. **Edit 3.1/3.2 compile fix.** The plan's replacement blocks compared
   `ins[failed_idx]` (a struct) to an int; applied with `.rc`.
3. **`test_durability_sync_failures` #25 reworked (behavior change).**
   The single-record fast path shares the bulk phase helpers, so its
   per-window payload barrier flows through the B2 coalescer: after a
   successful insert the segment's `dirty` flag is now legitimately 0
   (the barrier's own fdatasync covered it; the old code left the flag
   set and the sweeper re-synced clean data). The test's old assertion
   ("insert marks its cached segment dirty") encoded the artifact the
   coalescer removes; it now asserts the flag is clean after the insert
   and proves the sweep contract via a manual
   `durability_mark_dirty` + `durability_flush_dirty` round-trip.
4. **Hygiene.** Added the matching `pthread_mutex_destroy`/
   `pthread_cond_destroy` for the request's dir-sync state in
   `slotcask_bulk_request_end` (plan only specified init); updated two
   now-false B1 comments (gate-ordering rationale in
   `slotcask_bulk_request_begin`, the pipeline-flushes banner) and
   `slotcask.h`'s deferred-request comment to the concurrent model.

Task 5 (human-run) remains: ASan ×3 + TSan ×3 fresh full-suite gates,
then the 5+5 bench measurement against the acceptance targets.

## Measurement verdict (2026-09-09): MIXED — splits=8 pass/miss, splits=128 FAIL

Human-run benches on the executed tree (single runs so far, not yet 5+5
medians):

| median, M rows/s | waves baseline (splits=8) | B2 (splits=8) | B2 (splits=128) |
|---|---|---|---|
| single JSON | 1.02 | **1.33** (+30%) | 0.62 |
| single CSV | 1.30 | **1.75** (+35%) | 0.74 |
| parallel JSON | 0.70 | **0.87** (+24%) | 0.08 |
| parallel CSV | 0.77 | **1.04** (+35%) | 0.08 |

- **splits=8 single**: beats the baseline and the "within 5%" gate —
  per-shard pipelines overlap stage→P→publish→K across shards and the
  D2 coalescer restored request-wide sync dedup. Gate PASS.
- **splits=8 parallel 5-conn**: beats the wave baseline on both formats
  but misses the ≥1.2 M rows/s target (0.87 / 1.04). Gate MISS. The
  metrics show real concurrency (≈3.3 writer gates held on average) and
  a 5–7× durable-IO multiplication onto the object's fixed 16 stream
  files (`slotcask_streams_for_nproc`, independent of splits) plus
  whole-file K msyncs per shard — cross-request group-commit territory,
  not a B2 defect.
- **splits=128**: collapses for every case (single 0.62/0.74; parallel
  0.08/0.08, 12 s walls). Root cause is arithmetic, not a race: streams
  stay at 16 while pipelines scale with splits, so per-request
  pipelines/stream-file oversubscription goes 0.5× (splits=8) → 8×
  (single, 128) → 40× (parallel, 640 tasks). The coalescer's skip path
  rarely fires under continuous re-marking, so nearly every (task,
  file) pair issues a real fdatasync; K issues one msync per shard
  (128 vs 8); markers double (128 vs 64 windows; 640 vs 80 parallel).
  Aggregate barrier counters (kf_msync 29.7 s over a 1.6 s wall ≈ 18
  durable ops permanently in flight) show device-queue saturation.
  7,813 records/shard is also 10× below the documented 78K–200K sweet
  spot. Gate FAIL — splits=128 is outside the supported operating
  envelope for 1M-row bulk ingest; per-shard pipelining trades
  wide-split efficiency for cross-shard overlap.
- No waves-baseline numbers exist at splits=128 (the controlled
  baseline was splits=8 only), so the 128 comparison is against
  baseline-at-8 and against B2-at-8, not against waves-at-128.

**Disposition.** B2 stands at splits=8: strictly faster than baseline
everywhere measured, parallel still short of the stretch target. Next
levers (new plan required, none executed): cross-request group commit
for the P barrier (epoch-batch concurrent requests' dirty pages per
file, one fdatasync per file per epoch), range-scoped K barrier (msync
the window's kf byte range, not the whole file), and optionally a
streams knob at create-object. No tuning was applied post-measurement.

## Measured context (why B2 exists)

B1's controlled bench (same host/dir/commands, 5+5 medians, 2026-09-09):

| median, M rows/s | baseline (waves) | B1 (caller-serial pipelines) |
|---|---|---|
| single JSON | 1.02 | 0.44 (−57%) |
| single CSV | 1.30 | 0.45 (−65%) |
| parallel JSON | 0.70 | 0.43 (−39%) |
| parallel CSV | 0.77 | 0.50 (−35%) |

The autopsy (commit-phase metrics, single-conn JSON, per 1M rows) attributed
every millisecond to two mechanisms — **no bug**:

| phase | wave wall | B1 wall | cause |
|---|---|---|---|
| stage | 138 ms (8-way parallel) | 366 ms | lost shard-wave parallelism |
| P payload sync | 69 ms (one deduped pass) | 246 ms | dedup loss (64 vs 8 fdatasyncs) + serial |
| publish (marker fsyncs ×64) | ~50 ms wall | 383 ms | lost shard-wave parallelism |
| A/T sync | 67 ms (one deduped pass) | 237 ms | dedup loss + serial |
| K msync | ~66–90 ms wall | 115 ms | lost shard-wave parallelism |
| marker clear | 4.5 ms (1 dir fsync) | 44 ms (8) | dedup loss |

Phase totals did not inflate beyond the documented dedup loss
(segment_sync 136→483 ms = 64 vs 8 fdatasyncs; kf_msync totals dropped) —
the regression is designed serialization, not extra work. The parallel
case adds cross-request double-flushing of shared stream files (par
segment_sync totals 3.43 s aggregate vs 0.28 s baseline): each request's
fdatasync also flushes concurrent requests' dirty pages.

## Design

### D1 — Dispatch: one IO-pool task per shard pipeline

B1 runs the touched shards' pipelines sequentially on the caller thread
(the rev-3 correction of rev 1's pool-task model). B2 restores execution
parallelism: the request dispatches **one IO-pool task per touched shard**,
each running `slotcask_bulk_shard_pipeline(req, in)` unchanged (gate →
replay+stage → P → publish → M fsync → finalize → commit → fold →
terminal release → gate release), then joins.

**The invariant correction that makes this sound.** B1's plan (and the
2026-09-08 review) required "one gate per request". That invariant is
over-strong. Deadlock freedom requires only:

- **G1′ — no thread holds a writer gate while waiting on another writer
  gate.** A pipeline task holds exactly its own shard's gate
  start-to-finish and never waits on another gate while holding it.
  Cross-request, a task waiting for shard s's gate holds nothing.
  Wait-for-graph edges go task → gate only; a gate's holder is always a
  running task; no cycles.

The hold-and-wait property was already true of every existing gate user
(wave workers per phase, single-record writers, pre-grow); B1's
caller-loop achieved it trivially; B2's pool tasks achieve it per task.
Scenario 8's TEST_BUILD gate-hold counter asserts ≤1 gate held **per
thread** — unchanged and still enforceable (each task holds exactly one).

**Pool capacity / starvation argument.** With W = `parallel_threads() * 4`
IO-pool workers (min 2) and at most `num_shards` (≤4096, in practice ≤64)
gates held simultaneously, deadlock requires all W workers blocked on
gates whose holders are not running — impossible, because a gate is only
held while its task runs on a worker. Latency starvation (tasks queued
behind other subsystems' pool work) does not deadlock: a queued task holds
no gate. The B1-era circular wait (standalone gate-blocked pre-grow tasks
starving a running request) cannot recur: pre-grow stays folded into
stage, inside the gated task itself.

**Nested `parallel_for_io`.** A pipeline task runs on a pool worker, so
its internal `bulk_seg_apply_and_sync` per-file fan-out runs inline
(serial per file) per `parallel.c`'s `t_in_pool_worker` rule. Cost:
each shard's segment syncs serialize across its stream files — but the
eight tasks run concurrently, so aggregate overlap is restored at the
task level. Single-conn: the P/A/T walls converge toward the wave's
deduped-pass walls once D2's coalescing lands. The nested caller
(test-request-flush-batching scenario 7a) keeps today's fully-inline
behavior unchanged.

**What stays sequential.** Within one shard's pipeline, the step order is
unchanged (replay+stage → P → publish → M fsync → finalize → commit →
clear). Ascending dispatch (B1's G3) is **replaced**: tasks race freely;
per-shard mutual exclusion is enforced by the gate alone. G2/G4/G5 are
untouched. The durability invariants I1–I5 (docs/concepts/concurrency.md)
are per-window properties of the phase helpers and are unchanged.

### D2 — Coalesced durability syncs (the dedup loss)

D1 alone restores the stage/publish/K fan-out. The remaining B1 costs are
the dedup losses: per-shard fdatasyncs of the same shared stream files
(8× calls, sequential within a request; concurrent across requests,
double-flushing other requests' dirty pages) and per-shard kf-dir fsyncs.

**Per-file coalescing protocol** (for the segment groups; the kf files are
per-shard and need nothing). The segcache entry already tracks dirtiness
(`dirty`/`dirty_since_ms`, maintained by `durability_mark_dirty` at the
stage write sites — slotcask.c:5204). B2 adds per-entry sync state:

```
lock(sync_mu)
if (!dirty)                      → skip (provably clean: a completed sync
                                   already flushed everything up to its
                                   claim point, and no writer re-marked)
while (in_flight) wait(sync_cv)  → drain the in-flight sync, re-check
claim: dirty = 0 ; in_flight = 1        [claim BEFORE the IO]
unlock ; msync(range) + fdatasync(fd)
lock ; in_flight = 0 ; broadcast ; unlock
on IO failure: dirty = 1 (restore) and return -1
```

Claim-before-IO is the durability.c `durability_flush_dirty` order: a
writer marking during the IO re-sets `dirty = 1` (atomic store wins over
the earlier claim), so the next sync covers it. Skipping requires having
*observed a successful in-flight sync complete* with dirty still 0 — a
failed in-flight sync restores `dirty = 1`, so no skipper can proceed on
unconfirmed bytes. Uncached handles (`h.slot < 0`) fall back to plain
sync. `bulk_seg_sync_one_group`'s own flag stores (`store=1`) mark dirty
before claiming, so their bytes are always covered by their own sync.

**Marker-dir fsync coalescing.** `SlotcaskBulkRequest` gains
`dir_dirty`/`dir_sync` state; `bulk_publish_one_kf_window` (after a
successful publish) and the clear path's unlinks set it;
`slotcask_bulk_shard_flush_marker_dir` and the per-shard clear's dir
fsync coalesce on it (same protocol, without the per-file entry).
Expected: 8 dir fsyncs → 1–2 per request, and cross-request overlap.

**K barrier:** untouched — kf files are per-shard, no sharing, no dedup
loss (already parallel across tasks under D1).

### D3 — Test determinism under concurrent pipelines

Concurrent dispatch makes per-shard arrival order racy. Three existing
scenarios asserted order-dependent outcomes and are reworked to
order-independent ones (see Tasks); scenario 8 (gate counter) and
scenario 12 (temp sweep) are order-independent already. The multi-shard
crash test's "exactly one shard absent" three-state window no longer
exists under concurrency; its assertions become concurrency-honest while
still proving marker-replay recovery (see Task 3).

## Call-site / consumer inventory

- `slotcask_bulk_request_execute` — dispatch loop replacement; signature
  unchanged; callers unchanged (query_bulk.c `bulk_execute_prepared`,
  tests — same inventory as B1 §Call-sites).
- `slotcask_bulk_shard_pipeline` — unchanged; becomes a pool-task body.
- `bulk_seg_sync_one_group` — coalescing protocol added (only caller of
  the per-file sync; `bulk_seg_apply_and_sync` unchanged).
- `SegCacheEntry` (src/db/shard_db_internal.h) — +`sync_mu`, `sync_cv`,
  `sync_in_flight`; init at `segcache_init`, destroy at
  `segcache_shutdown` (entries recycled from a fixed array; pinned
  entries cannot be evicted, so the state is stable while a handle is
  held).
- `SlotcaskBulkRequest` — +dir-fsync coalescing fields (file-static
  struct).
- Scenarios 9, 10, 11 of test-request-flush-batching.c and
  `test_durability_bulk_pipeline_multishard_crash` — determinism rework.
- Docs: docs/concepts/concurrency.md (execution shape + a concurrency
  note on coalesced syncs), docs/reference/changelog.md Unreleased.
- `slotcask.h` deferred-request comment — wording already says
  "pipelines the touched shards" (B1); B2 appends "concurrently".

## Tasks

Execution rules from the B1 plan apply verbatim (branch
`perf/b1-writer-gate-pipelining`, work uncommitted, tasks in order,
PLAN_NOTES halt on anchor mismatch, stop-and-ask on uncovered decisions,
RTK-prefixed commands, executor never runs benches, sanitizer gate in
Task 5). Build with `rtk proxy env SKIP_TESTS=1 ./build.sh`; focused case
`rtk proxy ./build/bin/shard-db-test run <name>`; full suite
`rtk proxy ./build/bin/shard-db-test run-all`.

### Task 1 — Test-first: scenario 9 rework (intra-request overlap)

Rewrite scenario 9 so it deterministically proves **intra-request**
overlap: request A touches shards {0,1}; exactly one of A's two pipeline
tasks parks at the shard-published seam (occurrence 1), the other completes;
request B on shard 2 (disjoint) completes; after release both A shards
complete. On the B1 caller-serial tree the "exactly one" assertion fails
(ZERO of A's shards complete while the first parks) — red for the
dispatch-parallelism reason.

**Edit 1.1** (apply first) — test file, add the two-input driver next to
`rf_req_thread`. Anchor:

```c
static void *rf_req_thread(void *raw) {
    RfReqThr *a = raw;
    SlotcaskBulkOpts opts;
    rf_fill_opts(&opts, NULL);
    a->rc = rf_run_request(a->w, a->b, &opts);
    a->done = 1;
    return NULL;
}
```

insert immediately after:

```c
/* Drives one execute() over two FIXED shard inputs (no bucketing), so a
   test can pin exactly which shards a request touches. Caller owns the
   inputs for the request's whole life. */
struct RfTwoShardArgs {
    RfDb *w;
    SlotcaskBulkShardInput *ins;   /* 2 inputs */
    int done;
    int rc;
};

static void *rf_two_shard_req_thread(void *raw) {
    struct RfTwoShardArgs *a = raw;
    a->rc = slotcask_bulk_request_execute(&a->w->db, a->ins, 2);
    a->done = 1;
    return NULL;
}
```

**Edit 1.2** — test file, replace scenario 9's entire block (from its
comment through its closing brace before scenario 10's comment):

```c
    /* ── Scenario 9 (B2): intra-request overlap — A touches shards
     * {0,1}; exactly ONE of its two pipelines parks at the seam while
     * the other completes; B (disjoint shard 2) completes throughout.
     * Red on B1's caller-serial dispatch: A's second pipeline never
     * starts while the first is parked, so zero shards complete. ── */
    {
        static SlotcaskBulkShardInput ains[2];
        static RfBatch b0, b1;
        static char k0[16][24], v0[16][24];
        static SlotcaskBulkRec r0[16], p0[16];
        static char k1[16][24], v1[16][24];
        static SlotcaskBulkRec r1[16], p1[16];
        b0.keys = k0; b0.vals = v0; b0.recs = r0; b0.perm = p0;
        b1.keys = k1; b1.vals = v1; b1.recs = r1; b1.perm = p1;
        rf_batch_fill(&b0, 21, 16, "v21", 0);    /* shard 0 */
        rf_batch_fill(&b1, 23, 16, "v24", 1);    /* shard 1 */
        static RfBatch bb;
        static char keysb[16][24], valsb[16][24];
        static SlotcaskBulkRec recsb[16];
        static SlotcaskBulkRec permb[16];
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&bb, 22, 16, "v22", 2);    /* shard 2 only */

        SlotcaskBulkOpts oa;
        rf_fill_opts(&oa, NULL);
        memset(ains, 0, sizeof(ains));
        ains[0].kf_shard_id = 0; ains[0].recs = r0; ains[0].nrecs = b0.n;
        ains[0].kind = SLOTCASK_BULK_INPUT_UPSERT; ains[0].opts.upsert = oa;
        ains[1].kf_shard_id = 1; ains[1].recs = r1; ains[1].nrecs = b1.n;
        ains[1].kind = SLOTCASK_BULK_INPUT_UPSERT; ains[1].opts.upsert = oa;

        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_SHARD_PUBLISHED;
        g_shard_test_pause_occurrence = 1;

        static struct RfTwoShardArgs ta;
        ta.w = &w; ta.ins = ains; ta.done = 0; ta.rc = 0;
        pthread_t tha;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_two_shard_req_thread,
                                     &ta), 0,
                      "spawn A (shards 0+1)");
        rf_wait_pause_hit();
        ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                    "one of A's pipelines parked at its seam");

        RfReqThr tb = { .w = &w, .b = &bb, .done = 0, .rc = 0 };
        pthread_t thb;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_req_thread, &tb), 0,
                      "spawn B (shard 2)");
        ASSERT_TRUE(rf_wait_flag(&tb.done, 5000),
                    "B completes shard 2 while A is paused mid-pipeline");
        ASSERT_EQ_INT(tb.rc, 0, "B rc 0");
        ASSERT_TRUE(rf_record_visible(&w, bb.keys[0], "v22-0000"),
                    "B's record readable while A parked");

        /* Intra-request overlap: exactly one of A's shards committed
           while the other is parked. */
        int s0v = rf_record_visible(&w, k0[0], "v21-0000");
        int s1v = rf_record_visible(&w, k1[0], "v24-0000");
        ASSERT_TRUE(s0v != s1v,
                    "exactly one of A's pipelines completed while the "
                    "other is parked (0 on caller-serial dispatch)");

        rf_release_pause();
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        ASSERT_EQ_INT(ta.rc, 0, "A converges after release");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "all markers cleared");
        ASSERT_TRUE(rf_record_visible(&w, k0[0], "v21-0000") &&
                    rf_record_visible(&w, k1[0], "v24-0000"),
                    "both A shards visible after join");
    }
```

**Verify (red expected on B1):** build; run the focused case. Expected
red: `exactly one of A's pipelines completed…` fails (both sides false —
shard 0 parked, shard 1 never started). Everything else green. Paste both
outputs, then `run-all` (expected failures: scenarios 8 and 9-reworked
only — 10/11/crash-test determinism rework lands in Task 3; if 10/11 or
the crash test flake here, that is the expected concurrency exposure and
Task 3 fixes it — note the occurrences and continue to Task 3 without a
halt).

### Task 2 — Dispatch: pool-task pipelines

**Edit 2.1** — slotcask.c, add the task body next to the pipeline
function. Anchor:

```c
int slotcask_bulk_request_execute(SlotcaskDb *db,
                                  SlotcaskBulkShardInput *inputs,
                                  size_t ninputs) {
```

insert immediately before:

```c
typedef struct {
    SlotcaskBulkRequest    *req;
    SlotcaskBulkShardInput *in;
} BulkPipelineTask;

static void *bulk_pipeline_task(void *raw) {
    BulkPipelineTask *t = raw;
    slotcask_bulk_shard_pipeline(t->req, t->in);
    return NULL;
}
```

**Edit 2.2** — slotcask.c, replace the caller loop. Anchor:

```c
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
```

replace with:

```c
    /* Pipelined execution (B2): each touched shard's pipeline runs as
       one IO-pool task, so a request keeps the waves' cross-shard
       parallelism while per-shard gates admit concurrent requests
       shard-by-shard. Deadlock-free (G1'): a task holds exactly its own
       writer gate start-to-finish and never waits on another gate while
       holding it; cross-request, a task waiting for a gate holds
       nothing. Pool capacity: gates are held only by running tasks, so
       with workers ≥ distinct shards a holder always has a worker; a
       queued task holds no gate and cannot deadlock the holder. A
       request issued from inside a pool task runs its pipelines inline
       (parallel_for_io's nesting rule) — scenario 7a unchanged. */
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
    BulkPipelineTask *tasks = malloc(ninputs * sizeof(*tasks));
    if (!tasks) {
        free(order);
        slotcask_bulk_request_end(req);
        errno = ENOMEM;
        return -1;
    }
    for (size_t i = 0; i < ninputs; i++) {
        tasks[i].req = req;
        tasks[i].in = order[i];
    }
    parallel_for_io(bulk_pipeline_task, tasks, (int)ninputs,
                    sizeof(*tasks));
    free(tasks);
    free(order);
```

**Verify (green expected):** build; scenario 9 green (paste); focused
case green; full suite green except any 10/11/crash-test determinism
flakes noted for Task 3. If any OTHER case fails, halt and report.

### Task 3 — Determinism rework (scenarios 10, 11, multi-shard crash test)

Concurrent dispatch makes per-shard arrival order racy; the isolation
*properties* are unchanged, so the assertions become order-independent.

**Edit 3.1** — test file, scenario 10's outcome asserts. Anchor:

```c
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
```

replace with:

```c
        /* Under concurrent pipelines either shard may fail first; the
           isolation property is order-independent: exactly one input
           hard-failed, the sibling is clean and committed. */
        int failed_idx = ins[0].rc == -1 ? 0 : 1;
        ASSERT_TRUE(ins[0].rc == -1 || ins[1].rc == -1,
                    "exactly one shard hard-failed on P");
        ASSERT_EQ_INT(ins[failed_idx], -1, "failed shard rc");
        ASSERT_EQ_INT(ins[1 - failed_idx], 0, "sibling shard rc");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "no markers retained");
        ASSERT_EQ_INT(failed_idx == 0 ? deg0 : deg1, 0,
                      "failed shard not degraded (pre-M)");
        ASSERT_EQ_INT(failed_idx == 0 ? deg1 : deg0, 0,
                      "sibling not degraded");
        RfBatch *fb = failed_idx == 0 ? &b0 : &b1;
        RfBatch *sb = failed_idx == 0 ? &b1 : &b0;
        SlotcaskBulkRec *fr = failed_idx == 0 ? r0 : r1;
        SlotcaskBulkRec *sr = failed_idx == 0 ? r1 : r0;
        int fbad = 0, sok = 1;
        for (size_t i = 0; i < fb->n; i++) fbad |= fr[i].status != -1;
        for (size_t i = 0; i < sb->n; i++) sok &= sr[i].status == 0;
        ASSERT_TRUE(fbad == 0, "failed shard records all -1");
        ASSERT_TRUE(sok, "sibling records all 0");
        ASSERT_TRUE(rf_record_visible(&w, sb->keys[0], sb->vals[0]),
                    "sibling record visible after sibling P failure");
```

**Edit 3.2** — test file, scenario 11's outcome asserts. Anchor:

```c
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
```

replace with:

```c
        /* Order-independent: exactly one shard retained (-2 + degraded),
           the sibling clean and committed; both shards' records visible
           (K applied before the failed clear). */
        int ret_idx = ins[0].rc == -2 ? 0 : 1;
        ASSERT_TRUE(ins[0].rc == -2 || ins[1].rc == -2,
                    "exactly one shard retained on C failure");
        ASSERT_EQ_INT(ins[ret_idx], -2, "retained shard rc");
        ASSERT_EQ_INT(ret_idx == 0 ? deg0 : deg1, 1, "retained degraded");
        ASSERT_EQ_INT(ins[1 - ret_idx], 0, "sibling rc 0");
        ASSERT_EQ_INT(ret_idx == 0 ? deg1 : deg0, 0, "sibling clean");
        ASSERT_TRUE(rf_record_visible(&w, b0.keys[0], "vC-0000") &&
                    rf_record_visible(&w, b1.keys[0], "vD-0000"),
                    "both shards' records visible");
        /* Golden follow-up single writes on BOTH shards: whichever gate
           replays (or finds already-applied) leaves the state clean. */
        for (int pass = 0; pass < 2; pass++) {
            char fk[24], fv[24];
            snprintf(fk, sizeof(fk), "rf-follow-11-%d", pass);
            int attempt = 0;
            while (rf_shard_of(fk) != pass) {
                snprintf(fk, sizeof(fk), "rf-follow-11-%d-%d", pass,
                         attempt++);
            }
            snprintf(fv, sizeof(fv), "follow11");
            SlotcaskUpsertOpts so;
            memset(&so, 0, sizeof(so));
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, pass, fk,
                                                     strlen(fk), fv,
                                                     strlen(fv), &so, NULL),
                          0, "follow-up write converges");
        }
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "markers clean at end");
```

**Edit 3.3** — test_durability_ordering.c, crash-test concurrency
adjustments. Under concurrent dispatch shard 0 and shard 2 race shard 1's
parked pipeline, so pre-crash counts are racy; the deterministic
assertions are shard 1's marker existing at the pause and the exact
post-recovery state. Anchor (inside
`test_durability_bulk_pipeline_multishard_crash`):

```c
    ASSERT_EQ_INT(request_count(&env, object), 2,
                  "only cleared shard 0 is visible before crash");
    char mpaths_before[8][PATH_MAX];
    ASSERT_EQ_INT(scan_kf_markers(saved_db_root, object, mpaths_before, 8), 1,
                  "shard 1 has exactly one durable marker");
```

replace with:

```c
    /* Concurrent pipelines: shard 2 may have committed (or not) before
       shard 1 parked, so the visible count is 0–4; shard 1's durable
       marker is the deterministic fact. */
    {
        int mid = request_count(&env, object);
        ASSERT_TRUE(mid >= 0 && mid <= 4 && mid % 2 == 0,
                    "at-pause count is committed shards only "
                    "(shard 1 parked pre-finalize)");
    }
    char mpaths_before[8][PATH_MAX];
    ASSERT_TRUE(scan_kf_markers(saved_db_root, object, mpaths_before, 8) >= 1,
                  "shard 1 has at least its durable marker at the pause");
```

Anchor:

```c
        ASSERT_EQ_INT(request_count(&env, object), 4,
                      "shards 0 and 1 present; untouched shard 2 absent");
```

replace with:

```c
        ASSERT_EQ_INT(request_count(&env, object), 6,
                      "every record present after replay of every "
                      "unresolved marker");
```

Anchor:

```c
            ASSERT_EQ_INT(tu_parse_count(resp), 0,
                          "later shard remained untouched across crash");
```

replace with:

```c
            ASSERT_EQ_INT(tu_parse_count(resp), 2,
                          "shard 2 either committed pre-crash or replayed "
                          "from its own marker — exactly its 2 rows");
```

**Verify:** build; focused case green ×2 consecutive (paste); full suite
green ×2 consecutive (paste totals); `test-parallel-index-integrity`
×4 green; crash set green (`test-marker-v2`,
`test-durability-bulk-pipeline-multishard-crash`,
`test-durability-bulk-window-boundary`,
`test-durability-bulk-window-boundary-mixed-indexes`,
`test-durability-bulk-window-prepared-recovers`,
`test-durability-bulk-window-applied-recovers`).

### Task 4 — Coalesced durability syncs (the dedup loss)

**Edit 4.1** — src/db/shard_db_internal.h, `SegCacheEntry` fields.
Anchor:

```c
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
```

replace with:

```c
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
    /* B2: coalesced bulk sync state. dirty is the durability truth
       (claim-before-IO protocol); sync_mu/sync_cv serialize concurrent
       bulk syncs of one file and let later syncers skip once a
       completed sync provably covered their bytes. */
    pthread_mutex_t sync_mu;
    pthread_cond_t  sync_cv;
    int             sync_in_flight;
```

**Edit 4.2** — slotcask.c, initialize at install. Anchor:

```c
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
```

replace with:

```c
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    pthread_mutex_init(&e->sync_mu, NULL);
    pthread_cond_init(&e->sync_cv, NULL);
    e->sync_in_flight = 0;
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
```

(This is the install path under `g_segcache_lock`; a freshly installed
entry is not yet published (`used` set here), so initializing the mutex
here is race-free.)

**Edit 4.3** — slotcask.c, destroy at shutdown. Anchor:

```c
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
```

replace with:

```c
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
            pthread_mutex_destroy(&e->sync_mu);
            pthread_cond_destroy(&e->sync_cv);
```

**Edit 4.4** — slotcask.c, the coalescing protocol in
`bulk_seg_sync_one_group`. Anchor (the entire function):

```c
static int bulk_seg_sync_one_group(SlotcaskDb *db, const SegLoc *locs,
                                   size_t n, int store, uint8_t flag) {
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && locs[j].sid == locs[i].sid && locs[j].fid == locs[i].fid)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, locs[i].sid, locs[i].fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++) {
            if (store)
                __atomic_store_n(&h.map[locs[k].off + 18], flag,
                                 __ATOMIC_RELEASE);
        }
        size_t lo = locs[i].off, hi = locs[j - 1].off + 1;
        if (durability_msync_range(h.map, lo, hi - lo) != 0 ||
            fdatasync(h.fd) != 0) {
            segcache_release(&h);
            return -1;
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}
```

replace with:

```c
static int bulk_seg_sync_one_group(SlotcaskDb *db, const SegLoc *locs,
                                   size_t n, int store, uint8_t flag) {
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && locs[j].sid == locs[i].sid && locs[j].fid == locs[i].fid)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, locs[i].sid, locs[i].fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        SegCacheEntry *e = h.slot >= 0 ? &g_segcache[h.slot] : NULL;
        for (size_t k = i; k < j; k++) {
            if (store)
                __atomic_store_n(&h.map[locs[k].off + 18], flag,
                                 __ATOMIC_RELEASE);
        }
        if (store && e)
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
        /* B2 coalescing: concurrent pipelines (and concurrent requests)
           sync the same shared stream files. Serialize per file and skip
           when a completed sync provably covered our bytes. The claim
           (dirty → 0) happens BEFORE the IO so a writer marking during
           the sync re-sets dirty=1 and gets its own sync; a failed IO
           restores dirty=1, so no skipper proceeds on unconfirmed
           bytes. Our own flag stores mark dirty before the claim, so a
           group never skips its own bytes. */
        int skip = 0;
        if (e) {
            pthread_mutex_lock(&e->sync_mu);
            if (!atomic_load_explicit(&e->dirty, memory_order_acquire)) {
                skip = 1;
            } else {
                while (e->sync_in_flight)
                    pthread_cond_wait(&e->sync_cv, &e->sync_mu);
                if (!atomic_load_explicit(&e->dirty,
                                          memory_order_acquire))
                    skip = 1;
                else {
                    e->sync_in_flight = 1;
                    atomic_store_explicit(&e->dirty, 0,
                                          memory_order_release);
                }
            }
            pthread_mutex_unlock(&e->sync_mu);
        }
        if (!skip) {
            size_t lo = locs[i].off, hi = locs[j - 1].off + 1;
            int rc = durability_msync_range(h.map, lo, hi - lo) != 0
                   ? -1 : (fdatasync(h.fd) != 0 ? -1 : 0);
            if (e) {
                if (rc != 0)
                    atomic_store_explicit(&e->dirty, 1,
                                          memory_order_release);
                pthread_mutex_lock(&e->sync_mu);
                e->sync_in_flight = 0;
                pthread_cond_broadcast(&e->sync_cv);
                pthread_mutex_unlock(&e->sync_mu);
            }
            if (rc != 0) {
                segcache_release(&h);
                return -1;
            }
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}
```

**Correctness notes (reviewer checklist).** (a) Claim-before-IO matches
`durability_flush_dirty`'s ordering: a `durability_mark_dirty` during the
IO re-sets dirty=1 after the claim, forcing a later sync. (b) Skips
require observing in_flight drain with dirty still 0 — i.e. a *succeeded*
sync whose claim preceded our bytes' mark... wait: a skip after drain
means dirty==0, i.e. no writer (including us) has marked since the claim;
our own bytes marked at write time are therefore covered by that
completed sync. (c) Our own flag stores mark dirty before the claim (the
store loop precedes the protocol), so a group never skips its own
bytes. (d) Uncached handles (`h.slot < 0`) skip the protocol and always
sync. (e) Eviction cannot free a pinned entry, so `e` is stable while
the handle is held; `sync_mu` is initialized once at install and
destroyed only at shutdown.

**Edit 4.5** — slotcask.c, marker-dir fsync coalescing. `SlotcaskBulkRequest`
gains state; producers set it; the two fsync sites coalesce. Add to the
struct — anchor:

```c
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
};
```

replace with:

```c
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
    /* B2: marker-dir fsync coalescing — concurrent pipelines publish and
       clear against one shared data/kf dir; the fsync runs once and
       every waiter is covered. */
    pthread_mutex_t dir_sync_mu;
    pthread_cond_t  dir_sync_cv;
    int             dir_dirty;
    int             dir_sync_in_flight;
};
```

Initialize in `slotcask_bulk_request_begin`. Anchor:

```c
    SlotcaskBulkRequest *req = calloc(1, sizeof(*req));
    if (!req) return NULL;
    req->db = db;
```

replace with:

```c
    SlotcaskBulkRequest *req = calloc(1, sizeof(*req));
    if (!req) return NULL;
    pthread_mutex_init(&req->dir_sync_mu, NULL);
    pthread_cond_init(&req->dir_sync_cv, NULL);
    req->db = db;
```

Producers: in `bulk_publish_one_kf_window`, anchor:

```c
        rw->published = 1;
        __atomic_add_fetch(&g_commit_marker_publish_count, 1,
                           __ATOMIC_RELAXED);
        __atomic_add_fetch(&g_commit_windows_total, 1, __ATOMIC_RELAXED);
```

replace with:

```c
        rw->published = 1;
        pthread_mutex_lock(&txn->req->dir_sync_mu);
        txn->req->dir_dirty = 1;
        pthread_mutex_unlock(&txn->req->dir_sync_mu);
        __atomic_add_fetch(&g_commit_marker_publish_count, 1,
                           __ATOMIC_RELAXED);
        __atomic_add_fetch(&g_commit_windows_total, 1, __ATOMIC_RELAXED);
```

And in `slotcask_bulk_shard_flush_commit`'s clear loop, anchor:

```c
        rw->unlink_succeeded = 1;
        any_unlinked = 1;
    }
```

replace with:

```c
        rw->unlink_succeeded = 1;
        any_unlinked = 1;
    }
    if (any_unlinked) {
        pthread_mutex_lock(&req->dir_sync_mu);
        req->dir_dirty = 1;
        pthread_mutex_unlock(&req->dir_sync_mu);
    }
```

Coalesced fsync helper (insert immediately before
`slotcask_bulk_shard_flush_marker_dir`):

```c
/* B2: coalesced fsync(data/kf dir). Concurrent pipelines publish and
   clear against one shared directory; the fsync runs once per dirty
   episode and every concurrent fsync caller is covered by it (claim-
   before-IO: callers arriving during the IO see dir_dirty re-set by the
   publishers/clears themselves and run their own — a no-op on a clean
   dir is cheap). */
static int slotcask_bulk_request_fsync_dir_coalesced(
    SlotcaskBulkRequest *req) {
    pthread_mutex_lock(&req->dir_sync_mu);
    if (!req->dir_dirty) {
        pthread_mutex_unlock(&req->dir_sync_mu);
        return 0;
    }
    while (req->dir_sync_in_flight)
        pthread_cond_wait(&req->dir_sync_cv, &req->dir_sync_mu);
    if (!req->dir_dirty) {
        pthread_mutex_unlock(&req->dir_sync_mu);
        return 0;
    }
    req->dir_sync_in_flight = 1;
    req->dir_dirty = 0;
    pthread_mutex_unlock(&req->dir_sync_mu);
    int rc = fsync_dir(req->kf_dir);
    pthread_mutex_lock(&req->dir_sync_mu);
    req->dir_sync_in_flight = 0;
    if (rc != 0) req->dir_dirty = 1;
    pthread_cond_broadcast(&req->dir_sync_cv);
    pthread_mutex_unlock(&req->dir_sync_mu);
    return rc;
}
```

Then rewire the two fsync sites. In
`slotcask_bulk_shard_flush_marker_dir`, anchor:

```c
    if (!any) return 0;
    int rc = fsync_dir(req->kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) {
        errno = EIO;
        rc = -1;
    }
    return rc;
```

replace with:

```c
    if (!any) return 0;
    int rc = slotcask_bulk_request_fsync_dir_coalesced(req);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) {
        errno = EIO;
        rc = -1;
    }
    return rc;
```

In `slotcask_bulk_shard_flush_commit`'s clear, anchor:

```c
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked && fsync_dir(req->kf_dir) != 0) dir_rc = -1;
```

replace with:

```c
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked &&
        slotcask_bulk_request_fsync_dir_coalesced(req) != 0) dir_rc = -1;
```

**Verify (correctness-first; the perf claim belongs to the human's
bench):** build; focused case ×2 green; full suite ×2 green (paste);
crash set green; `test-parallel-index-integrity` ×4 green. The coalescing
is correctness-critical (P bytes durable before markers — invariant I2),
so the sanitizer gate below is mandatory for it.

### Task 5 — Sanitizer gate + measurement (human-run)

Unchanged from the B1 plan's Task 6: `BUILD_MODE=asan` build + 3 fresh
`run-all`s; `BUILD_MODE=tsan` build +
`TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"` + 3 fresh
`run-all`s (no `halt_on_error`, no suppressions, no `--jobs`). TSan is
the primary reviewer of D1 (concurrent gated pipelines) and D2 (coalesced
syncs).

Then the human measurement — same protocol as B1's (5+5 medians, same
host/dir/commands):

```bash
for i in 1 2 3 4 5; do rtk proxy env SHARD_BENCH_SPLITS=8 SHARD_TEST_TMPDIR=./db \
  SHARD_BENCH_TOTAL=1000000 SHARD_BENCH_CHUNK=200000 \
  ./shard-db-bench run bench-kv-parallel | grep "M rows/s"; done
# plus SHARD_BENCH_SPLITS=128 for the wide-shard regression check
```

Acceptance targets: 5-conn aggregate median ≥ 1.2 M rows/s; single-conn
medians within 5% of the wave baseline (1.02 / 1.30); splits=128 5-conn
median within 5% of baseline. If missed: report and stop — no tuning
without a new approved plan step.

## Documentation updates (fold into Task 5, before the gate)

- **Edit D.1** — docs/concepts/concurrency.md, the bulk section's shape
  list: replace the `gate : writer gate of shard s (ascending across the
  request)` line and the "caller thread pipelines" sentence with the
  concurrent form ("the request dispatches one pipeline task per touched
  shard to the IO pool — they run concurrently; per-shard gates provide
  admission"). Update the bullets: "the kf rwlock stays step-local" holds;
  add "concurrent pipelines coalesce durability syncs per file (a sync
  runs once; overlapping syncers skip once covered)".
- **Edit D.2** — docs/reference/changelog.md Unreleased: replace the B1
  paragraph's "caller processes touched shards in ascending order …
  before releasing the gate" with the concurrent-dispatch wording and a
  note that per-file durability syncs coalesce across concurrent
  pipelines.
- Verify: `rtk rg -n "ascending order on the calling thread|caller thread pipelines" docs/ src/db/slotcask.h AGENTS.md` → no matches.

## Out of scope (parked)

- Marker aggregation / window merge (unchanged from B1).
- Per-shard admission fairness mechanisms.
- Extending the coalescer to the kf K barrier (per-shard files — no
  sharing, nothing to coalesce).
- Any B1-tree disposition beyond what B2 builds on: the B1 pipeline,
  per-shard outcome semantics, seam, tests, and the 4b hotfix are the
  substrate B2 modifies and remain in place.
