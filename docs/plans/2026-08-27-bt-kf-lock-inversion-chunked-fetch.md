# Fix btree↔kfcache lock-order inversion: chunked-resumable indexed fetch (+ kf-held record copy)

## Status

Root cause confirmed against this branch's working tree by direct code read
(agents + manual verification, 2026-08-27) after a live production deadlock.
Fix approved by human 2026-08-27 (with baseline caveat: `test-stress-no-hang`
already red on this branch; other pre-existing failures possible — success
criterion is **no new failures vs a recorded pre-fix baseline**, not an
all-green suite).

## Baseline caveat

This branch (`fix/main-bulk-durability-window`) is mid-review with known
failures. Pre-fix `run-all` output is captured at `/tmp/baseline_runall.log`
(copy under `/tmp`; summarized in PLAN_NOTES.md). Post-fix comparisons are
made against that set. Tests green before must stay green.

## Root cause

Two lock subsystems acquired in opposite orders:

**Writer** — `bulk_commit_one_kf_window` (`src/db/slotcask.c:5211`) takes the
kf shard wrlock (`kf_shard_acquire(...,1)` :5218) and holds it through
M/A/I/K/T/C (release only at `out:` :5251). Phase I
(`bulk_apply_and_sync_indexes_locked` :4981, hook call :4999) applies
secondary indexes under that held wrlock → `v2_insert_apply_commit`
(storage.c:831 `index_parallel`) → `write_index_entry` → `btree_idx_insert`
→ `btree_insert` which blocks on bt **wrlock** (btree.c:2021 mutation mutex,
:1977 `bt_acquire(path,1)`).

**Reader** — four paths block on the kfcache rdlock while holding bt rdlocks:

| # | Site | Mechanism |
|---|---|---|
| 1 | `idx_find_streaming` / `stream_find_cb` (query.c:1534/:1502) | cb runs inside `shard_walk_dispatch`'s per-shard `btree_search/range*` frames holding that idx file's bt rdlock (btree.c:2193 acquire / :2218 cb / :2224 release); on buffer-full, `batch_buf_collect_hash` (:1320) flushes inline (:1353–1361) via `batch_buf_flush_copy` (:1287) → `slotcask_bulk_resolve_and_fetch` → blocking `kfcache_acquire` rdlock (slotcask.c:6080/:6131). Sibling collector threads condvar-park (:1338) each still holding their own shard's bt rdlock |
| 2 | `find_via_composite_key` (query.c:2208) | same BatchFetchBuf inline-flush under `btree_idx_search` fan-out |
| 3 | `wfc_worker` min/max fast path (query_aggregate.c:1634) | owns one open `BtRangeIter` (:1646); every WFC_BATCH=64 hashes calls `flush_wfc_batch` (:1680) → bulk fetch under the iter's bt rdlock |
| 4 | varchar-streaming group-by merge (query_aggregate.c:4507–4613) | k-way merge holds ALL per-shard cursors open across blocking `slotcask_lookup_by_hash` per hash (:4603–4604) |

find: BT-RD → waits KF-RD; insert: KF-WR → waits BT-WR ⇒ AB-BA. Both rwlocks
are writer-preferring, so once a writer queues, new readers queue behind it —
deterministic wedge when an indexed write overlaps a flushing find.

**Secondary defect fixed here (isolation contract):**
`slotcask_bulk_fetch_resolved` revalidates each record's address under a kf
rdlock but releases it (slotcask.c:6148) before any segment byte copy; the
copy phase (`seg_fetch_worker` :5989–6004) holds only segcache handles. The
2026-08-21 durability-window plan states the opposite contract — L214–217:
"Change Kf-based reads so the Kf read handle stays live until the selected
segment record has been checked against its hash/key and copied into
caller-owned memory", root causes #4/#5 at L96–100 — precisely because a
same-key pool-reuse rewrite between validate and copy can tear a record
(header hash matches both versions; klen/vlen mix can even read out of the
intended span). Landed reference implementations (`slotcask_scan_live_kf_*`,
`slotcask_lookup_scan_kf`) already comply; the bulk fetcher does not.

### Why not collect-all-then-fetch

Abandoning streaming for materialize-then-fetch would fix the deadlock but
regresses exactly the workload this executor exists for (guard query.c:7928:
limit-bound offset+limit ≤ 1000 finds whose candidate sets are unbounded —
low-selectivity eq/range post-filtered by siblings). Broad queries would hit
the query-buffer cap error instead of returning rows. Chunked traversal keeps
bounded memory AND preserves early-limit behavior; the release/reopen-under-
contention semantics match what ordered walks and cursor pagination already
ship (docs/plans/2026-08-10-kfcache-btree-lock-inversion.md precedent).

### Isolation semantics statement (approved)

No ACID claim of the branch changes. Window atomicity/durability are writer-
side and untouched byte-for-byte (phase order, marker protocol, forward-replay).
Per-record isolation is unchanged-or-strengthened: a returned row now always
reflects pre- or post-window bytes because resolve+validate+copy complete
under one continuous kf reader. Statement-level reads remain non-repeatable
in kind: today's continuous per-shard sweep already misses rows inserted
behind its walk position, and ordered walks/cursors already resume past
released positions; chunking moves the revisit boundary, never row-content
atomicity.

## Fix tasks

### Task 0a — regression test `test_bt_kf_inversion_stream_find.c`

Single-process direct-call case (runner-process ShardDb). Fixture: indexed
object, enough rows that a limit-bound find fills ≥ 2 chunks through the new
path. Arm TEST_BUILD pause knobs (`g_shard_test_pause_phase = WIN_I phase
index`, occurrence 1), spawn a worker thread performing an indexed insert —
it parks inside `bulk_commit_one_kf_window` after A, holding the kf WRLOCK.
Main thread issues streaming finds (`eq` broad + small limit) repeatedly;
every find must finish within a watchdog window or the case FAILS (this is
the fail-on-base assertion: base wedges in AB-BA). Also assert rows seen
while paused parse to whole pre- or post-version payloads (payload embeds its
own version string; hybrids fail validation). Release via
`g_shard_test_pause_release`, join writer, assert final data converges.
Prove red-on-base by reverting the Task 2/3 hunks before they exist:
instead run the case BEFORE applying Tasks 1–3 (test-first ordering), paste
both outputs into PLAN_NOTES.md.

### Task 0b — `test_stream_find_chunk_resume.c`

Isolated daemon started with tiny `QUERY_BUFFER_MB` (and therefore tiny
pending_cap ⇒ many close/reopen cycles); multi-shard object; finds using IN
lists, range ops, post-filter siblings, offsets/limits compared against
golden full-scan results. Green before AND after on base (guards my refactor
against result drift); primary value is under sanitizer/stress runs.

### Task 1 — kf-held validate+copy in `slotcask_bulk_fetch_resolved`

Replace the current structure (parallel `kf_revalidate_worker` partition pass
:6189–6216 → global qsort by (sid,fid) :6227 → cross-shard SegFetchArg fan-out
:6259–6265) with a per-shard pipeline. New static:

```c
/* One kf shard's partition: revalidate THEN copy, entirely under one
   continuously-held kf reader (plan 2026-08-21 L214-217). Segcache handles
   nest freely under a kf reader — writers take seg locks only before or
   while holding their own kf lock (A/T phases), never after releasing it,
   so no cycle exists. Runs on parallel_for_io workers; a worker holds at
   most ONE kf handle at a time. */
static void *kf_reval_fetch_worker(void *arg);
```

Body: `kfcache_acquire(shard, reader)` once → existing revalidation probe loop
(inlined from `kf_revalidate_worker` :6137–6147, marking retirees sid=0xFF)
→ compact survivors within the slice → qsort slice by (fid, off) → group
fid-runs → for each run `segcache_acquire` + `seg_rec_live_with_hash` +
user cb copy (reuse of `seg_fetch_worker` body semantics, inlined per run)
→ `kfcache_release` only after the last copy of this shard. Keep
`part_start/part_count` partition build as-is; dispatch all non-empty
partitions through the existing `nparts` `parallel_for_io`. Public signature
unchanged; contract comment added: record_cb executes under a held kf reader
and MUST NOT re-enter slotcask/btree APIs (verified true for all 12 callers'
callbacks — emit/mapping/copy only). Cross-file mmap sharing loss vs the old
global grouping is accepted (warm segcache; matches blessed scan pattern).

### Task 2 — worker-local chunked walker (query.c sites 1+2)

New helpers near the BatchFetchBuf block:

```c
typedef struct ChunkWalkArg {
    char              idx_path[PATH_MAX];
    char              lo[BT_MAX_VAL_LEN]; size_t lo_len; int lo_excl;
    char              hi[BT_MAX_VAL_LEN]; size_t hi_len; int hi_excl;
    int               desc;
    /* pure filter+collect callback; returns -1 to stop, 0 keep going */
    int             (*collect_cb)(const char *val, size_t vlen,
                                   const uint8_t *hash16, void *ctx);
    void             *collect_ctx;
    /* invoked with the walker's OWN iterator closed — safe to block */
    void             (*between_chunks)(void *flush_ctx);
    void             *flush_ctx;
    size_t            chunk_cap;
} ChunkWalkArg;

static void chunk_walk_one_shard(ChunkWalkArg *w);
```

`chunk_walk_one_shard` loop: open `BtRangeIter` → pull entries until
`chunk_cap` collected / stop / exhausted, tracking last delivered
(val≤BT_MAX_VAL_LEN, vlen, hash) → `btree_range_iter_close` → if stopping,
return; else `between_chunks(ctx)` (blocking fetch — NO bt lock held) →
reopen at `min_val = last_val, min_exclusive = 0` with untouched upper bound,
skipping reopened-head entries equal to last_val with
`memcmp(hash,last_hash) <= 0` (byte-exact (val,hash) total order; same skip
convention as cursor_find_cb query.c:5578–5598) → repeat until exhaustion.
Worker memory = one iter + cap×16 bytes, worker-private (no shared
BatchFetchBuf state during walks, no condvar parking, no cross-thread close).

Driver `stream_walk_dispatch_chunked(...)`: replicates btree_dispatch's op
mapping (query.c:636–779) ONLY for locked families — OP_EQUAL, OP_GREATER(_EQ),
OP_LESS(_EQ), OP_BETWEEN (incl. excl flags), OP_IN loop, OP_STARTS_WITH,
OP_LIKE exact/prefix forms, OP_NOT_EQUAL bool / two-range split (specs list,
processed sequentially; ranges disjoint so no dupes). Bitmap dispatch branch
and O_DIRECT/default leaf-scan branches are NOT reachable from these drivers'
locked families; anything unsupported falls back to legacy
`btree_dispatch(stream_find_cb...)` verbatim (safe: those paths hold no bt
locks; inline bfb flushing remains valid there). Fan-out over
`index_splits_for(splits)` shards via `parallel_for_io`.
`chunk_cap = pending_cap / n_workers` clamped to [64, 4096].

Rewire `idx_find_streaming` (:1577–1580) to call the chunked driver when the
primary op is in the supported set (else legacy fallback), keeping
`stream_find_record_cb` emission + sc->stop semantics identical; the
collect-only variant of `stream_find_cb` drops its buffer-full flush trigger.
Rewire `find_via_composite_key` (:2208) to the chunked driver with a single
eq spec `[encoded_key, encoded_key]`.

### Task 3 — aggregate close/reopen sites (query_aggregate.c)

**wfc_worker**: track last pulled (val,vlen,hash16) copies; on batch-full
flush boundary close `it` first, `flush_wfc_batch` (blocking, now lock-free),
reopen: asc → `min_val=last_val,inclusive` + tiebreak skip; desc →
`max_val=last_val,inclusive` + mirrored skip. Early-exit on `w->found`
preserved; budget/deadline checks preserved.

**VS varlen group-by merge**: add `int staged_slot[VS_RUN_HASH_CAP]` filled
alongside run_hashes (which staged slot each hash feeds). Accumulate runs
without fetching; when accumulated unfetched hashes reach VS_FLUSH_CAP=4096
or the loop ends: close all open `iters[s]` → single
`slotcask_bulk_resolve_and_fetch(vs_sdb, pending_hashes, n, ctx…)` where ctx
maps via staged_slot[] into `staged[]` accumulators (vs_lookup_cb gains a
base-staged-slot indirection) → reopen every `has_cur[s]` cursor at
`min_val=cur_keys[s], inclusive` (head snapshot survived; ascending order
guarantees ≥ snapshot; consumed heads were advanced pre-close so nothing is
redelivered) → restore heads via one `next()` per cursor exactly like the
init block (:4514–4521). Preserve abort/IGB fall-through and stage-commit
semantics unchanged.

### Task 4 — docs

docs/concepts/concurrency.md "Ordered index-walk lock rule" section becomes
the general invariant: *code holding ANY btree-cache lock must not block on
kfcache; owner-thread closes its iterators first and reopens from recorded
resume points* — naming sites 1–4. AGENTS.md Concurrency bullet updated to
match. docs/reference/changelog.md unreleased entry.

## Verification gate

Baseline run-all (pre-fix) → diff set. Then SKIP_TESTS=1 build + run-all +
new tests green. Then 3× ASan and 3× TSan full suites with AGENTS.md options.
PLAN_NOTES.md checkpoint appended at each milestone. All work left
uncommitted for human/reviewer raw-diff review.
