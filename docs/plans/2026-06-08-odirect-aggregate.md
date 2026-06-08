# Plan: O_DIRECT segment scan for aggregate full scans

**Goal**: Replace `parallel_agg_scan_shards_v2` (which calls `slotcask_walk_one_shard`
→ 3-pass mmap per KF shard) with a new `parallel_agg_scan_shards_o_direct` that fans out
over segment files with `seg_scan_o_direct`, bypassing the page cache entirely for
aggregate full scans.

**Why**: Aggregate full scans (`cmd_aggregate` with no index, or with non-ranged criteria)
call `parallel_agg_scan_shards_v2`, which launches one worker per KF shard. Each worker
calls `slotcask_walk_one_shard` → `walk_one_shard_inner` (3-pass mmap: KF scan, sort,
segment reads). All segment pages enter cache. Unlike `find`/`count`, which already use
`scan_shards_v2_o_direct`, aggregate has its own separate fan-out that was never converted.

**Approach**: Add `parallel_agg_scan_shards_o_direct` in `query.c`. Pattern mirrors
`scan_shards_v2_o_direct`: enumerate all `.dat` files, allocate one `AggOdSegArg` per
file, each with its own `local AggCtx`, dispatch via `parallel_for_io`, then merge.
Reuses the existing `od_seg_record_cb` / `OdSegAdapterCtx` adapter already in scope.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-aggregate`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `AggOdSegArg`, `agg_od_seg_worker`, and `parallel_agg_scan_shards_o_direct`

**File**: `src/db/query.c`

Locate the anchor (the comment immediately before `agg_v2_scan_worker`):
```
/* Per-kf-shard aggregate types and worker (mmap-based, proven path). */
typedef struct {
    V2ScanWrap   wrap;
    V2ShardArg   arg;
    AggCtx       local;
} AggV2ScanWork;
```

Insert the following IMMEDIATELY BEFORE that anchor:
```c
/* ── O_DIRECT per-segment aggregate fan-out ─────────────────────────────
 * One AggOdSegArg per .dat file. Each worker runs seg_scan_o_direct on its
 * file, accumulating results into a private local AggCtx. After
 * parallel_for_io joins, all locals are merged into main_ctx.
 *
 * Uses the existing od_seg_record_cb / OdSegAdapterCtx adapter so the
 * per-record hot path is identical to scan_shards_v2_o_direct. */
typedef struct {
    char       seg_path[PATH_MAX];
    int        slot_size;
    AggCtx     local;         /* per-segment private accumulator */
    V2ScanWrap wrap;          /* .cb = agg_scan_cb, .ctx = &this->local */
    int       *stop_flag;
    FILE      *parent_out;
} AggOdSegArg;

static void *agg_od_seg_worker(void *raw) {
    AggOdSegArg *arg = (AggOdSegArg *)raw;
    g_out = arg->parent_out ? arg->parent_out : stdout;
    if (__atomic_load_n(arg->stop_flag, __ATOMIC_RELAXED)) return NULL;
    OdSegAdapterCtx actx = { .wrap = &arg->wrap, .stop_flag = arg->stop_flag };
    seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    count_scan_cb_flush_thread();
    return NULL;
}

static void parallel_agg_scan_shards_o_direct(AggCtx *main_ctx,
                                               SlotcaskDb *sdb) {
    if (!sdb || sdb->num_streams <= 0) return;

    AggOdSegArg *args = NULL;
    size_t nargs = 0, cap = 0;
    int stop_flag = 0;
    FILE *parent_out = g_out;

    for (int s = 0; s < sdb->num_streams; s++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir),
                 "%s/data/streams/%03d", sdb->data_dir, s);
        DIR *dh = opendir(stream_dir);
        if (!dh) continue;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            size_t nlen = strlen(de->d_name);
            if (nlen < 4 || strcmp(de->d_name + nlen - 4, ".dat") != 0)
                continue;
            if (nargs >= cap) {
                size_t newcap = cap ? cap * 2 : 64;
                AggOdSegArg *t = realloc(args, newcap * sizeof(AggOdSegArg));
                if (!t) { closedir(dh); goto run; }
                args = t;
                cap  = newcap;
            }
            AggOdSegArg *a = &args[nargs];
            memset(a, 0, sizeof(*a));
            snprintf(a->seg_path, PATH_MAX, "%s/%s", stream_dir, de->d_name);
            a->slot_size   = sdb->slot_size;
            agg_ctx_clone_shared(&a->local, main_ctx);
            a->wrap.cb     = agg_scan_cb;
            a->wrap.ctx    = &a->local;
            a->stop_flag   = &stop_flag;
            a->parent_out  = parent_out;
            nargs++;
        }
        closedir(dh);
    }

run:
    if (nargs == 0) { free(args); return; }
    g_scan_stop = 0;
    parallel_for_io(agg_od_seg_worker, args, (int)nargs, sizeof(AggOdSegArg));
    for (size_t i = 0; i < nargs; i++) {
        if (args[i].local.budget_exceeded) main_ctx->budget_exceeded = 1;
        agg_ctx_merge(main_ctx, &args[i].local);
        agg_ctx_free_local(&args[i].local);
    }
    free(args);
}

```

---

## Task 2 — Replace `parallel_agg_scan_shards_v2` call in `cmd_aggregate`

**File**: `src/db/query.c`

Locate the anchor (the full-scan dispatch inside `cmd_aggregate`):
```
        if (sdb) parallel_agg_scan_shards_v2(ctx, sdb);
```

Replace with:
```c
        if (sdb) parallel_agg_scan_shards_o_direct(ctx, sdb);
```

**Verify**: confirm exactly one replacement was made. If the anchor matches more than one
line, stop and write `PLAN_NOTES.md`.

---

## Task 3 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
Pay attention to tests involving `aggregate` (test-aggregate, test-aggregate-group,
test-aggregate-having, etc.).

---

## Invariants and edge cases

- Each `AggOdSegArg` holds its own `local AggCtx`. `agg_ctx_clone_shared` copies the
  specs, group_by fields, and having expression — no shared mutable state between workers.
  `agg_ctx_merge` is called after `parallel_for_io` joins (single-threaded at merge time).
- `memset(a, 0, sizeof(*a))` before `agg_ctx_clone_shared` ensures all AggCtx pointers
  start NULL — `agg_ctx_clone_shared` treats a zero-init AggCtx as an empty fresh one.
- `parallel_for_io` dispatches using stride = `sizeof(AggOdSegArg)`. `AggOdSegArg` is
  large (contains a full AggCtx), but the stride calculation is identical to
  `AggV2ScanWork` which also embeds an AggCtx.
- The fan-out is per segment FILE (not per KF shard). The number of parallel workers
  equals the number of `.dat` files across all streams, which can be higher than
  `num_shards`. This is fine — `parallel_for_io` queues work across the IO pool.
- If `seg_scan_o_direct` falls back to buffered I/O internally, behaviour is identical to
  the O_DIRECT case from the caller's perspective.
- `parallel_agg_scan_shards_v2` and `agg_v2_scan_worker` remain in the file — they are
  unused after Task 2 but left in place. A follow-up cleanup can remove them.
- Aggregates WITH an index path use `parallel_indexed_agg` / `parallel_agg_scan_shards`
  (hash-keyed), not `parallel_agg_scan_shards_v2`. Those paths are not changed by this plan.
