# Plan: IGB btree Pass 1 — parallel_for → parallel_for_io

**Date:** 2026-06-09  
**Branch:** `feat/igb-btree-parallel-io`  
**File:** `src/db/query.c`

## Problem

`group by age top 5` (count-only, btree group field, IGB path) takes ~12 s at 25 M
records. The indexed Pass 1 dispatches via `parallel_for` (CPU thread pool, `nproc`
threads). Btree shard walks are I/O-bound: each `btree_range_iter_open` issues a random
read per 4 KB page, and each `btree_range_iter_next` reads compressed leaf pages
sequentially but still stalls on page-fault I/O when the btree is cold. The CPU pool
saturates its nproc threads quickly; additional I/O latency hides behind the pool limit.

`parallel_for_io` uses the IO pool (default `4 × nproc` threads). Switching Pass 1 to
`parallel_for_io` lets many more concurrent shard walks overlap I/O wait time —
expected to cut `group by age top 5` from ~12 s to ~3–4 s.

Additionally, the secondary-field hash16→string map build (multi-field `group_by`) runs
a sequential `for` loop across index shards. A new parallel worker for that loop gives
the same benefit when `ngroups > 1`.

## Why this is safe

`igb_pass1_worker` (the Pass 1 worker function) has **no internal calls** to
`parallel_for` or `parallel_for_io`. It calls only:
- `btree_range_iter_open` / `btree_range_iter_next` / `btree_range_iter_close`
- `agg_find_or_create` (on its own private `local` AggCtx — no shared state)
- `hbk_insert` (on its own private `local_hbk`)
- `keyset_contains` (read-only shared `crit_ks`)
- `hsm_get` (read-only shared `sec_maps`)

No nesting of `parallel_for_io` inside the IO pool → no deadlock risk.
`t_in_io_task=1` is set by the IO pool before calling each worker; since the worker
never calls `parallel_for_io`, the flag is irrelevant but harmless.

The scatter and merge phases after Pass 1 use `parallel_for` (CPU pool); those are
separate calls that run after the IO pool work is fully joined — no overlap, no issue.

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Never claim a step passed without the real build/test output.
- Locate every edit by the **quoted anchor text** below; if an anchor is not found
  exactly, stop and write `PLAN_NOTES.md` — do not guess.

---

## Task 1 — Switch Pass 1 dispatch to `parallel_for_io`

**File:** `src/db/query.c`

**Anchor** (exact text):
```
                    parallel_for(igb_pass1_worker, workers, n_idx_g,
                                 sizeof(IgbPass1Worker));
```

Replace with:

```c
                    parallel_for_io(igb_pass1_worker, workers, n_idx_g,
                                    sizeof(IgbPass1Worker));
```

That is the only change in this task — a single identifier swap. The surrounding
`if (workers)` allocation block, the merge phase, and all other `parallel_for` calls
(scatter_worker, partition merge) are unchanged.

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 2 — Parallelise secondary-field hash16→string map build

This task replaces the inner sequential shard loop (the loop that walks each secondary
group field's btree to build `sec_maps[g]`) with a parallel worker.

### 2a — Add `SecMapBuildWorker` struct and `sec_map_build_worker` function

**File:** `src/db/query.c`

**Anchor** (exact text — the start of `IgbPass1Worker`):
```
/* Per-shard worker for parallel indexed group_by Pass 1.
```

Insert the following block **immediately before** that anchor:

```c
/* Per-shard worker for building one secondary group_by field's hash16→string
   map in parallel. Each worker processes one btree shard of one secondary
   field, inserting decoded string values into a pre-initialised HashStrMap.
   hsm_insert is NOT thread-safe; each worker operates on a distinct shard
   range of the same map.  Because HashStrMap uses open addressing keyed by
   the 16-byte hash16, two workers inserting different hash16s into the same
   table can race.  Therefore each worker writes into its own LOCAL HashStrMap;
   the orchestrator merges all per-worker maps into the shared sec_maps[g]
   serially after join. */
typedef struct {
    int              shard_id;
    const char      *db_root;
    const char      *object;
    const char      *gfield_s;       /* secondary field name */
    const TypedField *gtf_s;
    KeySet          *crit_ks;        /* shared read-only filter (may be NULL) */
    HashStrMap       local_map;      /* per-worker output */
    int              cap_hint;       /* hsm_init capacity hint */
    int              arena_hint;     /* hsm_init arena hint */
    int              aborted;
    int              dl_counter;
    QueryDeadline   *dl;
} SecMapBuildWorker;

static void *sec_map_build_worker(void *arg) {
    SecMapBuildWorker *w = (SecMapBuildWorker *)arg;
    if (hsm_init(&w->local_map, (size_t)w->cap_hint,
                  (size_t)w->arena_hint) != 0) {
        w->aborted = 1;
        return NULL;
    }
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield_s, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->dl, &w->dl_counter)) {
            w->aborted = 1; break;
        }
        if (w->crit_ks && !keyset_contains(w->crit_ks, hash16)) continue;
        char dbuf[512];
        int dlen = decode_idx_to_buf(w->gtf_s, (const uint8_t *)val,
                                      vlen, dbuf, sizeof(dbuf), 0);
        if (dlen <= 0) continue;
        if (hsm_insert(&w->local_map, hash16, dbuf, (size_t)dlen) != 0) {
            w->aborted = 1; break;
        }
    }
    btree_range_iter_close(it);
    return NULL;
}

```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

### 2b — Replace the sequential secondary shard loop with parallel dispatch + merge

**File:** `src/db/query.c`

**Anchor** (exact text — the secondary field sequential loop):
```
                const TypedField *gtf_s = ctx.group_tfs[g + 1];
                const char *gfld_s = ctx.group_fields[g + 1];
                int sec_aborted = 0;
                for (int s = 0; s < n_idx_s && !sec_aborted; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root,
                                   object, gfld_s, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
                        if (query_deadline_tick(&dl, &dl_counter_sec)) {
                            sec_aborted = 1; break;
                        }
                        /* Filter by crit_ks here too so we don't waste arena
                           on records the criteria excludes. */
                        if (crit_ks && !keyset_contains(crit_ks, hash16)) continue;
                        char dbuf[512];
                        int dlen = decode_idx_to_buf(gtf_s,
                                                     (const uint8_t *)val,
                                                     vlen, dbuf, sizeof(dbuf), 0);
                        if (dlen <= 0) continue;
                        if (hsm_insert(&sec_maps[g], hash16, dbuf,
                                       (size_t)dlen) != 0) {
                            sec_aborted = 1; break;
                        }
                    }
                    btree_range_iter_close(it);
                }
                if (sec_aborted) {
```

Replace that entire block (from `const TypedField *gtf_s = ...` through
`if (sec_aborted) {`) with:

```c
                const TypedField *gtf_s = ctx.group_tfs[g + 1];
                const char *gfld_s = ctx.group_fields[g + 1];
                int sec_aborted = 0;
                int live_s = get_live_count(db_root, object);
                if (live_s <= 0) live_s = 1024;
                int per_s_hint = (live_s + n_idx_s - 1) / n_idx_s;
                int per_s_arena = per_s_hint * 12; /* avg 8-12B per entry */
                SecMapBuildWorker *sw = calloc((size_t)n_idx_s,
                                               sizeof(SecMapBuildWorker));
                if (!sw) { sec_aborted = 1; goto sec_aborted_label; }
                for (int s = 0; s < n_idx_s; s++) {
                    sw[s].shard_id   = s;
                    sw[s].db_root    = db_root;
                    sw[s].object     = object;
                    sw[s].gfield_s   = gfld_s;
                    sw[s].gtf_s      = gtf_s;
                    sw[s].crit_ks    = crit_ks;
                    sw[s].cap_hint   = per_s_hint;
                    sw[s].arena_hint = per_s_arena;
                    sw[s].dl         = &dl;
                }
                parallel_for_io(sec_map_build_worker, sw, n_idx_s,
                                 sizeof(SecMapBuildWorker));
                /* Serial merge: walk each per-worker local_map and insert
                   into the shared sec_maps[g].  hsm_insert on a single map
                   is safe here because the main thread is the only writer.
                   HashStrEntry stores value as (off, len) into local_map.arena;
                   dereference via lm->arena + he->off. */
                for (int s = 0; s < n_idx_s && !sec_aborted; s++) {
                    if (sw[s].aborted) { sec_aborted = 1; break; }
                    HashStrMap *lm = &sw[s].local_map;
                    if (!lm->entries) continue;
                    for (size_t bi = 0; bi < lm->cap; bi++) {
                        HashStrEntry *he = &lm->entries[bi];
                        if (!he->occupied) continue;
                        const char *sval = lm->arena + he->off;
                        size_t      slen = he->len;
                        if (hsm_insert(&sec_maps[g], he->hash, sval,
                                        slen) != 0) {
                            sec_aborted = 1; break;
                        }
                    }
                    hsm_free(lm);
                }
                for (int s = 0; s < n_idx_s; s++) {
                    if (sw[s].local_map.entries) hsm_free(&sw[s].local_map);
                }
                free(sw); sw = NULL;
sec_aborted_label:
                if (sec_aborted) {
```

**Note:** The code after `if (sec_aborted) {` continues with the cleanup block that was
already there. Leave everything from
`for (int k = 0; k <= g; k++) hsm_free(&sec_maps[k]);` onwards unchanged.

`HashStrEntry` fields (confirmed in codebase): `hash[16]`, `off` (uint32_t arena offset),
`len` (uint16_t), `occupied` (uint8_t). Values are stored in `lm->arena + he->off`.

Build: `SKIP_TESTS=1 ./build.sh` — must succeed with zero errors.

---

## Task 3 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

Paste the actual output. Do not claim pass without real output.

---

## Invariants and edge cases

| Case | Expected behaviour |
|---|---|
| `igb_pass1_worker` called from IO pool | No internal `parallel_for_io` calls → no nesting → safe |
| `n_idx_g < 4` or `live < 100 000` | Existing guard at `if (n_idx_g >= 4 && live_for_pass1 >= 100000)` still applies; serial fallback used |
| `n_idx_s == 1` (small object) | `parallel_for_io` with 1 worker is equivalent to serial |
| `sec_map_build_worker` alloc fails (`hsm_init` fails) | `aborted=1` → detected in merge loop → `sec_aborted=1` → existing cleanup + `igb_skip` |
| All scatter / merge `parallel_for` calls | Unchanged — they run after IO pool work is fully joined |
| Count-only queries (no `igb_needs_hbm`) | Pass 1 still uses IO pool; Pass 2 skipped (`hbk_ready=0`); result correct |

## Expected performance improvement

| Query | Before | After |
|---|---|---|
| `group by age top 5` (count-only, 25 M) | ~12 s | ~3–4 s |
| `group by age, avg(score)` (hbm, ≤ 6 M) | blocked by hbk budget → O_DIRECT | — (no change until bitmap-igb-hbm plan) |
| `group by username` (varchar, high cardinality) | already fast via parallel Pass 1 | no regression expected |
