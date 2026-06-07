# Plan: bulk-delete-criteria optimization (index-aware Phase 1 + batched index drops)

**Status:** Ready for execution.

## Problem

`cmd_bulk_delete_criteria` has two performance bottlenecks:

1. **Phase 1 is a full table scan.** Even for highly selective criteria like `type=job`
   (1,883 matches out of millions of records), `scan_dispatch` reads every data shard
   record and applies `criteria_match_tree` per slot.

2. **Phase 2 index drops are inline and serial per record.** Each deleted record calls
   `delete_index_entry` (and `update_idx_fn` for trigram) once per index field, holding
   the kf wrlock while doing btree writes. With 10 indexes and 1,000 deleted records,
   that is 10,000 individual btree operations inside the lock.

## Solution

**Part A — Index-aware Phase 1:** Call `plan_filter` + `build_keyset_from_plan` to
resolve matching hashes from the index. Use `slotcask_bulk_resolve_and_fetch` to
fetch keys and re-verify the full criteria tree per fetched record. Fall back to
`scan_dispatch` when `plan_filter` returns `FP_FULL_SCAN` or the keyset build fails.

**Part B — Batched index drops:** Remove inline index drops from
`v2_bulk_del_crit_pre_commit_bulk`. Instead, collect `(field_idx, itype, hash[16],
idx_key, idx_key_len)` tuples per shard worker. After Phase 2's `parallel_for`,
merge all workers' drop buffers, sort by `(field_idx, hash16[0:2])` for btree cache
locality, and process sequentially.

## Invariants (must hold throughout)

- Phase 2 always re-verifies the full criteria tree inside the kf wrlock
  (`criteria_match_tree` in `v2_bulk_del_crit_pre_commit_bulk`). This guards against
  stale index entries (records deleted between Phase 1 and Phase 2). **Do not remove
  this check.**
- `dry_run=1` returns matched count without touching Phase 2 or index drops. Unchanged.
- The indexed Phase 1 path produces the same result as the full scan path: all matching
  keys, subject to the same `limit` and deadline checks.
- Index drop correctness: tombstone happens before index cleanup (Phase 2 first, flush
  drops second). A stale index entry pointing to a tombstone is harmless — KF lookup
  rejects it. Wrong order is NOT acceptable (would create live records with missing
  index entries).

## All changes in `src/db/query.c`

Execution rules: branch off `main`; do steps in order; locate every insertion site by
the quoted anchor text (not line numbers); build with `SKIP_TESTS=1 ./build.sh`;
test with `./build/bin/shard-db-test run-all`; never claim a step passed without
the real output; stop and write `PLAN_NOTES.md` if any anchor is not found exactly.

---

### Step 1 — New `BulkDelCritDropEntry` struct

Insert immediately before the anchor text
`typedef struct {\n    SlotcaskDb     *sdb;\n    const char     *db_root;\n    const char     *object;\n    const Schema   *sch;\n    TypedSchema    *ts;\n    CriteriaNode   *tree;\n    FieldSchema    *fs;\n    SearchCriterion *cas_crit;`
(the opening of `BulkDelCritShardWork`).

```c
typedef struct {
    int              field_idx;   /* index into w->idx_fields[] */
    enum IndexType   itype;
    uint8_t          hash[16];
    uint8_t         *idx_key;     /* heap-alloc'd; freed by bulk_delete_flush_drops */
    size_t           idx_key_len;
} BulkDelCritDropEntry;

```

---

### Step 2 — Add drop buffer fields to `BulkDelCritShardWork`

Find the anchor:
```
    /* result */
    int             deleted;
    int             skipped;
} BulkDelCritShardWork;
```

Replace it with:

```c
    /* per-worker deferred index drop buffer (collected during Phase 2,
       flushed in bulk after parallel_for returns) */
    BulkDelCritDropEntry *drop_entries;
    int                   drop_count;
    int                   drop_cap;
    /* result */
    int             deleted;
    int             skipped;
} BulkDelCritShardWork;
```

---

### Step 3 — Collect instead of drop in `v2_bulk_del_crit_pre_commit_bulk`

Find the anchor (this is the entire index-drop block inside the function):
```c
    if (w->nidx > 0 && w->ts) {
        for (int fi = 0; fi < w->nidx; fi++) {
            uint8_t *buf = NULL; size_t blen = 0;
            if (build_index_key_from_record(w->ts, old->value,
                                              w->idx_fields[fi], &buf, &blen)) {
                enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
                if (itype == IT_TRIGRAM) {
                    UpdateIdxArg a = {0};
                    a.db_root = w->db_root; a.object = w->object;
                    a.field = w->idx_fields[fi]; a.splits = w->sch->splits;
                    a.new_key = NULL; a.new_len = 0;
                    a.old_key = buf;  a.old_len = blen;
                    a.hash = w->hashes[ki]; a.type = IT_TRIGRAM;
                    update_idx_fn(&a);
                } else {
                    delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                        w->sch->splits, buf, blen, w->hashes[ki]);
                }
                free(buf);
            }
        }
    }
```

Replace it with:

```c
    if (w->nidx > 0 && w->ts) {
        for (int fi = 0; fi < w->nidx; fi++) {
            uint8_t *buf = NULL; size_t blen = 0;
            if (!build_index_key_from_record(w->ts, old->value,
                                               w->idx_fields[fi], &buf, &blen))
                continue;
            enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            /* Grow drop buffer if needed */
            if (w->drop_count >= w->drop_cap) {
                int new_cap = w->drop_cap ? w->drop_cap * 2 : 16;
                BulkDelCritDropEntry *tmp = realloc(w->drop_entries,
                    (size_t)new_cap * sizeof(BulkDelCritDropEntry));
                if (!tmp) { free(buf); continue; }
                w->drop_entries = tmp;
                w->drop_cap = new_cap;
            }
            BulkDelCritDropEntry *e = &w->drop_entries[w->drop_count++];
            e->field_idx  = fi;
            e->itype      = itype;
            memcpy(e->hash, w->hashes[ki], 16);
            e->idx_key    = buf;   /* ownership transferred; freed by flush */
            e->idx_key_len = blen;
        }
    }
```

---

### Step 4 — New `bulk_delete_flush_drops` helper

Insert immediately before the anchor text
`int cmd_bulk_delete_criteria(const char *db_root, const char *object,`
(the function signature of `cmd_bulk_delete_criteria`).

```c
/* Comparator for bulk_delete_flush_drops: sort by (field_idx, hash[0:2])
   so drops to the same btree shard are adjacent (cache locality). */
static int cmp_bulk_del_drop(const void *a, const void *b) {
    const BulkDelCritDropEntry *ea = a, *eb = b;
    if (ea->field_idx != eb->field_idx) return ea->field_idx - eb->field_idx;
    return memcmp(ea->hash, eb->hash, 2);
}

/* Merge all per-worker drop buffers, sort for locality, process in order.
   Called after Phase 2's parallel_for returns. Workers own their idx_key
   buffers; this function frees them. */
static void bulk_delete_flush_drops(BulkDelCritShardWork *workers,
                                     int nshard_groups,
                                     const char *db_root,
                                     const char *object,
                                     int splits,
                                     char idx_fields[][256],
                                     const enum IndexType *idx_types) {
    /* Count total entries */
    int total = 0;
    for (int g = 0; g < nshard_groups; g++) total += workers[g].drop_count;
    if (total == 0) return;

    BulkDelCritDropEntry *all = malloc((size_t)total * sizeof(BulkDelCritDropEntry));
    if (!all) {
        /* OOM: free idx_key buffers in workers to avoid leaks */
        for (int g = 0; g < nshard_groups; g++)
            for (int i = 0; i < workers[g].drop_count; i++)
                free(workers[g].drop_entries[i].idx_key);
        return;
    }
    int pos = 0;
    for (int g = 0; g < nshard_groups; g++) {
        memcpy(&all[pos], workers[g].drop_entries,
               (size_t)workers[g].drop_count * sizeof(BulkDelCritDropEntry));
        pos += workers[g].drop_count;
    }

    qsort(all, (size_t)total, sizeof(BulkDelCritDropEntry), cmp_bulk_del_drop);

    for (int i = 0; i < total; i++) {
        BulkDelCritDropEntry *e = &all[i];
        if (e->itype == IT_TRIGRAM) {
            UpdateIdxArg a = {0};
            a.db_root  = db_root; a.object = object;
            a.field    = idx_fields[e->field_idx];
            a.splits   = splits;
            a.new_key  = NULL; a.new_len = 0;
            a.old_key  = e->idx_key; a.old_len = e->idx_key_len;
            a.hash     = e->hash; a.type = IT_TRIGRAM;
            update_idx_fn(&a);
        } else {
            delete_index_entry(db_root, object, idx_fields[e->field_idx],
                                splits, e->idx_key, e->idx_key_len, e->hash);
        }
        free(e->idx_key);
    }
    free(all);
}

/* Forward declaration: implemented after build_keyset_from_plan (which it
   depends on). Uses plan_filter + build_keyset_from_plan to resolve matching
   hashes from the index, then fetches keys. Returns 1 on success (out_ctx
   populated), 0 if caller must fall back to scan_dispatch. */
static int bulk_delete_phase1_indexed(const char *db_root, const char *object,
                                       const Schema *sch, FieldSchema *fs,
                                       CriteriaNode *tree, int limit,
                                       QueryDeadline *dl,
                                       BulkCriteriaCtx *out_ctx);

```

---

### Step 5 — Modify Phase 1 in `cmd_bulk_delete_criteria`

Find the anchor:
```c
    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
```

Replace it with:

```c
    /* Phase 1: resolve matching keys — indexed path first, full scan fallback */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    if (!bulk_delete_phase1_indexed(db_root, object, &sch, &fs,
                                     tree, limit, &dl, &ctx)) {
        scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
```

---

### Step 6 — Call `bulk_delete_flush_drops` and free drop buffers after Phase 2

Find the anchor (the result-merge loop immediately after `parallel_for`):
```c
    for (int g = 0; g < nshard_groups; g++) {
        deleted += workers[g].deleted;
        skipped += workers[g].skipped;
        free(workers[g].keys);
        free(workers[g].hashes);
    }
    free(workers); free(hashes); free(shard_ids);
```

Replace it with:

```c
    for (int g = 0; g < nshard_groups; g++) {
        deleted += workers[g].deleted;
        skipped += workers[g].skipped;
        free(workers[g].keys);
        free(workers[g].hashes);
    }
    /* Phase 3: flush deferred index drops in sorted (field, shard) order */
    bulk_delete_flush_drops(workers, nshard_groups,
                             db_root, object, sch.splits,
                             idx_fields, idx_types);
    for (int g = 0; g < nshard_groups; g++)
        free(workers[g].drop_entries);
    free(workers); free(hashes); free(shard_ids);
```

---

### Step 7 — New `bulk_criteria_indexed_cb` and `bulk_delete_phase1_indexed`

Insert immediately after the closing brace of `build_keyset_from_plan`
(anchor: the line `    case FP_FULL_SCAN:\n    default:\n        return NULL;\n    }\n}`
which ends `build_keyset_from_plan`).

```c
/* SlotcaskScanCb for bulk_delete_phase1_indexed: receives key + value from a
   slotcask_bulk_resolve_and_fetch call, re-verifies the full criteria tree,
   and appends the key to the BulkCriteriaCtx if it matches. Same locking and
   budget logic as bulk_criteria_scan_cb, different callback signature. */
static int bulk_criteria_indexed_cb(const uint8_t hash16[16],
                                     const void *key, size_t klen,
                                     const void *value, size_t vlen,
                                     void *raw_ctx) {
    (void)hash16; (void)vlen;
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)raw_ctx;
    if (bc->budget_exceeded) return 0;
    if (bc->limit > 0 && bc->count >= bc->limit) return 0;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 0;

    /* Re-verify full criteria tree (index may be slightly stale) */
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;

    char *k = malloc(klen + 1);
    if (!k) return 0;
    memcpy(k, key, klen);
    k[klen] = '\0';

    pthread_mutex_lock(&bc->lock);
    if (bc->budget_exceeded || (bc->limit > 0 && bc->count >= bc->limit)) {
        pthread_mutex_unlock(&bc->lock);
        free(k);
        return 0;
    }
    size_t key_bytes = sizeof(char *) + klen + 1;
    if (bc->buffer_bytes + key_bytes > g_query_buffer_max_bytes) {
        bc->budget_exceeded = 1;
        pthread_mutex_unlock(&bc->lock);
        free(k);
        return 0;
    }
    bc->buffer_bytes += key_bytes;
    if (bc->count >= bc->cap) {
        int new_cap = bc->cap ? bc->cap * 2 : 64;
        char **nk = realloc(bc->keys, (size_t)new_cap * sizeof(char *));
        if (!nk) {
            bc->budget_exceeded = 1;
            pthread_mutex_unlock(&bc->lock);
            free(k);
            return 0;
        }
        bc->keys = nk;
        bc->cap  = new_cap;
    }
    bc->keys[bc->count++] = k;
    pthread_mutex_unlock(&bc->lock);
    return 0;
}

/* Index-aware Phase 1 for cmd_bulk_delete_criteria.
   Returns 1 and populates out_ctx->keys[] when the planner finds a usable
   index. Returns 0 when the caller must fall back to scan_dispatch.
   Never returns 0 with out_ctx partially populated. */
static int bulk_delete_phase1_indexed(const char *db_root, const char *object,
                                       const Schema *sch, FieldSchema *fs,
                                       CriteriaNode *tree, int limit,
                                       QueryDeadline *dl,
                                       BulkCriteriaCtx *out_ctx) {
    size_t N = (size_t)get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, fs,
                                sch->splits, N,
                                NULL, 0 /* fetching=0, count semantics */,
                                limit);
    if (fp.kind == FP_FULL_SCAN) return 0;

    KeySet *ks = build_keyset_from_plan(&fp, db_root, object, sch, dl);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    if (keyset_size(ks) == 0) {
        /* Zero index matches → out_ctx already has count=0, correct. */
        keyset_free(ks);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { keyset_free(ks); return 0; }

    /* Iterate keyset and resolve in batches of 1024 (same as ordered-find). */
#define BDI_BATCH 1024
    uint8_t batch[BDI_BATCH][16];
    int batch_n = 0;
    for (size_t b = 0; b < ks->cap; b++) {
        uint32_t s = atomic_load_explicit(
            (_Atomic uint32_t *)&ks->state[b], memory_order_acquire);
        if (s != 2) continue;
        memcpy(batch[batch_n++], ks->keys[b], 16);
        if (batch_n == BDI_BATCH) {
            slotcask_bulk_resolve_and_fetch(sdb, batch, BDI_BATCH,
                                             out_ctx, bulk_criteria_indexed_cb);
            batch_n = 0;
            if (out_ctx->budget_exceeded || dl->timed_out) break;
            if (limit > 0 && out_ctx->count >= limit) break;
        }
    }
    if (batch_n > 0 && !out_ctx->budget_exceeded && !dl->timed_out)
        slotcask_bulk_resolve_and_fetch(sdb, batch, (size_t)batch_n,
                                         out_ctx, bulk_criteria_indexed_cb);
    keyset_free(ks);
    return 1;
}
```

---

### Step 8 — New test case `src/test/cases/test_bulk_delete_criteria_indexed.c`

Create a new file. Register it with `TEST_REGISTER(test_bulk_delete_criteria_indexed)`.

The test must:
1. Create object `bdc_idx` with `splits=8`, fields `status:varchar:16`, `score:int`,
   indexes on `status` and `score`.
2. Bulk-insert 500 records: 200 with `status=paid`, 300 with `status=pending`.
3. Run `bulk-delete-criteria` with `criteria=[status eq paid]`.
4. Assert `deleted=200`, `matched=200`, `skipped=0`.
5. Count `status=paid` → assert `0`.
6. Count `status=pending` → assert `300`.
7. Run `find` by `status=pending` → assert 300 results.
8. Count total (no criteria) → assert `300`.
9. Run `bulk-delete-criteria` with `criteria=[score gte 0]` (all remaining).
10. Assert `deleted=300`.
11. Count total → assert `0`.

Use `tc_request` / `ASSERT_EQ_INT` / `ASSERT_CONTAINS` from the test framework.
See `src/test/cases/test_slotcask_v2_bulk.c` for the exact pattern to follow.

---

## Correctness notes

| Concern | Requirement |
|---|---|
| **Stale index entries** | `bulk_criteria_indexed_cb` re-verifies full criteria tree per record. Phase 2 pre_commit also re-verifies under wrlock. Double check is intentional. |
| **Keyset overflow** | If `build_keyset_from_plan` returns NULL (budget exceeded, OOM), `bulk_delete_phase1_indexed` returns 0 → fall back to scan_dispatch. Correct. |
| **Deadline mid-batch** | `dl->timed_out` checked after each batch in `bulk_delete_phase1_indexed`. Break and return 1 (partial ctx). Phase 2 timeout check in `cmd_bulk_delete_criteria` catches the timeout. |
| **Drop buffer OOM** | In Step 3: `realloc` failure skips that entry with `continue`. The index entry is leaked (not dropped). This is acceptable — vacuum will clean it up. Do not skip the tombstone itself. |
| **Flush drops OOM** | `bulk_delete_flush_drops` malloc fails → frees all idx_key buffers and returns. Tombstones still committed; index entries remain stale. Vacuum cleans up. |
| **Order: tombstone before index cleanup** | Phase 2 commits tombstones. Phase 3 flushes index drops. This order is always preserved. |
| **dry_run** | Returns before Phase 2 (unchanged). `bulk_delete_phase1_indexed` is called to get `matched` count, then dry_run branch returns. `ctx.keys[]` is freed in the dry_run path already. |
| **`BDI_BATCH` macro scope** | `#define BDI_BATCH 1024` is local to `bulk_delete_phase1_indexed`. It must be `#undef BDI_BATCH` after the function ends to avoid leaking the macro. Add `#undef BDI_BATCH` immediately after the closing brace. |

## Net diff

~+140 lines (180 new, ~40 replaced).
