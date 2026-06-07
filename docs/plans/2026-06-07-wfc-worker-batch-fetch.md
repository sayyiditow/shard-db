# Plan: wfc_worker Batch-fetch (MIN/MAX + criteria fast path)

**Status:** Draft — ready for execution.

## Problem

`wfc_worker` does one `read_record_ref` per btree entry when walking the
MIN/MAX aggregate field's index.  `read_record_ref` calls
`slotcask_registry_get` + `slotcask_lookup_by_hash` per call — a KF-cache
acquire/release per record.  At budget=10000 per shard × 128 shards that's
1.28M individual KF probes.

## Solution

Replace per-record `read_record_ref` with `slotcask_bulk_resolve_and_fetch`
in batches of 64 hashes.  The batch callback runs `criteria_match_tree` and
re-decodes the aggregate value via `typed_field_to_double` from the full
record bytes.

## Bugs in the original draft (fixed below)

### Bug 1 — First-match ordering broken

`slotcask_bulk_resolve_and_fetch` internally sorts resolved records by
`(sid, fid)` and for >3 segment files dispatches them in **parallel** via
`parallel_for_io` (see `slotcask_bulk_fetch_resolved`, line ~4071).  Records
are therefore delivered to the callback in segment-file order, not in the
original btree order.

If 2+ records within a batch of 64 satisfy the criteria, the old callback took
the first one in seg-file order — which may have a larger aggregate value than
an earlier btree entry, giving the wrong MIN/MAX.

**Fix**: scan **all** records in the batch, update `best` whenever a record's
value is better (smaller for MIN, larger for MAX), then break the outer btree
walk after the batch if any match was found.  This requires passing `desc`
into `WfcBatchCtx`.

### Bug 2 — Data race on `*bc->best` / `*bc->found`

When `parallel_for_io` is used, multiple `seg_fetch_worker` threads invoke
the callback concurrently.  The old callback wrote `*bc->best` and `*bc->found`
without synchronisation — undefined behaviour in C.

**Fix**: add `pthread_mutex_t mu` to `WfcBatchCtx` and lock it around the
`best` / `found` update.

## Changes (all in `src/db/query.c`)

### 1. New struct `WfcBatchCtx` (before `WfcArg`, ~line 22760)

```c
typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    int               desc;     /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    double           *best;
    int              *found;
    pthread_mutex_t   mu;
} WfcBatchCtx;
```

### 2. New callback `wfc_batch_cb` (before `wfc_worker`)

Scans every record in the batch; keeps the best value seen so far.
Always returns 0 (continue) so all seg workers complete — necessary
because multiple workers run in parallel and we need the extremal match,
not just the first one delivered.

```c
static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;
    double v;
    if (!typed_field_to_double(bc->agg_tf,
                               (const uint8_t *)value + bc->agg_tf->offset,
                               &v))
        return 0;
    pthread_mutex_lock(&bc->mu);
    if (!*bc->found ||
        (!bc->desc && v < *bc->best) ||
        ( bc->desc && v > *bc->best)) {
        *bc->best  = v;
        *bc->found = 1;
    }
    pthread_mutex_unlock(&bc->mu);
    return 0;
}
```

### 3. New helper `flush_wfc_batch` (before `wfc_worker`)

```c
static void flush_wfc_batch(SlotcaskDb *sdb, WfcArg *w,
                             uint8_t (*batch)[16], int bn,
                             WfcBatchCtx *bc) {
    if (sdb) {
        slotcask_bulk_resolve_and_fetch(sdb, batch, (size_t)bn, bc, wfc_batch_cb);
    } else {
        for (int i = 0; i < bn; i++) {
            RecordRef rr;
            if (read_record_ref(w->db_root, w->object, w->sch,
                                batch[i], &rr) != 0) continue;
            wfc_batch_cb(batch[i], NULL, 0, rr.val, rr.vlen, bc);
            release_record_ref(&rr);
        }
    }
}
```

### 4. Modified `wfc_worker`

```c
static void *wfc_worker(void *arg) {
    WfcArg *w = (WfcArg *)arg;

    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(w->db_root, w->object, &info);

    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   w->db_root, w->object, w->agg_field, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, w->desc);
    if (!it) return NULL;

    const char *val; size_t vlen; const uint8_t *hash16;
    int walks = 0;
#define WFC_BATCH 64
    uint8_t batch[WFC_BATCH][16];
    int bn = 0;
    WfcBatchCtx bc = {
        .tree   = w->tree,
        .fs     = w->fs,
        .agg_tf = w->agg_tf,
        .desc   = w->desc,
        .best   = &w->best,
        .found  = &w->found,
        .mu     = PTHREAD_MUTEX_INITIALIZER,
    };

    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->deadline, &w->dl_counter)) break;
        if (++walks > w->budget) { w->budget_exceeded = 1; break; }

        double v;
        if (!decode_index_key_to_double(w->agg_tf, (const uint8_t *)val,
                                        vlen, &v)) continue;

        memcpy(batch[bn], hash16, 16);
        bn++;

        if (bn >= WFC_BATCH) {
            flush_wfc_batch(sdb, w, batch, bn, &bc);
            bn = 0;
            if (w->found) break;   /* first batch with a match → done */
        }
    }
    if (bn > 0 && !w->found)
        flush_wfc_batch(sdb, w, batch, bn, &bc);

    btree_range_iter_close(it);
    return NULL;
}
```

## Correctness notes

| Concern | Status |
|---|---|
| **First-match semantics** | Callback scans all records in the batch; keeps the min (ASC) or max (DESC) value via locked compare-and-update. After each flush the outer loop breaks if any match was found — subsequent btree entries can only have worse (or equal) values. Correct. |
| **Race on best/found** | Protected by `bc.mu` (PTHREAD_MUTEX_INITIALIZER). One mutex per `wfc_worker` invocation; doesn't affect cross-shard parallelism. |
| **Zero values** | `decode_index_key_to_double` (main loop) skips zeros before batching; `typed_field_to_double` (callback) also skips zeros — same semantics as today. |
| **Aggregate value** | Decoded from full record via `agg_tf->offset`. Equivalent to the btree-key decode per the comment at line ~22308. |
| **Deadlock (nested pools)** | `parallel_for` → `parallel_for_io` is safe — both pools have help-draining. The per-batch mutex (`bc.mu`) is only held during a tiny compare-and-update, never across a blocking call. |
| **sdb == NULL** | Falls back to `read_record_ref` + sequential direct callback. Correct. |
| **Budget/deadline mid-batch** | Main loop breaks before filling batch; remainder flushed (or skipped if found is already set). |
| **Stack size** | `batch[64][16]` = 1024 bytes + mutex. Within-safe. |

## Net diff

~+55 lines (65 new, ~10 replaced).
