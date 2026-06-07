# Plan: wfc_worker Batch-fetch (MIN/MAX + criteria fast path)

**Status:** Draft — reviewed but not yet implemented.

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

## Changes (all in `src/db/query.c`)

### 1. New struct `WfcBatchCtx` (before `WfcArg`, ~line 22760)

```c
typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    double           *best;
    int              *found;
    volatile int      stop;
} WfcBatchCtx;
```

### 2. New callback `wfc_batch_cb` (before `wfc_worker`)

```c
static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    if (__atomic_load_n(&bc->stop, __ATOMIC_ACQUIRE)) return -1;
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;
    double v;
    if (typed_field_to_double(bc->agg_tf,
                              (const uint8_t *)value + bc->agg_tf->offset,
                              &v)) {
        *bc->best = v;
        *bc->found = 1;
        __atomic_store_n(&bc->stop, 1, __ATOMIC_RELEASE);
    }
    return -1;
}
```

### 3. New helper `flush_wfc_batch` (before `wfc_worker`)

```c
static void flush_wfc_batch(SlotcaskDb *sdb, WfcArg *w,
                             uint8_t (*batch)[16], int bn,
                             WfcBatchCtx *bc) {
    if (sdb) {
        slotcask_bulk_resolve_and_fetch(sdb, batch, bn, bc, wfc_batch_cb);
    } else {
        for (int i = 0; i < bn; i++) {
            RecordRef rr;
            if (read_record_ref(w->db_root, w->object, w->sch,
                                batch[i], &rr) != 0) continue;
            wfc_batch_cb(batch[i], NULL, 0, rr.val, rr.vlen, bc);
            release_record_ref(&rr);
            if (__atomic_load_n(&bc->stop, __ATOMIC_ACQUIRE)) break;
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
    WfcBatchCtx bc = { w->tree, w->fs, w->agg_tf,
                        &w->best, &w->found, 0 };

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
            if (__atomic_load_n(&bc.stop, __ATOMIC_ACQUIRE)) break;
        }
    }
    if (bn > 0 && !__atomic_load_n(&bc.stop, __ATOMIC_ACQUIRE))
        flush_wfc_batch(sdb, w, batch, bn, &bc);

    btree_range_iter_close(it);
    return NULL;
}
```

## Correctness notes

| Concern | Status |
|---|---|
| **First-match semantics** | Callback sets `bc.stop = 1` and returns `-1`. `slotcask_bulk_resolve_and_fetch` stops processing further hashes. Remainder check guards against stale flush. |
| **Zero values** | `decode_index_key_to_double` (main loop) skips zeros before batching; `typed_field_to_double` (callback) also skips zeros — same semantics as today. |
| **Aggregate value** | Decoded from full record via `agg_tf->offset`. No parallel-array storage needed. |
| **Deadlock (nested pools)** | `parallel_for` → `parallel_for_io` is safe — both pools have help-draining. |
| **sdb == NULL** | Falls back to `read_record_ref` + direct callback per record. Same I/O as today. |
| **Budget/deadline mid-batch** | Main loop breaks; remainder flushed (or skipped if stop already set). |
| **Stack size** | `batch[64][16]` = 1024 bytes, within-safe. |

## Net diff

~+40 lines (55 new, ~15 replaced).
