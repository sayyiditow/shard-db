# Plan: Bitmap IGB + hbm support

**Date:** 2026-06-09  
**Branch:** `feat/bitmap-igb-hbm`  
**File:** `src/db/query.c`

## Problem

`group by active, avg(balance)` (bitmap group field + sum/avg/min/max agg spec) falls to
O_DIRECT full-scan (~5 s at 25 M records) because the disqualification at the top of
`cmd_aggregate`'s IGB block rejects `igb_group_uses_bitmap && igb_needs_hbm`. The bitmap
group field has no btree, so Pass 1 can't use the btree walk — but we can instead emit
hash16s from the bitmap itself and populate the hbk directly, then reuse the existing
indexed Pass 2 (agg-field btree walk) unchanged.

**Scope / expected benefit:** helps datasets where the hbk memory check passes (≤ ~6 M
records at `QUERY_BUFFER_MB=256`). At 25 M records the hbk footprint exceeds the budget
and the plan naturally falls through to O_DIRECT — no regression, no error.

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Never claim a step passed without the real build/test output.
- Locate every edit by the **quoted anchor text** below; if an anchor is not found
  exactly, stop and write `PLAN_NOTES.md` — do not guess.

---

## Task 1 — Narrow the bitmap IGB disqualification

**File:** `src/db/query.c`

**Anchor** (exact text to find):
```
    if (igb_eligible && igb_group_uses_bitmap &&
        (ctx.ngroups > 1 || igb_needs_hbm)) {
        /* Bitmap-only group_by with multi-field or sum/avg/min/max needs
           a btree walk + hash bucket map, which fails for bitmap-only
           group fields (no btree). Fall through to per-record scan.
           Criteria alone no longer disqualifies — the slot-level bitmap
           intersect path handles bitmap-only group_by + bitmap criteria. */
        igb_eligible = 0;
    }
```

Replace that entire block with:

```c
    if (igb_eligible && igb_group_uses_bitmap && ctx.ngroups > 1) {
        /* Multi-field group_by where the primary field is bitmap-only:
           no btree → secondary-map hash16 routing impossible.
           Single-field bitmap + hbm is handled in the bitmap IGB branch
           below (Phase 1b). */
        igb_eligible = 0;
    }
```

**Why:** Removes `igb_needs_hbm` from the disqualification. `ngroups > 1` stays because
multi-field IGB still needs a primary-field btree for hash16 routing into sec_maps.

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 2 — Add `BmHbkInsertCtx` and `bm_hbk_insert_cb`

**File:** `src/db/query.c`

**Anchor** (exact text to find, start of `BmDictCollectCtx`):
```
typedef struct {
    uint8_t  (*vals)[1024];
    size_t   *vlens;
    int      *n;
    int       cap;
} BmDictCollectCtx;
```

Insert the following block **immediately before** that anchor:

```c
/* Bitmap emit → hbk adapter.
   Called by bitmap_emit_for_shard once per matching record (via bm_emit_cb).
   Inserts (hash16 → bucket) into the caller's hbk.
   NOT thread-safe: must be called from a single thread or with external
   serialization. The bitmap IGB+hbm Phase 1b walks shards serially so
   this invariant holds. */
typedef struct {
    HashBktMap       *hbk;
    struct AggBucket *bucket;
} BmHbkInsertCtx;

static int bm_hbk_insert_cb(const char *value, size_t vlen,
                             const uint8_t *hash16, void *ctx) {
    (void)value; (void)vlen;
    BmHbkInsertCtx *bx = (BmHbkInsertCtx *)ctx;
    hbk_insert(bx->hbk, hash16, bx->bucket);
    return 0;
}

```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 3 — Add bitmap IGB Phase 1b (bitmap-only group field + hbm)

**File:** `src/db/query.c`

This task inserts a new `else if` branch inside the `if (igb_eligible)` block, right after
the closing `}` of the existing bitmap-only count-only branch.

**Anchor** (exact text; this is the comment+line that closes the count-only branch and
falls through to the btree path):
```
            /* Not slot-eligible → fall through to btree/scan path. */
        }
        const TypedField *gtf = ctx.group_tfs[0];
```

Replace with:

```c
            /* Not slot-eligible → fall through to btree/scan path. */
        }

        /* Phase 1b: bitmap-only group field + hbm (sum/avg/min/max agg specs
           on indexed non-varchar agg fields).

           Strategy:
             1. Collect unique bitmap dict values (reuse bm_n / bm_vals /
                bm_vlens already filled above — we re-run collection if
                bm_n==0 because this branch is also reached when ngroups==1
                && igb_needs_hbm even without going through the count-only
                block).
             2. Check hbk memory budget; bail to igb_skip if over.
             3. For each unique value create an AggBucket (count specs
                populated lazily in the bitmap walk; hbm used by Pass 2).
             4. Walk each shard's bitmap serially, emitting hash16→bucket
                into hbk via bm_hbk_insert_cb.
             5. Set hbk_ready=1 so the shared Pass 2 block (btree agg-field
                walk) runs normally and accumulates into the right buckets.

           Limit: hbk memory check gates this to ≤ ~6 M records at default
           QUERY_BUFFER_MB=256. At larger scale the else branch below falls
           to O_DIRECT as before. */
        if (igb_group_uses_bitmap && igb_needs_hbm) {
            /* Re-collect unique values if not already done by count-only branch. */
            uint8_t  bm_vals_h[256][1024];
            size_t   bm_vlens_h[256];
            int      bm_n_h = 0;
            const TypedField *gtf_bm_h = ctx.group_tfs[0];
            int      bm_dl_h = 0;
            for (int s = 0; s < sch.splits; s++) {
                if (query_deadline_tick(&dl, &bm_dl_h)) goto igb_skip;
                char bp[1024];
                bm_build_path(bp, sizeof(bp), db_root, object,
                              ctx.group_fields[0], s);
                BitmapShard *bms = bm_open(bp, 0, 0, 0, 0, 0);
                if (!bms) continue;
                BmDictCollectCtx dc = { .vals = bm_vals_h, .vlens = bm_vlens_h,
                                        .n = &bm_n_h, .cap = 256 };
                bm_iter_values(bms, bm_collect_uniq_cb, &dc);
                bm_close(bms);
            }
            if (bm_n_h == 0) { igb_done = 1; goto igb_skip; }

            /* hbk memory budget check (same formula as btree IGB path below). */
            {
                int live_h = get_live_count(db_root, object);
                if (live_h <= 0) live_h = 1024;
                size_t cap_h = 64;
                while (cap_h * 3 < (size_t)live_h * 4) cap_h <<= 1;
                size_t hbk_bytes_h = cap_h * sizeof(HashBktEntry);
                if (hbk_bytes_h > g_query_buffer_max_bytes / 2) goto igb_skip;
            }

            /* Initialise the shared hbk used by Pass 2. */
            {
                int live_h2 = get_live_count(db_root, object);
                if (live_h2 <= 0) live_h2 = 1024;
                HashBktMap hbk_bm = {0};
                if (hbk_init(&hbk_bm, (size_t)live_h2) != 0) goto igb_skip;

                /* Need sdb for kf path inside bitmap_emit_for_shard. */
                SlotcaskSchemaInfo bm_info = {
                    .splits   = sch.splits,
                    .slot_size = sch.slot_size,
                    .streams  = sch.streams
                };
                SlotcaskDb *bm_sdb = slotcask_registry_get(db_root, object,
                                                             &bm_info);
                if (!bm_sdb) { hbk_free(&hbk_bm); goto igb_skip; }

                int bm_aborted = 0;
                for (int v = 0; v < bm_n_h && !bm_aborted; v++) {
                    /* Decode display string for bucket key. */
                    char display_h[512];
                    if (decode_idx_to_buf(gtf_bm_h,
                                          bm_vals_h[v], bm_vlens_h[v],
                                          display_h, sizeof(display_h), 0) <= 0)
                        continue;
                    char *kvp_h[1] = { display_h };
                    AggBucket *bkt_h = agg_find_or_create(&ctx, kvp_h, 1,
                                                           NULL, 0);
                    if (!bkt_h) { bm_aborted = 1; break; }

                    /* Walk all bitmap shards for this value; emit hash16 → hbk. */
                    BmHbkInsertCtx bx = { .hbk = &hbk_bm, .bucket = bkt_h };
                    for (int s = 0; s < sch.splits && !bm_aborted; s++) {
                        if (query_deadline_tick(&dl, &bm_dl_h)) {
                            bm_aborted = 1; break;
                        }
                        bitmap_emit_for_shard(db_root, object,
                                              ctx.group_fields[0], s,
                                              bm_vals_h[v], bm_vlens_h[v],
                                              bm_hbk_insert_cb, &bx, bm_sdb);
                    }
                }

                if (bm_aborted) { hbk_free(&hbk_bm); goto igb_skip; }

                /* Hand the hbk off to Pass 2.  The Pass 2 block below checks
                   `hbk_ready` and uses the `hbk` local declared at the top of
                   the btree path.  We need to assign into that variable.
                   Declare hbk + hbk_ready at the top of the outer igb_eligible
                   block instead of deep in the btree-only path — see Task 4. */
                hbk      = hbk_bm;
                hbk_ready = 1;
                crit_ks   = NULL;  /* no criteria KeySet for bitmap path */
                /* Pass 2 runs below; skip btree Pass 1 by jumping past it. */
                goto igb_pass2;
            }
        }
        const TypedField *gtf = ctx.group_tfs[0];
```

**Important:** Task 4 below moves `hbk`, `hbk_ready`, and `crit_ks` declarations to the
top of the outer `if (igb_eligible)` block so that the `goto igb_pass2` target and the
Phase 1b assignment compile without "use before declaration" errors.

Build: `SKIP_TESTS=1 ./build.sh` — expected compile errors until Task 4; proceed.

---

## Task 4 — Hoist `hbk`, `hbk_ready`, `crit_ks` declarations; add `igb_pass2` label

**File:** `src/db/query.c`

### 4a — Hoist declarations

**Anchor** (exact text — this is the start of the btree IGB path, after the bitmap block):
```
        const TypedField *gtf = ctx.group_tfs[0];
        int n_idx_g = index_splits_for(sch.splits);
        HashBktMap hbk = {0};
        int hbk_ready = 0;
        if (igb_needs_hbm) {
```

Replace with:

```c
        const TypedField *gtf = ctx.group_tfs[0];
        int n_idx_g = index_splits_for(sch.splits);
        HashBktMap hbk = {0};
        int hbk_ready = 0;
        KeySet *crit_ks = NULL;
        if (igb_needs_hbm) {
```

**Note:** The old `KeySet *crit_ks = NULL;` declaration appears a few lines later (inside
the `if (tree)` block). Find it and remove it.

**Anchor for the old crit_ks declaration** (exact text):
```
        /* If we have criteria, build a candidate KeySet from the indexed
           plan and filter Pass 1 / Pass 2 by it. FP_PRIMARY_LEAF →
           build_keyset_from_leaf, FP_INTERSECT → intersect_indexed_leaves
           (with small-primary fallthrough since that path needs record-rematch
           which we don't do here), FP_UNION → build_or_keyset. On any
           failure, fall through to the scan path. */
        KeySet *crit_ks = NULL;
```

Replace with:

```c
        /* If we have criteria, build a candidate KeySet from the indexed
           plan and filter Pass 1 / Pass 2 by it. FP_PRIMARY_LEAF →
           build_keyset_from_leaf, FP_INTERSECT → intersect_indexed_leaves
           (with small-primary fallthrough since that path needs record-rematch
           which we don't do here), FP_UNION → build_or_keyset. On any
           failure, fall through to the scan path. */
```

(Remove only the `KeySet *crit_ks = NULL;` line; leave the comment intact.)

### 4b — Add `igb_pass2` label

**Anchor** (exact text — the comment that opens Pass 2):
```
        /* Pass 2 (only when there are non-count specs on indexed agg
           fields): walk each distinct agg field's btree ONCE, even when
           multiple specs target the same field (e.g. min(balance) +
           max(balance) — single walk, both specs updated per entry).
           Per agg-btree entry: one hbk_get + one decode + N accums where
           N is the number of specs sharing that field. */
        if (hbk_ready) {
```

Replace with:

```c
igb_pass2:
        /* Pass 2 (only when there are non-count specs on indexed agg
           fields): walk each distinct agg field's btree ONCE, even when
           multiple specs target the same field (e.g. min(balance) +
           max(balance) — single walk, both specs updated per entry).
           Per agg-btree entry: one hbk_get + one decode + N accums where
           N is the number of specs sharing that field. */
        if (hbk_ready) {
```

Build: `SKIP_TESTS=1 ./build.sh` — must now succeed.

---

## Task 5 — Build and test

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
| `group by active` (count-only, no criteria) | Unchanged: takes existing popcount path (line `if (ctx.ngroups == 1 && !igb_needs_hbm && igb_group_uses_bitmap)`) |
| `group by active, avg(balance)` ≤ ~6 M records | NEW: Phase 1b walks bitmap → hbk; Pass 2 walks balance btree; correct result |
| `group by active, avg(balance)` > ~6 M records | Falls through hbk budget check → `igb_skip` → O_DIRECT → correct (slower) |
| `group by active, active2` (multi-field bitmap) | Disqualified at `ngroups > 1` check; falls to O_DIRECT — no change |
| `group by age, avg(balance)` (btree group field) | `igb_group_uses_bitmap=0` → Phase 1b skipped; existing btree Pass 1 runs |
| Deadline exceeded inside Phase 1b | `bm_aborted=1` → `hbk_free` → `goto igb_skip` → O_DIRECT fallback |
| `slotcask_registry_get` returns NULL | `hbk_free` → `goto igb_skip` → O_DIRECT fallback |
| `hbk_init` fails (OOM) | `goto igb_skip` → O_DIRECT fallback |

## Verification query (manual, after bench is set up)

```bash
# group by active (bool bitmap) + avg(balance)
./shard-db query '{"mode":"aggregate","dir":"bench","object":"users",
  "aggregates":[{"fn":"count"},{"fn":"avg","field":"balance"}],
  "group_by":["active"]}'
# Expected: two rows (true / false) with counts summing to total records
# and avg(balance) values — same result as O_DIRECT path produces.
```
