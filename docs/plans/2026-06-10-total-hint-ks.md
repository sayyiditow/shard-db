# Plan: fix `total:null` for FP_PRIMARY_LEAF + post-filter queries (icontains + criteria)

**Date:** 2026-06-10 (v2 — fix bitmap-only overcount)
**Branch:** `feat/total-hint-ks`
**File:** `src/db/query.c`

## Problem (original)

`fp_compute_total` returns 0 / leaves `*out_null = 1` for any `FP_PRIMARY_LEAF` plan that has
`n_postfilter > 0`:

```c
if (fp->n_source == 0 || fp->n_postfilter != 0) return 0;
```

A query like:
```json
{"criteria":[{"field":"dead","op":"eq","value":"false"},
             {"field":"deleted","op":"eq","value":"false"},
             {"field":"title","op":"icontains","value":"postgres"}],
 "order_by":"score","cursor":null,"total":"true"}
```

routes as `FP_PRIMARY_LEAF` (trigram drives) with `n_postfilter=2` (dead, deleted) → total is
always `null` even though the data is correct.

## Problem (regression discovered during testing)

After implementing the hint_ks path, the count returned incorrect values (e.g. 11 instead of 2)
when all post-filter leaves had bitmap indexes. Root cause: `shard_count_worker` at line 10360-10362
takes an index-only shortcut when `all_postfilters_are_bm = 1`, counting entries without fetching
the record. This is correct for btree/bitmap primary sources (exact matches), but **wrong for
trigram** (approximate — returns false positives from trigram collisions). The `icontains`
re-verification via `criteria_match_tree` is skipped.

**Fix:** add `no_bm_shortcut` flag to `ShardCountCtx`. When set, `classify_bm_postfilters` forces
`all_postfilters_are_bm = 0`, preventing the skip-record-fetch shortcut. The bitmap **rejection**
path (line 10358-10359) still works, so entries that fail `dead eq false` / `deleted eq false`
are still skipped early without a record fetch.

## Key insight — reuse `cursor_prefilter_ks`

The cursor path already builds the trigram `KeySet` before the btree walk (stored as
`cursor_prefilter_ks`). Passing it as a hint to `fp_compute_total` means the trigram index is
**not re-walked** for the count: we verify the already-in-memory candidates against the tree.

For non-cursor call sites (`cursor_prefilter_ks` is NULL or not applicable) the function builds
a fresh KeySet from the primary leaf — same correctness, small extra walk, but those paths are
rare in practice.

## Problem (v3 — streaming find path overcount)

After implementing Tasks 1-4 and fixing `fp_compute_total`'s hint_ks path, the
`count` command returns correct results. But the **find+total** path (e.g.
`"total":"true"` with `"cursor":null` in a find query) still overcounts.

Root cause: the streaming find path (`idx_find_streaming` at line ~19427) for
`FP_PRIMARY_LEAF`/`FP_BITMAP_SMALLER` computes the total via:
```c
find_total = idx_count_for_leaf(db_root, object, &sch, &driver_fs,
                                 primary, &dl);
```
`idx_count_for_leaf` returns the count for the primary leaf only — **without
applying post-filters**. When the primary leaf is trigram and post-filters
include `dead=false` / `deleted=false`, the trigram count (25) includes records
that fail the bitmap post-filters (true count should be 21). The fallback call
to `fp_compute_total` at line 19628 is **never reached** because
`find_total_null` was already set to 0 at line 19443.

**Fix:** guard the `idx_count_for_leaf` call with `fp.n_postfilter == 0`. When
post-filters exist, leave `find_total_null = 1` so the code falls through to
`fp_compute_total` at line 19628, which correctly applies all post-filters (and
already has the `no_bm_shortcut` fix from Task 2).

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Locate every edit by the **quoted anchor text**; if not found exactly, stop
  and write `PLAN_NOTES.md` — do not guess.
- Never claim a step passed without pasting real output.

---

## Task 1 — Add `no_bm_shortcut` to `ShardCountCtx` and `parallel_indexed_count`

### 1a. Add field to struct

**Anchor:**
```
    int all_postfilters_are_bm;                       // 1 = Case A1 (index-only), 0 = Case A2 or none
    SearchCriterion *bm_criteria[MAX_INTERSECT_LEAVES]; // bitmap post-filter SearchCriterion*
```

Insert after `all_postfilters_are_bm` line:

```c
    int no_bm_shortcut;                               // 1 = prevent all_postfilters_are_bm shortcut (trigram primary)
    SearchCriterion *bm_criteria[MAX_INTERSECT_LEAVES]; // bitmap post-filter SearchCriterion*
```

### 1b. Apply `no_bm_shortcut` in `classify_bm_postfilters`

**Anchor:**
```
    ctx->all_postfilters_are_bm = (n_bm > 0 && n_bm == fp->n_postfilter);
}
```

Insert after `ctx->all_postfilters_are_bm = ...` line:

```c
    if (ctx->no_bm_shortcut) ctx->all_postfilters_are_bm = 0;
```

### 1c. Add parameter to `parallel_indexed_count` signature

**Anchor:**
```
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl,
                                     const FilterPlan *fp) {
    int group_starts[1024], group_sizes[1024];
```

Replace with:

```c
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl,
                                     const FilterPlan *fp,
                                     int no_bm_shortcut) {
    int group_starts[1024], group_sizes[1024];
```

### 1d. Thread `no_bm_shortcut` to workers

Inside the for loop in `parallel_indexed_count`, after the `classify_bm_postfilters` call:

**Anchor:**
```
        classify_bm_postfilters(&workers[g], fp, db_root, object, fs);
    }
```

Insert after the `classify_bm_postfilters` line:

```c
        workers[g].no_bm_shortcut = no_bm_shortcut;
```

Build: `SKIP_TESTS=1 ./build.sh` — will fail (callers not updated yet; proceed to Task 2).

---

## Task 2 — Update our hint_ks path in `fp_compute_total`

### 2a. Detect trigram primary and pass `no_bm_shortcut`

**Anchor (the `parallel_indexed_count` call in the hint_ks post-filter block):**
```
            if (entries && tree) {
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp);
                if (!dl->timed_out) *out_null = 0;
```

Replace with:

```c
            if (entries && tree) {
                int no_bm_shortcut =
                    (pick_index_for_leaf(db_root, object,
                                         fp->source_leaves[0])
                     == IT_TRIGRAM);
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp,
                                           no_bm_shortcut);
                if (!dl->timed_out) *out_null = 0;
```

Build: `SKIP_TESTS=1 ./build.sh` — will still fail (other callers not updated yet).

---

## Task 3 — Update all other `parallel_indexed_count` call sites

All 6 call sites need the new `no_bm_shortcut` argument added.

### Call site 1 — cmd_count negation/positive trigram (line ~16435)

**Anchor:**
```
                    pos_count = parallel_indexed_count(db_root, object, &sch,
                                                       entries, (int)n,
                                                       &pos_leaf, &fs, &dl, NULL);
```

Add `0` (fp is NULL, flag irrelevant):
```c
                    pos_count = parallel_indexed_count(db_root, object, &sch,
                                                       entries, (int)n,
                                                       &pos_leaf, &fs, &dl, NULL, 0);
```

### Call site 2 — cmd_count single-leaf IT_TRIGRAM (line ~16495)

**Anchor:**
```
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp);
```

Add `1` (trigram primary — fix same latent bug):
```c
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp, 1);
```

### Call site 3 — cmd_count multi-leaf IT_TRIGRAM (line ~16548)

**Anchor:**
```
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp);
```

Add `1` (trigram primary — fix same latent bug):
```c
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp, 1);
```

### Call site 4 — cmd_count multi-leaf non-trigram (line ~16580)

**Anchor:**
```
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl, &fp);
```

Add `0` (btree/bitmap primary — exact matches, shortcut is fine):
```c
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl, &fp, 0);
```

### Call site 5 — idx_count_for_leaf IT_TRIGRAM (line ~16725)

**Anchor:**
```
        size_t cnt = parallel_indexed_count(db_root, object, sch,
                                            entries, (int)n,
                                            &leaf_node, (FieldSchema *)fs, dl, NULL);
```

Add `0` (fp is NULL, flag irrelevant):
```c
        size_t cnt = parallel_indexed_count(db_root, object, sch,
                                            entries, (int)n,
                                            &leaf_node, (FieldSchema *)fs, dl, NULL, 0);
```

### Call site 6 — fp_compute_total FP_INTERSECT small_primary (line ~18069)

**Anchor:**
```
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp);
```

Add `0` (preserve existing behavior for FP_INTERSECT):
```c
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp, 0);
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed with zero errors.

---

## Task 4 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`. Paste actual output.

Verify manually with the server running (from the cloned repo root):
```bash
./shard-db server &
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"postgres"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```

Expected: `"total"` is the exact count (non-null integer), not an overcount.

---

## Invariants and edge cases

| Case | Expected |
|---|---|
| icontains + dead/deleted + cursor=null | total = exact count (hint_ks reused, no re-walk, no_bm_shortcut prevents bitmap-only shortcut) |
| icontains + dead/deleted + cursor=non-null | total = exact count (same; hint_ks still valid) |
| Clean FP_PRIMARY_LEAF (n_postfilter=0) | unchanged — uses idx_count_for_leaf fast path |
| FP_INTERSECT | unchanged — existing behaviour kept (no_bm_shortcut=0) |
| FP_FULL_SCAN | unchanged — still returns null |
| hint_ks=NULL at D1/D3 call sites | builds fresh KeySet from leaf, no_bm_shortcut triggers only for trigram |
| btree primary + bitmap post-filters | all_postfilters_are_bm shortcut preserved (exact matches, correct) |
| bitmap primary + bitmap post-filters | all_postfilters_are_bm shortcut preserved (exact matches, correct) |
| trigram primary + non-bitmap post-filters | no_bm_shortcut doesn't matter (n_bm_postfilter == 0, all entries fetched anyway) |
| Timed-out query | dl->timed_out checked at entry and inside parallel_indexed_count — safe |
| hint_ks owned by caller | fp_compute_total borrows only (built=0 → no free) — no double-free |
| FP_PRIMARY_LEAF + n_postfilter>0 streaming find path | leaves find_total_null=1 → falls through to fp_compute_total (correct post-filtered total) |
| FP_PRIMARY_LEAF + n_postfilter=0 streaming find path | idx_count_for_leaf fast path unchanged (correct — no post-filters to miss) |

---

## Task 5 — Fix streaming find path `idx_count_for_leaf` overcount

### 5a. Guard `idx_count_for_leaf` total call with `n_postfilter == 0`

**Anchor:**
```
        if (want_total && fp.kind != FP_INTERSECT) {
            find_total = idx_count_for_leaf(db_root, object, &sch, &driver_fs,
                                             primary, &dl);
            if (!dl.timed_out) find_total_null = 0;
        }
```

Replace the opening `if` guard:

```c
        if (want_total && fp.kind != FP_INTERSECT && fp.n_postfilter == 0) {
            find_total = idx_count_for_leaf(db_root, object, &sch, &driver_fs,
                                             primary, &dl);
            if (!dl.timed_out) find_total_null = 0;
        }
```

When post-filters exist, `find_total_null` stays 1, and line 19628 calls
`fp_compute_total` which applies the full criteria tree (including post-filters)
via the hint_ks path (with `no_bm_shortcut` from Task 2).

### 5b. Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Verify manually:
```bash
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"postgres"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```
Expected: `"total":` is the exact count (21, not 25).

Also verify the user's exact query:
```bash
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"hawking"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```
Expected: `"total":` is the exact count (21, not 25).

---

## Task 6 — Fix `no_bm_shortcut` override ordering bug (v4)

### Root cause

`classify_bm_postfilters` is called at line 10519 when `ctx->no_bm_shortcut` is still 0
(struct was zeroed by `calloc`). The guard inside the function:

```c
if (ctx->no_bm_shortcut) ctx->all_postfilters_are_bm = 0;
```

never fires because `ctx->no_bm_shortcut` is set on the very next line (10520) — too late.
Result: `all_postfilters_are_bm` stays 1 for trigram queries → bitmap shortcut → icontains
never re-verified → count returns 25 instead of 21.

### 6a. Apply override after setting the field

**Anchor (both lines must be present together):**
```
        classify_bm_postfilters(&workers[g], fp, db_root, object, fs);
        workers[g].no_bm_shortcut = no_bm_shortcut;
    }
```

Replace with:

```c
        classify_bm_postfilters(&workers[g], fp, db_root, object, fs);
        workers[g].no_bm_shortcut = no_bm_shortcut;
        if (no_bm_shortcut) workers[g].all_postfilters_are_bm = 0;
    }
```

### 6b. Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Verify the hawking query returns the exact count:

```bash
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"hawking"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```

Expected: `"total":21` (not 25). The `cursor` field must be `null` (all matches fit in one page).

The `if (ctx->no_bm_shortcut) ctx->all_postfilters_are_bm = 0;` line already inside
`classify_bm_postfilters` is now dead code but harmless — leave it in place.
