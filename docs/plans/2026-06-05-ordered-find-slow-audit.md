# Audit: Two Ordered-Find Queries Still Slow After WS1-WS3 Planner Upgrade

**Date:** 2026-06-05  
**Auditor:** Workflow Leader  
**Status:** Root-cause analysis complete — no fix yet

## Executive Summary

Two production queries remain slow (~31s and ~24s) despite the WS1-WS3 planner upgrades. Both diverge from their intended fix paths at **plan selection / candidate sizing gates**, not in the executors themselves. The fixes' tests passed on synthetic data but didn't reach these prod plan shapes.

---

## Query 1: `find comments {by=lif} ORDER BY time DESC limit 25`

**Observed:** 31.6s, returns 30 rows  
**Intended fix (WS2 Part A):** D1 composite prefix scan with varchar byte-successor bound → ms  
**Actual plan path:** D1 composite fires, but varchar byte-successor bound is skipped → over-wide 0xff*4 bound

### Symptom Analysis

**Critical clue:** `limit 1 = 16ms` but `limit 25 = 31s`

This means:
- The walk **starts** at the right partition (lif's ~30 rows) quickly
- But it **doesn't stop** at the partition boundary — continues walking past lif's rows into the rest of the 38M-row index
- Points directly at the **upper bound not being applied** or being too wide

### Code Path Trace

#### Planner Path (plan_filter, query.c:13314)

1. **Leaf collection:** nL=1, leaves[0] = {field:"by", op:OP_EQUAL, value:"lif"}
2. **skip_est check:** 
   - skip_est_single = (nL==1) && (!fetching || !(order_by && order_by[0]))
   - fetching=1, order_by="time" → skip_est_single = false
   - skip_est = false → calls most_selective_indexed
3. **Seed selection:** prim=0 (only leaf)
4. **Cardinality estimate:** est[0] = card_est_leaf for by=lif (btree EQ walk)
   - Should return ~30 rows (lif's partition size)
5. **Plan kind:** FP_PRIMARY_LEAF, source_leaves[0] = leaves[0]

#### Order Overlay (query.c:13619)

6. **D1 check:** find_covering_composite(db_root, object, leaves, nL, "time")
   - Iterates leaves, checks op_caps(OP_EQUAL).composite_seed → true
   - Checks composite_index_exists(db_root, object, "by", "time")
   - **Assuming "by+time" composite exists** → returns cc=0

7. **D1 fires (cc=0):**
   - obr = NULL (no time leaf in criteria — only by=lif exists)
   - seed_broad = est[0].estimable && !leaf_is_selective(est[0], N)
     - est[0].k ≈ 30, N ≈ 38M
     - selectivity_budget(38M) = 38M / 8 = 4.75M
     - 30 <= 4.75M → leaf_is_selective returns true → seed_broad = false
   - skip_composite = seed_broad && !obr && has_sel_other
     - seed_broad = false → skip_composite = false
   - **fp.order = FP_ORDER_COMPOSITE, fp.order_range = NULL**

#### Executor Path (find_via_composite_prefix, query.c:11339)

8. **Seed encoding:**
   - seed_tf_sv = resolve_idx_field(fs->ts, "by")
   - **ROOT CAUSE:** resolve_idx_field returns NULL for "by"
     - Possible reasons:
       - driver_fs.ts is NULL (line 18111: `(driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL`)
       - "by" field is not in the typed schema (legacy untyped field?)
       - FieldSchema not loaded correctly
   - seed_tf_sv = NULL → falls to else branch (line 11507)
   - Else branch: `memset(buf_hi + len_lo_sv, 0xff, 4); len_hi = len_lo_sv + 4;`
   - This is the **OLD over-wide bound** (0xff*4) instead of varchar byte-successor

9. **Over-wide bound (line 11507):**
   - For "lif" = [0x6c, 0x69, 0x66] (3 bytes):
     - buf_hi = [0x6c, 0x69, 0x66, 0xff, 0xff, 0xff, 0xff] (7 bytes)
     - len_hi = 7
   - Bounds: lo = "lif" (3 bytes), hi = "lif\xff\xff\xff\xff" (7 bytes, max_excl=0)

10. **DESC walk (btree.c:2046 iter_next_desc):**
    - For entry "lif" ‖ encode(time) (11 bytes):
      - cmp_max = val_cmp(entry, 11, "lif\xff\xff\xff\xff", 7)
        - memcmp first 7 bytes: "lif" matches, then encode(time) vs [0xff, 0xff, 0xff, 0xff]
        - If encode(time) < 0xffffffff → entry < max → don't skip
      - cmp_min = val_cmp(entry, 11, "lif", 3)
        - memcmp first 3 bytes: match → length tiebreak: 11 > 3 → positive → entry > min → yield
    - Walk continues past "lif" partition into "lig", "lih", etc. until entry < "lif"
    - **This matches the symptom: starts at lif, doesn't stop**

### Root Cause

**resolve_idx_field returns NULL for "by" → seed_tf_sv is NULL → varchar byte-successor bound is skipped → falls back to over-wide 0xff*4 bound → walk continues past the target partition.**

The varchar byte-successor bound (WS2 Part A fix) is gated on `seed_tf_sv && seed_tf_sv->type == FT_VARCHAR`. When seed_tf_sv is NULL, the gate fails and the code falls through to the legacy 0xff*4 bound, which creates an over-wide range that lets non-matching prefixes through.

**Why the fix's tests passed:**
- Tests likely used a schema where "by" was properly registered in the typed schema
- Prod schema may have "by" as an untyped field (legacy?) or driver_fs.ts may not be loaded correctly

### Verification Steps Required

1. **Check if "by+time" composite exists:**
   ```bash
   cd /var/lib/shard-db
   ls -la <comments_dir>/comments/indexes/ | grep "by+time"
   ```

2. **Check if "by" field is in typed schema:**
   ```bash
   cat <comments_dir>/comments/fields.conf | grep "^by:"
   ```

3. **Add debug logging to find_via_composite_prefix:**
   ```c
   fprintf(stderr, "seed_tf_sv=%p, type=%d\n", seed_tf_sv, seed_tf_sv ? seed_tf_sv->type : -1);
   fprintf(stderr, "buf_hi len=%zu, bytes=", len_hi);
   for (size_t i = 0; i < len_hi; i++) fprintf(stderr, "%02x ", buf_hi[i]);
   fprintf(stderr, "\n");
   ```

4. **Check driver_fs.ts at line 18111:**
   ```c
   fprintf(stderr, "driver_fs.ts=%p, driver_fs.nfields=%d\n", driver_fs.ts, driver_fs.nfields);
   ```

---

## Query 2: `find stories {dead=false, deleted=false, type=job, time>=T(7d)} ORDER BY score DESC limit 25 cursor:null`

**Observed:** 22-26s, returns ~6 rows  
**Intended fix (WS2 Part C):** Small candidate set → cursor fetch+sort by score → ms  
**Actual plan path:** Cursor walk with 17k prefilter → score index walk → ~24s

### Symptom Analysis

The query returns ~6 rows but takes 22-26s. This suggests:
- The candidate set is much larger than the final match count
- The walk scans far through the score index before finding 25 matches
- Jobs are sparse in the score-ordered index

### Code Path Trace

#### Cursor Path Entry (query.c:17727)

The query has `cursor:null` which opts INTO cursor pagination (keyset cursor). This triggers the cursor path (line 17730), NOT the non-cursor ordered-find path.

**Critical:** The cursor path does NOT use fp.order from plan_filter. It always walks the order_by index directly (line 17960+). The plan is only used to build the prefilter KeySet.

#### Plan Generation (query.c:17811)

1. **plan_filter call:**
   ```c
   FilterPlan cursor_fp = plan_filter(tree, db_root, object, &driver_fs,
                                       sch.splits, cursor_N_live,
                                       order_by="score", 1 /*fetching*/, limit=25);
   ```

2. **Leaf collection:** nL=4
   - leaves[0] = {field:"dead", op:OP_EQUAL, value:"false"} → bitmap
   - leaves[1] = {field:"deleted", op:OP_EQUAL, value:"false"} → bitmap
   - leaves[2] = {field:"type", op:OP_EQUAL, value:"job"} → bitmap
   - leaves[3] = {field:"time", op:OP_GREATER_EQ, value:"T(7d)"} → btree

3. **most_selective_indexed:**
   - Bitmaps are deprioritized (line 13138-13153)
   - Non-bitmap leaf: time>=T (btree)
   - prim = 3 (time leaf)
   - est[3] = card_est_leaf for time>=T (btree range walk, capped)
     - Should return ~6 (the actual match count after all filters)

4. **Multi-leaf AND block (line 13508):**
   - n_indexed = 4 (all indexed)
   - all_bitmap = 0 (time>=T is btree)
   - n_selective = ? (depends on bitmap counts)
     - dead=false → ~2.8M (broad, not selective)
     - deleted=false → ~2.8M (broad, not selective)
     - type=job → ~17k (selective: 17k < 4.75M budget)
     - time>=T → ~6 (selective)
     - n_selective = 2 (type=job and time>=T)
   - n_indexed >= 2 → true
   - all_bitmap → false
   - !fetching → false (fetching=1)
   - fetching && prim_sel → prim_sel = true (time>=T is selective)
   - Falls through to single-seed block (line 13550)

5. **Single-seed block (line 13576):**
   - prim_it = IT_BTREE (time>=T)
   - prim_sel = true
   - fp.kind = FP_PRIMARY_LEAF
   - fp.source_leaves[0] = leaves[3] = time>=T
   - fp.n_source = 1
   - fp.postfilter_leaves = [dead, deleted, type] (all other leaves)

6. **Order overlay (line 13619):**
   - D1: find_covering_composite(leaves, nL, "score")
     - Checks each leaf for composite_seed eligibility
     - dead=false → OP_EQUAL → composite_seed=1 → checks "dead+score" composite
     - deleted=false → OP_EQUAL → composite_seed=1 → checks "deleted+score" composite
     - type=job → OP_EQUAL → composite_seed=1 → checks "type+score" composite
     - time>=T → OP_GREATER_EQ → composite_seed=0 → skip
     - **If "type+score" composite exists** → returns cc=2 (type leaf)
   - If cc=2:
     - leaves[cc]->op == OP_EQUAL → true
     - Look for order_by range/eq leaf: strcmp(leaves[i]->field, "score") == 0
     - No score leaf in criteria → obr = NULL
     - seed_broad = est[2].estimable && !leaf_is_selective(est[2], N)
       - est[2] is for type=job (bitmap, exact count ~17k)
       - 17k < 4.75M → leaf_is_selective returns true → seed_broad = false
     - skip_composite = false
     - **fp.order = FP_ORDER_COMPOSITE, fp.order_range = NULL**
     - **fp.source_leaves[0] = leaves[2] = type=job (NOT time>=T!)**

#### Prefilter KeySet Build (query.c:17814)

7. **build_keyset_from_plan:**
   ```c
   KeySet *cursor_prefilter_ks = build_keyset_from_plan(&cursor_fp, ...);
   ```
   - cursor_fp.kind = FP_PRIMARY_LEAF
   - cursor_fp.source_leaves[0] = type=job (bitmap)
   - build_keyset_from_leaf for type=job → bitmap KeySet
   - bm_popcount_for_crit → ~17k
   - 17k < g_ordered_find_keyset_max (100k) → build KeySet
   - **KeySet size = 17k**

#### Fetch+Sort Gate (query.c:17841)

8. **prefer_fetch_sort check:**
   ```c
   if (cursor_prefilter_ks &&
       prefer_fetch_sort(keyset_size(cursor_prefilter_ks),
                         cursor_N_live, offset, limit) && ...)
   ```
   - prefer_fetch_sort(17k, 5.6M, 0, 25):
     - candidates = 17000, N = 5600000, want = 25
     - 17000² = 289,000,000
     - 25 * 5,600,000 = 140,000,000
     - 289M > 140M → returns false → **walk path**

9. **Walk path (line 17960+):**
   - Walks score index DESC with cursor_prefilter_ks as prefilter
   - For each score entry, checks keyset_contains(cursor_prefilter_ks, hash)
   - 17k job candidates are sparse in the 5.6M-entry score index
   - Walk scans far before finding 25 matches
   - **This matches the observed 22-26s**

### Root Cause

**The D1 composite overlay changes the source leaf from time>=T (btree, ~6 rows) to type=job (bitmap, ~17k rows). The cursor prefilter KeySet is built from type=job (17k), not incorporating the time>=T filter. prefer_fetch_sort sees 17k instead of ~6 and chooses the walk path.**

The decision tree:
1. plan_filter selects time>=T as the most selective seed (prim=3, ~6 rows)
2. D1 overlay finds "type+score" composite → changes source to type=job (~17k rows)
3. Cursor path builds prefilter KeySet from type=job → 17k entries
4. prefer_fetch_sort(17k, 5.6M, 0, 25) → false → walk path
5. Walk scans score index with 17k prefilter → jobs sparse → 22-26s

**The fix needs to:**
- Prevent the D1 composite overlay from changing the source leaf when the composite seed is broader than the original seed, OR
- Incorporate the time>=T filter into the prefilter KeySet build, OR
- Use the original seed's cardinality (time>=T, ~6) for the prefer_fetch_sort decision instead of the composite seed's cardinality (type=job, ~17k)

### Why the Fix's Tests Passed

The WS2 Part C fix (cursor fetch+sort for small candidate sets) works when:
- The candidate set is genuinely small (< sqrt(want * N))
- The prefilter KeySet accurately reflects the final match count

But in prod:
- The D1 composite overlay changes the source leaf to a broader bitmap (17k)
- The prefilter KeySet is built from the bitmap, not the original selective btree leaf
- So the candidate estimate (17k) is much larger than the original seed's estimate (~6)

### Verification Steps Required

1. **Check if "type+score" composite exists:**
   ```bash
   cd /var/lib/shard-db
   ls -la <stories_dir>/stories/indexes/ | grep "type+score"
   ```

2. **Add debug logging to plan_filter:**
   ```c
   fprintf(stderr, "plan kind=%d, order=%d, n_source=%d\n", fp.kind, fp.order, fp.n_source);
   if (fp.n_source > 0)
       fprintf(stderr, "source[0]: field=%s, op=%d\n", fp.source_leaves[0]->field, fp.source_leaves[0]->op);
   ```

3. **Add debug logging after build_keyset_from_plan:**
   ```c
   if (cursor_prefilter_ks)
       fprintf(stderr, "cursor_prefilter_ks size=%zu\n", keyset_size(cursor_prefilter_ks));
   ```

4. **Check prefer_fetch_sort decision:**
   ```c
   int pfs = prefer_fetch_sort(keyset_size(cursor_prefilter_ks), cursor_N_live, offset, limit);
   fprintf(stderr, "prefer_fetch_sort(%zu, %zu, %d, %d) = %d\n",
           keyset_size(cursor_prefilter_ks), cursor_N_live, offset, limit, pfs);
   ```

---

## Summary of Divergence Points

| Query | Intended Path | Actual Path | Divergence Point | Root Cause |
|-------|---------------|-------------|------------------|------------|
| Q1 (by=lif) | D1 composite prefix scan with varchar byte-successor bound | D1 composite with over-wide 0xff*4 bound | resolve_idx_field returns NULL for "by" | seed_tf_sv is NULL → varchar byte-successor gate fails → falls back to 0xff*4 bound |
| Q2 (type=job) | Cursor fetch+sort with ~6 candidates | Cursor walk with 17k prefilter | D1 composite overlay changes source leaf | "type+score" composite exists → source changes from time>=T (~6) to type=job (~17k) → prefer_fetch_sort sees 17k → walk path |

## Recommended Next Steps

1. **For Q1:** Confirm resolve_idx_field returns NULL for "by" by adding debug logging. If confirmed, trace why (driver_fs.ts not loaded? "by" not in typed schema?). Fix: ensure the typed schema is loaded correctly, or handle NULL seed_tf_sv gracefully.

2. **For Q2:** Confirm "type+score" composite exists and the D1 overlay changes the source leaf. Fix options:
   - Prevent D1 overlay when composite seed is broader than original seed
   - Incorporate postfilter leaves into the prefilter KeySet build
   - Use the original seed's cardinality for prefer_fetch_sort decision

3. **For both:** Write integration tests that reproduce the prod plan shapes (not just synthetic data) to catch these divergence points in CI.

---

**End of Audit**
