# Planner upgrade — Workstream 1: broad-set materialization guard (fixes gap D)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop `build_keyset_from_plan` from materializing a huge candidate KeySet for a *broad* `FP_INTERSECT` (multi-leaf AND) only to have the caller throw it away. Today `FP_PRIMARY_LEAF`/`FP_BITMAP_SMALLER` probe cardinality cheaply and skip when broad; `FP_INTERSECT` does **not** — so `dead=false AND deleted=false` (≈5.6M ∩ 5.6M) on a `cursor:null` find spends ~1.4s building a ~5.6M intersection before the size check discards it. Add the same pre-flight guard to the intersect path.

**Why it matters across modes:** `build_keyset_from_plan` / `intersect_indexed_leaves` is shared by **find** (cursor + ordered-find prefilter), **count** (`PRIMARY_INTERSECT` index-only count), and **aggregate** (group/min-max candidate collection). One guard speeds up broad-AND queries in all three — this is a structural fix, not a find-only patch.

**Architecture:** In the `FP_INTERSECT` case of `build_keyset_from_plan` (src/db/query.c), when every source leaf is a bitmap eq/in, compute the intersection popcount via the existing `bm_popcount_intersect()` (word-level AND, no materialization, ~ms) and `return NULL` if it exceeds the cap — mirroring the `FP_PRIMARY_LEAF` guard directly above. To make the guard testable without inserting 100k rows, convert the `ORDERED_FIND_KEYSET_MAX` compile-time constant into a tunable global with a `TEST_BUILD` setter.

**Tech Stack:** C. Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <name>` / `run-all`.

---

## Execution rules

- **Branch off `main`** (which now has Path A + the order-walk-range-bounds cursor fix): `git checkout main && git pull && git checkout -b perf/planner-materialization-guard`. Leave work **uncommitted**; the reviewer commits.
- Locate edits by the **quoted anchor text**, not line numbers.
- Build `SKIP_TESTS=1 ./build.sh`; test via `./build/bin/shard-db-test`. **Never report green without pasting the real `# total: N passed, 0 failed`.**
- If any quoted anchor/symbol/signature differs from this plan, **STOP and write `PLAN_NOTES.md`** — do not guess.

---

## Evidence (prod, hn/stories, ~5.6M live)

| query | time |
|---|---|
| `dead=false AND deleted=false ORDER BY score`, **cursor:null** | **1431ms** ❌ |
| same, **no filter**, cursor:null | 5ms ✅ |
| same, **non-cursor** (walk via Path A) | 48ms ✅ |

Audit root cause: `build_keyset_from_plan` `FP_INTERSECT` (src/db/query.c) calls `intersect_indexed_leaves()` with **no pre-flight cardinality check**; the cursor/ordered-find callers only discard the result *after* it's built (`> ORDERED_FIND_KEYSET_MAX`). For broad bitmaps that's ~1.4s of wasted materialization.

---

## Reference anchors (verified)

- The existing guard to mirror — `build_keyset_from_plan`, `FP_PRIMARY_LEAF`/`FP_BITMAP_SMALLER` case:
```c
        SearchCriterion *leaf = fp->n_source > 0 ? fp->source_leaves[0] : NULL;
        if (leaf && pick_index_for_leaf(db_root, object, leaf) == IT_BITMAP) {
            TypedSchema *ts = load_typed_schema(db_root, object);
            const TypedField *tf = resolve_idx_field(ts, leaf->field);
            size_t pop;
            if (leaf->op == OP_EQUAL || leaf->op == OP_IN) {
                pop = bm_popcount_for_crit(db_root, object, sch->splits, leaf, tf);
            } else {
                pop = bm_popcount_generic_for_crit(db_root, object, leaf->field, sch->splits, leaf, tf);
            }
            if (pop > ORDERED_FIND_KEYSET_MAX) return NULL;
        }
        return build_keyset_from_leaf(db_root, object, sch->splits, leaf, dl);
```
- The case to fix, immediately below it:
```c
    case FP_INTERSECT: {
        int small_primary = 0;
        return intersect_indexed_leaves(db_root, object, sch->splits,
                                        (SearchCriterion **)fp->source_leaves,
                                        fp->n_source,
                                        dl, &small_primary);
    }
```
- The cheap probe (no materialization): `static size_t bm_popcount_intersect(const char *db_root, const char *object, int splits, SearchCriterion **leaves, int n_leaves, const TypedSchema *ts, QueryDeadline *dl)` — handles `n_leaves` in `[2, MAX_INTERSECT_LEAVES]`, parallel per-shard word-AND popcount.
- The constant: `#define ORDERED_FIND_KEYSET_MAX 100000`.
- Test-hook preamble model: `plan_filter_kind_for_test` (the `g_db_root` + `eff_root`/`bare` split + `parse_criteria_tree(json,&err)` + `load_schema` + `init_field_schema`), inside `#ifdef TEST_BUILD`.
- `build_keyset_from_plan` signature (from the cursor call site): `build_keyset_from_plan(&fp, db_root, object, &sch, &dl)`.

---

## File structure

- **Modify:** `src/db/query.c` — (1) `ORDERED_FIND_KEYSET_MAX` `#define` → tunable global + `TEST_BUILD` setter; (2) add the `FP_INTERSECT` broad-set guard; (3) add `plan_keyset_materializes_for_test` hook.
- **Create:** `src/test/cases/test_planner_materialization_guard.c`.

---

## Task 1: Make the cap tunable (so the guard is testable on small data)

**Files:** Modify `src/db/query.c`

- [ ] **Step 1.** Replace the constant. Search:
```c
#define ORDERED_FIND_KEYSET_MAX 100000
```
Replace with:
```c
/* Ordered-find prefilter cap: above this many candidates, materializing a
   KeySet loses to walking the order_by index + post-filtering. A tunable
   global (not a #define) so tests can lower it without inserting 100k rows. */
size_t g_ordered_find_keyset_max = 100000;
```
> Then every existing `ORDERED_FIND_KEYSET_MAX` reference becomes `g_ordered_find_keyset_max`. Find them all: `grep -n ORDERED_FIND_KEYSET_MAX src/db/query.c`. Replace each usage with `g_ordered_find_keyset_max`. (They're comparisons like `> ORDERED_FIND_KEYSET_MAX` and `keyset_size(...) > ORDERED_FIND_KEYSET_MAX`.) If the symbol is referenced in any other .c/.h file, STOP and note it.

- [ ] **Step 2.** Add a `TEST_BUILD` setter near the other test hooks (search for the `#ifdef TEST_BUILD` block defining `leaf_selective_for_test` and add inside/after it):
```c
#ifdef TEST_BUILD
void set_ordered_find_keyset_max_for_test(size_t v) { g_ordered_find_keyset_max = v; }
size_t get_ordered_find_keyset_max_for_test(void)   { return g_ordered_find_keyset_max; }
#endif
```

- [ ] **Step 3.** Build: `SKIP_TESTS=1 ./build.sh` → clean.

---

## Task 2: Add the FP_INTERSECT broad-set guard

**Files:** Modify `src/db/query.c`

- [ ] **Step 1.** Replace the `FP_INTERSECT` case (anchor quoted above) with:
```c
    case FP_INTERSECT: {
        /* Broad-set guard (parity with FP_PRIMARY_LEAF above): when every
           source leaf is a bitmap eq/in, the intersection cardinality is
           computable by word-level AND popcount with NO KeySet materialized
           (~ms). If it exceeds the cap, skip building — the only consumers
           (ordered-find prefilter sites) discard oversized keysets anyway and
           fall back to walk + per-record criteria_match. Building a ~5M
           intersection just to throw it away wastes seconds (gap D). Mixed
           bitmap/btree intersects have no cheap probe, so they fall through to
           build (the reactive post-build size check still bounds them). */
        int all_bitmap_eqin = fp->n_source >= 2;
        for (int i = 0; i < fp->n_source; i++) {
            SearchCriterion *lf = fp->source_leaves[i];
            if (!lf || pick_index_for_leaf(db_root, object, lf) != IT_BITMAP ||
                (lf->op != OP_EQUAL && lf->op != OP_IN)) {
                all_bitmap_eqin = 0;
                break;
            }
        }
        if (all_bitmap_eqin) {
            TypedSchema *ts = load_typed_schema(db_root, object);
            size_t pop = bm_popcount_intersect(db_root, object, sch->splits,
                                               (SearchCriterion **)fp->source_leaves,
                                               fp->n_source, ts, dl);
            if (pop > g_ordered_find_keyset_max) return NULL;
        }
        int small_primary = 0;
        return intersect_indexed_leaves(db_root, object, sch->splits,
                                        (SearchCriterion **)fp->source_leaves,
                                        fp->n_source,
                                        dl, &small_primary);
    }
```
> Verify `bm_popcount_intersect`, `load_typed_schema`, `pick_index_for_leaf`, `IT_BITMAP`, `OP_EQUAL`, `OP_IN`, `MAX_INTERSECT_LEAVES`, and `sch->splits` are all already in scope here (they're used elsewhere in this file/function). If `bm_popcount_intersect` is defined *below* `build_keyset_from_plan`, add a forward declaration near the top of the planner-helpers section (same pattern the file uses for other forward decls). If anything is off, STOP and write `PLAN_NOTES.md`.

- [ ] **Step 2.** Build: `SKIP_TESTS=1 ./build.sh` → clean.

---

## Task 3: Test hook + RED→GREEN test

**Files:** Modify `src/db/query.c`; Create `src/test/cases/test_planner_materialization_guard.c`

- [ ] **Step 1.** Add the hook (inside a `#ifdef TEST_BUILD` block near the other hooks), modelled on `plan_filter_kind_for_test`'s preamble:
```c
#ifdef TEST_BUILD
/* Returns 1 if build_keyset_from_plan materializes a KeySet for `criteria`
   (count/no order_by plan), 0 if it skips (returns NULL → caller walks). */
int plan_keyset_materializes_for_test(const char *db_root, const char *object,
                                      const char *criteria_json) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) { size_t d=(size_t)(slash-object);
        snprintf(eff_root,sizeof(eff_root),"%s/%.*s",db_root,(int)d,object);
        snprintf(bare,sizeof(bare),"%s",slash+1);
    } else { snprintf(eff_root,sizeof(eff_root),"%s",db_root);
        snprintf(bare,sizeof(bare),"%s",object); }
    const char *err = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return -1;
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N,
                                NULL /*order_by*/, 0 /*fetching*/, 0 /*limit*/);
    QueryDeadline dl = { now_ms_coarse(), 0 /*no timeout*/, 0 };
    KeySet *ks = build_keyset_from_plan(&fp, eff_root, bare, &sc, &dl);
    int materialized = (ks != NULL);
    if (ks) keyset_free(ks);
    free_criteria_tree(tree);
    return materialized;
}
#endif
```
> Mirror `plan_filter_kind_for_test` for the exact preamble helpers (`now_ms_coarse`, `QueryDeadline` field order, `keyset_free`). Adjust if they differ; note in `PLAN_NOTES.md`.

- [ ] **Step 2.** Create the test (idioms from `test_composite_selectivity_guard.c`):
```c
/* Broad bitmap-AND must NOT materialize a prefilter keyset (gap D). With the
 * cap lowered, a broad `a=x AND b=y` intersection should be skipped (NULL →
 * walk), while a selective one still materializes. See
 * docs/plans/2026-06-04-planner-materialization-guard.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern int plan_keyset_materializes_for_test(const char *db_root, const char *object,
                                             const char *criteria_json);
extern void set_ordered_find_keyset_max_for_test(size_t v);

static int test_planner_materialization_guard(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"m\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"m\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:varchar:8\",\"b:varchar:8\"],"
        "\"indexes\":[\"a:bitmap\",\"b:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 300 rows: a=x for all; b=y for 290 (broad), b=z for 10 (selective). */
    for (int i = 0; i < 300; i++) {
        char req[256];
        const char *b = (i < 10) ? "z" : "y";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"m\",\"object\":\"ob\",\"key\":\"k%03d\","
            "\"value\":{\"a\":\"x\",\"b\":\"%s\"}}", i, b);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* Lower the cap so 290 counts as "broad". */
    set_ordered_find_keyset_max_for_test(50);

    /* Broad: a=x (300) AND b=y (290) → intersection 290 > 50 → must SKIP. */
    ASSERT_EQ_INT(plan_keyset_materializes_for_test(env.db_root, "m/ob",
        "[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},{\"field\":\"b\",\"op\":\"eq\",\"value\":\"y\"}]"),
        0, "broad bitmap AND skips materialization (returns NULL)");

    /* Selective: a=x (300) AND b=z (10) → intersection 10 <= 50 → materializes. */
    ASSERT_EQ_INT(plan_keyset_materializes_for_test(env.db_root, "m/ob",
        "[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},{\"field\":\"b\",\"op\":\"eq\",\"value\":\"z\"}]"),
        1, "selective bitmap AND still materializes");

    /* Correctness: the selective AND still returns the right rows via a real find. */
    set_ordered_find_keyset_max_for_test(100000);  /* restore */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"m\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":\"z\"}]}", &resp);
    ASSERT_CONTAINS(resp, "10", "selective AND count = 10");
    free(resp); resp=NULL;
    /* And the broad AND find returns correct rows whether or not it materialized. */
    set_ordered_find_keyset_max_for_test(50);
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"m\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":\"y\"}],"
        "\"order_by\":\"key\",\"order\":\"asc\",\"limit\":3,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "broad AND find still returns rows (walk path)");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-planner-materialization-guard", test_planner_materialization_guard)
```
> If `order_by:"key"` isn't valid for cursor (key may not be an indexable order field), use an indexed field instead — add an `int` field with a btree and order by it. Adjust and note it. The materialization assertions (the RED→GREEN core) don't depend on the find.

- [ ] **Step 3.** Register in `build.sh` (add `src/test/cases/test_planner_materialization_guard.c \` to the test source list, next to the other `test_planner_*` entries).

- [ ] **Step 4.** Build, then run the new test:
`SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-planner-materialization-guard`
Expected: PASS. To confirm it's a real RED→GREEN, temporarily revert Task 2 and observe the "broad … skips" assertion FAIL (materializes=1). Re-apply.

---

## Task 4: Full suite

- [ ] **Step 1.** `./build/bin/shard-db-test run-all` → `# total: N passed, 0 failed`. Paste it. The intersect/count/cursor/ordered-find cases are the ones to watch (`test_bm_intersect_count`, `test_and_intersection`, `test_find_cursor`, `test_order_walk_range_bounds`).
- [ ] **Step 2.** `git diff --stat` should show only `src/db/query.c`, `build.sh`, and the new test file. Hand back uncommitted.

---

## Task 5: Scale validation (acceptance — run by the reviewer on prod, not in the suite)

The suite proves correctness on small data; this proves the *perf* win at scale on the 25M `users` object (a real broad dataset). Reviewer runs against the deployed daemon **after** building+shipping this change:

- [ ] Pick two broad bitmap-indexed fields on `users` (or any object with ≥2 bitmaps; if `users` lacks them, use `hn/stories` `dead`+`deleted`). Time a `cursor:null` ordered find with a broad AND before vs after:
  - Before (current prod): `dead=false AND deleted=false ORDER BY score limit 25 cursor:null` ≈ 1.4s
  - After: expect **< ~50ms** (skips the keyset, walks the order index)
- [ ] Confirm result rows are identical before/after (correctness).
- [ ] Confirm a broad-AND `count` (`PRIMARY_INTERSECT`) is unaffected or faster (it shares `build_keyset_from_plan`; the guard returns NULL → count falls back to its scan path — verify count still returns the correct number and isn't slower for the broad case).

> NOTE the count caveat: for `count`, returning NULL from `build_keyset_from_plan` on a broad intersect means count loses the index-only fast path and may fall back to a scan. Verify this doesn't *regress* broad-AND count. If it does, the guard should be scoped to the ordered-find (fetching) callers only — i.e. gate the new guard on a flag/context indicating an ordered-find prefilter, not count. Record findings; this is the one real risk in this change.

---

## Self-review checklist

1. All `ORDERED_FIND_KEYSET_MAX` references converted to `g_ordered_find_keyset_max` (grep returns only the global definition + the setter).
2. The `FP_INTERSECT` guard only triggers for **all-bitmap eq/in** leaves; mixed intersects fall through unchanged.
3. Guard returns NULL → callers walk + post-filter → **results unchanged** (correctness rests on the post-filter, verified by Task 3's find/count + Task 5).
4. **Count regression check (Task 5):** broad-AND `count` not slower. If it is, scope the guard to ordered-find callers.
5. Suite green; `git diff --stat` clean.

---

## Roadmap context (not in this plan)

This is **Workstream 1** of the planner upgrade. Next, as separate sequenced plans (they share `query.c`):
- **WS2 — op-capability table:** replace the 9 scattered op-gates with one shared table all paths consult; then extend it to close coverage gaps (notably `OP_IN` seeding a composite via k-way merge — the "in-fold").
- **WS3 — verify + scale-prove:** confirm/fix issue-C (bitmap `OP_IN` card-est), and a standing acceptance test proving broad-filter find/count/aggregate stay bounded against the 25M `users` object.
