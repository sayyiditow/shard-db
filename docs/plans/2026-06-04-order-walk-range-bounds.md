# Planner: bound every order-by-index walk by the order_by-field range criteria

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a find is ordered by a field that also carries a range criterion (`time >= T`, `time <= T`, `between`), make the order-by-index walk **start/stop at that range** instead of scanning the whole index. Today two of the three order-walk executors hardcode full-range bounds (`""` … `\xff\xff\xff\xff`), so a *sparse* windowed query (matches < limit) runs off the end of the window through the entire index → 30 s timeout (observed: `title starts "Ask HN" AND time>=T ORDER BY time` over hn/stories, cursor:null, returns 9 rows in 30 s).

**Architecture:** Add one shared helper `order_walk_bounds()` (src/db/query.c) that scans the top-level AND leaves for range/eq criteria on the `order_by` field and produces encoded `[lo, hi]` walk bounds (mirroring the `order_range` fold that `find_via_composite_prefix` already does for the D1 composite path). Wire it into the two executors that currently hardcode bounds: the **cursor walk** (in `cmd_find`) and the **non-cursor D3 walk** (`find_via_order_index_walk`). The per-record `criteria_match_tree` post-filter stays in both — so the bound is a pure walk-narrowing **optimization** and correctness cannot regress even if a bound is loose or absent.

**Tech Stack:** C (shard-db daemon). Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <name>` / `run-all`. Tests are C cases under `src/test/cases/test_*.c`, registered via `TEST_REGISTER`, each fork-execs its own daemon.

---

## Execution rules (read before starting)

- **Sequence after the Path A plan.** This edits the same file (`src/db/query.c`) as `docs/plans/2026-06-04-planner-broad-ordered-walk.md`. If that plan's branch isn't merged yet, branch off the **same base** it used (or off `main` once it's merged) to avoid conflicts. Coordinate so the two don't run concurrently.
- **Branch off `main`** (or the merged Path A tip): `git checkout main && git pull && git checkout -b perf/order-walk-range-bounds`. Leave the work **uncommitted** when done; the reviewer commits.
- **Locate edits by searching the quoted anchor text**, not line numbers — line numbers drift.
- **Build:** `SKIP_TESTS=1 ./build.sh`. **Test:** `./build/bin/shard-db-test run <name>` / `run-all`. `list` shows names.
- **Never claim a step passed without pasting the real command output.** The suite prints `# total: N passed, 0 failed`.
- **If a quoted anchor/symbol isn't where this plan says**, stop and write `PLAN_NOTES.md` instead of guessing. Do not invent function names, struct fields, or macros.

---

## Problem & evidence (prod, hn/stories, N≈5.6M)

`type` bitmap; `score`/`time`/`title` btrees; `type+time`, `type+score` composites. Window T = ~1 day, contains only **9** "Ask HN" posts; limit 25.

| Variant | Path | Time | Rows |
|---|---|---|---|
| `... title starts "Ask HN" AND time>=T ORDER BY time`, **cursor:null** | cursor walk | **30 s** ❌ | 9 |
| same, **no** `title starts` | cursor walk | 118 ms | 26 |
| same, **no cursor** | D1 composite (`type+time`, folds `time>=T`) | **46 ms** ✅ | 9 |

The non-cursor D1 path is fast because it folds `time>=T` into the composite walk bounds. The cursor path treats `time>=T` as a post-filter only, with its walk lower-bound hardcoded to `""` — so when fewer than `limit` rows match, the walk doesn't stop at T and runs through all 5.6M entries (made worse by the `time>=T` prefilter keyset membership-check on every step). The non-cursor **D3** walk (`find_via_order_index_walk`) has the identical hardcoded bounds and the identical latent bug for any query the planner routes to it (e.g. a saturated btree seed, or — once the Path A plan lands — a broad filter).

---

## The three order-walk executors (only one bounds itself today)

| Executor | Where | Bounds by order_by range? |
|---|---|---|
| D1 composite | `find_via_composite_prefix` | **Yes** — `order_range` fold (the pattern this plan copies) |
| D3 single-index | `find_via_order_index_walk` | **No** — hardcoded `""` … `\xff\xff\xff\xff` |
| Cursor walk | `cmd_find` cursor block | **No** — hardcoded `""` (desc lower) / `\xff\xff\xff\xff` (asc upper) |

This plan fixes D3 and the cursor walk to match D1.

### Reference: the encoding pattern to copy (already in `find_via_composite_prefix`)

```c
const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
const char *lowv = NULL; int low_excl = 0;
const char *highv = NULL; int high_excl = 0;
switch (order_range->op) {
    case OP_GREATER_EQ: lowv = order_range->value; break;
    case OP_GREATER:    lowv = order_range->value; low_excl = 1; break;
    case OP_LESS_EQ:    highv = order_range->value; break;
    case OP_LESS:       highv = order_range->value; high_excl = 1; break;
    case OP_EQUAL:      lowv = highv = order_range->value; break;
    case OP_BETWEEN:
        lowv  = order_range->value;  low_excl  = order_range->min_exclusive;
        highv = order_range->value2; high_excl = order_range->max_exclusive;
        break;
    default: break;
}
/* encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el); ... */
```

The walk signature is `btree_idx_walk_ordered(db_root, object, field, splits, lo, lo_len, lo_excl, hi, hi_len, hi_excl, desc, cb, ctx)` — params 7 and 10 are the low/high **exclusivity** flags.

---

## File structure

- **Modify:** `src/db/query.c`
  - Add `OrderWalkBounds` struct + `order_walk_bounds()` helper (place it **after** `find_via_composite_prefix` and **before** `find_via_order_index_walk`, so both D3 and `cmd_find` can call it).
  - Rewire bounds in `find_via_order_index_walk`.
  - Rewire bounds in the `cmd_find` cursor block (desc + asc).
  - Add a test-only scan counter (`g_order_walk_scanned`) incremented in `order_index_walk_cb` and `cursor_find_cb`, plus extern accessors next to `plan_filter_kind_for_test`.
  - Add `order_walk_bounds_for_test` extern hook next to `plan_filter_kind_for_test`.
- **Create:** `src/test/cases/test_order_walk_range_bounds.c` — unit test of the helper + RED→GREEN scan-count tests for cursor and D3 + correctness checks.

No header changes (`order_walk_bounds`/`OrderWalkBounds` are file-local; the externs are declared in the test file).

---

## Task 1: The `order_walk_bounds` helper

**Files:** Modify `src/db/query.c`

- [ ] **Step 1: Insert the struct + helper after `find_via_composite_prefix`.**

`find_via_composite_prefix` ends with `return ctx.printed;` followed by `}` and then a comment `/* Forward declarations (defined later in this file). */`. Insert **between** that closing `}` and the forward-declarations comment:

```c
/* Encoded order-by walk bounds derived from the order_by-field range/eq leaves.
 * Defaults to the full range ("" .. 0xffffffff). Only TOP-LEVEL AND leaves are
 * consulted (OR/nested branches can't bound the walk). The walk's per-record
 * criteria_match_tree still runs, so a loose/absent bound never changes the
 * result set — it only narrows how far the index is walked. */
typedef struct {
    uint8_t lo[1024]; size_t lo_len; int lo_excl;
    uint8_t hi[1024]; size_t hi_len; int hi_excl;
    int has_lo;   /* a lower bound tighter than "" was set */
    int has_hi;   /* an upper bound tighter than 0xffffffff was set */
} OrderWalkBounds;

static void order_walk_bounds(CriteriaNode *tree, FieldSchema *fs,
                              const char *order_by, OrderWalkBounds *b) {
    memset(b, 0, sizeof(*b));
    /* defaults: lo = "" (len 0), hi = 4 x 0xff */
    memset(b->hi, 0xff, 4); b->hi_len = 4;
    if (!order_by || !order_by[0]) return;

    const TypedField *tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
    if (!tf) return;

    SearchCriterion *leaves[MAX_INTERSECT_LEAVES];
    int nL = collect_and_leaves(tree, leaves, MAX_INTERSECT_LEAVES);
    for (int i = 0; i < nL; i++) {
        if (strcmp(leaves[i]->field, order_by) != 0) continue;
        const char *lowv = NULL;  int low_excl = 0;
        const char *highv = NULL; int high_excl = 0;
        switch (leaves[i]->op) {
            case OP_GREATER_EQ: lowv = leaves[i]->value; break;
            case OP_GREATER:    lowv = leaves[i]->value; low_excl = 1; break;
            case OP_LESS_EQ:    highv = leaves[i]->value; break;
            case OP_LESS:       highv = leaves[i]->value; high_excl = 1; break;
            case OP_EQUAL:      lowv = highv = leaves[i]->value; break;
            case OP_BETWEEN:
                lowv  = leaves[i]->value;  low_excl  = leaves[i]->min_exclusive;
                highv = leaves[i]->value2; high_excl = leaves[i]->max_exclusive;
                break;
            default: break;   /* not a range/eq op → no bound */
        }
        if (lowv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(tf, lowv, strlen(lowv), enc, &el);
            /* Keep the TIGHTEST lower bound (largest encoded value). First one
             * always wins over the "" default. memcmp is valid because the
             * index encoding is order-preserving. */
            if (el > 0 && (!b->has_lo ||
                           el > b->lo_len ||
                           (el == b->lo_len && memcmp(enc, b->lo, el) > 0))) {
                memcpy(b->lo, enc, el); b->lo_len = el; b->lo_excl = low_excl;
                b->has_lo = 1;
            }
        }
        if (highv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(tf, highv, strlen(highv), enc, &el);
            /* Keep the TIGHTEST upper bound (smallest encoded value). */
            if (el > 0 && (!b->has_hi ||
                           el < b->hi_len ||
                           (el == b->hi_len && memcmp(enc, b->hi, el) < 0))) {
                memcpy(b->hi, enc, el); b->hi_len = el; b->hi_excl = high_excl;
                b->has_hi = 1;
            }
        }
    }
}
```

> The length-then-memcmp tightness test is a heuristic for the rare multi-leaf-same-side case; for the common single-leaf case it just takes that leaf. Correctness is guaranteed by the post-filter regardless. If `collect_and_leaves`, `resolve_idx_field`, `encode_criterion_value`, `min_exclusive`/`max_exclusive`, or `OP_*` names differ from the quoted reference, STOP and write `PLAN_NOTES.md`.

- [ ] **Step 2: Add the `order_walk_bounds_for_test` hook**, modelled exactly on `leaf_selective_for_test` / `plan_filter_kind_for_test` (same `dir/object` split + `g_db_root` setup). These hooks live inside `#ifdef TEST_BUILD ... #endif` blocks. Find the existing `#ifdef TEST_BUILD` block that defines `leaf_selective_for_test` and add this **inside** it (or add a new `#ifdef TEST_BUILD` block right after that one's `#endif`):

```c
#ifdef TEST_BUILD
/* Test hook: which sides of the order-by walk got bounded by a range/eq leaf
 * on order_by. Mirrors plan_filter_kind_for_test's dir/object split. */
int order_walk_bounds_for_test(const char *db_root, const char *object,
                               const char *criteria_json, const char *order_by,
                               int *out_has_lo, int *out_has_hi) {
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
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    OrderWalkBounds b;
    order_walk_bounds(tree, &fs, order_by, &b);
    if (out_has_lo) *out_has_lo = b.has_lo;
    if (out_has_hi) *out_has_hi = b.has_hi;
    free_criteria_tree(tree);
    return 0;
}
#endif
```

> This copies `leaf_selective_for_test`'s exact preamble (verify it: `g_db_root` global, `eff_root`/`bare` split, `parse_criteria_tree(json, &err)` — two args, `init_field_schema(&fs, eff_root, bare)`, no explicit `fs` free, `free_criteria_tree(tree)`). If any of those differ in the file, align to what's actually there and note it in `PLAN_NOTES.md`. `order_walk_bounds` itself is NOT test-only — it stays compiled in all builds (the executors call it).

- [ ] **Step 3: Build.**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: builds clean (transient `-Wunused-function` for `order_walk_bounds` is fine until Task 3).

---

## Task 2: Unit-test the helper

**Files:** Create `src/test/cases/test_order_walk_range_bounds.c`

- [ ] **Step 1: Write the file with the helper unit test.** Use `test_composite_selectivity_guard.c` for the harness idioms (includes, `test_env_start`, `tc_connect`, `tc_request`, `TEST_REGISTER`).

```c
/* Order-by walk bounds: the walk must start/stop at the order_by-field range
 * criterion instead of scanning the whole index. Regression guard for the 30s
 * `title starts "Ask HN" AND time>=T ORDER BY time` (cursor) timeout. See
 * docs/plans/2026-06-04-order-walk-range-bounds.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern int order_walk_bounds_for_test(const char *db_root, const char *object,
                                       const char *criteria_json, const char *order_by,
                                       int *out_has_lo, int *out_has_hi);
extern long order_walk_scanned_for_test(void);
extern void order_walk_scanned_reset_for_test(void);

static int test_order_walk_bounds_helper(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"w\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"w\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"cat\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    int has_lo = -1, has_hi = -1;

    /* gte on order_by → lower bound only. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"t\",\"op\":\"gte\",\"value\":100}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 1, "gte sets lower bound");
    ASSERT_EQ_INT(has_hi, 0, "gte leaves upper unbounded");

    /* lte on order_by → upper bound only. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"t\",\"op\":\"lte\",\"value\":100}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 0, "lte leaves lower unbounded");
    ASSERT_EQ_INT(has_hi, 1, "lte sets upper bound");

    /* range on a DIFFERENT field → no bounds on order_by. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 0, "non-order_by leaf doesn't bound");
    ASSERT_EQ_INT(has_hi, 0, "non-order_by leaf doesn't bound");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-bounds-helper", test_order_walk_bounds_helper)
```

- [ ] **Step 2: Build & run.**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-order-walk-bounds-helper`
Expected: PASS. (This tests the helper logic independent of wiring.)

---

## Task 3: Wire the helper into the D3 walk + add the scan counter

**Files:** Modify `src/db/query.c`

- [ ] **Step 1: Add the test-only scan counter** (`#ifdef TEST_BUILD` → zero production footprint, matching the file's other test hooks). At file scope before `order_index_walk_cb`, add:

```c
#ifdef TEST_BUILD
/* Counts index entries visited by the order-by walks, so a test can prove a
 * windowed query stops at the window instead of scanning the whole index. */
long g_order_walk_scanned = 0;
long order_walk_scanned_for_test(void)   { return g_order_walk_scanned; }
void order_walk_scanned_reset_for_test(void) { g_order_walk_scanned = 0; }
#endif
```

- [ ] **Step 2: Increment it in both walk callbacks.** Find `order_index_walk_cb` (the D3 callback) and `cursor_find_cb` (the cursor callback). At the very top of **each** function body, add (guarded so production builds are untouched):
```c
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
```

- [ ] **Step 3: Rewire `find_via_order_index_walk` bounds.** Search for:
```c
    /* Full range: lower = "" (len 0), upper = "\xff\xff\xff\xff" (len 4).
       btree_idx_walk_ordered's desc flag handles walk direction. */
    const char *lo = "";
    size_t      lo_len = 0;
    const char *hi = "\xff\xff\xff\xff";
    size_t      hi_len = 4;
```
Replace with:
```c
    /* Bound the walk by any range/eq leaf on order_by so a sparse windowed
       query stops at the window instead of scanning the whole index. The
       per-record criteria_match_tree still runs, so this only narrows the walk. */
    OrderWalkBounds owb;
    order_walk_bounds(tree, fs, order_by, &owb);
    const char *lo = owb.has_lo ? (const char *)owb.lo : "";
    size_t      lo_len  = owb.has_lo ? owb.lo_len  : 0;
    int         lo_excl = owb.has_lo ? owb.lo_excl : 0;
    const char *hi = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
    size_t      hi_len  = owb.has_hi ? owb.hi_len  : 4;
    int         hi_excl = owb.has_hi ? owb.hi_excl : 0;
```
Then find this call (a few lines below) and replace the two `0` exclusivity args with the computed flags:
```c
    btree_idx_walk_ordered(db_root, object, order_by, sch->splits,
                           lo, lo_len, 0,
                           hi, hi_len, 0,
                           order_desc, order_index_walk_cb, &ctx);
```
becomes:
```c
    btree_idx_walk_ordered(db_root, object, order_by, sch->splits,
                           lo, lo_len, lo_excl,
                           hi, hi_len, hi_excl,
                           order_desc, order_index_walk_cb, &ctx);
```

- [ ] **Step 4: Build.**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean (no unused-function warning now).

- [ ] **Step 5: Add the D3 scan-count + correctness test** to `test_order_walk_range_bounds.c` (new case). This forces D3 via a **saturated btree seed** (`cat=x` matching all rows → > budget → not selective → `FP_ORDER_INDEX_WALK`), with a sparse window:

```c
static int test_order_walk_d3_bounded(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"cat\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 1000 rows, all cat=x. t = i. Window t>=980 holds 20 rows (< limit 25). */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"cat\":\"x\",\"t\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    order_walk_scanned_reset_for_test();
    /* cat=x is broad/saturated → D3 order walk on t. Window t>=980 (20 rows). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"},"
        " {\"field\":\"t\",\"op\":\"gte\",\"value\":980}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":25}", &resp);
    /* correctness: exactly the 20 in-window rows, top is t=999. */
    ASSERT_CONTAINS(resp, "\"t\":999", "top in-window row present");
    ASSERT_CONTAINS(resp, "\"t\":980", "boundary in-window row present");
    long scanned = order_walk_scanned_for_test();
    /* bounded: ~20 (window); unbounded would be ~1000. Generous ceiling. */
    ASSERT_TRUE(scanned < 200, "D3 walk bounded to the window, not full index");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-d3-bounded", test_order_walk_d3_bounded)
```

- [ ] **Step 6: Build & run; verify the D3 case PASSES** (and would have failed before Step 3 — note: it only exists now, so confirm the `scanned < 200` assertion is the meaningful one by temporarily reverting Step 3's bounds locally if you want to see ~1000; optional).

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-order-walk-d3-bounded`
Expected: PASS. Paste the pass line.

---

## Task 4: Wire the helper into the cursor walk

**Files:** Modify `src/db/query.c`

- [ ] **Step 1: Replace the cursor walk bounds.** In `cmd_find`'s cursor block, search for:
```c
        if (desc) {
            /* DESC: walk backward. Upper bound = cursor value (inclusive),
               lower bound = "". Ties still handled in the callback. */
            const char *max_val_bytes = has_cur_bytes
                ? (const char *)cur_value_buf : "\xff\xff\xff\xff";
            size_t max_val_len = has_cur_bytes ? cur_value_len : 4;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   "", 0, 0,
                                   max_val_bytes, max_val_len, 0,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: walk forward. Lower bound = cursor value (inclusive),
               upper bound = "\xff\xff\xff\xff". Ties handled in callback. */
            const char *min_val_bytes = has_cur_bytes
                ? (const char *)cur_value_buf : "";
            size_t min_val_len = has_cur_bytes ? cur_value_len : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   min_val_bytes, min_val_len, 0,
                                   "\xff\xff\xff\xff", 4, 0,
                                   0, cursor_find_cb, &cc);
```
Replace with (note: the cursor still drives the *start* side; the order_by range bounds the *far* side and, on page 1, the start side too):
```c
        OrderWalkBounds owb;
        order_walk_bounds(tree, &driver_fs, order_by, &owb);
        if (desc) {
            /* DESC: start (upper) = cursor when resuming, else window-high or
               max; stop (lower) = window-low or "". */
            const char *hi_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff");
            size_t hi_l = has_cur_bytes ? cur_value_len : (owb.has_hi ? owb.hi_len : 4);
            int    hi_e = has_cur_bytes ? 0 : (owb.has_hi ? owb.hi_excl : 0);
            const char *lo_b = owb.has_lo ? (const char *)owb.lo : "";
            size_t lo_l = owb.has_lo ? owb.lo_len : 0;
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: start (lower) = cursor when resuming, else window-low or "";
               stop (upper) = window-high or max. */
            const char *lo_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_lo ? (const char *)owb.lo : "");
            size_t lo_l = has_cur_bytes ? cur_value_len : (owb.has_lo ? owb.lo_len : 0);
            int    lo_e = has_cur_bytes ? 0 : (owb.has_lo ? owb.lo_excl : 0);
            const char *hi_b = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
            size_t hi_l = owb.has_hi ? owb.hi_len : 4;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   0, cursor_find_cb, &cc);
```

> Confirm the criteria-tree variable in this block is `tree` and the field schema is `driver_fs` (the cursor block sets `cc.remaining = tree;` and `cc.fs = &driver_fs;`). If the names differ, use the locals actually in scope and note it.

- [ ] **Step 2: Build.**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean.

- [ ] **Step 3: Add the cursor scan-count + correctness test** to `test_order_walk_range_bounds.c`. This reproduces the prod bug shape (sparse post-filter + window, cursor:null):

```c
static int test_order_walk_cursor_bounded(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"c\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"tag\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 1000 rows. Only 3 have tag=rare, all in the window t>=990 (sparse: <limit).
     * The other 997 are tag=common spread across t=0..996. */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        const char *tag = (i >= 990 && i < 993) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"tag\":\"%s\",\"t\":%d}}", i, tag, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    order_walk_scanned_reset_for_test();
    /* cursor:null, sparse tag=rare + window t>=985, order by t. Matches=3<limit. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
        " {\"field\":\"t\",\"op\":\"gte\",\"value\":985}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":25,\"cursor\":null}", &resp);
    /* correctness: the 3 rare rows present; the {rows:...} wrapper is fine. */
    ASSERT_CONTAINS(resp, "\"t\":992", "rare row 992 present");
    ASSERT_CONTAINS(resp, "\"t\":990", "rare row 990 present");
    long scanned = order_walk_scanned_for_test();
    /* bounded: walk stops at t=985 (~15 entries); unbounded ran all ~1000. */
    ASSERT_TRUE(scanned < 200, "cursor walk bounded to the window, not full index");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-cursor-bounded", test_order_walk_cursor_bounded)
```

- [ ] **Step 4: Build & run the cursor case.**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-order-walk-cursor-bounded`
Expected: PASS (`scanned < 200`). To convince yourself it's real, temporarily revert Task 4 Step 1 and observe `scanned ≈ 1000` (optional; re-apply after).

---

## Task 5: Full suite

Existing cursor / order-by / composite cases must stay green — especially `test-d1-composite-executor`, `test-d3-order-walk-executor`, `test-find-orderby-selective`, `test-find-filter-first-orderby`, `test-small-prefilter-orderby`, and any cursor-pagination case (`list | grep -i cursor`). Pagination correctness matters most: a wrong far-bound could drop rows across pages.

- [ ] **Step 1: Run the whole suite.**

Run: `./build/bin/shard-db-test run-all`
Expected: `# total: N passed, 0 failed`. Paste the final line.

- [ ] **Step 2: Sanity-check multi-page cursor correctness** (the highest-risk regression). Run an existing cursor test if one exists; if `list | grep -i cursor` finds nothing that paginates a windowed query across ≥2 pages, add a small case: insert ~60 rows in a window, page through with `limit 25` following the returned cursor, and assert the union of pages equals the full in-window set with no dupes/gaps. Record the result.

- [ ] **Step 3: Hand back uncommitted.** Report the `run-all` total and `git diff --stat`.

---

## Self-review checklist

1. **All three executors consistent** — D1 already folds; D3 and the cursor walk now call `order_walk_bounds`. Grep `src/db/query.c` for `"\xff\xff\xff\xff"` and confirm the only remaining literal full-range bounds are the documented defaults *inside* `order_walk_bounds` / the `owb.has_* ? ... : default` fallbacks (no executor still passes a bare hardcoded `""`/`\xff` to `btree_idx_walk_ordered` for an order walk).
2. **Post-filter intact** — `criteria_match_tree` still runs in `order_index_walk_cb` and `cursor_find_cb` (you only added a counter increment; you did not remove filtering). Correctness can't depend on the bound being exact.
3. **Cursor start side preserved** — when a cursor is present, the *start* bound is still the cursor value (resume point); only the *far* bound comes from the range. Multi-page correctness verified in Task 5 Step 2.
4. **Scan counter is the proof** — both bounded tests assert `scanned < 200` on 1000-row datasets; this is what fails on unbounded code.
5. **Suite green** — `# total: N passed, 0 failed` pasted.

---

## Closing the gap class (context for the reviewer)

With this plan, all three order-walk executors bound themselves by the order_by-field range. That closes the "order walk ignores the order_by range" gap class entirely. Remaining, *separate* planner gaps catalogued during this work (out of scope here, for the pre-release review):

- **`OP_IN` can't seed a composite** (range-fold the `in` values) — only bites a *selective* `in` + order_by.
- **`card_est_leaf` bitmap `OP_IN` counts only the first value** — minor seed-ranking estimate bug.
- **All-time sparse ordered** (e.g. `title starts "Ask HN"` with no window, `cursor:null`) — ~6 s, no range to bound; needs a covering composite for the filtered+ordered shape or first-class Ask/Show HN handling.
