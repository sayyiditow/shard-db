# Composite Walk: Fold the order-by-field Range into the Seek Bounds

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans or superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.
> **Execution rules:** branch off `main`; do tasks in order; leave everything **uncommitted** for review; locate edits by searching the quoted anchor text (line numbers drift); build `SKIP_TESTS=1 ./build.sh`, test `./build/bin/shard-db-test run[-all]`; never claim a step passed without the real output; if a referenced symbol/anchor isn't where expected, STOP and write `PLAN_NOTES.md` rather than guess.

**Goal:** Make `seed=V AND order_by <range> T  ORDER BY order_by` *seek* to T in the `seed+order_by` composite instead of walking the whole `seed` prefix and post-filtering. Fixes the dominant warm-slow workload — HN comment-thread pagination: `story_root=X AND time>=T ORDER BY time ASC limit 51` and `story_root=X AND time<=T ORDER BY time DESC limit 50` (measured 2–5s warm; should be O(limit) ≈ ms).

**Architecture:** Phase A already routes these to `find_via_composite_prefix`, which walks `[encode(seed), encode(seed)+0xff×4]` in order_by order and post-filters the `time` range. For ASC+`>=T` it starts at the oldest and skips everything before T; for DESC+`<=T` it starts at the newest and skips everything after T — O(comments on one side of T). This change tightens the walk's min/max by appending the encoded range bound to the seed prefix, so the btree seeks directly to T. Correct because the composite key is exactly `encode(seed) ‖ encode(order_by)` and `encode_field_for_index` is the shared encoder (config.c:1948).

**Tech Stack:** C, `src/db/query.c` (`find_via_composite_prefix` executor + `plan_filter` overlay + `cmd_find` dispatch), C test harness.

**Scope:** Applies the fold **only when the seed op is `OP_EQUAL`**. A `STARTS_WITH` seed spans multiple first-field values, so `(seed,order_by)` order ≠ pure `order_by` order and appending a suffix bound is unsound — those keep the current whole-prefix walk. Multi-value `IN` seeds are a separate follow-up (see "Future Work").

---

## File Structure
- `src/db/query.c` — only production file.
  - `FilterPlan` struct (~10662): add `SearchCriterion *order_range;`.
  - `find_via_composite_prefix` (~11280): add an `order_range` param; build tightened bounds.
  - `plan_filter` order overlay (~13258, the `cc >= 0` branch): set `fp.order_range`.
  - `cmd_find` FP_ORDER_COMPOSITE dispatch (~17295 region): pass `fp.order_range`.
- `src/test/cases/test_composite_range_fold.c` — new test case file (add to `build.sh` compile list, next to `test_composite_prefix_routing.c`).

Verified facts:
- Composite walk call today: `btree_idx_walk_ordered(db_root, object, composite_field, sch->splits, buf_lo, len_lo, 0, buf_hi, len_hi, 0, order_desc, composite_prefix_cb, &ctx);` — args 6/9 are `min_exclusive`/`max_exclusive` (currently `0`).
- `encode_criterion_value(tf, val, strlen(val), out, &len)` encodes a criterion value to the index-key form (query.c:9802), byte-identical to the stored composite component.
- Range ops: `OP_GREATER`, `OP_GREATER_EQ`, `OP_LESS`, `OP_LESS_EQ`, `OP_BETWEEN`, `OP_EQUAL`. `SearchCriterion` has `value`, `value2` (BETWEEN high), `min_exclusive`, `max_exclusive`.

---

### Task 1: Thread an order_range bound through the composite executor

**Files:**
- Modify: `src/db/query.c` — `FilterPlan` struct; `find_via_composite_prefix` signature + bound construction.

- [ ] **Step 1: Add the FilterPlan field**

In the `FilterPlan` struct (the one with `composite_field[256]` added previously), add:

```c
    SearchCriterion *order_range;     /* range/eq leaf on order_by to fold into the composite seek (EQ seed only) */
```

- [ ] **Step 2: Add the param to find_via_composite_prefix**

Change its signature (and the forward usage in `cmd_find`) to accept the bound. New signature:

```c
static int find_via_composite_prefix(const char *db_root, const char *object,
                                     const Schema *sch, FieldSchema *fs,
                                     SearchCriterion *seed,
                                     const char *order_by,
                                     int order_desc,
                                     SearchCriterion *order_range,   /* NULL = whole-prefix walk */
                                     CriteriaNode *tree,
                                     ExcludedKeys *excluded,
                                     int offset, int limit,
                                     const char **proj_fields, int proj_count,
                                     int dict_fmt,
                                     QueryDeadline *dl)
```

- [ ] **Step 3: Build the tightened bounds**

Replace the bound-construction block (steps "1." and "2." at the top of the function — the `encode_criterion_value` of the seed and the `buf_hi = buf_lo + 0xff×4` block) with:

```c
    /* 1. Seed prefix = encode(seed value). */
    uint8_t buf_lo[1024 + 8];
    size_t  len_lo = 0;
    const TypedField *seed_tf = resolve_idx_field(fs ? fs->ts : NULL, seed->field);
    encode_criterion_value(seed_tf, seed->value, strlen(seed->value), buf_lo, &len_lo);
    size_t pfx_len = len_lo;

    /* 2. Upper bound: seed prefix + 4 × 0xff (STARTS_WITH idiom). */
    uint8_t buf_hi[1024 + 8];
    memcpy(buf_hi, buf_lo, len_lo);
    memset(buf_hi + len_lo, 0xff, 4);
    size_t  len_hi = len_lo + 4;
    int min_excl = 0, max_excl = 0;

    /* 2b. If an order_by range/eq leaf is present (EQ seed only — guaranteed
       by the planner), append its encoded value(s) to the seed prefix so the
       btree seeks to T instead of walking the whole prefix and post-filtering.
       Composite key = encode(seed) ‖ encode(order_by); appending encode(T)
       after the (fixed-for-EQ) seed prefix is the exact seek point. */
    if (order_range) {
        const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
        /* low bound: >=, >, BETWEEN-low, or == */
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
            default: break;   /* leave whole-prefix bounds */
        }
        if (lowv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el);
            if (pfx_len + el <= sizeof(buf_lo)) {
                memcpy(buf_lo + pfx_len, enc, el);
                len_lo = pfx_len + el;
                min_excl = low_excl;
            }
        }
        if (highv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
            if (pfx_len + el <= sizeof(buf_hi)) {
                memcpy(buf_hi + pfx_len, enc, el);
                len_hi = pfx_len + el;
                max_excl = high_excl;
            }
        }
    }
```

- [ ] **Step 4: Pass the exclusivity flags into the walk**

Change the `btree_idx_walk_ordered(...)` call's two `0` exclusivity args to `min_excl` / `max_excl`:

```c
    btree_idx_walk_ordered(db_root, object, composite_field, sch->splits,
                           (const char *)buf_lo, len_lo, min_excl,
                           (const char *)buf_hi, len_hi, max_excl,
                           order_desc, composite_prefix_cb, &ctx);
```

- [ ] **Step 5: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: compile error at the `find_via_composite_prefix(...)` call in `cmd_find` (arg count changed) — fixed in Task 2. If it compiles, the call site already matched; continue.

### Task 2: Planner sets order_range; dispatch passes it

**Files:**
- Modify: `src/db/query.c` — `plan_filter` overlay; `cmd_find` dispatch.
- Test: `src/test/cases/test_composite_range_fold.c` (create)

- [ ] **Step 1: Set fp.order_range in the overlay**

In `plan_filter`, in the `if (cc >= 0) { ... fp.order = FP_ORDER_COMPOSITE; }` branch (added by the previous plan), append — **only for an EQ seed** — a scan for the order_by range leaf:

```c
            fp.order_range = NULL;
            if (leaves[cc]->op == OP_EQUAL) {
                for (int i = 0; i < nL; i++) {
                    if (strcmp(leaves[i]->field, order_by) != 0) continue;
                    enum SearchOp o = leaves[i]->op;
                    if (o == OP_GREATER || o == OP_GREATER_EQ || o == OP_LESS ||
                        o == OP_LESS_EQ || o == OP_BETWEEN || o == OP_EQUAL) {
                        fp.order_range = leaves[i];
                        break;
                    }
                }
            }
```

(Place it right after `fp.order = FP_ORDER_COMPOSITE;` inside the `cc >= 0` block. Leave the legacy `else if (composite_index_exists(... source_leaves[0] ...))` branch as-is — it already only fires for an EQ/STARTS seed; set `fp.order_range = NULL` there too for safety if not already zero-initialised.)

- [ ] **Step 2: Pass it in the cmd_find dispatch**

In the `FP_ORDER_COMPOSITE` dispatch block in `cmd_find`, update the `find_via_composite_prefix(...)` call to pass `fp.order_range` in the new arg position (after `desc`):

```c
        find_via_composite_prefix(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            fp.source_leaves[0], order_by, desc,
            fp.order_range,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
```

- [ ] **Step 3: Write the correctness test**

Create `src/test/cases/test_composite_range_fold.c`:

```c
/* Composite walk must fold an order_by range bound into the seek so it
   returns the correct window in the correct order — the comment-thread
   pagination shape: story_root=X AND time {>=|<=} T ORDER BY time {asc|desc}. */
#include "../test.h"
#include "../../db/types.h"
#include <string.h>

static int test_range_fold_window(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"c\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c\",\"object\":\"cm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"story_root:long\",\"time:timestamp\"],"
        "\"indexes\":[\"story_root\",\"time\",\"story_root+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create cm"); free(resp); resp=NULL;

    /* story_root=100 with comments on days 1..10; story_root=200 decoy. */
    for (int d = 1; d <= 10; d++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"cm\",\"key\":\"a%02d\","
            "\"value\":{\"story_root\":100,\"time\":\"2026-05-%02d 00:00:00\"}}", d, d);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"cm\",\"key\":\"z1\","
        "\"value\":{\"story_root\":200,\"time\":\"2026-05-05 00:00:00\"}}", &resp);
    free(resp); resp=NULL;

    /* Newer page: time>=day5 ASC limit 3 → a05,a06,a07 in that order. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":100},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2026-05-05 00:00:00\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":3}", &resp);
    ASSERT_NOT_NULL(resp, "asc page");
    {
        const char *p5=strstr(resp,"a05"), *p6=strstr(resp,"a06"), *p7=strstr(resp,"a07");
        ASSERT_TRUE(p5 && p6 && p7 && p5<p6 && p6<p7, "asc: a05,a06,a07 in order");
        ASSERT_TRUE(!strstr(resp,"a04") && !strstr(resp,"a08"), "asc: window excludes a04/a08");
        ASSERT_TRUE(!strstr(resp,"z1"), "asc: decoy story_root excluded");
    }
    free(resp); resp=NULL;

    /* Older page: time<=day5 DESC limit 3 → a05,a04,a03 in that order. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":100},"
        " {\"field\":\"time\",\"op\":\"lte\",\"value\":\"2026-05-05 00:00:00\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_NOT_NULL(resp, "desc page");
    {
        const char *p5=strstr(resp,"a05"), *p4=strstr(resp,"a04"), *p3=strstr(resp,"a03");
        ASSERT_TRUE(p5 && p4 && p3 && p5<p4 && p4<p3, "desc: a05,a04,a03 in order");
        ASSERT_TRUE(!strstr(resp,"a06") && !strstr(resp,"a02"), "desc: window excludes a06/a02");
    }
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-range-fold-window", test_range_fold_window)

/* Regression: seed with NO order_by range leaf still returns all of the
   prefix in order (whole-prefix walk unaffected). */
static int test_range_fold_no_bound(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"c2\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c2\",\"object\":\"cm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"story_root:long\",\"time:timestamp\"],"
        "\"indexes\":[\"story_root\",\"time\",\"story_root+time\"]}", &resp);
    free(resp); resp=NULL;
    for (int d = 1; d <= 4; d++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c2\",\"object\":\"cm\",\"key\":\"b%02d\","
            "\"value\":{\"story_root\":7,\"time\":\"2026-05-%02d 00:00:00\"}}", d, d);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c2\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":7}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":50}", &resp);
    ASSERT_TRUE(resp && strstr(resp,"b01") && strstr(resp,"b04"), "no-bound: all rows returned");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-range-fold-no-bound", test_range_fold_no_bound)
```

- [ ] **Step 4: Register the test file in build.sh**

Add next to the existing entry:

```
    src/test/cases/test_composite_range_fold.c \
```

- [ ] **Step 5: Build + run the new tests**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-range-fold-window && ./build/bin/shard-db-test run test-range-fold-no-bound`
Expected: both PASS. (Without the fold, `test-range-fold-window` would still pass on this tiny dataset since post-filter is correct — the fold is a *performance* change, validated for *correctness* here. The window/order/decoy assertions guard against an off-by-one or wrong-bound seek.)

- [ ] **Step 6: Full suite**

Run: `./build/bin/shard-db-test run-all`
Expected: `0 failed`. Confirm `test_composite_prefix_routing` cases still pass (the seed/composite selection is unchanged).

---

## Future Work (separate plan)
- **`IN` on the seed:** `type IN (a,b,c) ORDER BY time` — walk the `type+time` composite once per IN value (each an EQ-style prefix, range-folded) and k-way merge the per-value ordered streams to `limit`. Bigger change (multi-prefix merge); plan separately if the `type IN` homepage query stays hot. Note: `type IN (story,job,poll)` is ~all rows, so a plain `time`/`score` index walk + post-filter may already suffice — measure first.
- **`order_by=score` / title prefixes:** schema-side (`type+score` composite — already advised) or trigram+sort; no executor change.

## Self-Review
- **Spec coverage:** Task 1 (executor bound-fold) + Task 2 (planner detection + dispatch + tests) deliver the comment-thread seek. EQ-seed-only gate keeps STARTS correctness. IN/score/title explicitly deferred.
- **Placeholders:** none — full code for struct field, signature, bound construction, planner detection, dispatch, and two tests.
- **Type consistency:** `fp.order_range` (Task 1 Step 1) set in Task 2 Step 1, read in Task 1 Step 3 via the new param passed in Task 2 Step 2. Ops `OP_GREATER/OP_GREATER_EQ/OP_LESS/OP_LESS_EQ/OP_BETWEEN/OP_EQUAL` and fields `value/value2/min_exclusive/max_exclusive` match `types.h`. `encode_criterion_value`, `resolve_idx_field`, `btree_idx_walk_ordered` (with min/max exclusivity args) match query.c usage.
