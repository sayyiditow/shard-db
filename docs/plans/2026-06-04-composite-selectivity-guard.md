# Selectivity-Aware Composite Routing (Phase A guard)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans or superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.
> **Execution rules:** branch off `main`; tasks in order; leave everything **uncommitted** for review; locate edits by searching the quoted anchor text (line numbers drift); build `SKIP_TESTS=1 ./build.sh`, test `./build/bin/shard-db-test run[-all]`; never claim a step passed without real output; if an anchor/symbol isn't where expected, STOP and write `PLAN_NOTES.md` instead of guessing.

**Goal:** Stop the planner from routing to a `seed+order_by` composite when doing so is *slower* than driving on a selective sibling. Concretely: `type=story AND time>=T ORDER BY score` was routed to the `type+score` composite → walk all ~5M `type=story` rows in score order, post-filtering `time>=T` (rejects almost all) → 30s timeout. It should instead drive on the selective `time>=T` leaf and sort (the pre-regression ~7–9s D2 plan).

**Root cause:** `find_covering_composite` (added in the composite-prefix work) routes to a composite purely on **existence**, with no cost check. A composite ordered by `order_by` walks the whole `seed` prefix and post-filters siblings — cheap only when the prefix is selective, or the `order_by` field itself carries the range (so the walk seeks/fills fast), or there's no better sibling to drive.

**Fix:** In the `cc >= 0` branch of the order overlay, add a guard: **skip the composite** when the seed prefix is provably broad AND there's a selective sibling on a field other than `order_by` AND `order_by` has no range leaf. Then fall through to the existing D2/D3 fork driven by the most-selective seed (`prim`). All the helpers (`est[]`, `leaf_is_selective`, `prim`, `N`) are already in scope at that point.

**Tech Stack:** C, `src/db/query.c` (`plan_filter` overlay), C test harness with `plan_filter_kind_for_test`.

**Scope:** Planner-only; no executor change. Does not alter results (composite vs D2/D3 return the same rows) — purely which plan is chosen. After this lands, re-adding `type+score` is safe.

---

## File Structure
- `src/db/query.c` — the `cc >= 0` branch inside the `order_overlay:` block of `plan_filter` (the branch that currently sets `fp.order = FP_ORDER_COMPOSITE` and scans for `fp.order_range`).
- `src/test/cases/test_composite_selectivity_guard.c` — new test (add to `build.sh` compile list).

Verified facts:
- `leaf_is_selective(CardEst e, size_t N)` → `e.estimable && !e.saturated && e.k <= selectivity_budget(N)`; `selectivity_budget(N) = N / g_random_seq_ratio` (default 8). (query.c)
- `est[]` is populated for all leaves whenever `order_by` is set (skip_est is false → `most_selective_indexed` ran). `prim` is the most-selective indexed leaf index. `N` = live count. (query.c `plan_filter`)
- `plan_filter_kind_for_test(..., order_by, fetching, out_field, fsz, out_order, osz, out_total_cheap)` fills `out_order` with the order-mode string (`"composite"`, `"sort"`, `"walk"`, `"none"`).

---

### Task 1: Add the selectivity guard

**Files:**
- Modify: `src/db/query.c` — the `cc >= 0` branch in the order overlay.

- [ ] **Step 1: Find the current branch**

Search for this exact block (the composite-prefix + order_range scan added earlier):

```c
        if (cc >= 0) {
            fp.kind            = FP_PRIMARY_LEAF;  /* composite executor requires this */
            fp.source_is_bitmap = 0;               /* driving via composite btree, not a bitmap */
            fp.source_leaves[0] = leaves[cc];
            fp.n_source         = 1;
            fp.order            = FP_ORDER_COMPOSITE;
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
        } else if (composite_index_exists(db_root, object,
```

- [ ] **Step 2: Replace it with the guarded version**

```c
        if (cc >= 0) {
            /* Find an order_by range/eq leaf (used both for the range-fold and
             * the guard below). EQ-seed only — STARTS seeds don't fold. */
            SearchCriterion *obr = NULL;
            if (leaves[cc]->op == OP_EQUAL) {
                for (int i = 0; i < nL; i++) {
                    if (strcmp(leaves[i]->field, order_by) != 0) continue;
                    enum SearchOp o = leaves[i]->op;
                    if (o == OP_GREATER || o == OP_GREATER_EQ || o == OP_LESS ||
                        o == OP_LESS_EQ || o == OP_BETWEEN || o == OP_EQUAL) {
                        obr = leaves[i];
                        break;
                    }
                }
            }

            /* Selectivity guard. A composite ordered by order_by walks the whole
             * seed prefix and post-filters siblings, so it's only cheap when:
             *   - the seed prefix is itself selective (small partition), OR
             *   - order_by carries the range (walk seeks / fills fast), OR
             *   - there's no more-selective sibling to drive a better plan.
             * When the seed is provably broad AND a selective sibling exists on
             * a non-order_by field AND order_by has no range, the composite walk
             * would scan the whole broad partition (e.g. type=story ≈ all rows,
             * ORDER BY score, time>=T post-filtered) — far worse than letting the
             * selective sibling (time>=T) drive a fetch+sort. Skip it. */
            int seed_broad = est[cc].estimable && !leaf_is_selective(est[cc], N);
            int has_sel_other = 0;
            for (int i = 0; i < nL; i++) {
                if (i == cc) continue;
                if (strcmp(leaves[i]->field, order_by) == 0) continue; /* order_by sibling → range-fold */
                if (leaf_is_selective(est[i], N)) { has_sel_other = 1; break; }
            }
            int skip_composite = seed_broad && !obr && has_sel_other;

            if (!skip_composite) {
                fp.kind             = FP_PRIMARY_LEAF;  /* composite executor requires this */
                fp.source_is_bitmap = 0;
                fp.source_leaves[0] = leaves[cc];
                fp.n_source         = 1;
                fp.order            = FP_ORDER_COMPOSITE;
                fp.order_range      = obr;
            } else {
                /* Drive on the most-selective seed instead (pre-overlay fp.kind /
                 * source_leaves already point at prim). D2 if bounded, else D3. */
                CardEst se = est[prim];
                fp.order = (se.estimable && !se.saturated)
                           ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
            }
        } else if (composite_index_exists(db_root, object,
```

(Everything from `} else if (composite_index_exists(...` onward is unchanged.)

- [ ] **Step 3: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean compile, no warnings. (`est`, `leaf_is_selective`, `prim`, `N` are all already used earlier in `plan_filter`.)

### Task 2: Test the guard via plan introspection

**Files:**
- Test: `src/test/cases/test_composite_selectivity_guard.c` (create); `build.sh` (register).

- [ ] **Step 1: Write the test**

Create `src/test/cases/test_composite_selectivity_guard.c`. Row counts assume the default `RANDOM_SEQ_COST_RATIO=8` → `selectivity_budget = N/8`; with 1000 `story` + 10 `poll` (N≈1010, budget≈126): `type=story` K≈1000 is **broad**, `type=poll` K=10 and `time>=future` K=0 are **selective**.

```c
/* The composite must be SKIPPED when the seed prefix is broad and a selective
   sibling exists (type=story AND time>=T ORDER BY score), and USED when the
   seed is selective, when order_by carries the range, or when there's no
   selective sibling. Asserts the chosen order-mode via plan introspection. */
#include "../test.h"
#include "../../db/types.h"
#include <string.h>

extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static const char *order_of(TestEnv *env, const char *crit, const char *order_by) {
    static char order[32];
    char field[64] = {0}; int cheap = -1;
    order[0] = '\0';
    plan_filter_kind_for_test(env->db_root, "g/st", crit, order_by, 1,
                              field, sizeof(field), order, sizeof(order), &cheap);
    return order;
}

static int test_selectivity_guard(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"g\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"g\",\"object\":\"st\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:enum(story,job,poll)\",\"time:timestamp\",\"score:int\"],"
        "\"indexes\":[\"type:bitmap\",\"time\",\"score\",\"type+score\",\"type+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create st"); free(resp); resp=NULL;

    /* 1000 old stories + 10 old polls. All time in 2020 → time>=2025 matches 0. */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"g\",\"object\":\"st\",\"key\":\"s%04d\","
            "\"value\":{\"type\":\"story\",\"time\":\"2020-01-01 00:00:00\",\"score\":%d}}",
            i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    for (int i = 0; i < 10; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"g\",\"object\":\"st\",\"key\":\"p%02d\","
            "\"value\":{\"type\":\"poll\",\"time\":\"2020-01-01 00:00:00\",\"score\":%d}}",
            i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* (a) broad seed + selective sibling (time>=future) + order by score
       → SKIP composite (drive on selective time, sort). */
    ASSERT_TRUE(strcmp(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2025-01-01 00:00:00\"}]",
        "score"), "composite") != 0,
        "broad story + selective time + order score → NOT composite");

    /* (b) selective seed (poll) + time>=future + order by score → USE composite. */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"poll\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2025-01-01 00:00:00\"}]",
        "score"), "composite",
        "selective poll + order score → composite");

    /* (c) broad seed, order by score, NO selective sibling → USE composite. */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}]",
        "score"), "composite",
        "broad story, no sibling, order score → composite");

    /* (d) broad seed + range ON order_by field (time) → USE composite (range-fold). */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2025-01-01 00:00:00\"}]",
        "time"), "composite",
        "broad story + time range + order BY time → composite");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-selectivity-guard", test_selectivity_guard)
```

- [ ] **Step 2: Register in build.sh**

Add next to the other composite test files:

```
    src/test/cases/test_composite_selectivity_guard.c \
```

- [ ] **Step 3: Build + run the new test**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-composite-selectivity-guard`
Expected: PASS (all four assertions).
If (a) reports `composite` (guard didn't trigger) or (b)/(c)/(d) report non-composite, the selectivity classification didn't land as assumed — check `selectivity_budget(N)` with the actual N (≈1010) and `g_random_seq_ratio` (8), confirm `type=story` K (≈1000) exceeds the budget and `type=poll`/`time>=2025` K are under it, and adjust the insert counts so the contrast is unambiguous. Do NOT weaken the assertions; fix the dataset. If still stuck, write findings to `PLAN_NOTES.md`.

- [ ] **Step 4: Full suite**

Run: `./build/bin/shard-db-test run-all`
Expected: `0 failed`. Confirm `test_composite_prefix_routing` and `test_composite_range_fold` still pass (selective-seed and order_by-range cases must still choose composite).

---

## Self-Review
- **Spec coverage:** Task 1 adds the guard exactly where composites are chosen; Task 2 asserts the four cases (skip when broad+selective-sibling+no-order-range; use otherwise). Results unchanged (plan-only).
- **Placeholders:** none — full replacement block + complete test. The one conditional note (Step 3) is a bounded "verify the threshold / adjust counts" with the formula given, not a TODO.
- **Type consistency:** `est`, `leaf_is_selective`, `prim`, `N`, `CardEst`, `FP_ORDER_SORT/INDEX_WALK/COMPOSITE`, `fp.order_range`, the `OP_*` range ops, and `plan_filter_kind_for_test`'s signature all match current query.c/types.h. `obr` is local; `fp.order_range` is the existing FilterPlan field.
