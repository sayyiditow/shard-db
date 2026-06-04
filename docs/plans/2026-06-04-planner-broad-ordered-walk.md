# Planner: broad-filter ordered finds must walk the order index, not materialize-and-sort

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `plan_filter` from picking the materialize-and-sort path (D2) for ordered finds whose candidate set is broad-but-exactly-counted (a saturated *bitmap* popcount, e.g. `type in (...)` ≈ 4.3M rows), which currently times out at 30 s; route those to the order-index walk (D3) instead — matching the guard the cursor path already has.

**Architecture:** One surgical change to the D2-vs-D3 fork inside `plan_filter` (src/db/query.c), behind a small helper `pick_sort_or_walk()`. The fork currently keys off `estimable && !saturated`; a bitmap is *always* `estimable && !saturated` no matter how many rows it matches, so an enormous bitmap candidate set is mis-classified as "small enough to sort". The fix keys off the existing `leaf_is_selective()` budget test (k ≤ N/8) and falls back to the walk (D3) when the set is broad and `order_by` is drivable. No new executor, no on-disk change, no new index type — D3 (`find_via_order_index_walk`) already exists and is wired into `cmd_find`.

**Tech Stack:** C (shard-db daemon). Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <name>` / `run-all`. Tests are C cases under `src/test/cases/test_*.c`, registered via `TEST_REGISTER`, each fork-execs its own daemon.

---

## Execution rules (read before starting)

- **Branch off `main`:** `git checkout main && git pull && git checkout -b perf/planner-broad-ordered-walk`. Leave the work **uncommitted** when done; the reviewer commits.
- **Locate edits by searching the quoted anchor text**, not line numbers — line numbers drift. Every task quotes the exact text to search for.
- **Build:** `SKIP_TESTS=1 ./build.sh` (builds without running the suite).
- **Test:** `./build/bin/shard-db-test run <name>` for one case, `./build/bin/shard-db-test run-all` for the full suite. `list` shows registered names.
- **Never claim a step passed without pasting the real command output.** The suite prints `# total: N passed, 0 failed` — that line is the proof.
- **If a quoted anchor or symbol isn't where this plan says**, stop and write `PLAN_NOTES.md` describing what you found instead of guessing. Do not invent function names or struct fields.
- The final deliverable is: new test(s) added, both fork sites changed, `run-all` green (`# total: N passed, 0 failed`).

---

## Problem & evidence (prod, hn/stories, 5.68M rows, N≈5.6M)

Measured against the production daemon. `type` is a bitmap; `score`/`time` have single btrees; `type+time` and `type+score` composites exist; `dead`/`deleted` are bitmaps.

| Query (limit 25) | Path | Time |
|---|---|---|
| `type=story ORDER BY time` | D1 composite (`type+time`) | 111 ms ✅ |
| `type=story ORDER BY score` | D1 composite (`type+score`) | 72 ms ✅ |
| `type in (story,job,poll) ORDER BY score` (**no cursor**) | **D2 sort** | **30 s timeout** ❌ |
| `type in (story,job,poll) ORDER BY time` (**no cursor**) | **D2 sort** | **30 s timeout** ❌ |
| `type in (story,job,poll) ORDER BY score`, `cursor:null` | cursor path (has guard) | 1.5 s |

**Why `eq` is fine but `in` times out:** `find_covering_composite` only accepts `OP_EQUAL`/`OP_STARTS_WITH` seeds, so `type=story` drives the `type+time`/`type+score` composite (D1). An `OP_IN` leaf can't seed a composite, so the planner falls through to the D2/D3 fork. There, the seed is a bitmap, whose `card_est_leaf` returns an **exact** count (`estimable=1, saturated=0`) — but that count is ~4.3M. The fork's condition `se.estimable && !se.saturated` reads that as "bounded → sort it", materializes ~4.3M rows and sorts → 30 s timeout.

**Why the cursor path doesn't time out:** the cursor executor builds its prefilter keyset via `build_keyset_from_plan`, which has a popcount guard (`if (pop > ORDERED_FIND_KEYSET_MAX) return NULL;`) that drops the keyset and walks the order index instead. The non-cursor D2/D3 fork has no equivalent guard. This plan adds the equivalent decision to the fork.

---

## Root cause: the D2/D3 fork (two identical sites)

`plan_filter` (src/db/query.c) decides D2 (sort) vs D3 (walk) in **two** places, both with the same buggy condition.

**Site 1** — inside the composite selectivity-guard `else` branch. Anchor:
```c
                /* Drive on the most-selective seed instead (pre-overlay fp.kind /
                 * source_leaves already point at prim). D2 if bounded, else D3. */
                CardEst se = est[prim];
                fp.order = (se.estimable && !se.saturated)
                           ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
```

**Site 2** — the main D2/D3 fork (no covering composite). Anchor:
```c
            CardEst se = (prim >= 0) ? est[prim] : (CardEst){0, 0, 0};
            fp.order = (se.estimable && !se.saturated)
                       ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
```

Both must change to consult magnitude (via `leaf_is_selective`) and `order_by` drivability.

**Existing helpers to reuse (do not reimplement):**
- `static int leaf_is_selective(CardEst e, size_t N)` — returns true iff `e.estimable && !e.saturated && e.k <= selectivity_budget(N)` (budget = `N / g_random_seq_ratio`, default ratio 8). A 4.3M bitmap on N=5.6M → `4.3M > 700k` → **not** selective. A rare value → selective.
- `static int order_field_drivable(const char *db_root, const char *object, const char *order_by)` — returns true iff `order_by` has a single-field btree (i.e. D3's `find_via_order_index_walk` can walk it).

**Why this is correct & safe:**
- `most_selective_indexed` makes `prim` the *most* selective leaf. If even `prim` is broad, the whole AND is broad → the order-walk's post-filter has a high pass-rate → D3 fills `limit` in ~`limit` fetches. If `prim` is selective, D2 sorts a genuinely small set (or D1 composite already took it). So the decision rests on the right signal.
- D3 needs a btree on `order_by`; if there isn't one we must keep D2 (best-effort sort, already bounded by `QUERY_BUFFER_MB`). The helper encodes exactly that fallback — and incidentally fixes a latent bug where the *old* `saturated → FP_ORDER_INDEX_WALK` branch could pick D3 even when `order_by` had no btree to walk.
- The cursor path calls `plan_filter` only to seed `build_keyset_from_plan`, which reads the plan's **kind/source leaves**, not `fp.order`. Changing `fp.order` therefore cannot affect the cursor path. (Confirm during Task 4's regression run.)

---

## Missing-paths catalogue (scope control)

This plan implements **Path A only** — the one the explorer is demonstrably slow at and the lowest-risk. The rest are documented here so the gap is known, not silently dropped. Do **not** implement B–E in this plan.

- **A — Broad-filter ordered find → order-index walk (THIS PLAN).** Fixes the 30 s non-cursor `type in (...) ORDER BY <indexed>` timeout.
- **B — `OP_IN` seeding a composite (range-fold).** Let `type in (a,b,c)` drive a `type+time` composite by folding each value into the prefix and k-way merging. Only matters for a **selective** `in` set (e.g. `type in (job,poll)` — both rare) + order_by; the explorer's `in` is non-selective so Path A's D3 already handles it. Deferred.
- **C — `card_est_leaf` bitmap `OP_IN` counts only the first value.** Minor estimate bug; for `type in (story,job,poll)` it counts only `story` (~4.3M), still broad, so Path A's outcome is unchanged. Worth a one-line follow-up but out of scope here.
- **D — Cursor path keyset-attempt overhead (~1.5 s).** The cursor executor builds then discards a candidate keyset (popcount guard) before walking; the attempt itself costs ~1.5 s for broad bitmaps. The homepage is served from the explorer's warm cache, so users don't pay it; deferred.
- **E — Sparse-filter ordered (e.g. `title starts "Ask HN" ... ORDER BY time`).** Selective filter whose matches are sparse in the order index → the walk has a low pass-rate and is slow even via D3. Needs a covering composite for the filtered+ordered shape (or treating Ask/Show HN as first-class). Separate, larger effort; deferred.

---

## File structure

- **Modify:** `src/db/query.c` — add `pick_sort_or_walk()` helper near `order_field_drivable`; replace the fork at Site 1 and Site 2.
- **Create:** `src/test/cases/test_planner_broad_ordered_walk.c` — new planner-path test case (uses the `plan_filter_kind_for_test` introspection hook to assert `"walk"` vs `"sort"`, plus a correctness check on returned rows).

No header changes: `pick_sort_or_walk` is `static`, file-local, and uses only existing types/helpers.

---

## Task 1: Add the failing planner-path test

**Files:**
- Create: `src/test/cases/test_planner_broad_ordered_walk.c`

The test asserts the *desired* behaviour, so it fails on today's code (which returns `"sort"`).

- [ ] **Step 1: Write the test file.**

Use `src/test/cases/test_composite_selectivity_guard.c` as the template for the harness idioms (`test_env_start`, `tc_connect`, the `plan_filter_kind_for_test` extern, the `order_of()` helper). Create:

```c
/* Broad-filter ordered finds must walk the order index (D3), not materialize-
 * and-sort (D2). Regression guard for the 30s `type in (...) ORDER BY score`
 * timeout: a bitmap candidate set is exact-but-huge (estimable && !saturated),
 * which the D2/D3 fork used to read as "small enough to sort". See
 * docs/plans/2026-06-04-planner-broad-ordered-walk.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Plan-path introspection hook (defined in query.c). Returns the order-overlay
 * kind as a string: "none" | "composite" | "sort" | "walk". */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static const char *order_of(TestEnv *env, const char *obj,
                            const char *crit, const char *order_by) {
    static char order[32];
    char field[64] = {0}; int cheap = -1;
    order[0] = '\0';
    plan_filter_kind_for_test(env->db_root, obj, crit, order_by, 1,
                              field, sizeof(field), order, sizeof(order), &cheap);
    return order;
}

static int test_planner_broad_ordered_walk(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"p\"}", &resp); free(resp); resp=NULL;

    /* type: bitmap; score: single btree (drivable order_by). No type+score
     * composite here on purpose — we want the OP_IN path that can't seed a
     * composite, exercising the D2/D3 fork directly. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"p\",\"object\":\"bw\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:8\",\"score:int\"],"
        "\"indexes\":[\"type:bitmap\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bw"); free(resp); resp=NULL;

    /* 1000 rows, almost all type=story (broad), a handful type=job (rare). */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        const char *t = (i % 200 == 0) ? "job" : "story";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"p\",\"object\":\"bw\",\"key\":\"k%04d\","
            "\"value\":{\"type\":\"%s\",\"score\":%d}}", i, t, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* BROAD: type in (story,job) covers ~all rows + ORDER BY score → must WALK. */
    ASSERT_EQ_STR(order_of(&env, "p/bw",
        "[{\"field\":\"type\",\"op\":\"in\",\"value\":\"story,job\"}]", "score"),
        "walk",
        "broad type IN + order score must walk the order index, not sort");

    /* Correctness: top-3 by score desc are the highest keys (score==i). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"p\",\"object\":\"bw\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"in\",\"value\":\"story,job\"}],"
        "\"order_by\":\"score\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_CONTAINS(resp, "\"score\":999", "top score present");
    ASSERT_CONTAINS(resp, "\"score\":998", "2nd score present");
    ASSERT_CONTAINS(resp, "\"score\":997", "3rd score present");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-planner-broad-ordered-walk", test_planner_broad_ordered_walk)
```

> Note: confirm the assert macro names against the template (`ASSERT_EQ_STR`, `ASSERT_CONTAINS`, `ASSERT_NOT_NULL`, `ASSERT_TRUE`). All four are defined in `src/test/test_assert.h` (`ASSERT_EQ_STR(actual, expected, desc)` — actual first). If any differs, use the one that exists and record the substitution in `PLAN_NOTES.md`. Do not invent macros.

- [ ] **Step 2: Build.**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: builds clean (the new file links via `TEST_REGISTER` static-init).

- [ ] **Step 3: Run the new test — verify it FAILS on current code.**

Run: `./build/bin/shard-db-test run test-planner-broad-ordered-walk`
Expected: FAIL on the `"walk"` assertion — current code returns `"sort"` for the broad IN + order-by case. Paste the failing assertion line as proof the test exercises the bug.

---

## Task 2: Add the `pick_sort_or_walk` helper

**Files:**
- Modify: `src/db/query.c`

- [ ] **Step 1: Insert the helper immediately after `order_field_drivable`.**

Search for the end of `order_field_drivable`:
```c
static int order_field_drivable(const char *db_root, const char *object,
                                const char *order_by) {
    if (!order_by || !order_by[0]) return 0;
    return field_has_index_type(db_root, object, order_by, IT_BTREE);
}
```
Add directly below it:
```c
/* D2 (fetch+sort) vs D3 (walk order index, post-filter to limit).
 *
 * D2 only pays off when the candidate set is genuinely small: it materializes
 * and sorts every match before applying limit. A *bitmap* seed yields an EXACT
 * count (estimable && !saturated) that can still be enormous (k ≈ N) — sorting
 * that many rows times out (observed: `type in (...) ORDER BY score` over 4.3M
 * → 30s). So gate on magnitude, not just estimability: when the most-selective
 * seed is still broad (fails the N/g_random_seq_ratio budget) and order_by has
 * a btree to walk, walk it (D3); the broad filter's high post-filter pass-rate
 * fills `limit` in ~limit fetches. Only fall back to sort when order_by isn't
 * drivable (no btree → nothing to walk; D2 is best-effort, bounded by
 * QUERY_BUFFER_MB). Mirrors the cursor path's ORDERED_FIND_KEYSET_MAX guard. */
static FilterOrderKind pick_sort_or_walk(const char *db_root, const char *object,
                                         const char *order_by, CardEst se, size_t N) {
    if (leaf_is_selective(se, N))
        return FP_ORDER_SORT;                       /* small known set → sort */
    if (order_field_drivable(db_root, object, order_by))
        return FP_ORDER_INDEX_WALK;                 /* broad → walk order index */
    return FP_ORDER_SORT;                            /* broad, but no order btree */
}
```

> `pick_sort_or_walk` references `leaf_is_selective` and `order_field_drivable` (both defined above this point) and `FilterOrderKind`/`CardEst`/`FP_ORDER_*` (defined earlier in the file). No forward declarations needed. If the compiler reports any of these as undeclared, the insertion point is wrong — stop and check ordering, note it in `PLAN_NOTES.md`.

- [ ] **Step 2: Build to confirm the helper compiles (unused-function warning is expected until Task 3).**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: builds clean. (`pick_sort_or_walk` is referenced in Task 3; a transient `-Wunused-function` is acceptable between steps but should be gone after Task 3.)

---

## Task 3: Use the helper at both fork sites

**Files:**
- Modify: `src/db/query.c`

- [ ] **Step 1: Replace Site 2 (main fork).**

Search for:
```c
            CardEst se = (prim >= 0) ? est[prim] : (CardEst){0, 0, 0};
            fp.order = (se.estimable && !se.saturated)
                       ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
```
Replace the `fp.order = ...` assignment (keep the `CardEst se = ...` line) with:
```c
            CardEst se = (prim >= 0) ? est[prim] : (CardEst){0, 0, 0};
            fp.order = pick_sort_or_walk(db_root, object, order_by, se, N);
```

- [ ] **Step 2: Replace Site 1 (composite-guard else branch).**

Search for:
```c
                CardEst se = est[prim];
                fp.order = (se.estimable && !se.saturated)
                           ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
```
Replace with:
```c
                CardEst se = est[prim];
                fp.order = pick_sort_or_walk(db_root, object, order_by, se, N);
```

- [ ] **Step 3: Build.**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: builds clean, no `-Wunused-function` for `pick_sort_or_walk`.

- [ ] **Step 4: Run the Task 1 test — verify it now PASSES.**

Run: `./build/bin/shard-db-test run test-planner-broad-ordered-walk`
Expected: PASS. Paste `# total: 1 passed, 0 failed` (or the case's pass line).

---

## Task 4: Regression — selective shapes must NOT change

**Files:**
- Modify: `src/test/cases/test_planner_broad_ordered_walk.c` (extend the existing case)

- [ ] **Step 1: Add selective-path assertions** before `tc_close` in `test_planner_broad_ordered_walk`:

```c
    /* SELECTIVE: type=job is rare (5/1000) → fetch+sort the small set (D2). */
    ASSERT_EQ_STR(order_of(&env, "p/bw",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"}]", "score"),
        "sort",
        "selective type=job + order score must sort (small set), not walk");

    /* order_by NOT drivable: no btree on a non-indexed field → must not pick
     * walk even when broad. Add a plain (unindexed) field via a fresh object. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"p\",\"object\":\"nd\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:8\",\"rank:int\"],"
        "\"indexes\":[\"type:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create nd"); free(resp); resp=NULL;
    for (int i = 0; i < 400; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"p\",\"object\":\"nd\",\"key\":\"k%04d\","
            "\"value\":{\"type\":\"story\",\"rank\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    /* broad type IN, but order_by `rank` has no btree → cannot walk → sort. */
    ASSERT_EQ_STR(order_of(&env, "p/nd",
        "[{\"field\":\"type\",\"op\":\"in\",\"value\":\"story\"}]", "rank"),
        "sort",
        "broad filter but non-drivable order_by must fall back to sort");
```

- [ ] **Step 2: Build & run the case.**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-planner-broad-ordered-walk`
Expected: PASS (all assertions).

---

## Task 5: Full suite — prove no collateral damage

The composite, intersect, cursor, and existing order-by selection cases must all still pass — in particular `test-composite-selectivity-guard`, `test-d1-composite-executor`, `test-d3-order-walk-executor`, `test-find-orderby-selective`, `test-find-filter-first-orderby`, `test-small-prefilter-orderby`, `test-planner-cost-model`.

- [ ] **Step 1: Run the whole suite.**

Run: `./build/bin/shard-db-test run-all`
Expected: `# total: N passed, 0 failed`. Paste the final line. If anything regresses, the most likely culprit is a previously-selective case now classified broad (or vice-versa) — re-read the failing case, confirm the `leaf_is_selective` budget interpretation, and record findings in `PLAN_NOTES.md` rather than weakening the new test.

- [ ] **Step 2: Hand back uncommitted.**

Leave all changes uncommitted. Report the `run-all` total line and the list of files changed.

---

## Self-review checklist (run before handing back)

1. **Both fork sites changed** — grep `src/db/query.c` for `se.estimable && !se.saturated` and confirm **zero** remaining occurrences in the order overlay (the only two were Site 1 and Site 2).
2. **Helper placement** — `pick_sort_or_walk` is defined after `leaf_is_selective` and `order_field_drivable`, before `plan_filter`.
3. **No header/struct changes** — `git diff --stat` should show only `src/db/query.c` and the new test file.
4. **Test asserts both directions** — broad→`walk`, selective→`sort`, non-drivable→`sort`, plus a row-correctness check.
5. **Suite green** — `# total: N passed, 0 failed` pasted.
