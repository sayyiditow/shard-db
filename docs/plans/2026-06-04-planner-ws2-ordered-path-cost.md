# Planner upgrade — Workstream 2: unified, limit-aware ordered-path cost + varchar composite bound

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Make ordered finds pick the *cheap* path for **small-but-sparse-in-order** result sets, consistently across the non-cursor and cursor paths, and fix the varchar composite-walk that scans past its partition. Today three separate, non-limit-aware thresholds decide fetch-and-sort vs index-walk, the cursor path can't fetch-and-sort at all, and a varchar-seeded composite walk over-scans — so queries like `type=job … ORDER BY score` (19s), `title starts "Ask HN" ORDER BY time` (6s), and `by=lif ORDER BY time` (31s) all hit slow walks despite tiny result sets.

**Across modes:** the decision logic (`pick_sort_or_walk`, the cursor prefilter block) and the composite executor are shared by **find / count / aggregate** ordered paths — one fix improves all three.

**Architecture (three independent parts, one branch):**
- **Part A — varchar composite bound.** Fix `find_via_composite_prefix` so a varchar (length-prefixed, variable-width) seed bounds the walk to its own partition, like enum/int seeds already do. Fixes the `by=X ORDER BY time` (D1) over-scan.
- **Part B — unified, limit-aware sort-vs-walk.** Replace the scattered cutoffs (`pick_sort_or_walk`'s `N/g_random_seq_ratio`, the executor's `SMALL_PREFILTER_THRESHOLD = 1000`) with one cost function that factors `offset+limit`: fetch+sort when `candidates ≲ sqrt((offset+limit)·N)`, else walk. (Walk cost ≈ `(offset+limit)/pass_rate`; sort cost ≈ `candidates`; they cross at `sqrt((offset+limit)·N)`.)
- **Part C — cursor fetch+sort.** Give the cursor path the same small-candidate fetch+sort shortcut the non-cursor path has (it currently only prefilter-walks or walks).

**Tech Stack:** C. Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run <name>` / `run-all`.

---

## Execution rules

- **Branch off `main` AFTER WS1 (`perf/planner-materialization-guard`) merges** — this shares `src/db/query.c` with WS1. `git checkout main && git pull && git checkout -b perf/planner-ws2-ordered-path-cost`. Confirm WS1's `g_ordered_find_keyset_max` global and the `FP_INTERSECT` guard are present before starting; if not, WS1 isn't merged yet — STOP.
- Leave work **uncommitted**; reviewer commits. Locate edits by **quoted anchor text**.
- Build `SKIP_TESTS=1 ./build.sh`; test via `./build/bin/shard-db-test`. **Never report green without the real `# total: N passed, 0 failed`.**
- If any anchor/symbol/signature differs, **STOP and write `PLAN_NOTES.md`**.
- Do the parts **in order** (A → B → C); each builds + tests green before the next.

---

## Evidence (prod, warm)

| query | time | note |
|---|---|---|
| `by=lif` (comments) ORDER BY time, limit **1** | 16ms | partition walk fills fast |
| `by=lif` ORDER BY time, limit **25** | **31s** | walk runs past lif's 30-row partition (Part A) |
| `by=lif` no order_by | 33ms | the `by` index itself is fine |
| `type=story` ORDER BY score (enum composite) | 72ms | enum seed bounds correctly — varchar is the bug |
| `type=job` + 7d + ORDER BY score, cursor:null | 19s | small set, sparse in score, cursor can't fetch+sort (Part C) |
| `title starts "Ask HN"` ORDER BY time, cursor:null | 6s | same class |

Audit refs: `pick_sort_or_walk` ignores `offset+limit` (gates on `leaf_is_selective`, budget `N/g_random_seq_ratio`); non-cursor executor has `SMALL_PREFILTER_THRESHOLD = 1000`; the cursor block builds a prefilter keyset then walks (no fetch+sort path).

---

## PART A — varchar composite-seed bound

### Reference anchors (`find_via_composite_prefix`, src/db/query.c)
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
```
The walk then runs `btree_idx_walk_ordered(... buf_lo,len_lo ... buf_hi,len_hi ... order_desc, composite_prefix_cb, &ctx)`.

The `+ 0xff*4` upper-bound idiom assumes the seed's encoding is a **fixed-width prefix** of the composite key (true for enum/int/date/etc.). A **varchar** seed encodes as `[uint16 length][content]` — variable width — so `prefix + 0xff*4` does **not** correctly cap "all composite keys whose first field == this varchar value": the following bytes are `encode(order_by)`, and for the seek to confine to the seed's partition the upper bound must be the seed prefix with its **last content byte incremented** (or the seed prefix followed by the max order_by value), not `seed ‖ 0xff*4` appended after a length-prefixed field of a different effective width. (Symptom: `by=lif` desc walk runs past lif's 30 rows → 31s; enum `type=story` is fine.)

- [ ] **A1: Add a scan counter to the composite walk** so the test can prove bounding. Find `composite_prefix_cb` and add at the top of its body (guarded):
```c
#ifdef TEST_BUILD
    extern long g_order_walk_scanned;   /* defined in the order-walk section */
    g_order_walk_scanned++;
#endif
```
> WS1/the cursor fix already define `g_order_walk_scanned` under `TEST_BUILD` with `order_walk_scanned_for_test()`/`order_walk_scanned_reset_for_test()`. Reuse them. If the `extern` needs a different placement, match how other callbacks reference it.

- [ ] **A2: Write the RED test** `src/test/cases/test_composite_varchar_bound.c` (idioms from `test_composite_typed.c`/`test_d1_composite_executor.c`). Create an object with a **varchar** field `name`, an int `t`, and a `name+t` composite; insert ~2000 rows where `name="rare"` for 5 rows (spread across the `t` range) and `name="common"` for 1995; then a `find name=rare ORDER BY t DESC limit 5` and assert via the scan counter that it scans **~the rare partition (≤ ~50)**, not ~2000:
```c
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
extern long order_walk_scanned_for_test(void);
extern void order_walk_scanned_reset_for_test(void);

static int test_composite_varchar_bound(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg); ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"v\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"v\",\"object\":\"ob\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:16\",\"t:long\"],"
        "\"indexes\":[\"name\",\"t\",\"name+t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create"); free(resp); resp=NULL;
    for (int i = 0; i < 2000; i++) {
        char req[256];
        const char *nm = (i % 400 == 0) ? "rare" : "common";  /* 5 rare, spread */
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"v\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"name\":\"%s\",\"t\":%d}}", i, nm, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    order_walk_scanned_reset_for_test();
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"v\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":5}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"rare\"", "returns rare rows");  /* correctness */
    long scanned = order_walk_scanned_for_test();
    ASSERT_TRUE(scanned < 50, "varchar composite walk bounded to its partition, not full index");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-varchar-bound", test_composite_varchar_bound)
```
Register in `build.sh`. Build + run: it should **FAIL** the `scanned < 50` assert (it over-scans ~2000) — that's the RED proving the bug. Paste the failing assertion.
> If the planner doesn't pick D1 composite for this shape (verify with `plan_filter_kind_for_test` returning `"composite"`), the bug is upstream in `find_covering_composite`/`composite_index_exists` for varchar — STOP and write `PLAN_NOTES.md` with the actual plan kind before changing the executor.

- [ ] **A3: Fix the upper bound for varchar seeds** in `find_via_composite_prefix`. The correct partition upper bound is "the seed prefix, exclusive of the next value" — i.e. increment the seed's encoded content rather than appending `0xff*4` after a length-prefixed field. Implement the minimal correct bound (and set the walk's `max_excl`/exclusivity accordingly) so the desc walk stops at the end of the seed's partition. Quote the exact replacement in `PLAN_NOTES.md` if the shape differs from the anchor. Re-run A2 → **GREEN** (`scanned < 50`), correctness intact.
> Keep enum/int behaviour unchanged — gate the new bound on the seed field being varchar (`seed_tf`'s type), or use a single correct "prefix successor" that works for both. Verify `test_d1_composite_executor` and `test_composite_typed` still pass.

- [ ] **A4:** `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` → green.

---

## PART B — unified, limit-aware sort-vs-walk

### Anchors
- `static FilterOrderKind pick_sort_or_walk(const char *db_root, const char *object, const char *order_by, CardEst se, size_t N)` — currently `sel = leaf_is_selective(se,N); return sel?SORT : (drivable?WALK:SORT)`. **No limit param.**
- `#define SMALL_PREFILTER_THRESHOLD 1000` and its use in the non-cursor executor.
- `plan_filter(..., int fetching, int limit)` — `limit` is already a parameter of `plan_filter`, and both call sites pass it; `pick_sort_or_walk` just isn't given it.

- [ ] **B1: Add a shared cost helper** near `pick_sort_or_walk`:
```c
/* Crossover for fetch+sort (D2) vs order-index walk (D3). D2 cost ≈ K (fetch+
 * sort all candidates). D3 cost ≈ (offset+limit)/pass_rate ≈ (offset+limit)*N/K
 * (walk the order index until `limit` matches fill). They cross at
 * K ≈ sqrt((offset+limit)*N): below it sort wins, above it walk wins. This
 * replaces the limit-agnostic N/g_random_seq_ratio cutoff and the fixed
 * SMALL_PREFILTER_THRESHOLD with one decision. */
static int prefer_fetch_sort(size_t candidates, size_t N, int offset, int limit) {
    if (candidates == 0) return 1;
    size_t want = (size_t)((offset > 0 ? offset : 0) + (limit > 0 ? limit : 1));
    /* sort wins when candidates*candidates < want*N (avoids sqrt/float). */
    /* guard overflow: candidates capped well below 2^32 in practice. */
    return candidates * candidates < want * N;
}
```

- [ ] **B2: Thread `limit`/`offset` into `pick_sort_or_walk`** and use the cost helper. Change its signature to accept `int offset, int limit`, and replace the body:
```c
static FilterOrderKind pick_sort_or_walk(const char *db_root, const char *object,
                                         const char *order_by, CardEst se, size_t N,
                                         int offset, int limit) {
    int driv = order_field_drivable(db_root, object, order_by);
    /* Unestimable/saturated → can't size the set; only walk if drivable. */
    if (!se.estimable || se.saturated)
        return driv ? FP_ORDER_INDEX_WALK : FP_ORDER_SORT;
    if (prefer_fetch_sort(se.k, N, offset, limit) || !driv)
        return FP_ORDER_SORT;
    return FP_ORDER_INDEX_WALK;
}
```
Update **both** call sites of `pick_sort_or_walk` (grep them) to pass the planner's `offset`/`limit`. (`plan_filter` has `limit`; if it lacks `offset`, pass `0` — offset is usually small for these paths; note it.)

- [ ] **B3: Retire / align `SMALL_PREFILTER_THRESHOLD`.** The non-cursor executor's `keyset_size(prefilter_ks) <= SMALL_PREFILTER_THRESHOLD` override should use the same cost decision so it's consistent with the planner. Replace the `<= SMALL_PREFILTER_THRESHOLD` check with `prefer_fetch_sort(keyset_size(prefilter_ks), <N>, offset, limit)` (use the live count `N` available there; if not in scope, fetch via `get_live_count`). Keep the executor-time override (it has the *actual* keyset size, better than the estimate) but make its rule match B1.

- [ ] **B4: Test** — extend `test_order_walk_range_bounds.c` or add `test_planner_sort_vs_walk.c`: with `N` known, assert via `plan_filter_kind_for_test` that a **small set sparse in order** (e.g. a rare bitmap value, k≈5, on a 2000-row object, order by an indexed int, limit 25) returns `"sort"` (was `"walk"` under the old N/8 rule for mid-size sets), and a **broad** set returns `"walk"`. Build + run.
> Pick sizes so the crossover flips: with N=2000, limit=25, sqrt(25*2000)≈224 — so k=5 → sort, k=1000 → walk. Use those.

- [ ] **B5:** `run-all` green.

---

## PART C — cursor fetch+sort shortcut

### Anchor
The cursor block in `cmd_find`: after `build_keyset_from_plan` → `cursor_prefilter_ks`, it either walks with the prefilter or (if NULL/oversized) walks without. There is **no** fetch+sort branch.

- [ ] **C1:** Before the cursor walk, add a small-candidate fetch+sort branch mirroring the non-cursor `SMALL_PREFILTER_THRESHOLD` shortcut, gated by the **same** `prefer_fetch_sort(keyset_size(cursor_prefilter_ks), cursor_N_live, offset, limit)` decision: when the prefilter keyset is small relative to the cost crossover, fetch those candidates, run the full criteria tree, sort by `order_by`, emit `limit`, and produce the next-page cursor from the last emitted row (same envelope shape the cursor path already returns). Reuse the non-cursor fetch+sort machinery (`find_via_fetch_sort` / the `SmallPrefilterRow` path) if it can be shared; otherwise factor a helper both call.
> The tricky bit is the **cursor envelope + next-cursor** from a sorted small set: derive `{<order_by>: lastRow.order_value, key: lastRow.key}` from the last emitted row. Verify against the existing cursor response format. If sharing `find_via_fetch_sort` would change its output framing, write a thin cursor-aware variant and note it.

- [ ] **C2: Test** in `test_order_walk_range_bounds.c`: a sparse-in-order small set via `cursor:null` (e.g. rare value + order by an unrelated indexed field, k≈5, limit 25) must be **bounded** — assert `order_walk_scanned_for_test()` is small (fetch+sort path scans ~k, not the whole index) AND the rows + next cursor are correct. Add a second page (resume from the returned cursor) asserting no dupes/gaps.

- [ ] **C3:** `run-all` green. `git diff --stat` should show `src/db/query.c`, `build.sh`, and the new/edited test files only. Hand back uncommitted.

---

## Scale validation (reviewer, on prod, after build+ship)

- [ ] `by=lif` (comments) `ORDER BY time DESC limit 25` (non-cursor): **31s → < ~50ms** (Part A); rows identical.
- [ ] `type=job` + 7d + `ORDER BY score` **cursor:null**: **19s → < ~100ms** (Parts B+C); rows identical.
- [ ] `title starts "Ask HN" ORDER BY time` cursor:null: **6s → fast**.
- [ ] Spot-check `count`/`aggregate` with the same selective filters are unaffected/faster.
- [ ] Re-warm the explorer cache and confirm the listing/profile queries drop out of the slow log even at `SLOW_QUERY_MS=500`.

---

## Self-review

1. Part A bounds **varchar** seeds without changing enum/int (existing composite tests green); scan-counter proves it.
2. Part B: one `prefer_fetch_sort` used by `pick_sort_or_walk` **and** the executor override; both `pick_sort_or_walk` call sites updated; limit/offset threaded.
3. Part C: cursor path can now fetch+sort; next-cursor correct; multi-page no dupes/gaps.
4. No silent regressions: `test_d1_composite_executor`, `test_composite_typed`, `test_find_cursor`, `test_order_walk_range_bounds`, `test_planner_*` all green.
5. `run-all` total pasted.

---

## Roadmap context

WS2 of the planner upgrade. **WS3** (next, separate plan): the op-capability table unification + `OP_IN` composite seeding (`in`-fold), plus the issue-C bitmap `OP_IN` card-est verification and the standing 25M-`users` scale acceptance test. WS2 deliberately leaves the *op-coverage* gaps to WS3; it only fixes the *cost/decision* + the varchar-bound executor bug.
