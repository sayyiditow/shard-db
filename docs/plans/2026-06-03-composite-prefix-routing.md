# Composite-Prefix Routing for find (+ count/aggregate follow-up) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the planner route `eq_field = V AND … ORDER BY other_field` to an existing `eq_field+other_field` composite index (sorted prefix scan) instead of walking the `order_by` index and post-filtering — fixing the 7–9s `hn/stories` queries where a rare `type` (job/poll) forces a long time-ordered walk.

**Architecture:** The composite executor (`find_via_composite_prefix`) and the `FP_ORDER_COMPOSITE` plan mode already exist and work. The bug is purely in plan *selection*: the D1 order overlay only inspects the single selectivity seed (`fp.source_leaves[0]`), and `most_selective_indexed` structurally prefers a non-bitmap leaf (here `time`), so a bitmap `eq` leaf like `type` — the one with a `type+time` composite — is never considered. The fix adds a helper that scans **all** AND-leaves for an `eq`/`starts_with` leaf whose `<field>+<order_by>` composite exists, and routes to `FP_ORDER_COMPOSITE` driven by that leaf. The rest of the criteria (`dead`, `deleted`, `time>=T`) are post-filtered per record by the existing executor — no executor changes needed.

**Tech Stack:** C (C11), shard-db `src/db/query.c` planner (`plan_filter`), the C test harness in `src/test/cases/` (`TEST_REGISTER`, `tc_request`, `plan_filter_kind_for_test`), `./build.sh` + `./build/bin/shard-db-test`.

**Scope note (read first):** Two phases, sequenced low-risk → higher-risk:

- **Phase A (Tasks 1–3): `eq + ORDER BY` → composite prefix scan.** The demonstrated production pain (`hn/stories` 7–9s). The executor (`find_via_composite_prefix`) already exists; this is a ~15-line planner change. Ship and validate this first.
- **Phase B (Tasks 4–7): multi-field equality → single composite lookup** (backlog item 10). `by=X AND time=Y` with a `by+time` composite, **no `order_by`** — today this intersects two separate index walks; Phase B routes it to ONE exact composite key lookup. Needs a small new executor (`find_via_composite_key`) plus a key builder. Sound because the query-side encoder (`encode_field_for_index`) is byte-identical to the build-side `typed_field_to_index_key` (verified: config.c:1948), so the concatenated search key matches the stored composite key exactly.

**Still deferred (own plan): item 9 — COUNT/AGGREGATE over composites.** Different executors (counting/aggregating callbacks), no measured slow query. See "Future Work."

---

## File Structure

- `src/db/query.c` — the only production file changed.
  - Add `find_covering_composite()` static helper next to the existing `composite_index_exists()` (around line 12789).
  - Modify the D1 order overlay inside `plan_filter()` (around lines 13117–13151).
  - No executor changes: `find_via_composite_prefix()` (≈ line 11280) and the `FP_ORDER_COMPOSITE` dispatch (≈ line 17295) are reused unchanged.
- `src/test/cases/test_composite_prefix_routing.c` — new test case file (planner-selection assertions + end-to-end correctness). Registered via `TEST_REGISTER`; the build auto-discovers `test_*.c`.

Key existing facts the implementation relies on (verified):
- `composite_index_exists(db_root, object, a, b)` returns true iff a btree index named `"a+b"` exists (query.c:12789).
- The D1 gate already restricts composite seeds to `OP_EQUAL`/`OP_STARTS_WITH` because `find_via_composite_prefix` bounds the walk as `[encode(v), encode(v)+0xff×4]` — range/IN seeds would mis-bound (query.c:13122-13130). The new helper keeps the same op restriction.
- The composite executor runs the **full** criteria `tree` per record (`composite_prefix_cb`), so siblings (`dead`, `deleted`, `time>=T`) are correctly post-filtered.
- The executor dispatch requires `fp.order == FP_ORDER_COMPOSITE && fp.kind == FP_PRIMARY_LEAF && fp.n_source >= 1` (query.c:17295), so the override must set all three.
- Test introspection: `plan_filter_kind_for_test(db_root, object, criteria_json, order_by, fetching, out_field, fsz, out_order, osz, out_total_cheap)` returns the plan-kind string and fills `out_field` with `source_leaves[0]->field` and `out_order` with the order-mode string (`"composite"`, `"walk"`, `"sort"`, `"none"`) (query.c:13192).

---

### Task 1: Planner helper + D1 overlay routing

**Files:**
- Modify: `src/db/query.c` (add helper ~12789; edit overlay ~13117-13151)
- Test: `src/test/cases/test_composite_prefix_routing.c` (create)

- [ ] **Step 1: Write the failing planner-selection test**

Create `src/test/cases/test_composite_prefix_routing.c`:

```c
/* Composite-prefix routing: a query of the shape
   `eq_field = V AND range_field >= T  ORDER BY range_field`
   must select FP_ORDER_COMPOSITE driven by eq_field when an
   `eq_field+range_field` composite exists — even though the
   selectivity seed would otherwise be the range_field btree. */
#include "../test.h"
#include "../../db/types.h"
#include <string.h>

/* Plan introspection hook (defined in query.c). */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static int test_composite_prefix_selected(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;

    /* type: enum (low cardinality) bitmap; time: timestamp btree;
       composite type+time. Mirrors hn/stories. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"st\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:enum(story,job,poll)\",\"time:timestamp\"],"
        "\"indexes\":[\"type:bitmap\",\"time\",\"type+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create st");
    free(resp); resp = NULL;

    /* A few rows so get_live_count > 0 (planner reads N). */
    for (int i = 0; i < 6; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"st\",\"key\":\"k%d\","
            "\"value\":{\"type\":\"%s\",\"time\":\"2026-01-%02d 00:00:00\"}}",
            i, (i % 3 == 0 ? "job" : "story"), i + 1);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    char field[64] = {0}, order[32] = {0}; int cheap = -1;
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "d/st",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2026-01-01 00:00:00\"}]",
        "time", 1 /* fetching */,
        field, sizeof(field), order, sizeof(order), &cheap);

    ASSERT_EQ_STR(order, "composite", "order mode is composite");
    ASSERT_EQ_STR(field, "type",      "composite seed is type (not time)");
    ASSERT_EQ_STR(kind,  "leaf",      "kind is PRIMARY_LEAF for composite drive");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-prefix-selected", test_composite_prefix_selected)
```

- [ ] **Step 2: Build and run the test to verify it fails**

Run:
```bash
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-composite-prefix-selected
```
Expected: FAIL — `order` is `"walk"` (D3) and `field` is `"time"`, because the current overlay only checks `source_leaves[0]` (= `time`).

- [ ] **Step 3: Add the `find_covering_composite` helper**

In `src/db/query.c`, immediately after the `composite_index_exists` function (ends ~line 12794), add:

```c
/* Scan all AND-leaves for an EQ/STARTS_WITH leaf whose "<field>+<order_by>"
 * composite btree exists. Returns that leaf's index, or -1 if none.
 *
 * Why this exists: most_selective_indexed() prefers a non-bitmap leaf as the
 * seed, so for `dead=… AND deleted=… AND type=X AND time>=T ORDER BY time`
 * the seed is always `time` (the lone btree), never the bitmap `type` — even
 * though `type+time` is exactly the composite that turns the ordered walk into
 * a bounded prefix scan. The D1 overlay must look past the single seed and
 * find the leaf the composite actually covers. EQ/STARTS_WITH only: those are
 * the ops find_via_composite_prefix bounds correctly (see the D1 gate). */
static int find_covering_composite(const char *db_root, const char *object,
                                   SearchCriterion **leaves, int nL,
                                   const char *order_by) {
    if (!order_by || !order_by[0]) return -1;
    for (int i = 0; i < nL; i++) {
        if (leaves[i]->op != OP_EQUAL && leaves[i]->op != OP_STARTS_WITH) continue;
        if (composite_index_exists(db_root, object, leaves[i]->field, order_by))
            return i;
    }
    return -1;
}
```

- [ ] **Step 4: Wire the helper into the D1 overlay**

In `plan_filter()`, replace the overlay block (currently starting `if (order_by && order_by[0] && fp.kind != FP_FULL_SCAN && fp.n_source > 0) {` at ~13117) so it consults the helper first. The full replacement:

```c
    if (order_by && order_by[0] && fp.kind != FP_FULL_SCAN && fp.n_source > 0) {
        /* D1 (preferred): any indexed eq/starts leaf whose <field>+<order_by>
         * composite exists drives a sorted prefix scan, independent of which
         * leaf most_selective_indexed chose. Fixes the bitmap-eq + ordered-range
         * shape (e.g. type=job ... ORDER BY time) that previously fell to D3. */
        int cc = find_covering_composite(db_root, object, leaves, nL, order_by);
        if (cc >= 0) {
            fp.kind            = FP_PRIMARY_LEAF;  /* composite executor requires this */
            fp.source_is_bitmap = 0;               /* driving via composite btree, not a bitmap */
            fp.source_leaves[0] = leaves[cc];
            fp.n_source         = 1;
            fp.order            = FP_ORDER_COMPOSITE;
        } else if (composite_index_exists(db_root, object,
                                   fp.source_leaves[0]->field, order_by)
            && (fp.source_leaves[0]->op == OP_EQUAL ||
                fp.source_leaves[0]->op == OP_STARTS_WITH)) {
            fp.order = FP_ORDER_COMPOSITE;
        } else {
            /* D2 vs D3: bounded seed → fetch + in-memory sort (D2); broad seed
             * → walk the order_by index directly (D3). */
            CardEst se = est[prim];
            fp.order = (se.estimable && !se.saturated)
                       ? FP_ORDER_SORT : FP_ORDER_INDEX_WALK;
        }
    }
```

(This preserves the original `else`/`else if` behaviour exactly; only the new leading `if (cc >= 0)` branch is added. Keep the original explanatory comments from lines 13118-13130 above this block — do not delete them.)

- [ ] **Step 5: Build and run the test to verify it passes**

Run:
```bash
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-composite-prefix-selected
```
Expected: PASS — `order == "composite"`, `field == "type"`, `kind == "leaf"`.

- [ ] **Step 6: Commit**

```bash
git add src/db/query.c src/test/cases/test_composite_prefix_routing.c
git commit -m "perf(planner): route eq+ORDER BY to a covering composite prefix scan

find_covering_composite scans all AND-leaves for an eq/starts leaf whose
<field>+<order_by> composite exists, instead of only checking the single
selectivity seed (which most_selective_indexed always picks as the non-bitmap
leaf). Fixes hn/stories type=job/poll ORDER BY time falling to a D3 time-walk."
```

---

### Task 2: End-to-end correctness — results are right and correctly ordered

This guards against the historical "composite + order_by returns empty / wrong results" failure mode (the prefix bounds bug noted at query.c:13122-13130). Task 1 only asserts the plan was *chosen*; this asserts the executor *produces correct output*.

**Files:**
- Test: `src/test/cases/test_composite_prefix_routing.c` (add a second case)

- [ ] **Step 1: Write the failing correctness test**

Append to `src/test/cases/test_composite_prefix_routing.c` (before EOF):

```c
static int test_composite_prefix_results_correct(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d2\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d2\",\"object\":\"st\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:enum(story,job,poll)\",\"time:timestamp\"],"
        "\"indexes\":[\"type:bitmap\",\"time\",\"type+time\"]}", &resp);
    free(resp); resp = NULL;

    /* 10 stories interleaved with 3 jobs at known times. Jobs at days 3,6,9. */
    for (int i = 1; i <= 12; i++) {
        char req[256];
        const char *t = (i == 3 || i == 6 || i == 9) ? "job" : "story";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d2\",\"object\":\"st\",\"key\":\"k%02d\","
            "\"value\":{\"type\":\"%s\",\"time\":\"2026-02-%02d 00:00:00\"}}",
            i, t, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* type=job ORDER BY time DESC → expect the 3 jobs newest-first: k09,k06,k03. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d2\",\"object\":\"st\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":25}", &resp);
    ASSERT_NOT_NULL(resp, "find returned");
    ASSERT_TRUE(strstr(resp, "k09") && strstr(resp, "k06") && strstr(resp, "k03"),
                "all 3 jobs returned");
    ASSERT_TRUE(!strstr(resp, "k01") && !strstr(resp, "k02"),
                "no stories leaked into job results");
    /* DESC order: k09 must appear before k06 before k03 in the payload. */
    {
        const char *p9 = strstr(resp, "k09");
        const char *p6 = strstr(resp, "k06");
        const char *p3 = strstr(resp, "k03");
        ASSERT_TRUE(p9 && p6 && p3 && p9 < p6 && p6 < p3,
                    "results are time-desc ordered");
    }
    free(resp); resp = NULL;

    /* Add the range sibling: type=job AND time>='2026-02-05' → only k09,k06. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d2\",\"object\":\"st\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2026-02-05 00:00:00\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":25}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "k09") && strstr(resp, "k06"),
                "range: k09,k06 present");
    ASSERT_TRUE(resp && !strstr(resp, "k03"),
                "range: k03 (before cutoff) excluded by post-filter");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-prefix-results-correct", test_composite_prefix_results_correct)
```

- [ ] **Step 2: Build and run**

Run:
```bash
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-composite-prefix-results-correct
```
Expected: PASS. (With Task 1 applied the query uses the composite; this confirms the executor returns the right ordered, range-post-filtered set.)

- [ ] **Step 3: Run the full suite to confirm no regressions**

Run:
```bash
./build/bin/shard-db-test run-all
```
Expected: `# total: N passed, 0 failed` (N = prior total + the new assertions). Pay attention to `test-plan-*` (planner cost model) cases — they must still pass unchanged.

- [ ] **Step 4: Commit**

```bash
git add src/test/cases/test_composite_prefix_routing.c
git commit -m "test(planner): composite-prefix find returns correct time-desc, range-filtered rows"
```

---

### Task 3: Verify against production query shapes (manual, no code)

**Files:** none (verification only).

- [ ] **Step 1: Confirm the slow shapes now plan as composite**

For each production shape below, the plan must report `order=composite, field=type`. Add them as extra assertions in `test-composite-prefix-selected` if you want them locked in, or verify via a scratch run. Shapes from the slow log:
- `dead=false AND deleted=false AND type=poll AND time>=T ORDER BY time DESC`
- `dead=false AND deleted=false AND type=job  AND time>=T ORDER BY time DESC`

Both have `type` as an `eq` leaf and `type+time` exists → `find_covering_composite` returns the `type` leaf → composite. The `dead`/`deleted`/`time>=T` leaves are post-filtered by the executor.

- [ ] **Step 2: Note the NOT-fixed shapes (expected)**

These remain on their existing paths (no `type` eq + no applicable composite); they are out of scope for this plan:
- `… type=story AND title starts "Show HN" … ORDER BY time` — `title` is trigram, no `title+time` composite. Stays D2/D3.
- `… type=story AND title starts "Ask HN"` (no time filter) — same.

If these need speeding up later, the fix is either a `title+time` composite + the same routing, or a D2 fetch-and-sort on the trigram seed — separate work.

---

---

## Phase B — multi-field equality → single composite lookup (item 10)

Routes `by=X AND time=Y` (both `eq`, both fields of a `by+time` composite, **no `order_by`**) to ONE exact composite-key search instead of intersecting two index walks. Reuses `CompositePrefixCtx` + `composite_prefix_cb` (which already fetch-by-hash and post-filter the full tree); the only new pieces are an exact-key builder and a `btree_idx_search`-based executor.

**Encoding guarantee (verified):** the composite index key for a record is `typed_field_to_index_key(f1) ‖ typed_field_to_index_key(f2)`, and `typed_field_to_index_key` is documented (config.c:1948) to match `encode_field_for_index` for equivalent text input. The query path encodes criterion values via `encode_field_for_index` (`encode_criterion_value` → `encode_field_for_index`, query.c:9810). So `encode_field_for_index(by,"X") ‖ encode_field_for_index(time,"Y")` is byte-identical to the stored composite key. Exact `btree_idx_search` on that key returns exactly the matching records.

**Gate:** Phase B fires only when `order_by` is absent (the listing/equality case). When `order_by` is present, Phase A already handles ordered prefix scans. This keeps the two phases non-overlapping and avoids emitting unordered results for an ordered request.

### Task 4: Plan plumbing — new order mode + composite field on FilterPlan

**Files:**
- Modify: `src/db/query.c` (the `FpOrder` enum ~10648; the `FilterPlan` struct ~10656)

- [ ] **Step 1: Add the enum value**

In the `FpOrder` enum (the one containing `FP_ORDER_COMPOSITE` at ~query.c:10648), add a new value:

```c
    FP_ORDER_COMPOSITE_EXACT, /* all composite fields pinned by eq → exact key lookup (Phase B) */
```

- [ ] **Step 2: Add the composite field name to FilterPlan**

In the `FilterPlan` struct (contains `source_leaves`, `n_source` ~10656), add:

```c
    char composite_field[256];   /* set when order==FP_ORDER_COMPOSITE_EXACT */
```

- [ ] **Step 3: Update the order-name debug stringifier**

In `fp_order_str` (the switch at ~query.c:13189), add a case so tests can assert it:

```c
    case FP_ORDER_COMPOSITE_EXACT: return "composite_exact";
```

- [ ] **Step 4: Build to confirm it compiles**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: builds clean (enum value unused so far is fine in C).

- [ ] **Step 5: Commit**

```bash
git add src/db/query.c
git commit -m "planner: add FP_ORDER_COMPOSITE_EXACT mode + composite_field slot"
```

### Task 5: Exact-composite key builder + planner detection

**Files:**
- Modify: `src/db/query.c` (helpers near `composite_index_exists` ~12789; detection inside `plan_filter` after `prim` is resolved, ~12988)
- Test: `src/test/cases/test_composite_prefix_routing.c`

- [ ] **Step 1: Write the failing detection test**

Append to `src/test/cases/test_composite_prefix_routing.c`:

```c
static int test_exact_composite_selected(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d3\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d3\",\"object\":\"ev\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"by:varchar:32\",\"time:timestamp\"],"
        "\"indexes\":[\"by\",\"time\",\"by+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ev"); free(resp); resp=NULL;
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d3\",\"object\":\"ev\",\"key\":\"k%d\","
            "\"value\":{\"by\":\"alice\",\"time\":\"2026-03-0%d 00:00:00\"}}", i, i+1);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    char field[64]={0}, order[32]={0}; int cheap=-1;
    plan_filter_kind_for_test(env.db_root, "d3/ev",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"},"
        " {\"field\":\"time\",\"op\":\"eq\",\"value\":\"2026-03-02 00:00:00\"}]",
        NULL /* no order_by */, 1, field, sizeof(field), order, sizeof(order), &cheap);
    ASSERT_EQ_STR(order, "composite_exact", "by=X AND time=Y → exact composite");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-exact-composite-selected", test_exact_composite_selected)
```

- [ ] **Step 2: Run to verify it fails**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-exact-composite-selected`
Expected: FAIL — `order` is `intersect` or `leaf`, not `composite_exact`.

- [ ] **Step 3: Add the key builder + composite-name iterator + detector**

Near `composite_index_exists` (~query.c:12794), add. (For the index-name list, mirror how `field_has_index_type` reads the per-object index cache — it exposes the object's index names; iterate them and pick those containing `'+'`. If no public iterator exists, add a thin `int list_object_indexes(db_root, object, char names[][256], int max)` that reads `<obj>/indexes/index.conf` line-by-line via `split_index_spec`, same as `idx_cache_ensure` at config.c:1077.)

```c
/* Build the exact composite key for `composite_field` (e.g. "by+time") from
 * eq leaves, in the composite's field order. encode_field_for_index is the
 * same encoder typed_field_to_index_key uses at build time (config.c:1948),
 * so the key is byte-identical to the stored composite key. Returns 1 with
 * *out_len set iff EVERY sub-field is matched by an OP_EQUAL leaf. */
static int build_exact_composite_key(FieldSchema *fs, const char *composite_field,
                                     SearchCriterion **leaves, int nL,
                                     uint8_t *out, size_t *out_len) {
    char buf[256]; strncpy(buf, composite_field, sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    size_t total = 0; char *save = NULL;
    for (char *sub = strtok_r(buf, "+", &save); sub; sub = strtok_r(NULL, "+", &save)) {
        SearchCriterion *m = NULL;
        for (int i = 0; i < nL; i++)
            if (leaves[i]->op == OP_EQUAL && strcmp(leaves[i]->field, sub) == 0) { m = leaves[i]; break; }
        if (!m) return 0;  /* sub-field not pinned by an eq leaf → not exact-covered */
        const TypedField *tf = resolve_idx_field(fs ? fs->ts : NULL, sub);
        size_t l = 0;
        encode_field_for_index(tf, m->value, strlen(m->value), out + total, &l);
        total += l;
    }
    *out_len = total;
    return 1;
}

/* Find a composite index fully covered by eq leaves. Returns its name in
 * out_name (size>=256) and 1 if found, else 0. Iterates the object's index
 * names (cache-backed; composites contain '+'). */
static int find_exact_covering_composite(const char *db_root, const char *object,
                                         FieldSchema *fs,
                                         SearchCriterion **leaves, int nL,
                                         char *out_name) {
    char names[MAX_FIELDS][256];
    int n = list_object_indexes(db_root, object, names, MAX_FIELDS);
    for (int i = 0; i < n; i++) {
        if (!strchr(names[i], '+')) continue;            /* composites only */
        uint8_t key[1024]; size_t klen = 0;
        if (build_exact_composite_key(fs, names[i], leaves, nL, key, &klen) && klen > 0) {
            snprintf(out_name, 256, "%s", names[i]);
            return 1;
        }
    }
    return 0;
}
```

- [ ] **Step 4: Wire detection into `plan_filter`**

Immediately after `if (prim < 0) { fp.kind = FP_FULL_SCAN; return fp; }` (~query.c:12988), before the multi-leaf AND block, add:

```c
    /* Phase B: a composite fully pinned by eq leaves → exact key lookup,
     * skipping the two-index intersect. Only without order_by (Phase A owns
     * ordered prefix scans). */
    if (!(order_by && order_by[0]) && nL >= 2) {
        char cname[256];
        if (find_exact_covering_composite(db_root, object, fs, leaves, nL, cname)) {
            fp.kind = FP_PRIMARY_LEAF;
            fp.order = FP_ORDER_COMPOSITE_EXACT;
            snprintf(fp.composite_field, sizeof(fp.composite_field), "%s", cname);
            /* All covered leaves go in source_leaves (composite order) so the
             * executor can rebuild the key; siblings stay in the tree for
             * post-filter. n_source>0 satisfies downstream guards. */
            fp.n_source = 0;
            char tmp[256]; strncpy(tmp, cname, sizeof(tmp)-1); tmp[sizeof(tmp)-1]='\0';
            char *sv=NULL;
            for (char *sub=strtok_r(tmp,"+",&sv); sub && fp.n_source<MAX_INTERSECT_LEAVES;
                 sub=strtok_r(NULL,"+",&sv))
                for (int i=0;i<nL;i++)
                    if (leaves[i]->op==OP_EQUAL && strcmp(leaves[i]->field,sub)==0)
                        { fp.source_leaves[fp.n_source++]=leaves[i]; break; }
            return fp;
        }
    }
```

- [ ] **Step 5: Run the detection test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-exact-composite-selected`
Expected: PASS — `order == "composite_exact"`.

- [ ] **Step 6: Commit**

```bash
git add src/db/query.c src/test/cases/test_composite_prefix_routing.c
git commit -m "planner: detect eq-covered composite → FP_ORDER_COMPOSITE_EXACT"
```

### Task 6: Exact-composite executor + find dispatch

**Files:**
- Modify: `src/db/query.c` (add `find_via_composite_key` near `find_via_composite_prefix` ~11278; dispatch in `cmd_find` near the `FP_ORDER_COMPOSITE` branch ~17295)
- Test: `src/test/cases/test_composite_prefix_routing.c`

- [ ] **Step 1: Write the failing correctness test**

Append to `src/test/cases/test_composite_prefix_routing.c`:

```c
static int test_exact_composite_results(void) {
    TestEnv env; TestConn *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    tc = tc_connect(&env);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d4\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d4\",\"object\":\"ev\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"by:varchar:32\",\"time:timestamp\"],"
        "\"indexes\":[\"by\",\"time\",\"by+time\"]}", &resp);
    free(resp); resp=NULL;
    /* alice@day2 is the target; decoys share one field but not both. */
    const char *rows[][3] = {
        {"k1","alice","2026-03-02 00:00:00"},  /* match */
        {"k2","alice","2026-03-03 00:00:00"},  /* same by, diff time */
        {"k3","bob",  "2026-03-02 00:00:00"},  /* same time, diff by */
        {"k4","alice","2026-03-02 00:00:00"},  /* match (dup by+time) */
    };
    for (int i=0;i<4;i++){ char req[256];
        snprintf(req,sizeof(req),
          "{\"mode\":\"insert\",\"dir\":\"d4\",\"object\":\"ev\",\"key\":\"%s\","
          "\"value\":{\"by\":\"%s\",\"time\":\"%s\"}}", rows[i][0],rows[i][1],rows[i][2]);
        tc_request(tc,req,&resp); free(resp); resp=NULL; }

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d4\",\"object\":\"ev\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"},"
        " {\"field\":\"time\",\"op\":\"eq\",\"value\":\"2026-03-02 00:00:00\"}]}", &resp);
    ASSERT_NOT_NULL(resp, "find returned");
    ASSERT_TRUE(strstr(resp,"k1") && strstr(resp,"k4"), "both alice@day2 rows returned");
    ASSERT_TRUE(!strstr(resp,"k2") && !strstr(resp,"k3"), "decoys excluded");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-exact-composite-results", test_exact_composite_results)
```

- [ ] **Step 2: Run to verify it fails**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-exact-composite-results`
Expected: FAIL — no dispatch for `FP_ORDER_COMPOSITE_EXACT` yet, so results are wrong/empty (the planner returns the new mode but `cmd_find` has no branch).

- [ ] **Step 3: Add the executor**

After `find_via_composite_prefix` (ends ~query.c:11335), add. It reuses `CompositePrefixCtx` + `composite_prefix_cb` verbatim (they fetch-by-hash and post-filter `tree`):

```c
/* Exact composite lookup: walk the composite btree for the single concatenated
 * key built from the eq leaves. composite_prefix_cb fetches each hash and
 * post-filters the full tree (so any non-composite siblings still apply). */
static int find_via_composite_key(const char *db_root, const char *object,
                                  const Schema *sch, FieldSchema *fs,
                                  const char *composite_field,
                                  SearchCriterion **eq_leaves, int n_eq,
                                  CriteriaNode *tree, ExcludedKeys *excluded,
                                  int offset, int limit,
                                  const char **proj_fields, int proj_count,
                                  int dict_fmt, QueryDeadline *dl) {
    /* Rebuild the exact key in composite field order. eq_leaves are already in
     * that order (planner filled them by walking the composite name). */
    uint8_t key[1024]; size_t klen = 0;
    if (!build_exact_composite_key(fs, composite_field, eq_leaves, n_eq, key, &klen)
        || klen == 0)
        return 0;

    CompositePrefixCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
    ctx.tree = tree; ctx.excluded = excluded;
    ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
    ctx.dict_fmt = dict_fmt;
    ctx.skip_remaining = (offset > 0) ? offset : 0;
    ctx.limit = (limit > 0) ? limit : INT_MAX;
    ctx.dl = dl; ctx.parent_out = g_out;

    btree_idx_search(db_root, object, composite_field, sch->splits,
                     (const char *)key, klen, composite_prefix_cb, &ctx);
    return ctx.printed;
}
```

(Confirm `composite_prefix_cb`'s signature matches `bt_result_cb` — it is `int(const char *val, size_t vlen, const uint8_t *hash16, void *ctx)` at query.c:11184, the exact `bt_result_cb` type `btree_idx_search` expects.)

- [ ] **Step 4: Add the dispatch in `cmd_find`**

Immediately before the `if (fp.order == FP_ORDER_COMPOSITE && fp.kind == FP_PRIMARY_LEAF …)` block (~query.c:17295), add a sibling branch with the same guards + envelope close:

```c
    if (fp.order == FP_ORDER_COMPOSITE_EXACT &&
        !has_joins && !rows_fmt && !csv_delim && fp.n_source >= 1) {
        find_via_composite_key(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            fp.composite_field, fp.source_leaves, fp.n_source,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        size_t ex_total = 0; int ex_null = 1;
        if (want_total) ex_total = fp_compute_total(&fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &ex_null);
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else if (ex_null) OUT("},\"total\":null}\n");
            else OUT("},\"total\":%zu}\n", ex_total);
        } else {
            if (!want_total) OUT("]\n");
            else if (ex_null) OUT("],\"total\":null}\n");
            else OUT("],\"total\":%zu}\n", ex_total);
        }
        free_excluded(&excluded);
        free_criteria_tree(tree);
        free_joins(joins, njoins);
        return 0;
    }
```

(If `fp_compute_total` does not yet recognize `FP_ORDER_COMPOSITE_EXACT`, have it fall back to `null` for that mode — `want_total` on an unordered exact lookup is rare; do not block on it.)

- [ ] **Step 5: Run the correctness test**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-exact-composite-results`
Expected: PASS — `k1,k4` returned; `k2,k3` excluded.

- [ ] **Step 6: Commit**

```bash
git add src/db/query.c src/test/cases/test_composite_prefix_routing.c
git commit -m "feat(query): exact composite-key lookup for multi-eq find (item 10)"
```

### Task 7: Full-suite regression + verify intersect path untouched

**Files:** none (verification only).

- [ ] **Step 1: Run the full suite**

Run: `./build/bin/shard-db-test run-all`
Expected: `0 failed`. Specifically confirm `test-bm-intersect-count`, `test-plan-b1-two-selective-btree`, and `test-plan-*` still pass — Phase B only diverts queries that have a fully-eq-covered composite; two-selective-btree without a covering composite must still report `intersect`.

- [ ] **Step 2: Confirm a non-covered two-eq still uses intersect**

Add to `test-exact-composite-selected` (or a sibling): an object with `by` + `time` indexed but **no** `by+time` composite, query `by=X AND time=Y` → assert `order != "composite_exact"` (should be `intersect`/`leaf`). This pins that Phase B only fires when the composite actually exists.

---

## Future Work (separate plan — backlog item 9)

**Not in this plan.** COUNT/AGGREGATE over composites need their own executors and have no measured slow query yet:

- **Item 9 — COUNT/AGGREGATE via composite:** `count {by=X AND time=Y}` and `aggregate … group_by …` could reuse the exact/prefix composite key with a counting/aggregating callback instead of `composite_prefix_cb`. Generalize the walk into `walk_composite_key(…, cb, ctx)` and add count/agg callbacks. Dispatch points: count `query.c:15813` (FP_INTERSECT branch), aggregate `query.c:22430`. Plan this once a slow count/agg is actually observed.

---

## Execution notes (for the worker)

- Work on a branch off `main`: `git checkout -b perf/composite-prefix-routing`.
- Do tasks **in order**; each ends in a commit. Phase A (Tasks 1–3) is independently shippable — if you stop after Task 3, that's a complete, valuable PR.
- Run `SKIP_TESTS=1 ./build.sh` to build; `./build/bin/shard-db-test run <name>` for one case; `./build/bin/shard-db-test run-all` for the suite. Never claim a step passed without pasting the actual output.
- If a step's code doesn't match the current line numbers (the file moves as you edit), search for the quoted anchor text rather than trusting the line number.
- Do **not** invent behaviour beyond the steps. If something is ambiguous or a referenced symbol (e.g. `list_object_indexes`, `fp_compute_total` handling of the new mode) isn't where expected, stop and leave a note in the commit body rather than guessing.

## Self-Review

- **Spec coverage:** Phase A (Tasks 1–3) = `eq + ORDER BY` → composite prefix scan (the production slow queries). Phase B (Tasks 4–7) = multi-eq `by=X AND time=Y` → exact composite lookup (item 10, the user's explicit ask). Item 9 (count/agg) deferred with rationale.
- **Placeholder scan:** All code is literal. Two intentional "confirm the primitive" notes remain, each bounded and actionable: (a) Task 5 may need a thin `list_object_indexes` if no index-name iterator exists — the implementation (read `index.conf` via `split_index_spec`) is given; (b) Task 6 notes `fp_compute_total` may fall back to `null` for the new mode. Neither is a "TODO later" — both have a concrete default.
- **Type consistency (Phase A):** `find_covering_composite(db_root, object, leaves, nL, order_by)` defined Task 1 Step 3, called Step 4 — same signature.
- **Type consistency (Phase B):** `FP_ORDER_COMPOSITE_EXACT` (Task 4) used in Tasks 5/6; `fp.composite_field` (Task 4) set in Task 5, read in Task 6. `build_exact_composite_key(fs, composite_field, leaves, nL, out, out_len)` defined Task 5 Step 3, reused in Task 6 Step 3. `find_exact_covering_composite(...)` defined + called Task 5. `find_via_composite_key(...)` defined Task 6 Step 3, called Task 6 Step 4. All reuse existing symbols verified in query.c: `CompositePrefixCtx`, `composite_prefix_cb` (matches `bt_result_cb`), `btree_idx_search`, `encode_field_for_index`, `resolve_idx_field`, `MAX_INTERSECT_LEAVES`, `fp_compute_total`, `free_excluded`/`free_criteria_tree`/`free_joins`.
