# Implementation Plan: Add "explain" Flag to Query Modes

**Date:** 2026-06-18  
**Feature:** Add `"explain":true` flag on find/count/aggregate modes. When set, query is NOT executed — returns JSON plan instead of results.

## Execution Rules

1. **Branch:** Create from `main` via `git checkout -b feat/explain-query`
2. **Build:** `SKIP_TESTS=1 ./build.sh`
3. **Test:** 
   - `./build/bin/shard-db-test run-all` (all tests)
   - `./build/bin/shard-db-test run test-explain` (new test)
4. **Anchor-based edits:** All locations use quoted anchor text, not line numbers
5. **If anchor not found:** Stop immediately and write `PLAN_NOTES.md` explaining the mismatch

## Expected Output Format

```json
{
  "plan": "leaf",
  "order": "sort",
  "total_cheap": false,
  "table_rows": 5700000,
  "source": [
    {"field":"score","op":"gt","index":"btree","role":"seed","estimated_rows":48200}
  ],
  "postfilter": [
    {"field":"title","op":"contains","index":null,"role":"postfilter","estimated_rows":null}
  ],
  "hints": [
    {"type":"add_index","field":"title","reason":"contains op on unindexed varchar field; trigram index would avoid full record scan"},
    {"type":"composite_index","field":"score+created_at","reason":"filter on score + order_by created_at; composite index eliminates in-memory sort"}
  ]
}
```

**Response fields:**
- `plan`: One of `"leaf"`, `"scan"`, `"bitmap"`, `"intersect"`, `"union"`
- `order`: One of `"none"`, `"composite"`, `"composite_exact"`, `"sort"`, `"index_walk"`
- `total_cheap`: Boolean — true if total count is O(1) from KeySet
- `table_rows`: Integer — live record count (calls `get_live_count()`)
- `source`: Array of leaf criteria that drive the plan (indexed seed leaves)
- `postfilter`: Array of leaf criteria that require per-record filtering
- `hints`: Array of optimization suggestions; see below

**Hints (emitted when `table_rows >= EXPLAIN_HINT_MIN_ROWS`, except add_trigram_index which is always emitted):**
- `{"type":"add_index","field":"<name>","reason":"..."}` — suggest btree index
- `{"type":"add_trigram_index","field":"<name>","reason":"..."}` — suggest trigram index (NO threshold)
- `{"type":"composite_index","field":"f1+f2","reason":"..."}` — suggest composite index

---

## Task 1: TDD — Write Test Case

**Location:** `src/test/cases/test_explain.c`

**Note on hint threshold:** The production threshold is `EXPLAIN_HINT_MIN_ROWS 100000`. Task 2 introduces a `#ifdef TEST_BUILD` override that sets it to 0 so hints always emit in tests. This test relies on that override — do Task 2 before running this test.

**Full code block:**

```c
/* src/test/cases/test_explain.c
 * Test the explain query mode — verify plan structure and hint generation.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Small enough to insert quickly (<1s), large enough for planner selectivity. */
#define N_ROWS 100

static int test_explain_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Setup: add dir, create object.
       Index both score and created_at so we can test plan==intersect. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ex\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:256\",\"created_at:int\"],"
        "\"indexes\":[\"score\",\"created_at\"]}",
        &resp);
    free(resp); resp = NULL;

    /* Populate N_ROWS records — one at a time is fine at this scale (<1s). */
    for (int i = 0; i < N_ROWS; i++) {
        char buf[256];
        snprintf(buf, sizeof(buf),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ex\","
            "\"key\":\"k%03d\",\"value\":{\"score\":%d,\"title\":\"title%d\","
            "\"created_at\":%d}}",
            i, i % 20, i, 1000000 + i);
        tc_request(tc, buf, &resp);
        free(resp); resp = NULL;
    }

    /* -- Test 1: indexed leaf -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":\"leaf\"") != NULL,
                "indexed eq criterion → plan==leaf");
    ASSERT_TRUE(strstr(resp, "\"role\":\"seed\"") != NULL,
                "seed leaf present in source");
    /* table_rows must match what we inserted — use a format string to avoid
       hard-coding the number in two places. */
    char expected_rows[64];
    snprintf(expected_rows, sizeof(expected_rows), "\"table_rows\":%d", N_ROWS);
    ASSERT_TRUE(strstr(resp, expected_rows) != NULL, "table_rows matches N_ROWS");
    free(resp); resp = NULL;

    /* -- Test 2: full scan + trigram hint (threshold=0 in TEST_BUILD) -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"title1\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":\"scan\"") != NULL,
                "unindexed contains → plan==scan");
    ASSERT_TRUE(strstr(resp, "add_trigram_index") != NULL,
                "trigram hint emitted (TEST_BUILD threshold=0)");
    free(resp); resp = NULL;

    /* -- Test 3: intersect (two indexed AND leaves) -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
        "{\"field\":\"created_at\",\"op\":\"gt\",\"value\":\"1000050\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":\"intersect\"") != NULL,
                "two indexed AND leaves → plan==intersect");
    free(resp); resp = NULL;

    /* -- Test 4: composite_index hint (indexed filter + order_by on unindexed field) -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"}],"
        "\"order_by\":\"title\",\"order\":\"asc\","
        "\"explain\":true}",
        &resp);
    /* order==sort because title has no index; composite_index hint expected */
    ASSERT_TRUE(resp && strstr(resp, "\"order\":\"sort\"") != NULL,
                "order_by unindexed field → order==sort");
    ASSERT_TRUE(strstr(resp, "composite_index") != NULL,
                "composite_index hint emitted for filter+order_by mismatch");
    free(resp); resp = NULL;

    /* -- Test 5: count with explain (plan returned, not a count integer) -- */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"lt\",\"value\":\"10\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":") != NULL,
                "count+explain returns plan object");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "count+explain has no error");
    free(resp); resp = NULL;

    /* -- Test 6: aggregate with explain (plan returned, not aggregate result) -- */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"10\"}],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":") != NULL,
                "aggregate+explain returns plan object");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "aggregate+explain has no error");
    free(resp); resp = NULL;

    /* -- Test 7: normal find (no explain flag) returns records, no plan field -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"}],"
        "\"limit\":5}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":") == NULL,
                "normal find returns no plan field");
    free(resp); resp = NULL;

    /* -- Test 8: invalid criteria field → error, not crash -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"nosuchfield\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") != NULL,
                "explain with invalid field returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-explain", test_explain_run);
```

**Invariants:**
- Tests must NOT depend on other tests' data
- Each `tc_request` must be followed by `free(resp); resp = NULL;`
- Use `ASSERT_TRUE` and `ASSERT_NOT_NULL` macros from `test_assert.h`
- All insert operations must complete before any explain queries
- Test 8 (error path) must not crash the daemon — verify `"error"` in response

---

## Task 2: Implement `cmd_explain()` in query.c

**Location in file:** Just before the anchor `int cmd_count(const char *db_root, const char *object, const char *criteria_json) {`

**Anchor text to find:** `int cmd_count(const char *db_root, const char *object, const char *criteria_json) {`

**Full function code block to insert BEFORE that anchor:**

```c
/* Hint threshold: suppress add_index/composite hints on small tables where
   a full scan is fast enough that an index adds more overhead than it saves.
   TEST_BUILD sets this to 0 so tests can verify hint logic without inserting 100k rows. */
#ifdef TEST_BUILD
#define EXPLAIN_HINT_MIN_ROWS 0
#else
#define EXPLAIN_HINT_MIN_ROWS 100000
#endif

/* cmd_explain — emit query plan (FilterPlan + hints) without executing.
   Always returns 0 (no errors — invalid criteria is reported in JSON).
   Called from server dispatch and CLI with explain=true on find/count/aggregate. */
void cmd_explain(const char *db_root, const char *object, const char *criteria_json,
                 const char *order_by, int fetching) {
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json ? criteria_json : "[]", &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return;
    }

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_criteria_tree(tree);
            return;
        }
    }
    if (tree) compile_criteria_tree(tree, fs.ts);

    /* Get table row count (O(1) metadata lookup) */
    int table_rows = get_live_count(db_root, object);

    /* Compute the FilterPlan; limit=0 means unbounded (we're not executing).
       fetching=1 for find, 0 for count/aggregate. */
    FilterPlan fp = plan_filter(tree, db_root, object, &fs, sch.splits,
                                 table_rows, order_by, fetching, 0);

    /* Map FilterPlan kind to string */
    const char *plan_str = "unknown";
    switch (fp.kind) {
        case FP_FULL_SCAN:       plan_str = "scan"; break;
        case FP_PRIMARY_LEAF:    plan_str = "leaf"; break;
        case FP_BITMAP_SMALLER:  plan_str = "bitmap"; break;
        case FP_INTERSECT:       plan_str = "intersect"; break;
        case FP_UNION:           plan_str = "union"; break;
    }

    /* Map FilterOrderKind to string */
    const char *order_str = "none";
    switch (fp.order) {
        case FP_ORDER_NONE:              order_str = "none"; break;
        case FP_ORDER_COMPOSITE:         order_str = "composite"; break;
        case FP_ORDER_COMPOSITE_EXACT:   order_str = "composite_exact"; break;
        case FP_ORDER_SORT:              order_str = "sort"; break;
        case FP_ORDER_INDEX_WALK:        order_str = "index_walk"; break;
    }

    /* Emit plan header */
    OUT("{\"plan\":\"%s\",\"order\":\"%s\",\"total_cheap\":%s,\"table_rows\":%d,"
        "\"source\":[", plan_str, order_str, fp.total_cheap ? "true" : "false", table_rows);

    /* Emit source leaves (indexed seed criteria) */
    for (int i = 0; i < fp.n_source; i++) {
        SearchCriterion *leaf = fp.source_leaves[i];
        if (!leaf) continue;

        int it = pick_index_for_leaf(db_root, object, leaf);
        const char *it_str = (it == IT_BTREE) ? "btree" : (it == IT_BITMAP) ? "bitmap" : "none";

        /* Estimate rows for this leaf via card_est_leaf.
           card_est_leaf takes a single TypedField (not TypedSchema), and
           needs a non-zero cap — use selectivity_budget(table_rows). */
        const TypedField *leaf_tf = resolve_idx_field(fs.ts, leaf->field);
        CardEst est = card_est_leaf(db_root, object, sch.splits, leaf,
                                    leaf_tf, selectivity_budget((size_t)table_rows));
        size_t est_rows = est.k;  /* CardEst.k is the estimated match count */

        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":\"%s\",\"role\":\"seed\",\"estimated_rows\":%zu}",
            leaf->field, 
            (leaf->op == OP_EQUAL) ? "eq" :
            (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" :
            (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" :
            (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" :
            (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" :
            (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            it_str, est_rows);
    }
    OUT("],\"postfilter\":[");

    /* Emit postfilter leaves. Report the actual index type on the field
       (the index exists but the planner chose not to use it as the primary
       driver), or null when the field truly has no index. */
    for (int i = 0; i < fp.n_postfilter; i++) {
        SearchCriterion *leaf = fp.postfilter_leaves[i];
        if (!leaf) continue;

        int it = pick_index_for_leaf(db_root, object, leaf);
        /* it < 0 means no usable index for this op/field combination */
        const char *pf_it_str = (it == IT_BTREE) ? "\"btree\"" :
                                (it == IT_BITMAP) ? "\"bitmap\"" :
                                (it == IT_TRIGRAM) ? "\"trigram\"" : "null";

        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":%s,\"role\":\"postfilter\",\"estimated_rows\":null}",
            leaf->field,
            (leaf->op == OP_EQUAL) ? "eq" :
            (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" :
            (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" :
            (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" :
            (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" :
            (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            pf_it_str);
    }
    OUT("],\"hints\":[");

    /* Generate hints */
    int hint_count = 0;

    /* Hint: add_index for unindexed postfilter leaves with high selectivity ops */
    if (table_rows >= EXPLAIN_HINT_MIN_ROWS) {
        for (int i = 0; i < fp.n_postfilter; i++) {
            SearchCriterion *leaf = fp.postfilter_leaves[i];
            if (!leaf) continue;

            int it = pick_index_for_leaf(db_root, object, leaf);
            if (it < 0) {  /* unindexed */
                /* Suggest btree index for range/eq ops; trigram for text ops */
                const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
                if (tf && tf->type == FT_VARCHAR &&
                    (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                     leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                     leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH)) {
                    /* Will emit trigram hint below */
                } else {
                    if (hint_count > 0) OUT(",");
                    OUT("{\"type\":\"add_index\",\"field\":\"%s\",\"reason\":\"unindexed field in postfilter; index avoids full record scan\"}", 
                        leaf->field);
                    hint_count++;
                }
            }
        }
    }

    /* Hint: add_trigram_index for unindexed varchar text-search ops (NO threshold) */
    if (fp.n_postfilter > 0 || fp.n_source > 0) {
        for (int j = 0; j < 2; j++) {  /* loop 0: source, 1: postfilter */
            int n = (j == 0) ? fp.n_source : fp.n_postfilter;
            SearchCriterion **leaves = (j == 0) ? fp.source_leaves : fp.postfilter_leaves;

            for (int i = 0; i < n; i++) {
                SearchCriterion *leaf = leaves[i];
                if (!leaf) continue;

                const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
                if (tf && tf->type == FT_VARCHAR &&
                    (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                     leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                     leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH ||
                     leaf->op == OP_NOT_LIKE || leaf->op == OP_NOT_CONTAINS ||
                     leaf->op == OP_INOT_LIKE || leaf->op == OP_INOT_CONTAINS ||
                     leaf->op == OP_ENDS_WITH || leaf->op == OP_IENDS_WITH)) {

                    int it = pick_index_for_leaf(db_root, object, leaf);
                    if (it != IT_TRIGRAM) {  /* not already a trigram index */
                        if (hint_count > 0) OUT(",");
                        OUT("{\"type\":\"add_trigram_index\",\"field\":\"%s\",\"reason\":\"varchar text-search op on %s field; trigram index enables substring matching without full record scan\"}",
                            leaf->field,
                            (it < 0) ? "unindexed" : "non-trigram-indexed");
                        hint_count++;
                    }
                }
            }
        }
    }

    /* Hint: composite_index — only when the planner chose FP_ORDER_SORT,
       meaning it has an indexed filter but no composite covering the order_by.
       Do not emit when fp.order is already COMPOSITE/COMPOSITE_EXACT/INDEX_WALK
       since the planner already found an index path for ordering. */
    if (table_rows >= EXPLAIN_HINT_MIN_ROWS && fp.order == FP_ORDER_SORT &&
        order_by && fp.n_source > 0) {
        SearchCriterion *seed = fp.source_leaves[0];
        if (seed && strcmp(seed->field, order_by) != 0 &&
            pick_index_for_leaf(db_root, object, seed) >= 0) {
            if (hint_count > 0) OUT(",");
            OUT("{\"type\":\"composite_index\",\"field\":\"%s+%s\","
                "\"reason\":\"filter on %s + order_by %s; composite index avoids in-memory sort\"}",
                seed->field, order_by, seed->field, order_by);
            hint_count++;
        }
    }

    OUT("]}\n");

    free_criteria_tree(tree);
}

```

**Invariants:**
- Must never call any function that reads segment files (no full scans, no record fetches)
- Uses only metadata and index-internal functions (get_live_count, card_est_leaf, pick_index_for_leaf)
- Must call `OUT()` to emit JSON, never `printf` or `fprintf`
- Must free the CriteriaNode tree before returning

---

## Task 3: Declare `cmd_explain` in types.h

**Location in file:** Near the other cmd declarations. Find the anchor:

**Anchor text:** `int cmd_count(const char *db_root, const char *object, const char *criteria_json);`

**Insert BEFORE that line:**

```c
void cmd_explain(const char *db_root, const char *object, const char *criteria_json,
                 const char *order_by, int fetching);
```

---

## Task 4: Intercept in server.c dispatch_json_query

**Three locations to edit:**

### Edit 4a: Count branch

**Anchor text:** `    } else if (strcmp(mode, "count") == 0) {`

**Replace the entire block from that line through:**

```
    } else if (strcmp(mode, "count") == 0) {
        char *criteria = json_obj_strdup_raw(&req, "criteria");
```

**With:**

```
    } else if (strcmp(mode, "count") == 0) {
        if (json_obj_is_true(&req, "explain")) {
            char *criteria = json_obj_strdup_raw(&req, "criteria");
            cmd_explain(db_root, object, criteria, NULL, 0);  /* fetching=0 for count */
            free(criteria);
        } else {
            char *criteria = json_obj_strdup_raw(&req, "criteria");
            cmd_count(db_root, object, criteria);
            free(criteria);
        }
```

Wait — this creates duplication. Better: refactor to keep it minimal.

**Actually, replace ONLY this portion:**

Find the anchor line:
```
    } else if (strcmp(mode, "count") == 0) {
        char *criteria = json_obj_strdup_raw(&req, "criteria");
        cmd_count(db_root, object, criteria);
        free(criteria);
```

**Replace with:**

```
    } else if (strcmp(mode, "count") == 0) {
        char *criteria = json_obj_strdup_raw(&req, "criteria");
        if (json_obj_is_true(&req, "explain")) {
            cmd_explain(db_root, object, criteria, NULL, 0);
        } else {
            cmd_count(db_root, object, criteria);
        }
        free(criteria);
```

### Edit 4b: Find branch

**Anchor text:** `    } else if (strcmp(mode, "find") == 0) {`

Following this, locate the line:
```
        if (criteria || join)
            cmd_find(db_root, object, criteria ? criteria : "[]",
```

**Find and replace this section (from `if (criteria || join)` through the corresponding `free(criteria);`):**

Old:
```
        if (criteria || join)
            cmd_find(db_root, object, criteria ? criteria : "[]",
                     off, lim, fields, excl, fmt, delim, join, ob, od, cur,
                     want_total);
        else OUT("{\"error\":\"Missing criteria\"}\n");
        free(criteria);
```

New:
```
        if (json_obj_is_true(&req, "explain")) {
            cmd_explain(db_root, object, criteria ? criteria : "[]", ob, 1);
        } else if (criteria || join) {
            cmd_find(db_root, object, criteria ? criteria : "[]",
                     off, lim, fields, excl, fmt, delim, join, ob, od, cur,
                     want_total);
        } else {
            OUT("{\"error\":\"Missing criteria\"}\n");
        }
        free(criteria);
```

### Edit 4c: Aggregate branch

**Anchor text (entire block to replace):**
```
    } else if (strcmp(mode, "aggregate") == 0) {
        char *crit = json_obj_strdup_raw(&req, "criteria");
        char *grp = json_obj_strdup_raw(&req, "group_by");
        char *aggs = json_obj_strdup_raw(&req, "aggregates");
        char *hav = json_obj_strdup_raw(&req, "having");
        char *ob = json_obj_strdup(&req, "order_by");
        char *od = json_obj_strdup(&req, "order");
        char *lim_s = json_obj_strdup(&req, "limit");
        char *fmt = json_obj_strdup(&req, "format");
        char *delim = json_obj_strdup(&req, "delimiter");
        int desc = (od && strcmp(od, "desc") == 0);
        int lim = lim_s ? atoi(lim_s) : 0;
        int want_total = json_obj_is_true(&req, "total");
        cmd_aggregate(db_root, object, crit, grp, aggs, hav, ob, desc, lim, fmt, delim, want_total);
        free(crit); free(grp); free(aggs); free(hav);
        free(ob); free(od); free(lim_s); free(fmt); free(delim);
```

**Replace with** (explain check fires before extracting params only needed by cmd_aggregate, so `desc`, `lim`, `want_total`, `grp`, `aggs`, `hav`, `fmt`, `delim` are never allocated on the explain path):

```
    } else if (strcmp(mode, "aggregate") == 0) {
        char *crit = json_obj_strdup_raw(&req, "criteria");
        char *ob   = json_obj_strdup(&req, "order_by");
        if (json_obj_is_true(&req, "explain")) {
            cmd_explain(db_root, object, crit, ob, 0);  /* fetching=0 for aggregate */
        } else {
            char *grp   = json_obj_strdup_raw(&req, "group_by");
            char *aggs  = json_obj_strdup_raw(&req, "aggregates");
            char *hav   = json_obj_strdup_raw(&req, "having");
            char *od    = json_obj_strdup(&req, "order");
            char *lim_s = json_obj_strdup(&req, "limit");
            char *fmt   = json_obj_strdup(&req, "format");
            char *delim = json_obj_strdup(&req, "delimiter");
            int desc = (od && strcmp(od, "desc") == 0);
            int lim = lim_s ? atoi(lim_s) : 0;
            int want_total = json_obj_is_true(&req, "total");
            cmd_aggregate(db_root, object, crit, grp, aggs, hav, ob, desc, lim, fmt, delim, want_total);
            free(grp); free(aggs); free(hav);
            free(od); free(lim_s); free(fmt); free(delim);
        }
        free(crit); free(ob);
```

---

## Task 5: CLI Subcommand

**Location in file:** `src/db/main.c`

**Anchor text to insert BEFORE:** `    if (strcmp(cmd, "estimate-index") == 0) {`

**Important:** CLI commands do NOT call daemon functions directly — they send JSON to the running server via `cmd_query_json(port, json)`, matching the established pattern in this file. `port` is already set earlier in `main()` via `read_server_port(db_root)`.

**Insert BEFORE that anchor line:**

```c
    /* explain find|count|aggregate <dir> <obj> <criteria> [order_by]
       Sends explain:true on the named mode to the running server.
       shard-db explain find  <dir> <obj> '[{"field":"score","op":"gt","value":"50"}]'
       shard-db explain count <dir> <obj> '[{"field":"active","op":"eq","value":"true"}]'
       shard-db explain aggregate <dir> <obj> '[...]' [order_by_field]
    */
    if (strcmp(cmd, "explain") == 0) {
        if (argc < 6) {
            fprintf(stderr, "Usage: shard-db explain find|count|aggregate <dir> <obj> <criteria> [order_by]\n");
            return 1;
        }
        const char *subcmd  = argv[2];
        const char *dir     = argv[3];
        const char *object  = argv[4];
        const char *criteria = argv[5];
        const char *order_by = (argc > 6) ? argv[6] : NULL;

        if (strcmp(subcmd, "find") != 0 && strcmp(subcmd, "count") != 0 &&
            strcmp(subcmd, "aggregate") != 0) {
            fprintf(stderr, "Unknown explain subcommand: %s (expected find, count, or aggregate)\n", subcmd);
            return 1;
        }

        char json[4096];
        if (order_by) {
            snprintf(json, sizeof(json),
                "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\","
                "\"criteria\":%s,\"order_by\":\"%s\",\"explain\":true}",
                subcmd, dir, object, criteria, order_by);
        } else {
            snprintf(json, sizeof(json),
                "{\"mode\":\"%s\",\"dir\":\"%s\",\"object\":\"%s\","
                "\"criteria\":%s,\"explain\":true}",
                subcmd, dir, object, criteria);
        }
        return cmd_query_json(port, json);
    }
```

---

## Task 6: TUI Footer Update in views.c

**Location:** `src/cli/views.c`, in the `tui_preview_json` function

**Find anchor:** `        attron(COLOR_PAIR(3));`
              `        mvprintw(rows - 3, 4,`
              `            "↑↓/jk scroll   r/⏎ run   ←/q/ESC back to edit   (line %d/%d)",`

**Replace the mvprintw line:**

Old:
```
        mvprintw(rows - 3, 4,
            "↑↓/jk scroll   r/⏎ run   ←/q/ESC back to edit   (line %d/%d)",
            top + 1, nlines);
```

New:
```
        mvprintw(rows - 3, 4,
            "↑↓/jk scroll   r/⏎ run   e=explain   ←/q/ESC back to edit   (line %d/%d)",
            top + 1, nlines);
```

**Also add case handling in the switch statement:**

**Find anchor:** `            case 'r': case '\n': case '\r': case KEY_ENTER:`
              `                return 1;`

**Replace with:**

```
            case 'r': case '\n': case '\r': case KEY_ENTER:
                return 1;
            case 'e':
                return 2;
```

---

## Task 7: TUI Main Function Handling in cli/main.c

**Three edits to add explain action handling in query_find, query_count, query_aggregate**

### Edit 7a: query_count

**Find anchor:** `        int act = tui_preview_json("count — query JSON (r=run  ←=back to edit)", req);`
              `        if (act != 1) { free(req); continue; }`

**Replace with:**

```
        int act = tui_preview_json("count — query JSON (r=run  ←=back to edit)", req);
        if (act == 2) {
            /* explain: append flag and re-query */
            size_t req_len = strlen(req);
            if (req[req_len - 1] == '}') {
                char *req_exp = malloc(req_len + 20);
                memcpy(req_exp, req, req_len - 1);
                sprintf(req_exp + req_len - 1, ",\"explain\":true}");
                
                char *resp = NULL; size_t rlen = 0;
                int rc = cli_query(c, req_exp, &resp, &rlen);
                if (rc == 0 && resp) {
                    tui_show_text("explain — count plan", resp);
                    free(resp);
                }
                free(req_exp);
            }
            free(req);
            continue;
        }
        if (act != 1) { free(req); continue; }
```

### Edit 7b: query_find

**Find anchor:** `            int act = tui_preview_json("find — query JSON (r=run  ←=back to edit)", req);`
              `            if (act != 1) { free(req); continue; }`

**Replace with:**

```
            int act = tui_preview_json("find — query JSON (r=run  ←=back to edit)", req);
            if (act == 2) {
                /* explain: append flag and re-query */
                size_t req_len = strlen(req);
                if (req[req_len - 1] == '}') {
                    char *req_exp = malloc(req_len + 20);
                    memcpy(req_exp, req, req_len - 1);
                    sprintf(req_exp + req_len - 1, ",\"explain\":true}");
                    
                    char *resp = NULL; size_t rlen = 0;
                    int rc = cli_query(c, req_exp, &resp, &rlen);
                    if (rc == 0 && resp) {
                        tui_show_text("explain — find plan", resp);
                        free(resp);
                    }
                    free(req_exp);
                }
                free(req);
                continue;
            }
            if (act != 1) { free(req); continue; }
```

### Edit 7c: query_aggregate

**Find the two instances of `tui_preview_json` in query_aggregate. There's one around line 955:**

**Anchor text:** `                    int act = tui_preview_json(`

Search for the one in query_aggregate that closes the agg builder loop. The second major call around line 955. Look for:

```
                    int act = tui_preview_json(
```

Following that anchor, find the full block pattern and replace similarly. Actually, let me be more specific:

**Find anchor:** In query_aggregate, around line 955, the line:
```
                    int act = tui_preview_json(
                        "aggregate — query JSON (r=run  ←=back to edit)", req);
                    if (act != 1) {
```

**Replace the entire `if (act != 1)` block with:**

```
                    int act = tui_preview_json(
                        "aggregate — query JSON (r=run  ←=back to edit)", req);
                    if (act == 2) {
                        /* explain: append flag and re-query */
                        size_t req_len = strlen(req);
                        if (req[req_len - 1] == '}') {
                            char *req_exp = malloc(req_len + 20);
                            memcpy(req_exp, req, req_len - 1);
                            sprintf(req_exp + req_len - 1, ",\"explain\":true}");
                            
                            char *resp = NULL; size_t rlen = 0;
                            int rc = cli_query(c, req_exp, &resp, &rlen);
                            if (rc == 0 && resp) {
                                tui_show_text("explain — aggregate plan", resp);
                                free(resp);
                            }
                            free(req_exp);
                        }
                        free(req);
                        continue;
                    }
                    if (act != 1) {
```

(Keep the rest of the block as-is.)

---

## Task 8: Documentation

Add `"explain":true` flag documentation to the three relevant query-protocol docs. No new files — edit existing ones.

**Edit `docs/query-protocol/find.md`:** In the "Per-request knobs" or parameters section, add:
> `"explain":true` — return the query plan without executing. Response is a JSON object with `plan`, `order`, `total_cheap`, `table_rows`, `source`, `postfilter`, and `hints` fields. No records are fetched. See [explain.md](explain.md).

**Edit `docs/query-protocol/count.md`:** Same one-liner, same wording.

**Edit `docs/query-protocol/aggregate.md`:** Same one-liner.

**Create `docs/query-protocol/explain.md`:** New file documenting the full explain response shape — the fields listed above, the plan/order value tables, the hint types (`add_index`, `add_trigram_index`, `composite_index`, `full_scan`), the 100k row hint threshold, and CLI usage (`shard-db explain find|count|aggregate`).

**No anchor text required for this task** — it's prose additions to existing docs files. The executor should follow the existing style in each doc.

---

## Summary Checklist

- [ ] Task 1: Write `src/test/cases/test_explain.c` with TEST_REGISTER
- [ ] Task 2: Add `cmd_explain()` function in `src/db/query.c` before `cmd_count` (includes `EXPLAIN_HINT_MIN_ROWS` macro)
- [ ] Task 3: Declare `cmd_explain` in `src/db/types.h`
- [ ] Task 4a: Add explain check in server.c count branch
- [ ] Task 4b: Add explain check in server.c find branch
- [ ] Task 4c: Add explain check in server.c aggregate branch (restructured to avoid unused vars)
- [ ] Task 5: Add CLI `explain` subcommand in `src/db/main.c`
- [ ] Task 6: Update footer text in `src/cli/views.c` and add case 'e'
- [ ] Task 7a: Add explain handling in `query_count()` in `src/cli/main.c`
- [ ] Task 7b: Add explain handling in `query_find()` in `src/cli/main.c`
- [ ] Task 7c: Add explain handling in `query_aggregate()` in `src/cli/main.c`
- [ ] Task 8: Add explain docs to find.md, count.md, aggregate.md; create explain.md
- [ ] Build: `SKIP_TESTS=1 ./build.sh`
- [ ] Test: `./build/bin/shard-db-test run-all` (all tests pass)
- [ ] Test: `./build/bin/shard-db-test run test-explain` (new test passes)
- [ ] No uncommitted files remain
