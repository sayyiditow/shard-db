# Plan: cursor + with_total in a single request

**Date:** 2026-06-09
**Branch:** `feat/cursor-with-total`
**Goal:** Lift the mutual-exclusion between `cursor` and `total:true` so a
paginated find can return `{"rows":[...],"cursor":...,"total":N}` in one
round-trip. The total uses `fp_compute_total` (same helper as the non-cursor
find paths) with a fetching=0 plan built from the same criteria tree.

## Execution rules

- Branch off `main`: `git checkout -b feat/cursor-with-total`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Never claim a step passed without showing the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — `src/db/query.c`: remove mutual exclusion guard

### Anchor:
```c
    /* Mutual exclusion: total and cursor conflict — cursor implies streaming
       pagination with a continuation token; total implies a fixed-page count.
       Reject early so neither path has to handle the combination. */
    if (want_total && cursor_json && cursor_json[0] &&
        strcmp(cursor_json, "null") != 0) {
        OUT("{\"error\":\"\\\"total\\\" and \\\"cursor\\\" are mutually exclusive\"}\n");
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return -1;
    }
```

### Replacement:
*(delete the block entirely — nothing replaces it)*

---

## Task 2 — `src/db/query.c`: empty-keyset early exit — emit `total:0` when requested

### Anchor:
```c
        if (cursor_prefilter_ks &&
            keyset_size(cursor_prefilter_ks) == 0) {
            OUT(dict_fmt ? "{\"rows\":{},\"cursor\":null}\n"
                         : "{\"rows\":[],\"cursor\":null}\n");
            keyset_free(cursor_prefilter_ks);
            free_joins(joins, njoins);
            free_criteria_tree(tree);
            free_excluded(&excluded);
            return 0;
        }
```

### Replacement:
```c
        if (cursor_prefilter_ks &&
            keyset_size(cursor_prefilter_ks) == 0) {
            if (want_total)
                OUT(dict_fmt ? "{\"rows\":{},\"cursor\":null,\"total\":0}\n"
                             : "{\"rows\":[],\"cursor\":null,\"total\":0}\n");
            else
                OUT(dict_fmt ? "{\"rows\":{},\"cursor\":null}\n"
                             : "{\"rows\":[],\"cursor\":null}\n");
            keyset_free(cursor_prefilter_ks);
            free_joins(joins, njoins);
            free_criteria_tree(tree);
            free_excluded(&excluded);
            return 0;
        }
```

---

## Task 3 — `src/db/query.c`: C1 fetch+sort path — add `total` to cursor response

The C1 path is the keyset-prefetch+sort shortcut inside the `if (cursor_prefilter_ks) {` block.

### Anchor:
```c
                OUT(dict_fmt ? "}" : "]");
                if (cc.printed >= limit && cc.last_value_str
                    && cc.last_key_str) {
                    OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}}",
                        order_by, cc.last_value_str, cc.last_key_str);
                } else {
                    OUT(",\"cursor\":null}");
                }
                OUT("\n");
                free(sp_rows);
                free(cc.last_value_str);
                free(cc.last_key_str);
                keyset_free(cursor_prefilter_ks);
                free_joins(joins, njoins);
                free_criteria_tree(tree);
                free_excluded(&excluded);
                return 0;
```

### Replacement:
```c
                OUT(dict_fmt ? "}" : "]");
                if (cc.printed >= limit && cc.last_value_str
                    && cc.last_key_str) {
                    OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                        order_by, cc.last_value_str, cc.last_key_str);
                } else {
                    OUT(",\"cursor\":null");
                }
                if (want_total) {
                    FilterPlan count_fp = plan_filter(tree, db_root, object,
                        &driver_fs, sch.splits, cursor_N_live,
                        NULL, 0 /*fetching*/, 0 /*limit*/);
                    int tnull = 1;
                    size_t ctotal = fp_compute_total(&count_fp, tree, db_root,
                                                     object, &sch, &driver_fs,
                                                     &cdl, &tnull);
                    if (tnull) OUT(",\"total\":null");
                    else       OUT(",\"total\":%zu", ctotal);
                }
                OUT("}\n");
                free(sp_rows);
                free(cc.last_value_str);
                free(cc.last_key_str);
                keyset_free(cursor_prefilter_ks);
                free_joins(joins, njoins);
                free_criteria_tree(tree);
                free_excluded(&excluded);
                return 0;
```

---

## Task 4 — `src/db/query.c`: btree-walk path — add `total` to cursor response

This is the fallback cursor path (btree `walk_ordered`), after the `} /* if (cursor_prefilter_ks) */` comment.

### Anchor:
```c
        OUT(dict_fmt ? "}" : "]");

        /* Emit next-page cursor if we actually hit the limit (there might be
           more). If printed < limit the walk drained to the end → null. */
        if (cc.printed >= limit && cc.last_value_str && cc.last_key_str) {
            OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}}",
                order_by, cc.last_value_str, cc.last_key_str);
        } else {
            OUT(",\"cursor\":null}");
        }
        OUT("\n");

        if (cursor_prefilter_ks) keyset_free(cursor_prefilter_ks);
        free(cc.last_value_str);
        free(cc.last_key_str);
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return 0;
```

### Replacement:
```c
        OUT(dict_fmt ? "}" : "]");

        /* Emit next-page cursor if we actually hit the limit (there might be
           more). If printed < limit the walk drained to the end → null. */
        if (cc.printed >= limit && cc.last_value_str && cc.last_key_str) {
            OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                order_by, cc.last_value_str, cc.last_key_str);
        } else {
            OUT(",\"cursor\":null");
        }
        if (want_total) {
            FilterPlan count_fp = plan_filter(tree, db_root, object,
                &driver_fs, sch.splits, cursor_N_live,
                NULL, 0 /*fetching*/, 0 /*limit*/);
            int tnull = 1;
            size_t ctotal = fp_compute_total(&count_fp, tree, db_root,
                                             object, &sch, &driver_fs,
                                             &cdl, &tnull);
            if (tnull) OUT(",\"total\":null");
            else       OUT(",\"total\":%zu", ctotal);
        }
        OUT("}\n");

        if (cursor_prefilter_ks) keyset_free(cursor_prefilter_ks);
        free(cc.last_value_str);
        free(cc.last_key_str);
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return 0;
```

---

## Task 5 — Build

```
SKIP_TESTS=1 ./build.sh
```

Expected: clean compile, no warnings.

---

## Task 6 — New test: `src/test/cases/test_cursor_with_total.c`

Create this file:

```c
/* src/test/cases/test_cursor_with_total.c
 * Verify that cursor + total:true works in a single find request,
 * returning {"rows":[...],"cursor":...,"total":N}.
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

/* Extract cursor JSON object as a string (the value of "cursor":{...} or "cursor":null).
   Returns 1 if a non-null cursor was found and copied into out. */
static int extract_cursor_json(const char *resp, char *out, size_t out_sz) {
    if (!resp) return 0;
    const char *p = strstr(resp, "\"cursor\":{");
    if (!p) return 0;
    p += strlen("\"cursor\":");
    /* find matching closing brace */
    const char *start = p;
    int depth = 0; const char *c = p;
    while (*c) {
        if (*c == '{') depth++;
        else if (*c == '}') { depth--; if (depth == 0) { c++; break; } }
        c++;
    }
    size_t n = (size_t)(c - start);
    if (n + 1 > out_sz) return 0;
    memcpy(out, start, n); out[n] = '\0';
    return 1;
}

/* Extract integer value of "total":N from response. Returns -1 if absent. */
static int extract_total(const char *resp) {
    if (!resp) return -1;
    const char *p = strstr(resp, "\"total\":");
    if (!p) return -1;
    p += strlen("\"total\":");
    if (*p == 'n') return -2; /* null */
    return atoi(p);
}

static int test_cursor_with_total_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"cwt\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with int field n, indexed. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"n\",\"type\":\"int\"}],"
        "\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;

    /* Insert 20 records, n = 1..20. */
    for (int i = 1; i <= 20; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"cwt\",\"object\":\"items\","
            "\"key\":\"k%02d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* PAGE 1: cursor:null + total:true — should return rows, cursor, total:20 */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "page1 uses rows wrapper");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "page1 emits a non-null cursor");
    ASSERT_TRUE(extract_total(resp) == 20, "page1 total == 20");
    ASSERT_CONTAINS(resp, "\"key\":\"k01\"", "page1 contains k01");
    free(resp); resp = NULL;

    /* PAGE 1 again — verify cursor field ordering: cursor comes before total. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    {
        const char *pc = strstr(resp, "\"cursor\":");
        const char *pt = strstr(resp, "\"total\":");
        ASSERT_TRUE(pc && pt && pc < pt, "cursor key appears before total key");
    }
    char cursor_json[256] = {0};
    int got_cursor = extract_cursor_json(resp, cursor_json, sizeof(cursor_json));
    ASSERT_TRUE(got_cursor, "extracted cursor from page1");
    free(resp); resp = NULL;

    /* PAGE 2: use cursor from page1 + total:true — total must still be 20. */
    if (got_cursor) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
            "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
            "\"cursor\":%s,\"total\":true}", cursor_json);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"rows\":", "page2 uses rows wrapper");
        ASSERT_TRUE(extract_total(resp) == 20, "page2 total == 20 (stable across pages)");
        ASSERT_CONTAINS(resp, "\"key\":\"k06\"", "page2 contains k06");
        free(resp); resp = NULL;
    }

    /* LAST PAGE: page 4 (records 16-20) — cursor should be null, total still 20. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"offset\":15,\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":null", "last page cursor is null");
    ASSERT_TRUE(extract_total(resp) == 20, "last page total == 20");
    free(resp); resp = NULL;

    /* CRITERIA + cursor + total: only records with n > 10 (10 records). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"10\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "criteria page1 uses rows wrapper");
    {
        int t = extract_total(resp);
        ASSERT_TRUE(t == 10, "criteria total == 10 (n > 10)");
    }
    free(resp); resp = NULL;

    /* Verify the old mutual-exclusion error is GONE. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_TRUE(strstr(resp, "mutually exclusive") == NULL,
                "no mutually exclusive error when cursor+total combined");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cursor-with-total", test_cursor_with_total_run)
```

---

## Task 7 — Run full test suite

```
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` where N ≥ previous count + assertions in the new test.

Leave all changes **uncommitted**. Report the exact test output line.
