/* src/test/cases/test_ft_float_consistency.c
 *
 * Regression lock for Phase 1c.1 (commit 3580f96):
 *   - FT_FLOAT scan path range/between ops now cast query literals to float32
 *     before comparing, matching the btree path (real bug fix).
 *   - FT_FLOAT equality/IN ops now use exact float == instead of fabs()<1e-6
 *     tolerance (deliberate behaviour change to match btree).
 *
 * The object has two float fields:
 *   price  (btree-indexed) → hits btree path
 *   price2 (NOT indexed)   → hits scan path
 *
 * Every query is run against both fields and the counts must match.
 * Selected queries also compare returned keys to catch silent divergence.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------------- */

/* Run a count query against 'price' (btree-indexed). */
static int count_indexed(TestClient *tc, const char *op,
                          const char *val, const char *val2)
{
    char req[512];
    if (val2) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"price\",\"op\":\"%s\","
            "\"value\":\"%s\",\"value2\":\"%s\"}]}",
            op, val, val2);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"price\",\"op\":\"%s\","
            "\"value\":\"%s\"}]}",
            op, val);
    }
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

/* Run a count query against 'price2' (non-indexed / scan path). */
static int count_scan(TestClient *tc, const char *op,
                       const char *val, const char *val2)
{
    char req[512];
    if (val2) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"price2\",\"op\":\"%s\","
            "\"value\":\"%s\",\"value2\":\"%s\"}]}",
            op, val, val2);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"price2\",\"op\":\"%s\","
            "\"value\":\"%s\"}]}",
            op, val);
    }
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

/* Run a find query and return the raw response (caller frees). */
static char *find_field(TestClient *tc, const char *field,
                         const char *op, const char *val, const char *val2)
{
    char req[512];
    if (val2) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"%s\",\"op\":\"%s\","
            "\"value\":\"%s\",\"value2\":\"%s\"}],"
            "\"fields\":[\"price\",\"price2\"]}",
            field, op, val, val2);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ff\","
            "\"criteria\":[{\"field\":\"%s\",\"op\":\"%s\","
            "\"value\":\"%s\"}],"
            "\"fields\":[\"price\",\"price2\"]}",
            field, op, val);
    }
    char *resp = NULL;
    tc_request(tc, req, &resp);
    return resp; /* caller frees */
}

/* Count occurrences of `needle` in `haystack` (non-overlapping). */
static int count_occurrences(const char *haystack, const char *needle)
{
    if (!haystack || !needle) return 0;
    int n = 0;
    const char *p = haystack;
    size_t nl = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

/* -------------------------------------------------------------------------
 * Test
 * ------------------------------------------------------------------------- */

static int test_ft_float_consistency_run(void)
{
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Setup ---------------------------------------------------------------- */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Two float fields: price (indexed btree), price2 (NOT indexed = scan). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ff\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"price:float\",\"price2:float\"],"
        "\"indexes\":[\"price\"]}",
        &resp); free(resp); resp = NULL;

    /* Insert known boundary rows.
     *
     * Values chosen to expose the float32 round-trip issue:
     *   3.14  as double is 3.14000000000000012434... but as float32
     *         is 3.14000010490417...  So (float)3.14 == stored 3.14f is TRUE,
     *         but (double)3.14 <= stored_as_double(3.14f) is FALSE.
     *   The pre-fix scan path compared double-literal <= double(stored-float),
     *   which would EXCLUDE the boundary row on lte/gte/between.
     *
     * Use string-quoted values (the existing float test confirms this
     * is the accepted wire format for FT_FLOAT). */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ff\","
        "\"records\":["
          "{\"key\":\"r1\",\"value\":{\"price\":\"3.14\",\"price2\":\"3.14\"}},"
          "{\"key\":\"r2\",\"value\":{\"price\":\"3.15\",\"price2\":\"3.15\"}},"
          "{\"key\":\"r3\",\"value\":{\"price\":\"3.13\",\"price2\":\"3.13\"}},"
          "{\"key\":\"r4\",\"value\":{\"price\":\"3.140001\",\"price2\":\"3.140001\"}},"
          "{\"key\":\"r5\",\"value\":{\"price\":\"3.139999\",\"price2\":\"3.139999\"}},"
          "{\"key\":\"r6\",\"value\":{\"price\":\"100.0\",\"price2\":\"100.0\"}}"
        "]}",
        &resp); free(resp); resp = NULL;

    /* -----------------------------------------------------------------------
     * SECTION 1 — Range-op consistency (real bug fix: scan must cast to float)
     *
     * The boundary value is 3.14.  Pre-fix, the scan path compared:
     *   stored_float32_as_double(3.14) <= double(3.14)
     *   i.e.  3.140000104... <= 3.14000000...  → FALSE  (row excluded!)
     * Post-fix, both paths cast to float32 first → boundary row included.
     * ----------------------------------------------------------------------- */

    /* lte 3.14: rows r1(3.14), r3(3.13), r5(3.139999) qualify → 3 */
    {
        int ni = count_indexed(tc, "lte", "3.14", NULL);
        int ns = count_scan   (tc, "lte", "3.14", NULL);
        ASSERT_EQ_INT(ni, 3, "lte 3.14 (btree): 3 rows");
        ASSERT_EQ_INT(ns, 3, "lte 3.14 (scan):  3 rows — boundary must be included (Phase 1c fix)");
        ASSERT_EQ_INT(ni, ns, "lte 3.14: btree == scan");
    }

    /* gte 3.14: rows r1(3.14), r2(3.15), r4(3.140001), r6(100.0) → 4 */
    {
        int ni = count_indexed(tc, "gte", "3.14", NULL);
        int ns = count_scan   (tc, "gte", "3.14", NULL);
        ASSERT_EQ_INT(ni, 4, "gte 3.14 (btree): 4 rows");
        ASSERT_EQ_INT(ns, 4, "gte 3.14 (scan):  4 rows — boundary must be included");
        ASSERT_EQ_INT(ni, ns, "gte 3.14: btree == scan");
    }

    /* lt 3.14: rows r3(3.13), r5(3.139999) → 2 (boundary excluded) */
    {
        int ni = count_indexed(tc, "lt", "3.14", NULL);
        int ns = count_scan   (tc, "lt", "3.14", NULL);
        ASSERT_EQ_INT(ni, 2, "lt 3.14 (btree): 2 rows");
        ASSERT_EQ_INT(ns, 2, "lt 3.14 (scan):  2 rows");
        ASSERT_EQ_INT(ni, ns, "lt 3.14: btree == scan");
    }

    /* gt 3.14: rows r2(3.15), r4(3.140001), r6(100.0) → 3 (boundary excluded) */
    {
        int ni = count_indexed(tc, "gt", "3.14", NULL);
        int ns = count_scan   (tc, "gt", "3.14", NULL);
        ASSERT_EQ_INT(ni, 3, "gt 3.14 (btree): 3 rows");
        ASSERT_EQ_INT(ns, 3, "gt 3.14 (scan):  3 rows");
        ASSERT_EQ_INT(ni, ns, "gt 3.14: btree == scan");
    }

    /* between 3.13 and 3.15 (inclusive): r1(3.14), r2(3.15), r3(3.13),
     * r4(3.140001), r5(3.139999) → 5  (100.0 excluded) */
    {
        int ni = count_indexed(tc, "between", "3.13", "3.15");
        int ns = count_scan   (tc, "between", "3.13", "3.15");
        ASSERT_EQ_INT(ni, 5, "between 3.13-3.15 incl (btree): 5 rows");
        ASSERT_EQ_INT(ns, 5, "between 3.13-3.15 incl (scan):  5 rows — boundaries must be included");
        ASSERT_EQ_INT(ni, ns, "between 3.13-3.15: btree == scan");
    }

    /* -----------------------------------------------------------------------
     * SECTION 2 — Equality exactness (deliberate behavior change: no tolerance)
     *
     * Under the old 1e-6 tolerance, `eq 3.14` would have matched 3.140001
     * (|3.14f - 3.140001f| < 1e-6 in float32).  Post-fix, exact float ==
     * is used on both paths, so only the row stored as exactly 3.14f matches.
     *
     * 3.14  stored as float32 = 0x4048F5C3 → (float)3.14 matches exactly.
     * 3.140001 stored as float32 = 0x4048F5C5 → different bit pattern.
     * ----------------------------------------------------------------------- */

    /* eq 3.14: exactly one match (r1) — NOT r4(3.140001) */
    {
        int ni = count_indexed(tc, "eq", "3.14", NULL);
        int ns = count_scan   (tc, "eq", "3.14", NULL);
        ASSERT_EQ_INT(ni, 1, "eq 3.14 (btree): exactly 1 match — no tolerance");
        ASSERT_EQ_INT(ns, 1, "eq 3.14 (scan):  exactly 1 match — no tolerance (Phase 1c fix)");
        ASSERT_EQ_INT(ni, ns, "eq 3.14: btree == scan");
    }

    /* eq 100.0: exactly one match (r6) — round-trips cleanly */
    {
        int ni = count_indexed(tc, "eq", "100.0", NULL);
        int ns = count_scan   (tc, "eq", "100.0", NULL);
        ASSERT_EQ_INT(ni, 1, "eq 100.0 (btree): 1 match");
        ASSERT_EQ_INT(ns, 1, "eq 100.0 (scan):  1 match");
        ASSERT_EQ_INT(ni, ns, "eq 100.0: btree == scan");
    }

    /* in [3.14, 3.15]: exactly two matches (r1, r2) — not 3 */
    {
        int ni = count_indexed(tc, "in", "[3.14,3.15]", NULL);
        int ns = count_scan   (tc, "in", "[3.14,3.15]", NULL);
        ASSERT_EQ_INT(ni, 2, "in [3.14,3.15] (btree): 2 matches");
        ASSERT_EQ_INT(ns, 2, "in [3.14,3.15] (scan):  2 matches — no tolerance");
        ASSERT_EQ_INT(ni, ns, "in [3.14,3.15]: btree == scan");
    }

    /* neq 3.14: 5 rows (all except r1) */
    {
        int ni = count_indexed(tc, "neq", "3.14", NULL);
        int ns = count_scan   (tc, "neq", "3.14", NULL);
        ASSERT_EQ_INT(ni, 5, "neq 3.14 (btree): 5 rows");
        ASSERT_EQ_INT(ns, 5, "neq 3.14 (scan):  5 rows");
        ASSERT_EQ_INT(ni, ns, "neq 3.14: btree == scan");
    }

    /* -----------------------------------------------------------------------
     * SECTION 3 — Scan-vs-btree KEY parity via find (not just count)
     *
     * Run the same find query via the btree-indexed field (price) and the
     * scan field (price2), then verify the same rows are returned by checking
     * that the expected row keys appear in both responses.
     *
     * This catches future drift where one path silently skips a row while
     * the count happens to match by coincidence.
     * ----------------------------------------------------------------------- */

    /* (3a) lte 3.14: expect keys r1, r3, r5 in both results */
    {
        char *ri = find_field(tc, "price",  "lte", "3.14", NULL);
        char *rs = find_field(tc, "price2", "lte", "3.14", NULL);
        ASSERT_CONTAINS(ri, "\"r1\"", "lte 3.14 btree: r1 present");
        ASSERT_CONTAINS(ri, "\"r3\"", "lte 3.14 btree: r3 present");
        ASSERT_CONTAINS(ri, "\"r5\"", "lte 3.14 btree: r5 present");
        ASSERT_CONTAINS(rs, "\"r1\"", "lte 3.14 scan:  r1 present");
        ASSERT_CONTAINS(rs, "\"r3\"", "lte 3.14 scan:  r3 present");
        ASSERT_CONTAINS(rs, "\"r5\"", "lte 3.14 scan:  r5 present");
        /* Boundary row r1 is the critical one the pre-fix scan would miss. */
        /* Count occurrences of record-key pattern to confirm no extra rows. */
        ASSERT_EQ_INT(count_occurrences(ri, "\"r"), 3, "lte 3.14 btree: exactly 3 records");
        ASSERT_EQ_INT(count_occurrences(rs, "\"r"), 3, "lte 3.14 scan:  exactly 3 records");
        free(ri); free(rs);
    }

    /* (3b) eq 3.14: expect only r1 in both results */
    {
        char *ri = find_field(tc, "price",  "eq", "3.14", NULL);
        char *rs = find_field(tc, "price2", "eq", "3.14", NULL);
        ASSERT_CONTAINS(ri, "\"r1\"", "eq 3.14 btree: r1 present");
        ASSERT_CONTAINS(rs, "\"r1\"", "eq 3.14 scan:  r1 present");
        ASSERT_EQ_INT(count_occurrences(ri, "\"r"), 1, "eq 3.14 btree: exactly 1 record");
        ASSERT_EQ_INT(count_occurrences(rs, "\"r"), 1, "eq 3.14 scan:  exactly 1 record");
        free(ri); free(rs);
    }

    /* (3c) between 3.13 and 3.15: expect r1–r5 in both results (r6 excluded) */
    {
        char *ri = find_field(tc, "price",  "between", "3.13", "3.15");
        char *rs = find_field(tc, "price2", "between", "3.13", "3.15");
        ASSERT_CONTAINS(ri, "\"r1\"", "between btree: r1 present");
        ASSERT_CONTAINS(ri, "\"r2\"", "between btree: r2 present");
        ASSERT_CONTAINS(ri, "\"r3\"", "between btree: r3 present");
        ASSERT_CONTAINS(ri, "\"r4\"", "between btree: r4 present");
        ASSERT_CONTAINS(ri, "\"r5\"", "between btree: r5 present");
        ASSERT_CONTAINS(rs, "\"r1\"", "between scan:  r1 present");
        ASSERT_CONTAINS(rs, "\"r2\"", "between scan:  r2 present");
        ASSERT_CONTAINS(rs, "\"r3\"", "between scan:  r3 present");
        ASSERT_CONTAINS(rs, "\"r4\"", "between scan:  r4 present");
        ASSERT_CONTAINS(rs, "\"r5\"", "between scan:  r5 present");
        ASSERT_EQ_INT(count_occurrences(ri, "\"r"), 5, "between btree: exactly 5 records");
        ASSERT_EQ_INT(count_occurrences(rs, "\"r"), 5, "between scan:  exactly 5 records");
        free(ri); free(rs);
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-ft-float-consistency", test_ft_float_consistency_run)
