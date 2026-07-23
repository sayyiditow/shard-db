/* src/test/cases/test_find_timestamp_criteria.c
 *
 * Regression for the FT_TIMESTAMP gap in compile_one (query.c) — fixed
 * 2026-05-23 after surfacing via the HN explorer (789K comments):
 *
 *   compile_one had a switch over cc->ftype that covered every typed
 *   field (FT_LONG/INT/SHORT/NUMERIC/DOUBLE/FLOAT/BOOL/BYTE/DATE/
 *   DATETIME/TIME/UUID/ENUM/VARCHAR) but no case for FT_TIMESTAMP.
 *   match_typed's FT_TIMESTAMP arm DID exist and read the record's
 *   int64 BE timestamp correctly, but cc->i1 was never populated —
 *   it stayed at the memset-zero default. So any time-eq/gte/lt/between/in
 *   leaf effectively compared against zero:
 *     time eq  X → match was eq 0  → 0 rows
 *     time gte X → match was gte 0 → ALL rows (filter ignored)
 *     time lt  X → match was lt 0  → 0 rows
 *
 *   The count path (encode_criterion_value → btree_idx_search) sat on
 *   the OTHER encoder which DID handle FT_TIMESTAMP, so count returned
 *   the right number while find returned [] or everything. Spotted by
 *   the user as "filter ignored on `comments` for `time`".
 *
 * This test covers scalar (eq / gte / lt / between) and IN-list forms
 * plus the AND-intersect shape (int eq + timestamp lt) which is how
 * the HN explorer's per-story comments view runs. Every value chosen
 * sits well above 2^31 so a buggy build that mis-parses to int32 also
 * trips an assertion.
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

static int count_substr(const char *resp, const char *s) {
    if (!resp || !s) return 0;
    int n = 0;
    const char *p = resp;
    size_t sl = strlen(s);
    while ((p = strstr(p, s)) != NULL) { n++; p += sl; }
    return n;
}

static int test_find_timestamp_criteria_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object: `time` is FT_TIMESTAMP and indexed; `story_root` is an
       indexed int so we can exercise the int + timestamp AND-intersect
       shape exactly the way the HN explorer's per-story view does. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"events\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16\",\"time:timestamp\",\"story_root:int\"],"
        "\"indexes\":[\"time\",\"story_root\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "create-object response");
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object created");
    free(resp); resp = NULL;

    /* Five rows. Three share story_root=42 (for the intersect test).
       Every `time` value is well above 2^31 so an int32-truncation
       regression also fails. */
    struct { const char *key; long long t; int sr; } rows[] = {
        { "r1", 1000000000000LL, 42 },   /* 2001-09-09 */
        { "r2", 1254980454000LL, 42 },   /* 2009-10-08 — the exact-match probe value */
        { "r3", 1500000000000LL, 42 },   /* 2017-07-14 */
        { "r4", 1700000000000LL,  7 },
        { "r5", 1900000000000LL,  7 },
    };
    char req[512];
    for (int i = 0; i < 5; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"events\","
            "\"key\":\"%s\",\"value\":{\"id\":\"%s\",\"time\":%lld,\"story_root\":%d}}",
            rows[i].key, rows[i].key, rows[i].t, rows[i].sr);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert ok");
        free(resp); resp = NULL;
    }

    /* === Case 1: time eq <large value> → exactly one match ===
       Pre-fix returned [] (cc->i1=0 → match was eq 0). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"eq\",\"value\":\"1254980454000\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find time eq response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 1,
        "time eq returns exactly 1 record");
    ASSERT_CONTAINS(resp, "\"r2\"", "eq match returned r2");
    free(resp); resp = NULL;

    /* === Case 2: time gte <large value> → r2..r5 (4 rows) ===
       Pre-fix returned ALL records (filter ignored — match was gte 0). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"gte\",\"value\":\"1254980454000\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find time gte response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 4,
        "time gte returns r2..r5");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"r1\"") == NULL,
        "time gte excludes r1 (earlier)");
    free(resp); resp = NULL;

    /* === Case 3: time lt <large value> → only r1 ===
       Pre-fix returned [] (match was lt 0). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"lt\",\"value\":\"1254980454000\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find time lt response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 1,
        "time lt returns exactly r1");
    ASSERT_CONTAINS(resp, "\"r1\"", "lt match returned r1");
    free(resp); resp = NULL;

    /* === Case 4: time between two large values — catches the i2 parse too === */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"between\","
        "\"value\":\"1254980454000\",\"value2\":\"1700000000000\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find time between response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 3,
        "between returns r2..r4");
    free(resp); resp = NULL;

    /* === Case 5: AND-intersect: int eq + timestamp lt ===
       story_root=42 has three rows (r1/r2/r3); time lt 1500000000000
       picks r1+r2. Pre-fix the timestamp sibling silently degraded to
       lt 0, so the intersect collapsed to 0 rows. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":\"42\"},"
                      "{\"field\":\"time\",\"op\":\"lt\",\"value\":\"1500000000000\"}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find AND-intersect response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 2,
        "intersect returns r1+r2");
    ASSERT_CONTAINS(resp, "\"r1\"", "intersect includes r1");
    ASSERT_CONTAINS(resp, "\"r2\"", "intersect includes r2");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"r3\"") == NULL,
        "intersect excludes r3 (time too high)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"r4\"") == NULL,
        "intersect excludes r4 (wrong story_root)");
    free(resp); resp = NULL;

    /* === Case 6: count must agree with find length ===
       Pre-fix, count went through encode_criterion_value (config.c,
       which DID handle FT_TIMESTAMP) so count was right while find
       was wrong. This check ensures both paths converge. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"gte\",\"value\":\"1254980454000\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "count time gte response");
    ASSERT_TRUE(resp[0] == '4',
        "count returns 4 for time gte (matches the find length above)");
    free(resp); resp = NULL;

    /* === Case 7: IN list — exercises in_i64 parse on FT_TIMESTAMP. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"time\",\"op\":\"in\","
        "\"value\":[\"1000000000000\",\"1900000000000\"]}],"
        "\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "find time in[...] response");
    ASSERT_TRUE(count_substr(resp, "\"key\"") == 2,
        "in returns r1+r5");
    ASSERT_CONTAINS(resp, "\"r1\"", "in includes r1");
    ASSERT_CONTAINS(resp, "\"r5\"", "in includes r5");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-find-timestamp-criteria", test_find_timestamp_criteria_run)
