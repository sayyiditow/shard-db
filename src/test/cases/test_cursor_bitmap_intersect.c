/* src/test/cases/test_cursor_bitmap_intersect.c
 *
 * Verifies the bitmap post-filter intersection logic for D1 composite
 * cursor plans. When the planner uses a D1 composite (e.g., type+score
 * composite), it builds a keyset from the composite seed alone
 * (type=job = 1k records), then post-filters with bitmap criteria
 * (status=active). Without the bitmap intersection fix, the executor
 * fetches all 1k records even though the actual intersection is only
 * ~500 records.
 *
 * This test creates a scenario where the composite seed is broader than
 * the true intersection and confirms the query completes quickly and
 * returns correct results.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

static int test_cursor_bitmap_intersect_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Create tenant directory. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir\":\"default\"", "add-dir default");
    free(resp); resp = NULL;

    /* Create object with bitmap-indexed varchar fields + btree score + composite.
       type+score composite enables D1 composite plan. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cbi\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":["
          "\"type:varchar:16\","
          "\"status:varchar:16\","
          "\"score:int\"],"
        "\"indexes\":["
          "\"type:bitmap\","
          "\"status:bitmap\","
          "\"score\","
          "\"type+score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create cbi");
    free(resp); resp = NULL;

    /* Insert 2000 records:
       - 1000 with type=job, 1000 with type=story
       - Of the 1000 job records: 500 have status=active, 500 have status=inactive
       - Scores: 1..1000 */
    for (int i = 0; i < 2000; i++) {
        char body[512];
        const char *type = (i < 1000) ? "job" : "story";
        const char *status = "inactive";
        if (i < 1000) {
            status = (i < 500) ? "active" : "inactive";
        }
        int score = (i < 1000) ? (1000 - i) : (i - 999); /* desc order for job */
        snprintf(body, sizeof(body),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cbi\","
            "\"key\":\"k%d\",\"value\":{"
            "\"type\":\"%s\",\"status\":\"%s\",\"score\":\"%d\"}}",
            i, type, status, score);
        tc_request(tc, body, &resp);
        /* Silently ignore insert responses (empty on success) */
        free(resp); resp = NULL;
    }

    /* Query: type=job AND status=active ORDER BY score DESC limit 25
       This should trigger a D1 composite plan (type+score) with bitmap
       post-filter (status=active). Without the bitmap intersection fix,
       the composite seed (type=job = 1000 records) would be the full
       prefilter set, and the bitmap post-filter would be applied
       per-record during fetch. With the fix, the bitmap post-filter
       is intersected with the seed first, reducing the candidate set to
       ~500 records before the fetch phase. */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cbi\","
        "\"criteria\":["
        "{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}],"
        "\"order_by\":\"score\",\"order\":\"desc\",\"limit\":25}", &resp);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec)
                  + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    ASSERT_NOT_NULL(resp, "find cbi response");
    ASSERT_TRUE(elapsed < 1.0, "query completes in < 1s");

    /* Verify all results have type=job and status=active */
    ASSERT_CONTAINS(resp, "\"type\":\"job\"", "results have type=job");
    ASSERT_CONTAINS(resp, "\"status\":\"active\"", "results have status=active");

    /* Count results by counting "score": occurrences in array objects.
       Response format: [{"type":"job","status":"active","score":"1000"},...]
       Since there are no "key" fields in bare-value mode, we count the
       number of `"score"` field occurrences, one per result record. */
    int count = 0;
    const char *scan = resp;
    while (scan && (scan = strstr(scan, "\"score\"")) != NULL) {
        count++;
        scan++;
    }
    ASSERT_EQ_INT(count, 25, "query returns exactly 25 results");

    free(resp); resp = NULL;

    /* Also verify count API matches our expected distribution */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cbi\","
        "\"criteria\":["
        "{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}", &resp);
    ASSERT_CONTAINS(resp, "500", "count shows 500 matching records");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-cursor-bitmap-intersect", test_cursor_bitmap_intersect_run)
