/* src/test/cases/test_explain.c
 * Test the explain query mode -- verify plan structure and hint generation.
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

    /* Populate N_ROWS records -- one at a time is fine at this scale (<1s). */
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
                "indexed eq criterion -> plan==leaf");
    ASSERT_TRUE(strstr(resp, "\"role\":\"seed\"") != NULL,
                "seed leaf present in source");
    /* table_rows must match what we inserted -- use a format string to avoid
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
                "unindexed contains -> plan==scan");
    ASSERT_TRUE(strstr(resp, "add_trigram_index") != NULL,
                "trigram hint emitted (TEST_BUILD threshold=0)");
    free(resp); resp = NULL;

    /* -- Test 3: two indexed AND leaves -> PRIMARY_LEAF with source+postfilter -- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ex\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
        "{\"field\":\"created_at\",\"op\":\"gt\",\"value\":\"1000050\"}],"
        "\"explain\":true}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"plan\":") != NULL,
                "two indexed AND leaves returns a plan");
    ASSERT_TRUE(strstr(resp, "\"role\":\"seed\"") != NULL,
                "seed leaf present in source");
    ASSERT_TRUE(strstr(resp, "\"role\":\"postfilter\"") != NULL,
                "postfilter leaf present");
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
                "order_by unindexed field -> order==sort");
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

    /* -- Test 8: invalid criteria field -> error, not crash -- */
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
