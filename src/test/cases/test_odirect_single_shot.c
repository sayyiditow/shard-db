/* src/test/cases/test_odirect_single_shot.c
 * Correctness test for the O_DIRECT single-shot fast path:
 * full scans on non-indexed fields must return the right results
 * regardless of whether the shard fits in one buffer.
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

static int count_json_key(const char *resp, const char *key) {
    if (!resp) return 0;
    int n = 0; const char *p = resp;
    while ((p = strstr(p, key))) { n++; p += strlen(key); }
    return n;
}

static int test_odirect_single_shot_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"oss\"}", &resp);
    free(resp); resp = NULL;

    /* Object with non-indexed int field. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"oss\",\"object\":\"items\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"tag:varchar:8\"]}",
        &resp);
    free(resp); resp = NULL;

    /* Insert 200 records: score 1..200, tag "even" or "odd". */
    for (int i = 1; i <= 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"oss\",\"object\":\"items\","
            "\"key\":\"k%03d\",\"value\":{\"score\":%d,\"tag\":\"%s\"}}",
            i, i, (i % 2 == 0) ? "even" : "odd");
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Full scan: count all records. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\"}",
        &resp);
    ASSERT_CONTAINS(resp, "200", "count == 200");
    free(resp); resp = NULL;

    /* Non-indexed filter: find all even scores (100 records). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"even\"}],"
        "\"limit\":200}",
        &resp);
    ASSERT_NOT_NULL(resp, "find even response");
    {
        int n = count_json_key(resp, "\"key\":\"k");
        ASSERT_TRUE(n == 100, "find tag=even returns 100 records");
    }
    free(resp); resp = NULL;

    /* Non-indexed range: score > 150 (50 records). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"150\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "50", "count score>150 == 50");
    free(resp); resp = NULL;

    /* Verify no records are duplicated: find all, check total. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"oss\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"1\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "200", "full criteria scan == 200");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-odirect-single-shot", test_odirect_single_shot_run)
