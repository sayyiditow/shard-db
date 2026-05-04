/* src/test/cases/test_request_timeout.c
 * Port of tests/test-request-timeout.sh — per-request "timeout_ms" override.
 * Covers: tight timeout trips on count/aggregate/bulk-update/bulk-delete,
 * timeout_ms:0 falls back to global, thread-local doesn't leak across reqs.
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
#include <unistd.h>

#define SEED_RECORDS 1500000

static int test_request_timeout_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    /* High timeout — bulk-insert of 1.5M records may take a few seconds. */
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:32\"]}",
        &resp); free(resp); resp = NULL;

    /* Write 1.5M records to a tempfile and bulk-insert via "file" — payload
       at this size (~165 MB) exceeds MAX_REQUEST_SIZE on the inline path. */
    char seed_path[256];
    snprintf(seed_path, sizeof(seed_path), "/tmp/rt_big_%d.json", (int)getpid());
    FILE *sf = fopen(seed_path, "w");
    if (!sf) { ASSERT_TRUE(0, "open seed file"); tc_close(tc); test_env_stop(&env); return 1; }
    fputc('[', sf);
    for (int i = 1; i <= SEED_RECORDS; i++) {
        if (i > 1) fputc(',', sf);
        fprintf(sf,
            "{\"key\":\"k%d\",\"value\":{\"status\":\"paid\",\"amount\":%d,"
            "\"note\":\"note_for_record_%d_padding_x\"}}",
            i, i % 1000, i);
    }
    fputc(']', sf);
    fclose(sf);

    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"file\":\"%s\"}", seed_path);
    int rc = tc_request(tc, req, &resp);
    unlink(seed_path);
    ASSERT_EQ_INT(rc, 0, "bulk-insert seed succeeds");
    free(resp); resp = NULL;

    /* Tight timeout trips on count. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"record\"}],"
        "\"timeout_ms\":10}", &resp);
    ASSERT_CONTAINS(resp, "\"query_timeout\"", "count tight timeout trips");
    free(resp); resp = NULL;

    /* aggregate with high-cardinality group_by. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"note\"],\"timeout_ms\":10}", &resp);
    ASSERT_CONTAINS(resp, "\"query_timeout\"", "aggregate tight timeout trips");
    free(resp); resp = NULL;

    /* bulk-update dry_run. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"record\"}],"
        "\"value\":{\"status\":\"updated\"},\"dry_run\":true,\"timeout_ms\":10}",
        &resp);
    ASSERT_CONTAINS(resp, "\"query_timeout\"", "bulk-update tight timeout trips");
    free(resp); resp = NULL;

    /* bulk-delete dry_run. */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"record\"}],"
        "\"dry_run\":true,\"timeout_ms\":10}", &resp);
    ASSERT_CONTAINS(resp, "\"query_timeout\"", "bulk-delete tight timeout trips");
    free(resp); resp = NULL;

    /* timeout_ms:0 → falls back to global (completes normally). amount=42 is
       a hit on every record where i%1000==42 → 1500 records. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"42\"}],"
        "\"timeout_ms\":0}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "query_timeout") == NULL,
                "timeout_ms:0 does not trip");
    ASSERT_CONTAINS(resp, "1500", "timeout_ms:0 returns result");
    free(resp); resp = NULL;

    /* timeout_ms absent → same as 0. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"42\"}]}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "query_timeout") == NULL,
                "absent timeout_ms does not trip");
    ASSERT_CONTAINS(resp, "1500", "absent timeout_ms returns result");
    free(resp); resp = NULL;

    /* Thread-local does not leak. Trip then issue clean request. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"record\"}],"
        "\"timeout_ms\":10}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rt_big\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"1\"}]}",
        &resp);
    ASSERT_TRUE(resp && strstr(resp, "query_timeout") == NULL,
                "follow-up count does not inherit timeout");
    ASSERT_CONTAINS(resp, "1500", "follow-up returns result");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-request-timeout", test_request_timeout_run)
