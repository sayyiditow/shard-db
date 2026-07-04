/* src/test/cases/test_varchar_overflow.c
 * Over-length varchar values must be rejected, not silently truncated. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int test_varchar_overflow_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vo\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vo\",\"object\":\"t\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"name:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj"); free(resp); resp = NULL;

    /* 8-byte cap; send 20 bytes → must be rejected, not truncated+stored. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"name\":\"abcdefghijklmnopqrst\"}}", &resp);
    ASSERT_CONTAINS(resp, "exceeds max", "over-length insert rejected"); free(resp); resp = NULL;

    /* The record must NOT exist (reject means no write). */
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "rejected insert left no record"); free(resp); resp = NULL;

    /* Exactly-at-cap value still succeeds. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"name\":\"abcdefgh\"}}", &resp);
    ASSERT_CONTAINS(resp, "inserted", "at-cap insert accepted"); free(resp); resp = NULL;

    /* Update path: over-length varchar must also be rejected, not truncated. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"name\":\"abcdefghijklmnopqrst\"}}", &resp);
    ASSERT_CONTAINS(resp, "exceeds max", "over-length update rejected"); free(resp); resp = NULL;

    /* k2's original at-cap value must be unchanged (reject means no write). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vo\",\"object\":\"t\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "abcdefgh", "rejected update left original value intact"); free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varchar-overflow", test_varchar_overflow_run)
