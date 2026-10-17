/* src/test/cases/test_cas_guards.c
 *
 * Conditional-writes F2 — if_match / if_no_match object-scope guards.
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

static int test_cas_guards_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gr\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"domain:varchar:40\",\"owner:varchar:20\"]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create gr");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kid\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"p:varchar:20\"]}",
        &resp);
    free(resp); resp = NULL;

    /* 1. if_no_match uniqueness: first insert wins, second same-domain
          insert fails with matched:1; count stays 1. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"domain\":\"d.io\",\"owner\":\"a\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "first guarded insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g2\",\"value\":{\"domain\":\"d.io\",\"owner\":\"b\"},"
        "\"if_no_match\":{\"or\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "guard blocks");
    ASSERT_CONTAINS(resp, "\"guard\":\"if_no_match\"", "guard named");
    ASSERT_CONTAINS(resp, "\"matched\":1", "one other matched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"gr\"}",
               &resp);
    ASSERT_CONTAINS(resp, "1", "exactly one record");
    free(resp); resp = NULL;

    /* 2. Non-indexed guard field works (owner carries no index). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g3\",\"value\":{\"domain\":\"e.io\",\"owner\":\"a\"},"
        "\"if_no_match\":[{\"field\":\"owner\",\"op\":\"eq\",\"value\":\"a\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":1", "scan-based guard blocks");
    free(resp); resp = NULL;

    /* 3. if_match sibling-exists on the kid object (same-object guard;
          rev-2 finding 10 — the guard queries `p`, which kid has). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-x\",\"value\":{\"p\":\"p1\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-y\",\"value\":{\"p\":\"p1\"},\"if_match\":"
        "[{\"field\":\"p\",\"op\":\"eq\",\"value\":\"p1\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "if_match passes");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-z\",\"value\":{\"p\":\"p1\"},\"if_match\":"
        "[{\"field\":\"p\",\"op\":\"eq\",\"value\":\"nope\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"guard\":\"if_match\"", "if_match named");
    ASSERT_CONTAINS(resp, "\"matched\":0", "nothing matched");
    free(resp); resp = NULL;

    /* 4. Self-exclusion on update: g1 is the only d.io record; updating
          it under a d.io no_match guard passes (itself excluded). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"owner\":\"a2\"},\"if_no_match\":"
        "[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "self excluded");
    free(resp); resp = NULL;
    /* ...and moving e.io's domain onto d.io fails (g1 is another match). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g3\",\"value\":{\"domain\":\"d.io\"},\"if_no_match\":"
        "[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"",
                    "collision rejected");
    free(resp); resp = NULL;

    /* 5. Guard + key-scoped if compose: guard passes, key-if fails. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"owner\":\"a3\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"zz.io\"}],"
        "\"if\":[{\"field\":\"owner\",\"op\":\"eq\",\"value\":\"ghost\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "key-if gates");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"guard\"") == NULL,
                "failure is the key-scoped one");
    free(resp); resp = NULL;

    /* 6. Malformed and empty guards reject with no write. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g9\",\"value\":{\"domain\":\"z.io\"},"
        "\"if_no_match\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "empty guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g9\",\"value\":{\"domain\":\"z.io\"},"
        "\"if_no_match\":[{\"field\":\"nosuch\",\"op\":\"eq\",\"value\":1}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "unknown-field guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"g9\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "g9 was never written");
    free(resp); resp = NULL;

    /* 7. Explicit null guards are absent; two non-null guards reject. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g10\",\"value\":{\"domain\":\"n.io\"},"
        "\"if_match\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "null guard = absent");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g11\",\"value\":{\"domain\":\"m.io\"},"
        "\"if_match\":null,\"if_no_match\":[{\"field\":\"domain\",\"op\":"
        "\"eq\",\"value\":\"m.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"",
                    "null guard absent; surviving no-match guard runs");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g12\",\"value\":{\"domain\":\"dual.io\"},"
        "\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"m.io\"}],"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"dual.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "two non-null guards rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"g12\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "g12 was never written");
    free(resp); resp = NULL;

    /* 8. Guards on non-write modes are ignored. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"gr\","
        "\"criteria\":[],\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "find ignores guards");
    free(resp); resp = NULL;

    /* 9. Auto-key guarded insert (server-generated key, no self to
          exclude). */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"aku\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"domain:varchar:40\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aku\","
        "\"value\":{\"domain\":\"u.io\"},\"if_no_match\":[{\"field\":\"domain\","
        "\"op\":\"eq\",\"value\":\"u.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key guard insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aku\","
        "\"value\":{\"domain\":\"u.io\"},\"if_no_match\":[{\"field\":\"domain\","
        "\"op\":\"eq\",\"value\":\"u.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "second blocked");
    free(resp); resp = NULL;

    /* 10. D8 boundary — guards reject on bulk modes. */
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"gr\",\"records\":[{\"key\":\"gb\","
        "\"value\":{\"domain\":\"b.io\"}}],"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"b.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "not supported on bulk", "bulk guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"gb\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "gb was never written");
    free(resp); resp = NULL;

    /* 11. Indexed equality and AND post-filter paths keep the guard. */
    tc_request(tc, "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"gr\","
        "\"field\":\"domain\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "index domain");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"idx-duplicate\",\"value\":{\"domain\":\"d.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "condition_not_met", "indexed guard rejects duplicate");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-guards", test_cas_guards_run)
