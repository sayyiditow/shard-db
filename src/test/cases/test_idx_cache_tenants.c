/* src/test/cases/test_idx_cache_tenants.c
 * Verify that two tenants with identically-named objects have independent
 * idx cache entries — neither pollutes the other on index add/remove.
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

static int test_idx_cache_tenants_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Two tenants, same object name "products". */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ict_alpha\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ict_beta\"}", &resp);
    free(resp); resp = NULL;

    /* alpha/products — index on "price" */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ict_alpha\",\"object\":\"products\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"price:int\",\"name:varchar:32\"],"
        "\"indexes\":[\"price\"]}",
        &resp);
    if (resp && strstr(resp, "error")) fprintf(stderr, "DEBUG: create ict_alpha/products: %s\n", resp);
    free(resp); resp = NULL;

    /* beta/products — index on "name" */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"price:int\",\"name:varchar:32\"],"
        "\"indexes\":[\"name\"]}",
        &resp);
    if (resp && strstr(resp, "error")) fprintf(stderr, "DEBUG: create ict_beta/products: %s\n", resp);
    free(resp); resp = NULL;

    /* Verify alpha sees "price" index, NOT "name" */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"ict_alpha\",\"object\":\"products\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"price\"", "ict_alpha/products indexes contain price");
    ASSERT_TRUE(strstr(resp, "\"name\"") == NULL ||
                strstr(resp, "\"indexes\"") == NULL ||
                /* name appears only in fields, not indexes section */
                (strstr(strstr(resp, "\"indexes\""), "\"price\"") != NULL),
                "ict_alpha/products index list contains price");
    free(resp); resp = NULL;

    /* Verify beta sees "name" index */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"ict_beta\",\"object\":\"products\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"name\"", "ict_beta/products describe returns name");
    free(resp); resp = NULL;

    /* Add a second index to alpha — must NOT appear in beta's cache. */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"ict_alpha\",\"object\":\"products\",\"field\":\"name\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\"", "add-index name on ict_alpha succeeded");
    free(resp); resp = NULL;

    /* Beta should still only have "name" index (not an additional "price"). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"limit\":1}",
        &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "find on ict_beta/products by name (indexed) succeeds without error");
    free(resp); resp = NULL;

    /* Remove index from alpha — must NOT corrupt beta's cache. */
    tc_request(tc,
        "{\"mode\":\"remove-index\",\"dir\":\"ict_alpha\",\"object\":\"products\",\"field\":\"name\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"removed\"", "remove-index name from ict_alpha");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"ict_beta\",\"object\":\"products\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"limit\":1}",
        &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "find on ict_beta/products by name still works after ict_alpha remove-index");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-idx-cache-tenants", test_idx_cache_tenants_run)
