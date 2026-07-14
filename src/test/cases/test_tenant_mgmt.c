/* src/test/cases/test_tenant_mgmt.c
 * Port of tests/test-tenant-mgmt.sh — add-dir / remove-dir + remove-token by fingerprint.
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

/* Extract the LAST `"token":"..."` value from a list-tokens response.
   Used to grab the fingerprint for the most-recently-added token. */
static char *extract_last_token(const char *resp) {
    if (!resp) return NULL;
    const char *last = NULL;
    const char *p = resp;
    while ((p = strstr(p, "\"token\":\""))) { last = p; p++; }
    if (!last) return NULL;
    last += 9;  /* past "token":" */
    const char *end = strchr(last, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - last);
    char *s = malloc(n + 1);
    if (!s) return NULL;
    memcpy(s, last, n); s[n] = '\0';
    return s;
}

static int test_tenant_mgmt_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* add-dir */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tm_alpha\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"dir_added\"", "add tm_alpha"); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"db-dirs\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tm_alpha\"", "tm_alpha appears in db-dirs"); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tm_alpha\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir_exists\"", "re-add returns dir_exists"); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tm_alpha\",\"object\":\"obj1\","
        "\"fields\":[\"k:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object under fresh tenant");
    free(resp); resp = NULL;

    /* validation */
    const char *bad[] = {"bad/name", "../escape", ".dotleader"};
    for (size_t i = 0; i < sizeof(bad)/sizeof(bad[0]); i++) {
        char req[128];
        snprintf(req, sizeof(req), "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", bad[i]);
        tc_request(tc, req, &resp);
        char desc[64];
        snprintf(desc, sizeof(desc), "reject '%s'", bad[i]);
        ASSERT_CONTAINS(resp, "invalid dir name", desc);
        free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"\"}", &resp);
    ASSERT_CONTAINS(resp, "Missing dir", "reject empty dir");
    free(resp); resp = NULL;

    /* remove-dir empty-check */
    tc_request(tc, "{\"mode\":\"remove-dir\",\"dir\":\"tm_alpha\"}", &resp);
    ASSERT_CONTAINS(resp, "not empty", "non-empty refused by default");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"tm_alpha\",\"object\":\"obj1\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-dir\",\"dir\":\"tm_alpha\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir_removed\"", "remove now-empty tenant");
    free(resp); resp = NULL;

    /* force remove non-empty */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tm_with_obj\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tm_with_obj\",\"object\":\"o\","
        "\"fields\":[\"k:varchar:8\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"remove-dir\",\"dir\":\"tm_with_obj\",\"check_empty\":\"false\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir_removed\"", "force-remove non-empty tenant");
    free(resp); resp = NULL;

    /* not-found */
    tc_request(tc, "{\"mode\":\"remove-dir\",\"dir\":\"never_existed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir_not_found\"", "remove unknown tenant");
    free(resp); resp = NULL;

    /* tenant survives across is_valid_dir reload */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tm_beta\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tm_beta\",\"object\":\"o\","
        "\"fields\":[\"k:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "is_valid_dir picks up new tenant");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"tm_beta\",\"object\":\"o\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-dir\",\"dir\":\"tm_beta\"}", &resp); free(resp); resp = NULL;

    /* remove-token by fingerprint */
    tc_request(tc, "{\"mode\":\"add-token\",\"token\":\"fp-test-aaaa00001111bbbb\",\"perm\":\"r\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"list-tokens\"}", &resp);
    char *fp = extract_last_token(resp);
    ASSERT_NOT_NULL(fp, "extract fingerprint from list-tokens");
    free(resp); resp = NULL;

    if (fp) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"remove-token\",\"fingerprint\":\"%s\"}", fp);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"token_removed\"", "remove-token by fingerprint");
        free(resp); resp = NULL;

        tc_request(tc, "{\"mode\":\"list-tokens\"}", &resp);
        ASSERT_TRUE(!resp || SAFE_STRSTR(resp, fp) == NULL,
                    "fingerprint absent after remove");
        free(resp); resp = NULL;
        free(fp);
    }

    /* full-token remove still works */
    tc_request(tc, "{\"mode\":\"add-token\",\"token\":\"fp-test2-cccc22223333dddd\",\"perm\":\"r\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-token\",\"token\":\"fp-test2-cccc22223333dddd\"}", &resp);
    ASSERT_CONTAINS(resp, "\"token_removed\"", "remove-token by full token");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"remove-token\"}", &resp);
    ASSERT_CONTAINS(resp, "Missing token or fingerprint",
                    "neither token nor fingerprint → error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-tenant-mgmt", test_tenant_mgmt_run)
