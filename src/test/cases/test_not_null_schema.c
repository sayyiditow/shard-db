/* src/test/cases/test_not_null_schema.c
 *
 * :not_null modifier — parse, structural validation, describe
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

static int test_not_null_schema_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* 1. create-object accepts trailing :not_null — incl. after a
          :default= on enum (exercises the membership-check dval rule) —
          and :nullable as an explicit no-op. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"inv\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"amount:numeric:18,2:not_null\","
                    "\"status:enum(open,paid):default=open:not_null\","
                    "\"uid:uuid\",\"free:bool:nullable\"]}", &resp);
    ASSERT_NOT_NULL(resp, "create inv response");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "create with trailing :not_null succeeds");
    free(resp); resp = NULL;

    /* 2. describe-object renders the flag exactly on constrained fields. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"inv\"}", &resp);
    ASSERT_NOT_NULL(resp, "describe inv");
    int nn_count = 0;
    for (const char *p = resp;
         p && (p = strstr(p, "\"not_null\":true")) != NULL; p++) nn_count++;
    ASSERT_EQ_INT(nn_count, 2, "amount + status render the flag");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"name\":\"free\",\"type\":\"bool\","
                             "\"not_null\"") == NULL,
                ":nullable field renders no flag");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"name\":\"uid\",\"type\":\"uuid\","
                             "\"not_null\"") == NULL,
                "unconstrained field renders no flag");
    free(resp); resp = NULL;

    /* 3. fields.conf round-trip: the wire spec lands verbatim. */
    char path[512];
    snprintf(path, sizeof(path), "%s/default/inv/fields.conf", env.db_root);
    char *fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "amount:numeric:18,2:not_null"),
                "amount line carries the token");
    ASSERT_TRUE(fconf &&
                strstr(fconf, "status:enum(open,paid):default=open:not_null"),
                "status line carries token after default");
    free(fconf);

    /* 4. The schema loads back and serves. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"inv\","
                   "\"key\":\"i1\",\"value\":{\"name\":\"a\","
                   "\"amount\":\"10.00\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert i1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"inv\","
                   "\"key\":\"i1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"open\"", "enum default landed");
    free(resp); resp = NULL;

    /* 5. Structural rejections — a token anywhere but the trailing
          position must fail on EVERY type (exact-match types already
          do; varchar/numeric tolerate trailing bytes and must be caught
          by the explicit structure rule), and duplicates must fail. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bad1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"x:int:not_null:default=5\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "int non-trailing rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bad2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"s:varchar:30:not_null:default=hello\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "varchar non-trailing rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bad3\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"d:varchar:30:not_null:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "duplicate token rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bad4\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:numeric:18,2:not_null:default=1\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "numeric non-trailing rejected");
    free(resp); resp = NULL;

    /* 6. add-field uses the same grammar, rejects malformed structure
       during pre-validation, and backfills a constrained new field. */
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"inv\","
        "\"fields\":[\"q:int:default=5:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "constrained field added");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"inv\",\"key\":\"i1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"q\":5", "new mandatory field backfilled");
    free(resp); resp = NULL;
    fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "q:int:default=5:not_null\n"),
                "add-field persists canonical constraint");
    free(fconf);
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"inv\","
        "\"fields\":[\"bad:varchar:30:not_null:default=oops\"]}", &resp);
    ASSERT_CONTAINS(resp, "Invalid field line", "non-trailing token rejected before rebuild");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"inv\","
        "\"fields\":[\"dup:numeric:18,2:not_null:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "Invalid field line", "duplicate token rejected before rebuild");
    free(resp); resp = NULL;
    fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && !strstr(fconf, "bad:") && !strstr(fconf, "dup:"),
                "invalid add-field specs leave schema unchanged");
    free(fconf);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-schema", test_not_null_schema_run)
