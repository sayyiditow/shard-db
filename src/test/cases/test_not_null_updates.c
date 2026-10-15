/* src/test/cases/test_not_null_updates.c
 *
 * :not_null modifier — update-side enforcement
 * single update, criteria-form bulk update, per-record JSON bulk update,
 * plus the delimited no-NULL-production controls.
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

#define UID1 "11111111-1111-1111-1111-111111111111"

static int test_not_null_updates_run(void) {
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
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u1\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\",\"amount:numeric:18,2:not_null\","
                    "\"uid:uuid:not_null\",\"req:int:not_null\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create u1");
    free(resp); resp = NULL;

    /* two clean rows, both name="u" so one criteria matches both */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"u\","
                   "\"amount\":\"1.00\",\"uid\":\"" UID1 "\",\"req\":1}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert k1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k2\",\"value\":{\"name\":\"u\","
                   "\"amount\":\"5.00\",\"uid\":\"" UID1 "\",\"req\":2}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert k2");
    free(resp); resp = NULL;

    /* 1. single update: explicit null refused, record unchanged. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\",\"value\":{\"amount\":null}}", &resp);
    ASSERT_CONTAINS(resp, "field 'amount' is not nullable",
                    "single update null refused");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":1", "k1 unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\",\"value\":{\"amount\":\"2.00\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "real-value update accepted");
    free(resp); resp = NULL;

    /* 2. criteria-form bulk update: one null patch fans out to every
          match → whole request refused, nothing staged. */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u1\","
                   "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\","
                   "\"value\":\"u\"}],\"value\":{\"amount\":null}}", &resp);
    ASSERT_CONTAINS(resp, "field 'amount' is not nullable",
                    "criteria bulk refused");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":2", "k1 untouched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":5", "k2 untouched");
    free(resp); resp = NULL;

    /* 3. per-record JSON bulk update: violating record skipped, peer
          applied (exact counts). */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u1\",\"records\":["
                   "{\"key\":\"k1\",\"value\":{\"amount\":\"3.00\"}},"
                   "{\"key\":\"k2\",\"value\":{\"amount\":null}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "one record updated");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "one record skipped");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":3", "k1 applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":5", "k2 skipped unchanged");
    free(resp); resp = NULL;

    /* 4. delimited controls — CSV has no NULL production. Cells map
          to fields.conf order after the key: name,amount,uid,req. */
    tc_request(tc, "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
                   "\"object\":\"u1\",\"delimiter\":\",\","
                   "\"data\":\"d1,ok,1.00," UID1 ",7\\n"
                   "d2,zero,," UID1 ",8\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "delimited insert accepts empty cell");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"d2\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"amount\":null") == NULL,
                "empty cell is a zero value, not NULL");
    free(resp); resp = NULL;

    /* bulk-update-delimited reads inline `data` with json_obj_strdup_raw,
       which does not decode JSON escapes (bulk-insert-delimited uses the
       unescaped variant), so a literal \n stays two bytes and multi-row
       payloads never split — deliver the rows via `file` instead. */
    char upd_csv[512];
    snprintf(upd_csv, sizeof(upd_csv), "/tmp/nn_upd_%d.csv", (int)getpid());
    FILE *uf = fopen(upd_csv, "w");
    if (!uf) { tc_close(tc); test_env_stop(&env); return 1; }
    fputs("d1,ok,," UID1 ",9\n"
          "d2,zero,4.00," UID1 ",8\n", uf);
    fclose(uf);
    char upd_req[600];
    snprintf(upd_req, sizeof(upd_req),
             "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\","
             "\"object\":\"u1\",\"delimiter\":\",\",\"file\":\"%s\"}", upd_csv);
    tc_request(tc, upd_req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "delimited update accepted");
    free(resp); resp = NULL;
    unlink(upd_csv);
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"d1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":1", "empty cell carried OLD forward");
    ASSERT_CONTAINS(resp, "\"req\":9", "written cell applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1\","
                   "\"key\":\"d2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":4", "written cell materialized");
    free(resp); resp = NULL;

    /* 5. a tombstoned field's :not_null spec is inert — every update
          path ignores a null written to a removed field. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"x:int:not_null\",\"n:int\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create u2");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u2\","
                   "\"key\":\"r1\",\"value\":{\"x\":1,\"n\":5}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert r1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\","
                   "\"object\":\"u2\",\"fields\":[\"x\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "remove x");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u2\","
                   "\"key\":\"r1\",\"value\":{\"n\":3,\"x\":null}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "single update ignores removed-field null");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u2\",\"criteria\":[{\"field\":\"n\","
                   "\"op\":\"eq\",\"value\":3}],\"value\":{\"n\":4,"
                   "\"x\":null}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "criteria bulk ignores removed-field null");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u2\",\"records\":["
                   "{\"key\":\"r1\",\"value\":{\"n\":6,\"x\":null}}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "per-record bulk ignores removed-field null");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u2\","
                   "\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":6", "all three updates applied");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"x\"") == NULL,
                "removed field not rendered");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-updates", test_not_null_updates_run)
