/* Packed copy of test_not_null_updates.c (class K original stays legacy
 * until T3).
 * Kept: single-update NULL refusal, per-record JSON bulk update, delimited
 * update carry-forward, removed-field null on single/per-record updates.
 * Dropped: the two criteria-form bulk-update blocks (packed refuses criteria
 * updates, AUD-PACKED-BULKUPDATE) and the delimited-insert empty-cell block
 * (packed drops a row whose CSV cell is empty where legacy zero-filled;
 * recorded packed gap). The rows that the delimited-update block needs are
 * seeded with plain inserts instead.
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

static int test_not_null_updates_packed_run(void) {
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
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"u1p\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\",\"amount:numeric:18,2:not_null\","
                    "\"uid:uuid:not_null\",\"req:int:not_null\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create u1p");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"u\","
                   "\"amount\":\"1.00\",\"uid\":\"" UID1 "\",\"req\":1}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert k1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k2\",\"value\":{\"name\":\"u\","
                   "\"amount\":\"5.00\",\"uid\":\"" UID1 "\",\"req\":2}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert k2");
    free(resp); resp = NULL;

    /* 1. single update: explicit null refused, record unchanged. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\",\"value\":{\"amount\":null}}", &resp);
    ASSERT_CONTAINS(resp, "field 'amount' is not nullable",
                    "single update null refused");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":1", "k1 unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\",\"value\":{\"amount\":\"2.00\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "real-value update accepted");
    free(resp); resp = NULL;

    /* 2. (criteria-form bulk update dropped: packed refusal pinned by
       test-packed-bulk-update-criteria-refused.) State is unchanged. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":2", "k1 state after single update");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":5", "k2 state after single update");
    free(resp); resp = NULL;

    /* 3. per-record JSON bulk update: violating record skipped, peer
          applied (exact counts). */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u1p\",\"records\":["
                   "{\"key\":\"k1\",\"value\":{\"amount\":\"3.00\"}},"
                   "{\"key\":\"k2\",\"value\":{\"amount\":null}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "one record updated");
    ASSERT_CONTAINS(resp, "invalid packed update",
                    "NULL patch is reported as a packed error entry");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"errors\":[") != NULL,
                "NULL patch is an error entry (packed reports errors, not skipped)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":3", "k1 applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":5", "k2 skipped unchanged");
    free(resp); resp = NULL;

    /* 4. delimited update controls. The original seeds these rows with a
       delimited insert whose empty cell legacy zero-filled; packed drops
       that row, so seed with plain inserts and keep the update contract. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"d1\",\"value\":{\"name\":\"ok\",\"amount\":\"1.00\","
                   "\"uid\":\"" UID1 "\",\"req\":7}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"d2\",\"value\":{\"name\":\"zero\",\"amount\":\"2.00\","
                   "\"uid\":\"" UID1 "\",\"req\":8}}", &resp); free(resp); resp = NULL;

    char upd_csv[512];
    snprintf(upd_csv, sizeof(upd_csv), "/tmp/nn_upd_p_%d.csv", (int)getpid());
    FILE *uf = fopen(upd_csv, "w");
    if (!uf) { tc_close(tc); test_env_stop(&env); return 1; }
    fputs("d1,ok,," UID1 ",9\n"
          "d2,zero,4.00," UID1 ",8\n", uf);
    fclose(uf);
    char upd_req[600];
    snprintf(upd_req, sizeof(upd_req),
             "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\","
             "\"object\":\"u1p\",\"delimiter\":\",\",\"file\":\"%s\"}", upd_csv);
    tc_request(tc, upd_req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "delimited update accepted");
    free(resp); resp = NULL;
    unlink(upd_csv);
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"d1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":1", "empty cell carried OLD forward");
    ASSERT_CONTAINS(resp, "\"req\":9", "written cell applied");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u1p\","
                   "\"key\":\"d2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":4", "written cell materialized");
    free(resp); resp = NULL;

    /* 5. a tombstoned field's :not_null spec is inert — single update rejects the
          removed name as unknown; per-record bulk-update still ignores it. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"u2p\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"x:int:not_null\",\"n:int\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create u2p");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u2p\","
                   "\"key\":\"r1\",\"value\":{\"x\":1,\"n\":5}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "insert r1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\","
                   "\"object\":\"u2p\",\"fields\":[\"x\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "remove x");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u2p\","
                   "\"key\":\"r1\",\"value\":{\"n\":3,\"x\":null}}", &resp);
    ASSERT_CONTAINS(resp, "unknown field 'x'",
                "single update rejects removed-field null (AUD-P3-7)");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"u2p\","
                   "\"key\":\"r1\",\"value\":{\"n\":3}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "single update without the removed field succeeds");
    free(resp); resp = NULL;
    /* criteria-form bulk update dropped (packed refusal). */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                   "\"object\":\"u2p\",\"records\":["
                   "{\"key\":\"r1\",\"value\":{\"n\":6,\"x\":null}}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "per-record bulk ignores removed-field null");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u2p\","
                   "\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":6", "both updates applied");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"x\"") == NULL,
                "removed field not rendered");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-updates-packed", test_not_null_updates_packed_run)
