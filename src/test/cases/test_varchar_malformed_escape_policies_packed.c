/* Packed copy of the varchar-malformed-escape-policies case from
 * test_index_varchar_unescape.c (class K original stays legacy until T3).
 * Sites 9 (insert), 4 (single update), 8 (per-record bulk update) and 7
 * (malformed NOT_IN) are copied. Site 5 (criteria bulk-update atomicity) is
 * dropped: packed refuses criteria updates (AUD-PACKED-BULKUPDATE).
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

static int test_varchar_malformed_escape_policies_packed_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"vue\",\"object\":\"bad1p\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    /* Site 9: insert rejects malformed JSON text. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1p\","
        "\"key\":\"bad\",\"value\":{\"category\":\"bad\\q\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed varchar insert is rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1p\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 4: malformed one field rejects the complete single-record patch. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"bad1p\",\"key\":\"k1\","
        "\"value\":{\"category\":\"bad\\q\",\"tag\":\"changed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "single update rejects malformed varchar patch atomically");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1p\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "single update leaves malformed field unchanged");
    ASSERT_CONTAINS(resp, "\"tag\":\"old\"", "single update leaves sibling field unchanged");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1p\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 8 is a packed gap: a malformed per-record value rejects the whole
       packed request instead of skipping only that record. Pin the packed
       refusal and prove neither record changed. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bad1p\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"category\":\"bad\\q\"}},"
        "{\"key\":\"k2\",\"value\":{\"category\":\"Good\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "invalid packed bulk input",
                    "packed rejects a malformed per-record patch as a whole");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1p\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "bad records entry leaves k1 unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1p\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "packed refusal leaves the valid peer unchanged");
    free(resp); resp = NULL;

    /* Site 7: malformed NOT_IN must reject the criterion. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"bad1p\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"not_in\",\"value\":[\"bad\\q\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed NOT_IN element rejects the whole criterion");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("varchar-malformed-escape-policies-packed",
              test_varchar_malformed_escape_policies_packed_run)
