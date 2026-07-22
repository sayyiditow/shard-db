/* test-index-varchar-unescape — regression coverage for the JSON-unescape
 * gap in varchar record/index-key encoding and criteria comparison.
 * See docs/plans/2026-07-22-index-key-json-unescape.md for the full
 * root-cause writeup; this file covers every live write/index/criterion
 * parser family identified by that plan.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* 1a. Indexed find-equality regression (sites 1-3: index-key corruption). */
static int test_idx_varchar_find_eq(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    /* Control: the record itself is stored correctly. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"idx1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "control: get returns correctly-escaped record");
    free(resp); resp = NULL;

    /* Bug: indexed equality find misses the record. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "indexed find eq must match the record via its own index key");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1b. Single-record update record-corruption regression (site 4). */
static int test_update_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"upd1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"upd1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "update: record decodes to single-escaped form, not double-escaped");
    ASSERT_TRUE(strstr(resp, "\\\\\"hi\\\\\"") == NULL, "update: no double-escaping artifact");
    free(resp); resp = NULL;

    /* Same field is indexed — re-exercise the update-diff index path too. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "update: post-update indexed find matches on the new value");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1c. bulk-update (criteria+value shape) record-corruption regression (site 5). */
static int test_bulk_update_criteria_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\",\"tag\":\"x\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\",\"tag\":\"x\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bu1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update k1: correctly single-escaped");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bu1\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update k2: correctly single-escaped");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1c2. bulk-update (records shape) record-corruption regression (site 8),
   covering both accepted top-level formats. */
static int test_bulk_update_json_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    /* Object-format records: {"key":{...partial fields...}}. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"records\":{\"k1\":{\"category\":\"He said \\\"hi\\\"\"}}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bj1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update-json object-format: correctly single-escaped");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    /* Array-format records: [{"key":"...","value":{...}}]. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bj2\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update-json array-format: correctly single-escaped");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1d. Unindexed criteria-comparison regression (site 6) — highest-value test
   in this plan: no index involved at all, proves the bug is in criteria
   parsing, not storage. */
static int test_criteria_unindexed_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"un1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "control: record itself is stored correctly");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "unindexed find eq must match despite no index involvement");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1e. IN-list criteria regression (site 7) — the array-element boundary
   scan must not truncate on an escaped quote. */
static int test_criteria_in_list_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"in\","
        "\"value\":[\"He said \\\"hi\\\"\",\"Plain\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "IN: escaped-quote element must still match k1");
    ASSERT_CONTAINS(resp, "\"k2\"", "IN: plain element must still match k2");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1f. BETWEEN array-form regression (site 7b) — varchar bounds containing a
   literal quote must not corrupt the split. */
static int test_criteria_between_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    /* Bound value is exactly the search value, with quotes on both sides so
       a lexicographic between of ["He said \"hi\"","He said \"hi\""] must
       include it. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"between\","
        "\"value\":[\"He said \\\"hi\\\"\",\"He said \\\"hi\\\"\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "BETWEEN array-form with quoted varchar bounds must match");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1g. \u0000 must be rejected, not silently truncate downstream strlen()
   consumers (see Edge cases / invariants for why). */
static int test_varchar_embedded_nul_rejected(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"nul1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"nul1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"a\\u0000b\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "insert with \\u0000 in a varchar value is rejected, not silently truncated");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1h. A valid varchar ending in a literal backslash must be bounded,
   stored, indexed, and compared correctly (site 10 + sites 1/6). */
static int test_varchar_trailing_backslash(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"],"
        "\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    /* Wire JSON contains two backslashes before the structural quote;
       decoded varchar content ends in one literal backslash. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"ends\\\\\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL,
                "trailing-backslash insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"trail1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "ends\\\\", "get preserves the literal trailing backslash");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"ends\\\\\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "indexed criterion ending in backslash matches");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1i. Re-inserting an existing key drives build_index_key_from_json, not
   cmd_update_v2's record-based helpers. Cover its single-field, composite,
   bitmap, and trigram routes explicitly (sites 2/3). */
static int test_upsert_json_index_routes_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"region:varchar:64\","
        "\"kind:varchar:64\",\"bio:varchar:128\"],"
        "\"indexes\":[\"category\",\"category+region\",\"kind:bitmap\",\"bio:trigram\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create route-coverage object");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"routes1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"old\",\"region\":\"old\","
        "\"kind\":\"old\",\"bio\":\"old biography\"}}", &resp);
    free(resp); resp = NULL;

    /* Same insert key = upsert, entering v2_insert_pre_commit's JSON-key
       diff path for every index type. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"routes1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\","
        "\"region\":\"R\\\\D\",\"kind\":\"K\\\\Q\","
        "\"bio\":\"profile said \\\"hi\\\" today\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL, "upsert succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\","
        "\"value\":\"He said \\\"hi\\\"\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "single btree JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"R\\\\D\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "composite JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"K\\\\Q\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "bitmap JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\","
        "\"value\":\"said \\\"hi\\\"\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "trigram JSON-key route matches");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1j. The backward-compatible simple-equality parsers bypass
   parse_one_criterion and therefore need their own coverage (sites 7c/7d). */
static int test_simple_criteria_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"simple1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"simple1\","
        "\"criteria\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "simple query criterion matches decoded value");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\","
        "\"value\":{\"tag\":\"new\"},"
        "\"if\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "condition_not_met") == NULL,
                "simple CAS condition matches decoded value");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"new\"", "simple CAS update committed");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1k. Malformed varchar escapes must be rejected at the documented
   granularity: insert/update/criteria bulk-update are atomic, per-record
   bulk-update skips only its bad record, and malformed IN/NOT_IN criteria
   reject rather than broadening a NOT_IN result set. Covers Tasks 2e, 2f,
   2h, 2i, and 2j. */
static int test_varchar_malformed_escape_policies(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    /* Site 9: insert must reject malformed JSON text instead of storing
       literal backslash bytes and leaving its index/write-side callers out
       of sync. The C literal's \\q produces the invalid JSON escape \q on
       the wire. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"bad\",\"value\":{\"category\":\"bad\\q\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed varchar insert is rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 4: malformed one field rejects the complete single-record
       patch; the otherwise-valid tag field must not be changed. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"bad\\q\",\"tag\":\"changed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "single update rejects malformed varchar patch atomically");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "single update leaves malformed field unchanged");
    ASSERT_CONTAINS(resp, "\"tag\":\"old\"", "single update leaves sibling field unchanged");
    free(resp); resp = NULL;

    /* Site 5: the shared criteria bulk-update patch is validated before
       worker dispatch, so no selected record receives even its valid field. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"old\"}],"
        "\"value\":{\"category\":\"bad\\q\",\"tag\":\"changed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "criteria bulk-update rejects malformed shared patch atomically");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"old\"", "criteria bulk-update writes no partial update");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 8: records carry independent patches, so only the bad entry is
       skipped while the valid peer still commits. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bad1\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"category\":\"bad\\q\"}},"
        "{\"key\":\"k2\",\"value\":{\"category\":\"Good\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "records bulk-update sees both entries");
    ASSERT_CONTAINS(resp, "\"updated\":1", "records bulk-update applies the valid entry");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "records bulk-update skips only the malformed entry");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "bad records entry leaves k1 unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Good\"", "valid records entry updates k2");
    free(resp); resp = NULL;

    /* Site 7: malformed NOT_IN must reject the criterion, never drop the
       bad element and broaden the result set. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"not_in\",\"value\":[\"bad\\q\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed NOT_IN element rejects the whole criterion");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1l. Cursor-pagination order_by regression (site 11, round 4): parse_cursor_object
   extracts the order_by value from the client-supplied cursor without decoding
   JSON escapes; resuming from a page whose order_by value contained an
   escaped character must not corrupt the walk's seek position. */
static int extract_cursor_obj(const char *resp, char *out, size_t out_sz) {
    if (!resp) return 0;
    const char *c = SAFE_STRSTR(resp, "\"cursor\":{");
    if (!c) return 0;
    const char *start = c + strlen("\"cursor\":");
    const char *q = start;
    int depth = 0, in_str = 0;
    for (; *q; q++) {
        if (in_str) {
            if (*q == '\\') { q++; continue; }
            if (*q == '"') in_str = 0;
            continue;
        }
        if (*q == '"') { in_str = 1; continue; }
        if (*q == '{') depth++;
        else if (*q == '}') { depth--; if (depth == 0) { q++; break; } }
    }
    size_t n = (size_t)(q - start);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, start, n); out[n] = '\0';
    return 1;
}

static int test_cursor_order_by_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:16\"],\"indexes\":[\"tag\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_a\",\"value\":{\"tag\":\"A\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_b\",\"value\":{\"tag\":\"Q\\\"\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_c\",\"value\":{\"tag\":\"Q#\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_a\"", "page 1 returns k_a");
    char cursor1[256];
    ASSERT_TRUE(extract_cursor_obj(resp, cursor1, sizeof(cursor1)), "page 1 emits a cursor");
    free(resp); resp = NULL;

    char req2[512];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":%s}", cursor1);
    tc_request(tc, req2, &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_b\"", "page 2 returns k_b (the escaped-quote value)");
    char cursor2[256];
    ASSERT_TRUE(extract_cursor_obj(resp, cursor2, sizeof(cursor2)),
                "page 2 emits a cursor carrying the JSON-escaped order_by value");
    free(resp); resp = NULL;

    /* limit:2 here (not 1) is deliberate: the server only emits
       "cursor":null when the walk drains before hitting the limit
       (cc.printed < limit), not whenever a page happens to be last —
       with only k_c remaining, printed(1) < limit(2) is what actually
       produces null. limit:1 would still correctly return k_c but would
       emit a same-position next-page cursor instead of null, since
       printed(1) >= limit(1); that's an existing, intentional cursor
       contract, not something this fix changes. */
    char req3[512];
    snprintf(req3, sizeof(req3),
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":2,"
        "\"cursor\":%s}", cursor2);
    tc_request(tc, req3, &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_c\"", "page 3 resumes correctly and returns k_c, not skipped");
    ASSERT_CONTAINS(resp, "\"cursor\":null", "k_c is the last record; cursor closes");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":{\"tag\":\"bad\\q\",\"key\":\"k_a\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed cursor order_by escape is rejected");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1m. Multi-get CSV output regression (site 12, round 4): cmd_get_multi's
   CSV branch extracts field text from an already-JSON-serialized record
   string; a varchar value containing a literal quote must come back
   RFC4180-quoted with the real character, not with its JSON escape bytes
   leaked into the CSV cell. */
static int test_get_multi_csv_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"keys\":[\"k1\"],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "\"He said \"\"hi\"\"\"",
                    "multi-get CSV decodes the JSON escape and RFC4180-quotes the real character");
    ASSERT_TRUE(strstr(resp, "\\\"hi\\\"") == NULL,
                "multi-get CSV must not leak raw JSON-escape bytes into the cell");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("idx-varchar-find-eq",              test_idx_varchar_find_eq)
TEST_REGISTER("update-varchar-unescape",          test_update_varchar_unescape)
TEST_REGISTER("bulk-update-criteria-varchar-unescape", test_bulk_update_criteria_varchar_unescape)
TEST_REGISTER("bulk-update-json-varchar-unescape",     test_bulk_update_json_varchar_unescape)
TEST_REGISTER("criteria-unindexed-varchar-unescape",   test_criteria_unindexed_varchar_unescape)
TEST_REGISTER("criteria-in-list-varchar-unescape",     test_criteria_in_list_varchar_unescape)
TEST_REGISTER("criteria-between-varchar-unescape",     test_criteria_between_varchar_unescape)
TEST_REGISTER("varchar-embedded-nul-rejected",         test_varchar_embedded_nul_rejected)
TEST_REGISTER("varchar-trailing-backslash",            test_varchar_trailing_backslash)
TEST_REGISTER("upsert-json-index-routes-unescape",     test_upsert_json_index_routes_unescape)
TEST_REGISTER("simple-criteria-varchar-unescape",      test_simple_criteria_varchar_unescape)
TEST_REGISTER("varchar-malformed-escape-policies",     test_varchar_malformed_escape_policies)
TEST_REGISTER("cursor-order-by-varchar-unescape",      test_cursor_order_by_varchar_unescape)
TEST_REGISTER("get-multi-csv-varchar-unescape",        test_get_multi_csv_varchar_unescape)
