#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *filled(size_t n, char c) {
    char *s = malloc(n + 1);
    memset(s, c, n);
    s[n] = '\0';
    return s;
}

static int test_trim_compact_oob_field3_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    char *f1 = filled(65535, 'A');
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"wide\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"f1:varchar:65535\",\"f2:bool\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create wide object");
    free(resp); resp = NULL;
    size_t cap = 65535 + 256;
    char *req = malloc(cap);
    snprintf(req, cap,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"wide\","
        "\"key\":\"k1\",\"value\":{\"f1\":\"%s\"}}", f1);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed wide insert");
    free(resp); resp = NULL; free(req);
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"wide\",\"criteria\":[],\"fields\":[\"f2\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "find returns seeded key");
    /* f2 was omitted at insert → NULL (§2.2 omitted + no default). The
       trim drops its zero bytes, but the bitmap bit survives and the
       projection renders the key as JSON null (§2.3). */
    ASSERT_CONTAINS(resp, "\"f2\":null", "trimmed f2 reads back as null (omitted, no default)");
    free(resp); free(f1); tc_close(tc); test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-trim-compact-oob-field3", test_trim_compact_oob_field3_run)

static int test_trim_compact_oob_field4_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    char *f1 = filled(2500, 'C');
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"agg\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"f0:varchar:50\",\"f1:varchar:2500\",\"f2:byte\"],\"indexes\":[\"f0\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create aggregate object");
    free(resp); resp = NULL;
    size_t cap = 2500 + 256;
    char *req = malloc(cap);
    snprintf(req, cap,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"agg\","
        "\"key\":\"k1\",\"value\":{\"f0\":\"idxval\",\"f1\":\"%s\"}}", f1);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed aggregate record");
    free(resp); resp = NULL; free(req);
    cap = 2500 + 512;
    req = malloc(cap);
    snprintf(req, cap,
        "{\"mode\":\"aggregate\",\"dir\":\"d1\",\"object\":\"agg\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
        "\"group_by\":[\"f2\"],\"criteria\":{\"or\":["
        "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"idxval\"},"
        "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"nonexistent\"}]}}");
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"cnt\":1", "aggregate counts trimmed record");
    /* Task 3 of the aggregate-type-correctness plan: a truncated field is
       a MISSING group value, not a fake zero — and never OOB garbage.
       2026.10.2 null plan Task 8: a missing/absent group component is
       NULL — it forms its own tagged bucket and renders JSON null
       (distinct from "" and the literal "null"). */
    ASSERT_CONTAINS(resp, "\"f2\":null", "group key reads trimmed f2 as NULL (own group), not a fake zero or OOB garbage");
    free(resp); free(req); free(f1); tc_close(tc); test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-trim-compact-oob-field4", test_trim_compact_oob_field4_run)
