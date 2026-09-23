/* Regression coverage for indexed IN/NOT_IN on calendar fields. */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>

static int count_keys(const char *resp) {
    int n = 0;
    for (const char *p = resp; p && (p = strstr(p, "\"key\"")); p += 5) n++;
    return n;
}

static void assert_temporal(TestClient *tc, const char *field,
                             const char *first, const char *second) {
    char req[2048];
    char *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"aggregate\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"aggregates\":["
        "{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":"
        "[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
        field, first);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"n\":1", "temporal eq matches one row");
    free(resp);

    snprintf(req, sizeof(req),
        "{\"mode\":\"aggregate\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"aggregates\":["
        "{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":"
        "[{\"field\":\"%s\",\"op\":\"in\",\"value\":[\"%s\",\"%s\"]}]}",
        field, first, second);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"n\":2", "temporal IN matches both rows");
    free(resp);

    snprintf(req, sizeof(req),
        "{\"mode\":\"aggregate\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"aggregates\":["
        "{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":"
        "[{\"field\":\"%s\",\"op\":\"not_in\",\"value\":[\"%s\"]}]}",
        field, first);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"n\":1", "temporal NOT_IN matches one row");
    free(resp);

    snprintf(req, sizeof(req),
        "{\"mode\":\"find\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"criteria\":"
        "[{\"field\":\"%s\",\"op\":\"in\",\"value\":[\"missing\"]}],"
        "\"limit\":10}", field);
    tc_request(tc, req, &resp);
    ASSERT_EQ_INT(count_keys(resp), 0, "temporal IN missing value returns no rows");
    free(resp);
}

static int test_temporal_in_criteria_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"dt:datetime\",\"dms:datetimems\","
        "\"du:datetime\",\"dmu:datetimems\"],"
        "\"indexes\":[\"dt\",\"dms\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "temporal object created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"temporal_in\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"dt\":\"20240101120000\","
        "\"dms\":\"20240101120000123\",\"du\":\"20240101120000\","
        "\"dmu\":\"20240101120000123\"}},"
        "{\"key\":\"b\",\"value\":{\"dt\":\"20240102120000\","
        "\"dms\":\"20240102120000456\",\"du\":\"20240102120000\","
        "\"dmu\":\"20240102120000456\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "temporal records inserted");
    free(resp); resp = NULL;

    assert_temporal(tc, "dt", "20240101120000", "20240102120000");
    assert_temporal(tc, "dms", "20240101120000123", "20240102120000456");
    assert_temporal(tc, "du", "20240101120000", "20240102120000");
    assert_temporal(tc, "dmu", "20240101120000123", "20240102120000456");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-temporal-in-criteria", test_temporal_in_criteria_run)
