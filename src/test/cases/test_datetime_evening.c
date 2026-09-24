/* Regression coverage for full-day datetime storage and criteria parsing. */
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

static int do_count(TestClient *tc, const char *object, const char *criteria) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":%s}",
             object, criteria);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_datetime_evening_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"fields\":[\"v:datetime\"],"
        "\"splits\":8}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-index\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"field\":\"v\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"records\":["
        "{\"key\":\"early\",\"value\":{\"v\":\"20240102004744\"}},"
        "{\"key\":\"noon\",\"value\":{\"v\":\"20240102120000\"}},"
        "{\"key\":\"last_ok\",\"value\":{\"v\":\"20240102181215\"}},"
        "{\"key\":\"first_wrap\",\"value\":{\"v\":\"20240102181216\"}},"
        "{\"key\":\"eod\",\"value\":{\"v\":\"20240102235959\"}}]}",
        &resp);
    free(resp); resp = NULL;

    const char *keys[] = {"early", "noon", "last_ok", "first_wrap", "eod"};
    const char *values[] = {"20240102004744", "20240102120000",
                            "20240102181215", "20240102181216",
                            "20240102235959"};
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"get\",\"dir\":\"default\","
                 "\"object\":\"test_dt_eve\",\"key\":\"%s\"}",
                 keys[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, values[i], values[i]);
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"criteria\":[{\"field\":\"v\","
        "\"op\":\"eq\",\"value\":\"20240102181216\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"first_wrap\"", "eq 181216 returns first_wrap");
    ASSERT_TRUE(!strstr(resp ? resp : "", "\"early\""),
                "eq 181216 does not return early");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"eq\","
        "\"value\":\"20240102190000\"}]"),
        0, "eq 19:00:00 does not match early");
    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"gte\","
        "\"value\":\"20240102181216\"}]"),
        2, "late-day gte");
    ASSERT_EQ_INT(do_count(tc, "test_dt_eve",
        "[{\"field\":\"v\",\"op\":\"between\","
        "\"value\":\"20240102181216\","
        "\"value2\":\"20240102235959\"}]"),
        2, "evening between");

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\","
        "\"object\":\"test_dt_eve\",\"order_by\":\"v\","
        "\"criteria\":[{\"field\":\"v\",\"op\":\"exists\"}]}",
        &resp);
    const char *p = resp ? resp : "";
    const char *p_early = strstr(p, "20240102004744");
    const char *p_noon = strstr(p, "20240102120000");
    const char *p_last = strstr(p, "20240102181215");
    const char *p_wrap = strstr(p, "20240102181216");
    const char *p_eod = strstr(p, "20240102235959");
    ASSERT_NOT_NULL(p_early, "early in ordered find");
    ASSERT_NOT_NULL(p_noon, "noon in ordered find");
    ASSERT_NOT_NULL(p_last, "last_ok in ordered find");
    ASSERT_NOT_NULL(p_wrap, "first_wrap in ordered find");
    ASSERT_NOT_NULL(p_eod, "eod in ordered find");
    if (p_early && p_noon && p_last && p_wrap && p_eod)
        ASSERT_TRUE(p_early < p_noon && p_noon < p_last &&
                    p_last < p_wrap && p_wrap < p_eod,
                    "full ascending datetime order");
    free(resp);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-datetime-evening", test_datetime_evening_run)
