/* test-datetimems — exercises the FT_DATETIMEMS field type. Storage is
 * 8 bytes: BE int32 yyyyMMdd date + BE uint32 ms-of-day (0..86399999).
 * Wire format is the 17-digit string "yyyyMMddHHmmssfff". Distinct from
 * FT_DATETIME (second precision, 6 bytes) and FT_TIMESTAMP (epoch ms,
 * no calendar semantics).
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
#include <time.h>

static void dtms_now_parts(struct tm *out_tm, int *out_msec) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t now = ts.tv_sec;
    localtime_r(&now, out_tm);
    *out_msec = (int)(ts.tv_nsec / 1000000L);
}

static long long dtms_to_ordinal(int y, int mo, int d, int hh, int mm, int ss, int fff) {
    return ((long long)y * 10000LL + mo * 100LL + d) * 100000000LL
         + ((long long)hh * 3600 + mm * 60 + ss) * 1000LL + fff;
}

static int test_datetimems_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"dtms\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"created_at:datetimems:auto_create\","
                    "\"updated_at:datetimems:auto_update\","
                    "\"event_time:datetimems\"],"
        "\"indexes\":[\"event_time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with datetimems fields succeeded");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"key\":\"e1\",\"value\":{\"event_time\":\"20260703161530123\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with auto_create datetimems");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"dtms\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"event_time\":\"20260703161530123\"", "event_time round-tripped");
    ASSERT_CONTAINS(resp, "\"created_at\":\"", "auto_create populated created_at");
    ASSERT_CONTAINS(resp, "\"updated_at\":\"", "auto_update populated updated_at");
    free(resp); resp = NULL;

    {
        struct tm tm_before; int msec_before;
        dtms_now_parts(&tm_before, &msec_before);
        long long before = dtms_to_ordinal(tm_before.tm_year + 1900, tm_before.tm_mon + 1,
            tm_before.tm_mday, tm_before.tm_hour, tm_before.tm_min, tm_before.tm_sec, msec_before);

        struct timespec sl = { 0, 10 * 1000000L }; nanosleep(&sl, NULL);

        tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"dtms\",\"object\":\"events\","
            "\"key\":\"e1\",\"value\":{\"event_time\":\"20260703170000456\"}}", &resp);
        free(resp); resp = NULL;

        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"dtms\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
        ASSERT_CONTAINS(resp, "\"event_time\":\"20260703170000456\"", "event_time updated");

        const char *uap = strstr(resp, "\"updated_at\":\"");
        ASSERT_NOT_NULL(uap, "updated_at present after update");
        if (uap) {
            const char *digits = uap + strlen("\"updated_at\":\"");
            int y, mo, d, hh, mm, ss, fff;
            sscanf(digits, "%4d%2d%2d%2d%2d%2d%3d", &y, &mo, &d, &hh, &mm, &ss, &fff);
            long long after = dtms_to_ordinal(y, mo, d, hh, mm, ss, fff);
            ASSERT_TRUE(after > before, "updated_at advanced past the pre-update ordinal");
        }
        free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"gte\",\"value\":\"20260101000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed gte query returned 1");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"lt\",\"value\":\"20200101000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "0", "indexed lt query excludes out-of-range datetimems");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"key\":\"e2\",\"value\":{\"event_time\":\"20260703170000455\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"dtms\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"gt\",\"value\":\"20260703170000455\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "ms-precision gt distinguishes 456 from 455");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"dtms\",\"object\":\"events\"}", &resp);
    ASSERT_CONTAINS(resp, "\"datetimems\"", "describe-object reports datetimems type");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-datetimems", test_datetimems_run)
