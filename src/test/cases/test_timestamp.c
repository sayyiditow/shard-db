/* test-timestamp — exercises the FT_TIMESTAMP field type added in
 * 2026.05.6. Storage is int64 BE (8 bytes), value semantics are Unix
 * epoch milliseconds, encoding matches FT_LONG; the type carries
 * the additional behaviour of producing clock_gettime(CLOCK_REALTIME)
 * in ms for :auto_create / :auto_update default kinds.
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

static long long ts_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static int test_timestamp_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ts\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ts\",\"object\":\"events\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"created_at:timestamp:auto_create\","
                    "\"updated_at:timestamp:auto_update\","
                    "\"event_time:timestamp\"],"
        "\"indexes\":[\"event_time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object with timestamp fields succeeded");
    free(resp); resp = NULL;

    /* Insert with explicit event_time only; created_at + updated_at
       should be auto-populated to "now". */
    long long t0 = ts_now_ms();
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ts\",\"object\":\"events\","
        "\"key\":\"e1\",\"value\":{\"event_time\":1745568000000}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "insert with auto_create timestamp");
    free(resp); resp = NULL;
    long long t1 = ts_now_ms();

    /* Read back — event_time should match exactly, created_at + updated_at
       should be in [t0..t1]. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ts\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"event_time\":1745568000000", "event_time round-tripped");
    ASSERT_CONTAINS(resp, "\"created_at\":", "auto_create populated created_at");
    ASSERT_CONTAINS(resp, "\"updated_at\":", "auto_update populated updated_at");

    /* Sanity-check the auto-generated values fall in [t0..t1+1000]. */
    {
        const char *cap = strstr(resp, "\"created_at\":");
        ASSERT_NOT_NULL(cap, "created_at substring present");
        if (cap) {
            long long got = atoll(cap + strlen("\"created_at\":"));
            ASSERT_TRUE(got >= t0 && got <= t1 + 1000,
                "created_at is approximately now (epoch ms)");
        }
    }
    free(resp); resp = NULL;

    /* Update — updated_at should advance; created_at should NOT. */
    long long t2 = ts_now_ms();
    /* small sleep to make t3 > t2 reliably */
    struct timespec sl = { 0, 5 * 1000000L }; nanosleep(&sl, NULL);
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"ts\",\"object\":\"events\","
        "\"key\":\"e1\",\"value\":{\"event_time\":1745654400000}}", &resp);
    free(resp); resp = NULL;
    long long t3 = ts_now_ms();

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ts\",\"object\":\"events\",\"key\":\"e1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"event_time\":1745654400000", "event_time updated");
    {
        const char *uap = strstr(resp, "\"updated_at\":");
        ASSERT_NOT_NULL(uap, "updated_at present after update");
        if (uap) {
            long long got = atoll(uap + strlen("\"updated_at\":"));
            ASSERT_TRUE(got >= t2 && got <= t3 + 1000,
                "updated_at advanced to approximately now");
        }
    }
    free(resp); resp = NULL;

    /* Indexed range query on event_time — should hit the btree path. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ts\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"gte\",\"value\":\"1745000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "indexed gte query returned 1");
    free(resp); resp = NULL;

    /* Range that excludes our record. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"ts\",\"object\":\"events\","
        "\"criteria\":[{\"field\":\"event_time\",\"op\":\"lt\",\"value\":\"1000000000000\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "0", "indexed lt query excludes future timestamps");
    free(resp); resp = NULL;

    /* describe-object should report the field type as "timestamp". */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"ts\",\"object\":\"events\"}", &resp);
    ASSERT_CONTAINS(resp, "\"timestamp\"", "describe-object reports timestamp type");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-timestamp", test_timestamp_run)
