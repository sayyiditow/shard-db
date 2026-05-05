/* src/test/cases/test_or_keyset_cap.c
 * Verify the type-aware OR keyset capacity estimator's correctness:
 *   - Eq/range/IN OR queries return the same counts as before.
 *   - Low-cardinality OR (eq on bool) gets a generous-enough estimate.
 *   - Overflow detection: when the estimate happens to be too low,
 *     the build returns NULL with budget_exceeded so the caller falls
 *     back to PRIMARY_NONE rather than silently missing rows.
 *
 * The overflow path is hard to trigger in a small test (we'd need a
 * field whose cardinality is much lower than our estimate assumes —
 * e.g. an int field with 2 distinct values). We exercise it with a
 * `byte` field that has only 2 values, so OP_EQUAL on it matches
 * ~50% of records, well above the live/256 estimate the planner uses.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int extract_int(const char *resp) {
    if (!resp) return -1;
    /* Bare integer response from count mode. */
    return atoi(resp);
}

static int test_or_keyset_cap_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    /* Mix of typed indexed fields:
       - age (int, ~10 distinct) — eq estimate live/50 hits
       - flag (bool, 2 distinct)  — eq estimate live/2 hits
       - byte_lc (byte, 2 distinct) — eq estimate live/256, ACTUAL 50%,
         exercises the overflow-fallback path. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"fields\":[\"age:int\",\"flag:bool\",\"byte_lc:byte\"],"
        "\"indexes\":[\"age\",\"flag\",\"byte_lc\"],\"splits\":16}",
        &resp); free(resp); resp = NULL;

    /* Seed 1000 rows. age cycles 1..10, flag toggles, byte_lc toggles. */
    size_t cap = 1000 * 80 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"or_t\",\"records\":[");
    for (int i = 1; i <= 1000; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"age\":%d,\"flag\":%s,\"byte_lc\":%d}}",
            i == 1 ? "" : ",", i,
            ((i - 1) % 10) + 1,
            (i % 2 == 0) ? "true" : "false",
            (i % 2 == 0) ? 1 : 2);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    char buf[64];

    /* OR 3 leaves on age (eq, ~10 distinct → 100 rows each).
       Actual union = 3 × 100 = 300. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"1\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"2\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"3\"}]}]}",
        &resp);
    int n_eq3 = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR 3 eq age count=%d", n_eq3);
    ASSERT_TRUE(n_eq3 == 300, buf);
    free(resp); resp = NULL;

    /* OR 5 leaves on age — actual union = 500. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"1\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"2\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"3\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"4\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"5\"}]}]}",
        &resp);
    int n_eq5 = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR 5 eq age count=%d", n_eq5);
    ASSERT_TRUE(n_eq5 == 500, buf);
    free(resp); resp = NULL;

    /* OR cross-field bool + int. age=1 → i ∈ {1,11,21,...,991} (100 odd
       indices), all of which have flag=false (i % 2 != 0). flag=true is
       all even i (500 rows). Union age=1 ∪ flag=true = 100 + 500 = 600
       (no overlap since age=1 is all-odd-indices and flag=true is even). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"1\"},"
                                "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}]}",
        &resp);
    int n_cross = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR cross-field count=%d (expect 600)", n_cross);
    ASSERT_TRUE(n_cross == 600, buf);
    free(resp); resp = NULL;

    /* OR 2 leaves on byte_lc (eq), low-card → estimate is too low →
       overflow detected → caller falls back to PRIMARY_NONE which still
       yields the correct count. byte_lc=1 (500) ∪ byte_lc=2 (500) = 1000. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"byte_lc\",\"op\":\"eq\",\"value\":\"1\"},"
                                "{\"field\":\"byte_lc\",\"op\":\"eq\",\"value\":\"2\"}]}]}",
        &resp);
    int n_byte_or = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR 2 eq byte_lc count=%d (expect 1000)", n_byte_or);
    ASSERT_TRUE(n_byte_or == 1000, buf);
    free(resp); resp = NULL;

    /* OR with IN clause — in_count=3 × per_eq estimate; should fit.
       IN values are comma-separated in the "value" field. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"in\","
                                "\"value\":\"1,2,3\"}]}]}",
        &resp);
    int n_in = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR with IN count=%d (expect 300)", n_in);
    ASSERT_TRUE(n_in == 300, buf);
    free(resp); resp = NULL;

    /* OR with range leaf (live/2 estimate). age in [1..5] OR age=10 = 500+100=600. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_t\","
        "\"criteria\":[{\"or\":[{\"field\":\"age\",\"op\":\"between\","
                                "\"value\":\"1\",\"value2\":\"5\"},"
                                "{\"field\":\"age\",\"op\":\"eq\",\"value\":\"10\"}]}]}",
        &resp);
    int n_range = extract_int(resp);
    snprintf(buf, sizeof(buf), "OR range+eq count=%d (expect 600)", n_range);
    ASSERT_TRUE(n_range == 600, buf);
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-or-keyset-cap", test_or_keyset_cap_run)
