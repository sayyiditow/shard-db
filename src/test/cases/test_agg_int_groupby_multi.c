/* src/test/cases/test_agg_int_groupby_multi.c
 *
 * Multi-field integer group_by on the full-scan (PRIMARY_NONE) path —
 * exercises the raw-bytes integer hash optimization.
 *
 * Two regressions guarded:
 *   1. Zero-collapse: a zero in any group field must NOT be omitted from
 *      the raw key. (0,5) and (5,0) are distinct groups; if the encoder
 *      treats v=0 as "no bytes" the two tuples collide on the same bucket.
 *   2. Width truncation: more group bytes than the inline raw_key cap
 *      must NOT silently drop trailing fields. (1,1,1) and (1,1,2) must
 *      be distinct even when sum-of-widths approaches the cap.
 *
 * Group fields are intentionally NOT indexed so the planner picks
 * PRIMARY_NONE (agg_scan_cb per-record path).
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

static int test_agg_int_groupby_multi_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* === Case 1: zero-collapse on two int fields, no indexes === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb_ii\","
        "\"fields\":[\"a:int\",\"b:int\"],"
        "\"indexes\":[],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* 3× (0,5), 2× (5,0), 1× (5,5). Six records, three distinct groups. */
    const char *seed_ii =
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"gb_ii\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"a\":0,\"b\":5}},"
        "{\"key\":\"k2\",\"value\":{\"a\":0,\"b\":5}},"
        "{\"key\":\"k3\",\"value\":{\"a\":0,\"b\":5}},"
        "{\"key\":\"k4\",\"value\":{\"a\":5,\"b\":0}},"
        "{\"key\":\"k5\",\"value\":{\"a\":5,\"b\":0}},"
        "{\"key\":\"k6\",\"value\":{\"a\":5,\"b\":5}}"
        "]}";
    tc_request(tc, seed_ii, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_ii\","
        "\"group_by\":[\"a\",\"b\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"format\":\"csv\"}", &resp);
    /* Note: typed_field_to_buf_raw renders an int field with value 0 as
       the empty string in its CSV cell — a pre-existing display choice in
       config.c / query.c. The integer raw-hash fast path stores the full
       4-byte BE encoding regardless, so (0,5) (5,0) (5,5) remain distinct
       buckets even though the CSV cell for zero is empty. */
    ASSERT_CONTAINS(resp, "a,b,n", "case1: CSV header");
    ASSERT_CONTAINS(resp, ",5,3", "case1: bucket (a=0,b=5) count=3");
    ASSERT_CONTAINS(resp, "5,,2", "case1: bucket (a=5,b=0) count=2");
    ASSERT_CONTAINS(resp, "5,5,1", "case1: bucket (a=5,b=5) count=1");
    free(resp); resp = NULL;

    /* Also verify via JSON form (default) — three distinct buckets must
       appear regardless of zero-cell rendering. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_ii\","
        "\"group_by\":[\"a\",\"b\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    int n_buckets = 0;
    for (const char *p = resp; (p = strchr(p, '{')) != NULL; p++) n_buckets++;
    ASSERT_EQ_INT(n_buckets, 3, "case1: JSON shape has 3 buckets");
    free(resp); resp = NULL;

    /* === Case 2: truncation on three long fields (24 bytes total) === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb_lll\","
        "\"fields\":[\"x:long\",\"y:long\",\"z:long\"],"
        "\"indexes\":[],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* Tuples differ only in z. If the raw key truncates to 16 bytes
       it would drop z and collapse all three into one bucket. */
    const char *seed_lll =
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"gb_lll\",\"records\":["
        "{\"key\":\"r1\",\"value\":{\"x\":1,\"y\":1,\"z\":1}},"
        "{\"key\":\"r2\",\"value\":{\"x\":1,\"y\":1,\"z\":2}},"
        "{\"key\":\"r3\",\"value\":{\"x\":1,\"y\":1,\"z\":3}}"
        "]}";
    tc_request(tc, seed_lll, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_lll\","
        "\"group_by\":[\"x\",\"y\",\"z\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "x,y,z,n", "case2: CSV header");
    ASSERT_CONTAINS(resp, "1,1,1,1", "case2: bucket (1,1,1)=1");
    ASSERT_CONTAINS(resp, "1,1,2,1", "case2: bucket (1,1,2)=1");
    ASSERT_CONTAINS(resp, "1,1,3,1", "case2: bucket (1,1,3)=1");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-int-groupby-multi", test_agg_int_groupby_multi_run)
