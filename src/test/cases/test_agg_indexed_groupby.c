/* src/test/cases/test_agg_indexed_groupby.c
 * Indexed group_by fast path. When group_by field has a btree and every
 * spec is count(*) or sum/avg/min/max on an indexed non-varchar field,
 * the planner walks the group_by btree directly and hash-maps to per-bucket
 * accumulators — skipping the per-record scan. Verifies values match the
 * per-record path's output across:
 *   - count(*) only with int group_by (high cardinality)
 *   - count + sum/avg with bool group_by (low cardinality)
 *   - min/max-on-same-field dedup (single agg btree walk)
 *   - having post-filter
 *   - order_by + limit
 *   - bails to per-record path for varchar group_by (correctness preserved)
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

static int extract_field(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp || !key) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":", key);
    const char *p = SAFE_STRSTR(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    size_t i = 0;
    if (*p == '-' && i + 1 < out_sz) out[i++] = *p++;
    while (i + 1 < out_sz && (*p == '.' || (*p >= '0' && *p <= '9'))) out[i++] = *p++;
    out[i] = '\0';
    return i > 0;
}

/* Extract a per-bucket numeric field within the bucket whose group_by
   matches `gb_key`:`gb_val`. The response shape is an array of objects:
   `[{"<gb_key>":"<v1>","n":N1,...},{"<gb_key>":"<v2>","n":N2,...}]`.
   We find the bucket-object substring then extract the desired field
   inside it. */
static int extract_bucket_field(const char *resp, const char *gb_key,
                                const char *gb_val, const char *agg_key,
                                char *out, size_t out_sz) {
    if (!resp) return 0;
    char gb_needle[128];
    snprintf(gb_needle, sizeof(gb_needle), "\"%s\":\"%s\"", gb_key, gb_val);
    const char *bucket = SAFE_STRSTR(resp, gb_needle);
    if (!bucket) return 0;
    /* find the closing brace for this bucket */
    const char *end = strchr(bucket, '}');
    if (!end) return 0;
    char tmp[1024];
    size_t blen = (size_t)(end - bucket) + 1;
    if (blen >= sizeof(tmp)) blen = sizeof(tmp) - 1;
    memcpy(tmp, bucket, blen); tmp[blen] = '\0';
    return extract_field(tmp, agg_key, out, out_sz);
}

static int test_agg_indexed_groupby_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    /* age (int), active (bool), balance (numeric:14,2), region (varchar) — all
       indexed except varchar region purposely indexed too so we can check
       the varchar-bail path. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"fields\":[\"age:int\",\"active:bool\",\"balance:numeric:14,2\","
                    "\"region:varchar:8\"],"
        "\"indexes\":[\"age\",\"active\",\"balance\",\"region\"],\"splits\":16}",
        &resp); free(resp); resp = NULL;

    /* Seed 200 rows. Buckets:
       - age in {10,20,30,40} (50 rows each, ascending)
       - active = (i%2==0)        (100 true, 100 false)
       - balance = i * 10.00      (range 10..2000)
       - region = "us"|"eu"|"ap"  (cycle by 3) */
    size_t cap = 200 * 128 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"gb_t\",\"records\":[");
    for (int i = 1; i <= 200; i++) {
        int age = ((i - 1) % 4 == 0) ? 10 :
                  ((i - 1) % 4 == 1) ? 20 :
                  ((i - 1) % 4 == 2) ? 30 : 40;
        const char *act = (i % 2 == 0) ? "true" : "false";
        const char *region = (i % 3 == 0) ? "us" : (i % 3 == 1) ? "eu" : "ap";
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"age\":%d,\"active\":%s,"
            "\"balance\":\"%d.00\",\"region\":\"%s\"}}",
            i == 1 ? "" : ",", i, age, act, i * 10, region);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    char buf[128];

    /* count(*) only, group_by=age (int, indexed). 4 buckets × 50 each. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_bucket_field(resp, "age", "10", "n", buf, sizeof(buf))
                && strcmp(buf, "50") == 0, "age=10 count=50");
    ASSERT_TRUE(extract_bucket_field(resp, "age", "20", "n", buf, sizeof(buf))
                && strcmp(buf, "50") == 0, "age=20 count=50");
    ASSERT_TRUE(extract_bucket_field(resp, "age", "30", "n", buf, sizeof(buf))
                && strcmp(buf, "50") == 0, "age=30 count=50");
    ASSERT_TRUE(extract_bucket_field(resp, "age", "40", "n", buf, sizeof(buf))
                && strcmp(buf, "50") == 0, "age=40 count=50");
    free(resp); resp = NULL;

    /* count + avg(balance), group_by=active (bool, indexed). Two buckets.
       balance per bucket: i=1..200 stepping 2 → odds get false, evens get true.
       sum of evens 2,4,...,200 (100 values) × 10 = 10*(2+200)*100/2 = 101000.
       avg(true) = 101000/100 = 1010. sum of odds = 10*(1+199)*100/2 = 100000;
       avg(false) = 1000. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"active\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"avg\",\"field\":\"balance\",\"alias\":\"avg_b\"}]}",
        &resp);
    ASSERT_TRUE(extract_bucket_field(resp, "active", "true", "n", buf, sizeof(buf))
                && strcmp(buf, "100") == 0, "active=true count=100");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "false", "n", buf, sizeof(buf))
                && strcmp(buf, "100") == 0, "active=false count=100");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "true", "avg_b", buf, sizeof(buf))
                && strcmp(buf, "1010") == 0, "avg balance true=1010");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "false", "avg_b", buf, sizeof(buf))
                && strcmp(buf, "1000") == 0, "avg balance false=1000");
    free(resp); resp = NULL;

    /* min(balance) + max(balance) on same indexed field — exercises the
       agg-field dedup (single balance btree walk for both specs).
       false → odds 1*10..199*10 → min=10, max=1990
       true  → evens 2*10..200*10 → min=20, max=2000 */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"active\"],"
        "\"aggregates\":[{\"fn\":\"min\",\"field\":\"balance\",\"alias\":\"mn\"},"
                        "{\"fn\":\"max\",\"field\":\"balance\",\"alias\":\"mx\"}]}",
        &resp);
    ASSERT_TRUE(extract_bucket_field(resp, "active", "false", "mn", buf, sizeof(buf))
                && strcmp(buf, "10") == 0, "min balance false=10");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "false", "mx", buf, sizeof(buf))
                && strcmp(buf, "1990") == 0, "max balance false=1990");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "true", "mn", buf, sizeof(buf))
                && strcmp(buf, "20") == 0, "min balance true=20");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "true", "mx", buf, sizeof(buf))
                && strcmp(buf, "2000") == 0, "max balance true=2000");
    free(resp); resp = NULL;

    /* sum(balance) — verifies sum accumulator. true=101000, false=100000. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"active\"],"
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}]}",
        &resp);
    ASSERT_TRUE(extract_bucket_field(resp, "active", "true", "s", buf, sizeof(buf))
                && strcmp(buf, "101000") == 0, "sum balance true=101000");
    ASSERT_TRUE(extract_bucket_field(resp, "active", "false", "s", buf, sizeof(buf))
                && strcmp(buf, "100000") == 0, "sum balance false=100000");
    free(resp); resp = NULL;

    /* having: keep only buckets with n > 49 (all 4 age buckets pass). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"49\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"10\"", "having keeps age=10");
    ASSERT_CONTAINS(resp, "\"age\":\"40\"", "having keeps age=40");
    free(resp); resp = NULL;

    /* having: drop everything (no bucket > 50). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"50\"}]}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"10\"") == NULL, "having drops age=10");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"40\"") == NULL, "having drops age=40");
    free(resp); resp = NULL;

    /* order_by + limit: top-2 by count descending → all tied at 50, but
       order_by produces a stable subset of size 2. Just assert size by
       checking 2 buckets present. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"n\",\"order\":\"desc\",\"limit\":2}", &resp);
    int bucket_count = 0;
    const char *p = resp;
    while ((p = strstr(p, "\"age\":\"")) != NULL) { bucket_count++; p++; }
    ASSERT_TRUE(bucket_count == 2, "order_by+limit returns 2 buckets");
    free(resp); resp = NULL;

    /* group_by on varchar (region) → fast path bails, per-record runs.
       Cycle: i%3==0→us (66 rows), i%3==1→eu (67 rows), i%3==2→ap (67). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"region\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_bucket_field(resp, "region", "us", "n", buf, sizeof(buf))
                && strcmp(buf, "66") == 0, "varchar group_by region=us count=66");
    ASSERT_TRUE(extract_bucket_field(resp, "region", "eu", "n", buf, sizeof(buf))
                && strcmp(buf, "67") == 0, "varchar group_by region=eu count=67");
    ASSERT_TRUE(extract_bucket_field(resp, "region", "ap", "n", buf, sizeof(buf))
                && strcmp(buf, "67") == 0, "varchar group_by region=ap count=67");
    free(resp); resp = NULL;

    /* CSV format on indexed group_by path. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_t\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "age,n", "CSV header");
    ASSERT_CONTAINS(resp, "10,50", "CSV bucket age=10");
    ASSERT_CONTAINS(resp, "40,50", "CSV bucket age=40");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-indexed-groupby", test_agg_indexed_groupby_run)
