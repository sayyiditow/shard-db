/* Regression: AUD-QRY-1 — aggregate group_by over a bitmap-only group
 * field with criteria whose plan is not slot-eligible silently returned
 * zero groups (the IGB fast path fell through to a btree walk of the
 * group field, which has no btree). After the fix the query routes to
 * the per-record scan executor and matches the no-index oracle twin.
 *
 * Shapes asserted (each compares per-label values between gb_plain —
 * no indexes, oracle — and gb_bm — group field v bitmap-only + btrees
 * for the criteria/agg leaves):
 *   1. btree-seeded criteria (t between)   — the core red on base
 *   2. same criteria + count+sum specs     — the hbm+criteria shape
 *   3. slot-eligible control (v eq)        — must stay correct
 *   4. no-criteria control                 — popcount path, stays correct
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

#define OBJ_ORACLE "gb_plain"
#define OBJ_BITMAP "gb_bm"
#define NROWS 400
#define NLABELS 4

static const char *LABELS[NLABELS] = { "cat_0", "cat_1", "cat_2", "cat_3" };

/* Extract "<label>", then the next "n":<int> after it. Returns -1 when
 * the label (or its count) is absent. */
static int label_count(const char *resp, const char *label) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"v\":\"%s\"", label);
    const char *p = SAFE_STRSTR(resp, pat);
    if (!p) return -1;
    const char *q = SAFE_STRSTR(p + strlen(pat), "\"n\":");
    if (!q) return -1;
    return atoi(q + strlen("\"n\":"));
}

/* Extract "<label>", then the next "s":<int> after it (sum alias). */
static long label_sum(const char *resp, const char *label) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"v\":\"%s\"", label);
    const char *p = SAFE_STRSTR(resp, pat);
    if (!p) return -1;
    const char *q = SAFE_STRSTR(p + strlen(pat), "\"s\":");
    if (!q) return -1;
    return atol(q + strlen("\"s\":"));
}

/* Run the same aggregate group_by request on both objects and assert
 * the per-label values match the oracle. with_sum also compares the
 * "s" alias. only_label >= 0 restricts the comparison to that one
 * label (a criteria-eq response legitimately contains a single group;
 * the other labels are asserted ABSENT on both objects). */
static void assert_groups_match(TestClient *tc, const char *criteria,
                                int with_sum, int only_label,
                                const char *msg) {
    for (int i = 0; i < NLABELS; i++) {
        if (only_label >= 0 && i != only_label) {
            char qo[512], qb[512];
            char *ra = NULL, *rb = NULL;
            snprintf(qo, sizeof(qo),
                     "{\"mode\":\"aggregate\",\"dir\":\"default\","
                     "\"object\":\"" OBJ_ORACLE "\",\"group_by\":\"v\","
                     "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]"
                     "%s%s}",
                     criteria ? ",\"criteria\":" : "",
                     criteria ? criteria : "");
            snprintf(qb, sizeof(qb),
                     "{\"mode\":\"aggregate\",\"dir\":\"default\","
                     "\"object\":\"" OBJ_BITMAP "\",\"group_by\":\"v\","
                     "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]"
                     "%s%s}",
                     criteria ? ",\"criteria\":" : "",
                     criteria ? criteria : "");
            tc_request(tc, qo, &ra);
            tc_request(tc, qb, &rb);
            ASSERT_NOT_NULL(ra, msg);
            ASSERT_NOT_NULL(rb, msg);
            ASSERT_TRUE(label_count(ra, LABELS[i]) < 0, msg);
            ASSERT_TRUE(label_count(rb, LABELS[i]) < 0, msg);
            free(ra);
            free(rb);
            continue;
        }
        char qo[512], qb[512];
        char *ra = NULL, *rb = NULL;
        snprintf(qo, sizeof(qo),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"" OBJ_ORACLE "\",\"group_by\":\"v\","
                 "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}"
                 "%s]%s%s}",
                 with_sum ? ",{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}"
                          : "",
                 criteria ? ",\"criteria\":" : "",
                 criteria ? criteria : "");
        snprintf(qb, sizeof(qb),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"" OBJ_BITMAP "\",\"group_by\":\"v\","
                 "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}"
                 "%s]%s%s}",
                 with_sum ? ",{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}"
                          : "",
                 criteria ? ",\"criteria\":" : "",
                 criteria ? criteria : "");
        tc_request(tc, qo, &ra);
        tc_request(tc, qb, &rb);
        ASSERT_NOT_NULL(ra, msg);
        ASSERT_NOT_NULL(rb, msg);
        int ca = label_count(ra, LABELS[i]);
        int cb = label_count(rb, LABELS[i]);
        ASSERT_TRUE(ca >= 0, msg);   /* label present on the oracle */
        ASSERT_TRUE(cb >= 0, msg);   /* label present on the bitmap twin */
        ASSERT_EQ_INT(ca, cb, msg);
        if (with_sum) {
            long sa = label_sum(ra, LABELS[i]);
            long sb = label_sum(rb, LABELS[i]);
            ASSERT_TRUE(sa >= 0, msg);
            ASSERT_TRUE(sb >= 0, msg);
            ASSERT_EQ_INT((int)sa, (int)sb, msg);
        }
        free(ra);
        free(rb);
    }
}

static int test_igb_bitmap_group_fallback_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Oracle twin: no indexes. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"" OBJ_ORACLE "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"t:long\",\"u:int\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle object created");
    free(resp); resp = NULL;

    /* Bitmap twin: group field v bitmap-ONLY; btrees on t/u so the
     * criteria leaves exist (without them the query routes to the
     * record scan and would not be red on base). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"" OBJ_BITMAP "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v:bitmap\",\"t\",\"u\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "bitmap object created");
    free(resp); resp = NULL;

    /* 400 rows: v = cat_(i%4), t = i, u = i%13. */
    const size_t reclen = 128;
    char *recs = malloc(NROWS * reclen + 16);
    ASSERT_NOT_NULL(recs, "alloc records");
    size_t off = 0;
    off += (size_t)snprintf(recs + off, 8, "[");
    for (int i = 0; i < NROWS; i++) {
        off += (size_t)snprintf(recs + off, reclen,
            "%s{\"key\":\"k%05d\",\"value\":{\"v\":\"cat_%d\","
            "\"t\":%d,\"u\":%d}}",
            i ? "," : "", i, i % 4, i, i % 13);
    }
    off += (size_t)snprintf(recs + off, 8, "]");
    char *ins = malloc(off + 128);
    ASSERT_NOT_NULL(ins, "alloc insert");
    snprintf(ins, off + 128,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"" OBJ_ORACLE "\",\"records\":%s}", recs);
    tc_request(tc, ins, &resp);
    ASSERT_NOT_NULL(resp, "bulk insert oracle");
    ASSERT_CONTAINS(resp, "\"inserted\":400", "oracle bulk inserted");
    free(resp); resp = NULL;
    free(ins);
    ins = malloc(off + 128);
    ASSERT_NOT_NULL(ins, "alloc insert2");
    snprintf(ins, off + 128,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"" OBJ_BITMAP "\",\"records\":%s}", recs);
    tc_request(tc, ins, &resp);
    ASSERT_NOT_NULL(resp, "bulk insert bitmap");
    ASSERT_CONTAINS(resp, "\"inserted\":400", "bitmap bulk inserted");
    free(resp); resp = NULL;
    free(ins);
    free(recs);

    /* 1. Core red: btree-seeded criteria — on base the bitmap twin
     *    drops groups (empty/partial bucket set from the missing-btree
     *    walk) while the oracle twin is exact. */
    assert_groups_match(tc,
        "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149}]",
        0, -1, "AUD-QRY-1: t-between criteria groups match the oracle");

    /* 2. Sum-spec variant (hbm + criteria shape): per-label count AND
     *    sum must match. */
    assert_groups_match(tc,
        "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149}]",
        1, -1, "AUD-QRY-1: count+sum groups match the oracle");

    /* 3. Slot-eligible control (already correct on base — guards
     *    over-fixing). The v-eq response contains exactly one group
     *    (cat_1); assert that label matches and the other three are
     *    absent on BOTH objects. */
    assert_groups_match(tc,
        "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"}]",
        0, 1, "control: slot-eligible eq criteria stay correct");

    /* 4. No-criteria control (popcount path — already correct). */
    assert_groups_match(tc, NULL, 0, -1,
        "control: no-criteria popcount path stays correct");

    tc_close(tc);   /* test_env_stop does not free clients */
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-igb-bitmap-group-fallback",
              test_igb_bitmap_group_fallback_run)
