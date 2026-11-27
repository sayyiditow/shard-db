/* Regression: AUD-QRY-8 — plan_filter Phase B serves criteria whose eq
 * leaves are fully covered by a composite index ("a+b" for [a eq, b eq])
 * as FP_PRIMARY_LEAF + FP_ORDER_COMPOSITE_EXACT, with source leaves in
 * composite order and NO standalone index required. Only `find` can
 * serve that plan (find_via_composite_key). count's primary-leaf branch
 * dispatched source_leaves[0]->field with no standalone index ->
 * btree_dispatch succeeds with ZERO entries -> prints 0; every
 * aggregate plan site drove the same phantom leaf -> 0/[]/wrong counts.
 * The fix: count's primary-leaf branch excludes the composite-exact
 * plan; every aggregate plan_filter site normalizes it to FP_FULL_SCAN
 * (the record-scan executor re-checks the full tree). find is untouched.
 *
 * Case map (numbers = docs/plans/2026-11-19-audit-qry8-*.md Task 1):
 *   1 count [a=1,b=0] ce_comp        RED on base (0 vs 50)
 *   2 aggregate count(*) ce_comp     RED on base (0)
 *   3 aggregate count(*) ce_mix      RED on base (100: b ignored)
 *   4 group_by=c ce_comp             RED on base
 *   5 TopN group_by=c ce_comp        RED on base
 *   6 find control (D1 path)         GREEN on base
 *   7 explain composite_exact        GREEN on base (fix must not
 *                                    disturb find's plan)
 *   8 bulk-delete ce_del == 50       GREEN on base (safe-by-accident;
 *                                    CBF_NONE fallback + full re-check)
 *   9 single-leaf control [b eq 1]   GREEN on base
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

#define NRECS 300
#define CRIT_AB \
    "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}," \
    "{\"field\":\"b\",\"op\":\"eq\",\"value\":0}]"

typedef struct {
    int label;
    long count;
} LabelCount;

/* Extract every ("c":<int>,"n":<int>) pair from a group_by=c response
 * and sort by (label asc) for order-independent vector equality. */
static int extract_pairs(const char *resp, LabelCount *out, int cap) {
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"c\":")) != NULL && n < cap) {
        int lbl = atoi(p + 4);
        const char *q = SAFE_STRSTR(p, "\"n\":");
        if (q) {
            out[n].label = lbl;
            out[n].count = atol(q + 4);
            n++;
        }
        p += 4;
    }
    for (int i = 1; i < n; i++) {
        LabelCount lc = out[i];
        int j = i - 1;
        while (j >= 0 && out[j].label > lc.label) {
            out[j + 1] = out[j];
            j--;
        }
        out[j + 1] = lc;
    }
    return n;
}

static void group_counts(TestClient *tc, const char *obj,
                         const char *criteria, LabelCount *out, int *nout) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"c\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
             "\"criteria\":%s}", obj, criteria);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "group_by response");
    *nout = resp ? extract_pairs(resp, out, 256) : 0;
    free(resp);
}

static void assert_group_vectors_equal(TestClient *tc, const char *obj,
                                       const char *criteria,
                                       const char *msg) {
    LabelCount va[256], vb[256];
    int na, nb;
    group_counts(tc, "ce_plain", criteria, va, &na);
    group_counts(tc, obj, criteria, vb, &nb);
    ASSERT_EQ_INT(na, nb, msg);
    if (na != nb) return;
    for (int i = 0; i < na; i++) {
        ASSERT_EQ_INT(va[i].label, vb[i].label, msg);
        ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count, msg);
    }
}

/* TopN request: group_by=c, order_by the count alias, desc, limit —
 * emitted (label,count) sequence compared exactly (post-fix both twins
 * run the identical scan executor, so tie resolution matches). */
static void assert_topn_equal(TestClient *tc, const char *obj,
                              const char *criteria, int limit,
                              const char *msg) {
    char req[1024];
    char *ra = NULL, *rb = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"c\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
             "\"criteria\":%s,\"order_by\":\"n\",\"order\":\"desc\","
             "\"limit\":%d}", "ce_plain", criteria, limit);
    tc_request(tc, req, &ra);
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"c\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
             "\"criteria\":%s,\"order_by\":\"n\",\"order\":\"desc\","
             "\"limit\":%d}", obj, criteria, limit);
    tc_request(tc, req, &rb);
    ASSERT_NOT_NULL(ra, msg);
    ASSERT_NOT_NULL(rb, msg);
    /* the emitted sequence must be IDENTICAL (labels + counts + order) */
    ASSERT_TRUE(ra && rb && strcmp(ra, rb) == 0, msg);
    free(ra);
    free(rb);
}

static long scalar_count(TestClient *tc, const char *obj,
                         const char *criteria) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"%s%s}",
             obj, criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "");
    tc_request(tc, req, &resp);
    long v = resp ? atol(resp) : -1;
    free(resp);
    return v;
}

static long agg_count(TestClient *tc, const char *obj,
                      const char *criteria) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
             "\"criteria\":%s}", obj, criteria);
    tc_request(tc, req, &resp);
    long v = -1;
    if (resp) {
        const char *q = SAFE_STRSTR(resp, "\"n\":");
        if (q) v = atol(q + 4);
    }
    free(resp);
    return v;
}

static void insert_fixture(TestClient *tc, const char *obj) {
    const size_t reclen = 80;
    char *body = malloc((size_t)NRECS * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < NRECS; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s{\"key\":\"k%04d\",\"value\":{\"a\":%d,\"b\":%d,\"c\":%d}}",
            i ? "," : "", i, i % 3, i % 2, (i * i) % 97);
    }
    off += (size_t)snprintf(body + off, 8, "]");
    char *req = malloc(off + 160);
    ASSERT_NOT_NULL(req, "alloc request");
    snprintf(req, off + 160,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"%s\",\"records\":%s}", obj, body);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "bulk insert response");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "bulk insert accepted");
    free(resp);
    free(req);
    free(body);
}

static int test_composite_exact_count_agg_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Oracle: no indexes. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ce_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle created");
    free(resp); resp = NULL;

    /* Composite-only twins plus a standalone btree on the GROUP field c
     * (round-1 M2: without it the IGB/TopN fast paths decline and
     * assertions 4-5 never reach the normalized sites). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ce_comp\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"b\",\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ce_comp created");
    free(resp); resp = NULL;

    /* Composite + standalone a/b (aggregate-wrong-only class). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ce_mix\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"a\",\"b\",\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ce_mix created");
    free(resp); resp = NULL;

    /* Bulk-delete twin. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ce_del\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"b\",\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ce_del created");
    free(resp); resp = NULL;

    insert_fixture(tc, "ce_plain");
    insert_fixture(tc, "ce_comp");
    insert_fixture(tc, "ce_mix");
    insert_fixture(tc, "ce_del");

    /* 1: count [a eq 1, b eq 0] on ce_comp == oracle (50). RED on base
     * (the phantom-leaf dispatch collects nothing and prints 0). */
    {
        long a = scalar_count(tc, "ce_plain", CRIT_AB);
        long b = scalar_count(tc, "ce_comp", CRIT_AB);
        ASSERT_EQ_INT((int)a, 50, "fixture: [a=1,b=0] matches 50 rows");
        ASSERT_EQ_INT((int)a, (int)b, "case1: count composite-exact");
    }

    /* 2: aggregate count(*) on ce_comp == oracle. RED on base (0). */
    {
        long a = agg_count(tc, "ce_plain", CRIT_AB);
        long b = agg_count(tc, "ce_comp", CRIT_AB);
        ASSERT_EQ_INT((int)a, 50, "fixture: agg count oracle = 50");
        ASSERT_EQ_INT((int)a, (int)b, "case2: aggregate count composite-exact");
    }

    /* 3: same on ce_mix — with standalone a/b indexes count() is
     * correct today but the AGGREGATE was wrong (|{a=1}| = 100, b
     * ignored). RED on base. */
    {
        long a = agg_count(tc, "ce_plain", CRIT_AB);
        long b = agg_count(tc, "ce_mix", CRIT_AB);
        ASSERT_EQ_INT((int)a, 50, "fixture: agg count oracle = 50 (mix)");
        ASSERT_EQ_INT((int)a, (int)b, "case3: aggregate count ce_mix");
    }

    /* 4: group_by=c vector equality on ce_comp. RED on base. */
    assert_group_vectors_equal(tc, "ce_comp", CRIT_AB,
                               "case4: group_by=c composite-exact");

    /* 5: TopN group_by=c, limit 3 — labels AND counts AND order equal. */
    assert_topn_equal(tc, "ce_comp", CRIT_AB, 3,
                      "case5: TopN composite-exact");

    /* 6: find control — the D1 composite-exact path is correct today
     * and must stay so. Compare sorted key sets. */
    {
        char req[1024];
        char *ra = NULL, *rb = NULL;
        int ka[300], kb[300], nka = 0, nkb = 0;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"ce_plain\",\"criteria\":%s,"
                 "\"limit\":300}", CRIT_AB);
        tc_request(tc, req, &ra);
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"ce_comp\",\"criteria\":%s,"
                 "\"limit\":300}", CRIT_AB);
        tc_request(tc, req, &rb);
        ASSERT_NOT_NULL(ra, "case6: oracle find");
        ASSERT_NOT_NULL(rb, "case6: ce_comp find");
        /* order-independent key-set comparison */
        const char *pp = ra;
        while (pp && (pp = SAFE_STRSTR(pp, "\"key\":\"k")) != NULL &&
               nka < 300) {
            ka[nka++] = atoi(pp + 8);
            pp += 8;
        }
        pp = rb;
        while (pp && (pp = SAFE_STRSTR(pp, "\"key\":\"k")) != NULL &&
               nkb < 300) {
            kb[nkb++] = atoi(pp + 8);
            pp += 8;
        }
        ASSERT_EQ_INT(nka, nkb, "case6: same row count");
        for (int i = 1; i < nka; i++) {
            int v = ka[i]; int j = i - 1;
            while (j >= 0 && ka[j] > v) { ka[j + 1] = ka[j]; j--; }
            ka[j + 1] = v;
        }
        for (int i = 1; i < nkb; i++) {
            int v = kb[i]; int j = i - 1;
            while (j >= 0 && kb[j] > v) { kb[j + 1] = kb[j]; j--; }
            kb[j + 1] = v;
        }
        for (int i = 0; i < nka && i < nkb; i++)
            ASSERT_EQ_INT(ka[i], kb[i], "case6: same key sets");
        free(ra);
        free(rb);
    }

    /* 7: explain control — find still reports the composite_exact plan
     * (the fix must not disturb the planner output find consumes). */
    {
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"ce_comp\",\"criteria\":%s,\"explain\":true}",
                 CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case7: explain response");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"order\":\"composite_exact\"") != NULL,
                    "case7: find plan stays composite_exact");
        free(resp); resp = NULL;
    }

    /* 8: bulk-delete destructive-op assertion (safe-by-accident on
     * base: CBF_NONE fallback + full-tree re-verification). */
    {
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"bulk-delete\",\"dir\":\"default\","
                 "\"object\":\"ce_del\",\"criteria\":%s}", CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case8: bulk-delete response");
        long del = -1;
        if (resp) {
            const char *q = SAFE_STRSTR(resp, "\"deleted\":");
            if (q) del = atol(q + strlen("\"deleted\":"));
        }
        if (del != 50)
            TAP_DIAG("# case8 response: %.300s\n", resp ? resp : "(null)");
        ASSERT_EQ_INT((int)del, 50, "case8: bulk-delete removed exactly 50");
        free(resp); resp = NULL;
        long post = scalar_count(tc, "ce_del", NULL);
        ASSERT_EQ_INT((int)post, NRECS - 50,
                      "case8: post-delete count == model (250)");
    }

    /* 9: single-leaf control — [b eq 1] uses a real standalone index on
     * every indexed twin; the normal leaf path stays fast and correct. */
    {
        const char *CB = "[{\"field\":\"b\",\"op\":\"eq\",\"value\":1}]";
        long a = scalar_count(tc, "ce_plain", CB);
        long b = scalar_count(tc, "ce_comp", CB);
        ASSERT_EQ_INT((int)a, 150, "fixture: [b=1] matches 150 rows");
        ASSERT_EQ_INT((int)a, (int)b, "case9: single-leaf control");
    }

    /* 10: perf-shape control — count/aggregate plans never force the
     * scan when standalone indexes exist. The planner serves no-fetch
     * callers (fetching=0) WITHOUT the composite-exact step, so the
     * composite-covered eq leaves compete as ordinary leaves:
     *   ce_mix (standalone a+b) → an index-driven plan (leaf on the
     *   most selective standalone index — seed + post-filter at this
     *   cardinality; intersect when both leaves are selective), and
     *   ce_comp (only b standalone) → leaf on b, a as post-filter.
     * RED before the planner change: both explains reported the
     * composite_exact order (which the count/aggregate executors then
     * normalized to a full scan). */
    {
        char req[1024];
        /* count explain on ce_mix: an index-driven, non-scan,
         * non-composite_exact plan seeding on a standalone index. */
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"ce_mix\",\"criteria\":%s,\"explain\":true}",
                 CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case10: ce_mix count explain");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"plan\":\"scan\"") == NULL,
                    "case10: ce_mix count is not forced to a scan "
                    "(standalone a/b indexes exist)");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"order\":\"composite_exact\"") == NULL,
                    "case10: ce_mix count plan is not composite_exact");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"role\":\"seed\"") != NULL &&
                    SAFE_STRSTR(resp, "\"index\":\"btree\"") != NULL,
                    "case10: ce_mix count seeds on a standalone btree index");
        free(resp); resp = NULL;

        /* count explain on ce_comp: leaf plan seeded on the standalone b
         * index (a rides as post-filter), not composite_exact, not scan. */
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"ce_comp\",\"criteria\":%s,\"explain\":true}",
                 CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case10: ce_comp count explain");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"plan\":\"leaf\"") != NULL,
                    "case10: ce_comp count plans a leaf (scan only when "
                    "no source leaf has its own index)");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"order\":\"composite_exact\"") == NULL,
                    "case10: ce_comp count plan is not composite_exact");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"field\":\"b\"") != NULL &&
                    SAFE_STRSTR(resp, "\"role\":\"seed\"") != NULL,
                    "case10: ce_comp count seeds on the standalone b index");
        free(resp); resp = NULL;

        /* aggregate explain on ce_mix: same no-fetch plan shape. */
        snprintf(req, sizeof(req),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"ce_mix\",\"criteria\":%s,\"explain\":true}",
                 CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case10: ce_mix aggregate explain");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"plan\":\"scan\"") == NULL,
                    "case10: aggregate is not forced to a scan");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"order\":\"composite_exact\"") == NULL,
                    "case10: aggregate plan is not composite_exact");
        ASSERT_TRUE(resp &&
                    SAFE_STRSTR(resp, "\"role\":\"seed\"") != NULL,
                    "case10: aggregate seeds on a standalone index");
        free(resp); resp = NULL;
    }

    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-composite-exact-count-agg",
              test_composite_exact_count_agg_run)
