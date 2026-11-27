/* Regression: AUD-QRY-2 — aggregate (scalar and group_by) silently
 * over-counts when the criteria tree carries an OR / nested-AND
 * sub-tree beside indexed leaves: the planner computed a local
 * `has_subtree` flag and threw it away ((void)has_subtree;), so every
 * no-fetch KeySet fast path (scalar aw, IGB crit_ks, IGB bitmap
 * can_slot, TopN prefilter) treated the plan's source leaves as the
 * full predicate and dropped the sub-tree. The fix carries the flag on
 * FilterPlan and refuses the no-fetch gates for sub-tree-bearing plans.
 *
 * Case map (numbers match docs/plans/2026-11-19-audit-qry2-*.md):
 *   1  scalar OR, mixed-indexed (aw drop)          RED on base
 *   2  scalar OR, all-indexed                      RED on base
 *   3  group_by OR (IGB crit_ks drop)              RED on base
 *   4  bitmap slot-level OR (can_slot drop)        RED on base
 *   5  TopN OR (on agg_btree)                      RED on base
 *   6  two indexed AND-leaves + OR (intersect      GREEN on base
 *      relocation shape; small_primary restores)
 *   7  AND-of-two-ORs (nL==0 partial union)        RED on base
 *   9  large-scale intersect + OR (agg_big;        RED on base
 *      Task 3 restore necessity)
 *   10 IGB intersect shape group_by (agg_bm)       RED on base
 *      + TopN variant (on agg_btree)
 *   11 array-form all-indexed OR control           GREEN on base
 *      (explain plan == union; result exact)
 *   12 orprobe shape on agg_bm (post-QRY-1         RED on base
 *      residual; can_slot route)
 *   13 over-8-leaves AND (MAX_INTERSECT_LEAVES     RED on base
 *      leaf loss; group_by 27 vs 2)
 *   14 (fixture spec for case 9: agg_big shape)
 *   15 in-process flag assertions live in a
 *      separate case (test-agg-subtree-flag-fields)
 *   8/16 controls: postfilter-leaf scalar, pure-OR
 *      root (case 11), fully-indexed AND           GREEN always
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

#define NLABELS 5
static const char *LABELS[NLABELS] = {
    "cat_0", "cat_1", "cat_2", "cat_3", "cat_4"
};

/* Extract "<label>", then the next "n":<int> after it. -1 when absent. */
static int label_count(const char *resp, const char *label) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"v\":\"%s\"", label);
    const char *p = SAFE_STRSTR(resp, pat);
    if (!p) return -1;
    const char *q = SAFE_STRSTR(p + strlen(pat), "\"n\":");
    if (!q) return -1;
    return atoi(q + strlen("\"n\":"));
}

/* Scalar aggregate count (no group_by): parse the "n":<int> value.
 * Returns 0 ok, -1 parse failure. NOTE: on base this drops OR
 * sub-trees on indexed twins — that is the defect under test. */
static int agg_scalar_count(TestClient *tc, const char *obj,
                            const char *criteria, long *out) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]%s%s}",
             obj,
             criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "");
    tc_request(tc, req, &resp);
    if (!resp) return -1;
    const char *q = SAFE_STRSTR(resp, "\"n\":");
    if (!q) { free(resp); return -1; }
    *out = atol(q + strlen("\"n\":"));
    free(resp);
    return 0;
}

/* count MODE (O(1)-metadata/validated executor — correct today on all
 * twins; used as the oracle cross-check). */
static int mode_count(TestClient *tc, const char *obj,
                      const char *criteria, long *out) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"%s%s}",
             obj,
             criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "");
    tc_request(tc, req, &resp);
    if (!resp || resp[0] == '\0') return -1;
    *out = atol(resp);
    free(resp);
    return 0;
}

/* group_by=v count: per-label counts for cat_0..cat_4 (-1 when the
 * label is absent from the response). */
static void group_vector(TestClient *tc, const char *obj,
                         const char *criteria, int counts[NLABELS]) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"v\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]%s%s}",
             obj,
             criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "group_by response");
    for (int i = 0; i < NLABELS; i++)
        counts[i] = resp ? label_count(resp, LABELS[i]) : -1;
    free(resp);
}

/* TopN response: emit (label,count) pairs in wire order. */
static void topn_pairs(TestClient *tc, const char *obj,
                       const char *criteria, int limit,
                       char labels[][16], int counts[], int *npairs) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"v\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]%s%s,"
             "\"order_by\":\"n\",\"order\":\"desc\",\"limit\":%d}",
             obj,
             criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "", limit);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "TopN response");
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"v\":\"cat_")) != NULL &&
           n < *npairs) {
        int lbl = atoi(p + strlen("\"v\":\"cat_"));
        const char *q = SAFE_STRSTR(p, "\"n\":");
        if (lbl >= 0 && lbl < NLABELS && q) {
            snprintf(labels[n], 16, "cat_%d", lbl);
            counts[n] = atoi(q + strlen("\"n\":"));
            n++;
        }
        p += strlen("\"v\":\"cat_");
    }
    *npairs = n;
    free(resp);
}

/* Compare two TopN responses per the plan's tie rule: the sorted count
 * vectors must be equal; labels must match only where a count is
 * strictly greater than the last (boundary) count — ties at the limit
 * boundary may pick different labels legitimately. */
static void assert_topn_match(TestClient *tc, const char *criteria,
                              int limit, const char *msg) {
    char la[8][16], lb[8][16];
    int ca[8], cb[8];
    int na = 8, nb = 8;
    topn_pairs(tc, "agg_plain", criteria, limit, la, ca, &na);
    topn_pairs(tc, "agg_btree", criteria, limit, lb, cb, &nb);
    ASSERT_EQ_INT(na, nb, msg);
    if (na != nb) return;
    /* insertion-sort counts desc (labels ride along) */
    for (int i = 1; i < na; i++) {
        int c = ca[i]; char l[16]; snprintf(l, sizeof(l), "%s", la[i]);
        int j = i - 1;
        while (j >= 0 && ca[j] < c) {
            ca[j + 1] = ca[j]; snprintf(la[j + 1], 16, "%s", la[j]); j--;
        }
        ca[j + 1] = c; snprintf(la[j + 1], 16, "%s", l);
    }
    for (int i = 1; i < nb; i++) {
        int c = cb[i]; char l[16]; snprintf(l, sizeof(l), "%s", lb[i]);
        int j = i - 1;
        while (j >= 0 && cb[j] < c) {
            cb[j + 1] = cb[j]; snprintf(lb[j + 1], 16, "%s", lb[j]); j--;
        }
        cb[j + 1] = c; snprintf(lb[j + 1], 16, "%s", l);
    }
    int boundary = cb[na - 1];
    for (int i = 0; i < na; i++) {
        ASSERT_EQ_INT(ca[i], cb[i], msg);
        if (cb[i] > boundary)
            ASSERT_EQ_STR(la[i], lb[i], msg);
    }
}

/* Insert the shared 400-row fixture into one object (dict bulk form,
 * one request — ~40 KB). Rows: v=cat_(i%5), u=i%13, t=i, w=w_(i%3). */
static void insert_fixture(TestClient *tc, const char *obj) {
    const int NROWS = 400;
    const size_t reclen = 96;
    char *body = malloc((size_t)NROWS * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc fixture body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "{");
    for (int i = 0; i < NROWS; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s\"k%03d\":{\"v\":\"cat_%d\",\"u\":%d,\"t\":%d,\"w\":\"w_%d\"}",
            i ? "," : "", i, i % 5, i % 13, i, i % 3);
    }
    off += (size_t)snprintf(body + off, 8, "}");
    char *req = malloc(off + 128);
    ASSERT_NOT_NULL(req, "alloc fixture request");
    snprintf(req, off + 128,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\","
             "\"records\":%s}", obj, body);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "bulk insert response");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "bulk insert accepted");
    free(resp);
    free(req);
    free(body);
}

/* Insert the 40k-row agg_big fixture (case 14 spec) in 2000-row
 * chunks. v: a/b by i%2; w: w_0/w_1 by (i/2)%2; u: i%13; keys k%06d. */
static void insert_big_fixture(TestClient *tc, const char *obj) {
    const int NROWS = 40000, CHUNK = 2000;
    const size_t reclen = 80;
    char *body = malloc((size_t)CHUNK * reclen + 16);
    char *req = malloc((size_t)CHUNK * reclen + 256);
    ASSERT_NOT_NULL(body, "alloc big body");
    ASSERT_NOT_NULL(req, "alloc big request");
    for (int base = 0; base < NROWS; base += CHUNK) {
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "{");
        for (int i = base; i < base + CHUNK; i++) {
            off += (size_t)snprintf(body + off, reclen,
                "%s\"k%06d\":{\"v\":\"%s\",\"w\":\"w_%d\",\"u\":%d}",
                i == base ? "" : ",", i,
                (i % 2) ? "b" : "a", (i / 2) % 2, i % 13);
        }
        off += (size_t)snprintf(body + off, 8, "}");
        snprintf(req, (size_t)CHUNK * reclen + 256,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"records\":%s}", obj, body);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "big bulk response");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "big bulk chunk accepted");
        free(resp);
    }
    free(body);
    free(req);
}

static int test_agg_or_subtree_full_tree_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Oracle twin: no indexes. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\",\"w:varchar:16\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle object created");
    free(resp); resp = NULL;

    /* Btree twin. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_btree\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\",\"w:varchar:16\"],"
        "\"indexes\":[\"v\",\"t\",\"w\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "btree object created");
    free(resp); resp = NULL;

    /* Bitmap twin: v/w bitmaps + t btree (u stays unindexed). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_bm\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\",\"w:varchar:16\"],"
        "\"indexes\":[\"v:bitmap\",\"w:bitmap\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "bitmap object created");
    free(resp); resp = NULL;

    insert_fixture(tc, "agg_plain");
    insert_fixture(tc, "agg_btree");
    insert_fixture(tc, "agg_bm");

    /* Case 14 fixture: agg_big (40k; v/w 2-value bitmaps, u btree) +
     * its no-index oracle twin. Each v/w leaf = 20000 rows and their
     * intersection = 10000 >= INTERSECT_MIN_PRIMARY, so small_primary
     * stays 0 and Task 3's restore condition is the only guard. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_big\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"w:varchar:16\",\"u:int\"],"
        "\"indexes\":[\"v:bitmap\",\"w:bitmap\",\"u\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "big object created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_big_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"w:varchar:16\",\"u:int\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "big oracle created");
    free(resp); resp = NULL;
    insert_big_fixture(tc, "agg_big");
    insert_big_fixture(tc, "agg_big_plain");

    const char *C1 =
        "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149},"
        "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
        "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]}]";
    const char *C2 =
        "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149},"
        "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
        "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_2\"}]}]";
    const char *C6 =
        "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
        "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
        "{\"or\":[{\"field\":\"u\",\"op\":\"eq\",\"value\":5},"
        "{\"field\":\"u\",\"op\":\"eq\",\"value\":9}]}]";

    /* Case 1: scalar OR, mixed-indexed — the aw fast path counts the
     * t-leaf's full key set (100) instead of the tree-checked value. */
    {
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C1, &a), 0,
                      "case1: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", C1, &b), 0,
                      "case1: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case1: scalar OR mixed-indexed matches oracle");
    }

    /* Case 2: scalar OR, all-indexed — same drop through a fully
     * indexed OR. */
    {
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C2, &a), 0,
                      "case2: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", C2, &b), 0,
                      "case2: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case2: scalar OR all-indexed matches oracle");
    }

    /* Case 3: group_by OR (IGB crit_ks drop) — per-label vector. */
    {
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", C1, va);
        group_vector(tc, "agg_btree", C1, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg), "case3: label %d vector match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
    }

    /* Case 4: bitmap slot-level OR on agg_bm (can_slot drop). */
    {
        const char *C4 =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"or\":[{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_2\"}]}]";
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", C4, va);
        group_vector(tc, "agg_bm", C4, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg), "case4: label %d vector match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
    }

    /* Case 5: TopN OR on agg_btree (TopN needs a btree group field). */
    assert_topn_match(tc, C1, 3, "case5: TopN OR matches oracle");

    /* Case 6: two indexed AND-leaves + OR — intersect relocation
     * shape. GREEN on base (small_primary restores the tree at
     * fixture scale) and must stay green post-fix. */
    {
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C6, &a), 0,
                      "case6: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", C6, &b), 0,
                      "case6: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case6: intersect+OR matches oracle");
    }

    /* Case 7: AND-of-two-ORs — the nL==0 union covers only the FIRST
     * fully-indexed OR conjunct (160 vs true ~106 class). */
    {
        const char *C7 =
            "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_2\"}]},"
            "{\"or\":[{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_0\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_2\"}]}]";
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C7, &a), 0,
                      "case7: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", C7, &b), 0,
                      "case7: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case7: AND-of-ORs matches oracle");
    }

    /* Case 9 (+14 fixture): large-scale intersect + OR — small_primary
     * stays 0 (leaves = 20000, intersection = 10000), so the tree is
     * NOT restored today and the OR drops. Task 3's restore condition
     * is what turns this green. */
    {
        const char *C9 =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"a\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_0\"},"
            "{\"or\":[{\"field\":\"u\",\"op\":\"eq\",\"value\":3},"
            "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]}]";
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_big_plain", C9, &a), 0,
                      "case9: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_big", C9, &b), 0,
                      "case9: big count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case9: large intersect+OR matches oracle");
    }

    /* Case 10: IGB intersect shape on agg_bm, group_by (reviewer
     * measured 27 vs 4 at fixture scale). */
    {
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", C6, va);
        group_vector(tc, "agg_bm", C6, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg), "case10: label %d vector match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
        /* TopN variant on agg_btree (round-6 minor: TopN is reachable
         * only with a btree group field; on agg_bm the shape goes
         * IGB + sort instead). */
        assert_topn_match(tc, C6, 3, "case10: TopN variant matches oracle");
    }

    /* Case 11: array-form all-indexed OR control — explain must report
     * the union plan (an unindexed OR child would scan and could not
     * catch a wrongly-firing n_children rule) AND the result must be
     * exact on both twins. */
    {
        const char *C11 =
            "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]}]";
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"agg_btree\",\"aggregates\":"
                 "[{\"fn\":\"count\",\"alias\":\"n\"}],\"criteria\":%s,"
                 "\"explain\":true}", C11);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case11: explain response");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"plan\":\"union\"") != NULL,
                    "case11: explain plan is union");
        free(resp); resp = NULL;
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C11, &a), 0,
                      "case11: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", C11, &b), 0,
                      "case11: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case11: array-form OR matches oracle");
    }

    /* Case 12: orprobe shape on agg_bm — the post-QRY-1 residual
     * (evidence AUD-QRY-1-residual-orprobe.{sh,out}: 14 vs 20). RED on
     * base via the IGB can_slot slot walk ignoring the OR; green with
     * the Task 2 fully_covered refusal alone. count MODE is correct
     * today on all twins (its executor re-checks) — pinned as the
     * cross-check; the AGGREGATE count(*) fast path is the defect
     * itself, so it is NOT used as an oracle here. */
    {
        const char *C12 =
            "[{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_0\"},"
            "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_2\"},"
            "{\"field\":\"u\",\"op\":\"between\",\"value\":2,\"value2\":9}]}]";
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", C12, va);
        group_vector(tc, "agg_bm", C12, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg), "case12: label %d vector match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
        long a = -1, b = -1;
        ASSERT_EQ_INT(mode_count(tc, "agg_plain", C12, &a), 0,
                      "case12: oracle count-mode");
        ASSERT_EQ_INT(mode_count(tc, "agg_bm", C12, &b), 0,
                      "case12: bitmap count-mode");
        ASSERT_EQ_INT((int)a, (int)b,
                      "case12: count MODE matches (correct today)");
        /* cross-check: count MODE on the oracle twin equals its own
         * group_by vector sum */
        long sum = 0;
        for (int i = 0; i < NLABELS; i++) sum += va[i] > 0 ? va[i] : 0;
        ASSERT_EQ_INT((int)sum, (int)a, "case12: vector sum == count-mode");
    }

    /* Case 13: over-8-leaves AND — 9 LEAF children; collect_and_leaves
     * silently drops leaf 9+ (u eq 7). True count = 2 (i=46, 241);
     * dropping u gives 27. count() is correct today (its executor
     * re-checks); group_by v on agg_bm is RED. */
    {
        const char *C13 =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]";
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", C13, va);
        group_vector(tc, "agg_bm", C13, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg), "case13: label %d vector match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
        long a = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", C13, &a), 0,
                      "case13: oracle count");
        ASSERT_EQ_INT((int)a, 2, "case13: true count is 2 (i=46,241)");
        long b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_bm", C13, &b), 0,
                      "case13: bitmap count");
        ASSERT_EQ_INT((int)b, 2, "case13: count() stays correct");
    }

    /* Controls (8/16): must pass before AND after. */
    {
        /* postfilter-leaf scalar: unindexed u forces the record-recheck
         * executor — proves fallback exactness. */
        const char *CC =
            "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149},"
            "{\"field\":\"u\",\"op\":\"gte\",\"value\":7}]";
        long a = -1, b = -1;
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", CC, &a), 0,
                      "control: oracle count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", CC, &b), 0,
                      "control: btree count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "control: postfilter-leaf scalar matches oracle");

        /* fully-indexed AND without OR: intersect, exact. */
        const char *CA =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]";
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_plain", CA, &a), 0,
                      "control: oracle AND count");
        ASSERT_EQ_INT(agg_scalar_count(tc, "agg_btree", CA, &b), 0,
                      "control: btree AND count");
        ASSERT_EQ_INT((int)a, (int)b,
                      "control: fully-indexed AND matches oracle");

        /* No-criteria popcount sanity on the bitmap twin. */
        int va[NLABELS], vb[NLABELS];
        group_vector(tc, "agg_plain", NULL, va);
        group_vector(tc, "agg_bm", NULL, vb);
        for (int i = 0; i < NLABELS; i++) {
            char msg[64];
            snprintf(msg, sizeof(msg),
                     "control: no-criteria label %d match", i);
            ASSERT_EQ_INT(va[i], vb[i], msg);
        }
    }

    tc_close(tc);   /* test_env_stop does not free clients */
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-agg-or-subtree-full-tree",
              test_agg_or_subtree_full_tree_run)
