/* Regression: AUD-QRY-5 — aggregate group_by over a bitmap-indexed
 * field holding MORE than 256 distinct values silently returns at most
 * 256 groups: bm_collect_uniq_cb stops collecting new distinct values
 * at the context cap (both call sites pass cap 256 with 256-row stack
 * arrays) on every shape of the IGB bitmap fast path — no-criteria
 * popcount, can_slot slot-level walk, and the Phase 1b hbm branch.
 * count/find over the same object are correct. Fix: the collection
 * callback latches an overflow signal; the bitmap fast path routes to
 * the record-scan executor (the differential oracle) when it fires.
 *
 * Shapes (numbers = docs/plans/2026-11-19-audit-qry5-*.md Task 2):
 *   1  no-criteria group_by=v, count-only spec (popcount branch)
 *   2  no-criteria group_by=v, count+sum(u) specs (Phase 1b hbm branch:
 *      sum on a btree-indexed non-count numeric spec sets igb_needs_hbm)
 *   3  criteria [w eq w_1] group_by=v (can_slot slot-level branch)
 *   4  cap_small control: <=256 distinct — correct before and after
 *      (correctness-only; fast-path retention for <=256-distinct
 *      objects is asserted by code-read: the latch fires only on
 *      genuine overflow)
 *   5  count (no criteria) on cap_bm == 900 — the defect is
 *      group_by-specific
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

#define NRECS 900
#define NDISTINCT 300

typedef struct {
    char label[16];
    long count;
    long sum;   /* -1 when the response carries no sum */
} GEntry;

static int gentry_cmp(const void *a, const void *b) {
    return strcmp(((const GEntry *)a)->label, ((const GEntry *)b)->label);
}

/* Extract every ("v":"vNNN","n":<int>[, "s":<int>]) triple from a
 * group_by=v response; sort by label. Returns the entry count. */
static int extract_groups(const char *resp, GEntry *out, int cap,
                          int with_sum) {
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"v\":\"v")) != NULL && n < cap) {
        const char *q = SAFE_STRSTR(p, "\"n\":");
        if (!q) break;
        snprintf(out[n].label, sizeof(out[n].label), "v%s",
                 p + strlen("\"v\":\"v"));
        char *end = strchr(out[n].label, '"');
        if (end) *end = '\0';
        out[n].count = atol(q + 4);
        out[n].sum = -1;
        if (with_sum) {
            const char *r = SAFE_STRSTR(q, "\"s\":");
            if (r) out[n].sum = atol(r + 4);
        }
        n++;
        p += strlen("\"v\":\"v");
    }
    qsort(out, (size_t)n, sizeof(GEntry), gentry_cmp);
    return n;
}

/* Run group_by=v on one object; caller compares against the oracle. */
static void group_v(TestClient *tc, const char *obj, const char *criteria,
                    int with_sum, GEntry *out, int *nout) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"v\",\"aggregates\":[{\"fn\":\"count\","
             "\"alias\":\"n\"}%s]%s%s}",
             obj,
             with_sum ? ",{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}"
                      : "",
             criteria ? ",\"criteria\":" : "",
             criteria ? criteria : "");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "group_by response");
    *nout = resp ? extract_groups(resp, out, 512, with_sum) : 0;
    free(resp);
}

static void assert_groups_equal(TestClient *tc, const char *oracle,
                                const char *obj, const char *criteria,
                                int with_sum, int expect_labels,
                                const char *msg) {
    GEntry va[512], vb[512];
    int na, nb;
    group_v(tc, oracle, criteria, with_sum, va, &na);
    group_v(tc, obj, criteria, with_sum, vb, &nb);
    if (na != expect_labels || na != nb)
        TAP_DIAG("# %s: oracle labels=%d (expect %d), twin labels=%d\n",
                 msg, na, expect_labels, nb);
    ASSERT_EQ_INT(na, expect_labels, msg);
    ASSERT_EQ_INT(na, nb, msg);
    if (na != nb) return;
    for (int i = 0; i < na; i++) {
        ASSERT_EQ_STR(va[i].label, vb[i].label, msg);
        ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count, msg);
        if (with_sum) {
            ASSERT_TRUE(va[i].sum >= 0 && vb[i].sum >= 0, msg);
            ASSERT_EQ_INT((int)va[i].sum, (int)vb[i].sum, msg);
        }
    }
}

static void insert_fixture(TestClient *tc, const char *obj, int distinct) {
    /* distinct = how many distinct v values the rows carry (i % distinct) */
    const size_t reclen = 80;
    char *body = malloc((size_t)NRECS * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < NRECS; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s{\"key\":\"k%04d\",\"value\":{\"v\":\"v%03d\",\"u\":%d,"
            "\"w\":\"w_%d\",\"t\":%d}}",
            i ? "," : "", i, i % distinct, i % 13, i % 2, i);
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

static int test_igb_bitmap_dict_cap_run(void) {
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
        "\"object\":\"cap_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"u:int\",\"w:varchar:8\",\"t:long\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle created");
    free(resp); resp = NULL;

    /* Bitmap twin: v bitmap(300) (over the 256 collection cap), w
     * bitmap (the can_slot criteria source), t/u btrees (u gives the
     * Phase 1b shape its btree-indexed numeric spec). Short values
     * only — >1024 B values are AUD-QRY-6's defect, not this one. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cap_bm\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"u:int\",\"w:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"v:bitmap(300)\",\"w:bitmap(4)\",\"t\",\"u\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cap_bm created");
    free(resp); resp = NULL;

    /* Over-fixing control: same bitmap(300) but only 100 distinct
     * values in its rows, plus its own no-index oracle twin (cap_plain
     * carries 300 distinct values, so it cannot serve as cap_small's
     * oracle). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cap_small\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"u:int\",\"w:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"v:bitmap(300)\",\"w:bitmap(4)\",\"t\",\"u\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cap_small created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cap_small_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"u:int\",\"w:varchar:8\",\"t:long\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cap_small oracle created");
    free(resp); resp = NULL;

    insert_fixture(tc, "cap_plain", NDISTINCT);
    insert_fixture(tc, "cap_bm", NDISTINCT);
    insert_fixture(tc, "cap_small", 100);
    insert_fixture(tc, "cap_small_plain", 100);

    /* 1: no-criteria group_by=v, count-only spec (popcount branch) —
     * full 300-label equality. RED on base: cap_bm returns 256. */
    {
        char msg[256] = "case1: no-criteria popcount branch";
        assert_groups_equal(tc, "cap_plain", "cap_bm", NULL, 0, NDISTINCT,
                            msg);
    }

    /* 2: no-criteria group_by=v with count+sum(u) (Phase 1b hbm
     * branch) — labels AND per-label sums equal. RED on base. */
    {
        char msg[256] = "case2: Phase 1b hbm branch";
        assert_groups_equal(tc, "cap_plain", "cap_bm", NULL, 1, NDISTINCT,
                            msg);
    }

    /* 3: criteria [w eq w_1] group_by=v (can_slot slot-level branch —
     * all criteria sources bitmap eq, no postfilter). The surviving
     * label set is the ODD v labels only (w_1 rows are odd i; v=i%300
     * keeps i's parity), 150 labels x 3 rows each. RED on base: the
     * slot-level walk serves wrong labels/counts (300/132-class vs the
     * oracle's 150). */
    {
        char msg[256] = "case3: can_slot slot-level branch";
        assert_groups_equal(tc, "cap_plain", "cap_bm",
                            "[{\"field\":\"w\",\"op\":\"eq\","
                            "\"value\":\"w_1\"}]",
                            0, NDISTINCT / 2, msg);
    }

    /* 4: cap_small control (100 distinct < cap) — correct today AND
     * post-fix. */
    {
        char msg[256] = "case4: <=256-distinct control";
        assert_groups_equal(tc, "cap_small_plain", "cap_small", NULL, 0,
                            100, msg);
    }

    /* 5: count (no criteria) on cap_bm == 900 — the defect is
     * group_by-specific. GREEN on base. */
    {
        char req[512];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"cap_bm\"}");
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case5: count response");
        ASSERT_TRUE(resp && atol(resp) == NRECS,
                    "case5: count == 900 (defect is group_by-specific)");
        free(resp); resp = NULL;
    }

    tc_close(tc);   /* test_env_stop does not free clients */
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-igb-bitmap-dict-cap", test_igb_bitmap_dict_cap_run)
