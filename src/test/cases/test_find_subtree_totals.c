/* Regression: AUD-QRY-4 — find drops OR sub-trees from rows and/or
 * `total` on routes that trust the plan's candidate set without a
 * full-tree re-match, and several total sites emit WRONG numbers where
 * they should decline to exact-or-null:
 *   site 1  keyset_find_from_intersect need_rematch + total (large
 *           primaries: [leaf, leaf, OR…] with both leaves >= 10000)
 *   site 9439-class  prepared_find_prepare intersect need_rematch
 *           (the bitmap-leaf twin: rows wrong while total stays exact)
 *   site 3  keyset_find_from_or's unconditional |union| total (the
 *           nL==0 AND-of-ORs plan over-counts: superset 160 vs true 106)
 *   site 4  the streaming-leaf total gate (sub-tree-blind)
 *   site 5  the non-streaming leaf total (no n_postfilter/has_subtree
 *           guard; count's use of the same helper IS guarded)
 *   the caller's unconditional find_total_null = 0 defeats every
 *           decline (wrong "total":0 class) — replaced by the
 *           null-alone decline contract with an out_declined out-channel
 *           for the intersect executor's small_primary condition.
 *
 * Case map (numbers = docs/plans/2026-11-19-audit-qry4-*.md Task 1):
 *   1  C1 shape limit:1 total:true on ft_ix     RED (streaming gate)
 *   2  AND-of-ORs total:true on ft_ix           RED (partial union total)
 *   3  large intersect + OR on ft_big           RED (rows AND total)
 *   3b prepared intersect + OR on ft_big        RED (rows; total exact)
 *   4  declined-total contract (small intersect) RED on base ("total":0)
 *   5  controls: pure-OR / leaf-only / small    GREEN
 *      intersect+OR rows
 *
 * Oracle provenance: every expected total comes from a COUNT-mode
 * response on the no-index twin (FULL_SCAN find totals are null — never
 * compare against those).
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

/* Parse the LAST "total": occurrence in a find(total:true) response.
 * Returns 0 ok; *is_null=1 when the total is null. */
static int find_total_of(const char *resp, long *out, int *is_null) {
    const char *last = NULL;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"total\":")) != NULL) {
        last = p;
        p += 8;
    }
    if (!last) return -1;
    if (SAFE_STRSTR(last, "null") == last) {
        *is_null = 1;
        *out = -1;
        return 0;
    }
    *is_null = 0;
    *out = atol(last + 8);
    return 0;
}

/* Collect the find response's key numbers (keys are k%04d), sorted. */
static int find_keys(TestClient *tc, const char *obj, const char *criteria,
                     const char *extra, int *out, int cap) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s,\"limit\":100000%s%s}",
             obj, criteria,
             extra ? "," : "", extra ? extra : "");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "find response");
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"key\":\"k")) != NULL && n < cap) {
        out[n++] = atoi(p + 8);
        p += 8;
    }
    free(resp);
    for (int i = 1; i < n; i++) {
        int v = out[i];
        int j = i - 1;
        while (j >= 0 && out[j] > v) { out[j + 1] = out[j]; j--; }
        out[j + 1] = v;
    }
    return n;
}

static long mode_count(TestClient *tc, const char *obj,
                       const char *criteria) {
    char req[1024];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s}", obj, criteria);
    tc_request(tc, req, &resp);
    long v = resp ? atol(resp) : -1;
    free(resp);
    return v;
}

static void insert_rows(TestClient *tc, const char *obj, int nrows,
                        int wide) {
    /* wide: the ft_big row shape (v/w/b 2-value, t=u=i); else the
     * ft_ix shape (v 5 cats, w 3 cats, t=u=i). */
    const int CHUNK = 1000;
    const size_t reclen = 110;
    char *body = malloc((size_t)CHUNK * reclen + 16);
    char *req = malloc((size_t)CHUNK * reclen + 256);
    ASSERT_NOT_NULL(body, "alloc body");
    ASSERT_NOT_NULL(req, "alloc request");
    for (int base = 0; base < nrows; base += CHUNK) {
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        int end = base + CHUNK < nrows ? base + CHUNK : nrows;
        for (int i = base; i < end; i++) {
            if (wide)
                off += (size_t)snprintf(body + off, reclen,
                    "%s{\"key\":\"k%05d\",\"value\":{\"v\":\"v%d\","
                    "\"w\":\"w%d\",\"b\":\"b_%d\",\"t\":%d,\"u\":%d}}",
                    i == base ? "" : ",", i, i % 2, (i / 2) % 2,
                    i % 2, i, i);
            else
                off += (size_t)snprintf(body + off, reclen,
                    "%s{\"key\":\"k%04d\",\"value\":{\"v\":\"cat_%d\","
                    "\"w\":\"w_%d\",\"t\":%d,\"u\":%d}}",
                    i == base ? "" : ",", i, i % 5, i % 3, i, i);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        snprintf(req, (size_t)CHUNK * reclen + 256,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"records\":%s}", obj, body);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "bulk response");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "bulk chunk accepted");
        free(resp);
    }
    free(body);
    free(req);
}

static int test_find_subtree_totals_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* ft_plain / ft_ix: 400-row pair (v 5 cats, w 3 cats, t=u=i). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ft_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"w:varchar:16\",\"t:long\",\"u:int\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ft_plain created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ft_ix\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"w:varchar:16\",\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v\",\"t\",\"w\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ft_ix created");
    free(resp); resp = NULL;

    /* ft_big_plain / ft_big: 24000-row pair (v/w/b 2-value — b is the
     * BITMAP leaf that makes the prepared-intersect route reachable;
     * t=u=i so the OR band [1000,12000) excludes ~22 000 intersection
     * rows and the red's algebra is checkable). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ft_big_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"w:varchar:16\",\"b:varchar:16\","
        "\"t:long\",\"u:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ft_big_plain created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"ft_big\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"w:varchar:16\",\"b:varchar:16\","
        "\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v\",\"t\",\"w\",\"b:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "ft_big created");
    free(resp); resp = NULL;

    insert_rows(tc, "ft_plain", 400, 0);
    insert_rows(tc, "ft_ix", 400, 0);
    insert_rows(tc, "ft_big_plain", 24000, 1);
    insert_rows(tc, "ft_big", 24000, 1);

    /* ===== 1: C1 shape, limit:1, total:true on ft_ix (streaming
     * gate). RED on base: total reads the seed leaf's count (100) —
     * the OR sub-tree is invisible to the gate. */
    {
        const char *C1 =
            "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149},"
            "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]}]";
        long oracle = mode_count(tc, "ft_plain", C1);
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_ix\","
                 "\"criteria\":%s,\"limit\":1,\"total\":true}", C1);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case1 find response");
        long total = -1;
        int is_null = 0;
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "case1: parse total");
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "case1: total (%ld) == oracle count (%ld)", total, oracle);
        ASSERT_EQ_INT((int)total, (int)oracle, msg);
        free(resp); resp = NULL;
    }

    /* ===== 2: AND-of-ORs, total:true, no limit on ft_ix (site 3 —
     * partial union total; reviewer measured 160 vs true 106 class).
     * Rows are exact today (rematch); total over-counts. */
    {
        const char *C2 =
            "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_2\"}]},"
            "{\"or\":[{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_0\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_2\"}]}]";
        long oracle = mode_count(tc, "ft_plain", C2);
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_ix\","
                 "\"criteria\":%s,\"total\":true}", C2);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case2 find response");
        long total = -1;
        int is_null = 0;
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "case2: parse total");
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "case2: total (%ld) == oracle count (%ld)", total, oracle);
        ASSERT_EQ_INT((int)total, (int)oracle, msg);
        free(resp); resp = NULL;
    }

    /* ===== 3: large intersect + OR on ft_big, NO limit, total:true
     * (site 1). Both eq leaves = 12000 candidates >= 10000, so
     * small_primary == 0 and the OR drops from rows AND total on base.
     * u = key index: rows outside the OR band [1000,12000) exist in
     * force (~23 000), so the red cannot silently die. */
    {
        const char *C3 =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"v0\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w0\"},"
            "{\"or\":[{\"field\":\"u\",\"op\":\"gte\",\"value\":12000},"
            "{\"field\":\"u\",\"op\":\"lt\",\"value\":1000}]}]";
        long oracle = mode_count(tc, "ft_big_plain", C3);
        int ka[30000], kb[30000];
        int na = find_keys(tc, "ft_big_plain", C3, NULL, ka, 30000);
        int nb = find_keys(tc, "ft_big", C3, "\"total\":true", kb, 30000);
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "case3: rows (%d) == oracle rows (%d)", nb, na);
        ASSERT_EQ_INT(na, nb, msg);
        if (na == nb)
            for (int i = 0; i < na; i++)
                ASSERT_EQ_INT(ka[i], kb[i], "case3: same key sets");
        /* total: parse from the twin response is already consumed; do a
         * second total:true request and compare against the oracle
         * count-mode number. */
        nb = find_keys(tc, "ft_big", C3, "\"total\":true", kb, 30000);
        (void)nb;
        free(resp); resp = NULL;
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_big\","
                 "\"criteria\":%s,\"total\":true}", C3);
        tc_request(tc, req, &resp);
        long total = -1;
        int is_null = 0;
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "case3: parse total");
        snprintf(msg, sizeof(msg),
                 "case3: total (%ld) == oracle count (%ld)", total, oracle);
        ASSERT_EQ_INT((int)total, (int)oracle, msg);
        free(resp); resp = NULL;
    }

    /* ===== 3b: prepared intersect + OR on ft_big (site 9439-class —
     * bitmap leaf b + btree leaf v): rows only; the emitted total stays
     * exact on base (rows-wrong/total-right pairing). */
    {
        const char *C3B =
            "[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"b_0\"},"
            "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"v0\"},"
            "{\"or\":[{\"field\":\"u\",\"op\":\"gte\",\"value\":12000},"
            "{\"field\":\"u\",\"op\":\"lt\",\"value\":1000}]}]";
        int ka[30000], kb[30000];
        int na = find_keys(tc, "ft_big_plain", C3B, NULL, ka, 30000);
        int nb = find_keys(tc, "ft_big", C3B, NULL, kb, 30000);
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "case3b: rows (%d) == oracle rows (%d)", nb, na);
        ASSERT_EQ_INT(na, nb, msg);
        if (na == nb)
            for (int i = 0; i < na; i++)
                ASSERT_EQ_INT(ka[i], kb[i], "case3b: same key sets");
    }

    /* ===== 4: declined-total contract — small intersect find with
     * total:true. On base: WRONG "total":0 (the caller's unconditional
     * find_total_null = 0 over an untouched slot). Post-fix: exact via
     * the fp_compute_total fallback, or null — never a wrong number. */
    {
        const char *C4 =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]";
        long oracle = mode_count(tc, "ft_plain", C4);
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_ix\","
                 "\"criteria\":%s,\"total\":true}", C4);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case4 find response");
        long total = -1;
        int is_null = 0;
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "case4: parse total");
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "case4: total exact (%ld == %ld) or null — never wrong",
                 total, oracle);
        ASSERT_TRUE(is_null || total == oracle, msg);
        free(resp); resp = NULL;
    }

    /* ===== 5: controls (GREEN before and after). */
    {
        /* pure-OR root with total: exact union. */
        const char *C5A =
            "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]}]";
        long oracle = mode_count(tc, "ft_plain", C5A);
        char req[1024];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_ix\","
                 "\"criteria\":%s,\"total\":true}", C5A);
        tc_request(tc, req, &resp);
        long total = -1;
        int is_null = 0;
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "control pure-OR: parse total");
        ASSERT_TRUE(!is_null && total == oracle,
                    "control: pure-OR total exact");
        free(resp); resp = NULL;

        /* leaf-only find with total (no OR): exact. */
        const char *C5B =
            "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149}]";
        oracle = mode_count(tc, "ft_plain", C5B);
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ft_ix\","
                 "\"criteria\":%s,\"total\":true}", C5B);
        tc_request(tc, req, &resp);
        ASSERT_EQ_INT(find_total_of(resp, &total, &is_null), 0,
                      "control leaf-only: parse total");
        ASSERT_TRUE(!is_null && total == oracle,
                    "control: leaf-only total exact");
        free(resp); resp = NULL;

        /* small intersect + OR: ROWS equal the oracle (rematch fires at
         * fixture scale today); its total is assertion 4's class — not
         * asserted here. */
        const char *C5C =
            "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
            "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
            "{\"or\":[{\"field\":\"u\",\"op\":\"eq\",\"value\":7},"
            "{\"field\":\"u\",\"op\":\"eq\",\"value\":9}]}]";
        int ka[400], kb[400];
        int na = find_keys(tc, "ft_plain", C5C, NULL, ka, 400);
        int nb = find_keys(tc, "ft_ix", C5C, NULL, kb, 400);
        ASSERT_EQ_INT(na, nb, "control: small intersect+OR rows equal");
        for (int i = 0; i < na && i < nb; i++)
            ASSERT_EQ_INT(ka[i], kb[i], "control: same key sets");
    }

    tc_close(tc);   /* test_env_stop does not free clients */
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-find-subtree-totals", test_find_subtree_totals_run)
