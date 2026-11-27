/* Regression: three more indexed-aggregate routes treat a btree
 * range-iterator open failure as an empty shard (fail-open), silently
 * dropping entries from a 200-OK aggregate — the same defect class the
 * Pass-1 group walk had (fixed separately):
 *
 *   1. Pass-2's aggregated-field walk (sum/avg/min/max over an indexed
 *      numeric field): `if (!it) continue;` in the n_idx_a loop — that
 *      shard's values never reach their buckets → wrong sums.
 *   2. sec_map_build_worker (multi-field group_by's secondary-field
 *      map): LOG_ERROR + return NULL — the shard's secondary values are
 *      missing from the map → those records silently drop from groups.
 *   3. The limited/streamed group-by k-way merge (varchar fast path):
 *      initial open `if (!iters[s]) { has_cur[s] = 0; continue; }` and
 *      the flush-reopen do the same → present-but-short group list.
 *
 * Fix: every site adopts the Pass-1 template — stat first (ENOENT is a
 * legitimate empty shard for lazy-created index files), an open failure
 * on an EXISTING shard fails closed with EIO (agg_report_error renders
 * "aggregate storage access failed").
 *
 * Fault: test_env_ctl_set_bt_range_open_fail_nth arms a one-shot EIO on
 * an .idx open, with a skip count so the shot can target a LATER
 * route's open of the same numbered shard (Pass-1 opens the group
 * field's 003.idx before Pass-2 opens the aggregated field's).
 *
 * Fixture: afr_plain (splits 8, no indexes — the scan oracle) and
 * afr_ix (splits 64 → 8 idx shards 000..007; indexes v, t, u), 500
 * rows: v = cat_(i%5), t = i, u = i%7.
 *
 *   4  k-way FLUSH-REOPEN (vs_flush_pending's want_reopen reopens):
 *      needs the deferred-lookup cap to fire mid-walk, so this shape
 *      runs 5000 DISTINCT group values (pend cap 4096+); skip 2 lands
 *      the shot on the first reopen open (shard 0) after the two
 *      initial opens consumed skips 0-1.
 *
 * Shapes (each: baseline matches oracle -> arm -> ERR pinned text ->
 * pf control -> disarm -> oracle match):
 *   1  sum(t) group_by v        (skip 1: Pass-1 consumes the group
 *                               field's 003 open first)
 *   2  group_by v,t count(*)    (skip 1: the secondary-field map)
 *   3  group_by v count+sum(u) LIMIT 10 (skip 0: the k-way merge IS the
 *                               group-field walk)
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
#include <sys/stat.h>

#define EIO_SUBSTR "aggregate storage access failed"
#define NROWS 500
#define NDISTINCT 5
#define FAULT_SHARD 3

static int is_err(const char *resp) {
    return resp && SAFE_STRSTR(resp, "\"error\"") != NULL;
}

/* Sorted label→number pairs from a group_by response ("v":"x","n":v or
 * the extra aggregate alias "s":v). */
typedef struct {
    char label[64];
    double num;
    long count;
} GEntry;

static int gentry_cmp(const void *a, const void *b) {
    return strcmp(((const GEntry *)a)->label, ((const GEntry *)b)->label);
}

/* Extract one group entry: everything between the opening \"v\":\" and
 * the next \"n\": (or the num alias for sum-only responses) is the
 * label; quoted segments (multi-field group_by emits \"v\":..\"t\":..)
 * are joined with '/' for a deterministic combined label. */
static void copy_label(const char *p, const char *end, char *out,
                       size_t cap) {
    size_t o = 0;
    int seg = 0;
    for (; p < end && o + 2 < cap; p++) {
        if (*p == '"') { seg = 1; continue; }
        if (*p == ',' || *p == ':') continue;
        if (seg && o > 0 && out[o - 1] != '/') out[o++] = '/';
        out[o++] = *p;
        seg = 0;
    }
    while (o > 0 && out[o - 1] == '/') o--;
    out[o] = '\0';
}

static int extract_groups(const char *resp, GEntry *out, int cap,
                          const char *numalias) {
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"v\":\"")) != NULL && n < cap) {
        const char *lbl_start = p + strlen("\"v\":\"");
        char needle[24];
        snprintf(needle, sizeof(needle), "\"%s\":", numalias ? numalias : "n");
        const char *q = SAFE_STRSTR(lbl_start, needle);
        if (!q) break;
        copy_label(lbl_start, q, out[n].label, sizeof(out[n].label));
        const char *val = q + strlen(needle);
        if (numalias) out[n].num = atof(val);
        out[n].count = numalias ? -1 : atol(val);
        n++;
        p = q;
    }
    qsort(out, (size_t)n, sizeof(GEntry), gentry_cmp);
    return n;
}

static void agg_req(TestClient *tc, const char *obj, const char *gb_json,
                    const char *aggs, const char *limit, char **resp) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":%s,\"aggregates\":%s%s%s}",
             obj, gb_json, aggs,
             limit ? ",\"limit\":" : "", limit ? limit : "");
    tc_request(tc, req, resp);
    ASSERT_NOT_NULL(*resp, "aggregate response");
}

/* Compare afr_ix's group vector against afr_plain's (the oracle). */
static void assert_match_oracle(TestClient *tc, const char *gb,
                                const char *aggs, const char *numalias,
                                int nexpect, const char *msg) {
    GEntry va[64], vb[64];
    int na, nb;
    char *resp = NULL;
    agg_req(tc, "afr_plain", gb, aggs, NULL, &resp);
    ASSERT_TRUE(!is_err(resp), msg);
    na = extract_groups(resp, va, 64, numalias);
    free(resp); resp = NULL;
    agg_req(tc, "afr_ix", gb, aggs, NULL, &resp);
    nb = extract_groups(resp, vb, 64, numalias);
    free(resp); resp = NULL;
    if (nexpect >= 0) ASSERT_EQ_INT(na, nexpect, msg);
    ASSERT_EQ_INT(na, nb, msg);
    for (int i = 0; i < na && i < nb; i++) {
        ASSERT_EQ_STR(va[i].label, vb[i].label, msg);
        if (va[i].count >= 0 && vb[i].count >= 0)
            ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count, msg);
        if (numalias)
            ASSERT_EQ_INT((int)va[i].num, (int)vb[i].num, msg);
    }
}

static void arm_open_fail(TestEnv *env, int shard, int skip,
                          const char *msg) {
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail_nth(env, shard, skip),
                  0, msg);
}

static void disarm(TestEnv *env) {
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail(env, -1), 0,
                  "disarm");
}

static int test_igb_agg_open_fail_routes_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"afr_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"t:long\",\"u:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "afr_plain created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"afr_ix\",\"splits\":64,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v\",\"t\",\"u\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "afr_ix created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"afr_pend\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v\",\"u\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "afr_pend created");
    free(resp); resp = NULL;

    {
        const size_t reclen = 80;
        char *body = malloc((size_t)NROWS * reclen + 16);
        ASSERT_NOT_NULL(body, "alloc body");
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        for (int i = 0; i < NROWS; i++) {
            off += (size_t)snprintf(body + off, reclen,
                "%s{\"key\":\"k%03d\",\"value\":{\"v\":\"cat_%d\","
                "\"t\":%d,\"u\":%d}}",
                i ? "," : "", i, i % NDISTINCT, i % 4, i % 7);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        char *req = malloc(off + 160);
        ASSERT_NOT_NULL(req, "alloc request");
        for (int which = 0; which < 2; which++) {
            snprintf(req, off + 160,
                     "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"records\":%s}",
                     which ? "afr_ix" : "afr_plain", body);
            tc_request(tc, req, &resp);
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                        "bulk insert accepted");
            free(resp); resp = NULL;
        }
        free(req);
        free(body);
    }

    /* Route 4 fixture: 5000 rows with 5000 DISTINCT v values — pushes
     * the vs k-way past the deferred-lookup cap so vs_flush_pending's
     * reopen path actually runs. */
    {
        const int PN = 20000, CHUNK = 5000;
        const size_t reclen = 80;
        char *body = malloc((size_t)CHUNK * reclen + 16);
        ASSERT_NOT_NULL(body, "alloc pend body");
        for (int base = 0; base < PN; base += CHUNK) {
            int end = base + CHUNK < PN ? base + CHUNK : PN;
            size_t off = 0;
            off += (size_t)snprintf(body + off, 8, "[");
            for (int i = base; i < end; i++) {
                off += (size_t)snprintf(body + off, reclen,
                    "%s{\"key\":\"p%05d\",\"value\":{\"v\":\"cat_%d\","
                    "\"u\":%d}}", i > base ? "," : "", i, i % 8, i % 7);
            }
            off += (size_t)snprintf(body + off, 8, "]");
            char *reqp = malloc(off + 160);
            ASSERT_NOT_NULL(reqp, "alloc pend req");
            snprintf(reqp, off + 160,
                     "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                     "\"object\":\"afr_pend\",\"records\":%s}", body);
            tc_request(tc, reqp, &resp);
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                        "pend bulk insert accepted");
            free(resp); free(reqp);
        }
        free(body);
    }

    /* ===== Route 1: Pass-2 aggregated-field walk (sum(t) group_by v).
     * Pass-1 (the group field's walk) opens v/003.idx first; skip 1
     * lands the shot on t/003.idx in Pass-2. RED on base: sums silently
     * under-counted (shard 003's entries never offered to their
     * buckets), no error object. */
    assert_match_oracle(tc, "[\"v\"]",
                        "[{\"fn\":\"sum\",\"field\":\"t\",\"alias\":\"s\"}]",
                        "s", NDISTINCT, "route1 baseline: sums match oracle");
    arm_open_fail(&env, FAULT_SHARD, 1, "route1: arm t/003.idx");
    agg_req(tc, "afr_ix", "[\"v\"]",
            "[{\"fn\":\"sum\",\"field\":\"t\",\"alias\":\"s\"}]", NULL,
            &resp);
    TAP_DIAG("# route1 base-state diag: afr_ix %s\n",
             is_err(resp) ? "ERR" : "OK (silently-wrong sums)");
    ASSERT_TRUE(is_err(resp), "route1: pass-2 open failure fails closed");
    ASSERT_CONTAINS(resp, EIO_SUBSTR, "route1: pinned EIO text");
    free(resp); resp = NULL;
    disarm(&env);
    assert_match_oracle(tc, "[\"v\"]",
                        "[{\"fn\":\"sum\",\"field\":\"t\",\"alias\":\"s\"}]",
                        "s", NDISTINCT, "route1: restored sums match oracle");

    /* ===== Route 2: sec_map_build_worker (multi-field group_by v,t).
     * skip 0: the secondary-field map workers run BEFORE the Pass-1
     * group walk in the multi-field flow, so t/003.idx is the FIRST
     * matching open. RED on base: shard 003's t values missing from the
     * map → records silently drop from groups. */
    assert_match_oracle(tc, "[\"v\",\"t\"]",
                        "[{\"fn\":\"count\",\"alias\":\"n\"}]",
                        NULL, NDISTINCT * 4, "route2 baseline: group pairs match oracle");
    arm_open_fail(&env, FAULT_SHARD, 0, "route2: arm t/003.idx");
    agg_req(tc, "afr_ix", "[\"v\",\"t\"]",
            "[{\"fn\":\"count\",\"alias\":\"n\"}]", NULL, &resp);
    TAP_DIAG("# route2 base-state diag: afr_ix %s\n",
             is_err(resp) ? "ERR" : "OK (silently-short group list)");
    ASSERT_TRUE(is_err(resp), "route2: sec_map open failure fails closed");
    ASSERT_CONTAINS(resp, EIO_SUBSTR, "route2: pinned EIO text");
    free(resp); resp = NULL;
    disarm(&env);
    assert_match_oracle(tc, "[\"v\",\"t\"]",
                        "[{\"fn\":\"count\",\"alias\":\"n\"}]",
                        NULL, NDISTINCT * 4, "route2: restored pairs match oracle");

    /* ===== Route 3: limited/streamed k-way merge (varchar group fast
     * path). skip 0: the group field's own walk is the merge — v/003
     * takes the shot directly. RED on base: a 200-OK with
     * present-but-undercounted groups. */
    assert_match_oracle(tc, "[\"v\"]",
                        "[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                        "s", NDISTINCT, "route3 baseline: unlimited matches oracle");
    arm_open_fail(&env, FAULT_SHARD, 0, "route3: arm v/003.idx");
    agg_req(tc, "afr_ix", "[\"v\"]",
            "[{\"fn\":\"count\",\"alias\":\"n\"},"
            "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
            "10", &resp);
    TAP_DIAG("# route3 base-state diag: afr_ix %s\n",
             is_err(resp) ? "ERR" : "OK (silently-short group list)");
    ASSERT_TRUE(is_err(resp),
                "route3: k-way merge open failure fails closed");
    ASSERT_CONTAINS(resp, EIO_SUBSTR, "route3: pinned EIO text");
    free(resp); resp = NULL;
    disarm(&env);
    {
        /* restore: limited run on the twin still returns the oracle's
           label set (limit 10 of 5 groups: all labels, full counts) */
        GEntry va[16], vb[16];
        int na, nb;
        agg_req(tc, "afr_plain", "[\"v\"]",
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                "10", &resp);
        ASSERT_TRUE(!is_err(resp), "route3 restore: oracle ok");
        na = extract_groups(resp, va, 64, "s");
        free(resp); resp = NULL;
        agg_req(tc, "afr_ix", "[\"v\"]",
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                "10", &resp);
        nb = extract_groups(resp, vb, 64, "s");
        free(resp); resp = NULL;
        ASSERT_EQ_INT(na, nb, "route3: restored limited run matches oracle");
        for (int i = 0; i < na && i < nb; i++) {
            ASSERT_EQ_STR(va[i].label, vb[i].label,
                          "route3: restored labels equal");
            ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count,
                          "route3: restored counts equal");
        }
    }

    /* ===== Route 4: vs_flush_pending FLUSH-REOPEN (want_reopen). The
     * two initial k-way opens consume skips 0-1; skip 2 arms the shot on
     * the first reopen open (group shard 0). GREEN on the round-2 fix
     * (lc->err -> agg_error); this test pins the coverage. */
    {
        char reqp[512];
        snprintf(reqp, sizeof(reqp),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"afr_pend\",\"group_by\":\"v\","
                 "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                 "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}],"
                 "\"limit\":10}");
        agg_req(tc, "afr_pend", "\"v\"",
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                "10", &resp);
        ASSERT_TRUE(!is_err(resp), "route4 baseline ok");
        free(resp); resp = NULL;

        /* The mid-walk flush needs pend >= 4096 within the first
           `limit` emits, so the fixture uses FEW groups with MANY rows
           each (8 groups x 2500 rows): the min-group's ~2500 hashes blow
           past the cap at the second emit, while both shards keep live
           cursors. The FIRST matching /000.idx open is the initial
           k-way open; the SECOND is shard 0's flush-reopen — skip 1
           lands the shot there. */
        ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail_nth(&env, 0, 1),
                      0, "route4: arm shard-0 reopen (skip 1)");
        agg_req(tc, "afr_pend", "\"v\"",
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                "10", &resp);
        TAP_DIAG("# route4 base-state diag: %s\n",
                 is_err(resp) ? "ERR" : "OK (reopen silently skipped)");
        ASSERT_TRUE(is_err(resp),
                    "route4: flush-reopen open failure fails closed");
        ASSERT_CONTAINS(resp, EIO_SUBSTR, "route4: pinned EIO text");
        free(resp); resp = NULL;
        disarm(&env);

        agg_req(tc, "afr_pend", "\"v\"",
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"sum\",\"field\":\"u\",\"alias\":\"s\"}]",
                "10", &resp);
        ASSERT_TRUE(!is_err(resp), "route4 restored ok");
        free(resp); resp = NULL;
        (void)reqp;
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-igb-agg-open-fail-routes", test_igb_agg_open_fail_routes_run)
