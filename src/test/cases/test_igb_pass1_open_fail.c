/* Regression: a btree open failure inside the indexed group-by Pass-1
 * walk is treated as an empty shard on BOTH routes (parallel worker:
 * LOG_ERROR + return NULL; serial fallback: `if (!it) continue;`), so
 * the faulted shard's entries silently vanish from the aggregate: a
 * 200-OK with present-but-undercounted groups. The block otherwise
 * fails closed (hbk_init, deadline, bitmap branches); agg_walk_worker
 * already codifies the correct template — stat first: ENOENT is a
 * legitimate empty shard (create-object-declared index files are
 * lazy-created), an open failure on an EXISTING shard fails closed.
 * Fix: adopt that template in both Pass-1 routes, normalize to EIO
 * (agg_report_error's map renders it as "aggregate storage access
 * failed"; a raw EACCES would render as the misleading "invalid typed
 * record").
 *
 * Fault: arm the btree range-iterator open-failure seam for the
 * group_by field's shard 003 via the daemon's test-control channel
 * (test_env_ctl_set_bt_range_open_fail): the NEXT btree_range_iter_open
 * on .../003.idx fails with EIO, one shot. In-process, deterministic,
 * root-safe (a chmod 000 fault is invisible to root, which bypasses
 * file modes), and needs no daemon restart — warmup caches no fd and
 * bt_cache installs no entry on a failed open, so the disarm-restore is
 * immediate. The faulted file must EXIST (asserted with stat): a
 * missing file is ENOENT = legitimate empty shard under the stat-first
 * template, where base and fix agree.
 *
 * Shapes (live-count threshold for the parallel Pass-1 route is
 * >= 100000 rows across >= 4 group-index shards; splits 64 -> 8 idx
 * shards):
 *   serial route (500 rows):
 *     base  pf_ix: baseline group_by == pf_plain (sanity) -> fault ->
 *           RED: a normal group list, undercounted (no error object)
 *     1     post-fix: pf_ix group_by -> EIO error object (pinned text)
 *     2     pf_plain group_by unchanged-correct while the fault is armed
 *     3     count(pf_ix) == 500 (control: the defect is group_by-specific)
 *     4     disarm -> pf_ix matches pf_plain again (the fault, not the
 *           data, caused it; no restart needed)
 *   parallel route (100000 rows):
 *     5     faulted group_by -> EIO error object (worker route)
 *     6     pf_plain control while the fault is armed
 *     7     disarm -> pf_ix group vector == exactly 5 labels x 20000
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h" /* igb_pass1_parallel_*_for_test (TEST_BUILD) */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define EIO_SUBSTR "aggregate storage access failed"
#define NROWS 500
#define NROWS_BIG 100000
#define NDISTINCT 5
#define FAULT_SHARD 3

typedef struct {
    char label[32];
    long count;
} GEntry;

static int gentry_cmp(const void *a, const void *b) {
    return strcmp(((const GEntry *)a)->label, ((const GEntry *)b)->label);
}

static int is_err(const char *resp) {
    return resp && SAFE_STRSTR(resp, "\"error\"") != NULL;
}

/* Extract ("v":"<label>","n":<int>) pairs from an OK group_by=v
 * response; sort by label. Returns the entry count. */
static int extract_groups(const char *resp, GEntry *out, int cap) {
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"v\":\"")) != NULL && n < cap) {
        p += strlen("\"v\":\"");
        const char *q = SAFE_STRSTR(p, "\"n\":");
        if (!q) break;
        size_t lbl = (size_t)(q - p);
        while (lbl > 0 && (p[lbl - 1] == ',' || p[lbl - 1] == '"')) lbl--;
        if (lbl >= sizeof(out[n].label)) lbl = sizeof(out[n].label) - 1;
        memcpy(out[n].label, p, lbl);
        out[n].label[lbl] = '\0';
        out[n].count = atol(q + strlen("\"n\":"));
        n++;
    }
    qsort(out, (size_t)n, sizeof(GEntry), gentry_cmp);
    return n;
}

static void group_v(TestClient *tc, const char *obj, char **resp) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"v\",\"aggregates\":[{\"fn\":\"count\","
             "\"alias\":\"n\"}]}", obj);
    tc_request(tc, req, resp);
    ASSERT_NOT_NULL(*resp, "group_by response");
}

/* Compare pf_ix's group list against pf_plain's (the oracle). */
static void assert_groups_match_oracle(TestClient *tc, const char *msg) {
    GEntry va[16], vb[16];
    int na, nb;
    char *resp = NULL;
    group_v(tc, "pf_plain", &resp);
    ASSERT_TRUE(!is_err(resp), msg);
    na = extract_groups(resp, va, 16);
    free(resp); resp = NULL;
    group_v(tc, "pf_ix", &resp);
    nb = extract_groups(resp, vb, 16);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(na, NDISTINCT, msg);
    ASSERT_EQ_INT(na, nb, msg);
    ASSERT_TRUE(na == nb, msg);
    for (int i = 0; i < na && i < nb; i++) {
        ASSERT_EQ_STR(va[i].label, vb[i].label, msg);
        ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count, msg);
    }
}

/* pf_plain-only control: 5 labels, NROWS/NDISTINCT each. Independent
 * of pf_ix's state — usable while the fault is armed. */
static void assert_pf_plain_ok(TestClient *tc, const char *msg) {
    GEntry va[16];
    char *resp = NULL;
    group_v(tc, "pf_plain", &resp);
    ASSERT_TRUE(!is_err(resp), msg);
    int na = extract_groups(resp, va, 16);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(na, NDISTINCT, msg);
    for (int i = 0; i < na; i++) {
        ASSERT_EQ_INT((int)va[i].count, NROWS / NDISTINCT, msg);
    }
}

/* Arm/disarm through the daemon's test-control channel. */
static void arm_open_fail(TestEnv *env, int shard, const char *msg) {
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail(env, shard), 0, msg);
}

static void assert_idx_shard_exists(TestEnv *env, const char *msg) {
    char path[1024];
    struct stat st;
    snprintf(path, sizeof(path), "%s/default/pf_ix/indexes/v/%03x.idx",
             env->db_root, FAULT_SHARD);
    ASSERT_EQ_INT(stat(path, &st), 0, msg);
}

static int bulk_insert_rows(TestClient *tc, const char *obj, int first,
                            int count) {
    const size_t reclen = 72;
    const int CHUNK = 10000;
    for (int base = first; base < first + count; base += CHUNK) {
        int end = base + CHUNK < first + count ? base + CHUNK : first + count;
        char *body = malloc((size_t)(end - base) * reclen + 16);
        ASSERT_NOT_NULL(body, "alloc body");
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        for (int i = base; i < end; i++) {
            off += (size_t)snprintf(body + off, reclen,
                "%s{\"key\":\"k%06d\",\"value\":{\"v\":\"cat_%d\"}}",
                i > base ? "," : "", i, i % NDISTINCT);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        char *req = malloc(off + 160);
        ASSERT_NOT_NULL(req, "alloc request");
        snprintf(req, off + 160,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"records\":%s}", obj, body);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "bulk insert accepted");
        free(resp);
        free(req);
        free(body);
    }
    return 0;
}

static int test_igb_pass1_open_fail_run(void) {
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
        "\"object\":\"pf_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"t:long\",\"u:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "pf_plain created");
    free(resp); resp = NULL;

    /* splits 64: index_splits_for(64) = 8 -> 000..007.idx exist once the
     * inserts run (the faulted 003.idx must EXIST: a missing file is a
     * legitimate ENOENT empty shard under the stat-first template). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"pf_ix\",\"splits\":64,\"max_key\":16,"
        "\"fields\":[\"v:varchar:16\",\"t:long\",\"u:int\"],"
        "\"indexes\":[\"v\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "pf_ix created");
    free(resp); resp = NULL;

    bulk_insert_rows(tc, "pf_plain", 0, NROWS);
    bulk_insert_rows(tc, "pf_ix", 0, NROWS);

    /* Baseline sanity: healthy pf_ix matches the oracle. */
    assert_groups_match_oracle(tc, "baseline: pf_ix == pf_plain");

    /* ===== Serial Pass-1 route (500 live rows < the 100000 parallel
     * threshold): the fault fires in the in-order fallback walk. ===== */
    ASSERT_EQ_INT(test_env_ctl_reset_pass1_parallel(&env), 0,
                  "reset the Pass-1 route counter");
    assert_idx_shard_exists(&env, "serial: 003.idx exists");
    arm_open_fail(&env, FAULT_SHARD, "serial: arm 003.idx open failure");

    /* 1: pf_ix group_by -> EIO error object. RED on base: a normal
     * group list with present-but-undercounted labels (shard 003's
     * entries silently skipped), no error object. */
    group_v(tc, "pf_ix", &resp);
    TAP_DIAG("# case1 base-state diag: pf_ix %s\n",
             is_err(resp) ? "ERR" : "OK (silently-short group list)");
    ASSERT_TRUE(is_err(resp), "case1: faulted group_by fails closed");
    ASSERT_CONTAINS(resp, EIO_SUBSTR, "case1: pinned EIO text");
    free(resp); resp = NULL;

    /* 2: pf_plain unchanged-correct while the fault is armed (control;
     * pf_plain-only — the faulted pf_ix comparison belongs to case1). */
    assert_pf_plain_ok(tc, "case2: pf_plain control");

    /* 3: count(pf_ix) == 500 (control: the defect is group_by-specific). */
    {
        tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"pf_ix\"}",
            &resp);
        ASSERT_NOT_NULL(resp, "case3: count response");
        ASSERT_TRUE(resp && atol(resp) == NROWS,
                    "case3: count == 500 (defect is group_by-specific)");
        free(resp); resp = NULL;
    }

    /* 4: disarm -> matches the oracle again, no restart needed
     * (control proving the fault, not the data, caused it). */
    arm_open_fail(&env, -1, "serial: disarm");
    assert_groups_match_oracle(tc, "case4: restored pf_ix == pf_plain");

    ASSERT_EQ_INT(test_env_ctl_get_pass1_parallel(&env), 0,
                  "serial case did not take the parallel Pass-1 route");

    /* ===== Parallel Pass-1 route (>= 100000 live rows): the fault fires
     * in the per-shard worker (igb_pass1_worker). ===== */
    bulk_insert_rows(tc, "pf_ix", NROWS, NROWS_BIG - NROWS);
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"pf_ix\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "parallel: count response");
    ASSERT_TRUE(resp && atol(resp) == NROWS_BIG,
                "parallel: pf_ix grown to 100000 rows");
    free(resp); resp = NULL;

    assert_idx_shard_exists(&env, "parallel: 003.idx exists");
    /* Route proof: a first UNFAULTED group_by at 100k rows must take the
     * parallel Pass-1 route (n_idx_g >= 4 && live >= 100000) — the fault
     * below targets the parallel worker, so the route must be pinned
     * first. */
    /* Route proof: the multi-field group_by (n_sec = 1 -> sec maps ->
       the Pass-1 orchestrator) at 100k live rows / 8 idx shards must
       take the PARALLEL Pass-1 branch (the fault below targets the
       parallel worker's walk). The counter lives in the daemon — read
       it through the control channel. */
    ASSERT_EQ_INT(test_env_ctl_reset_pass1_parallel(&env), 0,
                  "reset the Pass-1 route counter");
    {
        char reqs[512];
        snprintf(reqs, sizeof(reqs),
                 "{\"mode\":\"aggregate\",\"dir\":\"default\","
                 "\"object\":\"pf_ix\",\"group_by\":[\"v\",\"t\"],"
                 "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
        tc_request(tc, reqs, &resp);
        ASSERT_TRUE(!is_err(resp), "route-probe multi-field shape succeeds");
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(test_env_ctl_get_pass1_parallel(&env), 1,
                  "case5: the parallel Pass-1 route was taken");

    arm_open_fail(&env, FAULT_SHARD, "parallel: arm 003.idx open failure");

    /* 5: pf_ix group_by -> EIO error object via the parallel worker. */
    group_v(tc, "pf_ix", &resp);
    TAP_DIAG("# case5 base-state diag: pf_ix %s\n",
             is_err(resp) ? "ERR" : "OK (silently-short group list)");
    ASSERT_TRUE(is_err(resp),
                "case5: parallel-route open failure fails closed");
    ASSERT_CONTAINS(resp, EIO_SUBSTR, "case5: pinned EIO text");
    free(resp); resp = NULL;

    /* 6: pf_plain control while the fault is armed. */
    assert_pf_plain_ok(tc, "case6: pf_plain control");

    /* 7: disarm -> exact full vector (5 labels x 20000). */
    arm_open_fail(&env, -1, "parallel: disarm");
    {
        GEntry va[16];
        group_v(tc, "pf_ix", &resp);
        ASSERT_TRUE(!is_err(resp), "case7: restored group_by succeeds");
        int na = extract_groups(resp, va, 16);
        free(resp); resp = NULL;
        ASSERT_EQ_INT(na, NDISTINCT, "case7: 5 labels");
        for (int i = 0; i < na; i++) {
            ASSERT_EQ_INT((int)va[i].count, NROWS_BIG / NDISTINCT,
                          "case7: each label == 20000");
        }
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-igb-pass1-open-fail", test_igb_pass1_open_fail_run)
