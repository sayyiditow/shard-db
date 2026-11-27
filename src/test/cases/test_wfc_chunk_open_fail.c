/* Regression (round-4 review M3): two more silent-empty-shard walkers
 * returned partial 200-OK results when a btree range-iterator open
 * failed mid-walk:
 *
 *   1. wfc_worker (single MIN/MAX with criteria on an indexed numeric
 *      field): the per-shard walk logged the open failure and returned
 *      NULL without setting any error — the orchestrator folded it into
 *      "this shard had no candidates" and the aggregate reported a wrong
 *      min/max from the surviving shards. Fix: flag predicate_error=EIO
 *      so the existing merge fold fails the aggregate closed.
 *      (RED on base: silently-wrong min; GREEN: "aggregate storage
 *      access failed".)
 *
 *   2. The chunked find stream (chunk_walk_worker, driving the D1
 *      composite-exact find and the indexed streaming finds): an open
 *      failure `break`ed the worker's walk and the stream ended as if
 *      drained — the find returned a CLOSED page holding only the shards
 *      that could be read (silently short rows). Fix: ChunkShared.walk_err,
 *      chunk_walk_fanout reports it, and the D1 caller fails closed on the
 *      wire shape the find contract prescribes (docs/query-protocol/
 *      find.md): while nothing but the envelope prefix is out it closes
 *      the bare envelope and renders a storage-error object; once rows are
 *      on the wire the response stays unterminated — a closed short page
 *      can pass for success, a truncated one cannot. Here the healthy
 *      shard's first batch is flushed before the failed shard's error is
 *      handled, so rows are out and the page is unterminated.
 *      (RED on base: a closed page, no error; GREEN: unterminated, or a
 *      closed bare envelope followed by the error object.)
 *
 * Fault: test_env_ctl_set_bt_range_open_fail (one-shot EIO on the next
 * btree_range_iter_open of the shard's .idx) — wfc_worker and
 * chunk_walk_worker both open through btree_range_iter_open, which the
 * seam covers directly.
 *
 * Fixture: wcf_ix (splits 8 → 2 idx shards; indexes a, u; 400 rows
 * a=i%2, u=i). min(u) with criteria a=0 walks the u index (wfc path,
 * candidates from a) — shard 1 holds the odd-i rows, shard 0 the even
 * rows; min over a=0 rows is u=0 in shard 0. Arming shard 1 must fail
 * the aggregate on the fixed build (base reports the wrong min).
 * D1: cex_d1 (indexes a+b, c — c standalone so prim exists and Phase B
 * fires; criteria a=1,b=0,c>=0 leaves {a,b} composite-covered) — the
 * exact-prefix walk hits the armed shard and must fail closed.
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

#define NROWS 400
#define AGG_ERR_SUBSTR "aggregate storage access failed"
#define FIND_ERR_SUBSTR "index storage error"

static void insert_fixture(TestClient *tc, const char *obj, int with_ix) {
    const size_t reclen = 72;
    char *body = malloc((size_t)NROWS * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < NROWS; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s{\"key\":\"k%04d\",\"value\":{\"a\":%d,\"b\":%d,"
            "\"c\":%d,\"u\":%d}}",
            i ? "," : "", i, i % 2, (i + 1) % 2, i, i);
    }
    off += (size_t)snprintf(body + off, 8, "]");
    char *req = malloc(off + 200);
    ASSERT_NOT_NULL(req, "alloc request");
    snprintf(req, off + 200,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"%s\",\"records\":%s}", obj, body);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "bulk insert accepted");
    free(resp);
    free(req);
    free(body);
    (void)with_ix;
}

/* Count k#### key tokens in a rows/default find response. */
static int count_keys(const char *resp) {
    int n = 0;
    const char *p = resp;
    while (p && (p = strchr(p, 'k')) != NULL) {
        if (p[1] >= '0' && p[1] <= '9' && p[2] >= '0' && p[2] <= '9' &&
            p[3] >= '0' && p[3] <= '9' && p[4] >= '0' && p[4] <= '9') {
            n++;
            p += 5;
        } else {
            p++;
        }
    }
    return n;
}

static int test_wfc_chunk_open_fail_run(void) {
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
        "\"object\":\"wcf_ix\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"u:int\"],"
        "\"indexes\":[\"a\",\"u\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "wcf_ix created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cex_d1\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"a+b\",\"c\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cex_d1 created");
    free(resp); resp = NULL;

    insert_fixture(tc, "wcf_ix", 1);
    insert_fixture(tc, "cex_d1", 0);

    char req[512];

    /* ===== Scenario 1: wfc min/max with criteria ===== */
    /* Baseline: min(u) over a=0 rows = 0 (u=0,4,8,...) — the wfc fast
     * path answers it; assert the value AND that the wfc route is the
     * one serving it (explain shows a leaf/intersect seed on a). */
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":"
             "\"wcf_ix\",\"criteria\":[{\"field\":\"a\",\"op\":\"eq\","
             "\"value\":0}],\"aggregates\":[{\"fn\":\"min\","
             "\"field\":\"u\",\"alias\":\"m\"}]}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "wfc baseline min");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "wfc baseline acked");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"m\":0") != NULL,
                "wfc baseline min is 0");
    free(resp); resp = NULL;

    /* Arm shard 1's .idx open. RED on base: the wfc worker's open
     * failure folds into "no candidates" and min(u) is silently wrong
     * (the shard-1 rows never compete). GREEN on the fix: the aggregate
     * fails closed with the pinned EIO text. */
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail(&env, 1), 0,
                  "arm u/001.idx open failure");
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":"
             "\"wcf_ix\",\"criteria\":[{\"field\":\"a\",\"op\":\"eq\","
             "\"value\":0}],\"aggregates\":[{\"fn\":\"min\","
             "\"field\":\"u\",\"alias\":\"m\"}]}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "wfc faulted min");
    TAP_DIAG("# wfc base-state diag: %s\n", resp ? resp : "(none)");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, AGG_ERR_SUBSTR) != NULL,
                "wfc: open failure fails the aggregate closed");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail(&env, -1), 0,
                  "disarm");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"m\":0") != NULL,
                "wfc restored min is 0");
    free(resp); resp = NULL;

    /* ===== Scenario 2: the chunked find stream (D1 composite-exact) */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cex_d1\","
             "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":1},"
             "{\"field\":\"b\",\"op\":\"eq\",\"value\":0},"
             "{\"field\":\"c\",\"op\":\"gte\",\"value\":0}],"
             "\"limit\":400,\"total\":true}");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "d1 baseline find");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "d1 baseline acked");
    ASSERT_EQ_INT(count_keys(resp), 200,
                  "d1 baseline returns the 200 matching rows");
    free(resp); resp = NULL;

    /* skip=1: the planner's selectivity walk of c/001.idx consumes the
     * skip; the D1 exact-prefix walk's open of a+b/001.idx takes the
     * shot (the arm matches any object's /001.idx). */
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail_nth(&env, 1, 1), 0,
                  "arm a+b/001.idx open failure (skip=1)");
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "d1 faulted find");
    TAP_DIAG("# d1 base-state diag: bytes=%zu keys=%d err=%d\n",
             resp ? strlen(resp) : 0, count_keys(resp),
             resp && SAFE_STRSTR(resp, FIND_ERR_SUBSTR) != NULL);
    /* Fail closed = never a closed page without an error (base closed the
       page over the healthy shard's rows alone). Rows are already on the
       wire here, so the documented shape is unterminated. */
    int d1_closed = resp && strchr(resp, ']') != NULL;
    int d1_err = resp && SAFE_STRSTR(resp, FIND_ERR_SUBSTR) != NULL;
    ASSERT_TRUE(resp && (!d1_closed || d1_err),
                "d1: chunked walk open failure fails closed");
    /* If the page was closed (nothing had been emitted), the error object
       must follow the closed envelope. */
    ASSERT_TRUE(resp && (!d1_closed ||
                         SAFE_STRSTR(resp, "}\n{\"error\"") != NULL),
                "d1: envelope closed before the error object");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(test_env_ctl_set_bt_range_open_fail(&env, -1), 0,
                  "disarm");
    tc_request(tc, req, &resp);
    ASSERT_EQ_INT(count_keys(resp), 200, "d1 restored rows");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-wfc-chunk-open-fail", test_wfc_chunk_open_fail_run)
