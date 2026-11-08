/* F2: a bulk stage whose pre-grow cannot size the kf table must fail the
   whole shard stage atomically (BULK_STAGE_UNGROWN, every record error,
   retryable), instead of letting publish fail records one by one with a
   faceless partial success. Two ways coverage can be lost, both driven by
   existing TEST_BUILD seams: the kf tier seam pins the tier (growth cap),
   and the bitmap-remap prepare fault puts growth on its 5 s back-off.
   Each case runs in its own daemon and tmpdir. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../../db/shard_test_ctl.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BSU_N 1500

typedef struct {
    TestEnv env;
    TestClient *tc;
} Bsu;

static char *bsu_q(Bsu *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *bsu_q(Bsu *b, const char *fmt, ...) {
    char req[16384];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(b->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}

static int bsu_connect(Bsu *b) {
    TestClientCfg cfg = { .port = b->env.port, .io_timeout_ms = 120000 };
    b->tc = tc_connect(&cfg);
    return b->tc ? 0 : -1;
}

static void bsu_close(Bsu *b) {
    if (b->tc) tc_close(b->tc);
    b->tc = NULL;
    test_env_stop(&b->env);
}

static int bsu_open(Bsu *b, size_t kf_initial, size_t kf_max) {
    memset(b, 0, sizeof(*b));
    if (test_env_start(&b->env) != 0) return -1;
    /* The case sends all records as ONE request (~90 KB): raise the
       request-size cap (default MAX_LINE = 64 KB) and restart once. */
    {
        int port = b->env.port;
        char root[256];
        snprintf(root, sizeof(root), "%s", b->env.db_root);
        test_env_stop_keep(&b->env);
        ASSERT_EQ_INT(tu_run_cmd("echo 'export MAX_REQUEST_SIZE=8388608' >> \"$(dirname '%s')/db.env\"", root), 0,
                      "raise the request-size cap");
        ASSERT_EQ_INT(test_env_start_at(&b->env, root, port), 0, "restart with the raised cap");
    }
    if (test_env_ctl_set_kf_seams(&b->env, kf_initial, kf_max) != 0 || bsu_connect(b) != 0) {
        test_env_stop(&b->env);
        return -1;
    }
    char *r = bsu_q(b, "{\"mode\":\"add-dir\",\"dir\":\"bsu\"}");
    free(r);
    /* The object needs a bitmap index: the remap-prepare fault fires only
       when the resplit has bitmap work to do (kf_bmfix_prepare returns 0
       early for objects with no indexes directory). */
    r = bsu_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"bsu\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"n:int\"],\"indexes\":[\"flag:bitmap\",\"n\"]}");
    int ok = r && strstr(r, "\"status\":\"created\"");
    free(r);
    return ok ? 0 : -1;
}

/* One bulk-insert of every key; returns parsed "inserted" and "errors". */
static int bsu_bulk_all(Bsu *b, int *out_inserted, int *out_errors) {
    size_t cap = (size_t)BSU_N * 64 + 4096;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bsu\",\"object\":\"t\",\"records\":[");
    for (int i = 0; i < BSU_N; i++)
        n += (size_t)snprintf(req + n, cap - n,
            "%s{\"key\":\"k%05d\",\"value\":{\"flag\":%s,\"n\":%d}}",
            i ? "," : "", i, i % 2 ? "false" : "true", i);
    snprintf(req + n, cap - n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    if (rc != 0 || !resp) { free(resp); return -1; }
    *out_inserted = -1;
    *out_errors = -1;
    const char *ins = strstr(resp, "\"inserted\":");
    const char *err = strstr(resp, "\"errors\":");
    if (ins) *out_inserted = (int)strtol(ins + strlen("\"inserted\":"), NULL, 10);
    if (err) *out_errors = (int)strtol(err + strlen("\"errors\":"), NULL, 10);
    int had_error_field = strstr(resp, "\"error\"") != NULL;
    free(resp);
    return (had_error_field || (*out_errors > 0)) ? 1 : 0;   /* 1 = some records failed */
}

static int bsu_count_crit(Bsu *b, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"bsu\",\"object\":\"t\",\"criteria\":%s}", crit);
    char *r = NULL;
    int rc = tc_request(b->tc, req, &r);
    int n = rc == 0 && r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

static int bsu_log_has(Bsu *b, const char *what) {
    /* LOG_DIR is a sibling of db_root. Logging drains asynchronously;
       poll only that private log directory with a bounded wait. */
    for (int i = 0; i < 500; i++) {
        if (tu_run_cmd("grep -rq '%s' '%s/../logs' 2>/dev/null", what, b->env.db_root) == 0)
            return 1;
        usleep(10000);
    }
    return 0;
}

/* Case 1: the TEST_BUILD kf tier seam pins capacity at 64; the stage pre-grow
   cannot size the table for 1500 records over 8 shards (~188/shard). After
   the fix the stage fails atomically: inserted == 0, BULK_STAGE_UNGROWN
   logged. Raising the seam afterwards lets the retry succeed fully. */
static int test_bulk_stage_ungrown_seam(void) {
    Bsu *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    ASSERT_EQ_INT(bsu_open(b, 64, 64), 0, "open with kf tier pinned to 64 slots");
    int ins = -1, errs = -1;
    ASSERT_EQ_INT(bsu_bulk_all(b, &ins, &errs), 1, "bulk-insert reports failures");
    ASSERT_EQ_INT(ins, 0, "the whole stage failed atomically: nothing inserted");
    ASSERT_TRUE(bsu_log_has(b, "BULK_STAGE_UNGROWN"), "BULK_STAGE_UNGROWN logged");
    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&b->env, 64, 0), 0, "raise the tier cap");
    ASSERT_EQ_INT(bsu_bulk_all(b, &ins, &errs), 0, "retry with an uncapped tier succeeds");
    ASSERT_EQ_INT(ins, BSU_N, "all records inserted on retry");
    ASSERT_EQ_INT(bsu_count_crit(b, "[]"), BSU_N, "count == oracle after retry");
    ASSERT_EQ_INT(bsu_count_crit(b, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]"), BSU_N / 2,
                  "bitmap count == oracle after retry");
    bsu_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Case 2: production-reachable path. The bitmap-remap prepare fault makes
   every growth resplit fail and puts the shard on its 5 s growth back-off,
   so the stage pre-grow cannot size the table. Same fail-closed contract;
   then disarm, wait out the back-off, and the retry succeeds. */
static int test_bulk_stage_ungrown_backoff(void) {
    Bsu *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    ASSERT_EQ_INT(bsu_open(b, 64, 0), 0, "open with a small kf tier");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_PREPARE, 100000), 0,
                  "arm: fail every bitmap prepare");
    int ins = -1, errs = -1;
    ASSERT_EQ_INT(bsu_bulk_all(b, &ins, &errs), 1, "bulk-insert reports failures");
    ASSERT_EQ_INT(ins, 0, "the whole stage failed atomically: nothing inserted");
    ASSERT_TRUE(bsu_log_has(b, "BULK_STAGE_UNGROWN"), "BULK_STAGE_UNGROWN logged");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    int retry = -1;
    for (int attempt = 0; attempt < 200; attempt++) {
        retry = bsu_bulk_all(b, &ins, &errs);
        if (retry <= 0) break;   /* success or transport/parse failure */
        /* Different shards can recover on different retry attempts. The
           stage guarantee is per shard; retry all keys via upsert. */
        ASSERT_TRUE(ins >= 0 && ins <= BSU_N, "retry insertion count is bounded");
        usleep(100000);
    }
    ASSERT_EQ_INT(retry, 0, "retry after the back-off succeeds within bounded wait");
    ASSERT_EQ_INT(bsu_count_crit(b, "[]"), BSU_N, "count == oracle after retry");
    ASSERT_EQ_INT(bsu_count_crit(b, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]"), BSU_N / 2,
                  "bitmap count == oracle after retry");
    bsu_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Deletes need no new slots. With 1500 requested keys and 64-slot tiers,
   insertion coverage is impossible, but deleting the one present key must
   succeed and missing keys remain silently skipped. This case is green on
   base and catches an accidentally unconditional coverage rejection. */
static int test_bulk_stage_delete_ungrown(void) {
    Bsu *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (bsu_open(b, 64, 64) != 0) {
        ASSERT_TRUE(0, "open delete regression fixture");
        bsu_close(b); free(b); return 1;
    }
    char *r = bsu_q(b,
        "{\"mode\":\"insert\",\"dir\":\"bsu\",\"object\":\"t\",\"key\":\"k00000\","
        "\"value\":{\"flag\":true,\"n\":0}}");
    ASSERT_NOT_NULL(r, "seed delete fixture");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "seed succeeded");
    free(r);
    ASSERT_EQ_INT(bsu_count_crit(b, "[]"), 1, "one record before delete");
    size_t cap = (size_t)BSU_N * 16 + 256;
    char *q = malloc(cap);
    ASSERT_NOT_NULL(q, "allocate delete request");
    if (q) {
        size_t n = (size_t)snprintf(q, cap,
            "{\"mode\":\"bulk-delete\",\"dir\":\"bsu\",\"object\":\"t\",\"keys\":[");
        for (int i = 0; i < BSU_N; i++)
            n += (size_t)snprintf(q + n, cap - n, "%s\"k%05d\"", i ? "," : "", i);
        snprintf(q + n, cap - n, "]}");
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(b->tc, q, &resp), 0, "bulk delete transport");
        ASSERT_NOT_NULL(resp, "bulk delete response");
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "delete ignores insertion under-coverage");
        const char *d = resp ? strstr(resp, "\"deleted\":") : NULL;
        ASSERT_TRUE(d && strtol(d + strlen("\"deleted\":"), NULL, 10) == 1,
                    "delete removed the one present key");
        free(resp); free(q);
    }
    ASSERT_EQ_INT(bsu_count_crit(b, "[]"), 0, "empty after delete");
    ASSERT_EQ_INT(bsu_count_crit(b, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]"), 0,
                  "bitmap empty after delete");
    bsu_close(b); free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-stage-ungrown-seam", test_bulk_stage_ungrown_seam)
TEST_REGISTER("test-bulk-stage-ungrown-backoff", test_bulk_stage_ungrown_backoff)
TEST_REGISTER("test-bulk-stage-delete-ungrown", test_bulk_stage_delete_ungrown)
