/* Bitmap bits are kf slot numbers. Every keyfile rebuild -- in-place growth
   resplit (single insert, bulk window) and vacuum's tombstone compaction --
   renumbers slots, so it must renumber the bits with it. Each case compares
   count and find against a C-side oracle for every bitmap value, with the kf
   tier shrunk to 64 slots/shard so growth happens while earlier rows' bits
   already exist. Each case runs in its own daemon and tmpdir. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../../db/shard_test_ctl.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BR_N 3000

typedef struct { int flag; int kind; int live; } BrRow;
static const char *const br_kind[] = { "a", "b", "c" };

typedef struct {
    TestEnv env;
    TestClient *tc;
    BrRow rows[BR_N];
} Br;

static char *br_q(Br *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *br_q(Br *b, const char *fmt, ...) {
    char req[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(b->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}

static int br_connect(Br *b) {
    TestClientCfg cfg = { .port = b->env.port, .io_timeout_ms = 120000 };
    b->tc = tc_connect(&cfg);
    return b->tc ? 0 : -1;
}

static int br_open(Br *b, int legacy) {
    memset(b, 0, sizeof(*b));
    for (int i = 0; i < BR_N; i++) {
        b->rows[i].flag = (i % 3 == 0) || (i % 7 == 0);
        b->rows[i].kind = i % 3;
    }
    if (test_env_start(&b->env) != 0) return -1;
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0 || br_connect(b) != 0) {
        test_env_stop(&b->env);
        return -1;
    }
    char *r = br_q(b, "{\"mode\":\"add-dir\",\"dir\":\"br\"}");
    free(r);
    r = br_q(b,
        "{\"mode\":\"create-object\",%s\"dir\":\"br\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"n\"]}",
        legacy ? "\"declaration_layout\":\"legacy\"," : "");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object");
    free(r);
    return 0;
}

static void br_close(Br *b) {
    if (b->tc) tc_close(b->tc);
    b->tc = NULL;
    test_env_stop(&b->env);
}

static int br_bulk_tc(Br *b, TestClient *tc, int from, int to) {
    size_t cap = (size_t)(to - from) * 128 + 256;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"br\",\"object\":\"t\",\"records\":[");
    for (int i = from; i < to; i++) {
        n += (size_t)snprintf(req + n, cap - n,
            "%s{\"key\":\"k%05d\",\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
            i > from ? "," : "", i, b->rows[i].flag ? "true" : "false",
            br_kind[b->rows[i].kind], i);
        b->rows[i].live = 1;
    }
    snprintf(req + n, cap - n, "]}");
    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    free(req);
    int ok = rc == 0 && resp && !strstr(resp, "\"error\"");
    free(resp);
    return ok ? 0 : -1;
}

static int br_bulk(Br *b, int from, int to) { return br_bulk_tc(b, b->tc, from, to); }

static int br_single(Br *b, int i) {
    char *r = br_q(b,
        "{\"mode\":\"insert\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k%05d\","
        "\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
        i, b->rows[i].flag ? "true" : "false", br_kind[b->rows[i].kind], i);
    int ok = r && strstr(r, "\"status\":\"inserted\"");
    free(r);
    if (ok) b->rows[i].live = 1;
    return ok ? 0 : -1;
}

static int br_bulk_delete(Br *b, int step) {
    char *req = malloc((size_t)BR_N * 16 + 256);
    if (!req) return -1;
    size_t n = (size_t)sprintf(req,
        "{\"mode\":\"bulk-delete\",\"dir\":\"br\",\"object\":\"t\",\"keys\":[");
    int first = 1;
    for (int i = 0; i < BR_N; i += step) {
        if (!b->rows[i].live) continue;
        n += (size_t)sprintf(req + n, "%s\"k%05d\"", first ? "" : ",", i);
        first = 0;
        b->rows[i].live = 0;
    }
    sprintf(req + n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    int ok = rc == 0 && resp && !strstr(resp, "\"error\"");
    free(resp);
    return ok ? 0 : -1;
}

static int br_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"br\",\"object\":\"t\",\"criteria\":%s}", crit);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 && r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

static int br_find_total(TestClient *tc, const char *crit) {
    int total = 0;
    for (int off = 0;; off += 1000) {
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"br\",\"object\":\"t\",\"criteria\":%s,"
            "\"offset\":%d,\"limit\":1000,\"fields\":[\"n\"]}", crit, off);
        char *r = NULL;
        if (tc_request(tc, req, &r) != 0 || !r || strstr(r, "\"error\"")) { free(r); return -1; }
        int got = 0;
        for (const char *p = r; (p = strstr(p, "\"key\":\"")) != NULL; p += 7) got++;
        free(r);
        total += got;
        if (got < 1000) break;
    }
    return total;
}

static void br_expect(TestClient *tc, const char *when, const char *label, const char *crit, int want) {
    char desc[200];
    snprintf(desc, sizeof(desc), "%s: count %s == oracle (%d)", when, label, want);
    ASSERT_EQ_INT(br_count(tc, crit), want, desc);
    snprintf(desc, sizeof(desc), "%s: find %s == oracle (%d)", when, label, want);
    ASSERT_EQ_INT(br_find_total(tc, crit), want, desc);
}

/* Every bitmap predicate against the C-side oracle: bool, enum, and a
   bitmap AND bitmap intersect. */
static void br_check_tc(Br *b, TestClient *tc, const char *when) {
    int ft = 0, ff = 0, k[3] = { 0, 0, 0 }, both = 0;
    for (int i = 0; i < BR_N; i++) {
        if (!b->rows[i].live) continue;
        if (b->rows[i].flag) ft++; else ff++;
        k[b->rows[i].kind]++;
        if (b->rows[i].flag && b->rows[i].kind == 1) both++;
    }
    br_expect(tc, when, "flag=true",  "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]", ft);
    br_expect(tc, when, "flag=false", "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]", ff);
    br_expect(tc, when, "kind=a", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"a\"}]", k[0]);
    br_expect(tc, when, "kind=b", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]", k[1]);
    br_expect(tc, when, "kind=c", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"c\"}]", k[2]);
    br_expect(tc, when, "flag=true AND kind=b",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]",
        both);
}
static void br_check(Br *b, const char *when) { br_check_tc(b, b->tc, when); }

/* Bulk windows: growth happens inside later chunks while earlier chunks' bits exist. */
static int br_run_bulk(int legacy) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, legacy) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    br_check(b, "after growth");
    ASSERT_EQ_INT(br_bulk_delete(b, 5), 0, "bulk-delete every 5th");
    br_check(b, "after delete");
    for (int i = 0; i < 200; i++) {            /* flip kind on live rows via update */
        if (!b->rows[i].live) continue;
        b->rows[i].kind = (b->rows[i].kind + 1) % 3;
        char *r = br_q(b,
            "{\"mode\":\"update\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k%05d\","
            "\"value\":{\"kind\":\"%s\"}}", i, br_kind[b->rows[i].kind]);
        ASSERT_CONTAINS(r, "\"status\":\"updated\"", "update kind");
        free(r);
    }
    br_check(b, "after update");
    for (int i = 0; i < BR_N; i += 5) b->rows[i].live = 0;     /* re-insert the deleted rows */
    ASSERT_EQ_INT(br_bulk(b, 0, 1), 0, "re-insert k00000");
    for (int i = 5; i < BR_N; i += 5) ASSERT_EQ_INT(br_single(b, i), 0, "re-insert single");
    br_check(b, "after re-insert");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}
static int test_bitmap_resplit_bulk(void)   { return br_run_bulk(0); }
static int test_bitmap_resplit_legacy(void) { return br_run_bulk(1); }

/* Single-record inserts: kf_plan_insert_slot resplits with older rows' bits present. */
static int test_bitmap_resplit_single(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int i = 0; i < 1200; i++)
        ASSERT_EQ_INT(br_single(b, i), 0, "single insert");
    br_check(b, "after single-insert growth");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* vacuum rebuilds every kf shard with deleted > 0 at the same capacity. */
static int test_bitmap_resplit_vacuum(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_bulk_delete(b, 2), 0, "bulk-delete every 2nd");
    br_check(b, "before vacuum");
    char *r = br_q(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"vacuumed\"", "light vacuum (compacts kf)");
    free(r);
    br_check(b, "after vacuum");
    ASSERT_EQ_INT(br_bulk(b, 0, 300), 0, "insert after vacuum");
    br_check(b, "after insert following vacuum");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* The kf-tier seam is daemon process state. Startup opens every kf shard and
   extends any file smaller than the default tier, so a restarted daemon must
   get the same tier from db.env (TEST_BUILD knob KF_TEST_INITIAL_SLOTS). */
static void br_persist_seam(const char *root) {
    ASSERT_EQ_INT(tu_run_cmd("echo 'export KF_TEST_INITIAL_SLOTS=64' >> \"$(dirname '%s')/db.env\"", root),
                  0, "persist the kf tier across the restart");
}

static int br_restart(Br *b) {
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    br_persist_seam(root);
    if (test_env_start_at(&b->env, root, port) != 0) return -1;
    return br_connect(b);
}

/* Remapped bitmaps are durable: same answers after a clean restart. */
static int test_bitmap_resplit_restart(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_restart(b), 0, "restart daemon on the same root");
    br_check(b, "after restart");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Crash window: marker present + shard-0 bitmap body zeroed (stands for "keyfile
   renamed, bitmap not yet published"). Startup must rebuild and clear the marker. */
static int test_bitmap_resplit_marker_recovery(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    int port = b->env.port;
    char root[256], bm[600], marker[600];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-0 bitmap exists");
    /* bool bitmap: 32-byte header + 6-byte dict, bitmaps_off aligned to 40 */
    ASSERT_EQ_INT(tu_run_cmd("sz=$(stat -c%%s '%s'); dd if=/dev/zero of='%s' bs=1 seek=40 "
                             "count=$((sz-40)) conv=notrunc 2>/dev/null", bm, bm), 0, "zero the bitmap body");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the marker");
    br_persist_seam(root);
    ASSERT_EQ_INT(test_env_start_at(&b->env, root, port), 0, "restart with a pending marker");
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    br_check(b, "after marker recovery");
    ASSERT_TRUE(!tu_file_exists(marker), "marker removed after recovery");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Readers race resplits: no deadlock (test timeout), no error responses, exact
   answers at the end. Two readers run two-criterion counts in opposite criteria
   order: bitmap-only AND-count readers hold several bitmap read locks at once
   without the kf lock, so a resplit that took its bitmap write locks in a
   different order would deadlock against them. */
typedef struct { int port; _Atomic int stop; int errors; int reads; int reversed; } BrReader;
static void *br_reader_main(void *arg) {
    BrReader *rd = arg;
    TestClientCfg cfg = { .port = rd->port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { rd->errors++; return NULL; }
    const char *fwd = "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
                      "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]";
    const char *rev = "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"},"
                      "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]";
    while (!atomic_load(&rd->stop)) {
        int c = br_count(tc, rd->reversed ? rev : fwd);
        if (c < 0) rd->errors++;
        rd->reads++;
        usleep(100);   /* leave windows between reads: sustained reader load starves the writer-side rebuild */
    }
    tc_close(tc);
    return NULL;
}
static int test_bitmap_resplit_concurrent_readers(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    enum { NRD = 4 };
    BrReader rd[NRD];
    pthread_t th[NRD];
    for (int i = 0; i < NRD; i++) rd[i] = (BrReader){ .port = b->env.port, .reversed = i & 1 };
    for (int i = 0; i < NRD; i++)
        ASSERT_EQ_INT(pthread_create(&th[i], NULL, br_reader_main, &rd[i]), 0, "start reader");
    for (int from = 0; from < BR_N; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_bulk_delete(b, 3), 0, "bulk-delete every 3rd");
    char *r = br_q(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"vacuumed\"", "light vacuum (compacts kf)");
    free(r);
    for (int i = 0; i < NRD; i++) atomic_store(&rd[i].stop, 1);
    for (int i = 0; i < NRD; i++) pthread_join(th[i], NULL);
    for (int i = 0; i < NRD; i++) {
        ASSERT_EQ_INT(rd[i].errors, 0, "concurrent readers saw no error response");
        ASSERT_TRUE(rd[i].reads > 0, "reader made progress");
    }
    br_check(b, "after concurrent load");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- fault injection (TEST_BUILD seam, see test_env_ctl_set_bmfix_fault) ---- */

static int br_marker_exists(Br *b) {
    return tu_run_cmd("ls %s/br/t/indexes/.bmfix-* >/dev/null 2>&1", b->env.db_root) == 0;
}
static int br_remap_leftovers(Br *b) {
    return tu_run_cmd("test -n \"$(find %s/br/t/indexes -name '*.bm.remap' 2>/dev/null)\"",
                      b->env.db_root) == 0;
}
static int br_log_has(Br *b, const char *what) {
    return tu_run_cmd("grep -rq '%s' \"$(dirname '%s')\" 2>/dev/null", what, b->env.db_root) == 0;
}

/* Restart on the same root; kill=1 is a SIGKILL (unclean start: KFM2 sweep path). */
static int br_restart_kill(Br *b, int kill) {
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    if (kill) test_env_kill(&b->env); else test_env_stop_keep(&b->env);
    br_persist_seam(root);
    if (test_env_start_at(&b->env, root, port) != 0) return -1;
    return br_connect(b);
}

/* A prepare failure (ENOSPC) must abandon the resplit with the old keyfile and
   bitmaps untouched and no marker or temporary left; after the back-off the
   rebuild goes through and the answers stay exact. */

/* F2: submit a fault-window chunk without changing the expected-live oracle.
   Only a completely successful retry may publish those rows to the oracle. */
static int br_fault_stage_bulk(Br *b, int from, int to, int *inserted, int *errors) {
    size_t cap = (size_t)(to - from) * 128 + 256;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"br\",\"object\":\"t\",\"records\":[");
    for (int i = from; i < to; i++)
        n += (size_t)snprintf(req + n, cap - n,
            "%s{\"key\":\"k%05d\",\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
            i > from ? "," : "", i, b->rows[i].flag ? "true" : "false",
            br_kind[b->rows[i].kind], i);
    snprintf(req + n, cap - n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    if (rc != 0 || !resp) { free(resp); return -1; }
    const char *ins = strstr(resp, "\"inserted\":");
    const char *err = strstr(resp, "\"errors\":");
    *inserted = ins ? (int)strtol(ins + strlen("\"inserted\":"), NULL, 10) : -1;
    *errors = err ? (int)strtol(err + strlen("\"errors\":"), NULL, 10) : 0;
    int failed = strstr(resp, "\"error\"") != NULL || *errors > 0;
    free(resp);
    if (*inserted < 0) return -1;
    return failed ? 1 : 0;
}

static int br_fault_stage_log(Br *b, const char *what) {
    for (int i = 0; i < 500; i++) {
        if (br_log_has(b, what)) return 1;
        usleep(10000);
    }
    return 0;
}

static int br_fault_stage_retry(Br *b, int from, int to) {
    for (int attempt = 0; attempt < 200; attempt++) {
        int inserted = -1, errors = -1;
        int rc = br_fault_stage_bulk(b, from, to, &inserted, &errors);
        if (rc < 0) return -1;
        if (rc == 0) {
            for (int i = from; i < to; i++) b->rows[i].live = 1;
            return 0;
        }
        /* Cooldowns are per shard. A retry may commit recovered shards;
           retrying every key is safe because bulk-insert is an upsert. */
        usleep(100000);
    }
    return -1;
}

static int test_bitmap_resplit_fault_prepare(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    ASSERT_EQ_INT(br_bulk(b, 0, 220), 0, "seed records before the fault");
    br_check(b, "before failed prepares");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_PREPARE, 1000), 0, "arm: fail every bitmap prepare");
    int inserted = -1, errors = -1;
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "fault-blocked growth rejects the new stages");
    ASSERT_EQ_INT(inserted, 0, "no record committed from rejected stages");
    ASSERT_EQ_INT(errors, 512, "every submitted record was rejected");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 220, "seeded records unchanged after rejection");
    br_check(b, "after failed prepares");
    ASSERT_TRUE(br_fault_stage_log(b, "BULK_STAGE_UNGROWN"), "growth failure reported by stage");
    ASSERT_TRUE(br_fault_stage_log(b, "KF_BMFIX_PREPARE_FAILED"), "a resplit attempt failed and logged");
    ASSERT_TRUE(!br_marker_exists(b), "no marker after abandoned prepare");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after abandoned prepare");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    sleep(6);                                   /* the 5 s retry back-off */
    ASSERT_EQ_INT(br_fault_stage_retry(b, 220, 732), 0, "retry every rejected key succeeds");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 732, "all rejected keys present after retry");
    br_check(b, "after retrying rejected keys");
    for (int from = 732; from < 1500; from += 170) {
        int to = from + 170 < 1500 ? from + 170 : 1500;
        ASSERT_EQ_INT(br_bulk(b, from, to), 0, "later growth succeeds");
    }
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 1500, "final record count is exact");
    br_check(b, "after the retry");
    ASSERT_TRUE(!br_marker_exists(b), "marker removed after successful rebuild");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after successful rebuild");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* The keyfile rename fails after the bitmaps were prepared and the marker
   written: the abort path must remove the marker (it is ours) and the
   temporaries, and nothing may change. */
static int test_bitmap_resplit_fault_rename(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    ASSERT_EQ_INT(br_bulk(b, 0, 220), 0, "seed records before the fault");
    br_check(b, "before failed renames");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_RENAME, 1000), 0, "arm: fail every keyfile rename");
    int inserted = -1, errors = -1;
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "fault-blocked growth rejects the new stages");
    ASSERT_EQ_INT(inserted, 0, "no record committed from rejected stages");
    ASSERT_EQ_INT(errors, 512, "every submitted record was rejected");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 220, "seeded records unchanged after rejection");
    br_check(b, "after failed renames");
    ASSERT_TRUE(br_fault_stage_log(b, "BULK_STAGE_UNGROWN"), "growth failure reported by stage");
    ASSERT_TRUE(!br_marker_exists(b), "no marker after abandoned rename");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after abandoned rename");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    sleep(6);                                   /* a failed rename also delays the next attempt by 5 s */
    ASSERT_EQ_INT(br_fault_stage_retry(b, 220, 732), 0, "retry every rejected key succeeds");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 732, "all rejected keys present after retry");
    br_check(b, "after retrying rejected keys");
    for (int from = 732; from < 1500; from += 170) {
        int to = from + 170 < 1500 ? from + 170 : 1500;
        ASSERT_EQ_INT(br_bulk(b, from, to), 0, "later growth succeeds");
    }
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 1500, "final record count is exact");
    br_check(b, "after the retry");
    ASSERT_TRUE(!br_marker_exists(b), "marker removed after successful rebuild");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after successful rebuild");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* An unconfirmed bitmap msync keeps the marker; a later successful rebuild of
   the same shard must not remove it; startup (clean or after SIGKILL) then
   rebuilds the indexes and clears it. */
static int br_run_fault_commit(int kill) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_COMMIT_SYNC, 1000), 0, "arm: commit msync unconfirmed");
    for (int from = 0; from < 1000; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    br_check(b, "in-memory bitmaps are correct despite the unconfirmed msync");
    ASSERT_TRUE(br_marker_exists(b), "marker retained after an unconfirmed commit");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    for (int from = 1000; from < 3000; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk (later rebuilds succeed)");
    br_check(b, "after later successful rebuilds");
    ASSERT_TRUE(br_marker_exists(b), "an earlier marker is not removed by a later rebuild");
    ASSERT_EQ_INT(br_restart_kill(b, kill), 0, "restart");
    br_check(b, "after startup recovery");
    ASSERT_TRUE(!br_marker_exists(b), "startup recovery cleared the marker");
    ASSERT_TRUE(!br_remap_leftovers(b), "no temporaries left");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}
static int test_bitmap_resplit_fault_commit_clean(void)   { return br_run_fault_commit(0); }
static int test_bitmap_resplit_fault_commit_unclean(void) { return br_run_fault_commit(1); }

/* Concurrent bulk-inserts into one object: requests queued on a shard's writer
   gate merge into one commit chain, and a joiner's pre-grow must size the
   table for the whole chain (planning inside a chain may not resplit). Four
   clients insert disjoint ranges at once; every answer must stay exact. */
typedef struct { Br *b; int port; int from; int to; int rc; } BrChainArg;
static void *br_chain_main(void *arg) {
    BrChainArg *a = arg;
    TestClientCfg cfg = { .port = a->port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { a->rc = -1; return NULL; }
    for (int from = a->from; from < a->to; from += 150)
        if (br_bulk_tc(a->b, tc, from, from + 150) != 0) a->rc = -1;
    tc_close(tc);
    return NULL;
}
static int test_bitmap_resplit_chain(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    enum { NCL = 4 };
    BrChainArg arg[NCL];
    pthread_t th[NCL];
    for (int i = 0; i < NCL; i++) {
        arg[i] = (BrChainArg){ .b = b, .port = b->env.port, .from = i * 750, .to = (i + 1) * 750, .rc = 0 };
        ASSERT_EQ_INT(pthread_create(&th[i], NULL, br_chain_main, &arg[i]), 0, "start client");
    }
    for (int i = 0; i < NCL; i++) {
        pthread_join(th[i], NULL);
        ASSERT_EQ_INT(arg[i].rc, 0, "concurrent bulk-insert succeeded");
    }
    br_check(b, "after concurrent bulk inserts");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A bitmap whose header does not fit its file is already unreadable; it must
   not stop a keyfile rebuild. The rebuild skips it, keeps the marker, and the
   next start repairs it by reindexing. Driven through vacuum, which rebuilds
   every shard that has deletions without needing to write the damaged bitmap. */
static int test_bitmap_resplit_corrupt_bitmap(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    ASSERT_EQ_INT(br_bulk(b, 0, 300), 0, "load before the damage");
    ASSERT_EQ_INT(br_bulk_delete(b, 2), 0, "delete every 2nd row (shards now have tombstones)");
    char root[256], bm[600];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-0 bitmap exists");
    int port = b->env.port;
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "cut the bitmap body off");
    br_persist_seam(root);
    ASSERT_EQ_INT(test_env_start_at(&b->env, root, port), 0, "restart on the damaged root");
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    char *r = br_q(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"vacuumed\"", "vacuum compacts the keyfiles despite the damaged bitmap");
    free(r);
    ASSERT_TRUE(br_log_has(b, "KF_BMFIX_SKIP"), "the damaged bitmap was skipped and logged");
    ASSERT_TRUE(br_marker_exists(b), "marker kept for the damaged bitmap");
    ASSERT_EQ_INT(br_restart_kill(b, 0), 0, "restart");
    br_check(b, "after startup repair");
    ASSERT_TRUE(!br_marker_exists(b), "startup repair cleared the marker");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-resplit-bulk", test_bitmap_resplit_bulk)
TEST_REGISTER("test-bitmap-resplit-legacy", test_bitmap_resplit_legacy)
TEST_REGISTER("test-bitmap-resplit-single", test_bitmap_resplit_single)
TEST_REGISTER("test-bitmap-resplit-vacuum", test_bitmap_resplit_vacuum)
TEST_REGISTER("test-bitmap-resplit-restart", test_bitmap_resplit_restart)
TEST_REGISTER("test-bitmap-resplit-marker-recovery", test_bitmap_resplit_marker_recovery)
TEST_REGISTER("test-bitmap-resplit-concurrent-readers", test_bitmap_resplit_concurrent_readers)
TEST_REGISTER("test-bitmap-resplit-fault-prepare", test_bitmap_resplit_fault_prepare)
TEST_REGISTER("test-bitmap-resplit-fault-rename", test_bitmap_resplit_fault_rename)
TEST_REGISTER("test-bitmap-resplit-fault-commit-clean", test_bitmap_resplit_fault_commit_clean)
TEST_REGISTER("test-bitmap-resplit-fault-commit-unclean", test_bitmap_resplit_fault_commit_unclean)
TEST_REGISTER("test-bitmap-resplit-chain", test_bitmap_resplit_chain)
TEST_REGISTER("test-bitmap-resplit-corrupt-bitmap", test_bitmap_resplit_corrupt_bitmap)
