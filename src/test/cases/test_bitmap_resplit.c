/* Bitmap bits are kf slot numbers. Every keyfile rebuild -- in-place growth
   resplit (single insert, bulk window) and vacuum's tombstone compaction --
   renumbers slots, so it must renumber the bits with it. Each case compares
   count and find against a C-side oracle for every bitmap value, with the kf
   tier shrunk to 64 slots/shard so growth happens while earlier rows' bits
   already exist. Each case runs in its own daemon and tmpdir. */
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../../db/shard_test_ctl.h"
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define BR_N 3000
/* F7d seam: shortened growth back-off for the two fault cases (production
   default 5000 ms; the daemon-side seam is inert unless armed here).
   Margin rules: the waited bounds below are measured from t_req0 (the
   daemon stamps the back-off inside the faulted request, so t_fail would
   double-count that request's latency); the upper bound 4700 must stay
   below the production 5000 so unmodified code still fails the upper
   assertion; the held-window guard 2000 leaves >= 1500 ms of runway for
   the held 512-record request. */
/* The seam and the window margins scale under sanitizer builds: TSan/ASan
   inflate per-request wall time 10-40x on loaded CI runners, which eats the
   fixed 1200 ms margin the release bounds rely on. Release builds keep the
   strict bounds — the 4700 ms ceiling must stay below the production 5000 ms
   back-off so an unmodified back-off still fails the assertion. Under
   sanitizers the seam is raised ABOVE production instead: an ignored-seam
   daemon retries at ~production+latency and fails the waited >= seam bound,
   so every negative case stays detected. */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define BR_BACKOFF_MS    9000   /* seam: above the production 5000 ms */
#define BR_WINDOW_MS    30000   /* hang guard, absorbs sanitizer latency */
#define BR_REQ_MARGIN    9000   /* one faulted bulk request + checks */
#define BR_SEQ_MARGIN   11000   /* two sequential requests + checks */
#else
#define BR_BACKOFF_MS    3500
#define BR_WINDOW_MS     4700   /* < production 5000: unmodified code must fail */
#define BR_REQ_MARGIN    1000
#define BR_SEQ_MARGIN    2000
#endif

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
        n = tappendf(req, cap, n,
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

static long br_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

typedef enum { BR_LOG_SERVING, BR_LOG_RESTART, BR_LOG_REFUSE } BrLogScope;
static const char *const br_log_scope_rel[] = { "logs", "daemon-restart.log", "refuse.log" };
/* Single-shot grep of ONE log source (F7e). SERVING = <base>/logs (async
   writer; only lines logged while the daemon serves), RESTART = the fd-2
   capture of the latest direct start (startup recovery lines are written
   synchronously to stderr before the log writer exists), REFUSE = the raw
   spawn's stdout/stderr. grep -F: the needle is data, not a regex.
   The needle MUST NOT contain a single quote — it is interpolated into a
   single-quoted shell word, and a needle carrying one returns 0 (treated
   as not-found), never a partial match. */
static int br_log_grep(Br *b, BrLogScope sc, const char *what) {
    if (strchr(what, '\'')) return 0;   /* a needle with a single quote would break the shell */
    return tu_run_cmd("grep -rFq -- '%s' \"$(dirname '%s')/%s\" 2>/dev/null",
                      what, b->env.db_root, br_log_scope_rel[sc]) == 0;
}
/* Positive assertion helper: serving-time lines drain through the async
   writer, so that scope polls (bounded 5 s); startup scopes are synchronous. */
static int br_log_has(Br *b, BrLogScope sc, const char *what) {
    int tries = sc == BR_LOG_SERVING ? 500 : 1;
    for (int i = 0; i < tries; i++) {
        if (br_log_grep(b, sc, what)) return 1;
        if (i + 1 < tries) usleep(10000);
    }
    return 0;
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
        n = tappendf(req, cap, n,
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
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_backoff(&b->env, BR_BACKOFF_MS), 0, "shorten the growth back-off (F7d seam)");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_PREPARE, 1000), 0, "arm: fail every bitmap prepare");
    long t_req0 = br_now_ms();
    int inserted = -1, errors = -1;
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "fault-blocked growth rejects the new stages");
    ASSERT_EQ_INT(inserted, 0, "no record committed from rejected stages");
    ASSERT_EQ_INT(errors, 512, "every submitted record was rejected");
    long t_fail = br_now_ms();
    ASSERT_TRUE(t_fail - t_req0 < BR_REQ_MARGIN, "the faulted request left most of the back-off window");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    long t_disarm = br_now_ms();
    ASSERT_TRUE(t_disarm - t_req0 < BR_SEQ_MARGIN,
                "test is still inside the back-off window at the held-back-off check");
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "back-off still holds growth after the fault is disarmed");
    ASSERT_EQ_INT(inserted, 0, "back-off rejection committed nothing");
    ASSERT_EQ_INT(errors, 512, "back-off rejected every submitted record");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 220, "seeded records unchanged after rejection");
    br_check(b, "after failed prepares");
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "BULK_STAGE_UNGROWN"), "growth failure reported by stage");
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "KF_BMFIX_PREPARE_FAILED"), "a resplit attempt failed and logged");
    ASSERT_TRUE(!br_marker_exists(b), "no marker after abandoned prepare");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after abandoned prepare");
    ASSERT_EQ_INT(br_fault_stage_retry(b, 220, 732), 0, "retry every rejected key succeeds");
    /* waited is measured from t_req0: the daemon stamps the back-off inside
       the faulted request, so from t_fail the bound would double-count that
       request's latency. Lower: the last stamp is >= t_req0, so correct code
       always clears BR_BACKOFF_MS. Upper 4700 < production 5000: unmodified
       code (>= 5000 from t_req0) must still fail this assertion. */
    long waited = br_now_ms() - t_req0;
    ASSERT_TRUE(waited >= BR_BACKOFF_MS, "growth stayed held for the seam interval");
    ASSERT_TRUE(waited < BR_WINDOW_MS, "retry succeeded within the shortened back-off window");
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
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_backoff(&b->env, BR_BACKOFF_MS), 0, "shorten the growth back-off (F7d seam)");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_RENAME, 1000), 0, "arm: fail every keyfile rename");
    long t_req0 = br_now_ms();
    int inserted = -1, errors = -1;
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "fault-blocked growth rejects the new stages");
    ASSERT_EQ_INT(inserted, 0, "no record committed from rejected stages");
    ASSERT_EQ_INT(errors, 512, "every submitted record was rejected");
    long t_fail = br_now_ms();
    ASSERT_TRUE(t_fail - t_req0 < BR_REQ_MARGIN, "the faulted request left most of the back-off window");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    long t_disarm = br_now_ms();
    ASSERT_TRUE(t_disarm - t_req0 < BR_SEQ_MARGIN,
                "test is still inside the back-off window at the held-back-off check");
    ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                  "back-off still holds growth after the fault is disarmed");
    ASSERT_EQ_INT(inserted, 0, "back-off rejection committed nothing");
    ASSERT_EQ_INT(errors, 512, "back-off rejected every submitted record");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 220, "seeded records unchanged after rejection");
    br_check(b, "after failed renames");
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "BULK_STAGE_UNGROWN"), "growth failure reported by stage");
    ASSERT_TRUE(!br_marker_exists(b), "no marker after abandoned rename");
    ASSERT_TRUE(!br_remap_leftovers(b), "no remap temporaries after abandoned rename");
    ASSERT_EQ_INT(br_fault_stage_retry(b, 220, 732), 0, "retry every rejected key succeeds");
    /* waited is measured from t_req0 (see fault_prepare); bounds follow the
       same margin rules. */
    long waited = br_now_ms() - t_req0;
    ASSERT_TRUE(waited >= BR_BACKOFF_MS, "growth stayed held for the seam interval");
    ASSERT_TRUE(waited < BR_WINDOW_MS, "retry succeeded within the shortened back-off window");
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

/* Oracle smoke test under concurrent bulk load (F7c residual): four clients
   insert disjoint ranges at once while requests queue on shards' writer gates;
   every answer must stay exact. This case does NOT force a chain merge and
   carries no chain/pre-grow assertion — the deterministic merge + chain-aware
   pre-grow proof is test-bulk-chain-pregrow (test_bulk_chain_pregrow.c). */
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
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "KF_BMFIX_SKIP"), "the damaged bitmap was skipped and logged");
    ASSERT_TRUE(br_marker_exists(b), "marker kept for the damaged bitmap");
    ASSERT_EQ_INT(br_restart_kill(b, 0), 0, "restart");
    br_check(b, "after startup repair");
    ASSERT_TRUE(!br_marker_exists(b), "startup repair cleared the marker");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ==== Plan E: shard-scoped startup bitmap repair + escape (F5 + F7a) ====
   A pending .bmfix-NNN marker names the one kf shard whose bitmaps may use
   old slot numbers; startup repairs just that shard's bitmaps. When the
   repair fails (and no KFM2 marker is pending), startup writes
   .bmfix-disabled and serves the object read-only-with-bitmaps-off until
   `reindex` heals it. All startup lines here are LOG_WARN/LOG_ERROR (the
   fixture's db.env uses LOG_LEVEL=2). */

#define BR_REFUSAL_JSON \
    "{\"error\":\"object writes disabled: bitmap repair failed at startup; run reindex\"}"
#define BR_REFUSAL_TEXT \
    "Error: object writes disabled: bitmap repair failed at startup; run reindex"
#define BR_MISSING_GET "{\"error\":\"Not found\"}"

/* db.env seam line for direct starts: br_persist_seam appends
   unconditionally, so direct `test_env_start_at`/raw spawns call this to
   add the line only when an earlier start of the same root hasn't already. */
static void br_seam_once(const char *root) {
    ASSERT_EQ_INT(tu_run_cmd("grep -q KF_TEST_INITIAL_SLOTS \"$(dirname '%s')/db.env\" || "
                             "echo 'export KF_TEST_INITIAL_SLOTS=64' >> \"$(dirname '%s')/db.env\"",
                             root, root), 0, "ensure kf tier seam in db.env");
}
static void br_env_append(const char *root, const char *line) {
    ASSERT_EQ_INT(tu_run_cmd("echo '%s' >> \"$(dirname '%s')/db.env\"", line, root),
                  0, "append db.env knob");
}
static void br_env_remove_line(const char *root, const char *pattern) {
    ASSERT_EQ_INT(tu_run_cmd("sed -i '/%s/d' \"$(dirname '%s')/db.env\"", pattern, root),
                  0, "remove db.env knob");
}

/* A `.bmfix-*` entry other than `.bmfix-disabled` — every "marker" assertion
   in the Plan E cases uses this, never br_marker_exists (which also matches
   the flag). */
static int br_repair_marker_exists(Br *b) {
    return tu_run_cmd("test -n \"$(find %s/br/t/indexes -maxdepth 1 -name '.bmfix-*' "
                      "! -name '.bmfix-disabled' 2>/dev/null)\"", b->env.db_root) == 0;
}
static int br_flag_exists(Br *b) {
    char p[600];
    snprintf(p, sizeof(p), "%s/br/t/indexes/.bmfix-disabled", b->env.db_root);
    return tu_file_exists(p);
}
static int br_kfm2_marker_count(Br *b) {
    char *out = tu_capture_cmd("find %s/br/t/data/kf -name '*_marker.dat' 2>/dev/null | wc -l",
                               b->env.db_root);
    int n = out ? atoi(out) : -1;
    free(out);
    return n;
}

static void br_trim(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r')) s[--n] = '\0';
}

/* Capture the (restart) daemon's stderr around a direct spawn: the startup
   repair logs WARN/ERROR synchronously to stderr (the async disk writer is
   not running yet), and test_env_start_at's child inherits the caller's
   stderr, so dup fd 2 onto a file under <base> for the spawn window — the
   same mechanism test_bitmap_cache_lock_errors.c uses for its startup
   checkpoint. stderr is unbuffered, so the lines are on disk as soon as
   the start returns, and br_log_has's RESTART scope (a grep of
   <base>/daemon-restart.log) finds them. */
static int br_stderr_capture(const char *root) {
    fflush(stderr);
    int saved = dup(2);
    if (saved < 0) return -1;
    char base[600];
    snprintf(base, sizeof(base), "%s", root);
    char *slash = strrchr(base, '/');
    if (slash) *slash = '\0';
    char p[700];
    snprintf(p, sizeof(p), "%s/daemon-restart.log", base);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { close(saved); return -1; }
    if (dup2(fd, 2) < 0) { close(fd); close(saved); return -1; }
    close(fd);
    return saved;
}
static void br_stderr_restore(int saved) {
    if (saved < 0) return;
    fflush(stderr);
    dup2(saved, 2);
    close(saved);
}

/* br_open with `kind` dual-declared (bitmap + btree, P0.6): lets the escape
   case prove the btree plan survives while bitmap planning is disabled. */
static int br_open_dual(Br *b) {
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
        "{\"mode\":\"create-object\",\"dir\":\"br\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"kind:btree\",\"n\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object (kind dual-declared)");
    free(r);
    return 0;
}

/* Seeded object for the Plan E cases: rows k00000..k01499 in 250-row chunks. */
static void br_seed_1500(Br *b) {
    for (int from = 0; from < 1500; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    br_check(b, "after seed");
}

/* Inode+ctime snapshot of every bitmap and btree index file, keyed by
   "<field>/<name>" relative to indexes/. Taken after test_env_stop_keep and
   before any planted change; "unchanged" across a restart means the restart
   did not rewrite the file. ctime guards against inode reuse on non-tmpfs.
   2048 entries cover an 8-shard object as well as the 512-shard case
   (2 bitmap fields x 512 shards = 1024 files). */
#define BR_INO_MAX 2048
typedef struct { char rel[24]; ino_t ino; struct timespec ctim; } BrInoEnt;
typedef struct { BrInoEnt e[BR_INO_MAX]; int n; } BrInoSnap;

/* ctime as a timespec: Darwin names the member st_ctimespec. */
static struct timespec br_st_ctim(const struct stat *st) {
#if defined(__APPLE__)
    return st->st_ctimespec;
#else
    return st->st_ctim;
#endif
}
static void br_ino_scan(BrInoSnap *s, const char *dir, const char *relprefix) {
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != NULL && s->n < BR_INO_MAX) {
        char p[600];
        snprintf(p, sizeof(p), "%s/%s", dir, de->d_name);
        struct stat st;
        if (stat(p, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        BrInoEnt *e = &s->e[s->n++];
        snprintf(e->rel, sizeof(e->rel), "%s/%s", relprefix, de->d_name);
        e->ino = st.st_ino;
        e->ctim = br_st_ctim(&st);
    }
    closedir(d);
}
static void br_inode_snapshot(BrInoSnap *s, const char *root) {
    memset(s, 0, sizeof(*s));
    char p[600];
    snprintf(p, sizeof(p), "%s/br/t/indexes/flag", root); br_ino_scan(s, p, "flag");
    snprintf(p, sizeof(p), "%s/br/t/indexes/kind", root); br_ino_scan(s, p, "kind");
    snprintf(p, sizeof(p), "%s/br/t/indexes/n", root);    br_ino_scan(s, p, "n");
}
/* 1 = present in both snapshots with identical inode and ctime. */
static int br_inode_same(const BrInoSnap *a, const BrInoSnap *b, const char *rel) {
    const BrInoEnt *ea = NULL, *eb = NULL;
    for (int i = 0; i < a->n; i++)
        if (strcmp(a->e[i].rel, rel) == 0) { ea = &a->e[i]; break; }
    for (int i = 0; i < b->n; i++)
        if (strcmp(b->e[i].rel, rel) == 0) { eb = &b->e[i]; break; }
    if (!ea || !eb) return 0;
    return ea->ino == eb->ino && ea->ctim.tv_sec == eb->ctim.tv_sec &&
           ea->ctim.tv_nsec == eb->ctim.tv_nsec;
}

/* Local raw spawn (P0.8 pattern): fork the daemon against the stopped root
   with a 60 s budget. Returns its exit status, or -1 on timeout after
   SIGKILL+reap — so "exited with failure" is distinguishable from "timed
   out". The child chdirs to <base> (dirname of root), reads the same db.env,
   and writes stdout+stderr to <base>/refuse.log. Never br_connect after an
   expected-failure start. */
static int br_spawn_expect_exit(const char *root) {
    const char *cands[] = { "./build/bin/shard-db-test-server",
                            "./build/bin/shard-db",
                            "./shard-db", NULL };
    char bin[PATH_MAX];
    int found = 0;
    for (int i = 0; cands[i]; i++) {
        if (access(cands[i], X_OK) == 0) {
            if (!realpath(cands[i], bin)) continue;
            found = 1;
            break;
        }
    }
    ASSERT_TRUE(found, "locate daemon binary for raw spawn");
    if (!found) return -1;
    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0, "fork raw daemon");
    if (pid > 0) {
        int status = -1;
        for (int elapsed = 0; elapsed < 60000; elapsed += 100) {
            pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid) return status;
            usleep(100000);
        }
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;                       /* timeout — never a real exit status */
    }
    if (pid < 0) return -1;
    char base[600];
    snprintf(base, sizeof(base), "%s", root);
    char *slash = strrchr(base, '/');
    if (slash) *slash = '\0';
    if (chdir(base) != 0) _exit(126);
    char logp[700];
    snprintf(logp, sizeof(logp), "%s/refuse.log", base);
    int lfd = open(logp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (lfd >= 0) { dup2(lfd, 1); dup2(lfd, 2); close(lfd); }
    execl(bin, bin, "server", (char *)NULL);
    _exit(127);
}

static char *br_explain(Br *b, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"br\",\"object\":\"t\",\"criteria\":%s,\"explain\":true}",
        crit);
    return br_q(b, "%s", req);
}

static void br_expect_refusal(Br *b, const char *req, const char *what) {
    char *r = br_q(b, "%s", req);
    ASSERT_NOT_NULL(r, what);
    if (r) {
        br_trim(r);
        ASSERT_TRUE(strcmp(r, BR_REFUSAL_JSON) == 0, what);
    }
    free(r);
}

/* Text fast-path record mutation: exact \x1F line format the text dispatcher
   parses (cmd\x1Fdir\x1Fobj\x1Fkey\x1Fvalue). The \x1F escapes are followed
   by explicit string concatenation — C hex escapes would greedily swallow
   the following hex-digit characters ('b' of "br"!) into the escape. */
static void br_text_insert_expect_refusal(Br *b, const char *key) {
    char line[512];
    snprintf(line, sizeof(line),
             "insert" "\x1F" "br" "\x1F" "t" "\x1F" "%s" "\x1F"
             "{\"flag\":true,\"kind\":\"a\",\"n\":2002}", key);
    char *r = NULL;
    int rc = tc_request(b->tc, line, &r);
    ASSERT_EQ_INT(rc, 0, "text fast-path request answered");
    if (r) {
        br_trim(r);
        char msg[640];
        snprintf(msg, sizeof(msg), "text fast-path refusal (got: %.300s)", r);
        ASSERT_TRUE(strcmp(r, BR_REFUSAL_TEXT) == 0, msg);
    }
    free(r);
}

static int test_bmfix_repair_shard_scoped(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-0 bitmap exists");
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-0 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-0 marker");
    /* Snapshot AFTER planting the damage (same discipline as
       test-bmfix-repair-shard-300): "changed" then means exactly "replaced
       by the repair", never "ctime moved by my own truncate". */
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_repair_marker_exists(b), "marker gone");
    br_check(b, "after shard-scoped repair");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    char rel[64];
    snprintf(rel, sizeof(rel), "flag/%03d.bm", 0);
    ASSERT_TRUE(!br_inode_same(&before, &after, rel), "flag/000.bm inode changed");
    snprintf(rel, sizeof(rel), "kind/%03d.bm", 0);
    ASSERT_TRUE(!br_inode_same(&before, &after, rel), "kind/000.bm inode changed");
    for (int s = 1; s < 8; s++) {
        snprintf(rel, sizeof(rel), "flag/%03d.bm", s);
        ASSERT_TRUE(br_inode_same(&before, &after, rel), "other flag .bm unchanged");
        snprintf(rel, sizeof(rel), "kind/%03d.bm", s);
        ASSERT_TRUE(br_inode_same(&before, &after, rel), "other kind .bm unchanged");
    }
    for (int i = 0; i < before.n; i++) {
        if (strncmp(before.e[i].rel, "n/", 2) != 0) continue;
        ASSERT_TRUE(br_inode_same(&before, &after, before.e[i].rel), "n/ inode unchanged");
    }
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap rebuild of 1 shard"), "repair scoped to 1 shard");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_repair_bad_marker_name(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    char marker[600];
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-zz", root);
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the malformed marker");
    br_seam_once(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_repair_marker_exists(b), "marker gone");
    br_check(b, "after full-bitmap repair");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    char rel[64];
    for (int s = 0; s < 8; s++) {
        snprintf(rel, sizeof(rel), "flag/%03d.bm", s);
        ASSERT_TRUE(!br_inode_same(&before, &after, rel), "flag .bm inode changed");
        snprintf(rel, sizeof(rel), "kind/%03d.bm", s);
        ASSERT_TRUE(!br_inode_same(&before, &after, rel), "kind .bm inode changed");
    }
    for (int i = 0; i < before.n; i++) {
        if (strncmp(before.e[i].rel, "n/", 2) != 0) continue;
        ASSERT_TRUE(br_inode_same(&before, &after, before.e[i].rel), "n/ inode unchanged");
    }
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Escape case shared prologue: seeded dual-declared object, corrupt shard-0
   bitmaps (both bitmap fields — the marker names the shard, and the repair
   covers every bitmap field on it) + planted marker + armed build-failure
   knob, first (repair-failing) start. The caller continues with either the
   escape assertions (3) or the heal-at-startup flow (4). Returns 0 when the
   daemon started. */
static int br_escape_start(Br *b, char *root, size_t rootcap, int *port) {
    int rc = br_open_dual(b);
    if (rc != 0) return -1;
    br_seed_1500(b);
    *port = b->env.port;
    snprintf(root, rootcap, "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-0 bitmap");
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/kind/000.bm", root);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt kind shard-0 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-0 marker");
    br_env_append(root, "export BMFIX_REPAIR_TEST_FAIL_NEXT=5");
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, *port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) return -2;             /* caller exits via br_close */
    if (br_connect(b) != 0) return -2;
    return 0;
}

static int test_bmfix_escape_disabled(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    char root[256];
    int port = 0;
    int st = br_escape_start(b, root, sizeof(root), &port);
    if (st != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_TRUE(br_flag_exists(b), "disabled flag written");
    ASSERT_TRUE(br_repair_marker_exists(b), "markers kept");
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap planning DISABLED"), "escape logged");
    br_check(b, "reads exact with bitmap planning disabled");
    char *r = br_explain(b, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]");
    ASSERT_NOT_NULL(r, "explain flag eq true");
    ASSERT_TRUE(r && !strstr(r, "\"index\":\"bitmap\""), "flag explain avoids bitmap");
    free(r);
    r = br_explain(b, "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]");
    ASSERT_NOT_NULL(r, "explain kind eq b");
    ASSERT_TRUE(r && strstr(r, "\"index\":\"btree\""), "dual-declared kind explains btree");
    free(r);
    ASSERT_EQ_INT(br_count(b->tc, "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]"),
                  500, "kind eq b count exact via btree while flagged");

    br_expect_refusal(b, "{\"mode\":\"insert\",\"dir\":\"br\",\"object\":\"t\","
                         "\"key\":\"k02000\",\"value\":{\"flag\":true,\"kind\":\"a\",\"n\":2000}}",
                      "insert refused");
    br_expect_refusal(b, "{\"mode\":\"update\",\"dir\":\"br\",\"object\":\"t\","
                         "\"key\":\"k00001\",\"value\":{\"n\":99999}}",
                      "update refused");
    br_expect_refusal(b, "{\"mode\":\"delete\",\"dir\":\"br\",\"object\":\"t\","
                         "\"key\":\"k00002\"}",
                      "delete refused");
    br_expect_refusal(b, "{\"mode\":\"bulk-insert\",\"dir\":\"br\",\"object\":\"t\","
                         "\"records\":[{\"key\":\"k02001\","
                         "\"value\":{\"flag\":true,\"kind\":\"a\",\"n\":2001}}]}",
                      "bulk-insert refused");
    br_expect_refusal(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}",
                      "vacuum refused");
    br_text_insert_expect_refusal(b, "k02002");

    ASSERT_EQ_INT(br_count(b->tc, "[]"), 1500, "count unchanged after refusals");
    char *g = br_q(b, "{\"mode\":\"get\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k02000\"}");
    ASSERT_NOT_NULL(g, "get k02000");
    if (g) { br_trim(g); ASSERT_TRUE(strcmp(g, BR_MISSING_GET) == 0, "k02000 missing"); }
    free(g);
    g = br_q(b, "{\"mode\":\"get\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k02002\"}");
    ASSERT_NOT_NULL(g, "get k02002");
    if (g) { br_trim(g); ASSERT_TRUE(strcmp(g, BR_MISSING_GET) == 0, "k02002 missing"); }
    free(g);
    g = br_q(b, "{\"mode\":\"get\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k00001\"}");
    ASSERT_NOT_NULL(g, "get k00001");
    ASSERT_TRUE(g && strstr(g, "\"n\":1"), "k00001 keeps its old n");
    free(g);
    g = br_q(b, "{\"mode\":\"get\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k00002\"}");
    ASSERT_NOT_NULL(g, "get k00002");
    ASSERT_TRUE(g && strstr(g, "\"n\":2"), "k00002 present");
    free(g);

    ASSERT_EQ_INT(br_count(b->tc, "[{\"field\":\"n\",\"op\":\"gte\",\"value\":0}]"),
                  1500, "n gte 0 count exact");
    r = br_explain(b, "[{\"field\":\"n\",\"op\":\"gte\",\"value\":0}]");
    ASSERT_TRUE(r && strstr(r, "\"index\":\"btree\""), "n gte 0 explains btree");
    free(r);

    r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\",\"composites_only\":true}");
    ASSERT_CONTAINS(r, "\"status\":\"reindexed\"", "composites-only reindex runs");
    free(r);
    ASSERT_TRUE(br_flag_exists(b), "composites-only keeps the flag");
    ASSERT_TRUE(br_repair_marker_exists(b), "composites-only keeps the marker");

    r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"reindexed\"", "full reindex succeeds");
    free(r);
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "bitmap planning re-enabled"),
                "re-enabled logged");
    ASSERT_TRUE(!br_flag_exists(b), "flag cleared by reindex");
    ASSERT_TRUE(!br_repair_marker_exists(b), "markers cleared by reindex");
    ASSERT_EQ_INT(br_bulk(b, 2000, 2001), 0, "insert works after heal");
    br_check(b, "after heal");
    r = br_explain(b, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]");
    ASSERT_TRUE(r && strstr(r, "\"index\":\"bitmap\""), "bitmap plans return after heal");
    free(r);
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Pin (green before and after the fix): backup stays allowed on an object
   the bitmap-escape refusal flags as disabled — an operator wants a backup
   BEFORE reindex. Load-bearing by mutation: adding
   backup to mode_is_schema without the two gate exemptions turns both
   backup assertions red. */
static int test_bmfix_backup_allowed_when_disabled(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc"); if(!b) return 1;
    char root[256]; int port = 0;
    int st = br_escape_start(b, root, sizeof(root), &port);
    if (st != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_TRUE(br_flag_exists(b), "object is flagged disabled");            /* non-vacuity */
    br_expect_refusal(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}", "vacuum still refused while flagged");
    char *r = br_q(b, "{\"mode\":\"backup\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"backed_up\"", "JSON backup allowed on a flagged object"); free(r);
    r = br_q(b, "%s", "backup" "\x1F" "br" "\x1F" "t");   /* text path; concatenation after each \x1F */
    ASSERT_CONTAINS(r, "\"status\":\"backed_up\"", "text backup allowed on a flagged object"); free(r);
    ASSERT_TRUE(br_flag_exists(b), "flag untouched by backup");
    br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_escape_heals_at_startup(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    char root[256];
    int port = 0;
    int st = br_escape_start(b, root, sizeof(root), &port);
    if (st != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_TRUE(br_flag_exists(b), "disabled flag written");
    ASSERT_TRUE(br_repair_marker_exists(b), "markers kept");
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap planning DISABLED"), "escape logged");
    br_check(b, "reads exact with bitmap planning disabled");
    /* No mutations, no reindex: the next start retries the repair itself. */
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    br_env_remove_line(root, "BMFIX_REPAIR_TEST_FAIL_NEXT");
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK (heal)");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_flag_exists(b), "flag gone after heal at startup");
    ASSERT_TRUE(!br_repair_marker_exists(b), "markers gone after heal at startup");
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap planning re-enabled"), "re-enabled logged");
    ASSERT_EQ_INT(br_bulk(b, 2000, 2001), 0, "insert works after heal");
    br_check(b, "after heal at startup");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_escape_flag_write_fails(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600], flagdir[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    snprintf(flagdir, sizeof(flagdir), "%s/br/t/indexes/.bmfix-disabled", root);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-0 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-0 marker");
    br_env_append(root, "export BMFIX_REPAIR_TEST_FAIL_NEXT=5");
    ASSERT_EQ_INT(tu_run_cmd("mkdir '%s'", flagdir), 0, "flag path is a directory");
    br_seam_once(root);
    int status = br_spawn_expect_exit(root);
    ASSERT_TRUE(status >= 0, "daemon exited (not timeout)");
    ASSERT_TRUE(status != 0, "exit status nonzero");
    ASSERT_TRUE(br_log_has(b, BR_LOG_REFUSE, "could not write the disabled flag"),
                "flag-write failure logged");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_flag_without_markers(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    char flag[600];
    snprintf(flag, sizeof(flag), "%s/br/t/indexes/.bmfix-disabled", root);
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", flag), 0, "plant the stale flag (no markers)");
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_flag_exists(b), "flag gone");
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "stale disabled flag removed"), "stale flag removal logged");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    char rel[64];
    for (int s = 0; s < 8; s++) {
        snprintf(rel, sizeof(rel), "flag/%03d.bm", s);
        ASSERT_TRUE(br_inode_same(&before, &after, rel), "flag .bm inode unchanged");
        snprintf(rel, sizeof(rel), "kind/%03d.bm", s);
        ASSERT_TRUE(br_inode_same(&before, &after, rel), "kind .bm inode unchanged");
    }
    for (int i = 0; i < before.n; i++) {
        if (strncmp(before.e[i].rel, "n/", 2) != 0) continue;
        ASSERT_TRUE(br_inode_same(&before, &after, before.e[i].rel), "n/ inode unchanged");
    }
    ASSERT_EQ_INT(br_bulk(b, 1500, 1550), 0, "insert works");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_reindex_clears_serving_markers(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_COMMIT_SYNC, 100000),
                  0, "arm: commit msync unconfirmed");
    ASSERT_EQ_INT(br_bulk(b, 1500, 1750), 0, "fault-window chunk still commits");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    ASSERT_TRUE(br_repair_marker_exists(b), "serving-time marker retained");
    char *r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"reindexed\"", "full reindex succeeds");
    free(r);
    ASSERT_TRUE(!br_repair_marker_exists(b), "reindex cleared the serving-time marker");
    br_check(b, "after reindex");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* F7a: unclean start with BOTH a KFM2 window marker and a .bmfix marker —
   startup resolves both (repair before replay) and counts stay exact. The
   first half doubles as the P1b seam probe: the K-phase fault is armed
   sticky and one single-row bulk-insert must answer within the client
   timeout and leave its durable marker behind. */
static int test_bmfix_unclean_with_kfm2(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    /* Probe step 1: commit-sync fault leaves a durable .bmfix marker; the
       window itself commits and clears its own KFM2 marker (see P1b
       evidence). */
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_COMMIT_SYNC, 100000),
                  0, "arm: commit msync unconfirmed");
    ASSERT_EQ_INT(br_bulk(b, 1500, 1750), 0, "fault-window chunk still commits");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    ASSERT_TRUE(br_repair_marker_exists(b), "bmfix marker retained");
    /* Probe step 2 (P1b): K-phase fault sticky, single-row bulk answers
       within the client timeout. Its return value and counters are recorded
       as evidence only (the pending window rejects the request with a
       durability-pending error, so br_fault_stage_bulk reports -1/-1). */
    ASSERT_EQ_INT(test_env_ctl_set_fail_phase(&b->env, SHARD_TEST_PHASE_K, 1, 1),
                  0, "arm: fail phase K occ 1 sticky");
    int inserted = -1, errors = -1;
    int rc = br_fault_stage_bulk(b, 1750, 1751, &inserted, &errors);
    char lbl[160];
    snprintf(lbl, sizeof(lbl),
             "disarm fail phase (probe evidence: bulk rc=%d inserted=%d errors=%d)",
             rc, inserted, errors);
    ASSERT_EQ_INT(test_env_ctl_set_fail_phase(&b->env, -1, 0, 0), 0, lbl);
    b->rows[1750].live = 1;              /* the killed window's row, replayed at startup */
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_kill(&b->env);
    int km = br_kfm2_marker_count(b);
    snprintf(lbl, sizeof(lbl), "exactly one KFM2 marker pending after kill (count=%d)", km);
    ASSERT_TRUE(km == 1, lbl);
    ASSERT_TRUE(br_repair_marker_exists(b), "bmfix marker still present after kill");
    br_seam_once(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_EQ_INT(br_kfm2_marker_count(b), 0, "KFM2 markers replayed and cleared");
    ASSERT_TRUE(!br_repair_marker_exists(b), "bmfix markers cleared");
    br_check(b, "after unclean start resolved both");
    ASSERT_EQ_INT(br_count(b->tc, "[]"), 1751, "count is exact after replay");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bmfix_unclean_repair_fails_with_kfm2(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    br_seed_1500(b);
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_COMMIT_SYNC, 100000),
                  0, "arm: commit msync unconfirmed");
    ASSERT_EQ_INT(br_bulk(b, 1500, 1750), 0, "fault-window chunk still commits");
    ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");
    ASSERT_TRUE(br_repair_marker_exists(b), "bmfix marker retained");
    ASSERT_EQ_INT(test_env_ctl_set_fail_phase(&b->env, SHARD_TEST_PHASE_K, 1, 1),
                  0, "arm: fail phase K occ 1 sticky");
    int inserted = -1, errors = -1;
    (void)br_fault_stage_bulk(b, 1750, 1751, &inserted, &errors);
    ASSERT_EQ_INT(test_env_ctl_set_fail_phase(&b->env, -1, 0, 0), 0, "disarm fail phase");
    b->rows[1750].live = 1;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_kill(&b->env);
    br_env_append(root, "export BMFIX_REPAIR_TEST_FAIL_NEXT=5");
    br_seam_once(root);
    int status = br_spawn_expect_exit(root);
    ASSERT_TRUE(status >= 0, "daemon exited (not timeout)");
    ASSERT_TRUE(status != 0, "exit status nonzero");
    ASSERT_TRUE(br_log_has(b, BR_LOG_REFUSE, "with pending commit markers; startup refused"),
                "pending-KFM2 refusal logged");
    ASSERT_TRUE(!br_flag_exists(b), "no disabled flag written");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Carried Plan E review item: the one-shot fail-next knob used to be shared
   between reindex_object_checked_impl (checked reindex builds) and
   bitmap_rebuild_shards_durable (the bmfix startup repair) — whichever ran
   first ate it. The knob is split: INDEX_TEST_FAIL_NEXT_BUILD stays
   reindex-only (Part A keeps the old name — that is the point), and
   BMFIX_REPAIR_TEST_FAIL_NEXT is repair-only (Part B). Independence is
   pinned both ways. Base state (repair still consumes the shared knob; the
   new knob parsed but inert): failing set = {A2, A3, A5, B4, B5}. */
static int test_bmfix_test_knobs_independent(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;

    /* ---- Part A: the build knob does not hit the repair path ---- */
    if (br_open(b, 0) != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    br_seed_1500(b);
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-0 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-0 marker");
    br_env_append(root, "export INDEX_TEST_FAIL_NEXT_BUILD=5");
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");                                        /* A1 */
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    int rc_conn = br_connect(b);
    ASSERT_EQ_INT(rc_conn, 0, "reconnect");
    if (rc_conn != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_TRUE(!br_flag_exists(b), "repair ran and succeeded: no disabled flag");  /* A2 */
    ASSERT_TRUE(!br_repair_marker_exists(b), "marker cleared by the repair");       /* A3 */
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap rebuild of 1 shard"),
                "repair ran");                                                     /* A4 */
    br_check(b, "after repair");
    char *r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\":\"reindex failed\"") != NULL,
                "the build knob was still armed: the repair did not consume it");   /* A5 */
    free(r);
    r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"reindexed\"") != NULL,
                "one-shot: consumed by the reindex");                              /* A6 */
    free(r);

    /* ---- Part B: the repair knob does not hit reindex; it does hit the repair ---- */
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    ASSERT_EQ_INT(tu_run_cmd("sed -i '/INDEX_TEST_FAIL_NEXT_BUILD/d' \"$(dirname '%s')/db.env\"", root),
                  0, "remove the build-knob line from db.env");
    br_env_append(root, "export BMFIX_REPAIR_TEST_FAIL_NEXT=5");
    br_seam_once(root);
    saved_err = br_stderr_capture(root);
    started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK (repair-knob restart)");                  /* B1 */
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    rc_conn = br_connect(b);
    ASSERT_EQ_INT(rc_conn, 0, "reconnect");
    if (rc_conn != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    r = br_q(b, "{\"mode\":\"reindex\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_TRUE(r && strstr(r, "\"status\":\"reindexed\"") != NULL,
                "reindex ignores the repair knob");                            /* B2 */
    free(r);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "re-corrupt flag shard-0 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "re-plant the shard-0 marker");
    br_seam_once(root);
    saved_err = br_stderr_capture(root);
    started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK (armed repair)");                         /* B3 */
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    rc_conn = br_connect(b);
    ASSERT_EQ_INT(rc_conn, 0, "reconnect");
    if (rc_conn != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_TRUE(br_flag_exists(b),
                "repair honours the repair knob: escape flag written");        /* B4 */
    ASSERT_TRUE(br_log_has(b, BR_LOG_RESTART, "bitmap planning DISABLED"),
                "escape logged");                                              /* B5 */
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* F7e — br_log_grep must consult exactly ONE log source per scope, never the
   whole test base directory: a whole-base grep can pass on db.env, on an
   earlier run's log, or on the wrong phase's output. Planted canaries with
   positive controls and 9 negative cross-checks pin the isolation. On the
   Phase 1 shim (whole-base grep) the 5 positives pass and all 9 negatives
   fail. */
static int test_bitmap_resplit_log_scope(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    ASSERT_EQ_INT(tu_run_cmd("echo 'CANARY-SERVING' >> \"$(dirname '%s')/logs/zz-canary.log\"", root),
                  0, "plant serving canary");
    ASSERT_EQ_INT(tu_run_cmd("echo 'CANARY-RESTART' >> \"$(dirname '%s')/daemon-restart.log\"", root),
                  0, "plant restart canary");
    ASSERT_EQ_INT(tu_run_cmd("echo 'CANARY-REFUSE' >> \"$(dirname '%s')/refuse.log\"", root),
                  0, "plant refuse canary");
    /* Positive controls (3, raw br_log_grep). */
    ASSERT_TRUE(br_log_grep(b, BR_LOG_SERVING, "CANARY-SERVING"),
                "serving scope finds its own token");
    ASSERT_TRUE(br_log_grep(b, BR_LOG_RESTART, "CANARY-RESTART"),
                "restart scope finds its own token");
    ASSERT_TRUE(br_log_grep(b, BR_LOG_REFUSE, "CANARY-REFUSE"),
                "refuse scope finds its own token");
    ASSERT_TRUE(br_log_has(b, BR_LOG_SERVING, "CANARY-SERVING"),
                "polled serving scope finds its token");
    /* Positive control for the db.env token. */
    ASSERT_TRUE(tu_run_cmd("grep -q 'LOG_LEVEL=2' \"$(dirname '%s')/db.env\"", root) == 0,
                "db.env carries the LOG_LEVEL=2 token");
    /* Negative cross-checks (9): each scope against the other two scopes'
       tokens and the db.env-only token. */
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_SERVING, "CANARY-RESTART"),
                "serving scope must not see CANARY-RESTART");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_SERVING, "CANARY-REFUSE"),
                "serving scope must not see CANARY-REFUSE");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_SERVING, "LOG_LEVEL=2"),
                "serving scope must not see LOG_LEVEL=2");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_RESTART, "CANARY-SERVING"),
                "restart scope must not see CANARY-SERVING");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_RESTART, "CANARY-REFUSE"),
                "restart scope must not see CANARY-REFUSE");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_RESTART, "LOG_LEVEL=2"),
                "restart scope must not see LOG_LEVEL=2");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_REFUSE, "CANARY-SERVING"),
                "refuse scope must not see CANARY-SERVING");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_REFUSE, "CANARY-RESTART"),
                "refuse scope must not see CANARY-RESTART");
    ASSERT_TRUE(!br_log_grep(b, BR_LOG_REFUSE, "LOG_LEVEL=2"),
                "refuse scope must not see LOG_LEVEL=2");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Shard ids beyond one byte: a 512-shard object whose marker names shard
   300 (hex 12c) must repair exactly that shard. The packed selection array
   in build_bitmap_pass must hold full shard ids, not truncated bytes. */
static int test_bmfix_repair_shard_300(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    memset(b, 0, sizeof(*b));
    for (int i = 0; i < BR_N; i++) {
        b->rows[i].flag = (i % 3 == 0) || (i % 7 == 0);
        b->rows[i].kind = i % 3;
    }
    if (test_env_start(&b->env) != 0) { free(b); return 1; }
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0 || br_connect(b) != 0) {
        test_env_stop(&b->env);
        free(b);
        return 1;
    }
    char *r = br_q(b, "{\"mode\":\"add-dir\",\"dir\":\"br\"}");
    free(r);
    r = br_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"br\",\"object\":\"t\",\"splits\":512,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"n\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object (splits 512)");
    free(r);
    for (int from = 0; from < 500; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    br_check(b, "after seed");
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/12c.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-12c", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-300 bitmap exists");
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-300 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-300 marker");
    /* Snapshot AFTER planting the damage: "changed" then means exactly
       "replaced by the repair", never "ctime moved by my own truncate". */
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_repair_marker_exists(b), "marker gone");
    br_check(b, "after shard-300 repair");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    ASSERT_TRUE(!br_inode_same(&before, &after, "flag/12c.bm"), "flag/12c.bm inode changed");
    ASSERT_TRUE(!br_inode_same(&before, &after, "kind/12c.bm"), "kind/12c.bm inode changed");
    for (int i = 0; i < before.n; i++) {
        if (strcmp(before.e[i].rel, "flag/12c.bm") == 0 ||
            strcmp(before.e[i].rel, "kind/12c.bm") == 0)
            continue;
        ASSERT_TRUE(br_inode_same(&before, &after, before.e[i].rel),
                    "no other shard's index file changed");
    }
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* F11 pin (n == 0 branch): a plain no-marker restart sweeps every leftover
 * "<NNN>.bm.remap" temporary out of every field directory, while a
 * non-matching name survives, and the restart touches no other index file. */
static int test_bmfix_remap_sweep(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    memset(b, 0, sizeof(*b));
    for (int i = 0; i < BR_N; i++) {
        b->rows[i].flag = (i % 3 == 0) || (i % 7 == 0);
        b->rows[i].kind = i % 3;
    }
    if (test_env_start(&b->env) != 0) { free(b); return 1; }
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0 || br_connect(b) != 0) {
        test_env_stop(&b->env);
        free(b);
        return 1;
    }
    char *r = br_q(b, "{\"mode\":\"add-dir\",\"dir\":\"br\"}");
    free(r);
    r = br_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"br\",\"object\":\"t\",\"splits\":512,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"n\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object (splits 512)");
    free(r);
    for (int from = 0; from < 500; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    br_check(b, "after seed");
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char rmA[600], rmB[600], rmC[600], keepf[600];
    snprintf(rmA, sizeof(rmA),   "%s/br/t/indexes/flag/12c.bm.remap", root);   /* bitmap field, shard 300 */
    snprintf(rmB, sizeof(rmB),   "%s/br/t/indexes/kind/1ff.bm.remap", root);   /* bitmap field, shard 511 */
    snprintf(rmC, sizeof(rmC),   "%s/br/t/indexes/n/12c.bm.remap", root);      /* btree field dir: the sweep visits every field dir */
    snprintf(keepf, sizeof(keepf), "%s/br/t/indexes/flag/12c.bm.remap.keep", root);   /* suffix control: must survive */
    ASSERT_EQ_INT(tu_run_cmd(": > '%s' && : > '%s' && : > '%s' && : > '%s'", rmA, rmB, rmC, keepf), 0, "plant temporaries");
    ASSERT_TRUE(tu_file_exists(rmA) && tu_file_exists(rmB) && tu_file_exists(rmC) && tu_file_exists(keepf),
                "temporaries and control exist before the restart");
    ASSERT_EQ_INT(tu_run_cmd("test -z \"$(find %s/br/t/indexes -maxdepth 1 -name '.bmfix-*')\"", root), 0,
                "no repair marker (this is the no-marker branch)");
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!tu_file_exists(rmA), "startup swept the flag/12c .bm.remap");
    ASSERT_TRUE(!tu_file_exists(rmB), "startup swept the kind/1ff .bm.remap");
    ASSERT_TRUE(!tu_file_exists(rmC), "startup swept the n/12c .bm.remap");
    ASSERT_TRUE(tu_file_exists(keepf), "non-matching name survives the sweep");
    br_check(b, "after sweep restart");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    for (int i = 0; i < before.n; i++) {
        if (strcmp(before.e[i].rel, "flag/12c.bm.remap") == 0 ||
            strcmp(before.e[i].rel, "kind/1ff.bm.remap") == 0 ||
            strcmp(before.e[i].rel, "n/12c.bm.remap") == 0)
            continue;
        ASSERT_TRUE(br_inode_same(&before, &after, before.e[i].rel),
                    "the sweep touched only the temporaries");
    }
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* F11 pin (n > 0 branch): a restart with a pending .bmfix-NNN marker repairs
 * the named shards and slotcask_bmfix_clear then removes the .bm.remap
 * temporaries; the repaired bitmap's changed inode proves the repair (not
 * the sweep) ran. */
static int test_bmfix_remap_clear_with_marker(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    memset(b, 0, sizeof(*b));
    for (int i = 0; i < BR_N; i++) {
        b->rows[i].flag = (i % 3 == 0) || (i % 7 == 0);
        b->rows[i].kind = i % 3;
    }
    if (test_env_start(&b->env) != 0) { free(b); return 1; }
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0 || br_connect(b) != 0) {
        test_env_stop(&b->env);
        free(b);
        return 1;
    }
    char *r = br_q(b, "{\"mode\":\"add-dir\",\"dir\":\"br\"}");
    free(r);
    r = br_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"br\",\"object\":\"t\",\"splits\":512,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"n\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object (splits 512)");
    free(r);
    for (int from = 0; from < 500; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    br_check(b, "after seed");
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    char bm[600], marker[600], rmA[600], rmB[600];
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/12c.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-12c", root);
    snprintf(rmA, sizeof(rmA),   "%s/br/t/indexes/flag/12c.bm.remap", root);
    snprintf(rmB, sizeof(rmB),   "%s/br/t/indexes/kind/12c.bm.remap", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-300 bitmap exists");
    ASSERT_EQ_INT(tu_run_cmd("truncate -s 33 '%s'", bm), 0, "corrupt flag shard-300 bitmap");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the shard-300 marker");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s' && : > '%s'", rmA, rmB), 0, "plant .bm.remap temporaries");
    ASSERT_TRUE(tu_file_exists(rmA) && tu_file_exists(rmB),
                "temporaries exist before the restart");
    BrInoSnap before;
    br_inode_snapshot(&before, root);
    br_seam_once(root);
    int saved_err = br_stderr_capture(root);
    int started = test_env_start_at(&b->env, root, port) == 0;
    br_stderr_restore(saved_err);
    ASSERT_TRUE(started, "startup OK");
    if (!started) { br_close(b); free(b); return t_ctx->failed > 0 ? 1 : 0; }
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    ASSERT_TRUE(!br_repair_marker_exists(b), "marker gone");
    ASSERT_TRUE(!tu_file_exists(rmA), "the repair path cleared the flag/12c .bm.remap temporary");
    ASSERT_TRUE(!tu_file_exists(rmB), "the repair path cleared the kind/12c .bm.remap temporary");
    BrInoSnap after;
    br_inode_snapshot(&after, root);
    ASSERT_TRUE(!br_inode_same(&before, &after, "flag/12c.bm"), "the repair actually ran");
    br_check(b, "after repair restart");
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
TEST_REGISTER("test-bmfix-repair-shard-scoped", test_bmfix_repair_shard_scoped)
TEST_REGISTER("test-bmfix-repair-bad-marker-name", test_bmfix_repair_bad_marker_name)
TEST_REGISTER("test-bmfix-escape-disabled", test_bmfix_escape_disabled)
TEST_REGISTER("test-bmfix-backup-allowed-when-disabled", test_bmfix_backup_allowed_when_disabled)
TEST_REGISTER("test-bmfix-escape-heals-at-startup", test_bmfix_escape_heals_at_startup)
TEST_REGISTER("test-bmfix-escape-flag-write-fails", test_bmfix_escape_flag_write_fails)
TEST_REGISTER("test-bmfix-flag-without-markers", test_bmfix_flag_without_markers)
TEST_REGISTER("test-bmfix-reindex-clears-serving-markers", test_bmfix_reindex_clears_serving_markers)
TEST_REGISTER("test-bmfix-unclean-with-kfm2", test_bmfix_unclean_with_kfm2)
TEST_REGISTER("test-bmfix-unclean-repair-fails-with-kfm2", test_bmfix_unclean_repair_fails_with_kfm2)
TEST_REGISTER("test-bmfix-test-knobs-independent", test_bmfix_test_knobs_independent)
TEST_REGISTER("test-bitmap-resplit-log-scope", test_bitmap_resplit_log_scope)
TEST_REGISTER("test-bmfix-repair-shard-300", test_bmfix_repair_shard_300)
TEST_REGISTER("test-bmfix-remap-sweep", test_bmfix_remap_sweep)
TEST_REGISTER("test-bmfix-remap-clear-with-marker", test_bmfix_remap_clear_with_marker)
