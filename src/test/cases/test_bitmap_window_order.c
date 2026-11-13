/* F1/F7b: the bulk-insert window must take its multi-bitmap writer locks in
   the canonical ascending <field>/NNN.bm order, like bm_open_many_ro, the
   keyfile-rebuild remap and the single-insert staging loop. The schema here
   declares kind:bitmap BEFORE flag:bitmap, so first-use order (the old
   behavior) opens kind first — the exact inversion that deadlocks against a
   canonical reader or the resplit remap. The TEST_BUILD BMLOCK-ORDER trace
   (armed over the daemon control channel) logs one line per acquired bitmap
   entry lock; the case asserts the window's writer acquisitions are canonical.
   Runs in its own daemon and tmpdir. */
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <glob.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BWO_N 3000

typedef struct {
    TestEnv env;
    TestClient *tc;
    int flag[BWO_N];
    int kind[BWO_N];
    int live[BWO_N];
} Bwo;

static char *bwo_q(Bwo *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *bwo_q(Bwo *b, const char *fmt, ...) {
    char req[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(b->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}

static int bwo_bulk(Bwo *b, int from, int to) {
    size_t cap = (size_t)(to - from) * 128 + 256;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bwo\",\"object\":\"t\",\"records\":[");
    for (int i = from; i < to; i++) {
        n = tappendf(req, cap, n,
            "%s{\"key\":\"k%05d\",\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
            i > from ? "," : "", i, b->flag[i] ? "true" : "false",
            b->kind[i] == 0 ? "a" : b->kind[i] == 1 ? "b" : "c", i);
        b->live[i] = 1;
    }
    snprintf(req + n, cap - n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    int ok = rc == 0 && resp && !strstr(resp, "\"error\"");
    free(resp);
    return ok ? 0 : -1;
}

static int bwo_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"bwo\",\"object\":\"t\",\"criteria\":%s}", crit);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 && r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

/* Only this test's two fields are traced. Each worker holds at most one
   shard's flag/kind pair at a time. Check every transition per worker,
   including apply after prepare has released its pair. */
static int bwo_check_trace(const char *db_root) {
    struct { unsigned long tid; int used, flag, kind; } held[256] = {{0}};
    char pattern[1024];
    snprintf(pattern, sizeof(pattern), "%s/../logs/*-info.log", db_root);
    glob_t paths = {0};
    int grc = glob(pattern, 0, NULL, &paths);
    ASSERT_EQ_INT(grc, 0, "find daemon info logs");
    if (grc != 0) { globfree(&paths); return -1; }
    int events = 0, pairs = 0, inversions = 0;
    for (size_t f = 0; f < paths.gl_pathc; f++) {
        FILE *fp = fopen(paths.gl_pathv[f], "r");
        ASSERT_NOT_NULL(fp, "open daemon info log");
        if (!fp) continue;
        char line[4096];
        while (fgets(line, sizeof(line), fp)) {
            char *mark = strstr(line, "BMLOCK-ORDER ");
            if (!mark) continue;
            unsigned long tid;
            char op, path[1024];
            int n = sscanf(mark, "BMLOCK-ORDER tid=%lu op=%c %1023s", &tid, &op, path);
            ASSERT_EQ_INT(n, 3, "parse complete lock trace event");
            if (n != 3 || !strstr(path, "/bwo/t/indexes/")) continue;
            int flag = strstr(path, "/flag/") != NULL;
            int kind = strstr(path, "/kind/") != NULL;
            if (!flag && !kind) continue;
            size_t t;
            for (t = 0; t < 256; t++)
                if (!held[t].used || held[t].tid == tid) break;
            ASSERT_TRUE(t < 256, "bounded trace thread table");
            if (t == 256) continue;
            held[t].used = 1;
            held[t].tid = tid;
            int *own = flag ? &held[t].flag : &held[t].kind;
            if (op == '+') {
                ASSERT_EQ_INT(*own, 0, "no duplicate held field");
                if (flag && held[t].kind) inversions++;
                if ((flag && held[t].kind) || (kind && held[t].flag)) pairs++;
                *own = 1;
            } else if (op == '-') {
                ASSERT_EQ_INT(*own, 1, "release matches acquisition on same thread");
                *own = 0;
            } else {
                ASSERT_TRUE(0, "known trace event operation");
            }
            events++;
        }
        ASSERT_EQ_INT(ferror(fp), 0, "read complete trace file");
        fclose(fp);
    }
    globfree(&paths);
    for (size_t t = 0; t < 256; t++) {
        ASSERT_EQ_INT(held[t].flag + held[t].kind, 0, "no unmatched writer acquisitions");
    }
    ASSERT_TRUE(events > 0, "writer trace is present");
    ASSERT_EQ_INT(inversions, 0, "every writer acquisition is canonical");
    ASSERT_TRUE(pairs > 0, "trace exercises multi-bitmap held spans");
    return inversions ? -1 : 0;
}

static int test_bitmap_window_lock_order_run(void) {
    Bwo *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    for (int i = 0; i < BWO_N; i++) {
        b->flag[i] = (i % 3 == 0) || (i % 7 == 0);
        b->kind[i] = i % 3;
    }
    if (test_env_start(&b->env) != 0) { free(b); return 1; }
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0) { test_env_stop(&b->env); free(b); return 1; }
    TestClientCfg cfg = { .port = b->env.port, .io_timeout_ms = 120000 };
    b->tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(b->tc, "connect");
    if (!b->tc) { test_env_stop(&b->env); free(b); return 1; }
    char *r = bwo_q(b, "{\"mode\":\"add-dir\",\"dir\":\"bwo\"}");
    free(r);
    /* Declaration order kind BEFORE flag = the non-canonical order. */
    r = bwo_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"bwo\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"kind:bitmap\",\"flag:bitmap\",\"n\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object");
    free(r);

    /* Arm the lock-order trace before the load that forces growth resplits. */
    ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&b->env, 1), 0, "arm BMLOCK-ORDER trace");
    for (int from = 0; from < BWO_N; from += 500)
        ASSERT_EQ_INT(bwo_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&b->env, 0), 0, "disarm BMLOCK-ORDER trace");

    /* Answers stay exact across the resplits regardless of the order work. */
    int ft = 0, k[3] = { 0, 0, 0 };
    for (int i = 0; i < BWO_N; i++) {
        if (!b->live[i]) continue;
        if (b->flag[i]) ft++;
        k[b->kind[i]]++;
    }
    ASSERT_EQ_INT(bwo_count(b->tc, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]"), ft,
                  "count flag=true == oracle");
    ASSERT_EQ_INT(bwo_count(b->tc, "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]"), k[1],
                  "count kind=b == oracle");

    tc_close(b->tc);
    b->tc = NULL;
    /* Graceful shutdown drains the asynchronous logger. stop_keep retains
       the fixture tree; stop() alone would delete the evidence. */
    test_env_stop_keep(&b->env);
    (void)bwo_check_trace(b->env.db_root);
    char base[sizeof(b->env.db_root)];
    snprintf(base, sizeof(base), "%s", b->env.db_root);
    char *slash = strrchr(base, '/');
    ASSERT_NOT_NULL(slash, "fixture parent path");
    if (slash) {
        *slash = '\0';
        ASSERT_EQ_INT(tu_run_cmd("rm -rf '%s'", base), 0, "remove retained fixture");
    }
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-window-lock-order", test_bitmap_window_lock_order_run)
