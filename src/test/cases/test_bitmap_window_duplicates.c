/* Review fix regression: the bulk-insert window's preopen must open ONE
   writer handle per (field, kf_shard), even when the schema declares the
   same bitmap field twice (create-object accepts
   "indexes":["flag:bitmap","flag:bitmap"]). Without dedup the preopen loop
   acquires the same bitmap cache-entry writer lock twice on one thread —
   the second pthread_rwlock_wrlock returns EDEADLK (ignored by the opener),
   leaving two handles that both log an acquire and both unlock the same
   lock at apply. The BMLOCK-ORDER trace catches both symptoms: a duplicate
   held field on one thread and a release that does not match an
   acquisition. The declaration here also places the duplicate AFTER kind
   so the dedup must drop it wherever it sits in the sorted order.
   The load is one tiny chunk: at larger loads the unfixed daemon wedges
   once other multi-bitmap participants interleave with the fake
   double-held lock (two pre-fix runs hung 20+ minutes with every worker
   futex-blocked, trace fully paired), and this suite has no per-case
   timeout — the red run must fail deterministically, not hang. One
   touched shard still opens the duplicated field through preopen, apply
   and close, which is the whole mechanism. Runs in its own daemon and
   tmpdir. */
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

#define BWD_N 2

typedef struct {
    TestEnv env;
    TestClient *tc;
    int flag[BWD_N];
    int kind[BWD_N];
    int live[BWD_N];
} Bwd;

static char *bwd_q(Bwd *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *bwd_q(Bwd *b, const char *fmt, ...) {
    char req[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(b->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}

static int bwd_bulk(Bwd *b, int from, int to) {
    size_t cap = (size_t)(to - from) * 128 + 256;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bwd\",\"object\":\"t\",\"records\":[");
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

static int bwd_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"bwd\",\"object\":\"t\",\"criteria\":%s}", crit);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 && r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

/* Same per-thread held-lock accounting as the lock-order case: duplicates
   surface as a second '+' for a field the thread already holds. */
static int bwd_check_trace(const char *db_root) {
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
            if (n != 3 || !strstr(path, "/bwd/t/indexes/")) continue;
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

static int test_bitmap_window_duplicates_run(void) {
    Bwd *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    for (int i = 0; i < BWD_N; i++) {
        b->flag[i] = (i % 3 == 0) || (i % 7 == 0);
        b->kind[i] = i % 3;
    }
    if (test_env_start(&b->env) != 0) { free(b); return 1; }
    TestClientCfg cfg = { .port = b->env.port, .io_timeout_ms = 120000 };
    b->tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(b->tc, "connect");
    if (!b->tc) { test_env_stop(&b->env); free(b); return 1; }
    char *r = bwd_q(b, "{\"mode\":\"add-dir\",\"dir\":\"bwd\"}");
    free(r);
    /* flag:bitmap declared TWICE (also after kind) — the server accepts the
       duplicate, so preopen sees two entries for the same (field, shard). */
    r = bwd_q(b,
        "{\"mode\":\"create-object\",\"dir\":\"bwd\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"flag:bitmap\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object with duplicate bitmap index");
    free(r);

    ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&b->env, 1), 0, "arm BMLOCK-ORDER trace");
    ASSERT_EQ_INT(bwd_bulk(b, 0, BWD_N), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&b->env, 0), 0, "disarm BMLOCK-ORDER trace");

    int ft = 0, k[3] = { 0, 0, 0 };
    for (int i = 0; i < BWD_N; i++) {
        if (!b->live[i]) continue;
        if (b->flag[i]) ft++;
        k[b->kind[i]]++;
    }
    ASSERT_EQ_INT(bwd_count(b->tc, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]"), ft,
                  "count flag=true == oracle");
    ASSERT_EQ_INT(bwd_count(b->tc, "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]"), k[1],
                  "count kind=b == oracle");

    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    (void)bwd_check_trace(b->env.db_root);
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

TEST_REGISTER("test-bitmap-window-duplicates", test_bitmap_window_duplicates_run)
