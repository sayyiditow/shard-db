/* src/test/cases/test_stress_no_hang.c
 * Port of tests/test-stress-no-hang.sh — concurrent-client stress harness.
 * N worker threads run mixed operations for T seconds; a watchdog probes
 * the daemon every PROBE_INTERVAL with a wallclock timeout. Pass criteria:
 * no probe times out, daemon alive at end, final probe responsive,
 * workers made forward progress.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

/* Tunables — note that this test exists to detect HANGS, not to benchmark.
   With persistent connections (vs bash's fork-per-op CLI), the daemon
   sees ~35× more concurrent load than the bash original at the same
   worker count. PROBE_TIMEOUT_SEC=30 keeps a real hang detectable while
   tolerating GHA-runner slowness; the test is a hang detector, not a
   latency benchmark. */
#define WORKERS 8
#define DURATION_SEC 10
#define PROBE_INTERVAL_SEC 2
#define PROBE_TIMEOUT_SEC 30

static atomic_int g_stop = 0;

typedef struct {
    int id;
    int port;
    long ops;
    long errs;
} StressWorker;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void *worker_main(void *arg) {
    StressWorker *w = arg;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->errs++; return NULL; }

    unsigned int seed = (unsigned int)(w->id * 12345 + (int)time(NULL));
    char req[512]; char *resp = NULL;
    while (!atomic_load_explicit(&g_stop, memory_order_relaxed)) {
        int roll = rand_r(&seed) % 100;
        char key[64]; snprintf(key, sizeof(key), "w%d_%ld", w->id, w->ops);
        int rc;
        if (roll < 50) {
            const char *st = (w->ops % 3 == 0) ? "paid" : "pending";
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"stress\","
                "\"key\":\"%s\",\"value\":{\"status\":\"%s\",\"amount\":%ld,"
                "\"note\":\"x\"}}", key, st, w->ops);
            rc = tc_request(tc, req, &resp); free(resp); resp = NULL;
        } else if (roll < 65) {
            rc = tc_request(tc,
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"stress\","
                "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}",
                &resp);
            free(resp); resp = NULL;
        } else if (roll < 78) {
            rc = tc_request(tc,
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"stress\","
                "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],"
                "\"limit\":5}", &resp);
            free(resp); resp = NULL;
        } else if (roll < 88) {
            rc = tc_request(tc,
                "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"stress\","
                "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                "{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"100\"}]}",
                &resp);
            free(resp); resp = NULL;
        } else if (roll < 95) {
            rc = tc_request(tc,
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"stress\","
                "\"criteria\":[],\"order_by\":\"amount\",\"order\":\"asc\","
                "\"limit\":10,\"cursor\":null}", &resp);
            free(resp); resp = NULL;
        } else {
            long target_op = w->ops > 0 ? w->ops - 1 : 0;
            snprintf(req, sizeof(req),
                "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"stress\","
                "\"key\":\"w%d_%ld\",\"value\":{\"status\":\"refunded\"}}",
                w->id, target_op);
            rc = tc_request(tc, req, &resp); free(resp); resp = NULL;
        }
        w->ops++;
        if (rc != 0) w->errs++;
    }
    tc_close(tc);
    return NULL;
}

typedef struct {
    int port;
    int probes;
    int hangs;
    long max_ms;
} Watchdog;

/* Per-probe wallclock: fork a thread that does the probe; parent times it
   out. We use poll on the connect socket via TestClientCfg.io_timeout_ms;
   PROBE_TIMEOUT_SEC clamps every probe by setting both connect+io timeouts. */
static void *watchdog_main(void *arg) {
    Watchdog *w = arg;
    while (!atomic_load_explicit(&g_stop, memory_order_relaxed)) {
        struct timespec ts = { PROBE_INTERVAL_SEC, 0 };
        nanosleep(&ts, NULL);
        if (atomic_load_explicit(&g_stop, memory_order_relaxed)) break;

        w->probes++;
        long t0 = now_ms();
        TestClientCfg cfg = { .port = w->port,
                              .connect_timeout_ms = PROBE_TIMEOUT_SEC * 1000,
                              .io_timeout_ms = PROBE_TIMEOUT_SEC * 1000 };
        TestClient *tc = tc_connect(&cfg);
        if (!tc) { w->hangs++; continue; }
        char *resp = NULL;
        int rc = tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"stress\"}", &resp);
        long dt = now_ms() - t0;
        free(resp); tc_close(tc);
        if (rc != 0 || dt > PROBE_TIMEOUT_SEC * 1000) w->hangs++;
        if (dt > w->max_ms) w->max_ms = dt;
    }
    return NULL;
}

static int test_stress_no_hang_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"stress\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:32\"],"
        /* splits=8 — the test inserts at most a few hundred records over
           DURATION_SEC, well below the splits=8 sweet-spot range
           (~78K-200K records/shard per CLAUDE.md sizing). The previous
           splits=256 was over-provisioned for ~20-50M records and made
           slotcask_open's eager kf-materialise loop dominate startup
           on slow shared CI disks (256 × open+ftruncate+mmap+madvise
           ≈ 30s on GHA SSD). Hang-detection semantics are unchanged. */
        "\"indexes\":[\"status\",\"amount\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    /* Spawn workers + watchdog. */
    atomic_store(&g_stop, 0);
    pthread_t threads[WORKERS];
    StressWorker workers[WORKERS];
    for (int i = 0; i < WORKERS; i++) {
        workers[i].id = i + 1; workers[i].port = env.port;
        workers[i].ops = 0; workers[i].errs = 0;
        pthread_create(&threads[i], NULL, worker_main, &workers[i]);
    }
    pthread_t wdog;
    Watchdog wd = { .port = env.port };
    pthread_create(&wdog, NULL, watchdog_main, &wd);

    long start = now_ms();
    sleep(DURATION_SEC);
    atomic_store(&g_stop, 1);

    for (int i = 0; i < WORKERS; i++) pthread_join(threads[i], NULL);
    pthread_join(wdog, NULL);
    long elapsed_ms = now_ms() - start;

    /* Daemon still alive — final probe must complete promptly. */
    cfg.port = env.port;
    cfg.connect_timeout_ms = PROBE_TIMEOUT_SEC * 1000;
    cfg.io_timeout_ms = PROBE_TIMEOUT_SEC * 1000;
    long t0 = now_ms();
    TestClient *fp = tc_connect(&cfg);
    ASSERT_NOT_NULL(fp, "final probe connects");
    int final_rc = -1;
    if (fp) {
        final_rc = tc_request(fp,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"stress\"}", &resp);
        free(resp); tc_close(fp);
    }
    long final_dt = now_ms() - t0;
    ASSERT_TRUE(final_rc == 0 && final_dt < PROBE_TIMEOUT_SEC * 1000,
                "final probe responds within timeout");

    ASSERT_EQ_INT(wd.hangs, 0, "no watchdog probe ever timed out");

    long total_ops = 0, total_errs = 0;
    for (int i = 0; i < WORKERS; i++) { total_ops += workers[i].ops; total_errs += workers[i].errs; }
    ASSERT_TRUE(total_ops > 0, "workers made forward progress under load");

    /* Throughput sanity: should clear at least 100 ops/sec aggregate. */
    long ops_per_sec = elapsed_ms > 0 ? (total_ops * 1000) / elapsed_ms : 0;
    printf("# stress: %ld ops total (%ld errs), %ld ops/sec, %d probes (max %ld ms)\n",
           total_ops, total_errs, ops_per_sec, wd.probes, wd.max_ms);

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-stress-no-hang", test_stress_no_hang_run)
