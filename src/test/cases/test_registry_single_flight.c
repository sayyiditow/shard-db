/* src/test/cases/test_registry_single_flight.c
 *
 * Regression guard for the 2026-07-03 hn-explorer prod incident:
 * slotcask_registry_get's miss path is now single-flighted per key, so
 * N concurrent first-touch queries against the same freshly-created
 * object must all succeed and must not hang — previously each miss
 * independently repeated the full slotcask_open() (including its
 * parallel_for_io fan-out), and N of those racing at once could jam the
 * shared IO pool badly enough to look like a hang from outside.
 *
 * splits=256 mirrors the actual hn/comments object from the incident
 * (see the slotcask_registry_get comment) — large enough that a single
 * slotcask_open is not instantaneous, which is what gives the race
 * window real width instead of everything finishing before threads
 * even get scheduled.
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
#include <string.h>
#include <time.h>

#define N_WORKERS 16
#define SPLITS 256
/* Absolute hang-regression bound — generous on purpose (this is a
   correctness/no-hang guard, not a latency benchmark; see
   test_stress_no_hang.c for the established precedent on why strict
   timing assertions are unreliable across CI runners). */
#define BURST_TIMEOUT_MS 20000

/* Portable stand-in for pthread_barrier_t: Apple's libc doesn't implement
   the (optional) POSIX barrier API at all, so pthread_barrier_t/_init/
   _wait/_destroy fail to compile on macOS. This is a plain generation-
   counted mutex+cond barrier with identical release-all-at-once semantics. */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             count;
    int             total;
    int             generation;
} TestBarrier;

static void test_barrier_init(TestBarrier *b, int total) {
    pthread_mutex_init(&b->lock, NULL);
    pthread_cond_init(&b->cond, NULL);
    b->count = 0;
    b->total = total;
    b->generation = 0;
}

static void test_barrier_wait(TestBarrier *b) {
    pthread_mutex_lock(&b->lock);
    int gen = b->generation;
    b->count++;
    if (b->count == b->total) {
        b->generation++;
        b->count = 0;
        pthread_cond_broadcast(&b->cond);
    } else {
        while (gen == b->generation) pthread_cond_wait(&b->cond, &b->lock);
    }
    pthread_mutex_unlock(&b->lock);
}

static void test_barrier_destroy(TestBarrier *b) {
    pthread_mutex_destroy(&b->lock);
    pthread_cond_destroy(&b->cond);
}

typedef struct {
    int port;
    TestBarrier *barrier;
    int ok;      /* 1 if request succeeded with no "error" field */
    long ms;     /* wall-clock for this worker's single request */
} RaceWorker;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void *race_worker_main(void *arg) {
    RaceWorker *w = arg;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = BURST_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->ok = 0; return NULL; }

    /* Every worker connects first, then all release the barrier
       together — this forces genuinely concurrent arrival at the
       server's registry regardless of how fast slotcask_open itself
       happens to run in this environment. */
    test_barrier_wait(w->barrier);

    long t0 = now_ms();
    char *resp = NULL;
    int rc = tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"racetest\",\"object\":\"hot\"}", &resp);
    w->ms = now_ms() - t0;
    w->ok = (rc == 0 && resp && !SAFE_STRSTR(resp, "\"error\""));
    free(resp);
    tc_close(tc);
    return NULL;
}

static int test_registry_single_flight_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = BURST_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"racetest\"}", &resp);
    free(resp); resp = NULL;

    char create_req[512];
    snprintf(create_req, sizeof(create_req),
        "{\"mode\":\"create-object\",\"dir\":\"racetest\",\"object\":\"hot\","
        "\"splits\":%d,\"max_key\":16,"
        "\"fields\":[\"v:int\"]}", SPLITS);
    tc_request(tc, create_req, &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object succeeded");
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    /* create-object opens-then-closes the object once itself (not
       registered) — the registry is still empty for "hot" at this
       point, so the burst below is a genuine N-way concurrent miss,
       exactly matching the incident's post-restart state (objects
       exist on disk; registry is cold). */

    TestBarrier barrier;
    test_barrier_init(&barrier, N_WORKERS);

    pthread_t threads[N_WORKERS];
    RaceWorker workers[N_WORKERS];
    for (int i = 0; i < N_WORKERS; i++) {
        workers[i].port = env.port;
        workers[i].barrier = &barrier;
        workers[i].ok = 0;
        workers[i].ms = 0;
        pthread_create(&threads[i], NULL, race_worker_main, &workers[i]);
    }

    long burst_t0 = now_ms();
    for (int i = 0; i < N_WORKERS; i++) pthread_join(threads[i], NULL);
    long burst_ms = now_ms() - burst_t0;

    test_barrier_destroy(&barrier);

    int all_ok = 1;
    for (int i = 0; i < N_WORKERS; i++) if (!workers[i].ok) all_ok = 0;
    ASSERT_TRUE(all_ok, "all concurrent first-touch queries succeeded");
    ASSERT_TRUE(burst_ms < BURST_TIMEOUT_MS,
        "concurrent cold-open burst completed within the hang-regression bound");

    /* Best-effort anti-thundering-herd signal — local-only (see
       test_stress_no_hang.c precedent for why strict timing assertions
       are excluded on shared CI runners). Not required for correctness;
       demonstrates the fix's actual perf property when run locally. */
    if (!getenv("CI")) {
        TAP_DIAG("# registry-single-flight: burst of %d completed in %ldms\n",
               N_WORKERS, burst_ms);
    }

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-registry-single-flight", test_registry_single_flight_run)
