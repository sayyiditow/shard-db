#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

static _Atomic int g_rd_count;
static _Atomic int g_rd_ready;          /* workers bump after rdlock+increment */
static _Atomic int g_rd_release;        /* main signals workers to release */
static _Atomic int g_observed_rd_count; /* releaser records count snapshot */
static pthread_mutex_t g_rd_mu = PTHREAD_MUTEX_INITIALIZER;

/* Workers hold the rdlock + the g_rd_count bump until main flips
   g_rd_release. No timing assumptions: macOS-arm64 GH runners scheduled
   threads with multi-100ms gaps under load, which broke the earlier
   "sleep 500ms, hope both ran" approach. Atomic barriers are flake-proof. */
static void *rd_worker(void *arg) {
    const char *obj = (const char *)arg;
    objlock_rdlock("root", obj);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count++;
    pthread_mutex_unlock(&g_rd_mu);
    atomic_fetch_add(&g_rd_ready, 1);
    while (atomic_load(&g_rd_release) == 0) usleep(1000);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count--;
    pthread_mutex_unlock(&g_rd_mu);
    objlock_rdunlock("root", obj);
    return NULL;
}

static void *rd_wait_worker(void *arg) {
    const char *obj = (const char *)arg;
    objlock_rdlock("root", obj);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count++;
    pthread_mutex_unlock(&g_rd_mu);
    atomic_fetch_add(&g_rd_ready, 1);
    while (atomic_load(&g_rd_release) == 0) usleep(1000);
    objlock_rdunlock("root", obj);
    return NULL;
}

/* Background releaser for the wrlock-blocked-by-reader test. Records
   the observed g_rd_count snapshot — guaranteed to be 1 because the
   reader is parked inside the rdlock at this point — THEN flips
   g_rd_release so the reader can let go and the main thread's wrlock
   can acquire. Separate thread because objlock_wrlock blocks the caller.
   The assert runs from main (t_ctx is __thread; spawned threads have
   t_ctx==NULL so ASSERT_EQ_INT would null-deref). */
static void *release_after_check(void *arg) {
    (void)arg;
    atomic_store(&g_observed_rd_count, g_rd_count);
    atomic_store(&g_rd_release, 1);
    return NULL;
}

static int test_objlock_unit_run(void) {
    objlock_init();

    objlock_rdlock("root", "obj1");
    objlock_rdlock("root", "obj1");
    objlock_rdunlock("root", "obj1");
    objlock_rdunlock("root", "obj1");

    objlock_wrlock("root", "obj2");
    objlock_wrunlock("root", "obj2");

    objlock_rdlock("root", "obj1");
    objlock_wrlock("root", "obj2");
    objlock_rdunlock("root", "obj1");
    objlock_wrunlock("root", "obj2");

    pthread_t t1, t2;
    g_rd_count = 0;
    atomic_store(&g_rd_ready, 0);
    atomic_store(&g_rd_release, 0);
    pthread_create(&t1, NULL, rd_worker, (void *)"shared");
    pthread_create(&t2, NULL, rd_worker, (void *)"shared");
    /* Wait — unboundedly — for both workers to be parked inside their
       rdlock. No false negative from a missed scheduling window. */
    while (atomic_load(&g_rd_ready) < 2) usleep(1000);
    ASSERT_EQ_INT(g_rd_count, 2, "two concurrent readers");
    objlock_rdlock("root", "shared");
    objlock_rdunlock("root", "shared");
    atomic_store(&g_rd_release, 1);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    pthread_t wt;
    g_rd_count = 0;
    atomic_store(&g_rd_ready, 0);
    atomic_store(&g_rd_release, 0);
    pthread_create(&wt, NULL, rd_wait_worker, (void *)"wr_test");
    /* Wait for the reader to definitely hold its rdlock before we
       contend with wrlock. */
    while (atomic_load(&g_rd_ready) < 1) usleep(1000);
    /* The main thread will call objlock_wrlock and block until the reader
       releases. We need a side thread to (a) snapshot g_rd_count while
       the reader still holds, then (b) flip g_rd_release so the reader
       exits and the wrlock acquires. wrlock acquiring proves the
       releaser ran (reader only releases on g_rd_release), so by the
       time we assert below, g_observed_rd_count is set. */
    atomic_store(&g_observed_rd_count, -1);
    pthread_t releaser;
    pthread_create(&releaser, NULL, release_after_check, NULL);
    objlock_wrlock("root", "wr_test");
    ASSERT_EQ_INT(atomic_load(&g_observed_rd_count), 1, "wrlock held while reader active");
    objlock_wrunlock("root", "wr_test");
    pthread_join(releaser, NULL);
    pthread_join(wt, NULL);

    objlock_rdlock("root", "wr_test");
    objlock_rdunlock("root", "wr_test");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-unit", test_objlock_unit_run)
