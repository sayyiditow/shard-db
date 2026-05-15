#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <string.h>
#include <pthread.h>
#include <unistd.h>

static _Atomic int g_rd_count;
static pthread_mutex_t g_rd_mu = PTHREAD_MUTEX_INITIALIZER;

static void *rd_worker(void *arg) {
    const char *obj = (const char *)arg;
    objlock_rdlock("test_root", obj);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count++;
    pthread_mutex_unlock(&g_rd_mu);
    usleep(10000);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count--;
    pthread_mutex_unlock(&g_rd_mu);
    objlock_rdunlock("test_root", obj);
    return NULL;
}

static void *rd_wait_worker(void *arg) {
    const char *obj = (const char *)arg;
    objlock_rdlock("test_root", obj);
    pthread_mutex_lock(&g_rd_mu);
    g_rd_count++;
    pthread_mutex_unlock(&g_rd_mu);
    usleep(50000);
    objlock_rdunlock("test_root", obj);
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
    pthread_create(&t1, NULL, rd_worker, (void *)"shared");
    pthread_create(&t2, NULL, rd_worker, (void *)"shared");
    /* Poll up to 500ms for both readers to register — bare 5 ms sleep was
       fine on Linux but flaky on macOS-arm64 GH runners where thread
       scheduling can take 10-50 ms before both workers run. */
    for (int i = 0; i < 100 && g_rd_count < 2; i++) usleep(5000);
    ASSERT_EQ_INT(g_rd_count, 2, "two concurrent readers");
    objlock_rdlock("root", "shared");
    objlock_rdunlock("root", "shared");
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    pthread_t wt;
    g_rd_count = 0;
    pthread_create(&wt, NULL, rd_wait_worker, (void *)"wr_test");
    /* Wait for the reader to actually acquire its rdlock before we
       contend with wrlock — see comment above on macOS scheduling. */
    for (int i = 0; i < 100 && g_rd_count < 1; i++) usleep(5000);
    objlock_wrlock("root", "wr_test");
    ASSERT_EQ_INT(g_rd_count, 1, "wrlock held while reader active");
    objlock_wrunlock("root", "wr_test");
    pthread_join(wt, NULL);

    objlock_rdlock("root", "wr_test");
    objlock_rdunlock("root", "wr_test");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-unit", test_objlock_unit_run)
