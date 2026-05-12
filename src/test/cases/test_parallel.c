#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

static _Atomic int g_work_count;

static void *count_work(void *arg) {
    (void)arg;
    atomic_fetch_add_explicit(&g_work_count, 1, memory_order_relaxed);
    return NULL;
}

typedef struct {
    int id;
    int out;
} IntArg;

static void *set_id_work(void *arg) {
    IntArg *a = (IntArg *)arg;
    a->out = a->id * 2;
    return NULL;
}

static int test_parallel_run(void) {
    ASSERT_EQ_INT(parallel_pool_size(), 0, "pool not running before init");

    parallel_pool_init(4);
    ASSERT_TRUE(parallel_pool_size() > 0, "pool running after init");

    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 10, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 10, "parallel_for 10");

    IntArg args[5];
    for (int i = 0; i < 5; i++) { args[i].id = i; args[i].out = -1; }
    parallel_for(set_id_work, args, 5, sizeof(IntArg));
    for (int i = 0; i < 5; i++)
        ASSERT_EQ_INT(args[i].out, i * 2, "stride works");

    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 1, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 1, "single task");

    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 0, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 0, "zero tasks");

    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 100, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 100, "parallel_for 100");

    parallel_pool_shutdown();
    ASSERT_EQ_INT(parallel_pool_size(), 0, "pool stopped");

    parallel_pool_init(0);
    ASSERT_TRUE(parallel_pool_size() > 0, "auto nproc");
    parallel_pool_shutdown();

    parallel_pool_init(2);
    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 50, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 50, "pool size 2");
    parallel_pool_shutdown();

    atomic_store_explicit(&g_work_count, 0, memory_order_relaxed);
    parallel_for(count_work, NULL, 5, 0);
    ASSERT_EQ_INT((int)atomic_load_explicit(&g_work_count, memory_order_relaxed), 5, "no pool fallback");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-parallel", test_parallel_run)
