#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include <stdlib.h>
#include <string.h>

/* The heap helpers are declared `static` in query.c but exposed to
 * tests via the TOPN_VIS macro when TEST_BUILD is set. */
extern void *topn_heap_new(int cap, int order_desc);
extern void  topn_heap_destroy(void *h);
extern int   topn_heap_size(void *h);
extern int   topn_heap_offer(void *h, double metric,
                              const char *gk, size_t gklen,
                              int64_t count, double sum,
                              double min, double max);
extern int   topn_heap_drain(void *h, double *metrics_out,
                              char **gkeys_out, size_t *gklens_out,
                              int64_t *counts_out, double *sums_out,
                              double *mins_out, double *maxs_out);

static int test_topn_heap_basic_desc(void) {
    void *h = topn_heap_new(3, 1 /* desc */);
    ASSERT_TRUE(h != NULL, "heap created");
    ASSERT_EQ_INT(topn_heap_size(h), 0, "size starts 0");

    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);
    topn_heap_offer(h, 20, "d", 1, 20, 0, 0, 0);
    topn_heap_offer(h, 40, "e", 1, 40, 0, 0, 0);

    ASSERT_EQ_INT(topn_heap_size(h), 3, "size capped at 3");

    double metrics[3]; char *gks[3]; size_t gklens[3];
    int64_t counts[3]; double sums[3]; double mins[3]; double maxs[3];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns 3");

    ASSERT_TRUE(metrics[0] == 50.0, "drain[0] = 50");
    ASSERT_TRUE(metrics[1] == 40.0, "drain[1] = 40");
    ASSERT_TRUE(metrics[2] == 30.0, "drain[2] = 30");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

static int test_topn_heap_basic_asc(void) {
    void *h = topn_heap_new(3, 0 /* asc */);
    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);
    topn_heap_offer(h, 20, "d", 1, 20, 0, 0, 0);
    topn_heap_offer(h, 40, "e", 1, 40, 0, 0, 0);

    double metrics[3]; char *gks[3]; size_t gklens[3];
    int64_t counts[3]; double sums[3]; double mins[3]; double maxs[3];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns 3");

    ASSERT_TRUE(metrics[0] == 10.0, "drain[0] = 10");
    ASSERT_TRUE(metrics[1] == 20.0, "drain[1] = 20");
    ASSERT_TRUE(metrics[2] == 30.0, "drain[2] = 30");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

static int test_topn_heap_under_cap(void) {
    void *h = topn_heap_new(10, 1);
    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);

    ASSERT_EQ_INT(topn_heap_size(h), 3, "size = 3 (under cap)");

    double metrics[10]; char *gks[10]; size_t gklens[10];
    int64_t counts[10]; double sums[10]; double mins[10]; double maxs[10];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns all 3");
    ASSERT_TRUE(metrics[0] == 50.0 && metrics[1] == 30.0 && metrics[2] == 10.0,
                "under-cap drain still sorted desc");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

TEST_REGISTER("test-topn-heap-basic-desc", test_topn_heap_basic_desc)
TEST_REGISTER("test-topn-heap-basic-asc", test_topn_heap_basic_asc)
TEST_REGISTER("test-topn-heap-under-cap", test_topn_heap_under_cap)
