/* src/bench/bench_btree.c — standalone B+ tree microbenchmark
 *
 * Benchmarks btree_bulk_build, btree_insert_batch, btree_range,
 * and btree_delete directly (no daemon, no network, no JSON).
 * Opens a temp .idx file and times each phase in wall-clock ms.
 *
 * Key shape: "key-%010d" — sorted for bulk_build, out-of-order
 * for insert to trigger splits + page grows.
 *
 * Default scale: N=100000 (override via SHARD_BENCH_COUNT).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include "btree.h"
#include "bench_stats.h"
#include "test_runner.h"

/* Count matching entries in range, returned via callback context. */
typedef struct { int count; } Ctx;
static int count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h;
    ((Ctx *)ctx)->count++;
    return 0;
}

static int bench_btree_run(void)
{
    const char *env = getenv("SHARD_BENCH_COUNT");
    int N = env ? atoi(env) : 100000;
    if (N <= 0) N = 100000;

    printf("======================================\n");
    printf("  B+ Tree microbenchmark (%d entries)\n", N);
    printf("======================================\n\n");

    char tmpdir[] = "/tmp/btree-bench-XXXXXX";
    if (!mkdtemp(tmpdir)) { perror("mkdtemp"); return 1; }

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/bench.idx", tmpdir);

    /* Build sorted BtEntry array for bulk_build. */
    BtEntry *entries = calloc((size_t)N, sizeof(BtEntry));
    char    *strings = calloc((size_t)N, 24);
    if (!entries || !strings) {
        fprintf(stderr, "OOM\n");
        free(entries); free(strings);
        rmdir(tmpdir);
        return 1;
    }
    for (int i = 0; i < N; i++) {
        char *k = strings + (size_t)i * 24;
        snprintf(k, 24, "key-%010d", i);
        entries[i].value = k;
        entries[i].vlen  = strlen(k);
    }

    bt_cache_init(64);

    /* Phase 1: btree_bulk_build — sorted, no splits. */
    {
        uint64_t t0 = bench_now_ns();
        btree_bulk_build(path, entries, (size_t)N);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("Phase 1 — btree_bulk_build (%d entries): %.2f ms  (%.0f inserts/sec)\n",
               N, (double)elapsed / 1e6, (double)N / ((double)elapsed / 1e9));
    }

    /* Phase 2: btree_insert_batch — out-of-order, triggers splits + grows.
       Batch insert opens the file once, so N=100000 is fast. */
    {
        BtEntry *perm = malloc((size_t)N * sizeof(BtEntry));
        char    *pstr = malloc((size_t)N * 24);
        if (!perm || !pstr) { free(perm); free(pstr); free(entries); free(strings); return 1; }
        btree_cache_invalidate(path);
        unlink(path);

        int lo = 0, hi = N / 2;
        for (int i = 0; i < N; i++) {
            int idx;
            if (i % 2 == 0)      idx = lo++;
            else                 idx = hi++;
            char *k = pstr + (size_t)i * 24;
            snprintf(k, 24, "key-%010d", idx);
            perm[i].value = k;
            perm[i].vlen  = strlen(k);
        }

        uint64_t t0 = bench_now_ns();
        btree_insert_batch(path, perm, (size_t)N);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("Phase 2 — btree_insert_batch x%d (permuted, splits): %.2f ms  (%.0f inserts/sec)\n",
               N, (double)elapsed / 1e6, (double)N / ((double)elapsed / 1e9));
        free(perm); free(pstr);
    }

    /* Phase 3: btree_range x1000 — pull 1000 entries at a time. */
    {
        uint64_t t0 = bench_now_ns();
        for (int i = 0; i < N; i += 1000) {
            char min[24], max[24];
            snprintf(min, 24, "key-%010d", i);
            snprintf(max, 24, "key-%010d", i + 999);
            Ctx c = {0};
            btree_range(path, min, strlen(min), max, strlen(max), count_cb, &c);
            (void)c;
        }
        uint64_t elapsed = bench_now_ns() - t0;
        printf("Phase 3 — btree_range x%d (sorted, warm): %.2f ms  (%.0f ranges/sec)\n",
               N / 1000, (double)elapsed / 1e6, (double)(N / 1000) / ((double)elapsed / 1e9));
    }

    /* Phase 5: btree_range_desc_ex x1000 — descending range scans. */
    {
        uint64_t t0 = bench_now_ns();
        for (int i = 0; i < N; i += 1000) {
            char min[24], max[24];
            snprintf(min, 24, "key-%010d", i);
            snprintf(max, 24, "key-%010d", i + 999);
            Ctx c = {0};
            btree_range_desc_ex(path, min, strlen(min), 0, max, strlen(max), 0, count_cb, &c);
            (void)c;
        }
        uint64_t elapsed = bench_now_ns() - t0;
        printf("Phase 5 — btree_range_desc x%d (sorted, warm): %.2f ms  (%.0f ranges/sec)\n",
               N / 1000, (double)elapsed / 1e6, (double)(N / 1000) / ((double)elapsed / 1e9));
    }

    bt_cache_shutdown();
    unlink(path);
    rmdir(tmpdir);
    free(entries);
    free(strings);

    printf("\n======================================\n");
    printf("  btree microbenchmark complete\n");
    printf("======================================\n");
    return 0;
}

TEST_REGISTER("bench-btree", bench_btree_run);