/* src/bench/bench_cache_pollution.c
 *
 * Measure the cost of a single full-scan query on the latency of
 * subsequent indexed queries — the "cache pollution" pattern raised
 * by the operator after seeing varied warm/cold gaps in bench_queries.c
 * runs: full scans over non-indexed fields touch every data-shard
 * segment file end-to-end, populating the kernel page cache with
 * pages that have zero re-access locality, evicting the hot kfcache
 * + index leaves that drive every other query.
 *
 * Methodology
 * -----------
 * Reuses an existing 25 M-row users object (the same one bench_queries.c
 * leaves behind in persistent mode). Hot query set picks representative
 * indexed routes: point lookup, bool eq (bitmap), order_by+limit, etc.
 *
 *   Phase A : Warmup — run every hot query WARMUP_ITERS times so all
 *             relevant index pages + kfcache pages are resident in
 *             page cache.
 *   Phase B : Baseline — run every hot query MEASURE_ITERS times,
 *             take the median per query. This is the "warm" cost.
 *   Phase C : Pollute — run ONE full-scan COUNT over a non-indexed
 *             varchar field (bio). On 25 M rows this walks every
 *             data shard's segment file and reads ~10-20 GB of mmapped
 *             data into page cache (the working set HN explorer will
 *             actually see at full-scale).
 *   Phase D : Re-measure — run every hot query MEASURE_ITERS times
 *             again, take the median.
 *   Phase E : Report per-query warm vs post-pollution latency + the
 *             ratio. Anything > 2× is showing real eviction; 1.0×-1.2×
 *             is within noise; < 1.0× means the polluting scan
 *             warmed something useful (rare).
 *
 * Requires SHARD_BENCH_DB_ROOT — the bench needs an existing 25 M
 * dataset to be meaningful, and we don't want to spend 3 minutes
 * seeding one each run. Build the dataset once via:
 *     rm -rf db && SHARD_BENCH_COUNT=25000000 \
 *       SHARD_BENCH_DB_ROOT=./db ./build/bin/shard-db-bench run bench-queries
 * then invoke this bench against the same DB_ROOT.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define WARMUP_ITERS  3
#define MEASURE_ITERS 7

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* Run `json` N times, return the median latency in ns. We don't care
   about throughput here — we care about the distribution-stable point
   in the warm/post-pollution latency, and median is robust to the
   occasional GC pause / context-switch outlier that would skew a mean. */
static uint64_t run_n_median(TestClient *tc, const char *json, int n) {
    uint64_t samples[64];
    if (n > 64) n = 64;
    for (int i = 0; i < n; i++) {
        char *resp = NULL;
        uint64_t t0 = now_ns();
        tc_request(tc, json, &resp);
        uint64_t t1 = now_ns();
        samples[i] = t1 - t0;
        free(resp);
    }
    qsort(samples, (size_t)n, sizeof(samples[0]), cmp_u64);
    return bench_median_sorted_ns(samples, (size_t)n);
}

typedef struct {
    const char *label;
    const char *json;
} HotQuery;

static int bench_cache_pollution_run(void) {
    const char *db_root_env = getenv("SHARD_BENCH_DB_ROOT");
    if (!db_root_env) {
        fprintf(stderr,
            "bench-cache-pollution: SHARD_BENCH_DB_ROOT required (point at a "
            "pre-seeded 25M users DB; see bench_queries.c for how to seed)\n");
        return 1;
    }
    const char *port_env = getenv("SHARD_BENCH_PORT");
    int port = port_env ? atoi(port_env) : test_pick_port();
    if (port <= 0) { fprintf(stderr, "bench-cache-pollution: port alloc failed\n"); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root_env, port) != 0) {
        fprintf(stderr, "bench-cache-pollution: daemon spawn at %s failed\n", db_root_env);
        return 1;
    }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop_keep(&env); return 1; }

    /* Hot query set — representative of HN explorer's actual read mix.
       Every one of these must be sub-10ms when warm, otherwise the
       "warm baseline" itself drowns the pollution signal. */
    static const HotQuery HOT[] = {
        { "eq age=30 (int btree)",
          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"30\"}]}" },
        { "eq user_id=500000 (long btree)",
          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"user_id\",\"op\":\"eq\",\"value\":\"500000\"}]}" },
        { "eq active=false (bool bitmap)",
          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"false\"}]}" },
        { "eq username (varchar btree)",
          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"alice.smith0\"}]}" },
        { "find by username order_by age desc limit 10",
          "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"alice.smith0\"}],"
          "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10}" },
        { "cursor ASC by age limit 100",
          "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\","
          "\"order_by\":\"age\",\"order\":\"asc\",\"limit\":100,\"cursor\":{}}" },
        { "exists email (varchar idx)",
          "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
          "\"criteria\":[{\"field\":\"email\",\"op\":\"exists\"}]}" },
    };
    const int N_HOT = (int)(sizeof(HOT) / sizeof(HOT[0]));

    /* Polluting query — full scan over the non-indexed bio field.
       starts/contains/ends all hit the per-record-decode path that
       walks every data shard end-to-end. We use `starts` because it
       returns deterministic results (no false positives from
       randomised fixture data) and matches what an admin running an
       ad-hoc text search would do. */
    static const char *POLLUTE_JSON =
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"starts\",\"value\":\"Software\"}]}";

    uint64_t warm_med[16] = {0};
    uint64_t post_med[16] = {0};

    printf("\n=== Phase A: warmup (%d iters per query) ===\n", WARMUP_ITERS);
    for (int i = 0; i < N_HOT; i++) {
        run_n_median(tc, HOT[i].json, WARMUP_ITERS);
        printf("  warmed: %s\n", HOT[i].label);
    }

    printf("\n=== Phase B: warm baseline (%d iters, median) ===\n", MEASURE_ITERS);
    for (int i = 0; i < N_HOT; i++) {
        warm_med[i] = run_n_median(tc, HOT[i].json, MEASURE_ITERS);
        printf("  %-50s  %7.2f ms\n",
               HOT[i].label, (double)warm_med[i] / 1e6);
    }

    printf("\n=== Phase C: pollute (single full-scan over non-indexed bio) ===\n");
    uint64_t poll_t0 = now_ns();
    {
        char *resp = NULL;
        tc_request(tc, POLLUTE_JSON, &resp);
        free(resp);
    }
    uint64_t poll_ms = (now_ns() - poll_t0) / 1000000;
    printf("  bio starts 'Software' scan: %lu ms (walks every data shard)\n",
           (unsigned long)poll_ms);

    printf("\n=== Phase D: post-pollution re-measure (%d iters, median) ===\n", MEASURE_ITERS);
    for (int i = 0; i < N_HOT; i++) {
        post_med[i] = run_n_median(tc, HOT[i].json, MEASURE_ITERS);
        printf("  %-50s  %7.2f ms\n",
               HOT[i].label, (double)post_med[i] / 1e6);
    }

    printf("\n=== Phase E: report (warm → post-pollution) ===\n");
    printf("  %-50s %10s %10s %8s\n",
           "query", "warm ms", "post ms", "ratio");
    printf("  %-50s %10s %10s %8s\n",
           "---------------------------------------------",
           "---------", "---------", "-------");
    double worst_ratio = 0;
    const char *worst_label = HOT[0].label;
    for (int i = 0; i < N_HOT; i++) {
        double w = (double)warm_med[i] / 1e6;
        double p = (double)post_med[i] / 1e6;
        double r = w > 0 ? p / w : 0;
        printf("  %-50s %10.2f %10.2f %7.2fx\n",
               HOT[i].label, w, p, r);
        if (r > worst_ratio) { worst_ratio = r; worst_label = HOT[i].label; }
    }
    printf("\n  worst-case eviction signal: %.2fx on '%s'\n",
           worst_ratio, worst_label);
    printf("  reading: ratio > 2.0x ≈ real eviction, "
           "1.0-1.5x ≈ noise, < 1.0x ≈ accidental warming.\n");

    tc_close(tc);
    test_env_stop_keep(&env);
    return 0;
}

TEST_REGISTER("bench-cache-pollution", bench_cache_pollution_run)
