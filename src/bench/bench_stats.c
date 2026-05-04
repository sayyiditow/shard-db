/* src/bench/bench_stats.c */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "bench_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

uint64_t bench_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

uint64_t bench_hist_p50_ns(BenchHist *h) {
    if (!h || h->count == 0) return 0;
    qsort(h->samples_ns, h->count, sizeof(uint64_t), cmp_u64);
    return h->samples_ns[h->count / 2];
}

void bench_hist_report(BenchHist *h, const char *label, uint64_t total_wall_ns) {
    if (h->count == 0) {
        printf("%s: no samples\n", label);
        return;
    }
    qsort(h->samples_ns, h->count, sizeof(uint64_t), cmp_u64);
    uint64_t sum = 0;
    for (size_t i = 0; i < h->count; i++) sum += h->samples_ns[i];
    double mean_us = (double)sum / (double)h->count / 1000.0;
    double p50_us = (double)h->samples_ns[h->count / 2] / 1000.0;
    size_t p99_i = (size_t)((double)h->count * 0.99);
    if (p99_i >= h->count) p99_i = h->count - 1;
    double p99_us = (double)h->samples_ns[p99_i] / 1000.0;
    if (total_wall_ns == 0) total_wall_ns = sum;
    double throughput_kops = (double)h->count * 1000000.0 / ((double)total_wall_ns / 1000.0);
    printf("%s: N=%zu  mean=%.1fµs  p50=%.1fµs  p99=%.1fµs  throughput=%.2f k ops/sec  wall=%.2fms\n",
           label, h->count, mean_us, p50_us, p99_us, throughput_kops,
           (double)total_wall_ns / 1000000.0);
}
