/* src/bench/bench_incremental.c — port of bench/bench-incremental.sh
 *
 * Find the rebuild-vs-point-insert crossover empirically. SHARDKV_BULK_RATIO
 * is read by the SERVER at startup, so we tear down + respawn the daemon
 * once per strategy:
 *   ratio=0 → always REBUILD path
 *   ratio=1 → always POINT-INSERT path
 *
 * For each strategy: spin up daemon, insert 900K-record baseline, then
 * for each small-batch size in {1k, 10k, 50k, 100k, 200k}, measure
 * "insert N more + delete those N" round-trip. Best-of-3 per batch size.
 *
 * Output is the comparison table the bash bench prints, telling us
 * which strategy wins at which batch size.
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
#include <sys/syscall.h>
#include <unistd.h>

#define BASELINE 900000
static const int BATCHES[] = { 1000, 10000, 50000, 100000, 200000 };
#define N_BATCHES (int)(sizeof(BATCHES) / sizeof(BATCHES[0]))

static int make_memfd(const char *name, const char *data, size_t size) {
#if defined(__x86_64__)
    long sysno = 319;
#elif defined(__aarch64__)
    long sysno = 279;
#else
# error "memfd_create syscall number unknown for this arch"
#endif
    int fd = (int)syscall(sysno, name, 0u);
    if (fd < 0) return -1;
    size_t written = 0;
    while (written < size) {
        ssize_t n = write(fd, data + written, size - written);
        if (n <= 0) { close(fd); return -1; }
        written += (size_t)n;
    }
    return fd;
}

/* Build INV-NNNNNNNN range [from, to) as a JSON array bulk-insert payload.
   Each record: {"key":"INV-00000123","value":{"status":..,"region":..,"amount":..}} */
static int build_inc_range(int from, int to, char **out_buf, size_t *out_size) {
    static const char *const STATUSES[] = {"DRAFT","PENDING","APPROVED","REJECTED"};
    static const char *const REGIONS[]  = {"EU","US","APAC","LATAM","ME"};
    int count = to - from;
    size_t cap = (size_t)count * 130 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int i = from; i < to; i++) {
        if (pos + 130 > cap) {
            cap = pos + (size_t)(to - i) * 130 + 16;
            char *t = realloc(buf, cap);
            if (!t) { free(buf); return -1; }
            buf = t;
        }
        int n = snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"INV-%08d\",\"value\":{"
              "\"status\":\"%s\",\"region\":\"%s\",\"amount\":%.1f"
            "}}",
            i > from ? "," : "", i,
            STATUSES[i % 4], REGIONS[i % 5],
            (double)i * 1.5);
        if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    *out_buf = buf; *out_size = pos;
    return 0;
}

/* Build a JSON array of just the keys [from, to) for bulk-delete. */
static int build_del_keys(int from, int to, char **out_buf, size_t *out_size) {
    int count = to - from;
    size_t cap = (size_t)count * 16 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t pos = 0;
    buf[pos++] = '[';
    for (int i = from; i < to; i++) {
        int n = snprintf(buf + pos, cap - pos,
            "%s\"INV-%08d\"", i > from ? "," : "", i);
        if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    *out_buf = buf; *out_size = pos;
    return 0;
}

/* Run one full strategy: spawn daemon, insert baseline, run all batch
   sizes (best-of-3), record the winning time per batch. results[]
   must be sized N_BATCHES. */
static int run_strategy(const char *label, const char *ratio_value,
                        double *results) {
    /* Set SHARDKV_BULK_RATIO before spawning daemon — child inherits env. */
    setenv("SHARDKV_BULK_RATIO", ratio_value, 1);

    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-incremental: daemon spawn failed (strategy=%s)\n", label);
        return -1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return -1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"inc\","
        "\"splits\":64,\"max_key\":32,"
        "\"fields\":[\"status:varchar:16\",\"region:varchar:16\",\"amount:double\"],"
        "\"indexes\":[\"status\",\"region\"]}",
        &resp);
    free(resp); resp = NULL;

    printf("\n=== STRATEGY: %s (SHARDKV_BULK_RATIO=%s) ===\n", label, ratio_value);

    /* Insert baseline. */
    {
        char *buf = NULL; size_t sz = 0;
        if (build_inc_range(0, BASELINE, &buf, &sz) != 0) {
            tc_close(tc); test_env_stop(&env); return -1;
        }
        int fd = make_memfd("inc-baseline", buf, sz);
        free(buf);
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"inc\","
            "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), fd);
        uint64_t t0 = bench_now_ns();
        tc_request(tc, req, &resp);
        uint64_t t1 = bench_now_ns();
        printf("  baseline insert (%d): %.3fs resp=%s\n", BASELINE,
               (double)(t1 - t0) / 1e9, resp ? resp : "(null)");
        free(resp); resp = NULL;
        close(fd);
    }

    /* Best-of-3 per batch size. */
    for (int b = 0; b < N_BATCHES; b++) {
        int N = BATCHES[b];
        double best = 1e18;

        /* Pre-build the insert payload once per batch (reusable across iters). */
        char *ins_buf = NULL; size_t ins_sz = 0;
        if (build_inc_range(BASELINE, BASELINE + N, &ins_buf, &ins_sz) != 0) {
            fprintf(stderr, "bench-incremental: OOM batch=%d\n", N);
            continue;
        }
        char *del_buf = NULL; size_t del_sz = 0;
        if (build_del_keys(BASELINE, BASELINE + N, &del_buf, &del_sz) != 0) {
            fprintf(stderr, "bench-incremental: OOM del batch=%d\n", N);
            free(ins_buf); continue;
        }

        for (int iter = 0; iter < 3; iter++) {
            int ins_fd = make_memfd("inc-ins", ins_buf, ins_sz);
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"inc\","
                "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), ins_fd);

            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            uint64_t t1 = bench_now_ns();
            close(ins_fd);
            free(resp); resp = NULL;

            double secs = (double)(t1 - t0) / 1e9;
            if (secs < best) best = secs;

            /* Remove added records so the next iteration starts from baseline. */
            int del_fd = make_memfd("inc-del", del_buf, del_sz);
            snprintf(req, sizeof(req),
                "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"inc\","
                "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), del_fd);
            tc_request(tc, req, &resp);
            free(resp); resp = NULL;
            close(del_fd);
        }

        free(ins_buf); free(del_buf);
        results[b] = best;
        printf("    batch=%-7d %s best-of-3: %.3fs\n", N, label, best);
    }

    tc_close(tc);
    test_env_stop(&env);
    /* Don't unset the env var here — the next strategy overwrites it. */
    return 0;
}

static int bench_incremental_run(void) {
    double rebuild_times[N_BATCHES] = {0};
    double point_times[N_BATCHES]   = {0};

    printf("======================================\n");
    printf("  Incremental bulk-insert crossover\n");
    printf("  baseline=%d  batches=", BASELINE);
    for (int b = 0; b < N_BATCHES; b++) printf("%s%d", b ? "," : "", BATCHES[b]);
    printf("\n======================================\n");

    if (run_strategy("REBUILD", "0", rebuild_times) != 0) return 1;
    if (run_strategy("POINT",   "1", point_times)   != 0) return 1;

    printf("\n======================================\n");
    printf("  SUMMARY (best of 3 per batch size)\n");
    printf("======================================\n");
    printf("%-12s %-14s %-14s %-10s\n", "batch", "REBUILD(s)", "POINT(s)", "winner");
    for (int b = 0; b < N_BATCHES; b++) {
        const char *winner = (point_times[b] < rebuild_times[b]) ? "POINT" : "REBUILD";
        char rbuf[16], pbuf[16], nbuf[16];
        snprintf(rbuf, sizeof(rbuf), "%.3f", rebuild_times[b]);
        snprintf(pbuf, sizeof(pbuf), "%.3f", point_times[b]);
        snprintf(nbuf, sizeof(nbuf), "%d", BATCHES[b]);
        printf("%-12s %-14s %-14s %-10s\n", nbuf, rbuf, pbuf, winner);
    }

    /* Clean up env so subsequent benches in run-all don't inherit. */
    unsetenv("SHARDKV_BULK_RATIO");
    return 0;
}

TEST_REGISTER("bench-incremental", bench_incremental_run)
