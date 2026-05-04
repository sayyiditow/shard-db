/* src/bench/bench_grow.c — port of bench/bench-grow.sh
 *
 * Two scenarios that exercise different shard-grow regimes:
 *   - heavy-grow:  8 splits × 5M rows (was many grows pre-2026.05.x)
 *   - light-grow: 16 splits × 1M rows (typical production shape)
 *
 * Both use bulk-insert-delimited (CSV) with the input streamed from a
 * memfd to skip on-disk staging. Pre-grow makes both scenarios land on
 * ~1 grow per shard now; the bench just reports wall time so changes
 * to the grow path are visible regardless.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include "bench_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* ------------------------------------------------ memfd bulk-insert helper */

/*
 * Write data into a Linux memfd, return the open fd (caller closes).
 * The daemon child reads it via /proc/<bench-pid>/fd/<n>.
 */
static int make_memfd(const char *name, const char *data, size_t size)
{
    /* memfd_create syscall number on x86_64 = 319, aarch64 = 279 */
#if defined(__x86_64__)
    int fd = (int)syscall(319, name, 0);
#elif defined(__aarch64__)
    int fd = (int)syscall(279, name, 0);
#else
    /* Fallback: anonymous tmpfs via mkstemp */
    char path[] = "/tmp/bench-grow-XXXXXX";
    int fd = mkstemp(path);
    if (fd >= 0) unlink(path);
#endif
    if (fd < 0) { perror("memfd_create"); return -1; }

    size_t written = 0;
    while (written < size) {
        ssize_t r = write(fd, data + written, size - written);
        if (r <= 0) { perror("write memfd"); close(fd); return -1; }
        written += (size_t)r;
    }
    if (lseek(fd, 0, SEEK_SET) != 0) { perror("lseek"); close(fd); return -1; }
    return fd;
}

/* ---------------------------------------------------------------- scenario */

static void run_scenario(TestClient *tc, const char *label, int splits, int rows,
                         const char *obj)
{
    printf("\n--- scenario: %s  (splits=%d, rows=%d, obj=%s) ---\n",
           label, splits, rows, obj);
    fflush(stdout);

    /* Create object. */
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":%d,\"max_key\":24,"
        "\"fields\":[\"v:varchar:128\"]}", obj, splits);

    char *resp = NULL;
    if (tc_request(tc, req, &resp) != 0) {
        fprintf(stderr, "%s: create-object failed\n", label);
        free(resp);
        return;
    }
    free(resp);
    resp = NULL;

    /* Build CSV "kNNNNN,vNNNNN\n" × rows in a memfd. */
    printf("Generating CSV (%d rows)...\n", rows);
    fflush(stdout);

    size_t csv_cap  = (size_t)rows * 30 + 8;  /* conservative estimate */
    char  *csv_buf  = malloc(csv_cap);
    if (!csv_buf) {
        fprintf(stderr, "%s: OOM csv_buf\n", label);
        return;
    }

    size_t csv_size = 0;
    for (int i = 0; i < rows; i++) {
        int n = snprintf(csv_buf + csv_size, csv_cap - csv_size,
                         "k%010d,v%010d\n", i, i);
        if (n <= 0 || (size_t)n >= csv_cap - csv_size) {
            fprintf(stderr, "%s: CSV overflow at i=%d\n", label, i);
            free(csv_buf);
            return;
        }
        csv_size += (size_t)n;
    }
    printf("  CSV: %.1f MB\n\n", (double)csv_size / 1048576.0);

    /* Write to memfd. */
    int memfd = make_memfd("bench-grow", csv_buf, csv_size);
    free(csv_buf);
    csv_buf = NULL;

    if (memfd < 0) {
        fprintf(stderr, "%s: memfd_create failed\n", label);
        return;
    }

    char fdpath[64];
    snprintf(fdpath, sizeof(fdpath),
             "/proc/%d/fd/%d", (int)getpid(), memfd);

    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
        "\"object\":\"%s\",\"file\":\"%s\",\"delimiter\":\",\"}",
        obj, fdpath);

    uint64_t t0 = bench_now_ns();
    if (tc_request(tc, req, &resp) != 0) {
        fprintf(stderr, "%s: bulk-insert-delimited failed\n", label);
        close(memfd);
        free(resp);
        return;
    }
    long us = (long)((bench_now_ns() - t0) / 1000);
    close(memfd);
    free(resp); resp = NULL;

    char extra[64];
    snprintf(extra, sizeof(extra), "%d rows  %.2f M/sec",
             rows, (double)rows / 1e6 / ((double)us / 1e6));
    bench_table_record(label, us, 1, extra);

    /* Final shard-stats summary stays as raw printf — large JSON blob,
       not a per-row metric. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    if (tc_request(tc, req, &resp) == 0) {
        size_t len = resp ? strlen(resp) : 0;
        if (len > 800) {
            printf("\n--- final shard-stats (%s) ---\n", obj);
            printf("%.800s...\n", resp);
        } else if (len > 0) {
            printf("\n--- final shard-stats (%s) ---\n", obj);
            printf("%s\n", resp);
        }
        free(resp);
        resp = NULL;
    }
}

/* ---------------------------------------------------------------- main bench */

static int bench_grow_run(void)
{
    printf("======================================\n");
    printf("  shard-db grow benchmark\n");
    printf("======================================\n\n");

    /* Spawn daemon. */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-grow: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        fprintf(stderr, "bench-grow: connect failed\n");
        test_env_stop(&env);
        return 1;
    }

    /* Register "default" tenant. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp);
    resp = NULL;

    /* Run scenarios — both rows recorded into one BULK INSERT section so
       the bar chart compares heavy vs light side-by-side. */
    bench_table_section_begin("BULK INSERT (CSV, single conn) — splits sweep");
    run_scenario(tc, "heavy-grow  splits=8   rows=5M",  8,  5000000, "heavy");
    run_scenario(tc, "light-grow  splits=16  rows=1M", 16, 1000000, "light");
    bench_table_section_end();

    printf("\n");
    printf("======================================\n");
    printf("  grow benchmark complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-grow", bench_grow_run)
