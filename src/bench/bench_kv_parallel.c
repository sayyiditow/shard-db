/* src/bench/bench_kv_parallel.c — port of bench/bench-kv-parallel.sh
 *
 * Same K/V schema as bench_kv (16B hex key, varchar(100) value), but
 * exercises the parallel-conn path: 1M records split into 5 chunks of
 * 200K, each fed to its own TestClient on its own pthread. Produces
 * 4 measurements: single JSON, single CSV, parallel JSON, parallel CSV.
 * Final shard-stats summary tells you whether load is balanced and how
 * many grows fired.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include <openssl/sha.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KEY_LEN  16
#define VAL_LEN 100
#define DEFAULT_TOTAL   1000000
#define DEFAULT_CHUNK    200000
#define CONNS   5
#define SPLITS  128
/* TOTAL / CHUNK / NCHUNKS are now runtime-configurable via env vars
   SHARD_BENCH_TOTAL and SHARD_BENCH_CHUNK; resolved at the top of
   bench_kv_parallel_run(). NCHUNKS = TOTAL / CHUNK. */

/* ------------------------------------------------------------------ helpers */

/* sha256(decimal-i)[:16] hex — matches the bash bench's python loader. */
static void make_key(int i, char out[KEY_LEN + 1])
{
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%d", i);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)buf, (size_t)n, digest);
    static const char hex[] = "0123456789abcdef";
    for (int j = 0; j < KEY_LEN / 2; j++) {
        out[2 * j]     = hex[digest[j] >> 4];
        out[2 * j + 1] = hex[digest[j] & 0xF];
    }
    out[KEY_LEN] = '\0';
}

/* "val_<i>" padded to VAL_LEN chars with 'x'. */
static void make_val(int i, char out[VAL_LEN + 1])
{
    char tmp[VAL_LEN + 32];
    int n = snprintf(tmp, sizeof(tmp), "val_%d", i);
    if (n > VAL_LEN) n = VAL_LEN;
    memcpy(out, tmp, (size_t)n);
    for (int j = n; j < VAL_LEN; j++) out[j] = 'x';
    out[VAL_LEN] = '\0';
}

/* -------------------------------------------------------- memfd helper */

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
    char path[] = "/tmp/bench-kv-par-XXXXXX";
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

/* ----------------------------------------- chunk buffer builders */

/*
 * Build JSON array for records [from_i, to_i) into *out_buf / *out_size.
 * Caller must free(*out_buf).
 */
static int build_json_chunk(int from_i, int to_i,
                             char **out_buf, size_t *out_size)
{
    int count = to_i - from_i;
    /* Each record: {"key":"<16>","value":{"v":"<100>"}}  ~= 142 chars + comma */
    size_t cap = (size_t)count * 144 + 8;
    char *buf = malloc(cap);
    if (!buf) return -1;

    size_t pos = 0;
    buf[pos++] = '[';
    char k[KEY_LEN + 1], v[VAL_LEN + 1];
    for (int i = from_i; i < to_i; i++) {
        make_key(i, k);
        make_val(i, v);
        int n = snprintf(buf + pos, cap - pos,
                         "%s{\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
                         i > from_i ? "," : "", k, v);
        if (n <= 0 || (size_t)n >= cap - pos) {
            free(buf);
            return -1;
        }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    buf[pos]   = '\0';
    *out_buf  = buf;
    *out_size = pos;
    return 0;
}

/*
 * Build CSV "<key>,<val>\n" for records [from_i, to_i) into *out_buf / *out_size.
 * Caller must free(*out_buf).
 */
static int build_csv_chunk(int from_i, int to_i,
                            char **out_buf, size_t *out_size)
{
    int count = to_i - from_i;
    /* Each line: 16 + 1 + 100 + 1 = 118 bytes */
    size_t cap = (size_t)count * 120 + 8;
    char *buf = malloc(cap);
    if (!buf) return -1;

    size_t pos = 0;
    char k[KEY_LEN + 1], v[VAL_LEN + 1];
    for (int i = from_i; i < to_i; i++) {
        make_key(i, k);
        make_val(i, v);
        int n = snprintf(buf + pos, cap - pos, "%s,%s\n", k, v);
        if (n <= 0 || (size_t)n >= cap - pos) {
            free(buf);
            return -1;
        }
        pos += (size_t)n;
    }
    buf[pos] = '\0';
    *out_buf  = buf;
    *out_size = pos;
    return 0;
}

/* -------------------------------------------------------- single bulk-insert */

static int do_bulk_insert(TestClient *tc, const char *mode, int memfd)
{
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/fd/%d", (int)getpid(), memfd);

    char req[256];
    if (strcmp(mode, "json") == 0) {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"kvbench\",\"file\":\"%s\"}", path);
    } else {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
                 "\"object\":\"kvbench\",\"file\":\"%s\",\"delimiter\":\",\"}", path);
    }

    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    free(resp);
    return rc;
}

/* Truncate + re-create the object for a clean test. */
static void reset_object(TestClient *tc)
{
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"kvbench\"}",
        &resp);
    free(resp);

    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kvbench\","
        "\"splits\":%d,\"max_key\":%d,\"fields\":[\"v:varchar:%d\"]}",
        SPLITS, KEY_LEN, VAL_LEN);
    tc_request(tc, req, &resp);
    free(resp);
}

/* ---------------------------------------------------- parallel workers */

typedef struct {
    int         port;
    int         memfd;
    const char *mode;   /* "json" or "csv" */
    int         rc;
} ParallelArg;

static void *parallel_worker(void *vp)
{
    ParallelArg *w = vp;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->rc = -1; return NULL; }
    w->rc = do_bulk_insert(tc, w->mode, w->memfd);
    tc_close(tc);
    return NULL;
}

static void run_parallel(int port, const char *mode, int *chunk_memfds, int nchunks)
{
    pthread_t   *tids = malloc((size_t)nchunks * sizeof(pthread_t));
    ParallelArg *args = malloc((size_t)nchunks * sizeof(ParallelArg));
    if (!tids || !args) { free(tids); free(args); return; }

    for (int i = 0; i < nchunks; i++) {
        args[i].port  = port;
        args[i].memfd = chunk_memfds[i];
        args[i].mode  = mode;
        args[i].rc    = 0;
        pthread_create(&tids[i], NULL, parallel_worker, &args[i]);
    }
    for (int i = 0; i < nchunks; i++)
        pthread_join(tids[i], NULL);
    free(tids); free(args);
}

/* ---------------------------------------------------------------- main bench */

static int bench_kv_parallel_run(void)
{
    /* Resolve TOTAL / CHUNK from env vars (defaults match the bash bench). */
    const char *total_env = getenv("SHARD_BENCH_TOTAL");
    const char *chunk_env = getenv("SHARD_BENCH_CHUNK");
    int TOTAL = total_env ? atoi(total_env) : DEFAULT_TOTAL;
    int CHUNK = chunk_env ? atoi(chunk_env) : DEFAULT_CHUNK;
    if (TOTAL <= 0) TOTAL = DEFAULT_TOTAL;
    if (CHUNK <= 0) CHUNK = DEFAULT_CHUNK;
    if (CHUNK > TOTAL) CHUNK = TOTAL;
    int NCHUNKS = (TOTAL + CHUNK - 1) / CHUNK;

    /* Spawn daemon. */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-kv-parallel: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 300000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        fprintf(stderr, "bench-kv-parallel: connect failed\n");
        test_env_stop(&env);
        return 1;
    }

    /* Register "default" tenant + initial object. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kvbench\","
        "\"splits\":%d,\"max_key\":%d,\"fields\":[\"v:varchar:%d\"]}",
        SPLITS, KEY_LEN, VAL_LEN);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    printf("======================================\n");
    printf("  K/V parallel bench: total=%d chunk=%d conns=%d splits=%d\n",
           TOTAL, CHUNK, CONNS, SPLITS);
    printf("======================================\n\n");

    /* Build all chunk memfds + a combined single-blob memfd for tests 1a/1b. */
    printf("Generating data blobs...\n");
    fflush(stdout);

    int *chunk_json = malloc((size_t)NCHUNKS * sizeof(int));
    int *chunk_csv  = malloc((size_t)NCHUNKS * sizeof(int));
    if (!chunk_json || !chunk_csv) {
        fprintf(stderr, "bench-kv-parallel: OOM allocating chunk fd arrays\n");
        free(chunk_json); free(chunk_csv);
        tc_close(tc); test_env_stop(&env); return 1;
    }
    for (int c = 0; c < NCHUNKS; c++) {
        int from = c * CHUNK;
        int to   = from + CHUNK;

        char *buf = NULL; size_t sz = 0;
        if (build_json_chunk(from, to, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building JSON chunk %d\n", c);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        chunk_json[c] = make_memfd("kv-par-json", buf, sz);
        free(buf);

        if (build_csv_chunk(from, to, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building CSV chunk %d\n", c);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        chunk_csv[c] = make_memfd("kv-par-csv", buf, sz);
        free(buf);
    }

    /* Single combined blobs for the baseline tests. */
    {
        char *buf = NULL; size_t sz = 0;
        if (build_json_chunk(0, TOTAL, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building single JSON blob\n");
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  single JSON: %.1f MB\n", (double)sz / 1048576.0);
        int single_json = make_memfd("kv-par-json-single", buf, sz);
        free(buf);

        if (build_csv_chunk(0, TOTAL, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building single CSV blob\n");
            close(single_json);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  single CSV:  %.1f MB\n\n", (double)sz / 1048576.0);
        int single_csv = make_memfd("kv-par-csv-single", buf, sz);
        free(buf);

        uint64_t t0, t1;

        /* TEST 1a: single JSON */
        printf("--- TEST 1a: single JSON, %d records ---\n", TOTAL);
        fflush(stdout);
        reset_object(tc);
        t0 = bench_now_ns();
        do_bulk_insert(tc, "json", single_json);
        t1 = bench_now_ns();
        printf("Test 1a single JSON  %d rows: wall=%.2fs  throughput=%.2f M/sec\n\n",
               TOTAL,
               (double)(t1 - t0) / 1e9,
               (double)TOTAL / 1e6 / ((double)(t1 - t0) / 1e9));
        close(single_json);

        /* TEST 1b: single CSV */
        printf("--- TEST 1b: single CSV, %d records ---\n", TOTAL);
        fflush(stdout);
        reset_object(tc);
        t0 = bench_now_ns();
        do_bulk_insert(tc, "csv", single_csv);
        t1 = bench_now_ns();
        printf("Test 1b single CSV   %d rows: wall=%.2fs  throughput=%.2f M/sec\n\n",
               TOTAL,
               (double)(t1 - t0) / 1e9,
               (double)TOTAL / 1e6 / ((double)(t1 - t0) / 1e9));
        close(single_csv);
    }

    {
        uint64_t t0, t1;

        /* TEST 2: parallel JSON */
        printf("--- TEST 2: parallel JSON, %d conns × %d ---\n", NCHUNKS, CHUNK);
        fflush(stdout);
        reset_object(tc);
        t0 = bench_now_ns();
        run_parallel(env.port, "json", chunk_json, NCHUNKS);
        t1 = bench_now_ns();
        printf("Test 2 parallel JSON %d conns × %d: wall=%.2fs  throughput=%.2f M/sec\n\n",
               NCHUNKS, CHUNK,
               (double)(t1 - t0) / 1e9,
               (double)TOTAL / 1e6 / ((double)(t1 - t0) / 1e9));

        /* TEST 3: parallel CSV */
        printf("--- TEST 3: parallel CSV, %d conns × %d ---\n", NCHUNKS, CHUNK);
        fflush(stdout);
        reset_object(tc);
        t0 = bench_now_ns();
        run_parallel(env.port, "csv", chunk_csv, NCHUNKS);
        t1 = bench_now_ns();
        printf("Test 3 parallel CSV  %d conns × %d: wall=%.2fs  throughput=%.2f M/sec\n\n",
               NCHUNKS, CHUNK,
               (double)(t1 - t0) / 1e9,
               (double)TOTAL / 1e6 / ((double)(t1 - t0) / 1e9));
    }

    /* Cleanup chunk memfds. */
    for (int c = 0; c < NCHUNKS; c++) {
        close(chunk_json[c]);
        close(chunk_csv[c]);
    }
    free(chunk_json); free(chunk_csv);

    /* Shard-stats summary. */
    printf("--- shard-stats ---\n");
    tc_request(tc,
        "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"kvbench\"}",
        &resp);
    if (resp) {
        size_t len = strlen(resp);
        if (len > 600) printf("%.600s...\n", resp);
        else           printf("%s\n", resp);
        free(resp); resp = NULL;
    }

    /* Disk usage. */
    {
        char du_cmd[512];
        snprintf(du_cmd, sizeof(du_cmd), "du -sh \"%s/default/kvbench\"",
                 env.db_root);
        printf("\nDisk usage: ");
        fflush(stdout);
        system(du_cmd);
    }

    printf("\n======================================\n");
    printf("  K/V parallel bench complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-kv-parallel", bench_kv_parallel_run)
