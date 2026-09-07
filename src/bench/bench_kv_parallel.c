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
#include "bench_common.h"
#include "bench_table.h"
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
/* Runtime-resolved object shard count (SHARD_BENCH_SPLITS); reset_object
   runs outside bench_kv_parallel_run() so the env override lives here. */
static int g_bench_splits = SPLITS;
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

/* ------------------------------------------------- commit-phase counters
 * Diagnostic only: pulls the daemon's cumulative commit_* stats and prints
 * the delta since the last snapshot, so a single-connection bulk-insert's
 * time can be attributed to kf msync / segment sync / marker publish /
 * marker clear instead of guessed at. See docs stats field names in
 * server.c's "mode":"stats" JSON response.
 */
typedef struct {
    unsigned long count, lock_hold_us, sync_us, windows, mpub_us, mpub_n,
                  segsync_us, seg_p_us, seg_post_us,
                  idxsync_us, idxsync_n, mclear_us;
} CommitStats;

static unsigned long json_ulong_after(const char *hay, const char *key)
{
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(hay, pat);
    if (!p) return 0;
    return strtoul(p + strlen(pat), NULL, 10);
}

static void fetch_commit_stats(TestClient *tc, CommitStats *out)
{
    memset(out, 0, sizeof(*out));
    char *resp = NULL;
    if (tc_request(tc, "{\"mode\":\"stats\"}", &resp) != 0 || !resp) return;
    out->count        = json_ulong_after(resp, "count");
    out->lock_hold_us  = json_ulong_after(resp, "lock_hold_us_total");
    out->sync_us       = json_ulong_after(resp, "sync_us_total");
    out->windows       = json_ulong_after(resp, "windows_total");
    out->mpub_us       = json_ulong_after(resp, "marker_publish_us_total");
    out->mpub_n        = json_ulong_after(resp, "marker_publish_count");
    out->segsync_us    = json_ulong_after(resp, "segment_sync_us_total");
    out->seg_p_us      = json_ulong_after(resp, "segment_p_us_total");
    out->seg_post_us   = json_ulong_after(resp, "segment_post_us_total");
    out->idxsync_us    = json_ulong_after(resp, "index_sync_us_total");
    out->idxsync_n     = json_ulong_after(resp, "index_sync_ops_total");
    out->mclear_us     = json_ulong_after(resp, "marker_clear_us_total");
    free(resp);
}

static void print_commit_stats_delta(const char *label,
                                     const CommitStats *base,
                                     const CommitStats *cur)
{
    printf("  [%s] commit: requests=%lu  lock_hold_us=%lu  kf_msync_us=%lu  windows=%lu\n"
           "  [%s] marker_publish: n=%lu us=%lu   segment_sync_us=%lu "
           "(pre_marker_p=%lu post_marker=%lu)   "
           "index_sync: n=%lu us=%lu   marker_clear_us=%lu\n",
           label,
           cur->count - base->count,
           cur->lock_hold_us - base->lock_hold_us,
           cur->sync_us - base->sync_us,
           cur->windows - base->windows,
           label,
           cur->mpub_n - base->mpub_n, cur->mpub_us - base->mpub_us,
           cur->segsync_us - base->segsync_us,
           cur->seg_p_us - base->seg_p_us,
           cur->seg_post_us - base->seg_post_us,
           cur->idxsync_n - base->idxsync_n, cur->idxsync_us - base->idxsync_us,
           cur->mclear_us - base->mclear_us);
}

/* -------------------------------------------------------- memfd helper */

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
        "\"splits\":%d,\"max_key\":%d,"
        "\"fields\":[\"v:varchar:%d\"]}",
        g_bench_splits, KEY_LEN, VAL_LEN);
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
    /* Resolve TOTAL / CHUNK from env vars (defaults match the bash bench).
       Connection count = NCHUNKS (one conn per chunk; CHUNK=500000 with
       TOTAL=1000000 gives a 2-conn run). SHARD_BENCH_SPLITS overrides the
       object's shard count so K-phase cost can be measured at
       production-typical geometry (8) vs the wide default (128). */
    const char *total_env = getenv("SHARD_BENCH_TOTAL");
    const char *chunk_env = getenv("SHARD_BENCH_CHUNK");
    const char *splits_env = getenv("SHARD_BENCH_SPLITS");
    int TOTAL = total_env ? atoi(total_env) : DEFAULT_TOTAL;
    int CHUNK = chunk_env ? atoi(chunk_env) : DEFAULT_CHUNK;
    int SPLITS_N = splits_env ? atoi(splits_env) : SPLITS;
    if (TOTAL <= 0) TOTAL = DEFAULT_TOTAL;
    if (CHUNK <= 0) CHUNK = DEFAULT_CHUNK;
    if (CHUNK > TOTAL) CHUNK = TOTAL;
    if (SPLITS_N < 8 || SPLITS_N > 4096 ||
        (SPLITS_N & (SPLITS_N - 1)) != 0)
        SPLITS_N = SPLITS;   /* splits must be a power of 2 in [8, 4096] */
    g_bench_splits = SPLITS_N;
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
        "\"splits\":%d,\"max_key\":%d,"
        "\"fields\":[\"v:varchar:%d\"]}",
        g_bench_splits, KEY_LEN, VAL_LEN);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    printf("======================================\n");
    printf("  K/V parallel bench: total=%d chunk=%d conns=%d splits=%d\n",
           TOTAL, CHUNK, NCHUNKS, g_bench_splits);
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
        chunk_json[c] = bench_make_memfd("kv-par-json", buf, sz);
        free(buf);

        if (build_csv_chunk(from, to, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building CSV chunk %d\n", c);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        chunk_csv[c] = bench_make_memfd("kv-par-csv", buf, sz);
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
        int single_json = bench_make_memfd("kv-par-json-single", buf, sz);
        free(buf);

        if (build_csv_chunk(0, TOTAL, &buf, &sz) != 0) {
            fprintf(stderr, "bench-kv-parallel: OOM building single CSV blob\n");
            close(single_json);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  single CSV:  %.1f MB\n\n", (double)sz / 1048576.0);
        int single_csv = bench_make_memfd("kv-par-csv-single", buf, sz);
        free(buf);

        long us_1a = 0, us_1b = 0, us_2 = 0, us_3 = 0;
        CommitStats cs_before = {0}, cs_after = {0};

        /* TEST 1a: single JSON */
        reset_object(tc);
        fetch_commit_stats(tc, &cs_before);
        { uint64_t t0 = bench_now_ns();
          do_bulk_insert(tc, "json", single_json);
          us_1a = (long)((bench_now_ns() - t0) / 1000); }
        close(single_json);
        fetch_commit_stats(tc, &cs_after);
        print_commit_stats_delta("single JSON", &cs_before, &cs_after);

        /* TEST 1b: single CSV */
        reset_object(tc);
        fetch_commit_stats(tc, &cs_before);
        { uint64_t t0 = bench_now_ns();
          do_bulk_insert(tc, "csv", single_csv);
          us_1b = (long)((bench_now_ns() - t0) / 1000); }
        close(single_csv);
        fetch_commit_stats(tc, &cs_after);
        print_commit_stats_delta("single CSV ", &cs_before, &cs_after);

        /* TEST 2: parallel JSON */
        reset_object(tc);
        fetch_commit_stats(tc, &cs_before);
        { uint64_t t0 = bench_now_ns();
          run_parallel(env.port, "json", chunk_json, NCHUNKS);
          us_2 = (long)((bench_now_ns() - t0) / 1000); }
        fetch_commit_stats(tc, &cs_after);
        print_commit_stats_delta("par JSON    ", &cs_before, &cs_after);

        /* TEST 3: parallel CSV */
        reset_object(tc);
        fetch_commit_stats(tc, &cs_before);
        { uint64_t t0 = bench_now_ns();
          run_parallel(env.port, "csv", chunk_csv, NCHUNKS);
          us_3 = (long)((bench_now_ns() - t0) / 1000); }
        fetch_commit_stats(tc, &cs_after);
        print_commit_stats_delta("par CSV     ", &cs_before, &cs_after);

        char e1a[48], e1b[48], e2[48], e3[48], lbl2[64], lbl3[64];
        snprintf(e1a, sizeof(e1a), "%.2f M rows/s",
                 (double)TOTAL / 1e6 / ((double)us_1a / 1e6));
        snprintf(e1b, sizeof(e1b), "%.2f M rows/s",
                 (double)TOTAL / 1e6 / ((double)us_1b / 1e6));
        snprintf(e2, sizeof(e2), "%.2f M rows/s",
                 (double)TOTAL / 1e6 / ((double)us_2 / 1e6));
        snprintf(e3, sizeof(e3), "%.2f M rows/s",
                 (double)TOTAL / 1e6 / ((double)us_3 / 1e6));
        snprintf(lbl2, sizeof(lbl2), "parallel JSON  %d conns × %d", NCHUNKS, CHUNK);
        snprintf(lbl3, sizeof(lbl3), "parallel CSV   %d conns × %d", NCHUNKS, CHUNK);

        bench_table_section_begin("BULK INSERT throughput (single vs parallel)");
        bench_table_record("single JSON   1 file", us_1a, 1, e1a);
        bench_table_record("single CSV    1 file", us_1b, 1, e1b);
        bench_table_record(lbl2, us_2, 1, e2);
        bench_table_record(lbl3, us_3, 1, e3);
        bench_table_section_end();
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
        char path[512];
        snprintf(path, sizeof(path), "%s/default/kvbench", env.db_root);
        char *argv[] = { (char *)"du", (char *)"-sh", path, NULL };
        printf("\nDisk usage: ");
        fflush(stdout);
        bench_safe_exec(argv);
    }

    printf("\n======================================\n");
    printf("  K/V parallel bench complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-kv-parallel", bench_kv_parallel_run)
