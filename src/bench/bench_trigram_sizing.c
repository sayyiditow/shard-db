/* src/bench/bench_trigram_sizing.c — trigram index sizing + speedup.
 *
 * Three numbers a user adding a trigram index needs to know:
 *   1. How big does the .tg directory get?      (disk budget)
 *   2. How much does it slow bulk-insert?       (ingest cost)
 *   3. How much faster do contains queries get? (the payoff)
 *
 * Two objects with the same varchar schema (`body:varchar:1000`). One
 * declared with `body:trigram`, the other left as bare btree. Bulk-insert
 * N records with HN-comment-shaped bodies (English-ish character soup,
 * avg ~500 bytes, deterministic so re-runs are comparable), then run a
 * spread of contains queries on both. The section prints insert
 * throughput, on-disk size, and query latency side-by-side so the
 * trade-off is obvious.
 *
 * Scale: SHARD_BENCH_COUNT (default 100_000). Larger than that and the
 * btree side's full-scan latency dominates the bench run-time; smaller
 * and the trigram intersection hides in noise. 100K is the sweet spot
 * for ~30-second total run.
 *
 * Run:   ./build/bin/shard-db-bench run bench-trigram-sizing
 *        SHARD_BENCH_COUNT=1000000 ./build/bin/shard-db-bench run bench-trigram-sizing
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include "bench_table.h"
#include "bench_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
/* bench_du_bytes / bench_fmt_bytes — see bench_common.h. */

/* Deterministic HN-comment-shaped body: avg ~500 chars, distribution
   skewed toward shorter; uses a fixed dictionary of common words so
   query patterns ("fox", "the", "shard") match a predictable number
   of records. Length seeded from record id so two runs with the same
   count produce the same corpus. */
static const char *g_words[] = {
    "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
    "shard", "trigram", "index", "varchar", "btree", "search", "scan",
    "fast", "slow", "post", "comment", "title", "user", "score", "story",
    "hello", "world", "there", "here", "now", "later", "always", "never",
    "very", "quite", "almost", "really", "truly", "kind", "sort", "of",
    "data", "structure", "memory", "disk", "cache", "table", "row",
    "column", "value", "key", "field", "type", "code", "build", "test",
    "performance", "latency", "throughput", "scale", "production",
};
#define G_WORDS_N (sizeof(g_words) / sizeof(g_words[0]))

static int build_body(int id, char *out, size_t outlen) {
    /* Linear-congruential mix from id for a deterministic-but-varied
       word stream. Each body picks 30-160 words with id-controlled
       jitter — average ~95 words ≈ 500 chars. */
    uint32_t rng = (uint32_t)id * 2654435761u + 1u;
    int target_words = 30 + ((int)(rng >> 8) & 0x7F); /* 30..157 */
    size_t pos = 0;
    for (int i = 0; i < target_words; i++) {
        rng = rng * 1664525u + 1013904223u;
        const char *w = g_words[(rng >> 8) % G_WORDS_N];
        size_t wlen = strlen(w);
        if (pos + wlen + 2 >= outlen) break;
        if (pos > 0) out[pos++] = ' ';
        memcpy(out + pos, w, wlen);
        pos += wlen;
    }
    if (pos < outlen) out[pos] = '\0';
    return (int)pos;
}

static int create_object(TestClient *tc, const char *name, int with_trigram) {
    char req[512];
    char *resp = NULL;
    if (with_trigram) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"b\",\"object\":\"%s\","
            "\"splits\":32,\"max_key\":12,"
            "\"fields\":[\"body:varchar:2000\"],"
            "\"indexes\":[\"body:trigram\"]}", name);
    } else {
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"b\",\"object\":\"%s\","
            "\"splits\":32,\"max_key\":12,"
            "\"fields\":[\"body:varchar:2000\"]}", name);
    }
    tc_request(tc, req, &resp);
    int ok = resp && strstr(resp, "\"created\"") != NULL;
    free(resp);
    return ok ? 0 : -1;
}

static int bulk_insert_chunk(TestClient *tc, const char *object,
                              int start_id, int batch_size) {
    /* Each body up to ~600 bytes + 64 bytes per-record framing. */
    size_t cap = (size_t)batch_size * 700 + 256;
    char *buf = malloc(cap);
    if (!buf) return -1;

    size_t pos = 0;
    #define APPEND(...) do {                                          \
        if (pos + 1 >= cap) break;                                    \
        int _n = snprintf(buf + pos, cap - pos, __VA_ARGS__);         \
        if (_n < 0) { buf[pos] = '\0'; goto out; }                    \
        if ((size_t)_n >= cap - pos) { pos = cap - 1; buf[pos] = '\0'; goto out; } \
        pos += (size_t)_n;                                            \
    } while (0)

    APPEND("{\"mode\":\"bulk-insert\",\"dir\":\"b\",\"object\":\"%s\",\"records\":[",
           object);
    char body[1024];
    for (int i = 0; i < batch_size; i++) {
        int id = start_id + i;
        build_body(id, body, sizeof(body));
        APPEND("%s{\"key\":\"k%d\",\"value\":{\"body\":\"%s\"}}",
               i ? "," : "", id, body);
    }
    APPEND("]}");
out: ;
    #undef APPEND

    char *resp = NULL;
    tc_request(tc, buf, &resp);
    int ok = resp && strstr(resp, "\"inserted\"") != NULL;
    free(resp);
    free(buf);
    return ok ? 0 : -1;
}

static double run_insert(TestClient *tc, const char *object, int total) {
    const int chunk = 5000;
    uint64_t t0 = bench_now_ns();
    for (int s = 0; s < total; s += chunk) {
        int n = (total - s < chunk) ? (total - s) : chunk;
        bulk_insert_chunk(tc, object, s, n);
    }
    return (bench_now_ns() - t0) / 1e9;
}

static int bench_trigram_sizing_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }

    const char *count_env = getenv("SHARD_BENCH_COUNT");
    int count = count_env ? atoi(count_env) : 100000;
    if (count <= 0) count = 100000;

    fprintf(stderr, "\nbench-trigram-sizing — %d records each side (set SHARD_BENCH_COUNT to override)\n\n",
            count);

    /* ===== Setup ===== */
    if (create_object(tc, "with_tg", 1) != 0) {
        fprintf(stderr, "  create with_tg failed\n");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    if (create_object(tc, "no_idx", 0) != 0) {
        fprintf(stderr, "  create no_idx failed\n");
        tc_close(tc); test_env_stop(&env); return 1;
    }

    /* ===== Phase 1: insert throughput ===== */
    fprintf(stderr, "Phase 1: bulk-insert %d records into each object...\n", count);
    double t_no = run_insert(tc, "no_idx",  count);
    double t_tg = run_insert(tc, "with_tg", count);

    double rps_no = count / t_no;
    double rps_tg = count / t_tg;

    /* ===== Phase 2: on-disk size ===== */
    char tg_idx_dir[512], no_idx_dir[512];
    snprintf(tg_idx_dir, sizeof(tg_idx_dir),
             "%s/b/with_tg/indexes/body", env.db_root);
    snprintf(no_idx_dir, sizeof(no_idx_dir),
             "%s/b/no_idx/indexes/body",  env.db_root);
    long long tg_bytes = bench_du_bytes(tg_idx_dir);
    long long no_bytes = bench_du_bytes(no_idx_dir);   /* should be 0 (no index dir) */
    char tg_sz[32], no_sz[32];
    bench_fmt_bytes(tg_bytes, tg_sz, sizeof(tg_sz));
    bench_fmt_bytes(no_bytes, no_sz, sizeof(no_sz));

    fprintf(stderr,
        "\nResults:\n"
        "  insert throughput  (no-idx)  : %.0f rec/sec (%.2fs total)\n"
        "  insert throughput  (trigram) : %.0f rec/sec (%.2fs total)\n"
        "  insert overhead    (trigram) : %.2fx slower\n"
        "  on-disk .tg size             : %s\n"
        "  on-disk overhead per record  : %.1f bytes\n",
        rps_no, t_no, rps_tg, t_tg, t_no > 0 ? t_tg / t_no : 0.0,
        tg_sz, count > 0 ? (double)tg_bytes / count : 0.0);

    /* ===== Phase 3: read latency — contains queries ===== */
    bench_table_section_begin("CONTAINS — no-idx (full scan)");
    bench_table_run(tc, "contains 'fox'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"no_idx\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"fox\"}]}");
    bench_table_run(tc, "contains 'shard'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"no_idx\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"shard\"}]}");
    bench_table_run(tc, "contains 'performance'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"no_idx\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"performance\"}]}");
    bench_table_section_end();

    bench_table_section_begin("CONTAINS — trigram (.tg posting lists)");
    bench_table_run(tc, "contains 'fox'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"with_tg\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"fox\"}]}");
    bench_table_run(tc, "contains 'shard'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"with_tg\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"shard\"}]}");
    bench_table_run(tc, "contains 'performance'",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"with_tg\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"performance\"}]}");
    bench_table_section_end();

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-trigram-sizing", bench_trigram_sizing_run)
