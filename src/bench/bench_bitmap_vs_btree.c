/* src/bench/bench_bitmap_vs_btree.c — head-to-head latency comparison.
 *
 * Two objects with the same schema (`flag:bool`). One declared
 * `flag:bitmap` (auto-default behaviour, made explicit for clarity),
 * the other `flag:btree`. Bulk-insert N identical records into each,
 * then run count + find queries `flag eq false` / `flag eq true` on
 * both, sweeping `limit ∈ {1000, 100000, 1000000}` so the crossover
 * is visible: btree narrowly wins low-limit (less file-open setup),
 * bitmap wins mid/high-limit (ffs-over-uint64 beats leaf-walk). The
 * section formatter prints the rows side-by-side with a relative
 * bar chart that makes the bitmap win obvious.
 *
 * Scale: SHARD_BENCH_COUNT (default 1_000_000). At 1M records the
 * gap between bitmap popcount and btree walk is large enough to
 * dominate variance.
 *
 * Run:   ./build/bin/shard-db-bench run bench-bitmap-vs-btree
 *        SHARD_BENCH_COUNT=10000000 ./build/bin/shard-db-bench run bench-bitmap-vs-btree
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include "bench_table.h"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Recursive total bytes under a path. Used to compare on-disk footprint
   of bitmap vs btree index dirs. */
static long long du_bytes(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode)) return (long long)st.st_size;
        return 0;
    }
    long long total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char sub[1024];
        snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        struct stat st;
        if (stat(sub, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) total += du_bytes(sub);
        else if (S_ISREG(st.st_mode)) total += (long long)st.st_size;
    }
    closedir(d);
    return total;
}

static void fmt_bytes(long long b, char *out, size_t outlen) {
    if (b < 1024)             snprintf(out, outlen, "%lld B",   b);
    else if (b < 1024 * 1024) snprintf(out, outlen, "%.1f KB",  b / 1024.0);
    else if (b < 1LL << 30)   snprintf(out, outlen, "%.1f MB",  b / (1024.0 * 1024));
    else                      snprintf(out, outlen, "%.2f GB",  b / (1024.0 * 1024 * 1024));
}

static int create_object(TestClient *tc, const char *name, const char *type) {
    char req[512];
    char *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"b\",\"object\":\"%s\","
        "\"splits\":64,\"max_key\":12,"
        "\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:%s\"]}",
        name, type);
    tc_request(tc, req, &resp);
    int ok = resp && strstr(resp, "\"created\"") != NULL;
    free(resp);
    return ok ? 0 : -1;
}

static int bulk_insert_chunk(TestClient *tc, const char *object,
                              int start_id, int batch_size) {
    size_t cap = (size_t)batch_size * 64 + 256;
    char *buf = malloc(cap);
    if (!buf) return -1;

    /* CodeQL #90: previous code accumulated snprintf's *would-have-
       written* return into `pos`, then passed `cap - pos` as the next
       call's size. If a single record output exceeded the remaining
       buffer, `pos` walked past `cap` and subsequent (size_t) subtraction
       underflowed → snprintf got an enormous "remaining" and could
       overrun the heap allocation. Fixed by clamping pos after every
       snprintf and breaking the loop on truncation. */
    size_t pos = 0;
    #define APPEND(...) do {                                          \
        if (pos + 1 >= cap) break;                                    \
        int _n = snprintf(buf + pos, cap - pos, __VA_ARGS__);         \
        if (_n < 0) { buf[pos] = '\0'; goto out; }                    \
        if ((size_t)_n >= cap - pos) {                                \
            pos = cap - 1; buf[pos] = '\0';                           \
            goto out;                                                 \
        }                                                             \
        pos += (size_t)_n;                                            \
    } while (0)

    APPEND("{\"mode\":\"bulk-insert\",\"dir\":\"b\",\"object\":\"%s\",\"records\":[",
           object);
    for (int i = 0; i < batch_size; i++) {
        int id = start_id + i;
        APPEND("%s{\"key\":\"k%d\",\"value\":{\"flag\":%s}}",
               i ? "," : "", id, (id % 2 == 0) ? "true" : "false");
    }
    APPEND("]}");
out: ;  /* empty statement — pre-C23 label needs a statement, not a
           directly-following declaration. */
    #undef APPEND

    char *resp = NULL;
    tc_request(tc, buf, &resp);
    int ok = resp && strstr(resp, "\"inserted\"") != NULL;
    free(resp);
    free(buf);
    return ok ? 0 : -1;
}

static int bench_bitmap_vs_btree_run(void) {
    const char *env = getenv("SHARD_BENCH_COUNT");
    int N = env ? atoi(env) : 1000000;
    if (N <= 0) N = 1000000;

    printf("==========================================\n");
    printf("  bitmap vs btree — flag:bool eq (N=%d)\n", N);
    printf("==========================================\n\n");

    TestEnv te = {0};
    if (test_env_start(&te) != 0) return 1;
    TestClientCfg cfg = { .port = te.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&te); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"b\"}", &resp); free(resp); resp = NULL;
    if (create_object(tc, "bm",  "bitmap") != 0) { tc_close(tc); test_env_stop(&te); return 1; }
    if (create_object(tc, "btr", "btree")  != 0) { tc_close(tc); test_env_stop(&te); return 1; }

    /* Bulk-insert phase. Chunk size 10k matches typical real-world
       ingest. Time both inserts so the write-side cost is visible too. */
    const int chunk = 10000;
    bench_table_section_begin("insert (bulk, N total)");
    uint64_t t0 = bench_now_ns();
    for (int off = 0; off < N; off += chunk) {
        int sz = (off + chunk <= N) ? chunk : (N - off);
        if (bulk_insert_chunk(tc, "bm", off, sz) != 0) {
            fprintf(stderr, "bulk-insert bm failed at %d\n", off);
            tc_close(tc); test_env_stop(&te); return 1;
        }
    }
    long bm_ins_us = (long)((bench_now_ns() - t0) / 1000);
    char bm_extra[48];
    snprintf(bm_extra, sizeof(bm_extra), "%.0f rec/s", N * 1e6 / (double)bm_ins_us);
    bench_table_record("bulk-insert bitmap", bm_ins_us, 1, bm_extra);

    t0 = bench_now_ns();
    for (int off = 0; off < N; off += chunk) {
        int sz = (off + chunk <= N) ? chunk : (N - off);
        if (bulk_insert_chunk(tc, "btr", off, sz) != 0) {
            fprintf(stderr, "bulk-insert btr failed at %d\n", off);
            tc_close(tc); test_env_stop(&te); return 1;
        }
    }
    long btr_ins_us = (long)((bench_now_ns() - t0) / 1000);
    char btr_extra[48];
    snprintf(btr_extra, sizeof(btr_extra), "%.0f rec/s", N * 1e6 / (double)btr_ins_us);
    bench_table_record("bulk-insert btree",  btr_ins_us, 1, btr_extra);
    bench_table_section_end();

    /* Index file footprint: walk the indexes/ dir for each object and
       sum every shard file. The story bitmap wants to tell: for bool
       at 10M records, btree carries every value in its leaves while
       bitmap is just (slots/8) × n_values × num_shards. */
    {
        char bm_dir[1024], btr_dir[1024];
        snprintf(bm_dir,  sizeof(bm_dir),  "%s/b/bm/indexes",  te.db_root);
        snprintf(btr_dir, sizeof(btr_dir), "%s/b/btr/indexes", te.db_root);
        long long bm_sz  = du_bytes(bm_dir);
        long long btr_sz = du_bytes(btr_dir);
        char bm_buf[32], btr_buf[32];
        fmt_bytes(bm_sz,  bm_buf,  sizeof(bm_buf));
        fmt_bytes(btr_sz, btr_buf, sizeof(btr_buf));
        double ratio = bm_sz > 0 ? (double)btr_sz / (double)bm_sz : 0;
        printf("\nindex on-disk footprint\n");
        printf("--------------------------------------------------------------------------------------------------------------------\n");
        printf("  bitmap   indexes/   %12s\n", bm_buf);
        printf("  btree    indexes/   %12s   (%.1fx larger)\n", btr_buf, ratio);
        printf("--------------------------------------------------------------------------------------------------------------------\n");
    }

    /* Warm both indexes once so the first measured query isn't a
       page-fault-heavy outlier. */
    {
        char r[256];
        snprintf(r, sizeof(r), "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"bm\","
                 "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}");
        tc_request(tc, r, &resp); free(resp); resp = NULL;
        snprintf(r, sizeof(r), "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"btr\","
                 "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}");
        tc_request(tc, r, &resp); free(resp); resp = NULL;
    }

    /* count() — exhaustive, returns the whole popcount. The bitmap
       path's bm_count sums byte popcounts (cache-friendly), the btree
       path walks the leaf entries. */
    bench_table_section_begin("count flag eq <bool>");
    bench_table_run(tc, "bitmap flag=false",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}");
    bench_table_run(tc, "btree  flag=false",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]}");
    bench_table_run(tc, "bitmap flag=true",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}");
    bench_table_run(tc, "btree  flag=true",
        "{\"mode\":\"count\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}");
    bench_table_section_end();

    /* find limit=1000 — at low limit both stop early and the per-match
       cost is dominated by payload fetch. Bitmap pays a higher
       setup cost (one .bm per data shard vs ~splits/4 .idx for btree),
       so btree narrowly wins here. */
    bench_table_section_begin("find flag eq <bool> (limit=1000)");
    bench_table_run(tc, "bitmap flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":1000}");
    bench_table_run(tc, "btree  flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":1000}");
    bench_table_run(tc, "bitmap flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":1000}");
    bench_table_run(tc, "btree  flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":1000}");
    bench_table_section_end();

    /* find limit=100000 — mid-scale. Per-match key extraction starts
       to matter; bitmap's ffs-over-uint64 packs 64 candidates per
       word vs btree walking a leaf entry per match. Crossover region. */
    bench_table_section_begin("find flag eq <bool> (limit=100000)");
    bench_table_run(tc, "bitmap flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":100000}");
    bench_table_run(tc, "btree  flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":100000}");
    bench_table_run(tc, "bitmap flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":100000}");
    bench_table_run(tc, "btree  flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":100000}");
    bench_table_section_end();

    /* find limit=1000000 — high-scale. Index-scan cost dominates over
       setup. Bitmap's dense-bit iteration is now several times faster
       than walking a million btree leaf entries. */
    bench_table_section_begin("find flag eq <bool> (limit=1000000)");
    bench_table_run(tc, "bitmap flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":1000000}");
    bench_table_run(tc, "btree  flag=false",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}],"
        "\"limit\":1000000}");
    bench_table_run(tc, "bitmap flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"bm\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":1000000}");
    bench_table_run(tc, "btree  flag=true",
        "{\"mode\":\"find\",\"dir\":\"b\",\"object\":\"btr\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":1000000}");
    bench_table_section_end();

    printf("\nNotes:\n");
    printf("  · Both indexes serve eq queries; bitmap on bool is ~125 KB\n");
    printf("    per shard (2 dense bitmaps), btree carries every value.\n");
    printf("  · count() on bitmap is a popcount over each shard's bitmap;\n");
    printf("    count() on btree walks every matching leaf entry.\n");
    printf("  · find() crossover: bitmap opens one .bm per data shard\n");
    printf("    vs btree's ~splits/4 .idx files, so setup cost is higher.\n");
    printf("    At low limit (1k) btree edges ahead — fetch dominates and\n");
    printf("    the setup tax matters. At mid/high limits (100k, 1M) the\n");
    printf("    per-match scan cost flips it: bitmap's ffs-over-uint64\n");
    printf("    packs 64 candidates per word, btree walks leaf-by-leaf.\n");

    tc_close(tc);
    test_env_stop(&te);
    return 0;
}

TEST_REGISTER("bench-bitmap-vs-btree", bench_bitmap_vs_btree_run)
