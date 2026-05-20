/* src/bench/bench_bitmap_vs_btree.c — head-to-head latency comparison.
 *
 * Two objects with the same schema (`flag:bool`). One declared
 * `flag:bitmap` (auto-default behaviour, made explicit for clarity),
 * the other `flag:btree`. Bulk-insert N identical records into each,
 * then run count + find queries `flag eq false` / `flag eq true` on
 * both. The section formatter prints the rows side-by-side with a
 * relative bar chart that makes the bitmap win obvious.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    int pos = snprintf(buf, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"b\",\"object\":\"%s\",\"records\":[",
        object);
    for (int i = 0; i < batch_size; i++) {
        int id = start_id + i;
        pos += snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"k%d\",\"value\":{\"flag\":%s}}",
            i ? "," : "", id, (id % 2 == 0) ? "true" : "false");
    }
    pos += snprintf(buf + pos, cap - pos, "]}");

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

    /* find limit=1000 — same selector but materialises records.
       Bitmap still walks fewer bits before stopping at limit. */
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

    printf("\nNotes:\n");
    printf("  · Both indexes serve eq queries; bitmap on bool is ~125 KB\n");
    printf("    per shard (2 dense bitmaps), btree carries every value.\n");
    printf("  · count() on bitmap is a popcount over each shard's bitmap;\n");
    printf("    count() on btree walks every matching leaf entry.\n");
    printf("  · find() differs less because both still fetch ≤limit\n");
    printf("    record payloads from segments — the lookup-by-hash cost\n");
    printf("    is the same once you have the matching keys.\n");

    tc_close(tc);
    test_env_stop(&te);
    return 0;
}

TEST_REGISTER("bench-bitmap-vs-btree", bench_bitmap_vs_btree_run)
