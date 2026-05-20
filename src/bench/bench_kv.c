/* src/bench/bench_kv.c — port of bench/bench-kv.sh
 *
 * 16-byte hex key, varchar(100) value, SPLITS=128, COUNT=1M default
 * (override via SHARD_BENCH_COUNT env var). Mirrors db_bench / LMDB
 * shape so numbers compare directly. Single-connection throughout
 * except for the two parallel sections.
 *
 * Key shape  : sha256(decimal-i)[:16] hex — same as Python bench loader.
 * Value shape: "val_<i>" left-padded to 100 chars with 'x'.
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

#define KEY_LEN    16    /* hex chars per key  */
#define VAL_LEN   100    /* padded value length */
#define SPLITS    128
#define N_PAR       5    /* parallel worker count          */
#define N_PAR_OPS 10000  /* ops per parallel worker        */

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

/* --------------------------------------------------------- parallel workers */

typedef enum { PAR_GET, PAR_UPDATE } ParMode;

typedef struct {
    int         port;
    ParMode     mode;
    int         n;
    char      (*keys)[KEY_LEN+1];  /* pointer to master keys array */
    int         total_keys;
    uint64_t    total_ns;
} ParWorkerArg;

static void *par_worker(void *vp)
{
    ParWorkerArg *w = vp;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;

    char req[512];
    uint64_t s = bench_now_ns();
    for (int i = 0; i < w->n; i++) {
        const char *k = w->keys[(unsigned)rand() % (unsigned)w->total_keys];
        if (w->mode == PAR_GET) {
            snprintf(req, sizeof(req),
                     "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\"}",
                     k);
        } else {
            snprintf(req, sizeof(req),
                     "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\",\"value\":{\"v\":\"par_%s\"}}",
                     k, k);
        }
        char *resp = NULL;
        tc_request(tc, req, &resp);
        free(resp);
    }
    w->total_ns = bench_now_ns() - s;
    tc_close(tc);
    return NULL;
}

/* ------------------------------------------------ memfd bulk-insert helper */

/* Send a bulk-insert via memfd; return heap-allocated response (caller frees). */
static char *bulk_insert_memfd(TestClient *tc,
                               const char *mode,
                               const char *extra_json_fields,
                               const char *data, size_t data_size)
{
    int fd = bench_make_memfd("kv-blob", data, data_size);
    if (fd < 0) return NULL;

    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/fd/%d", (int)getpid(), fd);

    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"%s\",\"dir\":\"default\",\"object\":\"kvbench\","
             "\"file\":\"%s\"%s}",
             mode, path, extra_json_fields);

    char *resp = NULL;
    tc_request(tc, req, &resp);
    close(fd);
    return resp;
}

/* ---------------------------------------------------------------- main bench */

static int bench_kv_run(void)
{
    const char *count_env = getenv("SHARD_BENCH_COUNT");
    int COUNT = count_env ? atoi(count_env) : 1000000;
    if (COUNT <= 0) COUNT = 1000000;

    printf("======================================\n");
    printf("  shard-db K/V benchmark (%d records)\n", COUNT);
    printf("  key=16B hex, value=varchar(100) — matches db_bench / LMDB shape\n");
    printf("======================================\n\n");

    /* ---- 1. Spawn daemon -------------------------------------------- */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-kv: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 300000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        fprintf(stderr, "bench-kv: connect failed\n");
        test_env_stop(&env);
        return 1;
    }

    /* Register "default" tenant and create object. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    {
        char create[512];
        snprintf(create, sizeof(create),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kvbench\","
            "\"splits\":128,\"max_key\":16,"
            "\"fields\":[\"v:varchar:100\"]}");
        tc_request(tc, create, &resp);
        free(resp); resp = NULL;
    }

    /* ---- 2. Generate records ---------------------------------------- */
    printf("Generating %d records (16B hex key, ~100B value)...\n", COUNT);
    fflush(stdout);

    char (*keys)[KEY_LEN + 1] = malloc((size_t)COUNT * (KEY_LEN + 1));
    char (*vals)[VAL_LEN + 1] = malloc((size_t)COUNT * (VAL_LEN + 1));
    if (!keys || !vals) {
        fprintf(stderr, "bench-kv: OOM allocating keys/vals\n");
        free(keys); free(vals);
        tc_close(tc); test_env_stop(&env);
        return 1;
    }

    for (int i = 0; i < COUNT; i++) {
        make_key(i, keys[i]);
        make_val(i, vals[i]);
    }
    printf("  done.\n\n");

    /* Build JSON array blob.
       Each record: {"key":"<16>","value":{"v":"<100>"}}  = 142 chars (no comma)
       Commas between records add COUNT-1 bytes.
       Total: 1 + (COUNT * 142) + (COUNT - 1) + 1 + NUL = COUNT*143 + 2 + NUL. */
    printf("Building bulk-insert JSON blob...\n");
    fflush(stdout);

    size_t json_cap  = (size_t)COUNT * 144 + 8; /* 1 byte/rec slack + NUL */
    size_t json_size = 0;
    char  *json_buf  = malloc(json_cap);
    if (!json_buf) {
        fprintf(stderr, "bench-kv: OOM json_buf\n");
        free(vals); free(keys);
        tc_close(tc); test_env_stop(&env);
        return 1;
    }

    {
        size_t jpos = 0;
        json_buf[jpos++] = '[';
        for (int i = 0; i < COUNT; i++) {
            int n = snprintf(json_buf + jpos, json_cap - jpos,
                             "{\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}%s",
                             keys[i], vals[i],
                             i < COUNT - 1 ? "," : "");
            if (n <= 0 || (size_t)n >= json_cap - jpos) {
                fprintf(stderr, "bench-kv: JSON overflow at i=%d (jpos=%zu cap=%zu)\n",
                        i, jpos, json_cap);
                free(json_buf); free(vals); free(keys);
                tc_close(tc); test_env_stop(&env);
                return 1;
            }
            jpos += (size_t)n;
        }
        json_buf[jpos++] = ']';
        json_buf[jpos]   = '\0';  /* null-terminate */
        json_size = jpos;
        printf("  JSON: %.1f MB\n", (double)json_size / 1048576.0);
    }

    /* Build CSV blob: <key>,<val>\n
       Each line: 16 + 1 + 100 + 1 = 118 bytes. */
    printf("Building bulk-insert CSV blob...\n");
    fflush(stdout);

    size_t csv_cap  = (size_t)COUNT * 120 + 8;
    size_t csv_size = 0;
    char  *csv_buf  = malloc(csv_cap);
    if (!csv_buf) {
        fprintf(stderr, "bench-kv: OOM csv_buf\n");
        free(json_buf); free(vals); free(keys);
        tc_close(tc); test_env_stop(&env);
        return 1;
    }

    {
        size_t cpos = 0;
        for (int i = 0; i < COUNT; i++) {
            int n = snprintf(csv_buf + cpos, csv_cap - cpos,
                             "%s,%s\n", keys[i], vals[i]);
            if (n <= 0 || (size_t)n >= csv_cap - cpos) {
                fprintf(stderr, "bench-kv: CSV overflow at i=%d\n", i);
                free(csv_buf); free(json_buf); free(vals); free(keys);
                tc_close(tc); test_env_stop(&env);
                return 1;
            }
            cpos += (size_t)n;
        }
        csv_buf[cpos] = '\0';
        csv_size = cpos;
        printf("  CSV:  %.1f MB\n\n", (double)csv_size / 1048576.0);
    }

    /* ---- 3. BULK INSERT JSON — captured into table ------------------- */
    long bulk_json_us = 0;
    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, "bulk-insert", "", json_buf, json_size);
        bulk_json_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"kvbench\"}",
        &resp);
    printf("SIZE after JSON insert: %s\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* ---- 4. Truncate + BULK INSERT CSV — captured into table --------- */
    tc_request(tc,
        "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"kvbench\"}",
        &resp);
    free(resp); resp = NULL;

    long bulk_csv_us = 0;
    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, "bulk-insert-delimited",
                                 ",\"delimiter\":\",\"",
                                 csv_buf, csv_size);
        bulk_csv_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }
    free(csv_buf); csv_buf = NULL;

    tc_request(tc,
        "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"kvbench\"}",
        &resp);
    printf("SIZE after CSV insert:  %s\n\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* ---- BULK INSERT throughput table -------------------------------- */
    {
        char e_json[48], e_csv[48];
        snprintf(e_json, sizeof(e_json), "%.2f M rows/s",
                 (double)COUNT / 1e6 / ((double)bulk_json_us / 1e6));
        snprintf(e_csv, sizeof(e_csv), "%.2f M rows/s",
                 (double)COUNT / 1e6 / ((double)bulk_csv_us / 1e6));
        bench_table_section_begin("BULK INSERT (single conn)");
        bench_table_record("JSON 1 file", bulk_json_us, 1, e_json);
        bench_table_record("CSV  1 file", bulk_csv_us, 1, e_csv);
        bench_table_section_end();
    }

    /* ---- 7-12. Single-conn latency batches — captured into table ----- */
    long get_warm_us = 0;
    long get_total_us = 0; uint64_t get_p50 = 0;
    long exists_hit_total_us = 0; uint64_t exists_hit_p50 = 0;
    long exists_miss_total_us = 0; uint64_t exists_miss_p50 = 0;
    long update_total_us = 0; uint64_t update_p50 = 0;
    long delete_total_us = 0; uint64_t delete_p50 = 0;

    /* GET single (warm) */
    {
        int ri = (int)((unsigned)rand() % (unsigned)COUNT);
        char req[256];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"kvbench\","
                 "\"key\":\"%s\"}", keys[ri]);
        uint64_t t0 = bench_now_ns();
        tc_request(tc, req, &resp);
        get_warm_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }

    /* GET x10000 */
    {
        const int N = 10000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            int ri = (int)((unsigned)rand() % (unsigned)COUNT);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\"}", keys[ri]);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        get_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        get_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* EXISTS x10000 (hits) */
    {
        const int N = 10000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            int ri = (int)((unsigned)rand() % (unsigned)COUNT);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\"}", keys[ri]);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        exists_hit_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        exists_hit_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* EXISTS x10000 (all-miss) */
    {
        const int N = 10000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"nope_%d\"}", i + 1);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        exists_miss_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        exists_miss_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* UPDATE x10000 */
    {
        const int N = 10000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            int ri = (int)((unsigned)rand() % (unsigned)COUNT);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\",\"value\":{\"v\":\"updated_%s\"}}",
                     keys[ri], keys[ri]);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        update_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        update_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* DELETE x10000 (deterministic coverage of first N keys) */
    {
        const int N = 10000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"kvbench\","
                     "\"key\":\"%s\"}", keys[i]);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        delete_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        delete_p50 = bench_hist_p50_ns(&h);
        free(samples);

        tc_request(tc,
            "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"kvbench\"}",
            &resp);
        printf("SIZE after DELETE x10000: %s\n\n", resp ? resp : "(err)");
        free(resp); resp = NULL;
    }

    /* ---- Single-conn latency table ----------------------------------- */
    {
        char e_get[48], e_exh[48], e_exm[48], e_upd[48], e_del[48];
        snprintf(e_get, sizeof(e_get), "p50=%.0fµs  %.0f k op/s",
                 (double)get_p50 / 1000.0,
                 10.0 / ((double)get_total_us / 1e6));
        snprintf(e_exh, sizeof(e_exh), "p50=%.0fµs  %.0f k op/s",
                 (double)exists_hit_p50 / 1000.0,
                 10.0 / ((double)exists_hit_total_us / 1e6));
        snprintf(e_exm, sizeof(e_exm), "p50=%.0fµs  %.0f k op/s",
                 (double)exists_miss_p50 / 1000.0,
                 10.0 / ((double)exists_miss_total_us / 1e6));
        snprintf(e_upd, sizeof(e_upd), "p50=%.0fµs  %.0f k op/s",
                 (double)update_p50 / 1000.0,
                 10.0 / ((double)update_total_us / 1e6));
        snprintf(e_del, sizeof(e_del), "p50=%.0fµs  %.0f k op/s",
                 (double)delete_p50 / 1000.0,
                 10.0 / ((double)delete_total_us / 1e6));
        bench_table_section_begin("Single-conn latency batches (N=10000)");
        bench_table_record("GET   single warm", get_warm_us, 1, NULL);
        bench_table_record("GET    x10000 pipelined", get_total_us, 1, e_get);
        bench_table_record("EXISTS x10000 (hits)",   exists_hit_total_us, 1, e_exh);
        bench_table_record("EXISTS x10000 (all-miss)", exists_miss_total_us, 1, e_exm);
        bench_table_record("UPDATE x10000",          update_total_us, 1, e_upd);
        bench_table_record("DELETE x10000",          delete_total_us, 1, e_del);
        bench_table_section_end();
    }

    /* ---- Bulk ops (single conn, one request handles N keys) -----------
       Exercises the multi-key paths: bulk-get / bulk-exists land in
       cmd_get_multi / cmd_exists_multi (parallel_for shard fan-out);
       bulk-update-json / bulk-delete land in cmd_bulk_update_json /
       cmd_bulk_delete (parallel shard workers + slotcask bulk
       primitives on v2). All four reuse the same N=10000 sample so the
       table compares cleanly to single-conn latency batches above. */
    long bget_us = 0, bexi_us = 0, bupd_us = 0, bdel_us = 0;
    {
        const int N = 10000;
        /* Keys 0..9999 are dead from the single-conn DELETE batch above.
           Use keys[10000..19999] so bulk-get/exists/update see live keys. */
        const int START = 10000 < COUNT - N ? 10000 : 0;

        size_t key_buf_cap = (size_t)N * (size_t)(KEY_LEN + 4) + 64;
        char *key_arr = malloc(key_buf_cap);
        size_t kp = 0;
        BENCH_SB_APPEND(key_arr, kp, key_buf_cap, "[");
        for (int i = 0; i < N; i++)
            BENCH_SB_APPEND(key_arr, kp, key_buf_cap,
                             "%s\"%s\"", i ? "," : "", keys[START + i]);
        BENCH_SB_APPEND(key_arr, kp, key_buf_cap, "]");

        /* bulk-get */
        {
            size_t cap = key_buf_cap + 128;
            char *req = malloc(cap);
            snprintf(req, cap,
                "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"kvbench\","
                "\"keys\":%s}", key_arr);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bget_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }

        /* bulk-exists */
        {
            size_t cap = key_buf_cap + 128;
            char *req = malloc(cap);
            snprintf(req, cap,
                "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"kvbench\","
                "\"keys\":%s}", key_arr);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bexi_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }

        /* bulk-update-json — small per-record patch, all keys updated. */
        {
            size_t cap = (size_t)N * (size_t)(KEY_LEN + 64) + 128;
            char *req = malloc(cap);
            size_t p = 0;
            BENCH_SB_APPEND(req, p, cap,
                "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                "\"object\":\"kvbench\",\"records\":[");
            for (int i = 0; i < N; i++)
                BENCH_SB_APPEND(req, p, cap,
                    "%s{\"key\":\"%s\",\"value\":{\"v\":\"bulkupd_%d\"}}",
                    i ? "," : "", keys[START + i], i);
            BENCH_SB_APPEND(req, p, cap, "]}");
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bupd_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }

        /* bulk-delete (destructive — truncate+repopulate below restores). */
        {
            size_t cap = key_buf_cap + 128;
            char *req = malloc(cap);
            snprintf(req, cap,
                "{\"mode\":\"bulk-delete\",\"dir\":\"default\","
                "\"object\":\"kvbench\",\"keys\":%s}", key_arr);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bdel_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }
        free(key_arr);
    }
    {
        const int N = 10000;
        char e_get[48], e_exi[48], e_upd[48], e_del[48];
        snprintf(e_get, sizeof(e_get), "%.0f k op/s",
                 (double)N / ((double)bget_us / 1e3));
        snprintf(e_exi, sizeof(e_exi), "%.0f k op/s",
                 (double)N / ((double)bexi_us / 1e3));
        snprintf(e_upd, sizeof(e_upd), "%.0f k op/s",
                 (double)N / ((double)bupd_us / 1e3));
        snprintf(e_del, sizeof(e_del), "%.0f k op/s",
                 (double)N / ((double)bdel_us / 1e3));
        bench_table_section_begin("Bulk ops (single request, N=10000)");
        bench_table_record("BULK GET    x10000 in one call", bget_us, 1, e_get);
        bench_table_record("BULK EXISTS x10000 in one call", bexi_us, 1, e_exi);
        bench_table_record("BULK UPDATE x10000 in one call", bupd_us, 1, e_upd);
        bench_table_record("BULK DELETE x10000 in one call", bdel_us, 1, e_del);
        bench_table_section_end();
    }

    /* ---- 13. Re-populate for parallel section ----------------------- */
    printf("--- Re-populating for parallel test ---\n");
    fflush(stdout);
    {
        tc_request(tc,
            "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"kvbench\"}",
            &resp);
        free(resp); resp = NULL;

        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, "bulk-insert", "", json_buf, json_size);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("  repopulate: wall=%.0fms  %.2f M/sec\n\n",
               (double)elapsed / 1e6,
               (double)COUNT / ((double)elapsed / 1e9) / 1e6);
        free(resp); resp = NULL;
    }

    /* JSON blob no longer needed. */
    free(json_buf); json_buf = NULL;

    /* ---- 14-15. PARALLEL GET / UPDATE — table view ------------------- */
    long par_get_us = 0, par_upd_us = 0;
    {
        pthread_t    threads[N_PAR];
        ParWorkerArg args[N_PAR];
        uint64_t wall_start = bench_now_ns();
        for (int t = 0; t < N_PAR; t++) {
            args[t] = (ParWorkerArg){
                .port = env.port, .mode = PAR_GET, .n = N_PAR_OPS,
                .keys = keys, .total_keys = COUNT, .total_ns = 0,
            };
            pthread_create(&threads[t], NULL, par_worker, &args[t]);
        }
        for (int t = 0; t < N_PAR; t++) pthread_join(threads[t], NULL);
        par_get_us = (long)((bench_now_ns() - wall_start) / 1000);
    }
    {
        pthread_t    threads[N_PAR];
        ParWorkerArg args[N_PAR];
        uint64_t wall_start = bench_now_ns();
        for (int t = 0; t < N_PAR; t++) {
            args[t] = (ParWorkerArg){
                .port = env.port, .mode = PAR_UPDATE, .n = N_PAR_OPS,
                .keys = keys, .total_keys = COUNT, .total_ns = 0,
            };
            pthread_create(&threads[t], NULL, par_worker, &args[t]);
        }
        for (int t = 0; t < N_PAR; t++) pthread_join(threads[t], NULL);
        par_upd_us = (long)((bench_now_ns() - wall_start) / 1000);
    }
    {
        int total_ops = N_PAR * N_PAR_OPS;
        char e_get[48], e_upd[48], lbl[64];
        snprintf(e_get, sizeof(e_get), "%.0f k op/s",
                 (double)total_ops / ((double)par_get_us / 1e3));
        snprintf(e_upd, sizeof(e_upd), "%.0f k op/s",
                 (double)total_ops / ((double)par_upd_us / 1e3));
        bench_table_section_begin("Parallel latency batches "
                                  "(5 conns × 10k = 50k ops)");
        snprintf(lbl, sizeof(lbl), "PARALLEL GET    %d conns × %d", N_PAR, N_PAR_OPS);
        bench_table_record(lbl, par_get_us, 1, e_get);
        snprintf(lbl, sizeof(lbl), "PARALLEL UPDATE %d conns × %d", N_PAR, N_PAR_OPS);
        bench_table_record(lbl, par_upd_us, 1, e_upd);
        bench_table_section_end();
    }

    /* ---- 16. Disk usage -------------------------------------------- */
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/default/kvbench", env.db_root);
        char *argv[] = { (char *)"du", (char *)"-sh", path, NULL };
        printf("DISK USAGE: ");
        fflush(stdout);
        bench_safe_exec(argv);
        printf("\n");
    }

    printf("======================================\n");
    printf("  K/V Benchmark complete\n");
    printf("======================================\n");

    free(vals);
    free(keys);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-kv", bench_kv_run)
