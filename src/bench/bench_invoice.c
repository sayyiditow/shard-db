/* src/bench/bench_invoice.c — port of bench/bench-invoice.sh
 *
 * Single-connection invoice bench: 1M records, 64 fields, 14 indexes.
 * Realistic wide-record schema. Output covers bulk-insert (no idx),
 * read latencies, add-indexes, bulk-insert (with idx), 13 indexed
 * find queries, range/fetch/keys, deletes, vacuum, recount, disk usage.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include "bench_stats.h"
#include "bench_table.h"
#include "bench_invoice_schema.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <time.h>

/* ---------------------------------------------------------------- constants */

#define INV_SPLITS  64
#define INV_MAX_KEY 16   /* "INV-0000000" = 13 chars; 16 is safe */

/* Pools for synthetic field values — same cardinalities as the bash bench */
static const char *STATUSES[]      = { "DRAFT","PENDING","APPROVED","REJECTED","CANCELLED" };
static const char *IRBM_STATUSES[] = { "PENDING","VALID","INVALID","CANCELLED" };
static const char *CURRENCIES[]    = { "MYR","USD","SGD","EUR","GBP" };
static const char *INV_TYPES[]     = { "STANDARD","CREDIT_NOTE","DEBIT_NOTE","REFUND","SELF_BILLED" };
static const char *FREQS[]         = { "MONTHLY","QUARTERLY","YEARLY","DAILY","WEEKLY" };
static const char *SOURCES[]       = { "API","PORTAL","BATCH","IMPORT" };

/* ------------------------------------------------ memfd bulk-insert helper */

static int make_memfd(const char *name, const char *data, size_t size)
{
#if defined(__x86_64__)
    int fd = (int)syscall(319, name, 0);
#elif defined(__aarch64__)
    int fd = (int)syscall(279, name, 0);
#else
    char path[] = "/tmp/bench-invoice-XXXXXX";
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

/* Send bulk-insert from in-memory buffer via memfd path. */
static char *bulk_insert_memfd(TestClient *tc, const char *data, size_t data_size)
{
    int fd = make_memfd("inv-blob", data, data_size);
    if (fd < 0) return NULL;

    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/fd/%d", (int)getpid(), fd);

    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\","
             "\"file\":\"%s\"}",
             path);

    char *resp = NULL;
    tc_request(tc, req, &resp);
    close(fd);
    return resp;
}

/* ---------------------------------------------------------- key generators */

/* Key for record i: "INV-NNNNNNN" */
static void make_inv_key(int i, char out[16])
{
    snprintf(out, 16, "INV-%07d", i);
}

/* supplierId pool: 100 synthetic IDs — simple hex strings derived from i%100 */
static void make_supplier_id(int i, char out[48])
{
    /* Matches bash bench: SUP-<md5(i)> for i in 0..99.
       We approximate with a deterministic hex pattern so the index
       bench can look up by supplier 0..99. */
    snprintf(out, 48, "SUP-%032x", (unsigned)(i * 2654435761u));
}

/* buyerId pool: 500 synthetic IDs */
static void make_buyer_id(int i, char out[48])
{
    snprintf(out, 48, "BUY-%032x", (unsigned)(i * 1234567891u));
}

/* --------------------------------------------------------- record builder */

/*
 * Append one JSON record to buf at offset *pos.  The record is a
 * {"key":"INV-NNN","value":{...}} object suitable for bulk-insert array form.
 * Returns bytes written, or -1 if buffer would overflow.
 *
 * We populate ~20 representative fields; the rest are left at their schema
 * defaults (empty varchar / zero numeric).  The point is throughput, not
 * content.
 */
static int append_record(char *buf, size_t cap, size_t pos, int i, int last)
{
    char key[16];
    char supplier[48];
    char buyer[48];
    make_inv_key(i, key);
    make_supplier_id(i % 100, supplier);
    make_buyer_id(i % 500, buyer);

    /* datetime: base 20240101000000 + offset by day */
    int day = (i % 366) + 1;
    int month = (day / 31) + 1;
    if (month > 12) month = 12;
    int dom = (day % 28) + 1;
    char inv_date[16], created_at[16], updated_at[16], sub_date[16], val_date[16];
    snprintf(inv_date,  sizeof(inv_date),  "2024%02d%02d000000", month, dom);
    snprintf(created_at,sizeof(created_at),"2024%02d%02d000000", month, dom);
    snprintf(updated_at,sizeof(updated_at),"2024%02d%02d010000", month, dom);
    snprintf(sub_date,  sizeof(sub_date),  "2024%02d%02d003000", month, dom);
    snprintf(val_date,  sizeof(val_date),  "2024%02d%02d010000", month, dom);

    const char *status     = STATUSES[i % 5];
    const char *irbm_st    = IRBM_STATUSES[i % 4];
    const char *currency   = CURRENCIES[i % 5];
    const char *inv_type   = INV_TYPES[i % 5];
    const char *freq       = FREQS[i % 5];
    const char *source     = SOURCES[i % 4];
    int consolidated       = (i % 7 == 0) ? 1 : 0;
    int pdf_sent           = (i % 3 != 0) ? 1 : 0;
    double exc_tax         = 100.0 + (double)(i % 49900);
    double tax_amt         = exc_tax * 0.06;
    double inc_tax         = exc_tax + tax_amt;

    /* batch number: BATCH-NNNN (i/1000) */
    char batch[16];
    snprintf(batch, sizeof(batch), "BATCH-%04d", i / 1000);

    /* number: INV-2026-NNNNNNN */
    char number[24];
    snprintf(number, sizeof(number), "INV-2026-%07d", i);

    int n = snprintf(buf + pos, cap - pos,
        "{\"key\":\"%s\",\"value\":{"
        "\"buyerId\":\"%s\","
        "\"version\":\"1.0\","
        "\"number\":\"%s\","
        "\"supplierId\":\"%s\","
        "\"source\":\"%s\","
        "\"batchNumber\":\"%s\","
        "\"invoiceDate\":\"%s\","
        "\"createdAt\":\"%s\","
        "\"updatedAt\":\"%s\","
        "\"submissionDate\":\"%s\","
        "\"validationDate\":\"%s\","
        "\"status\":\"%s\","
        "\"irbmStatus\":\"%s\","
        "\"currencyCode\":\"%s\","
        "\"invoiceType\":\"%s\","
        "\"frequency\":\"%s\","
        "\"consolidated\":%s,"
        "\"pdfSent\":%s,"
        "\"exchangeRate\":1.0,"
        "\"totalExcludingTax\":%.2f,"
        "\"totalIncludingTax\":%.2f,"
        "\"totalPayableAmount\":%.2f,"
        "\"totalNetAmount\":%.2f,"
        "\"totalTaxAmount\":%.2f,"
        "\"totalSalesTaxable\":%.2f,"
        "\"totalSalesTaxAmount\":%.2f"
        "}}%s",
        key,
        buyer,
        number,
        supplier,
        source,
        batch,
        inv_date,
        created_at,
        updated_at,
        sub_date,
        val_date,
        status,
        irbm_st,
        currency,
        inv_type,
        freq,
        consolidated ? "true" : "false",
        pdf_sent ? "true" : "false",
        exc_tax, inc_tax, inc_tax, exc_tax, tax_amt,
        exc_tax, tax_amt,
        last ? "" : ","
    );
    return n;
}

/* ---------------------------------------------------------------- main bench */

static int bench_invoice_run(void)
{
    const char *count_env = getenv("SHARD_BENCH_COUNT");
    int COUNT = count_env ? atoi(count_env) : 1000000;
    if (COUNT <= 0) COUNT = 1000000;

    printf("======================================\n");
    printf("  shard-db INVOICE benchmark (%d records)\n", COUNT);
    printf("  64 fields, 14 indexes (incl. 4 composite)\n");
    printf("======================================\n\n");

    /* ---- 1. Spawn daemon ----------------------------------------------- */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-invoice: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        fprintf(stderr, "bench-invoice: connect failed\n");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;

    /* Register "default" tenant, create object. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    {
        char create[2048];
        snprintf(create, sizeof(create),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\","
            "\"splits\":64,\"max_key\":16,\"storage_version\":%d,"
            "\"fields\":[" INVOICE_SCHEMA_FIELDS "],"
            "\"indexes\":[]}",
            bench_storage_version());
        tc_request(tc, create, &resp);
        free(resp); resp = NULL;
    }

    /* ---- 2. Generate records ------------------------------------------- */
    printf("Generating %d invoice records...\n", COUNT);
    fflush(stdout);

    /*
     * Estimate per-record JSON size.  A realistic record with ~20 fields and
     * moderate varchar content is about 780 bytes.  Add slack.
     */
    size_t json_cap = (size_t)COUNT * 820 + 8;
    char  *json_buf = malloc(json_cap);
    if (!json_buf) {
        fprintf(stderr, "bench-invoice: OOM json_buf (%zu bytes)\n", json_cap);
        tc_close(tc); test_env_stop(&env); return 1;
    }

    size_t json_size = 0;
    json_buf[json_size++] = '[';
    for (int i = 0; i < COUNT; i++) {
        int n = append_record(json_buf, json_cap, json_size, i, i == COUNT - 1);
        if (n <= 0 || json_size + (size_t)n >= json_cap) {
            fprintf(stderr, "bench-invoice: JSON overflow at i=%d (pos=%zu cap=%zu)\n",
                    i, json_size, json_cap);
            free(json_buf); tc_close(tc); test_env_stop(&env); return 1;
        }
        json_size += (size_t)n;
    }
    json_buf[json_size++] = ']';
    json_buf[json_size]   = '\0';
    printf("  JSON blob: %.1f MB\n\n", (double)json_size / 1048576.0);

    /* ---- 3. BULK INSERT (no indexes) — captured into table below ------- */
    long bulk_no_idx_us = 0;
    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, json_buf, json_size);
        bulk_no_idx_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }

    /* size check (informational) */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"bench\"}", &resp);
    printf("SIZE after no-idx insert: %s\n\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* ---- 4. GET x1000 pipelined --------------------------------------- */
    long get_total_us = 0; uint64_t get_p50 = 0;
    {
        const int N = 1000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char key[16];
            make_inv_key((int)((unsigned)rand() % (unsigned)COUNT), key);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bench\","
                     "\"key\":\"%s\"}", key);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        get_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        get_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* ---- 5. EXISTS x1000 pipelined ------------------------------------- */
    long exists_total_us = 0; uint64_t exists_p50 = 0;
    {
        const int N = 1000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char key[16];
            make_inv_key((int)((unsigned)rand() % (unsigned)COUNT), key);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"bench\","
                     "\"key\":\"%s\"}", key);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        exists_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        exists_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* ---- 6. ADD INDEXES (14 indexes) ----------------------------------- */
    long add_idx_us = 0;
    {
        uint64_t t0 = bench_now_ns();
        tc_request(tc,
            "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\","
            "\"fields\":[" INVOICE_INDEX_FIELDS "]}",
            &resp);
        add_idx_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }

    /* ---- 7. INDEXED SEARCH x100 (supplierId) --------------------------- */
    long idx_search_total_us = 0; uint64_t idx_search_p50 = 0;
    {
        const int N = 100;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char supplier[48];
            make_supplier_id(i, supplier);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
                     "\"criteria\":[{\"field\":\"supplierId\",\"op\":\"eq\","
                     "\"value\":\"%s\"}],\"limit\":10}",
                     supplier);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        idx_search_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        idx_search_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* ---- 8. BULK INSERT with indexes — captured into table below ------- */
    /* Truncate first */
    tc_request(tc, "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"bench\"}",
               &resp);
    free(resp); resp = NULL;

    long bulk_idx_us = 0;
    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, json_buf, json_size);
        bulk_idx_us = (long)((bench_now_ns() - t0) / 1000);
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"bench\"}", &resp);
    printf("SIZE after 14-idx insert: %s\n\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* ---- BULK INSERT throughput — table view --------------------------- */
    {
        char extra1[48], extra2[48];
        snprintf(extra1, sizeof(extra1), "%.2f M rows/s",
                 (double)COUNT / 1e6 / ((double)bulk_no_idx_us / 1e6));
        snprintf(extra2, sizeof(extra2), "%.2f M rows/s",
                 (double)COUNT / 1e6 / ((double)bulk_idx_us / 1e6));
        bench_table_section_begin("BULK INSERT (1M invoice records, single-conn JSON)");
        bench_table_record("no indexes", bulk_no_idx_us, 1, extra1);
        bench_table_record("14 indexes (4 composite)", bulk_idx_us, 1, extra2);
        bench_table_section_end();
    }

    /* ---- READ + WRITE latency (pipelined) — table view ----------------- */
    {
        char e_get[48], e_exists[48], e_idx[48];
        snprintf(e_get, sizeof(e_get), "p50=%.0fµs  %.0f k op/s",
                 (double)get_p50 / 1000.0,
                 1e3 / (double)((double)get_total_us / 1000.0));
        snprintf(e_exists, sizeof(e_exists), "p50=%.0fµs  %.0f k op/s",
                 (double)exists_p50 / 1000.0,
                 1e3 / (double)((double)exists_total_us / 1000.0));
        snprintf(e_idx, sizeof(e_idx), "p50=%.0fµs  %.1f k op/s",
                 (double)idx_search_p50 / 1000.0,
                 100.0 / ((double)idx_search_total_us / 1e6) / 1000.0);
        bench_table_section_begin("READ latency (pipelined batches)");
        bench_table_record("GET x1000 (random keys)", get_total_us, 1, e_get);
        bench_table_record("EXISTS x1000 (random keys)", exists_total_us, 1, e_exists);
        bench_table_record("INDEXED FIND x100 (supplierId)", idx_search_total_us, 1, e_idx);
        bench_table_section_end();
    }

    /* ---- ADD INDEXES wall — single-row section ------------------------- */
    bench_table_section_begin("Schema ops");
    {
        char extra[48];
        snprintf(extra, sizeof(extra), "14 fields, single-pass build");
        bench_table_record("ADD INDEXES (post-load build)", add_idx_us, 1, extra);
    }
    bench_table_section_end();

    /* ---- Bulk ops on the indexed object --------------------------------
       Same N=10000-in-one-request shape as bench-kv, but against the
       fully-indexed invoice schema (14 indexes incl. 4 composites) so
       it exercises the indexed bulk paths: pre_commit hooks fire under
       the kf wrlock and update each indexed field's btree shards.
       Order: get, exists, update (touches one indexed field), delete.
       Bulk-delete is destructive but it's the last bench step. */
    long bget_us = 0, bexi_us = 0, bupd_us = 0, bdel_us = 0;
    {
        const int N = 10000;
        const int START = 0;        /* first 10K invoices, all alive after the indexed insert */

        /* Pre-build the JSON keys array — reused for get/exists/delete. */
        size_t key_buf_cap = (size_t)N * 24 + 64;
        char *key_arr = malloc(key_buf_cap);
        size_t kp = 0;
        kp += (size_t)snprintf(key_arr + kp, key_buf_cap - kp, "[");
        for (int i = 0; i < N; i++) {
            char key[16]; make_inv_key(START + i, key);
            kp += (size_t)snprintf(key_arr + kp, key_buf_cap - kp,
                                    "%s\"%s\"", i ? "," : "", key);
        }
        kp += (size_t)snprintf(key_arr + kp, key_buf_cap - kp, "]");

        /* bulk-get */
        {
            size_t cap = key_buf_cap + 128;
            char *req = malloc(cap);
            snprintf(req, cap,
                "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bench\","
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
                "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"bench\","
                "\"keys\":%s}", key_arr);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bexi_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }

        /* bulk-update-json — patch a non-indexed field (status is indexed,
           keep this simple by patching `notes`/`description`-equivalent;
           use the bench schema's `metaJson` field which exists in the
           invoice schema). Touching an indexed field would also exercise
           the index drop+insert hot path — switch the field name below
           to `status` if you want to bench that. */
        {
            size_t cap = (size_t)N * 96 + 256;
            char *req = malloc(cap);
            size_t p = 0;
            p += (size_t)snprintf(req + p, cap - p,
                "{\"mode\":\"bulk-update\",\"dir\":\"default\","
                "\"object\":\"bench\",\"records\":[");
            for (int i = 0; i < N; i++) {
                char key[16]; make_inv_key(START + i, key);
                p += (size_t)snprintf(req + p, cap - p,
                    "%s{\"key\":\"%s\",\"value\":{\"status\":\"APPROVED\"}}",
                    i ? "," : "", key);
            }
            p += (size_t)snprintf(req + p, cap - p, "]}");
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bupd_us = (long)((bench_now_ns() - t0) / 1000);
            free(resp); resp = NULL;
            free(req);
        }

        /* bulk-delete (destructive — last bench op). */
        {
            size_t cap = key_buf_cap + 128;
            char *req = malloc(cap);
            snprintf(req, cap,
                "{\"mode\":\"bulk-delete\",\"dir\":\"default\","
                "\"object\":\"bench\",\"keys\":%s}", key_arr);
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
        bench_table_section_begin("Bulk ops on indexed object (N=10000)");
        bench_table_record("BULK GET    x10000 in one call", bget_us, 1, e_get);
        bench_table_record("BULK EXISTS x10000 in one call", bexi_us, 1, e_exi);
        bench_table_record("BULK UPDATE x10000 (touches indexed status)", bupd_us, 1, e_upd);
        bench_table_record("BULK DELETE x10000 (drops idx entries)", bdel_us, 1, e_del);
        bench_table_section_end();
    }

    /* JSON blob no longer needed. */
    free(json_buf); json_buf = NULL;

    /* ---- 9. FIND query battery (13 queries) — table view --------------- */
    {
        /* Supplier/buyer IDs for eq queries — pool entry 0 */
        char sup0[48];   make_supplier_id(0, sup0);
        char buyer0[48]; make_buyer_id(0, buyer0);

        bench_table_section_begin("FIND battery (limit=10)");

        char req_sup[512];
        snprintf(req_sup, sizeof(req_sup),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"supplierId\",\"op\":\"eq\",\"value\":\"%s\"}],"
            "\"limit\":10}", sup0);
        bench_table_run(tc, "eq supplierId (~10K matches)", req_sup);

        char req_buy[512];
        snprintf(req_buy, sizeof(req_buy),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"buyerId\",\"op\":\"eq\",\"value\":\"%s\"}],"
            "\"limit\":10}", buyer0);
        bench_table_run(tc, "eq buyerId (~2K matches)", req_buy);

        bench_table_run(tc, "contains number (idx leaf scan)",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"number\",\"op\":\"contains\",\"value\":\"00050\"}],"
            "\"limit\":10}");

        bench_table_run(tc, "contains batchNumber (idx leaf scan)",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"batchNumber\",\"op\":\"contains\",\"value\":\"BATCH-05\"}],"
            "\"limit\":10}");

        bench_table_run(tc, "IN indexed (2 statuses)",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"APPROVED\",\"PENDING\"]}],"
            "\"limit\":10}");

        bench_table_run(tc, "indexed status + non-idx currency",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":["
            "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
            "{\"field\":\"currencyCode\",\"op\":\"eq\",\"value\":\"MYR\"}"
            "],\"limit\":10}");

        bench_table_run(tc, "indexed status + amount range",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":["
            "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
            "{\"field\":\"totalIncludingTax\",\"op\":\"gte\",\"value\":\"10000\"}"
            "],\"limit\":10}");

        bench_table_run(tc, "composite status+invoiceDate starts",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"status+invoiceDate\",\"op\":\"starts\","
            "\"value\":\"APPROVED2024\"}],\"limit\":10}");

        bench_table_run(tc, "composite status+source eq",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"status+source\",\"op\":\"eq\","
            "\"value\":\"APPROVEDAPI\"}],\"limit\":10}");

        bench_table_run(tc, "composite irbmStatus+pdfSent eq",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"irbmStatus+pdfSent\",\"op\":\"eq\","
            "\"value\":\"VALIDtrue\"}],\"limit\":10}");

        bench_table_run(tc, "RANGE invoiceDate (gte+lte)",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":["
            "{\"field\":\"invoiceDate\",\"op\":\"gte\",\"value\":\"20240101000000\"},"
            "{\"field\":\"invoiceDate\",\"op\":\"lte\",\"value\":\"20240201000000\"}"
            "],\"limit\":10}");

        bench_table_run(tc, "RANGE createdAt (gte+lte)",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":["
            "{\"field\":\"createdAt\",\"op\":\"gte\",\"value\":\"20240101000000\"},"
            "{\"field\":\"createdAt\",\"op\":\"lte\",\"value\":\"20240115000000\"}"
            "],\"limit\":10}");

        bench_table_run(tc, "OR: two indexed statuses",
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"or\":["
            "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
            "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"PENDING\"}"
            "]}],\"limit\":10}");
        bench_table_section_end();
    }

    /* ---- 10. FETCH / KEYS / COUNT — table view ------------------------- */
    bench_table_section_begin("FETCH / KEYS / COUNT");
    bench_table_run(tc, "FETCH page=100 offset=5000",
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"5000\",\"limit\":\"100\"}");
    bench_table_run(tc, "FETCH page=100 with projection",
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"0\",\"limit\":\"100\","
        "\"fields\":[\"number\",\"status\",\"totalIncludingTax\",\"supplierId\"]}");
    bench_table_run(tc, "KEYS first 100",
        "{\"mode\":\"keys\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"0\",\"limit\":\"100\"}");
    bench_table_run(tc, "COUNT full object",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"bench\"}");
    bench_table_section_end();

    /* ---- 11. SINGLE DELETE x1000 — captured into Maintenance section -- */
    long single_del_total_us = 0; uint64_t single_del_p50 = 0;
    {
        const int N = 1000;
        uint64_t *samples = malloc((size_t)N * sizeof(uint64_t));
        BenchHist h;
        bench_hist_init(&h, samples, (size_t)N);
        uint64_t wall_start = bench_now_ns();
        for (int i = 0; i < N; i++) {
            char key[16];
            make_inv_key((int)((unsigned)rand() % (unsigned)COUNT), key);
            char req[256];
            snprintf(req, sizeof(req),
                     "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"bench\","
                     "\"key\":\"%s\"}", key);
            uint64_t t0 = bench_now_ns();
            tc_request(tc, req, &resp);
            bench_hist_add(&h, bench_now_ns() - t0);
            free(resp); resp = NULL;
        }
        single_del_total_us = (long)((bench_now_ns() - wall_start) / 1000);
        single_del_p50 = bench_hist_p50_ns(&h);
        free(samples);
    }

    /* ---- 12-14. BULK DELETE / VACUUM / RECOUNT + SINGLE DELETE summary -- */
    {
        /* Build a 1000-key array request once so the table row is a clean
           single-shot timing. Random keys from the surviving population. */
        size_t kbuf_cap = (size_t)1000 * 20 + 8;
        char  *kbuf     = malloc(kbuf_cap);
        if (!kbuf) {
            fprintf(stderr, "bench-invoice: OOM bulk-delete key buf\n");
            tc_close(tc); test_env_stop(&env); return 1;
        }
        size_t kpos = 0;
        kbuf[kpos++] = '[';
        for (int i = 0; i < 1000; i++) {
            char key[16];
            make_inv_key((int)((unsigned)rand() % (unsigned)COUNT), key);
            int n = snprintf(kbuf + kpos, kbuf_cap - kpos,
                             "\"%s\"%s", key, i < 999 ? "," : "");
            if (n <= 0) break;
            kpos += (size_t)n;
        }
        kbuf[kpos++] = ']';
        kbuf[kpos]   = '\0';

        size_t req_cap = kpos + 128;
        char  *req     = malloc(req_cap);
        if (!req) { free(kbuf); tc_close(tc); test_env_stop(&env); return 1; }
        snprintf(req, req_cap,
                 "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bench\","
                 "\"keys\":%s}", kbuf);
        free(kbuf);

        bench_table_section_begin("Maintenance ops");
        {
            char extra[48];
            snprintf(extra, sizeof(extra), "p50=%.0fµs  %.1f k op/s",
                     (double)single_del_p50 / 1000.0,
                     1000.0 / ((double)single_del_total_us / 1e6) / 1000.0);
            bench_table_record("SINGLE DELETE x1000 (14 idx)",
                               single_del_total_us, 1, extra);
        }
        bench_table_run(tc, "BULK DELETE 1000 keys (14 idx)", req);
        free(req);
        bench_table_run(tc, "VACUUM",
            "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"bench\"}");
        bench_table_run(tc, "RECOUNT",
            "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"bench\"}");
        bench_table_section_end();
    }

    /* ---- 15. DISK USAGE --------------------------------------------------- */
    {
        char du_cmd[512];
        snprintf(du_cmd, sizeof(du_cmd), "du -sh \"%s/default/bench\"",
                 env.db_root);
        printf("DISK USAGE: ");
        fflush(stdout);
        system(du_cmd);
        printf("\n");
    }

    printf("======================================\n");
    printf("  Invoice Benchmark complete\n");
    printf("======================================\n");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-invoice", bench_invoice_run)
