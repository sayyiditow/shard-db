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

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\","
        "\"splits\":64,\"max_key\":16,"
        "\"fields\":[" INVOICE_SCHEMA_FIELDS "],"
        "\"indexes\":[]}",
        &resp);
    free(resp); resp = NULL;

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

    /* ---- 3. BULK INSERT (no indexes) ----------------------------------- */
    printf("--- BULK INSERT (%d records, no index) ---\n", COUNT);
    fflush(stdout);
    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, json_buf, json_size);
        uint64_t elapsed = bench_now_ns() - t0;
        double ms  = (double)elapsed / 1e6;
        double mps = (double)COUNT / ((double)elapsed / 1e9) / 1e6;
        printf("BULK INSERT (no idx): rows=%d  wall=%.0fms  throughput=%.2f M/sec\n",
               COUNT, ms, mps);
        if (resp) { printf("  response: %.120s\n", resp); free(resp); resp = NULL; }
    }

    /* size check */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"bench\"}", &resp);
    printf("SIZE: %s\n\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* ---- 4. GET x1000 pipelined ---------------------------------------- */
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
        bench_hist_report(&h, "GET x1000 pipelined", bench_now_ns() - wall_start);
        printf("\n");
        free(samples);
    }

    /* ---- 5. EXISTS x1000 pipelined ------------------------------------- */
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
        bench_hist_report(&h, "EXISTS x1000 pipelined", bench_now_ns() - wall_start);
        printf("\n");
        free(samples);
    }

    /* ---- 6. ADD INDEXES (14 indexes) ----------------------------------- */
    printf("--- ADD INDEXES (14 indexes, single-pass) ---\n");
    fflush(stdout);
    {
        uint64_t t0 = bench_now_ns();
        tc_request(tc,
            "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\","
            "\"fields\":[" INVOICE_INDEX_FIELDS "]}",
            &resp);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("ADD INDEXES: wall=%.0fms\n", (double)elapsed / 1e6);
        if (resp) { printf("  response: %.120s\n", resp); free(resp); resp = NULL; }
    }
    printf("\n");

    /* ---- 7. INDEXED SEARCH x100 (supplierId) --------------------------- */
    printf("--- INDEXED SEARCH x100 supplierId (pipelined) ---\n");
    fflush(stdout);
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
        bench_hist_report(&h, "INDEXED SEARCH x100 supplierId",
                          bench_now_ns() - wall_start);
        printf("\n");
        free(samples);
    }

    /* ---- 8. BULK INSERT with indexes ------------------------------------ */
    printf("--- BULK INSERT (%d records, 14 indexes) ---\n", COUNT);
    fflush(stdout);

    /* Truncate first */
    tc_request(tc, "{\"mode\":\"truncate\",\"dir\":\"default\",\"object\":\"bench\"}",
               &resp);
    free(resp); resp = NULL;

    {
        uint64_t t0 = bench_now_ns();
        resp = bulk_insert_memfd(tc, json_buf, json_size);
        uint64_t elapsed = bench_now_ns() - t0;
        double ms  = (double)elapsed / 1e6;
        double mps = (double)COUNT / ((double)elapsed / 1e9) / 1e6;
        printf("BULK INSERT (14 idx): rows=%d  wall=%.0fms  throughput=%.2f M/sec\n",
               COUNT, ms, mps);
        if (resp) { printf("  response: %.120s\n", resp); free(resp); resp = NULL; }
    }

    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"bench\"}", &resp);
    printf("SIZE: %s\n\n", resp ? resp : "(err)");
    free(resp); resp = NULL;

    /* JSON blob no longer needed. */
    free(json_buf); json_buf = NULL;

    /* ---- 9. FIND query battery (13 queries) ----------------------------- */
    printf("--- FIND query battery ---\n\n");

    /* Helper macro: single-shot timed request */
#define TIMED_FIND(label, req_json) do { \
        printf("--- FIND: %s ---\n", label); \
        fflush(stdout); \
        uint64_t _t0 = bench_now_ns(); \
        tc_request(tc, req_json, &resp); \
        uint64_t _el = bench_now_ns() - _t0; \
        printf("  wall=%.0fms  resp=%.80s\n\n", (double)_el / 1e6, resp ? resp : "(err)"); \
        free(resp); resp = NULL; \
    } while (0)

    /* Supplier ID for eq queries — use pool entry 0 */
    char sup0[48];
    make_supplier_id(0, sup0);
    char buyer0[48];
    make_buyer_id(0, buyer0);

    /* 1. eq indexed supplierId, ~10K matches */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"supplierId\",\"op\":\"eq\",\"value\":\"%s\"}],"
            "\"limit\":10}", sup0);
        TIMED_FIND("eq (indexed supplierId, ~10K matches)", req);
    }

    /* 2. eq indexed buyerId, ~2K matches */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
            "\"criteria\":[{\"field\":\"buyerId\",\"op\":\"eq\",\"value\":\"%s\"}],"
            "\"limit\":10}", buyer0);
        TIMED_FIND("eq (indexed buyerId, ~2K matches)", req);
    }

    /* 3. contains number (indexed leaf scan) */
    TIMED_FIND("contains number (indexed leaf scan)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"number\",\"op\":\"contains\",\"value\":\"00050\"}],"
        "\"limit\":10}");

    /* 4. contains batchNumber (indexed leaf scan) */
    TIMED_FIND("contains batchNumber (indexed leaf scan)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"batchNumber\",\"op\":\"contains\",\"value\":\"BATCH-05\"}],"
        "\"limit\":10}");

    /* 5. IN indexed (2 statuses) */
    TIMED_FIND("IN indexed (2 statuses)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"APPROVED\",\"PENDING\"]}],"
        "\"limit\":10}");

    /* 6. indexed status + non-indexed currency */
    TIMED_FIND("indexed status + non-indexed currency",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":["
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
        "{\"field\":\"currencyCode\",\"op\":\"eq\",\"value\":\"MYR\"}"
        "],\"limit\":10}");

    /* 7. indexed status + amount range */
    TIMED_FIND("indexed status + amount range",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":["
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
        "{\"field\":\"totalIncludingTax\",\"op\":\"gte\",\"value\":\"10000\"}"
        "],\"limit\":10}");

    /* 8. composite status+invoiceDate starts */
    TIMED_FIND("composite status+invoiceDate starts",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"status+invoiceDate\",\"op\":\"starts\","
        "\"value\":\"APPROVED2024\"}],\"limit\":10}");

    /* 9. composite status+source eq */
    TIMED_FIND("composite status+source eq",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"status+source\",\"op\":\"eq\","
        "\"value\":\"APPROVEDAPI\"}],\"limit\":10}");

    /* 10. composite irbmStatus+pdfSent eq */
    TIMED_FIND("composite irbmStatus+pdfSent eq",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"field\":\"irbmStatus+pdfSent\",\"op\":\"eq\","
        "\"value\":\"VALIDtrue\"}],\"limit\":10}");

    /* 11. RANGE indexed invoiceDate */
    TIMED_FIND("RANGE indexed invoiceDate (gte+lte)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":["
        "{\"field\":\"invoiceDate\",\"op\":\"gte\",\"value\":\"20240101000000\"},"
        "{\"field\":\"invoiceDate\",\"op\":\"lte\",\"value\":\"20240201000000\"}"
        "],\"limit\":10}");

    /* 12. RANGE indexed createdAt */
    TIMED_FIND("RANGE indexed createdAt (gte+lte)",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":["
        "{\"field\":\"createdAt\",\"op\":\"gte\",\"value\":\"20240101000000\"},"
        "{\"field\":\"createdAt\",\"op\":\"lte\",\"value\":\"20240115000000\"}"
        "],\"limit\":10}");

    /* 13. OR: two status values */
    TIMED_FIND("OR: two indexed statuses",
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bench\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"APPROVED\"},"
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"PENDING\"}"
        "]}],\"limit\":10}");

#undef TIMED_FIND

    /* ---- 10. FETCH / KEYS ---------------------------------------------- */
    printf("--- FETCH / KEYS ---\n\n");

#define TIMED_Q(label, req_json) do { \
        printf("--- %s ---\n", label); \
        fflush(stdout); \
        uint64_t _t0 = bench_now_ns(); \
        tc_request(tc, req_json, &resp); \
        uint64_t _el = bench_now_ns() - _t0; \
        printf("  wall=%.0fms\n\n", (double)_el / 1e6); \
        free(resp); resp = NULL; \
    } while (0)

    TIMED_Q("FETCH: page of 100, offset 5000",
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"5000\",\"limit\":\"100\"}");

    TIMED_Q("FETCH: with projection",
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"0\",\"limit\":\"100\","
        "\"fields\":[\"number\",\"status\",\"totalIncludingTax\",\"supplierId\"]}");

    TIMED_Q("KEYS: first 100",
        "{\"mode\":\"keys\",\"dir\":\"default\",\"object\":\"bench\","
        "\"offset\":\"0\",\"limit\":\"100\"}");

    TIMED_Q("COUNT (full object)",
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"bench\"}");

#undef TIMED_Q

    /* ---- 11. SINGLE DELETE x1000 --------------------------------------- */
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
        bench_hist_report(&h, "SINGLE DELETE x1000 (14 indexes)",
                          bench_now_ns() - wall_start);
        printf("\n");
        free(samples);
    }

    /* ---- 12. BULK DELETE x1000 ---------------------------------------- */
    printf("--- BULK DELETE x1000 (inline JSON, 14 indexes) ---\n");
    fflush(stdout);
    {
        /* Build key array: ["INV-NNN",...] — 1000 random keys */
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

        /* Build request */
        size_t req_cap = kpos + 128;
        char  *req     = malloc(req_cap);
        if (!req) { free(kbuf); tc_close(tc); test_env_stop(&env); return 1; }
        snprintf(req, req_cap,
                 "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bench\","
                 "\"keys\":%s}", kbuf);
        free(kbuf);

        uint64_t t0 = bench_now_ns();
        tc_request(tc, req, &resp);
        uint64_t elapsed = bench_now_ns() - t0;
        free(req);
        printf("BULK DELETE 1000: wall=%.0fms  resp=%.80s\n\n",
               (double)elapsed / 1e6, resp ? resp : "(err)");
        free(resp); resp = NULL;
    }

    /* ---- 13. VACUUM ------------------------------------------------------- */
    printf("--- VACUUM ---\n");
    fflush(stdout);
    {
        uint64_t t0 = bench_now_ns();
        tc_request(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"bench\"}",
            &resp);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("VACUUM: wall=%.0fms\n\n", (double)elapsed / 1e6);
        free(resp); resp = NULL;
    }

    /* ---- 14. RECOUNT ------------------------------------------------------ */
    printf("--- RECOUNT ---\n");
    fflush(stdout);
    {
        uint64_t t0 = bench_now_ns();
        tc_request(tc,
            "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"bench\"}",
            &resp);
        uint64_t elapsed = bench_now_ns() - t0;
        printf("RECOUNT: wall=%.0fms  resp=%.80s\n\n",
               (double)elapsed / 1e6, resp ? resp : "(err)");
        free(resp); resp = NULL;
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
