/* src/bench/bench_parallel.c — port of bench/bench-parallel.sh
 *
 * Parallel-connection invoice bench: 1M records × 5 conns × 200K chunks,
 * 64-field schema, 14 indexes. 5 tests covering single + parallel ×
 * JSON + CSV × no-idx-then-add vs pre-existing-indexes paths.
 *
 * Shares the schema definition with bench_invoice via
 * bench_invoice_schema.h. CSV delimiter is '|' (matching the bash bench
 * to avoid commas inside the quoted varchar values).
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
#include <sys/syscall.h>
#include <unistd.h>

#define TOTAL   1000000
#define CHUNK   200000
#define CONNS   5
#define SPLITS  64
#define NCHUNKS (TOTAL / CHUNK)

/* ----------------------------------------------------------------- helpers */

/* Write data into a Linux memfd, return the open fd (caller closes).
   The daemon reads it via /proc/<bench-pid>/fd/<n>. */
static int make_memfd(const char *name, const char *data, size_t size)
{
#if defined(__x86_64__)
    int fd = (int)syscall(319, name, 0);
#elif defined(__aarch64__)
    int fd = (int)syscall(279, name, 0);
#else
    char path[] = "/tmp/bench-par-XXXXXX";
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

/* Pools for synthetic field values — same cardinalities as the bash bench. */
static const char *STATUSES[]      = { "DRAFT","PENDING","APPROVED","REJECTED","CANCELLED" };
static const char *IRBM_STATUSES[] = { "PENDING","VALID","INVALID","CANCELLED" };
static const char *CURRENCIES[]    = { "MYR","USD","SGD","EUR","GBP" };
static const char *INV_TYPES[]     = { "STANDARD","CREDIT_NOTE","DEBIT_NOTE","REFUND","SELF_BILLED" };
static const char *FREQS[]         = { "MONTHLY","QUARTERLY","YEARLY","DAILY","WEEKLY" };
static const char *SOURCES[]       = { "API","PORTAL","BATCH","IMPORT" };

/* ---------------------------------------- JSON chunk builder */

/*
 * Build a JSON array for records [from_i, to_i) into *out_buf / *out_size.
 * Uses {"key":"...","value":{...}} array form.  Caller must free(*out_buf).
 */
static int build_json_chunk(int from_i, int to_i,
                             char **out_buf, size_t *out_size)
{
    int count = to_i - from_i;
    /* Each record is roughly 1000 bytes in the wide invoice JSON. */
    size_t cap = (size_t)count * 1024 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;

    size_t pos = 0;
    buf[pos++] = '[';

    for (int i = from_i; i < to_i; i++) {
        /* datetime: 2024-MM-DD HH:MM:SS in compact form */
        int day   = (i % 366) + 1;
        int month = (day / 31) + 1;
        if (month > 12) month = 12;
        int dom   = (day % 28) + 1;
        char inv_date[16], created_at[16], updated_at[16], sub_date[16], val_date[16];
        snprintf(inv_date,   sizeof(inv_date),   "2024%02d%02d000000", month, dom);
        snprintf(created_at, sizeof(created_at), "2024%02d%02d000000", month, dom);
        snprintf(updated_at, sizeof(updated_at), "2024%02d%02d010000", month, dom);
        snprintf(sub_date,   sizeof(sub_date),   "2024%02d%02d003000", month, dom);
        snprintf(val_date,   sizeof(val_date),   "2024%02d%02d010000", month, dom);

        const char *status   = STATUSES[i % 5];
        const char *irbm_st  = IRBM_STATUSES[i % 4];
        const char *currency = CURRENCIES[i % 5];
        const char *inv_type = INV_TYPES[i % 5];
        const char *freq     = FREQS[i % 5];
        const char *source   = SOURCES[i % 4];
        int consolidated     = (i % 7 == 0) ? 1 : 0;
        int pdf_sent         = (i % 3 != 0) ? 1 : 0;
        double exc_tax       = 100.0 + (double)(i % 49900);
        double tax_amt       = exc_tax * 0.06;
        double inc_tax       = exc_tax + tax_amt;

        char batch[16], number[24], buyer[48], supplier[48];
        snprintf(batch,    sizeof(batch),    "BATCH-%04d", i / 1000);
        snprintf(number,   sizeof(number),   "INV-2026-%07d", i);
        snprintf(buyer,    sizeof(buyer),    "BUY-%032x", (unsigned)(i % 500 * 1234567891u));
        snprintf(supplier, sizeof(supplier), "SUP-%032x", (unsigned)(i % 100 * 2654435761u));

        int n = snprintf(buf + pos, cap - pos,
            "%s{\"key\":\"INV-%07d\",\"value\":{"
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
            "}}",
            i > from_i ? "," : "",
            i,
            buyer, number, supplier, source, batch,
            inv_date, created_at, updated_at, sub_date, val_date,
            status, irbm_st, currency, inv_type, freq,
            consolidated ? "true" : "false",
            pdf_sent ? "true" : "false",
            exc_tax, inc_tax, inc_tax, exc_tax, tax_amt, exc_tax, tax_amt
        );
        if (n <= 0 || pos + (size_t)n + 2 >= cap) {
            free(buf);
            return -1;
        }
        pos += (size_t)n;
    }
    buf[pos++] = ']';
    buf[pos]   = '\0';
    *out_buf   = buf;
    *out_size  = pos;
    return 0;
}

/* ---------------------------------------- CSV chunk builder */

/*
 * Build CSV for records [from_i, to_i) using '|' as delimiter.
 * Column order matches INVOICE_SCHEMA_FIELDS (64 fields, 0-based):
 *   0  buyerId          4  supplierId
 *   2  number           6  source
 *   11 batchNumber      24 invoiceDate
 *   25 createdAt        26 updatedAt
 *   29 submissionDate   30 validationDate
 *   35 status           36 irbmStatus
 *   37 currencyCode     38 invoiceType
 *   39 frequency        40 consolidated
 *   41 pdfSent          42 exchangeRate
 *   43 totalExcludingTax  44 totalIncludingTax
 *   45 totalPayableAmount 46 totalNetAmount
 *   49 totalTaxAmount    54 totalSalesTaxable
 *   55 totalSalesTaxAmount
 * All other columns are left empty (schema defaults).
 * Caller must free(*out_buf).
 */
static int build_csv_chunk(int from_i, int to_i,
                            char **out_buf, size_t *out_size)
{
    int count = to_i - from_i;
    /* Each line: key + 64 fields + delimiters + newline, rough upper bound. */
    size_t cap = (size_t)count * 512 + 16;
    char *buf = malloc(cap);
    if (!buf) return -1;

    size_t pos = 0;

    for (int i = from_i; i < to_i; i++) {
        int day   = (i % 366) + 1;
        int month = (day / 31) + 1;
        if (month > 12) month = 12;
        int dom   = (day % 28) + 1;
        char inv_date[16], created_at[16], updated_at[16], sub_date[16], val_date[16];
        snprintf(inv_date,   sizeof(inv_date),   "2024%02d%02d000000", month, dom);
        snprintf(created_at, sizeof(created_at), "2024%02d%02d000000", month, dom);
        snprintf(updated_at, sizeof(updated_at), "2024%02d%02d010000", month, dom);
        snprintf(sub_date,   sizeof(sub_date),   "2024%02d%02d003000", month, dom);
        snprintf(val_date,   sizeof(val_date),   "2024%02d%02d010000", month, dom);

        const char *status   = STATUSES[i % 5];
        const char *irbm_st  = IRBM_STATUSES[i % 4];
        const char *currency = CURRENCIES[i % 5];
        const char *inv_type = INV_TYPES[i % 5];
        const char *freq     = FREQS[i % 5];
        const char *source   = SOURCES[i % 4];
        int consolidated     = (i % 7 == 0) ? 1 : 0;
        int pdf_sent         = (i % 3 != 0) ? 1 : 0;
        double exc_tax       = 100.0 + (double)(i % 49900);
        double tax_amt       = exc_tax * 0.06;
        double inc_tax       = exc_tax + tax_amt;

        char batch[16], number[24], buyer[48], supplier[48];
        snprintf(batch,    sizeof(batch),    "BATCH-%04d", i / 1000);
        snprintf(number,   sizeof(number),   "INV-2026-%07d", i);
        snprintf(buyer,    sizeof(buyer),    "BUY-%032x", (unsigned)(i % 500 * 1234567891u));
        snprintf(supplier, sizeof(supplier), "SUP-%032x", (unsigned)(i % 100 * 2654435761u));

        /* Key. */
        int n = snprintf(buf + pos, cap - pos, "INV-%07d", i);
        if (n <= 0 || pos + (size_t)n + 2 >= cap) { free(buf); return -1; }
        pos += (size_t)n;

        /* 64 columns in schema order, separated by '|'. */
        /* Schema column ordering (0-based):
           0:buyerId 1:version 2:number 3:originalReference 4:supplierId
           5:irbmIdentifier 6:source 7:createdBy 8:updatedBy 9:irbmLongId
           10:originalReferenceNumber 11:batchNumber 12:submissionUid
           13:submittedBy 14:taxExemptionReason 15:requestForRejectionReason
           16:cancellationReason 17:approvedBy 18:approvalRemarks
           19:transactionCodeId 20:productCodeId 21:cancellationSource
           22:internalCancellationBy 23:irbmSubmissionResponse
           24:invoiceDate 25:createdAt 26:updatedAt
           27:requestForRejectionDate 28:cancellationDate 29:submissionDate
           30:validationDate 31:approvalDate 32:billingPeriodStart
           33:billingPeriodEnd 34:internalCancellationDate
           35:status 36:irbmStatus 37:currencyCode 38:invoiceType
           39:frequency 40:consolidated 41:pdfSent 42:exchangeRate
           43:totalExcludingTax 44:totalIncludingTax 45:totalPayableAmount
           46:totalNetAmount 47:totalDiscountAmount 48:totalFee
           49:totalTaxAmount 50:roundingAmount 51:amountExemptedFromTax
           52:invoiceAdditionalDiscountAmount 53:invoiceAdditionalFeeAmount
           54:totalSalesTaxable 55:totalSalesTaxAmount
           56:totalServiceTaxable 57:totalServiceTaxAmount
           58:totalTourismTaxable 59:totalTourismTaxAmount
           60:totalHighValueTaxable 61:totalHighValueTaxAmount
           62:totalLowValueTaxable 63:totalLowValueTaxAmount
        */
        const char *cols[64];
        char col_buf[12][32];  /* scratch for per-row formatted values */
        memset(cols, 0, sizeof(cols));
        cols[0]  = buyer;
        cols[1]  = "1.0";         /* version */
        cols[2]  = number;
        /* cols[3] = originalReference — empty */
        cols[4]  = supplier;
        /* cols[5] = irbmIdentifier — empty */
        cols[6]  = source;
        /* cols[7..10] — empty */
        cols[11] = batch;
        /* cols[12..23] — empty */
        cols[24] = inv_date;
        cols[25] = created_at;
        cols[26] = updated_at;
        /* cols[27..28] — empty */
        cols[29] = sub_date;
        cols[30] = val_date;
        /* cols[31..34] — empty */
        cols[35] = status;
        cols[36] = irbm_st;
        cols[37] = currency;
        cols[38] = inv_type;
        cols[39] = freq;
        cols[40] = consolidated ? "true" : "false";
        cols[41] = pdf_sent ? "true" : "false";
        cols[42] = "1.0";   /* exchangeRate */
        snprintf(col_buf[0], sizeof(col_buf[0]),  "%.2f", exc_tax);  cols[43] = col_buf[0];
        snprintf(col_buf[1], sizeof(col_buf[1]),  "%.2f", inc_tax);  cols[44] = col_buf[1];
        snprintf(col_buf[2], sizeof(col_buf[2]),  "%.2f", inc_tax);  cols[45] = col_buf[2];
        snprintf(col_buf[3], sizeof(col_buf[3]),  "%.2f", exc_tax);  cols[46] = col_buf[3];
        /* cols[47..48] — empty (0.0) */
        snprintf(col_buf[4], sizeof(col_buf[4]),  "%.2f", tax_amt);  cols[49] = col_buf[4];
        /* cols[50..53] — empty */
        snprintf(col_buf[5], sizeof(col_buf[5]),  "%.2f", exc_tax);  cols[54] = col_buf[5];
        snprintf(col_buf[6], sizeof(col_buf[6]),  "%.2f", tax_amt);  cols[55] = col_buf[6];
        /* cols[56..63] — empty */

        for (int col = 0; col < 64; col++) {
            size_t left = cap - pos;
            const char *val = cols[col] ? cols[col] : "";
            n = snprintf(buf + pos, left, "|%s", val);
            if (n <= 0 || pos + (size_t)n + 2 >= cap) { free(buf); return -1; }
            pos += (size_t)n;
        }

        if (pos + 2 >= cap) { free(buf); return -1; }
        buf[pos++] = '\n';
    }
    buf[pos] = '\0';
    *out_buf  = buf;
    *out_size = pos;
    return 0;
}

/* ------------------------------------------------- bulk-insert senders */

static int do_bulk_insert_json(TestClient *tc, int memfd)
{
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\","
        "\"file\":\"/proc/%d/fd/%d\"}", (int)getpid(), memfd);
    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    free(resp);
    return rc;
}

static int do_bulk_insert_csv(TestClient *tc, int memfd)
{
    char req[300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
        "\"object\":\"bench\",\"file\":\"/proc/%d/fd/%d\",\"delimiter\":\"|\"}",
        (int)getpid(), memfd);
    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    free(resp);
    return rc;
}

/* ------------------------------------------------ object lifecycle */

static void recreate_object(TestClient *tc, int with_indexes)
{
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"bench\"}",
        &resp);
    free(resp); resp = NULL;

    /* Build the create-object request.  The schema macros expand to large
       string literals, so we heap-allocate. */
    size_t req_cap = 8192;
    char  *req     = malloc(req_cap);
    if (!req) return;

    int sv = bench_storage_version();
    if (with_indexes) {
        snprintf(req, req_cap,
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\","
            "\"splits\":%d,\"max_key\":64,\"storage_version\":%d,"
            "\"fields\":[" INVOICE_SCHEMA_FIELDS "],"
            "\"indexes\":[" INVOICE_INDEX_FIELDS "]}",
            SPLITS, sv);
    } else {
        snprintf(req, req_cap,
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\","
            "\"splits\":%d,\"max_key\":64,\"storage_version\":%d,"
            "\"fields\":[" INVOICE_SCHEMA_FIELDS "],"
            "\"indexes\":[]}",
            SPLITS, sv);
    }
    tc_request(tc, req, &resp);
    free(resp);
    free(req);
}

static void add_all_indexes(TestClient *tc)
{
    char req[2048];
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\","
        "\"fields\":[" INVOICE_INDEX_FIELDS "]}");
    char *resp = NULL;
    tc_request(tc, req, &resp);
    free(resp);
}

/* ------------------------------------------------- parallel worker */

typedef struct {
    int port;
    int memfd;
    int is_csv;
    int rc;
} ParArg;

static void *parallel_worker(void *vp)
{
    ParArg *w = vp;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 180000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->rc = -1; return NULL; }
    if (w->is_csv)
        w->rc = do_bulk_insert_csv(tc, w->memfd);
    else
        w->rc = do_bulk_insert_json(tc, w->memfd);
    tc_close(tc);
    return NULL;
}

static void run_parallel(int port, int is_csv, int *chunk_memfds)
{
    pthread_t tids[NCHUNKS];
    ParArg    args[NCHUNKS];
    for (int i = 0; i < NCHUNKS; i++) {
        args[i] = (ParArg){ .port = port, .memfd = chunk_memfds[i],
                            .is_csv = is_csv, .rc = 0 };
        pthread_create(&tids[i], NULL, parallel_worker, &args[i]);
    }
    for (int i = 0; i < NCHUNKS; i++)
        pthread_join(tids[i], NULL);
}

/* -------------------------------------------------------- build a memfd set */

/* Build NCHUNKS JSON memfds.  Returns 0 on success; closes + returns -1 on OOM. */
static int build_json_memfds(int *out_fds)
{
    for (int c = 0; c < NCHUNKS; c++) {
        int from = c * CHUNK;
        int to   = from + CHUNK;
        char *buf = NULL; size_t sz = 0;
        if (build_json_chunk(from, to, &buf, &sz) != 0) {
            fprintf(stderr, "bench-parallel: OOM building JSON chunk %d\n", c);
            for (int j = 0; j < c; j++) close(out_fds[j]);
            return -1;
        }
        out_fds[c] = make_memfd("inv-par-json", buf, sz);
        free(buf);
        if (out_fds[c] < 0) {
            for (int j = 0; j < c; j++) close(out_fds[j]);
            return -1;
        }
    }
    return 0;
}

static int build_csv_memfds(int *out_fds)
{
    for (int c = 0; c < NCHUNKS; c++) {
        int from = c * CHUNK;
        int to   = from + CHUNK;
        char *buf = NULL; size_t sz = 0;
        if (build_csv_chunk(from, to, &buf, &sz) != 0) {
            fprintf(stderr, "bench-parallel: OOM building CSV chunk %d\n", c);
            for (int j = 0; j < c; j++) close(out_fds[j]);
            return -1;
        }
        out_fds[c] = make_memfd("inv-par-csv", buf, sz);
        free(buf);
        if (out_fds[c] < 0) {
            for (int j = 0; j < c; j++) close(out_fds[j]);
            return -1;
        }
    }
    return 0;
}

static void close_fds(int *fds, int n)
{
    for (int i = 0; i < n; i++) {
        if (fds[i] >= 0) { close(fds[i]); fds[i] = -1; }
    }
}

/* ---------------------------------------------------------------- main bench */

static int bench_parallel_run(void)
{
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        fprintf(stderr, "bench-parallel: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 600000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        fprintf(stderr, "bench-parallel: connect failed\n");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    printf("======================================\n");
    printf("  Invoice parallel bench: total=%d chunk=%d conns=%d splits=%d\n",
           TOTAL, CHUNK, CONNS, SPLITS);
    printf("  64-field schema, 14 indexes (incl. 4 composite)\n");
    printf("======================================\n\n");

    /* Pre-build single-blob memfds (tests 1a, 1b). */
    printf("Generating data blobs...\n");
    fflush(stdout);

    int single_json_fd, single_csv_fd;
    {
        char *buf = NULL; size_t sz = 0;
        if (build_json_chunk(0, TOTAL, &buf, &sz) != 0) {
            fprintf(stderr, "bench-parallel: OOM single JSON\n");
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  single JSON: %.1f MB\n", (double)sz / 1048576.0);
        single_json_fd = make_memfd("inv-par-json-single", buf, sz);
        free(buf);
    }
    {
        char *buf = NULL; size_t sz = 0;
        if (build_csv_chunk(0, TOTAL, &buf, &sz) != 0) {
            fprintf(stderr, "bench-parallel: OOM single CSV\n");
            close(single_json_fd);
            tc_close(tc); test_env_stop(&env); return 1;
        }
        printf("  single CSV:  %.1f MB\n\n", (double)sz / 1048576.0);
        single_csv_fd = make_memfd("inv-par-csv-single", buf, sz);
        free(buf);
    }

    int chunk_json[NCHUNKS], chunk_csv[NCHUNKS];
    for (int i = 0; i < NCHUNKS; i++) chunk_json[i] = chunk_csv[i] = -1;

    long us_1a = 0, us_1b = 0, us_2 = 0, us_3 = 0, us_4 = 0, us_5 = 0;
    long us_1a_idx = 0, us_1b_idx = 0, us_2_idx = 0, us_4_idx = 0;

    /* TEST 1a: single JSON, no idx, then add-14-indexes. */
    recreate_object(tc, 0);
    { uint64_t t0 = bench_now_ns();
      do_bulk_insert_json(tc, single_json_fd);
      us_1a = (long)((bench_now_ns() - t0) / 1000); }
    { uint64_t t0 = bench_now_ns();
      add_all_indexes(tc);
      us_1a_idx = (long)((bench_now_ns() - t0) / 1000); }

    /* TEST 1b: single CSV, no idx, then add-14-indexes. */
    recreate_object(tc, 0);
    lseek(single_csv_fd, 0, SEEK_SET);
    { uint64_t t0 = bench_now_ns();
      do_bulk_insert_csv(tc, single_csv_fd);
      us_1b = (long)((bench_now_ns() - t0) / 1000); }
    { uint64_t t0 = bench_now_ns();
      add_all_indexes(tc);
      us_1b_idx = (long)((bench_now_ns() - t0) / 1000); }

    /* TEST 2: parallel JSON 5×200K, no idx, then add-14-indexes. */
    recreate_object(tc, 0);
    if (build_json_memfds(chunk_json) != 0) {
        close(single_json_fd); close(single_csv_fd);
        tc_close(tc); test_env_stop(&env); return 1;
    }
    { uint64_t t0 = bench_now_ns();
      run_parallel(env.port, 0, chunk_json);
      us_2 = (long)((bench_now_ns() - t0) / 1000); }
    close_fds(chunk_json, NCHUNKS);
    { uint64_t t0 = bench_now_ns();
      add_all_indexes(tc);
      us_2_idx = (long)((bench_now_ns() - t0) / 1000); }

    /* TEST 3: parallel JSON 5×200K, WITH 14 pre-existing indexes. */
    recreate_object(tc, 1);
    if (build_json_memfds(chunk_json) != 0) {
        close(single_json_fd); close(single_csv_fd);
        tc_close(tc); test_env_stop(&env); return 1;
    }
    { uint64_t t0 = bench_now_ns();
      run_parallel(env.port, 0, chunk_json);
      us_3 = (long)((bench_now_ns() - t0) / 1000); }
    close_fds(chunk_json, NCHUNKS);

    /* TEST 4: parallel CSV 5×200K, no idx, then add-14-indexes. */
    recreate_object(tc, 0);
    if (build_csv_memfds(chunk_csv) != 0) {
        close(single_json_fd); close(single_csv_fd);
        tc_close(tc); test_env_stop(&env); return 1;
    }
    { uint64_t t0 = bench_now_ns();
      run_parallel(env.port, 1, chunk_csv);
      us_4 = (long)((bench_now_ns() - t0) / 1000); }
    close_fds(chunk_csv, NCHUNKS);
    { uint64_t t0 = bench_now_ns();
      add_all_indexes(tc);
      us_4_idx = (long)((bench_now_ns() - t0) / 1000); }

    /* TEST 5: parallel CSV 5×200K, WITH 14 pre-existing indexes. */
    recreate_object(tc, 1);
    if (build_csv_memfds(chunk_csv) != 0) {
        close(single_json_fd); close(single_csv_fd);
        tc_close(tc); test_env_stop(&env); return 1;
    }
    { uint64_t t0 = bench_now_ns();
      run_parallel(env.port, 1, chunk_csv);
      us_5 = (long)((bench_now_ns() - t0) / 1000); }
    close_fds(chunk_csv, NCHUNKS);

    /* ---- BULK INSERT throughput table ---- */
    {
#define MK(varname, us) \
        char varname[48]; \
        snprintf(varname, sizeof(varname), "%.2f M rows/s", \
                 (double)TOTAL / 1e6 / ((double)(us) / 1e6));
        MK(e1a, us_1a);  MK(e1b, us_1b);
        MK(e2,  us_2);   MK(e3,  us_3);
        MK(e4,  us_4);   MK(e5,  us_5);
#undef MK
        bench_table_section_begin("BULK INSERT 5×200K = 1M (single vs parallel)");
        bench_table_record("1a single  JSON  no idx",      us_1a, 1, e1a);
        bench_table_record("1b single  CSV   no idx",      us_1b, 1, e1b);
        bench_table_record("2  parallel JSON no idx (5 conns)", us_2, 1, e2);
        bench_table_record("3  parallel JSON 14-idx pre-existing", us_3, 1, e3);
        bench_table_record("4  parallel CSV  no idx (5 conns)",  us_4, 1, e4);
        bench_table_record("5  parallel CSV  14-idx pre-existing", us_5, 1, e5);
        bench_table_section_end();

        bench_table_section_begin("ADD INDEXES (post-load build)");
        bench_table_record("after 1a single JSON",       us_1a_idx, 1, "14 fields");
        bench_table_record("after 1b single CSV",        us_1b_idx, 1, "14 fields");
        bench_table_record("after 2  parallel JSON",     us_2_idx,  1, "14 fields");
        bench_table_record("after 4  parallel CSV",      us_4_idx,  1, "14 fields");
        bench_table_section_end();
    }

    /* ---- shard-stats summary ---- */
    tc_request(tc,
        "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"bench\"}",
        &resp);
    if (resp) {
        size_t len = strlen(resp);
        if (len > 600)
            printf("shard-stats: %.600s...\n", resp);
        else
            printf("shard-stats: %s\n", resp);
        free(resp); resp = NULL;
    }

    /* ---- disk usage ---- */
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/default/bench", env.db_root);
        char *argv[] = { (char *)"du", (char *)"-sh", path, NULL };
        printf("\nDisk usage: ");
        fflush(stdout);
        bench_safe_exec(argv);
    }

    printf("\n======================================\n");
    printf("  Invoice parallel bench complete\n");
    printf("======================================\n");

    close(single_json_fd);
    close(single_csv_fd);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("bench-parallel", bench_parallel_run)
