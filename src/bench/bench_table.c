/* src/bench/bench_table.c — see bench_table.h for usage. */
#include "bench_table.h"
#include "bench_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LABEL    72
#define MAX_SNIPPET 128
#define MAX_ROWS    256
#define BAR_WIDTH    32

typedef struct {
    char  label[MAX_LABEL];
    long  us;                   /* microseconds — finer than ms for fast queries */
    char  snippet[MAX_SNIPPET];
    char  extra[48];            /* optional trailing column (e.g. "0.39 M/sec") */
    int   ok;
} BenchRow;

static BenchRow g_rows[MAX_ROWS];
static int      g_n = 0;
static char     g_section[128] = "";
static int      g_open = 0;

static void flush_section(void) {
    if (!g_open) return;
    g_open = 0;

    /* Header */
    printf("\n%s\n", g_section);
    /* dash line — same width as the longest possible row */
    static const char dashes[] =
        "------------------------------------------------------------------------"
        "------------------------------------------------------------------------";
    printf("%.*s\n", 116, dashes);

    if (g_n == 0) {
        printf("  (empty)\n");
        return;
    }

    /* Find max us for bar scaling. */
    long max_us = 0;
    for (int i = 0; i < g_n; i++) if (g_rows[i].us > max_us) max_us = g_rows[i].us;
    if (max_us < 1) max_us = 1;

    for (int i = 0; i < g_n; i++) {
        BenchRow *r = &g_rows[i];
        int w = (int)((double)r->us / (double)max_us * BAR_WIDTH);
        if (w < 0) w = 0;
        if (w > BAR_WIDTH) w = BAR_WIDTH;

        /* Format ms: <1ms shows as e.g. "  0.42ms"; >=1ms as "  12.3ms". */
        char ms_buf[16];
        if (r->us < 1000)
            snprintf(ms_buf, sizeof(ms_buf), "%5.2fms", (double)r->us / 1000.0);
        else if (r->us < 100 * 1000)
            snprintf(ms_buf, sizeof(ms_buf), "%5.1fms", (double)r->us / 1000.0);
        else
            snprintf(ms_buf, sizeof(ms_buf), "%5ldms", r->us / 1000);

        char bar[BAR_WIDTH + 1];
        for (int j = 0; j < w; j++) bar[j] = '#';
        bar[w] = '\0';

        if (r->ok) {
            if (r->extra[0]) {
                /* Trailing column for throughput-style rows
                   (bulk-insert "0.39 M/sec", batched-latency "31 k op/s"). */
                printf("  %-*s %s  %-*s %s\n",
                       MAX_LABEL - 4, r->label, ms_buf, BAR_WIDTH, bar, r->extra);
            } else {
                printf("  %-*s %s  %-*s\n",
                       MAX_LABEL - 4, r->label, ms_buf, BAR_WIDTH, bar);
            }
        } else {
            printf("  %-*s %s  ERR\n", MAX_LABEL - 4, r->label, ms_buf);
        }
    }

    /* Sort copies for percentile + min/max. Simple insertion sort, n <= 256.
       Zero-init silences gcc's maybe-uninitialized warning — it can't prove
       past the early-return for g_n==0 above. */
    long arr[MAX_ROWS] = {0};
    for (int i = 0; i < g_n; i++) arr[i] = g_rows[i].us;
    for (int i = 1; i < g_n; i++) {
        long v = arr[i]; int j = i - 1;
        while (j >= 0 && arr[j] > v) { arr[j+1] = arr[j]; j--; }
        arr[j+1] = v;
    }
    long mi = arr[0], ma = arr[g_n - 1];
    long p50 = arr[g_n / 2];
    long total = 0;
    for (int i = 0; i < g_n; i++) total += arr[i];

    printf("%.*s\n", 116, dashes);
    printf("  %d queries  min=%.2fms  p50=%.2fms  max=%.2fms  total=%.1fms\n",
           g_n,
           (double)mi    / 1000.0,
           (double)p50   / 1000.0,
           (double)ma    / 1000.0,
           (double)total / 1000.0);

    g_n = 0;
}

void bench_table_section_begin(const char *name) {
    flush_section();   /* idempotent if no section open */
    strncpy(g_section, name ? name : "", sizeof(g_section) - 1);
    g_section[sizeof(g_section) - 1] = '\0';
    g_n = 0;
    g_open = 1;
}

void bench_table_run(TestClient *tc, const char *label, const char *json) {
    if (g_n >= MAX_ROWS) return;
    BenchRow *r = &g_rows[g_n++];
    /* Zero every field — the slot is reused across sections (g_n resets,
       g_rows[] doesn't), and a stale `extra` from a prior throughput row
       would otherwise bleed into a query row's trailing column. */
    r->label[0] = '\0';
    r->snippet[0] = '\0';
    r->extra[0] = '\0';
    strncpy(r->label, label ? label : "", sizeof(r->label) - 1);
    r->label[sizeof(r->label) - 1] = '\0';

    char *resp = NULL;
    uint64_t t0 = bench_now_ns();
    int rc = tc_request(tc, json, &resp);
    uint64_t t1 = bench_now_ns();
    r->us = (long)((t1 - t0) / 1000);
    r->ok = (rc == 0);

    if (resp) {
        size_t len = strlen(resp);
        size_t copy = len > MAX_SNIPPET - 4 ? MAX_SNIPPET - 4 : len;
        memcpy(r->snippet, resp, copy);
        r->snippet[copy] = '\0';
        if (len > copy) {
            r->snippet[copy] = '.';
            r->snippet[copy+1] = '.';
            r->snippet[copy+2] = '.';
            r->snippet[copy+3] = '\0';
        }
        free(resp);
    } else {
        r->snippet[0] = '\0';
    }
}

void bench_table_record(const char *label, long us, int ok, const char *extra) {
    if (g_n >= MAX_ROWS) return;
    BenchRow *r = &g_rows[g_n++];
    strncpy(r->label, label ? label : "", sizeof(r->label) - 1);
    r->label[sizeof(r->label) - 1] = '\0';
    r->us = us;
    r->ok = ok;
    r->snippet[0] = '\0';
    if (extra && extra[0]) {
        strncpy(r->extra, extra, sizeof(r->extra) - 1);
        r->extra[sizeof(r->extra) - 1] = '\0';
    } else {
        r->extra[0] = '\0';
    }
}

void bench_table_section_end(void) {
    flush_section();
}
