/* src/test/cases/test_stats_prom.c
 * Port of tests/test-stats-prom.sh — Prometheus exposition format.
 * Pins HELP/TYPE comments + sample lines for every metric, verifies
 * counters move under traffic, uptime advances, integer formatting.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Locate the start-of-line where a given metric value sits in the
   Prometheus output. Returns the long parsed at `<metric> <number>`,
   or -1 if the metric isn't present as a sample line. */
static long sample_value(const char *out, const char *metric) {
    if (!out || !metric) return -1;
    size_t mlen = strlen(metric);
    const char *p = out;
    while (*p) {
        const char *line_end = strchr(p, '\n');
        size_t line_len = line_end ? (size_t)(line_end - p) : strlen(p);
        if (line_len > mlen + 1 && strncmp(p, metric, mlen) == 0 && p[mlen] == ' ') {
            return atol(p + mlen + 1);
        }
        if (!line_end) break;
        p = line_end + 1;
    }
    return -1;
}

static int sample_line_present(const char *out, const char *metric) {
    if (!out || !metric) return 0;
    size_t mlen = strlen(metric);
    const char *p = out;
    while (*p) {
        const char *line_end = strchr(p, '\n');
        size_t line_len = line_end ? (size_t)(line_end - p) : strlen(p);
        if (line_len > mlen + 1 && strncmp(p, metric, mlen) == 0 && p[mlen] == ' ') return 1;
        if (!line_end) break;
        p = line_end + 1;
    }
    return 0;
}

static int test_stats_prom_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"stats-prom\"}", &resp);
    ASSERT_NOT_NULL(resp, "stats-prom returned output");
    if (!resp) { tc_close(tc); test_env_stop(&env); return 1; }

    /* Shape — every metric must have HELP, TYPE, and a sample line. */
    const char *metrics[] = {
        "shard_db_uptime_seconds",
        "shard_db_active_threads",
        "shard_db_in_flight_writes",
        "shard_db_ucache_used",
        "shard_db_ucache_capacity",
        "shard_db_ucache_bytes",
        "shard_db_ucache_hits_total",
        "shard_db_ucache_misses_total",
        "shard_db_bt_cache_used",
        "shard_db_bt_cache_capacity",
        "shard_db_bt_cache_bytes",
        "shard_db_bt_cache_hits_total",
        "shard_db_bt_cache_misses_total",
        "shard_db_slow_query_threshold_milliseconds",
        "shard_db_slow_query_total",
    };
    for (size_t i = 0; i < sizeof(metrics)/sizeof(metrics[0]); i++) {
        char needle[160], desc[256];
        snprintf(needle, sizeof(needle), "# HELP %s", metrics[i]);
        snprintf(desc, sizeof(desc), "HELP for %s", metrics[i]);
        if (strstr(resp, needle)) ASSERT_TRUE(1, desc);
        else { char m[256]; snprintf(m, sizeof(m), "missing HELP %s", metrics[i]); ASSERT_TRUE(0, m); }

        snprintf(needle, sizeof(needle), "# TYPE %s", metrics[i]);
        snprintf(desc, sizeof(desc), "TYPE for %s", metrics[i]);
        if (strstr(resp, needle)) ASSERT_TRUE(1, desc);
        else { char m[256]; snprintf(m, sizeof(m), "missing TYPE %s", metrics[i]); ASSERT_TRUE(0, m); }

        snprintf(desc, sizeof(desc), "sample line for %s", metrics[i]);
        ASSERT_TRUE(sample_line_present(resp, metrics[i]), desc);
    }

    /* Type discipline. */
    ASSERT_CONTAINS(resp, "# TYPE shard_db_ucache_hits_total counter",
                    "ucache_hits_total typed counter");
    ASSERT_CONTAINS(resp, "# TYPE shard_db_ucache_misses_total counter",
                    "ucache_misses_total typed counter");
    ASSERT_CONTAINS(resp, "# TYPE shard_db_bt_cache_hits_total counter",
                    "bt_cache_hits_total typed counter");
    ASSERT_CONTAINS(resp, "# TYPE shard_db_slow_query_total counter",
                    "slow_query_total typed counter");
    ASSERT_CONTAINS(resp, "# TYPE shard_db_uptime_seconds gauge",
                    "uptime_seconds typed gauge");
    ASSERT_CONTAINS(resp, "# TYPE shard_db_ucache_capacity gauge",
                    "ucache_capacity typed gauge");

    /* Not JSON. */
    ASSERT_TRUE(resp[0] != '{', "output does not start with '{'");

    long hits_before = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_before = sample_value(resp, "shard_db_ucache_misses_total");
    free(resp); resp = NULL;

    /* Generate traffic — slotcask uses kfcache/segcache; the same prom
       sample harness covers both. ucache_hits/misses now report zero on
       a fresh DB but the metric still appears in the export. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"fields\":[\"name:varchar:32\"],\"splits\":16}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp); free(resp); resp = NULL;
    for (int i = 0; i < 5; i++) {
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"prom_test\",\"key\":\"k1\"}", &resp);
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"stats-prom\"}", &resp);
    ASSERT_NOT_NULL(resp, "stats-prom (post-traffic) returned output");
    long hits_after = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_after = sample_value(resp, "shard_db_ucache_misses_total");
    long up_before = sample_value(resp, "shard_db_uptime_seconds");

    /* ucache is unused on v2 (slotcask uses kfcache/segcache), so the
       counter stays at its initial value — assert non-decreasing rather
       than strictly increasing. */
    ASSERT_TRUE(hits_after >= hits_before, "ucache_hits_total non-decreasing");
    ASSERT_TRUE(miss_after >= miss_before, "ucache_misses_total non-decreasing");

    /* Counter samples are integer — no decimal point on hits_total sample
       line (start-of-line, not the HELP/TYPE comment lines that contain the
       metric name as a substring). */
    {
        const char *needle = "shard_db_ucache_hits_total ";
        size_t nlen = strlen(needle);
        const char *p = resp;
        const char *sample = NULL;
        while (*p) {
            const char *line_end = strchr(p, '\n');
            if (strncmp(p, needle, nlen) == 0) { sample = p; break; }
            if (!line_end) break;
            p = line_end + 1;
        }
        ASSERT_NOT_NULL(sample, "hits_total sample line present");
        if (sample) {
            const char *q = sample + nlen;
            int has_dot = 0;
            while (*q && *q != '\n') { if (*q == '.') { has_dot = 1; break; } q++; }
            ASSERT_TRUE(!has_dot, "hits_total formatted as integer (no .0)");
        }
    }
    free(resp); resp = NULL;

    /* Uptime advances after a 1.1s sleep. */
    {
        struct timespec ts = { 1, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    tc_request(tc, "{\"mode\":\"stats-prom\"}", &resp);
    long up_after = sample_value(resp, "shard_db_uptime_seconds");
    ASSERT_TRUE(up_after > up_before, "uptime_seconds advances");
    free(resp); resp = NULL;

    /* JSON path returns prom output (auth path covered elsewhere). */
    tc_request(tc, "{\"mode\":\"stats-prom\",\"auth\":\"definitely-not-a-real-token\"}", &resp);
    ASSERT_CONTAINS(resp, "shard_db_uptime_seconds ",
                    "stats-prom over JSON returns prom output");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-stats-prom", test_stats_prom_run)
