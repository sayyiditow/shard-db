/* src/test/cases/test_parallel_index_integrity.c
 * Port of tests/test-parallel-index-integrity.sh — every record inserted
 * via parallel bulk-insert must be findable via single + composite indexes.
 * Guards against the strtok-race regression where index entries silently
 * dropped under concurrent ingestion.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"   /* SB_APPEND — safe StringBuilder vs CodeQL snprintf-overflow flag */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHUNKS  5
#define PER_CHUNK 20000
#define TOTAL (CHUNKS * PER_CHUNK)

static const char *STATUSES[] = { "PAID", "PENDING", "REFUNDED", "CANCELLED" };
static const char *REGIONS[]  = { "EU", "US", "APAC", "LATAM", "ME" };
static const char *TIERS[]    = { "GOLD", "SILVER", "BRONZE" };

typedef struct {
    int port;
    int chunk;
    int rc;
} Worker;

/* Build a JSON bulk-insert payload for chunk c covering keys
   k(c*PER_CHUNK) .. k(c*PER_CHUNK + PER_CHUNK - 1). */
static char *build_payload(int chunk) {
    /* Generous: each record ~110 bytes + envelope. */
    size_t cap = (size_t)PER_CHUNK * 120 + 256;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    size_t len = 0;
    SB_APPEND(buf, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"idxtest\","
        "\"records\":[");
    for (int i = chunk * PER_CHUNK; i < (chunk + 1) * PER_CHUNK; i++) {
        const char *s = STATUSES[i % 4];
        const char *r = REGIONS[i % 5];
        const char *t = TIERS[i % 3];
        SB_APPEND(buf, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"status\":\"%s\",\"region\":\"%s\","
            "\"tier\":\"%s\",\"amount\":%d}}",
            (i == chunk * PER_CHUNK) ? "" : ",", i, s, r, t, i);
        if (len + 256 > cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return NULL; }
            buf = nb;
        }
    }
    SB_APPEND(buf, len, cap, "]}");
    return buf;
}

static void *worker_main(void *arg) {
    Worker *w = arg;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->rc = -1; return NULL; }
    char *payload = build_payload(w->chunk);
    if (!payload) { tc_close(tc); w->rc = -1; return NULL; }
    char *resp = NULL;
    int r = tc_request(tc, payload, &resp);
    free(payload);
    free(resp);
    tc_close(tc);
    w->rc = r;
    return NULL;
}


static int find_count_keys(TestClient *tc, const char *crit) {
    /* Use count via separate query to avoid huge find responses. */
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"idxtest\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_parallel_index_integrity_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"idxtest\","
        "\"splits\":64,\"max_key\":32,"
        "\"fields\":[\"status:varchar:16\",\"region:varchar:16\","
                    "\"tier:varchar:8\",\"amount:int\"],"
        "\"indexes\":[\"status\",\"region\",\"status+region\","
                     "\"region+tier\",\"status+region+tier\"]}",
        &resp); free(resp); resp = NULL;

    /* Spawn CHUNKS worker threads, each inserting PER_CHUNK records. */
    pthread_t threads[CHUNKS];
    Worker workers[CHUNKS];
    for (int i = 0; i < CHUNKS; i++) {
        workers[i].port = env.port; workers[i].chunk = i; workers[i].rc = 0;
        if (pthread_create(&threads[i], NULL, worker_main, &workers[i]) != 0) {
            ASSERT_TRUE(0, "pthread_create");
            tc_close(tc); test_env_stop(&env); return 1;
        }
    }
    int worker_failures = 0;
    for (int i = 0; i < CHUNKS; i++) {
        pthread_join(threads[i], NULL);
        if (workers[i].rc != 0) worker_failures++;
    }
    ASSERT_EQ_INT(worker_failures, 0, "all bulk-insert workers succeeded");

    /* Total record count. */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"idxtest\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), TOTAL, "100000 records present");
    free(resp); resp = NULL;

    /* status: 4 values × 25000. */
    char crit[256];
    for (int i = 0; i < 4; i++) {
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"%s\"}]", STATUSES[i]);
        char desc[64]; snprintf(desc, sizeof(desc), "status=%s → 25000 via index", STATUSES[i]);
        ASSERT_EQ_INT(find_count_keys(tc, crit), 25000, desc);
    }
    /* region: 5 values × 20000. */
    for (int i = 0; i < 5; i++) {
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"%s\"}]", REGIONS[i]);
        char desc[64]; snprintf(desc, sizeof(desc), "region=%s → 20000 via index", REGIONS[i]);
        ASSERT_EQ_INT(find_count_keys(tc, crit), 20000, desc);
    }

    /* status+region: every (status, region) pair sums to TOTAL. */
    int total_sr = 0;
    for (int i = 0; i < 4; i++) for (int j = 0; j < 5; j++) {
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"%s\"}]",
            STATUSES[i], REGIONS[j]);
        total_sr += find_count_keys(tc, crit);
    }
    ASSERT_EQ_INT(total_sr, TOTAL, "status+region composite: all 100000 reachable");

    /* region+tier: every pair sums to TOTAL. */
    int total_rt = 0;
    for (int i = 0; i < 5; i++) for (int j = 0; j < 3; j++) {
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"%s\"}]",
            REGIONS[i], TIERS[j]);
        total_rt += find_count_keys(tc, crit);
    }
    ASSERT_EQ_INT(total_rt, TOTAL, "region+tier composite: all 100000 reachable");

    /* status+region+tier 3-way. */
    int total_3 = 0;
    for (int i = 0; i < 4; i++) for (int j = 0; j < 5; j++) for (int k = 0; k < 3; k++) {
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"%s\"}]",
            STATUSES[i], REGIONS[j], TIERS[k]);
        total_3 += find_count_keys(tc, crit);
    }
    ASSERT_EQ_INT(total_3, TOTAL, "status+region+tier 3-way: all 100000 reachable");

    /* Spot checks: 10 known keys must be findable via 3-way composite. */
    int spot_ids[] = { 0, 7777, 15000, 33333, 49999, 60000, 77777, 88888, 95000, 99999 };
    for (size_t s = 0; s < sizeof(spot_ids)/sizeof(spot_ids[0]); s++) {
        int id = spot_ids[s];
        const char *st = STATUSES[id % 4];
        const char *rg = REGIONS[id % 5];
        const char *tr = TIERS[id % 3];
        snprintf(crit, sizeof(crit),
            "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"%s\"},"
            "{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"%s\"}]",
            st, rg, tr);
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"idxtest\","
            "\"criteria\":%s,\"limit\":200000,\"fields\":[\"status\"]}", crit);
        tc_request(tc, req, &resp);
        char want[64]; snprintf(want, sizeof(want), "\"k%d\"", id);
        char desc[64]; snprintf(desc, sizeof(desc), "k%d findable via 3-way composite", id);
        ASSERT_TRUE(resp && strstr(resp, want) != NULL, desc);
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-parallel-index-integrity", test_parallel_index_integrity_run)
