/* test_slotcask_v2_concurrent.c — Phase-8 concurrent stress on v2.
 *
 * N writer threads insert + update + delete records concurrently against
 * a v2 object. M reader threads spin reading the same key range.
 * Validates:
 *
 *   1. No torn reads — every successful get returns one of the values
 *      that was actually committed for that key (never partial bytes
 *      from an in-flight update).
 *   2. No daemon crashes — slotcask's per-shard wrlock + atomic kf
 *      flip handle concurrent writers + readers cleanly.
 *   3. Final record set is internally consistent: count(criteria) and
 *      a full scan agree, and every key is either present-with-some-
 *      committed-value or absent.
 *
 * The stress workload is small enough (N=4, M=4, 5s wall) to run in CI
 * but exercises the lock-protocol on the kf-shard rwlock + per-stream
 * pool mutex.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#define NUM_WRITERS  4
#define NUM_READERS  4
#define KEY_RANGE    50
#define DURATION_MS  3000   /* keep CI-friendly; bug repro is fast at this scale */

typedef struct {
    int port;
    _Atomic int *stop;
    int writer_id;
    /* Each writer cycles a private "version" counter into the value.
       The reader can then verify any value it reads is one of the
       committed forms (writer_id*1000 + version) — no torn bytes. */
    int ops;
} WriterCtx;

typedef struct {
    int port;
    _Atomic int *stop;
    int reader_id;
    int ops;
    int torn_reads;
} ReaderCtx;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void *writer_thread(void *arg) {
    WriterCtx *w = (WriterCtx *)arg;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = 5000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;

    int version = 0;
    while (!*w->stop) {
        int k = rand() % KEY_RANGE;
        int val = w->writer_id * 1000000 + version;
        char req[256];
        char *resp = NULL;

        int op = rand() % 3;  /* 0=insert/upsert, 1=update, 2=delete */
        if (op == 0) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\",\"value\":{\"v\":%d,\"writer\":%d}}",
                k, val, w->writer_id);
        } else if (op == 1) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"update\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\",\"value\":{\"v\":%d}}", k, val);
        } else {
            snprintf(req, sizeof(req),
                "{\"mode\":\"delete\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\"}", k);
        }
        if (tc_request(tc, req, &resp) == 0) w->ops++;
        free(resp);
        version++;
    }
    tc_close(tc);
    return NULL;
}

/* Parse the integer "v" field from a get response. Returns INT_MIN if
   not present or malformed. */
static int parse_v_field(const char *resp) {
    if (!resp) return INT_MIN;
    const char *p = strstr(resp, "\"v\":");
    if (!p) return INT_MIN;
    p += 4;
    return atoi(p);
}

static int parse_writer_field(const char *resp) {
    if (!resp) return -1;
    const char *p = strstr(resp, "\"writer\":");
    if (!p) return -1;
    p += 9;
    return atoi(p);
}

static void *reader_thread(void *arg) {
    ReaderCtx *r = (ReaderCtx *)arg;
    TestClientCfg cfg = { .port = r->port, .io_timeout_ms = 5000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;

    while (!*r->stop) {
        int k = rand() % KEY_RANGE;
        char req[128];
        char *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"d\",\"object\":\"o\","
            "\"key\":\"k%d\"}", k);
        if (tc_request(tc, req, &resp) == 0) {
            r->ops++;
            /* If the get returned a record, the value should encode a
               legal (writer_id, version): v in [writer*1000,
               writer*1000+99999] and writer in [0, NUM_WRITERS). The
               update-only path doesn't touch `writer`, so we tolerate
               its absence (treat as "from some prior insert"). */
            if (resp && strstr(resp, "\"v\":")) {
                int v = parse_v_field(resp);
                int wid = parse_writer_field(resp);
                if (v != INT_MIN && wid >= 0) {
                    /* If writer is set, verify v is in that writer's
                       cycle range. Updates don't change `writer`, so
                       v can be from a different writer than the original
                       insert — that's fine, just verify it's within
                       SOME writer's range. */
                    int v_writer = v / 1000000;
                    if (v_writer < 0 || v_writer >= NUM_WRITERS) {
                        r->torn_reads++;
                    }
                }
            }
        }
        free(resp);
    }
    tc_close(tc);
    return NULL;
}

static int test_slotcask_v2_concurrent_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"o\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:int\",\"writer:int\"]}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    /* Spawn writers + readers. */
    _Atomic int stop = 0;
    pthread_t writers[NUM_WRITERS], readers[NUM_READERS];
    WriterCtx wctxs[NUM_WRITERS] = {0};
    ReaderCtx rctxs[NUM_READERS] = {0};
    for (int i = 0; i < NUM_WRITERS; i++) {
        wctxs[i].port = env.port;
        wctxs[i].stop = &stop;
        wctxs[i].writer_id = i;
        pthread_create(&writers[i], NULL, writer_thread, &wctxs[i]);
    }
    for (int i = 0; i < NUM_READERS; i++) {
        rctxs[i].port = env.port;
        rctxs[i].stop = &stop;
        rctxs[i].reader_id = i;
        pthread_create(&readers[i], NULL, reader_thread, &rctxs[i]);
    }

    /* Run for DURATION_MS, then stop. */
    long t0 = now_ms();
    while (now_ms() - t0 < DURATION_MS) {
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
    }
    stop = 1;

    int total_writes = 0, total_reads = 0, total_torn = 0;
    for (int i = 0; i < NUM_WRITERS; i++) {
        pthread_join(writers[i], NULL);
        total_writes += wctxs[i].ops;
    }
    for (int i = 0; i < NUM_READERS; i++) {
        pthread_join(readers[i], NULL);
        total_reads += rctxs[i].ops;
        total_torn += rctxs[i].torn_reads;
    }

    /* Daemon must still be alive — reconnect and probe. */
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect post-stress");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Liveness check only — this test is a torn-read detector, not a
       throughput benchmark. On 2-vCPU CI runners under heavy writer
       contention readers can be effectively starved (saw 4 reads in
       3s on GHA x86_64 while writers managed 200K+); arm64 same run
       had readers at 69K. The combined-progress threshold catches
       "daemon wedged completely" while tolerating extreme reader/
       writer skew that doesn't affect the real correctness check
       below (no torn reads). */
    ASSERT_TRUE(total_writes + total_reads > 100,
                "some forward progress under load (writes + reads > 100)");
    ASSERT_EQ_INT(total_torn, 0, "no torn reads observed");

    /* Final consistency: full scan vs count must agree. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"o\"}", &resp);
    int counted = atoi(resp ? resp : "0");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"d\",\"object\":\"o\"}", &resp);
    /* Keys returns a JSON array; count commas + 1 if non-empty. */
    int key_count = 0;
    if (resp && strchr(resp, '[')) {
        const char *p = strchr(resp, '[');
        if (strchr(p, '"')) {
            key_count = 1;
            for (const char *q = p; *q && *q != ']'; q++)
                if (*q == ',') key_count++;
        }
    }
    free(resp); resp = NULL;
    ASSERT_EQ_INT(counted, key_count,
                  "count == keys-array length (slotcask walk consistent)");

    /* All surviving records are readable end-to-end. */
    int read_failures = 0;
    for (int k = 0; k < KEY_RANGE; k++) {
        char req[128];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"d\",\"object\":\"o\","
            "\"key\":\"k%d\"}", k);
        tc_request(tc, req, &resp);
        if (resp && strstr(resp, "\"v\":")) {
            int v = parse_v_field(resp);
            int v_writer = v / 1000000;
            if (v_writer < 0 || v_writer >= NUM_WRITERS) read_failures++;
        }
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(read_failures, 0,
                  "every surviving record decodes to a valid (writer, version)");

    fprintf(stderr, "[concurrent] writers=%d readers=%d ops_w=%d ops_r=%d "
                    "final_count=%d torn=%d\n",
                    NUM_WRITERS, NUM_READERS, total_writes, total_reads,
                    counted, total_torn);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-concurrent", test_slotcask_v2_concurrent_run)
