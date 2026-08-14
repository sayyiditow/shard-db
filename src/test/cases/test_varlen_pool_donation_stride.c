/* test_varlen_pool_donation_stride.c
 *
 * Deterministic (single connection, no threads, fixed seed) regression
 * test for a free-pool slot-capacity/header mismatch: when a donated
 * free-pool slot is larger than the record being written into it, the
 * record's true on-disk footprint used to be padded out to the donor's
 * full capacity while the record's own header only self-describes its
 * (smaller) logical size. Sequential stride-based walkers (the O_DIRECT
 * scanner backing `keys`/`fetch`) trust the header to compute the next
 * record's offset, so they land inside the prior record's own zero-
 * padding tail and misread it as a gap — triggering the scanner's
 * (expensive) resync path on nearly every record.
 *
 * This reproduces with a purely sequential, fixed-seed op sequence — no
 * concurrency required — mirroring test_slotcask_v2_concurrent.c's op
 * mix (insert full record / update partial field / delete, KEY_RANGE=50)
 * so that different-sized values keep getting donated into differently
 * shaped free-pool slots.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define KEY_RANGE 50
#define NUM_OPS   8000
#define NUM_WRITERS 4

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static int test_varlen_pool_donation_stride_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"o\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:int\",\"writer:int\"]}", &resp);
    free(resp); resp = NULL;

    /* Fixed-seed deterministic sequential workload — same op mix as
       test_slotcask_v2_concurrent.c's writer_thread, driven from a
       single connection with no threads at all. */
    unsigned seed = 12345u;
    int version = 0;
    for (int i = 0; i < NUM_OPS; i++) {
        int writer_id = i % NUM_WRITERS;
        int k = rand_r(&seed) % KEY_RANGE;
        int op = rand_r(&seed) % 3; /* 0=insert full, 1=update partial, 2=delete */
        int val = writer_id * 1000000 + version;
        char req[256];
        if (op == 0) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\",\"value\":{\"v\":%d,\"writer\":%d}}",
                k, val, writer_id);
        } else if (op == 1) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"update\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\",\"value\":{\"v\":%d}}", k, val);
        } else {
            snprintf(req, sizeof(req),
                "{\"mode\":\"delete\",\"dir\":\"d\",\"object\":\"o\","
                "\"key\":\"k%d\"}", k);
        }
        free(resp); resp = NULL;
        tc_request(tc, req, &resp);
        version++;
    }
    free(resp); resp = NULL;

    /* Oracle: count is always correct (kf-header-based, never walks
       segments). */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"o\"}", &resp);
    int counted = atoi(resp ? resp : "0");
    free(resp); resp = NULL;

    long t0 = now_ms();
    tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"d\",\"object\":\"o\"}", &resp);
    long dt = now_ms() - t0;

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
                  "count == keys-array length (no records silently dropped "
                  "by a misaligned sequential scan)");
    ASSERT_TRUE(dt < 2000,
                "keys scan completes within 2s (no per-record resync storm "
                "from donated-slot padding exceeding the record's own "
                "header-computed size)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-pool-donation-stride", test_varlen_pool_donation_stride_run)
