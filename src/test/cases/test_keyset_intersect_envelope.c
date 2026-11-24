/* Regression (round-4 review M2): keyset_find_from_intersect's NULL
 * KeySet handling broke the response envelope. The response envelope
 * ({"rows":[ / [) is opened BEFORE the intersect dispatch, and the
 * round-2 fail-closed change returned -1 for every build failure —
 * including timeout and hard errors — leaving an unterminated document
 * on the wire (malformed JSON, no error object).
 *
 * Fix: classify the build failure via the checked builder:
 *   deadline        -> deadline-style partial: return 0, the tail closes
 *                      the envelope with "total":null;
 *   capacity (tier) -> falls back to a streaming scan, exactly like
 *                      count's whole-record recovery (the AND-intersect
 *                      build runs with budget=NULL, so only a KeySet
 *                      outgrowing its tier cap - millions of rows -
 *                      reaches this; not reproducible at test scale, the
 *                      branch is covered by review);
 *   hard (I/O...)   -> the same envelope close, then a storage-error
 *                      object (fail closed, still parseable).
 *
 * Scenarios (both assert a CLOSED, parseable response):
 *   1. timeout - a wide two-leaf intersect find with "timeout_ms":1:
 *      the deadline expires in the candidate build or in the emit phase
 *      (which one fires is timing-dependent; both must leave a closed,
 *      parseable envelope with "total":null and no error object).
 *   2. hard - arm the per-leaf KeySet-build alloc-fault countdown (a
 *      deterministic ENOMEM on the seed build; the bt range-open seam
 *      only covers btree_range_iter_open — cursor/aggregate walks —
 *      not the find-side shard walks): closed envelope + the storage-
 *      error object (RED on base: silent-empty in the pre-round-2
 *      tree, malformed JSON in the round-2 tree).
 * The wide finds are issued WITHOUT limit (limit>0 flips planning to
 * the estimate-skip path, which single-seeds instead of intersecting).
 * Baselines: a limit-bounded wide find (data health) and an unbounded
 * one (the intersect dispatch healthy end to end).
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

#define NROWS 50000
#define CHUNK 10000
#define CRIT_AB \
    "[{\"field\":\"a\",\"op\":\"between\",\"value\":0,\"value2\":100000}," \
    "{\"field\":\"b\",\"op\":\"between\",\"value\":0,\"value2\":100000}]"

static void insert_rows(TestClient *tc, int n) {
    const size_t reclen = 72;
    char *body = malloc((size_t)CHUNK * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    for (int base = 0; base < n; base += CHUNK) {
        int end = base + CHUNK < n ? base + CHUNK : n;
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        for (int i = base; i < end; i++) {
            off += (size_t)snprintf(body + off, reclen,
                "%s{\"key\":\"k%06d\",\"value\":{\"a\":%d,\"b\":%d}}",
                i > base ? "," : "", i, i, i);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        char *req = malloc(off + 160);
        ASSERT_NOT_NULL(req, "alloc request");
        snprintf(req, off + 160,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"keb_ix\",\"records\":%s}", body);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "bulk insert accepted");
        free(resp);
        free(req);
    }
    free(body);
}

static int test_keyset_intersect_envelope_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"keb_ix\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\"],\"indexes\":[\"a\",\"b\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp); resp = NULL;

    insert_rows(tc, NROWS);

    char req[1024];

    /* Baseline 1: limit-bounded wide find (data health; the estimate
     * skip path single-seeds this shape - rows + real total). */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"keb_ix\","
             "\"criteria\":%s,\"limit\":2000,\"total\":true}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "baseline find");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "baseline: bounded wide find acked");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"total\":") != NULL &&
                SAFE_STRSTR(resp, "\"total\":null") == NULL,
                "baseline: closed envelope with a real total");
    free(resp); resp = NULL;

    /* Baseline 2: the SAME criteria WITHOUT limit - real selectivity
     * walks run, both leaves are broad, and the plan is an intersect
     * (the dispatch under test). Healthy end to end. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"keb_ix\","
             "\"criteria\":%s,\"total\":true}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "baseline intersect find");
    TAP_DIAG("# baseline2 diag: bytes=%zu err=%d lastkey=%d\n",
             resp ? strlen(resp) : 0,
             resp && SAFE_STRSTR(resp, "\"error\"") != NULL,
             resp && SAFE_STRSTR(resp, "k049999") != NULL);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "baseline: unbounded intersect find acked");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "k049999") != NULL,
                "baseline: all rows present (first and last key)");
    free(resp); resp = NULL;

    /* 1: timeout - a 10 ms per-request deadline expires before/during
     * the candidate build. RED on the -1-only behavior: unterminated
     * envelope, no total:null. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"keb_ix\","
             "\"criteria\":%s,\"timeout_ms\":10,\"total\":true}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "timeout find");
    TAP_DIAG("# timeout base-state diag: bytes=%zu totalnull=%d err=%d\n",
             resp ? strlen(resp) : 0,
             resp && SAFE_STRSTR(resp, "\"total\":null") != NULL,
             resp && SAFE_STRSTR(resp, "\"error\"") != NULL);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"total\":null") != NULL,
                "timeout: closed envelope with total:null");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "timeout: deadline-style partial, not an error");
    free(resp); resp = NULL;

    /* 2: hard build failure - the per-leaf KeySet-build alloc fault
     * fails the seed build with ENOMEM. RED on base: round-2 left an
     * unterminated envelope (and the pre-round-2 tree returned a
     * silent-empty result). */
    ASSERT_EQ_INT(test_env_ctl_set_keyset_leaf_fail_alloc(&env, 1), 0,
                  "arm the seed-build alloc failure");
    /* total=true is REQUIRED to reach the intersect executor: without it
     * plan_filter gets the limit (skip-estimate path) and single-seeds. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"keb_ix\","
             "\"criteria\":%s,\"total\":true}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "hard-failure find");
    TAP_DIAG("# hard base-state diag: bytes=%zu err=%d close=%d\n",
             resp ? strlen(resp) : 0,
             resp && SAFE_STRSTR(resp, "index storage error") != NULL,
             resp && SAFE_STRSTR(resp, "]\n{") != NULL);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "index storage error") != NULL,
                "hard: fail-closed error object rendered");
    /* total=true: the closed envelope ends ],"total":null} before the
     * error object. */
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "}\n{\"error\"") != NULL,
                "hard: envelope closed before the error object");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(test_env_ctl_set_keyset_leaf_fail_alloc(&env, 0), 0,
                  "disarm");
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"keb_ix\","
             "\"criteria\":%s,\"limit\":2000,\"total\":true}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "restored: wide intersect find acked again");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-keyset-intersect-envelope", test_keyset_intersect_envelope_run)
