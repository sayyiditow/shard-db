/* Regression: a deferred bulk window whose records are ALL rejected after
 * their kf slots were reserved (bitmap distinct-value cap) aborts before
 * marker publication. Its abandoned reservations must still be
 * tombstoned: left at flag==0 they hard-stop the kf probe chain, and a
 * later window of the SAME request that was placed beyond the hole (the
 * request's reservation map makes it probe past the hole) becomes
 * unreachable once the request completes — an acknowledged insert that
 * GET/exists/count can no longer find.
 *
 * Deterministic construction (wire, one kf shard):
 *   - BULK_COMMIT_WINDOW=16 (fixture passthrough), splits=8, v:bitmap(4).
 *   - Seed 4 records (distinct values a0..a3) so the shard's bitmap
 *     dictionary is saturated; read the shard's slot capacity from the kf
 *     file size.
 *   - Craft, per rejected key R_i (home slot h_i), a later key L_i routed
 *     to the SAME shard with the SAME home slot, using compute_hash_raw.
 *   - ONE bulk-insert: records 0..15 = R_i with 16 fresh values (all
 *     rejected after reservation → window 1 has nactive==0), records 16..
 *     = L_i with an existing value (accepted, committed beyond the holes).
 *   - Assert every L_i is readable and counted; every R_i is absent.
 * RED on a tree without the nactive==0 tombstone pass (L_i absent),
 * GREEN with it. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include "shard_test_ctl.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#define OBJ "apmh"
#define NSPLITS 8
#define NR 16            /* one full window of rejected records */
#define MAXKEY 16

static size_t home_slot(const char *key, size_t cap, int *shard) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    if (shard) *shard = compute_record_shard(h, NSPLITS);
    uint64_t v;
    memcpy(&v, h, 8);
    return (size_t)(v % cap);
}

static int exists_key(TestClient *tc, const char *key) {
    char req[256];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"" OBJ
             "\",\"key\":\"%s\"}", key);
    tc_request(tc, req, &resp);
    int present = resp && SAFE_STRSTR(resp, "true") != NULL;
    free(resp);
    return present;
}

/* fault_mode 0: window 1 is rejected whole by the bitmap cap (all
 * records rejected after reservation, nactive == 0).
 * fault_mode 1: window 1's records are all ACCEPTED, but its marker
 * publish is failed by the test-build M-phase fault, so it aborts
 * pre-marker holding active reservations.
 * fault_mode 2: window 1 mixes eight accepted records with eight rejected
 * ones (a PUBLISHING window with abandoned reservations); the durable
 * abandoned-tombstone sync that must precede its marker is failed by the
 * test-build fault. The window must then stay UNPUBLISHED — its accepted
 * records must not appear (a marker published ahead of the tombstone
 * sync would let replay commit them behind flag==0 holes after a crash)
 * — and the later window must still be fully readable. */
static int run_scenario(int fault_mode) {
    /* BULK_COMMIT_WINDOW is read by the fixture while it writes db.env;
       restore the caller's value so nothing else in this process sees it. */
    char *saved_bcw = getenv("BULK_COMMIT_WINDOW");
    char *saved_copy = saved_bcw ? strdup(saved_bcw) : NULL;
    setenv("BULK_COMMIT_WINDOW", "16", 1);
    TestEnv env = {0};
    int rc0 = test_env_start(&env);
    if (saved_copy) { setenv("BULK_COMMIT_WINDOW", saved_copy, 1); free(saved_copy); }
    else unsetenv("BULK_COMMIT_WINDOW");
    if (rc0 != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" OBJ
        "\",\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:16\"],"
        "\"indexes\":[\"v:bitmap(4)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp); resp = NULL;

    /* Pick the target shard from the first generated key. */
    char seed_key[4][16], r_key[NR][16], l_key[NR][16];
    int target = -1;
    int ns = 0;
    for (int i = 0; ns < 4 && i < 100000; i++) {
        char k[16];
        snprintf(k, sizeof(k), "s%d", i);
        int sh;
        (void)home_slot(k, 64, &sh);
        if (target < 0) target = sh;
        if (sh == target) { memcpy(seed_key[ns++], k, sizeof(k)); }
    }
    ASSERT_TRUE(ns == 4, "found 4 seed keys on the target shard");

    /* Seed request: saturate the shard's bitmap dictionary (cap 4). */
    {
        char req[1024];
        int off = snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\""
            OBJ "\",\"records\":[");
        for (int i = 0; i < 4; i++)
            off += snprintf(req + off, sizeof(req) - (size_t)off,
                "%s{\"key\":\"%s\",\"value\":{\"v\":\"a%d\"}}",
                i ? "," : "", seed_key[i], i);
        snprintf(req + off, sizeof(req) - (size_t)off, "]}");
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "seed response");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"inserted\":4") != NULL,
                    "seed inserted 4");
        free(resp); resp = NULL;
    }

    /* Slot capacity of the target shard from shard-stats. */
    size_t cap = 0;
    tc_request(tc,
        "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\""
        OBJ "\"}", &resp);
    ASSERT_NOT_NULL(resp, "shard-stats response");
    if (resp) {
        char needle[32];
        snprintf(needle, sizeof(needle), "\"keyfile\":%d", target);
        const char *p = SAFE_STRSTR(resp, needle);
        const char *q = p ? SAFE_STRSTR(p, "\"slots\":") : NULL;
        if (q) cap = (size_t)atol(q + strlen("\"slots\":"));
    }
    free(resp); resp = NULL;
    char msg[160];
    snprintf(msg, sizeof(msg), "kf slot capacity plausible (cap=%zu)", cap);
    ASSERT_TRUE(cap >= 32 && (cap & (cap - 1)) == 0, msg);
    if (!(cap >= 32 && (cap & (cap - 1)) == 0)) {
        tc_close(tc); test_env_stop(&env); return 1;
    }

    /* Rejected keys: distinct home slots, none equal to a seed's home. */
    size_t seed_home[4];
    for (int i = 0; i < 4; i++) seed_home[i] = home_slot(seed_key[i], cap, NULL);
    size_t r_home[NR];
    int nr = 0;
    for (int i = 0; nr < NR && i < 1000000; i++) {
        char k[16];
        snprintf(k, sizeof(k), "r%d", i);
        int sh;
        size_t h = home_slot(k, cap, &sh);
        if (sh != target) continue;
        int clash = 0;
        for (int j = 0; j < 4; j++) if (seed_home[j] == h) clash = 1;
        for (int j = 0; j < nr; j++) if (r_home[j] == h) clash = 1;
        if (clash) continue;
        r_home[nr] = h;
        memcpy(r_key[nr++], k, sizeof(k));
    }
    ASSERT_TRUE(nr == NR, "found 16 rejected keys with distinct homes");
    int nl = 0;
    for (int i = 0; nl < NR && i < 20000000; i++) {
        char k[16];
        snprintf(k, sizeof(k), "l%d", i);
        int sh;
        size_t h = home_slot(k, cap, &sh);
        if (sh != target || h != r_home[nl]) continue;
        memcpy(l_key[nl++], k, sizeof(k));
    }
    ASSERT_TRUE(nl == NR, "found 16 later keys sharing a rejected home slot");
    if (nr != NR || nl != NR) {
        tc_close(tc); test_env_stop(&env); return 1;
    }

    /* Re-read the capacity right before the request: a pre-grow/resplit
       between the first read and now would move every home slot and let
       the test pass vacuously. */
    size_t cap_now = 0;
    tc_request(tc,
        "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\""
        OBJ "\"}", &resp);
    if (resp) {
        char needle[32];
        snprintf(needle, sizeof(needle), "\"keyfile\":%d", target);
        const char *p = SAFE_STRSTR(resp, needle);
        const char *q = p ? SAFE_STRSTR(p, "\"slots\":") : NULL;
        if (q) cap_now = (size_t)atol(q + strlen("\"slots\":"));
    }
    free(resp); resp = NULL;
    ASSERT_TRUE(cap_now == cap, "slot capacity unchanged since key crafting");

    if (fault_mode)   /* fail the first marker publication (or its pre-sync) */
        ASSERT_EQ_INT(test_env_ctl_set_fail_phase(&env,
                          fault_mode == 2 ? SHARD_TEST_PHASE_ABANDONED
                                          : SHARD_TEST_PHASE_M,
                          1, 0), 0, "arm fault");

    /* The request: window 1 = rejected / failed, window 2 = later keys. */
    {
        size_t cap_req = 8192;
        char *req = malloc(cap_req);
        ASSERT_NOT_NULL(req, "alloc request");
        if (!req) { tc_close(tc); test_env_stop(&env); return 1; }
        int off = snprintf(req, cap_req,
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\""
            OBJ "\",\"records\":[");
        for (int i = 0; i < NR; i++)
            off += snprintf(req + off, cap_req - (size_t)off,
                "%s{\"key\":\"%s\",\"value\":{\"v\":\"%s%d\"}}",
                i ? "," : "", r_key[i], (fault_mode == 1 || (fault_mode == 2 && i < NR / 2)) ? "a" : "n",
                (fault_mode == 1 || (fault_mode == 2 && i < NR / 2)) ? i % 4 : i);
        for (int i = 0; i < NR; i++)
            off += snprintf(req + off, cap_req - (size_t)off,
                ",{\"key\":\"%s\",\"value\":{\"v\":\"a0\"}}", l_key[i]);
        snprintf(req + off, cap_req - (size_t)off, "]}");
        tc_request(tc, req, &resp);
        free(req);
        ASSERT_NOT_NULL(resp, "main bulk response");
        if (fault_mode) {
            /* The failed marker publication fails the request at the shard
               level: the response reports errors, yet the later window's
               markers were published and converge — error-but-durable is
               legal, an UNREADABLE record is not. */
            snprintf(msg, sizeof(msg), "failed window reported (resp=%.120s)",
                     resp ? resp : "(null)");
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\":\"some_records_dropped\"") != NULL, msg);
        } else {
            snprintf(msg, sizeof(msg),
                     "later window accepted all %d (resp=%.120s)", NR,
                     resp ? resp : "(null)");
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"inserted\":16") != NULL,
                        msg);
        }
        free(resp); resp = NULL;
    }

    if (fault_mode)
        test_env_ctl_set_fail_phase(&env, -1, 0, 0);   /* disarm */

    int missing = 0;
    for (int i = 0; i < NR; i++) {
        if (!exists_key(tc, l_key[i])) missing++;
        snprintf(msg, sizeof(msg), "window-1 key %s absent", r_key[i]);
        ASSERT_TRUE(!exists_key(tc, r_key[i]), msg);
    }
    snprintf(msg, sizeof(msg),
             "every acknowledged later-window insert is readable "
             "(%d of %d unreachable)", missing, NR);
    ASSERT_TRUE(missing == 0, msg);

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"" OBJ "\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "count response");
    snprintf(msg, sizeof(msg), "count == 4 seeds + 16 later (got %.40s)",
             resp ? resp : "(null)");
    ASSERT_TRUE(resp && atoi(resp) == 20, msg);
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

static int test_abort_pre_marker_holes_run(void) { return run_scenario(0); }
static int test_unpublished_window_holes_run(void) { return run_scenario(1); }
static int test_publishing_window_abandoned_sync_run(void) { return run_scenario(2); }

TEST_REGISTER("test-abort-pre-marker-holes", test_abort_pre_marker_holes_run)
TEST_REGISTER("test-unpublished-window-holes", test_unpublished_window_holes_run)
TEST_REGISTER("test-publishing-window-abandoned-sync", test_publishing_window_abandoned_sync_run)
