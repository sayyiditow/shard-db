/* src/test/cases/test_numeric_between_probe.c
 * TEMPORARY diagnostic probe for
 * docs/plans/2026-08-28-macos-arm64-numeric-between.md — NOT a
 * regression case. Expected to fail on macOS arm64 until the
 * numeric-between defect is root-caused and fixed; must pass 100% on
 * Linux. Delete together with the plan close-out.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "btree.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PROBE_MULT 100LL

static const char *PROBE_VALS[5] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
static const char *PROBE_LO = "-1";
static const char *PROBE_HI = "1";

static void local_numeric_key(const char *dec, int64_t mult, uint8_t out[8]) {
    double dv = atof(dec);
    int64_t v = (int64_t)(dv * (double)mult + (dv >= 0 ? 0.5 : -0.5));
    uint64_t u = (uint64_t)v ^ (1ULL << 63);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)(u >> (56 - 8 * i));
}

static void probe_hex(const char *tag, const uint8_t *k) {
    TAP_DIAG("    %s %02X%02X%02X%02X%02X%02X%02X%02X\n", tag,
             k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7]);
}

static uint8_t seen[8][8];
static int n_seen, n_range, n_walk;

static int range_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)h; (void)ctx;
    if (n_seen < 8 && vl == 8) memcpy(seen[n_seen], v, 8);
    n_seen++; n_range++;
    return 0;
}

static int walk_cb(const char *v, size_t vl, void *ctx) {
    (void)ctx;
    if (n_seen < 8 && vl == 8) memcpy(seen[n_seen], v, 8);
    n_seen++; n_walk++;
    return 0;
}

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static void phase_a(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-probe-%d.idx", (int)getpid());
    unlink(path);

    static const struct { const char *dec; const uint8_t key[8]; } golden[7] = {
        { "-999.99", { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFE,0x79,0x61 } },
        { "-0.01",   { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF } },
        { "0",       { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } },
        { "0.01",    { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x01 } },
        { "999.99",  { 0x80,0x00,0x00,0x00,0x00,0x01,0x86,0x9F } },
        { "-1",      { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x9C } },
        { "1",       { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x64 } },
    };
    char desc[64];
    for (int i = 0; i < 7; i++) {
        uint8_t k[8];
        local_numeric_key(golden[i].dec, PROBE_MULT, k);
        TAP_DIAG("  A1 local %-8s ->", golden[i].dec); probe_hex("", k);
        snprintf(desc, sizeof(desc), "A1 local key %d matches golden", i);
        ASSERT_TRUE(memcmp(k, golden[i].key, 8) == 0, desc);
    }

    TypedField f; memset(&f, 0, sizeof(f));
    f.type = FT_NUMERIC; f.size = 8; f.numeric_scale = 2;
    f.numeric_scale_mult = PROBE_MULT;
    for (int i = 0; i < 7; i++) {
        uint8_t k[32]; size_t klen = 0;
        encode_field_for_index(&f, golden[i].dec, strlen(golden[i].dec), k, &klen);
        TAP_DIAG("  A2 config %-8s -> len=%zu", golden[i].dec, klen);
        probe_hex("", k);
        snprintf(desc, sizeof(desc), "A2 encode_field_for_index %d golden", i);
        ASSERT_TRUE(klen == 8 && memcmp(k, golden[i].key, 8) == 0, desc);
    }

    for (int i = 0; i < 5; i++) {
        uint8_t k[8]; local_numeric_key(PROBE_VALS[i], PROBE_MULT, k);
        uint8_t hash[BT_HASH_SIZE]; memset(hash, 0, sizeof(hash));
        hash[15] = (uint8_t)(i + 1);
        int rc = btree_insert(path, (const char *)k, 8, hash);
        snprintf(desc, sizeof(desc), "A3 btree_insert %s", PROBE_VALS[i]);
        ASSERT_EQ_INT(rc, 0, desc);
    }

    n_walk = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_walk_all_values(path, walk_cb, NULL);
    TAP_DIAG("  A4 walk-all stored %d keys:", n_walk);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_walk, 5, "A4 walk-all sees 5 stored keys");

    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);
    probe_hex("  A5 lo", lo); probe_hex("  A5 hi", hi);

    n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_range(path, (const char *)lo, 8, (const char *)hi, 8, range_cb, NULL);
    TAP_DIAG("  A5 btree_range returned %d:", n_range);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_range, 3, "A5 btree_range [-1..1] returns 3");

    n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_range_ex(path, (const char *)lo, 8, 0, (const char *)hi, 8, 0, range_cb, NULL);
    TAP_DIAG("  A5 btree_range_ex returned %d:", n_range);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_range, 3, "A5 btree_range_ex [-1..1] returns 3");

    BtRangeIter *it = btree_range_iter_open(path, (const char *)lo, 8, 0,
                                             (const char *)hi, 8, 0, 0);
    ASSERT_NOT_NULL(it, "A6 btree_range_iter_open");
    int n_iter = 0;
    if (it) {
        const char *v; size_t vl; const uint8_t *h;
        TAP_DIAG("  A6 iter sequence:");
        while (btree_range_iter_next(it, &v, &vl, &h) == 1) {
            if (n_iter < 8 && vl == 8) {
                char t[16];
                snprintf(t, sizeof(t), "#%d", n_iter);
                probe_hex(t, (const uint8_t *)v);
            }
            n_iter++;
        }
        btree_range_iter_close(it);
    }
    ASSERT_EQ_INT(n_iter, 3, "A6 iter [-1..1] yields 3");
    unlink(path);
}

static void phase_b(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "B env start"); return; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "B connect");
    if (!tc) { test_env_stop(&env); return; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],\"indexes\":[\"amt\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, PROBE_VALS[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    ASSERT_EQ_INT(do_count(tc, "bi_num", "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\",\"value2\":\"1\"}]"), 3,
                  "B1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num", "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"), 2,
                  "B1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num", "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"), 3,
                  "B1 wire gte 0 = 3 (control)");
    tc_close(tc);

    int nshards = index_splits_for(16);
    TAP_DIAG("  B2 index_splits_for(16) = %d", nshards);
    int expected[8] = {0};
    for (int i = 0; i < 5; i++) {
        char key[16];
        snprintf(key, sizeof(key), "n_%d", i);
        uint8_t h[16];
        compute_hash_raw(key, strlen(key), h);
        expected[idx_shard_for_hash(h, 16)]++;
    }
    char desc[64];
    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);
    int total_walked = 0, total_ranged = 0, n_problems = 0;
    for (int s = 0; s < nshards; s++) {
        char p[512];
        build_idx_path(p, sizeof(p), env.db_root, "default/bi_num", "amt", s);
        TAP_DIAG("  B2 shard %d (%s) expected=%d", s, p, expected[s]);
        int readable = (access(p, R_OK) == 0);
        struct stat st;
        int regular = (stat(p, &st) == 0 && S_ISREG(st.st_mode));
        if (!readable || !regular) {
            if (expected[s] == 0) {
                TAP_DIAG("  B2 shard %d has no file; 0 records routed —"
                         " legitimately absent, walkers skipped\n", s);
                continue;
            }
            snprintf(desc, sizeof(desc),
                     "B2 shard %d (routes %d records) file readable+regular",
                     s, expected[s]);
            ASSERT_TRUE(0, desc);
            TAP_DIAG("  B2 shard %d (%s) readable=%d regular=%d — walkers skipped\n", s, p, readable, regular);
            n_problems++;
            continue;
        }
        TAP_DIAG("  B2 shard %d (%s) size=%lld bytes", s, p, (long long)st.st_size);
        n_walk = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
        btree_walk_all_values(p, walk_cb, NULL);
        TAP_DIAG("  B2 shard %d (%s) stores %d:", s, p, n_walk);
        for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
        total_walked += n_walk;
        snprintf(desc, sizeof(desc), "B2 shard %d stores its routed records", s);
        ASSERT_EQ_INT(n_walk, expected[s], desc);
        n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
        btree_range(p, (const char *)lo, 8, (const char *)hi, 8, range_cb, NULL);
        TAP_DIAG("  B2 shard %d btree_range returned %d", s, n_range);
        total_ranged += n_range;
    }
    TAP_DIAG("  B2 totals: walked=%d ranged=%d problems=%d", total_walked, total_ranged, n_problems);
    ASSERT_EQ_INT(n_problems, 0, "B2 every routed shard file present, readable, regular");
    ASSERT_EQ_INT(total_walked, 5, "B2 walk-all totals 5 stored keys");
    ASSERT_EQ_INT(total_ranged, 3, "B2 per-shard ranges total 3");
    test_env_stop(&env);
}

static int test_numeric_between_probe_run(void) {
    phase_a();
    phase_b();
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe", test_numeric_between_probe_run)
