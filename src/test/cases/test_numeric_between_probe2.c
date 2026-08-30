/* src/test/cases/test_numeric_between_probe2.c
 * TEMPORARY round-2 diagnostic probe for the macOS numeric-BETWEEN
 * defect — docs/plans/2026-08-30-macos-numeric-between-upper-layer.md.
 * Round 1 exonerated encoding arithmetic, the config.c encoder, the
 * btree seek/iter paths, and the on-disk index files. This probe
 * discriminates the remaining suspects: parallel_for_io dispatch,
 * idx_count_cb TLS batching, a false deadline trip, and the
 * planner-produced criterion. Expected to FAIL on macOS arm64 until the
 * defect is fixed; must pass 100% on Linux. Delete with the close-out.
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
#include "query_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROBE_MULT 100LL   /* amt:numeric:10,2 → ×100 */
static const char *PROBE_LO = "-1";
static const char *PROBE_HI = "1";

static void local_numeric_key(const char *dec, int64_t mult, uint8_t out[8]) {
    double dv = atof(dec);
    int64_t v = (int64_t)(dv * (double)mult + (dv >= 0 ? 0.5 : -0.5));
    uint64_t u = (uint64_t)v ^ (1ULL << 63);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)(u >> (56 - 8 * i));
}

/* C2/C3 plain counting callback — thread-safe: the fan-out fires it
   from pool workers. */
static int n_plain;
static int plain_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    __atomic_add_fetch(&n_plain, 1, __ATOMIC_RELAXED);
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

static void phase_c(TestEnv *env) {
    /* Mirror the daemon's pools: parallel_for_io runs INLINE when
       g_io_running is 0 (parallel.c:364), which would silently
       serialise C2/C4/C5 and prove nothing about dispatch. Both init
       calls are no-ops when already initialized. */
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc < 4) nproc = 4;
    parallel_pool_init((int)nproc);
    parallel_io_pool_init((int)nproc * 4);
    TAP_DIAG("  C0 pools: cpu=%d", parallel_pool_size());

    TypedField f; memset(&f, 0, sizeof(f));
    f.type = FT_NUMERIC; f.size = 8; f.numeric_scale = 2;
    f.numeric_scale_mult = PROBE_MULT;
    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);

    /* C2 — the real fan-out (shard_walk_dispatch → parallel_for_io)
       with a plain callback: no TLS batching, no deadline. */
    n_plain = 0;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    plain_cb, NULL);
    TAP_DIAG("  C2 fan-out plain-cb count = %d", n_plain);
    ASSERT_EQ_INT(n_plain, 3, "C2 fan-out + plain callback returns 3");

    /* C3 — serial per-file baseline, same callback, no parallel_for.
       Lazily-created empty shards legitimately have no file. */
    int nshards = index_splits_for(16);
    int serial = 0;
    for (int s = 0; s < nshards; s++) {
        char p[512];
        build_idx_path(p, sizeof(p), env->db_root, "default/bi_num",
                       "amt", s);
        if (access(p, R_OK) != 0) continue;
        n_plain = 0;
        btree_range(p, (const char *)lo, 8, (const char *)hi, 8,
                    plain_cb, NULL);
        serial += n_plain;
    }
    TAP_DIAG("  C3 serial per-file count = %d", serial);
    ASSERT_EQ_INT(serial, 3, "C3 serial per-file baseline returns 3");

    /* C4a — fan-out through idx_count_cb with the deadline disabled:
       isolates the __thread batched-count path (query.c:804-807). */
    IdxCountCtx ic; memset(&ic, 0, sizeof(ic));
    ic.deadline = NULL;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    idx_count_cb, &ic);
    TAP_DIAG("  C4a fan-out idx_count_cb (no deadline) = %zu", ic.count);
    ASSERT_EQ_INT((long)ic.count, 3, "C4a idx_count_cb TLS batching = 3");

    /* C4b — same with a live 60s deadline: discriminates a false
       query_deadline_tick trip (types.h:534) from a TLS-batch loss. */
    QueryDeadline dl;
    dl.t0_ms = now_ms(); dl.timeout_ms = 60000;
    atomic_init(&dl.timed_out, 0);
    IdxCountCtx ic2; memset(&ic2, 0, sizeof(ic2));
    ic2.deadline = &dl; ic2.tf = &f;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    idx_count_cb, &ic2);
    TAP_DIAG("  C4b fan-out idx_count_cb (60s deadline) = %zu timed_out=%d",
             ic2.count, atomic_load_explicit(&dl.timed_out,
                                             memory_order_relaxed));
    ASSERT_EQ_INT((long)ic2.count, 3, "C4b idx_count_cb with deadline = 3");

    /* C5 — the full in-process dispatch: btree_dispatch with a
       hand-built OP_BETWEEN criterion, replicating what the planner
       emits for a single user-supplied BETWEEN (inclusive both ends,
       types.h:289-294). field_index_type() reads the object's
       index.conf from db_root — the daemon must still be alive. */
    SearchCriterion pc; memset(&pc, 0, sizeof(pc));
    snprintf(pc.field, sizeof(pc.field), "amt");
    pc.op = OP_BETWEEN;
    snprintf(pc.value, sizeof(pc.value), "-1");
    snprintf(pc.value2, sizeof(pc.value2), "1");
    IdxCountCtx ic3; memset(&ic3, 0, sizeof(ic3));
    ic3.deadline = NULL; ic3.tf = &f;
    btree_dispatch(env->db_root, "default/bi_num", "amt", 16, &pc, &f,
                   idx_count_cb, &ic3);
    TAP_DIAG("  C5 btree_dispatch OP_BETWEEN = %zu", ic3.count);
    ASSERT_EQ_INT((long)ic3.count, 3, "C5 btree_dispatch between = 3");
}

static int test_numeric_between_probe2_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "env start"); return 1; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],"
        "\"indexes\":[\"amt\"]}", &resp);
    free(resp); resp = NULL;
    const char *vals[5] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, vals[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* W1 — the failing wire shape plus controls. */
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\","
        "\"value2\":\"1\"}]"),
        3, "W1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
        2, "W1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"),
        3, "W1 wire gte 0 = 3 (control)");

    /* W2 — dump the compiled plan for the same criteria. Diagnostic
       only: no shape assert, the Linux/macOS diff is the evidence. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"criteria\":[{\"field\":\"amt\",\"op\":\"between\","
        "\"value\":\"-1\",\"value2\":\"1\"}],\"explain\":true}", &resp);
    TAP_DIAG("  W2 explain response: %s\n",
             resp ? resp : "(null)");
    ASSERT_NOT_NULL(resp, "W2 explain returned a response");
    free(resp); resp = NULL;

    tc_close(tc);
    phase_c(&env);          /* daemon still alive: field_index_type +
                               btree files are read live */
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe2", test_numeric_between_probe2_run)

