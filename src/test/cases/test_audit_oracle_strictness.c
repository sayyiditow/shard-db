/* Oracle self-test for the audit harnesses' write model (audit_common.h):
 * the strict never-adopt pending resolution must FLAG the failure shapes
 * the old adopt-observed resolver silently passed:
 *
 *   A. Lost acked write behind a stale pending — a delete errors (goes
 *      pending), a LATER insert of the same key acks (superseding the
 *      pending), then the acked write is lost. The old resolver saw the
 *      stale delete pending + absent key, adopted "deleted", and the
 *      verify passed. Supersede-at-ack + strict resolve must report it.
 *   B. Third value on a pending key — an insert/update errors (pending
 *      W), the server actually holds a THIRD value. The old resolver
 *      adopted it into the model; strict resolve must report a violation
 *      and leave the model untouched.
 *   C. Rollback (legal absence) — a pending insert on a never-written
 *      key, observed absent: legal NOT_APPLIED, no violation.
 *   D. Landed delete (legal fold) — a pending delete that did land folds
 *      the key dead; no violation; count stays exact.
 *   E. Crash-restart single-pending resolve has the same strictness
 *      (third value flagged; legal absence clean; pend always cleared).
 *
 * Each scenario drives the REAL wire against a daemon, mutating server
 * state behind the model's back to synthesize the failure; the final
 * section restores consistency and proves the healthy path still
 * verifies clean (no false positives).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "audit/audit_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define AOS_OBJ "aos"

typedef struct {
    TestClient *tc;
    AudModel *m;
    FILE *logf;
} AosCtx;

static void aos_wire(TestClient *tc, const char *req) {
    char *resp = NULL;
    int rc = tc_request(tc, req, &resp);
    ASSERT_TRUE(rc == 0 && resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "wire request acked");
    free(resp);
}

static void aos_ack_insert(AosCtx *c, int idx, const char *val) {
    char req[256];
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\""
             AOS_OBJ "\",\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
             c->m->keys[idx], val);
    aos_wire(c->tc, req);
    aud_model_ack_insert(c->m, idx, val);
    aud_model_supersede(c->m, 0, idx);
}

/* Mutate the server behind the model's back (insert-as-upsert for an
   existing key; the model is deliberately NOT told). */
static void aos_behind_update(TestClient *tc, const char *key,
                              const char *val) {
    char req[256];
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\""
             AOS_OBJ "\",\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
             key, val);
    aos_wire(tc, req);
}

static void aos_behind_delete(TestClient *tc, const char *key) {
    char req[256];
    snprintf(req, sizeof(req),
             "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\""
             AOS_OBJ "\",\"key\":\"%s\"}", key);
    aos_wire(tc, req);
}

static int test_audit_oracle_strictness_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char logpath[512];
    snprintf(logpath, sizeof(logpath), "%s/../oracle-selftest.log", env.db_root);
    FILE *logf = fopen(logpath, "w");
    ASSERT_NOT_NULL(logf, "open self-test log");
    if (!logf) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\""
        AOS_OBJ "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp); resp = NULL;

    AudModel m;
    aud_model_init(&m);
    aud_model_add(&m, "a1");
    aud_model_add(&m, "a2");
    aud_model_add(&m, "a3");
    aud_model_add(&m, "a4");
    AosCtx c = { .tc = tc, .m = &m, .logf = logf };

    /* ===== Scenario A: lost acked write behind a stale delete pending.
     * RED on the old oracle (resolve adopted "deleted", verify passed);
     * strict resolve + supersede must flag the lost write. */
    aos_ack_insert(&c, 0, "A");
    ASSERT_EQ_INT(aud_model_add_pending(&m, 0, 0, NULL), 0,
                  "A: erroring delete goes pending");
    aos_ack_insert(&c, 0, "B"); /* later ack on the same key supersedes */
    ASSERT_EQ_INT(m.npend[0], 0, "A: acked insert dropped the stale pending");
    ASSERT_EQ_INT((int)m.live[0], 1, "A: model still live");
    aos_behind_delete(tc, "a1"); /* acked write LOST on the server */
    {
        int rv = aud_model_resolve_all_pending(tc, &m, AOS_OBJ, 0, logf, "A");
        ASSERT_EQ_INT(rv, 0, "A: no pendings left to resolve");
        ASSERT_EQ_INT((int)m.live[0], 1,
                       "A: model was never adopted to deleted");
        int v = aud_verify_model(tc, &m, AOS_OBJ, 0, 0, logf);
        TAP_DIAG("# scenario A: resolve=0 verify violations=%d\n", v);
        ASSERT_TRUE(v >= 1, "A: lost acked write is FLAGGED");
        fflush(logf);
    }
    aos_behind_update(tc, "a1", "B"); /* restore the server for later */

    /* ===== Scenario B: third value on a pending key — never adopt. */
    aos_ack_insert(&c, 1, "C");
    ASSERT_EQ_INT(aud_model_add_pending(&m, 0, 1, "W"), 0,
                  "B: erroring update goes pending");
    aos_behind_update(tc, "a2", "THIRD");
    {
        int rv = aud_model_resolve_all_pending(tc, &m, AOS_OBJ, 0, logf, "B");
        TAP_DIAG("# scenario B: resolve violations=%d\n", rv);
        ASSERT_TRUE(rv >= 1, "B: third value is FLAGGED by strict resolve");
        ASSERT_EQ_STR(m.vals[1], "C",
                      "B: model kept the acked value (not adopted)");
        ASSERT_EQ_INT((int)m.live[1], 1, "B: model still live");
        aos_behind_update(tc, "a2", "C"); /* restore */
    }

    /* ===== Scenario E: the crash-restart single-pending resolve is
     * equally strict (third value flagged, pend cleared). */
    aud_model_set_pending(&m, 0, 1, "F"); /* the SINGLE pend slot */
    ASSERT_EQ_INT(m.pend[0].key_idx, 1, "E: single-slot pending set");
    aos_behind_update(tc, "a2", "THIRD2");
    {
        int rv = aud_model_resolve_pending(tc, &m, AOS_OBJ, 0, logf, "E");
        TAP_DIAG("# scenario E: resolve violations=%d\n", rv);
        ASSERT_TRUE(rv >= 1, "E: single-pend third value is FLAGGED");
        ASSERT_EQ_STR(m.vals[1], "C", "E: model not adopted");
        ASSERT_TRUE(m.pend[0].key_idx < 0, "E: pend cleared after resolve");
        aos_behind_update(tc, "a2", "C"); /* restore */
        /* E2: legal absence on the single pend (pending insert on a
           never-written key, observed absent) is clean. */
        aud_model_set_pending(&m, 0, 2, "G");
        ASSERT_EQ_INT(m.pend[0].key_idx, 2, "E2: pend set on dead key");
        rv = aud_model_resolve_pending(tc, &m, AOS_OBJ, 0, logf, "E2");
        ASSERT_EQ_INT(rv, 0, "E2: legal absence resolves clean");
        ASSERT_EQ_INT((int)m.live[2], 0, "E2: dead key stays dead");
    }

    /* ===== Scenario C: rollback (legal absence) through the multi
     * resolver — pending insert on a never-written key. */
    ASSERT_EQ_INT(aud_model_add_pending(&m, 0, 2, "R"), 0,
                  "C: pending insert on dead key");
    {
        int rv = aud_model_resolve_all_pending(tc, &m, AOS_OBJ, 0, logf, "C");
        ASSERT_EQ_INT(rv, 0, "C: rollback absence is legal");
        ASSERT_EQ_INT((int)m.live[2], 0, "C: model stays dead");
    }

    /* ===== Scenario D: a landed pending delete folds dead cleanly. */
    aos_ack_insert(&c, 3, "D");
    ASSERT_EQ_INT(aud_model_add_pending(&m, 0, 3, NULL), 0,
                  "D: erroring delete goes pending");
    aos_behind_delete(tc, "a4"); /* the delete actually landed */
    {
        int live_before = m.live_count;
        int rv = aud_model_resolve_all_pending(tc, &m, AOS_OBJ, 0, logf, "D");
        ASSERT_EQ_INT(rv, 0, "D: landed delete resolves clean");
        ASSERT_EQ_INT((int)m.live[3], 0, "D: key folded dead");
        ASSERT_EQ_INT(m.live_count, live_before - 1, "D: live_count folded");
    }

    /* ===== Scenario F: rollback of an acked write behind a later failed
     * op. Values are unique per op (the harnesses stamp an op counter), so
     * an acked update (u1_5_3), an acked overwrite (bi1_5_4) and a failed
     * update (u1_5_7) are three distinct values; the server showing the
     * OLDER acked u1_5_3 is neither the last acked value nor the pending
     * one — a rolled-back acked write, which must be FLAGGED. (With
     * text-colliding values — u1_5 acked, then a failed u1_5 — the same
     * observation would be indistinguishable from the pending op having
     * landed; unique values are what make this detectable.) */
    aos_ack_insert(&c, 2, "u1_5_3");
    aos_ack_insert(&c, 2, "bi1_5_4");
    ASSERT_EQ_INT(aud_model_add_pending(&m, 0, 2, "u1_5_7"), 0,
                  "F: failed update goes pending");
    aos_behind_update(tc, "a3", "u1_5_3"); /* the acked bi1_5_4 rolled back */
    {
        int rv = aud_model_resolve_all_pending(tc, &m, AOS_OBJ, 0, logf, "F");
        TAP_DIAG("# scenario F: resolve violations=%d\n", rv);
        ASSERT_TRUE(rv >= 1, "F: rollback of an acked write is FLAGGED");
        ASSERT_EQ_STR(m.vals[2], "bi1_5_4",
                      "F: model kept the last acked value");
        aos_behind_update(tc, "a3", "bi1_5_4"); /* restore */
    }

    /* ===== Healthy path: the model and server are consistent again —
     * the strict verify must pass with zero violations (no false
     * positives). a1=B live, a2=C live, a3=bi1_5_4 live, a4 dead everywhere. */
    {
        int v = aud_verify_model(tc, &m, AOS_OBJ, 0, 0, logf);
        ASSERT_EQ_INT(v, 0, "healthy state verifies clean");
    }

    fclose(logf);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-audit-oracle-strictness", test_audit_oracle_strictness_run)
