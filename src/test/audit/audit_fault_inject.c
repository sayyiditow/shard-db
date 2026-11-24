/* src/test/audit/audit_fault_inject.c
 *
 * Audit-campaign fault injection (plan Phase 1 step 4, third harness;
 * docs/plans/2026-11-19-audit-campaign.md). Arms the audit/faultlib.c
 * LD_PRELOAD interposer (build/bin/audit-faultlib.so) against the
 * TEST_BUILD daemon with a seeded scenario matrix — one I/O op
 * (write/pwrite/fsync/fdatasync/open/mmap), one hit index, one errno
 * (ENOSPC/EIO/EMFILE), optionally a sticky (persistent) fault or a short
 * write — runs a small acknowledged-write workload, then restarts the
 * daemon CLEAN (no preload) and applies the full model oracle: acked
 * writes present, unacked writes fully applied or fully absent, no third
 * value, no resurrection, count == model. Fail-closed is the contract:
 * an erroring daemon is fine, a silent corruption is not; a daemon that
 * wedges is killed and must recover at restart.
 *
 * Also re-arms the engine's existing TEST_BUILD seams over the same
 * oracle: window-coordinator fail-phase (test_env_ctl_set_fail_phase,
 * sticky ENOSPC-style) and bitmap-remap fault (test_env_ctl_set_bmfix_fault),
 * per the plan's "reuse existing seams" decision. A seam combination
 * that has no coverage here is a gap, not an improvised mode.
 *
 * Seam availability: scenarios 8/9 need the control-channel era (the
 * fixtures wrappers and shard_test_ctl.h). Compile them out with
 * -DSHARD_AUDIT_NO_SEAMS (used by the 334b78d-era fault gate overlay,
 * whose parent predates the control channel — the human-approved
 * interposer-only variant), or disable them at runtime with
 * SHARD_AUDIT_FAULT_SEAMS=0. The seam scenarios stay ON for current-
 * code runs (default).
 *
 * Oracle: every non-acked op (error or timeout) is modeled as PENDING
 * in a per-replica multi-pending set — an erroring daemon may still have
 * made the write durable (error-but-durable forward replay) — and the
 * set is resolved against the restarted daemon's observed state before
 * the strict verify, so error-but-durable ops are legal and the final
 * count/value oracle stays exact.
 *
 * Fault placement: LD_PRELOAD is armed at spawn, so by default the Nth
 * hit can land in the daemon's startup I/O. Half the seeds (even) arm
 * SHARD_FAULT_AFTER_MS (seed-derived 1100..1700 ms), which delays the
 * interposer's counter until the workload is in flight — both
 * placements (startup and workload) are legal surfaces and both are
 * exercised.
 *
 * Budgets: SHARD_AUDIT_FAULT_SEEDS (default 2 smoke; plan budget is
 * every seam × 20 seeds via audit/scripts), SHARD_AUDIT_FAULT_OPS
 * (default 12), SHARD_AUDIT_FAULT_SEAMS (default 1). Violations log to
 * a PER-RUN directory under /tmp/shard-db-audit-logs/ (parallel runs
 * never append to each other's logs).
 *
 * Known-bug gate: deferred — the human picks the fix-commit pair from
 * the executor's candidate list (plan Phase 1 step 4, NEEDS-YOU).
 * Fault injection runs against normal builds only (sanitizer runtimes
 * win symbol interposition; see audit/faultlib.c header).
 *
 * Audit case: excluded from default run-all (TEST_REGISTER_AUDIT).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "audit_common.h"
#include "fixtures.h"
#ifndef SHARD_AUDIT_NO_SEAMS
#include "shard_test_ctl.h"
#define AUD_SEAMS_COMPILED 1
#endif
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define AUD_FOBJ "fo"
#define AUD_FOPS_MAX 24

/* Resolved once in the run function; aud_run_scenario reads it. */
static char g_faultlib_path[PATH_MAX];

static const char *FAULT_OPS[] = {
    "write", "pwrite", "fsync", "fdatasync", "open", "mmap"
};
#define AUD_FAULT_OP_N (int)(sizeof(FAULT_OPS) / sizeof(FAULT_OPS[0]))
static const int FAULT_ERRNOS[] = { 28 /*ENOSPC*/, 5 /*EIO*/, 24 /*EMFILE*/ };

/* Scenario-kind name for the log header (keeps the label from drifting
   away from what op_idx/hit/sticky actually drive). */
static const char *aud_kind_name(int scenario) {
    switch (scenario) {
    case 6: return "sticky-fsync";
    case 7: return "short-write";
    case 8: return "fail-phase-seam";
    case 9: return "bmfix-seam";
    default: return "interposed-op";
    }
}

typedef struct {
    char base[256], db_root[512];
    int port;
    /* Resolved absolute path of build/bin/audit-faultlib.so. LD_PRELOAD
       must be absolute: the daemonized server may chdir, so a repo-root
       relative path resolves against the wrong cwd. */
    char so_path[PATH_MAX];
} AudFaultCtx;

static void aud_rmrf_f(const char *dir) {
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    if (system(cmd) != 0) { /* best effort */ }
}

/* Spawn the daemon WITH the interposer armed per scenario. after_ms > 0
 * delays the interposer's hit counter past daemon startup, landing the
 * fault in the workload (see the header). */
static int aud_fault_spawn(TestEnv *env, const AudFaultCtx *fc, int op_idx,
                           int hit, int sticky, int errv, int shortw,
                           int after_ms) {
    char opn[16], nb[16], ab[16];
    snprintf(opn, sizeof(opn), "%s", FAULT_OPS[op_idx]);
    snprintf(nb, sizeof(nb), "%d", hit);
    snprintf(ab, sizeof(ab), "%d", after_ms);
    setenv("LD_PRELOAD", fc->so_path, 1);
    setenv("SHARD_FAULT_OP", opn, 1);
    setenv("SHARD_FAULT_N", nb, 1);
    if (sticky) setenv("SHARD_FAULT_STICKY", "1", 1);
    else unsetenv("SHARD_FAULT_STICKY");
    char eb[8];
    snprintf(eb, sizeof(eb), "%d", errv);
    setenv("SHARD_FAULT_ERRNO", eb, 1);
    if (shortw) setenv("SHARD_FAULT_SHORT", "1", 1);
    else unsetenv("SHARD_FAULT_SHORT");
    if (after_ms > 0) setenv("SHARD_FAULT_AFTER_MS", ab, 1);
    else unsetenv("SHARD_FAULT_AFTER_MS");
    int rc = test_env_start_at(env, fc->db_root, fc->port);
    unsetenv("LD_PRELOAD");
    unsetenv("SHARD_FAULT_OP");
    unsetenv("SHARD_FAULT_N");
    unsetenv("SHARD_FAULT_STICKY");
    unsetenv("SHARD_FAULT_ERRNO");
    unsetenv("SHARD_FAULT_SHORT");
    unsetenv("SHARD_FAULT_AFTER_MS");
    return rc;
}

/* Scenario taxonomy:
   0..5  interposed op failure (op = scenario), nth hit
   6     sticky fsync ENOSPC (persistent disk-full)
   7     short write (engine must detect, fail closed)
   8     window-coordinator fail seam (PHASE_P sticky, ENOSPC-style)
   9     bitmap-remap prepare-fail seam — 8/9 compiled only when the
         control-channel era provides them (see AUD_SEAMS_COMPILED);
         the interposer-only set is 0..7. */
#ifdef AUD_SEAMS_COMPILED
static const int AUD_SCENARIOS = 10;
#else
static const int AUD_SCENARIOS = 8;
#endif

static int aud_run_scenario(int seed, int n_ops, int seams, FILE *logf) {
    AudFaultCtx fc;
    memset(&fc, 0, sizeof(fc));
    snprintf(fc.base, sizeof(fc.base), "/tmp/shard-db-auditfault-%d-%d",
             (int)getpid(), seed);
    snprintf(fc.db_root, sizeof(fc.db_root), "%s/db", fc.base);
    snprintf(fc.so_path, sizeof(fc.so_path), "%s", g_faultlib_path);
    fc.port = test_pick_port();
    /* Even seeds land the fault in the workload (delayed arming); odd
     * seeds keep the nth-hit-from-load placement (startup faults). */
    int after_ms = (seed % 2 == 0) ? 1100 + (seed % 3) * 300 : 0;
    if (fc.port < 0) {
        fprintf(logf, "scenario %d: no port\n", seed);
        return 1;
    }
    aud_rmrf_f(fc.base);
    if (mkdir(fc.base, 0755) != 0 || mkdir(fc.db_root, 0755) != 0) {
        fprintf(logf, "scenario %d: mkdir failed\n", seed);
        return 1;
    }

    uint64_t rng = (uint64_t)seed * 6364136223846793005ULL + 1442695040888963407ULL;
    int n_scen = (seams && AUD_SCENARIOS > 8) ? AUD_SCENARIOS : 8;
    int scenario = (int)aud_rng_below(&rng, (uint32_t)n_scen);
    int op_idx = (int)aud_rng_below(&rng, (uint32_t)AUD_FAULT_OP_N);
    /* Pin kinds to the ops where the fault has physics: sticky-fsync
       (6) is meaningless on write/open, and a short write (7) on a
       sync/no-return op is a vacuous scenario. Draw order stays fixed
       (deterministic per seed). */
    if (scenario == 6)
        op_idx = 2 + (int)aud_rng_below(&rng, 2); /* fsync | fdatasync */
    else if (scenario == 7)
        op_idx = (int)aud_rng_below(&rng, 2);     /* write | pwrite */
    int hit = 1 + (int)aud_rng_below(&rng, 4);
    int errv = FAULT_ERRNOS[aud_rng_below(&rng, 3)];
    fprintf(logf, "scenario %d: kind=%d (%s) op=%s op_idx=%d hit=%d errno=%d\n",
            seed, scenario, aud_kind_name(scenario),
            scenario <= 7 ? FAULT_OPS[op_idx] : "(seam)", op_idx, hit, errv);

    AudModel model;
    aud_model_init(&model);
    AudOpLog oplog;
    aud_oplog_reset(&oplog);
    char key[16];
    for (int i = 0; i < 48; i++) {
        snprintf(key, sizeof(key), "%c%03d", i < 32 ? 'k' : 'f', i % 32);
        aud_model_add(&model, key);
    }

    TestEnv env = {0};
    int spawn_bad = 0;
    if (scenario == 8 || scenario == 9) {
        /* engine-seam scenarios start clean */
        if (test_env_start_at(&env, fc.db_root, fc.port) != 0) spawn_bad = 1;
    } else {
        if (aud_fault_spawn(&env, &fc, op_idx, hit,
                            scenario == 6, errv, scenario == 7,
                            after_ms) != 0)
            spawn_bad = 1;
    }
    if (spawn_bad) {
        /* A refused spawn is legal fail-closed behavior for open/EMFILE
           faults at startup; record and require the clean restart below
           to succeed on the (possibly initialized) root. */
        fprintf(logf, "scenario %d: initial spawn failed (legal fail-closed "
                "if startup I/O faulted); continuing to clean restart\n",
                seed);
    } else {
        aud_track_spawn(env.daemon_pid);
    }

    TestClientCfg cfg = { .port = fc.port, .io_timeout_ms = 8000 };
    TestClientCfg cfg_fast = { .port = fc.port, .io_timeout_ms = 4000 };
    TestClient *tc = spawn_bad ? NULL : tc_connect(&cfg);
    TestClient *tc_fast = spawn_bad ? NULL : tc_connect(&cfg_fast);

#ifdef AUD_SEAMS_COMPILED
    /* Engine-seam scenarios arm via the control channel after setup. */
    if (tc && scenario == 8) {
        (void)test_env_ctl_set_fail_phase(&env, SHARD_TEST_PHASE_P, hit, 1);
    } else if (tc && scenario == 9) {
        (void)test_env_ctl_set_bmfix_fault(&env, 1, hit);
    }
#endif

    /* Workload: baseline then mixed ops. Errors are fine (fail-closed);
       the oracle only cares about what ACKED. */
    int ops_done = 0;
    int ops_acked = 0;
    int acked_any = 0;
    if (tc) {
        char req[512], *resp = NULL;
        tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
        free(resp); resp = NULL;
        tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\""
            AUD_FOBJ "\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"v:varchar:48\"]}", &resp);
        free(resp); resp = NULL;
        for (int i = 0; i < n_ops && ops_done < AUD_FOPS_MAX; i++) {
            int idx = (int)aud_rng_below(&rng, 48);
            snprintf(key, sizeof(key), "%c%03d", idx < 32 ? 'k' : 'f',
                     idx % 32);
            int kidx = aud_model_idx(&model, key);
            uint32_t pick = aud_rng_below(&rng, 10);
            if (pick < 5) { /* insert (tolerate exists-error) */
                snprintf(req, sizeof(req),
                         "{\"mode\":\"insert\",\"dir\":\"default\","
                         "\"object\":\"" AUD_FOBJ "\",\"key\":\"%s\","
                         "\"value\":{\"v\":\"s%d_%d_%d\"}}", key, seed, idx,
                         ops_done);
                int rc = tc_request(tc, req, &resp);
                aud_oplog_add(&oplog, "insert %s v=s%d_%d_%d", key, seed, idx,
                              ops_done);
                if (rc == 0 && aud_acked(resp)) {
                    char v[AUD_VAL_MAX];
                    snprintf(v, sizeof(v), "s%d_%d_%d", seed, idx, ops_done);
                    aud_model_ack_insert(&model, kidx, v);
                    /* the ack supersedes any earlier in-flight op on the
                       key (error-but-durable replay lands BEFORE this
                       op's effect — same connection, sequential wire) */
                    aud_model_supersede(&model, 0, kidx);
                    aud_oplog_mark_acked(&oplog);
                    acked_any = 1;
                    ops_acked++;
                } else {
                    char v[AUD_VAL_MAX];
                    snprintf(v, sizeof(v), "s%d_%d_%d", seed, idx, ops_done);
                    aud_model_add_pending(&model, 0, kidx, v);
                }
                free(resp); resp = NULL;
            } else if (pick < 8) { /* update */
                snprintf(req, sizeof(req),
                         "{\"mode\":\"update\",\"dir\":\"default\","
                         "\"object\":\"" AUD_FOBJ "\",\"key\":\"%s\","
                         "\"value\":{\"v\":\"u%d_%d_%d\"}}", key, seed, idx,
                         ops_done);
                int rc = tc_request(tc, req, &resp);
                aud_oplog_add(&oplog, "update %s v=u%d_%d_%d", key, seed, idx,
                              ops_done);
                if (rc == 0 && aud_acked(resp)) {
                    char v[AUD_VAL_MAX];
                    snprintf(v, sizeof(v), "u%d_%d_%d", seed, idx, ops_done);
                    aud_model_ack_insert(&model, kidx, v);
                    aud_model_supersede(&model, 0, kidx);
                    aud_oplog_mark_acked(&oplog);
                    acked_any = 1;
                    ops_acked++;
                } else {
                    char v[AUD_VAL_MAX];
                    snprintf(v, sizeof(v), "u%d_%d_%d", seed, idx, ops_done);
                    aud_model_add_pending(&model, 0, kidx, v);
                }
                free(resp); resp = NULL;
            } else if (pick < 9) { /* delete */
                snprintf(req, sizeof(req),
                         "{\"mode\":\"delete\",\"dir\":\"default\","
                         "\"object\":\"" AUD_FOBJ "\",\"key\":\"%s\"}", key);
                int rc = tc_request(tc, req, &resp);
                aud_oplog_add(&oplog, "delete %s", key);
                if (rc == 0 && aud_acked(resp)) {
                    aud_model_ack_delete(&model, kidx);
                    aud_model_supersede(&model, 0, kidx);
                    aud_oplog_mark_acked(&oplog);
                    acked_any = 1;
                    ops_acked++;
                } else {
                    aud_model_add_pending(&model, 0, kidx, NULL);
                }
                free(resp); resp = NULL;
            } else { /* bulk-insert 4 */
                int bik[4], nbi = 0;
                int off = snprintf(req, sizeof(req),
                                   "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                                   "\"object\":\"" AUD_FOBJ "\",\"records\":[");
                for (int j = 0; j < 4; j++) {
                    int bidx = (int)aud_rng_below(&rng, 48);
                    snprintf(key, sizeof(key), "%c%03d",
                             bidx < 32 ? 'k' : 'f', bidx % 32);
                    off += snprintf(req + off, sizeof(req) - (size_t)off,
                                    "%s{\"key\":\"%s\",\"value\":{\"v\":"
                                    "\"bi%d_%d_%d\"}}", j ? "," : "", key,
                                    seed, bidx, ops_done);
                    bik[nbi++] = bidx;
                }
                off += snprintf(req + off, sizeof(req) - (size_t)off, "]}");
                int rc = tc_request(tc, req, &resp);
                aud_oplog_add(&oplog, "bulk4 rc=%d ack=%d %.*s", rc,
                              aud_acked(resp) ? 1 : 0,
                              (int)strlen(req) - 41, req + 40);
                if (rc == 0 && aud_acked(resp)) {
                    aud_oplog_mark_acked(&oplog);
                    acked_any = 1;
                    ops_acked++;
                    for (int j = 0; j < nbi; j++) {
                        char v[AUD_VAL_MAX];
                        snprintf(v, sizeof(v), "bi%d_%d_%d", seed, bik[j], ops_done);
                        aud_model_ack_insert(&model, bik[j], v);
                        aud_model_supersede(&model, 0, bik[j]);
                    }
                } else {
                    /* non-acked bulk: every record is pending (the window
                       may still have committed error-but-durably) */
                    for (int j = 0; j < nbi; j++) {
                        char v[AUD_VAL_MAX];
                        snprintf(v, sizeof(v), "bi%d_%d_%d", seed, bik[j], ops_done);
                        aud_model_add_pending(&model, 0, bik[j], v);
                    }
                }
                free(resp); resp = NULL;
            }
            ops_done++;
        }
        free(resp);
    }

    /* Teardown of the faulted daemon: wedged is expected (a parked/stuck
       window); kill unconditionally, then clean restart. */
    if (tc) tc_close(tc);
    if (tc_fast) tc_close(tc_fast);
    test_env_kill(&env);

    /* Clean restart — recovery must bring the root back serveable. */
    TestEnv env2 = {0};
    int restart_ok = (test_env_start_at(&env2, fc.db_root, fc.port) == 0);
    if (restart_ok) aud_track_spawn(env2.daemon_pid);
    int violations = 0;
    if (!restart_ok) {
        if (acked_any) {
            fprintf(logf, "VIOLATION [scenario %d]: clean restart failed "
                    "after fault with acknowledged writes — unrecoverable "
                    "state\n", seed);
            violations = 1;
        } else {
            /* Refusing a never-served root whose initialization faulted
               mid-stamp is legal fail-closed: nothing was acknowledged,
               so operator recovery is "wipe the stub root". */
            fprintf(logf, "scenario %d: clean restart refused a "
                    "never-served root (legal fail-closed; nothing was "
                    "acked)\n", seed);
        }
    } else {
        TestClient *vclient = tc_connect(&cfg);
        if (!vclient) {
            fprintf(logf, "VIOLATION [scenario %d]: connect failed after "
                    "clean restart\n", seed);
            violations = 1;
        } else {
            /* Resolve the pending set STRICTLY against the restarted
               daemon's observed state (an erroring daemon may still have
               made writes durable, so a value equal to the acked value or
               a pending value is legal — a third value, or absence under
               a live model, is a violation and is never adopted). A
               failed resolve means the replica state itself is unreadable
               — verify flags it. */
            int rv = aud_model_resolve_all_pending(vclient, &model, AUD_FOBJ,
                                                   0, logf, "fault");
            if (rv < 0) {
                fprintf(logf, "VIOLATION [scenario %d]: pending resolve "
                        "failed after clean restart\n", seed);
                violations = 1;
            }
            violations += rv > 0 ? rv : 0;
            /* Setup may have partially failed under faults: the object
               may not exist. Verify what ACKED on it; missing-object with
               a nonempty acked model is a violation. */
            violations += aud_verify_model(vclient, &model, AUD_FOBJ, 0, 0,
                                           logf);
            if (violations > 0) {
                /* Raw state dump for triage: count/describe responses as
                   the daemon actually returned them. */
                char dreq[256], *dresp = NULL;
                snprintf(dreq, sizeof(dreq),
                         "{\"mode\":\"count\",\"dir\":\"default\","
                         "\"object\":\"%s\"}", AUD_FOBJ);
                if (tc_request(vclient, dreq, &dresp) == 0)
                    fprintf(logf, "raw count resp: %.200s\n",
                            dresp ? dresp : "(none)");
                free(dresp); dresp = NULL;
                snprintf(dreq, sizeof(dreq),
                         "{\"mode\":\"describe-object\",\"dir\":"
                         "\"default\",\"object\":\"%s\"}", AUD_FOBJ);
                if (tc_request(vclient, dreq, &dresp) == 0)
                    fprintf(logf, "raw describe resp: %.300s\n",
                            dresp ? dresp : "(none)");
                free(dresp); dresp = NULL;
                fprintf(logf, "TRIAGE-KEEP base=%s\n", fc.base);
            }
            tc_close(vclient);
        }
        if (violations > 0) {
            /* keep the scratch root on disk for triage */
            test_env_stop_keep(&env2);
        } else {
            test_env_stop(&env2);
        }
    }
    (void)tc_fast;
    /* Reset the spawned-daemon registry per scenario: a long seed run
       (SHARD_AUDIT_FAULT_SEEDS=20 budget) would otherwise overflow
       AUD_MAX_SPAWNED and trip the loud-overflow abort. The sweep is a
       no-op here — both daemons were already stopped/killed above. */
    aud_kill_spawned();
    if (violations == 0) aud_rmrf_f(fc.base);

    if (violations > 0) {
        fprintf(logf, "scenario %d SUMMARY: %d violation(s)\n", seed,
                violations);
        fprintf(logf, "op log ([A]=acked [u]=unacked):\n");
        aud_oplog_dump(&oplog, logf);
        fprintf(logf, "model live at verify: ");
        for (int i = 0; i < model.nkeys; i++)
            if (model.live[i]) fprintf(logf, "%s ", model.keys[i]);
        fprintf(logf, "\n");
        return 1;
    }
    TAP_DIAG("# fault scenario %d: ok (kind=%d %s, after_ms=%d, ops %d/%d "
             "acked, model live %d)\n",
             seed, scenario, aud_kind_name(scenario), after_ms, ops_acked,
             ops_done, model.live_count);
    return 0;
}

/* This harness keeps the log minimal (scenario header + violations);
   the model's per-op history lives in the model itself. */

static int audit_fault_inject_run(void) {
    int seeds = aud_env_int("SHARD_AUDIT_FAULT_SEEDS", 2);
    int n_ops = aud_env_int("SHARD_AUDIT_FAULT_OPS", 12);
    int first = aud_env_int("SHARD_AUDIT_FAULT_FIRST_SEED", 1);
    int seams = aud_env_int("SHARD_AUDIT_FAULT_SEAMS", 1);
    if (seeds < 1) seeds = 1;
#ifndef AUD_SEAMS_COMPILED
    seams = 0; /* compiled out: interposer-only is the whole set */
#endif

    mkdir("/tmp/shard-db-audit-logs", 0755);

    /* Per-run log directory: parallel or back-to-back runs never append
       into each other's scenario logs. */
    char logdir[160];
    snprintf(logdir, sizeof(logdir), "/tmp/shard-db-audit-logs/fault-%ld-%d",
             (long)time(NULL), (int)getpid());
    mkdir(logdir, 0755);

    /* The interposer .so is built by build.sh (Linux only); tests run
       from the repo root, so the canonical path is build/bin. Resolve it
       once, absolute: LD_PRELOAD must survive the daemon's chdir. A
       missing .so fails the case outright — the interposer scenarios are
       the point of this harness, not a spawn-failure smoke. */
    {
        char raw[PATH_MAX];
        snprintf(raw, sizeof(raw), "./build/bin/audit-faultlib.so");
        if (!realpath(raw, g_faultlib_path) ||
            access(g_faultlib_path, X_OK) != 0) {
            ASSERT_TRUE(0,
                        "audit-faultlib.so missing (build/bin/audit-faultlib.so); "
                        "the fault-injection harness cannot run — build.sh "
                        "produces it on Linux");
            return t_ctx->failed > 0 ? 1 : 0;
        }
    }

    int failed = 0;
    for (int s = first; s < first + seeds; s++) {
        char logpath[256];
        snprintf(logpath, sizeof(logpath), "%s/seed%d.log", logdir, s);
        FILE *logf = fopen(logpath, "a");
        if (!logf) logf = stderr;
        if (aud_run_scenario(s, n_ops, seams, logf) != 0) failed++;
        if (logf != stderr) fclose(logf);
    }
    aud_kill_spawned();
    char desc[160];
    snprintf(desc, sizeof(desc),
             "audit-fault-inject: %d/%d scenarios passed (log: %s)",
             seeds - failed, seeds, logdir);
    ASSERT_EQ_INT(failed, 0, desc);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER_AUDIT("audit-fault-inject", audit_fault_inject_run)
