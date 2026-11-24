/* src/test/audit/audit_crash_restart.c
 *
 * Audit-campaign crash/restart model fuzzer (plan Phase 1 step 4, first
 * harness; docs/plans/2026-11-19-audit-campaign.md). Known-bug gate
 * (SHARD_AUDIT_MODE=crafted, deterministic): craft the incident shape the
 * way the fix's own regression test (test_slotcask_grave_run_frontier.c)
 * does — head-of-stream graves written by pwrite while the daemon is
 * down, live records beyond them — then run the normal kill → restart →
 * validate_live_refs cycle. Acceptance: red on 163b36a, green on
 * 2026.11.2, deterministically. Kill-only modes (default) stay as the
 * general fuzzer; their gate evidence (base-only SIGBUS, load-run oracle
 * failures) lives in docs/plans/audit-artifacts/ as AUD-GATE-1
 * supplementary material.
 *
 * Kill-only mode per seed: apply a seeded-random write sequence
 * (insert/update/delete/bulk-insert/bulk-delete/CAS-update/vacuum) to
 * two twin objects over the wire — "ca" plain full-scan and "cb"
 * btree-indexed on v — with ONE PER-REPLICA MODEL each ("models[2]"):
 * every op is built once from the RNG and replica-A's state, then sent
 * to A and mirrored to B; each replica's model is updated only by its
 * own acknowledged responses, so a kill op that only reached A never
 * poisons B's legal set. The TEST_BUILD daemon is SIGKILLed two ways:
 *   - uniform: op pushed un-waited (tc_send), random 0-4 ms, then kill;
 *   - biased: arm the payload-staging pause (SHARD_TEST_PHASE_P
 *     occurrence 1) via the test-control channel, push one update
 *     through the short-timeout connection, SIGKILL while the window
 *     coordinator is parked.
 * After every kill the root is reopened IN-PROCESS (slotcask_open +
 * slotcask_validate_live_refs == 0, oracle (a) exactly per the plan);
 * the daemon restarts (forward replay, bmfix, recovery), pending state
 * is resolved by classifying the OBSERVED value (the acked value or the
 * pending op's value — anything else is a violation, never adopted), the
 * strict wire oracle runs on both twins after EVERY restart, and the
 * sequence continues. The final boot runs it once more: acked writes
 * present, unacked writes fully applied or fully absent, no third value
 * ever, no resurrection, count == model (each twin against its own model).
 *
 * Crafted gate mode (deterministic): insert gA..gH on both twins, delete
 * gA/gB, stop the daemon gracefully, locate both records' bytes in the
 * segment files and pwrite their flag bytes to 0 (head-of-stream grave
 * run: gA+gB exceed the slot_size resync window with gC.. live beyond),
 * restart, run append pressure (base's mis-parked frontier crushes the
 * live records beyond the run), a biased parked kill, in-process
 * validation, and the strict wire oracle. Base must fail; fixed must
 * pass; identical every run.
 *
 * Budgets are env-driven so the default smoke (SEEDS=2, OPS=30,
 * BIASED_CYCLES=2) stays cheap while the plan budgets (2000 seeds × ≤50
 * ops normal; 300 seeds ASan) run from audit/scripts. Failing seeds dump
 * op-log + violations to /tmp/shard-db-audit-logs/. Replay one seed with
 * SHARD_AUDIT_FIRST_SEED=<N> SHARD_AUDIT_SEEDS=1.
 *
 * Audit case: excluded from default run-all (TEST_REGISTER_AUDIT).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "audit_common.h"
#include "fixtures.h"
#include "slotcask.h"
#include "types.h"    /* compute_hash_raw — crafted-mode key solve */
#include "shard_test_ctl.h"
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define AUD_OBJ_A "ca" /* plain twin — full-scan oracle replica 0 */
#define AUD_OBJ_B "cb" /* btree twin — replica 1 (index on v) */
#define AUD_HOT_KEYS 96
#define AUD_FRESH_KEYS 64
#define AUD_BASELINE 12

typedef struct {
    char base[256], db_root[512];
    int port;
    int splits, slot_size, streams;
} AudEnvCtx;

/* ---- small local helpers ---- */

static void aud_rmrf(const char *dir) {
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    if (system(cmd) != 0) {
        /* best effort */
    }
}

static int aud_parse_int_field(const char *resp, const char *field) {
    char pat[48];
    snprintf(pat, sizeof(pat), "\"%s\":", field);
    const char *p = resp ? strstr(resp, pat) : NULL;
    return p ? atoi(p + strlen(pat)) : -1;
}

static int aud_count_stream_dirs(const char *data_dir) {
    char p[512];
    snprintf(p, sizeof(p), "%s/streams", data_dir);
    DIR *d = opendir(p);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL)
        if (e->d_name[0] >= '0' && e->d_name[0] <= '9') n++;
    closedir(d);
    return n;
}

static void aud_make_key(char *out, size_t cap, int i, int fresh) {
    snprintf(out, cap, "%c%04d", fresh ? 'f' : 'k', i);
}

static int aud_pick_key(const AudModel *m, uint64_t *rng, int want_live) {
    for (int tries = 0; tries < 24; tries++) {
        int i = (int)aud_rng_below(rng, (uint32_t)m->nkeys);
        if (m->live[i] == (unsigned char)want_live) return i;
    }
    return -1;
}

/* ---- crafted deterministic gate mode (SHARD_AUDIT_MODE=crafted) ---- */

#define AUD_CRAFT_KEYS 8 /* gA..gH; 15 chars — model key buffers cap at 16 */
static const char *AUD_CRAFT_KEY[AUD_CRAFT_KEYS] = {
    "gA0000000000001", "gB0000000000002",
    "gC0000000000003", "gD0000000000004",
    "gE0000000000005", "gF0000000000006",
    "gG0000000000007", "gH0000000000008"
};
/* Values are ~40 bytes: record pitch lands near 88 (24 hdr + 15 key +
 * 43 val, padded), so the two crafted graves span ~176 — MORE than the
 * object's slot_size resync window (116 for v:varchar:64 + s:int) while
 * still being two records. Base's resync skips flag=0 targets and needs
 * a hash-valid live/tombstone header within slot_size bytes, so a run
 * longer than slot_size before the next live record breaks the scan and
 * parks reserve_off at the run start (the incident). With ~50-byte
 * values the pitch stays under the window and base always steps
 * correctly — the geometry is part of the gate. */
static const char *AUD_CRAFT_VAL[AUD_CRAFT_KEYS] = {
    "valAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "valBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "valCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
    "valDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
    "valEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
    "valFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    "valGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG",
    "valHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH"
};

/* Locate `key`'s record bytes in the object's segment files and pwrite
   the flag byte (record offset + 18) to 0 — the exact crafting
   test_slotcask_grave_run_frontier.c performs. Returns the number of
   records flipped (must be exactly 1; more hits means the crafted shape
   is ambiguous and the gate aborts as an environment failure). */
static int aud_craft_grave(const AudEnvCtx *ec, const char *obj,
                           const char *key, FILE *logf) {
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/default/%s/data/streams",
             ec->db_root, obj);
    DIR *d = opendir(dir);
    if (!d) {
        fprintf(logf, "CRAFT: opendir %s failed: %s\n", dir,
                strerror(errno));
        return -1;
    }
    struct dirent *e;
    char hit_path[768] = "";
    long hit_off = -1;
    int hits = 0;
    size_t klen = strlen(key);
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char seg[640];
        snprintf(seg, sizeof(seg), "%s/%s", dir, e->d_name);
        DIR *sd = opendir(seg);
        if (!sd) continue;
        struct dirent *se;
        while ((se = readdir(sd)) != NULL) {
            if (se->d_name[0] < '0' || se->d_name[0] > '9') continue;
            char path[768];
            snprintf(path, sizeof(path), "%s/%s", seg, se->d_name);
            int fd = open(path, O_RDONLY);
            if (fd < 0) continue;
            char *buf = NULL;
            size_t cap = 0, len = 0;
            for (;;) {
                if (len == cap) {
                    cap = cap ? cap * 2 : 65536;
                    char *np = realloc(buf, cap);
                    if (!np) break;
                    buf = np;
                }
                ssize_t n = read(fd, buf + len, cap - len);
                if (n < 0) break;
                if (n == 0) break;
                len += (size_t)n;
            }
            close(fd);
            if (!buf) continue;
            char *p = buf;
            for (;;) {
                char *q = memmem(p, len - (size_t)(p - buf), key, klen);
                if (!q) break;
                hits++;
                snprintf(hit_path, sizeof(hit_path), "%s", path);
                hit_off = (long)(q - buf);
                p = q + 1;
            }
            free(buf);
        }
        closedir(sd);
    }
    closedir(d);
    if (hits != 1) {
        fprintf(logf, "CRAFT: key %s found %d times in %s (want 1)\n",
                key, hits, dir);
        return -1;
    }
    int fd = open(hit_path, O_WRONLY);
    if (fd < 0) {
        fprintf(logf, "CRAFT: open %s for pwrite failed: %s\n", hit_path,
                strerror(errno));
        return -1;
    }
    ssize_t w = pwrite(fd, "\0", 1, hit_off + 18);
    close(fd);
    if (w != 1) {
        fprintf(logf, "CRAFT: flag pwrite at %s+%ld failed: %s\n",
                hit_path, hit_off + 18, strerror(errno));
        return -1;
    }
    fprintf(logf, "CRAFT: %s flag->0 at %s+%ld\n", key, hit_path,
            hit_off + 18);
    return 1;
}

/* ---- in-process validation of the killed root (oracle (a)) ---- */

/* Read-only oracle (a), exactly per the plan: open the killed root
   in-process (as test_slotcask_grave_run_frontier.c models) and require
   slotcask_validate_live_refs == 0 invalid refs. Frontier corruption
   needs no probe appends: on the buggy base the daemon's own startup
   recovery parks reserve_off inside a grave run, and the daemon's next
   wire writes clobber live records — caught by the wire oracle (b/c). */
static int aud_inprocess_validate(const AudEnvCtx *ec, const char *obj,
                                  FILE *logf, const char *stage) {
    SlotcaskDb db;
    char data_dir[512];
    snprintf(data_dir, sizeof(data_dir), "%s/default/%s/data",
             ec->db_root, obj);
    memset(&db, 0, sizeof(db));
    if (slotcask_open(&db, data_dir, ec->splits, ec->streams,
                      ec->slot_size) != 0) {
        fprintf(logf, "VIOLATION [%s] %s: in-process slotcask_open failed\n",
                obj, stage);
        return 1;
    }
    int bad = 0;
    uint64_t inv = 1;
    if (slotcask_validate_live_refs(&db, &inv) != 0 || inv != 0) {
        fprintf(logf,
                "VIOLATION [%s] %s: slotcask_validate_live_refs = %llu "
                "invalid refs (dangling/clobbered kf refs)\n",
                obj, stage, (unsigned long long)inv);
        bad = 1;
    }
    slotcask_close(&db);
    return bad;
}

/* ---- op specification: chosen once from the RNG, applied to both twins ---- */

typedef struct {
    int type;        /* 0 insert, 1 update, 2 delete, 3 bulk-insert,
                        4 bulk-delete, 5 cas-update, 6 vacuum */
    int idx;         /* primary model key (single-key ops) */
    int n;           /* bulk key count */
    int chosen[8];   /* bulk key model indices */
    char val[AUD_VAL_MAX];
} AudOp;

/* Every written value carries the op's sequence number (opn): a value is
   unique to one op, so a pending (error-but-durable) value can never be
   equal to an earlier ACKED value of the same key — an observed value
   that equals an acked one can then only be that acked write, never a
   later failed op re-using its text. */
static void aud_build_op(AudOp *op, uint64_t *rng, const AudModel *m,
                         int seed, int opn) {
    memset(op, 0, sizeof(*op));
    op->idx = -1;
    switch (aud_rng_below(rng, 7)) {
    case 0: {
        op->type = 0;
        op->idx = aud_pick_key(m, rng, 0);
        if (op->idx >= 0)
            snprintf(op->val, sizeof(op->val), "i%d_%d_%d", seed, op->idx, opn);
        break;
    }
    case 1:
    case 6: {
        op->type = 1;
        op->idx = aud_pick_key(m, rng, 1);
        if (op->idx >= 0)
            snprintf(op->val, sizeof(op->val), "u%d_%d_%d", seed, op->idx, opn);
        break;
    }
    case 2: {
        op->type = 2;
        op->idx = aud_pick_key(m, rng, 1);
        break;
    }
    case 3: {
        op->type = 3;
        snprintf(op->val, sizeof(op->val), "%d_%d", seed, opn);
        op->n = 4 + (int)aud_rng_below(rng, 5);
        if (op->n > 8) op->n = 8;
        for (int j = 0; j < op->n; j++) {
            int fresh = (int)aud_rng_below(rng, 100) < 75;
            int ki = (int)aud_rng_below(rng, fresh ? AUD_FRESH_KEYS
                                                   : AUD_HOT_KEYS);
            char key[16];
            aud_make_key(key, sizeof(key), ki, fresh);
            int kidx = aud_model_idx(m, key);
            if (kidx < 0) kidx = aud_model_add((AudModel *)m, key);
            op->chosen[j] = kidx;
        }
        break;
    }
    case 4: {
        op->type = 4;
        int want = 2 + (int)aud_rng_below(rng, 4);
        for (int j = 0; j < want && op->n < 8; j++) {
            int ci = aud_pick_key(m, rng, 1);
            if (ci < 0) break;
            int dup = 0;
            for (int k = 0; k < op->n; k++) dup |= (op->chosen[k] == ci);
            if (dup) continue;
            op->chosen[op->n++] = ci;
        }
        if (op->n == 0) op->type = 6; /* nothing to delete → vacuum instead */
        break;
    }
    case 5: {
        op->type = 5;
        op->idx = aud_pick_key(m, rng, 1);
        if (op->idx >= 0) {
            snprintf(op->val, sizeof(op->val), "cas%d_%d_%d", seed, op->idx, opn);
            /* guard: replica A's stable value */
        } else {
            op->type = 6;
        }
        break;
    }
    default:
        op->type = 6; /* vacuum */
        break;
    }
}

/* Build the JSON request for one op on one replica object. Returns the
   length written (0 = nothing to send). */
static int aud_op_request(const AudOp *op, const AudModel *m, const char *obj,
                          char *buf, size_t cap) {
    switch (op->type) {
    case 0:
        return snprintf(buf, cap,
                        "{\"mode\":\"insert\",\"dir\":\"default\","
                        "\"object\":\"%s\",\"key\":\"%s\","
                        "\"value\":{\"v\":\"%s\"}}",
                        obj, m->keys[op->idx], op->val);
    case 1:
        return snprintf(buf, cap,
                        "{\"mode\":\"update\",\"dir\":\"default\","
                        "\"object\":\"%s\",\"key\":\"%s\","
                        "\"value\":{\"v\":\"%s\"}}",
                        obj, m->keys[op->idx], op->val);
    case 2:
        return snprintf(buf, cap,
                        "{\"mode\":\"delete\",\"dir\":\"default\","
                        "\"object\":\"%s\",\"key\":\"%s\"}",
                        obj, m->keys[op->idx]);
    case 3: {
        int off = snprintf(buf, cap,
                           "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                           "\"object\":\"%s\",\"records\":[", obj);
        for (int j = 0; j < op->n && off < (int)cap - 128; j++) {
            int kidx = op->chosen[j];
            if (kidx < 0) continue;
            off += snprintf(buf + off, cap - (size_t)off,
                            "%s{\"key\":\"%s\",\"value\":{\"v\":\"bi%s_%d\"}}",
                            j ? "," : "", m->keys[kidx], op->val, kidx);
        }
        off += snprintf(buf + off, cap - (size_t)off, "]}");
        return off;
    }
    case 4: {
        int off = snprintf(buf, cap,
                           "{\"mode\":\"bulk-delete\",\"dir\":\"default\","
                           "\"object\":\"%s\",\"keys\":[", obj);
        for (int j = 0; j < op->n && off < (int)cap - 16; j++)
            off += snprintf(buf + off, cap - (size_t)off,
                            "%s\"%s\"", j ? "," : "", m->keys[op->chosen[j]]);
        off += snprintf(buf + off, cap - (size_t)off, "]}");
        return off;
    }
    case 5:
        return snprintf(buf, cap,
                        "{\"mode\":\"update\",\"dir\":\"default\","
                        "\"object\":\"%s\",\"key\":\"%s\","
                        "\"value\":{\"v\":\"%s\"},"
                        "\"if\":[{\"field\":\"v\",\"op\":\"eq\","
                        "\"value\":\"%s\"}]}",
                        obj, m->keys[op->idx], op->val, m->vals[op->idx]);
    default:
        return snprintf(buf, cap,
                        "{\"mode\":\"vacuum\",\"dir\":\"default\","
                        "\"object\":\"%s\"}", obj);
    }
}

/* Apply the model effect of one ACKED op to ONE replica's model. Bulk
   values mirror the request text exactly (bi<val>_<kidx>). Every acked
   key's earlier pendings are superseded (see audit_common.h). */
static void aud_op_model_apply(const AudOp *op, AudModel *m, int replica) {
    switch (op->type) {
    case 0:
    case 1:
    case 5:
        aud_model_ack_insert(m, op->idx, op->val);
        aud_model_supersede(m, replica, op->idx);
        break;
    case 2:
        aud_model_ack_delete(m, op->idx);
        aud_model_supersede(m, replica, op->idx);
        break;
    case 3:
        for (int j = 0; j < op->n; j++) {
            int kidx = op->chosen[j];
            if (kidx >= 0) {
                char v[AUD_VAL_MAX];
                snprintf(v, sizeof(v), "bi%s_%d", op->val, kidx);
                aud_model_ack_insert(m, kidx, v);
                aud_model_supersede(m, replica, kidx);
            }
        }
        break;
    case 4:
        for (int j = 0; j < op->n; j++)
            if (op->chosen[j] >= 0) {
                aud_model_ack_delete(m, op->chosen[j]);
                aud_model_supersede(m, replica, op->chosen[j]);
            }
        break;
    default:
        break;
    }
}

static const char *aud_op_name(const AudOp *op) {
    switch (op->type) {
    case 0: return "insert";
    case 1: return "update";
    case 2: return "delete";
    case 3: return "bulk-insert";
    case 4: return "bulk-delete";
    case 5: return "cas-update";
    default: return "vacuum";
    }
}

/* Run one normal op: send to replica A then mirror to B. Each replica's
   model is updated only by its own ack. Returns 2 when replica A's
   connection died (op unresolved there — caller treats as a crash). */
/* A request that was not acknowledged (error reply, timeout, dead
   connection) may still have been applied — forward replay can land an
   error-but-durable write. Record every key it touches as pending on that
   replica so verification accepts either outcome (and nothing else). */
static void aud_note_unacked(const AudOp *op, AudModel *m, int replica) {
    switch (op->type) {
    case 0: case 1: case 5:
        if (op->idx >= 0) aud_model_add_pending(m, replica, op->idx, op->val);
        break;
    case 2:
        if (op->idx >= 0) aud_model_add_pending(m, replica, op->idx, NULL);
        break;
    case 3:
        for (int j = 0; j < op->n; j++)
            if (op->chosen[j] >= 0) {
                char v[AUD_VAL_MAX];
                snprintf(v, sizeof(v), "bi%s_%d", op->val, op->chosen[j]);
                aud_model_add_pending(m, replica, op->chosen[j], v);
            }
        break;
    case 4:
        for (int j = 0; j < op->n; j++)
            if (op->chosen[j] >= 0)
                aud_model_add_pending(m, replica, op->chosen[j], NULL);
        break;
    default:
        break;
    }
}

static int aud_run_normal_op(TestClient *tc, const AudOp *op,
                             AudModel models[2], AudOpLog *log) {
    if (op->idx < 0 && op->type != 3 && op->type != 4 && op->type != 6)
        return 0;
    char req[2048], *resp = NULL;
    int rc = aud_op_request(op, &models[0], AUD_OBJ_A, req, sizeof(req));
    if (rc <= 0) return 0;
    int a_rc = tc_request(tc, req, &resp);
    aud_oplog_add(log, "%s %s", aud_op_name(op),
                  (op->idx >= 0 ? models[0].keys[op->idx] : "(bulk)"));
    int a_acked = (a_rc == 0) && aud_acked(resp);
    if (a_acked) {
        aud_oplog_mark_acked(log);
        aud_op_model_apply(op, &models[0], 0);
    } else {
        aud_note_unacked(op, &models[0], 0);
    }
    free(resp);
    if (a_rc != 0) return 2; /* connection-level failure: unresolved */

    rc = aud_op_request(op, &models[1], AUD_OBJ_B, req, sizeof(req));
    if (rc > 0) {
        int b_rc = tc_request(tc, req, &resp);
        int b_acked = (b_rc == 0) && aud_acked(resp);
        if (b_acked) {
            aud_op_model_apply(op, &models[1], 1);
        } else {
            aud_note_unacked(op, &models[1], 1);
            aud_oplog_add(log, "  replica B not acked (%.70s)",
                          resp ? resp : "no response");
        }
        free(resp);
    }
    return 0;
}

/* ---- kills ---- */

/* Biased kill: arm the P-phase pause (occurrence 1), push one update
   through the short-timeout connection, SIGKILL while parked. The op
   targets replica A only; models[0] records the pending state. Returns 1
   if killed parked (grave left), 0 if the op completed normally (pause
   missed — disarmed; both replicas mirrored). */
static int aud_biased_kill(TestEnv *env, TestClient *tc, TestClient *tc_fast,
                           AudModel models[2], AudOpLog *log, uint64_t *rng,
                           int seed, int cycle) {
    /* Deterministic bias: always the FIRST live key, so every cycle works
       the same stream (keys are hash-routed to one stream) — the plan's
       "repeated kills at the staged-write pause on one stream".
       Two-step shape, matching the incident forensics (fix plan: graves
       formed where a rewrite reused the key's own freed slot, leaving an
       INTERIOR grave with live records beyond it):
       1. COMPLETE one update of the target key — its old kf slot is
          tombstoned and freed (size-bucketed pool).
       2. Kill the follow-up update mid-P: its staged payload lands IN
          THAT FREED SLOT — an interior grave — while the pre-update's
          record sits beyond it at the tail. Base's bounded resync window
          (slot_size ~ 2× record pitch) then breaks at the grave on
          restart, parks reserve_off before it, and the next appends
          crush the live records beyond — dangling refs, wrong values.
       A tail-appended grave (kill without the pre-update) is overwritten
       inertly by base's mis-parked frontier and never reproduces. */
    int idx = -1;
    for (int i = 0; i < models[0].nkeys; i++) {
        if (models[0].live[i]) { idx = i; break; }
    }
    if (idx < 0) return 0;
    char val[48];
    snprintf(val, sizeof(val), "b%d_c%d", seed, cycle);
    char pval[48];
    snprintf(pval, sizeof(pval), "p%d_c%d", seed, cycle);

    /* Step 1 — completed update (both replicas), freeing the old slot. */
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
             AUD_OBJ_A, models[0].keys[idx], pval);
    int rc = tc_request(tc, req, &resp);
    aud_oplog_add(log, "pre-update %s -> %s", models[0].keys[idx], pval);
    if (rc == 0 && aud_acked(resp)) {
        aud_oplog_mark_acked(log);
        aud_model_ack_insert(&models[0], idx, pval);
        aud_model_supersede(&models[0], 0, idx);
    } else {
        /* not acked ≠ not applied: an error-but-durable pre-update is a
           legal outcome, so it goes to the pending set */
        aud_model_add_pending(&models[0], 0, idx, pval);
    }
    free(resp); resp = NULL;
    int len = snprintf(req, sizeof(req),
                       "{\"mode\":\"update\",\"dir\":\"default\","
                       "\"object\":\"%s\",\"key\":\"%s\","
                       "\"value\":{\"v\":\"%s\"}}",
                       AUD_OBJ_B, models[0].keys[idx], pval);
    if (len > 0) {
        int b_rc = tc_request(tc, req, &resp);
        if (b_rc == 0 && aud_acked(resp)) {
            aud_model_ack_insert(&models[1], idx, pval);
            aud_model_supersede(&models[1], 1, idx);
        } else {
            aud_model_add_pending(&models[1], 1, idx, pval);
        }
        free(resp); resp = NULL;
    }

    /* Step 2 — parked kill of the follow-up update (replica A only). */
    snprintf(req, sizeof(req),
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
             AUD_OBJ_A, models[0].keys[idx], val);
    if (test_env_ctl_set_pause_phase(env, SHARD_TEST_PHASE_P, 1) != 0)
        return 0;
    rc = tc_request(tc_fast, req, &resp);
    if (rc != 0 || !resp) {
        /* No response within the short timeout: the window coordinator is
           parked at the payload-staging barrier. Kill deterministically. */
        aud_oplog_add(log,
                      "BIASED KILL parked at P: update %s -> %s (replica A "
                      "pending)", models[0].keys[idx], val);
        aud_model_set_pending(&models[0], 0, idx, val);
        test_env_kill(env);
        return 1;
    }
    /* The attempt missed the park and got a response — but a response is
       not an ack: an error object means the update did not apply (or
       applied error-but-durably). Check before folding; a non-acked
       response goes to the pending set, never straight to acked. */
    int missed_acked = (rc == 0 && resp && aud_acked(resp));
    free(resp);
    resp = NULL;
    (void)test_env_ctl_set_pause_phase(env, -1, 0);
    if (missed_acked) {
        aud_oplog_add(log, "update %s (biased attempt missed the park)",
                      models[0].keys[idx]);
        aud_oplog_mark_acked(log);
        aud_model_ack_insert(&models[0], idx, val);
        aud_model_supersede(&models[0], 0, idx);
    } else {
        aud_oplog_add(log, "update %s (biased attempt missed the park, "
                      "not acked)", models[0].keys[idx]);
        aud_model_set_pending(&models[0], 0, idx, val);
    }
    len = snprintf(req, sizeof(req),
                       "{\"mode\":\"update\",\"dir\":\"default\","
                       "\"object\":\"%s\",\"key\":\"%s\","
                       "\"value\":{\"v\":\"%s\"}}",
                       AUD_OBJ_B, models[0].keys[idx], val);
    if (len > 0) {
        char *resp2 = NULL;
        /* The normal connection, not the 500 ms one: a timeout on the
           short connection would leave an unread reply behind and desync
           every later request on it (tc_request has no resync). */
        int b_rc = tc_request(tc, req, &resp2);
        if (b_rc == 0 && aud_acked(resp2)) {
            aud_model_ack_insert(&models[1], idx, val);
            aud_model_supersede(&models[1], 1, idx);
        } else {
            aud_model_add_pending(&models[1], 1, idx, val);
        }
        free(resp2);
    }
    return 0;
}

/* Uniform kill: push one op un-waited to replica A, sleep a random
   0-4 ms, SIGKILL. Replica A gets the pending entry; B was never sent
   the op and has no pending. */
static unsigned uk_n; /* per-process: every uniform-kill value unique */
static void aud_uniform_kill(TestEnv *env, TestClient *tc, AudModel models[2],
                             AudOpLog *log, uint64_t *rng, int seed) {
    uint32_t which = aud_rng_below(rng, 3);
    int idx;
    char val[48], req[512];
    int have_req = 0;
    if (which == 0 && (idx = aud_pick_key(&models[0], rng, 1)) >= 0) {
        snprintf(val, sizeof(val), "k%d_%d_u%u", seed, idx, ++uk_n);
        snprintf(req, sizeof(req),
                 "{\"mode\":\"update\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
                 AUD_OBJ_A, models[0].keys[idx], val);
        aud_oplog_add(log, "UNIFORM KILL during: update %s -> %s",
                      models[0].keys[idx], val);
        aud_model_set_pending(&models[0], 0, idx, val);
        have_req = 1;
    } else if (which == 1 && (idx = aud_pick_key(&models[0], rng, 0)) >= 0) {
        snprintf(val, sizeof(val), "k%d_%d_u%u", seed, idx, ++uk_n);
        snprintf(req, sizeof(req),
                 "{\"mode\":\"insert\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"key\":\"%s\",\"value\":{\"v\":\"%s\"}}",
                 AUD_OBJ_A, models[0].keys[idx], val);
        aud_oplog_add(log, "UNIFORM KILL during: insert %s",
                      models[0].keys[idx]);
        aud_model_set_pending(&models[0], 0, idx, val);
        have_req = 1;
    } else {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"vacuum\",\"dir\":\"default\","
                 "\"object\":\"%s\"}", AUD_OBJ_A);
        aud_oplog_add(log, "UNIFORM KILL during: vacuum");
    }
    if (!have_req || tc_send(tc, req) == 0) {
        struct timespec ts = {
            0, (long)aud_rng_below(rng, 4000) * 1000L /* 0-4 ms */
        };
        nanosleep(&ts, NULL);
    }
    test_env_kill(env);
}

/* ---- one seed ---- */

/* Make sure the previous daemon is gone, then boot a fresh one and
   STRICTLY re-resolve pending state (violations are counted, never
   adopted — see audit_common.h). Returns 0 on success, -1 on a
   transport-level failure; *viol_out accumulates resolve violations. */
static int aud_restart(TestEnv *env, const char *db_root, int port,
                       TestClient **tc, TestClient **tc_fast,
                       const TestClientCfg *cfg, const TestClientCfg *cfg_fast,
                       AudModel models[2], FILE *logf, int seed,
                       int *viol_out) {
    if (*tc) { tc_close(*tc); *tc = NULL; }
    if (*tc_fast) { tc_close(*tc_fast); *tc_fast = NULL; }
    /* A restart is only reached after a kill (or a missed-park attempt
       that left the daemon alive): kill any survivor so the new spawn
       cannot collide with a live holder of the root. */
    if (env->daemon_pid > 0 && kill(env->daemon_pid, 0) == 0)
        test_env_kill(env);
    if (test_env_start_at(env, db_root, port) != 0) {
        fprintf(logf, "seed %d: restart failed\n", seed);
        return -1;
    }
    aud_track_spawn(env->daemon_pid);
    *tc = tc_connect(cfg);
    *tc_fast = tc_connect(cfg_fast);
    if (!*tc || !*tc_fast) {
        fprintf(logf, "seed %d: reconnect failed\n", seed);
        return -1;
    }
    for (int r = 0; r < AUD_REPLICAS; r++) {
        int rv = aud_model_resolve_pending(*tc, &models[r],
                                           r == 0 ? AUD_OBJ_A : AUD_OBJ_B,
                                           r, logf, "restart");
        if (rv < 0) {
            fprintf(logf, "seed %d: pending resolution failed (replica %d)\n",
                    seed, r);
            return -1;
        }
        *viol_out += rv > 0 ? rv : 0;
        rv = aud_model_resolve_all_pending(*tc, &models[r],
                                           r == 0 ? AUD_OBJ_A : AUD_OBJ_B,
                                           r, logf, "restart-multi");
        *viol_out += rv > 0 ? rv : 0;
        /* Full verification after EVERY restart, not only the final one:
           an acked write on some other key lost at this kill must be
           caught now, before a later acked bulk-insert/-delete of that
           key can overwrite the evidence. */
        *viol_out += aud_verify_model(*tc, &models[r],
                                      r == 0 ? AUD_OBJ_A : AUD_OBJ_B, r, 0,
                                      logf);
    }
    /* Per-restart count telemetry (model live count for replica A). */
    int cnt = -1;
    (void)aud_count_of(*tc, AUD_OBJ_A, &cnt);
    fprintf(logf, "seed %d: after restart: count=%d\n", seed, cnt);
    return 0;
}

/* One deterministic crafted gate seed: the incident shape, byte-exact. */
static int aud_crafted_seed(int seed, FILE *logf) {
    AudEnvCtx ec;
    memset(&ec, 0, sizeof(ec));
    ec.port = -1;
    aud_spawned_n = 0;
    snprintf(ec.base, sizeof(ec.base), "/tmp/shard-db-auditcr-%d-%d",
             (int)getpid(), seed);
    snprintf(ec.db_root, sizeof(ec.db_root), "%s/db", ec.base);
    aud_rmrf(ec.base);
    if (mkdir(ec.base, 0755) != 0 || mkdir(ec.db_root, 0755) != 0) {
        fprintf(logf, "seed %d: mkdir failed: %s\n", seed, strerror(errno));
        return 1;
    }
    ec.port = test_pick_port();
    if (ec.port < 0) {
        fprintf(logf, "seed %d: no port\n", seed);
        aud_rmrf(ec.base);
        return 1;
    }

    AudModel models[2];
    aud_model_init(&models[0]);
    aud_model_init(&models[1]);

    /* Solve for six keys whose xxh128 byte[15] is equal MODULO 16:
       stream routing is hash[15] % num_streams and num_streams is
       nproc-derived (4/8/16 — slotcask_streams_for_nproc caps at 16, not
       pinnable on the wire daemon), so a shared low nibble puts kA..kF
       in ONE stream at any of those counts. kA/kB become the crafted
       head graves (run length > slot_size window), kC..kF the live
       records beyond them — AND the append-pressure updates below
       target kF..kC, so MULTIPLE appends land in the mis-parked stream
       (the incident's "appends restarted at offset 0 and crushed the
       live records beyond": one append would only overwrite the graves,
       inertly). Solved pairwise against kA's class (~16 trials per
       key), deterministic. */
    char ks[6][16];
    {
        uint8_t h0[16], hx[16];
        snprintf(ks[0], sizeof(ks[0]), "gA%013u", 1u);
        compute_hash_raw(ks[0], strlen(ks[0]), h0);
        uint8_t cls = (uint8_t)(h0[15] & 0x0F);
        int all_found = 1;
        for (int j = 1; j < 6 && all_found; j++) {
            int ok = 0;
            for (uint32_t u = 0; u < 100000 && !ok; u++) {
                snprintf(ks[j], sizeof(ks[j]), "g%c%013u", 'A' + j, u);
                compute_hash_raw(ks[j], strlen(ks[j]), hx);
                if ((hx[15] & 0x0F) == cls) ok = 1;
            }
            if (!ok) all_found = 0;
        }
        if (!all_found) {
            fprintf(logf, "seed %d: stream-collision key solve failed\n",
                    seed);
            goto fail_env;
        }
        fprintf(logf, "seed %d: stream-colliding keys %s/%s/%s/%s/%s/%s "
                "(hash15 class %u)\n", seed, ks[0], ks[1], ks[2], ks[3],
                ks[4], ks[5], cls);
    }
    for (int i = 0; i < AUD_CRAFT_KEYS; i++) {
        const char *k = (i < 6) ? ks[i] : AUD_CRAFT_KEY[i];
        aud_model_add(&models[0], k);
        aud_model_add(&models[1], k);
    }
    AudOpLog log;
    aud_oplog_reset(&log);
    int resolve_viol = 0; /* strict-resolve violations across restarts */

    TestEnv env = {0};
    TestClientCfg cfg = { .port = ec.port, .io_timeout_ms = 30000 };
    TestClientCfg cfg_fast = { .port = ec.port, .io_timeout_ms = 500 };
    TestClient *tc = NULL, *tc_fast = NULL;

    if (test_env_start_at(&env, ec.db_root, ec.port) != 0) {
        fprintf(logf, "seed %d: spawn failed\n", seed);
        aud_rmrf(ec.base);
        return 1;
    }
    aud_track_spawn(env.daemon_pid);
    tc = tc_connect(&cfg);
    tc_fast = tc_connect(&cfg_fast);
    if (!tc || !tc_fast) {
        fprintf(logf, "seed %d: connect failed\n", seed);
        goto fail_env;
    }

    /* Setup: dir, twins, index on the btree twin, geometry describe. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_A
        "\",\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\",\"s:int\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_B
        "\",\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\",\"s:int\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_B
        "\",\"field\":\"v\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\""
        AUD_OBJ_A "\"}", &resp);
    ec.splits = aud_parse_int_field(resp, "splits");
    ec.slot_size = aud_parse_int_field(resp, "slot_size");
    free(resp); resp = NULL;
    {
        char data_dir[512];
        snprintf(data_dir, sizeof(data_dir), "%s/default/%s/data",
                 ec.db_root, AUD_OBJ_A);
        ec.streams = aud_count_stream_dirs(data_dir);
    }
    if (ec.splits <= 0 || ec.slot_size <= 0 || ec.streams <= 0) {
        fprintf(logf, "seed %d: geometry discover failed\n", seed);
        goto fail_env;
    }

    /* Insert gA..gH, then delete gA/gB — the two future graves. */
    for (int i = 0; i < AUD_CRAFT_KEYS; i++) {
        AudOp op;
        memset(&op, 0, sizeof(op));
        op.type = 0;
        op.idx = i;
        snprintf(op.val, sizeof(op.val), "%s", AUD_CRAFT_VAL[i]);
        if (aud_run_normal_op(tc, &op, models, &log) != 0) {
            fprintf(logf, "seed %d: connection failure during inserts\n",
                    seed);
            goto fail_env;
        }
    }
    for (int d = 0; d < 2; d++) {
        AudOp op;
        memset(&op, 0, sizeof(op));
        op.type = 2;
        op.idx = d;
        if (aud_run_normal_op(tc, &op, models, &log) != 0) {
            fprintf(logf, "seed %d: connection failure during deletes\n",
                    seed);
            goto fail_env;
        }
    }
    {
        int cnt = -1;
        (void)aud_count_of(tc, AUD_OBJ_A, &cnt);
        fprintf(logf, "seed %d: pre-craft count=%d (want 6)\n", seed, cnt);
    }

    /* Graceful stop so the on-disk state is settled, then craft the two
       head graves on both replicas (daemon down; the fix regression's
       pwrite mechanism). */
    if (tc) { tc_close(tc); tc = NULL; }
    if (tc_fast) { tc_close(tc_fast); tc_fast = NULL; }
    test_env_stop_keep(&env);
    int craft_bad = 0;
    craft_bad |= (aud_craft_grave(&ec, AUD_OBJ_A, models[0].keys[0], logf) != 1);
    craft_bad |= (aud_craft_grave(&ec, AUD_OBJ_A, models[0].keys[1], logf) != 1);
    craft_bad |= (aud_craft_grave(&ec, AUD_OBJ_B, models[0].keys[0], logf) != 1);
    craft_bad |= (aud_craft_grave(&ec, AUD_OBJ_B, models[0].keys[1], logf) != 1);
    if (craft_bad) {
        fprintf(logf, "seed %d: crafting failed (environment failure)\n",
                seed);
        goto fail_env;
    }

    /* Restart on the crafted root and sanity-read gC (both engines must
       serve the crafted root; the frontier defect only affects appends). */
    if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast, &cfg,
                    &cfg_fast, models, logf, seed, &resolve_viol) != 0)
        goto fail_env;

    /* Append pressure: deterministic updates on kF..kC — DESCENDING key
       order over the six same-stream keys. On base the mis-parked
       frontier makes these four appends walk the mis-parked stream
       positions (graves, then the still-referenced live records beyond)
       while the updates repoint keys in DESCENDING order, so the cascade
       crushes kC's record before kC itself is repointed. On the fixed
       build the appends land at the true tail and every key stays
       intact. */
    for (int i = 5; i >= 2; i--) {
        AudOp op;
        memset(&op, 0, sizeof(op));
        op.type = 1;
        op.idx = i;
        snprintf(op.val, sizeof(op.val), "u1_%c", 'A' + i);
        if (aud_run_normal_op(tc, &op, models, &log) != 0) {
            fprintf(logf, "seed %d: connection failure during append "
                    "pressure\n", seed);
            goto fail_env;
        }
    }

    /* Normal kill → restart → validate cycle (deterministic biased
       parked kill on the first live key). */
    int crafted_violations = 0;
    {
        int rc = aud_biased_kill(&env, tc, tc_fast, models, &log, NULL,
                                 seed, 0);
        if (rc != 0) {
            if (aud_inprocess_validate(&ec, AUD_OBJ_A, logf,
                                       "crafted-post-kill") != 0) {
                fprintf(logf, "seed %d: oracle (a) VIOLATION "
                        "(validate_live_refs nonzero on the crafted "
                        "root)\n", seed);
                crafted_violations++;
            }
            if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast,
                            &cfg, &cfg_fast, models, logf, seed, &resolve_viol) != 0)
                goto fail_env;
        }
    }

    /* Two more deterministic ops, then the final clean boot. */
    for (int i = 2; i < 4; i++) {
        AudOp op;
        memset(&op, 0, sizeof(op));
        op.type = 1;
        op.idx = i + 2;
        snprintf(op.val, sizeof(op.val), "u2_%c", 'A' + i);
        if (aud_run_normal_op(tc, &op, models, &log) != 0) {
            fprintf(logf, "seed %d: connection failure post-kill\n", seed);
            goto fail_env;
        }
    }
    if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast, &cfg,
                    &cfg_fast, models, logf, seed, &resolve_viol) != 0)
        goto fail_env;

    int v = aud_verify_model(tc, &models[0], AUD_OBJ_A, 0, 0, logf);
    v += aud_verify_model(tc, &models[1], AUD_OBJ_B, 1, 0, logf);
    v += crafted_violations + resolve_viol;

    tc_close(tc);
    tc_close(tc_fast);
    /* KEEP must be checked BEFORE test_env_stop: that fixture deletes the
       tree (rm -rf of the root), which would defeat the keep. */
    int keep = aud_env_int("SHARD_AUDIT_KEEP", 0);
    if (keep)
        test_env_stop_keep(&env);
    else
        test_env_stop(&env);
    aud_kill_spawned();
    if (keep)
        fprintf(logf, "seed %d: root kept at %s (SHARD_AUDIT_KEEP=1)\n",
                seed, ec.base);
    else
        aud_rmrf(ec.base);

    if (v > 0) {
        fprintf(logf, "seed %d SUMMARY: %d wire-oracle violation(s); "
                "op-log follows\n", seed, v);
        aud_oplog_dump(&log, logf);
        return 1;
    }
    TAP_DIAG("# crafted seed %d: ok (%d log entries)\n", seed, log.n);
    return 0;

fail_env:
    fprintf(logf, "seed %d: environment failure; op-log follows\n", seed);
    aud_oplog_dump(&log, logf);
    if (tc) tc_close(tc);
    if (tc_fast) tc_close(tc_fast);
    test_env_kill(&env);
    aud_kill_spawned();
    aud_rmrf(ec.base);
    return 1;
}

static int aud_run_seed(int seed, int n_ops, int biased_cycles, FILE *logf) {
    AudEnvCtx ec;
    memset(&ec, 0, sizeof(ec));
    ec.port = -1;
    aud_spawned_n = 0;
    snprintf(ec.base, sizeof(ec.base), "/tmp/shard-db-auditcr-%d-%d",
             (int)getpid(), seed);
    snprintf(ec.db_root, sizeof(ec.db_root), "%s/db", ec.base);
    aud_rmrf(ec.base);
    if (mkdir(ec.base, 0755) != 0 || mkdir(ec.db_root, 0755) != 0) {
        fprintf(logf, "seed %d: mkdir failed: %s\n", seed, strerror(errno));
        return 1;
    }
    ec.port = test_pick_port();
    if (ec.port < 0) {
        fprintf(logf, "seed %d: no port\n", seed);
        aud_rmrf(ec.base);
        return 1;
    }

    AudModel models[2];
    aud_model_init(&models[0]);
    aud_model_init(&models[1]);
    char key[16];
    for (int i = 0; i < AUD_HOT_KEYS; i++) {
        aud_make_key(key, sizeof(key), i, 0);
        if (aud_model_add(&models[0], key) < 0) break;
        aud_model_add(&models[1], key);
    }
    for (int i = 0; i < AUD_FRESH_KEYS; i++) {
        aud_make_key(key, sizeof(key), i, 1);
        if (aud_model_add(&models[0], key) < 0) break;
        aud_model_add(&models[1], key);
    }
    AudOpLog log;
    aud_oplog_reset(&log);
    uint64_t rng = (uint64_t)seed * 2685821657736338717ULL + 1ULL;
    int resolve_viol = 0; /* strict-resolve violations across restarts */

    TestEnv env = {0};
    TestClientCfg cfg = { .port = ec.port, .io_timeout_ms = 30000 };
    TestClientCfg cfg_fast = { .port = ec.port, .io_timeout_ms = 500 };
    TestClient *tc = NULL, *tc_fast = NULL;

    if (test_env_start_at(&env, ec.db_root, ec.port) != 0) {
        fprintf(logf, "seed %d: first daemon spawn failed\n", seed);
        aud_rmrf(ec.base);
        return 1;
    }
    aud_track_spawn(env.daemon_pid);
    tc = tc_connect(&cfg);
    tc_fast = tc_connect(&cfg_fast);
    if (!tc || !tc_fast) {
        fprintf(logf, "seed %d: connect failed\n", seed);
        goto fail_env;
    }

    /* Setup: dir, twins, index on the btree twin, geometry describe. */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_A
        "\",\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\",\"s:int\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_B
        "\",\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\",\"s:int\"]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"" AUD_OBJ_B
        "\",\"field\":\"v\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\""
        AUD_OBJ_A "\"}", &resp);
    ec.splits = aud_parse_int_field(resp, "splits");
    ec.slot_size = aud_parse_int_field(resp, "slot_size");
    free(resp); resp = NULL;
    {
        char data_dir[512];
        snprintf(data_dir, sizeof(data_dir), "%s/default/%s/data",
                 ec.db_root, AUD_OBJ_A);
        ec.streams = aud_count_stream_dirs(data_dir);
    }
    if (ec.splits <= 0 || ec.slot_size <= 0 || ec.streams <= 0) {
        fprintf(logf, "seed %d: geometry discover failed (splits=%d "
                "slot_size=%d streams=%d)\n", seed, ec.splits, ec.slot_size,
                ec.streams);
        goto fail_env;
    }

    /* Live baseline on both twins so every op class has targets. */
    for (int i = 0; i < AUD_BASELINE; i++) {
        aud_make_key(key, sizeof(key), i, 0);
        int idx = aud_model_idx(&models[0], key);
        AudOp op;
        memset(&op, 0, sizeof(op));
        op.type = 0;
        op.idx = idx;
        snprintf(op.val, sizeof(op.val), "seed_%d", i);
        if (aud_run_normal_op(tc, &op, models, &log) != 0)
            goto fail_env;
    }

    /* Op loop: up to `biased_cycles` biased kills CHAINED BACK-TO-BACK
       (no intervening ops — any successful append to the stream between
       kills would reset the consecutive grave run the bug needs), then a
       uniform kill at an rng point; every kill is followed by in-process
       validation, restart, pending resolution, and the sequence
       continues. */
    int bias_pts[4] = { n_ops / 5, (2 * n_ops) / 5, (3 * n_ops) / 5, (4 * n_ops) / 5 };
    int bias_done = 0;
    int uniform_at = (n_ops * 3) / 4 +
                     (int)aud_rng_below(&rng, (uint32_t)(n_ops / 5 + 1));
    int uniform_done = 0;
    int kills = 0;
    int in_proc_bad = 0;
    for (int i = 0; i < n_ops; i++) {
        if (bias_done < biased_cycles && bias_done < 4 &&
            i == bias_pts[bias_done]) {
            /* Chain: kill → validate → restart → kill again, with no ops
               in between, until the cycle budget is spent or an attempt
               misses the park. */
            while (bias_done < biased_cycles && bias_done < 4) {
                int rc = aud_biased_kill(&env, tc, tc_fast, models, &log,
                                         &rng, seed, bias_done);
                bias_done++;
                if (rc == 0) break; /* missed the park; resume ops */
                kills++;
                in_proc_bad |= aud_inprocess_validate(&ec, AUD_OBJ_A, logf,
                                                      "post-biased-kill");
                if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast,
                                &cfg, &cfg_fast, models, logf, seed, &resolve_viol) != 0)
                    goto fail_env;
            }
            continue;
        }
        if (!uniform_done && i == uniform_at) {
            aud_uniform_kill(&env, tc, models, &log, &rng, seed);
            uniform_done = 1;
            kills++;
            in_proc_bad |= aud_inprocess_validate(&ec, AUD_OBJ_A, logf,
                                                  "post-uniform-kill");
            if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast,
                            &cfg, &cfg_fast, models, logf, seed, &resolve_viol) != 0)
                goto fail_env;
            continue;
        }
        AudOp op;
        aud_build_op(&op, &rng, &models[0], seed, i);
        if (aud_run_normal_op(tc, &op, models, &log) != 0) {
            /* replica A's connection died mid-op without our kill: treat
               as an unexpected daemon exit and verify whatever state the
               crash left (the unacked op is recorded as pending, so either
               outcome is legal) */
            aud_oplog_add(&log, "UNEXPECTED replica-A connection failure");
            kills++;
            in_proc_bad |= aud_inprocess_validate(&ec, AUD_OBJ_A, logf,
                                                  "post-conn-failure");
            if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast,
                            &cfg, &cfg_fast, models, logf, seed, &resolve_viol) != 0)
                goto fail_env;
        }
    }
    if (kills == 0) {
        in_proc_bad |= aud_inprocess_validate(&ec, AUD_OBJ_A, logf,
                                              "end-no-kill");
    }

    /* Final clean boot (deterministic startup: KFM2 replay, bmfix,
       recovery) before the strict wire oracle on both twins. */
    if (aud_restart(&env, ec.db_root, ec.port, &tc, &tc_fast, &cfg,
                    &cfg_fast, models, logf, seed, &resolve_viol) != 0)
        goto fail_env;

    int v = aud_verify_model(tc, &models[0], AUD_OBJ_A, 0, 0, logf);
    v += aud_verify_model(tc, &models[1], AUD_OBJ_B, 1, 0, logf);
    v += resolve_viol;

    tc_close(tc);
    tc_close(tc_fast);
    test_env_stop(&env);
    aud_kill_spawned();
    aud_rmrf(ec.base);

    if (in_proc_bad || v > 0) {
        fprintf(logf, "seed %d SUMMARY: %d wire-oracle violation(s)%s; "
                "op-log follows\n", seed, v,
                in_proc_bad ? " + in-process violations" : "");
        aud_oplog_dump(&log, logf);
        return 1;
    }
    TAP_DIAG("# seed %d: ok (%d log entries, %d kills)\n", seed, log.n,
             kills);
    return 0;

fail_env:
    fprintf(logf, "seed %d: environment failure; op-log follows\n", seed);
    aud_oplog_dump(&log, logf);
    if (tc) tc_close(tc);
    if (tc_fast) tc_close(tc_fast);
    test_env_kill(&env);
    aud_kill_spawned();
    aud_rmrf(ec.base);
    return 1;
}

static int audit_crash_restart_run(void) {
    int seeds = aud_env_int("SHARD_AUDIT_SEEDS", 2);
    int n_ops = aud_env_int("SHARD_AUDIT_OPS", 30);
    int first = aud_env_int("SHARD_AUDIT_FIRST_SEED", 1);
    int biased_cycles = aud_env_int("SHARD_AUDIT_BIASED_CYCLES", 2);
    const char *mode = getenv("SHARD_AUDIT_MODE");
    int crafted = (mode && strcmp(mode, "crafted") == 0);
    if (n_ops > 50) n_ops = 50; /* plan budget cap per seed */
    if (seeds < 1) seeds = 1;
    if (biased_cycles < 0) biased_cycles = 0;

    mkdir("/tmp/shard-db-audit-logs", 0755);

    /* Per-run log directory: parallel or back-to-back runs never append
       into each other's seed logs. */
    char logdir[160];
    snprintf(logdir, sizeof(logdir), "/tmp/shard-db-audit-logs/crash-%ld-%d",
             (long)time(NULL), (int)getpid());
    mkdir(logdir, 0755);

    int failed_seeds = 0;
    for (int s = first; s < first + seeds; s++) {
        char logpath[256];
        snprintf(logpath, sizeof(logpath), "%s/seed%d.log", logdir, s);
        FILE *logf = fopen(logpath, "w");
        if (!logf) logf = stderr;
        int rc = crafted ? aud_crafted_seed(s, logf)
                         : aud_run_seed(s, n_ops, biased_cycles, logf);
        if (rc != 0) failed_seeds++;
        if (logf != stderr) fclose(logf);
    }
    aud_kill_spawned();
    char desc[256];
    snprintf(desc, sizeof(desc),
             "audit-crash-restart (%s mode): %d/%d seeds passed (failures "
             "logged under %s; replay with "
             "SHARD_AUDIT_FIRST_SEED=<n> SHARD_AUDIT_SEEDS=1)",
             crafted ? "crafted" : "kills", seeds - failed_seeds, seeds,
             logdir);
    ASSERT_EQ_INT(failed_seeds, 0, desc);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER_AUDIT("audit-crash-restart", audit_crash_restart_run)
