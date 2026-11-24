/* src/test/audit/audit_common.h
 *
 * Shared building blocks for the audit-campaign oracle harnesses
 * (docs/plans/2026-11-19-audit-campaign.md Phase 1 step 4): a seeded
 * xorshift RNG so every seed is replayable, a small write-model that
 * tracks acknowledged vs pending (unacknowledged) mutations per replica
 * object, and wire helpers that speak the same JSON modes the product
 * clients use.
 *
 * Oracle contract (plan Phase 1, crash/restart fuzzer):
 *   - every ACKNOWLEDGED write must be visible after the crash;
 *   - an unacknowledged write may be fully applied or fully absent
 *     (forward replay) — never half-visible;
 *   - no key may show a value that was never written (torn/clobbered);
 *   - no acknowledged-deleted key may reappear (resurrection);
 *   - count must equal the model exactly (no phantoms, no omissions).
 *
 * Header-only (static inline) so each audit case is a single TU and the
 * build.sh wiring stays one line per case.
 */
#ifndef AUDIT_COMMON_H
#define AUDIT_COMMON_H

#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>

/* ---- Seeded RNG (xorshift64*): seed alone determines the op sequence ---- */

static inline uint64_t aud_rng_next(uint64_t *st) {
    uint64_t x = *st;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *st = x;
    return x * 2685821657736338717ULL;
}

/* Uniform in [0, n). n==0 yields 0. */
static inline uint32_t aud_rng_below(uint64_t *st, uint32_t n) {
    return n ? (uint32_t)(aud_rng_next(st) % n) : 0u;
}

static inline int aud_env_int(const char *name, int dflt) {
    const char *v = getenv(name);
    return (v && *v) ? atoi(v) : dflt;
}

/* ---- Spawned-daemon registry: teardown SIGKILLs survivors so a failed
   restart can never leak a daemon holding a scratch root. ---- */

#define AUD_MAX_SPAWNED 32
static pid_t aud_spawned[AUD_MAX_SPAWNED];
static int aud_spawned_n = 0;

static inline void aud_track_spawn(pid_t pid) {
    if (pid <= 0) return;
    if (aud_spawned_n >= AUD_MAX_SPAWNED) {
        /* Silent overflow would leak a live daemon holding a scratch
           root past teardown — fail the run loudly instead. */
        fprintf(stderr,
                "FATAL: aud_track_spawn overflow (AUD_MAX_SPAWNED=%d): "
                "teardown would leak pid %d\n", AUD_MAX_SPAWNED, (int)pid);
        abort();
    }
    aud_spawned[aud_spawned_n++] = pid;
}

static inline void aud_kill_spawned(void) {
    for (int i = 0; i < aud_spawned_n; i++) {
        /* Probe with WNOHANG, not kill(pid, 0): a pid already reaped by
           test_env_stop would make kill() hit whatever process the OS
           recycled the number onto. WNOHANG>0 = reaped here; ECHILD =
           reaped elsewhere — either way, never signal it. */
        if (waitpid(aud_spawned[i], NULL, WNOHANG) != 0) continue;
        kill(aud_spawned[i], SIGKILL);
        waitpid(aud_spawned[i], NULL, 0);
    }
    aud_spawned_n = 0;
}

/* ---- Wire helpers ---- */

/* Uniform ack rule: complete exchange, no error object, and no per-key
   bulk rejections ("errors":N with N > 0 — that field does not contain the
   quoted word "error", so it is checked separately). Anything borderline
   reads as "not acked": the op becomes pending, which only widens the
   legal set at verification time — the tolerant direction. */
static inline int aud_acked(const char *resp) {
    if (!resp || strstr(resp, "\"error\"")) return 0;
    const char *e = strstr(resp, "\"errors\":");
    if (e && atoi(e + strlen("\"errors\":")) > 0) return 0;
    return 1;
}

/* Extract the "v":"..." field from a get response into out. Values in the
   audit harnesses are plain [A-Za-z0-9_] so no JSON unescaping is needed.
   Returns 0 on success, -1 when absent/malformed. */
static inline int aud_extract_v(const char *resp, char *out, size_t cap) {
    const char *p = resp ? strstr(resp, "\"v\":\"") : NULL;
    if (!p) return -1;
    p += 5;
    size_t n = 0;
    while (*p && *p != '"' && n + 1 < cap) out[n++] = *p++;
    out[n] = '\0';
    return (*p == '"') ? 0 : -1;
}

/* ---- Write model ---- */

#define AUD_MAX_KEYS 512
#define AUD_VAL_MAX 96
#define AUD_REPLICAS 2 /* twin objects sharing one logical model */

/* An op that was in flight (unacknowledged) when its replica's daemon
   connection died. Sequential traffic means at most one per replica. */
typedef struct AudPending {
    int key_idx;                 /* -1 = none */
    unsigned char is_delete;
    char val[AUD_VAL_MAX];
} AudPending;

#define AUD_PENDS_MAX 64

typedef struct AudModel {
    int nkeys;
    char keys[AUD_MAX_KEYS][16];
    char vals[AUD_MAX_KEYS][AUD_VAL_MAX]; /* last ACKED value; valid iff live */
    unsigned char live[AUD_MAX_KEYS];     /* 1 = live, 0 = deleted/never */
    int live_count;
    AudPending pend[AUD_REPLICAS];       /* one in-flight slot per twin
                                            (crash/restart harness) */
    /* Multi-pending SET (fault-injection oracle): every non-acked op —
       an error or timeout does not tell us whether the write landed,
       because an erroring daemon may still have made it durable
       (error-but-durable forward replay). Unresolved pendings make the
       observed count inexact, so resolve the set before the strict
       verify. */
    int npend[AUD_REPLICAS];
    AudPending pends[AUD_REPLICAS][AUD_PENDS_MAX];
} AudModel;

static inline void aud_model_init(AudModel *m) {
    memset(m, 0, sizeof(*m));
    for (int r = 0; r < AUD_REPLICAS; r++) m->pend[r].key_idx = -1;
}

static inline int aud_model_idx(const AudModel *m, const char *key) {
    for (int i = 0; i < m->nkeys; i++)
        if (strcmp(m->keys[i], key) == 0) return i;
    return -1;
}

static inline int aud_model_add(AudModel *m, const char *key) {
    if (m->nkeys >= AUD_MAX_KEYS) return -1;
    snprintf(m->keys[m->nkeys], sizeof(m->keys[0]), "%s", key);
    return m->nkeys++;
}

/* Apply an ACKNOWLEDGED mutation to the shared logical model. */
static inline void aud_model_ack_insert(AudModel *m, int idx, const char *val) {
    if (!m->live[idx]) { m->live[idx] = 1; m->live_count++; }
    snprintf(m->vals[idx], sizeof(m->vals[0]), "%s", val);
}

static inline void aud_model_ack_delete(AudModel *m, int idx) {
    if (m->live[idx]) { m->live[idx] = 0; m->live_count--; }
    m->vals[idx][0] = '\0';
}

/* Mark replica `r` as having an op in flight: insert/update of `val`
   (val == NULL → delete). Only legal when that replica has no earlier
   pending op — sequential traffic per connection guarantees it. */
static inline void aud_model_set_pending(AudModel *m, int replica, int idx,
                                         const char *val) {
    m->pend[replica].key_idx = idx;
    m->pend[replica].is_delete = (val == NULL);
    if (val)
        snprintf(m->pend[replica].val, sizeof(m->pend[replica].val), "%s",
                 val);
}

/* Add one op to the replica's multi-pending set (val == NULL -> delete). */
static inline int aud_model_add_pending(AudModel *m, int replica, int idx,
                                        const char *val) {
    if (m->npend[replica] >= AUD_PENDS_MAX) return -1;
    AudPending *pd = &m->pends[replica][m->npend[replica]++];
    pd->key_idx = idx;
    pd->is_delete = (val == NULL);
    if (val)
        snprintf(pd->val, sizeof(pd->val), "%s", val);
    return 0;
}

static inline void aud_model_clear_pending(AudModel *m, int replica) {
    m->pend[replica].key_idx = -1;
    m->pend[replica].is_delete = 0;
    m->pend[replica].val[0] = '\0';
}

/* An ACKED op on key `idx` supersedes every earlier in-flight op on it.
   Wire traffic is sequential per connection, so the server applied the
   earlier pending op — if it landed at all — BEFORE the op that just
   acked: the acked op's effect is the key's final state, and the stale
   pendings must not survive as "legal outcomes" for it. Without this,
   an acked write that is later LOST hides behind a stale delete pending:
   resolve would adopt "deleted" and the strict verify would pass.
   Called at every ack site in the harnesses. */
static inline void aud_model_supersede(AudModel *m, int replica, int idx) {
    if (idx < 0) return;
    if (m->pend[replica].key_idx == idx) aud_model_clear_pending(m, replica);
    int w = 0;
    for (int j = 0; j < m->npend[replica]; j++) {
        if (m->pends[replica][j].key_idx == idx) continue;
        if (w != j) m->pends[replica][w] = m->pends[replica][j];
        w++;
    }
    m->npend[replica] = w;
}

/* ---- Strict pending resolution (never adopt an arbitrary observed
 *      state) ----
 *
 * Every pending on a key was issued after that key's last ACKED op (acks
 * supersede — see aud_model_supersede), so the model's current state for
 * the key is exactly what each pending raced from. The legal observed
 * outcomes are therefore closed sets, and the resolver only folds a
 * legal APPLIED outcome into the model; anything else is a VIOLATION:
 *
 *   observed VALUE: legal iff it equals the acked value (live model —
 *     the pending did not land) or equals one of the key's pending
 *     insert values (that write landed). A third value is
 *     torn/clobbered — never adopt it.
 *   observed ABSENT: legal iff the model is dead/never-written (nothing
 *     landed) or a delete is pending for the key (the delete landed —
 *     fold dead). Absent under a live model with no pending delete is a
 *     LOST ACKED WRITE — never adopt "deleted".
 */
typedef enum {
    AUD_OBS_NOT_APPLIED = 0, /* nothing landed; model stands */
    AUD_OBS_APPLIED_INS = 1, /* a pending insert landed: fold *fold_val */
    AUD_OBS_APPLIED_DEL = 2, /* a pending delete landed: fold dead */
    AUD_OBS_VIOLATION = 3    /* third value / lost acked write */
} AudObsClass;

/* Classify one observed get outcome. `ps`/`nps` is the pending list to
   consult (the multi set, or the single slot as a 1/0-length array);
   all entries must target key `idx`. */
static inline AudObsClass aud_classify_observed(int idx, int live,
                                                const char *acked_val,
                                                const AudPending *ps, int nps,
                                                int present, const char *got,
                                                char *fold_val) {
    int any_del = 0;
    for (int j = 0; j < nps; j++) {
        if (ps[j].key_idx != idx) continue;
        if (ps[j].is_delete) { any_del = 1; continue; }
    }
    if (present) {
        if (live && acked_val && strcmp(got, acked_val) == 0)
            return AUD_OBS_NOT_APPLIED;
        for (int j = 0; j < nps; j++) {
            if (ps[j].key_idx != idx || ps[j].is_delete) continue;
            if (strcmp(got, ps[j].val) == 0) {
                if (fold_val) snprintf(fold_val, AUD_VAL_MAX, "%s", ps[j].val);
                return AUD_OBS_APPLIED_INS;
            }
        }
        return AUD_OBS_VIOLATION;
    }
    /* observed absent */
    if (!live) return AUD_OBS_NOT_APPLIED;
    if (any_del) return AUD_OBS_APPLIED_DEL;
    return AUD_OBS_VIOLATION;
}

/* Mid-run pending resolution for one replica: after a restart we CONTINUE
   issuing ops, so the model must re-sync to the replayed durable state.
   STRICT: the observed state is classified against the acked value and
   the pending op (both outcomes — landed or not — are legal; a third
   value, or absence under a live model with no pending delete, is a
   violation and is never folded). Returns 0 when clean, the violation
   count (>0, logged to `logf` with `ctx`), or -1 if the get itself
   failed (caller treats the replica as unreadable). */
static inline int aud_model_resolve_pending(TestClient *tc, AudModel *m,
                                            const char *obj, int replica,
                                            FILE *logf, const char *ctx) {
    if (m->pend[replica].key_idx < 0) return 0;
    int idx = m->pend[replica].key_idx;
    char req[256], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\"}", obj, m->keys[idx]);
    int rc = tc_request(tc, req, &resp);
    if (rc != 0 || !resp) { free(resp); return -1; }
    int present = !strstr(resp, "\"error\"") && !strstr(resp, "Not found");
    char got[AUD_VAL_MAX] = "";
    int have_val = present && aud_extract_v(resp, got, sizeof(got)) == 0;
    int violations = 0;
    char fold_val[AUD_VAL_MAX] = "";
    AudObsClass cls;
    if (present && !have_val) {
        /* response-shape anomaly: not classifiable */
        fprintf(logf,
                "VIOLATION [%s] %s: key %s present but response has no v "
                "field\n", obj, ctx, m->keys[idx]);
        violations = 1;
    } else {
        cls = aud_classify_observed(idx, m->live[idx], m->vals[idx],
                                    &m->pend[replica],
                                    1, present, got, fold_val);
        switch (cls) {
        case AUD_OBS_APPLIED_INS:
            aud_model_ack_insert(m, idx, fold_val);
            break;
        case AUD_OBS_APPLIED_DEL:
            aud_model_ack_delete(m, idx);
            break;
        case AUD_OBS_VIOLATION:
            fprintf(logf,
                    "VIOLATION [%s] %s: key %s observed %s'%s' is neither "
                    "acked '%s' nor pending '%s' — not adopted\n",
                    obj, ctx, m->keys[idx],
                    present ? "" : "ABSENT (acked-live lost) ",
                    present ? got : "", m->vals[idx],
                    m->pend[replica].is_delete ? "(delete)"
                                               : m->pend[replica].val);
            violations = 1;
            break;
        default:
            break; /* NOT_APPLIED: model stands */
        }
    }
    aud_model_clear_pending(m, replica);
    free(resp);
    return violations;
}

/* Resolve the whole multi-pending set STRICTLY (same doctrine as
   aud_model_resolve_pending): each key's pendings were all issued after
   its last acked op, so the observed value must be the acked value or
   one of the key's pending insert values, and absence must mean
   acked-dead or a landed pending delete — a third value (error-but-
   clobbered) or a lost acked write is a VIOLATION and is never adopted
   into the model. Keys are resolved in issue order; all pendings on a
   key are consulted together (two failed ops on one key make both their
   values legal). Returns 0 when clean, the violation count (>0, logged),
   or -1 if any get failed (caller treats the replica as unverifyable). */
static inline int aud_model_resolve_all_pending(TestClient *tc, AudModel *m,
                                                const char *obj,
                                                int replica, FILE *logf,
                                                const char *ctx) {
    int n = m->npend[replica];
    unsigned char done[AUD_PENDS_MAX] = {0};
    int violations = 0;
    for (int j = 0; j < n; j++) {
        if (done[j]) continue;
        int idx = m->pends[replica][j].key_idx;
        char req[256], *resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"key\":\"%s\"}", obj, m->keys[idx]);
        int rc = tc_request(tc, req, &resp);
        if (rc != 0 || !resp) { free(resp); return -1; }
        int present = !strstr(resp, "\"error\"") && !strstr(resp, "Not found");
        char got[AUD_VAL_MAX] = "";
        int have_val = present && aud_extract_v(resp, got, sizeof(got)) == 0;
        /* mark every pending on this key consulted (in any order) */
        for (int k = j; k < n; k++)
            if (m->pends[replica][k].key_idx == idx) done[k] = 1;
        if (present && !have_val) {
            fprintf(logf,
                    "VIOLATION [%s] %s: key %s present but response has no "
                    "v field\n", obj, ctx, m->keys[idx]);
            violations++;
        } else {
            char fold_val[AUD_VAL_MAX] = "";
            AudObsClass cls = aud_classify_observed(
                idx, m->live[idx], m->vals[idx], m->pends[replica], n,
                present, got, fold_val);
            switch (cls) {
            case AUD_OBS_APPLIED_INS:
                aud_model_ack_insert(m, idx, fold_val);
                break;
            case AUD_OBS_APPLIED_DEL:
                aud_model_ack_delete(m, idx);
                break;
            case AUD_OBS_VIOLATION:
                fprintf(logf,
                        "VIOLATION [%s] %s: key %s observed %s'%s' matches "
                        "neither acked '%s' nor any pending insert/delete — "
                        "not adopted\n",
                        obj, ctx, m->keys[idx],
                        present ? "" : "ABSENT (acked-live lost) ",
                        present ? got : "", m->vals[idx]);
                violations++;
                break;
            default:
                break; /* NOT_APPLIED: model stands */
            }
        }
        free(resp);
    }
    m->npend[replica] = 0;
    return violations;
}

static inline int aud_count_of(TestClient *tc, const char *obj, int *out) {
    char req[256], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    int rc = tc_request(tc, req, &resp);
    int val = (rc == 0 && resp) ? atoi(resp) : -1;
    free(resp);
    *out = val;
    return (rc == 0 && val >= 0) ? 0 : -1;
}

/* The strict final oracle for one replica. `extra_live` counts records
   written through the raw slotcask probe path (not in the model).
   Returns violations found; writes details to `logf`. */
static inline int aud_verify_model(TestClient *tc, const AudModel *m,
                                   const char *obj, int replica,
                                   int extra_live, FILE *logf) {
    int violations = 0;
    const AudPending *pd = &m->pend[replica];
    const AudPending *ps = m->pends[replica];
    const int nps = m->npend[replica];   /* multi-pending set (fault oracle;
                                            resolved before strict verify —
                                            these scans are belt-and-braces) */
    int count = -1;
    if (aud_count_of(tc, obj, &count) != 0) {
        fprintf(logf, "VIOLATION [%s]: count request failed\n", obj);
        return 1;
    }
    if (count != m->live_count + extra_live) {
        fprintf(logf,
                "VIOLATION [%s]: count %d != model live %d + probes %d "
                "(phantom or lost records)\n",
                obj, count, m->live_count, extra_live);
        violations++;
    }
    for (int i = 0; i < m->nkeys; i++) {
        char req[256], *resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
                 "\"key\":\"%s\"}", obj, m->keys[i]);
        int rc = tc_request(tc, req, &resp);
        int have = (rc == 0 && resp && !strstr(resp, "\"error\"") &&
                    !strstr(resp, "Not found"));
        int pend_here = (pd->key_idx == i);
        int set_pend_ins = 0, set_pend_del = 0;
        for (int j = 0; j < nps; j++) {
            if (ps[j].key_idx != i) continue;
            if (ps[j].is_delete) set_pend_del = 1;
            else set_pend_ins = 1;
        }
        if (m->live[i]) {
            if (!have) {
                if (!((pend_here && pd->is_delete) || set_pend_del)) {
                    fprintf(logf,
                            "VIOLATION [%s]: key %s acked-live is missing "
                            "(lost write)\n", obj, m->keys[i]);
                    violations++;
                }
            } else {
                char got[AUD_VAL_MAX];
                if (aud_extract_v(resp, got, sizeof(got)) != 0) {
                    fprintf(logf,
                            "VIOLATION [%s]: key %s response has no v field\n",
                            obj, m->keys[i]);
                    violations++;
                } else {
                    int set_val_ok = 0;
                    for (int j = 0; j < nps; j++)
                        if (ps[j].key_idx == i && !ps[j].is_delete &&
                            strcmp(got, ps[j].val) == 0)
                            set_val_ok = 1;
                    if (!(strcmp(got, m->vals[i]) == 0 ||
                          (pend_here && !pd->is_delete &&
                           strcmp(got, pd->val) == 0) ||
                          set_val_ok)) {
                        fprintf(logf,
                                "VIOLATION [%s]: key %s value '%s' is neither "
                                "acked '%s' nor pending '%s' (torn/clobbered)\n",
                                obj, m->keys[i], got, m->vals[i], pd->val);
                        violations++;
                    }
                }
            }
        } else {
            /* dead key: may exist ONLY as a pending insert/update target */
            if (have && !((pend_here && !pd->is_delete) || set_pend_ins)) {
                fprintf(logf,
                        "VIOLATION [%s]: key %s is dead in the model but "
                        "reads back (resurrection/phantom)\n",
                        obj, m->keys[i]);
                violations++;
            }
        }
        free(resp);
    }
    return violations;
}

/* ---- Op-log (recorded for replay; dumped on failure) ---- */

#define AUD_LOG_MAX 512

typedef struct AudOpLog {
    int n;
    char desc[AUD_LOG_MAX][160];
    unsigned char acked[AUD_LOG_MAX];
} AudOpLog;

static inline void aud_oplog_reset(AudOpLog *l) { l->n = 0; }

static inline void aud_oplog_add(AudOpLog *l, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static inline void aud_oplog_add(AudOpLog *l, const char *fmt, ...) {
    if (l->n >= AUD_LOG_MAX) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(l->desc[l->n], sizeof(l->desc[0]), fmt, ap);
    va_end(ap);
    l->acked[l->n] = 0;
    l->n++;
}

static inline void aud_oplog_mark_acked(AudOpLog *l) {
    if (l->n > 0) l->acked[l->n - 1] = 1;
}

static inline void aud_oplog_dump(const AudOpLog *l, FILE *f) {
    for (int i = 0; i < l->n; i++)
        fprintf(f, "%3d [%c] %s\n", i, l->acked[i] ? 'A' : 'u', l->desc[i]);
}

#endif /* AUDIT_COMMON_H */
