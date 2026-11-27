/* src/test/audit/audit_gate_resplit_bitmap.c
 *
 * Audit-campaign KNOWN-BUG GATE for the bitmap-resplit fix e69170b
 * ("fix: remap bitmap slots across keyfile resplit", 2026-11-06) —
 * planner-gate pair chosen by the human (plan Phase 1 step 4 decision
 * 2): RED on parent e69170b^, GREEN on e69170b, deterministically.
 *
 * Method: twin objects differing only in index shape (plain oracle vs
 * v:bitmap), the W1 kf slot seam (test_env_ctl_set_kf_seams, initial
 * 64 slots/shard) forces repeated in-place growth resplits during a
 * bulk load, then bitmap-driven v-eq count/find must equal the plain
 * twin's results. An `explain` assertion proves the bitmap path is
 * actually driven on the indexed twin (the gate must not pass because
 * the planner quietly avoided the bitmap). A clean restart re-checks
 * everything (the fix persists remapped bitmaps durably; the parent
 * leaves them stale on disk too). Deletions after the resplit verify
 * the bitmap tracks tombstones against the remapped layout.
 *
 * RED on e69170b^: resplit moves records to new kf slot indices while
 * bit i still means "kf slot i" — bitmap walks hash-verify-skip every
 * moved record and undercount deterministically.
 * GREEN on e69170b: the remap keeps bitmaps coherent.
 *
 * Only count/find/explain are used — no aggregate/group_by — so the
 * gate is not polluted by the audit-base findings AUD-QRY-1/2.
 *
 * Audit case: excluded from default run-all (TEST_REGISTER_AUDIT).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "audit_common.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define RB_ROWS   2000
#define RB_CHUNK  50
#define RB_CATS   4

typedef struct {
    char base[256], db_root[512];
    int port;
} RbCtx;

static void rb_rmrf(const char *dir) {
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    if (system(cmd) != 0) { /* best effort */ }
}

/* Sorted key set from a find response (same shape as audit_planner_diff). */
#define RB_MAX_KEYS (RB_ROWS + 16)
typedef struct {
    int n;
    char keys[RB_MAX_KEYS][16];
} RbKeySet;

static int rb_keycmp(const void *a, const void *b) {
    return strcmp((const char *)a, (const char *)b);
}

static int rb_collect_keys(const char *resp, RbKeySet *ks) {
    ks->n = 0;
    const char *p = resp;
    while (p && (p = strstr(p, "\"key\":\"")) != NULL) {
        p += 7;
        if (ks->n >= RB_MAX_KEYS) return -1;
        size_t n = 0;
        while (*p && *p != '"' && n + 1 < sizeof(ks->keys[0]))
            ks->keys[ks->n][n++] = *p++;
        ks->keys[ks->n][n] = '\0';
        ks->n++;
    }
    qsort(ks->keys, (size_t)ks->n, sizeof(ks->keys[0]), rb_keycmp);
    return 0;
}

static int rb_keyset_diff(const RbKeySet *a, const RbKeySet *b,
                          char *out, size_t cap) {
    if (a->n != b->n) {
        snprintf(out, cap, "size %d vs %d", a->n, b->n);
        return 1;
    }
    for (int i = 0; i < a->n; i++)
        if (strcmp(a->keys[i], b->keys[i]) != 0) {
            snprintf(out, cap, "first diff at %d: '%s' vs '%s'",
                     i, a->keys[i], b->keys[i]);
            return 1;
        }
    return 0;
}

/* One cross-shape equality sweep: per-cat v-eq count + full key sets on
   both twins, plus the bitmap-plan usage proof on the indexed twin. */
static void rb_verify(TestClient *tc, const RbCtx *fc, const char *when) {
    char req[1024], *resp = NULL;
    for (int cat = 0; cat < RB_CATS; cat++) {
        char crit[96];
        snprintf(crit, sizeof(crit),
                 "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_%d\"}]",
                 cat);
        int c_plain = -1, c_bm = -1;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":"
                 "\"rb_plain\",\"criteria\":%s}", crit);
        ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, when);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), when);
        c_plain = resp ? atoi(resp) : -1;
        free(resp); resp = NULL;

        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":"
                 "\"rb_bitmap\",\"criteria\":%s}", crit);
        ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, when);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), when);
        c_bm = resp ? atoi(resp) : -1;
        free(resp); resp = NULL;

        TAP_DIAG("# RB %s cat_%d: plain=%d bitmap=%d\n", when, cat,
                 c_plain, c_bm);
        ASSERT_TRUE(c_plain >= 0 && c_plain == c_bm, when);
    }

    /* full key sets */
    RbKeySet kp, kb;
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":"
             "\"rb_plain\",\"criteria\":[{\"field\":\"v\",\"op\":"
             "\"starts_with\",\"value\":\"cat_\"}],\"offset\":0,"
             "\"limit\":%d}", RB_ROWS + 16);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, when);
    ASSERT_TRUE(resp && rb_collect_keys(resp, &kp) == 0, when);
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":"
             "\"rb_bitmap\",\"criteria\":[{\"field\":\"v\",\"op\":"
             "\"starts_with\",\"value\":\"cat_\"}],\"offset\":0,"
             "\"limit\":%d}", RB_ROWS + 16);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, when);
    ASSERT_TRUE(resp && rb_collect_keys(resp, &kb) == 0, when);
    free(resp); resp = NULL;
    char why[128];
    ASSERT_TRUE(!rb_keyset_diff(&kp, &kb, why, sizeof(why)), when);

    /* bitmap-plan usage proof: the indexed twin's v-eq find must be
       seeded by the bitmap index (a scan-driven pass would make the
       equality check vacuous). */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":"
             "\"rb_bitmap\",\"criteria\":[{\"field\":\"v\",\"op\":"
             "\"eq\",\"value\":\"cat_1\"}],\"explain\":true}");
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, when);
    ASSERT_TRUE(resp && strstr(resp, "\"index\":\"bitmap\""), when);
    free(resp); resp = NULL;

    (void)fc;
}

static void rb_load(TestClient *tc, const char *obj, const char *when) {
    char bulkreq[16384], *resp = NULL;
    for (int base_i = 0; base_i < RB_ROWS; base_i += RB_CHUNK) {
        int off = snprintf(bulkreq, sizeof(bulkreq),
                           "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                           "\"object\":\"%s\",\"records\":[", obj);
        for (int j = 0; j < RB_CHUNK; j++) {
            int i = base_i + j;
            off += snprintf(bulkreq + off, sizeof(bulkreq) - (size_t)off,
                            "%s{\"key\":\"k%05d\",\"value\":{\"v\":"
                            "\"cat_%d\",\"t\":%d}}", j ? "," : "", i,
                            i % RB_CATS, i);
        }
        off += snprintf(bulkreq + off, sizeof(bulkreq) - (size_t)off, "]}");
        ASSERT_EQ_INT(tc_request(tc, bulkreq, &resp), 0, when);
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), when);
        free(resp); resp = NULL;
    }
}

/* Prove the resplit actually happened: with the seam at 64 initial
   slots, 250 records/shard forces in-place doublings; the kf shard file
   on disk must have outgrown the 64-slot initial image by a wide
   factor (each slot entry is 24B header + key bytes). */
static void rb_assert_resplit(const RbCtx *fc, const char *when) {
    char path[512];
    snprintf(path, sizeof(path),
             "%s/default/rb_bitmap/data/kf/000.kf", fc->db_root);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, when);
    TAP_DIAG("# RB kf/000.kf size=%lld (64-slot image is ~2584)\n",
             (long long)st.st_size);
    ASSERT_TRUE((unsigned long long)st.st_size >= 10000ULL, when);
}

static int audit_gate_resplit_bitmap_run(void) {
    RbCtx fc;
    memset(&fc, 0, sizeof(fc));
    snprintf(fc.base, sizeof(fc.base), "/tmp/shard-db-audit-rb-%d",
             (int)getpid());
    snprintf(fc.db_root, sizeof(fc.db_root), "%s/db", fc.base);
    rb_rmrf(fc.base);
    if (mkdir(fc.base, 0755) != 0 || mkdir(fc.db_root, 0755) != 0) {
        ASSERT_TRUE(0, "audit-gate-resplit-bitmap: mkdir");
        return 1;
    }
    fc.port = test_pick_port();
    if (fc.port < 0) {
        rb_rmrf(fc.base);
        ASSERT_TRUE(0, "audit-gate-resplit-bitmap: pick port");
        return 1;
    }

    TestEnv env = {0};
    env.test_control_fd = -1;
    if (test_env_start_at(&env, fc.db_root, fc.port) != 0) {
        rb_rmrf(fc.base);
        ASSERT_TRUE(0, "audit-gate-resplit-bitmap: daemon spawn");
        return 1;
    }
    aud_track_spawn(env.daemon_pid);

    /* Force small initial kf tables so the load crosses the 75% growth
       threshold repeatedly (in-place doubling = the resplit the fix
       remaps bitmaps across). b=0 leaves the growth cap disarmed.
       KF_TEST_INITIAL_SLOTS in db.env persists the tier across the
       mid-case restart (the control channel only reaches a running
       daemon; the startup parser re-reads the env file). */
    {
        char envpath[320];
        snprintf(envpath, sizeof(envpath), "%s/db.env", fc.base);
        FILE *ef = fopen(envpath, "a");
        ASSERT_TRUE(ef != NULL, "append KF_TEST_INITIAL_SLOTS to db.env");
        if (ef) {
            fprintf(ef, "export KF_TEST_INITIAL_SLOTS=64\n");
            fclose(ef);
        }
    }
    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&env, 64, 0), 0,
                  "arm W1 kf slot seam");

    TestClientCfg cfg = { .port = fc.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        test_env_kill(&env);
        rb_rmrf(fc.base);
        ASSERT_TRUE(0, "audit-gate-resplit-bitmap: connect");
        return 1;
    }

    char req[1024], *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    for (int s = 0; s < 2; s++) {
        const char *obj = s ? "rb_bitmap" : "rb_plain";
        if (s)
            snprintf(req, sizeof(req),
                     "{\"mode\":\"create-object\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
                     "\"fields\":[\"v:varchar:32\",\"t:long\"],"
                     "\"indexes\":[\"v:bitmap\"]}", obj);
        else
            snprintf(req, sizeof(req),
                     "{\"mode\":\"create-object\",\"dir\":\"default\","
                     "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
                     "\"fields\":[\"v:varchar:32\",\"t:long\"]}", obj);
        ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "create twins");
        ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "create twins");
        free(resp); resp = NULL;
    }

    rb_load(tc, "rb_plain", "load plain twin");
    rb_load(tc, "rb_bitmap", "load bitmap twin (resplit under bitmap)");
    rb_assert_resplit(&fc, "resplit happened under the seam");

    rb_verify(tc, &fc, "post-resplit bitmap/plaint equality");

    /* Deletions after the resplit: the bitmap must track tombstones on
       the remapped layout. Delete every 5th key (400 keys, 100 per
       cat — stride 4 would hit exactly and only the cat_0 keys). */
    {
        /* bulk-delete takes a plain "keys" array (docs/query-protocol/bulk.md) */
        char bulkdel[8192];
        for (int s = 0; s < 2; s++) {
            int off = snprintf(bulkdel, sizeof(bulkdel),
                               "{\"mode\":\"bulk-delete\",\"dir\":"
                               "\"default\",\"object\":\"%s\",\"keys\":[",
                               s ? "rb_bitmap" : "rb_plain");
            for (int i = 0; i < RB_ROWS; i += 5) {
                off += snprintf(bulkdel + off, sizeof(bulkdel) - (size_t)off,
                                "%s\"k%05d\"", (i % 16) ? "," : "", i);
            }
            off += snprintf(bulkdel + off, sizeof(bulkdel) - (size_t)off, "]}");
            ASSERT_EQ_INT(tc_request(tc, bulkdel, &resp), 0, "bulk-delete");
            ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "bulk-delete");
            free(resp); resp = NULL;
        }
    }
    rb_verify(tc, &fc, "post-delete bitmap/plain equality");

    /* Clean restart: the fix persists remapped bitmaps durably; the
       parent leaves stale bitmaps on disk. */
    tc_close(tc);
    /* stop_keep: test_env_stop deletes the scratch tree; the restart
       must reopen the SAME root. */
    test_env_stop_keep(&env);
    if (test_env_start_at(&env, fc.db_root, fc.port) != 0) {
        rb_rmrf(fc.base);
        ASSERT_TRUE(0, "audit-gate-resplit-bitmap: restart");
        return 1;
    }
    aud_track_spawn(env.daemon_pid);
    tc = tc_connect(&cfg);
    ASSERT_TRUE(tc != NULL, "restart connect");
    if (tc) {
        rb_verify(tc, &fc, "post-restart bitmap/plain equality");
        tc_close(tc);
    }

    test_env_stop(&env);
    rb_rmrf(fc.base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER_AUDIT("audit-gate-resplit-bitmap", audit_gate_resplit_bitmap_run)
