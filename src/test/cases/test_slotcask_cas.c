/* test_slotcask_cas.c — Phase-2B unit test for CAS-aware slotcask APIs.
 *
 * Standalone (no daemon). Exercises slotcask_upsert_with_hooks +
 * slotcask_delete_with_hooks + slotcask_exists.
 *
 * Coverage:
 *   - upsert without hooks: insert-then-update flow, was_update reported
 *   - if_not_exists rejects existing key, returns current_value
 *   - require_existing rejects missing key
 *   - check_fn invoked with correct old payload (NULL on insert, bytes on update)
 *   - check_fn returning 0 → condition_not_met + current_value populated
 *   - pre_commit invoked with both old AND new bytes between data write and kf commit
 *   - pre_commit returning non-zero aborts (data written but rolled back to free pool)
 *   - delete with criteria match / mismatch
 *   - exists returns 1/0 correctly
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

static void rm_rf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd); (void)rc;
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_slotcask_cas_%d_%ld",
             base, (int)getpid(), (long)time(NULL));
}

/* ----- callback shims ------------------------------------------------- */

typedef struct {
    int      called;
    int      saw_old;
    int      old_match_expected;
    const char *expected_old;
    size_t      expected_old_len;
} CheckCtx;

static int check_always_proceed(const SlotcaskOldRecord *old, void *ctx) {
    CheckCtx *c = (CheckCtx *)ctx;
    c->called++;
    c->saw_old = (old != NULL);
    if (old && c->expected_old) {
        c->old_match_expected = (old->vlen == c->expected_old_len &&
                                  memcmp(old->value, c->expected_old, old->vlen) == 0);
    }
    return 1;
}
static int check_always_reject(const SlotcaskOldRecord *old, void *ctx) {
    CheckCtx *c = (CheckCtx *)ctx;
    c->called++;
    c->saw_old = (old != NULL);
    return 0;
}

typedef struct {
    int      called;
    int      saw_old;
    int      old_match_expected;
    int      new_match_expected;
    int      is_update_seen;
    const char *expected_old;
    size_t      expected_old_len;
    const char *expected_new;
    size_t      expected_new_len;
    int      return_value;   /* 0 = commit, non-zero = abort */
} PreCommitCtx;

static int pre_commit_recording(const SlotcaskOldRecord *old,
                                 const uint8_t *new_value, size_t new_vlen,
                                 int is_update, void *ctx) {
    PreCommitCtx *p = (PreCommitCtx *)ctx;
    p->called++;
    p->saw_old = (old != NULL);
    p->is_update_seen = is_update;
    if (old && p->expected_old) {
        p->old_match_expected = (old->vlen == p->expected_old_len &&
                                  memcmp(old->value, p->expected_old, old->vlen) == 0);
    }
    if (p->expected_new) {
        p->new_match_expected = (new_vlen == p->expected_new_len &&
                                  memcmp(new_value, p->expected_new, new_vlen) == 0);
    }
    return p->return_value;
}

static int delete_pre_commit_recording(const SlotcaskOldRecord *old, void *ctx) {
    PreCommitCtx *p = (PreCommitCtx *)ctx;
    p->called++;
    p->saw_old = (old != NULL);
    if (old && p->expected_old) {
        p->old_match_expected = (old->vlen == p->expected_old_len &&
                                  memcmp(old->value, p->expected_old, old->vlen) == 0);
    }
    return p->return_value;
}

/* ----- main test ------------------------------------------------------- */

static int test_slotcask_cas_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);
    slotcask_init(16, 16);

    SlotcaskDb db;
    int rc = slotcask_open(&db, dir, /*shards*/ 8, /*streams*/ 4,
                           /*slot_size*/ 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds");

    /* ===== plain upsert: insert path ===== */
    SlotcaskUpsertResult ur = {0};
    rc = slotcask_upsert_with_hooks(&db, -1, "k1", 2, "v1-FIRST", 8,
                                     NULL, &ur);
    ASSERT_EQ_INT(rc, 0, "upsert (insert) succeeds");
    ASSERT_EQ_INT(ur.was_update, 0, "first upsert reports was_update=0");
    ASSERT_EQ_INT(ur.condition_not_met, 0, "no condition_not_met on plain insert");

    /* ===== plain upsert: update path ===== */
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k1", 2, "v1-SECOND", 9,
                                     NULL, &ur);
    ASSERT_EQ_INT(rc, 0, "upsert (update) succeeds");
    ASSERT_EQ_INT(ur.was_update, 1, "second upsert reports was_update=1");

    void *vbuf = NULL; size_t vlen = 0;
    rc = slotcask_get(&db, "k1", 2, &vbuf, &vlen);
    ASSERT_EQ_INT(rc, 0, "get after upsert");
    ASSERT_TRUE(vlen == 9 && memcmp(vbuf, "v1-SECOND", 9) == 0,
                "get returns latest upsert value");
    free(vbuf);

    /* ===== if_not_exists rejects existing ===== */
    SlotcaskUpsertOpts ine_opts = { .if_not_exists = 1 };
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k1", 2, "v1-INE", 6,
                                     &ine_opts, &ur);
    ASSERT_EQ_INT(rc, -2, "if_not_exists rejects existing → -2");
    ASSERT_EQ_INT(ur.condition_not_met, 1, "condition_not_met=1");
    ASSERT_EQ_INT(ur.was_update, 1, "result reports the existing key was_update=1");
    ASSERT_TRUE(ur.current_value && ur.current_vlen == 9 &&
                memcmp(ur.current_value, "v1-SECOND", 9) == 0,
                "current_value carries the existing record");
    free(ur.current_value);

    /* if_not_exists allows new key */
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k2", 2, "v2", 2,
                                     &ine_opts, &ur);
    ASSERT_EQ_INT(rc, 0, "if_not_exists on new key succeeds");
    ASSERT_EQ_INT(ur.was_update, 0, "fresh insert was_update=0");

    /* ===== require_existing rejects missing ===== */
    SlotcaskUpsertOpts req_opts = { .require_existing = 1 };
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "missing_key", 11, "x", 1,
                                     &req_opts, &ur);
    ASSERT_EQ_INT(rc, -2, "require_existing rejects missing → -2");
    ASSERT_EQ_INT(ur.was_update, 0, "result reports key was missing");
    ASSERT_TRUE(ur.current_value == NULL, "current_value NULL when no record");

    /* ===== check_fn called with NULL old on insert ===== */
    CheckCtx cc = {0};
    SlotcaskUpsertOpts check_opts = { .check = check_always_proceed, .check_ctx = &cc };
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k3", 2, "v3", 2,
                                     &check_opts, &ur);
    ASSERT_EQ_INT(rc, 0, "upsert with proceeding check_fn succeeds");
    ASSERT_EQ_INT(cc.called, 1, "check_fn called once");
    ASSERT_EQ_INT(cc.saw_old, 0, "check_fn saw old=NULL on insert");

    /* check_fn called with old bytes on update */
    cc = (CheckCtx){0};
    cc.expected_old = "v3";
    cc.expected_old_len = 2;
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k3", 2, "v3-UPDATED", 10,
                                     &check_opts, &ur);
    ASSERT_EQ_INT(rc, 0, "upsert with check_fn (update) succeeds");
    ASSERT_EQ_INT(cc.saw_old, 1, "check_fn saw old non-NULL on update");
    ASSERT_EQ_INT(cc.old_match_expected, 1, "check_fn saw correct old bytes");

    /* check_fn rejecting → condition_not_met */
    CheckCtx cc_reject = {0};
    SlotcaskUpsertOpts reject_opts = { .check = check_always_reject,
                                       .check_ctx = &cc_reject };
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k3", 2, "v3-NEVER", 8,
                                     &reject_opts, &ur);
    ASSERT_EQ_INT(rc, -2, "rejecting check_fn → -2");
    ASSERT_EQ_INT(ur.condition_not_met, 1, "condition_not_met=1");
    ASSERT_TRUE(ur.current_value && ur.current_vlen == 10 &&
                memcmp(ur.current_value, "v3-UPDATED", 10) == 0,
                "current_value reflects the still-current record");
    free(ur.current_value);

    /* k3 still has the previous value (rejection didn't write). */
    vbuf = NULL; vlen = 0;
    slotcask_get(&db, "k3", 2, &vbuf, &vlen);
    ASSERT_TRUE(vlen == 10 && memcmp(vbuf, "v3-UPDATED", 10) == 0,
                "k3 unchanged after rejected upsert");
    free(vbuf);

    /* ===== pre_commit fires with both old and new ===== */
    PreCommitCtx pc = {0};
    pc.expected_old = "v3-UPDATED";
    pc.expected_old_len = 10;
    pc.expected_new = "v3-FINAL";
    pc.expected_new_len = 8;
    pc.return_value = 0;
    SlotcaskUpsertOpts pre_opts = {
        .pre_commit = pre_commit_recording,
        .pre_commit_ctx = &pc,
    };
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k3", 2, "v3-FINAL", 8,
                                     &pre_opts, &ur);
    ASSERT_EQ_INT(rc, 0, "upsert with pre_commit succeeds");
    ASSERT_EQ_INT(pc.called, 1, "pre_commit fired once");
    ASSERT_EQ_INT(pc.saw_old, 1, "pre_commit saw old non-NULL on update");
    ASSERT_EQ_INT(pc.old_match_expected, 1, "pre_commit got old bytes correct");
    ASSERT_EQ_INT(pc.new_match_expected, 1, "pre_commit got new bytes correct");
    ASSERT_EQ_INT(pc.is_update_seen, 1, "pre_commit saw is_update=1");

    /* ===== pre_commit aborts: new slot tombstoned, kf untouched ===== */
    pc = (PreCommitCtx){0};
    pc.return_value = 1;
    memset(&ur, 0, sizeof(ur));
    rc = slotcask_upsert_with_hooks(&db, -1, "k3", 2, "v3-DOOMED", 9,
                                     &pre_opts, &ur);
    ASSERT_EQ_INT(rc, -1, "pre_commit aborting → -1");
    ASSERT_EQ_INT(pc.called, 1, "pre_commit fired even on abort");

    /* k3 still reads v3-FINAL */
    vbuf = NULL; vlen = 0;
    slotcask_get(&db, "k3", 2, &vbuf, &vlen);
    ASSERT_TRUE(vlen == 8 && memcmp(vbuf, "v3-FINAL", 8) == 0,
                "k3 unchanged after pre_commit-aborted upsert");
    free(vbuf);

    /* ===== exists ===== */
    ASSERT_EQ_INT(slotcask_exists(&db, "k1", 2), 1, "exists k1 → 1");
    ASSERT_EQ_INT(slotcask_exists(&db, "missing_key", 11), 0, "exists missing → 0");

    /* ===== delete with criteria ===== */
    PreCommitCtx dpc = {0};
    dpc.expected_old = "v3-FINAL";
    dpc.expected_old_len = 8;
    dpc.return_value = 0;
    SlotcaskDeleteOpts del_opts = {
        .pre_commit = delete_pre_commit_recording,
        .pre_commit_ctx = &dpc,
    };
    SlotcaskDeleteResult dr = {0};
    rc = slotcask_delete_with_hooks(&db, "k3", 2, &del_opts, &dr);
    ASSERT_EQ_INT(rc, 0, "delete with pre_commit succeeds");
    ASSERT_EQ_INT(dpc.called, 1, "delete pre_commit fired");
    ASSERT_EQ_INT(dpc.old_match_expected, 1, "delete pre_commit saw correct old");
    ASSERT_EQ_INT(slotcask_exists(&db, "k3", 2), 0, "k3 gone after delete");

    /* delete with rejecting check */
    CheckCtx dcc = {0};
    SlotcaskDeleteOpts del_reject = { .check = check_always_reject, .check_ctx = &dcc };
    dr = (SlotcaskDeleteResult){0};
    rc = slotcask_delete_with_hooks(&db, "k1", 2, &del_reject, &dr);
    ASSERT_EQ_INT(rc, -2, "delete rejected by check → -2");
    ASSERT_EQ_INT(dr.condition_not_met, 1, "delete condition_not_met=1");
    ASSERT_TRUE(dr.current_value && dr.current_vlen == 9 &&
                memcmp(dr.current_value, "v1-SECOND", 9) == 0,
                "delete current_value reflects undeleted record");
    free(dr.current_value);
    ASSERT_EQ_INT(slotcask_exists(&db, "k1", 2), 1, "k1 survived rejected delete");

    /* delete missing key */
    dr = (SlotcaskDeleteResult){0};
    rc = slotcask_delete_with_hooks(&db, "missing_key", 11, NULL, &dr);
    ASSERT_EQ_INT(rc, -2, "delete missing → -2");
    ASSERT_EQ_INT(dr.not_found, 1, "delete not_found=1");

    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-cas", test_slotcask_cas_run)
