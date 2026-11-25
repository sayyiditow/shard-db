/* AUD-QRY-11: the K-phase commit is re-runnable (the window coordinator
 * replays it after a K msync failure). kf_commit_planned_slot booked the
 * kf header counters unconditionally, so a re-entry over an already
 * committed slot double-booked total++ (fresh placement) or deleted--
 * (tombstone reuse): count over-reported until the next header rebuild.
 * Drives the legacy in-process window path (the one that syncs the kf
 * inside K) with a single non-sticky K failure and reads the headers. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

typedef struct { SlotcaskDb db; char base[PATH_MAX]; } KrDb;

static void kr_rmtree(const KrDb *w) {
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", w->base);
    if (system(cmd) != 0) { /* best effort */ }
}

/* On any failure, removes the temp dir and shuts slotcask down so an early
   return from the caller leaks nothing. */
static int kr_open(KrDb *w) {
    slotcask_init(64, 64);
    memset(w, 0, sizeof(*w));
    char b[] = "/tmp/shard-db-kr-test-XXXXXX";
    if (!mkdtemp(b)) { slotcask_shutdown(); return -1; }
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) goto fail;
    if (slotcask_open(&w->db, w->base, 8, 1, 64) != 0) goto fail;
    w->db.bulk_commit_window = 0;
    shard_test_ctl_reset();
    return 0;
fail:
    kr_rmtree(w);
    slotcask_shutdown();
    return -1;
}

static void kr_close(KrDb *w) {
    slotcask_close(&w->db);
    kr_rmtree(w);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

static int test_kf_window_replay_double_booking(void) {
    KrDb w;
    ASSERT_EQ_INT(kr_open(&w), 0, "open db");
    if (t_ctx->failed) return 1;

    uint64_t key = 777;
    uint8_t h[16];
    compute_hash_raw((const char *)&key, sizeof(key), h);
    int shard = compute_record_shard(h, 8);
    uint64_t total = 0, deleted = 0;

    /* Fresh placement, one K msync failure: the coordinator replays K on
       the same plan and converges. */
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    int rc = kr_upsert(&w, key, shard);
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    ASSERT_EQ_INT(rc, 0, "fresh insert converges after one K failure");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&w.db, &total, &deleted), 0, "totals read");
    ASSERT_EQ_INT((int)total, 1, "fresh: total booked once");
    ASSERT_EQ_INT((int)deleted, 0, "fresh: deleted untouched");

    /* Tombstone reuse on a fresh db (scenario A's inflated header must not
       bleed in): insert + delete cleanly, then re-insert under the same
       one-shot K failure (reuse books deleted-- on commit). */
    kr_close(&w);
    if (kr_open(&w) != 0) { ASSERT_TRUE(0, "reopen fresh db"); return 1; }
    shard_test_ctl_reset();
    ASSERT_EQ_INT(kr_upsert(&w, key, shard), 0, "clean insert");
    {
        SlotcaskBulkRec drec;
        SlotcaskBulkDeleteOpts dopt;
        memset(&drec, 0, sizeof(drec));
        memset(&dopt, 0, sizeof(dopt));
        drec.key = &key; drec.klen = sizeof(key);
        ASSERT_EQ_INT(slotcask_bulk_delete_in_kfshard(&w.db, shard, &drec, 1,
                                                      &dopt), 0, "delete key");
    }
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&w.db, &total, &deleted), 0, "totals read");
    ASSERT_EQ_INT((int)(total - deleted), 0, "after delete: live 0");
    uint64_t total_before = total, deleted_before = deleted;
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    rc = kr_upsert(&w, key, shard);
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    ASSERT_EQ_INT(rc, 0, "reuse insert converges after one K failure");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&w.db, &total, &deleted), 0, "totals read");
    char msg[160];
    snprintf(msg, sizeof(msg),
             "reuse: live exact (total=%llu deleted=%llu, was %llu/%llu)",
             (unsigned long long)total, (unsigned long long)deleted,
             (unsigned long long)total_before, (unsigned long long)deleted_before);
    ASSERT_TRUE(total - deleted == 1, msg);

    kr_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-window-replay-double-booking", test_kf_window_replay_double_booking)
