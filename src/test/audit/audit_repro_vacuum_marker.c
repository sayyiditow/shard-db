/* src/test/audit/audit_repro_vacuum_marker.c
 *
 * Audit repro for AUD-VAC-KF-MARKER (docs/plans/2026-11-25-audit-storage-
 * k-reentry.md, Phase 2 area 1 part 3). cmd_vacuum's light path skips
 * segment compaction when KFM2 markers are pending (their replay may reuse
 * flag=0 segment space) but still calls slotcask_compact_kf, which rebuilds
 * every shard with deleted>0 and moves live entries to new slot numbers.
 * A retained update marker names its kf slot explicitly, and the replay
 * repoints that slot WITHOUT verifying which key sits there, so after the
 * compaction it writes into the wrong (here: empty) slot and tombstones
 * the record the real entry still points at.
 *
 * Scenario (1 shard, 256 slots): three keys sharing a start slot occupy
 * S,S+1,S+2; the first two are deleted; an update of the third fails
 * persistently at the activate phase (marker retained, rc -1/EINPROGRESS);
 * the fault clears; slotcask_compact_kf runs; the next clean write replays
 * the marker. Contract asserted: the updated key stays readable with one of
 * its two legal values, and a checked full walk still succeeds.
 *
 * Regression case for the fix (docs/plans/2026-11-25-fix-vac-kf-marker.md):
 * compaction now skips while markers are pending. Runs in the default suite.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);
extern size_t kf_slot_for(const uint8_t hash[16], size_t cap);

#define VM_SLOTS 256

static int vm_prepare(SlotcaskBulkRec *r, const size_t *a, size_t n, void *c,
                      void **ws) {
    (void)r; (void)a; (void)n; (void)c;
    *ws = NULL;
    return 0;
}
static int vm_apply(SlotcaskBulkRec *r, const size_t *a, size_t n, void *c,
                    void *ws) {
    (void)r; (void)a; (void)n; (void)c; (void)ws;
    return 0;
}

static int vm_upsert(SlotcaskDb *db, uint64_t key, const char *val,
                     int windowed) {
    SlotcaskBulkRec rec;
    memset(&rec, 0, sizeof(rec));
    rec.key = &key; rec.klen = sizeof(key);
    rec.value = (void *)val; rec.vlen = strlen(val);
    SlotcaskBulkOpts o;
    memset(&o, 0, sizeof(o));
    if (windowed) {
        o.has_indexed_fields = 1;
        o.prepare_window = vm_prepare;
        o.apply_window   = vm_apply;
    }
    return slotcask_bulk_upsert_in_kfshard(db, 0, &rec, 1, &o);
}

static int vm_walk_cb(const uint8_t h[16], const void *k, size_t kl,
                      const void *v, size_t vl, uint8_t hdr2, void *ctx) {
    (void)h; (void)k; (void)kl; (void)v; (void)vl; (void)hdr2;
    (*(int *)ctx)++;
    return 0;
}

static int audit_repro_vacuum_marker_run(void) {
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-vm-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, VM_SLOTS);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 1, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;
    db.bulk_commit_window = 0;

    /* Three keys with the same start slot -> slots S, S+1, S+2. */
    uint64_t keys[3];
    int nk = 0;
    size_t want = (size_t)-1;
    for (uint64_t c = 1; c < 200000 && nk < 3; c++) {
        uint8_t h[16];
        compute_hash_raw((const char *)&c, sizeof(c), h);
        size_t s = kf_slot_for(h, VM_SLOTS);
        if (want == (size_t)-1) want = s;
        if (s != want || s + 3 >= VM_SLOTS) continue;
        keys[nk++] = c;
    }
    ASSERT_EQ_INT(nk, 3, "found three colliding keys");
    if (t_ctx->failed) goto out;
    for (int i = 0; i < 3; i++)
        ASSERT_EQ_INT(vm_upsert(&db, keys[i], "old-value", 0), 0, "seed insert");
    for (int i = 0; i < 2; i++) {
        SlotcaskBulkRec drec;
        SlotcaskBulkDeleteOpts dopt;
        memset(&drec, 0, sizeof(drec));
        memset(&dopt, 0, sizeof(dopt));
        drec.key = &keys[i]; drec.klen = sizeof(keys[i]);
        ASSERT_EQ_INT(slotcask_bulk_delete_in_kfshard(&db, 0, &drec, 1, &dopt),
                      0, "delete filler");
    }

    /* Update the third key; activation fails persistently -> the window's
       marker is retained, the outcome is pending. */
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, VM_SLOTS);
    g_shard_test_fail_phase = SHARD_TEST_PHASE_A;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    int urc = vm_upsert(&db, keys[2], "new-value", 1);
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    g_shard_test_fail_sticky = 0;
    ASSERT_EQ_INT(urc, -1, "update reports pending");
    ASSERT_EQ_INT(errno, EINPROGRESS, "pending errno");

    /* What cmd_vacuum's light path does when markers are pending. */
    int kf_skipped = 0;
    ASSERT_EQ_INT(slotcask_compact_kf(&db, &kf_skipped), 0, "kf compaction call succeeds");
    ASSERT_EQ_INT(kf_skipped, 1, "kf compaction is skipped while a marker is pending");

    /* The next clean write gate-replays the marker. */
    int wrc = vm_upsert(&db, 900001, "tail", 1);
    char m[80];
    snprintf(m, sizeof(m), "clean write after compaction (rc=%d errno=%d)", wrc, errno);
    ASSERT_EQ_INT(wrc, 0, m);

    void *val = NULL; size_t vlen = 0;
    int grc = slotcask_get(&db, &keys[2], sizeof(keys[2]), &val, &vlen);
    ASSERT_EQ_INT(grc, 0, "updated key still readable");
    if (grc == 0) {
        int ok = (vlen == 9 && memcmp(val, "new-value", 9) == 0) ||
                 (vlen == 9 && memcmp(val, "old-value", 9) == 0);
        ASSERT_TRUE(ok, "updated key holds old or new value");
    }
    free(val);
    int walked = 0;
    int wlk = slotcask_walk_live_checked(&db, vm_walk_cb, &walked);
    snprintf(m, sizeof(m), "checked walk succeeds (rc=%d errno=%d, %d rows)",
             wlk, errno, walked);
    ASSERT_EQ_INT(wlk, 0, m);
    ASSERT_EQ_INT(walked, 2, "exactly the two live keys are visible");
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-vacuum-kf-pending-markers", audit_repro_vacuum_marker_run)
