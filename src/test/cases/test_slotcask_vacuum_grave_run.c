/* Regression: vacuum (compact) on segments whose live records sit
 * behind a run of abandoned staged records ("graves") —
 * docs/plans/2026-11-17-kf-grave-frontier-fix.md, Task 5.
 *
 * Fixture: 512 KiB segments (slotcask_test_set_seg_max_bytes), slot_size
 * 8192, one stream. The DONOR (file 0) holds, in order: keyX (live at
 * build time, deleted in the tombstone pass), kA (live), g1 and g2
 * (deleted, then flag-flipped to graves), kkeyC (live, BEHIND the
 * graves), then f0-* fillers. The RECIPIENT (file 1) ends up with
 * exactly two free slots after the tombstone pass: one flag=2
 * tombstone (f1-000000) and one flag=0 content grave (f1-000001,
 * flipped). Migrating the donor's two live records (kA, kkeyC) can only
 * succeed if the recipient free-list walk harvests the grave's capacity.
 *
 * On base: seg_stat_one_varlen stats the donor at live==1 (kA only —
 * the walk breaks at the graves), the donor O_DIRECT scan stops at the
 * graves having migrated only kA, and the donor is dropped with kkeyC
 * still in it — kkeyC reads fail.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define TEST_SEG_BYTES (512u * 1024u)

/* Record sizes here (docs/plans/2026-11-17-kf-grave-frontier-fix.md,
 * Task 5): kA = 24+2+1 -> 32 B, kkeyC = 24+5+6 -> 40 B, g1/g2 (2-byte
 * keys, 8000-byte values) = 8032 B, keyX = 24+4+5000 -> 5032 B, the
 * f0 and f1 fillers (9-byte keys, 8000-byte values) = 8040 B. */
#define FILL_VAL_LEN 8000

/* Walk a segment file by its 24-byte headers (klen at +16, flag at +18,
 * vlen at +20, size (24+klen+vlen+7)&~7) and flip the flag byte of the
 * record whose key bytes equal `key` to 0 — crafting a struct-valid
 * flag=0 record with intact content (an abandoned staged payload).
 * Asserts the record was found and was a tombstone before the flip —
 * never a blind offset. */
static void flip_record_to_grave(const char *path, const char *key,
                                 size_t klen, const char *desc) {
    int fd = open(path, O_RDWR);
    ASSERT_TRUE(fd >= 0, desc);
    if (fd < 0) return;
    struct stat st;
    ASSERT_EQ_INT(fstat(fd, &st), 0, desc);
    off_t off = 0;
    int found = 0;
    while (off + 24 <= (off_t)st.st_size) {
        uint8_t hdr[24];
        if (pread(fd, hdr, sizeof(hdr), off) != sizeof(hdr)) break;
        uint16_t kl;
        uint32_t vl;
        memcpy(&kl, hdr + 16, 2);
        memcpy(&vl, hdr + 20, 4);
        size_t rs = (24u + (size_t)kl + vl + 7u) & ~(size_t)7u;
        if (kl == klen && rs >= 24u + klen) {
            char kb[32];
            if (pread(fd, kb, klen, off + 24) == (ssize_t)klen &&
                memcmp(kb, key, klen) == 0) {
                ASSERT_EQ_INT((int)hdr[18], 2, "record is a tombstone before flip");
                ASSERT_EQ_INT((int)pwrite(fd, "\0", 1, off + 18), 1, desc);
                found = 1;
                break;
            }
        }
        off += (off_t)rs;
    }
    ASSERT_TRUE(found, desc);
    close(fd);
}

/* Walk a segment file and count flag==2 tombstones and flag==0
 * content records (graves) among its records. */
static void count_tombs_and_graves(const char *path, size_t *out_tombs,
                                   size_t *out_graves, const char *desc) {
    *out_tombs = 0;
    *out_graves = 0;
    int fd = open(path, O_RDONLY);
    ASSERT_TRUE(fd >= 0, desc);
    if (fd < 0) return;
    struct stat st;
    ASSERT_EQ_INT(fstat(fd, &st), 0, desc);
    off_t off = 0;
    while (off + 24 <= (off_t)st.st_size) {
        uint8_t hdr[24];
        if (pread(fd, hdr, sizeof(hdr), off) != sizeof(hdr)) break;
        uint16_t kl;
        uint32_t vl;
        memcpy(&kl, hdr + 16, 2);
        memcpy(&vl, hdr + 20, 4);
        size_t rs = (24u + (size_t)kl + vl + 7u) & ~(size_t)7u;
        if ((size_t)hdr[18] == 2) {
            (*out_tombs)++;
        } else if (hdr[18] == 0 && (kl != 0 || vl != 0)) {
            (*out_graves)++;
        }
        off += (off_t)rs;
    }
    close(fd);
}

static int test_slotcask_vacuum_grave_run_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-vacuum-grave-run-%d",
             (int)getpid());
    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(TEST_SEG_BYTES);

    char xval[5000], gval[FILL_VAL_LEN], fill[FILL_VAL_LEN];
    memset(xval, 'x', sizeof(xval));
    memset(gval, 'g', sizeof(gval));
    memset(fill, 'z', sizeof(fill));

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192); /* single stream: deterministic file layout */
    ASSERT_EQ_INT(ret, 0, "slotcask_open");

    /* Build order is load-bearing (plan Task 5, round-3 review): keyX
       stays LIVE until the tombstone pass — deleting it early would
       push its 5032-byte slot into the free pool and the next small
       insert would pop it, landing kA/kkeyC ahead of the graves. */
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, "keyX", 4, xval,
                                              sizeof(xval), NULL, NULL), 0,
                  "insert keyX (live for now)");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, "kA", 2, "v", 1,
                                              NULL, NULL), 0,
                  "insert kA (live, ahead of the graves)");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, "g1", 2, gval,
                                              sizeof(gval), NULL, NULL), 0,
                  "insert g1 (future grave)");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, "g2", 2, gval,
                                              sizeof(gval), NULL, NULL), 0,
                  "insert g2 (future grave)");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, "kkeyC", 5, "cvalue", 6,
                                              NULL, NULL), 0,
                  "insert kkeyC (live, BEHIND the graves)");

    /* Rotations are relative to the current active file (fixed->varlen
       migration starts a private file-ID base). */
    uint32_t base_file_id = db.streams[0].active_file_id;

    size_t rec_size = (24u + 9u + FILL_VAL_LEN + 7u) & ~(size_t)7u; /* 8040 */
    size_t key_cap = (size_t)(slotcask_seg_max_bytes() / rec_size) + 4u;
    char (*f0keys)[32] = calloc(key_cap, sizeof(*f0keys));
    ASSERT_NOT_NULL(f0keys, "alloc f0 key array");
    if (!f0keys) {
        slotcask_test_set_seg_max_bytes(0);
        slotcask_shutdown();
        system(rmcmd);
        return 1;
    }

    size_t f0_count = 0;
    while (db.streams[0].active_file_id == base_file_id) {
        if (f0_count >= key_cap) break;
        snprintf(f0keys[f0_count], 32, "f0-%06zu", f0_count);
        if (slotcask_insert_with_hooks(&db, 0, f0keys[f0_count],
                            strlen(f0keys[f0_count]), fill, sizeof(fill),
                            NULL, NULL) != 0) {
            ASSERT_TRUE(0, "insert f0 filler");
            break;
        }
        f0_count++;
    }
    ASSERT_TRUE(db.streams[0].active_file_id == base_file_id + 1,
                "rotation into file 1 happened");
    ASSERT_TRUE(f0_count >= 2, "at least two f0 fillers inserted");

    size_t f1_count = 0;
    char f1_first[32], f1_second[32];
    while (db.streams[0].active_file_id == base_file_id + 1) {
        char key[32];
        snprintf(key, sizeof(key), "f1-%06zu", f1_count);
        if (f1_count == 0) snprintf(f1_first, sizeof(f1_first), "%s", key);
        if (f1_count == 1) snprintf(f1_second, sizeof(f1_second), "%s", key);
        if (slotcask_insert_with_hooks(&db, 0, key, strlen(key), fill,
                                        sizeof(fill), NULL, NULL) != 0) {
            ASSERT_TRUE(0, "insert f1 filler");
            break;
        }
        f1_count++;
    }
    ASSERT_TRUE(db.streams[0].active_file_id == base_file_id + 2,
                "rotation into file 2 happened (file 2 holds only the trigger)");
    ASSERT_TRUE(f1_count >= 2, "at least two f1 fillers inserted");

    /* Tombstone pass — all deletes while the db is open. The LAST f0
       key is kept live: it triggered the rotation and landed at offset 0
       of file 1; deleting it would add a third free slot to the
       recipient and break the pin (plan Task 5 step 7, round-3 review). */
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, "keyX", 4, NULL, NULL), 0,
                  "delete keyX");
    for (size_t i = 0; i + 1 < f0_count; i++) {
        ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, f0keys[i],
                                      strlen(f0keys[i]), NULL, NULL), 0,
                      "delete f0 filler that landed in file 0");
    }
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, f1_first,
                                  strlen(f1_first), NULL, NULL), 0,
                  "delete first f1 filler (stays a tombstone)");
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, f1_second,
                                  strlen(f1_second), NULL, NULL), 0,
                  "delete second f1 filler (becomes a recipient grave)");
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, "g1", 2, NULL, NULL), 0,
                  "delete g1");
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db, "g2", 2, NULL, NULL), 0,
                  "delete g2");
    free(f0keys);

    slotcask_close(&db);

    /* Flip g1/g2 in the DONOR (file 0) and the second deleted f1 record
       in the recipient (file 1) to graves. Both g records live in file 0
       (inserted before any rotation). */
    char donor[512], recipient[512];
    snprintf(donor, sizeof(donor), "%s/data/streams/000/000000.dat", tmpdir);
    snprintf(recipient, sizeof(recipient), "%s/data/streams/000/000001.dat",
             tmpdir);
    flip_record_to_grave(donor, "g1", 2, "flip g1 to a grave in the donor");
    flip_record_to_grave(donor, "g2", 2, "flip g2 to a grave in the donor");
    flip_record_to_grave(recipient, f1_second, strlen(f1_second),
                         "flip the second f1 to a grave in the recipient");

    /* The recipient must now hold exactly one tombstone and one grave:
       two free slots for the donor's two live records. */
    size_t tombs = 0, graves = 0;
    count_tombs_and_graves(recipient, &tombs, &graves,
                           "walk recipient before compaction");
    ASSERT_EQ_INT((int)tombs, 1, "recipient has exactly one tombstone slot");
    ASSERT_EQ_INT((int)graves, 1, "recipient has exactly one grave slot");

    slotcask_test_set_seg_max_bytes(TEST_SEG_BYTES);
    memset(&db, 0, sizeof(db));
    ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(ret, 0, "reopen with donor graves");
    uint64_t inv = 0;
    ASSERT_EQ_INT(slotcask_validate_live_refs(&db, &inv), 0,
                  "fixture refs valid at reopen");
    ASSERT_EQ_INT((int)inv, 0, "no invalid refs at reopen");

    /* Pending KFM2 marker: the recipient's flag=0 grave may be
       live-pending, so compaction must neither reuse it nor drop the
       donor. Placeholder marker; the grave/tombstone shape and the donor
       must be untouched. */
    char mpath[600];
    snprintf(mpath, sizeof(mpath),
             "%s/data/kf/000_batch_1_0000000000000001_marker.dat", tmpdir);
    {
        FILE *mf = fopen(mpath, "w");
        ASSERT_NOT_NULL(mf, "create placeholder pending marker");
        if (mf) fclose(mf);
    }
    int dropped = 0, skipped = 0;
    ret = slotcask_compact_segs_ex(&db, &dropped, &skipped);
    ASSERT_EQ_INT(ret, 0, "compact_segs_ex with pending marker completes");
    ASSERT_EQ_INT(skipped, 1, "pending marker: compaction reported skipped");
    ASSERT_EQ_INT(dropped, 0, "pending marker: no segment dropped");
    {
        size_t t2 = 0, g2 = 0;
        count_tombs_and_graves(recipient, &t2, &g2,
                               "walk recipient after skipped compaction");
        ASSERT_EQ_INT((int)t2, 1, "pending marker: tombstone slot not reused");
        ASSERT_EQ_INT((int)g2, 1, "pending marker: grave slot not reused");
    }
    ASSERT_EQ_INT(access(donor, F_OK), 0, "pending marker: donor still exists");
    ASSERT_EQ_INT(unlink(mpath), 0, "remove placeholder marker");

    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes without an error");

    void *val_out = NULL;
    size_t vlen_out = 0;
    ret = slotcask_get(&db, "kA", 2, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get kA after compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 1, "kA vlen intact");
        ASSERT_TRUE(memcmp(val_out, "v", 1) == 0, "kA value intact");
        free(val_out);
        val_out = NULL;
    }
    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get kkeyC after compaction (was behind the graves)");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "kkeyC vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "kkeyC value intact");
        free(val_out);
        val_out = NULL;
    }
    ASSERT_EQ_INT(slotcask_get(&db, "g1", 2, &val_out, &vlen_out), -1,
                  "g1 absent after compaction");
    ASSERT_EQ_INT(slotcask_get(&db, "g2", 2, &val_out, &vlen_out), -1,
                  "g2 absent after compaction");
    ASSERT_EQ_INT(slotcask_get(&db, "keyX", 4, &val_out, &vlen_out), -1,
                  "keyX absent after compaction");

    struct stat st;
    ASSERT_TRUE(stat(donor, &st) != 0,
                "donor segment 000000.dat no longer exists on disk");
    ASSERT_TRUE(dropped >= 1, "donor was reclaimed (dropped >= 1)");

    /* Post-compaction refs must stay valid (Task 9's addition: a stale
       free-pool entry consumed by the migration would overwrite a
       migrated record on the next insert; validate catches clobbered
       live records). */
    inv = 0;
    ASSERT_EQ_INT(slotcask_validate_live_refs(&db, &inv), 0,
                  "refs valid after compaction");
    ASSERT_EQ_INT((int)inv, 0, "no invalid refs after compaction");

    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-vacuum-grave-run", test_slotcask_vacuum_grave_run_run)
