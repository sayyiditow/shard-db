/* Regression: recover_one_stream's tail heuristic vs abandoned staged
 * records ("graves") — docs/plans/2026-11-17-kf-grave-frontier-fix.md.
 *
 * Every mutation writes its record bytes with flag=0 (P phase) durably
 * before the KFM2 marker (M) and activation (A). A write whose window
 * dies between P and M leaves a structurally-valid flag=0 record with
 * intact content — an inert grave. A run of graves longer than one
 * slot_size defeated recover_one_stream's bounded resync window: the
 * scan broke as "ordinary unwritten tail" and parked reserve_off inside
 * the file, so later appends overwrote live records beyond the graves
 * (the 2026-11 incident: dangling live kf refs, refused by the layout
 * migration with "invalid live kf references").
 *
 * Scenarios (each in its own scratch dir; all share one registered case):
 *   1  incident shape: two graves, live record behind them
 *   2  interior grave run between live records
 *   3  grave run at the true tail
 *   4  tombstone run, no flag flips — no-regression pin (passes on base)
 *   5  zero gap then grave then live (the compound)
 *   6  far island behind a multi-MiB hole (hole-aware wide resync)
 *   7  hash-failed flag=0 header is never blindly stepped over
 *   8  healthy reopen must not materialize the sparse tail (guard;
 *      passes on base)
 *   9  record spanning into a hole-punched region (Linux only)
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "types.h"   /* compute_hash_raw (scenario 9 header craft) */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
#include <linux/falloc.h>
#endif

/* 24 hdr + 16 key + 28 val = 68 -> padded to 72; slot_size 120 as in the
 * incident object (players). Two graves = 144 > base's 120-byte window. */
#define REC 72
#define GF_SLOT 120
#define ISLAND_OFF (8LL * 1024 * 1024)

static size_t rec_sz(size_t klen, size_t vlen) {
    return (24u + klen + vlen + 7u) & ~(size_t)7u;
}

static void gf_rmrf(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    system(cmd);
}

static void fresh_dir(char *dir, size_t cap, int n) {
    snprintf(dir, cap, "/tmp/shard-db-grave-frontier-%d-%d", (int)getpid(), n);
    gf_rmrf(dir);
    mkdir(dir, 0755);
}

static void seg_path(char *out, size_t cap, const char *dir) {
    snprintf(out, cap, "%s/data/streams/000/000000.dat", dir);
}

/* Flip the flag byte of the record at `off` to 0, leaving a
 * structurally valid flag=0 record with intact content — the on-disk
 * shape of an abandoned staged payload (P durable, M/A never happened). */
static void craft_grave(const char *dir, long off, const char *desc) {
    char path[512];
    seg_path(path, sizeof(path), dir);
    int fd = open(path, O_WRONLY);
    ASSERT_TRUE(fd >= 0, desc);
    if (fd < 0) return;
    /* flag byte sits at record offset +18 */
    ASSERT_EQ_INT((int)pwrite(fd, "\0", 1, off + 18), 1, desc);
    close(fd);
}

/* Keys are the first 16 bytes of the string literals below (draft
 * parity): strlen(key) is 18, the engine calls use KLEN=16 so the
 * record arithmetic matches the plan's 16-byte-key sizes. */
#define KLEN 16

static int ins(SlotcaskDb *db, const char *key, const void *val, size_t vlen) {
    return slotcask_insert_with_hooks(db, 0, key, KLEN, val, vlen,
                                       NULL, NULL);
}

static int del(SlotcaskDb *db, const char *key) {
    return slotcask_delete_with_hooks(db, key, KLEN, NULL, NULL);
}

static int read_is(SlotcaskDb *db, const char *key,
                   const void *want, size_t want_len) {
    void *got = NULL;
    size_t got_len = 0;
    int rc = slotcask_get(db, key, KLEN, &got, &got_len);
    int ok = rc == 0 && got_len == want_len &&
             memcmp(got, want, want_len) == 0;
    free(got);
    return ok;
}

static int absent(SlotcaskDb *db, const char *key) {
    void *got = NULL;
    size_t got_len = 0;
    int rc = slotcask_get(db, key, KLEN, &got, &got_len);
    free(got);
    return rc != 0;
}

static void assert_refs_clean(SlotcaskDb *db, const char *desc) {
    uint64_t inv = 0;
    ASSERT_EQ_INT(slotcask_validate_live_refs(db, &inv), 0, desc);
    ASSERT_EQ_INT((int)inv, 0, desc);
}

static int open_db(SlotcaskDb *db, const char *dir, int slot_size,
                   const char *desc) {
    memset(db, 0, sizeof(*db));
    int rc = slotcask_open(db, dir, 8, 1, slot_size);
    ASSERT_EQ_INT(rc, 0, desc);
    return rc;
}

/* Scenario 1 — the incident shape: graves at 0 and 72, live C at 144.
 * Base parks reserve_off at 0; F's append clobbers C. */
static void scenario1_incident(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vb[REC - 44], vc[REC - 44], vd[REC - 44],
         ve[REC - 44], vf[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vb, 'b', sizeof(vb));
    memset(vc, 'c', sizeof(vc)); memset(vd, 'd', sizeof(vd));
    memset(ve, 'e', sizeof(ve)); memset(vf, 'f', sizeof(vf));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 1: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 1: insert A");
    ASSERT_EQ_INT(ins(&db, "key-B-0123456789ab", vb, sizeof(vb)), 0, "scenario 1: insert B");
    ASSERT_EQ_INT(ins(&db, "key-C-0123456789ab", vc, sizeof(vc)), 0, "scenario 1: insert C");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 1: frontier after A,B,C is 216");
    ASSERT_EQ_INT(del(&db, "key-A-0123456789ab"), 0, "scenario 1: delete A");
    ASSERT_EQ_INT(del(&db, "key-B-0123456789ab"), 0, "scenario 1: delete B");
    slotcask_close(&db);

    craft_grave(dir, 0, "scenario 1: grave A");
    craft_grave(dir, REC, "scenario 1: grave B");

    if (open_db(&db, dir, GF_SLOT, "scenario 1: reopen with grave run") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 1: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 1: frontier past the graves at 216");
    ASSERT_TRUE(read_is(&db, "key-C-0123456789ab", vc, sizeof(vc)),
                "scenario 1: C reads back after reopen");

    ASSERT_EQ_INT(ins(&db, "key-D-0123456789ab", vd, sizeof(vd)), 0, "scenario 1: insert D");
    ASSERT_EQ_INT(ins(&db, "key-E-0123456789ab", ve, sizeof(ve)), 0, "scenario 1: insert E");
    ASSERT_EQ_INT(ins(&db, "key-F-0123456789ab", vf, sizeof(vf)), 0, "scenario 1: insert F");
    ASSERT_TRUE(db.streams[0].reserve_off == 6 * REC,
                "scenario 1: frontier after D,E,F is 432");
    ASSERT_TRUE(read_is(&db, "key-C-0123456789ab", vc, sizeof(vc)),
                "scenario 1: C not clobbered by append");
    ASSERT_TRUE(read_is(&db, "key-D-0123456789ab", vd, sizeof(vd)), "scenario 1: D reads back");
    ASSERT_TRUE(read_is(&db, "key-E-0123456789ab", ve, sizeof(ve)), "scenario 1: E reads back");
    ASSERT_TRUE(read_is(&db, "key-F-0123456789ab", vf, sizeof(vf)), "scenario 1: F reads back");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 2 — interior grave run: A, [grave X], [grave Y], B.
 * Base parks at 72 (the first grave) and the reserve_off assert fails. */
static void scenario2_interior_run(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44], vb[REC - 44], vc[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy)); memset(vb, 'b', sizeof(vb));
    memset(vc, 'c', sizeof(vc));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 2: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 2: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 2: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 2: insert Y");
    ASSERT_EQ_INT(ins(&db, "key-B-0123456789ab", vb, sizeof(vb)), 0, "scenario 2: insert B");
    ASSERT_TRUE(db.streams[0].reserve_off == 4 * REC,
                "scenario 2: frontier after A,X,Y,B is 288");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 2: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 2: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "scenario 2: grave X");
    craft_grave(dir, 2 * REC, "scenario 2: grave Y");

    if (open_db(&db, dir, GF_SLOT, "scenario 2: reopen with interior run") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 2: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 4 * REC,
                "scenario 2: frontier past the interior graves at 288");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "scenario 2: A reads back after reopen");
    ASSERT_TRUE(read_is(&db, "key-B-0123456789ab", vb, sizeof(vb)),
                "scenario 2: B reads back after reopen");

    ASSERT_EQ_INT(ins(&db, "key-C-0123456789ab", vc, sizeof(vc)), 0,
                  "scenario 2: insert C after reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 5 * REC,
                "scenario 2: C appended at 288, frontier 360");
    ASSERT_TRUE(read_is(&db, "key-B-0123456789ab", vb, sizeof(vb)),
                "scenario 2: B still reads after the append");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 3 — grave run at the true tail. Base parks at 72. */
static void scenario3_tail_run(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 3: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 3: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 3: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 3: insert Y");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 3: frontier after A,X,Y is 216");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 3: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 3: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "scenario 3: grave X");
    craft_grave(dir, 2 * REC, "scenario 3: grave Y");

    if (open_db(&db, dir, GF_SLOT, "scenario 3: reopen with tail run") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 3: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 3: frontier past the tail graves at 216");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "scenario 3: A reads back after reopen");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 4 — tombstone run with NO flag flips: the no-regression pin.
 * Must pass on base and stay green after the fix. */
static void scenario4_tombstone_pin(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 4: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 4: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 4: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 4: insert Y");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 4: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 4: delete Y");
    slotcask_close(&db);

    if (open_db(&db, dir, GF_SLOT, "scenario 4: reopen (plain tombstones)") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 4: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 4: frontier past the tombstones at 216");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "scenario 4: A reads back after reopen");
    ASSERT_TRUE(absent(&db, "key-X-0123456789ab"), "scenario 4: X absent");
    ASSERT_TRUE(absent(&db, "key-Y-0123456789ab"), "scenario 4: Y absent");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 5 — the compound: [grave S 0..71][zero gap 72..119]
 * [grave A1 120..191][live A2 192..263]. Base's bounded resync finds no
 * flag∈{1,2} target inside [0,120] and parks reserve_off at 0. */
static void scenario5_gap_then_grave(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char vz[80], vs[REC - 44], va1[REC - 44], va2[REC - 44], vt[REC - 44];
    memset(vz, 'z', sizeof(vz)); memset(vs, 's', sizeof(vs));
    memset(va1, '1', sizeof(va1)); memset(va2, '2', sizeof(va2));
    memset(vt, 't', sizeof(vt));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 5: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-Z-0123456789ab", vz, sizeof(vz)), 0,
                  "scenario 5: insert Z (120 B record)");
    ASSERT_TRUE(db.streams[0].reserve_off == 120,
                "scenario 5: Z landed at 0, frontier 120");
    ASSERT_EQ_INT(ins(&db, "key-A1-0123456789ab", va1, sizeof(va1)), 0,
                  "scenario 5: insert A1");
    ASSERT_TRUE(db.streams[0].reserve_off == 192,
                "scenario 5: A1 landed at 120, frontier 192");
    ASSERT_EQ_INT(ins(&db, "key-A2-0123456789ab", va2, sizeof(va2)), 0,
                  "scenario 5: insert A2");
    ASSERT_TRUE(db.streams[0].reserve_off == 264,
                "scenario 5: A2 landed at 192, frontier 264");

    /* Z's 120-byte slot goes to the free pool (record becomes flag=2). */
    ASSERT_EQ_INT(del(&db, "key-Z-0123456789ab"), 0, "scenario 5: delete Z");
    /* S pops Z's slot: 72 B written at offset 0, pool_split_leftover
     * zeroes 72..119. reserve_off stays 264. */
    ASSERT_EQ_INT(ins(&db, "key-S-0123456789ab", vs, sizeof(vs)), 0,
                  "scenario 5: insert S into Z's pooled slot");
    ASSERT_TRUE(db.streams[0].reserve_off == 264,
                "scenario 5: pooled insert left the frontier at 264");

    ASSERT_EQ_INT(del(&db, "key-S-0123456789ab"), 0, "scenario 5: delete S");
    ASSERT_EQ_INT(del(&db, "key-A1-0123456789ab"), 0, "scenario 5: delete A1");
    slotcask_close(&db);

    craft_grave(dir, 0, "scenario 5: grave S");      /* flag byte at 18 */
    craft_grave(dir, 120, "scenario 5: grave A1");   /* flag byte at 138 */

    if (open_db(&db, dir, GF_SLOT, "scenario 5: reopen (gap then grave)") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 5: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 264,
                "scenario 5: frontier past grave-gap-grave at 264");
    ASSERT_TRUE(read_is(&db, "key-A2-0123456789ab", va2, sizeof(va2)),
                "scenario 5: A2 reads back after reopen");

    ASSERT_EQ_INT(ins(&db, "key-T-0123456789ab", vt, sizeof(vt)), 0,
                  "scenario 5: insert T after reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 336,
                "scenario 5: T appended at 264, frontier 336");
    ASSERT_TRUE(read_is(&db, "key-A2-0123456789ab", va2, sizeof(va2)),
                "scenario 5: A2 still reads after the append");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 6 — far island behind a multi-MiB hole: pins the hole-aware
 * wide resync (data extents found via SEEK_DATA). No refs assert: the
 * copied island record has no kf entry. */
static void scenario6_far_island(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 6: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 6: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 6: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 6: insert Y");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 6: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 6: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "scenario 6: grave X");
    craft_grave(dir, 2 * REC, "scenario 6: grave Y");

    /* Byte-for-byte copy of A's 72-byte record at 8 MiB: hash is the
     * xxh128 of its own key, so it is a valid live record well past any
     * bounded window, behind a real hole. */
    {
        char path[512];
        seg_path(path, sizeof(path), dir);
        int fd = open(path, O_RDWR);
        ASSERT_TRUE(fd >= 0, "scenario 6: open segment for island copy");
        if (fd >= 0) {
            uint8_t rec[REC];
            ASSERT_EQ_INT((int)pread(fd, rec, sizeof(rec), 0), (int)sizeof(rec),
                          "scenario 6: read A's record bytes");
            ASSERT_EQ_INT((int)pwrite(fd, rec, sizeof(rec), ISLAND_OFF),
                          (int)sizeof(rec), "scenario 6: pwrite island at 8 MiB");
            close(fd);
        }
    }

    if (open_db(&db, dir, GF_SLOT, "scenario 6: reopen (island behind hole)") != 0) { gf_rmrf(dir); return; }
    ASSERT_TRUE(db.streams[0].reserve_off == ISLAND_OFF + REC,
                "scenario 6: frontier resynced across the hole to 8 MiB + 72");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "scenario 6: A reads back after reopen");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 7 — hash-failed flag=0 header: Y's grave has a corrupted
 * hash, so the scan must not step over it blindly; it breaks at the
 * tail rules with reserve_off == 144 (Y's start, dead to EOF). */
static void scenario7_hash_failed_grave(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 7: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 7: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 7: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 7: insert Y");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 7: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 7: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "scenario 7: grave X");
    craft_grave(dir, 2 * REC, "scenario 7: grave Y (hash to be corrupted)");

    {
        char path[512];
        seg_path(path, sizeof(path), dir);
        int fd = open(path, O_WRONLY);
        ASSERT_TRUE(fd >= 0, "scenario 7: open segment for hash corruption");
        if (fd >= 0) {
            ASSERT_EQ_INT((int)pwrite(fd, "\xff", 1, 2 * REC), 1,
                          "scenario 7: corrupt Y's hash byte 0");
            close(fd);
        }
    }

    if (open_db(&db, dir, GF_SLOT, "scenario 7: reopen (hash-failed grave)") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 7: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 2 * REC,
                "scenario 7: no blind step over the hash-failed grave (144)");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "scenario 7: A reads back after reopen");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 8 — healthy reopen must not materialize the sparse tail.
 * Guard: passes on base; fails (~+128 MiB on tmpfs) on a wide search
 * that walks the mapping past an extent end. DEFAULT segment size. */
static void scenario8_no_materialize(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char vp[REC - 44], vq[REC - 44];
    memset(vp, 'p', sizeof(vp)); memset(vq, 'q', sizeof(vq));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 8: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-P-0123456789ab", vp, sizeof(vp)), 0, "scenario 8: insert P");
    ASSERT_EQ_INT(ins(&db, "key-Q-0123456789ab", vq, sizeof(vq)), 0, "scenario 8: insert Q");
    ASSERT_TRUE(db.streams[0].reserve_off == 2 * REC,
                "scenario 8: frontier after P,Q is 144");
    slotcask_close(&db);

    char path[512];
    seg_path(path, sizeof(path), dir);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "scenario 8: stat segment before reopen");
    off_t before = (off_t)st.st_blocks * 512;

    if (open_db(&db, dir, GF_SLOT, "scenario 8: healthy reopen") != 0) { gf_rmrf(dir); return; }
    assert_refs_clean(&db, "scenario 8: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 2 * REC,
                "scenario 8: frontier stays at 144");

    ASSERT_EQ_INT(stat(path, &st), 0, "scenario 8: stat segment after reopen");
    off_t after = (off_t)st.st_blocks * 512;
    /* tmpfs with huge pages may legitimately instantiate one 2 MiB page. */
    ASSERT_TRUE(after - before < 4LL * 1024 * 1024,
                "scenario 8: reopen must not materialize the sparse tail");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 9 — live record spanning into a hole-punched region
 * (sparsified segment; Linux fallocate only). R sits at 8 MiB with its
 * middle pages punched out; the wide search's zero padding must let
 * struct_ok see it across the hole. */
static void scenario9_spanning_hole(int n) {
#if defined(__linux__)
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, 16384, "scenario 9: open (slot_size 16384)") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 9: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0, "scenario 9: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0, "scenario 9: insert Y");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "scenario 9: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "scenario 9: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "scenario 9: grave X");
    craft_grave(dir, 2 * REC, "scenario 9: grave Y");

    /* Craft live record R at 8 MiB: 16-byte key, 16000-byte value
     * (64 non-zero + 15872 zero + 64 non-zero) -> declared record
     * 24+16+16000 = 16040 B, already 8-aligned. */
    static const off_t R_OFF = ISLAND_OFF;
    const size_t R_VAL = 16000;
    const size_t R_REC = rec_sz(16, R_VAL);   /* 16040 */
    uint8_t *rec = calloc(1, R_REC);
    ASSERT_NOT_NULL(rec, "scenario 9: alloc island record");
    if (!rec) { slotcask_close(&db); gf_rmrf(dir); return; }
    {
        char path[512];
        seg_path(path, sizeof(path), dir);
        int fd = open(path, O_RDWR);
        ASSERT_TRUE(fd >= 0, "scenario 9: open segment for island craft");
        if (fd < 0) { free(rec); gf_rmrf(dir); return; }
        char key[17] = "key-R-0123456789";
        char *val = malloc(R_VAL);
        if (!val) { close(fd); free(rec); gf_rmrf(dir); return; }
        memset(val, 0, R_VAL);
        memset(val, 'R', 64);
        memset(val + R_VAL - 64, 'r', 64);
        compute_hash_raw(key, 16, rec);            /* hash16 at [0,16) */
        uint16_t kl = 16; uint32_t vl = (uint32_t)R_VAL;
        memcpy(rec + 16, &kl, 2);                  /* native-endian header */
        rec[18] = 1;                               /* live */
        rec[19] = 0;                               /* hdr2 */
        memcpy(rec + 20, &vl, 4);
        memcpy(rec + 24, key, 16);
        memcpy(rec + 40, val, R_VAL);
        ASSERT_EQ_INT((int)pwrite(fd, rec, R_REC, R_OFF), (int)R_REC,
                      "scenario 9: pwrite island R at 8 MiB");
        int frc = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                            R_OFF + 4096, 8192);
        if (frc != 0 &&
            (errno == EOPNOTSUPP || errno == ENOTTY || errno == EINVAL)) {
            TAP_DIAG("# SKIP scenario 9b: fallocate(PUNCH_HOLE) unsupported (%s)\n",
                     strerror(errno));
            close(fd);
            free(val);
            free(rec);
            slotcask_close(&db);
            gf_rmrf(dir);
            return;
        }
        ASSERT_EQ_INT(frc, 0, "scenario 9: punch hole inside R");
        close(fd);
        free(val);
        free(rec);
    }

    if (open_db(&db, dir, 16384, "scenario 9: reopen (spanning hole)") != 0) { gf_rmrf(dir); return; }
    ASSERT_TRUE(db.streams[0].reserve_off == (uint64_t)(R_OFF + (off_t)R_REC),
                "scenario 9: R recognised across the hole (frontier 8 MiB + 16040)");

    slotcask_close(&db);
    gf_rmrf(dir);
#else
    TAP_DIAG("# SKIP scenario 9: requires Linux fallocate(PUNCH_HOLE)\n");
#endif
}

/* Scenario 10 — the wide resync fails closed (Task 10): with the
 * injection on, slotcask_open fails and the segment allocation does not
 * grow (st_blocks); after clearing the injection, reopen gives
 * reserve_off 216 and 0 invalid refs. */
static void scenario10_fail_closed(int n) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), n);
    char va[REC - 44], vb[REC - 44], vc[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vb, 'b', sizeof(vb));
    memset(vc, 'c', sizeof(vc));

    SlotcaskDb db;
    if (open_db(&db, dir, GF_SLOT, "scenario 10: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0, "scenario 10: insert A");
    ASSERT_EQ_INT(ins(&db, "key-B-0123456789ab", vb, sizeof(vb)), 0, "scenario 10: insert B");
    ASSERT_EQ_INT(ins(&db, "key-C-0123456789ab", vc, sizeof(vc)), 0, "scenario 10: insert C");
    ASSERT_EQ_INT(del(&db, "key-A-0123456789ab"), 0, "scenario 10: delete A");
    ASSERT_EQ_INT(del(&db, "key-B-0123456789ab"), 0, "scenario 10: delete B");
    slotcask_close(&db);

    craft_grave(dir, 0, "scenario 10: grave A");
    craft_grave(dir, REC, "scenario 10: grave B");

    char path[512];
    seg_path(path, sizeof(path), dir);
    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "scenario 10: stat segment before");
    off_t blocks_before = (off_t)st.st_blocks;

    slotcask_test_fail_wide_resync_opens(1);
    memset(&db, 0, sizeof(db));
    int rc = slotcask_open(&db, dir, 8, 1, GF_SLOT);
    ASSERT_EQ_INT(rc, -1, "scenario 10: open fails under injection");
    slotcask_test_fail_wide_resync_opens(0);

    ASSERT_EQ_INT(stat(path, &st), 0, "scenario 10: stat segment after failed open");
    ASSERT_TRUE(st.st_blocks == blocks_before,
                "scenario 10: segment allocation did not grow");

    if (open_db(&db, dir, GF_SLOT, "scenario 10: reopen after clearing") != 0) {
        gf_rmrf(dir);
        return;
    }
    assert_refs_clean(&db, "scenario 10: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "scenario 10: frontier at 216 after the failed open");
    ASSERT_TRUE(read_is(&db, "key-C-0123456789ab", vc, sizeof(vc)),
                "scenario 10: C reads back");
    slotcask_close(&db);
    gf_rmrf(dir);
}

static int test_grave_run_frontier_run(void) {
    slotcask_init(64, 64);
    scenario1_incident(1);
    scenario2_interior_run(2);
    scenario3_tail_run(3);
    scenario4_tombstone_pin(4);
    scenario5_gap_then_grave(5);
    scenario6_far_island(6);
    scenario7_hash_failed_grave(7);
    scenario8_no_materialize(8);
    scenario9_spanning_hole(9);
    scenario10_fail_closed(10);
    slotcask_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-grave-run-frontier",
              test_grave_run_frontier_run)
