/* src/test/audit/audit_craft_grave_frontier.c
 *
 * Audit-campaign known-bug gate for the kf grave-frontier fix
 * (docs/plans/2026-11-19-audit-campaign.md Phase 1 step 4; human
 * decision 2026-11-19, option (b): craft-seeded deterministic mode).
 *
 * Carries its own copy of the incident geometry — the regression test
 * that shipped WITH the fix (test_slotcask_grave_run_frontier.c) does
 * not exist at the gate's base commit (163b36a), so this file must be
 * self-contained. Scenario: synthetic stream layout (splits=8,
 * streams=1, slot_size=120, 72-byte records at a 72-byte pitch), three
 * live records A,B,C written in-process, A/B deleted (in-place
 * tombstones), their flag bytes pwritten to 0 while the "daemon" is
 * down (the on-disk shape of a write whose window died between the
 * P-phase bytes and the marker — head-of-stream grave run of 144 bytes
 * > the 120-byte resync window, live record C beyond it), then the
 * normal cycle: close → reopen → validate_live_refs == 0 → frontier
 * assertions → append D,E,F past the recovered frontier → C must read
 * back byte-identical, refs must stay clean.
 *
 * RED on 163b36a (base parks reserve_off at 0; F's append crushes C;
 * dangling refs), GREEN on 2026.11.2 — deterministically, no seeds.
 * Pure in-process: no daemon, no wire, no timing.
 *
 * Audit case: excluded from default run-all (TEST_REGISTER_AUDIT).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "slotcask.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define REC  72  /* 24 hdr + 16 key + 28 val = 68 -> padded to 72 */
#define SLOT 120 /* the incident object's slot_size (players) */

static void gf_rmrf(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    if (system(cmd) != 0) {
        /* best effort */
    }
}

static void fresh_dir(char *dir, size_t cap, const char *tag) {
    snprintf(dir, cap, "/tmp/shard-db-audit-craft-%s-%d", tag,
             (int)getpid());
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
    ASSERT_EQ_INT((int)pwrite(fd, "\0", 1, off + 18), 1, desc);
    close(fd);
}

#define KLEN 16

static int ins(SlotcaskDb *db, const char *key, const void *val,
               size_t vlen) {
    return slotcask_insert_with_hooks(db, 0, key, KLEN, val, vlen,
                                      NULL, NULL);
}

static int del(SlotcaskDb *db, const char *key) {
    return slotcask_delete_with_hooks(db, key, KLEN, NULL, NULL);
}

static int read_is(SlotcaskDb *db, const char *key, const void *want,
                   size_t want_len) {
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
    uint64_t inv = 1;
    ASSERT_EQ_INT(slotcask_validate_live_refs(db, &inv), 0, desc);
    ASSERT_EQ_INT((int)inv, 0, desc);
}

static int open_db(SlotcaskDb *db, const char *dir, const char *desc) {
    memset(db, 0, sizeof(*db));
    int rc = slotcask_open(db, dir, 8, 1, SLOT);
    ASSERT_EQ_INT(rc, 0, desc);
    return rc;
}

/* Scenario 1 — the incident shape: two head graves (A,B), live C
 * behind them. Base parks reserve_off at 0; F's append clobbers C.
 * RED on 163b36a (frontier + clobber asserts), GREEN on the fix. */
static void scenario1_incident(const char *tag) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), tag);
    char va[REC - 44], vb[REC - 44], vc[REC - 44], vd[REC - 44],
         ve[REC - 44], vf[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vb, 'b', sizeof(vb));
    memset(vc, 'c', sizeof(vc)); memset(vd, 'd', sizeof(vd));
    memset(ve, 'e', sizeof(ve));
    memset(vf, 'f', sizeof(vf));

    SlotcaskDb db;
    if (open_db(&db, dir, "s1: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0,
                  "s1: insert A");
    ASSERT_EQ_INT(ins(&db, "key-B-0123456789ab", vb, sizeof(vb)), 0,
                  "s1: insert B");
    ASSERT_EQ_INT(ins(&db, "key-C-0123456789ab", vc, sizeof(vc)), 0,
                  "s1: insert C");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "s1: frontier after A,B,C is 216");
    ASSERT_EQ_INT(del(&db, "key-A-0123456789ab"), 0, "s1: delete A");
    ASSERT_EQ_INT(del(&db, "key-B-0123456789ab"), 0, "s1: delete B");
    slotcask_close(&db);

    craft_grave(dir, 0, "s1: grave A");
    craft_grave(dir, REC, "s1: grave B");

    if (open_db(&db, dir, "s1: reopen with head grave run") != 0) {
        gf_rmrf(dir);
        return;
    }
    assert_refs_clean(&db, "s1: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "s1: frontier parks past the head grave run at 216 "
                "(base parks 0)");
    ASSERT_TRUE(read_is(&db, "key-C-0123456789ab", vc, sizeof(vc)),
                "s1: C reads back after reopen");

    ASSERT_EQ_INT(ins(&db, "key-D-0123456789ab", vd, sizeof(vd)), 0,
                  "s1: insert D past the frontier");
    ASSERT_EQ_INT(ins(&db, "key-E-0123456789ab", ve, sizeof(ve)), 0,
                  "s1: insert E");
    ASSERT_EQ_INT(ins(&db, "key-F-0123456789ab", vf, sizeof(vf)), 0,
                  "s1: insert F");
    ASSERT_TRUE(db.streams[0].reserve_off == 6 * REC,
                "s1: frontier after D,E,F is 432");
    ASSERT_TRUE(read_is(&db, "key-C-0123456789ab", vc, sizeof(vc)),
                "s1: C NOT clobbered by the appends (base: crushed)");
    ASSERT_TRUE(read_is(&db, "key-D-0123456789ab", vd, sizeof(vd)),
                "s1: D reads back");
    ASSERT_TRUE(read_is(&db, "key-E-0123456789ab", ve, sizeof(ve)),
                "s1: E reads back");
    ASSERT_TRUE(read_is(&db, "key-F-0123456789ab", vf, sizeof(vf)),
                "s1: F reads back");
    assert_refs_clean(&db, "s1: refs clean after appends");
    ASSERT_TRUE(absent(&db, "key-A-0123456789ab"), "s1: A stays deleted");
    ASSERT_TRUE(absent(&db, "key-B-0123456789ab"), "s1: B stays deleted");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 2 — interior grave run between live records: A, [grave X],
 * [grave Y], B. Base parks at 72; the frontier assert fails. */
static void scenario2_interior(const char *tag) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), tag);
    char va[REC - 44], vx[REC - 44], vy[REC - 44], vb[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy)); memset(vb, 'b', sizeof(vb));

    SlotcaskDb db;
    if (open_db(&db, dir, "s2: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0,
                  "s2: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0,
                  "s2: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0,
                  "s2: insert Y");
    ASSERT_EQ_INT(ins(&db, "key-B-0123456789ab", vb, sizeof(vb)), 0,
                  "s2: insert B");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "s2: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "s2: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "s2: grave X");
    craft_grave(dir, 2 * REC, "s2: grave Y");

    if (open_db(&db, dir, "s2: reopen with interior grave run") != 0) {
        gf_rmrf(dir);
        return;
    }
    assert_refs_clean(&db, "s2: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 4 * REC,
                "s2: frontier past the interior grave run at 288");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "s2: A reads back");
    ASSERT_TRUE(read_is(&db, "key-B-0123456789ab", vb, sizeof(vb)),
                "s2: B reads back");
    ASSERT_EQ_INT(ins(&db, "key-C-0123456789ab", vb, sizeof(vb)), 0,
                  "s2: insert C after reopen");
    ASSERT_TRUE(read_is(&db, "key-B-0123456789ab", vb, sizeof(vb)),
                "s2: B not clobbered by the append");

    slotcask_close(&db);
    gf_rmrf(dir);
}

/* Scenario 3 — grave run at the true tail. Base parks at 72. */
static void scenario3_tail(const char *tag) {
    char dir[256];
    fresh_dir(dir, sizeof(dir), tag);
    char va[REC - 44], vx[REC - 44], vy[REC - 44];
    memset(va, 'a', sizeof(va)); memset(vx, 'x', sizeof(vx));
    memset(vy, 'y', sizeof(vy));

    SlotcaskDb db;
    if (open_db(&db, dir, "s3: open") != 0) { gf_rmrf(dir); return; }
    ASSERT_EQ_INT(ins(&db, "key-A-0123456789ab", va, sizeof(va)), 0,
                  "s3: insert A");
    ASSERT_EQ_INT(ins(&db, "key-X-0123456789ab", vx, sizeof(vx)), 0,
                  "s3: insert X");
    ASSERT_EQ_INT(ins(&db, "key-Y-0123456789ab", vy, sizeof(vy)), 0,
                  "s3: insert Y");
    ASSERT_EQ_INT(del(&db, "key-X-0123456789ab"), 0, "s3: delete X");
    ASSERT_EQ_INT(del(&db, "key-Y-0123456789ab"), 0, "s3: delete Y");
    slotcask_close(&db);

    craft_grave(dir, REC, "s3: grave X");
    craft_grave(dir, 2 * REC, "s3: grave Y");

    if (open_db(&db, dir, "s3: reopen with tail grave run") != 0) {
        gf_rmrf(dir);
        return;
    }
    assert_refs_clean(&db, "s3: refs clean at reopen");
    ASSERT_TRUE(db.streams[0].reserve_off == 3 * REC,
                "s3: frontier past the tail grave run at 216");
    ASSERT_TRUE(read_is(&db, "key-A-0123456789ab", va, sizeof(va)),
                "s3: A reads back");

    slotcask_close(&db);
    gf_rmrf(dir);
}

static int audit_craft_grave_frontier_run(void) {
    scenario1_incident("s1");
    scenario2_interior("s2");
    scenario3_tail("s3");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER_AUDIT("audit-craft-grave-frontier",
                    audit_craft_grave_frontier_run)
