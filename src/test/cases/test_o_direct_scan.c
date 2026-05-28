/* test_o_direct_scan.c — bit-equality test for seg_scan_o_direct.
 *
 * Inserts ~100 records into a SlotcaskDb (in-process, no daemon), then:
 *   Pass A: collects all live record hash16s via slotcask_walk_live (mmap path)
 *   Pass B: collects all live record hash16s via seg_scan_o_direct (O_DIRECT path)
 *   Assert: count(A) == count(B) and every hash16 in A is in B (and vice versa).
 *
 * Also exercises btree_leaf_scan_o_direct on a small btree built with
 * btree_bulk_build, asserting the scan returns the same entries as the
 * mmap-based btree_walk_all_values path.
 *
 * Standalone: no daemon, no TCP, no schema.conf.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "btree.h"
#include "io_direct.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

/* ---- helpers ---- */

static void rm_rf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_odirect_test_%d_%ld",
             base, (int)getpid(), (long)time(NULL));
}

/* ---- hash16 set (open-addressed, no resize needed for 200 entries) ---- */

#define HSET_CAP 1024
typedef struct {
    uint8_t h[16];
    int     used;
} HSetSlot;

typedef struct {
    HSetSlot slots[HSET_CAP];
    int      count;
} HSet;

static void hset_init(HSet *s) { memset(s, 0, sizeof(*s)); }

static uint32_t hset_hash(const uint8_t h[16]) {
    uint32_t v;
    memcpy(&v, h, 4);
    return v;
}

static void hset_insert(HSet *s, const uint8_t h[16]) {
    uint32_t idx = hset_hash(h) & (HSET_CAP - 1);
    for (int i = 0; i < HSET_CAP; i++) {
        HSetSlot *sl = &s->slots[(idx + (uint32_t)i) & (HSET_CAP - 1)];
        if (!sl->used) {
            memcpy(sl->h, h, 16);
            sl->used = 1;
            s->count++;
            return;
        }
        if (memcmp(sl->h, h, 16) == 0) return; /* duplicate */
    }
}

static int hset_contains(const HSet *s, const uint8_t h[16]) {
    uint32_t idx = hset_hash(h) & (HSET_CAP - 1);
    for (int i = 0; i < HSET_CAP; i++) {
        const HSetSlot *sl = &s->slots[(idx + (uint32_t)i) & (HSET_CAP - 1)];
        if (!sl->used) return 0;
        if (memcmp(sl->h, h, 16) == 0) return 1;
    }
    return 0;
}

/* ---- mmap walk callback ---- */

typedef struct {
    HSet *set;
    int   count;
} WalkCtx;

static int mmap_walk_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *raw_ctx) {
    (void)key; (void)klen; (void)value; (void)vlen;
    WalkCtx *wc = (WalkCtx *)raw_ctx;
    hset_insert(wc->set, hash16);
    wc->count++;
    return 0;
}

/* ---- O_DIRECT seg scan callback ---- */

typedef struct {
    HSet *set;
    int   count;
} OdCtx;

static int od_seg_cb(const uint8_t *rec, size_t vlen,
                     const uint8_t hash16[16], void *raw_ctx) {
    (void)rec; (void)vlen;
    OdCtx *oc = (OdCtx *)raw_ctx;
    hset_insert(oc->set, hash16);
    oc->count++;
    return 0;
}

/* ---- enumerate segment files under a SlotcaskDb data_dir ---- */

typedef struct {
    char paths[256][PATH_MAX];
    int  count;
} SegList;

static void collect_seg_files(const char *data_dir, SegList *out) {
    out->count = 0;
    /* Segment files live at: <data_dir>/data/streams/<NNN>/<NNNNNN>.dat */
    char streams_dir[PATH_MAX];
    snprintf(streams_dir, sizeof(streams_dir), "%s/data/streams", data_dir);

    DIR *d = opendir(streams_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d)) && out->count < 256) {
        if (e->d_name[0] == '.') continue;
        /* Each entry is a stream directory e.g. "000", "001". */
        char sdir[PATH_MAX];
        snprintf(sdir, sizeof(sdir), "%s/%s", streams_dir, e->d_name);
        DIR *d2 = opendir(sdir);
        if (!d2) continue;
        struct dirent *f;
        while ((f = readdir(d2)) && out->count < 256) {
            if (f->d_name[0] == '.') continue;
            size_t nlen = strlen(f->d_name);
            if (nlen < 5 || strcmp(f->d_name + nlen - 4, ".dat") != 0)
                continue;
            snprintf(out->paths[out->count], PATH_MAX,
                     "%s/%s", sdir, f->d_name);
            out->count++;
        }
        closedir(d2);
    }
    closedir(d);
}

/* ====================================================================
 * SEG SCAN TEST
 * ==================================================================== */

static int test_seg_scan_bit_equality(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);
    slotcask_init(32, 32);

    SlotcaskDb db;
    /* slot_size = 256: 24B header + up to 232B of key+value. */
    int rc = slotcask_open(&db, dir, /*shards*/8, /*streams*/2,
                           /*slot_size*/256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds");

    /* Insert 100 records. */
    const int N = 100;
    int inserted = 0;
    for (int i = 0; i < N; i++) {
        char key[32], val[128];
        snprintf(key, sizeof(key), "key_%04d", i);
        int vlen = 32 + (i % 64);
        for (int j = 0; j < vlen; j++) val[j] = (char)('A' + ((i + j) % 26));
        if (slotcask_insert(&db, -1, key, strlen(key), val, (size_t)vlen) == 0)
            inserted++;
    }
    ASSERT_EQ_INT(inserted, N, "all 100 records inserted");

    /* Delete 10 of them to create tombstones that the scan must skip. */
    int deleted = 0;
    for (int i = 0; i < 10; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%04d", i * 7 % N);
        if (slotcask_delete(&db, key, strlen(key)) == 0)
            deleted++;
    }
    /* deleted may be < 10 if some keys collided; we just assert > 0. */
    ASSERT_TRUE(deleted > 0, "some records deleted (tombstones created)");

    /* Pass A: mmap walk via slotcask_walk_live. */
    HSet mmap_set;
    hset_init(&mmap_set);
    WalkCtx wc = { &mmap_set, 0 };
    int wrc = slotcask_walk_live(&db, mmap_walk_cb, &wc);
    ASSERT_EQ_INT(wrc, 0, "slotcask_walk_live returns 0");
    int mmap_count = wc.count;
    ASSERT_TRUE(mmap_count > 0, "mmap walk found live records");
    ASSERT_TRUE(mmap_count == N - deleted,
                "mmap walk count matches inserted minus deleted");

    /* Collect seg files. */
    SegList segs;
    collect_seg_files(db.data_dir, &segs);
    ASSERT_TRUE(segs.count > 0, "found at least one segment file");

    /* Pass B: O_DIRECT scan across all segment files. */
    HSet od_set;
    hset_init(&od_set);
    OdCtx oc = { &od_set, 0 };
    for (int i = 0; i < segs.count; i++) {
        int src = seg_scan_o_direct(segs.paths[i], db.slot_size, od_seg_cb, &oc);
        ASSERT_TRUE(src >= 0, "seg_scan_o_direct succeeds on seg file");
    }
    int od_count = oc.count;

    /* Assert counts match. */
    ASSERT_EQ_INT(od_count, mmap_count,
                  "O_DIRECT scan: live record count matches mmap walk");

    /* Assert every mmap hash16 appears in the O_DIRECT set. */
    int all_found = 1;
    for (int s = 0; s < HSET_CAP; s++) {
        if (!mmap_set.slots[s].used) continue;
        if (!hset_contains(&od_set, mmap_set.slots[s].h)) {
            all_found = 0;
            break;
        }
    }
    ASSERT_TRUE(all_found, "every mmap hash16 found in O_DIRECT set");

    /* Assert every O_DIRECT hash16 appears in the mmap set. */
    int no_extra = 1;
    for (int s = 0; s < HSET_CAP; s++) {
        if (!od_set.slots[s].used) continue;
        if (!hset_contains(&mmap_set, od_set.slots[s].h)) {
            no_extra = 0;
            break;
        }
    }
    ASSERT_TRUE(no_extra, "no extra hash16s in O_DIRECT set");

    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return 0;
}

/* ====================================================================
 * BTREE LEAF SCAN TEST
 * ==================================================================== */

/* Btree mmap walk callback: collects values into a simple array. */
typedef struct {
    char   vals[200][BT_MAX_VAL_LEN];
    size_t vlens[200];
    int    count;
} BtWalkCtx;

static int bt_mmap_cb(const char *value, size_t vlen, void *raw_ctx) {
    BtWalkCtx *bc = (BtWalkCtx *)raw_ctx;
    if (bc->count >= 200) return 0;
    size_t take = (vlen < BT_MAX_VAL_LEN) ? vlen : (BT_MAX_VAL_LEN - 1);
    memcpy(bc->vals[bc->count], value, take);
    bc->vlens[bc->count] = take;
    bc->count++;
    return 0;
}

/* Btree O_DIRECT leaf callback: same collection. */
typedef struct {
    char   vals[200][BT_MAX_VAL_LEN];
    size_t vlens[200];
    int    count;
} BtOdCtx;

static int bt_od_cb(const uint8_t *value, size_t vlen,
                    const uint8_t hash16[16], void *raw_ctx) {
    (void)hash16;
    BtOdCtx *bc = (BtOdCtx *)raw_ctx;
    if (bc->count >= 200) return 0;
    size_t take = (vlen < BT_MAX_VAL_LEN) ? vlen : (BT_MAX_VAL_LEN - 1);
    memcpy(bc->vals[bc->count], value, take);
    bc->vlens[bc->count] = take;
    bc->count++;
    return 0;
}

static int cmp_str_entry(const void *a, const void *b) {
    const BtEntry *ea = (const BtEntry *)a;
    const BtEntry *eb = (const BtEntry *)b;
    size_t minl = ea->vlen < eb->vlen ? ea->vlen : eb->vlen;
    int c = memcmp(ea->value, eb->value, minl);
    if (c) return c;
    if (ea->vlen < eb->vlen) return -1;
    if (ea->vlen > eb->vlen) return 1;
    return 0;
}

static int test_btree_leaf_scan_bit_equality(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);

    int mkrc = mkdir(dir, 0755);
    if (mkrc != 0 && errno != EEXIST) {
        /* Can't create dir — skip gracefully. */
        return 0;
    }

    char bt_path[PATH_MAX];
    snprintf(bt_path, sizeof(bt_path), "%s/test.idx", dir);

    /* Build a small btree with 50 entries. */
    const int BN = 50;
    BtEntry entries[BN];
    /* Use fixed dummy hashes (all zeros except entry index in byte 0). */
    static char keys[50][32];
    static uint8_t hashes[50][16];
    for (int i = 0; i < BN; i++) {
        snprintf(keys[i], sizeof(keys[i]), "val_%04d", i);
        entries[i].value = keys[i];
        entries[i].vlen  = strlen(keys[i]);
        memset(hashes[i], 0, 16);
        hashes[i][0] = (uint8_t)i;
        memcpy(entries[i].hash, hashes[i], 16);
    }
    /* btree_bulk_build requires sorted input. */
    qsort(entries, (size_t)BN, sizeof(BtEntry), cmp_str_entry);

    btree_bulk_build(bt_path, entries, (size_t)BN);

    /* Pass A: mmap walk via btree_walk_all_values. */
    BtWalkCtx mmap_bc;
    memset(&mmap_bc, 0, sizeof(mmap_bc));
    int wrc = btree_walk_all_values(bt_path, bt_mmap_cb, &mmap_bc);
    ASSERT_EQ_INT(wrc, 0, "btree_walk_all_values returns 0");
    ASSERT_EQ_INT(mmap_bc.count, BN, "mmap walk found all btree entries");

    /* Pass B: O_DIRECT leaf scan. */
    BtOdCtx od_bc;
    memset(&od_bc, 0, sizeof(od_bc));
    int odrc = btree_leaf_scan_o_direct(bt_path, bt_od_cb, &od_bc);
    ASSERT_TRUE(odrc >= 0, "btree_leaf_scan_o_direct returns >= 0");
    ASSERT_EQ_INT(od_bc.count, mmap_bc.count,
                  "O_DIRECT btree leaf scan entry count matches mmap walk");

    /* Assert each value present in mmap walk is in the O_DIRECT scan.
       Both walks emit in btree order (ASC), so we compare sorted sets. */
    int all_found = 1;
    for (int i = 0; i < mmap_bc.count; i++) {
        /* Search for mmap_bc.vals[i] in od_bc.vals[]. */
        int found = 0;
        for (int j = 0; j < od_bc.count; j++) {
            if (mmap_bc.vlens[i] == od_bc.vlens[j] &&
                memcmp(mmap_bc.vals[i], od_bc.vals[j], mmap_bc.vlens[i]) == 0) {
                found = 1; break;
            }
        }
        if (!found) { all_found = 0; break; }
    }
    ASSERT_TRUE(all_found, "every mmap btree value found in O_DIRECT scan");

    rm_rf(dir);
    return 0;
}

/* ====================================================================
 * Combined test function
 * ==================================================================== */

static int test_o_direct_scan_bit_equality(void) {
    int rc;
    rc = test_seg_scan_bit_equality();
    if (rc) return rc;
    rc = test_btree_leaf_scan_bit_equality();
    return rc;
}

TEST_REGISTER("test-o-direct-scan-bit-equality", test_o_direct_scan_bit_equality)
