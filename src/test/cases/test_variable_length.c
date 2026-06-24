/* test_variable_length.c — slotcask variable-length record format.
 *
 * Exercises offline fixed→varlen migration, verifies data survives
 * close+reopen, and confirms new varlen writes work correctly.
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
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

static void rm_rf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_varlen_test_%d_%ld",
             base, (int)getpid(), (long)time(NULL));
}

static int test_variable_length_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);

    slotcask_init(16, 16);

    /* ---------------------------------------------------------------
     * Phase 1: create a fixed-format DB, insert records, verify reads.
     * ------------------------------------------------------------- */
    SlotcaskDb db;
    int rc = slotcask_open(&db, dir, 8, 4, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open (fixed) succeeds");
    ASSERT_EQ_INT(db.format, SLOTCASK_FORMAT_FIXED, "default format is FIXED");

    int inserted = 0;
    for (int i = 0; i < 10; i++) {
        char key[32], val[200];
        snprintf(key, sizeof(key), "key_%03d", i);
        int vlen = 20 + (i * 7) % 120;
        for (int j = 0; j < vlen; j++) val[j] = (char)('A' + ((i + j) % 26));
        if (slotcask_insert(&db, -1, key, strlen(key), val, vlen) == 0) inserted++;
    }
    ASSERT_EQ_INT(inserted, 10, "10 inserts succeed");

    /* Verify every inserted key. */
    int ok = 0;
    for (int i = 0; i < 10; i++) {
        char key[32], expected[200];
        snprintf(key, sizeof(key), "key_%03d", i);
        int elen = 20 + (i * 7) % 120;
        for (int j = 0; j < elen; j++) expected[j] = (char)('A' + ((i + j) % 26));
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, key, strlen(key), &v, &vl) == 0) {
            if ((int)vl == elen && memcmp(v, expected, elen) == 0) ok++;
            free(v);
        }
    }
    ASSERT_EQ_INT(ok, 10, "all 10 inserted values match");

    slotcask_close(&db);

    /* ---------------------------------------------------------------
     * Phase 2: reopen fixed-format DB, run offline migration to varlen.
     * ------------------------------------------------------------- */
    rc = slotcask_open(&db, dir, 8, 4, 256);
    ASSERT_EQ_INT(rc, 0, "reopen succeeds before migration");
    ASSERT_EQ_INT(db.format, SLOTCASK_FORMAT_FIXED, "format still FIXED before migration");

    rc = slotcask_migrate_to_varlen(&db);
    ASSERT_EQ_INT(rc, 0, "migration succeeds");
    ASSERT_EQ_INT(db.format, SLOTCASK_FORMAT_VARIABLE, "format is VARIABLE after migration");

    slotcask_close(&db);

    /* ---------------------------------------------------------------
     * Phase 3: reopen as varlen, verify all data survived.
     * ------------------------------------------------------------- */
    rc = slotcask_open(&db, dir, 8, 4, 256);
    ASSERT_EQ_INT(rc, 0, "reopen after migration succeeds");
    ASSERT_EQ_INT(db.format, SLOTCASK_FORMAT_VARIABLE, "format persists across close+reopen");

    ok = 0;
    for (int i = 0; i < 10; i++) {
        char key[32], expected[200];
        snprintf(key, sizeof(key), "key_%03d", i);
        int elen = 20 + (i * 7) % 120;
        for (int j = 0; j < elen; j++) expected[j] = (char)('A' + ((i + j) % 26));
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, key, strlen(key), &v, &vl) == 0) {
            if ((int)vl == elen && memcmp(v, expected, elen) == 0) ok++;
            free(v);
        }
    }
    ASSERT_EQ_INT(ok, 10, "all 10 values readable after migration");

    /* ---------------------------------------------------------------
     * Phase 4: insert new records in varlen mode, verify they work.
     * ------------------------------------------------------------- */
    inserted = 0;
    for (int i = 100; i < 105; i++) {
        char key[32], val[200];
        snprintf(key, sizeof(key), "vkey_%03d", i);
        int vlen = 30 + (i * 13) % 90;
        for (int j = 0; j < vlen; j++) val[j] = (char)('a' + ((i * 3 + j) % 26));
        if (slotcask_insert(&db, -1, key, strlen(key), val, vlen) == 0) inserted++;
    }
    ASSERT_EQ_INT(inserted, 5, "5 varlen inserts succeed");

    ok = 0;
    for (int i = 100; i < 105; i++) {
        char key[32], expected[200];
        snprintf(key, sizeof(key), "vkey_%03d", i);
        int elen = 30 + (i * 13) % 90;
        for (int j = 0; j < elen; j++) expected[j] = (char)('a' + ((i * 3 + j) % 26));
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, key, strlen(key), &v, &vl) == 0) {
            if ((int)vl == elen && memcmp(v, expected, elen) == 0) ok++;
            free(v);
        }
    }
    ASSERT_EQ_INT(ok, 5, "all 5 varlen-inserted values match");

    /* ---------------------------------------------------------------
     * Phase 5: update a pre-migration key and verify in varlen mode.
     * ------------------------------------------------------------- */
    rc = slotcask_update(&db, -1, "key_003", 7, "UPDATED_VARLEN", 14);
    ASSERT_EQ_INT(rc, 0, "update pre-migration key in varlen mode succeeds");
    void *v = NULL; size_t vl = 0;
    rc = slotcask_get(&db, "key_003", 7, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "get updated key succeeds");
    int up_ok = (vl == 14 && v && memcmp(v, "UPDATED_VARLEN", 14) == 0);
    ASSERT_TRUE(up_ok, "updated value reflects varlen content");
    free(v);

    /* ---------------------------------------------------------------
     * Phase 6: delete a varlen-inserted key; verify pool reuse.
     * ------------------------------------------------------------- */
    rc = slotcask_delete(&db, "vkey_101", 8);
    ASSERT_EQ_INT(rc, 0, "delete varlen-inserted key succeeds");

    v = NULL; vl = 0;
    rc = slotcask_get(&db, "vkey_101", 8, &v, &vl);
    ASSERT_EQ_INT(rc, -1, "get after delete returns missing");
    if (v) free(v);

    /* Insert new key — should reuse the freed slot. */
    rc = slotcask_insert(&db, -1, "new_reuse", 9, "REUSED", 6);
    ASSERT_EQ_INT(rc, 0, "insert after delete in varlen mode succeeds (pool reuse)");
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "new_reuse", 9, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "reused key reads back");
    int reuse_ok = (vl == 6 && v && memcmp(v, "REUSED", 6) == 0);
    ASSERT_TRUE(reuse_ok, "reused value matches");
    free(v);

    /* ---------------------------------------------------------------
     * Phase 7: close+reopen varlen DB, confirm all data persists.
     * ------------------------------------------------------------- */
    slotcask_close(&db);

    rc = slotcask_open(&db, dir, 8, 4, 256);
    ASSERT_EQ_INT(rc, 0, "final reopen succeeds");
    ASSERT_EQ_INT(db.format, SLOTCASK_FORMAT_VARIABLE, "format still VARIABLE after second reopen");

    /* Original keys (now varlen) readable. */
    ok = 0;
    for (int i = 0; i < 10; i++) {
        if (i == 3) continue; /* updated — skip original check */
        char key[32], expected[200];
        snprintf(key, sizeof(key), "key_%03d", i);
        int elen = 20 + (i * 7) % 120;
        for (int j = 0; j < elen; j++) expected[j] = (char)('A' + ((i + j) % 26));
        void *vv = NULL; size_t vvl = 0;
        if (slotcask_get(&db, key, strlen(key), &vv, &vvl) == 0) {
            if ((int)vvl == elen && memcmp(vv, expected, elen) == 0) ok++;
            free(vv);
        }
    }
    ASSERT_EQ_INT(ok, 9, "9 original keys survive second reopen");

    /* Updated key survives. */
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "key_003", 7, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "updated key survives second reopen");
    int up2 = (vl == 14 && v && memcmp(v, "UPDATED_VARLEN", 14) == 0);
    ASSERT_TRUE(up2, "updated value survives second reopen");
    free(v);

    /* Varlen-inserted keys survive. */
    ok = 0;
    for (int i = 100; i < 105; i++) {
        if (i == 101) continue; /* deleted */
        char key[32], expected[200];
        snprintf(key, sizeof(key), "vkey_%03d", i);
        int elen = 30 + (i * 13) % 90;
        for (int j = 0; j < elen; j++) expected[j] = (char)('a' + ((i * 3 + j) % 26));
        void *vv = NULL; size_t vvl = 0;
        if (slotcask_get(&db, key, strlen(key), &vv, &vvl) == 0) {
            if ((int)vvl == elen && memcmp(vv, expected, elen) == 0) ok++;
            free(vv);
        }
    }
    ASSERT_EQ_INT(ok, 4, "4 varlen-inserted keys survive second reopen");

    /* Pool-reuse key survives. */
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "new_reuse", 9, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "pool-reuse key survives second reopen");
    int reuse2 = (vl == 6 && v && memcmp(v, "REUSED", 6) == 0);
    ASSERT_TRUE(reuse2, "pool-reuse value survives second reopen");
    free(v);

    slotcask_close(&db);
    rm_rf(dir);
    return 0;
}

TEST_REGISTER("test-variable-length", test_variable_length_run)
