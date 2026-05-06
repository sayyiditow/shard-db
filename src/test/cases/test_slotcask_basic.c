/* test_slotcask_basic.c — Phase-1 unit test for the slotcask storage engine.
 *
 * Standalone: calls slotcask_* directly. No daemon, no TCP, no schema.conf.
 * Exercises insert / get / update / delete + close-and-reopen durability +
 * pool reuse on delete-then-insert.
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
    /* Allow override (mirrors SHARD_TEST_TMPDIR convention from fixtures). */
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_slotcask_test_%d_%ld",
             base, (int)getpid(), (long)time(NULL));
}

/* Walk the directory recursively to confirm files were created. Returns 1 if
   at least one file matching `suffix` exists; 0 otherwise. */
static int has_file_with_suffix(const char *dir, const char *suffix) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "find %s -name '*%s' 2>/dev/null | head -1", dir, suffix);
    FILE *fp = popen(cmd, "r");
    if (!fp) return 0;
    char buf[1024];
    int found = (fgets(buf, sizeof(buf), fp) != NULL && buf[0] != '\0');
    pclose(fp);
    return found;
}

static int test_slotcask_basic_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);

    /* Phase-1 caches are global and process-scoped. Initialize with a small
       cap (16) to exercise the LRU path even with a tiny test. */
    slotcask_init(16, 16);

    SlotcaskDb db;
    int rc = slotcask_open(&db, dir,
                           /*shards*/ 8,
                           /*streams*/ 4,
                           /*slot_size*/ 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds");

    /* Insert 10 records, varying value sizes. */
    int inserted = 0;
    for (int i = 0; i < 10; i++) {
        char key[32], val[200];
        snprintf(key, sizeof(key), "user_%03d", i);
        int vlen = 50 + (i * 11) % 100;
        for (int j = 0; j < vlen; j++) val[j] = (char)('a' + ((i + j) % 26));
        if (slotcask_insert(&db, -1, key, strlen(key), val, vlen) == 0) inserted++;
    }
    ASSERT_EQ_INT(inserted, 10, "all 10 inserts succeed");

    /* Get each one back, verify value matches. */
    int got = 0, value_ok = 0;
    for (int i = 0; i < 10; i++) {
        char key[32], expected[200];
        snprintf(key, sizeof(key), "user_%03d", i);
        int elen = 50 + (i * 11) % 100;
        for (int j = 0; j < elen; j++) expected[j] = (char)('a' + ((i + j) % 26));
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, key, strlen(key), &v, &vl) == 0) {
            got++;
            if ((int)vl == elen && memcmp(v, expected, elen) == 0) value_ok++;
            free(v);
        }
    }
    ASSERT_EQ_INT(got, 10, "all 10 gets succeed");
    ASSERT_EQ_INT(value_ok, 10, "all 10 values match");

    /* Duplicate insert returns -2 (already exists). */
    int dup_rc = slotcask_insert(&db, -1, "user_005", 8, "xxx", 3);
    ASSERT_EQ_INT(dup_rc, -2, "duplicate insert returns -2");

    /* Update one key; get returns the new value. */
    rc = slotcask_update(&db, -1, "user_003", 8, "NEW_VALUE", 9);
    ASSERT_EQ_INT(rc, 0, "update succeeds");
    void *v = NULL; size_t vl = 0;
    rc = slotcask_get(&db, "user_003", 8, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "get after update succeeds");
    int new_match = (vl == 9 && v && memcmp(v, "NEW_VALUE", 9) == 0);
    ASSERT_TRUE(new_match, "updated value reflects new content");
    free(v);

    /* Delete two keys; subsequent gets fail. */
    rc = slotcask_delete(&db, "user_007", 8);
    ASSERT_EQ_INT(rc, 0, "delete user_007 succeeds");
    rc = slotcask_delete(&db, "user_008", 8);
    ASSERT_EQ_INT(rc, 0, "delete user_008 succeeds");

    v = NULL; vl = 0;
    rc = slotcask_get(&db, "user_007", 8, &v, &vl);
    ASSERT_EQ_INT(rc, -1, "get after delete returns missing");
    if (v) free(v);

    /* Insert a fresh key after deletes — should reuse a pool slot. */
    rc = slotcask_insert(&db, -1, "user_999", 8, "REUSE", 5);
    ASSERT_EQ_INT(rc, 0, "insert after delete (snake-game reuse) succeeds");
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "user_999", 8, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "reused-slot key reads back");
    int reuse_match = (vl == 5 && v && memcmp(v, "REUSE", 5) == 0);
    ASSERT_TRUE(reuse_match, "reused-slot value matches");
    free(v);

    /* Confirm the on-disk shape: at least one keyfile + one segment exist. */
    int has_kf = has_file_with_suffix(dir, ".kf");
    int has_dat = has_file_with_suffix(dir, ".dat");
    ASSERT_TRUE(has_kf, "keyfile_NNN.kf created on disk");
    ASSERT_TRUE(has_dat, "data_NNNNNN.dat created on disk");

    /* Close + reopen; verify durability by re-reading user_000 .. user_006. */
    slotcask_close(&db);
    /* Caches stay alive across close — that's the whole point — so the
       open below will pick up the existing files. */

    rc = slotcask_open(&db, dir, 8, 4, 256);
    ASSERT_EQ_INT(rc, 0, "reopen after close succeeds");

    int durable_ok = 0;
    for (int i = 0; i < 7; i++) {
        if (i == 3) continue;  /* updated; skip the original-value check */
        char key[32], expected[200];
        snprintf(key, sizeof(key), "user_%03d", i);
        int elen = 50 + (i * 11) % 100;
        for (int j = 0; j < elen; j++) expected[j] = (char)('a' + ((i + j) % 26));
        void *vv = NULL; size_t vvl = 0;
        if (slotcask_get(&db, key, strlen(key), &vv, &vvl) == 0) {
            if ((int)vvl == elen && memcmp(vv, expected, elen) == 0) durable_ok++;
            free(vv);
        }
    }
    ASSERT_EQ_INT(durable_ok, 6, "6 untouched keys survived close+reopen");

    /* Updated key still reads NEW_VALUE after reopen. */
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "user_003", 8, &v, &vl);
    ASSERT_EQ_INT(rc, 0, "updated key gets after reopen");
    int dur_match = (vl == 9 && v && memcmp(v, "NEW_VALUE", 9) == 0);
    ASSERT_TRUE(dur_match, "updated value persists across reopen");
    free(v);

    /* Deleted keys still missing after reopen. */
    v = NULL; vl = 0;
    rc = slotcask_get(&db, "user_007", 8, &v, &vl);
    ASSERT_EQ_INT(rc, -1, "deleted key still missing after reopen");
    if (v) free(v);

    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return 0;
}

TEST_REGISTER("test-slotcask-basic", test_slotcask_basic_run)
