/* src/test/cases/test_compact_donor_kf_sync.c
 * AUD-COMPACT-ORDER (docs/plans/2026-11-25-fix-resplit-compact-sync.md):
 * segment compaction repoints keyfile entries in the mapping and then
 * unlinks the donor file; the repointed entries must be durable first, or a
 * power loss leaves on-disk entries naming a deleted file. The keyfile sync
 * is forced to fail through SHARD_TEST_BMFIX_COMPACT_KFSYNC: compaction must
 * stop with the donor file still in place and every key readable; with the
 * fault cleared it completes and drops the donor.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "shard_test_ctl.h"
#include "varlen_compact_fixture.h"

#include <dirent.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int count_seg_files(const char *data_dir) {
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/data/streams/000", data_dir);
    DIR *d = opendir(dir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t l = strlen(e->d_name);
        if (l > 4 && strcmp(e->d_name + l - 4, ".dat") == 0) n++;
    }
    closedir(d);
    return n;
}

static int readable(SlotcaskDb *db, const char *key, size_t klen, const char *want, size_t wlen) {
    void *v = NULL; size_t vl = 0;
    int rc = slotcask_get(db, key, klen, &v, &vl);
    int ok = rc == 0 && vl == wlen && memcmp(v, want, wlen) == 0;
    free(v);
    return ok;
}

static int test_compact_donor_kf_sync(void) {
    char base[] = "/tmp/shard-db-cdk-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);
    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 8, 1, 8192), 0, "open db");
    if (t_ctx->failed) goto out;
    ASSERT_EQ_INT(varlen_compact_fixture_build(&db), 0, "fixture built");
    if (t_ctx->failed) goto close;

    int files_before = count_seg_files(base);
    atomic_store(&g_shard_test_bmfix_fault_mode, SHARD_TEST_BMFIX_COMPACT_KFSYNC);
    atomic_store(&g_shard_test_bmfix_fault_count, 1000);
    int dropped = 0;
    int rc = slotcask_compact_segs(&db, &dropped);
    atomic_store(&g_shard_test_bmfix_fault_mode, 0);
    ASSERT_TRUE(rc != 0, "compaction fails when the keyfile cannot be made durable");
    ASSERT_EQ_INT(count_seg_files(base), files_before, "the donor segment file is kept");
    ASSERT_TRUE(readable(&db, "kA", 2, "v", 1) && readable(&db, "kkeyC", 5, "cvalue", 6),
                "records stay readable after the failed compaction");

    dropped = 0;
    rc = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(rc, 0, "compaction succeeds once the keyfile sync works");
    ASSERT_TRUE(dropped >= 1 && count_seg_files(base) < files_before, "the donor is dropped");
    ASSERT_TRUE(readable(&db, "kA", 2, "v", 1) && readable(&db, "kkeyC", 5, "cvalue", 6),
                "records stay readable after the completed compaction");
close:
    slotcask_close(&db);
out:
    slotcask_test_set_seg_max_bytes(0);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-compact-donor-kf-sync", test_compact_donor_kf_sync)
