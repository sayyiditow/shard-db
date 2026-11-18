/* Regression/guard: backup/restore's copy_file must preserve sparseness
 * — docs/plans/2026-11-17-kf-grave-frontier-fix.md, Task 11 (M1).
 *
 * copy_file (query_maint.c) wrote every byte, so restored segments were
 * fully allocated and the hole-aware wide resync then pays ~155 ms per
 * 8-stream object on open (vs ~4 ms sparse). The sparse path walks the
 * source with lseek(SEEK_DATA/SEEK_HOLE), copies only data extents and
 * finishes with ftruncate, gated on od_seek_data_trusted (a dirty
 * unflushed page is never skipped).
 *
 * FAILS on base: the dense copy fully allocates the destination, so the
 * sparse-allocation assertions fail.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "query_internal.h"
#include "io_direct.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define BIG_SIZE (128LL * 1024 * 1024)
#define HOLE_SIZE (16LL * 1024 * 1024)

static int cmp_files_identical(const char *a, const char *b) {
    int fa = open(a, O_RDONLY);
    int fb = open(b, O_RDONLY);
    if (fa < 0 || fb < 0) { close(fa); close(fb); return 0; }
    static char buf_a[1024 * 1024];
    static char buf_b[1024 * 1024];
    for (;;) {
        ssize_t na = read(fa, buf_a, sizeof(buf_a));
        ssize_t nb = read(fb, buf_b, sizeof(buf_b));
        if (na != nb) { close(fa); close(fb); return 0; }
        if (na == 0) break;
        if (memcmp(buf_a, buf_b, (size_t)na) != 0) { close(fa); close(fb); return 0; }
    }
    close(fa);
    close(fb);
    return 1;
}

static void make_sparse_big(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    ASSERT_TRUE(fd >= 0, "create 128 MiB sparse source");
    if (fd < 0) return;
    char page[4096];
    memset(page, 'A', sizeof(page));
    ASSERT_EQ_INT((int)pwrite(fd, page, sizeof(page), 0), (int)sizeof(page),
                  "write data at 0");
    memset(page, 'B', sizeof(page));
    ASSERT_EQ_INT((int)pwrite(fd, page, sizeof(page), 64LL * 1024 * 1024 + 100),
                  (int)sizeof(page), "write data at 64 MiB + 100");
    ASSERT_EQ_INT((int)pwrite(fd, "C", 1, BIG_SIZE - 1), 1,
                  "write last byte");
    ASSERT_EQ_INT(ftruncate(fd, BIG_SIZE), 0, "ftruncate source to 128 MiB");
    close(fd);
}

static void make_all_hole(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    ASSERT_TRUE(fd >= 0, "create 16 MiB all-hole source");
    if (fd < 0) return;
    ASSERT_EQ_INT(ftruncate(fd, HOLE_SIZE), 0, "ftruncate hole source to 16 MiB");
    close(fd);
}

static int test_backup_copy_sparse_run(void) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/shard-db-backup-sparse-%d", (int)getpid());
    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", dir);
    system(rmcmd);
    mkdir(dir, 0755);

    char src_big[512], dst_big[512], src_hole[512], dst_hole[512];
    snprintf(src_big, sizeof(src_big), "%s/src_big", dir);
    snprintf(dst_big, sizeof(dst_big), "%s/dst_big", dir);
    snprintf(src_hole, sizeof(src_hole), "%s/src_hole", dir);
    snprintf(dst_hole, sizeof(dst_hole), "%s/dst_hole", dir);

    make_sparse_big(src_big);
    make_all_hole(src_hole);

    /* Is SEEK_DATA trusted where these files live? The sparse copy gates
       on this; when untrusted the dense fallback is correct, so skip the
       allocation assertions. */
    int probe_fd = open(src_big, O_RDONLY);
    ASSERT_TRUE(probe_fd >= 0, "open source for trust probe");
    int trusted = probe_fd >= 0 ? od_seek_data_trusted(probe_fd, src_big) : 0;
    if (probe_fd >= 0) close(probe_fd);

    maint_test_copy_file(src_big, dst_big);
    maint_test_copy_file(src_hole, dst_hole);

    struct stat sb, db_, sh, dh;
    ASSERT_EQ_INT(stat(src_big, &sb), 0, "stat big source");
    ASSERT_EQ_INT(stat(dst_big, &db_), 0, "stat big destination");
    ASSERT_EQ_INT(stat(src_hole, &sh), 0, "stat hole source");
    ASSERT_EQ_INT(stat(dst_hole, &dh), 0, "stat hole destination");
    ASSERT_EQ_INT((int)(db_.st_size - sb.st_size), 0,
                  "128 MiB copy has the same size");
    ASSERT_EQ_INT((int)(dh.st_size - sh.st_size), 0,
                  "16 MiB all-hole copy has the same size");
    ASSERT_TRUE(cmp_files_identical(src_big, dst_big),
                "128 MiB copy is byte-identical");
    ASSERT_TRUE(cmp_files_identical(src_hole, dst_hole),
                "16 MiB all-hole copy is byte-identical");

    if (trusted) {
        off_t alloc_big = (off_t)db_.st_blocks * 512;
        off_t alloc_hole = (off_t)dh.st_blocks * 512;
        /* Dense would be 128 MiB / 16 MiB; sparse keeps the three small
           data regions (tmpfs huge pages may instantiate one 2 MiB page). */
        ASSERT_TRUE(alloc_big < 8LL * 1024 * 1024,
                    "128 MiB copy allocation stays sparse");
        ASSERT_TRUE(alloc_hole < 1LL * 1024 * 1024,
                    "all-hole copy allocation stays zero-ish");
    } else {
        TAP_DIAG("# SKIP allocation assertions: SEEK_DATA not trusted here\n");
    }

    /* Dense fallback after a failed sparse attempt must truncate whatever
       the sparse pass scribbled (here: junk past EOF). Needs the sparse
       path to be reachable, i.e. SEEK_DATA trusted. */
    if (trusted) {
        char dst_fb[600];
        snprintf(dst_fb, sizeof(dst_fb), "%s/fallback.dat", dir);
        maint_test_copy_partial_fail(1);
        maint_test_copy_file(src_hole, dst_fb);
        maint_test_copy_partial_fail(0);
        struct stat sf2;
        ASSERT_EQ_INT(stat(dst_fb, &sf2), 0, "stat fallback destination");
        ASSERT_EQ_INT((int)(sf2.st_size - sh.st_size), 0,
                      "dense fallback truncates sparse-attempt junk (size)");
        ASSERT_TRUE(cmp_files_identical(src_hole, dst_fb),
                    "dense fallback copy is byte-identical");
    }

    system(rmcmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-backup-copy-sparse", test_backup_copy_sparse_run)
