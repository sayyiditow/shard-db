/* src/test/cases/test_varlen_compact_crash_mid_migration.c
 *
 * Regression tests for crash recovery during varlen compaction. Exercises
 * two pause points added by Tasks 4a/4b:
 *
 *   compact-after-recipient-sync — record synced to recipient, kf entry
 *     still points at donor.  A crash here leaves the live record at its
 *     original donor slot while the recipient copy is already durable.
 *     Recovery must not lose the record or duplicate it.
 *
 *   compact-after-kf-repoint — kf entry repointed to recipient, donor
 *     not yet erased.  A crash here leaves a dangling donor copy.
 *     Recovery must complete the migration or roll back cleanly.
 *
 * Uses the durability_test_pause marker-file convention: the child
 * configures g_db->durability_test_pause_phase and _ms, the parent
 * polls for the .durability-test-<phase>.active marker, then sends
 * SIGKILL.  After each simulated crash the parent reopens the database,
 * asserts data integrity, and runs a fresh slotcask_compact_segs to
 * prove the post-recovery state is self-consistent.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "varlen_compact_fixture.h"

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

/* Poll for the durability_test_pause marker.  present=1 waits for the
   marker to appear; present=0 waits for it to be cleaned up. */
static int wait_marker(const char *data_dir, const char *phase,
                       int present, int timeout_ms) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.durability-test-%s.active",
             data_dir, phase);
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 50) {
        struct stat st;
        if ((stat(path, &st) == 0) == present) return 0;
        usleep(50000);
    }
    return -1;
}

static void compact_child_main(const char *data_dir, const char *phase) {
    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int rc = slotcask_open(&db, data_dir, 8, 1, 8192);
    if (rc != 0) _exit(10);
    rc = slotcask_migrate_to_varlen(&db);
    if (rc != 0) { slotcask_close(&db); _exit(11); }
    rc = varlen_compact_fixture_build(&db);
    if (rc != 0) { slotcask_close(&db); _exit(12); }

    /* Capture the authoritative pre-crash live-KF count.  The fixture's
       filler records are tombstoned before compaction; comparing this count
       after recovery catches both duplicate publication and resurrection. */
    char baseline_path[PATH_MAX];
    snprintf(baseline_path, sizeof(baseline_path),
             "%s/.compact-child-baseline", data_dir);
    int baseline_fd = open(baseline_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (baseline_fd < 0) { slotcask_close(&db); _exit(13); }
    char baseline_buf[64];
    int baseline_n = snprintf(baseline_buf, sizeof(baseline_buf), "%lld\n",
                               (long long)slotcask_count_live(&db));
    if (write(baseline_fd, baseline_buf, (size_t)baseline_n) != baseline_n) {
        close(baseline_fd);
        slotcask_close(&db);
        _exit(14);
    }
    close(baseline_fd);

    /* Configure the pause point via g_db (set by test_init_process_db). */
    ShardDb *sdb = g_db;
    snprintf(sdb->durability_test_pause_phase,
             sizeof(sdb->durability_test_pause_phase), "%s", phase);
    sdb->durability_test_pause_ms = 30000;

    int dropped = 0;
    rc = slotcask_compact_segs(&db, &dropped);

    sdb->durability_test_pause_ms = 0;
    sdb->durability_test_pause_phase[0] = '\0';

    slotcask_close(&db);
    slotcask_shutdown();

    /* Write result to a well-known file so the parent can read it. */
    char result_path[PATH_MAX];
    snprintf(result_path, sizeof(result_path), "%s/.compact-child-result", data_dir);
    int fd = open(result_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        char buf[64];
        int n = snprintf(buf, sizeof(buf), "%d\n", rc);
        (void)write(fd, buf, (size_t)n);
        close(fd);
    }
    _exit(0);
}

/* Read the compact_rc from the child's result file. */
static int read_child_result(const char *data_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.compact-child-result", data_dir);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -999;
    char buf[64];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return -999;
    buf[n] = '\0';
    return atoi(buf);
}

static int read_baseline_count(const char *data_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.compact-child-baseline", data_dir);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    char buf[64];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return -1;
    buf[n] = '\0';
    char *end = NULL;
    long long count = strtoll(buf, &end, 10);
    if (end == buf || (*end != '\0' && *end != '\n')) return -1;
    if (count < 0 || count > INT_MAX) return -1;
    return (int)count;
}

typedef struct {
    int live_records;
    int a_records;
    int c_records;
} CrashVerifyCtx;

static int crash_verify_live_cb(const uint8_t hash16[16], const void *key,
                                size_t klen, const void *value, size_t vlen,
                                void *raw) {
    CrashVerifyCtx *ctx = (CrashVerifyCtx *)raw;
    if (klen == 2 && memcmp(key, "kA", 2) == 0) {
        ctx->a_records++;
        ASSERT_TRUE(vlen == 1 && memcmp(value, "v", 1) == 0,
                    "kA value remains intact in the live walk");
    } else if (klen == 5 && memcmp(key, "kkeyC", 5) == 0) {
        ctx->c_records++;
        ASSERT_TRUE(vlen == 6 && memcmp(value, "cvalue", 6) == 0,
                    "kkeyC value remains intact in the live walk");
    }
    ctx->live_records++;
    (void)hash16;
    return 0;
}

/* Walk every live KF entry and check the complete logical live set.  This is
   the user-visible record set: crash recovery may leave an orphaned physical
   copy in the old segment, but it must not publish a duplicate KF entry or
   lose any live record. */
static int verify_fixture_live_records(SlotcaskDb *db, int expected_live) {
    CrashVerifyCtx ctx = {0};
    ASSERT_EQ_INT(slotcask_walk_live(db, crash_verify_live_cb, &ctx), 0,
                  "walk every live KF entry after crash recovery");
    ASSERT_EQ_INT(ctx.live_records, expected_live,
                  "all logical fixture records remain after recovery");
    ASSERT_EQ_INT(ctx.a_records, 1, "kA has exactly one live KF entry");
    ASSERT_EQ_INT(ctx.c_records, 1, "kkeyC has exactly one live KF entry");
    return t_ctx->failed > 0 ? 1 : 0;
}

static int run_crash_test(const char *phase) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-compact-crash-%s-XXXXXX", phase);
    if (!mkdtemp(tmpdir)) {
        ASSERT_TRUE(0, "mkdtemp");
        return 1;
    }

    /* Phase 1: build fixture, trigger compaction with pause, SIGKILL. */
    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0, "fork");
    if (pid < 0) { rmdir(tmpdir); return 1; }

    if (pid == 0) {
        compact_child_main(tmpdir, phase);
        _exit(99); /* unreachable */
    }

    /* Wait for the marker to appear. */
    int marker_rc = wait_marker(tmpdir, phase, 1, 20000);
    if (marker_rc != 0) {
        /* The crash seam is the subject of this test.  A missing marker is
           a failed test, never a skip: otherwise the test can pass while
           compaction never executes the intended crash point. */
        ASSERT_TRUE(0, "crash pause marker was reached");
        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);
        int child_rc = read_child_result(tmpdir);
        if (child_rc == -999) child_rc = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
        system(cmd);
        TAP_DIAG("# phase %s: marker not reached (child rc=%d)\n", phase, child_rc);
        return 1;
    }

    /* Marker appeared — SIGKILL the child to simulate a crash. */
    kill(pid, SIGKILL);
    int status;
    waitpid(pid, &status, 0);

    /* Phase 2: reopen the database (simulating restart) and verify data. */
    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int rc = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(rc, 0, "reopen database after simulated crash");
    if (rc != 0) { slotcask_shutdown(); rmdir(tmpdir); return 1; }

    /* Verify A survived (key from fixture). */
    void *val = NULL;
    size_t vlen = 0;
    rc = slotcask_get(&db, "kA", 2, &val, &vlen);
    ASSERT_EQ_INT(rc, 0, "key A readable after crash recovery");
    if (rc == 0) {
        ASSERT_EQ_INT((int)vlen, 1, "A value length intact");
        ASSERT_TRUE(memcmp(val, "v", 1) == 0, "A value intact");
        free(val);
    }

    /* Verify C survived. */
    val = NULL; vlen = 0;
    rc = slotcask_get(&db, "kkeyC", 5, &val, &vlen);
    ASSERT_EQ_INT(rc, 0, "key C readable after crash recovery");
    if (rc == 0) {
        ASSERT_EQ_INT((int)vlen, 6, "C value length intact");
        ASSERT_TRUE(memcmp(val, "cvalue", 6) == 0, "C value intact");
        free(val);
    }

    int baseline_live = read_baseline_count(tmpdir);
    ASSERT_TRUE(baseline_live >= 0, "read pre-crash live-record baseline");
    if (baseline_live >= 0)
        ASSERT_EQ_INT((int)slotcask_count_live(&db), baseline_live,
                      "live-record count unchanged after crash recovery");
    verify_fixture_live_records(&db, baseline_live);

    /* Phase 3: a subsequent compact_segs must complete cleanly, proving
       the post-recovery state is self-consistent. */
    int dropped = 0;
    rc = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(rc, 0, "second compact_segs completes after recovery");

    /* Verify A and C still intact after the second compaction. */
    val = NULL; vlen = 0;
    rc = slotcask_get(&db, "kA", 2, &val, &vlen);
    ASSERT_EQ_INT(rc, 0, "key A readable after second compaction");
    if (rc == 0) { free(val); }

    val = NULL; vlen = 0;
    rc = slotcask_get(&db, "kkeyC", 5, &val, &vlen);
    ASSERT_EQ_INT(rc, 0, "key C readable after second compaction");
    if (rc == 0) { free(val); }
    if (baseline_live >= 0)
        ASSERT_EQ_INT((int)slotcask_count_live(&db), baseline_live,
                      "live-record count unchanged after second compaction");
    verify_fixture_live_records(&db, baseline_live);

    slotcask_close(&db);
    slotcask_shutdown();

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
    system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_compact_crash_recipient_sync_run(void) {
    return run_crash_test("compact-after-recipient-sync");
}

static int test_compact_crash_kf_repoint_run(void) {
    return run_crash_test("compact-after-kf-repoint");
}

TEST_REGISTER("test-compact-crash-recipient-sync",
              test_compact_crash_recipient_sync_run)
TEST_REGISTER("test-compact-crash-kf-repoint",
              test_compact_crash_kf_repoint_run)
