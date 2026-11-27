/* src/test/cases/test_backup_restore_errors.c
 * AUD-BACKUP-SILENT (docs/plans/2026-11-25-fix-backup-restore-errors.md):
 * backup and restore must report copy failures instead of claiming success,
 * a failed restore must leave the live object untouched, two backups in one
 * second must not merge, and an interrupted restore swap must roll back at
 * startup.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include "shard_test_ctl.h"
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int count_entries(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) if (e->d_name[0] != '.') n++;
    closedir(d);
    return n;
}

static int extract_path(const char *resp, char *out, size_t sz) {
    const char *p = resp ? strstr(resp, "\"path\":\"") : NULL;
    if (!p) return 0;
    p += 8;
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > sz) n = sz - 1;
    memcpy(out, p, n); out[n] = '\0';
    return 1;
}

static void write_text(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(text, f); fclose(f); }
}

static void remove_base(const char *base) {
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    if (system(cmd) != 0) { /* best effort */ }
}

/* In-process: a leftover staging dir with a SWAP marker rolls back. */
static int test_restore_swap_rollback(void) {
    char base[] = "/tmp/shard-db-rswap-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/data", base);               mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/new.txt", base);       write_text(p, "restored");
    snprintf(p, sizeof(p), "%s/.restore-77", base);        mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old", base);    mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data", base); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data/orig.txt", base); write_text(p, "original");
    snprintf(p, sizeof(p), "%s/.restore-77/SWAP", base);   write_text(p, "x");
    ASSERT_EQ_INT(maint_restore_swap_recover(base), 1,
                  "the marked stage reports a rolled-back swap");
    snprintf(p, sizeof(p), "%s/data/orig.txt", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "pre-restore data is back after rollback");
    snprintf(p, sizeof(p), "%s/data/new.txt", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "partially restored data is removed");
    snprintf(p, sizeof(p), "%s/.restore-77", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "staging directory is removed");

    /* No marker: the live object was never touched, staging is just deleted. */
    snprintf(p, sizeof(p), "%s/.restore-78", base);        mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-78/old", base);    mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-78/old/data", base); mkdir(p, 0755);
    ASSERT_EQ_INT(maint_restore_swap_recover(base), 0,
                  "an unmarked stage reports nothing to roll back");
    snprintf(p, sizeof(p), "%s/data/orig.txt", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "unmarked staging does not touch live data");
    snprintf(p, sizeof(p), "%s/.restore-78", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "unmarked staging removed");
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    if (system(cmd) != 0) { /* best effort */ }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Daemon: unreadable source -> backup errors and leaves no partial backup;
   same-second backups are distinct; a failing restore leaves live data. The
   backup/restore round-trip runs for every user; only the chmod-based
   copy-failure checks need a non-root user (root ignores file modes). */
static int ts_prefixed(const char *base, char ts[][32], int n) {
    for (int i = 0; i < n; i++) {
        size_t l = strlen(ts[i]);
        if (strncmp(base, ts[i], l) != 0) continue;
        if (base[l] == '\0') return 1;
        if (base[l] != '_') continue;
        const char *q = base + l + 1;
        if (!*q) continue;
        int digits = 1;
        for (; *q; q++) if (*q < '0' || *q > '9') digits = 0;
        if (digits) return 1;
    }
    return 0;
}

static const char *base_name(const char *path) {
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

static int test_backup_restore_failures(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bre\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bre\",\"key\":\"k1\","
        "\"value\":{\"name\":\"alpha\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"bre\","
        "\"filename\":\"a.txt\",\"data\":\"YWxwaGE=\"}", &resp);
    free(resp); resp = NULL;

    /* Deterministic same-second naming: pre-create the next six one-second
       bucket names, so the backups must fall back to _<n> suffixes (the
       clock cannot be pinned inside the daemon). */
    char bparent[512];
    snprintf(bparent, sizeof(bparent), "%s/default/bre/backup", env.db_root);
    ASSERT_EQ_INT(tu_run_cmd("mkdir -p %s", bparent), 0, "create the backup parent");
    char ts[6][32];
    for (int i = 0; i < 6; i++) {
        time_t when = time(NULL) + i;
        struct tm tbuf;
        struct tm *t = localtime_r(&when, &tbuf);
        ASSERT_NOT_NULL(t, "localtime_r");
        if (t) strftime(ts[i], sizeof(ts[i]), "%Y%m%d_%H%M%S", t);
        char dir[600];
        snprintf(dir, sizeof(dir), "%s/%s", bparent, ts[i]);
        ASSERT_EQ_INT(mkdir(dir, 0755), 0, "pre-create a same-second backup dir");
    }

    /* Two backups back to back get distinct directories. */
    char p1[512] = "", p2[512] = "";
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "first backup");
    extract_path(resp, p1, sizeof(p1)); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "second backup");
    extract_path(resp, p2, sizeof(p2)); free(resp); resp = NULL;
    ASSERT_TRUE(p1[0] && p2[0] && strcmp(p1, p2) != 0, "same-second backups use distinct directories");
    ASSERT_TRUE(p1[0] && p2[0] &&
                ts_prefixed(base_name(p1), ts, 6) &&
                ts_prefixed(base_name(p2), ts, 6),
                "each backup name is a pre-created second, disambiguated by _<n>");

    for (int i = 0; i < 6; i++) {
        char dir[600];
        snprintf(dir, sizeof(dir), "%s/%s", bparent, ts[i]);
        (void)rmdir(dir);
    }

    char fpath[512], bfile[600];
    snprintf(fpath, sizeof(fpath), "%s/default/bre/files/a.txt", env.db_root);
    snprintf(bfile, sizeof(bfile), "%s/files/a.txt", p1);
    const char *tail = strrchr(p1, '/');
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bre\",\"from\":\"%s\",\"force\":true}",
        tail ? tail + 1 : p1);

    if (geteuid() != 0) {
        /* Unreadable stored file: backup must fail and leave no partial dir. */
        int before = count_entries(bparent);
        ASSERT_TRUE(chmod(fpath, 0) == 0, "chmod source file");
        tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"backup failed", "backup reports the copy failure");
        ASSERT_TRUE(resp && !strstr(resp, "backed_up"), "backup does not claim success");
        free(resp); resp = NULL;
        ASSERT_EQ_INT(count_entries(bparent), before, "no partial backup directory is left");
        chmod(fpath, 0644);

        /* Restore from a backup with an unreadable file: error, live data intact. */
        ASSERT_TRUE(chmod(bfile, 0) == 0, "chmod backup file");
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"restore failed", "restore reports the copy failure");
        ASSERT_CONTAINS(resp, "live object was not changed", "restore says the live object is untouched");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bre\",\"key\":\"k1\"}", &resp);
        ASSERT_CONTAINS(resp, "alpha", "live record survives a failed restore");
        free(resp); resp = NULL;
        chmod(bfile, 0644);
    } else {
        TAP_DIAG("# running as root: chmod-based copy-failure checks skipped\n");
    }

    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore succeeds once the backup is readable");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bre\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "alpha", "restored record is readable");
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* In-process: a failed rollback rename keeps the stage and the original, so
   the next attempt can finish (I5). */
static int test_restore_recover_rename_failure_keeps_stage(void) {
    char base[] = "/tmp/shard-db-rrn-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/data", base);                  mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/new.txt", base);          write_text(p, "restored");
    snprintf(p, sizeof(p), "%s/.restore-77", base);           mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old", base);       mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data", base);  mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data/orig.txt", base);
    write_text(p, "original");
    snprintf(p, sizeof(p), "%s/.restore-77/SWAP", base);      write_text(p, "x");

    shard_test_ctl_reset();
    atomic_store(&g_restore_test_fault_mask, SHARD_TEST_RF_ROLLBACK_RENAME);
    ASSERT_EQ_INT(maint_restore_swap_recover(base), -1,
                  "a failed rollback rename stops recovery");
    atomic_store(&g_restore_test_fault_mask, 0);

    snprintf(p, sizeof(p), "%s/.restore-77/SWAP", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the stage keeps its SWAP marker");
    snprintf(p, sizeof(p), "%s/.restore-77/old/data/orig.txt", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the original is still parked in old/");

    ASSERT_EQ_INT(maint_restore_swap_recover(base), 1,
                  "the retried recovery rolls the swap back");
    snprintf(p, sizeof(p), "%s/data/orig.txt", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the original is back live");
    snprintf(p, sizeof(p), "%s/data/new.txt", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "the restored copy is gone");
    snprintf(p, sizeof(p), "%s/.restore-77", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "the stage is removed");

    remove_base(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* In-process: every mixed crash state (a: original live, b: original in old/
   and live absent, c: original in old/ and restored live) plus fields.conf
   rolls back to the pre-restore tree. */
static int test_restore_recover_mixed_swap_states(void) {
    char base[] = "/tmp/shard-db-rmx-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    /* state c: data */
    snprintf(p, sizeof(p), "%s/data", base);                    mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/r", base);                  write_text(p, "restored");
    snprintf(p, sizeof(p), "%s/.restore-77", base);             mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old", base);         mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data", base);    mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/data/o", base);  write_text(p, "orig");
    /* state b: indexes (live absent, restored only in new/) */
    snprintf(p, sizeof(p), "%s/.restore-77/old/indexes", base); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/old/indexes/o", base); write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/.restore-77/new", base);         mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/new/indexes", base); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/new/indexes/r", base); write_text(p, "restored");
    /* state a: metadata (never swapped) */
    snprintf(p, sizeof(p), "%s/metadata", base);                mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/metadata/o", base);              write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/.restore-77/new/metadata", base); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-77/new/metadata/r", base); write_text(p, "restored");
    /* fields.conf swapped */
    snprintf(p, sizeof(p), "%s/.restore-77/old/fields.conf", base); write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/fields.conf", base);             write_text(p, "restored");
    snprintf(p, sizeof(p), "%s/.restore-77/SWAP", base);        write_text(p, "x");

    shard_test_ctl_reset();
    ASSERT_EQ_INT(maint_restore_swap_recover(base), 1, "the swap is rolled back");

    snprintf(p, sizeof(p), "%s/data/o", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "state c: the original data is live");
    snprintf(p, sizeof(p), "%s/data/r", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "state c: the restored data is gone");
    snprintf(p, sizeof(p), "%s/indexes/o", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "state b: the original indexes are live");
    snprintf(p, sizeof(p), "%s/indexes/r", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "state b: the restored indexes are gone");
    snprintf(p, sizeof(p), "%s/metadata/o", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "state a: the live metadata is untouched");
    snprintf(p, sizeof(p), "%s/metadata/r", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "state a: the staged metadata is gone");
    snprintf(p, sizeof(p), "%s/fields.conf", base);
    char *fc = tu_read_file(p);
    ASSERT_NOT_NULL(fc, "fields.conf is readable");
    if (fc) {
        ASSERT_EQ_STR(fc, "orig", "fields.conf is rolled back");
        free(fc);
    }
    snprintf(p, sizeof(p), "%s/.restore-77", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "the stage is removed");

    remove_base(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* In-process: a non-directory rb/ cannot hold the restored copies, so
   recovery refuses without moving anything. */
static int test_restore_recover_refuses_non_dir_rb(void) {
    char base[] = "/tmp/shard-db-rrb-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/data", base);                     mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/r", base);                   write_text(p, "restored");
    snprintf(p, sizeof(p), "%s/.restore-79", base);              mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-79/old", base);          mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-79/old/data", base);     mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-79/old/data/o", base);   write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/.restore-79/SWAP", base);         write_text(p, "x");
    snprintf(p, sizeof(p), "%s/.restore-79/rb", base);           write_text(p, "not a dir");

    shard_test_ctl_reset();
    ASSERT_EQ_INT(maint_restore_swap_recover(base), -1,
                  "a non-directory rb/ refuses the rollback");
    snprintf(p, sizeof(p), "%s/.restore-79/old/data/o", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the original is left in old/");
    snprintf(p, sizeof(p), "%s/data/r", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the live tree is unchanged");
    snprintf(p, sizeof(p), "%s/.restore-79", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the stage is kept");

    remove_base(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* In-process (round-1 blocker): a SWAP stage with no old/, or an empty old/,
   rolls back as a no-op -- never an fsync of a vanished directory. */
static int test_restore_recover_swap_without_old(void) {
    char base[] = "/tmp/shard-db-rnl-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/data", base);                  mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/o", base);                write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/.restore-80", base);           mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-80/SWAP", base);      write_text(p, "x");
    snprintf(p, sizeof(p), "%s/.restore-81", base);           mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-81/old", base);       mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-81/SWAP", base);      write_text(p, "x");

    shard_test_ctl_reset();
    ASSERT_TRUE(maint_restore_swap_recover(base) >= 0,
                "SWAP stages without names in old/ recover");
    snprintf(p, sizeof(p), "%s/.restore-80", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "the no-old stage is removed");
    snprintf(p, sizeof(p), "%s/.restore-81", base);
    ASSERT_TRUE(access(p, F_OK) != 0, "the empty-old stage is removed");
    snprintf(p, sizeof(p), "%s/data/o", base);
    char *lc = tu_read_file(p);
    ASSERT_NOT_NULL(lc, "the live data is readable");
    if (lc) {
        ASSERT_EQ_STR(lc, "orig", "the live data is unchanged");
        free(lc);
    }

    remove_base(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* In-process: a regular file named .restore-* is neither failed on nor
   deleted. */
static int test_restore_recover_skips_stray_file(void) {
    char base[] = "/tmp/shard-db-rsf-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/data", base);          mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/data/o", base);        write_text(p, "orig");
    snprintf(p, sizeof(p), "%s/.restore-stray", base); write_text(p, "keep me");

    shard_test_ctl_reset();
    ASSERT_EQ_INT(maint_restore_swap_recover(base), 0,
                  "a stray .restore-* file is not a swap to recover");
    snprintf(p, sizeof(p), "%s/.restore-stray", base);
    ASSERT_TRUE(access(p, F_OK) == 0, "the stray file still exists");
    snprintf(p, sizeof(p), "%s/data/o", base);
    char *lc = tu_read_file(p);
    ASSERT_NOT_NULL(lc, "the live data is readable");
    if (lc) {
        ASSERT_EQ_STR(lc, "orig", "the live data is unchanged");
        free(lc);
    }

    remove_base(base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-restore-swap-rollback", test_restore_swap_rollback)
TEST_REGISTER("test-restore-recover-rename-failure-keeps-stage",
              test_restore_recover_rename_failure_keeps_stage)
TEST_REGISTER("test-restore-recover-mixed-swap-states",
              test_restore_recover_mixed_swap_states)
TEST_REGISTER("test-restore-recover-refuses-non-dir-rb",
              test_restore_recover_refuses_non_dir_rb)
TEST_REGISTER("test-restore-recover-swap-without-old",
              test_restore_recover_swap_without_old)
TEST_REGISTER("test-restore-recover-skips-stray-file",
              test_restore_recover_skips_stray_file)
TEST_REGISTER("test-backup-restore-failures", test_backup_restore_failures)
