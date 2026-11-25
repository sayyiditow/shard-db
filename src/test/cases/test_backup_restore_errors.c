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
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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
    maint_restore_swap_recover(base);
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
    maint_restore_swap_recover(base);
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
   same-second backups are distinct; a failing restore leaves live data. */
static int test_backup_restore_failures(void) {
    if (geteuid() == 0) return 0;   /* chmod-based failure needs a non-root user */
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

    /* Two backups back to back get distinct directories. */
    char p1[512] = "", p2[512] = "";
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "first backup");
    extract_path(resp, p1, sizeof(p1)); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "second backup");
    extract_path(resp, p2, sizeof(p2)); free(resp); resp = NULL;
    ASSERT_TRUE(p1[0] && p2[0] && strcmp(p1, p2) != 0, "same-second backups use distinct directories");

    /* Unreadable stored file: backup must fail and leave no partial dir. */
    char bparent[512], fpath[512];
    snprintf(bparent, sizeof(bparent), "%s/default/bre/backup", env.db_root);
    int before = count_entries(bparent);
    snprintf(fpath, sizeof(fpath), "%s/default/bre/files/a.txt", env.db_root);
    ASSERT_TRUE(chmod(fpath, 0) == 0, "chmod source file");
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bre\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"backup failed", "backup reports the copy failure");
    ASSERT_TRUE(resp && !strstr(resp, "backed_up"), "backup does not claim success");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(count_entries(bparent), before, "no partial backup directory is left");
    chmod(fpath, 0644);

    /* Restore from a backup with an unreadable file: error, live data intact. */
    char bfile[600];
    snprintf(bfile, sizeof(bfile), "%s/files/a.txt", p1);
    ASSERT_TRUE(chmod(bfile, 0) == 0, "chmod backup file");
    const char *tail = strrchr(p1, '/');
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bre\",\"from\":\"%s\",\"force\":true}",
        tail ? tail + 1 : p1);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"restore failed", "restore reports the copy failure");
    ASSERT_CONTAINS(resp, "live object was not changed", "restore says the live object is untouched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bre\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "alpha", "live record survives a failed restore");
    free(resp); resp = NULL;
    chmod(bfile, 0644);
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"bre\",\"from\":\"%s\",\"force\":true}",
        tail ? tail + 1 : p1);
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

TEST_REGISTER("test-restore-swap-rollback", test_restore_swap_rollback)
TEST_REGISTER("test-backup-restore-failures", test_backup_restore_failures)
