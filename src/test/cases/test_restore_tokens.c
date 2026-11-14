/* src/test/cases/test_restore_tokens.c
 * Restore merges the backup's object tokens into the running
 * token store (add-only), rewrites the object's tokens.conf, and restore
 * itself is an object-admin (rwx) mode on the JSON path.
 *
 * Requires DISABLE_LOCALHOST_TRUST so loopback doesn't bypass auth: the
 * fixture writes its own db.env, so we spawn the daemon manually (same
 * pattern as test_token_perms.c) instead of calling test_env_start.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Fixed global admin token. Slot = str_hash(GTOK) % 1024 = 346 (djb2,
   start 5381) — distinct from objtok-restore-1 (573), objtok-keep (864)
   and objtok-rw (36) so the token_set_remove probe-chain defect cannot
   affect this case. */
#define GTOK "sdb_rt_admin_fixed"

/* Pull "<key>":"<value>" out of a JSON-ish response. Stops on first '"'. */
static int extract_str(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *p = SAFE_STRSTR(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, p, n); out[n] = '\0';
    return 1;
}

/* Trailing path component (basename) of a slash-separated path. */
static const char *path_tail(const char *p) {
    const char *t = strrchr(p, '/');
    return t ? t + 1 : p;
}

/* Read up to 4 KiB of a file and substring-search it. */
static int file_contains(const char *path, const char *needle) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, needle) != NULL;
}

static int test_restore_merges_object_tokens(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-rtk-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Pre-seed: dirs.conf with default (so add-dir answers dir_exists —
       no assertion on it) and a global rwx admin token readable BEFORE
       the daemon starts (DISABLE_LOCALHOST_TRUST=1 would otherwise lock
       us out). */
    char dirs_path[300]; snprintf(dirs_path, sizeof(dirs_path), "%s/dirs.conf", db_root);
    FILE *f = fopen(dirs_path, "w");
    if (f) { fputs("default\n", f); fclose(f); }
    tu_run_cmd("touch %s/allowed_ips.conf", db_root);

    char gtok_path[300]; snprintf(gtok_path, sizeof(gtok_path), "%s/tokens.conf", db_root);
    f = fopen(gtok_path, "w");
    if (f) { fprintf(f, "%s\n", GTOK); fclose(f); }

    /* Write db.env: fixture content plus DISABLE_LOCALHOST_TRUST=1. */
    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
        "export DISABLE_LOCALHOST_TRUST=1\n",
        db_root, port, base);
    fclose(f);
    char logs_dir[400]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    /* Spawn daemon (manual fork — the stock fixture would overwrite env). */
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    TestEnv env = { .port = port, .daemon_pid = pid };
    snprintf(env.db_root, sizeof(env.db_root), "%s", db_root);

    /* Wait for the daemon by polling auth'd db-dirs through a TCP probe. */
    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", GTOK);
            if (tc_request(probe, req, &r) == 0 && r) {
                if (strstr(r, "\"error\"") == NULL) ready = 1;
            }
            free(r); tc_close(probe);
            if (ready) break;
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    if (!ready) {
        ASSERT_TRUE(0, "daemon ready");
        kill(pid, SIGKILL); waitpid(pid, NULL, 0);
        tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL; char req[1024];

    /* 1. add-dir (pre-seeded — dir_exists; no assertion) + create the object. */
    tc_request(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"default\",\"auth\":\"" GTOK "\"}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"tk\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"],"
        "\"auth\":\"" GTOK "\"}",
        &resp); free(resp); resp = NULL;

    /* 2. Object-scoped tokens. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"dir\":\"default\",\"object\":\"tk\","
        "\"token\":\"objtok-restore-1\",\"perm\":\"rw\",\"auth\":\"%s\"}", GTOK);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"token_added\"", "add objtok-restore-1");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"dir\":\"default\",\"object\":\"tk\","
        "\"token\":\"objtok-keep\",\"perm\":\"rw\",\"auth\":\"%s\"}", GTOK);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"token_added\"", "add objtok-keep");
    free(resp); resp = NULL;

    /* 3. Non-vacuity: the object token works before the backup. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"tk\","
        "\"key\":\"k1\",\"auth\":\"objtok-restore-1\"}");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && strstr(resp, "auth failed") == NULL,
                "object token works before backup");
    free(resp); resp = NULL;

    /* 4. Backup; it must carry the object's tokens.conf (0600). */
    tc_request(tc,
        "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"tk\","
        "\"auth\":\"" GTOK "\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base);
        return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    {
        char btp[512];
        snprintf(btp, sizeof(btp), "%s/default/tk/backup/%s/tokens.conf",
                 env.db_root, ts);
        ASSERT_TRUE(file_contains(btp, "objtok-restore-1"),
                    "backup copied tokens.conf");
        struct stat stm;
        ASSERT_TRUE(stat(btp, &stm) == 0 && (stm.st_mode & 0777) == 0600,
                    "backup tokens.conf is 0600");
    }

    /* 5. Revoke objtok-restore-1; demote objtok-keep to r; add objtok-rw. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"remove-token\",\"token\":\"objtok-restore-1\","
        "\"auth\":\"%s\"}", GTOK);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"tk\","
        "\"key\":\"k1\",\"auth\":\"objtok-restore-1\"}");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && strstr(resp, "auth failed") != NULL,
                "revoked token is rejected");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"dir\":\"default\",\"object\":\"tk\","
        "\"token\":\"objtok-keep\",\"perm\":\"r\",\"auth\":\"%s\"}", GTOK);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"dir\":\"default\",\"object\":\"tk\","
        "\"token\":\"objtok-rw\",\"perm\":\"rw\",\"auth\":\"%s\"}", GTOK);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    /* 6. A rw object token cannot restore (restore is object-admin). */
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"tk\","
        "\"from\":\"%s\",\"auth\":\"objtok-rw\"}", ts);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && strstr(resp, "auth failed") != NULL,
                "rw object token cannot restore");
    free(resp); resp = NULL;

    /* 7. Restore with force=true as the global admin. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"tk\","
        "\"from\":\"%s\",\"force\":true,\"auth\":\"%s\"}", ts, GTOK);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore status");
    free(resp); resp = NULL;

    /* 8. The restored token authenticates immediately (no restart), the
       live tokens.conf carries it, and an already-present token keeps
       its perm. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"tk\","
        "\"key\":\"k1\",\"auth\":\"objtok-restore-1\"}");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && strstr(resp, "auth failed") == NULL,
                "restored object token authenticates immediately");
    free(resp); resp = NULL;

    {
        char ltp[512];
        snprintf(ltp, sizeof(ltp), "%s/default/tk/tokens.conf", env.db_root);
        ASSERT_TRUE(file_contains(ltp, "objtok-restore-1"),
                    "live tokens.conf rewritten with the restored token");
    }

    {
        int insert_rejected = 0, get_ok = 0;
        char *r1 = NULL, *r2 = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"tk\","
            "\"key\":\"k9\",\"value\":{\"name\":\"x\"},\"auth\":\"objtok-keep\"}");
        tc_request(tc, req, &r1);
        insert_rejected = r1 && strstr(r1, "auth failed") != NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"tk\","
            "\"key\":\"k1\",\"auth\":\"objtok-keep\"}");
        tc_request(tc, req, &r2);
        get_ok = r2 && strstr(r2, "auth failed") == NULL;
        free(r1); free(r2);
        ASSERT_TRUE(insert_rejected && get_ok,
                    "restore does not change an existing token's perm");
    }

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-restore-merges-object-tokens", test_restore_merges_object_tokens)
