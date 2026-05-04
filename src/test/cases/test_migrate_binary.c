/* src/test/cases/test_migrate_binary.c
 * Port of tests/test-migrate-binary.sh — exercises ./migrate, the per-
 * release one-shot upgrade binary. For 2026.05.1 it does:
 *   phase 1: migrate-files (lift pre-2026.05.2 XX/XX hash buckets to flat)
 *   phase 2: reindex (rebuild B+ trees under per-shard btree layout)
 *
 * Migrate reads db.env from CWD and shells out to ./shard-db. We set up an
 * isolated working dir <base>/ containing db.env + symlinks to the just-built
 * shard-db / migrate binaries, then run `cd <base> && ./migrate`.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
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

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

static char *capture_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    FILE *fp = popen(cmd, "r");
    if (!fp) return NULL;
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { pclose(fp); return NULL; }
    int c;
    while ((c = fgetc(fp)) != EOF) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); pclose(fp); return NULL; }
            buf = nb;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    pclose(fp);
    return buf;
}

/* Pull `"<key>":N` integer value from a JSON-ish line. */
static int json_int(const char *hay, const char *key) {
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":", key);
    const char *p = strstr(hay, needle);
    if (!p) return -1;
    return atoi(p + strlen(needle));
}

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

/* Resolve the "summary line" — the line of `out` that contains the substring. */
static void summary_line(const char *out, const char *needle, char *dst, size_t dst_sz) {
    dst[0] = '\0';
    const char *p = strstr(out, needle);
    if (!p) return;
    const char *line_start = p;
    while (line_start > out && line_start[-1] != '\n') line_start--;
    size_t llen = 0;
    const char *q = line_start;
    while (*q && *q != '\n' && llen + 1 < dst_sz) dst[llen++] = *q++;
    dst[llen] = '\0';
}

static int test_migrate_binary_run(void) {
    char shard_db_abs[PATH_MAX], migrate_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); return 1;
    }
    const char *mig_rel = "./build/bin/migrate";
    if (access(mig_rel, X_OK) != 0) mig_rel = "./migrate";
    if (!realpath(mig_rel, migrate_abs)) {
        ASSERT_TRUE(0, "migrate not found"); return 1;
    }

    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-mig-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    /* symlinks so migrate's `./shard-db` and external `./migrate` resolve from base. */
    char link_path[400];
    snprintf(link_path, sizeof(link_path), "%s/shard-db", base);
    if (symlink(shard_db_abs, link_path) != 0) {
        ASSERT_TRUE(0, "symlink shard-db"); run_cmd("rm -rf %s", base); return 1;
    }
    snprintf(link_path, sizeof(link_path), "%s/migrate", base);
    if (symlink(migrate_abs, link_path) != 0) {
        ASSERT_TRUE(0, "symlink migrate"); run_cmd("rm -rf %s", base); return 1;
    }

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); run_cmd("rm -rf %s", base); return 1; }

    /* Spawn test daemon to create the schema for object "mft". */
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "first daemon spawn");
        run_cmd("rm -rf %s", base);
        return 1;
    }
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mft\","
        "\"fields\":[\"k:varchar:32\"]}", &resp); free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    /* Stop test daemon gracefully WITHOUT wiping db_root (test_env_stop wipes,
       so we shut down manually here). */
    kill(env.daemon_pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(env.daemon_pid, NULL, WNOHANG) == env.daemon_pid) {
            env.daemon_pid = -1; break;
        }
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    if (env.daemon_pid > 0) { kill(env.daemon_pid, SIGKILL); waitpid(env.daemon_pid, NULL, 0); env.daemon_pid = -1; }

    /* Seed XX/XX layout (pre-2026.05.2 install). */
    char obj[300]; snprintf(obj, sizeof(obj), "%s/default/mft", db_root);
    run_cmd("mkdir -p %s/files/ab/cd", obj);
    run_cmd("mkdir -p %s/files/12/34", obj);
    run_cmd("mkdir -p %s/files/ef/00", obj);
    run_cmd("mkdir -p %s/files/aa/bb", obj);
    run_cmd("echo alpha   > %s/files/ab/cd/alpha.pdf", obj);
    run_cmd("echo bravo   > %s/files/12/34/bravo.txt", obj);
    run_cmd("echo charlie > %s/files/ef/00/charlie.png", obj);
    run_cmd("echo delta   > %s/files/aa/bb/delta.csv", obj);

    char *count_s = capture_cmd("find %s/files -mindepth 2 -maxdepth 2 -type d | wc -l", obj);
    ASSERT_EQ_INT(count_s ? atoi(count_s) : -1, 4, "before: 4 leaf XX/XX dirs");
    free(count_s);
    count_s = capture_cmd("find %s/files -maxdepth 1 -mindepth 1 -type f | wc -l", obj);
    ASSERT_EQ_INT(count_s ? atoi(count_s) : -1, 0, "before: 0 flat files");
    free(count_s);

    /* Run migrate from <base>. It reads db.env there + shells out to ./shard-db. */
    char *out = capture_cmd("cd %s && ./migrate 2>&1", base);
    ASSERT_NOT_NULL(out, "migrate output captured");
    if (!out) { run_cmd("rm -rf %s", base); return 1; }

    ASSERT_CONTAINS(out, "\"status\":\"migrated\"", "phase 1 emitted status=migrated");
    char fl[1024];
    summary_line(out, "\"status\":\"migrated\"", fl, sizeof(fl));
    ASSERT_EQ_INT(json_int(fl, "files_moved"), 4, "files_moved=4");
    ASSERT_TRUE(json_int(fl, "objects_migrated") >= 1, "objects_migrated >= 1");
    ASSERT_EQ_INT(json_int(fl, "conflicts"), 0, "conflicts=0");
    ASSERT_CONTAINS(out, "phase 1/2", "phase 1 banner");
    ASSERT_CONTAINS(out, "phase 2/2", "phase 2 banner");
    ASSERT_CONTAINS(out, "migrate: complete", "complete banner");
    free(out);

    /* Layout flattened. */
    count_s = capture_cmd("find %s/files -mindepth 2 -maxdepth 2 -type d 2>/dev/null | wc -l", obj);
    ASSERT_EQ_INT(count_s ? atoi(count_s) : -1, 0, "after: 0 leaf XX/XX dirs");
    free(count_s);
    count_s = capture_cmd("find %s/files -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l", obj);
    ASSERT_EQ_INT(count_s ? atoi(count_s) : -1, 0, "after: 0 top-level XX dirs");
    free(count_s);
    count_s = capture_cmd("find %s/files -maxdepth 1 -mindepth 1 -type f | wc -l", obj);
    ASSERT_EQ_INT(count_s ? atoi(count_s) : -1, 4, "after: 4 flat files");
    free(count_s);

    /* list-files works after migration — re-spawn test daemon for verification. */
    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "respawn after migrate");
        run_cmd("rm -rf %s", base);
        return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after migrate");
    if (!tc) { test_env_kill(&env2); run_cmd("rm -rf %s", base); return 1; }

    tc_request(tc,
        "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"mft\"}", &resp);
    ASSERT_CONTAINS(resp, "alpha.pdf", "alpha.pdf listed");
    ASSERT_CONTAINS(resp, "bravo.txt", "bravo.txt listed");
    ASSERT_CONTAINS(resp, "charlie.png", "charlie.png listed");
    ASSERT_CONTAINS(resp, "delta.csv", "delta.csv listed");
    ASSERT_CONTAINS(resp, "\"total\":4", "total=4");
    free(resp); resp = NULL;

    tc_close(tc); tc = NULL;
    /* Stop without wiping. */
    kill(env2.daemon_pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(env2.daemon_pid, NULL, WNOHANG) == env2.daemon_pid) {
            env2.daemon_pid = -1; break;
        }
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    if (env2.daemon_pid > 0) { kill(env2.daemon_pid, SIGKILL); waitpid(env2.daemon_pid, NULL, 0); env2.daemon_pid = -1; }

    /* Idempotent second migrate run on files phase. */
    out = capture_cmd("cd %s && ./migrate 2>&1", base);
    if (out) {
        summary_line(out, "\"status\":\"migrated\"", fl, sizeof(fl));
        ASSERT_EQ_INT(json_int(fl, "files_moved"), 0, "second run files_moved=0");
        ASSERT_EQ_INT(json_int(fl, "objects_migrated"), 0, "second run objects_migrated=0");
        free(out);
    } else {
        ASSERT_TRUE(0, "second migrate captured");
    }

    /* Conflict handling: pre-create flat target collision, then re-run. */
    run_cmd("mkdir -p %s/files/ab/cd", obj);
    run_cmd("echo new-content > %s/files/ab/cd/alpha.pdf", obj);
    out = capture_cmd("cd %s && ./migrate 2>&1", base);
    if (out) {
        summary_line(out, "\"status\":\"migrated\"", fl, sizeof(fl));
        ASSERT_EQ_INT(json_int(fl, "conflicts"), 1, "conflict reported");
        free(out);
    } else {
        ASSERT_TRUE(0, "third migrate captured");
    }
    char *flat = capture_cmd("cat %s/files/alpha.pdf", obj);
    ASSERT_TRUE(flat && strstr(flat, "alpha") != NULL,
                "no overwrite: alpha.pdf flat content unchanged");
    free(flat);
    char keep[400]; snprintf(keep, sizeof(keep), "%s/files/ab/cd/alpha.pdf", obj);
    ASSERT_TRUE(file_exists(keep), "conflicting bucket leaf preserved");

    run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-migrate-binary", test_migrate_binary_run)
