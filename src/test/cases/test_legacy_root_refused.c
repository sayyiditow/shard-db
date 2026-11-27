/* T1: the layout gate refuses a DB_ROOT that is not entirely packed.
 * In-process fixtures call shard_db_layout_gate directly (with g_db cleared,
 * proving the gate uses no g_* state or logging); a daemon-subprocess case
 * proves cmd_server reports the refusal on stderr before it forks.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define LG_STREAMS 16

static int lg_write(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    int rc = fputs(text, f) < 0 ? -1 : 0;
    if (fclose(f) != 0) rc = -1;
    return rc;
}

static int lg_mkdir(const char *path) {
    return mkdir(path, 0755) == 0 || errno == EEXIST ? 0 : -1;
}

/* Creates <root>/d/o (optionally), fields.conf, schema.conf (`line`) and
   an optional .layout-migrated stamp. */
static int lg_fixture(char *root, const char *line, int object_dir, int stamp) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/d", root);
    if (lg_mkdir(path) != 0) return -1;
    if (object_dir) {
        snprintf(path, sizeof(path), "%s/d/o", root);
        if (lg_mkdir(path) != 0) return -1;
        snprintf(path, sizeof(path), "%s/d/o/fields.conf", root);
        if (lg_write(path, "n:int\n") != 0) return -1;
    }
    snprintf(path, sizeof(path), "%s/schema.conf", root);
    if (lg_write(path, line) != 0) return -1;
    if (stamp) {
        snprintf(path, sizeof(path), "%s/.layout-migrated", root);
        if (lg_write(path, "stamp\n") != 0) return -1;
    }
    return 0;
}

static int lg_call(const char *root, char *err, size_t errsz) {
    ShardDb *saved = g_db;
    g_db = NULL;                 /* the gate runs before any instance exists */
    int rc = shard_db_layout_gate(root, err, errsz);
    g_db = saved;
    return rc;
}

static int lg_root(char *out, size_t cap, const char *tag) {
    snprintf(out, cap, "/tmp/lg_%d_%s_XXXXXX", (int)getpid(), tag);
    return mkdtemp(out) ? 0 : -1;
}

static int test_legacy_root_refused_run(void) {
    char err[1024];

    /* (a) version floor: 2026.11.1 is below the required 2026.11.2. */
    ASSERT_EQ_INT(shard_db_version_decide("2026.11.1", 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_TOO_OLD,
                  "(a) 2026.11.1 root is TOO_OLD");

    /* (b) token-less schema line for an existing object is refused. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "b"), 0, "mkdtemp b");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16\n", 1, 0), 0, "fixture b");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(b) token-less object refused");
        ASSERT_CONTAINS(err, "object d/o is not in the packed layout",
                        "(b) refusal names d/o");
        ASSERT_CONTAINS(err, "2026.11.2", "(b) refusal names the upgrade release");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (b2) token-less line with no object directory is a stale line. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "b2"), 0, "mkdtemp b2");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16\n", 0, 0), 0, "fixture b2");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(b2) stale line refused");
        ASSERT_CONTAINS(err, "stale schema.conf line for d/o lacks layout=packed",
                        "(b2) stale-line message");
        ASSERT_CONTAINS(err, "delete the line from schema.conf", "(b2) remedy named");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (b3) an object legitimately named "layout" with the token passes. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "b3"), 0, "mkdtemp b3");
        ASSERT_EQ_INT(lg_fixture(root, "d:layout:8:16:2:16:layout=packed\n", 1, 0), 0,
                      "fixture b3");
        /* rename d/o to d/layout so the packed line matches a real object */
        char from[PATH_MAX], to[PATH_MAX];
        snprintf(from, sizeof(from), "%s/d/o", root);
        snprintf(to, sizeof(to), "%s/d/layout", root);
        ASSERT_EQ_INT(rename(from, to), 0, "rename b3 object");
        char fld[PATH_MAX];
        snprintf(fld, sizeof(fld), "%s/fields.conf", to);
        ASSERT_EQ_INT(lg_write(fld, "n:int\n"), 0, "b3 fields");
        err[0] = '\0';
        ASSERT_EQ_INT(lg_call(root, err, sizeof(err)), 0, "(b3) object named layout passes");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (b4) same as (b) plus .layout-migrated: the stamp-dependent variant. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "b4"), 0, "mkdtemp b4");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16\n", 1, 1), 0, "fixture b4");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(b4) refused");
        ASSERT_CONTAINS(err, "To convert it: remove stale schema.conf lines",
                        "(b4) stamp-aware variant");
        ASSERT_CONTAINS(err, ".layout-migrated", "(b4) names the stamp to remove");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (c) packed line but active txn with old_layout=0 is a conversion artifact. */
    {
        char root[PATH_MAX], p[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "c"), 0, "mkdtemp c");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16:layout=packed\n", 1, 0), 0,
                      "fixture c");
        snprintf(p, sizeof(p), "%s/d/o/.rebuild_txn.active", root);
        ASSERT_EQ_INT(lg_mkdir(p), 0, "c active dir");
        snprintf(p, sizeof(p), "%s/d/o/.rebuild_txn.active/meta", root);
        ASSERT_EQ_INT(lg_write(p, "version=1\nold_splits=8\nold_streams=16\n"
                                  "indexes_may_change=1\nold_layout=0\n"), 0, "c meta");
        snprintf(p, sizeof(p), "%s/d/o/.rebuild_txn.active/fields.conf.rollback", root);
        ASSERT_EQ_INT(lg_write(p, "n:int\n"), 0, "c rollback");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(c) old_layout=0 refused");
        ASSERT_CONTAINS(err, "object d/o is not in the packed layout",
                        "(c) rule-1 hint names d/o");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (c2) old_layout=1 active txn, .preparing and .done are valid states. */
    {
        char root[PATH_MAX], p[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "c2"), 0, "mkdtemp c2");
        ASSERT_EQ_INT(lg_mkdir(root), 0, "c2 root");
        char d[PATH_MAX];
        snprintf(d, sizeof(d), "%s/d", root);
        ASSERT_EQ_INT(lg_mkdir(d), 0, "c2 d");
        snprintf(p, sizeof(p), "%s/schema.conf", root);
        ASSERT_EQ_INT(lg_write(p,
                               "d:o1:8:16:2:16:layout=packed\n"
                               "d:o2:8:16:2:16:layout=packed\n"
                               "d:o3:8:16:2:16:layout=packed\n"), 0, "c2 schema");
        for (int i = 1; i <= 3; i++) {
            snprintf(p, sizeof(p), "%s/o%d", d, i);
            ASSERT_EQ_INT(lg_mkdir(p), 0, "c2 object dir");
        }
        snprintf(p, sizeof(p), "%s/o1/.rebuild_txn.active", d);
        ASSERT_EQ_INT(lg_mkdir(p), 0, "c2 o1 active");
        snprintf(p, sizeof(p), "%s/o1/.rebuild_txn.active/meta", d);
        ASSERT_EQ_INT(lg_write(p, "version=1\nold_splits=8\nold_streams=16\n"
                                  "indexes_may_change=1\nold_layout=1\n"), 0, "c2 meta");
        snprintf(p, sizeof(p), "%s/o2/.rebuild_txn.preparing", d);
        ASSERT_EQ_INT(lg_mkdir(p), 0, "c2 o2 preparing");
        snprintf(p, sizeof(p), "%s/o3/.rebuild_txn.done", d);
        ASSERT_EQ_INT(lg_mkdir(p), 0, "c2 o3 done");
        err[0] = '\0';
        ASSERT_EQ_INT(lg_call(root, err, sizeof(err)), 0,
                      "(c2) old_layout=1/preparing/done all pass");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (c3) active txn with missing/garbled meta is invalid metadata. */
    {
        char root[PATH_MAX], p[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "c3"), 0, "mkdtemp c3");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16:layout=packed\n", 1, 0), 0,
                      "fixture c3");
        snprintf(p, sizeof(p), "%s/d/o/.rebuild_txn.active", root);
        ASSERT_EQ_INT(lg_mkdir(p), 0, "c3 active dir");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(c3) missing meta refused");
        ASSERT_CONTAINS(err, "invalid rebuild metadata under d/o",
                        "(c3) invalid-meta message");
        ASSERT_CONTAINS(err, "preserving it", "(c3) preserving message");
        snprintf(p, sizeof(p), "%s/d/o/.rebuild_txn.active/meta", root);
        ASSERT_EQ_INT(lg_write(p, "version=2\nnonsense\n"), 0, "c3 garbled meta");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(c3) garbled meta refused");
        ASSERT_CONTAINS(err, "invalid rebuild metadata under d/o",
                        "(c3) garbled meta message");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (d) clean packed object passes. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "d"), 0, "mkdtemp d");
        ASSERT_EQ_INT(lg_fixture(root, "d:o:8:16:2:16:layout=packed\n", 1, 0), 0,
                      "fixture d");
        err[0] = '\0';
        ASSERT_EQ_INT(lg_call(root, err, sizeof(err)), 0, "(d) clean packed root passes");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (e) no schema.conf passes (empty root). */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "e"), 0, "mkdtemp e");
        err[0] = '\0';
        ASSERT_EQ_INT(lg_call(root, err, sizeof(err)), 0, "(e) no schema.conf passes");
        tu_run_cmd("rm -rf %s", root);
    }

    /* (f) a hostile/escaping schema line is a parse failure, not a stat. */
    {
        char root[PATH_MAX];
        ASSERT_EQ_INT(lg_root(root, sizeof(root), "f"), 0, "mkdtemp f");
        ASSERT_EQ_INT(lg_fixture(root, "../escape:sweep:8:16:2:16\n", 0, 0), 0,
                      "fixture f");
        err[0] = '\0';
        ASSERT_TRUE(lg_call(root, err, sizeof(err)) != 0, "(f) hostile line refused");
        ASSERT_CONTAINS(err, "schema.conf is unreadable or malformed",
                        "(f) schema-unreadable message");
        tu_run_cmd("rm -rf %s", root);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

/* Daemon subprocess: cmd_server refuses before opening/forking and prints
   the hint to stderr. */
static int spawn_refusal(const char *base, const char *bin, int port,
                         const char *stderr_path) {
    pid_t pid = fork();
    if (pid < 0) return 0;
    if (pid == 0) {
        int fd = open(stderr_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd >= 0) { dup2(fd, STDERR_FILENO); close(fd); }
        chdir(base);
        execl(bin, bin, "server", (char *)NULL);
        _exit(127);
    }
    for (int i = 0; i < 30; i++) {
        int status;
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) return WIFEXITED(status) && WEXITSTATUS(status) != 0;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    return 0;
}

static int test_legacy_root_refused_daemon_run(void) {
    char base[256], root[300], p[400];
    snprintf(base, sizeof(base), "/tmp/shard-db-lgate-%d", (int)getpid());
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    snprintf(root, sizeof(root), "%s/db", base);
    mkdir(root, 0755);
    char logs[300];
    snprintf(logs, sizeof(logs), "%s/logs", base);
    mkdir(logs, 0755);

    int port = test_pick_port();
    ASSERT_TRUE(port > 0, "pick port");
    if (port <= 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    snprintf(p, sizeof(p), "%s/db.env", base);
    FILE *f = fopen(p, "w");
    ASSERT_NOT_NULL(f, "db.env");
    if (f) {
        fprintf(f, "export DB_ROOT=\"%s\"\nexport PORT=%d\n"
                   "export LOG_DIR=\"%s/logs\"\nexport LOG_LEVEL=2\n"
                   "export TLS_ENABLE=0\n", root, port, base);
        fclose(f);
    }
    snprintf(p, sizeof(p), "%s/.version", root);
    ASSERT_EQ_INT(lg_write(p, "2026.11.2\n"), 0, "version file");
    snprintf(p, sizeof(p), "%s/dirs.conf", root);
    ASSERT_EQ_INT(lg_write(p, "d\n"), 0, "dirs.conf");
    snprintf(p, sizeof(p), "%s/schema.conf", root);
    ASSERT_EQ_INT(lg_write(p, "d:o:8:16:2:16\n"), 0, "schema.conf");
    snprintf(p, sizeof(p), "%s/d", root);
    ASSERT_EQ_INT(lg_mkdir(p), 0, "d dir");
    snprintf(p, sizeof(p), "%s/d/o", root);
    ASSERT_EQ_INT(lg_mkdir(p), 0, "d/o dir");
    snprintf(p, sizeof(p), "%s/d/o/fields.conf", root);
    ASSERT_EQ_INT(lg_write(p, "n:int\n"), 0, "fields.conf");

    char bin[PATH_MAX];
    const char *rel = "./build/bin/shard-db";
    if (access(rel, X_OK) != 0) rel = "./shard-db";
    ASSERT_NOT_NULL(realpath(rel, bin), "shard-db binary found");

    char stderr_path[400];
    snprintf(stderr_path, sizeof(stderr_path), "%s/stderr.txt", base);
    int refused = spawn_refusal(base, bin, port, stderr_path);
    ASSERT_TRUE(refused, "daemon refuses to start on a legacy object");
    char *msg = tu_read_file(stderr_path);
    ASSERT_NOT_NULL(msg, "stderr captured");
    if (msg) {
        ASSERT_CONTAINS(msg, "is not in the packed layout", "stderr names the layout refusal");
        ASSERT_CONTAINS(msg, "d/o", "stderr names d/o");
        ASSERT_CONTAINS(msg, "2026.11.2", "stderr names the upgrade release");
        free(msg);
    }

    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-legacy-root-refused", test_legacy_root_refused_run)
TEST_REGISTER("test-legacy-root-refused-daemon", test_legacy_root_refused_daemon_run)
