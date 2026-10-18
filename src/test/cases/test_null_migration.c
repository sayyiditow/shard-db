/* src/test/cases/test_null_migration.c
 *
 * BTRI startup-migration contract tests (null plan Task 6 item 2, §2.4.3).
 *
 * The 2026.10.2 startup migration reindexes every indexed object
 * ('BTRH' → 'BTRI') inside shard_db_recover_before_stamp() — after
 * rebuild_recovery, before the clean-flag early-return, before KFM2
 * replay — and writes a durable completion stamp only after every
 * strict rebuild and the replay succeed. These cases exercise:
 *
 *   (a) a 'BTRH' fixture migrates before serving; first query on the
 *       field succeeds; rebuilt shards are 'BTRI'; stamp written;
 *   (b) the second start skips the migration (stamp present);
 *   (c) a build failure fails startup (INDEXED_ABORT_FAIL_AFTER seam);
 *   (d) an unindexed sibling object is unaffected throughout;
 *   (e) a durable KFM2 marker + de-stamped root: the next start
 *       reindexes first and the replay completes against 'BTRI';
 *   (f) an interrupted migration (mixed BTRH/BTRI, absent stamp)
 *       converges: every requested shard is rebuilt before serving,
 *       abandoned temp/spill files swept;
 *   (g) pre-existing object-rebuild staging (indexes.new) is resolved
 *       by rebuild_recovery before the migration scans the object;
 *   (h) post-rename publication fsync failure (BT_TEST_PUBLISH_FAIL_
 *       STAGE=2) and stamp-write fsync failures (BTRI_STAMP_TEST_FAIL)
 *       fail startup; the permissive checked wrapper accepts an
 *       unconfirmed publication while the strict wrapper rejects it.
 *
 * The 'BTRH' fixtures are real 'BTRI' index files with the magic bytes
 * flipped back — the migration never reads old index content (rebuild
 * scans segments), so a flipped-magic file is exactly the on-disk state
 * a pre-upgrade root presents.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include "shard_db_internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ── fixture helpers ──────────────────────────────────────────────── */

typedef struct {
    char base[256];      /* mkdtemp dir: db.env + logs + db_root */
    char db_root[300];
    int  port;
} MigRoot;

static void mig_root_init(MigRoot *m) {
    memset(m, 0, sizeof(*m));
    char tmpl[] = "/tmp/shard-db-mig-XXXXXX";
    char *d = mkdtemp(tmpl);
    ASSERT_NOT_NULL(d, "mkdtemp migration root");
    if (!d) return;
    snprintf(m->base, sizeof(m->base), "%s", d);
    snprintf(m->db_root, sizeof(m->db_root), "%s/data", d);
    ASSERT_EQ_INT(mkdir(m->db_root, 0755), 0, "mkdir db_root");
    m->port = test_pick_port();
    ASSERT_TRUE(m->port > 0, "pick port");
}

/* Write db.env at base/ — test_env_start_at honours an existing db.env
   (O_EXCL), so the knobs here reach the daemon verbatim. */
static void mig_write_env(const MigRoot *m, const char *extra) {
    char path[300];
    snprintf(path, sizeof(path), "%s/db.env", m->base);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f, "write db.env");
    if (!f) return;
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n",
        m->db_root, m->port, m->base);
    if (extra && extra[0]) fprintf(f, "%s", extra);
    fclose(f);
}

static void mig_root_destroy(MigRoot *m) {
    char cmd[400];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", m->base);
    tu_run_cmd("%s", cmd);
}

static TestClient *mig_connect(int port) {
    TestClientCfg cfg = { .host = "127.0.0.1", .port = port };
    return tc_connect(&cfg);
}

/* Build the fixture: indexed object mo(int k) with 5 rows + unindexed
   sibling plain(int v) with 2 rows. Daemon must be running. */
static void mig_build_fixture(TestClient *tc) {
    char *r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"mig\"}", &r), 0, "add-dir mig");
    ASSERT_NOT_NULL(r, "add-dir resp");
    ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "add-dir ok");
    free(r); r = NULL;
    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"mig\",\"object\":\"mo\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:int\"],\"indexes\":[\"k\"]}",
        &r), 0, "create mo");
    ASSERT_NOT_NULL(r, "create mo resp");
    ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "create mo ok");
    free(r); r = NULL;

    ASSERT_EQ_INT(tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"mig\",\"object\":\"plain\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:int\"]}",
        &r), 0, "create plain");
    ASSERT_NOT_NULL(r, "create plain resp");
    ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "create plain ok");
    free(r); r = NULL;

    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"key\":\"row%d\",\"value\":{\"k\":\"%d\"}}", i, i);
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert mo row");
        ASSERT_NOT_NULL(r, "insert resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "insert mo ok");
        free(r); r = NULL;
    }
    for (int i = 1; i <= 2; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"mig\",\"object\":\"plain\","
            "\"key\":\"p%d\",\"value\":{\"v\":\"%d\"}}", i, i * 10);
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert plain row");
        ASSERT_NOT_NULL(r, "insert plain resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "insert plain ok");
        free(r); r = NULL;
    }
}

/* ── on-disk surgery helpers ──────────────────────────────────────── */

/* On-disk byte patterns: BT_MAGIC is a little-endian u32 in the file
   header, so 'BTRI' (0x42545249) reads byte-wise as "IRTB" and 'BTRH'
   (0x42545248) as "HRTB" on x86/ARM64. */
#define BTRI_MAGIC "IRTB"
#define BTRH_MAGIC "HRTB"

static int idx_dir(char *out, size_t cap, const char *db_root,
                   const char *obj, const char *field) {
    return snprintf(out, cap, "%s/mig/%s/indexes/%s", db_root, obj, field);
}

/* Flip the 4-byte magic of every idx shard whose magic == `from`.
   Returns the number flipped, or -1 on I/O error. */
static int flip_idx_magic(const char *db_root, const char *obj,
                          const char *field, const char *from) {
    char dir[512];
    idx_dir(dir, sizeof(dir), db_root, obj, field);
    DIR *d = opendir(dir);
    if (!d) return -1;
    int flipped = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t nl = strlen(e->d_name);
        if (nl < 7 || strcmp(e->d_name + nl - 4, ".idx") != 0) continue;
        char p[600];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        int fd = open(p, O_RDWR);
        if (fd < 0) { closedir(d); return -1; }
        char magic[4];
        if (pread(fd, magic, 4, 0) != 4) { close(fd); closedir(d); return -1; }
        if (memcmp(magic, from, 4) == 0) {
            /* LE magic: 'BTRI' ↔ 'BTRH' differs in the first byte. */
            char out[4] = { (char)(from[0] == 'I' ? 'H' : 'I'),
                            from[1], from[2], from[3] };
            if (pwrite(fd, out, 4, 0) != 4) { close(fd); closedir(d); return -1; }
            flipped++;
        }
        close(fd);
    }
    closedir(d);
    return flipped;
}

/* 1 if every idx shard of `field` carries `magic` (and at least one exists). */
static int all_idx_magic(const char *db_root, const char *obj,
                         const char *field, const char *magic) {
    char dir[512];
    idx_dir(dir, sizeof(dir), db_root, obj, field);
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0, bad = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t nl = strlen(e->d_name);
        if (nl < 7 || strcmp(e->d_name + nl - 4, ".idx") != 0) continue;
        char p[600];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        int fd = open(p, O_RDONLY);
        if (fd < 0) { closedir(d); return 0; }
        char got[4];
        if (pread(fd, got, 4, 0) != 4 || memcmp(got, magic, 4) != 0) bad++;
        close(fd);
        n++;
    }
    closedir(d);
    return n > 0 && bad == 0;
}

static void stamp_path(char *out, size_t cap, const char *db_root) {
    snprintf(out, cap, "%s/.btri-migrated", db_root);
}

static int stamp_exists(const char *db_root) {
    char p[400];
    stamp_path(p, sizeof(p), db_root);
    struct stat st;
    return stat(p, &st) == 0;
}

static void stamp_remove(const char *db_root) {
    char p[400];
    stamp_path(p, sizeof(p), db_root);
    ASSERT_EQ_INT(unlink(p), 0, "remove migration stamp");
}

/* Nanosecond mtime: glibc names the field st_mtim, Darwin st_mtimespec —
   both struct timespec, so one accessor covers both. */
#ifdef __APPLE__
#define ST_MTIM(st) ((st).st_mtimespec)
#else
#define ST_MTIM(st) ((st).st_mtim)
#endif

/* mtime (sec+nsec) of every idx shard, summarised so (b) can assert
   "no shard was rewritten". */
static int idx_mtime_sum(const char *db_root, const char *obj,
                         const char *field, long long *out_sum) {
    char dir[512];
    idx_dir(dir, sizeof(dir), db_root, obj, field);
    DIR *d = opendir(dir);
    if (!d) return -1;
    long long sum = 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t nl = strlen(e->d_name);
        if (nl < 7 || strcmp(e->d_name + nl - 4, ".idx") != 0) continue;
        char p[600];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        struct stat st;
        if (stat(p, &st) != 0) { closedir(d); return -1; }
        sum += (long long)ST_MTIM(st).tv_sec * 1000000000LL
             + (long long)ST_MTIM(st).tv_nsec;
        n++;
    }
    closedir(d);
    *out_sum = (n > 0) ? sum : -1;
    return (n > 0) ? 0 : -1;
}

/* Single check: is a live `.durability-test-<phase>.active` marker
   present under dir? `phase` may end in '*' to match any suffix
   (e.g. "shard-published-*" — the per-kf-shard phase carries the shard
   id). */
static int pause_marker_present(const char *dir, const char *phase) {
    int globbed = 0;
    char tail[128] = "";
    const char *star = strchr(phase, '*');
    if (star) {
        globbed = 1;
        snprintf(tail, sizeof(tail), "%s", star + 1);
    }
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *e;
    int hit = 0;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, ".durability-test-", 17) != 0)
            continue;
        const char *rest = e->d_name + 17;
        size_t rl = strlen(rest);
        size_t sl = strlen(".active");
        if (rl <= sl || strcmp(rest + rl - sl, ".active") != 0)
            continue;
        size_t body_len = rl - sl;              /* strip .active */
        if (globbed) {
            size_t pl = (size_t)(star - phase);  /* body before '*' */
            size_t tl = strlen(tail);
            if (body_len < pl + tl) continue;
            if (strncmp(rest, phase, pl) != 0) continue;
            if (strcmp(rest + body_len - tl, tail) != 0) continue;
        } else {
            if (body_len != strlen(phase) ||
                memcmp(rest, phase, body_len) != 0) continue;
        }
        hit = 1;
        break;
    }
    closedir(d);
    return hit;
}

/* Fork+exec the daemon binary with CWD=base and stdout/stderr diverted
   to refuse.log. Returns the child pid, or 0 after a failed assert
   (binary not found / fork failed) — callers must treat 0 as no child
   and skip every kill/waitpid on it. */
static pid_t fork_raw_daemon(const MigRoot *m) {
    /* Prefer the TEST_BUILD daemon — the seam knobs (fail-next-build,
       publish-fail stage, durability pauses) only exist there. Mirrors
       fixtures.c's binary resolution order. */
    const char *cands[] = { "./build/bin/shard-db-test-server",
                            "./build/bin/shard-db",
                            "./shard-db", NULL };
    char bin[PATH_MAX];
    int found = 0;
    for (int i = 0; cands[i]; i++) {
        if (access(cands[i], X_OK) == 0) {
            if (!realpath(cands[i], bin)) continue;
            found = 1;
            break;
        }
    }
    ASSERT_TRUE(found, "locate daemon binary for raw spawn");
    if (!found) return 0;

    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0, "fork raw daemon");
    if (pid > 0) return pid;
    if (pid != 0) return 0;   /* fork failed; assert already recorded */
    chdir(m->base);
    char logp[400];
    snprintf(logp, sizeof(logp), "%s/refuse.log", m->base);
    int lfd = open(logp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (lfd >= 0) { dup2(lfd, 1); dup2(lfd, 2); close(lfd); }
    execl(bin, bin, "server", (char *)NULL);
    _exit(127);
}

/* Raw daemon spawn: fork + exec `server`, return the raw waitpid status
   once the child exits within timeout_ms (*exited=1). If it is still
   alive at the deadline, SIGKILL it and return -1 (*exited=0 — the
   child is reaped here, the caller owns nothing). Refusal cases use
   this: their daemons exit on their own, so the deadline is only a
   backstop. */
static int spawn_daemon_raw(const MigRoot *m, int timeout_ms, int *exited,
                            pid_t *out_pid) {
    pid_t pid = fork_raw_daemon(m);
    if (pid <= 0) { *exited = -1; *out_pid = 0; return -1; }
    *out_pid = pid;
    *exited = 0;
    int status = 0;
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 100) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) { *exited = 1; return status; }
        usleep(100000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    return -1;
}

/* Raw-spawn the daemon and SIGKILL it at the FIRST sighting of a
   durability pause marker, with a WNOHANG liveness check riding the same
   poll loop. Killing mid-pause keeps the marker on disk (the hook's
   unlink never runs) and pins the interrupt point to the first shard
   publication regardless of machine speed — a fixed kill deadline
   instead races the migration's *second* 30s pause, and slow macOS
   runners overshoot the deadline, letting the migration finish (stamp
   written, all shards BTRI) before the test interrupts. Returns 0 only
   when the marker was seen while the child was still alive;
   *saw_marker and *alive_at_marker report which precondition failed.
   The child is always killed and reaped before return. */
static int spawn_kill_at_pause(const MigRoot *m, const char *marker_dir,
                               const char *phase, int timeout_ms,
                               int *saw_marker, int *alive_at_marker) {
    *saw_marker = 0;
    *alive_at_marker = 0;
    pid_t pid = fork_raw_daemon(m);
    if (pid <= 0) return -1;
    int status = 0;
    int exited = 0;
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 50) {
        if (waitpid(pid, &status, WNOHANG) == pid) { exited = 1; break; }
        if (pause_marker_present(marker_dir, phase)) {
            *saw_marker = 1;
            *alive_at_marker = 1;
            break;
        }
        usleep(50000);
    }
    if (!exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    return *saw_marker ? 0 : -1;
}

/* Dump the raw-spawn daemon log — used when a refusal case doesn't
   refuse, so CI shows the daemon's actual output. */
static void dump_refuse_log(const MigRoot *m) {
    char logp[400];
    snprintf(logp, sizeof(logp), "%s/refuse.log", m->base);
    char *txt = tu_read_file(logp);
    if (txt) {
        fprintf(stderr, "---- refuse.log ----\n%s\n--------------------\n",
                txt);
        free(txt);
    }
}

static int port_refuses(int port) {
    TestClient *tc = mig_connect(port);
    if (tc) { tc_close(tc); return 0; }
    return 1;
}

/* Poll until a durability pause marker appears. `phase` may end in '*'
   (see pause_marker_present). */
static int wait_pause_marker(const char *dir, const char *phase,
                             int timeout_ms) {
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 50) {
        if (pause_marker_present(dir, phase)) return 0;
        usleep(50000);
    }
    return -1;
}

/* ── (a)+(b)+(d): migrate before serving; stamp skips; sibling intact ── */

static int test_null_migration_abd(void) {
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    if (t_ctx->failed) { mig_root_destroy(&m); return 1; }
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) {
        mig_build_fixture(tc);

        /* First start of this binary on a fresh root stamps completion. */
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "fresh start writes stamp");
        tc_close(tc);
    }
    test_env_stop_keep(&env);

    /* (a): flip to BTRH, drop the stamp — the pre-upgrade state. */
    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flipped at least one shard to BTRH");
    ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRH_MAGIC), 1,
                  "all mo shards BTRH before migration");
    stamp_remove(m.db_root);

    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 2");
    tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 2");
    if (tc) {
        /* Migration reindexed before serving: the first query on the
         * field succeeds and every shard is BTRI again. */
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"3\"}]}",
            &r), 0, "find eq 3 post-migration");
        ASSERT_NOT_NULL(r, "find resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, "row3"),
                    "eq 3 finds row3 after migration");
        free(r); r = NULL;

        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "all mo shards BTRI after migration");
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp written post-migration");

        /* (d): unindexed sibling unaffected throughout. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"mig\",\"object\":\"plain\","
            "\"keys\":[\"p1\",\"p2\"]}", &r), 0, "get plain");
        ASSERT_NOT_NULL(r, "get plain resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, "p1"),
                    "unindexed sibling serves");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop_keep(&env);

    /* (b): second start skips the migration — shard mtimes unchanged. */
    long long before = 0;
    ASSERT_EQ_INT(idx_mtime_sum(m.db_root, "mo", "k", &before), 0,
                  "stat shards before (b)");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 3");
    tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 3");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"3\"}]}",
            &r), 0, "find eq 3 (b)");
        ASSERT_NOT_NULL(r, "find resp (b)");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "query works (b)");
        free(r);
        tc_close(tc);
    }
    long long after = 0;
    ASSERT_EQ_INT(idx_mtime_sum(m.db_root, "mo", "k", &after), 0,
                  "stat shards after (b)");
    ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp still present (b)");
    ASSERT_EQ_INT(before == after, 1, "stamped start did NOT rewrite shards");

    test_env_stop(&env);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-abd", test_null_migration_abd)

/* ── (c): migration build failure fails startup ───────────────────── */

static int test_null_migration_c_refuses(void) {
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) { mig_build_fixture(tc); tc_close(tc); }
    test_env_stop_keep(&env);

    /* Pre-upgrade state + a one-shot seam that fails the reindex build
       itself (before any publication). The strict migration must fail
       the whole startup. */
    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flip to BTRH");
    stamp_remove(m.db_root);
    mig_write_env(&m, "INDEX_TEST_FAIL_NEXT_BUILD=5\n");

    int exited = 0;
    pid_t raw_pid = 0;
    int status = spawn_daemon_raw(&m, 30000, &exited, &raw_pid);
    if (!exited) dump_refuse_log(&m);
    ASSERT_EQ_INT(exited, 1, "daemon exited (startup refused)");
    if (exited) {
        ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) != 0,
                    "daemon exit status non-zero");
    }
    ASSERT_EQ_INT(stamp_exists(m.db_root), 0, "no stamp on failed migration");
    ASSERT_EQ_INT(port_refuses(m.port), 1, "refusing daemon never serves");

    /* Recovery: disarm the seam and restart — migration completes. */
    mig_write_env(&m, NULL);
    TestEnv env2;
    ASSERT_EQ_INT(test_env_start_at(&env2, m.db_root, m.port), 0,
                  "restart after failure");
    TestClient *tc2 = mig_connect(m.port);
    ASSERT_NOT_NULL(tc2, "connect after recovery");
    if (tc2) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc2,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"5\"}]}",
            &r), 0, "find eq 5 after recovery");
        ASSERT_NOT_NULL(r, "find resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, "row5"),
                    "migration completed after disarming the seam");
        free(r);
        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "shards BTRI after recovery");
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp after recovery");
        tc_close(tc2);
    }
    test_env_stop(&env2);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-c-refuses", test_null_migration_c_refuses)

/* ── (e): durable marker + destamped root → reindex first, replay ──── */

static int test_null_migration_e_marker_order(void) {
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) { mig_build_fixture(tc); tc_close(tc); }
    test_env_stop_keep(&env);

    /* Run 2: pause the indexed bulk mid-apply — win-I fires inside the
       per-shard index apply, AFTER the chain's markers are published and
       dir-durable. SIGKILL there leaves every durable intent behind with
       the affected windows not yet fully applied. */
    mig_write_env(&m, "DURABILITY_TEST_PAUSE_PHASE=win-I\n"
                      "DURABILITY_TEST_PAUSE_MS=30000\n");
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0,
                  "start 2 (pause armed)");
    tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 2");
    if (tc) {
        /* Send-only: the request parks inside the paused shard chain.
           Track the remaining space explicitly — feeding snprintf's return
           back in lets a (here impossible) truncation underflow the size
           argument. Clamp so the bound holds by construction. */
        char bulk[4096];
        int off = snprintf(bulk, sizeof(bulk),
            "{\"mode\":\"bulk-insert\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"records\":{");
        int rem = (int)sizeof(bulk) - off;
        for (int i = 6; i <= 45 && rem > 0; i++) {
            int n = snprintf(bulk + off, (size_t)rem,
                             "%s\"r%d\":{\"k\":\"%d\"}", i > 6 ? "," : "", i, i);
            if (n < 0) break;
            if (n >= rem) n = rem - 1;
            off += n; rem -= n;
        }
        if (rem > 2)
            snprintf(bulk + off, (size_t)rem, "}}");
        ASSERT_EQ_INT(tc_send(tc, bulk), 0, "send paused bulk");
        /* durability_test_pause writes the marker under the object dir
           (SlotcaskDb.data_dir = <tenant>/<object>), not the DB_ROOT. */
        char mo_data[600];
        snprintf(mo_data, sizeof(mo_data), "%s/mig/mo", m.db_root);
        if (wait_pause_marker(mo_data, "win-I", 20000) != 0) {
            /* Diagnostic: list every durability-test marker under the
               object data dir, then drain the bulk response. */
            DIR *dd = opendir(mo_data);
            if (dd) {
                struct dirent *de;
                fprintf(stderr, "---- markers in mo/data ----\n");
                while ((de = readdir(dd)))
                    if (strncmp(de->d_name, ".durability-test-", 17) == 0)
                        fprintf(stderr, "  %s\n", de->d_name);
                closedir(dd);
            }
            char *resp = NULL;
            if (tc_recv(tc, &resp) == 0 && resp) {
                fprintf(stderr, "---- bulk response ----\n%s\n", resp);
                free(resp);
            }
        }
        ASSERT_EQ_INT(wait_pause_marker(mo_data, "win-I", 20000),
                      0, "win-I pause marker reached");
        tc_close(tc);
    }
    test_env_kill(&env);

    /* Surgically recreate the upgrade-crash state: marker durable on
       disk, indexes flipped back to BTRH, completion stamp gone. */
    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flip to BTRH (e)");
    stamp_remove(m.db_root);

    /* Run 3: the migration must reindex BEFORE the replay, and the
       replay then completes against BTRI. */
    mig_write_env(&m, NULL);
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 3");
    tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 3");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"mo\"}",
            &r), 0, "count (e)");
        ASSERT_NOT_NULL(r, "count resp (e)");
        ASSERT_TRUE(atoi(r) == 45, "replayed rows present (45 total)");
        free(r); r = NULL;

        /* Every replayed row reachable through the migrated index. */
        for (int i = 6; i <= 45; i++) {
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
                "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"%d\"}]}",
                i);
            ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "eq replayed row");
            ASSERT_NOT_NULL(r, "eq resp");
            char want[32];
            snprintf(want, sizeof(want), "r%d", i);
            ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, want),
                        "replayed row found via index");
            free(r); r = NULL;
        }
        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "shards BTRI after replay");
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp after replay");
        tc_close(tc);
    }
    test_env_stop(&env);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-e-marker-order",
              test_null_migration_e_marker_order)

/* ── (f): interrupted migration (mixed magic, no stamp) converges ──── */

static int test_null_migration_f_crash_retry(void) {
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) { mig_build_fixture(tc); tc_close(tc); }
    test_env_stop_keep(&env);

    /* Pre-upgrade state + a pause INSIDE the migration's first shard
       publication (bt-publish-before-rename fires in bt_publish_replace).
       The migration runs before the listener opens, so spawn raw and
       SIGKILL on the first marker sighting under the field's index dir —
       the interrupt lands mid-first-publication at any machine speed. */
    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flip to BTRH (f)");
    stamp_remove(m.db_root);
    mig_write_env(&m, "DURABILITY_TEST_PAUSE_PHASE=bt-publish-before-rename\n"
                      "DURABILITY_TEST_PAUSE_MS=30000\n");

    int saw_marker = 0, alive_at_marker = 0;
    char idx_field_dir[600];
    idx_dir(idx_field_dir, sizeof(idx_field_dir), m.db_root, "mo", "k");
    spawn_kill_at_pause(&m, idx_field_dir, "bt-publish-before-rename",
                        60000, &saw_marker, &alive_at_marker);
    if (!saw_marker) dump_refuse_log(&m);
    ASSERT_EQ_INT(alive_at_marker, 1,
                  "daemon alive while paused pre-publication");
    ASSERT_EQ_INT(saw_marker, 1, "publication pause reached");
    sleep(1);

    /* Interrupted state: stamp absent. Publication was paused
       pre-rename, so shard 000 is still BTRH — flip it to BTRI by hand
       to construct the mixed live object the plan describes. */
    ASSERT_EQ_INT(stamp_exists(m.db_root), 0, "no stamp after interrupt");
    {
        char dir[600];
        idx_dir(dir, sizeof(dir), m.db_root, "mo", "k");
        char p[700];
        snprintf(p, sizeof(p), "%s/000.idx", dir);
        int fd = open(p, O_RDWR);
        ASSERT_TRUE(fd >= 0, "open shard 000");
        if (fd >= 0) {
            ASSERT_EQ_INT(pwrite(fd, BTRI_MAGIC, 4, 0), 4,
                          "flip 000 to BTRI");
            close(fd);
        }
    }
    ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRH_MAGIC), 0,
                  "not all BTRH (mixed state)");
    ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 0,
                  "not all BTRI (mixed state)");

    /* Restart clean: absent stamp ⇒ migration rebuilds EVERY requested
       shard (never infers completion from a BTRI subset), the temp/spill
       sweep runs, and the object serves with correct contents. */
    mig_write_env(&m, NULL);
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0,
                  "restart (f)");
    tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect (f) restart");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"mo\"}",
            &r), 0, "count (f)");
        ASSERT_NOT_NULL(r, "count resp (f)");
        ASSERT_TRUE(atoi(r) == 5, "all rows served (f)");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"gte\",\"value\":\"1\"}]}",
            &r), 0, "range via index (f)");
        ASSERT_NOT_NULL(r, "range resp (f)");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL &&
                    strstr(r, "row1") && strstr(r, "row5"),
                    "index-driven range complete after convergence");
        free(r);
        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "every requested shard rebuilt to BTRI");
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp written (f)");

        /* index_rebuild_temp_sweep consumed the abandoned rebuild
           temp/spill artifacts: only NNN.idx names remain. */
        char field_dir[600];
        idx_dir(field_dir, sizeof(field_dir), m.db_root, "mo", "k");
        DIR *d = opendir(field_dir);
        ASSERT_NOT_NULL(d, "open field index dir");
        if (d) {
            struct dirent *e;
            int junk = 0;
            while ((e = readdir(d))) {
                if (e->d_name[0] == '.') continue;
                size_t nl = strlen(e->d_name);
                if (nl < 7 || strcmp(e->d_name + nl - 4, ".idx") != 0) junk++;
            }
            closedir(d);
            ASSERT_EQ_INT(junk, 0, "no abandoned temp/spill files remain");
        }
        tc_close(tc);
    }
    test_env_stop(&env);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-f-crash-retry", test_null_migration_f_crash_retry)

/* ── (g): object-rebuild staging resolved before migration scans ───── */

static int test_null_migration_g_staging_first(void) {
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) { mig_build_fixture(tc); tc_close(tc); }
    test_env_stop_keep(&env);

    /* One start carrying BOTH: unresolved object-rebuild staging
       (indexes.new with a stale file) AND a BTRH root needing
       migration. rebuild_recovery must resolve the staging before the
       migration scans the object; both outcomes visible after start. */
    char obj_dir[600], staging[700];
    snprintf(obj_dir, sizeof(obj_dir), "%s/mig/mo", m.db_root);
    snprintf(staging, sizeof(staging), "%s/indexes.new", obj_dir);
    ASSERT_EQ_INT(tu_run_cmd("mkdir -p %s", staging), 0, "mkdir staging");
    ASSERT_EQ_INT(tu_run_cmd("echo stale > %s/stale.idx", staging), 0,
                  "drop stale idx into staging");
    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flip to BTRH (g)");
    stamp_remove(m.db_root);

    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 2");
    TestClient *tc2 = mig_connect(m.port);
    ASSERT_NOT_NULL(tc2, "connect 2");
    if (tc2) {
        char *r = NULL;
        struct stat st;
        ASSERT_TRUE(stat(staging, &st) != 0,
                    "indexes.new resolved by rebuild_recovery");
        ASSERT_EQ_INT(tc_request(tc2,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"2\"}]}",
            &r), 0, "find eq 2 (g)");
        ASSERT_NOT_NULL(r, "find resp (g)");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, "row2"),
                    "migration completed despite staging (g)");
        free(r);
        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "shards BTRI (g)");
        ASSERT_EQ_INT(stamp_exists(m.db_root), 1, "stamp (g)");
        tc_close(tc2);
    }
    test_env_stop(&env);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-g-staging-first", test_null_migration_g_staging_first)

/* ── (h): unconfirmed publication + stamp-write failures fail startup ──
 *
 * Also the plan's test-first demonstration: with a post-rename fsync
 * failure armed, the PERMISSIVE checked wrapper returns success while
 * the STRICT wrapper rejects the same build result. */

static int test_null_migration_h_durability_gate(void) {
    /* Unit half (process-local db): permissive vs strict on the SAME
       injected unconfirmed publication. */
    {
        ShardDb *db = test_get_process_db();
        ASSERT_NOT_NULL(db, "process db");
        const char *root = test_get_process_db_root();
        ASSERT_NOT_NULL(root, "process db root");
        if (db && root) {
            char *r = NULL;
            ASSERT_EQ_INT(tu_pdb_request(db,
                "{\"mode\":\"add-dir\",\"dir\":\"migunit\"}", &r), 0,
                "unit add-dir");
            free(r); r = NULL;
            ASSERT_EQ_INT(tu_pdb_request(db,
                "{\"mode\":\"create-object\",\"dir\":\"migunit\","
                "\"object\":\"u1\",\"splits\":8,\"max_key\":16,"
                "\"fields\":[\"k:int\"],\"indexes\":[\"k\"]}", &r), 0,
                "unit create");
            ASSERT_NOT_NULL(r, "unit create resp");
            ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "unit create ok");
            free(r); r = NULL;
            ASSERT_EQ_INT(tu_pdb_request(db,
                "{\"mode\":\"insert\",\"dir\":\"migunit\",\"object\":\"u1\","
                "\"key\":\"a\",\"value\":{\"k\":\"1\"}}", &r), 0, "unit insert");
            free(r); r = NULL;

            char eff[512];
            snprintf(eff, sizeof(eff), "%s/migunit", root);

            /* Fire the post-rename parent-durability reporting failure on
               the first shard publication (one-shot seam). */
            btree_test_publish_fail_stage(2);
            int n = 0, eno = 0;
            int permissive = reindex_object_checked_ex(eff, "u1", 0, &n, &eno);
            ASSERT_EQ_INT(permissive, 0,
                          "permissive wrapper accepts unconfirmed publication");

            btree_test_publish_fail_stage(2);
            int strict = reindex_object_durable_checked_ex(eff, "u1", 0, &n, &eno);
            ASSERT_EQ_INT(strict, -1,
                          "strict wrapper rejects unconfirmed publication");
            ASSERT_EQ_INT(eno, EIO, "strict reports injected errno");

            /* Disarmed: the strict wrapper succeeds on a confirmed build. */
            int ok = reindex_object_durable_checked_ex(eff, "u1", 0, &n, &eno);
            ASSERT_EQ_INT(ok, 0, "strict accepts confirmed rebuild");
            ASSERT_TRUE(n >= 1, "strict rebuilt the index");

            tu_pdb_drop_object(db, "migunit", "u1");
        }
    }

    /* Daemon half: BT_TEST_PUBLISH_FAIL_STAGE=2 makes the migration's
       first shard publication unconfirmed → startup refuses, no stamp. */
    MigRoot m;
    mig_root_init(&m);
    if (t_ctx->failed) return 1;
    mig_write_env(&m, NULL);

    TestEnv env;
    ASSERT_EQ_INT(test_env_start_at(&env, m.db_root, m.port), 0, "start 1");
    TestClient *tc = mig_connect(m.port);
    ASSERT_NOT_NULL(tc, "connect 1");
    if (tc) { mig_build_fixture(tc); tc_close(tc); }
    test_env_stop_keep(&env);

    ASSERT_TRUE(flip_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC) >= 1,
                "flip to BTRH (h)");
    stamp_remove(m.db_root);
    mig_write_env(&m, "BT_TEST_PUBLISH_FAIL_STAGE=2\n");

    int exited = 0;
    pid_t raw_pid = 0;
    int status = spawn_daemon_raw(&m, 30000, &exited, &raw_pid);
    if (!exited) dump_refuse_log(&m);
    ASSERT_EQ_INT(exited, 1, "daemon exited on unconfirmed publication");
    if (exited) {
        ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) != 0,
                    "unconfirmed publication exits non-zero");
    }
    ASSERT_EQ_INT(stamp_exists(m.db_root), 0,
                  "no stamp attempted on unconfirmed build");
    ASSERT_EQ_INT(port_refuses(m.port), 1, "never serves (h)");

    /* Stamp-file fsync failure: migration builds fine, the stamp write
       fails, startup refuses, and no stamp exists. */
    mig_write_env(&m, "BTRI_STAMP_TEST_FAIL=1\n");
    exited = 0;
    status = spawn_daemon_raw(&m, 60000, &exited, &raw_pid);
    if (!exited) dump_refuse_log(&m);
    ASSERT_EQ_INT(exited, 1, "daemon exited on stamp fsync failure");
    ASSERT_EQ_INT(stamp_exists(m.db_root), 0,
                  "stamp-file fsync failure leaves no stamp");
    ASSERT_EQ_INT(port_refuses(m.port), 1, "never serves (stamp fsync)");

    /* Stamp-directory fsync failure: the renamed stamp MAY be visible,
       but startup still refuses — every index was already durable
       before that rename. */
    mig_write_env(&m, "BTRI_STAMP_TEST_FAIL=2\n");
    exited = 0;
    status = spawn_daemon_raw(&m, 60000, &exited, &raw_pid);
    if (!exited) dump_refuse_log(&m);
    ASSERT_EQ_INT(exited, 1, "daemon exited on stamp dir-fsync failure");
    int stamped = stamp_exists(m.db_root);
    if (stamped) {
        ASSERT_EQ_INT(all_idx_magic(m.db_root, "mo", "k", BTRI_MAGIC), 1,
                      "visible-but-unconfirmed stamp: indexes already durable");
    }
    ASSERT_EQ_INT(port_refuses(m.port), 1, "never serves (stamp dir fsync)");

    /* Recovery: knobs removed — the visible stamp short-circuits the
       migration (or it re-runs and stamps); either way the root serves. */
    mig_write_env(&m, NULL);
    TestEnv env2;
    ASSERT_EQ_INT(test_env_start_at(&env2, m.db_root, m.port), 0,
                  "restart after stamp failures");
    TestClient *tc2 = mig_connect(m.port);
    ASSERT_NOT_NULL(tc2, "connect after stamp failures");
    if (tc2) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc2,
            "{\"mode\":\"find\",\"dir\":\"mig\",\"object\":\"mo\","
            "\"criteria\":[{\"field\":\"k\",\"op\":\"eq\",\"value\":\"4\"}]}",
            &r), 0, "find eq 4 after recovery");
        ASSERT_NOT_NULL(r, "find resp");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL && strstr(r, "row4"),
                    "serving after stamp-failure recovery");
        free(r);
        tc_close(tc2);
    }
    test_env_stop(&env2);
    mig_root_destroy(&m);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-migration-h-durability-gate",
              test_null_migration_h_durability_gate)
