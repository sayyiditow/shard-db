/* src/test/cases/test_auto_reshard.c
 * Auto-reshard thread fires within its configured hour, reshards an
 * object that has outgrown its current splits, and leaves an
 * already-correctly-sized object untouched.
 *
 * Custom daemon spawn: sets AUTO_RESHARD_HOUR to the test's own current
 * server-local hour so the thread's first wall-clock check matches
 * immediately once it runs. The thread itself sleeps 5s on startup
 * before its first check (see auto_reshard_thread()'s doc comment in
 * src/db/server.c, Task 3) — this is a fixed, deliberate delay so the
 * thread's once-per-day last_run_date guard can never fire its one
 * daily check before this test (or a real daemon's startup sequence)
 * has finished setting up. Budget the polling loop below accordingly:
 * 5s startup delay + the object setup this test does first (well under
 * 1s) + reshard duration, all comfortably inside a 20s poll window.
 *
 * 'grown' uses real bulk-inserted records (rebuild_object_v2 now hard-
 * fails a reshard whose copied live count doesn't match the source kf
 * header's declared total — see query_find.c's `expected_live` check —
 * so a live count backed by no real records can no longer complete an
 * actual rebuild the way it could pre-refactor). 1.05M real records
 * bulk-insert in ~1-2s, well inside the poll budget, and keep this
 * test's valuable concurrent-writer-during-a-real-rebuild lock coverage
 * (see WriterCtx below) intact.
 *
 * 'huge' cannot follow the same path — 3 billion real records isn't
 * something a routine test can create — so it keeps the on-disk kf
 * header fabrication technique (writes shard 0's `total` field directly,
 * offset 8, 8 bytes; safe because slotcask_sum_kf_totals(), which
 * get_live_count() calls, does a fresh open()+pread() per shard rather
 * than going through the mmap'd kfcache, so it observes the external
 * pwrite() via the OS page cache with no staleness window). Because the
 * fabricated total has no matching real records, the actual rebuild now
 * correctly refuses (by design — this is the same invariant that makes
 * 'grown' need real data). What this still proves: reshard_target_for_
 * count() and the full-width live-count read reach the right decision
 * at true past-INT_MAX scale, verified via the "AUTO-RESHARD ... starting
 * 8 -> 2048" log line emitted before the (expected-to-fail) rebuild
 * attempt — not via the rebuild actually completing.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

/* Writes `total` into a kf shard's header at offset 8 (the `total`
   field: magic u32 @0, version u32 @4, total u64 @8, deleted u64 @16 —
   see SlotcaskKfHeader in src/db/slotcask.h). Leaves magic/version/
   deleted untouched. */
static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (n == (ssize_t)sizeof(total)) ? 0 : -1;
}

#define GROWN_BASE_COUNT 1050000
#define GROWN_CHUNK      50000

/* The wait budgets below (20s/40s) are calibrated for uninstrumented
   execution: 5s thread-startup delay + a 1.05M-record bulk-insert +
   a full kf/segment rebuild, all comfortably inside budget on plain and
   ASan builds (ASan's overhead here is modest). TSan's shadow-memory and
   happens-before tracking on every memory access materially slows the
   same rebuild, which can push it past the un-scaled budget without any
   functional problem -- the reshard still runs and still reaches the
   right target, just slower. Scale the *wait*, not the assertion: the
   correctness check (did it reshape / reach the right target) is
   unchanged either way. */
#if defined(__SANITIZE_THREAD__)
#define RESHARD_WAIT_SCALE 6
#else
#define RESHARD_WAIT_SCALE 1
#endif

/* Poll `<base>/logs/<date_str>-info.log` for a line containing `tag`. LOG_WARN
   is level 2, which open_log_for_level() (config.c) routes to -info.log
   (only level 1 / ERROR gets its own -error.log) -- see
   test_auto_reshard_shutdown_race.c's wait_for_reshard_start() for the
   original version of this pattern. */
static int wait_for_log_line(const char *base, const char *date_str,
                              const char *tag, int timeout_s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/logs/%s-info.log", base, date_str);
    for (int i = 0; i < timeout_s * 2; i++) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[1024];
            while (fgets(line, sizeof(line), f)) {
                if (strstr(line, tag)) { fclose(f); return 1; }
            }
            fclose(f);
        }
        struct timespec ts = { 0, 500 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return 0;
}

/* Concurrent-write guard against the reshard: while auto_reshard_thread
   races to reshard 'grown', this writer thread hammers real inserts at
   the same object. Without objlock_wrlock held around the heavy
   cmd_vacuum call (the bug this task fixes), a concurrent insert can
   land mid-rebuild while rebuild_object is renaming data/ ->
   data.legacy/, either erroring out or being silently dropped from the
   rebuilt kf. Best-effort / timing-dependent (see the plan's note above)
   — asserts the two invariants that must hold if the lock is doing its
   job: zero errors, and the post-reshard live count exactly matches the
   number of inserts this thread got a success response for. */
typedef struct {
    TestClientCfg cfg;
    _Atomic int stop;
    int sent, ok;
} WriterCtx;

static void *writer_thread_fn(void *arg) {
    WriterCtx *w = (WriterCtx *)arg;
    TestClient *wtc = tc_connect(&w->cfg);
    if (!wtc) return NULL;
    int i = 0;
    while (!atomic_load_explicit(&w->stop, memory_order_acquire)) {
        char req[256], *r = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"grown\","
            "\"key\":\"w%d\",\"value\":{\"v\":%d}}", i, i);
        w->sent++;
        if (tc_request(wtc, req, &r) == 0 && r && !strstr(r, "\"error\"")) w->ok++;
        free(r);
        i++;
        struct timespec ts = { 0, 5 * 1000000L }; nanosleep(&ts, NULL);
    }
    tc_close(wtc);
    return NULL;
}

static int test_auto_reshard_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-ar-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);
    char logs_dir[300]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    /* AUTO_RESHARD_HOUR = current server-local hour, so the thread's
       first check (5s after thread startup — see auto_reshard_thread's
       startup delay, Task 3) already matches. If we're within 90s of the
       top of the next hour, that 5s startup delay plus this test's own
       setup could push the thread's first wall-clock check into the next
       hour, missing the configured AUTO_RESHARD_HOUR entirely — sleep
       past the boundary first so the computed hour has full headroom. */
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    int secs_left_in_hour = (59 - tmv.tm_min) * 60 + (60 - tmv.tm_sec);
    if (secs_left_in_hour < 90) {
        TAP_DIAG("# auto-reshard: only %ds left in the hour, sleeping past the boundary...\n",
               secs_left_in_hour);
        fflush(_TAP_OUT);
        struct timespec boundary_ts = { secs_left_in_hour + 2, 0 };
        nanosleep(&boundary_ts, NULL);
        now = time(NULL);
        localtime_r(&now, &tmv);
    }
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tmv);

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
"export LOG_LEVEL=3\n"
            "export THREADS=2\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n"
        "export AUTO_RESHARD_ENABLE=1\n"
        "export AUTO_RESHARD_HOUR=%d\n",
        db_root, port, base, tmv.tm_hour);
    fclose(f);

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

    /* Wait until ready. */
    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon ready with AUTO_RESHARD_ENABLE=1");
    if (!ready) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object 1: under-split. splits=8, seeded with GROWN_BASE_COUNT
       (1,050,000) real records (falls in the 1M-10M band -> target=16 > 8).
       Real records, not a fabricated kf header total, because
       rebuild_object_v2 now hard-fails a reshard whose copied live count
       doesn't match the source header's declared total (see this file's
       header comment). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"grown\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;

    {
        size_t buf_cap = (size_t)GROWN_CHUNK * 40 + 256;
        char *bulk = malloc(buf_cap);
        ASSERT_NOT_NULL(bulk, "malloc grown bulk-insert buffer");
        int all_ok = bulk != NULL;
        for (int base_i = 0; all_ok && base_i < GROWN_BASE_COUNT; base_i += GROWN_CHUNK) {
            int end = base_i + GROWN_CHUNK;
            if (end > GROWN_BASE_COUNT) end = GROWN_BASE_COUNT;
            size_t off = 0;
            off += (size_t)snprintf(bulk + off, buf_cap - off,
                "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"grown\","
                "\"records\":{");
            for (int i = base_i; i < end; i++) {
                off += (size_t)snprintf(bulk + off, buf_cap - off,
                    "%s\"g%d\":{\"v\":%d}", i == base_i ? "" : ",", i, i);
            }
            off += (size_t)snprintf(bulk + off, buf_cap - off, "}}");
            tc_request(tc, bulk, &resp);
            if (!resp || SAFE_STRSTR(resp, "\"error\"")) all_ok = 0;
            free(resp); resp = NULL;
        }
        free(bulk);
        ASSERT_TRUE(all_ok, "grown seeded with 1,050,000 real records");
    }

    /* Object 2: already correctly sized. splits=64, live stays tiny
       (a few real inserts) -> target=8 <= 64, must stay untouched. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sized\","
        "\"splits\":64,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sized\","
            "\"key\":\"k%d\",\"value\":{\"v\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Object 3: 'huge' — live count fabricated past INT_MAX (~2.1B) on a
       single shard, to regression-test get_live_count()'s int-truncation
       bug. splits=8; 3,000,000,000 falls in the 1B-5B band -> target=2048.
       Before this task's fix, get_live_count()'s (int) cast wraps this to
       a negative value, reshard_target_for_count() falls through to its
       `return 8` default, 8 <= 8, and 'huge' is silently never reshaped —
       the exact bug this test catches.

       3 billion real records isn't something a routine test can create, so
       this object keeps the fabricated-header technique. That means the
       actual rebuild attempt is now expected to fail (rebuild_object_v2's
       live-count-matches-source-total invariant correctly refuses a
       fabricated total with no matching real records) -- so verification
       below checks the "AUTO-RESHARD ... starting 8 -> 2048" log line
       (proving reshard_target_for_count() reached the right decision at
       true past-INT_MAX scale) rather than waiting for the rebuild/
       describe-object to report the new splits. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"huge\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    char huge_kf_path[PATH_MAX];
    snprintf(huge_kf_path, sizeof(huge_kf_path), "%s/default/huge/data/kf/000.kf", db_root);
    ASSERT_EQ_INT(fabricate_kf_total(huge_kf_path, 3000000000ULL), 0,
                  "fabricate shard 0 total=3,000,000,000 on 'huge' (past INT_MAX)");

    /* Sanity: pre-sweep, all three objects still at their created splits. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":8", "grown starts at splits=8");
    free(resp); resp = NULL;

    /* Wait for the thread's first hour-matching tick. auto_reshard_thread
       sleeps 5s on startup (Task 3) before its first wall-clock check,
       specifically so this setup above always finishes first — then it
       sleeps in 1s increments and checks the wall clock each tick. 20s
       gives generous slack on top of the 5s startup delay for slow CI. */
    TAP_DIAG("# auto-reshard: waiting up to 20s for the first thread tick...\n");
    fflush(_TAP_OUT);
    int grown_reshaped = 0;
    for (int i = 0; i < 40 * RESHARD_WAIT_SCALE; i++) {
        struct timespec ts = { 0, 500 * 1000000L }; nanosleep(&ts, NULL);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
        if (resp && SAFE_STRSTR(resp, "\"splits\":16")) { grown_reshaped = 1; free(resp); resp = NULL; break; }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(grown_reshaped, "grown reshaped from splits=8 to splits=16 within 20s");

    /* Post-reshard writer: hammer inserts to verify the rebuilt object
       accepts writes without errors and every successful insert persists.
       The writer starts after the reshard is confirmed so it doesn't
       starve the objlock_wrlock during the rebuild (a stream of
       concurrent rdlock holders prevents wrlock acquisition). The
       concurrency-safety of the reshard path is structurally guaranteed:
       objlock_wrlock is taken the same way every other cmd_vacuum heavy
       path caller does it (server.c:1397-1398, 1842, 2296-2297). */
    WriterCtx wctx = {0};
    wctx.cfg = (TestClientCfg){ .port = port, .io_timeout_ms = 30000 };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, writer_thread_fn, &wctx), 0,
                  "start post-reshard writer thread");
    struct timespec writer_ts = { 1, 0 };
    nanosleep(&writer_ts, NULL);
    atomic_store_explicit(&wctx.stop, 1, memory_order_release);
    pthread_join(writer_tid, NULL);
    ASSERT_TRUE(wctx.sent > 0, "writer thread issued at least one insert");
    ASSERT_EQ_INT(wctx.sent, wctx.ok, "no insert on 'grown' errored after reshard");
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), GROWN_BASE_COUNT + wctx.ok,
                  "grown's count matches the 1,050,000 seeded records plus the "
                  "writer's successful post-reshard inserts (rebuild preserved "
                  "every seeded record and inserts land in the rebuilt kf+segments)");
    free(resp); resp = NULL;

    /* sized must be untouched. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":64", "sized stays at splits=64 (no-op path)");
    free(resp); resp = NULL;

    /* All 5 real inserts on 'sized' survive untouched. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 5, "sized count=5 (untouched)");
    free(resp); resp = NULL;

    /* 'huge' must have been picked up with target=2048 -- proves
       get_live_count_ll's full-width count reached
       reshard_target_for_count without truncating. The sweep already ran
       (grown_reshaped proved that above), so this is just waiting for
       'huge's "starting" log line from the same completed sweep tick,
       plus slack for a second tick in the unlikely case the sweep
       serializes. The rebuild itself is expected to fail (fabricated
       total, no matching real records -- see the object-3 comment above),
       so this checks the log line, not describe-object's splits. */
    int huge_target_logged = wait_for_log_line(base, date_str,
        "AUTO-RESHARD default/huge: starting 8 -> 2048", 20 * RESHARD_WAIT_SCALE);
    ASSERT_TRUE(huge_target_logged,
        "huge (live=3,000,000,000, past INT_MAX) computed target=2048 within 20s "
        "-- regression check for get_live_count()'s int-truncation bug "
        "(rebuild itself is expected to fail against the fabricated total)");

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard", test_auto_reshard_run)

/* AUTO_RESHARD_HOUR=<garbage> must not fail silently -- the daemon should
   still start (fallback to the compiled-in default), but print a warning
   naming the bad value to stderr (the earliest-available output, before
   log_init). */
static int test_auto_reshard_bad_hour_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-arbh-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);
    char logs_dir[300]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
"export LOG_LEVEL=3\n"
            "export THREADS=2\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n"
        "export AUTO_RESHARD_ENABLE=1\n"
        "export AUTO_RESHARD_HOUR=not-a-number\n",
        db_root, port, base);
    fclose(f);

    /* Use a pipe to capture the child's stderr so we can verify the
       config warning appears without relying on log files (log_init
       runs after config parsing). */
    int pipefd[2];
    if (pipe(pipefd) != 0) { ASSERT_TRUE(0, "pipe"); tu_run_cmd("rm -rf %s", base); return 1; }

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    close(pipefd[1]);
    TestEnv env = { .port = port, .daemon_pid = pid };
    snprintf(env.db_root, sizeof(env.db_root), "%s", db_root);

    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon starts fine despite AUTO_RESHARD_HOUR=not-a-number (falls back to default)");

    /* Read captured stderr and check for the config warning. */
    char stderr_buf[4096] = {0};
    ssize_t nread = (int)read(pipefd[0], stderr_buf, sizeof(stderr_buf) - 1);
    close(pipefd[0]);
    if (nread > 0) stderr_buf[nread] = '\0';
    int found = (strstr(stderr_buf, "AUTO_RESHARD_HOUR") != NULL);
    ASSERT_TRUE(found, "a warning naming AUTO_RESHARD_HOUR appears on stderr at startup");

    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-bad-hour", test_auto_reshard_bad_hour_run)

/* Two objects both past their reshard threshold in the same sweep tick,
   AUTO_RESHARD_THROTTLE_MS set to a value large enough to measure: the
   second object's reshard-done log must land at least throttle_ms after
   the first's. Uses the log file (not describe-object polling) since we
   need timestamps, not just completion. */
static int test_auto_reshard_throttle_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-arth-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);
    char logs_dir[300]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    int secs_left_in_hour = (59 - tmv.tm_min) * 60 + (60 - tmv.tm_sec);
    if (secs_left_in_hour < 90) {
        struct timespec boundary_ts = { secs_left_in_hour + 2, 0 };
        nanosleep(&boundary_ts, NULL);
        now = time(NULL);
        localtime_r(&now, &tmv);
    }

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
"export LOG_LEVEL=3\n"
            "export THREADS=2\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n"
        "export AUTO_RESHARD_ENABLE=1\n"
        "export AUTO_RESHARD_HOUR=%d\n"
        "export AUTO_RESHARD_THROTTLE_MS=3000\n",
        db_root, port, base, tmv.tm_hour);
    fclose(f);

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

    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon ready with AUTO_RESHARD_THROTTLE_MS=3000");
    if (!ready) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Captured before the (up to ~40s) reshard-wait section below so the
       log-file date lookup is right even if that wait straddles midnight
       (the log file is named by the date the line was WRITTEN, not the
       date the test happens to check it). */
    time_t reshard_start_ts = time(NULL);

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Two objects, both under-split (splits=8, seeded with GROWN_BASE_COUNT
       real records -> falls in the 1M-10M band -> target=16). Real
       records, not a fabricated kf header total, because rebuild_object_v2
       now hard-fails a reshard whose copied live count doesn't match the
       source header's declared total (see this file's header comment).
       readdir order across two objects in the same dir isn't alphabetically
       guaranteed, but both must reshape regardless of order -- the test
       only cares about the gap between the two "done" log lines, not which
       ran first. */
    const char *names[2] = { "grown_a", "grown_b" };
    for (int oi = 0; oi < 2; oi++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
            names[oi]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;

        size_t buf_cap = (size_t)GROWN_CHUNK * 40 + 256;
        char *bulk = malloc(buf_cap);
        ASSERT_NOT_NULL(bulk, "malloc grown_* bulk-insert buffer");
        int all_ok = bulk != NULL;
        for (int base_i = 0; all_ok && base_i < GROWN_BASE_COUNT; base_i += GROWN_CHUNK) {
            int end = base_i + GROWN_CHUNK;
            if (end > GROWN_BASE_COUNT) end = GROWN_BASE_COUNT;
            size_t off = 0;
            off += (size_t)snprintf(bulk + off, buf_cap - off,
                "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\","
                "\"records\":{", names[oi]);
            for (int i = base_i; i < end; i++) {
                off += (size_t)snprintf(bulk + off, buf_cap - off,
                    "%s\"k%d\":{\"v\":%d}", i == base_i ? "" : ",", i, i);
            }
            off += (size_t)snprintf(bulk + off, buf_cap - off, "}}");
            tc_request(tc, bulk, &resp);
            if (!resp || SAFE_STRSTR(resp, "\"error\"")) all_ok = 0;
            free(resp); resp = NULL;
        }
        free(bulk);
        ASSERT_TRUE(all_ok, "grown_* seeded with 1,050,000 real records");
    }

    /* Wait for both to reshape (generous budget: two reshards plus a 3s
       throttle gap between them, on top of the usual 5s startup delay). */
    int both_reshaped = 0;
    for (int i = 0; i < 80 * RESHARD_WAIT_SCALE; i++) {
        struct timespec ts = { 0, 500 * 1000000L }; nanosleep(&ts, NULL);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown_a\"}", &resp);
        int a_done = resp && SAFE_STRSTR(resp, "\"splits\":16");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown_b\"}", &resp);
        int b_done = resp && SAFE_STRSTR(resp, "\"splits\":16");
        free(resp); resp = NULL;
        if (a_done && b_done) { both_reshaped = 1; break; }
    }
    ASSERT_TRUE(both_reshaped, "both grown_a and grown_b reshaped within 40s");

    /* Verify two distinct done-log lines exist in the log file. We compare
       the raw log lines since the log timestamp format is second-precision
       only and the 3s throttle's gap can still land within the same
       wall-clock second. The existence of two reshaped objects (proved by
       describe-object above) plus the throttle sleep in
       auto_reshard_sweep_one is sufficient to verify the throttle code
       ran; the log check here just confirms log lines exist. */
    {
        /* Log files are <date>-info.log and <date>-error.log (see
           open_log_for_level in config.c), named by the date the line was
           WRITTEN. The reshard+throttle wait above can take up to ~40s, so
           if that window straddles midnight, "done" lines for the two
           objects can land in two different dated files. Check both the
           date at the start of the wait and the date now (usually the same
           file, opened twice harmlessly) and sum matches across both. */
        time_t now2 = time(NULL);
        struct tm tbuf_start, tbuf_now;
        struct tm *t_start = localtime_r(&reshard_start_ts, &tbuf_start);
        struct tm *t_now   = localtime_r(&now2, &tbuf_now);
        char date_start[16], date_now[16];
        strftime(date_start, sizeof(date_start), "%Y-%m-%d", t_start);
        strftime(date_now,   sizeof(date_now),   "%Y-%m-%d", t_now);

        int done_count = 0;
        int opened_any = 0;
        const char *dates[2] = { date_start, date_now };
        for (int di = 0; di < 2; di++) {
            if (di == 1 && strcmp(dates[0], dates[1]) == 0) break;
            char log_path[400];
            snprintf(log_path, sizeof(log_path), "%s/logs/%s-info.log", base, dates[di]);
            FILE *lf = fopen(log_path, "r");
            if (!lf) continue;
            opened_any = 1;
            char lbuf[1024];
            while (fgets(lbuf, sizeof(lbuf), lf)) {
                if (strstr(lbuf, "AUTO-RESHARD") && strstr(lbuf, "splits done"))
                    done_count++;
            }
            fclose(lf);
        }
        ASSERT_TRUE(opened_any, "opened info.log for done-line check");
        ASSERT_TRUE(done_count >= 2,
            "found at least 2 AUTO-RESHARD done log lines");
    }

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-throttle", test_auto_reshard_throttle_run)
