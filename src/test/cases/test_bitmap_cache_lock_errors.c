/* Bitmap-cache returned-lock-error regression cases.
 *
 * Five independent cases:
 *   test-bitmap-cache-selfhold         — 16 writer-held entries + a 17th cold
 *                                        writer open must fail with EDEADLK
 *                                        instead of retrying forever.
 *   test-bitmap-cache-lock-errors      — per-scenario fork-isolated matrix:
 *                                        hit/fill/drop acquisition faults, one-
 *                                        shot consumed at the intended site, no
 *                                        handle without a lock, no unlock of an
 *                                        unacquired lock, cache-owned installs,
 *                                        checked-invalidation failure, and
 *                                        publication nowait policy preserved.
 *   test-bitmap-cache-contention       — ordinary blocking contention must
 *                                        still wait and complete (real
 *                                        other-thread holders, observed via the
 *                                        TEST_BUILD acquisition-entry seam).
 *   test-bitmap-cache-selfhold-vacuum  — daemon integration: seed with the
 *                                        ordinary cache (40 enum(0,1,2,3)
 *                                        bitmap fields), restart the test
 *                                        daemon with BM_TEST_CACHE_CAP=16,
 *                                        light vacuum must refuse with
 *                                        {"error":"keyfile compaction failed"}
 *                                        + KF_BMFIX_PREPARE_FAILED at the
 *                                        drop-wait EDEADLK trigger, data and
 *                                        bitmap bytes unchanged, and full
 *                                        recovery after an ordinary-capacity
 *                                        restart. Runs under a 150 s work
 *                                        budget (600 s in sanitizer builds)
 *                                        + 30 s cleanup reserve with the
 *                                        fixtures' opt-in startup deadline.
 *   test-bitmap-cache-fixture-watchdog — deterministic proof of the opt-in
 *                                        readiness watchdog: a parking stub is
 *                                        bounded by an absolute deadline, an
 *                                        expired deadline never launches a
 *                                        probe, and a shortened budget drives a
 *                                        parked request to full cleanup with
 *                                        the child, daemon and stub reaped.
 *
 * Every potentially-blocking child runs under a monotonic watchdog with
 * exact-PID kill/reap and progress stages; no broad pkill anywhere. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "test_client.h"
#include "types.h"
#include "bitmap.h"

#include <dirent.h>
#include <errno.h>
#include <ftw.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

/* CodeQL remediation (2026-11 PR #385): the fixture-cleanup and evidence
   paths used to shell out through system(); every command argument derived
   (via tmpdir templates) from the environment, which CodeQL flags as
   command injection. Everything below is plain C: recursive remove via
   nftw, log dumps via readdir+fread, marker scans via opendir. */

static int bcl_rm_entry(const char *p, const struct stat *st, int type, struct FTW *f) {
    (void)st; (void)type; (void)f; return remove(p);
}
static int bcl_rmrf(const char *path) {
    if (!path || !path[0]) return -1;
    errno = 0;
    int rc = nftw(path, bcl_rm_entry, 32, FTW_DEPTH | FTW_PHYS);
    if (rc != 0) {
        /* rm -rf semantics: a tree that is already gone (test_env_stop also
           removes parts of it) is not an error. */
        struct stat st;
        if (lstat(path, &st) != 0 && errno == ENOENT) rc = 0;
    }
    return rc;
}
/* Failure-evidence dump: names <base>/logs and prints the first 20 lines of
   the captured stderr file (the old `ls -la ... ; cat ... | head -20`). */
static void bcl_dump_failure_evidence(const char *base, const char *cap_path) {
    TAP_DIAG("# logs dump:");
    char sub[PATH_MAX];
    if (base && snprintf(sub, sizeof(sub), "%s/logs", base) > 0) {
        DIR *d = opendir(sub);
        if (d) {
            struct dirent *e;
            while ((e = readdir(d)))
                TAP_DIAG("#   %s/%s", sub, e->d_name);
            closedir(d);
        } else {
            TAP_DIAG("#   (no logs dir at %s)", sub);
        }
    }
    if (cap_path && cap_path[0]) {
        FILE *f = fopen(cap_path, "r");
        if (!f) { TAP_DIAG("# == captured stderr: (unreadable: %s)", cap_path); return; }
        TAP_DIAG("# == captured stderr:");
        char line[512];
        int n = 0;
        while (n < 20 && fgets(line, sizeof(line), f)) { TAP_DIAG("#   %s", line); n++; }
        fclose(f);
    }
}
/* 1 = no .bmfix-* marker or *.bm.remap temporary under <obj>/indexes (the
   old `test -z "$(find ... -name '.bmfix-*' -o -name '*.bm.remap')"`. */
static int bcl_indexes_clean_of_txn_artifacts(const char *obj_dir) {
    char idx[PATH_MAX];
    if (snprintf(idx, sizeof(idx), "%s/indexes", obj_dir) >= (int)sizeof(idx))
        return -1;
    DIR *d = opendir(idx);
    if (!d) return -1;
    struct dirent *e;
    int clean = 1;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char sub[PATH_MAX];
        if (snprintf(sub, sizeof(sub), "%s/%s", idx, e->d_name) >= (int)sizeof(sub))
            continue;
        struct stat st;
        if (stat(sub, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        DIR *sd = opendir(sub);
        if (!sd) continue;
        struct dirent *se;
        while ((se = readdir(sd))) {
            if (strncmp(se->d_name, ".bmfix-", 7) == 0)
                clean = 0;
            size_t len = strlen(se->d_name);
            if (len > 6 && strcmp(se->d_name + len - 6, ".remap") == 0)
                clean = 0;
        }
        closedir(sd);
    }
    closedir(d);
    return clean ? 0 : 1;
}
#include <time.h>
#include <unistd.h>

/* ───────────────────────── child runner + stages ───────────────────────── */

/* Sanitizer builds widen the time limits (as test_runner.c widens its
 * per-case watchdog to 1200 s): under TSan the post-vacuum CRUD child alone
 * runs past 30 s and a light vacuum past 3 s, on CPU, with no lock wait. */
#if defined(BUILD_TSAN) || defined(BUILD_ASAN)
#define BCL_SAN_WIDE 1
#else
#define BCL_SAN_WIDE 0
#endif
#define BCL_UNIT_CHILD_TIMEOUT_MS (BCL_SAN_WIDE ? 120000 : 30000)
#define BCL_POLL_SLICE_MS         50

/* Integration budget: 150 s of work (600 s in sanitizer builds), then up to
 * 30 s reserved for cleanup; both fit the runner's per-case watchdog
 * (180 s, or 1200 s in sanitizer builds). */
#define BCL_WORK_BUDGET_MS     (BCL_SAN_WIDE ? 600000 : 150000)
#define BCL_CLEANUP_RESERVE_MS 30000

static __thread int bcl_stage_fd = -1;

static void bcl_stage(const char *msg) {
    if (bcl_stage_fd >= 0) dprintf(bcl_stage_fd, "STAGE %s\n", msg);
}

/* g_db is thread-local: threads created inside a forked child start with
 * g_db == NULL and must adopt the child's db before touching cache globals
 * (same pattern as test_durability_sync_cache_paths.c's writer threads). */
static ShardDb *bcl_child_db;

/* Child bodies are int fns: 0 = pass, nonzero = failure code. */
typedef int (*BclChildFn)(void);

typedef struct {
    int  ran;         /* 1 = child status collected */
    int  exit_code;   /* child exit status (or -1 on signal) */
    int  timed_out;   /* 1 = watchdog fired */
    int  termsig;     /* signal that killed the child, 0 if none */
    char stages[4096];
} BclChildResult;

static int64_t bcl_monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Integration work-budget anchor (0 = inactive; scoped to the integration
 * case only and cleared on every return). */
static int64_t bcl_budget_anchor;
/* Budget LENGTH, fixed when a scope is entered (the integration case uses
 * the full 150 s; the shortened self-check uses 4 s) and never reset
 * mid-scope. */
static int64_t bcl_budget_total_ms = BCL_WORK_BUDGET_MS;

static int64_t bcl_work_remaining_ms(void) {
    if (bcl_budget_anchor == 0) return BCL_UNIT_CHILD_TIMEOUT_MS;
    int64_t left = bcl_budget_total_ms - (bcl_monotonic_ms() - bcl_budget_anchor);
    return left > 0 ? left : 0;
}

static int bcl_budget_expired(void) {
    return bcl_budget_anchor != 0 && bcl_work_remaining_ms() == 0;
}

/* Unit child bound: min(30 s, remaining work budget). */
static int bcl_child_bound_ms(void) {
    int64_t left = bcl_work_remaining_ms();
    if (left > BCL_UNIT_CHILD_TIMEOUT_MS) return BCL_UNIT_CHILD_TIMEOUT_MS;
    return (int)left;
}

/* Nested drivers stay in their outer driver's private process group. */
static pid_t bcl_child_group;

static int bcl_run_child_bounded(const char *label, BclChildFn fn,
                                 BclChildResult *out, int timeout_ms) {
    memset(out, 0, sizeof(*out));
    int pfd[2];
    if (pipe(pfd) != 0) return -1;
    if (fcntl(pfd[0], F_SETFL, O_NONBLOCK) != 0) {
        close(pfd[0]); close(pfd[1]); return -1;
    }
    int owns_group = bcl_child_group == 0;
    int life[2] = {-1, -1};
    if (owns_group && pipe(life) != 0) { close(pfd[0]); close(pfd[1]); return -1; }
    pid_t pid = fork();
    if (pid < 0) {
        close(pfd[0]); close(pfd[1]);
        if (owns_group) { close(life[0]); close(life[1]); }
        return -1;
    }
    if (pid == 0) {
        close(pfd[0]);
        if (owns_group) {
            close(life[1]);
            if (setpgid(0, 0) != 0) _exit(125);
            bcl_child_group = getpid();
            pid_t guard = fork();
            if (guard < 0) _exit(125);
            if (guard == 0) {
                close(pfd[1]);
                char byte;
                ssize_t n;
                do { n = read(life[0], &byte, 1); } while (n < 0 && errno == EINTR);
                /* Only the monitor owns a writer. Its exit, including a
                   suite-wide group kill, closes this lifeline. Membership
                   keeps the private PGID pinned while we terminate it. */
                kill(-getpgrp(), SIGKILL);
                _exit(125);
            }
            close(life[0]);
        }
        bcl_stage_fd = pfd[1];
        bcl_child_db = g_db;
        int rc = fn();
        if (rc != 0) bcl_stage("CHILD-FAILED");
        close(bcl_stage_fd);
        _exit(rc);
    }
    close(pfd[1]);
    if (owns_group) close(life[0]);
    int monitor_error = 0;
    /* Both sides establish the group before any driver work, closing the
       timeout-before-child-scheduled race. Never target our caller's group. */
    if (owns_group && setpgid(pid, pid) != 0 && getpgid(pid) != pid)
        monitor_error = 1;
    int64_t deadline = bcl_monotonic_ms() + timeout_ms;
    char buf[sizeof(out->stages)];
    size_t got = 0;
    int status = 0, done = 0;
    while (!monitor_error) {
        int64_t left = deadline - bcl_monotonic_ms();
        if (left <= 0) break;
        int wait_ms = left < BCL_POLL_SLICE_MS ? (int)left : BCL_POLL_SLICE_MS;
        struct pollfd p = { .fd = pfd[0], .events = POLLIN };
        if (poll(&p, 1, wait_ms) < 0 && errno != EINTR) {
            monitor_error = 1; break;
        }
        while (got < sizeof(buf) - 1) {
            ssize_t n = read(pfd[0], buf + got, sizeof(buf) - 1 - got);
            if (n > 0) { got += (size_t)n; continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                monitor_error = 1;
            break;
        }
        siginfo_t si;
        memset(&si, 0, sizeof(si));
        /* Keep the leader unreaped until group cleanup: its zombie pins the
           PID/PGID, so a recycled identifier can never select another group. */
        if (waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0) {
            if (si.si_pid == pid) { done = 1; break; }
        } else if (errno != EINTR) { monitor_error = 1; break; }
    }
    if (!done && !monitor_error) out->timed_out = 1;
    if (owns_group) {
        /* Includes daemon/stub/probe descendants of a wedged owner. Nested
           request watchdogs kill only their request; this outer group owns
           final containment, even after a successful driver exit. */
        if (kill(-pid, SIGKILL) != 0 && errno != ESRCH) monitor_error = 1;
        kill(pid, SIGKILL);
    } else if (!done) {
        kill(pid, SIGKILL);
    }
    if (owns_group) close(life[1]);
    pid_t reaped;
    do { reaped = waitpid(pid, &status, 0); } while (reaped < 0 && errno == EINTR);
    if (reaped != pid) monitor_error = 1;
    /* All writers are terminated or exiting. Read only currently available
       bytes: descendants may still own a pipe endpoint until exit. */
    while (got < sizeof(buf) - 1) {
        ssize_t n = read(pfd[0], buf + got, sizeof(buf) - 1 - got);
        if (n > 0) { got += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
    close(pfd[0]);
    buf[got] = '\0';
    snprintf(out->stages, sizeof(out->stages), "%s", buf);
    if (monitor_error) {
        TAP_DIAG("# %s: child monitor failed\n", label);
        return -1;
    }
    out->ran = 1;
    out->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    out->termsig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
    if (out->timed_out || out->exit_code != 0 || out->termsig)
        TAP_DIAG("# %s: timed_out=%d exit=%d termsig=%d stages:\n# %s\n",
                 label, out->timed_out, out->exit_code, out->termsig, out->stages);
    return 1;
}

/* Standard child run: bounded by min(30 s, remaining work budget). */
static int bcl_run_child(const char *label, BclChildFn fn, BclChildResult *out) {
    return bcl_run_child_bounded(label, fn, out, bcl_child_bound_ms());
}

/* Child-side single-line check: stage the reason and fail the child. */
#define BCL_CHECK(cond, code, msg) \
    do { if (!(cond)) { bcl_stage("FAIL " msg); return (code); } } while (0)

/* Bounded wall-clock sleep for coordinating threads (never a sole ordering
 * assertion — always paired with a seam observation). */
static void bcl_sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* ───────────────────────── in-child cache fixtures ───────────────────────── */

/* Fresh 16-entry cache (32 hash slots, eviction at 16 retained entries). */
static int bcl_child_cache16(void) {
    bm_cache_shutdown();
    if (bm_test_set_cache_cap(16) != 0) return 90;
    bm_cache_init(4096);              /* override forces the cap-16 geometry */
    if (g_bm_cache_slots != 32) return 91;
    bm_test_reset_lock_failure();
    return 0;
}

static int bcl_child_cache_default(void) {
    bm_cache_shutdown();
    bm_test_set_cache_cap(0);   /* never inherit a scoped cap16 override */
    bm_test_reset_lock_failure();
    bm_cache_init(4096);
    return 0;
}

/* Child-scope tail: drop the cache and clear the process-global cap override
 * so nothing leaks into a later fixture in the same child. */
static void bcl_child_cache_teardown(void) {
    bm_cache_shutdown();
    bm_test_set_cache_cap(0);
    bm_test_reset_lock_failure();
}

static int bcl_count_fds(void) {
    DIR *d = opendir("/proc/self/fd");
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) n++;
    closedir(d);
    return n - 1;   /* exclude the directory handle itself */
}

static int bcl_find_cache_slot(const char *path) {
    for (int i = 0; i < g_bm_cache_slots; i++) {
        if (g_bm_cache[i].fd >= 0 && strcmp(g_bm_cache[i].path, path) == 0)
            return i;
    }
    return -1;
}

/* ───────────────────── case 1: writer self-hold exhaustion ───────────────────── */

#define BCL_SELFHOLD_FILES 17
#define BCL_SELFHOLD_SLOTS 64

typedef struct {
    char dir[128];
    char paths[BCL_SELFHOLD_FILES][256];
} BclSelfhold;

static BclSelfhold bcl_sh;   /* written before fork, read-only in the child */

static int bcl_selfhold_prepare(void) {
    snprintf(bcl_sh.dir, sizeof(bcl_sh.dir), "/tmp/shard-db-bcl-selfhold-XXXXXX");
    if (!mkdtemp(bcl_sh.dir)) return -1;
    for (int i = 0; i < BCL_SELFHOLD_FILES; i++)
        snprintf(bcl_sh.paths[i], sizeof(bcl_sh.paths[i]), "%s/f%02d.bm", bcl_sh.dir, i);
    return 0;
}

static int bcl_selfhold_child(void) {
    int rc = bcl_child_cache16();
    BCL_CHECK(rc == 0, 100 + rc, "cap16 cache fixture failed");
    bcl_stage("cache cap16 slots32 threshold16");

    BitmapShard *held[BCL_SELFHOLD_FILES - 1];
    for (int i = 0; i < BCL_SELFHOLD_FILES - 1; i++) {
        held[i] = bm_open(bcl_sh.paths[i], BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
        BCL_CHECK(held[i] != NULL, 102, "retained writer open failed");
        BCL_CHECK(bm_set(held[i], (const uint8_t *)"v", 1, (uint32_t)i) == 0,
                  103, "bm_set on retained handle failed");
        BCL_CHECK(bm_test(held[i], (const uint8_t *)"v", 1, (uint32_t)i) == 1,
                  104, "saved bit does not read back");
    }
    BCL_CHECK(g_bm_cache_count == BCL_SELFHOLD_FILES - 1, 105,
              "unexpected cache count before the 17th open");
    bcl_stage("seeded16; attempting17");

    int unlocks_before = bm_test_entry_unlock_count();
    errno = 0;
    BitmapShard *bm17 = bm_open(bcl_sh.paths[BCL_SELFHOLD_FILES - 1],
                                BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    int err = errno;
    int unlocks_after = bm_test_entry_unlock_count();
    BCL_CHECK(bm17 == NULL, 106, "17th writer open returned a handle");
    BCL_CHECK(err == EDEADLK, 107, "17th open errno != EDEADLK");
    BCL_CHECK(unlocks_after == unlocks_before, 108,
              "failed open performed entry unlocks");
    bcl_stage("open17 refused EDEADLK; verifying retained handles");

    for (int i = 0; i < BCL_SELFHOLD_FILES - 1; i++) {
        BCL_CHECK(bm_test(held[i], (const uint8_t *)"v", 1, (uint32_t)i) == 1,
                  109, "retained handle lost its bit");
        BCL_CHECK(bm_close_checked(held[i]) == 0, 110, "retained close failed");
    }
    bcl_stage("retained16 closed; reopening all files");

    for (int i = 0; i < BCL_SELFHOLD_FILES; i++) {
        BitmapShard *bm = bm_open(bcl_sh.paths[i], 0, 0, 0, 0, 0);
        BCL_CHECK(bm != NULL, 111, "reopen after release failed");
        if (i < BCL_SELFHOLD_FILES - 1)
            BCL_CHECK(bm_test(bm, (const uint8_t *)"v", 1, (uint32_t)i) == 1,
                      112, "reopened file lost its bit");
        else
            BCL_CHECK(bm_test(bm, (const uint8_t *)"v", 1, 0) == 0,
                      113, "17th file unexpectedly carries a bit");
        bm_close(bm);
    }
    bcl_stage("reopens verified");
    bcl_child_cache_teardown();
    return 0;
}

static int test_bitmap_cache_selfhold_run(void) {
    ASSERT_EQ_INT(bcl_selfhold_prepare(), 0, "selfhold fixture dir");
    BclChildResult r;
    int ran = bcl_run_child("selfhold", bcl_selfhold_child, &r);
    ASSERT_EQ_INT(ran, 1, "selfhold child ran (watchdog infrastructure)");
    ASSERT_EQ_INT(r.timed_out, 0,
                  "17th writer open fails promptly (no endless eviction retry)");
    ASSERT_EQ_INT(r.exit_code, 0, "selfhold child passed all checks");
    ASSERT_EQ_INT(bcl_rmrf(bcl_sh.dir), 0, "selfhold fixture cleanup");
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ───────────────────────── case 2: lock-error matrix ───────────────────────── */

static char bcl_mx_dir[128];
static char bcl_mx_path_a[256];   /* warm file (created + cached) */
static char bcl_mx_path_b[256];   /* cold file for fill/eviction scenarios */
static char bcl_mx_paths16[16][256];

static int bcl_mx_prepare(void) {
    snprintf(bcl_mx_dir, sizeof(bcl_mx_dir), "/tmp/shard-db-bcl-mx-XXXXXX");
    if (!mkdtemp(bcl_mx_dir)) return -1;
    snprintf(bcl_mx_path_a, sizeof(bcl_mx_path_a), "%s/a.bm", bcl_mx_dir);
    snprintf(bcl_mx_path_b, sizeof(bcl_mx_path_b), "%s/b.bm", bcl_mx_dir);
    for (int i = 0; i < 16; i++)
        snprintf(bcl_mx_paths16[i], sizeof(bcl_mx_paths16[i]), "%s/s%02d.bm", bcl_mx_dir, i);
    return 0;
}

/* Seed path A with two set bits; optionally drop the cache afterwards so the
 * next open of A is a cold fill. */
static int bcl_mx_seed_a(int drop_cache_after) {
    bm_cache_shutdown();
    bm_test_reset_lock_failure();
    bm_cache_init(4096);
    BitmapShard *bm = bm_open(bcl_mx_path_a, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    if (!bm) { bcl_stage("seed: open failed"); return 1; }
    if (bm_set(bm, (const uint8_t *)"a", 1, 7) != 0) { bm_close(bm); bcl_stage("seed: set failed"); return 2; }
    if (bm_test(bm, (const uint8_t *)"a", 1, 7) != 1) { bm_close(bm); bcl_stage("seed: test failed"); return 3; }
    if (bm_close_checked(bm) != 0) { bcl_stage("seed: close failed"); return 4; }
    if (drop_cache_after) {
        bm_cache_shutdown();
        bm_cache_init(4096);
    }
    return 0;
}

/* S1: hit RD fault. Failed open returns no handle, consumes at HIT_RD,
 * performs no unlock, and a real holder stays excluded. */
static int bcl_mx_hit_rd(void) {
    BCL_CHECK(bcl_mx_seed_a(0) == 0, 10, "seed A failed");
    bm_test_reset_lock_failure();

    /* S1a: no holder. */
    int u0 = bm_test_entry_unlock_count();
    bm_test_fail_lock_next(BM_TEST_LOCK_HIT_RD, EAGAIN);
    errno = 0;
    BitmapShard *bm = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(bm == NULL && errno == EAGAIN, 11, "hit-rd open did not fail with EAGAIN");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_HIT_RD) == 1, 12,
              "fault not consumed at hit-rd");
    BCL_CHECK(bm_test_entry_unlock_count() == u0, 13, "failed hit open unlocked");

    BitmapShard *held = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(held != NULL, 14, "later ordinary hit open failed");
    BCL_CHECK(bm_test(held, (const uint8_t *)"a", 1, 7) == 1, 15, "data lost");

    /* S1b: a real holder must stay excluded after the failed open. */
    bm_test_fail_lock_next(BM_TEST_LOCK_HIT_RD, EAGAIN);
    u0 = bm_test_entry_unlock_count();
    errno = 0;
    BitmapShard *bm2 = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(bm2 == NULL && errno == EAGAIN, 16, "second hit-rd open did not fail");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_HIT_RD) == 2, 17,
              "second fault not consumed");
    BCL_CHECK(bm_test_entry_unlock_count() == u0, 18, "failed open unlocked the holder's lock");
    int slot = bcl_find_cache_slot(bcl_mx_path_a);
    BCL_CHECK(slot >= 0, 19, "cached entry vanished");
    BCL_CHECK(pthread_rwlock_trywrlock(&g_bm_cache[slot].rwlock) == EBUSY,
              20, "original holder no longer excluded after failed open");
    BCL_CHECK(bm_close_checked(held) == 0, 21, "holder close failed");
    BCL_CHECK(pthread_rwlock_trywrlock(&g_bm_cache[slot].rwlock) == 0,
              22, "entry not free after holder close");
    BCL_CHECK(pthread_rwlock_unlock(&g_bm_cache[slot].rwlock) == 0, 23, "probe unlock failed");
    return 0;
}

/* S2: hit WR fault, same exclusions; later writer open succeeds and writes. */
static int bcl_mx_hit_wr(void) {
    BCL_CHECK(bcl_mx_seed_a(0) == 0, 10, "seed A failed");
    bm_test_reset_lock_failure();

    BitmapShard *holder = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 1);   /* writer hold */
    BCL_CHECK(holder != NULL, 11, "holder writer open failed");

    int u0 = bm_test_entry_unlock_count();
    bm_test_fail_lock_next(BM_TEST_LOCK_HIT_WR, EDEADLK);
    errno = 0;
    BitmapShard *bm = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 1);
    BCL_CHECK(bm == NULL && errno == EDEADLK, 12, "hit-wr open did not fail with EDEADLK");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_HIT_WR) == 1, 13,
              "fault not consumed at hit-wr");
    BCL_CHECK(bm_test_entry_unlock_count() == u0, 14, "failed hit-wr open unlocked");
    int slot = bcl_find_cache_slot(bcl_mx_path_a);
    BCL_CHECK(slot >= 0, 15, "cached entry vanished");
    BCL_CHECK(pthread_rwlock_trywrlock(&g_bm_cache[slot].rwlock) == EBUSY,
              16, "writer holder no longer excluded after failed open");
    BCL_CHECK(bm_set(holder, (const uint8_t *)"w", 1, 9) == 0, 17, "holder cannot write");
    BCL_CHECK(bm_close_checked(holder) == 0, 18, "holder close failed");

    BitmapShard *after = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 1);
    BCL_CHECK(after != NULL, 19, "later writer open failed");
    BCL_CHECK(bm_test(after, (const uint8_t *)"w", 1, 9) == 1, 20, "written bit lost");
    bm_close(after);
    return 0;
}

/* S3/S4: fill faults on a cold path. The fd/map installed before the failed
 * acquisition remain cache-owned; a later ordinary open succeeds. */
static int bcl_mx_fill(int writer, int site, int errcode) {
    BCL_CHECK(bcl_mx_seed_a(1) == 0, 10, "seed A failed");
    BCL_CHECK(g_bm_cache_count == 0, 11, "cache not empty after reset");
    int fds0 = bcl_count_fds();
    BCL_CHECK(fds0 >= 0, 12, "fd scan failed");
    bm_test_reset_lock_failure();

    int u0 = bm_test_entry_unlock_count();
    bm_test_fail_lock_next(site, errcode);
    errno = 0;
    BitmapShard *bm = bm_open(bcl_mx_path_a, 0, 0, 0, 0, writer);
    BCL_CHECK(bm == NULL && errno == errcode, 13, "fill open did not fail with injected errno");
    BCL_CHECK(bm_test_lock_failure_consumed(site) == 1, 14, "fault not consumed at fill site");
    BCL_CHECK(bm_test_entry_unlock_count() == u0, 15, "failed fill open unlocked");
    BCL_CHECK(g_bm_cache_count == 1, 16, "installed fill mapping is not cache-owned");
    int fds1 = bcl_count_fds();
    BCL_CHECK(fds1 == fds0 + 1, 17, "installed fill fd not retained by the cache");

    /* Later ordinary open: hit path on the installed entry, data intact. */
    BitmapShard *after = bm_open(bcl_mx_path_a, 0, 0, 0, 0, writer);
    BCL_CHECK(after != NULL, 18, "later open after failed fill failed");
    if (writer) {
        BCL_CHECK(bm_set(after, (const uint8_t *)"n", 1, 21) == 0, 19, "write after failed fill failed");
        BCL_CHECK(bm_test(after, (const uint8_t *)"n", 1, 21) == 1, 20, "new bit lost");
    } else {
        BCL_CHECK(bm_test(after, (const uint8_t *)"a", 1, 7) == 1, 21, "original bit lost");
    }
    bm_close(after);
    bm_cache_shutdown();
    int fds2 = bcl_count_fds();
    BCL_CHECK(fds2 == fds0, 22, "child-local shutdown did not release installed fds");
    return 0;
}
static int bcl_mx_fill_rd(void) { return bcl_mx_fill(0, BM_TEST_LOCK_FILL_RD, EAGAIN); }
static int bcl_mx_fill_wr(void) { return bcl_mx_fill(1, BM_TEST_LOCK_FILL_WR, EDEADLK); }

/* S5: repeated fill failures must not grow live fd/map count without bound. */
static int bcl_mx_fill_repeat(void) {
    BCL_CHECK(bcl_child_cache_default() == 0, 10, "default cache fixture failed");
    int fds0 = bcl_count_fds();
    BCL_CHECK(fds0 >= 0, 11, "fd scan failed");
    bm_test_reset_lock_failure();
    for (int k = 0; k < 3; k++) {
        bm_test_fail_lock_next(BM_TEST_LOCK_FILL_WR, EDEADLK);
        errno = 0;
        BitmapShard *bm = bm_open(bcl_mx_paths16[k], BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
        BCL_CHECK(bm == NULL && errno == EDEADLK, 20, "repeat fill did not fail");
        BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_FILL_WR) == k + 1,
                  21, "repeat fill consumption count wrong");
    }
    BCL_CHECK(g_bm_cache_count == 3, 22, "each failed fill must install exactly one entry");
    int fds1 = bcl_count_fds();
    BCL_CHECK(fds1 == fds0 + 3, 23, "installed fd count grew unexpectedly");
    bm_cache_shutdown();
    int fds2 = bcl_count_fds();
    BCL_CHECK(fds2 == fds0, 24, "shutdown did not release installed fds");
    return 0;
}

/* Shared fixture: the calling thread write-holds all 16 cache entries. */
static int bcl_mx_seed16_selfheld(BitmapShard **held) {
    BCL_CHECK(bcl_child_cache16() == 0, 10, "cap16 cache fixture failed");
    for (int i = 0; i < 16; i++) {
        held[i] = bm_open(bcl_mx_paths16[i], BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
        BCL_CHECK(held[i] != NULL, 11, "self-held seed open failed");
    }
    BCL_CHECK(g_bm_cache_count == 16, 12, "seed16 cache count wrong");
    return 0;
}

/* S6: hard try error (DROP_TRY) followed by busy candidates; the selected
 * blocking wait really returns EDEADLK (self-held) and reaches the caller.
 * DROP_WAIT consumption stays 0, proving the wait error came from pthread,
 * not the seam; the try error neither masks it nor loops. */
static int bcl_mx_drop_try_einval_selfhold(void) {
    BitmapShard *held[16];
    int rc = bcl_mx_seed16_selfheld(held);
    BCL_CHECK(rc == 0, 10 + rc, "self-held seed failed");
    bm_test_reset_lock_failure();

    int u0 = bm_test_entry_unlock_count();
    bm_test_fail_lock_next(BM_TEST_LOCK_DROP_TRY, EINVAL);
    errno = 0;
    BitmapShard *bm = bm_open(bcl_mx_path_b, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_CHECK(bm == NULL && errno == EDEADLK, 20,
              "open did not fail with the selected wait's EDEADLK");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_TRY) == 1, 21,
              "try fault not consumed at drop-try");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_WAIT) == 0, 22,
              "wait error must come from pthread, not the seam");
    BCL_CHECK(bm_test_entry_unlock_count() == u0, 23, "failed open unlocked");
    for (int i = 0; i < 16; i++) BCL_CHECK(bm_close_checked(held[i]) == 0, 24, "close failed");
    BitmapShard *after = bm_open(bcl_mx_path_b, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_CHECK(after != NULL, 25, "open after release failed");
    bm_close(after);
    return 0;
}

/* Helper-thread fixture: another thread holds locks on all 16 entries
 * (real cross-thread contention; no self-upgrade anywhere). writer_mode=1
 * holds writer locks (saturated-cold-writer scenario); 0 holds readers. */
typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int  writer_mode;
    int  fail_after;
    int  ready;
    int  release;
    int  closed;
    int  rc;
    BitmapShard *held[16];
} BclHolder;

#define BCL_GUARD(cond, code, msg) \
    do { if (!(cond)) { bcl_stage(msg); rc = (code); goto cleanup; } } while (0)

static void *bcl_holder_main(void *arg) {
    BclHolder *h = arg;
    g_db = bcl_child_db;
    int failed = 0;
    for (int i = 0; i < 16; i++) {
        if (h->fail_after == i + 1)
            bm_test_fail_lock_next(h->writer_mode ? BM_TEST_LOCK_FILL_WR : BM_TEST_LOCK_FILL_RD, EIO);
        h->held[i] = bm_open(bcl_mx_paths16[i], BCL_SELFHOLD_SLOTS, 1, 0, 0,
                             h->writer_mode);
        if (!h->held[i]) { failed = 1; break; }
    }
    pthread_mutex_lock(&h->mu);
    if (!failed) {
        h->ready = 1;
        pthread_cond_broadcast(&h->cv);
        while (!h->release) pthread_cond_wait(&h->cv, &h->mu);
    }
    pthread_mutex_unlock(&h->mu);
    for (int i = 0; i < 16; i++)
        if (h->held[i]) { bm_close(h->held[i]); h->held[i] = NULL; }
    bm_test_reset_lock_failure();
    pthread_mutex_lock(&h->mu);
    h->rc = failed;
    h->ready = 1;
    h->closed = 1;
    pthread_cond_broadcast(&h->cv);
    pthread_mutex_unlock(&h->mu);
    return NULL;
}

static int bcl_mx_wait_ready(BclHolder *h) {
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    pthread_mutex_lock(&h->mu);
    while (!h->ready)
        if (pthread_cond_timedwait(&h->cv, &h->mu, &dl) != 0) {
            pthread_mutex_unlock(&h->mu); return -1;
        }
    int rc = h->rc ? -1 : 0;
    pthread_mutex_unlock(&h->mu);
    return rc;
}

static void bcl_mx_release_holders(BclHolder *h) {
    pthread_mutex_lock(&h->mu);
    h->release = 1;
    pthread_cond_broadcast(&h->cv);
    pthread_mutex_unlock(&h->mu);
}


/* Arm faults, run a cold 17th writer open against a fully reader-held cache,
 * and expect it to fail with `expect_errno`. site_b < 0 arms only site_a. */
static int bcl_mx_open_fail_expect(int site_a, int err_a, int site_b, int err_b,
                                   int expect_errno, int failure_stage) {
    BclHolder h;
    memset(&h, 0, sizeof(h));
    pthread_mutex_init(&h.mu, NULL);
    pthread_cond_init(&h.cv, NULL);
    h.fail_after = failure_stage == 2 ? 4 : 0;
    int rc = 0, started = 0;
    pthread_t tid;
    BitmapShard *bm = NULL;
    BCL_GUARD(bcl_child_cache16() == 0, 10, "cap16 cache fixture failed");
    BCL_GUARD(pthread_create(&tid, NULL, bcl_holder_main, &h) == 0, 11, "holder thread failed");
    started = 1;
    BCL_GUARD(bcl_mx_wait_ready(&h) == 0, 12, "holder not ready");
    if (failure_stage == 1) { rc = 29; goto cleanup; }
    bm_test_reset_lock_failure();
    int u0 = bm_test_entry_unlock_count();
    bm_test_fail_lock_next(site_a, err_a);
    if (site_b >= 0) bm_test_fail_lock_next(site_b, err_b);
    errno = 0;
    bm = bm_open(bcl_mx_path_b, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    int err = errno;
    BCL_GUARD(bm == NULL && err == expect_errno, 20, "open did not fail with expected errno");
    BCL_GUARD(bm_test_lock_failure_consumed(site_a) == 1, 21, "site_a fault not consumed");
    if (site_b >= 0)
        BCL_GUARD(bm_test_lock_failure_consumed(site_b) == 1, 22, "site_b fault not consumed");
    BCL_GUARD(bm_test_entry_unlock_count() == u0, 23, "failed open unlocked");
cleanup:
    if (bm) bm_close(bm);
    bm_test_reset_lock_failure();
    if (started) {
        bcl_mx_release_holders(&h);
        if (pthread_join(tid, NULL) != 0) _exit(125);
    }
    pthread_cond_destroy(&h.cv);
    pthread_mutex_destroy(&h.mu);
    if (rc != 0) return rc;
    for (int i = 0; i < 16; i++) {
        BitmapShard *s = bm_open(bcl_mx_paths16[i], 0, 0, 0, 0, 0);
        BCL_CHECK(s != NULL, 26, "cached entry lost after failed open");
        bm_close(s);
    }
    BitmapShard *b = bm_open(bcl_mx_path_b, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_CHECK(b != NULL, 27, "cold open after release failed");
    bm_close(b);
    return 0;
}

static int bcl_mx_combined_edeadlk(void) {
    /* First try candidate hard-errors (EINVAL); the later candidates are
     * genuinely busy; the selected blocking wait is injected with EDEADLK.
     * The blocking error must take precedence. */
    return bcl_mx_open_fail_expect(BM_TEST_LOCK_DROP_TRY, EINVAL,
                                   BM_TEST_LOCK_DROP_WAIT, EDEADLK, EDEADLK, 0);
}
static int bcl_mx_combined_eagain(void) {
    return bcl_mx_open_fail_expect(BM_TEST_LOCK_DROP_TRY, EINVAL,
                                   BM_TEST_LOCK_DROP_WAIT, EAGAIN, EAGAIN, 0);
}
static int bcl_mx_wait_edeadlk_only(void) {
    return bcl_mx_open_fail_expect(BM_TEST_LOCK_DROP_WAIT, EDEADLK, -1, 0, EDEADLK, 0);
}

/* S10: checked invalidation must expose the blocking acquisition error. */
static int bcl_mx_checked_invalidate(void) {
    BCL_CHECK(bcl_mx_seed_a(0) == 0, 10, "seed A failed");
    bm_test_reset_lock_failure();
    bm_test_fail_lock_next(BM_TEST_LOCK_DROP_WAIT, EDEADLK);
    errno = 0;
    int rc = bm_cache_invalidate_checked(bcl_mx_path_a);
    BCL_CHECK(rc == -1 && errno == EDEADLK, 11,
              "checked invalidation must fail with the returned lock error");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_WAIT) == 1, 12,
              "fault not consumed at drop-wait");
    /* Entry untouched: a plain open still hits it. */
    BitmapShard *bm = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(bm != NULL, 13, "entry lost after failed invalidation");
    BCL_CHECK(bm_test(bm, (const uint8_t *)"a", 1, 7) == 1, 14, "bits changed");
    bm_close(bm);
    /* Later invalidate (entry now free) succeeds and a reopen refills. */
    BCL_CHECK(bm_cache_invalidate_checked(bcl_mx_path_a) == 0, 15,
              "later checked invalidation failed");
    BitmapShard *bm2 = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(bm2 != NULL, 16, "reopen after invalidation failed");
    BCL_CHECK(bm_test(bm2, (const uint8_t *)"a", 1, 7) == 1, 17, "bits lost");
    bm_close(bm2);
    return 0;
}

/* S11: bm_cache_invalidate_nowait through existing publication behavior. */
static int bcl_mx_publish_nowait(void) {
    BCL_CHECK(bcl_child_cache_default() == 0, 10, "default cache fixture failed");
    char tmp1[256], tmp2[256];
    snprintf(tmp1, sizeof(tmp1), "%s/pub1.bm", bcl_mx_dir);
    snprintf(tmp2, sizeof(tmp2), "%s/pub2.bm", bcl_mx_dir);
    bm_test_reset_lock_failure();

    /* (a) busy retained target: publication must tolerate the ordinary
     *     busy (0) result and the stale entry must retire afterwards. */
    BitmapShard *seed = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 1);
    BCL_CHECK(seed != NULL, 11, "target seed open failed");
    BCL_CHECK(bm_close_checked(seed) == 0, 12, "seed close failed");
    BitmapShard *busy = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(busy != NULL, 13, "retained reader failed");

    BitmapShard *t1 = bm_open(tmp1, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_CHECK(t1 != NULL, 14, "tmp1 create failed");
    BCL_CHECK(bm_set(t1, (const uint8_t *)"p1", 2, 3) == 0, 15, "tmp1 set failed");
    BCL_CHECK(bm_close_checked(t1) == 0, 16, "tmp1 close failed");

    BCL_CHECK(bm_publish_replace(bcl_mx_path_a, tmp1) == BM_PUBLISH_OK, 17,
              "publication must tolerate a busy target");
    BCL_CHECK(bm_close_checked(busy) == 0, 18, "busy reader close failed");
    BitmapShard *a1 = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(a1 != NULL, 19, "open after publish failed");
    BCL_CHECK(bm_test(a1, (const uint8_t *)"p1", 2, 3) == 1, 20, "published bits not served");
    bm_close(a1);

    /* (b) hard try error inside the post-rename nowait invalidation is
     *     discarded by the existing publication policy (void consumer). */
    bm_test_fail_lock_next(BM_TEST_LOCK_DROP_TRY, EINVAL);
    BitmapShard *t2 = bm_open(tmp2, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_CHECK(t2 != NULL, 21, "tmp2 create failed");
    BCL_CHECK(bm_set(t2, (const uint8_t *)"p2", 2, 3) == 0, 22, "tmp2 set failed");
    BCL_CHECK(bm_close_checked(t2) == 0, 23, "tmp2 close failed");
    BCL_CHECK(bm_publish_replace(bcl_mx_path_a, tmp2) == BM_PUBLISH_OK, 24,
              "publication must not surface the discarded nowait error");
    BCL_CHECK(bm_test_lock_failure_consumed(BM_TEST_LOCK_DROP_TRY) == 1, 25,
              "nowait path did not consume the try fault");
    BitmapShard *a2 = bm_open(bcl_mx_path_a, 0, 0, 0, 0, 0);
    BCL_CHECK(a2 != NULL, 26, "open after second publish failed");
    BCL_CHECK(bm_test(a2, (const uint8_t *)"p2", 2, 3) == 1, 27, "second publish not served");
    bm_close(a2);
    unlink(tmp1);
    unlink(tmp2);
    return 0;
}

/* Coordinator thread for S12: waits for the main thread's blocking wait to be
 * entered (seam observation), then releases the reader holders. */
typedef struct {
    BclHolder *h;
    int baseline;
    int observed;
} BclReleaser;

static void *bcl_releaser_main(void *arg) {
    BclReleaser *r = arg;
    g_db = bcl_child_db;
    for (int i = 0; i < 10000; i++) {
        if (bm_test_lock_entered(BM_TEST_LOCK_DROP_WAIT) > r->baseline) {
            r->observed = 1;
            break;
        }
        pthread_mutex_lock(&r->h->mu);
        int released = r->h->release;
        pthread_mutex_unlock(&r->h->mu);
        if (released) break;
        bcl_sleep_ms(1);
    }
    bcl_mx_release_holders(r->h);
    return NULL;
}

/* S12: real EBUSY try results followed by a real blocking wait that
 * completes when the other thread releases — ordinary contention preserved,
 * never refused. */
static int bcl_mx_busy_then_wait(void) {
    BclHolder h;
    memset(&h, 0, sizeof(h));
    pthread_mutex_init(&h.mu, NULL);
    pthread_cond_init(&h.cv, NULL);
    int rc = 0, started = 0, releaser_started = 0;
    pthread_t tid, rtid;
    BitmapShard *bm = NULL;
    BclReleaser rel = { .h = &h };
    BCL_GUARD(bcl_child_cache16() == 0, 10, "cap16 cache fixture failed");
    BCL_GUARD(pthread_create(&tid, NULL, bcl_holder_main, &h) == 0, 11, "holder thread failed");
    started = 1;
    BCL_GUARD(bcl_mx_wait_ready(&h) == 0, 12, "holder not ready");
    bm_test_reset_lock_failure();
    rel.baseline = bm_test_lock_entered(BM_TEST_LOCK_DROP_WAIT);
    BCL_GUARD(pthread_create(&rtid, NULL, bcl_releaser_main, &rel) == 0, 13,
              "releaser thread failed");
    releaser_started = 1;
    bm = bm_open(bcl_mx_path_b, BCL_SELFHOLD_SLOTS, 1, 0, 0, 1);
    BCL_GUARD(bm != NULL, 20, "open under real contention failed");
    BCL_GUARD(pthread_join(rtid, NULL) == 0, 23, "releaser join failed");
    releaser_started = 0;
    BCL_GUARD(rel.observed, 21, "blocking wait never entered");
    BCL_GUARD(bm_test_lock_entered(BM_TEST_LOCK_DROP_WAIT) > rel.baseline, 22,
              "blocking wait observation missing");
cleanup:
    if (bm) bm_close(bm);
    if (started) bcl_mx_release_holders(&h);
    if (releaser_started && pthread_join(rtid, NULL) != 0) _exit(125);
    if (started && pthread_join(tid, NULL) != 0) _exit(125);
    pthread_cond_destroy(&h.cv);
    pthread_mutex_destroy(&h.mu);
    return rc;
}

typedef struct { const char *label; BclChildFn fn; } BclMxScenario;

static const BclMxScenario bcl_mx_scenarios[] = {
    { "hit-rd-eagain",          bcl_mx_hit_rd },
    { "hit-wr-edeadlk",         bcl_mx_hit_wr },
    { "fill-rd-eagain",         bcl_mx_fill_rd },
    { "fill-wr-edeadlk",        bcl_mx_fill_wr },
    { "fill-repeat-bounded",    bcl_mx_fill_repeat },
    { "drop-try-einval-self",   bcl_mx_drop_try_einval_selfhold },
    { "combined-wait-edeadlk",  bcl_mx_combined_edeadlk },
    { "combined-wait-eagain",   bcl_mx_combined_eagain },
    { "wait-edeadlk-only",      bcl_mx_wait_edeadlk_only },
    { "checked-invalidate",     bcl_mx_checked_invalidate },
    { "publish-nowait",         bcl_mx_publish_nowait },
    { "busy-then-wait",         bcl_mx_busy_then_wait },
};
#define BCL_MX_N ((int)(sizeof(bcl_mx_scenarios) / sizeof(bcl_mx_scenarios[0])))

/* One fork-isolated child PER scenario: each starts from a fresh cache and
 * leaves nothing behind, so a base/revert failure in one scenario never
 * masks the others (one isolated child per scenario). */
static int bcl_mx_scenario_index;   /* set before fork, read in the child */

static int bcl_mx_scenario_child(void) {
    int i = bcl_mx_scenario_index;
    char msg[96];
    snprintf(msg, sizeof(msg), "scenario %d (%s)", i, bcl_mx_scenarios[i].label);
    bcl_stage(msg);
    int rc = bcl_mx_scenarios[i].fn();
    if (rc != 0) bcl_stage(msg);
    if (rc == 0) bcl_child_cache_teardown();
    return rc;
}

static int test_bitmap_cache_lock_errors_run(void) {
    ASSERT_EQ_INT(bcl_mx_prepare(), 0, "matrix fixture dir");
    for (int i = 0; i < BCL_MX_N; i++) {
        bcl_mx_scenario_index = i;
        char label[96], desc[128];
        snprintf(label, sizeof(label), "matrix[%d:%s]", i, bcl_mx_scenarios[i].label);
        snprintf(desc, sizeof(desc), "scenario %d (%s) passed all checks",
                 i, bcl_mx_scenarios[i].label);
        BclChildResult r;
        int ran = bcl_run_child(label, bcl_mx_scenario_child, &r);
        ASSERT_EQ_INT(ran, 1, "matrix child ran (watchdog infrastructure)");
        ASSERT_EQ_INT(r.timed_out, 0, "matrix child finished within the bound");
        ASSERT_EQ_INT(r.exit_code, 0, desc);
    }
    ASSERT_EQ_INT(bcl_rmrf(bcl_mx_dir), 0, "matrix fixture cleanup");
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ───────────────────────── case 3: ordinary contention ───────────────────────── */

/* Waiter thread: opens, validates contents, and closes the handle in THIS
 * thread — an rwlock acquired here must be released here; a parent-side
 * bm_close would unlock another thread's lock. The parent only reads the
 * recorded outcome. rc encoding: 0 ok; -1 open failed; -2 data/closed
 * validation failed. */
typedef struct {
    char path[256];
    int  writer;
    int  create;      /* 1 = cold-path open (creates the file) */
    int  go;
    int  done;
    int  rc;          /* see encoding above */
    pthread_mutex_t mu;
    pthread_cond_t  cv;
} BclWaiter;

static void *bcl_waiter_main(void *arg) {
    BclWaiter *w = arg;
    g_db = bcl_child_db;
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    pthread_mutex_lock(&w->mu);
    while (!w->go)
        if (pthread_cond_timedwait(&w->cv, &w->mu, &dl) != 0) {
            w->rc = -1; w->done = 1;
            pthread_cond_broadcast(&w->cv);
            pthread_mutex_unlock(&w->mu);
            return NULL;
        }
    pthread_mutex_unlock(&w->mu);

    BitmapShard *bm = bm_open(w->path, w->create ? BCL_SELFHOLD_SLOTS : 0,
                              w->create, 0, 0, w->writer);
    w->rc = bm ? 0 : -1;
    if (bm) {
        int ok;
        if (w->writer) {
            ok = bm_set(bm, (const uint8_t *)"c", 1, 5) == 0 &&
                 bm_test(bm, (const uint8_t *)"c", 1, 5) == 1;
        } else {
            ok = bm_test(bm, (const uint8_t *)"a", 1, 7) == 1;
        }
        if (!ok) w->rc = -2;
        if (bm_close_checked(bm) != 0) w->rc = -2;
    }
    pthread_mutex_lock(&w->mu);
    w->done = 1;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    return NULL;
}

static void bcl_waiter_go(BclWaiter *w) {
    pthread_mutex_lock(&w->mu);
    w->go = 1;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
}

static int bcl_waiter_done_wait(BclWaiter *w) {
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    pthread_mutex_lock(&w->mu);
    while (!w->done)
        if (pthread_cond_timedwait(&w->cv, &w->mu, &dl) != 0) { pthread_mutex_unlock(&w->mu); return -1; }
    int rc = w->rc;
    pthread_mutex_unlock(&w->mu);
    return rc;
}

/* Shared scenario body: a real other-thread holder blocks the waiter's
 * acquisition; the waiter must wait (not refuse), then complete after
 * release with correct data. The coordinator loop runs on the calling
 * thread, which never blocks. */
static int bcl_contention_scenario(int writer_waiter, int holder_writer,
                                   const char *path_waiter, const char *path_holder) {
    BclWaiter w;
    memset(&w, 0, sizeof(w));
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cv, NULL);
    snprintf(w.path, sizeof(w.path), "%s", path_waiter);
    w.writer = writer_waiter;
    int rc = 0, started = 0;
    pthread_t wtid;
    BitmapShard *held_handle = NULL;
    int site = writer_waiter ? BM_TEST_LOCK_HIT_WR : BM_TEST_LOCK_HIT_RD;
    held_handle = bm_open(path_holder, 0, 0, 0, 0, holder_writer);
    BCL_GUARD(held_handle != NULL, 10, "holder open failed");
    int entered0 = bm_test_lock_entered(site);
    BCL_GUARD(pthread_create(&wtid, NULL, bcl_waiter_main, &w) == 0, 11, "waiter thread failed");
    started = 1;
    bcl_waiter_go(&w);
    int entered = 0, stayed = 0;
    for (int i = 0; i < 10000; i++) {
        if (bm_test_lock_entered(site) > entered0) { entered = 1; break; }
        bcl_sleep_ms(1);
    }
    BCL_GUARD(entered, 12, "waiter never entered its acquisition site");
    pthread_mutex_lock(&w.mu);
    stayed = !w.done;
    pthread_mutex_unlock(&w.mu);
    BCL_GUARD(stayed, 13, "contended acquisition completed before release");
    bcl_sleep_ms(100);
    pthread_mutex_lock(&w.mu);
    stayed = !w.done;
    pthread_mutex_unlock(&w.mu);
    BCL_GUARD(stayed, 14, "contended acquisition completed while held");
    int close_rc = bm_close_checked(held_handle);
    held_handle = NULL;
    BCL_GUARD(close_rc == 0, 15, "holder close failed");
    BCL_GUARD(bcl_waiter_done_wait(&w) == 0, 16, "waiter failed after release");
cleanup:
    if (held_handle) bm_close(held_handle);
    if (started && pthread_join(wtid, NULL) != 0) _exit(125);
    pthread_cond_destroy(&w.cv);
    pthread_mutex_destroy(&w.mu);
    return rc;
}

static int bcl_cont_child(void) {
    bcl_stage("hit-reader waits for writer");
    BCL_CHECK(bcl_child_cache_default() == 0, 10, "cache fixture failed");
    BCL_CHECK(bcl_mx_seed_a(0) == 0, 11, "seed A failed");
    int rc = bcl_contention_scenario(0, 1, bcl_mx_path_a, bcl_mx_path_a);
    BCL_CHECK(rc == 0, 12, "hit-reader contention scenario failed");
    bm_cache_shutdown();

    bcl_stage("hit-writer waits for reader");
    BCL_CHECK(bcl_child_cache_default() == 0, 20, "cache fixture failed");
    BCL_CHECK(bcl_mx_seed_a(0) == 0, 21, "seed A failed");
    rc = bcl_contention_scenario(1, 0, bcl_mx_path_a, bcl_mx_path_a);
    BCL_CHECK(rc == 0, 22, "hit-writer contention scenario failed");
    bm_cache_shutdown();

    bcl_stage("cold writer waits for other-thread writer");
    BclHolder h;
    BclWaiter w;
    memset(&h, 0, sizeof(h));
    memset(&w, 0, sizeof(w));
    pthread_mutex_init(&h.mu, NULL);
    pthread_cond_init(&h.cv, NULL);
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cv, NULL);
    h.writer_mode = 1;
    w.writer = 1;
    w.create = 1;
    snprintf(w.path, sizeof(w.path), "%s", bcl_mx_path_b);
    int holder_started = 0, waiter_started = 0;
    pthread_t htid, wtid;
    BCL_GUARD(bcl_child_cache16() == 0, 30, "cap16 cache fixture failed");
    BCL_GUARD(pthread_create(&htid, NULL, bcl_holder_main, &h) == 0, 31, "holder thread failed");
    holder_started = 1;
    BCL_GUARD(bcl_mx_wait_ready(&h) == 0, 32, "holder not ready");
    BCL_GUARD(g_bm_cache_count == 16, 33, "saturation cache count wrong");
    int entered0 = bm_test_lock_entered(BM_TEST_LOCK_DROP_WAIT);
    BCL_GUARD(pthread_create(&wtid, NULL, bcl_waiter_main, &w) == 0, 34, "waiter thread failed");
    waiter_started = 1;
    bcl_waiter_go(&w);
    int entered = 0, stayed = 0;
    for (int i = 0; i < 10000; i++) {
        if (bm_test_lock_entered(BM_TEST_LOCK_DROP_WAIT) > entered0) { entered = 1; break; }
        bcl_sleep_ms(1);
    }
    BCL_GUARD(entered, 35, "cold writer never entered blocking eviction");
    pthread_mutex_lock(&w.mu);
    stayed = !w.done;
    pthread_mutex_unlock(&w.mu);
    BCL_GUARD(stayed, 36, "cold writer completed before release");
    bcl_sleep_ms(100);
    pthread_mutex_lock(&w.mu);
    stayed = !w.done;
    pthread_mutex_unlock(&w.mu);
    BCL_GUARD(stayed, 37, "cold writer completed while held");
    bcl_mx_release_holders(&h);
    BCL_GUARD(bcl_waiter_done_wait(&w) == 0, 38, "cold writer failed after release");
cleanup:
    if (holder_started) bcl_mx_release_holders(&h);
    if (holder_started && pthread_join(htid, NULL) != 0) _exit(125);
    if (waiter_started && pthread_join(wtid, NULL) != 0) _exit(125);
    pthread_cond_destroy(&h.cv);
    pthread_mutex_destroy(&h.mu);
    pthread_cond_destroy(&w.cv);
    pthread_mutex_destroy(&w.mu);
    if (rc == 0) bcl_child_cache_teardown();
    return rc;
}

static int test_bitmap_cache_contention_run(void) {
    ASSERT_EQ_INT(bcl_mx_prepare(), 0, "contention fixture dir");
    BclChildResult r;
    int ran = bcl_run_child("contention", bcl_cont_child, &r);
    ASSERT_EQ_INT(ran, 1, "contention child ran (watchdog infrastructure)");
    ASSERT_EQ_INT(r.timed_out, 0, "contention child finished within the bound");
    ASSERT_EQ_INT(r.exit_code, 0, "contention child passed all checks");
    ASSERT_EQ_INT(bcl_rmrf(bcl_mx_dir), 0, "contention fixture cleanup");
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ─────────────────── shared integration helpers (cases 4 and 5) ─────────────────── */

/* Checked growable string builder. Every append
 * measures first, checks SIZE_MAX arithmetic, grows before writing and
 * preserves the old pointer on realloc failure. Any failure latches
 * `failed`; callers must check it before issuing a request. */
typedef struct { char *buf; size_t len; size_t cap; int failed; } BclBuf;

static void bcl_buf_init(BclBuf *b) { b->buf = NULL; b->len = 0; b->cap = 0; b->failed = 0; }
static void bcl_buf_free(BclBuf *b) { free(b->buf); b->buf = NULL; b->len = 0; b->cap = 0; b->failed = 0; }

/* Grow to fit `extra` more bytes (+1 NUL). Preserves the old pointer on
 * realloc failure; latches `failed`. Returns 0 when room is available. */
static int bcl_buf_reserve(BclBuf *b, size_t extra) {
    if (b->failed) return -1;
    if (extra > SIZE_MAX - b->len - 1) { b->failed = 1; return -1; }   /* overflow */
    if (b->len + extra + 1 > b->cap) {
        size_t ncap = b->cap ? b->cap : 512;
        while (ncap < b->len + extra + 1) {
            if (ncap > SIZE_MAX / 2) { b->failed = 1; return -1; }
            ncap *= 2;
        }
        char *nb = realloc(b->buf, ncap);
        if (!nb) { b->failed = 1; return -1; }   /* old pointer preserved */
        b->buf = nb;
        b->cap = ncap;
    }
    return 0;
}

static void bcl_v_appendf(BclBuf *b, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void bcl_v_appendf(BclBuf *b, const char *fmt, ...) {
    if (b->failed) return;
    va_list ap;
    va_start(ap, fmt);
    int needed = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (needed < 0) { b->failed = 1; return; }
    if (bcl_buf_reserve(b, (size_t)needed) != 0) return;
    va_start(ap, fmt);
    int w = vsnprintf(b->buf + b->len, b->cap - b->len, fmt, ap);
    va_end(ap);
    if (w < 0 || (size_t)w != (size_t)needed) { b->failed = 1; return; }
    b->len += (size_t)w;
}

/* Cheap structural check of a generated JSON request: non-empty, object at
 * top level, balanced quotes/braces/brackets. */
static int bcl_v_json_sane(const BclBuf *b) {
    if (b->failed || !b->buf || b->len == 0) return 0;
    if (b->buf[0] != '{' || b->buf[b->len - 1] != '}') return 0;
    int depth_obj = 0, depth_arr = 0, in_str = 0, esc = 0;
    for (size_t i = 0; i < b->len; i++) {
        char c = b->buf[i];
        if (in_str) {
            if (esc) esc = 0;
            else if (c == '\\') esc = 1;
            else if (c == '"') in_str = 0;
            continue;
        }
        if (c == '"') in_str = 1;
        else if (c == '{') depth_obj++;
        else if (c == '}') { if (--depth_obj < 0) return 0; }
        else if (c == '[') depth_arr++;
        else if (c == ']') { if (--depth_arr < 0) return 0; }
    }
    return !in_str && depth_obj == 0 && depth_arr == 0;
}

/* ─────────────── strict response parsing ─────────────── */

static const char *bcl_json_ws(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    return p;
}

/* Parse a JSON string at `p` (pointing at the opening quote). Returns the
 * position after the closing quote and a heap-decoded copy, or NULL on
 * malformed/truncated input or allocation failure. \u escapes are rejected
 * (the keys and values this suite reads are plain). */
static const char *bcl_json_string(const char *p, char **out) {
    *out = NULL;
    if (*p != '"') return NULL;
    p++;
    size_t cap = 32, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    while (*p != '"') {
        unsigned char c = (unsigned char)*p++;
        if (c < 0x20) { free(buf); return NULL; }
        if (c == '\\') {
            unsigned char e = (unsigned char)*p++;
            switch (e) {
            case '"': case '\\': case '/': c = e; break;
            case 'n': c = '\n'; break;
            case 't': c = '\t'; break;
            case 'r': c = '\r'; break;
            case 'b': c = '\b'; break;
            case 'f': c = '\f'; break;
            case 'u': {
                unsigned int code = 0;
                for (int i = 0; i < 4; i++) {
                    unsigned char h = (unsigned char)*p;
                    unsigned int digit;
                    if (h >= '0' && h <= '9') digit = h - '0';
                    else if (h >= 'a' && h <= 'f') digit = h - 'a' + 10;
                    else if (h >= 'A' && h <= 'F') digit = h - 'A' + 10;
                    else { free(buf); return NULL; }
                    code = code * 16 + digit;
                    p++;
                }
                /* These fixture identities are ASCII; reject unsupported
                   Unicode rather than confuse escaped member names. */
                if (code == 0 || code > 0x7f) { free(buf); return NULL; }
                c = (unsigned char)code;
                break;
            }
            default: free(buf); return NULL;
            }
        }
        if (len + 2 > cap) {
            if (cap > SIZE_MAX / 2) { free(buf); return NULL; }
            size_t next = cap * 2;
            char *nb = realloc(buf, next);
            if (!nb) { free(buf); return NULL; }
            buf = nb; cap = next;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    *out = buf;
    return p + 1;
}

/* Skip one well-formed JSON value (object/array/string/number/literal).
 * Returns the position after it, or NULL. Bounded recursion depth. */
typedef struct {
    char *name;
    const char *value;
    const char *end;
} BclJsonMember;

typedef struct {
    BclJsonMember *members;
    size_t count;
} BclJsonObject;

static const char *bcl_json_object(const char *p, int depth, BclJsonObject *out);

static void bcl_json_object_free(BclJsonObject *obj) {
    for (size_t i = 0; i < obj->count; i++) free(obj->members[i].name);
    free(obj->members);
    memset(obj, 0, sizeof(*obj));
}

static const char *bcl_json_skip_value(const char *p, int depth) {
    if (depth > 32) return NULL;
    p = bcl_json_ws(p);
    if (*p == '"') {
        char *tmp = NULL;
        const char *r = bcl_json_string(p, &tmp);
        free(tmp);
        return r;
    }
    if (*p == '{') return bcl_json_object(p, depth, NULL);
    if (*p == '[') {
        p = bcl_json_ws(p + 1);
        if (*p == ']') return p + 1;
        for (;;) {
            p = bcl_json_skip_value(p, depth + 1);
            if (!p) return NULL;
            p = bcl_json_ws(p);
            if (*p == ',') { p = bcl_json_ws(p + 1); continue; }
            if (*p == ']') return p + 1;
            return NULL;
        }
    }
    if (strncmp(p, "true", 4) == 0) return p + 4;
    if (strncmp(p, "false", 5) == 0) return p + 5;
    if (strncmp(p, "null", 4) == 0) return p + 4;
    if (*p == '-') p++;
    if (*p == '0') {
        p++;
        if (*p >= '0' && *p <= '9') return NULL;
    } else if (*p >= '1' && *p <= '9') {
        do { p++; } while (*p >= '0' && *p <= '9');
    } else return NULL;
    if (*p == '.') {
        p++;
        if (*p < '0' || *p > '9') return NULL;
        do { p++; } while (*p >= '0' && *p <= '9');
    }
    if (*p == 'e' || *p == 'E') {
        p++;
        if (*p == '+' || *p == '-') p++;
        if (*p < '0' || *p > '9') return NULL;
        do { p++; } while (*p >= '0' && *p <= '9');
    }
    return p;
}

/* Bare nonnegative JSON integer: whitespace, 1+ digits, whitespace, end.
 * Rejects empty/error shapes, signs, overflow and trailing junk. */
static const char *bcl_json_object(const char *p, int depth, BclJsonObject *out) {
    BclJsonObject obj = {0};
    if (depth > 32 || *p != '{') return NULL;
    p = bcl_json_ws(p + 1);
    if (*p != '}') for (;;) {
        char *name = NULL;
        const char *q = bcl_json_string(p, &name);
        if (!q) goto fail;
        for (size_t i = 0; i < obj.count; i++)
            if (strcmp(obj.members[i].name, name) == 0) { free(name); goto fail; }
        q = bcl_json_ws(q);
        if (*q != ':') { free(name); goto fail; }
        q = bcl_json_ws(q + 1);
        const char *end = bcl_json_skip_value(q, depth + 1);
        if (!end || obj.count >= SIZE_MAX / sizeof(*obj.members) - 1) {
            free(name); goto fail;
        }
        BclJsonMember *next = realloc(obj.members, (obj.count + 1) * sizeof(*next));
        if (!next) { free(name); goto fail; }
        obj.members = next;
        obj.members[obj.count++] = (BclJsonMember){name, q, end};
        p = bcl_json_ws(end);
        if (*p == ',') { p = bcl_json_ws(p + 1); continue; }
        if (*p == '}') break;
        goto fail;
    }
    if (out) *out = obj;
    else bcl_json_object_free(&obj);
    return p + 1;
fail:
    bcl_json_object_free(&obj);
    return NULL;
}

static int bcl_v_parse_nonneg_int(const char *s, long long *out) {
    if (!s || !out) return -1;
    const char *p = bcl_json_ws(s);
    if (*p < '0' || *p > '9') return -1;
    if (*p == '0' && p[1] >= '0' && p[1] <= '9') return -1;
    errno = 0;
    char *end = NULL;
    long long v = strtoll(p, &end, 10);
    if (errno == ERANGE || end == p) return -1;
    /* strtoll accepts a sign; the leading-digit guard above excludes it,
     * and this re-check rejects any digit-adjacent sign the scan skipped. */
    if (end > p && (p[0] == '+' || p[0] == '-')) return -1;
    const char *t = bcl_json_ws(end);
    if (*t != '\0') return -1;
    *out = v;
    return 0;
}

/* Bare JSON true/false with optional trailing whitespace. */
static int bcl_v_parse_bare_bool(const char *s, int *out) {
    if (!s || !out) return -1;
    const char *p = bcl_json_ws(s);
    if (strncmp(p, "true", 4) == 0) { *out = 1; p += 4; }
    else if (strncmp(p, "false", 5) == 0) { *out = 0; p += 5; }
    else return -1;
    if (*bcl_json_ws(p) != '\0') return -1;
    return 0;
}

/* Extract one integer member value from a JSON object response
 * ("\"<member>\":<int>"). Rejects error objects and malformed shapes. */
static int bcl_v_json_int_member(const char *resp, const char *member, long long *out) {
    if (!resp || !member || !out) return -1;
    BclJsonObject obj = {0};
    const char *end = bcl_json_object(bcl_json_ws(resp), 0, &obj);
    int rc = -1;
    if (!end || *bcl_json_ws(end)) goto done;
    for (size_t i = 0; i < obj.count; i++)
        if (strcmp(obj.members[i].name, "error") == 0) goto done;
    for (size_t i = 0; i < obj.count; i++) {
        BclJsonMember *m = &obj.members[i];
        if (strcmp(m->name, member) != 0) continue;
        if (*m->value < '0' || *m->value > '9') break;
        char *nend = NULL;
        errno = 0;
        long long value = strtoll(m->value, &nend, 10);
        if (errno == ERANGE || nend != m->end) break;
        *out = value;
        rc = 0;
        break;
    }
done:
    bcl_json_object_free(&obj);
    return rc;
}

/* Validate a complete find response: a JSON array of row objects, each with
 * exactly one "key" string member (other members must be well-formed JSON).
 * Returns 0 with a sorted heap key list (possibly 0 entries for a valid
 * empty array); -1 on error objects, malformed/truncated arrays, unexpected
 * types, missing/duplicate keys or allocation failure. */
static int bcl_v_parse_find_keys(const char *s, char ***out_keys, int *out_n) {
    *out_keys = NULL;
    *out_n = 0;
    if (!s) return -1;
    const char *p = bcl_json_ws(s);
    if (*p != '[') return -1;
    p = bcl_json_ws(p + 1);
    int cap = 16, n = 0;
    char **keys = malloc((size_t)cap * sizeof(char *));
    if (!keys) return -1;
    char *rowkey = NULL;
    if (*p == ']') {
        if (*bcl_json_ws(p + 1) != '\0') { free(keys); return -1; }
        *out_keys = keys;
        return 0;
    }
    for (;;) {
        p = bcl_json_ws(p);
        if (*p != '{') goto fail;
        p = bcl_json_ws(p + 1);
        if (*p == '}' || *p == '\0') goto fail;      /* row must carry a key */
        int have_key = 0;
        for (;;) {
            char *member = NULL;
            p = bcl_json_string(p, &member);
            if (!p) goto fail;
            p = bcl_json_ws(p);
            if (*p != ':') { free(member); goto fail; }
            p = bcl_json_ws(p + 1);
            if (strcmp(member, "key") == 0) {
                free(member);
                if (have_key) goto fail;             /* duplicate key member */
                p = bcl_json_string(p, &rowkey);
                if (!p) goto fail;
                have_key = 1;
            } else {
                free(member);
                p = bcl_json_skip_value(p, 0);
                if (!p) goto fail;
            }
            p = bcl_json_ws(p);
            if (*p == ',') { p = bcl_json_ws(p + 1); continue; }
            if (*p == '}') { p = bcl_json_ws(p + 1); break; }
            goto fail;
        }
        if (!have_key || !rowkey) goto fail;
        if (n == cap) {
            int ncap = cap * 2;
            char **nk = realloc(keys, (size_t)ncap * sizeof(char *));
            if (!nk) goto fail;
            keys = nk;
            cap = ncap;
        }
        keys[n++] = rowkey;
        rowkey = NULL;
        if (*p == ',') { p = bcl_json_ws(p + 1); continue; }
        if (*p == ']') {
            p = bcl_json_ws(p + 1);
            break;
        }
        goto fail;
    }
    if (*p != '\0') goto fail;
    /* sort + reject duplicate keys */
    for (int i = 1; i < n; i++) {
        char *k = keys[i];
        int j = i - 1;
        while (j >= 0 && strcmp(keys[j], k) > 0) { keys[j + 1] = keys[j]; j--; }
        keys[j + 1] = k;
    }
    for (int i = 1; i < n; i++)
        if (strcmp(keys[i - 1], keys[i]) == 0) goto fail;
    *out_keys = keys;
    *out_n = n;
    return 0;
fail:
    free(rowkey);
    for (int i = 0; i < n; i++) free(keys[i]);
    free(keys);
    return -1;
}

/* Strict success-response validation: a complete JSON object with no
 * trailing junk, no duplicate members, no "error" member, whose "status"
 * string equals `expected_status` and — when `expected_key` is non-NULL —
 * whose "key" string equals it. Unknown members are parsed (must be
 * well-formed JSON) and ignored. Returns 0 or -1; never substring-matched. */
static int bcl_v_validate_status(const char *resp, const char *expected_status,
                                 const char *expected_key) {
    if (!resp) return -1;
    BclJsonObject obj = {0};
    const char *end = bcl_json_object(bcl_json_ws(resp), 0, &obj);
    int status_ok = 0, key_ok = expected_key == NULL, rc = -1;
    if (!end || *bcl_json_ws(end)) goto done;
    for (size_t i = 0; i < obj.count; i++) {
        BclJsonMember *m = &obj.members[i];
        if (strcmp(m->name, "error") == 0) goto done;
        if (strcmp(m->name, "status") == 0 || strcmp(m->name, "key") == 0) {
            char *value = NULL;
            const char *q = bcl_json_string(m->value, &value);
            if (!q || q != m->end) { free(value); goto done; }
            if (strcmp(m->name, "status") == 0)
                status_ok = strcmp(value, expected_status) == 0;
            else if (expected_key)
                key_ok = strcmp(value, expected_key) == 0;
            free(value);
        }
    }
    if (status_ok && key_ok) rc = 0;
done:
    bcl_json_object_free(&obj);
    return rc;
}

/* Strict single-error-object validation: a complete JSON object whose ONLY
 * member is "error" with exactly `expected_error` — no duplicates, no
 * trailing junk, no additional members. */
static int bcl_v_validate_error(const char *resp, const char *expected_error) {
    if (!resp) return -1;
    BclJsonObject obj = {0};
    const char *end = bcl_json_object(bcl_json_ws(resp), 0, &obj);
    int rc = -1;
    if (end && !*bcl_json_ws(end) && obj.count == 1 &&
        strcmp(obj.members[0].name, "error") == 0) {
        char *value = NULL;
        const char *q = bcl_json_string(obj.members[0].value, &value);
        if (q && q == obj.members[0].end && strcmp(value, expected_error) == 0) rc = 0;
        free(value);
    }
    bcl_json_object_free(&obj);
    return rc;
}

/* Child-local parser self-checks: valid shapes must
 * pass, error/malformed/truncated/wrong-type/junk responses must fail. */
static int bcl_v_parser_selfcheck_child(void) {
    long long v;
    int b;
    BCL_CHECK(bcl_v_parse_nonneg_int("0", &v) == 0 && v == 0, 1, "int 0 must parse");
    BCL_CHECK(bcl_v_parse_nonneg_int("300\n", &v) == 0 && v == 300, 2, "int with newline must parse");
    BCL_CHECK(bcl_v_parse_nonneg_int(" 42\t", &v) == 0 && v == 42, 3, "int with JSON ws must parse");
    BCL_CHECK(bcl_v_parse_nonneg_int("", &v) == -1, 4, "empty must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("abc", &v) == -1, 5, "letters must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("12x", &v) == -1, 6, "trailing junk must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("-1", &v) == -1, 7, "negative must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("+1", &v) == -1, 8, "plus sign must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("99999999999999999999999", &v) == -1, 9, "overflow must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("1 2", &v) == -1, 10, "two ints must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("00", &v) == -1, 58, "leading zeros must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("0300", &v) == -1, 59, "octal-like count must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("true", &v) == -1, 11, "bool as int must fail");
    BCL_CHECK(bcl_v_parse_nonneg_int("{\"count\":1}", &v) == -1, 12, "object as int must fail");

    BCL_CHECK(bcl_v_parse_bare_bool("true", &b) == 0 && b == 1, 13, "true must parse");
    BCL_CHECK(bcl_v_parse_bare_bool("false\n", &b) == 0 && b == 0, 14, "false must parse");
    BCL_CHECK(bcl_v_parse_bare_bool("1", &b) == -1, 15, "int as bool must fail");
    BCL_CHECK(bcl_v_parse_bare_bool("tru", &b) == -1, 16, "truncated bool must fail");
    BCL_CHECK(bcl_v_parse_bare_bool("true junk", &b) == -1, 17, "bool + junk must fail");
    BCL_CHECK(bcl_v_parse_bare_bool("{\"error\":\"x\"}", &b) == -1, 18, "error as bool must fail");

    char **keys = NULL;
    int n = 0;
    BCL_CHECK(bcl_v_parse_find_keys("[]", &keys, &n) == 0 && n == 0, 19,
              "valid empty array must pass");
    free(keys);
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\"}]", &keys, &n) == 0 && n == 1 &&
              strcmp(keys[0], "a") == 0, 20, "single-key array must parse");
    for (int i = 0; i < n; i++) free(keys[i]);
    free(keys);
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\",\"value\":{\"n\":1}}]", &keys, &n) == 0 &&
              n == 1, 21, "row with value object must parse");
    for (int i = 0; i < n; i++) free(keys[i]);
    free(keys);
    BCL_CHECK(bcl_v_parse_find_keys("true", &keys, &n) == -1, 22, "bool as find must fail");
    BCL_CHECK(bcl_v_parse_find_keys("{\"error\":\"e\"}", &keys, &n) == -1, 23,
              "error object as find must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\"", &keys, &n) == -1, 24,
              "truncated array must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\",\"key\":\"b\"}]", &keys, &n) == -1, 25,
              "duplicate key member must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\"},{\"key\":\"a\"}]", &keys, &n) == -1, 26,
              "duplicate row keys must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\"},]", &keys, &n) == -1, 27,
              "trailing comma must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":5}]", &keys, &n) == -1, 28,
              "wrong key type must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"nokey\":1}]", &keys, &n) == -1, 29,
              "missing key member must fail");
    BCL_CHECK(bcl_v_parse_find_keys("[{\"key\":\"a\"}x]", &keys, &n) == -1, 30,
              "trailing junk after array must fail");

    BCL_CHECK(bcl_v_validate_status("{\"status\":\"inserted\",\"key\":\"k00400\"}",
                                    "inserted", "k00400") == 0, 40,
              "valid status+key response must pass");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"updated\",\"key\":\"k00001\",\"record\":{\"n\":1}}",
                                    "updated", "k00001") == 0, 41,
              "status+key+record object must pass");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"vacuumed\",\"cleaned\":3}",
                                    "vacuumed", NULL) == 0, 42,
              "vacuumed with cleaned member must pass");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"inserted\",\"key\":\"k00400\"} \n",
                                    "inserted", "k00400") == 0, 43,
              "trailing JSON whitespace must pass");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"inserted\",\"key\":\"k00400\"}x",
                                    "inserted", "k00400") == -1, 44,
              "trailing junk must fail");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"deleted\",\"key\":\"k00002\",\"key\":\"k00002\"}",
                                    "deleted", "k00002") == -1, 45,
              "duplicate member must fail");
    BCL_CHECK(bcl_v_validate_status("{\"error\":\"boom\",\"status\":\"inserted\",\"key\":\"k00400\"}",
                                    "inserted", "k00400") == -1, 46,
              "error member must fail");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"updated\",\"key\":\"wrong\"}",
                                    "updated", "k00001") == -1, 47,
              "wrong key must fail");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"deleted\"}",
                                    "deleted", "k00002") == -1, 48,
              "missing key member must fail");
    BCL_CHECK(bcl_v_validate_status("{\"status\":\"inser", "inserted", NULL) == -1, 49,
              "truncated response must fail");
    BCL_CHECK(bcl_v_validate_status("true", "inserted", NULL) == -1, 50,
              "non-object response must fail");

    BCL_CHECK(bcl_v_validate_error("{\"error\":\"keyfile compaction failed\"}",
                                   "keyfile compaction failed") == 0, 51,
              "exact error object must pass");
    BCL_CHECK(bcl_v_validate_error("{\"error\":\"other\"}",
                                   "keyfile compaction failed") == -1, 52,
              "wrong error text must fail");
    BCL_CHECK(bcl_v_validate_error("{\"error\":\"x\",\"extra\":1}",
                                   "x") == -1, 53,
              "additional member must fail");
    BCL_CHECK(bcl_v_validate_error("{\"error\":\"x\",\"error\":\"x\"}", "x") == -1, 54,
              "duplicate error member must fail");
    BCL_CHECK(bcl_v_validate_error("{\"error\":\"x\"} junk", "x") == -1, 55,
              "trailing junk must fail");

    const char *bad_status[] = {
        "{\"status\":\"created\",\"error\":\"boom\"}",
        "{\"status\":\"created\",\"cleaned\":1,\"cleaned\":2}",
        "{\"status\":\"created\",\"extra\":01}",
        "{\"status\":\"created\",\"extra\":1.}",
        "{\"status\":\"created\",\"extra\":0x1}",
        "{\"status\":\"cre\\ated\"}",
        "{\"status\":\"cre\nated\"}"
    };
    for (size_t i = 0; i < sizeof(bad_status) / sizeof(bad_status[0]); i++)
        BCL_CHECK(bcl_v_validate_status(bad_status[i], "created", NULL) == -1,
                  56, "invalid success object must fail");
    const char *bad_bulk[] = {
        "{\"inserted\":100", "[{\"inserted\":100}]",
        "{\"inserted\":100} junk", "{\"inserted\":100,\"inserted\":9}",
        "{\"nested\":{\"inserted\":100}}", "{\"inserted\":01}",
        "{\"inserted\":100,\"extra\":1.}"
    };
    long long ins = -1;
    for (size_t i = 0; i < sizeof(bad_bulk) / sizeof(bad_bulk[0]); i++)
        BCL_CHECK(bcl_v_json_int_member(bad_bulk[i], "inserted", &ins) == -1,
                  57, "invalid bulk response must fail");
    BCL_CHECK(bcl_v_json_int_member("{\"inserted\":100}", "inserted", &ins) == 0 &&
              ins == 100, 31, "int member must parse");
    BCL_CHECK(bcl_v_json_int_member("{\"inserted\":100,\"skipped\":0}", "skipped", &ins) == 0 &&
              ins == 0, 32, "second int member must parse");
    BCL_CHECK(bcl_v_json_int_member("{\"error\":\"boom\"}", "inserted", &ins) == -1, 33,
              "error object member must fail");
    BCL_CHECK(bcl_v_json_int_member("{\"inserted\":\"x\"}", "inserted", &ins) == -1, 34,
              "string member must fail");
    bcl_stage("parser self-checks passed");
    return 0;
}

/* ───────────────────── case 4: daemon vacuum integration ───────────────────── */

#define BCL_V_DIR      "bcl"
#define BCL_V_OBJECT   "t"
#define BCL_V_FIELDS   40
#define BCL_V_ROWS     400
#define BCL_V_SPLITS   8
#define BCL_V_IO_MS    (BCL_SAN_WIDE ? 30000 : 3000)

static TestEnv bcl_v_env;
static TestClient *bcl_v_tc;
static char bcl_v_root[512];
static char bcl_v_base[512];

/* Row oracle. `live[]` is LOGICAL row membership (find/get/exists);
 * `expected_deleted` is the physical tombstone count (orphaned). Child-local
 * copies never propagate to the parent — the runner keeps the parent oracle
 * authoritative and updates it only after a fully verified stage. */
typedef struct {
    unsigned char live[BCL_V_ROWS + 1];
    int nn[BCL_V_ROWS + 1];
    int expected_deleted;
} BclOracle;

/* Parent-maintained oracle; children only read their fork copy. */
static BclOracle bcl_v_oracle_shared;

static int bcl_v_field_value(int row, int field) { return (row + field) % 4; }

static int bcl_v_row_key(char *out, size_t cap, int row) {
    return snprintf(out, cap, "k%05d", row) > 0 ? 0 : -1;
}

/* ---- request transport (checked construction; used inside children) ---- */

static int bcl_v_connect(int port) {
    TestClientCfg cfg = { .port = port, .io_timeout_ms = BCL_V_IO_MS };
    bcl_v_tc = tc_connect(&cfg);
    return bcl_v_tc ? 0 : -1;
}

/* Build a request with the growable builder, sanity-check it, send it.
 * Returns NULL on any construction/sanity/transport failure. */
static char *bcl_v_q(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static char *bcl_v_q(const char *fmt, ...) {
    BclBuf req;
    bcl_buf_init(&req);
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap2);
    va_end(ap2);
    if (needed < 0 || bcl_buf_reserve(&req, (size_t)needed) != 0) {
        va_end(ap);
        bcl_buf_free(&req);
        return NULL;
    }
    int w = vsnprintf(req.buf + req.len, req.cap - req.len, fmt, ap);
    va_end(ap);
    if (w < 0 || (size_t)w != (size_t)needed) { bcl_buf_free(&req); return NULL; }
    req.len += (size_t)w;
    if (!bcl_v_json_sane(&req)) { bcl_buf_free(&req); return NULL; }
    char *resp = NULL;
    int rc = tc_request(bcl_v_tc, req.buf, &resp);
    bcl_buf_free(&req);
    if (rc != 0) { free(resp); return NULL; }
    return resp;
}

/* Strict bare-integer request helpers. -1 on request or parse failure. */
static int bcl_v_count(const char *crit, int *out) {
    char *r = bcl_v_q("{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s}",
                      BCL_V_DIR, BCL_V_OBJECT, crit);
    long long v = -1;
    int rc = bcl_v_parse_nonneg_int(r, &v);
    free(r);
    if (rc != 0 || v < 0 || v > 1000000000LL) return -1;
    *out = (int)v;
    return 0;
}

static int bcl_v_orphaned(int *out) {
    char *r = bcl_v_q("{\"mode\":\"orphaned\",\"dir\":\"%s\",\"object\":\"%s\"}",
                      BCL_V_DIR, BCL_V_OBJECT);
    long long v = -1;
    int rc = bcl_v_parse_nonneg_int(r, &v);
    free(r);
    if (rc != 0 || v < 0 || v > 1000000000LL) return -1;
    *out = (int)v;
    return 0;
}

static int bcl_v_exists(const char *key, int *out) {
    /* key is a fixed-shape "k%05d" string; the request goes through the
       checked builder, so truncation cannot occur for valid keys. */
    char *r = bcl_v_q("{\"mode\":\"exists\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
                      BCL_V_DIR, BCL_V_OBJECT, key);
    int rc = bcl_v_parse_bare_bool(r, out);
    free(r);
    return rc;
}

/* Strict find: full validation, sorted key list, error != empty. */
static int bcl_v_find_keys(const char *crit, char ***out_keys, int *out_n) {
    char *r = bcl_v_q("{\"mode\":\"find\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":%s,"
                      "\"limit\":%d,\"fields\":[\"n\"]}",
                      BCL_V_DIR, BCL_V_OBJECT, crit, BCL_V_ROWS + 1);
    int rc = bcl_v_parse_find_keys(r, out_keys, out_n);
    free(r);
    return rc;
}

/* Sorted expected key list for one (field,value) predicate. */
static int bcl_v_expected_keys(const BclOracle *o, int field, int value,
                               char ***out_keys, int *out_n) {
    *out_keys = NULL;
    *out_n = 0;
    int cap = 64, n = 0;
    char **keys = malloc((size_t)cap * sizeof(char *));
    if (!keys) return -1;
    for (int row = 0; row <= BCL_V_ROWS; row++) {
        if (!o->live[row]) continue;
        if (bcl_v_field_value(row, field) != value) continue;
        if (n == cap) {
            int ncap = cap * 2;
            char **nk = realloc(keys, (size_t)ncap * sizeof(char *));
            if (!nk) { for (int i = 0; i < n; i++) free(keys[i]); free(keys); return -1; }
            keys = nk;
            cap = ncap;
        }
        keys[n] = malloc(16);
        if (!keys[n]) { for (int i = 0; i < n; i++) free(keys[i]); free(keys); return -1; }
        bcl_v_row_key(keys[n], 16, row);
        n++;
    }
    *out_keys = keys;
    *out_n = n;
    return 0;
}

static int bcl_v_keysets_equal(char **a, int na, char **b, int nb) {
    if (na != nb) return 0;
    for (int i = 0; i < na; i++)
        if (strcmp(a[i], b[i]) != 0) return 0;
    return 1;
}

/* Full oracle round: exact live/deleted counts, exists for every seed key
 * plus k00400, per-key values, and all 160 exact (field,value) count/find
 * comparisons. Returns 0 or a failure code; any helper error fails the
 * round (a zero-count predicate must come from a valid response). */
static int bcl_v_validate_row(const char *resp, int row, int expected_n) {
    if (!resp) return -1;
    BclJsonObject obj = {0};
    const char *end = bcl_json_object(bcl_json_ws(resp), 0, &obj);
    uint64_t fields = 0;
    int saw_n = 0, rc = -1;
    if (!end || *bcl_json_ws(end) || obj.count != BCL_V_FIELDS + 1) goto done;
    for (size_t i = 0; i < obj.count; i++) {
        BclJsonMember *m = &obj.members[i];
        if (strcmp(m->name, "n") == 0) {
            char *q = NULL;
            errno = 0;
            long value = strtol(m->value, &q, 10);
            if (errno == ERANGE || q == m->value || q != m->end || value != expected_n)
                goto done;
            saw_n = 1;
            continue;
        }
        if (m->name[0] != 'f' || m->name[1] < '0' || m->name[1] > '9') goto done;
        char *field_end = NULL;
        errno = 0;
        unsigned long parsed = strtoul(m->name + 1, &field_end, 10);
        if (errno == ERANGE || *field_end || parsed >= BCL_V_FIELDS) goto done;
        int field = (int)parsed;
        char canonical[32];
        snprintf(canonical, sizeof(canonical), "f%d", field);
        if (strcmp(canonical, m->name) != 0) goto done;
        char *value = NULL;
        const char *q = bcl_json_string(m->value, &value);
        int ok = q && q == m->end && value[0] == '0' + bcl_v_field_value(row, field) &&
                 value[1] == '\0';
        free(value);
        if (!ok) goto done;
        fields |= UINT64_C(1) << field;
    }
    if (saw_n && fields == (UINT64_C(1) << BCL_V_FIELDS) - 1) rc = 0;
done:
    bcl_json_object_free(&obj);
    return rc;
}

static int bcl_v_run_oracles(const BclOracle *o) {
    int live = 0;
    for (int row = 0; row <= BCL_V_ROWS; row++)
        if (o->live[row]) live++;
    int got_live = -1;
    if (bcl_v_count("[]", &got_live) != 0 || got_live != live) {
        char msg[128];
        char *raw = bcl_v_q("{\"mode\":\"count\",\"dir\":\"%s\",\"object\":\"%s\",\"criteria\":[]}",
                            BCL_V_DIR, BCL_V_OBJECT);
        snprintf(msg, sizeof(msg), "count[]: want %d got %d raw=%s",
                 live, got_live, raw ? raw : "(null)");
        bcl_stage(msg);
        free(raw);
        return 1;
    }
    int got_deleted = -1;
    if (bcl_v_orphaned(&got_deleted) != 0 || got_deleted != o->expected_deleted) {
        char msg[128];
        char *raw = bcl_v_q("{\"mode\":\"orphaned\",\"dir\":\"%s\",\"object\":\"%s\"}",
                            BCL_V_DIR, BCL_V_OBJECT);
        snprintf(msg, sizeof(msg), "orphaned: want %d got %d raw=%s",
                 o->expected_deleted, got_deleted, raw ? raw : "(null)");
        bcl_stage(msg);
        free(raw);
        return 2;
    }

    for (int row = 0; row <= BCL_V_ROWS; row++) {
        char key[16];
        if (bcl_v_row_key(key, sizeof(key), row) != 0) return 3;
        int present = -1;
        if (bcl_v_exists(key, &present) != 0) return 3;
        if (present != (o->live[row] ? 1 : 0)) return 4;
        if (!o->live[row]) continue;
        char req[64];
        int nw = snprintf(req, sizeof(req),
                          "{\"mode\":\"get\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"%s\"}",
                          BCL_V_DIR, BCL_V_OBJECT, key);
        if (nw < 0 || (size_t)nw >= sizeof(req)) return 3;   /* fail closed */
        char *r = bcl_v_q("%s", req);
        int bad = bcl_v_validate_row(r, row, o->nn[row]) != 0;
        free(r);
        if (bad) return 5;
    }

    for (int f = 0; f < BCL_V_FIELDS; f++) {
        for (int v = 0; v < 4; v++) {
            int expect = 0;
            for (int row = 0; row <= BCL_V_ROWS; row++)
                if (o->live[row] && bcl_v_field_value(row, f) == v) expect++;
            char crit[160];
            int cw = snprintf(crit, sizeof(crit),
                              "[{\"field\":\"f%d\",\"op\":\"eq\",\"value\":\"%d\"}]", f, v);
            if (cw < 0 || (size_t)cw >= sizeof(crit)) return 6;   /* fail closed */
            int got = -1;
            if (bcl_v_count(crit, &got) != 0 || got != expect) return 6;
            char **gotk, **wantk;
            int gn, wn;
            if (bcl_v_find_keys(crit, &gotk, &gn) != 0) return 7;
            if (bcl_v_expected_keys(o, f, v, &wantk, &wn) != 0) {
                for (int i = 0; i < gn; i++) free(gotk[i]);
                free(gotk);
                return 8;
            }
            int eq = bcl_v_keysets_equal(gotk, gn, wantk, wn);
            for (int i = 0; i < gn; i++) free(gotk[i]);
            free(gotk);
            for (int i = 0; i < wn; i++) free(wantk[i]);
            free(wantk);
            if (!eq) return 9;
        }
    }
    return 0;
}

/* ---- setup child: parser self-checks, create object, seed, delete ---- */

static int bcl_v_setup_child(void) {
    int sc = bcl_v_parser_selfcheck_child();
    BCL_CHECK(sc == 0, 1, "parser self-checks failed");
    if (bcl_v_connect(bcl_v_env.port) != 0) return 2;
    bcl_stage("connected");

    char *r = bcl_v_q("{\"mode\":\"add-dir\",\"dir\":\"%s\"}", BCL_V_DIR);
    if (!r) return 3;
    free(r);

    /* 40 enum(0,1,2,3) bitmap fields + unindexed n:int (checked builder). */
    BclBuf req;
    bcl_buf_init(&req);
    bcl_v_appendf(&req, "{\"mode\":\"create-object\",\"dir\":\"%s\",\"object\":\"%s\","
                        "\"splits\":%d,\"max_key\":16,\"fields\":[\"n:int\"",
                  BCL_V_DIR, BCL_V_OBJECT, BCL_V_SPLITS);
    for (int f = 0; f < BCL_V_FIELDS; f++)
        bcl_v_appendf(&req, ",\"f%d:enum(0,1,2,3)\"", f);
    bcl_v_appendf(&req, "],\"indexes\":[");
    for (int f = 0; f < BCL_V_FIELDS; f++)
        bcl_v_appendf(&req, "%s\"f%d:bitmap\"", f ? "," : "", f);
    bcl_v_appendf(&req, "]}");
    if (req.failed || !bcl_v_json_sane(&req)) { bcl_buf_free(&req); return 4; }
    r = NULL;
    if (tc_request(bcl_v_tc, req.buf, &r) != 0) { bcl_buf_free(&req); return 5; }
    bcl_buf_free(&req);
    int created = bcl_v_validate_status(r, "created", NULL) == 0;
    if (r) bcl_stage(r);
    free(r);
    if (!created) return 6;
    bcl_stage("object created (40 enum bitmap fields)");

    /* Ordinary capacity admits all forty fields before the tiny-cache trigger. */
    for (int from = 0; from < BCL_V_ROWS; from += 100) {
        BclBuf bulk;
        bcl_buf_init(&bulk);
        bcl_v_appendf(&bulk, "{\"mode\":\"bulk-insert\",\"dir\":\"%s\",\"object\":\"%s\",\"records\":[",
                      BCL_V_DIR, BCL_V_OBJECT);
        for (int i = from; i < from + 100; i++) {
            bcl_v_appendf(&bulk, "%s{\"key\":\"k%05d\",\"value\":{\"n\":%d",
                          i > from ? "," : "", i, i);
            for (int f = 0; f < BCL_V_FIELDS; f++)
                bcl_v_appendf(&bulk, ",\"f%d\":\"%d\"", f, bcl_v_field_value(i, f));
            bcl_v_appendf(&bulk, "}}");
        }
        bcl_v_appendf(&bulk, "]}");
        if (bulk.failed || !bcl_v_json_sane(&bulk)) { bcl_buf_free(&bulk); return 7; }
        char *resp = NULL;
        int qrc = tc_request(bcl_v_tc, bulk.buf, &resp);
        bcl_buf_free(&bulk);
        long long inserted = -1;
        int prc = bcl_v_json_int_member(resp, "inserted", &inserted);
        if (inserted != 100 && resp) bcl_stage(resp);
        free(resp);
        if (qrc != 0 || prc != 0 || inserted != 100) return 8;
    }
    bcl_stage("400 rows seeded (inserted=100 x4)");

    /* Tombstones trigger compaction; this pattern also leaves one empty
       enum value group per field to exercise legitimate zero-result oracles. */
    BclBuf del;
    bcl_buf_init(&del);
    bcl_v_appendf(&del, "{\"mode\":\"bulk-delete\",\"dir\":\"%s\",\"object\":\"%s\",\"keys\":[",
                  BCL_V_DIR, BCL_V_OBJECT);
    int first = 1, ndel = 0;
    for (int row = 0; row < BCL_V_ROWS; row += 4) {
        char key[16];
        bcl_v_row_key(key, sizeof(key), row);
        bcl_v_appendf(&del, "%s\"%s\"", first ? "" : ",", key);
        first = 0;
        ndel++;
    }
    bcl_v_appendf(&del, "]}");
    if (del.failed || !bcl_v_json_sane(&del)) { bcl_buf_free(&del); return 9; }
    char *dresp = NULL;
    int drc = tc_request(bcl_v_tc, del.buf, &dresp);
    bcl_buf_free(&del);
    long long deletedn = -1;
    int dprc = bcl_v_json_int_member(dresp, "deleted", &deletedn);
    if (deletedn != ndel && dresp) bcl_stage(dresp);
    free(dresp);
    if (drc != 0 || dprc != 0 || deletedn != ndel) return 10;
    bcl_stage("100 rows deleted (deleted=100)");

    /* Adopt the verified delete into this child's fork of the oracle before
     * running the oracles (the parent adopts the same state on success). */
    for (int row = 0; row < BCL_V_ROWS; row += 4)
        bcl_v_oracle_shared.live[row] = 0;
    bcl_v_oracle_shared.expected_deleted = ndel;

    int orc = bcl_v_run_oracles(&bcl_v_oracle_shared);
    if (orc != 0) return 10 + orc;
    bcl_stage("setup oracles exact (live300/deleted100)");
    tc_close(bcl_v_tc);
    bcl_v_tc = NULL;
    return 0;
}

/* ---- oracle-only child (post-refusal, pre-success, post-vacuum) ---- */

static int bcl_v_oracle_child(void) {
    if (bcl_v_connect(bcl_v_env.port) != 0) return 1;
    int rc = bcl_v_run_oracles(&bcl_v_oracle_shared);
    tc_close(bcl_v_tc);
    bcl_v_tc = NULL;
    return rc ? 10 + rc : 0;
}

/* ---- vacuum request child ---- */

static int bcl_v_vacuum_child(void) {
    if (bcl_v_connect(bcl_v_env.port) != 0) return 3;
    char *r = bcl_v_q("{\"mode\":\"vacuum\",\"dir\":\"%s\",\"object\":\"%s\"}",
                      BCL_V_DIR, BCL_V_OBJECT);
    tc_close(bcl_v_tc);
    bcl_v_tc = NULL;
    if (!r) { bcl_stage("vacuum request timed out / failed"); return 2; }
    bcl_stage(r);   /* raw response: diagnostics only */
    if (bcl_v_validate_error(r, "keyfile compaction failed") == 0)
        bcl_stage("VACUUM-REFUSED-EXACT");
    else if (bcl_v_validate_status(r, "vacuumed", NULL) == 0)
        bcl_stage("VACUUM-OK-EXACT");
    else
        bcl_stage("VACUUM-RESPONSE-INVALID");
    free(r);
    return 0;
}

/* ---- CRUD child: insert k00400, update k00001, delete k00002 ---- */

static int bcl_v_crud_child(void) {
    BclOracle *o = &bcl_v_oracle_shared;   /* fork copy; never propagated */
    if (bcl_v_connect(bcl_v_env.port) != 0) return 1;

    /* insert k00400: n=400, f=(400+f)%4 — string enum labels */
    {
        BclBuf req;
        bcl_buf_init(&req);
        bcl_v_appendf(&req, "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
                            "\"key\":\"k00400\",\"value\":{\"n\":400",
                      BCL_V_DIR, BCL_V_OBJECT);
        for (int f = 0; f < BCL_V_FIELDS; f++)
            bcl_v_appendf(&req, ",\"f%d\":\"%d\"", f, bcl_v_field_value(BCL_V_ROWS, f));
        bcl_v_appendf(&req, "}}");
        if (req.failed || !bcl_v_json_sane(&req)) { bcl_buf_free(&req); return 2; }
        char *r = NULL;
        int qrc = tc_request(bcl_v_tc, req.buf, &r);
        bcl_buf_free(&req);
        int ok = qrc == 0 && bcl_v_validate_status(r, "inserted", "k00400") == 0;
        if (!ok && r) bcl_stage(r);
        free(r);
        if (!ok) return 3;
        o->live[BCL_V_ROWS] = 1;
        o->nn[BCL_V_ROWS] = BCL_V_ROWS;
        int c = -1, d = -1;
        if (bcl_v_count("[]", &c) != 0 || c != 301) return 4;
        if (bcl_v_orphaned(&d) != 0 || d != 0) return 5;
        int orc = bcl_v_run_oracles(o);
        if (orc != 0) return 10 + orc;
    }
    bcl_stage("insert k00400 verified (live301/deleted0)");

    /* update live k00001: n=1001, f=(1001+f)%4 == (1+f)%4 — the bitmap labels
     * are unchanged by design; the update must not add a tombstone. */
    {
        BclBuf req;
        bcl_buf_init(&req);
        bcl_v_appendf(&req, "{\"mode\":\"update\",\"dir\":\"%s\",\"object\":\"%s\","
                            "\"key\":\"k00001\",\"value\":{\"n\":1001",
                      BCL_V_DIR, BCL_V_OBJECT);
        for (int f = 0; f < BCL_V_FIELDS; f++)
            bcl_v_appendf(&req, ",\"f%d\":\"%d\"", f, bcl_v_field_value(1001, f));
        bcl_v_appendf(&req, "}}");
        if (req.failed || !bcl_v_json_sane(&req)) { bcl_buf_free(&req); return 6; }
        char *r = NULL;
        int qrc = tc_request(bcl_v_tc, req.buf, &r);
        bcl_buf_free(&req);
        int ok = qrc == 0 && bcl_v_validate_status(r, "updated", "k00001") == 0;
        if (!ok && r) bcl_stage(r);
        free(r);
        if (!ok) return 7;
        o->nn[1] = 1001;
        int c = -1, d = -1;
        if (bcl_v_count("[]", &c) != 0 || c != 301) return 8;
        if (bcl_v_orphaned(&d) != 0 || d != 0) return 9;
        int orc = bcl_v_run_oracles(o);
        if (orc != 0) return 10 + orc;
    }
    bcl_stage("update k00001 verified (live301/deleted0, no new tombstone)");

    /* delete live k00002 */
    {
        char *r = bcl_v_q("{\"mode\":\"delete\",\"dir\":\"%s\",\"object\":\"%s\",\"key\":\"k00002\"}",
                          BCL_V_DIR, BCL_V_OBJECT);
        int ok = bcl_v_validate_status(r, "deleted", "k00002") == 0;
        if (!ok && r) bcl_stage(r);
        free(r);
        if (!ok) return 11;
        o->live[2] = 0;
        o->expected_deleted = 1;   /* the fresh delete is this oracle's tombstone */
        int c = -1, d = -1;
        if (bcl_v_count("[]", &c) != 0 || c != 300) return 12;
        if (bcl_v_orphaned(&d) != 0 || d != 1) return 13;
        int orc = bcl_v_run_oracles(o);
        if (orc != 0) return 10 + orc;
    }
    bcl_stage("delete k00002 verified (live300/deleted1)");
    tc_close(bcl_v_tc);
    bcl_v_tc = NULL;
    return 0;
}

/* ───────────────────── filesystem snapshots + log helpers ───────────────────── */

typedef struct {
    off_t  kf_size;
    ino_t  kf_inode;
    unsigned char kf_hdr[24];
    unsigned char *kf_id;      /* per-slot bytes 0..16 (hash+flag) */
    size_t kf_slots;
    unsigned char **bm_bytes;  /* BCL_V_FIELDS bitmap file images */
    size_t bm_lens[BCL_V_FIELDS];
} BclSnapshot;

static void bcl_v_bm_path(char *out, size_t cap, const char *root, int field, int shard) {
    snprintf(out, cap, "%s/%s/%s/indexes/f%d/%03x.bm", root, BCL_V_DIR, BCL_V_OBJECT,
             field, shard);
}

static void bcl_v_kf_path(char *out, size_t cap, const char *root, int shard) {
    snprintf(out, cap, "%s/%s/%s/data/kf/%03x.kf", root, BCL_V_DIR, BCL_V_OBJECT, shard);
}

/* Read a whole file. The buffer is allocated one byte larger than the file
 * and NUL-terminated so text callers can strstr it safely; *out_len is the
 * exact file size (binary callers must compare lengths, not rely on the
 * terminator). */
static int bcl_v_read_file(const char *path, unsigned char **out, size_t *out_len) {
    *out = NULL;
    *out_len = 0;
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0) { close(fd); return -1; }
    size_t sz = (size_t)st.st_size;
    if (sz == SIZE_MAX) { close(fd); return -1; }
    unsigned char *buf = malloc(sz + 1);
    if (!buf) { close(fd); return -1; }
    size_t got = 0;
    while (got < sz) {
        if (bcl_budget_expired()) { free(buf); close(fd); return -1; }
        size_t chunk = sz - got;
        if (chunk > 65536) chunk = 65536;
        ssize_t n = pread(fd, buf + got, chunk, (off_t)got);
        if (n <= 0) { free(buf); close(fd); return -1; }
        got += (size_t)n;
    }
    close(fd);
    buf[got] = '\0';
    *out = buf;
    *out_len = got;
    return 0;
}

/* Snapshot the target shard's keyfile identity (inode, size, 24 B header,
 * per-slot hash+flag identity bytes) and all BCL_V_FIELDS bitmap images. */
static int bcl_v_snapshot(const char *root, int shard, BclSnapshot *s) {
    memset(s, 0, sizeof(*s));
    char kfp[1024];
    bcl_v_kf_path(kfp, sizeof(kfp), root, shard);
    /* Open before stat-ing (CodeQL TOCTOU): fstat pins the identity of the
       file actually read, so a concurrent swap cannot validate one inode
       and read another. */
    int fd = open(kfp, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return -1; }
    s->kf_size = st.st_size;
    s->kf_inode = st.st_ino;
    if (s->kf_size < 24) { close(fd); return -1; }
    if (pread(fd, s->kf_hdr, 24, 0) != 24) { close(fd); return -1; }
    s->kf_slots = (size_t)(s->kf_size - 24) / 24;
    s->kf_id = malloc(s->kf_slots * 17);
    if (!s->kf_id) { close(fd); return -1; }
    for (size_t i = 0; i < s->kf_slots; i++) {
        if (bcl_budget_expired()) { close(fd); return -1; }
        if (pread(fd, s->kf_id + i * 17, 17, (off_t)(24 + i * 24)) != 17) {
            close(fd);
            return -1;
        }
    }
    close(fd);
    s->bm_bytes = calloc(BCL_V_FIELDS, sizeof(unsigned char *));
    if (!s->bm_bytes) return -1;
    for (int f = 0; f < BCL_V_FIELDS; f++) {
        char bp[1024];
        bcl_v_bm_path(bp, sizeof(bp), root, f, shard);
        if (bcl_v_read_file(bp, &s->bm_bytes[f], &s->bm_lens[f]) != 0) return -1;
    }
    return 0;
}

static void bcl_v_snapshot_free(BclSnapshot *s) {
    free(s->kf_id);
    if (s->bm_bytes)
        for (int f = 0; f < BCL_V_FIELDS; f++) free(s->bm_bytes[f]);
    free(s->bm_bytes);
    memset(s, 0, sizeof(*s));
}

static int bcl_v_snapshots_equal(const BclSnapshot *a, const BclSnapshot *b) {
    if (!a->kf_id || !b->kf_id || !a->bm_bytes || !b->bm_bytes) return 0;
    if (a->kf_size != b->kf_size || a->kf_inode != b->kf_inode) return 0;
    if (memcmp(a->kf_hdr, b->kf_hdr, 24) != 0) return 0;
    if (a->kf_slots != b->kf_slots) return 0;
    if (memcmp(a->kf_id, b->kf_id, a->kf_slots * 17) != 0) return 0;
    for (int f = 0; f < BCL_V_FIELDS; f++) {
        if (!a->bm_bytes[f] || !b->bm_bytes[f]) return 0;
        if (a->bm_lens[f] != b->bm_lens[f]) return 0;
        if (memcmp(a->bm_bytes[f], b->bm_bytes[f], a->bm_lens[f]) != 0) return 0;
    }
    return 1;
}

/* kf header (SlotcaskKfHeader): magic u32, version u32, total u64, deleted u64 */
static uint64_t bcl_v_kf_deleted(const unsigned char *hdr) {
    uint64_t v;
    memcpy(&v, hdr + 16, 8);
    return v;
}
static uint64_t bcl_v_kf_total(const unsigned char *hdr) {
    uint64_t v;
    memcpy(&v, hdr + 8, 8);
    return v;
}

/* Verify every populated shard carries tombstones; the target shard must
 * hold more than 16 VALID bitmap files (header-describable). Returns the
 * first shard with deleted>0, or -1/-2 on failure. */
static int bcl_v_find_target_shard(const char *root, int *out_populated) {
    int target = -1;
    *out_populated = 0;
    for (int shard = 0; shard < BCL_V_SPLITS; shard++) {
        char kfp[1024];
        bcl_v_kf_path(kfp, sizeof(kfp), root, shard);
        /* Open before stat-ing (CodeQL TOCTOU), same rationale as
           bcl_v_snapshot. */
        int fd = open(kfp, O_RDONLY);
        if (fd < 0) continue;
        struct stat st;
        if (fstat(fd, &st) != 0 || st.st_size < 24) { close(fd); continue; }
        unsigned char hdr[24];
        if (pread(fd, hdr, 24, 0) != 24) { close(fd); continue; }
        close(fd);
        if (bcl_v_kf_total(hdr) == 0) continue;
        (*out_populated)++;
        if (bcl_v_kf_deleted(hdr) == 0) return -2;   /* populated without tombstones */
        if (target < 0) target = shard;
    }
    if (target >= 0) {
        int valid = 0;
        for (int f = 0; f < BCL_V_FIELDS; f++) {
            char bp[1024];
            bcl_v_bm_path(bp, sizeof(bp), root, f, target);
            if (!bm_file_is_corrupt(bp)) valid++;
        }
        if (valid <= 16) return -3;   /* target lacks >16 valid bitmap files */
    }
    return target;
}

/* ─────────────── log checkpoints ─────────────── */

typedef struct {
    char names[16][64];
    off_t sizes[16];
    ino_t inodes[16];
    int n;
} BclLogMark;

static void bcl_v_log_mark(const char *base, BclLogMark *m) {
    m->n = 0;
    char dir[600];
    snprintf(dir, sizeof(dir), "%s/logs", base);
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *e;
    while (m->n < 16 && !bcl_budget_expired() && (e = readdir(d)) != NULL) {
        size_t len = strlen(e->d_name);
        if (len < 5 || len >= 64 || strcmp(e->d_name + len - 4, ".log") != 0) continue;
        struct stat st;
        char p[700];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        if (stat(p, &st) != 0) continue;
        snprintf(m->names[m->n], sizeof(m->names[m->n]), "%s", e->d_name);
        m->sizes[m->n] = st.st_size;
        m->inodes[m->n] = st.st_ino;
        m->n++;
    }
    closedir(d);
}

/* Scan finite snapshots of the appended regions: files created/rotated after
 * the mark start at offset 0; the saved offset applies only to the same
 * inode whose size has not shrunk. Returns 1 found, 0 not found, -1 on
 * allocation/read failure. */
static int bcl_v_log_grep_since(const char *base, const BclLogMark *m,
                                const char *needle, int64_t deadline) {
    char dir[600];
    snprintf(dir, sizeof(dir), "%s/logs", base);
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *e;
    int result = 0;
    while (result == 0 && bcl_monotonic_ms() < deadline &&
           !bcl_budget_expired() && (e = readdir(d)) != NULL) {
        size_t len = strlen(e->d_name);
        if (len < 5 || strcmp(e->d_name + len - 4, ".log") != 0) continue;
        char p[700];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        /* Open before stat-ing (CodeQL TOCTOU): st then describes the file
           actually read, so the saved inode/size mark can never validate a
           path that has since been replaced. */
        int lfd = open(p, O_RDONLY);
        if (lfd < 0) continue;
        struct stat st;
        if (fstat(lfd, &st) != 0 || !S_ISREG(st.st_mode)) { close(lfd); continue; }
        off_t start = 0;
        for (int i = 0; i < m->n; i++) {
            if (strcmp(m->names[i], e->d_name) == 0) {
                if (m->inodes[i] == st.st_ino && st.st_size >= m->sizes[i])
                    start = m->sizes[i];
                break;   /* rotated/truncated file: rescan from 0 */
            }
        }
        FILE *f = fdopen(lfd, "r");
        if (!f) { close(lfd); continue; }
        size_t cap = 65536, got = 0;
        char *buf = malloc(cap);
        int failure = 0;
        if (!buf || fseek(f, start, SEEK_SET) != 0) failure = 1;
        size_t nread = 0;
        while (!failure && bcl_monotonic_ms() < deadline && !bcl_budget_expired() &&
               (nread = fread(buf + got, 1, cap - got - 1, f)) > 0) {
            got += nread;
            if (cap - got - 1 == 0) {
                if (cap >= 8u << 20) break;   /* finite snapshot cap */
                size_t ncap = cap * 2;
                char *nb = realloc(buf, ncap);
                if (!nb) failure = 1;
                else { buf = nb; cap = ncap; }
            }
        }
        if (!failure && ferror(f)) failure = 1;
        fclose(f);   /* single close on every path */
        if (failure) { free(buf); result = -1; break; }
        buf[got] = '\0';
        if (strstr(buf, needle)) result = 1;
        free(buf);
    }
    closedir(d);
    return result;
}

/* Monotonic-bounded poll (bounded further by the work budget at call sites). */
static int bcl_v_log_poll(const char *base, const BclLogMark *m,
                          const char *needle, int timeout_ms) {
    if (timeout_ms <= 0 || bcl_budget_expired()) return 0;
    int64_t deadline = bcl_monotonic_ms() + timeout_ms;
    if (bcl_budget_anchor && bcl_budget_anchor + bcl_budget_total_ms < deadline)
        deadline = bcl_budget_anchor + bcl_budget_total_ms;
    while (bcl_monotonic_ms() < deadline) {
        int r = bcl_v_log_grep_since(base, m, needle, deadline);
        if (r != 0) return r;
        int64_t left = deadline - bcl_monotonic_ms();
        if (left <= 0) break;
        bcl_sleep_ms(left < 100 ? (int)left : 100);
    }
    return 0;
}

/* ───────────────────── case 4 runner ───────────────────── */

/* Capture the restart daemon's stderr into a fixture-tree file: the seam
 * diagnostic is emitted from bm_cache_init, which the daemon reaches through
 * shard_db_open_internal BEFORE cmd_server's log_init starts the log writer,
 * so pre-startup LOG_WARN lines land on the daemon's stderr (inherited by
 * start_at spawns), not in $LOG_DIR. */
static int bcl_v_daemon_stderr_capture(const char *path) {
    fflush(stderr);
    int saved = dup(2);
    if (saved < 0) return -1;
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { close(saved); return -1; }
    /* dup2 returns the NEW descriptor (2) on success — only < 0 is failure. */
    if (dup2(fd, 2) < 0) { close(fd); close(saved); return -1; }
    close(fd);
    return saved;
}

static void bcl_v_daemon_stderr_restore(int saved_fd) {
    if (saved_fd < 0) return;
    fflush(stderr);
    dup2(saved_fd, 2);
    close(saved_fd);
}

static int bcl_v_target_shard;   /* identified before the vacuum trigger */
static int bcl_v_env_append(const char *base, const char *line);
static int bcl_v_env_remove(const char *base, const char *prefix);

/* Never enter another normal stage inside the cleanup reserve. */
#define BCL_BUDGET_GATE(label) \
    do { \
        if (bcl_budget_expired()) { \
            ASSERT_TRUE(0, "work budget expired before " label); \
            goto cleanup; \
        } \
    } while (0)

static int test_bitmap_cache_selfhold_vacuum_run(void) {
    int rc = 1;
    bcl_v_base[0] = '\0';
    bcl_v_root[0] = '\0';
    memset(&bcl_v_env, 0, sizeof(bcl_v_env));
    bcl_v_env.daemon_pid = -1;
    BclSnapshot snap1;
    memset(&snap1, 0, sizeof(snap1));
    int64_t prev_deadline = 0;
    bcl_budget_anchor = 0;

    /* Parent oracle (authoritative; children only see fork copies). */
    memset(&bcl_v_oracle_shared, 0, sizeof(bcl_v_oracle_shared));
    for (int row = 0; row <= BCL_V_ROWS; row++) {
        bcl_v_oracle_shared.live[row] = 1;
        bcl_v_oracle_shared.nn[row] = row;
    }
    bcl_v_oracle_shared.live[BCL_V_ROWS] = 0;   /* k00400 absent until CRUD */
    bcl_v_oracle_shared.expected_deleted = 0;

    /* Total budget starts before the first daemon start; never reset. */
    bcl_budget_anchor = bcl_monotonic_ms();
    bcl_budget_total_ms = BCL_WORK_BUDGET_MS;
    prev_deadline = test_fixture_set_start_deadline_ms(
        bcl_budget_anchor + BCL_WORK_BUDGET_MS);

    if (test_env_start(&bcl_v_env) != 0) {
        ASSERT_TRUE(0, "daemon start (ordinary capacity)");
        goto cleanup;
    }
    snprintf(bcl_v_root, sizeof(bcl_v_root), "%s", bcl_v_env.db_root);
    {
        const char *slash = strrchr(bcl_v_root, '/');
        size_t bl = slash ? (size_t)(slash - bcl_v_root) : 0;
        if (bl == 0 || bl >= sizeof(bcl_v_base)) bl = sizeof(bcl_v_base) - 1;
        memcpy(bcl_v_base, bcl_v_root, bl);
        bcl_v_base[bl] = '\0';
    }

    BCL_BUDGET_GATE("setup");
    /* Phase 1 — setup at ordinary capacity (watched child; daemon owned here). */
    {
        BclChildResult r;
        int ran = bcl_run_child("vacuum-setup", bcl_v_setup_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0;
        ASSERT_TRUE(ok, "setup: seed+delete+oracles at ordinary capacity");
        if (!ok) goto cleanup;
    }
    /* Adopt the verified setup into the parent oracle: rows multiple of four
     * are absent, 100 tombstones, k00400 still absent. */
    for (int row = 0; row < BCL_V_ROWS; row += 4)
        bcl_v_oracle_shared.live[row] = 0;
    bcl_v_oracle_shared.live[BCL_V_ROWS] = 0;
    bcl_v_oracle_shared.expected_deleted = 100;

    BCL_BUDGET_GATE("tiny restart");
    /* Stop cleanly; preserve root/port; append the TEST_BUILD seam line. */
    test_env_stop_keep(&bcl_v_env);   /* reaps the daemon; root preserved */
    ASSERT_EQ_INT(bcl_v_env_append(bcl_v_base, "BM_TEST_CACHE_CAP=16"), 0,
                  "append BM_TEST_CACHE_CAP=16 to the fixture db.env");
    /* Capture the restart daemon's stderr BEFORE the restart: the whole
     * truncated file is the startup checkpoint, so cap acceptance cannot be
     * missed by rotation or a mark taken too late. */
    char cap_path[700];
    snprintf(cap_path, sizeof(cap_path), "%s/daemon-restart.log", bcl_v_base);
    int saved_err = bcl_v_daemon_stderr_capture(cap_path);
    if (test_env_start_at(&bcl_v_env, bcl_v_root, bcl_v_env.port) != 0) {
        bcl_v_daemon_stderr_restore(saved_err);
        ASSERT_TRUE(0, "restart with BM_TEST_CACHE_CAP=16");
        bcl_v_env_remove(bcl_v_base, "BM_TEST_CACHE_CAP=");
        goto cleanup;
    }
    bcl_v_daemon_stderr_restore(saved_err);

    /* Seam acceptance from the captured startup window; failure is an
     * invalid setup, not the expected red — go to cleanup before any
     * vacuum trigger. */
    {
        int poll_ms = 5000;
        if (bcl_budget_anchor && bcl_work_remaining_ms() < poll_ms)
            poll_ms = (int)bcl_work_remaining_ms();
        int64_t deadline = bcl_monotonic_ms() + poll_ms;
        int seam_ok = 0;
        while (bcl_monotonic_ms() < deadline) {
            unsigned char *txt = NULL;
            size_t len = 0;
            if (bcl_v_read_file(cap_path, &txt, &len) == 0) {
                if (strstr((const char *)txt,
                           "BM_TEST_CACHE_CAP=16: effective cap=16 hash slots=32 "
                           "eviction threshold=16") != NULL)
                    seam_ok = 1;
                free(txt);
            } else {
                seam_ok = -1;
            }
            if (seam_ok != 0) break;
            bcl_sleep_ms(100);
        }
        if (seam_ok != 1) {
            /* Failure evidence: dump the captured startup output. */
            bcl_dump_failure_evidence(bcl_v_base, cap_path);
        }
        ASSERT_EQ_INT(seam_ok, 1,
                      "startup diagnostic shows cap16/slots32/threshold16");
        if (seam_ok != 1) goto cleanup;
    }

    /* Arm the acquisition-failure diagnostic (bmlock-log observability). */
    ASSERT_EQ_INT(test_env_ctl_set_bmlock_log(&bcl_v_env, 1), 0,
                  "arm BMLOCK acquisition diagnostic");

    BCL_BUDGET_GATE("target identification");
    /* Identify the vacuum target shard; snapshot before triggering. */
    {
        int populated = 0;
        int target = bcl_v_find_target_shard(bcl_v_root, &populated);
        ASSERT_TRUE(target >= 0, "target shard: populated, tombstones, >16 valid bitmaps");
        if (target < 0) goto cleanup;
        bcl_v_target_shard = target;
        if (bcl_v_snapshot(bcl_v_root, target, &snap1) != 0) {
            ASSERT_TRUE(0, "pre-vacuum snapshot (target shard)");
            goto cleanup;
        }
        TAP_DIAG("# setup complete; vacuum target shard %d (populated=%d)\n",
                 target, populated);
    }

    char acquire_needle[1400], prepare_needle[1400], expected_path[1024];
    bcl_v_bm_path(expected_path, sizeof(expected_path), bcl_v_root, 0, bcl_v_target_shard);
    int acq_len = snprintf(acquire_needle, sizeof(acquire_needle),
                          "BMLOCK-ACQUIRE-FAIL site=drop-wait error=%d path=%s",
                          EDEADLK, expected_path);
    bcl_v_kf_path(expected_path, sizeof(expected_path), bcl_v_root, bcl_v_target_shard);
    int prep_len = snprintf(prepare_needle, sizeof(prepare_needle),
                           "KF_BMFIX_PREPARE_FAILED path=%s", expected_path);
    if (acq_len < 0 || (size_t)acq_len >= sizeof(acquire_needle) ||
        prep_len < 0 || (size_t)prep_len >= sizeof(prepare_needle)) {
        ASSERT_TRUE(0, "diagnostic path formatting"); goto cleanup;
    }
    /* Phase 2 — light vacuum on the tiny-capacity daemon. */
    {
        BCL_BUDGET_GATE("vacuum trigger");
        BclLogMark mark2;
        bcl_v_log_mark(bcl_v_base, &mark2);   /* immediately before the trigger */
        BclChildResult vr;
        int vran = bcl_run_child("vacuum-request", bcl_v_vacuum_child, &vr);
        int vacuum_failed = 0;
        if (vran != 1 || vr.timed_out || vr.exit_code != 0) {
            /* Base red: the request failed to return within its bound. The
             * red is only valid AT the trigger — require the acquisition-
             * failure diagnostic (drop-wait + EDEADLK) in the marked window. */
            int poll_ms = 5000;
            if (bcl_budget_anchor && bcl_work_remaining_ms() < poll_ms)
                poll_ms = (int)bcl_work_remaining_ms();
            int diag_ok = bcl_v_log_poll(bcl_v_base, &mark2, acquire_needle, poll_ms);
            ASSERT_EQ_INT(diag_ok, 1,
                          "red is at the trigger: drop-wait EDEADLK acquisition reached");
            vacuum_failed = 1;
            ASSERT_TRUE(0, "light vacuum refused promptly with keyfile compaction failed");
        } else {
            /* Exact refusal, validated in the child by strict parsing. */
            ASSERT_TRUE(strstr(vr.stages, "VACUUM-REFUSED-EXACT") != NULL,
                        "vacuum response is exactly the keyfile compaction error");
            /* Correlation: the acquisition-failure diagnostic names the site
             * and errno (EDEADLK formatted from the constant, not hardcoded);
             * KF_BMFIX_PREPARE_FAILED names the abandoned rebuild. */
            int poll_ms = 5000;
            if (bcl_budget_anchor && bcl_work_remaining_ms() < poll_ms)
                poll_ms = (int)bcl_work_remaining_ms();
            int diag_ok = bcl_v_log_poll(bcl_v_base, &mark2, acquire_needle, poll_ms);
            ASSERT_EQ_INT(diag_ok, 1,
                          "drop-wait acquisition diagnostic logged (EDEADLK)");
            int prep_ok = bcl_v_log_poll(bcl_v_base, &mark2, prepare_needle, poll_ms);
            ASSERT_EQ_INT(prep_ok, 1, "KF_BMFIX_PREPARE_FAILED logged (resplit abandoned)");
            if (strstr(vr.stages, "VACUUM-REFUSED-EXACT") == NULL ||
                diag_ok != 1 || prep_ok != 1) vacuum_failed = 1;
        }
        if (vacuum_failed) goto cleanup;
    }

    BCL_BUDGET_GATE("post-refusal checks");
    /* Phase 3 — after refusal: nothing published, everything intact. */
    {
        char obj_dir_chk[PATH_MAX];
        snprintf(obj_dir_chk, sizeof(obj_dir_chk), "%s/%s/%s",
                 bcl_v_root, BCL_V_DIR, BCL_V_OBJECT);
        ASSERT_EQ_INT(bcl_indexes_clean_of_txn_artifacts(obj_dir_chk), 0,
                      "no bmfix markers or remap temporaries after refusal");
    }
    {
        BclSnapshot snap2;
        int snapshot_rc = bcl_v_snapshot(bcl_v_root, bcl_v_target_shard, &snap2);
        ASSERT_EQ_INT(snapshot_rc, 0, "post-refusal snapshot (target shard)");
        if (snapshot_rc != 0) { bcl_v_snapshot_free(&snap2); goto cleanup; }
        ASSERT_TRUE(bcl_v_snapshots_equal(&snap1, &snap2),
                    "keyfile identity + bitmap bytes unchanged after refusal");
        bcl_v_snapshot_free(&snap2);
    }
    {
        BclChildResult r;
        int ran = bcl_run_child("vacuum-oracle", bcl_v_oracle_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0;
        ASSERT_TRUE(ok, "post-refusal oracles (live300/deleted100, per-field, per-key, exists)");
        if (!ok) goto cleanup;
    }

    BCL_BUDGET_GATE("ordinary restart");
    /* Phase 4 — ordinary capacity restart, successful vacuum, CRUD. */
    test_env_stop_keep(&bcl_v_env);   /* reaps the daemon; root preserved */
    ASSERT_EQ_INT(bcl_v_env_remove(bcl_v_base, "BM_TEST_CACHE_CAP="), 0,
                  "remove the test-only capacity line");
    snprintf(cap_path, sizeof(cap_path), "%s/daemon-restart2.log", bcl_v_base);
    saved_err = bcl_v_daemon_stderr_capture(cap_path);
    if (test_env_start_at(&bcl_v_env, bcl_v_root, bcl_v_env.port) != 0) {
        bcl_v_daemon_stderr_restore(saved_err);
        ASSERT_TRUE(0, "restart at ordinary capacity");
        goto cleanup;
    }
    bcl_v_daemon_stderr_restore(saved_err);
    {
        unsigned char *txt = NULL;
        size_t len = 0;
        int seam_absent = bcl_v_read_file(cap_path, &txt, &len) == 0 &&
                          strstr((const char *)txt, "BM_TEST_CACHE_CAP=") == NULL;
        free(txt);
        ASSERT_TRUE(seam_absent,
                    "no seam diagnostic after the test setting was removed");
    }
    BCL_BUDGET_GATE("pre-success oracles");
    {
        BclChildResult r;
        int ran = bcl_run_child("pre-vacuum-oracle", bcl_v_oracle_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0;
        ASSERT_TRUE(ok, "pre-success oracles at ordinary capacity (live300/deleted100)");
        if (!ok) goto cleanup;
    }
    BCL_BUDGET_GATE("successful vacuum");
    {
        BclChildResult r;
        int ran = bcl_run_child("vacuum-success", bcl_v_vacuum_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0 &&
                 strstr(r.stages, "VACUUM-OK-EXACT") != NULL;
        ASSERT_TRUE(ok, "light vacuum succeeds at ordinary capacity");
        if (!ok) goto cleanup;
    }
    /* Same row membership; physical tombstones are now gone. */
    bcl_v_oracle_shared.expected_deleted = 0;
    BCL_BUDGET_GATE("post-vacuum oracles");
    {
        BclChildResult r;
        int ran = bcl_run_child("post-vacuum-oracle", bcl_v_oracle_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0;
        ASSERT_TRUE(ok, "post-vacuum oracles (live300/deleted0)");
        if (!ok) goto cleanup;
    }
    BCL_BUDGET_GATE("CRUD steps");
    {
        BclChildResult r;
        int ran = bcl_run_child("crud", bcl_v_crud_child, &r);
        int ok = ran == 1 && !r.timed_out && r.exit_code == 0;
        ASSERT_TRUE(ok, "insert/update/delete steps with exact responses + oracles");
        if (!ok) goto cleanup;
    }
    /* Adopt the verified CRUD steps into the parent oracle. */
    bcl_v_oracle_shared.live[BCL_V_ROWS] = 1;
    bcl_v_oracle_shared.nn[1] = 1001;
    bcl_v_oracle_shared.live[2] = 0;
    bcl_v_oracle_shared.expected_deleted = 1;
    rc = 0;

cleanup:
    /* Startup can fail after creating the directory but before publishing
       our normal cleanup path. Recover that path from the fixture itself. */
    if (!bcl_v_base[0] && bcl_v_env.db_root[0]) {
        const char *slash = strrchr(bcl_v_env.db_root, '/');
        if (slash && slash != bcl_v_env.db_root) {
            size_t n = (size_t)(slash - bcl_v_env.db_root);
            if (n < sizeof(bcl_v_base)) {
                memcpy(bcl_v_base, bcl_v_env.db_root, n);
                bcl_v_base[n] = '\0';
            }
        }
    }
    /* The parent always owns and reaps the daemon, on every exit path, and
     * removes this case's own fixture tree (test_env_stop also rm -rf's the
     * base when it stops a live daemon; the explicit removal covers the
     * stop_keep paths). The 5 s TERM→KILL teardown bound stays inside the
     * 30 s cleanup reserve. */
    if (bcl_v_env.daemon_pid > 0)
        test_env_stop(&bcl_v_env);
    if (bcl_v_base[0]) {
        ASSERT_EQ_INT(bcl_rmrf(bcl_v_base), 0, "vacuum fixture cleanup (own tmp tree)");
    }
    bcl_v_snapshot_free(&snap1);
    test_fixture_set_start_deadline_ms(prev_deadline);
    bcl_budget_anchor = 0;   /* scope the budget state to this integration */
    return rc == 0 ? (t_ctx->failed > 0 ? 1 : 0) : 1;
}

/* ───────────────────── case 5: fixture watchdog proofs ───────────────────── */

/* Parking stub: listens on an ephemeral loopback port and accepts incoming
 * connections, never responding. Runs as its own forked child so the caller
 * can kill/reap the exact PID. _exit(0) after SIGTERM (default). */
static pid_t bcl_stub_spawn(int *out_port) {
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) return -1;
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = 0,
                                .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
    if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) != 0) { close(ls); return -1; }
    socklen_t len = sizeof(addr);
    if (getsockname(ls, (struct sockaddr *)&addr, &len) != 0) { close(ls); return -1; }
    if (listen(ls, 8) != 0) { close(ls); return -1; }
    *out_port = ntohs(addr.sin_port);
    pid_t pid = fork();
    if (pid < 0) { close(ls); return -1; }
    if (pid == 0) {
        alarm(15);   /* A broken owner watchdog still cannot park this stub forever. */
        for (;;) {
            int fd = accept(ls, NULL, NULL);
            if (fd < 0 && errno == EINTR) continue;
            if (fd < 0) _exit(1);
        }   /* accept and park every connection */
        _exit(0);
    }
    close(ls);
    return pid;
}

/* (a)+(b): helper semantics, expired-deadline refusal, and the readiness-
 * stall proof against a parking stub — all inside one watched child. */
static int bcl_fw_stall_child(void) {
    /* Helper semantics. */
    int64_t prev = test_fixture_set_start_deadline_ms(123456);
    BCL_CHECK(prev == 0, 10, "fresh thread must report no prior deadline");
    int64_t prev2 = test_fixture_set_start_deadline_ms(234567);
    BCL_CHECK(prev2 == 123456, 11, "setter must return the previous deadline");
    errno = 0;
    int64_t bad = test_fixture_set_start_deadline_ms(-1);
    BCL_CHECK(bad == -1 && errno == EINVAL, 12, "negative input must fail with EINVAL");
    BCL_CHECK(test_fixture_set_start_deadline_ms(0) == 234567, 13,
              "0 must disable and return the previous deadline");

    int port = -1;
    pid_t stub = bcl_stub_spawn(&port);
    if (stub <= 0 || port <= 0) { bcl_stage("stub spawn failed"); return 14; }
    int rc = 0;
    bcl_stage("stub listening; expired-deadline call");
    int64_t t0 = bcl_monotonic_ms();
    errno = 0;
    int rc_expired = test_fixture_wait_ready_until(port, stub, t0 - 1000);
    if (rc_expired != -1 || errno != ETIMEDOUT) {
        bcl_stage("FAIL expired deadline must return -1/ETIMEDOUT without a probe");
        rc = 15;
        goto reap;
    }
    if (bcl_monotonic_ms() - t0 >= 1000) {
        bcl_stage("FAIL expired-deadline call must return immediately (no probe wait)");
        rc = 16;
        goto reap;
    }

    bcl_stage("stub ready-stall under a shortened absolute deadline");
    t0 = bcl_monotonic_ms();
    errno = 0;
    int rc_stall = test_fixture_wait_ready_until(port, stub, bcl_monotonic_ms() + 2000);
    int64_t waited = bcl_monotonic_ms() - t0;
    if (rc_stall != -1 || errno != ETIMEDOUT) {
        bcl_stage("FAIL parked readiness probe must time out against the deadline");
        rc = 17;
        goto reap;
    }
    if (waited < 1900 || waited >= 6000) {
        bcl_stage("FAIL stall wait must be bounded by the absolute deadline, not 30 s");
        rc = 18;
        goto reap;
    }
    bcl_stage("reaping stub");
    if (kill(stub, SIGKILL) != 0) { bcl_stage("FAIL stub kill"); rc = 19; goto reap; }
    {
        int st = 0;
        if (waitpid(stub, &st, 0) != stub) { bcl_stage("FAIL stub reap"); rc = 20; goto reap; }
    }
    bcl_stage("stall proof complete");
    stub = -1;
reap:
    /* The stub must never outlive this child, on any path. */
    if (stub > 0) {
        kill(stub, SIGKILL);
        waitpid(stub, NULL, 0);
    }
    return rc;
}

/* (c): shortened-budget self-check — a deliberately parked request reaches
 * full cleanup (request child, daemon and stub all killed/reaped) and the
 * driver returns before any larger external timeout. */
static int bcl_fw_stub_port;   /* set by the mini-driver before the fork */

static int bcl_fw_parked_request_child(void) {
    /* Runs INSIDE the mini-driver: connects to the parking stub and issues a
     * request that can never complete. Exit status is irrelevant; reaping is
     * what matters. */
    TestClientCfg cfg = { .port = bcl_fw_stub_port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(7);
    char *resp = NULL;
    (void)tc_request(tc, "{\"mode\":\"db-dirs\"}", &resp);
    _exit(8);   /* only reached if the stub answered (must not happen) */
}

static int bcl_fw_budget_child(void) {
    int rc = 1;
    TestEnv env;
    memset(&env, 0, sizeof(env));
    pid_t stub = -1;
    int stub_port = -1;

    /* Shortened work budget: 4 s of work + the standard cleanup reserve. */
    int64_t anchor = bcl_monotonic_ms();
    int64_t prev = test_fixture_set_start_deadline_ms(anchor + 4000);
    bcl_budget_anchor = anchor;      /* child-local copy of the budget state */
    bcl_budget_total_ms = 4000;      /* shortened budget length for this scope */

    if (test_env_start(&env) != 0) { bcl_stage("daemon start failed"); goto out; }
    bcl_stage("daemon ready; spawning parked request child");

    stub = bcl_stub_spawn(&stub_port);
    if (stub <= 0) { bcl_stage("stub spawn failed"); goto out; }
    bcl_fw_stub_port = stub_port;

    {
        BclChildResult r;
        int ran = bcl_run_child("parked-request", bcl_fw_parked_request_child, &r);
        if (ran != 1 || !r.timed_out) { bcl_stage("parked request was not bounded"); goto out; }
    }
    bcl_stage("budget expired; cleanup reached");

    /* The request child was reaped by the runner; the stub is killed and
     * reaped here and must be gone. The daemon is stopped by the shared
     * cleanup below and verified gone afterwards. */
    kill(stub, SIGKILL);
    pid_t reaped;
    do { reaped = waitpid(stub, NULL, 0); } while (reaped < 0 && errno == EINTR);
    if (reaped != stub) { bcl_stage("stub reap failed"); goto out; }
    stub = -1;
    rc = 0;

out:
    if (env.daemon_pid > 0) {
        test_env_stop(&env);
        if (env.daemon_pid > 0) { bcl_stage("daemon reap failed"); rc = 1; }
    }
    if (stub > 0) {
        kill(stub, SIGKILL);
        pid_t reaped;
        do { reaped = waitpid(stub, NULL, 0); } while (reaped < 0 && errno == EINTR);
        if (reaped == stub) stub = -1;
    }
    if (env.db_root[0]) {
        const char *slash = strrchr(env.db_root, '/');
        if (slash && slash != env.db_root) {
            char base_dir[PATH_MAX];
            snprintf(base_dir, sizeof(base_dir), "%.*s",
                     (int)(slash - env.db_root), env.db_root);
            (void)bcl_rmrf(base_dir);
        }
    }
    test_fixture_set_start_deadline_ms(prev);
    bcl_budget_anchor = 0;
    return rc;
}

/* (d): no-override legacy path still starts daemons (probe-free). */
static int bcl_fw_legacy_child(void) {
    BCL_CHECK(test_fixture_set_start_deadline_ms(0) == 0, 10, "no prior override expected");
    TestEnv env;
    memset(&env, 0, sizeof(env));
    int rc = test_env_start(&env);
    if (rc == 0) bcl_stage("legacy startup ok");
    else bcl_stage("legacy startup failed");
    if (env.daemon_pid > 0) test_env_stop(&env);
    if (env.db_root[0]) {
        const char *slash = strrchr(env.db_root, '/');
        if (slash && slash != env.db_root) {
            char base_dir[PATH_MAX];
            snprintf(base_dir, sizeof(base_dir), "%.*s",
                     (int)(slash - env.db_root), env.db_root);
            if (bcl_rmrf(base_dir) != 0) return 12;
        }
    }
    return rc == 0 ? 0 : 11;
}

typedef struct { const char *label; BclChildFn fn; } BclFwScenario;
static const BclFwScenario bcl_fw_scenarios[] = {
    { "deadline-helper+stall",  bcl_fw_stall_child },
    { "shortened-budget-cleanup", bcl_fw_budget_child },
    { "legacy-no-override",     bcl_fw_legacy_child },
};
#define BCL_FW_N ((int)(sizeof(bcl_fw_scenarios) / sizeof(bcl_fw_scenarios[0])))

static int test_bitmap_cache_fixture_watchdog_run(void) {
    for (int i = 0; i < BCL_FW_N; i++) {
        char label[96], desc[128];
        snprintf(label, sizeof(label), "fixture-watchdog[%s]", bcl_fw_scenarios[i].label);
        snprintf(desc, sizeof(desc), "fixture-watchdog scenario (%s) passed all checks",
                 bcl_fw_scenarios[i].label);
        BclChildResult r;
        int ran = bcl_run_child(label, bcl_fw_scenarios[i].fn, &r);
        ASSERT_EQ_INT(ran, 1, "fixture-watchdog child ran (outer watchdog)");
        ASSERT_EQ_INT(r.timed_out, 0, "fixture-watchdog child finished within the bound");
        ASSERT_EQ_INT(r.exit_code, 0, desc);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ───────────────────── db.env helpers (shared by case 4) ───────────────────── */

static int bcl_v_env_append(const char *base, const char *line) {
    char path[600];
    snprintf(path, sizeof(path), "%s/db.env", base);
    FILE *f = fopen(path, "a");
    if (!f) return -1;
    fprintf(f, "export %s\n", line);
    return fclose(f) == 0 ? 0 : -1;
}

static int bcl_v_env_remove(const char *base, const char *prefix) {
    char path[600], tmp[640];
    snprintf(path, sizeof(path), "%s/db.env", base);
    snprintf(tmp, sizeof(tmp), "%s/db.env.bcltmp", base);
    FILE *in = fopen(path, "r");
    if (!in) return -1;
    FILE *out = fopen(tmp, "w");
    if (!out) { fclose(in); return -1; }
    char line[1024];
    while (fgets(line, sizeof(line), in)) {
        const char *p = line;
        while (*p == ' ') p++;
        if (strncmp(p, "export ", 7) == 0) p += 7;
        if (strncmp(p, prefix, strlen(prefix)) == 0) continue;
        fputs(line, out);
    }
    fclose(in);
    if (fclose(out) != 0) { unlink(tmp); return -1; }
    if (rename(tmp, path) != 0) { unlink(tmp); return -1; }
    return 0;
}

TEST_REGISTER("test-bitmap-cache-selfhold", test_bitmap_cache_selfhold_run)
TEST_REGISTER("test-bitmap-cache-lock-errors", test_bitmap_cache_lock_errors_run)
TEST_REGISTER("test-bitmap-cache-contention", test_bitmap_cache_contention_run)
TEST_REGISTER("test-bitmap-cache-selfhold-vacuum", test_bitmap_cache_selfhold_vacuum_run)
TEST_REGISTER("test-bitmap-cache-fixture-watchdog", test_bitmap_cache_fixture_watchdog_run)
static int bcl_harness_cleanup_child(void) {
    BclBuf record;
    bcl_buf_init(&record);
    bcl_v_appendf(&record, "{\"n\":0");
    for (int f = 0; f < BCL_V_FIELDS; f++)
        bcl_v_appendf(&record, ",\"f%d\":\"%d\"", f, f % 4);
    bcl_v_appendf(&record, "}");
    BCL_CHECK(!record.failed && bcl_v_validate_row(record.buf, 0, 0) == 0,
              4, "complete GET row must pass");
    record.buf[--record.len] = '\0';
    bcl_v_appendf(&record, ",\"error\":\"boom\"}");
    BCL_CHECK(!record.failed && bcl_v_validate_row(record.buf, 0, 0) == -1,
              5, "GET row containing an error must fail");
    bcl_buf_free(&record);
    for (int stage = 1; stage <= 2; stage++) {
        int rc = bcl_mx_open_fail_expect(BM_TEST_LOCK_DROP_WAIT, EDEADLK,
                                       -1, 0, EDEADLK, stage);
        BCL_CHECK(rc == (stage == 1 ? 29 : 12), 2, "forced failure not reached");
        /* The helper has joined its holder before its stack expires. Every
           cache entry must now be exclusively acquirable by this thread. */
        for (int i = 0; i < g_bm_cache_slots; i++) {
            if (!g_bm_cache[i].used) continue;
            BCL_CHECK(pthread_rwlock_trywrlock(&g_bm_cache[i].rwlock) == 0, 3,
                      "forced failure retained a holder lock");
            pthread_rwlock_unlock(&g_bm_cache[i].rwlock);
        }
        bcl_child_cache_teardown();
    }
    return 0;
}

static int bcl_parent_death_report_fd = -1;

static int bcl_harness_parked_owner(void) {
    int port = -1;
    pid_t stub = bcl_stub_spawn(&port);
    if (stub <= 0) { bcl_stage(strerror(errno)); return 1; }
    char line[96];
    snprintf(line, sizeof(line), "DESCENDANT pid=%ld port=%d", (long)stub, port);
    bcl_stage(line);
    if (bcl_parent_death_report_fd >= 0) {
        ssize_t n = write(bcl_parent_death_report_fd, &port, sizeof(port));
        close(bcl_parent_death_report_fd);
        if (n != sizeof(port)) return 2;
    }
    alarm(20);
    for (;;) pause();
    return 0;
}

static int bcl_harness_owner_death(void) {
    int fd[2];
    if (pipe(fd) != 0) return -1;
    if (fcntl(fd[0], F_SETFL, O_NONBLOCK) != 0) {
        close(fd[0]); close(fd[1]); return -1;
    }
    pid_t owner = fork();
    if (owner < 0) { close(fd[0]); close(fd[1]); return -1; }
    if (owner == 0) {
        close(fd[0]);
        bcl_parent_death_report_fd = fd[1];
        if (setpgid(0, 0) != 0) _exit(125);
        BclChildResult result;
        (void)bcl_run_child_bounded("enclosed-owner", bcl_harness_parked_owner, &result, 30000);
        _exit(125);
    }
    close(fd[1]);
    int ready = setpgid(owner, owner) == 0 || getpgid(owner) == owner;
    int port = -1;
    size_t got = 0;
    int64_t deadline = bcl_monotonic_ms() + 5000;
    while (ready && got < sizeof(port) && bcl_monotonic_ms() < deadline) {
        struct pollfd p = {.fd = fd[0], .events = POLLIN};
        if (poll(&p, 1, 50) < 0 && errno != EINTR) break;
        ssize_t n = read(fd[0], (char *)&port + got, sizeof(port) - got);
        if (n > 0) got += (size_t)n;
        else if (n == 0 || (errno != EAGAIN && errno != EINTR)) break;
    }
    close(fd[0]);
    /* This is our unreaped direct child, so its PGID cannot be recycled. */
    if (ready) kill(-owner, SIGKILL);
    kill(owner, SIGKILL);
    pid_t reaped;
    do { reaped = waitpid(owner, NULL, 0); } while (reaped < 0 && errno == EINTR);
    int refused = 0;
    if (got == sizeof(port) && port > 0 && port <= 65535) {
        deadline = bcl_monotonic_ms() + 2000;
        do {
            TestClientCfg cfg = {.port = port, .connect_timeout_ms = 100};
            TestClient *tc = tc_connect(&cfg);
            if (!tc) { refused = 1; break; }
            tc_close(tc);
            bcl_sleep_ms(10);
        } while (bcl_monotonic_ms() < deadline);
    }
    if (!refused) {
        /* Deliberately broken containment remains a safe negative test:
           parked owner/stub fixtures have 20/15-second self-expiry. */
        bcl_sleep_ms(21000);
    }
    return reaped == owner && ready && refused ? 0 : -1;
}

static int test_bitmap_cache_harness_errors_run(void) {
    int prepared = bcl_mx_prepare();
    ASSERT_EQ_INT(prepared, 0, "harness fixture paths");
    if (prepared != 0) return 1;
    BclChildResult r;
    int ran = bcl_run_child("strict-parser", bcl_v_parser_selfcheck_child, &r);
    ASSERT_TRUE(ran == 1 && !r.timed_out && r.exit_code == 0, "strict response regressions");
    ran = bcl_run_child("holder-failure-cleanup", bcl_harness_cleanup_child, &r);
    ASSERT_TRUE(ran == 1 && !r.timed_out && r.exit_code == 0, "holder failures release and join");
    ran = bcl_run_child_bounded("parked-owner", bcl_harness_parked_owner, &r, 500);
    ASSERT_TRUE(ran == 1 && r.timed_out && r.termsig == SIGKILL, "owner watchdog fires");
    long stub = -1;
    int port = -1;
    const char *p = strstr(r.stages, "DESCENDANT ");
    int identity = p && sscanf(p, "DESCENDANT pid=%ld port=%d", &stub, &port) == 2 &&
                   stub > 0 && port > 0 && port <= 65535;
    ASSERT_TRUE(identity, "parked owner published descendant identity");
    int refused = 0;
    if (identity) {
        int64_t until = bcl_monotonic_ms() + 2000;
        do {
            TestClientCfg cfg = {.port = port, .connect_timeout_ms = 100};
            TestClient *tc = tc_connect(&cfg);
            if (!tc) { refused = 1; break; }
            tc_close(tc);
            bcl_sleep_ms(10);
        } while (bcl_monotonic_ms() < until);
        /* Keep the negative test safe even if containment regresses. */
        if (!refused) {
            int64_t cleanup_deadline = bcl_monotonic_ms() + 16000;
            while (bcl_monotonic_ms() < cleanup_deadline) {
                TestClientCfg cfg = {.port = port, .connect_timeout_ms = 100};
                TestClient *tc = tc_connect(&cfg);
                if (!tc) break;
                tc_close(tc);
                bcl_sleep_ms(50);
            }
        }
        /* No signaling by a PID whose owner has already been reaped. */
    }
    ASSERT_TRUE(refused, "watchdog terminates parked descendant listener");
    ASSERT_EQ_INT(bcl_harness_owner_death(), 0, "enclosing owner death terminates private helpers");
    ASSERT_EQ_INT(bcl_rmrf(bcl_mx_dir), 0, "harness fixture cleanup");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-cache-harness-errors", test_bitmap_cache_harness_errors_run)
