# Fix: auto-reshard/auto-vacuum vs. kfcache_shutdown() use-after-free race

## Goal

Close a real use-after-free/destroy-while-locked SIGSEGV: `auto_reshard_thread`
and `auto_vacuum_thread` are spawned detached and never joined before
`cmd_server`'s shutdown sequence tears down `kfcache` (and the other
slotcask-owned caches). If either thread is still inside a reshard/vacuum
when shutdown reaches `kfcache_shutdown()`, it races the teardown with zero
mutual exclusion at the cache-array-lifecycle level.

## Root cause

`kfcache_invalidate_prefix()` (`src/db/slotcask.c`) deliberately never
acquires `g_kfcache_lock` — only each matching entry's per-entry `rwlock` —
specifically to avoid a lock-order inversion with `kfcache_acquire()`'s
install path (documented in `kfcache_invalidate_prefix`'s own comment).
`kfcache_shutdown()` frees the entire `g_kfcache` array and calls
`pthread_rwlock_destroy()` on every entry while holding only
`g_kfcache_lock` — a mutex `kfcache_invalidate_prefix()` never touches.
There is no synchronization between them at the array-lifecycle level.

In production, `auto_reshard_thread` / `auto_vacuum_thread` are spawned via
`db_thread_create()` then immediately `pthread_detach()`'d in `cmd_server`
(`src/db/server.c`). `cmd_server`'s shutdown sequence joins the connection
worker pool and waits up to 30s for in-flight writes, but never joins these
two background threads before calling `slotcask_shutdown()` →
`kfcache_shutdown()`. If a reshard/vacuum thread is still inside
`kfcache_invalidate_prefix()` — reached via
`auto_reshard_sweep_one → cmd_vacuum → rebuild_object_v2 →
slotcask_registry_invalidate → kfcache_invalidate_prefix` — when shutdown
reaches `kfcache_shutdown()`, the crash occurs: the thread may
dereference/lock a freed/destroyed `KfCacheEntry`.

Real-world trigger: any operator restart (`./shard-db stop` or a supervisor
sending SIGTERM) during the opt-in nightly auto-reshard/auto-vacuum window
while a reshard/vacuum happens to be running.

## Fix

Spawn `auto_reshard_thread` / `auto_vacuum_thread` joinable (drop the
`pthread_detach()` calls) and `pthread_join()` both from `cmd_server`'s
shutdown sequence, before any cache teardown call
(`slotcask_shutdown()`/`kfcache_shutdown()`, `bt_cache_shutdown()`,
`fcache_shutdown()`).

## Behavior change this introduces (must be called out, not silent)

`sweep_all_objects` (`src/db/server.c`) checks `server_running` between
dirs and between objects in its readdir loop, but **not** inside the
per-object callback itself. Once a specific object's reshard/vacuum begins,
it runs to completion regardless of `server_running`. That was already true
today — the difference is that today shutdown proceeds anyway (racing the
in-flight thread against cache teardown); after this fix, shutdown blocks
until that in-flight item finishes. This means `./shard-db stop` can now
take as long as an in-progress heavy reshard/vacuum takes, instead of
returning immediately. This is a deliberate, arguably more-correct
tradeoff (bounded by the fact that a heavy reshard/vacuum already holds the
object's exclusive `objlock` for its full duration, blocking all other
traffic on that object regardless) — not a new source of unbounded risk,
but it is an observable latency change for operators and must be documented
in `docs/concepts/concurrency.md`.

## Explicitly out of scope: the warmup thread

`cmd_server`'s startup-warmup thread (`src/db/server.c`, the
`if (strcmp(g_warmup_mode, "off") != 0)` block, async branch) is also
spawned via `db_thread_create()` + `pthread_detach()`, and `warmup_thread`
does call `kfcache_acquire()` while priming the page cache. In principle
this is the same class of race: if `stop` arrives while warmup is still
running, `kfcache_shutdown()` could race it too. This plan does not join
it, for a different reason than "it's safe by design" — it's a real gap,
but a much narrower one: warmup is a single bounded pass over existing kf
headers/index shards run once at startup (not a recurring, opt-in,
potentially-slow nightly sweep like reshard/vacuum), so the exposure
window is startup-to-warmup-completion, not "any time of day, indefinitely,
if enabled." Given that narrower and different-shaped risk, it's being
tracked separately rather than folded into this fix — flag to the human
whether a follow-up task should be opened for it.

## Global constraints

- **Branch**: fresh branch off `main`.
- **Build**: `SKIP_TESTS=1 ./build.sh`
- **Test**: `./build/bin/shard-db-test run-all --filter auto-reshard` (and
  a full `./build/bin/shard-db-test run-all` before calling this done).
- **Execution mode (this repo's standing exception)**: leave work
  **uncommitted** after execution — Sonnet reviews the raw `git diff`
  before anything is committed. Plan execution is carried out by a model
  outside the Claude family (Gemini/GPT) on the fresh branch — do not spawn
  a Haiku/Claude subagent for execution.
- If a quoted anchor below is not found **exactly** in the target file,
  stop immediately, write `PLAN_NOTES.md` describing the mismatch, and
  halt the entire run — do not guess, reinterpret, or continue to any
  other task, even an unrelated one.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- The new `KFCACHE_TEST_HOLD_MS` env var is **test-only** and must stay
  that way: default `0` (off), a single `if (hold_ms > 0)` gate with no
  other behavioral surface, and **not** documented in
  `docs/getting-started/configuration.md` (that table is the operator-
  facing config reference; this knob has no supported production use).
  Do not add anything beyond what's specified below.

## Background reading (for the executor, no action needed)

- `src/db/slotcask.c`: `kfcache_init`, `kfcache_shutdown`,
  `kfcache_invalidate_prefix`.
- `src/db/query_find.c`: `rebuild_object_v2` (calls
  `slotcask_registry_invalidate` as effectively its first step, which is
  why the injected test hold fires early and reliably).
- `src/db/server.c`: `cmd_server`, `auto_vacuum_thread`,
  `auto_reshard_thread`, `auto_reshard_sweep_one`, `sweep_all_objects`.
- `src/test/cases/test_auto_reshard.c`: existing reference pattern for
  fork+execl daemon spawn, `fabricate_kf_total()`, top-of-hour boundary
  handling, and log-polling (`*-info.log`).
- Log routing (`open_log_for_level` in `src/db/config.c`): level ≤1
  (ERROR) → `YYYY-MM-DD-error.log`; **everything else** (WARN, INFO,
  DEBUG) → `YYYY-MM-DD-info.log`. `log.h`'s comment that mentions a
  separate `-warn.log` is stale — do **not** poll a `-warn.log` file.
  The "AUTO-RESHARD ... starting" line is `LOG_WARN` and still lands in
  `-info.log`; the "... done" line is `LOG_INFO` (same file). Task 2's
  test polls `-info.log`, matching `test_auto_reshard.c`.

## Tasks

### Task 1 — Add the test-only `KFCACHE_TEST_HOLD_MS` config knob (infra, inert at 0)

This task only adds plumbing. It changes no behavior when the var is unset
(default 0), so it carries no regression-test requirement of its own — it
exists to make Task 3's regression test deterministic.

**1a. `src/db/shard_db_internal.h`** — add a new `ShardDb` struct field.

Anchor (quoted exactly):

```c
    int auto_reshard_hour;
    int auto_reshard_throttle_ms;
```

Replace with:

```c
    int auto_reshard_hour;
    int auto_reshard_throttle_ms;
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
```

Then, in the macro block (note: these lines are column-aligned — match
spacing exactly):

Anchor (quoted exactly):

```c
#define g_auto_reshard_throttle_ms  (g_db->auto_reshard_throttle_ms)
#define g_warmup_mode               (g_db->warmup_mode)
```

Replace with:

```c
#define g_auto_reshard_throttle_ms  (g_db->auto_reshard_throttle_ms)
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_mode               (g_db->warmup_mode)
```

No default-value change is needed elsewhere: `ShardDb` is allocated via
`calloc(1, sizeof(ShardDb))` in `shard_db_open_internal`
(`src/db/embedded.c`), so the new field is `0` by construction.

**1b. `src/db/config.c`** — parse the env var.

Anchor (quoted exactly — note 8-space indentation, matching the file):

```c
        } else if (strncmp(p, "AUTO_RESHARD_THROTTLE_MS=", 26) == 0) {
            int n = atoi(p + 26);
            if (n >= 0 && g_db) g_auto_reshard_throttle_ms = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

Replace with:

```c
        } else if (strncmp(p, "AUTO_RESHARD_THROTTLE_MS=", 26) == 0) {
            int n = atoi(p + 26);
            if (n >= 0 && g_db) g_auto_reshard_throttle_ms = n;
        } else if (strncmp(p, "KFCACHE_TEST_HOLD_MS=", 21) == 0) {
            /* Test-only knob (widens kfcache_invalidate_prefix's hold window
               deterministically for the shutdown-race regression test). Not
               a documented production setting — do not add to
               configuration.md. */
            int n = atoi(p + 21);
            if (n >= 0 && g_db) g_kfcache_test_hold_ms = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

**1c. `src/db/slotcask.c`** — apply the hold inside
`kfcache_invalidate_prefix()`.

Anchor (quoted exactly, the full current function):

```c
static void kfcache_invalidate_prefix(const char *prefix) {
    if (!g_kfcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_kfcache_slots; i++) {
        KfCacheEntry *e = &g_kfcache[i];
        if (!__atomic_load_n(&e->used, __ATOMIC_ACQUIRE)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

Replace with:

```c
static void kfcache_invalidate_prefix(const char *prefix) {
    if (!g_kfcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_kfcache_slots; i++) {
        KfCacheEntry *e = &g_kfcache[i];
        if (!__atomic_load_n(&e->used, __ATOMIC_ACQUIRE)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (g_db && g_kfcache_test_hold_ms > 0) {
                /* Test-only hook (KFCACHE_TEST_HOLD_MS): widens this
                   window deterministically for the shutdown-race
                   regression test. 0 in production.
                   Retry on EINTR so the hold duration is reliable
                   even if a signal (other than the blocked SIGTERM/
                   SIGINT on the auto-reshard/auto-vacuum threads —
                   see Task 3f) arrives mid-sleep. */
                struct timespec hold_ts = { g_kfcache_test_hold_ms / 1000,
                                             (long)(g_kfcache_test_hold_ms % 1000) * 1000000L };
                int ret;
                do {
                    ret = nanosleep(&hold_ts, &hold_ts);
                } while (ret != 0 && errno == EINTR);
            }
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
            /* Test-only early exit: unlock this entry, then leave the
               remaining prefix-matched entries alone.  One held entry is
               enough for the shutdown-race regression test; iterating
               the rest would multiply HOLD_MS and blow the test's 10s
               waitpid timeout at high splits.  Production
               (g_kfcache_test_hold_ms=0) never takes this path — all
               matching entries are invalidated in one pass.
               IMPORTANT: unlock before break. A bare `break` would leak
               the per-entry wrlock on the test path. */
            if (g_db && g_kfcache_test_hold_ms > 0) {
                pthread_rwlock_unlock(&e->rwlock);
                break;
            }
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}
```

(`<time.h>` is already included in `slotcask.c` near the other system
headers; do **not** add a second `#include <time.h>`. The `g_db &&`
guard mirrors the pattern used elsewhere in this file before touching
`g_db`-backed globals.)

**Build check for this task**: `SKIP_TESTS=1 ./build.sh` must succeed with
no new warnings. Paste the build output.

### Task 2 — Write the regression test (must FAIL on current code)

Create `src/test/cases/test_auto_reshard_shutdown_race.c`:

```c
/* src/test/cases/test_auto_reshard_shutdown_race.c
 *
 * Regression test for a use-after-free race: kfcache_shutdown() frees the
 * entire kfcache array and destroys every entry's rwlock while holding
 * only g_kfcache_lock. kfcache_invalidate_prefix() (reachable from an
 * in-flight auto-reshard via rebuild_object_v2 -> slotcask_registry_invalidate)
 * never takes that lock by design (see its comment in slotcask.c), so
 * before the fix, a reshard still running when shutdown reached
 * kfcache_shutdown() raced it with zero mutual exclusion.
 *
 * Uses KFCACHE_TEST_HOLD_MS (test-only, default 0/off in production) to
 * deterministically widen kfcache_invalidate_prefix()'s hold window, then
 * sends SIGTERM the instant the reshard's "starting" log line appears.
 * Two independent assertions distinguish joined (fixed) from detached
 * (buggy) shutdown:
 *   1. The daemon must not die by signal (the crash symptom itself).
 *   2. Shutdown must take at least ~HOLD_MS: pre-fix, cmd_server proceeds
 *      to kfcache_shutdown() immediately after SIGTERM regardless of the
 *      in-flight thread, so elapsed time is near-instant; post-fix,
 *      cmd_server blocks on pthread_join() until the held reshard call
 *      returns. This timing assertion is the primary, robust proof: it
 *      fails deterministically pre-fix and passes deterministically
 *      post-fix even on a run where the crash doesn't happen to manifest.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#define HOLD_MS 2000

static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t w = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (w == (ssize_t)sizeof(total)) ? 0 : -1;
}

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Poll the INFO log for the "AUTO-RESHARD ... starting" line.
   Note: LOG_WARN is level 2 which open_log_for_level() in config.c routes
   to the -info.log file (only level 1 / ERROR gets its own -error.log).
   The "starting" line goes to -info.log alongside INFO-level messages. */
static int wait_for_reshard_start(const char *db_root, const char *date_str,
                                   int timeout_s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/logs/%s-info.log", db_root, date_str);
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[1024];
            while (fgets(line, sizeof(line), f)) {
                if (strstr(line, "AUTO-RESHARD") && strstr(line, "starting")) {
                    fclose(f);
                    return 1;
                }
            }
            fclose(f);
        }
        usleep(100000);
    }
    return 0;
}

static int test_auto_reshard_shutdown_race_run(void) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    int secs_left_in_hour = (59 - tmv.tm_min) * 60 + (60 - tmv.tm_sec);
    if (secs_left_in_hour < 90) {
        sleep(secs_left_in_hour + 5);
        now = time(NULL);
        localtime_r(&now, &tmv);
    }
    int target_hour = tmv.tm_hour;
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tmv);

    char base[] = "/tmp/shard-db-reshard-shutdown-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    int port = test_pick_port();
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/root", base);
    mkdir(db_root, 0755);

    /* Daemon reads ./db.env relative to its cwd (load_db_root(), main.c) —
       it does NOT take db_root as a CLI arg for `server`. Write db.env in
       `base` and chdir() the child into `base` before execl, matching
       test_auto_reshard.c's working pattern exactly. DB_ROOT inside
       db.env points at the separate `root` subdirectory. */
    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *ef = fopen(env_path, "w");
    ASSERT_NOT_NULL(ef, "open db.env for write");
    if (!ef) { char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c); return 1; }
    fprintf(ef,
        "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=2\nFCACHE_MAX=4096\nTLS_ENABLE=0\n"
        "AUTO_RESHARD_ENABLE=1\nAUTO_RESHARD_HOUR=%d\n"
        "KFCACHE_TEST_HOLD_MS=%d\n",
        db_root, port, target_hour, HOLD_MS);
    fclose(ef);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found");
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }

    /* Wait until ready (same poll pattern as test_auto_reshard.c). */
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
    if (!ready) {
        kill(pid, SIGKILL);
        int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) {
        kill(pid, SIGKILL);
        int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"widgets\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    /* Real insert: seeds a genuine "used" kfcache entry for this object's
       data_dir prefix. fabricate_kf_total() below writes straight to disk
       and never touches the in-memory kfcache -- without this insert, the
       invalidate loop in kfcache_invalidate_prefix() finds no matching
       entry and the injected hold never fires. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"widgets\","
                   "\"key\":\"w1\",\"value\":{\"name\":\"gear\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    /* Fabricate a live count that forces reshard_target_for_count() to
       recommend growth, so auto_reshard_sweep_one acts on this object. */
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/widgets/data/kf/000.kf", db_root);
    ASSERT_TRUE(fabricate_kf_total(kf_path, 5000000ULL) == 0, "fabricate kf total");

    ASSERT_TRUE(wait_for_reshard_start(db_root, date_str, 90),
        "AUTO-RESHARD starting line observed in -info.log");

    long t0 = now_ms();
    kill(pid, SIGTERM);

    int status = 0;
    pid_t r = 0;
    for (int i = 0; i < 100; i++) { /* up to 10s */
        r = waitpid(pid, &status, WNOHANG);
        if (r == pid) break;
        usleep(100000);
    }
    if (r != pid) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    long elapsed = now_ms() - t0;

    ASSERT_TRUE(!WIFSIGNALED(status),
        "daemon does not die by signal when SIGTERM lands mid-reshard");
    if (WIFSIGNALED(status))
        TAP_DIAG("# daemon killed by signal %d\n", WTERMSIG(status));

    ASSERT_TRUE(elapsed >= (HOLD_MS - 300),
        "shutdown waits for the in-flight reshard (proves join, not detach)");
    TAP_DIAG("# elapsed shutdown time: %ldms (hold=%dms)\n", elapsed, HOLD_MS);

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-shutdown-race", test_auto_reshard_shutdown_race_run)
```

Register it in `build.sh`.

Anchor (quoted exactly):

```
    src/test/cases/test_auto_vacuum.c \
    src/test/cases/test_reshard_target.c \
    src/test/cases/test_auto_reshard.c \
    src/test/cases/test_shard_stats_hint.c \
```

Replace with:

```
    src/test/cases/test_auto_vacuum.c \
    src/test/cases/test_reshard_target.c \
    src/test/cases/test_auto_reshard.c \
    src/test/cases/test_auto_reshard_shutdown_race.c \
    src/test/cases/test_shard_stats_hint.c \
```

The spawn/readiness pattern above (`chdir(base)` + db.env written to
`base/db.env`, not `db_root/db.env`; `server` invoked with no CLI arg;
readiness proven by polling a `db-dirs` request rather than a raw TCP
connect) mirrors `src/test/cases/test_auto_reshard.c`'s already-working
daemon spawn — same structure, but bare `KEY=value` lines instead of that
file's `export KEY="value"\n` style. Both parse identically (`config.c`
strips a leading `export ` prefix before matching any key), so this is a
cosmetic difference, not a functional one. `test_pick_port()` from
`src/test/fixtures.h` is confirmed to exist with signature `int
test_pick_port(void)`. Do not substitute a different spawn pattern (e.g.
passing `db_root` as an argv to `server`, or a `test_wait_for_port` helper)
— `load_db_root()` (`src/db/main.c`) reads `./db.env` relative to the
daemon's cwd and ignores any positional arg on `server`, so anything else
will silently fail to find the right `db.env`.

**Prove it fails on current (unfixed) code**:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-auto-reshard-shutdown-race
```

Paste the full output. Expect either the signal assertion, the timing
assertion, or both to fail (the timing assertion is the reliable one —
treat a run where only the signal assertion fails, but timing also fails,
as still a confirmed pre-fix failure). If the test passes outright on
unfixed code, stop — that means the reproduction isn't working as designed
(e.g. the hold isn't firing) — write `PLAN_NOTES.md` and halt.

### Task 3 — Apply the production fix in `src/db/server.c`

**3a. Hoist thread-tracking variables to function scope.**

Anchor (quoted exactly):

```c
    int port = g_port;
```

Replace with:

```c
    int port = g_port;
    pthread_t auto_vac_tid = 0;
    int auto_vac_spawned = 0;
    pthread_t auto_reshard_tid = 0;
    int auto_reshard_spawned = 0;
```

**3b. Stop detaching; track spawn success instead.**

Anchor (quoted exactly):

```c
    /* Auto-vacuum is opt-in. Detached thread; exits on server_running=0. */
    if (g_auto_vacuum_enable) {
        pthread_t auto_vac_tid;
        AutoVacuumArg *av = malloc(sizeof(AutoVacuumArg));
        if (av) {
            strncpy(av->db_root, db_root, PATH_MAX - 1);
            av->db_root[PATH_MAX - 1] = '\0';
            if (db_thread_create(&auto_vac_tid, auto_vacuum_thread, av) == 0)
                pthread_detach(auto_vac_tid);
            else
                free(av);
        }
    }

    /* Auto-reshard is opt-in. Detached thread; exits on server_running=0. */
    if (g_auto_reshard_enable) {
        pthread_t auto_reshard_tid;
        AutoReshardArg *ar = malloc(sizeof(AutoReshardArg));
        if (ar) {
            strncpy(ar->db_root, db_root, PATH_MAX - 1);
            ar->db_root[PATH_MAX - 1] = '\0';
            if (db_thread_create(&auto_reshard_tid, auto_reshard_thread, ar) == 0)
                pthread_detach(auto_reshard_tid);
            else
                free(ar);
        }
    }
```

Replace with:

```c
    /* Auto-vacuum is opt-in. Joined (not detached) on shutdown below —
       see the shutdown sequence's pthread_join() comment for why. */
    if (g_auto_vacuum_enable) {
        AutoVacuumArg *av = malloc(sizeof(AutoVacuumArg));
        if (av) {
            strncpy(av->db_root, db_root, PATH_MAX - 1);
            av->db_root[PATH_MAX - 1] = '\0';
            if (db_thread_create(&auto_vac_tid, auto_vacuum_thread, av) == 0)
                auto_vac_spawned = 1;
            else
                free(av);
        }
    }

    /* Auto-reshard is opt-in. Joined (not detached) on shutdown below —
       see the shutdown sequence's pthread_join() comment for why. */
    if (g_auto_reshard_enable) {
        AutoReshardArg *ar = malloc(sizeof(AutoReshardArg));
        if (ar) {
            strncpy(ar->db_root, db_root, PATH_MAX - 1);
            ar->db_root[PATH_MAX - 1] = '\0';
            if (db_thread_create(&auto_reshard_tid, auto_reshard_thread, ar) == 0)
                auto_reshard_spawned = 1;
            else
                free(ar);
        }
    }
```

**3c. Join both threads in the shutdown sequence, before cache teardown.**

Anchor (quoted exactly):

```c
    /* Wait for any remaining in-flight writes (up to 30s) */
    for (int i = 0; i < 300 && in_flight_writes > 0; i++) usleep(100000);

    remove_pid_file(db_root);
    parallel_io_pool_shutdown();
    parallel_pool_shutdown();
    counts_flush_all();        /* persist in-memory atomic counts → disk */
    fcache_shutdown();
    bt_cache_shutdown();
    slotcask_shutdown();
    tls_shutdown();
```

Replace with:

```c
    /* Wait for any remaining in-flight writes (up to 30s) */
    for (int i = 0; i < 300 && in_flight_writes > 0; i++) usleep(100000);

    /* Join the auto-vacuum/auto-reshard threads before any teardown that
       touches slotcask/kfcache/btcache. Both are spawned joinable (not
       detached) specifically for this: kfcache_shutdown() (invoked below
       via slotcask_shutdown()) frees the whole kfcache array and destroys
       every entry's rwlock while holding only g_kfcache_lock -- a mutex
       kfcache_invalidate_prefix() (reachable from a reshard/vacuum still
       in flight on either thread) deliberately never takes, to avoid a
       lock-order inversion with kfcache_acquire()'s install path (see
       slotcask.c). Without this join, a reshard/vacuum still running when
       shutdown reached kfcache_shutdown() raced it with zero mutual
       exclusion -- a genuine use-after-free/destroy-while-locked SIGSEGV.
       Bounded in practice: both threads already check server_running
       between sweep items (not mid-item), and a heavy vacuum/reshard
       already holds the object's exclusive lock for its full duration
       regardless -- so this join only ever waits as long as an
       in-progress heavy op that was already blocking all other traffic
       on that object. */
    if (auto_vac_spawned) pthread_join(auto_vac_tid, NULL);
    if (auto_reshard_spawned) pthread_join(auto_reshard_tid, NULL);

    remove_pid_file(db_root);
    parallel_io_pool_shutdown();
    parallel_pool_shutdown();
    counts_flush_all();        /* persist in-memory atomic counts → disk */
    fcache_shutdown();
    bt_cache_shutdown();
    slotcask_shutdown();
    tls_shutdown();
```

**3d. Correct the stale `auto_vacuum_thread` doc comment.**

Anchor (quoted exactly, the last sentence of the comment block):

```c
 * Sleep is sliced into 1-second chunks so SIGTERM (server_running=0)
 * brings shutdown latency down to <1s instead of waiting out the full
 * interval. Detached — no join on shutdown; it just exits its loop.
 */
```

Replace with:

```c
 * Sleep is sliced into 1-second chunks so SIGTERM (server_running=0) is
 * noticed within a second between ticks. Joined (not detached) on
 * shutdown — see cmd_server's shutdown sequence: a vacuum already in
 * flight when SIGTERM arrives is allowed to finish before cache teardown
 * proceeds, closing a use-after-free race against kfcache_shutdown().
 */
```

**3e. Correct the stale `auto_reshard_thread` doc comment.**

Anchor (quoted exactly, the last sentence of the comment block):

```c
 * Sleep is sliced into 1-second chunks so SIGTERM (server_running=0)
 * brings shutdown latency down to <1s. Detached — no join on shutdown.
 */
```

Replace with:

```c
 * Sleep is sliced into 1-second chunks so SIGTERM (server_running=0) is
 * noticed within a second between ticks. Joined (not detached) on
 * shutdown — see cmd_server's shutdown sequence: a reshard already in
 * flight when SIGTERM arrives runs to completion (it already holds the
 * object's exclusive lock for the whole rebuild) before cache teardown
 * proceeds, closing a use-after-free race against kfcache_shutdown().
 */
```

**3f. Block SIGTERM/SIGINT on both background threads.**

Both threads use sliced `sleep`/`nanosleep` loops, and the test-only
`KFCACHE_TEST_HOLD_MS` path also sleeps under a per-entry wrlock. If
SIGTERM is delivered to either thread (Linux picks any non-blocking
thread for a process-directed signal), it interrupts those sleeps and
can make the hold duration unreliable for the regression test's timing
assertion. Blocking the signals on these threads routes them to the
main thread's handler, which still sets `server_running=0`; the bg
threads notice that via their existing loop checks. No early-wake
requirement — between-item checks already bound responsiveness.

**3f-i. `auto_vacuum_thread`** — immediately after the `g_db = ...` bind,
before opening `/dev/null` for `g_out`:

Anchor (quoted exactly):

```c
    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Discard cmd_vacuum's JSON output — there's no client connection.
```

Replace with:

```c
    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Block SIGTERM/SIGINT on this thread so its nanosleep/sleep calls
       aren't interrupted by the process-wide shutdown signal. The main
       thread's signal handler will still set server_running=0, which this
       thread picks up via its loop check — no need to wake early. */
    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGTERM);
    sigaddset(&block_mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &block_mask, NULL);

    /* Discard cmd_vacuum's JSON output — there's no client connection.
```

**3f-ii. `auto_reshard_thread`** — same insert, same anchor pattern
(immediately after its own `g_db = g_shard_db_instance;` bind, before
the startup grace `sleep(5)`):

Anchor (quoted exactly):

```c
    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Startup grace period — see the function doc comment above for why
```

Replace with:

```c
    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Block SIGTERM/SIGINT on this thread so its nanosleep/sleep calls
       aren't interrupted by the process-wide shutdown signal. The main
       thread's signal handler will still set server_running=0, which this
       thread picks up via its loop check — no need to wake early. */
    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGTERM);
    sigaddset(&block_mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &block_mask, NULL);

    /* Startup grace period — see the function doc comment above for why
```

(`signal.h` is already pulled in transitively for `cmd_server`'s signal
handling in this TU; no new include needed.)

**Prove the regression test now passes**:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-auto-reshard-shutdown-race
```

Paste the full output — both assertions must pass.

### Task 4 — Documentation

**4a. `docs/concepts/concurrency.md`** — note the shutdown-latency change.

Anchor (quoted exactly):

```
## Write drain on shutdown

`./shard-db stop` sets `server_running = 0` (atomic) to refuse new connections and waits up to 30 seconds for the `in_flight_writes` atomic to reach zero. This guarantees that every write that entered the server before shutdown either committed or returned an error — no half-written records.

Reads are not drained; they're safe to abandon mid-scan.
```

Replace with:

```
## Write drain on shutdown

`./shard-db stop` sets `server_running = 0` (atomic) to refuse new connections and waits up to 30 seconds for the `in_flight_writes` atomic to reach zero. This guarantees that every write that entered the server before shutdown either committed or returned an error — no half-written records.

Reads are not drained; they're safe to abandon mid-scan.

`AUTO_VACUUM`/`AUTO_RESHARD_ENABLE`'s background threads are joined (not detached) as part of this same shutdown sequence, before any cache teardown (`slotcask_shutdown`/`kfcache_shutdown`, `bt_cache_shutdown`, `fcache_shutdown`). If either thread is mid-sweep on an object when `stop` is issued, shutdown waits for that item to finish — unbounded in theory, but no worse in practice than the exclusive objlock that operation already holds against all other traffic on that object. This closes a use-after-free race: without the join, `kfcache_shutdown()` could free/destroy the kfcache array while a reshard/vacuum thread was still using it.
```

**4b. `docs/getting-started/configuration.md`** — no change. `KFCACHE_TEST_HOLD_MS`
is test-only and must not appear in the operator-facing config table (see
Global Constraints above).

## Definition of done

- [ ] `SKIP_TESTS=1 ./build.sh` clean, no new warnings.
- [ ] `./build/bin/shard-db-test run test-auto-reshard-shutdown-race` fails
      on pre-Task-3 code (pasted output) and passes post-Task-3 (pasted
      output).
- [ ] `./build/bin/shard-db-test run-all --filter auto-reshard` passes
      (covers the new race test + existing auto-reshard cases).
- [ ] Full `./build/bin/shard-db-test run-all` passes when the suite is
      otherwise green. Known concurrent-test CI flakiness elsewhere in
      the suite is **not** a reason to rework this change — re-check
      only failures in the auto-reshard / shutdown-join path.
- [ ] `docs/concepts/concurrency.md` updated (write-drain paragraph);
      `configuration.md` deliberately untouched (`KFCACHE_TEST_HOLD_MS`
      stays test-only and undocumented for operators).
- [ ] Plan matches shipped code for: unlock-before-break on the test
      hold path, SIGTERM/SIGINT block on both bg threads, and
      `-info.log` polling (not a non-existent `-warn.log`).
- [ ] Work left uncommitted per this repo's execution mode — no commits,
      no push.
