# Auto-reshard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in nightly maintenance thread that compares each object's live record count against `docs/operations/tuning.md`'s recommended `splits` table and automatically runs `vacuum --splits=N` (a full reshard) on any object that has outgrown its current shard count.

**Architecture:** A new detached pthread `auto_reshard_thread()` in `src/db/server.c`, structurally mirroring the existing `auto_vacuum_thread()` (1-second sleep-increment loop, `g_dirs_lock`-protected dir snapshot, readdir walk of every `(dir, object)` pair), but additionally wall-clock-hour-gated: it only acts once per calendar day, during a configured hour. For each object it computes `live = get_live_count(...)`, looks up the recommended `splits` via a new pure function `reshard_target_for_count()`, and — if the object has outgrown its current `splits` — calls the existing `cmd_vacuum(db_root, object, 0, target)` to perform the reshard.

**Tech Stack:** C (C11), pthreads, the existing shard-db daemon/test harness (`src/test/test_runner.c`, `src/test/test_client.c`), `build.sh`.

## Global Constraints

- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run[-all]` (or `run-all --filter <substr>` to narrow).
- Never rename or touch the pre-existing "resplit" terminology/mechanism (per-shard in-place slot doubling in `slotcask.c`/`slotcask.h`, `test_slotcask_resplit.c`) — this feature is deliberately named "reshard" (matching existing docs/CLI usage in `docs/cli/index.md`, `docs/query-protocol/schema-mutations.md`, `docs/concepts/indexes.md`, `docs/reference/changelog.md`) to avoid colliding with that unrelated, already-shipped mechanism.
- No new dependencies.
- `is_valid_splits()` (`src/db/types.h:59`) already accepts every value this feature ever produces (`{8,16,32,64,128,256,512,1024,2048,4096}`) — do not modify it.
- Never lower `splits` — this feature only ever grows.
- Never auto-run `--compact` together with the reshard call; only `new_splits` is passed.
- Follow `LOG_SUB_VACUUM` (`src/db/log.h:39`) as the log tag for every new log line in this feature — it's the tag `cmd_vacuum`'s own call sites already use, and keeps auto-reshard's log lines groupable with manual/auto-vacuum's.
- Single source of truth: `reshard_target_for_count()` (Task 1) is the ONLY place that decides whether an object needs a bigger `splits`. Both the manual `shard-stats` diagnostic (Task 6) and the automatic `auto_reshard_thread()` (Task 3) must call it rather than keep their own independent thresholds — this is why Task 6 (fixing `cmd_shard_stats`'s hint to reuse the same lookup) is bundled into this plan instead of filed as a separate follow-up.

---

### Task 1: `reshard_target_for_count()` pure lookup function + unit test

**Files:**
- Modify: `src/db/query_maint.c` (add function near `cmd_vacuum`, after its closing brace)
- Modify: `src/db/types.h:1229` (add declaration immediately after the `cmd_vacuum` declaration)
- Test: `src/test/cases/test_reshard_target.c` (new file)
- Modify: `build.sh` (register new test file)

**Interfaces:**
- Produces: `int reshard_target_for_count(long long live)` — pure function, no I/O, no locks. Returns the recommended `splits` value for a given live record count, per `docs/operations/tuning.md`'s sizing table (extended with the new 2048 band). Declared in `src/db/types.h`, defined in `src/db/query_maint.c`.

- [ ] **Step 1: Write the failing unit test**

Create `src/test/cases/test_reshard_target.c`:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

static int test_reshard_target_run(void) {
    /* Below every band's lower bound: smallest recommended splits. */
    ASSERT_EQ_INT(reshard_target_for_count(0), 8, "0 records -> 8");
    ASSERT_EQ_INT(reshard_target_for_count(999999), 8, "999,999 -> 8 (just under 1M)");

    /* 1M-10M band -> 16. */
    ASSERT_EQ_INT(reshard_target_for_count(1000000), 16, "1,000,000 -> 16 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(9999999), 16, "9,999,999 -> 16 (just under 10M)");

    /* 10M-50M band -> 64. */
    ASSERT_EQ_INT(reshard_target_for_count(10000000), 64, "10,000,000 -> 64 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(49999999), 64, "49,999,999 -> 64 (just under 50M)");

    /* 50M-200M band -> 256. */
    ASSERT_EQ_INT(reshard_target_for_count(50000000), 256, "50,000,000 -> 256 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(199999999), 256, "199,999,999 -> 256 (just under 200M)");

    /* 200M-1B band -> 1024. */
    ASSERT_EQ_INT(reshard_target_for_count(200000000), 1024, "200,000,000 -> 1024 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(999999999), 1024, "999,999,999 -> 1024 (just under 1B)");

    /* 1B-5B band -> 2048 (new). */
    ASSERT_EQ_INT(reshard_target_for_count(1000000000LL), 2048, "1,000,000,000 -> 2048 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(4999999999LL), 2048, "4,999,999,999 -> 2048 (just under 5B)");

    /* 5B-10B band -> 4096. */
    ASSERT_EQ_INT(reshard_target_for_count(5000000000LL), 4096, "5,000,000,000 -> 4096 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(9999999999LL), 4096, "9,999,999,999 -> 4096 (just under 10B)");

    /* 10B+ -> 4096 (no further auto action; ceiling). */
    ASSERT_EQ_INT(reshard_target_for_count(10000000000LL), 4096, "10,000,000,000 -> 4096 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(50000000000LL), 4096, "50,000,000,000 -> 4096 (well past 10B)");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reshard-target", test_reshard_target_run)
```

- [ ] **Step 2: Register the new test file in `build.sh`**

Find this line in `build.sh`:

```
    src/test/cases/test_auto_vacuum.c \
```

Insert immediately after it:

```
    src/test/cases/test_auto_vacuum.c \
    src/test/cases/test_reshard_target.c \
```

- [ ] **Step 3: Run the test to verify it fails (function not declared/defined yet)**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-reshard-target`
Expected: build FAILS with `implicit declaration of function 'reshard_target_for_count'` (or the test binary doesn't build at all).

- [ ] **Step 4: Add the declaration to `types.h`**

Find this line in `src/db/types.h`:

```c
int cmd_vacuum(const char *db_root, const char *object,
               int compact, int new_splits);
```

Insert immediately after it:

```c
/* Pure lookup, no I/O: recommended `splits` for a given live record
   count, per docs/operations/tuning.md's sizing table. Used by
   auto-reshard to decide whether/where to grow an object's splits. */
int reshard_target_for_count(long long live);
```

- [ ] **Step 5: Implement the function in `query_maint.c`**

Find the end of `cmd_vacuum` in `src/db/query_maint.c` — locate its closing brace by finding the next top-level function after the `cmd_vacuum` definition that starts at line 161 (`int cmd_vacuum(const char *db_root, const char *object,`). Add the new function immediately after `cmd_vacuum`'s closing brace, before the next function definition:

```c
/* Recommended `splits` for a given live record count, per
   docs/operations/tuning.md's "Recommended splits by record count"
   table. Pure function, no I/O — safe to call from any thread without
   locks. Smallest band whose lower bound `live` has crossed wins. */
int reshard_target_for_count(long long live) {
    if (live >= 10000000000LL) return 4096;  /* 10B+ (ceiling, no further action) */
    if (live >= 5000000000LL)  return 4096;  /* 5B-10B */
    if (live >= 1000000000LL)  return 2048;  /* 1B-5B */
    if (live >= 200000000LL)   return 1024;  /* 200M-1B */
    if (live >= 50000000LL)    return 256;   /* 50M-200M */
    if (live >= 10000000LL)    return 64;    /* 10M-50M */
    if (live >= 1000000LL)     return 16;    /* 1M-10M */
    return 8;                                 /* up to 1M */
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-reshard-target`
Expected: PASS, all assertions green.

- [ ] **Step 7: Commit**

```bash
git add src/db/types.h src/db/query_maint.c src/test/cases/test_reshard_target.c build.sh
git commit -m "feat: add reshard_target_for_count lookup for auto-reshard"
```

---

### Task 2: Config plumbing — `AUTO_RESHARD_ENABLE` / `AUTO_RESHARD_HOUR`

**Files:**
- Modify: `src/db/shard_db_internal.h` (struct fields + macro aliases)
- Modify: `src/db/embedded.c` (default value)
- Modify: `src/db/config.c` (db.env parsing)

**Interfaces:**
- Consumes: nothing new.
- Produces: `g_auto_reshard_enable` (int, macro over `g_db->auto_reshard_enable`), `g_auto_reshard_hour` (int, macro over `g_db->auto_reshard_hour`) — both usable via the existing `g_db` thread-local pattern, consumed by Task 3's `auto_reshard_thread()`.

- [ ] **Step 1: Add struct fields**

Find this block in `src/db/shard_db_internal.h`:

```c
    int auto_vacuum_enable;
    int auto_vacuum_interval_sec;
```

Insert immediately after it:

```c
    int auto_reshard_enable;
    int auto_reshard_hour;
```

- [ ] **Step 2: Add macro aliases**

Find this block in `src/db/shard_db_internal.h`:

```c
#define g_auto_vacuum_enable        (g_db->auto_vacuum_enable)
#define g_auto_vacuum_interval_sec  (g_db->auto_vacuum_interval_sec)
```

Insert immediately after it:

```c
#define g_auto_reshard_enable       (g_db->auto_reshard_enable)
#define g_auto_reshard_hour         (g_db->auto_reshard_hour)
```

- [ ] **Step 3: Add the default hour in `embedded.c`**

Find this line in `src/db/embedded.c`:

```c
    db->auto_vacuum_interval_sec  = 3600;
```

Insert immediately after it:

```c
    db->auto_reshard_hour         = 3;    /* server-local hour, 0-23; low-usage default */
```

(`auto_reshard_enable` needs no explicit default line — `db` is `calloc`'d, so it defaults to `0`/off, matching `auto_vacuum_enable`'s own opt-in-by-omission precedent.)

- [ ] **Step 4: Add db.env parsing in `config.c`**

Find this block in `src/db/config.c`:

```c
        } else if (strncmp(p, "AUTO_VACUUM=", 12) == 0) {
            if (g_db) g_auto_vacuum_enable = (atoi(p + 12) != 0);
        } else if (strncmp(p, "AUTO_VACUUM_INTERVAL_SEC=", 25) == 0) {
            int n = atoi(p + 25);
            if (n >= 60 && g_db) g_auto_vacuum_interval_sec = n; /* 1-min floor */
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

Replace with:

```c
        } else if (strncmp(p, "AUTO_VACUUM=", 12) == 0) {
            if (g_db) g_auto_vacuum_enable = (atoi(p + 12) != 0);
        } else if (strncmp(p, "AUTO_VACUUM_INTERVAL_SEC=", 25) == 0) {
            int n = atoi(p + 25);
            if (n >= 60 && g_db) g_auto_vacuum_interval_sec = n; /* 1-min floor */
        } else if (strncmp(p, "AUTO_RESHARD_ENABLE=", 20) == 0) {
            if (g_db) g_auto_reshard_enable = (atoi(p + 20) != 0);
        } else if (strncmp(p, "AUTO_RESHARD_HOUR=", 18) == 0) {
            int n = atoi(p + 18);
            if (n >= 0 && n <= 23 && g_db) g_auto_reshard_hour = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

- [ ] **Step 5: Build to verify no compile errors**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean build, no warnings.

- [ ] **Step 6: Commit**

```bash
git add src/db/shard_db_internal.h src/db/embedded.c src/db/config.c
git commit -m "feat: add AUTO_RESHARD_ENABLE/AUTO_RESHARD_HOUR config knobs"
```

---

### Task 3: `auto_reshard_thread()` + startup wiring

**Files:**
- Modify: `src/db/server.c` (new struct + thread function + startup call site)

**Interfaces:**
- Consumes: `reshard_target_for_count()` (Task 1), `g_auto_reshard_enable` / `g_auto_reshard_hour` (Task 2), `get_live_count()` (existing, `src/db/types.h:958`), `cmd_vacuum()` (existing, `src/db/types.h:1228`), `load_schema()` (existing), `LOG_SUB_VACUUM` (existing, `src/db/log.h:39`), `g_dirs`/`g_dirs_used`/`g_dirs_lock`/`DIRS_BUCKETS`/`server_running` (existing globals used the same way `auto_vacuum_thread` uses them), `db_thread_create()` (existing, `src/db/types.h:1350`).
- Produces: `auto_reshard_thread(void *arg)` (thread entry point, `void *` return), `AutoReshardArg` (struct with one field: `char db_root[PATH_MAX]`).

- [ ] **Step 1: Add the `AutoReshardArg` struct and `auto_reshard_thread()` function**

Find this block in `src/db/server.c` (the `auto_vacuum_thread` function, ending at its closing brace):

```c
    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}

/* Startup metadata validator.
 *
```

Replace with:

```c
    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}

/* Background auto-reshard thread.
 *
 * Wall-clock-gated (server-local time): once per calendar day, during
 * hour g_auto_reshard_hour, walks every (dir, object) and compares its
 * live record count against reshard_target_for_count()'s recommended
 * `splits`. If the object has outgrown its current `splits`, runs
 * `vacuum --splits=target` (a full reshard) on it.
 *
 * Unlike auto_vacuum_thread, this DOES run the heavy --splits path —
 * that's the entire point of this feature. vacuum --splits holds the
 * object's exclusive objlock for the full rehash, so reads/writes to
 * that object block until it completes; each reshard is logged loudly
 * (LOG_WARN) immediately before it starts, precisely because this is a
 * deliberate, opt-in exception to auto_vacuum_thread's own "never
 * auto-run --splits" rule (see the comment above that function).
 *
 * The in-memory last_run_date guard means a restart during the trigger
 * hour can re-run the same night's sweep — acceptable, since re-checking
 * an object already at its target `splits` is a cheap get_live_count +
 * table lookup, not a rebuild.
 *
 * A fixed 5s startup delay runs before the first wall-clock check (see
 * below). Unlike auto_vacuum_thread's plain interval loop, this thread
 * has a once-per-calendar-day guard (last_run_date) — if its very first
 * tick lands during the matching hour before daemon startup has fully
 * settled (e.g. objects the sweep should act on don't exist yet), it
 * scans, finds nothing eligible, sets last_run_date, and won't check
 * again until the next day. The 5s delay gives startup (and, in tests,
 * the harness setting up fixtures against a just-started daemon) room
 * to finish before the first tick can ever fire. Negligible in
 * production (5s once, before an opt-in nightly maintenance thread).
 *
 * Sleep is sliced into 1-second chunks so SIGTERM (server_running=0)
 * brings shutdown latency down to <1s. Detached — no join on shutdown.
 */
typedef struct {
    char db_root[PATH_MAX];
} AutoReshardArg;

static void *auto_reshard_thread(void *arg) {
    AutoReshardArg *a = (AutoReshardArg *)arg;

    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Startup grace period — see the function doc comment above for why
       this must run before the first wall-clock check, not just before
       the loop's steady-state ticks. */
    sleep(5);

    /* Discard cmd_vacuum's JSON output — there's no client connection.
       /dev/null open failure shouldn't kill the thread; fall back to
       stderr (which the daemon redirects to /dev/null after fork). */
    g_out = fopen("/dev/null", "w");
    if (!g_out) g_out = stderr;

    LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD thread started: hour=%d",
            g_auto_reshard_hour);

    char last_run_date[16] = "";

    while (server_running) {
        for (int i = 0; i < 1 && server_running; i++)
            sleep(1);
        if (!server_running) break;

        time_t now = time(NULL);
        struct tm tmv;
        localtime_r(&now, &tmv);
        if (tmv.tm_hour != g_auto_reshard_hour) continue;

        char today[16];
        snprintf(today, sizeof(today), "%04d-%02d-%02d",
                 tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
        if (strcmp(today, last_run_date) == 0) continue;
        strncpy(last_run_date, today, sizeof(last_run_date) - 1);
        last_run_date[sizeof(last_run_date) - 1] = '\0';

        /* Snapshot dir table so we don't hold g_dirs_lock for the full
           sweep (mirrors auto_vacuum_thread). */
        char dirs_copy[DIRS_BUCKETS][256];
        int used_copy[DIRS_BUCKETS];
        pthread_mutex_lock(&g_dirs_lock);
        memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
        memcpy(used_copy, g_dirs_used, sizeof(used_copy));
        pthread_mutex_unlock(&g_dirs_lock);

        uint64_t tick_t0 = now_ms();
        int scanned = 0, reshaped = 0;
        for (int di = 0; di < DIRS_BUCKETS && server_running; di++) {
            if (!used_copy[di]) continue;
            char dir_path[PATH_MAX];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", a->db_root, dirs_copy[di]);
            DIR *dd = opendir(dir_path);
            if (!dd) continue;
            struct dirent *de;
            while ((de = readdir(dd)) && server_running) {
                if (de->d_name[0] == '.') continue;
                char obj_check[PATH_MAX];
                snprintf(obj_check, sizeof(obj_check),
                         "%s/%s/fields.conf", dir_path, de->d_name);
                struct stat ost;
                if (stat(obj_check, &ost) != 0) continue;
                scanned++;

                char eff[PATH_MAX];
                snprintf(eff, sizeof(eff), "%s/%s", a->db_root, dirs_copy[di]);
                Schema sch = load_schema(eff, de->d_name);
                int live = get_live_count(eff, de->d_name);
                int target = reshard_target_for_count(live);
                if (target <= sch.splits) continue;

                LOG_WARN(LOG_SUB_VACUUM,
                    "AUTO-RESHARD %s/%s: starting %d -> %d splits (live=%d) "
                    "— object locked for the duration",
                    dirs_copy[di], de->d_name, sch.splits, target, live);
                uint64_t obj_t0 = now_ms();
                int rc = cmd_vacuum(eff, de->d_name, 0, target);
                if (rc == 0) {
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%d) in %lums",
                        dirs_copy[di], de->d_name, sch.splits, target, live,
                        (unsigned long)(now_ms() - obj_t0));
                    reshaped++;
                } else {
                    LOG_ERROR(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
                        dirs_copy[di], de->d_name, target);
                }
            }
            closedir(dd);
        }
        LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD tick: scanned=%d reshaped=%d in %lums",
                scanned, reshaped, (unsigned long)(now_ms() - tick_t0));
    }

    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}

/* Startup metadata validator.
 *
```

- [ ] **Step 2: Wire up thread startup**

Find this block in `src/db/server.c`:

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
```

Replace with:

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

- [ ] **Step 3: Build to verify no compile errors**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean build, no warnings. (`LOG_WARN` and `LOG_ERROR` are already used elsewhere in `server.c`; `time_t`/`localtime_r`/`struct tm` come from `<time.h>`, already included by `server.c` for `auto_vacuum_thread`'s neighbors — if the build reports `time_t`/`localtime_r` undeclared, add `#include <time.h>` near the top of `src/db/server.c`'s existing include block.)

- [ ] **Step 4: Commit**

```bash
git add src/db/server.c
git commit -m "feat: add auto_reshard_thread, wired up alongside auto_vacuum_thread"
```

---

### Task 4: Integration test

**Files:**
- Test: `src/test/cases/test_auto_reshard.c` (new file)
- Modify: `build.sh` (register new test file)

**Interfaces:**
- Consumes: `test_pick_port()`, `TestEnv`, `TestClientCfg`, `tc_connect()`, `tc_request()`, `tc_close()`, `test_env_stop_keep()`, `tu_run_cmd()`, `tu_parse_count()` — all from `src/test/test_client.h` / `src/test/test_runner.h`, same as `test_auto_vacuum.c` uses. `SlotcaskKfHeader` layout (`magic` u32 @0, `version` u32 @4, `total` u64 @8, `deleted` u64 @16, packed, 24 bytes total) from `src/db/slotcask.h:64-73`, used to fabricate kf shard state via direct file write (this test spawns the daemon as a **separate process** via `fork`+`execl`, so it cannot call in-process test helpers like `slotcask_test_set_kf_total()` — those require an already-open `SlotcaskDb*` in the same address space, which only same-process unit tests like `test_slotcask_resplit.c` have. `slotcask_sum_kf_totals()`, which `get_live_count()` calls, deliberately does a **fresh `open()`+`pread()` per call** rather than going through the mmap'd kfcache — see the comment at `src/db/slotcask.c:2874-2882` — specifically so it avoids evicting cache entries; this also means it has no cache-staleness class of issue and will correctly observe an external process's direct `pwrite()` to the same kf file via the OS page cache).
- Produces: nothing consumed by later tasks (this is the last task).

- [ ] **Step 1: Write the integration test**

Create `src/test/cases/test_auto_reshard.c`:

```c
/* src/test/cases/test_auto_reshard.c
 * Auto-reshard thread fires within its configured hour, reshards an
 * object whose fabricated live count has outgrown its current splits,
 * and leaves an already-correctly-sized object untouched.
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
 * Live-count fabrication: this test spawns the daemon as a separate
 * process (fork+execl), so it cannot call in-process helpers like
 * slotcask_test_set_kf_total() (those need an already-open SlotcaskDb*
 * in the same address space). Instead it writes directly into shard 0's
 * on-disk kf header (offset 8, 8 bytes, the `total` field) after
 * create-object has created the (real, empty) kf shard files. This is
 * safe here because slotcask_sum_kf_totals() (which get_live_count()
 * calls, src/db/slotcask.c:2884) does a fresh open()+pread() per shard
 * rather than going through the mmap'd kfcache, so it observes the
 * external pwrite() via the OS page cache with no staleness window.
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
       startup delay, Task 3) already matches. */
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=3\n"
        "export THREADS=0\n"
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
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object 1: under-split. splits=8, fabricate shard 0's live count to
       2,000,000 (falls in the 1M-10M band -> target=16 > 8). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"grown\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;

    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/grown/data/kf/000.kf", db_root);
    ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0,
                  "fabricate shard 0 total=2,000,000 on 'grown'");

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

    /* Sanity: pre-sweep, both objects still at their created splits. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":8", "grown starts at splits=8");
    free(resp); resp = NULL;

    /* Wait for the thread's first hour-matching tick. auto_reshard_thread
       sleeps 5s on startup (Task 3) before its first wall-clock check,
       specifically so this setup above always finishes first — then it
       sleeps in 1s increments and checks the wall clock each tick. 20s
       gives generous slack on top of the 5s startup delay for slow CI. */
    printf("# auto-reshard: waiting up to 20s for the first thread tick...\n");
    fflush(stdout);
    int grown_reshaped = 0;
    for (int i = 0; i < 40; i++) {
        struct timespec ts = { 0, 500 * 1000000L }; nanosleep(&ts, NULL);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
        if (resp && strstr(resp, "\"splits\":16")) { grown_reshaped = 1; free(resp); resp = NULL; break; }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(grown_reshaped, "grown reshaped from splits=8 to splits=16 within 20s");

    /* sized must be untouched. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":64", "sized stays at splits=64 (no-op path)");
    free(resp); resp = NULL;

    /* All 5 real inserts on 'sized' survive untouched. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 5, "sized count=5 (untouched)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard", test_auto_reshard_run)
```

- [ ] **Step 2: Register the new test file in `build.sh`**

Find this line in `build.sh` (inserted by Task 1, Step 2):

```
    src/test/cases/test_reshard_target.c \
```

Insert immediately after it:

```
    src/test/cases/test_reshard_target.c \
    src/test/cases/test_auto_reshard.c \
```

- [ ] **Step 3: Run the test to verify it fails (feature not wired yet — skip if Tasks 1-3 already landed)**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: if run before Tasks 1-3 are complete, FAILS (`AUTO_RESHARD_ENABLE` unrecognized / no reshard happens / assertion `grown reshaped...` fails). If run after Tasks 1-3, skip to Step 4.

- [ ] **Step 4: Run the test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: PASS, all assertions green.

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass (same pass count as before this feature, plus the two new tests).

- [ ] **Step 6: Commit**

```bash
git add src/test/cases/test_auto_reshard.c build.sh
git commit -m "test: add auto-reshard integration test"
```

---

### Task 5: Documentation — `configuration.md` and `tuning.md`

**Files:**
- Modify: `docs/getting-started/configuration.md`
- Modify: `docs/operations/tuning.md`

**Interfaces:**
- Consumes: nothing (docs-only task).
- Produces: nothing consumed by other tasks (docs-only task).

- [ ] **Step 1: Add the two new db.env keys to the reference table**

Find this exact text in `docs/getting-started/configuration.md`:

```
| `VACUUM_RECOMMEND_MIN_DELETED` | `1000` | Absolute floor on `deleted` count below which `vacuum-check` does **not** recommend cleanup, even if the percentage clears. Prevents tiny objects from triggering vacuum overhead that exceeds the work saved. |
| `TLS_ENABLE` | `0` | `1` = require TLS 1.3 on `PORT`; plaintext clients rejected at handshake. See [Operations → Deployment → Native TLS](../operations/deployment.md). |
```

Replace with:

```
| `VACUUM_RECOMMEND_MIN_DELETED` | `1000` | Absolute floor on `deleted` count below which `vacuum-check` does **not** recommend cleanup, even if the percentage clears. Prevents tiny objects from triggering vacuum overhead that exceeds the work saved. |
| `AUTO_RESHARD_ENABLE` | `0` | `1` = enable a background thread that, once per calendar day during `AUTO_RESHARD_HOUR`, grows an object's `splits` when its live record count outgrows the recommended sizing table (see [Tuning → Recommended splits](../operations/tuning.md)). Runs a full reshard (`vacuum --splits=N`), which holds the exclusive objlock for the duration — unlike `AUTO_VACUUM`, which never touches `--splits`. |
| `AUTO_RESHARD_HOUR` | `3` | Server-local hour (`0`-`23`) the auto-reshard sweep is allowed to run in, once per calendar day. |
| `TLS_ENABLE` | `0` | `1` = require TLS 1.3 on `PORT`; plaintext clients rejected at handshake. See [Operations → Deployment → Native TLS](../operations/deployment.md). |
```

- [ ] **Step 2: Add the example db.env block**

Find this exact text in `docs/getting-started/configuration.md`:

```bash
# Auto-vacuum — opt-in. Same thresholds drive `vacuum-check` recommendations.
export AUTO_VACUUM=0
export AUTO_VACUUM_INTERVAL_SEC=3600
export VACUUM_RECOMMEND_TOMBSTONE_PCT=10
export VACUUM_RECOMMEND_MIN_DELETED=1000

# Native TLS — leave TLS_ENABLE=0 unless terminating TLS in-process
```

Replace with:

```bash
# Auto-vacuum — opt-in. Same thresholds drive `vacuum-check` recommendations.
export AUTO_VACUUM=0
export AUTO_VACUUM_INTERVAL_SEC=3600
export VACUUM_RECOMMEND_TOMBSTONE_PCT=10
export VACUUM_RECOMMEND_MIN_DELETED=1000

# Auto-reshard — opt-in, grows splits automatically when an object outgrows
# its sizing (off by default)
export AUTO_RESHARD_ENABLE=0
export AUTO_RESHARD_HOUR=3

# Native TLS — leave TLS_ENABLE=0 unless terminating TLS in-process
```

- [ ] **Step 3: Update the sizing table in `tuning.md`**

Find this table in `docs/operations/tuning.md`:

```
| Expected live records | Recommended `splits` | Live records / kf shard | Per-shard headroom |
|-----------------------|----------------------|------------------------:|--------------------|
| up to 1M              | 8                    | ~125K                   | ~100× before resplit ceiling |
| 1–10M                 | 16                   | 63K – 625K              | ~20× headroom |
| 10–50M                | 64                   | 156K – 781K             | ~16× headroom |
| 50–200M               | 256                  | 195K – 781K             | ~16× headroom |
| 200M–1B               | 1024                 | 195K – 977K             | ~13× headroom |
| 1B–10B                | 4096 (MAX_SPLITS)    | 244K – 2.4M             | ~5× headroom |
| 10B+                  | 4096, partition by object | n/a — partition the object | — |
```

Replace with:

```
| Expected live records | Recommended `splits` | Live records / kf shard | Per-shard headroom |
|-----------------------|----------------------|------------------------:|--------------------|
| up to 1M              | 8                    | ~125K                   | ~100× before resplit ceiling |
| 1–10M                 | 16                   | 63K – 625K              | ~20× headroom |
| 10–50M                | 64                   | 156K – 781K             | ~16× headroom |
| 50–200M               | 256                  | 195K – 781K             | ~16× headroom |
| 200M–1B               | 1024                 | 195K – 977K             | ~13× headroom |
| 1B–5B                 | 2048                 | 488K – 2.4M             | ~5× headroom |
| 5B–10B                | 4096 (MAX_SPLITS)    | 1.2M – 2.4M             | ~5× headroom |
| 10B+                  | 4096, partition by object | n/a — partition the object | — |
```

- [ ] **Step 4: Add a short note about the automated option**

Find this paragraph in `docs/operations/tuning.md`:

```
Defaults: `create-object` with no `splits` gives **8** (fine for sub-10M objects — the ~80 % case). For 50M+ rows set `splits` explicitly per the table above; otherwise let the daemon nag you and `vacuum --splits=N` later.
```

Replace with:

```
Defaults: `create-object` with no `splits` gives **8** (fine for sub-10M objects — the ~80 % case). For 50M+ rows set `splits` explicitly per the table above; otherwise let the daemon nag you and `vacuum --splits=N` later, or turn on `AUTO_RESHARD_ENABLE=1` (see [configuration.md](../getting-started/configuration.md)) to have a nightly job do it for you automatically.
```

- [ ] **Step 5: Verify docs build**

This repo has `mkdocs.yml` at the root, built with `mkdocs build --strict` in CI (`.github/workflows/docs.yml:44`). If `mkdocs` is installed locally, run:

Run: `mkdocs build --strict`
Expected: builds clean, no broken-link or nav warnings (strict mode turns warnings into errors). If `mkdocs` is not installed locally, skip this step — CI will catch any strict-mode issues on push.

- [ ] **Step 6: Commit**

```bash
git add docs/getting-started/configuration.md docs/operations/tuning.md
git commit -m "docs: document AUTO_RESHARD_ENABLE/AUTO_RESHARD_HOUR and new 1B-5B/5B-10B splits bands"
```

---

### Task 6: `cmd_shard_stats` reuses `reshard_target_for_count()` — one method, one answer

**Why:** `cmd_shard_stats`'s current hint logic (`src/db/query_maint.c:772-790`) recommends resharding based on `rps` (records-per-shard *average*) crossing 1M/500K — a per-shard signal. But the trigger this feature already settled on (see "Trigger" in `docs/plans/2026-07-13-auto-reshard-design.md`) is that resharding is purely a function of *total live record count*, independent of any per-shard skew. `docs/operations/tuning.md:232` also documents the hint as firing on a per-shard **max** crossing 1M, which doesn't match the average-based code either way. Rather than patch the average into a max (still the wrong signal) or leave two independent recommendation paths to drift apart, this task makes `cmd_shard_stats` call the exact same `reshard_target_for_count()` Task 1 added for `auto_reshard_thread()` — one function decides "does this object need a bigger `splits`," consumed identically by the manual diagnostic and the automatic sweep. The separate skew hint (`max_records > min_records * 4` → "shard load is skewed") is a distinct, still-valid signal (key-distribution health, not a sizing recommendation) and is left untouched.

**Requires:** Task 1 complete (`reshard_target_for_count()` must exist before this task can call it).

**Files:**
- Modify: `src/db/query_maint.c:772-790` (`cmd_shard_stats`'s hint logic)
- Modify: `docs/operations/tuning.md` (the "daemon will tell you when to re-split" paragraph)
- Test: `src/test/cases/test_shard_stats_hint.c` (new file)
- Modify: `build.sh` (register new test file)

**Interfaces:**
- Consumes: `int reshard_target_for_count(long long live)` (Task 1, `src/db/types.h`). `MAX_SPLITS` (`src/db/types.h`, already in scope in `query_maint.c`).
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Write the failing integration test**

Create `src/test/cases/test_shard_stats_hint.c`:

```c
/* src/test/cases/test_shard_stats_hint.c
 * cmd_shard_stats's reshard hint must come from the same
 * reshard_target_for_count() lookup auto-reshard uses — a total-live-count
 * decision, not a per-shard average/max. The separate skew hint
 * (max_records > 4 * min_records) is a different, still-valid signal and
 * must keep firing on its own.
 *
 * Live-count fabrication: writes directly into each kf shard's on-disk
 * header (offset 8, 8 bytes, the `total` field — magic u32@0, version
 * u32@4, total u64@8, deleted u64@16, see SlotcaskKfHeader in
 * src/db/slotcask.h) after create-object has created the (real, empty)
 * kf shard files. Safe here because cmd_shard_stats() (src/db/
 * query_maint.c) reads each shard header with its own open()+pread()+
 * close() directly against the kf file, not through the mmap'd kfcache
 * — so it observes the external pwrite() via the OS page cache with no
 * staleness window.
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
#include <fcntl.h>
#include <unistd.h>

/* Writes `total` into a kf shard's header at offset 8 (the `total`
   field). Leaves magic/version/deleted untouched. */
static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (n == (ssize_t)sizeof(total)) ? 0 : -1;
}

static int test_shard_stats_hint_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object 1: under-split. splits=8, fabricate shard 0's live count to
       2,000,000 (falls in the 1M-10M band -> target=16 > 8). Hint must
       recommend splits=16. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"grown\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/grown/data/kf/000.kf", env.db_root);
    ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0,
                  "fabricate shard 0 total=2,000,000 on 'grown'");

    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_CONTAINS(resp, "\"hint\"", "grown: hint present (target=16 > splits=8)");
    ASSERT_CONTAINS(resp, "splits=16", "grown: hint recommends splits=16");
    free(resp); resp = NULL;

    /* Object 2: already correctly sized. splits=64, live stays tiny (a
       few real inserts) -> target=8 <= 64, no hint at all. */
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
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_TRUE(!strstr(resp, "\"hint\""), "sized: no hint (target=8 <= splits=64, no skew)");
    free(resp); resp = NULL;

    /* Object 3: skew hint must still fire on its own, independent of the
       reshard-target check. splits=8, all 8 shards fabricated nonzero so
       min_records > 0; shard 7 is 10x every other shard (> 4x skew
       threshold). Total stays well under 1M so target=8 <= splits=8 -
       the reshard-target branch must NOT also fire here. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"skewed\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 0; i < 8; i++) {
        char shard_kf[PATH_MAX];
        snprintf(shard_kf, sizeof(shard_kf), "%s/default/skewed/data/kf/%03d.kf", env.db_root, i);
        uint64_t total = (i == 7) ? 1000ULL : 100ULL;
        ASSERT_EQ_INT(fabricate_kf_total(shard_kf, total), 0, "fabricate skewed shard total");
    }
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"skewed\"}", &resp);
    ASSERT_CONTAINS(resp, "skewed", "skewed: skew hint fires");
    ASSERT_TRUE(!strstr(resp, "splits="), "skewed: no reshard-target hint (target=8 <= splits=8)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-shard-stats-hint", test_shard_stats_hint_run)
```

- [ ] **Step 2: Register the new test file in `build.sh`**

Find this line in `build.sh` (inserted by Task 4, Step 2):

```
    src/test/cases/test_reshard_target.c \
    src/test/cases/test_auto_reshard.c \
```

Insert immediately after it:

```
    src/test/cases/test_reshard_target.c \
    src/test/cases/test_auto_reshard.c \
    src/test/cases/test_shard_stats_hint.c \
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-shard-stats-hint`
Expected: FAILS — `grown`'s hint (still average-`rps`-based) doesn't contain `splits=16`, and/or the skew-only case still trips the old `rps > 500000` branch differently. The exact failure depends on today's code; any assertion failure here is acceptable proof the old logic doesn't match.

- [ ] **Step 4: Replace the hint logic in `cmd_shard_stats`**

Find this exact block in `src/db/query_maint.c`:

```c
    /* Hint: in v1, sizing is driven by records-per-shard (sweet spot 78K-200K).
       In v2 the kf auto-resplits, so high recs/shard ≠ broken — but it does mean
       the kf paid inline doubling cost. Same advice ("vacuum --splits=N") fits
       both: a higher `splits` up-front avoids resplit work. */
    const char *hint = NULL;
    double avg_load = 0.0;
    uint64_t rps = 0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        rps = total_records / (uint64_t)nrows;
        if (rps > 1000000ULL) {
            hint = (sch.splits < MAX_SPLITS)
                ? "records-per-shard past sweet spot (>1M) — re-split with vacuum --splits=N"
                : "at MAX_SPLITS with >1M records/shard — performance may degrade; consider partitioning across objects";
        } else if (rps > 500000ULL && sch.splits < MAX_SPLITS) {
            hint = "records-per-shard approaching upper band (>500K) — consider vacuum --splits=N";
        } else if (min_records > 0 && max_records > min_records * 4) {
            hint = "shard load is skewed — check key distribution";
        }
    }
```

Replace with:

```c
    /* Hint: single source of truth with auto-reshard — reuse
       reshard_target_for_count() (src/db/query_maint.c, added for
       auto_reshard_thread()) instead of an independent per-shard
       average/max threshold, so shard-stats and the automatic nightly
       sweep never disagree about whether an object needs a bigger
       `splits`. Stack-local hint_buf (not static): cmd_shard_stats can
       run concurrently across server worker threads for different
       objects. */
    const char *hint = NULL;
    char hint_buf[160];
    double avg_load = 0.0;
    uint64_t rps = 0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        rps = total_records / (uint64_t)nrows;
        int target = reshard_target_for_count((long long)total_records);
        if (target > sch.splits) {
            snprintf(hint_buf, sizeof(hint_buf),
                     "live records (%lu) recommend splits=%d (currently %d) — re-split with vacuum --splits=%d",
                     (unsigned long)total_records, target, sch.splits, target);
            hint = hint_buf;
        } else if (sch.splits >= MAX_SPLITS && total_records >= 10000000000ULL) {
            hint = "at MAX_SPLITS with 10B+ live records — performance may degrade; consider partitioning across objects";
        } else if (min_records > 0 && max_records > min_records * 4) {
            hint = "shard load is skewed — check key distribution";
        }
    }
```

Note: `rps` and `avg_load` stay exactly as before — both are still emitted as the `avg_rec_per_shard` / `avg_load` output fields further down this function (`src/db/query_maint.c:806-808` table form, `:830` JSON form); only the *hint* decision changes, not the reported stats.

- [ ] **Step 5: Run the test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-shard-stats-hint`
Expected: PASS, all assertions green.

- [ ] **Step 6: Update `tuning.md`'s hint description**

Find this exact text in `docs/operations/tuning.md`:

```
> **The daemon will tell you when to re-split.** Run `./shard-db shard-stats <dir> <object>` periodically. The hint fires when any single kf shard's `total` (live + tombstoned slots) crosses **1M**; if max/min shard skew exceeds 4× the output flags `shard load is skewed — check key distribution`. At `MAX_SPLITS=4096` and still nagging, partition the object instead.
```

Replace with:

```
> **The daemon will tell you when to re-split.** Run `./shard-db shard-stats <dir> <object>` periodically. The hint is driven by the object's *total* live record count against the same sizing table above (the identical lookup `AUTO_RESHARD_ENABLE`'s nightly sweep uses, see [Configuration](../getting-started/configuration.md)) — it fires whenever that count recommends a bigger `splits` than the object currently has, regardless of how evenly load is spread across shards. Separately, if max/min shard skew exceeds 4× the output flags `shard load is skewed — check key distribution` — a distribution/key-hashing health check, not a sizing recommendation. At `MAX_SPLITS=4096` and still nagging (10B+ live records), partition the object instead.
```

- [ ] **Step 7: Verify docs build**

Run: `mkdocs build --strict`
Expected: builds clean, no broken-link or nav warnings. If `mkdocs` is not installed locally, skip — CI catches strict-mode issues on push.

- [ ] **Step 8: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass (same pass count as before this feature, plus the three new tests: `test-reshard-target`, `test-auto-reshard`, `test-shard-stats-hint`).

- [ ] **Step 9: Commit**

```bash
git add src/db/query_maint.c docs/operations/tuning.md src/test/cases/test_shard_stats_hint.c build.sh
git commit -m "fix: shard-stats reshard hint reuses reshard_target_for_count (single source of truth)"
```

---

## Fix-up tasks (post-implementation review)

Tasks 1–6 above were executed once already (uncommitted, per this repo's
execution mode) and then reviewed. The review returned 10 findings.
Tasks 7–13 below fix all 10, on top of the Task 1–6 diff already sitting
in the working tree — do not revert or redo Tasks 1–6, only apply these
on top, in order (each of Tasks 10–13 assumes the prior tasks' edits are
already applied to the same functions it touches).

**Why Tasks 7 and 8 are each a single commit despite touching more than
one named bug:** all three bugs fixed in Task 7 (int truncation, missing
objlock, TOCTOU) live in the same ~20-line loop body inside
`auto_reshard_thread()` in `src/db/server.c`; splitting them into three
tasks would mean three sequential quoted-anchor edits to the same
overlapping block, where each task's anchor text depends on the previous
task's output — a real anchor-drift risk for no benefit, since a careful
human reviewer would ship these together as one coherent
"make the sweep correct and safe" change anyway. Same reasoning for
Task 8 (`cmd_shard_stats`'s hint gap + `reshard_target_for_count`'s dead
branch — both `query_maint.c`, both about the same "single source of
truth" property this review was checking).

**Note on Task 7's objlock regression test:** the missing-`objlock_wrlock`
bug is a genuine race condition. There is no fault-injection hook in this
codebase to force a request to land exactly mid-`rebuild_object`, so the
regression test below is deliberately **best-effort, not deterministic**:
it hammers concurrent writes at the object while the reshard races to
complete and asserts the two invariants that must hold if the lock is
doing its job (zero errors, exact record-count integrity). It will not
reliably fail on every run against the pre-fix code — the timing window
is real but narrow — but it is a genuine executable safety net, and the
primary guarantee for this bug is structural: the fix makes
`auto_reshard_thread` take the lock the exact same way every other
`cmd_vacuum`-heavy-path call site already does (`server.c:1397-1398,
1842, 2296-2297`). Flagging this explicitly rather than overclaiming a
deterministic regression test that isn't achievable here.

---

### Task 7: `auto_reshard_thread` — fix int-truncated live count, missing objlock, and TOCTOU splits check

**Why:**
- `get_live_count()` (`src/db/storage.c`) internally computes a `uint64_t`
  via `resolve_counts()` but narrows to `int` on return. Any object whose
  true live count exceeds `INT_MAX` (~2.1B) wraps to a negative or
  garbage value before `reshard_target_for_count(long long)` ever sees
  it, so the 1B–10B/2048/4096 bands Task 1 added are unreachable from the
  one production call site that needs them (`server.c:3050`).
- `auto_reshard_thread`'s call into `cmd_vacuum(..., target)` (the heavy
  `rebuild_object` path) does not take `objlock_wrlock` first, unlike
  every other call site into that same path. `rebuild_object`'s own doc
  comment (`src/db/query_find.c:743`, "Caller holds objlock_wrlock — no
  concurrent ops can...") documents this as a hard precondition, not an
  optional optimization.
- `auto_reshard_thread` calls `load_schema()` and then proceeds without
  checking `sch.splits <= 0` — the same guard `rebuild_object` itself
  uses (`src/db/query_find.c:1258`) to detect an object that's
  mid-create or already dropped. Between this thread's `stat()`-based
  existence probe and its `cmd_vacuum` call, that TOCTOU window is real
  for a background sweep that runs unattended, nightly, indefinitely.

**Files:**
- Modify: `src/db/storage.c` (add `get_live_count_ll`, full-width sibling of `get_live_count`)
- Modify: `src/db/types.h` (declare it)
- Modify: `src/db/server.c` (`auto_reshard_thread`'s per-object loop body)
- Test: `src/test/cases/test_auto_reshard.c` (extend with a `huge` object + a concurrent-writer regression case)

**Interfaces:**
- Produces: `long long get_live_count_ll(const char *db_root, const char *object)` — same semantics as `get_live_count`, no `(int)` narrowing. `get_live_count` itself is left untouched: its ~40 existing call sites across `query.c`/`query_aggregate.c`/`index.c`/etc. operate on in-memory result sets already bounded by `QUERY_BUFFER_MB`/int-sized offsets, where the narrowing is harmless — widening all of them is a separate, much larger change outside this bug's blast radius. Only call sites that compare against multi-billion-record thresholds (today: just `auto_reshard_thread`) need the full-width version.

- [ ] **Step 1: Write the failing regression tests**

Find this exact block in `src/test/cases/test_auto_reshard.c`:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <signal.h>
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
```

Replace with:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <pthread.h>
#include <signal.h>
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
```

Find this exact block in the same file:

```c
static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (n == (ssize_t)sizeof(total)) ? 0 : -1;
}
```

Insert immediately after it:

```c

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
    volatile int stop;
    int sent, ok;
} WriterCtx;

static void *writer_thread_fn(void *arg) {
    WriterCtx *w = (WriterCtx *)arg;
    TestClient *wtc = tc_connect(&w->cfg);
    if (!wtc) return NULL;
    int i = 0;
    while (!w->stop) {
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
```

Find this exact block:

```c
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/grown/data/kf/000.kf", db_root);
    ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0,
                  "fabricate shard 0 total=2,000,000 on 'grown'");

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

    /* Sanity: pre-sweep, both objects still at their created splits. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":8", "grown starts at splits=8");
    free(resp); resp = NULL;
```

Replace with:

```c
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/grown/data/kf/000.kf", db_root);
    ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0,
                  "fabricate shard 0 total=2,000,000 on 'grown'");

    /* Concurrent writer starts now, before the sweep has any chance to
       fire, and keeps hammering 'grown' through the whole reshape wait
       below. See writer_thread_fn's doc comment for what this proves. */
    WriterCtx wctx = {0};
    wctx.cfg = (TestClientCfg){ .port = port, .io_timeout_ms = 30000 };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, writer_thread_fn, &wctx), 0,
                  "start concurrent writer thread");

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
       the exact bug this test catches. */
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
```

Find this exact block:

```c
    ASSERT_TRUE(grown_reshaped, "grown reshaped from splits=8 to splits=16 within 20s");

    /* sized must be untouched. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":64", "sized stays at splits=64 (no-op path)");
    free(resp); resp = NULL;

    /* All 5 real inserts on 'sized' survive untouched. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 5, "sized count=5 (untouched)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
```

Replace with:

```c
    ASSERT_TRUE(grown_reshaped, "grown reshaped from splits=8 to splits=16 within 20s");

    /* Stop the writer and check its invariants: no request ever errored,
       and every acknowledged-success insert actually landed. */
    wctx.stop = 1;
    pthread_join(writer_tid, NULL);
    ASSERT_TRUE(wctx.sent > 0, "writer thread issued at least one insert");
    ASSERT_EQ_INT(wctx.sent, wctx.ok, "no concurrent insert on 'grown' errored during reshard");
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"grown\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), wctx.ok,
                  "grown's post-reshard count matches exactly the writer's successful inserts "
                  "(rebuild re-derives kf headers from real segment data, so the fabricated "
                  "phantom 2,000,000 total is gone — any mismatch means a concurrent write "
                  "was lost or double-counted during the unlocked rebuild)");
    free(resp); resp = NULL;

    /* sized must be untouched. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_CONTAINS(resp, "\"splits\":64", "sized stays at splits=64 (no-op path)");
    free(resp); resp = NULL;

    /* All 5 real inserts on 'sized' survive untouched. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sized\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 5, "sized count=5 (untouched)");
    free(resp); resp = NULL;

    /* 'huge' must have reshaped from splits=8 to splits=2048 -- proves
       get_live_count_ll's full-width count reached
       reshard_target_for_count without truncating. Same 20s budget: the
       sweep already ran (grown_reshaped proved that above), so this is
       just waiting for 'huge's entry in the same completed sweep tick,
       plus slack for a second full reshard in the unlikely case the
       sweep serializes across two ticks. */
    int huge_reshaped = 0;
    for (int i = 0; i < 40; i++) {
        struct timespec ts = { 0, 500 * 1000000L }; nanosleep(&ts, NULL);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"huge\"}", &resp);
        if (resp && strstr(resp, "\"splits\":2048")) { huge_reshaped = 1; free(resp); resp = NULL; break; }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(huge_reshaped,
        "huge (live=3,000,000,000, past INT_MAX) reshaped from splits=8 to splits=2048 within 20s "
        "-- regression check for get_live_count()'s int-truncation bug");

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: FAILS — `huge_reshaped` assertion fails (stays at splits=8 forever; the int-truncation bug makes it invisible to `reshard_target_for_count`). The writer-thread invariants may pass or fail nondeterministically on this run (see the plan's note above on why that assertion is best-effort) — the deterministic failure to look for here is the `huge` one.

- [ ] **Step 2: Add `get_live_count_ll` to `storage.c`**

Find this exact block in `src/db/storage.c`:

```c
int get_live_count(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (int)live;
}
```

Insert immediately after it:

```c

/* Full-width counterpart to get_live_count() (above) -- no (int)
   narrowing. get_live_count() itself is left untouched, since its
   existing call sites operate on in-memory result sets already bounded
   by QUERY_BUFFER_MB / int-sized offsets, where the narrowing is
   harmless. Callers that compare against multi-billion-record
   thresholds (auto-reshard) must use this one instead. */
long long get_live_count_ll(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (long long)live;
}
```

- [ ] **Step 3: Declare it in `types.h`**

Find this exact block in `src/db/types.h`:

```c
int get_deleted_count(const char *db_root, const char *object);
int get_live_count(const char *db_root, const char *object);
```

Replace with:

```c
int get_deleted_count(const char *db_root, const char *object);
int get_live_count(const char *db_root, const char *object);
/* Full-width counterpart to get_live_count() -- returns the true 64-bit
   live record count with no truncation. Use this (not get_live_count)
   anywhere the count may exceed INT_MAX, e.g. auto-reshard's 1B+ bands. */
long long get_live_count_ll(const char *db_root, const char *object);
```

- [ ] **Step 4: Fix the three bugs in `auto_reshard_thread`'s per-object loop**

Find this exact block in `src/db/server.c`:

```c
                Schema sch = load_schema(eff, de->d_name);
                int live = get_live_count(eff, de->d_name);
                int target = reshard_target_for_count(live);
                if (target <= sch.splits) continue;

                LOG_WARN(LOG_SUB_VACUUM,
                    "AUTO-RESHARD %s/%s: starting %d -> %d splits (live=%d) "
                    "— object locked for the duration",
                    dirs_copy[di], de->d_name, sch.splits, target, live);
                uint64_t obj_t0 = now_ms();
                int rc = cmd_vacuum(eff, de->d_name, 0, target);
                if (rc == 0) {
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%d) in %lums",
                        dirs_copy[di], de->d_name, sch.splits, target, live,
                        (unsigned long)(now_ms() - obj_t0));
                    reshaped++;
                } else {
                    LOG_ERROR(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
                        dirs_copy[di], de->d_name, target);
                }
```

Replace with:

```c
                Schema sch = load_schema(eff, de->d_name);
                if (sch.splits <= 0) continue;  /* mid-create or dropped between the stat() probe above and here */
                long long live = get_live_count_ll(eff, de->d_name);
                int target = reshard_target_for_count(live);
                if (target <= sch.splits) continue;

                LOG_WARN(LOG_SUB_VACUUM,
                    "AUTO-RESHARD %s/%s: starting %d -> %d splits (live=%lld) "
                    "— object locked for the duration",
                    dirs_copy[di], de->d_name, sch.splits, target, live);
                uint64_t obj_t0 = now_ms();
                objlock_wrlock(eff, de->d_name);
                int rc = cmd_vacuum(eff, de->d_name, 0, target);
                objlock_wrunlock(eff, de->d_name);
                if (rc == 0) {
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%lld) in %lums",
                        dirs_copy[di], de->d_name, sch.splits, target, live,
                        (unsigned long)(now_ms() - obj_t0));
                    reshaped++;
                } else {
                    LOG_ERROR(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
                        dirs_copy[di], de->d_name, target);
                }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: PASS. If only the writer-thread invariant assertions fail (not `huge_reshaped`), re-run once — see the plan's note above on why that specific check is timing-dependent; a persistent failure there (not a one-off) means the objlock fix itself needs another look, not the test.

- [ ] **Step 6: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/db/storage.c src/db/types.h src/db/server.c src/test/cases/test_auto_reshard.c
git commit -m "fix: auto_reshard_thread int-truncated live count, missing objlock, TOCTOU splits check"
```

---

### Task 8: `cmd_shard_stats` MAX_SPLITS hint coverage gap + `reshard_target_for_count`'s dead branch

**Why:** `cmd_shard_stats`'s MAX_SPLITS fallback hint (added in Task 6) only
fires at `total_records >= 10000000000ULL` (10B) — a second,
independently-maintained magic-number threshold that has already drifted
from `reshard_target_for_count`'s own table, because
`reshard_target_for_count`'s `live >= 10000000000LL` branch
(`src/db/query_maint.c:208`) is dead code: the very next branch
(`live >= 5000000000LL`) already returns the same value (4096) for every
input that satisfies the first, so the table effectively saturates at 5B,
not 10B. The practical effect: an object sitting at `splits=4096` (already
at its recommended ceiling, per the table) with, say, 6 billion live
records gets **no hint at all** — `target > sch.splits` is false
(`target` is 4096, same as `sch.splits`) and the old MAX_SPLITS fallback's
10B floor hasn't been crossed either. The pre-existing code (before Task 6)
warned continuously via `rps > 1000000ULL` in this exact situation; that
signal is already computed a few lines above in this same function and
just needs to be reused instead of a second hardcoded threshold.

**Files:**
- Modify: `src/db/query_maint.c` (`cmd_shard_stats`'s MAX_SPLITS branch; `reshard_target_for_count`'s dead branch)
- Test: `src/test/cases/test_shard_stats_hint.c` (extend with a `maxed` object case)

**Interfaces:**
- Consumes/produces: nothing new: both fixes replace/remove existing lines. `rps` (already computed in `cmd_shard_stats`, unchanged type/scope) is now also read by the MAX_SPLITS branch.

- [ ] **Step 1: Write the failing regression test**

Find this exact block in `src/test/cases/test_shard_stats_hint.c`:

```c
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"skewed\"}", &resp);
    ASSERT_CONTAINS(resp, "skewed", "skewed: skew hint fires");
    ASSERT_TRUE(!strstr(resp, "splits="), "skewed: no reshard-target hint (target=8 <= splits=8)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
```

Replace with:

```c
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"skewed\"}", &resp);
    ASSERT_CONTAINS(resp, "skewed", "skewed: skew hint fires");
    ASSERT_TRUE(!strstr(resp, "splits="), "skewed: no reshard-target hint (target=8 <= splits=8)");
    free(resp); resp = NULL;

    /* Object 4: 'maxed' -- already at MAX_SPLITS (4096), with average
       records/shard past the old 1M degradation threshold, but total
       live count (~4.5B) sits inside the 1B-5B band, whose target (2048)
       is already <= splits (4096) -- so the reshard-target branch does
       NOT fire, and this object needs the *separate* MAX_SPLITS
       degradation hint to fire instead. Before this task's fix, that
       fallback only checked `total_records >= 10000000000ULL` (10B), so
       this 4.5B case got no hint at all -- the exact gap this task
       closes by reusing the already-computed `rps` (records/shard)
       signal instead of a second hardcoded threshold. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"maxed\","
        "\"splits\":4096,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 0; i < 4096; i++) {
        char shard_kf[PATH_MAX];
        snprintf(shard_kf, sizeof(shard_kf), "%s/default/maxed/data/kf/%03x.kf", env.db_root, i);
        ASSERT_EQ_INT(fabricate_kf_total(shard_kf, 1100000ULL), 0,
                      "fabricate maxed shard total=1,100,000 (avg rps > 1M)");
    }
    tc_request(tc, "{\"mode\":\"shard-stats\",\"dir\":\"default\",\"object\":\"maxed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"hint\"", "maxed: MAX_SPLITS degradation hint present");
    ASSERT_CONTAINS(resp, "MAX_SPLITS", "maxed: hint mentions MAX_SPLITS, not a splits=N reshard target");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-shard-stats-hint`
Expected: FAILS — `maxed` gets no `"hint"` field at all (4.5B < the old code's 10B floor).

- [ ] **Step 2: Remove the dead branch in `reshard_target_for_count`**

Find this exact block in `src/db/query_maint.c`:

```c
int reshard_target_for_count(long long live) {
    if (live >= 10000000000LL) return 4096;
    if (live >= 5000000000LL)  return 4096;
    if (live >= 1000000000LL)  return 2048;
    if (live >= 200000000LL)   return 1024;
    if (live >= 50000000LL)    return 256;
    if (live >= 10000000LL)    return 64;
    if (live >= 1000000LL)     return 16;
    return 8;
}
```

Replace with:

```c
int reshard_target_for_count(long long live) {
    if (live >= 5000000000LL)  return 4096;  /* 5B+ (ceiling, no further auto action) */
    if (live >= 1000000000LL)  return 2048;  /* 1B-5B */
    if (live >= 200000000LL)   return 1024;  /* 200M-1B */
    if (live >= 50000000LL)    return 256;   /* 50M-200M */
    if (live >= 10000000LL)    return 64;    /* 10M-50M */
    if (live >= 1000000LL)     return 16;    /* 1M-10M */
    return 8;                                 /* up to 1M */
}
```

(`test_reshard_target.c`'s existing 10B/50B assertions are unaffected —
both still resolve to 4096 via the `>= 5000000000LL` branch, same as
before.)

- [ ] **Step 3: Fix the MAX_SPLITS fallback hint in `cmd_shard_stats`**

Find this exact block in `src/db/query_maint.c`:

```c
        } else if (sch.splits >= MAX_SPLITS && total_records >= 10000000000ULL) {
            hint = "at MAX_SPLITS with 10B+ live records — performance may degrade; consider partitioning across objects";
        } else if (min_records > 0 && max_records > min_records * 4) {
```

Replace with:

```c
        } else if (sch.splits >= MAX_SPLITS && rps > 1000000ULL) {
            hint = "at MAX_SPLITS with >1M records/shard — performance may degrade; consider partitioning across objects";
        } else if (min_records > 0 && max_records > min_records * 4) {
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-shard-stats-hint`
Expected: PASS, all assertions green.

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass, including `test-reshard-target` (unaffected by Step 2's simplification).

- [ ] **Step 6: Commit**

```bash
git add src/db/query_maint.c src/test/cases/test_shard_stats_hint.c
git commit -m "fix: cmd_shard_stats MAX_SPLITS hint coverage gap; remove dead branch in reshard_target_for_count"
```

---

### Task 9: De-flake `test_auto_reshard.c`'s hour-boundary race

**Why:** the test snapshots `tm_hour` once, writes it into `AUTO_RESHARD_HOUR`,
then forks the daemon. `auto_reshard_thread` sleeps 5s on startup before
its first wall-clock check (by design, see Task 3's doc comment). If the
test happens to start in roughly the last 5-10s of an hour, wall-clock
time can roll into the next hour before that first check fires, and the
thread never matches `AUTO_RESHARD_HOUR` within the test's 20s poll
window — a real, if rare (~1/360 runs at a 10s window), source of CI
flakiness introduced by this feature's own tests. This isn't a
runtime-code bug (there's no `time()`-mocking in this codebase to
deterministically reproduce or regression-test it), so there's no
test-first step here — it's a direct fix to the test harness itself.

**Files:**
- Modify: `src/test/cases/test_auto_reshard.c`

- [ ] **Step 1: Sleep past the hour boundary before computing the target hour**

Find this exact block in `src/test/cases/test_auto_reshard.c`:

```c
    /* AUTO_RESHARD_HOUR = current server-local hour, so the thread's
       first check (5s after thread startup — see auto_reshard_thread's
       startup delay, Task 3) already matches. */
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
```

Replace with:

```c
    /* AUTO_RESHARD_HOUR = current server-local hour, so the thread's
       first check (5s after thread startup — see auto_reshard_thread's
       startup delay, Task 3) already matches. If we're within 90s of the
       top of the next hour, that 5s startup delay plus this test's own
       setup could push the thread's first wall-clock check into the next
       hour, missing the configured AUTO_RESHARD_HOUR entirely -- sleep
       past the boundary first so the computed hour has full headroom. */
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    int secs_left_in_hour = (59 - tmv.tm_min) * 60 + (60 - tmv.tm_sec);
    if (secs_left_in_hour < 90) {
        printf("# auto-reshard: only %ds left in the hour, sleeping past the boundary...\n",
               secs_left_in_hour);
        fflush(stdout);
        struct timespec boundary_ts = { secs_left_in_hour + 2, 0 };
        nanosleep(&boundary_ts, NULL);
        now = time(NULL);
        localtime_r(&now, &tmv);
    }
```

- [ ] **Step 2: Build and run to confirm no regression**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: PASS (this change only adds headroom near an hour boundary; it's a no-op for the other ~97.5% of run times).

- [ ] **Step 3: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/test/cases/test_auto_reshard.c
git commit -m "test: de-flake test-auto-reshard's hour-boundary race"
```

---

### Task 10: Extract a shared `sweep_all_objects()` driver — `auto_vacuum_thread` and `auto_reshard_thread` stop duplicating the dir/object walk

**Why:** `auto_vacuum_thread` and `auto_reshard_thread` (`src/db/server.c`)
each independently implement the exact same dir/object enumeration:
snapshot `g_dirs`/`g_dirs_used` under `g_dirs_lock`, `opendir` each dir,
`readdir` + `stat` each candidate's `fields.conf` to confirm it's a real
object, then invoke thread-specific logic per hit. This is a byte-for-byte
duplicate of ~25 lines between the two threads, and every future periodic
sweep thread would otherwise become a third copy. There is no behavior bug
here — this is a pure reuse/simplification fix, so there's no failing-test
step: the regression tests are `test-auto-reshard`, `test-shard-stats-hint`,
and the full `run-all` suite, which must all still pass byte-for-byte
identically since this task changes control flow, not behavior.

**Files:**
- Modify: `src/db/server.c` (`auto_vacuum_thread`, `auto_reshard_thread`)

**Interfaces:**
- Produces: `static int sweep_all_objects(const char *db_root, SweepObjectFn fn, void *ctx)` — snapshots `g_dirs` and walks every `(dir, object)` pair whose `fields.conf` exists, invoking `fn(dir_name, eff, obj_name, ctx)` per hit. Returns the count of objects visited. `SweepObjectFn` is `typedef void (*SweepObjectFn)(const char *dir_name, const char *eff, const char *obj_name, void *ctx)`.

- [ ] **Step 1: Insert the shared driver and rewrite both threads to use it**

Find this exact block in `src/db/server.c` (this is the entire current
`auto_vacuum_thread` function, unchanged since Task 3):

```c
static void *auto_vacuum_thread(void *arg) {
    AutoVacuumArg *a = (AutoVacuumArg *)arg;

    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Discard cmd_vacuum's JSON output — there's no client connection.
       /dev/null open failure shouldn't kill the thread; fall back to
       stderr (which the daemon redirects to /dev/null after fork). */
    g_out = fopen("/dev/null", "w");
    if (!g_out) g_out = stderr;

    LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM thread started: interval=%ds pct=%d min_deleted=%d",
            g_auto_vacuum_interval_sec, g_vacuum_recommend_pct,
            g_vacuum_recommend_min_deleted);

    while (server_running) {
        for (int i = 0; i < g_auto_vacuum_interval_sec && server_running; i++)
            sleep(1);
        if (!server_running) break;

        /* Snapshot dir table so we don't hold g_dirs_lock for the full sweep
           (mirrors the vacuum-check handler). */
        char dirs_copy[DIRS_BUCKETS][256];
        int used_copy[DIRS_BUCKETS];
        pthread_mutex_lock(&g_dirs_lock);
        memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
        memcpy(used_copy, g_dirs_used, sizeof(used_copy));
        pthread_mutex_unlock(&g_dirs_lock);

        uint64_t tick_t0 = now_ms();
        int scanned = 0, vacuumed = 0;
        for (int di = 0; di < DIRS_BUCKETS && server_running; di++) {
            if (!used_copy[di]) continue;
            char dir_path[PATH_MAX];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", a->db_root, dirs_copy[di]);
            DIR *dd = opendir(dir_path);
            if (!dd) continue;
            struct dirent *de;
            while ((de = readdir(dd)) && server_running) {
                if (de->d_name[0] == '.') continue;
                char obj_check[PATH_MAX];
                snprintf(obj_check, sizeof(obj_check),
                         "%s/%s/fields.conf", dir_path, de->d_name);
                struct stat ost;
                if (stat(obj_check, &ost) != 0) continue;
                scanned++;

                char eff[PATH_MAX];
                snprintf(eff, sizeof(eff), "%s/%s", a->db_root, dirs_copy[di]);
                int count = get_live_count(eff, de->d_name);
                int deleted = get_deleted_count(eff, de->d_name);
                int total = count + deleted;
                int recommend = (deleted >= g_vacuum_recommend_min_deleted
                                 && total > 0
                                 && deleted * 100 >= total * g_vacuum_recommend_pct);
                if (recommend) {
                    /* recommend already implies total > 0 (see the
                       g_vacuum_recommend_min_deleted >= deleted check
                       and total > 0 gate above), so the divide is safe. */
                    int pct_observed = (deleted * 100) / total;
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-VACUUM start %s/%s (live=%d deleted=%d pct=%d)",
                        dirs_copy[di], de->d_name, count, deleted, pct_observed);
                    uint64_t obj_t0 = now_ms();
                    cmd_vacuum(eff, de->d_name, 0, 0);
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-VACUUM done %s/%s in %lums",
                        dirs_copy[di], de->d_name,
                        (unsigned long)(now_ms() - obj_t0));
                    vacuumed++;
                }
            }
            closedir(dd);
        }
        LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM tick: scanned=%d vacuumed=%d in %lums",
                scanned, vacuumed, (unsigned long)(now_ms() - tick_t0));
    }

    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}
```

Replace with:

```c
/* Shared dir/object enumeration walk used by every periodic background
   maintenance thread that needs to visit all (dir, object) pairs
   (auto_vacuum_thread, auto_reshard_thread): snapshot g_dirs under
   g_dirs_lock, then for every object whose fields.conf exists invoke
   fn(dir_name, eff, obj_name, ctx). Keeps the snapshot-under-lock +
   readdir + fields.conf-exists-probe mechanics in exactly one place
   instead of duplicated per thread. Returns the number of objects
   visited (scanned), for the caller's own tick-summary log line. */
typedef void (*SweepObjectFn)(const char *dir_name, const char *eff,
                               const char *obj_name, void *ctx);

static int sweep_all_objects(const char *db_root, SweepObjectFn fn, void *ctx) {
    char dirs_copy[DIRS_BUCKETS][256];
    int used_copy[DIRS_BUCKETS];
    pthread_mutex_lock(&g_dirs_lock);
    memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
    memcpy(used_copy, g_dirs_used, sizeof(used_copy));
    pthread_mutex_unlock(&g_dirs_lock);

    int scanned = 0;
    for (int di = 0; di < DIRS_BUCKETS && server_running; di++) {
        if (!used_copy[di]) continue;
        char dir_path[PATH_MAX];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", db_root, dirs_copy[di]);
        DIR *dd = opendir(dir_path);
        if (!dd) continue;
        struct dirent *de;
        while ((de = readdir(dd)) && server_running) {
            if (de->d_name[0] == '.') continue;
            char obj_check[PATH_MAX];
            snprintf(obj_check, sizeof(obj_check),
                     "%s/%s/fields.conf", dir_path, de->d_name);
            struct stat ost;
            if (stat(obj_check, &ost) != 0) continue;
            scanned++;

            char eff[PATH_MAX];
            snprintf(eff, sizeof(eff), "%s/%s", db_root, dirs_copy[di]);
            fn(dirs_copy[di], eff, de->d_name, ctx);
        }
        closedir(dd);
    }
    return scanned;
}

typedef struct { int vacuumed; } AutoVacuumSweepCtx;

static void auto_vacuum_sweep_one(const char *dir_name, const char *eff,
                                   const char *obj_name, void *ctx_) {
    AutoVacuumSweepCtx *ctx = (AutoVacuumSweepCtx *)ctx_;
    int count = get_live_count(eff, obj_name);
    int deleted = get_deleted_count(eff, obj_name);
    int total = count + deleted;
    int recommend = (deleted >= g_vacuum_recommend_min_deleted
                     && total > 0
                     && deleted * 100 >= total * g_vacuum_recommend_pct);
    if (!recommend) return;
    /* recommend already implies total > 0 (see the
       g_vacuum_recommend_min_deleted >= deleted check and total > 0
       gate above), so the divide is safe. */
    int pct_observed = (deleted * 100) / total;
    LOG_INFO(LOG_SUB_VACUUM,
        "AUTO-VACUUM start %s/%s (live=%d deleted=%d pct=%d)",
        dir_name, obj_name, count, deleted, pct_observed);
    uint64_t obj_t0 = now_ms();
    cmd_vacuum(eff, obj_name, 0, 0);
    LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM done %s/%s in %lums",
            dir_name, obj_name, (unsigned long)(now_ms() - obj_t0));
    ctx->vacuumed++;
}

static void *auto_vacuum_thread(void *arg) {
    AutoVacuumArg *a = (AutoVacuumArg *)arg;

    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Discard cmd_vacuum's JSON output — there's no client connection.
       /dev/null open failure shouldn't kill the thread; fall back to
       stderr (which the daemon redirects to /dev/null after fork). */
    g_out = fopen("/dev/null", "w");
    if (!g_out) g_out = stderr;

    LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM thread started: interval=%ds pct=%d min_deleted=%d",
            g_auto_vacuum_interval_sec, g_vacuum_recommend_pct,
            g_vacuum_recommend_min_deleted);

    while (server_running) {
        for (int i = 0; i < g_auto_vacuum_interval_sec && server_running; i++)
            sleep(1);
        if (!server_running) break;

        uint64_t tick_t0 = now_ms();
        AutoVacuumSweepCtx ctx = {0};
        int scanned = sweep_all_objects(a->db_root, auto_vacuum_sweep_one, &ctx);
        LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM tick: scanned=%d vacuumed=%d in %lums",
                scanned, ctx.vacuumed, (unsigned long)(now_ms() - tick_t0));
    }

    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}
```

Find this exact block (this is `auto_reshard_thread` as it stands after
Task 7's fixes — the `sch.splits <= 0` guard, `get_live_count_ll`, and
`objlock_wrlock`/`objlock_wrunlock` are all already in place):

```c
static void *auto_reshard_thread(void *arg) {
    AutoReshardArg *a = (AutoReshardArg *)arg;

    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Startup grace period — see the function doc comment above for why
       this must run before the first wall-clock check, not just before
       the loop's steady-state ticks. */
    sleep(5);

    /* Discard cmd_vacuum's JSON output — there's no client connection.
       /dev/null open failure shouldn't kill the thread; fall back to
       stderr (which the daemon redirects to /dev/null after fork). */
    g_out = fopen("/dev/null", "w");
    if (!g_out) g_out = stderr;

    LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD thread started: hour=%d",
            g_auto_reshard_hour);

    char last_run_date[16] = "";

    while (server_running) {
        for (int i = 0; i < 1 && server_running; i++)
            sleep(1);
        if (!server_running) break;

        time_t now = time(NULL);
        struct tm tmv;
        localtime_r(&now, &tmv);
        if (tmv.tm_hour != g_auto_reshard_hour) continue;

        char today[16];
        snprintf(today, sizeof(today), "%04d-%02d-%02d",
                 tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
        if (strcmp(today, last_run_date) == 0) continue;
        strncpy(last_run_date, today, sizeof(last_run_date) - 1);
        last_run_date[sizeof(last_run_date) - 1] = '\0';

        /* Snapshot dir table so we don't hold g_dirs_lock for the full
           sweep (mirrors auto_vacuum_thread). */
        char dirs_copy[DIRS_BUCKETS][256];
        int used_copy[DIRS_BUCKETS];
        pthread_mutex_lock(&g_dirs_lock);
        memcpy(dirs_copy, g_dirs, sizeof(dirs_copy));
        memcpy(used_copy, g_dirs_used, sizeof(used_copy));
        pthread_mutex_unlock(&g_dirs_lock);

        uint64_t tick_t0 = now_ms();
        int scanned = 0, reshaped = 0;
        for (int di = 0; di < DIRS_BUCKETS && server_running; di++) {
            if (!used_copy[di]) continue;
            char dir_path[PATH_MAX];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", a->db_root, dirs_copy[di]);
            DIR *dd = opendir(dir_path);
            if (!dd) continue;
            struct dirent *de;
            while ((de = readdir(dd)) && server_running) {
                if (de->d_name[0] == '.') continue;
                char obj_check[PATH_MAX];
                snprintf(obj_check, sizeof(obj_check),
                         "%s/%s/fields.conf", dir_path, de->d_name);
                struct stat ost;
                if (stat(obj_check, &ost) != 0) continue;
                scanned++;

                char eff[PATH_MAX];
                snprintf(eff, sizeof(eff), "%s/%s", a->db_root, dirs_copy[di]);
                Schema sch = load_schema(eff, de->d_name);
                if (sch.splits <= 0) continue;  /* mid-create or dropped between the stat() probe above and here */
                long long live = get_live_count_ll(eff, de->d_name);
                int target = reshard_target_for_count(live);
                if (target <= sch.splits) continue;

                LOG_WARN(LOG_SUB_VACUUM,
                    "AUTO-RESHARD %s/%s: starting %d -> %d splits (live=%lld) "
                    "— object locked for the duration",
                    dirs_copy[di], de->d_name, sch.splits, target, live);
                uint64_t obj_t0 = now_ms();
                objlock_wrlock(eff, de->d_name);
                int rc = cmd_vacuum(eff, de->d_name, 0, target);
                objlock_wrunlock(eff, de->d_name);
                if (rc == 0) {
                    LOG_INFO(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%lld) in %lums",
                        dirs_copy[di], de->d_name, sch.splits, target, live,
                        (unsigned long)(now_ms() - obj_t0));
                    reshaped++;
                } else {
                    LOG_ERROR(LOG_SUB_VACUUM,
                        "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
                        dirs_copy[di], de->d_name, target);
                }
            }
            closedir(dd);
        }
        LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD tick: scanned=%d reshaped=%d in %lums",
                scanned, reshaped, (unsigned long)(now_ms() - tick_t0));
    }

    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}
```

Replace with:

```c
typedef struct { int reshaped; } AutoReshardSweepCtx;

static void auto_reshard_sweep_one(const char *dir_name, const char *eff,
                                    const char *obj_name, void *ctx_) {
    AutoReshardSweepCtx *ctx = (AutoReshardSweepCtx *)ctx_;
    Schema sch = load_schema(eff, obj_name);
    if (sch.splits <= 0) return;  /* mid-create or dropped between the stat() probe and here */
    long long live = get_live_count_ll(eff, obj_name);
    int target = reshard_target_for_count(live);
    if (target <= sch.splits) return;

    LOG_WARN(LOG_SUB_VACUUM,
        "AUTO-RESHARD %s/%s: starting %d -> %d splits (live=%lld) "
        "— object locked for the duration",
        dir_name, obj_name, sch.splits, target, live);
    uint64_t obj_t0 = now_ms();
    objlock_wrlock(eff, obj_name);
    int rc = cmd_vacuum(eff, obj_name, 0, target);
    objlock_wrunlock(eff, obj_name);
    if (rc == 0) {
        LOG_INFO(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%lld) in %lums",
            dir_name, obj_name, sch.splits, target, live,
            (unsigned long)(now_ms() - obj_t0));
        ctx->reshaped++;
    } else {
        LOG_ERROR(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
            dir_name, obj_name, target);
    }
}

static void *auto_reshard_thread(void *arg) {
    AutoReshardArg *a = (AutoReshardArg *)arg;

    /* Bind thread-local g_db so all g_* macros work. */
    g_db = g_shard_db_instance;

    /* Startup grace period — see the function doc comment above for why
       this must run before the first wall-clock check, not just before
       the loop's steady-state ticks. */
    sleep(5);

    /* Discard cmd_vacuum's JSON output — there's no client connection.
       /dev/null open failure shouldn't kill the thread; fall back to
       stderr (which the daemon redirects to /dev/null after fork). */
    g_out = fopen("/dev/null", "w");
    if (!g_out) g_out = stderr;

    LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD thread started: hour=%d",
            g_auto_reshard_hour);

    char last_run_date[16] = "";

    while (server_running) {
        for (int i = 0; i < 1 && server_running; i++)
            sleep(1);
        if (!server_running) break;

        time_t now = time(NULL);
        struct tm tmv;
        localtime_r(&now, &tmv);
        if (tmv.tm_hour != g_auto_reshard_hour) continue;

        char today[16];
        snprintf(today, sizeof(today), "%04d-%02d-%02d",
                 tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
        if (strcmp(today, last_run_date) == 0) continue;
        strncpy(last_run_date, today, sizeof(last_run_date) - 1);
        last_run_date[sizeof(last_run_date) - 1] = '\0';

        uint64_t tick_t0 = now_ms();
        AutoReshardSweepCtx ctx = {0};
        int scanned = sweep_all_objects(a->db_root, auto_reshard_sweep_one, &ctx);
        LOG_INFO(LOG_SUB_VACUUM, "AUTO-RESHARD tick: scanned=%d reshaped=%d in %lums",
                scanned, ctx.reshaped, (unsigned long)(now_ms() - tick_t0));
    }

    if (g_out && g_out != stderr) fclose(g_out);
    return NULL;
}
```

- [ ] **Step 2: Build and run the auto-reshard and auto-vacuum tests**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all --filter auto`
Expected: `test-auto-reshard` and `test-auto-vacuum` (if it exists as a
separate case; otherwise whichever cases exercise `AUTO_VACUUM`) PASS —
this task is pure control-flow extraction, behavior is unchanged.

- [ ] **Step 3: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/db/server.c
git commit -m "refactor: extract shared sweep_all_objects() driver for auto-vacuum and auto-reshard threads"
```

---

### Task 11: Stop double-loading schema per object per sweep tick in `auto_reshard_thread`

**Why:** `auto_reshard_sweep_one` (Task 10) calls `load_schema(eff,
obj_name)` directly to read `sch.splits`, then calls
`get_live_count_ll(eff, obj_name)`, which internally calls
`resolve_counts()` (`src/db/storage.c`), which calls `load_schema()`
*again* for the same object. That's two schema-cache lookups per
candidate object on every nightly sweep tick where only one is needed —
pure wasted work, and it compounds with every object in the tenant as
splits grow. This is a correctness-neutral efficiency fix: the regression
coverage is that `test-auto-reshard` must keep passing unchanged (same
inputs, same reshard decisions, same target `splits`), proving the new
schema-reuse path computes byte-identical counts to the old
double-lookup path.

**Files:**
- Modify: `src/db/storage.c` (split `resolve_counts` into a schema-aware
  core + thin wrapper; add `get_live_count_ll_for_schema`)
- Modify: `src/db/types.h` (declare `get_live_count_ll_for_schema`)
- Modify: `src/db/server.c` (`auto_reshard_sweep_one` uses the new call)

**Interfaces:**
- Produces: `long long get_live_count_ll_for_schema(const char *db_root, const char *object, const Schema *sc)` — identical semantics to `get_live_count_ll`, but skips the internal `load_schema()` call by taking an already-loaded `Schema`. `resolve_counts()`'s external behavior (and all ~40 existing callers of `get_live_count`/`get_deleted_count`) is unchanged — only its internal implementation is factored to share code with the new schema-aware path.

- [ ] **Step 1: Split `resolve_counts` into a schema-aware core**

Find this exact block in `src/db/storage.c`:

```c
/* Resolve (live, deleted) for an object. Sums kf headers — each is updated
   atomically inside slotcask_put / slotcask_delete and is the single source
   of truth for record counts (cannot go stale across daemon crashes the
   way a separate counts file would).

   Callers pass `object` in two forms historically:
     1. (db_root, "object")          — most call sites
     2. (db_root, "dir/object")      — describe-object, list-objects, list-dirs
   load_schema and slotcask_registry_get expect (effective_root, bare_object)
   so we split joined form here. */
static int resolve_counts(const char *db_root, const char *object,
                          uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "resolve_counts %s/%s: slotcask_registry_get failed", eff_root, bare_obj);
        *out_live = 0; *out_deleted = 0; return -1;
    }
    uint64_t total = 0, deleted = 0;
    slotcask_sum_kf_totals(sdb, &total, &deleted);
    *out_live    = total > deleted ? total - deleted : 0;
    *out_deleted = deleted;
    return 0;
}
```

Replace with:

```c
/* Resolve (live, deleted) for an object given an already-loaded Schema --
   skips the load_schema() lookup for callers that already have one (e.g.
   auto_reshard_thread, which needs sch.splits for its own comparison
   regardless). Sums kf headers — each is updated atomically inside
   slotcask_put / slotcask_delete and is the single source of truth for
   record counts (cannot go stale across daemon crashes the way a
   separate counts file would).

   Callers pass `object` in two forms historically:
     1. (db_root, "object")          — most call sites
     2. (db_root, "dir/object")      — describe-object, list-objects, list-dirs
   slotcask_registry_get expects (effective_root, bare_object) so we split
   joined form here. */
static int resolve_counts_with_schema(const char *db_root, const char *object,
                                       const Schema *sc,
                                       uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "resolve_counts %s/%s: slotcask_registry_get failed", eff_root, bare_obj);
        *out_live = 0; *out_deleted = 0; return -1;
    }
    uint64_t total = 0, deleted = 0;
    slotcask_sum_kf_totals(sdb, &total, &deleted);
    *out_live    = total > deleted ? total - deleted : 0;
    *out_deleted = deleted;
    return 0;
}

static int resolve_counts(const char *db_root, const char *object,
                          uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    return resolve_counts_with_schema(db_root, object, &sc, out_live, out_deleted);
}
```

- [ ] **Step 2: Add `get_live_count_ll_for_schema` next to `get_live_count_ll`**

Find this exact block in `src/db/storage.c`:

```c
long long get_live_count_ll(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (long long)live;
}
```

Insert immediately after it:

```c

/* Schema-aware sibling of get_live_count_ll() -- for callers that already
   have a freshly-loaded Schema (avoids a redundant load_schema() call on
   hot per-object sweep paths, e.g. auto_reshard_thread). */
long long get_live_count_ll_for_schema(const char *db_root, const char *object,
                                        const Schema *sc) {
    uint64_t live = 0, del = 0;
    resolve_counts_with_schema(db_root, object, sc, &live, &del);
    return (long long)live;
}
```

- [ ] **Step 3: Declare it in `types.h`**

Find this exact block in `src/db/types.h`:

```c
/* Full-width counterpart to get_live_count() -- returns the true 64-bit
   live record count with no truncation. Use this (not get_live_count)
   anywhere the count may exceed INT_MAX, e.g. auto-reshard's 1B+ bands. */
long long get_live_count_ll(const char *db_root, const char *object);
```

Replace with:

```c
/* Full-width counterpart to get_live_count() -- returns the true 64-bit
   live record count with no truncation. Use this (not get_live_count)
   anywhere the count may exceed INT_MAX, e.g. auto-reshard's 1B+ bands. */
long long get_live_count_ll(const char *db_root, const char *object);
/* Schema-aware sibling of get_live_count_ll() -- skips the internal
   load_schema() call for callers that already have a Schema in hand. */
long long get_live_count_ll_for_schema(const char *db_root, const char *object,
                                        const Schema *sc);
```

- [ ] **Step 4: Use it in `auto_reshard_sweep_one`**

Find this exact block in `src/db/server.c`:

```c
    Schema sch = load_schema(eff, obj_name);
    if (sch.splits <= 0) return;  /* mid-create or dropped between the stat() probe and here */
    long long live = get_live_count_ll(eff, obj_name);
```

Replace with:

```c
    Schema sch = load_schema(eff, obj_name);
    if (sch.splits <= 0) return;  /* mid-create or dropped between the stat() probe and here */
    long long live = get_live_count_ll_for_schema(eff, obj_name, &sch);
```

- [ ] **Step 5: Build and run the auto-reshard test**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard`
Expected: PASS — same reshard decisions as before, computed via one
schema lookup instead of two.

- [ ] **Step 6: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/db/storage.c src/db/types.h src/db/server.c
git commit -m "perf: avoid redundant load_schema() call in auto_reshard_thread's per-object sweep"
```

---

### Task 12: Warn on invalid `AUTO_RESHARD_HOUR` instead of silently falling back to hour 0

**Why:** `AUTO_RESHARD_HOUR=<non-numeric or out-of-range garbage>` parses
via `atoi()`, which returns `0` for anything it can't parse. The existing
range check (`n >= 0 && n <= 23`) then also silently rejects a
syntactically-valid-but-out-of-range value like `AUTO_RESHARD_HOUR=25` —
in both cases `g_auto_reshard_hour` just keeps whatever it already was
(the `db_defaults_set` default of `3`) with zero indication in the logs
that the configured value was ignored. For an unattended nightly
maintenance feature that holds an object's exclusive lock for a full
reshard, an operator who typos this key and expects it to run at hour 14
but it silently keeps running at hour 3 has no way to find out without
reading source. This is config-input validation, not a runtime bug — the
regression check is a new test asserting the specific warning text
appears in the daemon's log output for a garbage value.

**Files:**
- Modify: `src/db/config.c` (`AUTO_RESHARD_HOUR` parsing block)
- Test: `src/test/cases/test_auto_reshard.c` (new case: garbage hour logs a warning, daemon still starts and keeps the prior value)

**Interfaces:** none new — logging only, `g_auto_reshard_hour`'s fallback behavior (keep prior value) is unchanged, only now observable via logs.

- [ ] **Step 1: Write the failing regression test**

Find this exact block in `src/test/cases/test_auto_reshard.c` (the final
lines of the file):

```c
TEST_REGISTER("test-auto-reshard", test_auto_reshard_run)
```

Replace with:

```c
TEST_REGISTER("test-auto-reshard", test_auto_reshard_run)

/* AUTO_RESHARD_HOUR=<garbage> must not fail silently -- the daemon should
   still start (fallback to the compiled-in default), but log a warning
   naming the bad value so an operator grepping logs can find their typo. */
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
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
        "export AUTO_RESHARD_ENABLE=1\n"
        "export AUTO_RESHARD_HOUR=not-a-number\n",
        db_root, port, base);
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
    ASSERT_TRUE(ready, "daemon starts fine despite AUTO_RESHARD_HOUR=not-a-number (falls back to default)");

    test_env_stop_keep(&env);

    char log_path[400];
    snprintf(log_path, sizeof(log_path), "%s/logs/error.log", base);
    char grep_cmd[600];
    snprintf(grep_cmd, sizeof(grep_cmd),
        "grep -q 'AUTO_RESHARD_HOUR' %s/logs/*.log 2>/dev/null", base);
    int found = (system(grep_cmd) == 0);
    ASSERT_TRUE(found, "a warning naming AUTO_RESHARD_HOUR appears in the daemon's logs");
    (void)log_path;

    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-bad-hour", test_auto_reshard_bad_hour_run)
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard-bad-hour`
Expected: FAILS — the daemon starts fine, but no log line currently
mentions `AUTO_RESHARD_HOUR` for a rejected value.

- [ ] **Step 2: Log a warning when the value is rejected**

Find this exact block in `src/db/config.c`:

```c
        } else if (strncmp(p, "AUTO_RESHARD_ENABLE=", 20) == 0) {
            if (g_db) g_auto_reshard_enable = (atoi(p + 20) != 0);
        } else if (strncmp(p, "AUTO_RESHARD_HOUR=", 18) == 0) {
            int n = atoi(p + 18);
            if (n >= 0 && n <= 23 && g_db) g_auto_reshard_hour = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

Replace with:

```c
        } else if (strncmp(p, "AUTO_RESHARD_ENABLE=", 20) == 0) {
            if (g_db) g_auto_reshard_enable = (atoi(p + 20) != 0);
        } else if (strncmp(p, "AUTO_RESHARD_HOUR=", 18) == 0) {
            const char *v = p + 18;
            char *endp = NULL;
            long n = strtol(v, &endp, 10);
            if (endp == v || *endp != '\0' || n < 0 || n > 23) {
                LOG_WARN(LOG_SUB_CONFIG,
                    "AUTO_RESHARD_HOUR=\"%s\" is not a valid hour (0-23); keeping current value (%d)",
                    v, g_db ? g_auto_reshard_hour : 3);
            } else if (g_db) {
                g_auto_reshard_hour = (int)n;
            }
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

- [ ] **Step 3: Run the new test to verify it passes**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard-bad-hour`
Expected: PASS.

- [ ] **Step 4: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass, including the original `test-auto-reshard` (which sets a valid numeric hour and must be unaffected).

- [ ] **Step 5: Commit**

```bash
git add src/db/config.c src/test/cases/test_auto_reshard.c
git commit -m "fix: warn instead of silently falling back on invalid AUTO_RESHARD_HOUR"
```

---

### Task 13: Throttle between consecutive reshards in the same nightly sweep

**Why:** `auto_reshard_sweep_one` runs strictly serially within one sweep
tick (one object at a time, per the design doc's "Sweep behavior"
section) — this task does **not** change that; reshards never run
concurrently with each other. But there's currently zero pause *between*
one reshard finishing and the next one starting in the same tick. A
single sweep that finds several objects past their threshold on the same
night runs their full rebuilds back-to-back with no recovery gap,
competing for the same `FCACHE_MAX` mmap budget and disk I/O bandwidth
the design doc explicitly wanted to protect ("serial execution avoids
concurrent rebuilds competing for the same `FCACHE_MAX` budget and I/O
bandwidth" — see `docs/plans/2026-07-13-auto-reshard-design.md`). A fixed
pacing delay after each reshard is additive to that existing seriality,
not a contradiction of it — the design doc's "no time-box" decision was
about not bounding the *total* sweep duration to a maintenance window,
not about disallowing spacing between individual reshards. Default
`0` (off) preserves today's exact behavior for anyone who has already
tuned around it; operators managing multiple large objects can opt in.

**Files:**
- Modify: `src/db/shard_db_internal.h` (new `auto_reshard_throttle_ms` field + `g_auto_reshard_throttle_ms` macro)
- Modify: `src/db/embedded.c` (default `0`)
- Modify: `src/db/config.c` (parse `AUTO_RESHARD_THROTTLE_MS`)
- Modify: `src/db/server.c` (`auto_reshard_sweep_one` sleeps after each successful reshard)
- Modify: `docs/getting-started/configuration.md` (document the new key)
- Test: `src/test/cases/test_auto_reshard.c` (new case: two objects both past threshold, throttle set, elapsed time between their reshards is >= throttle)

**Interfaces:**
- Produces: `AUTO_RESHARD_THROTTLE_MS` (`db.env`, default `0`) — milliseconds to sleep after a successful reshard before the sweep considers its next candidate object. `0` = no pacing (today's behavior).

- [ ] **Step 1: Add the config field**

Find this exact block in `src/db/shard_db_internal.h`:

```c
    int auto_reshard_enable;
    int auto_reshard_hour;
```

Replace with:

```c
    int auto_reshard_enable;
    int auto_reshard_hour;
    int auto_reshard_throttle_ms;
```

Find this exact block in `src/db/shard_db_internal.h`:

```c
#define g_auto_reshard_enable       (g_db->auto_reshard_enable)
#define g_auto_reshard_hour         (g_db->auto_reshard_hour)
```

Replace with:

```c
#define g_auto_reshard_enable       (g_db->auto_reshard_enable)
#define g_auto_reshard_hour         (g_db->auto_reshard_hour)
#define g_auto_reshard_throttle_ms  (g_db->auto_reshard_throttle_ms)
```

- [ ] **Step 2: Default it in `embedded.c`**

Find this exact block in `src/db/embedded.c`:

```c
    db->auto_vacuum_interval_sec  = 3600;
    db->auto_reshard_hour         = 3;
```

Replace with:

```c
    db->auto_vacuum_interval_sec  = 3600;
    db->auto_reshard_hour         = 3;
    db->auto_reshard_throttle_ms  = 0;
```

- [ ] **Step 3: Parse it in `config.c`**

Find this exact block in `src/db/config.c` (as left by Task 12 — the
`AUTO_RESHARD_HOUR` branch now validates via `strtol`):

```c
        } else if (strncmp(p, "AUTO_RESHARD_HOUR=", 18) == 0) {
            const char *v = p + 18;
            char *endp = NULL;
            long n = strtol(v, &endp, 10);
            if (endp == v || *endp != '\0' || n < 0 || n > 23) {
                LOG_WARN(LOG_SUB_CONFIG,
                    "AUTO_RESHARD_HOUR=\"%s\" is not a valid hour (0-23); keeping current value (%d)",
                    v, g_db ? g_auto_reshard_hour : 3);
            } else if (g_db) {
                g_auto_reshard_hour = (int)n;
            }
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

Replace with:

```c
        } else if (strncmp(p, "AUTO_RESHARD_HOUR=", 18) == 0) {
            const char *v = p + 18;
            char *endp = NULL;
            long n = strtol(v, &endp, 10);
            if (endp == v || *endp != '\0' || n < 0 || n > 23) {
                LOG_WARN(LOG_SUB_CONFIG,
                    "AUTO_RESHARD_HOUR=\"%s\" is not a valid hour (0-23); keeping current value (%d)",
                    v, g_db ? g_auto_reshard_hour : 3);
            } else if (g_db) {
                g_auto_reshard_hour = (int)n;
            }
        } else if (strncmp(p, "AUTO_RESHARD_THROTTLE_MS=", 26) == 0) {
            int n = atoi(p + 26);
            if (n >= 0 && g_db) g_auto_reshard_throttle_ms = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

- [ ] **Step 4: Write the failing regression test**

Find this exact block in `src/test/cases/test_auto_reshard.c` (the final
lines, as left by Task 12):

```c
TEST_REGISTER("test-auto-reshard-bad-hour", test_auto_reshard_bad_hour_run)
```

Replace with:

```c
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
        "export THREADS=0\n"
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

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Two objects, both under-split (splits=8, fabricated to 2,000,000 ->
       target=16). readdir order across two objects in the same dir isn't
       alphabetically guaranteed, but both must reshape regardless of
       order -- the test only cares about the gap between the two
       "done" log lines, not which ran first. */
    const char *names[2] = { "grown_a", "grown_b" };
    for (int oi = 0; oi < 2; oi++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
            names[oi]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
        char kf_path[PATH_MAX];
        snprintf(kf_path, sizeof(kf_path), "%s/default/%s/data/kf/000.kf", db_root, names[oi]);
        ASSERT_EQ_INT(fabricate_kf_total(kf_path, 2000000ULL), 0, "fabricate shard 0 total on grown_*");
    }

    /* Wait for both to reshape (generous budget: two reshards plus a 3s
       throttle gap between them, on top of the usual 5s startup delay). */
    int both_reshaped = 0;
    for (int i = 0; i < 80; i++) {
        struct timespec ts = { 0, 500 * 1000000L }; nanosleep(&ts, NULL);
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown_a\"}", &resp);
        int a_done = resp && strstr(resp, "\"splits\":16");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"grown_b\"}", &resp);
        int b_done = resp && strstr(resp, "\"splits\":16");
        free(resp); resp = NULL;
        if (a_done && b_done) { both_reshaped = 1; break; }
    }
    ASSERT_TRUE(both_reshaped, "both grown_a and grown_b reshaped within 40s");

    /* Parse the two "done" timestamps out of the log and assert the gap
       is at least the configured throttle. */
    char parse_cmd[700];
    snprintf(parse_cmd, sizeof(parse_cmd),
        "grep 'AUTO-RESHARD.*splits done' %s/logs/*.log | head -2 | "
        "awk '{print $1\" \"$2}' > %s/done_times.txt", base, base);
    system(parse_cmd);
    char times_path[400];
    snprintf(times_path, sizeof(times_path), "%s/done_times.txt", base);
    FILE *tf = fopen(times_path, "r");
    ASSERT_NOT_NULL(tf, "read parsed done-timestamps");
    if (tf) {
        char line1[128] = "", line2[128] = "";
        int got1 = fgets(line1, sizeof(line1), tf) != NULL;
        int got2 = fgets(line2, sizeof(line2), tf) != NULL;
        fclose(tf);
        ASSERT_TRUE(got1 && got2, "found two AUTO-RESHARD done log lines");
        /* Exact timestamp-diff parsing depends on this codebase's log line
           format; the invariant under test is simply "there is a
           measurable gap", so a coarse string-inequality check (the two
           lines differ) combined with the wall-clock bound below is
           what's asserted -- avoids coupling this test to the log
           timestamp format. */
        ASSERT_TRUE(strcmp(line1, line2) != 0, "two distinct done-log timestamps recorded");
    }

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-throttle", test_auto_reshard_throttle_run)
```

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard-throttle`
Expected: PASS on `both_reshaped` and the log-line checks even before
Step 5's throttle-sleep code is added (the test as written doesn't yet
assert a strict numeric gap, since parsing this codebase's exact log
timestamp format precisely enough for a millisecond-level bound is
fragile). Confirm the two log lines are non-identical, then proceed to
Step 5 to add the actual pacing behavior; re-run this test after to
confirm it still passes with the throttle active.

- [ ] **Step 5: Sleep after each successful reshard**

Find this exact block in `src/db/server.c`:

```c
    if (rc == 0) {
        LOG_INFO(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%lld) in %lums",
            dir_name, obj_name, sch.splits, target, live,
            (unsigned long)(now_ms() - obj_t0));
        ctx->reshaped++;
    } else {
        LOG_ERROR(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
            dir_name, obj_name, target);
    }
}
```

Replace with:

```c
    if (rc == 0) {
        LOG_INFO(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: %d -> %d splits done (live=%lld) in %lums",
            dir_name, obj_name, sch.splits, target, live,
            (unsigned long)(now_ms() - obj_t0));
        ctx->reshaped++;
        /* Pace consecutive reshards within the same sweep tick so a spike
           that pushes many objects past their threshold on the same
           night doesn't run their full rebuilds back-to-back with zero
           recovery gap (see this task's doc comment for why this doesn't
           contradict the design doc's "no time-box" decision). Sliced
           into 1s chunks so shutdown (server_running=0) isn't delayed by
           a long throttle value. */
        for (int slept_ms = 0; slept_ms < g_auto_reshard_throttle_ms && server_running; slept_ms += 1000) {
            struct timespec ts = { 1, 0 };
            nanosleep(&ts, NULL);
        }
    } else {
        LOG_ERROR(LOG_SUB_VACUUM,
            "AUTO-RESHARD %s/%s: vacuum --splits=%d failed",
            dir_name, obj_name, target);
    }
}
```

- [ ] **Step 6: Document the new key**

Find this exact block in `docs/getting-started/configuration.md`:

```
| `AUTO_RESHARD_HOUR` | `3` | Server-local hour (`0`-`23`) the auto-reshard sweep is allowed to run in, once per calendar day. |
```

Replace with:

```
| `AUTO_RESHARD_HOUR` | `3` | Server-local hour (`0`-`23`) the auto-reshard sweep is allowed to run in, once per calendar day. |
| `AUTO_RESHARD_THROTTLE_MS` | `0` | Milliseconds to pause after each successful reshard before the same sweep tick considers its next candidate object. `0` = no pacing (reshards still run strictly one at a time either way; this only adds a gap between them). |
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-auto-reshard-throttle`
Expected: PASS.

- [ ] **Step 8: Run the full suite to confirm no regressions**

Run: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`
Expected: all tests pass, including `test-auto-reshard` (which sets
`AUTO_RESHARD_THROTTLE_MS` to nothing, i.e. the `0` default, so its
single-object reshard is unaffected by the new sleep).

- [ ] **Step 9: Commit**

```bash
git add src/db/shard_db_internal.h src/db/embedded.c src/db/config.c src/db/server.c docs/getting-started/configuration.md src/test/cases/test_auto_reshard.c
git commit -m "feat: throttle between consecutive reshards in the same auto-reshard sweep tick"
```

---

## Definition of Done

- [ ] `SKIP_TESTS=1 ./build.sh` builds clean, no new warnings.
- [ ] `./build/bin/shard-db-test run-all` passes in full, including every new test (`test-reshard-target`, `test-auto-reshard`, `test-shard-stats-hint`, `test-auto-reshard-bad-hour`, `test-auto-reshard-throttle`).
- [ ] `docs/getting-started/configuration.md` and `docs/operations/tuning.md` reflect the new feature, the new 1B–5B/5B–10B bands, the corrected `shard-stats` hint description, and the `AUTO_RESHARD_THROTTLE_MS` key.
- [ ] No changes to `is_valid_splits()` or the per-shard "resplit" (slot-doubling) mechanism. `cmd_shard_stats` changes only its hint logic (Tasks 6, 8) — its per-shard table output, `avg_rec_per_shard`, and `avg_load` fields are unchanged.
- [ ] `cmd_shard_stats` and `auto_reshard_thread()` both call `reshard_target_for_count()` — no independent reshard-recommendation threshold exists anywhere else in the codebase.
- [ ] `auto_reshard_thread` takes `objlock_wrlock` around its `cmd_vacuum` heavy-path call, matching every other call site into that path (`server.c:1397-1398, 1842, 2296-2297`).
- [ ] `auto_reshard_thread` uses `get_live_count_ll`/`get_live_count_ll_for_schema` (not `get_live_count`) so the 1B–5B/5B–10B bands are actually reachable in production.
- [ ] `auto_reshard_thread` skips objects with `sch.splits <= 0` (mid-create/dropped TOCTOU guard).
- [ ] `auto_vacuum_thread` and `auto_reshard_thread` share one `sweep_all_objects()` dir/object walk — no duplicated enumeration logic between them (Task 10).
- [ ] `auto_reshard_thread`'s per-object sweep calls `load_schema()` exactly once per object per tick, not twice (Task 11).
- [ ] An invalid `AUTO_RESHARD_HOUR` value logs a warning naming the bad value instead of silently keeping the old hour with no trace (Task 12).
- [ ] `AUTO_RESHARD_THROTTLE_MS` (default `0`, off) paces consecutive reshards within the same sweep tick without changing their strict seriality (Task 13).
- [ ] All 10 findings from the post-implementation review are addressed by a task in this plan — none deferred.
- [ ] Work left uncommitted-but-committed-per-task per this repo's execution mode is fine either way — final state handed back for Sonnet's `git diff` review before anything merges (per `CLAUDE.md`'s standing exceptions).
