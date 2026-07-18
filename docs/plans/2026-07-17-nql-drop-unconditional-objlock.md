# Finding 4 — stop NQL dispatch from taking an objlock its JSON twin never needs

Source: Finding 4 in `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`.
Second in the agreed order (7 → **4** → 8 → rest).

## Root cause

`dispatch_json_query` (`server.c:1394-1398`) gates per-object locking by
command *category*:

```c
    /* Per-object locking: wrlock for schema/rebuild, rdlock for writes, none for reads. */
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock && mode_is_write(mode);
    if (took_wrlock) objlock_wrlock(db_root, object);
    else if (took_rdlock) objlock_rdlock(db_root, object);
```

`mode_is_write` (`server.c:31-40`) and `mode_is_schema` (`server.c:53-62`)
list every write/schema mode by name; `find`/`count`/`aggregate` appear in
neither list, so a JSON `find`/`count`/`aggregate` takes **no** objlock at
all.

`dispatch_nql_query` (`server.c:592-673`) does not have this gate — it takes
`objlock_rdlock(db_root, cmd.obj)` unconditionally at line 621, before its
`switch (cmd.mode)`, and releases at line 671 after every case. `NqlMode`
(`nql.h:43`) is `{ NQL_FIND, NQL_COUNT, NQL_AGGREGATE }` — there is no NQL
write mode, ever. So the unconditional rdlock in `dispatch_nql_query` is
guarding a switch whose every branch is a read that JSON's own policy, at
the identical choke point, would give zero lock.

Confirmed both wire protocols converge on the same read execution cores, so
this isn't a hidden NQL-specific safety requirement quietly relying on the
lock: JSON's `cmd_count` and NQL's `cmd_count_tree` converge on
`cmd_count_with_tree`; JSON's `cmd_find` and NQL's `cmd_find_tree` converge on
`cmd_find_do`; JSON's `cmd_aggregate` and NQL's `cmd_aggregate_tree` converge
on `cmd_aggregate_do`. The storage/index strategy selected inside those cores
can vary by query plan (mmap, O_DIRECT, or an index walk), but it is identical
for the JSON and NQL forms of the same query. Only NQL additionally serializes
against the object's rwlock first.

**Consequence**: under `objlock.c`'s
`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` policy, any new rdlock
acquisition — including one from an NQL `find`/`count`/`aggregate` — must
wait behind a schema wrlock currently held (trivially, any rdlock call
blocks while a writer holds the lock) *and*, more subtly, behind one that's
merely pending against existing readers (writer-preferring semantics queue
new readers behind a waiting writer even before it acquires). The
JSON-equivalent read on the same object never contends at all, so the exact
same logical query gets different latency depending only on which wire
protocol the client happened to use — a real, currently-shipping
inconsistency between two client-facing protocols.

## Fix

100% of NQL modes are reads, so no `mode_is_write`/`mode_is_schema`-style
gating is needed — the correct amount of locking for every NQL mode is
none. Delete the lock acquire/release pair outright.

### `src/db/server.c` — remove NQL's objlock pair

Anchor (exact existing text):
```c
    /* Build db_root = g_db->db_root / dir */
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof db_root, "%s/%s", raw_db_root, cmd.dir);

    objlock_rdlock(db_root, cmd.obj);

    switch (cmd.mode) {
```
Replace with:
```c
    /* Build db_root = g_db->db_root / dir */
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof db_root, "%s/%s", raw_db_root, cmd.dir);

    /* No objlock here: every NqlMode (NQL_FIND/NQL_COUNT/NQL_AGGREGATE) is a
       read, and reads take no per-object lock — matches dispatch_json_query's
       mode_is_write/mode_is_schema gating. The JSON and NQL forms converge on
       the same cmd_count_with_tree/cmd_find_do/cmd_aggregate_do read cores.
       If NQL ever grows a write mode, that mode's case must take and release
       the appropriate lock — see mode_is_write/mode_is_schema for JSON. */
    switch (cmd.mode) {
```

Anchor (exact existing text):
```c
    }

    objlock_rdunlock(db_root, cmd.obj);
    nql_free_command(&cmd);
}
```
Replace with:
```c
    }

    nql_free_command(&cmd);
}
```
(This is the only occurrence of `objlock_rdunlock(db_root, cmd.obj);` in the
file — verify with `grep -n "objlock_rdunlock(db_root, cmd.obj)" src/db/server.c`
before deleting, since `db_root` is a common local-variable name reused by
other functions in this file; the anchor's surrounding `nql_free_command`
call makes the match unambiguous regardless.)

## Task 1 — regression test (test-first)

The test must prove: while an object's schema wrlock is **held**, a
concurrent NQL `find` on that object blocks pre-fix and does not block
post-fix. Reproducing the full "pending-writer-preference" nuance precisely
isn't necessary to prove the code change — pre-fix, `dispatch_nql_query`
takes `objlock_rdlock` unconditionally, which blocks for as long as the
wrlock is held, full stop; post-fix, it takes no lock, so it can't block on
this object's lock at all. This is a strictly stronger and simpler proof of
the actual diff than reconstructing the writer-preference queueing order.

To make the wrlock-held window deterministic (not dependent on how fast a
real `vacuum` happens to run), add a test-only delay knob that fires while
`dispatch_json_query` holds a wrlock. While the delay is active, the hook
creates a synchronous marker file under the effective db root and removes it
before command execution resumes. The test waits for that marker and verifies
it is still present after the NQL response; therefore a pre-fix query cannot
falsely pass merely because it started near the end of the delay window.

This mirrors the two test-only delay knobs this codebase already has:
`KFCACHE_TEST_HOLD_MS` (`config.c:452-459`, widens
`kfcache_invalidate_prefix`'s hold window) and `WARMUP_TEST_DELAY_MS`
(`config.c:460-466`, `server.c:2695-2698`, widens
`warmup_kf_task_fn`'s objlock-held window) — both documented in their own
source comments as "not a documented production setting — do not add to
configuration.md," the same rule applies to the new knob.

### 1a. `shard_db_internal.h` — new field + accessor macro

Anchor (exact existing text):
```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
```
Replace with:
```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
    int schema_wrlock_test_delay_ms; /* test-only; 0 = off in production */
```

Anchor (exact existing text):
```c
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
```
Replace with:
```c
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
#define g_schema_wrlock_test_delay_ms (g_db->schema_wrlock_test_delay_ms)
```

No default-initialization change is needed — `ShardDb` is zero-allocated at
startup (same as `kfcache_test_hold_ms`/`warmup_test_delay_ms`, neither of
which has a separate default-set line either); the new `int` field is `0`
or "off in production" like its two siblings.

### 1b. `config.c` — parse the new env var

Anchor (exact existing text):
```c
        } else if (strncmp(p, "WARMUP_TEST_DELAY_MS=", 21) == 0) {
            /* Test-only knob (widens warmup_kf_task_fn's objlock-held
               window deterministically for the warmup-vs-vacuum UAF
               regression test). Not a documented production setting —
               do not add to configuration.md. */
            int n = atoi(p + 21);
            if (n >= 0 && g_db) g_warmup_test_delay_ms = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```
Replace with:
```c
        } else if (strncmp(p, "WARMUP_TEST_DELAY_MS=", 21) == 0) {
            /* Test-only knob (widens warmup_kf_task_fn's objlock-held
               window deterministically for the warmup-vs-vacuum UAF
               regression test). Not a documented production setting —
               do not add to configuration.md. */
            int n = atoi(p + 21);
            if (n >= 0 && g_db) g_warmup_test_delay_ms = n;
        } else if (strncmp(p, "SCHEMA_WRLOCK_TEST_DELAY_MS=", 28) == 0) {
            /* Test-only knob (widens dispatch_json_query's schema-mode
               objlock_wrlock-held window and publishes a synchronous marker
               for the NQL-vs-JSON lock-contention regression test). Not a
               documented production setting — do not add to
               configuration.md. */
            int n = atoi(p + 28);
            if (n >= 0 && g_db) g_schema_wrlock_test_delay_ms = n;
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
```

### 1c. `server.c` — inject the delay right after taking the wrlock

Anchor (exact existing text, `server.c:1394-1399`):
```c
    /* Per-object locking: wrlock for schema/rebuild, rdlock for writes, none for reads. */
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock && mode_is_write(mode);
    if (took_wrlock) objlock_wrlock(db_root, object);
    else if (took_rdlock) objlock_rdlock(db_root, object);
```
Replace with:
```c
    /* Per-object locking: wrlock for schema/rebuild, rdlock for writes, none for reads. */
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock && mode_is_write(mode);
    if (took_wrlock) objlock_wrlock(db_root, object);
    else if (took_rdlock) objlock_rdlock(db_root, object);
    if (took_wrlock && g_db && g_schema_wrlock_test_delay_ms > 0) {
        /* Synchronous marker: unlike LOG_INFO (written by the async log
           thread), its presence proves the test delay has started and has
           not yet ended. The test also checks it remains present after the
           concurrent NQL response, eliminating a late-observation false pass. */
        char marker_path[PATH_MAX];
        snprintf(marker_path, sizeof marker_path,
                 "%s/.schema-wrlock-test-delay-%s.active", db_root, object);
        FILE *marker = fopen(marker_path, "w");
        if (marker) {
            fprintf(marker, "mode=%s object=%s\n", mode, object);
            fclose(marker);
        }
        struct timespec delay_ts = { g_schema_wrlock_test_delay_ms / 1000,
                                      (long)(g_schema_wrlock_test_delay_ms % 1000) * 1000000L };
        nanosleep(&delay_ts, NULL);
        unlink(marker_path);
    }
```

### 1d. New test file `src/test/cases/test_nql_no_objlock_contention.c`

Modeled on `test_warmup_vacuum_race.c`'s structure (spawn daemon via
`fork`/`execl` so `db.env` can be rewritten with the test-only knob before
each round, marker-file polling for the active delay window, and a
`now_ms()`/`clock_gettime` timing assertion). Unlike the older test, this one
must guard every signal with `pid > 0` and forcibly reap a daemon that does
not exit inside the graceful-shutdown window.

```c
/* src/test/cases/test_nql_no_objlock_contention.c
 * Finding 4 regression: dispatch_nql_query used to take an unconditional
 * objlock_rdlock before its find/count/aggregate switch, even though every
 * NqlMode is a read and dispatch_json_query's own mode_is_write/
 * mode_is_schema gating already takes zero lock for the JSON equivalents
 * (find/count/aggregate are in neither list). That meant an NQL read could
 * block behind another connection's held schema wrlock on the same object
 * while the JSON-wire-protocol version of the identical read would not.
 *
 * SCHEMA_WRLOCK_TEST_DELAY_MS (test-only, 0/off in production) widens
 * dispatch_json_query's wrlock-held window deterministically: a `vacuum`
 * request sleeps for the configured duration immediately after acquiring
 * the object's wrlock, before doing any real work. A synchronous marker file
 * exists only while that delay is active. After observing the marker, the
 * test fires a concurrent NQL `find` on the same object and requires the
 * marker to still exist when the NQL response arrives.
 *
 *   - Pre-fix: dispatch_nql_query's own objlock_rdlock blocks until the
 *     held wrlock releases. The delay marker has necessarily been removed
 *     before the NQL response can arrive.
 *   - Post-fix: dispatch_nql_query takes no lock at all -- the NQL find
 *     returns well under DELAY_MS/2 while the marker is still present.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define DELAY_MS 2000

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* The server creates this file synchronously after acquiring the wrlock and
   removes it when the injected delay ends. Checking its contents avoids
   accepting an unrelated or stale marker. */
static int wait_for_wrlock_delay_start(const char *marker_path, int timeout_s) {
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(marker_path, "r");
        if (f) {
            char line[256] = {0};
            int matched = fgets(line, sizeof line, f) &&
                          strstr(line, "mode=vacuum") &&
                          strstr(line, "object=lockrace");
            fclose(f);
            if (matched) return 1;
        }
        usleep(100000);
    }
    return 0;
}

static int write_env(const char *env_path, const char *db_root, int port,
                      int with_delay) {
    FILE *ef = fopen(env_path, "w");
    if (!ef) return -1;
    fprintf(ef,
        "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=4\nFCACHE_MAX=4096\nTLS_ENABLE=0\n",
        db_root, port);
    if (with_delay) fprintf(ef, "SCHEMA_WRLOCK_TEST_DELAY_MS=%d\n", DELAY_MS);
    fclose(ef);
    return 0;
}

static pid_t spawn_daemon(const char *base, const char *shard_db_abs) {
    pid_t pid = fork();
    if (pid == 0) {
        if (chdir(base) != 0) _exit(126);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    return pid;
}

/* Never signal a non-positive pid: kill(-1, ...) targets every process the
   test user may signal. Always reap the child, escalating only after the
   graceful-shutdown window expires. */
static void stop_daemon(pid_t pid) {
    if (pid <= 0) return;
    (void)kill(pid, SIGTERM);
    for (int i = 0; i < 100; i++) {
        int status = 0;
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid || (w < 0 && errno == ECHILD)) return;
        usleep(100000);
    }
    (void)kill(pid, SIGKILL);
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
}

static int wait_ready(int port, int tries) {
    for (int i = 0; i < tries; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                free(r); tc_close(probe); return 1;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    return 0;
}

static int test_nql_no_objlock_contention_run(void) {
    char base[] = "/tmp/shard-db-nql-lock-race-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    int port = test_pick_port();
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/root", base);
    mkdir(db_root, 0755);

    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    ASSERT_TRUE(write_env(env_path, db_root, port, 0) == 0, "write initial db.env");

    char marker_path[PATH_MAX];
    snprintf(marker_path, sizeof marker_path,
             "%s/default/.schema-wrlock-test-delay-lockrace.active", db_root);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found");
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Round 1: plain daemon, seed the object. */
    pid_t pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 1)");
    if (pid <= 0) {
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }
    int ready = wait_ready(port, 100);
    ASSERT_TRUE(ready, "daemon ready (round 1)");
    if (!ready) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect (round 1)");
    if (!tc) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"lockrace\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"lockrace\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"age\":30}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    stop_daemon(pid);
    pid = -1;

    /* Round 2: restart with the delay knob armed. */
    ASSERT_TRUE(write_env(env_path, db_root, port, 1) == 0, "write db.env with SCHEMA_WRLOCK_TEST_DELAY_MS");
    pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 2)");
    if (pid <= 0) {
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }
    ready = wait_ready(port, 100);
    ASSERT_TRUE(ready, "daemon ready (round 2)");
    if (!ready) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClient *vc = tc_connect(&cfg);
    ASSERT_NOT_NULL(vc, "connect for vacuum");
    if (!vc) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Fire the wrlock-holding vacuum asynchronously: send the request and
       poll the synchronous active-marker rather than waiting for the
       response (which won't arrive until after DELAY_MS has elapsed). */
    int sent = tc_send(vc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"lockrace\"}") == 0;
    ASSERT_TRUE(sent, "vacuum request sent");
    if (!sent) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    int marker_seen = wait_for_wrlock_delay_start(marker_path, 10);
    ASSERT_TRUE(marker_seen,
        "synchronous schema-wrlock delay marker observed");
    if (!marker_seen) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* While the wrlock is held (vacuum is sleeping inside it), fire a
       concurrent NQL find on the SAME object over a second connection. */
    TestClient *nc = tc_connect(&cfg);
    ASSERT_NOT_NULL(nc, "connect for NQL find");
    if (nc) {
        long t0 = now_ms();
        char *nresp = NULL;
        int nql_rc = tc_request(nc, "find default lockrace", &nresp);
        long elapsed = now_ms() - t0;
        int marker_still_active = access(marker_path, F_OK) == 0;
        tc_close(nc);

        ASSERT_TRUE(nql_rc == 0, "NQL find round-trip succeeds");
        ASSERT_TRUE(nresp != NULL && !SAFE_STRSTR(nresp, "\"error\""),
            "NQL find succeeds while a schema wrlock is held on the same object");
        if (nresp) TAP_DIAG("# NQL find response: %s\n", nresp);
        free(nresp);

        ASSERT_TRUE(marker_still_active,
            "NQL response arrives before the held-wrlock delay ends");
        ASSERT_TRUE(elapsed < (DELAY_MS / 2),
            "NQL find does not block behind the held schema wrlock (no objlock taken)");
        TAP_DIAG("# NQL find elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    }

    /* Drain the vacuum response so the connection doesn't leak past the
       delay window. */
    char *vresp = NULL;
    tc_recv(vc, &vresp);
    free(vresp);
    tc_close(vc);

    stop_daemon(pid);

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-no-objlock-contention", test_nql_no_objlock_contention_run)
```

Register in `build.sh`. Anchor (exact existing line):
```
    src/test/cases/test_warmup_vacuum_race.c \
```
Add immediately after it:
```
    src/test/cases/test_nql_no_objlock_contention.c \
```
(If this exact line isn't found, match on any adjacent
`src/test/cases/test_*.c \` line instead and add the new line next to it —
the ordering in `build.sh`'s list is not itself meaningful.)

**Prove it fails first**: apply only 1a/1b/1c/1d (the test-only delay knob
and the new test), leaving `dispatch_nql_query`'s objlock pair untouched.
Build with `SKIP_TESTS=1 ./build.sh`, run
`./build/bin/shard-db-test run test-nql-no-objlock-contention`. Expected
failure: the "NQL response arrives before the held-wrlock delay ends"
assertion must fail, because the pre-fix NQL rdlock cannot return until after
the writer releases and the marker has been removed. The elapsed-time
assertion will normally fail too, but its exact value is the remaining delay
after marker polling, not necessarily the full `DELAY_MS`. If the marker
assertion unexpectedly passes pre-fix, stop and record the output in
`PLAN_NOTES.md`; do not apply the production fix until the red proof is
deterministic. Paste the actual failing output. Then apply the Fix section
above (delete `objlock_rdlock`/`objlock_rdunlock` in
`dispatch_nql_query`), rebuild, rerun, and paste the passing output (marker
still present and elapsed well under `DELAY_MS/2`).

## Task 2 — apply the fix

Apply the two edits in the Fix section above (`server.c:621` acquire and
`server.c:671` release, exact anchors given there).

## Task 3 — build, full suite

1. `SKIP_TESTS=1 ./build.sh` — zero new warnings.
2. `./build/bin/shard-db-test run test-nql-no-objlock-contention` — passes.
3. `./build/bin/shard-db-test run-all` — full suite green. Pay particular
   attention to any existing NQL test (`test_nql.c` and anything else
   matching `*nql*`) and anything exercising concurrent schema mutations
   (`test_warmup_vacuum_race.c`, `test_auto_reshard.c`) — this change
   touches the same locking machinery those tests already probe from a
   different angle.

## Edge cases and invariants (explicit)

- **NQL never has a write mode today** (`NqlMode` is exactly
  `{NQL_FIND, NQL_COUNT, NQL_AGGREGATE}`, confirmed in `nql.h:43`) — this
  fix assumes that invariant holds today; it does not need to defend
  against a hypothetical future NQL write mode. The comment left in place
  at the deletion site explicitly calls this out so a future contributor
  adding an NQL write mode knows to add its own lock acquisition, rather
  than silently inheriting an assumption that's no longer true.
- **`cmd.explain` branches** (`NQL_COUNT`/`NQL_FIND`/`NQL_AGGREGATE` each
  have an `if (cmd.explain) cmd_explain_tree(...)` branch) are unaffected —
  `cmd_explain_tree` is itself read-only and was already covered by the
  same (now-removed) unconditional lock; nothing about `explain` needs
  separate handling.
- **Auth/parse-error early returns** (`server.c:595-599`, `:604-608`,
  `:612-614`) all return *before* the objlock acquire line and are
  untouched by this change.
- **The new test-only knob only fires for schema-mode (`took_wrlock`)
  requests** — write-mode (`took_rdlock`) requests are untouched by 1c's
  edit (`if (took_wrlock && ...)` guards it), matching the finding's actual
  scope (schema wrlock contention), not a broader "delay every locked
  request" knob that would be harder to reason about.
- **Interrupt/crash safety**: no on-disk format, no multi-step mutation, no
  new lock ordering — this removes two lock calls from a read-only dispatch
  path and adds a test-only sleep gated to a knob that defaults to `0` (off)
  in every production `db.env`. There is no partial-state window introduced.

## Execution rules (embedded, per CORE-PROCESS)

- Branch off `main`. Work stays uncommitted per this repo's standing
  execution-mode exception.
- Build with `SKIP_TESTS=1 ./build.sh`; test with
  `./build/bin/shard-db-test run[-all]`.
- Order: Task 1 (test + knob, prove the test fails against unmodified
  `dispatch_nql_query`) → Task 2 (fix) → Task 3 (full verification).
- If a quoted anchor isn't found exactly, write `PLAN_NOTES.md` describing
  the mismatch and halt the run immediately — do not guess or continue to
  another task.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
