# Single-flight `slotcask_registry_get` cold-open

## Context (why this exists)

Production incident, 2026-07-03: `shard-db-hn-explorer` hung after every
restart, even a restart that had *already* been tried once and re-hung. Root
cause traced to `slotcask_registry_get()` in `src/db/slotcask.c`: on a
registry miss it deliberately opens the object **outside** `g_reg_lock`
(so a slow open doesn't stall unrelated registry ops), but this means
concurrent misses for the *same* (dir, object) key each independently run
the full `slotcask_open()` — which itself fans out up to three
`parallel_for_io()` waves across the shared IO thread pool. On a cold
restart, many threads (the app's background refresh tick + every
simultaneously-arriving live request) miss on the same 2-3 hot objects at
once, and N-fold redundant opens jam the shared pool, inflating query
latency to tens of seconds to minutes (observed: 64s/257s/277s in the
incident journal) even though nothing is truly deadlocked — it's wasted,
duplicated work, not a stuck lock.

The app-level stopgap (serially priming `stories`/`comments`/`users` at
boot, before the server accepts traffic) is already deployed and confirmed
working. This plan is the underlying engine-level fix: make the miss path
single-flighted so concurrent misses for the same key never duplicate the
open, regardless of when or how many callers arrive cold.

## Execution rules

- Branch off `main`: `git checkout -b fix/slotcask-registry-single-flight`.
- Do the tasks in order; each task's test must pass before moving to the next.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Run the full suite with `./build/bin/shard-db-test run-all` (or
  `./build/bin/shard-db-test run <name>` for a single case while iterating).
- Never claim a step passed without pasting the real command output.
- Every insertion point below is located by **quoted anchor text**, not line
  numbers (another branch may be in flight concurrently, so line numbers
  drift). If a quoted anchor is not found byte-for-byte in the target file,
  **stop** and write `PLAN_NOTES.md` in the repo root describing the
  mismatch — do not guess or reinterpret the surrounding code.

## Invariants this fix must preserve

1. At most one `slotcask_open()` call is ever in flight for a given
   (effective_root, object) key at a time. All other concurrent callers for
   that same key wait for it instead of repeating it.
2. Concurrent misses for **different** keys still proceed fully in
   parallel — this fix must not introduce any new serialization across
   unrelated objects. (Exception: a rare hash-bucket collision between two
   *different* keys can cause one to spuriously wait on the other's
   in-flight open once; it then wakes, re-probes, finds its own slot
   correctly, and opens normally. This is bounded, self-resolving, and not
   a correctness bug — do not attempt to eliminate it.)
3. If `slotcask_open()` fails, the reservation is always released and
   `g_reg_cond` is always broadcast — waiters must never block forever on
   a key whose open failed. The next caller (winner or a waiter that wakes
   after the broadcast) retries the open itself.
4. `slotcask_registry_invalidate()` and `slotcask_registry_shutdown()` are
   **not modified**. They only ever act on slots where `used == 1` (a
   fully-installed entry); a slot with `opening == 1, used == 0` is
   invisible to them, which is exactly how they already treat "not present
   in the registry" — no new interaction bug is introduced.
5. The registry install step, after an open completes, must use the
   **slot index remembered at reservation time** — never re-run
   `reg_probe()` a second time to find where to install. A concurrent
   `slotcask_registry_invalidate()` of some unrelated, earlier-in-probe-chain
   key can free a slot in between; a fresh `reg_probe()` call would stop at
   that newly-freed slot instead of reaching the original reservation,
   orphaning it permanently (a leaked slot stuck at `opening=1` forever,
   plus a corrupted install into the wrong slot). Using the remembered
   index sidesteps this entirely, since nothing else is permitted to touch
   a slot while `opening == 1` on it (registry_get's own claim path checks
   `opening` before writing to a slot; invalidate requires `used == 1`).

## Task 1 — add the `opening` flag to `RegEntry`

**File:** `src/db/shard_db_internal.h`

Find this exact block:

```c
#define SLOTCASK_REG_BUCKETS 1024
typedef struct {
    char        key[PATH_MAX];
    struct SlotcaskDb *db;
    int         used;
} RegEntry;
```

Replace it with:

```c
#define SLOTCASK_REG_BUCKETS 1024
typedef struct {
    char        key[PATH_MAX];
    struct SlotcaskDb *db;
    int         used;
    int         opening;   /* 1 while some thread's slotcask_open() is in
                               flight for this key; see slotcask_registry_get. */
} RegEntry;
```

## Task 2 — add `reg_cond` to `ShardDb` + macro

**File:** `src/db/shard_db_internal.h`

Find this exact block:

```c
    /* slotcask object registry */
    RegEntry        reg[SLOTCASK_REG_BUCKETS];
    pthread_mutex_t reg_lock;
```

Replace it with:

```c
    /* slotcask object registry */
    RegEntry        reg[SLOTCASK_REG_BUCKETS];
    pthread_mutex_t reg_lock;
    pthread_cond_t  reg_cond;   /* broadcast whenever any slot's `opening`
                                   flag clears (success or failure) */
```

Then find this exact line:

```c
#define g_reg                       (g_db->reg)
#define g_reg_lock                  (g_db->reg_lock)
```

Replace it with:

```c
#define g_reg                       (g_db->reg)
#define g_reg_lock                  (g_db->reg_lock)
#define g_reg_cond                  (g_db->reg_cond)
```

## Task 3 — init/destroy `g_reg_cond`

**File:** `src/db/embedded.c`

Find this exact line (inside `db_mutexes_init`):

```c
    pthread_mutex_init(&g_reg_lock,              NULL);
```

Replace it with:

```c
    pthread_mutex_init(&g_reg_lock,              NULL);
    pthread_cond_init(&g_reg_cond,               NULL);
```

Find this exact line (inside `db_mutexes_destroy`):

```c
    pthread_mutex_destroy(&g_reg_lock);
```

Replace it with:

```c
    pthread_mutex_destroy(&g_reg_lock);
    pthread_cond_destroy(&g_reg_cond);
```

## Task 4 — single-flight `slotcask_registry_get`

**File:** `src/db/slotcask.c`

Find this exact function (including its leading comment) — it starts right
after the `reg_probe` helper:

```c
SlotcaskDb *slotcask_registry_get(const char *effective_root,
                                  const char *object,
                                  const SlotcaskSchemaInfo *info) {
    if (!info) return NULL;
    if (info->splits <= 0 || info->slot_size <= 0 || info->streams <= 0)
        return NULL;

    char key[PATH_MAX];
    reg_key(key, effective_root, object);

    /* Fast path: probe under the lock; hit returns immediately. */
    pthread_mutex_lock(&g_reg_lock);
    int slot = reg_probe(key);
    if (slot < 0) {
        pthread_mutex_unlock(&g_reg_lock);
        fprintf(stderr, "slotcask_registry: table full (%d buckets)\n",
                SLOTCASK_REG_BUCKETS);
        return NULL;
    }
    if (g_reg[slot].used) {
        SlotcaskDb *db = g_reg[slot].db;
        pthread_mutex_unlock(&g_reg_lock);
        return db;
    }
    pthread_mutex_unlock(&g_reg_lock);

    /* Miss — open OUTSIDE the registry lock.  slotcask_open mmaps 8 stream
       segments × 128 MiB and can take seconds on large objects (e.g.
       hn/comments at splits=256).  Holding g_reg_lock across that would
       block every other registry operation (other opens via warmup's
       parallel_for, drop-object's slotcask_registry_invalidate, any
       slotcask_registry_get caller in a query handler).  Pre-fix this
       caused drop-object to wait 7-12s under warmup contention.

       Race: two concurrent misses for the same key both reach this
       point.  Re-probe inside the install lock below; the loser frees
       its own SlotcaskDb and returns the winner's. */
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s", effective_root, object);

    SlotcaskDb *db = calloc(1, sizeof(SlotcaskDb));
    if (!db) return NULL;
    if (slotcask_open(db, data_dir, info->splits, info->streams,
                      info->slot_size) != 0) {
        free(db);
        fprintf(stderr, "slotcask_registry: open failed for %s/%s\n",
                effective_root, object);
        return NULL;
    }

    /* Install (or lose the race + free ours). */
    pthread_mutex_lock(&g_reg_lock);
    slot = reg_probe(key);
    if (slot < 0) {
        pthread_mutex_unlock(&g_reg_lock);
        slotcask_close(db);
        free(db);
        fprintf(stderr, "slotcask_registry: table full (%d buckets)\n",
                SLOTCASK_REG_BUCKETS);
        return NULL;
    }
    if (g_reg[slot].used) {
        /* Another thread installed while we were opening.  Discard ours
           and use theirs. */
        SlotcaskDb *winner = g_reg[slot].db;
        pthread_mutex_unlock(&g_reg_lock);
        slotcask_close(db);
        free(db);
        return winner;
    }
    snprintf(g_reg[slot].key, sizeof(g_reg[slot].key), "%s", key);
    g_reg[slot].db = db;
    g_reg[slot].used = 1;
    pthread_mutex_unlock(&g_reg_lock);
    return db;
}
```

Replace the entire function with:

```c
SlotcaskDb *slotcask_registry_get(const char *effective_root,
                                  const char *object,
                                  const SlotcaskSchemaInfo *info) {
    if (!info) return NULL;
    if (info->splits <= 0 || info->slot_size <= 0 || info->streams <= 0)
        return NULL;

    char key[PATH_MAX];
    reg_key(key, effective_root, object);

    /* Fast path: probe under the lock; hit returns immediately.  On a
       miss, this thread either becomes the sole opener for `key` (and
       reserves its slot before releasing the lock) or, if another
       thread is already opening the same key, waits on g_reg_cond
       instead of redundantly repeating slotcask_open — which itself
       fans out up to three parallel_for_io() waves across the shared
       IO pool.  Concurrent misses on the same key used to each pay
       that cost independently; on a cold restart with several callers
       missing on the same hot object at once, that duplicated,
       wasted work is what inflated query latency to minutes (2026-07-03
       hn-explorer incident) even though nothing was truly deadlocked. */
    pthread_mutex_lock(&g_reg_lock);
    for (;;) {
        int slot = reg_probe(key);
        if (slot < 0) {
            pthread_mutex_unlock(&g_reg_lock);
            fprintf(stderr, "slotcask_registry: table full (%d buckets)\n",
                    SLOTCASK_REG_BUCKETS);
            return NULL;
        }
        if (g_reg[slot].used) {
            SlotcaskDb *db = g_reg[slot].db;
            pthread_mutex_unlock(&g_reg_lock);
            return db;
        }
        if (g_reg[slot].opening) {
            /* Someone else is opening this key (or a colliding one) —
               wait for them to finish, then re-probe from scratch. */
            pthread_cond_wait(&g_reg_cond, &g_reg_lock);
            continue;
        }

        /* We are the sole opener. Reserve the slot by index — do NOT
           re-probe after opening (see plan invariant 5): a concurrent
           slotcask_registry_invalidate() of an unrelated, earlier-in-chain
           key could free a slot that a fresh reg_probe() would stop at
           first, orphaning this reservation. */
        int reserved = slot;
        snprintf(g_reg[reserved].key, sizeof(g_reg[reserved].key), "%s", key);
        g_reg[reserved].opening = 1;
        pthread_mutex_unlock(&g_reg_lock);

        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/%s", effective_root, object);

        SlotcaskDb *db = calloc(1, sizeof(SlotcaskDb));
        int open_rc = db ? slotcask_open(db, data_dir, info->splits,
                                          info->streams, info->slot_size)
                          : -1;

        pthread_mutex_lock(&g_reg_lock);
        if (open_rc != 0 || !db) {
            if (db) free(db);
            g_reg[reserved].opening = 0;
            g_reg[reserved].key[0] = '\0';
            pthread_cond_broadcast(&g_reg_cond);
            pthread_mutex_unlock(&g_reg_lock);
            fprintf(stderr, "slotcask_registry: open failed for %s/%s\n",
                    effective_root, object);
            return NULL;
        }
        g_reg[reserved].db = db;
        g_reg[reserved].used = 1;
        g_reg[reserved].opening = 0;
        pthread_cond_broadcast(&g_reg_cond);
        pthread_mutex_unlock(&g_reg_lock);
        return db;
    }
}
```

Note: `calloc(1, sizeof(ShardDb))` in `shard_db_open_internal` zero-fills
the entire `reg[]` array at startup, so every `RegEntry.opening` starts at
0 — no separate init needed for the new field.

## Task 5 — regression test: concurrent cold-open on the same object

**File:** `src/test/cases/test_registry_single_flight.c` (new file)

```c
/* src/test/cases/test_registry_single_flight.c
 *
 * Regression guard for the 2026-07-03 hn-explorer prod incident:
 * slotcask_registry_get's miss path is now single-flighted per key, so
 * N concurrent first-touch queries against the same freshly-created
 * object must all succeed and must not hang — previously each miss
 * independently repeated the full slotcask_open() (including its
 * parallel_for_io fan-out), and N of those racing at once could jam the
 * shared IO pool badly enough to look like a hang from outside.
 *
 * splits=256 mirrors the actual hn/comments object from the incident
 * (see the slotcask_registry_get comment) — large enough that a single
 * slotcask_open is not instantaneous, which is what gives the race
 * window real width instead of everything finishing before threads
 * even get scheduled.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define N_WORKERS 16
#define SPLITS 256
/* Absolute hang-regression bound — generous on purpose (this is a
   correctness/no-hang guard, not a latency benchmark; see
   test_stress_no_hang.c for the established precedent on why strict
   timing assertions are unreliable across CI runners). */
#define BURST_TIMEOUT_MS 20000

typedef struct {
    int port;
    pthread_barrier_t *barrier;
    int ok;      /* 1 if request succeeded with no "error" field */
    long ms;     /* wall-clock for this worker's single request */
} RaceWorker;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void *race_worker_main(void *arg) {
    RaceWorker *w = arg;
    TestClientCfg cfg = { .port = w->port, .io_timeout_ms = BURST_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { w->ok = 0; return NULL; }

    /* Every worker connects first, then all release the barrier
       together — this forces genuinely concurrent arrival at the
       server's registry regardless of how fast slotcask_open itself
       happens to run in this environment. */
    pthread_barrier_wait(w->barrier);

    long t0 = now_ms();
    char *resp = NULL;
    int rc = tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"racetest\",\"object\":\"hot\"}", &resp);
    w->ms = now_ms() - t0;
    w->ok = (rc == 0 && resp && !strstr(resp, "\"error\""));
    free(resp);
    tc_close(tc);
    return NULL;
}

static int test_registry_single_flight_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = BURST_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"racetest\"}", &resp);
    free(resp); resp = NULL;

    char create_req[512];
    snprintf(create_req, sizeof(create_req),
        "{\"mode\":\"create-object\",\"dir\":\"racetest\",\"object\":\"hot\","
        "\"splits\":%d,\"max_key\":16,"
        "\"fields\":[\"v:int\"]}", SPLITS);
    tc_request(tc, create_req, &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "create-object succeeded");
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    /* create-object opens-then-closes the object once itself (not
       registered) — the registry is still empty for "hot" at this
       point, so the burst below is a genuine N-way concurrent miss,
       exactly matching the incident's post-restart state (objects
       exist on disk; registry is cold). */

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, N_WORKERS);

    pthread_t threads[N_WORKERS];
    RaceWorker workers[N_WORKERS];
    for (int i = 0; i < N_WORKERS; i++) {
        workers[i].port = env.port;
        workers[i].barrier = &barrier;
        workers[i].ok = 0;
        workers[i].ms = 0;
        pthread_create(&threads[i], NULL, race_worker_main, &workers[i]);
    }

    long burst_t0 = now_ms();
    for (int i = 0; i < N_WORKERS; i++) pthread_join(threads[i], NULL);
    long burst_ms = now_ms() - burst_t0;

    pthread_barrier_destroy(&barrier);

    int all_ok = 1;
    for (int i = 0; i < N_WORKERS; i++) if (!workers[i].ok) all_ok = 0;
    ASSERT_TRUE(all_ok, "all concurrent first-touch queries succeeded");
    ASSERT_TRUE(burst_ms < BURST_TIMEOUT_MS,
        "concurrent cold-open burst completed within the hang-regression bound");

    /* Best-effort anti-thundering-herd signal — local-only (see
       test_stress_no_hang.c precedent for why strict timing assertions
       are excluded on shared CI runners). Not required for correctness;
       demonstrates the fix's actual perf property when run locally. */
    if (!getenv("CI")) {
        printf("# registry-single-flight: burst of %d completed in %ldms\n",
               N_WORKERS, burst_ms);
    }

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-registry-single-flight", test_registry_single_flight_run)
```

Register it in the build. Find this exact line in `build.sh`:

```
    src/test/cases/test_o_direct_scan.c \
```

Replace it with:

```
    src/test/cases/test_o_direct_scan.c \
    src/test/cases/test_registry_single_flight.c \
```

## Verification

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-registry-single-flight
./build/bin/shard-db-test run-all
```

Paste the real output of all three commands. The suite must end with
`# total: N passed, 0 failed`. Do not claim success without this output.
