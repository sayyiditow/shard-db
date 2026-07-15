# Concurrency Crashes + CI Regressions + Leak Fixes — Implementation Plan

**Goal**: `shard-db-test run-all` under high `--jobs` (and three GitHub
Actions workflows — CI, Coverage, Sanitizers — that call `run-all` with no
`--jobs` flag at all, so they silently inherit full-nproc parallelism since
the `feat/parallel-test-runner` merge) has been crashing and failing. This
plan fixes every root cause found during investigation:

1. **CI-workflow regression (root cause of Coverage + Sanitizers going
   red)**: `ci.yml`, `codecov.yml`, `sanitizers.yml` never pass `--jobs` to
   `run-all`, so it defaults to `sysconf(_SC_NPROCESSORS_ONLN)`.
   `codecov.yml`'s own comment says the run must stay sequential (gcov
   counters aren't safe across overlapping test cases) — that invariant
   silently broke. Sanitizers/CI legs see genuine contention-driven request
   timeouts (`expected N got -1`) under full-nproc parallelism on shared/
   weak runners.
2. **Real concurrency bug**: `src/db/btree.c`'s `bt_cache_evict_slot()`
   unconditionally detaches a cache slot's `fd`/`map`/`map_size` — including
   while another thread holds that slot's rwlock for a long-lived read (a
   `BtRangeIter`, which the API contract documents as holding the rdlock for
   the iterator's entire lifetime). The evicting thread then calls
   `bt_dispose_mapping()`, which `munmap()`s and `close()`s that same
   mapping while the reader is still dereferencing it — a genuine
   use-after-unmap that segfaults under real concurrent load. This is
   independent of the CI-workflow regression above and can fire any time a
   long-lived range iterator coexists with enough cache pressure to evict
   its slot.
3. **Three confirmed memory leaks** (found via local ad-hoc ASan runs —
   note CI's `sanitizers.yml` runs with `detect_leaks=0`, so these do not
   show up there today; they were only visible locally with
   `detect_leaks=1`).

**Explicitly NOT included** (investigated and ruled out of scope):
- A "worker-thread teardown" fix for `test_runner.c`'s parallel workers
  calling `shard_db_close()` — investigated and confirmed **no such call
  exists anywhere** in `worker_main()` today (grepped `shard_db_close` /
  `g_shard_db_instance` across `src/db` and `src/test` — the only writers
  are `embedded.c`/`server.c` at daemon-owning-process init time; test
  workers only ever *set* their thread-local `g_db`, never close it, and
  the process exits right after `run-all` returns). There is no live bug
  here to fix; inventing a close-and-teardown path that doesn't exist today
  would be exactly the kind of speculative abstraction CORE-PROCESS.md
  tells us not to add. Dropped from this plan.
- A fix for the macOS CI `Build & test (macos arm64)` leg's Bus-error
  (exit 138) crash. Root cause is **not** confidently known — Task 7 below
  is a diagnostic-only task (reproduce, instrument, report) per
  CORE-PROCESS.md's root-cause-first rule. No fix is proposed until the
  diagnostic task identifies one.

## Global Constraints

- No new dependencies.
- Build: `SKIP_TESTS=1 ./build.sh`. Test:
  `./build/bin/shard-db-test run-all` /
  `./build/bin/shard-db-test run <name>` /
  `./build/bin/shard-db-test run-all --filter <substr>`.
- **Execution mode for this repo**: leave work **uncommitted** after
  executing this plan — do not `git add`/`git commit`. A human (Sonnet)
  reviews the raw `git diff` before anything is committed.
- **Plan execution happens on a fresh branch off `main`, run by a model
  outside the Claude family** (Gemini/GPT) per this repo's standing
  exception — do not spawn a Haiku/Claude subagent for execution.
- Branch off `main` before starting (fresh branch, e.g.
  `fix/btree-cache-race-and-leaks`).
- Do every task **in order**; each task's tests must pass before starting
  the next.
- If a quoted anchor is not found **exactly** in the target file, **stop**
  and write `PLAN_NOTES.md` at the repo root describing the mismatch — do
  not guess or reinterpret the surrounding code.
- If you hit a decision this plan does not cover, **stop and ask** — do
  not improvise.
- Paste real command output for every build/test step — never claim a
  step passed without showing it.
- **Dynamic-safety tooling gate** (CORE-PROCESS.md): Tasks 2–5 touch
  shared/concurrent state or memory ownership. For each of those tasks,
  locally build with `BUILD_MODE=asan` (`BUILD_MODE=asan SKIP_TESTS=1
  ./build.sh`) and run the affected test case(s) with
  `ASAN_OPTIONS=detect_leaks=1` (CI's own `sanitizers.yml` runs with
  `detect_leaks=0` — do not rely on CI to catch these; verify locally).
  This is the required "revert → confirm-fail → reapply → confirm-pass"
  evidence for each fix: run the regression test against the pre-fix code
  first (leak/crash observed), then against the fix (clean), and paste
  both outputs.
- **Merge gate (explicit user instruction)**: before this branch merges to
  `main`, all GitHub Actions workflows must be green — not just local
  `run-all`. After pushing, run `gh pr checks <number> --watch` (or `gh run
  list --branch <branch>`) and confirm every workflow (CI all 3 matrix
  legs, Coverage, Sanitizers, plus whatever else is enabled) reports
  success before requesting the human's merge go-ahead.

## Background reading (do this before Task 1)

- `src/test/shard-db-test.c` — `run-all`'s `--jobs` parsing; defaults to
  `sysconf(_SC_NPROCESSORS_ONLN)` when `--jobs` is absent or `<= 0`.
- `.github/workflows/ci.yml`, `.github/workflows/codecov.yml`,
  `.github/workflows/sanitizers.yml` — all three invoke
  `./build/bin/shard-db-test run-all` with no `--jobs` flag.
  `codecov.yml`'s "Run full C test suite (sequential)" step has a comment
  explicitly asserting sequential-only safety for gcov.
- `src/db/btree.c` — `bt_cache_evict_slot` (detaches a slot's resources
  under `bt_cache_lock`, no check of whether the slot's rwlock is
  currently held), its two callers `bt_acquire`'s LRU-eviction block and
  `bt_cache_drop_slot`, and `bt_dispose_mapping` (does the actual
  `munmap`/`close`, called after `bt_cache_lock` is released).
- `src/db/btree.h` — `BtRangeIter`'s doc comment: "Holds the underlying
  btree's rdlock for the iterator's lifetime — callers must call
  `btree_range_iter_close` to release." This is the long-lived holder that
  the eviction race clobbers.
- `src/db/config.c`'s `parse_field_type` (the `enum(...)` branch, ~line
  1265-1319) and `free_enum_values` (~line 1112) — leak fix #1's
  allocation/cleanup pair.
- `src/db/query_find.c`'s `parse_field_line` (~line 700) — calls
  `parse_field_type` **unconditionally**, even on the path that makes it
  return failure; this is why a "parse failed" early return can still have
  allocated `enum_values` that need freeing.
- `src/db/query_schema.c`'s `cmd_edit_fields` (~line 360-559) — leak fix
  #1's 11 early-return sites, and the 2 already-correct cleanup loops
  (~line 544, ~line 555) that this fix mirrors.
- `src/db/server.c`'s `dispatch_json_query` — `mode`/`dir`/`object` are
  `json_obj_strdup`'d near the top (~line 703, ~line 1237-1238) and freed
  at the function's single shared epilogue (~line 2048: `free(mode);
  free(dir); free(object);`). Leak fix #2 is 4 early `return;` sites that
  bypass that epilogue.
- `src/db/query_bulk.c`'s `bulk_ins_run` (~line 985-1100) — leak fix #3.
  `wire_for_record = strdup(id)` (~line 1009) is only freed on the
  `_enc == -2` path (~line 1062) or transferred into `wire_keys[]`
  (~line 1088, both only reached when `id` is still non-NULL). When
  `parse_uuid_string`/`parse_seq_key` fails, `id` is set to `NULL`
  (~line 1019, ~line 1030) and the whole `if (id && data_ptr)` block that
  would free-or-store `wire_for_record` is skipped — leaking it.
- `src/test/cases/test_auto_key.c` lines ~344-351 — **already exercises**
  the exact leaking path in fix #3 today (a malformed provided key on a
  `seq(...)` auto-key object, asserting the batch-reject error) — this
  existing test is reused as the leak's regression proof, no new test
  needed for that one.
- `src/test/cases/test_edit_field.c` / `test_enum.c` — already exercise 6
  of fix #1's 11 leak sites via existing error-path assertions
  (`cross-type refused`, `tombstoned refused`, `unknown field refused`,
  `duplicate edit refused`, enum shrink `remove: error`, enum rename
  `rename no flag: error`). Only 4 sites need new coverage (Task 3).
- No existing test exercises `find` with a negative offset, or `compact`
  mode at all — both need new coverage (Task 4).

## Task 1: CI workflow `--jobs` fix

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/codecov.yml`
- Modify: `.github/workflows/sanitizers.yml`

This is a YAML-only change; there is no C regression test for it. The
verification is a real GitHub Actions run going green (see "Merge gate"
above) — do that check at the end of this plan (Task 8), not here.

- [ ] **Step 1: `ci.yml`** — CI runs the full matrix (linux x86_64, linux
  arm64, macos arm64) on GitHub-hosted runners, which have 2-4 vCPUs.
  Keep parallelism modest so it doesn't reintroduce the same contention
  that broke Sanitizers. In `.github/workflows/ci.yml`, find:

  ```yaml
      - name: Run full C test suite
        # 40+ cases / 1000+ assertions. Each case spawns its own daemon on
        # a free port at a unique tmpdir, so cases are CWD-independent and
        # parallel-safe within the runner.
        run: ./build/bin/shard-db-test run-all
  ```

  Replace with:

  ```yaml
      - name: Run full C test suite
        # 40+ cases / 1000+ assertions. Each case spawns its own daemon on
        # a free port at a unique tmpdir, so cases are CWD-independent and
        # parallel-safe within the runner. --jobs pinned explicitly:
        # GitHub-hosted runners are 2-4 vCPU, and leaving --jobs unset
        # defaults to sysconf(_SC_NPROCESSORS_ONLN), which over-subscribes
        # these shared/weak runners and caused real contention-driven
        # request timeouts (see docs/plans/2026-07-15-concurrency-and-leak-fixes.md).
        run: ./build/bin/shard-db-test run-all --jobs 4
  ```

- [ ] **Step 2: `codecov.yml`** — must stay **sequential**; the step's own
  comment documents why (gcov counters aren't safe across overlapping test
  cases). In `.github/workflows/codecov.yml`, find:

  ```yaml
      - name: Run full C test suite (sequential)
        # gcov counters aren't safe across forked processes when test
        # cases overlap. The test runner already spawns one daemon per
        # case sequentially, so .gcda files stay coherent without extra
        # serialization.
        run: ./build/bin/shard-db-test run-all
  ```

  Replace with:

  ```yaml
      - name: Run full C test suite (sequential)
        # gcov counters aren't safe across forked/overlapping test cases.
        # run-all now defaults to parallel (--jobs unset = nproc) since
        # the parallel test runner landed, so --jobs 1 must be passed
        # explicitly here to keep this step actually sequential.
        run: ./build/bin/shard-db-test run-all --jobs 1
  ```

- [ ] **Step 3: `sanitizers.yml`** — ASan/UBSan instrumentation is CPU- and
  memory-heavy per test; keep parallelism low to avoid both slowdown-driven
  timeouts and contention. In `.github/workflows/sanitizers.yml`, find:

  ```yaml
      - name: Run full C test suite under ASan + UBSan
        run: ./build/bin/shard-db-test run-all
  ```

  Replace with:

  ```yaml
      - name: Run full C test suite under ASan + UBSan
        # --jobs pinned low: ASan/UBSan instrumentation is CPU/memory
        # heavy per test case, and this runner is shared/weak — full-nproc
        # default parallelism caused contention-driven request timeouts.
        run: ./build/bin/shard-db-test run-all --jobs 2
  ```

- [ ] **Step 4: build.** `SKIP_TESTS=1 ./build.sh`. Paste output.
- [ ] **Step 5: local sanity.** Run
  `./build/bin/shard-db-test run-all --jobs 1` and
  `./build/bin/shard-db-test run-all --jobs 4` locally; both must report
  the same `total: N passed, 0 failed across M cases` line. Paste both.
  (Real workflow-green verification happens in Task 8, after Tasks 2-7
  land — the YAML fix alone won't turn Sanitizers/Coverage green if the
  btree race or leaks are still present and happen to get exercised.)

## Task 2: TDD regression test for the `bt_cache` eviction race

Write this test **before** touching `btree.c`, so it demonstrates the
crash against the current code first.

**Files:**
- Create: `src/test/cases/test_btcache_evict_race.c`
- Modify: `build.sh` (register the new case)

**Design** (verified against the current `btree.c`/`btree.h`, not the
stale sketch from earlier investigation notes — in particular,
`bt_cache_init(cap)` clamps `cap` to a **minimum of 16**
(`if (cap < 16) cap = 16;`), giving `bt_cache_slots =
bt_next_pow2(16*2) = 32` and an eviction threshold of
`bt_cache_count >= bt_cache_slots/2 == 16`. Critically, that check runs
at the *start* of `bt_acquire`, against the count *before* the current
insert's own slot is installed — so the Nth insert's eviction check sees
`bt_cache_count == N-1`. The test must therefore install **17 total
paths** (1 iterator-held "victim" + 16 fillers), not 16 — the 17th
insert is the first one whose pre-insert count (16) actually meets the
threshold. (An earlier draft of this plan used 16 total paths / 15
fillers, which only ever reaches a pre-insert count of 15 and never
triggers eviction at all — that version of the test would pass on both
buggy and fixed code, defeating its own purpose. Corrected here.):

1. `fork()` — the crash-prone section runs in a child process so a
   pre-fix SIGSEGV/SIGBUS only kills the child; the parent reports the
   outcome via `waitpid`/`WIFSIGNALED`.
2. In the child: `bt_cache_shutdown()` then `bt_cache_init(16)` for a
   small, deterministic cache (this repo already uses the
   shutdown-then-reinit-small idiom in `test_btree_inplace_leaf.c`, and
   `bt_acquire()` falls back safely to uncached direct-mmap whenever
   `bt_cache` is left `NULL` afterward — confirmed no new hazard from
   following the same pattern here).
3. `btree_insert(pathA, ...)` — installs `pathA`'s slot (`bt_cache_count`
   goes from 0 to 1, `last_access` = 1, the lowest of anything that
   follows).
4. `btree_range_iter_open(pathA, ...)` — takes and holds `pathA`'s slot
   rdlock for the rest of the child process's life, standing in for any
   real long-lived concurrent reader (this is exactly the documented
   `BtRangeIter` contract, not a synthetic hack).
5. `btree_insert` 16 more distinct paths (`pathA`'s slot untouched by
   any of these — each is a fresh writer `bt_acquire` on a new path).
   The 16th (final) filler insert is the first whose pre-insert
   `bt_cache_count` reaches 16 (inserts 1 through 16 overall — pathA plus
   the first 15 fillers — each check a pre-insert count of 0 through 15,
   never meeting the threshold; the 17th insert overall, i.e. this final
   filler, checks a pre-insert count of 16), meeting the eviction
   threshold; LRU picks the globally-oldest `last_access`, which is still
   `pathA`'s (bumped once at iterator-open time, then never again, while
   every other path gets a strictly larger clock value).
6. `btree_range_iter_next(it, ...)` on the now-evicted-out-from-under-it
   iterator. **Pre-fix**: `bt_cache_evict_slot` unconditionally cleared
   and `bt_dispose_mapping` `munmap`'d/`close`'d the mapping the iterator
   is still holding open via its rdlock → reading through `it` touches
   unmapped memory → SIGSEGV/SIGBUS, child dies by signal. **Post-fix**:
   the eviction scan skips `pathA`'s busy slot and evicts a different
   (idle) one instead; the iterator reads the correct single entry back.
7. Assert (in the parent, after `waitpid`): child did **not** die by
   signal, exited 0, and (encoded via a distinct exit code from the child
   if the readback assertion inside the child itself failed) the readback
   matched.

Create `src/test/cases/test_btcache_evict_race.c`:

```c
/* src/test/cases/test_btcache_evict_race.c
 *
 * Regression test for a use-after-unmap race in btree.c's bt_cache LRU
 * eviction: bt_cache_evict_slot() used to detach (and bt_dispose_mapping
 * then munmap/close) a cache slot's mapping without checking whether the
 * slot's per-entry rwlock was currently held by a long-lived reader (e.g.
 * a BtRangeIter, which the API contract holds the rdlock for the
 * iterator's entire lifetime). Runs the crash-prone section in a forked
 * child so a pre-fix SIGSEGV/SIGBUS only kills the child; the parent
 * reports pass/fail via waitpid.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static int run_child(const char *base) {
    bt_cache_shutdown();
    bt_cache_init(16); /* clamped to 16 -> 32 slots, evict threshold 16 */

    char pathA[600];
    snprintf(pathA, sizeof(pathA), "%s/a.idx", base);

    uint8_t hash[16];
    memset(hash, 0, sizeof(hash));
    btree_insert(pathA, "v1", 2, hash);

    BtRangeIter *it = btree_range_iter_open(pathA, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) _exit(2);

    /* bt_acquire's eviction check runs against the PRE-insert count (see
       btree.c:707), so this needs 16 filler inserts (not 15) — pathA plus
       the first 15 fillers each see a pre-insert count of 0..15 (never
       >=16); only the 16th filler's pre-insert count of 16 crosses the
       threshold and triggers the LRU eviction scan. */
    for (int i = 0; i < 16; i++) {
        char p[600];
        snprintf(p, sizeof(p), "%s/d%d.idx", base, i);
        hash[0] = (uint8_t)(i + 1);
        btree_insert(p, "v1", 2, hash);
    }

    const char *v; size_t vl; const uint8_t *h;
    int rc = btree_range_iter_next(it, &v, &vl, &h);
    if (rc != 1 || vl != 2 || memcmp(v, "v1", 2) != 0) _exit(3);

    btree_range_iter_close(it);
    _exit(0);
}

static int test_btcache_evict_race_run(void) {
    char base[] = "/tmp/shard-db-btcache-race-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    pid_t pid = fork();
    if (pid < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid == 0) run_child(base); /* never returns */

    int status = 0;
    waitpid(pid, &status, 0);

    ASSERT_TRUE(!WIFSIGNALED(status),
        "held cache slot survives concurrent LRU eviction (no crash)");
    if (WIFSIGNALED(status))
        TAP_DIAG("# child killed by signal %d\n", WTERMSIG(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "iterator reads correct data after surviving eviction pressure");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btcache-evict-race", test_btcache_evict_race_run)
```

- [ ] **Step 2: register in `build.sh`.** Find (exact text, in the
  `shard-db-test` link list):

  ```
      src/test/cases/test_btree_inplace_leaf.c \
      src/test/cases/test_btree_value_hash_sort.c \
  ```

  Replace with:

  ```
      src/test/cases/test_btree_inplace_leaf.c \
      src/test/cases/test_btcache_evict_race.c \
      src/test/cases/test_btree_value_hash_sort.c \
  ```

- [ ] **Step 3: build and confirm the test fails for the right reason
  (pre-fix).** `SKIP_TESTS=1 ./build.sh`, then
  `./build/bin/shard-db-test run test-btcache-evict-race`. Expect a
  `not ok` on the "no crash" assertion (or the whole process aborting if
  the parent's own memory gets corrupted by the child's crash somehow —
  if so, note that in the output; the fork isolation is specifically
  designed to prevent this, so it should not happen). Paste the real
  output. Also run once under `BUILD_MODE=asan` per the dynamic-safety
  gate — ASan will likely report the invalid access with a clearer stack
  trace than a bare SIGSEGV; paste that too.

## Task 3: fix the `bt_cache` eviction race

**Files:**
- Modify: `src/db/btree.c`

**Call sites of `bt_cache_evict_slot`** (enumerated via
`grep -rn bt_cache_evict_slot src/`): exactly 2 — `bt_cache_drop_slot`
(line ~480) and `bt_acquire`'s LRU-eviction block (line ~717). Both are
updated in this task.

- [ ] **Step 1: make `bt_cache_evict_slot` non-blocking and
  contract-honest.** Find (exact text):

  ```c
  /* Detach a slot's resources under bt_cache_lock without doing any syscalls.
     The caller disposes the returned fd/map AFTER releasing bt_cache_lock via
     bt_dispose_mapping(). Same no-rwlock-holder contract as before. */
  static void bt_cache_evict_slot(int slot, int *out_fd, uint8_t **out_map,
                                  size_t *out_sz) {
      BtCacheEntry *e = &bt_cache[slot];
      *out_fd = -1; *out_map = NULL; *out_sz = 0;
      if (!e->used) return;
      *out_fd = e->fd;
      *out_map = e->map;
      *out_sz = e->map_size;
      e->map = NULL;
      e->fd = -1;
      e->map_size = 0;
      e->used = 0;
      e->path[0] = '\0';
      bt_cache_count--;
  }
  ```

  Replace with:

  ```c
  /* Detach a slot's resources under bt_cache_lock without doing any syscalls.
     The caller disposes the returned fd/map AFTER releasing bt_cache_lock via
     bt_dispose_mapping(). Non-blocking: tries the slot's own rwlock with
     pthread_rwlock_trywrlock before touching anything. A held rwlock means a
     long-lived holder is mid-use (e.g. a BtRangeIter, which holds rdlock for
     its entire lifetime per btree.h) — clearing the slot out from under that
     holder and then munmap/close-ing its mapping (in bt_dispose_mapping) is a
     use-after-unmap. Returns 0 on success (slot detached, out params filled),
     -1 if the slot is currently held (out params left at -1/NULL/0, slot
     untouched) — callers must treat -1 as "try a different slot". */
  static int bt_cache_evict_slot(int slot, int *out_fd, uint8_t **out_map,
                                 size_t *out_sz) {
      BtCacheEntry *e = &bt_cache[slot];
      *out_fd = -1; *out_map = NULL; *out_sz = 0;
      if (!e->used) return 0;
      if (pthread_rwlock_trywrlock(&e->rwlock) != 0) return -1;
      *out_fd = e->fd;
      *out_map = e->map;
      *out_sz = e->map_size;
      e->map = NULL;
      e->fd = -1;
      e->map_size = 0;
      e->used = 0;
      e->path[0] = '\0';
      bt_cache_count--;
      pthread_rwlock_unlock(&e->rwlock);
      return 0;
  }
  ```

- [ ] **Step 2: `bt_acquire`'s LRU-eviction block becomes a bounded scan
  that skips busy slots.** Find (exact text):

  ```c
      /* Evict LRU when over half-full or the probe couldn't find an empty slot. */
      if (slot < 0 || bt_cache_count >= bt_cache_slots / 2) {
          int lru = -1;
          uint64_t oldest = UINT64_MAX;
          for (int i = 0; i < bt_cache_slots; i++) {
              if (bt_cache[i].used && bt_cache[i].last_access < oldest) {
                  oldest = bt_cache[i].last_access;
                  lru = i;
              }
          }
          if (lru >= 0) {
              bt_cache_evict_slot(lru, &vic_fd, &vic_map, &vic_sz);
              slot = lru;
          }
      }
  ```

  Replace with:

  ```c
      /* Evict LRU when over half-full or the probe couldn't find an empty
         slot. Bounded scan: a candidate slot may be busy (a long-lived
         holder such as a BtRangeIter has its rwlock locked) — skip it and
         try the next-oldest candidate rather than blocking or clobbering
         it. Gives up after 8 attempts and serves the request uncached
         (slot stays -1) rather than looping indefinitely on a cache that's
         entirely full of busy slots. */
      if (slot < 0 || bt_cache_count >= bt_cache_slots / 2) {
          uint64_t floor_ts = 0;
          for (int attempt = 0; attempt < 8; attempt++) {
              int lru = -1;
              uint64_t oldest = UINT64_MAX;
              for (int i = 0; i < bt_cache_slots; i++) {
                  if (bt_cache[i].used && bt_cache[i].last_access >= floor_ts &&
                      bt_cache[i].last_access < oldest) {
                      oldest = bt_cache[i].last_access;
                      lru = i;
                  }
              }
              if (lru < 0) break; /* no more candidates at all */
              if (bt_cache_evict_slot(lru, &vic_fd, &vic_map, &vic_sz) == 0) {
                  slot = lru;
                  break;
              }
              /* Busy — try the next-oldest candidate. */
              floor_ts = oldest + 1;
          }
      }
  ```

- [ ] **Step 3: `bt_cache_drop_slot` gets a bounded retry-with-sleep
  loop.** This caller (admin/invalidate path — `remove-index`) is not
  hot, so a brief blocking retry is acceptable here (unlike the
  `bt_acquire` hot path in Step 2). Find (exact text):

  ```c
  static void bt_cache_drop_slot(int slot) {
      int fd; uint8_t *map; size_t sz;
      bt_cache_evict_slot(slot, &fd, &map, &sz);
      bt_dispose_mapping(fd, map, sz);
  }
  ```

  Replace with:

  ```c
  static void bt_cache_drop_slot(int slot) {
      int fd; uint8_t *map; size_t sz;
      for (int attempt = 0; attempt < 50; attempt++) {
          if (bt_cache_evict_slot(slot, &fd, &map, &sz) == 0) {
              bt_dispose_mapping(fd, map, sz);
              return;
          }
          struct timespec ts = { 0, 1000000L }; /* 1ms */
          nanosleep(&ts, NULL);
      }
      LOG_WARN(LOG_SUB_BTREE,
          "bt_cache_drop_slot: slot %d still held after 50 retries (50ms), giving up",
          slot);
  }
  ```

  Check `<time.h>` is already included in `btree.c` for `nanosleep`
  (it's used elsewhere in the file already via `struct timespec` — if not,
  add the include).

- [ ] **Step 4: build and confirm the Task 2 regression test now
  passes.** `SKIP_TESTS=1 ./build.sh`, then
  `./build/bin/shard-db-test run test-btcache-evict-race`. Paste output —
  expect all assertions `ok`. Also rerun under `BUILD_MODE=asan` with
  `ASAN_OPTIONS=detect_leaks=1`; paste output — expect clean.
- [ ] **Step 5: full suite sanity.** `./build/bin/shard-db-test run-all
  --jobs 1` and `--jobs 4` (or higher, matching your machine's core
  count) locally. Paste both. No regressions vs. the pre-existing
  baseline.

## Task 4: leak fix #1 — `cmd_edit_fields`'s 11 early returns

**Files:**
- Modify: `src/db/query_schema.c`
- Modify: `src/test/cases/test_edit_field.c` (4 new assertions — the other
  7 of 11 leak sites are already exercised by existing assertions in this
  file and in `test_enum.c`, see Background reading)

- [ ] **Step 1: extend `test_edit_field.c` to cover the 3 currently-
  untested easily-reachable leak sites** (`:removed` marker, invalid
  field line, invalid field name — all checked at parse time in
  `cmd_edit_fields`, before the target field is even looked up, so they
  don't depend on the test object's existing schema). Find the existing
  block (verified verbatim against the file at
  `src/test/cases/test_edit_field.c:207-212`):

  ```c
      /* === 11. duplicate-edit-in-request refused ============================== */
      tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                     "\"fields\":[\"name:varchar:128\",\"name:varchar:256\"]}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "duplicate edit refused");
      ASSERT_CONTAINS(resp, "Duplicate", "duplicate reason");
      free(resp); resp = NULL;
  ```

  Insert immediately after it:

  ```c

      /* === 11b. ':removed' marker rejected on edit-field (use remove-field) === */
      tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                     "\"fields\":[\"bio:removed\"]}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "':removed' marker rejected on edit-field");
      ASSERT_CONTAINS(resp, "use remove-field", "removed-marker reason");
      free(resp); resp = NULL;

      /* === 11c. invalid field line (no ':' spec) rejected ====================== */
      tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                     "\"fields\":[\"justaname\"]}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "invalid field line rejected");
      ASSERT_CONTAINS(resp, "Invalid field line", "invalid field line reason");
      free(resp); resp = NULL;

      /* === 11d. invalid field name rejected ==================================== */
      tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                     "\"fields\":[\"bad name:int\"]}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "invalid field name rejected");
      ASSERT_CONTAINS(resp, "Invalid field name", "invalid field name reason");
      free(resp); resp = NULL;
  ```

  Verified against `query_schema.c`'s `edit_valid_name` (line 147-158):
  it rejects names that are empty, `>=128` bytes, or contain `: + / \n \r
  space tab`. `"bad name:int"` is deliberately chosen so
  `parse_field_line` (which splits only on the *first* `:`, `query_find.c`
  line 720-738) succeeds — `name = "bad name"`, type-spec `"int"` parses
  fine — so this reaches and fails `edit_valid_name` specifically (site
  3, line 384-386), not the "invalid field line" check (site 2, line
  380-382, which only fires when there's no `:` at all or the type-spec
  itself doesn't parse). This is why `"justaname"` (no colon) is used for
  site 2's test above instead.

- [ ] **Step 2: add the enum 2-byte-to-1-byte narrow-refusal case to
  `test_enum.c`.** Find the existing enum-rename block (exact text —
  confirm against the file; this mirrors the existing
  `"rename no flag: error"` assertion's surrounding structure) and add,
  after the object under test already has an enum field with `enum_width
  == 2` (>256 values) or, more simply, add a small dedicated block:
  create a fresh object with a 2-byte enum (`>256` comma-separated
  values — generate programmatically in the test, don't hand-write 257
  literals) and attempt an edit-field down to a short (`<=256`-value,
  1-byte) list; assert `"\"error\""`. If constructing a real >256-value
  enum object is disproportionate effort relative to the leak's severity,
  it is acceptable to skip this one specific site with a comment noting
  why, and rely on code-review symmetry with the adjacent (already fully
  covered) shrink-refusal branch instead — **do not invent a fake/looser
  test just to hit the line**. Use judgement; if skipping, note it
  explicitly in the PR description.

- [ ] **Step 3: build and confirm the new/extended tests fail for the
  right functional reason is not the point here — they should already
  pass today** (these are existing validation behaviors, only the *leak*
  is new). Run `SKIP_TESTS=1 ./build.sh` then
  `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` (two separate builds — do not
  mix), and under the ASan build run:

  ```
  ASAN_OPTIONS=detect_leaks=1 ./build/bin/shard-db-test run test-edit-field
  ASAN_OPTIONS=detect_leaks=1 ./build/bin/shard-db-test run test-enum
  ```

  Paste both outputs — expect LeakSanitizer to report the `config.c:1293`
  / `config.c:1302` leak (matching the original ASan report) confirming
  the pre-fix leak is real and reachable via this test.

- [ ] **Step 4: apply the fix.** In `src/db/query_schema.c`'s
  `cmd_edit_fields`, insert
  `for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);`
  immediately before each of the following 11 `return 1;` statements
  (each is inside the `for (int e = 0; e < n_edits; e++)` loop — verify
  the surrounding line before editing since several lines read
  `return 1;` verbatim and are only distinguished by their preceding
  `OUT(...)` call; match on the full block, not just the return line):

  1. After `OUT("{\"error\":\"Cannot edit with ':removed' marker; use remove-field\"}\n");`
  2. After `OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[e]);`
  3. After `OUT("{\"error\":\"Invalid field name: %s\"}\n", parsed[e].name);`
  4. After `OUT("{\"error\":\"Duplicate edit for field [%s] in request\"}\n", parsed[e].name);`
  5. After `OUT("{\"error\":\"Field [%s] not found\"}\n", parsed[e].name);`
  6. After `OUT("{\"error\":\"Field [%s] is tombstoned; cannot edit\"}\n", parsed[e].name);`
  7. After the cross-type-refusal `OUT("{\"error\":\"Cross-type edit refused for [%s]; use add-field <new> + remove-field <old> + bulk-update\"}\n", parsed[e].name);`
  8. After the enum-shrink-refusal `OUT("{\"error\":\"enum edit refused for [%s]: cannot remove or shrink the value list ...\"}\n", parsed[e].name);`
  9. After the enum-2B-to-1B-narrow-refusal `OUT("{\"error\":\"enum edit refused for [%s]: cannot narrow 2-byte → 1-byte enum.\"}\n", parsed[e].name);`
  10. After the enum-value-list-corrupt `OUT("{\"error\":\"enum edit for [%s]: internal value list corrupt at position %d\"}\n", parsed[e].name, i);`
  11. After the enum-rename-without-allow_rename `OUT("{\"error\":\"enum edit refused for [%s]: at least one value at an existing position changed ...\"}\n", parsed[e].name);`

  Example for site 1 (find, exact text):

  ```c
          if (strstr(lines[e], ":removed")) {
              OUT("{\"error\":\"Cannot edit with ':removed' marker; use remove-field\"}\n");
              return 1;
          }
  ```

  Replace with:

  ```c
          if (strstr(lines[e], ":removed")) {
              OUT("{\"error\":\"Cannot edit with ':removed' marker; use remove-field\"}\n");
              for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
              return 1;
          }
  ```

  Apply the same `for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);`
  insertion (immediately before `return 1;`, after the `OUT(...)` call) at
  each of the other 10 sites, matching each site's exact surrounding text
  from the file at investigation time.

  Note: at sites 1-2, `parsed[e]` has not necessarily had
  `parse_field_type` populate `enum_values` yet at the point of the
  return for site 1 specifically (it returns before `parse_field_line` is
  even called) — `free_enum_values` is a guarded no-op on a still-zeroed
  `TypedField` (`if (!f || !f->enum_values) return;`), so including index
  `e` in the loop bound uniformly at every site is always safe, matching
  the existing idiom used at the function's two already-correct success-path
  cleanup loops.

- [ ] **Step 5: rebuild under ASan, rerun the same two test cases with
  leak detection, confirm clean.** Paste output — this is the
  "reapply → confirm-pass" half of the required evidence.
- [ ] **Step 6: full suite sanity** (`--jobs 1` and parallel). Paste
  output.

## Task 5: leak fix #2 — `dispatch_json_query`'s 4 early returns

**Files:**
- Modify: `src/db/server.c`
- Create: `src/test/cases/test_dispatch_leak_paths.c` (no existing test
  exercises negative-offset `find` or `compact` mode at all — confirmed
  via `grep -rl "offset must not be negative\|\"mode\":\"compact\""
  src/test/cases/*.c` returning nothing)
- Modify: `build.sh` (register the new case)

- [ ] **Step 1: write the regression test.** Covers: (a) `find` with
  `"offset":-1` (leak site at server.c ~line 1620), (b) `compact` on a
  nonexistent object (leak site ~line 1840), (c) `compact` success on a
  real v2 object (exercises the `return;` at ~line 1847, which is
  *correct behavior* already but was never covered by any test — also a
  leak site). The `compact`-failed path (~line 1845) is not black-box
  reachable without inducing an internal `slotcask_compact` failure;
  leaving it uncovered by a live-request test and relying on code
  symmetry with the (now covered) adjacent two `return;` sites in the
  same `else if (strcmp(mode, "compact") == 0)` block — same pragmatic
  scoping call as Task 4 Step 2.

  Create `src/test/cases/test_dispatch_leak_paths.c`:

  ```c
  /* src/test/cases/test_dispatch_leak_paths.c
   *
   * Regression coverage for dispatch_json_query() early-return paths that
   * bypass the function's shared free(mode)/free(dir)/free(object)
   * epilogue: negative-offset find, and compact mode (previously
   * completely untested). Functional correctness of these paths is not
   * new; what's new is exercising them at all so a local ASan
   * detect_leaks=1 run can prove the leak fix.
   */
  #ifndef _GNU_SOURCE
  #define _GNU_SOURCE
  #endif
  #include "test_runner.h"
  #include "test_assert.h"
  #include "test_client.h"
  #include "fixtures.h"
  #include <stdio.h>
  #include <stdlib.h>
  #include <string.h>

  static int test_dispatch_leak_paths_run(void) {
      TestEnv env = {0};
      if (test_env_start(&env) != 0) return 1;

      TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
      TestClient *tc = tc_connect(&cfg);
      ASSERT_NOT_NULL(tc, "connect");
      if (!tc) { test_env_stop(&env); return 1; }

      char *resp = NULL;
      tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
      free(resp); resp = NULL;

      tc_request(tc,
          "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"widgets\","
          "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
      free(resp); resp = NULL;

      tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"widgets\","
                     "\"key\":\"w1\",\"value\":{\"name\":\"gear\"}}", &resp);
      free(resp); resp = NULL;

      /* (a) negative offset on find. */
      tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"widgets\","
                     "\"criteria\":[],\"offset\":-1}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "negative offset rejected");
      free(resp); resp = NULL;

      /* (b) compact on a nonexistent object. */
      tc_request(tc, "{\"mode\":\"compact\",\"dir\":\"default\",\"object\":\"nope\"}", &resp);
      ASSERT_CONTAINS(resp, "\"error\"", "compact on nonexistent object rejected");
      free(resp); resp = NULL;

      /* (c) compact success on a real object. */
      tc_request(tc, "{\"mode\":\"compact\",\"dir\":\"default\",\"object\":\"widgets\"}", &resp);
      ASSERT_CONTAINS(resp, "\"ok\":true", "compact succeeds on real object");
      free(resp); resp = NULL;

      tc_close(tc);
      test_env_stop(&env);
      return t_ctx->failed > 0 ? 1 : 0;
  }

  TEST_REGISTER("test-dispatch-leak-paths", test_dispatch_leak_paths_run)
  ```

- [ ] **Step 2: register in `build.sh`.** Find (exact text):

  ```
      src/test/cases/test_explain.c \
      src/test/cases/test_keyset.c \
  ```

  Replace with:

  ```
      src/test/cases/test_explain.c \
      src/test/cases/test_dispatch_leak_paths.c \
      src/test/cases/test_keyset.c \
  ```

  (If this exact 2-line pair isn't found verbatim in `build.sh`, add the
  new line adjacent to any existing `test_*.c` entry in the same list —
  ordering within the list has no functional significance, this is just
  for reviewability. Stop and write `PLAN_NOTES.md` only if the
  `shard-db-test` link list itself can't be located.)

- [ ] **Step 3: build, run functionally (pre-fix — should already pass,
  behavior isn't changing), then rebuild under ASan and confirm the leak
  is real.**

  ```
  SKIP_TESTS=1 ./build.sh
  ./build/bin/shard-db-test run test-dispatch-leak-paths
  BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
  ASAN_OPTIONS=detect_leaks=1 ./build/bin/shard-db-test run test-dispatch-leak-paths
  ```

  Paste all output — expect the functional run clean, the ASan run to
  report leaked `mode`/`dir`/`object` strings from `json_obj_strdup`.

- [ ] **Step 4: apply the fix.** In `src/db/server.c`'s
  `dispatch_json_query`, four sites:

  1. Find (exact text, in the `find` mode branch):

     ```c
          if (off < 0) {
              OUT("{\"error\":\"offset must not be negative\"}\n");
              free(criteria); free(off_s); free(lim_s); free(fields); free(excl); free(fmt);
              free(delim); free(join); free(ob); free(od); free(cur);
              return;
          }
     ```

     Replace with:

     ```c
          if (off < 0) {
              OUT("{\"error\":\"offset must not be negative\"}\n");
              free(criteria); free(off_s); free(lim_s); free(fields); free(excl); free(fmt);
              free(delim); free(join); free(ob); free(od); free(cur);
              free(mode); free(dir); free(object);
              return;
          }
     ```

  2. Find (exact text, in the `compact` mode branch):

     ```c
          if (!sdb || sdb->format != SLOTCASK_FORMAT_VARIABLE) {
              OUT("{\"error\":\"object not found or not in VARIABLE format\"}\n"); return;
          }
     ```

     Replace with:

     ```c
          if (!sdb || sdb->format != SLOTCASK_FORMAT_VARIABLE) {
              OUT("{\"error\":\"object not found or not in VARIABLE format\"}\n");
              free(mode); free(dir); free(object);
              return;
          }
     ```

  3. and 4. Find (exact text, immediately below, still in the `compact`
     branch):

     ```c
          objlock_wrlock(db_root, object);
          int rc = slotcask_compact(sdb, schema_trim_fn, (void *)ts);
          objlock_wrunlock(db_root, object);
          if (rc != 0) { OUT("{\"error\":\"compact failed\"}\n"); return; }
          OUT("{\"ok\":true}\n");
          return;
     ```

     Replace with:

     ```c
          objlock_wrlock(db_root, object);
          int rc = slotcask_compact(sdb, schema_trim_fn, (void *)ts);
          objlock_wrunlock(db_root, object);
          if (rc != 0) {
              OUT("{\"error\":\"compact failed\"}\n");
              free(mode); free(dir); free(object);
              return;
          }
          OUT("{\"ok\":true}\n");
          free(mode); free(dir); free(object);
          return;
     ```

  Do **not** touch `took_wrlock`/`took_rdlock` handling at these sites —
  confirmed via `mode_is_write()`/`mode_is_schema()` that neither `find`
  nor `compact` sets those flags (verified during investigation: this is
  a pure memory leak, not also a lock leak).

- [ ] **Step 5: rebuild under ASan, rerun `test-dispatch-leak-paths` with
  leak detection, confirm clean.** Paste output.
- [ ] **Step 6: full suite sanity** (`--jobs 1` and parallel). Paste
  output.

## Task 6: leak fix #3 — `bulk_ins_run`'s `wire_for_record`

**Files:**
- Modify: `src/db/query_bulk.c`

No new test needed — `src/test/cases/test_auto_key.c` (existing case
`test-auto-key`, ~line 344-351) already exercises this exact path: a
bulk-insert with one omit-key record and one malformed provided key on a
`seq(...)` auto-key object, asserting the batch is rejected with
`"record 1"` named as the failing index. That's precisely the
`parse_seq_key` failure branch that leaks `wire_for_record`.

- [ ] **Step 1: confirm the leak is real under the existing test.**

  ```
  BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
  ASAN_OPTIONS=detect_leaks=1 ./build/bin/shard-db-test run test-auto-key
  ```

  Paste output — expect LeakSanitizer to report a leaked `strdup`
  allocation from `query_bulk.c:1009` (or the current line number at
  investigation time — confirm it matches).

- [ ] **Step 2: apply the fix.** In `src/db/query_bulk.c`'s
  `bulk_ins_run`, two sites (both inside the
  `if (data_ptr && auto_key_mode != AK_NONE) { if (id) { ... } }` block).
  Find (exact text):

  ```c
              if (auto_key_mode == AK_UUID) {
                  uint8_t bin[16];
                  if (parse_uuid_string(id, bin) == 0) {
                      id = (char *)arena_alloc(&arena, 16);
                      memcpy(id, bin, 16);
                      klen = 16;
                  } else {
                      if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                      errors++;
                      id = NULL;
                  }
              } else { /* AK_SEQ */
                  int64_t v;
                  if (parse_seq_key(id, &v) == 0) {
                      id = (char *)arena_alloc(&arena, 8);
                      for (int b = 7; b >= 0; b--) { id[b] = (char)(v & 0xFF); v >>= 8; }
                      klen = 8;
                  } else {
                      if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                      errors++;
                      id = NULL;
                  }
              }
  ```

  Replace with:

  ```c
              if (auto_key_mode == AK_UUID) {
                  uint8_t bin[16];
                  if (parse_uuid_string(id, bin) == 0) {
                      id = (char *)arena_alloc(&arena, 16);
                      memcpy(id, bin, 16);
                      klen = 16;
                  } else {
                      if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                      errors++;
                      /* Parse failed: id is nulled below, so the id&&data_ptr
                         block that would otherwise store-or-free
                         wire_for_record is never reached — free it here. */
                      free(wire_for_record);
                      wire_for_record = NULL;
                      id = NULL;
                  }
              } else { /* AK_SEQ */
                  int64_t v;
                  if (parse_seq_key(id, &v) == 0) {
                      id = (char *)arena_alloc(&arena, 8);
                      for (int b = 7; b >= 0; b--) { id[b] = (char)(v & 0xFF); v >>= 8; }
                      klen = 8;
                  } else {
                      if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                      errors++;
                      free(wire_for_record);
                      wire_for_record = NULL;
                      id = NULL;
                  }
              }
  ```

- [ ] **Step 3: rebuild under ASan, rerun `test-auto-key` with leak
  detection, confirm clean.** Paste output.
- [ ] **Step 4: full suite sanity** (`--jobs 1` and parallel). Paste
  output.

## Task 7: macOS CI SIGBUS — diagnostic-first task (no fix yet)

Root cause of the `Build & test (macos arm64)` leg's exit-138
(SIGBUS) crash is **not** confidently known. Investigation so far has
ruled out two candidates but not identified the real cause:
- **Ruled out**: the documented `test-objlock-unit` fork-sensitivity
  issue — it completes successfully ("2 passed, 0 failed") in its
  isolated sequential pre-pass, before any parallel worker thread exists
  to fork a daemon. The crash happens later, at the very start of the
  parallel phase.
- **Ruled out**: the `sem_init failed... slot cap disabled` log line —
  confirmed via code reading (`src/db/config.c`'s `slot_init()`) to be a
  safe, already-handled fallback (macOS doesn't support unnamed POSIX
  semaphores; the code has a pass-through with no bounds/indexing
  hazard).

Per CORE-PROCESS.md's root-cause-first rule, do not guess a fix. This
task is diagnostic only.

**Files:** none modified in this task — investigation and a written
report only.

- [ ] **Step 1: reproduce locally if possible.** If a macOS ARM64 machine
  is available, build with `SKIP_TESTS=1 ./build.sh` and run
  `./build/bin/shard-db-test run-all` (default parallel, matching what CI
  did pre-Task-1-fix) repeatedly, trying to reproduce the crash. If no
  macOS machine is available, skip to Step 2 and rely on GitHub Actions
  runs directly (each `gh run view <id> --log` on the macOS matrix leg
  gives the same visibility).
- [ ] **Step 2: push the current branch (with Tasks 1-6 applied) and
  inspect the macOS CI leg's raw log via
  `gh run view <run-id> --log-failed` (or `--log` for full output)**,
  paying attention to: which test case (if any) was mid-flight in each
  worker slot at crash time (the watchdog/worker output printed just
  before the crash), whether `--jobs 4` (from Task 1) changes the crash's
  timing/reproducibility at all vs. the old full-nproc default, and
  whether the process crashes before or after any `test_init_process_db`
  call completes on the affected worker.
- [ ] **Step 3: check whether Task 3's btree fix (or Task 1's `--jobs`
  fix) incidentally resolves it.** SIGBUS is classically raised by
  accessing mmap'd memory beyond a truncated/resized file backing store —
  thematically consistent with (but not proven to be) the same
  `bt_cache` eviction race class this plan already fixes in Task 3. If
  the macOS leg is green after Tasks 1-6 land, say so plainly and close
  this out as "resolved as a side effect of Task 3, unconfirmed
  mechanism" rather than claiming a root cause was found.
- [ ] **Step 4: if still crashing, write up findings** (which test/worker
  was active, stack signature if `dmesg`/crash-report output is
  available via the runner log, whether it's reproducible across
  multiple pushes) as a new section in this plan's PR description or a
  follow-up `docs/plans/` diagnostic note — do not merge a guessed fix
  under time pressure. If it's still unresolved when Task 8's merge gate
  is reached, surface it to the human explicitly before requesting a
  merge go-ahead; do not silently proceed with a red macOS leg.

## Task 8: full verification + merge-gate check

- [ ] **Step 1: full local suite**, both modes:

  ```
  SKIP_TESTS=1 ./build.sh
  ./build/bin/shard-db-test run-all --jobs 1
  ./build/bin/shard-db-test run-all
  ```

  Paste both — 0 failures, same total pass count both ways.

- [ ] **Step 2: full local ASan suite** (dynamic-safety gate, whole
  suite, not just the touched tests — this branch touched shared
  concurrent state in `btree.c`):

  ```
  BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
  ASAN_OPTIONS=detect_leaks=1 ./build/bin/shard-db-test run-all --jobs 2
  ```

  Paste output. Any new leak or sanitizer finding outside the 3 fixed in
  this plan is a stop-and-report, not a silent ignore.

- [ ] **Step 3: push, open PR, wait for all GitHub Actions workflows.**
  Per the standing user instruction, do not request a merge go-ahead
  until every workflow (CI's 3 matrix legs, Coverage, Sanitizers, and any
  other enabled workflow) reports success —
  `gh pr checks <number> --watch` or repeated `gh run list --branch
  <branch>`. If the macOS leg (Task 7) is still red at this point, stop
  and report to the human rather than merging anyway.

- [ ] **Step 4: hand back for review.** Per this repo's execution mode,
  leave the branch **uncommitted locally is not applicable here** — wait,
  correction: per CORE-PROCESS + this repo's standing exception, work
  stays as a diff for Sonnet's review before commit. Do not `git commit`
  or push without the human's go-ahead beyond what's needed to get CI
  signal (pushing a branch and opening a draft PR to see workflow results
  is expected and fine; merging is not).
