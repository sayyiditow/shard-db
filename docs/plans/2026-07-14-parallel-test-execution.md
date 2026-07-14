# Parallel Test Execution — Implementation Plan

**Goal**: `./build/bin/shard-db-test run-all` currently runs all 276 C test
cases strictly sequentially (measured baseline on this 16-core dev machine:
**3:53.74 wall, 101% CPU, 276 cases, 10187 assertions, 0 failed**). Make
`run-all` run test cases in parallel across worker threads by default, while
keeping the existing sequential path available and byte-for-byte unchanged
as an explicit fallback (`--jobs 1`). Success criterion (user-verified,
locally): a plain `./build/bin/shard-db-test run-all` on this machine
completes well under 5 minutes — in practice it should approach
`3:53 / min(jobs, 276)` since every case forks its own isolated daemon.

**Architecture**: reuse the *pattern* already proven in this codebase's
production thread pools (`src/db/parallel.c`'s `parallel_for`/`parallel_for_io`)
— a self-draining work queue where idle workers pull the next unit of work
via an atomic fetch-add, so no worker ever blocks waiting for another to
finish and there is no deadlock potential. The full `parallel.c` machinery
(bounded circular queue, condition variables, nested-call help-drain,
dynamic runtime submission) is overkill here: the test runner's entire
workload (all `TestCaseEntry` registrations) is known upfront before the
first worker starts, so a flat array + a single `_Atomic int next_idx`
fetch-add is sufficient — each of N worker pthreads loops
`idx = atomic_fetch_add(&next, 1); if (idx >= n) break; run test[idx];`.
Per-test TAP output is buffered per-worker via `open_memstream()` and
flushed atomically under a mutex when each test completes, so concurrent
tests' `ok N - ...` lines never interleave. A separate watchdog thread
hard-aborts (`_exit(124)`) if any single test exceeds a timeout, since
safely cancelling a stuck pthread mid-syscall is not feasible in C.

**Tech Stack**: C11, pthreads (`-lpthread`, already linked into
`shard-db-test`), `<stdatomic.h>`, `open_memstream()` (POSIX, glibc + macOS
libc both support it).

## Global Constraints

- No new dependencies (pure pthreads + stdatomic, already available).
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  (and `./build/bin/shard-db-test run-all --filter <substr>` for narrower
  iteration while developing).
- **Execution mode for this repo**: leave work **uncommitted** after
  executing this plan — do not `git add`/`git commit`. A human (Sonnet)
  reviews the raw `git diff` before anything is committed.
- Branch off `main` before starting (fresh branch, name
  `feat/parallel-test-runner` or similar) — do not commit to `main`.
- Do every task **in order**; each task's tests must pass before starting
  the next.
- If a quoted anchor is not found **exactly** in the target file, **stop**
  and write `PLAN_NOTES.md` at the repo root describing the mismatch — do
  not guess or reinterpret the surrounding code.
- If you hit a decision this plan does not cover, **stop and ask** — do
  not improvise.
- Paste real command output for every build/test step — never claim a
  step passed without showing it.

## Background reading (do this before Task 1)

- `src/test/test_runner.c` — the sequential runner (`test_run_all`,
  `run_case`, `test_run_one`, the `TestCaseEntry` linked list `g_head`).
- `src/test/test_runner.h` — the `TestCaseEntry`/`TEST_REGISTER` registry
  and current `test_run_all(const char *filter)` declaration (this plan
  changes its signature to add a `jobs` parameter).
- `src/test/test_assert.h` — `TestCtx` (currently `test_num`/`passed`/
  `failed`/`name`), `_TAP_OK`/`_TAP_FAIL` macros (currently hardcoded
  `printf`).
- `src/test/fixtures.c` / `fixtures.h` — per-test daemon fixture
  (`test_env_start`, `test_env_start_at`). Note the atomic
  `__atomic_fetch_add(&counter, 1, __ATOMIC_RELAXED)` in `test_env_start`
  used for db_root/port uniqueness — already safe for concurrent callers,
  no change needed there.
- `src/db/embedded.c`'s `test_init_process_db()` — sets the *thread-local*
  `g_db` (safe per worker) but also unconditionally writes the
  *process-global* `g_shard_db_instance` pointer with an unguarded
  check-and-set — this is a genuine data race once multiple worker threads
  call it concurrently, fixed in Task 5.
- `src/db/parallel.c` — read `parallel_for`/`pool_worker`/`try_pop_task`
  purely as the architectural reference for the self-draining pattern.
  **Do not** call into or modify this file — the test runner gets its own,
  simpler, standalone implementation (see Architecture above for why).
- Invariants to preserve:
  - `--jobs 1` (or any explicit `jobs<=1`) must produce **exactly** the
    same TAP output the current `test_run_all` produces today — same
    lines, same order, no extra headers. This is the safety fallback and
    must not regress.
  - Every currently-passing test must still pass, in both `--jobs 1` and
    default (parallel) modes.
  - No CI workflow YAML changes are required — `run-all` is invoked with
    no flags in `ci.yml`/`sanitizers.yml`/`codecov.yml` today, and default
    parallel with `--jobs` unset must be a strict speed improvement over
    today's behavior, not a behavior change that could flip a currently
    green run red.
  - `tsan.yml` invokes individual cases via `run <name>`, not `run-all` —
    unaffected by this plan, do not touch it.

## Task 1: TDD anchor — failing meta-test for `--jobs`

Write the regression test first, before any runner changes exist, so it
fails for the right reason (`--jobs` flag unrecognized / `run-all` missing
a jobs parameter) and then passes once Tasks 2–8 land.

**Files:**
- Create: `src/test/cases/test_runner_parallel.c`
- Modify: `build.sh` (register the new case in the `shard-db-test` compile
  command)

**Interfaces:**
- Consumes: the built `./build/bin/shard-db-test` binary as a subprocess
  (via `popen`/`pclose`), exercising the CLI end-to-end — this test does
  **not** link against `test_runner.c` internals.

- [ ] **Step 1: create the meta-test.**

This test shells out to the already-built `shard-db-test` binary twice —
once with `--jobs 1 --filter criteria`, once with `--jobs 4 --filter
criteria` — and asserts: (a) both exit 0, (b) both report the same
`total: N passed, 0 failed across M cases` line (parses it out of the
captured output), (c) the parallel run's raw output contains no TAP line
that looks interleaved (a simple sanity check: every `ok `/`not ok `
line's test number is >= 1, and the count of `ok `/`not ok ` lines
emitted equals the sum of all `# <case>: X passed, Y failed` lines'
X+Y — if output got interleaved mid-line, this arithmetic check fails
because a corrupted line either won't match the `ok/not ok` prefix scan
or the numbers won't reconcile).

The `"criteria"` filter was chosen because it is fast and deterministic
in isolation: **10 cases, 506 assertions, ~2.4s sequential**, verified by
running `./build/bin/shard-db-test run-all --filter criteria` on this
machine prior to writing this plan.

Create `src/test/cases/test_runner_parallel.c`:

```c
/* src/test/cases/test_runner_parallel.c
 *
 * Meta-test: exercises the shard-db-test CLI itself (as a subprocess)
 * to validate --jobs N parallel run-all against the --jobs 1 sequential
 * baseline — same pass/fail totals, no interleaved TAP output.
 */
#include "test_assert.h"
#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Runs "./build/bin/shard-db-test run-all --filter criteria --jobs N",
   captures combined stdout, and returns it as a malloc'd buffer (caller
   frees). Also writes the process exit code to *exit_code. */
static char *run_subcommand(int jobs, int *exit_code) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
             "./build/bin/shard-db-test run-all --filter criteria --jobs %d 2>&1",
             jobs);
    FILE *p = popen(cmd, "r");
    if (!p) { *exit_code = -1; return NULL; }

    size_t cap = 65536, len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    char line[4096];
    while (fgets(line, sizeof(line), p)) {
        size_t l = strlen(line);
        if (len + l + 1 > cap) { cap *= 2; buf = realloc(buf, cap); }
        memcpy(buf + len, line, l + 1);
        len += l;
    }
    int rc = pclose(p);
    *exit_code = WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
    return buf;
}

/* Parses "# total: N passed, M failed across K cases" out of `out`.
   Returns 1 on match (fills *passed/*failed/*cases), 0 if not found. */
static int parse_total(const char *out, int *passed, int *failed, int *cases) {
    const char *p = strstr(out, "# total: ");
    if (!p) return 0;
    return sscanf(p, "# total: %d passed, %d failed across %d cases",
                   passed, failed, cases) == 3;
}

/* Counts lines starting with "ok " or "not ok " (TAP result lines). */
static int count_tap_lines(const char *out) {
    int n = 0;
    const char *p = out;
    while (*p) {
        if (strncmp(p, "ok ", 3) == 0 || strncmp(p, "not ok ", 7) == 0) n++;
        const char *nl = strchr(p, '\n');
        if (!nl) break;
        p = nl + 1;
    }
    return n;
}

/* Sums X+Y across every "# <case>: X passed, Y failed" line. */
static int sum_case_totals(const char *out) {
    int sum = 0;
    const char *p = out;
    while ((p = strstr(p, ": ")) != NULL) {
        int x, y;
        if (sscanf(p, ": %d passed, %d failed", &x, &y) == 2) sum += x + y;
        p += 2;
    }
    return sum;
}

static int test_runner_parallel_matches_sequential(void) {
    int exit_seq = -1, exit_par = -1;
    char *out_seq = run_subcommand(1, &exit_seq);
    char *out_par = run_subcommand(4, &exit_par);

    ASSERT_TRUE(out_seq && out_par, "both subcommand runs captured output");
    if (!out_seq || !out_par) { free(out_seq); free(out_par); return 1; }

    ASSERT_EQ_INT(exit_seq, 0, "sequential run-all exits 0");
    ASSERT_EQ_INT(exit_par, 0, "parallel run-all exits 0");

    int p_passed, p_failed, p_cases, q_passed, q_failed, q_cases;
    int ok_seq = parse_total(out_seq, &p_passed, &p_failed, &p_cases);
    int ok_par = parse_total(out_par, &q_passed, &q_failed, &q_cases);
    ASSERT_TRUE(ok_seq, "sequential output has a parseable total line");
    ASSERT_TRUE(ok_par, "parallel output has a parseable total line");

    if (ok_seq && ok_par) {
        ASSERT_EQ_INT(q_passed, p_passed, "parallel total passed matches sequential");
        ASSERT_EQ_INT(q_failed, p_failed, "parallel total failed matches sequential");
        ASSERT_EQ_INT(q_cases, p_cases, "parallel case count matches sequential");
        ASSERT_EQ_INT(p_failed, 0, "sequential run has 0 failures");
    }

    /* No interleaving: TAP line count must equal the sum of all
       per-case passed+failed totals, for both runs. */
    ASSERT_EQ_INT(count_tap_lines(out_seq), sum_case_totals(out_seq),
                  "sequential TAP line count reconciles with per-case totals");
    ASSERT_EQ_INT(count_tap_lines(out_par), sum_case_totals(out_par),
                  "parallel TAP line count reconciles with per-case totals (no interleaving)");

    free(out_seq);
    free(out_par);
    return 0;
}

TEST_REGISTER("test-runner-parallel", test_runner_parallel_matches_sequential)
```

- [ ] **Step 2: register the new case file in `build.sh`.**

Find this exact line **in `build.sh`** (part of the `shard-db-test`
compile command):

```
    src/test/cases/test_json_aggregate_order_case.c \
    src/db/util.c \
```

Replace with:

```
    src/test/cases/test_json_aggregate_order_case.c \
    src/test/cases/test_runner_parallel.c \
    src/db/util.c \
```

- [ ] **Step 3: confirm it fails for the right reason.**

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-runner-parallel
```

Expected at this point: build succeeds (the new file only uses existing
headers), but the test itself fails — `run-all` doesn't understand
`--jobs` yet, so both subprocess invocations either error or silently
ignore the flag and just run sequentially twice; the exact failure mode
doesn't matter, what matters is it's red before Task 2–8 land. Paste the
real output.

## Task 2: per-test output buffering (`TestCtx.out` + `TAP_DIAG`)

**Files:**
- Modify: `src/test/test_assert.h`

**Interfaces:**
- Produces: `TestCtx.out` (a `FILE *`, `NULL` = write straight to real
  `stdout`), the `TAP_DIAG(fmt, ...)` macro.
- Consumes: nothing new.

- [ ] **Step 1: add the `out` field and the `_TAP_OUT` redirect macro.**

Find this exact block **in `src/test/test_assert.h`**:

```c
typedef struct {
    int test_num;       /* 1-based counter within current case */
    int passed;
    int failed;
    const char *name;
} TestCtx;

extern __thread TestCtx *t_ctx;

#define _TAP_OK(desc)   do { t_ctx->test_num++; t_ctx->passed++; \
    printf("ok %d - %s\n", t_ctx->test_num, (desc)); } while (0)

#define _TAP_FAIL(desc, fmt, ...) do { t_ctx->test_num++; t_ctx->failed++; \
    printf("not ok %d - %s\n#   " fmt "\n", t_ctx->test_num, (desc), ##__VA_ARGS__); } while (0)
```

Replace with:

```c
typedef struct {
    int test_num;       /* 1-based counter within current case */
    int passed;
    int failed;
    const char *name;
    FILE *out;           /* NULL = write straight to real stdout (list /
                             run / --jobs 1); non-NULL = per-worker
                             open_memstream buffer under parallel run-all,
                             flushed atomically once the case completes. */
} TestCtx;

extern __thread TestCtx *t_ctx;

#define _TAP_OUT (t_ctx->out ? t_ctx->out : stdout)

#define _TAP_OK(desc)   do { t_ctx->test_num++; t_ctx->passed++; \
    fprintf(_TAP_OUT, "ok %d - %s\n", t_ctx->test_num, (desc)); } while (0)

#define _TAP_FAIL(desc, fmt, ...) do { t_ctx->test_num++; t_ctx->failed++; \
    fprintf(_TAP_OUT, "not ok %d - %s\n#   " fmt "\n", t_ctx->test_num, (desc), ##__VA_ARGS__); } while (0)

/* For diagnostic/skip/progress lines emitted by test bodies outside the
   ASSERT_* macros. Routes through the same stream as TAP output so
   parallel run-all's per-test buffering never interleaves these with
   another concurrently-running test's lines. */
#define TAP_DIAG(fmt, ...) fprintf(_TAP_OUT, fmt, ##__VA_ARGS__)
```

Note: existing `TestCtx ctx = { .name = tc->name };`-style initializers
elsewhere (designated-initializer, all other fields zero) already leave
`out` as `NULL` with no changes needed at those call sites — `NULL` is
exactly "write straight to stdout", preserving today's behavior
everywhere except the new parallel path (Task 7).

- [ ] **Step 2: build and run the existing suite sequentially to confirm
zero behavior change.**

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter criteria
```

Expected: `10 cases, 506 assertions, 0 failed` (same as the pre-existing
baseline for this filter) — output byte-for-byte identical in shape to
before this task, since every `TestCtx` still zero-initializes `out` to
`NULL`.

## Task 3: convert the 9 raw `printf()` call sites to `TAP_DIAG`

Raw `printf()` inside a test case body writes straight to the real
process-wide `stdout`, bypassing per-test buffering — under parallel
`run-all` these would interleave with other workers' TAP output regardless
of Task 2's fix. Convert every one found by
`grep -rn '\bprintf(' src/test/cases/`.

**Files:**
- Modify: `src/test/cases/test_registry_single_flight.c`
- Modify: `src/test/cases/test_stress_no_hang.c`
- Modify: `src/test/cases/test_tls.c`
- Modify: `src/test/cases/test_variable_length.c`
- Modify: `src/test/cases/test_auto_vacuum.c`
- Modify: `src/test/cases/test_auto_reshard.c`

**Interfaces:** none (mechanical `printf(` → `TAP_DIAG(` rename at each of
the 9 sites; argument lists are unchanged).

- [ ] **Step 1 — `test_registry_single_flight.c`.** Find this exact block:

```c
    if (!getenv("CI")) {
        printf("# registry-single-flight: burst of %d completed in %ldms\n",
               N_WORKERS, burst_ms);
```

Replace with:

```c
    if (!getenv("CI")) {
        TAP_DIAG("# registry-single-flight: burst of %d completed in %ldms\n",
               N_WORKERS, burst_ms);
```

- [ ] **Step 2 — `test_stress_no_hang.c`, site 1.** Find this exact block:

```c
    if (getenv("CI") && !getenv("SHARD_TEST_STRESS")) {
        printf("ok 1 - skipped on CI (set SHARD_TEST_STRESS=1 to enable)\n");
        return 0;
```

Replace with:

```c
    if (getenv("CI") && !getenv("SHARD_TEST_STRESS")) {
        TAP_DIAG("ok 1 - skipped on CI (set SHARD_TEST_STRESS=1 to enable)\n");
        return 0;
```

- [ ] **Step 3 — `test_stress_no_hang.c`, site 2.** Find this exact block:

```c
    long ops_per_sec = elapsed_ms > 0 ? (total_ops * 1000) / elapsed_ms : 0;
    printf("# stress: %ld ops total (%ld errs), %ld ops/sec, %d probes (max %ld ms)\n",
           total_ops, total_errs, ops_per_sec, wd.probes, wd.max_ms);
```

Replace with:

```c
    long ops_per_sec = elapsed_ms > 0 ? (total_ops * 1000) / elapsed_ms : 0;
    TAP_DIAG("# stress: %ld ops total (%ld errs), %ld ops/sec, %d probes (max %ld ms)\n",
           total_ops, total_errs, ops_per_sec, wd.probes, wd.max_ms);
```

- [ ] **Step 4 — `test_tls.c`.** Find this exact block:

```c
    if (system("command -v openssl >/dev/null 2>&1") != 0) {
        printf("# test-tls: openssl CLI not present, skipping\n");
        return 0;
```

Replace with:

```c
    if (system("command -v openssl >/dev/null 2>&1") != 0) {
        TAP_DIAG("# test-tls: openssl CLI not present, skipping\n");
        return 0;
```

- [ ] **Step 5 — `test_variable_length.c`.** Find this exact block:

```c

    printf("  trim_len_basics: passed\n");
}
```

Replace with:

```c

    TAP_DIAG("  trim_len_basics: passed\n");
}
```

- [ ] **Step 6 — `test_auto_vacuum.c`, both sites in one block.** Find
this exact block:

```c
    if (getenv("SHARD_TEST_FAST")) {
        printf("# auto-vacuum: SHARD_TEST_FAST set, skipping the 90s wake test\n");
    } else {
        printf("# auto-vacuum: sleeping 90s for the first thread tick…\n");
        fflush(stdout);
```

Replace with:

```c
    if (getenv("SHARD_TEST_FAST")) {
        TAP_DIAG("# auto-vacuum: SHARD_TEST_FAST set, skipping the 90s wake test\n");
    } else {
        TAP_DIAG("# auto-vacuum: sleeping 90s for the first thread tick…\n");
        fflush(_TAP_OUT);
```

- [ ] **Step 7 — `test_auto_reshard.c`, site 1.** Find this exact block:

```c
    if (secs_left_in_hour < 90) {
        printf("# auto-reshard: only %ds left in the hour, sleeping past the boundary...\n",
               secs_left_in_hour);
```

Replace with:

```c
    if (secs_left_in_hour < 90) {
        TAP_DIAG("# auto-reshard: only %ds left in the hour, sleeping past the boundary...\n",
               secs_left_in_hour);
```

- [ ] **Step 8 — `test_auto_reshard.c`, site 2.** Find this exact block:

```c
   gives generous slack on top of the 5s startup delay for slow CI. */
    printf("# auto-reshard: waiting up to 20s for the first thread tick...\n");
    fflush(stdout);
```

Replace with:

```c
   gives generous slack on top of the 5s startup delay for slow CI. */
    TAP_DIAG("# auto-reshard: waiting up to 20s for the first thread tick...\n");
    fflush(_TAP_OUT);
```

- [ ] **Step 9: verify no raw `printf(` remains in any test case file.**

```bash
grep -rn '\bprintf(' src/test/cases/ | grep -v TAP_DIAG
```

Expected: no output (every match should now read `TAP_DIAG(`, which
itself expands to an `fprintf` call and won't match this grep).

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter criteria
```

Expected: still `10 cases, 506 assertions, 0 failed`.

## Task 4: eliminate the `SHARD_TEST_QUERY_BUFFER_MB` env-var race

The sole `setenv`/`unsetenv` call site in the suite
(`test_agg_topn_stream.c`) mutates process-wide environment state around a
`test_env_start()` call — safe today only because execution is strictly
sequential. Under parallel `run-all`, another worker thread's concurrent
`getenv()` inside `test_env_start()` could race against this `setenv`/
`unsetenv` pair. Fix: thread the override through an explicit parameter
(`test_env_start_ex`) instead of the process environment.

**Files:**
- Modify: `src/test/fixtures.h`
- Modify: `src/test/fixtures.c`
- Modify: `src/test/cases/test_agg_topn_stream.c`

**Interfaces:**
- Produces: `int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override);`
- Consumes: nothing new. `test_env_start` becomes a thin wrapper over
  `test_env_start_ex`.

- [ ] **Step 1: write a regression test first** — a new case asserting
that two calls to `test_env_start_ex` with different overrides, issued
back-to-back with no shared env var, both produce daemons with the
override actually applied (read back via `stats`/`describe-object`-style
introspection is overkill; simplest robust check: start two envs with
`"1"` and with `NULL` respectively and confirm `test_env_start_ex`
returns 0 for both and that the writes don't clobber each other's
`db.env` file — since each env gets its own `base` dir via the existing
atomic counter, this is really a compile/link/behavior-parity check).
Add to the bottom of `src/test/cases/test_agg_topn_stream.c` (same file,
since it already has the fixtures/fields boilerplate this test needs):

Find this exact anchor **in `src/test/cases/test_agg_topn_stream.c`**
(the existing test that used `setenv`/`unsetenv`):

```c
static int test_topn_stream_bitmap_postfilter_bounded(void) {
    setenv("SHARD_TEST_QUERY_BUFFER_MB", "1", 1);
    TestEnv env = {0};
    int started = test_env_start(&env);
    unsetenv("SHARD_TEST_QUERY_BUFFER_MB");
    if (started != 0) {
        ASSERT_TRUE(0, "test_env_start failed");
        return 1;
    }
```

Replace with (drops the env-var mutation entirely, passes the override
as a parameter — this alone is the regression test: it will fail to
compile until Task 4's Step 2/3 add `test_env_start_ex`, which is the
TDD-red state to confirm before proceeding):

```c
static int test_topn_stream_bitmap_postfilter_bounded(void) {
    TestEnv env = {0};
    int started = test_env_start_ex(&env, "1");
    if (started != 0) {
        ASSERT_TRUE(0, "test_env_start failed");
        return 1;
    }
```

Build now and confirm it fails with an "implicit declaration" /
"undefined reference" error for `test_env_start_ex` — paste the real
compiler output.

- [ ] **Step 2: declare `test_env_start_ex` in `fixtures.h`.** Find this
exact block **in `src/test/fixtures.h`**:

```c
/* Allocate db_root + free port, write a minimal db.env, fork the daemon,
   wait until it accepts connections. Returns 0 on success. */
int test_env_start(TestEnv *env);
```

Replace with:

```c
/* Allocate db_root + free port, write a minimal db.env, fork the daemon,
   wait until it accepts connections. Returns 0 on success. */
int test_env_start(TestEnv *env);

/* Same as test_env_start, but takes the QUERY_BUFFER_MB override
   explicitly instead of reading SHARD_TEST_QUERY_BUFFER_MB from the
   process environment — avoids a setenv/unsetenv race when tests run
   concurrently under parallel run-all. Pass NULL for "no override"
   (daemon default). */
int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override);
```

- [ ] **Step 3: implement in `fixtures.c`** — rename the existing function
to `test_env_start_ex`, take the override as a parameter instead of
`getenv`, and add a thin `test_env_start` wrapper that preserves
exactly today's behavior for every other (unchanged) call site in the
suite.

Find this exact anchor **in `src/test/fixtures.c`**:

```c
int test_env_start(TestEnv *env) {
    if (!env) return -1;
```

Replace with:

```c
int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override) {
    if (!env) return -1;
```

Then find this exact block (a few lines further down, still inside the
same function):

```c
    /* Optional per-test override: a test that needs to exercise the
       per-query memory cap (e.g. forcing a bitmap KeySet past budget)
       sets SHARD_TEST_QUERY_BUFFER_MB before test_env_start, then unsets
       it. Absent → daemon default. Kept out of TestEnv so the many
       uninitialised `TestEnv env;` callers are unaffected. */
    const char *qbuf_mb = getenv("SHARD_TEST_QUERY_BUFFER_MB");
    if (qbuf_mb && *qbuf_mb)
        fprintf(f, "export QUERY_BUFFER_MB=%s\n", qbuf_mb);
```

Replace with:

```c
    /* Optional per-test override: a test that needs to exercise the
       per-query memory cap (e.g. forcing a bitmap KeySet past budget)
       passes qbuf_mb_override via test_env_start_ex. NULL → daemon
       default. Kept out of TestEnv so the many uninitialised
       `TestEnv env;` callers are unaffected. */
    if (qbuf_mb_override && *qbuf_mb_override)
        fprintf(f, "export QUERY_BUFFER_MB=%s\n", qbuf_mb_override);
```

Finally, add the thin wrapper immediately after the function's closing
brace. Find this exact anchor (the end of the just-renamed function,
immediately followed by the next function):

```c
    return 0;
}

int test_env_start_at(TestEnv *env, const char *db_root, int port) {
```

Replace with:

```c
    return 0;
}

int test_env_start(TestEnv *env) {
    return test_env_start_ex(env, getenv("SHARD_TEST_QUERY_BUFFER_MB"));
}

int test_env_start_at(TestEnv *env, const char *db_root, int port) {
```

This keeps every other existing call site (`test_env_start(&env)`,
unchanged elsewhere in the suite) working identically — none of them
ever call `setenv("SHARD_TEST_QUERY_BUFFER_MB", ...)` themselves, so
`getenv` here just returns `NULL` for them, same as before.

- [ ] **Step 4: build and verify.**

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter topn_stream
./build/bin/shard-db-test run-all --filter criteria
```

Expected: both pass, 0 failed. Paste real output.

## Task 5: fix the `g_shard_db_instance` race in `test_init_process_db()`

**Files:**
- Modify: `src/db/embedded.c`

**Interfaces:** none new — internal fix only, no signature change.

- [ ] **Step 1: confirm the race exists as described** (read-only step,
no code change) — `g_db` is `__thread` (safe per worker thread), but
`g_shard_db_instance` is a plain process-global pointer. Once Task 7
lands, multiple worker pthreads call `test_init_process_db()`
concurrently (each with its own thread-local `g_db`, each racing on the
shared `if (!g_shard_db_instance) g_shard_db_instance = g_db;`
check-and-set). This is a genuine data race (concurrent unsynchronized
read-modify-write) even though the practical consequence today (last
writer wins, a slightly "wrong" but still valid `ShardDb *` pointer
ends up cached) is benign for these tests specifically — fix it anyway
since it's real UB and will show up under TSan.

- [ ] **Step 2: guard it with a static mutex.** Find this exact block
**in `src/db/embedded.c`**:

```c
void test_init_process_db(void) {
    if (g_db) return;
    char tmpdir[] = "/tmp/shard-db-unit-XXXXXX";
    if (!mkdtemp(tmpdir)) return;
    shard_db_open_internal(tmpdir);  /* sets g_db as a side effect */
    /* Expose the instance so threads spawned by test code can bind their
       own g_db via the g_shard_db_instance fallback in storage functions. */
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
}
```

Replace with:

```c
void test_init_process_db(void) {
    if (g_db) return;
    char tmpdir[] = "/tmp/shard-db-unit-XXXXXX";
    if (!mkdtemp(tmpdir)) return;
    shard_db_open_internal(tmpdir);  /* sets g_db as a side effect */
    /* Expose the instance so threads spawned by test code can bind their
       own g_db via the g_shard_db_instance fallback in storage functions.
       Guarded: under parallel run-all, multiple worker threads call this
       function concurrently (each with its own thread-local g_db) — an
       unguarded check-and-set on the process-global g_shard_db_instance
       was a genuine data race. */
    static pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&instance_lock);
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
    pthread_mutex_unlock(&instance_lock);
}
```

`pthread.h` is already available here transitively via `types.h`
(`src/db/types.h` includes `<pthread.h>`, and `embedded.c` includes
`types.h`) — no new `#include` needed.

- [ ] **Step 3: build and verify no regression.**

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter criteria
```

Expected: `0 failed`. Paste real output.

## Task 6: shrink the daemon's CPU pool per test (`THREADS=0` → `THREADS=2`)

`THREADS=0` (auto-detect) sizes each spawned test daemon's CPU thread pool
at up to `nproc - 2` (see `src/db/server.c`'s `cmd_server` startup path).
With N test daemons running concurrently under parallel `run-all` on an
N-core machine, `THREADS=0` oversubscribes badly (N daemons × up to
`nproc-2` CPU threads each, all fighting for the same cores). `THREADS=2`
gives each daemon a small, fixed, non-oversubscribing CPU pool — floor is
2 in `cmd_server`'s clamp logic regardless, so `2` is the minimum useful
value, not an arbitrary pick.

(Considered and rejected: also capping `IO_THREADS`. `cmd_server` floors
the I/O pool at `nproc` unconditionally — it cannot be configured below
that. Left as-is: I/O-pool threads spend most of their time blocked on
I/O syscalls rather than spinning on CPU, so per-daemon I/O-pool
oversubscription does not meaningfully compete with the CPU pool or with
other daemons' worker threads for actual CPU time. No fix needed.)

**Files:**
- Modify: `src/test/fixtures.c` (2 sites: `test_env_start`,
  `test_env_start_at`)
- Modify: `src/test/cases/test_auto_reshard.c` (3 identical sites)
- Modify: `src/test/cases/test_auto_vacuum.c` (1 site)
- Modify: `src/test/cases/test_per_tenant_auth.c` (1 site)
- Modify: `src/test/cases/test_tls.c` (1 site)
- Modify: `src/test/cases/test_token_perms.c` (1 site)

**Interfaces:** none — literal string substitution in each file's db.env
template.

- [ ] **Step 1 — `fixtures.c`, `test_env_start_ex`.** Find this exact
block (note: 8-space indentation on the `fprintf` args):

```c
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n",
        env->db_root, env->port, base);
```

Replace with:

```c
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n",
        env->db_root, env->port, base);
```

- [ ] **Step 2 — `fixtures.c`, `test_env_start_at`.** Find this exact
block (note: 12-space indentation — this is the nested-in-`if`
duplicate, distinct from Step 1's block):

```c
        fprintf(f,
            "export DB_ROOT=\"%s\"\n"
            "export PORT=%d\n"
            "export TIMEOUT=0\n"
            "export LOG_DIR=\"%s/logs\"\n"
            "export LOG_LEVEL=2\n"
            "export THREADS=0\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n",
            env->db_root, env->port, base);
```

Replace with:

```c
        fprintf(f,
            "export DB_ROOT=\"%s\"\n"
            "export PORT=%d\n"
            "export TIMEOUT=0\n"
            "export LOG_DIR=\"%s/logs\"\n"
            "export LOG_LEVEL=2\n"
            "export THREADS=2\n"
            "export FCACHE_MAX=4096\n"
            "export TLS_ENABLE=0\n",
            env->db_root, env->port, base);
```

If this exact block isn't found (e.g. whitespace differs from what's
quoted here), stop and write `PLAN_NOTES.md` rather than guessing —
do not fall back to a fuzzy match.

- [ ] **Step 3 — `test_auto_reshard.c` (3 identical occurrences).** This
file has the same block repeated 3 times verbatim (verified: all 3 are
byte-identical). Find this exact block, **replace all 3 occurrences**:

```c
        "export LOG_LEVEL=3\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

Replace with (apply to all 3 matches in the file):

```c
        "export LOG_LEVEL=3\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

- [ ] **Step 4 — `test_auto_vacuum.c`.** Find this exact block:

```c
        "export LOG_LEVEL=3\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

Replace with:

```c
        "export LOG_LEVEL=3\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

- [ ] **Step 5 — `test_per_tenant_auth.c`.** Find this exact block:

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

Replace with:

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

- [ ] **Step 6 — `test_tls.c`.** Find this exact block (note: this one's
4th line is `TLS_ENABLE=%d`, not `=0` — distinguishes it from the other
sites, keep as-is):

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=%d\n"
```

Replace with:

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=%d\n"
```

- [ ] **Step 7 — `test_token_perms.c`.** Find this exact block:

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

Replace with:

```c
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
```

- [ ] **Step 8: confirm no `THREADS=0` remains anywhere in the test
suite.**

```bash
grep -rn 'THREADS=0' src/test/
```

Expected: no output.

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter reshard
./build/bin/shard-db-test run-all --filter vacuum
./build/bin/shard-db-test run-all --filter tls
./build/bin/shard-db-test run-all --filter tenant
./build/bin/shard-db-test run-all --filter token
```

Expected: all pass, `0 failed` each. Paste real output for all five.

## Task 7: the parallel runner itself

**Files:**
- Modify: `src/test/test_runner.h`
- Modify: `src/test/test_runner.c`

**Interfaces:**
- Changes: `int test_run_all(const char *filter);` →
  `int test_run_all(const char *filter, int jobs);` (breaking signature
  change — Task 8 updates the sole call site in `shard-db-test.c`).
- Produces internally: `run_all_sequential` (identical behavior to
  today's `test_run_all` body), `run_all_parallel` (new), a
  `collect_matching` helper shared by both, a watchdog thread.

- [ ] **Step 1: update the declaration.** Find this exact block **in
`src/test/test_runner.h`**:

```c
/* Run a single test by name. Returns 0 on pass, non-zero on fail. */
int test_run_one(const char *name);

/* Run all (optionally filtered by substring). Returns total fail count. */
int test_run_all(const char *filter);
```

Replace with:

```c
/* Run a single test by name. Returns 0 on pass, non-zero on fail. */
int test_run_one(const char *name);

/* Run all (optionally filtered by substring), using `jobs` worker
   threads. jobs<=1 runs strictly sequentially (byte-identical output to
   the pre-parallel implementation) — this is the safety fallback.
   jobs>1 runs a self-draining worker pool: each worker atomically pulls
   the next case index, buffers its TAP output via open_memstream, and
   flushes it atomically under a print mutex on completion, so
   concurrent tests' output never interleaves. A watchdog thread
   _exit(124)s the whole process if any single case exceeds
   SHARD_TEST_WATCHDOG_SEC (default 180s) — a stuck pthread cannot be
   safely cancelled mid-syscall in C, so this mirrors `timeout(1)`'s
   hard-kill convention instead. Returns total fail count. */
int test_run_all(const char *filter, int jobs);
```

- [ ] **Step 2: replace the whole implementation.** Find this exact block
**in `src/test/test_runner.c`** (the file's `#include`s):

```c
/* src/test/test_runner.c */
#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef TEST_BUILD
extern void test_reset_caches(void);
extern void test_init_process_db(void);  /* ensures g_db is set for in-process tests */
#endif
```

Replace with:

```c
/* src/test/test_runner.c */
#include "test_runner.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef TEST_BUILD
extern void test_reset_caches(void);
extern void test_init_process_db(void);  /* ensures g_db is set for in-process tests */
#endif
```

Then find this exact block (the current `test_run_all` — everything
from its definition to end of file):

```c
int test_run_all(const char *filter) {
    int total_fail = 0, ran = 0, total_passed = 0;
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (filter && !strstr(p->name, filter)) continue;
        TestCtx ctx = { .name = p->name };
        t_ctx = &ctx;
        printf("# %s\n", p->name);
#ifdef TEST_BUILD
        test_init_process_db();
#endif
        int rc = p->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        total_fail += ctx.failed;
        total_passed += ctx.passed;
        ran++;
        printf("# %s: %d passed, %d failed\n", p->name, ctx.passed, ctx.failed);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
    }
    printf("1..%d\n", ran);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_fail, ran);
    return total_fail;
}
```

Replace with:

```c
/* Builds a flat array of every registered case matching `filter` (NULL
   = all). Returns the count; *out receives a malloc'd array the caller
   must free. Both the sequential and parallel paths share this so
   filtering behavior is identical between them. */
static int collect_matching(const char *filter, TestCaseEntry ***out) {
    int cap = 16, n = 0;
    TestCaseEntry **arr = malloc((size_t)cap * sizeof(*arr));
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (filter && !strstr(p->name, filter)) continue;
        if (n == cap) {
            cap *= 2;
            TestCaseEntry **np = realloc(arr, (size_t)cap * sizeof(*arr));
            if (!np) { free(arr); *out = NULL; return 0; }
            arr = np;
        }
        arr[n++] = p;
    }
    *out = arr;
    return n;
}

/* Strictly sequential — byte-identical output to the pre-parallel
   implementation this replaces. This is the --jobs 1 fallback. */
static int run_all_sequential(TestCaseEntry **cases, int n) {
    int total_fail = 0, total_passed = 0;
    for (int i = 0; i < n; i++) {
        TestCaseEntry *p = cases[i];
        TestCtx ctx = { .name = p->name };
        t_ctx = &ctx;
        printf("# %s\n", p->name);
#ifdef TEST_BUILD
        test_init_process_db();
#endif
        int rc = p->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        total_fail += ctx.failed;
        total_passed += ctx.passed;
        printf("# %s: %d passed, %d failed\n", p->name, ctx.passed, ctx.failed);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
    }
    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_fail, n);
    return total_fail;
}

/* ---- Parallel path (jobs > 1) ---- */

typedef struct {
    _Atomic(const char *) name;   /* current test name; NULL = worker idle */
    _Atomic long start_ms;
} WorkerSlot;

typedef struct {
    TestCaseEntry **cases;
    int n;
    _Atomic int next;             /* self-draining index, mirrors parallel.c's
                                      atomic fetch-add dispatch pattern */
    _Atomic int total_passed;
    _Atomic int total_failed;
    pthread_mutex_t print_lock;   /* serializes each completed test's buffered
                                      output flush to real stdout */
    WorkerSlot *slots;
    int nslots;
    _Atomic int done;             /* set 1 once all workers have joined, tells
                                      the watchdog to stop polling */
} ParallelCtx;

typedef struct {
    ParallelCtx *pc;
    int slot;
} WorkerArg;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Hard-aborts the whole process if any single test case runs longer
   than the watchdog limit. A stuck pthread (e.g. blocked in a syscall
   against a wedged daemon) cannot be safely cancelled mid-flight in C,
   so this mirrors the `timeout(1)` convention: kill the process, exit
   124, let CI/the human re-run and investigate which case hung. */
static void *watchdog_main(void *arg) {
    ParallelCtx *pc = arg;
    long limit_ms = 180000;
    const char *env = getenv("SHARD_TEST_WATCHDOG_SEC");
    if (env && atoi(env) > 0) limit_ms = (long)atoi(env) * 1000L;

    while (!atomic_load_explicit(&pc->done, memory_order_acquire)) {
        long now = now_ms();
        for (int i = 0; i < pc->nslots; i++) {
            const char *name = atomic_load_explicit(&pc->slots[i].name, memory_order_acquire);
            if (!name) continue;
            long start = atomic_load_explicit(&pc->slots[i].start_ms, memory_order_acquire);
            if (now - start > limit_ms) {
                fflush(stdout);
                fprintf(stderr,
                    "\n# WATCHDOG: test '%s' exceeded %lds on worker %d — aborting run-all\n",
                    name, limit_ms / 1000, i);
                fflush(stderr);
                _exit(124);
            }
        }
        struct timespec req = { 1, 0 };
        nanosleep(&req, NULL);
    }
    return NULL;
}

static void *worker_main(void *arg_) {
    WorkerArg *wa = arg_;
    ParallelCtx *pc = wa->pc;
    int slot = wa->slot;

    for (;;) {
        int idx = atomic_fetch_add_explicit(&pc->next, 1, memory_order_relaxed);
        if (idx >= pc->n) break;
        TestCaseEntry *tc = pc->cases[idx];

        char *buf = NULL;
        size_t bufsz = 0;
        FILE *mem = open_memstream(&buf, &bufsz);
        TestCtx ctx = { .name = tc->name, .out = mem };
        t_ctx = &ctx;

        atomic_store_explicit(&pc->slots[slot].start_ms, now_ms(), memory_order_relaxed);
        atomic_store_explicit(&pc->slots[slot].name, tc->name, memory_order_release);

        fprintf(mem, "# %s\n", tc->name);
#ifdef TEST_BUILD
        test_init_process_db();
#endif
        int rc = tc->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        fprintf(mem, "# %s: %d passed, %d failed\n", tc->name, ctx.passed, ctx.failed);
        fflush(mem);

        atomic_store_explicit(&pc->slots[slot].name, NULL, memory_order_release);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
        atomic_fetch_add_explicit(&pc->total_passed, ctx.passed, memory_order_relaxed);
        atomic_fetch_add_explicit(&pc->total_failed, ctx.failed, memory_order_relaxed);

        pthread_mutex_lock(&pc->print_lock);
        fwrite(buf, 1, bufsz, stdout);
        pthread_mutex_unlock(&pc->print_lock);

        fclose(mem);
        free(buf);
    }
    return NULL;
}

static int run_all_parallel(TestCaseEntry **cases, int n, int jobs) {
    if (jobs > n) jobs = n > 0 ? n : 1;

    printf("# run-all: %d worker(s)\n", jobs);
    fflush(stdout);

    ParallelCtx pc;
    pc.cases = cases;
    pc.n = n;
    atomic_init(&pc.next, 0);
    atomic_init(&pc.total_passed, 0);
    atomic_init(&pc.total_failed, 0);
    atomic_init(&pc.done, 0);
    pthread_mutex_init(&pc.print_lock, NULL);
    pc.nslots = jobs;
    pc.slots = calloc((size_t)jobs, sizeof(*pc.slots));
    for (int i = 0; i < jobs; i++) {
        atomic_init(&pc.slots[i].name, (const char *)NULL);
        atomic_init(&pc.slots[i].start_ms, 0L);
    }

    pthread_t wd;
    pthread_create(&wd, NULL, watchdog_main, &pc);

    pthread_t *threads = malloc((size_t)jobs * sizeof(pthread_t));
    WorkerArg *wargs = malloc((size_t)jobs * sizeof(WorkerArg));
    for (int i = 0; i < jobs; i++) {
        wargs[i].pc = &pc;
        wargs[i].slot = i;
        pthread_create(&threads[i], NULL, worker_main, &wargs[i]);
    }
    for (int i = 0; i < jobs; i++) pthread_join(threads[i], NULL);

    atomic_store_explicit(&pc.done, 1, memory_order_release);
    pthread_join(wd, NULL);

    int total_passed = atomic_load_explicit(&pc.total_passed, memory_order_relaxed);
    int total_failed = atomic_load_explicit(&pc.total_failed, memory_order_relaxed);
    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_failed, n);

    free(threads);
    free(wargs);
    free(pc.slots);
    pthread_mutex_destroy(&pc.print_lock);
    return total_failed;
}

int test_run_all(const char *filter, int jobs) {
    TestCaseEntry **cases = NULL;
    int n = collect_matching(filter, &cases);
    int result = (jobs <= 1) ? run_all_sequential(cases, n)
                              : run_all_parallel(cases, n, jobs);
    free(cases);
    return result;
}
```

Note: `n == 0` (an empty filter match) with `jobs > 1` sets `jobs = 1`
in `run_all_parallel`'s clamp (`n > 0 ? n : 1`), so exactly one no-op
worker thread runs, joins immediately, and the function prints
`1..0` / `0 passed, 0 failed across 0 cases` — matching what the
sequential path already does for an unmatched filter today (`ran` stays
`0`). No special-casing needed beyond the clamp.

- [ ] **Step 3: build.** `test_runner.c` now uses `open_memstream`, which
requires `_POSIX_C_SOURCE >= 200809L` (glibc) or is available directly
on macOS libc — check `$MODE_CFLAGS` in `build.sh` for an existing
`-D_POSIX_C_SOURCE`/`-D_GNU_SOURCE` define before assuming it's needed;
if the build fails with an implicit-declaration warning/error for
`open_memstream`, add `#define _POSIX_C_SOURCE 200809L` as the very
first line of `test_runner.c` (before any `#include`) and rebuild.

```bash
SKIP_TESTS=1 ./build.sh
```

This step will also fail to link until Task 8 updates
`shard-db-test.c`'s call site (`test_run_all(filter)` → 1-arg call is
now a compile error against the 2-arg declaration) — expected, proceed
to Task 8 immediately, then come back and finish verifying this task.

## Task 8: `--jobs N` CLI flag

**Files:**
- Modify: `src/test/shard-db-test.c`

**Interfaces:** none new — CLI argv parsing only.

- [ ] **Step 1: update the run-all branch and usage string.** Find this
exact block **in `src/test/shard-db-test.c`**:

```c
/* src/test/shard-db-test.c */
#include "test_runner.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: shard-db-test list | run <name> | run-all [--filter <substr>]\n");
        return 1;
    }
    const char *cmd = argv[1];
    if (strcmp(cmd, "list") == 0) {
        for (const TestCaseEntry *p = test_first(); p; p = p->next)
            printf("%s\n", p->name);
        return 0;
    }
    if (strcmp(cmd, "run") == 0 && argc >= 3) {
        return test_run_one(argv[2]);
    }
    if (strcmp(cmd, "run-all") == 0) {
        const char *filter = NULL;
        for (int i = 2; i + 1 < argc; i++)
            if (strcmp(argv[i], "--filter") == 0) { filter = argv[i + 1]; break; }
        return test_run_all(filter) == 0 ? 0 : 1;
    }
    fprintf(stderr, "unknown subcommand: %s\n", cmd);
    return 1;
}
```

Replace with:

```c
/* src/test/shard-db-test.c */
#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,
            "usage: shard-db-test list | run <name> | run-all [--filter <substr>] [--jobs N]\n");
        return 1;
    }
    const char *cmd = argv[1];
    if (strcmp(cmd, "list") == 0) {
        for (const TestCaseEntry *p = test_first(); p; p = p->next)
            printf("%s\n", p->name);
        return 0;
    }
    if (strcmp(cmd, "run") == 0 && argc >= 3) {
        return test_run_one(argv[2]);
    }
    if (strcmp(cmd, "run-all") == 0) {
        const char *filter = NULL;
        int jobs = 0;
        for (int i = 2; i + 1 < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0) filter = argv[i + 1];
            else if (strcmp(argv[i], "--jobs") == 0) jobs = atoi(argv[i + 1]);
        }
        if (jobs <= 0) {
            long nproc = sysconf(_SC_NPROCESSORS_ONLN);
            jobs = (nproc > 0) ? (int)nproc : 4;
        }
        return test_run_all(filter, jobs) == 0 ? 0 : 1;
    }
    fprintf(stderr, "unknown subcommand: %s\n", cmd);
    return 1;
}
```

Note the loop no longer `break`s after finding one flag — both
`--filter` and `--jobs` must be discoverable regardless of which comes
first on the command line (`run-all --jobs 4 --filter criteria` and
`run-all --filter criteria --jobs 4` must behave identically).

Default-parallel: omitting `--jobs` entirely (the case every existing
CI workflow hits, since none of them pass it) now defaults to
`nproc()`, not `1` — this is the deliberate behavior change this whole
plan exists to make. `--jobs 1` is the explicit sequential opt-out.

- [ ] **Step 2: build and run the full suite.**

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: clean build, no new warnings.

## Task 9: opt-in watchdog self-test

Prove the watchdog actually fires, gated behind an injection env var
(mirrors the existing `SHARD_TEST_STRESS` pattern) so it never runs in
normal CI — a real 3-minute hang-and-kill test would blow every CI
timeout budget otherwise.

**Files:**
- Modify: `src/test/cases/test_runner_parallel.c`

**Interfaces:** none new.

- [ ] **Step 1: add the opt-in case.** Append to
`src/test/cases/test_runner_parallel.c` (after the existing
`TEST_REGISTER` line, replacing it — find this exact anchor):

```c
TEST_REGISTER("test-runner-parallel", test_runner_parallel_matches_sequential)
```

Replace with:

```c
/* Opt-in only (SHARD_TEST_WATCHDOG_SELFTEST=1) — proves the watchdog
   thread actually fires and hard-aborts on a hung case. Spawns
   run-all with a deliberately-hanging filter target and a 2s watchdog,
   and asserts the child process was killed with exit code 124 within a
   generous 15s wall-clock bound. Not run by default: CI's normal
   timeout budgets aren't built to tolerate a real multi-second
   deliberate hang-and-kill cycle on every run. */
static int test_runner_watchdog_fires(void) {
    if (!getenv("SHARD_TEST_WATCHDOG_SELFTEST")) {
        TAP_DIAG("ok 1 - skipped (set SHARD_TEST_WATCHDOG_SELFTEST=1 to enable)\n");
        return 0;
    }

    /* test-stress-no-hang, run with SHARD_TEST_STRESS unset, exits fast
       (its own internal CI-skip path) — not useful as a "hang" target.
       Instead this reuses the auto-vacuum 90s-sleep case, which sleeps
       well past any short watchdog window when SHARD_TEST_FAST is
       unset — giving the watchdog something real to catch. */
    /* --jobs 2 (not 1): the watchdog thread only runs inside
       run_all_parallel, which the jobs<=1 sequential path never calls.
       This must stay >1 to actually exercise the watchdog. */
    int exit_code = -1;
    FILE *p = popen(
        "SHARD_TEST_WATCHDOG_SEC=2 SHARD_TEST_FAST= "
        "./build/bin/shard-db-test run-all --filter auto-vacuum --jobs 2 2>&1",
        "r");
    ASSERT_TRUE(p != NULL, "popen succeeded");
    if (!p) return 1;

    char line[256];
    while (fgets(line, sizeof(line), p)) { /* drain, discard */ }
    int rc = pclose(p);
    exit_code = WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;

    ASSERT_EQ_INT(exit_code, 124, "watchdog hard-aborts a hung case with exit 124");
    return 0;
}

TEST_REGISTER("test-runner-parallel", test_runner_parallel_matches_sequential)
TEST_REGISTER("test-runner-watchdog", test_runner_watchdog_fires)
```

Registered test names use hyphens (`test-auto-vacuum`, confirmed in
`src/test/cases/test_auto_vacuum.c`), not underscores — only the source
*filename* uses an underscore. `--filter auto_vacuum` matches nothing;
it must be `--filter auto-vacuum`.

Edge case: the `--filter auto-vacuum` match set under `--jobs 2` pulls
in whatever other `auto-vacuum`-named cases exist alongside the slow
90s-sleep one — with only 2 workers draining the matched set, the slow
case is guaranteed to still be running (or about to start) well past
the 2s watchdog window regardless of how the other matched cases in the
set are distributed across the 2 workers, so this remains a reliable
trigger.

Zero-match guard: per `collect_matching`/`run_all_parallel`'s documented
`n == 0` clamp (`jobs = n > 0 ? n : 1`), a filter that matches nothing
must return in well under a second — one no-op worker thread that
breaks out of its fetch-add loop immediately, `1..0` / `0 passed, 0
failed across 0 cases`, no watchdog firing. If a future edit to this
path ever makes a zero-match `run-all` hang instead of no-op'ing fast,
that is a regression in the clamp/short-circuit logic, not expected
behavior — treat it as its own bug, not something this self-test papers
over.

- [ ] **Step 2: verify manually (this test is opt-in, so `run-all`
without the env var won't exercise it).**

```bash
SKIP_TESTS=1 ./build.sh
SHARD_TEST_WATCHDOG_SELFTEST=1 ./build/bin/shard-db-test run test-runner-watchdog
```

Expected: `ok` — the inner subprocess gets killed at ~2s with exit 124,
well inside the outer 15s-generous bound this test's design implies
(there is no explicit outer timeout coded — `pclose` simply blocks
until the child exits, which happens fast once the watchdog fires).
Paste real output.

```bash
./build/bin/shard-db-test run-all --filter criteria
```

Expected: `test-runner-watchdog` is not in this filter's match set
(name doesn't contain "criteria"), so this is just a sanity check that
nothing else broke. `0 failed`.

## Task 10: full suite verification + timing comparison

This is the user's explicit acceptance criterion — run it for real,
locally, and report the real numbers.

**Files:** none (verification only).

- [ ] **Step 1: full sequential baseline (for direct before/after
comparison on this exact build).**

```bash
time ./build/bin/shard-db-test run-all --jobs 1
```

Expected: `276` (or more, +2 for the two new meta-test cases registered
in this plan = `278`) cases, `0 failed`, wall time close to the
pre-existing baseline (`3:53.74`) — some deviation expected since
`THREADS=2` (Task 6) changes each daemon's own internal concurrency
slightly, but this remains the sequential path so wall time should
still land in the same ballpark.

- [ ] **Step 2: full parallel run — the actual acceptance check.**

```bash
time ./build/bin/shard-db-test run-all
```

Expected: same case count, `0 failed`, wall time **well under 5
minutes** (this machine has `nproc=16`; even accounting for
daemon-spawn overhead and the top few slowest individual cases
dominating the tail — e.g. `test_auto_vacuum`'s 90s sleep, which no
amount of parallelism shrinks since it's one case's wall time — total
wall time should land far below the sequential 3:53). Paste the real
`time` output; this is the number the human asked to see with their own
eyes.

- [ ] **Step 3: confirm CI-equivalent invocations still work exactly as
CI calls them** (no flags, matching `ci.yml`/`sanitizers.yml`/
`codecov.yml`'s literal invocation):

```bash
./build/bin/shard-db-test run-all
echo "exit code: $?"
```

Expected: `exit code: 0`.

- [ ] **Step 4: run the new meta-tests standalone one more time, isolated
from the full suite, to confirm they aren't order-dependent.**

```bash
./build/bin/shard-db-test run test-runner-parallel
SHARD_TEST_WATCHDOG_SELFTEST=1 ./build/bin/shard-db-test run test-runner-watchdog
```

Expected: both `ok`.

## Definition of done

- [ ] All steps above pass with real, pasted command output — no step
      claimed done without evidence.
- [ ] `grep -rn '\bprintf(' src/test/cases/` shows zero raw `printf(`
      call sites (all converted to `TAP_DIAG`).
- [ ] `grep -rn 'THREADS=0' src/test/` shows no matches.
- [ ] `grep -rn 'setenv\|unsetenv' src/test/cases/` shows no matches
      (the sole prior site was removed in Task 4).
- [ ] `./build/bin/shard-db-test run-all --jobs 1` output is unchanged in
      shape/content from the pre-plan baseline (modulo the 2 new
      registered cases).
- [ ] `./build/bin/shard-db-test run-all` (default, parallel) completes
      well under 5 minutes on the human's machine, confirmed by the human
      running it themselves.
- [ ] No new compiler warnings introduced by `SKIP_TESTS=1 ./build.sh`.
- [ ] Work is left **uncommitted** (per this repo's standing execution
      mode) for the human/Sonnet review pass on the raw `git diff`.

## Considered and rejected

- **Fixed hardcoded `/tmp` paths in `test_btree.c` / `test_btree_value_hash_sort.c`
  / `test_btree_inplace_leaf.c`.** All three use mutually distinct literal
  filenames (verified via grep), and each test still runs exactly once
  per `run-all` invocation regardless of parallelism — no collision risk,
  no fix needed.
- **Capping `IO_THREADS` alongside `THREADS` in Task 6.** `cmd_server`
  floors the I/O pool at `nproc` unconditionally (cannot be configured
  below that via any env var). Left as-is: I/O-pool threads spend most of
  their time blocked on I/O syscalls, not spinning on CPU, so per-daemon
  I/O-pool oversubscription doesn't meaningfully compete for CPU time the
  way an oversized CPU pool would.
- **Reusing `src/db/parallel.c`'s full `parallel_for` machinery directly
  for the test runner.** That implementation targets *dynamic* runtime
  task submission (bounded circular queue, condition-variable
  coordination, nested-call help-drain for callers already inside a pool
  worker). The test runner's entire workload is known upfront as a flat
  array before the first worker starts, so a single atomic fetch-add
  index is sufficient and considerably simpler — no queue, no
  backpressure, no nested-call reentrancy concerns (test bodies don't
  call back into the test-runner's own pool).
- **Graceful thread cancellation for the watchdog instead of
  `_exit(124)`.** Safely cancelling a pthread stuck mid-syscall (e.g.
  blocked in `read()` against a wedged daemon) is not reliable in C
  without extensive cleanup-handler discipline throughout the whole
  fixture/daemon-client code path. A hard process-level abort mirroring
  `timeout(1)`'s convention is simpler, and the failure mode (rerun,
  investigate which case hung) is acceptable for a test harness.
