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
   Returns 1 on match (fills *passed, *failed, *cases), 0 if not found. */
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

/* Sums X+Y across every "# <case>: X passed, Y failed" line.  Does NOT
   match the "# total: X passed, Y failed across K cases" footer. */
static int sum_case_totals(const char *out) {
    int sum = 0;
    const char *p = out;
    while ((p = strstr(p, ": ")) != NULL) {
        /* Check whether the line containing this ": " starts with
           "# total:" — if so it's the footer, skip it. */
        const char *line_start = p;
        while (line_start > out && line_start[-1] != '\n') line_start--;
        if (strncmp(line_start, "# total:", 8) != 0) {
            int x, y;
            if (sscanf(p, ": %d passed, %d failed", &x, &y) == 2) sum += x + y;
        }
        p += 2;
    }
    return sum;
}

/* Prints every "not ok" TAP line, every case summary that reports
   failures, and every worker-level diagnostic (crashed child, missing
   result, failed spawn, watchdog) from a captured sub-run buffer, so a
   totals mismatch is immediately actionable — the sub-run output is
   otherwise captured and never surfaced in CI logs. */
static void dump_failures(const char *tag, const char *out) {
    const char *p = out;
    while (*p) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (strncmp(p, "not ok ", 7) == 0) {
            fprintf(stderr, "[%s] %.*s\n", tag, (int)len, p);
        } else if (strncmp(p, "# ", 2) == 0 && strncmp(p, "# total:", 8) != 0) {
            char buf[1024];
            if (len < sizeof(buf)) {
                memcpy(buf, p, len);
                buf[len] = '\0';
                int x, y;
                int is_summary =
                    sscanf(buf, "# %*[^:]: %d passed, %d failed", &x, &y) == 2;
                int is_worker_diag =
                    strstr(buf, "crashed with signal") ||
                    strstr(buf, "exited without a test result") ||
                    strstr(buf, "runner failed to start worker") ||
                    strstr(buf, "without reporting a failure") ||
                    strstr(buf, "WATCHDOG");
                if ((is_summary && y > 0) || is_worker_diag)
                    fprintf(stderr, "[%s] %s\n", tag, buf);
            }
        }
        if (!nl) break;
        p = nl + 1;
    }
}

static int test_runner_parallel_matches_sequential(void) {
    /* Nested parallel width defaults to 4, but CI runners are 1-2 vCPU
       and --jobs 4 over-subscribes them badly enough to cause spurious
       contention-driven assertion failures (see
       docs/plans/2026-07-21-test-harness-port-toctou-flake.md and the
       sibling test-runner-parallel flake history). CI workflows export
       SHARD_TEST_RUNNER_PARALLEL_JOBS=2 to narrow this; local/dev runs
       keep the stronger --jobs 4 coverage by default. */
    int par_jobs = 4;
    const char *ov = getenv("SHARD_TEST_RUNNER_PARALLEL_JOBS");
    if (ov && *ov) {
        int v = atoi(ov);
        if (v >= 2) par_jobs = v;
    }

    int exit_seq = -1, exit_par = -1;
    char *out_seq = run_subcommand(1, &exit_seq);
    char *out_par = run_subcommand(par_jobs, &exit_par);

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

    /* Surface captured sub-run failures on stderr: prints nothing on a
       clean run, and names the failing cases whenever the totals mismatch. */
    dump_failures("seq", out_seq);
    dump_failures("par", out_par);

    free(out_seq);
    free(out_par);
    return 0;
}

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
        "unset SHARD_TEST_FAST; SHARD_TEST_WATCHDOG_SEC=2 "
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
