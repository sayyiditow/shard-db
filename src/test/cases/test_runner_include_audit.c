/* Regression: `run-all --include-audit` must actually run the
 * audit-campaign cases (TEST_REGISTER_AUDIT), and the flag must parse in
 * any position — including as the LAST argument. The original parser
 * advanced with `i + 1 < argc`, which required every flag to be followed
 * by another argument, so a trailing `--include-audit` was silently
 * dropped and the audit cases never ran.
 *
 * Check: re-invoke the runner binary itself (resolved from
 * /proc/self/exe) with a --filter selecting exactly one cheap audit case
 * (audit-planner-diff at minimum budget), with and without the flag:
 *   without → the filter matches nothing that runs: "1..0"
 *   with    → the audit case is selected and runs: "1..1"
 * The probe asserts SELECTION only — the inner audit case's own pass /
 * fail is that case's gate, not this CLI probe's, so the default suite
 * stays decoupled from audit-case health (a failure is surfaced as a
 * diagnostic). Non-Linux (no /proc/self/exe): pass with a diagnostic —
 * the runner CLI itself is platform-neutral; only this probe mechanism
 * is Linux-only.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define AUD_CASE "audit-planner-diff"

static int invoke_runner(char *out, size_t outcap, const char *flag) {
    char exe[1024];
    ssize_t r = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (r <= 0) return -1;
    exe[r] = '\0';
    char cmd[1400];
    snprintf(cmd, sizeof(cmd),
             "SHARD_AUDIT_DIFF_ROWS=10 SHARD_AUDIT_DIFF_CRITERIA=4 "
             "%s run-all --jobs 1 --filter " AUD_CASE " %s 2>/dev/null",
             exe, flag);
    FILE *p = popen(cmd, "r");
    if (!p) return -1;
    size_t got = 0;
    size_t n;
    while ((n = fread(out + got, 1, outcap - got - 1, p)) > 0) got += n;
    out[got] = '\0';
    int rc = pclose(p);
    return rc;
}

static int test_runner_include_audit_run(void) {
    static char out[8192];

    /* Skip at entry on platforms without /proc/self/exe (macOS): the
     * runner CLI itself is platform-neutral; only this probe mechanism
     * is Linux-specific. */
#ifndef __linux__
    TAP_DIAG("# non-Linux platform: no /proc/self/exe; skipping the "
             "run-all --include-audit CLI probe\n");
    return 0;
#else
    {
        char exe[1024];
        ssize_t r = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
        if (r <= 0) {
            TAP_DIAG("# /proc/self/exe unavailable; skipping the "
                     "run-all --include-audit CLI probe\n");
            return 0;
        }
    }
#endif

    /* Without the flag: the audit case is skipped — an empty plan. */
    int rc = invoke_runner(out, sizeof(out), "");
    ASSERT_TRUE(rc == 0, "filtered run-all without the flag exits 0");
    ASSERT_TRUE(strstr(out, "1..0") != NULL,
                "without --include-audit the audit case is skipped (1..0)");

    /* With the flag as the LAST argument (the exact position the old
     * parser dropped): the audit case is selected and RUNS. Selection is
     * the assertion — the inner case's pass/fail is its own gate and is
     * only surfaced as a diagnostic, so the default suite does not
     * depend on audit-case health. */
    rc = invoke_runner(out, sizeof(out), "--include-audit");
    if (rc < 0 && out[0] == '\0') {
        /* popen/read unavailable: skip with a diag. */
        TAP_DIAG("# /proc/self/exe unavailable; skipping the CLI probe\n");
        return 0;
    }
    ASSERT_TRUE(strstr(out, "1..1") != NULL,
                "with --include-audit the audit case is selected and runs "
                "(1..1)");
    int inner_passed = (rc == 0);
    TAP_DIAG("# inner audit-planner-diff %s (its own gate; the CLI probe "
             "asserts selection only)\n",
             inner_passed ? "passed" : "FAILED");
    return 0;
}

TEST_REGISTER("test-runner-include-audit", test_runner_include_audit_run)
