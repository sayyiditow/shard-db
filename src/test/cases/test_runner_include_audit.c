/* Regression: `run-all --include-audit` must actually run the
 * audit-campaign cases (TEST_REGISTER_AUDIT), and the flag must parse in
 * any position — including as the LAST argument. The original parser
 * advanced with `i + 1 < argc`, which required every flag to be followed
 * by another argument, so a trailing `--include-audit` was silently
 * dropped and the audit cases never ran.
 *
 * The case asserts the parser and the selection IN PROCESS (no nested
 * runner invocation): the argv vectors are built from char arrays so the
 * build stays warning-free.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include <stdio.h>
#include <string.h>

#define AUD_CASE "audit-planner-diff"

static int test_runner_include_audit_run(void) {
    /* A value flag followed by the standalone flag: the filter is captured,
       the audit flag survives. */
    {
        char a0[] = "shard-db-test";
        char a1[] = "run-all";
        char a2[] = "--filter";
        char a3[] = AUD_CASE;
        char a4[] = "--include-audit";
        char *argv[] = { a0, a1, a2, a3, a4, NULL };
        const char *filter = NULL, *exclude = NULL;
        int jobs = 0, include_audit = 0;
        int rc = test_parse_run_all_args(5, argv, 2, &filter, &exclude,
                                         &jobs, &include_audit);
        ASSERT_EQ_INT(rc, 0, "filter + trailing --include-audit parses");
        ASSERT_EQ_INT(include_audit, 1,
                      "the trailing --include-audit is not dropped");
        ASSERT_TRUE(filter && strcmp(filter, AUD_CASE) == 0,
                    "the filter value is captured");
    }

    /* Standalone flag before a value flag. */
    {
        char a0[] = "shard-db-test";
        char a1[] = "run-all";
        char a2[] = "--include-audit";
        char a3[] = "--jobs";
        char a4[] = "3";
        char *argv[] = { a0, a1, a2, a3, a4, NULL };
        const char *filter = NULL, *exclude = NULL;
        int jobs = 0, include_audit = 0;
        int rc = test_parse_run_all_args(5, argv, 2, &filter, &exclude,
                                         &jobs, &include_audit);
        ASSERT_EQ_INT(rc, 0, "--include-audit --jobs 3 parses");
        ASSERT_EQ_INT(include_audit, 1, "the audit flag is set");
        ASSERT_EQ_INT(jobs, 3, "the job count is captured");
    }

    /* A value flag followed by another option consumes no value. */
    {
        char a0[] = "shard-db-test";
        char a1[] = "run-all";
        char a2[] = "--jobs";
        char a3[] = "--include-audit";
        char *argv[] = { a0, a1, a2, a3, NULL };
        const char *filter = NULL, *exclude = NULL;
        int jobs = 0, include_audit = 0;
        int rc = test_parse_run_all_args(4, argv, 2, &filter, &exclude,
                                         &jobs, &include_audit);
        ASSERT_EQ_INT(rc, 0, "--jobs --include-audit parses");
        ASSERT_EQ_INT(jobs, 0, "the next option is not taken as a value");
        ASSERT_EQ_INT(include_audit, 1, "the audit flag is kept");
    }

    /* A typo must be refused, not silently ignored. */
    {
        char a0[] = "shard-db-test";
        char a1[] = "run-all";
        char a2[] = "--include_audit";
        char *argv[] = { a0, a1, a2, NULL };
        const char *filter = NULL, *exclude = NULL;
        int jobs = 0, include_audit = 0;
        int rc = test_parse_run_all_args(3, argv, 2, &filter, &exclude,
                                         &jobs, &include_audit);
        ASSERT_EQ_INT(rc, 2, "an unknown option is refused");
    }

    /* Selection: the audit case is invisible by default and selected with
       the flag. */
    ASSERT_EQ_INT(test_count_selected(AUD_CASE, NULL, 0), 0,
                  "without --include-audit the audit case is not selected");
    ASSERT_EQ_INT(test_count_selected(AUD_CASE, NULL, 1), 1,
                  "with --include-audit the audit case is selected");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-runner-include-audit", test_runner_include_audit_run)
