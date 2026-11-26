/* src/test/test_runner.h
 *
 * Static-init test registry. Each test case calls TEST_REGISTER() at
 * file scope to add itself to a global linked list. The runner walks
 * the list at startup, supports filtering by name (substring match),
 * and dispatches each test in isolation.
 */
#ifndef TEST_RUNNER_H
#define TEST_RUNNER_H
#include "test_assert.h"

typedef int (*TestFn)(void);

typedef struct TestCaseEntry {
    const char *name;
    TestFn fn;
    /* An exclusive case runs alone in the process-pool scheduler. */
    int exclusive;
    /* Audit cases (TEST_REGISTER_AUDIT) are campaign tooling, not part of
       the regression suite: default `run-all` skips them; they run only
       under --include-audit, via `run <name>`, or from the audit scripts. */
    int audit;
    struct TestCaseEntry *next;
} TestCaseEntry;

void test_register(TestCaseEntry *entry);

#define TEST_REGISTER_IMPL(name_, fn_, exclusive_, audit_)           \
    static int fn_(void);                                            \
    static TestCaseEntry _tce_##fn_ = { name_, fn_, exclusive_, audit_, NULL }; \
    __attribute__((constructor)) static void _tcr_##fn_(void) {      \
        test_register(&_tce_##fn_);                                  \
    }

#define TEST_REGISTER(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0, 0)

/* Use only for intentionally high-contention integration cases whose
   deadline is meaningful only when they have the machine to themselves. */
#define TEST_REGISTER_EXCLUSIVE(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 1, 0)

/* Audit-campaign cases (2026-11-19-audit-campaign.md Phase 1): excluded
   from default `run-all` and every CI/sanitizer workflow; run explicitly
   via --include-audit or `run <name>`. */
#define TEST_REGISTER_AUDIT(name_, fn_) TEST_REGISTER_IMPL(name_, fn_, 0, 1)

/* Walk the registry (for `list` / `run-all` subcommands). Returns count. */
int test_count(void);
const TestCaseEntry *test_first(void);

/* Run a single test by name. Returns 0 on pass, non-zero on fail. */
int test_run_one(const char *name);

/* Run all (optionally filtered by substring and excluded by comma-separated
   exact case names), using `jobs` worker
   processes. jobs<=1 runs strictly sequentially (byte-identical output to
   the pre-parallel implementation) — this is the safety fallback.
   jobs>1 uses a bounded process pool: a single-threaded parent forks one
   isolated child per active case, captures each child's output in a private
   file, and flushes it after reaping the child, so concurrent tests' output
   never interleaves and their process-global test state cannot race. The
   parent _exit(124)s after killing active children if any one exceeds
   SHARD_TEST_WATCHDOG_SEC (default 180s; 1200s in TSan builds, where the
   instrumentation slows heavy cases past the release-budget default).
   Audit-campaign cases (TEST_REGISTER_AUDIT) run only when include_audit is
   nonzero (--include-audit); default callers pass 0 so `run-all` behavior
   is unchanged. Returns total fail count. */
int test_run_all(const char *filter, const char *exclude, int jobs,
                 int include_audit);

/* Parse `run-all` options starting at argv[first] (same semantics and stderr
   message as the CLI always had). *filter / *exclude start NULL, *jobs and
   *include_audit start at the caller's defaults and are updated in place.
   Returns 0 on success, 2 for an unknown or incomplete option. */
int test_parse_run_all_args(int argc, char **argv, int first,
                            const char **filter, const char **exclude,
                            int *jobs, int *include_audit);

/* Number of registered cases the same filter/exclude/include_audit selection
   would run (the CLI probe's in-process selection count). */
int test_count_selected(const char *filter, const char *exclude,
                        int include_audit);

#endif
