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
    struct TestCaseEntry *next;
} TestCaseEntry;

void test_register(TestCaseEntry *entry);

#define TEST_REGISTER(name_, fn_)                                    \
    static int fn_(void);                                            \
    static TestCaseEntry _tce_##fn_ = { name_, fn_, NULL };          \
    __attribute__((constructor)) static void _tcr_##fn_(void) {      \
        test_register(&_tce_##fn_);                                  \
    }

/* Walk the registry (for `list` / `run-all` subcommands). Returns count. */
int test_count(void);
const TestCaseEntry *test_first(void);

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

#endif
