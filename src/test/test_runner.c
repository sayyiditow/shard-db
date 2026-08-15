/* src/test/test_runner.c */
#include "test_runner.h"
#include "fixtures.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef TEST_BUILD
extern void test_reset_caches(void);
extern void test_init_process_db(void);  /* ensures g_db is set for in-process tests */
#endif

__thread TestCtx *t_ctx = NULL;

static TestCaseEntry *g_head = NULL;

void test_register(TestCaseEntry *entry) {
    /* Insert at end so registration order matches source order. */
    if (!g_head) { g_head = entry; return; }
    TestCaseEntry *p = g_head;
    while (p->next) p = p->next;
    p->next = entry;
}

int test_count(void) {
    int n = 0;
    for (TestCaseEntry *p = g_head; p; p = p->next) n++;
    return n;
}

const TestCaseEntry *test_first(void) { return g_head; }

typedef struct {
    int passed;
    int failed;
} TestResult;

/* Runs one test in this process and exposes its assertion totals to the
   process-pool parent. The usual `run` and sequential paths use the same
   body, so their TAP output remains unchanged. */
static int run_case_result(const TestCaseEntry *tc, TestResult *result) {
    TestCtx ctx = { .name = tc->name };
    t_ctx = &ctx;
    printf("# %s\n", tc->name);
    fflush(stdout);  /* preserve the case name if it crashes mid-test */
#ifdef TEST_BUILD
    test_init_process_db();
#endif
    int rc = tc->fn();
    if (rc != 0 && ctx.failed == 0) ctx.failed = 1; /* fn signalled fail without assert */
    printf("# %s: %d passed, %d failed\n", tc->name, ctx.passed, ctx.failed);
    t_ctx = NULL;
    if (result) *result = (TestResult){ .passed = ctx.passed, .failed = ctx.failed };
    return ctx.failed;
}

static int run_case(const TestCaseEntry *tc) {
    return run_case_result(tc, NULL);
}

int test_run_one(const char *name) {
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (strcmp(p->name, name) == 0) return run_case(p);
    }
    fprintf(stderr, "no test named '%s'\n", name);
    return 1;
}

/* `exclude` is a comma-separated list of complete case names. Exact matching
   keeps a CI fast-tier exclusion from accidentally dropping a similarly
   named regression case. */
static int is_excluded(const char *name, const char *exclude) {
    if (!exclude || !*exclude) return 0;
    const char *item = exclude;
    while (*item) {
        const char *end = strchr(item, ',');
        size_t len = end ? (size_t)(end - item) : strlen(item);
        if (strlen(name) == len && memcmp(name, item, len) == 0) return 1;
        if (!end) break;
        item = end + 1;
    }
    return 0;
}

/* Builds a flat array of registered cases matching `filter` (NULL = all)
   except exact names in `exclude`. Returns the count; *out receives a
   malloc'd array the caller must free. Both scheduler paths share this. */
static int collect_matching(const char *filter, const char *exclude,
                            TestCaseEntry ***out) {
    int cap = 16, n = 0;
    TestCaseEntry **arr = malloc((size_t)cap * sizeof(*arr));
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (filter && !strstr(p->name, filter)) continue;
        if (is_excluded(p->name, exclude)) continue;
        if (n == cap) {
            cap *= 2;
            void *np = realloc(arr, (size_t)cap * sizeof(*arr));
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
        TestResult result;
        run_case_result(cases[i], &result);
        total_fail += result.failed;
        total_passed += result.passed;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
    }
    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_fail, n);
    return total_fail;
}

/* ---- Parallel path (jobs > 1) ----

   Test cases are not thread-safe: many invoke fork(), popen(), or system(),
   and several use the test-only in-process database globals. Forking from a
   multithreaded runner leaves the child with only the calling thread and can
   inherit libc or application locks held by siblings. On macOS this showed
   up as SIGBUS in CI. Keep the runner parent single-threaded and execute
   cases in independent child processes instead. */

typedef struct {
    pid_t pid;
    const TestCaseEntry *tc;
    int result_fd;
    long start_ms;
    char output_path[PATH_MAX];
} ProcessSlot;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static int write_full(int fd, const void *buf, size_t len) {
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n > 0) { p += n; len -= (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int read_full(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n > 0) { p += n; len -= (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

/* Child output goes to a file rather than a pipe: some test cases produce
   more than a macOS pipe buffer, so a parent that only reads after waitpid()
   could otherwise deadlock the child before its result is available. */
static void flush_case_output(ProcessSlot *slot) {
    FILE *f = fopen(slot->output_path, "r");
    if (f) {
        char buf[8192];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
            fwrite(buf, 1, n, stdout);
        fclose(f);
    }
    unlink(slot->output_path);
    slot->output_path[0] = '\0';
}

/* Starts a single test from the single-threaded parent. Returns 0 once a
   child occupies `slot`; returns -1 only for a runner infrastructure error. */
static int start_case(ProcessSlot *slot, const TestCaseEntry *tc) {
    char path[] = "/tmp/shard-db-test-output-XXXXXX";
    int output_fd = mkstemp(path);
    if (output_fd < 0) return -1;

    int result_pipe[2];
    if (pipe(result_pipe) != 0) {
        close(output_fd);
        unlink(path);
        return -1;
    }
    /* Daemons exec'd by a test must not keep this control pipe open. */
    fcntl(result_pipe[1], F_SETFD, FD_CLOEXEC);

    /* The parent flushes completed cases after reaping them. A subsequent
       fork would otherwise copy that still-buffered TAP text into the child,
       whose fflush() would duplicate prior cases in its private output file. */
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        close(output_fd);
        close(result_pipe[0]);
        close(result_pipe[1]);
        unlink(path);
        return -1;
    }
    if (pid == 0) {
        close(result_pipe[0]);
        /* A watchdog kill must include the daemon and helper processes this
           case starts; otherwise they outlive a timed-out test as orphans. */
        setpgid(0, 0);
        if (dup2(output_fd, STDOUT_FILENO) < 0 ||
            dup2(output_fd, STDERR_FILENO) < 0) {
            _exit(127);
        }
        if (output_fd > STDERR_FILENO) close(output_fd);

        TestResult result;
        int failed = run_case_result(tc, &result);
        fflush(NULL);
        write_full(result_pipe[1], &result, sizeof(result));
        close(result_pipe[1]);
        _exit(failed ? 1 : 0);
    }

    close(output_fd);
    close(result_pipe[1]);
    /* Close the small parent/child race around setpgid() above. */
    setpgid(pid, pid);
    slot->pid = pid;
    slot->tc = tc;
    slot->result_fd = result_pipe[0];
    slot->start_ms = now_ms();
    snprintf(slot->output_path, sizeof(slot->output_path), "%s", path);
    return 0;
}

static void clear_slot(ProcessSlot *slot) {
    if (slot->result_fd >= 0) close(slot->result_fd);
    if (slot->output_path[0]) unlink(slot->output_path);
    *slot = (ProcessSlot){ .result_fd = -1 };
}

static void collect_case(ProcessSlot *slot, int status, int *total_passed,
                         int *total_failed) {
    TestResult result;
    int got_result = read_full(slot->result_fd, &result, sizeof(result)) == 0;
    close(slot->result_fd);
    slot->result_fd = -1;

    flush_case_output(slot);
    if (WIFSIGNALED(status)) {
        fprintf(stderr, "# %s: crashed with signal %d\n", slot->tc->name,
                WTERMSIG(status));
        (*total_failed)++;
    } else if (!WIFEXITED(status) || !got_result) {
        fprintf(stderr, "# %s: exited without a test result\n", slot->tc->name);
        (*total_failed)++;
    } else {
        *total_passed += result.passed;
        *total_failed += result.failed;
        if (WEXITSTATUS(status) != 0 && result.failed == 0) {
            fprintf(stderr, "# %s: exited %d without reporting a failure\n",
                    slot->tc->name, WEXITSTATUS(status));
            (*total_failed)++;
        }
    }
    clear_slot(slot);
}

static void watchdog_abort(ProcessSlot *slots, int nslots, const ProcessSlot *stuck,
                           long limit_ms) {
    fflush(stdout);
    fprintf(stderr,
            "\n# WATCHDOG: test '%s' exceeded %lds — aborting run-all\n",
            stuck->tc->name, limit_ms / 1000L);
    for (int i = 0; i < nslots; i++) {
        if (slots[i].pid > 0) {
            /* Workers lead their own process groups, so this reaps a
               fixture daemon as well as the immediate test child. */
            if (kill(-slots[i].pid, SIGKILL) < 0) kill(slots[i].pid, SIGKILL);
        }
    }
    for (int i = 0; i < nslots; i++) {
        if (slots[i].pid > 0) waitpid(slots[i].pid, NULL, 0);
        clear_slot(&slots[i]);
    }
    fflush(stderr);
    _exit(124);
}

static int run_all_parallel(TestCaseEntry **cases, int n, int jobs) {
    if (jobs > n) jobs = n > 0 ? n : 1;

    printf("# run-all: %d worker process(es)\n", jobs);
    fflush(stdout);

    ProcessSlot *slots = calloc((size_t)jobs, sizeof(*slots));
    if (!slots) return 1;
    for (int i = 0; i < jobs; i++) slots[i].result_fd = -1;

    long limit_ms = 180000;
    const char *env = getenv("SHARD_TEST_WATCHDOG_SEC");
    if (env && atoi(env) > 0) limit_ms = (long)atoi(env) * 1000L;

    int next = 0, active = 0, total_passed = 0, total_failed = 0;
    while (next < n || active > 0) {
        int exclusive_active = 0;
        for (int i = 0; i < jobs; i++) {
            if (slots[i].pid > 0 && slots[i].tc->exclusive) {
                exclusive_active = 1;
                break;
            }
        }

        /* Preserve registry order around an exclusive case: drain ordinary
           workers before it starts, then leave every other slot idle until
           it completes. This is a scheduling property, not a name-based
           exception, so future high-contention cases can opt in explicitly. */
        if (!exclusive_active && next < n &&
            (!cases[next]->exclusive || active == 0)) {
            for (int i = 0; i < jobs && next < n; i++) {
                if (slots[i].pid != 0) continue;
                if (cases[next]->exclusive && active > 0) break;
                if (start_case(&slots[i], cases[next]) == 0) {
                    active++;
                } else {
                    fprintf(stderr, "# %s: runner failed to start worker: %s\n",
                            cases[next]->name, strerror(errno));
                    total_failed++;
                }
                int started_exclusive = cases[next]->exclusive;
                next++;
                if (started_exclusive) break;
            }
        }

        int progressed = 0;
        long now = now_ms();
        for (int i = 0; i < jobs; i++) {
            if (slots[i].pid == 0) continue;
            if (now - slots[i].start_ms > limit_ms)
                watchdog_abort(slots, jobs, &slots[i], limit_ms);

            int status;
            pid_t r = waitpid(slots[i].pid, &status, WNOHANG);
            if (r == slots[i].pid) {
                collect_case(&slots[i], status, &total_passed, &total_failed);
                active--;
                progressed = 1;
            } else if (r < 0) {
                fprintf(stderr, "# %s: waitpid failed: %s\n",
                        slots[i].tc->name, strerror(errno));
                clear_slot(&slots[i]);
                total_failed++;
                active--;
                progressed = 1;
            }
        }
        if (active > 0 && !progressed) {
            struct timespec req = { 0, 10000000L };
            nanosleep(&req, NULL);
        }
    }

    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_failed, n);
    free(slots);
    return total_failed;
}

int test_run_all(const char *filter, const char *exclude, int jobs) {
    test_fixture_set_jobs(jobs);
    TestCaseEntry **cases = NULL;
    int n = collect_matching(filter, exclude, &cases);

    int result = jobs <= 1 ? run_all_sequential(cases, n)
                           : run_all_parallel(cases, n, jobs);
    free(cases);
    return result;
}
