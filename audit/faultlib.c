/* audit/faultlib.c
 *
 * Audit-campaign fault-injection interposer (plan Phase 1 step 4, third
 * harness). LD_PRELOAD into the TEST_BUILD daemon (shard-db-test-server);
 * fails I/O syscalls on demand so fault tests can exercise ENOSPC / EIO /
 * EMFILE / short-write behavior with zero engine edits. Built by
 * build.sh as build/bin/audit-faultlib.so; never linked into a shipped
 * binary and never preloaded outside audit runs.
 *
 * Arming environment (read once at library load; every switch treats 0
 * / absent as disabled):
 *   SHARD_FAULT_OP        write|pwrite|fsync|fdatasync|open|mmap
 *   SHARD_FAULT_N         1-based call index to fail (0 = disarmed)
 *   SHARD_FAULT_STICKY    nonzero = fail that call and every later call
 *                         (persistent fault, e.g. disk-full)
 *   SHARD_FAULT_ERRNO     errno to report (default 28 = ENOSPC)
 *   SHARD_FAULT_SHORT     nonzero = on the armed write/pwrite, perform
 *                         the real call with one fewer byte and report
 *                         that count (short write; the engine must
 *                         detect and fail the record — never silently
 *                         accept)
 *   SHARD_FAULT_AFTER_MS  nonzero = start counting calls only after this
 *                         many ms of process uptime, so the fault lands
 *                         in the harness's workload instead of the
 *                         daemon's startup I/O (SHARD_FAULT_N counts
 *                         post-deadline calls; 0 = count from load)
 *
 * Deliberate v1 scope (recorded in the campaign index):
 *   - malloc failure is NOT interposed here. dlsym itself allocates, so
 *     a naive malloc hook recurses; allocation-failure coverage ships
 *     via the engine's existing OOM seams (test_alloc_error_propagation,
 *     test_bulk_update_json_oom, test_query_plan_in_oom). A safe malloc
 *     bootstrap is a v2 improvement.
 *   - No fd→path tracking: write/pwrite faults fire for the Nth call on
 *     ANY fd (per-path targeting is a v2 improvement).
 *   - Sanitizer builds: the ASan/TSan runtimes live in the executable
 *     and their syscall interceptors win symbol interposition over a
 *     preloaded library, so fault injection runs against normal builds.
 *
 * Async-signal discipline: the hooks only touch an _Atomic counter and
 * pre-loaded function pointers; no allocation, no locks.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static _Atomic unsigned long g_fault_count; /* calls seen for armed op */

static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static int (*real_open)(const char *, int, ...);
static void *(*real_mmap)(void *, size_t, int, int, int, off_t);

static int fault_op_code(const char *s) {
    if (!s || !*s) return 0;
    if (strcmp(s, "write") == 0) return 1;
    if (strcmp(s, "pwrite") == 0) return 2;
    if (strcmp(s, "fsync") == 0) return 3;
    if (strcmp(s, "fdatasync") == 0) return 4;
    if (strcmp(s, "open") == 0) return 5;
    if (strcmp(s, "mmap") == 0) return 6;
    return 0;
}

static int g_op;             /* armed op code (0 = disarmed) */
static unsigned long g_n;    /* 1-based armed index (0 = disarmed) */
static int g_sticky;
static int g_errno_val;
static int g_short;
static long g_after_ms;      /* workload-delay (ms of uptime; 0 = off) */
static struct timespec g_start;

/* The armed hit is index g_n, or (sticky) any index >= g_n — counted
   after the AFTER_MS deadline when one is armed. */
static int fault_hits_now(void) {
    if (g_after_ms > 0) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        long el = (now.tv_sec - g_start.tv_sec) * 1000L +
                  (now.tv_nsec - g_start.tv_nsec) / 1000000L;
        if (el < g_after_ms) return 0;
    }
    unsigned long c = ++g_fault_count;
    if (g_n == 0) return 0;
    if (c == g_n) return 1;
    if (g_sticky && c > g_n) return 1;
    return 0;
}

static void faultlib_init(void) __attribute__((constructor));
static void faultlib_init(void) {
    g_op = fault_op_code(getenv("SHARD_FAULT_OP"));
    const char *n = getenv("SHARD_FAULT_N");
    g_n = (n && *n) ? strtoul(n, NULL, 10) : 0;
    const char *st = getenv("SHARD_FAULT_STICKY");
    g_sticky = (st && *st) ? (atoi(st) != 0) : 0;
    const char *e = getenv("SHARD_FAULT_ERRNO");
    g_errno_val = (e && *e) ? atoi(e) : ENOSPC;
    if (g_errno_val <= 0) {
        /* 0 (or negative) is not a reportable errno: a -1 return with
           errno==0 reads as success-with-failure-semantics to callers.
           Fall back to the default fault errno. */
        g_errno_val = ENOSPC;
    }
    const char *sh = getenv("SHARD_FAULT_SHORT");
    g_short = (sh && *sh) ? (atoi(sh) != 0) : 0;
    const char *am = getenv("SHARD_FAULT_AFTER_MS");
    g_after_ms = (am && *am) ? atol(am) : 0;
    clock_gettime(CLOCK_MONOTONIC, &g_start);
    g_fault_count = 0;
    real_write = dlsym(RTLD_NEXT, "write");
    real_pwrite = dlsym(RTLD_NEXT, "pwrite");
    real_fsync = dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_open = dlsym(RTLD_NEXT, "open");
    real_mmap = dlsym(RTLD_NEXT, "mmap");
}

ssize_t write(int fd, const void *buf, size_t count) {
    if (g_op == 1 && fault_hits_now()) {
        if (g_short && real_write && count > 0)
            return real_write(fd, buf, count - 1);
        errno = g_errno_val;
        return -1;
    }
    return real_write ? real_write(fd, buf, count) : -1;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    if (g_op == 2 && fault_hits_now()) {
        if (g_short && real_pwrite && count > 0)
            return real_pwrite(fd, buf, count - 1, offset);
        errno = g_errno_val;
        return -1;
    }
    return real_pwrite ? real_pwrite(fd, buf, count, offset) : -1;
}

int fsync(int fd) {
    if (g_op == 3 && fault_hits_now()) {
        errno = g_errno_val;
        return -1;
    }
    return real_fsync ? real_fsync(fd) : -1;
}

int fdatasync(int fd) {
    if (g_op == 4 && fault_hits_now()) {
        errno = g_errno_val;
        return -1;
    }
    return real_fdatasync ? real_fdatasync(fd) : -1;
}

int open(const char *path, int flags, ...) {
    if (!real_open) { errno = EIO; return -1; }
    if (g_op == 5 && fault_hits_now()) {
        errno = (g_errno_val == ENOSPC) ? EMFILE : g_errno_val;
        /* ENOSPC is meaningless for open; translate to EMFILE unless the
           test explicitly chose another errno (EIO etc.). */
        return -1;
    }
#ifdef O_TMPFILE
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
#else
    if (flags & O_CREAT) {
#endif
        mode_t mode;
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
        return real_open(path, flags, mode);
    }
    return real_open(path, flags);
}


void *mmap(void *addr, size_t length, int prot, int flags, int fd,
           off_t offset) {
    if (g_op == 6 && fault_hits_now()) {
        errno = (g_errno_val == ENOSPC) ? ENOMEM : g_errno_val;
        return MAP_FAILED;
    }
    if (!real_mmap) { errno = ENOMEM; return MAP_FAILED; }
    return real_mmap(addr, length, prot, flags, fd, offset);
}
