#include "types.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

#ifdef TEST_BUILD
static pthread_mutex_t g_durability_msync_test_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_durability_msync_fail_remaining;
static int g_durability_msync_fail_on_call;
static int g_durability_msync_call_count;
static int g_durability_msync_fail_errno;
static int g_durability_msync_succeeded;
static int g_durability_msync_failed;

void durability_test_msync_reset(void) {
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_fail_remaining = 0;
    g_durability_msync_fail_on_call = 0;
    g_durability_msync_call_count = 0;
    g_durability_msync_fail_errno = 0;
    g_durability_msync_succeeded = 0;
    g_durability_msync_failed = 0;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
}

void durability_test_msync_fail_next(int count, int err) {
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_fail_remaining = count > 0 ? count : 0;
    g_durability_msync_fail_on_call = 0;
    g_durability_msync_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
}

void durability_test_msync_fail_on_call(int call_number, int err) {
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_fail_remaining = 0;
    g_durability_msync_fail_on_call = call_number > 0 ? call_number : 0;
    g_durability_msync_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
}

void durability_test_msync_counts(int *succeeded, int *failed) {
    pthread_mutex_lock(&g_durability_msync_test_lock);
    if (succeeded) *succeeded = g_durability_msync_succeeded;
    if (failed) *failed = g_durability_msync_failed;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
}
#endif

int durability_msync(void *addr, size_t len) {
#ifdef TEST_BUILD
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_call_count++;
    if (g_durability_msync_fail_remaining > 0 ||
        (g_durability_msync_fail_on_call > 0 &&
         g_durability_msync_call_count == g_durability_msync_fail_on_call)) {
        if (g_durability_msync_fail_remaining > 0)
            g_durability_msync_fail_remaining--;
        g_durability_msync_fail_on_call = 0;
        g_durability_msync_failed++;
        int injected_errno = g_durability_msync_fail_errno;
        pthread_mutex_unlock(&g_durability_msync_test_lock);
        errno = injected_errno;
        return -1;
    }
    pthread_mutex_unlock(&g_durability_msync_test_lock);
#endif

    int rc = msync(addr, len, MS_SYNC);

#ifdef TEST_BUILD
    int saved_errno = errno;
    pthread_mutex_lock(&g_durability_msync_test_lock);
    if (rc == 0) g_durability_msync_succeeded++;
    else g_durability_msync_failed++;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
    errno = saved_errno;
#endif

    return rc;
}

int durability_msync_range(void *base, size_t offset, size_t len) {
    static long page_size = 0;
    if (!base || len == 0) { errno = EINVAL; return -1; }
    long ps = __atomic_load_n(&page_size, __ATOMIC_RELAXED);
    if (ps == 0) {
        ps = sysconf(_SC_PAGESIZE);
        if (ps <= 0) return -1;
        __atomic_store_n(&page_size, ps, __ATOMIC_RELAXED);
    }
    uintptr_t addr = (uintptr_t)base + offset;
    uintptr_t aligned = addr & ~((uintptr_t)page_size - 1);
    size_t front_pad = (size_t)(addr - aligned);
    size_t sync_len = len + front_pad;
    sync_len = (sync_len + (size_t)page_size - 1) & ~((size_t)page_size - 1);
    return durability_msync((void *)aligned, sync_len);
}

#ifdef TEST_BUILD
static pthread_mutex_t g_durability_fsync_test_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_durability_fsync_call_count;
static int g_durability_fsync_fail_on_call;
static int g_durability_fsync_fail_errno;

void durability_test_fsync_reset(void) {
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_call_count = 0;
    g_durability_fsync_fail_on_call = 0;
    g_durability_fsync_fail_errno = 0;
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
}

void durability_test_fsync_fail_on_call(int call_number, int err) {
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_fail_on_call = call_number > 0 ? call_number : 0;
    g_durability_fsync_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
}
#endif

int durability_fsync(int fd) {
#ifdef TEST_BUILD
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_call_count++;
    if (g_durability_fsync_fail_on_call > 0 &&
        g_durability_fsync_call_count == g_durability_fsync_fail_on_call) {
        int injected_errno = g_durability_fsync_fail_errno;
        g_durability_fsync_fail_on_call = 0;
        pthread_mutex_unlock(&g_durability_fsync_test_lock);
        errno = injected_errno;
        return -1;
    }
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
#endif
    return fsync(fd);
}

/* fsync a regular file by path (open O_RDONLY, durability_fsync, close).
   Preserves the first errno across the close(). */
int fsync_file_path(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    int rc = durability_fsync(fd);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return rc;
}

/* fsync the parent directory of `path` (durability requirement for a rename
   to be crash-safe). Preserves the first errno across the close(). */
int fsync_parent_dir(const char *path) {
    char parent[PATH_MAX];
    if (parent_dir_copy(path, parent, sizeof(parent)) != 0) return -1;
    int fd = open(parent, O_DIRECTORY | O_RDONLY);
    if (fd < 0) return -1;
    int rc = durability_fsync(fd);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return rc;
}

int durability_publish_replace(const char *target, const char *tmp_path,
                               durability_after_rename_fn after_rename,
                               void *after_rename_ctx) {
    if (fsync_file_path(tmp_path) != 0) return -1;
    if (rename(tmp_path, target) != 0) return -1;
    if (after_rename) after_rename(target, after_rename_ctx);
    if (fsync_parent_dir(target) != 0) return 1;
    return 0;
}

int durability_same_open_inode(int fd, const char *path) {
    struct stat opened;
    struct stat current;
    if (fstat(fd, &opened) != 0) return 0;
    if (stat(path, &current) != 0) return 0;
    return opened.st_dev == current.st_dev && opened.st_ino == current.st_ino;
}

void durability_test_pause(const char *data_dir, const char *phase) {
    if (!data_dir || !g_db || g_durability_test_pause_ms <= 0 ||
        strcmp(g_durability_test_pause_phase, phase) != 0) return;
    char marker[PATH_MAX];
    int n = snprintf(marker, sizeof(marker), "%s/.durability-test-%s.active",
                     data_dir, phase);
    if (n < 0 || n >= (int)sizeof(marker)) return;
    int fd = open(marker, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) close(fd);
    int remaining = g_durability_test_pause_ms;
    while (remaining > 0) {
        int slice = remaining > 100 ? 100 : remaining;
        struct timespec ts = { slice / 1000, (long)(slice % 1000) * 1000000L };
        while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
        remaining -= slice;
    }
    unlink(marker);
}

static void durability_restore_earliest(_Atomic uint64_t *dirty_since_ms,
                                        uint64_t claimed_since_ms);

int durability_flush_dirty(_Atomic int *dirty,
                           _Atomic uint64_t *dirty_since_ms,
                           void *addr, size_t len) {
    uint64_t claimed_since = atomic_load_explicit(dirty_since_ms,
                                                  memory_order_acquire);
    if (!atomic_exchange_explicit(dirty, 0, memory_order_acq_rel)) return 0;

    if (durability_msync(addr, len) != 0) {
        int saved_errno = errno;
        durability_restore_earliest(dirty_since_ms, claimed_since);
        atomic_store_explicit(dirty, 1, memory_order_release);
        errno = saved_errno;
        return -1;
    }

    if (!atomic_load_explicit(dirty, memory_order_acquire)) {
        uint64_t expected = claimed_since;
        atomic_compare_exchange_strong_explicit(dirty_since_ms, &expected, 0,
                                                memory_order_acq_rel,
                                                memory_order_acquire);
    }
    return 1;
}

static void durability_restore_earliest(_Atomic uint64_t *dirty_since_ms,
                                        uint64_t claimed_since_ms) {
    uint64_t current = atomic_load_explicit(dirty_since_ms,
                                            memory_order_acquire);
    while (claimed_since_ms < current &&
           !atomic_compare_exchange_weak_explicit(dirty_since_ms, &current,
                                                  claimed_since_ms,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
    }
}
