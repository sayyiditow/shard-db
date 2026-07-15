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
#include <signal.h>
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

    /* 16 fillers (not 15): the eviction check in bt_acquire runs at the
       START of each insert, when bt_cache_count is still the prior value.
       With 1 victim (pathA) + 16 fillers = 17 inserts, the 17th insert is
       the first whose pre-insert bt_cache_count reaches the threshold of
       16, so it is the first to trigger the LRU scan that would wrongly
       evict pathA. 15 fillers (16 inserts) never crosses the threshold at
       check time and would NOT reproduce the bug. pathA stays the
       globally-oldest (its cache-hit bump at btree.c:624 only promotes it
       to value 2, still less than every filler's 3..18), so it is the LRU
       victim both pre- and post-fix. */
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

/* Wait for the child, but bound it: a pre-fix child deadlocks (the evicted
   slot's wrlock is taken while the iterator still holds its rdlock), which
   would otherwise hang waitpid — and the whole test runner — forever. On
   timeout we SIGKILL the child so WIFSIGNALED reports true and the
   assertion below fails cleanly. */
static int wait_child_bounded(pid_t pid, int *status, int secs) {
    for (int s = 0; s < secs; s++) {
        pid_t r = waitpid(pid, status, WNOHANG);
        if (r == pid) return 1;
        if (r < 0) return -1;
        usleep(100000); /* 100ms */
    }
    kill(pid, SIGKILL);
    waitpid(pid, status, 0);
    return 0;
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
    int got = wait_child_bounded(pid, &status, 30);

    ASSERT_TRUE(got != 0 || WIFSIGNALED(status),
        "child did not hang (pre-fix deadlock on evicted slot's wrlock)");
    ASSERT_TRUE(!WIFSIGNALED(status),
        "held cache slot survives concurrent LRU eviction (no crash/deadlock)");
    if (WIFSIGNALED(status))
        TAP_DIAG("# child killed by signal %d\n", WTERMSIG(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "iterator reads correct data after surviving eviction pressure");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btcache-evict-race", test_btcache_evict_race_run)
