/* Regression for the same lock inversion through single insert's
 * apply_commit -> btree_insert_locked chain. */
#include "test_runner.h"

extern int ordered_walk_kfcache_deadlock_run_single(void);

static int test_ordered_walk_kfcache_deadlock_single_write_run(void) {
    return ordered_walk_kfcache_deadlock_run_single();
}

TEST_REGISTER("test-ordered-walk-kfcache-deadlock-single-write",
              test_ordered_walk_kfcache_deadlock_single_write_run)
