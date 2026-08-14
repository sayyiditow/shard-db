#include "test_assert.h"
#include "test_runner.h"
#include "types.h"

#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_version_startup_shared_seam(void) {
    char root[] = "/tmp/shard-db-version-startup-XXXXXX";
    ASSERT_TRUE(mkdtemp(root) != NULL, "created startup fixture");
    ASSERT_EQ_INT(shard_db_recover_before_stamp(root, &(int){0}), 0,
                  "shared recovery accepts an empty root");
    ASSERT_EQ_INT(shard_db_validate_before_stamp(root), 0,
                  "shared validation accepts an empty schema set");
    ASSERT_EQ_INT(shard_db_version_stamp(root), SHARD_DB_VERSION_STAMP_OK,
                  "shared startup seam stamps after validation");
    ASSERT_EQ_INT(shard_db_mark_clean_if_safe(root), 0,
                  "shared shutdown seam writes clean evidence");
    return 0;
}

static int test_version_startup_failure_seam_is_explicit(void) {
    char root[] = "/tmp/shard-db-version-startup-failure-XXXXXX";
    ASSERT_TRUE(mkdtemp(root) != NULL, "created failure fixture");
    ASSERT_EQ_INT(shard_db_version_check(root, NULL, 0),
                  SHARD_DB_VERSION_STAMP,
                  "read-only check precedes initialization");
    return 0;
}

TEST_REGISTER("test-version-startup-paths", test_version_startup_shared_seam)
TEST_REGISTER("test-version-startup-failure-path", test_version_startup_failure_seam_is_explicit)
