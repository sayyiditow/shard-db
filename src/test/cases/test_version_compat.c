#include "test_assert.h"
#include "test_runner.h"
#include "types.h"
#include "version.h"

#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_version_compatibility_table(void) {
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 1,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_STAMP,
                  "empty root without marker stamps");
    ASSERT_EQ_INT(shard_db_version_decide(SHARD_DB_VERSION, 1, 1,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_INVALID,
                  "empty root with marker is invalid evidence");
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_INVALID,
                  "non-empty root without marker refuses");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.1", 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_STAMP,
                  "required source version stamps");
    ASSERT_EQ_INT(shard_db_version_decide(SHARD_DB_VERSION, 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_NOOP,
                  "current version is a no-op");
    ASSERT_EQ_INT(shard_db_version_decide("2026.07.3", 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_TOO_OLD,
                  "older version refuses");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.1", 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_DOWNGRADE,
                  "newer version refuses downgrade");
    ASSERT_EQ_INT(shard_db_version_decide("malformed", 1, 0,
                                          SHARD_DB_VERSION,
                                          SHARD_DB_REQUIRED_SOURCE_VERSION),
                  SHARD_DB_VERSION_INVALID,
                  "malformed marker refuses");
    return 0;
}

static int test_version_file_roundtrip_and_check(void) {
    char root[] = "/tmp/shard-db-version-compat-XXXXXX";
    ASSERT_TRUE(mkdtemp(root) != NULL, "created compatibility fixture");
    if (!shard_db_version_is_valid(SHARD_DB_VERSION)) return 1;

    ASSERT_EQ_INT(shard_db_version_check(root, NULL, 0),
                  SHARD_DB_VERSION_STAMP,
                  "filesystem-empty root is eligible for initialization");
    ASSERT_EQ_INT(shard_db_version_file_write(root, "2026.08.1"), 0,
                  "wrote source marker");
    char version[64] = {0};
    ASSERT_EQ_INT(shard_db_version_file_read(root, version, sizeof(version)),
                  SHARD_DB_VERSION_FILE_OK,
                  "read source marker");
    ASSERT_EQ_STR(version, "2026.08.1", "source marker round trips");
    ASSERT_EQ_INT(shard_db_version_check(root, version, sizeof(version)),
                  SHARD_DB_VERSION_STAMP,
                  "source marker is eligible for advancement");
    ASSERT_EQ_INT(shard_db_version_stamp(root), SHARD_DB_VERSION_STAMP_OK,
                  "compatibility stamp succeeds");
    ASSERT_EQ_INT(shard_db_version_check(root, version, sizeof(version)),
                  SHARD_DB_VERSION_NOOP,
                  "current marker is a no-op");

    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker), "%s/.version", root);
    ASSERT_EQ_INT(unlink(marker), 0, "removed marker for missing-evidence case");
    char extra[PATH_MAX];
    snprintf(extra, sizeof(extra), "%s/extra", root);
    int fd = open(extra, O_CREAT | O_WRONLY, 0600);
    ASSERT_TRUE(fd >= 0, "created non-marker root entry");
    if (fd >= 0) close(fd);
    ASSERT_EQ_INT(shard_db_version_check(root, NULL, 0),
                  SHARD_DB_VERSION_INVALID,
                  "non-empty root without marker refuses");
    unlink(extra);
    rmdir(root);
    return 0;
}

TEST_REGISTER("test-version-compat", test_version_compatibility_table)
TEST_REGISTER("test-version-compat-files", test_version_file_roundtrip_and_check)
