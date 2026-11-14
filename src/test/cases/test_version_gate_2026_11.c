#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "version.h"
#include <string.h>

static int test_version_gate_2026_11_run(void) {
    ASSERT_TRUE(strcmp(SHARD_DB_VERSION, "2026.11.1") == 0, "release is 2026.11.1");
    ASSERT_TRUE(strcmp(SHARD_DB_REQUIRED_SOURCE_VERSION, "2026.10.2") == 0,
                "minimum source is the previous release");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_STAMP,
        "previous release opens and re-stamps");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_TOO_OLD,
        "pre-previous release refuses with the upgrade hint");
    ASSERT_EQ_INT(shard_db_version_decide("2026.11.1", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_NOOP,
        "current release is noop");
    ASSERT_EQ_INT(shard_db_version_decide("2026.11.2", 1, 0,
        SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION), SHARD_DB_VERSION_DOWNGRADE,
        "newer on-disk version refuses (downgrade barrier)");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-version-gate-2026-11", test_version_gate_2026_11_run)
