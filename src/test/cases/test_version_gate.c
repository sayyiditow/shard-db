#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

static int test_version_gate_run(void) {
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.1", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_MIGRATE,
        "previous release migrates");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.2", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_MIGRATE,
        "minimum supported release migrates");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.2", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_NOOP,
        "current release is noop");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.1", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_TOO_OLD,
        "pre-required release is too old");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.1", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_DOWNGRADE,
        "newer on-disk version refuses");
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 1,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_STAMP,
        "empty root stamps");
    ASSERT_EQ_INT(shard_db_version_decide("garbage", 1, 0,
        "2026.09.2", "2026.08.2"), SHARD_DB_VERSION_INVALID,
        "malformed on-disk version is invalid");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-version-gate", test_version_gate_run)
