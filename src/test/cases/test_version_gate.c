#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

/* 2026.10.1 removed the startup migration: roots stamped by the floor
   release (== REQUIRED_SOURCE_VERSION) open as-is and re-stamp, anything
   older refuses with the upgrade-ladder hint. */
static int test_version_gate_run(void) {
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.2", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_STAMP,
        "floor release opens as-is and re-stamps");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.1", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_NOOP,
        "current release is noop");
    ASSERT_EQ_INT(shard_db_version_decide("2026.09.1", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_TOO_OLD,
        "pre-floor release refuses (upgrade ladder)");
    ASSERT_EQ_INT(shard_db_version_decide("2026.08.2", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_TOO_OLD,
        "retired floor refuses (upgrade ladder)");
    ASSERT_EQ_INT(shard_db_version_decide("2026.10.2", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_DOWNGRADE,
        "newer on-disk version refuses");
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 1,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_STAMP,
        "empty root stamps");
    ASSERT_EQ_INT(shard_db_version_decide("garbage", 1, 0,
        "2026.10.1", "2026.09.2"), SHARD_DB_VERSION_INVALID,
        "malformed on-disk version is invalid");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-version-gate", test_version_gate_run)
