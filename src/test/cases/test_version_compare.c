#include "test_assert.h"
#include "test_runner.h"
#include "types.h"
#include "fixtures.h"
#include <unistd.h>
#include <sys/stat.h>

static int test_version_compare_orders_calver_numerically(void) {
    /* Numeric, not lexical: "2026.09.5" < "2026.10.1" though "9" months
       into a 2-digit month means the month field is zero-padded. */
    ASSERT_TRUE(shard_db_version_compare("2026.09.5", "2026.10.1") < 0,
                "month compares numerically, not lexically");
    ASSERT_TRUE(shard_db_version_compare("2026.07.3", "2026.08.1") < 0,
                "older month is less");
    ASSERT_TRUE(shard_db_version_compare("2026.08.1", "2026.07.3") > 0,
                "newer month is greater");
    ASSERT_EQ_INT(shard_db_version_compare("2026.08.1", "2026.08.1"), 0,
                  "equal versions compare equal");
    ASSERT_TRUE(shard_db_version_compare("2027.01.1", "2026.12.9") > 0,
                "year dominates month/counter");
    ASSERT_TRUE(shard_db_version_compare("0.0.0", "2026.08.1") < 0,
                "the missing-.version sentinel sorts before any real release");
    ASSERT_TRUE(shard_db_version_compare("garbage", "2026.08.1") < 0,
                "malformed string sorts as older than any well-formed version");
    ASSERT_TRUE(shard_db_version_compare("2026.08.1junk", "2026.08.1") < 0,
                "trailing garbage is malformed, not equal");
    ASSERT_TRUE(shard_db_version_compare("2026.13.1", "2026.08.1") < 0,
                "out-of-range month is malformed");
    ASSERT_TRUE(shard_db_version_compare("026.08.1", "2026.08.1") < 0,
                "non-CalVer year width is malformed");
    /* Regression: the canonical zero-padded CalVer ("2026.08.1",
       SHARD_DB_VERSION, repo release-naming convention) must be VALID —
       the startup gate round-trips .version through shard_db_version_is_valid,
       and a parser that only accepted unpadded months would make every
       running install refuse to start on its own .version file. */
    ASSERT_TRUE(shard_db_version_is_valid("2026.08.1"),
                "zero-padded month is valid (the canonical on-disk form)");
    ASSERT_TRUE(shard_db_version_is_valid("2026.10.1"),
                "two-digit month is valid");
    ASSERT_TRUE(shard_db_version_is_valid("2026.12.9"),
                "unpadded counter is valid");
    ASSERT_TRUE(!shard_db_version_is_valid("2026.8.1"),
                "unpadded month is malformed");
    return 0;
}

TEST_REGISTER("version-compare", test_version_compare_orders_calver_numerically)

static int test_version_file_roundtrip(void) {
    char tmpdir[] = "/tmp/shard-db-version-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(tmpdir) != NULL, "made tmpdir");

    char buf[64];
    ASSERT_EQ_INT(shard_db_version_file_read(tmpdir, buf, sizeof(buf)),
                  SHARD_DB_VERSION_FILE_MISSING,
                  "missing .version file is distinguishable");

    ASSERT_EQ_INT(shard_db_version_file_write(tmpdir, "2026.08.1"), 0,
                  "write succeeds");
    ASSERT_EQ_INT(shard_db_version_file_read(tmpdir, buf, sizeof(buf)), 0,
                  "read succeeds after write");
    ASSERT_EQ_STR(buf, "2026.08.1", "round-tripped content matches");

    char path[300];
    snprintf(path, sizeof(path), "%s/.version", tmpdir);
    FILE *f = fopen(path, "w");
    ASSERT_TRUE(f != NULL, "opened marker for malformed-content test");
    if (f) {
        fputs("2026.08.1\njunk\n", f);
        fclose(f);
    }
    ASSERT_EQ_INT(shard_db_version_file_read(tmpdir, buf, sizeof(buf)),
                  SHARD_DB_VERSION_FILE_ERROR,
                  "trailing marker content is rejected");

    unlink(path);
    ASSERT_EQ_INT(rmdir(tmpdir), 0, "cleaned up tmpdir");
    return 0;
}

TEST_REGISTER("version-file-roundtrip", test_version_file_roundtrip)

static int test_version_subcommand_prints_compiled_version(void) {
    char *out = tu_capture_cmd("./build/bin/shard-db version 2>&1");
    ASSERT_TRUE(out != NULL, "captured version subcommand output");
    if (out) {
        ASSERT_TRUE(strstr(out, SHARD_DB_VERSION) != NULL,
                    "output contains the compiled-in version string");
        ASSERT_TRUE(strstr(out, SHARD_DB_REQUIRED_SOURCE_VERSION) != NULL,
                    "output contains the required source version");
        ASSERT_TRUE(strstr(out, "enforced") != NULL,
                    "output states that the source requirement is enforced");
        free(out);
    }
    return 0;
}

TEST_REGISTER("version-subcommand", test_version_subcommand_prints_compiled_version)
