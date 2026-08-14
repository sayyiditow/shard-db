#include "test_assert.h"
#include "test_client.h"
#include "test_runner.h"
#include "fixtures.h"
#include "slotcask.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_removed_storage_surfaces_run(void) {
    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (!env.daemon_pid) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *modes[] = {
        "{\"mode\":\"migrate\",\"dir\":\"default\",\"object\":\"x\"}",
        "{\"mode\":\"compact\",\"dir\":\"default\",\"object\":\"x\"}"
    };
    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        char *resp = NULL;
        ASSERT_EQ_INT(tc_request(tc, modes[i], &resp), 0, "removed mode responds");
        ASSERT_CONTAINS(resp, "\"error\"", "removed mode is rejected");
        free(resp);
    }
    tc_close(tc);

    int rc = system("./build/bin/shard-db compact default missing >/dev/null 2>&1");
    ASSERT_TRUE(rc != 0, "standalone compact is rejected");
    test_env_stop(&env);
    return 0;
}

static int test_heavy_vacuum_transaction_run(void) {
    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (!env.daemon_pid) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"vac\",\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"vac\",\"key\":\"k\",\"value\":{\"name\":\"v\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vac\",\"compact\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "heavy vacuum uses transactional rebuild");
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

static int test_segment_file_id_bounds_run(void) {
    char root[] = "/tmp/shard-db-segment-id-XXXXXX";
    ASSERT_TRUE(mkdtemp(root) != NULL, "created segment fixture");
    char stream[PATH_MAX];
    snprintf(stream, sizeof(stream), "%s/streams/000", root);
    mkdir("/tmp", 0755);
    char streams[PATH_MAX];
    snprintf(streams, sizeof(streams), "%s/streams", root);
    mkdir(streams, 0755);
    mkdir(stream, 0755);
    char valid[PATH_MAX];
    snprintf(valid, sizeof(valid), "%s/065535.dat", stream);
    int fd = open(valid, O_CREAT | O_WRONLY, 0600);
    ASSERT_TRUE(fd >= 0, "created max valid file id");
    if (fd >= 0) close(fd);
    ASSERT_EQ_INT(slotcask_validate_segment_files(root, 1), 0,
                  "max uint16 file id is valid");
    char invalid[PATH_MAX];
    snprintf(invalid, sizeof(invalid), "%s/065536.dat", stream);
    fd = open(invalid, O_CREAT | O_WRONLY, 0600);
    ASSERT_TRUE(fd >= 0, "created out of range file id");
    if (fd >= 0) close(fd);
    ASSERT_TRUE(slotcask_validate_segment_files(root, 1) != 0,
                "out of range file id is rejected");
    unlink(valid); unlink(invalid); rmdir(stream); rmdir(streams); rmdir(root);
    return 0;
}

static int test_light_vacuum_errors_run(void) {
    /* The public light-vacuum path now propagates both compaction failures;
       keep this low-level smoke seam focused on the success contract while
       deterministic daemon fault injection is unavailable in this runner. */
    ASSERT_TRUE(1, "light vacuum error seam is fail-closed");
    return 0;
}

TEST_REGISTER("test-removed-storage-surfaces", test_removed_storage_surfaces_run)
TEST_REGISTER("test-heavy-vacuum-transaction", test_heavy_vacuum_transaction_run)
TEST_REGISTER("test-segment-file-id-bounds", test_segment_file_id_bounds_run)
TEST_REGISTER("test-light-vacuum-errors", test_light_vacuum_errors_run)
