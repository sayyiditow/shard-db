#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "test_client.h"
#include "version.h"
#include "types.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int read_format_marker(const char *db_root, const char *dir,
                              const char *object) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/.format", db_root, dir, object);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    char byte = 0;
    ssize_t n = read(fd, &byte, 1);
    int close_rc = close(fd);
    if (n != 1 || close_rc != 0) return -1;
    return (byte == '0') ? 0 : (byte == '1') ? 1 : -1;
}

/* load_schema() resolves schema.conf through the active embedded handle's
   g_db_root. Temporarily point the process-local test handle at this fresh
   daemon fixture while exercising the public startup seam, then restore it
   so the next process-local case remains isolated. */
static int startup_migrate_at(const char *db_root) {
    ShardDb *process_db = test_get_process_db();
    if (!process_db) return -1;
    char saved_root[PATH_MAX];
    snprintf(saved_root, sizeof(saved_root), "%s", process_db->db_root);
    snprintf(process_db->db_root, sizeof(process_db->db_root), "%s", db_root);
    int rc = shard_db_startup_migrate(db_root, NULL, 0);
    snprintf(process_db->db_root, sizeof(process_db->db_root), "%s", saved_root);
    return rc;
}

static int test_startup_format_sweep_run(void) {
    /* Use a daemon fixture so the object is created through the public
       schema-driven API in a fresh db_root, then run the shared startup seam
       offline against that same root. */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[PATH_MAX];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connected to fresh daemon fixture");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sweep\","
        "\"object\":\"fmt_test\",\"fields\":[\"v:int\"],"
        "\"splits\":8}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "create fresh FIXED-format object");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"sweep\","
        "\"object\":\"fmt_test\",\"key\":\"k1\","
        "\"value\":{\"v\":10}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "insert k1 succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"sweep\","
        "\"object\":\"fmt_test\",\"key\":\"k2\","
        "\"value\":{\"v\":20}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "insert k2 succeeds");
    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop_keep(&env);

    ASSERT_EQ_INT(read_format_marker(saved_db_root, "sweep", "fmt_test"),
                  0, "fresh object starts in FIXED format");

    /* Exercise the version-gated path and the unconditional format sweep
       together. */
    ASSERT_EQ_INT(shard_db_version_file_write(saved_db_root,
                                               SHARD_DB_MIN_VERSION),
                  0, "write minimum supported version");
    ASSERT_EQ_INT(startup_migrate_at(saved_db_root), 0,
                  "startup migration converts the fresh object");
    ASSERT_EQ_INT(read_format_marker(saved_db_root, "sweep", "fmt_test"),
                  1, "startup sweep writes VARIABLE format marker");

    /* Reopen through the daemon to prove every record survived the actual
       startup path, not merely the marker rewrite. */
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart after startup format sweep");
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after startup format sweep");
    if (tc) {
        tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"sweep\","
            "\"object\":\"fmt_test\",\"key\":\"k1\"}", &resp);
        ASSERT_CONTAINS(resp, "\"v\":10", "k1 survives conversion");
        free(resp); resp = NULL;
        tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"sweep\","
            "\"object\":\"fmt_test\",\"key\":\"k2\"}", &resp);
        ASSERT_CONTAINS(resp, "\"v\":20", "k2 survives conversion");
        free(resp); resp = NULL;
        tc_close(tc);
    }
    test_env_stop_keep(&env);

    /* Current version takes the NOOP path; the unconditional sweep remains
       idempotent and must leave the converted object unchanged. */
    ASSERT_EQ_INT(shard_db_version_file_write(saved_db_root,
                                               SHARD_DB_VERSION),
                  0, "write current version");
    ASSERT_EQ_INT(startup_migrate_at(saved_db_root), 0,
                  "second startup migration takes NOOP path");
    ASSERT_EQ_INT(read_format_marker(saved_db_root, "sweep", "fmt_test"),
                  1, "NOOP startup leaves VARIABLE marker unchanged");

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-startup-format-sweep", test_startup_format_sweep_run)
