/* Maintenance-command validation regressions.
 *
 * These tests exercise the public JSON/TCP and exported-command boundaries:
 * missing schemas, schema-valid objects that cannot be opened, and recount
 * failures after a registry handle has already been cached.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int request_response(TestClient *tc, const char *json, char **resp,
                            const char *desc) {
    int rc = tc_request(tc, json, resp);
    ASSERT_EQ_INT(rc, 0, desc);
    ASSERT_NOT_NULL(*resp, desc);
    return rc == 0 && *resp != NULL;
}

static int test_vacuum_recount_missing_object_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) {
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    if (request_response(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
            &resp, "add default directory")) {
        ASSERT_CONTAINS(resp, "\"status\"", "default directory added");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"ghost\"}",
            &resp, "vacuum missing object responds")) {
        ASSERT_CONTAINS(resp, "\"error\"",
                        "vacuum missing object reports an error");
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"status\":\"vacuumed\""),
                    "vacuum missing object does not report success");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"ghost\"}",
            &resp, "recount missing object responds")) {
        ASSERT_CONTAINS(resp, "\"error\"",
                        "recount missing object reports an error");
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"count\""),
                    "recount missing object does not report a count");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"create-object\",\"dir\":\"default\","
            "\"object\":\"real_empty\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"v:int\"]}",
            &resp, "create real empty object")) {
        ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                        "real empty object created");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"real_empty\"}",
            &resp, "vacuum real empty object responds")) {
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""),
                    "vacuum real empty object succeeds");
        ASSERT_CONTAINS(resp, "\"status\":\"vacuumed\"",
                        "vacuum real empty object reports vacuumed");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"real_empty\"}",
            &resp, "recount real empty object responds")) {
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""),
                    "recount real empty object succeeds");
        ASSERT_CONTAINS(resp, "\"count\":0",
                        "recount real empty object reports zero");
    }
    free(resp);

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-vacuum-recount-missing-object",
              test_vacuum_recount_missing_object_run)

static int test_vacuum_recount_command_boundary_missing_object_run(void) {
    const char *db_root = test_get_process_db_root();
    ASSERT_NOT_NULL(db_root, "process-local db root");
    if (!db_root) return 1;

    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/default", db_root);

    char *resp = NULL;
    size_t resp_len = 0;
    FILE *capture = open_memstream(&resp, &resp_len);
    ASSERT_NOT_NULL(capture, "capture vacuum output");
    if (!capture) return 1;

    FILE *previous_out = g_out;
    g_out = capture;
    int rc = cmd_vacuum(effective_root, "ghost", 0, 0);
    fclose(capture);
    g_out = previous_out;

    ASSERT_EQ_INT(rc, 1, "vacuum command rejects missing schema");
    ASSERT_CONTAINS(resp, "\"error\":\"object not found\"",
                    "vacuum command uses maintenance missing-schema error");
    free(resp); resp = NULL; resp_len = 0;

    capture = open_memstream(&resp, &resp_len);
    ASSERT_NOT_NULL(capture, "capture recount output");
    if (!capture) return 1;

    previous_out = g_out;
    g_out = capture;
    rc = cmd_recount(effective_root, "ghost");
    fclose(capture);
    g_out = previous_out;

    ASSERT_EQ_INT(rc, 1, "recount command rejects missing schema");
    ASSERT_CONTAINS(resp, "\"error\":\"object not found\"",
                    "recount command uses maintenance missing-schema error");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"count\""),
                "recount command does not publish a missing-object count");
    free(resp);
    return 0;
}

TEST_REGISTER("test-vacuum-recount-command-boundary-missing-object",
              test_vacuum_recount_command_boundary_missing_object_run)

static int test_vacuum_recount_object_not_open_run(void) {
    if (geteuid() == 0) {
        TAP_DIAG("# skipping: running as root, chmod-based open-failure "
                 "injection does not apply\n");
        return 0;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) {
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    if (request_response(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
            &resp, "add default directory for open failure")) {
        ASSERT_CONTAINS(resp, "\"status\"", "open-failure directory added");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"create-object\",\"dir\":\"default\","
            "\"object\":\"maint_not_open\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"v:int\"]}",
            &resp, "create object for open failure")) {
        ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                        "open-failure object created");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"insert\",\"dir\":\"default\","
            "\"object\":\"maint_not_open\",\"key\":\"k1\","
            "\"value\":{\"v\":1}}",
            &resp, "insert open-failure control record")) {
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""),
                    "open-failure control record inserted");
    }
    free(resp); resp = NULL;

    char saved_db_root[PATH_MAX];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path),
             "%s/default/maint_not_open/data/kf/000.kf", saved_db_root);

    struct stat kf_st;
    int stat_rc = stat(kf_path, &kf_st);
    ASSERT_EQ_INT(stat_rc, 0, "stat kf shard before open-failure injection");
    if (stat_rc != 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    tc_close(tc);
    tc = NULL;
    test_env_stop_keep(&env);

    int chmod_changed = 0;
    int restarted = 0;
    TestClient *tc2 = NULL;
    int chmod_rc = chmod(kf_path, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions");
    if (chmod_rc != 0) goto cleanup;
    chmod_changed = 1;

    int start_rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_EQ_INT(start_rc, 0, "restart daemon with unreadable kf shard");
    if (start_rc != 0) goto cleanup;
    restarted = 1;

    cfg.port = env.port;
    tc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc2, "reconnect after permission injection");
    if (!tc2) goto cleanup;

    if (request_response(tc2,
            "{\"mode\":\"vacuum\",\"dir\":\"default\","
            "\"object\":\"maint_not_open\"}",
            &resp, "vacuum unopenable object responds")) {
        ASSERT_CONTAINS(resp, "\"error\":\"object not open\"",
                        "vacuum reports registry open failure");
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"status\":\"vacuumed\""),
                    "vacuum open failure does not report success");
    }
    free(resp); resp = NULL;

    if (request_response(tc2,
            "{\"mode\":\"recount\",\"dir\":\"default\","
            "\"object\":\"maint_not_open\"}",
            &resp, "recount unopenable object responds")) {
        ASSERT_CONTAINS(resp, "\"error\":\"object not open\"",
                        "recount reports registry open failure");
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"count\""),
                    "recount open failure does not report a count");
    }
    free(resp); resp = NULL;

cleanup:
    free(resp);
    if (tc2) tc_close(tc2);
    if (chmod_changed) {
        ASSERT_EQ_INT(chmod(kf_path, original_mode), 0,
                      "restore kf shard permissions");
    }
    if (restarted) {
        test_env_stop(&env);
    } else if (test_env_start_at(&env, saved_db_root, saved_port) == 0) {
        test_env_stop(&env);
    }
    return 0;
}

TEST_REGISTER("test-vacuum-recount-object-not-open",
              test_vacuum_recount_object_not_open_run)

static int test_recount_kf_header_read_failure_run(void) {
    if (geteuid() == 0) {
        TAP_DIAG("# skipping: running as root, chmod-based kf-read failure "
                 "injection does not apply\n");
        return 0;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) {
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    if (request_response(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
            &resp, "add default directory for recount read failure")) {
        ASSERT_CONTAINS(resp, "\"status\"",
                        "recount-read-failure directory added");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"create-object\",\"dir\":\"default\","
            "\"object\":\"recount_read_fail\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"v:int\"]}",
            &resp, "create object for recount read failure")) {
        ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                        "recount-read-failure object created");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"insert\",\"dir\":\"default\","
            "\"object\":\"recount_read_fail\",\"key\":\"k1\","
            "\"value\":{\"v\":1}}",
            &resp, "insert recount control record")) {
        ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""),
                    "recount control record inserted");
    }
    free(resp); resp = NULL;

    if (request_response(tc,
            "{\"mode\":\"recount\",\"dir\":\"default\","
            "\"object\":\"recount_read_fail\"}",
            &resp, "control recount responds")) {
        ASSERT_CONTAINS(resp, "\"count\":1", "control recount reports one");
    }
    free(resp); resp = NULL;

    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path),
             "%s/default/recount_read_fail/data/kf/000.kf", env.db_root);
    struct stat kf_st;
    int stat_rc = stat(kf_path, &kf_st);
    ASSERT_EQ_INT(stat_rc, 0, "stat kf shard before recount read failure");
    if (stat_rc != 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    int chmod_rc = chmod(kf_path, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions while cached");
    if (chmod_rc == 0) {
        if (request_response(tc,
                "{\"mode\":\"recount\",\"dir\":\"default\","
                "\"object\":\"recount_read_fail\"}",
                &resp, "recount with unreadable header responds")) {
            ASSERT_CONTAINS(resp, "\"error\":\"recount failed\"",
                            "recount reports kf-header read failure");
            ASSERT_TRUE(!SAFE_STRSTR(resp, "\"count\""),
                        "failed recount does not publish a partial count");
        }
        free(resp); resp = NULL;
        ASSERT_EQ_INT(chmod(kf_path, original_mode), 0,
                      "restore cached object's kf shard permissions");
    }

    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-recount-kf-header-read-failure",
              test_recount_kf_header_read_failure_run)
