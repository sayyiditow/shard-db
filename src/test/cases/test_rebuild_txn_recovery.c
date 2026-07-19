#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int create_object_with_records(TestEnv *env, const char *object,
                                      int count) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;

    char *resp = NULL;
    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) {
        tc_close(tc);
        return -1;
    }
    free(resp);

    char req[768];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"]}", object);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp);
        tc_close(tc);
        return -1;
    }
    free(resp);

    for (int i = 0; i < count; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\","
            "\"object\":\"%s\",\"key\":\"item%04d\","
            "\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            object, i, i * 5, i);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp);
            tc_close(tc);
            return -1;
        }
        free(resp);
    }

    tc_close(tc);
    return 0;
}

static int request_count(TestEnv *env, const char *object) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}",
        object);
    char *resp = NULL;
    int result = -1;
    if (tc_request(tc, req, &resp) == 0) result = tu_parse_count(resp);
    free(resp);
    tc_close(tc);
    return result;
}

static int read_schema_splits(const char *db_root, const char *object) {
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/schema.conf", db_root);
    FILE *f = fopen(conf_path, "r");
    if (!f) return -1;
    char prefix[300];
    snprintf(prefix, sizeof(prefix), "default:%s:", object);
    size_t plen = strlen(prefix);
    char line[512];
    int splits = -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, prefix, plen) == 0) {
            sscanf(line + plen, "%d", &splits);
            break;
        }
    }
    fclose(f);
    return splits;
}

static int append_pause_config(const char *db_root, const char *phase) {
    char base[PATH_MAX], env_path[PATH_MAX];
    snprintf(base, sizeof(base), "%s", db_root);
    char *slash = strrchr(base, '/');
    if (!slash) return -1;
    *slash = '\0';
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "a");
    if (!f) return -1;
    fprintf(f, "export REBUILD_TEST_PAUSE_PHASE=%s\n"
               "export REBUILD_TEST_PAUSE_MS=30000\n", phase);
    return fclose(f);
}

static int wait_for_path(const char *path, int timeout_ms) {
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 20) {
        if (access(path, F_OK) == 0) return 0;
        usleep(20000);
    }
    return -1;
}

static pid_t trigger_splits_rebuild(TestEnv *env, const char *object) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"vacuum\",\"dir\":\"default\","
             "\"object\":\"%s\",\"splits\":16}", object);
    int rc = tc_request(tc, req, &resp);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static pid_t trigger_edit_rebuild(TestEnv *env, const char *object) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"edit-field\",\"dir\":\"default\","
             "\"object\":\"%s\",\"fields\":[\"title:varchar:128\"]}",
             object);
    int rc = tc_request(tc, req, &resp);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static char *request_get(TestEnv *env, const char *object, const char *key) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\"}", object, key);
    if (tc_request(tc, req, &resp) != 0) { free(resp); resp = NULL; }
    tc_close(tc);
    return resp;
}

static int test_legacy_stage1(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    ASSERT_EQ_INT(create_object_with_records(&env, "legacys1", 50), 0,
                  "create object and records");
    test_env_stop_keep(&env);

    char data_dir[PATH_MAX], legacy_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/default/legacys1/data",
             saved_db_root);
    snprintf(legacy_dir, sizeof(legacy_dir), "%s/default/legacys1/data.legacy",
             saved_db_root);
    ASSERT_EQ_INT(rename(data_dir, legacy_dir), 0,
                  "construct pre-stage legacy crash layout");
    ASSERT_TRUE(access(data_dir, F_OK) != 0,
                "live data path absent before restart");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "daemon restarts after unambiguous legacy crash");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, "legacys1"), 50,
                      "all records restored before requests are served");
        ASSERT_TRUE(access(data_dir, F_OK) == 0,
                    "recovery restores live data path");
        ASSERT_TRUE(access(legacy_dir, F_OK) != 0,
                    "recovery consumes data.legacy");
        test_env_stop(&env);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_legacy_stage2_no_data(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    ASSERT_EQ_INT(create_object_with_records(&env, "legacys2", 50), 0,
                  "create object and records");
    test_env_stop_keep(&env);

    char data_dir[PATH_MAX], legacy_root[PATH_MAX], legacy_data[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/default/legacys2/data",
             saved_db_root);
    snprintf(legacy_root, sizeof(legacy_root),
             "%s/default/legacys2/.rebuild_legacy_root", saved_db_root);
    snprintf(legacy_data, sizeof(legacy_data), "%s/data", legacy_root);
    ASSERT_EQ_INT(mkdir(legacy_root, 0755), 0,
                  "create legacy rebuild root");
    ASSERT_EQ_INT(rename(data_dir, legacy_data), 0,
                  "construct post-stage legacy crash layout");
    ASSERT_TRUE(access(data_dir, F_OK) != 0,
                "live data path absent before restart");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "daemon restarts after unambiguous post-stage crash");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, "legacys2"), 50,
                      "all post-stage records restored before requests");
        ASSERT_TRUE(access(data_dir, F_OK) == 0,
                    "recovery restores post-stage live data path");
        ASSERT_TRUE(access(legacy_root, F_OK) != 0,
                    "recovery consumes legacy rebuild root");
        test_env_stop(&env);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_legacy_ambiguous(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    ASSERT_EQ_INT(create_object_with_records(&env, "legacyamb", 20), 0,
                  "create ambiguous-layout fixture");
    test_env_stop_keep(&env);

    char data_dir[PATH_MAX], legacy_root[PATH_MAX], legacy_data[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/default/legacyamb/data",
             saved_db_root);
    snprintf(legacy_root, sizeof(legacy_root),
             "%s/default/legacyamb/.rebuild_legacy_root", saved_db_root);
    snprintf(legacy_data, sizeof(legacy_data), "%s/data", legacy_root);
    ASSERT_EQ_INT(mkdir(legacy_root, 0755), 0,
                  "create ambiguous legacy root");
    ASSERT_EQ_INT(tu_run_cmd("cp -a '%s' '%s'", data_dir, legacy_data), 0,
                  "copy data into ambiguous legacy root");

    int start_rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(start_rc != 0,
                "daemon refuses ambiguous legacy crash layout");
    ASSERT_TRUE(access(data_dir, F_OK) == 0,
                "startup refusal preserves live data");
    ASSERT_TRUE(access(legacy_data, F_OK) == 0,
                "startup refusal preserves legacy data");
    if (start_rc == 0) test_env_stop(&env);
    else tu_run_cmd("rm -rf '%s'", saved_db_root);

    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_txn_crash_phase(const char *phase, const char *object) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    ASSERT_EQ_INT(create_object_with_records(&env, object, 60), 0,
                  "create transaction-crash fixture");
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(append_pause_config(saved_db_root, phase), 0,
                  "enable deterministic rebuild pause");
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart with pause hook");
    if (env.daemon_pid <= 0) return 1;

    pid_t request_pid = trigger_splits_rebuild(&env, object);
    ASSERT_TRUE(request_pid > 0, "spawn rebuild request");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker), "%s/default/%s/.rebuild-test-%s.active",
             saved_db_root, object, phase);
    int marker_rc = wait_for_path(marker, 5000);
    if (marker_rc != 0) {
        char base[PATH_MAX];
        snprintf(base, sizeof(base), "%s", saved_db_root);
        char *slash = strrchr(base, '/');
        if (slash) { *slash = '\0'; tu_run_cmd("tail -50 '%s/daemon.log'", base); }
    }
    ASSERT_EQ_INT(marker_rc, 0,
                  "rebuild reaches deterministic pause");
    if (marker_rc == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart rolls back active transaction");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, object), 60,
                      "rollback restores all pre-rebuild records");
        char active[PATH_MAX];
        snprintf(active, sizeof(active), "%s/default/%s/.rebuild_txn.active",
                 saved_db_root, object);
        ASSERT_TRUE(access(active, F_OK) != 0,
                    "successful recovery consumes active transaction");
        ASSERT_EQ_INT(read_schema_splits(saved_db_root, object), 8,
                      "schema.conf splits reverted to pre-rebuild value");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_txn_crash_after_stage(void) {
    return test_txn_crash_phase("after-stage", "txnstage");
}

static int test_txn_crash_after_walk(void) {
    return test_txn_crash_phase("after-walk", "txnwalk");
}

static int test_txn_crash_after_metadata_splits(void) {
    return test_txn_crash_phase("after-metadata", "txnmetasplits");
}

static int write_text_file(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    int rc = fputs(text, f) < 0 ? -1 : 0;
    if (fclose(f) != 0) rc = -1;
    return rc;
}

static int test_done_cleanup(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    ASSERT_EQ_INT(create_object_with_records(&env, "txndone", 40), 0,
                  "create committed-cleanup fixture");
    test_env_stop_keep(&env);

    char done[PATH_MAX], bad_data[PATH_MAX], sentinel[PATH_MAX];
    snprintf(done, sizeof(done),
             "%s/default/txndone/.rebuild_txn.done", root);
    snprintf(bad_data, sizeof(bad_data), "%s/data", done);
    snprintf(sentinel, sizeof(sentinel), "%s/incomplete", bad_data);
    ASSERT_EQ_INT(mkdir(done, 0755), 0, "create committed transaction state");
    ASSERT_EQ_INT(mkdir(bad_data, 0755), 0, "create deliberately incomplete backup");
    ASSERT_EQ_INT(write_text_file(sentinel, "never restore\n"), 0,
                  "write incomplete backup sentinel");

    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0,
                  "restart cleans committed state");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, "txndone"), 40,
                      "complete live data remains authoritative");
        ASSERT_TRUE(access(done, F_OK) != 0,
                    "committed transaction directory is cleanup-only");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_edit_crash_after_metadata(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    ASSERT_EQ_INT(create_object_with_records(&env, "txnedit", 3), 0,
                  "create edit-field crash fixture");
    char *before = request_get(&env, "txnedit", "item0001");
    ASSERT_TRUE(before && SAFE_STRSTR(before, "\"score\":5") &&
                SAFE_STRSTR(before, "\"title\":\"t1\""),
                "original record decodes before edit");
    free(before);
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(append_pause_config(root, "after-metadata"), 0,
                  "enable after-metadata pause");
    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0,
                  "restart edit fixture with pause hook");
    if (env.daemon_pid <= 0) return 1;

    pid_t request_pid = trigger_edit_rebuild(&env, "txnedit");
    ASSERT_TRUE(request_pid > 0, "spawn edit-field request");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker),
             "%s/default/txnedit/.rebuild-test-after-metadata.active", root);
    int marker_rc = wait_for_path(marker, 5000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "edit rebuild reaches metadata pause");
    if (marker_rc == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);

    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0,
                  "restart rolls edit-field transaction back");
    if (env.daemon_pid > 0) {
        char fields_path[PATH_MAX];
        snprintf(fields_path, sizeof(fields_path),
                 "%s/default/txnedit/fields.conf", root);
        char *fields = tu_read_file(fields_path);
        ASSERT_TRUE(fields && SAFE_STRSTR(fields, "title:varchar:64") &&
                    !SAFE_STRSTR(fields, "title:varchar:128"),
                    "rollback restores original fields.conf");
        free(fields);
        char *after = request_get(&env, "txnedit", "item0001");
        ASSERT_TRUE(after && SAFE_STRSTR(after, "\"score\":5") &&
                    SAFE_STRSTR(after, "\"title\":\"t1\""),
                    "rollback restores original record encoding and value");
        free(after);
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_recovery_idempotent(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    ASSERT_EQ_INT(create_object_with_records(&env, "txnidempotent", 35), 0,
                  "create idempotent-recovery fixture");
    test_env_stop_keep(&env);

    char obj[PATH_MAX], active[PATH_MAX], rollback[PATH_MAX], fields[PATH_MAX],
         meta[PATH_MAX];
    snprintf(obj, sizeof(obj), "%s/default/txnidempotent", root);
    snprintf(active, sizeof(active), "%s/.rebuild_txn.active", obj);
    snprintf(rollback, sizeof(rollback), "%s/fields.conf.rollback", active);
    snprintf(fields, sizeof(fields), "%s/fields.conf", obj);
    snprintf(meta, sizeof(meta), "%s/meta", active);
    ASSERT_EQ_INT(mkdir(active, 0755), 0,
                  "construct rollback-after-data-rename state");
    ASSERT_EQ_INT(tu_run_cmd("cp '%s' '%s'", fields, rollback), 0,
                  "retain fields rollback copy");
    ASSERT_EQ_INT(write_text_file(meta,
                  "version=1\nold_splits=8\nold_streams=1\n"
                  "indexes_may_change=0\n"), 0,
                  "write valid transaction manifest");

    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0,
                  "first restart finishes interrupted rollback");
    if (env.daemon_pid > 0) test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0,
                  "second restart is a no-op");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, "txnidempotent"), 35,
                      "repeated recovery preserves all records");
        ASSERT_TRUE(access(active, F_OK) != 0,
                    "active state remains consumed after repeated recovery");
        test_env_stop(&env);
    }
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_embedded_open_respects_daemon_lock(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_EQ_INT(create_object_with_records(&env, "txnlock", 5), 0,
                  "create embedded-lock fixture");
    char preparing[PATH_MAX], sentinel[PATH_MAX];
    snprintf(preparing, sizeof(preparing),
             "%s/default/txnlock/.rebuild_txn.preparing", env.db_root);
    snprintf(sentinel, sizeof(sentinel), "%s/preserve", preparing);
    ASSERT_EQ_INT(mkdir(preparing, 0755), 0,
                  "create recovery artifact while daemon owns root");
    ASSERT_EQ_INT(write_text_file(sentinel, "preserve\n"), 0,
                  "write recovery artifact sentinel");

    ASSERT_EQ_INT(tu_run_cmd("./build/bin/embedded_lock_harness '%s'",
                             env.db_root), 0,
                  "embedded open is refused while daemon owns DB root");
    ASSERT_TRUE(access(sentinel, F_OK) == 0,
                "refused embedded open does not mutate recovery artifacts");
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-legacy-stage1-safe-restore", test_legacy_stage1)
TEST_REGISTER("test-rebuild-legacy-stage2-no-data-safe-restore", test_legacy_stage2_no_data)
TEST_REGISTER("test-rebuild-legacy-ambiguous-refuses-start", test_legacy_ambiguous)
TEST_REGISTER("test-rebuild-txn-crash-after-stage", test_txn_crash_after_stage)
TEST_REGISTER("test-rebuild-txn-crash-after-walk", test_txn_crash_after_walk)
TEST_REGISTER("test-rebuild-txn-crash-after-metadata-splits", test_txn_crash_after_metadata_splits)
TEST_REGISTER("test-rebuild-txn-edit-field-after-metadata", test_edit_crash_after_metadata)
TEST_REGISTER("test-rebuild-txn-done-cleanup-never-restores", test_done_cleanup)
TEST_REGISTER("test-rebuild-txn-recovery-idempotent", test_recovery_idempotent)
TEST_REGISTER("test-rebuild-txn-embedded-lock", test_embedded_open_respects_daemon_lock)
