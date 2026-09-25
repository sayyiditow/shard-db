#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char *get_value(TestClient *tc, const char *object, const char *key) {
    char req[256];
    snprintf(req, sizeof(req),
             "{\"mode\":\"get\",\"dir\":\"default\","
             "\"object\":\"%s\",\"key\":\"%s\"}", object, key);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    return resp;
}

static int read_contains(const char *path, const char *needle) {
    char *text = tu_read_file(path);
    int found = text && strstr(text, needle) != NULL;
    free(text);
    return found;
}

static int test_datetime_migration_run(void) {
    char base_template[] = "/tmp/shard-db-dt-migrate-XXXXXX";
    char *base = mkdtemp(base_template);
    ASSERT_NOT_NULL(base, "mkdtemp");
    if (!base) return 1;

    char root[256];
    snprintf(root, sizeof(root), "%s/db", base);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "./build/bin/datetime-migration-fixture '%s'", root);
    ASSERT_EQ_INT(tu_run_cmd("%s", cmd), 0, "old fixture created");
    if (t_ctx->failed) return 1;

    TestEnv env = {0};
    int port = test_pick_port();
    ASSERT_TRUE(port > 0, "pick migration port");
    if (port <= 0 || test_env_start_at(&env, root, port) != 0) return 1;
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect migrated root");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *keys[] = {"noon", "last_ok", "first_wrap", "eod"};
    const char *values[] = {"20240102120000", "20240102181215",
                            "20240102000000", "20240102054743"};
    for (int i = 0; i < 4; i++) {
        char *resp = get_value(tc, "migrate_dt", keys[i]);
        ASSERT_CONTAINS(resp, values[i], keys[i]);
        free(resp);
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\","
        "\"object\":\"migrate_dt\",\"key\":\"new_eod\","
        "\"value\":{\"v\":\"20240102235959\"}}", &resp);
    free(resp);
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
        "\"object\":\"migrate_dt\",\"criteria\":[{\"field\":\"v\","
        "\"op\":\"gte\",\"value\":\"20240102181216\"}]}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 1, "late-day count after migration");
    free(resp);

    char path[512];
    snprintf(path, sizeof(path), "%s/.version", root);
    ASSERT_TRUE(read_contains(path, "2026.09.2"), "version stamped after migration");
    snprintf(path, sizeof(path), "%s/default/migrate_dt/fields.conf", root);
    ASSERT_TRUE(read_contains(path, "#datetime_7byte"), "migration marker written");
    resp = get_value(tc, "control", "one");
    ASSERT_CONTAINS(resp, "1", "control object remains readable");
    free(resp);

    tc_close(tc);
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart migrated root");
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect migrated root");
    if (tc) {
        resp = get_value(tc, "migrate_dt", "new_eod");
        ASSERT_CONTAINS(resp, "20240102235959", "rerun keeps widened value");
        free(resp);
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Stale schema.conf entries — tenant dirs (or an object dir) listed in
   schema.conf but never materialized, or removed without pruning — must be
   skipped by the startup datetime migration, not abort it. Regression: a
   missing fields.conf on a stale entry returned -1 out of
   shard_db_startup_migrate, leaving the root unstamped and refusing the
   open entirely. */
static int test_datetime_migration_stale_schema_run(void) {
    char base_template[] = "/tmp/shard-db-dt-migrate-stale-XXXXXX";
    char *base = mkdtemp(base_template);
    ASSERT_NOT_NULL(base, "mkdtemp");
    if (!base) return 1;

    char root[256];
    snprintf(root, sizeof(root), "%s/db", base);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "./build/bin/datetime-migration-fixture '%s'", root);
    ASSERT_EQ_INT(tu_run_cmd("%s", cmd), 0, "old fixture created");
    if (t_ctx->failed) return 1;

    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", root);
    FILE *f = fopen(schema_path, "a");
    ASSERT_NOT_NULL(f, "append stale entries to schema.conf");
    if (f) {
        fprintf(f, "co_ghost:customers:8:64:2:8\n");
        fprintf(f, "dbg-old:customers:8:64:2:8\n");
        fprintf(f, "default:ghost_object:8:64:2:8\n");
        fclose(f);
    }

    TestEnv env = {0};
    int port = test_pick_port();
    ASSERT_TRUE(port > 0, "pick stale-schema migration port");
    if (port <= 0 || test_env_start_at(&env, root, port) != 0) return 1;
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect root with stale schema entries");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = get_value(tc, "migrate_dt", "eod");
    ASSERT_CONTAINS(resp, "20240102054743", "migrated value readable");
    free(resp);

    char path[512];
    snprintf(path, sizeof(path), "%s/.version", root);
    ASSERT_TRUE(read_contains(path, "2026.09.2"),
                "version stamped despite stale schema entries");
    snprintf(path, sizeof(path), "%s/default/migrate_dt/fields.conf", root);
    ASSERT_TRUE(read_contains(path, "#datetime_7byte"), "migration marker written");
    resp = get_value(tc, "control", "one");
    ASSERT_CONTAINS(resp, "1", "control object remains readable");
    free(resp);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-datetime-migration", test_datetime_migration_run)
TEST_REGISTER("test-datetime-migration-stale-schema", test_datetime_migration_stale_schema_run)
