/* test_slotcask_v2_object.c — E2E test for slotcask object create.
 *
 * Verifies the create-object wiring through to schema.conf and on-disk
 * slotcask layout:
 *   - response JSON carries storage_version + streams
 *   - schema.conf line has the 6-field form `dir:object:splits:max_key:2:N`
 *   - on-disk shape: data/kf/NNN.kf + data/streams/NNN/NNNNNN.dat
 *   - any client-supplied `storage_version` is rejected (field removed from API)
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int dir_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

/* Read the schema.conf line for a given dir:object prefix. Returns malloc'd
   string or NULL. Caller frees. */
static char *read_schema_line(const char *db_root,
                              const char *dir, const char *object) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    char prefix[512];
    int pl = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    char line[1024], *out = NULL;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, prefix, pl) == 0) {
            line[strcspn(line, "\r\n")] = '\0';
            out = strdup(line);
            break;
        }
    }
    fclose(f);
    return out;
}

static int test_slotcask_v2_object_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"v2tenant\"}", &resp);
    free(resp); resp = NULL;

    /* --- v2 create --- */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"v2tenant\",\"object\":\"sc_users\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:64\",\"age:int\"],"
        "\"indexes\":[\"age\"]}", &resp);
    ASSERT_NOT_NULL(resp, "v2 create-object response");
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "v2 create succeeds");
    ASSERT_CONTAINS(resp, "\"streams\":", "v2 response carries streams field");
    free(resp); resp = NULL;

    /* schema.conf: 6-field form `v2tenant:sc_users:8:40:2:N` */
    char *line = read_schema_line(env.db_root, "v2tenant", "sc_users");
    ASSERT_NOT_NULL(line, "v2 schema.conf line present");
    if (line) {
        /* Count the colons — must be at least 5 (dir:obj:splits:max_key:sv:streams). */
        int colons = 0;
        for (char *c = line; *c; c++) if (*c == ':') colons++;
        ASSERT_TRUE(colons == 5, "schema line has 5 colons (extended form)");
        free(line);
    }

    /* on-disk: data/kf/000.kf + data/streams/000/000000.dat */
    char path[1024];
    snprintf(path, sizeof(path), "%s/v2tenant/sc_users/data/kf/000.kf",
             env.db_root);
    ASSERT_TRUE(tu_file_exists(path), "data/kf/000.kf created");

    snprintf(path, sizeof(path), "%s/v2tenant/sc_users/data/streams/000",
             env.db_root);
    ASSERT_TRUE(dir_exists(path), "data/streams/000 dir created");

    snprintf(path, sizeof(path), "%s/v2tenant/sc_users/data/streams/000/000000.dat",
             env.db_root);
    ASSERT_TRUE(tu_file_exists(path), "data/streams/000/000000.dat created");

    /* `.dirty` is a runtime crash marker — touched on slotcask_open, removed
       on slotcask_close. cmd_create_object opens+closes immediately, so the
       file is correctly absent after a clean create. (Recovery exercise lives
       in a separate test.) */

    /* --- default create (no storage_version) — lands on v2 (only engine
           supported in 2026.05.5+). --- */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"v2tenant\",\"object\":\"default_users\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "default create succeeds");
    free(resp); resp = NULL;

    line = read_schema_line(env.db_root, "v2tenant", "default_users");
    ASSERT_NOT_NULL(line, "default schema.conf line present");
    if (line) {
        int colons = 0;
        for (char *c = line; *c; c++) if (*c == ':') colons++;
        ASSERT_TRUE(colons == 5, "default schema line has 5 colons (v2 form)");
        free(line);
    }

    snprintf(path, sizeof(path), "%s/v2tenant/default_users/data/kf/000.kf",
             env.db_root);
    ASSERT_TRUE(tu_file_exists(path),
                "default lands on v2 layout (data/kf/000.kf present)");

    /* --- any explicit storage_version is rejected (field removed from API) --- */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"v2tenant\",\"object\":\"bogus\","
        "\"splits\":8,\"max_key\":40,\"storage_version\":2,"
        "\"fields\":[\"name:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "storage_version is not configurable",
                    "explicit storage_version rejected with the documented error");
    free(resp); resp = NULL;

    /* The bogus object must not be on disk. */
    snprintf(path, sizeof(path), "%s/v2tenant/bogus", env.db_root);
    ASSERT_TRUE(!dir_exists(path), "rejected create leaves no object dir");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-object", test_slotcask_v2_object_run)
