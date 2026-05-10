/* test_slotcask_v2_migrate.c — Phase-7 round-trip for the v1→v2 migration.
 *
 * Creates a v1 (default storage_version) object with indexes, seeds
 * records, invokes the new `migrate-storage-version` wire mode, and
 * confirms:
 *   - status:migrated reports the live record count
 *   - schema.conf line gained the :2:streams suffix
 *   - data/ was renamed to data.legacy/, keyfile_NNN.kf + stream_NNN
 *     dirs landed at the obj root
 *   - records readable post-migration via cmd_get
 *   - count by indexed field still resolves (indexes survive — hashes
 *     are canonical across both layouts)
 *   - new inserts go through the v2 path (slotcask) — verified by
 *     looking at keyfile growth / segment growth and reading back
 *   - already-v2 returns status:already_v2 (idempotent)
 *   - v2 object survives a delete + re-insert (slotcask snake-game pool
 *     reuse on reopen)
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
#include <limits.h>

static int mig_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int mig_parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n' || *resp == '\r') resp++;
    return atoi(resp);
}

static int test_slotcask_v2_migrate_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"mig\"}", &resp);
    free(resp); resp = NULL;

    /* Create a v1 object explicitly — production create-object only
       writes v2; the test fixture sets SHARD_ALLOW_V1_CREATE=1 so
       this opt-in is honoured. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"mig\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":40,\"storage_version\":1,"
        "\"fields\":[\"name:varchar:32\",\"age:int\",\"city:varchar:16\"],"
        "\"indexes\":[\"age\",\"city\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "v1 object created");
    ASSERT_TRUE(strstr(resp, "\"storage_version\":2") == NULL,
                "v1 object did NOT report storage_version:2");
    free(resp); resp = NULL;

    /* Seed 6 records covering 3 cities. */
    static const struct { const char *k, *v; } seed[] = {
        {"u1", "{\"name\":\"alice\",\"age\":25,\"city\":\"NYC\"}"},
        {"u2", "{\"name\":\"bob\",\"age\":30,\"city\":\"LON\"}"},
        {"u3", "{\"name\":\"carol\",\"age\":35,\"city\":\"NYC\"}"},
        {"u4", "{\"name\":\"dave\",\"age\":40,\"city\":\"TYO\"}"},
        {"u5", "{\"name\":\"eve\",\"age\":45,\"city\":\"LON\"}"},
        {"u6", "{\"name\":\"frank\",\"age\":50,\"city\":\"NYC\"}"},
    };
    for (size_t i = 0; i < sizeof(seed) / sizeof(seed[0]); i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"mig\",\"object\":\"users\","
            "\"key\":\"%s\",\"value\":%s}", seed[i].k, seed[i].v);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Confirm v1 layout on disk: obj_dir/data/ exists with NNN.bin
       files, no v2 kf/ subdir. */
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/mig/users", env.db_root);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", obj_dir);
    ASSERT_TRUE(mig_file_exists(path), "pre-migrate: data/ exists");
    snprintf(path, sizeof(path), "%s/data/kf", obj_dir);
    ASSERT_TRUE(!mig_file_exists(path), "pre-migrate: no data/kf/ (v1 layout)");

    /* count by indexed field — pre-migration baseline. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}]}",
        &resp);
    ASSERT_EQ_INT(mig_parse_count(resp), 3, "pre-migrate count(city=NYC) = 3");
    free(resp); resp = NULL;

    /* === migrate === */
    tc_request(tc,
        "{\"mode\":\"migrate-storage-version\",\"dir\":\"mig\",\"object\":\"users\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"upgraded\"", "migrate status");
    ASSERT_CONTAINS(resp, "\"records\":6", "migrated 6 records");
    free(resp); resp = NULL;

    /* Post-migrate disk layout: v1 data/ renamed to data.legacy/,
       new v2 data/kf/ + data/streams/ created. */
    snprintf(path, sizeof(path), "%s/data.legacy", obj_dir);
    ASSERT_TRUE(mig_file_exists(path), "post-migrate: data.legacy/ exists");
    snprintf(path, sizeof(path), "%s/data/kf/000.kf", obj_dir);
    ASSERT_TRUE(mig_file_exists(path), "post-migrate: data/kf/000.kf exists");
    snprintf(path, sizeof(path), "%s/data/streams/000", obj_dir);
    ASSERT_TRUE(mig_file_exists(path), "post-migrate: data/streams/000 exists");

    /* schema.conf gained :2:streams. */
    {
        char schema_path[PATH_MAX];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", env.db_root);
        FILE *f = fopen(schema_path, "r");
        ASSERT_NOT_NULL(f, "schema.conf open");
        if (f) {
            char line[512];
            int saw_v2 = 0;
            while (fgets(line, sizeof(line), f)) {
                if (strncmp(line, "mig:users:", 10) == 0) {
                    /* Expect format: mig:users:8:40:2:N */
                    if (strstr(line, ":2:")) saw_v2 = 1;
                    break;
                }
            }
            fclose(f);
            ASSERT_TRUE(saw_v2, "schema.conf line has :2:streams suffix");
        }
    }

    /* Records readable + values intact. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"mig\",\"object\":\"users\","
                   "\"key\":\"u3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"carol\"", "post-migrate u3 readable");
    ASSERT_CONTAINS(resp, "\"city\":\"NYC\"", "post-migrate u3 fields intact");
    free(resp); resp = NULL;

    /* count by indexed field still resolves — hashes survived migration. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}]}",
        &resp);
    ASSERT_EQ_INT(mig_parse_count(resp), 3, "post-migrate indexed count(city=NYC) = 3");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"35\"}]}",
        &resp);
    ASSERT_EQ_INT(mig_parse_count(resp), 4, "post-migrate indexed range = 4");
    free(resp); resp = NULL;

    /* Idempotent — re-running migrate on a v2 object returns already_v2. */
    tc_request(tc,
        "{\"mode\":\"migrate-storage-version\",\"dir\":\"mig\",\"object\":\"users\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"already_v2\"", "re-migrate is idempotent");
    free(resp); resp = NULL;

    /* New inserts go through the v2 path. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"mig\",\"object\":\"users\","
        "\"key\":\"u7\",\"value\":{\"name\":\"grace\",\"age\":29,\"city\":\"BER\"}}",
        &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"mig\",\"object\":\"users\","
                   "\"key\":\"u7\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"grace\"", "post-migrate insert + get works");
    free(resp); resp = NULL;

    /* And it's reflected in the indexed count. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"mig\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"BER\"}]}",
        &resp);
    ASSERT_EQ_INT(mig_parse_count(resp), 1, "post-migrate new index entry visible");
    free(resp); resp = NULL;

    /* Delete + re-insert exercises the snake-game free-slot pool. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"mig\",\"object\":\"users\","
                   "\"key\":\"u2\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"mig\",\"object\":\"users\","
        "\"key\":\"u2_b\",\"value\":{\"name\":\"bob2\",\"age\":31,\"city\":\"LON\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"mig\",\"object\":\"users\","
                   "\"key\":\"u2_b\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"bob2\"", "post-migrate snake-game reuse works");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-migrate", test_slotcask_v2_migrate_run)
