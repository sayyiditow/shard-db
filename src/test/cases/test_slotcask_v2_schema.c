/* test_slotcask_v2_schema.c — Phase-5 E2E for schema mutations on v2.
 *
 * Covers:
 *   - vacuum (no-arg) on v2 → no-op cleaned=0 (snake-game pool already
 *     reuses tombstoned slots, no Zone A flag-2 sweep)
 *   - add-field with backfill → existing records keep old fields, new
 *     field defaults to zero-init in encoded payload
 *   - remove-field → field tombstoned in fields.conf, indexes for that
 *     field dropped, records still readable
 *   - rename-field → fields.conf renamed, indexes follow, data unchanged
 *   - vacuum --compact → drops tombstoned fields physically
 *   - vacuum --splits → resplits the keyfile shard count
 *   - truncate → live records gone, schema preserved
 *   - drop-object → object dir gone
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

static int sch_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int sch_parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n' || *resp == '\r') resp++;
    return atoi(resp);
}

static int sch_count_total(TestClient *tc, const char *obj) {
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"sch2\",\"object\":\"%s\"}", obj);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = sch_parse_count(resp);
    free(resp);
    return n;
}

static int test_slotcask_v2_schema_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"sch2\"}", &resp);
    free(resp); resp = NULL;

    /* ===== no-arg vacuum on v2 → no-op ===== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sch2\",\"object\":\"vac\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"sch2\",\"object\":\"vac\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\",\"age\":%d}}", i, i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"sch2\",\"object\":\"vac\",\"key\":\"k1\"}",
                    &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"sch2\",\"object\":\"vac\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"vacuumed\"", "v2 plain vacuum returns status");
    ASSERT_CONTAINS(resp, "\"cleaned\":0", "v2 plain vacuum is a no-op (snake-game pool)");
    free(resp); resp = NULL;

    /* ===== add-field with backfill ===== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"],\"indexes\":[\"age\"]}", &resp);
    free(resp); resp = NULL;

    for (int i = 1; i <= 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"sch2\",\"object\":\"users\","
            "\"key\":\"u%d\",\"value\":{\"name\":\"n%d\",\"age\":%d}}",
            i, i, 20 + i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(sch_count_total(tc, "users"), 4, "users seeded with 4");

    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"fields\":[\"score:int\",\"city:varchar:16\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field rebuilt v2");
    ASSERT_CONTAINS(resp, "\"live\":4", "add-field preserved live=4");
    free(resp); resp = NULL;

    /* All 4 records readable, with new fields zero-initialized. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n2\"", "u2 name preserved");
    ASSERT_CONTAINS(resp, "\"age\":22", "u2 age preserved");
    ASSERT_TRUE(strstr(resp, "\"score\":0") != NULL, "u2 score zero-initialized");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(sch_count_total(tc, "users"), 4, "count still 4 post add-field");

    /* Indexed query against pre-existing field still works. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"sch2\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"22\"}]}", &resp);
    ASSERT_EQ_INT(sch_parse_count(resp), 3, "indexed age>=22 = 3 post add-field");
    free(resp); resp = NULL;

    /* New field can be written via update + read back. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u1\","
        "\"value\":{\"score\":99,\"city\":\"NYC\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"score\":99", "u1 score updated to 99");
    ASSERT_CONTAINS(resp, "\"city\":\"NYC\"", "u1 city updated");
    free(resp); resp = NULL;

    /* ===== remove-field (metadata-only, drops affected indexes) ===== */
    tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"fields\":[\"city\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"removed\"", "remove-field on v2 status");
    free(resp); resp = NULL;

    /* Records still readable; removed field absent from output. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n1\"", "u1 name still readable post remove-field");
    ASSERT_CONTAINS(resp, "\"score\":99", "u1 score still readable");
    ASSERT_TRUE(strstr(resp, "\"city\":") == NULL, "u1 city no longer in output");
    free(resp); resp = NULL;

    /* ===== rename-field (metadata-only) ===== */
    tc_request(tc,
        "{\"mode\":\"rename-field\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"old\":\"score\",\"new\":\"points\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"renamed\"", "rename-field on v2 status");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"points\":99", "u1 points (renamed from score) readable");
    ASSERT_TRUE(strstr(resp, "\"score\":") == NULL, "u1 score gone (renamed away)");
    free(resp); resp = NULL;

    /* ===== vacuum --compact: drop the tombstoned `city` from fields.conf ===== */
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"compact\":\"true\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "vacuum --compact rebuilt v2");
    ASSERT_CONTAINS(resp, "\"compact\":true", "compact=true reflected in response");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(sch_count_total(tc, "users"), 4, "count preserved across compact");
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"points\":99", "u1 points preserved post-compact");
    free(resp); resp = NULL;

    /* ===== vacuum --splits: change shard count, indexes rebuilt ===== */
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"splits\":16}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "vacuum --splits rebuilt v2");
    ASSERT_CONTAINS(resp, "\"splits\":16", "splits=16 reflected");
    free(resp); resp = NULL;

    /* Records still findable post resplit, indexed criteria still works. */
    ASSERT_EQ_INT(sch_count_total(tc, "users"), 4, "count = 4 post resplit");
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"sch2\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"22\"}]}", &resp);
    ASSERT_EQ_INT(sch_parse_count(resp), 3, "indexed age>=22 = 3 post resplit");
    free(resp); resp = NULL;

    /* schema.conf preserves storage_version=2 across the resplit. */
    {
        char schema_path[PATH_MAX];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", env.db_root);
        FILE *f = fopen(schema_path, "r");
        ASSERT_NOT_NULL(f, "schema.conf open");
        if (f) {
            char line[512];
            int saw_v2 = 0;
            while (fgets(line, sizeof(line), f)) {
                if (strncmp(line, "sch2:users:", 11) == 0) {
                    if (strstr(line, ":2:")) saw_v2 = 1;
                    break;
                }
            }
            fclose(f);
            ASSERT_TRUE(saw_v2, "schema.conf preserves storage_version=2 after resplit");
        }
    }

    /* ===== truncate: data wiped, schema preserved ===== */
    tc_request(tc,
        "{\"mode\":\"truncate\",\"dir\":\"sch2\",\"object\":\"users\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"truncated\"", "truncate v2 status");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(sch_count_total(tc, "users"), 0, "count = 0 post-truncate");

    /* New inserts work post-truncate (slotcask reopens fresh). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"sch2\",\"object\":\"users\","
        "\"key\":\"fresh\",\"value\":{\"name\":\"f\",\"age\":1,\"points\":10}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"sch2\",\"object\":\"users\",\"key\":\"fresh\"}",
                    &resp);
    ASSERT_CONTAINS(resp, "\"points\":10", "post-truncate insert + get works");
    free(resp); resp = NULL;

    /* ===== drop-object: dir gone ===== */
    tc_request(tc,
        "{\"mode\":\"drop-object\",\"dir\":\"sch2\",\"object\":\"users\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"dropped\"", "drop-object v2 status");
    free(resp); resp = NULL;

    {
        char obj_path[PATH_MAX];
        snprintf(obj_path, sizeof(obj_path), "%s/sch2/users", env.db_root);
        ASSERT_TRUE(!sch_file_exists(obj_path), "object dir removed post drop-object");
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-schema", test_slotcask_v2_schema_run)
