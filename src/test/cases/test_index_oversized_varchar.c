/* src/test/cases/test_index_oversized_varchar.c
 *
 * Best-effort index policy for oversized index keys (varchar content such
 * that the encoded index key exceeds BT_MAX_VAL_LEN=512):
 *   - bulk index build (btree_bulk_build_locked) SKIPS such entries instead
 *     of overflowing the prefix-compression buffer (Coverity CID 1699814);
 *   - deletes of such keys are no-ops (btree_idx_delete parity), so CRUD on
 *     records carrying oversized indexed values never fails;
 *   - composite index-key accumulation pre-checks scratch space before the
 *     write (index.c mf_append_field / query_bulk.c bulk worker), so
 *     composites whose concatenated parts exceed the 4 KB scratch buffer
 *     are skipped, not overflowed.
 *
 * Wire-level end-to-end validation: an object whose indexed varchar field
 * holds oversized values must service bulk-insert (upsert), update, and
 * bulk-delete without errors, with the index simply containing no entries.
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

static int test_index_oversized_varchar_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Indexed varchar:600 — content of 580 chars encodes to 582 bytes,
       over the 512-byte leaf key limit. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ovz_t\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"tag:varchar:600\"],"
        "\"indexes\":[\"tag\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create oversized-indexed object");
    free(resp); resp = NULL;

    /* Seed: 50 records, tag = 580 'x's (~30 KB request). */
    char *buf = malloc(512 * 1024);
    char *tagv = malloc(600);
    memset(tagv, 'x', 580); tagv[580] = '\0';
    size_t off = 0;
    off += (size_t)snprintf(buf + off, 512 * 1024 - off,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"records\":[");
    for (int i = 0; i < 50; i++) {
        off += (size_t)snprintf(buf + off, 512 * 1024 - off,
            "%s{\"key\":\"k%d\",\"value\":{\"tag\":\"%s\"}}",
            i ? "," : "", i, tagv);
    }
    off += (size_t)snprintf(buf + off, 512 * 1024 - off, "]}");
    tc_request(tc, buf, &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "bulk-insert oversized values succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\"}",
               &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 50, "record count after seed");
    free(resp); resp = NULL;

    /* Best-effort index: the oversized keys are skipped, so an indexed
       equality probe finds nothing even though the records exist. */
    char crit[4096];
    snprintf(crit, sizeof(crit),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\","
        "\"criteria\":{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"%s\"}}", tagv);
    tc_request(tc, crit, &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 0, "indexed eq finds no oversized entries");
    free(resp); resp = NULL;

    /* Upsert with a NEW oversized value (590 'y's): the old-entry index
       delete is a no-op (btree_idx_delete parity) and the new insert is
       skipped — the write must not report failure. */
    memset(tagv, 'y', 590); tagv[590] = '\0';
    off = 0;
    off += (size_t)snprintf(buf + off, 512 * 1024 - off,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"records\":[");
    for (int i = 0; i < 50; i++) {
        off += (size_t)snprintf(buf + off, 512 * 1024 - off,
            "%s{\"key\":\"k%d\",\"value\":{\"tag\":\"%s\"}}",
            i ? "," : "", i, tagv);
    }
    off += (size_t)snprintf(buf + off, 512 * 1024 - off, "]}");
    tc_request(tc, buf, &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                "upsert with oversized old+new values succeeds");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\"}",
               &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 50, "record count after upsert");
    free(resp); resp = NULL;

    /* Bulk-delete: each record carries an oversized old tag; the index
       deletes must be no-ops, and the deletes themselves must succeed. */
    off = 0;
    off += (size_t)snprintf(buf + off, 512 * 1024 - off,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"ovz_t\",\"keys\":[");
    for (int i = 0; i < 50; i++) {
        off += (size_t)snprintf(buf + off, 512 * 1024 - off,
            "%s\"k%d\"", i ? "," : "", i);
    }
    off += (size_t)snprintf(buf + off, 512 * 1024 - off, "]}");
    tc_request(tc, buf, &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "bulk-delete oversized records succeeds");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\"}",
               &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 0, "all records deleted");
    free(resp); resp = NULL;

    /* Composite-index scratch guard: two 3000-byte varchar parts
       concatenate to 6000 bytes, beyond the 4096-byte scratch buffer.
       add-index + rebuild must skip (not overflow); the object still
       serves reads. A 4500-byte simple key exercises the bulk-build
       skip through add-index's streaming build path. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ovz_big\","
        "\"splits\":8,\"max_key\":64,\"fields\":[\"big:varchar:5000\","
        "\"a:varchar:3000\",\"b:varchar:3000\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create big-field object");
    free(resp); resp = NULL;

    char *bigv = malloc(4600);
    char *abv = malloc(3100);
    memset(bigv, 'z', 4500); bigv[4500] = '\0';
    memset(abv, 'q', 3000); abv[3000] = '\0';
    off = 0;
    off += (size_t)snprintf(buf + off, 512 * 1024 - off,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_big\",\"records\":[");
    for (int i = 0; i < 20; i++) {
        off += (size_t)snprintf(buf + off, 512 * 1024 - off,
            "%s{\"key\":\"b%d\",\"value\":{\"big\":\"%s\",\"a\":\"%s\",\"b\":\"%s\"}}",
            i ? "," : "", i, bigv, abv, abv);
    }
    off += (size_t)snprintf(buf + off, 512 * 1024 - off, "]}");
    tc_request(tc, buf, &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "bulk-insert big-field records succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"ovz_big\","
        "\"field\":\"big\"}", &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "add-index big (oversized simple key) succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"ovz_big\","
        "\"field\":\"a+b\"}", &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "add-index a+b (oversized composite) succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"reindex\",\"dir\":\"default\",\"object\":\"ovz_big\"}", &resp);
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "reindex big-field object succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_big\"}",
               &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 20, "big-field record count after reindex");
    free(resp); resp = NULL;

    snprintf(crit, sizeof(crit),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_big\","
        "\"criteria\":{\"field\":\"big\",\"op\":\"eq\",\"value\":\"%s\"}}", bigv);
    tc_request(tc, crit, &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 0, "indexed eq finds no oversized big entries");
    free(resp); resp = NULL;

    free(buf);
    free(tagv);
    free(bigv);
    free(abv);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-index-oversized-varchar", test_index_oversized_varchar_run)