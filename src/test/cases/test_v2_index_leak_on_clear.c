/* src/test/cases/test_v2_index_leak_on_clear.c
 *
 * Regression guard for the v2 per-field index-update dispatcher in
 * v2_insert_pre_commit (storage.c). The dispatcher's earlier shape
 * marked `changed = 1` only for these cases:
 *
 *   - have_new && !have_old   (insert into index)
 *   - have_new && have_old && bytes_differ   (update in index)
 *
 * The fourth case `!have_new && have_old` (the user updated a record
 * and the indexed field went from "indexable" to "empty") fell through
 * with changed=0, so the OLD btree entry pointing at this record was
 * never removed. A subsequent count(field=old_value) would still hit
 * the stale entry and incorrectly count the record.
 *
 * This test inserts a record with an indexed varchar value, asserts
 * the index sees it, then updates the record to clear that value,
 * and asserts the index no longer sees it.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>

static int test_v2_index_leak_on_clear_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"city:varchar:16\"],"
        "\"indexes\":[\"city\"]}", &resp);
    free(resp); resp = NULL;

    /* Insert with city=NYC — this writes a btree entry city=NYC → u1. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"users\",\"key\":\"u1\","
        "\"value\":{\"name\":\"alice\",\"city\":\"NYC\"}}", &resp);
    free(resp); resp = NULL;

    /* Pre-clear: indexed count for the value must be 1. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}]}",
        &resp);
    ASSERT_EQ_INT(resp ? atoi(resp) : -1, 1,
                  "pre-clear: count(city=NYC) == 1");
    free(resp); resp = NULL;

    /* Update u1 to clear the city field. The new typed record has
       city="" (varchar empty); build_index_key_from_json on "" returns
       have_new=0. Pre-fix the dispatcher would skip this entirely and
       the btree entry for "NYC" would survive. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"d\",\"object\":\"users\",\"key\":\"u1\","
        "\"value\":{\"name\":\"alice\",\"city\":\"\"}}", &resp);
    free(resp); resp = NULL;

    /* Post-clear: the stale btree entry must be gone. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"users\","
        "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}]}",
        &resp);
    ASSERT_EQ_INT(resp ? atoi(resp) : -1, 0,
                  "post-clear: count(city=NYC) == 0 (no stale btree entry)");
    free(resp); resp = NULL;

    /* Verify the record itself still exists with the cleared field. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"d\",\"object\":\"users\",\"key\":\"u1\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "record still readable");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-v2-index-leak-on-clear", test_v2_index_leak_on_clear_run)
