/* src/test/cases/test_small_prefilter_orderby.c
 *
 * Locks the small-prefilter ordered-find shortcut introduced after
 * bench-cache-pollution surfaced a pathological case: selective
 * 1-match criterion + order_by on a different indexed field, where
 * the matching record's order-key happens to land at the tail of
 * the desc walk. Without the shortcut the executor walks the whole
 * order_by btree looking for the 1 hash16 it cares about — O(N)
 * for what should be O(1). Real symptom: 1.7 s vs 1 ms.
 *
 * The test seeds a small object (50 rows is enough; the shortcut
 * threshold is K ≤ 1000 and the bug shape is order-agnostic), then
 * exercises every combination of:
 *   - selective eq criterion → 1-match prefilter
 *   - order_by ASC vs DESC
 *   - limit smaller than match-count (so cursor_find_cb's
 *     printed >= limit short-circuit gets exercised)
 *   - offset application
 *   - empty result (selective criterion that matches nothing)
 *
 * Correctness is what we lock — perf is a separate bench concern.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static int test_small_prefilter_orderby_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Schema: indexed username + indexed age, varchar:32 unique
       username per row, age cycles 18..67 deterministically so we
       can predict desc-order without parsing. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"spo\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"username:varchar:32\",\"age:int\",\"bio:varchar:64\"],"
        "\"indexes\":[\"username\",\"age\"]}", &resp);
    free(resp); resp = NULL;

    /* 50 records. user_NN with age = 18 + (NN % 50). user_0 has age=18,
       user_49 has age=17? wait: 18 + 49 = 67. So user_49 has age=67. */
    for (int i = 0; i < 50; i++) {
        char body[256];
        snprintf(body, sizeof(body),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"spo\","
            "\"key\":\"k%d\",\"value\":{\"username\":\"user_%d\","
            "\"age\":%d,\"bio\":\"bio for user_%d\"}}",
            i, i, 18 + i, i);
        tc_request(tc, body, &resp);
        free(resp); resp = NULL;
    }

    /* ===== single-match selective criterion + order_by desc =====
       user_5 has age=23. find by username='user_5' order_by age desc
       limit 10 should return exactly the user_5 row. Without the
       shortcut the executor walks age desc from 67 → 23 (45 entries)
       to find the single hash16 — at small scale it's fast either
       way, but the result must be correct. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"user_5\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_NOT_NULL(resp, "find user_5 order_by age desc");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k5\"") != NULL,
                "find user_5 returns k5");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k4\"") == NULL,
                "find user_5 doesn't return k4");
    free(resp); resp = NULL;

    /* ===== ASC order, same shape =====
       user_5 is also at age=23. Same single row expected. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"user_5\"}],"
        "\"order_by\":\"age\",\"order\":\"asc\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k5\"") != NULL,
                "find user_5 asc returns k5");
    free(resp); resp = NULL;

    /* ===== empty match — selective criterion that hits nothing =====
       Different from the existing empty-keyset short-circuit
       (prefilter_ks built with 0 entries via PRIMARY_NONE) — here
       prefilter has 0 hits because the value doesn't exist. Should
       emit `[]` correctly. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"nonexistent\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && (strcmp(resp, "[]\n") == 0 || strcmp(resp, "[]") == 0),
                "find nonexistent returns []");
    free(resp); resp = NULL;

    /* ===== range criterion → multi-match prefilter, still small =====
       age between 30 and 35 → 6 matches (user_12..user_17). order_by
       age desc limit 3 should return user_17, user_16, user_15.
       Exercises K=6 inside the shortcut (still ≤ 1000). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\","
                       "\"value\":\"30\",\"value2\":\"35\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k17\"") != NULL, "k17 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k16\"") != NULL, "k16 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k15\"") != NULL, "k15 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k14\"") == NULL, "k14 NOT present (under limit)");
    free(resp); resp = NULL;

    /* ===== offset within shortcut path =====
       Same 6 matches, but offset=2 limit=2 → user_15, user_14. Verifies
       cursor_find_cb's skip_remaining decrement still works when the
       sorted entries are emitted via the shortcut. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\","
                       "\"value\":\"30\",\"value2\":\"35\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"offset\":2,\"limit\":2}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k15\"") != NULL, "offset=2: k15 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k14\"") != NULL, "offset=2: k14 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k17\"") == NULL, "offset=2: k17 skipped");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k16\"") == NULL, "offset=2: k16 skipped");
    free(resp); resp = NULL;

    /* ===== projection through the shortcut =====
       Confirm projection works (proj path inside cursor_find_cb is
       shared). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"user_10\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,"
        "\"fields\":[\"username\",\"age\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"username\":\"user_10\"") != NULL,
                "projection includes username");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"age\":\"28\"") != NULL,
                "projection includes age=28");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"bio\":") == NULL,
                "projection excludes bio");
    free(resp); resp = NULL;

    /* ===== dict format through the shortcut ===== */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"spo\","
        "\"criteria\":[{\"field\":\"username\",\"op\":\"eq\",\"value\":\"user_5\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,"
        "\"format\":\"dict\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"k5\":") != NULL,
                "dict format key:value shape");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-small-prefilter-orderby", test_small_prefilter_orderby_run)
