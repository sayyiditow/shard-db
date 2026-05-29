/* src/test/cases/test_planner_trigram_selectivity.c
 *
 * Regression test: the planner must NOT pick a broad pure-bitmap
 * FP_INTERSECT over a far-more-selective trigram FP_PRIMARY_LEAF.
 *
 * Live symptom (hn/stories, 2026-05-26): count of
 *   (dead=false AND deleted=false AND type IN [...] AND title icontains X)
 * took ~20s. The planner intersected the 3 bitmaps (~4.3M rows) and
 * post-filtered the 527-match trigram across all of them, instead of
 * leading with the trigram (~97ms). Root cause in choose_primary_source:
 * find_intersect_leaves fires on >=2 intersect-eligible (bitmap) leaves
 * and the non-intersect-eligible trigram leaf is dropped + demoted to a
 * post-filter, before the selectivity scoring runs.
 *
 * White-box: a result-only test can't catch this (the count is correct
 * either way), so we assert the chosen plan KIND via the
 * plan_filter_kind_for_test hook (TEST_BUILD only).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#ifdef TEST_BUILD
extern const char *plan_filter_kind_for_test(const char *db_root, const char *object,
                                             const char *criteria_json,
                                             const char *order_by, int fetching,
                                             char *out_field, size_t fsz,
                                             char *out_order, size_t osz,
                                             int *out_total_cheap);
extern char g_db_root[];
#endif

static int test_planner_trigram_over_bitmaps(void) {
#ifdef TEST_BUILD
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* title trigram + two bool bitmaps — mirrors hn/stories' shape
     * (title:trigram, dead:bitmap, deleted:bitmap) that triggered the 20s count. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"posts\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"title:varchar:64\",\"dead:bool\",\"deleted:bool\"],"
        "\"indexes\":[\"title:trigram\",\"dead:bitmap\",\"deleted:bitmap\"]}", &resp);
    free(resp); resp = NULL;

    /* Insert enough records so that selectivity_budget(N) > 0 and the bitmap
     * fields (dead/deleted, mostly false) are broad relative to the budget,
     * while the trigram "hawking" matches only a few. With N=100, budget=12;
     * dead=false matches ~97 records (broad), trigram matches 2 (selective).
     * This ensures plan_filter's selectivity scoring correctly prefers the
     * trigram primary over a pure-bitmap intersect. */
    {
        char bulk[65536]; size_t pos = 0;
        SB_APPEND(bulk, pos, sizeof(bulk),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"posts\",\"records\":[");
        /* 2 hawking records */
        SB_APPEND(bulk, pos, sizeof(bulk),
            "{\"key\":\"1\",\"value\":{\"title\":\"stephen hawking dies\",\"dead\":false,\"deleted\":false}},");
        SB_APPEND(bulk, pos, sizeof(bulk),
            "{\"key\":\"2\",\"value\":{\"title\":\"hawking radiation explained\",\"dead\":false,\"deleted\":false}}");
        /* 98 unrelated records to make bitmap fields broad */
        for (int i = 3; i <= 100; i++) {
            SB_APPEND(bulk, pos, sizeof(bulk),
                ",{\"key\":\"%d\",\"value\":{\"title\":\"generic news story number %d\","
                "\"dead\":false,\"deleted\":false}}", i, i);
        }
        /* 3 dead records to ensure dead bitmap exists and has some entries */
        SB_APPEND(bulk, pos, sizeof(bulk),
            ",{\"key\":\"101\",\"value\":{\"title\":\"old post a\",\"dead\":true,\"deleted\":false}}");
        SB_APPEND(bulk, pos, sizeof(bulk),
            ",{\"key\":\"102\",\"value\":{\"title\":\"old post b\",\"dead\":true,\"deleted\":true}}");
        SB_APPEND(bulk, pos, sizeof(bulk),
            ",{\"key\":\"103\",\"value\":{\"title\":\"old post c\",\"dead\":true,\"deleted\":true}}");
        SB_APPEND(bulk, pos, sizeof(bulk), "]}");
        tc_request(tc, bulk, &resp); free(resp); resp = NULL;
    }
    tc_close(tc);

    /* plan_filter_kind_for_test takes db_root + "dir/object" combined.
     * It sets g_db_root internally; no need to set it here. */
    char field[256];
    const char *kind;

    /* THE BUG: trigram + 2 bitmaps must lead with the trigram (FP_PRIMARY_LEAF),
     * not collapse to a pure-bitmap FP_INTERSECT that post-filters it.
     * Use fetching=1 (find path): the original regression was a slow FIND/count
     * query; the plan_filter "find" path picks the most-selective single seed
     * rather than intersecting multiple selective leaves. */
    kind = plan_filter_kind_for_test(env.db_root, "default/posts",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"hawking\"},"
        "{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"},"
        "{\"field\":\"deleted\",\"op\":\"eq\",\"value\":\"false\"}]",
        NULL /*order_by*/, 1 /*fetching*/,
        field, sizeof(field), NULL, 0, NULL);
    ASSERT_EQ_STR(kind, "leaf", "trigram + 2 bitmaps -> FP_PRIMARY_LEAF (not bitmap intersect)");
    ASSERT_EQ_STR(field, "title", "FP_PRIMARY_LEAF drives on the selective trigram field");

    /* Regression guard: pure-bitmap AND (no selective leaf) must STAY on the
     * popcount FP_INTERSECT — the landing-page fast path. */
    kind = plan_filter_kind_for_test(env.db_root, "default/posts",
        "[{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"},"
        "{\"field\":\"deleted\",\"op\":\"eq\",\"value\":\"false\"}]",
        NULL /*order_by*/, 1 /*fetching*/,
        field, sizeof(field), NULL, 0, NULL);
    ASSERT_EQ_STR(kind, "intersect", "pure-bitmap AND stays FP_INTERSECT (landing page)");

    /* trigram + 1 bitmap already worked (intersect needs >=2 leaves) — lock it. */
    kind = plan_filter_kind_for_test(env.db_root, "default/posts",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"hawking\"},"
        "{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"}]",
        NULL /*order_by*/, 1 /*fetching*/,
        field, sizeof(field), NULL, 0, NULL);
    ASSERT_EQ_STR(kind, "leaf", "trigram + 1 bitmap -> FP_PRIMARY_LEAF");

    test_env_stop(&env);
#endif
    return 0;
}

TEST_REGISTER("test-planner-trigram-over-bitmaps", test_planner_trigram_over_bitmaps)
