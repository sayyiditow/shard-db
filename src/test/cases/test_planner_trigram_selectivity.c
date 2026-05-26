/* src/test/cases/test_planner_trigram_selectivity.c
 *
 * Regression test: the planner must NOT pick a broad pure-bitmap
 * PRIMARY_INTERSECT over a far-more-selective trigram PRIMARY_LEAF.
 *
 * Live symptom (hn/stories, 2026-05-26): count of
 *   (dead=false AND deleted=false AND type IN [...] AND title icontains X)
 * took ~20s. The planner intersected the 3 bitmaps (~4.3M rows) and
 * post-filtered the 527-match trigram across all of them, instead of
 * leading with the trigram (~97ms). Root cause in choose_primary_source:
 * find_intersect_leaves fires on >=2 intersect-eligible (bitmap) leaves
 * and the non-intersect-eligible trigram leaf is dropped + demoted to a
 * post-filter, before find_primary_leaf's selectivity scoring runs.
 *
 * White-box: a result-only test can't catch this (the count is correct
 * either way), so we assert the chosen plan KIND via the
 * planner_primary_kind_for_test hook (TEST_BUILD only).
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
extern const char *planner_primary_kind_for_test(const char *db_root, const char *object,
                                                 const char *criteria_json,
                                                 char *out_field, size_t out_sz);
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

    /* A few records so the index files exist on disk for the planner to read. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"posts\",\"records\":["
        "{\"key\":\"1\",\"value\":{\"title\":\"stephen hawking dies\",\"dead\":false,\"deleted\":false}},"
        "{\"key\":\"2\",\"value\":{\"title\":\"a quiet day in tech\",\"dead\":false,\"deleted\":false}},"
        "{\"key\":\"3\",\"value\":{\"title\":\"hawking radiation explained\",\"dead\":true,\"deleted\":false}}"
        "]}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    /* The planner runs in THIS process: point g_db_root at the daemon's root
     * and pass the effective tenant root + bare object (same convention the
     * daemon uses; index config is read from disk). */
    snprintf(g_db_root, PATH_MAX, "%s", env.db_root);
    char eff_root[512];
    snprintf(eff_root, sizeof(eff_root), "%s/default", env.db_root);

    char field[256];
    const char *kind;

    /* THE BUG: trigram + 2 bitmaps must lead with the trigram (PRIMARY_LEAF),
     * not collapse to a pure-bitmap PRIMARY_INTERSECT that post-filters it. */
    kind = planner_primary_kind_for_test(eff_root, "posts",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"hawking\"},"
        "{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"},"
        "{\"field\":\"deleted\",\"op\":\"eq\",\"value\":\"false\"}]", field, sizeof(field));
    ASSERT_EQ_STR(kind, "leaf", "trigram + 2 bitmaps -> PRIMARY_LEAF (not bitmap intersect)");
    ASSERT_EQ_STR(field, "title", "PRIMARY_LEAF drives on the selective trigram field");

    /* Regression guard: pure-bitmap AND (no selective leaf) must STAY on the
     * popcount PRIMARY_INTERSECT — the landing-page fast path. */
    kind = planner_primary_kind_for_test(eff_root, "posts",
        "[{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"},"
        "{\"field\":\"deleted\",\"op\":\"eq\",\"value\":\"false\"}]", field, sizeof(field));
    ASSERT_EQ_STR(kind, "intersect", "pure-bitmap AND stays PRIMARY_INTERSECT (landing page)");

    /* trigram + 1 bitmap already worked (intersect needs >=2 leaves) — lock it. */
    kind = planner_primary_kind_for_test(eff_root, "posts",
        "[{\"field\":\"title\",\"op\":\"icontains\",\"value\":\"hawking\"},"
        "{\"field\":\"dead\",\"op\":\"eq\",\"value\":\"false\"}]", field, sizeof(field));
    ASSERT_EQ_STR(kind, "leaf", "trigram + 1 bitmap -> PRIMARY_LEAF");

    test_env_stop(&env);
#endif
    return 0;
}

TEST_REGISTER("test-planner-trigram-over-bitmaps", test_planner_trigram_over_bitmaps)
