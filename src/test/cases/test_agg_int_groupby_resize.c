/* src/test/cases/test_agg_int_groupby_resize.c
 *
 * Regression test for the agg_ht_resize integer-key rehash bug: after the
 * aggregate hash table grows past AGG_HT_INIT (256 slots), agg_ht_resize
 * rehashed every bucket with agg_hash() (the string/djb2 hash) regardless
 * of ctx->use_int_keys, while agg_find_or_create's lookup for integer
 * group fields hashes with agg_hash_int(). After a resize, an int-keyed
 * bucket lands under the wrong slot for future lookups, so a later
 * record for the same group key is silently treated as "not found" and
 * gets a brand-new duplicate bucket instead of accumulating into the
 * existing one — splitting one group's true count across multiple rows.
 *
 * Seeds 600 distinct int group keys (well past the 256-slot initial
 * capacity, forcing two resizes: 256->512->1024, mirroring the
 * production repro that produced 3 duplicate rows for one key) with
 * exactly 3 records apiece on a full-scan (PRIMARY_NONE, unindexed)
 * group_by field, then asserts the aggregate response contains exactly
 * 600 distinct group rows — no more. Any duplicate bucket for a group
 * key pushes the row count above 600.
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

#define N_GROUPS 600
#define PER_GROUP 3
#define BUF_SIZE (N_GROUPS * PER_GROUP * 64 + 4096)

static int test_agg_int_groupby_resize_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"fields\":[\"grp:int\"],"
        "\"indexes\":[],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* 600 distinct groups (grp = 1..600), 3 records each = 1800 records.
       Values start at 1 (not 0) to sidestep the unrelated CSV zero-cell
       rendering quirk documented in test_agg_int_groupby_multi.c. */
    char *bulk_insert = malloc(BUF_SIZE);
    ASSERT_NOT_NULL(bulk_insert, "malloc bulk_insert buffer");
    int offset = 0;
    offset += snprintf(bulk_insert + offset, BUF_SIZE - offset,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"records\":[");
    int first = 1;
    for (int g = 1; g <= N_GROUPS; g++) {
        for (int r = 0; r < PER_GROUP; r++) {
            int rem = BUF_SIZE - offset;
            if (!first && rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem, ",");
            first = 0;
            rem = BUF_SIZE - offset;
            if (rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem,
                "{\"key\":\"k%d_%d\",\"value\":{\"grp\":%d}}", g, r, g);
        }
    }
    { int rem = BUF_SIZE - offset; if (rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem, "]}"); }

    tc_request(tc, bulk_insert, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":1800", "bulk-insert: 1800 inserted");
    free(resp); resp = NULL;
    free(bulk_insert);

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"group_by\":[\"grp\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":2000}", &resp);
    ASSERT_TRUE(resp != NULL && SAFE_STRSTR(resp, "\"error\"") == NULL,
        "aggregate did not error");

    int n_buckets = 0;
    for (const char *p = resp; (p = strchr(p, '{')) != NULL; p++) n_buckets++;
    ASSERT_EQ_INT(n_buckets, N_GROUPS,
        "aggregate returns exactly 600 distinct groups (no duplicate buckets "
        "from the int-key hash table resize bug)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-int-groupby-resize", test_agg_int_groupby_resize_run)
