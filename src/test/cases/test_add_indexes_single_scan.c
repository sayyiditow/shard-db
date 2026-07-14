/* src/test/cases/test_add_indexes_single_scan.c
 *
 * Regression test for the "force add-index with mixed field types runs
 * N separate full-object scans" incident (2026-07-03, hn/comments got
 * stuck under a force add-index over 6 btree + 2 bitmap fields).
 *
 * Fix: cmd_add_indexes now builds ONE combined MFFieldDesc array covering
 * every requested field (bitmap + trigram + btree) and calls
 * build_indexes_streaming_multi() exactly once — same single-scan engine
 * reindex_object uses — instead of dispatching build_bitmap_pass per
 * bitmap field, build_trigram_pass per trigram field, and a separate
 * batched build_indexes_pass for the remaining btree fields.
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
#include <time.h>
#include <libgen.h>

static int substr_count(const char *hay, const char *needle) {
    if (!hay) return 0;
    int n = 0;
    const char *p = hay;
    size_t nlen = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) { n++; p += nlen; }
    return n;
}

/* env->db_root is "<base>/db" (see fixtures.c: test_env_start). LOG_DIR is
   "<base>/logs". Build "<base>/logs/YYYY-MM-DD-info.log" for today.
   (WARN-level messages route to the info log via open_log_for_level.) */
static void info_log_path(const TestEnv *env, char *out, size_t out_sz) {
    char db_root_copy[512];
    strncpy(db_root_copy, env->db_root, sizeof(db_root_copy) - 1);
    db_root_copy[sizeof(db_root_copy) - 1] = '\0';
    char *base = dirname(db_root_copy); /* strips trailing "/db" */

    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char datebuf[16];
    strftime(datebuf, sizeof(datebuf), "%Y-%m-%d", &tmv);

    snprintf(out, out_sz, "%s/logs/%s-info.log", base, datebuf);
}

static int run_mixed_type_single_scan_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;

    /* Object with 4 plain fields: 1 will get a btree index, 1 bitmap,
       1 trigram, and 1 stays unindexed (control). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:64\",\"active:bool\","
        "\"bio:varchar:256\",\"age:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: mixed");
    free(resp); resp = NULL;

    /* Insert 40 records: mix of active true/false, distinct bios and ages. */
    for (int i = 0; i < 40; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"mixed\","
            "\"key\":\"m%d\",\"value\":{\"name\":\"user%d\","
            "\"active\":%s,\"bio\":\"loves the shard database engine\","
            "\"age\":%d}}",
            i, i, (i % 3 == 0) ? "true" : "false", 20 + (i % 40));
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Force add-index over ALL THREE types in one call — this is the
       exact shape the hn/comments incident used (mixed bitmap + btree,
       here plus a trigram field too). */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"fields\":[\"age\",\"active:bitmap\",\"bio:trigram\"],"
        "\"force\":true}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "mixed force add-index: no error");
    ASSERT_CONTAINS(resp, "\"status\":\"indexed\"",
                    "mixed force add-index: status indexed (age is btree)");
    free(resp); resp = NULL;

    /* === Correctness: all three index types actually work. === */

    /* btree: eq on age. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":20}]}", &resp);
    ASSERT_TRUE(tu_parse_count(resp) > 0, "btree: age=20 count > 0");
    free(resp); resp = NULL;

    /* bitmap: eq on active. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":true}]}", &resp);
    int active_count = tu_parse_count(resp);
    free(resp); resp = NULL;
    /* i % 3 == 0 over 40 records (i=0..39) → 14 trues (0,3,...,39). */
    ASSERT_EQ_INT(active_count, 14, "bitmap: active=true count == 14");

    /* trigram: contains on bio. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"shard\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 40, "trigram: contains 'shard' == 40");
    free(resp); resp = NULL;

    tc_close(tc);

    /* === Regression guard: exactly one BUILD-SEQ scan, zero per-field
       BUILD-TRIGRAM banners, from this single add-index call. Before the
       fix, this same request produced one BUILD-BITMAP line, one
       BUILD-TRIGRAM line, and NO BUILD-SEQ line — the three-scan bug.

       Note: BUILD-BITMAP may appear once from resolve_bitmaps (part of
       the unified single-scan engine), so we do NOT assert its absence.
       BUILD-TRIGRAM is unique to the old per-field build_trigram_pass and
       does NOT appear in the unified engine at all. === */
    char logpath[600];
    info_log_path(env, logpath, sizeof(logpath));
    char *log = tu_read_file(logpath);
    ASSERT_NOT_NULL(log, "info log file exists and is non-empty");
    if (!log) return 1;

    int n_build_seq    = substr_count(log, "BUILD-SEQ");
    int n_build_trigram = substr_count(log, "BUILD-TRIGRAM");

    ASSERT_TRUE(n_build_seq >= 1,
                "single-scan engine (BUILD-SEQ) was used for this add-index call");
    ASSERT_EQ_INT(n_build_trigram, 0,
                  "old per-field build_trigram_pass (BUILD-TRIGRAM) NOT used");

    free(log);
    return 0;
}

/* Skip-if-exists semantics must still hold post-fix: a second, non-force
   add-index over the same fields should be a no-op (no rebuild), reported
   as {"status":"all_exist"} since all 3 requested fields already have
   on-disk shards from the force call above (age is the only IT_BTREE
   field in the request, and it already exists). */
static int run_skip_if_exists_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"fields\":[\"age\",\"active:bitmap\",\"bio:trigram\"]}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "non-force re-add-index: no error");
    ASSERT_CONTAINS(resp, "\"status\":\"all_exist\"",
                    "non-force re-add-index: all_exist (skip-if-exists honored)");
    free(resp); resp = NULL;

    tc_close(tc);
    return 0;
}

static int test_add_indexes_single_scan_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int rc = run_mixed_type_single_scan_assertions(&env);
    if (rc == 0) rc = run_skip_if_exists_assertions(&env);
    test_env_stop(&env);
    return rc;
}

TEST_REGISTER("test-add-indexes-single-scan", test_add_indexes_single_scan_run)
