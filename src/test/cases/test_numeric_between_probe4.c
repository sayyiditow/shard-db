/* src/test/cases/test_numeric_between_probe4.c
 * TEMPORARY round-4 diagnostic probe — collection-count-vs-validated-count
 * seam for the macOS numeric-BETWEEN defect, per
 * docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md.
 * W  — wire repro (expected red on macOS). Also the source of the
 *      NB2TRACE4 seam lines S2 reads: the fixture daemon runs a real
 *      logging worker, so its LOG_AUDIT output reliably lands in
 *      audit.log.
 * S1 — the REAL orchestrator (cmd_count) run in-process, output captured
 *      through g_out. Reused unchanged from round 3 — already proved the
 *      defect reproduces with no daemon/wire runtime involved. Its
 *      LOG_AUDIT calls are NOT relied on for S2: shard_db_open_internal
 *      builds a local instance with no logging worker running, so S1's
 *      trace output goes to stderr, not audit.log. S1 here only confirms
 *      the in-process return value.
 * S2 — dumps the daemon's NB2TRACE4 seam lines from the audit log
 *      (produced by the W calls above): collected-candidate count vs.
 *      final Kf-revalidated count for the same idx_count_for_leaf call.
 * Expected to FAIL on macOS arm64 until the defect is fixed; must pass
 * 100% on Linux. Delete with the plan close-out.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Internal embedded initializer: constructs the fully initialized execution
   context required by S1 without taking the public embedded API's DB-root
   lock (the fixture daemon already owns that lock). */
extern ShardDb *shard_db_open_internal(const char *db_root);

static const char *BETWEEN_CRIT =
    "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\",\"value2\":\"1\"}]";

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

/* S2 — scan the daemon's log dir for NB2TRACE4 lines and dump them.
   LOG_DIR for the standard fixture is <parent-of-db_root>/logs. */
static int s2_dump_traces(TestEnv *env) {
    char base[300], logs_dir[320];
    snprintf(base, sizeof(base), "%s", env->db_root);
    char *slash = strrchr(base, '/');
    if (!slash) { TAP_DIAG("  S2 no parent dir of %s\n", env->db_root); return 0; }
    *slash = '\0';
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);

    DIR *d = opendir(logs_dir);
    if (!d) { TAP_DIAG("  S2 cannot open %s\n", logs_dir); return 0; }
    int matches = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char p[640];
        snprintf(p, sizeof(p), "%s/%s", logs_dir, de->d_name);
        FILE *f = fopen(p, "r");
        if (!f) continue;
        char line[1024];
        while (fgets(line, sizeof(line), f))
            if (strstr(line, "NB2TRACE4")) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe4_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "env start"); return 1; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],"
        "\"indexes\":[\"amt\"]}", &resp);
    free(resp); resp = NULL;
    const char *vals[5] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, vals[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* W1 — the failing wire shape plus controls. */
    ASSERT_EQ_INT(do_count(tc, "bi_num", BETWEEN_CRIT),
        3, "W1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
        2, "W1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"),
        3, "W1 wire gte 0 = 3 (control)");

    /* S1 — the REAL orchestrator, in-process, output captured through
       g_out. The server passes db_root = <root>/<dir> and the bare
       object name (server.c:1622-1628 dispatch shape). The fixture daemon
       owns a separate process and its g_db is not shared, so S1 needs a
       fully initialized local instance bound to the same fixture root. */
    char dir_root[512];
    snprintf(dir_root, sizeof(dir_root), "%s/default", env.db_root);
    ShardDb *s1_db = shard_db_open_internal(env.db_root);
    ASSERT_NOT_NULL(s1_db, "S1 full local instance");
    if (s1_db) {
        parallel_pool_init(2);
        parallel_io_pool_init(2);
        char *cap = NULL; size_t caplen = 0;
        FILE *ms = open_memstream(&cap, &caplen);
        ASSERT_NOT_NULL(ms, "S1 open_memstream");
        if (ms) {
            FILE *old_out = g_out;
            g_out = ms;
            int rc = cmd_count(dir_root, "bi_num", BETWEEN_CRIT);
            fflush(ms);
            g_out = old_out;
            fclose(ms);
            TAP_DIAG("  S1 cmd_count rc=%d captured='%s'\n", rc, cap ? cap : "");
            ASSERT_EQ_INT(rc, 0, "S1 cmd_count rc = 0");
            int s1 = cap ? atoi(cap) : -1;
            ASSERT_EQ_INT(s1, 3, "S1 in-process cmd_count returns 3");
        }
        free(cap);
    }

    /* S2 — collect/validate seam lines from the audit log, produced by
       the three W1 wire queries above (the fixture daemon runs a real
       logging worker, so LOG_AUDIT reliably lands in audit.log there;
       S1's in-process LOG_AUDIT calls do not and are not counted here).
       Each single-leaf indexed count runs through idx_count_for_leaf's
       IT_BTREE block once, emitting one "collect" + one "validate" line,
       so the three W1 queries (between + two controls) must leave
       exactly 6 lines; this exact bound proves the capture mechanics
       work end to end. The between query's own pair is the one with
       op_between=1 — the read-out is in its actual in=/out= values,
       inspected by hand from the saved log. */
    int traces = s2_dump_traces(&env);
    ASSERT_EQ_INT(traces, 6, "S2 audit log holds exactly 6 NB2TRACE4 lines (3 W1 queries x collect+validate)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe4", test_numeric_between_probe4_run)
