/* TEMPORARY round-8 write-vs-fetch trim-boundary diagnostic probe. */
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
#include <time.h>  /* struct timespec / nanosleep, for s2_wait_for_count */

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

static int s2_count_matching(TestEnv *env, const char *tag, const char *substr) {
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
            if (strstr(line, tag) && strstr(line, substr)) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

/* Poll s2_count_matching() until it reaches at least `want`, or
   timeout_s elapses -- bridges LOG_AUDIT's asynchronous ring-buffer
   writer (log_writer_thread, src/db/config.c): a scan issued
   immediately after the triggering request completes can race the
   writer thread's drain-and-fflush cycle and see a partial or empty
   file. The writer wakes on a condition variable the instant an entry
   is pushed (not on a fixed timer), so steady-state latency is a
   single scheduling quantum; 100ms poll / 5s timeout gives wide margin.
   Returns the last-observed count (not a bool) so a genuine timeout
   still drives a precise ASSERT_EQ_INT/ASSERT_TRUE failure message
   instead of a bare "false". */
static int s2_wait_for_count(TestEnv *env, const char *tag, const char *substr,
                              int want, int timeout_s) {
    int n = 0;
    for (int i = 0; i < timeout_s * 10; i++) {
        n = s2_count_matching(env, tag, substr);
        if (n >= want) return n;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return n;
}

static int test_numeric_between_probe8_run(void) {
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

    ASSERT_EQ_INT(do_count(tc, "bi_num", BETWEEN_CRIT), 3,
        "W1 wire between -1 and 1 = 3 (expected red on macOS)");

    int w_total = s2_wait_for_count(&env, "NB2TRACE8W", "", 5, 5);
    ASSERT_EQ_INT(w_total, 5,
        "S2 audit log holds exactly 5 NB2TRACE8W bulk_phase3 lines");
    int w_n2 = s2_wait_for_count(&env, "NB2TRACE8W", "key=n_2", 1, 5);
    ASSERT_TRUE(w_n2 >= 1, "S2 audit log has at least one NB2TRACE8W line for key=n_2");

    int f_total = s2_wait_for_count(&env, "NB2TRACE8F", "", 3, 5);
    ASSERT_EQ_INT(f_total, 3,
        "S2 audit log holds exactly 3 NB2TRACE8F kf_fetch lines");
    int f_n2 = s2_wait_for_count(&env, "NB2TRACE8F", "key=n_2", 1, 5);
    ASSERT_TRUE(f_n2 >= 1, "S2 audit log has at least one NB2TRACE8F line for key=n_2");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe8", test_numeric_between_probe8_run)
