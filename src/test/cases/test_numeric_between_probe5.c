/* TEMPORARY round-5 numeric-BETWEEN Kf-fetch diagnostic probe. */
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

/* Scan the fixture daemon log directory for matching diagnostic lines. */
static int s2_count_matching(TestEnv *env, const char *substr) {
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
            if (strstr(line, "NB2TRACE5") && strstr(line, substr)) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe5_run(void) {
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
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"), 2,
        "W1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"), 3,
        "W1 wire gte 0 = 3 (control)");

    int fetch_in_between = s2_count_matching(&env, "fetch_in op_between=1");
    ASSERT_EQ_INT(fetch_in_between, 3,
        "S2 audit log holds exactly 3 fetch_in op_between=1 lines");
    int total = s2_count_matching(&env, "");
    ASSERT_TRUE(total >= 18,
        "S2 audit log holds a plausible minimum of NB2TRACE5 lines "
        "(proves capture mechanics work end to end)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe5", test_numeric_between_probe5_run)
