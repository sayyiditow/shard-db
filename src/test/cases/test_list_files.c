/* src/test/cases/test_list_files.c
 * Port of tests/test-list-files.sh — list-files mode (alphabetical
 * pagination, prefix/suffix/contains/glob match modes).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


/* Pull "total":N out of a list-files response. */
static int parse_total(const char *resp) {
    if (!resp) return -1;
    const char *p = strstr(resp, "\"total\":");
    return p ? atoi(p + 8) : -1;
}

static int test_list_files_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"lft\","
        "\"splits\":16,\"max_key\":32,\"fields\":[\"k:varchar:8\"]}", &resp);
    free(resp); resp = NULL;

    /* Stage 6 files in /tmp, put-file into the object. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/lft_test_%d", (int)getpid());
    tu_run_cmd("rm -rf %s && mkdir -p %s", tmpdir, tmpdir);
    const char *names[] = {"alpha.pdf","alpha2.pdf","beta.pdf","delta.pdf","gamma.txt","zeta.png"};
    for (size_t i = 0; i < sizeof(names)/sizeof(names[0]); i++) {
        char path[400];
        snprintf(path, sizeof(path), "%s/%s", tmpdir, names[i]);
        FILE *f = fopen(path, "w"); if (f) { fputs("x\n", f); fclose(f); }
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"lft\","
            "\"path\":\"%s\"}", path);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tu_run_cmd("rm -rf %s", tmpdir);

    /* Full listing alphabetical */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 6, "total=6");
    ASSERT_CONTAINS(resp, "\"files\":[\"alpha.pdf\",\"alpha2.pdf\"", "alphabetical: alpha first");
    ASSERT_CONTAINS(resp, "zeta.png\"]", "alphabetical: zeta last");
    free(resp); resp = NULL;

    /* prefix filter */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\",\"prefix\":\"alpha\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 2, "prefix alpha → total=2");
    ASSERT_CONTAINS(resp, "alpha.pdf", "prefix alpha → alpha.pdf");
    ASSERT_CONTAINS(resp, "alpha2.pdf", "prefix alpha → alpha2.pdf");
    ASSERT_TRUE(strstr(resp, "beta.pdf") == NULL, "prefix alpha rejects beta.pdf");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\",\"prefix\":\"z\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 1, "prefix z → total=1");
    ASSERT_CONTAINS(resp, "zeta.png", "prefix z → zeta.png");
    free(resp); resp = NULL;

    /* limit / offset */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\",\"limit\":2}", &resp);
    ASSERT_CONTAINS(resp, "\"files\":[\"alpha.pdf\",\"alpha2.pdf\"]", "limit=2 first page");
    ASSERT_EQ_INT(parse_total(resp), 6, "limit=2 echoes total=6");
    ASSERT_CONTAINS(resp, "\"limit\":2", "limit echoed");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"offset\":2,\"limit\":2}", &resp);
    ASSERT_CONTAINS(resp, "\"files\":[\"beta.pdf\",\"delta.pdf\"]", "offset=2 limit=2 → beta,delta");
    ASSERT_CONTAINS(resp, "\"offset\":2", "offset echoed");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"offset\":4,\"limit\":10}", &resp);
    ASSERT_CONTAINS(resp, "gamma.txt", "offset=4 → tail starts gamma");
    ASSERT_CONTAINS(resp, "zeta.png", "offset=4 → ends with zeta");
    free(resp); resp = NULL;

    /* prefix + pagination */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"prefix\":\"alpha\",\"offset\":1,\"limit\":1}", &resp);
    ASSERT_CONTAINS(resp, "\"files\":[\"alpha2.pdf\"]", "prefix+offset → just alpha2");
    ASSERT_EQ_INT(parse_total(resp), 2, "prefix-filtered total=2");
    free(resp); resp = NULL;

    /* no match */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"prefix\":\"xxx\"}", &resp);
    ASSERT_CONTAINS(resp, "\"files\":[]", "no match → []");
    ASSERT_EQ_INT(parse_total(resp), 0, "no match → total=0");
    free(resp); resp = NULL;

    /* offset past end */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"offset\":100,\"limit\":10}", &resp);
    ASSERT_CONTAINS(resp, "\"files\":[]", "offset>total → empty page");
    ASSERT_EQ_INT(parse_total(resp), 6, "but total still reports full set");
    free(resp); resp = NULL;

    /* match=suffix */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\".pdf\",\"match\":\"suffix\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 4, "suffix .pdf → total=4");
    ASSERT_CONTAINS(resp, "alpha.pdf", "suffix → alpha.pdf");
    ASSERT_TRUE(strstr(resp, "gamma.txt") == NULL, "suffix .pdf rejects gamma.txt");
    free(resp); resp = NULL;

    /* match=contains */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"lpha\",\"match\":\"contains\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 2, "contains lpha → total=2");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"eta\",\"match\":\"contains\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 2, "contains eta → total=2");
    ASSERT_CONTAINS(resp, "beta.pdf", "contains eta → beta.pdf");
    ASSERT_CONTAINS(resp, "zeta.png", "contains eta → zeta.png");
    ASSERT_TRUE(strstr(resp, "delta.pdf") == NULL, "contains eta rejects delta.pdf");
    free(resp); resp = NULL;

    /* match=glob */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"*.pdf\",\"match\":\"glob\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 4, "glob *.pdf → total=4");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"alpha?.pdf\",\"match\":\"glob\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 1, "glob alpha?.pdf → total=1");
    ASSERT_CONTAINS(resp, "alpha2.pdf", "glob alpha?.pdf → alpha2.pdf");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"[ab]*\",\"match\":\"glob\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 3, "glob [ab]* → total=3");
    free(resp); resp = NULL;

    /* invalid match */
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"pattern\":\"foo\",\"match\":\"regex\"}", &resp);
    ASSERT_CONTAINS(resp, "invalid match mode", "invalid match → error");
    free(resp); resp = NULL;

    /* survives delete-file */
    tc_request(tc, "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"lft\","
                   "\"filename\":\"beta.pdf\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"list-files\",\"dir\":\"default\",\"object\":\"lft\"}", &resp);
    ASSERT_EQ_INT(parse_total(resp), 5, "after delete → total=5");
    ASSERT_TRUE(strstr(resp, "beta.pdf") == NULL, "deleted beta.pdf gone from list");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-list-files", test_list_files_run)
