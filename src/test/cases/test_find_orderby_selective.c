#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../test_client.h"
#include "../fixtures.h"
#include "types.h"   /* SB_APPEND — safe StringBuilder vs CodeQL snprintf-overflow flag */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* find [by=X] order_by time desc limit N must return X's own records in
 * time-desc order — NOT leak other authors' records (the order_by-walk
 * bug returned wrong rows when it walked the time index and mis-filtered).
 * 3 authors; a=10 recs (times 100..109), b=10 (200..209), c=10 (300..309).
 * find [by=a] order_by time desc limit 3 -> a's three newest: 109,108,107. */
static int test_find_by_orderby_time_correct(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"prof\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"by:varchar:16\",\"time:long\"],"
        "\"indexes\":[\"by\",\"time\",\"by+time\"]}", &resp); free(resp); resp=NULL;

    char body[8192]; size_t p = 0; int k = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    const char *names[] = {"a","b","c"};
    for (int g = 0; g < 3; g++)
        for (int j = 0; j < 10; j++) {
            SB_APPEND(body, p, sizeof(body), "%s\"k%d\":{\"by\":\"%s\",\"time\":%d}",
                      k==0?"":",", k, names[g], (g+1)*100 + j);
            k++;
        }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[9216];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"prof\",\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"prof\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"a\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_TRUE(resp != NULL, "find response");
    /* a's three newest = 109,108,107; must NOT contain b/c (200+/300+). */
    ASSERT_TRUE(strstr(resp, "\"time\":109") != NULL, "newest 109 present");
    ASSERT_TRUE(strstr(resp, "\"time\":108") != NULL, "108 present");
    ASSERT_TRUE(strstr(resp, "\"time\":107") != NULL, "107 present");
    ASSERT_TRUE(strstr(resp, "\"by\":\"b\"") == NULL, "no author b leaked");
    ASSERT_TRUE(strstr(resp, "\"by\":\"c\"") == NULL, "no author c leaked");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-by-orderby-time-correct", test_find_by_orderby_time_correct)

/* count(criteria) must equal the number of rows find(criteria) returns for
 * the multi-criteria shapes the aggregate fast-paths use — the cross-check
 * that caught the leaf-drop. tag (btree) + active (bitmap):
 * 30 tag=x&active=true, 20 tag=x&active=false, 10 tag=y&active=true.
 * count [tag=x AND active=true] must be 30, not tag=x-only 50. */
static int test_count_find_parity(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"par\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"tag:varchar:8\",\"active:bool\"],"
        "\"indexes\":[\"tag\",\"active:bitmap\"]}", &resp); free(resp); resp=NULL;

    char body[16384]; size_t p=0; int k=0;
    SB_APPEND(body, p, sizeof(body), "{");
    struct { int n; const char*t; const char*a; } seg[]={{30,"x","true"},{20,"x","false"},{10,"y","true"}};
    for (size_t g=0; g<sizeof(seg)/sizeof(seg[0]); g++)
        for (int j=0;j<seg[g].n;j++){ SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"tag\":\"%s\",\"active\":%s}",k==0?"":",",k,seg[g].t,seg[g].a); k++; }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[17408];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"par\",\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"par\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"x\"},"
                      "{\"field\":\"active\",\"op\":\"eq\",\"value\":true}]}", &resp);
    ASSERT_TRUE(resp != NULL && strstr(resp, "30") != NULL, "count [tag=x AND active=true] = 30");
    ASSERT_TRUE(resp != NULL && strstr(resp, "50") == NULL, "not tag=x-only (50)");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-count-find-parity", test_count_find_parity)
