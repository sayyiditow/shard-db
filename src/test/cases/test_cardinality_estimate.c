#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct { size_t k; int saturated; int estimable; } CardEst;
extern CardEst card_est_by_field(const char *db_root, const char *object,
                                 const char *field, const char *value, size_t cap);
extern CardEst card_est_by_field_contains(const char *db_root, const char *object,
                                          const char *field, const char *value, size_t cap);

static int test_card_est_bitmap_exact(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ce\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"active:bool\"],"
        "\"indexes\":[\"active:bitmap\"]}", &resp); free(resp); resp=NULL;
    char body[16384]; size_t p=0; int k=0;
    SB_APPEND(body,p,sizeof(body),"{");
    for (int i=0;i<70;i++){SB_APPEND(body,p,sizeof(body),"%s\"k%d\":{\"active\":true}",k==0?"":",",k);k++;}
    for (int i=0;i<30;i++){SB_APPEND(body,p,sizeof(body),",\"k%d\":{\"active\":false}",k);k++;}
    SB_APPEND(body,p,sizeof(body),"}");
    char req[17408]; snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ce\",\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    CardEst e = card_est_by_field(env.db_root, "default/ce", "active", "true", 10);
    ASSERT_EQ_INT((int)e.k, 70, "active=true exact count = 70");
    ASSERT_EQ_INT(e.saturated, 0, "bitmap exact, not saturated even with cap<k");
    ASSERT_EQ_INT(e.estimable, 1, "bitmap is estimable");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-card-est-bitmap-exact", test_card_est_bitmap_exact)

static int test_card_est_btree_capped(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ceb\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"tag:varchar:8\"],"
        "\"indexes\":[\"tag\"]}", &resp); free(resp); resp=NULL;
    char body[65536]; size_t p=0; int k=0;
    SB_APPEND(body,p,sizeof(body),"{");
    for (int i=0;i<5;i++){SB_APPEND(body,p,sizeof(body),"%s\"k%d\":{\"tag\":\"rare\"}",k==0?"":",",k);k++;}
    for (int i=0;i<200;i++){SB_APPEND(body,p,sizeof(body),",\"k%d\":{\"tag\":\"common\"}",k);k++;}
    SB_APPEND(body,p,sizeof(body),"}");
    char req[66560]; snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ceb\",\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    CardEst r = card_est_by_field(env.db_root, "default/ceb", "tag", "rare", 10);
    ASSERT_EQ_INT((int)r.k, 5, "tag=rare exact = 5");
    ASSERT_EQ_INT(r.saturated, 0, "rare not saturated under cap 10");
    ASSERT_EQ_INT(r.estimable, 1, "btree eq is estimable");
    CardEst c = card_est_by_field(env.db_root, "default/ceb", "tag", "common", 10);
    ASSERT_EQ_INT(c.saturated, 1, "tag=common saturated over cap 10");
    ASSERT_TRUE(c.k > 10, "common k reported >= cap");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-card-est-btree-capped", test_card_est_btree_capped)

static int test_card_est_trigram(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cet\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"title:varchar:64\"],"
        "\"indexes\":[\"title:trigram\"]}", &resp); free(resp); resp=NULL;

    /* Insert 4 records containing rare substring "zqxj"
     * and 150 records containing common substring "the". */
    char body[65536]; size_t p=0; int k=0;
    SB_APPEND(body,p,sizeof(body),"{");
    for (int i=0;i<4;i++){
        SB_APPEND(body,p,sizeof(body),"%s\"k%d\":{\"title\":\"a zqxj title %d\"}",
                  k==0?"":",",k,i);
        k++;
    }
    for (int i=0;i<150;i++){
        SB_APPEND(body,p,sizeof(body),",\"k%d\":{\"title\":\"the common one %d\"}",k,i);
        k++;
    }
    SB_APPEND(body,p,sizeof(body),"}");
    char req[66560];
    snprintf(req,sizeof(req),
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cet\","
             "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    /* Rare substring "zqxj": grams are "zqx" and "qxj".
     * Both have posting size 4 (only the 4 inserted records).
     * Estimate should be small (<=10) and not saturated under cap 50. */
    CardEst r = card_est_by_field_contains(env.db_root, "default/cet",
                                           "title", "zqxj", 50);
    ASSERT_EQ_INT(r.estimable, 1, "trigram contains is estimable");
    ASSERT_TRUE(r.k >= 4 && r.k <= 10,
                "rare 'zqxj' estimate small (>=4 matches, <=10 candidates)");
    ASSERT_EQ_INT(r.saturated, 0, "rare 'zqxj' not saturated under cap 50");

    /* Common substring "the": every one of the 150 "the common one N" records
     * contains "the", plus the rare ones don't.  150 > cap=50, so saturated. */
    CardEst c = card_est_by_field_contains(env.db_root, "default/cet",
                                           "title", "the", 50);
    ASSERT_EQ_INT(c.estimable, 1, "trigram contains 'the' is estimable");
    ASSERT_EQ_INT(c.saturated, 1, "common 'the' saturated over cap 50");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-card-est-trigram", test_card_est_trigram)

/* Part B issue-C: bitmap OP_IN estimate sums ALL values, not just the first.
 * Field `val:varchar:4` with bitmap index; values a(100), b(50), c(10).
 * COUNT query with tag in (a,b,c) should return 160, not 100. */
static int test_card_est_bitmap_in_sum(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ce_in\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"val:varchar:4\"],"
        "\"indexes\":[\"val:bitmap\"]}", &resp); free(resp); resp=NULL;
    char body[16384]; size_t p=0; int k=0;
    SB_APPEND(body,p,sizeof(body),"{");
    for (int i=0;i<100;i++){SB_APPEND(body,p,sizeof(body),"%s\"k%d\":{\"val\":\"a\"}",k==0?"":",",k);k++;}
    for (int i=0;i<50; i++){SB_APPEND(body,p,sizeof(body),",\"k%d\":{\"val\":\"b\"}",k);k++;}
    for (int i=0;i<10; i++){SB_APPEND(body,p,sizeof(body),",\"k%d\":{\"val\":\"c\"}",k);k++;}
    SB_APPEND(body,p,sizeof(body),"}");
    char req[17408];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ce_in\","
        "\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp); resp=NULL;

    /* COUNT: val in (a,b,c) → should be 160 */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ce_in\","
                    "\"criteria\":[{\"field\":\"val\",\"op\":\"in\",\"value\":\"a,b,c\"}]}",
               &resp);
    ASSERT_CONTAINS(resp, "160", "bitmap IN count: a(100)+b(50)+c(10) = 160");
    free(resp);

    /* Single-value counts still correct */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ce_in\","
                    "\"criteria\":[{\"field\":\"val\",\"op\":\"eq\",\"value\":\"a\"}]}",
               &resp);
    ASSERT_CONTAINS(resp, "100", "bitmap EQ count: a = 100");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-card-est-bitmap-in-sum", test_card_est_bitmap_in_sum)
