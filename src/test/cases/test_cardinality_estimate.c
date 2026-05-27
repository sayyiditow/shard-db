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

static int test_card_est_bitmap_exact(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ce\","
        "\"splits\":8,\"max_key\":12,\"fields\":[\"active:bool\"],"
        "\"indexes\":[\"active:bitmap\"]}", &resp); free(resp); resp=NULL;
    char body[16384]; int p=0,k=0; p+=snprintf(body+p,sizeof(body)-p,"{");
    for (int i=0;i<70;i++){p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"active\":true}",k==0?"":",",k);k++;}
    for (int i=0;i<30;i++){p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"active\":false}",k);k++;}
    p+=snprintf(body+p,sizeof(body)-p,"}");
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
