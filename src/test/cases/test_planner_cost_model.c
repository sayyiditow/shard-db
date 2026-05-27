#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

extern int leaf_selective_for_test(const char *db_root, const char *object,
                                   const char *field, const char *value, size_t *out_k);

/* Shared fixture: object `cm` with a btree-indexed `tag` field;
 * 5 rows tag=rare, 200 rows tag=common (N=205, budget=205/8=25). */
static TestClient *cm_setup(TestEnv *env, const char *obj, const char *fields,
                            const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp=NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    char co[1024];
    snprintf(co,sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp=NULL;
    return tc;
}
static void cm_insert_tags(TestClient *tc, const char *obj) {
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for (int i=0;i<5;i++){p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\"}",k==0?"":",",k);k++;}
    for (int i=0;i<200;i++){p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\"}",k);k++;}
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":%s}",obj,body);
    tc_request(tc, req, &resp); free(resp);
}

static int test_cost_selectivity_primitive(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "cm", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "cm");
    size_t kr=0, kc=0;
    int sr = leaf_selective_for_test(env.db_root, "default/cm", "tag", "rare", &kr);
    int sc = leaf_selective_for_test(env.db_root, "default/cm", "tag", "common", &kc);
    ASSERT_EQ_INT((int)kr, 5, "rare K=5");
    ASSERT_EQ_INT(sr, 1, "rare selective (5 <= budget 25)");
    ASSERT_EQ_INT(sc, 0, "common not selective (200 > budget 25)");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-cost-selectivity-primitive", test_cost_selectivity_primitive)
