/* NEW ordered keys are admitted strictly; historical OLD remains cleanable. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
static void ov_state(TestClient *tc,const char *old){
    char *r=NULL,req[2048];
    tc_request(tc,"{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ovz_t\",\"key\":\"a\"}",&r);
    char expected[700];snprintf(expected,sizeof(expected),"\"tag\":\"%s\"",old);
    ASSERT_CONTAINS(r,expected,"OLD preserved after rejected mutation");free(r);
    tc_request(tc,"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\"}",&r);
    ASSERT_EQ_INT(tu_parse_count(r),1,"rejected mutation preserves count");free(r);
    snprintf(req,sizeof(req),"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_t\",\"criteria\":{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"%s\"}}",old);
    tc_request(tc,req,&r);ASSERT_EQ_INT(tu_parse_count(r),1,"OLD index preserved");free(r);
}
static void ov_build_limits(TestClient *tc){
    char *r=NULL;size_t cap=512*1024;char *request=malloc(cap);
    char *big=malloc(4501),*part=malloc(3001);
    ASSERT_NOT_NULL(request,"large unindexed request");ASSERT_NOT_NULL(big,"large standalone value");
    ASSERT_NOT_NULL(part,"large composite component");
    if(!request||!big||!part){free(request);free(big);free(part);return;}
    memset(big,'z',4500);big[4500]=0;memset(part,'q',3000);part[3000]=0;
    tc_request(tc,"{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"ovz_big\",\"splits\":8,\"max_key\":64,\"fields\":[\"big:varchar:5000\",\"a:varchar:3000\",\"b:varchar:3000\"]}",&r);
    ASSERT_CONTAINS(r,"created","create large unindexed fields");free(r);
    size_t used=(size_t)snprintf(request,cap,"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_big\",\"records\":[");
    for(int i=0;i<20;i++){
        int n=snprintf(request+used,cap-used,"%s{\"key\":\"b%d\",\"value\":{\"big\":\"%s\",\"a\":\"%s\",\"b\":\"%s\"}}",i?",":"",i,big,part,part);
        if(n<0||(size_t)n>=cap-used){ASSERT_TRUE(0,"large fixture fits complete request");goto done;}
        used+=(size_t)n;
    }
    if(used+3>cap){ASSERT_TRUE(0,"large fixture terminator fits");goto done;}
    memcpy(request+used,"]}",3);tc_request(tc,request,&r);
    ASSERT_CONTAINS(r,"\"inserted\":20","large unindexed values remain supported");
    ASSERT_TRUE(r && strstr(r,"\"error\"")==NULL &&
                (strstr(r,"\"errors\"")==NULL||strstr(r,"\"errors\":0")!=NULL),
                "unindexed values have no ordered-key admission error");free(r);
    tc_request(tc,"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_big\"}",&r);
    ASSERT_EQ_INT(tu_parse_count(r),20,"all large unindexed rows are live");free(r);
    tc_request(tc,"{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ovz_big\",\"key\":\"b0\"}",&r);
    ASSERT_CONTAINS(r,big,"large unindexed value is complete");
    ASSERT_CONTAINS(r,part,"large unindexed component is complete");free(r);
    const char *fields[]={"big","a+b"};
    for(int i=0;i<2;i++){
        snprintf(request,cap,"{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"ovz_big\",\"field\":\"%s\"}",fields[i]);
        tc_request(tc,request,&r);ASSERT_CONTAINS(r,"error","oversized live key fails index build");free(r);
        snprintf(request,cap,"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_big\",\"criteria\":{\"field\":\"big\",\"op\":\"eq\",\"value\":\"%s\"}}",big);
        tc_request(tc,request,&r);ASSERT_EQ_INT(tu_parse_count(r),20,"failed build cannot publish an incomplete declared index");free(r);
    }
    tc_request(tc,"{\"mode\":\"reindex\",\"dir\":\"default\",\"object\":\"ovz_big\"}",&r);
    ASSERT_TRUE(r&&strstr(r,"\"error\"")==NULL,"failed additions leave no broken reindex declaration");free(r);
    tc_request(tc,"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_big\"}",&r);
    ASSERT_EQ_INT(tu_parse_count(r),20,"large records survive failed builds and reindex");free(r);
    tc_request(tc,"{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"ovz_prefix\",\"splits\":8,\"max_key\":16,\"fields\":[\"a:varchar:8\",\"b:int\"],\"indexes\":[\"a+b\"]}",&r);
    ASSERT_CONTAINS(r,"created","legacy varchar-leading composite stays creatable");free(r);
    tc_request(tc,"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_prefix\",\"records\":{\"one\":{\"a\":\"open\",\"b\":1},\"two\":{\"a\":\"open\",\"b\":2}}}",&r);
    ASSERT_CONTAINS(r,"\"inserted\":2","legacy varchar-leading composite stays writable");free(r);
    tc_request(tc,"{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ovz_prefix\",\"criteria\":{\"field\":\"a\",\"op\":\"eq\",\"value\":\"open\"}}",&r);
    ASSERT_EQ_INT(tu_parse_count(r),2,"legacy varchar-leading composite stays filterable");free(r);
done:free(request);free(big);free(part);
}

static int test_index_oversized_varchar_run(void){
    TestEnv env={0};if(test_env_start(&env))return 1;
    TestClientCfg cfg={.port=env.port,.io_timeout_ms=60000};TestClient *tc=tc_connect(&cfg);
    ASSERT_NOT_NULL(tc,"connect");if(!tc){test_env_stop(&env);return 1;}
    char *r=NULL,req[4096],big[545],ok[544];memset(big,'x',544);big[544]=0;memset(ok,'x',543);ok[543]=0;
    tc_request(tc,"{\"mode\":\"add-dir\",\"dir\":\"default\"}",&r);free(r);
    tc_request(tc,"{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"ovz_t\",\"splits\":8,\"max_key\":16,\"fields\":[\"tag:varchar:600\"],\"indexes\":[\"tag\"]}",&r);
    ASSERT_CONTAINS(r,"created","create indexed object");free(r);
    snprintf(req,sizeof(req),"{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"key\":\"a\",\"value\":{\"tag\":\"%s\"}}",ok);
    tc_request(tc,req,&r);ASSERT_CONTAINS(r,"inserted","544 bytes admitted");free(r);ov_state(tc,ok);
    const char *forms[]={
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"key\":\"a\",\"value\":{\"tag\":\"%s\"}}",
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ovz_t\",\"key\":\"a\",\"value\":{\"tag\":\"%s\"}}",
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"records\":{\"a\":{\"tag\":\"%s\"}}}",
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"ovz_t\",\"delimiter\":\"|\",\"data\":\"a|%s\\n\"}",
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"ovz_t\",\"records\":[{\"key\":\"a\",\"value\":{\"tag\":\"%s\"}}]}",
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"ovz_t\",\"criteria\":{\"field\":\"tag\",\"op\":\"exists\"},\"value\":{\"tag\":\"%s\"}}",
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"ovz_t\",\"delimiter\":\"|\",\"data\":\"a|%s\\n\"}"
    };
    for(size_t i=0;i<sizeof(forms)/sizeof(*forms);i++){
        snprintf(req,sizeof(req),forms[i],big);
        if(i==6){
            char path[512];snprintf(path,sizeof(path),"%s/oversize.csv",env.db_root);
            FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private delimited source");
            if(fp){fprintf(fp,"a|%s\n",big);fclose(fp);}
            snprintf(req,sizeof(req),"{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"ovz_t\",\"delimiter\":\"|\",\"file\":\"%s\"}",path);
        }
        tc_request(tc,req,&r);
        ASSERT_CONTAINS(r,"error","545 byte NEW is error on every route");
        if(i>=2){ASSERT_CONTAINS(r,"\"skipped\":0","oversize failure not skipped");}
        free(r);ov_state(tc,ok);
    }
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ovz_t\",\"records\":{\"bad\":{\"tag\":\"%s\"},\"good\":{\"tag\":\"small\"}}}",big);
    tc_request(tc,req,&r);ASSERT_CONTAINS(r,"\"inserted\":1","mixed valid row commits");ASSERT_CONTAINS(r,"\"errors\":1","mixed oversized row errors");free(r);
    tc_request(tc,"{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"ovz_t\",\"key\":\"bad\"}",&r);ASSERT_CONTAINS(r,"false","invalid fresh row absent");free(r);
    ov_build_limits(tc);
    tc_close(tc);test_env_stop(&env);return t_ctx->failed?1:0;
}
TEST_REGISTER("test-index-oversized-varchar",test_index_oversized_varchar_run)
