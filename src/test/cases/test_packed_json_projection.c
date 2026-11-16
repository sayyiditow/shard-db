#define _XOPEN_SOURCE 700
/* Darwin: keep the BSD surface (mkdtemp, funopen) visible despite the
   strict-POSIX _XOPEN_SOURCE above; the macro is inert elsewhere. */
#define _DARWIN_C_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
static TypedSchema *p_schema(int maximum){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"private projection schema");if(!ts)return NULL;
    ts->typed=1;ts->nfields=maximum?MAX_FIELDS:7;const char *types[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        types[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"numeric":i==3?"bool":i==4?"int":i==6?"enum(yes,no)":"varchar";
        if(!strncmp(types[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else if(!strcmp(types[i],"varchar"))ts->fields[i].type=FT_VARCHAR;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,types,(size_t)ts->nfields,NULL,0);ASSERT_EQ_INT(rc,0,"bind private schema");
    if(rc){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);return NULL;}return ts;
}
static void p_free(TypedSchema *ts){if(ts){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);}}
static int p_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);ASSERT_EQ_INT(rc,0,"encode fixture");return rc;
}
static void p_object(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *fields,const char *expected){
    char *out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,SIZE_MAX);
    ASSERT_NOT_NULL(out,"projected object available");ASSERT_TRUE(out&&!strcmp(out,expected),"projected object exact");
    size_t budget=strlen(expected)+1;free(out);
    out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,budget);
    ASSERT_NOT_NULL(out,"exact object budget");ASSERT_TRUE(out&&!strcmp(out,expected),"exact-budget bytes");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,budget-1);
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short object budget rejects");free(out);
}
static void p_cell(TypedSchema *ts,const uint8_t *bytes,size_t n,int index,const char *expected){
    char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,SIZE_MAX);
    ASSERT_NOT_NULL(out,"JSON cell available");ASSERT_TRUE(out&&!strcmp(out,expected),"JSON cell exact");free(out);
    out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,strlen(expected)+1);
    ASSERT_NOT_NULL(out,"exact cell budget");ASSERT_TRUE(out&&!strcmp(out,expected),"exact cell bytes");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,strlen(expected));
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short cell budget rejects");free(out);
    out=json_typed_row_cell(ts,bytes,(int)n,index,PKR_FLAGS);
    ASSERT_NOT_NULL(out,"rows helper returns JSON");ASSERT_TRUE(out&&!strcmp(out,expected),"rows helper exact");free(out);
    FieldSchema fs={.ts=ts};out=json_projected_field((const char*)bytes,n,ts->fields[index].name,&fs,PKR_FLAGS);
    ASSERT_NOT_NULL(out,"named helper returns JSON");ASSERT_TRUE(out&&!strcmp(out,expected),"named helper exact");free(out);
}
static void p_basics(void){
    TypedSchema *ts=p_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *json="{\"f0\":\"1234567890123456.1234567890\",\"f1\":1e-26,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":true,\"f4\":0,\"f5\":null,\"f6\":\"yes\"}";
    if(p_encode(ts,json,&bytes,&n)){p_free(ts);return;}
    p_cell(ts,bytes,n,0,"1234567890123456.1234567890");
    p_cell(ts,bytes,n,1,"0.00000000000000000000000001");
    p_cell(ts,bytes,n,2,"\"a\\u0000b\\\"\\\\\\n\"");p_cell(ts,bytes,n,3,"\"true\"");
    p_cell(ts,bytes,n,4,"0");p_cell(ts,bytes,n,5,"null");p_cell(ts,bytes,n,6,"\"yes\"");
    p_object(ts,bytes,n,"f3,f0,f5,unknown,f4","{\"f3\":\"true\",\"f0\":1234567890123456.1234567890,\"f5\":null,\"unknown\":null,\"f4\":0}");
    p_object(ts,bytes,n,",,f4,,f4, f4,","{\"f4\":0,\"f4\":0,\" f4\":null}");
    p_object(ts,bytes,n,"","{}");p_object(ts,bytes,n,",,,","{}");
    FieldSchema fs={.ts=ts};char *out=json_projected_field((const char*)bytes,n,"missing",&fs,PKR_FLAGS);
    ASSERT_EQ_STR(out,"null","unknown named field on flagged row");free(out);
    errno=0;out=json_projected_field((const char*)bytes,n,"f2+f0",&fs,PKR_FLAGS);
    ASSERT_EQ_STR(out,"\"a\\u0000b\\\"\\\\\\n1234567890123456.1234567890\"","packed composite named helper exact");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f4,f2+f0",SIZE_MAX);
    ASSERT_EQ_STR(out,"{\"f4\":0,\"f2+f0\":\"a\\u0000b\\\"\\\\\\n1234567890123456.1234567890\"}","mixed composite request exact");free(out);
    ts->fields[4].removed=1;p_cell(ts,bytes,n,4,"null");p_object(ts,bytes,n,"f4","{\"f4\":null}");ts->fields[4].removed=0;
    strcpy(ts->fields[4].name,"x\"\\\n");ts->fields[4].name_len=4;
    p_object(ts,bytes,n,ts->fields[4].name,"{\"x\\\"\\\\\\n\":0}");
    free(bytes);p_free(ts);
}
static void p_failures(void){
    TypedSchema *ts=p_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(p_encode(ts,"{\"f0\":1,\"f2\":\"value\"}",&bytes,&n)){p_free(ts);return;}
    const uint8_t flags[]={0,SREC_FLAG_NULLMAP,SREC_FLAG_PACKED,0x80};
    for(size_t i=0;i<sizeof(flags);i++){
        errno=0;char *out=typed_packed_project_json(ts,bytes,n,flags[i],"f0",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"object generation mismatch");free(out);
        errno=0;out=typed_packed_projected_cell(ts,bytes,n,flags[i],0,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"cell generation mismatch");free(out);
        FieldSchema fs={.ts=ts};errno=0;out=json_projected_field((const char*)bytes,n,"missing",&fs,flags[i]);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unknown field cannot bypass corrupt generation");free(out);
    }
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"corrupt directory copy");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);
        errno=0;char *out=typed_packed_project_json(ts,bad,n,PKR_FLAGS,"f4",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unselected corrupt descriptor still rejects");free(out);free(bad);}
    errno=0;char *out=typed_packed_project_json(ts,bytes,n-1,PKR_FLAGS,"f0",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EILSEQ,"truncated record rejects");free(out);
    errno=0;out=typed_packed_project_json(ts,NULL,0,PKR_FLAGS,"f0",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL record argument rejects");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,NULL,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL projection rejects");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,MAX_FIELDS,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"invalid cell index");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f0",0);
    ASSERT_TRUE(!out&&errno==ENOMEM,"zero object budget");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,0,0);
    ASSERT_TRUE(!out&&errno==ENOMEM,"zero cell budget");free(out);
    char *longlist=malloc(MAX_LINE+1);ASSERT_NOT_NULL(longlist,"overlong selector");
    if(longlist){memset(longlist,'x',MAX_LINE);longlist[MAX_LINE]=0;errno=0;
        out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,longlist,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==E2BIG,"no silent selector truncation");free(out);free(longlist);}
    char count[(MAX_FIELDS+1)*3+1];size_t at=0;
    for(int i=0;i<MAX_FIELDS+1;i++){memcpy(count+at,"f4,",3);at+=3;}count[at]=0;errno=0;
    out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,count,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==E2BIG,"no silent field-count truncation");free(out);
    errno=0;out=json_typed_row_cell(ts,bytes,-1,0,PKR_FLAGS);
    ASSERT_TRUE(!out&&errno==EILSEQ,"negative rows length rejects");free(out);
    free(bytes);p_free(ts);
}
static void p_legacy_controls(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy control schema");if(!ts)return;
    ts->typed=1;ts->nfields=2;ts->total_size=7;ts->nullmap_prefix=2;
    ts->fields[0].type=FT_INT;ts->fields[0].size=4;strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;
    ts->fields[1].type=FT_BOOL;ts->fields[1].size=1;ts->fields[1].offset=4;strcpy(ts->fields[1].name,"f1");ts->fields[1].name_len=2;
    uint8_t bytes[]={1,0,0,0,0,7,1};FieldSchema fs={.ts=ts};
    char *out=json_projected_field((const char*)bytes,sizeof(bytes),"f0",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"7","legacy numeric remains bare");free(out);
    out=json_typed_row_cell(ts,bytes,sizeof(bytes),1,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"\"true\"","legacy bool cell stays quoted");free(out);
    out=json_projected_field((const char*)bytes,sizeof(bytes),"f0+f1",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"\"7true\"","legacy composite unchanged");free(out);
    out=json_projected_field((const char*)bytes,sizeof(bytes),"missing",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"null","legacy flagged unknown null");free(out);
    free(ts);
}
static void p_limits(void){
    TypedSchema *ts=p_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!p_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){
        ASSERT_TRUE(ts->total_size>1000000000&&n<4096,"max schema tiny actual row");
        p_object(ts,bytes,n,"f127,absent","{\"f127\":\"tiny\",\"absent\":null}");free(bytes);}
    p_free(ts);ts=p_schema(0);if(!ts)return;
    size_t len=4u*1024*1024;char *content=malloc(len);ASSERT_NOT_NULL(content,"maximum binary varchar");
    if(content){memset(content,'x',len);content[1]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare large record");
        TypedInput input={content,len,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"set large varchar");
        TypedInput zero={"0",1,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,4,&zero,NULL,0),0,"set small selected field");
        int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate actual large row");
        if(!rc){p_object(ts,bytes,n,"f4","{\"f4\":0}");
            char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,2,SIZE_MAX);ASSERT_NOT_NULL(out,"large JSON cell");
            if(out){ASSERT_TRUE(strlen(out)==len+7,"embedded NUL escaped without truncation");ASSERT_TRUE(!memcmp(out,"\"x\\u0000",8),"large prefix escaped");}free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    len=131072+1+16383;content=malloc(len+1);ASSERT_NOT_NULL(content,"maximum decimal input");
    if(content){memset(content,'1',len);content[131072]='.';content[len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare full decimal");TypedInput input={content,len,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"set full bare numeric");int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate full decimal");
        if(!rc){char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,1,len+1);ASSERT_NOT_NULL(out,"full numeric cell");ASSERT_TRUE(out&&!strcmp(out,content),"every decimal digit survives");free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    p_free(ts);
}
static void p_schema_widths(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"fixed width schema");if(!ts)return;
    ts->typed=1;ts->nfields=1;parse_field_type("bool",&ts->fields[0]);
    strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;const char *types[]={"bool"};
    int rc=typed_schema_bind_packed(ts,types,1,NULL,0);ASSERT_EQ_INT(rc,0,"bind one-byte fixed schema");
    if(rc){free(ts);return;}
    uint8_t *encoded=NULL;size_t n=0;if(p_encode(ts,"{\"f0\":true}",&encoded,&n)){free(ts);return;}
    /* Keep extra allocation bytes so RED exercises wrong-span reads without
       going beyond the C allocation. The supplied record length stays exact. */
    uint8_t bytes[32]={0};ASSERT_TRUE(n<sizeof(bytes),"one fixed record fits fixture");
    if(n>=sizeof(bytes)){free(encoded);free(ts);return;}memcpy(bytes,encoded,n);free(encoded);
    const enum FieldType wider[]={FT_SHORT,FT_INT,FT_LONG,FT_FLOAT,FT_DOUBLE,
        FT_DATE,FT_DATETIME,FT_DATETIMEMS,FT_TIME,FT_TIMESTAMP,FT_UUID,FT_IPV4,FT_IPV6};
    for(size_t i=0;i<sizeof(wider)/sizeof(wider[0]);i++){
        ts->fields[0].type=wider[i];TypedRowView view;errno=0;
        ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"view rejects logical width mismatch");
        errno=0;char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,0,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EINVAL,"cell rejects mismatched fixed schema");free(out);
        errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"absent",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EINVAL,"unknown selection cannot bypass schema width guard");free(out);
    }
    ts->fields[0].type=FT_NONE;TypedRowView view;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"invalid fixed logical type rejects");
    ts->fields[0].type=FT_ENUM;ts->fields[0].n_enum_values=1;ts->fields[0].enum_width=2;
    char *values[]={"yes"};ts->fields[0].enum_values=values;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"enum carrier width mismatch rejects");
    ts->fields[0].enum_width=1;ts->fields[0].n_enum_values=257;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"enum count and width disagree");
    ts->fields[0].n_enum_values=1;ts->fields[0].enum_values=NULL;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"missing enum table rejects");
    ts->fields[0].type=FT_BOOL;ts->fields[0].n_enum_values=0;ts->fields[0].enum_width=0;
    ASSERT_EQ_INT(typed_row_open(ts,bytes,n,PKR_FLAGS,&view),0,"restored schema remains readable");free(ts);
}
static int test_packed_json_projection_run(void){p_basics();p_failures();p_legacy_controls();p_limits();p_schema_widths();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-json-projection",test_packed_json_projection_run)
