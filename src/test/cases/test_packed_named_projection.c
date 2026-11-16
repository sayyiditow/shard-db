#define _XOPEN_SOURCE 700
/* Darwin: keep the BSD surface (mkdtemp, funopen) visible despite the
   strict-POSIX _XOPEN_SOURCE above; the macro is inert elsewhere. */
#define _DARWIN_C_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
static TypedSchema *n_schema(int maximum){
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
static void n_free(TypedSchema *ts){if(ts){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);}}
static int n_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);ASSERT_EQ_INT(rc,0,"encode fixture");return rc;
}
static void n_value(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *name,
                    const char *expected,size_t length,const char *json){
    size_t used=99;errno=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX,&used);
    ASSERT_NOT_NULL(out,"named text present");ASSERT_TRUE(out&&used==length&&!memcmp(out,expected,length)&&!out[length],"exact raw bytes and terminator");free(out);
    used=99;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,length+1,&used);
    ASSERT_TRUE(out&&used==length&&!memcmp(out,expected,length),"exact raw budget");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,length,&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"one-short raw budget");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX);
    ASSERT_EQ_STR(out,json,"named JSON exact");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,strlen(json)+1);
    ASSERT_EQ_STR(out,json,"exact JSON budget");free(out);
    errno=0;out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,strlen(json));
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short JSON budget");free(out);
    FieldSchema fs={.ts=ts};out=json_projected_field((const char*)bytes,n,name,&fs,PKR_FLAGS);
    ASSERT_EQ_STR(out,json,"shared named cell integration");free(out);
}
static void n_absent(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *name){
    size_t used=99;errno=ERANGE;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==0&&used==0,"absence distinct from error");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX);ASSERT_EQ_STR(out,"null","absent JSON literal");free(out);
}
static void n_basics(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(n_encode(ts,"{\"f0\":\"1234567890123456.1234567890\",\"f1\":1e-26,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":true,\"f4\":0,\"f5\":null,\"f6\":\"yes\"}",&bytes,&n)){n_free(ts);return;}
    static const char binary[]={'a',0,'b','"','\\','\n'};
    n_value(ts,bytes,n,"f2",binary,sizeof(binary),"\"a\\u0000b\\\"\\\\\\n\"");
    n_value(ts,bytes,n,"f4+f6","0yes",4,"\"0yes\"");
    n_value(ts,bytes,n,"++f4+++absent+f5+f6+","0yes",4,"\"0yes\"");
    n_value(ts,bytes,n,"f0+f4","1234567890123456.12345678900",28,"\"1234567890123456.12345678900\"");
    n_value(ts,bytes,n,"f3+f6","trueyes",7,"\"trueyes\"");
    n_value(ts,bytes,n," f4+f4","0",1,"\"0\"");
    n_value(ts,bytes,n,"f4","0",1,"0");n_value(ts,bytes,n,"f3","true",4,"\"true\"");
    char joined[7];memcpy(joined,binary,sizeof(binary));joined[6]='0';
    n_value(ts,bytes,n,"f2+f4",joined,sizeof(joined),"\"a\\u0000b\\\"\\\\\\n0\"");
    n_absent(ts,bytes,n,"f5");n_absent(ts,bytes,n,"unknown");n_absent(ts,bytes,n,"f5+unknown");n_absent(ts,bytes,n,"+++");n_absent(ts,bytes,n,"");
    ts->fields[6].removed=1;n_absent(ts,bytes,n,"f6");n_value(ts,bytes,n,"f6+f4","0",1,"\"0\"");ts->fields[6].removed=0;
    char selector[310];memset(selector,'x',300);memcpy(selector+300,"+f4",4);
    n_value(ts,bytes,n,selector,"0",1,"\"0\"");
    char *out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f4+f6,f0,f5+unknown",SIZE_MAX);
    ASSERT_EQ_STR(out,"{\"f4+f6\":\"0yes\",\"f0\":1234567890123456.1234567890,\"f5+unknown\":null}","object scalar/composite policy");free(out);
    errno=0;out=typed_get_field_str(ts,bytes,(int)n,2,PKR_FLAGS);
    ASSERT_TRUE(!out&&errno==EILSEQ,"legacy raw getter stays unconverted");free(out);
    free(bytes);n_free(ts);
}
static void n_empty(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!n_encode(ts,"{\"f2\":\"\",\"f5\":\"\",\"f4\":0}",&bytes,&n)){
        n_value(ts,bytes,n,"f2","",0,"\"\"");n_absent(ts,bytes,n,"f2+f5");n_value(ts,bytes,n,"f2+f4","0",1,"\"0\"");free(bytes);}
    n_free(ts);
}
static void n_failures(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(n_encode(ts,"{\"f0\":1,\"f2\":\"abc\",\"f4\":0}",&bytes,&n)){n_free(ts);return;}
    const uint8_t flags[]={0,SREC_FLAG_NULLMAP,SREC_FLAG_PACKED,0x80};
    for(size_t i=0;i<sizeof(flags);i++){
        size_t used=99;errno=0;char *out=typed_packed_named_text(ts,bytes,n,flags[i],"f2+f4",SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==EILSEQ&&used==0,"raw generation mismatch");free(out);
        errno=0;out=typed_packed_named_json(ts,bytes,n,flags[i],"missing+f5",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"empty composite cannot bypass generation");free(out);
    }
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"bad descriptor fixture");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);errno=0;
        char *out=typed_packed_named_json(ts,bad,n,PKR_FLAGS,"f4+f4",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unselected corruption rejects");free(out);free(bad);}
    char many[(MAX_FIELDS+1)*3+1];size_t pos=0;
    for(int i=0;i<MAX_FIELDS;i++){memcpy(many+pos,"f4+",3);pos+=3;}many[pos-1]=0;
    char expected[MAX_FIELDS+1];memset(expected,'0',MAX_FIELDS);expected[MAX_FIELDS]=0;
    char json[MAX_FIELDS+3];json[0]='"';memcpy(json+1,expected,MAX_FIELDS);json[MAX_FIELDS+1]='"';json[MAX_FIELDS+2]=0;
    n_value(ts,bytes,n,many,expected,MAX_FIELDS,json);
    many[pos-1]='+';memcpy(many+pos,"f4",3);size_t used=99;errno=0;
    char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,many,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"component count bound");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,many,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==E2BIG,"object propagates component limit");free(out);
    char *longname=malloc(MAX_LINE+1);ASSERT_NOT_NULL(longname,"long name fixture");
    if(longname){memset(longname,'x',MAX_LINE);longname[MAX_LINE]=0;used=99;errno=0;
        out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,longname,SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"selector byte bound");free(out);free(longname);}
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,NULL,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL selector");free(out);
    errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f4",SIZE_MAX,NULL);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL length output");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,NULL,0,PKR_FLAGS,"f4",SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL record");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"missing",0,&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"zero budget fails before absence");free(out);
    ts->fields[3].type=FT_LONG;errno=0;out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,"f4+f4",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"unselected bad fixed schema");free(out);ts->fields[3].type=FT_BOOL;
    free(bytes);n_free(ts);
}
static void n_limits(void){
    TypedSchema *ts=n_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!n_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){n_value(ts,bytes,n,"f0+f127","tiny",4,"\"tiny\"");free(bytes);}n_free(ts);
    ts=n_schema(0);if(!ts)return;size_t len=131072+1+16383;char *digits=malloc(len+1);ASSERT_NOT_NULL(digits,"full numeric fixture");
    if(digits){memset(digits,'1',len);digits[131072]='.';digits[len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare maximum numeric");TypedInput input={digits,len,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"set maximum numeric");int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate maximum numeric");
        if(!rc){size_t used=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f1+f1",2*len+1,&used);
            ASSERT_TRUE(out&&used==2*len&&!memcmp(out,digits,len)&&!memcmp(out+len,digits,len),"full composite numeric every digit");free(out);free(bytes);}
        typed_prepared_free(&p);free(digits);}
    len=4u*1024*1024;char *content=malloc(len);ASSERT_NOT_NULL(content,"maximum varchar fixture");
    if(content){memset(content,'x',len);content[1]=0;TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare maximum varchar");
        TypedInput input={content,len,1,0,0},zero={"0",1,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"set maximum varchar");ASSERT_EQ_INT(typed_prepared_set(&p,4,&zero,NULL,0),0,"set zero component");
        int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate maximum varchar");
        if(!rc){size_t used=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f2+f4",len+2,&used);
            ASSERT_TRUE(out&&used==len+1&&!memcmp(out,content,len)&&out[len]=='0',"maximum composite binary text intact");free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    n_free(ts);
}
static void n_chunks(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;
    size_t one=8193,total=3*one,json_len=6*total+2;
    char *input=calloc(one,1),*expected=calloc(total+1,1),*json=malloc(json_len+1);
    ASSERT_NOT_NULL(input,"chunk input");ASSERT_NOT_NULL(expected,"chunk display expectation");ASSERT_NOT_NULL(json,"chunk JSON expectation");
    if(input&&expected&&json){
        json[0]='"';for(size_t i=0;i<total;i++)memcpy(json+1+6*i,"\\u0000",6);
        json[json_len-1]='"';json[json_len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare chunk record");
        TypedInput in={input,one,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&in,NULL,0),0,"set chunk bytes");
        uint8_t *bytes=NULL;size_t n=0;int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate chunk record");
        if(!rc){n_value(ts,bytes,n,"f2+f2+f2",expected,total,json);free(bytes);}
        typed_prepared_free(&p);
    }
    free(input);free(expected);free(json);n_free(ts);
}
static int test_packed_named_projection_run(void){n_basics();n_empty();n_failures();n_limits();n_chunks();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-named-projection",test_packed_named_projection_run)
