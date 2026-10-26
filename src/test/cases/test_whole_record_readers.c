#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static TypedSchema *r_schema(int maximum){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"private reader schema");if(!ts)return NULL;
    ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"numeric":i==3?"int":"varchar";
        ts->fields[i].type=i<2&&!maximum?FT_NUMERIC:i==3&&!maximum?FT_INT:FT_VARCHAR;
        if(ts->fields[i].type==FT_INT)parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind private schema");if(rc){free(ts);return NULL;}return ts;
}
static int r_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode independent input");return rc;
}
static char *r_stream(const TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr,int *rc){
    char *out=NULL;size_t used=0;FILE *fp=open_memstream(&out,&used);
    ASSERT_NOT_NULL(fp,"capture stream");if(!fp){*rc=-1;return NULL;}
    *rc=typed_decode_stream(ts,bytes,n,fp,hdr);int saved=errno;
    ASSERT_EQ_INT(fclose(fp),0,"close capture");errno=saved;return out;
}
static void r_equal(TypedSchema *ts,const uint8_t *bytes,size_t n,uint8_t hdr,const char *expected){
    char *out=typed_decode(ts,bytes,(int)n,hdr);ASSERT_NOT_NULL(out,"allocated JSON available");
    ASSERT_TRUE(out&&strcmp(out,expected)==0,"allocated JSON exact without truncation");free(out);
    int rc=0;out=r_stream(ts,bytes,(int)n,hdr,&rc);ASSERT_EQ_INT(rc,0,"stream success");
    ASSERT_TRUE(out&&strcmp(out,expected)==0,"stream JSON exact without truncation");free(out);
}
static void r_failure(TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr,int error){
    errno=0;char *out=typed_decode(ts,bytes,n,hdr);ASSERT_TRUE(out==NULL,"bad record allocated decode rejects");
    ASSERT_EQ_INT(errno,error,"bad record errno");free(out);
    out=NULL;size_t used=0;FILE *fp=open_memstream(&out,&used);ASSERT_NOT_NULL(fp,"capture invalid stream");if(!fp)return;
    fputs("PREFIX",fp);errno=0;int rc=typed_decode_stream(ts,bytes,n,fp,hdr),saved=errno;
    ASSERT_EQ_INT(rc,-1,"bad stream rejects");ASSERT_EQ_INT(saved,error,"bad stream errno");
    ASSERT_EQ_INT(fclose(fp),0,"close invalid capture");ASSERT_EQ_STR(out,"PREFIX","failure emits no JSON bytes");free(out);
}
static void r_invalid(TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr){r_failure(ts,bytes,n,hdr,EILSEQ);}
static void r_packed(void){
    TypedSchema *ts=r_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *input="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"0.00000000000000000000000001\",\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":0,\"f4\":null,\"f5\":\"\"}";
    const char *expected="{\"f0\":1234567890123456.1234567890,\"f1\":0.00000000000000000000000001,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":0,\"f4\":null,\"f5\":\"\"}";
    if(r_encode(ts,input,&bytes,&n)){free(ts);return;}
    r_equal(ts,bytes,n,PKR_FLAGS,expected);
    r_invalid(ts,bytes,(int)n,0);r_invalid(ts,bytes,(int)n,SREC_FLAG_NULLMAP);
    r_invalid(ts,bytes,(int)n,SREC_FLAG_PACKED);r_invalid(ts,bytes,(int)n,0x80);
    r_invalid(ts,bytes,(int)n-1,PKR_FLAGS);r_failure(ts,NULL,0,PKR_FLAGS,EINVAL);r_invalid(ts,bytes,-1,PKR_FLAGS);
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"corrupt descriptor copy");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);
        r_invalid(ts,bad,(int)n,PKR_FLAGS);free(bad);}
    FILE *ro=fopen("/dev/null","r");ASSERT_NOT_NULL(ro,"read-only output stream");
    if(ro){setvbuf(ro,NULL,_IONBF,0);errno=0;
        ASSERT_EQ_INT(typed_decode_stream(ts,bytes,(int)n,ro,PKR_FLAGS),-1,"write failure reported");
        ASSERT_TRUE(ferror(ro),"write failure marks FILE");fclose(ro);}
    for(int i=0;i<ts->nfields;i++)ts->fields[i].removed=1;
    r_equal(ts,bytes,n,PKR_FLAGS,"{}");free(bytes);free(ts);
}
static void r_legacy(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=3;ts->nullmap_prefix=2;ts->total_size=1009;
    for(int i=0;i<3;i++){snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    ts->fields[0].type=FT_INT;ts->fields[0].size=4;
    ts->fields[1].type=FT_VARCHAR;ts->fields[1].offset=4;ts->fields[1].size=1002;
    TypedField *e=&ts->fields[2];e->type=FT_ENUM;e->offset=1006;e->size=1;e->enum_width=1;e->n_enum_values=1;
    e->enum_values=calloc(1,sizeof(char*));ASSERT_NOT_NULL(e->enum_values,"enum catalog");if(!e->enum_values){free(ts);return;}
    e->enum_values[0]=malloc(1001);ASSERT_NOT_NULL(e->enum_values[0],"long enum");if(!e->enum_values[0]){free(e->enum_values);free(ts);return;}
    memset(e->enum_values[0],'z',1000);e->enum_values[0][1000]=0;
    uint8_t bytes[1009]={1,0};bytes[6]=3;bytes[7]=132;memset(bytes+8,'v',900);
    char expected[2000];memcpy(expected,"{\"f0\":0,\"f1\":\"",14);memset(expected+14,'v',900);
    size_t at=914;memcpy(expected+at,"\",\"f2\":\"",8);at+=8;memset(expected+at,'z',1000);at+=1000;
    memcpy(expected+at,"\"}",3);
    r_equal(ts,bytes,sizeof(bytes),SREC_FLAG_NULLMAP,expected);
    r_invalid(ts,bytes,sizeof(bytes),PKR_FLAGS);
    free_enum_values(e);ts->nfields=1;ts->total_size=6;
    const uint8_t short_old[]={1};r_equal(ts,short_old,sizeof(short_old),0,"{\"f0\":0}");
    const uint8_t short_flagged[]={1,0,9};r_equal(ts,short_flagged,sizeof(short_flagged),SREC_FLAG_NULLMAP,"{\"f0\":0}");
    const uint8_t null[]={1,1};r_equal(ts,null,sizeof(null),SREC_FLAG_NULLMAP,"{\"f0\":null}");
    ts->fields[0].size=70000;r_invalid(ts,short_flagged,sizeof(short_flagged),SREC_FLAG_NULLMAP);free(ts);
}
static int r_rm(const char *p,const struct stat *s,int kind,struct FTW *f){(void)s;(void)kind;(void)f;return remove(p);}
static void r_physical(void){
    TypedSchema *ts=r_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *input="{\"f0\":1.25,\"f2\":\"on disk\"}";
    if(r_encode(ts,input,&bytes,&n)){free(ts);return;}
    char path[]="/tmp/whole-read-XXXXXX";char *created=mkdtemp(path);
    ASSERT_NOT_NULL(created,"private physical directory");
    if(!created){free(bytes);free(ts);return;}
    size_t bound=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&bound),DEC_OK,"physical bound");
    slotcask_init(32,32);SlotcaskDb db;int rc=slotcask_open(&db,path,1,1,(int)bound);ASSERT_EQ_INT(rc,0,"open physical object");
    if(!rc){SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,bytes,n,&opts,NULL),0,"store packed record");
        slotcask_close(&db);rc=slotcask_open(&db,path,1,1,(int)bound);ASSERT_EQ_INT(rc,0,"reopen physical object");
        if(!rc){void *raw=NULL;size_t len=0;uint8_t hdr=0;
            ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&raw,&len,&hdr),0,"fetch actual physical bytes");
            ASSERT_EQ_INT(len,n,"actual stored length");ASSERT_EQ_INT(hdr,PKR_FLAGS,"stored generation");
            if(raw)r_equal(ts,raw,len,hdr,"{\"f0\":1.2500000000,\"f1\":null,\"f2\":\"on disk\",\"f3\":null,\"f4\":null,\"f5\":null}");
            free(raw);slotcask_close(&db);}}
    slotcask_shutdown();ASSERT_EQ_INT(nftw(path,r_rm,16,FTW_DEPTH|FTW_PHYS),0,"remove private object");free(bytes);free(ts);
}
static void r_limits(void){
    TypedSchema *ts=r_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!r_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){
        ASSERT_TRUE(ts->total_size>1000000000,"maximum schema bound");
        ASSERT_TRUE(n<4096,"tiny actual record");char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);
        ASSERT_NOT_NULL(out,"maximum schema decode");
        if(out){ASSERT_CONTAINS(out,"\"f127\":\"tiny\"","selected tiny content");ASSERT_TRUE(strlen(out)<4096,"actual output length");}
        int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"maximum schema stream");
        ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"maximum schema readers agree");free(out);free(stream);free(bytes);}
    free(ts);ts=r_schema(0);if(!ts)return;
    size_t length=4u*1024*1024;char *content=malloc(length);ASSERT_NOT_NULL(content,"max varchar content");
    if(content){memset(content,'x',length);TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare large row");
        TypedInput input={content,length,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"assign maximum varchar");
        if(!typed_prepared_alloc(&p,&bytes,&n,NULL,0)){
            char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);ASSERT_NOT_NULL(out,"maximum varchar decode");
            if(out){ASSERT_TRUE(strlen(out)>length&&strlen(out)<length+128,"complete maximum varchar JSON");
                ASSERT_TRUE(strstr(out,"\"f2\":\"xxx")!=NULL,"maximum prefix");}
            int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"large stream succeeds");
            ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"large readers byte-identical");free(out);free(stream);free(bytes);}
        else ASSERT_TRUE(0,"allocate maximum value");
        typed_prepared_free(&p);free(content);}
    /* Full PostgreSQL-sized decimal, without a legacy integer carrier. */
    length=131072+1+16383;content=malloc(length+1);ASSERT_NOT_NULL(content,"maximum decimal text");
    if(content){memset(content,'1',length);content[131072]='.';content[length]=0;
        TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare huge decimal");
        TypedInput input={content,length,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"assign full decimal range");
        if(!typed_prepared_alloc(&p,&bytes,&n,NULL,0)){
            char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);ASSERT_NOT_NULL(out,"maximum decimal decode");
            if(out)ASSERT_TRUE(strstr(out,content)!=NULL,"every decimal digit preserved");
            int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"huge decimal stream");
            ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"huge decimal readers agree");free(out);free(stream);free(bytes);}
        else ASSERT_TRUE(0,"allocate huge decimal");
        typed_prepared_free(&p);free(content);}
    free(ts);
}
static int test_whole_record_readers_run(void){r_packed();r_legacy();r_physical();r_limits();return t_ctx->failed?1:0;}
TEST_REGISTER("test-whole-record-readers",test_whole_record_readers_run)
