#define _GNU_SOURCE
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {TypedSchema *ts;FieldSchema fs;Schema sc;SlotcaskDb db;char root[96];int opened;} CFixture;
static int c_remove(const char *p,const struct stat *st,int kind,struct FTW *walk){
    (void)st;(void)walk;return kind==FTW_DP?rmdir(p):unlink(p);
}
static void c_close(CFixture *f){
    if(f->opened)slotcask_close(&f->db);
    slotcask_shutdown();free(f->ts);
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,c_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove CSV object");
}
static int c_open(CFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));ASSERT_NOT_NULL(f->ts,"command schema");if(!f->ts)return -1;
    f->ts->typed=1;f->ts->nfields=maximum?MAX_FIELDS:3;const char *types[MAX_FIELDS];
    for(int i=0;i<f->ts->nfields;i++){
        types[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":"numeric";
        f->ts->fields[i].type=maximum||i==1?FT_VARCHAR:FT_NUMERIC;
        snprintf(f->ts->fields[i].name,sizeof(f->ts->fields[i].name),"f%d",i);
        f->ts->fields[i].name_len=(int)strlen(f->ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(f->ts,types,(size_t)f->ts->nfields,NULL,0);ASSERT_EQ_INT(rc,0,"bind command schema");if(rc)return -1;
    size_t bound=0;ASSERT_EQ_INT(pkr_slot_bound(&f->ts->packed,32,&bound),DEC_OK,"command slot bound");
    f->sc=(Schema){.splits=1,.streams=1,.slot_size=(int)bound,.max_key=32,.max_value=f->ts->total_size};
    f->fs.ts=f->ts;f->fs.auto_key_schema_snapshot=f->sc;
    strcpy(f->root,"/tmp/shard-packed-csv-command-XXXXXX");
    ASSERT_NOT_NULL(mkdtemp(f->root),"command directory");
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,1,1,(int)bound);f->opened=rc==0;
    ASSERT_EQ_INT(rc,0,"open command object");return rc;
}
static int c_seed(CFixture *f,const void *key,size_t klen,const char *json,uint8_t flags){
    uint8_t *bytes=NULL;size_t n=0;
    int rc=typed_encode_alloc(f->ts,json,1,NULL,NULL,&bytes,&n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode command record");if(rc)return rc;
    SlotcaskUpsertOpts options={.record_hdr2=flags};
    rc=slotcask_upsert_with_hooks(&f->db,-1,key,klen,bytes,n,&options,NULL);
    ASSERT_EQ_INT(rc,0,"store command record");free(bytes);return rc;
}
static char *c_call(CFixture *f,int multi,const char *keys,const char *fields,
    const char *cursor,int offset,int limit,int total,size_t budget,char delimiter,int *status){
    char *text=NULL;size_t n=0;FILE *out=open_memstream(&text,&n);
    ASSERT_NOT_NULL(out,"command output");if(!out)return NULL;
    PackedCsvOptions options={.fields=fields,.delimiter=delimiter,.buffer_budget=budget};
    ASSERT_EQ_INT(objlock_rdlock(f->root,"obj"),0,"caller read lock");
    *status=multi?packed_csv_get_multi(&f->db,&f->fs,&f->sc,keys,&options,out):
        packed_csv_fetch(&f->db,&f->fs,offset,limit,cursor,total,&options,out);
    objlock_rdunlock(f->root,"obj");
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f->db),0,"no reservation leak");
    ASSERT_EQ_INT(fclose(out),0,"close command output");return text;
}
static void c_error(CFixture *f,int multi,const char *keys,const char *fields,
    const char *cursor,int offset,int total,size_t budget){
    int rc=0;char *text=c_call(f,multi,keys,fields,cursor,offset,10,total,budget,',',&rc);
    ASSERT_EQ_INT(rc,-1,"command failure status");
    ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9)&&!strstr(text,"key,f0"),"one error before any CSV prefix");free(text);
}
static void c_routes(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}int rc=0;
    char *text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"empty fetch");ASSERT_EQ_STR(text,"key,f0,f1,f2\n","empty fetch header");free(text);
    text=c_call(&f,1,"[]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"empty multi-get");ASSERT_EQ_STR(text,"key,f0,f1,f2\n","empty multi header");free(text);
    const char *a="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"a\\u0000b\\\\u0000\",\"f2\":\"1.2300\"}";
    c_seed(&f,"a",1,a,PKR_FLAGS);
    c_seed(&f,"b",1,"{\"f0\":2,\"f1\":\"b,\\\"\\n\",\"f2\":null}",PKR_FLAGS);
    const char *arow="a,1234567890123456.1234567890,a\\u0000b\\\\u0000,1.2300\n";
    const char *brow="b,2.0000000000,\"b,\"\"\\u000a\",\n";
    char expected[512];snprintf(expected,sizeof(expected),"key,f0,f1,f2\n%smissing,,,\n%s%s",arow,arow,brow);
    text=c_call(&f,1,"[\"a\",\"missing\",\"a\",\"b\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"multi-get CSV");ASSERT_EQ_STR(text,expected,"request order, duplicate, missing and exact values");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"fetch CSV");ASSERT_TRUE(text&&strstr(text,arow)&&strstr(text,brow),"fetch complete exact rows");free(text);
    char *first=c_call(&f,0,NULL,"f2,f1+f0",NULL,0,1,0,4096,'|',&rc);
    ASSERT_EQ_INT(rc,0,"fetch projection/delimiter");ASSERT_TRUE(first&&!strncmp(first,"key|f2|f1+f0\n",13),"projection header");
    char *next=c_call(&f,0,NULL,"f2,f1+f0","1",0,1,0,4096,'|',&rc);
    ASSERT_EQ_INT(rc,0,"fetch cursor");ASSERT_TRUE(first&&next&&strcmp(first,next),"cursor advances one physical live record");free(first);free(next);
    c_error(&f,0,NULL,NULL,"junk",0,0,4096);c_error(&f,0,NULL,NULL,"-1",0,0,4096);
    c_error(&f,0,NULL,NULL,"9223372036854775807",1,0,4096);
    c_error(&f,0,NULL,NULL,NULL,0,1,4096);c_error(&f,0,NULL,NULL,NULL,0,0,1);
    c_error(&f,1,"[\"a\",0]",NULL,NULL,0,0,4096);c_error(&f,1,"{}",NULL,NULL,0,0,4096);
    c_error(&f,1,"[\"a\"] trailing",NULL,NULL,0,0,4096);c_error(&f,1,"[\"a\"]",NULL,NULL,0,0,64);
    c_seed(&f,"a",1,a,SREC_FLAG_NULLMAP);c_error(&f,0,NULL,NULL,NULL,0,0,4096);
    c_error(&f,1,"[\"a\"]",NULL,NULL,0,0,4096);c_seed(&f,"a",1,a,PKR_FLAGS);
    const char binary[]={'x',0,'z'};c_seed(&f,binary,sizeof(binary),a,PKR_FLAGS);
    text=c_call(&f,1,"[\"x\\u0000z\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"binary-key lookup");ASSERT_TRUE(text&&strstr(text,"x\\u0000z,1234567890123456.1234567890"),"binary key length preserved");free(text);
    c_close(&f);
}
static void c_auto(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}int rc=0;
    f.sc.auto_key=AK_UUID;f.fs.auto_key=AK_UUID;f.fs.auto_key_schema_snapshot=f.sc;
    uint8_t uuid[16];ASSERT_EQ_INT(parse_uuid_string("00112233-4455-6677-8899-aabbccddeeff",uuid),0,"UUID key");
    c_seed(&f,uuid,16,"{\"f0\":1}",PKR_FLAGS);
    char *text=c_call(&f,1,"[\"00112233-4455-6677-8899-aabbccddeeff\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"UUID multi-get");ASSERT_TRUE(text&&strstr(text,"00112233-4455-6677-8899-aabbccddeeff,1.0000000000,,"),"UUID storage lookup and display");free(text);
    char escaped[256];size_t pos=0;escaped[pos++]='[';escaped[pos++]='"';
    const char *uuid_text="00112233-4455-6677-8899-aabbccddeeff";
    for(size_t i=0;i<strlen(uuid_text);i++)pos+=(size_t)snprintf(escaped+pos,sizeof(escaped)-pos,"\\u%04x",(unsigned char)uuid_text[i]);
    escaped[pos++]='"';escaped[pos++]=']';escaped[pos]=0;
    text=c_call(&f,1,escaped,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"escaped UUID uses text bound rather than binary width");
    ASSERT_TRUE(text&&strstr(text,"00112233-4455-6677-8899-aabbccddeeff,1.0000000000"),"escaped UUID exact lookup");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"UUID fetch");ASSERT_TRUE(text&&strstr(text,uuid_text),"UUID fetch uses stored-key display formatter");free(text);
    c_error(&f,1,"[\"broken\"]",NULL,NULL,0,0,4096);c_close(&f);
    if(c_open(&f,0)){c_close(&f);return;}
    f.sc.auto_key=AK_SEQ;f.sc.max_key=8;f.fs.auto_key=AK_SEQ;f.fs.auto_key_schema_snapshot=f.sc;
    uint8_t seq[8];memset(seq,255,8);c_seed(&f,seq,8,"{\"f2\":\"-0.5000\"}",PKR_FLAGS);
    text=c_call(&f,1,"[\"-1\",\"2\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"sequence multi-get");ASSERT_EQ_STR(text,"key,f0,f1,f2\n-1,,,-0.5000\n2,,,\n","sequence/missing key CSV");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"sequence fetch");ASSERT_TRUE(text&&strstr(text,"-1,,,-0.5000"),"sequence fetch formatting");free(text);
    c_error(&f,1,"[\"01\"]",NULL,NULL,0,0,4096);c_close(&f);
}
typedef struct {CFixture *fixture;char bytes[4096];size_t n,allow;int held;} CWriter;
static ssize_t c_write(void *ptr,const char *text,size_t n){
    CWriter *w=ptr;if(slotcask_write_reservation_held(&w->fixture->db))w->held=1;
    size_t take=n;if(take>w->allow)take=w->allow;
    if(take>sizeof(w->bytes)-1-w->n)take=sizeof(w->bytes)-1-w->n;
    memcpy(w->bytes+w->n,text,take);w->n+=take;w->bytes[w->n]=0;
    if(!take){errno=EIO;return -1;}w->allow-=take;return (ssize_t)take;
}
#if defined(__linux__)
static FILE *c_cookie(CWriter *writer){cookie_io_functions_t io={.write=c_write};return fopencookie(writer,"w",io);}
#else
static int c_funwrite(void *ptr,const char *text,int n){return (int)c_write(ptr,text,(size_t)n);}
static FILE *c_cookie(CWriter *writer){return funopen(writer,NULL,c_funwrite,NULL,NULL);}
#endif
static void c_io(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    c_seed(&f,"a",1,"{\"f0\":1}",PKR_FLAGS);
    PackedCsvOptions options={.delimiter=',',.buffer_budget=4096};
    for(int buffered=0;buffered<2;buffered++){
        CWriter writer={.fixture=&f,.allow=3};FILE *out=c_cookie(&writer);ASSERT_NOT_NULL(out,"failing CSV FILE");if(!out)continue;
        if(!buffered)setvbuf(out,NULL,_IONBF,0);
        ASSERT_EQ_INT(objlock_rdlock(f.root,"obj"),0,"I/O caller lock");
        int rc=packed_csv_fetch(&f.db,&f.fs,0,10,NULL,0,&options,out);objlock_rdunlock(f.root,"obj");
        ASSERT_EQ_INT(rc,-1,"short write/flush fails command");ASSERT_TRUE(!strstr(writer.bytes,"error"),"no error appended to CSV prefix");
        ASSERT_EQ_INT(writer.held,0,"writer reservation released before I/O");
        ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db),0,"I/O failure releases reservation");fclose(out);
    }
    c_close(&f);
}
static void c_maximum(void){
    CFixture f;if(c_open(&f,1)){c_close(&f);return;}c_seed(&f,"a",1,"{\"f127\":\"tiny\"}",PKR_FLAGS);
    int rc=0;char *text=c_call(&f,0,NULL,"f0,f127",NULL,0,10,0,100,',',&rc);
    ASSERT_EQ_INT(rc,0,"maximum schema bounded fetch");ASSERT_EQ_STR(text,"key,f0,f127\na,,tiny\n","actual-size response at large schema ceiling");free(text);c_close(&f);
}
static int test_packed_csv_commands_run(void){c_routes();c_auto();c_io();c_maximum();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-csv-commands",test_packed_csv_commands_run)
