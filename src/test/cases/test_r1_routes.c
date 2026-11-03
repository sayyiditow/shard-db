#define _GNU_SOURCE
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "packed_csv_command.h"
#include "typed_read_command.h"
#include "typed_bulk_insert.h"
#include "schema_wire.h"
#include "fixtures.h"
#include "test_client.h"
#include <errno.h>
#include <fcntl.h>
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

static char *r_read(CFixture *f,TypedReadKind kind,const char *format,const char *fields,const char *keys,size_t budget,int *rc){
    char *text=NULL;size_t size=0;FILE *out=open_memstream(&text,&size);
    ASSERT_NOT_NULL(out,"read stream");if(!out)return NULL;
    TypedReadOptions options={.kind=kind,.format=format,.fields=fields,.keys=keys,
        .limit=10,.budget=budget,.delimiter=','};
    *rc=typed_read_command(&f->db,&f->fs,&f->sc,&options,out);
    ASSERT_EQ_INT(fclose(out),0,"close read stream");return text;
}
static void r_reads(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    const char *input="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"a\\u0000b\\\\u0000\",\"f2\":\"1.2300\"}";
    c_seed(&f,"a",1,input,PKR_FLAGS);const char binary[]={'x',0,'z'};c_seed(&f,binary,3,input,PKR_FLAGS);
    int rc=0;char *text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"fetch exact packed JSON");
    ASSERT_CONTAINS(text,"1234567890123456.1234567890","decimal lexeme exact");
    ASSERT_CONTAINS(text,"x\\u0000z","binary key JSON escaped");free(text);
    text=r_read(&f,TYPED_READ_FETCH,"rows","f2,f0",NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"rows projection");ASSERT_CONTAINS(text,"1.2300,1234567890123456.1234567890","rows exact order");free(text);
    text=r_read(&f,TYPED_READ_FETCH,"dict",NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"dict fetch");ASSERT_CONTAINS(text,"\"results\":{","dict envelope");free(text);
    text=r_read(&f,TYPED_READ_GET_MULTI,NULL,NULL,"[\"a\",\"missing\",\"a\",\"x\\u0000z\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"multi-get duplicates and binary key");ASSERT_CONTAINS(text,"\"missing\":null","missing explicit null");free(text);
    text=r_read(&f,TYPED_READ_KEYS,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"keys");ASSERT_CONTAINS(text,"x\\u0000z","keys exact bytes");free(text);
    text=r_read(&f,TYPED_READ_EXISTS_MULTI,NULL,NULL,"[\"a\",\"missing\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"multi exists");ASSERT_CONTAINS(text,"\"a\":true","found true");ASSERT_CONTAINS(text,"\"missing\":false","absent false");free(text);
    text=r_read(&f,TYPED_READ_NOT_EXISTS,NULL,NULL,"[\"a\",\"missing\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"missing key list");ASSERT_EQ_STR(text,"[\"missing\"]\n","missing key only");free(text);
    text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,32,&rc);
    ASSERT_EQ_INT(rc,-1,"quota rejection");ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9)&&!strstr(text,"results"),"no partial success prefix");free(text);
    text=r_read(&f,TYPED_READ_GET_MULTI,NULL,NULL,"[\"a\",7]",65536,&rc);
    ASSERT_EQ_INT(rc,-1,"malformed multi-get");ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9),"malformed request no prefix");free(text);
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db),0,"ordinary reads use no writer reservation");
    c_close(&f);
}
static void r_computed(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    uint8_t *old=NULL,*result=NULL;size_t n=0,used=0;char err[256]={0};
    ASSERT_EQ_INT(typed_encode_alloc(f.ts,"{\"f0\":\"1234567890123456.1234567890\",\"f2\":\"1.2300\"}",0,NULL,NULL,&old,&n,err,sizeof(err)),0,"OLD exact encoded");
    SlotcaskOldRecord record={.value=old,.vlen=n,.hdr2=PKR_FLAGS};
    ASSERT_EQ_INT(typed_update_computed_alloc(f.ts,&record,"{\"f0\":{\"$incr\":\"0.00000000005\"},\"f2\":{\"$incr\":\"0.0001\"}}",&result,&used,err,sizeof(err)),0,"exact increments");
    if(result){char *text=typed_decode(f.ts,result,(int)used,PKR_FLAGS);
        ASSERT_CONTAINS(text,"1234567890123456.1234567891","constrained assignment ties away once");
        ASSERT_CONTAINS(text,"1.2301","bare sum scale");free(text);free(result);result=NULL;}
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f0\":{\"$copy\":\"f2\"}}",&result,&used,err,sizeof(err))!=0,"incompatible numeric copy refused");
    ASSERT_TRUE(result==NULL&&used==0,"failure output contract");
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f2\":{\"$incr\":1,\"$copy\":\"f2\"}}",&result,&used,err,sizeof(err))!=0,"mixed operators refused");
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f2\":{\"$incr\":\"junk\"},\"f2\":1}",&result,&used,err,sizeof(err))!=0,"earlier invalid computed duplicate cannot be overwritten");
    free(result);result=NULL;used=0;
    char *text=typed_decode(f.ts,old,(int)n,PKR_FLAGS);ASSERT_CONTAINS(text,"1234567890123456.1234567890","OLD unchanged");free(text);free(old);c_close(&f);
}
static void r_declarations(void){
    const char *good="[\"cash:numeric:26,10:default=0:not_null\",\"note:varchar:20:default=a\\\\b\"]";
    char *specs[MAX_FIELDS];size_t count=0;
    ASSERT_EQ_INT(typed_field_specs_parse(good,specs,&count),0,"schema strings decoded once");ASSERT_EQ_INT(count,2,"all declarations");
    if(count==2)ASSERT_EQ_STR(specs[1],"note:varchar:20:default=a\\b","declaration slash exact");
    for(size_t i=0;i<count;i++)free(specs[i]);
    const char *bad[]={"[]","[\"x:int\",\"x:long\"]","[\"x:int\",]","[\"x:int\\u0000\"]","[\"x:int\\n\"]"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++)ASSERT_EQ_INT(typed_field_specs_parse(bad[i],specs,&count),-1,"invalid declarations before mutation");
    const char *invalid[]={"varchar:20:default=random(2)junk","varchar:20:default=seq(a)junk","int:auto_create:auto_update","int:default=1:auto_update"};
    for(size_t i=0;i<sizeof(invalid)/sizeof(invalid[0]);i++){
        char buffer[128];snprintf(buffer,sizeof(buffer),"%s",invalid[i]);TypedField field={0};
        ASSERT_EQ_INT(parse_default_modifier_checked(buffer,&field),-1,"generator/default complete consumption");
    }
    char valid[]="varchar:300:default=random(150):not_null";TypedField field={0};
    ASSERT_EQ_INT(parse_default_modifier_checked(valid,&field),1,"generated default > old fixed buffer");
    parse_field_type(valid,&field);char *value=generate_default_owned(&field,NULL,NULL,1024);
    ASSERT_NOT_NULL(value,"owned random default");if(value)ASSERT_EQ_INT(strlen(value),300,"no 256 byte truncation");free(value);
    field.default_invalid=1;ASSERT_TRUE(generate_default_owned(&field,NULL,NULL,1024)==NULL,"legacy malformed default cannot generate truncated value");
}

static void r_schema_routes(void){
    TestEnv env={0};if(test_env_start(&env))return;
    TestClientCfg cfg={.port=env.port,.io_timeout_ms=30000};TestClient *client=tc_connect(&cfg);
    ASSERT_NOT_NULL(client,"schema route connect");if(!client){test_env_stop(&env);return;}
    char *response=NULL;
    tc_request(client,"{\"mode\":\"add-dir\",\"dir\":\"r1\"}",&response);free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"declaration_layout\":\"legacy\",\"fields\":[\"\\u0063ash:numeric:26,10:default=0:not_null\",\"note:varchar:30:default=a\\\\b\"]}",&response);
    ASSERT_TRUE(response&&!strstr(response,"\"error\""),"escaped declarations accepted");free(response);
    tc_request(client,"{\"mode\":\"describe-object\",\"dir\":\"r1\",\"object\":\"roundtrip\"}",&response);
    ASSERT_CONTAINS(response,"\"spec\":\"cash:numeric:26,10:default=0:not_null\"","original precision and modifier declaration preserved");
    ASSERT_CONTAINS(response,"\"declaration_layout\":\"legacy\"","legacy layout identity explicit");free(response);
    tc_request(client,"{\"mode\":\"add-field\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"fields\":[\"escaped:varchar:30:default=x\\\\y\"]}",&response);
    ASSERT_TRUE(response&&!strstr(response,"\"error\""),"add-field unescapes once");free(response);
    tc_request(client,"{\"mode\":\"edit-field\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"fields\":[\"escaped:varchar:30:default=random(2)junk\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","bad default edit refused");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"bad\",\"fields\":[\"x:int:default=999999999999\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","invalid literal default refused at create");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"wronglayout\",\"declaration_layout\":\"packed\",\"fields\":[\"x:numeric:26,10\"]}",&response);
    ASSERT_CONTAINS(response,"\"status\":\"created\"","packed declaration creates a packed object (R5)");free(response);
    tc_request(client,"{\"mode\":\"describe-object\",\"dir\":\"r1\",\"object\":\"wronglayout\"}",&response);
    ASSERT_CONTAINS(response,"\"declaration_layout\":\"packed\"","packed object never described as legacy");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"streamloss\",\"declaration_layout\":\"packed\",\"streams\":3,\"fields\":[\"x:int\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","unsupported imported stream count never silently replaced");free(response);
    tc_close(client);test_env_stop(&env);
}


static char *r_bulk_call(CFixture *f,const char *json,int *rc){
    char *text=NULL;size_t length=0;FILE *out=open_memstream(&text,&length);
    ASSERT_NOT_NULL(out,"bulk stream");if(!out){*rc=-1;return NULL;}
    FILE *saved=g_out;g_out=out;
    *rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,json,strlen(json),0,',',0);
    g_out=saved;ASSERT_EQ_INT(fclose(out),0,"bulk close");return text;
}
static void r_bulk_routes(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    f.sc.auto_key=AK_UUID;f.sc.max_key=16;f.fs.auto_key=AK_UUID;f.fs.auto_key_schema_snapshot=f.sc;
    int rc=0;char *text=r_bulk_call(&f,"[{\"value\":{\"f0\":\"1234567890123456.1234567890\"}},{\"value\":{\"f2\":\"0.0000000000000000000001\"}}]",&rc);
    ASSERT_EQ_INT(rc,0,"bulk omitted auto keys supported");ASSERT_CONTAINS(text,"\"inserted\":2","both rows inserted");ASSERT_CONTAINS(text,"\"keys\":[","generated keys returned");free(text);
    text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"read auto-key bulk results");ASSERT_CONTAINS(text,"1234567890123456.1234567890","bulk exact constrained value");ASSERT_CONTAINS(text,"0.0000000000000000000001","bulk exact bare value");free(text);
    text=r_bulk_call(&f,"[{\"key\":\"00112233-4455-6677-8899-aabbccddeeff\",\"value\":{}},{\"key\":\"00112233-4455-6677-8899-AABBCCDDEEFF\",\"value\":{}}]",&rc);
    ASSERT_TRUE(rc!=0,"normalized duplicate keys rejected before request");ASSERT_CONTAINS(text,"\"error\"","duplicate explicit error");free(text);c_close(&f);
}

static int r_fetch_page(CFixture *f, const char *format, int offset,
    const char *cursor, char **text) {
    size_t length = 0;
    FILE *out = open_memstream(text, &length);
    ASSERT_NOT_NULL(out, "pagination output stream");
    if (!out) return -1;
    TypedReadOptions options = {
        .kind = TYPED_READ_FETCH, .format = format, .fields = "f0",
        .offset = offset, .limit = 1, .cursor = cursor,
        .budget = 65536, .delimiter = ','
    };
    int rc = typed_read_command(&f->db, &f->fs, &f->sc, &options, out);
    ASSERT_EQ_INT(fclose(out), 0, "pagination output close");
    return rc;
}

static void r_fetch_skip_layout(int packed) {
    CFixture f;
    if (c_open(&f, 0)) { c_close(&f); return; }
    if (!packed) {
        memset(f.ts, 0, sizeof(*f.ts));
        f.ts->typed = 1;
        f.ts->nfields = 1;
        f.ts->nullmap_prefix = 2;
        f.ts->total_size = 6;
        strcpy(f.ts->fields[0].name, "f0");
        f.ts->fields[0].name_len = 2;
        f.ts->fields[0].type = FT_INT;
        f.ts->fields[0].size = 4;
        f.sc.max_value = f.ts->total_size;
        f.fs.auto_key_schema_snapshot = f.sc;
    }
    uint8_t flags = packed ? PKR_FLAGS : SREC_FLAG_NULLMAP;
    if (c_seed(&f, "a", 1, "{\"f0\":7}", flags) ||
        c_seed(&f, "b", 1, "{\"f0\":7}", flags)) {
        c_close(&f); return;
    }
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s/data/kf/000.kf", f.root);
    ASSERT_TRUE(length > 0 && (size_t)length < sizeof(path), "pagination keyfile path");
    if (length < 1 || (size_t)length >= sizeof(path)) { c_close(&f); return; }
    int fd = open(path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "pagination keyfile open");
    if (fd < 0) { c_close(&f); return; }
    struct stat st;
    int rc = fstat(fd, &st);
    ASSERT_EQ_INT(rc, 0, "pagination keyfile size");
    if (rc) { close(fd); c_close(&f); return; }
    SlotcaskKfEntry first = {0}, second = {0};
    off_t first_at = 0;
    int live = 0;
    for (off_t at = SLOTCASK_KF_HDR_SIZE;
         at <= st.st_size - (off_t)sizeof(SlotcaskKfEntry);
         at += sizeof(SlotcaskKfEntry)) {
        SlotcaskKfEntry entry;
        ssize_t got = pread(fd, &entry, sizeof(entry), at);
        if (got != sizeof(entry)) { live = -1; break; }
        if (entry.flag != 1) continue;
        if (!live) { first = entry; first_at = at; }
        else if (live == 1) second = entry;
        live++;
    }
    ASSERT_EQ_INT(live, 2, "two pagination records in traversal order");
    if (live != 2) { close(fd); c_close(&f); return; }
    uint8_t a_hash[16];
    compute_hash_raw("a", 1, a_hash);
    const char *survivor = memcmp(second.hash, a_hash, 16) ? "b" : "a";
    uint32_t unavailable = UINT32_MAX;
    off_t offset_at = first_at + offsetof(SlotcaskKfEntry, offset);
    ssize_t written = pwrite(fd, &unavailable, sizeof(unavailable), offset_at);
    ASSERT_EQ_INT(written, sizeof(unavailable), "inject unavailable skipped location");
    if (written != sizeof(unavailable)) { close(fd); c_close(&f); return; }
    const char *formats[] = {NULL, "rows", "dict", "csv"};
    for (size_t i = 0; i < sizeof(formats) / sizeof(formats[0]); i++) {
        char *text = NULL;
        rc = r_fetch_page(&f, formats[i], 1, NULL, &text);
        ASSERT_EQ_INT(rc, 0, "offset skips segment access");
        char expected[16];
        snprintf(expected, sizeof(expected), formats[i] && !strcmp(formats[i], "csv") ?
                 "%s," : "\"%s\"", survivor);
        ASSERT_CONTAINS(text, expected, "page returns the next live key");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 0, "1", &text);
        ASSERT_EQ_INT(rc, 0, "cursor skips segment access");
        ASSERT_CONTAINS(text, expected, "cursor returns the next live key");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 1, "1", &text);
        ASSERT_EQ_INT(rc, 0, "skip beyond records avoids segment access");
        ASSERT_TRUE(text && !strstr(text, "\"error\""), "empty page is successful");
        free(text); text = NULL;
        rc = r_fetch_page(&f, formats[i], 0, NULL, &text);
        ASSERT_EQ_INT(rc, -1, "returned records still require valid locations");
        ASSERT_TRUE(text && !strncmp(text, "{\"error\":", 9), "returned-record error has no success prefix");
        free(text);
    }
    written = pwrite(fd, &first.offset, sizeof(first.offset), offset_at);
    ASSERT_EQ_INT(written, sizeof(first.offset), "restore injected location");
    ASSERT_EQ_INT(close(fd), 0, "pagination keyfile close");
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db), 0, "pagination takes no writer reservation");
    c_close(&f);
}

static int test_r1_fetch_skip(void) {
    r_fetch_skip_layout(0);
    r_fetch_skip_layout(1);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-r1-fetch-skip", test_r1_fetch_skip)

static int test_r1_routes(void){r_reads();r_computed();r_declarations();r_schema_routes();r_bulk_routes();return t_ctx->failed?1:0;}
TEST_REGISTER("test-r1-routes",test_r1_routes)
