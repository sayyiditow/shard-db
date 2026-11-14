#define _XOPEN_SOURCE 700
/* Darwin: keep the BSD surface (mkdtemp, funopen) visible despite the
   strict-POSIX _XOPEN_SOURCE above; the macro is inert elsewhere. */
#define _DARWIN_C_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} BFixture;
static int b_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void b_close(BFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    free(f->ts);slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,b_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove private fixture");
}
static int b_open(BFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private packed schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:5;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":i==2?"numeric":i==3?"int":"timestamp";
        if(!strncmp(tokens[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else if(!strncmp(tokens[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind private packed schema");if(rc)return -1;
    if(!maximum){ts->fields[3].default_kind=DK_LITERAL;strcpy(ts->fields[3].default_val,"7");
        ts->fields[4].default_kind=DK_AUTO_CREATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema bound");
    f->sc=(Schema){.splits=4,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-packed-bulk-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,4,1,(int)slot);
    f->opened=!rc;ASSERT_EQ_INT(rc,0,"open multi-shard private object");return rc;
}
static int b_call(BFixture *f,const char *data,size_t n,int delim,int strict,char **out){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,data,n,delim,'|',strict);
    g_out=save;if(fclose(fp))return -1;return rc;
}
static int b_json(BFixture *f,const char *s,int strict,char **out){return b_call(f,s,strlen(s),0,strict,out);}
static char *b_get(BFixture *f,const char *key,size_t *n){
    void *bytes=NULL;uint8_t hdr=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&bytes,n,&hdr))return NULL;
    ASSERT_EQ_INT(hdr,PKR_FLAGS,"physical generation is packed");
    char *s=typed_packed_decode(f->ts,bytes,*n,hdr,16*1024*1024);free(bytes);return s;
}
static uint64_t b_pos(BFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return p;
}
static void b_reject(BFixture *f,const char *json){
    size_t n=0,m=0;char *before=b_get(f,"a",&n),*out=NULL;uint64_t pos=b_pos(f),t=0,d=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t,&d),0,"count before rejection");
    ASSERT_EQ_INT(b_json(f,json,0,&out),1,"bad batch/row rejected");
    ASSERT_NOT_NULL(out,"error response exists");if(out){ASSERT_CONTAINS(out,"error","error response");free(out);}
    char *after=b_get(f,"a",&m);ASSERT_EQ_INT(n,m,"OLD actual size unchanged");
    if(before&&after)ASSERT_EQ_STR(after,before,"OLD exact value unchanged");
    ASSERT_TRUE(b_pos(f)==pos,"no segment reservation on rejected input");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"count after rejection");
    ASSERT_TRUE(t==t2&&d==d2,"rejected counts unchanged");free(before);free(after);
}
static void b_routes(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":1234567890123456.1234567890,\"f1\":\"old\",\"f2\":1.0e2},\"b\":{\"f2\":-0.0000000000000000000000000001}}",0,&out),0,"dict across shards");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":2","two fresh insertions");free(out);}
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"dict physical value");char ac[80]={0};
    if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","exact fixed numeric");ASSERT_CONTAINS(s,"\"f2\":100","bare exponent");
        ASSERT_CONTAINS(s,"\"f3\":7","JSON default");char *v=strstr(s,"\"f4\":");if(v)memcpy(ac,v,strcspn(v,",}"));free(s);}
    ASSERT_TRUE(n<256,"small physical row");
    ASSERT_EQ_INT(b_json(&f,"[{\"key\":\"a\",\"value\":{\"f1\":\"a\\u0000b\",\"f4\":1}},{\"key\":\"c\",\"value\":{\"f1\":null}}]",0,&out),0,"array upsert and fresh NULL");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","updates not counted as fresh");free(out);}
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"upsert physical value");
    if(s){ASSERT_CONTAINS(s,"a\\u0000b","embedded NUL varchar");ASSERT_TRUE(ac[0]&&strstr(s,ac),"AC copied from locked OLD");free(s);}
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":\"invalid\"},\"d\":{\"f2\":2}}",1,&out),0,"strict existing skip before constructor");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","strict new committed");ASSERT_CONTAINS(out,"\"skipped\":1","existing is condition skip");free(out);}
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":\"\"},\"e\":{\"f2\":3}}",0,&out),1,"mixed row failure distinct from skip");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","good row commits");ASSERT_CONTAINS(out,"\"errors\":1","bad constructor counted error");ASSERT_CONTAINS(out,"\"skipped\":0","not condition skip");free(out);}
    b_reject(&f,"{\"a\":{},\"z\":{\"f1\":\"\\uZZZZ\"}}");
    b_reject(&f,"{\"a\":{\"f3\":2147483648}}");
    b_reject(&f,"{\"a\":{\"f2\":\"\"}}");
    b_reject(&f,"[{\"key\":\"a\",\"value\":{}}]garbage");
    ASSERT_EQ_INT(b_json(&f,"{\"esc\\u0061ped\":{\"f1\":\"x\"}}",0,&out),0,"escaped dictionary key decoded");free(out);
    s=b_get(&f,"escaped",&n);ASSERT_NOT_NULL(s,"decoded key stored");free(s);
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,f.root,4,1,slot),0,"reopen physical bulk object");f.opened=1;
    s=b_get(&f,"b",&n);ASSERT_NOT_NULL(s,"reopened exact bare value");
    if(s){ASSERT_CONTAINS(s,"-0.0000000000000000000000000001","tiny decimal preserved");free(s);}b_close(&f);
}
static void b_delimited(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    const char data[]="a|1.25|first|2.5|9|1\r\nb|2.5||0|\nc|3|text|4|5|1|ignored\n";
    ASSERT_EQ_INT(b_call(&f,data,sizeof(data)-1,1,0,&out),0,"positional CRLF and ignored surplus");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":3","delimited inserted");free(out);}
    char *s=b_get(&f,"b",&n);ASSERT_NOT_NULL(s,"empty positional physical row");
    if(s){ASSERT_CONTAINS(s,"\"f1\":\"\"","empty varchar present");ASSERT_CONTAINS(s,"\"f3\":0","empty scalar zero; no JSON defaults");free(s);}
    const char binary[]={'a','|','4','|','x',0,'y','|','5','|','6','\n'};
    ASSERT_EQ_INT(b_call(&f,binary,sizeof(binary),1,0,&out),0,"length-delimited embedded NUL");free(out);
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"binary-delimited physical row");if(s){ASSERT_CONTAINS(s,"x\\u0000y","raw varchar is byte-exact");free(s);}
    uint64_t pos=b_pos(&f);const char *bad="a||bad|5|6\n";
    ASSERT_EQ_INT(b_call(&f,bad,strlen(bad),1,0,&out),1,"empty numeric rejects, not zero");
    if(out){ASSERT_CONTAINS(out,"\"errors\":1","delimited encode is error");free(out);}ASSERT_TRUE(b_pos(&f)==pos,"invalid delimiter row no append");
    b_close(&f);
}
static void b_guards(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{}}",0,&out),0,"guard seed");free(out);
    f.sc.auto_key=AK_UUID;b_reject(&f,"{\"a\":{}}");f.sc.auto_key=AK_NONE;
    char path[256];snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata directory");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private indexes directory");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private manifest");
    if(fp){ASSERT_TRUE(fputs("f0\n",fp)>=0,"manifest content");ASSERT_EQ_INT(fclose(fp),0,"manifest close");}
    invalidate_idx_cache(f.root,"obj");
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{}}",0,&out),0,"packed indexed upsert supported");free(out);
    remove(path);invalidate_idx_cache(f.root,"obj");
    f.ts->fields[1].not_null=1;b_reject(&f,"{\"a\":{\"f1\":null}}");f.ts->fields[1].not_null=0;
    SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};const uint8_t legacy[]={1,0};
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"legacy",6,legacy,sizeof(legacy),&o,NULL),0,"legacy generation seed");
    uint64_t pos=b_pos(&f);ASSERT_EQ_INT(b_json(&f,"{\"legacy\":{}}",0,&out),1,"reject legacy OLD");free(out);ASSERT_TRUE(b_pos(&f)==pos,"generation reject no append");
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"guard seed survives");free(s);b_close(&f);
}
static void b_maximum(void){
    BFixture f;if(b_open(&f,1)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_TRUE(f.sc.slot_size>1000000000,"one GiB theoretical schema");
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f127\":\"small\"},\"b\":{\"f128\":\"tiny\"}}",0,&out),0,"large schema small batch");free(out);
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"small packed maximum-schema row");
    ASSERT_TRUE(n<4096,"actual size independent of schema maximum");free(s);
    uint64_t pos=b_pos(&f),total=0,deleted=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"key-only count snapshot");
    const char *key_only="a\norphan\r\nno-newline";
    ASSERT_EQ_INT(b_call(&f,key_only,strlen(key_only),1,0,&out),0,"key-only lines skipped");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":0","key-only lines never insert");free(out);}
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"key-only OLD still readable");
    if(s){ASSERT_CONTAINS(s,"small","key-only line cannot replace OLD");free(s);}
    void *absent=NULL;size_t absent_n=0;
    ASSERT_TRUE(slotcask_get(&f.db,"orphan",6,&absent,&absent_n)!=0,"key-only line cannot create row");free(absent);
    ASSERT_TRUE(b_pos(&f)==pos,"key-only lines never reserve segment bytes");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&t2,&d2),0,"key-only counts after");
    ASSERT_TRUE(total==t2&&deleted==d2,"key-only lines preserve counts");
    const char *explicit_empty="empty|\n";
    ASSERT_EQ_INT(b_call(&f,explicit_empty,strlen(explicit_empty),1,0,&out),0,"explicit empty column still writes");free(out);
    s=b_get(&f,"empty",&n);ASSERT_NOT_NULL(s,"explicit empty row present");
    if(s){ASSERT_CONTAINS(s,"\"f0\":\"\"","explicit empty varchar present");free(s);}
    b_close(&f);
    if(b_open(&f,0)){b_close(&f);return;}
    size_t len=4u*1024*1024;char *json=malloc(len+32);ASSERT_NOT_NULL(json,"4 MiB request");
    if(json){memcpy(json,"{\"a\":{\"f1\":\"",12);memset(json+12,'x',len);memcpy(json+12+len,"\"}}",4);
        ASSERT_EQ_INT(b_json(&f,json,0,&out),0,"maximum varchar bulk insert");free(out);
        s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"maximum varchar physical value");ASSERT_TRUE(n>len&&n<len+256,"actual maximum payload");free(s);free(json);}
    b_close(&f);
}
static int test_typed_bulk_insert_run(void){b_routes();b_delimited();b_guards();b_maximum();return t_ctx->failed?1:0;}
TEST_REGISTER("test-typed-bulk-insert",test_typed_bulk_insert_run)

static int bi_count(const char *v,size_t n,const uint8_t *h,void *p){
    (void)v;(void)n;(void)h;(*(int*)p)++;return 0;
}
static void bi_index(BFixture *f){
    char path[256];const char *parts[]={"obj","obj/indexes","obj/indexes/f1"};
    for(int i=0;i<3;i++){snprintf(path,sizeof(path),"%s/%s",f->root,parts[i]);ASSERT_TRUE(mkdir(path,0700)==0||errno==EEXIST,"private index path");}
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f->root);
    FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private index declaration");
    if(fp){fputs("f1\n",fp);fclose(fp);}invalidate_idx_cache(f->root,"obj");
}
static int bi_hits(BFixture *f,const char *value){
    size_t n=strlen(value);int count=0;
    btree_idx_search(f->root,"obj","f1",f->sc.splits,value,n,bi_count,&count);return count;
}
static int test_typed_bulk_index_admission_run(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return 1;}
    char physical[256];snprintf(physical,sizeof(physical),"%s/obj",f.root);
    ASSERT_EQ_INT(mkdir(physical,0700),0,"physical object directory");
    slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,physical,f.sc.splits,1,f.sc.slot_size),0,"open real physical shape");f.opened=1;
    bi_index(&f);
    char big[545],ok[544],json[4096],*out=NULL;memset(big,'x',544);big[544]=0;memset(ok,'x',543);ok[543]=0;
    snprintf(json,sizeof(json),"{\"a\":{\"f1\":\"%s\"}}",ok);
    ASSERT_EQ_INT(b_json(&f,json,0,&out),0,"544 byte ordered key admitted");free(out);
    ASSERT_EQ_INT(bi_hits(&f,ok),1,"boundary key indexed");
    snprintf(json,sizeof(json),"{\"a\":{\"f1\":\"%s\"}}",big);b_reject(&f,json);
    ASSERT_EQ_INT(bi_hits(&f,ok),1,"rejected NEW keeps OLD index");
    snprintf(json,sizeof(json),"{\"bad\":{\"f1\":\"%s\"},\"good\":{\"f1\":\"small\"}}",big);
    ASSERT_EQ_INT(b_json(&f,json,0,&out),1,"mixed valid and oversized rows");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","mixed good committed");ASSERT_CONTAINS(out,"\"errors\":1","oversized is error");ASSERT_CONTAINS(out,"\"skipped\":0","oversized never skip");free(out);}
    size_t n=0;char *value=b_get(&f,"bad",&n);ASSERT_TRUE(!value,"oversized fresh key absent");free(value);
    ASSERT_EQ_INT(bi_hits(&f,"small"),1,"mixed good indexed");
    size_t used=0;FILE *fp=open_memstream(&out,&used);FILE *save=g_out;g_out=fp;
    snprintf(json,sizeof(json),"{\"a\":{\"f1\":\"%s\"}}",big);
    uint64_t pos=b_pos(&f);ASSERT_EQ_INT(typed_bulk_update_command(f.root,"obj",&f.sc,f.ts,&f.db,json,strlen(json),0,0),1,"packed update oversize rejects");
    g_out=save;fclose(fp);if(out){ASSERT_CONTAINS(out,"\"skipped\":0","packed update error not skipped");ASSERT_CONTAINS(out,"\"errors\":[","packed update reason");free(out);}
    ASSERT_TRUE(b_pos(&f)==pos,"packed update rejection before append");ASSERT_EQ_INT(bi_hits(&f,ok),1,"packed update OLD index intact");
#ifdef TYPED_BULK_INDEX_BRIDGE_API
    /* A local budget proves exact bridge/window lifetimes without mutable
       test-wide configuration or an engine fault-injection switch. */
    TypedBulkMemory memory={.limit=SIZE_MAX};atomic_init(&memory.used,0);
    uint8_t *encoded=NULL;size_t encoded_len=0;
    ASSERT_EQ_INT(typed_insert_alloc(f.ts,NULL,"{\"f1\":\"budget\"}",f.root,"obj",
        &encoded,&encoded_len,NULL,0),0,"encode budget fixture");
    SlotcaskBulkRec record={.key="budget",.klen=6,.value=encoded,.vlen=encoded_len,.hdr2=PKR_FLAGS};
    SlotcaskBulkShardInput input={.recs=&record,.nrecs=1,.kf_shard_id=0};
    char fields[1][256]={"f1"};enum IndexType types[1]={IT_BTREE};
    void *bridge=typed_bulk_index_bridge_create(f.root,"obj",&f.sc,f.ts,fields,types,1,&input,&memory);
    ASSERT_NOT_NULL(bridge,"local-budget bridge");
    if(bridge){
        size_t baseline=atomic_load(&memory.used),active=0;void *window=NULL;
        memory.limit=baseline+64;
        ASSERT_EQ_INT(input.opts.upsert.prepare_window(&record,&active,1,
            input.opts.upsert.bulk_hook_ctx,&window),-1,"window budget failure before allocation");
        ASSERT_TRUE(window==NULL,"failed preparation yields no window owner");
        ASSERT_TRUE(atomic_load(&memory.used)==baseline,"failed prepare restores actual budget");
        memory.limit=SIZE_MAX;
        ASSERT_EQ_INT(input.opts.upsert.prepare_window(&record,&active,1,
            input.opts.upsert.bulk_hook_ctx,&window),0,"prepare within actual budget");
        ASSERT_TRUE(atomic_load(&memory.used)>baseline,"live window charged");
        input.opts.upsert.abort_window(input.opts.upsert.bulk_hook_ctx,window);
        ASSERT_TRUE(atomic_load(&memory.used)==baseline,"abort releases every actual window allocation");
        typed_bulk_index_bridge_free(bridge);
        ASSERT_TRUE(atomic_load(&memory.used)==0,"bridge release restores local budget");
    }
    free(encoded);
#endif
    b_close(&f);return t_ctx->failed?1:0;
}
TEST_REGISTER("test-typed-bulk-index-admission",test_typed_bulk_index_admission_run)
