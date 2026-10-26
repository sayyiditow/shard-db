#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} UFixture;
static int u_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void u_close(UFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    free(f->ts);slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,u_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove private fixture");
}
static int u_open(UFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private packed schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;
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
        ts->fields[4].default_kind=DK_AUTO_CREATE;ts->fields[5].default_kind=DK_AUTO_UPDATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema bound");
    f->sc=(Schema){.splits=4,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-packed-bulk-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,4,1,(int)slot);
    f->opened=!rc;ASSERT_EQ_INT(rc,0,"open multi-shard private object");return rc;
}
static int u_call(UFixture *f,const char *data,size_t n,int delim,char separator,char **out){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_update_command(f->root,"obj",&f->sc,f->ts,&f->db,data,n,delim,separator);
    g_out=save;if(fclose(fp))return -1;return rc;
}
static int u_json(UFixture *f,const char *s,char **out){return u_call(f,s,strlen(s),0,0,out);}
static int u_seed(UFixture *f,const char *json){
    char *out=NULL;size_t n=0;FILE *fp=open_memstream(&out,&n);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,json,strlen(json),0,0,0);
    g_out=save;fclose(fp);free(out);return rc;
}
static int u_source(UFixture *f,const char *source,int file,char **out){
    size_t n=0;*out=NULL;FILE *fp=open_memstream(out,&n);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_update_source(f->root,"obj",&f->sc,f->ts,&f->db,source,file);
    g_out=save;fclose(fp);return rc;
}
static char *u_get(UFixture *f,const char *key,size_t *n){
    void *bytes=NULL;uint8_t hdr=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&bytes,n,&hdr))return NULL;
    ASSERT_EQ_INT(hdr,PKR_FLAGS,"physical generation is packed");
    char *s=typed_packed_decode(f->ts,bytes,*n,hdr,16*1024*1024);free(bytes);return s;
}
static uint64_t u_pos(UFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return p;
}
static void u_reject_input(UFixture *f,const char *data,int delim,char sep){
    size_t n=0,m=0;char *before=u_get(f,"a",&n),*out=NULL;uint64_t pos=u_pos(f),t=0,d=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t,&d),0,"count before rejection");
    ASSERT_EQ_INT(u_call(f,data,strlen(data),delim,sep,&out),1,"bad batch/row rejected");
    ASSERT_NOT_NULL(out,"error response exists");if(out){ASSERT_CONTAINS(out,"error","error response");free(out);}
    char *after=u_get(f,"a",&m);ASSERT_EQ_INT(n,m,"OLD actual size unchanged");
    if(before&&after)ASSERT_EQ_STR(after,before,"OLD exact value unchanged");
    ASSERT_TRUE(u_pos(f)==pos,"no segment reservation on rejected input");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"count after rejection");
    ASSERT_TRUE(t==t2&&d==d2,"rejected counts unchanged");free(before);free(after);
}
static void u_reject(UFixture *f,const char *json){u_reject_input(f,json,0,0);}

static void u_routes(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f0\":1234567890123456.1234567890,\"f1\":\"OLD\",\"f2\":1e-26,\"f3\":10},\"b\":{\"f3\":20}}"),0,"seed physical packed rows");
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"seed readable");char ac[80]={0};
    if(s){char *p=strstr(s,"\"f4\":");if(p)memcpy(ac,p,strcspn(p,",}"));free(s);}
    uint64_t total=0,deleted=0,t2=0,d2=0;ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"counts before updates");
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f1\":\"changed\",\"f5\":1},\"b\":{\"f3\":21},\"missing\":{\"f3\":3}}",&out),0,"dict partial updates and missing key");
    if(out){ASSERT_CONTAINS(out,"\"matched\":3","matched input shape");ASSERT_CONTAINS(out,"\"updated\":2","only existing updated");ASSERT_CONTAINS(out,"\"skipped\":1","missing key skipped");free(out);}
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"partial result");
    if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","omitted exact numeric preserved");ASSERT_CONTAINS(s,"0.00000000000000000000000001","omitted bare numeric preserved");ASSERT_CONTAINS(s,"\"f3\":10","no literal default rerun");ASSERT_TRUE(ac[0]&&strstr(s,ac),"omitted auto_create preserved");ASSERT_TRUE(!strstr(s,"\"f5\":1}"),"auto_update stamped over client input");free(s);}
    ASSERT_EQ_INT(u_json(&f,"[{\"key\":\"a\",\"value\":{\"f1\":null,\"f0\":\"1.25\"}},{\"key\":\"b\",\"value\":{\"f1\":\"a\\u0000b\"}}]",&out),0,"array exact NULL and binary string patches");free(out);
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"NULL physical row");if(s){ASSERT_CONTAINS(s,"\"f1\":null","explicit NULL");ASSERT_CONTAINS(s,"1.2500000000","exact typmod output");free(s);}
    s=u_get(&f,"b",&n);ASSERT_NOT_NULL(s,"binary physical row");if(s){ASSERT_CONTAINS(s,"a\\u0000b","decoded embedded NUL");free(s);}
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f0\":\"\"},\"b\":{\"f3\":22}}",&out),1,"mixed invalid row and valid peer");
    if(out){ASSERT_CONTAINS(out,"\"matched\":2","bad scalar remains matched");ASSERT_CONTAINS(out,"\"updated\":1","good peer commits");ASSERT_CONTAINS(out,"\"skipped\":1","bad row skipped");ASSERT_CONTAINS(out,"\"errors\":[","bad row distinguished from missing");ASSERT_CONTAINS(out,"\"key\":\"a\"","error identifies row");free(out);}
    u_reject(&f,"{\"a\":{\"f1\":\"duplicate patch\"},\"\\u0061\":{\"f3\":999}}");
    u_reject(&f,"{\"a\":{\"f3\":2147483648}}");
    u_reject(&f,"{\"a\":{\"f3\":{\"$incr\":1}}}");
    u_reject(&f,"[{\"key\":\"a\",\"value\":{}},{\"key\":\"\\u0061\",\"value\":null}]");
    u_reject(&f,"[{\"key\":\"a\",\"value\":{\"f3\":11},\"if\":[]},{\"key\":\"b\",\"value\":{\"f3\":23}}]");
    u_reject(&f,"{\"a\":{\"f1\":\"ok\"},\"b\":{\"f1\":\"\\uZZZZ\"}}");
    f.ts->fields[1].not_null=1;u_reject(&f,"{\"a\":{\"f1\":null}}");f.ts->fields[1].not_null=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&t2,&d2),0,"counts after updates");ASSERT_TRUE(total==t2&&deleted==d2,"updates never insert/delete");
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;ASSERT_EQ_INT(slotcask_open(&f.db,f.root,4,1,slot),0,"reopen updated rows");f.opened=1;
    s=u_get(&f,"b",&n);ASSERT_NOT_NULL(s,"reopened updated row");if(s){ASSERT_CONTAINS(s,"\"f3\":22","valid peer durable");free(s);}u_close(&f);
}
static void u_delimited(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f0\":1.5,\"f1\":\"OLD\",\"f2\":2.5,\"f3\":9}}"),0,"positional seed");
    const char *data="a,,NEW,,13,\r\nmissing,3\nkey-only\n";
    ASSERT_EQ_INT(u_call(&f,data,strlen(data),1,',',&out),0,"positional blanks preserve fields");
    if(out){ASSERT_CONTAINS(out,"\"matched\":2","key-only line ignored");ASSERT_CONTAINS(out,"\"updated\":1","one existing row");ASSERT_CONTAINS(out,"\"skipped\":1","one missing target");free(out);}
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"positional row");
    if(s){ASSERT_CONTAINS(s,"1.5000000000","blank numeric unchanged");ASSERT_CONTAINS(s,"\"f2\":2.5","omitted numeric unchanged");ASSERT_CONTAINS(s,"\"f1\":\"NEW\"","nonempty cell patched");ASSERT_CONTAINS(s,"\"f3\":13","integer patched");free(s);}
    const char binary[]={'a','|','|','x',0,'y','|','|','1','4','\n'};
    ASSERT_EQ_INT(u_call(&f,binary,sizeof(binary),1,'|',&out),0,"bounded binary positional span");free(out);
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"binary positional result");if(s){ASSERT_CONTAINS(s,"x\\u0000y","raw bytes preserved");free(s);}
    u_reject_input(&f,"a,,duplicate patch\na,,,,999\n",1,',');
    uint64_t pos=u_pos(&f);const char *bad="a,invalid\n";
    ASSERT_EQ_INT(u_call(&f,bad,strlen(bad),1,',',&out),1,"bad nonempty numeric rejected");free(out);ASSERT_TRUE(u_pos(&f)==pos,"bad positional row no append");
    u_close(&f);
}
static void u_files_guards(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f3\":9}}"),0,"file seed");
    char path[256];snprintf(path,sizeof(path),"%s/patch.json",f.root);FILE *fp=fopen(path,"wb");ASSERT_NOT_NULL(fp,"file input");
    if(fp){ASSERT_TRUE(fputs("{\"a\":{\"f1\":\"from file\"}}",fp)>=0,"file write");ASSERT_EQ_INT(fclose(fp),0,"file close");}
    ASSERT_EQ_INT(u_source(&f,path,1,&out),0,"file source uses same packed core");free(out);
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"file update physical");if(s){ASSERT_CONTAINS(s,"from file","file input applied");free(s);}
    ASSERT_EQ_INT(u_source(&f,"{\"a\":{\"f3\":10}}",0,&out),0,"inline source shares command");free(out);
    uint64_t pos=u_pos(&f);remove(path);ASSERT_EQ_INT(u_source(&f,path,1,&out),1,"missing file rejects");free(out);ASSERT_TRUE(u_pos(&f)==pos,"read failure no append");
    f.sc.auto_key=AK_UUID;u_reject(&f,"{\"a\":{}}");f.sc.auto_key=AK_NONE;
    snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata dir");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private index dir");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"manifest");if(fp){fputs("f0\n",fp);fclose(fp);}
    invalidate_idx_cache(f.root,"obj");u_reject(&f,"{\"a\":{}}");remove(path);invalidate_idx_cache(f.root,"obj");
    SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};const uint8_t bytes[]={1,0};ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"legacy",6,bytes,sizeof(bytes),&o,NULL),0,"legacy OLD seed");
    pos=u_pos(&f);ASSERT_EQ_INT(u_json(&f,"{\"legacy\":{}}",&out),1,"legacy OLD refuses packed patch");free(out);ASSERT_TRUE(u_pos(&f)==pos,"generation rejection no append");u_close(&f);
}
static void u_maximum(void){
    UFixture f;if(u_open(&f,1)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_TRUE(f.sc.slot_size>1000000000,"max schema over one GiB");
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f127\":\"OLD\",\"f128\":\"tiny\"}}"),0,"tiny max-schema seed");
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f128\":\"changed\"}}",&out),0,"tiny max-schema partial update");free(out);
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"tiny max-schema result");if(s){ASSERT_CONTAINS(s,"OLD","omitted max-schema field kept");ASSERT_CONTAINS(s,"changed","max-schema changed field");free(s);}ASSERT_TRUE(n<4096,"actual size independent of schema bound");u_close(&f);
    if(u_open(&f,0)){u_close(&f);return;}ASSERT_EQ_INT(u_seed(&f,"{\"a\":{}}"),0,"large patch seed");
    size_t len=4u*1024*1024;char *json=malloc(len+32);ASSERT_NOT_NULL(json,"4 MiB patch");
    if(json){memcpy(json,"{\"a\":{\"f1\":\"",12);memset(json+12,'x',len);memcpy(json+12+len,"\"}}",4);
        ASSERT_EQ_INT(u_json(&f,json,&out),0,"grow to full varchar limit");free(out);
        s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"large result");ASSERT_TRUE(n>len&&n<len+256,"large actual record");free(s);free(json);}
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f1\":\"\"}}",&out),0,"shrink with explicit empty JSON");free(out);s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"shrunk result");if(s){ASSERT_CONTAINS(s,"\"f1\":\"\"","empty not NULL");free(s);}ASSERT_TRUE(n<256,"shrunk actual size");u_close(&f);
}
typedef struct {pthread_mutex_t lock;pthread_cond_t cond;int ready,go;} UBarrier;
typedef struct {UFixture *f;UBarrier *barrier;ShardDb *owner;const char *json;int rc;char *out;} UThread;
static void *u_thread(void *arg){
    UThread *t=arg;g_db=t->owner;pthread_mutex_lock(&t->barrier->lock);t->barrier->ready++;
    pthread_cond_broadcast(&t->barrier->cond);while(!t->barrier->go)pthread_cond_wait(&t->barrier->cond,&t->barrier->lock);
    pthread_mutex_unlock(&t->barrier->lock);t->rc=u_json(t->f,t->json,&t->out);g_db=NULL;return NULL;
}
static void u_concurrent(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}ASSERT_EQ_INT(u_seed(&f,"{\"a\":{}}"),0,"concurrent seed");
    UBarrier b={PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0,0};
    UThread a={&f,&b,g_db,"{\"a\":{\"f1\":\"writer\"}}",-1,NULL},c={&f,&b,g_db,"{\"a\":{\"f3\":123}}",-1,NULL};pthread_t ta,tc;
    if(pthread_create(&ta,NULL,u_thread,&a)||pthread_create(&tc,NULL,u_thread,&c)){fprintf(stderr,"cannot start update threads\n");abort();}
    pthread_mutex_lock(&b.lock);while(b.ready<2)pthread_cond_wait(&b.cond,&b.lock);b.go=1;pthread_cond_broadcast(&b.cond);pthread_mutex_unlock(&b.lock);
    pthread_join(ta,NULL);pthread_join(tc,NULL);pthread_cond_destroy(&b.cond);pthread_mutex_destroy(&b.lock);
    ASSERT_EQ_INT(a.rc,0,"first bulk patch commits");ASSERT_EQ_INT(c.rc,0,"second bulk patch commits");free(a.out);free(c.out);
    size_t n=0;char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"concurrent result");if(s){ASSERT_CONTAINS(s,"writer","first disjoint patch kept");ASSERT_CONTAINS(s,"\"f3\":123","second disjoint patch kept");free(s);}u_close(&f);
}
static int test_typed_bulk_update_run(void){u_routes();u_delimited();u_files_guards();u_maximum();u_concurrent();return t_ctx->failed?1:0;}
TEST_REGISTER("test-typed-bulk-update",test_typed_bulk_update_run)
