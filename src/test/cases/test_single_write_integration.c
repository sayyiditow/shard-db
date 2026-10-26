#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include "single_write_test.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <pthread.h>

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} WFixture;
static int w_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void w_close(WFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    if(f->ts){for(int i=0;i<f->ts->nfields;i++)free_enum_values(&f->ts->fields[i]);free(f->ts);}
    slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,w_remove,16,FTW_DEPTH|FTW_PHYS),0,"fixture removed");
}
static int w_open(WFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":i==2?"numeric":i==3?"int":"timestamp";
        if(!strncmp(tokens[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else if(!strncmp(tokens[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind packed only in fixture");if(rc)return -1;
    if(!maximum){ts->fields[3].default_kind=DK_LITERAL;strcpy(ts->fields[3].default_val,"7");
        ts->fields[4].default_kind=DK_AUTO_CREATE;ts->fields[5].default_kind=DK_AUTO_UPDATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema slot bound");
    f->sc=(Schema){.splits=1,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-single-write-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,1,1,(int)slot);
    f->opened=rc==0;ASSERT_EQ_INT(rc,0,"open private physical object");return rc;
}
static int w_call(WFixture *f,int update,const char *key,const char *json,
    const char *condition,int flag,int ret,char **text){
    size_t n=0;*text=NULL;FILE *mem=open_memstream(text,&n);if(!mem)return -1;
    FILE *save=g_out;g_out=mem;
    int rc=update?shard_test_single_update(f->root,"obj",key,strlen(key),json,condition,flag,ret,&f->sc,f->ts,&f->db):
        shard_test_single_insert(f->root,"obj",key,strlen(key),json,condition,flag,ret,&f->sc,f->ts,&f->db);
    g_out=save;if(fclose(mem))return -1;return rc;
}
static char *w_get(WFixture *f,const char *key,size_t *n){
    void *b=NULL;uint8_t h=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&b,n,&h))return NULL;
    ASSERT_EQ_INT(h,PKR_FLAGS,"persisted packed generation");
    char *s=typed_packed_decode(f->ts,b,*n,h,16*1024*1024);free(b);return s;
}
static uint64_t w_pos(WFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t n=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return n;
}
static void w_assert_reject(WFixture *f,int update,const char *json,const char *condition){
    size_t n=0,after_n=0;char *old=w_get(f,"a",&n),*out=NULL;
    uint64_t pos=w_pos(f),total=0,deleted=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&total,&deleted),0,"before reject counts");
    ASSERT_EQ_INT(w_call(f,update,"a",json,condition,0,1,&out),1,"invalid write rejected");
    ASSERT_NOT_NULL(out,"error response");if(out){ASSERT_CONTAINS(out,"\"error\"","error JSON");free(out);}
    char *now=w_get(f,"a",&after_n);
    ASSERT_EQ_INT(n,after_n,"actual OLD length preserved");
    if(old&&now)ASSERT_EQ_STR(now,old,"OLD fields preserved");
    ASSERT_TRUE(w_pos(f)==pos,"no segment reservation on rejection");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"after reject counts");
    ASSERT_TRUE(total==t2&&deleted==d2,"counts unchanged");free(old);free(now);
}
static void w_observe(int locked,void *p){*(int *)p=locked?1:-1;}
static void w_routes(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f1\":\"old\",\"f0\":1234567890123456.1234567890,\"f2\":1.0e2,\"f4\":1}",NULL,0,1,&out),0,"single actual-size insert");
    if(out){ASSERT_CONTAINS(out,"1234567890123456.1234567890","response exact numeric");
        ASSERT_CONTAINS(out,"\"f2\":100","bare exponent normalized");ASSERT_CONTAINS(out,"\"f3\":7","literal default");free(out);}
    size_t n=0;char *before=w_get(&f,"a",&n);ASSERT_NOT_NULL(before,"physical insert");
    if(!before){w_close(&f);return;}
    ASSERT_TRUE(n<256,"tiny actual value under multi-MiB schema");
    ASSERT_TRUE(!strstr(before,"\"f4\":1,"),"client cannot set auto_create");
    char *ac=strstr(before,"\"f4\":");char ac_copy[80]={0};
    if(ac){size_t k=strcspn(ac,",}");memcpy(ac_copy,ac,k);}
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f0\":2,\"f1\":\"replaced\",\"f4\":2}",NULL,0,1,&out),0,"upsert existing constructor");
    if(out){ASSERT_CONTAINS(out,ac_copy,"auto_create carried from locked OLD");free(out);}
    int observed=0;slotcask_test_set_after_old_hook(w_observe,&observed);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f2\":-0.0000000000000000000000000001,\"f1\":\"longer content\",\"f3\":8}",NULL,0,1,&out),0,"partial update grows actual bytes");
    slotcask_test_set_after_old_hook(NULL,NULL);
    ASSERT_EQ_INT(observed,1,"update constructor under write lock");
    if(out){ASSERT_CONTAINS(out,"\"f0\":2.0000000000","omitted value preserved");
        ASSERT_CONTAINS(out,"-0.0000000000000000000000000001","bare precision survives");ASSERT_CONTAINS(out,ac_copy,"partial preserves AC");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":null,\"f3\":9,\"f3\":10}",NULL,0,1,&out),0,"NULL and duplicate patch");
    if(out){ASSERT_CONTAINS(out,"\"f1\":null","explicit null");ASSERT_CONTAINS(out,"\"f3\":10","last valid duplicate");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":\"a\\u0000b\"}",NULL,0,1,&out),0,"embedded NUL decoded once");
    if(out){ASSERT_CONTAINS(out,"a\\u0000b","embedded NUL byte span");free(out);}
    w_assert_reject(&f,0,"{\"f0\":\"\"}",NULL);
    w_assert_reject(&f,1,"{\"f0\":\"\"}",NULL);
    w_assert_reject(&f,1,"{\"f3\":{\"$incr\":1}}",NULL);
    w_assert_reject(&f,1,"{\"f3\":32768000000}",NULL);
    w_assert_reject(&f,1,"{\"f1\":\"\\uZZZZ\"}",NULL);
    w_assert_reject(&f,1,"{\"f3\":1}","[]");
    f.ts->fields[1].not_null=1;w_assert_reject(&f,1,"{\"f1\":null}",NULL);f.ts->fields[1].not_null=0;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,1,0,&out),1,"strict insert duplicate rejected");
    if(out){ASSERT_CONTAINS(out,"condition_not_met","duplicate response");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"absent","{}",NULL,0,0,&out),1,"update requires OLD");free(out);
    uint64_t pos=w_pos(&f);ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,1,0,&out),0,"dry run existing");free(out);
    ASSERT_TRUE(w_pos(&f)==pos,"dry run no append");
    char *final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"final physical row");
    if(final){ASSERT_CONTAINS(final,"\"f3\":10","last successful fixed field");free(final);}
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,f.root,1,1,slot),0,"reopen actual rows");f.opened=1;
    final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"reopened physical row");free(final);free(before);w_close(&f);
}
static void w_mismatch(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};
    const uint8_t raw[]={1,0,0};ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,raw,sizeof(raw),&o,NULL),0,"legacy raw seed");
    uint64_t p=w_pos(&f);char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,0,1,&out),1,"packed update rejects legacy OLD");free(out);
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),1,"packed upsert rejects legacy OLD");free(out);
    ASSERT_TRUE(w_pos(&f)==p,"generation failures before append");w_close(&f);
}
static void w_maximum(void){
    WFixture f;if(w_open(&f,1)){w_close(&f);return;}
    ASSERT_TRUE(f.sc.slot_size>1000000000,"maximum schema bound over one GiB");char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f127\":\"tiny\"}",NULL,0,1,&out),0,"max schema tiny single insert");free(out);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f128\":\"small\"}",NULL,0,1,&out),0,"max schema tiny update");free(out);
    char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"max schema row");if(s){ASSERT_CONTAINS(s,"tiny","omitted field survives");ASSERT_CONTAINS(s,"small","changed field");free(s);}
    ASSERT_TRUE(n<4096,"physical size independent of schema maximum");w_close(&f);
}
static int w_fresh(const SlotcaskOldRecord *old,uint8_t **b,size_t *n,void *p){
    int *calls=p;(*calls)++;if(old)return -1;*b=malloc(1);if(!*b)return -1;**b=42;*n=1;return 0;
}
static void w_transport(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}int calls=0;SlotcaskUpsertOpts o={0};
    o.new_from_old_alloc=w_fresh;o.new_from_old_alloc_ctx=&calls;o.new_from_old_alloc_on_insert=1;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"fresh",5,NULL,0,&o,NULL),0,"explicit fresh owning callback");
    ASSERT_EQ_INT(calls,1,"fresh constructor once");
    o.require_existing=1;ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"none",4,NULL,0,&o,NULL),-2,"require-existing precedes constructor");
    ASSERT_EQ_INT(calls,1,"missing target no constructor");
    o.require_existing=0;o.new_from_old_alloc=NULL;errno=0;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"bad",3,NULL,0,&o,NULL),-1,"flag alone rejects");ASSERT_EQ_INT(errno,EINVAL,"invalid option errno");
    w_close(&f);
}
static void w_defaults_guards(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    /* Variable-width auto_create must be completed before NOT NULL validation. */
    f.ts->fields[1].default_kind=DK_AUTO_CREATE;f.ts->fields[1].not_null=1;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),0,"NOT NULL varchar auto_create");free(out);
    size_t n=0;char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"variable auto_create stored");free(s);
    f.ts->fields[0].default_kind=DK_LITERAL;strcpy(f.ts->fields[0].default_val,"");
    w_assert_reject(&f,0,"{}",NULL);f.ts->fields[0].default_kind=DK_NONE;
    char path[256];snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata directory");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private index directory");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private index manifest");
    if(fp){ASSERT_TRUE(fputs("f0\n",fp)>=0,"index manifest write");ASSERT_EQ_INT(fclose(fp),0,"index manifest close");}
    invalidate_idx_cache(f.root,"obj");w_assert_reject(&f,0,"{}",NULL);w_assert_reject(&f,1,"{}",NULL);
    ASSERT_EQ_INT(remove(path),0,"remove private index declaration");invalidate_idx_cache(f.root,"obj");
    void *bytes=NULL;uint8_t hdr=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&bytes,&n,&hdr),0,"packed seed for reverse mismatch");
    /* A separate immutable copy represents a legacy caller; no cache alteration. */
    TypedSchema *legacy=malloc(sizeof(*legacy));ASSERT_NOT_NULL(legacy,"private legacy schema copy");
    if(legacy){memset(legacy,0,sizeof(*legacy));legacy->typed=1;legacy->nfields=1;
        legacy->nullmap_prefix=2;legacy->total_size=6;
        legacy->fields[0].type=FT_INT;legacy->fields[0].size=4;
        strcpy(legacy->fields[0].name,"f0");legacy->fields[0].name_len=2;
        TypedSchema *saved=f.ts;f.ts=legacy;uint64_t pos=w_pos(&f);
        ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,0,1,&out),1,"legacy schema rejects packed OLD");free(out);
        ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),1,"legacy upsert rejects packed OLD");free(out);
        ASSERT_TRUE(w_pos(&f)==pos,"reverse mismatch no append");f.ts=saved;free(legacy);
        /* The legacy copy was never published; remove its borrowed trim binding. */
        atomic_store_explicit(&f.db.trim_fn,NULL,memory_order_release);f.db.trim_ctx=NULL;
    }
    free(bytes);w_close(&f);
}
static void w_large(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}
    const size_t length=4u*1024*1024;char *json=malloc(length+32),*out=NULL;
    ASSERT_NOT_NULL(json,"maximum varchar request");if(!json){w_close(&f);return;}
    memcpy(json,"{\"f1\":\"",7);memset(json+7,'x',length);memcpy(json+7+length,"\"}",3);
    ASSERT_EQ_INT(w_call(&f,0,"large",json,NULL,0,0,&out),0,"full 4 MiB single insert");free(out);
    size_t n=0;char *text=w_get(&f,"large",&n);ASSERT_NOT_NULL(text,"full large physical row");
    ASSERT_TRUE(n>length&&n<length+256,"4 MiB actual bytes plus bounded overhead");free(text);
    ASSERT_EQ_INT(w_call(&f,1,"large","{\"f1\":\"\"}",NULL,0,1,&out),0,"large-to-empty update");
    if(out){ASSERT_CONTAINS(out,"\"f1\":\"\"","returned empty distinct from NULL");free(out);}
    text=w_get(&f,"large",&n);ASSERT_NOT_NULL(text,"shrunk row physical");ASSERT_TRUE(n<256,"actual shrinking tail");free(text);
    free(json);w_close(&f);
}
typedef struct {pthread_mutex_t lock;pthread_cond_t cond;int ready,go;} WBarrier;
static void w_wait(WBarrier *b){pthread_mutex_lock(&b->lock);b->ready++;
    pthread_cond_broadcast(&b->cond);while(!b->go)pthread_cond_wait(&b->cond,&b->lock);
    pthread_mutex_unlock(&b->lock);}
typedef struct {WFixture *f;WBarrier *barrier;const char *json;char *out;int rc;ShardDb *owner;} WThread;
static void *w_thread(void *p){WThread *t=p;g_db=t->owner;w_wait(t->barrier);
    t->rc=w_call(t->f,1,"a",t->json,NULL,0,1,&t->out);g_db=NULL;return NULL;}
static void w_concurrent(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,0,&out),0,"concurrent seed");free(out);
    WBarrier barrier={PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0,0};
    WThread a={&f,&barrier,"{\"f1\":\"writer-a\"}",NULL,-1,g_db},b={&f,&barrier,"{\"f3\":123}",NULL,-1,g_db};pthread_t ta,tb;
    int ra=pthread_create(&ta,NULL,w_thread,&a),rb=pthread_create(&tb,NULL,w_thread,&b);
    /* Thread creation failure is fatal: a missing participant cannot be joined safely. */
    if(ra||rb){fprintf(stderr,"cannot start private writer pair\n");abort();}
    pthread_mutex_lock(&barrier.lock);while(barrier.ready<2)pthread_cond_wait(&barrier.cond,&barrier.lock);
    barrier.go=1;pthread_cond_broadcast(&barrier.cond);pthread_mutex_unlock(&barrier.lock);
    pthread_join(ta,NULL);pthread_join(tb,NULL);pthread_cond_destroy(&barrier.cond);pthread_mutex_destroy(&barrier.lock);
    ASSERT_EQ_INT(a.rc,0,"first concurrent partial update");ASSERT_EQ_INT(b.rc,0,"second concurrent partial update");free(a.out);free(b.out);
    size_t n=0;char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"concurrent final row");
    if(s){ASSERT_CONTAINS(s,"writer-a","disjoint varchar patch retained");ASSERT_CONTAINS(s,"\"f3\":123","disjoint fixed patch retained");free(s);}w_close(&f);
}

static void w_legacy_render(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}
    free(f.ts);f.ts=calloc(1,sizeof(*f.ts));ASSERT_NOT_NULL(f.ts,"private legacy render schema");
    if(!f.ts){w_close(&f);return;}
    TypedSchema *ts=f.ts;ts->typed=1;ts->nfields=2;ts->nullmap_prefix=2;ts->total_size=1029;
    TypedField *v=&ts->fields[0],*e=&ts->fields[1];
    v->type=FT_VARCHAR;v->size=1026;v->offset=0;strcpy(v->name,"f0");v->name_len=2;
    e->type=FT_ENUM;e->size=1;e->offset=1026;e->enum_width=1;e->n_enum_values=1;
    strcpy(e->name,"f1");e->name_len=2;e->enum_values=calloc(1,sizeof(char*));
    ASSERT_NOT_NULL(e->enum_values,"legacy enum catalog");if(!e->enum_values){w_close(&f);return;}
    e->enum_values[0]=malloc(1001);ASSERT_NOT_NULL(e->enum_values[0],"long enum value");
    if(!e->enum_values[0]){w_close(&f);return;}
    memset(e->enum_values[0],'z',1000);e->enum_values[0][1000]=0;
    uint8_t bytes[1029]={1,0};bytes[2]=3;bytes[3]=232;memset(bytes+4,'v',1000);
    char *decoded=typed_decode(ts,bytes,sizeof(bytes),SREC_FLAG_NULLMAP);
    ASSERT_NOT_NULL(decoded,"legacy complete decode");
    if(decoded){ASSERT_CONTAINS(decoded,e->enum_values[0],"long enum is not silently omitted");
        ASSERT_TRUE(strlen(decoded)>2010&&decoded[strlen(decoded)-1]=='}',"complete legacy JSON");free(decoded);}
    char json[2048];snprintf(json,sizeof(json),"{\"f0\":\"ok\",\"f1\":\"%s\"}",e->enum_values[0]);
    f.sc.max_value=ts->total_size;char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"legacy",json,NULL,0,1,&out),0,"legacy insert with long enum return_record");
    if(out){ASSERT_CONTAINS(out,e->enum_values[0],"insert returns full enum");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"legacy","{\"f0\":\"changed\"}",NULL,0,1,&out),0,"legacy update preserves long enum");
    if(out){ASSERT_CONTAINS(out,e->enum_values[0],"update returns full enum");free(out);}
    w_close(&f);
}

static int test_single_write_integration_run(void){
    w_legacy_render();w_transport();w_routes();w_mismatch();w_maximum();w_defaults_guards();w_large();w_concurrent();return t_ctx->failed?1:0;
}
TEST_REGISTER("test-single-write-integration",test_single_write_integration_run)
