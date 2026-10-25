#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

typedef struct {
    TypedSchema *ts;
    SlotcaskDb db;
    char root[64];
    int opened;
} OwnFixture;
typedef struct {
    TypedSchema *ts;
    TypedInput inputs[MAX_FIELDS];
    int mode, calls, pre_calls, reject_pre, check_calls, allow;
    size_t fake_len, produced;
    const uint8_t *pending;
    int observed;
} OwnCtx;
static int own_remove(const char *p,const struct stat *st,int kind,struct FTW *f){
    (void)st;(void)kind;(void)f;return remove(p);
}
static void own_close(OwnFixture *f){
    if(f->opened)slotcask_close(&f->db);
    if(f->ts){for(int i=0;i<f->ts->nfields;i++)free_enum_values(&f->ts->fields[i]);free(f->ts);}
    slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,own_remove,16,FTW_DEPTH|FTW_PHYS),0,"own fixture cleanup");
}
static int own_open(OwnFixture *f,int maximum){
    memset(f,0,sizeof(*f));
    f->ts=calloc(1,sizeof(*f->ts));ASSERT_NOT_NULL(f->ts,"private schema");if(!f->ts)return -1;
    f->ts->typed=1;f->ts->nfields=maximum?MAX_FIELDS:2;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<f->ts->nfields;i++){
        tokens[i]=maximum||i?"varchar":"numeric:26,10";
        f->ts->fields[i].type=maximum||i?FT_VARCHAR:FT_NUMERIC;
        snprintf(f->ts->fields[i].name,sizeof(f->ts->fields[i].name),"f%d",i);
        f->ts->fields[i].name_len=(int)strlen(f->ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(f->ts,tokens,(size_t)f->ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind explicit schema");if(rc)return -1;
    size_t bound=0;dec_status st=pkr_slot_bound(&f->ts->packed,1024,&bound);
    ASSERT_EQ_INT(st,DEC_OK,"slot bound");if(st!=DEC_OK)return -1;
    strcpy(f->root,"/tmp/shard-own-hook-XXXXXX");
    char *root=mkdtemp(f->root);ASSERT_NOT_NULL(root,"isolated owning object");if(!root){f->root[0]=0;return -1;}
    slotcask_init(64,64);
    rc=slotcask_open(&f->db,f->root,1,1,(int)bound);f->opened=rc==0;
    ASSERT_EQ_INT(rc,0,"open owning object");return rc;
}
static int own_seed(OwnFixture *f,const char *key){
    uint8_t *b=NULL;size_t n=0;char err[256];
    const char *json=f->ts->nfields==2?"{\"f0\":0,\"f1\":\"old\"}":"{\"f127\":\"tiny\"}";
    int rc=typed_encode_alloc(f->ts,json,1,NULL,NULL,&b,&n,err,sizeof(err));
    ASSERT_EQ_INT(rc,0,"actual seed encoding");if(rc)return -1;
    SlotcaskUpsertOpts o={0};o.record_hdr2=PKR_FLAGS;
    rc=slotcask_upsert_with_hooks(&f->db,-1,key,strlen(key),b,n,&o,NULL);
    ASSERT_EQ_INT(rc,0,"seed physical record");free(b);return rc;
}
static int own_make(const SlotcaskOldRecord *old,uint8_t **out,size_t *n,void *ptr){
    OwnCtx *c=ptr;c->calls++;
    if(c->mode==1){*out=malloc(1);*n=1;errno=EINVAL;return -1;}
    if(c->mode==2){errno=ENOMEM;return -1;}
    if(c->mode==3)return 0;
    if(c->mode==4){*out=malloc(1);*n=c->fake_len;return *out?0:-1;}
    if(c->mode==5){*out=malloc(1);*n=0;return *out?0:-1;}
    char err[256];
    int rc=typed_update_alloc(c->ts,old,c->inputs,0,out,n,err,sizeof(err));
    if(!rc){c->pending=*out;c->produced=*n;}
    return rc;
}
static int own_legacy(const SlotcaskOldRecord *old,uint8_t *out,size_t cap,size_t *n,void *ptr){
    OwnCtx *c=ptr;c->calls++;
    if(!old||old->vlen>cap)return -1;
    if(old->vlen)memcpy(out,old->value,old->vlen);
    *n=old->vlen;return 0;
}
static int own_check(const SlotcaskOldRecord *old,void *ptr){
    OwnCtx *c=ptr;c->check_calls++;return old&&c->allow;
}
static int own_pre(const SlotcaskOldRecord *old,const uint8_t *b,size_t n,int was_update,void *ptr){
    (void)was_update;OwnCtx *c=ptr;c->pre_calls++;
    if(old&&b&&b==c->pending&&n==c->produced)c->observed=1;
    return c->reject_pre;
}
static size_t own_trim(const void *b,size_t n,void *ptr){
    int *calls=ptr;(*calls)++;(void)b;return n;
}
static SlotcaskUpsertOpts own_opts(OwnCtx *c){
    SlotcaskUpsertOpts o={0};o.require_existing=1;o.record_hdr2=PKR_FLAGS;
    o.new_from_old_alloc=own_make;o.new_from_old_alloc_ctx=c;return o;
}
static void own_result_free(SlotcaskUpsertResult *r){free(r->current_value);}
static void own_assert_seed(OwnFixture *f,const char *key){
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f->db,key,strlen(key),&b,&n,&h),0,"OLD remains readable");
    ASSERT_EQ_INT(h,PKR_FLAGS,"OLD generation preserved");
    if(b){
        size_t len=0;char *s=typed_packed_field_text(f->ts,b,n,h,f->ts->nfields==2?1:127,&len);
        ASSERT_NOT_NULL(s,"OLD field text");
        if(s){ASSERT_EQ_STR(s,f->ts->nfields==2?"old":"tiny","OLD preserved on rejection");free(s);}
        free(b);
    }
}
static uint64_t own_segment_size(OwnFixture *f){
    SlotcaskStream *p=&f->db.streams[0];
    pthread_mutex_lock(&p->rotation_lock);
    uint64_t n=((uint64_t)p->active_file_id<<32)|p->reserve_off;
    pthread_mutex_unlock(&p->rotation_lock);return n;
}
static void own_success(void){
    OwnFixture f;if(own_open(&f,0)){own_close(&f);return;}
    if(own_seed(&f,"a")){own_close(&f);return;}
    OwnCtx c={0};c.ts=f.ts;
    c.inputs[0]=(TypedInput){"1234567890123456.1234567890",27,1,0,0};
    c.inputs[1]=(TypedInput){"new",3,1,0,0};
    SlotcaskUpsertOpts o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;
    int trims=0;f.db.trim_ctx=&trims;atomic_store_explicit(&f.db.trim_fn,own_trim,memory_order_release);
    const char ignored[]="ignored";
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,ignored,sizeof(ignored),&o,NULL),0,"owned update");
    ASSERT_EQ_INT(c.calls,1,"constructor exactly once");ASSERT_EQ_INT(trims,0,"owned bytes bypass legacy trimmer");
    ASSERT_EQ_INT(c.pre_calls,1,"precommit once");ASSERT_EQ_INT(c.observed,1,"owned buffer alive through hook");
    atomic_store_explicit(&f.db.trim_fn,NULL,memory_order_release);
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"owned result get");
    ASSERT_EQ_INT(n,c.produced,"stored actual result size");ASSERT_EQ_INT(h,PKR_FLAGS,"owned generation");
    if(b){
        char *s=typed_packed_decode(f.ts,b,n,h,1024);
        ASSERT_NOT_NULL(s,"owned result render");
        if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","exact finance digits");ASSERT_CONTAINS(s,"\"f1\":\"new\"","new varchar");free(s);}
        free(b);
    }
    int slot_size=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    int rc=slotcask_open(&f.db,f.root,1,1,slot_size);f.opened=rc==0;
    ASSERT_EQ_INT(rc,0,"owning record reopens");
    if(!rc){b=NULL;n=0;h=0;ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"reopened owned value");
        ASSERT_EQ_INT(h,PKR_FLAGS,"reopened generation");ASSERT_EQ_INT(n,c.produced,"reopened actual bytes");free(b);}
    own_close(&f);
}
static void own_rejections(void){
    OwnFixture f;if(own_open(&f,0)){own_close(&f);return;}
    if(own_seed(&f,"a")){own_close(&f);return;}
    for(int mode=1;mode<=4;mode++){
        if(own_seed(&f,"a"))break;
        OwnCtx c={0};c.ts=f.ts;c.mode=mode;c.fake_len=(size_t)f.db.slot_size;
        SlotcaskUpsertOpts o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;
        uint64_t total=0,deleted=0,after_total=0,after_deleted=0;
        ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"before rejection counts");
        uint64_t before=own_segment_size(&f);SlotcaskUpsertResult r={0};errno=0;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"owned constructor rejection is hard");
        ASSERT_EQ_INT(errno,mode==2?ENOMEM:mode==4?EFBIG:EINVAL,"constructor failure errno");
        ASSERT_EQ_INT(r.condition_not_met,0,"hard failure is not CAS failure");
        own_result_free(&r);ASSERT_EQ_INT(c.calls,1,"rejecting constructor once");
        ASSERT_EQ_INT(c.pre_calls,0,"reject before precommit");
        ASSERT_EQ_INT(own_segment_size(&f),before,"no segment growth on constructor rejection");
        ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&after_total,&after_deleted),0,"after rejection counts");
        ASSERT_TRUE(total==after_total&&deleted==after_deleted,"counts unchanged on constructor failure");
        own_assert_seed(&f,"a");
    }
    OwnCtx c={0};c.ts=f.ts;SlotcaskUpsertOpts o=own_opts(&c);
    o.check=own_check;o.check_ctx=&c;c.allow=0;SlotcaskUpsertResult r={0};
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-2,"check rejects softly");
    ASSERT_EQ_INT(c.check_calls,1,"check ran once");ASSERT_EQ_INT(c.calls,0,"check rejection precedes allocation");own_result_free(&r);
    o.check=NULL;o.require_existing=1;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"missing",7,NULL,0,&o,&r),-2,"missing OLD soft rejection");
    ASSERT_EQ_INT(c.calls,0,"missing OLD does not allocate");own_result_free(&r);
    o.require_existing=0;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"missing",7,NULL,0,&o,&r),-2,"insert-only constructor needs OLD");
    ASSERT_EQ_INT(c.calls,0,"fresh insert does not construct from absent OLD");own_result_free(&r);
    ASSERT_EQ_INT(slotcask_exists(&f.db,"missing",7),0,"absent key remains absent");
    o.new_from_old=own_legacy;o.new_from_old_ctx=&c;errno=0;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,NULL),-1,"two constructors reject upsert");
    ASSERT_EQ_INT(errno,EINVAL,"upsert conflicting options errno");errno=0;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"z",1,NULL,0,&o,NULL),-1,"two constructors reject insert");
    ASSERT_EQ_INT(errno,EINVAL,"insert conflicting options errno");ASSERT_EQ_INT(c.calls,0,"conflict invokes neither constructor");
    own_assert_seed(&f,"a");
    c.mode=0;c.reject_pre=1;c.inputs[1]=(TypedInput){"reject",6,1,0,0};
    o=own_opts(&c);o.pre_commit=own_pre;o.pre_commit_ctx=&c;memset(&r,0,sizeof(r));
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"precommit refusal remains hard");
    ASSERT_EQ_INT(c.observed,1,"partial window sees live owned buffer");own_result_free(&r);own_assert_seed(&f,"a");
    own_close(&f);
}
static void own_maximum(void){
    OwnFixture f;if(own_open(&f,1)){own_close(&f);return;}
    ASSERT_TRUE(f.db.slot_size>1000000000,"theoretical slot exceeds 1GB");
    if(own_seed(&f,"a")){own_close(&f);return;}
    OwnCtx c={0};c.ts=f.ts;c.inputs[127]=(TypedInput){"longer",6,1,0,0};
    SlotcaskUpsertOpts o=own_opts(&c);
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,NULL),0,"small owned NEW in maximum schema");
    ASSERT_EQ_INT(c.calls,1,"maximum-schema constructor once");ASSERT_TRUE(c.produced<4096,"actual NEW stays compact");
    void *b=NULL;size_t n=0;uint8_t h=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&b,&n,&h),0,"maximum-schema read");ASSERT_EQ_INT(n,c.produced,"maximum-schema actual vlen");
    if(b){TypedRowView v;TypedRowSpan span;
        int rc=typed_row_open(f.ts,b,n,h,&v);ASSERT_EQ_INT(rc,0,"maximum-schema view");
        if(!rc){ASSERT_EQ_INT(typed_row_span(&v,127,&span),0,"maximum-schema changed span");ASSERT_EQ_INT(span.len,6,"new content length");
            if(span.len==6&&span.data)ASSERT_EQ_INT(memcmp(span.data,"longer",6),0,"new bytes");}free(b);}
    own_seed(&f,"a");c.mode=4;c.fake_len=(size_t)SLOTCASK_SEG_MAX_BYTES;
    SlotcaskUpsertResult r={0};errno=0;uint64_t before=own_segment_size(&f);
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,NULL,0,&o,&r),-1,"actual record over segment max rejects");
    ASSERT_EQ_INT(errno,EFBIG,"segment ceiling errno");ASSERT_EQ_INT(r.condition_not_met,0,"segment limit is hard");
    ASSERT_EQ_INT(own_segment_size(&f),before,"segment overflow cannot reserve");own_result_free(&r);own_assert_seed(&f,"a");
    own_close(&f);
}
static void own_empty_and_legacy(void){
    char root[]="/tmp/shard-own-empty-XXXXXX";char *made=mkdtemp(root);
    ASSERT_NOT_NULL(made,"empty fixture");if(!made)return;
    slotcask_init(64,64);SlotcaskDb db;int rc=slotcask_open(&db,root,1,1,128);
    ASSERT_EQ_INT(rc,0,"small legacy object");
    if(!rc){
        SlotcaskUpsertOpts plain={0};ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,"old",3,&plain,NULL),0,"legacy seed");
        OwnCtx c={0};SlotcaskUpsertOpts o={0};o.require_existing=1;o.new_from_old=own_legacy;o.new_from_old_ctx=&c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,NULL,0,&o,NULL),0,"legacy callback still works");
        ASSERT_EQ_INT(c.calls,1,"legacy callback once");
        c.mode=5;c.calls=0;o.new_from_old=NULL;o.new_from_old_alloc=own_make;o.new_from_old_alloc_ctx=&c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,NULL,0,&o,NULL),0,"allocated empty value succeeds");
        ASSERT_EQ_INT(c.calls,1,"empty constructor once");
        void *b=NULL;size_t n=99;uint8_t h=99;ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&b,&n,&h),0,"empty get");
        ASSERT_EQ_INT(n,0,"empty length exact");ASSERT_EQ_INT(h,0,"empty legacy generation");free(b);slotcask_close(&db);
    }
    slotcask_shutdown();ASSERT_EQ_INT(nftw(root,own_remove,16,FTW_DEPTH|FTW_PHYS),0,"empty fixture cleanup");
}
static int test_owning_write_hooks_run(void){
    own_success();own_rejections();own_maximum();own_empty_and_legacy();
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-owning-write-hooks",test_owning_write_hooks_run)
