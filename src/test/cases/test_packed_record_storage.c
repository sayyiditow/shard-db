#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "seg_scan_varlen.h"
#include "packed_record.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftw.h>
#include <errno.h>

static int remove_entry(const char *path, const struct stat *st, int kind,
                         struct FTW *state) {
    (void)st; (void)kind; (void)state; return remove(path);
}
typedef struct { const PkrSchema *schema; int rows, bad; } Walk;
static int check_row(const uint8_t hash[16], const void *key, size_t klen,
                      const void *value, size_t vlen, uint8_t hdr2, void *ctx) {
    (void)hash;(void)key;(void)klen;
    Walk *w=ctx;PkrView v;
    if(hdr2!=PKR_FLAGS||pkr_view(w->schema,value,vlen,hdr2,&v)!=DEC_OK)w->bad++;
    w->rows++;return 0;
}
/* This independent regression exercises the old engine before any codec
   implementation exists: bit preservation must really be RED, not hidden
   behind an unimplemented schema prerequisite. */
static int invalid_generation_compute(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec) {
    (void)old;
    rec->hdr2 = 2; /* A callback may rewrite NEW; packed-only is invalid. */
    return 0;
}
static void generation_transport(void) {
    uint8_t fake[40]={0};uint16_t keylen=1;uint32_t vlen=11;
    memcpy(fake+16,&keylen,2);fake[18]=1;memcpy(fake+20,&vlen,4);
    uint8_t flags[]={0,1,2,3,4};int expected[]={1,1,0,1,0};
    for(size_t i=0;i<sizeof(flags);i++){
        size_t recn=0;uint8_t flag=0,hdr2=0;uint16_t k=0;uint32_t n=0;fake[19]=flags[i];
        ASSERT_EQ_INT(seg_scan_varlen_struct_ok(fake,sizeof(fake),0,64,&recn,&flag,&k,&n,&hdr2),
                      expected[i],"known generation combinations only");
    }
    char root[]="/tmp/shard-packed-flag-XXXXXX";char *created=mkdtemp(root);
    ASSERT_NOT_NULL(created,"flag regression isolated object");if(!created)return;
    slotcask_init(64,64);SlotcaskDb db;
    int rc=slotcask_open(&db,root,1,1,64);ASSERT_EQ_INT(rc,0,"flag object opens");
    if(rc==0){uint8_t value[11]={1,0,0,0,0,8,0,0,0,1,'x'};
        SlotcaskUpsertOpts opts={0};opts.record_hdr2=PKR_FLAGS;
        ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,value,sizeof(value),&opts,NULL),0,
                      "flag regression writes raw future payload");
        void *got=NULL;size_t n=0;uint8_t hdr2=0;
        ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&got,&n,&hdr2),0,"flag regression reads");
        ASSERT_EQ_INT(n,sizeof(value),"raw future vlen");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"packed bit survives physical emitter");free(got);
        uint8_t bad_flags[]={2,0x80,0x83};
        for(size_t i=0;i<sizeof(bad_flags);i++){
            opts.record_hdr2=bad_flags[i];errno=0;
            ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"z",1,value,sizeof(value),&opts,NULL),-1,
                          "invalid generation insert rejected");
            ASSERT_EQ_INT(errno,EINVAL,"invalid insert flags reported");
            errno=0;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,value,sizeof(value),&opts,NULL),-1,
                          "invalid generation update rejected");
            ASSERT_EQ_INT(errno,EINVAL,"invalid update flags reported");
            SlotcaskBulkRec recs[2]={0};recs[0].key="b";recs[1].key="c";
            for(int j=0;j<2;j++){recs[j].klen=1;recs[j].value=value;
                recs[j].vlen=sizeof(value);recs[j].hdr2=j?bad_flags[i]:PKR_FLAGS;}
            SlotcaskBulkOpts bulk={0};errno=0;
            ASSERT_EQ_INT(slotcask_bulk_upsert_in_kfshard(&db,0,recs,2,&bulk),-1,
                          "direct batch invalid generation rejected before any row");
            ASSERT_EQ_INT(errno,EINVAL,"invalid direct batch flags reported");
            SlotcaskBulkShardInput input={0};input.kf_shard_id=0;
            input.recs=recs;input.nrecs=2;input.kind=SLOTCASK_BULK_INPUT_UPSERT;errno=0;
            ASSERT_EQ_INT(slotcask_bulk_request_execute(&db,&input,1),-1,
                          "pipeline batch invalid generation rejected before any row");
            ASSERT_EQ_INT(errno,EINVAL,"invalid pipeline flags reported");
        }
        for(int pipeline=0;pipeline<2;pipeline++){
            for(int existing=0;existing<2;existing++){
                SlotcaskBulkRec rec={0};rec.key=existing?"a":(pipeline?"g":"f");
                rec.klen=1;rec.value=value;rec.vlen=sizeof(value);rec.hdr2=PKR_FLAGS;
                SlotcaskBulkOpts bulk={0};bulk.value_compute=invalid_generation_compute;
                bulk.value_rewrites_payload=1;
                int result;
                if(pipeline){SlotcaskBulkShardInput input={0};input.kf_shard_id=0;
                    input.recs=&rec;input.nrecs=1;input.kind=SLOTCASK_BULK_INPUT_UPSERT;
                    input.opts.upsert=bulk;
                    result=slotcask_bulk_request_execute(&db,&input,1);
                }else result=slotcask_bulk_upsert_in_kfshard(&db,0,&rec,1,&bulk);
                ASSERT_EQ_INT(result,-1,"computed invalid generation fails window");
            }
        }
        ASSERT_EQ_INT(slotcask_exists(&db,"f",1),0,"computed direct insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"g",1),0,"computed pipeline insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"z",1),0,"rejected insert absent");
        ASSERT_EQ_INT(slotcask_exists(&db,"b",1),0,"valid batch prefix not written");
        ASSERT_EQ_INT(slotcask_exists(&db,"c",1),0,"invalid batch suffix not written");
        got=NULL;n=0;hdr2=0;
        ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&got,&n,&hdr2),0,"original survives rejected update");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"rejected updates preserve generation");free(got);
        slotcask_close(&db);}
    slotcask_shutdown();ASSERT_EQ_INT(nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS),0,
                                     "flag fixture removed");
}
static int test_packed_record_storage_run(void) {
    generation_transport();
    PkrField field,fields[PKR_MAX_FIELDS];PkrSchema schema;
    ASSERT_EQ_INT(pkr_field_parse("varchar",&field),DEC_OK,"physical field");
    for(size_t i=0;i<PKR_MAX_FIELDS;i++)fields[i]=field;
    dec_status st=pkr_schema_build(fields,PKR_MAX_FIELDS,&schema);
    ASSERT_EQ_INT(st,DEC_OK,"physical maximum schema");if(st!=DEC_OK)return 1;
    PkrCell cells[PKR_MAX_FIELDS];
    for(size_t i=0;i<PKR_MAX_FIELDS;i++)cells[i]=(PkrCell){NULL,0,1};
    cells[127]=(PkrCell){(const uint8_t*)"tiny",4,0};size_t n=0;
    ASSERT_EQ_INT(pkr_measure(&schema,cells,PKR_MAX_FIELDS,&n),DEC_OK,"physical actual sizing");
    ASSERT_EQ_INT(n,2085,"actual tiny value in1GiB-schema");
    uint8_t *value=malloc(n);ASSERT_NOT_NULL(value,"allocate actual row");
    if(!value)return 1;
    size_t used=0;
    st=pkr_compose(&schema,cells,PKR_MAX_FIELDS,value,n,&used);
    ASSERT_EQ_INT(st,DEC_OK,"physical compose");if(st!=DEC_OK){free(value);return 1;}
    size_t bound=0,record_size=0;
    ASSERT_EQ_INT(pkr_slot_bound(&schema,1024,&bound),DEC_OK,"maximum slot size");
    ASSERT_EQ_INT(pkr_record_fits(&schema,n,1,SLOTCASK_SEG_MAX_BYTES,&record_size),DEC_OK,
                  "actual record fits unchanged segment ceiling");
    char root[]="/tmp/shard-packed-record-XXXXXX";
    char *created=mkdtemp(root);ASSERT_NOT_NULL(created,"isolated physical object");
    if(!created){free(value);return 1;}
    slotcask_init(64,64);SlotcaskDb db;
    int rc=slotcask_open(&db,root,1,1,(int)bound);
    ASSERT_EQ_INT(rc,0,"open huge-bound object without huge row allocation");
    if(rc){free(value);slotcask_shutdown();nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS);return 1;}
    SlotcaskUpsertOpts opts={0};opts.record_hdr2=PKR_FLAGS;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"a",1,value,n,&opts,NULL),0,"first packed row");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"b",1,value,n,&opts,NULL),0,"second packed row");
    void *got=NULL;size_t gotn=0;uint8_t hdr2=0;
    rc=slotcask_get_ex(&db,"a",1,&got,&gotn,&hdr2);
    ASSERT_EQ_INT(rc,0,"raw packed read");ASSERT_EQ_INT(gotn,n,"raw length actual");
    ASSERT_EQ_INT(hdr2,PKR_FLAGS,"generation bit preserved by emitter");
    if(rc==0&&got&&gotn==n)ASSERT_EQ_INT(memcmp(got,value,n),0,"physical bytes exact");
    free(got);
    char path[PATH_MAX];snprintf(path,sizeof(path),"%s/data/streams/000/000000.dat",root);
    int fd=open(path,O_RDONLY);ASSERT_TRUE(fd>=0,"raw segment opens");
    if(fd>=0){
        uint8_t header[24];ssize_t count=pread(fd,header,sizeof(header),0);
        ASSERT_EQ_INT(count,24,"first header");
        if(count==24){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
            ASSERT_EQ_INT(disk_n,n,"on-disk vlen is actual not maximum");
            ASSERT_EQ_INT(header[19],PKR_FLAGS,"on-disk hdr2");}
        count=pread(fd,header,sizeof(header),(off_t)record_size);
        ASSERT_EQ_INT(count,24,"second header at actual aligned offset");
        if(count==24){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
            ASSERT_EQ_INT(disk_n,n,"second actual vlen");
            ASSERT_EQ_INT(header[18],1,"second row live at compact offset");}
        close(fd);
    }
    Walk w={&schema,0,0};
    ASSERT_EQ_INT(slotcask_walk_live_strict(&db,check_row,&w),0,"strict walk accepts packed bit");
    ASSERT_EQ_INT(w.rows,2,"strict walk sees both packed rows");
    ASSERT_EQ_INT(w.bad,0,"strict walker bytes validate");

    uint8_t *large=malloc(PKR_VARCHAR_MAX);ASSERT_NOT_NULL(large,"4MiB content allocation");
    if(large){
        memset(large,'A',PKR_VARCHAR_MAX);cells[127]=(PkrCell){large,PKR_VARCHAR_MAX,0};
        size_t large_n=0;ASSERT_EQ_INT(pkr_measure(&schema,cells,PKR_MAX_FIELDS,&large_n),DEC_OK,
                                      "maximum field actual measure");
        uint8_t *record=malloc(large_n);ASSERT_NOT_NULL(record,"4MiB actual record");
        if(record){
            st=pkr_compose(&schema,cells,PKR_MAX_FIELDS,record,large_n,&used);
            ASSERT_EQ_INT(st,DEC_OK,"4MiB field compose");
            if(st==DEC_OK){
                ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"c",1,record,used,&opts,NULL),0,
                              "maximum field stored");
                got=NULL;gotn=0;hdr2=0;rc=slotcask_get_ex(&db,"c",1,&got,&gotn,&hdr2);
                ASSERT_EQ_INT(rc,0,"maximum field get");ASSERT_EQ_INT(gotn,large_n,"maximum actual bytes");
                if(rc==0&&got){PkrView view;PkrSpan span;
                    st=pkr_view(&schema,got,gotn,hdr2,&view);
                    ASSERT_EQ_INT(st,DEC_OK,"maximum physical view");
                    if(st==DEC_OK){ASSERT_EQ_INT(pkr_span(&view,127,&span),DEC_OK,"maximum physical span");
                        ASSERT_EQ_INT(span.len,PKR_VARCHAR_MAX,"full4MiB length");
                        if(span.data&&span.len==PKR_VARCHAR_MAX)
                            ASSERT_EQ_INT(memcmp(span.data,large,PKR_VARCHAR_MAX),0,"all4MiB bytes intact");}}
                free(got);
            }
            free(record);
        }
        free(large);
    }
    /* Reuse a large hole twice; the second small row uses its split leftover. */
    ASSERT_EQ_INT(slotcask_delete_with_hooks(&db,"c",1,NULL,NULL),0,"delete large donor");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"d",1,value,n,&opts,NULL),0,"reuse donor head");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&db,-1,"e",1,value,n,&opts,NULL),0,"reuse split leftover");
    fd=open(path,O_RDONLY);ASSERT_TRUE(fd>=0,"reused segment opens");
    if(fd>=0){
        for(int i=0;i<2;i++){
            uint8_t header[25];off_t at=(off_t)((2+(size_t)i)*record_size);
            ssize_t count=pread(fd,header,sizeof(header),at);
            ASSERT_EQ_INT(count,25,"small reuse at exact compact offset");
            if(count==25){uint32_t disk_n=0;memcpy(&disk_n,header+20,4);
                ASSERT_EQ_INT(header[18],1,"reused record live");
                ASSERT_EQ_INT(header[19],PKR_FLAGS,"reused generation preserved");
                ASSERT_EQ_INT(disk_n,n,"reused extent is actual length");
                ASSERT_EQ_INT(header[24],i?'e':'d',"donor head then leftover reused");}
        }
        close(fd);
    }
    slotcask_close(&db);
    rc=slotcask_open(&db,root,1,1,(int)bound);
    ASSERT_EQ_INT(rc,0,"reopen recovers packed physical records");
    if(rc==0){got=NULL;gotn=0;hdr2=0;
        rc=slotcask_get_ex(&db,"a",1,&got,&gotn,&hdr2);
        ASSERT_EQ_INT(rc,0,"read after reopen");ASSERT_EQ_INT(gotn,n,"reopen actual size");
        ASSERT_EQ_INT(hdr2,PKR_FLAGS,"reopen generation");free(got);slotcask_close(&db);}
    slotcask_shutdown();free(value);
    ASSERT_EQ_INT(nftw(root,remove_entry,16,FTW_DEPTH|FTW_PHYS),0,"physical fixture removed");
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-packed-record-storage",test_packed_record_storage_run)
