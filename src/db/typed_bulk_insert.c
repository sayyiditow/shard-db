#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    const char *line;
    size_t line_len;
    int shard,error;
    uint8_t *owned;
    const TypedSchema *ts;
    const Schema *sc;
    const char *root,*object;
} BulkRow;
typedef struct {BulkRow *rows;size_t count,cap,errors;} BulkRows;
static const char *bulk_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int bulk_fail(int code){errno=code;return -1;}
static char *bulk_copy(const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    char *s=malloc(n+1);if(!s)return NULL;
    memcpy(s,p,n);s[n]=0;return s;
}
static void bulk_free(BulkRows *b){
    for(size_t i=0;i<b->count;i++){free(b->rows[i].key);free(b->rows[i].json);free(b->rows[i].owned);}
    free(b->rows);memset(b,0,sizeof(*b));
}
/* Ownership of key/json transfers only on success. Row pointers are not exposed
 * to the engine until collection and sorting are complete. */
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(!key||!kn||kn>(size_t)sc->max_key||(!json&&!line)){
        b->errors++;free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16];compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
static int bulk_key(const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    return json_unescape_string(p+1,(size_t)(end-p-2),key,len);
}
static int bulk_expired(const QueryDeadline *d){
    return d->timeout_ms&&now_ms_coarse()-d->t0_ms>(uint64_t)d->timeout_ms;
}
/* Whole-document validation happens before this collector; skip_value never
 * sees malformed input. Non-object values are row errors, not partial JSON. */
static int bulk_json(BulkRows *b,const char *text,const Schema *sc,QueryDeadline *dl){
    if(typed_json_validate(text))return -1;
    const char *p=bulk_space(text);int dict=*p=='{';
    if(!dict&&*p!='[')return bulk_fail(EINVAL);
    char close=dict?'}':']';p=bulk_space(p+1);
    while(*p!=close){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        char *key=NULL,*value=NULL;size_t kn=0;
        const char *v=NULL,*ve=NULL,*end=NULL;
        if(dict){
            end=json_skip_value(p);
            if(bulk_key(p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(v&&*v=='{'){
            value=bulk_copy(v,(size_t)(ve-v));
            if(!value){free(key);return -1;}
        }
        if(bulk_add(b,key,kn,value,NULL,0,sc)){free(key);free(value);return -1;}
        p=bulk_space(p);if(*p==',')p=bulk_space(p+1);
    }
    return 0;
}
static int bulk_delimited(BulkRows *b,const char *data,size_t len,
    const Schema *sc,char delimiter,QueryDeadline *dl){
    if(!len||!delimiter||delimiter=='\n'||delimiter=='\r')return bulk_fail(EINVAL);
    size_t at=0;
    while(at<len){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        size_t start=at;while(at<len&&data[at]!='\n'&&data[at]!='\r')at++;
        size_t end=at;
        if(at<len&&data[at]=='\r')at++;
        if(at<len&&data[at]=='\n')at++;
        if(end==start)continue;
        size_t key_end=start;while(key_end<end&&data[key_end]!=delimiter)key_end++;
        if(key_end==end)continue; /* A key alone is not a positional record. */
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        const char *line=data+(key_end<end?key_end+1:end);
        size_t ln=(size_t)((data+end)-line);
        if(bulk_add(b,key,key_end-start,NULL,line,ln,sc)){free(key);return -1;}
    }
    return 0;
}
static int bulk_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;
    if(x->shard!=y->shard)return x->shard<y->shard?-1:1;
    return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
/* Immutable delimiter and row pointer remain valid through synchronous join. */
typedef struct {BulkRow *row;char delimiter;} BulkCell;
static int bulk_value(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    BulkCell *cell=rec->user_ctx;BulkRow *r=cell->row;
    free(r->owned);r->owned=NULL;r->error=0;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;return -1;
    }
    size_t len=0;int rc;
    if(r->json)rc=typed_insert_alloc(r->ts,old,r->json,r->root,r->object,&r->owned,&len,NULL,0);
    else{
        TypedInput inputs[MAX_FIELDS]={0};size_t at=0;
        for(int i=0;i<r->ts->nfields;i++){
            if(r->ts->fields[i].removed)continue;
            size_t begin=at;while(at<r->line_len&&r->line[at]!=cell->delimiter)at++;
            size_t n=at-begin;if(at<r->line_len)at++;
            enum FieldType t=r->ts->fields[i].type;
            /* Legacy positional empty fixed scalars remain present zero.
             * Empty numeric is invalid; varchar remains present empty. */
            inputs[i]=(TypedInput){r->line+begin,n,(uint8_t)(n||t==FT_NUMERIC||t==FT_VARCHAR),0,0};
        }
        rc=typed_insert_inputs_alloc(r->ts,old,inputs,&r->owned,&len,NULL,0);
    }
    size_t physical=0;
    if(rc||len>(size_t)r->sc->max_value||
        pkr_record_fits(&r->ts->packed,len,r->klen,slotcask_seg_max_bytes(),&physical)!=DEC_OK||
        physical>(size_t)r->sc->slot_size){
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;return -1;
    }
    rec->value=r->owned;rec->vlen=len;rec->hdr2=PKR_FLAGS;return 0;
}
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    if(!root||!object||!sc||!ts||!data||format<0||format>1||!ts->packed.nfields||
        ts->nfields<1||ts->nfields>MAX_FIELDS||ts->packed.nfields!=(uint32_t)ts->nfields||
        sc->splits<1||sc->splits>MAX_SPLITS||sc->streams<1||sc->slot_size<32||sc->max_key<1||
        sc->max_value<0||ts->total_size>sc->max_value||
        (borrowed&&(borrowed->num_shards!=sc->splits||borrowed->slot_size!=sc->slot_size))){
        errno=EINVAL;OUT("{\"error\":\"invalid packed bulk input\"}\n");return 1;
    }
    char indexes[MAX_FIELDS][256];
    if(sc->auto_key!=AK_NONE||load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk auto-key and indexes await integration\"}\n");return 1;
    }
    BulkRows b={0};char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy(data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(b.count>SIZE_MAX/sizeof(SlotcaskBulkRec)||b.count>SIZE_MAX/sizeof(BulkCell)){
        errno=EOVERFLOW;goto fail;
    }
    if(!b.count){
        size_t errors=b.errors;bulk_free(&b);free(json);
        if(errors){OUT("{\"inserted\":0,\"skipped\":0,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",errors);return 1;}
        OUT("{\"inserted\":0}\n");return 0;
    }
    qsort(b.rows,b.count,sizeof(*b.rows),bulk_order);
    size_t groups=1;
    for(size_t i=1;i<b.count;i++)if(b.rows[i].shard!=b.rows[i-1].shard)groups++;
    SlotcaskBulkRec *recs=calloc(b.count,sizeof(*recs));
    BulkCell *cells=calloc(b.count,sizeof(*cells));
    SlotcaskBulkShardInput *inputs=calloc(groups,sizeof(*inputs));
    if(!recs||!cells||!inputs){free(recs);free(cells);free(inputs);errno=ENOMEM;goto fail;}
    for(size_t i=0,g=0;i<b.count;i++){
        BulkRow *r=&b.rows[i];r->ts=ts;r->sc=sc;r->root=root;r->object=object;
        cells[i]=(BulkCell){r,delimiter};
        recs[i]=(SlotcaskBulkRec){.key=r->key,.klen=r->klen,.hdr2=PKR_FLAGS,.user_ctx=&cells[i],.status=-1};
        if(!i||r->shard!=b.rows[i-1].shard){
            if(i)g++;
            inputs[g]=(SlotcaskBulkShardInput){.kf_shard_id=r->shard,.recs=recs+i,
                .kind=SLOTCASK_BULK_INPUT_UPSERT,.rc=-1,
                .opts.upsert={.if_not_exists=if_not_exists,.value_compute=bulk_value,.value_rewrites_payload=1}};
        }
        inputs[g].nrecs++;
    }
    SlotcaskSchemaInfo info={.splits=sc->splits,.slot_size=sc->slot_size,.streams=sc->streams};
    SlotcaskDb *owned SDB_REG_REF=borrowed?NULL:slotcask_registry_get(root,object,&info);
    SlotcaskDb *db=borrowed?borrowed:owned;
    if(!db){free(inputs);free(cells);free(recs);goto fail;}
    rc=slotcask_bulk_request_execute(db,inputs,groups);
    size_t inserted=0,skipped=0,errors=b.errors;int pending=0;
    for(size_t g=0;g<groups;g++){
        if(inputs[g].rc==-2)pending=1;
        for(size_t i=0;i<inputs[g].nrecs;i++){
            SlotcaskBulkRec *r=&inputs[g].recs[i];BulkCell *c=r->user_ctx;
            if(c->row->error)errors++;
            else if(r->status==-2)skipped++;
            else if(inputs[g].rc||r->status)errors++;
            else if(!r->was_update)inserted++;
        }
    }
    /* A coordinator-level failure without a per-shard status is never success. */
    if(rc&&!pending&&!errors)errors++;
    free(inputs);free(cells);free(recs);bulk_free(&b);free(json);
    if(pending){OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    if(errors){OUT("{\"inserted\":%zu,\"skipped\":%zu,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",inserted,skipped,errors);return 1;}
    if(skipped)OUT("{\"inserted\":%zu,\"skipped\":%zu}\n",inserted,skipped);
    else OUT("{\"inserted\":%zu}\n",inserted);
    return 0;
fail:{
    int saved=errno?errno:EINVAL;bulk_free(&b);free(json);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":saved==ETIMEDOUT?"query_timeout":"invalid packed bulk input");
    return 1;
}}
