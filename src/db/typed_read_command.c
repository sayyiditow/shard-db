#include "typed_read_command.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <stdatomic.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
typedef struct ReadPart {struct ReadPart *next;char *data;size_t len;} ReadPart;
typedef struct {
    const FieldSchema *fs;const Schema *schema;const TypedReadOptions *options;
    ReadPart *head,*tail;size_t retained,count;int error,rows,dict,csv,found;
    int64_t skip,start;uint64_t began;const void *key;size_t keylen;
    const char *wire;size_t wirelen;
    char *selection,*legacy;size_t legacy_charge;
    const char *names[MAX_FIELDS];int indices[MAX_FIELDS];size_t nfields;
    const char *legacy_values[MAX_FIELDS];size_t legacy_lengths[MAX_FIELDS];
} ReadCommand;
static int read_error(FILE *out,int code){
    if(!out){errno=code;return -1;}
    const char *message=code==ENOMEM||code==ENOBUFS?
        "{\"error\":\"query memory buffer exceeded or allocation failed\"}\n":
        code==ETIMEDOUT?"{\"error\":\"query timed out\"}\n":
        code==ENOENT?"{\"error\":\"Not found\"}\n":
        code==EIO||code==EILSEQ?"{\"error\":\"corrupt record or storage read failed\"}\n":
        "{\"error\":\"invalid read request\"}\n";
    size_t n=strlen(message);
    if(fwrite(message,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    errno=code;return -1;
}
static void read_free(ReadCommand *c){
    while(c->head){ReadPart *p=c->head;c->head=p->next;free(p->data);free(p);}
    free(c->legacy);c->legacy=NULL;c->legacy_charge=0;
    free(c->selection);c->selection=NULL;c->tail=NULL;c->retained=0;
}
static size_t read_available(ReadCommand *c){
    size_t max=c->options->budget;
    if(c->retained>=max||max-c->retained<=sizeof(ReadPart)){c->error=ENOBUFS;return 0;}
    return max-c->retained-sizeof(ReadPart);
}
static int read_owned_size(ReadCommand *c,char *data,size_t n,size_t allocation){
    if(!data){c->error=errno?errno:EIO;return -1;}
    if(n==SIZE_MAX||n+1>allocation||allocation>read_available(c)){free(data);c->error=ENOBUFS;return -1;}
    ReadPart *p=malloc(sizeof(*p));if(!p){free(data);c->error=ENOMEM;return -1;}
    *p=(ReadPart){.data=data,.len=n};if(c->tail)c->tail->next=p;else c->head=p;
    c->tail=p;c->retained+=sizeof(*p)+allocation;return 0;
}
static int read_owned(ReadCommand *c,char *data,size_t n){
    if(n==SIZE_MAX){free(data);c->error=EOVERFLOW;return -1;}
    return read_owned_size(c,data,n,n+1);
}
static int read_literal(ReadCommand *c,const char *text){
    size_t n=strlen(text);if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(n+1);if(!p){c->error=ENOMEM;return -1;}
    memcpy(p,text,n+1);return read_owned(c,p,n);
}
static int read_quote(ReadCommand *c,const char *s,size_t n){
    size_t need=2;
    for(size_t i=0;i<n;i++){
        unsigned char v=(unsigned char)s[i];size_t k=v=='"'||v=='\\'||v=='\b'||v=='\f'||v=='\n'||v=='\r'||v=='\t'?2:v<32?6:1;
        if(need>SIZE_MAX-k){c->error=EOVERFLOW;return -1;}need+=k;
    }
    if(need==SIZE_MAX||need+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(need+1);if(!p){c->error=ENOMEM;return -1;}
    p[0]='"';int written=json_escape_into(p+1,need-1,s,n);
    if(written<0||(size_t)written!=need-2){free(p);c->error=EILSEQ;return -1;}
    p[need-1]='"';p[need]=0;return read_owned(c,p,need);
}
static int read_deadline(ReadCommand *c){
    if(c->options->timeout_ms&&now_ms_coarse()-c->began>=c->options->timeout_ms){c->error=ETIMEDOUT;return -1;}return 0;
}
static int read_finish(ReadCommand *c,FILE *out){
    if(c->error){int code=c->error;read_free(c);return read_error(out,code);}
    for(ReadPart *p=c->head;p;p=p->next)if(fwrite(p->data,1,p->len,out)!=p->len||ferror(out)){
        read_free(c);errno=EIO;return -1;}
    read_free(c);if(fflush(out)||ferror(out)){errno=EIO;return -1;}return 0;
}
static int read_selection(ReadCommand *c){
    const char *fields=c->options->fields;
    if(!fields||!*fields){
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed)
            {c->names[c->nfields]=c->fs->ts->fields[i].name;c->indices[c->nfields++]=i;}
        return 0;
    }
    size_t n=strnlen(fields,MAX_LINE);
    if(n==MAX_LINE){c->error=E2BIG;return -1;}
    if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    c->selection=malloc(n+1);if(!c->selection){c->error=ENOMEM;return -1;}
    memcpy(c->selection,fields,n+1);c->retained+=n+1;
    char *save=NULL;
    for(char *p=strtok_r(c->selection,",",&save);p;p=strtok_r(NULL,",",&save)){
        if(c->nfields==MAX_FIELDS){c->error=E2BIG;return -1;}c->names[c->nfields]=p;c->indices[c->nfields++]=-1;
    }
    return 0;
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n);
static void read_legacy_close(ReadCommand *c){
    if(c->legacy){free(c->legacy);c->legacy=NULL;c->retained-=c->legacy_charge;c->legacy_charge=0;
        memset(c->legacy_values,0,sizeof(c->legacy_values));memset(c->legacy_lengths,0,sizeof(c->legacy_lengths));}
}
static int read_legacy_open(ReadCommand *c,const void *v,size_t n,uint8_t flags){
    if(c->legacy)return 0;
    size_t available=read_available(c);if(n>INT_MAX||available<8){c->error=ENOBUFS;return -1;}
    size_t reserve=available/2;
    c->legacy=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,reserve,&c->legacy_charge);
    if(!c->legacy){c->error=errno?errno:EILSEQ;return -1;}
    c->retained+=c->legacy_charge;
    const char *p=json_skip(c->legacy);if(*p++!='{'){c->error=EILSEQ;return -1;}
    p=json_skip(p);
    while(*p&&*p!='}'){
        if(*p++!='"'){c->error=EILSEQ;return -1;}
        const char *key=p,*end=json_raw_string_end(key);
        if(*end!='"'||(size_t)(end-key)+1>read_available(c)){c->error=ENOBUFS;return -1;}
        char *name=NULL;size_t name_len=0;
        if(json_unescape_cstring(key,(size_t)(end-key),&name,&name_len)){c->error=errno?errno:EINVAL;return -1;}
        p=json_skip(end+1);if(*p++!=':'){free(name);c->error=EILSEQ;return -1;}p=json_skip(p);
        const char *value=p,*after=json_skip_value(p);
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed&&
            name_len==(size_t)c->fs->ts->fields[i].name_len&&!memcmp(name,c->fs->ts->fields[i].name,name_len)){
            c->legacy_values[i]=value;c->legacy_lengths[i]=(size_t)(after-value);
        }
        free(name);p=json_skip(after);if(*p==',')p=json_skip(p+1);else if(*p!='}'){c->error=EILSEQ;return -1;}
    }
    return 0;
}
static void read_legacy_span(ReadCommand *c,const char *name,size_t name_len,
    const char **value,size_t *length){
    *value="null";*length=4;
    for(int i=0;i<c->fs->ts->nfields;i++)if(name_len==(size_t)c->fs->ts->fields[i].name_len&&
       !memcmp(name,c->fs->ts->fields[i].name,name_len)){
        if(c->legacy_values[i]){*value=c->legacy_values[i];*length=c->legacy_lengths[i];}
        return;
    }
}

static char *read_legacy_component(ReadCommand *c,const char *name,size_t name_len,size_t *len,int *is_null){
    const char *raw=NULL;size_t n=0;*len=0;*is_null=0;read_legacy_span(c,name,name_len,&raw,&n);
    if(n==4&&!memcmp(raw,"null",4)){*is_null=1;return NULL;}
    size_t available=read_available(c);
    if(n==SIZE_MAX||n+1>available/4){c->error=ENOBUFS;return NULL;}
    char *text=NULL;
    if(n>=2&&raw[0]=='"'&&raw[n-1]=='"'){
        if(json_unescape_string(raw+1,n-2,&text,len)){c->error=errno?errno:EINVAL;return NULL;}
    }else{
        text=malloc(n+1);if(!text){c->error=ENOMEM;return NULL;}memcpy(text,raw,n);text[n]=0;*len=n;
    }
    return text;
}
static char *read_legacy_text(ReadCommand *c,const char *name,int index,size_t *len,int *is_null){
    *len=0;*is_null=0;
    if(index>=0||!strchr(name,'+'))return read_legacy_component(c,name,strlen(name),len,is_null);
    size_t total=0,count=0;const char *p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        if(++count>MAX_FIELDS){c->error=E2BIG;return NULL;}
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        free(part);if(c->error)return NULL;
        if(n>SIZE_MAX-total){c->error=EOVERFLOW;return NULL;}total+=n;p=end;
    }
    if(!total){*is_null=1;return NULL;}
    if(total==SIZE_MAX||total+1>read_available(c)/4){c->error=ENOBUFS;return NULL;}
    char *text=malloc(total+1);if(!text){c->error=ENOMEM;return NULL;}
    c->retained+=total+1;size_t at=0;p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        if(c->error){free(part);free(text);c->retained-=total+1;return NULL;}
        if(n)memcpy(text+at,part,n);
        at+=n;free(part);p=end;
    }
    c->retained-=total+1;text[at]=0;*len=at;return text;
}
static int read_legacy_cell(ReadCommand *c,const char *name,int index){
    if(index<0&&strchr(name,'+')){
        size_t n=0;int null=0;char *text=read_legacy_text(c,name,index,&n,&null);
        if(c->error){free(text);return -1;}if(null)return read_literal(c,"null");
        c->retained+=n+1;int rc=read_quote(c,text,n);c->retained-=n+1;free(text);return rc;
    }
    const char *raw=NULL;size_t n=0;read_legacy_span(c,name,strlen(name),&raw,&n);
    int i=index>=0?index:typed_field_index(c->fs->ts,name);
    if(i>=0&&c->fs->ts->fields[i].type==FT_BOOL&&!(n==4&&!memcmp(raw,"null",4)))return read_quote(c,raw,n);
    if(n==SIZE_MAX||n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *copy=malloc(n+1);if(!copy){c->error=ENOMEM;return -1;}memcpy(copy,raw,n);copy[n]=0;
    return read_owned(c,copy,n);
}
static int read_csv_header(ReadCommand *c){
    if(read_literal(c,"key"))return -1;
    if(c->options->kind==TYPED_READ_EXISTS_MULTI){char tail[16];snprintf(tail,sizeof(tail),"%cexists\n",c->options->delimiter);return read_literal(c,tail);}
    if(c->options->kind==TYPED_READ_KEYS||c->options->kind==TYPED_READ_NOT_EXISTS)return read_literal(c,"\n");
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t n=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(c->names[i],strlen(c->names[i]),c->options->delimiter,available,&n):NULL;
        if(read_owned(c,cell,n))return -1;
    }
    return read_literal(c,"\n");
}
static int read_legacy_csv_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(read_legacy_open(c,v,n,flags)||read_csv_key(c,wire,wirelen))return -1;
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t length=0;int null=0;char *text=read_legacy_text(c,c->names[i],c->indices[i],&length,&null);
        if(c->error){free(text);return -1;}if(null)continue;
        c->retained+=length+1;size_t used=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(text,length,c->options->delimiter,available,&used):NULL;
        c->retained-=length+1;free(text);if(read_owned(c,cell,used))return -1;
    }
    return read_literal(c,"\n");
}

static int read_value(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *name,int index){
    size_t available=read_available(c);if(!available)return -1;
    if(!c->fs->ts->packed.nfields){
        if(name||(c->options->fields&&*c->options->fields)){
            if(read_legacy_open(c,v,n,flags))return -1;
            if(name)return read_legacy_cell(c,name,index);
            if(read_literal(c,"{"))return -1;
            for(size_t i=0;i<c->nfields;i++){
                if((i&&read_literal(c,","))||read_quote(c,c->names[i],strlen(c->names[i]))||
                    read_literal(c,":")||read_legacy_cell(c,c->names[i],c->indices[i]))return -1;
            }
            return read_literal(c,"}");
        }
        if(n>INT_MAX){c->error=EOVERFLOW;return -1;}
        size_t allocated=0;char *text=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,available,&allocated);
        return read_owned_size(c,text,text?strlen(text):0,allocated);
    }
    size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,available);
    if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
    char *text=name?(index>=0?
        typed_packed_projected_cell(c->fs->ts,v,n,flags,(size_t)index,allowance):
        typed_packed_named_json(c->fs->ts,v,n,flags,name,allowance)):
        c->options->fields&&*c->options->fields?
        typed_packed_project_json(c->fs->ts,v,n,flags,c->options->fields,allowance):
        typed_packed_decode(c->fs->ts,v,n,flags,allowance);
    return read_owned(c,text,text?strlen(text):0);
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n){
    size_t available=read_available(c),used=0;
    char *cell=available?typed_csv_cell_text(wire,n,c->options->delimiter,available,&used):NULL;
    return read_owned(c,cell,used);
}
static int read_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(c->csv&&c->options->kind!=TYPED_READ_KEYS){
        if(c->fs->ts->packed.nfields){
            size_t available=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,read_available(c)),used=0;
            char *text=available?typed_packed_csv_row(c->fs->ts,v,n,flags,wire,wirelen,c->options->fields,c->options->delimiter,available,&used):NULL;
            return read_owned(c,text,used);
        }
        return read_legacy_csv_row(c,v,n,flags,wire,wirelen);
    }
    if(c->count&&!c->csv&&read_literal(c,","))return -1;
    if(c->options->kind==TYPED_READ_KEYS){
        if(c->csv)return read_csv_key(c,wire,wirelen)||read_literal(c,"\n");
        return read_quote(c,wire,wirelen);
    }
    if(c->rows){
        if(c->fs->ts->packed.nfields){
            size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,read_available(c));
            if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
            char *row=typed_packed_row_json(c->fs->ts,v,n,flags,wire,wirelen,c->options->fields,allowance);
            return read_owned(c,row,row?strlen(row):0);
        }
        if(read_legacy_open(c,v,n,flags))return -1;
        if(read_literal(c,"[")||read_quote(c,wire,wirelen))return -1;
        for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_value(c,v,n,flags,c->names[i],c->indices[i]))return -1;
        return read_literal(c,"]");
    }
    if(!c->dict&&read_literal(c,"{\"key\":"))return -1;
    if(read_quote(c,wire,wirelen)||read_literal(c,c->dict?":":",\"value\":"))return -1;
    if(read_value(c,v,n,flags,NULL,-1))return -1;
    return c->dict?0:read_literal(c,"}");
}
static int read_walk(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;if(read_deadline(c))return 1;
    char wire[MAX_KEY_CEILING+1];
    if(!klen||klen>MAX_KEY_CEILING){c->error=EILSEQ;return 1;}
    int n=format_wire_key(c->schema,key,klen,wire,sizeof(wire));
    if(n<0){c->error=EILSEQ;return 1;}
    int rc=read_row(c,value,vlen,flags,wire,(size_t)n);read_legacy_close(c);
    if(rc)return 1;
    c->count++;return c->count>=(size_t)c->options->limit;
}
static int read_lookup(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;
    if(klen!=c->keylen||memcmp(key,c->key,klen))return 0;
    c->found=1;
    if(c->options->kind==TYPED_READ_GET_MULTI){
        if(c->csv)read_row(c,value,vlen,flags,c->wire,c->wirelen);
        else read_value(c,value,vlen,flags,NULL,-1);
        read_legacy_close(c);
    }
    return 1;
}
static int read_offset(const TypedReadOptions *o,int64_t *skip){
    uint64_t n=0;
    for(const unsigned char *p=(const unsigned char*)(o->cursor?o->cursor:"");*p;p++){
        if(*p<'0'||*p>'9'||n>((uint64_t)INT64_MAX-(*p-'0'))/10){errno=EINVAL;return -1;}
        n=n*10+(*p-'0');
    }
    uint64_t add=o->offset>0?(uint64_t)o->offset:0;
    if(n>(uint64_t)INT64_MAX-add){errno=EINVAL;return -1;}
    *skip=(int64_t)(n+add);return 0;
}
static int read_open(ReadCommand *c,const FieldSchema *fs,const Schema *sc,const TypedReadOptions *o,FILE *out){
    memset(c,0,sizeof(*c));c->fs=fs;c->schema=sc;c->options=o;
    if(!fs||!sc||!o||!out||((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&(!fs->ts||!fs->ts->typed))){c->error=EINVAL;return -1;}
    if(o->kind<TYPED_READ_FETCH||o->kind>TYPED_READ_NOT_EXISTS||
       ((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&
        (fs->ts->nfields<1||fs->ts->nfields>MAX_FIELDS))){c->error=EINVAL;return -1;}
    c->began=now_ms_coarse();c->dict=o->format&&!strcmp(o->format,"dict");
    c->rows=o->format&&!strcmp(o->format,"rows");
    c->csv=o->format&&!strcmp(o->format,"csv");
    unsigned char separator=(unsigned char)o->delimiter;
    if(c->csv&&separator!='\t'&&(separator<32||separator>126||separator=='"'||separator=='\\')){
        c->error=EINVAL;return -1;
    }
    if((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&read_selection(c))return -1;
    if(o->kind==TYPED_READ_FETCH){
        if(o->limit<=0||read_offset(o,&c->skip)){c->error=EINVAL;return -1;}c->start=c->skip;
        if(c->csv&&o->want_total){c->error=ENOTSUP;return -1;}
    }
    if(c->csv)return read_csv_header(c);
    if(o->kind==TYPED_READ_FETCH){
        if(c->rows){
            if(read_literal(c,"{\"columns\":[\"key\""))return -1;
            for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_quote(c,c->names[i],strlen(c->names[i])))return -1;
            return read_literal(c,"],\"rows\":[");
        }
        return read_literal(c,o->want_total?(c->dict?"{\"rows\":{":"{\"rows\":["):
            (c->dict?"{\"results\":{":"{\"results\":["));
    }
    if(c->csv){
        if(o->kind!=TYPED_READ_EXISTS_MULTI)return read_literal(c,"key\n");
        char header[16];snprintf(header,sizeof(header),"key%cexists\n",o->delimiter);
        return read_literal(c,header);
    }
    return read_literal(c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"[":"{");
}
/* Retain the existing one-worker-per-touched-shard dispatch and batched
 * lookup/resolve-fetch paths. Only ownership, bounds and error status change. */
typedef struct {size_t limit;_Atomic size_t used;} ReadMemory;
static int batch_take(ReadMemory *m,size_t n){
    size_t used=atomic_load(&m->used);
    do{if(used>m->limit||n>m->limit-used){errno=ENOBUFS;return -1;}}
    while(!atomic_compare_exchange_weak(&m->used,&used,used+n));
    return 0;
}
static void batch_release(ReadMemory *m,size_t n){atomic_fetch_sub(&m->used,n);}
typedef struct {
    char *wire;size_t wirelen,wirecharge,klen;uint8_t binary[16],hash[16];
    int shard,found;ReadPart *head,*tail;size_t retained;
} ReadEntry;
typedef struct {
    SlotcaskDb *db;ReadCommand *parent;ReadMemory *memory;
    ReadEntry **entries;size_t count;int error;
} ReadShard;
static const void *batch_key(const ReadEntry *e,const Schema *sc){return sc->auto_key==AK_NONE?(const void*)e->wire:(const void*)e->binary;}
static void batch_parts_free(ReadEntry *e){
    while(e->head){ReadPart *p=e->head;e->head=p->next;free(p->data);free(p);}
    e->tail=NULL;e->retained=0;
}
static uint32_t batch_u32(const uint8_t *p){return (uint32_t)p[0]<<24|(uint32_t)p[1]<<16|(uint32_t)p[2]<<8|p[3];}
/* No allocation: reserve output plus actual numeric/display workspace before
 * any validator/renderer allocates. This scan is never a validity substitute. */
static int batch_render_cost(const ReadCommand *c,const uint8_t *value,size_t len,size_t *cost){
    const TypedSchema *ts=c->fs->ts;size_t text=64,workspace=0;
    if(ts->packed.nfields){
        if(pkr_schema_validate(&ts->packed)!=DEC_OK||ts->packed.nfields!=(uint32_t)ts->nfields)return -1;
        if(!value||len<(size_t)ts->packed.prefix+ts->packed.directory)return -1;
        for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];const PkrField *pf=&ts->packed.fields[i];
            size_t part=512;
            if(pf->kind!=PKR_FIXED){
                const uint8_t *d=value+ts->packed.prefix+pf->offset;
                size_t off=batch_u32(d),n=batch_u32(d+4);
                if(n>pf->payload_max||off>len-ts->packed.prefix||n>len-ts->packed.prefix-off)return -1;
                if(pf->kind==PKR_VARCHAR){if(n>(SIZE_MAX-4)/6)return -1;part=6*n+4;}
                else if(n){
                    if(n<16)return -1;
                    const uint8_t *p=value+ts->packed.prefix+off;
                    int64_t nd=batch_u32(p+4),m=(int32_t)batch_u32(p+8),scale=batch_u32(p+12);
                    if(nd>DEC_MAX_COEFF_DIGITS||scale>DEC_MAX_DSCALE||nd+m>DEC_MAX_INT_DIGITS)return -1;
                    int64_t integer=nd+m;if(integer<1)integer=1;
                    part=(size_t)integer+(size_t)scale+4;
                    if(n>(SIZE_MAX-128)/8)return -1;
                    size_t needed=8*n+128;if(needed>workspace)workspace=needed;
                }else part=4;
            }else if(f->type==FT_ENUM){
                size_t longest=0;
                for(int j=0;f->enum_values&&j<f->n_enum_values;j++)if(f->enum_values[j]){
                    size_t n=strlen(f->enum_values[j]);if(n>longest)longest=n;
                }
                if(longest>(SIZE_MAX-4)/6)return -1;
                part=6*longest+4;
            }
            size_t name=strnlen(f->name,sizeof(f->name));
            if(part>SIZE_MAX-text||6*name+16>SIZE_MAX-text-part)return -1;
            text+=part+6*name+16;
        }
    }else{
        if(len>(SIZE_MAX-64)/6)return -1;
        text+=6*len;
        for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];size_t part=512;
            if(f->type==FT_ENUM)for(int j=0;f->enum_values&&j<f->n_enum_values;j++)if(f->enum_values[j]){
                size_t n=strlen(f->enum_values[j]);if(n>(SIZE_MAX-4)/6)return -1;
                if(6*n+4>part)part=6*n+4;
            }
            size_t name=strnlen(f->name,sizeof(f->name));
            if(part>SIZE_MAX-text||6*name+16>SIZE_MAX-text-part)return -1;
            text+=part+6*name+16;
        }
        workspace=text;
    }
    size_t extra=4096+(size_t)ts->nfields*96+c->wirelen*6;
    if(c->options->fields){size_t n=strlen(c->options->fields);if(n>SIZE_MAX-extra-1)return -1;extra+=n+1;}
    if(text>(SIZE_MAX-workspace)/2||extra>SIZE_MAX-workspace-2*text)return -1;
    *cost=2*text+workspace+extra;return 0;
}
static int batch_fetch(const uint8_t hash[16],const void *key,size_t klen,const void *value,size_t len,uint8_t flags,void *ptr){
    ReadShard *w=ptr;
    for(size_t i=0;i<w->count&&!w->error;i++){
        ReadEntry *e=w->entries[i];
        if(e->found||memcmp(e->hash,hash,16)||e->klen!=klen||memcmp(batch_key(e,w->parent->schema),key,klen))continue;
        TypedReadOptions options=*w->parent->options;
        ReadCommand row={.fs=w->parent->fs,.schema=w->parent->schema,.options=&options,
            .began=w->parent->began,.key=key,.keylen=klen,.wire=e->wire,.wirelen=e->wirelen,.csv=w->parent->csv};
        size_t reserve=0;
        if(batch_render_cost(&row,value,len,&reserve)){w->error=EILSEQ;break;}
        if(batch_take(w->memory,reserve)){w->error=errno;break;}
        options.budget=reserve;
        if(read_selection(&row)||read_deadline(&row)){}
        else read_lookup(hash,key,klen,value,len,flags,&row);
        read_legacy_close(&row);
        size_t selection=row.selection?strlen(options.fields)+1:0;
        free(row.selection);row.selection=NULL;row.retained-=selection;
        if(row.error){w->error=row.error;read_free(&row);batch_release(w->memory,reserve);break;}
        e->found=row.found;e->head=row.head;e->tail=row.tail;e->retained=row.retained;
        batch_release(w->memory,reserve-row.retained);
    }
    return w->error?1:0;
}
static void *batch_shard_worker(void *ptr){
    ReadShard *w=ptr;if(!w->count)return NULL;
    if(w->parent->options->kind==TYPED_READ_GET_MULTI){
        uint8_t (*hashes)[16]=malloc(w->count*sizeof(*hashes));
        if(!hashes){w->error=ENOMEM;return NULL;}
        for(size_t i=0;i<w->count;i++)memcpy(hashes[i],w->entries[i]->hash,16);
        if(slotcask_bulk_resolve_and_fetch_checked(w->db,hashes,w->count,w,batch_fetch)<0&&!w->error)w->error=errno?errno:EIO;
        free(hashes);
    }else{
        SlotcaskBulkRec *records=calloc(w->count,sizeof(*records));
        if(!records){w->error=ENOMEM;return NULL;}
        for(size_t i=0;i<w->count;i++){records[i].key=batch_key(w->entries[i],w->parent->schema);records[i].klen=w->entries[i]->klen;}
        if(slotcask_bulk_lookup_in_kfshard_checked(w->db,w->entries[0]->shard,records,w->count)<0)w->error=errno?errno:EIO;
        else for(size_t i=0;i<w->count;i++){
            if(records[i].status==-1){w->error=EIO;break;}
            w->entries[i]->found=records[i].status==0;
        }
        free(records);
    }
    return NULL;
}
static int read_keys_json(ReadCommand *c,SlotcaskDb *db){
    const Schema *sc=c->schema;const char *json=c->options->keys;
    if(!json||typed_json_validate(json)||sc->max_key<1||sc->max_key>MAX_KEY_CEILING||sc->splits<1||sc->splits!=db->num_shards||sc->splits>MAX_SPLITS){c->error=EINVAL;return -1;}
    if(c->csv&&c->options->kind==TYPED_READ_GET_MULTI&&c->options->fields){c->error=EINVAL;return -1;}
    ReadMemory memory={.limit=c->options->budget};atomic_init(&memory.used,c->retained);
    ReadEntry *entries=NULL;size_t count=0,capacity=0;ReadEntry **ordered=NULL;
    size_t *counts=NULL,*starts=NULL,*fill=NULL;ReadShard *workers=NULL;size_t nworkers=0;
    const char *p=json_skip(json);if(*p++!='['){c->error=EINVAL;return -1;}p=json_skip(p);
    while(*p!=']'&&!c->error){
        if(read_deadline(c))break;
        if(*p!='"'){c->error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        size_t maxwire=sc->auto_key==AK_NONE?(size_t)sc->max_key:MAX_KEY_CEILING;
        if(*end!='"'||raw>6*maxwire){c->error=E2BIG;break;}
        if(count==capacity){
            size_t next=capacity?capacity*2:16;
            if(next<capacity||next>SIZE_MAX/sizeof(*entries)||batch_take(&memory,next*sizeof(*entries))){c->error=ENOBUFS;break;}
            ReadEntry *grown=calloc(next,sizeof(*entries));if(!grown){c->error=ENOMEM;break;}
            if(count)memcpy(grown,entries,count*sizeof(*entries));
            free(entries);batch_release(&memory,capacity*sizeof(*entries));entries=grown;capacity=next;
        }
        ReadEntry *e=&entries[count];
        if(batch_take(&memory,raw+1)){c->error=errno;break;}
        if(json_unescape_string(start,raw,&e->wire,&e->wirelen)){c->error=errno?errno:EINVAL;break;}
        e->wirecharge=raw+1;count++;
        if(!e->wirelen||e->wirelen>maxwire){c->error=EINVAL;break;}
        e->klen=e->wirelen;
        if(sc->auto_key==AK_UUID){if(strlen(e->wire)!=e->wirelen||parse_uuid_string(e->wire,e->binary))c->error=EINVAL;e->klen=16;}
        else if(sc->auto_key==AK_SEQ){
            int64_t n;if(strlen(e->wire)!=e->wirelen||parse_seq_key(e->wire,&n))c->error=EINVAL;
            else for(int j=7;j>=0;j--){e->binary[j]=(uint8_t)(uint64_t)n;n=(int64_t)((uint64_t)n>>8);}
            e->klen=8;
        }else if(sc->auto_key!=AK_NONE)c->error=EINVAL;
        if(e->klen>(size_t)sc->max_key)c->error=E2BIG;
        if(c->error)break;
        compute_hash_raw(batch_key(e,sc),e->klen,e->hash);e->shard=compute_record_shard(e->hash,sc->splits);
        p=json_skip(end+1);if(*p==',')p=json_skip(p+1);else if(*p!=']')c->error=EINVAL;
    }
    if(c->error||!count)goto done;
    size_t arrays=(size_t)sc->splits*3*sizeof(size_t)+count*sizeof(*ordered);
    if(batch_take(&memory,arrays)){c->error=errno;goto done;}
    counts=calloc((size_t)sc->splits,sizeof(*counts));starts=calloc((size_t)sc->splits,sizeof(*starts));fill=calloc((size_t)sc->splits,sizeof(*fill));ordered=malloc(count*sizeof(*ordered));
    if(!counts||!starts||!fill||!ordered){c->error=ENOMEM;goto done;}
    for(size_t i=0;i<count;i++)counts[entries[i].shard]++;
    size_t total=0;
    for(int i=0;i<sc->splits;i++){starts[i]=total;total+=counts[i];if(counts[i])nworkers++;}
    /* Admission covers the existing bulk readers' per-shard metadata while
     * every worker runs with its original dispatch/handle lifetime. */
    size_t per=(size_t)sc->splits*5*sizeof(size_t)+512;
    if(nworkers>SIZE_MAX/per||count>SIZE_MAX/(3*sizeof(SlotcaskResolvedRec)+sizeof(SlotcaskBulkRec)+32)){c->error=ENOBUFS;goto done;}
    size_t core=nworkers*per+count*(3*sizeof(SlotcaskResolvedRec)+sizeof(SlotcaskBulkRec)+32);
    if(batch_take(&memory,core)||batch_take(&memory,nworkers*sizeof(*workers))){c->error=errno;goto done;}
    workers=calloc(nworkers,sizeof(*workers));if(!workers){c->error=ENOMEM;goto done;}
    for(size_t i=0;i<count;i++)ordered[starts[entries[i].shard]+fill[entries[i].shard]++]=&entries[i];
    size_t wi=0;
    for(int i=0;i<sc->splits;i++)if(counts[i])workers[wi++]=(ReadShard){db,c,&memory,ordered+starts[i],counts[i],0};
    parallel_for_io(batch_shard_worker,workers,(int)nworkers,sizeof(*workers));
    for(size_t i=0;i<nworkers;i++)if(workers[i].error&&!c->error)c->error=workers[i].error;
    free(workers);workers=NULL;free(ordered);ordered=NULL;free(counts);counts=NULL;free(starts);starts=NULL;free(fill);fill=NULL;
    if(c->error)goto done;
    size_t metadata=capacity*sizeof(*entries),results=0;
    for(size_t i=0;i<count;i++){metadata+=entries[i].wirecharge;results+=entries[i].retained;}
    c->retained+=metadata+results;
    for(size_t i=0;i<count&&!c->error;i++){
        ReadEntry *e=&entries[i];
        if(c->options->kind!=TYPED_READ_NOT_EXISTS){
            if(c->count&&!c->csv&&read_literal(c,","))break;
            if(c->csv&&c->options->kind==TYPED_READ_GET_MULTI&&e->found){}
            else if(c->csv){
                if(read_csv_key(c,e->wire,e->wirelen))break;
                char separator[2]={c->options->delimiter,0};
                if((c->options->kind==TYPED_READ_EXISTS_MULTI||c->nfields)&&read_literal(c,separator))break;
            }else if(read_quote(c,e->wire,e->wirelen)||read_literal(c,":"))break;
        }
        if(c->options->kind==TYPED_READ_GET_MULTI){
            if(e->found){
                if(c->tail)c->tail->next=e->head;else c->head=e->head;
                if(e->tail)c->tail=e->tail;
                e->head=e->tail=NULL;
            }else if(c->csv){
                char separator[2]={c->options->delimiter,0};
                /* The first separator was emitted with the missing key. */
                for(size_t j=1;j<c->nfields&&!c->error;j++)read_literal(c,separator);
                read_literal(c,"\n");
            }else read_literal(c,"null");
            c->count++;
        }else if(c->options->kind==TYPED_READ_EXISTS_MULTI){read_literal(c,e->found?"true":"false");if(c->csv)read_literal(c,"\n");c->count++;}
        else if(!e->found){if(c->count&&!c->csv)read_literal(c,",");if(c->csv){read_csv_key(c,e->wire,e->wirelen);read_literal(c,"\n");}else read_quote(c,e->wire,e->wirelen);c->count++;}
    }
    c->retained-=metadata;
done:
    for(size_t i=0;i<count;i++){free(entries[i].wire);batch_parts_free(&entries[i]);}
    free(entries);free(ordered);free(counts);free(starts);free(fill);free(workers);
    return c->error?-1:0;
}
int typed_read_command(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const TypedReadOptions *o,FILE *out){
    if(o&&o->format&&!strcmp(o->format,"csv")&&fs&&fs->ts&&fs->ts->packed.nfields){
        PackedCsvOptions csv={.fields=o->fields,.delimiter=o->delimiter,.buffer_budget=o->budget,.timeout_ms=o->timeout_ms};
        if(o->kind==TYPED_READ_FETCH)return packed_csv_fetch(db,fs,o->offset,o->limit,o->cursor,o->want_total,&csv,out);

    }
    ReadCommand c;if(read_open(&c,fs,schema,o,out))return read_finish(&c,out);
    if(!db){c.error=EIO;return read_finish(&c,out);}
    if(o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_KEYS){
        if(o->limit<=0){c.error=EINVAL;}
        else{
            if(o->kind==TYPED_READ_KEYS)c.skip=o->offset>0?o->offset:0;
            int rc=slotcask_walk_live_skip_checked(db,c.skip,read_walk,&c);
            if(rc<0&&!c.error)c.error=EIO;
        }
    }else read_keys_json(&c,db);
    if(!c.error)read_deadline(&c);
    if(!c.error){
        if(o->kind==TYPED_READ_FETCH){
            if(c.csv){}
            else if(c.rows)read_literal(&c,"]}\n");
            else{
                read_literal(&c,c.dict?"}":"]");
                if(o->want_total)read_literal(&c,",\"total\":null}\n");
                else if(c.count==(size_t)o->limit){
                    if(c.start>INT64_MAX-(int64_t)c.count)c.error=EOVERFLOW;
                    else{char tail[96];snprintf(tail,sizeof(tail),",\"cursor\":\"%lld\"}\n",(long long)(c.start+(int64_t)c.count));read_literal(&c,tail);}
                }else read_literal(&c,",\"cursor\":null}\n");
            }
        }else if(!c.csv)read_literal(&c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"]\n":"}\n");
    }
    return read_finish(&c,out);
}
int typed_read_single(SlotcaskDb *db,const FieldSchema *fs,const void *key,
    size_t keylen,const char *fields,size_t budget,FILE *out){
    TypedReadOptions options={.fields=fields,.budget=budget,.kind=TYPED_READ_GET_MULTI};
    ReadCommand c;memset(&c,0,sizeof(c));c.fs=fs;c.options=&options;
    if(!db||!fs||!fs->ts||!fs->ts->typed||!key||!keylen||keylen>MAX_KEY_CEILING||!out)
        return read_error(out,EINVAL);
    if(read_selection(&c))return read_finish(&c,out);
    c.key=key;c.keylen=keylen;
    uint8_t hash[16];compute_hash_raw(key,keylen,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0&&!c.error)c.error=EIO;
    if(!c.found&&!c.error)c.error=ENOENT;
    if(!c.error)read_literal(&c,"\n");
    return read_finish(&c,out);
}

int typed_read_route(const char *root,const char *object,const TypedReadOptions *o){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_command(db,&fs,&sc,o,g_out?g_out:stdout);
}
int typed_read_single_route(const char *root,const char *object,const void *key,
    size_t len,const char *fields){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_single(db,&fs,key,len,fields,g_query_buffer_max_bytes,g_out?g_out:stdout)!=0;
}
int typed_read_exists(const char *root,const char *object,const void *key,size_t len){
    FILE *out=g_out?g_out:stdout;
    if(!key||!len||len>MAX_KEY_CEILING)return read_error(out,EINVAL);
    Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    if(!db)return read_error(out,EIO);
    TypedReadOptions options={.kind=TYPED_READ_EXISTS_MULTI};
    ReadCommand c={.key=key,.keylen=len,.options=&options};
    uint8_t hash[16];compute_hash_raw(key,len,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0)return read_error(out,EIO);
    const char *text=c.found?"true\n":"false\n";size_t n=strlen(text);
    if(fwrite(text,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    return c.found?0:1;
}
