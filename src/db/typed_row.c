#include "typed_row.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int row_error(char *err, size_t cap, int code, const char *message) {
    if (err && cap) snprintf(err, cap, "%s", message);
    errno = code; return -1;
}
static uint32_t packed_fixed_width(const TypedField *f){
    switch(f->type){
    case FT_BOOL:case FT_BYTE:return 1;
    case FT_SHORT:return 2;
    case FT_TIME:return 3;
    case FT_INT:case FT_FLOAT:case FT_DATE:case FT_IPV4:return 4;
    case FT_DATETIME:return 7;
    case FT_LONG:case FT_DOUBLE:case FT_TIMESTAMP:case FT_DATETIMEMS:return 8;
    case FT_UUID:case FT_IPV6:return 16;
    case FT_ENUM:
        if(f->n_enum_values<1||f->n_enum_values>65535||!f->enum_values||
           f->enum_width!=(f->n_enum_values<=256?1:2))return 0;
        return (uint32_t)f->enum_width;
    default:return 0;
    }
}
static int packed_schema_ok(const TypedSchema *ts) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS ||
        ts->packed.nfields != (uint32_t)ts->nfields ||
        ts->packed.prefix != (uint32_t)ts->nullmap_prefix ||
        ts->packed.maximum != (uint32_t)ts->total_size) return 0;
    for (int i=0;i<ts->nfields;i++) {
        const PkrField *p=&ts->packed.fields[i];const TypedField *f=&ts->fields[i];
        if ((p->kind==PKR_FIXED &&
             (packed_fixed_width(f)==0 || packed_fixed_width(f)!=p->width)) ||
            f->name_len<1 || f->name_len>=(int)sizeof(f->name) ||
            strnlen(f->name,sizeof(f->name))!=(size_t)f->name_len ||
            f->offset<0 || f->size<0 || (uint32_t)f->offset!=p->offset ||
            (uint32_t)f->size!=p->width ||
            (p->kind==PKR_VARCHAR && f->type!=FT_VARCHAR) ||
            (p->kind==PKR_NUMERIC && f->type!=FT_NUMERIC) ||
            (p->kind==PKR_FIXED && (f->type==FT_VARCHAR||f->type==FT_NUMERIC)))
            return 0;
    }
    return 1;
}
int typed_schema_bind_packed(TypedSchema *ts,const char *const *types,
                              size_t n,char *err,size_t errsz) {
    if (!ts||!types||!ts->typed||n<1||n>MAX_FIELDS||n!=(size_t)ts->nfields)
        return row_error(err,errsz,EINVAL,"invalid packed schema");
    PkrField fields[MAX_FIELDS];enum FieldType kinds[MAX_FIELDS];
    for(size_t i=0;i<n;i++){
        const TypedField *field=&ts->fields[i];
        if(field->name_len<1||field->name_len>=(int)sizeof(field->name)||
           strnlen(field->name,sizeof(field->name))!=(size_t)field->name_len)
            return row_error(err,errsz,EINVAL,"invalid field name");
        if(!types[i])return row_error(err,errsz,EINVAL,"missing type token");
        if(!strncmp(types[i],"varchar",7)||!strncmp(types[i],"numeric",7)||
           !strcmp(types[i],"currency")){
            if(pkr_field_parse(types[i],&fields[i])!=DEC_OK)
                return row_error(err,errsz,EINVAL,"invalid packed declaration");
            kinds[i]=fields[i].kind==PKR_VARCHAR?FT_VARCHAR:FT_NUMERIC;
            if(ts->fields[i].type!=FT_NONE&&ts->fields[i].type!=kinds[i])
                return row_error(err,errsz,EINVAL,"packed logical type mismatch");
        }else{
            if(!strncmp(types[i],"enum(",5)){
                const char *close=strrchr(types[i],')');
                if(!close||close[1])
                    return row_error(err,errsz,EINVAL,"enum token contains trailing declaration text");
            }
            TypedField check={0};errno=0;parse_field_type(types[i],&check);
            int parse_errno=errno;
            int good=check.type!=FT_NONE&&check.type==ts->fields[i].type&&
                check.size==ts->fields[i].size&&check.size>=1&&check.size<=16;
            if(check.type==FT_ENUM){
                good=good&&check.n_enum_values==ts->fields[i].n_enum_values;
                for(int k=0;good&&k<check.n_enum_values;k++)
                    good=!strcmp(check.enum_values[k],ts->fields[i].enum_values[k]);
            }
            free_enum_values(&check);
            if(!good||pkr_field_fixed((uint32_t)ts->fields[i].size,&fields[i])!=DEC_OK)
                return row_error(err,errsz,parse_errno==ENOMEM?ENOMEM:EINVAL,"invalid fixed declaration");
            kinds[i]=ts->fields[i].type;
        }
    }
    PkrSchema next;
    if(pkr_schema_build(fields,n,&next)!=DEC_OK)
        return row_error(err,errsz,ERANGE,"packed schema exceeds bounds");
    for(size_t i=0;i<n;i++){
        ts->fields[i].type=kinds[i];ts->fields[i].offset=(int)next.fields[i].offset;
        ts->fields[i].size=(int)next.fields[i].width;
        if(kinds[i]==FT_NUMERIC){
            ts->fields[i].numeric_scale=next.fields[i].scale;
            ts->fields[i].numeric_scale_mult=0;
        }
    }
    ts->packed=next;ts->nullmap_prefix=(int)next.prefix;
    ts->total_size=(int)next.maximum;return 0;
}
uint8_t typed_schema_hdr2(const TypedSchema *ts) {
    return ts&&ts->packed.nfields?PKR_FLAGS:SREC_FLAG_NULLMAP;
}
int typed_row_open(const TypedSchema *ts,const uint8_t *value,size_t len,
                    uint8_t hdr2,TypedRowView *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!ts||!ts->typed||ts->nfields<1||ts->nfields>MAX_FIELDS||!value){
        errno=EINVAL;return -1;}
    TypedRowView v={0};v.schema=ts;v.value=value;v.len=len;v.hdr2=hdr2;
    if(ts->packed.nfields){
        if(!packed_schema_ok(ts)){errno=EILSEQ;return -1;}
        dec_status st=pkr_view(&ts->packed,value,len,hdr2,&v.packed);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        v.base=value+ts->packed.prefix;v.region=len-ts->packed.prefix;
        v.bitmap=value+1;v.bitmap_bytes=(int)ts->packed.prefix-1;
    }else{
        if(typed_record_view(value,len,hdr2,&v.base,&v.bitmap,
                              &v.bitmap_bytes,&v.region)!=0){
            errno=EILSEQ;return -1;}
    }
    *out=v;return 0;
}
int typed_row_span(const TypedRowView *v,size_t index,TypedRowSpan *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!v||!v->schema||index>=(size_t)v->schema->nfields){errno=EINVAL;return -1;}
    if(v->schema->packed.nfields){
        PkrSpan p;if(pkr_span(&v->packed,index,&p)!=DEC_OK){errno=EILSEQ;return -1;}
        out->data=p.data;out->len=p.len;out->is_null=p.state==PKR_NULL;
        out->complete=1;return 0;
    }
    const TypedField *f=&v->schema->fields[index];
    if(f->offset<0||f->size<1||f->size>65537){errno=EILSEQ;return -1;}
    out->is_null=typed_field_null(v->bitmap,v->bitmap_bytes,(int)index);
    size_t off=(size_t)f->offset,cap=(size_t)f->size;
    if(off<v->region){
        out->len=v->region-off;if(out->len>cap)out->len=cap;
        out->data=v->base+off;
    }
    out->complete=out->len==cap;return 0;
}
int typed_span_read(const TypedSchema *ts,size_t index,const TypedRowSpan *s,
                      const uint8_t **out) {
    static const uint8_t zero[65537];
    if(!out){errno=EINVAL;return -1;}*out=NULL;
    if(!ts||!s||index>=(size_t)ts->nfields||s->is_null){errno=EINVAL;return -1;}
    if(ts->packed.nfields){if(!s->complete){errno=EILSEQ;return -1;}*out=s->data;}
    else *out=s->complete?s->data:zero;
    return 0;
}
int typed_span_copy_legacy(const TypedSchema *ts,size_t index,
                             const TypedRowSpan *s,uint8_t *out,size_t cap){
    if(!ts||ts->packed.nfields||index>=(size_t)ts->nfields||!s||!out||
       ts->fields[index].size<1||cap<(size_t)ts->fields[index].size){
        errno=EINVAL;return -1;}
    size_t width=(size_t)ts->fields[index].size;
    if(s->len>width||(!s->data&&s->len)){errno=EILSEQ;return -1;}
    memset(out,0,width);if(s->len)memcpy(out,s->data,s->len);return 0;
}
void typed_prepared_init(TypedPrepared *p){if(p)memset(p,0,sizeof(*p));}
void typed_prepared_free(TypedPrepared *p){
    if(!p)return;
    for(size_t i=0;i<MAX_FIELDS;i++)free(p->owned[i]);
    memset(p,0,sizeof(*p));
}
static int prepared_check(const TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(p->schema))
        return row_error(err,cap,EINVAL,"invalid prepared schema");
    return 0;
}
/* Legacy scalar helpers report status/text but do not promise errno for
   validation failures. Establish the new adapter boundary's error contract. */
static int row_encode_fixed(const TypedField *field,const char *text,size_t len,
                              int quoted,uint8_t *out,char *err,size_t cap){
    errno=0;
    int rc=encode_json_field_checked(field,text,len,quoted,out,err,cap);
    if(rc)errno=errno==ENOMEM?ENOMEM:EINVAL;
    return rc;
}
static size_t prepared_heap_used(const TypedPrepared *p){
    size_t used=0;
    for(int i=0;i<p->schema->nfields;i++)if(p->owned[i]){
        size_t n=p->cells[i].len?p->cells[i].len:1;
        if(n>SIZE_MAX-used)return SIZE_MAX;
        used+=n;
    }
    return used;
}
static int replace_cell(TypedPrepared *p,size_t i,uint8_t *owner,
                          const uint8_t *data,size_t n,int is_null,
                          char *err,size_t cap){
    const PkrField *f=&p->schema->packed.fields[i];
    size_t prior=p->cells[i].is_null||f->kind==PKR_FIXED?0:p->cells[i].len;
    size_t added=is_null||f->kind==PKR_FIXED?0:n;
    if(p->bytes<prior||added>SLOTCASK_SEG_MAX_BYTES-24||
       p->bytes-prior>SLOTCASK_SEG_MAX_BYTES-24-added){
        free(owner);return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");}
    size_t heap=prepared_heap_used(p),allocation=owner?(n?n:1):0;
    if(heap>g_query_buffer_max_bytes||allocation>g_query_buffer_max_bytes-heap){
        free(owner);return row_error(err,cap,ENOMEM,"query memory buffer exceeded");}
    free(p->owned[i]);p->owned[i]=owner;p->cells[i]=(PkrCell){data,n,(uint8_t)is_null};
    p->bytes=p->bytes-prior+added;return 0;
}
int typed_prepared_set(TypedPrepared *p,size_t i,const TypedInput *in,
                        char *err,size_t cap){
    if(prepared_check(p,err,cap)||!in||i>=(size_t)p->schema->nfields)
        return row_error(err,cap,EINVAL,"invalid field input");
    if(!in->supplied)return 0;
    const TypedField *f=&p->schema->fields[i];const PkrField *pf=&p->schema->packed.fields[i];
    if(f->removed)return 0;
    if(in->is_null){
        if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;return -1;}
        int rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);
        if(!rc)p->seen[i]=1;
        return rc;
    }
    if(!in->data&&in->len)return row_error(err,cap,EINVAL,"invalid value span");
    const char *text=in->data?in->data:"";size_t n=in->len;char *decoded=NULL;
    size_t heap=prepared_heap_used(p),remaining=heap<=g_query_buffer_max_bytes?g_query_buffer_max_bytes-heap:0;
    size_t factor=pf->kind==PKR_NUMERIC?12u:2u;
    if(remaining<1024||n>(remaining-1024)/factor)
        return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    int string_type=f->type==FT_VARCHAR||f->type==FT_NUMERIC||
        f->type==FT_ENUM||f->type==FT_DATE||f->type==FT_DATETIME||
        f->type==FT_DATETIMEMS||f->type==FT_TIME||f->type==FT_UUID||
        f->type==FT_IPV4||f->type==FT_IPV6;
    int was_quoted=in->quoted;
    if(was_quoted&&string_type){
        errno=0;
        int rc=f->type==FT_VARCHAR?json_unescape_string(text,n,&decoded,&n):
            json_unescape_cstring(text,n,&decoded,&n);
        if(rc)return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"malformed JSON string");
        text=decoded;was_quoted=0;
    }
    uint8_t *bytes=NULL;size_t used=0;int rc=-1;
    if(pf->kind==PKR_VARCHAR){
        if(n>pf->payload_max){row_error(err,cap,ERANGE,"varchar content exceeds declaration");goto done;}
        bytes=malloc(n?n:1);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(n)memcpy(bytes,text,n);
        used=n;
    }else if(pf->kind==PKR_NUMERIC){
        dec_t a,b;dec_init(&a);dec_init(&b);
        dec_status st=dec_parse(text,n,&a,err,cap);const dec_t *value=&a;
        if(st==DEC_OK&&!pf->bare){
            st=dec_apply_precision(&a,pf->precision,pf->scale,&b,err,cap);value=&b;}
        if(st==DEC_OK){
            used=dec_payload_len(value);
            if(used>pf->payload_max)st=DEC_ERR_RANGE;
            else{bytes=malloc(used);if(!bytes)st=DEC_ERR_NOMEM;
                else st=dec_pack(value,bytes,used,&used);}
        }
        dec_free(&b);dec_free(&a);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EINVAL;goto done;}
    }else{
        if(f->type==FT_ENUM&&n&&enum_value_index(f,text,n)<0){
            if(err&&cap)snprintf(err,cap,"unknown enum value for field '%s'",f->name);
            errno=EINVAL;goto done;
        }
        if(!sentinel_span_parses(f->type,text,n)){
            if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;goto done;}
            rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);goto done;
        }
        bytes=calloc(1,pf->width);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(row_encode_fixed(f,text,n,was_quoted,bytes,err,cap)!=0)goto done;
        used=pf->width;
    }
    rc=replace_cell(p,i,bytes,bytes,used,0,err,cap);bytes=NULL;
done:if(!rc)p->seen[i]=1;
    int saved_errno=errno;
    free(bytes);free(decoded);
    if(rc)errno=saved_errno;
    return rc;
}
int typed_prepared_zero(const TypedSchema *ts,TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++)p->cells[i]=(PkrCell){NULL,0,1};
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        const PkrField *pf=&ts->packed.fields[i];
        if(pf->kind==PKR_FIXED){
            uint8_t *z=calloc(1,pf->width);if(!z){typed_prepared_free(p);return row_error(err,cap,ENOMEM,"oom");}
            if(replace_cell(p,(size_t)i,z,z,pf->width,0,err,cap))goto fail;
        }else{
            TypedInput in={pf->kind==PKR_NUMERIC?"0":"",pf->kind==PKR_NUMERIC?1:0,1,0,0};
            if(typed_prepared_set(p,(size_t)i,&in,err,cap))goto fail;
        }
    }
    memset(p->seen,0,sizeof(p->seen));return 0;
fail:typed_prepared_free(p);return -1;
}
int typed_prepared_old(const TypedSchema *ts,const SlotcaskOldRecord *old,
                        TypedPrepared *p,char *err,size_t cap){
    if(!p||!old||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid OLD");
    size_t retained=p->schema?prepared_heap_used(p):0;
    size_t available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
    if(!typed_packed_render_allowance(ts,old->value,old->vlen,old->hdr2,NULL,available))
        return row_error(err,cap,errno,"OLD validation exceeds query memory or has invalid structure");
    TypedRowView v;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&v))
        return row_error(err,cap,errno,"invalid OLD record");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++){
        TypedRowSpan sp;if(typed_row_span(&v,(size_t)i,&sp))goto fail;
        int null=sp.is_null||ts->fields[i].removed;
        p->cells[i]=(PkrCell){null?NULL:sp.data,null?0:sp.len,(uint8_t)null};
        if(!null&&ts->packed.fields[i].kind!=PKR_FIXED)p->bytes+=sp.len;
    }
    if(p->bytes>SLOTCASK_SEG_MAX_BYTES-24){
        typed_prepared_free(p);
        return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");
    }
    return 0;
fail:typed_prepared_free(p);return row_error(err,cap,EILSEQ,"invalid OLD field");
}
static int prepared_defaults_mode(TypedPrepared *p,const char *root,const char *object,
    int defer_ac,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||p->seen[i]||(defer_ac&&f->default_kind==DK_AUTO_CREATE))continue;
        TypedInput in={NULL,0,1,1,0};char *owned=NULL;
        if(f->default_kind!=DK_NONE){
            size_t heap=prepared_heap_used(p),available=heap<g_query_buffer_max_bytes?g_query_buffer_max_bytes-heap:0;
            /* Generator text overlaps its checked payload copy. Account the
             * already retained owners before requesting either allocation. */
            size_t generator_budget=available>2048?(available-2048)/2:0;
            if(!generator_budget)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
            owned=generate_default_owned(f,root,object,generator_budget);
            if(!owned)return row_error(err,cap,errno,errno==ENOMEM||errno==ENOBUFS?"query memory buffer exceeded":"default generator failed");
            in.data=owned;in.len=strlen(owned);in.is_null=0;
        }
        int rc=typed_prepared_set(p,(size_t)i,&in,err,cap);int saved=errno;
        free(owned);if(rc){errno=saved;return -1;}
    }
    return 0;
}
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    return prepared_defaults_mode(p,root,object,0,err,cap);
}
int typed_prepared_stamp(TypedPrepared *p,enum DefaultKind kind,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    if(kind!=DK_AUTO_CREATE&&kind!=DK_AUTO_UPDATE)
        return row_error(err,cap,EINVAL,"invalid timestamp kind");
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||f->default_kind!=kind)continue;
        char b[256];const char *v=generate_default(f,b,sizeof(b),NULL,NULL);
        if(!v)return row_error(err,cap,EIO,"automatic timestamp failed");
        TypedInput in={v,strlen(v),1,0,0};
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
static int prepared_valid(const TypedPrepared *p,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        if(!p->schema->fields[i].removed&&p->schema->fields[i].not_null&&p->cells[i].is_null){
            fmt_not_null_err(&p->schema->fields[i],err,cap);errno=EINVAL;return -1;}
    }return 0;
}
int typed_prepared_compose(const TypedPrepared *p,uint8_t *out,size_t cap,
                            size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    if(prepared_valid(p,err,errsz))return -1;
    dec_status st=pkr_compose(&p->schema->packed,p->cells,(size_t)p->schema->nfields,out,cap,used);
    if(st!=DEC_OK)return row_error(err,errsz,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record composition failed");
    return 0;
}
int typed_prepared_alloc(const TypedPrepared *p,uint8_t **out,size_t *len,
                          char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(prepared_valid(p,err,cap))return -1;
    size_t need=0;dec_status st=pkr_measure(&p->schema->packed,p->cells,(size_t)p->schema->nfields,&need);
    if(st!=DEC_OK)return row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record sizing failed");
    if(prepared_heap_used(p)>g_query_buffer_max_bytes||need>g_query_buffer_max_bytes-prepared_heap_used(p))return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    uint8_t *buf=malloc(need);if(!buf)return row_error(err,cap,ENOMEM,"oom");
    size_t used=0;if(typed_prepared_compose(p,buf,need,&used,err,cap)){free(buf);return -1;}
    *out=buf;*len=used;return 0;
}

/* Validate syntax before span collection. json_skip_value locates spans but
   deliberately is not a validator. Depth is bounded without global state. */
static const char *row_json_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int row_json_string(const char **cursor){
    const unsigned char *p=(const unsigned char*)*cursor;
    if(*p++!='"')return -1;
    while(*p&&*p!='"'){
        if(*p<0x20)return -1;
        if(*p++!='\\')continue;
        unsigned char c=*p++;
        if(!c)return -1;
        if(c=='u'){
            for(int i=0;i<4;i++){
                unsigned char h=*p++;
                if(!((h>='0'&&h<='9')||(h>='a'&&h<='f')||(h>='A'&&h<='F')))return -1;
            }
        }else if(c!='"'&&c!='\\'&&c!='/'&&c!='b'&&c!='f'&&c!='n'&&c!='r'&&c!='t')return -1;
    }
    if(*p!='"')return -1;
    *cursor=(const char*)p+1;return 0;
}
static int row_json_value(const char **cursor,unsigned depth){
    const char *p=row_json_space(*cursor);
    if(*p=='"'){
        if(row_json_string(&p))return -1;
    }else if(*p=='{'||*p=='['){
        if(depth>=64)return -1;
        int object=*p=='{';char end=object?'}':']';p=row_json_space(p+1);
        if(*p!=end)for(;;){
            if(object){
                if(row_json_string(&p))return -1;
                p=row_json_space(p);if(*p++!=':')return -1;
            }
            if(row_json_value(&p,depth+1))return -1;
            p=row_json_space(p);if(*p==end)break;
            if(*p++!=',')return -1;
            p=row_json_space(p);
        }
        p++;
    }else if(!strncmp(p,"null",4))p+=4;
    else if(!strncmp(p,"true",4))p+=4;
    else if(!strncmp(p,"false",5))p+=5;
    else{
        if(*p=='-')p++;
        if(*p=='0')p++;
        else{
            if(*p<'1'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='.'){
            p++;if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='e'||*p=='E'){
            p++;if(*p=='+'||*p=='-')p++;
            if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
    }
    *cursor=p;return 0;
}
#include <math.h>
/* Computed operands are borrowed JSON spans; results own their payloads. */
typedef struct {int op,source;const char *literal;size_t len;int quoted;} RowCompute;
static int row_compute_parse(const TypedSchema *ts,size_t index,const char *json,
    size_t n,RowCompute *c,char *err,size_t cap){
    memset(c,0,sizeof(*c));
    const TypedField *f=&ts->fields[index];
    if(f->type!=FT_SHORT&&f->type!=FT_INT&&f->type!=FT_LONG&&
       f->type!=FT_NUMERIC&&f->type!=FT_FLOAT&&f->type!=FT_DOUBLE)
        return row_error(err,cap,EINVAL,"unsupported computed target");
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    p=row_json_space(p);if(*p!='"')goto bad;
    const char *key=p+1,*end=p;if(row_json_string(&end))goto bad;
    char *name=NULL;size_t length=0;
    if(json_unescape_cstring(key,(size_t)(end-key-1),&name,&length))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid computed operator");
    c->op=length==5&&!memcmp(name,"$incr",5)?1:
          length==5&&!memcmp(name,"$copy",5)?2:0;
    free(name);if(!c->op)goto bad;
    p=row_json_space(end);if(*p++!=':')goto bad;p=row_json_space(p);
    const char *v=p;if(row_json_value(&p,0))goto bad;
    size_t vn=(size_t)(p-v);p=row_json_space(p);
    if(*p++!='}'||p!=json+n)goto bad;
    c->quoted=vn>=2&&v[0]=='"'&&v[vn-1]=='"';
    if(c->op==1){
        if(!vn||(!c->quoted&&(v[0]=='{'||v[0]=='['||v[0]=='n'||v[0]=='t'||v[0]=='f')))goto bad;
        c->literal=c->quoted?v+1:v;c->len=c->quoted?vn-2:vn;return 0;
    }
    if(!c->quoted)goto bad;
    char *source=NULL;size_t sn=0;
    if(json_unescape_cstring(v+1,vn-2,&source,&sn))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid copy source");
    c->source=-1;
    for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed&&
       sn==(size_t)ts->fields[i].name_len&&!memcmp(source,ts->fields[i].name,sn)){c->source=i;break;}
    free(source);if(c->source<0)goto bad;
    const TypedField *sf=&ts->fields[c->source];
    const PkrField *a=&ts->packed.fields[index],*b=&ts->packed.fields[c->source];
    if(sf->type!=f->type||a->kind!=b->kind||a->width!=b->width||
       (a->kind==PKR_NUMERIC&&(a->bare!=b->bare||a->precision!=b->precision||a->scale!=b->scale)))goto bad;
    return 0;
bad:return row_error(err,cap,EINVAL,"malformed or incompatible computed value");
}
static int row_compute_apply(TypedPrepared *p,const SlotcaskOldRecord *old,
    const RowCompute *computes,char *err,size_t cap){
    const TypedSchema *ts=p->schema;TypedRowView view;
    size_t retained=prepared_heap_used(p),available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
    if(!typed_packed_render_allowance(ts,old->value,old->vlen,old->hdr2,NULL,available))
        return row_error(err,cap,errno,"OLD validation exceeds query memory or has invalid structure");
    if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))return -1;
    for(int i=0;i<ts->nfields;i++){
        const RowCompute *c=&computes[i];if(!c->op)continue;
        const TypedField *f=&ts->fields[i];const PkrField *pf=&ts->packed.fields[i];
        TypedRowSpan sp;
        if(typed_row_span(&view,(size_t)i,&sp))return -1;
        if(sp.is_null)return row_error(err,cap,EINVAL,"NULL computed target");
        if(c->op==2){
            if(computes[c->source].op)return row_error(err,cap,EINVAL,"copy source is computed");
            if(typed_row_span(&view,(size_t)c->source,&sp))return -1;
            if(sp.is_null)return row_error(err,cap,EINVAL,"NULL copy source");
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            if(f->type==FT_DOUBLE){double v;memcpy(&v,sp.data,8);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            retained=prepared_heap_used(p);available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
            if(sp.len>available)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
            uint8_t *bytes=malloc(sp.len?sp.len:1);
            if(!bytes)return row_error(err,cap,ENOMEM,"oom");
            if(sp.len)memcpy(bytes,sp.data,sp.len);
            if(replace_cell(p,(size_t)i,bytes,bytes,sp.len,0,err,cap))return -1;
            p->seen[i]=1;continue;
        }
        retained=prepared_heap_used(p);available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
        if(available<1024||sp.len>(available-1024)/8||c->len>(available-1024-8*sp.len)/12)
            return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
        char *decoded=NULL;const char *literal=c->literal;size_t length=c->len;
        if(c->quoted){
            if(json_unescape_cstring(literal,length,&decoded,&length))
                return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid increment");
            literal=decoded;
        }
        int rc=-1;char text[128];
        if(f->type==FT_FLOAT||f->type==FT_DOUBLE){
            char *operand=malloc(length+1);
            if(!operand){free(decoded);return row_error(err,cap,ENOMEM,"oom");}
            memcpy(operand,literal,length);operand[length]=0;
            char *end=NULL;errno=0;double delta=strtod(operand,&end);
            int valid=end==operand+length&&end!=operand&&errno!=ERANGE&&isfinite(delta);
            double current;
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);current=v;}
            else memcpy(&current,sp.data,8);
            free(operand);
            double result=current+delta;
            if(valid&&isfinite(current)&&isfinite(result)){
                int written;
                if(f->type==FT_FLOAT){float v=(float)result;written=isfinite(v)?snprintf(text,sizeof(text),"%.9g",(double)v):-1;}
                else written=snprintf(text,sizeof(text),"%.17g",result);
                if(written>0&&(size_t)written<sizeof(text)){
                    TypedInput in={text,(size_t)written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);
                }
            }
            if(rc)row_error(err,cap,EINVAL,"invalid or overflowing floating increment");
        }else{
            dec_t current,delta,sum;dec_init(&current);dec_init(&delta);dec_init(&sum);
            dec_status st;
            if(pf->kind==PKR_NUMERIC)st=dec_unpack(sp.data,sp.len,&current);
            else{
                uint64_t bits=0;for(size_t j=0;j<sp.len;j++)bits=(bits<<8)|sp.data[j];
                if(sp.len<8&&(sp.data[0]&128))bits|=UINT64_MAX<<(sp.len*8);
                int64_t value;memcpy(&value,&bits,8);
                int written=snprintf(text,sizeof(text),"%lld",(long long)value);
                st=dec_parse(text,(size_t)written,&current,err,cap);
            }
            if(st==DEC_OK&&pf->kind!=PKR_NUMERIC){
                char *integer=malloc(length+1);
                if(!integer)st=DEC_ERR_NOMEM;
                else{
                    memcpy(integer,literal,length);integer[length]=0;char *end=NULL;errno=0;
                    (void)strtoll(integer,&end,10);
                    int valid=end!=integer&&errno!=ERANGE;
                    while(*end==' '||*end=='\t'||*end=='\r'||*end=='\n'||*end=='\f'||*end=='\v')end++;
                    if(*end||!valid)st=DEC_ERR_SYNTAX;
                    free(integer);
                }
            }
            if(st==DEC_OK)st=dec_parse(literal,length,&delta,err,cap);
            if(st==DEC_OK){
                int64_t high=current.ndigits?current.weight:delta.weight;
                int64_t low=current.ndigits?(int64_t)current.weight-current.ndigits+1:(int64_t)delta.weight-delta.ndigits+1;
                if(delta.ndigits){if(delta.weight>high)high=delta.weight;int64_t dlow=(int64_t)delta.weight-delta.ndigits+1;if(dlow<low)low=dlow;}
                uint64_t span=high>=low?(uint64_t)(high-low+2)*4:4;
                int64_t integer=(high+1)*4;if(integer<1)integer=1;
                int64_t scale=current.dscale>delta.dscale?current.dscale:delta.dscale;
                uint64_t rendered=(uint64_t)integer+(uint64_t)scale+4;
                uint64_t needed=1024+16*span+16*rendered+8*(uint64_t)sp.len+12*(uint64_t)c->len;
                if(needed>available)st=DEC_ERR_NOMEM;
            }
            if(st==DEC_OK)st=dec_add(&current,&delta,&sum);
            if(st==DEC_OK){
                size_t n=dec_text_len(&sum);char *exact=n?malloc(n):NULL;
                if(!exact)st=DEC_ERR_NOMEM;
                else{
                    size_t written=0;st=dec_to_text(&sum,exact,n,&written);
                    if(st==DEC_OK){TypedInput in={exact,written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);}
                    free(exact);
                }
            }
            dec_free(&sum);dec_free(&delta);dec_free(&current);
            if(st!=DEC_OK)rc=row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"invalid or overflowing exact increment");
        }
        free(decoded);if(rc)return -1;
    }
    return 0;
}

static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,const SlotcaskOldRecord *old,int computed,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    size_t request_length=strnlen(json,g_query_buffer_max_bytes);
    if(request_length>=g_query_buffer_max_bytes/8)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(old?typed_prepared_old(ts,old,out,err,cap):typed_prepared_zero(ts,out,err,cap))return -1;
    RowCompute computes[MAX_FIELDS]={0};
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    int first=1;
    while(1){
        p=row_json_space(p);if(*p=='}'){p++;break;}
        if(!first){if(*p++!=',')goto bad;p=row_json_space(p);}
        if(*p!='"')goto bad;
        const char *key=p+1,*after=p;
        if(row_json_string(&after))goto bad;
        size_t kn=(size_t)(after-key-1);p=row_json_space(after);
        if(*p++!=':')goto bad;
        p=row_json_space(p);
        const char *v=p;
        if(row_json_value(&p,0))goto bad;
        size_t n=(size_t)(p-v);char *name=NULL;size_t name_len=0;
        /* Longer encoded keys cannot equal a <=255-byte schema name. */
        if(kn<=6*(sizeof(ts->fields[0].name)-1)){
            errno=0;
            if(json_unescape_string(key,kn,&name,&name_len)){
                row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid field name");goto fail;
            }
        }
        for(int i=0;name&&i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];
            if(f->removed||name_len!=(size_t)f->name_len||memcmp(f->name,name,name_len))continue;
            int quoted=n>=2&&v[0]=='"'&&v[n-1]=='"';
            if(!quoted&&(v[0]=='{'||v[0]=='[')){
                if(!computed||!old||v[0]!='{'){free(name);goto bad;}
                if(row_compute_parse(ts,(size_t)i,v,n,&computes[i],err,cap)){free(name);goto fail;}
                RowCompute one[MAX_FIELDS]={0};one[i]=computes[i];
                if(row_compute_apply(out,old,one,err,cap)){free(name);goto fail;}
                break;
            }
            computes[i]=(RowCompute){0};
            TypedInput in={quoted?v+1:v,quoted?n-2:n,1,
                (uint8_t)(!quoted&&json_span_is_null(v,n)),(uint8_t)quoted};
            if(typed_prepared_set(out,(size_t)i,&in,err,cap)){free(name);goto fail;}
            break;
        }
        free(name);first=0;
    }
    if(*row_json_space(p))goto bad;
    if(computed)for(int i=0;i<ts->nfields;i++)if(computes[i].op==2&&computes[computes[i].source].op){
        row_error(err,cap,EINVAL,"copy source is computed");goto fail;
    }
    if(defaults&&typed_prepared_defaults(out,root,object,err,cap))goto fail;
    return 0;
bad:row_error(err,cap,EINVAL,"invalid record JSON");
fail:typed_prepared_free(out);return -1;
}
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
int typed_encode_alloc(const TypedSchema *ts,const char *json,int defaults,
                         const char *root,const char *object,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!ts->typed||ts->total_size<1)
        return row_error(err,cap,EINVAL,"invalid typed schema");
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    if(!ts->packed.nfields){
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        errno=0;
        int rc=defaults?typed_encode_defaults(ts,json,b,ts->total_size,root,object,err,cap):
            typed_encode(ts,json,b,ts->total_size,err,cap);
        if(rc<0){int cause=errno==ENOMEM?ENOMEM:EINVAL;free(b);errno=cause;return rc;}
        *out=b;*len=(size_t)rc;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
/* Validate a complete JSON value before bulk record collection. */
int typed_json_validate(const char *json){
    if(!json)return row_error(NULL,0,EINVAL,"invalid JSON");
    const char *p=json;
    if(row_json_value(&p,0)||*row_json_space(p))
        return row_error(NULL,0,EINVAL,"invalid JSON");
    return 0;
}
static int prepared_insert_finish(TypedPrepared *p,const SlotcaskOldRecord *old,
    uint8_t **out,size_t *len,char *err,size_t cap){
    const TypedSchema *ts=p->schema;int rc=0;
    if(old){
        TypedRowView view;
        if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))
            rc=row_error(err,cap,EILSEQ,"invalid OLD record");
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;
            if(typed_row_span(&view,(size_t)i,&span)){
                rc=row_error(err,cap,EILSEQ,"invalid OLD field");break;}
            rc=replace_cell(p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else rc=typed_prepared_stamp(p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(p,out,len,err,cap);
    return rc;
}
/* Complete packed insertion against locked OLD. Ordinary defaults run once;
   auto_create is stamped only when fresh, or copied with OLD nullness.
   Public encode/default APIs retain their existing independent contract. */
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,root,object,&p,NULL,0,err,cap);
    if(!rc)rc=prepared_defaults_mode(&p,root,object,1,err,cap);
    if(!rc)rc=prepared_insert_finish(&p,old,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!inputs)return row_error(err,cap,EINVAL,"invalid packed inputs");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=typed_prepared_zero(ts,&p,err,cap);
    for(int i=0;!rc&&i<ts->nfields;i++)
        if(!ts->fields[i].removed&&inputs[i].supplied)
            rc=typed_prepared_set(&p,(size_t)i,&inputs[i],err,cap);
    if(!rc)rc=prepared_insert_finish(&p,old,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

/* Packed scalar JSON patch. Known object/array values, including computed
   expressions, fail before composition; legacy computed semantics stay in
   storage.c until Phase 4 supplies their exact-decimal replacement. */
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,0,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_computed_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,1,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                         const TypedInput *inputs,int stamp,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!old)return row_error(err,cap,EINVAL,"invalid update");
    if(!ts->packed.nfields){
        if(ts->total_size<1)return row_error(err,cap,EINVAL,"invalid schema");
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        if(update_compose_new_from_old(ts,old,b,NULL,NULL,err,cap)){
            free(b);return row_error(err,cap,EILSEQ,"invalid OLD record");
        }
        if(inputs)for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];const TypedInput *in=&inputs[i];
            if(f->removed||!in->supplied)continue;
            uint8_t *dst=b+ts->nullmap_prefix+f->offset;
            if(in->is_null){
                if(f->not_null){fmt_not_null_err(f,err,cap);free(b);errno=EINVAL;return -1;}
                memset(dst,0,(size_t)f->size);typed_set_null(b+1,ts->nullmap_prefix-1,i);
            }else{
                if(in->len&&!in->data){free(b);return row_error(err,cap,EINVAL,"invalid value span");}
                if(row_encode_fixed(f,in->data?in->data:"",in->len,in->quoted,dst,err,cap)){
                    int cause=errno;free(b);errno=cause;return -1;
                }
                typed_clear_null(b+1,ts->nullmap_prefix-1,i);
            }
        }
        if(stamp)update_stamp_auto_update_fields(ts,b);
        *out=b;*len=(size_t)ts->total_size;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=typed_prepared_old(ts,old,&p,err,cap);
    if(!rc&&inputs)for(int i=0;i<ts->nfields;i++){
        if(typed_prepared_set(&p,(size_t)i,&inputs[i],err,cap)){rc=-1;break;}}
    if(!rc&&stamp)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_transfer_ac_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                             const uint8_t *value,size_t vlen,uint8_t **out,
                             size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    SlotcaskOldRecord current={value,vlen,PKR_FLAGS};TypedPrepared p;typed_prepared_init(&p);
    if(typed_prepared_old(ts,&current,&p,err,cap))return -1;
    int rc=0;
    if(old&&old->value){
        TypedRowView view;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))rc=-1;
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;if(typed_row_span(&view,(size_t)i,&span)){rc=-1;break;}
            rc=replace_cell(&p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else rc=typed_prepared_stamp(&p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

/* One bounded field description at a time; no varchar-sized scratch buffer.
   Packed-view validation and decimal workspace are separate from output budget. */
typedef struct {
    const char *data;
    size_t len;
    int quoted, numeric;
    char fixed[512];
    dec_t decimal;
} RowText;
static void row_text_free(RowText *text){dec_free(&text->decimal);}
static int row_text_open(const TypedRowView *view,size_t index,RowText *text){
    memset(text,0,sizeof(*text));dec_init(&text->decimal);
    TypedRowSpan span;if(typed_row_span(view,index,&span))return -1;
    const TypedField *f=&view->schema->fields[index];
    const PkrField *pf=&view->schema->packed.fields[index];
    if(span.is_null||f->removed){text->data="null";text->len=4;return 0;}
    if(pf->kind==PKR_VARCHAR){
        text->data=(const char*)span.data;text->len=span.len;text->quoted=1;return 0;
    }
    if(pf->kind==PKR_NUMERIC){
        dec_status st=dec_unpack(span.data,span.len,&text->decimal);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        size_t cap=dec_text_len(&text->decimal);
        if(!cap){errno=EILSEQ;return -1;}
        text->numeric=1;text->len=cap-1;return 0;
    }
    if(f->type==FT_ENUM){
        int i=f->enum_width==2?(int)(((uint16_t)span.data[0]<<8)|span.data[1]):span.data[0];
        /* Keep the legacy renderer's corrupt-enum empty-string policy. */
        text->data=i>=0&&i<f->n_enum_values&&f->enum_values&&f->enum_values[i]?
            f->enum_values[i]:"";
        text->len=strlen(text->data);text->quoted=1;return 0;
    }
    int n=decode_field_to_buf(f,span.data,text->fixed,sizeof(text->fixed),1);
    if(n<1||(size_t)n>=sizeof(text->fixed)){errno=EILSEQ;return -1;}
    text->data=text->fixed;text->len=(size_t)n;
    /* These fixed string renderings contain only their canonical ASCII. */
    if(n>=2&&text->fixed[0]=='"'&&text->fixed[n-1]=='"'){
        text->data++;text->len-=2;text->quoted=1;
    }
    return 0;
}
static int quoted_length(const char *data,size_t n,size_t *out){
    size_t len=2;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)data[i];
        size_t add=(c=='"'||c=='\\'||c=='\b'||c=='\f'||c=='\n'||c=='\r'||c=='\t')?2:
            c<0x20?6:1;
        if(len>SIZE_MAX-add){errno=EOVERFLOW;return -1;}len+=add;
    }
    *out=len;return 0;
}
static int quote_into(const char *data,size_t n,char *out,size_t cap,size_t *used){
    size_t need=0;if(quoted_length(data,n,&need))return -1;
    if(need>cap){errno=EOVERFLOW;return -1;}
    /* json_escape_into returns int. Each 4096-byte piece expands to at most
       24576 bytes, fitting even the minimum C INT_MAX; totals remain size_t. */
    size_t pos=1,offset=0;out[0]='"';
    while(offset<n){
        size_t piece=n-offset;if(piece>4096)piece=4096;
        int count=json_escape_into(out+pos,need-1-pos,data+offset,piece);
        if(count<0||(size_t)count>need-1-pos){errno=EOVERFLOW;return -1;}
        pos+=(size_t)count;offset+=piece;
    }
    if(pos!=need-1){errno=EOVERFLOW;return -1;}
    out[pos++]='"';*used=pos;return 0;
}
static int row_text_length(const RowText *text,int json,size_t *out){
    if(json&&text->quoted)return quoted_length(text->data,text->len,out);
    *out=text->len;return 0;
}
static int row_text_write(const RowText *text,int json,char *out,size_t cap,size_t *used){
    *used=0;
    if(text->numeric){
        dec_status st=dec_to_text(&text->decimal,out,cap,used);
        if(st!=DEC_OK){errno=EOVERFLOW;return -1;}return 0;
    }
    if(json&&text->quoted)return quote_into(text->data,text->len,out,cap,used);
    if(text->len>cap){errno=EOVERFLOW;return -1;}
    if(text->len)memcpy(out,text->data,text->len);
    *used=text->len;return 0;
}
char *typed_packed_field_text(const TypedSchema *ts,const uint8_t *value,size_t len,
                               uint8_t hdr2,size_t index,size_t *text_len){
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||index>=(size_t)ts->nfields){errno=EINVAL;return NULL;}
    TypedRowView view;TypedRowSpan span;
    if(typed_row_open(ts,value,len,hdr2,&view)||typed_row_span(&view,index,&span))return NULL;
    if(span.is_null||ts->fields[index].removed){errno=0;return NULL;}
    RowText text;char *out=NULL;
    if(row_text_open(&view,index,&text)){row_text_free(&text);return NULL;}
    if(text.len==SIZE_MAX){errno=EOVERFLOW;goto done;}
    out=malloc(text.len+1);if(!out){errno=ENOMEM;goto done;}
    size_t used=0;
    if(row_text_write(&text,0,out,text.len+1,&used)){free(out);out=NULL;goto done;}
    out[used]=0;*text_len=used;
done:row_text_free(&text);return out;
}
char *typed_packed_decode(const TypedSchema *ts,const uint8_t *value,size_t len,
                           uint8_t hdr2,size_t budget){
    /* Budget is exact JSON output bytes including the terminating NUL.
       Fail before any output-sized allocation, including for empty objects. */
    if(budget<3){errno=ENOMEM;return NULL;}
    if(!packed_schema_ok(ts)){errno=EINVAL;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    size_t need=3;int first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        size_t name=0,part=0;RowText text;
        if(quoted_length(f->name,(size_t)f->name_len,&name))return NULL;
        int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_length(&text,1,&part);
        row_text_free(&text);if(rc)return NULL;
        size_t sep=first?1:2;
        if(name>SIZE_MAX-sep||part>SIZE_MAX-name-sep||need>SIZE_MAX-name-part-sep){
            errno=EOVERFLOW;return NULL;
        }
        need+=name+part+sep;
        if(need>budget){errno=ENOMEM;return NULL;}first=0;
    }
    char *out=malloc(need);if(!out){errno=ENOMEM;return NULL;}
    size_t pos=0;out[pos++]='{';first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        if(!first)out[pos++]=',';
        first=0;size_t used=0;
        if(quote_into(f->name,(size_t)f->name_len,out+pos,need-pos,&used))goto fail;
        pos+=used;out[pos++]=':';
        RowText text;int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_write(&text,1,out+pos,need-pos,&used);
        row_text_free(&text);if(rc)goto fail;
        pos+=used;
    }
    out[pos++]='}';out[pos]=0;
    if(pos+1!=need){errno=EILSEQ;goto fail;}return out;
fail:free(out);return NULL;
}
/* JSON cells retain the existing projection/rows wire policy: numeric fields
   are bare, bool and other display scalars are strings, NULL remains null. */
static int projection_text_open(const TypedRowView *view,int index,RowText *text){
    if(index<0){memset(text,0,sizeof(*text));dec_init(&text->decimal);
        text->data="null";text->len=4;return 0;}
    int rc=row_text_open(view,(size_t)index,text);if(rc)return rc;
    if(view->schema->fields[index].type==FT_BOOL&&!view->schema->fields[index].removed){
        TypedRowSpan span;if(typed_row_span(view,(size_t)index,&span))return -1;
        if(!span.is_null)text->quoted=1;
    }
    return 0;
}
char *typed_packed_projected_cell(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,size_t index,size_t budget){
    if(!packed_schema_ok(ts)||index>=(size_t)ts->nfields){errno=EINVAL;return NULL;}
    if(budget<2){errno=ENOMEM;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    RowText text;char *out=NULL;
    int rc=projection_text_open(&view,(int)index,&text);size_t need=0,used=0;
    if(!rc)rc=row_text_length(&text,1,&need);
    if(rc)goto done;
    if(need==SIZE_MAX){errno=EOVERFLOW;goto done;}
    if(need+1>budget){errno=ENOMEM;goto done;}
    out=malloc(need+1);if(!out){errno=ENOMEM;goto done;}
    rc=row_text_write(&text,1,out,need+1,&used);
    if(rc||used!=need){
        if(!rc)errno=EILSEQ;
        free(out);out=NULL;goto done;
    }
    out[used]=0;
done:row_text_free(&text);return out;
}
typedef struct {
    RowText value;
    char *joined;
    int missing;
} NamedText;
static void named_text_free(NamedText *text){
    free(text->joined);row_text_free(&text->value);memset(text,0,sizeof(*text));
}
static int named_component_index(const TypedSchema *ts,const char *name,size_t len){
    for(int i=0;i<ts->nfields;i++)
        if(!ts->fields[i].removed&&(size_t)ts->fields[i].name_len==len&&
           !memcmp(ts->fields[i].name,name,len))return i;
    return -1;
}
static int named_component_open(const TypedRowView *view,int index,RowText *text){
    memset(text,0,sizeof(*text));dec_init(&text->decimal);
    if(index<0)return 0;
    TypedRowSpan span;if(typed_row_span(view,(size_t)index,&span))return -1;
    if(span.is_null||view->schema->fields[index].removed)return 0;
    return row_text_open(view,(size_t)index,text);
}
/* Explicit bounded scan, semantically identical to strnlen(s,MAX_LINE): spelled
   out so inlined callers with smaller buffer objects (for example the
   SearchCriterion.field[256] at the match_typed_checked named-text site) are
   not flagged by -Wstringop-overread for the caller-independent bound. */
static size_t bounded_line_length(const char *s){
    size_t n=0;while(n<MAX_LINE&&s[n])n++;return n;
}
static int named_text_open(const TypedRowView *view,const char *name,size_t budget,
                           NamedText *text){
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    size_t length=bounded_line_length(name);if(length==MAX_LINE){errno=E2BIG;return -1;}
    if(!strchr(name,'+')){
        int index=typed_field_index(view->schema,name);TypedRowSpan span;
        if(index<0){text->missing=1;return projection_text_open(view,-1,&text->value);}
        if(typed_row_span(view,(size_t)index,&span))return -1;
        text->missing=span.is_null||view->schema->fields[index].removed;
        return projection_text_open(view,index,&text->value);
    }
    int indices[MAX_FIELDS];size_t count=0;const char *p=name;
    while(*p){
        while(*p=='+')p++;
        if(!*p)break;
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        if(count==MAX_FIELDS){errno=E2BIG;return -1;}
        indices[count++]=named_component_index(view->schema,p,(size_t)(end-p));p=end;
    }
    size_t total=0;
    for(size_t i=0;i<count;i++){
        RowText part;int rc=named_component_open(view,indices[i],&part);
        size_t n=part.len;row_text_free(&part);if(rc)return -1;
        if(n>SIZE_MAX-total){errno=EOVERFLOW;return -1;}total+=n;
    }
    if(!total){text->missing=1;return projection_text_open(view,-1,&text->value);}
    if(total==SIZE_MAX){errno=EOVERFLOW;return -1;}
    if(total+1>budget){errno=ENOMEM;return -1;}
    text->joined=malloc(total+1);if(!text->joined){errno=ENOMEM;return -1;}
    size_t pos=0;
    for(size_t i=0;i<count;i++){
        RowText part;int rc=named_component_open(view,indices[i],&part);size_t used=0;
        if(!rc)rc=row_text_write(&part,0,text->joined+pos,total+1-pos,&used);
        row_text_free(&part);if(rc)return -1;pos+=used;
    }
    if(pos!=total){errno=EILSEQ;return -1;}text->joined[pos]=0;
    text->value.data=text->joined;text->value.len=total;text->value.quoted=1;return 0;
}
char *typed_packed_named_text(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget,size_t *text_len){
    if(text_len)*text_len=0;
    if(!text_len||!name||!packed_schema_ok(ts)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    NamedText text;char *out=NULL;
    if(named_text_open(&view,name,budget,&text))goto done;
    if(text.missing){errno=0;goto done;}
    if(text.value.len==SIZE_MAX||text.value.len+1>budget){errno=ENOMEM;goto done;}
    if(text.joined){out=text.joined;text.joined=NULL;*text_len=text.value.len;goto done;}
    out=malloc(text.value.len+1);if(!out){errno=ENOMEM;goto done;}
    size_t used=0;
    int rc=row_text_write(&text.value,0,out,text.value.len+1,&used);
    if(rc||used!=text.value.len){
        if(!rc)errno=EILSEQ;
        free(out);out=NULL;goto done;
    }
    out[used]=0;*text_len=used;
done:named_text_free(&text);return out;
}
char *typed_packed_named_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget){
    if(!name||!packed_schema_ok(ts)){errno=EINVAL;return NULL;}
    if(budget<2){errno=ENOMEM;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    NamedText text;char *out=NULL;
    if(named_text_open(&view,name,budget,&text))goto done;
    size_t need=0,used=0;if(row_text_length(&text.value,1,&need))goto done;
    if(need==SIZE_MAX||need+1>budget){errno=ENOMEM;goto done;}
    out=malloc(need+1);if(!out){errno=ENOMEM;goto done;}
    int rc=row_text_write(&text.value,1,out,need+1,&used);
    if(rc||used!=need){if(!rc)errno=EILSEQ;free(out);out=NULL;goto done;}out[used]=0;
done:named_text_free(&text);return out;
}
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const char *fields_csv,size_t budget){
    if(!packed_schema_ok(ts)||!fields_csv){errno=EINVAL;return NULL;}
    if(budget<3){errno=ENOMEM;return NULL;}
    size_t length=bounded_line_length(fields_csv);
    if(length==MAX_LINE){errno=E2BIG;return NULL;}
    char *copy=malloc(length+1);if(!copy){errno=ENOMEM;return NULL;}
    memcpy(copy,fields_csv,length+1);
    struct {char *name;} selected[MAX_FIELDS];size_t count=0;
    char *save=NULL,*token=strtok_r(copy,",",&save);char *out=NULL;
    while(token){
        if(count==MAX_FIELDS){errno=E2BIG;goto done;}
        selected[count].name=token;
        count++;token=strtok_r(NULL,",",&save);
    }
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))goto done;
    size_t need=3; /* braces and NUL; requested order/duplicates are preserved */
    for(size_t i=0;i<count;i++){
        size_t name=0,part=0,sep=i?2:1;NamedText text;
        if(quoted_length(selected[i].name,strlen(selected[i].name),&name))goto done;
        int rc=named_text_open(&view,selected[i].name,budget,&text);
        if(!rc)rc=row_text_length(&text.value,1,&part);
        named_text_free(&text);if(rc)goto done;
        if(name>SIZE_MAX-sep||part>SIZE_MAX-name-sep||need>SIZE_MAX-name-part-sep){errno=EOVERFLOW;goto done;}
        need+=name+part+sep;if(need>budget){errno=ENOMEM;goto done;}
    }
    out=malloc(need);if(!out){errno=ENOMEM;goto done;}
    size_t pos=0;out[pos++]='{';
    for(size_t i=0;i<count;i++){
        size_t used=0;if(i)out[pos++]=',';
        if(quote_into(selected[i].name,strlen(selected[i].name),out+pos,need-pos,&used))goto fail;
        pos+=used;out[pos++]=':';NamedText text;
        int rc=named_text_open(&view,selected[i].name,budget,&text);
        if(!rc)rc=row_text_write(&text.value,1,out+pos,need-pos,&used);
        named_text_free(&text);if(rc)goto fail;pos+=used;
    }
    out[pos++]='}';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    free(copy);return out;
fail:free(out);out=NULL;
done:free(copy);return out;
}

typedef struct {
    char *data;
    size_t used, cap;
} CsvSink;
typedef struct {
    char *copy;
    const char *names[MAX_FIELDS];
    int indices[MAX_FIELDS]; /* default fields retain identity; explicit names use -1 */
    size_t count;
} CsvSelection;
static int csv_delim_ok(char delimiter) {
    unsigned char d=(unsigned char)delimiter;
    return d=='\t'||(d>=0x20 && d<=0x7e && d!='"' && d!='\\');
}
static int csv_put(CsvSink *sink,const char *data,size_t len) {
    if(len>SIZE_MAX-sink->used){errno=EOVERFLOW;return -1;}
    if(len>sink->cap-sink->used){errno=ENOMEM;return -1;}
    if(sink->data&&len)memcpy(sink->data+sink->used,data,len);
    sink->used+=len;return 0;
}
/* Literal backslash doubles; C0 controls and DEL become six-byte \\u00xx.
   Other bytes (including UTF-8) pass through. CSV quoting is the outer layer. */
static size_t csv_byte(unsigned char c,char out[6]) {
    static const char hex[]="0123456789abcdef";
    if(c=='\\'){out[0]='\\';out[1]='\\';return 2;}
    if(c<0x20||c==0x7f){
        memcpy(out,"\\u00",4);out[4]=hex[c>>4];out[5]=hex[c&15];return 6;
    }
    out[0]=(char)c;return 1;
}
static int csv_span(CsvSink *sink,const void *bytes,size_t len,char delimiter) {
    const unsigned char *data=bytes;int quoted=0;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++)if(token[j]==delimiter||token[j]=='"')quoted=1;
    }
    if(quoted&&csv_put(sink,"\"",1))return -1;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++){
            if(token[j]=='"'&&csv_put(sink,"\"",1))return -1;
            if(csv_put(sink,token+j,1))return -1;
        }
    }
    return quoted?csv_put(sink,"\"",1):0;
}
static int csv_components_ok(const char *name) {
    size_t count=0;const char *p=name;
    while(*p){
        while(*p=='+')p++;
        if(!*p)break;
        if(++count>MAX_FIELDS){errno=E2BIG;return 0;}
        while(*p&&*p!='+')p++;
    }
    return 1;
}
static int csv_selection_open(const TypedSchema *ts,const char *fields,
                              CsvSelection *selection) {
    memset(selection,0,sizeof(*selection));
    if(!fields||!*fields){
        for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed){
            selection->names[selection->count]=ts->fields[i].name;
            selection->indices[selection->count++]=i;
        }
        return 0;
    }
    size_t n=bounded_line_length(fields);
    if(n==MAX_LINE){errno=E2BIG;return -1;}
    selection->copy=malloc(n+1);
    if(!selection->copy){errno=ENOMEM;return -1;}
    memcpy(selection->copy,fields,n+1);char *save=NULL;
    for(char *name=strtok_r(selection->copy,",",&save);name;
        name=strtok_r(NULL,",",&save)){
        if(selection->count==MAX_FIELDS){errno=E2BIG;return -1;}
        if(!csv_components_ok(name))return -1;
        selection->names[selection->count]=name;
        selection->indices[selection->count++]=-1;
    }
    return 0;
}
static int csv_selected_text_open(const TypedRowView *view,const char *name,
                                   int index,size_t budget,NamedText *text) {
    if(index<0)return named_text_open(view,name,budget,text);
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    TypedRowSpan span;if(typed_row_span(view,(size_t)index,&span))return -1;
    text->missing=span.is_null||view->schema->fields[index].removed;
    return row_text_open(view,(size_t)index,&text->value);
}
static int csv_selected_span(CsvSink *sink,const TypedRowView *view,
    const char *name,int index,char delimiter,size_t budget) {
    NamedText text;int rc=csv_selected_text_open(view,name,index,budget,&text);
    char *number=NULL;
    if(rc)goto done;
    if(text.missing){rc=csv_span(sink,NULL,0,delimiter);goto done;}
    const char *data=text.value.data;size_t n=text.value.len;
    if(n==SIZE_MAX||n+1>budget){errno=ENOMEM;rc=-1;goto done;}
    if(text.value.numeric){
        number=malloc(n+1);if(!number){errno=ENOMEM;rc=-1;goto done;}
        size_t used=0;rc=row_text_write(&text.value,0,number,n+1,&used);
        if(rc)goto done;
        if(used!=n){errno=EILSEQ;rc=-1;goto done;}data=number;
    }
    rc=csv_span(sink,data,n,delimiter);
done:free(number);named_text_free(&text);return rc;
}
char *typed_csv_cell_text(const void *data,size_t len,char delimiter,
                          size_t budget,size_t *text_len) {
    if(text_len)*text_len=0;
    if(!text_len||(!data&&len)||!csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    CsvSink measure={.cap=budget-1};
    if(csv_span(&measure,data,len,delimiter))return NULL;
    char *out=malloc(measure.used+1);if(!out){errno=ENOMEM;return NULL;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_span(&write,data,len,delimiter)){free(out);return NULL;}
    if(write.used!=measure.used){errno=EILSEQ;free(out);return NULL;}
    out[write.used]=0;*text_len=write.used;return out;
}
static int csv_record_pass(CsvSink *sink,const CsvSelection *selection,
                            const TypedRowView *view,const void *key,size_t key_len,
                            char delimiter,size_t budget,int header) {
    if(csv_span(sink,header?"key":key,header?3:key_len,delimiter))return -1;
    for(size_t i=0;i<selection->count;i++){
        if(csv_put(sink,&delimiter,1))return -1;
        if(header){
            const char *name=selection->names[i];
            if(csv_span(sink,name,strlen(name),delimiter))return -1;
        }else if(csv_selected_span(sink,view,selection->names[i],selection->indices[i],delimiter,budget))return -1;
    }
    return csv_put(sink,"\n",1);
}
static char *csv_record_text(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len,int header) {
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||(!key&&key_len)||
       !csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    if(header&&pkr_schema_validate(&ts->packed)!=DEC_OK){errno=EINVAL;return NULL;}
    TypedRowView view={0};
    if(!header&&typed_row_open(ts,value,len,hdr2,&view))return NULL;
    CsvSelection selection;char *out=NULL;
    if(csv_selection_open(ts,fields,&selection))goto done;
    CsvSink measure={.cap=budget-1};
    if(csv_record_pass(&measure,&selection,&view,key,key_len,delimiter,budget,header))goto done;
    out=malloc(measure.used+1);if(!out){errno=ENOMEM;goto done;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_record_pass(&write,&selection,&view,key,key_len,delimiter,budget,header))goto fail;
    if(write.used!=measure.used){errno=EILSEQ;goto fail;}
    out[write.used]=0;*text_len=write.used;goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
char *typed_packed_csv_header(const TypedSchema *ts,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,NULL,0,0,NULL,0,fields,delimiter,budget,text_len,1);
}
char *typed_packed_csv_row(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *wire_key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,value,len,hdr2,wire_key,key_len,fields,delimiter,budget,text_len,0);
}

/* Conservative peak heap allowance: dec_unpack digits + grown limb storage,
 * projection selector copy, and at most one composite join beside output.
 * Record/cache/input bytes and bounded automatic frames remain borrowed. */
size_t typed_packed_render_allowance(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t flags,const char *fields,size_t available){
    if(!packed_schema_ok(ts)||!value||flags!=PKR_FLAGS||
       len<(size_t)ts->packed.prefix+ts->packed.directory||
       len>ts->packed.maximum||value[0]!=ts->packed.prefix-1){errno=EILSEQ;return 0;}
    size_t largest=0;
    for(int i=0;i<ts->nfields;i++)if(ts->packed.fields[i].kind==PKR_NUMERIC){
        const PkrField *f=&ts->packed.fields[i];const uint8_t *entry=value+ts->packed.prefix+f->offset;
        uint32_t bytes=0;for(int j=0;j<4;j++)bytes=(bytes<<8)|entry[4+j];
        if(bytes>f->payload_max||bytes>len-ts->packed.prefix){errno=EILSEQ;return 0;}
        if(bytes>largest)largest=bytes;
    }
    size_t selector=fields?bounded_line_length(fields):0;
    if(selector==MAX_LINE){errno=E2BIG;return 0;}
    if(largest>(SIZE_MAX-selector-129)/8){errno=EOVERFLOW;return 0;}
    size_t workspace=largest?8*largest+128:0;
    if(fields)workspace+=selector+1;
    if(available<=workspace+1){errno=ENOBUFS;return 0;}
    /* Admission precedes pkr_view/decimal allocation; full view validation is
     * still mandatory, never replaced by the inexpensive length scan. */
    size_t allowance=(available-workspace)/2;
    if(!allowance)errno=ENOBUFS;
    return allowance;
}

static int row_selected_open(const TypedRowView *view,const CsvSelection *selection,
    size_t index,size_t budget,NamedText *text){
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    if(selection->indices[index]>=0)
        return projection_text_open(view,selection->indices[index],&text->value);
    return named_text_open(view,selection->names[index],budget,text);
}
char *typed_packed_row_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t flags,const void *key,size_t key_len,const char *fields,size_t budget){
    if(!packed_schema_ok(ts)||(!key&&key_len)){errno=EINVAL;return NULL;}
    if(budget<4){errno=ENOMEM;return NULL;}
    CsvSelection selection;
    if(csv_selection_open(ts,fields,&selection)){free(selection.copy);return NULL;}
    TypedRowView view;char *out=NULL;
    if(typed_row_open(ts,value,len,flags,&view))goto done;
    size_t key_bytes=0,need=3;
    if(quoted_length(key,key_len,&key_bytes))goto done;
    if(key_bytes>SIZE_MAX-need){errno=EOVERFLOW;goto done;}
    need+=key_bytes;if(need>budget){errno=ENOMEM;goto done;}
    for(size_t i=0;i<selection.count;i++){
        NamedText text;size_t part=0;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_length(&text.value,1,&part);
        named_text_free(&text);if(rc)goto done;
        if(part==SIZE_MAX||need>SIZE_MAX-part-1){errno=EOVERFLOW;goto done;}
        need+=part+1;if(need>budget){errno=ENOMEM;goto done;}
    }
    out=malloc(need);if(!out){errno=ENOMEM;goto done;}
    size_t pos=0,used=0;out[pos++]='[';
    if(quote_into(key,key_len,out+pos,need-pos,&used))goto fail;
    pos+=used;
    for(size_t i=0;i<selection.count;i++){
        out[pos++]=',';NamedText text;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_write(&text.value,1,out+pos,need-pos,&used);
        named_text_free(&text);if(rc)goto fail;pos+=used;
    }
    out[pos++]=']';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
