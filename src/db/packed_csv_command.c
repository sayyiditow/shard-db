#include "packed_csv_command.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct CsvLine {struct CsvLine *next;char *text;size_t len;} CsvLine;
typedef struct {
    const FieldSchema *fs;
    const PackedCsvOptions *options;
    CsvLine *head,*tail;
    size_t bytes;
    int error,found;
    int64_t skip;
    size_t count,limit;
    const uint8_t *storage_key;
    size_t storage_len;
    const char *wire_key;
    size_t wire_len;
    uint64_t started;
} CsvCommand;
static void csv_lines_free(CsvCommand *c){
    while(c->head){CsvLine *line=c->head;c->head=line->next;free(line->text);free(line);}
    c->tail=NULL;c->bytes=0;
}
static int csv_error(FILE *out,int code){
    const char *message=code==ENOMEM?"{\"error\":\"CSV response allocation or buffer limit exceeded\"}\n":
        code==ENOBUFS?"{\"error\":\"query memory buffer exceeded; reduce CSV rows or projection\"}\n":
        code==EILSEQ?"{\"error\":\"invalid packed CSV record or schema\"}\n":
        code==EIO?"{\"error\":\"CSV storage read failed\"}\n":
        code==ETIMEDOUT?"{\"error\":\"query timed out\"}\n":
        code==E2BIG?"{\"error\":\"CSV selector or key exceeds limit\"}\n":
        code==ENOTSUP?"{\"error\":\"total with format=csv is not supported\"}\n":
        "{\"error\":\"invalid packed CSV request\"}\n";
    if(!out){errno=code;return -1;}
    size_t n=strlen(message);
    if(fwrite(message,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    errno=code;return -1;
}
static int csv_deadline(CsvCommand *c){
    if(c->options->timeout_ms&&now_ms_coarse()-c->started>=c->options->timeout_ms){
        c->error=ETIMEDOUT;return -1;
    }
    return 0;
}
static size_t csv_available(CsvCommand *c){
    size_t budget=c->options->buffer_budget;
    if(c->bytes>budget||budget-c->bytes<=sizeof(CsvLine)){
        c->error=ENOBUFS;return 0;
    }
    return budget-c->bytes-sizeof(CsvLine);
}
static int csv_add(CsvCommand *c,char *text,size_t n){
    if(!text){c->error=errno?errno:EIO;return -1;}
    size_t available=csv_available(c);
    if(n==SIZE_MAX||n+1>available){free(text);c->error=ENOBUFS;return -1;}
    CsvLine *line=malloc(sizeof(*line));
    if(!line){free(text);c->error=ENOMEM;return -1;}
    *line=(CsvLine){.text=text,.len=n};
    if(c->tail)c->tail->next=line;else c->head=line;
    c->tail=line;c->bytes+=sizeof(*line)+n+1;return 0;
}
static int csv_finish(CsvCommand *c,FILE *out){
    if(c->error){int code=c->error;csv_lines_free(c);return csv_error(out,code);}
    for(CsvLine *p=c->head;p;p=p->next){
        if(fwrite(p->text,1,p->len,out)!=p->len||ferror(out)){csv_lines_free(c);errno=EIO;return -1;}
    }
    csv_lines_free(c);if(fflush(out)||ferror(out)){errno=EIO;return -1;}return 0;
}
static int csv_start(CsvCommand *c,const FieldSchema *fs,
    const PackedCsvOptions *options,FILE *out){
    memset(c,0,sizeof(*c));
    if(!out||!fs||!fs->ts||!fs->ts->packed.nfields||!options){c->error=EINVAL;return -1;}
    c->fs=fs;c->options=options;c->started=now_ms_coarse();
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *text=typed_packed_csv_header(fs->ts,options->fields,options->delimiter,budget,&n);
    return csv_add(c,text,n);
}
static int csv_record(CsvCommand *c,const void *value,size_t vlen,uint8_t hdr2,
    const char *wire,size_t wire_len){
    if(csv_deadline(c))return -1;
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *text=typed_packed_csv_row(c->fs->ts,value,vlen,hdr2,wire,wire_len,
        c->options->fields,c->options->delimiter,budget,&n);
    return csv_add(c,text,n);
}
static int csv_fetch_record(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t hdr2,void *ptr){
    (void)hash;CsvCommand *c=ptr;
    if(csv_deadline(c))return 1;
    if(c->skip){c->skip--;return 0;}
    if(!klen||klen>MAX_KEY_CEILING){c->error=EILSEQ;return 1;}
    char wire[MAX_KEY_CEILING+1];
    const Schema *sc=c->fs->auto_key==AK_NONE?NULL:&c->fs->auto_key_schema_snapshot;
    int len=format_wire_key(sc,key,klen,wire,sizeof(wire));
    if(len<0){c->error=EILSEQ;return 1;}
    if(csv_record(c,value,vlen,hdr2,wire,(size_t)len))return 1;
    c->count++;return c->count>=c->limit;
}
static int csv_offset(const char *cursor,int offset,int64_t *out){
    uint64_t n=0;
    if(cursor)for(const unsigned char *p=(const unsigned char*)cursor;*p;p++){
        if(*p<'0'||*p>'9'||n>((uint64_t)INT64_MAX-(*p-'0'))/10){errno=EINVAL;return -1;}
        n=n*10+(*p-'0');
    }
    uint64_t add=offset>0?(uint64_t)offset:0;
    if(n>(uint64_t)INT64_MAX-add){errno=EINVAL;return -1;}
    *out=(int64_t)(n+add);return 0;
}
int packed_csv_fetch(SlotcaskDb *db,const FieldSchema *fs,int offset,int limit,
    const char *cursor,int want_total,const PackedCsvOptions *options,FILE *out){
    CsvCommand c;
    if(csv_start(&c,fs,options,out))return csv_finish(&c,out);
    if(!db){c.error=EIO;return csv_finish(&c,out);}
    if(limit<=0||csv_offset(cursor,offset,&c.skip)){c.error=EINVAL;return csv_finish(&c,out);}
    if(want_total){c.error=ENOTSUP;return csv_finish(&c,out);}
    c.limit=(size_t)limit;SlotcaskWriteReservation reservation={0};errno=0;
    if(slotcask_write_reservation_begin(db,&reservation)){c.error=errno==ENOMEM?ENOMEM:EIO;return csv_finish(&c,out);}
    int rc=slotcask_walk_live_strict(db,csv_fetch_record,&c);
    slotcask_write_reservation_end(&reservation);
    if(rc<0&&!c.error)c.error=EIO;
    if(rc==1&&!c.error&&c.count!=c.limit)c.error=EIO;
    if(!c.error)csv_deadline(&c);
    return csv_finish(&c,out);
}
static int csv_get_record(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t hdr2,void *ptr){
    (void)hash;CsvCommand *c=ptr;
    if(klen!=c->storage_len||memcmp(key,c->storage_key,klen))return 0;
    c->found=1;csv_record(c,value,vlen,hdr2,c->wire_key,c->wire_len);return 1;
}
static int csv_missing(CsvCommand *c){
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *key=typed_csv_cell_text(c->wire_key,c->wire_len,c->options->delimiter,budget,&n);
    if(!key){c->error=errno?errno:EIO;return -1;}
    size_t fields=0;
    for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed)fields++;
    if(n>SIZE_MAX-fields-2||n+fields+2>budget){free(key);c->error=ENOBUFS;return -1;}
    char *line=malloc(n+fields+2);if(!line){free(key);c->error=ENOMEM;return -1;}
    memcpy(line,key,n);free(key);
    memset(line+n,c->options->delimiter,fields);n+=fields;line[n++]='\n';line[n]=0;
    return csv_add(c,line,n);
}
int packed_csv_get_multi(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const char *keys_json,const PackedCsvOptions *options,FILE *out){
    CsvCommand c;
    if(csv_start(&c,fs,options,out))return csv_finish(&c,out);
    if(!db){c.error=EIO;return csv_finish(&c,out);}
    if(!schema||schema->max_key<1||schema->max_key>MAX_KEY_CEILING||
       !keys_json||options->fields||typed_json_validate(keys_json)){
        c.error=EINVAL;return csv_finish(&c,out);
    }
    const char *p=json_skip(keys_json);if(*p++!='['){c.error=EINVAL;return csv_finish(&c,out);}
    SlotcaskWriteReservation reservation={0};errno=0;
    if(slotcask_write_reservation_begin(db,&reservation)){c.error=errno==ENOMEM?ENOMEM:EIO;return csv_finish(&c,out);}
    size_t wire_max=schema->auto_key==AK_NONE?(size_t)schema->max_key:MAX_KEY_CEILING;
    p=json_skip(p);
    while(*p!=']'&&!c.error){
        if(csv_deadline(&c))break;
        if(*p!='"'){c.error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        if(*end!='"'||raw>6u*wire_max){c.error=E2BIG;break;}
        char *wire=NULL;size_t wire_len=0;errno=0;
        if(json_unescape_string(start,raw,&wire,&wire_len)){c.error=errno==ENOMEM?ENOMEM:EINVAL;break;}
        if(wire_len>wire_max){free(wire);c.error=E2BIG;break;}
        uint8_t storage[16];const uint8_t *key=(const uint8_t*)wire;size_t key_len=wire_len;
        if(schema->auto_key==AK_UUID){
            if(strlen(wire)!=wire_len||parse_uuid_string(wire,storage)){c.error=EINVAL;}
            key=storage;key_len=16;
        }else if(schema->auto_key==AK_SEQ){
            int64_t value;
            if(strlen(wire)!=wire_len||parse_seq_key(wire,&value))c.error=EINVAL;
            else for(int i=7;i>=0;i--){storage[i]=(uint8_t)((uint64_t)value&255);value=(int64_t)((uint64_t)value>>8);}
            key=storage;key_len=8;
        }else if(schema->auto_key!=AK_NONE)c.error=EINVAL;
        if(!key_len||key_len>(size_t)schema->max_key)c.error=E2BIG;
        if(!c.error){
            uint8_t hash[16];compute_hash_raw((const char*)key,key_len,hash);
            c.storage_key=key;c.storage_len=key_len;c.wire_key=wire;c.wire_len=wire_len;c.found=0;
            int rc=slotcask_guard_lookup(db,hash,csv_get_record,&c);
            if(rc<0&&!c.error)c.error=EIO;
            if(!c.error&&!c.found)csv_missing(&c);
        }
        free(wire);p=json_skip(end+1);
        if(*p==',')p=json_skip(p+1);
        else if(*p!=']')c.error=EINVAL;
    }
    slotcask_write_reservation_end(&reservation);
    if(!c.error)csv_deadline(&c);
    return csv_finish(&c,out);
}
