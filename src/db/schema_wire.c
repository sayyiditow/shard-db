#include "schema_wire.h"
#include "typed_row.h"
#include "packed_record.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
int schema_wire_quote(FILE *out,const char *s,size_t n){
    static const char hex[]="0123456789abcdef";
    if(fputc('"',out)==EOF)return -1;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='"'||c=='\\'){if(fputc('\\',out)==EOF||fputc(c,out)==EOF)return -1;}
        else if(c<32){char escape[6]={'\\','u','0','0',hex[c>>4],hex[c&15]};if(fwrite(escape,1,6,out)!=6)return -1;}
        else if(fputc(c,out)==EOF)return -1;
    }
    return fputc('"',out)==EOF?-1:0;
}
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget){
    if(!root||!object||budget<3){errno=EINVAL;return NULL;}

    if(ts&&(ts->nfields<0||ts->nfields>MAX_FIELDS)){errno=EILSEQ;return NULL;}
    char path[PATH_MAX];int pn=snprintf(path,sizeof(path),"%s/%s/fields.conf",root,object);
    if(pn<0||(size_t)pn>=sizeof(path)){errno=ENAMETOOLONG;return NULL;}
    FILE *source=fopen(path,"r");
    if(!source){
        if(errno==ENOENT&&(!ts||!ts->nfields)){
            char *empty=malloc(3);if(empty)memcpy(empty,"[]",3);return empty;
        }
        return NULL;
    }
    FILE *out=tmpfile();if(!out){int saved=errno;fclose(source);errno=saved;return NULL;}
    int error=0,index=0,printed=0;char *result=NULL;
    if(fputc('[',out)==EOF){error=EIO;goto done;}
    for(;;){
        int failure=0;char *line=conf_read_line(source,MAX_FIELD_DEF,&failure);
        if(!line){if(failure||ferror(source))error=EIO;break;}
        if(!*line||*line=='#'){free(line);continue;}
        const char *colon=strchr(line,':');
        if(!ts||index>=ts->nfields||!colon){free(line);error=EILSEQ;break;}
        const TypedField *f=&ts->fields[index];size_t name=(size_t)(colon-line);
        if(name!=(size_t)f->name_len||memcmp(f->name,line,name)){free(line);error=EILSEQ;break;}
        size_t length=strlen(line);int removed=length>=8&&!strcmp(line+length-8,":removed");
        if(removed!=!!f->removed){free(line);error=EILSEQ;break;}
        if(f->removed){index++;free(line);continue;}
        char *clean=strdup(colon+1);if(!clean){free(line);error=ENOMEM;break;}
        TypedField modifiers={0};
        if(parse_default_modifier_checked(clean,&modifiers)<0||modifiers.default_kind!=f->default_kind||
           modifiers.not_null!=f->not_null||strcmp(modifiers.default_val,f->default_val)){
            free(clean);free(line);error=EILSEQ;break;}
        PkrField declaration={0};int has_decl=pkr_field_parse(clean,&declaration)==DEC_OK;
        if(ts->packed.nfields&&(f->type==FT_NUMERIC||f->type==FT_VARCHAR)){
            const PkrField *bound=&ts->packed.fields[index];
            if(!has_decl||bound->kind!=declaration.kind||bound->payload_max!=declaration.payload_max||
               bound->bare!=declaration.bare||bound->currency!=declaration.currency||
               bound->precision!=declaration.precision||bound->scale!=declaration.scale){
                free(clean);free(line);error=EILSEQ;break;}
        }
        if(printed&&fputc(',',out)==EOF)error=EIO;
        if(!error&&(fputs("{\"name\":",out)==EOF||schema_wire_quote(out,f->name,name)||
           fputs(",\"type\":",out)==EOF||schema_wire_quote(out,typed_type_name(f->type),strlen(typed_type_name(f->type)))||
           fprintf(out,",\"size\":%d,\"spec\":",f->size)<0||schema_wire_quote(out,line,length)))error=EIO;
        if(!error&&f->type==FT_NUMERIC){
            /* Legacy P was informational; export spec preserves original text.
             * Its one-argument syntax used scale 2, not packed scale 0. */
            if(fprintf(out,",\"scale\":%d",f->numeric_scale)<0)error=EIO;
            if(has_decl){
                if(fprintf(out,",\"precision\":%d,\"bare\":%s,\"currency\":%s",
                    declaration.precision,declaration.bare?"true":"false",declaration.currency?"true":"false")<0)error=EIO;
            }
        }
        if(!error&&f->type==FT_VARCHAR){
            uint32_t capacity=ts->packed.nfields?ts->packed.fields[index].payload_max:(uint32_t)(f->size-2);
            if(fprintf(out,",\"capacity\":%u,\"bare\":%s",capacity,
                ts->packed.nfields&&declaration.bare?"true":"false")<0)error=EIO;
        }
        if(!error&&f->default_kind!=DK_NONE){
            char default_text[300];const char *value=f->default_val;
            if(f->default_kind==DK_AUTO_CREATE)value="auto_create";
            else if(f->default_kind==DK_AUTO_UPDATE)value="auto_update";
            else if(f->default_kind==DK_UUID)value="uuid()";
            else if(f->default_kind==DK_SEQ||f->default_kind==DK_RANDOM){
                int n=snprintf(default_text,sizeof(default_text),"%s(%s)",f->default_kind==DK_SEQ?"seq":"random",f->default_val);
                if(n<0||(size_t)n>=sizeof(default_text))error=EOVERFLOW;
                value=default_text;
            }
            if(!error&&(fputs(",\"default\":",out)==EOF||schema_wire_quote(out,value,strlen(value))))error=EIO;
        }
        if(!error&&f->not_null&&fputs(",\"not_null\":true",out)==EOF)error=EIO;
        if(!error&&fputc('}',out)==EOF)error=EIO;
        free(clean);free(line);index++;printed++;
        off_t size=ftello(out);if(size<0)error=EIO;else if((uintmax_t)size>=budget)error=ENOBUFS;
        if(error)break;
    }
    if(!error&&ts&&index!=ts->nfields)error=EILSEQ;
    if(!error&&(fputc(']',out)==EOF||fflush(out)))error=EIO;
    if(!error){
        off_t length=ftello(out);
        if(length<0||(uintmax_t)length>=budget||(uintmax_t)length>SIZE_MAX-1)error=ENOBUFS;
        else{
            result=malloc((size_t)length+1);
            if(!result)error=ENOMEM;
            else if(fseeko(out,0,SEEK_SET)||fread(result,1,(size_t)length,out)!=(size_t)length)error=EIO;
            else result[length]=0;
        }
    }
done:
    if(fclose(source)&&!error)error=EIO;
    if(fclose(out)&&!error)error=EIO;
    if(error){free(result);errno=error;return NULL;}return result;
}

int typed_legacy_decl_size(char *buf) {

    /* Strip :removed */
    size_t blen = strlen(buf);
    if (blen >= 8 && strcmp(buf + blen - 8, ":removed") == 0) {
        buf[blen - 8] = '\0';
    }
    /* Strip a trailing :not_null / :nullable so the type matcher below
       sees the bare type spec, then reject any further occurrence —
       varchar/numeric/enum tolerate trailing bytes, so a buried token
       would otherwise be accepted and silently unenforced. */
    size_t nn_len = strlen(buf);
    if (nn_len >= 9 && (strcmp(buf + nn_len - 9, ":not_null") == 0 ||
                        strcmp(buf + nn_len - 9, ":nullable") == 0)) {
        buf[nn_len - 9] = '\0';
    }
    if (strstr(buf, ":not_null") || strstr(buf, ":nullable")) return 0;
    /* Strip default modifiers: :auto_create, :auto_update, :default=... */
    char *dm;
    if ((dm = strstr(buf, ":auto_update")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":auto_create")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":default=")) != NULL) *dm = '\0';

    const char *type = buf;
    if (strncmp(type, "varchar:", 8) == 0) {
        const char *p=type+8;if(*p<'0'||*p>'9')return 0;
        unsigned sz=0;
        for(;*p;p++){
            if(*p<'0'||*p>'9'||sz>(65535u-(unsigned)(*p-'0'))/10)return 0;
            sz=sz*10+(unsigned)(*p-'0');
        }
        if (!sz || sz > 65535) return 0;   /* bounded: 1..65535 content bytes */
        return sz + 2;                          /* 2-byte length prefix */
    }
    if (strcmp(type, "varchar") == 0) return 0; /* bare "varchar" — require :N */
    if (strcmp(type, "long") == 0)   return 8;
    if (strcmp(type, "int") == 0)    return 4;
    if (strcmp(type, "short") == 0)  return 2;
    if (strcmp(type, "double") == 0) return 8;
    if (strcmp(type, "float") == 0)  return 4;
    if (strcmp(type, "bool") == 0)   return 1;
    if (strcmp(type, "byte") == 0)   return 1;
    if (strcmp(type, "date") == 0)   return 4;
    if (strcmp(type, "datetime") == 0) return 7;
    if (strcmp(type, "datetimems") == 0) return 8;
    if (strcmp(type, "time") == 0)    return 3;
    if (strcmp(type, "timestamp") == 0) return 8;
    if (strcmp(type, "uuid") == 0)    return 16;
    if (strcmp(type, "ipv4") == 0)    return 4;
    if (strcmp(type, "ipv6") == 0)    return 16;
    if (strcmp(type, "currency") == 0) return 8;
    if (strncmp(type, "numeric:", 8) == 0) {
        /* Full-input P[,S] validation. P stays informational until the
           packed exact format lands; S must fit the int64 scaled storage:
           0..18 (10^18 is the largest power of ten in int64; S = 0 is
           valid). Legacy "numeric:N" without a comma keeps its
           historical scale-2 meaning. Junk suffixes are rejected, not
           atoi-prefixed. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        while (*ps >= '0' && *ps <= '9') ps++;
        if (*ps == ',') {
            ps++;
            if (!(*ps >= '0' && *ps <= '9')) return 0;
            while (*ps >= '0' && *ps <= '9') ps++;
        }
        if (*ps != '\0') return 0;
        const char *comma = strchr(type + 8, ',');
        long s = comma ? strtol(comma + 1, NULL, 10) : 2;
        if (s < 0 || s > 18) return 0;
        return 8;
    }
    if (strncmp(type, "enum(", 5) == 0) {
        /* Validate enum(v1,v2,...) — empty list, duplicates, missing
           close paren, and over-ceiling all → invalid (return 0).
           Returns the storage byte width: 1 for ≤256 values, 2 for
           257..65535. create-object is cold path, simple alloc is fine. */
        const char *open  = type + 5;
        const char *close = strrchr(open, ')');
        if (!close || close <= open || close[1]) return 0;

        /* Pass 1: count + duplicate-check via a heap-allocated value list.
           A worst-case 65535-entry list of avg ~8B = ~640 KB peak — fine. */
        char **vals = calloc(65536, sizeof(char *));
        if (!vals) return 0;
        int n = 0;
        const char *p = open;
        while (p < close) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl == 0) goto enum_invalid;            /* empty value */
            if (n >= 65535)            goto enum_invalid;
            char *v = malloc(vl + 1);
            if (!v) goto enum_invalid;
            memcpy(v, p, vl); v[vl] = '\0';
            for (int i = 0; i < n; i++) {
                if (strcmp(vals[i], v) == 0) {  /* duplicate */
                    free(v);
                    goto enum_invalid;
                }
            }
            vals[n++] = v;
            p = (next < close) ? next + 1 : close;
        }
        if (n == 0) goto enum_invalid;
        int width = (n <= 256) ? 1 : 2;
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return width;

enum_invalid:
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return 0;
    }
    return 0;
}

int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count){
    if(!specs||!count){errno=EINVAL;return -1;}
    memset(specs,0,MAX_FIELDS*sizeof(*specs));*count=0;
    if(!json||typed_json_validate(json)){errno=EINVAL;return -1;}
    const char *p=json_skip(json);if(*p++!='['){errno=EINVAL;return -1;}p=json_skip(p);
    while(*p!=']'){
        if(*p!='"'||*count==MAX_FIELDS){errno=E2BIG;goto fail;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        if(*end!='"'||raw>6u*(MAX_FIELD_DEF-1u)){errno=E2BIG;goto fail;}
        char *text=NULL;size_t length=0;errno=0;
        if(json_unescape_cstring(start,raw,&text,&length))goto fail;
        if(!length||length>=MAX_FIELD_DEF||memchr(text,'\n',length)||memchr(text,'\r',length)){
            free(text);errno=EINVAL;goto fail;}
        const char *colon=strchr(text,':');size_t name=colon?(size_t)(colon-text):0;
        if(!name||name>255){free(text);errno=EINVAL;goto fail;}
        for(size_t i=0;i<*count;i++){
            const char *c=strchr(specs[i],':');
            if((size_t)(c-specs[i])==name&&!memcmp(specs[i],text,name)){free(text);errno=EEXIST;goto fail;}
        }
        specs[(*count)++]=text;p=json_skip(end+1);if(*p==',')p=json_skip(p+1);
    }
    if(!*count){errno=EINVAL;goto fail;}return 0;
fail:{int saved=errno?errno:EINVAL;for(size_t i=0;i<*count;i++){free(specs[i]);specs[i]=NULL;}*count=0;errno=saved;return -1;}
}
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines){
    if(!lines||!nlines){errno=EINVAL;return -1;}*lines=NULL;*nlines=0;
    char *specs[MAX_FIELDS];size_t count=0;if(typed_field_specs_parse(json,specs,&count))return -1;
    size_t copied=0;for(size_t i=0;i<count;i++)copied+=strlen(specs[i])+1;
    int error=0;
    if(count>SIZE_MAX/MAX_FIELD_DEF||count*MAX_FIELD_DEF>g_query_buffer_max_bytes||
       copied>g_query_buffer_max_bytes-count*MAX_FIELD_DEF){error=ENOMEM;}
    char (*rows)[MAX_FIELD_DEF]=error?NULL:calloc(count,MAX_FIELD_DEF);
    if(!rows&&!error)error=ENOMEM;
    if(!error)for(size_t i=0;i<count;i++)memcpy(rows[i],specs[i],strlen(specs[i])+1);
    for(size_t i=0;i<count;i++)free(specs[i]);
    if(error){free(rows);errno=error;return -1;}*lines=rows;*nlines=(int)count;return 0;
}

char *typed_schema_indexes_json(const char *root,const char *object,size_t budget){
    char path[PATH_MAX];int n=snprintf(path,sizeof(path),"%s/%s/indexes/index.conf",root,object);
    if(n<0||(size_t)n>=sizeof(path)){errno=ENAMETOOLONG;return NULL;}
    FILE *source=fopen(path,"r");
    if(!source){if(errno!=ENOENT)return NULL;char *empty=malloc(3);if(empty)memcpy(empty,"[]",3);return empty;}
    FILE *out=tmpfile();if(!out){int e=errno;fclose(source);errno=e;return NULL;}
    int error=fputc('[',out)==EOF?EIO:0,printed=0;char *text=NULL;
    while(!error){
        int failure=0;char *line=conf_read_line(source,MAX_FIELD_DEF,&failure);
        if(!line){if(failure||ferror(source))error=EIO;break;}
        if(!*line||*line=='#'){free(line);continue;}
        if((printed&&fputc(',',out)==EOF)||schema_wire_quote(out,line,strlen(line)))error=EIO;
        free(line);printed++;
        off_t used=ftello(out);if(used<0)error=EIO;else if((uintmax_t)used>=budget)error=ENOBUFS;
    }
    if(!error&&(fputc(']',out)==EOF||fflush(out)))error=EIO;
    off_t size=error?-1:ftello(out);
    if(!error&&(size<0||(uintmax_t)size>=budget||(uintmax_t)size>SIZE_MAX-1))error=ENOBUFS;
    if(!error){
        text=malloc((size_t)size+1);if(!text)error=ENOMEM;
        else if(fseeko(out,0,SEEK_SET)||fread(text,1,(size_t)size,out)!=(size_t)size)error=EIO;
        else text[size]=0;
    }
    if(fclose(source)&&!error)error=EIO;
    if(fclose(out)&&!error)error=EIO;
    if(error){free(text);errno=error;return NULL;}return text;
}

int typed_default_validate(const TypedField *f){
    if(!f||f->type==FT_NONE||f->size<=0||f->default_invalid){errno=EINVAL;return -1;}
    if(f->default_kind==DK_RANDOM){
        size_t count=(size_t)strtoul(f->default_val,NULL,10);
        if(!count||count>2097152u||f->type!=FT_VARCHAR||count>(size_t)(f->size-2)/2){errno=EINVAL;return -1;}
        return 0;
    }
    if(f->default_kind!=DK_LITERAL)return 0;
    if(f->type==FT_ENUM){
        for(int i=0;i<f->n_enum_values;i++)if(!strcmp(f->default_val,f->enum_values[i]))return 0;
        errno=EINVAL;return -1;
    }
    if((size_t)f->size>g_query_buffer_max_bytes){errno=ENOBUFS;return -1;}
    uint8_t *bytes=malloc((size_t)f->size);if(!bytes)return -1;
    int rc=encode_field_checked(f,f->default_val,strlen(f->default_val),bytes,NULL,0);
    free(bytes);if(rc)errno=EINVAL;return rc;
}
