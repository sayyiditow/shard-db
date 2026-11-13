#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "bappend.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static TypedSchema *c_schema(int maximum){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"CSV schema");if(!ts)return NULL;
    ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;const char *types[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        types[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"numeric":i==3?"bool":i==4?"int":"varchar";
        if(!strncmp(types[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else if(!strcmp(types[i],"varchar"))ts->fields[i].type=FT_VARCHAR;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,types,(size_t)ts->nfields,NULL,0);ASSERT_EQ_INT(rc,0,"bind CSV schema");
    if(rc){free(ts);return NULL;}return ts;
}
static void c_exact(const void *data,size_t n,char delim,const char *want){
    size_t used=99;char *out=typed_csv_cell_text(data,n,delim,SIZE_MAX,&used);
    ASSERT_EQ_STR(out,want,"CSV cell exact");ASSERT_TRUE(out&&used==strlen(want)&&!memchr(out,0,used),"authoritative NUL-free span");free(out);
    out=typed_csv_cell_text(data,n,delim,strlen(want)+1,&used);
    ASSERT_EQ_STR(out,want,"exact CSV budget");free(out);
    used=99;errno=0;out=typed_csv_cell_text(data,n,delim,strlen(want),&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"short CSV budget rejects");free(out);
}
static void c_cells(void){
    c_exact(NULL,0,',',"");c_exact("123.4500",8,',',"123.4500");
    c_exact("a,b\"c",5,',',"\"a,b\"\"c\"");c_exact("a|b",3,'|',"\"a|b\"");
    c_exact("a\tb",3,'\t',"a\\u0009b");
    const unsigned char binary[]={0,'x','\\','u','0','0','0','0','\r','\n',0x7f,0x80,0xff};
    c_exact(binary,sizeof(binary),',',"\\u0000x\\\\u0000\\u000d\\u000a\\u007f\x80\xff");
    c_exact("x y",3,' ',"\"x y\"");c_exact("null",4,',',"null");
    char all[256],expected[1600];size_t pos=0;
    for(int i=0;i<256;i++){
        all[i]=(char)i;
        if(i<32||i==127)pos=tappendf(expected,sizeof(expected),pos,"\\u00%02x",i);
        else if(i=='\\'){expected[pos++]='\\';expected[pos++]='\\';}
        else if(i=='"'){expected[pos++]='"';expected[pos++]='"';}
        else expected[pos++]=(char)i;
    }
    expected[pos]=0;char quoted[1602];quoted[0]='"';memcpy(quoted+1,expected,pos);quoted[pos+1]='"';quoted[pos+2]=0;
    c_exact(all,sizeof(all),',',quoted);
    const char invalid[]={0,'\n','\r','"','\\',(char)0x80};
    for(size_t i=0;i<sizeof(invalid);i++){
        size_t n=99;errno=0;char *out=typed_csv_cell_text("a",1,invalid[i],SIZE_MAX,&n);
        ASSERT_TRUE(!out&&errno==EINVAL&&n==0,"invalid delimiter rejects");free(out);
    }
    size_t n=99;errno=0;char *out=typed_csv_cell_text(NULL,1,',',SIZE_MAX,&n);
    ASSERT_TRUE(!out&&errno==EINVAL&&n==0,"NULL bytes with positive length reject");free(out);
    errno=0;out=typed_csv_cell_text("a",1,',',SIZE_MAX,NULL);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL output length rejects");free(out);
}
static void c_output(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *fields,
                      const char *header,const char *row){
    size_t used=99;char *out=typed_packed_csv_header(ts,fields,',',SIZE_MAX,&used);
    ASSERT_EQ_STR(out,header,"header exact");ASSERT_TRUE(out&&used==strlen(header),"header length");free(out);
    out=typed_packed_csv_header(ts,fields,',',strlen(header)+1,&used);ASSERT_EQ_STR(out,header,"exact header budget");free(out);
    used=99;errno=0;out=typed_packed_csv_header(ts,fields,',',strlen(header),&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"short header budget");free(out);
    const char key[]={'k',0,'\\','"'};
    out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,key,sizeof(key),fields,',',SIZE_MAX,&used);
    ASSERT_EQ_STR(out,row,"packed CSV row exact");ASSERT_TRUE(out&&used==strlen(row)&&out[used-1]=='\n',"complete row and length");free(out);
    out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,key,sizeof(key),fields,',',strlen(row)+1,&used);
    ASSERT_EQ_STR(out,row,"exact row budget");free(out);
    used=99;errno=0;out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,key,sizeof(key),fields,',',strlen(row),&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"short row budget");free(out);
}
static void c_rows(void){
    TypedSchema *ts=c_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    int rc=typed_encode_alloc(ts,"{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"1.2300\",\"f2\":\"a\\u0000b\\\\u0000,\\\"\\n\",\"f3\":false,\"f4\":0,\"f5\":null}",1,NULL,NULL,&bytes,&n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode packed CSV fixture");if(rc){free(ts);return;}
    const char *key="\"k\\u0000\\\\\"\"\"";
    char row[512];
    snprintf(row,sizeof(row),"%s,1234567890123456.1234567890,1.2300,\"a\\u0000b\\\\u0000,\"\"\\u000a\",false,0,\n",key);
    c_output(ts,bytes,n,NULL,"key,f0,f1,f2,f3,f4,f5\n",row);
    snprintf(row,sizeof(row),"%s,1.2300,1234567890123456.1234567890,,,false0,1.2300\n",key);
    c_output(ts,bytes,n,"f1,f0,missing,f5,f3+f4,f1","key,f1,f0,missing,f5,f3+f4,f1\n",row);
    snprintf(row,sizeof(row),"%s,\n",key);
    c_output(ts,bytes,n,"+missing++f5+","key,+missing++f5+\n",row);
    snprintf(row,sizeof(row),"%s\n",key);
    c_output(ts,bytes,n,",,,","key\n",row);
    ts->fields[1].removed=1;
    snprintf(row,sizeof(row),"%s,,0\n",key);
    c_output(ts,bytes,n,"f1,f4","key,f1,f4\n",row);ts->fields[1].removed=0;
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"corrupt CSV fixture");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);
        size_t used=99;errno=0;char *out=typed_packed_csv_row(ts,bad,n,PKR_FLAGS,"k",1,"f4",',',SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==EILSEQ&&used==0,"unselected CSV descriptor corruption rejects");free(out);free(bad);}
    const uint8_t flags[]={0,1,2,0x80};
    for(size_t i=0;i<sizeof(flags);i++){size_t used=99;errno=0;
        char *out=typed_packed_csv_row(ts,bytes,n,flags[i],"k",1,",",',',SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==EILSEQ&&used==0,"generation mismatch before key-only row");free(out);}
    char many[(MAX_FIELDS+1)*3+1];size_t pos=0;
    for(int i=0;i<MAX_FIELDS+1;i++){memcpy(many+pos,"f4,",3);pos+=3;}many[pos-1]=0;
    size_t used=99;errno=0;char *out=typed_packed_csv_header(ts,many,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"too many CSV columns");free(out);
    for(size_t i=0;i<pos;i++)if(many[i]==',')many[i]='+';
    used=99;errno=0;out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,many,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"too many composite parts");free(out);
    used=99;errno=0;out=typed_packed_csv_header(ts,many,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"header also enforces composite limits");free(out);
    char *longname=malloc(MAX_LINE+1);ASSERT_NOT_NULL(longname,"long CSV selector");
    if(longname){memset(longname,'x',MAX_LINE);longname[MAX_LINE]=0;used=99;errno=0;
        out=typed_packed_csv_header(ts,longname,',',SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"selector byte bound");free(out);free(longname);}
    used=99;errno=0;out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,NULL,1,NULL,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL positive key span");free(out);
    used=99;errno=0;out=typed_packed_csv_row(ts,NULL,0,PKR_FLAGS,"k",1,NULL,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL packed row");free(out);
    ts->fields[3].type=FT_LONG;used=99;errno=0;
    out=typed_packed_csv_header(ts,NULL,',',SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"header validates schema metadata");free(out);
    ts->fields[3].type=FT_BOOL;
    free(bytes);free(ts);
}
static void c_limits(void){
    TypedSchema *ts=c_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0,used=0;
    int rc=typed_encode_alloc(ts,"{\"f127\":\"tiny\"}",1,NULL,NULL,&bytes,&n,NULL,0);
    ASSERT_EQ_INT(rc,0,"maximum-schema small record");
    if(!rc){char *out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,"f0,f127",',',10,&used);
        ASSERT_EQ_STR(out,"k,,tiny\n","small selected CSV at maximum schema ceiling");
        ASSERT_TRUE(n<3000&&used==8,"actual size independent of schema bound");free(out);free(bytes);}
    free(ts);ts=c_schema(0);if(!ts)return;
    size_t len=131072+1+16383;char *digits=malloc(len+1);ASSERT_NOT_NULL(digits,"maximum CSV numeric");
    if(digits){memset(digits,'1',len);digits[131072]='.';digits[len]=0;
        TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare CSV numeric");
        TypedInput input={digits,len,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"set CSV numeric");
        rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate CSV numeric");
        if(!rc){char *out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,"f1",',',len+4,&used);
            ASSERT_TRUE(out&&used==len+3&&!memcmp(out,"k,",2)&&!memcmp(out+2,digits,len)&&out[len+2]=='\n',"every maximum numeric digit in CSV");free(out);free(bytes);}
        typed_prepared_free(&p);free(digits);}
    len=4u*1024*1024;char *content=calloc(len,1);ASSERT_NOT_NULL(content,"maximum NUL CSV varchar");
    if(content){TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare CSV varchar");
        TypedInput input={content,len,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"set CSV varchar");
        rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate CSV varchar");
        if(!rc){char *out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,"f2",',',6*len+4,&used);int ok=out&&used==6*len+3;
            if(ok){ok=!memcmp(out,"k,",2)&&out[used-1]=='\n'&&!out[used];
                for(size_t i=0;ok&&i<len;i++)if(memcmp(out+2+6*i,"\\u0000",6))ok=0;}
            ASSERT_TRUE(ok,"all maximum binary bytes escaped without NUL or truncation");free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    free(ts);
}

static void c_schema_failures(void){
    TypedSchema *ts=c_schema(0);if(!ts)return;
    ASSERT_EQ_INT(pkr_schema_validate(&ts->packed),DEC_OK,"valid codec schema");
    ASSERT_EQ_INT(pkr_schema_validate(NULL),DEC_ERR_INVALID,"NULL codec schema");
    PkrSchema original=ts->packed;int offset=ts->fields[1].offset;
    int prefix=ts->nullmap_prefix,total=ts->total_size;
    for(int which=0;which<7;which++){
        ts->packed=original;ts->fields[1].offset=offset;
        ts->nullmap_prefix=prefix;ts->total_size=total;
        switch(which){
        case 0:ts->packed.fields[2].kind=(PkrKind)99;break;
        case 1:ts->packed.directory++;break;
        case 2:ts->packed.fields[0].precision=0;break;
        case 3:ts->packed.fields[0].scale=1001;break;
        case 4:ts->packed.fields[2].payload_max++;ts->packed.maximum++;ts->total_size++;break;
        case 5:ts->packed.fields[1].offset++;ts->fields[1].offset++;break;
        default:ts->packed.prefix++;ts->nullmap_prefix++;break;
        }
        ASSERT_EQ_INT(pkr_schema_validate(&ts->packed),DEC_ERR_INVALID,"codec rejects malformed metadata");
        size_t used=99;errno=0;char *out=typed_packed_csv_header(ts,"f4",',',SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"header rejects malformed unselected metadata");free(out);
    }
    free(ts);
}
static void c_default_identity(void){
    TypedSchema *ts=c_schema(0);if(!ts)return;
    strcpy(ts->fields[2].name,"f3+f4");ts->fields[2].name_len=5;
    const char *types[]={"numeric:26,10","numeric","varchar","bool","int","varchar"};
    int rc=typed_schema_bind_packed(ts,types,6,NULL,0);
    ASSERT_EQ_INT(rc,0,"binder accepts operator-like stored field name");
    uint8_t *bytes=NULL;size_t n=0;
    if(!rc)rc=typed_encode_alloc(ts,"{\"f3+f4\":\"hello\",\"f3\":false,\"f4\":0}",1,NULL,NULL,&bytes,&n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode stored field identity");
    if(!rc){size_t used=0;char *out=typed_packed_csv_header(ts,NULL,',',SIZE_MAX,&used);
        ASSERT_EQ_STR(out,"key,f0,f1,f3+f4,f3,f4,f5\n","default header keeps actual name");free(out);
        out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,NULL,',',SIZE_MAX,&used);
        ASSERT_EQ_STR(out,"k,,,hello,false,0,\n","default row uses actual field index");free(out);
        out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,"",',',SIZE_MAX,&used);
        ASSERT_EQ_STR(out,"k,,,hello,false,0,\n","empty selector also uses actual field index");free(out);
        out=typed_packed_csv_row(ts,bytes,n,PKR_FLAGS,"k",1,"f3+f4",',',SIZE_MAX,&used);
        ASSERT_EQ_STR(out,"k,false0\n","explicit composite retains expression meaning");free(out);
    }
    free(bytes);free(ts);
}
static int test_packed_csv_run(void){c_cells();c_rows();c_limits();c_schema_failures();c_default_identity();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-csv",test_packed_csv_run)
