#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

static TypedSchema *schema_new(const char *const *types,size_t n){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return NULL;
    ts->typed=1;ts->nfields=(int)n;ts->nullmap_prefix=1+typed_nullmap_bytes((int)n);
    for(size_t i=0;i<n;i++){
        if(!strncmp(types[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else if(!strncmp(types[i],"numeric",7)||!strcmp(types[i],"currency"))
            ts->fields[i].type=FT_NUMERIC;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%zu",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    if(typed_schema_bind_packed(ts,types,n,NULL,0)){
        for(size_t i=0;i<n;i++)free_enum_values(&ts->fields[i]);
        free(ts);return NULL;
    }
    return ts;
}
static void schema_free(TypedSchema *ts){
    if(!ts)return;
    for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);
    free(ts);
}
static void encoding(void){
    const char *types[]={"numeric:26,10","varchar","int","currency","numeric:2,-2"};
    TypedSchema *ts=schema_new(types,5);ASSERT_NOT_NULL(ts,"explicit packed schema");
    if(!ts)return;
    uint8_t *out=NULL;size_t n=0;char err[256];
    const char *json="{\"f4\":9949,\"f1\":\"abc\",\"f0\":\"1234567890123456.1234567890\",\"f2\":7,\"f3\":\"12.34565\"}";
    ASSERT_EQ_INT(typed_encode_alloc(ts,json,1,NULL,NULL,&out,&n,err,sizeof(err)),0,"reverse JSON schema-order encoding");
    ASSERT_TRUE(n<(size_t)ts->total_size,"actual allocation below maximum");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024*1024);
        ASSERT_NOT_NULL(text,"exact packed render");
        if(text){
            ASSERT_CONTAINS(text,"1234567890123456.1234567890","full decimal value");
            ASSERT_CONTAINS(text,"\"f1\":\"abc\"","raw varchar rendered");
            ASSERT_CONTAINS(text,"\"f2\":7","fixed int directory");
            ASSERT_CONTAINS(text,"\"f3\":12.3457","currency precision");
            ASSERT_CONTAINS(text,"\"f4\":9900","negative scale");
            free(text);
        }
        TypedRowView view;ASSERT_EQ_INT(typed_row_open(ts,out,n,1,&view),-1,"packed schema rejects legacy generation");
        ASSERT_EQ_INT(typed_row_open(ts,out,n,PKR_FLAGS,&view),0,"packed schema view");
        TypedRowSpan span;ASSERT_EQ_INT(typed_row_span(&view,1,&span),0,"varchar span");
        ASSERT_EQ_INT(span.len,3,"content span no uint16 prefix");
        if(span.data)ASSERT_EQ_INT(memcmp(span.data,"abc",3),0,"content exact");
        ASSERT_EQ_INT(typed_schema_hdr2(ts),PKR_FLAGS,"packed output generation");
        errno=0;text=typed_packed_decode(ts,out,n,PKR_FLAGS,8);
        ASSERT_TRUE(!text&&errno==ENOMEM,"output budget error");
        free(out);
    }
    out=(void*)1;n=999;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"empty numeric rejects");
    ASSERT_TRUE(!out&&!n,"failed owned output reset");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":0,\"f1\":\"\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"zero empty null distinct");
    if(out){
        TypedRowView v;TypedRowSpan a,b,c;ASSERT_EQ_INT(typed_row_open(ts,out,n,PKR_FLAGS,&v),0,"zero empty view");
        ASSERT_EQ_INT(typed_row_span(&v,0,&a),0,"zero span");ASSERT_TRUE(!a.is_null&&a.len>=16,"zero actual decimal payload");
        ASSERT_EQ_INT(typed_row_span(&v,1,&b),0,"empty span");ASSERT_TRUE(!b.is_null&&b.len==0&&b.data,"present empty");
        ASSERT_EQ_INT(typed_row_span(&v,2,&c),0,"omitted span");ASSERT_TRUE(c.is_null,"defaults insertion omission null");
        free(out);
    }
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f2\":1,\"f2\":2}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"last valid duplicate wins");
    if(out){
        size_t len=0;char *s=typed_packed_field_text(ts,out,n,PKR_FLAGS,2,&len);
        ASSERT_NOT_NULL(s,"duplicate display");if(s){ASSERT_EQ_STR(s,"2","duplicate result");free(s);}
        free(out);
    }
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"1.25");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"literal default");
    if(out){
        size_t len=0;char *s=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
        ASSERT_NOT_NULL(s,"default text");if(s){ASSERT_EQ_STR(s,"1.2500000000","default scale exact");free(s);}free(out);
    }
    ts->fields[1].not_null=1;out=(void*)1;n=99;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"omission not null");
    ASSERT_TRUE(!out&&!n,"not-null failure outputs reset");
    ts->fields[1].not_null=0;
    schema_free(ts);
}
static void updates_and_reuse(void){
    const char *types[]={"varchar","numeric:3,1","int"};
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"update schema");if(!ts)return;
    uint8_t *old=NULL;size_t oldn=0;char err[256];
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"old\",\"f1\":\"1.25\",\"f2\":7}",1,NULL,NULL,&old,&oldn,err,sizeof(err)),0,"update seed");
    if(!old){schema_free(ts);return;}
    SlotcaskOldRecord record={old,oldn,PKR_FLAGS};
    TypedInput inputs[MAX_FIELDS]={0};inputs[0]=(TypedInput){"much longer",11,1,0,0};
    uint8_t *next=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&record,inputs,0,&next,&n,err,sizeof(err)),0,"growing update actual allocation");
    if(next){
        char *text=typed_packed_decode(ts,next,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"updated text");if(text){
            ASSERT_CONTAINS(text,"much longer","new varchar");
            ASSERT_CONTAINS(text,"\"f1\":1.3","omitted exact decimal carried");
            ASSERT_CONTAINS(text,"\"f2\":7","omitted fixed carried");free(text);}
        free(next);
    }
    inputs[1]=(TypedInput){"99.95",5,1,0,0};next=(void*)1;n=99;
    ASSERT_EQ_INT(typed_update_alloc(ts,&record,inputs,0,&next,&n,err,sizeof(err)),-1,"precision overflow rejects update");
    ASSERT_TRUE(!next&&!n,"failed update owned output clear");
    char *before=typed_packed_decode(ts,old,oldn,PKR_FLAGS,4096);
    ASSERT_NOT_NULL(before,"old still decodes");if(before){
        ASSERT_CONTAINS(before,"\"f0\":\"old\"","failed update did not change OLD");free(before);}
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_old(ts,&record,&p,err,sizeof(err)),0,"borrow OLD prepared");
    size_t cap=p.bytes;uint8_t *buf=malloc(cap+8);ASSERT_NOT_NULL(buf,"actual capacity canary");
    if(buf){memset(buf,0xA5,cap+8);size_t used=99;
        ASSERT_EQ_INT(typed_prepared_compose(&p,buf,cap-1,&used,err,sizeof(err)),-1,"short destination");
        ASSERT_EQ_INT(used,0,"short used reset");int same=1;
        for(size_t i=0;i<cap+8;i++)if(buf[i]!=0xA5)same=0;
        ASSERT_TRUE(same,"failed compose untouched");
        ASSERT_EQ_INT(typed_prepared_compose(&p,buf,cap,&used,err,sizeof(err)),0,"actual capacity accepts");
        same=1;for(size_t i=cap;i<cap+8;i++)if(buf[i]!=0xA5)same=0;
        ASSERT_TRUE(same,"tail canary untouched");free(buf);}
    typed_prepared_free(&p);free(old);schema_free(ts);
}
static void legacy_views(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=2;ts->nullmap_prefix=2;ts->total_size=10;
    parse_field_type("int",&ts->fields[0]);ts->fields[0].offset=0;
    parse_field_type("date",&ts->fields[1]);ts->fields[1].offset=4;
    uint8_t trimmed[]={1};TypedRowView view;
    ASSERT_EQ_INT(typed_row_open(ts,trimmed,sizeof(trimmed),0,&view),0,"legacy trimmed generation");
    TypedRowSpan span;ASSERT_EQ_INT(typed_row_span(&view,0,&span),0,"legacy partial span");
    ASSERT_EQ_INT(span.len,1,"actual stored partial bytes");
    const uint8_t *read=NULL;ASSERT_EQ_INT(typed_span_read(ts,0,&span,&read),0,"legacy reader zero substitute");
    if(read)ASSERT_EQ_INT(read[0],0,"reader requires whole field");
    uint8_t carried[4]={0xA5,0xA5,0xA5,0xA5};ASSERT_EQ_INT(typed_span_copy_legacy(ts,0,&span,carried,sizeof(carried)),0,"legacy carry materialization");
    ASSERT_EQ_INT(carried[0],1,"carry preserves partial leading byte");
    ASSERT_EQ_INT(carried[1],0,"carry zero fills suffix");
    ASSERT_EQ_INT(typed_row_open(ts,trimmed,sizeof(trimmed),PKR_FLAGS,&view),-1,"legacy schema rejects packed generation");
    free(ts);
}
static void maximum_and_binary(void){
    const char *types[MAX_FIELDS];for(int i=0;i<MAX_FIELDS;i++)types[i]="varchar";
    TypedSchema *ts=schema_new(types,MAX_FIELDS);ASSERT_NOT_NULL(ts,"maximum packed schema");if(!ts)return;
    ASSERT_EQ_INT(ts->total_size,1073743905,"schema maximum remains bound");
    TypedPrepared p;typed_prepared_init(&p);char err[256];
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"maximum schema prepare");
    uint8_t binary[]={'a',0,'b'};TypedInput in={(const char*)binary,sizeof(binary),1,0,0};
    ASSERT_EQ_INT(typed_prepared_set(&p,127,&in,err,sizeof(err)),0,"raw CSV binary span");
    uint8_t *out=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"maximum schema actual allocation");
    ASSERT_EQ_INT(n,2084,"actual descriptor directory plus three bytes");
    if(out){char *s=typed_packed_decode(ts,out,n,PKR_FLAGS,16384);
        ASSERT_NOT_NULL(s,"binary JSON full byte span");if(s){ASSERT_CONTAINS(s,"a\\u0000b","NUL not silently dropped");free(s);}free(out);}
    typed_prepared_free(&p);schema_free(ts);
}

static void review_regressions(void){
    char err[256];const char *types[]={"varchar","ipv4","uuid"};
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"review schema");if(!ts)return;
    uint8_t *out=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"a\\u0000b\",\"f1\":\"\\u0031.2.3.4\",\"f2\":\"\\u00300000000-0000-0000-0000-000000000001\"}",1,NULL,NULL,&out,&n,err,sizeof(err)),0,"decode string before sentinel validation");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"escaped binary render");if(text){
            ASSERT_CONTAINS(text,"a\\u0000b","binary JSON round trip");
            ASSERT_CONTAINS(text,"1.2.3.4","escaped IP stays non-null");
            ASSERT_CONTAINS(text,"00000000-0000-0000-0000-000000000001","escaped UUID stays non-null");
            size_t exact=strlen(text)+1;
            char *fit=typed_packed_decode(ts,out,n,PKR_FLAGS,exact);
            ASSERT_NOT_NULL(fit,"exact rendering budget");free(fit);
            errno=0;fit=typed_packed_decode(ts,out,n,PKR_FLAGS,exact-1);
            ASSERT_TRUE(!fit&&errno==ENOMEM,"one short output budget");free(fit);free(text);
        }
        size_t rawlen=0;char *raw=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&rawlen);
        ASSERT_NOT_NULL(raw,"binary projection owns span");
        if(raw){ASSERT_EQ_INT(rawlen,3,"binary projection length");
            ASSERT_TRUE(raw[0]=='a'&&raw[1]==0&&raw[2]=='b',"projection retains NUL");free(raw);}
        for(int i=0;i<3;i++)ts->fields[i].removed=1;
        errno=0;text=typed_packed_decode(ts,out,n,PKR_FLAGS,0);
        ASSERT_TRUE(!text&&errno==ENOMEM,"empty object respects budget");
        text=typed_packed_decode(ts,out,n,PKR_FLAGS,3);
        ASSERT_NOT_NULL(text,"empty object exact budget");if(text){ASSERT_EQ_STR(text,"{}","removed fields omitted");free(text);}
        free(out);for(int i=0;i<3;i++)ts->fields[i].removed=0;
    }
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"fallback");
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"prepare failed-set fixture");
    TypedInput bad={"\\q",2,1,0,1};
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&bad,err,sizeof(err)),-1,"invalid escape fails assignment");
    ASSERT_EQ_INT(p.seen[0],0,"failed setter preserves seen");
    ASSERT_EQ_INT(typed_prepared_defaults(&p,NULL,NULL,err,sizeof(err)),0,"defaults after rejected set");
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"compose after rejected set");
    if(out){size_t nn=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&nn);
        ASSERT_NOT_NULL(text,"default not suppressed");if(text){ASSERT_EQ_STR(text,"fallback","failed set leaves default eligible");free(text);}free(out);}
    typed_prepared_free(&p);
    strcpy(ts->fields[0].name,"x\"\\\n");ts->fields[0].name_len=4;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",0,NULL,NULL,&out,&n,err,sizeof(err)),0,"escaped field-name fixture");
    if(out){char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,4096);
        ASSERT_NOT_NULL(text,"escaped name renderer");if(text){ASSERT_CONTAINS(text,"\"x\\\"\\\\\\n\":","field name escaped");free(text);}free(out);}
    schema_free(ts);
}

static void schema_and_failure_contracts(void){
    const char *types[]={"numeric","varchar:3","int"};char err[256];
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"failure schema");if(!ts)return;
    TypedSchema *snapshot=malloc(sizeof(*snapshot));ASSERT_NOT_NULL(snapshot,"schema snapshot");
    if(snapshot){memcpy(snapshot,ts,sizeof(*snapshot));
        const char *bad[]={"numeric","varchar:4194305","int"};
        ASSERT_EQ_INT(typed_schema_bind_packed(ts,bad,3,err,sizeof(err)),-1,"bad declaration rejects");
        ASSERT_EQ_INT(memcmp(snapshot,ts,sizeof(*ts)),0,"binding failure preserves schema");free(snapshot);}
    uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f1\":\"abcd\"}",0,NULL,NULL,&row,&n,err,sizeof(err)),-1,"varchar cap no truncation");
    ASSERT_TRUE(!row&&!n,"cap error owned outputs clear");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":1,\"f0\":\"bad\"}",0,NULL,NULL,&row,&n,err,sizeof(err)),-1,"every duplicate must validate");
    ASSERT_TRUE(!row&&!n,"duplicate failure outputs clear");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"unknown\":{\"ignored\":[1,2]},\"f2\":3}",0,NULL,NULL,&row,&n,err,sizeof(err)),0,"unknown nested value ignored");
    if(row){
        TypedRowView v;ASSERT_EQ_INT(typed_row_open(ts,row,n,PKR_FLAGS,&v),0,"valid row before corruption");
        size_t descriptor=(size_t)ts->packed.prefix+ts->packed.fields[1].offset;
        uint8_t saved=row[descriptor];row[descriptor]=0xFF;
        ASSERT_EQ_INT(typed_row_open(ts,row,n,PKR_FLAGS,&v),-1,"bad descriptor rejected");
        ASSERT_TRUE(!v.schema&&!v.value,"invalid view cleared");
        row[descriptor]=saved;
        TypedPrepared prep;typed_prepared_init(&prep);SlotcaskOldRecord old={row,n,PKR_FLAGS};
        ASSERT_EQ_INT(typed_prepared_old(ts,&old,&prep,err,sizeof(err)),0,"reuse old fixture");
        TypedInput invalid={"bad",3,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&prep,0,&invalid,err,sizeof(err)),-1,"failed setter retains cell");
        uint8_t *after=NULL;size_t an=0;
        ASSERT_EQ_INT(typed_prepared_alloc(&prep,&after,&an,err,sizeof(err)),0,"retained cells still compose");
        if(after){ASSERT_EQ_INT(an,n,"failed setter retained size");
            ASSERT_EQ_INT(memcmp(after,row,n),0,"failed setter retained bytes");free(after);}
        typed_prepared_free(&prep);typed_prepared_free(&prep);
        ASSERT_TRUE(!prep.schema&&!prep.bytes,"idempotent prepared free");free(row);
    }
    schema_free(ts);
}
static void decimal_limits_and_types(void){
    const char *types[]={"numeric","enum(red,blue)","float","double","timestamp"};
    TypedSchema *ts=schema_new(types,5);ASSERT_NOT_NULL(ts,"bare decimal schema");if(!ts)return;
    size_t digits=131072+1+16383;char *number=malloc(digits+1);ASSERT_NOT_NULL(number,"maximum legal numeric text");
    if(number){memset(number,'9',131072);number[131072]='.';memset(number+131073,'1',16383);number[digits]=0;
        TypedPrepared p;typed_prepared_init(&p);char err[256];
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"prepare maximum decimal");
        TypedInput in={number,digits,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,0,&in,err,sizeof(err)),0,"full PostgreSQL-sized bare numeric accepted");
        uint8_t *row=NULL;size_t n=0;
        ASSERT_EQ_INT(typed_prepared_alloc(&p,&row,&n,err,sizeof(err)),0,"maximum numeric actual payload");
        if(row){size_t got=0;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
            ASSERT_NOT_NULL(text,"maximum numeric exact text");if(text){ASSERT_EQ_INT(got,digits,"maximum text length");
                ASSERT_EQ_INT(memcmp(text,number,digits),0,"all maximum digits survive");free(text);}free(row);}
        typed_prepared_free(&p);free(number);
    }
    char err[256];uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"1.0e2\",\"f1\":\"blue\",\"f2\":1.23456776,\"f3\":0.10000000000000001,\"f4\":0}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"fixed scalars share checked core");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,4096);ASSERT_NOT_NULL(text,"fixed and exponent render");
        if(text){ASSERT_CONTAINS(text,"\"f0\":100","exponent canonical scale");
            ASSERT_CONTAINS(text,"\"f1\":\"blue\"","enum directory value");
            ASSERT_CONTAINS(text,"1.23456776","float nine digits");
            ASSERT_CONTAINS(text,"0.10000000000000001","double seventeen digits");
            ASSERT_CONTAINS(text,"\"f4\":0","zero timestamp is value under bitmap");free(text);}free(row);}
    schema_free(ts);
}

static void json_contracts(void){
    const char *types[]={"varchar"};char err[256];TypedSchema *ts=schema_new(types,1);
    ASSERT_NOT_NULL(ts,"JSON schema");if(!ts)return;
    const char *bad[]={"{\"f0\":NaN}","{\"f0\":[1,,2]}","{\"unknown\":{\"x\":}}",
        "{\"unknown\":\"\\q\"}","{\"f0\":\"raw\nnewline\"}","{\"f0\":01}",
        "{\"f0\":1.}","{\"f0\":1e}","{\"f0\":true,}","{} trailing",
        "{\"f0\":{\"$copy\":\"f0\"}}","{\"f0\":[1,2]}"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
        uint8_t *row=(void*)1;size_t n=99;
        ASSERT_EQ_INT(typed_encode_alloc(ts,bad[i],1,NULL,NULL,&row,&n,err,sizeof(err)),-1,"malformed or non-scalar known value rejects");
        ASSERT_TRUE(!row&&!n,"JSON failure clears owned outputs");
    }
    uint8_t *row=NULL;size_t n=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"\\u00660\":\"value\"}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"escaped key matches schema");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,1024);ASSERT_NOT_NULL(text,"escaped key render");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"value\"}","decoded key retains supplied value");free(text);}free(row);}

    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":null\t}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"tab after primitive null accepts");
    if(row){size_t got=99;errno=123;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
        ASSERT_TRUE(!text&&got==0&&errno==0,"tab-terminated null stays NULL");free(text);free(row);}
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":123\r}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"CR after primitive accepts");
    if(row){size_t got=0;char *text=typed_packed_field_text(ts,row,n,PKR_FLAGS,0,&got);
        ASSERT_NOT_NULL(text,"primitive text with CR delimiter");if(text){ASSERT_EQ_STR(text,"123","JSON whitespace not field content");free(text);}free(row);}
    strcpy(ts->fields[0].name,"x\"y");ts->fields[0].name_len=3;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"x\\\"y\":\"value\"}",1,NULL,NULL,&row,&n,err,sizeof(err)),0,"escaped name consumes own JSON");
    if(row){char *text=typed_packed_decode(ts,row,n,PKR_FLAGS,1024);ASSERT_NOT_NULL(text,"round-trip escaped name");
        if(text){uint8_t *round=NULL;size_t rn=0;
            ASSERT_EQ_INT(typed_encode_alloc(ts,text,1,NULL,NULL,&round,&rn,err,sizeof(err)),0,"rendered JSON can be read");
            if(round){ASSERT_EQ_INT(rn,n,"round-trip name size");ASSERT_EQ_INT(memcmp(round,row,n),0,"round-trip name bytes");free(round);}free(text);}free(row);}
    char depth[512];size_t pos=0;memcpy(depth,"{\"unknown\":",11);pos=11;
    for(int i=0;i<63;i++)depth[pos++]='[';
    depth[pos++]='0';
    for(int i=0;i<63;i++)depth[pos++]=']';
    depth[pos++]='}';depth[pos]=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,depth,1,NULL,NULL,&row,&n,err,sizeof(err)),0,"64-container JSON depth accepts");free(row);
    memmove(depth+12,depth+11,pos-11+1);depth[11]='[';pos++;
    depth[pos-1]=']';depth[pos++]='}';depth[pos]=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,depth,1,NULL,NULL,&row,&n,err,sizeof(err)),-1,"65-container JSON depth rejects");
    schema_free(ts);
}

static void automatic_fields(void){
    const char *types[]={"varchar","timestamp","int"};char err[256];
    TypedSchema *ts=schema_new(types,3);ASSERT_NOT_NULL(ts,"automatic field schema");if(!ts)return;
    ts->fields[0].default_kind=DK_AUTO_CREATE;ts->fields[1].default_kind=DK_AUTO_UPDATE;
    uint8_t *old=NULL,*incoming=NULL;size_t oldn=0,incomingn=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"born\",\"f1\":1,\"f2\":5}",0,NULL,NULL,&old,&oldn,err,sizeof(err)),0,"automatic OLD seed");
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{\"f0\":\"overwrite\",\"f1\":2,\"f2\":6}",0,NULL,NULL,&incoming,&incomingn,err,sizeof(err)),0,"automatic incoming seed");
    if(old&&incoming){
        SlotcaskOldRecord record={old,oldn,PKR_FLAGS};uint8_t *out=NULL;size_t n=0;
        ASSERT_EQ_INT(typed_transfer_ac_alloc(ts,&record,incoming,incomingn,&out,&n,err,sizeof(err)),0,"auto-create carries OLD before composition");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
            ASSERT_NOT_NULL(text,"carried auto-create text");if(text){ASSERT_EQ_STR(text,"born","supplied overwrite ignored");free(text);}free(out);}
        ASSERT_EQ_INT(typed_transfer_ac_alloc(ts,NULL,incoming,incomingn,&out,&n,err,sizeof(err)),0,"fresh auto-create stamp actual size");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,0,&len);
            ASSERT_NOT_NULL(text,"fresh auto-create text");if(text){ASSERT_EQ_INT(len,14,"varchar auto timestamp canonical length");free(text);}free(out);}
        ASSERT_EQ_INT(typed_update_alloc(ts,&record,NULL,1,&out,&n,err,sizeof(err)),0,"auto-update stamp before compose");
        if(out){size_t len=0;char *text=typed_packed_field_text(ts,out,n,PKR_FLAGS,1,&len);
            ASSERT_NOT_NULL(text,"auto-update timestamp text");if(text){ASSERT_TRUE(strtoll(text,NULL,10)>1,"timestamp refreshed without clock deadline");free(text);}free(out);}
        TypedPrepared prep;typed_prepared_init(&prep);
        ASSERT_EQ_INT(typed_prepared_old(ts,&record,&prep,err,sizeof(err)),0,"stamp validation fixture");
        ASSERT_EQ_INT(typed_prepared_stamp(&prep,DK_SEQ,err,sizeof(err)),-1,"timestamp API refuses arbitrary generator kinds");
        typed_prepared_free(&prep);
    }
    free(old);free(incoming);schema_free(ts);
}

static void blind_review_boundaries(void){
    char err[256];uint8_t *out=NULL;size_t len=0;
    const char *types[]={"int"};TypedSchema *ts=schema_new(types,1);
    ASSERT_NOT_NULL(ts,"blind packed scalar schema");
    if(ts){
        TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"blind prepare scalar");
        TypedInput bad={"bad",3,1,0,0};errno=0;
        ASSERT_EQ_INT(typed_prepared_set(&p,0,&bad,err,sizeof(err)),-1,"invalid packed scalar fails");
        ASSERT_EQ_INT(errno,EINVAL,"packed scalar failure sets errno");
        ASSERT_EQ_INT(p.seen[0],0,"packed scalar failure retains seen");
        typed_prepared_free(&p);schema_free(ts);
    }
    const char *enums[]={"enum(red,blue)"};ts=schema_new(enums,1);
    ASSERT_NOT_NULL(ts,"blind enum schema");
    if(ts){
        TypedSchema *before=malloc(sizeof(*before));ASSERT_NOT_NULL(before,"blind enum snapshot");
        if(before){memcpy(before,ts,sizeof(*before));
            const char *bad[]={"enum(red,blue):default=bad","enum(red,blue):auto_update","enum(red,blue)junk"};
            for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
                const char *token[]={bad[i]};errno=ENOMEM;
                ASSERT_EQ_INT(typed_schema_bind_packed(ts,token,1,err,sizeof(err)),-1,"enum suffix rejected");
                ASSERT_EQ_INT(errno,EINVAL,"enum syntax failure is deterministic");
                ASSERT_EQ_INT(memcmp(ts,before,sizeof(*ts)),0,"enum suffix failure preserves schema");
            }free(before);
        }schema_free(ts);
    }
    ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"blind legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=1;ts->nullmap_prefix=2;ts->total_size=6;
    parse_field_type("int",&ts->fields[0]);strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;
    for(int defaults=0;defaults<=1;defaults++){
        out=(void*)1;len=99;errno=ENOMEM;
        ASSERT_TRUE(typed_encode_alloc(ts,"{\"f0\":\"bad\"}",defaults,NULL,NULL,&out,&len,err,sizeof(err))<0,"legacy scalar encoding fails");
        ASSERT_TRUE(!out&&!len,"legacy encode failure clears owned output");
        ASSERT_EQ_INT(errno,EINVAL,"legacy scalar encoding sets fresh errno");
    }
    uint8_t bytes[]={1,0,0,0,0,1};SlotcaskOldRecord old={bytes,sizeof(bytes),1};
    TypedInput in[MAX_FIELDS]={0};in[0]=(TypedInput){"bad",3,1,0,0};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&old,in,0,&out,&len,err,sizeof(err)),-1,"legacy scalar update fails");
    ASSERT_TRUE(!out&&!len,"legacy update failure clears output");
    ASSERT_EQ_INT(errno,EINVAL,"legacy scalar update sets errno");
    in[0]=(TypedInput){NULL,1,1,0,0};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&old,in,0,&out,&len,err,sizeof(err)),-1,"invalid legacy span fails");
    ASSERT_EQ_INT(errno,EINVAL,"invalid legacy span sets errno");
    uint8_t broken[]={0};SlotcaskOldRecord malformed={broken,sizeof(broken),1};errno=0;
    ASSERT_EQ_INT(typed_update_alloc(ts,&malformed,NULL,0,&out,&len,err,sizeof(err)),-1,"malformed legacy OLD fails");
    ASSERT_TRUE(!out&&!len,"malformed OLD clears output");
    ASSERT_EQ_INT(errno,EILSEQ,"malformed OLD reports corruption");
    /* Keep a bad-input crash isolated from the runner. No global fixture or
       environment is changed; the child touches only this inherited schema. */
    pid_t child=fork();
    if(child!=0)ASSERT_TRUE(child>=0,"fork NULL JSON boundary probe");
    if(child==0){
        alarm(5);
        for(int defaults=0;defaults<=1;defaults++){
            uint8_t *value=(void*)1;size_t n=99;errno=0;
            int rc=typed_encode_alloc(ts,NULL,defaults,NULL,NULL,&value,&n,NULL,0);
            if(rc>=0||errno!=EINVAL||value||n)_exit(1);
        }
        _exit(0);
    }
    if(child>0){int status=0;pid_t got=waitpid(child,&status,0);
        ASSERT_TRUE(got==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"NULL JSON rejects without crashing in both modes");}
    free(ts);
}
static void enum_membership_regressions(void){
    const char *types[]={"enum(red,blue)"};
    TypedSchema *ts=schema_new(types,1);ASSERT_NOT_NULL(ts,"enum membership schema");if(!ts)return;
    char err[256];uint8_t *out=NULL;size_t n=0;
    const char *bad[]={"{\"f0\":\"bogus\"}","{\"f0\":\"bogus\",\"f0\":\"blue\"}","{\"f0\":true}"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
        out=(void*)1;n=99;errno=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,bad[i],1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"unknown enum rejects before coercion or duplicate replacement");
        ASSERT_EQ_INT(errno,EINVAL,"unknown enum is invalid input");
        ASSERT_TRUE(!out&&!n,"unknown enum clears owned outputs");
        free(out&&out!=(void*)1?out:NULL);
    }
    const char *good[]={"{\"f0\":\"\\u0062lue\"}","{\"f0\":\"\"}","{\"f0\":null}"};
    const char *expected[]={"{\"f0\":\"blue\"}","{\"f0\":\"red\"}","{\"f0\":null}"};
    for(size_t i=0;i<sizeof(good)/sizeof(good[0]);i++){
        out=NULL;n=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,good[i],1,NULL,NULL,&out,&n,err,sizeof(err)),0,"escaped member empty and null retain contracts");
        if(out){
            char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
            ASSERT_NOT_NULL(text,"accepted enum renders");
            if(text){ASSERT_EQ_STR(text,expected[i],"accepted enum value exact");free(text);}
            free(out);
        }
    }
    TypedPrepared p;typed_prepared_init(&p);
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"enum prepared zero");
    TypedInput valid={"blue",4,1,0,0},invalid={"bogus",5,1,0,0};
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&valid,err,sizeof(err)),0,"enum set valid member");
    PkrCell cell=p.cells[0];uint8_t *owner=p.owned[0],seen=p.seen[0];size_t bytes=p.bytes;
    uint8_t saved[2]={0};ASSERT_TRUE(cell.len<=sizeof(saved),"enum snapshot bounded");
    if(cell.len<=sizeof(saved)&&cell.data)memcpy(saved,cell.data,cell.len);
    errno=0;
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&invalid,err,sizeof(err)),-1,"direct unknown enum rejects");
    ASSERT_EQ_INT(errno,EINVAL,"direct rejection errno");
    ASSERT_TRUE(p.cells[0].data==cell.data&&p.cells[0].len==cell.len&&p.cells[0].is_null==cell.is_null,"rejected setter preserves cell");
    ASSERT_TRUE(p.owned[0]==owner&&p.seen[0]==seen&&p.bytes==bytes,"rejected setter preserves ownership seen and size");
    ASSERT_TRUE(p.cells[0].len==cell.len&&cell.len<=sizeof(saved)&&!memcmp(p.cells[0].data,saved,cell.len),"rejected setter preserves bytes");
    typed_prepared_free(&p);
    ts->fields[0].default_kind=DK_LITERAL;strcpy(ts->fields[0].default_val,"blue");
    ASSERT_EQ_INT(typed_prepared_zero(ts,&p,err,sizeof(err)),0,"enum zero for default eligibility");
    ASSERT_EQ_INT(typed_prepared_set(&p,0,&invalid,err,sizeof(err)),-1,"invalid supplied enum does not count as seen");
    ASSERT_EQ_INT(p.seen[0],0,"failed setter leaves omission eligible");
    ASSERT_EQ_INT(typed_prepared_defaults(&p,NULL,NULL,err,sizeof(err)),0,"default after rejected setter");
    out=NULL;n=0;
    ASSERT_EQ_INT(typed_prepared_alloc(&p,&out,&n,err,sizeof(err)),0,"compose default enum");
    if(out){
        char *text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
        ASSERT_NOT_NULL(text,"default enum renders");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"blue\"}","unseen default applied");free(text);}
        SlotcaskOldRecord old={out,n,PKR_FLAGS};TypedInput inputs[MAX_FIELDS]={0};inputs[0]=invalid;
        uint8_t *next=(void*)1;size_t nextn=99;errno=0;
        ASSERT_EQ_INT(typed_update_alloc(ts,&old,inputs,0,&next,&nextn,err,sizeof(err)),-1,"unknown enum rejects update");
        ASSERT_EQ_INT(errno,EINVAL,"enum update invalid input");
        ASSERT_TRUE(!next&&!nextn,"enum update clears outputs");
        free(next&&next!=(void*)1?next:NULL);
        text=typed_packed_decode(ts,out,n,PKR_FLAGS,1024);
        ASSERT_NOT_NULL(text,"OLD remains readable");
        if(text){ASSERT_EQ_STR(text,"{\"f0\":\"blue\"}","failed enum update preserves OLD");free(text);}
        free(out);
    }
    typed_prepared_free(&p);
    strcpy(ts->fields[0].default_val,"bogus");out=(void*)1;n=99;errno=0;
    ASSERT_EQ_INT(typed_encode_alloc(ts,"{}",1,NULL,NULL,&out,&n,err,sizeof(err)),-1,"unknown enum default rejects");
    ASSERT_EQ_INT(errno,EINVAL,"invalid enum default errno");
    ASSERT_TRUE(!out&&!n,"invalid enum default clears outputs");
    free(out&&out!=(void*)1?out:NULL);
    schema_free(ts);
}

static int test_typed_row_adapter_run(void){
    encoding();updates_and_reuse();legacy_views();maximum_and_binary();review_regressions();schema_and_failure_contracts();decimal_limits_and_types();json_contracts();automatic_fields();blind_review_boundaries();enum_membership_regressions();
    return t_ctx->failed?1:0;
}
TEST_REGISTER("test-typed-row-adapter",test_typed_row_adapter_run)
