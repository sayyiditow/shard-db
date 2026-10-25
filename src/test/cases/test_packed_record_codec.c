#include "test_runner.h"
#include "test_assert.h"
#include "packed_record.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>
_Static_assert(PKR_MAX_FIELDS == MAX_FIELDS, "packed field-count contract");

static void numeric_case(const char *decl, const char *text, dec_status expected,
                          const char *render) {
    PkrField f; memset(&f, 0, sizeof(f));
    dec_status st = pkr_field_parse(decl, &f);
    ASSERT_EQ_INT(st, DEC_OK, "numeric declaration");
    if (st != DEC_OK) return;
    uint8_t bytes[1024]; memset(bytes, 0xA5, sizeof(bytes));
    size_t used = 99; char err[256];
    st = pkr_numeric_encode(&f, text, strlen(text), bytes, sizeof(bytes), &used,
                             err, sizeof(err));
    ASSERT_EQ_INT(st, expected, "exact numeric encoding status");
    if (st != DEC_OK) {
        ASSERT_EQ_INT(used, 0, "numeric failure clears length");
        ASSERT_EQ_INT(bytes[0], 0xA5, "numeric failure preserves buffer");
        return;
    }
    dec_t d; dec_init(&d); char out[2048]; size_t outn = 0;
    st = dec_unpack(bytes, used, &d);
    ASSERT_EQ_INT(st, DEC_OK, "numeric payload independently decodes");
    if (st == DEC_OK) {
        ASSERT_EQ_INT(dec_to_text(&d, out, sizeof(out), &outn), DEC_OK,
                      "numeric renders");
        ASSERT_EQ_STR(out, render, "exact numeric text");
    }
    dec_free(&d);
}
static void declarations(void) {
    const char *valid[] = {"varchar", "varchar:1", "varchar:1000000",
        "varchar:1000001", "varchar:4194304", "numeric", "numeric:26,10",
        "numeric:3,5", "numeric:4,-2", "numeric:1000,-1000", "currency",
        "numeric:1,+2", "numeric:00000000000000000001"};
    for (size_t i = 0; i < sizeof(valid)/sizeof(valid[0]); i++) {
        PkrField f; memset(&f, 0xA5, sizeof(f));
        ASSERT_EQ_INT(pkr_field_parse(valid[i], &f), DEC_OK, valid[i]);
    }
    const char *invalid[] = {"", "n:numeric", "varchar:0", "varchar:4194305",
        "varchar:-1", "varchar:12garbage", "numeric:0", "numeric:1001",
        "numeric:1,1001", "numeric:1,-1001", "numeric:1,", "numeric:1,2x",
        "numeric:+1", "numeric:9999999999999999999999999999999999999",
        "varchar:9999999999999999999999999999999999999"};
    for (size_t i = 0; i < sizeof(invalid)/sizeof(invalid[0]); i++) {
        PkrField f; memset(&f, 0xA5, sizeof(f));
        ASSERT_EQ_INT(pkr_field_parse(invalid[i], &f), DEC_ERR_INVALID, invalid[i]);
        ASSERT_EQ_INT(f.kind, 0, "invalid declaration clears metadata");
    }
    PkrField a, b;
    ASSERT_EQ_INT(pkr_field_parse(NULL, &a), DEC_ERR_INVALID, "NULL declaration");
    ASSERT_EQ_INT(pkr_field_parse("numeric", NULL), DEC_ERR_INVALID, "NULL field output");
    ASSERT_EQ_INT(pkr_field_fixed(0, &a), DEC_ERR_INVALID, "zero fixed width");
    ASSERT_EQ_INT(pkr_field_fixed(17, &a), DEC_ERR_INVALID, "oversized fixed width");
    ASSERT_EQ_INT(pkr_field_fixed(4, &a), DEC_OK, "four fixed bytes");
    ASSERT_EQ_INT(pkr_field_parse("numeric", &a), DEC_OK, "bare numeric metadata");
    ASSERT_EQ_INT(a.bare, 1, "bare numeric retained");
    ASSERT_EQ_INT(a.precision, -1, "bare precision marker");
    ASSERT_EQ_INT(a.payload_max, DEC_PAYLOAD_MAX, "bare numeric cap");
    ASSERT_EQ_INT(pkr_field_parse("numeric:1,1000", &a), DEC_OK, "S>P declaration");
    ASSERT_EQ_INT(a.payload_max, 17, "canonical S>P coefficient cap is P digits");
    ASSERT_EQ_INT(pkr_field_parse("numeric:2,-2", &a), DEC_OK, "negative scale");
    ASSERT_EQ_INT(a.payload_max, 17, "negative scale implicit zeros are not coefficient digits");
    ASSERT_EQ_INT(pkr_field_parse("currency", &a), DEC_OK, "currency metadata");
    ASSERT_EQ_INT(a.currency, 1, "currency identity retained");
    ASSERT_EQ_INT(pkr_field_parse("numeric:19,4", &b), DEC_OK, "same numeric contract");
    ASSERT_EQ_INT(b.currency, 0, "explicit numeric identity distinct");
    numeric_case("numeric:26,10", "1234567890123456.1234567890", DEC_OK,
                  "1234567890123456.1234567890");
    numeric_case("numeric:3,1", "1.25", DEC_OK, "1.3");
    numeric_case("numeric:3,1", "-1.25", DEC_OK, "-1.3");
    numeric_case("numeric:3,1", "99.95", DEC_ERR_PRECISION, NULL);
    numeric_case("numeric:3,5", "0.00999", DEC_OK, "0.00999");
    numeric_case("numeric:2,-2", "9949", DEC_OK, "9900");
    numeric_case("numeric:2,-2", "9950", DEC_ERR_PRECISION, NULL);
    numeric_case("currency", "12.34565", DEC_OK, "12.3457");
    numeric_case("numeric", "1.2300", DEC_OK, "1.2300");
    numeric_case("numeric", "1.0e2", DEC_OK, "100");
    numeric_case("numeric", "", DEC_ERR_SYNTAX, NULL);
    numeric_case("numeric", "NaN", DEC_ERR_SYNTAX, NULL);
}
static void mixed_and_capacity(void) {
    uint8_t integer[] = {0,0,0,7}, text[] = {'a','b'}, num[64]; size_t nn = 0;
    PkrField fnum;
    if (pkr_field_parse("numeric", &fnum) != DEC_OK) {
        ASSERT_TRUE(0, "mixed test prerequisite numeric parser"); return;
    }
    ASSERT_EQ_INT(pkr_numeric_encode(&fnum, "1.50", 4, num, sizeof(num), &nn,
                                     NULL, 0), DEC_OK, "mixed numeric payload");
    for (int pos = 0; pos < 3; pos++) {
        PkrField fields[3]; PkrCell cells[3];
        for (int i = 0; i < 3; i++) {
            if (i == pos) {
                pkr_field_fixed(4, &fields[i]);
                cells[i] = (PkrCell){integer,4,0};
            } else if (i == (pos+1)%3) {
                pkr_field_parse("varchar:8", &fields[i]);
                cells[i] = (PkrCell){text,2,0};
            } else { fields[i] = fnum; cells[i] = (PkrCell){num,nn,0}; }
        }
        PkrSchema s; dec_status st = pkr_schema_build(fields,3,&s);
        ASSERT_EQ_INT(st,DEC_OK,"mixed schema"); if (st != DEC_OK) continue;
        size_t need = 0; st = pkr_measure(&s,cells,3,&need);
        ASSERT_EQ_INT(st,DEC_OK,"measure mixed"); if (st != DEC_OK) continue;
        ASSERT_EQ_INT(need,2+4+8+8+2+nn,"actual includes only real payloads");
        uint8_t *out = malloc(need+16); ASSERT_NOT_NULL(out,"exact capacity plus canary");
        if (!out) continue;
        memset(out,0xA5,need+16); size_t used = 99;
        ASSERT_EQ_INT(pkr_compose(&s,cells,3,out,need-1,&used),DEC_ERR_TRUNCATED,
                      "one byte short");
        ASSERT_EQ_INT(used,0,"short failure length");
        int intact=1;for(size_t k=0;k<need+16;k++)if(out[k]!=0xA5)intact=0;
        ASSERT_TRUE(intact,"failed composition did not write");
        st=pkr_compose(&s,cells,3,out,need,&used);
        ASSERT_EQ_INT(st,DEC_OK,"actual capacity accepted below schema maximum");
        ASSERT_EQ_INT(used,need,"stored length exact");
        int canary=1;for(size_t k=need;k<need+16;k++)if(out[k]!=0xA5)canary=0;
        ASSERT_TRUE(canary,"no write after actual capacity");
        PkrView v;
        ASSERT_EQ_INT(pkr_view(&s,out,used,PKR_FLAGS,&v),DEC_OK,"composed view");
        for(size_t i=0;i<3;i++) {
            PkrSpan sp; st=pkr_span(&v,i,&sp);
            ASSERT_EQ_INT(st,DEC_OK,"mixed span");
            ASSERT_EQ_INT(sp.state,PKR_PRESENT,"mixed present");
            ASSERT_EQ_INT(sp.len,cells[i].len,"span length");
            if(st==DEC_OK&&sp.data&&sp.len==cells[i].len)
                ASSERT_EQ_INT(memcmp(sp.data,cells[i].data,sp.len),0,"exact bytes incl fixed int 7");
        }
        free(out);
    }
}
static void validation(void) {
    PkrField fields[2];pkr_field_parse("varchar:8",&fields[0]);
    pkr_field_parse("varchar:8",&fields[1]);PkrSchema s;
    if(pkr_schema_build(fields,2,&s)!=DEC_OK){ASSERT_TRUE(0,"view prerequisite");return;}
    const uint8_t a[]="abcd",b[]="wxyz";
    PkrCell cells[2]={{a,4,0},{b,4,0}};uint8_t good[64],bad[64];size_t used=0;
    dec_status st=pkr_compose(&s,cells,2,good,sizeof(good),&used);
    ASSERT_EQ_INT(st,DEC_OK,"validation seed");if(st!=DEC_OK)return;
    for(int test=0;test<12;test++) {
        memcpy(bad,good,used);uint8_t *d=bad+s.prefix;size_t n=used;
        switch(test) {
        case 0:d[3]=4;break;                /* inside directory */
        case 1:d[7]=5;break;                /* overlap */
        case 2:d[11]=21;break;              /* gap */
        case 3:d[3]=20;d[11]=16;break;      /* reverse order */
        case 4:d[0]=d[1]=d[2]=d[3]=255;break;
        case 5:d[7]=9;break;                /* field cap */
        case 6:d[3]=0;break;
        case 7:d[7]=0;break;                /* nonzero off, zero len */
        case 8:bad[1]|=1;break;             /* NULL with live payload */
        case 9:bad[0]=0;break;
        case 10:bad[1]|=0x80;break;         /* noncanonical bitmap padding */
        default:bad[n++]=0;break;           /* trailing bytes */
        }
        PkrView v;memset(&v,0xA5,sizeof(v));
        ASSERT_EQ_INT(pkr_view(&s,bad,n,PKR_FLAGS,&v),DEC_ERR_PAYLOAD,
                      "malformed record rejected");
        ASSERT_TRUE(!v.schema&&!v.value&&!v.len,"failed view cleared");
    }
    uint8_t flags[]={0,1,2,0xFF};
    for(size_t i=0;i<sizeof(flags);i++) {
        PkrView v;ASSERT_EQ_INT(pkr_view(&s,good,used,flags[i],&v),DEC_ERR_PAYLOAD,
                                "incompatible generation rejected");
    }
    PkrView v;ASSERT_EQ_INT(pkr_view(&s,good,used-1,PKR_FLAGS,&v),DEC_ERR_PAYLOAD,
                            "truncation rejected");
    size_t n=0;
    PkrCell alias[2]={{good+18,4,0},{b,4,0}};
    ASSERT_EQ_INT(pkr_compose(&s,alias,2,good,sizeof(good),&n),DEC_ERR_INVALID,
                  "overlapping output and payload rejected");
    cells[0]=(PkrCell){NULL,0,1};cells[1]=(PkrCell){NULL,0,0};
    ASSERT_EQ_INT(pkr_compose(&s,cells,2,good,sizeof(good),&used),DEC_OK,
                  "NULL and empty compose");
    ASSERT_EQ_INT(pkr_view(&s,good,used,PKR_FLAGS,&v),DEC_OK,"NULL/empty view");
    PkrSpan sp;
    ASSERT_EQ_INT(pkr_span(&v,0,&sp),DEC_OK,"NULL span");
    ASSERT_TRUE(sp.state==PKR_NULL&&!sp.data&&!sp.len,"NULL distinct");
    ASSERT_EQ_INT(pkr_span(&v,1,&sp),DEC_OK,"empty span");
    ASSERT_TRUE(sp.state==PKR_PRESENT&&sp.data&&sp.len==0,"empty present non-NULL pointer");
    ASSERT_EQ_INT(pkr_span(&v,2,&sp),DEC_ERR_INVALID,"bad index");
    ASSERT_EQ_INT(sp.state,PKR_INVALID,"failed span clear");
    s.fields[0].offset++;
    ASSERT_EQ_INT(pkr_measure(&s,cells,2,&used),DEC_ERR_INVALID,"tampered schema rejected");
}
static void numeric_cells(void) {
    PkrField f;PkrSchema s;uint8_t pay[64],record[128];size_t n=0,used=0;
    if(pkr_field_parse("numeric",&f)!=DEC_OK||pkr_schema_build(&f,1,&s)!=DEC_OK){
        ASSERT_TRUE(0,"numeric cell prerequisite");return;
    }
    ASSERT_EQ_INT(pkr_numeric_encode(&f,"0",1,pay,sizeof(pay),&n,NULL,0),DEC_OK,"zero encodes");
    ASSERT_EQ_INT(n,16,"numeric zero has header");
    PkrCell c={pay,n,0};
    ASSERT_EQ_INT(pkr_compose(&s,&c,1,record,sizeof(record),&used),DEC_OK,"zero compose");
    PkrView v;ASSERT_EQ_INT(pkr_view(&s,record,used,PKR_FLAGS,&v),DEC_OK,"zero view");
    PkrSpan sp;ASSERT_EQ_INT(pkr_span(&v,0,&sp),DEC_OK,"zero span");
    ASSERT_TRUE(sp.state==PKR_PRESENT&&sp.len==16,"zero not NULL or empty");
    c=(PkrCell){NULL,0,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"empty numeric payload rejected");
    pay[0]=99;c=(PkrCell){pay,n,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"invalid numeric rejected in sizing");
    ASSERT_EQ_INT(pkr_numeric_encode(&f,"1.25",4,pay,sizeof(pay),&n,NULL,0),DEC_OK,"bare1.25");
    ASSERT_EQ_INT(pkr_field_parse("numeric:3,1",&f),DEC_OK,"constrained cell schema");
    ASSERT_EQ_INT(pkr_schema_build(&f,1,&s),DEC_OK,"constrained schema");c=(PkrCell){pay,n,0};
    ASSERT_EQ_INT(pkr_measure(&s,&c,1,&used),DEC_ERR_PAYLOAD,"non-assigned constrained payload rejected");
}
static void largest_numeric(void) {
    PkrField f;PkrSchema schema;
    if(pkr_field_parse("numeric",&f)!=DEC_OK||pkr_schema_build(&f,1,&schema)!=DEC_OK){
        ASSERT_TRUE(0,"largest numeric prerequisite");return;
    }
    size_t chars=DEC_MAX_INT_DIGITS+1+DEC_MAX_FRAC_DIGITS;
    char *text=malloc(chars+1);uint8_t *payload=malloc(DEC_PAYLOAD_MAX);
    ASSERT_NOT_NULL(text,"147455-digit input");ASSERT_NOT_NULL(payload,"73744-byte payload");
    if(!text||!payload){free(text);free(payload);return;}
    memset(text,'9',chars);text[DEC_MAX_INT_DIGITS]='.';text[chars]='\0';size_t n=0;
    dec_status st=pkr_numeric_encode(&f,text,chars,payload,DEC_PAYLOAD_MAX,&n,NULL,0);
    ASSERT_EQ_INT(st,DEC_OK,"entire agreed numeric domain");
    ASSERT_EQ_INT(n,DEC_PAYLOAD_MAX,"max numeric payload exact");
    if(st==DEC_OK){PkrCell c={payload,n,0};size_t need=0;
        ASSERT_EQ_INT(pkr_measure(&schema,&c,1,&need),DEC_OK,"max numeric sizing");
        uint8_t *record=malloc(need);ASSERT_NOT_NULL(record,"max numeric actual row");
        if(record){size_t used=0;ASSERT_EQ_INT(pkr_compose(&schema,&c,1,record,need,&used),DEC_OK,
                                             "max numeric compose");
            PkrView v;ASSERT_EQ_INT(pkr_view(&schema,record,used,PKR_FLAGS,&v),DEC_OK,
                                    "max numeric validated view");
            free(record);}}
    free(payload);free(text);
}
static void maxima(void) {
    PkrField fields[PKR_MAX_FIELDS];PkrSchema s;PkrCell cells[PKR_MAX_FIELDS];
    PkrField f;if(pkr_field_parse("varchar",&f)!=DEC_OK){ASSERT_TRUE(0,"max prerequisite");return;}
    for(size_t i=0;i<PKR_MAX_FIELDS;i++){fields[i]=f;cells[i]=(PkrCell){NULL,0,1};}
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS,&s),DEC_OK,"256 varchar schema");
    ASSERT_EQ_INT(s.maximum,1073743905,"4MiB maximum value bound");
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&s,1024,&slot),DEC_OK,"maximum slot bound");
    ASSERT_EQ_INT(slot,1073744960,"maximum aligned slot");
    cells[127]=(PkrCell){(const uint8_t*)"tiny",4,0};size_t used=0;
    ASSERT_EQ_INT(pkr_measure(&s,cells,PKR_MAX_FIELDS,&used),DEC_OK,"maximum schema tiny measure");
    ASSERT_EQ_INT(used,2085,"small row no padding to maximum");
    uint8_t *out=malloc(used+1);ASSERT_NOT_NULL(out,"allocate actual2085 not1GiB");
    if(out){out[used]=0xA5;size_t n=0;
        ASSERT_EQ_INT(pkr_compose(&s,cells,PKR_MAX_FIELDS,out,used,&n),DEC_OK,"max schema exact output");
        ASSERT_EQ_INT(out[used],0xA5,"maxschema canary");
        PkrView v;ASSERT_EQ_INT(pkr_view(&s,out,n,PKR_FLAGS,&v),DEC_OK,"maxschema validates");free(out);}
    size_t bytes=0;
    ASSERT_EQ_INT(pkr_record_fits(&s,used,1,4096,&bytes),DEC_OK,"tiny record fits segment");
    ASSERT_EQ_INT(pkr_record_fits(&s,s.maximum,1024,128u*1024u*1024u,&bytes),DEC_ERR_RANGE,
                  "maximum theoretical row exceeds segment");
    ASSERT_EQ_INT(bytes,0,"segment rejection clears size");
    ASSERT_EQ_INT(pkr_record_fits(&s,used,1,32,&bytes),DEC_ERR_RANGE,"actual capacity gate");
    ASSERT_EQ_INT(pkr_slot_bound(&s,SIZE_MAX,&bytes),DEC_ERR_RANGE,"key overflow rejected");
    pkr_field_parse("numeric",&f);for(size_t i=0;i<PKR_MAX_FIELDS;i++)fields[i]=f;
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS,&s),DEC_OK,"maxnumeric schema");
    ASSERT_EQ_INT(s.maximum,18880545,"maxnumeric bound");
    ASSERT_EQ_INT(pkr_slot_bound(&s,1024,&bytes),DEC_OK,"maxnumeric slot");
    ASSERT_EQ_INT(bytes,18881600,"maxnumeric aligned slot");
    ASSERT_EQ_INT(pkr_schema_build(fields,0,&s),DEC_ERR_INVALID,"zero fields rejected");
    ASSERT_EQ_INT(s.nfields,0,"failed schema cleared");
    ASSERT_EQ_INT(pkr_schema_build(fields,PKR_MAX_FIELDS+1,&s),DEC_ERR_INVALID,"too many fields");
}
static int test_packed_record_codec_run(void) {
    declarations();mixed_and_capacity();validation();numeric_cells();largest_numeric();maxima();
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-packed-record-codec",test_packed_record_codec_run)
