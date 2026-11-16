#include "types.h"
#include "typed_row.h"
#include "shard_db_internal.h"
#include "query_internal.h"
#ifndef R2_BASE
#include "typed_index.h"
#endif
#include "test_runner.h"
#include "test_assert.h"
#include <stdio.h>
#define CHECK(c,msg) ASSERT_TRUE((c),msg)
static int matches(const uint8_t *record,size_t n,SearchCriterion *raw,TypedSchema *ts,uint8_t flags){
    FieldSchema fs={0};fs.ts=ts;
    CompiledCriterion *cc=compile_criteria(raw,1,ts);
    if(!cc)return -2;
#ifdef R2_BASE
    int m=match_typed(record,n,cc,&fs,flags);
#else
    int m=99;int rc=match_typed_checked(record,n,cc,&fs,flags,&m);
    if(rc)m=-2;
#endif
    free_compiled_criteria(cc,1);return m;
}
#ifndef R2_BASE
extern void query_plan_test_set_fail_in_copy(int fail_n);
static void predicate_budget_checks(TypedSchema *ts,const uint8_t *packed,size_t packed_len,const char *long_text){
    /* g_db is thread-local: isolate the cap without changing shared request
       configuration or any live database/background worker's state. */
    ShardDb *previous=g_db,*isolated=calloc(1,sizeof(*isolated));
    CHECK(isolated!=NULL,"isolated thread-local budget context");if(!isolated)return;
    isolated->query_buffer_max_bytes=128*1024;g_db=isolated;
    SearchCriterion raw={0};
    CHECK(!search_criterion_set(&raw,0,long_text,70000),"first owned operand fits collection budget");
    errno=0;CHECK(search_criterion_set(&raw,1,long_text,70000)==-1&&errno==ENOMEM&&!raw.owned_value2,
        "second owned operand charged before allocation");search_criterion_clear(&raw);
    char *json=malloc(150300),*medium=malloc(40001);
    CHECK(json&&medium,"budget test borrowed input fixtures");
    if(json&&medium){
        memset(medium,'a',40000);medium[40000]=0;
        snprintf(json,150300,"{\"or\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"%s\"},{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"%s\"}]}",medium,medium);
        const char *error=NULL;CriteriaNode *tree=parse_criteria_tree(json,&error);
        CHECK(tree!=NULL,"retained parsed operands fit cumulative cap");
        errno=0;CHECK(tree&&compile_criteria_tree_checked(tree,ts,NULL,0)==-1&&errno==ENOMEM&&
            !tree->children[0]->compiled&&!tree->children[1]->compiled,
            "whole-tree compile preflight rejects copies before any leaf prepare");free_criteria_tree(tree);
        snprintf(json,150300,"{\"or\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"%s\"},{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"%s\"}]}",long_text,long_text);
        errno=0;tree=parse_criteria_tree(json,&error);
        CHECK(!tree&&errno==ENOMEM,"parser charges complete retained collection before second long allocation");free_criteria_tree(tree);
    }
    free(json);free(medium);
    raw=(SearchCriterion){0};strcpy(raw.field,"f1+f1");raw.op=OP_EQUAL;strcpy(raw.value,"x");
    CompiledCriterion *compiled=compile_criteria(&raw,1,ts);FieldSchema fs={0};fs.ts=ts;int matched=99;
    errno=0;CHECK(compiled&&match_typed_checked(packed,packed_len,compiled,&fs,PKR_FLAGS,&matched)==-1&&
        errno==ENOMEM&&matched==0,"packed composite matching admits actual workspace before allocation");
    free_compiled_criteria(compiled,1);
    g_db=previous;free(isolated);
}
#endif

static int test_r2_predicate_verification_run(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ts->typed=1;ts->nfields=2;
    for(int i=0;i<2;i++){TypedField *f=&ts->fields[i];f->type=FT_NUMERIC;f->size=8;f->offset=8*i;f->numeric_scale=i?2:1;f->numeric_scale_mult=i?100:10;snprintf(f->name,sizeof(f->name),"n%d",i);}
    uint8_t record[16]={0};record[7]=10;record[15]=100;
    SearchCriterion raw={0};strcpy(raw.field,"n0");strcpy(raw.value,"1.04");raw.op=OP_EQUAL;
    CHECK(matches(record,sizeof(record),&raw,ts,0)==0,"numeric eq literal is exact, never rounded to field scale");
    raw.op=OP_GREATER;strcpy(raw.value,"0.96");
    CHECK(matches(record,sizeof(record),&raw,ts,0)==1,"numeric range literal exact between storage quanta");
    raw.op=OP_EQ_FIELD;strcpy(raw.value,"n1");
    CHECK(matches(record,sizeof(record),&raw,ts,0)==1,"mixed-scale numeric field equality");
#ifndef R2_BASE
    SearchCriterion *oom_criteria = NULL;
    int oom_count = -1;
    query_plan_test_set_fail_in_copy(1);
    errno = 0;
    int oom_status = parse_criteria_json(
        "[{\"field\":\"n0\",\"op\":\"in\",\"value\":\"1,2\"}]",
        &oom_criteria, &oom_count);
    query_plan_test_set_fail_in_copy(0);
    CHECK(oom_status == -1 && errno == ENOMEM && !oom_criteria && oom_count == 0,
          "membership copy allocation failure returns clean parse error");
    free_criteria(oom_criteria, oom_count > 0 ? oom_count : 0);
    const char *recompile_error = NULL;
    CriteriaNode *recompiled = parse_criteria_tree(
        "{\"field\":\"n0\",\"op\":\"eq\",\"value\":\"1.0\"}",
        &recompile_error);
    CHECK(recompiled && !compile_criteria_tree_checked(recompiled, ts, NULL, 0),
          "prepare valid compiled leaf before operand mutation");
    if (recompiled) {
        CHECK(!search_criterion_set(&recompiled->leaf, 0, "invalid", 7),
              "replace compiled operand with malformed decimal");
        errno = 0;
        recompile_criteria_tree(recompiled, ts);
        CHECK(errno == EINVAL && recompiled->compiled == NULL,
              "failed recompile leaves no success-shaped compiled cache");
        FieldSchema invalid_fs = {0}; invalid_fs.ts = ts;
        int invalid_matched = 99;
        CHECK(criteria_match_tree_checked(record, sizeof(record), recompiled,
              &invalid_fs, 0, &invalid_matched) == -1 && invalid_matched == 0,
              "failed recompile cannot silently become a false condition");
    }
    free_criteria_tree(recompiled);
    strcpy(raw.field,"n0+n1");raw.op=OP_EQUAL;strcpy(raw.value,"11");
    CHECK(matches(record,sizeof(record),&raw,ts,0)==1,"checked legacy composite preserves display concatenation");
    uint8_t null_record[18]={1,1};memcpy(null_record+2,record,sizeof(record));strcpy(raw.value,"1");
    CHECK(matches(null_record,sizeof(null_record),&raw,ts,SREC_FLAG_NULLMAP)==1,"checked legacy composite skips NULL components");
    strcpy(raw.field,"n0");raw.op=OP_EQUAL;strcpy(raw.value,"1.0");
    CHECK(search_criterion_indexable(ts,&raw)==1,"representable legacy numeric keeps index eligibility");
    strcpy(raw.value,"1.04");CHECK(search_criterion_indexable(ts,&raw)==0,"nonrepresentable legacy numeric exact-scan fallback");
    raw.op=OP_BETWEEN;strcpy(raw.value,"0.0");strcpy(raw.value2,"1.04");
    CHECK(search_criterion_indexable(ts,&raw)==0,"second nonrepresentable bound rejects index shortcut");
#endif
    char *needle=malloc(70001);memset(needle,'a',70000);needle[70000]=0;
    size_t jcap=70100;char *json=malloc(jcap);snprintf(json,jcap,"{\"field\":\"v\",\"op\":\"eq\",\"value\":\"%s\"}",needle);
    const char *err=NULL;CriteriaNode *tree=parse_criteria_tree(json,&err);
    CHECK(tree!=NULL,"long scalar criterion parses");
#ifdef R2_BASE
    CHECK(tree&&strlen(tree->leaf.value)==70000,"scalar operand retains all 70000 bytes");
#else
    CHECK(tree&&search_criterion_value_len(&tree->leaf)==70000,"scalar operand retains all 70000 bytes");
    SearchCriterion cloned={0};CHECK(tree&&!search_criterion_clone(&cloned,&tree->leaf),"deep-clone long operand");
    char *array_json=malloc(70110);snprintf(array_json,70110,"[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"%s\"}]",needle);
    SearchCriterion *flat=NULL;int flatn=0;
    CHECK(!parse_criteria_json(array_json,&flat,&flatn)&&flatn==1&&search_criterion_value_len(&flat[0])==70000,"flat array parser keeps complete long operand");
    free_criteria(flat,flatn);free(array_json);
    char *bounds=malloc(140300);snprintf(bounds,140300,"[{\"field\":\"v\",\"op\":\"gte\",\"value\":\"%s\"},{\"field\":\"v\",\"op\":\"lt\",\"value\":\"%s\"}]",needle,needle);
    CriteriaNode *merged=parse_criteria_tree(bounds,&err);
    CHECK(merged&&merged->n_children==1&&merged->children[0]->leaf.op==OP_BETWEEN&&search_criterion_value_len(&merged->children[0]->leaf)==70000&&search_criterion_value2_len(&merged->children[0]->leaf)==70000,"coalescer owns both complete long bound operands");
    free_criteria_tree(merged);free(bounds);
    free_criteria_tree(tree);tree=NULL;
    CHECK(search_criterion_value_len(&cloned)==70000&&search_criterion_value(&cloned)[69999]=='a',"clone outlives parsed owner");
    search_criterion_clear(&cloned);
    const char *types[]={"numeric:26,10","varchar","int","numeric:4,2"};
    ts->nfields=4;
    for(int i=0;i<4;i++){memset(&ts->fields[i],0,sizeof(ts->fields[i]));ts->fields[i].type=(i==0||i==3)?FT_NUMERIC:i==1?FT_VARCHAR:FT_INT;snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;if(i==2)ts->fields[i].size=4;}
    CHECK(!typed_schema_bind_packed(ts,types,4,NULL,0),"bind isolated packed schema");
    char *rowjson=malloc(70200);snprintf(rowjson,70200,"{\"f0\":\"1.0000000000\",\"f1\":\"%s\",\"f2\":7,\"f3\":\"1.00\"}",needle);
    uint8_t *packed=NULL;size_t pn=0;
    CHECK(!typed_encode_alloc(ts,rowjson,1,NULL,NULL,&packed,&pn,NULL,0),"encode >65535 packed varchar");
    uint8_t literal_key[544],stored_key[544];size_t literal_len=0,stored_len=0;
    CHECK(encode_criterion_value_checked(ts,&ts->fields[0],"1.0000000000",12,literal_key,sizeof(literal_key),&literal_len)==1&&
        typed_index_field_key_into(ts,0,packed,pn,PKR_FLAGS,stored_key,sizeof(stored_key),&stored_len)==1&&
        stored_len==literal_len+1&&stored_key[0]==0&&!memcmp(stored_key+1,literal_key,literal_len),
        "packed literal producer matches stored canonical numeric payload without double tier");
    uint8_t tiny_key[2]={0x5a,0x5a};size_t tiny_len=99;errno=0;
    CHECK(encode_criterion_value_checked(ts,&ts->fields[0],"1.0",3,tiny_key,sizeof(tiny_key),&tiny_len)==-1&&
        errno==ENOBUFS&&tiny_len==0&&tiny_key[0]==0x5a&&tiny_key[1]==0x5a,
        "literal producer rejects actual destination cap before any write");
    raw=(SearchCriterion){0};strcpy(raw.field,"f1");raw.op=OP_EQUAL;
    CHECK(!search_criterion_set(&raw,0,needle,70000),"prepare owned long operand");
    CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==1,"packed varchar matches full 70000-byte span");
    raw.op=OP_ENDS_WITH;CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==1,"packed varchar ends-with handles long needle");
    search_criterion_clear(&raw);
    raw=(SearchCriterion){0};strcpy(raw.field,"f1");raw.op=OP_REGEX;strcpy(raw.value,".*a$");
    CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==1,"complex regex without literal anchors keeps regexec semantics");
    raw=(SearchCriterion){0};strcpy(raw.field,"f0");raw.op=OP_EQ_FIELD;strcpy(raw.value,"f3");
    CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==1,"packed mixed-scale decimal field equality");
    raw.op=OP_EQUAL;strcpy(raw.value,"1.00000000001");
    CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==0,"packed exact literal beyond declared scale");
    raw=(SearchCriterion){0};strcpy(raw.field,"f2");raw.op=OP_EQUAL;strcpy(raw.value,"7");
    CHECK(matches(packed,pn,&raw,ts,PKR_FLAGS)==1,"packed fixed scalar uses shared pointer matcher");
    FieldSchema fs={0};fs.ts=ts;CompiledCriterion cc={0};int matched=99;
    CHECK(!compile_one_checked(&cc,&raw,ts,NULL,0),"compile fixed scalar checked");
    CHECK(match_typed_checked(packed,1,&cc,&fs,PKR_FLAGS,&matched)==-1&&matched==0&&errno==EILSEQ,"corrupt OLD row reports status and resets matched");
    CompiledCriterion *heap=malloc(sizeof(cc));*heap=cc;free_compiled_criteria(heap,1);
    strcpy(raw.value,"7junk");CHECK(compile_one_checked(&cc,&raw,ts,NULL,0)==-1,"checked compilation preserves scalar grammar rejection");
    predicate_budget_checks(ts,packed,pn,needle);
    free(packed);free(rowjson);
#endif
    free_criteria_tree(tree);free(json);free(needle);free(ts);
    return t_ctx->failed?1:0;
}

TEST_REGISTER("test-r2-predicate-verification",test_r2_predicate_verification_run)
