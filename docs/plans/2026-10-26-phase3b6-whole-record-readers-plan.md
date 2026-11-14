# Phase 3B.6 — schema-aware whole-record JSON readers

## Primary goal

Return complete, exact financial records as JSON without silently losing fields,
truncating decimals or returning a successful partial record after allocation
failure, while existing applications remain on the legacy format.

**Status:** Revision 2, independently reviewed and repaired; awaiting approval.
Not implemented in production.
Planner: GPT 6.1 Sol. Intended executor: GLM 5.3, per the user's assignment.
Base: `35e49ab8cd7a7efe7350562e34eaeac62ea4afae` on
`fix/phase1-safe-scalar-conversions` (reviewed Phase 3B.5).
All code below is complete and normal-compiled/tested only in scratch.

## Revision 2 — independent review fixes

- Spec P2: disclose the unchanged legacy decoder's conservative response capacity
  and simultaneous field workspace, separately from exact packed JSON sizing.
  The implementation stays within the approved proposed boundary; no unplanned
  legacy sizing rewrite is hidden in the correction.
- Spec P3: reconcile driver stage hashes with start/end protected-file checks;
  repeated hash-only validation is allowed and the final hash command is explicit.
- Fixture safety: stop and free prepared input/schema if mkdtemp fails, before
  storage initialization; never open or recursively remove a failed template.
- Probe hardening: require every allocation sweep to be nonempty and assert
  normal packed GET output exactly before testing its newline boundary.

## Proposal and explicit tradeoff

Keep this child to the stable whole-record allocating/FILE APIs and their GET
caller. Dispatch packed decoding by schema to the existing exact renderer;
preserve the legacy allocating decoder's semantics. Render the entire JSON
string before writing to FILE so conversion/allocation failure cannot emit a
success prefix. Check writes and the GET newline; never append a replacement
error object after a FILE failure has already accepted a prefix.

**Approval of this plan accepts an owned whole-response buffer on legacy and
private packed GET.** This replaces the old direct streaming loop rather than
retaining a second independent renderer. Packed output allocation is exact JSON
bytes plus NUL (with independently bounded decimal workspaces). Legacy output
allocation remains conservative: est =4 + sum(strlen(name) + need +4) over
nonremoved fields, where need is5 for NULL, 6*n+3 for varchar, 6*n+4 for enum,
and512 for other scalars. n is actual/clamped varchar content or selected enum
text length. A heap field workspace of need bytes may coexist with est during
rendering when need >512. Legacy peak therefore includes raw record + est +
largest such workspace, not just the final JSON bytes; an ordinary string can
reserve about6x its length and a scalar512 bytes. Workspaces are freed before
FILE output. Tiny values never allocate the declared schema ceiling, but the
legacy estimate is not an exact-size allocation. Escaping one4MiB packed
all-control varchar can produce about24MiB JSON. This child preserves the
legacy allocator rather than adding an unrelated exact-sizing rewrite.
No output cap/QUERY_BUFFER_MB policy or performance improvement is invented
here. A lower-memory verified streaming design would require a separately
reviewed plan; there is no silent fallback or mid-execution redesign.

Only full-record APIs are bridged. Field projections, CSV/rows/composites,
criteria/CAS, sorting, joins, indexes, aggregates, maintenance, migration and
public activation remain separate work. `typed_record_view` stays legacy-only.
`typed_get_field_str` must not be casually routed to a binary text API: its
callers use strlen and confuse some failures with NULL. This child is not 3B
closure or public packed-read acceptance.

## Task 0 — documentation/API discovery and checkpoint

**How this serves the primary goal:** fixes the real decoder/GET contracts before
making any consumer accept a variable-length decimal or varchar payload.

Read AGENTS.md/CORE-PROCESS.md, the Phase 3 work index, parent recovery roadmap,
reviewed 3B.5 plan/evidence/raw-diff review, typed-records.md, typed-row-adapter.md,
packed-record-codec.md, query-protocol/overview.md and find.md. Never read the
protected certification file. Discovery used independent read-only fact-gathering
agents; the planner checked the signatures, named bodies and caller searches.

Allowed APIs (existing at this checkpoint):
- `typed_decode(ts, data, int data_len, hdr2)` returns owned NUL-terminated JSON
  or NULL. Its legacy implementation already checks output allocations and
  uses whole-field zero substitution for an incomplete legacy field.
- `typed_decode_stream(ts, data, int data_len, FILE *out, hdr2)` returns 0/-1;
  one production caller is cmd_get. It borrows FILE; ownership stays with caller.
- `typed_packed_decode(ts, value, size_t len, hdr2, size_t budget)` validates
  one schema-bound packed view, measures exact JSON including NUL, and returns
  one owned output. SIZE_MAX retains the allocating API's existing uncapped
  output contract; no new format or decimal arithmetic is introduced.
- `typed_row_open`, `typed_schema_bind_packed`, `typed_prepared_zero/set/alloc`,
  `slotcask_get_ex`, `slotcask_upsert_with_hooks`, `pkr_slot_bound` are used by
  private tests. Binder has no loader/create caller and gains none here.
- `fwrite`/`ferror`/`fputc` check bytes accepted by FILE. Flush/close belong to
  the owner; this is not a promise that buffered bytes already reached a socket.

Copy patterns from test_single_write_integration.c:w_legacy_render (long enum,
legacy varchar, private schema), test_typed_row_adapter.c:review_regressions
(binary JSON and exact decimals) and test_packed_record_storage.c (actual bytes,
private physical store/reopen). Ordinary TEST_REGISTER/assert headers are used.
No new global switch, registry override or test-only production seam is added.

### Consumer census at fixed HEAD

| API/surface | Actual current consumers and impact |
|---|---|
| typed_decode | query_find.c:decode_value and v2_fetch_cb (two output branches); storage.c:single_write_decode, cmd_update_v2 dry response, cmd_delete_v2 response paths and multi_get_fetch_cb |
| typed_decode_stream | storage.c:cmd_get only production caller; declaration in types.h |
| packed full renderer | typed_row.c:typed_packed_decode; current external consumer single_write_decode; shared typed_decode becomes the new bridge |
| decode_value | shared query/find/join emission; decoding a packed full row does not make their matcher, planner or output-error handling packed-ready |
| Unconverted field strings | typed_get_field_str in query_find.c (decode_field, json_typed_row_cell, CSV), query.c and query_join.c; their length/error contracts stay unchanged |
| Raw layout consumers | typed_record_view callers in query/plan/join/aggregate/index/schema/storage/rebuild still assume fixed offsets; flag guard must not be widened |
| GET dispatch | server.c calls cmd_get; its registry reference and fetched bytes keep existing ownership; error output changes only for actual conversion/ENOMEM/I/O failure |
| Build | same existing five config/typed_row source lists; only add the new regression case once to runner |
| Public loader/metadata | no binder call, no schema version change, migration, manifest write, activation or default-format switch |

Known later error contracts remain visible: multi_get_fetch_cb currently replaces
decode failure with null; find/fetch/joins have their own missing/corrupt-output
policies; projection/rows can turn ENOMEM into null; composites ignore some NULL
returns. None is claimed solved by enabling the full allocating decoder.
Their caller integration is required before public packed activation.

Root causes being fixed:
1. Both full-record APIs call schema-less typed_record_view, which rightly rejects
   PKR_FLAGS. That helper cannot validate directories and must remain legacy.
   Worse, a packed schema with legacy flags can interpret directory offsets as
   fixed scalar fields. Dispatch by explicit schema into typed_packed_decode
   closes both schema/generation mismatch directions before field rendering.
2. The stream loop uses a fixed 512-byte enum buffer and ignores render failure.
   For varchar malloc failure it falls back to that small buffer. Thus it can
   return success with omitted/truncated fields after already emitting `{`.
   Reuse the checked allocating decoder and write only after it succeeds.
3. The stream loop ignores FILE errors and GET ignores newline failure. Once
   writes are checked, GET must not append an error object after a partially
   accepted response. Before-output ENOMEM is instead a single explicit error.

Execution constraints:
- Await explicit approval; verify exact HEAD/branch/plan SHA before starting.
  Do not branch/reset/stash or perform Git writes. Keep unrelated planning files.
- Verify protected docs/plans/2026-10-19-durability-certification-rig.md by
  hash at start and again explicitly at the end. The driver also revalidates
  its hash before every stage/check-only call; these repeated hashes are allowed.
  Expected fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
  Access stays hash-only: never read contents, edit/move/stage/copy/incorporate/execute it.
- Bootstrap frozen blocks into fresh /tmp. Driver --check-only verifies all
  anchors and file/block hashes before mutation. Apply stages in order only.
  A mismatch, unexpected count/warning/failure or uncovered decision means
  append PLAN_NOTES.md and halt the entire run. Planner/human must amend it
  before resumption; never guess, improvise, weaken tests or rerun-until-green.
- All shell commands rtk-prefixed. Build `SKIP_TESTS=1 ./build.sh`; tests via
  build/bin/shard-db-test. Use socket-capable permissions if needed; no skipped
  daemon cases. No benchmarks or sanitizers; branch-final safety gates stay
  after all fixes immediately before an authorized push/PR.
- Initial RED is mandatory before source fixes. Later scratch-only reverse/
  reapply proof additionally satisfies CORE-PROCESS; never revert checkout
  sources or binaries. No new allocator seam or checked-in scratch tooling.
- Stop unstaged/uncommitted for executed plan-aware and independent blind
  raw-diff reviews. No add/commit/push/PR/merge or later-phase implementation.

## Task 1 — regression first against unchanged readers

**How this serves the primary goal:** proves missing exact reads and silent
partial-output bugs on the committed baseline before adding a new read path.

Apply stage1: full new C test plus one build registration, no source changes.
Normal build must be zero-warning, exit0. Focused RED must be **124 passed /
38 failed**, exit38. Baseline rejection passes are not evidence of implemented
packed behavior. The invalid descriptor/flag/mismatch tests deliberately keep
separate invalid-argument EINVAL versus malformed-record EILSEQ expectations.

Run checks --mode red before stage2. It compiles actual config/row/packed/decimal
modules with local malloc/calloc/realloc/free substitutions. The probe extracts
the actual cmd_get function body from storage.c, with local read/schema/registry
mocks; source logic is not reimplemented. No production override is added.
Require probe exit1 and exactly **16 failed gates** in the legacy omission/OOM,
packed-read, GET output/mismatch and maximum-schema classes; setup failures
exit2 and are never accepted as RED. The runner
handles expected exit1 internally. Raw diagnostics must be captured.

## Task 2 — full JSON bridge, checked FILE output and GET handling

**How this serves the primary goal:** gets complete exact JSON from validated
records and stops allocation failures from masquerading as successful records.

Apply stage2's five complete hunks: config typed_decode entry dispatch, complete
replacement typed_decode_stream, types.h API comment, GET output/error tail,
and typed_row.h supported-boundary comment. Preserve every other legacy decoder
line. No signature or source-list changes; no alteration of typed_record_view,
typed_get_field_str, metadata or field arithmetic.

Invalid API schema reports EINVAL; negative data_len and malformed records
report EILSEQ. Packed typed_row_open's NULL-input EINVAL is propagated, not
rewritten as corruption. Owned output is free-compatible. FILE adapter failure
before fwrite emits no bytes. After actual FILE failure it may have emitted a
prefix; GET suppresses replacement errors when ferror(out) is set and returns
failure for a failed newline too. Neither API flushes/closes the borrowed FILE.
GET checks vlen <= INT_MAX before passing the existing int decoder signature.

Normal rebuild must be zero-warning. Focused GREEN **167/0**, exit0. The new
case's maximum-size checks assert complete digits/strings, not throughput.
Run checks --mode green: **22 allocation injections** (legacy direct3/GET3,
packed direct8/GET8), all106 gates pass, selected buffers live0 and a released
registry ref on every mocked GET path. Maximum-schema row actual2085 and largest
selected allocation2966 (<4096 despite >1 GiB declared ceiling).
Every successful path must also assert its allocation sweep is nonempty;
normal packed GET must match its complete JSON body including newline, before
using that body's byte length to inject newline failure.
Output-failure fixtures cover readonly FILE, a short/partial accepted write,
and newline failure; no replacement error after an I/O prefix. Cookie writer
uses fopencookie on Linux and funopen on Apple; /dev/null is POSIX. Only the
Linux x86_64 probe is planning-time executed here.

Run checks --mode regression-proof after GREEN. It copies fixed config.c and
storage.c to /tmp, reverses exact source hunks, verifies byte-identical committed
baseline hashes, compiles those actual copies and requires the same16 failing
probe gates. Reapply hunks, verify fixed hashes, recompile and require all106
gates pass. Both outputs go into evidence. Checkout sources/build products
remain untouched. Header comment edits have no executable role in reversal.

## Task 3 — documentation, full verification and stop

**How this serves the primary goal:** records the supported read boundary and
checks that existing applications retain their legacy behavior.

Apply stage3: full new whole-record-readers.md and exact link/clarification hunks
in typed-row-adapter.md and typed-records.md. Document the memory tradeoff,
error/FILE ownership, actual-size bounds and the unconverted field/query routes.

Run a fresh normal full suite after docs: **20045 passed / 0 failed across524
cases**, exit0. Repeat green probes, all final owned hashes, git diff --check,
and census that loader/create-object still have no typed_schema_bind_packed.
Finally hash the protected file explicitly and compare the exact expected hash.
No TODO/FIXME/debug/log/probe/cache artifacts may enter the diff. Nine owned
files must match checkpoint3 exactly; curated evidence is the only additional
execution deliverable at docs/plans/2026-10-26-phase3b6-whole-reader-evidence.md.

Record actual start/end identity, compiler, hashes, commands, RED/GREEN/probe/
reversion/full outputs and remaining scope; raw logs/sources/objects stay /tmp.
Stop for raw-diff reviews. This is not the approval to commit or to implement
projections, activate packed storage or start the protected certification rig.

## Planning evidence and limits

Scratch archive of exact HEAD: /tmp/p3b6-plan-a4g9aeu_; protected untracked file
was never copied/read. Fresh baseline final fixture replay:
/tmp/p3b6-stage1-8v0jvemc. All compilation ordinary, no sanitizers/benchmarks.
- Final baseline normal build zero-warning; focused RED124/38, exit38.
- Final scratch build zero-warning; focused167/0 and full20045/0 across524.
- Probe green: all106 gates, allocation counts3+3+8+8; maximum2085/2966/live0.
- Exact extracted checks scripts passed baseline RED16 failed gates, GREEN106
  pass gates and scratch-only reversal16 fail then reapply106 pass. Root driver
  stage1 --check-only passed (two files, no writes). All22 manifest block hashes
  and all nine owned-file final hashes were checked; all Python blocks parse.
  Final reviewed raw logs stay /tmp/p3b6-r2-stage1-{build,red}.log,
  /tmp/p3b6-r2-{build,green,full}.log, /tmp/p3b6-r2-probe-{red,green}.log and
  /tmp/p3b6-r2-proof.log. Earlier /tmp/p3b6-final-* logs predate fixture/probe
  hardening and are historical, not the final106-gate evidence.
- Initial fixture check caught an unset fixed-int size and a wrong expectation
  that an incomplete unflagged int was omitted; actual legacy behavior renders
  zero. The fixture was corrected before the final baseline run. A subsequent
  check caught NULL input incorrectly classified as corruption; existing
  typed_row_open specifies EINVAL. Correcting this expectation produced genuine
  additional baseline failures; production code was not weakened to fit tests.
- A private direct decoder/physical-storage test does not prove public packed
  GET or the complete find/fetch/multi-get pipeline. The standalone GET body
  is actual source but dependencies are mocked; metadata/network/registry
  concurrency acceptance remains later work.
- The FILE guarantee is accepted bytes/error detection, not flush completion,
  all-or-nothing network delivery or crash durability. No speedup is claimed.

## Following work

After execution/reviews/commit, prepare a length-aware field/projection/rows/CSV
child. Explicitly fix absence versus corruption/OOM propagation through its
callers instead of silently teaching strlen-based APIs to truncate binary
varchar. Matching/sorting/joins/index/aggregate and maintenance/delete consumers
retain their owning child/roadmap phases; 3C owns durable metadata/publication,
Phases4–6 exact write/query/index/aggregate completion, Phase7 other type contracts,
Phase8 conversion/migration, Phase9 application acceptance and activation.
Protected durability certification remains separate after the roadmap lands.

## Appendix A — bootstrap and exact invocation order

Execute bootstrap below with rtk proxy python3 (a temporary script is acceptable).
Use its printed folder for /tmp/p3b6-exec-XXXX. Capture unfiltered stdout/stderr
and exit status; RED exit38 is required, not a reason to run stage2 prematurely.
Checks red/regression-proof handle their expected probe exits internally.

<!-- block:bootstrap -->
```python
import pathlib,re,tempfile
plan=pathlib.Path('docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md')
folder=pathlib.Path(tempfile.mkdtemp(prefix='p3b6-exec-'))
for m in re.finditer(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text()):
    (folder/(m.group(1)+'.txt')).write_text(m.group(2)+'\n')
for name in ['driver','checks']:
    (folder/(name+'.py')).write_bytes((folder/(name+'.txt')).read_bytes())
print(folder)
```

~~~sh
rtk proxy python3 /tmp/p3b6-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --stage 1 --check-only
rtk proxy python3 /tmp/p3b6-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-whole-record-readers
rtk proxy python3 /tmp/p3b6-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --mode red
rtk proxy python3 /tmp/p3b6-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-whole-record-readers
rtk proxy python3 /tmp/p3b6-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --mode green
rtk proxy python3 /tmp/p3b6-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --mode regression-proof
rtk proxy python3 /tmp/p3b6-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 /tmp/p3b6-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md --mode green
rtk git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
~~~

## Appendix B — complete anchored edits and checkpoints

All named blocks are frozen and manifest-hashed. The driver preflights entire
stages and file hashes before writing; each quoted anchor must match once.
New files must be absent. No implementation is left as prose or ad-hoc edits.

### reader-test

<!-- block:reader-test -->
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static TypedSchema *r_schema(int maximum){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"private reader schema");if(!ts)return NULL;
    ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"numeric":i==3?"int":"varchar";
        ts->fields[i].type=i<2&&!maximum?FT_NUMERIC:i==3&&!maximum?FT_INT:FT_VARCHAR;
        if(ts->fields[i].type==FT_INT)parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind private schema");if(rc){free(ts);return NULL;}return ts;
}
static int r_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode independent input");return rc;
}
static char *r_stream(const TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr,int *rc){
    char *out=NULL;size_t used=0;FILE *fp=open_memstream(&out,&used);
    ASSERT_NOT_NULL(fp,"capture stream");if(!fp){*rc=-1;return NULL;}
    *rc=typed_decode_stream(ts,bytes,n,fp,hdr);int saved=errno;
    ASSERT_EQ_INT(fclose(fp),0,"close capture");errno=saved;return out;
}
static void r_equal(TypedSchema *ts,const uint8_t *bytes,size_t n,uint8_t hdr,const char *expected){
    char *out=typed_decode(ts,bytes,(int)n,hdr);ASSERT_NOT_NULL(out,"allocated JSON available");
    ASSERT_TRUE(out&&strcmp(out,expected)==0,"allocated JSON exact without truncation");free(out);
    int rc=0;out=r_stream(ts,bytes,(int)n,hdr,&rc);ASSERT_EQ_INT(rc,0,"stream success");
    ASSERT_TRUE(out&&strcmp(out,expected)==0,"stream JSON exact without truncation");free(out);
}
static void r_failure(TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr,int error){
    errno=0;char *out=typed_decode(ts,bytes,n,hdr);ASSERT_TRUE(out==NULL,"bad record allocated decode rejects");
    ASSERT_EQ_INT(errno,error,"bad record errno");free(out);
    out=NULL;size_t used=0;FILE *fp=open_memstream(&out,&used);ASSERT_NOT_NULL(fp,"capture invalid stream");if(!fp)return;
    fputs("PREFIX",fp);errno=0;int rc=typed_decode_stream(ts,bytes,n,fp,hdr),saved=errno;
    ASSERT_EQ_INT(rc,-1,"bad stream rejects");ASSERT_EQ_INT(saved,error,"bad stream errno");
    ASSERT_EQ_INT(fclose(fp),0,"close invalid capture");ASSERT_EQ_STR(out,"PREFIX","failure emits no JSON bytes");free(out);
}
static void r_invalid(TypedSchema *ts,const uint8_t *bytes,int n,uint8_t hdr){r_failure(ts,bytes,n,hdr,EILSEQ);}
static void r_packed(void){
    TypedSchema *ts=r_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *input="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"0.00000000000000000000000001\",\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":0,\"f4\":null,\"f5\":\"\"}";
    const char *expected="{\"f0\":1234567890123456.1234567890,\"f1\":0.00000000000000000000000001,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":0,\"f4\":null,\"f5\":\"\"}";
    if(r_encode(ts,input,&bytes,&n)){free(ts);return;}
    r_equal(ts,bytes,n,PKR_FLAGS,expected);
    r_invalid(ts,bytes,(int)n,0);r_invalid(ts,bytes,(int)n,SREC_FLAG_NULLMAP);
    r_invalid(ts,bytes,(int)n,SREC_FLAG_PACKED);r_invalid(ts,bytes,(int)n,0x80);
    r_invalid(ts,bytes,(int)n-1,PKR_FLAGS);r_failure(ts,NULL,0,PKR_FLAGS,EINVAL);r_invalid(ts,bytes,-1,PKR_FLAGS);
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"corrupt descriptor copy");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);
        r_invalid(ts,bad,(int)n,PKR_FLAGS);free(bad);}
    FILE *ro=fopen("/dev/null","r");ASSERT_NOT_NULL(ro,"read-only output stream");
    if(ro){setvbuf(ro,NULL,_IONBF,0);errno=0;
        ASSERT_EQ_INT(typed_decode_stream(ts,bytes,(int)n,ro,PKR_FLAGS),-1,"write failure reported");
        ASSERT_TRUE(ferror(ro),"write failure marks FILE");fclose(ro);}
    for(int i=0;i<ts->nfields;i++)ts->fields[i].removed=1;
    r_equal(ts,bytes,n,PKR_FLAGS,"{}");free(bytes);free(ts);
}
static void r_legacy(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy schema");if(!ts)return;
    ts->typed=1;ts->nfields=3;ts->nullmap_prefix=2;ts->total_size=1009;
    for(int i=0;i<3;i++){snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    ts->fields[0].type=FT_INT;ts->fields[0].size=4;
    ts->fields[1].type=FT_VARCHAR;ts->fields[1].offset=4;ts->fields[1].size=1002;
    TypedField *e=&ts->fields[2];e->type=FT_ENUM;e->offset=1006;e->size=1;e->enum_width=1;e->n_enum_values=1;
    e->enum_values=calloc(1,sizeof(char*));ASSERT_NOT_NULL(e->enum_values,"enum catalog");if(!e->enum_values){free(ts);return;}
    e->enum_values[0]=malloc(1001);ASSERT_NOT_NULL(e->enum_values[0],"long enum");if(!e->enum_values[0]){free(e->enum_values);free(ts);return;}
    memset(e->enum_values[0],'z',1000);e->enum_values[0][1000]=0;
    uint8_t bytes[1009]={1,0};bytes[6]=3;bytes[7]=132;memset(bytes+8,'v',900);
    char expected[2000];memcpy(expected,"{\"f0\":0,\"f1\":\"",14);memset(expected+14,'v',900);
    size_t at=914;memcpy(expected+at,"\",\"f2\":\"",8);at+=8;memset(expected+at,'z',1000);at+=1000;
    memcpy(expected+at,"\"}",3);
    r_equal(ts,bytes,sizeof(bytes),SREC_FLAG_NULLMAP,expected);
    r_invalid(ts,bytes,sizeof(bytes),PKR_FLAGS);
    free_enum_values(e);ts->nfields=1;ts->total_size=6;
    const uint8_t short_old[]={1};r_equal(ts,short_old,sizeof(short_old),0,"{\"f0\":0}");
    const uint8_t short_flagged[]={1,0,9};r_equal(ts,short_flagged,sizeof(short_flagged),SREC_FLAG_NULLMAP,"{\"f0\":0}");
    const uint8_t null[]={1,1};r_equal(ts,null,sizeof(null),SREC_FLAG_NULLMAP,"{\"f0\":null}");
    ts->fields[0].size=70000;r_invalid(ts,short_flagged,sizeof(short_flagged),SREC_FLAG_NULLMAP);free(ts);
}
static int r_rm(const char *p,const struct stat *s,int kind,struct FTW *f){(void)s;(void)kind;(void)f;return remove(p);}
static void r_physical(void){
    TypedSchema *ts=r_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *input="{\"f0\":1.25,\"f2\":\"on disk\"}";
    if(r_encode(ts,input,&bytes,&n)){free(ts);return;}
    char path[]="/tmp/whole-read-XXXXXX";char *created=mkdtemp(path);
    ASSERT_NOT_NULL(created,"private physical directory");
    if(!created){free(bytes);free(ts);return;}
    size_t bound=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&bound),DEC_OK,"physical bound");
    slotcask_init(32,32);SlotcaskDb db;int rc=slotcask_open(&db,path,1,1,(int)bound);ASSERT_EQ_INT(rc,0,"open physical object");
    if(!rc){SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db,-1,"a",1,bytes,n,&opts,NULL),0,"store packed record");
        slotcask_close(&db);rc=slotcask_open(&db,path,1,1,(int)bound);ASSERT_EQ_INT(rc,0,"reopen physical object");
        if(!rc){void *raw=NULL;size_t len=0;uint8_t hdr=0;
            ASSERT_EQ_INT(slotcask_get_ex(&db,"a",1,&raw,&len,&hdr),0,"fetch actual physical bytes");
            ASSERT_EQ_INT(len,n,"actual stored length");ASSERT_EQ_INT(hdr,PKR_FLAGS,"stored generation");
            if(raw)r_equal(ts,raw,len,hdr,"{\"f0\":1.2500000000,\"f1\":null,\"f2\":\"on disk\",\"f3\":null,\"f4\":null,\"f5\":null}");
            free(raw);slotcask_close(&db);}}
    slotcask_shutdown();ASSERT_EQ_INT(nftw(path,r_rm,16,FTW_DEPTH|FTW_PHYS),0,"remove private object");free(bytes);free(ts);
}
static void r_limits(void){
    TypedSchema *ts=r_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!r_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){
        ASSERT_TRUE(ts->total_size>1000000000,"maximum schema bound");
        ASSERT_TRUE(n<4096,"tiny actual record");char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);
        ASSERT_NOT_NULL(out,"maximum schema decode");
        if(out){ASSERT_CONTAINS(out,"\"f127\":\"tiny\"","selected tiny content");ASSERT_TRUE(strlen(out)<4096,"actual output length");}
        int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"maximum schema stream");
        ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"maximum schema readers agree");free(out);free(stream);free(bytes);}
    free(ts);ts=r_schema(0);if(!ts)return;
    size_t length=4u*1024*1024;char *content=malloc(length);ASSERT_NOT_NULL(content,"max varchar content");
    if(content){memset(content,'x',length);TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare large row");
        TypedInput input={content,length,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"assign maximum varchar");
        if(!typed_prepared_alloc(&p,&bytes,&n,NULL,0)){
            char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);ASSERT_NOT_NULL(out,"maximum varchar decode");
            if(out){ASSERT_TRUE(strlen(out)>length&&strlen(out)<length+128,"complete maximum varchar JSON");
                ASSERT_TRUE(strstr(out,"\"f2\":\"xxx")!=NULL,"maximum prefix");}
            int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"large stream succeeds");
            ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"large readers byte-identical");free(out);free(stream);free(bytes);}
        else ASSERT_TRUE(0,"allocate maximum value");
        typed_prepared_free(&p);free(content);}
    /* Full PostgreSQL-sized decimal, without a legacy integer carrier. */
    length=131072+1+16383;content=malloc(length+1);ASSERT_NOT_NULL(content,"maximum decimal text");
    if(content){memset(content,'1',length);content[131072]='.';content[length]=0;
        TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare huge decimal");
        TypedInput input={content,length,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"assign full decimal range");
        if(!typed_prepared_alloc(&p,&bytes,&n,NULL,0)){
            char *out=typed_decode(ts,bytes,(int)n,PKR_FLAGS);ASSERT_NOT_NULL(out,"maximum decimal decode");
            if(out)ASSERT_TRUE(strstr(out,content)!=NULL,"every decimal digit preserved");
            int rc=0;char *stream=r_stream(ts,bytes,(int)n,PKR_FLAGS,&rc);ASSERT_EQ_INT(rc,0,"huge decimal stream");
            ASSERT_TRUE(out&&stream&&!strcmp(out,stream),"huge decimal readers agree");free(out);free(stream);free(bytes);}
        else ASSERT_TRUE(0,"allocate huge decimal");
        typed_prepared_free(&p);free(content);}
    free(ts);
}
static int test_whole_record_readers_run(void){r_packed();r_legacy();r_physical();r_limits();return t_ctx->failed?1:0;}
TEST_REGISTER("test-whole-record-readers",test_whole_record_readers_run)
```

### build-register-old

<!-- block:build-register-old -->
```c
    src/test/cases/test_typed_bulk_update.c \
```

### build-register-new

<!-- block:build-register-new -->
```c
    src/test/cases/test_typed_bulk_update.c \
    src/test/cases/test_whole_record_readers.c \
```

### reader-edit-1-old

<!-- block:reader-edit-1-old -->
```c
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS)
        return NULL;
    const uint8_t *fbase = NULL, *bm = NULL;
```

### reader-edit-1-new

<!-- block:reader-edit-1-new -->
```c
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS) {
        errno = EINVAL;
        return NULL;
    }
    if (data_len < 0) { errno = EILSEQ; return NULL; }
    if (ts->packed.nfields)
        return typed_packed_decode(ts, data, (size_t)data_len, hdr2, SIZE_MAX);
    const uint8_t *fbase = NULL, *bm = NULL;
```

### reader-edit-2-old

<!-- block:reader-edit-2-old -->
```c
int typed_decode_stream(const TypedSchema *ts, const uint8_t *data,
                        int data_len, FILE *out, uint8_t hdr2) {
    if (!ts || !ts->typed || !out) return -1;
    const uint8_t *fbase = NULL; const uint8_t *bm = NULL;
    int bm_bytes = 0; size_t field_region_len = 0;
    if (data_len < 0 ||
        typed_record_view(data, (size_t)data_len, hdr2, &fbase, &bm,
                          &bm_bytes, &field_region_len) != 0) {
        errno = EILSEQ;   /* corrupt flagged record — fail, never emit */
        return -1;
    }
    int render_zeros = (hdr2 & SREC_FLAG_NULLMAP) ? 1 : 0;
    fputc('{', out);
    int first = 1;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;

        static const uint8_t zero_field[65537];
        const uint8_t *field_src = ((size_t)f->offset + (size_t)f->size <= field_region_len)
            ? fbase + f->offset
            : zero_field;

        if (typed_field_null(bm, bm_bytes, i)) {
            if (!first) fputc(',', out);
            fprintf(out, "\"%s\":null", f->name);
            first = 0;
            continue;
        }

        char vbuf_stack[512];
        char *vbuf = vbuf_stack;
        int vbufsz = (int)sizeof(vbuf_stack);
        if (f->type == FT_VARCHAR) {
            int need = 6 * (f->size > 2 ? f->size - 2 : 0) + 3;
            if (need > vbufsz) {
                vbuf = malloc((size_t)need);
                if (!vbuf) { vbuf = vbuf_stack; }
                else        { vbufsz = need; }
            }
        }
        int vlen = decode_field_to_buf(f, field_src, vbuf, vbufsz, render_zeros);
        if (vlen <= 0) { if (vbuf != vbuf_stack) free(vbuf); continue; }

        if (!first) fputc(',', out);
        fprintf(out, "\"%s\":%s", f->name, vbuf);
        first = 0;
        if (vbuf != vbuf_stack) free(vbuf);
    }
    fputc('}', out);
    return 0;
}
```

### reader-edit-2-new

<!-- block:reader-edit-2-new -->
```c
int typed_decode_stream(const TypedSchema *ts, const uint8_t *data,
                        int data_len, FILE *out, uint8_t hdr2) {
    if (!out) { errno = EINVAL; return -1; }
    if (ferror(out)) { errno = EIO; return -1; }
    /* Complete validation/rendering precedes output. In particular ENOMEM
       never falls back to a smaller field buffer or drops an enum value. */
    char *json = typed_decode(ts, data, data_len, hdr2);
    if (!json) return -1;
    size_t length = strlen(json);
    errno = 0;
    size_t written = fwrite(json, 1, length, out);
    int saved = errno;
    free(json);
    if (written != length || ferror(out)) {
        errno = saved ? saved : EIO;
        return -1;
    }
    return 0;
}
```

### reader-edit-3-old

<!-- block:reader-edit-3-old -->
```c
/* Stream variant — writes the JSON object directly to `out` without
   allocating an intermediate string. Use on the GET hot path; saves one
   malloc + one memcpy per response vs. typed_decode + OUT("%s\n").
   Returns 0 on success, -1 (errno=EILSEQ) on a corrupt flagged record —
   callers must fail the request, never emit a partial object. */
```

### reader-edit-3-new

<!-- block:reader-edit-3-new -->
```c
/* Checked FILE variant. Builds the complete JSON string before
   emitting bytes, so validation/render/allocation failures leave out untouched.
   Adds one owned whole-response buffer on the GET path; no schema-max arena.
   Returns 0 after fwrite accepts all bytes, -1 with errno on failure. FILE I/O
   errors may have emitted a prefix: callers must not append a replacement JSON
   error when ferror(out) is set. Does not flush, close or own the FILE. */
```

### reader-edit-4-old

<!-- block:reader-edit-4-old -->
```c
    /* Fail closed on a structurally invalid flagged record (bad bm_nbytes,
       unknown hdr2 bits): typed_decode_stream emits nothing in that case —
       a bare newline would masquerade as an empty record dict. */
    if (typed_decode_stream(ts, (const uint8_t *)val, (uint32_t)vlen,
                            g_out ? g_out : stdout, get_hdr2) != 0) {
        OUT("{\"error\":\"corrupt record\"}\n");
        free(val);
        return 1;
    }
    fputc('\n', g_out ? g_out : stdout);
    free(val);
    return 0;
```

### reader-edit-4-new

<!-- block:reader-edit-4-new -->
```c
    FILE *out = g_out ? g_out : stdout;
    if (vlen > INT_MAX) {
        OUT("{\"error\":\"record exceeds decode limit\"}\n");
        free(val); errno = EOVERFLOW; return 1;
    }
    if (typed_decode_stream(ts, (const uint8_t *)val, (int)vlen,
                            out, get_hdr2) != 0) {
        int saved = errno;
        /* Rendering fails before output. An I/O failure may already have
           emitted bytes, so do not concatenate a replacement JSON error. */
        if (!ferror(out))
            OUT("{\"error\":\"%s\"}\n", saved == ENOMEM ? "out of memory" : "corrupt record");
        free(val); errno = saved; return 1;
    }
    int rc = fputc('\n', out) == EOF ? 1 : 0;
    free(val);
    return rc;
```

### reader-edit-5-old

<!-- block:reader-edit-5-old -->
```c
 * old typed_* and raw-offset APIs cannot interpret packed fields.
```

### reader-edit-5-new

<!-- block:reader-edit-5-new -->
```c
 * typed_decode/typed_decode_stream support whole packed records; legacy
 * field-string and raw-offset APIs still cannot interpret packed fields.
```

### reader-doc

<!-- block:reader-doc -->
```markdown
# Whole-record typed JSON readers (packed format inactive)

`typed_decode` dispatches by immutable schema binding. An explicitly bound
packed schema uses `typed_packed_decode`; legacy schemas keep existing checked
fixed-field rendering. The packed view validates exact generation, directory,
NULL entries and decimal payloads before rendering. Packed schema/legacy bytes
and legacy schema/packed bytes both reject. Transport flag recognition alone
never chooses a layout. Public loaders and creates still do not bind packed.

The shared full-record APIs now preserve exact numeric text, raw varchar bytes
through JSON escaping, explicit NULL, empty/zero values and removed fields.
Whole-field zero substitution for incomplete legacy fields is unchanged; this
is distinct from a writer's partial-prefix carry/pad policy. Bare varchar and
numeric limits remain 4 MiB and 131072 integer/16383 fractional digits.

`typed_decode_stream` is a checked FILE adapter over the allocating decoder.
It fully validates/renders before writing, so input, conversion and allocation
failures leave the output untouched. Returns 0 only if fwrite accepts every
byte and no FILE error is present. The caller owns flushing and closing. It
does not promise transactional I/O: a failing FILE can accept a prefix first.
GET therefore does not append a replacement JSON error when ferror is set,
and it checks its final newline. Pre-output ENOMEM becomes an explicit error.

This intentionally replaces the old direct streaming loop, whose failed large
field allocation fell back to 512 bytes and whose enum/render failures were
silently omitted. It adds an owned response buffer to GET, including legacy GET. Packed
rendering allocates exact escaped JSON bytes plus NUL, with separately bounded
decimal workspaces. Legacy rendering retains its conservative estimate: base4
plus, per nonremoved field, name bytes + need +4; need is5 for NULL, 6*n+3 for
varchar, 6*n+4 for enum, and512 otherwise. Here n is actual/clamped stored varchar
length or selected enum text length. During rendering a heap field workspace
(when need >512) can coexist with that response buffer; all such workspaces
are freed before output. Thus legacy peak includes raw record + estimated
response capacity + largest simultaneous heap field workspace, not merely
final JSON length plus NUL. An ordinary string may reserve about six times its
text length; scalar fields reserve512 bytes each. There is no declared-schema
ceiling allocation. One 4 MiB packed all-control varchar needs about24 MiB JSON;
other fields add their output. This deliberately does not change the legacy
allocator just to make its memory estimate exact.
No new response cap or QUERY_BUFFER_MB enforcement is claimed. The configured
query buffer cap and limits of other read routes remain later activation work.
No benchmark or throughput claim accompanies this correctness tradeoff.

This is a rendering bridge, not query integration. `typed_get_field_str`,
projections, composite/CSV/rows/join extraction, matching, sorting, indexes and
aggregates still have legacy assumptions and separate error/length contracts.
A binary varchar cannot be safely passed to their strlen-based paths. A decoded
full row does not prove the query's planner/matcher/index/emit-failure path is
packed-ready. Existing callers that substitute null on decode failure remain
explicit follow-up work; no public packed activation is authorized here.

Private C tests cover exact JSON and stream equality, NULL/empty/binary data,
all removed fields, both layout mismatches, invalid flags/descriptors/truncation,
legacy short tails/long enums, actual disk bytes and reopen, full decimal limits,
4 MiB values and a tiny row under a >1 GiB schema. Scratch-only probes inject
allocation and output failures in actual decoder code and the source-extracted
GET body. They mock schema/registry/read dependencies locally, never install a
production global switch or bind a public object. Public packed GET acceptance,
metadata reload and end-to-end network failure handling remain later gates.
```

### adapter-doc-old

<!-- block:adapter-doc-old -->
```markdown
packed schemas must only reach the new adapters and codec until each old
consumer has been converted. Passing one to a legacy raw-offset API is unsafe.
The ordinary loader does not call this binder.
```

### adapter-doc-new

<!-- block:adapter-doc-new -->
```markdown
packed schemas may reach the new adapters, codec and the integrated
[whole-record JSON readers](whole-record-readers.md). Field-string, projection
and raw-offset consumers still require their own conversion. Passing a bound
schema to a legacy raw-offset API is unsafe. The ordinary loader does not call
this binder.
```

### records-doc-old

<!-- block:records-doc-old -->
```markdown
Record payloads are encoded by `typed_encode()` (in `config.c`), which packs each field in declaration order. A reader uses `typed_decode()` — also cached per object via `g_typed_cache` — to unpack without re-reading the schema.
```

### records-doc-new

<!-- block:records-doc-new -->
```markdown
Record payloads are encoded by `typed_encode()` (in `config.c`), which packs each field in declaration order. A reader uses `typed_decode()` — also cached per object via `g_typed_cache` — to unpack without re-reading the schema.

The fixed-field description below is the currently public legacy format.
The private [whole-record JSON reader bridge](whole-record-readers.md) also
recognizes explicitly bound packed schemas; it does not activate their loaders,
projection/query consumers or migration. GET validates and builds its actual
JSON response before output, with checked allocation/write errors.
```

### driver

<!-- block:driver -->
```python
#!/usr/bin/env python3
import argparse,hashlib,json,pathlib,re,subprocess,sys

def digest(b):return hashlib.sha256(b).hexdigest()
def blocks(plan):
    return {m.group(1):m.group(2).encode()+b'\n' for m in re.finditer(
        r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text())}
def git(*args):return subprocess.check_output(['rtk','proxy','git',*args],text=True).strip()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--plan',type=pathlib.Path,required=True)
    ap.add_argument('--stage',type=int,choices=[1,2,3],required=True)
    ap.add_argument('--check-only',action='store_true');args=ap.parse_args()
    b=blocks(args.plan);m=json.loads(b['manifest'])
    for key,want in m['blocks'].items():
        if key not in b or digest(b[key])!=want:raise ValueError('changed code block: '+key)
    if git('branch','--show-current')!='fix/phase1-safe-scalar-conversions':raise ValueError('wrong branch')
    subprocess.run(['rtk','proxy','git','merge-base','--is-ancestor',m['ancestor'],'HEAD'],check=True)
    if git('rev-parse','HEAD')!=m['base_head']:raise ValueError('HEAD differs from reviewed 3B.5 commit')
    protected=pathlib.Path(m['protected'])
    if digest(protected.read_bytes())!=m['protected_sha']:raise ValueError('protected file hash mismatch')
    for path,want in m['parent_sources'].items():
        if not git('ls-files','--error-unmatch',path):raise ValueError('parent source is not committed: '+path)
        head=subprocess.check_output(['rtk','proxy','git','show','HEAD:'+path])
        if digest(head)!=want:raise ValueError('parent committed source differs: '+path)
    before=m['checkpoints'][str(args.stage-1)];after=m['checkpoints'][str(args.stage)]
    for path,want in before.items():
        p=pathlib.Path(path);actual=digest(p.read_bytes()) if p.exists() else None
        if actual!=want:raise ValueError('checkpoint mismatch: '+path)
    pending={}
    for e in m['edits'][str(args.stage)]:
        path=e['path'];p=pathlib.Path(path)
        current=pending.get(path,p.read_bytes() if p.exists() else None)
        replacement=b[e['new']]
        if e['old'] is None:
            if current is not None:raise ValueError('new file already exists: '+path)
            current=replacement
        else:
            anchor=b[e['old']]
            if current is None or current.count(anchor)!=1:raise ValueError('anchor count != 1: '+e['old'])
            current=current.replace(anchor,replacement)
        pending[path]=current
    for path,data in pending.items():
        if digest(data)!=after[path]:raise ValueError('computed output mismatch: '+path)
    if args.check_only:
        print('stage',args.stage,'preflight verified:',len(pending),'files; no writes');return
    for path,data in pending.items():
        p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
    for path,want in after.items():
        p=pathlib.Path(path);actual=digest(p.read_bytes()) if p.exists() else None
        if actual!=want:raise ValueError('post-write checkpoint mismatch: '+path)
    print('stage',args.stage,'verified:',len(pending),'files')
try:main()
except Exception as e:
    message='Phase 3B.6 execution halted: '+str(e)+'\nNo later task may run. Planner/human must review this note before resuming.\n'
    pathlib.Path('PLAN_NOTES.md').open('a').write(message)
    print(message,file=sys.stderr);sys.exit(1)
```

### checks

<!-- block:checks -->
```python
#!/usr/bin/env python3
import argparse,hashlib,json,pathlib,re,subprocess,tempfile

def extract(plan,key):
    m=re.search(r'(?ms)^<!-- block:'+re.escape(key)+r' -->\n`{3}[^\n]*\n(.*?)\n`{3}(?=\n|$)',plan.read_text())
    if not m:raise ValueError('missing block '+key)
    return m.group(1)+'\n'
def run(args,expected=0):
    print('+',' '.join(args),flush=True)
    p=subprocess.run(['rtk','proxy',*args],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    print(p.stdout,end='',flush=True)
    if p.returncode!=expected:raise RuntimeError('exit '+str(p.returncode)+' expected '+str(expected))
def check(work,probe,config,storage,expected):
    source=storage.read_text();start=source.index('int cmd_get(')
    end=source.index('\n/*',start);command=work/'cmd_get.inc';command.write_text(source[start:end])
    objects=[]
    for name in ['config','typed_row','packed_record','decnum']:
        obj=work/(name+'.o');objects.append(str(obj))
        src=config if name=='config' else pathlib.Path('src/db/'+name+'.c')
        run(['gcc','-O1','-g','-Isrc/db','-Dmalloc=p6_malloc','-Dcalloc=p6_calloc',
             '-Drealloc=p6_realloc','-Dfree=p6_free','-c',str(src),'-o',str(obj)])
    binary=work/'reader-probe'
    run(['gcc','-O1','-g','-Isrc/db','-DP6_CMD_SOURCE="'+str(command)+'"',str(probe),
         *objects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-lm','-o',str(binary)])
    run([str(binary)],expected)
def main():
    a=argparse.ArgumentParser();a.add_argument('--plan',type=pathlib.Path,required=True)
    a.add_argument('--mode',choices=['red','green','regression-proof'],required=True);args=a.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b6-check-'));print('scratch:',work,flush=True)
    probe=work/'probe.c';probe.write_text(extract(args.plan,'reader-probe'))
    config=pathlib.Path('src/db/config.c');storage=pathlib.Path('src/db/storage.c')
    if args.mode!='regression-proof':
        check(work,probe,config,storage,1 if args.mode=='red' else 0);return
    manifest=json.loads(extract(args.plan,'manifest'));digest=lambda b:hashlib.sha256(b).hexdigest()
    copies={}
    for path in [str(config),str(storage)]:
        data=pathlib.Path(path).read_bytes()
        if digest(data)!=manifest['checkpoints']['2'][path]:raise ValueError('fixed source mismatch '+path)
        copies[path]=data
    edits=[e for e in manifest['edits']['2'] if e['path'] in copies]
    for e in reversed(edits):
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(new)!=1:raise ValueError('reverse anchor '+e['new'])
        copies[e['path']]=data.replace(new,old)
    for path,data in copies.items():
        if digest(data)!=manifest['parent_sources'][path]:raise ValueError('baseline hash mismatch '+path)
        (work/pathlib.Path(path).name).write_bytes(data)
    check(work,probe,work/'config.c',work/'storage.c',1)
    for e in edits:
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(old)!=1:raise ValueError('reapply anchor '+e['old'])
        copies[e['path']]=data.replace(old,new)
    for path,data in copies.items():
        if digest(data)!=manifest['checkpoints']['2'][path]:raise ValueError('reapply hash mismatch '+path)
        (work/pathlib.Path(path).name).write_bytes(data)
    check(work,probe,work/'config.c',work/'storage.c',0)
    print('scratch revert/reapply proof passed; checkout source and binaries untouched')
if __name__=='__main__':main()
```

### reader-probe

<!-- block:reader-probe -->
```c
#define _GNU_SOURCE
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
ShardDb *shard_db_open_internal(const char *);
void shard_db_close(ShardDb *);
#define MUST(x) do{if(!(x)){fprintf(stderr,"setup line %d: %s errno=%d\n",__LINE__,#x,errno);exit(2);}}while(0)
static int armed;static size_t nth,calls,live,largest;static void *owned[4096];
static int find(void *p){for(size_t i=0;i<live;i++)if(owned[i]==p)return (int)i;return -1;}
static int allow(size_t n){if(!armed)return 1;calls++;if(n>largest)largest=n;if(n>8u*1024*1024||calls==nth){errno=ENOMEM;return 0;}return 1;}
static void remember(void *p){if(armed&&p){MUST(live<4096);owned[live++]=p;}}
void *p6_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *p6_calloc(size_t a,size_t b){if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}if(!allow(a*b))return NULL;void *p=calloc(a,b);remember(p);return p;}
void *p6_realloc(void *p,size_t n){if(!allow(n))return NULL;int i=find(p);void *q=realloc(p,n);if(q){if(i>=0)owned[i]=owned[--live];remember(q);}return q;}
void p6_free(void *p){int i=find(p);if(i>=0)owned[i]=owned[--live];free(p);}
static TypedSchema *fixture;static const uint8_t *record;static size_t record_len;static uint8_t generation;
static SlotcaskDb dummy;static int released;
static Schema probe_schema(const char *root,const char *object){(void)root;(void)object;return (Schema){.splits=1,.streams=1,.slot_size=128};}
static TypedSchema *probe_typed(const char *root,const char *object){(void)root;(void)object;return fixture;}
static SlotcaskDb *probe_registry(const char *r,const char *o,const SlotcaskSchemaInfo *i){(void)r;(void)o;(void)i;return &dummy;}
static void probe_release(SlotcaskDb **db){MUST(*db==&dummy);released++;}
static int probe_get(SlotcaskDb *db,const void *key,size_t klen,void **out,size_t *len,uint8_t *hdr){
    (void)db;(void)key;(void)klen;*out=malloc(record_len?record_len:1);MUST(*out);
    memcpy(*out,record,record_len);*len=record_len;*hdr=generation;return 0;
}
#define cmd_get probe_cmd_get
#define load_schema probe_schema
#define load_typed_schema probe_typed
#define slotcask_registry_get probe_registry
#define slotcask_registry_ref_release probe_release
#define slotcask_get_ex probe_get
#include P6_CMD_SOURCE
#undef cmd_get
#undef load_schema
#undef load_typed_schema
#undef slotcask_registry_get
#undef slotcask_registry_ref_release
#undef slotcask_get_ex
typedef struct {size_t accepted,limit;char tail[8];size_t seen;int error_attempted;} Writer;
static ssize_t write_span(void *ctx,const char *data,size_t n){
    Writer *w=ctx;
    for(size_t i=0;i<n;i++){
        memmove(w->tail,w->tail+1,6);w->tail[6]=data[i];w->tail[7]=0;w->seen++;
        if(w->seen>=7&&!memcmp(w->tail,"\"error\"",7))w->error_attempted=1;
    }
    size_t room=w->limit-w->accepted;if(!room){errno=EIO;return -1;}
    size_t count=n<room?n:room;w->accepted+=count;return (ssize_t)count;
}
#ifdef __APPLE__
static int write_mac(void *ctx,const char *data,int n){return (int)write_span(ctx,data,(size_t)n);}
static FILE *writer_open(Writer *w){return funopen(w,NULL,write_mac,NULL,NULL);}
#else
static FILE *writer_open(Writer *w){cookie_io_functions_t ops={.write=write_span};return fopencookie(w,"w",ops);}
#endif
static int failures;
static void gate(const char *what,int ok){printf("%s: %s\n",what,ok?"PASS":"FAIL");if(!ok)failures++;}
static int invoke(int get,size_t fail,char **out,int *cause){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);MUST(fp);FILE *save=g_out;g_out=fp;
    calls=0;nth=fail;released=0;armed=1;
    int rc=get?probe_cmd_get("root","obj","a",1):typed_decode_stream(fixture,record,(int)record_len,fp,generation);
    *cause=errno;armed=0;g_out=save;MUST(!fclose(fp));
    gate("selected buffers released",live==0);if(get)gate("registry reference released once",released==1);return rc;
}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f){(void)s;(void)k;(void)f;return remove(p);}
int main(void){
    char root[]="/tmp/p3b6-probe-XXXXXX";MUST(mkdtemp(root));ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    TypedSchema *legacy=calloc(1,sizeof(*legacy));MUST(legacy);legacy->typed=1;legacy->nfields=2;legacy->nullmap_prefix=2;legacy->total_size=1005;
    TypedField *v=&legacy->fields[0],*e=&legacy->fields[1];
    v->type=FT_VARCHAR;v->size=1002;strcpy(v->name,"f0");v->name_len=2;
    e->type=FT_ENUM;e->offset=1002;e->size=1;e->enum_width=1;e->n_enum_values=1;strcpy(e->name,"f1");e->name_len=2;
    e->enum_values=calloc(1,sizeof(char*));MUST(e->enum_values);e->enum_values[0]=malloc(1001);MUST(e->enum_values[0]);memset(e->enum_values[0],'z',1000);e->enum_values[0][1000]=0;
    uint8_t old[1005]={1,0};old[2]=1;old[3]=244;memset(old+4,'v',500);
    TypedSchema *packed=calloc(1,sizeof(*packed));MUST(packed);packed->typed=1;packed->nfields=2;
    packed->fields[0].type=FT_NUMERIC;packed->fields[1].type=FT_VARCHAR;
    strcpy(packed->fields[0].name,"f0");strcpy(packed->fields[1].name,"f1");packed->fields[0].name_len=packed->fields[1].name_len=2;
    const char *types[]={"numeric:26,10","varchar"};MUST(!typed_schema_bind_packed(packed,types,2,NULL,0));
    uint8_t *new_record=NULL;size_t new_len=0;MUST(!typed_encode_alloc(packed,"{\"f0\":1.25,\"f1\":\"a\\u0000b\"}",1,NULL,NULL,&new_record,&new_len,NULL,0));
    const char *expected_body="{\"f0\":1.2500000000,\"f1\":\"a\\u0000b\"}";
    const char *expected_get="{\"f0\":1.2500000000,\"f1\":\"a\\u0000b\"}\n";
    for(int layout=0;layout<2;layout++)for(int get=0;get<2;get++){
        fixture=layout?packed:legacy;record=layout?new_record:old;record_len=layout?new_len:sizeof(old);generation=layout?PKR_FLAGS:SREC_FLAG_NULLMAP;
        char *text=NULL;int cause=0,rc=invoke(get,0,&text,&cause);size_t count=calls;
        gate("normal decode succeeds",rc==0);
        gate("allocation sweep is nonempty",count>0);
        if(layout)gate("packed exact JSON",text&&!strcmp(text,get?expected_get:expected_body));
        else gate("legacy long enum complete",text&&strstr(text,e->enum_values[0]));
        free(text);
        for(size_t i=1;i<=count;i++){
            rc=invoke(get,i,&text,&cause);
            gate("allocation failure signalled",rc==(get?1:-1)&&cause==ENOMEM);
            gate("no partial or false-success output",text&&(!get?!text[0]:!strcmp(text,"{\"error\":\"out of memory\"}\n")));
            free(text);
        }
        printf("layout=%d get=%d allocation injections=%zu\n",layout,get,count);
    }
    fixture=packed;record=new_record;record_len=new_len;generation=PKR_FLAGS;
    FILE *ro=fopen("/dev/null","r");MUST(ro);setvbuf(ro,NULL,_IONBF,0);FILE *save=g_out;g_out=ro;released=0;
    int rc=probe_cmd_get("root","obj","a",1);g_out=save;
    gate("GET output failure returns error",rc==1&&ferror(ro)&&released==1);fclose(ro);
    for(int newline=0;newline<2;newline++){
        Writer writer={.limit=newline?strlen(expected_body):7};
        FILE *bad=writer_open(&writer);MUST(bad);MUST(!setvbuf(bad,NULL,_IONBF,0));
        save=g_out;g_out=bad;released=0;rc=probe_cmd_get("root","obj","a",1);g_out=save;
        gate(newline?"newline failure returned":"partial write failure returned",rc==1&&released==1);
        gate("no replacement error after output failure",writer.accepted==writer.limit&&!writer.error_attempted);
        fclose(bad);
    }
    /* Corruption is an error object, never success/null/concatenated JSON. */
    generation=SREC_FLAG_NULLMAP;char *text=NULL;int cause=0;rc=invoke(1,0,&text,&cause);
    gate("generation mismatch reports one error",rc==1&&text&&!strcmp(text,"{\"error\":\"corrupt record\"}\n"));free(text);
    TypedSchema *maximum=calloc(1,sizeof(*maximum));MUST(maximum);maximum->typed=1;maximum->nfields=MAX_FIELDS;const char *tokens[MAX_FIELDS];
    for(int i=0;i<MAX_FIELDS;i++){tokens[i]="varchar";maximum->fields[i].type=FT_VARCHAR;snprintf(maximum->fields[i].name,sizeof(maximum->fields[i].name),"f%d",i);maximum->fields[i].name_len=(int)strlen(maximum->fields[i].name);}
    MUST(!typed_schema_bind_packed(maximum,tokens,MAX_FIELDS,NULL,0));MUST(maximum->total_size>1000000000);
    uint8_t *small=NULL;size_t small_n=0;MUST(!typed_encode_alloc(maximum,"{\"f127\":\"tiny\"}",1,NULL,NULL,&small,&small_n,NULL,0));
    fixture=maximum;record=small;record_len=small_n;generation=PKR_FLAGS;largest=0;rc=invoke(1,0,&text,&cause);
    gate("max schema GET uses actual size",rc==0&&small_n<4096&&largest<4096&&text&&strstr(text,"\"f127\":\"tiny\""));
    printf("maximum actual=%zu largest=%zu live=%zu\n",small_n,largest,live);free(text);free(small);free(maximum);
    printf("probe failures=%d\n",failures);free(new_record);free_enum_values(e);free(legacy);free(packed);
    shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return failures?1:0;
}
```

### manifest

<!-- block:manifest -->
```json
{
  "base_head": "35e49ab8cd7a7efe7350562e34eaeac62ea4afae",
  "ancestor": "35e49ab8cd7a7efe7350562e34eaeac62ea4afae",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "parent_sources": {
    "build.sh": "149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999",
    "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
    "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
    "src/db/types.h": "088df474438380b58b8fc264c3f330445f9e9270a2390928e2733056277c6d28",
    "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
    "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
    "src/db/packed_record.c": "8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d",
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f"
  },
  "blocks": {
    "reader-test": "34ead3ccfcfb4a8f1b1c63ca733c35c22518d4173e8ee682765d626bc6d892ff",
    "build-register-old": "339c8e1ca647b26b09f26937d8e3f090d2382f4a8d9a16ea1ce1d4fb6620ebe0",
    "build-register-new": "322c7135210891c7a3e49ec2e1a4c875c71a63c031a6a0cbf14bcd76dad87bd1",
    "reader-edit-1-old": "6451fac989b62c410a52593aecd0cc0d13d357c94beb1107cbc511da1050f8db",
    "reader-edit-1-new": "21687b215aff46105a95fc14005fa82f7f2609e8d35f4ade6e4a7973a12af95d",
    "reader-edit-2-old": "e0e79c109394953305d3c90854d60d9c9f2cfe67d9d11e2715375139ebef545d",
    "reader-edit-2-new": "ba6bba8f55d4a2141b47f2eba2f2c90af29f107985c7688d9a290cde1fbdc5fb",
    "reader-edit-3-old": "f9666e56cff360e0cf92c2f15d57a127c98c01f1774e946bde5b2d66da5be24d",
    "reader-edit-3-new": "a3e2e1f92d22f2d2a2441f29c666e3cbd9330112450c94bde7acb137d2ff4b7f",
    "reader-edit-4-old": "1fab8adcb330f22ff4178c083592d02f8ec46da2d7ce46cef70656e03544e5b8",
    "reader-edit-4-new": "3da64ac1a3c265070c621e46505f2a934729aba6487cf08069957ce006ea00a4",
    "reader-edit-5-old": "84bce45d36f849c0a92a2adaf74572d890fec90bbfe5bbfcc31a60a49116d425",
    "reader-edit-5-new": "d0cd2bed4bc5a1eea00c3c15b483c43467d7f49c40ffb32a434892febd959152",
    "reader-doc": "62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22",
    "adapter-doc-old": "bf93453019ea49b04d6e5d589926e344ca0da53333e2bf4c16a1f271b3c5e4a2",
    "adapter-doc-new": "6b9d2e50934fae3a805511b0546c5ac75329510279c24c581348d198a4f8bbeb",
    "records-doc-old": "37b91f429bdb7c02efe258217260f63385c9849d4e88c31be0177b6cf6993948",
    "records-doc-new": "a802c90c9297f9b57e0491c75be20ae34c08233ffa7d7bc2bdc45e60c846d5c4",
    "driver": "49af0525494d945553b27aaecc5264637ddd2aefbbf293c69b3a0273d2cd36e4",
    "checks": "3cb81bf0e6fc5cdf3a0b01e5cda4121ad5e089e4a4574e7ef55c2bc8f360137b",
    "reader-probe": "ec87ec8e67b968fb4990274e7c7513fe23491044a5f7418f57a19e505936d5d2",
    "bootstrap": "b880994c1b8350eb130431a5f68433d3b9fdf9926668ff343286b11a572445af"
  },
  "edits": {
    "1": [
      {
        "path": "src/test/cases/test_whole_record_readers.c",
        "old": null,
        "new": "reader-test"
      },
      {
        "path": "build.sh",
        "old": "build-register-old",
        "new": "build-register-new"
      }
    ],
    "2": [
      {
        "path": "src/db/config.c",
        "old": "reader-edit-1-old",
        "new": "reader-edit-1-new"
      },
      {
        "path": "src/db/config.c",
        "old": "reader-edit-2-old",
        "new": "reader-edit-2-new"
      },
      {
        "path": "src/db/types.h",
        "old": "reader-edit-3-old",
        "new": "reader-edit-3-new"
      },
      {
        "path": "src/db/storage.c",
        "old": "reader-edit-4-old",
        "new": "reader-edit-4-new"
      },
      {
        "path": "src/db/typed_row.h",
        "old": "reader-edit-5-old",
        "new": "reader-edit-5-new"
      }
    ],
    "3": [
      {
        "path": "docs/concepts/whole-record-readers.md",
        "old": null,
        "new": "reader-doc"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "adapter-doc-old",
        "new": "adapter-doc-new"
      },
      {
        "path": "docs/concepts/typed-records.md",
        "old": "records-doc-old",
        "new": "records-doc-new"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "build.sh": "149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999",
      "docs/concepts/typed-records.md": "d32d3ed755736801ab130c27712fdf780c447008c3a6b0a4ae8ab559cbff3762",
      "docs/concepts/typed-row-adapter.md": "0a84766a6041f615e76e69403975e002eda99aeb0a22bed34aec74f67bdadf3a",
      "docs/concepts/whole-record-readers.md": null,
      "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
      "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
      "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
      "src/db/types.h": "088df474438380b58b8fc264c3f330445f9e9270a2390928e2733056277c6d28",
      "src/test/cases/test_whole_record_readers.c": null
    },
    "1": {
      "build.sh": "488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0",
      "docs/concepts/typed-records.md": "d32d3ed755736801ab130c27712fdf780c447008c3a6b0a4ae8ab559cbff3762",
      "docs/concepts/typed-row-adapter.md": "0a84766a6041f615e76e69403975e002eda99aeb0a22bed34aec74f67bdadf3a",
      "docs/concepts/whole-record-readers.md": null,
      "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
      "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
      "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
      "src/db/types.h": "088df474438380b58b8fc264c3f330445f9e9270a2390928e2733056277c6d28",
      "src/test/cases/test_whole_record_readers.c": "34ead3ccfcfb4a8f1b1c63ca733c35c22518d4173e8ee682765d626bc6d892ff"
    },
    "2": {
      "build.sh": "488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0",
      "docs/concepts/typed-records.md": "d32d3ed755736801ab130c27712fdf780c447008c3a6b0a4ae8ab559cbff3762",
      "docs/concepts/typed-row-adapter.md": "0a84766a6041f615e76e69403975e002eda99aeb0a22bed34aec74f67bdadf3a",
      "docs/concepts/whole-record-readers.md": null,
      "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
      "src/db/storage.c": "3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab",
      "src/db/typed_row.h": "389b9d9b13e4c96f620c2ce851a29f1985e79781c6df0726604ec4ebd6131da7",
      "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
      "src/test/cases/test_whole_record_readers.c": "34ead3ccfcfb4a8f1b1c63ca733c35c22518d4173e8ee682765d626bc6d892ff"
    },
    "3": {
      "build.sh": "488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0",
      "docs/concepts/typed-records.md": "1d4670538b63e5df02f425974c874653993a492b448777f5824b68966b48a6d5",
      "docs/concepts/typed-row-adapter.md": "4020adc7a8295014702e6288994d9d643be86e4faa2f089d2a35b14556b03f21",
      "docs/concepts/whole-record-readers.md": "62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22",
      "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
      "src/db/storage.c": "3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab",
      "src/db/typed_row.h": "389b9d9b13e4c96f620c2ce851a29f1985e79781c6df0726604ec4ebd6131da7",
      "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
      "src/test/cases/test_whole_record_readers.c": "34ead3ccfcfb4a8f1b1c63ca733c35c22518d4173e8ee682765d626bc6d892ff"
    }
  }
}
```
