# Phase 3B.8 — length-aware named display text and composite JSON

## Primary goal

Return concatenated financial fields with every decimal digit and stored string
byte intact, and report any component failure instead of silently returning a
shorter successful value, while existing applications stay on the legacy format.

**Status: Revision 2, blind-reviewed and repaired; awaiting explicit approval.** Planner: GPT 6.1 Sol.
Intended executor: GLM 5.3, per the user's series assignment. Base is reviewed
Phase3B.7 commit `cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7` on
`fix/phase1-safe-scalar-conversions`. No root implementation is authorized here.
Complete code below has been normal-compiled and tested only in scratch.

## Revision 2 — blind review repairs

- STD-01 (P1): every Appendix A driver/checks --plan argument now targets this
  Phase3B.8 document, rather than the prior Phase3B.7 plan. The published command
  sequence is checked for consistent plan/bootstrap/script paths.
- SPEC-01 (P2): duplicate large strings can produce JSON beyond INT_MAX while
  the existing escaping backend returns int. quote_into now feeds at most4096
  source bytes per backend call (at most24576 escaped bytes), accumulating all
  offsets/counts as size_t. No new response ceiling or public signature change.
- The permanent case adds a full multi-chunk NUL composite check. A scratch
  wrapper delegates unchanged to the real backend while recording input spans;
  the old quote function demonstrably makes an oversized call, the repair keeps
  every span<=4096. This validates the count-safety invariant without allocating
  multi-gigabyte test output; no artificial backend failure is injected.

## Scope and decisions presented for approval

Add two output-only named APIs and a private checked component resolver. Enable
packed '+' JSON projections in the shared named cell and complete projected
object, replacing Phase3B.7's explicit ENOTSUP refusal. Preserve existing legacy
raw helpers and every public legacy branch. Neither matching/sorting/grouping
nor CSV gains packed dispatch. Public loaders/create-object still do not bind
packed. No metadata/default-format/version/migration edit or dependency.

| Contract | Chosen behavior |
|---|---|
| Raw named display API | Owned NUL-terminated bytes and authoritative text_len excluding NUL; binary varchar suffix survives; budget includes terminator |
| Raw absence/error | NULL/removed/unknown scalar or a zero-byte composite: NULL, errno=0, text_len=0; error: NULL, nonzero errno, text_len=0 |
| Scalar empty string | Owned empty string, length0; distinct from absence |
| Scalar JSON | Existing numeric/bool/string/NULL projection policies unchanged |
| Composite JSON | Concatenate component display bytes in order, with no separator; quote and escape the entire result exactly once; numeric components become text inside that one string |
| Missing and empty parts | Skip unknown, removed and NULL components; empty strings add0 bytes; a total0-byte concatenation becomes null, matching legacy concatenation behavior |
| Zero/false | Real display text, never mistaken for missing |
| Grammar | '+' separates components; empty '+' tokens collapse; whitespace significant; preserve duplicates/order; unknown names do not error |
| Bounds | MAX_LINE-1 selector bytes and MAX_FIELDS nonempty components, including unknown names; E2BIG rather than legacy255-byte truncation |
| Validation | Whole packed schema/view validated before rendering any selector; unselected malformed descriptors/decimals or fixed-schema mismatch still reject |
| Memory | Exact raw concatenation workspace plus selected JSON output and bounded decimal workspaces; no schema-ceiling or whole unselected-varchar JSON allocation |
| Budget | Direct APIs enforce supplied output budget; shared production JSON callers retain SIZE_MAX until enclosing activation/memory policy work |

Approval accepts the deliberate private packed-composite behavior change and
updates the two old ENOTSUP assertions to exact-value assertions. This is not
assertion weakening: new behavior replaces an explicitly temporary refusal and
is proved RED before implementation. Legacy composite output stays unchanged.

Duplicate components can make output larger than the source record. A composite
raw workspace coexists with its JSON cell or the final object during rendering;
object measurement/rendering each build and release that workspace. The bound is
the explicit output budget and component count, not vlen. No lower-memory or
performance claim is made. This child does not enforce QUERY_BUFFER_MB; public
activation still requires a complete enclosing request-memory policy.

## Task 0 — documentation discovery and checkpoint

**How this serves the primary goal:** separates byte-aware output from old string
APIs that also drive matching, sorting, grouping and cursor behavior.

Read AGENTS.md/CORE-PROCESS.md, the Phase3 work index/parent recovery roadmap,
reviewed Phase3B.7 plan/evidence/raw-diff review, typed-records.md,
typed-row-adapter.md, packed-json-projection.md, whole-record-readers.md and
query-protocol/find.md/overview.md/joins.md. Never read the protected rig contents.
Two read-only discovery agents supplied actual signatures, source/caller searches,
copy-ready fixtures and known gaps; planner rechecked relevant bodies. Repository
search is required because semantic reference results miss cross-TU consumers.

### Allowed APIs and copy-ready patterns

- typed_row.c: reuse RowText, projection_text_open, row_text_open/free/length/write
  and the checked quote/size helpers, all private to this existing translation
  unit. New NamedText owns only its optional joined buffer plus RowText decimal
  state; every open attempt is initialized and freed, including failure paths.
- typed_row.h: typed_row_open/typed_row_span borrow immutable schema/record spans;
  they reject wrong generation, descriptors/decimal payloads and fixed metadata.
  New named APIs open one whole view; the object renderer retains its single view.
- config.c/types.h: typed_field_index uses exact names and excludes removed fields;
  parse_field_type is void; typed_get_field_str remains legacy-only and unchanged.
  Do not broaden it or decode_field: their strlen consumers cannot carry binary.
- types.h/query_find.c: json_projected_field still returns one owned JSON value;
  packed branch delegates to typed_packed_named_json, legacy body stays identical.
  json_typed_row_cell remains index-based and unchanged by this child.
- storage.c: cmd_get_fields packed branch already renders a complete object before
  output, maps E2BIG/ENOMEM/errors and checks fwrite/ferror/newline. No command
  source edits are needed. Probe extracts its real body instead of reimplementing it.
- Copy private schema/encode/budget/assert patterns from
  test_packed_json_projection.c:p_schema/p_free/p_encode/p_basics/p_failures/
  p_limits. Use real TEST_REGISTER/assert headers. New tests do not swap caches,
  process environment or public schema registries. Two changed assertions remain
  in the existing case; new behavior/limits/OOM mechanisms get their own case.
- Legacy composite reference: test_get_fields.c:test_get_fields_composite_overflow
  verifies full5798-byte display concatenation. CSV examples are in
  test_csv_export.c and auto-key tests, but their integration is not claimed here.

### Consumer census at fixed HEAD

| Surface | Actual consumers and scope |
|---|---|
| New typed_packed_named_text | New committed test/scratch probe only; no production raw-field consumer |
| New typed_packed_named_json | json_projected_field packed branch; new test/probe |
| Shared quote_into | row_text_write (quoted scalar/named/composite output) and field-name quoting in typed_packed_decode/project_json; escaping bytes are unchanged, int backend calls are bounded |
| New private NamedText resolver | New APIs and typed_packed_project_json measuring/writing; no escaping through legacy decode_field |
| typed_packed_project_json | cmd_get_fields packed branch, private tests/probe; whole-object order/duplicates/names unchanged |
| json_projected_field | storage.c cmd_get_fields; query_find.c print_record_json/dict/row; query.c at1704/1721,2261/2278,2330/2347,2899/2916,5107/5129/5146,6800/6821/6838,9205/9227/9243; query_join.c at748/770/786 |
| typed_get_field_str | query_find composite/scalar decode_field, legacy row cell, CSV; query.c:6874 cursor capture; query_join.c:540/550 CSV driver extraction; unchanged |
| decode_field | query_find named legacy JSON/CSV; query_plan.c:917 matching fallback; query.c:6411 sorting fallback; query_aggregate.c:2442 grouping fallback; unchanged |
| CSV emitters | csv_emit_row called by query.c:1695/5100/9195, query_join.c:741, query_find.c:1863; csv_emit_cell also keys/get-multi/aggregates; all unchanged |
| Other emitters | print_record_json/dict definitions only; print_record_row called by v2_fetch_cb; their enclosing failure/envelope behavior stays separate work |
| Shared indexed JSON cell | json_typed_row_cell callers in query_find/query/query_join unchanged; no named-component grammar added to indexed API |
| Public binding | typed_schema_bind_packed remains declaration/definition/test fixtures only, no loader/create caller |
| Build | One test registration after test_packed_json_projection; no new production TU/source-list/dependency |

Line numbers are census evidence only. All executable edits use exact quoted
anchors and complete blocks below. No existing public C signature changes.

### Root causes

1. The private packed output path explicitly refuses '+', while legacy decode_field
   uses a 255-byte selector copy, strlen and int concatenation lengths. Calling
   that raw path for packed data would read descriptors as old fields and drop
   embedded NUL suffixes. Add an isolated explicit-length output resolver; do not
   activate old matcher/sort/group consumers as a side effect.
2. Legacy component NULL conflates absence with some failures and can skip a
   failed component. The new component walk treats each open failure as fatal,
   frees unpublished output and resets raw text_len; missing parts have explicit
   zero contribution. Genuine empty composite status remains errno zero.
3. A composite must be escaped once after concatenation, not joined from JSON
   fragments. Build exact raw display bytes using RowText's non-JSON writer,
   then render one quoted span; scalar JSON continues using its existing policy.

4. The backend json_escape_into returns an int although it accumulates size_t.
   Repeating a4MiB all-NUL field86 times produces360710144 raw bytes and
   2164260864 escaped payload bytes, beyond INT_MAX without size_t overflow.
   Original quote_into called the backend once and rejected its narrowed count
   after allocating/writing the output. Bound each backend input chunk to4096,
   for at most24576 emitted bytes (within even the minimum C INT_MAX), and retain
   size_t totals. This is shared packed quoting, not a global util API rewrite.

### Execution rules

- Await explicit approval; verify exact branch/HEAD/plan SHA and starting status.
  Stay on this branch; no reset/stash/branch or Git writes. Preserve all pre-existing
  plan/index/review files and the protected untracked plan.
- Protected docs/plans/2026-10-19-durability-certification-rig.md hash must equal
  fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
  Hash-only at start/end; driver repeats at each stage/check-only. Never read its
  contents, copy, edit/move, stage, incorporate or execute it. Checks.py probes do
  not hash this file; do not overstate their scope in evidence.
- Extract frozen bootstrap/driver/checks to fresh /tmp. Stage1 --check-only must
  verify anchors and all checkpoint0 hashes before any source writes. Apply
  stages in order. A mismatch, warning, unexpected result/count or uncovered
  decision requires PLAN_NOTES.md and a halt of the entire run. Planner/human
  must amend before resume. No guesses, ad-hoc anchor repair, weaker assertions,
  skipped daemon cases or rerun-until-green.
- All shell commands rtk-prefixed. Normal build SKIP_TESTS=1 ./build.sh; tests via
  build/bin/shard-db-test. Use socket-capable execution if the sandbox forbids
  daemon sockets. Full suite remains mandatory. No benchmarks or sanitizers;
  branch-final ASan+UBSan/TSan gates stay before an authorized push/PR.
- Only nine owned checkpoint files plus curated evidence are execution deliverables.
  Logs/probes/binaries/extraction scripts/caches stay /tmp. New production test
  globals, runtime flags, loader seams or schema activation are forbidden.
- Stop unstaged/uncommitted for plan-aware and independent blind raw-diff reviews.
  No commit/push/PR/merge, later-phase execution or protected certification work.

## Task 1 — regressions and linkable RED before implementation

**How this serves the primary goal:** proves exact named/composite output is absent
and verifies the new contract fails before the feature code exists.

Apply frozen stage1: new test_packed_named_projection.c, its single build line,
new header declarations/contract and linkable ENOTSUP stubs. Update only the two
existing packed-composite assertions from refusal to exact escaped output. Legacy
controls remain unchanged; no production dispatch/implementation is added yet.

Zero-warning normal build, exit0. Capture both focused RED results:
- test-packed-named-projection: **38 passed/158 failed, exit158**.
- test-packed-json-projection: **216 passed/2 failed, exit2**, only the two
  deliberately advanced composite assertions fail.
Run checks --mode red: expected probe exit1, internally handled, exactly
**21 passing/17 failing gates**. Reject setup failures (exit2), signals or a
wrong gate count as RED. Baseline allocation counts0 for absent new cell paths
are not OOM-safety evidence. Command has one existing CSV-selector-copy allocation;
its normal composite output still refuses before the proposed renderer exists.

## Task 2 — checked named display text and composite JSON

**How this serves the primary goal:** carries exact component bytes through one
owned display result and renders a complete JSON value or an explicit error.

Apply frozen stage2: replace stubs with the full NamedText resolver/raw/JSON APIs,
replace the complete packed object function with named resolution, simplify the
packed shared cell branch, replace quote_into with the bounded-chunk writer,
and update the header's old composite-refusal comment.
No existing raw getter, CSV/criteria/sort/group/join logic or command source edit.

The resolver bounds/parses components without truncating their selector, resolves
exact name spans, measures actual display bytes, allocates one exact concatenation,
and writes every component. Scalar raw output is an owned display copy; composite
raw output transfers its joined buffer. Missing status is explicit, not inferred
from a NULL allocation. Each error releases all temporary decimal/joined/output
storage. JSON quotes a joined byte span exactly once. Full object validates one
view and remains unpublished until every selected field is complete.

Require zero-warning build and focused **196/0** plus prior projection **218/0**.
Run checks --mode green: **258 passing gates**, zero failures, including
**77 allocation injections** (raw numeric composite12, direct JSON13, named
binary JSON9, raw binary composite8, projected command35). Every injected failure
returns NULL/ENOMEM with text_len0 where applicable, or one command error without
success prefix. All selected buffers and the mocked registry ref are released.
A delegated-backend probe additionally checks full147476-byte quoted output
from24579 raw NUL bytes and a largest backend input<=4096. The old writer
returns correct medium output but fails that safety-invariant gate; the2GiB
example is established by exact arithmetic, never allocated in tests.
Command tests also cover empty composite/null, over-component E2BIG, unselected
corruption, actual partial writes/newline errors and no replacement error after
an I/O prefix. This is FILE accepted-byte checking, not socket transaction/flush.

Maximum-schema row actual2085/largest selected allocation36/live0 despite >1GiB
schema ceiling. Mocked raw-read allocation is excluded from selected tracking,
not claimed eliminated. Permanent tests verify every byte of4MiB binary input
and two concatenated full-range decimal values, plus limits/budgets/absence.

Run checks --mode regression-proof: copy actual typed_row.c/query_find.c/storage.c
into /tmp, reverse stage2 only and verify checkpoint1 (row with stubs; query_find
at committed base). storage.c is unchanged by this child and must match its
committed parent-source hash. Recompile those real copies and require21/17;
reapply exact hunks, verify checkpoint2/unchanged-storage hashes, compile and
require258/0. Paste both outputs. Never revert root sources or binaries.

## Task 3 — documentation, final verification and stop

**How this serves the primary goal:** records the exact output-only boundary and
proves existing applications keep their legacy behavior.

Apply frozen stage3: new packed-named-projection.md plus exact link/contract
corrections in packed-json-projection.md and typed-row-adapter.md. Explain
composite output/absence/byte budgets and actual workspace coexistence, including
repeated-component output larger than a record. Do not claim query activation.

Run a fresh normal full suite after docs: **20459 passed/0 failed across526
cases**, exit0. Repeat frozen green checks, all final nine-file checkpoint hashes,
git diff --check and binder census. Explicitly re-hash protected file at finish.
No debug/TODO/FIXME/log/cache/probe additions. Curated evidence goes only to
`docs/plans/2026-10-26-phase3b8-packed-named-projection-evidence.md`, with actual
commands/identity/compiler/hashes, both REDs, GREENs, probe/reversal/full outputs,
allocation accounting and remaining scope. Do not replace actual results with
forecast numbers. Stop for executed reviews without staging or committing.

## Planning evidence and remaining limits

Scratch git archive of cfc4970: /tmp/p3b8-plan-jowcaa_x. The protected untracked
file was never copied/read. All planning builds/tests are ordinary, no dynamic
sanitizers or benchmarks. Exact frozen blocks/checkpoint/script verification and
current runtime logs are recorded in Appendix D after the code blocks.
Appendix C remains historical original-plan evidence, not current gate counts.

The command probe extracts real cmd_get_fields with local schema/registry/read
mocks and instruments real typed_row/query_find/packed_record/decnum code. No
production seam or public cache binding is added. Raw-reader allocation and libc
FILE internals remain outside tracked allocation sweeps. Linux x86_64 is exercised;
the FILE probe supplies funopen on Apple but that platform is not run here.

The plan does not fix old legacy raw-field OOM or CSV loss/error policies, does
not prove public network/metadata/matching correctness and does not introduce
new decimal arithmetic or a live PostgreSQL dependency. Existing Phase2/3A exact
rendering is reused; named raw bytes do not become safe strlen inputs.

## Following work

Next close CSV representation/error propagation and enclosing read/query emitter
contracts without activating unsafe raw matching/sort/group/cursor consumers.
Decide binary/NUL CSV behavior explicitly in that child, rather than silently
truncating or choosing a new encoding. Complete remaining read/delete/index/
maintenance/rebuild inventory before 3B closure. Phase3C owns metadata/publication;
Phases4–6 assignment, query/index and aggregates; Phase7 other field contracts;
Phase8 existing-data conversion; Phase9 application acceptance/activation.
The separate protected durability certification work remains afterward.

## Appendix A — frozen extraction and exact command order

Execute bootstrap below with rtk proxy python3 from repo root. Substitute its
printed fresh /tmp folder into every command and record unfiltered outputs/exit
status. Focused RED exits158 and2 are required; checks internally handle expected
probe exit1 and enforce exact gate counts. Neither setup errors nor signals count.

<!-- block:bootstrap -->
```python
import pathlib,re,tempfile
plan=pathlib.Path('docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md')
folder=pathlib.Path(tempfile.mkdtemp(prefix='p3b8-exec-'))
for m in re.finditer(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text()):
    (folder/(m.group(1)+'.txt')).write_text(m.group(2)+'\n')
for name in ['driver','checks']:
    (folder/(name+'.py')).write_bytes((folder/(name+'.txt')).read_bytes())
print(folder)
```

~~~sh
rtk proxy python3 /tmp/p3b8-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --stage 1 --check-only
rtk proxy python3 /tmp/p3b8-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-named-projection
rtk proxy ./build/bin/shard-db-test run test-packed-json-projection
rtk proxy python3 /tmp/p3b8-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --mode red
rtk proxy python3 /tmp/p3b8-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-named-projection
rtk proxy ./build/bin/shard-db-test run test-packed-json-projection
rtk proxy python3 /tmp/p3b8-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --mode green
rtk proxy python3 /tmp/p3b8-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --mode regression-proof
rtk proxy python3 /tmp/p3b8-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 /tmp/p3b8-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md --mode green
rtk git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
~~~

## Appendix B — complete anchored code and checkpoints

Every named block is frozen and manifest-hashed. A missing/duplicate anchor or
checkpoint mismatch halts before stage mutation. New files must be absent.
Each task is fully specified; no code is left for executor design.

### named-test

<!-- block:named-test -->
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
static TypedSchema *n_schema(int maximum){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"private projection schema");if(!ts)return NULL;
    ts->typed=1;ts->nfields=maximum?MAX_FIELDS:7;const char *types[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        types[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"numeric":i==3?"bool":i==4?"int":i==6?"enum(yes,no)":"varchar";
        if(!strncmp(types[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else if(!strcmp(types[i],"varchar"))ts->fields[i].type=FT_VARCHAR;
        else parse_field_type(types[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,types,(size_t)ts->nfields,NULL,0);ASSERT_EQ_INT(rc,0,"bind private schema");
    if(rc){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);return NULL;}return ts;
}
static void n_free(TypedSchema *ts){if(ts){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);}}
static int n_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);ASSERT_EQ_INT(rc,0,"encode fixture");return rc;
}
static void n_value(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *name,
                    const char *expected,size_t length,const char *json){
    size_t used=99;errno=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX,&used);
    ASSERT_NOT_NULL(out,"named text present");ASSERT_TRUE(out&&used==length&&!memcmp(out,expected,length)&&!out[length],"exact raw bytes and terminator");free(out);
    used=99;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,length+1,&used);
    ASSERT_TRUE(out&&used==length&&!memcmp(out,expected,length),"exact raw budget");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,length,&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"one-short raw budget");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX);
    ASSERT_EQ_STR(out,json,"named JSON exact");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,strlen(json)+1);
    ASSERT_EQ_STR(out,json,"exact JSON budget");free(out);
    errno=0;out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,strlen(json));
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short JSON budget");free(out);
    FieldSchema fs={.ts=ts};out=json_projected_field((const char*)bytes,n,name,&fs,PKR_FLAGS);
    ASSERT_EQ_STR(out,json,"shared named cell integration");free(out);
}
static void n_absent(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *name){
    size_t used=99;errno=ERANGE;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==0&&used==0,"absence distinct from error");free(out);
    out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,name,SIZE_MAX);ASSERT_EQ_STR(out,"null","absent JSON literal");free(out);
}
static void n_basics(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(n_encode(ts,"{\"f0\":\"1234567890123456.1234567890\",\"f1\":1e-26,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":true,\"f4\":0,\"f5\":null,\"f6\":\"yes\"}",&bytes,&n)){n_free(ts);return;}
    static const char binary[]={'a',0,'b','"','\\','\n'};
    n_value(ts,bytes,n,"f2",binary,sizeof(binary),"\"a\\u0000b\\\"\\\\\\n\"");
    n_value(ts,bytes,n,"f4+f6","0yes",4,"\"0yes\"");
    n_value(ts,bytes,n,"++f4+++absent+f5+f6+","0yes",4,"\"0yes\"");
    n_value(ts,bytes,n,"f0+f4","1234567890123456.12345678900",28,"\"1234567890123456.12345678900\"");
    n_value(ts,bytes,n,"f3+f6","trueyes",7,"\"trueyes\"");
    n_value(ts,bytes,n," f4+f4","0",1,"\"0\"");
    n_value(ts,bytes,n,"f4","0",1,"0");n_value(ts,bytes,n,"f3","true",4,"\"true\"");
    char joined[7];memcpy(joined,binary,sizeof(binary));joined[6]='0';
    n_value(ts,bytes,n,"f2+f4",joined,sizeof(joined),"\"a\\u0000b\\\"\\\\\\n0\"");
    n_absent(ts,bytes,n,"f5");n_absent(ts,bytes,n,"unknown");n_absent(ts,bytes,n,"f5+unknown");n_absent(ts,bytes,n,"+++");n_absent(ts,bytes,n,"");
    ts->fields[6].removed=1;n_absent(ts,bytes,n,"f6");n_value(ts,bytes,n,"f6+f4","0",1,"\"0\"");ts->fields[6].removed=0;
    char selector[310];memset(selector,'x',300);memcpy(selector+300,"+f4",4);
    n_value(ts,bytes,n,selector,"0",1,"\"0\"");
    char *out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f4+f6,f0,f5+unknown",SIZE_MAX);
    ASSERT_EQ_STR(out,"{\"f4+f6\":\"0yes\",\"f0\":1234567890123456.1234567890,\"f5+unknown\":null}","object scalar/composite policy");free(out);
    errno=0;out=typed_get_field_str(ts,bytes,(int)n,2,PKR_FLAGS);
    ASSERT_TRUE(!out&&errno==EILSEQ,"legacy raw getter stays unconverted");free(out);
    free(bytes);n_free(ts);
}
static void n_empty(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!n_encode(ts,"{\"f2\":\"\",\"f5\":\"\",\"f4\":0}",&bytes,&n)){
        n_value(ts,bytes,n,"f2","",0,"\"\"");n_absent(ts,bytes,n,"f2+f5");n_value(ts,bytes,n,"f2+f4","0",1,"\"0\"");free(bytes);}
    n_free(ts);
}
static void n_failures(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(n_encode(ts,"{\"f0\":1,\"f2\":\"abc\",\"f4\":0}",&bytes,&n)){n_free(ts);return;}
    const uint8_t flags[]={0,SREC_FLAG_NULLMAP,SREC_FLAG_PACKED,0x80};
    for(size_t i=0;i<sizeof(flags);i++){
        size_t used=99;errno=0;char *out=typed_packed_named_text(ts,bytes,n,flags[i],"f2+f4",SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==EILSEQ&&used==0,"raw generation mismatch");free(out);
        errno=0;out=typed_packed_named_json(ts,bytes,n,flags[i],"missing+f5",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"empty composite cannot bypass generation");free(out);
    }
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"bad descriptor fixture");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);errno=0;
        char *out=typed_packed_named_json(ts,bad,n,PKR_FLAGS,"f4+f4",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unselected corruption rejects");free(out);free(bad);}
    char many[(MAX_FIELDS+1)*3+1];size_t pos=0;
    for(int i=0;i<MAX_FIELDS;i++){memcpy(many+pos,"f4+",3);pos+=3;}many[pos-1]=0;
    char expected[MAX_FIELDS+1];memset(expected,'0',MAX_FIELDS);expected[MAX_FIELDS]=0;
    char json[MAX_FIELDS+3];json[0]='"';memcpy(json+1,expected,MAX_FIELDS);json[MAX_FIELDS+1]='"';json[MAX_FIELDS+2]=0;
    n_value(ts,bytes,n,many,expected,MAX_FIELDS,json);
    many[pos-1]='+';memcpy(many+pos,"f4",3);size_t used=99;errno=0;
    char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,many,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"component count bound");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,many,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==E2BIG,"object propagates component limit");free(out);
    char *longname=malloc(MAX_LINE+1);ASSERT_NOT_NULL(longname,"long name fixture");
    if(longname){memset(longname,'x',MAX_LINE);longname[MAX_LINE]=0;used=99;errno=0;
        out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,longname,SIZE_MAX,&used);
        ASSERT_TRUE(!out&&errno==E2BIG&&used==0,"selector byte bound");free(out);free(longname);}
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,NULL,SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL selector");free(out);
    errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f4",SIZE_MAX,NULL);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL length output");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,NULL,0,PKR_FLAGS,"f4",SIZE_MAX,&used);
    ASSERT_TRUE(!out&&errno==EINVAL&&used==0,"NULL record");free(out);
    used=99;errno=0;out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"missing",0,&used);
    ASSERT_TRUE(!out&&errno==ENOMEM&&used==0,"zero budget fails before absence");free(out);
    ts->fields[3].type=FT_LONG;errno=0;out=typed_packed_named_json(ts,bytes,n,PKR_FLAGS,"f4+f4",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"unselected bad fixed schema");free(out);ts->fields[3].type=FT_BOOL;
    free(bytes);n_free(ts);
}
static void n_limits(void){
    TypedSchema *ts=n_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!n_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){n_value(ts,bytes,n,"f0+f127","tiny",4,"\"tiny\"");free(bytes);}n_free(ts);
    ts=n_schema(0);if(!ts)return;size_t len=131072+1+16383;char *digits=malloc(len+1);ASSERT_NOT_NULL(digits,"full numeric fixture");
    if(digits){memset(digits,'1',len);digits[131072]='.';digits[len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare maximum numeric");TypedInput input={digits,len,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"set maximum numeric");int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate maximum numeric");
        if(!rc){size_t used=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f1+f1",2*len+1,&used);
            ASSERT_TRUE(out&&used==2*len&&!memcmp(out,digits,len)&&!memcmp(out+len,digits,len),"full composite numeric every digit");free(out);free(bytes);}
        typed_prepared_free(&p);free(digits);}
    len=4u*1024*1024;char *content=malloc(len);ASSERT_NOT_NULL(content,"maximum varchar fixture");
    if(content){memset(content,'x',len);content[1]=0;TypedPrepared p;typed_prepared_init(&p);ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare maximum varchar");
        TypedInput input={content,len,1,0,0},zero={"0",1,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"set maximum varchar");ASSERT_EQ_INT(typed_prepared_set(&p,4,&zero,NULL,0),0,"set zero component");
        int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate maximum varchar");
        if(!rc){size_t used=0;char *out=typed_packed_named_text(ts,bytes,n,PKR_FLAGS,"f2+f4",len+2,&used);
            ASSERT_TRUE(out&&used==len+1&&!memcmp(out,content,len)&&out[len]=='0',"maximum composite binary text intact");free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    n_free(ts);
}
static void n_chunks(void){
    TypedSchema *ts=n_schema(0);if(!ts)return;
    size_t one=8193,total=3*one,json_len=6*total+2;
    char *input=calloc(one,1),*expected=calloc(total+1,1),*json=malloc(json_len+1);
    ASSERT_NOT_NULL(input,"chunk input");ASSERT_NOT_NULL(expected,"chunk display expectation");ASSERT_NOT_NULL(json,"chunk JSON expectation");
    if(input&&expected&&json){
        json[0]='"';for(size_t i=0;i<total;i++)memcpy(json+1+6*i,"\\u0000",6);
        json[json_len-1]='"';json[json_len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare chunk record");
        TypedInput in={input,one,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&in,NULL,0),0,"set chunk bytes");
        uint8_t *bytes=NULL;size_t n=0;int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate chunk record");
        if(!rc){n_value(ts,bytes,n,"f2+f2+f2",expected,total,json);free(bytes);}
        typed_prepared_free(&p);
    }
    free(input);free(expected);free(json);n_free(ts);
}
static int test_packed_named_projection_run(void){n_basics();n_empty();n_failures();n_limits();n_chunks();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-named-projection",test_packed_named_projection_run)
```

### named-edit-1-old

<!-- block:named-edit-1-old -->
```c
char *typed_packed_project_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *fields_csv, size_t budget);
```

### named-edit-1-new

<!-- block:named-edit-1-new -->
```c
char *typed_packed_project_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *fields_csv, size_t budget);
/* Named packed display text accepts '+' components, skips unknown/NULL parts
   and returns exact bytes plus NUL. text_len excludes NUL; it resets on error.
   NULL/removed/unknown and an empty composite yield NULL with errno=0.
   Scalar empty varchar yields an owned empty string. Budget includes NUL.
   Names/components preserve whitespace; MAX_LINE-1/MAX_FIELDS are enforced.
   The JSON counterpart keeps scalar display policy and quotes a composite
   once, escaping its entire byte span. Errors are NULL with nonzero errno. */
char *typed_packed_named_text(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *name, size_t budget, size_t *text_len);
char *typed_packed_named_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *name, size_t budget);
```

### named-edit-2-old

<!-- block:named-edit-2-old -->
```c
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
```

### named-edit-2-new

<!-- block:named-edit-2-new -->
```c
char *typed_packed_named_text(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget,size_t *text_len){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)name;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}
char *typed_packed_named_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)name;(void)budget;
    errno=ENOTSUP;return NULL;
}
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
```

### named-edit-3-old

<!-- block:named-edit-3-old -->
```c
char *typed_packed_named_text(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget,size_t *text_len){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)name;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}
char *typed_packed_named_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t hdr2,const char *name,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)name;(void)budget;
    errno=ENOTSUP;return NULL;
}
```

### named-edit-3-new

<!-- block:named-edit-3-new -->
```c
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
static int named_text_open(const TypedRowView *view,const char *name,size_t budget,
                           NamedText *text){
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    size_t length=strnlen(name,MAX_LINE);if(length==MAX_LINE){errno=E2BIG;return -1;}
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
```

### named-edit-4-old

<!-- block:named-edit-4-old -->
```c
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const char *fields_csv,size_t budget){
    if(!packed_schema_ok(ts)||!fields_csv){errno=EINVAL;return NULL;}
    if(budget<3){errno=ENOMEM;return NULL;}
    size_t length=strnlen(fields_csv,MAX_LINE);
    if(length==MAX_LINE){errno=E2BIG;return NULL;}
    char *copy=malloc(length+1);if(!copy){errno=ENOMEM;return NULL;}
    memcpy(copy,fields_csv,length+1);
    struct {char *name;int index;} selected[MAX_FIELDS];size_t count=0;
    char *save=NULL,*token=strtok_r(copy,",",&save);char *out=NULL;
    while(token){
        if(count==MAX_FIELDS){errno=E2BIG;goto done;}
        if(strchr(token,'+')){errno=ENOTSUP;goto done;}
        selected[count].name=token;selected[count].index=typed_field_index(ts,token);
        count++;token=strtok_r(NULL,",",&save);
    }
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))goto done;
    size_t need=3; /* braces and NUL; requested order/duplicates are preserved */
    for(size_t i=0;i<count;i++){
        size_t name=0,part=0,sep=i?2:1;RowText text;
        if(quoted_length(selected[i].name,strlen(selected[i].name),&name))goto done;
        int rc=projection_text_open(&view,selected[i].index,&text);
        if(!rc)rc=row_text_length(&text,1,&part);
        row_text_free(&text);if(rc)goto done;
        if(name>SIZE_MAX-sep||part>SIZE_MAX-name-sep||need>SIZE_MAX-name-part-sep){errno=EOVERFLOW;goto done;}
        need+=name+part+sep;if(need>budget){errno=ENOMEM;goto done;}
    }
    out=malloc(need);if(!out){errno=ENOMEM;goto done;}
    size_t pos=0;out[pos++]='{';
    for(size_t i=0;i<count;i++){
        size_t used=0;if(i)out[pos++]=',';
        if(quote_into(selected[i].name,strlen(selected[i].name),out+pos,need-pos,&used))goto fail;
        pos+=used;out[pos++]=':';RowText text;
        int rc=projection_text_open(&view,selected[i].index,&text);
        if(!rc)rc=row_text_write(&text,1,out+pos,need-pos,&used);
        row_text_free(&text);if(rc)goto fail;pos+=used;
    }
    out[pos++]='}';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    free(copy);return out;
fail:free(out);out=NULL;
done:free(copy);return out;
}
```

### named-edit-4-new

<!-- block:named-edit-4-new -->
```c
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const char *fields_csv,size_t budget){
    if(!packed_schema_ok(ts)||!fields_csv){errno=EINVAL;return NULL;}
    if(budget<3){errno=ENOMEM;return NULL;}
    size_t length=strnlen(fields_csv,MAX_LINE);
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
```

### named-edit-5-old

<!-- block:named-edit-5-old -->
```c
    if(fs&&fs->ts&&fs->ts->packed.nfields){
        errno=0;
        if(!field){errno=EINVAL;return NULL;}
        if(strchr(field,'+')){errno=ENOTSUP;return NULL;}
        int index=typed_field_index(fs->ts,field);
        if(index>=0)return typed_packed_projected_cell(fs->ts,(const uint8_t*)raw,
            raw_len,hdr2,(size_t)index,SIZE_MAX);
        TypedRowView view;if(typed_row_open(fs->ts,(const uint8_t*)raw,raw_len,hdr2,&view))return NULL;
        char *out=malloc(5);if(!out){errno=ENOMEM;return NULL;}memcpy(out,"null",5);return out;
    }
```

### named-edit-5-new

<!-- block:named-edit-5-new -->
```c
    if(fs&&fs->ts&&fs->ts->packed.nfields)
        return typed_packed_named_json(fs->ts,(const uint8_t*)raw,raw_len,hdr2,field,SIZE_MAX);
```

### named-edit-6-old

<!-- block:named-edit-6-old -->
```c
    ASSERT_TRUE(!out&&errno==ENOTSUP,"packed composite named helper refuses");free(out);
```

### named-edit-6-new

<!-- block:named-edit-6-new -->
```c
    ASSERT_EQ_STR(out,"\"a\\u0000b\\\"\\\\\\n1234567890123456.1234567890\"","packed composite named helper exact");free(out);
```

### named-edit-7-old

<!-- block:named-edit-7-old -->
```c
    ASSERT_TRUE(!out&&errno==ENOTSUP,"mixed composite request refuses entirely");free(out);
```

### named-edit-7-new

<!-- block:named-edit-7-new -->
```c
    ASSERT_EQ_STR(out,"{\"f4\":0,\"f2+f0\":\"a\\u0000b\\\"\\\\\\n1234567890123456.1234567890\"}","mixed composite request exact");free(out);
```

### named-edit-8-old

<!-- block:named-edit-8-old -->
```c
   Packed composites containing '+' refuse ENOTSUP until integrated. */
```

### named-edit-8-new

<!-- block:named-edit-8-new -->
```c
   Packed composites quote the exact concatenated display bytes once. */
```

### named-edit-9-old

<!-- block:named-edit-9-old -->
```c
    src/test/cases/test_packed_json_projection.c \
```

### named-edit-9-new

<!-- block:named-edit-9-new -->
```c
    src/test/cases/test_packed_json_projection.c \
    src/test/cases/test_packed_named_projection.c \
```

### named-edit-10-old

<!-- block:named-edit-10-old -->
```c
static int quote_into(const char *data,size_t n,char *out,size_t cap,size_t *used){
    size_t need=0;if(quoted_length(data,n,&need))return -1;
    if(need>cap){errno=EOVERFLOW;return -1;}
    out[0]='"';int count=json_escape_into(out+1,need-2,data,n);
    if(count<0||(size_t)count!=need-2){errno=EOVERFLOW;return -1;}
    out[need-1]='"';*used=need;return 0;
}
```

### named-edit-10-new

<!-- block:named-edit-10-new -->
```c
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
```

### named-doc

<!-- block:named-doc -->
```markdown
# Packed named display text and composite JSON (public format inactive)

`typed_packed_named_text` returns owned display bytes and an authoritative length
excluding its terminating NUL. It accepts one name or '+' concatenation, validates
one complete packed view first, and never routes through legacy fixed offsets.
The caller must consume text_len, not strlen: varchar and enum display text can
contain bytes needing JSON escaping, and varchar can contain embedded NUL.
Errors return NULL with nonzero errno and reset text_len to zero. Legitimate
NULL/removed/unknown scalar and a composite with zero combined display bytes
return NULL with errno zero. Scalar empty varchar is an owned empty string.
Budgets include NUL; zero budget rejects even absence. Legacy raw APIs remain
unchanged and do not gain packed dispatch.

`typed_packed_named_json` uses the same explicit view, rendering policy and
checked ownership. Scalars preserve existing projection policy: exact numeric
text is bare, boolean is quoted, strings are escaped, NULL is literal null.
A composite is always one quoted display string, irrespective of numeric parts;
its entire concatenation is escaped once. Components are joined in order with
no separator; unknown/removed/NULL parts are skipped, zero/false are real text.
An empty concatenation produces literal null, including all-empty varchar parts,
matching the established legacy concatenation convention. Embedded NUL and its
suffix survive as escaped JSON bytes. Full-record boolean behavior is separate.

Both selectors are NUL-terminated and bounded by MAX_LINE-1 bytes. '+' is the
composite separator; empty '+' tokens collapse, whitespace is significant,
and at most MAX_FIELDS nonempty components are allowed, including unknown names.
No 255-byte selector truncation is inherited from legacy decode_field. Limits
reject E2BIG; malformed views reject EILSEQ; invalid API/schema arguments reject
EINVAL; allocation/budget failure rejects ENOMEM; arithmetic overflow rejects
EOVERFLOW. Legitimate missing output must be distinguished using errno/text_len.
The APIs do not promise errno zero for non-NULL successful output.

Packed quoting feeds the int-returning escape backend pieces of at most4096
input bytes, each emitting at most24576 bytes, while totals remain size_t.
Repeated components may therefore escape beyond INT_MAX without narrowing the
complete count or imposing an undocumented response ceiling.

The shared packed JSON projection helper and complete projected GET object now
use named rendering. Their former packed-composite ENOTSUP gate is deliberately
removed. Object selector comma parsing still has its existing MAX_LINE/MAX_FIELDS
limits and preserves order/duplicates; every selection remains unpublished until
the whole object is built. A component allocation failure aborts the whole cell/
object, never skips a failed component or emits a successful shorter value.
The unchanged projected GET command checks writes and its newline; a FILE may
accept a prefix before failing, so no transactional I/O guarantee is claimed.

Memory is actual selected output plus NUL and, for a composite, one exact raw
concatenation workspace plus bounded scalar/decimal workspaces. During JSON
rendering that raw concatenation coexists with cell/object output. Object sizing
and writing each create and release that temporary independently. Duplicate
components can make selected output larger than the stored record; the bound
is the caller's explicit output budget, not original vlen. No schema-maximum
allocation, whole unselected-varchar JSON copy, speedup or QUERY_BUFFER_MB
integration is claimed. Production JSON adapters retain SIZE_MAX while packed
binding remains inactive; activation still requires an enclosing memory policy.

This is output-only integration. typed_get_field_str, decode_field, CSV, matching,
sorting, cursor capture, aggregate grouping, join building and surrounding query
error handling retain their own contracts. Teaching their strlen callers to
accept binary data would truncate it; no such change occurs here. The new raw
named API has no production consumer. Loaders/create-object still never bind
packed schemas; no metadata/version/migration/default-format change is made.
Private tests and actual-source mocked command probes prove this boundary, not
public packed network/query acceptance or concurrency safety of activation.
```

### projection-doc-old

<!-- block:projection-doc-old -->
```markdown
Unknown/removed names map to null. Oversized selectors reject with E2BIG rather
than being silently truncated in this private helper. Any '+' token refuses
ENOTSUP for the whole packed projection, because legacy composite concatenation
uses strlen and cannot safely handle binary varchar. Legacy composites and
legacy selector parsing are unchanged. Upstream JSON-to-CSV parsing still has
its existing limits/unescaping behavior; this helper does not repair ingress.
```

### projection-doc-new

<!-- block:projection-doc-new -->
```markdown
Unknown/removed names map to null. Oversized selectors reject with E2BIG rather
than being silently truncated in this private helper. Packed '+' projections now
use [length-aware named display text](packed-named-projection.md): skip absent
parts, concatenate exact bytes in order, then quote/escape once. This replaces
the former private ENOTSUP restriction. Legacy raw-string composites/parsing
remain unchanged; upstream JSON-to-CSV parsing retains its own limits/unescaping
behavior. This helper does not repair ingress or activate packed queries.
```

### adapter-doc-old

<!-- block:adapter-doc-old -->
```markdown
CSV/composite, query-envelope and raw-offset consumers still require conversion.
Passing a bound schema to a legacy raw-offset API is unsafe. The ordinary loader
```

### adapter-doc-new

<!-- block:adapter-doc-new -->
```markdown
Legacy composite/CSV, query-envelope and raw-offset consumers still require
conversion. [Packed named/composite JSON](packed-named-projection.md) has its
own checked length-aware output path. Passing a bound schema to a legacy
raw-offset API is unsafe. The ordinary loader
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
    if git('rev-parse','HEAD')!=m['base_head']:raise ValueError('HEAD differs from reviewed 3B.7 commit')
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
    message='Phase 3B.8 execution halted: '+str(e)+'\nNo later task may run. Planner/human must review this note before resuming.\n'
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
    return p.stdout
def check(work,probe,row,query,storage,expected):
    source=storage.read_text();start=source.index('int cmd_get_fields(')
    end=source.index('\n/*',start);command=work/'cmd_get_fields.inc';command.write_text(source[start:end])
    objects=[]
    for name in ['typed_row','query_find','packed_record','decnum']:
        obj=work/(name+'.o');objects.append(str(obj))
        src=row if name=='typed_row' else query if name=='query_find' else pathlib.Path('src/db/'+name+'.c')
        run(['gcc','-O1','-g','-Isrc/db','-Dmalloc=p8_malloc','-Dcalloc=p8_calloc',
             '-Drealloc=p8_realloc','-Dfree=p8_free','-Djson_escape_into=p8_escape','-c',str(src),'-o',str(obj)])
    binary=work/'projection-probe'
    run(['gcc','-O1','-g','-Isrc/db','-DP8_CMD_SOURCE="'+str(command)+'"',str(probe),
         *objects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-lm','-o',str(binary)])
    output=run([str(binary)],expected)
    want=(21,17) if expected else (258,0)
    actual=(output.count(': PASS\n'),output.count(': FAIL\n'))
    if actual!=want:raise RuntimeError('probe gate totals '+str(actual)+' expected '+str(want))

def main():
    a=argparse.ArgumentParser();a.add_argument('--plan',type=pathlib.Path,required=True)
    a.add_argument('--mode',choices=['red','green','regression-proof'],required=True);args=a.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b8-check-'));print('scratch:',work,flush=True)
    probe=work/'probe.c';probe.write_text(extract(args.plan,'projection-probe'))
    row=pathlib.Path('src/db/typed_row.c');query=pathlib.Path('src/db/query_find.c');storage=pathlib.Path('src/db/storage.c')
    if args.mode!='regression-proof':
        check(work,probe,row,query,storage,1 if args.mode=='red' else 0);return
    manifest=json.loads(extract(args.plan,'manifest'));digest=lambda b:hashlib.sha256(b).hexdigest()
    copies={}
    for path in [str(row),str(query),str(storage)]:
        data=pathlib.Path(path).read_bytes()
        if digest(data)!=manifest['checkpoints']['2'].get(path,manifest['parent_sources'][path]):raise ValueError('fixed source mismatch '+path)
        copies[path]=data
    edits=[e for e in manifest['edits']['2'] if e['path'] in copies]
    for e in reversed(edits):
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(new)!=1:raise ValueError('reverse anchor '+e['new'])
        copies[e['path']]=data.replace(new,old)
    for path,data in copies.items():
        if digest(data)!=manifest['checkpoints']['1'].get(path,manifest['parent_sources'][path]):raise ValueError('baseline hash mismatch '+path)
        (work/pathlib.Path(path).name).write_bytes(data)
    check(work,probe,work/'typed_row.c',work/'query_find.c',work/'storage.c',1)
    for e in edits:
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(old)!=1:raise ValueError('reapply anchor '+e['old'])
        copies[e['path']]=data.replace(old,new)
    for path,data in copies.items():
        if digest(data)!=manifest['checkpoints']['2'].get(path,manifest['parent_sources'][path]):raise ValueError('reapply hash mismatch '+path)
        (work/pathlib.Path(path).name).write_bytes(data)
    check(work,probe,work/'typed_row.c',work/'query_find.c',work/'storage.c',0)
    print('scratch revert/reapply proof passed; checkout source and binaries untouched')
if __name__=='__main__':main()
```

### projection-probe

<!-- block:projection-probe -->
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
void *p8_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *p8_calloc(size_t a,size_t b){if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}if(!allow(a*b))return NULL;void *p=calloc(a,b);remember(p);return p;}
void *p8_realloc(void *p,size_t n){if(!allow(n))return NULL;int i=find(p);void *q=realloc(p,n);if(q){if(i>=0)owned[i]=owned[--live];remember(q);}return q;}
void p8_free(void *p){int i=find(p);if(i>=0)owned[i]=owned[--live];free(p);}
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
static void probe_fields(FieldSchema *fs,const char *root,const char *object){(void)root;(void)object;memset(fs,0,sizeof(*fs));fs->ts=fixture;}
#define init_field_schema probe_fields
#define cmd_get_fields probe_cmd_get_fields
#define load_schema probe_schema
#define load_typed_schema probe_typed
#define slotcask_registry_get probe_registry
#define slotcask_registry_ref_release probe_release
#define slotcask_get_ex probe_get
#define free p8_free
#include P8_CMD_SOURCE
#undef free
#undef init_field_schema
#undef cmd_get_fields
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

static size_t escape_largest;
int p8_escape(char *dst,size_t cap,const char *src,size_t n){
    if(n>escape_largest)escape_largest=n;
    return json_escape_into(dst,cap,src,n);
}
static int failures;
static const char *fields="f0+f0,f1+f0,missing+f2";
static void gate(const char *what,int ok){printf("%s: %s\n",what,ok?"PASS":"FAIL");if(!ok)failures++;}
static char *cell(int mode,size_t *used){
    FieldSchema fs={.ts=fixture};
    if(mode==0)return typed_packed_named_text(fixture,record,record_len,generation,"f0+f0",SIZE_MAX,used);
    if(mode==1)return typed_packed_named_json(fixture,record,record_len,generation,"f0+f0",SIZE_MAX);
    if(mode==2)return json_projected_field((const char*)record,record_len,"f1+f0",&fs,generation);
    return typed_packed_named_text(fixture,record,record_len,generation,"f1+f0",SIZE_MAX,used);
}
static void cells(void){
    static const char expected0[]="1.25000000001.2500000000",expected3[]="a\0b1.2500000000";
    const char *expected[]={expected0,"\"1.25000000001.2500000000\"","\"a\\u0000b1.2500000000\"",expected3};
    const size_t lengths[]={sizeof(expected0)-1,26,22,sizeof(expected3)-1};
    for(int mode=0;mode<4;mode++){
        calls=0;nth=0;armed=1;errno=0;size_t used=99;char *out=cell(mode,&used);size_t count=calls;armed=0;
        size_t length=(mode==0||mode==3)?lengths[mode]:strlen(expected[mode]);
        gate("composite cell exact",out&&!memcmp(out,expected[mode],length)&&!out[length]&&((mode==0||mode==3)?used==length:1));p8_free(out);
        gate("allocation sweep nonempty",count>0);gate("normal buffers released",live==0);
        for(size_t i=1;i<=count;i++){
            calls=0;nth=i;armed=1;errno=0;used=99;out=cell(mode,&used);int cause=errno;armed=0;
            gate("component failure explicit",!out&&cause==ENOMEM&&((mode==0||mode==3)?used==0:1));p8_free(out);gate("failed buffers released",live==0);
        }
        printf("mode=%d allocation injections=%zu\n",mode,count);
    }
}
static int invoke(size_t fail,char **out,int *cause){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);MUST(fp);FILE *save=g_out;g_out=fp;
    calls=0;nth=fail;released=0;armed=1;int rc=probe_cmd_get_fields("root","obj","a",1,fields);
    *cause=errno;armed=0;g_out=save;MUST(!fclose(fp));
    gate("command buffers released",live==0);gate("registry reference released once",released==1);return rc;
}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f){(void)s;(void)k;(void)f;return remove(p);}
int main(void){
    char root[]="/tmp/p3b8-probe-XXXXXX";MUST(mkdtemp(root));ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=3;const char *types[]={"numeric:26,10","varchar","bool"};
    for(int i=0;i<3;i++){if(i==0)ts->fields[i].type=FT_NUMERIC;else if(i==1)ts->fields[i].type=FT_VARCHAR;else parse_field_type("bool",&ts->fields[i]);snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    MUST(!typed_schema_bind_packed(ts,types,3,NULL,0));uint8_t *bytes=NULL;size_t n=0;
    MUST(!typed_encode_alloc(ts,"{\"f0\":1.25,\"f1\":\"a\\u0000b\",\"f2\":true}",1,NULL,NULL,&bytes,&n,NULL,0));
    fixture=ts;record=bytes;record_len=n;generation=PKR_FLAGS;cells();
    const char *body="{\"f0+f0\":\"1.25000000001.2500000000\",\"f1+f0\":\"a\\u0000b1.2500000000\",\"missing+f2\":\"true\"}";
    const char *expected="{\"f0+f0\":\"1.25000000001.2500000000\",\"f1+f0\":\"a\\u0000b1.2500000000\",\"missing+f2\":\"true\"}\n";
    char *text=NULL;int cause=0;int rc=invoke(0,&text,&cause);size_t count=calls;
    gate("projected GET composite exact",rc==0&&text&&!strcmp(text,expected));free(text);gate("command sweep nonempty",count>0);
    for(size_t i=1;i<=count;i++){
        rc=invoke(i,&text,&cause);gate("command allocation failure explicit",rc==1&&cause==ENOMEM);
        gate("one error no partial success",text&&!strcmp(text,"{\"error\":\"out of memory\"}\n"));free(text);
    }
    printf("command allocation injections=%zu\n",count);
    fields="missing+absent";rc=invoke(0,&text,&cause);gate("empty composite is null",rc==0&&text&&!strcmp(text,"{\"missing+absent\":null}\n"));free(text);
    char many[(MAX_FIELDS+1)*3+1];size_t pos=0;for(int i=0;i<MAX_FIELDS+1;i++){memcpy(many+pos,"f2+",3);pos+=3;}many[pos-1]=0;fields=many;
    rc=invoke(0,&text,&cause);gate("component limit command error",rc==1&&cause==E2BIG&&text&&!strcmp(text,"{\"error\":\"projection exceeds limit\"}\n"));free(text);
    fields="f2+f2";uint8_t *bad=malloc(n);MUST(bad);memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[1].offset,255,4);record=bad;
    rc=invoke(0,&text,&cause);gate("unselected corruption still rejects",rc==1&&cause==EILSEQ&&text&&!strcmp(text,"{\"error\":\"corrupt record\"}\n"));free(text);free(bad);record=bytes;
    fields="f0+f0,f1+f0,missing+f2";
    for(int newline=0;newline<2;newline++){
        Writer writer={.limit=newline?strlen(body):7};FILE *badout=writer_open(&writer);MUST(badout);MUST(!setvbuf(badout,NULL,_IONBF,0));
        FILE *save=g_out;g_out=badout;released=0;rc=probe_cmd_get_fields("root","obj","a",1,fields);g_out=save;
        gate(newline?"newline failure returned":"partial write failure returned",rc==1&&released==1);gate("no replacement error after I/O prefix",writer.accepted==writer.limit&&!writer.error_attempted);fclose(badout);
    }
    TypedSchema *maximum=calloc(1,sizeof(*maximum));MUST(maximum);maximum->typed=1;maximum->nfields=MAX_FIELDS;const char *tokens[MAX_FIELDS];
    for(int i=0;i<MAX_FIELDS;i++){tokens[i]="varchar";maximum->fields[i].type=FT_VARCHAR;snprintf(maximum->fields[i].name,sizeof(maximum->fields[i].name),"f%d",i);maximum->fields[i].name_len=(int)strlen(maximum->fields[i].name);}
    MUST(!typed_schema_bind_packed(maximum,tokens,MAX_FIELDS,NULL,0));MUST(maximum->total_size>1000000000);
    uint8_t *small=NULL;size_t small_n=0;MUST(!typed_encode_alloc(maximum,"{\"f127\":\"tiny\"}",1,NULL,NULL,&small,&small_n,NULL,0));
    fixture=maximum;record=small;record_len=small_n;fields="f0+f127,absent+f1";largest=0;rc=invoke(0,&text,&cause);
    gate("maximum schema actual sizing",rc==0&&small_n<4096&&largest<4096&&text&&!strcmp(text,"{\"f0+f127\":\"tiny\",\"absent+f1\":null}\n"));printf("maximum actual=%zu largest=%zu live=%zu\n",small_n,largest,live);free(text);free(small);free(maximum);
    fixture=ts;size_t one=8193;char *zeros=calloc(one,1);MUST(zeros);
    TypedPrepared prepared;typed_prepared_init(&prepared);MUST(!typed_prepared_zero(ts,&prepared,NULL,0));
    TypedInput in={zeros,one,1,0,0};MUST(!typed_prepared_set(&prepared,1,&in,NULL,0));
    uint8_t *chunk=NULL;size_t chunk_n=0;MUST(!typed_prepared_alloc(&prepared,&chunk,&chunk_n,NULL,0));typed_prepared_free(&prepared);free(zeros);
    escape_largest=0;char *quoted=typed_packed_named_json(ts,chunk,chunk_n,PKR_FLAGS,"f1+f1+f1",SIZE_MAX);
    gate("multi-chunk escaped output complete",quoted&&strlen(quoted)==3*one*6+2&&!memcmp(quoted,"\"\\u0000",7)&&quoted[3*one*6+1]=='"');
    gate("backend int count always bounded",escape_largest>0&&escape_largest<=4096);
    printf("largest escape input=%zu\n",escape_largest);p8_free(quoted);free(chunk);
    printf("probe failures=%d\n",failures);free(bytes);free(ts);shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return failures?1:0;
}
```

### manifest

<!-- block:manifest -->
```json
{
  "base_head": "cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7",
  "ancestor": "cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "parent_sources": {
    "build.sh": "238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e",
    "src/db/typed_row.h": "c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602",
    "src/db/typed_row.c": "71d5e928af46a46fe304bf8f34b87831d0f465c758971699a98bf06c154411b1",
    "src/db/query_find.c": "8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc",
    "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
    "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
    "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
    "src/db/packed_record.c": "8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d",
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
    "src/test/cases/test_packed_json_projection.c": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe"
  },
  "blocks": {
    "named-test": "24a57573b0b88a432cef3dd3fe8ff224262e7c67122b5e91142d4831e03ee71c",
    "named-edit-1-old": "599ac2cb80dd54d3681337b0b3aa18fdc38a40519ceb83b53ee9497022a65fb6",
    "named-edit-1-new": "e840f83c59a8b0cf7001133d7f9e3e682f02d01ce47b1032b947e28bdd894303",
    "named-edit-2-old": "79afdc66398ed4555645227c830b49f3ebb4d7933331743c19916501409bb3a6",
    "named-edit-2-new": "e5f1cc9da895353644564e436ca57fc9eb70946f83c4446f574c9187f4b5ee83",
    "named-edit-3-old": "8e07c507231c03b36432cbac958969fcd208e7733952e7bfcdfdd9e96066f96f",
    "named-edit-3-new": "45b68da748f258067cddc016ca035e34ae05d109f653a8ca6c9659f1afcb6337",
    "named-edit-4-old": "c2c87492cf4f1f404d2e3eca30c3a9ad7fe04e3ae35837e974b194b6dcab2751",
    "named-edit-4-new": "97cdf9e58314e078ca6d125d30e8ae13f2b25ad38b957f2f746f9262aa53de90",
    "named-edit-5-old": "36211e04957d0d2f17b9f0ed67d38f174077c8e07bd20e476722432bd386c486",
    "named-edit-5-new": "a6f588439c57337278bca5845af7e236cc6d0479b6d7504c45bc44dd7fb9e7a7",
    "named-edit-6-old": "c2e57b47aed7de47abfb3e5b408adbed7024cdb399ed92c505df7a7969c5e336",
    "named-edit-6-new": "81e2362369a9e33419119a1d84f9dde8cd88bb807511a4a3d9e1f37039235da8",
    "named-edit-7-old": "fbb3e7eefe0d3247d4d40b1adc181ee21f3d620b12f13c5215e46ebc227f0ece",
    "named-edit-7-new": "dcf0a3410c2e72cce0451e01136d7ae62bf6c31e7475bcd6661083a662e52c0c",
    "named-edit-8-old": "a86d182a815eb5d79874acacd4c10595ea833b659342250db6c4b85b04f5b58c",
    "named-edit-8-new": "efce406d502f8c757249582995fb1bf610fcf0ebd236d156498c590f4ebed2bc",
    "named-edit-9-old": "9c93bcb365fd2a33e66a5617bc320960c3e9bc4e70f773fbd1f54e3cd62179f6",
    "named-edit-9-new": "5e6869cb96000ee70657608182ffd6b2b92e1508630ef4ca12d9aa414e0b302b",
    "named-edit-10-old": "d5821a9ab593543718cda6531b0ac0b8f1b830ba55dc03d69f251f92850c6b4f",
    "named-edit-10-new": "74c2d2305a3b459ac20b33208df46adc9f241dc85bbe45dacbeb60f475946ee5",
    "named-doc": "54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284",
    "projection-doc-old": "a93c63d8a8ff10beb3cfb7cce0f136c8528d91c4f7434268b6f9de549c2f304c",
    "projection-doc-new": "0746988c7bd8f69831bcd40abfb6d978140eb7bd94df81cf450cd4d799116637",
    "adapter-doc-old": "d8ee3c7ccd03b8b8388c74f02063c0d9785e0292f1620ce08782512ba35f4e4e",
    "adapter-doc-new": "33bb2d33b650eb3f44d7cfa528dd248d9d605f3c80c9a2db541e39431d6720a7",
    "driver": "838009277092e23455271897c97fc9c368ee829ff3ce6489320a931a3c040ca5",
    "checks": "daa6075230cc17fabbb57022f1fb84c1bf40337a8536eb55dcf6454ced1f6cc4",
    "projection-probe": "ebfa9691a7ca808a0610d7acbe50d908e9a2b52ce68b8d25c5665105b6081478",
    "bootstrap": "5dbe194bf10e841ae2bb1e62a8cfa44e3947d822650959b624b72fb7786e4709"
  },
  "edits": {
    "1": [
      {
        "path": "src/test/cases/test_packed_named_projection.c",
        "old": null,
        "new": "named-test"
      },
      {
        "path": "src/db/typed_row.h",
        "old": "named-edit-1-old",
        "new": "named-edit-1-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "named-edit-2-old",
        "new": "named-edit-2-new"
      },
      {
        "path": "src/test/cases/test_packed_json_projection.c",
        "old": "named-edit-6-old",
        "new": "named-edit-6-new"
      },
      {
        "path": "src/test/cases/test_packed_json_projection.c",
        "old": "named-edit-7-old",
        "new": "named-edit-7-new"
      },
      {
        "path": "build.sh",
        "old": "named-edit-9-old",
        "new": "named-edit-9-new"
      }
    ],
    "2": [
      {
        "path": "src/db/typed_row.c",
        "old": "named-edit-3-old",
        "new": "named-edit-3-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "named-edit-4-old",
        "new": "named-edit-4-new"
      },
      {
        "path": "src/db/query_find.c",
        "old": "named-edit-5-old",
        "new": "named-edit-5-new"
      },
      {
        "path": "src/db/typed_row.h",
        "old": "named-edit-8-old",
        "new": "named-edit-8-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "named-edit-10-old",
        "new": "named-edit-10-new"
      }
    ],
    "3": [
      {
        "path": "docs/concepts/packed-named-projection.md",
        "old": null,
        "new": "named-doc"
      },
      {
        "path": "docs/concepts/packed-json-projection.md",
        "old": "projection-doc-old",
        "new": "projection-doc-new"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "adapter-doc-old",
        "new": "adapter-doc-new"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "build.sh": "238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e",
      "docs/concepts/packed-json-projection.md": "8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62",
      "docs/concepts/packed-named-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "736ba37fda8ac618815989e0eb452d859422885e8c147f89b56d36938ee45a6a",
      "src/db/query_find.c": "8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc",
      "src/db/typed_row.c": "71d5e928af46a46fe304bf8f34b87831d0f465c758971699a98bf06c154411b1",
      "src/db/typed_row.h": "c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602",
      "src/test/cases/test_packed_json_projection.c": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe",
      "src/test/cases/test_packed_named_projection.c": null
    },
    "1": {
      "build.sh": "cefe04d25ba0b9fb2df3a37daab53a98c271081b61d2f1a53203dd84a67a50c1",
      "docs/concepts/packed-json-projection.md": "8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62",
      "docs/concepts/packed-named-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "736ba37fda8ac618815989e0eb452d859422885e8c147f89b56d36938ee45a6a",
      "src/db/query_find.c": "8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc",
      "src/db/typed_row.c": "87f60edc181f4841ce8bdf275a16f79bc17744fe86b79c7770c3b58ebf4bd571",
      "src/db/typed_row.h": "1bd5f039cc3f5fd12b63a4dd8add0c40f8cd0f3409928e29ebb19b6d77d6d215",
      "src/test/cases/test_packed_json_projection.c": "fb517576276f5b952b8bf6147d59eff7070789983156bcc7350edb8993894e16",
      "src/test/cases/test_packed_named_projection.c": "24a57573b0b88a432cef3dd3fe8ff224262e7c67122b5e91142d4831e03ee71c"
    },
    "2": {
      "build.sh": "cefe04d25ba0b9fb2df3a37daab53a98c271081b61d2f1a53203dd84a67a50c1",
      "docs/concepts/packed-json-projection.md": "8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62",
      "docs/concepts/packed-named-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "736ba37fda8ac618815989e0eb452d859422885e8c147f89b56d36938ee45a6a",
      "src/db/query_find.c": "0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a",
      "src/db/typed_row.c": "001ef5983f43c1df8b1b53fff32e1b7311cd6fae0c550a578b5361356d758267",
      "src/db/typed_row.h": "0b56077eaf8f14f70a17460eac915d2e9eeb2a7ffcde409eaedf0b64b6c3129a",
      "src/test/cases/test_packed_json_projection.c": "fb517576276f5b952b8bf6147d59eff7070789983156bcc7350edb8993894e16",
      "src/test/cases/test_packed_named_projection.c": "24a57573b0b88a432cef3dd3fe8ff224262e7c67122b5e91142d4831e03ee71c"
    },
    "3": {
      "build.sh": "cefe04d25ba0b9fb2df3a37daab53a98c271081b61d2f1a53203dd84a67a50c1",
      "docs/concepts/packed-json-projection.md": "d4ebc9b21baf3aff076ec79404de710768b152efa83288af6539ce8b27b5421a",
      "docs/concepts/packed-named-projection.md": "54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284",
      "docs/concepts/typed-row-adapter.md": "78cbe03002f468649a89bb3b08baeff9dd6b94c9ce15a839cc76491350652eae",
      "src/db/query_find.c": "0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a",
      "src/db/typed_row.c": "001ef5983f43c1df8b1b53fff32e1b7311cd6fae0c550a578b5361356d758267",
      "src/db/typed_row.h": "0b56077eaf8f14f70a17460eac915d2e9eeb2a7ffcde409eaedf0b64b6c3129a",
      "src/test/cases/test_packed_json_projection.c": "fb517576276f5b952b8bf6147d59eff7070789983156bcc7350edb8993894e16",
      "src/test/cases/test_packed_named_projection.c": "24a57573b0b88a432cef3dd3fe8ff224262e7c67122b5e91142d4831e03ee71c"
    }
  }
}
```

## Appendix C — original planning verification (historical)

No Phase3B.8 implementation was written to the root checkout. Scratch copy
`/tmp/p3b8-plan-jowcaa_x` was a git archive of exact committed cfc4970; the
protected untracked document was never copied/read. All compilation was normal,
not sanitizer/benchmark execution.

| Gate | Actual result / raw log |
|---|---|
| Stage1 build |zero warnings, exit0; /tmp/p3b8-red-build.log |
| New focused RED |30/150, exit150; /tmp/p3b8-red-new.log |
| Existing projection RED |216/2, exit2; /tmp/p3b8-red-old.log |
| Actual-source command/allocator RED |21/15, expected probe exit1; /tmp/p3b8-red-probe.log |
| Final code build |zero warnings, exit0; /tmp/p3b8-green-build.log |
| New focused GREEN |180/0, exit0; /tmp/p3b8-green-new.log |
| Prior projection GREEN |218/0, exit0; /tmp/p3b8-green-old.log |
| Allocator/command GREEN |256/0, all77 injections; /tmp/p3b8-green-probe.log |
| Frozen scratch reverse/reapply |checkpoint1/parent hashes,21/15; checkpoint2/parent hashes,256/0; /tmp/p3b8-proof.log |
| Fresh full normal suite |20443/0 across526 cases, exit0; /tmp/p3b8-full.log |
| Frozen final green checks |256/0, same77 injections; /tmp/p3b8-frozen-green.log |
| Root driver stage1 --check-only |five files preflight verified, no writes |
| Manifest/source verification |all29 blocks and Python scripts; every unique anchor and all nine final scratch file hashes verified |

The complete full suite ran the exact final source before the documentation
stage; final docs were then applied and byte-verified. The executor still runs
Task3's new full suite after docs in the specified order. Frozen reversal uses
copied source only and includes the real unchanged storage command. Runtime
counts/bytes above are measured, not merely forecasts.

Before freezing the plan, the planner corrected one hand-written decimal test
length (the numeric value plus its appended zero has28 characters), and expanded
the two old-test replacement anchors to their complete physical lines. Both
corrections preceded the frozen plan; the driver now preflights them uniquely.
No assertion was weakened after RED and no implementation failure was hidden.
The raw display APIs were not wired to legacy matching/sort/group/CSV consumers.
No public cache binding, test-only production seam, on-disk/version change or
certification action occurred. The root tree after planning contains only this
new child plan and work-index planning edits on top of committed Phase3B.7.

## Appendix D — Revision 2 review verification (current)

Revision2's frozen code, commands/checkpoints and gate counts supersede the
historical Appendix C. Original scratch code results remain historical evidence;
they did not establish that Appendix A's original published plan paths were right.
All eight current --plan arguments now target this document, and bootstrap/script
paths plus the root stage1 --check-only command were verified as published.

| Gate | Actual result / raw log |
|---|---|
| Original quote count-bound regression |257/1, largest backend input24579; /tmp/p3b8-r2-quote-red.log |
| Fresh amended stage1 build |zero warnings, exit0; /tmp/p3b8-r2-red-build.log |
| New focused RED |38/158, exit158; /tmp/p3b8-r2-red-new.log |
| Prior projection RED |216/2, exit2; /tmp/p3b8-r2-red-old.log |
| Frozen RED probe |21/17, expected exit1; /tmp/p3b8-r2-red-probe.log |
| Repaired normal build |zero warnings, exit0; /tmp/p3b8-r2-build.log |
| New focused GREEN |196/0, exit0; /tmp/p3b8-r2-green-new.log |
| Prior projection GREEN |218/0, exit0; /tmp/p3b8-r2-green-old.log |
| Frozen GREEN probe |258/0,77 injections, backend input<=4096; /tmp/p3b8-r2-green-probe.log |
| Frozen source reversal/reapply |checkpoint1/parent hashes,21/17; checkpoint2/parent hashes,258/0; /tmp/p3b8-r2-proof.log |
| Fresh full suite after final docs |20459/0 across526 cases, exit0; /tmp/p3b8-r2-full.log |
| Independent repair checks |all31 frozen blocks, unique anchors and stage0–3 checkpoints; no open findings |

The quote regression wrapper delegates to the real backend without altering
results. Its failed safe-span gate on the original writer, and passed gate on
the repaired writer, establish bounded int counts through actual calls. The
2GiB example is calculated from valid limits, not executed or allocated. The
permanent medium fixture verifies every escaped byte, exact/short budgets and
raw byte lengths; it is not presented as a multi-gigabyte stress test.

Planning copy /tmp/p3b8-plan-jowcaa_x contains final compiled/tested source and
docs. Fresh stage1 replay /tmp/p3b8-r2-replay-_u2t7n9q matches checkpoint1 before
both RED cases/probe. All nine final hashes were verified; maximum-schema
actual2085/largest selected36/live0 and all77 allocation failures remain unchanged.
No production source edit, Git write, public binder activation, sanitizer,
benchmark, migration or protected certification activity occurred. Root changes
are planning/index/review documents only; the executor must still run Tasks0–3
literally and stop uncommitted for both executed raw-diff reviews.
