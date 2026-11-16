# Phase 3B.7 — checked packed JSON cells and projected GET

## Primary goal

Return selected financial fields with every decimal digit and string byte intact,
and report failure instead of a successful incomplete selected record, while
existing applications continue using the legacy format.

**Status: Revision 2, blind-reviewed and repaired; awaiting explicit approval.** This document is
not authorization to implement. Planner: GPT 6.1 Sol. Intended executor: GLM 5.3,
per the user's series assignment. Base `decbee6a18fc950bbf18e2f4ce6ad68956cad554`
on `fix/phase1-safe-scalar-conversions` (committed, reviewed Phase 3B.6).
All new C below has been exercised only in an isolated planning copy. The root
production tree has no Phase 3B.7 implementation.

## Revision 2 — blind review repair

- SPEC-01 (P2): reject inconsistent fixed logical type/carrier widths and enum
  width/count/table metadata in the shared packed schema guard, before any
  renderer reads a fixed span. The original guard checked directory width
  against TypedField.size but not the logical type's required read width.
- Add 48 committed assertions covering all 13 wider scalar substitutions,
  invalid type/enum metadata, unknown selections and restored valid schemas;
  add an actual-command regression for an unselected inconsistent fixed field.
- The guard-only regression was proved RED on the complete Revision1 code:
  175/43 focused and121/1 probe gates, then fixed without weakening assertions.
  New whole-plan checkpoints/counts below supersede Revision1 Appendix C.
- Standards review independently replayed all original anchors/checkpoints and
  checked mock boundaries. No additional standards finding was raised.

## Scope and contract

Add two length-aware packed JSON primitives using the row adapter's existing
checked scalar/decimal text and escaping helpers. Route the two shared JSON
cell adapters and the single projected GET command to them only for an explicitly
bound packed schema. Keep the existing legacy branches byte-for-byte, and keep
packed storage publicly inactive. No loader/create-object gains a binder call.
This is a small read integration child, not closure of Phase 3B or public query
acceptance. The existing supported bare limits remain 4 MiB varchar, 131072
integer digits and 16383 fractional digits; neither declaration nor storage
format changes here.

| Surface | Contract in this child |
|---|---|
| New cell API | Owned escaped JSON cell, explicit length/generation/index, budget includes NUL; NULL/removed is owned `null`; error is NULL + errno |
| New selected-object API | One owned escaped JSON object, preserves requested order and duplicate keys; whole record validated before selected rendering |
| Numbers and boolean display | Numeric types remain bare exact JSON numbers; projection/rows bool retains existing quoted `"true"`/`"false"`; full-record bool remains native |
| Varchar | Escape the complete byte span, including embedded NUL as `\u0000`; no strlen over record content |
| Unknown/removed names | Render null after validating the record; malformed unselected fields cannot be hidden by selecting an absent name |
| CSV selection | At most MAX_LINE-1 bytes, at most MAX_FIELDS nonempty tokens; commas collapse empty tokens; whitespace significant; order/duplicates preserved |
| Empty selection | New direct object API returns `{}`; existing public empty-fields dispatch still chooses full GET |
| Oversize/composite | E2BIG on new helper's byte/token limits; ENOTSUP for any packed selector containing `+`, entire packed request refused before output |
| Output/ownership | Allocate before publishing output; free-compatible success buffer; caller owns FILE, flush and close; no replacement JSON after a failing FILE has accepted a prefix |
| Memory | Exact selected JSON + NUL, actual CSV copy, bounded decimal validation/render workspaces; no declared-schema or full unselected-varchar JSON allocation |
| Production budget | Shared adapters/GET pass SIZE_MAX, preserving uncapped output policy; QUERY_BUFFER_MB enforcement remains activation work |

The input CSV is assumed NUL terminated. Name escaping in direct private tests
does not assert that the upstream JSON parser accepts arbitrary escaped schema
names. util.c's existing JSON string/array-to-CSV parser still has its own
truncation/unescaping behavior. This child does not change it. Similarly, making
rows/join cell text exact does not fix their surrounding error envelopes,
matching/sorting or raw-offset field readers. Those remain explicit blockers.

Tradeoffs for approval: preserve established quoted projection booleans; refuse
packed composites until a length-aware design exists; use an owned complete
selected object before GET output; retain existing public legacy behavior and
uncapped output policy during inactive integration. No speedup claim or new
third-party dependency. Legacy raw-field allocation/error bugs are not endorsed
as final behavior: their owning field/query/caller phases remain required.

## Task 0 — documentation discovery and checkpoint

**How this serves the primary goal:** pins the real display, ownership and caller
contracts before making any path consume an exact variable-length decimal.

Read AGENTS.md/CORE-PROCESS.md, the Phase 3 work index and parent recovery plan,
the reviewed 3B.6 plan/evidence/raw-diff review, typed-records.md,
typed-row-adapter.md, whole-record-readers.md, packed-record-codec.md and
query-protocol/overview.md/find.md. Never read the protected rig's contents.
Two read-only fact-gathering agents enumerated signatures, source bodies,
consumer searches and existing fixtures; the planner checked the source and
consolidated their evidence. There is no external-source dependency here.

### Allowed APIs and copy patterns at the fixed base

- typed_row.h: `typed_row_open(ts, value, size_t len, hdr2, TypedRowView *)`
  validates schema/record generation and all packed spans; `typed_row_span`
  returns a borrowed span. Neither authorizes metadata activation.
- typed_row.c: copy the exact checked `RowText`, `row_text_open/free/length/write`,
  `quoted_length`, `quote_into` and sizing pattern from `typed_packed_decode`.
  These static helpers are already in this translation unit. Do not expose
  internal RowText or invent an int128/alternate decimal format. Fixed carrier
  widths copy parse_field_type exactly; type_desc.int_width is aggregate
  membership, not a complete storage-width table (timestamp is excluded).
- types.h/config.c: `typed_field_index(ts, name)` returns an exact name match,
  excluding removed fields; no trimming. `parse_field_type` is void and fills
  fixed-type size/enum metadata. `free_enum_values` releases a fixture enum.
- typed_row.h: existing `typed_prepared_init/zero/set/alloc/free` and
  `typed_encode_alloc` create private records; copy fixture setup from
  test_whole_record_readers.c and test_typed_row_adapter.c. Use real types,
  not guessed struct layouts or a process-global packed toggle.
- types.h/query_find.c: `json_projected_field(const char *, size_t, const char *,
  FieldSchema *, uint8_t)` and `json_typed_row_cell(const TypedSchema *,
  const uint8_t *, int, int, uint8_t)` return owned cell strings. Their legacy
  branches are retained. Negative packed rows length becomes EILSEQ.
- storage.c/types.h: `cmd_get_fields(root, object, key, size_t klen, fields_csv)`
  owns fetched bytes and a cleanup registry reference. Copy 3B.6's checked
  fwrite/ferror/newline policy, without flushing/closing the borrowed g_out.
- test_assert.h/test_runner.h: copy the real two-argument ASSERT_NOT_NULL and
  TEST_REGISTER pattern; each test runs in its normal worker process. New
  committed tests use private heap schemas, no environment/global registry swap.

### Consumer census (source search, not inferred from the call graph)

| API/surface | Consumers at base, and what changes |
|---|---|
| Shared packed guard | Existing typed_row_open, prepared_check, typed_prepared_zero/old, typed_insert_alloc/inputs_alloc, typed_update_json_alloc, typed_transfer_ac_alloc, typed_packed_field_text/decode; new two APIs. Additional rejects apply only to inconsistent packed metadata, no valid legacy behavior or wire/layout change |
| New two packed JSON APIs | Declarations/definitions + new test; new cell API called by two packed query_find branches; new object API called by packed cmd_get_fields only |
| json_projected_field | storage.c cmd_get_fields; query_find.c print_record_json/dict/row; query.c at 1704/1721,2261/2278,2330/2347,2899/2916,5107/5129/5146,6800/6821/6838,9205/9227/9243; query_join.c at 748/770/786 |
| json_typed_row_cell | query_find.c print_record_row; query.c at 5115/6808/9212; query_join.c at 755 |
| json_projected_value | Used by the two existing cell adapters; no signature or legacy behavior change |
| cmd_get_fields | storage.c definition, types.h declaration, server.c GET branch at 1705; embedded JSON dispatch uses that same server dispatcher |
| GET parsing/locks | server.c at 1685 chooses key-array first, then nonempty fields, then full GET; object read lock already held by dispatch; util.c json_obj_string_or_array stays unchanged |
| Raw field/composite text | typed_get_field_str/decode_field; query_find CSV, query.c sorting, query_plan criteria, query_aggregate group keys, query_join/raw readers; no integration claimed here |
| Build | Add regression once to existing runner case list; typed_row/query_find already link in existing daemon/library/test lists; no new source list or dependency |
| Public metadata | typed_schema_bind_packed has no loader/create caller and gains none; no default-format, schema version, migration or activation edit |

Line numbers above are census evidence only. Executable edits use the complete
quoted anchors in Appendix B. No existing API signature changes.

### Root causes

1. json_projected_field/json_typed_row_cell still use legacy fixed offsets via
   decode_field/typed_get_field_str. A packed descriptor is not a scalar value
   or a uint16 varchar length. Enabling them by flag alone would misread records.
   Use schema-bound typed_row_open/spans and the existing exact decimal renderer.
2. strlen-based raw-field strings cannot represent embedded NUL and their NULL
   results conflate absent/NULL with some conversion/OOM failures. New packed
   JSON cells have owned literal null on successful absence, NULL only on error.
   Do not teach their raw-string consumers to accept binary data in this slice.
3. cmd_get_fields emits `{` before converting fields and skips a NULL cell,
   making an allocation/corruption failure appear to be successful omission.
   The packed branch must build a complete selected object before the first
   success byte, then check accepted bytes and newline. The legacy branch's
   allocation/streaming contract remains separate work, not silently rewritten.
4. A naive whole-row decode/project would allocate unselected 4 MiB content,
   and still risk reparsing exact decimals through floating-point/string paths.
   Select spans directly; validate the entire row once in the object renderer.

5. The shared packed_schema_ok checked fixed width against f->size but not the
   logical decoder's read width. A one-byte bool directory paired with FT_LONG
   or an enum_width=2 could reach a wider read, even though all directory bounds
   passed. Add a nonallocating fixed-width check using config.c's actual carrier
   widths and enum width/count/table consistency. Public binder output is already
   consistent; this changes invalid metadata rejection only. No format/migration.

### Execution rules

- Await explicit approval. Verify exact branch, HEAD, plan SHA and starting
  status. Do not branch/reset/stash, stage, commit, push, open PR or merge.
- Expected protected hash:
  fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
  Hash docs/plans/2026-10-19-durability-certification-rig.md at start/end;
  driver repeats the hash before stages/check-only. Hash-only access: never
  read contents, edit, move, copy, stage, incorporate or execute it.
- Preserve all pre-existing planning/review files, including the modified Phase
  3 work index/new child plan. Work only the nine owned checkpoint files plus
  curated execution evidence. Logs, probes, objects and extraction scripts
  stay outside version control in a fresh /tmp directory.
- Use the frozen bootstrap/driver/checks, in order; --check-only preflights
  every anchor/file/block before writing. Any mismatch, warning, unexpected
  count/failure or uncovered decision requires PLAN_NOTES.md and a halt of the
  entire run. Planner/human must amend before resume. Never weaken assertions,
  repair anchors ad hoc, silently skip daemon tests or loop until green.
- All shell commands rtk-prefixed. Build with SKIP_TESTS=1 ./build.sh; tests
  via build/bin/shard-db-test. Request socket-capable execution if the sandbox
  refuses daemon sockets. Normal full suite is mandatory; no sanitizers or
  benches. Branch safety gates stay after all fixes immediately before push.
- Mandatory regression RED before implementation; additional scratch-only
  reverse/reapply proof after GREEN. Never revert root sources/binaries.
- Stop unstaged/uncommitted for plan-aware and independent blind raw-diff
  reviews. This plan never authorizes later phases, public binding or the rig.

## Task 1 — complete regression and linkable RED stubs

**How this serves the primary goal:** proves exact selected reads are missing
before adding the packed implementation or command branch.

Apply stage1: complete test_packed_json_projection.c + one runner registration,
new header API declarations/comments and two linkable ENOTSUP stubs appended to
typed_row.c. The new fixed-schema tests exercise the existing shared guard before its fix.
The stubs are feature scaffolding only; production query/GET code
is unchanged. Rebuild normal, zero warnings and exit0. Run focused regression:
**40 passed /176 failed, exit176**. Two assertions conditional on successful
large output run only after implementation, explaining 216 RED vs218 GREEN.
Legacy numeric/bool/composite/unknown controls pass on both sides; incidental
invalid-input passes on stubs are not evidence of implemented validation.

Run checks --mode red before stage2. It compiles actual typed_row/query_find/
packed_record/decnum with scratch-only allocator substitutions, links the normal
library and extracts the actual cmd_get_fields body with local schema/registry/
read mocks. Require exactly **22 passing/20 failing probe gates**, exit1
(handled by checks); setup failure exit2 is never accepted as RED. This catches
wrong/missing cells, false successful omissions, composite/mismatch/corruption
handling, unchecked I/O and max-schema allocation behavior. The zero allocation
counts on baseline prove the new paths are absent, not OOM safety.

## Task 2 — exact packed projection and checked command

**How this serves the primary goal:** renders complete selected financial values
and makes allocation/corruption errors distinguishable from legitimate NULL.

Apply stage2's complete stubs-to-module replacement, query_find include + two
packed-only branches, cmd_get_fields packed-only branch and the shared fixed
schema-width guard. The full guard replacement and its nonallocating helper
are frozen as schema-width-guard-old/new. Fixed enum validation checks the
carrier against count and a present value table; it preserves the existing
empty-string policy for an invalid stored enum index. Use the checked
RowText routines exactly as supplied. Legacy branches remain byte-identical.
No raw-string decoder, criteria, index, field arithmetic, loader or schema edit.

The complete object renderer preflights selectors, validates the full view, then
measures and renders selected fields with separate checked temporary workspaces.
A second-pass failure frees the unpublished output. Cell helpers validate too;
unknown-name helper validates before allocating null. Boolean quoting follows
existing projection policy. The complete selected object owns escaped field
names and values, so command code does not interpolate raw names into JSON.

Require fresh zero-warning build, focused **218/0**, exit0. Run green checks:
**122 passing gates**, zero failures; **31 nonempty allocation injections**
(cell6 + named6 + rows6 + unknown4 + command9). Every injected failure returns
NULL/ENOMEM or one explicit command error, frees all tracked buffers and releases
the mocked registry ref once. Unknown-field sweeps include decimal-validation
allocations, not just malloc of null. A selected output buffer must also be
released when later rendering fails. No production test seam/global override.

Output tests exercise short writes, failing newline, readonly FILE, corruption,
ENOTSUP/E2BIG errors and no appended replacement error after an I/O prefix.
They use fopencookie on Linux/funopen on Apple; only Linux x86_64 is executed in
planning. The borrowed FILE is not flushed/closed by cmd_get_fields. Existing
missing-key/registry behavior is untouched. Atomicity covers pre-output render
failure; a failing FILE can accept a success prefix, so no network transaction
or flush-completion guarantee is claimed.

Maximum schema: actual row2085 bytes, largest selected allocation30 bytes,
live0 despite >1 GiB theoretical schema. Unselected 4 MiB varchar: actual
4194339 bytes, largest selected allocation15 bytes, live0. The fetched raw
record is still allocated by the real reader; probe excludes its mocked plain
malloc from selected-allocation accounting. This proves no full JSON/schema
ceiling allocation, not zero total request memory. Decimal validation can still
allocate bounded workspaces for unselected numeric fields.

Run checks --mode regression-proof: copy fixed typed_row.c/query_find.c/storage.c
to /tmp; reverse stage2, verify all three hashes equal checkpoint1, compile and
reproduce20 failing gates; reapply, verify checkpoint2 and require122/0. For
query_find/storage, checkpoint1 is the committed baseline; typed_row checkpoint1
includes the explicit new API stubs required to link the feature regression.
This distinction is deliberate and not a claim that absent APIs existed at HEAD.
Checkout source/build products are never reverted. Paste both outputs.

## Task 3 — documentation, full verification and stop

**How this serves the primary goal:** documents the supported selected-read
boundary and proves ordinary applications retain their current behavior.

Apply stage3: complete packed-json-projection.md and exact adapter/whole-record
reader link corrections. These clarify new cells versus unsupported surrounding
query/rows/join/raw-string paths. No activation or migration claim.

Fresh normal full suite: **20263 passed/0 failed across525 cases**, exit0.
Repeat green checks, all nine final checkpoint hashes, git diff --check and
binder census (no loader/create caller). Hash protected file explicitly again.
Do not add debug prints/TODO/FIXME, logs, probe binaries, temporary scripts or
caches to the root diff. Curated evidence only at
`docs/plans/2026-10-26-phase3b7-packed-json-projection-evidence.md`, recording
actual identity/compiler/hashes/commands, RED/GREEN/full/probe/reverse outputs,
allocation accounting and remaining scope. Do not weaken assertions or replace
measured results with forecasts. Stop for executed reviews, no Git writes.

## Planning evidence and limitations

Scratch git archive of exact HEAD: /tmp/p3b7-plan-nvaml3ww; protected untracked
file never copied/read. Ordinary compilation only, no sanitizers or benchmarks.
Frozen-block/checkpoint verification and log references are in Appendix C. Revision2 verification is recorded in Appendix D; Revision1 figures below
are historical. All regressions remain unchanged after genuine RED capture.
The initial probe fixture omitted fixed bool size metadata, causing a setup
failure exit2; it was corrected by calling the real void parse_field_type.
No setup failure was counted as RED. A fresh checkpoint1 probe capture after
that correction reproduces the original19 defect gates, followed by119 passing gates on
implementation. The later blind pass found the schema-width gap fixed in Revision2. Scratch testing is evidence, not root implementation.

Scope limits: the probe uses actual command source with mocked read/schema/
registry dependencies; it does not prove network delivery, lock races, public
metadata reload or packed query matching. The direct C test's escaped private
field name does not repair public parser escaping. Existing legacy projection
OOM/null and unchecked streaming paths are explicit future field/caller work.
No live PostgreSQL, external oracle or new decimal policy is required: this
child reuses Phase 2/3A exact packed decimal rendering and validates its outputs.

## Following work

After approval/execution/reviews/commit, prepare remaining length-aware field
and query-emission work: raw-field/CSV/composite contracts, caller error propagation
and packed-safe scan/projection envelopes. Audit delete/read/index/rebuild and
maintenance consumers before declaring 3B complete. Phase3C owns durable metadata/
publication safety; Phases4–6 complete assignment, query/index and aggregates;
Phase7 the other type contracts; Phase8 existing-data conversion; Phase9 final
application acceptance/activation. Further child sizing is decided against each
reviewed checkpoint. Protected durability certification stays separate afterward.

## Appendix A — frozen extraction and invocation order

Execute the bootstrap below with rtk proxy python3 from the repository root.
Use its printed fresh /tmp folder in every command. Log unfiltered outputs and
exit status. The focused RED exit176 is required. Checks internally enforce
expected RED exit1, reject setup exits and verify exact probe gate totals.

<!-- block:bootstrap -->
```python
import pathlib,re,tempfile
plan=pathlib.Path('docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md')
folder=pathlib.Path(tempfile.mkdtemp(prefix='p3b7-exec-'))
for m in re.finditer(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text()):
    (folder/(m.group(1)+'.txt')).write_text(m.group(2)+'\n')
for name in ['driver','checks']:
    (folder/(name+'.py')).write_bytes((folder/(name+'.txt')).read_bytes())
print(folder)
```

~~~sh
rtk proxy python3 /tmp/p3b7-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --stage 1 --check-only
rtk proxy python3 /tmp/p3b7-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-json-projection
rtk proxy python3 /tmp/p3b7-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --mode red
rtk proxy python3 /tmp/p3b7-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-json-projection
rtk proxy python3 /tmp/p3b7-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --mode green
rtk proxy python3 /tmp/p3b7-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --mode regression-proof
rtk proxy python3 /tmp/p3b7-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 /tmp/p3b7-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md --mode green
rtk git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
~~~

## Appendix B — complete anchored code and checkpoints

Every named block is frozen and manifest-hashed. A missing/duplicate anchor or
checkpoint mismatch halts before stage mutation. New files must be absent.
Each task is fully specified; no code is left for executor design.

### projection-test

<!-- block:projection-test -->
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_row.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
static TypedSchema *p_schema(int maximum){
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
static void p_free(TypedSchema *ts){if(ts){for(int i=0;i<ts->nfields;i++)free_enum_values(&ts->fields[i]);free(ts);}}
static int p_encode(TypedSchema *ts,const char *json,uint8_t **bytes,size_t *n){
    int rc=typed_encode_alloc(ts,json,1,NULL,NULL,bytes,n,NULL,0);ASSERT_EQ_INT(rc,0,"encode fixture");return rc;
}
static void p_object(TypedSchema *ts,const uint8_t *bytes,size_t n,const char *fields,const char *expected){
    char *out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,SIZE_MAX);
    ASSERT_NOT_NULL(out,"projected object available");ASSERT_TRUE(out&&!strcmp(out,expected),"projected object exact");
    size_t budget=strlen(expected)+1;free(out);
    out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,budget);
    ASSERT_NOT_NULL(out,"exact object budget");ASSERT_TRUE(out&&!strcmp(out,expected),"exact-budget bytes");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,fields,budget-1);
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short object budget rejects");free(out);
}
static void p_cell(TypedSchema *ts,const uint8_t *bytes,size_t n,int index,const char *expected){
    char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,SIZE_MAX);
    ASSERT_NOT_NULL(out,"JSON cell available");ASSERT_TRUE(out&&!strcmp(out,expected),"JSON cell exact");free(out);
    out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,strlen(expected)+1);
    ASSERT_NOT_NULL(out,"exact cell budget");ASSERT_TRUE(out&&!strcmp(out,expected),"exact cell bytes");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,(size_t)index,strlen(expected));
    ASSERT_TRUE(!out&&errno==ENOMEM,"one-short cell budget rejects");free(out);
    out=json_typed_row_cell(ts,bytes,(int)n,index,PKR_FLAGS);
    ASSERT_NOT_NULL(out,"rows helper returns JSON");ASSERT_TRUE(out&&!strcmp(out,expected),"rows helper exact");free(out);
    FieldSchema fs={.ts=ts};out=json_projected_field((const char*)bytes,n,ts->fields[index].name,&fs,PKR_FLAGS);
    ASSERT_NOT_NULL(out,"named helper returns JSON");ASSERT_TRUE(out&&!strcmp(out,expected),"named helper exact");free(out);
}
static void p_basics(void){
    TypedSchema *ts=p_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    const char *json="{\"f0\":\"1234567890123456.1234567890\",\"f1\":1e-26,\"f2\":\"a\\u0000b\\\"\\\\\\n\",\"f3\":true,\"f4\":0,\"f5\":null,\"f6\":\"yes\"}";
    if(p_encode(ts,json,&bytes,&n)){p_free(ts);return;}
    p_cell(ts,bytes,n,0,"1234567890123456.1234567890");
    p_cell(ts,bytes,n,1,"0.00000000000000000000000001");
    p_cell(ts,bytes,n,2,"\"a\\u0000b\\\"\\\\\\n\"");p_cell(ts,bytes,n,3,"\"true\"");
    p_cell(ts,bytes,n,4,"0");p_cell(ts,bytes,n,5,"null");p_cell(ts,bytes,n,6,"\"yes\"");
    p_object(ts,bytes,n,"f3,f0,f5,unknown,f4","{\"f3\":\"true\",\"f0\":1234567890123456.1234567890,\"f5\":null,\"unknown\":null,\"f4\":0}");
    p_object(ts,bytes,n,",,f4,,f4, f4,","{\"f4\":0,\"f4\":0,\" f4\":null}");
    p_object(ts,bytes,n,"","{}");p_object(ts,bytes,n,",,,","{}");
    FieldSchema fs={.ts=ts};char *out=json_projected_field((const char*)bytes,n,"missing",&fs,PKR_FLAGS);
    ASSERT_EQ_STR(out,"null","unknown named field on flagged row");free(out);
    errno=0;out=json_projected_field((const char*)bytes,n,"f2+f0",&fs,PKR_FLAGS);
    ASSERT_TRUE(!out&&errno==ENOTSUP,"packed composite named helper refuses");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f4,f2+f0",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==ENOTSUP,"mixed composite request refuses entirely");free(out);
    ts->fields[4].removed=1;p_cell(ts,bytes,n,4,"null");p_object(ts,bytes,n,"f4","{\"f4\":null}");ts->fields[4].removed=0;
    strcpy(ts->fields[4].name,"x\"\\\n");ts->fields[4].name_len=4;
    p_object(ts,bytes,n,ts->fields[4].name,"{\"x\\\"\\\\\\n\":0}");
    free(bytes);p_free(ts);
}
static void p_failures(void){
    TypedSchema *ts=p_schema(0);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(p_encode(ts,"{\"f0\":1,\"f2\":\"value\"}",&bytes,&n)){p_free(ts);return;}
    const uint8_t flags[]={0,SREC_FLAG_NULLMAP,SREC_FLAG_PACKED,0x80};
    for(size_t i=0;i<sizeof(flags);i++){
        errno=0;char *out=typed_packed_project_json(ts,bytes,n,flags[i],"f0",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"object generation mismatch");free(out);
        errno=0;out=typed_packed_projected_cell(ts,bytes,n,flags[i],0,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"cell generation mismatch");free(out);
        FieldSchema fs={.ts=ts};errno=0;out=json_projected_field((const char*)bytes,n,"missing",&fs,flags[i]);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unknown field cannot bypass corrupt generation");free(out);
    }
    uint8_t *bad=malloc(n);ASSERT_NOT_NULL(bad,"corrupt directory copy");
    if(bad){memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[2].offset,255,4);
        errno=0;char *out=typed_packed_project_json(ts,bad,n,PKR_FLAGS,"f4",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EILSEQ,"unselected corrupt descriptor still rejects");free(out);free(bad);}
    errno=0;char *out=typed_packed_project_json(ts,bytes,n-1,PKR_FLAGS,"f0",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EILSEQ,"truncated record rejects");free(out);
    errno=0;out=typed_packed_project_json(ts,NULL,0,PKR_FLAGS,"f0",SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL record argument rejects");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,NULL,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"NULL projection rejects");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,MAX_FIELDS,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==EINVAL,"invalid cell index");free(out);
    errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"f0",0);
    ASSERT_TRUE(!out&&errno==ENOMEM,"zero object budget");free(out);
    errno=0;out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,0,0);
    ASSERT_TRUE(!out&&errno==ENOMEM,"zero cell budget");free(out);
    char *longlist=malloc(MAX_LINE+1);ASSERT_NOT_NULL(longlist,"overlong selector");
    if(longlist){memset(longlist,'x',MAX_LINE);longlist[MAX_LINE]=0;errno=0;
        out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,longlist,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==E2BIG,"no silent selector truncation");free(out);free(longlist);}
    char count[(MAX_FIELDS+1)*3+1];size_t at=0;
    for(int i=0;i<MAX_FIELDS+1;i++){memcpy(count+at,"f4,",3);at+=3;}count[at]=0;errno=0;
    out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,count,SIZE_MAX);
    ASSERT_TRUE(!out&&errno==E2BIG,"no silent field-count truncation");free(out);
    errno=0;out=json_typed_row_cell(ts,bytes,-1,0,PKR_FLAGS);
    ASSERT_TRUE(!out&&errno==EILSEQ,"negative rows length rejects");free(out);
    free(bytes);p_free(ts);
}
static void p_legacy_controls(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"legacy control schema");if(!ts)return;
    ts->typed=1;ts->nfields=2;ts->total_size=7;ts->nullmap_prefix=2;
    ts->fields[0].type=FT_INT;ts->fields[0].size=4;strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;
    ts->fields[1].type=FT_BOOL;ts->fields[1].size=1;ts->fields[1].offset=4;strcpy(ts->fields[1].name,"f1");ts->fields[1].name_len=2;
    uint8_t bytes[]={1,0,0,0,0,7,1};FieldSchema fs={.ts=ts};
    char *out=json_projected_field((const char*)bytes,sizeof(bytes),"f0",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"7","legacy numeric remains bare");free(out);
    out=json_typed_row_cell(ts,bytes,sizeof(bytes),1,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"\"true\"","legacy bool cell stays quoted");free(out);
    out=json_projected_field((const char*)bytes,sizeof(bytes),"f0+f1",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"\"7true\"","legacy composite unchanged");free(out);
    out=json_projected_field((const char*)bytes,sizeof(bytes),"missing",&fs,SREC_FLAG_NULLMAP);ASSERT_EQ_STR(out,"null","legacy flagged unknown null");free(out);
    free(ts);
}
static void p_limits(void){
    TypedSchema *ts=p_schema(1);if(!ts)return;uint8_t *bytes=NULL;size_t n=0;
    if(!p_encode(ts,"{\"f127\":\"tiny\"}",&bytes,&n)){
        ASSERT_TRUE(ts->total_size>1000000000&&n<4096,"max schema tiny actual row");
        p_object(ts,bytes,n,"f127,absent","{\"f127\":\"tiny\",\"absent\":null}");free(bytes);}
    p_free(ts);ts=p_schema(0);if(!ts)return;
    size_t len=4u*1024*1024;char *content=malloc(len);ASSERT_NOT_NULL(content,"maximum binary varchar");
    if(content){memset(content,'x',len);content[1]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare large record");
        TypedInput input={content,len,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,2,&input,NULL,0),0,"set large varchar");
        TypedInput zero={"0",1,1,0,0};ASSERT_EQ_INT(typed_prepared_set(&p,4,&zero,NULL,0),0,"set small selected field");
        int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate actual large row");
        if(!rc){p_object(ts,bytes,n,"f4","{\"f4\":0}");
            char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,2,SIZE_MAX);ASSERT_NOT_NULL(out,"large JSON cell");
            if(out){ASSERT_TRUE(strlen(out)==len+7,"embedded NUL escaped without truncation");ASSERT_TRUE(!memcmp(out,"\"x\\u0000",8),"large prefix escaped");}free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    len=131072+1+16383;content=malloc(len+1);ASSERT_NOT_NULL(content,"maximum decimal input");
    if(content){memset(content,'1',len);content[131072]='.';content[len]=0;TypedPrepared p;typed_prepared_init(&p);
        ASSERT_EQ_INT(typed_prepared_zero(ts,&p,NULL,0),0,"prepare full decimal");TypedInput input={content,len,1,0,0};
        ASSERT_EQ_INT(typed_prepared_set(&p,1,&input,NULL,0),0,"set full bare numeric");int rc=typed_prepared_alloc(&p,&bytes,&n,NULL,0);ASSERT_EQ_INT(rc,0,"allocate full decimal");
        if(!rc){char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,1,len+1);ASSERT_NOT_NULL(out,"full numeric cell");ASSERT_TRUE(out&&!strcmp(out,content),"every decimal digit survives");free(out);free(bytes);}
        typed_prepared_free(&p);free(content);}
    p_free(ts);
}
static void p_schema_widths(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));ASSERT_NOT_NULL(ts,"fixed width schema");if(!ts)return;
    ts->typed=1;ts->nfields=1;parse_field_type("bool",&ts->fields[0]);
    strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;const char *types[]={"bool"};
    int rc=typed_schema_bind_packed(ts,types,1,NULL,0);ASSERT_EQ_INT(rc,0,"bind one-byte fixed schema");
    if(rc){free(ts);return;}
    uint8_t *encoded=NULL;size_t n=0;if(p_encode(ts,"{\"f0\":true}",&encoded,&n)){free(ts);return;}
    /* Keep extra allocation bytes so RED exercises wrong-span reads without
       going beyond the C allocation. The supplied record length stays exact. */
    uint8_t bytes[32]={0};ASSERT_TRUE(n<sizeof(bytes),"one fixed record fits fixture");
    if(n>=sizeof(bytes)){free(encoded);free(ts);return;}memcpy(bytes,encoded,n);free(encoded);
    const enum FieldType wider[]={FT_SHORT,FT_INT,FT_LONG,FT_FLOAT,FT_DOUBLE,
        FT_DATE,FT_DATETIME,FT_DATETIMEMS,FT_TIME,FT_TIMESTAMP,FT_UUID,FT_IPV4,FT_IPV6};
    for(size_t i=0;i<sizeof(wider)/sizeof(wider[0]);i++){
        ts->fields[0].type=wider[i];TypedRowView view;errno=0;
        ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"view rejects logical width mismatch");
        errno=0;char *out=typed_packed_projected_cell(ts,bytes,n,PKR_FLAGS,0,SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EINVAL,"cell rejects mismatched fixed schema");free(out);
        errno=0;out=typed_packed_project_json(ts,bytes,n,PKR_FLAGS,"absent",SIZE_MAX);
        ASSERT_TRUE(!out&&errno==EINVAL,"unknown selection cannot bypass schema width guard");free(out);
    }
    ts->fields[0].type=FT_NONE;TypedRowView view;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"invalid fixed logical type rejects");
    ts->fields[0].type=FT_ENUM;ts->fields[0].n_enum_values=1;ts->fields[0].enum_width=2;
    char *values[]={"yes"};ts->fields[0].enum_values=values;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"enum carrier width mismatch rejects");
    ts->fields[0].enum_width=1;ts->fields[0].n_enum_values=257;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"enum count and width disagree");
    ts->fields[0].n_enum_values=1;ts->fields[0].enum_values=NULL;errno=0;
    ASSERT_TRUE(typed_row_open(ts,bytes,n,PKR_FLAGS,&view)==-1&&errno==EILSEQ,"missing enum table rejects");
    ts->fields[0].type=FT_BOOL;ts->fields[0].n_enum_values=0;ts->fields[0].enum_width=0;
    ASSERT_EQ_INT(typed_row_open(ts,bytes,n,PKR_FLAGS,&view),0,"restored schema remains readable");free(ts);
}
static int test_packed_json_projection_run(void){p_basics();p_failures();p_legacy_controls();p_limits();p_schema_widths();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-json-projection",test_packed_json_projection_run)
```

### build-register-old

<!-- block:build-register-old -->
```c
    src/test/cases/test_whole_record_readers.c \
```

### build-register-new

<!-- block:build-register-new -->
```c
    src/test/cases/test_whole_record_readers.c \
    src/test/cases/test_packed_json_projection.c \
```

### projection-header-old

<!-- block:projection-header-old -->
```c
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len);
```

### projection-header-new

<!-- block:projection-header-new -->
```c
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len);
/* Complete JSON cells for projection/rows, preserving their bool-as-string
   policy. NULL/removed produces owned "null"; any error is NULL with errno.
   Budget includes NUL. Names are not parsed by the indexed cell API.
   Projection CSV is bounded by MAX_LINE-1/MAX_FIELDS, skips empty tokens,
   preserves order/duplicates and whitespace, maps unknown/removed to null.
   Packed composites containing '+' refuse ENOTSUP until integrated. */
char *typed_packed_projected_cell(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, size_t index, size_t budget);
char *typed_packed_project_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *fields_csv, size_t budget);
```

### projection-stubs-old

<!-- block:projection-stubs-old -->
```c
    if(pos+1!=need){errno=EILSEQ;goto fail;}return out;
fail:free(out);return NULL;
}
```

### projection-stubs-new

<!-- block:projection-stubs-new -->
```c
    if(pos+1!=need){errno=EILSEQ;goto fail;}return out;
fail:free(out);return NULL;
}

char *typed_packed_projected_cell(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,size_t index,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)index;(void)budget;
    errno=ENOTSUP;return NULL;
}
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const char *fields_csv,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)fields_csv;(void)budget;
    errno=ENOTSUP;return NULL;
}
```

### projection-module-old

<!-- block:projection-module-old -->
```c

char *typed_packed_projected_cell(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,size_t index,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)index;(void)budget;
    errno=ENOTSUP;return NULL;
}
char *typed_packed_project_json(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const char *fields_csv,size_t budget){
    (void)ts;(void)value;(void)len;(void)hdr2;(void)fields_csv;(void)budget;
    errno=ENOTSUP;return NULL;
}
```

### projection-module-new

<!-- block:projection-module-new -->
```c
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

### projection-edit-1-old

<!-- block:projection-edit-1-old -->
```c
#include "query_internal.h"
```

### projection-edit-1-new

<!-- block:projection-edit-1-new -->
```c
#include "query_internal.h"
#include "typed_row.h"
```

### projection-edit-2-old

<!-- block:projection-edit-2-old -->
```c
char *json_projected_field(const char *raw, size_t raw_len, const char *field,
                           FieldSchema *fs, uint8_t hdr2) {
```

### projection-edit-2-new

<!-- block:projection-edit-2-new -->
```c
char *json_projected_field(const char *raw, size_t raw_len, const char *field,
                           FieldSchema *fs, uint8_t hdr2) {
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

### projection-edit-3-old

<!-- block:projection-edit-3-old -->
```c
    if (!ts || idx < 0 || idx >= ts->nfields) return NULL;
    errno = 0;
```

### projection-edit-3-new

<!-- block:projection-edit-3-new -->
```c
    if (!ts || idx < 0 || idx >= ts->nfields) return NULL;
    if(ts->packed.nfields){
        if(raw_len<0){errno=EILSEQ;return NULL;}
        return typed_packed_projected_cell(ts,raw,(size_t)raw_len,hdr2,(size_t)idx,SIZE_MAX);
    }
    errno = 0;
```

### projection-edit-4-old

<!-- block:projection-edit-4-old -->
```c
    FieldSchema pfs;
    init_field_schema(&pfs, db_root, object);
    char proj_buf[MAX_LINE];
```

### projection-edit-4-new

<!-- block:projection-edit-4-new -->
```c
    FieldSchema pfs;
    init_field_schema(&pfs, db_root, object);
    if(pfs.ts&&pfs.ts->packed.nfields){
        FILE *out=g_out?g_out:stdout;
        if(ferror(out)){free(val);errno=EIO;return 1;}
        char *json=typed_packed_project_json(pfs.ts,val,vlen,cur_hdr2,fields_csv,SIZE_MAX);
        if(!json){
            int saved=errno;
            const char *why=saved==ENOMEM?"out of memory":saved==ENOTSUP?
                "packed composite projections await integration":saved==E2BIG?
                "projection exceeds limit":"corrupt record";
            OUT("{\"error\":\"%s\"}\n",why);free(val);errno=saved;return 1;
        }
        size_t length=strlen(json);errno=0;size_t written=fwrite(json,1,length,out);int saved=errno;
        free(json);free(val);
        if(written!=length||ferror(out)){errno=saved?saved:EIO;return 1;}
        return fputc('\n',out)==EOF?1:0;
    }
    char proj_buf[MAX_LINE];
```

### schema-width-guard-old

<!-- block:schema-width-guard-old -->
```c
static int packed_schema_ok(const TypedSchema *ts) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS ||
        ts->packed.nfields != (uint32_t)ts->nfields ||
        ts->packed.prefix != (uint32_t)ts->nullmap_prefix ||
        ts->packed.maximum != (uint32_t)ts->total_size) return 0;
    for (int i=0;i<ts->nfields;i++) {
        const PkrField *p=&ts->packed.fields[i];const TypedField *f=&ts->fields[i];
        if (f->name_len<1 || f->name_len>=(int)sizeof(f->name) ||
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
```

### schema-width-guard-new

<!-- block:schema-width-guard-new -->
```c
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
```

### projection-doc

<!-- block:projection-doc -->
```markdown
# Checked JSON projections (packed format inactive)

The private packed JSON APIs produce exact selected values, rather than decoding
an entire row and parsing it again. `typed_packed_projected_cell` accepts a field
index and returns one owned JSON cell. `typed_packed_project_json` accepts a CSV
selector and returns one complete owned JSON object. Both use explicit record
lengths and reject inconsistent fixed carrier/type and enum metadata before
reading scalar spans. They validate the whole packed view, including unselected descriptors and
decimals, before rendering. Schema/generation disagreement rejects. Budgets
include NUL; errors return NULL with errno, never a successful partial string.
NULL or removed fields render as bare null. Unknown selected names render null
only after record validation. Numeric values are exact bare JSON numbers;
projection/rows booleans retain the existing quoted display strings "true" and
"false". Full-record JSON keeps its separate native boolean policy. Varchar
uses its full byte span, so embedded NUL is escaped as \u0000 and later bytes
survive. Names and string values are escaped with the same checked helpers.

The CSV selector is at most MAX_LINE-1 bytes and at most MAX_FIELDS nonempty
names. Empty comma tokens collapse, whitespace is significant, order and
duplicates are preserved. Empty selectors produce {} at this direct API;
the existing public dispatcher sends an empty fields argument to full GET.
Unknown/removed names map to null. Oversized selectors reject with E2BIG rather
than being silently truncated in this private helper. Any '+' token refuses
ENOTSUP for the whole packed projection, because legacy composite concatenation
uses strlen and cannot safely handle binary varchar. Legacy composites and
legacy selector parsing are unchanged. Upstream JSON-to-CSV parsing still has
its existing limits/unescaping behavior; this helper does not repair ingress.

Packed-only branches in json_projected_field and json_typed_row_cell call the
new checked cell API. Their legacy branches are unchanged. Packed cmd_get_fields
builds the complete selected object before writing: conversion, corruption or
allocation failure emits one error response without a success prefix. It checks
fwrite, FILE errors and its newline, and never appends a replacement error after
an I/O prefix. FILE ownership, flushing and closing stay with the caller; this
is accepted-byte checking, not transactional socket delivery. ENOMEM becomes
"out of memory", ENOTSUP explains pending packed composites, E2BIG reports the
projection limit and malformed records become "corrupt record".

Object output allocation is exact escaped selected JSON plus NUL, with an
actual-length CSV copy and separately bounded decimal workspaces. An unselected
large varchar is validated as a span but never copied into a whole JSON row.
View validation can allocate decimal validation workspace even for unselected
decimals. cmd_get_fields and the shared cell adapters pass SIZE_MAX, preserving
the existing uncapped output policy. QUERY_BUFFER_MB integration is future
activation work. No schema-max allocation or speed improvement is claimed.

This adds a rendering primitive, not packed query/rows/join activation. Those
callers still have independent matching, sorting, raw-offset, envelope and error
policies, including NULL/error substitution. Their use of a correct cell helper
does not establish end-to-end packed correctness. typed_get_field_str,
decode_field, CSV, composites, legacy allocation/error handling, and legacy
projected GET streaming remain unconverted. No public object binds packed.
Metadata, migration and the separate certification rig remain later work.

Private C tests exercise exact decimal cells and digit limits, binary strings,
NULL/removed/unknown values, duplicate selectors, quoted names, limits, invalid
flags and unselected corruption, and ordinary legacy scalar/composite controls.
Scratch probes instrument actual row/query modules and the source-extracted
cmd_get_fields body with local schema/registry/read mocks, exercising every
selected allocation, output failure, maximum-schema bounds and unselected
4 MiB content. They do not prove public network/metadata acceptance.
```

### adapter-doc-old

<!-- block:adapter-doc-old -->
```markdown
[whole-record JSON readers](whole-record-readers.md). Field-string, projection
and raw-offset consumers still require their own conversion. Passing a bound
schema to a legacy raw-offset API is unsafe. The ordinary loader does not call
this binder.
```

### adapter-doc-new

<!-- block:adapter-doc-new -->
```markdown
[whole-record JSON readers](whole-record-readers.md) and checked
[JSON projection cells/projected GET](packed-json-projection.md). Field-string,
CSV/composite, query-envelope and raw-offset consumers still require conversion.
Passing a bound schema to a legacy raw-offset API is unsafe. The ordinary loader
does not call this binder.
```

### whole-doc-old

<!-- block:whole-doc-old -->
```markdown
This is a rendering bridge, not query integration. `typed_get_field_str`,
projections, composite/CSV/rows/join extraction, matching, sorting, indexes and
aggregates still have legacy assumptions and separate error/length contracts.
A binary varchar cannot be safely passed to their strlen-based paths. A decoded
full row does not prove the query's planner/matcher/index/emit-failure path is
packed-ready. Existing callers that substitute null on decode failure remain
explicit follow-up work; no public packed activation is authorized here.
```

### whole-doc-new

<!-- block:whole-doc-new -->
```markdown
This is a rendering bridge, not query integration. Checked
[JSON cells and projected GET](packed-json-projection.md) now support private
packed schemas. `typed_get_field_str`, composites/CSV, query/rows/join envelopes,
matching, sorting, indexes and aggregates retain independent error/length and
layout contracts. Binary varchar cannot be safely passed to their strlen-based
paths. A decoded row/cell does not prove the planner/matcher/index/emit-failure
path is packed-ready. Callers that substitute null on decode failure remain
explicit follow-up work; no public packed activation is authorized here.
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
    if git('rev-parse','HEAD')!=m['base_head']:raise ValueError('HEAD differs from reviewed 3B.6 commit')
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
    message='Phase 3B.7 execution halted: '+str(e)+'\nNo later task may run. Planner/human must review this note before resuming.\n'
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
        run(['gcc','-O1','-g','-Isrc/db','-Dmalloc=p7_malloc','-Dcalloc=p7_calloc',
             '-Drealloc=p7_realloc','-Dfree=p7_free','-c',str(src),'-o',str(obj)])
    binary=work/'projection-probe'
    run(['gcc','-O1','-g','-Isrc/db','-DP7_CMD_SOURCE="'+str(command)+'"',str(probe),
         *objects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-lm','-o',str(binary)])
    output=run([str(binary)],expected)
    want=(22,20) if expected else (122,0)
    actual=(output.count(': PASS\n'),output.count(': FAIL\n'))
    if actual!=want:raise RuntimeError('probe gate totals '+str(actual)+' expected '+str(want))
def main():
    a=argparse.ArgumentParser();a.add_argument('--plan',type=pathlib.Path,required=True)
    a.add_argument('--mode',choices=['red','green','regression-proof'],required=True);args=a.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b7-check-'));print('scratch:',work,flush=True)
    probe=work/'probe.c';probe.write_text(extract(args.plan,'projection-probe'))
    row=pathlib.Path('src/db/typed_row.c');query=pathlib.Path('src/db/query_find.c');storage=pathlib.Path('src/db/storage.c')
    if args.mode!='regression-proof':
        check(work,probe,row,query,storage,1 if args.mode=='red' else 0);return
    manifest=json.loads(extract(args.plan,'manifest'));digest=lambda b:hashlib.sha256(b).hexdigest()
    copies={}
    for path in [str(row),str(query),str(storage)]:
        data=pathlib.Path(path).read_bytes()
        if digest(data)!=manifest['checkpoints']['2'][path]:raise ValueError('fixed source mismatch '+path)
        copies[path]=data
    edits=[e for e in manifest['edits']['2'] if e['path'] in copies]
    for e in reversed(edits):
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(new)!=1:raise ValueError('reverse anchor '+e['new'])
        copies[e['path']]=data.replace(new,old)
    for path,data in copies.items():
        if digest(data)!=manifest['checkpoints']['1'][path]:raise ValueError('baseline hash mismatch '+path)
        (work/pathlib.Path(path).name).write_bytes(data)
    check(work,probe,work/'typed_row.c',work/'query_find.c',work/'storage.c',1)
    for e in edits:
        old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode();data=copies[e['path']]
        if data.count(old)!=1:raise ValueError('reapply anchor '+e['old'])
        copies[e['path']]=data.replace(old,new)
    for path,data in copies.items():
        if digest(data)!=manifest['checkpoints']['2'][path]:raise ValueError('reapply hash mismatch '+path)
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
void *p7_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *p7_calloc(size_t a,size_t b){if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}if(!allow(a*b))return NULL;void *p=calloc(a,b);remember(p);return p;}
void *p7_realloc(void *p,size_t n){if(!allow(n))return NULL;int i=find(p);void *q=realloc(p,n);if(q){if(i>=0)owned[i]=owned[--live];remember(q);}return q;}
void p7_free(void *p){int i=find(p);if(i>=0)owned[i]=owned[--live];free(p);}
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
#define free p7_free
#include P7_CMD_SOURCE
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

static int failures;
static const char *fields="f0,f1,f2,missing";
static void gate(const char *what,int ok){printf("%s: %s\n",what,ok?"PASS":"FAIL");if(!ok)failures++;}
static int invoke(size_t fail,char **out,int *cause){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);MUST(fp);FILE *save=g_out;g_out=fp;
    calls=0;nth=fail;released=0;armed=1;
    int rc=probe_cmd_get_fields("root","obj","a",1,fields);
    *cause=errno;armed=0;g_out=save;MUST(!fclose(fp));
    gate("command selected buffers released",live==0);gate("registry reference released once",released==1);return rc;
}
static void cells(void){
    FieldSchema fs={.ts=fixture};
    for(int mode=0;mode<4;mode++){
        calls=0;nth=0;armed=1;errno=0;
        char *out=mode==0?typed_packed_projected_cell(fixture,record,record_len,generation,0,SIZE_MAX):
            mode==1?json_projected_field((const char*)record,record_len,"f0",&fs,generation):
            mode==2?json_typed_row_cell(fixture,record,(int)record_len,0,generation):
            json_projected_field((const char*)record,record_len,"absent",&fs,generation);
        size_t count=calls;armed=0;
        gate("normal cell exact",out&&!strcmp(out,mode==3?"null":"1.2500000000"));p7_free(out);
        gate("cell allocation sweep nonempty",count>0);gate("normal cell released",live==0);
        for(size_t i=1;i<=count;i++){
            calls=0;nth=i;armed=1;errno=0;
            out=mode==0?typed_packed_projected_cell(fixture,record,record_len,generation,0,SIZE_MAX):
                mode==1?json_projected_field((const char*)record,record_len,"f0",&fs,generation):
                mode==2?json_typed_row_cell(fixture,record,(int)record_len,0,generation):
                json_projected_field((const char*)record,record_len,"absent",&fs,generation);
            int cause=errno;armed=0;gate("cell allocation failure is explicit",!out&&cause==ENOMEM);p7_free(out);gate("failed cell released",live==0);
        }
        printf("cell mode=%d allocation injections=%zu\n",mode,count);
    }
}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f){(void)s;(void)k;(void)f;return remove(p);}
int main(void){
    char root[]="/tmp/p3b7-probe-XXXXXX";MUST(mkdtemp(root));ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=3;
    const char *types[]={"numeric:26,10","varchar","bool"};
    for(int i=0;i<3;i++){if(i==0)ts->fields[i].type=FT_NUMERIC;else if(i==1)ts->fields[i].type=FT_VARCHAR;else parse_field_type("bool",&ts->fields[i]);snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    MUST(!typed_schema_bind_packed(ts,types,3,NULL,0));uint8_t *bytes=NULL;size_t n=0;
    MUST(!typed_encode_alloc(ts,"{\"f0\":1.25,\"f1\":\"a\\u0000b\",\"f2\":true}",1,NULL,NULL,&bytes,&n,NULL,0));
    fixture=ts;record=bytes;record_len=n;generation=PKR_FLAGS;cells();
    const char *body="{\"f0\":1.2500000000,\"f1\":\"a\\u0000b\",\"f2\":\"true\",\"missing\":null}";
    const char *expected="{\"f0\":1.2500000000,\"f1\":\"a\\u0000b\",\"f2\":\"true\",\"missing\":null}\n";
    char *text=NULL;int cause=0;int rc=invoke(0,&text,&cause);size_t count=calls;
    gate("projected GET exact",rc==0&&text&&!strcmp(text,expected));free(text);gate("command sweep nonempty",count>0);
    for(size_t i=1;i<=count;i++){
        rc=invoke(i,&text,&cause);gate("command allocation failure explicit",rc==1&&cause==ENOMEM);
        gate("one error with no success prefix",text&&!strcmp(text,"{\"error\":\"out of memory\"}\n"));free(text);
    }
    printf("projected command allocation injections=%zu\n",count);
    fields="f0,f1+f2";rc=invoke(0,&text,&cause);gate("composite request refuses atomically",rc==1&&cause==ENOTSUP&&text&&!strcmp(text,"{\"error\":\"packed composite projections await integration\"}\n"));free(text);
    fields="f0";generation=SREC_FLAG_NULLMAP;rc=invoke(0,&text,&cause);gate("generation mismatch refuses before output",rc==1&&cause==EILSEQ&&text&&!strcmp(text,"{\"error\":\"corrupt record\"}\n"));free(text);generation=PKR_FLAGS;
    uint8_t *bad=malloc(n);MUST(bad);memcpy(bad,bytes,n);memset(bad+ts->packed.prefix+ts->packed.fields[1].offset,255,4);record=bad;
    rc=invoke(0,&text,&cause);gate("unselected corrupt descriptor refuses",rc==1&&cause==EILSEQ&&text&&!strcmp(text,"{\"error\":\"corrupt record\"}\n"));free(text);free(bad);record=bytes;
    ts->fields[2].type=FT_LONG;fields="f0";rc=invoke(0,&text,&cause);
    gate("unselected fixed schema mismatch refuses",rc==1&&cause==EINVAL&&text&&!strcmp(text,"{\"error\":\"corrupt record\"}\n"));free(text);ts->fields[2].type=FT_BOOL;
    char *longlist=malloc(MAX_LINE+1);MUST(longlist);memset(longlist,'x',MAX_LINE);longlist[MAX_LINE]=0;fields=longlist;
    rc=invoke(0,&text,&cause);gate("overlong selection explicit",rc==1&&cause==E2BIG&&text&&!strcmp(text,"{\"error\":\"projection exceeds limit\"}\n"));free(text);free(longlist);
    fields="f0,f1,f2,missing";
    for(int newline=0;newline<2;newline++){
        Writer writer={.limit=newline?strlen(body):7};FILE *badout=writer_open(&writer);MUST(badout);MUST(!setvbuf(badout,NULL,_IONBF,0));
        FILE *save=g_out;g_out=badout;released=0;rc=probe_cmd_get_fields("root","obj","a",1,fields);g_out=save;
        gate(newline?"newline failure returned":"partial write failure returned",rc==1&&released==1);
        gate("no replacement error after I/O prefix",writer.accepted==writer.limit&&!writer.error_attempted);fclose(badout);
    }
    FILE *ro=fopen("/dev/null","r");MUST(ro);MUST(!setvbuf(ro,NULL,_IONBF,0));FILE *save=g_out;g_out=ro;released=0;rc=probe_cmd_get_fields("root","obj","a",1,fields);g_out=save;
    gate("readonly output failure detected",rc==1&&ferror(ro)&&released==1);fclose(ro);
    TypedSchema *maximum=calloc(1,sizeof(*maximum));MUST(maximum);maximum->typed=1;maximum->nfields=MAX_FIELDS;const char *tokens[MAX_FIELDS];
    for(int i=0;i<MAX_FIELDS;i++){tokens[i]="varchar";maximum->fields[i].type=FT_VARCHAR;snprintf(maximum->fields[i].name,sizeof(maximum->fields[i].name),"f%d",i);maximum->fields[i].name_len=(int)strlen(maximum->fields[i].name);}
    MUST(!typed_schema_bind_packed(maximum,tokens,MAX_FIELDS,NULL,0));MUST(maximum->total_size>1000000000);
    uint8_t *small=NULL;size_t small_n=0;MUST(!typed_encode_alloc(maximum,"{\"f127\":\"tiny\"}",1,NULL,NULL,&small,&small_n,NULL,0));
    fixture=maximum;record=small;record_len=small_n;fields="f127,absent";largest=0;rc=invoke(0,&text,&cause);
    gate("max schema projection actual-size",rc==0&&small_n<4096&&largest<4096&&text&&!strcmp(text,"{\"f127\":\"tiny\",\"absent\":null}\n"));printf("maximum actual=%zu largest=%zu live=%zu\n",small_n,largest,live);free(text);free(small);free(maximum);
    fixture=ts;size_t big_n=4u*1024*1024;char *content=malloc(big_n);MUST(content);memset(content,'x',big_n);TypedPrepared prepared;typed_prepared_init(&prepared);
    MUST(!typed_prepared_zero(ts,&prepared,NULL,0));TypedInput input={content,big_n,1,0,0};MUST(!typed_prepared_set(&prepared,1,&input,NULL,0));
    uint8_t *big=NULL;size_t encoded=0;MUST(!typed_prepared_alloc(&prepared,&big,&encoded,NULL,0));typed_prepared_free(&prepared);free(content);
    record=big;record_len=encoded;fields="f2";largest=0;rc=invoke(0,&text,&cause);
    gate("unselected 4MiB does not allocate full JSON",rc==0&&largest<4096&&text&&!strcmp(text,"{\"f2\":\"false\"}\n"));printf("large unselected actual=%zu largest=%zu live=%zu\n",encoded,largest,live);free(text);free(big);
    printf("probe failures=%d\n",failures);free(bytes);free(ts);shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return failures?1:0;
}
```

### manifest

<!-- block:manifest -->
```json
{
  "base_head": "decbee6a18fc950bbf18e2f4ce6ad68956cad554",
  "ancestor": "decbee6a18fc950bbf18e2f4ce6ad68956cad554",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "parent_sources": {
    "build.sh": "488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0",
    "src/db/typed_row.h": "389b9d9b13e4c96f620c2ce851a29f1985e79781c6df0726604ec4ebd6131da7",
    "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
    "src/db/query_find.c": "1c2a85af1ef94eb8edf513819e29dbdb5020cdd3c1afc7b68a602f5ed5ec6660",
    "src/db/storage.c": "3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab",
    "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
    "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
    "src/db/packed_record.c": "8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d",
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f"
  },
  "blocks": {
    "projection-test": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe",
    "build-register-old": "76ad21f0d1827951551a286c534ceaf2d5892be653cbda3d0fe8875e86c2299f",
    "build-register-new": "9bbc9d6c9a9f22db02a1ee9ce9ea2424544603b0f087f383dc48ed7a73d1b4bb",
    "projection-header-old": "0d00bcb1232f3479ebf435ee9a8dfdbfd8ea5beb91f3e942aab661f4aa85e1fc",
    "projection-header-new": "0da3a9596c4fa32e1863507ea598a7692e2cda1b881fb0789de4e88fdd6b9fc9",
    "projection-stubs-old": "1631af91e63177d11d8a6346f9781340a994cdcdc7d94a27f370a4fc11c99b24",
    "projection-stubs-new": "c27555d5dbc0ec9db543c5ada86d38cc6a2039d11591e73992703ef0a0f39e89",
    "projection-module-old": "f6f5c5adecad47f4cbf2bcfd3e39949f01f5ae90b6b756f321a700c4cb282e27",
    "projection-module-new": "e5bc37b88673ef52d603954cc7b8713a6d944e33cd41b505396cf2775ab1bb4e",
    "projection-edit-1-old": "d02e65283aa742e0f195ab226e375b16f2a3f5b36fde32e14274c73d1b36a4af",
    "projection-edit-1-new": "10d48b926d15044491163353b50d7c07bc1541011b03c1387d5611eecafb5ef8",
    "projection-edit-2-old": "62b2e2cd265188c35b1f5a9b4fcccd8cf8f63fc712ecc2dbc116d3d0cf1e1516",
    "projection-edit-2-new": "bab0760412c0b5ba66eb1de205da58d8128dae1feccfe170692e169d5c7cbc05",
    "projection-edit-3-old": "5cfca75fb56137e5ab6a49c671cb9a9aad51ce5efa78b44a22693dd9f9bde802",
    "projection-edit-3-new": "93aaae708ffeecbcbb61617075c52f86f4b9473060279ee8990efbb3512d797e",
    "projection-edit-4-old": "99fe012f9c95e68df95bc7b799f0a30fdc10a59a7fc656dc29fe6e1d6f09d05b",
    "projection-edit-4-new": "80103c5af94b8f07dd070e42f85a0480395951b525f4bc9860816b9ed62b6acb",
    "schema-width-guard-old": "3cd5aea6d04b5977acc0d7e39d8599f5e163d3865ba765a23f600902d91f5323",
    "schema-width-guard-new": "b13829af26b0dfe4ad1ee27e4ade820062eef54d3dc43a6ca4ddbe12cf7f1e6f",
    "projection-doc": "8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62",
    "adapter-doc-old": "f1fd430f6ae836f751512201348bc8ffba0344978f2e2a62c002155f96b211d7",
    "adapter-doc-new": "d8b849d10c1630ae23e19564b5610c0e3fe2da95e108285c0c28ff7cb58801ab",
    "whole-doc-old": "b68c43b5967d51ae3cb9016dcf1fa2ccab4d497eef6e4c1a331ff29c260ad7a6",
    "whole-doc-new": "748ce7525e96a636c32eeb21ed913d2438a74f961237f02547b95ccf0f115137",
    "driver": "b7fca19b11756a70574a7625716fc145966603c1f010716840ab0be2644ff6eb",
    "checks": "0d7e8453af15bd443fc293c1aeaa6282ae7b2ea8183c557f3f7a10afb5686153",
    "projection-probe": "eb8332f8ba3891cd5cf54df78f638395f47e3732d8112ad71ce145fd70bd5191",
    "bootstrap": "0dd7b108c128c191dcf9c6e6a0d1d4bec8b4d29496e94ab20898d5452b6f437a"
  },
  "edits": {
    "1": [
      {
        "path": "src/test/cases/test_packed_json_projection.c",
        "old": null,
        "new": "projection-test"
      },
      {
        "path": "build.sh",
        "old": "build-register-old",
        "new": "build-register-new"
      },
      {
        "path": "src/db/typed_row.h",
        "old": "projection-header-old",
        "new": "projection-header-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "projection-stubs-old",
        "new": "projection-stubs-new"
      }
    ],
    "2": [
      {
        "path": "src/db/typed_row.c",
        "old": "projection-module-old",
        "new": "projection-module-new"
      },
      {
        "path": "src/db/query_find.c",
        "old": "projection-edit-1-old",
        "new": "projection-edit-1-new"
      },
      {
        "path": "src/db/query_find.c",
        "old": "projection-edit-2-old",
        "new": "projection-edit-2-new"
      },
      {
        "path": "src/db/query_find.c",
        "old": "projection-edit-3-old",
        "new": "projection-edit-3-new"
      },
      {
        "path": "src/db/storage.c",
        "old": "projection-edit-4-old",
        "new": "projection-edit-4-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "schema-width-guard-old",
        "new": "schema-width-guard-new"
      }
    ],
    "3": [
      {
        "path": "docs/concepts/packed-json-projection.md",
        "old": null,
        "new": "projection-doc"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "adapter-doc-old",
        "new": "adapter-doc-new"
      },
      {
        "path": "docs/concepts/whole-record-readers.md",
        "old": "whole-doc-old",
        "new": "whole-doc-new"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "build.sh": "488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0",
      "docs/concepts/packed-json-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "4020adc7a8295014702e6288994d9d643be86e4faa2f089d2a35b14556b03f21",
      "docs/concepts/whole-record-readers.md": "62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22",
      "src/db/query_find.c": "1c2a85af1ef94eb8edf513819e29dbdb5020cdd3c1afc7b68a602f5ed5ec6660",
      "src/db/storage.c": "3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab",
      "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
      "src/db/typed_row.h": "389b9d9b13e4c96f620c2ce851a29f1985e79781c6df0726604ec4ebd6131da7",
      "src/test/cases/test_packed_json_projection.c": null
    },
    "1": {
      "build.sh": "238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e",
      "docs/concepts/packed-json-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "4020adc7a8295014702e6288994d9d643be86e4faa2f089d2a35b14556b03f21",
      "docs/concepts/whole-record-readers.md": "62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22",
      "src/db/query_find.c": "1c2a85af1ef94eb8edf513819e29dbdb5020cdd3c1afc7b68a602f5ed5ec6660",
      "src/db/storage.c": "3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab",
      "src/db/typed_row.c": "f7c31f80608600da5e3d5f923c19ef1d9bd55e27e26c9908164e129f689950cb",
      "src/db/typed_row.h": "c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602",
      "src/test/cases/test_packed_json_projection.c": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe"
    },
    "2": {
      "build.sh": "238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e",
      "docs/concepts/packed-json-projection.md": null,
      "docs/concepts/typed-row-adapter.md": "4020adc7a8295014702e6288994d9d643be86e4faa2f089d2a35b14556b03f21",
      "docs/concepts/whole-record-readers.md": "62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22",
      "src/db/query_find.c": "8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc",
      "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
      "src/db/typed_row.c": "71d5e928af46a46fe304bf8f34b87831d0f465c758971699a98bf06c154411b1",
      "src/db/typed_row.h": "c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602",
      "src/test/cases/test_packed_json_projection.c": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe"
    },
    "3": {
      "build.sh": "238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e",
      "docs/concepts/packed-json-projection.md": "8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62",
      "docs/concepts/typed-row-adapter.md": "736ba37fda8ac618815989e0eb452d859422885e8c147f89b56d36938ee45a6a",
      "docs/concepts/whole-record-readers.md": "dcba70251ac6366042ccf6075f5342dbfda999243d22f667a31adb27127f00f0",
      "src/db/query_find.c": "8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc",
      "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
      "src/db/typed_row.c": "71d5e928af46a46fe304bf8f34b87831d0f465c758971699a98bf06c154411b1",
      "src/db/typed_row.h": "c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602",
      "src/test/cases/test_packed_json_projection.c": "7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe"
    }
  }
}
```

## Appendix C — Revision 1 planning verification (historical)

All source execution below occurred outside the root checkout; no Phase 3B.7
production implementation, staging or commit occurred. The approved Phase3B.6
implementation was cleaned and committed separately as decbee6 before planning.

| Check | Measured result / raw log |
|---|---|
| Initial checkpoint1 normal build | zero warnings, exit0; /tmp/p3b7-red-build.log |
| Initial focused RED | 35/133, exit133; /tmp/p3b7-red-test.log |
| Exact final code normal build | zero warnings, exit0; /tmp/p3b7-green-build.log |
| Focused GREEN | 170/0, exit0; /tmp/p3b7-green-test.log |
| Fresh normal full suite | 20215/0 across525 cases, exit0; /tmp/p3b7-green-full.log |
| Frozen green checks | all119 pass; cell allocations6+6+6+4, command9; /tmp/p3b7-frozen-green.log |
| Frozen scratch reversal/reapply | checkpoint1 hashes, 20/19 gates; checkpoint2 hashes,119/0; /tmp/p3b7-proof.log |
| Fresh frozen checkpoint1 replay | zero-warning build,35/133 exit133; /tmp/p3b7-replay-red-build.log, /tmp/p3b7-replay-red-test.log |
| Fresh frozen RED probe | 20/19 gates, expected exit1 handled; /tmp/p3b7-frozen-red.log |
| Driver preflight in root | stage1 --check-only:4 files verified, no writes |
| Extraction/source verification | all27 blocks, Python syntax, every quoted anchor and all nine checkpoint3 final file hashes verified |

The fresh replay directory is /tmp/p3b7-replay-p1o42g55, a git archive of the
committed base with only manifest stages applied. Stage1 was built and tested
before the frozen RED checks; stages2/3 were then applied from the same frozen
blocks and every resulting hash matched the previously compiled/green-tested
copy. The root driver was exercised only in --check-only mode, while the scratch
replay applied its manifest's exact substitutions and verified each checkpoint;
this is not a claim that a real root execution/approval occurred. The ordinary
full suite used the exact final code before the documentation stage; final docs
were then applied and byte-verified. Executor still runs Task3's fresh full suite
in the stated order after docs. There is no additional planning-time sanitizer,
benchmark, public metadata binding or certification activity.

Maximum-schema and unselected-large-value tracked allocations are reported
separately from mocked raw-read storage. No raw logs/probe tools belong in the
commit. Protected-file hash was checked again after writing the plan and remains
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.

## Appendix D — Revision 2 blind review verification (current)

Revision 2 supersedes Revision 1's code/test/checkpoint hashes and gate counts.
The historical Appendix C remains an account of the original planning run,
not executor instructions. Current frozen code lives in Appendix B and current
commands/counts in Tasks1–3/Appendix A.

| Check | Actual result / raw log |
|---|---|
| Guard regression RED against complete R1 code |175/43, exit43; /tmp/p3b7-r2-guard-red-test.log |
| Guard-only actual-command RED |121/1; /tmp/p3b7-r2-guard-red-probe.log |
| Amended whole-plan fresh checkpoint1 build/RED |zero warnings,40/176, exit176; /tmp/p3b7-r2-replay-red-build.log and /tmp/p3b7-r2-replay-red-test.log |
| Amended frozen RED probe |22/20, expected exit1; /tmp/p3b7-r2-replay-red-probe.log |
| Final repaired build/focused |zero warnings,218/0; /tmp/p3b7-r2-green-build.log and /tmp/p3b7-r2-green-test.log |
| Fresh full normal suite |20263/0 across525 cases, exit0; /tmp/p3b7-r2-full.log |
| Final frozen green checks |122/0,31 allocation injections; /tmp/p3b7-r2-green-probe.log |
| Frozen scratch reversal/reapply |checkpoint1 hash match22/20, then checkpoint2 hash match122/0; /tmp/p3b7-r2-proof.log |

The independent repair reviewer rechecked all29 frozen blocks/anchors and
checkpoints. Planner verified the root driver stage1 --check-only, all scripts,
all nine final source/docs hashes and the protected hash. New fixed-schema
validation allocates nothing; the established allocation counts/bounds remain
6+6+6+4+9, max-schema2085/30 and unselected4MiB4194339/15, tracked live0.
The full suite used the exact final code; documentation was subsequently
byte-verified. Executor still runs its fresh Task3 full suite after stage3.

Planning copies: /tmp/p3b7-plan-nvaml3ww (final compiled/tested code) and
/tmp/p3b7-r2-replay-4kqq1vgc (fresh amended checkpoint1). Root source remains at
committed decbee6; only planning/index/review documents change in this turn.
No sanitizers, benchmarks, Git writes or public packed binding. Execution must
still stop uncommitted for plan-aware and independent blind raw-diff reviews.
