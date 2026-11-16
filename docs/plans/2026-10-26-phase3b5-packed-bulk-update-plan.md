# Phase 3B.5 — packed keyed bulk partial updates

## Revision 2 — independent review fixes

- P3B5-R1: repeated dictionary/positional keys could lose disjoint partial
  patches. Screen every update storage key, add escaped-alias/delimited
  preservation regressions, and retain insert behavior. Review regression
  before fixes: 227 passed / 8 failed; all failures were genuine mutation
  or false-success checks, not test-stub rejection passes.
- P3B5-R2: a hard first-window error incorrectly erased successful later-window
  results. Fold update rows by terminal status and separately emit the partial
  request warning. A one-shot pre-marker link failure reproduced 4 response
  failures while independent reopen proved OLD then committed NEW. Add the
  complete deterministic standalone probe below; no production failure seam.
- P3B5-R3: reconcile initial RED-first with CORE-PROCESS temporary-revert proof.
  The checks runner now reverses/reapplies exact frozen fixes only in scratch
  copies, verifies hashes, and executes RED/GREEN probes. Production sources
  and checked-out binaries are never reverted.

## Primary goal

Prepare exact, compact bulk partial updates that preserve untouched financial
values and safely distinguish missing targets from failed writes, while existing
applications remain on the supported legacy format until packed integration and
migration are ready.

**Status:** Revision 2, independently reviewed and repaired; awaiting approval.
Planner: GPT 6.1 Sol. Intended executor: GLM 5.3 (per the user). Based exactly on Phase 3B.4 commit
`03617a898d5dff291c539b3ab33522054fd36cfb` on
`fix/phase1-safe-scalar-conversions`. No production implementation, Git writes,
metadata activation, benchmarks or sanitizers occur in planning. All proposed
code and regressions were compiled/executed only in scratch.

## Scope and explicit proposals

This child completes plain keyed JSON dict/array, JSON file/inline, and bounded
positional partial-update construction under private packed schemas. It reuses
the existing insertion module's collection, grouping and joined request lifetime,
with a private update mode and existing row constructors; it introduces no new
commit protocol or bulk owning hook. Its historical filename remains
`typed_bulk_insert.c` to avoid a file/build-list rename unrelated to correctness.
Existing insert API/signature stays stable and its full regression remains green.

Approval chooses these interim private boundaries: every packed per-record
`if` rejects the whole request before writes; packed criteria updates/dry runs,
auto-key objects and secondary-index writes also refuse explicitly. Legacy
public criteria/CAS/index behavior remains unchanged. This is deliberately not
claimed as complete packed bulk-update functionality or Phase 3B closure.

Plain JSON update returns existing matched/updated/skipped fields; failed row
construction additionally supplies per-key errors and returns nonzero. Missing
OLD remains a condition skip, without invoking construction/defaults. Every
update form screens decoded storage-key duplicates before any write, including
unusable JSON values. Repeated partial patches must never independently compose
against the same OLD. Insert duplicate behavior remains unchanged.
Omitted AUTO_CREATE is preserved; explicitly patched AUTO_CREATE retains current
single-update semantics rather than becoming immutable in this child.

Four narrow legacy safety repairs belong here because the census confirms real
crashes: three update request blocks copy into a possibly-NULL calloc result
before reaching their error check, and the delimiter callback memcpy's into an
unchecked heap cell buffer. Broader legacy parse OOM auditing/error-response
policies remain Phase 4, explicitly not claimed solved by these guards.

## Task 0 — documentation discovery and fixed checkpoint

**How this serves the primary goal:** fixes the real update contracts before
sharing machinery whose insertion semantics differ in important ways.

Read AGENTS.md and CORE-PROCESS.md, Phase 3 index/recovery plan (excluding the
protected certification rig), approved 3B.4 plan/evidence/raw-diff review, and
`docs/query-protocol/bulk.md`, `docs/query-protocol/cas.md`,
`docs/concepts/packed-bulk-insert.md`, `typed-row-adapter.md`, `concurrency.md`.
Read actual API contracts in typed_row.h, slotcask.h and packed_record.h.

Documentation/source discovery was gathered independently and checked by the
planner. Copy patterns from test_typed_bulk_insert.c (private schema/physical
fixture/TLS capture) and test_single_write_integration.c (concurrent disjoint
updates with thread-local g_db ownership). Existing public legacy compatibility
cases: test_bulk_update_json, test_bulk_update_delimited, test_null_bulk,
test_not_null_updates, test_computed_update, test_bulk_update_json_oom and
criterion/CAS tests. They are full-suite gates, not replacements for private
packed row tests or future public activation acceptance.

Verified allowed APIs:
- `typed_update_json_alloc(ts, old, json, &out, &len, err, cap)` applies supplied
  fields to validated OLD, stamps AUTO_UPDATE and allocates measured output.
- `typed_update_alloc(ts, old, inputs, 1, &out, &len, err, cap)` uses supplied
  TypedInput spans for positional partial updates; unsupplied is a no-op.
- `typed_row_open`, `typed_prepared_old/set/stamp/alloc`, `pkr_record_fits`;
  no new row/decimal core API or layout change is required.
- `SlotcaskBulkOpts.require_existing/value_compute/value_rewrites_payload`;
  no bulk `check`, `dry_run` or single owning options exist here.
- `slotcask_bulk_request_execute` joins pipelines before return; caller-owned
  source/context/NEW must outlive it. Nonzero compute return maps to status -2,
  so the row-local error side channel is mandatory for construction failure.
- `json_unescape_string`, `json_skip_value` only after whole JSON validation;
  standard checked open/fstat/read/close for owned file input.

Consumer/dispatch census, verified at this checkpoint:

| Surface | Consumers and consequence |
|---|---|
| Existing insert API | two production runners bulk_ins_run and bulk_ins_delim_run plus private insert test; signature stable, now thin wrapper around shared command |
| New update command | packed bulk_upd_delim_run and new source wrapper; private tests/probes pass explicit immutable schema and borrowed physical DB |
| New source wrapper | early packed bulk_upd_json_run branch, shared by cmd_bulk_update_json/_string and private file/inline fixture |
| Server dispatch | JSON `records` before `file`; criteria presence takes priority and has separate cmd_bulk_update; delimited data/file separate; no public signature or SDK change |
| Conditions | request-level if/dry_run not forwarded for keyed update forms; per-record if exists only in JSON array. Never ignore it and update unconditionally |
| Criteria command | cmd_bulk_update guards packed schema before parsing/compiling/scanning; legacy criteria, request if, limit/dry_run stay on the old path |
| Callback constructors | packed JSON uses existing typed_update_json_alloc; delimited uses existing typed_update_alloc; no caller-OLD override; no defaults rerun |
| Legacy crash sites | cmd_bulk_update, bulk_upd_delim_run, bulk_upd_json_run request input-copy loops; v2_bulk_upd_delim_value_compute heap cell copy |
| Build lists | existing module already in daemon/library/test-server/runner/bench lists, all five retained; only the new test registration is added |
| Recovery/metadata | coordinator/markers/record flags unchanged; loader/create-object binder still absent; no format publication/migration/activation |

Specific root causes and invariants:
1. Legacy update constructors allocate schema-maximum slabs, set vlen=total_size
   and patch raw scalar offsets. Packed descriptors require the existing
   validated OLD/actual-size row APIs instead. No new raw-offset packed access.
2. Delimited blanks mean omitted on update, not zero/empty on insertion. Missing
   trailing cells also preserve OLD. Unknown/removed fields do not consume
   removed slots; AUTO_UPDATE stamps after patches. JSON NULL and empty varchar
   remain explicit, unlike unsupplied delimited cells.
3. `value_compute` nonzero is a condition skip in transport. A row error side
   channel distinguishes decode/range/not-null/size/OOM/generation failures;
   `require_existing=1` filters missing targets before calling constructors.
4. All update storage keys are collected before value/key-size validation; JSON
   dictionary and array keys are decoded, delimiter keys stay byte-exact. Reject
   duplicates before any coordinator call. Do not lose malformed duplicate
   references by dropping the row before duplicate screening. Partial patches
   otherwise compose independently against the same OLD and discard fields.
5. Old JSON array insertion ignores unknown metadata, which is unsafe if reused
   blindly for update `if`. Any present decoded array `if` in update mode aborts
   before writes; prior parsed rows are freed. No accidental unconditional CAS.
6. Each row/context stays stable after shard/ordinal sorting and survives the
   entire synchronous request, including error/pending paths. NEW remains
   caller-owned; existing durable replay derives bytes from segments.
7. Physical fit uses actual key/value, schema/slot and unchanged segment cap.
   The >1 GiB schema bound must never become a temporary small-row allocation.
8. Three legacy loops currently dereference `inputs[k++]` after calloc failure
   before their NULL branch; guard the copy loop. Heap positional cell copy
   must reject NULL before memcpy. These fixes retain existing legacy skip
   policy; they do not certify every old allocation or response contract.
9. A shard rc=-1 can coexist with completed rows: one pre-marker window fails,
   while later windows commit and clear. For packed updates, records start at
   status -1, failed unpublished windows reset zero statuses to -1, and retained
   published markers produce shard rc=-2. After execute with no pending shard,
   status0 therefore identifies a completed row even when shard rc=-1. Fold
   update counts per row and report the request failure separately; leave the
   existing insert fold unchanged. Never claim counts on pending durability.

Execution rules:
- Await explicit human approval of this exact written plan. Execute only on
  `fix/phase1-safe-scalar-conversions`, HEAD
  `03617a898d5dff291c539b3ab33522054fd36cfb`; no branch/reset/Git writes.
- Record start/end HEAD/branch, approved plan SHA and protected hash. Protected
  `docs/plans/2026-10-19-durability-certification-rig.md` is hash-only; never read
  contents, edit/move/stage/incorporate/execute it. Driver only hashes bytes.
- Inventory pre-existing planning artifacts. Owned sources/docs must match
  checkpoint 0; preserve unrelated user/planning work.
- Extract Appendix A blocks verbatim into fresh /tmp. Use driver --check-only
  before mutation and apply stages1/2/3 only after all stage anchors and hashes
  preflight. No hand transcription or guesses. Any mismatch, unexpected warning,
  failing result, flaky failure or uncovered decision means append PLAN_NOTES.md
  and halt the entire run. Planner/human must read it and amend before resume;
  execution never resumes on its own initiative.
- RED first: add full tests plus explicit update stubs and registration, leaving
  query_bulk unchanged; build and capture focused/legacy RED before stage2.
  Never replace initial RED-first with later reversion or weaken assertions.
  After GREEN, run the separate scratch-only temporary-revert/reapply proof
  below, as CORE-PROCESS requires for bug fixes. It must never mutate checkout
  sources, rebuild the checked-out binaries, or substitute for initial RED.
- Shell always rtk prefixed. Normal builds only:
  `rtk proxy env SKIP_TESTS=1 ./build.sh`; tests through build/bin/shard-db-test.
  Use socket-capable tool permissions if necessary for full suite; never skip
  server cases or silently change the test mode.
- No sanitizers or benchmarks; branch-series sanitizer gates remain after all
  fixes immediately before authorized push/PR. Benchmark binary build is okay.
- Stop unstaged/uncommitted for actual plan-aware and independent blind
  raw-diff reviews; no add/commit/push/PR/merge or later implementation phase.
  Store raw logs/probes/binaries only in /tmp; curated evidence goes in Markdown.

## Task 1 — executable RED against stubs and unchanged legacy paths

**How this serves the primary goal:** proves the intended partial-update behavior
is missing and confirms legacy crash mechanisms before adding the fix.

Apply stage1 through the extracted driver. It changes four files: two new internal
API declarations, explicit ENOTSUP update stubs appended to the existing insert
module, the complete new private physical test, and one build registration.
No new module list is needed; all five existing typed_bulk_insert.c lists stay.

Zero-warning normal build, then focused test must be **200 passed / 35 failed**,
exit35. Its seeds still use working insertion; named RED assertions demonstrate
missing outcomes, partial/NULL/exact/string handling, file input, positional
updates, maximum-size growth/shrink and both disjoint concurrent patches. Do not
count incidental rejection passes on the stubs as validation evidence.

Run the checks runner --mode red before stage2. It compiles actual query_bulk
with scratch-only malloc/calloc substitutions and source-extracted request blocks,
not reimplemented decision logic or a production failure seam. Five isolated
children must fail: three request-copy loops and cell-copy SIGSEGV, plus the
missing packed criteria refusal. The runner treats probe exit1 as expected RED;
record raw output **0 passed / 5 failed**, with signals11 for cases0–3. A crash
outside those deliberately isolated baseline reproductions is not an accepted RED.

## Task 2 — shared keyed command mode and narrow legacy guards

**How this serves the primary goal:** composes safe partial NEW from locked OLD
through the existing bulk request instead of constructing truncated/fixed rows.

Apply stage2 exact blocks: full shared module implementation; seven unique
query_bulk hunks (JSON route, positional route, early criteria guard, three
request-copy guards and heap-cell guard). Every changed/new function and hunk is
complete below. No typed_row code/signature changes. No module rename.

Update mode sets require_existing=1 and value_rewrites_payload=1. JSON and
positional constructors use their existing distinct supplied-field semantics.
It does not stamp AUTO_CREATE, rerun defaults, mutate schemas or expose aliases.
Whole JSON validation precedes collection; all JSON updates track every decoded
key in dictionaries and arrays; positional updates track each storage key too.
Reject duplicates even when a patch would be unusable. Condition fields
are refused, not silently ignored. Keys in error output use a bounded byte-span
JSON writer rather than a fallible post-write escaping allocation.

File/inline JSON enters the packed branch before legacy field/criteria setup.
The source wrapper owns exactly st_size+1 input bytes for files, handles EINTR,
fails on short reads, closes before execute, and frees input after synchronous
return. File imports and embedded requests retain existing ingress policies;
this adds no new QUERY_BUFFER_MB command-wide cap. Actual payload expansion
budgeting remains explicit later activation work.

Rebuild zero-warning. Focused GREEN **235/0**, exit0. The original packed insert
regression remains unchanged and must still pass **178/0**. Green checks must
produce five legacy/criteria passes and **112** allocation failures with exact
OLD/absence/count/position/live0 preservation: dict existing26/missing8,
array existing26, positional existing25, file existing27. No new production
allocator seam. Compile the four selected objects at ordinary O1, without LTO
and without TEST_BUILD when linking the normal static library. Maximum-schema
partial update actual2091 / largest selected3072 (<8 MiB probe guard).

The green checks also run the standalone partial-outcome/duplicate probe:
19 gates must pass. It fails exactly one first-window marker link in one shard
with window size1, independently reopens OLD/NEW, checks response counts/keyed
errors/partial warning, and tests dictionary/delimited rejection with exact
OLD/count/position preservation. Link-wrapper injection is scratch-only.

After GREEN run --mode regression-proof. This satisfies CORE-PROCESS's required
temporary-revert/fail/reapply/pass proof **in scratch copies only**. The runner
reverses all seven frozen query hunks, verifies exact committed-source hash,
compiles the actual copied source and requires legacy/criteria 0/5 with four
SIGSEGV children; it reapplies them, verifies the fixed hash and requires 5/0.
It also reverses the four exact review-fix hunks in a copied bulk module, links
that object ahead of the normal static library, and requires the independent
partial/duplicate probe to fail with its expected defect classes (10 failed
gates); reapply/check hash/recompile must pass all19 gates. Neither checked-out
source nor its build products are changed. Preserve both transcripts.

## Task 3 — document partial update, conditions and remaining boundaries

**How this serves the primary goal:** keeps later consumers and application users
from assuming a guarded private integration is fully available public support.

Apply stage3: complete new packed-bulk-update concept doc and exact links/wording
in packed-bulk-insert.md and typed-row-adapter.md. Explicitly distinguish blank
positional cells, JSON explicit NULL/empty, current AUTO_CREATE patch policy,
AUTO_UPDATE stamping, duplicate/error/missing semantics, lifetime, conditions,
file reads, memory limits and legacy crash-fix scope. Do not silently remove
remaining query/CAS/index/metadata/migration obligations.

## Task 4 — final verification and hand-back

**How this serves the primary goal:** checks the full existing application behavior
alongside the new physical partial-update paths before any commit or activation.

Run a fresh normal full suite after stage3: **19878 passed / 0 failed across
523 cases**, exit0. Repeat the extracted green checks, compare every final owned
file hash, run git diff --check and verify no public packed binding/default schema
switch appeared. Verify both exported update APIs and all guards/callers match
Appendix B. No TODO/FIXME/debug prints or temporary artifacts in the diff.

Record start/end identity/compiler and protected hash, actual commands/transcripts,
RED/GREEN results, final checkpoint hashes, source caller census and any deviations
in `docs/plans/2026-10-26-phase3b5-packed-bulk-update-evidence.md`. Evidence is a
curated actual transcript, not a prefilled claim; raw logs/probe sources/objects/
binaries remain in /tmp. Stop for both executed raw-diff reviews. Plan validation
is not a substitute for review of the actual implementation.

## Planning-time evidence and limitations

Ordinary scratch only at `/tmp/p3b5-plan-xizlzd9a`, from git archive of the exact
base. The untracked protected file was not copied or inspected.
- Stage1 normal build zero-warning; real focused RED **200/35**, rc35, captured
  before applying the module/query implementation in scratch.
- Final normal build zero-warning; focused **235/0**; normal full suite
  **19878/0 across523**, rc0; prior insertion case **178/0** inside that suite.
- Allocation probe **112** passes (26+8+26+25+27); maximum actual2091, largest3072,
  all selected live buffers0. This injects selected modules only, not libc/core.
- Legacy/criteria proof independently tested against untouched production source
  in the root: **0/5** with four SIGSEGV reproductions. Against scratch fix:
  **5/0**. Those isolated child crashes are the deliberate fault oracle.
- A first scratch harness incorrectly undefined calloc substitution before the
  extracted blocks, producing vacuous request failures (no signal). Corrected
  substitution scope yielded genuine crashes on baseline and five green cases;
  only the corrected complete script below is normative. It never modified
  production code or added a production seam. No silent assertion weakening.
- Independent review reproduced dictionary/delimited mutation **227/8** before
  the review fixes and the first-window failure's four false response claims.
  Final scratch temporary-revert/reapply proof is mandatory and separately
  captured query 0/5 then5/0 and review regression10 failed gates then0.
- New test coverage is private explicit-schema integration. Public packed
  command acceptance, full per-record CAS/criteria/index support, command-wide
  expansion budgeting and remaining legacy OOM/error audits are not certified.
No sanitizers, benchmarks, production builds, production implementation edits,
staging, commit, push or metadata activation occurred while planning.

## Following work

After this child's execution/reviews/commit, continue the remaining 3B read/query/
maintenance/delete consumer inventory; split further where interfaces or failure
proofs make it necessary. Criteria/CAS remain guarded until their readers and
exact Phase5 semantics are ready. 3C owns durable metadata/publication without
premature activation; Phases4–6 finish writes/query/index/aggregates, Phase7
remaining type contracts, Phase8 transactional conversion/migration, Phase9
application acceptance/docs/activation and final reviews. Run branch-final
sanitizers only immediately before an explicitly authorized push/PR. Protected
durability certification remains separate work after this roadmap lands.

## Appendix A — bootstrap and exact invocation order

Extract to a fresh /tmp directory with the bootstrap block below, then substitute
its printed directory for /tmp/p3b5-exec-XXXX in these commands. Capture unfiltered
stdout/stderr and exit status in that folder. Focused RED exit35 is required;
the checks runner handles expected legacy probe exit1 internally. Do not guess
counts or continue after unexpected output. --check-only performs no source writes.

<!-- block:bootstrap -->
```python
import pathlib,re,tempfile
plan=pathlib.Path('docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md')
folder=pathlib.Path(tempfile.mkdtemp(prefix='p3b5-exec-'))
for m in re.finditer(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text()):
    (folder/(m.group(1)+'.txt')).write_text(m.group(2)+'\n')
for name in ['driver','checks']:
    (folder/(name+'.py')).write_bytes((folder/(name+'.txt')).read_bytes())
print(folder)
```

~~~sh
rtk proxy python3 /tmp/p3b5-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --stage 1 --check-only
rtk proxy python3 /tmp/p3b5-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-bulk-update
rtk proxy python3 /tmp/p3b5-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --mode red
rtk proxy python3 /tmp/p3b5-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-bulk-update
rtk proxy ./build/bin/shard-db-test run test-typed-bulk-insert
rtk proxy python3 /tmp/p3b5-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --mode green
rtk proxy python3 /tmp/p3b5-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --mode regression-proof
rtk proxy python3 /tmp/p3b5-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 /tmp/p3b5-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md --mode green
rtk git diff --check
~~~

## Appendix B — exact complete code, anchors and file checkpoints

Each edit names full old/new blocks; new files require absence. The driver
preflights the entire stage and its expected complete file hashes before writing.
No prose-only implementation task is delegated to the executor. Existing
ancestor/base pins intentionally continue this reviewed branch; no new checkout
is required or authorized.

### bulk-header-old

<!-- block:bulk-header-old -->
```c
#ifndef TYPED_BULK_INSERT_H
#define TYPED_BULK_INSERT_H
#include "typed_row.h"
/* Internal, synchronous command adapter. Never binds or publishes schemas.
 * borrows schema/input/optional database through return. Production passes NULL
 * for borrowed; tests pass a private physical object with the same geometry.
 * format=0 JSON dict/array, format=1 positional delimited input.
 * Packed auto-key and secondary indexes fail closed until later integrations.
 */
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists);
#endif
```

### bulk-header-new

<!-- block:bulk-header-new -->
```c
#ifndef TYPED_BULK_INSERT_H
#define TYPED_BULK_INSERT_H
#include "typed_row.h"
/* Internal, synchronous command adapter. Never binds or publishes schemas.
 * borrows schema/input/optional database through return. Production passes NULL
 * for borrowed; tests pass a private physical object with the same geometry.
 * format=0 JSON dict/array, format=1 positional delimited input.
 * Packed auto-key and secondary indexes fail closed until later integrations.
 */
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists);
/* Partial updates require OLD; omitted fields stay unchanged. Positional
 * empty/missing cells are unsupplied, JSON empty/null remain explicit values.
 * Unsupported packed if/criteria/index/auto-key semantics fail closed.
 * source() borrows inline input or owns file bytes through synchronous return. */
int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter);
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file);
#endif
```

### bulk-stubs-old

<!-- block:bulk-stubs-old -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    const char *line;
    size_t line_len;
    int shard,error;
    uint8_t *owned;
    const TypedSchema *ts;
    const Schema *sc;
    const char *root,*object;
} BulkRow;
typedef struct {BulkRow *rows;size_t count,cap,errors;} BulkRows;
static const char *bulk_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int bulk_fail(int code){errno=code;return -1;}
static char *bulk_copy(const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    char *s=malloc(n+1);if(!s)return NULL;
    memcpy(s,p,n);s[n]=0;return s;
}
static void bulk_free(BulkRows *b){
    for(size_t i=0;i<b->count;i++){free(b->rows[i].key);free(b->rows[i].json);free(b->rows[i].owned);}
    free(b->rows);memset(b,0,sizeof(*b));
}
/* Ownership of key/json transfers only on success. Row pointers are not exposed
 * to the engine until collection and sorting are complete. */
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(!key||!kn||kn>(size_t)sc->max_key||(!json&&!line)){
        b->errors++;free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16];compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
static int bulk_key(const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    return json_unescape_string(p+1,(size_t)(end-p-2),key,len);
}
static int bulk_expired(const QueryDeadline *d){
    return d->timeout_ms&&now_ms_coarse()-d->t0_ms>(uint64_t)d->timeout_ms;
}
/* Whole-document validation happens before this collector; skip_value never
 * sees malformed input. Non-object values are row errors, not partial JSON. */
static int bulk_json(BulkRows *b,const char *text,const Schema *sc,QueryDeadline *dl){
    if(typed_json_validate(text))return -1;
    const char *p=bulk_space(text);int dict=*p=='{';
    if(!dict&&*p!='[')return bulk_fail(EINVAL);
    char close=dict?'}':']';p=bulk_space(p+1);
    while(*p!=close){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        char *key=NULL,*value=NULL;size_t kn=0;
        const char *v=NULL,*ve=NULL,*end=NULL;
        if(dict){
            end=json_skip_value(p);
            if(bulk_key(p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(v&&*v=='{'){
            value=bulk_copy(v,(size_t)(ve-v));
            if(!value){free(key);return -1;}
        }
        if(bulk_add(b,key,kn,value,NULL,0,sc)){free(key);free(value);return -1;}
        p=bulk_space(p);if(*p==',')p=bulk_space(p+1);
    }
    return 0;
}
static int bulk_delimited(BulkRows *b,const char *data,size_t len,
    const Schema *sc,char delimiter,QueryDeadline *dl){
    if(!len||!delimiter||delimiter=='\n'||delimiter=='\r')return bulk_fail(EINVAL);
    size_t at=0;
    while(at<len){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        size_t start=at;while(at<len&&data[at]!='\n'&&data[at]!='\r')at++;
        size_t end=at;
        if(at<len&&data[at]=='\r')at++;
        if(at<len&&data[at]=='\n')at++;
        if(end==start)continue;
        size_t key_end=start;while(key_end<end&&data[key_end]!=delimiter)key_end++;
        if(key_end==end)continue; /* A key alone is not a positional record. */
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        const char *line=data+(key_end<end?key_end+1:end);
        size_t ln=(size_t)((data+end)-line);
        if(bulk_add(b,key,key_end-start,NULL,line,ln,sc)){free(key);return -1;}
    }
    return 0;
}
static int bulk_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;
    if(x->shard!=y->shard)return x->shard<y->shard?-1:1;
    return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
/* Immutable delimiter and row pointer remain valid through synchronous join. */
typedef struct {BulkRow *row;char delimiter;} BulkCell;
static int bulk_value(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    BulkCell *cell=rec->user_ctx;BulkRow *r=cell->row;
    free(r->owned);r->owned=NULL;r->error=0;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;return -1;
    }
    size_t len=0;int rc;
    if(r->json)rc=typed_insert_alloc(r->ts,old,r->json,r->root,r->object,&r->owned,&len,NULL,0);
    else{
        TypedInput inputs[MAX_FIELDS]={0};size_t at=0;
        for(int i=0;i<r->ts->nfields;i++){
            if(r->ts->fields[i].removed)continue;
            size_t begin=at;while(at<r->line_len&&r->line[at]!=cell->delimiter)at++;
            size_t n=at-begin;if(at<r->line_len)at++;
            enum FieldType t=r->ts->fields[i].type;
            /* Legacy positional empty fixed scalars remain present zero.
             * Empty numeric is invalid; varchar remains present empty. */
            inputs[i]=(TypedInput){r->line+begin,n,(uint8_t)(n||t==FT_NUMERIC||t==FT_VARCHAR),0,0};
        }
        rc=typed_insert_inputs_alloc(r->ts,old,inputs,&r->owned,&len,NULL,0);
    }
    size_t physical=0;
    if(rc||len>(size_t)r->sc->max_value||
        pkr_record_fits(&r->ts->packed,len,r->klen,slotcask_seg_max_bytes(),&physical)!=DEC_OK||
        physical>(size_t)r->sc->slot_size){
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;return -1;
    }
    rec->value=r->owned;rec->vlen=len;rec->hdr2=PKR_FLAGS;return 0;
}
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    if(!root||!object||!sc||!ts||!data||format<0||format>1||!ts->packed.nfields||
        ts->nfields<1||ts->nfields>MAX_FIELDS||ts->packed.nfields!=(uint32_t)ts->nfields||
        sc->splits<1||sc->splits>MAX_SPLITS||sc->streams<1||sc->slot_size<32||sc->max_key<1||
        sc->max_value<0||ts->total_size>sc->max_value||
        (borrowed&&(borrowed->num_shards!=sc->splits||borrowed->slot_size!=sc->slot_size))){
        errno=EINVAL;OUT("{\"error\":\"invalid packed bulk input\"}\n");return 1;
    }
    char indexes[MAX_FIELDS][256];
    if(sc->auto_key!=AK_NONE||load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk auto-key and indexes await integration\"}\n");return 1;
    }
    BulkRows b={0};char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy(data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(b.count>SIZE_MAX/sizeof(SlotcaskBulkRec)||b.count>SIZE_MAX/sizeof(BulkCell)){
        errno=EOVERFLOW;goto fail;
    }
    if(!b.count){
        size_t errors=b.errors;bulk_free(&b);free(json);
        if(errors){OUT("{\"inserted\":0,\"skipped\":0,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",errors);return 1;}
        OUT("{\"inserted\":0}\n");return 0;
    }
    qsort(b.rows,b.count,sizeof(*b.rows),bulk_order);
    size_t groups=1;
    for(size_t i=1;i<b.count;i++)if(b.rows[i].shard!=b.rows[i-1].shard)groups++;
    SlotcaskBulkRec *recs=calloc(b.count,sizeof(*recs));
    BulkCell *cells=calloc(b.count,sizeof(*cells));
    SlotcaskBulkShardInput *inputs=calloc(groups,sizeof(*inputs));
    if(!recs||!cells||!inputs){free(recs);free(cells);free(inputs);errno=ENOMEM;goto fail;}
    for(size_t i=0,g=0;i<b.count;i++){
        BulkRow *r=&b.rows[i];r->ts=ts;r->sc=sc;r->root=root;r->object=object;
        cells[i]=(BulkCell){r,delimiter};
        recs[i]=(SlotcaskBulkRec){.key=r->key,.klen=r->klen,.hdr2=PKR_FLAGS,.user_ctx=&cells[i],.status=-1};
        if(!i||r->shard!=b.rows[i-1].shard){
            if(i)g++;
            inputs[g]=(SlotcaskBulkShardInput){.kf_shard_id=r->shard,.recs=recs+i,
                .kind=SLOTCASK_BULK_INPUT_UPSERT,.rc=-1,
                .opts.upsert={.if_not_exists=if_not_exists,.value_compute=bulk_value,.value_rewrites_payload=1}};
        }
        inputs[g].nrecs++;
    }
    SlotcaskSchemaInfo info={.splits=sc->splits,.slot_size=sc->slot_size,.streams=sc->streams};
    SlotcaskDb *owned SDB_REG_REF=borrowed?NULL:slotcask_registry_get(root,object,&info);
    SlotcaskDb *db=borrowed?borrowed:owned;
    if(!db){free(inputs);free(cells);free(recs);goto fail;}
    rc=slotcask_bulk_request_execute(db,inputs,groups);
    size_t inserted=0,skipped=0,errors=b.errors;int pending=0;
    for(size_t g=0;g<groups;g++){
        if(inputs[g].rc==-2)pending=1;
        for(size_t i=0;i<inputs[g].nrecs;i++){
            SlotcaskBulkRec *r=&inputs[g].recs[i];BulkCell *c=r->user_ctx;
            if(c->row->error)errors++;
            else if(r->status==-2)skipped++;
            else if(inputs[g].rc||r->status)errors++;
            else if(!r->was_update)inserted++;
        }
    }
    /* A coordinator-level failure without a per-shard status is never success. */
    if(rc&&!pending&&!errors)errors++;
    free(inputs);free(cells);free(recs);bulk_free(&b);free(json);
    if(pending){OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    if(errors){OUT("{\"inserted\":%zu,\"skipped\":%zu,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",inserted,skipped,errors);return 1;}
    if(skipped)OUT("{\"inserted\":%zu,\"skipped\":%zu}\n",inserted,skipped);
    else OUT("{\"inserted\":%zu}\n",inserted);
    return 0;
fail:{
    int saved=errno?errno:EINVAL;bulk_free(&b);free(json);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":saved==ETIMEDOUT?"query_timeout":"invalid packed bulk input");
    return 1;
}}
```

### bulk-stubs-new

<!-- block:bulk-stubs-new -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    const char *line;
    size_t line_len;
    int shard,error;
    uint8_t *owned;
    const TypedSchema *ts;
    const Schema *sc;
    const char *root,*object;
} BulkRow;
typedef struct {BulkRow *rows;size_t count,cap,errors;} BulkRows;
static const char *bulk_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int bulk_fail(int code){errno=code;return -1;}
static char *bulk_copy(const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    char *s=malloc(n+1);if(!s)return NULL;
    memcpy(s,p,n);s[n]=0;return s;
}
static void bulk_free(BulkRows *b){
    for(size_t i=0;i<b->count;i++){free(b->rows[i].key);free(b->rows[i].json);free(b->rows[i].owned);}
    free(b->rows);memset(b,0,sizeof(*b));
}
/* Ownership of key/json transfers only on success. Row pointers are not exposed
 * to the engine until collection and sorting are complete. */
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(!key||!kn||kn>(size_t)sc->max_key||(!json&&!line)){
        b->errors++;free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16];compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
static int bulk_key(const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    return json_unescape_string(p+1,(size_t)(end-p-2),key,len);
}
static int bulk_expired(const QueryDeadline *d){
    return d->timeout_ms&&now_ms_coarse()-d->t0_ms>(uint64_t)d->timeout_ms;
}
/* Whole-document validation happens before this collector; skip_value never
 * sees malformed input. Non-object values are row errors, not partial JSON. */
static int bulk_json(BulkRows *b,const char *text,const Schema *sc,QueryDeadline *dl){
    if(typed_json_validate(text))return -1;
    const char *p=bulk_space(text);int dict=*p=='{';
    if(!dict&&*p!='[')return bulk_fail(EINVAL);
    char close=dict?'}':']';p=bulk_space(p+1);
    while(*p!=close){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        char *key=NULL,*value=NULL;size_t kn=0;
        const char *v=NULL,*ve=NULL,*end=NULL;
        if(dict){
            end=json_skip_value(p);
            if(bulk_key(p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(v&&*v=='{'){
            value=bulk_copy(v,(size_t)(ve-v));
            if(!value){free(key);return -1;}
        }
        if(bulk_add(b,key,kn,value,NULL,0,sc)){free(key);free(value);return -1;}
        p=bulk_space(p);if(*p==',')p=bulk_space(p+1);
    }
    return 0;
}
static int bulk_delimited(BulkRows *b,const char *data,size_t len,
    const Schema *sc,char delimiter,QueryDeadline *dl){
    if(!len||!delimiter||delimiter=='\n'||delimiter=='\r')return bulk_fail(EINVAL);
    size_t at=0;
    while(at<len){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        size_t start=at;while(at<len&&data[at]!='\n'&&data[at]!='\r')at++;
        size_t end=at;
        if(at<len&&data[at]=='\r')at++;
        if(at<len&&data[at]=='\n')at++;
        if(end==start)continue;
        size_t key_end=start;while(key_end<end&&data[key_end]!=delimiter)key_end++;
        if(key_end==end)continue; /* A key alone is not a positional record. */
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        const char *line=data+(key_end<end?key_end+1:end);
        size_t ln=(size_t)((data+end)-line);
        if(bulk_add(b,key,key_end-start,NULL,line,ln,sc)){free(key);return -1;}
    }
    return 0;
}
static int bulk_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;
    if(x->shard!=y->shard)return x->shard<y->shard?-1:1;
    return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
/* Immutable delimiter and row pointer remain valid through synchronous join. */
typedef struct {BulkRow *row;char delimiter;} BulkCell;
static int bulk_value(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    BulkCell *cell=rec->user_ctx;BulkRow *r=cell->row;
    free(r->owned);r->owned=NULL;r->error=0;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;return -1;
    }
    size_t len=0;int rc;
    if(r->json)rc=typed_insert_alloc(r->ts,old,r->json,r->root,r->object,&r->owned,&len,NULL,0);
    else{
        TypedInput inputs[MAX_FIELDS]={0};size_t at=0;
        for(int i=0;i<r->ts->nfields;i++){
            if(r->ts->fields[i].removed)continue;
            size_t begin=at;while(at<r->line_len&&r->line[at]!=cell->delimiter)at++;
            size_t n=at-begin;if(at<r->line_len)at++;
            enum FieldType t=r->ts->fields[i].type;
            /* Legacy positional empty fixed scalars remain present zero.
             * Empty numeric is invalid; varchar remains present empty. */
            inputs[i]=(TypedInput){r->line+begin,n,(uint8_t)(n||t==FT_NUMERIC||t==FT_VARCHAR),0,0};
        }
        rc=typed_insert_inputs_alloc(r->ts,old,inputs,&r->owned,&len,NULL,0);
    }
    size_t physical=0;
    if(rc||len>(size_t)r->sc->max_value||
        pkr_record_fits(&r->ts->packed,len,r->klen,slotcask_seg_max_bytes(),&physical)!=DEC_OK||
        physical>(size_t)r->sc->slot_size){
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;return -1;
    }
    rec->value=r->owned;rec->vlen=len;rec->hdr2=PKR_FLAGS;return 0;
}
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    if(!root||!object||!sc||!ts||!data||format<0||format>1||!ts->packed.nfields||
        ts->nfields<1||ts->nfields>MAX_FIELDS||ts->packed.nfields!=(uint32_t)ts->nfields||
        sc->splits<1||sc->splits>MAX_SPLITS||sc->streams<1||sc->slot_size<32||sc->max_key<1||
        sc->max_value<0||ts->total_size>sc->max_value||
        (borrowed&&(borrowed->num_shards!=sc->splits||borrowed->slot_size!=sc->slot_size))){
        errno=EINVAL;OUT("{\"error\":\"invalid packed bulk input\"}\n");return 1;
    }
    char indexes[MAX_FIELDS][256];
    if(sc->auto_key!=AK_NONE||load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk auto-key and indexes await integration\"}\n");return 1;
    }
    BulkRows b={0};char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy(data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(b.count>SIZE_MAX/sizeof(SlotcaskBulkRec)||b.count>SIZE_MAX/sizeof(BulkCell)){
        errno=EOVERFLOW;goto fail;
    }
    if(!b.count){
        size_t errors=b.errors;bulk_free(&b);free(json);
        if(errors){OUT("{\"inserted\":0,\"skipped\":0,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",errors);return 1;}
        OUT("{\"inserted\":0}\n");return 0;
    }
    qsort(b.rows,b.count,sizeof(*b.rows),bulk_order);
    size_t groups=1;
    for(size_t i=1;i<b.count;i++)if(b.rows[i].shard!=b.rows[i-1].shard)groups++;
    SlotcaskBulkRec *recs=calloc(b.count,sizeof(*recs));
    BulkCell *cells=calloc(b.count,sizeof(*cells));
    SlotcaskBulkShardInput *inputs=calloc(groups,sizeof(*inputs));
    if(!recs||!cells||!inputs){free(recs);free(cells);free(inputs);errno=ENOMEM;goto fail;}
    for(size_t i=0,g=0;i<b.count;i++){
        BulkRow *r=&b.rows[i];r->ts=ts;r->sc=sc;r->root=root;r->object=object;
        cells[i]=(BulkCell){r,delimiter};
        recs[i]=(SlotcaskBulkRec){.key=r->key,.klen=r->klen,.hdr2=PKR_FLAGS,.user_ctx=&cells[i],.status=-1};
        if(!i||r->shard!=b.rows[i-1].shard){
            if(i)g++;
            inputs[g]=(SlotcaskBulkShardInput){.kf_shard_id=r->shard,.recs=recs+i,
                .kind=SLOTCASK_BULK_INPUT_UPSERT,.rc=-1,
                .opts.upsert={.if_not_exists=if_not_exists,.value_compute=bulk_value,.value_rewrites_payload=1}};
        }
        inputs[g].nrecs++;
    }
    SlotcaskSchemaInfo info={.splits=sc->splits,.slot_size=sc->slot_size,.streams=sc->streams};
    SlotcaskDb *owned SDB_REG_REF=borrowed?NULL:slotcask_registry_get(root,object,&info);
    SlotcaskDb *db=borrowed?borrowed:owned;
    if(!db){free(inputs);free(cells);free(recs);goto fail;}
    rc=slotcask_bulk_request_execute(db,inputs,groups);
    size_t inserted=0,skipped=0,errors=b.errors;int pending=0;
    for(size_t g=0;g<groups;g++){
        if(inputs[g].rc==-2)pending=1;
        for(size_t i=0;i<inputs[g].nrecs;i++){
            SlotcaskBulkRec *r=&inputs[g].recs[i];BulkCell *c=r->user_ctx;
            if(c->row->error)errors++;
            else if(r->status==-2)skipped++;
            else if(inputs[g].rc||r->status)errors++;
            else if(!r->was_update)inserted++;
        }
    }
    /* A coordinator-level failure without a per-shard status is never success. */
    if(rc&&!pending&&!errors)errors++;
    free(inputs);free(cells);free(recs);bulk_free(&b);free(json);
    if(pending){OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    if(errors){OUT("{\"inserted\":%zu,\"skipped\":%zu,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",inserted,skipped,errors);return 1;}
    if(skipped)OUT("{\"inserted\":%zu,\"skipped\":%zu}\n",inserted,skipped);
    else OUT("{\"inserted\":%zu}\n",inserted);
    return 0;
fail:{
    int saved=errno?errno:EINVAL;bulk_free(&b);free(json);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":saved==ETIMEDOUT?"query_timeout":"invalid packed bulk input");
    return 1;
}}

int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)data;
    (void)len;(void)format;(void)delimiter;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk update unavailable\"}\n");return 1;
}
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)input;(void)is_file;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk update unavailable\"}\n");return 1;
}
```

### update-test

<!-- block:update-test -->
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} UFixture;
static int u_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void u_close(UFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    free(f->ts);slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,u_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove private fixture");
}
static int u_open(UFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private packed schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":i==2?"numeric":i==3?"int":"timestamp";
        if(!strncmp(tokens[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else if(!strncmp(tokens[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind private packed schema");if(rc)return -1;
    if(!maximum){ts->fields[3].default_kind=DK_LITERAL;strcpy(ts->fields[3].default_val,"7");
        ts->fields[4].default_kind=DK_AUTO_CREATE;ts->fields[5].default_kind=DK_AUTO_UPDATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema bound");
    f->sc=(Schema){.splits=4,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-packed-bulk-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,4,1,(int)slot);
    f->opened=!rc;ASSERT_EQ_INT(rc,0,"open multi-shard private object");return rc;
}
static int u_call(UFixture *f,const char *data,size_t n,int delim,char separator,char **out){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_update_command(f->root,"obj",&f->sc,f->ts,&f->db,data,n,delim,separator);
    g_out=save;if(fclose(fp))return -1;return rc;
}
static int u_json(UFixture *f,const char *s,char **out){return u_call(f,s,strlen(s),0,0,out);}
static int u_seed(UFixture *f,const char *json){
    char *out=NULL;size_t n=0;FILE *fp=open_memstream(&out,&n);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,json,strlen(json),0,0,0);
    g_out=save;fclose(fp);free(out);return rc;
}
static int u_source(UFixture *f,const char *source,int file,char **out){
    size_t n=0;*out=NULL;FILE *fp=open_memstream(out,&n);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_update_source(f->root,"obj",&f->sc,f->ts,&f->db,source,file);
    g_out=save;fclose(fp);return rc;
}
static char *u_get(UFixture *f,const char *key,size_t *n){
    void *bytes=NULL;uint8_t hdr=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&bytes,n,&hdr))return NULL;
    ASSERT_EQ_INT(hdr,PKR_FLAGS,"physical generation is packed");
    char *s=typed_packed_decode(f->ts,bytes,*n,hdr,16*1024*1024);free(bytes);return s;
}
static uint64_t u_pos(UFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return p;
}
static void u_reject_input(UFixture *f,const char *data,int delim,char sep){
    size_t n=0,m=0;char *before=u_get(f,"a",&n),*out=NULL;uint64_t pos=u_pos(f),t=0,d=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t,&d),0,"count before rejection");
    ASSERT_EQ_INT(u_call(f,data,strlen(data),delim,sep,&out),1,"bad batch/row rejected");
    ASSERT_NOT_NULL(out,"error response exists");if(out){ASSERT_CONTAINS(out,"error","error response");free(out);}
    char *after=u_get(f,"a",&m);ASSERT_EQ_INT(n,m,"OLD actual size unchanged");
    if(before&&after)ASSERT_EQ_STR(after,before,"OLD exact value unchanged");
    ASSERT_TRUE(u_pos(f)==pos,"no segment reservation on rejected input");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"count after rejection");
    ASSERT_TRUE(t==t2&&d==d2,"rejected counts unchanged");free(before);free(after);
}
static void u_reject(UFixture *f,const char *json){u_reject_input(f,json,0,0);}

static void u_routes(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f0\":1234567890123456.1234567890,\"f1\":\"OLD\",\"f2\":1e-26,\"f3\":10},\"b\":{\"f3\":20}}"),0,"seed physical packed rows");
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"seed readable");char ac[80]={0};
    if(s){char *p=strstr(s,"\"f4\":");if(p)memcpy(ac,p,strcspn(p,",}"));free(s);}
    uint64_t total=0,deleted=0,t2=0,d2=0;ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"counts before updates");
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f1\":\"changed\",\"f5\":1},\"b\":{\"f3\":21},\"missing\":{\"f3\":3}}",&out),0,"dict partial updates and missing key");
    if(out){ASSERT_CONTAINS(out,"\"matched\":3","matched input shape");ASSERT_CONTAINS(out,"\"updated\":2","only existing updated");ASSERT_CONTAINS(out,"\"skipped\":1","missing key skipped");free(out);}
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"partial result");
    if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","omitted exact numeric preserved");ASSERT_CONTAINS(s,"0.00000000000000000000000001","omitted bare numeric preserved");ASSERT_CONTAINS(s,"\"f3\":10","no literal default rerun");ASSERT_TRUE(ac[0]&&strstr(s,ac),"omitted auto_create preserved");ASSERT_TRUE(!strstr(s,"\"f5\":1}"),"auto_update stamped over client input");free(s);}
    ASSERT_EQ_INT(u_json(&f,"[{\"key\":\"a\",\"value\":{\"f1\":null,\"f0\":\"1.25\"}},{\"key\":\"b\",\"value\":{\"f1\":\"a\\u0000b\"}}]",&out),0,"array exact NULL and binary string patches");free(out);
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"NULL physical row");if(s){ASSERT_CONTAINS(s,"\"f1\":null","explicit NULL");ASSERT_CONTAINS(s,"1.2500000000","exact typmod output");free(s);}
    s=u_get(&f,"b",&n);ASSERT_NOT_NULL(s,"binary physical row");if(s){ASSERT_CONTAINS(s,"a\\u0000b","decoded embedded NUL");free(s);}
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f0\":\"\"},\"b\":{\"f3\":22}}",&out),1,"mixed invalid row and valid peer");
    if(out){ASSERT_CONTAINS(out,"\"matched\":2","bad scalar remains matched");ASSERT_CONTAINS(out,"\"updated\":1","good peer commits");ASSERT_CONTAINS(out,"\"skipped\":1","bad row skipped");ASSERT_CONTAINS(out,"\"errors\":[","bad row distinguished from missing");ASSERT_CONTAINS(out,"\"key\":\"a\"","error identifies row");free(out);}
    u_reject(&f,"{\"a\":{\"f1\":\"duplicate patch\"},\"\\u0061\":{\"f3\":999}}");
    u_reject(&f,"{\"a\":{\"f3\":2147483648}}");
    u_reject(&f,"{\"a\":{\"f3\":{\"$incr\":1}}}");
    u_reject(&f,"[{\"key\":\"a\",\"value\":{}},{\"key\":\"\\u0061\",\"value\":null}]");
    u_reject(&f,"[{\"key\":\"a\",\"value\":{\"f3\":11},\"if\":[]},{\"key\":\"b\",\"value\":{\"f3\":23}}]");
    u_reject(&f,"{\"a\":{\"f1\":\"ok\"},\"b\":{\"f1\":\"\\uZZZZ\"}}");
    f.ts->fields[1].not_null=1;u_reject(&f,"{\"a\":{\"f1\":null}}");f.ts->fields[1].not_null=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&t2,&d2),0,"counts after updates");ASSERT_TRUE(total==t2&&deleted==d2,"updates never insert/delete");
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;ASSERT_EQ_INT(slotcask_open(&f.db,f.root,4,1,slot),0,"reopen updated rows");f.opened=1;
    s=u_get(&f,"b",&n);ASSERT_NOT_NULL(s,"reopened updated row");if(s){ASSERT_CONTAINS(s,"\"f3\":22","valid peer durable");free(s);}u_close(&f);
}
static void u_delimited(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f0\":1.5,\"f1\":\"OLD\",\"f2\":2.5,\"f3\":9}}"),0,"positional seed");
    const char *data="a,,NEW,,13,\r\nmissing,3\nkey-only\n";
    ASSERT_EQ_INT(u_call(&f,data,strlen(data),1,',',&out),0,"positional blanks preserve fields");
    if(out){ASSERT_CONTAINS(out,"\"matched\":2","key-only line ignored");ASSERT_CONTAINS(out,"\"updated\":1","one existing row");ASSERT_CONTAINS(out,"\"skipped\":1","one missing target");free(out);}
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"positional row");
    if(s){ASSERT_CONTAINS(s,"1.5000000000","blank numeric unchanged");ASSERT_CONTAINS(s,"\"f2\":2.5","omitted numeric unchanged");ASSERT_CONTAINS(s,"\"f1\":\"NEW\"","nonempty cell patched");ASSERT_CONTAINS(s,"\"f3\":13","integer patched");free(s);}
    const char binary[]={'a','|','|','x',0,'y','|','|','1','4','\n'};
    ASSERT_EQ_INT(u_call(&f,binary,sizeof(binary),1,'|',&out),0,"bounded binary positional span");free(out);
    s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"binary positional result");if(s){ASSERT_CONTAINS(s,"x\\u0000y","raw bytes preserved");free(s);}
    u_reject_input(&f,"a,,duplicate patch\na,,,,999\n",1,',');
    uint64_t pos=u_pos(&f);const char *bad="a,invalid\n";
    ASSERT_EQ_INT(u_call(&f,bad,strlen(bad),1,',',&out),1,"bad nonempty numeric rejected");free(out);ASSERT_TRUE(u_pos(&f)==pos,"bad positional row no append");
    u_close(&f);
}
static void u_files_guards(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f3\":9}}"),0,"file seed");
    char path[256];snprintf(path,sizeof(path),"%s/patch.json",f.root);FILE *fp=fopen(path,"wb");ASSERT_NOT_NULL(fp,"file input");
    if(fp){ASSERT_TRUE(fputs("{\"a\":{\"f1\":\"from file\"}}",fp)>=0,"file write");ASSERT_EQ_INT(fclose(fp),0,"file close");}
    ASSERT_EQ_INT(u_source(&f,path,1,&out),0,"file source uses same packed core");free(out);
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"file update physical");if(s){ASSERT_CONTAINS(s,"from file","file input applied");free(s);}
    ASSERT_EQ_INT(u_source(&f,"{\"a\":{\"f3\":10}}",0,&out),0,"inline source shares command");free(out);
    uint64_t pos=u_pos(&f);remove(path);ASSERT_EQ_INT(u_source(&f,path,1,&out),1,"missing file rejects");free(out);ASSERT_TRUE(u_pos(&f)==pos,"read failure no append");
    f.sc.auto_key=AK_UUID;u_reject(&f,"{\"a\":{}}");f.sc.auto_key=AK_NONE;
    snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata dir");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private index dir");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"manifest");if(fp){fputs("f0\n",fp);fclose(fp);}
    invalidate_idx_cache(f.root,"obj");u_reject(&f,"{\"a\":{}}");remove(path);invalidate_idx_cache(f.root,"obj");
    SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};const uint8_t bytes[]={1,0};ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"legacy",6,bytes,sizeof(bytes),&o,NULL),0,"legacy OLD seed");
    pos=u_pos(&f);ASSERT_EQ_INT(u_json(&f,"{\"legacy\":{}}",&out),1,"legacy OLD refuses packed patch");free(out);ASSERT_TRUE(u_pos(&f)==pos,"generation rejection no append");u_close(&f);
}
static void u_maximum(void){
    UFixture f;if(u_open(&f,1)){u_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_TRUE(f.sc.slot_size>1000000000,"max schema over one GiB");
    ASSERT_EQ_INT(u_seed(&f,"{\"a\":{\"f127\":\"OLD\",\"f128\":\"tiny\"}}"),0,"tiny max-schema seed");
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f128\":\"changed\"}}",&out),0,"tiny max-schema partial update");free(out);
    char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"tiny max-schema result");if(s){ASSERT_CONTAINS(s,"OLD","omitted max-schema field kept");ASSERT_CONTAINS(s,"changed","max-schema changed field");free(s);}ASSERT_TRUE(n<4096,"actual size independent of schema bound");u_close(&f);
    if(u_open(&f,0)){u_close(&f);return;}ASSERT_EQ_INT(u_seed(&f,"{\"a\":{}}"),0,"large patch seed");
    size_t len=4u*1024*1024;char *json=malloc(len+32);ASSERT_NOT_NULL(json,"4 MiB patch");
    if(json){memcpy(json,"{\"a\":{\"f1\":\"",12);memset(json+12,'x',len);memcpy(json+12+len,"\"}}",4);
        ASSERT_EQ_INT(u_json(&f,json,&out),0,"grow to full varchar limit");free(out);
        s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"large result");ASSERT_TRUE(n>len&&n<len+256,"large actual record");free(s);free(json);}
    ASSERT_EQ_INT(u_json(&f,"{\"a\":{\"f1\":\"\"}}",&out),0,"shrink with explicit empty JSON");free(out);s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"shrunk result");if(s){ASSERT_CONTAINS(s,"\"f1\":\"\"","empty not NULL");free(s);}ASSERT_TRUE(n<256,"shrunk actual size");u_close(&f);
}
typedef struct {pthread_mutex_t lock;pthread_cond_t cond;int ready,go;} UBarrier;
typedef struct {UFixture *f;UBarrier *barrier;ShardDb *owner;const char *json;int rc;char *out;} UThread;
static void *u_thread(void *arg){
    UThread *t=arg;g_db=t->owner;pthread_mutex_lock(&t->barrier->lock);t->barrier->ready++;
    pthread_cond_broadcast(&t->barrier->cond);while(!t->barrier->go)pthread_cond_wait(&t->barrier->cond,&t->barrier->lock);
    pthread_mutex_unlock(&t->barrier->lock);t->rc=u_json(t->f,t->json,&t->out);g_db=NULL;return NULL;
}
static void u_concurrent(void){
    UFixture f;if(u_open(&f,0)){u_close(&f);return;}ASSERT_EQ_INT(u_seed(&f,"{\"a\":{}}"),0,"concurrent seed");
    UBarrier b={PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0,0};
    UThread a={&f,&b,g_db,"{\"a\":{\"f1\":\"writer\"}}",-1,NULL},c={&f,&b,g_db,"{\"a\":{\"f3\":123}}",-1,NULL};pthread_t ta,tc;
    if(pthread_create(&ta,NULL,u_thread,&a)||pthread_create(&tc,NULL,u_thread,&c)){fprintf(stderr,"cannot start update threads\n");abort();}
    pthread_mutex_lock(&b.lock);while(b.ready<2)pthread_cond_wait(&b.cond,&b.lock);b.go=1;pthread_cond_broadcast(&b.cond);pthread_mutex_unlock(&b.lock);
    pthread_join(ta,NULL);pthread_join(tc,NULL);pthread_cond_destroy(&b.cond);pthread_mutex_destroy(&b.lock);
    ASSERT_EQ_INT(a.rc,0,"first bulk patch commits");ASSERT_EQ_INT(c.rc,0,"second bulk patch commits");free(a.out);free(c.out);
    size_t n=0;char *s=u_get(&f,"a",&n);ASSERT_NOT_NULL(s,"concurrent result");if(s){ASSERT_CONTAINS(s,"writer","first disjoint patch kept");ASSERT_CONTAINS(s,"\"f3\":123","second disjoint patch kept");free(s);}u_close(&f);
}
static int test_typed_bulk_update_run(void){u_routes();u_delimited();u_files_guards();u_maximum();u_concurrent();return t_ctx->failed?1:0;}
TEST_REGISTER("test-typed-bulk-update",test_typed_bulk_update_run)
```

### build-register-old

<!-- block:build-register-old -->
```c
    src/test/cases/test_typed_bulk_insert.c \
```

### build-register-new

<!-- block:build-register-new -->
```c
    src/test/cases/test_typed_bulk_insert.c \
    src/test/cases/test_typed_bulk_update.c \
```

### bulk-implementation-old

<!-- block:bulk-implementation-old -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    const char *line;
    size_t line_len;
    int shard,error;
    uint8_t *owned;
    const TypedSchema *ts;
    const Schema *sc;
    const char *root,*object;
} BulkRow;
typedef struct {BulkRow *rows;size_t count,cap,errors;} BulkRows;
static const char *bulk_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int bulk_fail(int code){errno=code;return -1;}
static char *bulk_copy(const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    char *s=malloc(n+1);if(!s)return NULL;
    memcpy(s,p,n);s[n]=0;return s;
}
static void bulk_free(BulkRows *b){
    for(size_t i=0;i<b->count;i++){free(b->rows[i].key);free(b->rows[i].json);free(b->rows[i].owned);}
    free(b->rows);memset(b,0,sizeof(*b));
}
/* Ownership of key/json transfers only on success. Row pointers are not exposed
 * to the engine until collection and sorting are complete. */
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(!key||!kn||kn>(size_t)sc->max_key||(!json&&!line)){
        b->errors++;free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16];compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
static int bulk_key(const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    return json_unescape_string(p+1,(size_t)(end-p-2),key,len);
}
static int bulk_expired(const QueryDeadline *d){
    return d->timeout_ms&&now_ms_coarse()-d->t0_ms>(uint64_t)d->timeout_ms;
}
/* Whole-document validation happens before this collector; skip_value never
 * sees malformed input. Non-object values are row errors, not partial JSON. */
static int bulk_json(BulkRows *b,const char *text,const Schema *sc,QueryDeadline *dl){
    if(typed_json_validate(text))return -1;
    const char *p=bulk_space(text);int dict=*p=='{';
    if(!dict&&*p!='[')return bulk_fail(EINVAL);
    char close=dict?'}':']';p=bulk_space(p+1);
    while(*p!=close){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        char *key=NULL,*value=NULL;size_t kn=0;
        const char *v=NULL,*ve=NULL,*end=NULL;
        if(dict){
            end=json_skip_value(p);
            if(bulk_key(p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(v&&*v=='{'){
            value=bulk_copy(v,(size_t)(ve-v));
            if(!value){free(key);return -1;}
        }
        if(bulk_add(b,key,kn,value,NULL,0,sc)){free(key);free(value);return -1;}
        p=bulk_space(p);if(*p==',')p=bulk_space(p+1);
    }
    return 0;
}
static int bulk_delimited(BulkRows *b,const char *data,size_t len,
    const Schema *sc,char delimiter,QueryDeadline *dl){
    if(!len||!delimiter||delimiter=='\n'||delimiter=='\r')return bulk_fail(EINVAL);
    size_t at=0;
    while(at<len){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        size_t start=at;while(at<len&&data[at]!='\n'&&data[at]!='\r')at++;
        size_t end=at;
        if(at<len&&data[at]=='\r')at++;
        if(at<len&&data[at]=='\n')at++;
        if(end==start)continue;
        size_t key_end=start;while(key_end<end&&data[key_end]!=delimiter)key_end++;
        if(key_end==end)continue; /* A key alone is not a positional record. */
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        const char *line=data+(key_end<end?key_end+1:end);
        size_t ln=(size_t)((data+end)-line);
        if(bulk_add(b,key,key_end-start,NULL,line,ln,sc)){free(key);return -1;}
    }
    return 0;
}
static int bulk_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;
    if(x->shard!=y->shard)return x->shard<y->shard?-1:1;
    return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
/* Immutable delimiter and row pointer remain valid through synchronous join. */
typedef struct {BulkRow *row;char delimiter;} BulkCell;
static int bulk_value(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    BulkCell *cell=rec->user_ctx;BulkRow *r=cell->row;
    free(r->owned);r->owned=NULL;r->error=0;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;return -1;
    }
    size_t len=0;int rc;
    if(r->json)rc=typed_insert_alloc(r->ts,old,r->json,r->root,r->object,&r->owned,&len,NULL,0);
    else{
        TypedInput inputs[MAX_FIELDS]={0};size_t at=0;
        for(int i=0;i<r->ts->nfields;i++){
            if(r->ts->fields[i].removed)continue;
            size_t begin=at;while(at<r->line_len&&r->line[at]!=cell->delimiter)at++;
            size_t n=at-begin;if(at<r->line_len)at++;
            enum FieldType t=r->ts->fields[i].type;
            /* Legacy positional empty fixed scalars remain present zero.
             * Empty numeric is invalid; varchar remains present empty. */
            inputs[i]=(TypedInput){r->line+begin,n,(uint8_t)(n||t==FT_NUMERIC||t==FT_VARCHAR),0,0};
        }
        rc=typed_insert_inputs_alloc(r->ts,old,inputs,&r->owned,&len,NULL,0);
    }
    size_t physical=0;
    if(rc||len>(size_t)r->sc->max_value||
        pkr_record_fits(&r->ts->packed,len,r->klen,slotcask_seg_max_bytes(),&physical)!=DEC_OK||
        physical>(size_t)r->sc->slot_size){
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;return -1;
    }
    rec->value=r->owned;rec->vlen=len;rec->hdr2=PKR_FLAGS;return 0;
}
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    if(!root||!object||!sc||!ts||!data||format<0||format>1||!ts->packed.nfields||
        ts->nfields<1||ts->nfields>MAX_FIELDS||ts->packed.nfields!=(uint32_t)ts->nfields||
        sc->splits<1||sc->splits>MAX_SPLITS||sc->streams<1||sc->slot_size<32||sc->max_key<1||
        sc->max_value<0||ts->total_size>sc->max_value||
        (borrowed&&(borrowed->num_shards!=sc->splits||borrowed->slot_size!=sc->slot_size))){
        errno=EINVAL;OUT("{\"error\":\"invalid packed bulk input\"}\n");return 1;
    }
    char indexes[MAX_FIELDS][256];
    if(sc->auto_key!=AK_NONE||load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk auto-key and indexes await integration\"}\n");return 1;
    }
    BulkRows b={0};char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy(data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(b.count>SIZE_MAX/sizeof(SlotcaskBulkRec)||b.count>SIZE_MAX/sizeof(BulkCell)){
        errno=EOVERFLOW;goto fail;
    }
    if(!b.count){
        size_t errors=b.errors;bulk_free(&b);free(json);
        if(errors){OUT("{\"inserted\":0,\"skipped\":0,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",errors);return 1;}
        OUT("{\"inserted\":0}\n");return 0;
    }
    qsort(b.rows,b.count,sizeof(*b.rows),bulk_order);
    size_t groups=1;
    for(size_t i=1;i<b.count;i++)if(b.rows[i].shard!=b.rows[i-1].shard)groups++;
    SlotcaskBulkRec *recs=calloc(b.count,sizeof(*recs));
    BulkCell *cells=calloc(b.count,sizeof(*cells));
    SlotcaskBulkShardInput *inputs=calloc(groups,sizeof(*inputs));
    if(!recs||!cells||!inputs){free(recs);free(cells);free(inputs);errno=ENOMEM;goto fail;}
    for(size_t i=0,g=0;i<b.count;i++){
        BulkRow *r=&b.rows[i];r->ts=ts;r->sc=sc;r->root=root;r->object=object;
        cells[i]=(BulkCell){r,delimiter};
        recs[i]=(SlotcaskBulkRec){.key=r->key,.klen=r->klen,.hdr2=PKR_FLAGS,.user_ctx=&cells[i],.status=-1};
        if(!i||r->shard!=b.rows[i-1].shard){
            if(i)g++;
            inputs[g]=(SlotcaskBulkShardInput){.kf_shard_id=r->shard,.recs=recs+i,
                .kind=SLOTCASK_BULK_INPUT_UPSERT,.rc=-1,
                .opts.upsert={.if_not_exists=if_not_exists,.value_compute=bulk_value,.value_rewrites_payload=1}};
        }
        inputs[g].nrecs++;
    }
    SlotcaskSchemaInfo info={.splits=sc->splits,.slot_size=sc->slot_size,.streams=sc->streams};
    SlotcaskDb *owned SDB_REG_REF=borrowed?NULL:slotcask_registry_get(root,object,&info);
    SlotcaskDb *db=borrowed?borrowed:owned;
    if(!db){free(inputs);free(cells);free(recs);goto fail;}
    rc=slotcask_bulk_request_execute(db,inputs,groups);
    size_t inserted=0,skipped=0,errors=b.errors;int pending=0;
    for(size_t g=0;g<groups;g++){
        if(inputs[g].rc==-2)pending=1;
        for(size_t i=0;i<inputs[g].nrecs;i++){
            SlotcaskBulkRec *r=&inputs[g].recs[i];BulkCell *c=r->user_ctx;
            if(c->row->error)errors++;
            else if(r->status==-2)skipped++;
            else if(inputs[g].rc||r->status)errors++;
            else if(!r->was_update)inserted++;
        }
    }
    /* A coordinator-level failure without a per-shard status is never success. */
    if(rc&&!pending&&!errors)errors++;
    free(inputs);free(cells);free(recs);bulk_free(&b);free(json);
    if(pending){OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    if(errors){OUT("{\"inserted\":%zu,\"skipped\":%zu,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",inserted,skipped,errors);return 1;}
    if(skipped)OUT("{\"inserted\":%zu,\"skipped\":%zu}\n",inserted,skipped);
    else OUT("{\"inserted\":%zu}\n",inserted);
    return 0;
fail:{
    int saved=errno?errno:EINVAL;bulk_free(&b);free(json);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":saved==ETIMEDOUT?"query_timeout":"invalid packed bulk input");
    return 1;
}}

int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)data;
    (void)len;(void)format;(void)delimiter;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk update unavailable\"}\n");return 1;
}
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)input;(void)is_file;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk update unavailable\"}\n");return 1;
}
```

### bulk-implementation-new

<!-- block:bulk-implementation-new -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    const char *line;
    size_t line_len;
    int shard,error;
    uint8_t *owned;
    const TypedSchema *ts;
    const Schema *sc;
    const char *root,*object;
} BulkRow;
typedef struct {char *key;size_t len;} BulkKey;
typedef struct {
    BulkRow *rows;size_t count,cap,errors,skipped;
    BulkKey *keys;size_t nkeys,key_cap;
    int update;
} BulkRows;
static const char *bulk_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int bulk_fail(int code){errno=code;return -1;}
static char *bulk_copy(const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    char *s=malloc(n+1);if(!s)return NULL;
    memcpy(s,p,n);s[n]=0;return s;
}
static void bulk_free(BulkRows *b){
    for(size_t i=0;i<b->count;i++){free(b->rows[i].key);free(b->rows[i].json);free(b->rows[i].owned);}
    for(size_t i=0;i<b->nkeys;i++)free(b->keys[i].key);
    free(b->keys);free(b->rows);memset(b,0,sizeof(*b));
}
/* Ownership of key/json transfers only on success. Row pointers are not exposed
 * to the engine until collection and sorting are complete. */
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(!key||!kn||kn>(size_t)sc->max_key||(!json&&!line)){
        if(b->update)b->skipped++;else b->errors++;
        free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16];compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
/* Track every update storage key before patch/key-size validation. Partial
 * patches of one key must not compose independently against the same OLD. */
static int bulk_track_key(BulkRows *b,const char *key,size_t len){
    if(!key)return 0;
    if(b->nkeys==b->key_cap){
        size_t cap=b->key_cap?b->key_cap*2:32;
        if(cap<b->key_cap||cap>SIZE_MAX/sizeof(*b->keys))return bulk_fail(EOVERFLOW);
        BulkKey *p=realloc(b->keys,cap*sizeof(*p));if(!p)return -1;
        b->keys=p;b->key_cap=cap;
    }
    char *copy=bulk_copy(key,len);if(!copy)return -1;
    b->keys[b->nkeys++]=(BulkKey){copy,len};return 0;
}
static int bulk_key_order(const void *a,const void *b){
    const BulkKey *x=a,*y=b;size_t n=x->len<y->len?x->len:y->len;
    int c=memcmp(x->key,y->key,n);return c?c:x->len<y->len?-1:x->len>y->len;
}
static int bulk_check_duplicates(BulkRows *b){
    if(b->nkeys>1){
        qsort(b->keys,b->nkeys,sizeof(*b->keys),bulk_key_order);
        for(size_t i=1;i<b->nkeys;i++)
            if(!bulk_key_order(&b->keys[i-1],&b->keys[i]))return bulk_fail(EEXIST);
    }
    return 0;
}
static int bulk_key(const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    return json_unescape_string(p+1,(size_t)(end-p-2),key,len);
}
static int bulk_expired(const QueryDeadline *d){
    return d->timeout_ms&&now_ms_coarse()-d->t0_ms>(uint64_t)d->timeout_ms;
}
/* Whole-document validation happens before this collector; skip_value never
 * sees malformed input. Non-object values are row errors, not partial JSON. */
static int bulk_json(BulkRows *b,const char *text,const Schema *sc,QueryDeadline *dl){
    if(typed_json_validate(text))return -1;
    const char *p=bulk_space(text);int dict=*p=='{';
    if(!dict&&*p!='[')return bulk_fail(EINVAL);
    char close=dict?'}':']';p=bulk_space(p+1);
    while(*p!=close){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        char *key=NULL,*value=NULL;size_t kn=0;
        const char *v=NULL,*ve=NULL,*end=NULL;
        if(dict){
            end=json_skip_value(p);
            if(bulk_key(p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    else if(b->update&&nn==2&&!memcmp(name,"if",2)){
                        free(name);free(key);return bulk_fail(ENOTSUP);
                    }
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(b->update&&bulk_track_key(b,key,kn)){free(key);return -1;}
        if(v&&*v=='{'){
            value=bulk_copy(v,(size_t)(ve-v));
            if(!value){free(key);return -1;}
        }
        if(bulk_add(b,key,kn,value,NULL,0,sc)){free(key);free(value);return -1;}
        p=bulk_space(p);if(*p==',')p=bulk_space(p+1);
    }
    return 0;
}
static int bulk_delimited(BulkRows *b,const char *data,size_t len,
    const Schema *sc,char delimiter,QueryDeadline *dl){
    if(!len||!delimiter||delimiter=='\n'||delimiter=='\r')return bulk_fail(EINVAL);
    size_t at=0;
    while(at<len){
        if(bulk_expired(dl))return bulk_fail(ETIMEDOUT);
        size_t start=at;while(at<len&&data[at]!='\n'&&data[at]!='\r')at++;
        size_t end=at;
        if(at<len&&data[at]=='\r')at++;
        if(at<len&&data[at]=='\n')at++;
        if(end==start)continue;
        size_t key_end=start;while(key_end<end&&data[key_end]!=delimiter)key_end++;
        if(key_end==end)continue; /* A key alone is not a positional record. */
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        if(b->update&&bulk_track_key(b,key,key_end-start)){free(key);return -1;}
        const char *line=data+(key_end<end?key_end+1:end);
        size_t ln=(size_t)((data+end)-line);
        if(bulk_add(b,key,key_end-start,NULL,line,ln,sc)){free(key);return -1;}
    }
    return 0;
}
static int bulk_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;
    if(x->shard!=y->shard)return x->shard<y->shard?-1:1;
    return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
/* Immutable delimiter and row pointer remain valid through synchronous join. */
typedef struct {BulkRow *row;char delimiter;int update;} BulkCell;
static int bulk_value(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    BulkCell *cell=rec->user_ctx;BulkRow *r=cell->row;
    free(r->owned);r->owned=NULL;r->error=0;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;return -1;
    }
    size_t len=0;int rc;
    if(cell->update&&!old){r->error=EILSEQ;return -1;}
    if(r->json){
        if(cell->update)rc=typed_update_json_alloc(r->ts,old,r->json,&r->owned,&len,NULL,0);
        else rc=typed_insert_alloc(r->ts,old,r->json,r->root,r->object,&r->owned,&len,NULL,0);
    }
    else{
        TypedInput inputs[MAX_FIELDS]={0};size_t at=0;
        for(int i=0;i<r->ts->nfields;i++){
            if(r->ts->fields[i].removed)continue;
            size_t begin=at;while(at<r->line_len&&r->line[at]!=cell->delimiter)at++;
            size_t n=at-begin;if(at<r->line_len)at++;
            enum FieldType t=r->ts->fields[i].type;
            /* Legacy positional empty fixed scalars remain present zero.
             * Empty numeric is invalid; varchar remains present empty. */
            inputs[i]=(TypedInput){r->line+begin,n,
                (uint8_t)(cell->update?n!=0:n||t==FT_NUMERIC||t==FT_VARCHAR),0,0};
        }
        if(cell->update)rc=typed_update_alloc(r->ts,old,inputs,1,&r->owned,&len,NULL,0);
        else rc=typed_insert_inputs_alloc(r->ts,old,inputs,&r->owned,&len,NULL,0);
    }
    size_t physical=0;
    if(rc||len>(size_t)r->sc->max_value||
        pkr_record_fits(&r->ts->packed,len,r->klen,slotcask_seg_max_bytes(),&physical)!=DEC_OK||
        physical>(size_t)r->sc->slot_size){
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;return -1;
    }
    rec->value=r->owned;rec->vlen=len;rec->hdr2=PKR_FLAGS;return 0;
}
/* Emit keys as length-delimited JSON without a post-write allocation whose
 * failure could erase the error's identity. Values are never rendered here. */
static void bulk_quote(const char *s,size_t n){
    OUT("\"");
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='"'||c=='\\')OUT("\\%c",c);
        else if(c<32)OUT("\\u%04x",(unsigned)c);
        else OUT("%c",c);
    }
    OUT("\"");
}
static void bulk_update_response(const BulkRows *b,size_t updated,size_t skipped,int global_error){
    OUT("{\"matched\":%zu,\"updated\":%zu,\"skipped\":%zu",b->count,updated,skipped);
    int first=1;
    for(size_t i=0;i<b->count;i++)if(b->rows[i].error){
        const BulkRow *r=&b->rows[i];
        OUT("%s",first?",\"errors\":[":",");first=0;
        OUT("{\"key\":");bulk_quote(r->key,r->klen);
        const char *why=r->error==ENOMEM?"out of memory":r->error==EILSEQ?
            "invalid OLD record":r->error==E2BIG?"record exceeds storage bound":
            r->error==EIO?"bulk update failed":"invalid packed update";
        OUT(",\"error\":\"%s\"}",why);
    }
    if(!first)OUT("]");
    if(global_error)OUT(",\"error\":\"bulk update failed; outcome may be partial\"");
    OUT("}\n");
}
static int typed_bulk_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists,int update){
    if(!root||!object||!sc||!ts||!data||format<0||format>1||!ts->packed.nfields||
        ts->nfields<1||ts->nfields>MAX_FIELDS||ts->packed.nfields!=(uint32_t)ts->nfields||
        sc->splits<1||sc->splits>MAX_SPLITS||sc->streams<1||sc->slot_size<32||sc->max_key<1||
        sc->max_value<0||ts->total_size>sc->max_value||
        (borrowed&&(borrowed->num_shards!=sc->splits||borrowed->slot_size!=sc->slot_size))){
        errno=EINVAL;OUT("{\"error\":\"invalid packed bulk input\"}\n");return 1;
    }
    char indexes[MAX_FIELDS][256];
    if(sc->auto_key!=AK_NONE||load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk auto-key and indexes await integration\"}\n");return 1;
    }
    BulkRows b={.update=update};char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy(data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(update&&bulk_check_duplicates(&b))goto fail;
    if(b.count>SIZE_MAX/sizeof(SlotcaskBulkRec)||b.count>SIZE_MAX/sizeof(BulkCell)){
        errno=EOVERFLOW;goto fail;
    }
    if(!b.count){
        if(update){bulk_update_response(&b,0,b.skipped,0);bulk_free(&b);free(json);return 0;}
        size_t errors=b.errors;bulk_free(&b);free(json);
        if(errors){OUT("{\"inserted\":0,\"skipped\":0,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",errors);return 1;}
        OUT("{\"inserted\":0}\n");return 0;
    }
    qsort(b.rows,b.count,sizeof(*b.rows),bulk_order);
    size_t groups=1;
    for(size_t i=1;i<b.count;i++)if(b.rows[i].shard!=b.rows[i-1].shard)groups++;
    SlotcaskBulkRec *recs=calloc(b.count,sizeof(*recs));
    BulkCell *cells=calloc(b.count,sizeof(*cells));
    SlotcaskBulkShardInput *inputs=calloc(groups,sizeof(*inputs));
    if(!recs||!cells||!inputs){free(recs);free(cells);free(inputs);errno=ENOMEM;goto fail;}
    for(size_t i=0,g=0;i<b.count;i++){
        BulkRow *r=&b.rows[i];r->ts=ts;r->sc=sc;r->root=root;r->object=object;
        cells[i]=(BulkCell){r,delimiter,update};
        recs[i]=(SlotcaskBulkRec){.key=r->key,.klen=r->klen,.hdr2=PKR_FLAGS,.user_ctx=&cells[i],.status=-1};
        if(!i||r->shard!=b.rows[i-1].shard){
            if(i)g++;
            inputs[g]=(SlotcaskBulkShardInput){.kf_shard_id=r->shard,.recs=recs+i,
                .kind=SLOTCASK_BULK_INPUT_UPSERT,.rc=-1,
                .opts.upsert={.if_not_exists=if_not_exists,.require_existing=update,
                    .value_compute=bulk_value,.value_rewrites_payload=1}};
        }
        inputs[g].nrecs++;
    }
    SlotcaskSchemaInfo info={.splits=sc->splits,.slot_size=sc->slot_size,.streams=sc->streams};
    SlotcaskDb *owned SDB_REG_REF=borrowed?NULL:slotcask_registry_get(root,object,&info);
    SlotcaskDb *db=borrowed?borrowed:owned;
    if(!db){free(inputs);free(cells);free(recs);goto fail;}
    rc=slotcask_bulk_request_execute(db,inputs,groups);
    size_t inserted=0,skipped=b.skipped,errors=b.errors,updated=0;int pending=0;
    for(size_t g=0;g<groups;g++){
        if(inputs[g].rc==-2)pending=1;
        for(size_t i=0;i<inputs[g].nrecs;i++){
            SlotcaskBulkRec *r=&inputs[g].recs[i];BulkCell *c=r->user_ctx;
            if(c->row->error){errors++;if(update)skipped++;}
            else if(r->status==-2)skipped++;
            else if(r->status||(!update&&inputs[g].rc)){
                errors++;if(update){skipped++;c->row->error=EIO;}
            }else if(update)updated++;
            else if(!r->was_update)inserted++;
        }
    }
    /* A hard shard failure may coexist with completed later windows. Count
     * update rows by terminal status, and report the request failure separately.
     * Pending durability still overrides all counts below. */
    int global_error=update?rc&&!pending:rc&&!pending&&!errors;
    if(global_error)errors++;
    free(inputs);free(cells);free(recs);
    if(update){
        if(pending)OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        else bulk_update_response(&b,updated,skipped,global_error);
        bulk_free(&b);free(json);return pending||errors?1:0;
    }
    bulk_free(&b);free(json);
    if(pending){OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    if(errors){OUT("{\"inserted\":%zu,\"skipped\":%zu,\"errors\":%zu,\"error\":\"some_records_dropped\"}\n",inserted,skipped,errors);return 1;}
    if(skipped)OUT("{\"inserted\":%zu,\"skipped\":%zu}\n",inserted,skipped);
    else OUT("{\"inserted\":%zu}\n",inserted);
    return 0;
fail:{
    int saved=errno?errno:EINVAL;bulk_free(&b);free(json);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":saved==ETIMEDOUT?"query_timeout":
        saved==ENOTSUP?"packed bulk conditions await integration":
        saved==EEXIST?"duplicate key in records":"invalid packed bulk input");
    return 1;
}}

int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    return typed_bulk_command(root,object,sc,ts,borrowed,data,len,format,delimiter,if_not_exists,0);
}
int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter){
    return typed_bulk_command(root,object,sc,ts,borrowed,data,len,format,delimiter,0,1);
}
/* File and inline JSON meet here before any legacy parser/default/index access.
 * A short read is a request failure; the full buffer lives through execute. */
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file){
    if(!input){errno=EINVAL;OUT("{\"error\":\"invalid packed update source\"}\n");return 1;}
    if(!is_file)return typed_bulk_update_command(root,object,sc,ts,borrowed,input,strlen(input),0,0);
    int fd=open(input,O_RDONLY);char *data=NULL;size_t len=0;int saved=0;
    if(fd<0)goto failed;
    struct stat st;
    if(fstat(fd,&st)||st.st_size<=0||(uintmax_t)st.st_size>SIZE_MAX-1){
        saved=EINVAL;goto failed;
    }
    len=(size_t)st.st_size;data=malloc(len+1);if(!data){saved=ENOMEM;goto failed;}
    size_t at=0;
    while(at<len){
        size_t want=len-at;if(want>(size_t)SSIZE_MAX)want=(size_t)SSIZE_MAX;
        ssize_t n=read(fd,data+at,want);
        if(n<0&&errno==EINTR)continue;
        if(n<=0){saved=n<0?errno:EIO;goto failed;}at+=(size_t)n;
    }
    if(close(fd)){fd=-1;goto failed;}fd=-1;data[len]=0;
    int rc=typed_bulk_update_command(root,object,sc,ts,borrowed,data,len,0,0);
    saved=errno;free(data);errno=saved;return rc;
failed:
    if(!saved)saved=errno?errno:EIO;
    if(fd>=0)close(fd);
    free(data);errno=saved;
    OUT("{\"error\":\"%s\"}\n",saved==ENOMEM?"out of memory":"cannot read packed update source");return 1;
}
```

### query-edit-1-old

<!-- block:query-edit-1-old -->
```c
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    FieldSchema cas_fs;
```

### query-edit-1-new

<!-- block:query-edit-1-new -->
```c
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    if(ts->packed.nfields){
        Schema packed_sc=load_schema(db_root,object);
        return typed_bulk_update_source(db_root,object,&packed_sc,ts,NULL,input,input_is_file);
    }
    FieldSchema cas_fs;
```

### query-edit-2-old

<!-- block:query-edit-2-old -->
```c
        OUT("{\"error\":\"Delimited update requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    if (!data || size == 0) {
```

### query-edit-2-new

<!-- block:query-edit-2-new -->
```c
        OUT("{\"error\":\"Delimited update requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    if(ts->packed.nfields){
        Schema packed_sc=load_schema(db_root,object);
        return typed_bulk_update_command(db_root,object,&packed_sc,ts,NULL,data,size,1,delimiter);
    }
    if (!data || size == 0) {
```

### query-edit-3-old

<!-- block:query-edit-3-old -->
```c
int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
```

### query-edit-3-new

<!-- block:query-edit-3-new -->
```c
int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    TypedSchema *guard_ts=load_typed_schema(db_root,object);
    if(guard_ts&&guard_ts->packed.nfields){
        OUT("{\"error\":\"packed criteria updates await query integration\"}\n");return 1;
    }
```

### query-edit-4-old

<!-- block:query-edit-4-old -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_finish_request(&workers[g]);
    }
```

### query-edit-4-new

<!-- block:query-edit-4-new -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        if (inputs) for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_finish_request(&workers[g]);
    }
```

### query-edit-5-old

<!-- block:query-edit-5-old -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_delim_finish_request(&workers[g]);
    }
```

### query-edit-5-new

<!-- block:query-edit-5-new -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        if (inputs) for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_delim_finish_request(&workers[g]);
    }
```

### query-edit-6-old

<!-- block:query-edit-6-old -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_json_finish_request(&workers[g]);
    }
```

### query-edit-6-new

<!-- block:query-edit-6-new -->
```c
    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        if (inputs) for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_json_finish_request(&workers[g]);
    }
```

### query-edit-7-old

<!-- block:query-edit-7-old -->
```c
                char *cellbuf = (cell_len < sizeof(tmpcell))
                    ? tmpcell : malloc(cell_len + 1);
                memcpy(cellbuf, cell_start, cell_len);
```

### query-edit-7-new

<!-- block:query-edit-7-new -->
```c
                char *cellbuf = (cell_len < sizeof(tmpcell))
                    ? tmpcell : malloc(cell_len + 1);
                if (!cellbuf) return -1; /* Keep OLD intact on allocation failure. */
                memcpy(cellbuf, cell_start, cell_len);
```

### update-doc

<!-- block:update-doc -->
```markdown
# Packed keyed bulk updates (inactive schema format)

The internal `typed_bulk_update_command` composes partial replacements for
explicitly bound packed schemas through the same synchronous deferred bulk
coordinator as insertion. `typed_bulk_update_source` owns bounded file bytes or
borrows inline JSON through return. Public loaders/creates still do not bind
packed schemas. This document records private integration, not activation.

JSON dictionaries and arrays patch only supplied known fields. Omitted values
and omitted AUTO_CREATE fields carry from locked OLD; JSON empty varchar and
explicit NULL remain real values. Current scalar-update policy still permits
an explicitly supplied AUTO_CREATE patch; this slice does not silently make
that field immutable. AUTO_UPDATE is stamped after the patch, overriding client
input. Defaults, sequences and insert-time generation are not rerun.

Delimited updates differ from insertion: every blank or absent cell is
unsupplied, including numeric/varchar cells, so OLD remains unchanged. There
is no positional spelling for explicit NULL or empty varchar; use JSON for
those. Active nonremoved fields consume columns in schema order; surplus
columns are ignored. No-separator lines are skipped, including EOF/LF/CRLF;
a separator followed by an empty cell is a real no-field patch and still
stamps AUTO_UPDATE on an existing target. Input is length-bounded and may carry
embedded NUL in varchar content; this adds no CSV quoting/escape grammar.

Valid key/object row shapes count toward `matched`; malformed/missing/overlong
key or missing/non-object value rows count `skipped` without being matched.
Missing stored targets count `skipped` and never insert. Construction failures
also count skipped but produce an optional per-key `errors` array and a nonzero
command return, distinguishing them from condition misses. Existing successful
responses keep `{matched,updated,skipped}`. Encoding/NOT NULL/size/generation/OOM
failure is row-atomic; valid peers may commit. This is not a batch transaction.
A hard shard failure does not erase successful later-window outcomes: update
counts use terminal per-row statuses. Any hard coordinator failure also emits
a top-level partial-outcome warning and returns nonzero, even when keyed row
errors are already present. Pending durability overrides all counts with an
explicit do-not-retry error.

All update forms screen storage-key duplicates before any writes, including
escaped dictionary aliases and repeated delimited keys. JSON keys are decoded
before screening, even when the referenced row has an invalid patch or key
length. Repeated partial patches reject the entire request: composing each
against the same OLD could silently discard a previous patch. Insert duplicate
policy is unchanged.

Packed array `if` conditions reject the entire request before writes, including
an explicitly present empty or null condition. This is a temporary fail-closed
boundary, not implementation of legacy per-record CAS. Packed criteria updates,
including criteria dry runs, reject before any criteria compilation/scan.
Packed auto-key and secondary-index objects also reject before construction.
Existing public legacy CAS, criteria, index and dispatch policies stay active
and unchanged; their complete packed support remains later query/write work.

The adapter owns each NEW allocation until the complete synchronous request
returns, with one input per touched shard and exactly one coordinator call.
`require_existing=1` filters missing keys before the constructor;
`value_rewrites_payload=1` prevents pre-staging placeholder bytes. The callback
validates OLD generation, uses the actual-size row helpers, and checks actual
key/value bytes against slot/schema/unchanged segment limits. It never supplies
caller-OLD overrides, changes index/marker/recovery hooks or uses the single
owning callback. Recovery continues to use durable segment evidence.

Row error JSON is emitted from length-delimited keys without allocating an
escape buffer after the write. All row/input/key/NEW allocations are released
on success, hard failure or pending durability; pending results remain explicit
errors and must not be retried blindly. Raw input and schema stay immutable
and alive through every joined pipeline. File short reads fail before dispatch.

Allocation follows actual source/payload sizes; no schema-maximum arena or slab
is introduced. A maximum 256-varchar schema remains usable for small rows.
Bulk command-wide expansion budgeting remains future activation work; this
slice does not claim to impose QUERY_BUFFER_MB on previously uncapped bulk paths.

Three legacy update request blocks now guard their input-copy loops after a
failed calloc, instead of dereferencing NULL before the existing error branch.
The legacy delimiter callback also checks its heap cell copy before memcpy.
These narrow crash fixes retain existing legacy skip/response policy; broader
legacy hard-error propagation and parser allocation auditing remain Phase 4
obligations. Packed constructors already carry per-row hard-error state.

Private physical tests cover exact decimals, NULL/empty/omission, independent
shards, file input, missing targets, forbidden conditions, duplicate request
rejection, OLD/count/segment preservation, 4 MiB growth/shrinkage, a >1 GiB
schema's small actual row, reopen and concurrent disjoint patches. Public packed
route acceptance, criteria/CAS, indexes, bulk-delete, remaining readers and
migration are not discharged by those fixtures. No metadata activation occurs.
```

### insert-doc-link-old

<!-- block:insert-doc-link-old -->
```markdown
packed commands, bulk-update/delete, readers, schema rewrites and migration
are not discharged by these fixtures. Existing public legacy tests remain
mandatory compatibility coverage.
```

### insert-doc-link-new

<!-- block:insert-doc-link-new -->
```markdown
packed commands, criteria/CAS updates, bulk-delete, readers, schema rewrites
and migration are not discharged by these fixtures. Existing public legacy
tests remain mandatory compatibility coverage.

[Keyed partial bulk updates](packed-bulk-update.md) reuse this collection and
deferred-request lifetime while requiring OLD. Their blank positional cells
preserve OLD rather than writing insertion's empty/zero values. Packed
conditions/criteria remain guarded until query integration is ready.
```

### row-doc-link-old

<!-- block:row-doc-link-old -->
```markdown
Bulk-update/delete and read/index consumers remain future work; packed schemas
remain inactive in public loaders and creates.
```

### row-doc-link-new

<!-- block:row-doc-link-new -->
```markdown
[Keyed bulk updates](packed-bulk-update.md) use the existing actual-size partial
constructors and preserve omitted OLD fields. Criteria/CAS updates, bulk-delete
and remaining read/index consumers stay future work; packed schemas remain
inactive in public loaders and creates.
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
    if git('rev-parse','HEAD')!=m['base_head']:raise ValueError('HEAD differs from reviewed 3B.4 commit')
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
    message='Phase 3B.5 execution halted: '+str(e)+'\nNo later task may run. Planner/human must review this note before resuming.\n'
    pathlib.Path('PLAN_NOTES.md').open('a').write(message)
    print(message,file=sys.stderr);sys.exit(1)
```

### checks

<!-- block:checks -->
```python
#!/usr/bin/env python3
import argparse,hashlib,json,pathlib,re,subprocess,tempfile

def extract(plan,key):
    m=re.search(r'(?ms)^<!-- block:'+re.escape(key)+r' -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text())
    if not m:raise ValueError('missing block '+key)
    return m.group(1)+'\n'
def run(args,expected=0):
    print('+',' '.join(args),flush=True)
    p=subprocess.run(['rtk','proxy',*args],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    print(p.stdout,end='',flush=True)
    if p.returncode!=expected:raise RuntimeError('unexpected exit '+str(p.returncode)+' expected '+str(expected))
def partial(plan,work,module=None,expected=0):
    probe=work/'partial.c';probe.write_text(extract(plan,'partial-probe'))
    extra=[]
    if module:
        obj=work/'module.o'
        run(['gcc','-O1','-g','-Isrc/db','-c',str(module),'-o',str(obj)])
        extra=[str(obj)]
    binary=work/'partial'
    run(['gcc','-O1','-g','-Isrc/db',str(probe),*extra,'build/bin/libshard-db.a',
         '-Wl,--wrap=link','-lpthread','-lssl','-lcrypto','-lm','-o',str(binary)])
    run([str(binary)],expected)
def main():
    a=argparse.ArgumentParser();a.add_argument('--plan',type=pathlib.Path,required=True)
    a.add_argument('--mode',choices=['red','green','regression-proof'],required=True);args=a.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b5-check-'));print('scratch:',work,flush=True)
    legacy=work/'legacy.py';legacy.write_text(extract(args.plan,'legacy-probe'))
    if args.mode=='regression-proof':
        manifest=json.loads(extract(args.plan,'manifest'))
        source=pathlib.Path('src/db/query_bulk.c').read_bytes()
        def digest(data):return hashlib.sha256(data).hexdigest()
        if digest(source)!=manifest['checkpoints']['2']['src/db/query_bulk.c']:
            raise ValueError('fixed source checkpoint mismatch')
        copy=work/'query_bulk.c';copy.write_bytes(source)
        edits=[e for e in manifest['edits']['2'] if e['path']=='src/db/query_bulk.c']
        for e in reversed(edits):
            old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode()
            if source.count(new)!=1:raise ValueError('reverse anchor '+e['new'])
            source=source.replace(new,old)
        if digest(source)!=manifest['parent_sources']['src/db/query_bulk.c']:
            raise ValueError('reverted source is not baseline')
        copy.write_bytes(source)
        run(['python3',str(legacy),'--source',str(copy),'--red'])
        for e in edits:
            old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode()
            if source.count(old)!=1:raise ValueError('reapply anchor '+e['old'])
            source=source.replace(old,new)
        if digest(source)!=manifest['checkpoints']['2']['src/db/query_bulk.c']:
            raise ValueError('reapplied source checkpoint mismatch')
        copy.write_bytes(source)
        run(['python3',str(legacy),'--source',str(copy)])
        module=pathlib.Path('src/db/typed_bulk_insert.c').read_bytes()
        want=manifest['checkpoints']['2']['src/db/typed_bulk_insert.c']
        if digest(module)!=want:raise ValueError('fixed module checkpoint mismatch')
        copy=work/'typed_bulk_insert.c'
        for e in reversed(manifest['review_edits']):
            old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode()
            if module.count(new)!=1:raise ValueError('review reverse anchor '+e['new'])
            module=module.replace(new,old)
        copy.write_bytes(module);partial(args.plan,work,copy,1)
        for e in manifest['review_edits']:
            old=extract(args.plan,e['old']).encode();new=extract(args.plan,e['new']).encode()
            if module.count(old)!=1:raise ValueError('review reapply anchor '+e['old'])
            module=module.replace(old,new)
        if digest(module)!=want:raise ValueError('reapplied module checkpoint mismatch')
        copy.write_bytes(module);partial(args.plan,work,copy)
        print('scratch temporary-revert/reapply proof passed; checkout untouched')
        return
    run(['python3',str(legacy),*(['--red'] if args.mode=='red' else [])])
    if args.mode=='red':return
    objects=[]
    for name in ['typed_bulk_insert','typed_row','packed_record','decnum']:
        obj=work/(name+'.o');objects.append(str(obj))
        run(['gcc','-O1','-g','-Isrc/db','-Dmalloc=p4_malloc','-Dcalloc=p4_calloc',
             '-Drealloc=p4_realloc','-Dfree=p4_free','-c','src/db/'+name+'.c','-o',str(obj)])
    probe=work/'oom.c';probe.write_text(extract(args.plan,'oom-probe'));binary=work/'oom'
    run(['gcc','-O1','-g','-Isrc/db',str(probe),*objects,'build/bin/libshard-db.a',
         '-lpthread','-lssl','-lcrypto','-o',str(binary)])
    run([str(binary)]);partial(args.plan,work);print('all Phase 3B.5 checks passed')
if __name__=='__main__':main()
```

### legacy-probe

<!-- block:legacy-probe -->
```python
#!/usr/bin/env python3
import argparse,pathlib,subprocess,tempfile

def run(args,expected=0):
    print('+',' '.join(args),flush=True)
    r=subprocess.run(['rtk','proxy',*args],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    print(r.stdout,end='',flush=True)
    if r.returncode!=expected:raise RuntimeError('exit '+str(r.returncode)+' expected '+str(expected))
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--red',action='store_true');ap.add_argument('--source',type=pathlib.Path,default=pathlib.Path('src/db/query_bulk.c'));a=ap.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b5-legacy-'));print('scratch:',work,flush=True)
    source=a.source.resolve();s=source.read_text()
    jobs=[('int cmd_bulk_update(', 'static int v2_bulk_upd_delim_pre_commit_bulk(', 'BulkUpdShardWork','bulk_upd_finish_request'),
          ('static int bulk_upd_delim_run(', 'int cmd_bulk_update_delimited(', 'BulkUpdDelimShardWork','bulk_upd_delim_finish_request'),
          ('static int bulk_upd_json_run(', 'int cmd_bulk_update_json(', 'BulkUpdJsonShardWork','bulk_upd_json_finish_request')]
    body=''
    for i,(name,following,kind,finish) in enumerate(jobs):
        lo=s.index(name);hi=s.index(following,lo);region=s[lo:hi]
        start=region.index('    /* Request execution: one deferred request over every worker\'s prepared\n')
        end=region.index('            '+finish+'(&workers[g]);',start)+len('            '+finish+'(&workers[g]);\n    }\n')
        block=region[start:end]
        body+='\nstatic int request'+str(i)+'(void){\n'+kind+' workers[1]={{0}};workers[0].prepared=1;\n'
        body+='int nshard_groups=1;Schema sch={0};const char *db_root="unused",*object="unused";\n'
        body+='armed=1;\n'+block+'armed=0;return workers[0].input.rc==-1;\n}\n'
    pre=r'''#define _XOPEN_SOURCE 700
#include "types.h"
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
static int armed;
static void *p5_calloc(size_t a,size_t b){if(armed){errno=ENOMEM;return NULL;}return calloc(a,b);}
static void *p5_malloc(size_t n){if(armed&&n==301){errno=ENOMEM;return NULL;}return malloc(n);}
static Schema p5_schema(const char *a,const char *b){(void)a;(void)b;return (Schema){0};}
static TypedSchema *p5_typed(const char *a,const char *b){(void)a;(void)b;static TypedSchema t;t.packed.nfields=1;return &t;}
#define calloc p5_calloc
#define malloc p5_malloc
#define load_schema p5_schema
#define load_typed_schema p5_typed
#include P5_QUERY_SOURCE
#undef malloc
#undef load_schema
#undef load_typed_schema
'''
    post=r'''
#undef calloc
static int cell(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return 0;
    ts->typed=1;ts->nfields=1;ts->nullmap_prefix=2;ts->total_size=404;
    ts->fields[0].type=FT_VARCHAR;ts->fields[0].size=402;ts->fields[0].name_len=2;strcpy(ts->fields[0].name,"f0");
    uint8_t old_bytes[404]={1,0},new_bytes[404]={0};char text[300];memset(text,'x',sizeof(text));
    int active[]={0};BulkUpdDelimShardWork w={.ts=ts,.active_indices=active,.active_count=1,.delimiter=','};
    BulkUpdDelimRec row={.body_start=text,.line_end=text+sizeof(text)};V2BulkUpdDelimCtx ctx={.w=&w,.rec=&row};
    SlotcaskBulkRec rec={.value=new_bytes,.user_ctx=&ctx};
    SlotcaskOldRecord old={.value=old_bytes,.vlen=sizeof(old_bytes),.hdr2=SREC_FLAG_NULLMAP};
    armed=1;int rc=v2_bulk_upd_delim_value_compute(&old,&rec);armed=0;free(ts);return rc!=0;
}
static int criteria(void){
    char *out=NULL;size_t n=0;FILE *fp=open_memstream(&out,&n);if(!fp)return 0;
    FILE *save=g_out;g_out=fp;int rc=cmd_bulk_update("unused","unused",NULL,NULL,NULL,0,1);g_out=save;fclose(fp);
    int ok=rc==1&&out&&strstr(out,"packed criteria updates await query integration");free(out);return ok;
}
int main(void){
    int (*cases[])(void)={request0,request1,request2,cell,criteria};int failed=0;
    for(int i=0;i<5;i++){
        fflush(NULL);pid_t p=fork();if(p<0)return 2;
        if(!p)_exit(cases[i]()?0:1);
        int status=0;if(waitpid(p,&status,0)!=p)return 2;
        int ok=WIFEXITED(status)&&WEXITSTATUS(status)==0;
        if(!ok)failed++;
        printf("legacy/criteria case %d: %s",i,ok?"PASS":"RED");
        if(WIFSIGNALED(status))printf(" signal %d",WTERMSIG(status));
        puts("");
    }
    printf("legacy/criteria: %d passed, %d failed\n",5-failed,failed);return failed?1:0;
}
'''
    probe=work/'probe.c';probe.write_text(pre+body+post);binary=work/'probe'
    run(['gcc','-O1','-ffunction-sections','-fdata-sections','-DP5_QUERY_SOURCE="'+str(source)+'"',
         '-Isrc/db',str(probe),'build/bin/libshard-db.a','-Wl,--gc-sections','-lpthread','-lssl','-lcrypto','-o',str(binary)])
    run([str(binary)],1 if a.red else 0)
if __name__=='__main__':main()
```

### oom-probe

<!-- block:oom-probe -->
```c
#define _XOPEN_SOURCE 700
#include "typed_bulk_insert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ftw.h>
#include <sys/stat.h>
#include <stdio.h>
ShardDb *shard_db_open_internal(const char *);
void shard_db_close(ShardDb *);
#define MUST(x) do{if(!(x)){fprintf(stderr,"probe failure line %d: %s errno %d\n",__LINE__,#x,errno);exit(1);}}while(0)
static size_t nth,calls,live,largest;static int armed;
static void *held[4096];
static int index_of(void *p){for(size_t i=0;i<live;i++)if(held[i]==p)return (int)i;return -1;}
static int allow(size_t n){if(!armed)return 1;if(n>largest)largest=n;calls++;
    if(n>8u*1024*1024||calls==nth){errno=ENOMEM;return 0;}return 1;}
static void remember(void *p){if(armed&&p){MUST(live<4096);held[live++]=p;}}
void *p4_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *p4_calloc(size_t a,size_t b){if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}if(!allow(a*b))return NULL;
    void *p=calloc(a,b);remember(p);return p;}
void *p4_realloc(void *p,size_t n){if(!allow(n))return NULL;int i=index_of(p);void *q=realloc(p,n);
    if(q){if(i>=0)held[i]=held[--live];remember(q);}return q;}
void p4_free(void *p){int i=index_of(p);if(i>=0)held[i]=held[--live];free(p);}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f){(void)s;(void)k;(void)f;return remove(p);}
static uint64_t pos(SlotcaskDb *db){SlotcaskStream *s=&db->streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return p;}
static char input_file[512];
static int invoke(const char *root,const Schema *sc,TypedSchema *ts,SlotcaskDb *db,int mode,int fresh,size_t fail){
    const char *json=fresh?"{\"b\":{\"f0\":1.25,\"f1\":\"NEW\"}}":"{\"a\":{\"f0\":1.25,\"f1\":\"NEW\"}}";
    const char *array="[{\"key\":\"a\",\"value\":{\"f0\":1.25,\"f1\":\"NEW\"}}]";
    const char *delim="a|1.25|NEW";const char *data=mode==1?array:mode==2?delim:json;
    char *text=NULL;size_t n=0;FILE *fp=open_memstream(&text,&n);MUST(fp);FILE *save=g_out;g_out=fp;
    calls=0;nth=fail;armed=1;
    int rc=mode==3?typed_bulk_update_source(root,"obj",sc,ts,db,input_file,1):
        typed_bulk_update_command(root,"obj",sc,ts,db,data,strlen(data),mode==2,'|');
    armed=0;g_out=save;MUST(!fclose(fp));MUST(live==0);free(text);return rc;
}
int main(void){
    char root[]="/tmp/p3b4-owner-XXXXXX";MUST(mkdtemp(root));ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    char object[256];snprintf(object,sizeof(object),"%s/physical",root);MUST(!mkdir(object,0700));
    TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=2;
    ts->fields[0].type=FT_NUMERIC;ts->fields[1].type=FT_VARCHAR;
    for(int i=0;i<2;i++){snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    const char *tokens[]={"numeric:26,10","varchar"};MUST(!typed_schema_bind_packed(ts,tokens,2,NULL,0));
    size_t bound=0;MUST(pkr_slot_bound(&ts->packed,32,&bound)==DEC_OK);
    Schema sc={.splits=1,.streams=1,.slot_size=(int)bound,.max_key=32,.max_value=ts->total_size};
    SlotcaskDb db;MUST(!slotcask_open(&db,object,1,1,(int)bound));
    uint8_t *seed=NULL;size_t seedn=0;MUST(!typed_encode_alloc(ts,"{\"f0\":2,\"f1\":\"OLD\"}",1,NULL,NULL,&seed,&seedn,NULL,0));
    SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};
    snprintf(input_file,sizeof(input_file),"%s/patch.json",root);
    FILE *input=fopen(input_file,"w");MUST(input);MUST(fputs("{\"a\":{\"f0\":1.25,\"f1\":\"NEW\"}}",input)>=0);MUST(!fclose(input));
    for(int format=0;format<4;format++)for(int fresh=0;fresh<(format==0?2:1);fresh++){
        MUST(!slotcask_upsert_with_hooks(&db,-1,"a",1,seed,seedn,&opts,NULL));
        (void)slotcask_delete_with_hooks(&db,"b",1,NULL,NULL);
        MUST(!invoke(root,&sc,ts,&db,format,fresh,0));size_t count=calls;MUST(count>0);
        for(size_t k=1;k<=count;k++){
            MUST(!slotcask_upsert_with_hooks(&db,-1,"a",1,seed,seedn,&opts,NULL));
            (void)slotcask_delete_with_hooks(&db,"b",1,NULL,NULL);
            uint64_t p=pos(&db),total=0,deleted=0,t2=0,d2=0;
            MUST(!slotcask_sum_kf_totals(&db,&total,&deleted));
            MUST(invoke(root,&sc,ts,&db,format,fresh,k)==1);
            void *value=NULL;size_t vn=0;uint8_t hdr=0;
            int rc=slotcask_get_ex(&db,fresh?"b":"a",1,&value,&vn,&hdr);
            if(fresh)MUST(rc!=0);
            else{MUST(!rc&&vn==seedn&&hdr==PKR_FLAGS&&!memcmp(value,seed,seedn));}free(value);
            MUST(pos(&db)==p);MUST(!slotcask_sum_kf_totals(&db,&t2,&d2));MUST(total==t2&&deleted==d2);
        }
        printf("mode %d missing %d: %zu allocation failures preserve OLD/absence, counts, position, live=0\n",format,fresh,count);
    }
    free(seed);free(ts);slotcask_close(&db);
    ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=MAX_FIELDS;
    const char *maximum_tokens[MAX_FIELDS];
    for(int i=0;i<MAX_FIELDS;i++){
        maximum_tokens[i]="varchar";ts->fields[i].type=FT_VARCHAR;
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    MUST(!typed_schema_bind_packed(ts,maximum_tokens,MAX_FIELDS,NULL,0));
    MUST(pkr_slot_bound(&ts->packed,32,&bound)==DEC_OK);MUST(bound>1000000000);
    sc.slot_size=(int)bound;sc.max_value=ts->total_size;
    snprintf(object,sizeof(object),"%s/maximum",root);MUST(!mkdir(object,0700));
    MUST(!slotcask_open(&db,object,1,1,(int)bound));
    uint8_t *maximum_seed=NULL;size_t maximum_len=0;
    MUST(!typed_encode_alloc(ts,"{\"f127\":\"OLD\"}",1,NULL,NULL,&maximum_seed,&maximum_len,NULL,0));
    MUST(!slotcask_upsert_with_hooks(&db,-1,"a",1,maximum_seed,maximum_len,&opts,NULL));free(maximum_seed);
    MUST(!invoke(root,&sc,ts,&db,0,0,0));
    void *small=NULL;size_t small_len=0;uint8_t small_hdr=0;
    MUST(!slotcask_get_ex(&db,"a",1,&small,&small_len,&small_hdr));
    MUST(small_len<4096&&small_hdr==PKR_FLAGS);free(small);
    printf("maximum schema actual value %zu; largest selected allocation %zu (<8 MiB), live=0\n",small_len,largest);
    free(ts);slotcask_close(&db);invalidate_idx_cache(root,"obj");shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return 0;
}
```

### partial-probe

<!-- block:partial-probe -->
```c
#define _XOPEN_SOURCE 700
#include "typed_bulk_insert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ftw.h>
#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>
ShardDb *shard_db_open_internal(const char *);
void shard_db_close(ShardDb *);
#define MUST(x) do { if (!(x)) { fprintf(stderr,"probe setup failure line %d: %s errno %d\n",__LINE__,#x,errno); exit(2); } } while(0)
static _Atomic int armed, fired;
static char marker_prefix[512];
int __real_link(const char *,const char *);
int __wrap_link(const char *oldpath,const char *newpath) {
    size_t n=strlen(newpath), prefix=strlen(marker_prefix);
    if(atomic_load(&armed) && !strncmp(newpath,marker_prefix,prefix) &&
       strstr(newpath+prefix,"_batch_0_") && n>=11 &&
       !strcmp(newpath+n-11,"_marker.dat") && !atomic_exchange(&fired,1)) {
        errno=EIO;return -1;
    }
    return __real_link(oldpath,newpath);
}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f) {
    (void)s;(void)k;(void)f;return remove(p);
}
static int check_row(SlotcaskDb *db,TypedSchema *ts,const char *key,const char *expected) {
    void *bytes=NULL;size_t n=0;uint8_t hdr=0;
    if(slotcask_get_ex(db,key,strlen(key),&bytes,&n,&hdr))return 0;
    char *json=typed_packed_decode(ts,bytes,n,hdr,1024*1024);free(bytes);
    int ok=hdr==PKR_FLAGS && json && strstr(json,expected);
    printf("reopened key=%s value=%s expected=%s => %s\n",key,json?json:"(decode failure)",expected,ok?"PASS":"FAIL");
    free(json);return ok;
}
static uint64_t position(SlotcaskDb *db){
    SlotcaskStream *s=&db->streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;
    pthread_mutex_unlock(&s->rotation_lock);return p;
}
static int duplicate_reject(const char *root,const Schema *sc,TypedSchema *ts,SlotcaskDb *db,
                            const char *data,int delim){
    void *before=NULL,*after=NULL;size_t bn=0,an=0;uint8_t bh=0,ah=0;
    MUST(!slotcask_get_ex(db,"a",1,&before,&bn,&bh));
    uint64_t p=position(db),t=0,d=0,t2=0,d2=0;MUST(!slotcask_sum_kf_totals(db,&t,&d));
    char *out=NULL;size_t n=0;FILE *fp=open_memstream(&out,&n);MUST(fp);FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_update_command(root,"obj",sc,ts,db,data,strlen(data),delim,',');
    g_out=save;MUST(!fclose(fp));MUST(!slotcask_get_ex(db,"a",1,&after,&an,&ah));
    MUST(!slotcask_sum_kf_totals(db,&t2,&d2));
    int failures=0;
#define DUP_GATE(label,condition) do{int ok=!!(condition);printf("duplicate format=%d %s => %s\n",delim,label,ok?"PASS":"FAIL");if(!ok)failures++;}while(0)
    DUP_GATE("reject decoded repeated key",rc==1&&out&&strstr(out,"duplicate key"));
    DUP_GATE("exact OLD preserved",bn==an&&bh==ah&&!memcmp(before,after,bn));
    DUP_GATE("no reservation",position(db)==p);
    DUP_GATE("counts preserved",t==t2&&d==d2);
    free(before);free(after);free(out);return failures;
}
int main(void) {
    int failures=0;
    char root[]="/tmp/p3b5-partial-owner-XXXXXX";MUST(mkdtemp(root));
    ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    char object[256];snprintf(object,sizeof(object),"%s/physical",root);MUST(!mkdir(object,0700));
    TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=2;
    ts->fields[0].type=FT_NUMERIC;ts->fields[1].type=FT_VARCHAR;
    for(int i=0;i<2;i++){snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    const char *tokens[]={"numeric:26,10","varchar"};MUST(!typed_schema_bind_packed(ts,tokens,2,NULL,0));
    size_t bound=0;MUST(pkr_slot_bound(&ts->packed,32,&bound)==DEC_OK);
    Schema sc={.splits=1,.streams=1,.slot_size=(int)bound,.max_key=32,.max_value=ts->total_size};
    SlotcaskDb db;MUST(!slotcask_open(&db,object,1,1,(int)bound));
    uint8_t *seed=NULL;size_t seedn=0;
    MUST(!typed_encode_alloc(ts,"{\"f0\":2,\"f1\":\"OLD\"}",1,NULL,NULL,&seed,&seedn,NULL,0));
    SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};
    MUST(!slotcask_upsert_with_hooks(&db,-1,"a",1,seed,seedn,&opts,NULL));
    MUST(!slotcask_upsert_with_hooks(&db,-1,"b",1,seed,seedn,&opts,NULL));free(seed);
    uint64_t total=0,deleted=0,t2=0,d2=0;MUST(!slotcask_sum_kf_totals(&db,&total,&deleted));
    MUST(total==2 && deleted==0);db.bulk_commit_window=1;
    snprintf(marker_prefix,sizeof(marker_prefix),"%s/data/kf/",object);
    const char *patch="{\"a\":{\"f1\":\"NEW-A\"},\"b\":{\"f1\":\"NEW-B\"}}";
    char *out=NULL;size_t used=0;FILE *fp=open_memstream(&out,&used);MUST(fp);
    FILE *save=g_out;g_out=fp;atomic_store(&armed,1);
    int rc=typed_bulk_update_command(root,"obj",&sc,ts,&db,patch,strlen(patch),0,0);
    atomic_store(&armed,0);g_out=save;MUST(!fclose(fp));
    printf("command rc=%d injected_link_failures=%d response=%s",rc,atomic_load(&fired),out?out:"(null)\n");
#define GATE(label,condition) do { int ok=!!(condition);printf("gate %s => %s\n",label,ok?"PASS":"FAIL");if(!ok)failures++; } while(0)
    GATE("one pre-marker link failure",atomic_load(&fired)==1);
    GATE("command reports failure",rc==1);
    GATE("matched=2",out&&strstr(out,"\"matched\":2"));
    GATE("updated=1",out&&strstr(out,"\"updated\":1"));
    GATE("skipped=1",out&&strstr(out,"\"skipped\":1"));
    GATE("failed row a identified",out&&strstr(out,"\"errors\":[")&&strstr(out,"\"key\":\"a\""));
    GATE("committed row b not called an error",out&&!strstr(out,"\"key\":\"b\""));
    GATE("partial outcome warning",out&&strstr(out,"outcome may be partial"));free(out);
    slotcask_close(&db);MUST(!slotcask_open(&db,object,1,1,(int)bound));
    GATE("first row remains OLD after reopen",check_row(&db,ts,"a","\"f1\":\"OLD\""));
    GATE("second row persists NEW after reopen",check_row(&db,ts,"b","\"f1\":\"NEW-B\""));
    MUST(!slotcask_sum_kf_totals(&db,&t2,&d2));
    GATE("live/deleted totals preserved",total==t2&&deleted==d2);
    db.bulk_commit_window=1024;
    failures+=duplicate_reject(root,&sc,ts,&db,
        "{\"a\":{\"f1\":\"DUPLICATE\"},\"\\u0061\":{\"f0\":3}}",0);
    failures+=duplicate_reject(root,&sc,ts,&db,"a,,DUPLICATE\na,4\n",1);
    printf("gate failures=%d\n",failures);
    slotcask_close(&db);free(ts);invalidate_idx_cache(root,"obj");shard_db_close(owner);
    MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return failures?1:0;
}
```

### review-json-key-screen-old

<!-- block:review-json-key-screen-old -->
```c
        if(b->update&&!dict&&bulk_track_key(b,key,kn)){free(key);return -1;}
```

### review-json-key-screen-new

<!-- block:review-json-key-screen-new -->
```c
        if(b->update&&bulk_track_key(b,key,kn)){free(key);return -1;}
```

### review-delimiter-key-screen-old

<!-- block:review-delimiter-key-screen-old -->
```c
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
```

### review-delimiter-key-screen-new

<!-- block:review-delimiter-key-screen-new -->
```c
        char *key=bulk_copy(data+start,key_end-start);if(!key)return -1;
        if(b->update&&bulk_track_key(b,key,key_end-start)){free(key);return -1;}
```

### review-row-status-fold-old

<!-- block:review-row-status-fold-old -->
```c
            else if(inputs[g].rc||r->status){
```

### review-row-status-fold-new

<!-- block:review-row-status-fold-new -->
```c
            else if(r->status||(!update&&inputs[g].rc)){
```

### review-partial-outcome-old

<!-- block:review-partial-outcome-old -->
```c
    int global_error=rc&&!pending&&!errors;
```

### review-partial-outcome-new

<!-- block:review-partial-outcome-new -->
```c
    int global_error=update?rc&&!pending:rc&&!pending&&!errors;
```

### manifest

<!-- block:manifest -->
```json
{
  "ancestor": "03617a898d5dff291c539b3ab33522054fd36cfb",
  "base_head": "03617a898d5dff291c539b3ab33522054fd36cfb",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "parent_sources": {
    "build.sh": "867acec0e443ed44f57111644b4bc8e1df6af5759d332de21c96a588bf18ab87",
    "src/db/query_bulk.c": "e3d8b998b3b909a474bbc681941a66d95fb0c4206780b6b19a4e5eb39cc0d1a5",
    "src/db/typed_bulk_insert.c": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
    "src/db/typed_bulk_insert.h": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
    "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
    "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
    "src/test/cases/test_typed_bulk_insert.c": "2d044445cd86e51d62ae5a311f68e7f9d077c22a7aac2a75695425088787b304"
  },
  "blocks": {
    "bulk-header-old": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
    "bulk-header-new": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
    "bulk-stubs-old": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
    "bulk-stubs-new": "68f6e8978f952bccaff9af762005d0d1de6658fdc7551f8d5e28ed7c162bb06e",
    "update-test": "a9480ccbaf4e32bea9d231f58bccf05381ac7e867742c90610eaea532b1195b0",
    "build-register-old": "0f71735a2cf918cbb848a9d65a84619c18f13cca83d71ac695e7a990c85f6d39",
    "build-register-new": "e4dae46cc9b2b1026f54bed75fa0df44b6f5b3a7e8514b5e087891b20e51231d",
    "bulk-implementation-old": "68f6e8978f952bccaff9af762005d0d1de6658fdc7551f8d5e28ed7c162bb06e",
    "bulk-implementation-new": "90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b",
    "query-edit-1-old": "afede2b23edbd2a2dd0403a1531264202b73436409d476b6435d02375ea5442d",
    "query-edit-1-new": "346b8f52d2683ad81bc9d30a69b65a5533fb56f81821beb85b093319699430cb",
    "query-edit-2-old": "657f21946cb4493c525985181401554d3c5912f3626c07928744660d46c251fe",
    "query-edit-2-new": "91a0caf7846b85e95141c692802e79d066d340b6ee2d70bdf675056bdc6e660e",
    "query-edit-3-old": "f9e0842c8221f47e1d85564819f395a41d07b3a4a406f62abd511790fe81de1b",
    "query-edit-3-new": "fcb5e4f8cc7876ac108b07238375c656472680ac9b712db8f4d51632f3ae87d5",
    "query-edit-4-old": "7c336640ecdddd5b574a6d710649b75f72e366d3070a2729810340ab50764388",
    "query-edit-4-new": "2c630785f8b317265dd4ee13b4fd111334513ae0827bc3777b8046d676024269",
    "query-edit-5-old": "07c5d133e1abfb5f59dbd757c559aa4c6dd4d3bce0a4938e7d047dd4bc76214e",
    "query-edit-5-new": "bfa98d6f105c8b2b4852a43c82ec9ba8ed19d9e2bf71414f50eb1d2675ac5440",
    "query-edit-6-old": "3dd562d8659dc74e5a6c83d3405f6f34cfddd1f29354e7382cd58db3623c6c0b",
    "query-edit-6-new": "93430cf4274db156a30fdef02a0489108422ad5f2315db874ae1a655da42aa61",
    "query-edit-7-old": "7a4772f25f8c8e03431345c27a71f125428d90dfce2338699ae54464601478ae",
    "query-edit-7-new": "dc187ac441c88b6a7b7ee6c9607047fba4305861d62aff22339882f674eedbf4",
    "update-doc": "a3ad8b1d8b5d65e066b1e763b45122964eb6766ab5cd5ba230c9301f36e268e8",
    "insert-doc-link-old": "9032ef9d16addcf3d16d5c3e92fd5707659780b21ffc81d91714c18f09096d3e",
    "insert-doc-link-new": "8a606f0b6ca694f35efe3c1caf9969a17c09b04408d16edd207f1900efbe3ad0",
    "row-doc-link-old": "60d158c1712f3e1b9286cbef6e30f11f91ed8d49b13dfc7b7cfa8c5110811ff3",
    "row-doc-link-new": "46afba61dcc444fad7b90927ed55c00e72a8fc4e1b70195e517f4613aff54928",
    "driver": "c28dce7de3b26b6b5fe9297f276dfa7938002d297a4b9e8a9492635537a45ff1",
    "checks": "396b06d6b56b663bdc06c6f2dd1e38a66bbf9d5272916b5baa7f7b406676487a",
    "legacy-probe": "b3f523601d4489c89bcb8544dc033f53f2f8825ba14b4bbcd2f3ec028caf0b67",
    "oom-probe": "8f3a1f4886507bb48b7a2c489c0910fed0d82250043e6a05bd37a843d389acf8",
    "partial-probe": "47bf5363afa7c39e763338b33842ce830d1b7bc9b4da923ec70c0ec4f9a9afcf",
    "bootstrap": "a2fb7b05730b9d4cbaaa73623d4b60ffab854ddba61d014ed3d1db3e0ebcec33",
    "review-json-key-screen-old": "63aa7e9e4331cb297736ffe7a0e95246fd2db5e6280ae704cc40a812b93f801d",
    "review-json-key-screen-new": "1ef2e1b3aacb8fb482c55f0125bf8dae190e14e661322a9dd39cb78986da7be5",
    "review-delimiter-key-screen-old": "f6fcb11bf90570769d848a230408315a726e26a1613d5b98ee221c72f94e401d",
    "review-delimiter-key-screen-new": "313567299590106682c65203e53b23bafb6a091d57ec946884c0737f4c7415ed",
    "review-row-status-fold-old": "40ea0faa84b87a4cadaa32f97115f864e8868241563e232ca886dae8d519d577",
    "review-row-status-fold-new": "9fbc492970ce6b93096a93a7860efbc746ea35967b5a77693aa759a882babccb",
    "review-partial-outcome-old": "ee9641941c7c5d856537c84bb2e25d63ef5249d9aa682a5b0365d18fd326208f",
    "review-partial-outcome-new": "d0bc9fdf5d5405588d6f1151b29954fff5546d409b6b8cbc3091a99731fb5077"
  },
  "edits": {
    "1": [
      {
        "path": "src/db/typed_bulk_insert.h",
        "old": "bulk-header-old",
        "new": "bulk-header-new"
      },
      {
        "path": "src/db/typed_bulk_insert.c",
        "old": "bulk-stubs-old",
        "new": "bulk-stubs-new"
      },
      {
        "path": "src/test/cases/test_typed_bulk_update.c",
        "old": null,
        "new": "update-test"
      },
      {
        "path": "build.sh",
        "old": "build-register-old",
        "new": "build-register-new"
      }
    ],
    "2": [
      {
        "path": "src/db/typed_bulk_insert.c",
        "old": "bulk-implementation-old",
        "new": "bulk-implementation-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-1-old",
        "new": "query-edit-1-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-2-old",
        "new": "query-edit-2-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-3-old",
        "new": "query-edit-3-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-4-old",
        "new": "query-edit-4-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-5-old",
        "new": "query-edit-5-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-6-old",
        "new": "query-edit-6-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "query-edit-7-old",
        "new": "query-edit-7-new"
      }
    ],
    "3": [
      {
        "path": "docs/concepts/packed-bulk-update.md",
        "old": null,
        "new": "update-doc"
      },
      {
        "path": "docs/concepts/packed-bulk-insert.md",
        "old": "insert-doc-link-old",
        "new": "insert-doc-link-new"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "row-doc-link-old",
        "new": "row-doc-link-new"
      }
    ]
  },
  "review_edits": [
    {
      "old": "review-json-key-screen-old",
      "new": "review-json-key-screen-new"
    },
    {
      "old": "review-delimiter-key-screen-old",
      "new": "review-delimiter-key-screen-new"
    },
    {
      "old": "review-row-status-fold-old",
      "new": "review-row-status-fold-new"
    },
    {
      "old": "review-partial-outcome-old",
      "new": "review-partial-outcome-new"
    }
  ],
  "checkpoints": {
    "0": {
      "build.sh": "867acec0e443ed44f57111644b4bc8e1df6af5759d332de21c96a588bf18ab87",
      "docs/concepts/packed-bulk-insert.md": "9c35f7d94275d92471a1eda6cff1759a77e43b2738ea34d55d3d5e9abed14bff",
      "docs/concepts/packed-bulk-update.md": null,
      "docs/concepts/typed-row-adapter.md": "c01b0e2110e4dbad7cd5ebfddf97160d2b40ed9d2baa7775ebe4edfd670e6ad0",
      "src/db/query_bulk.c": "e3d8b998b3b909a474bbc681941a66d95fb0c4206780b6b19a4e5eb39cc0d1a5",
      "src/db/typed_bulk_insert.c": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
      "src/db/typed_bulk_insert.h": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
      "src/test/cases/test_typed_bulk_update.c": null
    },
    "1": {
      "build.sh": "149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999",
      "docs/concepts/packed-bulk-insert.md": "9c35f7d94275d92471a1eda6cff1759a77e43b2738ea34d55d3d5e9abed14bff",
      "docs/concepts/packed-bulk-update.md": null,
      "docs/concepts/typed-row-adapter.md": "c01b0e2110e4dbad7cd5ebfddf97160d2b40ed9d2baa7775ebe4edfd670e6ad0",
      "src/db/query_bulk.c": "e3d8b998b3b909a474bbc681941a66d95fb0c4206780b6b19a4e5eb39cc0d1a5",
      "src/db/typed_bulk_insert.c": "68f6e8978f952bccaff9af762005d0d1de6658fdc7551f8d5e28ed7c162bb06e",
      "src/db/typed_bulk_insert.h": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
      "src/test/cases/test_typed_bulk_update.c": "a9480ccbaf4e32bea9d231f58bccf05381ac7e867742c90610eaea532b1195b0"
    },
    "2": {
      "build.sh": "149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999",
      "docs/concepts/packed-bulk-insert.md": "9c35f7d94275d92471a1eda6cff1759a77e43b2738ea34d55d3d5e9abed14bff",
      "docs/concepts/packed-bulk-update.md": null,
      "docs/concepts/typed-row-adapter.md": "c01b0e2110e4dbad7cd5ebfddf97160d2b40ed9d2baa7775ebe4edfd670e6ad0",
      "src/db/query_bulk.c": "c619ab1d8439f9468b80e7bd12fbc6f3d578b29436cef45d47a568db91f07908",
      "src/db/typed_bulk_insert.c": "90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b",
      "src/db/typed_bulk_insert.h": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
      "src/test/cases/test_typed_bulk_update.c": "a9480ccbaf4e32bea9d231f58bccf05381ac7e867742c90610eaea532b1195b0"
    },
    "3": {
      "build.sh": "149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999",
      "docs/concepts/packed-bulk-insert.md": "a647f6caaa3bdcb93776283a2e520eadd005c178e1f826ffda3766bd89c40c28",
      "docs/concepts/packed-bulk-update.md": "a3ad8b1d8b5d65e066b1e763b45122964eb6766ab5cd5ba230c9301f36e268e8",
      "docs/concepts/typed-row-adapter.md": "0a84766a6041f615e76e69403975e002eda99aeb0a22bed34aec74f67bdadf3a",
      "src/db/query_bulk.c": "c619ab1d8439f9468b80e7bd12fbc6f3d578b29436cef45d47a568db91f07908",
      "src/db/typed_bulk_insert.c": "90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b",
      "src/db/typed_bulk_insert.h": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
      "src/test/cases/test_typed_bulk_update.c": "a9480ccbaf4e32bea9d231f58bccf05381ac7e867742c90610eaea532b1195b0"
    }
  }
}
```
