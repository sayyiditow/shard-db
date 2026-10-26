# Phase 3B.4 — packed bulk insert/upsert (inactive)

## Primary goal

Prepare JSON and positional bulk insert/upsert to store exact finance decimals
and compact variable-length strings without losing OLD values, misreporting
construction failures as condition skips, or allocating a schema-maximum buffer
for every row. Existing applications keep working while packed schemas remain
inactive until the required integrations and migration are ready.

**Status:** Revision 2, independent blind review corrected; handoff after human approval.

Revision 2 fixes the blind review's P2 delimiter-free-line data-loss issue:
key-only lines are skipped, with existing-row/absent-key/count/segment regressions
and an explicit-empty-column guard. It pins the now-committed 3B.3 base, adds
read-only driver preflight, and revalidates all extracted blocks and checkpoints.
Planner: GPT 6.1 Sol. No executor identity is inferred for this child.
Prepared against reviewed Phase 3B.3, committed as
`a97f9f21caa0afcd29ae54ec665abf79466783ec` on
`fix/phase1-safe-scalar-conversions`. Production files were not
changed while preparing this plan. All proposed code was exercised in scratch.

## Boundary and proposed design decisions

This is one complete bulk insert/upsert slice: JSON dict/array and bounded
positional delimited input, covering both file and inline wrappers through the
two existing runners. It excludes bulk-update/delete, reader integration,
public activation, metadata publication, index semantics, and migration.

The proposed internal `typed_bulk_insert` module collects source spans and owns
actual NEW buffers through the existing synchronous deferred request. This is
a deep command adapter, not a new commit protocol or a rewrite of legacy bulk
workers. Packed branches in both runners reach the exact same function exercised
by explicit-schema physical tests. Loader/create-object/SDK surfaces remain
unchanged; no global testing switch or schema-cache mutation is added.

Approval of this child chooses these interim guardrails: packed auto-key objects
and all packed secondary indexes reject before generators or writes; their full
contracts remain Phase 4/5 work. JSON defaults and AUTO_CREATE use the existing
row adapter; positional input preserves its separate no-JSON-defaults semantics.
The shared legacy collector's concrete OOM reporting bug is repaired here.
No new dependency, segment cap, public format or command-wide memory cap is added.
Memory is checked for allocation/size overflow and grows with actual data;
request-wide expansion budgeting remains explicit later activation work, not
claimed solved by this child. No performance gain is claimed or benchmark run.

## Task 0 — documentation, consumer census and checkpoint

**How this serves the primary goal:** establishes the exact supported routes,
ownership and durability contracts before introducing another writer.

Read AGENTS.md, CORE-PROCESS.md and these documents (the certification rig is
strictly excluded):
- `docs/plans/2026-10-23-phase3-packed-records-plan.md`, the approved 3B.3 plan,
  its evidence and raw-diff review; shared codec/row/owning-write concept docs.
- `docs/query-protocol/bulk.md`, `docs/query-protocol/cas.md`,
  `docs/concepts/concurrency.md`, `docs/concepts/storage-model.md`.
- `src/db/typed_row.h`, `src/db/slotcask.h`, `src/db/packed_record.h`.

Verified allowed APIs: `typed_insert_alloc`, `typed_prepared_zero/set/alloc`,
`typed_row_open/span`, `typed_prepared_stamp`, `pkr_record_fits`,
`json_unescape_string`, `json_skip_value` (after full validation),
`slotcask_bulk_request_execute`, `slotcask_registry_get/put`, `SDB_REG_REF`,
`compute_hash_raw`, `compute_record_shard`, `QueryDeadline`,
`slotcask_get_ex`, `slotcask_sum_kf_totals`. New internal APIs are fully defined
below; do not invent bulk owning options or pass bulk buffers to the single
owning constructor. The M-phase `value_compute` return is a skip, requiring
our row-local hard-error side channel. One request input per touched kf shard;
request execution is synchronous and joins the pipelines before buffer release.

Consumer inventory, verified against this source:

| Surface | Existing consumers / consequence |
|---|---|
| JSON insertion runner | `cmd_bulk_insert`, `cmd_bulk_insert_string` -> `bulk_ins_run`; file/stdin and inline share the new packed branch |
| Positional runner | `cmd_bulk_insert_delimited`, `cmd_bulk_insert_delimited_string` -> `bulk_ins_delim_run`; explicit byte lengths retained |
| Dispatch | `server.c` routes dict/array and delimited requests to those wrappers; embedded dispatch shares the command implementation; no SDK signature change |
| Existing bulk collector | `bulk_ins_collect_inputs` called by both runners; the requested count must survive calloc failure so both callers mark prepared inputs failed |
| Row construction | existing `typed_insert_alloc`: storage single insert callback and its tests; same signature and behavior retained, AC finalization extracted completely |
| New row helpers | validator used only by new collector; positional constructor used only by new value callback; shared finalizer used by both JSON and positional insert |
| Transport | `SlotcaskBulkRec`/`SlotcaskBulkOpts`/`SlotcaskBulkShardInput` ABI and coordinator unchanged; no new recovery consumer |
| Build | five module lists: daemon, embedded static library, test-server daemon, test runner, benchmark binary; benchmark is built but never run |
| Future bulk-update | criteria, JSON, positional callbacks remain legacy/fixed-layout and separate; this child does not silently enable them |
| External availability | packed binder remains absent from public load/create paths; private adapter coverage is not public packed route acceptance |

Root cause and invariants:
1. Both insertion runners allocate `ts->total_size` per row and legacy helpers
   interpret offsets as scalar bytes; that is invalid for packed descriptors.
   New code collects exact spans and calls the shared packed constructor.
2. Bulk callback nonzero means `status=-2`; blindly using it for encode/OOM
   failures reports a false condition skip. Error side-channel is owned per row.
3. The old collector sets `*ninputs=0`, then returns NULL on calloc failure;
   both callers test `ninputs>0 && !inputs`, so they miss OOM and can report
   unexecuted prepared rows as inserted. Set requested count before allocation.
4. No partial malformed JSON document writes: validate the complete value first.
   Per-row invalid key/value/scalar uses best-effort accounting, not transaction
   rollback. No generator rollback is promised. `if_not_exists` skips OLD before
   invoking defaults/construction. OLD generation is checked before defaults.
5. Buffers outlive the entire request, including pending/error results. No OLD
   aliasing, shared row mutation, schema mutation or mutable global context.
6. Physical fit uses actual key/value, slot bound and existing segment ceiling.
   Small records under a maximum schema never allocate the theoretical slot.
7. Duplicate fields inside rows follow the shared last-valid-occurrence policy.
   Record duplicate outcomes remain the engine's, in input order per shard;
   no blanket last-wins or whole-batch-atomic promise is introduced. A planning
   probe observed two fresh duplicate keys in one window: first inserted,
   second error. Full cross-window policy remains Phase 4 activation work.
8. Empty numeric rejects; NULL and empty varchar remain distinct. Positional
   missing fields are padded empty; surplus ignored; removed fields consume no
   column. A nonempty line without any separator is skipped without writes;
   an explicit separator followed by empty content is a real input column.
   Empty fixed scalars stay present zero. No CSV quoting extension.

Execution rules:
- Do not start until this exact plan is reviewed and explicitly approved.
- Start exactly at `a97f9f21caa0afcd29ae54ec665abf79466783ec`, the committed
  reviewed 3B.3 checkpoint, on the existing fix branch. Do not create/reset
  branches or use a fresh main checkout. The driver pins this exact HEAD and
  committed parent-source hashes at every stage. Do not commit in this run.
  Its `--check-only` option performs full stage preflight without source writes;
  use it at Task 0 before applying stage 1.
- Record branch/HEAD, approved plan SHA256 and the protected SHA at start/end.
  Protected file is hash-only: never inspect its contents, edit/move/stage it,
  or incorporate/execute it. The driver reads its bytes only into SHA256.
- Planning artifacts may preexist; inventory them. Baseline owned sources must
  match checkpoint 0, and no unrelated implementation changes may be present.
- Execute Tasks 1–4 in order. All writes go through the extracted driver after
  all anchors/checksums for a stage preflight successfully. Never reinterpret
  an anchor or write code from memory. A mismatch, unexpected warning/result,
  failed test, or uncovered decision means append PLAN_NOTES.md and halt the
  entire run. The planner/human must read it and amend the plan before resume.
- Capture RED with stub + tests before implementation, not by reverting fixes.
  Do not weaken assertions or rerun a flaky failure until green.
- Shell commands always use rtk. Normal builds only:
  `rtk proxy env SKIP_TESTS=1 ./build.sh` and normal test binary below.
  If local sockets are denied, request the permitted socket-capable tool mode
  before the full suite; do not skip server cases or silently change test mode.
- No sanitizers or benchmarks in this child. Branch sanitizer gates remain
  after ALL fixes and immediately before an explicitly authorized push/PR.
- Leave all work unstaged/uncommitted for plan-aware and independent blind
  raw-diff review. No add/commit/push/PR/merge, no Phase 3B.5 or activation.
  Keep raw logs/probe source/objects/binaries under a fresh /tmp directory only.

## Task 1 — add the real regression and prove RED

**How this serves the primary goal:** proves actual record writes and outcome
handling fail without the proposed integration, rather than merely testing a codec.

Bootstrap Appendix A's `driver` and `checks` blocks verbatim to a fresh /tmp
folder. A bootstrap extraction recipe appears below. The driver applies stage 1:
new internal header, explicit ENOTSUP stub, full regression, all five build-list
edits and one test registration. It does not change existing production behavior.

Run stage 1, build, focused RED, then the shared-collector RED probe in exactly
that order; preserve complete raw output and exit status. A zero-warning build
is mandatory. Expected focused RED: **102 passed / 33 failed, exit 33**.
Failures must show no physical rows, no fresh counts/default/AC/NULL outcomes,
no successful positional replacement, and no actual-size small/max-varchar
insertion. Some rejection assertions pass incidentally on a stub; they do not
prove validation. The same final case becomes **178/0** as real physical reads
make additional downstream assertions reachable. Never demand a fixed assertion
count across branches that return early; use the measured counts here.

The collector probe must show `pointer=(nil) requested=0 (expected NULL/2)`
and exit 1; the verification runner treats that expected failing result as
successful RED evidence. It compiles the actual private static helper through
an include from source with a local calloc substitute; no production test seam.

Copy exact fixture conventions from test_single_write_integration.c:
heap schema, private mkdtemp physical object, TLS response capture, actual raw
get/decode, reopen and guarded segment/count snapshots. No loader cache binding,
fixed ports, process environment mutation or testing activation switch.

## Task 2 — integrate construction, transport and failure accounting

**How this serves the primary goal:** writes packed rows through the real bulk
coordinator with exact values and safe ownership across every joined pipeline.

Apply stage 2 only after both RED proofs. The manifest lists every complete
old/new hunk: full new module; row declarations; complete JSON/positional insert
construction/finalizer functions; query_bulk include and two route branches;
and the shared collector's requested-count repair. Anchors include enough
context to be unique (the old invariant comment alone appears twice).

The module sorts collected rows by shard and original ordinal before any
callback exposes pointers, fills one copied option/input per touched shard,
and calls the existing coordinator exactly once. NEW buffers are owned by rows,
not by Slotcask's single owning hook. Retain them until the request returns and
free on every result, including errors/pending. `value_rewrites_payload=1`
forces construction under locked OLD before any NEW staging. A full OLD view
is validated even when no AUTO_CREATE field exists. Preserve decoded key spans,
NULL, embedded NUL varchar, exact numeric text and distinct row hard errors.

Rebuild zero-warning, run focused GREEN **178/0**, then the green checks runner.
All 90 injected failures (JSON existing 26/fresh 20, positional existing 25/fresh
19) must return error and preserve exact OLD/absence, counts, segment position,
and zero tracked live buffers. Instrument only the four listed modules;
compile selected objects without LTO and **without TEST_BUILD** when linking
against the normal embedded library (otherwise segment-limit API differs).
The maximum-schema probe must compose 2088 actual bytes; largest selected
allocation is 3072, below the probe's 8 MiB guard. No claim that uninstrumented
core/libc allocations were injected. The separate collector probe must now
show requested=2 and exit 0.

Anti-pattern guards: no `total_size` row arena/slab; no single owning option
for bulk; no use-after-return source spans; no error-to-skipped conversion;
no NEW/source bytes staged before construction; no legacy trim binding for
packed; no per-row separate coordinator transactions; no new locks/replay path.
The new constructor shares the existing AC finalizer; it does not run JSON
literal/sequence/random/AU defaults for positional input. Existing JSON single
write behavior must stay green after that extraction.

## Task 3 — synchronize the ownership and availability documentation

**How this serves the primary goal:** prevents callers or later planners from
mistaking private bulk support for finished public numeric support.

Apply stage 3: new packed-bulk-insert concept document, exact row-adapter doc
hunks and correction of the stale "no production constructor caller" paragraph
in owning-write-hooks.md. These are complete blocks, not prose editing tasks.
Document actual bytes versus theoretical schema bound, per-row best effort,
condition/error distinction, separate bulk lifetime, decoder/positional rules,
interim auto-key/index guard, memory-cap boundaries and all activation deferrals.

## Task 4 — full verification, evidence and hand-back

**How this serves the primary goal:** checks existing applications alongside the
new private physical integration and hands back an auditable, reviewable diff.

After stage 3, run the fresh normal full suite and all green probes. Compare
all checkpoint-3 hashes, `rtk git diff --check`, and production census. Required:
**19,643 passed / 0 failed across 522 cases**, exit 0; focused **178/0**;
collector probe requested=2; allocation probe all **90** failures pass and
maximum-schema actual 2088 / largest selected 3072. A different result requires
investigation and PLAN_NOTES.md, not editing expected counts on the fly.

Record actual command transcripts, environment/compiler, start/end identity,
protected hash, RED/GREEN counts, probe outputs, file census and deviations in
`docs/plans/2026-10-26-phase3b4-packed-bulk-insert-evidence.md`. Do not commit raw
logs, debug sources, probe binaries or any temporary build artifacts. Evidence
is a curated Markdown ledger; it is intentionally not a frozen predetermined
claim. Confirm no new TODO/FIXME/debug prints, warnings or public packed binding.
All declared new functions/callbacks and command routes must match Appendix B.

Both raw-diff passes remain mandatory before cleanup/commit: one checks the
approved plan and one reviewer receives only actual source/diff/repo context,
without the plan. This plan's planning-time checks do not substitute for that
post-execution review. Leave changes unstaged/uncommitted and stop.

## Planning-time verification (ordinary scratch only)

Scratch: `/tmp/p3b4-plan-y4v75qxd`, created from HEAD archive with the exact
reviewed 3B.3 code overlaid. Protected contents were never copied or read.
- Initial Revision 1 stage-1 normal build was zero-warning, RED97/28.
  Revision 2's expanded case is RED **102/33**, rc33, measured in an isolated
  stub-linked normal-library runner with an explicit private ShardDb owner;
  executor still uses the prescribed complete normal build at stage 1.
- Complete proposed source builds warning-clean under normal production flags.
- Focused GREEN **178/0**; full **19643/0**, 522 cases, rc0.
- Allocation probe: 26 + 20 + 25 + 19 failures, all preserve state/ownership.
- Maximum schema: actual2088, largest selected3072, live0.
- Collector actual helper: RED count0/rc1; GREEN count2/rc0.
- Blind-review regression against the unfixed real module: **173/5**, rc5;
  exactly the new preservation checks fail. After the separator guard: **178/0**.
- Supplemental duplicate probe: fresh duplicate keys in one window report
  inserted1/errors1 and preserve FIRST. This does not certify cross-window policy.
No sanitizer, benchmark or production implementation ran for this child.
The human-requested reviewed 3B.3 commit is separate from scratch plan validation.

Assembly corrections are disclosed: the first anchor candidate (the invariant
comment) matched twice, caught before plan publication and replaced by unique
context; an early scratch build was stopped after that preflight failed. The
first OOM harness link used TEST_BUILD objects with the normal library and
failed on slotcask_seg_max_bytes; final complete commands deliberately omit that
flag. A supplemental duplicate probe initially assumed rc0; its actual rc1
exposed the existing per-row duplicate rejection, now documented rather than
changing the engine or weakening the committed regression. Final code/probe
outputs above are from the corrected recipes. A final comment-only assembly
cleanup moved the insertion comment back above its function and preserved the
existing scalar-patch comment; the compiled C tokens are unchanged. No persistent warnings accepted. The first isolated stub-runner attempt
omitted the test runner's process ShardDb initialization and stopped at open;
that scratch harness was corrected to establish/release a private owner before
measuring Revision 2 RED. It is not part of the plan or production sources.

## What follows this child

This is an order, not a promise of a fixed remaining child-plan count:
1. Review/cleanup/commit 3B.4 only after actual raw-diff review; no push yet.
2. Next write child: all bulk-update forms, computed/criteria guard ownership,
   deletion paths as required by their consumer census, actual-size composition
   and failure-preserving outcome accounting. Split further if that inventory
   cannot be implemented and validated coherently in one child.
3. Remaining 3B readers/query/field spans, sort/join/group/index extraction and
   maintenance/rebuild/vacuum/recovery consumers; support validated spans or
   explicit private guards, never interpret descriptors as legacy scalar bytes.
4. 3C authoritative metadata, downgrade gates, crash-safe publication; keep
   format inactive until required write/query/index/aggregate support is ready.
5. Phase 4 completes public write/default/auto-key/computed and related field
   surfaces; Phase 5 exact criteria/index ordering; Phase 6 exact aggregates.
6. Phase 7 remaining scalar type correctness (calendar/bool/UUID/IP/enum/string).
7. Phase 8 validated transactional conversion and migration of old physical
   rows/indexes; a migration cannot recover numeric digits already corrupted.
8. Phase 9 application acceptance, activation gate, docs and final review;
   normal full suite plus branch-final ASan+UBSan 3x and TSan 3x (`--jobs 2`)
   immediately before an explicitly authorized push/PR. Activation requires
   the full reader/writer/metadata/migration contract, not merely 3B completion.
9. Protected durability certification work only after this roadmap lands.

## Appendix A — extraction and exact commands

Use the bootstrap block below to extract every named block to a fresh /tmp
folder. The extraction is planning tooling only and stays out of version control.
Then call `driver.py` stages1/2/3 and `checks.py` as specified. Record the fresh
folder and exact approved plan SHA. Do not use the planning scratch path as the
executor's evidence path.

<!-- block:bootstrap -->
```python
import pathlib,re,tempfile
plan=pathlib.Path('docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md')
folder=pathlib.Path(tempfile.mkdtemp(prefix='p3b4-exec-'))
for m in re.finditer(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',plan.read_text()):
    (folder/(m.group(1)+'.txt')).write_text(m.group(2)+'\n')
for name in ['driver','checks']:
    (folder/(name+'.py')).write_bytes((folder/(name+'.txt')).read_bytes())
print(folder)
```

Illustrative invocation order below: substitute the printed fresh folder for
`/tmp/p3b4-exec-XXXX` and capture raw output/exit in that same folder. These
commands have no permission to add/commit files. Expected RED exit33 is required,
not a reason to skip Task 1 evidence. The checks runner captures the collector's
expected RED exit1 internally.

~~~sh
rtk proxy python3 /tmp/p3b4-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --stage 1 --check-only
rtk proxy python3 /tmp/p3b4-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-bulk-insert
rtk proxy python3 /tmp/p3b4-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --mode red-collect
rtk proxy python3 /tmp/p3b4-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-typed-bulk-insert
rtk proxy python3 /tmp/p3b4-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --mode green
rtk proxy python3 /tmp/p3b4-exec-XXXX/driver.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 /tmp/p3b4-exec-XXXX/checks.py --plan docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md --mode green
rtk git diff --check
~~~

## Appendix B — complete code and edit blocks

Each manifest edit references `old` and `new` blocks below. Match exact bytes
once. New-file edits require absence. The driver preflights a whole stage and
checks complete-file output hashes before writing. All code is normative; the
source census and measured counts are not permission to improvise replacements.


### bulk-header

<!-- block:bulk-header -->
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

### bulk-stub

<!-- block:bulk-stub -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)data;
    (void)len;(void)format;(void)delimiter;(void)if_not_exists;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk insert unavailable\"}\n");return 1;
}
```

### bulk-test

<!-- block:bulk-test -->
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

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} BFixture;
static int b_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void b_close(BFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    free(f->ts);slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,b_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove private fixture");
}
static int b_open(BFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private packed schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:5;
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
        ts->fields[4].default_kind=DK_AUTO_CREATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema bound");
    f->sc=(Schema){.splits=4,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-packed-bulk-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,4,1,(int)slot);
    f->opened=!rc;ASSERT_EQ_INT(rc,0,"open multi-shard private object");return rc;
}
static int b_call(BFixture *f,const char *data,size_t n,int delim,int strict,char **out){
    size_t used=0;*out=NULL;FILE *fp=open_memstream(out,&used);if(!fp)return -1;
    FILE *save=g_out;g_out=fp;
    int rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,data,n,delim,'|',strict);
    g_out=save;if(fclose(fp))return -1;return rc;
}
static int b_json(BFixture *f,const char *s,int strict,char **out){return b_call(f,s,strlen(s),0,strict,out);}
static char *b_get(BFixture *f,const char *key,size_t *n){
    void *bytes=NULL;uint8_t hdr=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&bytes,n,&hdr))return NULL;
    ASSERT_EQ_INT(hdr,PKR_FLAGS,"physical generation is packed");
    char *s=typed_packed_decode(f->ts,bytes,*n,hdr,16*1024*1024);free(bytes);return s;
}
static uint64_t b_pos(BFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t p=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return p;
}
static void b_reject(BFixture *f,const char *json){
    size_t n=0,m=0;char *before=b_get(f,"a",&n),*out=NULL;uint64_t pos=b_pos(f),t=0,d=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t,&d),0,"count before rejection");
    ASSERT_EQ_INT(b_json(f,json,0,&out),1,"bad batch/row rejected");
    ASSERT_NOT_NULL(out,"error response exists");if(out){ASSERT_CONTAINS(out,"error","error response");free(out);}
    char *after=b_get(f,"a",&m);ASSERT_EQ_INT(n,m,"OLD actual size unchanged");
    if(before&&after)ASSERT_EQ_STR(after,before,"OLD exact value unchanged");
    ASSERT_TRUE(b_pos(f)==pos,"no segment reservation on rejected input");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"count after rejection");
    ASSERT_TRUE(t==t2&&d==d2,"rejected counts unchanged");free(before);free(after);
}
static void b_routes(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":1234567890123456.1234567890,\"f1\":\"old\",\"f2\":1.0e2},\"b\":{\"f2\":-0.0000000000000000000000000001}}",0,&out),0,"dict across shards");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":2","two fresh insertions");free(out);}
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"dict physical value");char ac[80]={0};
    if(s){ASSERT_CONTAINS(s,"1234567890123456.1234567890","exact fixed numeric");ASSERT_CONTAINS(s,"\"f2\":100","bare exponent");
        ASSERT_CONTAINS(s,"\"f3\":7","JSON default");char *v=strstr(s,"\"f4\":");if(v)memcpy(ac,v,strcspn(v,",}"));free(s);}
    ASSERT_TRUE(n<256,"small physical row");
    ASSERT_EQ_INT(b_json(&f,"[{\"key\":\"a\",\"value\":{\"f1\":\"a\\u0000b\",\"f4\":1}},{\"key\":\"c\",\"value\":{\"f1\":null}}]",0,&out),0,"array upsert and fresh NULL");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","updates not counted as fresh");free(out);}
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"upsert physical value");
    if(s){ASSERT_CONTAINS(s,"a\\u0000b","embedded NUL varchar");ASSERT_TRUE(ac[0]&&strstr(s,ac),"AC copied from locked OLD");free(s);}
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":\"invalid\"},\"d\":{\"f2\":2}}",1,&out),0,"strict existing skip before constructor");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","strict new committed");ASSERT_CONTAINS(out,"\"skipped\":1","existing is condition skip");free(out);}
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f0\":\"\"},\"e\":{\"f2\":3}}",0,&out),1,"mixed row failure distinct from skip");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":1","good row commits");ASSERT_CONTAINS(out,"\"errors\":1","bad constructor counted error");ASSERT_CONTAINS(out,"\"skipped\":0","not condition skip");free(out);}
    b_reject(&f,"{\"a\":{},\"z\":{\"f1\":\"\\uZZZZ\"}}");
    b_reject(&f,"{\"a\":{\"f3\":2147483648}}");
    b_reject(&f,"{\"a\":{\"f2\":\"\"}}");
    b_reject(&f,"[{\"key\":\"a\",\"value\":{}}]garbage");
    ASSERT_EQ_INT(b_json(&f,"{\"esc\\u0061ped\":{\"f1\":\"x\"}}",0,&out),0,"escaped dictionary key decoded");free(out);
    s=b_get(&f,"escaped",&n);ASSERT_NOT_NULL(s,"decoded key stored");free(s);
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,f.root,4,1,slot),0,"reopen physical bulk object");f.opened=1;
    s=b_get(&f,"b",&n);ASSERT_NOT_NULL(s,"reopened exact bare value");
    if(s){ASSERT_CONTAINS(s,"-0.0000000000000000000000000001","tiny decimal preserved");free(s);}b_close(&f);
}
static void b_delimited(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    const char data[]="a|1.25|first|2.5|9|1\r\nb|2.5||0|\nc|3|text|4|5|1|ignored\n";
    ASSERT_EQ_INT(b_call(&f,data,sizeof(data)-1,1,0,&out),0,"positional CRLF and ignored surplus");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":3","delimited inserted");free(out);}
    char *s=b_get(&f,"b",&n);ASSERT_NOT_NULL(s,"empty positional physical row");
    if(s){ASSERT_CONTAINS(s,"\"f1\":\"\"","empty varchar present");ASSERT_CONTAINS(s,"\"f3\":0","empty scalar zero; no JSON defaults");free(s);}
    const char binary[]={'a','|','4','|','x',0,'y','|','5','|','6','\n'};
    ASSERT_EQ_INT(b_call(&f,binary,sizeof(binary),1,0,&out),0,"length-delimited embedded NUL");free(out);
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"binary-delimited physical row");if(s){ASSERT_CONTAINS(s,"x\\u0000y","raw varchar is byte-exact");free(s);}
    uint64_t pos=b_pos(&f);const char *bad="a||bad|5|6\n";
    ASSERT_EQ_INT(b_call(&f,bad,strlen(bad),1,0,&out),1,"empty numeric rejects, not zero");
    if(out){ASSERT_CONTAINS(out,"\"errors\":1","delimited encode is error");free(out);}ASSERT_TRUE(b_pos(&f)==pos,"invalid delimiter row no append");
    b_close(&f);
}
static void b_guards(void){
    BFixture f;if(b_open(&f,0)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{}}",0,&out),0,"guard seed");free(out);
    f.sc.auto_key=AK_UUID;b_reject(&f,"{\"a\":{}}");f.sc.auto_key=AK_NONE;
    char path[256];snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata directory");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private indexes directory");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private manifest");
    if(fp){ASSERT_TRUE(fputs("f0\n",fp)>=0,"manifest content");ASSERT_EQ_INT(fclose(fp),0,"manifest close");}
    invalidate_idx_cache(f.root,"obj");b_reject(&f,"{\"a\":{}}");remove(path);invalidate_idx_cache(f.root,"obj");
    f.ts->fields[1].not_null=1;b_reject(&f,"{\"a\":{\"f1\":null}}");f.ts->fields[1].not_null=0;
    SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};const uint8_t legacy[]={1,0};
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"legacy",6,legacy,sizeof(legacy),&o,NULL),0,"legacy generation seed");
    uint64_t pos=b_pos(&f);ASSERT_EQ_INT(b_json(&f,"{\"legacy\":{}}",0,&out),1,"reject legacy OLD");free(out);ASSERT_TRUE(b_pos(&f)==pos,"generation reject no append");
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"guard seed survives");free(s);b_close(&f);
}
static void b_maximum(void){
    BFixture f;if(b_open(&f,1)){b_close(&f);return;}char *out=NULL;size_t n=0;
    ASSERT_TRUE(f.sc.slot_size>1000000000,"one GiB theoretical schema");
    ASSERT_EQ_INT(b_json(&f,"{\"a\":{\"f127\":\"small\"},\"b\":{\"f128\":\"tiny\"}}",0,&out),0,"large schema small batch");free(out);
    char *s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"small packed maximum-schema row");
    ASSERT_TRUE(n<4096,"actual size independent of schema maximum");free(s);
    uint64_t pos=b_pos(&f),total=0,deleted=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&total,&deleted),0,"key-only count snapshot");
    const char *key_only="a\norphan\r\nno-newline";
    ASSERT_EQ_INT(b_call(&f,key_only,strlen(key_only),1,0,&out),0,"key-only lines skipped");
    if(out){ASSERT_CONTAINS(out,"\"inserted\":0","key-only lines never insert");free(out);}
    s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"key-only OLD still readable");
    if(s){ASSERT_CONTAINS(s,"small","key-only line cannot replace OLD");free(s);}
    void *absent=NULL;size_t absent_n=0;
    ASSERT_TRUE(slotcask_get(&f.db,"orphan",6,&absent,&absent_n)!=0,"key-only line cannot create row");free(absent);
    ASSERT_TRUE(b_pos(&f)==pos,"key-only lines never reserve segment bytes");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f.db,&t2,&d2),0,"key-only counts after");
    ASSERT_TRUE(total==t2&&deleted==d2,"key-only lines preserve counts");
    const char *explicit_empty="empty|\n";
    ASSERT_EQ_INT(b_call(&f,explicit_empty,strlen(explicit_empty),1,0,&out),0,"explicit empty column still writes");free(out);
    s=b_get(&f,"empty",&n);ASSERT_NOT_NULL(s,"explicit empty row present");
    if(s){ASSERT_CONTAINS(s,"\"f0\":\"\"","explicit empty varchar present");free(s);}
    b_close(&f);
    if(b_open(&f,0)){b_close(&f);return;}
    size_t len=4u*1024*1024;char *json=malloc(len+32);ASSERT_NOT_NULL(json,"4 MiB request");
    if(json){memcpy(json,"{\"a\":{\"f1\":\"",12);memset(json+12,'x',len);memcpy(json+12+len,"\"}}",4);
        ASSERT_EQ_INT(b_json(&f,json,0,&out),0,"maximum varchar bulk insert");free(out);
        s=b_get(&f,"a",&n);ASSERT_NOT_NULL(s,"maximum varchar physical value");ASSERT_TRUE(n>len&&n<len+256,"actual maximum payload");free(s);free(json);}
    b_close(&f);
}
static int test_typed_bulk_insert_run(void){b_routes();b_delimited();b_guards();b_maximum();return t_ctx->failed?1:0;}
TEST_REGISTER("test-typed-bulk-insert",test_typed_bulk_insert_run)
```

### build-source-1-old

<!-- block:build-source-1-old -->
```c
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### build-source-1-new

<!-- block:build-source-1-new -->
```c
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### build-source-2-old

<!-- block:build-source-2-old -->
```c
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c \
```

### build-source-2-new

<!-- block:build-source-2-new -->
```c
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c \
```

### build-source-3-old

<!-- block:build-source-3-old -->
```c
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### build-source-3-new

<!-- block:build-source-3-new -->
```c
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### build-source-4-old

<!-- block:build-source-4-old -->
```c
    src/db/typed_row.c \
```

### build-source-4-new

<!-- block:build-source-4-new -->
```c
    src/db/typed_row.c src/db/typed_bulk_insert.c \
```

### build-source-5-old

<!-- block:build-source-5-old -->
```c
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c \
```

### build-source-5-new

<!-- block:build-source-5-new -->
```c
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c \
```

### build-register-old

<!-- block:build-register-old -->
```c
    src/test/cases/test_single_write_integration.c \
```

### build-register-new

<!-- block:build-register-new -->
```c
    src/test/cases/test_single_write_integration.c \
    src/test/cases/test_typed_bulk_insert.c \
```

### bulk-implementation-old

<!-- block:bulk-implementation-old -->
```c
#include "typed_bulk_insert.h"
#include <errno.h>
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists){
    (void)root;(void)object;(void)sc;(void)ts;(void)borrowed;(void)data;
    (void)len;(void)format;(void)delimiter;(void)if_not_exists;
    errno=ENOTSUP;OUT("{\"error\":\"packed bulk insert unavailable\"}\n");return 1;
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

### row-declarations-old

<!-- block:row-declarations-old -->
```c
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
```

### row-declarations-new

<!-- block:row-declarations-new -->
```c
/* Validates one complete JSON value, including unknown nested fields. */
int typed_json_validate(const char *json);
/* Positional full replacement: no JSON defaults; preserve/stamp AUTO_CREATE
   under the same locked OLD contract as typed_insert_alloc. inputs has nfields
   entries. Unprovided cells are present zero/empty (removed cells stay NULL). */
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
```

### row-construction-old

<!-- block:row-construction-old -->
```c
/* Complete packed insertion against locked OLD. Ordinary defaults run once;
   auto_create is stamped only when fresh, or copied with OLD nullness.
   Public encode/default APIs retain their existing independent contract. */
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,root,object,&p,NULL,err,cap);
    if(!rc)rc=prepared_defaults_mode(&p,root,object,1,err,cap);
    if(!rc&&old){
        TypedRowView view;
        if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))
            rc=row_error(err,cap,EILSEQ,"invalid OLD record");
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;
            if(typed_row_span(&view,(size_t)i,&span)){
                rc=row_error(err,cap,EILSEQ,"invalid OLD field");break;}
            rc=replace_cell(&p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
/* Packed scalar JSON patch. Known object/array values, including computed
   expressions, fail before composition; legacy computed semantics stay in
   storage.c until Phase 4 supplies their exact-decimal replacement. */
```

### row-construction-new

<!-- block:row-construction-new -->
```c
/* Validate a complete JSON value before bulk record collection. */
int typed_json_validate(const char *json){
    if(!json)return row_error(NULL,0,EINVAL,"invalid JSON");
    const char *p=json;
    if(row_json_value(&p,0)||*row_json_space(p))
        return row_error(NULL,0,EINVAL,"invalid JSON");
    return 0;
}
static int prepared_insert_finish(TypedPrepared *p,const SlotcaskOldRecord *old,
    uint8_t **out,size_t *len,char *err,size_t cap){
    const TypedSchema *ts=p->schema;int rc=0;
    if(old){
        TypedRowView view;
        if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))
            rc=row_error(err,cap,EILSEQ,"invalid OLD record");
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;
            if(typed_row_span(&view,(size_t)i,&span)){
                rc=row_error(err,cap,EILSEQ,"invalid OLD field");break;}
            rc=replace_cell(p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else rc=typed_prepared_stamp(p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(p,out,len,err,cap);
    return rc;
}
/* Complete packed insertion against locked OLD. Ordinary defaults run once;
   auto_create is stamped only when fresh, or copied with OLD nullness.
   Public encode/default APIs retain their existing independent contract. */
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,root,object,&p,NULL,err,cap);
    if(!rc)rc=prepared_defaults_mode(&p,root,object,1,err,cap);
    if(!rc)rc=prepared_insert_finish(&p,old,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!inputs)return row_error(err,cap,EINVAL,"invalid packed inputs");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=typed_prepared_zero(ts,&p,err,cap);
    for(int i=0;!rc&&i<ts->nfields;i++)
        if(!ts->fields[i].removed&&inputs[i].supplied)
            rc=typed_prepared_set(&p,(size_t)i,&inputs[i],err,cap);
    if(!rc)rc=prepared_insert_finish(&p,old,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

/* Packed scalar JSON patch. Known object/array values, including computed
   expressions, fail before composition; legacy computed semantics stay in
   storage.c until Phase 4 supplies their exact-decimal replacement. */
```

### bulk-include-old

<!-- block:bulk-include-old -->
```c
#include "types.h"
```

### bulk-include-new

<!-- block:bulk-include-new -->
```c
#include "types.h"
#include "typed_bulk_insert.h"
```

### bulk-json-route-old

<!-- block:bulk-json-route-old -->
```c
        return 1;
    }

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
```

### bulk-json-route-new

<!-- block:bulk-json-route-new -->
```c
        return 1;
    }
    if(ts->packed.nfields)
        return typed_bulk_insert_command(db_root,object,&sc,ts,NULL,data,len,0,0,if_not_exists);

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
```

### bulk-delimited-route-old

<!-- block:bulk-delimited-route-old -->
```c
    Schema sc = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
```

### bulk-delimited-route-new

<!-- block:bulk-delimited-route-new -->
```c
    Schema sc = load_schema(db_root, object);
    if(ts->packed.nfields)
        return typed_bulk_insert_command(db_root,object,&sc,ts,NULL,data,size,1,delimiter,if_not_exists);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
```

### legacy-collect-oom-old

<!-- block:legacy-collect-oom-old -->
```c
    if (np == 0) return NULL;
    SlotcaskBulkShardInput *inputs = calloc(np, sizeof(*inputs));
```

### legacy-collect-oom-new

<!-- block:legacy-collect-oom-new -->
```c
    *ninputs = np; /* Preserve the requested count on allocation failure. */
    if (np == 0) return NULL;
    SlotcaskBulkShardInput *inputs = calloc(np, sizeof(*inputs));
```

### bulk-doc

<!-- block:bulk-doc -->
```markdown
# Packed bulk insert adapter (inactive schema format)

`typed_bulk_insert_command` is an internal synchronous adapter used by both
JSON bulk-insert runners (dict/array, inline/file) and positional delimited
runners (inline/file) when an explicitly bound packed schema is present.
Public loaders and object creation still produce legacy schemas. This document
records private integration, not public availability or migration completion.

Rows carry source spans and actual composed bytes rather than `total_size`
arenas. Keys/JSON values are copied with checked allocations; raw delimited
spans borrow the input through the complete deferred request. A schema with
256 bare varchar fields can have a theoretical slot bound above 1 GiB, while
small rows still consume roughly two KiB. Bare varchar remains 4 MiB; decimal
limits and assignment rules come from the shared decimal/packed core.

The adapter validates complete JSON before any write. It handles JSON dict and
array forms, decoded keys, escaped/embedded-NUL varchar values, NULL, duplicates
inside field objects via the shared row collector, and ignored fields only
after syntax validation. Keys must be nonempty and fit max_key. Non-object or
missing record values are per-row errors. Duplicate record keys keep input
order within each shard and retain the existing coordinator's outcomes rather
than adding last-wins semantics: two fresh duplicate keys in the measured
single window insert the first and report the later row as an error. A unified
record-duplicate policy across windows remains a Phase 4 activation contract.

Delimited input uses bounded spans and active schema field order, treats CR/LF
as line boundaries, pads missing columns with empty spans, ignores surplus
columns, and does not add CSV quoting/escaping. Lines with no separator are
skipped; an explicit empty column remains a real value. Empty varchar is present empty;
empty numeric is invalid; empty fixed scalars retain present zero. Removed
fields consume no input column. Unlike JSON, positional import does not run
JSON defaults or AUTO_UPDATE stamping. AUTO_CREATE is preserved from locked
OLD or stamped for a fresh row, through the same finalizer as JSON insertion.

Records are grouped by kf shard with one input per shard and one synchronous
`slotcask_bulk_request_execute` per command. `value_compute` runs through the
existing M-phase locked-OLD path with `value_rewrites_payload=1`; NEW never
gets staged from placeholder/source bytes. The callback validates OLD's full
generation, constructs an owning actual-size row, and checks the real key,
record length, schema slot bound and unchanged segment ceiling before returning
it. The caller retains each allocated NEW through the complete request and
frees it once after all pipelines return. Existing deferred durability,
markers/replay and per-shard gates remain authoritative; no new commit protocol
or metadata publication is introduced.

The bulk engine represents a nonzero value callback result as a condition skip.
This adapter keeps a row-local construction-error side channel, so syntax,
range, generation, allocation and size failures count as `errors`, while an
existing key under `if_not_exists` counts as `skipped` and does not execute the
constructor/defaults. `inserted` counts only fresh rows. Mixed valid/invalid
rows retain best-effort semantics; the batch is not a transaction. Coordinator
failures cannot become success, and pending durability is an explicit error
that must not be retried blindly. Counts remain in keyfile headers.

The shared legacy collector now reports its requested input count even if its
allocation fails. Both existing callers can therefore mark prepared workers
failed instead of folding an unexecuted batch as successful.

Allocation scales with actual source and composed values. This child does not
introduce a new command-wide memory-budget contract: bulk-insert is not one of
the existing QUERY_BUFFER_MB collections. Ordinary TCP ingress keeps its
existing request limit; embedded commands and file imports retain their
existing entry-point policies. Defaults and input/output expansion, command-wide
memory budgeting, auto-generated keys and packed indexed writes remain Phase 4/5
activation work. The 8 MiB allocation ceiling in the scratch probe is an
instrumented assertion for small maximum-schema rows, not a new public limit.

Packed auto-key objects and every packed secondary-index declaration currently
fail closed before defaults or mutation. Private adapter tests exercise actual
storage and request execution. Public schema activation, public end-to-end
packed commands, bulk-update/delete, readers, schema rewrites and migration
are not discharged by these fixtures. Existing public legacy tests remain
mandatory compatibility coverage.
```

### row-hook-doc-old

<!-- block:row-hook-doc-old -->
```markdown
The [owning write hook](owning-write-hooks.md) supplies a transport-level
actual-size NEW buffer lifetime for future integrations. It is optional and
has no production caller yet. Its presence does not convert public commands,
bulk row ownership, return_record, indexes or criteria to packed storage.
```

### row-hook-doc-new

<!-- block:row-hook-doc-new -->
```markdown
The [owning write hook](owning-write-hooks.md) supplies the actual-size NEW
buffer lifetime used by single-command integration. Bulk insertion uses its
separate caller-owned deferred-request contract; it does not pass bulk buffers
to the single-record owning hook. Neither path activates packed metadata.
```

### row-bulk-doc-old

<!-- block:row-bulk-doc-old -->
```markdown
actual-size owning updates. Bulk/read/index consumers remain future work;
packed schemas remain inactive in public loaders and creates.
```

### row-bulk-doc-new

<!-- block:row-bulk-doc-new -->
```markdown
actual-size owning updates. [Bulk insert integration](packed-bulk-insert.md)
adds JSON and positional insert/upsert preparation under explicit schemas.
Bulk-update/delete and read/index consumers remain future work; packed schemas
remain inactive in public loaders and creates.
```

### owning-doc-old

<!-- block:owning-doc-old -->
```markdown
There is no production constructor caller yet. This hook does not activate
packed schemas, change persisted formats, migrate objects, or complete single
and bulk command conversion. Explicit-schema tests exercise real physical
updates; public legacy tests remain the compatibility gate. Later write/read,
index, metadata and activation plans retain their own obligations.
```

### owning-doc-new

<!-- block:owning-doc-new -->
```markdown
Single-command insert/upsert/update now uses the constructor under the
[write integration](single-write-integration.md). [Packed bulk insertion](packed-bulk-insert.md)
uses a separate caller-owned value callback: its buffers survive the entire
deferred bulk request and are freed by that adapter, not by this owning hook.
Both integrations remain private while packed schemas are inactive. Public
legacy tests remain the compatibility gate; later read/index, metadata,
migration and activation plans retain their obligations.
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
    if git('rev-parse','HEAD')!=m['base_head']:raise ValueError('HEAD differs from reviewed 3B.3 commit')
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
    message='Phase 3B.4 execution halted: '+str(e)+'\nNo later task may run. Planner/human must review this note before resuming.\n'
    pathlib.Path('PLAN_NOTES.md').open('a').write(message)
    print(message,file=sys.stderr);sys.exit(1)
```

### checks

<!-- block:checks -->
```python
#!/usr/bin/env python3
import argparse,pathlib,re,subprocess,tempfile

def extract(plan,key):
    pattern=r'(?ms)^<!-- block:'+re.escape(key)+r' -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)'
    match=re.search(pattern,plan.read_text());assert match,key
    return match.group(1)+'\n'
def run(args,expected=0):
    print('+',' '.join(args),flush=True)
    p=subprocess.run(['rtk','proxy',*args],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    print(p.stdout,end='',flush=True)
    if p.returncode!=expected:raise RuntimeError('exit '+str(p.returncode)+' expected '+str(expected))
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--plan',type=pathlib.Path,required=True)
    ap.add_argument('--mode',choices=['red-collect','green'],required=True);a=ap.parse_args()
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b4-check-'));print('scratch:',work,flush=True)
    collect=work/'collect.c';collect.write_text(extract(a.plan,'collect-probe'))
    source=pathlib.Path('src/db/query_bulk.c').resolve();binary=work/'collect'
    run(['gcc','-O1','-ffunction-sections','-fdata-sections',
        '-DP4_QUERY_SOURCE="'+str(source)+'"','-Isrc/db',str(collect),
        'build/bin/libshard-db.a','-Wl,--gc-sections','-lpthread','-lssl','-lcrypto','-o',str(binary)])
    run([str(binary)],1 if a.mode=='red-collect' else 0)
    if a.mode=='red-collect':return
    objects=[]
    for name in ['typed_bulk_insert','typed_row','packed_record','decnum']:
        obj=work/(name+'.o');objects.append(str(obj))
        run(['gcc','-O1','-g','-Isrc/db','-Dmalloc=p4_malloc','-Dcalloc=p4_calloc',
            '-Drealloc=p4_realloc','-Dfree=p4_free','-c','src/db/'+name+'.c','-o',str(obj)])
    probe=work/'oom.c';probe.write_text(extract(a.plan,'oom-probe'));binary=work/'oom'
    run(['gcc','-O1','-g','-Isrc/db',str(probe),*objects,'build/bin/libshard-db.a',
        '-lpthread','-lssl','-lcrypto','-o',str(binary)])
    run([str(binary)])
    print('all verification probes passed')
if __name__=='__main__':main()
```

### collect-probe

<!-- block:collect-probe -->
```c
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
static void *p4_collect_calloc(size_t n,size_t s){(void)n;(void)s;errno=ENOMEM;return NULL;}
#define calloc p4_collect_calloc
#include P4_QUERY_SOURCE
#undef calloc
int main(void){
    BulkInsShardWork workers[2]={0};workers[0].prepared=workers[1].prepared=1;
    size_t n=999;SlotcaskBulkShardInput *p=bulk_ins_collect_inputs(workers,2,&n);
    printf("collect failure: pointer=%p requested=%zu (expected NULL/2)\n",(void *)p,n);
    return p||n!=2;
}
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
static int invoke(const char *root,const Schema *sc,TypedSchema *ts,SlotcaskDb *db,int format,int fresh,size_t fail){
    const char *json=fresh?"{\"b\":{\"f0\":1.25,\"f1\":\"NEW\"}}":"{\"a\":{\"f0\":1.25,\"f1\":\"NEW\"}}";
    const char *delim=fresh?"b|1.25|NEW":"a|1.25|NEW";const char *data=format?delim:json;
    char *text=NULL;size_t n=0;FILE *fp=open_memstream(&text,&n);MUST(fp);FILE *save=g_out;g_out=fp;
    calls=0;nth=fail;armed=1;int rc=typed_bulk_insert_command(root,"obj",sc,ts,db,data,strlen(data),format,'|',0);armed=0;
    g_out=save;MUST(!fclose(fp));MUST(live==0);free(text);return rc;
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
    for(int format=0;format<2;format++)for(int fresh=0;fresh<2;fresh++){
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
        printf("format %d fresh %d: %zu allocation failures preserve OLD/absence, counts, position, live=0\n",format,fresh,count);
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
    MUST(!invoke(root,&sc,ts,&db,0,1,0));
    void *small=NULL;size_t small_len=0;uint8_t small_hdr=0;
    MUST(!slotcask_get_ex(&db,"b",1,&small,&small_len,&small_hdr));
    MUST(small_len<4096&&small_hdr==PKR_FLAGS);free(small);
    printf("maximum schema actual value %zu; largest selected allocation %zu (<8 MiB), live=0\n",small_len,largest);
    free(ts);slotcask_close(&db);invalidate_idx_cache(root,"obj");shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return 0;
}
```

### manifest

<!-- block:manifest -->
```json
{
  "ancestor": "82e5d4053431b0b163a621712772d20727d4ff28",
  "base_head": "a97f9f21caa0afcd29ae54ec665abf79466783ec",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "parent_sources": {
    "build.sh": "a340db74277ce62346a34fbbc260c531b2c25f305aeb8746b26cab50194bdcf4",
    "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
    "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
    "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
    "src/db/typed_row.c": "fd9c7d70c2bff55a99176af45081ab926a29219e40cb86877604fa0adbc710da",
    "src/db/typed_row.h": "ffe36c203d85461750c8a20523d2acfbf245c1583673b6373474a72f145ba39a",
    "src/db/single_write_test.h": "9788c60b2e4336f90fe28c52656470c8c1e8f21a1b5ed9a4d8ad67aae2fa895c",
    "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
  },
  "blocks": {
    "bulk-header": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
    "bulk-stub": "05aef8af2d54fea8e8b3b7b392fd761d5cb4f6a97c6809d3ce01898cf4ed4763",
    "bulk-test": "2d044445cd86e51d62ae5a311f68e7f9d077c22a7aac2a75695425088787b304",
    "build-source-1-old": "a21cdeb4c3b0560b6cc2117e61c3c7f6feac928a0c800c285db679be1fde716f",
    "build-source-1-new": "8e08079f67b5d78405c9bb24b4db32dc1d7e1eeade070fdd7465a63c259e3a11",
    "build-source-2-old": "83e0ad9e25ada9f208ccc03439345fc01ba8a2dd10a4e4d3549768f3d39d281d",
    "build-source-2-new": "1638f6432c997e2988c4aaba3a691396a935e4d2bda850a7abb6de723eb295ad",
    "build-source-3-old": "e4cf4644cdc09d822d038648db54efaa57bfb63041d28ff6c4eaba83b85cb72b",
    "build-source-3-new": "a8b3faf46261da7c018ca1f35f9c3af5ccd8782049d761f7631df9555ace52f1",
    "build-source-4-old": "aa3bebcbcaac166f2f3df3847409777138dc48ce3866d1cf39d0f0c7595f5fd1",
    "build-source-4-new": "468cc91f3111c744be753b663a2254a2063afa9b7e5ea417170680df8775cb1f",
    "build-source-5-old": "c881d0915d5569646fd7b2492e8f484f3f1933f9099f167042afbd4e19c0c4bb",
    "build-source-5-new": "d1d8b7da14dac768fc2bd3b2cdd7d4f9baca8f5d0f7e41968a9ad8a4350871de",
    "build-register-old": "05a69a6aa6d4b3aa9b83a931073294f26a521ae2c2642296fb650191c58958b6",
    "build-register-new": "24473d44ff7e211f21a213401ed51bb906a5601bb1bfc4de27693c5cfdfef0a6",
    "bulk-implementation-old": "05aef8af2d54fea8e8b3b7b392fd761d5cb4f6a97c6809d3ce01898cf4ed4763",
    "bulk-implementation-new": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
    "row-declarations-old": "e238b19aa928e91bf50000b6a8614bd1e9bac720471b8a080297d11b67f3d05d",
    "row-declarations-new": "fc97b876c2acb9d68ffdf0062f19965ccd5f27bbe5ba65fcde4fa136cc04dc4b",
    "row-construction-old": "7fbf2388d9c7f54986831c8e8132ef03ad46720fc7049cb2057700110f88605f",
    "row-construction-new": "80744128cdc2a9947013307556a948b7f158ff805f7cf75fcb80f9333d5be692",
    "bulk-include-old": "3d21f4d067557f52fc3ca8eef5a61423099294e50836ffd2380757bc859ad31b",
    "bulk-include-new": "26f7a5d54ddf5a5f800b457c150a4daf839bd49860bafe3d3564af1e37fc0fa8",
    "bulk-json-route-old": "983e458ec5b9c035efb0d5d5823173700c9a0d6665a0e11c4ff81c16b91e5e9e",
    "bulk-json-route-new": "5fe89dad78348cb6dea8fb0032e33bb1ee0711e05a5f04033d6bc85f7f09ec27",
    "bulk-delimited-route-old": "d3e37e9835b6e626382e3d2aed4f39bb50b7d5ca97b31d73414fb42340db12ba",
    "bulk-delimited-route-new": "58b14c75555ffd619fdc781c17e6e1ad1b15d8f3fe09972a2775977fc9ade4bd",
    "legacy-collect-oom-old": "71079208431a34bc29a71437158582b1658fd76aa8862381d8182f624199cd82",
    "legacy-collect-oom-new": "34092b453b447ec23612eb15b10027287261840261dee885018a99ad211435d1",
    "bulk-doc": "9c35f7d94275d92471a1eda6cff1759a77e43b2738ea34d55d3d5e9abed14bff",
    "row-hook-doc-old": "f39b76f6954448e27ae4895c2ac533844fbb9c939264f99fffd141c49169bac6",
    "row-hook-doc-new": "c6778e19e386938f79675de8209b294397bbbf983cfb22438c61ae853a178cf9",
    "row-bulk-doc-old": "6f4039c57654547815782fb5dfe72bc8fe759369d0edbd3635986f704bde65ce",
    "row-bulk-doc-new": "6f90b2bdd17050f6aa20ab843c585ee9f74db305d9fff3adbf838841fc8aba91",
    "owning-doc-old": "3fad7df260e050db01cf489375d77346df97bcbd664d1e00120ee918ff95b27d",
    "owning-doc-new": "6571b4e600b434a0d5226b006a6f15acd2dab077991affa2c0b23e5748f40265",
    "driver": "2200849f26bf190b210e5191e5f48e1bbff0504e9e2eba5a9fe8c2eef0ef70a1",
    "checks": "67f67957f88a155f6e62c81e02112aada96f3cb3f0539bf0df09b78fb4bed4f1",
    "collect-probe": "386227d89b3cebedbb4e0495b31a9a9692a8d207a2c5fe291187a120a530a2c7",
    "oom-probe": "12e460f6b18302317d044d72fffba1fff9239ae9422fb3e9c114f4b57efaead7",
    "bootstrap": "e8c92245efde9a28df584dba7b572a2252e3bf47545db6872f13774b1424171b"
  },
  "edits": {
    "1": [
      {
        "path": "src/db/typed_bulk_insert.h",
        "old": null,
        "new": "bulk-header"
      },
      {
        "path": "src/db/typed_bulk_insert.c",
        "old": null,
        "new": "bulk-stub"
      },
      {
        "path": "src/test/cases/test_typed_bulk_insert.c",
        "old": null,
        "new": "bulk-test"
      },
      {
        "path": "build.sh",
        "old": "build-source-1-old",
        "new": "build-source-1-new"
      },
      {
        "path": "build.sh",
        "old": "build-source-2-old",
        "new": "build-source-2-new"
      },
      {
        "path": "build.sh",
        "old": "build-source-3-old",
        "new": "build-source-3-new"
      },
      {
        "path": "build.sh",
        "old": "build-source-4-old",
        "new": "build-source-4-new"
      },
      {
        "path": "build.sh",
        "old": "build-source-5-old",
        "new": "build-source-5-new"
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
        "path": "src/db/typed_row.h",
        "old": "row-declarations-old",
        "new": "row-declarations-new"
      },
      {
        "path": "src/db/typed_row.c",
        "old": "row-construction-old",
        "new": "row-construction-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "bulk-include-old",
        "new": "bulk-include-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "bulk-json-route-old",
        "new": "bulk-json-route-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "bulk-delimited-route-old",
        "new": "bulk-delimited-route-new"
      },
      {
        "path": "src/db/query_bulk.c",
        "old": "legacy-collect-oom-old",
        "new": "legacy-collect-oom-new"
      }
    ],
    "3": [
      {
        "path": "docs/concepts/packed-bulk-insert.md",
        "old": null,
        "new": "bulk-doc"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "row-hook-doc-old",
        "new": "row-hook-doc-new"
      },
      {
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "row-bulk-doc-old",
        "new": "row-bulk-doc-new"
      },
      {
        "path": "docs/concepts/owning-write-hooks.md",
        "old": "owning-doc-old",
        "new": "owning-doc-new"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "build.sh": "a340db74277ce62346a34fbbc260c531b2c25f305aeb8746b26cab50194bdcf4",
      "docs/concepts/owning-write-hooks.md": "3f3a54555d926d5b9ac5355efba522d601e5e204b0ffd2d1e6e0d65d0d9c3816",
      "docs/concepts/packed-bulk-insert.md": null,
      "docs/concepts/typed-row-adapter.md": "3bd6c2f685a616ff09b31cc7c9b4d7f4c9da2c446aec4aaf2bb498ee22f8f9c9",
      "src/db/query_bulk.c": "79bacb4944546fb2e48c7dac15254e9b02a4516d76625c7f60b84799baffec41",
      "src/db/typed_bulk_insert.c": null,
      "src/db/typed_bulk_insert.h": null,
      "src/db/typed_row.c": "fd9c7d70c2bff55a99176af45081ab926a29219e40cb86877604fa0adbc710da",
      "src/db/typed_row.h": "ffe36c203d85461750c8a20523d2acfbf245c1583673b6373474a72f145ba39a",
      "src/test/cases/test_typed_bulk_insert.c": null
    },
    "1": {
      "build.sh": "867acec0e443ed44f57111644b4bc8e1df6af5759d332de21c96a588bf18ab87",
      "docs/concepts/owning-write-hooks.md": "3f3a54555d926d5b9ac5355efba522d601e5e204b0ffd2d1e6e0d65d0d9c3816",
      "docs/concepts/packed-bulk-insert.md": null,
      "docs/concepts/typed-row-adapter.md": "3bd6c2f685a616ff09b31cc7c9b4d7f4c9da2c446aec4aaf2bb498ee22f8f9c9",
      "src/db/query_bulk.c": "79bacb4944546fb2e48c7dac15254e9b02a4516d76625c7f60b84799baffec41",
      "src/db/typed_bulk_insert.c": "05aef8af2d54fea8e8b3b7b392fd761d5cb4f6a97c6809d3ce01898cf4ed4763",
      "src/db/typed_bulk_insert.h": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
      "src/db/typed_row.c": "fd9c7d70c2bff55a99176af45081ab926a29219e40cb86877604fa0adbc710da",
      "src/db/typed_row.h": "ffe36c203d85461750c8a20523d2acfbf245c1583673b6373474a72f145ba39a",
      "src/test/cases/test_typed_bulk_insert.c": "2d044445cd86e51d62ae5a311f68e7f9d077c22a7aac2a75695425088787b304"
    },
    "2": {
      "build.sh": "867acec0e443ed44f57111644b4bc8e1df6af5759d332de21c96a588bf18ab87",
      "docs/concepts/owning-write-hooks.md": "3f3a54555d926d5b9ac5355efba522d601e5e204b0ffd2d1e6e0d65d0d9c3816",
      "docs/concepts/packed-bulk-insert.md": null,
      "docs/concepts/typed-row-adapter.md": "3bd6c2f685a616ff09b31cc7c9b4d7f4c9da2c446aec4aaf2bb498ee22f8f9c9",
      "src/db/query_bulk.c": "e3d8b998b3b909a474bbc681941a66d95fb0c4206780b6b19a4e5eb39cc0d1a5",
      "src/db/typed_bulk_insert.c": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
      "src/db/typed_bulk_insert.h": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
      "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
      "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
      "src/test/cases/test_typed_bulk_insert.c": "2d044445cd86e51d62ae5a311f68e7f9d077c22a7aac2a75695425088787b304"
    },
    "3": {
      "build.sh": "867acec0e443ed44f57111644b4bc8e1df6af5759d332de21c96a588bf18ab87",
      "docs/concepts/owning-write-hooks.md": "faea4d7661ee2579ee07092a048309ac2ed66c21333ac7a3992c6151b109c9c5",
      "docs/concepts/packed-bulk-insert.md": "9c35f7d94275d92471a1eda6cff1759a77e43b2738ea34d55d3d5e9abed14bff",
      "docs/concepts/typed-row-adapter.md": "c01b0e2110e4dbad7cd5ebfddf97160d2b40ed9d2baa7775ebe4edfd670e6ad0",
      "src/db/query_bulk.c": "e3d8b998b3b909a474bbc681941a66d95fb0c4206780b6b19a4e5eb39cc0d1a5",
      "src/db/typed_bulk_insert.c": "ae221896da480813b11c30dd84fab555319c14ed9d4f22105c6f4d10598dd928",
      "src/db/typed_bulk_insert.h": "8042b04b16d27dc55ee40e9d7d8c30f059354841b8dffde46a6e22a6082f1080",
      "src/db/typed_row.c": "333c666fa131e3aa10090b733f91ff3ef53ade84cf8a803dc52dc701b9ba3fb6",
      "src/db/typed_row.h": "3dc865bb8921eeb79f0f0c0b2ceccc769e42e0c1f32ed3793eef6fd730ea5941",
      "src/test/cases/test_typed_bulk_insert.c": "2d044445cd86e51d62ae5a311f68e7f9d077c22a7aac2a75695425088787b304"
    }
  }
}
```
