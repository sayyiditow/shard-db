# Phase 3B.10 — checked packed CSV fetch and multi-key GET

## Primary goal

Return exact finance decimals and complete binary strings through CSV fetch and
multi-key GET, with explicit failure instead of silently skipped records, empty
replacement cells or a successful-looking partial response.

**Status: blind and plan-aware review complete; ready for executor handoff.** Planner: GPT 6.1 Sol.
Intended executor: GLM 5.3. Base is reviewed/committed Phase 3B.9
`174f7d8e99bee4a619a23aed3dfa07a3cbb05bcf` on
`fix/phase1-safe-scalar-conversions`. No root production implementation is
authorized by this planning document. Complete source/hunks and measured
ordinary scratch verification are below.

## Proposed scope and decisions for approval

Connect the checked CSV line renderers to two complete command paths: fetch and
multi-key GET. Add one private packed command module and packed-only early
dispatch branches. Existing legacy/non-CSV branches remain byte-identical;
CSV get-multi performs one additional typed-schema cache lookup to select the
inactive packed branch. No new mode/flag, schema loading policy, format version,
migration or public activation is introduced.

| Decision | Proposed contract |
|---|---|
| Stable read interval | Caller holds shared object lock and pins matching database/schema. Backend reserves every existing writer gate for that object while collecting; never upgrades the object lock. Release before FILE I/O. No nested reservation. |
| Storage reads | Reuse slotcask_walk_live_strict and slotcask_guard_lookup under their documented reservation precondition. An unreadable/invalid requested reference aborts; never convert it to absence. |
| Publication | Prepare header and every selected row before writing anything. A late construction/read failure discards all prepared CSV and emits one JSON error. No fallback to partial rows. |
| I/O | Check fwrite, ferror and fflush. A write failure returns -1 without appending replacement JSON to an accepted CSV prefix. No transactional transport claim. |
| Response quota | Sum(sizeof(CsvLine)+line_length+1) across retained header/rows must fit options.buffer_budget; production samples QUERY_BUFFER_MB. No realloc/copy of the entire accumulated response. |
| Memory exclusions | Borrowed request input, caches, fixed command frames, reservation bookkeeping and transient decoded-key/renderer/decimal workspaces are outside that owned-response quota. This is not the full activation-time peak-RAM policy. |
| Fetch | NULL or empty cursor means zero; otherwise a strict nonnegative decimal cursor plus positive offset must fit int64. Leading zeros accepted. Negative offset remains zero. Effective limit must be positive after existing default resolution. Projection/composites and byte escapes reuse 3B.9; total+CSV rejects. Empty object returns only a header. |
| Multi-get | Valid JSON array of strings, decoded with explicit lengths. Preserve order/duplicates, one row per key; missing keys produce empty value cells. Empty array returns only a header. No JSON-response reparse or new projection support. |
| Keys | Plain binary keys preserve NUL; UUID/sequence use existing storage-key parsers, with text bounds separate from binary widths. Malformed auto-keys reject. Multi-get echoes decoded requested text; fetch renders canonical stored wire form. |
| Deadlines | Sample during collection and after traversal. Existing reservation acquisition and stdio writes block; no cancellation guarantee while waiting on gates/client I/O. |

Reserving all writer gates deliberately serializes packed CSV collection against
writes. This uses an already-tested lock order and a fail-closed reader rather
than the legacy best-effort skip walker. Skipped fetch offsets still read backing
references; no O(limit), lock-wait or throughput improvement is claimed. Gates
are not held while sending to a slow client. Snapshot/cursor stability across
separate requests is not promised.

GET missing-row and empty-array behavior follows the documented CSV table shape;
legacy CSV currently skips missing rows and returns {} for empty input. Only the
private packed branch changes. Packed keys are decoded exactly once, so escaped
UUID text is not incorrectly bounded by its 16-byte storage width. Normal public
legacy behavior remains available throughout the roadmap.

Find, joins, aggregates, keys/exists, import decoding, other read/delete/index/
maintenance/rebuild consumers and end-to-end request-memory accounting remain
open. This is real command integration, not another standalone cell helper;
it does not claim every CSV/query consumer or Phase 3B is complete.

## Task 0 — documentation discovery and checkpoint

**How this serves the primary goal:** verifies the actual read/lock/output
contracts before selecting a safe command boundary.

Read AGENTS.md/CORE-PROCESS.md, the Phase 3 work index and parent roadmap,
reviewed 3B.9 plan/evidence/reviews, concepts/packed-csv-rendering.md,
typed-row-adapter.md, storage-model.md, concurrency.md, and query-protocol
overview.md/find.md/joins.md. The protected certification plan is hash-only:
never read/copy/edit/move/stage/incorporate/execute it.

Read-only documentation-discovery delegation was unavailable; the planner
performed the actual source/doc checks directly. No unverified agent conclusion
was used. Allowed APIs and source patterns:

| Source / symbol | Verified contract and use |
|---|---|
| typed_row.h/.c checked CSV APIs | Owned explicit-length complete cells/headers/rows, exact decimal text, visible NUL/backslash escaping, full schema/view validation and output budgets. Reuse unchanged. |
| slotcask.h/.c SlotcaskWriteReservation | Zero-init, same-thread begin/end, shared object lock and pinned same database, no nesting. Existing begin releases partial acquisitions before waiting; end releases reservation before I/O. |
| slotcask_walk_live_strict | 0 complete, 1 callback stop, -1 unreadable/invalid reference; only legal under exclusive object lock or shared lock plus every writer gate. Do not use it with merely the shared lock. |
| slotcask_guard_lookup | Fail-closed hash lookup under the same reservation; callback compares actual storage key bytes to handle hash collisions. Its caller comment is updated, not implementation. |
| query_find.c cmd_fetch_v2 | Initializes FieldSchema, then formerly parsed/truncated selector and streamed header/rows. New packed branch precedes that legacy parsing. |
| storage.c cmd_get_multi | Formerly decoded JSON then reparsed it for CSV; strlen paths discard NUL suffixes and missing keys. New packed branch precedes legacy allocation/parallel work. |
| util.c JSON helpers | typed_json_validate for the entire value; json_skip/raw_string_end and json_unescape_string for exact string spans. Do not use C-string convenience decoding for binary keys. |
| types.h / config.c | format_wire_key, parse_uuid_string, parse_seq_key, resolve_timeout_ms, now_ms_coarse and existing query buffer setting. No invented overload or new runtime flag. |
| test_single_write_integration.c / test_owning_write_hooks.c | Direct physical Slotcask fixture, exact slot bound, record_hdr2=PKR_FLAGS, local temporary roots, normal upsert, close/shutdown/recursive cleanup. |
| server.c | Existing JSON and text read dispatch hold the shared object lock; no server/auth/framing edit is needed. Direct helper callers/tests must satisfy that lock/lifetime contract. |

Root causes: legacy CSV emitters use strlen/void status, stream before the
remaining response is proven, and may silently erase allocation failures.
The cheap fetch skip walker silently skips unreadable references. Multi-get
round-trips through JSON and loses binary lengths. Reusing it for packed CSV
would preserve those defects, even though the new cell renderer is correct.

Consumer census at this base:

- cmd_fetch_v2/its callback: this child adds only packed CSV dispatch. Legacy
  callback and all JSON/rows/dict branches remain unchanged.
- cmd_get_multi: this child adds only packed CSV dispatch. Legacy parsing,
  parallel worker/output code remains unchanged; no broad legacy OOM-hardening
  claim is made.
- csv_emit_row: query.c three paths, query_find.c fetch, query_join.c scan.
  csv_emit_header: query.c envelope and query_find.c fetch. These old void APIs
  are not globally changed; untouched find/join paths retain separate owners.
- Joined CSV builders and direct csv_emit_cell consumers in aggregate/keys/
  multi-get/exists remain in the remaining inventory. No descriptor may be
  routed through them after future public activation without conversion/guards.
- New backends have two production callers after this child, both guarded by
  CSV format and an explicitly bound packed schema. Loader/create-object binder
  census must remain definition/declaration only in production.
- Five source lists require the new module: daemon, static library, test server,
  test runner and benchmark binary. Build does not run benchmarks. Six build.sh
  anchors include all five lists and one test registration; each is unique.

Confirm exact branch/HEAD, approved plan SHA from handoff, staged diff empty,
and only the approved plan, work-index, review and protected files pre-existing.
The review document is docs/plans/2026-10-27-phase3b10-plan-review.md;
preserve it alongside the plan/index and hash-only protected file. Protected
SHA must equal fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
Preflight sockets before daemon tests; if sandbox denial is confirmed, obtain
normal execution permission rather than skip cases or reinterpret failures.

## Task 1 — tests and link stubs, mandatory RED

**How this serves the primary goal:** proves absent checked output/error behavior
and the existing command dispatch defects before integrating either backend.

Apply stage 1 only: new header, two ENOTSUP backend stubs, new permanent test and
all six quoted build edits. Four files change. The new permanent test uses real
physical records and holds the object read lock explicitly. No metadata loader
seam, mutable environment variable or fixed TCP port is added.

Build with zero warnings, then capture test-packed-csv-commands RED: 178 passed /
39 failed, exit39. Preserve actual semantic failures, not just counts. Setup,
some rejection/I/O assertions pass incidentally on stubs; do not count them as
proof of the unimplemented behavior.

Run checks.py --mode red. It compiles the actual source-extracted fetch and
multi-get frontends, with only local schema/registry mocks, against real storage
fixtures and the new stubs. Four ancillary gates pass and two dispatch-output
gates fail: six gates, two failures, internal exit1 handled by the wrapper.
The predicates inspect actual old-command output; they do not manufacture RED
from the backend warmup return. Allocation tracking is deliberately disabled in
RED because the absent backend cannot warm storage caches; it makes no baseline
leak claim. Full allocation sweeps belong to the warmed GREEN reader.

## Task 2 — complete command backends and packed-only dispatch

**How this serves the primary goal:** makes both complete commands preserve
bytes and propagate construction/read/output failure without success-shaped
truncation.

Apply stage 2: replace STUB with FULL, add the new header includes, then the
two exact early dispatch branches. Three files change. Legacy code outside
those includes/branches stays byte-identical. Full code, not implementation
sketches, is frozen below.

The new module owns response lines and their list nodes, samples the existing
buffer/deadline options, validates keys/cursors, takes one reservation, performs
strict reads and releases before output. No mutation/index operation is issued
inside its callback. All error paths free partial lines and unwind the existing
reservation. Scoped registry references remain owned by each outer command.

Build warning-free and run focused GREEN: 217/0. Coverage includes exact large
decimals, scale/trailing zeros, NUL versus literal escape text, composite
projection, custom delimiter, cursor/offset, late invalid input, quota rejection,
generation mismatch, binary keys, UUID/sequence storage/display and fully
escaped UUID text, missing/duplicate/empty key sets, both short-write and
buffered-flush failure, gate release before I/O and maximum-schema actual size.

Run published green checks: 388/0 gates, 71 selected allocation failures
(fetch24 + multi-get47), all explicit NULL-free response errors with errnoENOMEM,
zero selected live buffers, one scoped registry cleanup per entry point and
released writer reservations. Force storage/registry/deadline failure through
delegating scratch wrappers; no production seam. Maximum schema largest selected
allocation24/live0. Tests/probes use a one-shard real physical fixture; no
cross-platform, throughput or end-to-end public metadata acceptance claim.

Then run regression-proof in scratch only: reverse the new module plus both
frontend files to exact stage-1 hashes and reproduce six gates/two actual
dispatch-output failures. Reapply every hunk, verify exact stage-2 hashes and
rerun all 388 gates. Never revert checked-out sources or weaken assertions.

## Task 3 — docs, full suite, exact hand-back

**How this serves the primary goal:** states the client/error/memory/locking
contract and proves existing legacy applications still pass their full suite.

Apply stage 3: new command concept doc, two adjacent concept updates and the
strict lookup caller comment. Four files change. No physical storage code is
modified. Run a fresh normal full suite: expected 20820/0 across528 cases.
Repeat green checks and verify all ten final checkpoint hashes plus unchanged
boundaries. Counts are binding on this workstation; record actual output and
halt on mismatch rather than changing assertions or rerunning until green.

The probe instruments allocation calls in packed_csv_command, typed_row,
packed_record, decnum, util and slotcask. Cache/physical setup is warmed before
arming thread-local tracking. Stdio/request metadata and unrelated library
allocations are outside that metric. The source-extracted frontends execute
real backend/storage work; mocked metadata/registry does not prove public loader
or network activation. Forced strict-reader errors prove propagation, not a new
storage-engine corruption oracle. The existing engine's strict validators stay
unchanged and covered by the full suite.

Write curated evidence to docs/plans/2026-10-27-phase3b10-packed-csv-commands-evidence.md
with actual identity, all commands/outputs/exit codes, bounds, reference/gate
cleanup, limitations and ten final hashes. Raw logs, scripts, probes and binaries
stay under /tmp. Stop unstaged/uncommitted for plan-aware and fresh blind
raw-diff review. No sanitizers/benchmarks here: branch-final ASan+UBSan/TSan
gates remain immediately before an authorized push/PR per AGENTS.md.

## Execution rules and next boundary

1. Execute only after approval, on the exact named fix branch/base. Do not
   create/reset/switch a branch, stage, commit, push, PR, merge or start later
   work. Keeping this child private does not authorize packed activation.
2. Read the docs; execute Tasks 0–3 and Appendix A in order. All shell commands
   start with rtk. Use rtk proxy for byte-preserving raw logs.
3. On any anchor/hash/checkpoint mismatch, unexpected warning/failure or
   uncovered decision, write PLAN_NOTES.md with actual identity/output and
   partial/untouched state, then halt the whole run. No guessing, workaround,
   fallback, assertion weakening or unrelated task. Resume only on a repaired
   plan from the human/planner.
4. The edit driver validates all pre/postimages before writes; filesystem write
   failures can still leave partial source edits. Record and stop, never
   silently retry or claim transactional editing.
5. Preserve prior plans/reviews and the protected rig. Hash only; never read,
   copy, edit, move, stage, incorporate or execute its contents.
6. Normal full suite is mandatory. Benchmarks belong to the user; sanitizers
   remain at the branch-final pre-push gate, not between individual slices.

Following work is the remaining CSV/query emitter/error and 3B consumer audit:
find/joins/aggregate/keys/exists plus read/delete/index/maintenance/rebuild.
Exact criteria/index and aggregate semantics keep their Phase5/6 owners; fail
safely at unsupported private seams while preserving public legacy behavior.
3C metadata/publication, Phase4 import/write completion, full request-memory and
deadline policies, SDK/CSV decoding and explicit activation acceptance remain
required. No fixed number of additional child plans or premature 3B closure.

## Appendix A — exact execution order

First compare identity, root status, empty staged diff, approved plan SHA and
protected hash. Bootstrap only after the approved plan SHA from the handoff
matches. No existing /tmp planning artifact is a required input:

```sh
rtk proxy python3 - docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md <<'PY'
import pathlib,re,sys
text=pathlib.Path(sys.argv[1]).read_text()
match=re.search(r'^### Block BOOTSTRAP\n```python\n(.*?)\n```',text,re.M|re.S)
assert match
exec(compile(match.group(1),'<approved CSV command bootstrap>','exec'))
PY
```

Set P3B10_WORK to the exact emitted temporary path and retain raw output and exit
status of each command there. Use the repository root for all commands:

```sh
rtk proxy python3 "$P3B10_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --stage 1 --check-only
rtk proxy python3 "$P3B10_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-csv-commands
rtk proxy python3 "$P3B10_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --mode red
rtk proxy python3 "$P3B10_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-csv-commands
rtk proxy python3 "$P3B10_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --mode green
rtk proxy python3 "$P3B10_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --mode regression-proof
rtk proxy python3 "$P3B10_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 "$P3B10_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b10-packed-csv-commands-plan.md --mode green
rtk git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

The focused RED exits39 intentionally; capture and assess it before proceeding.
Red/proof wrappers handle their expected internal exit1; setup/compile failure
is never accepted as RED. Do not put the whole sequence in an abort-on-error
shell or mask unexpected exits.

## Appendix B — planning verification

Normal scratch tree /tmp/p3b10-author/tree was created from git archive of the
exact base, without copying the protected/untracked file. Final code/tests build
warning-free. Captured RED178/39 exit39 precedes implementation; GREEN217/0
exit0; fresh normal full suite20820/0 across528 cases. Both real command bodies
are extracted for the probe; schema/registry mocks are local to scratch.
Green probe388/0 with71 injections and largest24/live0. Published frozen-block,
anchor/postimage, bootstrap and reversal checks also pass: all34 manifest block
hashes (35 extracted blocks including MANIFEST), real-root stage1 --check-only
for four files with no writes, and transformation replay of stages1/2/3 for
four/three/four files. All ten final postimages match the scratch tree. The
published regression proof reverses/restores all three source files exactly,
reproduces six gates/two real command-output failures, and returns388/0 after
restoration. Stage3 concept text was authored after the compiled-code full
suite and checked separately by exact postimages; execution still requires the
literal fresh post-docs full run. All production/test/header/build inputs are
identical to the verified full-suite build.
This planning turn makes no root production edit and runs no sanitizer or bench.

Planning-time executable checking caught a stage-boundary issue: after docs,
slotcask.h has the approved caller-comment update. Green/proof checks now accept
only approved checkpoint2 or checkpoint3 source/header hashes; all runtime code
hashes are identical between those checkpoints. Red remains checkpoint1 only.
No file is skipped or admitted by a loose match. Early probe construction also
corrected a missing copied prototype/header and a duplicate feature macro before
freezing. RED allocation tracking stays disabled because stub backends cannot
warm caches; allocation/error ownership assertions run only on the warmed GREEN
backend. Actual old-command output, not a failed warmup, drives both RED gates.

Current reference logs live under /tmp/p3b10-author/: red-final-build.log,
red-focused.log, green-build.log, green-focused.log, full.log,
probe-red-final.log, probe-final.log and published-proof.log. Both normal build
logs and final probe compilation contain zero warnings. Root production files
remain at the committed base; only this plan and the work index are new planning
changes. No model-credit assumption or result from unavailable discovery agents
is represented as evidence.

## Pre-execution review — 2026-10-27

Fresh blind source/standards review and separate plan-aware execution review
reported no actionable findings. The blind reviewer saw the proposed source,
tests and concept docs, not this plan or planning evidence. Review limitations
are recorded in 2026-10-27-phase3b10-plan-review.md. The opening fetch summary
now states the already-frozen NULL/empty-cursor-as-zero contract explicitly;
Task 0 also lists the new review artifact as a permitted pre-existing file.
No executable block, manifest, production postimage or expected count changed.
Fresh checks: all 34 block hashes, all ten final postimages and root stage-1
read-only preflight pass; focused scratch test 217/0 and published green probe
388/0, including all 71 allocation injections, pass. The previously captured
20820/0 normal full suite remains evidence for identical compiled inputs;
executor must still perform its own fresh full run after stage 3.

## Frozen executable blocks

### Block HEADER
```c
#ifndef PACKED_CSV_COMMAND_H
#define PACKED_CSV_COMMAND_H
#include "typed_row.h"
typedef struct {
    const char *fields;
    char delimiter;
    size_t buffer_budget; /* owned response lines + NULs + list nodes */
    uint32_t timeout_ms;
} PackedCsvOptions;
/* Private packed-only command backends. Caller holds shared object lock and
   keeps the matching registry instance, schema, options and inputs alive.
   No nested write reservation. These functions reserve every writer gate
   during collection, release before FILE I/O, then emit a complete prepared
   response. Construction/read failures emit one JSON error before any CSV.
   I/O errors return -1 without appending a replacement error to a CSV prefix.
   Budgets exclude borrowed input, cache maps and temporary renderer/decoder
   workspaces; this is not a total peak-memory cap. No public activation. */
int packed_csv_fetch(SlotcaskDb *db,const FieldSchema *fs,int offset,int limit,
    const char *cursor,int want_total,const PackedCsvOptions *options,FILE *out);
int packed_csv_get_multi(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const char *keys_json,const PackedCsvOptions *options,FILE *out);
#endif
```

### Block STUB
```c
#include "packed_csv_command.h"
#include <errno.h>
int packed_csv_fetch(SlotcaskDb *db,const FieldSchema *fs,int offset,int limit,
    const char *cursor,int want_total,const PackedCsvOptions *options,FILE *out){
    (void)db;(void)fs;(void)offset;(void)limit;(void)cursor;(void)want_total;(void)options;(void)out;
    errno=ENOTSUP;return -1;
}
int packed_csv_get_multi(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const char *keys_json,const PackedCsvOptions *options,FILE *out){
    (void)db;(void)fs;(void)schema;(void)keys_json;(void)options;(void)out;
    errno=ENOTSUP;return -1;
}
```

### Block FULL
```c
#include "packed_csv_command.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct CsvLine {struct CsvLine *next;char *text;size_t len;} CsvLine;
typedef struct {
    const FieldSchema *fs;
    const PackedCsvOptions *options;
    CsvLine *head,*tail;
    size_t bytes;
    int error,found;
    int64_t skip;
    size_t count,limit;
    const uint8_t *storage_key;
    size_t storage_len;
    const char *wire_key;
    size_t wire_len;
    uint64_t started;
} CsvCommand;
static void csv_lines_free(CsvCommand *c){
    while(c->head){CsvLine *line=c->head;c->head=line->next;free(line->text);free(line);}
    c->tail=NULL;c->bytes=0;
}
static int csv_error(FILE *out,int code){
    const char *message=code==ENOMEM?"{\"error\":\"CSV response allocation or buffer limit exceeded\"}\n":
        code==ENOBUFS?"{\"error\":\"query memory buffer exceeded; reduce CSV rows or projection\"}\n":
        code==EILSEQ?"{\"error\":\"invalid packed CSV record or schema\"}\n":
        code==EIO?"{\"error\":\"CSV storage read failed\"}\n":
        code==ETIMEDOUT?"{\"error\":\"query timed out\"}\n":
        code==E2BIG?"{\"error\":\"CSV selector or key exceeds limit\"}\n":
        code==ENOTSUP?"{\"error\":\"total with format=csv is not supported\"}\n":
        "{\"error\":\"invalid packed CSV request\"}\n";
    if(!out){errno=code;return -1;}
    size_t n=strlen(message);
    if(fwrite(message,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    errno=code;return -1;
}
static int csv_deadline(CsvCommand *c){
    if(c->options->timeout_ms&&now_ms_coarse()-c->started>=c->options->timeout_ms){
        c->error=ETIMEDOUT;return -1;
    }
    return 0;
}
static size_t csv_available(CsvCommand *c){
    size_t budget=c->options->buffer_budget;
    if(c->bytes>budget||budget-c->bytes<=sizeof(CsvLine)){
        c->error=ENOBUFS;return 0;
    }
    return budget-c->bytes-sizeof(CsvLine);
}
static int csv_add(CsvCommand *c,char *text,size_t n){
    if(!text){c->error=errno?errno:EIO;return -1;}
    size_t available=csv_available(c);
    if(n==SIZE_MAX||n+1>available){free(text);c->error=ENOBUFS;return -1;}
    CsvLine *line=malloc(sizeof(*line));
    if(!line){free(text);c->error=ENOMEM;return -1;}
    *line=(CsvLine){.text=text,.len=n};
    if(c->tail)c->tail->next=line;else c->head=line;
    c->tail=line;c->bytes+=sizeof(*line)+n+1;return 0;
}
static int csv_finish(CsvCommand *c,FILE *out){
    if(c->error){int code=c->error;csv_lines_free(c);return csv_error(out,code);}
    for(CsvLine *p=c->head;p;p=p->next){
        if(fwrite(p->text,1,p->len,out)!=p->len||ferror(out)){csv_lines_free(c);errno=EIO;return -1;}
    }
    csv_lines_free(c);if(fflush(out)||ferror(out)){errno=EIO;return -1;}return 0;
}
static int csv_start(CsvCommand *c,const FieldSchema *fs,
    const PackedCsvOptions *options,FILE *out){
    memset(c,0,sizeof(*c));
    if(!out||!fs||!fs->ts||!fs->ts->packed.nfields||!options){c->error=EINVAL;return -1;}
    c->fs=fs;c->options=options;c->started=now_ms_coarse();
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *text=typed_packed_csv_header(fs->ts,options->fields,options->delimiter,budget,&n);
    return csv_add(c,text,n);
}
static int csv_record(CsvCommand *c,const void *value,size_t vlen,uint8_t hdr2,
    const char *wire,size_t wire_len){
    if(csv_deadline(c))return -1;
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *text=typed_packed_csv_row(c->fs->ts,value,vlen,hdr2,wire,wire_len,
        c->options->fields,c->options->delimiter,budget,&n);
    return csv_add(c,text,n);
}
static int csv_fetch_record(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t hdr2,void *ptr){
    (void)hash;CsvCommand *c=ptr;
    if(csv_deadline(c))return 1;
    if(c->skip){c->skip--;return 0;}
    if(!klen||klen>MAX_KEY_CEILING){c->error=EILSEQ;return 1;}
    char wire[MAX_KEY_CEILING+1];
    const Schema *sc=c->fs->auto_key==AK_NONE?NULL:&c->fs->auto_key_schema_snapshot;
    int len=format_wire_key(sc,key,klen,wire,sizeof(wire));
    if(len<0){c->error=EILSEQ;return 1;}
    if(csv_record(c,value,vlen,hdr2,wire,(size_t)len))return 1;
    c->count++;return c->count>=c->limit;
}
static int csv_offset(const char *cursor,int offset,int64_t *out){
    uint64_t n=0;
    if(cursor)for(const unsigned char *p=(const unsigned char*)cursor;*p;p++){
        if(*p<'0'||*p>'9'||n>((uint64_t)INT64_MAX-(*p-'0'))/10){errno=EINVAL;return -1;}
        n=n*10+(*p-'0');
    }
    uint64_t add=offset>0?(uint64_t)offset:0;
    if(n>(uint64_t)INT64_MAX-add){errno=EINVAL;return -1;}
    *out=(int64_t)(n+add);return 0;
}
int packed_csv_fetch(SlotcaskDb *db,const FieldSchema *fs,int offset,int limit,
    const char *cursor,int want_total,const PackedCsvOptions *options,FILE *out){
    CsvCommand c;
    if(csv_start(&c,fs,options,out))return csv_finish(&c,out);
    if(!db){c.error=EIO;return csv_finish(&c,out);}
    if(limit<=0||csv_offset(cursor,offset,&c.skip)){c.error=EINVAL;return csv_finish(&c,out);}
    if(want_total){c.error=ENOTSUP;return csv_finish(&c,out);}
    c.limit=(size_t)limit;SlotcaskWriteReservation reservation={0};errno=0;
    if(slotcask_write_reservation_begin(db,&reservation)){c.error=errno==ENOMEM?ENOMEM:EIO;return csv_finish(&c,out);}
    int rc=slotcask_walk_live_strict(db,csv_fetch_record,&c);
    slotcask_write_reservation_end(&reservation);
    if(rc<0&&!c.error)c.error=EIO;
    if(rc==1&&!c.error&&c.count!=c.limit)c.error=EIO;
    if(!c.error)csv_deadline(&c);
    return csv_finish(&c,out);
}
static int csv_get_record(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t hdr2,void *ptr){
    (void)hash;CsvCommand *c=ptr;
    if(klen!=c->storage_len||memcmp(key,c->storage_key,klen))return 0;
    c->found=1;csv_record(c,value,vlen,hdr2,c->wire_key,c->wire_len);return 1;
}
static int csv_missing(CsvCommand *c){
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    char *key=typed_csv_cell_text(c->wire_key,c->wire_len,c->options->delimiter,budget,&n);
    if(!key){c->error=errno?errno:EIO;return -1;}
    size_t fields=0;
    for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed)fields++;
    if(n>SIZE_MAX-fields-2||n+fields+2>budget){free(key);c->error=ENOBUFS;return -1;}
    char *line=malloc(n+fields+2);if(!line){free(key);c->error=ENOMEM;return -1;}
    memcpy(line,key,n);free(key);
    memset(line+n,c->options->delimiter,fields);n+=fields;line[n++]='\n';line[n]=0;
    return csv_add(c,line,n);
}
int packed_csv_get_multi(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const char *keys_json,const PackedCsvOptions *options,FILE *out){
    CsvCommand c;
    if(csv_start(&c,fs,options,out))return csv_finish(&c,out);
    if(!db){c.error=EIO;return csv_finish(&c,out);}
    if(!schema||schema->max_key<1||schema->max_key>MAX_KEY_CEILING||
       !keys_json||options->fields||typed_json_validate(keys_json)){
        c.error=EINVAL;return csv_finish(&c,out);
    }
    const char *p=json_skip(keys_json);if(*p++!='['){c.error=EINVAL;return csv_finish(&c,out);}
    SlotcaskWriteReservation reservation={0};errno=0;
    if(slotcask_write_reservation_begin(db,&reservation)){c.error=errno==ENOMEM?ENOMEM:EIO;return csv_finish(&c,out);}
    size_t wire_max=schema->auto_key==AK_NONE?(size_t)schema->max_key:MAX_KEY_CEILING;
    p=json_skip(p);
    while(*p!=']'&&!c.error){
        if(csv_deadline(&c))break;
        if(*p!='"'){c.error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        if(*end!='"'||raw>6u*wire_max){c.error=E2BIG;break;}
        char *wire=NULL;size_t wire_len=0;errno=0;
        if(json_unescape_string(start,raw,&wire,&wire_len)){c.error=errno==ENOMEM?ENOMEM:EINVAL;break;}
        if(wire_len>wire_max){free(wire);c.error=E2BIG;break;}
        uint8_t storage[16];const uint8_t *key=(const uint8_t*)wire;size_t key_len=wire_len;
        if(schema->auto_key==AK_UUID){
            if(strlen(wire)!=wire_len||parse_uuid_string(wire,storage)){c.error=EINVAL;}
            key=storage;key_len=16;
        }else if(schema->auto_key==AK_SEQ){
            int64_t value;
            if(strlen(wire)!=wire_len||parse_seq_key(wire,&value))c.error=EINVAL;
            else for(int i=7;i>=0;i--){storage[i]=(uint8_t)((uint64_t)value&255);value=(int64_t)((uint64_t)value>>8);}
            key=storage;key_len=8;
        }else if(schema->auto_key!=AK_NONE)c.error=EINVAL;
        if(!key_len||key_len>(size_t)schema->max_key)c.error=E2BIG;
        if(!c.error){
            uint8_t hash[16];compute_hash_raw((const char*)key,key_len,hash);
            c.storage_key=key;c.storage_len=key_len;c.wire_key=wire;c.wire_len=wire_len;c.found=0;
            int rc=slotcask_guard_lookup(db,hash,csv_get_record,&c);
            if(rc<0&&!c.error)c.error=EIO;
            if(!c.error&&!c.found)csv_missing(&c);
        }
        free(wire);p=json_skip(end+1);
        if(*p==',')p=json_skip(p+1);
        else if(*p!=']')c.error=EINVAL;
    }
    slotcask_write_reservation_end(&reservation);
    if(!c.error)csv_deadline(&c);
    return csv_finish(&c,out);
}
```

### Block TEST
```c
#define _GNU_SOURCE
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {TypedSchema *ts;FieldSchema fs;Schema sc;SlotcaskDb db;char root[96];int opened;} CFixture;
static int c_remove(const char *p,const struct stat *st,int kind,struct FTW *walk){
    (void)st;(void)walk;return kind==FTW_DP?rmdir(p):unlink(p);
}
static void c_close(CFixture *f){
    if(f->opened)slotcask_close(&f->db);
    slotcask_shutdown();free(f->ts);
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,c_remove,16,FTW_DEPTH|FTW_PHYS),0,"remove CSV object");
}
static int c_open(CFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));ASSERT_NOT_NULL(f->ts,"command schema");if(!f->ts)return -1;
    f->ts->typed=1;f->ts->nfields=maximum?MAX_FIELDS:3;const char *types[MAX_FIELDS];
    for(int i=0;i<f->ts->nfields;i++){
        types[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":"numeric";
        f->ts->fields[i].type=maximum||i==1?FT_VARCHAR:FT_NUMERIC;
        snprintf(f->ts->fields[i].name,sizeof(f->ts->fields[i].name),"f%d",i);
        f->ts->fields[i].name_len=(int)strlen(f->ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(f->ts,types,(size_t)f->ts->nfields,NULL,0);ASSERT_EQ_INT(rc,0,"bind command schema");if(rc)return -1;
    size_t bound=0;ASSERT_EQ_INT(pkr_slot_bound(&f->ts->packed,32,&bound),DEC_OK,"command slot bound");
    f->sc=(Schema){.splits=1,.streams=1,.slot_size=(int)bound,.max_key=32,.max_value=f->ts->total_size};
    f->fs.ts=f->ts;f->fs.auto_key_schema_snapshot=f->sc;
    strcpy(f->root,"/tmp/shard-packed-csv-command-XXXXXX");
    ASSERT_NOT_NULL(mkdtemp(f->root),"command directory");
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,1,1,(int)bound);f->opened=rc==0;
    ASSERT_EQ_INT(rc,0,"open command object");return rc;
}
static int c_seed(CFixture *f,const void *key,size_t klen,const char *json,uint8_t flags){
    uint8_t *bytes=NULL;size_t n=0;
    int rc=typed_encode_alloc(f->ts,json,1,NULL,NULL,&bytes,&n,NULL,0);
    ASSERT_EQ_INT(rc,0,"encode command record");if(rc)return rc;
    SlotcaskUpsertOpts options={.record_hdr2=flags};
    rc=slotcask_upsert_with_hooks(&f->db,-1,key,klen,bytes,n,&options,NULL);
    ASSERT_EQ_INT(rc,0,"store command record");free(bytes);return rc;
}
static char *c_call(CFixture *f,int multi,const char *keys,const char *fields,
    const char *cursor,int offset,int limit,int total,size_t budget,char delimiter,int *status){
    char *text=NULL;size_t n=0;FILE *out=open_memstream(&text,&n);
    ASSERT_NOT_NULL(out,"command output");if(!out)return NULL;
    PackedCsvOptions options={.fields=fields,.delimiter=delimiter,.buffer_budget=budget};
    ASSERT_EQ_INT(objlock_rdlock(f->root,"obj"),0,"caller read lock");
    *status=multi?packed_csv_get_multi(&f->db,&f->fs,&f->sc,keys,&options,out):
        packed_csv_fetch(&f->db,&f->fs,offset,limit,cursor,total,&options,out);
    objlock_rdunlock(f->root,"obj");
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f->db),0,"no reservation leak");
    ASSERT_EQ_INT(fclose(out),0,"close command output");return text;
}
static void c_error(CFixture *f,int multi,const char *keys,const char *fields,
    const char *cursor,int offset,int total,size_t budget){
    int rc=0;char *text=c_call(f,multi,keys,fields,cursor,offset,10,total,budget,',',&rc);
    ASSERT_EQ_INT(rc,-1,"command failure status");
    ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9)&&!strstr(text,"key,f0"),"one error before any CSV prefix");free(text);
}
static void c_routes(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}int rc=0;
    char *text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"empty fetch");ASSERT_EQ_STR(text,"key,f0,f1,f2\n","empty fetch header");free(text);
    text=c_call(&f,1,"[]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"empty multi-get");ASSERT_EQ_STR(text,"key,f0,f1,f2\n","empty multi header");free(text);
    const char *a="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"a\\u0000b\\\\u0000\",\"f2\":\"1.2300\"}";
    c_seed(&f,"a",1,a,PKR_FLAGS);
    c_seed(&f,"b",1,"{\"f0\":2,\"f1\":\"b,\\\"\\n\",\"f2\":null}",PKR_FLAGS);
    const char *arow="a,1234567890123456.1234567890,a\\u0000b\\\\u0000,1.2300\n";
    const char *brow="b,2.0000000000,\"b,\"\"\\u000a\",\n";
    char expected[512];snprintf(expected,sizeof(expected),"key,f0,f1,f2\n%smissing,,,\n%s%s",arow,arow,brow);
    text=c_call(&f,1,"[\"a\",\"missing\",\"a\",\"b\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"multi-get CSV");ASSERT_EQ_STR(text,expected,"request order, duplicate, missing and exact values");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"fetch CSV");ASSERT_TRUE(text&&strstr(text,arow)&&strstr(text,brow),"fetch complete exact rows");free(text);
    char *first=c_call(&f,0,NULL,"f2,f1+f0",NULL,0,1,0,4096,'|',&rc);
    ASSERT_EQ_INT(rc,0,"fetch projection/delimiter");ASSERT_TRUE(first&&!strncmp(first,"key|f2|f1+f0\n",13),"projection header");
    char *next=c_call(&f,0,NULL,"f2,f1+f0","1",0,1,0,4096,'|',&rc);
    ASSERT_EQ_INT(rc,0,"fetch cursor");ASSERT_TRUE(first&&next&&strcmp(first,next),"cursor advances one physical live record");free(first);free(next);
    c_error(&f,0,NULL,NULL,"junk",0,0,4096);c_error(&f,0,NULL,NULL,"-1",0,0,4096);
    c_error(&f,0,NULL,NULL,"9223372036854775807",1,0,4096);
    c_error(&f,0,NULL,NULL,NULL,0,1,4096);c_error(&f,0,NULL,NULL,NULL,0,0,1);
    c_error(&f,1,"[\"a\",0]",NULL,NULL,0,0,4096);c_error(&f,1,"{}",NULL,NULL,0,0,4096);
    c_error(&f,1,"[\"a\"] trailing",NULL,NULL,0,0,4096);c_error(&f,1,"[\"a\"]",NULL,NULL,0,0,64);
    c_seed(&f,"a",1,a,SREC_FLAG_NULLMAP);c_error(&f,0,NULL,NULL,NULL,0,0,4096);
    c_error(&f,1,"[\"a\"]",NULL,NULL,0,0,4096);c_seed(&f,"a",1,a,PKR_FLAGS);
    const char binary[]={'x',0,'z'};c_seed(&f,binary,sizeof(binary),a,PKR_FLAGS);
    text=c_call(&f,1,"[\"x\\u0000z\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"binary-key lookup");ASSERT_TRUE(text&&strstr(text,"x\\u0000z,1234567890123456.1234567890"),"binary key length preserved");free(text);
    c_close(&f);
}
static void c_auto(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}int rc=0;
    f.sc.auto_key=AK_UUID;f.fs.auto_key=AK_UUID;f.fs.auto_key_schema_snapshot=f.sc;
    uint8_t uuid[16];ASSERT_EQ_INT(parse_uuid_string("00112233-4455-6677-8899-aabbccddeeff",uuid),0,"UUID key");
    c_seed(&f,uuid,16,"{\"f0\":1}",PKR_FLAGS);
    char *text=c_call(&f,1,"[\"00112233-4455-6677-8899-aabbccddeeff\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"UUID multi-get");ASSERT_TRUE(text&&strstr(text,"00112233-4455-6677-8899-aabbccddeeff,1.0000000000,,"),"UUID storage lookup and display");free(text);
    char escaped[256];size_t pos=0;escaped[pos++]='[';escaped[pos++]='"';
    const char *uuid_text="00112233-4455-6677-8899-aabbccddeeff";
    for(size_t i=0;i<strlen(uuid_text);i++)pos+=(size_t)snprintf(escaped+pos,sizeof(escaped)-pos,"\\u%04x",(unsigned char)uuid_text[i]);
    escaped[pos++]='"';escaped[pos++]=']';escaped[pos]=0;
    text=c_call(&f,1,escaped,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"escaped UUID uses text bound rather than binary width");
    ASSERT_TRUE(text&&strstr(text,"00112233-4455-6677-8899-aabbccddeeff,1.0000000000"),"escaped UUID exact lookup");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"UUID fetch");ASSERT_TRUE(text&&strstr(text,uuid_text),"UUID fetch uses stored-key display formatter");free(text);
    c_error(&f,1,"[\"broken\"]",NULL,NULL,0,0,4096);c_close(&f);
    if(c_open(&f,0)){c_close(&f);return;}
    f.sc.auto_key=AK_SEQ;f.sc.max_key=8;f.fs.auto_key=AK_SEQ;f.fs.auto_key_schema_snapshot=f.sc;
    uint8_t seq[8];memset(seq,255,8);c_seed(&f,seq,8,"{\"f2\":\"-0.5000\"}",PKR_FLAGS);
    text=c_call(&f,1,"[\"-1\",\"2\"]",NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"sequence multi-get");ASSERT_EQ_STR(text,"key,f0,f1,f2\n-1,,,-0.5000\n2,,,\n","sequence/missing key CSV");free(text);
    text=c_call(&f,0,NULL,NULL,NULL,0,10,0,4096,',',&rc);
    ASSERT_EQ_INT(rc,0,"sequence fetch");ASSERT_TRUE(text&&strstr(text,"-1,,,-0.5000"),"sequence fetch formatting");free(text);
    c_error(&f,1,"[\"01\"]",NULL,NULL,0,0,4096);c_close(&f);
}
typedef struct {CFixture *fixture;char bytes[4096];size_t n,allow;int held;} CWriter;
static ssize_t c_write(void *ptr,const char *text,size_t n){
    CWriter *w=ptr;if(slotcask_write_reservation_held(&w->fixture->db))w->held=1;
    size_t take=n;if(take>w->allow)take=w->allow;
    if(take>sizeof(w->bytes)-1-w->n)take=sizeof(w->bytes)-1-w->n;
    memcpy(w->bytes+w->n,text,take);w->n+=take;w->bytes[w->n]=0;
    if(!take){errno=EIO;return -1;}w->allow-=take;return (ssize_t)take;
}
#if defined(__linux__)
static FILE *c_cookie(CWriter *writer){cookie_io_functions_t io={.write=c_write};return fopencookie(writer,"w",io);}
#else
static int c_funwrite(void *ptr,const char *text,int n){return (int)c_write(ptr,text,(size_t)n);}
static FILE *c_cookie(CWriter *writer){return funopen(writer,NULL,c_funwrite,NULL,NULL);}
#endif
static void c_io(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    c_seed(&f,"a",1,"{\"f0\":1}",PKR_FLAGS);
    PackedCsvOptions options={.delimiter=',',.buffer_budget=4096};
    for(int buffered=0;buffered<2;buffered++){
        CWriter writer={.fixture=&f,.allow=3};FILE *out=c_cookie(&writer);ASSERT_NOT_NULL(out,"failing CSV FILE");if(!out)continue;
        if(!buffered)setvbuf(out,NULL,_IONBF,0);
        ASSERT_EQ_INT(objlock_rdlock(f.root,"obj"),0,"I/O caller lock");
        int rc=packed_csv_fetch(&f.db,&f.fs,0,10,NULL,0,&options,out);objlock_rdunlock(f.root,"obj");
        ASSERT_EQ_INT(rc,-1,"short write/flush fails command");ASSERT_TRUE(!strstr(writer.bytes,"error"),"no error appended to CSV prefix");
        ASSERT_EQ_INT(writer.held,0,"writer reservation released before I/O");
        ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db),0,"I/O failure releases reservation");fclose(out);
    }
    c_close(&f);
}
static void c_maximum(void){
    CFixture f;if(c_open(&f,1)){c_close(&f);return;}c_seed(&f,"a",1,"{\"f127\":\"tiny\"}",PKR_FLAGS);
    int rc=0;char *text=c_call(&f,0,NULL,"f0,f127",NULL,0,10,0,100,',',&rc);
    ASSERT_EQ_INT(rc,0,"maximum schema bounded fetch");ASSERT_EQ_STR(text,"key,f0,f127\na,,tiny\n","actual-size response at large schema ceiling");free(text);c_close(&f);
}
static int test_packed_csv_commands_run(void){c_routes();c_auto();c_io();c_maximum();return t_ctx->failed?1:0;}
TEST_REGISTER("test-packed-csv-commands",test_packed_csv_commands_run)
```

### Block PROBE
```c
#define _GNU_SOURCE
#include "packed_csv_command.h"
#include "query_internal.h"
#include "shard_db.h"
#include "test_assert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ftw.h>
#include <unistd.h>
__thread TestCtx *t_ctx;
#include P10_FIXTURES
#define MUST(x) do{if(!(x)){fprintf(stderr,"setup %d: %s errno=%d\n",__LINE__,#x,errno);exit(2);}}while(0)
static _Thread_local int armed;
static _Thread_local size_t calls,nth,live,largest;
static _Thread_local void *owned[4096];
static int find(void *p){for(size_t i=0;i<live;i++)if(owned[i]==p)return (int)i;return -1;}
static int allow(size_t n){if(!armed)return 1;calls++;if(n>largest)largest=n;if(calls==nth||n>32u*1024*1024){errno=ENOMEM;return 0;}return 1;}
static void remember(void *p){if(armed&&p){MUST(live<4096);owned[live++]=p;}}
void *p10_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *p10_calloc(size_t n,size_t m){if(n&&m>SIZE_MAX/n){errno=ENOMEM;return NULL;}if(!allow(n*m))return NULL;void *p=calloc(n,m);remember(p);return p;}
void *p10_realloc(void *old,size_t n){if(!allow(n))return NULL;int i=find(old);void *p=realloc(old,n);if(p){if(i>=0)owned[i]=owned[--live];remember(p);}return p;}
void p10_free(void *p){int i=find(p);if(i>=0)owned[i]=owned[--live];free(p);}
static CFixture *fixture;
static int released,gets,force_storage,force_registry,force_clock;
static uint64_t clock_steps;
uint64_t p10_clock(void){if(force_clock)return 1000+clock_steps++;return now_ms_coarse();}
int p10_walk(SlotcaskDb *db,SlotcaskScanCb cb,void *ptr){if(force_storage){errno=EIO;return -1;}return slotcask_walk_live_strict(db,cb,ptr);}
int p10_lookup(SlotcaskDb *db,const uint8_t hash[16],SlotcaskScanCb cb,void *ptr){if(force_storage){errno=EIO;return -1;}return slotcask_guard_lookup(db,hash,cb,ptr);}
static Schema probe_schema(const char *root,const char *object){MUST(!strcmp(root,fixture->root)&&!strcmp(object,"obj"));return fixture->sc;}
static TypedSchema *probe_typed(const char *root,const char *object){(void)probe_schema(root,object);return fixture->ts;}
static void probe_fields(FieldSchema *fs,const char *root,const char *object){(void)probe_schema(root,object);*fs=fixture->fs;}
static SlotcaskDb *probe_registry(const char *root,const char *object,const SlotcaskSchemaInfo *info){
    (void)probe_schema(root,object);MUST(info->slot_size==fixture->sc.slot_size);gets++;
    if(force_registry){errno=EIO;return NULL;}return &fixture->db;
}
static void probe_release(SlotcaskDb **db){MUST(*db==NULL||*db==&fixture->db);released++;}
#define init_field_schema probe_fields
#define load_schema probe_schema
#define load_typed_schema probe_typed
#define slotcask_registry_get probe_registry
#define slotcask_registry_ref_release probe_release
#define cmd_fetch probe_cmd_fetch
#define cmd_get_multi probe_cmd_get_multi
#define free p10_free
#include P10_COMMANDS
#undef free
#undef init_field_schema
#undef load_schema
#undef load_typed_schema
#undef slotcask_registry_get
#undef slotcask_registry_ref_release
#undef cmd_fetch
#undef cmd_get_multi
static int failures,gates,red_mode;
static void gate(const char *name,int ok){gates++;printf("%s: %s\n",name,ok?"PASS":"FAIL");if(!ok)failures++;}
static int invoke(int mode,size_t fail,char **text,int *cause){
    size_t n=0;FILE *out=open_memstream(text,&n);MUST(out);FILE *save=g_out;g_out=out;
    calls=0;nth=fail;released=gets=0;armed=!red_mode;errno=0;
    MUST(!objlock_rdlock(fixture->root,"obj"));
    int rc=mode?probe_cmd_get_multi(fixture->root,"obj","[\"a\",\"missing\",\"a\"]","csv",","):
        probe_cmd_fetch(fixture->root,"obj",0,10,"f0,f1,f2",NULL,"csv",",",0);
    *cause=errno;objlock_rdunlock(fixture->root,"obj");armed=0;g_out=save;MUST(!fclose(out));
    if(!red_mode)gate("no selected live buffers",live==0);gate("one scoped registry cleanup",gets==1&&released==1);
    gate("all writer gates released",!slotcask_write_reservation_held(&fixture->db));return rc;
}
static void routes(void){
    const char *row="a,1234567890123456.1234567890,a\\u0000b\\\\u0000,1.2300\n";
    char expected[512];int cause=0;
    for(int mode=0;mode<2;mode++){
        snprintf(expected,sizeof(expected),mode?"key,f0,f1,f2\n%smissing,,,\n%s":"key,f0,f1,f2\n%s",row,row);
        char *text=NULL;int rc=invoke(mode,0,&text,&cause);size_t count=calls;
        gate("actual command dispatch exact",rc==0&&text&&!strcmp(text,expected));free(text);
        if(rc||!count){printf("mode=%d no green sweep\n",mode);continue;}
        for(size_t i=1;i<=count;i++){
            rc=invoke(mode,i,&text,&cause);
            gate("construction failure explicit",rc==-1&&cause==ENOMEM);
            gate("one JSON error no CSV prefix",text&&!strncmp(text,"{\"error\":",9)&&!strstr(text,"key,f0"));free(text);
        }
        printf("mode=%d allocation injections=%zu\n",mode,count);
        force_storage=1;rc=invoke(mode,0,&text,&cause);force_storage=0;
        gate("storage failure before success prefix",rc==-1&&cause==EIO&&text&&!strncmp(text,"{\"error\":",9));free(text);
        force_registry=1;rc=invoke(mode,0,&text,&cause);force_registry=0;
        gate("registry failure before success prefix",rc==-1&&cause==EIO&&text&&!strncmp(text,"{\"error\":",9));free(text);
        force_clock=1;clock_steps=0;g_request_timeout_ms=1;rc=invoke(mode,0,&text,&cause);
        force_clock=0;g_request_timeout_ms=0;
        gate("deadline before success prefix",rc==-1&&cause==ETIMEDOUT&&text&&!strncmp(text,"{\"error\":",9));free(text);
    }
}
int main(int argc,char **argv){
    red_mode=argc>1&&!strcmp(argv[1],"red");
    char owner_root[]="/tmp/p3b10-owner-XXXXXX";MUST(mkdtemp(owner_root));
    ShardDb *owner=shard_db_open_internal(owner_root);MUST(owner);
    FILE *tap=tmpfile();MUST(tap);TestCtx context={.out=tap};t_ctx=&context;
    CFixture f;MUST(!c_open(&f,0));fixture=&f;
    MUST(!c_seed(&f,"a",1,"{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"a\\u0000b\\\\u0000\",\"f2\":\"1.2300\"}",PKR_FLAGS));
    g_query_buffer_max_bytes=1024*1024;
    /* Warm storage/cache mappings before selected-allocation sweeps. */
    PackedCsvOptions options={.delimiter=',',.buffer_budget=1024*1024};
    FILE *warm=tmpfile();MUST(warm);MUST(!objlock_rdlock(f.root,"obj"));
    int warm_rc=packed_csv_fetch(&f.db,&f.fs,0,10,NULL,0,&options,warm);objlock_rdunlock(f.root,"obj");fclose(warm);
    if(argc>1&&!strcmp(argv[1],"red")){
        char *text=NULL;int cause;int rc=invoke(0,0,&text,&cause);
        gate("actual fetch reaches checked backend",rc==0&&text&&strstr(text,"a\\u0000b\\\\u0000"));free(text);
        rc=invoke(1,0,&text,&cause);gate("actual multi-get reaches checked backend",rc==0&&text&&strstr(text,"missing,,,\n"));free(text);
    }else{
        MUST(warm_rc==0);routes();
        c_close(&f);MUST(!c_open(&f,1));fixture=&f;MUST(!c_seed(&f,"a",1,"{\"f127\":\"tiny\"}",PKR_FLAGS));
        char *text=NULL;size_t n=0;FILE *out=open_memstream(&text,&n);MUST(out);
        PackedCsvOptions tiny={.fields="f0,f127",.delimiter=',',.buffer_budget=100};
        calls=nth=largest=0;armed=1;MUST(!objlock_rdlock(f.root,"obj"));
        int rc=packed_csv_fetch(&f.db,&f.fs,0,10,NULL,0,&tiny,out);
        objlock_rdunlock(f.root,"obj");armed=0;MUST(!fclose(out));
        gate("maximum schema bounded actual allocations",rc==0&&text&&!strcmp(text,"key,f0,f127\na,,tiny\n")&&largest<4096&&live==0);free(text);
        printf("maximum largest=%zu live=%zu\n",largest,live);
    }
    c_close(&f);MUST(!context.failed);fclose(tap);shard_db_close(owner);MUST(!nftw(owner_root,c_remove,16,FTW_DEPTH|FTW_PHYS));
    printf("final selected live=%zu\n",live);printf("probe gates=%d failures=%d\n",gates,failures);return failures?1:0;
}
```

### Block PROBE_TOOLS
```python
import pathlib,re,subprocess,tempfile
TOKEN=re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|/\*.*?\*/|//[^\n]*|[{}]',re.S)
def function(source,name):
    match=re.search(r'^(?:static\s+)?(?:int|void)\s+'+re.escape(name)+r'\(',source,re.M)
    assert match,name
    depth=0;started=False
    for token in TOKEN.finditer(source,match.start()):
        text=token.group()
        if text=='{':depth+=1;started=True
        elif text=='}':
            depth-=1
            if started and not depth:return source[match.start():token.end()]+'\n'
    raise AssertionError(name)
def command_source(root):
    find=(root/'src/db/query_find.c').read_text();storage=(root/'src/db/storage.c').read_text()
    a=find.index('/* === v2 cmd_fetch worker ===');z=find.index('static int v2_fetch_cb(',a)
    fetch=find[a:z]+function(find,'render_wire_key')+function(find,'v2_fetch_cb')+function(find,'cmd_fetch_v2')+function(find,'cmd_fetch')
    a=storage.index('/* ========== Parallel multi-key GET ========== */');z=storage.index('int cmd_get_multi(',a)
    get=function(storage,'parse_multi_key')+storage[a:z]+function(storage,'cmd_get_multi')
    declaration=function(find,'print_record_row').split('{',1)[0].strip()+';\n'
    return declaration+fetch+get
def run(cmd,cwd,expect=0):
    print('+',' '.join(map(str,cmd)),flush=True)
    result=subprocess.run(cmd,cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    print(result.stdout.decode('utf-8','backslashreplace'),end='',flush=True)
    assert result.returncode==expect,(result.returncode,expect)
    return result.stdout
def build(root,work,probe,source=None,find_source=None,storage_source=None):
    (work/'probe.c').write_text(probe)
    # Copy code inputs used by the actual frontends, retaining complete legacy
    # branches. Only schema/registry lookup is mocked by the C probe.
    if find_source is not None or storage_source is not None:
        shadow=work/'shadow';(shadow/'src/db').mkdir(parents=True,exist_ok=True)
        (shadow/'src/db/query_find.c').write_text(find_source or (root/'src/db/query_find.c').read_text())
        (shadow/'src/db/storage.c').write_text(storage_source or (root/'src/db/storage.c').read_text())
        commands=command_source(shadow)
    else:commands=command_source(root)
    (work/'commands.inc').write_text(commands)
    test=(root/'src/test/cases/test_packed_csv_commands.c').read_text()
    fixtures=test[:test.index('static void c_routes(void)')]
    fixtures=re.sub(r'^#define (?:_GNU_SOURCE|_XOPEN_SOURCE)[^\n]*\n','',fixtures,flags=re.M)
    (work/'fixtures.inc').write_text(fixtures)
    flags=['gcc','-O1','-g','-I'+str(root/'src/db'),'-I'+str(root/'src/test')]
    wrap=['-D'+name+'=p10_'+name for name in ['malloc','calloc','realloc','free']]
    objects=[]
    for module in ['packed_csv_command','typed_row','packed_record','decnum','util','slotcask']:
        src=root/'src/db'/f'{module}.c'
        if module=='packed_csv_command' and source is not None:src=work/'command.c';src.write_text(source)
        obj=work/f'{module}.o';extra=[]
        if module=='packed_csv_command':extra=['-Dslotcask_walk_live_strict=p10_walk','-Dslotcask_guard_lookup=p10_lookup','-Dnow_ms_coarse=p10_clock']
        run(flags+wrap+extra+['-c',str(src),'-o',str(obj)],root);objects.append(str(obj))
    binary=work/'probe'
    run(flags+['-DP10_FIXTURES="'+str(work/'fixtures.inc')+'"','-DP10_COMMANDS="'+str(work/'commands.inc')+'"',str(work/'probe.c')]+objects+
        [str(root/'build/bin/libshard-db.a'),'-lpthread','-lssl','-lcrypto','-lm','-o',str(binary)],root)
    return binary
```

### Block CHECKS
```python
import argparse,importlib.util,pathlib,tempfile
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True,type=pathlib.Path)
    parser.add_argument('--mode',required=True,choices=['red','green','regression-proof']);args=parser.parse_args()
    here=pathlib.Path(__file__).parent
    spec=importlib.util.spec_from_file_location('driver',here/'driver.py');driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    spec=importlib.util.spec_from_file_location('probe_tools',here/'probe_tools.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    blocks,manifest=driver.read_plan(args.plan);root=pathlib.Path.cwd()
    for name,digest in manifest['unchanged'].items():assert driver.sha((root/name).read_bytes())==digest,('boundary',name)
    stage=1 if args.mode=='red' else 2
    for name,digest in manifest['checkpoints'][str(stage)].items():
        if name.startswith('src/') or name=='build.sh':
            path=root/name;actual=driver.sha(path.read_bytes()) if path.exists() else None
            allowed={digest}
            if stage==2:allowed.add(manifest['checkpoints']['3'][name])
            assert actual in allowed,('checkpoint',name)
    work=pathlib.Path(tempfile.mkdtemp(prefix='p3b10-check-'));print('scratch:',work,flush=True)
    source=(root/'src/db/packed_csv_command.c').read_text()
    find=(root/'src/db/query_find.c').read_text();storage=(root/'src/db/storage.c').read_text()
    if args.mode=='regression-proof':
        prior={
            'src/db/packed_csv_command.c':source,
            'src/db/query_find.c':find,
            'src/db/storage.c':storage,
        }
        for edit in reversed(manifest['stages']['2']):
            path=edit['path'];new=blocks[edit['new']];old=blocks[edit['old']]
            assert prior[path].count(new)==1,('reverse anchor',path)
            prior[path]=prior[path].replace(new,old,1)
        for name,text in prior.items():assert driver.sha(text.encode())==manifest['checkpoints']['1'][name],('reversal',name)
        binary=helper.build(root,work,blocks['PROBE'],prior['src/db/packed_csv_command.c'],prior['src/db/query_find.c'],prior['src/db/storage.c'])
        out=helper.run([str(binary),'red'],root,expect=1);assert b'probe gates=6 failures=2' in out
        for edit in manifest['stages']['2']:
            path=edit['path'];old=blocks[edit['old']];new=blocks[edit['new']]
            assert prior[path].count(old)==1;prior[path]=prior[path].replace(old,new,1)
        for name,text in prior.items():assert driver.sha(text.encode())==manifest['checkpoints']['2'][name],('restoration',name)
        assert prior['src/db/packed_csv_command.c']==source and prior['src/db/query_find.c']==find and prior['src/db/storage.c']==storage
    binary=helper.build(root,work,blocks['PROBE'])
    if args.mode=='red':
        out=helper.run([str(binary),'red'],root,expect=1);assert b'probe gates=6 failures=2' in out
    else:
        out=helper.run([str(binary)],root)
        for marker in [b'probe gates=388 failures=0',b'mode=0 allocation injections=24',b'mode=1 allocation injections=47',b'maximum largest=24 live=0',b'final selected live=0']:
            assert marker in out,marker
    if args.mode=='regression-proof':print('all three reversed/restored source hashes exact; checkout sources and binaries untouched')
if __name__=='__main__':main()
```

### Block DRIVER
```python
import argparse,hashlib,json,pathlib,re,subprocess,sys
PATTERN=r'^### Block ([A-Z0-9_]+)\n```[^\n]*\n(.*?)\n```'
def sha(data):return hashlib.sha256(data).hexdigest()
def read_plan(path):
    matches=re.findall(PATTERN,path.read_text(),re.M|re.S)
    blocks={k:v+'\n' for k,v in matches}
    assert len(blocks)==len(matches),'duplicate block name'
    manifest=json.loads(blocks['MANIFEST'])
    assert set(blocks)==set(manifest['blocks'])|{'MANIFEST'},'block inventory'
    for name,digest in manifest['blocks'].items():assert sha(blocks[name].encode())==digest,('block',name)
    return blocks,manifest
def changes(root,blocks,manifest,stage):
    before=manifest['checkpoints'][str(stage-1)]
    for name,digest in before.items():
        path=root/name
        assert (sha(path.read_bytes()) if path.exists() else None)==digest,('checkpoint',stage-1,name)
    pending={}
    for edit in manifest['stages'][str(stage)]:
        name=edit['path'];path=root/name
        assert not pathlib.PurePosixPath(name).is_absolute() and '..' not in pathlib.PurePosixPath(name).parts
        if edit['kind']=='new':assert not path.exists();text=blocks[edit['block']]
        else:
            text=pending.get(name,path.read_text())
            old=blocks[edit['old']];new=blocks[edit['new']]
            assert text.count(old)==1,('anchor',stage,name,edit['old'])
            text=text.replace(old,new,1)
        pending[name]=text
    after=manifest['checkpoints'][str(stage)]
    for name,text in pending.items():assert sha(text.encode())==after[name],('postimage',name)
    for name,digest in after.items():
        if name not in pending:
            path=root/name
            assert (sha(path.read_bytes()) if path.exists() else None)==digest,('unchanged',name)
    return pending
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True,type=pathlib.Path)
    parser.add_argument('--stage',required=True,type=int,choices=[1,2,3]);parser.add_argument('--check-only',action='store_true')
    args=parser.parse_args();root=pathlib.Path.cwd();blocks,manifest=read_plan(args.plan)
    assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==manifest['base']
    assert subprocess.check_output(['git','branch','--show-current'],text=True).strip()==manifest['branch']
    assert sha((root/manifest['protected']).read_bytes())==manifest['protected_sha']
    for name,digest in manifest['unchanged'].items():assert sha((root/name).read_bytes())==digest,('boundary',name)
    pending=changes(root,blocks,manifest,args.stage)
    if args.check_only:print('stage',args.stage,'preflight verified:',len(pending),'files; no writes');return
    # All preconditions/postimages checked before any write. A filesystem write
    # failure still halts; record the exact partial state instead of retrying.
    for name,text in pending.items():(root/name).write_text(text)
    for name,digest in manifest['checkpoints'][str(args.stage)].items():
        path=root/name;assert (sha(path.read_bytes()) if path.exists() else None)==digest
    print('stage',args.stage,'verified:',len(pending),'files')
if __name__=='__main__':main()
```

### Block CONCEPT
```text
# Packed CSV fetch and multi-get commands (public format inactive)

`packed_csv_fetch` and `packed_csv_get_multi` are concrete, packed-only read
backends. Existing fetch and multi-key GET select them only for CSV and an
explicitly bound packed schema. Loaders/create-object still never bind packed;
legacy CSV and all non-CSV branches retain their previous implementation.
No new query mode, format flag or startup migration is introduced.

The caller holds the shared object lock and pins the matching registry instance
through the call. Tests may own a stable directly opened physical fixture.
Schema/options/input storage stays immutable for the duration. The backend
reserves every existing per-kf-shard writer gate for the object while collecting values, using
the established reservation API and strict walk/lookup contracts. It releases
the reservation before any FILE output, including errors. It never upgrades an
object read lock to a write lock and does not perform index work or database
reentry from its storage callback. No nested reservation is supported.

This deliberately serializes packed CSV collection against writers. It buys a
stable read interval and lets unreadable/invalid references fail closed instead
of silently disappearing. No throughput or lock-wait improvement is claimed.
Fetch offsets use strict traversal, so skipped rows still require backing
reference reads; this differs from the legacy cheap-skip optimization. Limits
stop further scanning once enough rows are collected. No global integrity proof
or cross-request cursor snapshot is implied.

Each line is rendered completely with the checked byte-aware CSV helpers. The
backend owns a linked list of exact header/row buffers and publishes nothing
until collection succeeds. Request/conversion/allocation/record/reference errors
free all prepared lines and emit one JSON error. A late bad requested key or row
cannot turn earlier prepared rows into a successful partial response.

After collection the backend writes the prepared lines and checks fwrite,
ferror and final fflush. If a FILE accepts a prefix and then fails, the backend
returns -1 and does not append replacement JSON to that prefix. This is not
transactional transport; the caller owns framing/connection handling. Command
status reaches the immediate fetch/get-multi caller; existing outer dispatch
conventions remain unchanged.

`PackedCsvOptions.buffer_budget` covers all retained response line allocations,
their NULs and list nodes: sum(sizeof(CsvLine) + line_length + 1). Production
uses the existing QUERY_BUFFER_MB value. Failure before first output is explicit;
there is no fallback to truncated rows or streaming beyond the quota. Borrowed
request input, storage cache mappings, fixed command stack frames, reservation
bookkeeping and temporary decoded-key/renderer/decimal workspaces are excluded. This is an owned-response cap, not
an end-to-end peak-RAM guarantee; full activation still requires the enclosing
request-memory policy. Small selected values never allocate the schema ceiling.

Fetch accepts NULL/empty cursor as zero, otherwise only nonnegative decimal
digits; leading zeros are allowed. Cursor+positive offset must fit int64.
Negative offset remains zero; the effective limit must be positive after the
command applies its existing global default. Fields/composites/delimiter follow
the checked CSV helpers, without the legacy selector truncation. total+CSV is
rejected. Empty objects produce a header-only response.

Multi-get requires a valid JSON array of strings. It decodes each string once,
uses explicit key lengths, preserves request order and duplicates, and emits a
row with empty value cells for each missing key. An empty array produces only
the header. Keys are not reparsed from response JSON. Plain binary keys preserve
NUL; UUID/sequence wire strings use the existing strict storage-key parsers and
reject embedded NUL or malformed forms. Raw escaped text bounds account for
the difference between an auto-key's display length and binary storage width.
The response echoes each decoded requested key; fetch formats stored keys in
the existing canonical wire form. Multi-get field projection is not added.

The command samples its deadline during collection and after traversal. The
existing writer-reservation acquisition and stdio write are blocking operations;
no cancellation guarantee while waiting for gates or client I/O is claimed.
Error paths release owned lines, reservations and the caller's scoped registry
reference. Construction errors are returned as -1 with errno and a JSON error;
I/O failure also returns -1, with EIO.

Visible NUL encoding remains `\u0000` with doubled literal backslashes. Ordinary
CSV parsing precedes byte unescaping. Public import/SDK decoding is still an
activation gate, not delivered by these output backends. Find/joins, aggregate
CSV, keys/exists and other read/delete/index/maintenance consumers remain in
the Phase 3B inventory; exact numeric criteria/indexes and aggregate operations
retain their later roadmap owners. This child does not close Phase 3B.
```

### Block BOOTSTRAP
```python
import hashlib,json,pathlib,re,sys,tempfile
plan=pathlib.Path(sys.argv[1])
matches=re.findall(r'^### Block ([A-Z0-9_]+)\n```[^\n]*\n(.*?)\n```',plan.read_text(),re.M|re.S)
blocks={name:body+'\n' for name,body in matches}
assert len(blocks)==len(matches)
manifest=json.loads(blocks['MANIFEST'])
assert set(blocks)==set(manifest['blocks'])|{'MANIFEST'}
for name,digest in manifest['blocks'].items():
    assert hashlib.sha256(blocks[name].encode()).hexdigest()==digest,name
work=pathlib.Path(tempfile.mkdtemp(prefix='p3b10-exec-'))
for name,file in [('DRIVER','driver.py'),('CHECKS','checks.py'),('PROBE_TOOLS','probe_tools.py')]:
    (work/file).write_text(blocks[name])
print('P3B10_WORK='+str(work))
```

### Block BUILD_SOURCE_1_OLD
```text
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### Block BUILD_SOURCE_1_NEW
```text
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### Block BUILD_SOURCE_2_OLD
```text
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c \
```

### Block BUILD_SOURCE_2_NEW
```text
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c \
```

### Block BUILD_SOURCE_3_OLD
```text
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### Block BUILD_SOURCE_3_NEW
```text
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

### Block BUILD_SOURCE_4_OLD
```text
    src/db/typed_row.c src/db/typed_bulk_insert.c \
```

### Block BUILD_SOURCE_4_NEW
```text
    src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c \
```

### Block BUILD_SOURCE_5_OLD
```text
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c \
```

### Block BUILD_SOURCE_5_NEW
```text
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c \
```

### Block BUILD_TEST_OLD
```text
    src/test/cases/test_packed_csv.c \
```

### Block BUILD_TEST_NEW
```text
    src/test/cases/test_packed_csv.c \
    src/test/cases/test_packed_csv_commands.c \
```

### Block INCLUDE_OLD
```text
#include "typed_row.h"
```

### Block INCLUDE_NEW
```text
#include "typed_row.h"
#include "packed_csv_command.h"
```

### Block FETCH_OLD
```text
    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;
```

### Block FETCH_NEW
```text
    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;
    if (csv_delim && fs_fetch.ts && fs_fetch.ts->packed.nfields) {
        SlotcaskSchemaInfo info = {.splits=sch->splits,.slot_size=sch->slot_size,.streams=sch->streams};
        SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(db_root,object,&info);
        PackedCsvOptions options={.fields=proj_str,.delimiter=csv_delim,
            .buffer_budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
        return packed_csv_fetch(db,&fs_fetch,offset,limit,cursor,want_total,&options,g_out?g_out:stdout);
    }
```

### Block GET_OLD
```text
int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
```

### Block GET_NEW
```text
int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    if (csv_delim) {
        TypedSchema *ts=load_typed_schema(db_root,object);
        if (ts && ts->packed.nfields) {
            FieldSchema fs;init_field_schema(&fs,db_root,object);
            Schema schema=load_schema(db_root,object);
            SlotcaskSchemaInfo info={.splits=schema.splits,.slot_size=schema.slot_size,.streams=schema.streams};
            SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(db_root,object,&info);
            PackedCsvOptions options={.delimiter=csv_delim,.buffer_budget=g_query_buffer_max_bytes,
                .timeout_ms=resolve_timeout_ms()};
            return packed_csv_get_multi(db,&fs,&schema,keys_json,&options,g_out?g_out:stdout);
        }
    }
```

### Block SLOT_DOC_OLD
```text
   query.c's guard_has_other. */
```

### Block SLOT_DOC_NEW
```text
   query.c's guard_has_other and the private packed CSV multi-read backend. */
```

### Block CSV_DOC_OLD
```text
No production caller uses these new APIs yet. Legacy cell/header/row writers,
CSV find/fetch/get-multi/joins/aggregates/keys, query error propagation and
matching/sort/group/index consumers remain unchanged. A following consumer
integration plan must use owned complete output, carry allocation/conversion/
I/O failure to the command result, avoid treating errors as empty cells, define
errors after any previously accepted CSV prefix, and enforce request memory
limits before public packed activation. Loaders/create-object remain inactive.
```

### Block CSV_DOC_NEW
```text
The private packed [fetch and multi-get backends](packed-csv-commands.md) now
consume these renderers and propagate construction/storage/I/O failures through
their command entry points. They buffer complete response lines before output
and cap retained response allocations, including line nodes. Temporary renderer
workspaces remain outside that cap. Existing legacy cell/header/row writers and
CSV find/joins/aggregates/keys remain unchanged. Further emitter/error integration,
enclosing request-memory limits and import/SDK decoding remain activation gates.
Loaders/create-object remain inactive.
```

### Block ADAPTER_DOC_OLD
```text
[Checked packed CSV rendering](packed-csv-rendering.md) supplies owned exact
cells, headers and rows with reversible visible byte escapes. These APIs remain
private; legacy CSV writers and command error propagation are not converted.
```

### Block ADAPTER_DOC_NEW
```text
[Checked packed CSV rendering](packed-csv-rendering.md) supplies owned exact
cells, headers and rows with reversible visible byte escapes. The private
[CSV fetch/multi-get backends](packed-csv-commands.md) connect those helpers to
their command entry points with checked failure handling. Legacy CSV writers,
find/join/aggregate/keys consumers and public packed activation remain separate.
```

### Block MANIFEST
```json
{
  "base": "174f7d8e99bee4a619a23aed3dfa07a3cbb05bcf",
  "branch": "fix/phase1-safe-scalar-conversions",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "unchanged": {
    "src/db/typed_row.c": "dad6b85428457d5abb9004b77610cc8b2f5670bcea62524af737783666ff5998",
    "src/db/typed_row.h": "13cacbc07ece575b17ac0a22788f5a6ff86e2e9aa547e1e2f368138f5c50fdea",
    "src/db/packed_record.c": "17cc0dd151f0a747abaff98291f6f7364d39ecd7e1cdf7c7798dbf4b73c7664c",
    "src/db/packed_record.h": "56682286bd12a932d69fc8bdd46961db2a594357134cf35d90d61a402925ddab",
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed",
    "src/db/decnum.h": "2148631ae519f28649ad3cf386511982ed295850f3176215cbab6c65b265e3a9",
    "src/db/util.c": "6f35842513de4c3655987fc806ee33a1b85c189b7dbf6602da98186a1ff9f27e",
    "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
    "src/db/objlock.c": "0da1ea43c5a4541a184a70cbd8b565cffc1be9a15c3ed782b0a745a2951a9f9c",
    "src/db/query.c": "3c97197a529ea68c4a077581c46a53733fbac183108925d9230f0b9b00402e9e",
    "src/db/query_join.c": "df03bf908c1251d99b4fbdf1f18a84eafb401272faee71540333955d0c73cc05",
    "src/db/query_aggregate.c": "6fc3b133351e59fc0a7bd708e94df4bf6fefa0b3e5b56dcbb02507dbeea09c5f",
    "src/db/query_plan.c": "57cf9a45fd755fc4c4a38b7e13edd212ddba192ab2b2f46ca9aeac697237c5dd",
    "src/db/query_schema.c": "ea3530375358664e300821703a2e718c9da21deb850e9ea15f58cd384cdeea37",
    "src/db/query_internal.h": "89a05d7de59587ada9232e155d1ddc3069b1bdc843d2074178f2acedf29b5400",
    "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
    "src/db/server.c": "a831a4800fd0d3b0537d46657f82dbf1ace8e79793d6e3b7ddb82a404a5761f2",
    "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
    "src/db/index.c": "2951fdc581f44c0024b8dde5d495fff7b64248d3ff5ecb4ea9bfc59cf25ae710",
    "src/db/embedded.c": "6311c45af87446db88e0c9d387296c77c705af3928f5096e9f585e2445591cf6"
  },
  "stages": {
    "1": [
      {
        "kind": "new",
        "path": "src/db/packed_csv_command.h",
        "block": "HEADER"
      },
      {
        "kind": "new",
        "path": "src/db/packed_csv_command.c",
        "block": "STUB"
      },
      {
        "kind": "new",
        "path": "src/test/cases/test_packed_csv_commands.c",
        "block": "TEST"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_SOURCE_1_OLD",
        "new": "BUILD_SOURCE_1_NEW"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_SOURCE_2_OLD",
        "new": "BUILD_SOURCE_2_NEW"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_SOURCE_3_OLD",
        "new": "BUILD_SOURCE_3_NEW"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_SOURCE_4_OLD",
        "new": "BUILD_SOURCE_4_NEW"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_SOURCE_5_OLD",
        "new": "BUILD_SOURCE_5_NEW"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_TEST_OLD",
        "new": "BUILD_TEST_NEW"
      }
    ],
    "2": [
      {
        "kind": "replace",
        "path": "src/db/packed_csv_command.c",
        "old": "STUB",
        "new": "FULL"
      },
      {
        "kind": "replace",
        "path": "src/db/query_find.c",
        "old": "INCLUDE_OLD",
        "new": "INCLUDE_NEW"
      },
      {
        "kind": "replace",
        "path": "src/db/storage.c",
        "old": "INCLUDE_OLD",
        "new": "INCLUDE_NEW"
      },
      {
        "kind": "replace",
        "path": "src/db/query_find.c",
        "old": "FETCH_OLD",
        "new": "FETCH_NEW"
      },
      {
        "kind": "replace",
        "path": "src/db/storage.c",
        "old": "GET_OLD",
        "new": "GET_NEW"
      }
    ],
    "3": [
      {
        "kind": "new",
        "path": "docs/concepts/packed-csv-commands.md",
        "block": "CONCEPT"
      },
      {
        "kind": "replace",
        "path": "src/db/slotcask.h",
        "old": "SLOT_DOC_OLD",
        "new": "SLOT_DOC_NEW"
      },
      {
        "kind": "replace",
        "path": "docs/concepts/packed-csv-rendering.md",
        "old": "CSV_DOC_OLD",
        "new": "CSV_DOC_NEW"
      },
      {
        "kind": "replace",
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "ADAPTER_DOC_OLD",
        "new": "ADAPTER_DOC_NEW"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "src/db/packed_csv_command.h": null,
      "src/db/packed_csv_command.c": null,
      "src/test/cases/test_packed_csv_commands.c": null,
      "build.sh": "b6520796dfb4ffe876e0b326a93fed5c145406fddd9b12edb5a37aa9671b481c",
      "src/db/query_find.c": "0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a",
      "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
      "docs/concepts/packed-csv-commands.md": null,
      "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
      "docs/concepts/packed-csv-rendering.md": "5b96104d1b060f629e6574aff03ff2e30e032f134b06f9dd85dbd0350bbbba0f",
      "docs/concepts/typed-row-adapter.md": "bbdfe7d3f44c4bc27d4794fa77ea1cf6693df0ca43b3d4cea8d5b61fa000c452"
    },
    "1": {
      "src/db/packed_csv_command.h": "d81b0e468ca0b1db9ba1ee15cb7d42e15ac63ab9b5ea2180921bf1298178924d",
      "src/db/packed_csv_command.c": "918cc4a12d7739baf019abd8923403fb1254979c13132f1b0bd610d794817286",
      "src/test/cases/test_packed_csv_commands.c": "99006ee654f77b5e41f44feae7489574b2d163321d71462d90fdbd1804898c1d",
      "build.sh": "f6355ec998a85821e70da3897af243903627caa90071c101d6c194f43c3a9623",
      "src/db/query_find.c": "0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a",
      "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
      "docs/concepts/packed-csv-commands.md": null,
      "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
      "docs/concepts/packed-csv-rendering.md": "5b96104d1b060f629e6574aff03ff2e30e032f134b06f9dd85dbd0350bbbba0f",
      "docs/concepts/typed-row-adapter.md": "bbdfe7d3f44c4bc27d4794fa77ea1cf6693df0ca43b3d4cea8d5b61fa000c452"
    },
    "2": {
      "src/db/packed_csv_command.h": "d81b0e468ca0b1db9ba1ee15cb7d42e15ac63ab9b5ea2180921bf1298178924d",
      "src/db/packed_csv_command.c": "b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb",
      "src/test/cases/test_packed_csv_commands.c": "99006ee654f77b5e41f44feae7489574b2d163321d71462d90fdbd1804898c1d",
      "build.sh": "f6355ec998a85821e70da3897af243903627caa90071c101d6c194f43c3a9623",
      "src/db/query_find.c": "912588f74c20223c288aacffed15d8995e117d3db0a7398699073ac235436159",
      "src/db/storage.c": "dd0e0a9e72c9e4bcd38eda3ba1b9240ec6165d07a5c949f16af2f824087ec6cd",
      "docs/concepts/packed-csv-commands.md": null,
      "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
      "docs/concepts/packed-csv-rendering.md": "5b96104d1b060f629e6574aff03ff2e30e032f134b06f9dd85dbd0350bbbba0f",
      "docs/concepts/typed-row-adapter.md": "bbdfe7d3f44c4bc27d4794fa77ea1cf6693df0ca43b3d4cea8d5b61fa000c452"
    },
    "3": {
      "src/db/packed_csv_command.h": "d81b0e468ca0b1db9ba1ee15cb7d42e15ac63ab9b5ea2180921bf1298178924d",
      "src/db/packed_csv_command.c": "b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb",
      "src/test/cases/test_packed_csv_commands.c": "99006ee654f77b5e41f44feae7489574b2d163321d71462d90fdbd1804898c1d",
      "build.sh": "f6355ec998a85821e70da3897af243903627caa90071c101d6c194f43c3a9623",
      "src/db/query_find.c": "912588f74c20223c288aacffed15d8995e117d3db0a7398699073ac235436159",
      "src/db/storage.c": "dd0e0a9e72c9e4bcd38eda3ba1b9240ec6165d07a5c949f16af2f824087ec6cd",
      "docs/concepts/packed-csv-commands.md": "10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4",
      "src/db/slotcask.h": "59a76eb30281627e2dd50b889f3327905c7b00462e5f747105bd8f8f5b59d24c",
      "docs/concepts/packed-csv-rendering.md": "9f21d52090ea98652df6bd9a550ebf6a79ba62b411e437ca1593e673a3d086f0",
      "docs/concepts/typed-row-adapter.md": "2ab98d158cc7408d85e48ddf2cf5485098dd43eefe072cba272d96aeaf0ca24f"
    }
  },
  "blocks": {
    "HEADER": "d81b0e468ca0b1db9ba1ee15cb7d42e15ac63ab9b5ea2180921bf1298178924d",
    "STUB": "918cc4a12d7739baf019abd8923403fb1254979c13132f1b0bd610d794817286",
    "FULL": "b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb",
    "TEST": "99006ee654f77b5e41f44feae7489574b2d163321d71462d90fdbd1804898c1d",
    "PROBE": "1f431bb27eb675244712d734dda4794d532099ad9ab7e0ecb83c486499cc36bc",
    "PROBE_TOOLS": "2c98688c15564e78631dca6ab25e7bca32f64ac83073127c782d80957e6b65fa",
    "CHECKS": "59c744480052593e60482706dfef19af674087e58115227c1b8ffe780522b29c",
    "DRIVER": "faac6208dcc4e27f880ea442eb55c395791fdd95574b11b6115f1b539f293c40",
    "CONCEPT": "10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4",
    "BOOTSTRAP": "8fb88928ce49bcec90a1371a8cd60b5419f5163cb61720e2be910d9420d21a22",
    "BUILD_SOURCE_1_OLD": "8e08079f67b5d78405c9bb24b4db32dc1d7e1eeade070fdd7465a63c259e3a11",
    "BUILD_SOURCE_1_NEW": "7fa01072a2bdc9e49e275d49ffabc179064794288631cf649a9d859e449189e5",
    "BUILD_SOURCE_2_OLD": "1638f6432c997e2988c4aaba3a691396a935e4d2bda850a7abb6de723eb295ad",
    "BUILD_SOURCE_2_NEW": "44f77ef1a8e5975ff001536c93f413228a13f73d76c930a52a91669c0b252523",
    "BUILD_SOURCE_3_OLD": "a8b3faf46261da7c018ca1f35f9c3af5ccd8782049d761f7631df9555ace52f1",
    "BUILD_SOURCE_3_NEW": "601dfa0e84c4e64b297bc27f0a481e063989a896df3d1c0556eb854d0646ff91",
    "BUILD_SOURCE_4_OLD": "468cc91f3111c744be753b663a2254a2063afa9b7e5ea417170680df8775cb1f",
    "BUILD_SOURCE_4_NEW": "e120896c4fc2fba77a4c32eaabd9ce7490ee21cd3c8e472c914b32b1999b8a1e",
    "BUILD_SOURCE_5_OLD": "d1d8b7da14dac768fc2bd3b2cdd7d4f9baca8f5d0f7e41968a9ad8a4350871de",
    "BUILD_SOURCE_5_NEW": "84a09078bc95767b05ee237d4077283798b943f2e8833faf88e10c9015942df5",
    "BUILD_TEST_OLD": "a6d52b117345890ba405dd978d675c423114fdb93a9e0e7e15dc644e29918d59",
    "BUILD_TEST_NEW": "7a7ca8d50321f22c97953a5c3ce0a5256183d111c26c58c2cc454399bbd7d835",
    "INCLUDE_OLD": "542172e135ad31a086d447e84e47161cacf88c23e8b0502d8a37e5bf7c145445",
    "INCLUDE_NEW": "1727de13a58b34ecbcc12df4ed4cc212187d6bcd5a937d575f79af7747c837f5",
    "FETCH_OLD": "5649ef24b74b22d9ea7be38e4280e7e380899bd98bdc7fcf4650118edd33baf6",
    "FETCH_NEW": "433dce97ed250fdda0f9f8f1f3904af1e73d5cb2661a8f0de7833df819fdd2a3",
    "GET_OLD": "7375970debabe0224bffc3056af0a624bed9eb3d0bc921257f61ad1af1b5770a",
    "GET_NEW": "6576786ff73107911d4db9b1a52a1b4ceff5686113dd9e2641c20d9880826611",
    "SLOT_DOC_OLD": "08e4a3dcd1d0d3ccf35e4d87ca3bc1d58c8ee8d52fd21cb9e7bed42dbe3f52ed",
    "SLOT_DOC_NEW": "122ecd7c6cd55237d383de2f1f0c1d64888cfe146f0a9f0afa837655469318ea",
    "CSV_DOC_OLD": "dd771dfbf9ac38f0149f14c48a9a2f2324b1ad6cc209ce26c06695c751f1cb1c",
    "CSV_DOC_NEW": "1101696a5fa6a02421a955049a14f76020c8d073b552a33bedc6cce025055900",
    "ADAPTER_DOC_OLD": "9ff86dc260c1c3daf49b7a3cee180c31b47c830396ab3314c923d0ce7c2263be",
    "ADAPTER_DOC_NEW": "6427ed75acb942023ea306f380d13cae63d48424100ed720992bf1e0b68f7532"
  }
}
```
