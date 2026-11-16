# Phase 3B.9 — checked packed CSV cells, headers and complete rows

## Primary goal

Make exact financial values and binary strings available to CSV output without
losing decimal digits, truncating at NUL, or returning a successful partial row,
while existing applications remain on their working legacy format.

**Status: Revision 2, independently reviewed and repaired; ready for executor
handoff. This planning turn applies no root production implementation.** Planner: GPT 6.1 Sol. Intended executor: GLM 5.3,
per the user's assignment. Base is reviewed/committed Phase 3B.8
`994e2eb43497721e34cd4aecf0159a00e42c226a` on
`fix/phase1-safe-scalar-conversions`. Complete frozen edits are below; ordinary
planning compilation/probes occur only in `/tmp/p3b9-author/`, not this checkout.

## Revision 2 — independent review repairs

- SPEC-01 (P2): expose pkr_schema_validate as a pure wrapper around the codec's
  existing schema_ok rules. CSV headers now validate full structural metadata,
  not only adapter correspondence. Seven corruption cases and direct validator
  contracts are regression-tested before implementation.
- STD-01 (P2): default all-fields selection retains physical indices so a stored
  field name containing '+' is not silently reinterpreted as a composite.
  The binder accepts such internal schemas even though public field-name
  validation forbids '+'. Explicit selectors keep their composite semantics.
- Both fixes are complete frozen edits. Task 1 now touches six files, Task 2
  two, and nine final files are checkpointed. No public activation or caller
  conversion is added.

## Scope and policy proposed for approval

The user requested reversible CSV encoding, then specifically chose visible
`\u0000` instead of Base64. This plan follows that direction. CSV itself does
not interpret JSON escapes; the packed CSV byte layer is therefore explicit:

| Input bytes | Cell text before ordinary CSV quoting |
|---|---|
| Actual NUL | six visible bytes `\u0000` |
| Literal backslash | two backslashes `\\` |
| Literal six-character `\u0000` | seven-character `\\u0000`, distinct from NUL |
| Other C0 controls, including CR/LF/tab, and DEL | lowercase six-byte `\u00xx` |
| Other bytes, including UTF-8 | unchanged |

Apply ordinary CSV delimiter/quote handling after byte escaping. Decode CSV
first, then this byte layer, then the chosen text charset. Escaped `\u00xx`
denotes one original byte, not a Unicode code point converted to UTF-8. Doubling
backslashes prevents literal escape sequences from changing meaning. No magic
prefix or Base64. High bytes remain byte-preserving; no UTF-8 validation claim.

Approval also accepts two private format choices that must be documented before
activation: packed CR/LF become reversible escapes instead of legacy spaces;
new helpers accept printable ASCII delimiters except quote/backslash, plus tab.
Legacy delimiter parsing/output is unchanged. This is a deliberate private
packed contract, not a claim that ordinary CSV readers automatically unescape
the bytes or that existing CSV clients need no later compatibility note.

Add all three useful units together: an explicit-length CSV cell encoder and
complete packed header/row renderers. The row renderer consumes the existing
named display semantics, including '+' composites. This plan does not alter
void legacy emitters or silently activate them: their callers cannot propagate
errors today. Command/FILE integration needs the following emitter/error plan.

| Contract | Behavior |
|---|---|
| Ownership | owned malloc output; caller frees; authoritative length excludes final NUL; budget includes NUL |
| Failure | NULL, nonzero errno, length reset to zero; never return a shorter successful row; success need not clear errno |
| Output | cell has no LF; header/row have exactly one terminal LF; no raw C0/DEL occurs inside a cell |
| NULL/empty | missing/unknown/removed/NULL and empty field both become empty CSV cells; literal `null`, zero and false remain real text |
| Key | explicit byte span of an already formatted display key; no storage-key/UUID/sequence reinterpretation here |
| Selection | NULL/empty string: all nonremoved fields; otherwise comma tokens, preserving order/duplicates/whitespace and skipping empty tokens; `,,,` selects key only |
| Composites | existing '+' raw display concatenation, then byte escaping, then CSV quoting; skip absent components; empty composite becomes empty cell |
| Limits | MAX_LINE-1 selector bytes; MAX_FIELDS columns and nonempty components per column; E2BIG on excess |
| Validation | header validates schema metadata; row validates whole schema/view first, including unselected payloads; generation mismatch refuses before output |
| Memory | actual output plus one selected raw composite/decimal workspace and bounded validation workspaces; never schema-ceiling-sized output |
| Budget | explicit output bound; does not claim a total peak or QUERY_BUFFER_MB integration; max all-NUL 4 MiB field expands to 24 MiB |

Callers keep schema, record, key and selector storage stable for the duration
of each call; these local renderers do not introduce an internal lock.

No new production caller, runtime flag, loader binding, metadata/version,
migration, matching/sorting/grouping/index/aggregate semantics, or dependency.
No changes to legacy CSV/JSON behavior. Public packed storage remains inactive.

## Task 0 — documentation discovery and checkpoint

**How this serves the primary goal:** makes the new byte-preserving output
contract explicit without feeding binary data to existing strlen consumers.

Read AGENTS.md/CORE-PROCESS.md, the Phase 3 work index and parent recovery plan,
reviewed 3B.8 plan/evidence/reviews, concepts/typed-row-adapter.md,
packed-named-projection.md, packed-json-projection.md, typed-records.md and
query-protocol/overview.md/find.md/joins.md. The protected certification plan is
hash-only: never read/copy/edit/move/stage/incorporate/execute it.

Documentation discovery used a read-only agent plus planner verification. Allowed
APIs, verified in the actual base (one-based locations are navigation aids only;
edits below use quoted anchors):

| API/source | Allowed use / evidence |
|---|---|
| New packed_record.h/c API: pkr_schema_validate | introduced in this child as a pure wrapper of existing schema_ok; complete frozen code below, no invented pre-existing API |
| typed_row.h, typed_row.c | typed_schema_bind_packed, typed_encode_alloc, TypedPrepared, typed_row_open; real fixture/ownership contracts |
| typed_row.c named_text_open, row_text_write, packed_schema_ok | private reuse inside same TU; safe full-view/schema checks and exact raw decimal rendering |
| test_packed_named_projection.c | local schema/record fixtures and `TEST_REGISTER("name", fn)` pattern; no mutable environment or loader seam |
| query_find.c:1707/1748/1771 | existing void csv_emit_cell/header/row; strlen, incremental output and silent allocation failure are why they are not reused here |
| query_join.c:492/522 | csv_cell_to_buf/build_joined_csv_row clip to caller buffer; not a safe replacement for new complete owned output |
| query-protocol/find.md CSV output | legacy quotes delimiter/quote, doubles quotes, normalizes CR/LF to spaces; new private escape policy differs deliberately |
| query-protocol/overview.md framing | raw NUL terminates the response; emitting stored NUL literally would truncate the frame |

Root causes: current CSV routines use strlen and cannot see suffix bytes after
NUL; temporary malloc failures can silently erase cells; incremental/void writers
cannot return a row-construction error; join scratch buffers silently clip.
Using the JSON `json_escape_into` output as CSV directly would also conflate
JSON quoting with CSV quoting and inherit its int count. This implementation
keeps counters size_t and handles the two layers explicitly.

Consumer inventory, re-check at execution base:

- csv_emit_row: query.c at 1695/5100/9195, query_find.c v2_fetch_cb,
  query_join.c scan emission. Declarations in query_internal.h.
- csv_emit_header: query.c main find envelope and query_find.c cmd_fetch_v2.
- build_joined_csv_row: query.c materialized join output and query_join.c scan;
  emit_joined_csv_header: query.c; declarations in query_internal.h.
- csv_emit_cell: row/header helpers plus query_find.c keys, storage.c get-multi,
  and aggregate output throughout query_aggregate.c; declaration types.h.
- get-multi parses complete JSON again before CSV and can discard NUL lengths.
  No new CSV API is inserted there in this child.
- Raw decode_field/typed_get_field_str still also feed matching, sorting,
  grouping, cursor and joins. They are not globally changed to packed dispatch.
- New APIs have no callers before this plan, and after execution only new tests
  call them; existing production TU/caller files are checkpointed unchanged.

Confirm branch/HEAD and approved plan SHA from the handoff, staged diff empty,
and only the approved planning/index/protected artifacts pre-existing. The
protected SHA256 must equal
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Preflight local socket access before starting full-suite work. If the sandbox
blocks sockets, obtain the tool's normal execution permission before testing;
do not reinterpret daemon failures as success or silently skip cases.

## Task 1 — regression first, link stubs only

**How this serves the primary goal:** proves the tests detect absent formatting
and failure behavior before the renderer exists.

Apply stage 1 only. Six files: codec/adapter declarations and link stubs,
new test and one test-registration line. Every replacement is in MANIFEST's stage 1 with
quoted old/new blocks; driver asserts each anchor occurs exactly once. Build
with zero warnings, run test-packed-csv, then checks.py --mode red. Capture raw
output/exit codes before any stage 2 edit. RED diagnostics can contain arbitrary
non-UTF-8 expected bytes; preserve raw logs as bytes, and use backslashreplace
only for viewing them. Never change test bytes to make a logger decode.
Expected focused RED: 32 passed /
112 failed, exit 112. The runtime probe must compile and report three semantic
failures (handled expected exit 1), not a setup/link failure.

RED covers exact byte/NUL/literal-escape distinction, delimiters/quotes/control
bytes, NULL/empty, exact/one-short budgets, header/row projections, duplicates,
composites, limits, schema/generation/corruption failures, maximum numeric
digits and 4 MiB all-NUL varchar. All 256 input byte values are checked in the
committed case; the independent Python oracle later recomputes expectations.
Setup passes on stubs are incidental, not proof of missing behavior.

## Task 2 — complete checked renderers

**How this serves the primary goal:** returns an entire exact CSV cell or line,
or a clear construction error; no success-shaped truncation or omitted cell.

Apply stage 2: replace PKR_VALID_STUB with PKR_VALID_FULL and the entire
quoted CSV_STUB with CSV_FULL. This adds
private CsvSink/CsvSelection helpers, byte escaping/CSV quoting, shared checked
selection, one-cell encoding and complete two-pass header/row builders. The
module reuses the existing NamedText/RowText lifecycle; no per-record format
flag or new production source list. All code is in the frozen block.

Sizing and writing use the same functions. CsvSink checks size_t overflow and
remaining capacity. Selection/composite temporaries are freed on every path;
public output length stays zero until success. Numeric fields use dec_to_text
through the existing raw writer with space for its terminator. Header/row
selection agrees, and invalid unselected data still rejects rows.

Build warning-free, run focused GREEN 144/0, then green checks. Python's csv
module independently computes outer quoting over 607 analytic/seeded byte
samples and five delimiters (3035 cases); a separate byte decoder must recover
every input exactly. Allocation substitutions are scratch-only in typed_row.c,
packed_record.c and decnum.c; 15 injections (cell1/header2/row12) each prove
NULL/ENOMEM/length0 and zero selected live buffers. Max-schema selected output
must report actual2085/largest9/live0, not a schema-bound allocation.

Run regression-proof in scratch: reverse the complete frozen renderer
and the new codec validator back to link stubs; both hashes must match stage 1. Recompile actual source and observe
three runtime semantic failures. Also compile CSV_REVIEW_BASELINE (the exact
pre-review renderer, never a driver stage) with the current codec entry point:
its malformed-header and two default-identity gates must fail (exit1, three
failures), then the fixed renderer must pass those exact gates. This proves the
review repairs against functioning old code, not merely link stubs. Restore/hash
stage 2 and rerun all green probes. Never revert checked-out production code or weaken an assertion.

## Task 3 — docs, full suite and review hand-back

**How this serves the primary goal:** records the exact client decoding contract
and verifies existing applications retain the unchanged legacy behavior.

Apply stage 3: new concept doc and two adjacent links, with complete quoted
blocks below. Run one fresh normal full suite after all edits, then repeat green
checks and verify all nine final file hashes. Expected full suite for this base:
20603 passed / 0 failed across 527 cases. Record actual full output, not just
this forecast. Stage counts/checks are binding on this workstation; halt on a
mismatch rather than adjusting expectations or rerunning until green.

No public route acceptance, FILE atomicity, platform coverage or performance
claim follows from these helper tests. The OOM probe instruments the three
selected modules, not unrelated libc/static-library allocations; the maximum
allocation metric excludes pre-existing schema/input/record fixture storage.
Code runs ordinary gcc modes (-O0/-O1/-O2) only. No sanitizers/benchmarks in planning or
execution; branch-final sanitizer gates remain immediately before push/PR.

Create a curated execution ledger at
`docs/plans/2026-10-27-phase3b9-packed-csv-evidence.md` with identity, commands,
real RED/GREEN/full/oracle/OOM/proof output, limits and final inventory. Raw
logs, probe sources, binaries and caches remain in /tmp, never version control.
Stop unstaged/uncommitted for plan-aware and independent blind raw-diff reviews.

## Execution rules

1. Approval is for this child only. Continue the named fix branch at the exact
   base; do not create/reset/switch branches or touch earlier commits.
2. Read docs before code. Execute Tasks 0–3 in order. Do not stage, commit,
   push, PR, merge or start later work. No public packed activation.
3. If an anchor/hash/checkpoint does not match exactly, write PLAN_NOTES.md with
   actual identity, command/output, mismatch and untouched/partial state, and
   halt the entire run. No guessing, reinterpretation or further task. Resume
   only after the human/planner supplies a repaired plan.
4. Stop for any uncovered decision, unexpected warning, failure or contract
   contradiction. Do not weaken assertions, add broad fallbacks/suppressions,
   silently accept partial rows, or rerun intermittent failures until green.
5. The driver validates every pre/postimage before writes. A filesystem write
   failure can still leave partial edits: record them and halt; do not retry
   automatically or pretend this source-edit script is transactional storage.
6. Preserve all prior planning/review files and the protected hash-only rig.
   All shell commands start with rtk. Raw output may use rtk proxy.
7. The socket-capable normal full suite remains mandatory. Sanitizers only at
   the branch-final pre-push gate; benchmarks belong to the user.

## Following work and closure gates

The next emitter plan should consume these complete lines across CSV read
routes, together with surrounding query output/error propagation. Avoid another
cell-only helper plan. Inventory find/fetch/get-multi/join/keys/aggregate uses,
respect later Phase 5/6 semantics, define failure after prior CSV prefixes, and
bound enclosing request memory. Do not reinterpret this helper as completing
those routes. SDK/public compatibility notes must explain the escape layer
before activation; raw binary does not become safe by passing it to strlen.
CSV ingress/import decoding remains a required Phase 4/activation gate as well:
current bulk CSV import does not automatically undo the new byte escapes.
The oracle proves the representation reversible, not that an unchanged server
import path already round-trips it. Preserve literal legacy CSV semantics when
adding packed import/SDK decoding; documentation alone is not that integration.

Then finish the remaining 3B consumer audit/read/delete/index/maintenance/rebuild
integration, failing closed at private unsupported seams while preserving
public legacy behavior. Phase 3C covers authoritative metadata/publication and
replay/compatibility safety while still inactive. Phases 4–6 complete writes,
queries/indexes and exact aggregates before explicit activation; the remaining
parent roadmap contracts/migration/acceptance/docs gates and protected rig
remain later work. No fixed count of additional child plans is claimed here.

## Appendix A — exact command sequence

First record identity, approved SHA from the handoff and the protected hash;
inspect `rtk git status --short`, `rtk git diff --cached --name-only` and compare
against Task 0. Bootstrap only after the approved SHA matches:

```sh
rtk proxy python3 - docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md <<'PY'
import pathlib,re,sys
text=pathlib.Path(sys.argv[1]).read_text()
match=re.search(r'^### Block BOOTSTRAP\n```python\n(.*?)\n```',text,re.M|re.S)
assert match
exec(compile(match.group(1),'<approved CSV bootstrap>','exec'))
PY
```

Set shell variable P3B9_WORK to the exact emitted /tmp path (no invented path),
and preserve every command's raw output and exit status under that directory.
The following commands execute in the repository root:

```sh
rtk proxy python3 "$P3B9_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --stage 1 --check-only
rtk proxy python3 "$P3B9_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-csv
rtk proxy python3 "$P3B9_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --mode red
rtk proxy python3 "$P3B9_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-packed-csv
rtk proxy python3 "$P3B9_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --mode green
rtk proxy python3 "$P3B9_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --mode regression-proof
rtk proxy python3 "$P3B9_WORK/driver.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --stage 3
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 "$P3B9_WORK/checks.py" --plan docs/plans/2026-10-27-phase3b9-packed-csv-rendering-plan.md --mode green
rtk git diff --check
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
```

The first focused RED command exits112 intentionally; capture it and continue
only when its exact named failure mechanism matches. Do not wrap the entire
sequence in a shell that aborts or hides expected RED. Probe red/proof wrappers
handle their internal expected exit1; setup/build failure is never accepted.

## Appendix B — Revision 1 planning-time verification (historical)

Reference tree: `/tmp/p3b9-author/tree`, made from `git archive` of the exact
base; no protected/untracked file copied. Final frozen blocks are checked
against that tree and the driver's postimages before delivery. Planning uses
ordinary compilation only; no root production edit, sanitizer or benchmark.
Actual final rehearsal outputs and limitations are appended below after runs;
expected results above are not substitutes for execution evidence.

Actual final rehearsal (2026-10-27, ordinary gcc16.2.1 on Linux x86_64):

- Stub full build: exit0, zero warnings; focused RED 18/100, exit100, captured
  before renderer implementation. The final frozen test is additionally linked
  with the stubs and real TEST_BUILD runner/daemon sources at ordinary -O0:
  build exit0/zero warnings, RED again18/100, exit100. That supplemental binary
  registers only this case; it is not represented as a full-suite build.
  `/tmp/p3b9-author/red-final-command.txt` records its exact source list;
  red-final-build.log/final-red-focused.log retain the outputs.
- Final full build: exit0, zero warnings; final focused 118/0, exit0.
- Fresh final full suite after that build: 20577/0 across527 cases, exit0.
  A preliminary full run during policy construction is not used as this gate;
  `/tmp/p3b9-author/final-full.log` is the separate final run.
- Published bootstrap verifies all20 manifest block hashes (21 extracted blocks
  includes MANIFEST); real root stage1 --check-only verifies4 files, no writes.
- Driver transformation replay in a separate scratch directory matches all
  quoted anchors and checkpoint postimages: stage1 four files, stage2 one,
  stage3 three. Scratch final build inputs match the frozen stage2 hashes.
  Identity checks run on the real root in check-only mode; the scratch archive
  has no .git, so no pretend production identity check is claimed there.
- Published checks.py --mode green: three semantic gates; all15 allocation
  injections (1+2+12), zero live selected buffers; maximum-schema actual2085 /
  largest9 / live0; Python CSV/byte oracle3035/0, seed20261027.
- Published regression-proof: exact reversed stage1 hash, three semantic
  failures; restored exact stage2 hash, all green probes/oracle again.

Raw build/test logs, probes and extracted tools stay in /tmp. No root production
source edits, sanitizers, benchmarks or git writes occurred while planning.
Planning compilation caught and corrected test API/macro mistakes and stub
warning formatting before publication. Source-extracted module probes are
ordinary recompilations, not a substitute for public socket route tests. The
CSV APIs remain deliberately uncalled by production.


## Appendix C — Revision 2 verification

Revised frozen blocks are being verified in a new scratch tree. Revision 1
logs under /tmp were cleaned externally after their original capture; the
historical record above is retained, not represented as current log inspection.
Revised focused results are 144/0; the same frozen case against link stubs is
32/112, exit112, captured before implementation. Both ordinary full builds have
zero warnings. Published green checks pass all three semantic checks, three
repair checks, 15 OOM injections and 3035 oracle cases; maximum actual2085 /
largest9 / live0. Regression proof reverses both new implementations to their
exact stage1 hashes (three semantic failures), then runs the exact pre-review
renderer (three repair failures), and finally the restored renderer passes all
checks. The fresh normal full suite after the final build and docs passes
20603/0 across527 cases, exit0 (`/tmp/p3b9-r2/full.log`). Both build logs remain
warning-free. All nine final file hashes match checkpoint3; all unchanged
production boundaries match the base. The published bootstrap verifies all27
manifest block hashes (28 extracted blocks including MANIFEST), and actual-root
stage1 --check-only passes for six files without writes. The independent code
and execution reviewers rechecked both repairs with no remaining findings.
Current raw build/focused/probe/proof logs live under `/tmp/p3b9-r2/`; the
scratch tree is `/tmp/p3b9-r2-tree-9j6h54m3`. No root production edits, Git
writes, sanitizers or benchmarks occurred during review. See the separate
[review record](2026-10-27-phase3b9-plan-review.md).

## Frozen executable blocks

### Block CSV_HEADER
```c
/* Checked CSV output, private until command/error integration is complete.
   Byte layer: backslash doubles; C0 controls and DEL use \\u00xx.
   Other bytes (including UTF-8) pass through. Parse CSV before byte unescaping.
   No raw NUL/CR/LF enters a cell.
   Delimiter is printable ASCII except quote/backslash, or tab. Cell output
   has no newline; header/row include their final LF. Budget includes NUL;
   text_len excludes NUL and resets on error. All returned buffers are owned.
   NULL data is valid only at length zero. Missing/NULL fields are empty cells.
   Row key is caller-formatted display bytes, not a binary storage key.
   fields NULL/empty selects all nonremoved fields; otherwise comma parsing
   preserves order, duplicates and whitespace, skips empty tokens and supports
   '+' named composites, bounded by MAX_LINE-1/MAX_FIELDS. */
char *typed_csv_cell_text(const void *data, size_t len, char delimiter,
    size_t budget, size_t *text_len);
char *typed_packed_csv_header(const TypedSchema *ts, const char *fields,
    char delimiter, size_t budget, size_t *text_len);
char *typed_packed_csv_row(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

```

### Block CSV_STUB
```c

char *typed_csv_cell_text(const void *data,size_t len,char delimiter,size_t budget,size_t *text_len) {
    (void)data;(void)len;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}

char *typed_packed_csv_header(const TypedSchema *ts,const char *fields,char delimiter,size_t budget,size_t *text_len) {
    (void)ts;(void)fields;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}

char *typed_packed_csv_row(const TypedSchema *ts,const uint8_t *value,size_t len,uint8_t hdr2,const void *wire_key,size_t key_len,const char *fields,char delimiter,size_t budget,size_t *text_len) {
    (void)ts;(void)value;(void)len;(void)hdr2;(void)wire_key;(void)key_len;(void)fields;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}
```

### Block CSV_FULL
```c

typedef struct {
    char *data;
    size_t used, cap;
} CsvSink;
typedef struct {
    char *copy;
    const char *names[MAX_FIELDS];
    int indices[MAX_FIELDS]; /* default fields retain identity; explicit names use -1 */
    size_t count;
} CsvSelection;
static int csv_delim_ok(char delimiter) {
    unsigned char d=(unsigned char)delimiter;
    return d=='\t'||(d>=0x20 && d<=0x7e && d!='"' && d!='\\');
}
static int csv_put(CsvSink *sink,const char *data,size_t len) {
    if(len>SIZE_MAX-sink->used){errno=EOVERFLOW;return -1;}
    if(len>sink->cap-sink->used){errno=ENOMEM;return -1;}
    if(sink->data&&len)memcpy(sink->data+sink->used,data,len);
    sink->used+=len;return 0;
}
/* Literal backslash doubles; C0 controls and DEL become six-byte \\u00xx.
   Other bytes (including UTF-8) pass through. CSV quoting is the outer layer. */
static size_t csv_byte(unsigned char c,char out[6]) {
    static const char hex[]="0123456789abcdef";
    if(c=='\\'){out[0]='\\';out[1]='\\';return 2;}
    if(c<0x20||c==0x7f){
        memcpy(out,"\\u00",4);out[4]=hex[c>>4];out[5]=hex[c&15];return 6;
    }
    out[0]=(char)c;return 1;
}
static int csv_span(CsvSink *sink,const void *bytes,size_t len,char delimiter) {
    const unsigned char *data=bytes;int quoted=0;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++)if(token[j]==delimiter||token[j]=='"')quoted=1;
    }
    if(quoted&&csv_put(sink,"\"",1))return -1;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++){
            if(token[j]=='"'&&csv_put(sink,"\"",1))return -1;
            if(csv_put(sink,token+j,1))return -1;
        }
    }
    return quoted?csv_put(sink,"\"",1):0;
}
static int csv_components_ok(const char *name) {
    size_t count=0;const char *p=name;
    while(*p){
        while(*p=='+')p++;
        if(!*p)break;
        if(++count>MAX_FIELDS){errno=E2BIG;return 0;}
        while(*p&&*p!='+')p++;
    }
    return 1;
}
static int csv_selection_open(const TypedSchema *ts,const char *fields,
                              CsvSelection *selection) {
    memset(selection,0,sizeof(*selection));
    if(!fields||!*fields){
        for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed){
            selection->names[selection->count]=ts->fields[i].name;
            selection->indices[selection->count++]=i;
        }
        return 0;
    }
    size_t n=strnlen(fields,MAX_LINE);
    if(n==MAX_LINE){errno=E2BIG;return -1;}
    selection->copy=malloc(n+1);
    if(!selection->copy){errno=ENOMEM;return -1;}
    memcpy(selection->copy,fields,n+1);char *save=NULL;
    for(char *name=strtok_r(selection->copy,",",&save);name;
        name=strtok_r(NULL,",",&save)){
        if(selection->count==MAX_FIELDS){errno=E2BIG;return -1;}
        if(!csv_components_ok(name))return -1;
        selection->names[selection->count]=name;
        selection->indices[selection->count++]=-1;
    }
    return 0;
}
static int csv_selected_text_open(const TypedRowView *view,const char *name,
                                   int index,size_t budget,NamedText *text) {
    if(index<0)return named_text_open(view,name,budget,text);
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    TypedRowSpan span;if(typed_row_span(view,(size_t)index,&span))return -1;
    text->missing=span.is_null||view->schema->fields[index].removed;
    return row_text_open(view,(size_t)index,&text->value);
}
static int csv_selected_span(CsvSink *sink,const TypedRowView *view,
    const char *name,int index,char delimiter,size_t budget) {
    NamedText text;int rc=csv_selected_text_open(view,name,index,budget,&text);
    char *number=NULL;
    if(rc)goto done;
    if(text.missing){rc=csv_span(sink,NULL,0,delimiter);goto done;}
    const char *data=text.value.data;size_t n=text.value.len;
    if(n==SIZE_MAX||n+1>budget){errno=ENOMEM;rc=-1;goto done;}
    if(text.value.numeric){
        number=malloc(n+1);if(!number){errno=ENOMEM;rc=-1;goto done;}
        size_t used=0;rc=row_text_write(&text.value,0,number,n+1,&used);
        if(rc)goto done;
        if(used!=n){errno=EILSEQ;rc=-1;goto done;}data=number;
    }
    rc=csv_span(sink,data,n,delimiter);
done:free(number);named_text_free(&text);return rc;
}
char *typed_csv_cell_text(const void *data,size_t len,char delimiter,
                          size_t budget,size_t *text_len) {
    if(text_len)*text_len=0;
    if(!text_len||(!data&&len)||!csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    CsvSink measure={.cap=budget-1};
    if(csv_span(&measure,data,len,delimiter))return NULL;
    char *out=malloc(measure.used+1);if(!out){errno=ENOMEM;return NULL;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_span(&write,data,len,delimiter)){free(out);return NULL;}
    if(write.used!=measure.used){errno=EILSEQ;free(out);return NULL;}
    out[write.used]=0;*text_len=write.used;return out;
}
static int csv_record_pass(CsvSink *sink,const CsvSelection *selection,
                            const TypedRowView *view,const void *key,size_t key_len,
                            char delimiter,size_t budget,int header) {
    if(csv_span(sink,header?"key":key,header?3:key_len,delimiter))return -1;
    for(size_t i=0;i<selection->count;i++){
        if(csv_put(sink,&delimiter,1))return -1;
        if(header){
            const char *name=selection->names[i];
            if(csv_span(sink,name,strlen(name),delimiter))return -1;
        }else if(csv_selected_span(sink,view,selection->names[i],selection->indices[i],delimiter,budget))return -1;
    }
    return csv_put(sink,"\n",1);
}
static char *csv_record_text(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len,int header) {
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||(!key&&key_len)||
       !csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    if(header&&pkr_schema_validate(&ts->packed)!=DEC_OK){errno=EINVAL;return NULL;}
    TypedRowView view={0};
    if(!header&&typed_row_open(ts,value,len,hdr2,&view))return NULL;
    CsvSelection selection;char *out=NULL;
    if(csv_selection_open(ts,fields,&selection))goto done;
    CsvSink measure={.cap=budget-1};
    if(csv_record_pass(&measure,&selection,&view,key,key_len,delimiter,budget,header))goto done;
    out=malloc(measure.used+1);if(!out){errno=ENOMEM;goto done;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_record_pass(&write,&selection,&view,key,key_len,delimiter,budget,header))goto fail;
    if(write.used!=measure.used){errno=EILSEQ;goto fail;}
    out[write.used]=0;*text_len=write.used;goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
char *typed_packed_csv_header(const TypedSchema *ts,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,NULL,0,0,NULL,0,fields,delimiter,budget,text_len,1);
}
char *typed_packed_csv_row(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *wire_key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,value,len,hdr2,wire_key,key_len,fields,delimiter,budget,text_len,0);
}
```

### Block TEST
```c
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
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
        if(i<32||i==127)pos+=(size_t)snprintf(expected+pos,sizeof(expected)-pos,"\\u00%02x",i);
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
```

### Block PROBE
```c
#define _XOPEN_SOURCE 700
#include "typed_row.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
static void *owned[4096];static size_t live,calls,fail_at,largest;static int armed;
static void remember(void *p,size_t n){
    if(!p)return;
    for(size_t i=0;i<4096;i++)if(!owned[i]){owned[i]=p;live++;if(n>largest)largest=n;return;}
    abort();
}
static void forget(void *p){for(size_t i=0;i<4096;i++)if(p&&owned[i]==p){owned[i]=NULL;live--;return;}}
void *p9_malloc(size_t n){if(armed&&++calls==fail_at){errno=ENOMEM;return NULL;}void *p=malloc(n);if(armed)remember(p,n);return p;}
void *p9_calloc(size_t n,size_t m){if(armed&&++calls==fail_at){errno=ENOMEM;return NULL;}void *p=calloc(n,m);if(armed)remember(p,n*m);return p;}
void *p9_realloc(void *old,size_t n){
    if(armed&&++calls==fail_at){errno=ENOMEM;return NULL;}
    size_t index=4096;for(size_t i=0;i<4096;i++)if(old&&owned[i]==old){index=i;break;}
    void *p=realloc(old,n);if(!p)return NULL;
    if(index<4096){owned[index]=NULL;live--;}
    if(armed)remember(p,n);return p;
}
void p9_free(void *p){forget(p);free(p);}
static char *invoke(int mode,TypedSchema *ts,const uint8_t *record,size_t n,size_t *used){
    const char raw[]={0,'\\','"',',',0xff};
    if(mode==0)return typed_csv_cell_text(raw,sizeof(raw),',',SIZE_MAX,used);
    if(mode==1)return typed_packed_csv_header(ts,"f0,f1+f2,missing",',',SIZE_MAX,used);
    return typed_packed_csv_row(ts,record,n,PKR_FLAGS,"k",1,"f0,f1+f2,missing",',',SIZE_MAX,used);
}
static int oom(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return 2;
    ts->typed=1;ts->nfields=3;const char *types[]={"numeric","varchar","int"};
    for(int i=0;i<3;i++){ts->fields[i].type=i==0?FT_NUMERIC:i==1?FT_VARCHAR:FT_INT;snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    parse_field_type("int",&ts->fields[2]);
    uint8_t *record=NULL;size_t n=0;
    if(typed_schema_bind_packed(ts,types,3,NULL,0)||typed_encode_alloc(ts,"{\"f0\":\"123.4500\",\"f1\":\"a\\u0000z\",\"f2\":0}",1,NULL,NULL,&record,&n,NULL,0)){free(ts);return 2;}
    size_t injections=0;
    for(int mode=0;mode<3;mode++){
        armed=1;calls=fail_at=largest=0;size_t used=99;char *out=invoke(mode,ts,record,n,&used);
        if(!out||!used){fprintf(stderr,"setup mode %d\n",mode);return 2;}
        size_t total=calls;p9_free(out);if(live)return 1;
        for(size_t i=1;i<=total;i++){
            calls=0;fail_at=i;used=99;errno=0;out=invoke(mode,ts,record,n,&used);
            int ok=!out&&errno==ENOMEM&&used==0;p9_free(out);
            if(!ok||live){fprintf(stderr,"OOM mode=%d ordinal=%zu errno=%d len=%zu live=%zu\n",mode,i,errno,used,live);return 1;}injections++;
        }
        printf("mode=%d injections=%zu\n",mode,total);armed=0;
    }
    p9_free(record);free(ts);printf("OOM injections=%zu live=%zu\n",injections,live);return 0;
}
static int semantics(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return 2;
    ts->typed=1;ts->nfields=3;const char *types[]={"numeric","varchar","int"};
    for(int i=0;i<3;i++){ts->fields[i].type=i==0?FT_NUMERIC:i==1?FT_VARCHAR:FT_INT;snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    parse_field_type("int",&ts->fields[2]);
    uint8_t *record=NULL;size_t n=0;
    if(typed_schema_bind_packed(ts,types,3,NULL,0)||typed_encode_alloc(ts,"{\"f0\":\"123.4500\",\"f1\":\"a\\u0000z\",\"f2\":0}",1,NULL,NULL,&record,&n,NULL,0)){free(ts);return 2;}
    int failed=0;
    for(int mode=0;mode<3;mode++){
        size_t used=99;char *out=mode==0?typed_csv_cell_text("a\0b",3,',',SIZE_MAX,&used):invoke(mode,ts,record,n,&used);
        const char *want=mode==0?"a\\u0000b":mode==1?"key,f0,f1+f2,missing\n":"k,123.4500,a\\u0000z0,\n";
        int ok=out&&used==strlen(want)&&!strcmp(out,want);
        printf("semantics mode=%d %s\n",mode,ok?"PASS":"FAIL");if(!ok)failed++;
        free(out);
    }
    p9_free(record);free(ts);printf("semantics failed=%d\n",failed);return failed?1:0;
}
static int repairs(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return 2;
    ts->typed=1;ts->nfields=3;const char *types[]={"numeric","varchar","int"};
    for(int i=0;i<3;i++){ts->fields[i].type=i==0?FT_NUMERIC:i==1?FT_VARCHAR:FT_INT;snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=2;}
    parse_field_type("int",&ts->fields[2]);
    uint8_t *record=NULL;size_t n=0;
    if(typed_schema_bind_packed(ts,types,3,NULL,0)||typed_encode_alloc(ts,"{\"f0\":\"123.4500\",\"f1\":\"a\\u0000z\",\"f2\":0}",1,NULL,NULL,&record,&n,NULL,0)){free(ts);return 2;}
    int failed=0;size_t used=99;PkrKind saved=ts->packed.fields[1].kind;
    ts->packed.fields[1].kind=(PkrKind)99;errno=0;
    char *out=typed_packed_csv_header(ts,"f0",',',SIZE_MAX,&used);
    int ok=!out&&errno==EINVAL&&used==0;
    printf("repair header metadata %s\n",ok?"PASS":"FAIL");if(!ok)failed++;free(out);
    ts->packed.fields[1].kind=saved;
    strcpy(ts->fields[1].name,"f0+f2");ts->fields[1].name_len=5;
    for(int empty=0;empty<2;empty++){
        out=typed_packed_csv_row(ts,record,n,PKR_FLAGS,"k",1,empty?"":NULL,',',SIZE_MAX,&used);
        const char *want="k,123.4500,a\\u0000z,0\n";
        ok=out&&used==strlen(want)&&!strcmp(out,want);
        printf("repair default identity %d %s\n",empty,ok?"PASS":"FAIL");if(!ok)failed++;free(out);
    }
    p9_free(record);free(ts);printf("repair failures=%d\n",failed);return failed?1:0;
}
static int maximum(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts)return 2;
    ts->typed=1;ts->nfields=MAX_FIELDS;const char *types[MAX_FIELDS];
    for(int i=0;i<MAX_FIELDS;i++){types[i]="varchar";ts->fields[i].type=FT_VARCHAR;snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=(int)strlen(ts->fields[i].name);}
    uint8_t *record=NULL;size_t n=0;
    if(typed_schema_bind_packed(ts,types,MAX_FIELDS,NULL,0)||typed_encode_alloc(ts,"{\"f127\":\"tiny\"}",1,NULL,NULL,&record,&n,NULL,0)){free(ts);return 2;}
    armed=1;calls=fail_at=largest=0;size_t used=99;
    char *out=typed_packed_csv_row(ts,record,n,PKR_FLAGS,"k",1,"f0,f127",',',10,&used);
    int ok=out&&!strcmp(out,"k,,tiny\n")&&used==8&&n<3000&&largest<4096;
    p9_free(out);ok=ok&&!live;armed=0;
    printf("maximum actual=%zu largest=%zu live=%zu %s\n",n,largest,live,ok?"PASS":"FAIL");
    p9_free(record);free(ts);return ok?0:1;
}
static int nibble(char c){return c<='9'?c-'0':c-'a'+10;}
int main(int argc,char **argv){
    if(argc==2&&!strcmp(argv[1],"semantics"))return semantics();
    if(argc==2&&!strcmp(argv[1],"repairs"))return repairs();
    if(argc==2&&!strcmp(argv[1],"oom"))return oom();
    if(argc==2&&!strcmp(argv[1],"maximum"))return maximum();
    char *line=NULL;size_t cap=0;ssize_t got;
    while((got=getline(&line,&cap,stdin))>0){
        if(got<4)return 2;char delimiter=(char)((nibble(line[0])<<4)|nibble(line[1]));
        char *hex=line+3;size_t count=strcspn(hex,"\r\n");
        if(count==1&&hex[0]=='-')count=0;
        if(count%2)return 2;size_t n=count/2;unsigned char *raw=malloc(n?n:1);if(!raw)return 2;
        for(size_t i=0;i<n;i++)raw[i]=(unsigned char)((nibble(hex[2*i])<<4)|nibble(hex[2*i+1]));
        size_t used=99;char *out=typed_csv_cell_text(raw,n,delimiter,SIZE_MAX,&used);free(raw);
        if(!out)return 2;
        for(size_t i=0;i<used;i++)printf("%02x",(unsigned char)out[i]);putchar('\n');free(out);
    }
    free(line);return 0;
}
```

### Block ORACLE
```python
import csv,io,random,subprocess,sys
probe=sys.argv[1]
rng=random.Random(20261027)
samples=[b'',b'\0',b'\\u0000',b'\0\\u0000',bytes(range(256)),b'a,"b\r\n',b'caf\xc3\xa9']
samples += [bytes(rng.randrange(256) for _ in range(rng.randrange(513))) for _ in range(600)]
cases=[(d,b) for d in [b',',b'|',b'\t',b' ',b'u'] for b in samples]
request=''.join(d.hex()+' '+(b.hex() or '-')+'\n' for d,b in cases)
run=subprocess.run([probe],input=request,text=True,capture_output=True,check=True)
lines=run.stdout.splitlines()
assert len(lines)==len(cases),(len(lines),len(cases))
def decode(cell):
    out=bytearray();i=0
    while i<len(cell):
        if cell[i]!='\\':out.append(ord(cell[i]));i+=1
        elif cell[i+1]=='\\':out.append(92);i+=2
        else:
            assert cell[i:i+4]=='\\u00'
            out.append(int(cell[i+4:i+6],16));i+=6
    return bytes(out)
for i,((delim,raw),line) in enumerate(zip(cases,lines)):
    emitted=bytes.fromhex(line).decode('latin1')
    inner=''.join('\\\\' if b==92 else '\\u00%02x'%b if b<32 or b==127 else chr(b) for b in raw)
    writer=io.StringIO(newline='')
    csv.writer(writer,delimiter=delim.decode(),lineterminator='\n').writerow([inner])
    expected=writer.getvalue()[:-1]
    # Python's writer quotes its lone empty field; this API deliberately uses
    # the existing empty-cell convention. Parse with a sentinel second column.
    if not inner:expected=''
    assert emitted==expected,(i,raw,emitted,expected)
    parsed=next(csv.reader(io.StringIO(emitted+delim.decode()+'sentinel\n'),delimiter=delim.decode()))
    assert len(parsed)==2 and parsed[1]=='sentinel'
    assert decode(parsed[0])==raw,(i,raw,parsed[0])
print('Python CSV/byte oracle: %d passed, 0 failed; seed=20261027'%len(cases))
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

### Block CHECKS
```python
import argparse,importlib.util,pathlib,subprocess,tempfile
def run(command,expect=0,**kwargs):
    print('+',' '.join(map(str,command)),flush=True)
    result=subprocess.run(command,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,**kwargs)
    print(result.stdout,end='',flush=True)
    assert result.returncode==expect,(command,result.returncode,expect)
    return result.stdout
def compile_probe(root,work,source,blocks,pkr_source=None):
    (work/'typed_row.c').write_text(source)
    if pkr_source is not None:(work/'packed_record.c').write_text(pkr_source)
    (work/'probe.c').write_text(blocks['PROBE'])
    flags=['gcc','-O1','-g','-I'+str(root/'src/db')]
    wrap=['-D'+name+'=p9_'+name for name in ['malloc','calloc','realloc','free']]
    objects=[]
    for name in ['typed_row','packed_record','decnum']:
        src=work/'typed_row.c' if name=='typed_row' else work/'packed_record.c' if name=='packed_record' and pkr_source is not None else root/'src/db'/f'{name}.c'
        obj=work/f'{name}.o';run(flags+wrap+['-c',str(src),'-o',str(obj)]);objects.append(str(obj))
    probe=work/'probe'
    run(flags+[str(work/'probe.c')]+objects+[str(root/'build/bin/libshard-db.a'),'-lpthread','-lssl','-lcrypto','-lm','-o',str(probe)])
    return probe
def green(probe,work,blocks):
    text=run([str(probe),'semantics']);assert 'semantics failed=0' in text
    text=run([str(probe),'repairs']);assert 'repair failures=0' in text
    text=run([str(probe),'oom']);assert 'OOM injections=15 live=0' in text
    text=run([str(probe),'maximum']);assert 'maximum actual=2085 largest=9 live=0 PASS' in text
    oracle=work/'oracle.py';oracle.write_text(blocks['ORACLE'])
    text=run(['python3',str(oracle),str(probe)]);assert '3035 passed, 0 failed' in text
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',type=pathlib.Path,required=True)
    parser.add_argument('--mode',choices=['red','green','regression-proof'],required=True);args=parser.parse_args()
    spec=importlib.util.spec_from_file_location('p9driver',pathlib.Path(__file__).with_name('driver.py'))
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    blocks,manifest=driver.read_plan(args.plan);root=pathlib.Path.cwd()
    for name,digest in manifest['unchanged'].items():
        assert driver.sha((root/name).read_bytes())==digest,('boundary',name)
    checkpoint=1 if args.mode=='red' else 2
    for name,digest in manifest['checkpoints'][str(checkpoint)].items():
        if name.startswith('src/') or name=='build.sh':
            path=root/name;assert (driver.sha(path.read_bytes()) if path.exists() else None)==digest,('source',name)
    source=(root/'src/db/typed_row.c').read_text();work=pathlib.Path(tempfile.mkdtemp(prefix='p3b9-check-'));print('scratch:',work,flush=True)
    if args.mode=='regression-proof':
        assert source.count(blocks['CSV_FULL'])==1
        before=source.replace(blocks['CSV_FULL'],blocks['CSV_STUB'],1)
        assert driver.sha(before.encode())==manifest['checkpoints']['1']['src/db/typed_row.c']
        codec=(root/'src/db/packed_record.c').read_text()
        assert codec.count(blocks['PKR_VALID_FULL'])==1
        codec_before=codec.replace(blocks['PKR_VALID_FULL'],blocks['PKR_VALID_STUB'],1)
        assert driver.sha(codec_before.encode())==manifest['checkpoints']['1']['src/db/packed_record.c']
        probe=compile_probe(root,work,before,blocks,codec_before)
        text=run([str(probe),'semantics'],expect=1);assert 'semantics failed=3' in text
        restored=before.replace(blocks['CSV_STUB'],blocks['CSV_FULL'],1)
        assert restored==source
        codec_restored=codec_before.replace(blocks['PKR_VALID_STUB'],blocks['PKR_VALID_FULL'],1)
        assert codec_restored==codec
        assert restored.count(blocks['CSV_FULL'])==1
        review_before=restored.replace(blocks['CSV_FULL'],blocks['CSV_REVIEW_BASELINE'],1)
        probe=compile_probe(root,work,review_before,blocks,codec_restored)
        text=run([str(probe),'repairs'],expect=1);assert 'repair failures=3' in text
        probe=compile_probe(root,work,restored,blocks,codec_restored);green(probe,work,blocks)
        print('scratch reversal/reapply hashes verified; checkout untouched')
    else:
        probe=compile_probe(root,work,source,blocks)
        if args.mode=='red':
            text=run([str(probe),'semantics'],expect=1);assert 'semantics failed=3' in text
        else:green(probe,work,blocks)
if __name__=='__main__':main()
```

### Block CONCEPT
```text
# Checked packed CSV rendering (public format inactive)

`typed_csv_cell_text` converts an explicit byte span into one owned CSV cell.
`typed_packed_csv_header` and `typed_packed_csv_row` return complete owned lines,
including LF. Every API returns an authoritative length excluding its final NUL;
the supplied budget includes that NUL. Failure returns NULL, nonzero errno and
length zero. NULL data/key with positive length is invalid; zero-length spans
are valid. These helpers do not write to FILE or activate a public route.

CSV has no built-in JSON unescaping. This private packed contract therefore
defines a byte escape layer inside ordinary CSV quoting:

- NUL becomes the six visible bytes `\u0000`.
- Other C0 controls and DEL become lowercase `\u00xx`, including CR/LF/tab.
- A literal backslash becomes `\\`, so literal `\u0000` differs from NUL.
- Other bytes pass through unchanged, preserving readable UTF-8. Arbitrary
  non-UTF-8 bytes are still bytes; this layer makes no charset validation claim.
- After byte escaping, a cell containing the delimiter or quote is surrounded
  by quotes, and each internal quote doubles. Decode ordinary CSV first, then
  byte-unescape `\\` and `\u00xx`, then interpret text in its chosen charset.
  This is byte escaping, not a general JSON/Unicode-codepoint decoder.

The escape layer is reversible for every byte value. It needs neither Base64
nor a magic prefix. Empty string and missing/NULL field both remain empty CSV
cells. The literal text `null` remains text. Packed rows preserve embedded CR/LF
through escapes; legacy CSV still normalizes CR/LF to spaces. Existing clients
see visible escape text unless they opt into the documented unescape step.
Public activation must document this format difference and SDK behavior.

Delimiters are printable ASCII except quote/backslash, plus tab. Invalid
delimiters reject EINVAL rather than producing ambiguous output. This does not
change the existing public `parse_csv_delim` or legacy CSV grammar.

Header and row selection agree: NULL/empty selector means all nonremoved fields;
nonempty comma lists preserve order, duplicates and whitespace and skip empty
tokens. Unknown, removed and NULL selections produce empty cells. '+' joins raw
display bytes before escaping; numeric components retain every decimal digit.
Selector limits are MAX_LINE-1 bytes, MAX_FIELDS columns and MAX_FIELDS nonempty
components per column. Oversized input rejects E2BIG. Row keys must already be
formatted display bytes (UUID/sequence formatting stays with the caller).

Rows validate the complete schema/packed view before projection, including
unselected payloads. Generation mismatch/corruption rejects; header validation
checks schema metadata but has no record to validate. No descriptor is treated
as a legacy scalar. Helpers build a complete line before returning it; this is
not an atomicity guarantee for an enclosing response or FILE write.

Memory is actual encoded output plus one selected raw composite/decimal
workspace at a time and bounded validation workspaces. Sizing and writing each
construct and release that workspace. The output budget does not bound total
peak memory; no QUERY_BUFFER_MB integration or performance improvement is
claimed. A 4 MiB all-NUL varchar expands to 24 MiB; a one-field row with key `k`
needs 25,165,828 bytes including separators, LF and NUL. The >1 GiB maximum
schema does not imply a >1 GiB rendering allocation for small selected values.

No production caller uses these new APIs yet. Legacy cell/header/row writers,
CSV find/fetch/get-multi/joins/aggregates/keys, query error propagation and
matching/sort/group/index consumers remain unchanged. A following consumer
integration plan must use owned complete output, carry allocation/conversion/
I/O failure to the command result, avoid treating errors as empty cells, define
errors after any previously accepted CSV prefix, and enforce request memory
limits before public packed activation. Loaders/create-object remain inactive.

Default all-fields output retains physical field indices rather than parsing
stored names as expressions. Explicit selectors still interpret '+'. Header
metadata validation uses the codec's pure pkr_schema_validate entry point;
unknown kinds, bad directory/prefix/offsets, declaration limits and payload
bounds reject even if the malformed field is not selected.
```

### Block HEADER_ANCHOR
```text
/* Existing scalar semantics, shared internally by the adapter. */
```

### Block BUILD_OLD
```text
    src/test/cases/test_packed_named_projection.c \
```

### Block SOURCE_ANCHOR
```text
    out[pos++]='}';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    free(copy);return out;
fail:free(out);out=NULL;
done:free(copy);return out;
}
```

### Block ADAPTER_OLD
```text
inactive in public loaders and creates.
```

### Block NAMED_OLD
```text
public packed network/query acceptance or concurrency safety of activation.
```

### Block HEADER_NEW
```text
/* Checked CSV output, private until command/error integration is complete.
   Byte layer: backslash doubles; C0 controls and DEL use \\u00xx.
   Other bytes (including UTF-8) pass through. Parse CSV before byte unescaping.
   No raw NUL/CR/LF enters a cell.
   Delimiter is printable ASCII except quote/backslash, or tab. Cell output
   has no newline; header/row include their final LF. Budget includes NUL;
   text_len excludes NUL and resets on error. All returned buffers are owned.
   NULL data is valid only at length zero. Missing/NULL fields are empty cells.
   Row key is caller-formatted display bytes, not a binary storage key.
   fields NULL/empty selects all nonremoved fields; otherwise comma parsing
   preserves order, duplicates and whitespace, skips empty tokens and supports
   '+' named composites, bounded by MAX_LINE-1/MAX_FIELDS. */
char *typed_csv_cell_text(const void *data, size_t len, char delimiter,
    size_t budget, size_t *text_len);
char *typed_packed_csv_header(const TypedSchema *ts, const char *fields,
    char delimiter, size_t budget, size_t *text_len);
char *typed_packed_csv_row(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
```

### Block BUILD_NEW
```text
    src/test/cases/test_packed_named_projection.c \
    src/test/cases/test_packed_csv.c \
```

### Block SOURCE_STUB
```text
    out[pos++]='}';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    free(copy);return out;
fail:free(out);out=NULL;
done:free(copy);return out;
}

char *typed_csv_cell_text(const void *data,size_t len,char delimiter,size_t budget,size_t *text_len) {
    (void)data;(void)len;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}

char *typed_packed_csv_header(const TypedSchema *ts,const char *fields,char delimiter,size_t budget,size_t *text_len) {
    (void)ts;(void)fields;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}

char *typed_packed_csv_row(const TypedSchema *ts,const uint8_t *value,size_t len,uint8_t hdr2,const void *wire_key,size_t key_len,const char *fields,char delimiter,size_t budget,size_t *text_len) {
    (void)ts;(void)value;(void)len;(void)hdr2;(void)wire_key;(void)key_len;(void)fields;(void)delimiter;(void)budget;
    if(text_len)*text_len=0;
    errno=ENOTSUP;return NULL;
}
```

### Block ADAPTER_NEW
```text
inactive in public loaders and creates.

[Checked packed CSV rendering](packed-csv-rendering.md) supplies owned exact
cells, headers and rows with reversible visible byte escapes. These APIs remain
private; legacy CSV writers and command error propagation are not converted.
```

### Block NAMED_NEW
```text
public packed network/query acceptance or concurrency safety of activation.

The [checked CSV rendering layer](packed-csv-rendering.md) consumes the same
named display semantics internally, preserving NUL through visible `\u0000`.
Its explicit-length cell/header/row APIs have no public command callers yet.
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
work=pathlib.Path(tempfile.mkdtemp(prefix='p3b9-exec-'))
for name,file in [('DRIVER','driver.py'),('CHECKS','checks.py')]:
    (work/file).write_text(blocks[name])
print('P3B9_WORK='+str(work))
```

### Block PKR_SCHEMA_ANCHOR
```text
static int schema_ok(const PkrSchema *s) {
    if (!s || !s->nfields || s->nfields > PKR_MAX_FIELDS ||
        s->prefix != 1 + (s->nfields + 7) / 8) return 0;
    uint64_t dir = 0, payload = 0;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        if (!field_ok(f) || f->offset != dir) return 0;
        dir += f->width; payload += f->payload_max;
    }
    return dir == s->directory && s->prefix + dir + payload <= INT32_MAX &&
           s->maximum == s->prefix + dir + payload;
}
```

### Block PKR_VALID_STUB
```c

dec_status pkr_schema_validate(const PkrSchema *schema) {
    (void)schema;return DEC_ERR_INVALID;
}
```

### Block PKR_VALID_FULL
```c

dec_status pkr_schema_validate(const PkrSchema *schema) {
    return schema_ok(schema)?DEC_OK:DEC_ERR_INVALID;
}
```

### Block PKR_SCHEMA_STUB
```text
static int schema_ok(const PkrSchema *s) {
    if (!s || !s->nfields || s->nfields > PKR_MAX_FIELDS ||
        s->prefix != 1 + (s->nfields + 7) / 8) return 0;
    uint64_t dir = 0, payload = 0;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        if (!field_ok(f) || f->offset != dir) return 0;
        dir += f->width; payload += f->payload_max;
    }
    return dir == s->directory && s->prefix + dir + payload <= INT32_MAX &&
           s->maximum == s->prefix + dir + payload;
}

dec_status pkr_schema_validate(const PkrSchema *schema) {
    (void)schema;return DEC_ERR_INVALID;
}
```

### Block PKR_HEADER_ANCHOR
```text
/* Maximum slot bound and actual-row segment gate. Both include existing
 * 24-byte header, actual key bytes, align-8 and floor 32. No cap is changed.
 * Requests/queries remain separately bounded by their existing callers. */
dec_status pkr_slot_bound(const PkrSchema *schema, size_t max_key, size_t *out);
```

### Block PKR_HEADER_NEW
```text
/* Pure structural metadata check; no allocation. Returns DEC_OK or
 * DEC_ERR_INVALID, including for NULL. Does not inspect any record bytes. */
dec_status pkr_schema_validate(const PkrSchema *schema);
/* Maximum slot bound and actual-row segment gate. Both include existing
 * 24-byte header, actual key bytes, align-8 and floor 32. No cap is changed.
 * Requests/queries remain separately bounded by their existing callers. */
dec_status pkr_slot_bound(const PkrSchema *schema, size_t max_key, size_t *out);
```

### Block CSV_REVIEW_BASELINE
```text

typedef struct {
    char *data;
    size_t used, cap;
} CsvSink;
typedef struct {
    char *copy;
    const char *names[MAX_FIELDS];
    size_t count;
} CsvSelection;
static int csv_delim_ok(char delimiter) {
    unsigned char d=(unsigned char)delimiter;
    return d=='\t'||(d>=0x20 && d<=0x7e && d!='"' && d!='\\');
}
static int csv_put(CsvSink *sink,const char *data,size_t len) {
    if(len>SIZE_MAX-sink->used){errno=EOVERFLOW;return -1;}
    if(len>sink->cap-sink->used){errno=ENOMEM;return -1;}
    if(sink->data&&len)memcpy(sink->data+sink->used,data,len);
    sink->used+=len;return 0;
}
/* Literal backslash doubles; C0 controls and DEL become six-byte \\u00xx.
   Other bytes (including UTF-8) pass through. CSV quoting is the outer layer. */
static size_t csv_byte(unsigned char c,char out[6]) {
    static const char hex[]="0123456789abcdef";
    if(c=='\\'){out[0]='\\';out[1]='\\';return 2;}
    if(c<0x20||c==0x7f){
        memcpy(out,"\\u00",4);out[4]=hex[c>>4];out[5]=hex[c&15];return 6;
    }
    out[0]=(char)c;return 1;
}
static int csv_span(CsvSink *sink,const void *bytes,size_t len,char delimiter) {
    const unsigned char *data=bytes;int quoted=0;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++)if(token[j]==delimiter||token[j]=='"')quoted=1;
    }
    if(quoted&&csv_put(sink,"\"",1))return -1;
    for(size_t i=0;i<len;i++){
        char token[6];size_t n=csv_byte(data[i],token);
        for(size_t j=0;j<n;j++){
            if(token[j]=='"'&&csv_put(sink,"\"",1))return -1;
            if(csv_put(sink,token+j,1))return -1;
        }
    }
    return quoted?csv_put(sink,"\"",1):0;
}
static int csv_components_ok(const char *name) {
    size_t count=0;const char *p=name;
    while(*p){
        while(*p=='+')p++;
        if(!*p)break;
        if(++count>MAX_FIELDS){errno=E2BIG;return 0;}
        while(*p&&*p!='+')p++;
    }
    return 1;
}
static int csv_selection_open(const TypedSchema *ts,const char *fields,
                              CsvSelection *selection) {
    memset(selection,0,sizeof(*selection));
    if(!fields||!*fields){
        for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed)
            selection->names[selection->count++]=ts->fields[i].name;
        return 0;
    }
    size_t n=strnlen(fields,MAX_LINE);
    if(n==MAX_LINE){errno=E2BIG;return -1;}
    selection->copy=malloc(n+1);
    if(!selection->copy){errno=ENOMEM;return -1;}
    memcpy(selection->copy,fields,n+1);char *save=NULL;
    for(char *name=strtok_r(selection->copy,",",&save);name;
        name=strtok_r(NULL,",",&save)){
        if(selection->count==MAX_FIELDS){errno=E2BIG;return -1;}
        if(!csv_components_ok(name))return -1;
        selection->names[selection->count++]=name;
    }
    return 0;
}
static int csv_named_span(CsvSink *sink,const TypedRowView *view,
                           const char *name,char delimiter,size_t budget) {
    NamedText text;int rc=named_text_open(view,name,budget,&text);
    char *number=NULL;
    if(rc)goto done;
    if(text.missing){rc=csv_span(sink,NULL,0,delimiter);goto done;}
    const char *data=text.value.data;size_t n=text.value.len;
    if(n==SIZE_MAX||n+1>budget){errno=ENOMEM;rc=-1;goto done;}
    if(text.value.numeric){
        number=malloc(n+1);if(!number){errno=ENOMEM;rc=-1;goto done;}
        size_t used=0;rc=row_text_write(&text.value,0,number,n+1,&used);
        if(rc)goto done;
        if(used!=n){errno=EILSEQ;rc=-1;goto done;}data=number;
    }
    rc=csv_span(sink,data,n,delimiter);
done:free(number);named_text_free(&text);return rc;
}
char *typed_csv_cell_text(const void *data,size_t len,char delimiter,
                          size_t budget,size_t *text_len) {
    if(text_len)*text_len=0;
    if(!text_len||(!data&&len)||!csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    CsvSink measure={.cap=budget-1};
    if(csv_span(&measure,data,len,delimiter))return NULL;
    char *out=malloc(measure.used+1);if(!out){errno=ENOMEM;return NULL;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_span(&write,data,len,delimiter)){free(out);return NULL;}
    if(write.used!=measure.used){errno=EILSEQ;free(out);return NULL;}
    out[write.used]=0;*text_len=write.used;return out;
}
static int csv_record_pass(CsvSink *sink,const CsvSelection *selection,
                            const TypedRowView *view,const void *key,size_t key_len,
                            char delimiter,size_t budget,int header) {
    if(csv_span(sink,header?"key":key,header?3:key_len,delimiter))return -1;
    for(size_t i=0;i<selection->count;i++){
        if(csv_put(sink,&delimiter,1))return -1;
        if(header){
            const char *name=selection->names[i];
            if(csv_span(sink,name,strlen(name),delimiter))return -1;
        }else if(csv_named_span(sink,view,selection->names[i],delimiter,budget))return -1;
    }
    return csv_put(sink,"\n",1);
}
static char *csv_record_text(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len,int header) {
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||(!key&&key_len)||
       !csv_delim_ok(delimiter)){errno=EINVAL;return NULL;}
    if(!budget){errno=ENOMEM;return NULL;}
    TypedRowView view={0};
    if(!header&&typed_row_open(ts,value,len,hdr2,&view))return NULL;
    CsvSelection selection;char *out=NULL;
    if(csv_selection_open(ts,fields,&selection))goto done;
    CsvSink measure={.cap=budget-1};
    if(csv_record_pass(&measure,&selection,&view,key,key_len,delimiter,budget,header))goto done;
    out=malloc(measure.used+1);if(!out){errno=ENOMEM;goto done;}
    CsvSink write={.data=out,.cap=measure.used};
    if(csv_record_pass(&write,&selection,&view,key,key_len,delimiter,budget,header))goto fail;
    if(write.used!=measure.used){errno=EILSEQ;goto fail;}
    out[write.used]=0;*text_len=write.used;goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
char *typed_packed_csv_header(const TypedSchema *ts,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,NULL,0,0,NULL,0,fields,delimiter,budget,text_len,1);
}
char *typed_packed_csv_row(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t hdr2,const void *wire_key,size_t key_len,const char *fields,
    char delimiter,size_t budget,size_t *text_len) {
    return csv_record_text(ts,value,len,hdr2,wire_key,key_len,fields,delimiter,budget,text_len,0);
}
```

### Block MANIFEST
```json
{
  "base": "994e2eb43497721e34cd4aecf0159a00e42c226a",
  "branch": "fix/phase1-safe-scalar-conversions",
  "protected": "docs/plans/2026-10-19-durability-certification-rig.md",
  "protected_sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135",
  "unchanged": {
    "src/db/query_find.c": "0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a",
    "src/db/query_join.c": "df03bf908c1251d99b4fbdf1f18a84eafb401272faee71540333955d0c73cc05",
    "src/db/query.c": "3c97197a529ea68c4a077581c46a53733fbac183108925d9230f0b9b00402e9e",
    "src/db/storage.c": "05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7",
    "src/db/query_internal.h": "89a05d7de59587ada9232e155d1ddc3069b1bdc843d2074178f2acedf29b5400",
    "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
    "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
    "src/db/index.c": "2951fdc581f44c0024b8dde5d495fff7b64248d3ff5ecb4ea9bfc59cf25ae710",
    "src/db/query_plan.c": "57cf9a45fd755fc4c4a38b7e13edd212ddba192ab2b2f46ca9aeac697237c5dd",
    "src/db/query_aggregate.c": "6fc3b133351e59fc0a7bd708e94df4bf6fefa0b3e5b56dcbb02507dbeea09c5f",
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed"
  },
  "stages": {
    "1": [
      {
        "kind": "replace",
        "path": "src/db/packed_record.h",
        "old": "PKR_HEADER_ANCHOR",
        "new": "PKR_HEADER_NEW"
      },
      {
        "kind": "replace",
        "path": "src/db/packed_record.c",
        "old": "PKR_SCHEMA_ANCHOR",
        "new": "PKR_SCHEMA_STUB"
      },
      {
        "kind": "replace",
        "path": "src/db/typed_row.h",
        "old": "HEADER_ANCHOR",
        "new": "HEADER_NEW"
      },
      {
        "kind": "replace",
        "path": "src/db/typed_row.c",
        "old": "SOURCE_ANCHOR",
        "new": "SOURCE_STUB"
      },
      {
        "kind": "new",
        "path": "src/test/cases/test_packed_csv.c",
        "block": "TEST"
      },
      {
        "kind": "replace",
        "path": "build.sh",
        "old": "BUILD_OLD",
        "new": "BUILD_NEW"
      }
    ],
    "2": [
      {
        "kind": "replace",
        "path": "src/db/packed_record.c",
        "old": "PKR_VALID_STUB",
        "new": "PKR_VALID_FULL"
      },
      {
        "kind": "replace",
        "path": "src/db/typed_row.c",
        "old": "CSV_STUB",
        "new": "CSV_FULL"
      }
    ],
    "3": [
      {
        "kind": "new",
        "path": "docs/concepts/packed-csv-rendering.md",
        "block": "CONCEPT"
      },
      {
        "kind": "replace",
        "path": "docs/concepts/typed-row-adapter.md",
        "old": "ADAPTER_OLD",
        "new": "ADAPTER_NEW"
      },
      {
        "kind": "replace",
        "path": "docs/concepts/packed-named-projection.md",
        "old": "NAMED_OLD",
        "new": "NAMED_NEW"
      }
    ]
  },
  "checkpoints": {
    "0": {
      "build.sh": "cefe04d25ba0b9fb2df3a37daab53a98c271081b61d2f1a53203dd84a67a50c1",
      "src/db/typed_row.h": "0b56077eaf8f14f70a17460eac915d2e9eeb2a7ffcde409eaedf0b64b6c3129a",
      "src/db/typed_row.c": "001ef5983f43c1df8b1b53fff32e1b7311cd6fae0c550a578b5361356d758267",
      "src/test/cases/test_packed_csv.c": null,
      "docs/concepts/packed-csv-rendering.md": null,
      "docs/concepts/typed-row-adapter.md": "78cbe03002f468649a89bb3b08baeff9dd6b94c9ce15a839cc76491350652eae",
      "docs/concepts/packed-named-projection.md": "54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284",
      "src/db/packed_record.h": "181abc577b8baa6a51206816cc33fd19e2ac347f3570a2016a04315880a6468a",
      "src/db/packed_record.c": "8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d"
    },
    "1": {
      "build.sh": "b6520796dfb4ffe876e0b326a93fed5c145406fddd9b12edb5a37aa9671b481c",
      "src/db/typed_row.h": "13cacbc07ece575b17ac0a22788f5a6ff86e2e9aa547e1e2f368138f5c50fdea",
      "src/db/typed_row.c": "2628d1f3c953b2664564d9c79735e3aecd5d4401ee4988db20bb005a5aa2e134",
      "src/test/cases/test_packed_csv.c": "f10205a3d3d25348aebf6b0bbc529e532e9519d6cfc7c2d8699e6754f0c1f6d7",
      "docs/concepts/packed-csv-rendering.md": null,
      "docs/concepts/typed-row-adapter.md": "78cbe03002f468649a89bb3b08baeff9dd6b94c9ce15a839cc76491350652eae",
      "docs/concepts/packed-named-projection.md": "54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284",
      "src/db/packed_record.h": "56682286bd12a932d69fc8bdd46961db2a594357134cf35d90d61a402925ddab",
      "src/db/packed_record.c": "d2af06f97f219f67d13dba1bd9f20eb8d3b8a3562f33478edf8c6d48ae0cd1fb"
    },
    "2": {
      "build.sh": "b6520796dfb4ffe876e0b326a93fed5c145406fddd9b12edb5a37aa9671b481c",
      "src/db/typed_row.h": "13cacbc07ece575b17ac0a22788f5a6ff86e2e9aa547e1e2f368138f5c50fdea",
      "src/db/typed_row.c": "dad6b85428457d5abb9004b77610cc8b2f5670bcea62524af737783666ff5998",
      "src/test/cases/test_packed_csv.c": "f10205a3d3d25348aebf6b0bbc529e532e9519d6cfc7c2d8699e6754f0c1f6d7",
      "docs/concepts/packed-csv-rendering.md": null,
      "docs/concepts/typed-row-adapter.md": "78cbe03002f468649a89bb3b08baeff9dd6b94c9ce15a839cc76491350652eae",
      "docs/concepts/packed-named-projection.md": "54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284",
      "src/db/packed_record.h": "56682286bd12a932d69fc8bdd46961db2a594357134cf35d90d61a402925ddab",
      "src/db/packed_record.c": "17cc0dd151f0a747abaff98291f6f7364d39ecd7e1cdf7c7798dbf4b73c7664c"
    },
    "3": {
      "build.sh": "b6520796dfb4ffe876e0b326a93fed5c145406fddd9b12edb5a37aa9671b481c",
      "src/db/typed_row.h": "13cacbc07ece575b17ac0a22788f5a6ff86e2e9aa547e1e2f368138f5c50fdea",
      "src/db/typed_row.c": "dad6b85428457d5abb9004b77610cc8b2f5670bcea62524af737783666ff5998",
      "src/test/cases/test_packed_csv.c": "f10205a3d3d25348aebf6b0bbc529e532e9519d6cfc7c2d8699e6754f0c1f6d7",
      "docs/concepts/packed-csv-rendering.md": "5b96104d1b060f629e6574aff03ff2e30e032f134b06f9dd85dbd0350bbbba0f",
      "docs/concepts/typed-row-adapter.md": "bbdfe7d3f44c4bc27d4794fa77ea1cf6693df0ca43b3d4cea8d5b61fa000c452",
      "docs/concepts/packed-named-projection.md": "fb27dbbc3a8d5a0cb1c6cd43c336c5d0085638cf047b599d3fca22459f180e7e",
      "src/db/packed_record.h": "56682286bd12a932d69fc8bdd46961db2a594357134cf35d90d61a402925ddab",
      "src/db/packed_record.c": "17cc0dd151f0a747abaff98291f6f7364d39ecd7e1cdf7c7798dbf4b73c7664c"
    }
  },
  "blocks": {
    "CSV_HEADER": "f4f304d3313b419bab62ce55d6cd418927f41ffe9c26862f0042f3fd443824d6",
    "CSV_STUB": "3ce681007c3a16b9fb21eafa59aa64bdfd370d96e86f5640a6f121e633e9afcf",
    "CSV_FULL": "98fad2a2cbd8f14631dd4d17c27e2d4830a3e907449f12d57d7aa4481439002e",
    "TEST": "f10205a3d3d25348aebf6b0bbc529e532e9519d6cfc7c2d8699e6754f0c1f6d7",
    "PROBE": "bae9cfb593871f1383b27a925a1e417f75ceab4dc75500a6ff0a31235e194944",
    "ORACLE": "bd6ece58ae0d9514ffb8aa64204d09ab7fd35b65b1e11c24a89fbf9dc5198640",
    "DRIVER": "faac6208dcc4e27f880ea442eb55c395791fdd95574b11b6115f1b539f293c40",
    "CHECKS": "8d452474b0ce5c4a6fe926cbf41b806976e253787635c80b9af6e74eed53fd93",
    "CONCEPT": "5b96104d1b060f629e6574aff03ff2e30e032f134b06f9dd85dbd0350bbbba0f",
    "HEADER_ANCHOR": "a72aba1806773d1315d9a9ef23fb9d8593287b910b25e1abfc00f8c2c9f75242",
    "BUILD_OLD": "1174ae1f9f698af0d47c78499e3836ed8fb5aad1337912212d0122d3bb8b6cb4",
    "SOURCE_ANCHOR": "4c19efab8175370c8134f4fce21755d69afb4b8d6bb9ffd23b67ef4c80985ebf",
    "ADAPTER_OLD": "6b6666c49f6d6a2c9d163c7153489021b496746ceffecefc2048007d206f6e8c",
    "NAMED_OLD": "585e16b5f1e509689c67c2e0782b26ac1db9dd293ffb954a33f251ed9945fa6a",
    "HEADER_NEW": "572975d92604739e5c115f17eff55df45b51b5741c1af09ad721750493069dbd",
    "BUILD_NEW": "cfc714a5559978cc1da74142c0f70ddc8f2e925e51b820a4043a8842c5c58b3f",
    "SOURCE_STUB": "cc94a6bb17779348aa529d1a070ae8d253a96e80bf712c036828207b986edd12",
    "ADAPTER_NEW": "150f253661d46b01d146e4b0fcd5a211f28b2d039a9f6ea3ce92b9bd67c9b51d",
    "NAMED_NEW": "4bf5a2940ada9a8c15497cb6ddc596fedae75af369b3996a113e8dc977d33bf9",
    "BOOTSTRAP": "a2e88372e832e9ac5ee90705f4beb25de88576d888707ac3edb38b3d26de45cd",
    "PKR_SCHEMA_ANCHOR": "890dde6ff65c52152ebf7fd1be7e8f21915fe9e8677cf56a5617400f0b5f7ab5",
    "PKR_VALID_STUB": "ffd55561a726889763b5579ac6522dda43efe606a588d267a0a7f4db02d0835c",
    "PKR_VALID_FULL": "fba30589d59a391d559f95d8c46fdff30e8e56d79e84134f5bb166ad83e5d618",
    "PKR_SCHEMA_STUB": "ed1621800fd5c9525393b1c8a6fd27949bc7840448b256dc0e217a35759de5a6",
    "PKR_HEADER_ANCHOR": "1fef007e8035d1e12bd958d59899eb7c60df1482361f7e5dcb62e8980859f450",
    "PKR_HEADER_NEW": "e2ef5180cfaaba815bd09ddbe566f5d6b4672e9b5c34834907d530538b27827e",
    "CSV_REVIEW_BASELINE": "f328e31f3eddf214de737fcc7da14e30b3c4e1838b76a0bd825371b15e1fcf9d"
  }
}
```

