# R1 — ordinary writes, reads and declaration round trips

## Primary goal

Make ordinary finance writes and reads preserve both exact numeric forms and
complete strings across direct, bulk, default, computed and schema-transfer
routes, without activating incomplete public packed storage.

**REVISION 3 — BLIND REVIEW FIXES COMPLETE; READY FOR HUMAN-AUTHORIZED EXECUTION.** Planner GPT 6.1 Sol;
intended executor GLM 5.3. Exact base `0ef1a0ba3b33ec94c947e85091f866b39a1ee583`
on `fix/phase1-safe-scalar-conversions`. This is one consolidated delivery plan,
not a new sequence of helper plans. No production code is changed while authoring.
The ordinary scratch prototypes below are not implementation permission.

## Concurrency constraint — user clarification

Preserve existing ordinary-reader per-shard/cache locking. Do not add writer
reservations, writer-gate acquisition, new locks, lock-order changes or stronger
cross-record snapshot guarantees. The planner withdrew its scratch writer-gate
reader prototype; it never entered the root production tree. Bounds/return-status
improvements may use the same existing read-handle lifetime. Committed 3B.10's
private all-shard CSV reservation is not permission to broaden that policy.
Any proposed modification beyond existing read locking requires explicit review
of the concrete necessity first.

## Scope and ownership

R1 combines remaining ordinary write/read integration with original Phase 4.
R2 owns predicate/index/CAS matching, ordered reads and joins. R3 owns aggregate
semantics/output. R4 owns remaining field grammars. R5 owns authoritative layout
binding/loading, durable schema publication and migration. R6 owns activation and
application acceptance. Keep existing public legacy behavior available and packed
loading/creation inactive until explicit activation approval.

The following cannot be marked complete merely because their helpers exist:
all single and bulk input formats; generated/literal defaults; computed updates;
auto keys; duplicate input keys; read envelopes/projections/keys/exists; failure
and request-memory accounting; schema/client identity and import/export handling.
Every remaining item must have a complete patch/test or an explicit original
owner that is not a new micro-plan.

## Phase 0 — documentation and source discovery

**How this serves the primary goal:** establishes complete route ownership and
actual APIs before changing finance data paths.

Three independent read-only fact-gathering passes covered writes/defaults,
ordinary reads, and declaration/client surfaces. Sources: AGENTS.md,
CORE-PROCESS.md; parent recovery/consolidation; Phase 3 work index; typed-records,
typed-row-adapter, owning-write-hooks, single-write, packed-bulk, whole-reader,
projection/named-projection/CSV concepts; query protocol overview/find/bulk/CAS/
schema-mutations/diagnostics; npm raw-result docs and source.

Verified existing APIs: typed_insert_alloc, typed_insert_inputs_alloc,
typed_update_json_alloc, typed_update_alloc; TypedPrepared and typed_row_span;
dec_parse/unpack/add/apply_precision/pack/to_text; checked packed JSON/CSV
renderers; existing keyfile/segment read handles; the strict walker/lookup status APIs (the ordinary checked counterparts are new in R1); conf_read_line;
pkr_field_parse; query_collect and CLI query/JSON span helpers. Source reference
search must include direct content discovery because the language-server caller
index omitted cross-translation-unit references during planning.

Root causes: prepare_json rejects computed objects; legacy compute code writes
fixed offsets and scaled int64; non-CSV readers stream success prefixes and
swallow decoder/OOM failures; multi-get fallback can reuse freed results; key
output truncates/does not escape; schema export guesses P=18 from physical size;
default grammar accepts suffixes/truncation and counts nullability against the
literal limit. Bare/one-argument numeric semantics must remain layout-specific.
Npm query already returns a raw string; instruct finance callers to retain number
lexemes instead of introducing an unnecessary client runtime dependency.

## Phase 1 — shared ordinary read ownership and memory bounds

**How this serves the primary goal:** a failed read cannot return a plausible
partial financial result, and complete values/keys survive all ordinary formats.

Prepare checked ordinary command collection with explicit key lengths, bounded
owned chunks and decimal/projection workspace allowance; retain the approved CSV
code through shared contracts. Integrate single GET/projection, fetch envelopes,
rows/dict, multi-get, keys and existence outcomes. Prepare before output; release
existing cache read handles before FILE I/O; no replacement JSON after accepted bytes.
Prove projected numeric lexemes, complete JSON framing, binary/auto keys, missing
and duplicate keys, cursor/offset bounds, quota/read/deadline/OOM/I/O failures.

## Phase 2 — exact writes, computed updates and defaults

**How this serves the primary goal:** every accepted input route stores the same
exact value and a failed update preserves OLD, indexes and counts.

Use immutable OLD spans for every computed operand, then exact addition and
column assignment once. Preserve copy compatibility and NULL/nonfinite policies.
Bulk remains on its deferred request lifetime, not single owning hooks. Preserve
positional versus JSON default semantics explicitly. Finish generated default,
auto-key, duplicate-key and import decoding policies with complete route tests.

## Phase 3 — declarations, schema transfer and clients

**How this serves the primary goal:** exporting/importing a finance schema cannot
silently change precision, scale, capacity, defaults or nullability.

Expose a checked exact original declaration in describe, preserving raw legacy
meaning and explicit packed identity. Export that declaration instead of P=18
or descriptor widths. Cover argv and TUI without fixed 256-byte export buffers;
check errors before advertising a complete manifest. Keep packed metadata
publication/loading inactive (R5). Use the existing npm raw-result route and
update precision-losing examples/comments.

## Phase 4 — verify the complete delivery

**How this serves the primary goal:** proves the route matrix rather than a
collection of individually passing helpers.

Regressions first, genuine RED on the base. Complete anchored implementation
hunks and manifests must be supplied before executor handoff. Ordinary builds
must be warning-free; focused/public-route tests and a fresh full suite are
mandatory. Allocation-failure and regression reversal proofs must cover each
shared boundary, with exact OLD/absence/index/count preservation. No benchmarks;
branch-final sanitizers run only after all roadmap fixes before authorized push.
Stop unstaged/uncommitted for raw-diff and independent blind review.

## Execution rules and checkpoint

Planner/reviewer: GPT-6.1 Sol. Executor: GLM 5.3, as confirmed by the user.
Continue the existing fix branch at the exact base; do not create/reset a branch,
commit, stage, push, merge or start R2. Execution requires the human's approval
of this plan after its blind review. Root authoring changes are Markdown only;
all measured C implementations below were compiled in disposable scratch copies.

Read AGENTS.md, CORE-PROCESS.md, the parent recovery/consolidation and the Phase 3
work index first. Access the protected certification file by SHA256 only. Its
contents are never planning input or execution work. Preserve all existing plans,
review files and other agents' changes. Raw logs, probe sources/objects/binaries
and extracted drivers live under a fresh `/tmp/r1-exec-*`, never in Git.

Every quoted old block must match exactly once. The frozen driver checks every
owned file before a stage, checks untouched boundaries, computes every postimage
before writing, and then applies only the published changes. On an anchor,
checksum, warning, test or uncovered-decision mismatch: preserve the real output,
write PLAN_NOTES.md, and halt the entire run. Resume only with a planner/human
amendment. Do not repair code, reinterpret anchors or weaken assertions yourself.

Builds and tests on a given checkout are sequential: never rebuild/copy binaries
while its suite is running. Commands are `rtk`-prefixed. Build normally with
`rtk proxy env SKIP_TESTS=1 ./build.sh`; use the built test runner, not an old
binary. Zero compiler warnings at both stages. No sanitizers or benchmarks here.
The branch-final ASan+UBSan and TSan 3x gates stay immediately before an authorized
push/PR, after all roadmap fixes. TSan then uses --jobs 2 as AGENTS.md requires.

## Route, consumer and compatibility inventory

| Boundary | Concrete integration | Contract / later owner |
|---|---|---|
| Ordinary reads | storage.c cmd_get/cmd_get_fields/cmd_get_multi/cmd_exists_multi/cmd_not_exists; query_find.c cmd_fetch_v2/cmd_keys/cmd_exists | New typed_read_command owns responses for both layouts; retain existing envelopes and exact key lengths. Predicate/find/join/cursor engine consumers remain R2. |
| Checked storage references | new slotcask_walk_live_checked/slotcask_lookup_by_hash_checked plus checked batched lookup/resolve-fetch counterparts | Existing KF/segment read handles only. Original slotcask.c is an exact byte prefix of final source. No writer gate/reservation or new locks in appended code. Multi-key modes retain shard buckets, one existing I/O-pool job per touched shard and batched storage reads; no serial per-key replacement. |
| Packed CSV commands | packed_csv_fetch/packed_csv_get_multi | Remove private all-shard reservation, use checked ordinary readers; account selector/key/render workspace. Shared CSV cell encoding remains frozen. |
| Decoder ownership | typed_decode_bounded_size/typed_decode_bounded; existing typed_decode and typed_decode_stream | Existing wrappers remain callable; allocated capacity is exposed internally for response admission. Keep malformed input errno and legacy zero/short-tail semantics. |
| Packed computed update | typed_update_computed_alloc; storage.c v2_update_owned | Evaluate immutable OLD; exact decimal sum, one assignment rounding; no matching/guard implementation. Bulk retains literal-only policy. |
| Legacy computed update | apply_computed_fields / legacy_numeric_increment; V2FieldCompute ownership | Exact sum with actual legacy scaled-int64 bounds, not double; legacy declared P stays informational until migration audit. |
| Generated defaults | parse_default_modifier_checked, generate_default_owned, typed_default_validate; TypedField.default_invalid | Strict new declarations; retain historical loader interpretation for old records. Unsafe old defaults cannot silently generate truncated text. Generator state may consume gaps on failure. |
| Packed bulk | typed_bulk_insert_command/update_command/source; query_bulk.c file/inline dispatch | Existing deferred request engine and worker/lock scheduling unchanged. UUID/sequence keys normalize before duplicate checks. Checked file reads and conservative request admission. |
| Schema declaration input | cmd_create_object, cmd_add_fields, parse_field_line/edit, server add/edit dispatch; typed_field_specs_parse/typed_field_lines_parse | Full JSON decode once, no truncated names/specs, duplicates checked before mutation. Scalar literal defaults reuse checked encoders; R4 owns remaining grammar. |
| Schema describe/export/import | typed_schema_fields_json/indexes_json; cmd_describe_object; server field_obj_to_spec/emit callbacks/cmd_export_schema; CLI transfer helpers | Exact original spec, logical capacity, layout/streams/auto-key identity; checked staging and existing destination preservation. Packed import refuses until R5 rather than interpreting packed as legacy. |
| Node client | query already returns raw text; index.js/index.d.ts/README and csv-bytes.js | No native addon/runtime dependency/concurrency changes. Reversible cell decoder after ordinary CSV parsing. Preserve decimal lexemes in finance applications. |
| Build lists | daemon, static library, test server, test runner, bench binary source lists | Both new modules must appear in every relevant list. Building the bench binary is not running benchmarks. |

Search references again before approval/final review: `typed_decode`,
`parse_default_modifier`, `generate_default`, `parse_field_line`,
`typed_update_json_alloc`, `typed_bulk_*`, `typed_read_*`,
`field_obj_to_spec`, `collect_string_array`, `typed_schema_bind_packed`.
The complete frozen hunks include every affected consumer; no signature change
is left to executor discovery. No callback is allowed to reenter the database,
perform client I/O or change its surrounding transaction's locking protocol.

Compatibility changes are deliberate: malformed new declarations/defaults fail
earlier; ordinary read failures no longer masquerade as absence/partial success;
legacy numeric increments with excess fractional input round once on assignment
instead of rejecting it; ordinary CSV uses reversible control-byte escaping.
CSV remains unable to distinguish NULL from empty. It is not a lossless typed
bulk-import grammar. Legacy historical duplicate-key behavior is retained;
packed duplicate refusal was already its approved private policy. Migration and
public activation, including release compatibility notes, remain R5/R6.

Existing-test changes are contract updates, not assertion weakening:
- test_single_write_integration asserts a valid computed update succeeds and its
  returned/stored value advances; an invalid mixed operator remains rejected.
- test_computed_update asserts exact sub-scale addition succeeds under the newly
  specified assignment rule; all other update/guard/index assertions remain.
- test_scalar_conversion_safety moves overflowing-default rejection to create
  and asserts the rejected declaration leaves no object.
- test_not_null_edit_modifiers checks the actual default property instead of
  searching an entire describe response that now legitimately includes `spec`.

## Revision 2 blind-review closure — 2026-10-28

Two independent reviewers inspected the proposed source diff against the exact
base, without the new plan/rationale. All seven actionable findings are fixed:

| Finding | Correction and regression evidence |
|---|---|
| STD-01: multi-key shard workers were replaced by serial lookups | Retain shard buckets, existing I/O-pool dispatch and checked versions of the existing batched lookup/resolve-fetch pipelines. Two touched shards produce two jobs/two bulk calls in GET, exists and not-exists. The pre-review reader reproduces zero jobs/calls. No new locks/gates or storage-engine prefix changes. |
| STD-02: query_collect omitted the response terminator | Reserve cap+1 and terminate framed and EOF responses. An exact-capacity response probe now parses safely; the baseline reproduces SIGSEGV. Probes are local transport stubs only and create no network service. |
| SPEC-01: generated defaults exceeded quota before rejection | Admit cumulative prepared owners plus generator text and its copied payload before allocation. Two 40,000-byte defaults under a 65,536-byte cap previously peaked at 120,002; the corrected rejection peaks at 2, with no live buffers. |
| SPEC-02: exported stream metadata was silently replaced on import | Tagged manifests with a stream count differing from this host fail explicitly until R5 can honor geometry. Untagged historical create requests retain their behavior. No implicit stream routing or concurrency reconfiguration is added. |
| SPEC-03: OLD validation/computed workspace exceeded reduced quota | Admit numeric validation before opening OLD, then copy/parse/aligned arithmetic/result workspace against retained owners. A 60,000-digit OLD under budget 4,096 previously peaked at 125,574; corrected rejection peaks at 0, with no live buffers. |
| Planner-01: overwritten invalid computed duplicate escaped validation | Validate every computed occurrence against immutable OLD immediately. Later literals cannot erase earlier invalid inputs. Final copy-source compatibility checks remain. |
| Planner-02: computed-operator OOM was rewritten as invalid JSON | Preserve allocation failure through cleanup; all 26 computed allocation-injection cases assert ENOMEM as well as rejection and ownership cleanup. |

The second source review of scheduling/ownership and final quota guards found no
further actionable findings. This is source/probe review, not the later executed
raw-diff approval. The checks include scratch-only baseline modules reproducing
these defects and never apply those modules to production. No sanitizer runs or
concurrency redesign are part of this correction.

## Revision 3 additional blind review — 2026-10-28

Fresh reviewers inspected only the proposed source and repository requirements,
without this plan, its rationale or earlier review conclusions. The review found
that argv schema export treated a valid JSON list-objects error envelope, or a
response lacking objects, as a successfully empty tenant. The staged export could
then replace an existing destination with an incomplete manifest. The frozen
callback now rejects both explicit error members (even alongside objects) and
missing objects. Existing malformed-array and callback errors remain fatal.
The frozen export transport regression must fail against the pre-correction
binary and pass after the callback correction; it checks nonzero status, no
stdout manifest, preservation of an existing destination, and a valid empty
objects success control. This is part of Task 3 checks, not optional executor work.

A second defect was reproduced in the checked bulk storage APIs: flag 3 was
accepted by lookup and silently retired during resolve/revalidation. Bulk exists
reported true and bulk GET reported null, while checked single GET correctly
reported EUCLEAN. Appended checked bulk probes now distinguish corruption from
valid missing/tombstoned/repointed records and propagate EUCLEAN. The original
storage prefix, cache read-handle lifetime, scheduling and locks remain unchanged.
The frozen checks run the same corruption probe against final code and a scratch
pre-correction checked-reader suffix: RED has exactly three failed bulk checks,
GREEN passes all six checks, including single-read and tombstone controls.

The extraction instructions now publish expected driver/checks hashes, making
Task 0's required checksum comparison executable. Production source remains
unchanged; review and verification use disposable archived trees only.

Final corrected verification: a warning-free ordinary build; focused case
109 passed / 0 failed; full suite 20,931 passed / 0 failed across 529 cases,
exit 0. The extended frozen checks pass 126 allocation failures with live=0,
all six corruption/tombstone checks, all 18 export scenarios, and their isolated
pre-correction RED proofs. All three stages replay exactly in a fresh archive;
final owned postimages match the measured tree. Independent source cross-review
found no further actionable issue in either correction. See
[the review evidence](2026-10-28-r1-blind-review.md). No production code, Git
writes, protected certification edits, sanitizers or benchmarks were performed.

## Task 0 — preflight and extraction (no implementation)

1. Verify branch/HEAD, clean production source, planning artifact inventory and
   protected SHA. Record exact outputs in a curated evidence ledger.
2. Extract driver/checks/probe blocks from this file into a fresh temporary
   directory using Appendix A. Verify the driver block checksum before use.
3. Run driver stage 1 --check-only. It must report no writes and exact checkpoint
   0/boundaries. Review the route table and concurrency checks above.
4. Confirm the environment can start an isolated daemon. A demonstrated socket
   EPERM requires the documented socket-capable invocation, not test changes.
   Infrastructure failures are never numeric/data correctness RED evidence.

## Task 1 — RED first, additive declarations and link stubs

Root causes: existing ordinary packed read routes cannot interpret descriptors;
legacy streaming branches can publish before late failure; default/declaration
parsers accept/truncate prefixes; schema export guesses P; computed objects are
refused and legacy addition rounds through restricted conversion paths. The new
case exercises those behaviors before final implementations are installed.

Apply stage 1 only. This adds the regression and build wiring, compatible header
additions and deliberately incomplete link stubs. It does not install final
read/write/declaration code. Build with zero warnings, then capture the focused
case unfiltered: **69 passed, 36 failed, exit 36** on the frozen base/stubs.
The intended failure classes are ordinary reads/escaping/projection, exact
computed addition, default consumption/size, describe identity, packed import
refusal and auto-key bulk. Incidental passing stub rejections are not claimed as
proof of final behavior. Stop on a mismatch; do not alter the test to match a count.

Task 3's frozen checks prove the additional export regression against a scratch
binary differing only in the pre-correction tenant callback. Exactly six RED
scenarios must fail while empty-tenant success controls pass; the final binary
must pass all 18 scenarios. This isolates the callback defect from the base's
separate unterminated-query-collect bug and avoids accepting incidental UB or
fixture failure as RED. No production source is reverted.

## Task 2 — final C integration, GREEN and normal suite

Apply stage 2 exactly. It replaces stubs and installs all checked read/write/
default/schema/TUI code plus the explicit existing-test contract updates above.
Build with zero warnings. Focused case: **109 passed, 0 failed, exit 0**. This
includes private packed physical fixtures plus isolated public legacy schema
requests. Re-run the existing scalar conversion, computed-update, enum,
whole-record-reader, schema-export and CSV-command cases if diagnosing a failure;
the required fresh full suite still runs, never only these subsets.

Run `rtk proxy ./build/bin/shard-db-test run-all` and capture its real final total,
all cases green and exit 0. The planning reference is recorded below after its
fresh run, not inferred from prior counts. No rebuild while that suite is live.

## Task 3 — client/docs checks, literal replay and review hand-back

Apply stage 3. No further C implementation is allowed here. Verify checkpoint 3,
run the frozen checks script (126 selected allocation failures across six routes,
no live tracked buffers, sequence bulk keys, CLI escape/Unicode checks, 2000 seeded
JS byte round trips), and run git diff --check. Admission/status failures must
not publish a success prefix. The OOM instrumentation is scratch-only and wraps
real selected translation units, not production allocator seams.

Prove regression reversal in a fresh scratch tree: archive the exact base without
copying any protected/untracked file; apply only stage 1; build/capture genuine
RED. Apply stages 2/3; build/capture GREEN and run checks. Alternatively retain
and verify the exact stage-1 scratch checkpoint produced for initial RED, then
apply frozen stages there. Never temporarily revert production checkout files.
Verify the same tests and expectation blocks are used in both states.

Write a concise curated evidence document under docs/plans with command outputs,
identity, actual counts, manifests, failure mechanisms and deviations (if any).
Raw logs/probe binaries remain under /tmp. Re-hash the protected file; perform the
binder/concurrency/owned-file census. Stop unstaged/uncommitted for plan-aware and
independent blind raw-diff review. No automatic commit or R2 work.

## Planning-time verification disclosure

All production prototypes and test execution were in `/tmp/r1-author/tree` and
an independent archived RED tree. Root production stayed at 0ef1a0b. Earlier
failed authoring runs are retained: socket-denied daemon failures; provisional
test wiring compile errors; a Text-file-busy scratch copy caused by overlapping
build/test; compatibility errno/message regressions; and a CLI Unicode probe
correction. These are not green evidence and are not hidden by rerun-until-green.
Each code defect was identified, corrected and then checked on a fresh build.
No sanitizer or benchmark was run.

Frozen focused RED: 69/36, exit 36. Frozen focused GREEN: 109/0, exit 0.
Selected allocation probe: 25 fetch + 47 multi-get + 26 computed + 2 declaration
+ 15 packed CSV + 11 single GET = 126 deterministic failures, all live=0.
A standalone transport probe checks UTF-8, surrogate pairs and malformed arrays.
The JS decoder checks all 256 bytes, NUL/literal distinctions and 2000 seeded
round trips. Full-suite final reference: **20931 passed, 0 failed across 529 cases, exit 0**,
on the fresh warning-free ordinary build. Both RED and GREEN builds had zero
warnings. The frozen tools are re-extracted verbatim after each revision, apply all three stages
to a fresh archive and verified every final postimage against the measured tree;
the extracted checks script passed on that final tree. No production source changed.

Known scope limits are explicit: no live PostgreSQL oracle is needed here (the
already-reviewed decimal core's contract/oracle remains authoritative); no bare
numeric/varchar loader activation, index/criterion/aggregate conversion or durable
schema/migration work is claimed. A pre-existing Node async-handle lifetime concern
was observed while inspecting the client; no native lifecycle changes are included
or implicitly approved. R6 client acceptance must account for it or obtain a
separate concrete fix authorization without redesigning engine concurrency.

## Appendix A — extract the frozen tools

Run this only after approval, from the repository root. Substitute the printed
fresh directory in subsequent commands; do not reuse historical scratch binaries.

```python
from pathlib import Path
import re,tempfile,hashlib
plan=Path('docs/plans/2026-10-27-r1-write-read-integration-plan.md')
fence='`'*8
items=re.findall(r'<!-- R1-BLOCK ([A-Za-z0-9_-]+) -->\n'+fence+r'[^\n]*\n(.*?)^'+fence+r'\n',plan.read_text(),re.S|re.M)
assert len(dict(items))==len(items)
blocks=dict(items);directory=Path(tempfile.mkdtemp(prefix='r1-exec-'))
for block,filename in [('driver','driver.py'),('checks','checks.py'),('export-probe','export_probe.py')]:
    data=blocks[block].encode();(directory/filename).write_bytes(data)
    print(filename,hashlib.sha256(data).hexdigest())
print(directory)
```

Invoke the Python bootstrap through `rtk proxy python3` (a here-document is fine).
Compare both printed SHA256 values with these frozen expected values before
invoking either tool; on a mismatch write PLAN_NOTES.md and halt:

```text
driver.py  940e76e7257711614f1edf4f8e765d5dc126c7a6657cc46ce44a007536427a9f
checks.py  8ef94cdb84a82c2b866a9147fe9d158142248a93d7176f39ea03deed860d0f86
```

Then, with the actual temporary directory replacing TMP below:

```sh
rtk proxy python3 TMP/driver.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --stage 1 --check-only
rtk proxy python3 TMP/driver.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --stage 1
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-r1-routes
# Capture/handle the required RED exit 36 explicitly, then continue only if it matched.
rtk proxy python3 TMP/driver.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --stage 2
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-r1-routes
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy python3 TMP/driver.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --stage 3
rtk proxy python3 TMP/driver.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --stage 3 --verify
rtk proxy python3 TMP/checks.py --plan docs/plans/2026-10-27-r1-write-read-integration-plan.md --output TMP/checks
rtk git diff --check
```

The --scratch driver option is restricted to `/tmp` and omits Git/protected-file
identity checks there because an archive has neither. All owned and boundary
postimages are still mandatory. It is never used on the production checkout.

## Frozen executable material

The old blocks below are quoted anchors, not line-number instructions. Each
replacement is complete and checksum-verified. All three stages were replayed
in memory while authoring; no executor interpretation is required. Eight-backtick
fences allow literal Markdown and source strings inside the replacements.

<!-- R1-BLOCK s1e1-old -->
````````text
        DO_STRIP=0
        ;;
    *)
        echo "build.sh: unknown BUILD_MODE=$BUILD_MODE (release|asan|tsan|debug|coverage)" >&2
        exit 1
        ;;
esac

# -flto: link-time optimization (cross-TU inlining; usually helps perf, definitely
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut). Skipped
#        for sanitizer/debug builds — symbols are needed for readable stack traces.
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c \
          src/db/nql.c src/db/embedded.c"
LIB_OBJS=""
mkdir -p build/bin build/obj
for f in $LIB_SRCS; do
    obj="build/obj/$(basename "${f%.c}").o"
    gcc $MODE_CFLAGS -c "$f" -Isrc/db $OSSL_CFLAGS -o "$obj"
    LIB_OBJS="$LIB_OBJS $obj"
done
ar rcs build/bin/libshard-db.a $LIB_OBJS
cp src/db/shard_db.h build/bin/shard_db.h
gcc $MODE_CFLAGS -o build/bin/embedded_lock_harness \
    src/test/embedded_lock_harness.c build/bin/libshard-db.a \
````````

<!-- R1-BLOCK s1e1-new -->
````````text
        DO_STRIP=0
        ;;
    *)
        echo "build.sh: unknown BUILD_MODE=$BUILD_MODE (release|asan|tsan|debug|coverage)" >&2
        exit 1
        ;;
esac

# -flto: link-time optimization (cross-TU inlining; usually helps perf, definitely
#        shrinks the binary by eliminating dead code visible only across files).
# strip: remove symbol/debug tables from the shipped binary (~25K cut). Skipped
#        for sanitizer/debug builds — symbols are needed for readable stack traces.
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/typed_read_command.c src/db/schema_wire.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db

# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/typed_read_command.c src/db/schema_wire.c src/db/type_desc.c \
          src/db/nql.c src/db/embedded.c"
LIB_OBJS=""
mkdir -p build/bin build/obj
for f in $LIB_SRCS; do
    obj="build/obj/$(basename "${f%.c}").o"
    gcc $MODE_CFLAGS -c "$f" -Isrc/db $OSSL_CFLAGS -o "$obj"
    LIB_OBJS="$LIB_OBJS $obj"
done
ar rcs build/bin/libshard-db.a $LIB_OBJS
cp src/db/shard_db.h build/bin/shard_db.h
gcc $MODE_CFLAGS -o build/bin/embedded_lock_harness \
    src/test/embedded_lock_harness.c build/bin/libshard-db.a \
````````

<!-- R1-BLOCK s1e2-old -->
````````text
    Darwin) NCURSES_LDFLAGS="-lncurses" ;;
esac
gcc $MODE_CFLAGS -o shard-cli src/cli/main.c src/cli/widgets.c src/cli/views.c src/cli/conn.c -Isrc/cli $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS $NCURSES_LDFLAGS -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-cli

# shard-db-test-server — TEST_BUILD-only daemon for deterministic
# cross-process test control. Same source list as the production shard-db
# target plus src/db/test_control.c, compiled with -DTEST_BUILD (which also
# enables the slotcask pause-hook seam). Test fixtures prefer this binary so
# TCP cases reach the deterministic seam; the production shard-db binary and
# its flags are never built from TEST_BUILD.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test-server \
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test-server

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
# (src/db/util.c) for response parsing; otherwise self-contained (TCP/TLS
# client + assertion macros + per-test daemon fixtures). Future test cases
# under src/test/cases/ get listed here.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/db/decnum.c \
    src/db/packed_record.c \
    src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/test/cases/test_bool_literal_update_roundtrip.c \
    src/test/cases/test_trim_compact_oob_fields.c \
    src/test/cases/test_or_logic.c \
    src/test/cases/test_crash_safety.c \
    src/test/cases/test_rename_field.c \
    src/test/cases/test_edit_field.c \
    src/test/cases/test_edit_field_polish.c \
    src/test/cases/test_add_field_computed_defaults.c \
    src/test/cases/test_auto_key.c \
````````

<!-- R1-BLOCK s1e2-new -->
````````text
    Darwin) NCURSES_LDFLAGS="-lncurses" ;;
esac
gcc $MODE_CFLAGS -o shard-cli src/cli/main.c src/cli/widgets.c src/cli/views.c src/cli/conn.c -Isrc/cli $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS $NCURSES_LDFLAGS -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-cli

# shard-db-test-server — TEST_BUILD-only daemon for deterministic
# cross-process test control. Same source list as the production shard-db
# target plus src/db/test_control.c, compiled with -DTEST_BUILD (which also
# enables the slotcask pause-hook seam). Test fixtures prefer this binary so
# TCP cases reach the deterministic seam; the production shard-db binary and
# its flags are never built from TEST_BUILD.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test-server \
    src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/main.c src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/typed_read_command.c src/db/schema_wire.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-test-server

# shard-db-test — TAP-style C test runner. Links daemon's JSON helpers
# (src/db/util.c) for response parsing; otherwise self-contained (TCP/TLS
# client + assertion macros + per-test daemon fixtures). Future test cases
# under src/test/cases/ get listed here.
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/db/decnum.c \
    src/db/packed_record.c \
    src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/typed_read_command.c src/db/schema_wire.c \
    src/test/test_client.c \
    src/test/test_runner.c \
    src/test/fixtures.c \
    src/test/cases/test_bool_literal_update_roundtrip.c \
    src/test/cases/test_trim_compact_oob_fields.c \
    src/test/cases/test_or_logic.c \
    src/test/cases/test_crash_safety.c \
    src/test/cases/test_rename_field.c \
    src/test/cases/test_edit_field.c \
    src/test/cases/test_edit_field_polish.c \
    src/test/cases/test_add_field_computed_defaults.c \
    src/test/cases/test_auto_key.c \
````````

<!-- R1-BLOCK s1e3-old -->
````````text
    src/test/cases/test_packed_record_codec.c \
    src/test/cases/test_packed_record_storage.c \
    src/test/cases/test_typed_row_adapter.c \
    src/test/cases/test_owning_write_hooks.c \
    src/test/cases/test_single_write_integration.c \
    src/test/cases/test_typed_bulk_insert.c \
    src/test/cases/test_typed_bulk_update.c \
    src/test/cases/test_whole_record_readers.c \
    src/test/cases/test_packed_json_projection.c \
    src/test/cases/test_packed_named_projection.c \
    src/test/cases/test_packed_csv.c \
    src/test/cases/test_packed_csv_commands.c \
    src/test/cases/test_version_gate.c \
    src/test/cases/test_datetime_evening.c \
    src/test/cases/test_error_paths.c \
    src/test/cases/test_explain.c \
    src/test/cases/test_dispatch_leak_paths.c \
    src/test/cases/test_removed_storage_surfaces.c \
    src/test/cases/test_keyset.c \
    src/test/cases/test_objlock_dynamic_growth.c \
    src/test/cases/test_objlock_unit.c \
    src/test/cases/test_query_plan_in_oom.c \
    src/test/cases/test_parallel.c \
    src/test/cases/test_planner_edge_cases.c \
````````

<!-- R1-BLOCK s1e3-new -->
````````text
    src/test/cases/test_packed_record_codec.c \
    src/test/cases/test_packed_record_storage.c \
    src/test/cases/test_typed_row_adapter.c \
    src/test/cases/test_owning_write_hooks.c \
    src/test/cases/test_single_write_integration.c \
    src/test/cases/test_typed_bulk_insert.c \
    src/test/cases/test_typed_bulk_update.c \
    src/test/cases/test_whole_record_readers.c \
    src/test/cases/test_packed_json_projection.c \
    src/test/cases/test_packed_named_projection.c \
    src/test/cases/test_packed_csv.c \
    src/test/cases/test_packed_csv_commands.c \
    src/test/cases/test_r1_routes.c \
    src/test/cases/test_version_gate.c \
    src/test/cases/test_datetime_evening.c \
    src/test/cases/test_error_paths.c \
    src/test/cases/test_explain.c \
    src/test/cases/test_dispatch_leak_paths.c \
    src/test/cases/test_removed_storage_surfaces.c \
    src/test/cases/test_keyset.c \
    src/test/cases/test_objlock_dynamic_growth.c \
    src/test/cases/test_objlock_unit.c \
    src/test/cases/test_query_plan_in_oom.c \
    src/test/cases/test_parallel.c \
    src/test/cases/test_planner_edge_cases.c \
````````

<!-- R1-BLOCK s1e4-old -->
````````text
    src/db/slotcask.c \
    src/db/simd.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c \
    src/db/type_desc.c \
    src/db/embedded.c \
    src/db/nql.c \
    -Isrc/db -Isrc/test -Isrc/bench \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-bench

mkdir -p build/bin

# Purge any dev-run artifacts so `./build.sh` always emits a clean tree.
# When the daemon is exercised from build/bin during local testing it
# creates build/db (DB_ROOT="../db") + build/logs (LOG_DIR="../logs") +
````````

<!-- R1-BLOCK s1e4-new -->
````````text
    src/db/slotcask.c \
    src/db/simd.c \
    src/db/io_direct.c \
    src/db/query.c \
    src/db/query_aggregate.c \
    src/db/query_join.c \
    src/db/query_plan.c \
    src/db/query_maint.c \
    src/db/query_schema.c \
    src/db/query_bulk.c \
    src/db/query_find.c \
    src/db/server.c \
    src/db/config.c src/db/decnum.c src/db/packed_record.c src/db/typed_row.c src/db/typed_bulk_insert.c src/db/packed_csv_command.c src/db/typed_read_command.c src/db/schema_wire.c \
    src/db/type_desc.c \
    src/db/embedded.c \
    src/db/nql.c \
    -Isrc/db -Isrc/test -Isrc/bench \
    $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
[ "$DO_STRIP" = 1 ] && strip shard-db-bench

mkdir -p build/bin

# Purge any dev-run artifacts so `./build.sh` always emits a clean tree.
# When the daemon is exercised from build/bin during local testing it
# creates build/db (DB_ROOT="../db") + build/logs (LOG_DIR="../logs") +
````````

<!-- R1-BLOCK s1e5-old -->
````````text
   bm_cache_init/slotcask_init are all no-ops when already initialized,
   so this is a safe unconditional call regardless of what the prior test
   left behind. */
void test_reset_caches(void) {
    schema_caches_shutdown();
    if (g_db) {
        bt_cache_init(g_db->btcache_cap);
        bm_cache_init(g_db->btcache_cap);
        slotcask_init(g_db->fcache_cap, g_db->fcache_cap);
    }
}
#endif
````````

<!-- R1-BLOCK s1e5-new -->
````````c
   bm_cache_init/slotcask_init are all no-ops when already initialized,
   so this is a safe unconditional call regardless of what the prior test
   left behind. */
void test_reset_caches(void) {
    schema_caches_shutdown();
    if (g_db) {
        bt_cache_init(g_db->btcache_cap);
        bm_cache_init(g_db->btcache_cap);
        slotcask_init(g_db->fcache_cap, g_db->fcache_cap);
    }
}
#endif

int parse_default_modifier_checked(char*s,TypedField*f){return parse_default_modifier(s,f);}
char *generate_default_owned(const TypedField*f,const char*r,const char*o,size_t cap){
 if(cap<256||f->default_invalid)return NULL;
 char *b=malloc(256);if(!b)return NULL;
 const char*v=generate_default(f,b,256,r,o);
 if(!v){free(b);return NULL;}if(v!=b)snprintf(b,256,"%s",v);return b;
}
````````

<!-- R1-BLOCK s1e6-old -->
````````text
````````

<!-- R1-BLOCK s1e6-new -->
````````c
#include "schema_wire.h"
#include <errno.h>
int typed_field_specs_parse(const char*j,char *s[MAX_FIELDS],size_t*n){(void)j;(void)s;*n=0;errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s1e7-old -->
````````text
````````

<!-- R1-BLOCK s1e7-new -->
````````c
#ifndef SHARD_SCHEMA_WIRE_H
#define SHARD_SCHEMA_WIRE_H
#include "types.h"
/* Caller pins the object schema under its object lock. The checked original
 * declaration is the export identity, not physical descriptor width. */
int schema_wire_quote(FILE *out,const char *text,size_t len);
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget);
int typed_legacy_decl_size(char *clean_type);
int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count);
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines);
char *typed_schema_indexes_json(const char *root,const char *object,size_t budget);
#endif
````````

<!-- R1-BLOCK s1e8-old -->
````````text
````````

<!-- R1-BLOCK s1e8-new -->
````````c
#include "typed_read_command.h"
#include <errno.h>
int typed_read_command(SlotcaskDb*d,const FieldSchema*f,const Schema*s,const TypedReadOptions*o,FILE*out){(void)d;(void)f;(void)s;(void)o;if(out)fputs("{\"error\":\"not implemented\"}\n",out);errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s1e9-old -->
````````text
````````

<!-- R1-BLOCK s1e9-new -->
````````c
#ifndef SHARD_TYPED_READ_COMMAND_H
#define SHARD_TYPED_READ_COMMAND_H
#include "typed_row.h"
/* Caller holds the object read lock, pins matching metadata/registry and has
 * no writer reservation. Owned response chunks are unpublished until success. */
typedef enum { TYPED_READ_FETCH,TYPED_READ_GET_MULTI,TYPED_READ_KEYS,
               TYPED_READ_EXISTS_MULTI,TYPED_READ_NOT_EXISTS } TypedReadKind;
typedef struct {
    TypedReadKind kind;
    const char *format,*fields,*cursor,*keys;
    int offset,limit,want_total;
    char delimiter;
    size_t budget;
    uint32_t timeout_ms;
} TypedReadOptions;
int typed_read_command(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
                        const TypedReadOptions *options,FILE *out);
int typed_read_single(SlotcaskDb *db,const FieldSchema *fs,const void *key,
                       size_t keylen,const char *fields,size_t budget,FILE *out);
int typed_read_route(const char *root,const char *object,const TypedReadOptions *options);
int typed_read_single_route(const char *root,const char *object,const void *key,
    size_t len,const char *fields);
int typed_read_exists(const char *root,const char *object,const void *key,size_t len);
#endif
````````

<!-- R1-BLOCK s1e10-old -->
````````text
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
````````

<!-- R1-BLOCK s1e10-new -->
````````c
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

int typed_update_computed_alloc(const TypedSchema*t,const SlotcaskOldRecord*o,const char*j,uint8_t**b,size_t*n,char*e,size_t c){(void)t;(void)o;(void)j;(void)e;(void)c;*b=NULL;*n=0;errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s1e11-old -->
````````text
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
#endif
````````

<!-- R1-BLOCK s1e11-new -->
````````c
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
#endif

int typed_update_computed_alloc(const TypedSchema*,const SlotcaskOldRecord*,const char*,uint8_t**,size_t*,char*,size_t);
````````

<!-- R1-BLOCK s1e12-old -->
````````text
                           its bytes stay reserved until vacuum compacts them out */
    enum DefaultKind default_kind;
    char default_val[256]; /* literal value, seq name, or random byte count */

    /* not_null=1 rejects every write that would store NULL for this
       field. not_null_set marks that the parsed spec stated nullability
       EXPLICITLY (:not_null or :nullable) — edit-field uses it to tell
       "edit omits nullability → carry the old state" apart from
       ":nullable → remove". Zeroed by the memset at every construction
       site, so both default to unconstrained. */
    uint8_t not_null;
    uint8_t not_null_set;

    /* FT_ENUM only — populated by load_typed_schema, freed by
       free_typed_schema. Empty/zeroed for every other type.
       `enum_values` is a heap-allocated array of strdup'd value strings;
       a record's encoded byte index `i` decodes to `enum_values[i]`. */
    char **enum_values;
    int    n_enum_values;
    int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */
} TypedField;

typedef struct TypedSchema {
    TypedField fields[MAX_FIELDS];
````````

<!-- R1-BLOCK s1e12-new -->
````````c
                           its bytes stay reserved until vacuum compacts them out */
    enum DefaultKind default_kind;
    char default_val[256]; /* literal value, seq name, or random byte count */

    /* not_null=1 rejects every write that would store NULL for this
       field. not_null_set marks that the parsed spec stated nullability
       EXPLICITLY (:not_null or :nullable) — edit-field uses it to tell
       "edit omits nullability → carry the old state" apart from
       ":nullable → remove". Zeroed by the memset at every construction
       site, so both default to unconstrained. */
    uint8_t not_null;
    uint8_t not_null_set;
    uint8_t default_invalid;

    /* FT_ENUM only — populated by load_typed_schema, freed by
       free_typed_schema. Empty/zeroed for every other type.
       `enum_values` is a heap-allocated array of strdup'd value strings;
       a record's encoded byte index `i` decodes to `enum_values[i]`. */
    char **enum_values;
    int    n_enum_values;
    int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */
} TypedField;

typedef struct TypedSchema {
    TypedField fields[MAX_FIELDS];
````````

<!-- R1-BLOCK s1e13-old -->
````````text
   is all-zero bytes; canonical wire form is the 36-char dashed lowercase
   hex. Used by decode_field_to_buf, typed_get_field_str,
   typed_field_to_buf_raw, and decode_idx_to_buf. */
int  uuid_is_zero(const uint8_t b[16]);
int  uuid_format_canonical(char *buf, size_t buflen, const uint8_t b[16]);

/* query.c — estimate-index. Sample N records, project on-disk size for
   a hypothetical trigram index on the given field. Spec format
   "<field>:trigram". See [[trigram-impl-map]] phase 5. */
int cmd_estimate_index(const char *db_root, const char *object,
                       const char *spec);
#endif
````````

<!-- R1-BLOCK s1e13-new -->
````````c
   is all-zero bytes; canonical wire form is the 36-char dashed lowercase
   hex. Used by decode_field_to_buf, typed_get_field_str,
   typed_field_to_buf_raw, and decode_idx_to_buf. */
int  uuid_is_zero(const uint8_t b[16]);
int  uuid_format_canonical(char *buf, size_t buflen, const uint8_t b[16]);

/* query.c — estimate-index. Sample N records, project on-disk size for
   a hypothetical trigram index on the given field. Spec format
   "<field>:trigram". See [[trigram-impl-map]] phase 5. */
int cmd_estimate_index(const char *db_root, const char *object,
                       const char *spec);
#endif

int parse_default_modifier_checked(char*,TypedField*);
char *generate_default_owned(const TypedField*,const char*,const char*,size_t);
````````

<!-- R1-BLOCK s1e14-old -->
````````text
````````

<!-- R1-BLOCK s1e14-new -->
````````c
#define _GNU_SOURCE
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "packed_csv_command.h"
#include "typed_read_command.h"
#include "typed_bulk_insert.h"
#include "schema_wire.h"
#include "fixtures.h"
#include "test_client.h"
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

static char *r_read(CFixture *f,TypedReadKind kind,const char *format,const char *fields,const char *keys,size_t budget,int *rc){
    char *text=NULL;size_t size=0;FILE *out=open_memstream(&text,&size);
    ASSERT_NOT_NULL(out,"read stream");if(!out)return NULL;
    TypedReadOptions options={.kind=kind,.format=format,.fields=fields,.keys=keys,
        .limit=10,.budget=budget,.delimiter=','};
    *rc=typed_read_command(&f->db,&f->fs,&f->sc,&options,out);
    ASSERT_EQ_INT(fclose(out),0,"close read stream");return text;
}
static void r_reads(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    const char *input="{\"f0\":\"1234567890123456.1234567890\",\"f1\":\"a\\u0000b\\\\u0000\",\"f2\":\"1.2300\"}";
    c_seed(&f,"a",1,input,PKR_FLAGS);const char binary[]={'x',0,'z'};c_seed(&f,binary,3,input,PKR_FLAGS);
    int rc=0;char *text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"fetch exact packed JSON");
    ASSERT_CONTAINS(text,"1234567890123456.1234567890","decimal lexeme exact");
    ASSERT_CONTAINS(text,"x\\u0000z","binary key JSON escaped");free(text);
    text=r_read(&f,TYPED_READ_FETCH,"rows","f2,f0",NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"rows projection");ASSERT_CONTAINS(text,"1.2300,1234567890123456.1234567890","rows exact order");free(text);
    text=r_read(&f,TYPED_READ_FETCH,"dict",NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"dict fetch");ASSERT_CONTAINS(text,"\"results\":{","dict envelope");free(text);
    text=r_read(&f,TYPED_READ_GET_MULTI,NULL,NULL,"[\"a\",\"missing\",\"a\",\"x\\u0000z\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"multi-get duplicates and binary key");ASSERT_CONTAINS(text,"\"missing\":null","missing explicit null");free(text);
    text=r_read(&f,TYPED_READ_KEYS,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"keys");ASSERT_CONTAINS(text,"x\\u0000z","keys exact bytes");free(text);
    text=r_read(&f,TYPED_READ_EXISTS_MULTI,NULL,NULL,"[\"a\",\"missing\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"multi exists");ASSERT_CONTAINS(text,"\"a\":true","found true");ASSERT_CONTAINS(text,"\"missing\":false","absent false");free(text);
    text=r_read(&f,TYPED_READ_NOT_EXISTS,NULL,NULL,"[\"a\",\"missing\"]",65536,&rc);
    ASSERT_EQ_INT(rc,0,"missing key list");ASSERT_EQ_STR(text,"[\"missing\"]\n","missing key only");free(text);
    text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,32,&rc);
    ASSERT_EQ_INT(rc,-1,"quota rejection");ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9)&&!strstr(text,"results"),"no partial success prefix");free(text);
    text=r_read(&f,TYPED_READ_GET_MULTI,NULL,NULL,"[\"a\",7]",65536,&rc);
    ASSERT_EQ_INT(rc,-1,"malformed multi-get");ASSERT_TRUE(text&&!strncmp(text,"{\"error\":",9),"malformed request no prefix");free(text);
    ASSERT_EQ_INT(slotcask_write_reservation_held(&f.db),0,"ordinary reads use no writer reservation");
    c_close(&f);
}
static void r_computed(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    uint8_t *old=NULL,*result=NULL;size_t n=0,used=0;char err[256]={0};
    ASSERT_EQ_INT(typed_encode_alloc(f.ts,"{\"f0\":\"1234567890123456.1234567890\",\"f2\":\"1.2300\"}",0,NULL,NULL,&old,&n,err,sizeof(err)),0,"OLD exact encoded");
    SlotcaskOldRecord record={.value=old,.vlen=n,.hdr2=PKR_FLAGS};
    ASSERT_EQ_INT(typed_update_computed_alloc(f.ts,&record,"{\"f0\":{\"$incr\":\"0.00000000005\"},\"f2\":{\"$incr\":\"0.0001\"}}",&result,&used,err,sizeof(err)),0,"exact increments");
    if(result){char *text=typed_decode(f.ts,result,(int)used,PKR_FLAGS);
        ASSERT_CONTAINS(text,"1234567890123456.1234567891","constrained assignment ties away once");
        ASSERT_CONTAINS(text,"1.2301","bare sum scale");free(text);free(result);result=NULL;}
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f0\":{\"$copy\":\"f2\"}}",&result,&used,err,sizeof(err))!=0,"incompatible numeric copy refused");
    ASSERT_TRUE(result==NULL&&used==0,"failure output contract");
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f2\":{\"$incr\":1,\"$copy\":\"f2\"}}",&result,&used,err,sizeof(err))!=0,"mixed operators refused");
    ASSERT_TRUE(typed_update_computed_alloc(f.ts,&record,"{\"f2\":{\"$incr\":\"junk\"},\"f2\":1}",&result,&used,err,sizeof(err))!=0,"earlier invalid computed duplicate cannot be overwritten");
    free(result);result=NULL;used=0;
    char *text=typed_decode(f.ts,old,(int)n,PKR_FLAGS);ASSERT_CONTAINS(text,"1234567890123456.1234567890","OLD unchanged");free(text);free(old);c_close(&f);
}
static void r_declarations(void){
    const char *good="[\"cash:numeric:26,10:default=0:not_null\",\"note:varchar:20:default=a\\\\b\"]";
    char *specs[MAX_FIELDS];size_t count=0;
    ASSERT_EQ_INT(typed_field_specs_parse(good,specs,&count),0,"schema strings decoded once");ASSERT_EQ_INT(count,2,"all declarations");
    if(count==2)ASSERT_EQ_STR(specs[1],"note:varchar:20:default=a\\b","declaration slash exact");
    for(size_t i=0;i<count;i++)free(specs[i]);
    const char *bad[]={"[]","[\"x:int\",\"x:long\"]","[\"x:int\",]","[\"x:int\\u0000\"]","[\"x:int\\n\"]"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++)ASSERT_EQ_INT(typed_field_specs_parse(bad[i],specs,&count),-1,"invalid declarations before mutation");
    const char *invalid[]={"varchar:20:default=random(2)junk","varchar:20:default=seq(a)junk","int:auto_create:auto_update","int:default=1:auto_update"};
    for(size_t i=0;i<sizeof(invalid)/sizeof(invalid[0]);i++){
        char buffer[128];snprintf(buffer,sizeof(buffer),"%s",invalid[i]);TypedField field={0};
        ASSERT_EQ_INT(parse_default_modifier_checked(buffer,&field),-1,"generator/default complete consumption");
    }
    char valid[]="varchar:300:default=random(150):not_null";TypedField field={0};
    ASSERT_EQ_INT(parse_default_modifier_checked(valid,&field),1,"generated default > old fixed buffer");
    parse_field_type(valid,&field);char *value=generate_default_owned(&field,NULL,NULL,1024);
    ASSERT_NOT_NULL(value,"owned random default");if(value)ASSERT_EQ_INT(strlen(value),300,"no 256 byte truncation");free(value);
    field.default_invalid=1;ASSERT_TRUE(generate_default_owned(&field,NULL,NULL,1024)==NULL,"legacy malformed default cannot generate truncated value");
}

static void r_schema_routes(void){
    TestEnv env={0};if(test_env_start(&env))return;
    TestClientCfg cfg={.port=env.port,.io_timeout_ms=30000};TestClient *client=tc_connect(&cfg);
    ASSERT_NOT_NULL(client,"schema route connect");if(!client){test_env_stop(&env);return;}
    char *response=NULL;
    tc_request(client,"{\"mode\":\"add-dir\",\"dir\":\"r1\"}",&response);free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"fields\":[\"\\u0063ash:numeric:26,10:default=0:not_null\",\"note:varchar:30:default=a\\\\b\"]}",&response);
    ASSERT_TRUE(response&&!strstr(response,"\"error\""),"escaped declarations accepted");free(response);
    tc_request(client,"{\"mode\":\"describe-object\",\"dir\":\"r1\",\"object\":\"roundtrip\"}",&response);
    ASSERT_CONTAINS(response,"\"spec\":\"cash:numeric:26,10:default=0:not_null\"","original precision and modifier declaration preserved");
    ASSERT_CONTAINS(response,"\"declaration_layout\":\"legacy\"","legacy layout identity explicit");free(response);
    tc_request(client,"{\"mode\":\"add-field\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"fields\":[\"escaped:varchar:30:default=x\\\\y\"]}",&response);
    ASSERT_TRUE(response&&!strstr(response,"\"error\""),"add-field unescapes once");free(response);
    tc_request(client,"{\"mode\":\"edit-field\",\"dir\":\"r1\",\"object\":\"roundtrip\",\"fields\":[\"escaped:varchar:30:default=random(2)junk\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","bad default edit refused");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"bad\",\"fields\":[\"x:int:default=999999999999\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","invalid literal default refused at create");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"wronglayout\",\"declaration_layout\":\"packed\",\"fields\":[\"x:numeric:26,10\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","packed export never silently imported as legacy");free(response);
    tc_request(client,"{\"mode\":\"create-object\",\"dir\":\"r1\",\"object\":\"streamloss\",\"declaration_layout\":\"legacy\",\"streams\":3,\"fields\":[\"x:int\"]}",&response);
    ASSERT_CONTAINS(response,"\"error\"","unsupported imported stream count never silently replaced");free(response);
    tc_close(client);test_env_stop(&env);
}


static char *r_bulk_call(CFixture *f,const char *json,int *rc){
    char *text=NULL;size_t length=0;FILE *out=open_memstream(&text,&length);
    ASSERT_NOT_NULL(out,"bulk stream");if(!out){*rc=-1;return NULL;}
    FILE *saved=g_out;g_out=out;
    *rc=typed_bulk_insert_command(f->root,"obj",&f->sc,f->ts,&f->db,json,strlen(json),0,',',0);
    g_out=saved;ASSERT_EQ_INT(fclose(out),0,"bulk close");return text;
}
static void r_bulk_routes(void){
    CFixture f;if(c_open(&f,0)){c_close(&f);return;}
    f.sc.auto_key=AK_UUID;f.sc.max_key=16;f.fs.auto_key=AK_UUID;f.fs.auto_key_schema_snapshot=f.sc;
    int rc=0;char *text=r_bulk_call(&f,"[{\"value\":{\"f0\":\"1234567890123456.1234567890\"}},{\"value\":{\"f2\":\"0.0000000000000000000001\"}}]",&rc);
    ASSERT_EQ_INT(rc,0,"bulk omitted auto keys supported");ASSERT_CONTAINS(text,"\"inserted\":2","both rows inserted");ASSERT_CONTAINS(text,"\"keys\":[","generated keys returned");free(text);
    text=r_read(&f,TYPED_READ_FETCH,NULL,NULL,NULL,65536,&rc);
    ASSERT_EQ_INT(rc,0,"read auto-key bulk results");ASSERT_CONTAINS(text,"1234567890123456.1234567890","bulk exact constrained value");ASSERT_CONTAINS(text,"0.0000000000000000000001","bulk exact bare value");free(text);
    text=r_bulk_call(&f,"[{\"key\":\"00112233-4455-6677-8899-aabbccddeeff\",\"value\":{}},{\"key\":\"00112233-4455-6677-8899-AABBCCDDEEFF\",\"value\":{}}]",&rc);
    ASSERT_TRUE(rc!=0,"normalized duplicate keys rejected before request");ASSERT_CONTAINS(text,"\"error\"","duplicate explicit error");free(text);c_close(&f);
}

static int test_r1_routes(void){r_reads();r_computed();r_declarations();r_schema_routes();r_bulk_routes();return t_ctx->failed?1:0;}
TEST_REGISTER("test-r1-routes",test_r1_routes)
````````

<!-- R1-BLOCK s2e15-old -->
````````text
} CritRow;

#define MAX_CRIT_ROWS 16

/* List-with-actions UX over an in/out CritRow array. *n_io is the current
   row count; the builder updates rows[] and n_io as the user adds, edits,
   and deletes. Returns 0 on submit with criteria_out set to a malloc'd
   JSON array (caller frees), or -1 on cancel. AND-only. */
int tui_criteria_builder(const ObjectInfo *oi,
                         CritRow *rows, int *n_io,
                         char **criteria_out);

#endif
````````

<!-- R1-BLOCK s2e15-new -->
````````c
} CritRow;

#define MAX_CRIT_ROWS 16

/* List-with-actions UX over an in/out CritRow array. *n_io is the current
   row count; the builder updates rows[] and n_io as the user adds, edits,
   and deletes. Returns 0 on submit with criteria_out set to a malloc'd
   JSON array (caller frees), or -1 on cancel. AND-only. */
int tui_criteria_builder(const ObjectInfo *oi,
                         CritRow *rows, int *n_io,
                         char **criteria_out);



/* Shared read-only JSON span helpers: returned pointers borrow the input. */
const char *cli_json_skip_ws(const char *text);
const char *cli_json_skip_value(const char **text);
const char *cli_json_find_key(const char *json,const char *key,size_t *length);

#endif
````````

<!-- R1-BLOCK s2e16-old -->
````````text
/* shard-cli main entry — parses env (db.env should be sourced), opens a
   connection (lazy on first action), drives the top-level menu. */

#define _GNU_SOURCE
#include "cli.h"
#include <unistd.h>
#include <sys/wait.h>
#include <spawn.h>

/* ---- helpers ---- */

static CliConn *g_conn = NULL;

static CliConn *get_conn(void) {
    if (g_conn) return g_conn;
    g_conn = cli_connect(g_cli_host, g_cli_port);
    if (!g_conn)
````````

<!-- R1-BLOCK s2e16-new -->
````````c
#include <limits.h>
#include <stdint.h>
#include <unistd.h>
/* shard-cli main entry — parses env (db.env should be sourced), opens a
   connection (lazy on first action), drives the top-level menu. */

#define _GNU_SOURCE
#include "cli.h"
#include <limits.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/wait.h>
#include <spawn.h>

/* ---- helpers ---- */

static CliConn *g_conn = NULL;

static CliConn *get_conn(void) {
    if (g_conn) return g_conn;
    g_conn = cli_connect(g_cli_host, g_cli_port);
    if (!g_conn)
````````

<!-- R1-BLOCK s2e17-old -->
````````text
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "describe-object failed");
        return;
    }
    char title[128];
    snprintf(title, sizeof(title), "%s/%s", dir, object);
    show_response(title, resp);
    free(resp);
}

static int collect_string_array(const char *resp, const char *key,
                                char ***out_arr, int *out_n) {
    /* Parse {"key":"...","key2":[...]} where key2 is an array of strings.
       Or top-level array if key is NULL. */
    const char *arr = resp;
    if (key) {
        size_t vlen;
        extern const char *json_find_key_extern(const char *, const char *, size_t *);
        /* Inline parse — duplicate of json_find_key in views.c kept private. */
        const char *p = resp;
        while (*p && *p != '[') {
            if (strncmp(p, key, strlen(key)) == 0 &&
                p > resp && p[-1] == '"') {
                p += strlen(key);
                while (*p && *p != '[' && *p != '{') p++;
                arr = p;
                break;
            }
            p++;
        }
        (void)vlen;
    }
    if (!arr || *arr != '[') return -1;
    /* Walk array. */
    int cap = 16, n = 0;
    char **out = malloc(cap * sizeof(char *));
    const char *p = arr + 1;
    while (*p) {
        while (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t') p++;
        if (*p == ']' || *p == '\0') break;
        if (*p != '"') break;
        p++;
        const char *s = p;
        while (*p && *p != '"') p++;
        size_t L = p - s;
        if (n >= cap) {
            cap *= 2;
            char **t = xrealloc_or_free(out, (size_t)cap * sizeof(char *));
            if (!t) { *out_arr = NULL; *out_n = 0; return -1; }
            out = t;
        }
        out[n] = malloc(L + 1);
        memcpy(out[n], s, L);
        out[n][L] = '\0';
        n++;
        if (*p == '"') p++;
    }
    *out_arr = out;
    *out_n = n;
    return 0;
}

/* Loop at the object-pick level: ←/q goes back to tenant pick; describe view
   closes back to the object pick. Each level loops until user backs out. */
static void menu_browse_object_list(CliConn *c, const char *dir) {
    for (;;) {
        char req[256];
        snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dir);
        char *resp = NULL;
        size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) {
            tui_alert("error", "list-objects failed");
````````

<!-- R1-BLOCK s2e17-new -->
````````c
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0) {
        tui_alert("error", "describe-object failed");
        return;
    }
    char title[128];
    snprintf(title, sizeof(title), "%s/%s", dir, object);
    show_response(title, resp);
    free(resp);
}

static int transfer_hex(unsigned char c){
    if(c>='0'&&c<='9')return c-'0';
    if(c>='a'&&c<='f')return c-'a'+10;
    if(c>='A'&&c<='F')return c-'A'+10;
    return -1;
}
static int transfer_code(const char **text,unsigned *code){
    unsigned value=0;
    for(int i=0;i<4;i++){
        int digit=transfer_hex((unsigned char)**text);if(digit<0)return -1;
        (*text)++;value=value*16+(unsigned)digit;
    }
    *code=value;return 0;
}
static char *transfer_string(const char **text){
    const char *start=*text;if(*start!='"')return NULL;
    const char *end=start;cli_json_skip_value(&end);
    size_t raw=(size_t)(end-start);
    if(raw<2||end[-1]!='"')return NULL;
    char *out=malloc(raw);if(!out)return NULL;
    size_t n=0;const char *p=start+1;
    while(p<end-1){
        unsigned code=(unsigned char)*p++;
        if(code<32)goto fail;
        if(code=='\\'){
            unsigned char e=(unsigned char)*p++;
            switch(e){
            case '"':code='"';break;case '\\':code='\\';break;case '/':code='/';break;
            case 'b':code=8;break;case 'f':code=12;break;case 'n':code=10;break;
            case 'r':code=13;break;case 't':code=9;break;
            case 'u':
                if(end-1-p<4||transfer_code(&p,&code))goto fail;
                if(code>=0xd800&&code<=0xdbff){
                    unsigned low=0;
                    if(end-1-p<6||p[0]!='\\'||p[1]!='u')goto fail;
                    p+=2;if(transfer_code(&p,&low)||low<0xdc00||low>0xdfff)goto fail;
                    code=0x10000+((code-0xd800)<<10)+(low-0xdc00);
                }else if(code>=0xdc00&&code<=0xdfff)goto fail;
                if(!code)goto fail;
                if(code>=0x10000){out[n++]=(char)(0xf0|(code>>18));out[n++]=(char)(0x80|((code>>12)&63));}
                else if(code>=0x800)out[n++]=(char)(0xe0|(code>>12));
                if(code>=0x80)out[n++]=(char)(code>=0x800?0x80|((code>>6)&63):0xc0|(code>>6));
                code=code<0x80?code:0x80|(code&63);break;
            default:goto fail;
            }
        }
        out[n++]=(char)code;
    }
    out[n]=0;*text=end;return out;
fail:free(out);return NULL;
}
static int collect_string_array(const char *resp,const char *key,char ***out_arr,int *out_n){
    if(!out_arr||!out_n)return -1;
    *out_arr=NULL;*out_n=0;
    size_t length=0;const char *array=key?cli_json_find_key(resp,key,&length):resp;
    const char *p=array?cli_json_skip_ws(array):NULL;
    if(!p||*p++!='[')return -1;
    char **rows=NULL;int count=0;size_t capacity=0;
    p=cli_json_skip_ws(p);
    while(*p!=']'){
        char *value=transfer_string(&p);if(!value)goto fail;
        if((size_t)count==capacity){
            size_t next=capacity?capacity*2:16;
            if(next>SIZE_MAX/sizeof(*rows)||next>INT_MAX){free(value);goto fail;}
            char **grown=realloc(rows,next*sizeof(*rows));if(!grown){free(value);goto fail;}
            rows=grown;capacity=next;
        }
        rows[count++]=value;p=cli_json_skip_ws(p);
        if(*p==']')break;
        if(*p++!=',')goto fail;
        p=cli_json_skip_ws(p);if(*p==']')goto fail;
    }
    if(!key&&*cli_json_skip_ws(p+1))goto fail;
    *out_arr=rows;*out_n=count;return 0;
fail:
    for(int i=0;i<count;i++)free(rows[i]);
    free(rows);return -1;
}

/* Loop at the object-pick level: ←/q goes back to tenant pick; describe view
   closes back to the object pick. Each level loops until user backs out. */
static void menu_browse_object_list(CliConn *c, const char *dir) {
    for (;;) {
        char req[256];
        snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dir);
        char *resp = NULL;
        size_t rlen = 0;
        if (cli_query(c, req, &resp, &rlen) != 0) {
            tui_alert("error", "list-objects failed");
````````

<!-- R1-BLOCK s2e18-old -->
````````text

/* ---- Schema Transfer menu ---- schema export/import for local→prod bootstrap.
   No data, no tokens — just object/dir/index definitions. Roundtrip uses
   the existing db-dirs, list-objects, describe-object, and create-object
   modes; no server-side changes needed. */

/* Reconstruct the "name:type[:size][:scale]" spec string the daemon expects
   when it parses a fields.conf entry. describe-object emits the on-disk
   varchar size (= N+2 for the length prefix), so we strip 2 to recover the
   user-facing N. Numeric P (precision) isn't preserved — describe-object
   only round-trips S (scale) — so we default P=18 (max int64 digits) on
   export. P is informational; it doesn't affect storage. */
static void cached_field_to_spec(const CachedField *fd, char *out, size_t out_sz) {
    if (strcmp(fd->type, "varchar") == 0) {
        int n = fd->size - 2;
        if (n < 0) n = 0;
        snprintf(out, out_sz, "%s:%s:%d", fd->name, fd->type, n);
    } else if (strcmp(fd->type, "numeric") == 0) {
        snprintf(out, out_sz, "%s:%s:18,%d", fd->name, fd->type, fd->scale);
    } else {
        snprintf(out, out_sz, "%s:%s", fd->name, fd->type);
    }
}

static void schema_transfer_export(CliConn *c) {
    FormField fs[1] = {0};
    fs[0].label = "output path"; fs[0].kind = FF_TEXT;
    snprintf(fs[0].value, sizeof(fs[0].value), "schema.json");
    if (tui_form("export-schema → file", fs, 1) != 0) return;
    if (!fs[0].value[0]) { tui_alert("export", "output path required"); return; }

    FILE *out = fopen(fs[0].value, "w");
    if (!out) { tui_alert("export", "cannot open output file for writing"); return; }

    char *resp = NULL; size_t rlen = 0;
    if (cli_query(c, "{\"mode\":\"db-dirs\"}", &resp, &rlen) != 0) {
        tui_alert("error", "db-dirs failed"); fclose(out); return;
    }
    char **dirs = NULL; int ndirs = 0;
    collect_string_array(resp, NULL, &dirs, &ndirs);
    free(resp);

    fprintf(out, "{\n");
    fprintf(out, "  \"version\": \"2026.05\",\n");
    fprintf(out, "  \"dirs\": [");
    for (int i = 0; i < ndirs; i++) fprintf(out, "%s\"%s\"", i ? "," : "", dirs[i]);
    fprintf(out, "],\n");
    fprintf(out, "  \"objects\": [\n");

    int total = 0;
    int first = 1;
    for (int i = 0; i < ndirs; i++) {
        char req[256];
        snprintf(req, sizeof(req), "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dirs[i]);
        char *list_resp = NULL;
        if (cli_query(c, req, &list_resp, &rlen) != 0) continue;
        char **objs = NULL; int nobjs = 0;
        collect_string_array(list_resp, "objects", &objs, &nobjs);
        free(list_resp);
        for (int j = 0; j < nobjs; j++) {
            ObjectInfo oi;
            if (describe_object(c, dirs[i], objs[j], &oi) != 0) continue;
            if (!first) fprintf(out, ",\n");
            fprintf(out,
                "    {\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"fields\":[",
                oi.dir, oi.object, oi.splits, oi.max_key);
            for (int f = 0; f < oi.nfields; f++) {
                char spec[256];
                cached_field_to_spec(&oi.fields[f], spec, sizeof(spec));
                fprintf(out, "%s\"%s\"", f ? "," : "", spec);
            }
            fprintf(out, "],\"indexes\":[");
            for (int x = 0; x < oi.nindexes; x++)
                fprintf(out, "%s\"%s\"", x ? "," : "", oi.indexes[x]);
            fprintf(out, "]}");
            first = 0;
            total++;
        }
        for (int j = 0; j < nobjs; j++) free(objs[j]);
        free(objs);
    }
    fprintf(out, "\n  ]\n}\n");
    fclose(out);

    for (int i = 0; i < ndirs; i++) free(dirs[i]);
    free(dirs);

    char msg[512];
    snprintf(msg, sizeof(msg),
        "exported %d object%s across %d tenant%s to %s\n\n"
        "no data and no tokens were copied — only schema/index definitions.\n"
        "use Schema Transfer → Import on the prod machine to recreate.",
        total, total == 1 ? "" : "s",
        ndirs, ndirs == 1 ? "" : "s", fs[0].value);
    tui_alert("export-schema", msg);
}

/* Walk a JSON array of object entries and dispatch one create-object per row.
   Each row has dir/object/splits/max_key/fields[]/indexes[]. We pass the
   entry's `fields` and `indexes` arrays through verbatim — the manifest's
   shape exactly matches what create-object expects. */
typedef struct {
    CliConn *c;
    int      created;
    int      skipped;   /* if_not_exists hit */
    int      failed;
    int      if_not_exists;
} ImportCtx;
````````

<!-- R1-BLOCK s2e18-new -->
````````c

/* ---- Schema Transfer menu ---- schema export/import for local→prod bootstrap.
   No data, no tokens — just object/dir/index definitions. Roundtrip uses
   the existing db-dirs, list-objects, describe-object, and create-object
   modes; no server-side changes needed. */

/* Reconstruct the "name:type[:size][:scale]" spec string the daemon expects
   when it parses a fields.conf entry. describe-object emits the on-disk
   varchar size (= N+2 for the length prefix), so we strip 2 to recover the
   user-facing N. Numeric P (precision) isn't preserved — describe-object
   only round-trips S (scale) — so we default P=18 (max int64 digits) on
   export. P is informational; it doesn't affect storage. */


static int transfer_quote(FILE *out,const char *s,size_t n){
    static const char hex[]="0123456789abcdef";
    if(fputc('"',out)==EOF)return -1;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='"'||c=='\\'){if(fputc('\\',out)==EOF||fputc(c,out)==EOF)return -1;}
        else if(c<32){char escape[6]={'\\','u','0','0',hex[c>>4],hex[c&15]};if(fwrite(escape,1,6,out)!=6)return -1;}
        else if(fputc(c,out)==EOF)return -1;
    }
    return fputc('"',out)==EOF?-1:0;
}
static int transfer_specs(FILE *out,const char *array,size_t length){
    if(length<2||array[0]!='['||array[length-1]!=']')return -1;
    const char *p=array+1,*end=array+length-1;int first=1;
    while((p=cli_json_skip_ws(p))<end){
        if(*p!='{')return -1;
        const char *start=p;cli_json_skip_value(&p);if(p>end)return -1;
        size_t n=0;const char *spec=cli_json_find_key(start,"spec",&n);
        if(!spec||n<2||spec[0]!='"'||spec[n-1]!='"'||spec+n>p)return -1;
        if((!first&&fputc(',',out)==EOF)||fwrite(spec,1,n,out)!=n)return -1;
        first=0;p=cli_json_skip_ws(p);
        if(p<end){if(*p++!=',')return -1;if(cli_json_skip_ws(p)==end)return -1;}
    }
    return 0;
}
static int transfer_publish(FILE *stage,const char *path){
    if(ferror(stage)||fflush(stage)||fseek(stage,0,SEEK_SET))return -1;
    size_t n=strlen(path);if(n>SIZE_MAX-16)return -1;
    char *temporary=malloc(n+16);if(!temporary)return -1;
    snprintf(temporary,n+16,"%s.tmp.XXXXXX",path);int fd=mkstemp(temporary);
    if(fd<0){free(temporary);return -1;}
    FILE *out=fdopen(fd,"w");if(!out){close(fd);unlink(temporary);free(temporary);return -1;}
    int error=0;char block[8192];size_t take;
    while((take=fread(block,1,sizeof(block),stage)))if(fwrite(block,1,take,out)!=take){error=1;break;}
    if(ferror(stage)||ferror(out)||fflush(out)||fsync(fileno(out)))error=1;
    if(fclose(out))error=1;
    if(!error&&rename(temporary,path))error=1;
    if(error)unlink(temporary);
    free(temporary);return error?-1:0;
}
static int transfer_request(CliConn *c,const char *mode,const char *dir,
    const char *object,char **response,size_t *length){
    *response=NULL;*length=0;
    char *request=NULL;size_t n=0;FILE *out=open_memstream(&request,&n);
    if(!out)return -1;
    int error=fputs("{\"mode\":",out)==EOF||transfer_quote(out,mode,strlen(mode));
    if(!error&&dir)error=fputs(",\"dir\":",out)==EOF||transfer_quote(out,dir,strlen(dir));
    if(!error&&object)error=fputs(",\"object\":",out)==EOF||transfer_quote(out,object,strlen(object));
    if(fputc('}',out)==EOF)error=1;
    if(fclose(out))error=1;
    if(!error)error=cli_query(c,request,response,length)!=0;
    free(request);
    if(!error&&(!*response||cli_json_find_key(*response,"error",length)))error=1;
    return error?-1:0;
}
static void schema_transfer_export(CliConn *c){
    FormField field[1]={0};field[0].label="output path";field[0].kind=FF_TEXT;
    snprintf(field[0].value,sizeof(field[0].value),"schema.json");
    if(tui_form("export-schema → file",field,1))return;
    if(!field[0].value[0]){tui_alert("export","output path required");return;}
    FILE *stage=tmpfile();if(!stage){tui_alert("export","cannot stage schema manifest");return;}
    char *response=NULL;size_t length=0;char **dirs=NULL;int ndirs=0,total=0,first=1,error=0;
    if(transfer_request(c,"db-dirs",NULL,NULL,&response,&length))error=1;
    if(!error&&collect_string_array(response,NULL,&dirs,&ndirs))error=1;
    free(response);response=NULL;
    if(fputs("{\"dirs\":[",stage)==EOF)error=1;
    for(int i=0;i<ndirs&&!error;i++)if((i&&fputc(',',stage)==EOF)||transfer_quote(stage,dirs[i],strlen(dirs[i])))error=1;
    if(fputs("],\"objects\":[",stage)==EOF)error=1;
    for(int i=0;i<ndirs&&!error;i++){
        char **objects=NULL;int count=0;
        if(transfer_request(c,"list-objects",dirs[i],NULL,&response,&length))error=1;
        if(!error&&collect_string_array(response,"objects",&objects,&count))error=1;
        free(response);response=NULL;
        for(int j=0;j<count&&!error;j++){
            if(transfer_request(c,"describe-object",dirs[i],objects[j],&response,&length)){error=1;free(response);response=NULL;break;}
            size_t fn=0,in=0;const char *fields=cli_json_find_key(response,"fields",&fn),*indexes=cli_json_find_key(response,"indexes",&in);
            if(!fields||!indexes){error=1;free(response);response=NULL;break;}
            if((!first&&fputc(',',stage)==EOF)||fputs("{\"dir\":",stage)==EOF||transfer_quote(stage,dirs[i],strlen(dirs[i]))||
               fputs(",\"object\":",stage)==EOF||transfer_quote(stage,objects[j],strlen(objects[j])))error=1;
            const char *names[]={"splits","max_key","streams","auto_key","declaration_layout"};
            for(size_t k=0;k<sizeof(names)/sizeof(names[0])&&!error;k++){
                size_t n=0;const char *value=cli_json_find_key(response,names[k],&n);
                if(value&&(fprintf(stage,",\"%s\":",names[k])<0||fwrite(value,1,n,stage)!=n))error=1;
            }
            if(!error&&(fputs(",\"fields\":[",stage)==EOF||transfer_specs(stage,fields,fn)||
                fputs("],\"indexes\":",stage)==EOF||fwrite(indexes,1,in,stage)!=in||fputc('}',stage)==EOF))error=1;
            first=0;total++;free(response);response=NULL;
        }
        for(int j=0;j<count;j++)free(objects[j]);
        free(objects);
    }
    if(fputs("]}\n",stage)==EOF)error=1;
    if(!error&&transfer_publish(stage,field[0].value))error=1;
    if(fclose(stage))error=1;
    for(int i=0;i<ndirs;i++)free(dirs[i]);
    free(dirs);free(response);
    if(error)tui_alert("export","schema export failed; no complete new manifest published");
    else{char message[128];snprintf(message,sizeof(message),"exported %d objects",total);tui_alert("export-schema",message);}
}


/* Walk a JSON array of object entries and dispatch one create-object per row.
   Each row has dir/object/splits/max_key/fields[]/indexes[]. We pass the
   entry's `fields` and `indexes` arrays through verbatim — the manifest's
   shape exactly matches what create-object expects. */
typedef struct {
    CliConn *c;
    int      created;
    int      skipped;   /* if_not_exists hit */
    int      failed;
    int      if_not_exists;
} ImportCtx;
````````

<!-- R1-BLOCK s2e19-old -->
````````text
                object[ve - vs - 1] = '\0';
            }
        }
        p = colon + 1;
        /* Skip past this value naively — fine since we only care about two
           keys and any miss just means we move on. */
        const char *next = strchr(p, ',');
        if (!next || next >= end) break;
        p = next + 1;
    }

    /* Build request: prepend mode, append if_not_exists if requested. */
    size_t req_cap = len + 256;
    char *req = malloc(req_cap);
    /* The manifest entry is {"dir":"...","object":"...","splits":...,...}.
       create-object expects the same shape under mode "create-object", so
       we just prepend "mode":"create-object", into the entry. */
    if (len < 2) { free(req); ic->failed++; return 0; }
    int n;
    if (ic->if_not_exists) {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",\"if_not_exists\":true,%.*s",
            (int)(len - 1), elem + 1);
    } else {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",%.*s",
            (int)(len - 1), elem + 1);
    }
    (void)n;

    char *resp = NULL; size_t rlen = 0;
    int rc = cli_query(ic->c, req, &resp, &rlen);
    free(req);
    if (rc != 0 || !resp) { ic->failed++; if (resp) free(resp); return 0; }
    /* Parse status/error from response. */
    if (strstr(resp, "\"status\":\"created\""))    ic->created++;
    else if (strstr(resp, "\"status\":\"exists\""))ic->skipped++;
    else if (strstr(resp, "\"error\":"))           ic->failed++;
    else                                           ic->created++;
    free(resp);
    return 0;
````````

<!-- R1-BLOCK s2e19-new -->
````````c
                object[ve - vs - 1] = '\0';
            }
        }
        p = colon + 1;
        /* Skip past this value naively — fine since we only care about two
           keys and any miss just means we move on. */
        const char *next = strchr(p, ',');
        if (!next || next >= end) break;
        p = next + 1;
    }

    /* Build request: prepend mode, append if_not_exists if requested. */
    if(len<2||len>INT_MAX||len>SIZE_MAX-256){ic->failed++;return 0;}
    size_t req_cap=len+256;
    char *req=malloc(req_cap);
    if(!req){ic->failed++;return 0;}
    /* The manifest entry is {"dir":"...","object":"...","splits":...,...}.
       create-object expects the same shape under mode "create-object", so
       we just prepend "mode":"create-object", into the entry. */
    if (len < 2) { free(req); ic->failed++; return 0; }
    int n;
    if (ic->if_not_exists) {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",\"if_not_exists\":true,%.*s",
            (int)(len - 1), elem + 1);
    } else {
        n = snprintf(req, req_cap,
            "{\"mode\":\"create-object\",%.*s",
            (int)(len - 1), elem + 1);
    }
    if(n<0||(size_t)n>=req_cap){free(req);ic->failed++;return 0;}

    char *resp = NULL; size_t rlen = 0;
    int rc = cli_query(ic->c, req, &resp, &rlen);
    free(req);
    if (rc != 0 || !resp) { ic->failed++; if (resp) free(resp); return 0; }
    /* Parse status/error from response. */
    if (strstr(resp, "\"status\":\"created\""))    ic->created++;
    else if (strstr(resp, "\"status\":\"exists\""))ic->skipped++;
    else if (strstr(resp, "\"error\":"))           ic->failed++;
    else                                           ic->created++;
    free(resp);
    return 0;
````````

<!-- R1-BLOCK s2e20-old -->
````````text
   for menu population and result display. */

#define _GNU_SOURCE
#include "cli.h"

#include <stdarg.h>
#include <unistd.h>

/* ============================================================
   Tiny JSON parser
   ============================================================ */

static const char *skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* If *p is at a JSON value, advance *p past the entire value (string/number/
   bool/null/object/array). Returns the start byte offset (so caller can copy
   the slice). Updates *p to one past the value. */
static const char *skip_value(const char **p) {
    *p = skip_ws(*p);
    const char *start = *p;
    if (**p == '"') {
        (*p)++;
        while (**p && **p != '"') {
            if (**p == '\\' && *(*p + 1)) (*p)++;
            (*p)++;
        }
        if (**p == '"') (*p)++;
    } else if (**p == '{' || **p == '[') {
        char open = **p, close = (open == '{' ? '}' : ']');
        int depth = 0;
        do {
````````

<!-- R1-BLOCK s2e20-new -->
````````c
   for menu population and result display. */

#define _GNU_SOURCE
#include "cli.h"

#include <stdarg.h>
#include <unistd.h>

/* ============================================================
   Tiny JSON parser
   ============================================================ */

const char *cli_json_skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* If *p is at a JSON value, advance *p past the entire value (string/number/
   bool/null/object/array). Returns the start byte offset (so caller can copy
   the slice). Updates *p to one past the value. */
const char *cli_json_skip_value(const char **p) {
    *p = cli_json_skip_ws(*p);
    const char *start = *p;
    if (**p == '"') {
        (*p)++;
        while (**p && **p != '"') {
            if (**p == '\\' && *(*p + 1)) (*p)++;
            (*p)++;
        }
        if (**p == '"') (*p)++;
    } else if (**p == '{' || **p == '[') {
        char open = **p, close = (open == '{' ? '}' : ']');
        int depth = 0;
        do {
````````

<!-- R1-BLOCK s2e21-old -->
````````text
        } while (depth > 0 && **p);
    } else {
        while (**p && **p != ',' && **p != '}' && **p != ']' &&
               **p != ' ' && **p != '\n' && **p != '\t' && **p != '\r')
            (*p)++;
    }
    return start;
}

/* Find a top-level "key" inside a JSON object string. Returns pointer to the
   value start (just past the colon's whitespace), or NULL. *value_len is set
   to the length of the value's verbatim slice. */
static const char *json_find_key(const char *json, const char *key, size_t *value_len) {
    size_t klen = strlen(key);
    const char *p = skip_ws(json);
    if (*p != '{') return NULL;
    p++;
    while (*p) {
        p = skip_ws(p);
        if (*p == '}') return NULL;
        if (*p != '"') return NULL;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t this_klen = p - kstart;
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') return NULL;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        if (this_klen == klen && memcmp(kstart, key, klen) == 0) {
            *value_len = (size_t)(p - vstart);
            return vstart;
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return NULL;
}

/* Copy an unescaped JSON string value into out (max out_sz-1 bytes). The
   value at vstart is `"...."` with quotes; this strips them and unescapes
   common escapes. Returns 0 on success. */
static int json_string_into(const char *vstart, size_t vlen, char *out, size_t out_sz) {
    if (vlen < 2 || vstart[0] != '"') {
        /* Not a string — emit verbatim slice. */
        size_t n = vlen < out_sz - 1 ? vlen : out_sz - 1;
````````

<!-- R1-BLOCK s2e21-new -->
````````c
        } while (depth > 0 && **p);
    } else {
        while (**p && **p != ',' && **p != '}' && **p != ']' &&
               **p != ' ' && **p != '\n' && **p != '\t' && **p != '\r')
            (*p)++;
    }
    return start;
}

/* Find a top-level "key" inside a JSON object string. Returns pointer to the
   value start (just past the colon's whitespace), or NULL. *value_len is set
   to the length of the value's verbatim slice. */
const char *cli_json_find_key(const char *json, const char *key, size_t *value_len) {
    size_t klen = strlen(key);
    const char *p = cli_json_skip_ws(json);
    if (*p != '{') return NULL;
    p++;
    while (*p) {
        p = cli_json_skip_ws(p);
        if (*p == '}') return NULL;
        if (*p != '"') return NULL;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t this_klen = p - kstart;
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') return NULL;
        p++;
        p = cli_json_skip_ws(p);
        const char *vstart = p;
        cli_json_skip_value(&p);
        if (this_klen == klen && memcmp(kstart, key, klen) == 0) {
            *value_len = (size_t)(p - vstart);
            return vstart;
        }
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    return NULL;
}

/* Copy an unescaped JSON string value into out (max out_sz-1 bytes). The
   value at vstart is `"...."` with quotes; this strips them and unescapes
   common escapes. Returns 0 on success. */
static int json_string_into(const char *vstart, size_t vlen, char *out, size_t out_sz) {
    if (vlen < 2 || vstart[0] != '"') {
        /* Not a string — emit verbatim slice. */
        size_t n = vlen < out_sz - 1 ? vlen : out_sz - 1;
````````

<!-- R1-BLOCK s2e22-old -->
````````text
    return 0;
}

/* Iterate elements of a JSON array slice (vstart includes the [ ]). Calls
   cb for each element with its verbatim slice. Stops when cb returns nonzero. */
static void json_array_iter(const char *vstart, size_t vlen,
                            int (*cb)(const char *elem, size_t len, void *ctx),
                            void *ctx) {
    if (vlen < 2 || vstart[0] != '[') return;
    const char *p = vstart + 1;
    const char *end = vstart + vlen - 1; /* points at closing ] */
    while (p < end) {
        p = skip_ws(p);
        if (p >= end || *p == ']') break;
        const char *estart = p;
        skip_value(&p);
        size_t elen = (size_t)(p - estart);
        if (cb(estart, elen, ctx) != 0) return;
        p = skip_ws(p);
        if (*p == ',') p++;
    }
}

/* ============================================================
   Text + JSON object display
   ============================================================ */

int tui_preview_json(const char *title, const char *body) {
    int rows, cols;
    int top = 0;
    int blen = (int)strlen(body);
````````

<!-- R1-BLOCK s2e22-new -->
````````c
    return 0;
}

/* Iterate elements of a JSON array slice (vstart includes the [ ]). Calls
   cb for each element with its verbatim slice. Stops when cb returns nonzero. */
static void json_array_iter(const char *vstart, size_t vlen,
                            int (*cb)(const char *elem, size_t len, void *ctx),
                            void *ctx) {
    if (vlen < 2 || vstart[0] != '[') return;
    const char *p = vstart + 1;
    const char *end = vstart + vlen - 1; /* points at closing ] */
    while (p < end) {
        p = cli_json_skip_ws(p);
        if (p >= end || *p == ']') break;
        const char *estart = p;
        cli_json_skip_value(&p);
        size_t elen = (size_t)(p - estart);
        if (cb(estart, elen, ctx) != 0) return;
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
}

/* ============================================================
   Text + JSON object display
   ============================================================ */

int tui_preview_json(const char *title, const char *body) {
    int rows, cols;
    int top = 0;
    int blen = (int)strlen(body);
````````

<!-- R1-BLOCK s2e23-old -->
````````text
                return 0;
        }
    }
}

/* Iterate JSON array of objects, call cb for each {…} slice. */
static void iter_array_objs(const char *vstart, size_t vlen,
                            void (*cb)(const char *o, size_t olen, void *ctx),
                            void *ctx) {
    if (vlen < 2 || vstart[0] != '[') return;
    const char *p = vstart + 1, *end = vstart + vlen - 1;
    while (p < end) {
        p = skip_ws(p);
        if (*p == ']') break;
        if (*p != '{') { p++; continue; }
        const char *ostart = p;
        int depth = 0;
        while (p < end) {
            if (*p == '{') depth++;
            else if (*p == '}') { depth--; if (!depth) { p++; break; } }
            p++;
        }
        cb(ostart, (size_t)(p - ostart), ctx);
        p = skip_ws(p);
        if (*p == ',') p++;
    }
}

/* Callback context for collecting source entries */
typedef struct { char lines[64][256]; int n; } ExplainLines;

static void source_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t fvlen, opvlen, ixvlen, ervlen;
    const char *f  = json_find_key(o, "field",          &fvlen);
    const char *op = json_find_key(o, "op",             &opvlen);
    const char *ix = json_find_key(o, "index",          &ixvlen);
    const char *er = json_find_key(o, "estimated_rows", &ervlen);
    if (!f) return;
    char fld[64], opc[32], idxc[32], est[32] = "?";
    json_string_into(f,  fvlen,  fld,  sizeof(fld));
    json_string_into(op, opvlen, opc,  sizeof(opc));
    json_string_into(ix, ixvlen, idxc, sizeof(idxc));
    if (er) { size_t n = ervlen<31?ervlen:31; memcpy(est,er,n); est[n]=0; }
    snprintf(el->lines[el->n], sizeof(el->lines[0]),
             "  %s %s  [%s]  ~%s rows", fld, opc, idxc, est);
    el->n++;
}

static void postfilter_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t fvlen, opvlen;
    const char *f  = json_find_key(o, "field", &fvlen);
    const char *op = json_find_key(o, "op",    &opvlen);
    if (!f) return;
    char fld[64], opc[32];
    json_string_into(f,  fvlen,  fld, sizeof(fld));
    json_string_into(op, opvlen, opc, sizeof(opc));
    snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s %s", fld, opc);
    el->n++;
}

static void hint_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t tvlen, rvlen;
    const char *t = json_find_key(o, "type",   &tvlen);
    const char *r = json_find_key(o, "reason", &rvlen);
    if (!t) return;
    char typ[64];
    json_string_into(t, tvlen, typ, sizeof(typ));
    if (r) {
        char reas[512]; json_string_into(r, rvlen, reas, sizeof(reas));
        snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s: %s", typ, reas);
    } else {
        snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s", typ);
    }
    el->n++;
}

````````

<!-- R1-BLOCK s2e23-new -->
````````c
                return 0;
        }
    }
}

/* Iterate JSON array of objects, call cb for each {…} slice. */
static void iter_array_objs(const char *vstart, size_t vlen,
                            void (*cb)(const char *o, size_t olen, void *ctx),
                            void *ctx) {
    if (vlen < 2 || vstart[0] != '[') return;
    const char *p = vstart + 1, *end = vstart + vlen - 1;
    while (p < end) {
        p = cli_json_skip_ws(p);
        if (*p == ']') break;
        if (*p != '{') { p++; continue; }
        const char *ostart = p;
        int depth = 0;
        while (p < end) {
            if (*p == '{') depth++;
            else if (*p == '}') { depth--; if (!depth) { p++; break; } }
            p++;
        }
        cb(ostart, (size_t)(p - ostart), ctx);
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
}

/* Callback context for collecting source entries */
typedef struct { char lines[64][256]; int n; } ExplainLines;

static void source_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t fvlen, opvlen, ixvlen, ervlen;
    const char *f  = cli_json_find_key(o, "field",          &fvlen);
    const char *op = cli_json_find_key(o, "op",             &opvlen);
    const char *ix = cli_json_find_key(o, "index",          &ixvlen);
    const char *er = cli_json_find_key(o, "estimated_rows", &ervlen);
    if (!f) return;
    char fld[64], opc[32], idxc[32], est[32] = "?";
    json_string_into(f,  fvlen,  fld,  sizeof(fld));
    json_string_into(op, opvlen, opc,  sizeof(opc));
    json_string_into(ix, ixvlen, idxc, sizeof(idxc));
    if (er) { size_t n = ervlen<31?ervlen:31; memcpy(est,er,n); est[n]=0; }
    snprintf(el->lines[el->n], sizeof(el->lines[0]),
             "  %s %s  [%s]  ~%s rows", fld, opc, idxc, est);
    el->n++;
}

static void postfilter_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t fvlen, opvlen;
    const char *f  = cli_json_find_key(o, "field", &fvlen);
    const char *op = cli_json_find_key(o, "op",    &opvlen);
    if (!f) return;
    char fld[64], opc[32];
    json_string_into(f,  fvlen,  fld, sizeof(fld));
    json_string_into(op, opvlen, opc, sizeof(opc));
    snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s %s", fld, opc);
    el->n++;
}

static void hint_cb(const char *o, size_t olen, void *ctx) {
    (void)olen;
    ExplainLines *el = (ExplainLines *)ctx;
    if (el->n >= 64) return;
    size_t tvlen, rvlen;
    const char *t = cli_json_find_key(o, "type",   &tvlen);
    const char *r = cli_json_find_key(o, "reason", &rvlen);
    if (!t) return;
    char typ[64];
    json_string_into(t, tvlen, typ, sizeof(typ));
    if (r) {
        char reas[512]; json_string_into(r, rvlen, reas, sizeof(reas));
        snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s: %s", typ, reas);
    } else {
        snprintf(el->lines[el->n], sizeof(el->lines[0]), "  %s", typ);
    }
    el->n++;
}

````````

<!-- R1-BLOCK s2e24-old -->
````````text
    if (_n > 0 && (size_t)_n < _room) (bo) += (size_t)_n;                  \
    else if (_n > 0) (bo) += _room > 0 ? _room - 1 : 0;                    \
} while (0)

/* Format an explain JSON response into human-readable text.
   Returns malloc'd string; caller must free. */
char *format_explain_text(const char *json) {
    char buf[16384];
    size_t bo = 0;
    size_t vlen;
    const char *v;

    v = json_find_key(json, "plan", &vlen);
    if (v) { char tmp[64]; json_string_into(v, vlen, tmp, sizeof(tmp));
        SAFE_SNPRINTF(buf, bo, "Plan:  %s\n", tmp); }

    v = json_find_key(json, "order", &vlen);
    if (v) { char tmp[64]; json_string_into(v, vlen, tmp, sizeof(tmp));
        SAFE_SNPRINTF(buf, bo, "Order: %s\n", tmp); }

    v = json_find_key(json, "total_cheap", &vlen);
    if (v)
        SAFE_SNPRINTF(buf, bo, "Cheap: %s\n",
                       (vlen==4&&memcmp(v,"true",4)==0)?"yes":"no");

    v = json_find_key(json, "table_rows", &vlen);
    if (v) {
        char tmp[32]; size_t n = vlen<31?vlen:31; memcpy(tmp,v,n); tmp[n]=0;
        SAFE_SNPRINTF(buf, bo, "Rows:  %s\n\n", tmp);
    }

    /* Source */
    SAFE_SNPRINTF(buf, bo, "Source:\n");
    v = json_find_key(json, "source", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, source_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    /* Post-filter */
    SAFE_SNPRINTF(buf, bo, "\nPost-filter:\n");
    v = json_find_key(json, "postfilter", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, postfilter_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    /* Hints */
    SAFE_SNPRINTF(buf, bo, "\nHints:\n");
    v = json_find_key(json, "hints", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, hint_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    buf[bo] = '\0';
    return strdup(buf);
}

void tui_show_text(const char *title, const char *body) {
````````

<!-- R1-BLOCK s2e24-new -->
````````c
    if (_n > 0 && (size_t)_n < _room) (bo) += (size_t)_n;                  \
    else if (_n > 0) (bo) += _room > 0 ? _room - 1 : 0;                    \
} while (0)

/* Format an explain JSON response into human-readable text.
   Returns malloc'd string; caller must free. */
char *format_explain_text(const char *json) {
    char buf[16384];
    size_t bo = 0;
    size_t vlen;
    const char *v;

    v = cli_json_find_key(json, "plan", &vlen);
    if (v) { char tmp[64]; json_string_into(v, vlen, tmp, sizeof(tmp));
        SAFE_SNPRINTF(buf, bo, "Plan:  %s\n", tmp); }

    v = cli_json_find_key(json, "order", &vlen);
    if (v) { char tmp[64]; json_string_into(v, vlen, tmp, sizeof(tmp));
        SAFE_SNPRINTF(buf, bo, "Order: %s\n", tmp); }

    v = cli_json_find_key(json, "total_cheap", &vlen);
    if (v)
        SAFE_SNPRINTF(buf, bo, "Cheap: %s\n",
                       (vlen==4&&memcmp(v,"true",4)==0)?"yes":"no");

    v = cli_json_find_key(json, "table_rows", &vlen);
    if (v) {
        char tmp[32]; size_t n = vlen<31?vlen:31; memcpy(tmp,v,n); tmp[n]=0;
        SAFE_SNPRINTF(buf, bo, "Rows:  %s\n\n", tmp);
    }

    /* Source */
    SAFE_SNPRINTF(buf, bo, "Source:\n");
    v = cli_json_find_key(json, "source", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, source_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    /* Post-filter */
    SAFE_SNPRINTF(buf, bo, "\nPost-filter:\n");
    v = cli_json_find_key(json, "postfilter", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, postfilter_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    /* Hints */
    SAFE_SNPRINTF(buf, bo, "\nHints:\n");
    v = cli_json_find_key(json, "hints", &vlen);
    if (v) {
        ExplainLines el = {0};
        iter_array_objs(v, vlen, hint_cb, &el);
        for (int i = 0; i < el.n; i++)
            SAFE_SNPRINTF(buf, bo, "%s\n", el.lines[i]);
    }

    buf[bo] = '\0';
    return strdup(buf);
}

void tui_show_text(const char *title, const char *body) {
````````

<!-- R1-BLOCK s2e25-old -->
````````text
                break;
            case 'g': case KEY_HOME: top = 0; break;
            case 'G': case KEY_END:  top = nlines - 1; break;
            case 'q': case 27: return;
        }
    }
}

void tui_show_object(const char *title, const char *json) {
    /* Pretty-print: walk top-level keys, emit as "key: value" lines. */
    char buf[16384];
    size_t bo = 0;
    const char *p = skip_ws(json);
    if (*p != '{') {
        tui_show_text(title, json);
        return;
    }
    p++;
    while (*p && bo < sizeof(buf) - 1) {
        p = skip_ws(p);
        if (*p == '}' || *p == '\0') break;
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        bo += snprintf(buf + bo, sizeof(buf) - bo, "%-14.*s : ", (int)klen, kstart);
        if (vlen > 0 && vstart[0] == '"') {
            char tmp[2048];
            json_string_into(vstart, vlen, tmp, sizeof(tmp));
            bo += snprintf(buf + bo, sizeof(buf) - bo, "%s\n", tmp);
        } else {
            size_t n = vlen < (sizeof(buf) - bo - 2) ? vlen : (sizeof(buf) - bo - 2);
            memcpy(buf + bo, vstart, n); bo += n;
            if (bo < sizeof(buf) - 1) buf[bo++] = '\n';
        }

        p = skip_ws(p);
        if (*p == ',') p++;
    }
    buf[bo] = '\0';
    tui_show_text(title, buf);
}

/* ============================================================
   Table rendering — JSON array of objects → columnar
   ============================================================ */

#define MAX_COLS 16
#define MAX_ROWS 4096
````````

<!-- R1-BLOCK s2e25-new -->
````````c
                break;
            case 'g': case KEY_HOME: top = 0; break;
            case 'G': case KEY_END:  top = nlines - 1; break;
            case 'q': case 27: return;
        }
    }
}

void tui_show_object(const char *title, const char *json) {
    /* Pretty-print: walk top-level keys, emit as "key: value" lines. */
    char buf[16384];
    size_t bo = 0;
    const char *p = cli_json_skip_ws(json);
    if (*p != '{') {
        tui_show_text(title, json);
        return;
    }
    p++;
    while (*p && bo < sizeof(buf) - 1) {
        p = cli_json_skip_ws(p);
        if (*p == '}' || *p == '\0') break;
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') break;
        p++;
        p = cli_json_skip_ws(p);
        const char *vstart = p;
        cli_json_skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        bo += snprintf(buf + bo, sizeof(buf) - bo, "%-14.*s : ", (int)klen, kstart);
        if (vlen > 0 && vstart[0] == '"') {
            char tmp[2048];
            json_string_into(vstart, vlen, tmp, sizeof(tmp));
            bo += snprintf(buf + bo, sizeof(buf) - bo, "%s\n", tmp);
        } else {
            size_t n = vlen < (sizeof(buf) - bo - 2) ? vlen : (sizeof(buf) - bo - 2);
            memcpy(buf + bo, vstart, n); bo += n;
            if (bo < sizeof(buf) - 1) buf[bo++] = '\n';
        }

        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    buf[bo] = '\0';
    tui_show_text(title, buf);
}

/* ============================================================
   Table rendering — JSON array of objects → columnar
   ============================================================ */

#define MAX_COLS 16
#define MAX_ROWS 4096
````````

<!-- R1-BLOCK s2e26-old -->
````````text
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '{') return 0;
    /* For "key" responses (find): {"key":"..","value":{...}} — flatten value
       into top-level columns. */
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    /* Walk keys */
    const char *p = elem + 1;
    const char *end = elem + len - 1;
    while (p < end) {
        p = skip_ws(p);
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (p < end && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (p < end) p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        if (klen == 5 && memcmp(kstart, "value", 5) == 0 &&
            vlen > 0 && vstart[0] == '{') {
            /* Recurse into value object as flat columns. */
            const char *vp = vstart + 1;
            const char *vend = vstart + vlen - 1;
            while (vp < vend) {
                vp = skip_ws(vp);
                if (*vp != '"') break;
                vp++;
                const char *kk = vp;
                while (vp < vend && *vp != '"') vp++;
                size_t kkl = (size_t)(vp - kk);
                if (vp < vend) vp++;
                vp = skip_ws(vp);
                if (*vp != ':') break;
                vp++;
                vp = skip_ws(vp);
                const char *vv = vp;
                skip_value(&vp);
                size_t vvl = (size_t)(vp - vv);
                int col = table_col_index(t, kk, kkl);
                if (col >= 0) {
                    char *cell;
                    if (vvl > 0 && vv[0] == '"') {
                        cell = malloc(vvl);
                        json_string_into(vv, vvl, cell, vvl);
                    } else {
                        cell = malloc(vvl + 1);
                        memcpy(cell, vv, vvl);
                        cell[vvl] = '\0';
                    }
                    t->cells[row][col] = cell;
                    int w = (int)strlen(cell);
                    if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
                }
                vp = skip_ws(vp);
                if (*vp == ',') vp++;
            }
        } else {
            int col = table_col_index(t, kstart, klen);
            if (col >= 0) {
                char *cell;
                if (vlen > 0 && vstart[0] == '"') {
                    cell = malloc(vlen);
                    json_string_into(vstart, vlen, cell, vlen);
                } else {
                    cell = malloc(vlen + 1);
                    memcpy(cell, vstart, vlen);
                    cell[vlen] = '\0';
                }
                t->cells[row][col] = cell;
                int w = (int)strlen(cell);
                if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
            }
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

static void table_free(Table *t) {
    for (int r = 0; r < t->nrows; r++) {
        for (int c = 0; c < t->ncols; c++)
            free(t->cells[r][c]);
        free(t->drilldown[r]);
    }
}
````````

<!-- R1-BLOCK s2e26-new -->
````````c
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '{') return 0;
    /* For "key" responses (find): {"key":"..","value":{...}} — flatten value
       into top-level columns. */
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    /* Walk keys */
    const char *p = elem + 1;
    const char *end = elem + len - 1;
    while (p < end) {
        p = cli_json_skip_ws(p);
        if (*p != '"') break;
        p++;
        const char *kstart = p;
        while (p < end && *p != '"') p++;
        size_t klen = (size_t)(p - kstart);
        if (p < end) p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') break;
        p++;
        p = cli_json_skip_ws(p);
        const char *vstart = p;
        cli_json_skip_value(&p);
        size_t vlen = (size_t)(p - vstart);

        if (klen == 5 && memcmp(kstart, "value", 5) == 0 &&
            vlen > 0 && vstart[0] == '{') {
            /* Recurse into value object as flat columns. */
            const char *vp = vstart + 1;
            const char *vend = vstart + vlen - 1;
            while (vp < vend) {
                vp = cli_json_skip_ws(vp);
                if (*vp != '"') break;
                vp++;
                const char *kk = vp;
                while (vp < vend && *vp != '"') vp++;
                size_t kkl = (size_t)(vp - kk);
                if (vp < vend) vp++;
                vp = cli_json_skip_ws(vp);
                if (*vp != ':') break;
                vp++;
                vp = cli_json_skip_ws(vp);
                const char *vv = vp;
                cli_json_skip_value(&vp);
                size_t vvl = (size_t)(vp - vv);
                int col = table_col_index(t, kk, kkl);
                if (col >= 0) {
                    char *cell;
                    if (vvl > 0 && vv[0] == '"') {
                        cell = malloc(vvl);
                        json_string_into(vv, vvl, cell, vvl);
                    } else {
                        cell = malloc(vvl + 1);
                        memcpy(cell, vv, vvl);
                        cell[vvl] = '\0';
                    }
                    t->cells[row][col] = cell;
                    int w = (int)strlen(cell);
                    if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
                }
                vp = cli_json_skip_ws(vp);
                if (*vp == ',') vp++;
            }
        } else {
            int col = table_col_index(t, kstart, klen);
            if (col >= 0) {
                char *cell;
                if (vlen > 0 && vstart[0] == '"') {
                    cell = malloc(vlen);
                    json_string_into(vstart, vlen, cell, vlen);
                } else {
                    cell = malloc(vlen + 1);
                    memcpy(cell, vstart, vlen);
                    cell[vlen] = '\0';
                }
                t->cells[row][col] = cell;
                int w = (int)strlen(cell);
                if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
            }
        }
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

static void table_free(Table *t) {
    for (int r = 0; r < t->nrows; r++) {
        for (int c = 0; c < t->ncols; c++)
            free(t->cells[r][c]);
        free(t->drilldown[r]);
    }
}
````````

<!-- R1-BLOCK s2e27-old -->
````````text
   column; mismatched lengths fill missing cells with empty strings. */
static int row_collect_array_cb(const char *elem, size_t len, void *ctx) {
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '[') return 0;
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    const char *p = elem + 1;
    const char *end = elem + len - 1;
    int col = 0;
    while (p < end && col < t->ncols) {
        p = skip_ws(p);
        if (*p == ']') break;
        const char *vstart = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vstart);
        char *cell;
        if (vlen > 0 && vstart[0] == '"') {
            cell = malloc(vlen);
            json_string_into(vstart, vlen, cell, vlen);
        } else {
            cell = malloc(vlen + 1);
            memcpy(cell, vstart, vlen);
            cell[vlen] = '\0';
        }
        t->cells[row][col] = cell;
        int w = (int)strlen(cell);
        if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
        col++;
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

/* Returns 1 if json is a JSON object whose first non-null value is itself a
   JSON object — the signature of a multi-get dict-of-dicts response. */
static int is_dict_of_dicts(const char *json) {
    if (!json || *json != '{') return 0;
    const char *p = skip_ws(json + 1);
    while (*p && *p != '}') {
        if (*p != '"') return 0;
        p++;
        while (*p && *p != '"') p++;
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') return 0;
        p++;
        p = skip_ws(p);
        /* skip null entries when deciding */
        if (strncmp(p, "null", 4) == 0) {
            p += 4;
            p = skip_ws(p);
            if (*p == ',') p++;
            continue;
        }
        return (*p == '{');
    }
    return 0;  /* empty or all-null — fall through to Shape 6 */
}

/* Render a dict-of-dicts into the table: top-level key becomes the first
   column ("key"), inner object fields become additional columns. */
static void render_dict_of_dicts(const char *json, Table *t) {
    /* First pass: collect column names from inner objects. */
    static const char key_col[] = "key";
    int key_idx = table_col_index(t, key_col, sizeof(key_col) - 1);
    (void)key_idx;

    const char *p = skip_ws(json + 1);
    while (*p && *p != '}' && t->nrows < MAX_ROWS) {
        p = skip_ws(p);
        if (*p != '"') break;
        p++;
        const char *ks = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - ks);
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vs = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vs);

        int row = t->nrows++;
        for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;
        t->drilldown[row] = NULL;

        /* Column 0: the lookup key. */
        int kcol = table_col_index(t, "key", 3);
        if (kcol >= 0) {
            t->cells[row][kcol] = malloc(klen + 1);
            memcpy(t->cells[row][kcol], ks, klen);
            t->cells[row][kcol][klen] = '\0';
            if ((int)klen > t->widths[kcol]) t->widths[kcol] = (klen > 32 ? 32 : (int)klen);
        }

        if (vlen == 4 && strncmp(vs, "null", 4) == 0) {
            /* Missing key — row has only the key column, others null. */
        } else if (vlen >= 2 && vs[0] == '{') {
            /* Inner object: walk its fields as columns. */
            const char *ip = vs + 1;
            const char *iend = vs + vlen - 1;
            while (ip < iend) {
                ip = skip_ws(ip);
                if (*ip != '"') break;
                ip++;
                const char *fks = ip;
                while (ip < iend && *ip != '"') ip++;
                size_t fkl = (size_t)(ip - fks);
                if (ip < iend) ip++;
                ip = skip_ws(ip);
                if (*ip != ':') break;
                ip++;
                ip = skip_ws(ip);
                const char *fvs = ip;
                skip_value(&ip);
                size_t fvl = (size_t)(ip - fvs);
                int col = table_col_index(t, fks, fkl);
                if (col >= 0) {
                    char *cell;
                    if (fvl > 0 && fvs[0] == '"') {
                        cell = malloc(fvl);
                        json_string_into(fvs, fvl, cell, fvl);
                    } else {
                        cell = malloc(fvl + 1);
                        memcpy(cell, fvs, fvl);
                        cell[fvl] = '\0';
                    }
                    t->cells[row][col] = cell;
                    int w = (int)strlen(cell);
                    if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
                }
                ip = skip_ws(ip);
                if (*ip == ',') ip++;
            }
        }

        p = skip_ws(p);
        if (*p == ',') p++;
    }
}

/* tui_show_table returns 0 on close (q/←/ESC), 1 if user pressed 'e' to
   request CSV export. Callers that pass a JSON response can re-issue the
   underlying request with format:"csv" and write the result to a file. */
int tui_show_table(const char *title, const char *json) {
    Table t;
    memset(&t, 0, sizeof(t));

    /* tui_show_table is the single render path for every data-returning
````````

<!-- R1-BLOCK s2e27-new -->
````````c
   column; mismatched lengths fill missing cells with empty strings. */
static int row_collect_array_cb(const char *elem, size_t len, void *ctx) {
    Table *t = (Table *)ctx;
    if (t->nrows >= MAX_ROWS) return 1;
    if (len < 2 || elem[0] != '[') return 0;
    int row = t->nrows++;
    for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;

    const char *p = elem + 1;
    const char *end = elem + len - 1;
    int col = 0;
    while (p < end && col < t->ncols) {
        p = cli_json_skip_ws(p);
        if (*p == ']') break;
        const char *vstart = p;
        cli_json_skip_value(&p);
        size_t vlen = (size_t)(p - vstart);
        char *cell;
        if (vlen > 0 && vstart[0] == '"') {
            cell = malloc(vlen);
            json_string_into(vstart, vlen, cell, vlen);
        } else {
            cell = malloc(vlen + 1);
            memcpy(cell, vstart, vlen);
            cell[vlen] = '\0';
        }
        t->cells[row][col] = cell;
        int w = (int)strlen(cell);
        if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
        col++;
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    return 0;
}

/* Returns 1 if json is a JSON object whose first non-null value is itself a
   JSON object — the signature of a multi-get dict-of-dicts response. */
static int is_dict_of_dicts(const char *json) {
    if (!json || *json != '{') return 0;
    const char *p = cli_json_skip_ws(json + 1);
    while (*p && *p != '}') {
        if (*p != '"') return 0;
        p++;
        while (*p && *p != '"') p++;
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') return 0;
        p++;
        p = cli_json_skip_ws(p);
        /* skip null entries when deciding */
        if (strncmp(p, "null", 4) == 0) {
            p += 4;
            p = cli_json_skip_ws(p);
            if (*p == ',') p++;
            continue;
        }
        return (*p == '{');
    }
    return 0;  /* empty or all-null — fall through to Shape 6 */
}

/* Render a dict-of-dicts into the table: top-level key becomes the first
   column ("key"), inner object fields become additional columns. */
static void render_dict_of_dicts(const char *json, Table *t) {
    /* First pass: collect column names from inner objects. */
    static const char key_col[] = "key";
    int key_idx = table_col_index(t, key_col, sizeof(key_col) - 1);
    (void)key_idx;

    const char *p = cli_json_skip_ws(json + 1);
    while (*p && *p != '}' && t->nrows < MAX_ROWS) {
        p = cli_json_skip_ws(p);
        if (*p != '"') break;
        p++;
        const char *ks = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - ks);
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') break;
        p++;
        p = cli_json_skip_ws(p);
        const char *vs = p;
        cli_json_skip_value(&p);
        size_t vlen = (size_t)(p - vs);

        int row = t->nrows++;
        for (int i = 0; i < MAX_COLS; i++) t->cells[row][i] = NULL;
        t->drilldown[row] = NULL;

        /* Column 0: the lookup key. */
        int kcol = table_col_index(t, "key", 3);
        if (kcol >= 0) {
            t->cells[row][kcol] = malloc(klen + 1);
            memcpy(t->cells[row][kcol], ks, klen);
            t->cells[row][kcol][klen] = '\0';
            if ((int)klen > t->widths[kcol]) t->widths[kcol] = (klen > 32 ? 32 : (int)klen);
        }

        if (vlen == 4 && strncmp(vs, "null", 4) == 0) {
            /* Missing key — row has only the key column, others null. */
        } else if (vlen >= 2 && vs[0] == '{') {
            /* Inner object: walk its fields as columns. */
            const char *ip = vs + 1;
            const char *iend = vs + vlen - 1;
            while (ip < iend) {
                ip = cli_json_skip_ws(ip);
                if (*ip != '"') break;
                ip++;
                const char *fks = ip;
                while (ip < iend && *ip != '"') ip++;
                size_t fkl = (size_t)(ip - fks);
                if (ip < iend) ip++;
                ip = cli_json_skip_ws(ip);
                if (*ip != ':') break;
                ip++;
                ip = cli_json_skip_ws(ip);
                const char *fvs = ip;
                cli_json_skip_value(&ip);
                size_t fvl = (size_t)(ip - fvs);
                int col = table_col_index(t, fks, fkl);
                if (col >= 0) {
                    char *cell;
                    if (fvl > 0 && fvs[0] == '"') {
                        cell = malloc(fvl);
                        json_string_into(fvs, fvl, cell, fvl);
                    } else {
                        cell = malloc(fvl + 1);
                        memcpy(cell, fvs, fvl);
                        cell[fvl] = '\0';
                    }
                    t->cells[row][col] = cell;
                    int w = (int)strlen(cell);
                    if (w > t->widths[col]) t->widths[col] = (w > 32 ? 32 : w);
                }
                ip = cli_json_skip_ws(ip);
                if (*ip == ',') ip++;
            }
        }

        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
}

/* tui_show_table returns 0 on close (q/←/ESC), 1 if user pressed 'e' to
   request CSV export. Callers that pass a JSON response can re-issue the
   underlying request with format:"csv" and write the result to a file. */
int tui_show_table(const char *title, const char *json) {
    Table t;
    memset(&t, 0, sizeof(t));

    /* tui_show_table is the single render path for every data-returning
````````

<!-- R1-BLOCK s2e28-old -->
````````text
                body[n] = '\0';
            }
            tui_alert(title, body);
            return 0;
        }
    }

    const char *arr = json;
    int rendered = 0;

    if (*json == '{') {
        size_t cols_len, rows_len, res_len;
        const char *cols_v = json_find_key(json, "columns", &cols_len);
        const char *rows_v = json_find_key(json, "rows", &rows_len);
        const char *res_v  = json_find_key(json, "results", &res_len);

        if (cols_v && rows_v) {
            /* Shape 1: explicit columns + array rows. */
            const char *cp = cols_v + 1;
            const char *cend = cols_v + cols_len - 1;
            while (cp < cend && t.ncols < MAX_COLS) {
                cp = skip_ws(cp);
                if (*cp == ']') break;
                const char *vstart = cp;
                skip_value(&cp);
                size_t vlen = (size_t)(cp - vstart);
                char tmp[64];
                if (vlen > 0 && vstart[0] == '"')
                    json_string_into(vstart, vlen, tmp, sizeof(tmp));
                else
                    snprintf(tmp, sizeof(tmp), "%.*s", (int)vlen, vstart);
                int idx = t.ncols++;
                snprintf(t.cols[idx], sizeof(t.cols[0]), "%s", tmp);
                t.widths[idx] = (int)strlen(tmp);
                cp = skip_ws(cp);
                if (*cp == ',') cp++;
            }
            json_array_iter(rows_v, rows_len, row_collect_array_cb, &t);
            rendered = 1;
        } else if (res_v) {       /* Shape 2: unwrap and re-point. */
            arr = res_v;
        } else if (rows_v) {       /* Shape 3: unwrap and re-point. */
            arr = rows_v;
        }
        if (!rendered && *json == '{' && !cols_v && !rows_v && !res_v &&
            is_dict_of_dicts(json)) {
            /* Shape 7: dict-of-dicts — multi-get result {"k1":{...},"k2":{...}}.
               Render as a table: "key" column + union of inner-object fields. */
            render_dict_of_dicts(json, &t);
            rendered = 1;
        }
        if (!rendered && *json == '{' && !cols_v && !rows_v && !res_v) {
            /* Shape 6: plain object → 2-column metric/value table. */
            snprintf(t.cols[0], sizeof(t.cols[0]), "metric");
            snprintf(t.cols[1], sizeof(t.cols[0]), "value");
            t.widths[0] = 6; t.widths[1] = 5;
            t.ncols = 2;

            const char *p = skip_ws(json) + 1;  /* past leading { */
            while (*p && t.nrows < MAX_ROWS) {
                p = skip_ws(p);
                if (*p == '}') break;
                if (*p != '"') break;
                p++;
                const char *ks = p;
                while (*p && *p != '"') p++;
                size_t klen = (size_t)(p - ks);
                if (*p == '"') p++;
                p = skip_ws(p);
                if (*p != ':') break;
                p++;
                p = skip_ws(p);
                const char *vs = p;
                skip_value(&p);
                size_t vlen = (size_t)(p - vs);

                int row = t.nrows++;
                t.cells[row][0] = malloc(klen + 1);
                memcpy(t.cells[row][0], ks, klen);
                t.cells[row][0][klen] = '\0';
                if ((int)klen > t.widths[0]) t.widths[0] = klen > 32 ? 32 : (int)klen;

                /* If the value is a nested array or object, store the raw
                   JSON in drilldown[row] so Enter/→ can recurse to view
                   it as a sub-table. The visible cell becomes a hint. */
                int is_array  = (vlen > 0 && vs[0] == '[');
````````

<!-- R1-BLOCK s2e28-new -->
````````c
                body[n] = '\0';
            }
            tui_alert(title, body);
            return 0;
        }
    }

    const char *arr = json;
    int rendered = 0;

    if (*json == '{') {
        size_t cols_len, rows_len, res_len;
        const char *cols_v = cli_json_find_key(json, "columns", &cols_len);
        const char *rows_v = cli_json_find_key(json, "rows", &rows_len);
        const char *res_v  = cli_json_find_key(json, "results", &res_len);

        if (cols_v && rows_v) {
            /* Shape 1: explicit columns + array rows. */
            const char *cp = cols_v + 1;
            const char *cend = cols_v + cols_len - 1;
            while (cp < cend && t.ncols < MAX_COLS) {
                cp = cli_json_skip_ws(cp);
                if (*cp == ']') break;
                const char *vstart = cp;
                cli_json_skip_value(&cp);
                size_t vlen = (size_t)(cp - vstart);
                char tmp[64];
                if (vlen > 0 && vstart[0] == '"')
                    json_string_into(vstart, vlen, tmp, sizeof(tmp));
                else
                    snprintf(tmp, sizeof(tmp), "%.*s", (int)vlen, vstart);
                int idx = t.ncols++;
                snprintf(t.cols[idx], sizeof(t.cols[0]), "%s", tmp);
                t.widths[idx] = (int)strlen(tmp);
                cp = cli_json_skip_ws(cp);
                if (*cp == ',') cp++;
            }
            json_array_iter(rows_v, rows_len, row_collect_array_cb, &t);
            rendered = 1;
        } else if (res_v) {       /* Shape 2: unwrap and re-point. */
            arr = res_v;
        } else if (rows_v) {       /* Shape 3: unwrap and re-point. */
            arr = rows_v;
        }
        if (!rendered && *json == '{' && !cols_v && !rows_v && !res_v &&
            is_dict_of_dicts(json)) {
            /* Shape 7: dict-of-dicts — multi-get result {"k1":{...},"k2":{...}}.
               Render as a table: "key" column + union of inner-object fields. */
            render_dict_of_dicts(json, &t);
            rendered = 1;
        }
        if (!rendered && *json == '{' && !cols_v && !rows_v && !res_v) {
            /* Shape 6: plain object → 2-column metric/value table. */
            snprintf(t.cols[0], sizeof(t.cols[0]), "metric");
            snprintf(t.cols[1], sizeof(t.cols[0]), "value");
            t.widths[0] = 6; t.widths[1] = 5;
            t.ncols = 2;

            const char *p = cli_json_skip_ws(json) + 1;  /* past leading { */
            while (*p && t.nrows < MAX_ROWS) {
                p = cli_json_skip_ws(p);
                if (*p == '}') break;
                if (*p != '"') break;
                p++;
                const char *ks = p;
                while (*p && *p != '"') p++;
                size_t klen = (size_t)(p - ks);
                if (*p == '"') p++;
                p = cli_json_skip_ws(p);
                if (*p != ':') break;
                p++;
                p = cli_json_skip_ws(p);
                const char *vs = p;
                cli_json_skip_value(&p);
                size_t vlen = (size_t)(p - vs);

                int row = t.nrows++;
                t.cells[row][0] = malloc(klen + 1);
                memcpy(t.cells[row][0], ks, klen);
                t.cells[row][0][klen] = '\0';
                if ((int)klen > t.widths[0]) t.widths[0] = klen > 32 ? 32 : (int)klen;

                /* If the value is a nested array or object, store the raw
                   JSON in drilldown[row] so Enter/→ can recurse to view
                   it as a sub-table. The visible cell becomes a hint. */
                int is_array  = (vlen > 0 && vs[0] == '[');
````````

<!-- R1-BLOCK s2e29-old -->
````````text
                } else {
                    t.cells[row][1] = malloc(vlen + 1);
                    if (vlen > 0 && vs[0] == '"') {
                        json_string_into(vs, vlen, t.cells[row][1], vlen + 1);
                    } else {
                        memcpy(t.cells[row][1], vs, vlen);
                        t.cells[row][1][vlen] = '\0';
                    }
                }
                int w = (int)strlen(t.cells[row][1]);
                if (w > t.widths[1]) t.widths[1] = (w > 64 ? 64 : w);

                p = skip_ws(p);
                if (*p == ',') p++;
            }
            rendered = 1;
        }
    }

    if (!rendered && *arr == '[') {
        /* Peek the first element to choose object-row vs string-row path. */
        const char *q = arr + 1;
        while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
        if (*q == '{') {
            /* Shape 4: array of objects — auto-detect columns from keys. */
````````

<!-- R1-BLOCK s2e29-new -->
````````c
                } else {
                    t.cells[row][1] = malloc(vlen + 1);
                    if (vlen > 0 && vs[0] == '"') {
                        json_string_into(vs, vlen, t.cells[row][1], vlen + 1);
                    } else {
                        memcpy(t.cells[row][1], vs, vlen);
                        t.cells[row][1][vlen] = '\0';
                    }
                }
                int w = (int)strlen(t.cells[row][1]);
                if (w > t.widths[1]) t.widths[1] = (w > 64 ? 64 : w);

                p = cli_json_skip_ws(p);
                if (*p == ',') p++;
            }
            rendered = 1;
        }
    }

    if (!rendered && *arr == '[') {
        /* Peek the first element to choose object-row vs string-row path. */
        const char *q = arr + 1;
        while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
        if (*q == '{') {
            /* Shape 4: array of objects — auto-detect columns from keys. */
````````

<!-- R1-BLOCK s2e30-old -->
````````text
    else if (vlen > 0 && vstart[0] == '[')
        snprintf(rows[n].value, sizeof(rows[n].value), "<array>");
    else {
        size_t L = vlen < sizeof(rows[n].value) - 1 ? vlen : sizeof(rows[n].value) - 1;
        memcpy(rows[n].value, vstart, L);
        rows[n].value[L] = '\0';
    }
    return n + 1;
}

static int parse_stats_into(const char *json, StatRow *rows, int max_rows) {
    int n = 0;
    const char *p = skip_ws(json);
    if (*p != '{') return 0;
    p++;
    while (*p && n < max_rows) {
        p = skip_ws(p);
        if (*p == '}') break;
        if (*p != '"') break;
        p++;
        const char *ks = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - ks);
        if (*p == '"') p++;
        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);
        const char *vs = p;
        skip_value(&p);
        size_t vlen = (size_t)(p - vs);

        /* Flatten one level of nested object: bt_cache → bt_cache.used,
           bt_cache.total, … (much more readable than dumping the JSON). */
        if (vlen > 0 && vs[0] == '{') {
            char prefix[64];
            snprintf(prefix, sizeof(prefix), "%.*s", (int)klen, ks);
            const char *np = vs + 1;
            const char *nend = vs + vlen - 1;
            while (np < nend && n < max_rows) {
                np = skip_ws(np);
                if (*np == '}' || np >= nend) break;
                if (*np != '"') break;
                np++;
                const char *nks = np;
                while (np < nend && *np != '"') np++;
                size_t nklen = (size_t)(np - nks);
                if (np < nend) np++;
                np = skip_ws(np);
                if (*np != ':') break;
                np++;
                np = skip_ws(np);
                const char *nvs = np;
                skip_value(&np);
                size_t nvlen = (size_t)(np - nvs);
                n = emit_row(rows, n, max_rows, prefix, nks, nklen, nvs, nvlen);
                np = skip_ws(np);
                if (*np == ',') np++;
            }
        } else {
            n = emit_row(rows, n, max_rows, NULL, ks, klen, vs, vlen);
        }
        p = skip_ws(p);
        if (*p == ',') p++;
    }
    return n;
}

void tui_stats_live(CliConn *c, int interval_sec) {
    int rows, cols;
    halfdelay(interval_sec * 10);  /* deciseconds */
    int tick = 0;
    StatRow stat_rows[64];
    int top = 0;

````````

<!-- R1-BLOCK s2e30-new -->
````````c
    else if (vlen > 0 && vstart[0] == '[')
        snprintf(rows[n].value, sizeof(rows[n].value), "<array>");
    else {
        size_t L = vlen < sizeof(rows[n].value) - 1 ? vlen : sizeof(rows[n].value) - 1;
        memcpy(rows[n].value, vstart, L);
        rows[n].value[L] = '\0';
    }
    return n + 1;
}

static int parse_stats_into(const char *json, StatRow *rows, int max_rows) {
    int n = 0;
    const char *p = cli_json_skip_ws(json);
    if (*p != '{') return 0;
    p++;
    while (*p && n < max_rows) {
        p = cli_json_skip_ws(p);
        if (*p == '}') break;
        if (*p != '"') break;
        p++;
        const char *ks = p;
        while (*p && *p != '"') p++;
        size_t klen = (size_t)(p - ks);
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') break;
        p++;
        p = cli_json_skip_ws(p);
        const char *vs = p;
        cli_json_skip_value(&p);
        size_t vlen = (size_t)(p - vs);

        /* Flatten one level of nested object: bt_cache → bt_cache.used,
           bt_cache.total, … (much more readable than dumping the JSON). */
        if (vlen > 0 && vs[0] == '{') {
            char prefix[64];
            snprintf(prefix, sizeof(prefix), "%.*s", (int)klen, ks);
            const char *np = vs + 1;
            const char *nend = vs + vlen - 1;
            while (np < nend && n < max_rows) {
                np = cli_json_skip_ws(np);
                if (*np == '}' || np >= nend) break;
                if (*np != '"') break;
                np++;
                const char *nks = np;
                while (np < nend && *np != '"') np++;
                size_t nklen = (size_t)(np - nks);
                if (np < nend) np++;
                np = cli_json_skip_ws(np);
                if (*np != ':') break;
                np++;
                np = cli_json_skip_ws(np);
                const char *nvs = np;
                cli_json_skip_value(&np);
                size_t nvlen = (size_t)(np - nvs);
                n = emit_row(rows, n, max_rows, prefix, nks, nklen, nvs, nvlen);
                np = cli_json_skip_ws(np);
                if (*np == ',') np++;
            }
        } else {
            n = emit_row(rows, n, max_rows, NULL, ks, klen, vs, vlen);
        }
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    return n;
}

void tui_stats_live(CliConn *c, int interval_sec) {
    int rows, cols;
    halfdelay(interval_sec * 10);  /* deciseconds */
    int tick = 0;
    StatRow stat_rows[64];
    int top = 0;

````````

<!-- R1-BLOCK s2e31-old -->
````````text
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
             dir, object);
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0 || !resp) {
        if (resp) free(resp);
        return -1;
    }
    /* Reject error responses early. */
    size_t vlen;
    if (json_find_key(resp, "error", &vlen)) { free(resp); return -1; }

    memset(oi, 0, sizeof(*oi));
    snprintf(oi->dir,    sizeof(oi->dir),    "%s", dir);
    snprintf(oi->object, sizeof(oi->object), "%s", object);

    const char *v;
    if ((v = json_find_key(resp, "splits", &vlen)))    oi->splits = atoi(v);
    if ((v = json_find_key(resp, "max_key", &vlen)))   oi->max_key = atoi(v);
    if ((v = json_find_key(resp, "max_value", &vlen))) oi->max_value = atoi(v);
    if ((v = json_find_key(resp, "record_count", &vlen))) oi->record_count = atoi(v);

    /* fields[] */
    if ((v = json_find_key(resp, "fields", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nfields < MAX_FIELDS_CACHED) {
            p = skip_ws(p);
            if (*p == ']') break;
            if (*p != '{') break;
            const char *fstart = p;
            skip_value(&p);
            size_t flen = (size_t)(p - fstart);
            CachedField *fld = &oi->fields[oi->nfields];
            const char *fv;
            size_t fvlen;
            char tmp[256];
            if ((fv = json_find_key(fstart, "name", &fvlen))) {
                /* json_find_key here works because skip_value left fstart..p as
                   a complete object; passing fstart with NUL after isn't safe,
                   but json_find_key only looks for keys, stops at }. Need a
                   bounded variant — write our own walk below. */
                (void)fvlen;
            }
            /* Walk fstart..fstart+flen as an object directly. */
            const char *fp = fstart + 1;
            const char *fend = fstart + flen - 1;
            while (fp < fend) {
                fp = skip_ws(fp);
                if (*fp != '"') break;
                fp++;
                const char *kk = fp;
                while (fp < fend && *fp != '"') fp++;
                size_t kkl = (size_t)(fp - kk);
                if (fp < fend) fp++;
                fp = skip_ws(fp);
                if (*fp != ':') break;
                fp++;
                fp = skip_ws(fp);
                const char *vv = fp;
                skip_value(&fp);
                size_t vvl = (size_t)(fp - vv);
                if (kkl == 4 && memcmp(kk, "name", 4) == 0)
                    json_string_into(vv, vvl, fld->name, sizeof(fld->name));
                else if (kkl == 4 && memcmp(kk, "type", 4) == 0)
                    json_string_into(vv, vvl, fld->type, sizeof(fld->type));
                else if (kkl == 4 && memcmp(kk, "size", 4) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->size = atoi(tmp);
                }
                else if (kkl == 5 && memcmp(kk, "scale", 5) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->scale = atoi(tmp);
                }
                fp = skip_ws(fp);
                if (*fp == ',') fp++;
            }
            oi->nfields++;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
    }

    /* indexes[] (array of strings) */
    if ((v = json_find_key(resp, "indexes", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nindexes < MAX_FIELDS_CACHED) {
            p = skip_ws(p);
            if (*p == ']') break;
            const char *estart = p;
            skip_value(&p);
            size_t elen = (size_t)(p - estart);
            json_string_into(estart, elen,
                             oi->indexes[oi->nindexes],
                             sizeof(oi->indexes[0]));
            oi->nindexes++;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
    }

    free(resp);
    return 0;
}

/* ============================================================
   Criteria builder — list-with-actions UX
   ============================================================

````````

<!-- R1-BLOCK s2e31-new -->
````````c
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
             dir, object);
    char *resp = NULL;
    size_t rlen = 0;
    if (cli_query(c, req, &resp, &rlen) != 0 || !resp) {
        if (resp) free(resp);
        return -1;
    }
    /* Reject error responses early. */
    size_t vlen;
    if (cli_json_find_key(resp, "error", &vlen)) { free(resp); return -1; }

    memset(oi, 0, sizeof(*oi));
    snprintf(oi->dir,    sizeof(oi->dir),    "%s", dir);
    snprintf(oi->object, sizeof(oi->object), "%s", object);

    const char *v;
    if ((v = cli_json_find_key(resp, "splits", &vlen)))    oi->splits = atoi(v);
    if ((v = cli_json_find_key(resp, "max_key", &vlen)))   oi->max_key = atoi(v);
    if ((v = cli_json_find_key(resp, "max_value", &vlen))) oi->max_value = atoi(v);
    if ((v = cli_json_find_key(resp, "record_count", &vlen))) oi->record_count = atoi(v);

    /* fields[] */
    if ((v = cli_json_find_key(resp, "fields", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nfields < MAX_FIELDS_CACHED) {
            p = cli_json_skip_ws(p);
            if (*p == ']') break;
            if (*p != '{') break;
            const char *fstart = p;
            cli_json_skip_value(&p);
            size_t flen = (size_t)(p - fstart);
            CachedField *fld = &oi->fields[oi->nfields];
            const char *fv;
            size_t fvlen;
            char tmp[256];
            if ((fv = cli_json_find_key(fstart, "name", &fvlen))) {
                /* cli_json_find_key here works because cli_json_skip_value left fstart..p as
                   a complete object; passing fstart with NUL after isn't safe,
                   but cli_json_find_key only looks for keys, stops at }. Need a
                   bounded variant — write our own walk below. */
                (void)fvlen;
            }
            /* Walk fstart..fstart+flen as an object directly. */
            const char *fp = fstart + 1;
            const char *fend = fstart + flen - 1;
            while (fp < fend) {
                fp = cli_json_skip_ws(fp);
                if (*fp != '"') break;
                fp++;
                const char *kk = fp;
                while (fp < fend && *fp != '"') fp++;
                size_t kkl = (size_t)(fp - kk);
                if (fp < fend) fp++;
                fp = cli_json_skip_ws(fp);
                if (*fp != ':') break;
                fp++;
                fp = cli_json_skip_ws(fp);
                const char *vv = fp;
                cli_json_skip_value(&fp);
                size_t vvl = (size_t)(fp - vv);
                if (kkl == 4 && memcmp(kk, "name", 4) == 0)
                    json_string_into(vv, vvl, fld->name, sizeof(fld->name));
                else if (kkl == 4 && memcmp(kk, "type", 4) == 0)
                    json_string_into(vv, vvl, fld->type, sizeof(fld->type));
                else if (kkl == 4 && memcmp(kk, "size", 4) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->size = atoi(tmp);
                }
                else if (kkl == 5 && memcmp(kk, "scale", 5) == 0) {
                    json_string_into(vv, vvl, tmp, sizeof(tmp));
                    fld->scale = atoi(tmp);
                }
                fp = cli_json_skip_ws(fp);
                if (*fp == ',') fp++;
            }
            oi->nfields++;
            p = cli_json_skip_ws(p);
            if (*p == ',') p++;
        }
    }

    /* indexes[] (array of strings) */
    if ((v = cli_json_find_key(resp, "indexes", &vlen)) && vlen > 0 && v[0] == '[') {
        const char *p = v + 1;
        const char *end = v + vlen - 1;
        while (p < end && oi->nindexes < MAX_FIELDS_CACHED) {
            p = cli_json_skip_ws(p);
            if (*p == ']') break;
            const char *estart = p;
            cli_json_skip_value(&p);
            size_t elen = (size_t)(p - estart);
            json_string_into(estart, elen,
                             oi->indexes[oi->nindexes],
                             sizeof(oi->indexes[0]));
            oi->nindexes++;
            p = cli_json_skip_ws(p);
            if (*p == ',') p++;
        }
    }

    free(resp);
    return 0;
}

/* ============================================================
   Criteria builder — list-with-actions UX
   ============================================================

````````

<!-- R1-BLOCK s2e32-old -->
````````text
#include "types.h"
#include "typed_row.h"
#include "slotcask.h"
#include "bitmap.h"
#include <math.h>
#include <stdarg.h>
#include <assert.h>

/* ========== Config ========== */

_Thread_local uint32_t g_request_timeout_ms = 0;  /* per-request override; 0 = use g_timeout */

````````

<!-- R1-BLOCK s2e32-new -->
````````c
#include "schema_wire.h"
#include "types.h"
#include "typed_row.h"
#include "slotcask.h"
#include "bitmap.h"
#include <math.h>
#include <stdarg.h>
#include <assert.h>

/* ========== Config ========== */

_Thread_local uint32_t g_request_timeout_ms = 0;  /* per-request override; 0 = use g_timeout */

````````

<!-- R1-BLOCK s2e33-old -->
````````text
   0 otherwise. Safe to call on a spec with no modifier — leaves the spec
   untouched and tf->default_kind unchanged.

   Centralises the parse rules that used to live inline inside
   load_typed_schema (config.c) — and were therefore skipped by
   cmd_add_fields / cmd_edit_fields, which called parse_field_type
   directly and rejected every :default=... spec.

   IMPORTANT: this function does NOT initialise tf->default_kind or
   tf->default_val on entry; on a no-modifier call it leaves them
   alone. Callers that want a guaranteed clean slate should zero those
   fields before invoking. */
int field_default_too_long(const char *field_spec) {
    const char *d = strstr(field_spec, ":default=");
    if (!d) return 0;
    return strlen(d + 9) >= sizeof(((TypedField *)0)->default_val);
}

/* Strip default/nullability modifiers in place. Returns 1 when a default
   modifier was stripped, 0 when none was present, -1 when the modifier
   STRUCTURE is invalid: a :not_null / :nullable token appears anywhere
   except as the single trailing token (duplicates included). Callers
   must treat -1 as an unparsable spec — reject at the wire, refuse the whole schema at load. */
int parse_default_modifier(char *type_spec_inout, TypedField *tf) {
    if (!type_spec_inout || !tf) return 0;
    /* Trailing nullability tokens (:not_null / :nullable) strip FIRST —
       the :default= scan below consumes to end-of-string, so a token
       after it would be swallowed into the default literal. Grammar:
````````

<!-- R1-BLOCK s2e33-new -->
````````c
   0 otherwise. Safe to call on a spec with no modifier — leaves the spec
   untouched and tf->default_kind unchanged.

   Centralises the parse rules that used to live inline inside
   load_typed_schema (config.c) — and were therefore skipped by
   cmd_add_fields / cmd_edit_fields, which called parse_field_type
   directly and rejected every :default=... spec.

   IMPORTANT: this function does NOT initialise tf->default_kind or
   tf->default_val on entry; on a no-modifier call it leaves them
   alone. Callers that want a guaranteed clean slate should zero those
   fields before invoking. */
int field_default_too_long(const char *spec){
    if(!spec)return 0;
    const char *value=strstr(spec,":default=");if(!value)return 0;value+=9;
    size_t n=strlen(value);
    if(n>=8&&!strcmp(value+n-8,":removed"))n-=8;
    if(n>=9&&(!memcmp(value+n-9,":not_null",9)||!memcmp(value+n-9,":nullable",9)))n-=9;
    return n>=sizeof(((TypedField*)0)->default_val);
}

/* Strip default/nullability modifiers in place. Returns 1 when a default
   modifier was stripped, 0 when none was present, -1 when the modifier
   STRUCTURE is invalid: a :not_null / :nullable token appears anywhere
   except as the single trailing token (duplicates included). Callers
   must treat -1 as an unparsable spec — reject at the wire, refuse the whole schema at load. */
int parse_default_modifier(char *type_spec_inout, TypedField *tf) {
    if (!type_spec_inout || !tf) return 0;
    /* Trailing nullability tokens (:not_null / :nullable) strip FIRST —
       the :default= scan below consumes to end-of-string, so a token
       after it would be swallowed into the default literal. Grammar:
````````

<!-- R1-BLOCK s2e34-old -->
````````text
                 * binaries or hand edits — the wire gates reject these. */
                LOG_WARN(LOG_SUB_CONFIG,
                         "fields.conf: :default= literal exceeds %d bytes "
                         "and will be truncated",
                         (int)sizeof(tf->default_val) - 1);
            }
            strncpy(tf->default_val, dval, sizeof(tf->default_val) - 1);
            tf->default_val[sizeof(tf->default_val) - 1] = '\0';
        }
        *dflt = '\0';
        return 1;
    }
    return 0;
}

void parse_field_type(const char *spec, TypedField *f) {
    /* Defensive cleanup — if the same TypedField is re-parsed in place,
       drop the old enum value list before overwriting. */
    free_enum_values(f);

    f->type = FT_NONE;
    f->size = 0;
    f->numeric_scale = 0;

````````

<!-- R1-BLOCK s2e34-new -->
````````c
                 * binaries or hand edits — the wire gates reject these. */
                LOG_WARN(LOG_SUB_CONFIG,
                         "fields.conf: :default= literal exceeds %d bytes "
                         "and will be truncated",
                         (int)sizeof(tf->default_val) - 1);
            }
            strncpy(tf->default_val, dval, sizeof(tf->default_val) - 1);
            tf->default_val[sizeof(tf->default_val) - 1] = '\0';
        }
        *dflt = '\0';
        return 1;
    }
    return 0;
}

int parse_default_modifier_checked(char *spec,TypedField *f){
    if(!spec||!f)return 0;
    if(strchr(spec,'\n')||strchr(spec,'\r'))return -1;
    f->default_invalid=0;
    f->not_null=0;f->not_null_set=0;f->default_kind=DK_NONE;f->default_val[0]=0;
    size_t n=strlen(spec);
    if(n>=9&&!strcmp(spec+n-9,":not_null")){spec[n-9]=0;f->not_null=1;f->not_null_set=1;}
    else if(n>=9&&!strcmp(spec+n-9,":nullable")){spec[n-9]=0;f->not_null_set=1;}
    if(strstr(spec,":not_null")||strstr(spec,":nullable"))return -1;
    char *d=strstr(spec,":default=");
    if(d){
        const char *value=d+9;size_t length=strlen(value);
        if(strstr(value,":auto_create")||strstr(value,":auto_update")||strstr(value,":default="))return -1;
        if(length>=sizeof(f->default_val))return -1;
        if(!strncmp(value,"seq(",4)||!strncmp(value,"random(",7)){
            int random=!strncmp(value,"random(",7);size_t prefix=random?7:4;
            if(length<=prefix+1||value[length-1]!=')')return -1;
            size_t arglen=length-prefix-1;
            if(!arglen||arglen>=sizeof(f->default_val))return -1;
            const char *arg=value+prefix;
            if(random){
                unsigned long count=0;
                for(size_t i=0;i<arglen;i++){
                    if(arg[i]<'0'||arg[i]>'9'||count>(2097152u-(unsigned)(arg[i]-'0'))/10)return -1;
                    count=count*10+(unsigned)(arg[i]-'0');
                }
                if(!count||count>2097152u)return -1;
                f->default_kind=DK_RANDOM;
            }else{
                if(arglen>=128)return -1;
                for(size_t i=0;i<arglen;i++)if(arg[i]==':'||arg[i]=='+'||arg[i]=='/'||
                    arg[i]=='('||arg[i]==')'||(unsigned char)arg[i]<=32)return -1;
                f->default_kind=DK_SEQ;
            }
            memcpy(f->default_val,arg,arglen);f->default_val[arglen]=0;
        }else if(!strcmp(value,"uuid()"))f->default_kind=DK_UUID;
        else{f->default_kind=DK_LITERAL;memcpy(f->default_val,value,length+1);}
        *d=0;return 1;
    }
    char *a=strstr(spec,":auto_create"),*u=strstr(spec,":auto_update");
    if(a&&u)return -1;
    d=a?a:u;
    if(d){if(d[12])return -1;*d=0;f->default_kind=a?DK_AUTO_CREATE:DK_AUTO_UPDATE;return 1;}
    return 0;
}

void parse_field_type(const char *spec, TypedField *f) {
    /* Defensive cleanup — if the same TypedField is re-parsed in place,
       drop the old enum value list before overwriting. */
    free_enum_values(f);

    f->type = FT_NONE;
    f->size = 0;
    f->numeric_scale = 0;

````````

<!-- R1-BLOCK s2e35-old -->
````````text
           of a spec can be up to MAX_FIELD_DEF bytes (enum value lists)
           — it must never be copied into a smaller buffer. */
        char *type_spec = colon + 1;
        size_t ts_len = strlen(type_spec);
        if (ts_len >= 8 && strcmp(type_spec + ts_len - 8, ":removed") == 0) {
            type_spec[ts_len - 8] = '\0';
            tf->removed = 1;
        }

        /* Default modifiers — strip from type_spec before parse_field_type.
           Format: :auto_create, :auto_update, :default=<val>
           These are always the last colon-segment (before :removed). */
        if (parse_default_modifier(type_spec, tf) < 0) {
            /* Never drop a field: later offsets/bitmap positions would
               shift. Refuse the entire schema, without caching it. */
            LOG_ERROR(LOG_SUB_CONFIG,
                      "invalid nullability structure in %s — schema refused",
                      path);
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            free_enum_values(tf);
            free(line);
            fclose(f);
            return NULL;
````````

<!-- R1-BLOCK s2e35-new -->
````````c
           of a spec can be up to MAX_FIELD_DEF bytes (enum value lists)
           — it must never be copied into a smaller buffer. */
        char *type_spec = colon + 1;
        size_t ts_len = strlen(type_spec);
        if (ts_len >= 8 && strcmp(type_spec + ts_len - 8, ":removed") == 0) {
            type_spec[ts_len - 8] = '\0';
            tf->removed = 1;
        }

        /* Default modifiers — strip from type_spec before parse_field_type.
           Format: :auto_create, :auto_update, :default=<val>
           These are always the last colon-segment (before :removed). */
        char *default_check=strdup(type_spec);
        if(!default_check){free(line);fclose(f);for(int i=0;i<ts.nfields;i++)free_enum_values(&ts.fields[i]);return NULL;}
        TypedField checked_default={0};
        tf->default_invalid=parse_default_modifier_checked(default_check,&checked_default)<0;
        free(default_check);
        if (parse_default_modifier(type_spec, tf) < 0) {
            /* Never drop a field: later offsets/bitmap positions would
               shift. Refuse the entire schema, without caching it. */
            LOG_ERROR(LOG_SUB_CONFIG,
                      "invalid nullability structure in %s — schema refused",
                      path);
            for (int i = 0; i < ts.nfields; i++)
                free_enum_values(&ts.fields[i]);
            free_enum_values(tf);
            free(line);
            fclose(f);
            return NULL;
````````

<!-- R1-BLOCK s2e36-old -->
````````text
    }
    /* AK_NONE: verbatim copy (key is a regular string). */
    if (klen + 1 > outcap) {
        LOG_WARN(LOG_SUB_CONFIG, "format_wire_key: output buffer too small for key (klen=%zu, outcap=%zu)", klen, outcap);
        return -1;
    }
    memcpy(out, key, klen);
    out[klen] = '\0';
    return (int)klen;
}

/* Random N bytes, hex-encoded (2*N chars + NUL) */
static void gen_random_hex(int nbytes, char *buf, size_t bufsz) {
    if (nbytes <= 0 || (size_t)(nbytes * 2 + 1) > bufsz) { buf[0] = '\0'; return; }
    uint8_t raw[256];
    if (nbytes > (int)sizeof(raw)) nbytes = (int)sizeof(raw);
    if (fill_random(raw, (size_t)nbytes) != 0) { buf[0] = '\0'; return; }
    for (int i = 0; i < nbytes; i++)
        snprintf(buf + i * 2, 3, "%02x", raw[i]);
}

/* Durable sequence-state write: temp file in the same directory, full
   write, fdatasync, rename, parent-dir fsync. The flock caller holds the
   per-sequence lock across read-modify-write, so rename's replace
   semantics are safe. Returns 0, or -1 with the state file unchanged. */
static int seq_state_write(const char *seq_dir, const char *seq_path,
                           long long val) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", seq_path, (int)getpid());
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
````````

<!-- R1-BLOCK s2e36-new -->
````````c
    }
    /* AK_NONE: verbatim copy (key is a regular string). */
    if (klen + 1 > outcap) {
        LOG_WARN(LOG_SUB_CONFIG, "format_wire_key: output buffer too small for key (klen=%zu, outcap=%zu)", klen, outcap);
        return -1;
    }
    memcpy(out, key, klen);
    out[klen] = '\0';
    return (int)klen;
}

/* Random N bytes, hex-encoded (2*N chars + NUL) */
static void gen_random_hex(int nbytes,char *buf,size_t bufsz){
    if(!buf||!bufsz)return;
    buf[0]=0;
    if(nbytes<=0||(size_t)nbytes>(bufsz-1)/2)return;
    static const char hex[]="0123456789abcdef";uint8_t raw[256];size_t done=0;
    while(done<(size_t)nbytes){
        size_t take=(size_t)nbytes-done;if(take>sizeof(raw))take=sizeof(raw);
        if(fill_random(raw,take)){buf[0]=0;return;}
        for(size_t i=0;i<take;i++){buf[2*(done+i)]=hex[raw[i]>>4];buf[2*(done+i)+1]=hex[raw[i]&15];}
        done+=take;
    }
    buf[2*(size_t)nbytes]=0;
}

/* Durable sequence-state write: temp file in the same directory, full
   write, fdatasync, rename, parent-dir fsync. The flock caller holds the
   per-sequence lock across read-modify-write, so rename's replace
   semantics are safe. Returns 0, or -1 with the state file unchanged. */
static int seq_state_write(const char *seq_dir, const char *seq_path,
                           long long val) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", seq_path, (int)getpid());
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
````````

<!-- R1-BLOCK s2e37-old -->
````````text
        return -1;
    }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
}

/* Generate a default value string for a field. Returns static/malloc'd string
   in gen_buf (caller-provided, >= 256 bytes). Returns NULL if no default. */
const char *generate_default(const TypedField *tf, char *gen_buf, size_t bufsz,
                                    const char *db_root, const char *object) {
    switch (tf->default_kind) {
    case DK_NONE:
        return NULL;
    case DK_LITERAL:
        return tf->default_val;
    case DK_AUTO_CREATE:
    case DK_AUTO_UPDATE:
        if (tf->type == FT_TIMESTAMP)
            gen_timestamp_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIMEMS)
            gen_datetimems_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIME)
````````

<!-- R1-BLOCK s2e37-new -->
````````c
        return -1;
    }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
}

/* Generate a default value string for a field. Returns static/malloc'd string
   in gen_buf (caller-provided, >= 256 bytes). Returns NULL if no default. */
const char *generate_default(const TypedField *tf, char *gen_buf, size_t bufsz,
                                    const char *db_root, const char *object) {
    if(!tf||tf->default_invalid){errno=EINVAL;return NULL;}
    switch (tf->default_kind) {
    case DK_NONE:
        return NULL;
    case DK_LITERAL:
        return tf->default_val;
    case DK_AUTO_CREATE:
    case DK_AUTO_UPDATE:
        if (tf->type == FT_TIMESTAMP)
            gen_timestamp_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIMEMS)
            gen_datetimems_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIME)
````````

<!-- R1-BLOCK s2e38-old -->
````````text
          if (v < 0) return NULL;
          snprintf(gen_buf, bufsz, "%lld", v);
          return gen_buf; }
    case DK_UUID:
        gen_uuid4(gen_buf, bufsz);
        return gen_buf[0] ? gen_buf : NULL;
    case DK_RANDOM:
        { int n = atoi(tf->default_val);
          gen_random_hex(n, gen_buf, bufsz);
          return gen_buf[0] ? gen_buf : NULL; }
    }
    return NULL;
}

/* Helper: render an actionable "unknown enum value" error message into
   err_buf. Truncates the legal-values list cleanly when there's no room
   left, keeping the message readable for large enums.

   CodeQL #91 ("potentially overflowing call to snprintf"): the previous
   implementation accumulated snprintf's *would-have-written* return
   value into `pos`, then passed `err_buf_size - pos` as the next call's
   remaining-size. If snprintf returned a value larger than the actual
   space, `pos` walked past `err_buf_size` and the next subtraction
   underflowed (size_t), letting subsequent snprintf calls write
````````

<!-- R1-BLOCK s2e38-new -->
````````c
          if (v < 0) return NULL;
          snprintf(gen_buf, bufsz, "%lld", v);
          return gen_buf; }
    case DK_UUID:
        gen_uuid4(gen_buf, bufsz);
        return gen_buf[0] ? gen_buf : NULL;
    case DK_RANDOM:
        { int n = atoi(tf->default_val);
          gen_random_hex(n, gen_buf, bufsz);
          return gen_buf[0] ? gen_buf : NULL; }
    }
    return NULL;
}

char *generate_default_owned(const TypedField *f,const char *root,const char *object,size_t budget){
    if(!f||!budget||f->default_invalid){errno=EINVAL;return NULL;}
    size_t need=256;
    if(f->default_kind==DK_LITERAL)need=strlen(f->default_val)+1;
    else if(f->default_kind==DK_RANDOM){
        const char *p=f->default_val;size_t count=0;
        if(!*p){errno=EINVAL;return NULL;}
        for(;*p;p++){
            if(*p<'0'||*p>'9'||count>(2097152u-(unsigned)(*p-'0'))/10){errno=EINVAL;return NULL;}
            count=count*10+(unsigned)(*p-'0');
        }
        if(!count||count>2097152u){errno=EINVAL;return NULL;}need=2*count+1;
    }
    if(need>budget){errno=ENOBUFS;return NULL;}
    char *out=malloc(need);if(!out){errno=ENOMEM;return NULL;}
    const char *value=generate_default(f,out,need,root,object);
    if(!value){free(out);errno=EIO;return NULL;}
    if(value!=out)memcpy(out,value,strlen(value)+1);
    return out;
}

/* Helper: render an actionable "unknown enum value" error message into
   err_buf. Truncates the legal-values list cleanly when there's no room
   left, keeping the message readable for large enums.

   CodeQL #91 ("potentially overflowing call to snprintf"): the previous
   implementation accumulated snprintf's *would-have-written* return
   value into `pos`, then passed `err_buf_size - pos` as the next call's
   remaining-size. If snprintf returned a value larger than the actual
   space, `pos` walked past `err_buf_size` and the next subtraction
   underflowed (size_t), letting subsequent snprintf calls write
````````

<!-- R1-BLOCK s2e39-old -->
````````text
            if (ts->fields[i].not_null) {
                char message[320];
                fmt_not_null_err(&ts->fields[i], message, sizeof(message));
                LOG_WARN(LOG_SUB_CONFIG, "%s/%s: %s (omitted, no default)",
                         db_root ? db_root : "?", object ? object : "?", message);
                fmt_not_null_err(&ts->fields[i], err_buf, err_buf_size);
                return -2;
            }
            /* Omitted, no default → NULL for nullmap records. */
            typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
            continue;
        }
        char gen_buf[256];
        const char *dv = generate_default(&ts->fields[i], gen_buf, sizeof(gen_buf),
                                          db_root, object);
        if (!dv) {
            /* The loop already skipped DK_NONE, so NULL here always means the
               generator failed (seq lock/filesystem error, random source
               unavailable). Silently storing zeros was the old behavior —
               refuse the write instead, using the same -2 + err_buf contract
               as enum validation. */
            if (err_buf && err_buf_size > 0)
                snprintf(err_buf, err_buf_size,
                         "default generator failed for field [%s]",
                         ts->fields[i].name);
            return -2;
        }
        if (encode_field_checked(&ts->fields[i], dv, strlen(dv),
                                  out + ts->nullmap_prefix + ts->fields[i].offset,
                                  err_buf, err_buf_size) != 0)
            return -2;
    }
    return ts->total_size;
}

/* ---- Decode: binary → JSON ---- */

/* render_zeros: set for nullmap records (the bitmap is authoritative —
   a bit-clear zero is a real value and must render). Legacy records pass
   0 and keep the skip-on-zero absence heuristics. */
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                               char *buf, int buflen, int render_zeros) {
    switch (f->type) {
````````

<!-- R1-BLOCK s2e39-new -->
````````c
            if (ts->fields[i].not_null) {
                char message[320];
                fmt_not_null_err(&ts->fields[i], message, sizeof(message));
                LOG_WARN(LOG_SUB_CONFIG, "%s/%s: %s (omitted, no default)",
                         db_root ? db_root : "?", object ? object : "?", message);
                fmt_not_null_err(&ts->fields[i], err_buf, err_buf_size);
                return -2;
            }
            /* Omitted, no default → NULL for nullmap records. */
            typed_set_null(out + 1, ts->nullmap_prefix - 1, i);
            continue;
        }
        char *dv=generate_default_owned(&ts->fields[i],db_root,object,g_query_buffer_max_bytes);
        if (!dv) {
            /* The loop already skipped DK_NONE, so NULL here always means the
               generator failed (seq lock/filesystem error, random source
               unavailable). Silently storing zeros was the old behavior —
               refuse the write instead, using the same -2 + err_buf contract
               as enum validation. */
            if (err_buf && err_buf_size > 0)
                snprintf(err_buf, err_buf_size,
                         "default generator failed for field [%s]",
                         ts->fields[i].name);
            return -2;
        }
        int rc=encode_field_checked(&ts->fields[i],dv,strlen(dv),
            out+ts->nullmap_prefix+ts->fields[i].offset,err_buf,err_buf_size);
        free(dv);if(rc)return -2;
    }
    return ts->total_size;
}

/* ---- Decode: binary → JSON ---- */

/* render_zeros: set for nullmap records (the bitmap is authoritative —
   a bit-clear zero is a real value and must render). Legacy records pass
   0 and keep the skip-on-zero absence heuristics. */
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                               char *buf, int buflen, int render_zeros) {
    switch (f->type) {
````````

<!-- R1-BLOCK s2e40-old -->
````````text
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for enum field [%s] value [%s]", f->name, s);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}

char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len,
                   uint8_t hdr2) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS) {
        errno = EINVAL;
        return NULL;
    }
    if (data_len < 0) { errno = EILSEQ; return NULL; }
    if (ts->packed.nfields)
        return typed_packed_decode(ts, data, (size_t)data_len, hdr2, SIZE_MAX);
    const uint8_t *fbase = NULL, *bm = NULL;
    int bm_bytes = 0;
    size_t region = 0;
    if (data_len < 0 || typed_record_view(data, (size_t)data_len, hdr2,
                                         &fbase, &bm, &bm_bytes, &region)) {
        errno = EILSEQ;
        return NULL;
    }
    static const uint8_t zero_field[65537];
    int capacities[MAX_FIELDS] = {0};
    size_t est = 4;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        if (f->offset < 0 || f->size < 1 || f->size > (int)sizeof(zero_field)) {
            errno = EILSEQ;
            return NULL;
        }
        const uint8_t *src = (size_t)f->offset + (size_t)f->size <= region
            ? fbase + f->offset : zero_field;
        size_t need = 512;
        if (typed_field_null(bm, bm_bytes, i)) need = 5;
        else if (f->type == FT_VARCHAR) {
````````

<!-- R1-BLOCK s2e40-new -->
````````c
            LOG_ERROR(LOG_SUB_CONFIG, "decode_field_to_buf: json_escape_into overflow for enum field [%s] value [%s]", f->name, s);
            return -1;
        }
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}

char *typed_decode_bounded_size(const TypedSchema *ts,const uint8_t *data,int data_len,
    uint8_t hdr2,size_t budget,size_t *allocated) {
    if(allocated)*allocated=0;
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS) {
        errno = EINVAL;
        return NULL;
    }
    if (data_len < 0) { errno = EILSEQ; return NULL; }
    if (ts->packed.nfields)
        {
        if(!data){errno=EINVAL;return NULL;}
        size_t allowance=typed_packed_render_allowance(ts,data,(size_t)data_len,hdr2,NULL,budget);
        char *text=allowance?typed_packed_decode(ts,data,(size_t)data_len,hdr2,allowance):NULL;
        if(text&&allocated)*allocated=strlen(text)+1;
        return text;
    }
    const uint8_t *fbase = NULL, *bm = NULL;
    int bm_bytes = 0;
    size_t region = 0;
    if (data_len < 0 || typed_record_view(data, (size_t)data_len, hdr2,
                                         &fbase, &bm, &bm_bytes, &region)) {
        errno = EILSEQ;
        return NULL;
    }
    static const uint8_t zero_field[65537];
    int capacities[MAX_FIELDS] = {0};
    size_t est=4,workspace=0;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        if (f->offset < 0 || f->size < 1 || f->size > (int)sizeof(zero_field)) {
            errno = EILSEQ;
            return NULL;
        }
        const uint8_t *src = (size_t)f->offset + (size_t)f->size <= region
            ? fbase + f->offset : zero_field;
        size_t need = 512;
        if (typed_field_null(bm, bm_bytes, i)) need = 5;
        else if (f->type == FT_VARCHAR) {
````````

<!-- R1-BLOCK s2e41-old -->
````````text
            if (n > (size_t)f->size - 2) n = (size_t)f->size - 2;
            need = 6 * n + 3;
        } else if (f->type == FT_ENUM) {
            if (f->enum_width == 2 && f->size < 2) { errno = EILSEQ; return NULL; }
            unsigned index = f->enum_width == 2
                ? ((unsigned)src[0] << 8) | src[1] : src[0];
            const char *v = f->enum_values && f->n_enum_values > 0 && index < (unsigned)f->n_enum_values
                ? f->enum_values[index] : NULL;
            size_t n = v ? strlen(v) : 0;
            if (n > (INT_MAX - 4u) / 6u) { errno = EOVERFLOW; return NULL; }
            need = 6 * n + 4;
        }
        size_t name = strlen(f->name);
        if (name > SIZE_MAX - need - 4 || est > SIZE_MAX - name - need - 4) {
            errno = EOVERFLOW;
            return NULL;
        }
        capacities[i] = (int)need;
        est += name + need + 4;
    }
    char *buf = malloc(est);
    if (!buf) { errno = ENOMEM; return NULL; }
    size_t pos = 0;
    buf[pos++] = '{';
    int first = 1;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        const uint8_t *src = (size_t)f->offset + (size_t)f->size <= region
            ? fbase + f->offset : zero_field;
        char stack[512], *value = stack;
        int len;
````````

<!-- R1-BLOCK s2e41-new -->
````````c
            if (n > (size_t)f->size - 2) n = (size_t)f->size - 2;
            need = 6 * n + 3;
        } else if (f->type == FT_ENUM) {
            if (f->enum_width == 2 && f->size < 2) { errno = EILSEQ; return NULL; }
            unsigned index = f->enum_width == 2
                ? ((unsigned)src[0] << 8) | src[1] : src[0];
            const char *v = f->enum_values && f->n_enum_values > 0 && index < (unsigned)f->n_enum_values
                ? f->enum_values[index] : NULL;
            size_t n = v ? strlen(v) : 0;
            if (n > (INT_MAX - 4u) / 6u) { errno = EOVERFLOW; return NULL; }
            need = 6 * n + 4;
        }
        if(need>512&&need>workspace)workspace=need;
        size_t name=strlen(f->name);
        if(name>(SIZE_MAX-2)/6){errno=EOVERFLOW;return NULL;}
        name=6*name+2;
        if (name > SIZE_MAX - need - 4 || est > SIZE_MAX - name - need - 4) {
            errno = EOVERFLOW;
            return NULL;
        }
        capacities[i] = (int)need;
        est += name + need + 4;
    }
    if(est>budget||workspace>budget-est){errno=ENOMEM;return NULL;}
    char *buf = malloc(est);
    if (!buf) { errno = ENOMEM; return NULL; }
    size_t pos = 0;
    buf[pos++] = '{';
    int first = 1;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        const uint8_t *src = (size_t)f->offset + (size_t)f->size <= region
            ? fbase + f->offset : zero_field;
        char stack[512], *value = stack;
        int len;
````````

<!-- R1-BLOCK s2e42-old -->
````````text
            len = decode_field_to_buf(f, src, value,
                                      cap > (int)sizeof(stack) ? cap : (int)sizeof(stack),
                                      (hdr2 & SREC_FLAG_NULLMAP) != 0);
            if (len < 0 || len >= (cap > (int)sizeof(stack) ? cap : (int)sizeof(stack))) {
                if (value != stack) free(value);
                free(buf);
                errno = EILSEQ;
                return NULL;
            }
        }
        /* Preserve unflagged legacy skip-on-zero/absence semantics. */
        if (len == 0) { if (value != stack) free(value); continue; }
        int written = snprintf(buf + pos, est - pos, "%s\"%s\":%s",
                               first ? "" : ",", f->name, value);
        if (value != stack) free(value);
        if (written < 0 || (size_t)written >= est - pos) {
            free(buf);
            errno = EOVERFLOW;
            return NULL;
        }
        pos += (size_t)written;
        first = 0;
    }
    if (pos > est - 2) { free(buf); errno = EOVERFLOW; return NULL; }
    buf[pos++] = '}';
    buf[pos] = '\0';
    return buf;
}

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
````````

<!-- R1-BLOCK s2e42-new -->
````````c
            len = decode_field_to_buf(f, src, value,
                                      cap > (int)sizeof(stack) ? cap : (int)sizeof(stack),
                                      (hdr2 & SREC_FLAG_NULLMAP) != 0);
            if (len < 0 || len >= (cap > (int)sizeof(stack) ? cap : (int)sizeof(stack))) {
                if (value != stack) free(value);
                free(buf);
                errno = EILSEQ;
                return NULL;
            }
        }
        /* Preserve unflagged legacy skip-on-zero/absence semantics. */
        if (len == 0) { if (value != stack) free(value); continue; }
        if(!first)buf[pos++]=',';
        buf[pos++]='"';
        int escaped=json_escape_into(buf+pos,est-pos,f->name,strlen(f->name));
        if(escaped<0){if(value!=stack)free(value);free(buf);errno=EOVERFLOW;return NULL;}
        pos+=(size_t)escaped;buf[pos++]='"';buf[pos++]=':';
        int written=snprintf(buf+pos,est-pos,"%s",value);
        if (value != stack) free(value);
        if (written < 0 || (size_t)written >= est - pos) {
            free(buf);
            errno = EOVERFLOW;
            return NULL;
        }
        pos += (size_t)written;
        first = 0;
    }
    if (pos > est - 2) { free(buf); errno = EOVERFLOW; return NULL; }
    buf[pos++] = '}';
    buf[pos] = '\0';
    if(allocated)*allocated=est;
    return buf;
}
char *typed_decode_bounded(const TypedSchema *ts,const uint8_t *data,int len,uint8_t flags,size_t budget){
    return typed_decode_bounded_size(ts,data,len,flags,budget,NULL);
}
char *typed_decode(const TypedSchema *ts,const uint8_t *data,int data_len,uint8_t hdr2){
    return typed_decode_bounded(ts,data,data_len,hdr2,g_query_buffer_max_bytes);
}

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
````````

<!-- R1-BLOCK s2e43-old -->
````````text
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        /* Heap copy — parse_default_modifier mutates the buffer, and
           lines[a] is written verbatim to fields.conf later, so the
           original must stay intact. The type portion can be up to
           MAX_FIELD_DEF bytes; it must never be copied into a smaller
           buffer. */
        char *clean_spec = strdup(colon + 1);
        if (!clean_spec) {
            OUT("{\"error\":\"out of memory\"}\n");
            return 1;
        }
        if (parse_default_modifier(clean_spec, &tf) < 0) {
            free(clean_spec);
            free_enum_values(&tf);
            OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[a]);
            return 1;
        }
        parse_field_type(clean_spec, &tf);
        free(clean_spec);
        if (tf.type == FT_NONE || tf.size <= 0) {
            OUT("{\"error\":\"Invalid type in: %s\"}\n", lines[a]);
            return 1;
        }
        /* Pre-flight: refuse random(N) with hex output that exceeds the
           field's storage. random(N) generates 2*N hex chars; for varchar
           the on-disk content cap is f->size - 2. For other types the
           encoded representation is rendered through encode_field which
           takes a string — best-effort is to refuse if the raw hex won't
           fit a varchar. Non-varchar destinations are caller error. */
        if (tf.default_kind == DK_RANDOM) {
            int n_bytes = atoi(tf.default_val);
            int hex_chars = n_bytes * 2;
            if (tf.type == FT_VARCHAR) {
                int cap = tf.size - 2;  /* uint16 length prefix */
````````

<!-- R1-BLOCK s2e43-new -->
````````c
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        /* Heap copy — parse_default_modifier mutates the buffer, and
           lines[a] is written verbatim to fields.conf later, so the
           original must stay intact. The type portion can be up to
           MAX_FIELD_DEF bytes; it must never be copied into a smaller
           buffer. */
        char *clean_spec = strdup(colon + 1);
        if (!clean_spec) {
            OUT("{\"error\":\"out of memory\"}\n");
            return 1;
        }
        if (parse_default_modifier_checked(clean_spec, &tf) < 0) {
            free(clean_spec);
            free_enum_values(&tf);
            OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[a]);
            return 1;
        }
        if(typed_legacy_decl_size(clean_spec)<=0){
            free(clean_spec);OUT("{\"error\":\"Invalid field declaration\"}\n");return 1;
        }
        parse_field_type(clean_spec,&tf);
        free(clean_spec);
        if (tf.type == FT_NONE || tf.size <= 0) {
            OUT("{\"error\":\"Invalid type in: %s\"}\n", lines[a]);
            return 1;
        }
        if(typed_default_validate(&tf)){
            free_enum_values(&tf);OUT("{\"error\":\"Invalid default in field declaration\"}\n");return 1;
        }
        /* Pre-flight: refuse random(N) with hex output that exceeds the
           field's storage. random(N) generates 2*N hex chars; for varchar
           the on-disk content cap is f->size - 2. For other types the
           encoded representation is rendered through encode_field which
           takes a string — best-effort is to refuse if the raw hex won't
           fit a varchar. Non-varchar destinations are caller error. */
        if (tf.default_kind == DK_RANDOM) {
            int n_bytes = atoi(tf.default_val);
            int hex_chars = n_bytes * 2;
            if (tf.type == FT_VARCHAR) {
                int cap = tf.size - 2;  /* uint16 length prefix */
````````

<!-- R1-BLOCK s2e44-old -->
````````text
   bm_cache_init/slotcask_init are all no-ops when already initialized,
   so this is a safe unconditional call regardless of what the prior test
   left behind. */
void test_reset_caches(void) {
    schema_caches_shutdown();
    if (g_db) {
        bt_cache_init(g_db->btcache_cap);
        bm_cache_init(g_db->btcache_cap);
        slotcask_init(g_db->fcache_cap, g_db->fcache_cap);
    }
}
#endif

int parse_default_modifier_checked(char*s,TypedField*f){return parse_default_modifier(s,f);}
char *generate_default_owned(const TypedField*f,const char*r,const char*o,size_t cap){
 if(cap<256||f->default_invalid)return NULL;
 char *b=malloc(256);if(!b)return NULL;
 const char*v=generate_default(f,b,256,r,o);
 if(!v){free(b);return NULL;}if(v!=b)snprintf(b,256,"%s",v);return b;
}
````````

<!-- R1-BLOCK s2e44-new -->
````````c
   bm_cache_init/slotcask_init are all no-ops when already initialized,
   so this is a safe unconditional call regardless of what the prior test
   left behind. */
void test_reset_caches(void) {
    schema_caches_shutdown();
    if (g_db) {
        bt_cache_init(g_db->btcache_cap);
        bm_cache_init(g_db->btcache_cap);
        slotcask_init(g_db->fcache_cap, g_db->fcache_cap);
    }
}
#endif
````````

<!-- R1-BLOCK s2e45-old -->
````````text
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
````````

<!-- R1-BLOCK s2e45-new -->
````````c
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
    size_t selector=options->fields?strnlen(options->fields,MAX_LINE)+1:0;
    if(selector>MAX_LINE||selector>=budget){c->error=ENOBUFS;return -1;}
    budget-=selector;
    char *text=typed_packed_csv_header(fs->ts,options->fields,options->delimiter,budget,&n);
    return csv_add(c,text,n);
}
static int csv_record(CsvCommand *c,const void *value,size_t vlen,uint8_t hdr2,
    const char *wire,size_t wire_len){
    if(csv_deadline(c))return -1;
    size_t budget=csv_available(c),n=0;if(!budget)return -1;
    budget=typed_packed_render_allowance(c->fs->ts,value,vlen,hdr2,c->options->fields,budget);
    if(!budget){c->error=errno?errno:ENOBUFS;return -1;}
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
````````

<!-- R1-BLOCK s2e46-old -->
````````text
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
````````

<!-- R1-BLOCK s2e46-new -->
````````c
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
    c.limit=(size_t)limit;    int rc=slotcask_walk_live_checked(db,csv_fetch_record,&c);
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
````````

<!-- R1-BLOCK s2e47-old -->
````````text
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
````````

<!-- R1-BLOCK s2e47-new -->
````````c
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
        size_t wire_max=schema->auto_key==AK_NONE?(size_t)schema->max_key:MAX_KEY_CEILING;
    p=json_skip(p);
    while(*p!=']'&&!c.error){
        if(csv_deadline(&c))break;
        if(*p!='"'){c.error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        if(*end!='"'||raw>6u*wire_max){c.error=E2BIG;break;}
        if(raw==SIZE_MAX||raw+1>csv_available(&c)){c.error=ENOBUFS;break;}
        char *wire=NULL;size_t wire_len=0;errno=0;
        if(json_unescape_string(start,raw,&wire,&wire_len)){c.error=errno==ENOMEM?ENOMEM:EINVAL;break;}
        c.bytes+=raw+1;
        if(wire_len>wire_max){free(wire);c.bytes-=raw+1;c.error=E2BIG;break;}
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
            int rc=slotcask_lookup_by_hash_checked(db,hash,csv_get_record,&c);
            if(rc<0&&!c.error)c.error=EIO;
            if(!c.error&&!c.found)csv_missing(&c);
        }
        free(wire);c.bytes-=raw+1;p=json_skip(end+1);
        if(*p==',')p=json_skip(p+1);
        else if(*p!=']')c.error=EINVAL;
    }
    if(!c.error)csv_deadline(&c);
    return csv_finish(&c,out);
}
````````

<!-- R1-BLOCK s2e48-old -->
````````text
        fprintf(stderr, "%d errors during bulk insert (see info log for dropped keys)\n", errors);
    } else if (skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if already freed via the auto-key branch */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert(const char *db_root, const char *object, const char *input,
                    int if_not_exists) {
    size_t len;
    char *json;
    int json_mmaped = 0;
    if (input) {
        /* mmap the file instead of malloc — OS pages in/out as we scan */
        int ifd = open(input, O_RDONLY);
        if (ifd < 0) { fprintf(stderr, "Error: Cannot open %s\n", input); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); fprintf(stderr, "Error: Cannot stat %s\n", input); return 1; }
        len = st.st_size;
        if (len == 0) { close(ifd); fprintf(stderr, "Error: Empty input\n"); return 1; }
        /* MAP_PRIVATE on a file fd lets us write the trailing NUL into
````````

<!-- R1-BLOCK s2e48-new -->
````````c
        fprintf(stderr, "%d errors during bulk insert (see info log for dropped keys)\n", errors);
    } else if (skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if already freed via the auto-key branch */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert(const char *db_root, const char *object, const char *input,
                    int if_not_exists) {
    TypedSchema *packed=load_typed_schema(db_root,object);
    if(packed&&packed->packed.nfields){
        Schema schema=load_schema(db_root,object);
        return typed_bulk_insert_source(db_root,object,&schema,packed,input,0,0,if_not_exists);
    }

    size_t len;
    char *json;
    int json_mmaped = 0;
    if (input) {
        /* mmap the file instead of malloc — OS pages in/out as we scan */
        int ifd = open(input, O_RDONLY);
        if (ifd < 0) { fprintf(stderr, "Error: Cannot open %s\n", input); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); fprintf(stderr, "Error: Cannot stat %s\n", input); return 1; }
        len = st.st_size;
        if (len == 0) { close(ifd); fprintf(stderr, "Error: Empty input\n"); return 1; }
        /* MAP_PRIVATE on a file fd lets us write the trailing NUL into
````````

<!-- R1-BLOCK s2e49-old -->
````````text
    } else if (delim_skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, delim_skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if NULL */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter,
                               int if_not_exists) {
    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }

    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st;
    if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
    if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
    const char *data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, ifd, 0);
    int data_mmaped = 1;
    if (data == MAP_FAILED) {
        char *buf = malloc(st.st_size);
        if (!buf) { close(ifd); return 1; }
````````

<!-- R1-BLOCK s2e49-new -->
````````c
    } else if (delim_skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, delim_skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if NULL */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter,
                               int if_not_exists) {
    TypedSchema *packed=load_typed_schema(db_root,object);
    if(packed&&packed->packed.nfields){
        Schema schema=load_schema(db_root,object);
        return typed_bulk_insert_source(db_root,object,&schema,packed,filepath,1,delimiter,if_not_exists);
    }

    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }

    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st;
    if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
    if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
    const char *data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, ifd, 0);
    int data_mmaped = 1;
    if (data == MAP_FAILED) {
        char *buf = malloc(st.st_size);
        if (!buf) { close(ifd); return 1; }
````````

<!-- R1-BLOCK s2e50-old -->
````````text
#include "types.h"
#include "slotcask.h"
#include "io_direct.h"
#include "query_internal.h"
#include "typed_row.h"
#include "packed_csv_command.h"
#include <dirent.h>
#include <fnmatch.h>

/* ========== Probing helpers ========== */

/* Check if slot at given offset matches our key. Returns:
   1 = exact match (same key, active)
   0 = empty slot (can write here)
  -1 = tombstone (deleted, can write here on insert, skip on get)
  -2 = occupied by different key (continue probing) */
/* ========== v2 (slotcask) scan bridge ========== */

````````

<!-- R1-BLOCK s2e50-new -->
````````c
#include "schema_wire.h"
#include "types.h"
#include "slotcask.h"
#include "io_direct.h"
#include "query_internal.h"
#include "typed_row.h"
#include "packed_csv_command.h"
#include "typed_read_command.h"
#include <dirent.h>
#include <fnmatch.h>

/* ========== Probing helpers ========== */

/* Check if slot at given offset matches our key. Returns:
   1 = exact match (same key, active)
   0 = empty slot (can write here)
  -1 = tombstone (deleted, can write here on insert, skip on get)
  -2 = occupied by different key (continue probing) */
/* ========== v2 (slotcask) scan bridge ========== */

````````

<!-- R1-BLOCK s2e51-old -->
````````text
                                     const char *seq_name, int n);
extern void encode_field(const TypedField *f, const char *val, uint8_t *out);

/* Parse a single fields.conf-style line "name:type[:param][:default=...]"
   into a TypedField. Returns 1 on success, 0 on failure (bad line). Does
   NOT set offset. Default-modifier suffix is captured on out->default_kind
   / default_val so both add-field's rebuild append and edit-field carry
   the new default through end-to-end. */
int parse_field_line(const char *line, TypedField *out) {
    const char *colon = strchr(line, ':');
    if (!colon || colon == line) return 0;
    size_t nlen = colon - line;
    if (nlen >= 256) nlen = 255;
    memcpy(out->name, line, nlen);
    out->name[nlen] = '\0';
    /* Strip default-modifier suffix into out->default_kind/default_val so
       both add-field's rebuild append AND edit-field carry computed
       defaults through end-to-end. Without this, parse_field_type would
       see "int:default=99" and return FT_NONE (no exact-match), making
       cmd_edit_fields reject every :default= edit. Heap copy: the type
       portion can be up to MAX_FIELD_DEF bytes (enum value lists) and
       parse_default_modifier mutates the buffer — the caller reuses the
       original line verbatim for the fields.conf write. */
    char *clean_spec = strdup(colon + 1);
    if (!clean_spec) return 0;
    if (parse_default_modifier(clean_spec, out) < 0) {
        /* Invalid modifier structure — reject the line; the caller
           emits "Invalid field line". */
        free(clean_spec);
        return 0;
    }
    parse_field_type(clean_spec, out);
    free(clean_spec);
    return out->type != FT_NONE && out->size > 0;
}

/* === v2 (slotcask) rebuild path ===
 *
 * Used by add-fields, vacuum --compact, and vacuum --splits when the object
 * is storage_version=2. Caller holds objlock_wrlock — no concurrent ops can
 * race with the rebuild.
 *
 * Strategy: stage the object's current slotcask data files into a
 * `.legacy/` subdir, open a read-only slotcask handle against that dir,
 * open a fresh write handle in the object root with the new schema, walk
 * legacy via slotcask_walk_live, transform each record (memcpy by
````````

<!-- R1-BLOCK s2e51-new -->
````````c
                                     const char *seq_name, int n);
extern void encode_field(const TypedField *f, const char *val, uint8_t *out);

/* Parse a single fields.conf-style line "name:type[:param][:default=...]"
   into a TypedField. Returns 1 on success, 0 on failure (bad line). Does
   NOT set offset. Default-modifier suffix is captured on out->default_kind
   / default_val so both add-field's rebuild append and edit-field carry
   the new default through end-to-end. */
int parse_field_line(const char *line, TypedField *out) {
    const char *colon = strchr(line, ':');
    if (!colon || colon == line) return 0;
    size_t nlen = colon - line;
    if (nlen >= sizeof(out->name)) return 0;
    out->name_len=(int)nlen;
    memcpy(out->name, line, nlen);
    out->name[nlen] = '\0';
    /* Strip default-modifier suffix into out->default_kind/default_val so
       both add-field's rebuild append AND edit-field carry computed
       defaults through end-to-end. Without this, parse_field_type would
       see "int:default=99" and return FT_NONE (no exact-match), making
       cmd_edit_fields reject every :default= edit. Heap copy: the type
       portion can be up to MAX_FIELD_DEF bytes (enum value lists) and
       parse_default_modifier mutates the buffer — the caller reuses the
       original line verbatim for the fields.conf write. */
    char *clean_spec = strdup(colon + 1);
    if (!clean_spec) return 0;
    if (parse_default_modifier_checked(clean_spec, out) < 0) {
        /* Invalid modifier structure — reject the line; the caller
           emits "Invalid field line". */
        free(clean_spec);
        return 0;
    }
    if(typed_legacy_decl_size(clean_spec)<=0){free(clean_spec);return 0;}
    parse_field_type(clean_spec,out);
    free(clean_spec);
    return out->type != FT_NONE && out->size > 0 && !typed_default_validate(out);
}

/* === v2 (slotcask) rebuild path ===
 *
 * Used by add-fields, vacuum --compact, and vacuum --splits when the object
 * is storage_version=2. Caller holds objlock_wrlock — no concurrent ops can
 * race with the rebuild.
 *
 * Strategy: stage the object's current slotcask data files into a
 * `.legacy/` subdir, open a read-only slotcask handle against that dir,
 * open a fresh write handle in the object root with the new schema, walk
 * legacy via slotcask_walk_live, transform each record (memcpy by
````````

<!-- R1-BLOCK s2e52-old -->
````````text
    while (*s) {
        if (*s == '.' && !has_dot) { has_dot = 1; s++; continue; }
        if (*s < '0' || *s > '9') return 0;
        s++;
    }
    return 1;
}

/* ========== EXISTS ========== */

int cmd_exists(const char *db_root, const char *object,
               const char *key, size_t klen) {
    Schema sc = load_schema(db_root, object);

    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("false\n"); return 1; }
    int rc = slotcask_exists(sdb, key, klen);
    OUT("%s\n", rc == 1 ? "true" : "false");
    return rc == 1 ? 0 : 1;
}



/* ========== KEYS ========== */

typedef struct {
    int offset; int limit; int count; int printed;
    char csv_delim;   /* 0 = JSON mode; else CSV (delim-less, single column) */
    pthread_mutex_t lock;  /* serializes the emit section across parallel shards */
} KeysCtx;

int keys_cb(const SlotHeader *hdr, const uint8_t *block,
                    void *ctx) {
    KeysCtx *kc = (KeysCtx *)ctx;

    /* Copy the key bytes to a thread-local buffer outside the lock. */
    char kbuf[1024];
    size_t kl = hdr->key_len < sizeof(kbuf) - 1 ? hdr->key_len : sizeof(kbuf) - 1;
    memcpy(kbuf, block, kl); kbuf[kl] = '\0';

    pthread_mutex_lock(&kc->lock);
    if (kc->limit > 0 && kc->printed >= kc->limit) {
        pthread_mutex_unlock(&kc->lock);
        return 1;
    }
    kc->count++;
    if (kc->count <= kc->offset) {
        pthread_mutex_unlock(&kc->lock);
        return 0;
    }
    if (kc->csv_delim) {
        csv_emit_cell(kbuf, kc->csv_delim);
        OUT("\n");
    } else {
        OUT("%s\"%s\"", kc->printed ? "," : "", kbuf);
    }
    kc->printed++;
    pthread_mutex_unlock(&kc->lock);
    return 0;
}

int cmd_keys(const char *db_root, const char *object, int offset, int limit,
             const char *format, const char *delimiter) {
    if (limit <= 0) limit = g_global_limit;
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    KeysCtx ctx = { offset, limit, 0, 0, csv_delim, PTHREAD_MUTEX_INITIALIZER };
    if (csv_delim) OUT("key\n");  /* header */
    else OUT("[");
    /* For v2 with a small caller-set limit, route through the streaming
       per-shard walker — Pass-1 ref-buffering wastes time collecting refs
       a limit-bound caller never reads. The threshold (1000) is well below
       typical full-scan workloads but covers the common KEYS first N case
       and admin previews. */
    int use_streaming = (limit > 0 && limit <= 1000);
    if (use_streaming) {
        SlotcaskSchemaInfo info = {
            .splits = sch.splits, .slot_size = sch.slot_size,
            .streams = sch.streams,
        };
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (sdb) scan_shards_v2_streaming(sdb, keys_cb, &ctx);
        else     scan_dispatch(db_root, object, &sch, data_dir, keys_cb, &ctx);
    } else {
        scan_dispatch(db_root, object, &sch, data_dir, keys_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
    if (!csv_delim) OUT("]\n");
    return 0;
}

/* ========== FETCH (paginated scan with optional field projection) ========== */

/* Render block's key bytes as the wire-form string, honouring the
   object's auto_key mode (carried on FieldSchema). For AK_NONE the
   output is a verbatim NUL-terminated copy; for AK_UUID the 16 bytes
   render as 36-char dashed; for AK_SEQ the 8 bytes render as decimal. */
static void render_wire_key(const FieldSchema *fs, const uint8_t *block,
                             uint16_t klen, char out[1100]) {
    const Schema *sc = (fs && fs->auto_key != AK_NONE)
                        ? &fs->auto_key_schema_snapshot : NULL;
````````

<!-- R1-BLOCK s2e52-new -->
````````c
    while (*s) {
        if (*s == '.' && !has_dot) { has_dot = 1; s++; continue; }
        if (*s < '0' || *s > '9') return 0;
        s++;
    }
    return 1;
}

/* ========== EXISTS ========== */

int cmd_exists(const char *db_root, const char *object,
               const char *key, size_t klen) {
    return typed_read_exists(db_root,object,key,klen);
}



/* ========== KEYS ========== */




int cmd_keys(const char *db_root, const char *object, int offset, int limit,
             const char *format, const char *delimiter) {
    TypedReadOptions options={.kind=TYPED_READ_KEYS,.format=format,
        .delimiter=(format&&!strcmp(format,"csv"))?parse_csv_delim(delimiter):0,
        .offset=offset,.limit=limit>0?limit:g_global_limit,.budget=g_query_buffer_max_bytes,
        .timeout_ms=resolve_timeout_ms()};
    return typed_read_route(db_root,object,&options);
}

/* ========== FETCH (paginated scan with optional field projection) ========== */

/* Render block's key bytes as the wire-form string, honouring the
   object's auto_key mode (carried on FieldSchema). For AK_NONE the
   output is a verbatim NUL-terminated copy; for AK_UUID the 16 bytes
   render as 36-char dashed; for AK_SEQ the 8 bytes render as decimal. */
static void render_wire_key(const FieldSchema *fs, const uint8_t *block,
                             uint16_t klen, char out[1100]) {
    const Schema *sc = (fs && fs->auto_key != AK_NONE)
                        ? &fs->auto_key_schema_snapshot : NULL;
````````

<!-- R1-BLOCK s2e53-old -->
````````text
}

/* === v2 cmd_fetch worker ===
 *
 * v1's cursor encodes `(shard_path_idx, slot_idx)` and resumes mid-walk; v2
 * has no analogous addressable position (slotcask_walk_live walks all kf
 * shards). The v2 cursor is a simple `"N"` integer = how many live records
 * were already returned by prior pages. Resume = skip N entries before
 * emitting. Stable across calls because slotcask_walk_live's order is
 * deterministic given an unchanged dataset. Concurrent inserts can shift
 * the slot a record lives in but the per-kf-shard scan itself is
 * deterministic for read-only iteration. */
typedef struct {
    int     csv_delim;
    int     rows_fmt;
    int     dict_fmt;
    int     to_skip;            /* records still to skip (cursor + offset combined) */
    int     limit;
    int     printed;            /* emitted so far (this call) */
    int     scanned_so_far;     /* live records observed (skipped + emitted) */
    const char **proj_fields;
    int     proj_count;
    FieldSchema *fs;
} V2FetchCtx;

static int v2_fetch_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        uint8_t hdr2, void *ctxp) {
    (void)hash16;
    V2FetchCtx *ctx = (V2FetchCtx *)ctxp;
    ctx->scanned_so_far++;
    if (ctx->to_skip > 0) { ctx->to_skip--; return 0; }
    if (ctx->printed >= ctx->limit) return 1;

    /* Reuse the existing v1 emit helpers — they take a SlotHeader + block
       pointer where block = key bytes followed by value bytes. Synthesize a
       SlotHeader for v2 (flag=1, klen, vlen) and compose the block in a
       small heap buffer. */
    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash16, 16);
    hdr.flag = 1 | (((unsigned)hdr2 & 0xFF) << 8);
    hdr.key_len = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk)) ? stk : malloc(klen + vlen);
    if (!block) return 1;
    memcpy(block, key, klen);
    memcpy(block + klen, value, vlen);

    char wire_key[1100];
    render_wire_key(ctx->fs, block, hdr.key_len, wire_key);

    if (ctx->csv_delim) {
        csv_emit_row(wire_key, block + klen, (uint32_t)vlen,
                      ctx->proj_count > 0 ? ctx->proj_fields : NULL,
                      ctx->proj_count, ctx->fs, (char)ctx->csv_delim, hdr2);
        ctx->printed++;
    } else if (ctx->rows_fmt) {
        print_record_row(&hdr, block, ctx->proj_fields, ctx->proj_count,
                          &ctx->printed, ctx->fs);
    } else if (ctx->dict_fmt) {
        OUT("%s\"%s\":", ctx->printed ? "," : "", wire_key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen, hdr2) : NULL;
        OUT("%s", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    } else {
        OUT("%s{\"key\":\"%s\",\"value\":", ctx->printed ? "," : "", wire_key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen, hdr2) : NULL;
        OUT("%s}", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    }

    if (block != stk) free(block);
    return 0;
}

static int cmd_fetch_v2(const char *db_root, const char *object,
                         int offset, int limit, const char *proj_str,
                         const char *cursor, const char *format,
                         const char *delimiter, const Schema *sch,
                         int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    if (limit <= 0) limit = g_global_limit;

    FieldSchema fs_fetch;
    init_field_schema(&fs_fetch, db_root, object);
    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;
    if (csv_delim && fs_fetch.ts && fs_fetch.ts->packed.nfields) {
        SlotcaskSchemaInfo info = {.splits=sch->splits,.slot_size=sch->slot_size,.streams=sch->streams};
        SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(db_root,object,&info);
        PackedCsvOptions options={.fields=proj_str,.delimiter=csv_delim,
            .buffer_budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
        return packed_csv_fetch(db,&fs_fetch,offset,limit,cursor,want_total,&options,g_out?g_out:stdout);
    }

    const char *proj_fields[MAX_FIELDS];
    char proj_buf[MAX_LINE];
    int proj_count = 0;
    if (proj_str && proj_str[0]) {
        snprintf(proj_buf, sizeof(proj_buf), "%s", proj_str);
        char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
        while (tok && proj_count < MAX_FIELDS) {
            proj_fields[proj_count++] = tok;
            tok = strtok_r(NULL, ",", &_tok_save);
        }
    }

    int cursor_n = 0;
    if (cursor && cursor[0]) cursor_n = atoi(cursor);

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);

    if (csv_delim) {
        if (want_total) {
            OUT("{\"error\":\"\\\"total\\\" with format=csv is not supported\"}\n");
            return -1;
        }
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count, fs_ptr, csv_delim);
    } else if (rows_fmt)
        emit_rows_columns(proj_fields, proj_count, fs_ptr);
    else if (dict_fmt)
        OUT(want_total ? "{\"rows\":{" : "{\"results\":{");
    else
        OUT(want_total ? "{\"rows\":[" : "{\"results\":[");

    V2FetchCtx fctx = {0};
    fctx.csv_delim = csv_delim;
    fctx.rows_fmt = rows_fmt;
    fctx.dict_fmt = dict_fmt;
    fctx.to_skip = 0;        /* skip handled by walk_live_skip below */
    fctx.limit = limit;
    fctx.proj_fields = proj_fields;
    fctx.proj_count = proj_count;
    fctx.fs = fs_ptr;
    int64_t skip_n = (int64_t)cursor_n + (offset > 0 ? offset : 0);
    /* slotcask_walk_live_skip cheaply skips past the first N live
       records (no segcache touch) before invoking the callback —
       offset=5000 went from ~5ms (per-record segcache_acquire even on
       skip) to <1ms. */
    if (sdb) slotcask_walk_live_skip(sdb, skip_n, v2_fetch_cb, &fctx);

    if (csv_delim || rows_fmt) {
        /* No JSON wrapper. CSV / rows formats stream as-is. */
    } else if (want_total) {
        /* total mode: emit {"rows":[...],"total":null} — cursor is omitted */
        const char *close = dict_fmt ? "}" : "]";
        OUT("%s,\"total\":null}\n", close);
    } else {
        const char *close = dict_fmt ? "}" : "]";
        if (fctx.printed >= limit) {
            int next_cur = cursor_n + fctx.printed;
            OUT("%s,\"cursor\":\"%d\"}\n", close, next_cur);
        } else {
            OUT("%s,\"cursor\":null}\n", close);
        }
    }
    return 0;
}

/* Cursor-based fetch — scans from cursor position, returns next cursor.
   Cursor format: "shard_path_idx:slot_idx" or empty for start.
   Response: {"results":[...],"cursor":"..."} */
int cmd_fetch(const char *db_root, const char *object,
                     int offset, int limit, const char *proj_str,
                     const char *cursor, const char *format,
                     const char *delimiter, int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
````````

<!-- R1-BLOCK s2e53-new -->
````````c
}

/* === v2 cmd_fetch worker ===
 *
 * v1's cursor encodes `(shard_path_idx, slot_idx)` and resumes mid-walk; v2
 * has no analogous addressable position (slotcask_walk_live walks all kf
 * shards). The v2 cursor is a simple `"N"` integer = how many live records
 * were already returned by prior pages. Resume = skip N entries before
 * emitting. Stable across calls because slotcask_walk_live's order is
 * deterministic given an unchanged dataset. Concurrent inserts can shift
 * the slot a record lives in but the per-kf-shard scan itself is
 * deterministic for read-only iteration. */



static int cmd_fetch_v2(const char *db_root, const char *object,
                         int offset, int limit, const char *proj_str,
                         const char *cursor, const char *format,
                         const char *delimiter, const Schema *sch,
                         int want_total) {
    TypedReadOptions options={.kind=TYPED_READ_FETCH,.fields=proj_str,.format=format,
        .delimiter=(format&&!strcmp(format,"csv"))?parse_csv_delim(delimiter):0,
        .cursor=cursor,.offset=offset,.limit=limit>0?limit:g_global_limit,.want_total=want_total,
        .budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
    return typed_read_route(db_root,object,&options);
}

/* Cursor-based fetch — scans from cursor position, returns next cursor.
   Cursor format: "shard_path_idx:slot_idx" or empty for start.
   Response: {"results":[...],"cursor":"..."} */
int cmd_fetch(const char *db_root, const char *object,
                     int offset, int limit, const char *proj_str,
                     const char *cursor, const char *format,
                     const char *delimiter, int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
````````

<!-- R1-BLOCK s2e54-old -->
````````text
#include "types.h"
#include "type_desc.h"
#include "slotcask.h"
#include "bitmap.h"
#include "query_internal.h"
#include <dirent.h>

/* ========== edit-field ==========
 *
 * Same-type, in-place field edits (varchar grow/shrink, integer
 * widen/narrow, numeric scale change, float→double widen). Wire shape:
 *   {"mode":"edit-field","dir":..,"object":..,"fields":["name:varchar:200", ..]}
````````

<!-- R1-BLOCK s2e54-new -->
````````c
#include "schema_wire.h"
#include "types.h"
#include "type_desc.h"
#include "slotcask.h"
#include "bitmap.h"
#include "query_internal.h"
#include <dirent.h>

/* ========== edit-field ==========
 *
 * Same-type, in-place field edits (varchar grow/shrink, integer
 * widen/narrow, numeric scale change, float→double widen). Wire shape:
 *   {"mode":"edit-field","dir":..,"object":..,"fields":["name:varchar:200", ..]}
````````

<!-- R1-BLOCK s2e55-old -->
````````text
        finalize_ctx.idx_skipped);
    for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
    return 0;
}

/* __attribute__((cleanup)) helper for cmd_create_object's field_specs.
   GCC passes the cleanup the address of the variable, so the parameter
   is a pointer to the array, not the decayed char **. */
static void free_field_specs(char *(*specs)[MAX_FIELDS]) {
    for (int i = 0; i < MAX_FIELDS; i++) free((*specs)[i]);
}

static int validate_field_type_clean(char *buf);

/* Validate a field type spec like "name:varchar:30" or "age:int".
   Returns the storage size (>0) on success, 0 on invalid. */
static int validate_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0; /* no type separator or empty name */

    /* Heap copy so the type portion can be MAX_FIELD_DEF-sized (enum value
       lists). _clean mutates buf in place (strips modifiers). */
    char *buf = strdup(colon + 1);
    if (!buf) return 0;
    int rc = validate_field_type_clean(buf);
    free(buf);
    return rc;
}

static int validate_field_type_clean(char *buf) {

    /* Strip :removed */
    size_t blen = strlen(buf);
    if (blen >= 8 && strcmp(buf + blen - 8, ":removed") == 0) {
        buf[blen - 8] = '\0';
    }
    /* Strip a trailing :not_null / :nullable so the type matcher below
       sees the bare type spec, then reject any further occurrence —
       varchar/numeric/enum tolerate trailing bytes, so a buried token
       would otherwise be accepted and silently unenforced. */
    size_t nn_len = strlen(buf);
    if (nn_len >= 9 && (strcmp(buf + nn_len - 9, ":not_null") == 0 ||
                        strcmp(buf + nn_len - 9, ":nullable") == 0)) {
        buf[nn_len - 9] = '\0';
    }
    if (strstr(buf, ":not_null") || strstr(buf, ":nullable")) return 0;
    /* Strip default modifiers: :auto_create, :auto_update, :default=... */
    char *dm;
    if ((dm = strstr(buf, ":auto_update")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":auto_create")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":default=")) != NULL) *dm = '\0';

    const char *type = buf;
    if (strncmp(type, "varchar:", 8) == 0) {
        int sz = atoi(type + 8);
        if (sz <= 0 || sz > 65535) return 0;   /* bounded: 1..65535 content bytes */
        return sz + 2;                          /* 2-byte length prefix */
    }
    if (strcmp(type, "varchar") == 0) return 0; /* bare "varchar" — require :N */
    if (strcmp(type, "long") == 0)   return 8;
    if (strcmp(type, "int") == 0)    return 4;
    if (strcmp(type, "short") == 0)  return 2;
    if (strcmp(type, "double") == 0) return 8;
    if (strcmp(type, "float") == 0)  return 4;
    if (strcmp(type, "bool") == 0)   return 1;
    if (strcmp(type, "byte") == 0)   return 1;
    if (strcmp(type, "date") == 0)   return 4;
    if (strcmp(type, "datetime") == 0) return 7;
    if (strcmp(type, "datetimems") == 0) return 8;
    if (strcmp(type, "time") == 0)    return 3;
    if (strcmp(type, "timestamp") == 0) return 8;
    if (strcmp(type, "uuid") == 0)    return 16;
    if (strcmp(type, "ipv4") == 0)    return 4;
    if (strcmp(type, "ipv6") == 0)    return 16;
    if (strcmp(type, "currency") == 0) return 8;
    if (strncmp(type, "numeric:", 8) == 0) {
        /* Full-input P[,S] validation. P stays informational until the
           packed exact format lands; S must fit the int64 scaled storage:
           0..18 (10^18 is the largest power of ten in int64; S = 0 is
           valid). Legacy "numeric:N" without a comma keeps its
           historical scale-2 meaning. Junk suffixes are rejected, not
           atoi-prefixed. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        while (*ps >= '0' && *ps <= '9') ps++;
        if (*ps == ',') {
            ps++;
            if (!(*ps >= '0' && *ps <= '9')) return 0;
            while (*ps >= '0' && *ps <= '9') ps++;
        }
        if (*ps != '\0') return 0;
        const char *comma = strchr(type + 8, ',');
        long s = comma ? strtol(comma + 1, NULL, 10) : 2;
        if (s < 0 || s > 18) return 0;
        return 8;
    }
    if (strncmp(type, "enum(", 5) == 0) {
        /* Validate enum(v1,v2,...) — empty list, duplicates, missing
           close paren, and over-ceiling all → invalid (return 0).
           Returns the storage byte width: 1 for ≤256 values, 2 for
           257..65535. create-object is cold path, simple alloc is fine. */
        const char *open  = type + 5;
        const char *close = strrchr(open, ')');
        if (!close || close <= open) return 0;

        /* Pass 1: count + duplicate-check via a heap-allocated value list.
           A worst-case 65535-entry list of avg ~8B = ~640 KB peak — fine. */
        char **vals = calloc(65536, sizeof(char *));
        if (!vals) return 0;
        int n = 0;
        const char *p = open;
        while (p < close) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl == 0) goto enum_invalid;            /* empty value */
            if (n >= 65535)            goto enum_invalid;
            char *v = malloc(vl + 1);
            if (!v) goto enum_invalid;
            memcpy(v, p, vl); v[vl] = '\0';
            for (int i = 0; i < n; i++) {
                if (strcmp(vals[i], v) == 0) {  /* duplicate */
                    free(v);
                    goto enum_invalid;
                }
            }
            vals[n++] = v;
            p = (next < close) ? next + 1 : close;
        }
        if (n == 0) goto enum_invalid;
        int width = (n <= 256) ? 1 : 2;
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return width;

enum_invalid:
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return 0;
    }
    return 0;
}

/* Validate + parse the auto_key spec. Returns 0 on AK_NONE, 1 on
   AK_UUID, 2 on AK_SEQ (with seq_name filled). On invalid input writes
   the {"error":...} JSON to OUT and returns -1. Empty/NULL → 0. */
static int parse_auto_key_spec(const char *spec, char *out_seq_name, size_t seq_cap) {
    if (!spec || !spec[0]) return 0;
    if (strcmp(spec, "uuid") == 0) return 1;
    if (strncmp(spec, "seq(", 4) == 0) {
        size_t slen = strlen(spec);
        if (slen < 6 || spec[slen - 1] != ')') {
            OUT("{\"error\":\"Invalid auto_key=seq(...) form; expected seq(<name>)\"}\n");
            return -1;
````````

<!-- R1-BLOCK s2e55-new -->
````````c
        finalize_ctx.idx_skipped);
    for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
    return 0;
}

/* __attribute__((cleanup)) helper for cmd_create_object's field_specs.
   GCC passes the cleanup the address of the variable, so the parameter
   is a pointer to the array, not the decayed char **. */
static void free_field_specs(char *(*specs)[MAX_FIELDS]) {
    for (int i = 0; i < MAX_FIELDS; i++) free((*specs)[i]);
}


/* Validate a field type spec like "name:varchar:30" or "age:int".
   Returns the storage size (>0) on success, 0 on invalid. */
static int validate_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0; /* no type separator or empty name */

    /* Heap copy so the type portion can be MAX_FIELD_DEF-sized (enum value
       lists). _clean mutates buf in place (strips modifiers). */
    char *buf = strdup(colon + 1);
    if (!buf) return 0;
    TypedField modifiers={0};
    int parsed=parse_default_modifier_checked(buf,&modifiers);
    int rc=parsed<0?0:typed_legacy_decl_size(buf);
    if(rc){parse_field_type(buf,&modifiers);if(modifiers.type!=FT_ENUM&&typed_default_validate(&modifiers))rc=0;}
    free_enum_values(&modifiers);free(buf);
    return rc;
}



/* Validate + parse the auto_key spec. Returns 0 on AK_NONE, 1 on
   AK_UUID, 2 on AK_SEQ (with seq_name filled). On invalid input writes
   the {"error":...} JSON to OUT and returns -1. Empty/NULL → 0. */
static int parse_auto_key_spec(const char *spec, char *out_seq_name, size_t seq_cap) {
    if (!spec || !spec[0]) return 0;
    if (strcmp(spec, "uuid") == 0) return 1;
    if (strncmp(spec, "seq(", 4) == 0) {
        size_t slen = strlen(spec);
        if (slen < 6 || spec[slen - 1] != ')') {
            OUT("{\"error\":\"Invalid auto_key=seq(...) form; expected seq(<name>)\"}\n");
            return -1;
````````

<!-- R1-BLOCK s2e56-old -->
````````text
        if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) {
            if (if_not_exists) {
                OUT("{\"status\":\"exists\",\"object\":\"%s\",\"dir\":\"%s\"}\n",
                    object, dir);
                return 0;
            }
            OUT("{\"error\":\"object already exists\",\"dir\":\"%s\",\"object\":\"%s\",\"hint\":\"pass \\\"if_not_exists\\\":true for idempotent create, or drop the object first\"}\n",
                dir, object);
            return 1;
        }
    }

    /* Validate fields array — must be non-empty, every field must have a valid type */
    const char *p = json_skip(fields_json);
    if (*p != '[') {
        OUT("{\"error\":\"fields must be a JSON array\"}\n");
        return 1;
    }
    p++;

    /* First pass: validate all fields before creating anything. Rows are
       heap strings (a MAX_FIELD_DEF-sized spec must fit); the cleanup
       attribute frees every row on any of this function's early exits. */
    char *field_specs[MAX_FIELDS] __attribute__((cleanup(free_field_specs))) = {0};
    int nfields = 0;
    int total_value_size = 0;

    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '"') {
            OUT("{\"error\":\"fields array must contain strings\"}\n");
            return 1;
        }
        p++;
        const char *start = p;
        while (*p && *p != '"') p++;
        int flen = (int)(p - start);
        if (*p == '"') p++;

        if (nfields >= MAX_FIELDS) {
            OUT("{\"error\":\"too many fields (max %d)\"}\n", MAX_FIELDS);
            return 1;
        }
        if (flen <= 0 || flen >= MAX_FIELD_DEF) {
            OUT("{\"error\":\"invalid field definition (empty or longer than %d bytes)\"}\n",
                MAX_FIELD_DEF - 1);
            return 1;
        }
        field_specs[nfields] = malloc((size_t)flen + 1);
        if (!field_specs[nfields]) {
            OUT("{\"error\":\"out of memory\"}\n");
            return 1;
        }
        memcpy(field_specs[nfields], start, flen);
        field_specs[nfields][flen] = '\0';

        int field_size = validate_field_type(field_specs[nfields]);
        if (field_size <= 0) {
            OUT("{\"error\":\"invalid field type: \\\"%s\\\" — valid types: varchar:N, int, long, short, double, float, bool, byte, date, datetime, datetimems, time, timestamp, uuid, ipv4, ipv6, currency, numeric:P,S, enum(v1,v2,...)\"}\n",
                   field_specs[nfields]);
            return 1;
        }

        /* enum + :default=X — refuse at create-object if X isn't in the
           declared value list. The spec layout puts :default= AFTER the
           enum's closing paren, so the strstr lookup is unambiguous. */
        const char *spec = field_specs[nfields];
        const char *enum_open = strstr(spec, ":enum(");
````````

<!-- R1-BLOCK s2e56-new -->
````````c
        if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) {
            if (if_not_exists) {
                OUT("{\"status\":\"exists\",\"object\":\"%s\",\"dir\":\"%s\"}\n",
                    object, dir);
                return 0;
            }
            OUT("{\"error\":\"object already exists\",\"dir\":\"%s\",\"object\":\"%s\",\"hint\":\"pass \\\"if_not_exists\\\":true for idempotent create, or drop the object first\"}\n",
                dir, object);
            return 1;
        }
    }

    char *field_specs[MAX_FIELDS] __attribute__((cleanup(free_field_specs)))={0};
    size_t parsed_fields=0;
    if(typed_field_specs_parse(fields_json,field_specs,&parsed_fields)){
        OUT("{\"error\":\"invalid fields array, duplicate field or field definition longer than limit\"}\n");return 1;
    }
    int nfields=0,total_value_size=0;
    for(;nfields<(int)parsed_fields;nfields++){
        int field_size = validate_field_type(field_specs[nfields]);
        if (field_size <= 0) {
            OUT("{\"error\":\"invalid field type: \\\"%s\\\" — valid types: varchar:N, int, long, short, double, float, bool, byte, date, datetime, datetimems, time, timestamp, uuid, ipv4, ipv6, currency, numeric:P,S, enum(v1,v2,...)\"}\n",
                   field_specs[nfields]);
            return 1;
        }

        /* enum + :default=X — refuse at create-object if X isn't in the
           declared value list. The spec layout puts :default= AFTER the
           enum's closing paren, so the strstr lookup is unambiguous. */
        const char *spec = field_specs[nfields];
        const char *enum_open = strstr(spec, ":enum(");
````````

<!-- R1-BLOCK s2e57-old -->
````````text
                return 1;
            }
        }

        /* A :default= literal beyond default_val's 255-byte capacity would
           be silently truncated at parse/insert time — reject instead. */
        if (field_default_too_long(field_specs[nfields])) {
            OUT("{\"error\":\"field [%s]: :default= literal too long (max 255 bytes)\"}\n",
                field_specs[nfields]);
            return 1;
        }

        total_value_size += field_size;
        nfields++;
    }

    if (nfields == 0) {
        OUT("{\"error\":\"fields array is empty — at least one typed field required\"}\n");
        return 1;
    }

    /* Defaults + strict validation. As of 2026.05.1, splits must be a
       power of 2 in [8, 4096]. The per-shard index layout
       (index_splits_for(splits) — see types.h for the curve) relies on
       this regularity. */
    if (splits <= 0) splits = DEFAULT_SPLITS;
````````

<!-- R1-BLOCK s2e57-new -->
````````c
                return 1;
            }
        }

        /* A :default= literal beyond default_val's 255-byte capacity would
           be silently truncated at parse/insert time — reject instead. */
        if (field_default_too_long(field_specs[nfields])) {
            OUT("{\"error\":\"field [%s]: :default= literal too long (max 255 bytes)\"}\n",
                field_specs[nfields]);
            return 1;
        }

        if(field_size>INT_MAX-total_value_size){OUT("{\"error\":\"schema size overflow\"}\n");return 1;}
        total_value_size+=field_size;
    }

    if (nfields == 0) {
        OUT("{\"error\":\"fields array is empty — at least one typed field required\"}\n");
        return 1;
    }

    /* Defaults + strict validation. As of 2026.05.1, splits must be a
       power of 2 in [8, 4096]. The per-shard index layout
       (index_splits_for(splits) — see types.h for the curve) relies on
       this regularity. */
    if (splits <= 0) splits = DEFAULT_SPLITS;
````````

<!-- R1-BLOCK s2e58-old -->
````````text
            }                                                                 \
            _matched;                                                         \
        })
    #define FIELD_TYPE_PREFIX_IS(fname, fnlen, prefix)                        \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = _tok != NULL &&                                    \
                           strncmp(_tok, (prefix), strlen(prefix)) == 0;      \
            _matched;                                                         \
        })

    if (indexes_json && indexes_json[0]) {
        p = json_skip(indexes_json);
        if (*p == '[') {
            p++;
            while (*p) {
                p = json_skip(p);
                if (*p == ']') break;
                if (*p == ',') { p++; continue; }
                if (*p == '"') {
                    p++;
                    const char *istart = p;
                    while (*p && *p != '"') p++;
                    int ilen = (int)(p - istart);
                    if (*p == '"') p++;
````````

<!-- R1-BLOCK s2e58-new -->
````````c
            }                                                                 \
            _matched;                                                         \
        })
    #define FIELD_TYPE_PREFIX_IS(fname, fnlen, prefix)                        \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = _tok != NULL &&                                    \
                           strncmp(_tok, (prefix), strlen(prefix)) == 0;      \
            _matched;                                                         \
        })

    if (indexes_json && indexes_json[0]) {
        const char *p=json_skip(indexes_json);
        if (*p == '[') {
            p++;
            while (*p) {
                p = json_skip(p);
                if (*p == ']') break;
                if (*p == ',') { p++; continue; }
                if (*p == '"') {
                    p++;
                    const char *istart = p;
                    while (*p && *p != '"') p++;
                    int ilen = (int)(p - istart);
                    if (*p == '"') p++;
````````

<!-- R1-BLOCK s2e59-old -->
````````text
            const char *name_end = strchr(name_start, ':');
            if (!name_end || name_end == name_start) continue;
            OUT("%s\"%.*s\"", printed ? "," : "",
                (int)(name_end - name_start), name_start);
            printed++;
        }
        fclose(f);
    }
    OUT("]}\n");
    return 0;
}

static const char *field_type_str(enum FieldType t) {
    const TypeDescriptor *d = type_desc(t);
    return d ? d->name : "unknown";
}

/* Describe an object: schema (typed fields), indexes, splits, max_key, max_value,
   live record_count. Used by shard-cli to populate criteria builders + table
   headers without forcing the caller to read the on-disk fields.conf. */
int cmd_describe_object(const char *db_root, const char *dir, const char *object) {
    if (!dir || !dir[0])    { OUT("{\"error\":\"dir is required\"}\n"); return 1; }
    if (!object || !object[0]) { OUT("{\"error\":\"object is required\"}\n"); return 1; }

    char obj_root[PATH_MAX];
    snprintf(obj_root, sizeof(obj_root), "%s/%s/%s", db_root, dir, object);
    struct stat st;
    if (stat(obj_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
````````

<!-- R1-BLOCK s2e59-new -->
````````c
            const char *name_end = strchr(name_start, ':');
            if (!name_end || name_end == name_start) continue;
            OUT("%s\"%.*s\"", printed ? "," : "",
                (int)(name_end - name_start), name_start);
            printed++;
        }
        fclose(f);
    }
    OUT("]}\n");
    return 0;
}


/* Describe an object: schema (typed fields), indexes, splits, max_key, max_value,
   live record_count. Used by shard-cli to populate criteria builders + table
   headers without forcing the caller to read the on-disk fields.conf. */
int cmd_describe_object(const char *db_root, const char *dir, const char *object) {
    if (!dir || !dir[0])    { OUT("{\"error\":\"dir is required\"}\n"); return 1; }
    if (!object || !object[0]) { OUT("{\"error\":\"object is required\"}\n"); return 1; }

    char obj_root[PATH_MAX];
    snprintf(obj_root, sizeof(obj_root), "%s/%s/%s", db_root, dir, object);
    struct stat st;
    if (stat(obj_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
````````

<!-- R1-BLOCK s2e60-old -->
````````text

    /* load_schema / load_typed_schema take an `effective_root` (db_root/dir)
       and the bare object name as separate args. eff_obj kept around for
       get_live_count which uses the joined form. */
    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", db_root, dir);
    char eff_obj[PATH_MAX];
    snprintf(eff_obj, sizeof(eff_obj), "%s/%s", dir, object);

    Schema sch = load_schema(effective_root, object);
    TypedSchema *ts = load_typed_schema(effective_root, object);

    OUT("{\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"max_value\":%d,\"slot_size\":%d",
        dir, object, sch.splits, sch.max_key, sch.max_value, sch.slot_size);

    OUT(",\"fields\":[");
    if (ts && ts->nfields > 0) {
        int printed = 0;
        for (int i = 0; i < ts->nfields; i++) {
            const TypedField *f = &ts->fields[i];
            if (f->removed) continue;
            OUT("%s{\"name\":\"%s\",\"type\":\"%s\",\"size\":%d",
                printed ? "," : "", f->name, field_type_str(f->type), f->size);
            if (f->type == FT_NUMERIC)
                OUT(",\"scale\":%d", f->numeric_scale);
            switch (f->default_kind) {
                case DK_LITERAL:     OUT(",\"default\":\"%s\"", f->default_val); break;
                case DK_AUTO_CREATE: OUT(",\"default\":\"auto_create\""); break;
                case DK_AUTO_UPDATE: OUT(",\"default\":\"auto_update\""); break;
                case DK_SEQ:         OUT(",\"default\":\"seq(%s)\"", f->default_val); break;
                case DK_UUID:        OUT(",\"default\":\"uuid()\""); break;
                case DK_RANDOM:      OUT(",\"default\":\"random(%s)\"", f->default_val); break;
                case DK_NONE:        break;
            }
            if (f->not_null)
                OUT(",\"not_null\":true");
            OUT("}");
            printed++;
        }
    }
    OUT("]");

    /* Index list: read $obj_root/indexes/index.conf — one declared index
       per line. The .idx files only materialize after the first insert, so
       scanning them would miss empty objects. */
    OUT(",\"indexes\":[");
    char idx_conf[PATH_MAX];
    snprintf(idx_conf, sizeof(idx_conf), "%s/indexes/index.conf", obj_root);
    FILE *iconf = fopen(idx_conf, "r");
    if (iconf) {
        char line[256];
        int printed = 0;
        while (fgets(line, sizeof(line), iconf)) {
            char *end = line + strlen(line);
            while (end > line && (end[-1] == '\n' || end[-1] == '\r' || end[-1] == ' ')) *--end = '\0';
            if (line[0] == '\0' || line[0] == '#') continue;
            OUT("%s\"%s\"", printed ? "," : "", line);
            printed++;
        }
        fclose(iconf);
    }
    OUT("]");

    int rc_count = get_live_count(db_root, eff_obj);
    OUT(",\"record_count\":%d}\n", rc_count);
    return 0;
}
````````

<!-- R1-BLOCK s2e60-new -->
````````c

    /* load_schema / load_typed_schema take an `effective_root` (db_root/dir)
       and the bare object name as separate args. eff_obj kept around for
       get_live_count which uses the joined form. */
    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", db_root, dir);
    char eff_obj[PATH_MAX];
    snprintf(eff_obj, sizeof(eff_obj), "%s/%s", dir, object);

    Schema sch = load_schema(effective_root, object);
    TypedSchema *ts = load_typed_schema(effective_root, object);

    char *fields_json=typed_schema_fields_json(effective_root,object,ts,g_query_buffer_max_bytes);
    if(!fields_json){OUT("{\"error\":\"invalid or unreadable schema declaration\"}\n");return 1;}
    size_t used=strlen(fields_json)+1;
    char *indexes_json=used<g_query_buffer_max_bytes?
        typed_schema_indexes_json(effective_root,object,g_query_buffer_max_bytes-used):NULL;
    if(!indexes_json){free(fields_json);OUT("{\"error\":\"invalid or unreadable index declaration\"}\n");return 1;}
    OUT("{\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"max_value\":%d,\"slot_size\":%d,\"fields\":%s",
        dir,object,sch.splits,sch.max_key,sch.max_value,sch.slot_size,fields_json);
    free(fields_json);
    OUT(",\"streams\":%d,\"declaration_layout\":\"%s\"",sch.streams,
        ts&&ts->packed.nfields?"packed":"legacy");
    if(sch.auto_key!=AK_NONE){
        char automatic[300];
        if(sch.auto_key==AK_UUID)snprintf(automatic,sizeof(automatic),"uuid");
        else snprintf(automatic,sizeof(automatic),"seq(%s)",sch.auto_key_seq_name);
        OUT(",\"auto_key\":");
        if(schema_wire_quote(g_out?g_out:stdout,automatic,strlen(automatic))){free(indexes_json);return 1;}
    }

    OUT(",\"indexes\":%s",indexes_json);free(indexes_json);

    int rc_count = get_live_count(db_root, eff_obj);
    OUT(",\"record_count\":%d}\n", rc_count);
    return 0;
}
````````

<!-- R1-BLOCK s2e61-old -->
````````text
#include "schema_wire.h"
#include <errno.h>
int typed_field_specs_parse(const char*j,char *s[MAX_FIELDS],size_t*n){(void)j;(void)s;*n=0;errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s2e61-new -->
````````c
#include "schema_wire.h"
#include "typed_row.h"
#include "packed_record.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
int schema_wire_quote(FILE *out,const char *s,size_t n){
    static const char hex[]="0123456789abcdef";
    if(fputc('"',out)==EOF)return -1;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='"'||c=='\\'){if(fputc('\\',out)==EOF||fputc(c,out)==EOF)return -1;}
        else if(c<32){char escape[6]={'\\','u','0','0',hex[c>>4],hex[c&15]};if(fwrite(escape,1,6,out)!=6)return -1;}
        else if(fputc(c,out)==EOF)return -1;
    }
    return fputc('"',out)==EOF?-1:0;
}
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget){
    if(!root||!object||budget<3){errno=EINVAL;return NULL;}

    if(ts&&(ts->nfields<0||ts->nfields>MAX_FIELDS)){errno=EILSEQ;return NULL;}
    char path[PATH_MAX];int pn=snprintf(path,sizeof(path),"%s/%s/fields.conf",root,object);
    if(pn<0||(size_t)pn>=sizeof(path)){errno=ENAMETOOLONG;return NULL;}
    FILE *source=fopen(path,"r");
    if(!source){
        if(errno==ENOENT&&(!ts||!ts->nfields)){
            char *empty=malloc(3);if(empty)memcpy(empty,"[]",3);return empty;
        }
        return NULL;
    }
    FILE *out=tmpfile();if(!out){int saved=errno;fclose(source);errno=saved;return NULL;}
    int error=0,index=0,printed=0;char *result=NULL;
    if(fputc('[',out)==EOF){error=EIO;goto done;}
    for(;;){
        int failure=0;char *line=conf_read_line(source,MAX_FIELD_DEF,&failure);
        if(!line){if(failure||ferror(source))error=EIO;break;}
        if(!*line||*line=='#'){free(line);continue;}
        const char *colon=strchr(line,':');
        if(!ts||index>=ts->nfields||!colon){free(line);error=EILSEQ;break;}
        const TypedField *f=&ts->fields[index];size_t name=(size_t)(colon-line);
        if(name!=(size_t)f->name_len||memcmp(f->name,line,name)){free(line);error=EILSEQ;break;}
        size_t length=strlen(line);int removed=length>=8&&!strcmp(line+length-8,":removed");
        if(removed!=!!f->removed){free(line);error=EILSEQ;break;}
        if(f->removed){index++;free(line);continue;}
        char *clean=strdup(colon+1);if(!clean){free(line);error=ENOMEM;break;}
        TypedField modifiers={0};
        if(parse_default_modifier_checked(clean,&modifiers)<0||modifiers.default_kind!=f->default_kind||
           modifiers.not_null!=f->not_null||strcmp(modifiers.default_val,f->default_val)){
            free(clean);free(line);error=EILSEQ;break;}
        PkrField declaration={0};int has_decl=pkr_field_parse(clean,&declaration)==DEC_OK;
        if(ts->packed.nfields&&(f->type==FT_NUMERIC||f->type==FT_VARCHAR)){
            const PkrField *bound=&ts->packed.fields[index];
            if(!has_decl||bound->kind!=declaration.kind||bound->payload_max!=declaration.payload_max||
               bound->bare!=declaration.bare||bound->currency!=declaration.currency||
               bound->precision!=declaration.precision||bound->scale!=declaration.scale){
                free(clean);free(line);error=EILSEQ;break;}
        }
        if(printed&&fputc(',',out)==EOF)error=EIO;
        if(!error&&(fputs("{\"name\":",out)==EOF||schema_wire_quote(out,f->name,name)||
           fputs(",\"type\":",out)==EOF||schema_wire_quote(out,typed_type_name(f->type),strlen(typed_type_name(f->type)))||
           fprintf(out,",\"size\":%d,\"spec\":",f->size)<0||schema_wire_quote(out,line,length)))error=EIO;
        if(!error&&f->type==FT_NUMERIC){
            /* Legacy P was informational; export spec preserves original text.
             * Its one-argument syntax used scale 2, not packed scale 0. */
            if(fprintf(out,",\"scale\":%d",f->numeric_scale)<0)error=EIO;
            if(has_decl){
                if(fprintf(out,",\"precision\":%d,\"bare\":%s,\"currency\":%s",
                    declaration.precision,declaration.bare?"true":"false",declaration.currency?"true":"false")<0)error=EIO;
            }
        }
        if(!error&&f->type==FT_VARCHAR){
            uint32_t capacity=ts->packed.nfields?ts->packed.fields[index].payload_max:(uint32_t)(f->size-2);
            if(fprintf(out,",\"capacity\":%u,\"bare\":%s",capacity,
                ts->packed.nfields&&declaration.bare?"true":"false")<0)error=EIO;
        }
        if(!error&&f->default_kind!=DK_NONE){
            char default_text[300];const char *value=f->default_val;
            if(f->default_kind==DK_AUTO_CREATE)value="auto_create";
            else if(f->default_kind==DK_AUTO_UPDATE)value="auto_update";
            else if(f->default_kind==DK_UUID)value="uuid()";
            else if(f->default_kind==DK_SEQ||f->default_kind==DK_RANDOM){
                int n=snprintf(default_text,sizeof(default_text),"%s(%s)",f->default_kind==DK_SEQ?"seq":"random",f->default_val);
                if(n<0||(size_t)n>=sizeof(default_text))error=EOVERFLOW;
                value=default_text;
            }
            if(!error&&(fputs(",\"default\":",out)==EOF||schema_wire_quote(out,value,strlen(value))))error=EIO;
        }
        if(!error&&f->not_null&&fputs(",\"not_null\":true",out)==EOF)error=EIO;
        if(!error&&fputc('}',out)==EOF)error=EIO;
        free(clean);free(line);index++;printed++;
        off_t size=ftello(out);if(size<0)error=EIO;else if((uintmax_t)size>=budget)error=ENOBUFS;
        if(error)break;
    }
    if(!error&&ts&&index!=ts->nfields)error=EILSEQ;
    if(!error&&(fputc(']',out)==EOF||fflush(out)))error=EIO;
    if(!error){
        off_t length=ftello(out);
        if(length<0||(uintmax_t)length>=budget||(uintmax_t)length>SIZE_MAX-1)error=ENOBUFS;
        else{
            result=malloc((size_t)length+1);
            if(!result)error=ENOMEM;
            else if(fseeko(out,0,SEEK_SET)||fread(result,1,(size_t)length,out)!=(size_t)length)error=EIO;
            else result[length]=0;
        }
    }
done:
    if(fclose(source)&&!error)error=EIO;
    if(fclose(out)&&!error)error=EIO;
    if(error){free(result);errno=error;return NULL;}return result;
}

int typed_legacy_decl_size(char *buf) {

    /* Strip :removed */
    size_t blen = strlen(buf);
    if (blen >= 8 && strcmp(buf + blen - 8, ":removed") == 0) {
        buf[blen - 8] = '\0';
    }
    /* Strip a trailing :not_null / :nullable so the type matcher below
       sees the bare type spec, then reject any further occurrence —
       varchar/numeric/enum tolerate trailing bytes, so a buried token
       would otherwise be accepted and silently unenforced. */
    size_t nn_len = strlen(buf);
    if (nn_len >= 9 && (strcmp(buf + nn_len - 9, ":not_null") == 0 ||
                        strcmp(buf + nn_len - 9, ":nullable") == 0)) {
        buf[nn_len - 9] = '\0';
    }
    if (strstr(buf, ":not_null") || strstr(buf, ":nullable")) return 0;
    /* Strip default modifiers: :auto_create, :auto_update, :default=... */
    char *dm;
    if ((dm = strstr(buf, ":auto_update")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":auto_create")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":default=")) != NULL) *dm = '\0';

    const char *type = buf;
    if (strncmp(type, "varchar:", 8) == 0) {
        const char *p=type+8;if(*p<'0'||*p>'9')return 0;
        unsigned sz=0;
        for(;*p;p++){
            if(*p<'0'||*p>'9'||sz>(65535u-(unsigned)(*p-'0'))/10)return 0;
            sz=sz*10+(unsigned)(*p-'0');
        }
        if (!sz || sz > 65535) return 0;   /* bounded: 1..65535 content bytes */
        return sz + 2;                          /* 2-byte length prefix */
    }
    if (strcmp(type, "varchar") == 0) return 0; /* bare "varchar" — require :N */
    if (strcmp(type, "long") == 0)   return 8;
    if (strcmp(type, "int") == 0)    return 4;
    if (strcmp(type, "short") == 0)  return 2;
    if (strcmp(type, "double") == 0) return 8;
    if (strcmp(type, "float") == 0)  return 4;
    if (strcmp(type, "bool") == 0)   return 1;
    if (strcmp(type, "byte") == 0)   return 1;
    if (strcmp(type, "date") == 0)   return 4;
    if (strcmp(type, "datetime") == 0) return 7;
    if (strcmp(type, "datetimems") == 0) return 8;
    if (strcmp(type, "time") == 0)    return 3;
    if (strcmp(type, "timestamp") == 0) return 8;
    if (strcmp(type, "uuid") == 0)    return 16;
    if (strcmp(type, "ipv4") == 0)    return 4;
    if (strcmp(type, "ipv6") == 0)    return 16;
    if (strcmp(type, "currency") == 0) return 8;
    if (strncmp(type, "numeric:", 8) == 0) {
        /* Full-input P[,S] validation. P stays informational until the
           packed exact format lands; S must fit the int64 scaled storage:
           0..18 (10^18 is the largest power of ten in int64; S = 0 is
           valid). Legacy "numeric:N" without a comma keeps its
           historical scale-2 meaning. Junk suffixes are rejected, not
           atoi-prefixed. */
        const char *ps = type + 8;
        if (!(*ps >= '0' && *ps <= '9')) return 0;
        while (*ps >= '0' && *ps <= '9') ps++;
        if (*ps == ',') {
            ps++;
            if (!(*ps >= '0' && *ps <= '9')) return 0;
            while (*ps >= '0' && *ps <= '9') ps++;
        }
        if (*ps != '\0') return 0;
        const char *comma = strchr(type + 8, ',');
        long s = comma ? strtol(comma + 1, NULL, 10) : 2;
        if (s < 0 || s > 18) return 0;
        return 8;
    }
    if (strncmp(type, "enum(", 5) == 0) {
        /* Validate enum(v1,v2,...) — empty list, duplicates, missing
           close paren, and over-ceiling all → invalid (return 0).
           Returns the storage byte width: 1 for ≤256 values, 2 for
           257..65535. create-object is cold path, simple alloc is fine. */
        const char *open  = type + 5;
        const char *close = strrchr(open, ')');
        if (!close || close <= open || close[1]) return 0;

        /* Pass 1: count + duplicate-check via a heap-allocated value list.
           A worst-case 65535-entry list of avg ~8B = ~640 KB peak — fine. */
        char **vals = calloc(65536, sizeof(char *));
        if (!vals) return 0;
        int n = 0;
        const char *p = open;
        while (p < close) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl == 0) goto enum_invalid;            /* empty value */
            if (n >= 65535)            goto enum_invalid;
            char *v = malloc(vl + 1);
            if (!v) goto enum_invalid;
            memcpy(v, p, vl); v[vl] = '\0';
            for (int i = 0; i < n; i++) {
                if (strcmp(vals[i], v) == 0) {  /* duplicate */
                    free(v);
                    goto enum_invalid;
                }
            }
            vals[n++] = v;
            p = (next < close) ? next + 1 : close;
        }
        if (n == 0) goto enum_invalid;
        int width = (n <= 256) ? 1 : 2;
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return width;

enum_invalid:
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return 0;
    }
    return 0;
}

int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count){
    if(!specs||!count){errno=EINVAL;return -1;}
    memset(specs,0,MAX_FIELDS*sizeof(*specs));*count=0;
    if(!json||typed_json_validate(json)){errno=EINVAL;return -1;}
    const char *p=json_skip(json);if(*p++!='['){errno=EINVAL;return -1;}p=json_skip(p);
    while(*p!=']'){
        if(*p!='"'||*count==MAX_FIELDS){errno=E2BIG;goto fail;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        if(*end!='"'||raw>6u*(MAX_FIELD_DEF-1u)){errno=E2BIG;goto fail;}
        char *text=NULL;size_t length=0;errno=0;
        if(json_unescape_cstring(start,raw,&text,&length))goto fail;
        if(!length||length>=MAX_FIELD_DEF||memchr(text,'\n',length)||memchr(text,'\r',length)){
            free(text);errno=EINVAL;goto fail;}
        const char *colon=strchr(text,':');size_t name=colon?(size_t)(colon-text):0;
        if(!name||name>255){free(text);errno=EINVAL;goto fail;}
        for(size_t i=0;i<*count;i++){
            const char *c=strchr(specs[i],':');
            if((size_t)(c-specs[i])==name&&!memcmp(specs[i],text,name)){free(text);errno=EEXIST;goto fail;}
        }
        specs[(*count)++]=text;p=json_skip(end+1);if(*p==',')p=json_skip(p+1);
    }
    if(!*count){errno=EINVAL;goto fail;}return 0;
fail:{int saved=errno?errno:EINVAL;for(size_t i=0;i<*count;i++){free(specs[i]);specs[i]=NULL;}*count=0;errno=saved;return -1;}
}
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines){
    if(!lines||!nlines){errno=EINVAL;return -1;}*lines=NULL;*nlines=0;
    char *specs[MAX_FIELDS];size_t count=0;if(typed_field_specs_parse(json,specs,&count))return -1;
    size_t copied=0;for(size_t i=0;i<count;i++)copied+=strlen(specs[i])+1;
    int error=0;
    if(count>SIZE_MAX/MAX_FIELD_DEF||count*MAX_FIELD_DEF>g_query_buffer_max_bytes||
       copied>g_query_buffer_max_bytes-count*MAX_FIELD_DEF){error=ENOMEM;}
    char (*rows)[MAX_FIELD_DEF]=error?NULL:calloc(count,MAX_FIELD_DEF);
    if(!rows&&!error)error=ENOMEM;
    if(!error)for(size_t i=0;i<count;i++)memcpy(rows[i],specs[i],strlen(specs[i])+1);
    for(size_t i=0;i<count;i++)free(specs[i]);
    if(error){free(rows);errno=error;return -1;}*lines=rows;*nlines=(int)count;return 0;
}

char *typed_schema_indexes_json(const char *root,const char *object,size_t budget){
    char path[PATH_MAX];int n=snprintf(path,sizeof(path),"%s/%s/indexes/index.conf",root,object);
    if(n<0||(size_t)n>=sizeof(path)){errno=ENAMETOOLONG;return NULL;}
    FILE *source=fopen(path,"r");
    if(!source){if(errno!=ENOENT)return NULL;char *empty=malloc(3);if(empty)memcpy(empty,"[]",3);return empty;}
    FILE *out=tmpfile();if(!out){int e=errno;fclose(source);errno=e;return NULL;}
    int error=fputc('[',out)==EOF?EIO:0,printed=0;char *text=NULL;
    while(!error){
        int failure=0;char *line=conf_read_line(source,MAX_FIELD_DEF,&failure);
        if(!line){if(failure||ferror(source))error=EIO;break;}
        if(!*line||*line=='#'){free(line);continue;}
        if((printed&&fputc(',',out)==EOF)||schema_wire_quote(out,line,strlen(line)))error=EIO;
        free(line);printed++;
        off_t used=ftello(out);if(used<0)error=EIO;else if((uintmax_t)used>=budget)error=ENOBUFS;
    }
    if(!error&&(fputc(']',out)==EOF||fflush(out)))error=EIO;
    off_t size=error?-1:ftello(out);
    if(!error&&(size<0||(uintmax_t)size>=budget||(uintmax_t)size>SIZE_MAX-1))error=ENOBUFS;
    if(!error){
        text=malloc((size_t)size+1);if(!text)error=ENOMEM;
        else if(fseeko(out,0,SEEK_SET)||fread(text,1,(size_t)size,out)!=(size_t)size)error=EIO;
        else text[size]=0;
    }
    if(fclose(source)&&!error)error=EIO;
    if(fclose(out)&&!error)error=EIO;
    if(error){free(text);errno=error;return NULL;}return text;
}

int typed_default_validate(const TypedField *f){
    if(!f||f->type==FT_NONE||f->size<=0||f->default_invalid){errno=EINVAL;return -1;}
    if(f->default_kind==DK_RANDOM){
        size_t count=(size_t)strtoul(f->default_val,NULL,10);
        if(!count||count>2097152u||f->type!=FT_VARCHAR||count>(size_t)(f->size-2)/2){errno=EINVAL;return -1;}
        return 0;
    }
    if(f->default_kind!=DK_LITERAL)return 0;
    if(f->type==FT_ENUM){
        for(int i=0;i<f->n_enum_values;i++)if(!strcmp(f->default_val,f->enum_values[i]))return 0;
        errno=EINVAL;return -1;
    }
    if((size_t)f->size>g_query_buffer_max_bytes){errno=ENOBUFS;return -1;}
    uint8_t *bytes=malloc((size_t)f->size);if(!bytes)return -1;
    int rc=encode_field_checked(f,f->default_val,strlen(f->default_val),bytes,NULL,0);
    free(bytes);if(rc)errno=EINVAL;return rc;
}
````````

<!-- R1-BLOCK s2e62-old -->
````````text
#ifndef SHARD_SCHEMA_WIRE_H
#define SHARD_SCHEMA_WIRE_H
#include "types.h"
/* Caller pins the object schema under its object lock. The checked original
 * declaration is the export identity, not physical descriptor width. */
int schema_wire_quote(FILE *out,const char *text,size_t len);
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget);
int typed_legacy_decl_size(char *clean_type);
int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count);
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines);
char *typed_schema_indexes_json(const char *root,const char *object,size_t budget);
#endif
````````

<!-- R1-BLOCK s2e62-new -->
````````c
#ifndef SHARD_SCHEMA_WIRE_H
#define SHARD_SCHEMA_WIRE_H
#include "types.h"
/* Caller pins the object schema under its object lock. The checked original
 * declaration is the export identity, not physical descriptor width. */
int schema_wire_quote(FILE *out,const char *text,size_t len);
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget);
int typed_legacy_decl_size(char *clean_type);
int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count);
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines);
char *typed_schema_indexes_json(const char *root,const char *object,size_t budget);
int typed_default_validate(const TypedField *field);
#endif
````````

<!-- R1-BLOCK s2e63-old -->
````````text
#include "types.h"
#include "nql.h"
#ifndef EMBED_NO_TLS
#include "tls.h"
#else
/* OpenSSL-free forward declarations for embedded/npm builds (tls_stub.c
 * provides the no-op definitions; the TLS code paths are never entered
 * because g_tls_enable defaults to 0). */
struct ssl_ctx_st; typedef struct ssl_ctx_st SSL_CTX;
struct ssl_st;     typedef struct ssl_st     SSL;
extern SSL_CTX *g_tls_server_ctx;
extern SSL_CTX *g_tls_client_ctx;
````````

<!-- R1-BLOCK s2e63-new -->
````````c
#include "schema_wire.h"
#include "types.h"
#include "nql.h"
#ifndef EMBED_NO_TLS
#include "tls.h"
#else
/* OpenSSL-free forward declarations for embedded/npm builds (tls_stub.c
 * provides the no-op definitions; the TLS code paths are never entered
 * because g_tls_enable defaults to 0). */
struct ssl_ctx_st; typedef struct ssl_ctx_st SSL_CTX;
struct ssl_st;     typedef struct ssl_st     SSL;
extern SSL_CTX *g_tls_server_ctx;
extern SSL_CTX *g_tls_client_ctx;
````````

<!-- R1-BLOCK s2e64-old -->
````````text
    objlock_rdunlock(db_root, cmd.obj);

    nql_free_command(&cmd);
}

/* ========== JSON QUERY DISPATCH ========== */

/* Dispatch a JSON query object: {"mode":"get","object":"users","key":"k1",...} */
/* __attribute__((cleanup)) helper for MAX_FIELD_DEF spec-row arrays.
   GCC passes the cleanup the ADDRESS of the variable, so the parameter is
   a pointer to the row pointer and the heap block is *lines — free(that),
   never the variable itself. */
static void free_spec_lines(char (**lines)[MAX_FIELD_DEF]) { free(*lines); }

void dispatch_json_query(const char *raw_db_root, const char *json, const char *client_ip) {
    /* Concurrency cap: take a slot up-front; release on any return path
       via the cleanup attribute. Bounds peak query-buffer RAM at
       max_concurrent × QUERY_BUFFER_MB regardless of how many TCP
       threads accept connections at once. Try-acquire is non-blocking
       — clients get an immediate retry-please response rather than
       holding the TCP thread under load. */
    int slot_held __attribute__((cleanup(slot_cleanup))) = 0;
    if (slot_try_acquire() == 0) {
        slot_held = 1;
    } else {
````````

<!-- R1-BLOCK s2e64-new -->
````````c
    objlock_rdunlock(db_root, cmd.obj);

    nql_free_command(&cmd);
}

/* ========== JSON QUERY DISPATCH ========== */

/* Dispatch a JSON query object: {"mode":"get","object":"users","key":"k1",...} */
/* __attribute__((cleanup)) helper for MAX_FIELD_DEF spec-row arrays.
   GCC passes the cleanup the ADDRESS of the variable, so the parameter is
   a pointer to the row pointer and the heap block is *lines — free(that),
   never the variable itself. */


void dispatch_json_query(const char *raw_db_root, const char *json, const char *client_ip) {
    /* Concurrency cap: take a slot up-front; release on any return path
       via the cleanup attribute. Bounds peak query-buffer RAM at
       max_concurrent × QUERY_BUFFER_MB regardless of how many TCP
       threads accept connections at once. Try-acquire is non-blocking
       — clients get an immediate retry-please response rather than
       holding the TCP thread under load. */
    int slot_held __attribute__((cleanup(slot_cleanup))) = 0;
    if (slot_try_acquire() == 0) {
        slot_held = 1;
    } else {
````````

<!-- R1-BLOCK s2e65-old -->
````````text
    /* Object names are interpolated into filesystem paths below; reject any
       traversal ("/", "..") before create-object/drop-object/restore and every
       data path build their $DB_ROOT/<dir>/<object> paths. Without this a
       tenant token could pass object:"../other/obj" and escape its tenant. */
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
        free(mode); free(dir); free(object);
        return;
    }

    /* create-object bypasses dir validation (dir may not exist yet) */
    if (strcmp(mode, "create-object") == 0) {
        char *fields_j = json_obj_strdup_raw(&req, "fields");
        char *indexes_j = json_obj_strdup_raw(&req, "indexes");
        char *splits_s = json_obj_strdup(&req, "splits");
        char *max_key_s = json_obj_strdup(&req, "max_key");
        char *ine_s = json_obj_strdup(&req, "if_not_exists");
        char *sv_s = json_obj_strdup(&req, "storage_version");
        int if_not_exists = json_obj_is_true(&req, "if_not_exists") ||
                            (ine_s && strcmp(ine_s, "1") == 0);

        if (sv_s) {
            OUT("{\"error\":\"storage_version is not configurable; objects are always v2 (slotcask)\"}\n");
            free(fields_j); free(indexes_j);
````````

<!-- R1-BLOCK s2e65-new -->
````````c
    /* Object names are interpolated into filesystem paths below; reject any
       traversal ("/", "..") before create-object/drop-object/restore and every
       data path build their $DB_ROOT/<dir>/<object> paths. Without this a
       tenant token could pass object:"../other/obj" and escape its tenant. */
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
        free(mode); free(dir); free(object);
        return;
    }

    /* create-object bypasses dir validation (dir may not exist yet) */
    if (strcmp(mode, "create-object") == 0) {
        const char *stream_value=NULL,*manifest_layout=NULL;size_t stream_len=0,manifest_len=0;
        int marked_manifest=json_obj_get(&req,"declaration_layout",&manifest_layout,&manifest_len);
        if(marked_manifest&&json_obj_get(&req,"streams",&stream_value,&stream_len)){
            char expected[32];int n=snprintf(expected,sizeof(expected),"%d",slotcask_streams_for_nproc());
            if(n<1||stream_len!=(size_t)n||memcmp(stream_value,expected,stream_len)){
                OUT("{\"error\":\"imported stream count differs from this host; explicit geometry import awaits migration integration\"}\n");
                free(mode);free(dir);free(object);return;
            }
        }
        const char *layout_value=NULL;size_t layout_len=0;
        if(json_obj_get(&req,"declaration_layout",&layout_value,&layout_len)&&
           (layout_len!=8||memcmp(layout_value,"\"legacy\"",8))){
            OUT("{\"error\":\"packed or unknown schema layout import is inactive until migration integration\"}\n");
            free(mode);free(dir);free(object);return;
        }
        char *fields_j = json_obj_strdup_raw(&req, "fields");
        char *indexes_j = json_obj_strdup_raw(&req, "indexes");
        char *splits_s = json_obj_strdup(&req, "splits");
        char *max_key_s = json_obj_strdup(&req, "max_key");
        char *ine_s = json_obj_strdup(&req, "if_not_exists");
        char *sv_s = json_obj_strdup(&req, "storage_version");
        int if_not_exists = json_obj_is_true(&req, "if_not_exists") ||
                            (ine_s && strcmp(ine_s, "1") == 0);

        if (sv_s) {
            OUT("{\"error\":\"storage_version is not configurable; objects are always v2 (slotcask)\"}\n");
            free(fields_j); free(indexes_j);
````````

<!-- R1-BLOCK s2e66-old -->
````````text
           no flags means fast in-place tombstone reclaim. */
        char *splits_s  = json_obj_strdup(&req, "splits");
        int compact = json_obj_is_true(&req, "compact");
        int new_splits = splits_s ? atoi(splits_s) : 0;
        cmd_vacuum(db_root, object, compact, new_splits);
        free(splits_s);
    } else if (strcmp(mode, "rename-field") == 0) {
        char *oldn = json_obj_strdup(&req, "old");
        char *newn = json_obj_strdup(&req, "new");
        if (oldn && newn) cmd_rename_field(db_root, object, oldn, newn);
        else OUT("{\"error\":\"Missing 'old' or 'new' field name\"}\n");
        free(oldn); free(newn);
    } else if (strcmp(mode, "add-field") == 0) {
        /* fields is a JSON array of spec lines, e.g. ["phone:varchar:20","dob:date"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char (*lines)[MAX_FIELD_DEF] __attribute__((cleanup(free_spec_lines))) =
                calloc(MAX_FIELDS, MAX_FIELD_DEF);
            if (!lines) {
                OUT("{\"error\":\"out of memory\"}\n");
                free(fields_arr);
            } else {
                int nlines = 0;
                int too_long = 0;
                const char *p = fields_arr;
                while (*p && nlines < MAX_FIELDS) {
                    while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                    if (*p == ']' || *p == '\0') break;
                    if (*p == '"') {
                        p++;
                        const char *start = p;
                        while (*p && *p != '"') p++;
                        size_t l = p - start;
                        if (l > 0 && l < MAX_FIELD_DEF) {
                            memcpy(lines[nlines], start, l);
                            lines[nlines][l] = '\0';
                            nlines++;
                        } else if (l >= MAX_FIELD_DEF) {
                            too_long = 1;
                        }
                        if (*p == '"') p++;
                    } else p++;
                }
                if (too_long)
                    OUT("{\"error\":\"field definition longer than %d bytes\"}\n",
                        MAX_FIELD_DEF - 1);
                else if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
                else cmd_add_fields(db_root, object, lines, nlines);
                free(fields_arr);
            }
        }
    } else if (strcmp(mode, "edit-field") == 0) {
        /* fields is a JSON array of spec lines, e.g. ["name:varchar:200","age:long"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char (*lines)[MAX_FIELD_DEF] __attribute__((cleanup(free_spec_lines))) =
                calloc(MAX_FIELDS, MAX_FIELD_DEF);
            if (!lines) {
                OUT("{\"error\":\"out of memory\"}\n");
                free(fields_arr);
            } else {
                int nlines = 0;
                int too_long = 0;
                const char *p = fields_arr;
                while (*p && nlines < MAX_FIELDS) {
                    while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                    if (*p == ']' || *p == '\0') break;
                    if (*p == '"') {
                        p++;
                        const char *start = p;
                        while (*p && *p != '"') p++;
                        size_t l = p - start;
                        if (l > 0 && l < MAX_FIELD_DEF) {
                            memcpy(lines[nlines], start, l);
                            lines[nlines][l] = '\0';
                            nlines++;
                        } else if (l >= MAX_FIELD_DEF) {
                            too_long = 1;
                        }
                        if (*p == '"') p++;
                    } else p++;
                }
                /* Optional `allow_rename` flag for FT_ENUM rename edits.
                   Without it, any rename at an existing enum position is
                   rejected. */
                int allow_rename = json_obj_is_true(&req, "allow_rename");
                char *dry_s = json_obj_strdup(&req, "dry_run");
                int dry = json_obj_is_true(&req, "dry_run") ||
                          (dry_s && strcmp(dry_s, "1") == 0);
                free(dry_s);
                if (too_long)
                    OUT("{\"error\":\"field definition longer than %d bytes\"}\n",
                        MAX_FIELD_DEF - 1);
                else if (nlines == 0) OUT("{\"error\":\"No fields in 'fields' array\"}\n");
                else cmd_edit_fields(db_root, object, lines, nlines, allow_rename, dry);
                free(fields_arr);
            }
        }
    } else if (strcmp(mode, "remove-field") == 0) {
        /* fields is a JSON array of field names, e.g. ["email","age"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char names[MAX_FIELDS][256];
            int nnames = 0;
            const char *p = fields_arr;
            while (*p && nnames < MAX_FIELDS) {
                while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                if (*p == ']' || *p == '\0') break;
                if (*p == '"') {
````````

<!-- R1-BLOCK s2e66-new -->
````````c
           no flags means fast in-place tombstone reclaim. */
        char *splits_s  = json_obj_strdup(&req, "splits");
        int compact = json_obj_is_true(&req, "compact");
        int new_splits = splits_s ? atoi(splits_s) : 0;
        cmd_vacuum(db_root, object, compact, new_splits);
        free(splits_s);
    } else if (strcmp(mode, "rename-field") == 0) {
        char *oldn = json_obj_strdup(&req, "old");
        char *newn = json_obj_strdup(&req, "new");
        if (oldn && newn) cmd_rename_field(db_root, object, oldn, newn);
        else OUT("{\"error\":\"Missing 'old' or 'new' field name\"}\n");
        free(oldn); free(newn);
    } else if (!strcmp(mode,"add-field") || !strcmp(mode,"edit-field")) {
        char *fields_arr=json_obj_strdup_raw(&req,"fields");
        char (*lines)[MAX_FIELD_DEF]=NULL;int nlines=0;
        if(!fields_arr||typed_field_lines_parse(fields_arr,&lines,&nlines))
            OUT("{\"error\":\"Invalid field line, Duplicate field or field definition longer than limit\"}\n");
        else if(!strcmp(mode,"add-field"))cmd_add_fields(db_root,object,lines,nlines);
        else{
            int allow_rename=json_obj_is_true(&req,"allow_rename");
            char *dry_s=json_obj_strdup(&req,"dry_run");
            int dry=json_obj_is_true(&req,"dry_run")||(dry_s&&!strcmp(dry_s,"1"));
            free(dry_s);cmd_edit_fields(db_root,object,lines,nlines,allow_rename,dry);
        }
        free(lines);free(fields_arr);
    } else if (strcmp(mode, "remove-field") == 0) {
        /* fields is a JSON array of field names, e.g. ["email","age"] */
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        if (!fields_arr) { OUT("{\"error\":\"Missing 'fields' array\"}\n"); }
        else {
            char names[MAX_FIELDS][256];
            int nnames = 0;
            const char *p = fields_arr;
            while (*p && nnames < MAX_FIELDS) {
                while (*p == '[' || *p == ',' || *p == ' ' || *p == '\t') p++;
                if (*p == ']' || *p == '\0') break;
                if (*p == '"') {
````````

<!-- R1-BLOCK s2e67-old -->
````````text
        if (n == 0) {
            LOG_ERROR(LOG_SUB_SERVER, "write_all: write(fd=%d) returned 0 with %zu bytes remaining", fd, len - w);
            return -1;
        }
        w += (size_t)n;
    }
    return 0;
}

/* Connect to local server, send JSON line, return accumulated response (up to first \0).
   Caller frees *out. Returns 0 on success, -1 on error. */
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        LOG_WARN(LOG_SUB_SERVER, "query_collect: client_send_all failed for port=%d json_len=%zu", port, json_len);
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SERVER, "query_collect: malloc(%zu) failed", cap);
        client_close(&cc); return -1;
    }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) {
                        LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while framing response (len=%zu)", cap, len);
                        free(buf); client_close(&cc); return -1;
                    }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) {
                LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while accumulating response (len=%zu)", cap, len);
                free(buf); client_close(&cc); return -1;
            }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    *out = buf; *out_len = len;
    return 0;
}

/* ========== Schema export / import (CLI argv form) ==========
   Mirrors the TUI's Schema Transfer menu (src/cli/main.c::schema_transfer_export /
   schema_transfer_import) so the two are wire-compatible — manifests written by
   one can be imported by the other. Uses only the existing JSON modes
   db-dirs / list-objects / describe-object / create-object. */

/* Internal: walk a top-level JSON string array, invoking cb(elem, len, ctx)
   for each element. Returns 0 on success. */
static int walk_string_array(const char *s,
                             void (*cb)(const char *, size_t, void *),
                             void *ctx) {
    const char *p = json_skip(s);
    if (*p != '[') return -1;
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') return 0;
        if (*p == ',') { p++; continue; }
        if (*p != '"') return -1;
        p++;
        const char *start = p;
        while (*p && !(*p == '"' && p[-1] != '\\')) p++;
        if (*p != '"') return -1;
        cb(start, (size_t)(p - start), ctx);
        p++;
    }
    return -1;
}

/* Internal: walk a top-level JSON object array, invoking cb(elem_start,
   elem_len, ctx) for each {...} element. Element span includes the outer
   braces. Returns 0 on success. */
static int walk_object_array(const char *s,
                             void (*cb)(const char *, size_t, void *),
                             void *ctx) {
    const char *p = json_skip(s);
    if (*p != '[') return -1;
````````

<!-- R1-BLOCK s2e67-new -->
````````c
        if (n == 0) {
            LOG_ERROR(LOG_SUB_SERVER, "write_all: write(fd=%d) returned 0 with %zu bytes remaining", fd, len - w);
            return -1;
        }
        w += (size_t)n;
    }
    return 0;
}

/* Connect to local server, send JSON line, return accumulated response (up to first \0).
   Caller frees *out. Returns 0 on success, -1 on error. */
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    if(!out||!out_len)return -1;
    *out=NULL;*out_len=0;
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        LOG_WARN(LOG_SUB_SERVER, "query_collect: client_send_all failed for port=%d json_len=%zu", port, json_len);
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap+1);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SERVER, "query_collect: malloc(%zu) failed", cap);
        client_close(&cc); return -1;
    }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap+1);
                    if (!nb) {
                        LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while framing response (len=%zu)", cap, len);
                        free(buf); client_close(&cc); return -1;
                    }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                buf[len]=0;
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap+1);
            if (!nb) {
                LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while accumulating response (len=%zu)", cap, len);
                free(buf); client_close(&cc); return -1;
            }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    buf[len]=0;
    *out = buf; *out_len = len;
    return 0;
}

/* ========== Schema export / import (CLI argv form) ==========
   Mirrors the TUI's Schema Transfer menu (src/cli/main.c::schema_transfer_export /
   schema_transfer_import) so the two are wire-compatible — manifests written by
   one can be imported by the other. Uses only the existing JSON modes
   db-dirs / list-objects / describe-object / create-object. */

/* Internal: walk a top-level JSON string array, invoking cb(elem, len, ctx)
   for each element. Returns 0 on success. */
static int walk_string_array(const char *json,
    void (*callback)(const char*,size_t,void*),void *ctx){
    const char *p=json_skip(json);if(*p++!='[')return -1;p=json_skip(p);
    if(*p==']')return *json_skip(p+1)?-1:0;
    while(*p){
        if(*p!='"')return -1;
        const char *start=p+1,*end=json_raw_string_end(start);
        if(*end!='"')return -1;
        char *decoded=NULL;size_t length=0;
        if(json_unescape_cstring(start,(size_t)(end-start),&decoded,&length))return -1;
        callback(decoded,length,ctx);free(decoded);p=json_skip(end+1);
        if(*p==']')return *json_skip(p+1)?-1:0;
        if(*p++!=',')return -1;
        p=json_skip(p);
    }
    return -1;
}

/* Internal: walk a top-level JSON object array, invoking cb(elem_start,
   elem_len, ctx) for each {...} element. Element span includes the outer
   braces. Returns 0 on success. */
static int walk_object_array(const char *s,
                             void (*cb)(const char *, size_t, void *),
                             void *ctx) {
    const char *p = json_skip(s);
    if (*p != '[') return -1;
````````

<!-- R1-BLOCK s2e68-old -->
````````text
        if (*p == ',') { p++; continue; }
        if (*p != '{') return -1;
        const char *start = p;
        const char *eov = json_skip_value(p);
        cb(start, (size_t)(eov - start), ctx);
        p = eov;
    }
    return -1;
}

/* Compose a fields.conf-style spec ("name:type[:size|P,S]") from one
   describe-object field entry — same shape create-object expects. */
static void field_obj_to_spec(const char *obj, size_t len,
                              char *out, size_t out_sz) {
    JsonObj fo;
    if (json_parse_object(obj, len, &fo) <= 0) { out[0] = '\0'; return; }
    char name[64] = "", type[16] = "";
    json_obj_copy(&fo, "name", name, sizeof(name));
    json_obj_copy(&fo, "type", type, sizeof(type));
    int size  = json_obj_int(&fo, "size",  0);
    int scale = json_obj_int(&fo, "scale", 0);
    if (strcmp(type, "varchar") == 0) {
        /* describe-object reports on-disk size including the 2-byte length
           prefix; create-object expects content size, so subtract. */
        int n = size - 2;
        if (n < 0) n = 0;
        snprintf(out, out_sz, "%s:%s:%d", name, type, n);
    } else if (strcmp(type, "numeric") == 0) {
        snprintf(out, out_sz, "%s:%s:18,%d", name, type, scale);
    } else {
        snprintf(out, out_sz, "%s:%s", name, type);
    }
}

typedef struct {
    FILE *out;
    int   first_object;   /* tracks whether to emit a leading comma */
    int   total;          /* objects written */
} ExportCtx;

/* Per-field cb: append a JSON-quoted spec into the in-flight fields array. */
typedef struct {
    FILE *out;
    int   first;
} EmitFieldsCtx;

static void emit_one_field(const char *obj, size_t len, void *vctx) {
    EmitFieldsCtx *ec = (EmitFieldsCtx *)vctx;
    char spec[256];
    field_obj_to_spec(obj, len, spec, sizeof(spec));
    if (!spec[0]) return;
    fprintf(ec->out, "%s\"%s\"", ec->first ? "" : ",", spec);
    ec->first = 0;
}

static void emit_one_index(const char *str, size_t len, void *vctx) {
    EmitFieldsCtx *ec = (EmitFieldsCtx *)vctx;
    fprintf(ec->out, "%s\"%.*s\"", ec->first ? "" : ",", (int)len, str);
    ec->first = 0;
}

typedef struct {
    int  port;
    char dir[64];
    ExportCtx *exp;
} DescribeCtx;

/* Per-object cb invoked by walk_string_array on the list-objects response.
   Drives one describe-object query and emits a manifest entry. */
static void describe_one_object(const char *str, size_t len, void *vctx) {
    DescribeCtx *dc = (DescribeCtx *)vctx;
    if (len >= 64) return;
    char object[64];
    memcpy(object, str, len); object[len] = '\0';

    char req[256];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
        dc->dir, object);
    if (rl <= 0 || rl >= (int)sizeof(req)) return;

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(dc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: describe-object %s/%s failed; skipping\n",
                dc->dir, object);
        free(resp);
        return;
    }

    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: describe-object %s/%s returned malformed JSON\n",
                dc->dir, object);
        free(resp);
        return;
    }
    /* Bail if the response is an error envelope. */
    const char *errv; size_t errl;
    if (json_obj_get(&o, "error", &errv, &errl)) {
        fprintf(stderr, "warn: describe-object %s/%s: %.*s\n",
                dc->dir, object, (int)errl, errv);
        free(resp);
        return;
    }

    int splits  = json_obj_int(&o, "splits",  0);
    int max_key = json_obj_int(&o, "max_key", 0);

    if (!dc->exp->first_object) fprintf(dc->exp->out, ",\n");
    fprintf(dc->exp->out,
        "    {\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"fields\":[",
        dc->dir, object, splits, max_key);

    const char *fv; size_t fl;
    if (json_obj_get(&o, "fields", &fv, &fl)) {
        char *fields_buf = malloc(fl + 1);
        memcpy(fields_buf, fv, fl); fields_buf[fl] = '\0';
        EmitFieldsCtx ec = { dc->exp->out, 1 };
        walk_object_array(fields_buf, emit_one_field, &ec);
        free(fields_buf);
    }
    fprintf(dc->exp->out, "],\"indexes\":[");

    const char *iv; size_t il;
    if (json_obj_get(&o, "indexes", &iv, &il)) {
        char *idx_buf = malloc(il + 1);
        memcpy(idx_buf, iv, il); idx_buf[il] = '\0';
        EmitFieldsCtx ec = { dc->exp->out, 1 };
        walk_string_array(idx_buf, emit_one_index, &ec);
        free(idx_buf);
    }
    fprintf(dc->exp->out, "]}");

    dc->exp->first_object = 0;
    dc->exp->total++;
    free(resp);
}

typedef struct {
    int  port;
    ExportCtx *exp;
} TenantCtx;

/* Per-tenant cb invoked by walk_string_array on the db-dirs response.
   Lists objects under that tenant and describes each. */
static void describe_one_tenant(const char *str, size_t len, void *vctx) {
    TenantCtx *tc = (TenantCtx *)vctx;
    if (len >= 64) return;
    DescribeCtx dc;
    dc.port = tc->port;
    dc.exp  = tc->exp;
    memcpy(dc.dir, str, len); dc.dir[len] = '\0';

    char req[128];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dc.dir);
    if (rl <= 0 || rl >= (int)sizeof(req)) return;

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(tc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: list-objects %s failed; skipping tenant\n", dc.dir);
        free(resp);
        return;
    }
    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: list-objects %s returned malformed JSON\n", dc.dir);
        free(resp);
        return;
    }
    const char *ov; size_t ol;
    if (json_obj_get(&o, "objects", &ov, &ol)) {
        char *objs_buf = malloc(ol + 1);
        memcpy(objs_buf, ov, ol); objs_buf[ol] = '\0';
        walk_string_array(objs_buf, describe_one_object, &dc);
        free(objs_buf);
    }
    free(resp);
}

/* Manifest is JSON; contains schema and index definitions only — no data,
   no tokens. out_path may be NULL or "-" to write to stdout. */
int cmd_export_schema(int port, const char *out_path) {
    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(port, "{\"mode\":\"db-dirs\"}", 19, &resp, &resp_len) != 0
        || !resp) {
        fprintf(stderr, "Error: db-dirs query failed\n");
        free(resp);
        return 1;
    }

    FILE *out;
    int close_out = 0;
    if (!out_path || !out_path[0] || strcmp(out_path, "-") == 0) {
        out = stdout;
    } else {
        out = fopen(out_path, "w");
        if (!out) {
            fprintf(stderr, "Error: cannot open %s for writing: %s\n",
                    out_path, strerror(errno));
            free(resp);
            return 1;
        }
        close_out = 1;
    }

    fprintf(out, "{\n  \"dirs\": [");
    /* Re-use the same response twice: once to emit the dirs[] list, then
       walk it again to drive list-objects + describe-object. */
    {
        EmitFieldsCtx ec = { out, 1 };
        walk_string_array(resp, emit_one_index, &ec);
    }
    fprintf(out, "],\n  \"objects\": [\n");

    ExportCtx exp = { out, 1, 0 };
    TenantCtx tc = { port, &exp };
    walk_string_array(resp, describe_one_tenant, &tc);

    fprintf(out, "\n  ]\n}\n");
    if (close_out) fclose(out);
    free(resp);

    fprintf(stderr, "exported %d object%s%s%s\n",
            exp.total, exp.total == 1 ? "" : "s",
            close_out ? " to " : "",
            close_out ? out_path : "");
    return 0;
}

typedef struct {
    int  port;
    int  if_not_exists;
    int  created;
    int  skipped;
    int  failed;
} ImportCtx;

/* Per-object cb on the manifest's objects[] array. Wraps the entry into a
   create-object request and dispatches it. */
````````

<!-- R1-BLOCK s2e68-new -->
````````c
        if (*p == ',') { p++; continue; }
        if (*p != '{') return -1;
        const char *start = p;
        const char *eov = json_skip_value(p);
        cb(start, (size_t)(eov - start), ctx);
        p = eov;
    }
    return -1;
}

/* Compose a fields.conf-style spec ("name:type[:size|P,S]") from one
   describe-object field entry — same shape create-object expects. */
static char *field_obj_to_spec(const char *obj,size_t len){
    JsonObj field;const char *raw=NULL;size_t n=0;
    if(json_parse_object(obj,len,&field)<=0||!json_obj_get(&field,"spec",&raw,&n)||
       n<2||raw[0]!='"'||raw[n-1]!='"'){errno=EINVAL;return NULL;}
    char *spec=NULL;size_t length=0;
    if(json_unescape_cstring(raw+1,n-2,&spec,&length))return NULL;
    if(!length||length>=MAX_FIELD_DEF){free(spec);errno=E2BIG;return NULL;}
    return spec;
}

typedef struct {
    FILE *out;
    int   first_object;   /* tracks whether to emit a leading comma */
    int   total;          /* objects written */
    int   failed;
} ExportCtx;

/* Per-field cb: append a JSON-quoted spec into the in-flight fields array. */
typedef struct {
    FILE *out;
    int   first;
    int   failed;
} EmitFieldsCtx;

static void emit_one_field(const char *obj,size_t len,void *ptr){
    EmitFieldsCtx *c=ptr;if(c->failed)return;
    char *spec=field_obj_to_spec(obj,len);
    if(!spec){c->failed=1;return;}
    if((!c->first&&fputc(',',c->out)==EOF)||schema_wire_quote(c->out,spec,strlen(spec)))c->failed=1;
    free(spec);c->first=0;
}

static void emit_one_index(const char *text,size_t len,void *ptr){
    EmitFieldsCtx *c=ptr;if(c->failed)return;
    if((!c->first&&fputc(',',c->out)==EOF)||schema_wire_quote(c->out,text,len))c->failed=1;
    c->first=0;
}

typedef struct {
    int  port;
    char dir[256];
    ExportCtx *exp;
} DescribeCtx;

/* Per-object cb invoked by walk_string_array on the list-objects response.
   Drives one describe-object query and emits a manifest entry. */
static void describe_one_object(const char *str, size_t len, void *vctx) {
    DescribeCtx *dc = (DescribeCtx *)vctx;
    if(dc->exp->failed)return;
    if(len>=256){dc->exp->failed=1;return;}
    char object[256];
    memcpy(object, str, len); object[len] = '\0';

    char req[1024];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"describe-object\",\"dir\":\"%s\",\"object\":\"%s\"}",
        dc->dir, object);
    if(rl<=0||rl>=(int)sizeof(req)){dc->exp->failed=1;return;}

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(dc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: describe-object %s/%s failed; skipping\n",
                dc->dir, object);
        free(resp);
        dc->exp->failed=1;return;
    }

    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: describe-object %s/%s returned malformed JSON\n",
                dc->dir, object);
        free(resp);
        dc->exp->failed=1;return;
    }
    /* Bail if the response is an error envelope. */
    const char *errv; size_t errl;
    if (json_obj_get(&o, "error", &errv, &errl)) {
        fprintf(stderr, "warn: describe-object %s/%s: %.*s\n",
                dc->dir, object, (int)errl, errv);
        free(resp);
        dc->exp->failed=1;return;
    }

    int splits  = json_obj_int(&o, "splits",  0);
    int max_key = json_obj_int(&o, "max_key", 0);

    if (!dc->exp->first_object) fprintf(dc->exp->out, ",\n");
    fprintf(dc->exp->out,
        "    {\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"fields\":[",
        dc->dir, object, splits, max_key);

    const char *fv; size_t fl;
    if (json_obj_get(&o, "fields", &fv, &fl)) {
        char *fields_buf = malloc(fl + 1);
        if(!fields_buf){dc->exp->failed=1;free(resp);return;}
        memcpy(fields_buf, fv, fl); fields_buf[fl] = '\0';
        EmitFieldsCtx ec = { dc->exp->out, 1, 0 };
        if(walk_object_array(fields_buf,emit_one_field,&ec)||ec.failed)dc->exp->failed=1;
        free(fields_buf);
    }
    else dc->exp->failed=1;
    fprintf(dc->exp->out, "],\"indexes\":[");

    const char *iv; size_t il;
    if (json_obj_get(&o, "indexes", &iv, &il)) {
        char *idx_buf = malloc(il + 1);
        if(!idx_buf){dc->exp->failed=1;free(resp);return;}
        memcpy(idx_buf, iv, il); idx_buf[il] = '\0';
        EmitFieldsCtx ec = { dc->exp->out, 1, 0 };
        if(walk_string_array(idx_buf,emit_one_index,&ec)||ec.failed)dc->exp->failed=1;
        free(idx_buf);
    }
    else dc->exp->failed=1;
    fprintf(dc->exp->out,"]");
    const char *metadata[]={"streams","auto_key","declaration_layout"};
    for(size_t k=0;k<sizeof(metadata)/sizeof(metadata[0]);k++){
        const char *value=NULL;size_t n=0;
        if(json_obj_get(&o,metadata[k],&value,&n)){
            if(fprintf(dc->exp->out,",\"%s\":",metadata[k])<0||fwrite(value,1,n,dc->exp->out)!=n)dc->exp->failed=1;
        }
    }
    if(fputc('}',dc->exp->out)==EOF)dc->exp->failed=1;

    dc->exp->first_object = 0;
    dc->exp->total++;
    free(resp);
}

typedef struct {
    int  port;
    ExportCtx *exp;
} TenantCtx;

/* Per-tenant cb invoked by walk_string_array on the db-dirs response.
   Lists objects under that tenant and describes each. */
static void describe_one_tenant(const char *str, size_t len, void *vctx) {
    TenantCtx *tc = (TenantCtx *)vctx;
    if(tc->exp->failed)return;
    if(len>=256){tc->exp->failed=1;return;}
    DescribeCtx dc;
    dc.port = tc->port;
    dc.exp  = tc->exp;
    memcpy(dc.dir, str, len); dc.dir[len] = '\0';

    char req[512];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dc.dir);
    if(rl<=0||rl>=(int)sizeof(req)){tc->exp->failed=1;return;}

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(tc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: list-objects %s failed; skipping tenant\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: list-objects %s returned malformed JSON\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    const char *ov; size_t ol;
    const char *error_value=NULL;size_t error_length=0;
    if(json_obj_get(&o,"error",&error_value,&error_length)){
        tc->exp->failed=1;free(resp);return;
    }
    if (json_obj_get(&o, "objects", &ov, &ol)) {
        char *objs_buf = malloc(ol + 1);
        if(!objs_buf){tc->exp->failed=1;free(resp);return;}
        memcpy(objs_buf, ov, ol); objs_buf[ol] = '\0';
        if(walk_string_array(objs_buf,describe_one_object,&dc))tc->exp->failed=1;
        free(objs_buf);
    }else tc->exp->failed=1;
    free(resp);
}

/* Manifest is JSON; contains schema and index definitions only — no data,
   no tokens. out_path may be NULL or "-" to write to stdout. */
int cmd_export_schema(int port,const char *path){
    char *dirs=NULL;size_t length=0;int error=0;
    if(query_collect(port,"{\"mode\":\"db-dirs\"}",19,&dirs,&length)||!dirs){free(dirs);return 1;}
    FILE *stage=tmpfile();if(!stage){free(dirs);return 1;}
    fputs("{\n  \"dirs\": [",stage);
    EmitFieldsCtx names={stage,1,0};
    if(walk_string_array(dirs,emit_one_index,&names)||names.failed)error=1;
    fputs("],\n  \"objects\": [\n",stage);
    ExportCtx export={stage,1,0,0};TenantCtx tenants={port,&export};
    if(!error&&walk_string_array(dirs,describe_one_tenant,&tenants))error=1;
    free(dirs);fputs("\n  ]\n}\n",stage);
    if(export.failed||ferror(stage)||fflush(stage)||fseeko(stage,0,SEEK_SET))error=1;
    FILE *out=stdout;char temporary[PATH_MAX]={0};int named=path&&*path&&strcmp(path,"-");
    if(!error&&named){
        int n=snprintf(temporary,sizeof(temporary),"%s.tmp.XXXXXX",path);
        if(n<0||(size_t)n>=sizeof(temporary))error=1;
        else{
            int fd=mkstemp(temporary);
            if(fd<0)error=1;
            else{out=fdopen(fd,"w");if(!out){close(fd);error=1;}}
        }
    }
    if(!error){
        char block[8192];size_t n;
        while((n=fread(block,1,sizeof(block),stage)))if(fwrite(block,1,n,out)!=n){error=1;break;}
        if(ferror(stage)||ferror(out)||fflush(out))error=1;
        if(named&&fsync(fileno(out)))error=1;
    }
    if(named&&out&&out!=stdout&&fclose(out))error=1;
    if(fclose(stage))error=1;
    if(named&&temporary[0]){if(!error){if(rename(temporary,path))error=1;}if(error)unlink(temporary);}
    if(error){fprintf(stderr,"Error: schema export failed; no complete manifest published\n");return 1;}
    fprintf(stderr,"exported %d object%s\n",export.total,export.total==1?"":"s");return 0;
}

typedef struct {
    int  port;
    int  if_not_exists;
    int  created;
    int  skipped;
    int  failed;
} ImportCtx;

/* Per-object cb on the manifest's objects[] array. Wraps the entry into a
   create-object request and dispatches it. */
````````

<!-- R1-BLOCK s2e69-old -->
````````text
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0)
            return -1;
        int rc = 0;
        if (kh.hdr && kh.hdr->deleted > 0)
            rc = kfcache_resplit_locked(&kh, kh.capacity);
        kfcache_release(&kh);
        if (rc != 0) return -1;
    }
    return 0;
}
````````

<!-- R1-BLOCK s2e69-new -->
````````c
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0)
            return -1;
        int rc = 0;
        if (kh.hdr && kh.hdr->deleted > 0)
            rc = kfcache_resplit_locked(&kh, kh.capacity);
        kfcache_release(&kh);
        if (rc != 0) return -1;
    }
    return 0;
}


/* Ordinary checked reads use exactly the existing keyfile -> segment cache
 * read-handle order. The caller already holds the matching object read lock;
 * no writer gate/reservation is acquired and no global snapshot is promised. */
static int slotcask_emit_checked(SlotcaskDb *db,const SlotcaskKfEntry *entry,
    SlotcaskScanCb callback,void *ctx){
    if(entry->stream_id>=db->num_streams){errno=EUCLEAN;return -1;}
    char path[PATH_MAX];seg_path_for(path,db->data_dir,entry->stream_id,entry->file_id);
    SlotcaskSegHandle segment={.slot=-1,.fd=-1};
    if(segcache_acquire(&segment,path,0,0,0))return -1;
    int rc=0;
    if(entry->offset>segment.map_size||segment.map_size-entry->offset<24){errno=EUCLEAN;rc=-1;}
    else{
        const uint8_t *record=segment.map+entry->offset;
        uint16_t keylen=seg_rec_klen(record);uint32_t value_len=seg_rec_vlen(record);
        size_t encoded=24u+(size_t)keylen+(size_t)value_len;
        size_t padded=slotcask_record_size_varlen(keylen,value_len);
        if(encoded>(size_t)db->slot_size||padded>(size_t)db->slot_size||
           padded>segment.map_size-entry->offset||!seg_rec_live_with_hash(record,entry->hash)){
            errno=EUCLEAN;rc=-1;
        }else if(callback(entry->hash,record+24,keylen,record+24+keylen,value_len,record[19],ctx))rc=1;
    }
    int saved=errno;segcache_release(&segment);errno=saved;return rc;
}
int slotcask_walk_live_checked(SlotcaskDb *db,SlotcaskScanCb callback,void *ctx){
    if(!db||!callback||db->num_shards<1){errno=EINVAL;return -1;}
    for(int shard=0;shard<db->num_shards;shard++){
        char path[PATH_MAX];kf_path_for(path,db->data_dir,shard);
        SlotcaskKfHandle keyfile;
        if(kfcache_acquire(&keyfile,path,db->slots_per_shard,0))return -1;
        int rc=0;
        for(size_t slot=0;slot<keyfile.capacity;slot++){
            const SlotcaskKfEntry *entry=&keyfile.map[slot];
            uint8_t flag=__atomic_load_n(&entry->flag,__ATOMIC_ACQUIRE);
            if(!flag||flag==2)continue;
            if(flag!=1){errno=EUCLEAN;rc=-1;break;}
            rc=slotcask_emit_checked(db,entry,callback,ctx);if(rc)break;
        }
        int saved=errno;kfcache_release(&keyfile);errno=saved;if(rc)return rc;
    }
    return 0;
}
int slotcask_lookup_by_hash_checked(SlotcaskDb *db,const uint8_t hash[16],
    SlotcaskScanCb callback,void *ctx){
    if(!db||!hash||!callback||db->num_shards<1){errno=EINVAL;return -1;}
    int shard=shard_for_hash(hash,db->num_shards);char path[PATH_MAX];
    kf_path_for(path,db->data_dir,shard);SlotcaskKfHandle keyfile;
    SlotRef *ref=db->kf_slot_refs?&db->kf_slot_refs[shard]:NULL;
    if(kfcache_acquire_direct(&keyfile,ref,path,db->slots_per_shard,db,shard))return -1;
    int rc=0;size_t start=kf_slot_for(hash,keyfile.capacity);
    for(size_t i=0;i<keyfile.capacity;i++){
        const SlotcaskKfEntry *entry=&keyfile.map[(start+i)%keyfile.capacity];
        uint8_t flag=__atomic_load_n(&entry->flag,__ATOMIC_ACQUIRE);
        if(!flag)break;
        if(flag==2)continue;
        if(flag!=1){errno=EUCLEAN;rc=-1;break;}
        if(memcmp(entry->hash,hash,16))continue;
        rc=slotcask_emit_checked(db,entry,callback,ctx);if(rc)break;
    }
    int saved=errno;kfcache_release(&keyfile);errno=saved;return rc;
}

/* Checked probes distinguish a valid missing/tombstoned key from an
 * invalid flag anywhere in the traversed probe chain. The caller owns the
 * existing keyfile reader for the whole probe. */
static int kf_lookup_no_verify_checked(SlotcaskKfHandle *kh,
    const uint8_t hash[16],uint8_t *flag_out,uint8_t *stream_id_out,
    uint16_t *file_id_out,uint32_t *offset_out,size_t *slot_out){
    size_t start=kf_slot_for(hash,kh->capacity);
    for(size_t i=0;i<kh->capacity;i++){
        size_t slot=(start+i)%kh->capacity;
        const SlotcaskKfEntry *entry=&kh->map[slot];
        uint8_t flag=__atomic_load_n(&entry->flag,__ATOMIC_ACQUIRE);
        if(!flag)return -1;
        if(flag!=1&&flag!=2){errno=EUCLEAN;return -2;}
        if(memcmp(entry->hash,hash,16))continue;
        if(flag==2)return -1;
        *flag_out=flag;*stream_id_out=entry->stream_id;
        *file_id_out=entry->file_id;*offset_out=entry->offset;
        *slot_out=slot;return 0;
    }
    return -1;
}

/* Checked bulk readers preserve the existing bucket/resolve/revalidation and
 * dispatch patterns. Error status and bounds are explicit; no lock/gate or
 * successful read-handle lifetime changes. Existing public implementations
 * above remain byte-identical for consumers owned by later integrations. */
int slotcask_bulk_lookup_in_kfshard_checked(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n) {
    if(n==0)return 0;
    if(!db||!recs||n>INT_MAX||kf_shard_id<0||kf_shard_id>=db->num_shards){errno=EINVAL;return -1;}
    int failed=0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[kf_shard_id] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, kf_shard_id) != 0) return -1;

    SlotcaskBulkLookupState *st = calloc(n, sizeof(SlotcaskBulkLookupState));
    if (!st) { kfcache_release(&kh); return -1; }

    /* Phase 1: probe-only lookup under one rdlock. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX) { r->status = -1; continue; }
        compute_hash(r->key, r->klen, st[i].hash);
        uint8_t flag = 0;
        size_t slot;
        int rc = kf_lookup_no_verify_checked(&kh, st[i].hash, &flag,
                                      &st[i].sid, &st[i].fid, &st[i].off, &slot);
        if(rc==-2){r->status=-1;failed=EUCLEAN;continue;}
        if (rc < 0) { r->status = -2; continue; }
        st[i].kf_found = 1;
    }
    /* The kf reader stays held through phase 2: the window contract
       ("Kf read handle stays live until the segment record has been
       checked against its hash/key and copied into caller-owned
       memory") requires it. Released here, a window could tombstone
       (T) and re-emit (P) this slot while phase 2 reads it under an
       independent segcache rdlock — plain-vs-plain, zero common lock.
       Same discipline as slotcask_get and kf_reval_fetch_one. */
#ifdef TEST_BUILD
    /* Regression hook (docs/plans/2026-08-28-eliminate-tsan-supp.md
       Task B1): parks the caller after the probe phase so a test can
       run a full window's worth of slot churn in the gap. Post-fix the
       kf reader is still held here, so any window T step in the gap
       blocks — which is exactly the assertion the test makes. */
    if (atomic_load(&g_shard_test_bulk_lookup_gap) &&
        atomic_fetch_add(&g_shard_test_bulk_lookup_gap_hit, 1) == 0) {
        while (!atomic_load(&g_shard_test_bulk_lookup_gap_release))
            nanosleep(&(struct timespec){0, 1000000L}, NULL);
    }
#endif

    /* Phase 2: batched verify_stored_key — sort kf-hits by (sid, fid),
       take segcache rdlock once per unique file, verify each record
       under the held handle. Records that fail verify (hash collision
       on a different stored key) get status=-2. */
    int *vidx = malloc(n * sizeof(int));
    if (vidx) {
        int vcount = 0;
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status == 0 && st[i].kf_found) vidx[vcount++] = (int)i;
        }
        for (int a = 1; a < vcount; a++) {
            int tmp = vidx[a];
            uint8_t  ta_sid = st[tmp].sid; uint16_t ta_fid = st[tmp].fid;
            int b = a - 1;
            while (b >= 0) {
                int bi = vidx[b];
                if (st[bi].sid < ta_sid ||
                    (st[bi].sid == ta_sid && st[bi].fid <= ta_fid)) break;
                vidx[b + 1] = vidx[b];
                b--;
            }
            vidx[b + 1] = tmp;
        }
        int k = 0;
        while (k < vcount) {
            int run_end = k + 1;
            uint8_t  sid = st[vidx[k]].sid;
            uint16_t fid = st[vidx[k]].fid;
            while (run_end < vcount &&
                   st[vidx[run_end]].sid == sid &&
                   st[vidx[run_end]].fid == fid)
                run_end++;
            char path[PATH_MAX];
            if(sid>=db->num_streams){failed=EILSEQ;for(int j=k;j<run_end;j++)recs[vidx[j]].status=-1;k=run_end;continue;}
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            SlotRef *seg_ref = seg_ref_for(db, (int)sid, (uint32_t)fid);
            if (segcache_acquire_direct(&h, seg_ref, path) != 0) {
                failed=errno?errno:EIO;
                for (int j = k; j < run_end; j++) recs[vidx[j]].status = -1;
                k = run_end;
                continue;
            }
            for (int j = k; j < run_end; j++) {
                int i = vidx[j];
                SlotcaskBulkRec *r = &recs[i];
                if(st[i].sid>=db->num_streams||st[i].off>h.map_size||h.map_size-st[i].off<24){r->status=-1;failed=EILSEQ;continue;}
                const uint8_t *rec = h.map + st[i].off;
                size_t physical=slotcask_record_size_varlen(seg_rec_klen(rec),seg_rec_vlen(rec));
                if(physical>(size_t)db->slot_size||physical>h.map_size-st[i].off||!seg_rec_live_with_hash(rec,st[i].hash)){
                    r->status=-1;failed=EILSEQ;continue;
                }
                if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -2; continue; }
                uint16_t k_stored = seg_rec_klen(rec);
                if (k_stored != r->klen) { r->status = -2; continue; }
                if (memcmp(rec + 24, r->key, r->klen) != 0) { r->status = -2; continue; }
                st[i].verified = 1;
            }
            segcache_release(&h);
            k = run_end;
        }
        free(vidx);
    } else {
        failed=ENOMEM;
    }

    kfcache_release(&kh);
    free(st);
    if(failed){errno=failed;return -1;}return 0;
}
static SlotcaskResolvedRec *slotcask_bulk_resolve_hashes_checked(SlotcaskDb *db,
                                                    const uint8_t (*hashes)[16],
                                                    size_t n,
                                                    size_t *out_n) {
    errno=0;
    if (n == 0 || !out_n) { if (out_n) *out_n = 0; return NULL; }

    SlotcaskResolvedRec *resolved = calloc(n, sizeof(SlotcaskResolvedRec));
    if (!resolved) { *out_n = 0; return NULL; }

    size_t resolved_n = 0;int failure=0;
    int nshards = db->num_shards;

    /* Pass 1: count hashes per KF shard */
    size_t *per_shard_count = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_count) { free(resolved); *out_n = 0; return NULL; }

    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_count[sid]++;
    }

    /* Pass 2: allocate per-shard index arrays */
    size_t **per_shard_idxs = calloc((size_t)nshards, sizeof(size_t *));
    size_t *per_shard_pos   = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_idxs || !per_shard_pos) {
        free(per_shard_count);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    int alloc_ok = 1;
    for (int s = 0; s < nshards; s++) {
        if (per_shard_count[s] > 0) {
            per_shard_idxs[s] = malloc(per_shard_count[s] * sizeof(size_t));
            if (!per_shard_idxs[s]) { alloc_ok = 0; break; }
        }
    }
    if (!alloc_ok) {
        for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(per_shard_count);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    /* Pass 3: fill per-shard index arrays */
    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_idxs[sid][per_shard_pos[sid]++] = i;
    }

    /* Pass 4: for each non-empty shard, acquire KF, probe all its hashes */
    char kf_path[PATH_MAX];
    for (int s = 0; s < nshards; s++) {
        size_t cnt = per_shard_count[s];
        if (cnt == 0) continue;

        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0){failure=errno?errno:EIO;break;}

        for (size_t j = 0; j < cnt; j++) {
            size_t hash_idx = per_shard_idxs[s][j];
            uint8_t flag = 0;
            uint8_t sid;
            uint16_t fid;
            uint32_t off;
            size_t slot;
            int rc = kf_lookup_no_verify_checked(&kh, hashes[hash_idx], &flag,
                                          &sid, &fid, &off, &slot);
            if(rc==-2){failure=EUCLEAN;break;}
            if (rc == 0) {
                SlotcaskResolvedRec *rr = &resolved[resolved_n++];
                memcpy(rr->hash, hashes[hash_idx], 16);
                rr->sid = sid;
                rr->fid = fid;
                rr->off = off;
            }
        }
        kfcache_release(&kh);
        if(failure)break;
    }

    /* Cleanup per-shard structures */
    for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
    free(per_shard_idxs);
    free(per_shard_pos);
    free(per_shard_count);

    if(failure){free(resolved);*out_n=0;errno=failure;return NULL;}
    *out_n = resolved_n;
    if (resolved_n == 0) { free(resolved); return NULL; }
    return resolved;
}
typedef struct {
    SlotcaskDb *db;SlotcaskResolvedRec *recs;size_t start,count;int kf_shard;
    SlotcaskScanCb cb;void *ctx;int error;
} CheckedRevalFetchArg;
static void checked_reval_fetch_one(CheckedRevalFetchArg *fa) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        fa->error=errno?errno:EIO;
        /* Whole partition unreadable — retire every record in it. */
        for (size_t i = 0; i < fa->count; i++)
            fa->recs[fa->start + i].sid = 0xFF;
        return;
    }

    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        uint8_t flag = 0, sid = 0;
        uint16_t fid = 0;
        uint32_t off = 0;
        size_t slot = 0;
        int rc=kf_lookup_no_verify_checked(&kh,r->hash,&flag,&sid,&fid,&off,&slot);
        if(rc==-2){fa->error=EUCLEAN;break;}
        if (rc != 0 || flag != 1 || sid != r->sid || fid != r->fid || off != r->off)
            r->sid = 0xFF;  /* repointed or gone since resolve */
    }
    if(fa->error){kfcache_release(&kh);return;}

    /* Compact survivors within the slice (disjoint per partition). */
    size_t live_n = 0;
    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        if (r->sid != 0xFF) fa->recs[fa->start + live_n++] = *r;
    }

    if (live_n > 0) {
        qsort(&fa->recs[fa->start], live_n, sizeof(SlotcaskResolvedRec),
              compare_sid_fid_off);
        /* Copy every survivor's bytes under the STILL-HELD reader. */
        size_t run_start = 0;
        for (size_t i = 0; i < live_n; i++) {
            int last = (i == live_n - 1);
            if (!last &&
                fa->recs[fa->start + i].sid ==
                    fa->recs[fa->start + i + 1].sid &&
                fa->recs[fa->start + i].fid ==
                    fa->recs[fa->start + i + 1].fid)
                continue;
            char seg_path[PATH_MAX];
            SlotcaskSegHandle h;
            if(fa->recs[fa->start+run_start].sid>=fa->db->num_streams){fa->error=EILSEQ;break;}
            seg_path_for(seg_path, fa->db->data_dir,
                         fa->recs[fa->start + run_start].sid,
                         fa->recs[fa->start + run_start].fid);
            if (segcache_acquire(&h, seg_path, 0, 0, 0) == 0) {
                for (size_t j = run_start; j <= i; j++) {
                    const SlotcaskResolvedRec *r =
                        &fa->recs[fa->start + j];
                    if(r->sid>=fa->db->num_streams||r->off>h.map_size||h.map_size-r->off<24){fa->error=EILSEQ;break;}
                    const uint8_t *rec = h.map + r->off;
                    size_t physical=slotcask_record_size_varlen(seg_rec_klen(rec),seg_rec_vlen(rec));
                    if(physical>(size_t)fa->db->slot_size||physical>h.map_size-r->off||!seg_rec_live_with_hash(rec,r->hash)){fa->error=EILSEQ;break;}
                    uint16_t klen = seg_rec_klen(rec);
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, rec[19], fa->ctx) != 0)
                        break;
                }
                segcache_release(&h);
            }else fa->error=errno?errno:EIO;
            if(fa->error)break;
            run_start = i + 1;
        }
    }

    kfcache_release(&kh);
}
static void *checked_reval_fetch_worker(void *arg){checked_reval_fetch_one(arg);return NULL;}
int slotcask_bulk_fetch_resolved_checked(SlotcaskDb *db,
                                  SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  SlotcaskScanCb cb) {
    if (n == 0 || !cb) return 0;

    /* Partition in place by KF shard: single pass with an index-keyed
       bucket layout (avoids a comparator that needs db->num_shards). */
    int nshards = db->num_shards;
    size_t *part_start = calloc((size_t)nshards, sizeof(size_t));
    size_t *part_count = calloc((size_t)nshards, sizeof(size_t));
    if (!part_start || !part_count) {
        free(part_start); free(part_count); return -1;
    }
    for (size_t i = 0; i < n; i++)
        part_count[shard_for_hash(recs[i].hash, nshards)]++;
    size_t acc = 0;
    for (int s = 0; s < nshards; s++) {
        part_start[s] = acc;
        acc += part_count[s];
    }
    SlotcaskResolvedRec *sorted = malloc(n * sizeof(SlotcaskResolvedRec));
    size_t *fill = calloc((size_t)nshards, sizeof(size_t));
    if (!sorted || !fill) {
        free(part_start); free(part_count); free(fill); free(sorted);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        int s = shard_for_hash(recs[i].hash, nshards);
        sorted[part_start[s] + fill[s]++] = recs[i];
    }
    free(fill);

    /* Dispatch one reval+fetch pipeline per non-empty partition. */
    int nparts = 0;
    for (int s = 0; s < nshards; s++)
        if (part_count[s] > 0) nparts++;
    if (nparts > 0) {
        CheckedRevalFetchArg *fargs = calloc((size_t)nparts, sizeof(CheckedRevalFetchArg));
        if (!fargs) {
            free(part_start); free(part_count); free(sorted);
            return -1;
        }
        int fi = 0;
        for (int s = 0; s < nshards; s++) {
            if (part_count[s] == 0) continue;
            fargs[fi].db = db;
            fargs[fi].recs = sorted;
            fargs[fi].start = part_start[s];
            fargs[fi].count = part_count[s];
            fargs[fi].kf_shard = s;
            fargs[fi].cb = cb;
            fargs[fi].ctx = ctx;
            fi++;
        }
        if (nparts <= 3) {
            for (int i = 0; i < nparts; i++) checked_reval_fetch_one(&fargs[i]);
        } else {
            parallel_for_io(checked_reval_fetch_worker, fargs, nparts,
                            sizeof(CheckedRevalFetchArg));
        }
        int failure=0;for(int i=0;i<nparts;i++)if(fargs[i].error){failure=fargs[i].error;break;}
        free(fargs);
        if(failure){free(part_start);free(part_count);free(sorted);errno=failure;return -1;}
    }
    free(part_start);
    free(part_count);
    free(sorted);
    return 0;
}
int slotcask_bulk_resolve_and_fetch_checked(SlotcaskDb *db,const uint8_t (*hashes)[16],size_t n,void *ctx,SlotcaskScanCb cb){
    if(!db||(!hashes&&n)||!cb){errno=EINVAL;return -1;}
    size_t count=0;errno=0;
    SlotcaskResolvedRec *resolved=slotcask_bulk_resolve_hashes_checked(db,hashes,n,&count);
    if(!resolved){int error=errno;return error?-1:0;}
    int rc=slotcask_bulk_fetch_resolved_checked(db,resolved,count,ctx,cb);int error=errno;
    free(resolved);errno=error;return rc;
}
````````

<!-- R1-BLOCK s2e70-old -->
````````text
   Caller must hold objlock_wrlock, so a failed check cannot be a concurrent
   repoint race. Returns 0 when all entries are valid, 1 when one or more
   entries are invalid, and -1 when the kf itself cannot be read. */
int slotcask_validate_live_refs(SlotcaskDb *db, uint64_t *out_invalid);

/* Sequential, fail-closed live walk. Caller holds objlock_wrlock, or —
   live guarded-write use (conditional-writes F2) — the object's shared
   objlock with the same registry instance pinned and every writer gate
   reserved. Returns 0 for a complete walk, 1 for callback-requested
   stop, -1 on any unreadable or invalid live reference. Does not
   silently skip. */
int slotcask_walk_live_strict(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);

/* Walk every live (flag=1) record. Parallelized across keyfile shards
   internally; cb may run on multiple threads — caller's ctx is responsible
   for its own synchronization. */
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);

/* Same as slotcask_walk_one_shard but the callback also receives the
   kf slot index. Used by the bitmap-index reindex path which needs to
   key bit positions by (kf_shard, kf_slot).

   Caller must hold `kh` for `kf_shard_id` for the entire call. This
   function neither acquires nor releases it; making ownership explicit
````````

<!-- R1-BLOCK s2e70-new -->
````````c
   Caller must hold objlock_wrlock, so a failed check cannot be a concurrent
   repoint race. Returns 0 when all entries are valid, 1 when one or more
   entries are invalid, and -1 when the kf itself cannot be read. */
int slotcask_validate_live_refs(SlotcaskDb *db, uint64_t *out_invalid);

/* Sequential, fail-closed live walk. Caller holds objlock_wrlock, or —
   live guarded-write use (conditional-writes F2) — the object's shared
   objlock with the same registry instance pinned and every writer gate
   reserved. Returns 0 for a complete walk, 1 for callback-requested
   stop, -1 on any unreadable or invalid live reference. Does not
   silently skip. */
int slotcask_walk_live_strict(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);
/* Checked ordinary-reader counterparts. Use the existing per-shard cache read
 * handles (no writer gate/reservation, no new lock/order/global snapshot).
 * Caller pins matching registry/schema and holds the object read lock.
 * Callbacks borrow the current record while KF/segment handles remain live;
 * validate/render bounded owned output only, never perform client I/O or
 * nested storage/pool work. Returns 0 complete/miss, 1 callback stop, -1 invalid
 * or unreadable reference. Cache handles are released on every return path. */
int slotcask_walk_live_checked(SlotcaskDb *db,SlotcaskScanCb cb,void *ctx);
int slotcask_lookup_by_hash_checked(SlotcaskDb *db,const uint8_t hash[16],SlotcaskScanCb cb,void *ctx);


/* Walk every live (flag=1) record. Parallelized across keyfile shards
   internally; cb may run on multiple threads — caller's ctx is responsible
   for its own synchronization. */
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);

/* Same as slotcask_walk_one_shard but the callback also receives the
   kf slot index. Used by the bitmap-index reindex path which needs to
   key bit positions by (kf_shard, kf_slot).

   Caller must hold `kh` for `kf_shard_id` for the entire call. This
   function neither acquires nor releases it; making ownership explicit
````````

<!-- R1-BLOCK s2e71-old -->
````````text
                           const uint8_t hash16[16],
                           uint32_t *out_slot);

/* Compute the starting slot index for hash in a KF of capacity `cap`.
 * Public so hot loops can inline the probe without per-hash acquire. */
size_t kf_slot_for(const uint8_t hash[16], size_t cap);

/* Build the canonical kf shard path under a data_dir.
 * Public so query workers can pre-open KF handles. */
void kf_path_for(char out[PATH_MAX], const char *data_dir, int shard_id);

#endif
````````

<!-- R1-BLOCK s2e71-new -->
````````c
                           const uint8_t hash16[16],
                           uint32_t *out_slot);

/* Compute the starting slot index for hash in a KF of capacity `cap`.
 * Public so hot loops can inline the probe without per-hash acquire. */
size_t kf_slot_for(const uint8_t hash[16], size_t cap);

/* Build the canonical kf shard path under a data_dir.
 * Public so query workers can pre-open KF handles. */
void kf_path_for(char out[PATH_MAX], const char *data_dir, int shard_id);

#endif

/* Checked bulk counterparts keep existing shard/segment batching and pool
 * dispatch, with explicit allocation/unreadable-reference failure status. */
int slotcask_bulk_lookup_in_kfshard_checked(SlotcaskDb*,int,SlotcaskBulkRec*,size_t);
int slotcask_bulk_resolve_and_fetch_checked(SlotcaskDb*,const uint8_t (*)[16],size_t,void*,SlotcaskScanCb);
````````

<!-- R1-BLOCK s2e72-old -->
````````text
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <math.h>
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
/* Defined here (next to its only reader): forces the update-error JSON
 * escape to fail so tests can exercise the static-fallback emission.
 * _Atomic: control-thread writes, request-thread reads. */
_Atomic int g_shard_test_json_escape_fail;
#endif

int legacy_field_absent(const TypedField *f, const uint8_t *p);

````````

<!-- R1-BLOCK s2e72-new -->
````````c
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include "packed_csv_command.h"
#include "typed_read_command.h"
#include <errno.h>
#include <math.h>
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
/* Defined here (next to its only reader): forces the update-error JSON
 * escape to fail so tests can exercise the static-fallback emission.
 * _Atomic: control-thread writes, request-thread reads. */
_Atomic int g_shard_test_json_escape_fail;
#endif

int legacy_field_absent(const TypedField *f, const uint8_t *p);

````````

<!-- R1-BLOCK s2e73-old -->
````````text
    uint64_t live = 0, del = 0;
    resolve_counts_with_schema(db_root, object, sc, &live, &del);
    return (long long)live;
}

/* Forward declaration */
int is_number(const char *s);

/* ========== GET ========== */

int cmd_get(const char *db_root, const char *object,
            const char *key, size_t klen) {
    Schema sc = load_schema(db_root, object);

    /* Route through slotcask. Wire response shape (bare value dict for
       single-key get, per 2026.05.1) is preserved. */
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    uint8_t get_hdr2 = 0;
    if (slotcask_get_ex(sdb, key, klen, &val, &vlen, &get_hdr2) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }
    LOG_DEBUG(LOG_SUB_SLOTCASK, "GET %s (klen=%zu, %zu bytes)", object, klen, vlen);
    TypedSchema *ts = load_typed_schema(db_root, object);
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
}

/* Single-key get with a field projection. Same slotcask read path as
   cmd_get; response is a bare dict of the requested fields (matching
   cmd_get's bare-value contract — no {"key":...,"value":{...}} wrapper).
   fields_csv is a comma-separated field list (composite fields use '+',
   same as decode_field elsewhere). */
int cmd_get_fields(const char *db_root, const char *object,
                    const char *key, size_t klen, const char *fields_csv) {
    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    uint8_t cur_hdr2 = 0;
    if (slotcask_get_ex(sdb, key, klen, &val, &vlen, &cur_hdr2) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }

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
    strncpy(proj_buf, fields_csv, MAX_LINE - 1);
    proj_buf[MAX_LINE - 1] = '\0';
    const char *flds[MAX_FIELDS];
    int nf = 0;
    char *_tok_save = NULL;
    char *tok = strtok_r(proj_buf, ",", &_tok_save);
    while (tok && nf < MAX_FIELDS) { flds[nf++] = tok; tok = strtok_r(NULL, ",", &_tok_save); }

    OUT("{");
    int first = 1;
    for (int fi = 0; fi < nf; fi++) {
        char *pv = json_projected_field((const char *)val, vlen, flds[fi],
            (pfs.ts || pfs.nfields > 0) ? &pfs : NULL, cur_hdr2);
        if (!pv) continue;
        OUT("%s\"%s\":%s", first ? "" : ",", flds[fi], pv);
        first = 0; free(pv);
    }
    OUT("}\n");
    free(val);
    return 0;
}

/* ========== CAS (Compare-and-Set) helper ========== */

/* Evaluate a COMPILED criteria tree against a typed record. NULL tree
   = pass. Read-only on tree and fs — one compiled tree may be shared
   across bulk workers. value_len may be < ts->total_size for
   trim-encoded records. */
int cas_check_tree(FieldSchema *fs, const uint8_t *value_ptr,
                   int value_len, uint8_t hdr2, const CriteriaNode *tree) {
    if (!tree) return 1;
    return cas_match_tree_checked(value_ptr, (size_t)value_len, tree, fs, hdr2) > 0;
}

/* ========== INSERT — v2 (slotcask) helper ==========
 *
 * Closure carries everything the upsert callbacks need. check_fn validates
 * if_json criteria; pre_commit_fn diffs and updates indexes between data
 * write and kf commit (Option B). The ts pointer is borrowed from
 * load_typed_schema's cache and stays valid for the request's lifetime.
 */

static char *single_write_decode(const TypedSchema *ts,const uint8_t *b,
    size_t n,uint8_t hdr2){
    TypedRowView view;
    if(typed_row_open(ts,b,n,hdr2,&view))return NULL;
    return ts->packed.nfields?typed_packed_decode(ts,b,n,hdr2,SIZE_MAX):
        typed_decode(ts,b,(uint32_t)n,hdr2);
}
static int single_write_finish(const TypedSchema *ts,SlotcaskDb *db,size_t klen,
    uint8_t *b,size_t *n,int want_record,char **record,char *err,size_t cap){
    if(!ts->packed.nfields)*n=schema_trim_fn(b,*n,(void *)ts);
    if(*n>UINT32_MAX||*n>SIZE_MAX-24-klen-7||
       ((24+klen+*n+7)&~(size_t)7)>(size_t)db->slot_size||
       ((24+klen+*n+7)&~(size_t)7)>slotcask_seg_max_bytes()){
        snprintf(err,cap,"record exceeds slot or segment capacity");errno=EFBIG;return -1;
    }
    if(want_record){
        *record=single_write_decode(ts,b,*n,typed_schema_hdr2(ts));
        if(!*record){snprintf(err,cap,"cannot render committed record");errno=ENOMEM;return -1;}
````````

<!-- R1-BLOCK s2e73-new -->
````````c
    uint64_t live = 0, del = 0;
    resolve_counts_with_schema(db_root, object, sc, &live, &del);
    return (long long)live;
}

/* Forward declaration */
int is_number(const char *s);

/* ========== GET ========== */

int cmd_get(const char *db_root, const char *object,
            const char *key, size_t klen) {
    return typed_read_single_route(db_root,object,key,klen,NULL);
}

/* Single-key get with a field projection. Same slotcask read path as
   cmd_get; response is a bare dict of the requested fields (matching
   cmd_get's bare-value contract — no {"key":...,"value":{...}} wrapper).
   fields_csv is a comma-separated field list (composite fields use '+',
   same as decode_field elsewhere). */
int cmd_get_fields(const char *db_root, const char *object,
                    const char *key, size_t klen, const char *fields_csv) {
    return typed_read_single_route(db_root,object,key,klen,fields_csv);
}

/* ========== CAS (Compare-and-Set) helper ========== */

/* Evaluate a COMPILED criteria tree against a typed record. NULL tree
   = pass. Read-only on tree and fs — one compiled tree may be shared
   across bulk workers. value_len may be < ts->total_size for
   trim-encoded records. */
int cas_check_tree(FieldSchema *fs, const uint8_t *value_ptr,
                   int value_len, uint8_t hdr2, const CriteriaNode *tree) {
    if (!tree) return 1;
    return cas_match_tree_checked(value_ptr, (size_t)value_len, tree, fs, hdr2) > 0;
}

/* ========== INSERT — v2 (slotcask) helper ==========
 *
 * Closure carries everything the upsert callbacks need. check_fn validates
 * if_json criteria; pre_commit_fn diffs and updates indexes between data
 * write and kf commit (Option B). The ts pointer is borrowed from
 * load_typed_schema's cache and stays valid for the request's lifetime.
 */

static char *single_write_decode(const TypedSchema *ts,const uint8_t *b,size_t n,uint8_t hdr2){
    if(n>INT_MAX||n>=g_query_buffer_max_bytes){errno=ENOMEM;return NULL;}
    return typed_decode_bounded(ts,b,(int)n,hdr2,g_query_buffer_max_bytes-n);
}
static int single_write_finish(const TypedSchema *ts,SlotcaskDb *db,size_t klen,
    uint8_t *b,size_t *n,int want_record,char **record,char *err,size_t cap){
    if(!ts->packed.nfields)*n=schema_trim_fn(b,*n,(void *)ts);
    if(*n>UINT32_MAX||*n>SIZE_MAX-24-klen-7||
       ((24+klen+*n+7)&~(size_t)7)>(size_t)db->slot_size||
       ((24+klen+*n+7)&~(size_t)7)>slotcask_seg_max_bytes()){
        snprintf(err,cap,"record exceeds slot or segment capacity");errno=EFBIG;return -1;
    }
    if(want_record){
        *record=single_write_decode(ts,b,*n,typed_schema_hdr2(ts));
        if(!*record){snprintf(err,cap,"cannot render committed record");errno=ENOMEM;return -1;}
````````

<!-- R1-BLOCK s2e74-old -->
````````text
       check_fn alongside saved_old_value. */
    uint8_t           old_hdr2;
    int               return_record;
    char             *ret_json; /* rendered before reservation; command-owned */
    SlotcaskDb       *db;
    size_t            key_len;
} V2UpdateCtx;

/* Parsed computed-field spec for one update (conditional-writes F1).
   op: 0 = none, 1 = $incr, 2 = $copy. */
typedef struct {
    int  op;
    char literal[128];   /* $incr: literal span, quotes stripped */
    int  src_idx;        /* $copy: sibling TypedField index */
} V2FieldCompute;

/* NEW-from-OLD constructor for single partial updates. Runs inside
   upsert_slow_path while the kf-shard write lock is held, after
   if_not_exists / require_existing / check have accepted the current OLD —
   so the replacement is built from the same OLD that the commit will
   overwrite, never from a stale caller-side snapshot. Copies every
   untouched field, applies only fields present in the request, preserves
   removed fields, and stamps auto_update fields exactly once. */
/* Rendering predicate for legacy records: 1 if the legacy decoder would
   SKIP this field's bytes. Used ONLY to port a legacy record into
   nullmap form without changing what clients see — query existence uses
   legacy_field_query_exists (query_plan.c) instead, and indexed-exists
````````

<!-- R1-BLOCK s2e74-new -->
````````c
       check_fn alongside saved_old_value. */
    uint8_t           old_hdr2;
    int               return_record;
    char             *ret_json; /* rendered before reservation; command-owned */
    SlotcaskDb       *db;
    size_t            key_len;
} V2UpdateCtx;

/* Parsed computed-field spec for one update (conditional-writes F1).
   op: 0 = none, 1 = $incr, 2 = $copy. */
typedef struct {
    int  op;
    char *literal;      /* owned exact $incr text, JSON-decoded once */
    int  src_idx;        /* $copy: sibling TypedField index */
} V2FieldCompute;
typedef struct {V2FieldCompute fields[MAX_FIELDS];} V2ComputeSet;
static void v2_compute_free(V2ComputeSet *set){
    for(int i=0;i<MAX_FIELDS;i++)free(set->fields[i].literal);
}


/* NEW-from-OLD constructor for single partial updates. Runs inside
   upsert_slow_path while the kf-shard write lock is held, after
   if_not_exists / require_existing / check have accepted the current OLD —
   so the replacement is built from the same OLD that the commit will
   overwrite, never from a stale caller-side snapshot. Copies every
   untouched field, applies only fields present in the request, preserves
   removed fields, and stamps auto_update fields exactly once. */
/* Rendering predicate for legacy records: 1 if the legacy decoder would
   SKIP this field's bytes. Used ONLY to port a legacy record into
   nullmap form without changing what clients see — query existence uses
   legacy_field_query_exists (query_plan.c) instead, and indexed-exists
````````

<!-- R1-BLOCK s2e75-old -->
````````text
                      f->type != FT_FLOAT && f->type != FT_DOUBLE;
    if (unsupported) {
        free(incr); free(copy);
        snprintf(err_buf, err_sz,
                 "field '%s': unsupported type for computed update",
                 f->name);
        return -1;
    }
    if (incr) {
        out->op = 1;
        size_t n = strlen(incr);
        if (n >= 2 && incr[0] == '"' && incr[n - 1] == '"') {
            memmove(incr, incr + 1, n - 2);
            incr[n - 2] = '\0';
        }
        if (!incr[0] || strlen(incr) >= sizeof(out->literal)) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': bad computed literal", f->name);
            return -1;
        }
        memcpy(out->literal, incr, strlen(incr) + 1);
    } else {
        out->op = 2;
        size_t n = strlen(copy);
        char src[256]; char *decoded = NULL; size_t dlen = 0;
        if (n < 2 || copy[0] != '"' || copy[n - 1] != '"' ||
            json_unescape_cstring(copy + 1, n - 2, &decoded, &dlen) != 0 ||
            dlen >= sizeof(src)) {
            free(decoded); free(incr); free(copy); goto malformed;
        }
        memcpy(src, decoded, dlen); src[dlen] = '\0'; free(decoded);
        int si = typed_field_index((TypedSchema *)ts, src);
        if (si < 0) {
````````

<!-- R1-BLOCK s2e75-new -->
````````c
                      f->type != FT_FLOAT && f->type != FT_DOUBLE;
    if (unsupported) {
        free(incr); free(copy);
        snprintf(err_buf, err_sz,
                 "field '%s': unsupported type for computed update",
                 f->name);
        return -1;
    }
    if (incr) {
        out->op = 1;
        size_t n = strlen(incr);
        if (n >= 2 && incr[0] == '"' && incr[n - 1] == '"') {
            char *decoded=NULL;size_t decoded_len=0;
            if(json_unescape_cstring(incr+1,n-2,&decoded,&decoded_len)){
                free(incr);free(copy);return -1;
            }
            free(incr);incr=decoded;
        }
        if(!incr[0]||strlen(incr)>=g_query_buffer_max_bytes) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': bad computed literal", f->name);
            return -1;
        }
        out->literal=incr;incr=NULL;
    } else {
        out->op = 2;
        size_t n = strlen(copy);
        char src[256]; char *decoded = NULL; size_t dlen = 0;
        if (n < 2 || copy[0] != '"' || copy[n - 1] != '"' ||
            json_unescape_cstring(copy + 1, n - 2, &decoded, &dlen) != 0 ||
            dlen >= sizeof(src)) {
            free(decoded); free(incr); free(copy); goto malformed;
        }
        memcpy(src, decoded, dlen); src[dlen] = '\0'; free(decoded);
        int si = typed_field_index((TypedSchema *)ts, src);
        if (si < 0) {
````````

<!-- R1-BLOCK s2e76-old -->
````````text
             "field '%s': malformed computed value object", field_name);
    return -1;
}

/* Apply parsed computed fields to NEW (out_value), reading currents
   from the lock-protected OLD. Called after
   update_compose_new_from_old (computed slots carry OLD then) and
   before auto_update stamping, inside the same kf-wrlock hold as the
   CAS check. Results are written as final field bytes: BE int64 for
   integer/numeric, host order for float/double (the record layout's
   own convention). Returns 0, or -1 with err_buf set — the caller
   aborts the whole update (nothing has been written yet). */
static int apply_computed_fields(const TypedSchema *ts,
                                 const SlotcaskOldRecord *old,
                                 uint8_t *out_value,
                                 const V2FieldCompute *comp,
                                 char *err_buf, size_t err_sz) {
    const uint8_t *o_base; const uint8_t *o_bm; int o_bm_bytes;
    size_t o_region;
    if (typed_record_view(old->value, (size_t)old->vlen, old->hdr2,
                          &o_base, &o_bm, &o_bm_bytes, &o_region) != 0) {
        snprintf(err_buf, err_sz, "corrupt record header on update target");
        return -1;
    }
````````

<!-- R1-BLOCK s2e76-new -->
````````c
             "field '%s': malformed computed value object", field_name);
    return -1;
}

/* Apply parsed computed fields to NEW (out_value), reading currents
   from the lock-protected OLD. Called after
   update_compose_new_from_old (computed slots carry OLD then) and
   before auto_update stamping, inside the same kf-wrlock hold as the
   CAS check. Results are written as final field bytes: BE int64 for
   integer/numeric, host order for float/double (the record layout's
   own convention). Returns 0, or -1 with err_buf set — the caller
   aborts the whole update (nothing has been written yet). */
static int legacy_numeric_increment(const TypedField *f,const uint8_t *old,
    const char *literal,uint8_t *out,char *err,size_t cap){
    if(f->numeric_scale<0||f->numeric_scale>18||f->numeric_scale_mult<=0){errno=EINVAL;return -1;}
    char current[96];int n=decode_field_to_buf(f,old,current,sizeof(current),1);
    if(n<1||(size_t)n>=sizeof(current)){errno=EILSEQ;return -1;}
    dec_t a,b,sum,rounded;dec_init(&a);dec_init(&b);dec_init(&sum);dec_init(&rounded);
    dec_status st=dec_parse(current,(size_t)n,&a,err,cap);
    if(st==DEC_OK)st=dec_parse(literal,strlen(literal),&b,err,cap);
    if(st==DEC_OK)st=dec_add(&a,&b,&sum);
    /* Legacy P is informational. Only its actual int64/scale interpretation
     * constrains assignment; migration audits the declared P separately. */
    if(st==DEC_OK)st=dec_apply_precision(&sum,DEC_MAX_PRECISION,f->numeric_scale,&rounded,err,cap);
    int rc=-1;
    if(st==DEC_OK){
        size_t bytes=dec_text_len(&rounded);
        char *text=bytes&&bytes<=g_query_buffer_max_bytes?malloc(bytes):NULL;
        if(!text)st=DEC_ERR_NOMEM;
        else{
            size_t used=0;st=dec_to_text(&rounded,text,bytes,&used);
            int64_t value;
            if(st==DEC_OK&&!conv_numeric_text(f,text,used,&value,err,cap)){
                uint64_t bits=(uint64_t)value;
                for(int i=7;i>=0;i--){out[i]=(uint8_t)bits;bits>>=8;}rc=0;
            }
            free(text);
        }
    }
    dec_free(&rounded);dec_free(&sum);dec_free(&b);dec_free(&a);
    if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EINVAL;}
    return rc;
}

static int apply_computed_fields(const TypedSchema *ts,
                                 const SlotcaskOldRecord *old,
                                 uint8_t *out_value,
                                 const V2FieldCompute *comp,
                                 char *err_buf, size_t err_sz) {
    const uint8_t *o_base; const uint8_t *o_bm; int o_bm_bytes;
    size_t o_region;
    if (typed_record_view(old->value, (size_t)old->vlen, old->hdr2,
                          &o_base, &o_bm, &o_bm_bytes, &o_region) != 0) {
        snprintf(err_buf, err_sz, "corrupt record header on update target");
        return -1;
    }
````````

<!-- R1-BLOCK s2e77-old -->
````````text
            if (end == comp[i].literal || *end || errno == ERANGE) goto badliteral;
            int64_t r;
            if (__builtin_add_overflow(v, (int64_t)d, &r)) goto overflow;
            int64_t hi = (w == 2) ? INT16_MAX
                       : (w == 4) ? INT32_MAX : INT64_MAX;
            int64_t lo = (w == 2) ? INT16_MIN
                       : (w == 4) ? INT32_MIN : INT64_MIN;
            if (r < lo || r > hi) goto overflow;
            uint64_t be = (uint64_t)r;
            for (int b = 0; b < w; b++)
                dst[b] = (uint8_t)(be >> (8 * (w - 1 - b)));
            typed_clear_null(out_value + 1, nbm_bytes, i);
        } else if (f->type == FT_NUMERIC) {
            uint64_t raw = 0;
            for (int b = 0; b < 8; b++) raw = (raw << 8) | cur[b];
            int64_t v = (int64_t)raw;
            const char *s = comp[i].literal;
            int neg = 0;
            if (*s == '-') { neg = 1; s++; }
            else if (*s == '+') s++;
            const char *dot = strchr(s, '.');
            size_t intlen = dot ? (size_t)(dot - s) : strlen(s);
            size_t frlen  = dot ? strlen(dot + 1) : 0;
            if (frlen > (size_t)f->numeric_scale) goto badliteral;
            if (!intlen && !frlen) goto badliteral;
            uint64_t magnitude = 0;
            for (size_t b = 0; b < intlen; b++) {
                if (s[b] < '0' || s[b] > '9') goto badliteral;
                if (__builtin_mul_overflow(magnitude, UINT64_C(10), &magnitude) ||
                    __builtin_add_overflow(magnitude, (uint64_t)(s[b] - '0'),
                                           &magnitude)) goto overflow;
            }
            /* Append fraction, then pad to S. Scale exactly ONCE. */
            for (size_t b = 0; b < (size_t)f->numeric_scale; b++) {
                uint64_t digit = 0;
                if (b < frlen) {
                    if (dot[1 + b] < '0' || dot[1 + b] > '9') goto badliteral;
                    digit = (uint64_t)(dot[1 + b] - '0');
                }
                if (__builtin_mul_overflow(magnitude, UINT64_C(10), &magnitude) ||
                    __builtin_add_overflow(magnitude, digit, &magnitude)) goto overflow;
            }
            uint64_t limit = (uint64_t)INT64_MAX + (neg ? 1u : 0u);
            if (magnitude > limit) goto overflow;
            int64_t scaled = neg
                ? (magnitude == (uint64_t)INT64_MAX + 1u
                    ? INT64_MIN : -(int64_t)magnitude)
                : (int64_t)magnitude;
            int64_t r;
            if (__builtin_add_overflow(v, scaled, &r)) goto overflow;
            uint64_t be = (uint64_t)r;
            for (int b = 0; b < 8; b++)
                dst[b] = (uint8_t)(be >> (8 * (7 - b)));
            typed_clear_null(out_value + 1, nbm_bytes, i);
        } else {   /* FT_FLOAT / FT_DOUBLE */
            double v;
            if (f->type == FT_FLOAT) {
                float fv = 0.0f;
                memcpy(&fv, cur, 4);
                v = (double)fv;
            } else {
                memcpy(&v, cur, 8);
            }
            errno = 0;
            char *end = NULL;
            double d = strtod(comp[i].literal, &end);
````````

<!-- R1-BLOCK s2e77-new -->
````````c
            if (end == comp[i].literal || *end || errno == ERANGE) goto badliteral;
            int64_t r;
            if (__builtin_add_overflow(v, (int64_t)d, &r)) goto overflow;
            int64_t hi = (w == 2) ? INT16_MAX
                       : (w == 4) ? INT32_MAX : INT64_MAX;
            int64_t lo = (w == 2) ? INT16_MIN
                       : (w == 4) ? INT32_MIN : INT64_MIN;
            if (r < lo || r > hi) goto overflow;
            uint64_t be = (uint64_t)r;
            for (int b = 0; b < w; b++)
                dst[b] = (uint8_t)(be >> (8 * (w - 1 - b)));
            typed_clear_null(out_value + 1, nbm_bytes, i);
        } else if(f->type==FT_NUMERIC){
            if(legacy_numeric_increment(f,cur,comp[i].literal,dst,err_buf,err_sz))return -1;
            typed_clear_null(out_value+1,nbm_bytes,i);
        } else {   /* FT_FLOAT / FT_DOUBLE */
            double v;
            if (f->type == FT_FLOAT) {
                float fv = 0.0f;
                memcpy(&fv, cur, 4);
                v = (double)fv;
            } else {
                memcpy(&v, cur, 8);
            }
            errno = 0;
            char *end = NULL;
            double d = strtod(comp[i].literal, &end);
````````

<!-- R1-BLOCK s2e78-old -->
````````text
        (size_t)c->idx_ts->total_size > out_capacity) return -1;
    c->saved_old_value = old->value;
    c->saved_old_vlen = old->vlen;
    c->old_hdr2 = old->hdr2;
#ifdef TEST_BUILD
    slotcask_test_after_old(1);
#endif
    JsonObj patch;
    if (json_parse_object(c->partial_json, strlen(c->partial_json), &patch) < 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "invalid update value");
        return -1;
    }
    V2FieldCompute comp[MAX_FIELDS] = {0};
    uint8_t is_computed[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const char *v; size_t n;
        if (!c->idx_ts->fields[i].removed &&
            json_obj_get(&patch, c->idx_ts->fields[i].name, &v, &n) && n && *v == '{')
            is_computed[i] = 1;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        if (!is_computed[i]) continue;
        char *span = json_obj_strdup_raw(&patch, c->idx_ts->fields[i].name);
        if (!span) { snprintf(c->err_buf, sizeof(c->err_buf), "oom"); return -1; }
        int rc = parse_computed_span(c->idx_ts, c->idx_ts->fields[i].name,
````````

<!-- R1-BLOCK s2e78-new -->
````````c
        (size_t)c->idx_ts->total_size > out_capacity) return -1;
    c->saved_old_value = old->value;
    c->saved_old_vlen = old->vlen;
    c->old_hdr2 = old->hdr2;
#ifdef TEST_BUILD
    slotcask_test_after_old(1);
#endif
    JsonObj patch;
    if (json_parse_object(c->partial_json, strlen(c->partial_json), &patch) < 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "invalid update value");
        return -1;
    }
    V2ComputeSet set __attribute__((cleanup(v2_compute_free)))={0};
    V2FieldCompute *comp=set.fields;
    uint8_t is_computed[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const char *v; size_t n;
        if (!c->idx_ts->fields[i].removed &&
            json_obj_get(&patch, c->idx_ts->fields[i].name, &v, &n) && n && *v == '{')
            is_computed[i] = 1;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        if (!is_computed[i]) continue;
        char *span = json_obj_strdup_raw(&patch, c->idx_ts->fields[i].name);
        if (!span) { snprintf(c->err_buf, sizeof(c->err_buf), "oom"); return -1; }
        int rc = parse_computed_span(c->idx_ts, c->idx_ts->fields[i].name,
````````

<!-- R1-BLOCK s2e79-old -->
````````text
    size_t *n,void *ptr){
    V2UpdateCtx *c=ptr;TypedSchema *ts=c->idx_ts;
    if(!old){errno=EINVAL;return -1;}
    TypedRowView view;
    if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view)){
        snprintf(c->err_buf,sizeof(c->err_buf),"invalid OLD generation or value");errno=EILSEQ;return -1;}
    c->saved_old_value=old->value;c->saved_old_vlen=old->vlen;c->old_hdr2=old->hdr2;
    int rc;
    if(ts->packed.nfields){
#ifdef TEST_BUILD
        slotcask_test_after_old(1);
#endif
        rc=typed_update_json_alloc(ts,old,c->partial_json,out,n,c->err_buf,sizeof(c->err_buf));
    }else{
        *out=malloc((size_t)ts->total_size);
        if(!*out){snprintf(c->err_buf,sizeof(c->err_buf),"oom");errno=ENOMEM;return -1;}
        rc=v2_update_new_from_old(old,*out,(size_t)ts->total_size,n,c);
    }
    if(rc)return -1;
    return single_write_finish(ts,c->db,c->key_len,*out,n,c->return_record,
        &c->ret_json,c->err_buf,sizeof(c->err_buf));
}


static int v2_update_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
````````

<!-- R1-BLOCK s2e79-new -->
````````c
    size_t *n,void *ptr){
    V2UpdateCtx *c=ptr;TypedSchema *ts=c->idx_ts;
    if(!old){errno=EINVAL;return -1;}
    TypedRowView view;
    if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view)){
        snprintf(c->err_buf,sizeof(c->err_buf),"invalid OLD generation or value");errno=EILSEQ;return -1;}
    c->saved_old_value=old->value;c->saved_old_vlen=old->vlen;c->old_hdr2=old->hdr2;
    int rc;
    if(ts->packed.nfields){
#ifdef TEST_BUILD
        slotcask_test_after_old(1);
#endif
        rc=typed_update_computed_alloc(ts,old,c->partial_json,out,n,c->err_buf,sizeof(c->err_buf));
    }else{
        size_t budget=g_query_buffer_max_bytes;
        size_t source=strnlen(c->partial_json,budget);
        if(source==budget||(size_t)ts->total_size>budget||
            source>(budget-(size_t)ts->total_size)/4){errno=ENOMEM;return -1;}
        *out=malloc((size_t)ts->total_size);
        if(!*out){snprintf(c->err_buf,sizeof(c->err_buf),"oom");errno=ENOMEM;return -1;}
        rc=v2_update_new_from_old(old,*out,(size_t)ts->total_size,n,c);
    }
    if(rc)return -1;
    return single_write_finish(ts,c->db,c->key_len,*out,n,c->return_record,
        &c->ret_json,c->err_buf,sizeof(c->err_buf));
}


static int v2_update_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
````````

<!-- R1-BLOCK s2e80-old -->
````````text
int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run, int return_record) {
    Schema sc = load_schema(db_root, object);
    return cmd_delete_v2(db_root, object, key, klen, if_json,
                          dry_run, return_record, &sc);
}

/* ========== MULTI-KEY GET ========== */

/* ========== Parallel multi-key EXISTS ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output */
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    int orig_idx;
    int found;
} MultiExistsEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiExistsEntry *entries;
    int count;
} MultiExistsShardWork;

/* Parse one wire-form key from a JSON array element into its storage
   form, computing hash + kf-shard id along the way. Shared by every
   multi-key path (bulk_exists, bulk_get). Auto-key UUID/seq forms get
   parsed to binary; otherwise the storage form is verbatim bytes.
   On malformed auto-key forms returns 0 — caller treats the row as a
   miss (found=0 / result_json=NULL) so clients see the same "missing"
   shape as a non-existent key. Returns 1 on success.
   Heap allocations land in *out_wire_key and *out_storage_key — caller
   owns both. */
static int parse_multi_key(const char *src, size_t klen, const Schema *sc,
                            char  **out_wire_key,
                            uint8_t **out_storage_key, size_t *out_storage_klen,
                            uint8_t out_hash[16], int *out_shard_id) {
    char *wire = malloc(klen + 1);
    if (!wire) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "parse_multi_key: malloc(%zu) failed for wire key buffer", klen + 1);
        return -1;
    }
    memcpy(wire, src, klen);
    wire[klen] = '\0';
    *out_wire_key = wire;

    uint8_t *skey = NULL;
    size_t   slen = 0;
    if (sc->auto_key == AK_UUID) {
        uint8_t bin[16];
        if (parse_uuid_string(wire, bin) == 0) {
            skey = malloc(16);
            if (skey) { memcpy(skey, bin, 16); slen = 16; }
        }
    } else if (sc->auto_key == AK_SEQ) {
        int64_t v;
        if (parse_seq_key(wire, &v) == 0) {
            skey = malloc(8);
            if (skey) {
                for (int b = 7; b >= 0; b--) { skey[b] = (uint8_t)(v & 0xFF); v >>= 8; }
                slen = 8;
            }
        }
    } else {
        skey = malloc(klen + 1);
        if (skey) { memcpy(skey, src, klen); skey[klen] = '\0'; slen = klen; }
    }
    *out_storage_key = skey;
    *out_storage_klen = slen;
    if (skey) {
        compute_hash_raw((const char *)skey, slen, out_hash);
        *out_shard_id = compute_record_shard(out_hash, sc->splits);
    } else {
        /* Malformed auto-key wire form → never matches; keep shard
           deterministic so bucket-sort works. */
        memset(out_hash, 0, 16);
        *out_shard_id = 0;
    }
    return 1;
}

static void *multi_exists_shard_worker(void *arg) {
    MultiExistsShardWork *sw = (MultiExistsShardWork *)arg;
    if (sw->count == 0) return NULL;

    /* bulk_lookup_in_kfshard amortises kfcache_acquire + segcache_acquire
       across the worker's records. The dispatcher already aligned
       shard_id with compute_record_shard so all entries here hash to the
       same kf shard. */
    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    if (!batch) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: calloc(%d) failed for batch, keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }
    for (int ei = 0; ei < sw->count; ei++) {
        MultiExistsEntry *e = &sw->entries[ei];
        batch[ei].key       = e->key;
        batch[ei].klen      = e->klen;
        batch[ei].value     = NULL;
        batch[ei].vlen      = 0;
        batch[ei].user_ctx  = NULL;
        batch[ei].old_value = NULL;
        batch[ei].old_vlen  = 0;
        batch[ei].status    = 0;
        batch[ei].was_update = 0;
    }
    int kf_shard_id = sw->entries[0].shard_id;  /* aligned by dispatcher */
    slotcask_bulk_lookup_in_kfshard(sdb, kf_shard_id, batch, (size_t)sw->count);
    for (int ei = 0; ei < sw->count; ei++) {
        sw->entries[ei].found = (batch[ei].status == 0) ? 1 : 0;
    }
    free(batch);
    return NULL;
}

/* Bucket-sort entries by shard_id, dispatch multi_exists_shard_worker
   in parallel across shards, copy `.found` results back into `entries`
   in original order. Shared by cmd_exists_multi, cmd_not_exists, and
   cmd_get_multi — they each parse their key list into `entries[]` then
   call this; afterwards they format their type-specific response from
   the populated `entries[].found`.

   Replaces the O(n²) insertion sort that previously dominated BULK
   EXISTS / BULK GET: at 10K keys × ~128 shards the sort was ~50M
   swaps before parallel_for even started. The bucket-sort is a
   single pass over entries plus one pass to fan out into per-shard
   buckets. */
static void multi_bucket_dispatch(MultiExistsEntry *entries, int key_count,
                                  const Schema *sc,
                                  const char *db_root, const char *object) {
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts    = calloc(sc->splits, sizeof(int));
    int *shard_to_worker = malloc(sc->splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc->splits; s++) if (shard_counts[s] > 0) nshard++;

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc->splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object  = object;
                workers[g].sch     = sc;
                workers[g].count   = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiExistsEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    parallel_for_io(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    /* Copy results back via orig_idx (no sorted[] indirection). */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].found = workers[g].entries[i].found;

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    free(shard_counts);
    free(shard_to_worker);
}

/* mode=exists with keys[], returns {"k1":true,"k2":false,...} */
int cmd_exists_multi(const char *db_root, const char *object, const char *keys_json,
                     const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* xrealloc_or_free freed the old buffer; per-key
                       strings inside it are leaked (no double-free,
                       since the array holding their pointers is gone)
                       — acceptable on OOM. Coverity CID 1693843: the
                       previous "entries=NULL; break" left key_count>0
                       and the loop below would deref NULL. */
                    OUT("{\"error\":\"oom: bulk_exists keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

    /* Output in original order — build the response in a single buffer
       and OUT it once. Was 10K fprintf() calls in a loop, each taking
       the per-FILE stdio lock + parsing the format string (~15+ ms for
       10K keys at 1-2 µs/call). One snprintf-into-buffer + one OUT is
       hundreds of µs. */
    if (csv_delim) {
        OUT("key%cexists\n", csv_delim);
        size_t cap = (size_t)key_count * 64 + 64;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) for (int i = 0; i < key_count; i++) {
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
            if (pos + klen + 16 > cap) {
                cap = (pos + klen + 16) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: re-assert the post-grow invariant in subtractive
               form (no addition-overflow path). The pre-grow above already
               guarantees this — the check is dead code on the happy path
               and the compiler DCEs it — but Coverity's flow analysis
               loses the size-aliasing through the realloc→t→buf chain
               and flags the memcpy below as OVERRUN. CID 1693857/1693869. */
            if (cap < pos || cap - pos < klen) { free(buf); buf = NULL; break; }
            /* csv_emit_cell quotes if needed; do it via the existing helper but
               into our buffer via snprintf — replicate the no-quote-needed shape
               here to avoid re-implementing the quote logic. */
            /* CID 1693857/1693869 - bounds checked above, triage */
            memcpy(buf + pos, out_key, klen); pos += klen;
            /* Use SB_APPEND for bounded write — pre-grow above guarantees
               room, but the macro silences the CodeQL "potentially
               overflowing snprintf" finding by clamping to cap-1. */
            SB_APPEND(buf, pos, cap, "%c%s\n",
                       csv_delim, entries[i].found ? "true" : "false");
        }
        if (buf) {
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    } else {
        size_t cap = (size_t)key_count * 32 + 32;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) {
            buf[pos++] = '{';
            for (int i = 0; i < key_count; i++) {
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
                if (pos + klen + 16 > cap) {
                    cap = (pos + klen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow
                   invariant — see the equivalent comment in the CSV branch
                   above for rationale. Tautological on the happy path. */
                if (cap < pos || cap - pos < klen + 16) { free(buf); buf = NULL; break; }
                if (i) buf[pos++] = ',';
                buf[pos++] = '"';
                memcpy(buf + pos, out_key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].found) {
                    memcpy(buf + pos, "true", 4); pos += 4;
                } else {
                    memcpy(buf + pos, "false", 5); pos += 5;
                }
            }
        }
        if (buf) {
            buf[pos++] = '}'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        } else {
            OUT("{}\n");
        }
    }

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

/* mode=not-exists with keys[], returns keys that don't exist */
int cmd_not_exists(const char *db_root, const char *object, const char *keys_json) {
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* See cmd_exists_multi above for rationale (Coverity
                       CID 1693844). */
                    OUT("{\"error\":\"oom: bulk_get keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

    /* Build output in one buffer; one fwrite. */
    size_t cap = (size_t)key_count * 32 + 16;
    char *buf = malloc(cap);
    if (buf) {
        size_t pos = 0;
        buf[pos++] = '[';
        int first = 1;
        for (int i = 0; i < key_count; i++) {
            if (entries[i].found) continue;
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
            if (pos + klen + 8 > cap) {
                cap = (pos + klen + 8) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693871 */
            if (cap < pos || cap - pos < klen + 8) { free(buf); buf = NULL; break; }
            if (!first) buf[pos++] = ',';
            buf[pos++] = '"';
            /* CID 1693871 - bounds checked above, triage */
            memcpy(buf + pos, out_key, klen); pos += klen;
            buf[pos++] = '"';
            first = 0;
        }
        if (buf) {
            buf[pos++] = ']'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    }
    if (!buf) OUT("[]\n");

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

/* ========== Parallel multi-key GET ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output (NULL → use key) */
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    int orig_idx;
    char *result_json; /* NULL if not found */
} MultiGetEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiGetEntry *entries;
    int count;
    FieldSchema *fs;
} MultiGetShardWork;

/* Callback for multi-get: decodes value inline into entry's result_json. */
static int multi_get_fetch_cb(const uint8_t hash[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
                               uint8_t hdr2, void *ctx_ptr) {
    (void)key; (void)klen;
    MultiGetShardWork *sw = (MultiGetShardWork *)ctx_ptr;
    for (int ei = 0; ei < sw->count; ei++) {
        if (memcmp(sw->entries[ei].hash, hash, 16) == 0) {
            char *decoded = sw->fs ? typed_decode(sw->fs->ts,
                                                   (const uint8_t *)value,
                                                   (uint32_t)vlen, hdr2) : NULL;
            sw->entries[ei].result_json = decoded ? decoded : strdup("null");
            break;
        }
    }
    return 0;
}

static void *multi_get_shard_worker(void *arg) {
    MultiGetShardWork *sw = (MultiGetShardWork *)arg;
    if (sw->count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as missing", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    /* Extract pre-computed hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sw->count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: malloc(%d * 16) failed for hashes array", sw->db_root, sw->object, sw->count);
        return NULL;
    }
    for (int ei = 0; ei < sw->count; ei++)
        memcpy(hashes[ei], sw->entries[ei].hash, 16);

    /* Batch resolve+fetch — two-phase model resolves KF shards internally
       and parallelizes segment reads. Callback decodes each found record. */
    slotcask_bulk_resolve_and_fetch(sdb, hashes, (size_t)sw->count,
                                     sw, multi_get_fetch_cb);

    free(hashes);
    return NULL;
}

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
    /* Parse keys */
    int key_count = 0, key_cap = 256;
    MultiGetEntry *entries = malloc(key_cap * sizeof(MultiGetEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                /* Plain realloc (not xrealloc_or_free): on failure we still
                   need the old `entries` array intact to free each already-
                   parsed entry's key/wire_key before dropping the array
                   itself (CID 1696478). Resetting key_count to 0 here also
                   makes the `key_count == 0` early-return below fire
                   correctly instead of falling through to a NULL entries[]
                   dereference. */
                MultiGetEntry *t = realloc(entries, key_cap * sizeof(*t));
                if (!t) {
                    for (int j = 0; j < key_count; j++) {
                        free(entries[j].key);
                        free(entries[j].wire_key);
                    }
                    free(entries);
                    entries = NULL;
                    key_count = 0;
                    break;
                }
                entries = t;
            }
            MultiGetEntry *e = &entries[key_count++];
            /* Wire form for response output; storage form (binary for
               auto_key=uuid/seq, verbatim otherwise); hash + kf-shard
               bucket — see parse_multi_key. */
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->result_json = NULL;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    /* Bucket-sort by shard_id — same fix as cmd_exists_multi above. */
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts = calloc(sc.splits, sizeof(int));
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard++;

    FieldSchema fs; init_field_schema(&fs, db_root, object);
    MultiGetShardWork *workers = calloc(nshard, sizeof(MultiGetShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object = object;
                workers[g].sch = &sc;
                workers[g].fs = (fs.ts || fs.nfields > 0) ? &fs : NULL;
                workers[g].count = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiGetEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    /* Parallel fetch */
    parallel_for_io(multi_get_shard_worker, workers, nshard, sizeof(MultiGetShardWork));

    /* Copy results back to entries[] via orig_idx. */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].result_json = workers[g].entries[i].result_json;
    free(shard_counts); free(shard_to_worker);

    /* Output in original key order */
    if (csv_delim) {
        /* Header: key + schema fields (no projection on get-multi). */
        OUT("key");
        if (fs.ts) {
            for (int i = 0; i < fs.ts->nfields; i++) {
                if (fs.ts->fields[i].removed) continue;
                char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                csv_emit_cell(fs.ts->fields[i].name, csv_delim);
            }
        }
        OUT("\n");
        for (int i = 0; i < key_count; i++) {
            if (!entries[i].result_json) continue;
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            csv_emit_cell(out_key, csv_delim);
            JsonObj value_obj;
            int have_value = json_parse_object(entries[i].result_json,
                                               strlen(entries[i].result_json),
                                               &value_obj) >= 0;
            if (fs.ts) {
                for (int fi = 0; fi < fs.ts->nfields; fi++) {
                    if (fs.ts->fields[fi].removed) continue;
                    char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                    char *pv = !have_value ? NULL :
                        (fs.ts->fields[fi].type == FT_VARCHAR
                         ? json_obj_strdup_unescaped(&value_obj, fs.ts->fields[fi].name, NULL)
                         : json_obj_strdup(&value_obj, fs.ts->fields[fi].name));
                    csv_emit_cell(pv, csv_delim);
                    free(pv);
                }
            }
            OUT("\n");
            free(entries[i].result_json);
        }
    } else {
        /* Build the response in one buffer + one fwrite. Was 10K+
           fprintf calls in a loop (each takes the per-FILE stdio lock
           + parses the format string), the dominant cost in BULK GET
           on top of the bucket-sort fix. */
        size_t cap = (size_t)key_count * 256 + 64;
        char *buf = malloc(cap);
        if (buf) {
            size_t pos = 0;
            buf[pos++] = '{';
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
                size_t vlen = entries[i].result_json ? strlen(entries[i].result_json) : 4;
                if (pos + klen + vlen + 16 > cap) {
                    cap = (pos + klen + vlen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693870 */
                if (cap < pos || cap - pos < klen + vlen + 16) { free(buf); buf = NULL; break; }
                if (!first) buf[pos++] = ',';
                first = 0;
                buf[pos++] = '"';
                /* CID 1693870 - bounds checked above, triage */
                memcpy(buf + pos, out_key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].result_json) {
                    /* CID 1693872 - bounds checked above, triage */
                    memcpy(buf + pos, entries[i].result_json, vlen); pos += vlen;
                    free(entries[i].result_json);
                } else {
                    memcpy(buf + pos, "null", 4); pos += 4;
                }
            }
            if (buf) {
                buf[pos++] = '}'; buf[pos++] = '\n';
                fwrite(buf, 1, pos, g_out ? g_out : stdout);
                free(buf);
            }
        }
        if (!buf) {
            /* OOM fallback — old per-record path. */
            OUT("{");
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                if (!first) OUT(",");
                first = 0;
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                if (entries[i].result_json) {
                    OUT("\"%s\":%s", out_key, entries[i].result_json);
                    free(entries[i].result_json);
                } else {
                    OUT("\"%s\":null", out_key);
                }
            }
            OUT("}\n");
        }
    }

    /* cmd_get_multi has its own MultiGetEntry/MultiGetShardWork types
       (extra `fs` FieldSchema field, different result type) so it can't
       share the multi_bucket_dispatch helper used by exists/not_exists;
       clean up workers explicitly. */
    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

#ifdef TEST_BUILD
#include "single_write_test.h"
int shard_test_single_insert(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int only_new,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
    return cmd_insert_v2_impl(root,object,key,klen,json,condition,only_new,return_record,sc,ts,db);
}
int shard_test_single_update(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int dry_run,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
````````

<!-- R1-BLOCK s2e80-new -->
````````c
int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run, int return_record) {
    Schema sc = load_schema(db_root, object);
    return cmd_delete_v2(db_root, object, key, klen, if_json,
                          dry_run, return_record, &sc);
}

/* ========== MULTI-KEY GET ========== */

/* ========== Parallel multi-key EXISTS ========== */

int cmd_exists_multi(const char *db_root, const char *object, const char *keys_json,
                     const char *format, const char *delimiter) {
    TypedReadOptions options={.kind=TYPED_READ_EXISTS_MULTI,.keys=keys_json,
        .format=format,.delimiter=(format&&!strcmp(format,"csv"))?parse_csv_delim(delimiter):0,
        .budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
    return typed_read_route(db_root,object,&options);
}

int cmd_not_exists(const char *db_root, const char *object, const char *keys_json) {
    TypedReadOptions options={.kind=TYPED_READ_NOT_EXISTS,.keys=keys_json,
        .format=NULL,.delimiter=0,
        .budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
    return typed_read_route(db_root,object,&options);
}

int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter) {
    TypedReadOptions options={.kind=TYPED_READ_GET_MULTI,.keys=keys_json,.format=format,
        .delimiter=(format&&!strcmp(format,"csv"))?parse_csv_delim(delimiter):0,
        .budget=g_query_buffer_max_bytes,.timeout_ms=resolve_timeout_ms()};
    return typed_read_route(db_root,object,&options);
}

#ifdef TEST_BUILD
#include "single_write_test.h"
int shard_test_single_insert(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int only_new,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
    return cmd_insert_v2_impl(root,object,key,klen,json,condition,only_new,return_record,sc,ts,db);
}
int shard_test_single_update(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int dry_run,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
````````

<!-- R1-BLOCK s2e81-old -->
````````text
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
````````

<!-- R1-BLOCK s2e81-new -->
````````c
#include <stdatomic.h>
#include "typed_bulk_insert.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {size_t limit;_Atomic size_t used;} BulkMemory;
static int bulk_take(BulkMemory *m,size_t n){
    size_t used=atomic_load(&m->used);
    do{
        if(used>m->limit||n>m->limit-used){errno=ENOMEM;return -1;}
    }while(!atomic_compare_exchange_weak(&m->used,&used,used+n));
    return 0;
}
static void bulk_release(BulkMemory *m,size_t n){atomic_fetch_sub(&m->used,n);}

typedef struct {
    char *key,*json;
    size_t klen,ordinal;
    BulkMemory *memory;size_t charged;
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
    int update;BulkMemory memory;
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
static char *bulk_copy_b(BulkRows *b,const char *p,size_t n){
    if(n==SIZE_MAX){errno=EOVERFLOW;return NULL;}
    if(bulk_take(&b->memory,n+1))return NULL;
    return bulk_copy(p,n);
}
static int bulk_add(BulkRows *b,char *key,size_t kn,char *json,
    const char *line,size_t ln,const Schema *sc){
    if(((!key||!kn)&&(b->update||sc->auto_key==AK_NONE))||kn>(sc->auto_key==AK_NONE?(size_t)sc->max_key:MAX_KEY_CEILING)||(!json&&!line)){
        if(b->update)b->skipped++;else b->errors++;
        free(key);free(json);return 0;
    }
    if(b->count==b->cap){
        size_t cap=b->cap?b->cap*2:32;
        if(cap<b->cap||cap>SIZE_MAX/sizeof(*b->rows))return bulk_fail(EOVERFLOW);
        if(bulk_take(&b->memory,cap*sizeof(*b->rows)))return -1;
        BulkRow *r=realloc(b->rows,cap*sizeof(*r));if(!r)return -1;
        b->rows=r;b->cap=cap;
    }
    uint8_t hash[16]={0};if(key&&kn)compute_hash_raw(key,kn,hash);
    b->rows[b->count]=(BulkRow){.key=key,.klen=kn,.json=json,.line=line,.line_len=ln,
        .ordinal=b->count,.shard=compute_record_shard(hash,sc->splits)};
    b->count++;return 0;
}
/* Track every update storage key before patch/key-size validation. Partial
 * patches of one key must not compose independently against the same OLD. */
static int bulk_track_key(BulkRows *b,const char *key,size_t len){
    if(!key||!len)return 0;
    if(b->nkeys==b->key_cap){
        size_t cap=b->key_cap?b->key_cap*2:32;
        if(cap<b->key_cap||cap>SIZE_MAX/sizeof(*b->keys))return bulk_fail(EOVERFLOW);
        if(bulk_take(&b->memory,cap*sizeof(*b->keys)))return -1;
        BulkKey *p=realloc(b->keys,cap*sizeof(*p));if(!p)return -1;
        b->keys=p;b->key_cap=cap;
    }
    char *copy=bulk_copy_b(b,key,len);if(!copy)return -1;
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
static int bulk_key(BulkRows *b,const char *p,const char *end,char **key,size_t *len){
    *key=NULL;*len=0;
    if(end-p<2||p[0]!='"'||end[-1]!='"')return 0;
    if(bulk_take(&b->memory,(size_t)(end-p-1)))return -1;
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
            if(bulk_key(b,p,end,&key,&kn))return -1;
            v=bulk_space(bulk_space(end)+1);ve=json_skip_value(v);p=ve;
        }else{
            end=json_skip_value(p);
            if(*p=='{'){
                const char *q=bulk_space(p+1);
                while(*q!='}'){
                    const char *ke=json_skip_value(q);char *name=NULL;size_t nn=0;
                    if(bulk_key(b,q,ke,&name,&nn)){free(key);return -1;}
                    const char *x=bulk_space(bulk_space(ke)+1),*xe=json_skip_value(x);
                    if(nn==3&&!memcmp(name,"key",3)){
                        free(key);key=NULL;kn=0;
                        if(bulk_key(b,x,xe,&key,&kn)){free(name);return -1;}
                    }else if(nn==5&&!memcmp(name,"value",5)){v=x;ve=xe;}
                    else if(b->update&&nn==2&&!memcmp(name,"if",2)){
                        free(name);free(key);return bulk_fail(ENOTSUP);
                    }
                    free(name);q=bulk_space(xe);if(*q==',')q=bulk_space(q+1);
                }
            }
            p=end;
        }
        if(bulk_track_key(b,key,kn)){free(key);return -1;}
        if(v&&*v=='{'){
            value=bulk_copy_b(b,v,(size_t)(ve-v));
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
        char *key=bulk_copy_b(b,data+start,key_end-start);if(!key)return -1;
        if(bulk_track_key(b,key,key_end-start)){free(key);return -1;}
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
    if(r->charged){bulk_release(r->memory,r->charged);r->charged=0;}
    size_t source=r->json?strlen(r->json):r->line_len;
    size_t base=(size_t)r->ts->packed.prefix+r->ts->packed.directory;
    if(old&&old->vlen>SIZE_MAX-base){r->error=EOVERFLOW;return -1;}
    if(old)base+=old->vlen;
    if(source>SIZE_MAX-base){r->error=EOVERFLOW;return -1;}base+=source;
    for(int i=0;i<r->ts->nfields;i++){
        const TypedField *f=&r->ts->fields[i];size_t extra=0;
        if(r->ts->packed.fields[i].kind==PKR_FIXED)extra=r->ts->packed.fields[i].width;
        if(f->default_kind==DK_LITERAL)extra+=strlen(f->default_val);
        else if(f->default_kind==DK_RANDOM)extra+=2*(size_t)strtoul(f->default_val,NULL,10);
        else if(f->default_kind!=DK_NONE)extra+=256;
        if(extra>SIZE_MAX-base){r->error=EOVERFLOW;return -1;}base+=extra;
    }
    if(base>(SIZE_MAX-4096)/8){r->error=EOVERFLOW;return -1;}
    size_t reservation=8*base+4096;
    if(bulk_take(r->memory,reservation)){r->error=ENOMEM;return -1;}
    r->charged=reservation;
    TypedRowView view;
    if(old&&typed_row_open(r->ts,old->value,old->vlen,old->hdr2,&view)){
        r->error=EILSEQ;bulk_release(r->memory,r->charged);r->charged=0;return -1;
    }
    size_t len=0;int rc;
    if(cell->update&&!old){r->error=EILSEQ;bulk_release(r->memory,r->charged);r->charged=0;return -1;}
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
        r->error=rc?(errno?errno:EINVAL):E2BIG;free(r->owned);r->owned=NULL;bulk_release(r->memory,r->charged);r->charged=0;return -1;
    }
    if(len>r->charged){r->error=EOVERFLOW;free(r->owned);r->owned=NULL;bulk_release(r->memory,r->charged);r->charged=0;return -1;}
    bulk_release(r->memory,r->charged-len);r->charged=len;
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
        OUT("{\"key\":");
        if(r->sc&&r->sc->auto_key!=AK_NONE){
            char wire[MAX_KEY_CEILING+1];int n=format_wire_key(r->sc,r->key,r->klen,wire,sizeof(wire));
            if(n<0)bulk_quote("",0);else bulk_quote(wire,(size_t)n);
        }else bulk_quote(r->key,r->klen);
        const char *why=r->error==ENOMEM?"out of memory":r->error==EILSEQ?
            "invalid OLD record":r->error==E2BIG?"record exceeds storage bound":
            r->error==EIO?"bulk update failed":"invalid packed update";
        OUT(",\"error\":\"%s\"}",why);
    }
    if(!first)OUT("]");
    if(global_error)OUT(",\"error\":\"bulk update failed; outcome may be partial\"");
    OUT("}\n");
}
static int bulk_row_key_order(const void *a,const void *b){
    const BulkRow *x=*(const BulkRow*const*)a,*y=*(const BulkRow*const*)b;
    size_t n=x->klen<y->klen?x->klen:y->klen;int c=memcmp(x->key,y->key,n);
    return c?c:x->klen<y->klen?-1:x->klen>y->klen;
}
static int bulk_normalize(BulkRows *b,const Schema *sc,const char *root,const char *object){
    if(sc->auto_key!=AK_NONE&&sc->auto_key!=AK_UUID&&sc->auto_key!=AK_SEQ)return bulk_fail(EINVAL);
    size_t missing=0;for(size_t i=0;i<b->count;i++)if(!b->rows[i].klen)missing++;
    if(missing&&(b->update||sc->auto_key==AK_NONE))return bulk_fail(EINVAL);
    if(missing>INT_MAX)return bulk_fail(EOVERFLOW);
    uint8_t *uuids=NULL;long long sequence=0;
    if(missing&&sc->auto_key==AK_UUID){
        if(missing>SIZE_MAX/16||bulk_take(&b->memory,16*missing))return -1;
        uuids=malloc(16*missing);
        if(!uuids)return -1;
        if(gen_uuid4_batch(uuids,(int)missing)){free(uuids);return bulk_fail(EIO);}
    }else if(missing){
        sequence=seq_next_val_batch(root,object,sc->auto_key_seq_name,(int)missing);
        if(sequence<0||(uint64_t)sequence>(uint64_t)INT64_MAX-(missing-1)){free(uuids);return bulk_fail(EIO);}
    }
    size_t generated=0;
    for(size_t i=0;i<b->count;i++){
        BulkRow *r=&b->rows[i];
        if(sc->auto_key!=AK_NONE){
            uint8_t bytes[16];size_t width=sc->auto_key==AK_UUID?16:8;
            if(r->klen&&strlen(r->key)!=r->klen){free(uuids);return bulk_fail(EINVAL);}
            if(sc->auto_key==AK_UUID){
                if(!r->klen)memcpy(bytes,uuids+16*generated++,16);
                else if(parse_uuid_string(r->key,bytes)){free(uuids);return bulk_fail(EINVAL);}
            }else{
                int64_t number;
                if(!r->klen)number=(int64_t)(sequence+(long long)generated++);
                else if(parse_seq_key(r->key,&number)){free(uuids);return bulk_fail(EINVAL);}
                for(int j=7;j>=0;j--){bytes[j]=(uint8_t)(uint64_t)number;number=(int64_t)((uint64_t)number>>8);}
            }
            if(width>(size_t)sc->max_key||bulk_take(&b->memory,width+1)){free(uuids);return -1;}
            char *key=bulk_copy((const char*)bytes,width);if(!key){free(uuids);return -1;}
            free(r->key);r->key=key;r->klen=width;
        }
        uint8_t hash[16];compute_hash_raw(r->key,r->klen,hash);
        r->shard=compute_record_shard(hash,sc->splits);r->memory=&b->memory;
    }
    free(uuids);
    if(b->count>1){
        if(b->count>SIZE_MAX/sizeof(BulkRow*)||bulk_take(&b->memory,b->count*sizeof(BulkRow*)))return -1;
        BulkRow **order=malloc(b->count*sizeof(*order));if(!order)return -1;
        for(size_t i=0;i<b->count;i++)order[i]=&b->rows[i];
        qsort(order,b->count,sizeof(*order),bulk_row_key_order);
        int duplicate=0;for(size_t i=1;i<b->count;i++)if(!bulk_row_key_order(&order[i-1],&order[i])){duplicate=1;break;}
        free(order);if(duplicate)return bulk_fail(EEXIST);
    }
    return 0;
}
static int bulk_ordinal_order(const void *a,const void *b){
    const BulkRow *x=a,*y=b;return x->ordinal<y->ordinal?-1:x->ordinal>y->ordinal;
}
static int bulk_insert_response(BulkRows *b,const Schema *sc,size_t inserted,size_t skipped,size_t errors){
    OUT("{\"inserted\":%zu",inserted);
    if(skipped||errors)OUT(",\"skipped\":%zu",skipped);
    if(errors)OUT(",\"errors\":%zu,\"error\":\"some_records_dropped\"",errors);
    if(sc->auto_key!=AK_NONE){
        qsort(b->rows,b->count,sizeof(*b->rows),bulk_ordinal_order);OUT(",\"keys\":[");
        for(size_t i=0;i<b->count;i++){
            char wire[MAX_KEY_CEILING+1];int n=format_wire_key(sc,b->rows[i].key,b->rows[i].klen,wire,sizeof(wire));
            if(n<0)return 1;
            if(i)OUT(",");
            bulk_quote(wire,(size_t)n);
        }
        OUT("]");
    }
    OUT("}\n");FILE *out=g_out?g_out:stdout;
    return errors||ferror(out)||fflush(out)?1:0;
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
    if(load_index_fields(root,object,indexes,MAX_FIELDS)>0){
        errno=ENOTSUP;OUT("{\"error\":\"packed bulk indexes await R2 integration\"}\n");return 1;
    }
    BulkRows b={.update=update};b.memory.limit=g_query_buffer_max_bytes;
    atomic_init(&b.memory.used,0);char *json=NULL;
    QueryDeadline dl={now_ms_coarse(),resolve_timeout_ms(),0};
    int rc;
    if(len==SIZE_MAX||bulk_take(&b.memory,len+1))goto fail;
    if(!format){
        if(memchr(data,0,len)){errno=EINVAL;goto fail;}
        json=bulk_copy_b(&b,data,len);if(!json)goto fail;
        rc=bulk_json(&b,json,sc,&dl);
    }else rc=bulk_delimited(&b,data,len,sc,delimiter,&dl);
    if(rc||bulk_expired(&dl)){if(!rc)errno=ETIMEDOUT;goto fail;}
    if(bulk_check_duplicates(&b)||bulk_normalize(&b,sc,root,object))goto fail;
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
    size_t metadata=b.count*(sizeof(SlotcaskBulkRec)+sizeof(BulkCell));
    if(groups>SIZE_MAX/sizeof(SlotcaskBulkShardInput)||metadata>SIZE_MAX-groups*sizeof(SlotcaskBulkShardInput)||
       bulk_take(&b.memory,metadata+groups*sizeof(SlotcaskBulkShardInput)))goto fail;
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
````````

<!-- R1-BLOCK s2e82-old -->
````````text
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
````````

<!-- R1-BLOCK s2e82-new -->
````````c
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
    if(pending){bulk_free(&b);free(json);OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");return 1;}
    int response=bulk_insert_response(&b,sc,inserted,skipped,errors);
    bulk_free(&b);free(json);return response;
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
static int bulk_read_source(const char *path,char **out,size_t *length){
    *out=NULL;*length=0;
    int fd=path?open(path,O_RDONLY):dup(STDIN_FILENO);if(fd<0)return -1;
    size_t limit=g_query_buffer_max_bytes,capacity=limit/4<16384?limit/4:16384;
    if(capacity<2){close(fd);return bulk_fail(ENOMEM);}
    char *data=malloc(capacity);if(!data){close(fd);return -1;}
    size_t used=0;int error=0;
    for(;;){
        if(used==capacity-1){
            if(capacity==limit){char extra;ssize_t n;do{n=read(fd,&extra,1);}while(n<0&&errno==EINTR);
                if(n)error=n<0?errno:ENOMEM;
                break;}
            size_t next=capacity>limit/2?limit:capacity*2;
            /* Account for both buffers at a possible moving realloc. */
            if(next>limit-capacity){error=ENOMEM;break;}
            char *grown=realloc(data,next);if(!grown){error=ENOMEM;break;}data=grown;capacity=next;
        }
        ssize_t n=read(fd,data+used,capacity-1-used);
        if(n<0&&errno==EINTR)continue;
        if(n<0){error=errno;break;}if(!n)break;used+=(size_t)n;
    }
    if(close(fd)&&!error)error=errno?errno:EIO;
    if(error){free(data);return bulk_fail(error);}
    if(used+1<capacity){
        if(used+1>limit-capacity){free(data);return bulk_fail(ENOMEM);}
        char *exact=realloc(data,used+1);if(!exact){free(data);return -1;}data=exact;
    }
    data[used]=0;*out=data;*length=used;return 0;
}
int typed_bulk_insert_source(const char *root,const char *object,const Schema *sc,
    const TypedSchema *ts,const char *input,int format,char delimiter,int if_not_exists){
    char *data=NULL;size_t len=0;
    if(bulk_read_source(input,&data,&len)){
        OUT("{\"error\":\"cannot read packed insert source within query memory limit\"}\n");return 1;}
    int rc=typed_bulk_insert_command(root,object,sc,ts,NULL,data,len,format,delimiter,if_not_exists);
    int saved=errno;free(data);errno=saved;return rc;
}

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
    len=(size_t)st.st_size;
    if(len>=g_query_buffer_max_bytes){saved=ENOMEM;goto failed;}
    data=malloc(len+1);if(!data){saved=ENOMEM;goto failed;}
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
````````

<!-- R1-BLOCK s2e83-old -->
````````text
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
````````

<!-- R1-BLOCK s2e83-new -->
````````c
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
int typed_bulk_insert_source(const char *root,const char *object,const Schema *sc,
    const TypedSchema *ts,const char *input,int format,char delimiter,int if_not_exists);
#endif
````````

<!-- R1-BLOCK s2e84-old -->
````````text
#include "typed_read_command.h"
#include <errno.h>
int typed_read_command(SlotcaskDb*d,const FieldSchema*f,const Schema*s,const TypedReadOptions*o,FILE*out){(void)d;(void)f;(void)s;(void)o;if(out)fputs("{\"error\":\"not implemented\"}\n",out);errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s2e84-new -->
````````c
#include "typed_read_command.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <stdatomic.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
typedef struct ReadPart {struct ReadPart *next;char *data;size_t len;} ReadPart;
typedef struct {
    const FieldSchema *fs;const Schema *schema;const TypedReadOptions *options;
    ReadPart *head,*tail;size_t retained,count;int error,rows,dict,csv,found;
    int64_t skip,start;uint64_t began;const void *key;size_t keylen;
    const char *wire;size_t wirelen;
    char *selection,*legacy;size_t legacy_charge;
    const char *names[MAX_FIELDS];int indices[MAX_FIELDS];size_t nfields;
    const char *legacy_values[MAX_FIELDS];size_t legacy_lengths[MAX_FIELDS];
} ReadCommand;
static int read_error(FILE *out,int code){
    if(!out){errno=code;return -1;}
    const char *message=code==ENOMEM||code==ENOBUFS?
        "{\"error\":\"query memory buffer exceeded or allocation failed\"}\n":
        code==ETIMEDOUT?"{\"error\":\"query timed out\"}\n":
        code==ENOENT?"{\"error\":\"Not found\"}\n":
        code==EIO||code==EILSEQ?"{\"error\":\"corrupt record or storage read failed\"}\n":
        "{\"error\":\"invalid read request\"}\n";
    size_t n=strlen(message);
    if(fwrite(message,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    errno=code;return -1;
}
static void read_free(ReadCommand *c){
    while(c->head){ReadPart *p=c->head;c->head=p->next;free(p->data);free(p);}
    free(c->legacy);c->legacy=NULL;c->legacy_charge=0;
    free(c->selection);c->selection=NULL;c->tail=NULL;c->retained=0;
}
static size_t read_available(ReadCommand *c){
    size_t max=c->options->budget;
    if(c->retained>=max||max-c->retained<=sizeof(ReadPart)){c->error=ENOBUFS;return 0;}
    return max-c->retained-sizeof(ReadPart);
}
static int read_owned_size(ReadCommand *c,char *data,size_t n,size_t allocation){
    if(!data){c->error=errno?errno:EIO;return -1;}
    if(n==SIZE_MAX||n+1>allocation||allocation>read_available(c)){free(data);c->error=ENOBUFS;return -1;}
    ReadPart *p=malloc(sizeof(*p));if(!p){free(data);c->error=ENOMEM;return -1;}
    *p=(ReadPart){.data=data,.len=n};if(c->tail)c->tail->next=p;else c->head=p;
    c->tail=p;c->retained+=sizeof(*p)+allocation;return 0;
}
static int read_owned(ReadCommand *c,char *data,size_t n){
    if(n==SIZE_MAX){free(data);c->error=EOVERFLOW;return -1;}
    return read_owned_size(c,data,n,n+1);
}
static int read_literal(ReadCommand *c,const char *text){
    size_t n=strlen(text);if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(n+1);if(!p){c->error=ENOMEM;return -1;}
    memcpy(p,text,n+1);return read_owned(c,p,n);
}
static int read_quote(ReadCommand *c,const char *s,size_t n){
    size_t need=2;
    for(size_t i=0;i<n;i++){
        unsigned char v=(unsigned char)s[i];size_t k=v=='"'||v=='\\'||v=='\b'||v=='\f'||v=='\n'||v=='\r'||v=='\t'?2:v<32?6:1;
        if(need>SIZE_MAX-k){c->error=EOVERFLOW;return -1;}need+=k;
    }
    if(need==SIZE_MAX||need+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(need+1);if(!p){c->error=ENOMEM;return -1;}
    p[0]='"';int written=json_escape_into(p+1,need-1,s,n);
    if(written<0||(size_t)written!=need-2){free(p);c->error=EILSEQ;return -1;}
    p[need-1]='"';p[need]=0;return read_owned(c,p,need);
}
static int read_deadline(ReadCommand *c){
    if(c->options->timeout_ms&&now_ms_coarse()-c->began>=c->options->timeout_ms){c->error=ETIMEDOUT;return -1;}return 0;
}
static int read_finish(ReadCommand *c,FILE *out){
    if(c->error){int code=c->error;read_free(c);return read_error(out,code);}
    for(ReadPart *p=c->head;p;p=p->next)if(fwrite(p->data,1,p->len,out)!=p->len||ferror(out)){
        read_free(c);errno=EIO;return -1;}
    read_free(c);if(fflush(out)||ferror(out)){errno=EIO;return -1;}return 0;
}
static int read_selection(ReadCommand *c){
    const char *fields=c->options->fields;
    if(!fields||!*fields){
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed)
            {c->names[c->nfields]=c->fs->ts->fields[i].name;c->indices[c->nfields++]=i;}
        return 0;
    }
    size_t n=strnlen(fields,MAX_LINE);
    if(n==MAX_LINE){c->error=E2BIG;return -1;}
    if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    c->selection=malloc(n+1);if(!c->selection){c->error=ENOMEM;return -1;}
    memcpy(c->selection,fields,n+1);c->retained+=n+1;
    char *save=NULL;
    for(char *p=strtok_r(c->selection,",",&save);p;p=strtok_r(NULL,",",&save)){
        if(c->nfields==MAX_FIELDS){c->error=E2BIG;return -1;}c->names[c->nfields]=p;c->indices[c->nfields++]=-1;
    }
    return 0;
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n);
static void read_legacy_close(ReadCommand *c){
    if(c->legacy){free(c->legacy);c->legacy=NULL;c->retained-=c->legacy_charge;c->legacy_charge=0;
        memset(c->legacy_values,0,sizeof(c->legacy_values));memset(c->legacy_lengths,0,sizeof(c->legacy_lengths));}
}
static int read_legacy_open(ReadCommand *c,const void *v,size_t n,uint8_t flags){
    if(c->legacy)return 0;
    size_t available=read_available(c);if(n>INT_MAX||available<8){c->error=ENOBUFS;return -1;}
    size_t reserve=available/2;
    c->legacy=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,reserve,&c->legacy_charge);
    if(!c->legacy){c->error=errno?errno:EILSEQ;return -1;}
    c->retained+=c->legacy_charge;
    const char *p=json_skip(c->legacy);if(*p++!='{'){c->error=EILSEQ;return -1;}
    p=json_skip(p);
    while(*p&&*p!='}'){
        if(*p++!='"'){c->error=EILSEQ;return -1;}
        const char *key=p,*end=json_raw_string_end(key);
        if(*end!='"'||(size_t)(end-key)+1>read_available(c)){c->error=ENOBUFS;return -1;}
        char *name=NULL;size_t name_len=0;
        if(json_unescape_cstring(key,(size_t)(end-key),&name,&name_len)){c->error=errno?errno:EINVAL;return -1;}
        p=json_skip(end+1);if(*p++!=':'){free(name);c->error=EILSEQ;return -1;}p=json_skip(p);
        const char *value=p,*after=json_skip_value(p);
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed&&
            name_len==(size_t)c->fs->ts->fields[i].name_len&&!memcmp(name,c->fs->ts->fields[i].name,name_len)){
            c->legacy_values[i]=value;c->legacy_lengths[i]=(size_t)(after-value);
        }
        free(name);p=json_skip(after);if(*p==',')p=json_skip(p+1);else if(*p!='}'){c->error=EILSEQ;return -1;}
    }
    return 0;
}
static void read_legacy_span(ReadCommand *c,const char *name,size_t name_len,
    const char **value,size_t *length){
    *value="null";*length=4;
    for(int i=0;i<c->fs->ts->nfields;i++)if(name_len==(size_t)c->fs->ts->fields[i].name_len&&
       !memcmp(name,c->fs->ts->fields[i].name,name_len)){
        if(c->legacy_values[i]){*value=c->legacy_values[i];*length=c->legacy_lengths[i];}
        return;
    }
}

static char *read_legacy_component(ReadCommand *c,const char *name,size_t name_len,size_t *len,int *is_null){
    const char *raw=NULL;size_t n=0;*len=0;*is_null=0;read_legacy_span(c,name,name_len,&raw,&n);
    if(n==4&&!memcmp(raw,"null",4)){*is_null=1;return NULL;}
    size_t available=read_available(c);
    if(n==SIZE_MAX||n+1>available/4){c->error=ENOBUFS;return NULL;}
    char *text=NULL;
    if(n>=2&&raw[0]=='"'&&raw[n-1]=='"'){
        if(json_unescape_string(raw+1,n-2,&text,len)){c->error=errno?errno:EINVAL;return NULL;}
    }else{
        text=malloc(n+1);if(!text){c->error=ENOMEM;return NULL;}memcpy(text,raw,n);text[n]=0;*len=n;
    }
    return text;
}
static char *read_legacy_text(ReadCommand *c,const char *name,int index,size_t *len,int *is_null){
    *len=0;*is_null=0;
    if(index>=0||!strchr(name,'+'))return read_legacy_component(c,name,strlen(name),len,is_null);
    size_t total=0,count=0;const char *p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        if(++count>MAX_FIELDS){c->error=E2BIG;return NULL;}
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        free(part);if(c->error)return NULL;
        if(n>SIZE_MAX-total){c->error=EOVERFLOW;return NULL;}total+=n;p=end;
    }
    if(!total){*is_null=1;return NULL;}
    if(total==SIZE_MAX||total+1>read_available(c)/4){c->error=ENOBUFS;return NULL;}
    char *text=malloc(total+1);if(!text){c->error=ENOMEM;return NULL;}
    c->retained+=total+1;size_t at=0;p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        if(c->error){free(part);free(text);c->retained-=total+1;return NULL;}
        if(n)memcpy(text+at,part,n);
        at+=n;free(part);p=end;
    }
    c->retained-=total+1;text[at]=0;*len=at;return text;
}
static int read_legacy_cell(ReadCommand *c,const char *name,int index){
    if(index<0&&strchr(name,'+')){
        size_t n=0;int null=0;char *text=read_legacy_text(c,name,index,&n,&null);
        if(c->error){free(text);return -1;}if(null)return read_literal(c,"null");
        c->retained+=n+1;int rc=read_quote(c,text,n);c->retained-=n+1;free(text);return rc;
    }
    const char *raw=NULL;size_t n=0;read_legacy_span(c,name,strlen(name),&raw,&n);
    int i=index>=0?index:typed_field_index(c->fs->ts,name);
    if(i>=0&&c->fs->ts->fields[i].type==FT_BOOL&&!(n==4&&!memcmp(raw,"null",4)))return read_quote(c,raw,n);
    if(n==SIZE_MAX||n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *copy=malloc(n+1);if(!copy){c->error=ENOMEM;return -1;}memcpy(copy,raw,n);copy[n]=0;
    return read_owned(c,copy,n);
}
static int read_csv_header(ReadCommand *c){
    if(read_literal(c,"key"))return -1;
    if(c->options->kind==TYPED_READ_EXISTS_MULTI){char tail[16];snprintf(tail,sizeof(tail),"%cexists\n",c->options->delimiter);return read_literal(c,tail);}
    if(c->options->kind==TYPED_READ_KEYS||c->options->kind==TYPED_READ_NOT_EXISTS)return read_literal(c,"\n");
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t n=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(c->names[i],strlen(c->names[i]),c->options->delimiter,available,&n):NULL;
        if(read_owned(c,cell,n))return -1;
    }
    return read_literal(c,"\n");
}
static int read_legacy_csv_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(read_legacy_open(c,v,n,flags)||read_csv_key(c,wire,wirelen))return -1;
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t length=0;int null=0;char *text=read_legacy_text(c,c->names[i],c->indices[i],&length,&null);
        if(c->error){free(text);return -1;}if(null)continue;
        c->retained+=length+1;size_t used=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(text,length,c->options->delimiter,available,&used):NULL;
        c->retained-=length+1;free(text);if(read_owned(c,cell,used))return -1;
    }
    return read_literal(c,"\n");
}

static int read_value(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *name,int index){
    size_t available=read_available(c);if(!available)return -1;
    if(!c->fs->ts->packed.nfields){
        if(name||(c->options->fields&&*c->options->fields)){
            if(read_legacy_open(c,v,n,flags))return -1;
            if(name)return read_legacy_cell(c,name,index);
            if(read_literal(c,"{"))return -1;
            for(size_t i=0;i<c->nfields;i++){
                if((i&&read_literal(c,","))||read_quote(c,c->names[i],strlen(c->names[i]))||
                    read_literal(c,":")||read_legacy_cell(c,c->names[i],c->indices[i]))return -1;
            }
            return read_literal(c,"}");
        }
        if(n>INT_MAX){c->error=EOVERFLOW;return -1;}
        size_t allocated=0;char *text=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,available,&allocated);
        return read_owned_size(c,text,text?strlen(text):0,allocated);
    }
    size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,available);
    if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
    char *text=name?(index>=0?
        typed_packed_projected_cell(c->fs->ts,v,n,flags,(size_t)index,allowance):
        typed_packed_named_json(c->fs->ts,v,n,flags,name,allowance)):
        c->options->fields&&*c->options->fields?
        typed_packed_project_json(c->fs->ts,v,n,flags,c->options->fields,allowance):
        typed_packed_decode(c->fs->ts,v,n,flags,allowance);
    return read_owned(c,text,text?strlen(text):0);
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n){
    size_t available=read_available(c),used=0;
    char *cell=available?typed_csv_cell_text(wire,n,c->options->delimiter,available,&used):NULL;
    return read_owned(c,cell,used);
}
static int read_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(c->csv&&c->options->kind!=TYPED_READ_KEYS){
        if(c->fs->ts->packed.nfields){
            size_t available=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,read_available(c)),used=0;
            char *text=available?typed_packed_csv_row(c->fs->ts,v,n,flags,wire,wirelen,c->options->fields,c->options->delimiter,available,&used):NULL;
            return read_owned(c,text,used);
        }
        return read_legacy_csv_row(c,v,n,flags,wire,wirelen);
    }
    if(c->count&&!c->csv&&read_literal(c,","))return -1;
    if(c->options->kind==TYPED_READ_KEYS){
        if(c->csv)return read_csv_key(c,wire,wirelen)||read_literal(c,"\n");
        return read_quote(c,wire,wirelen);
    }
    if(c->rows){
        if(c->fs->ts->packed.nfields){
            size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,read_available(c));
            if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
            char *row=typed_packed_row_json(c->fs->ts,v,n,flags,wire,wirelen,c->options->fields,allowance);
            return read_owned(c,row,row?strlen(row):0);
        }
        if(read_legacy_open(c,v,n,flags))return -1;
        if(read_literal(c,"[")||read_quote(c,wire,wirelen))return -1;
        for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_value(c,v,n,flags,c->names[i],c->indices[i]))return -1;
        return read_literal(c,"]");
    }
    if(!c->dict&&read_literal(c,"{\"key\":"))return -1;
    if(read_quote(c,wire,wirelen)||read_literal(c,c->dict?":":",\"value\":"))return -1;
    if(read_value(c,v,n,flags,NULL,-1))return -1;
    return c->dict?0:read_literal(c,"}");
}
static int read_walk(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;if(read_deadline(c))return 1;
    if(c->skip){c->skip--;return 0;}
    char wire[MAX_KEY_CEILING+1];
    if(!klen||klen>MAX_KEY_CEILING){c->error=EILSEQ;return 1;}
    int n=format_wire_key(c->schema,key,klen,wire,sizeof(wire));
    if(n<0){c->error=EILSEQ;return 1;}
    int rc=read_row(c,value,vlen,flags,wire,(size_t)n);read_legacy_close(c);
    if(rc)return 1;
    c->count++;return c->count>=(size_t)c->options->limit;
}
static int read_lookup(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;
    if(klen!=c->keylen||memcmp(key,c->key,klen))return 0;
    c->found=1;
    if(c->options->kind==TYPED_READ_GET_MULTI){
        if(c->csv)read_row(c,value,vlen,flags,c->wire,c->wirelen);
        else read_value(c,value,vlen,flags,NULL,-1);
        read_legacy_close(c);
    }
    return 1;
}
static int read_offset(const TypedReadOptions *o,int64_t *skip){
    uint64_t n=0;
    for(const unsigned char *p=(const unsigned char*)(o->cursor?o->cursor:"");*p;p++){
        if(*p<'0'||*p>'9'||n>((uint64_t)INT64_MAX-(*p-'0'))/10){errno=EINVAL;return -1;}
        n=n*10+(*p-'0');
    }
    uint64_t add=o->offset>0?(uint64_t)o->offset:0;
    if(n>(uint64_t)INT64_MAX-add){errno=EINVAL;return -1;}
    *skip=(int64_t)(n+add);return 0;
}
static int read_open(ReadCommand *c,const FieldSchema *fs,const Schema *sc,const TypedReadOptions *o,FILE *out){
    memset(c,0,sizeof(*c));c->fs=fs;c->schema=sc;c->options=o;
    if(!fs||!sc||!o||!out||((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&(!fs->ts||!fs->ts->typed))){c->error=EINVAL;return -1;}
    if(o->kind<TYPED_READ_FETCH||o->kind>TYPED_READ_NOT_EXISTS||
       ((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&
        (fs->ts->nfields<1||fs->ts->nfields>MAX_FIELDS))){c->error=EINVAL;return -1;}
    c->began=now_ms_coarse();c->dict=o->format&&!strcmp(o->format,"dict");
    c->rows=o->format&&!strcmp(o->format,"rows");
    c->csv=o->format&&!strcmp(o->format,"csv");
    unsigned char separator=(unsigned char)o->delimiter;
    if(c->csv&&separator!='\t'&&(separator<32||separator>126||separator=='"'||separator=='\\')){
        c->error=EINVAL;return -1;
    }
    if((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&read_selection(c))return -1;
    if(o->kind==TYPED_READ_FETCH){
        if(o->limit<=0||read_offset(o,&c->skip)){c->error=EINVAL;return -1;}c->start=c->skip;
        if(c->csv&&o->want_total){c->error=ENOTSUP;return -1;}
    }
    if(c->csv)return read_csv_header(c);
    if(o->kind==TYPED_READ_FETCH){
        if(c->rows){
            if(read_literal(c,"{\"columns\":[\"key\""))return -1;
            for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_quote(c,c->names[i],strlen(c->names[i])))return -1;
            return read_literal(c,"],\"rows\":[");
        }
        return read_literal(c,o->want_total?(c->dict?"{\"rows\":{":"{\"rows\":["):
            (c->dict?"{\"results\":{":"{\"results\":["));
    }
    if(c->csv){
        if(o->kind!=TYPED_READ_EXISTS_MULTI)return read_literal(c,"key\n");
        char header[16];snprintf(header,sizeof(header),"key%cexists\n",o->delimiter);
        return read_literal(c,header);
    }
    return read_literal(c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"[":"{");
}
/* Retain the existing one-worker-per-touched-shard dispatch and batched
 * lookup/resolve-fetch paths. Only ownership, bounds and error status change. */
typedef struct {size_t limit;_Atomic size_t used;} ReadMemory;
static int batch_take(ReadMemory *m,size_t n){
    size_t used=atomic_load(&m->used);
    do{if(used>m->limit||n>m->limit-used){errno=ENOBUFS;return -1;}}
    while(!atomic_compare_exchange_weak(&m->used,&used,used+n));
    return 0;
}
static void batch_release(ReadMemory *m,size_t n){atomic_fetch_sub(&m->used,n);}
typedef struct {
    char *wire;size_t wirelen,wirecharge,klen;uint8_t binary[16],hash[16];
    int shard,found;ReadPart *head,*tail;size_t retained;
} ReadEntry;
typedef struct {
    SlotcaskDb *db;ReadCommand *parent;ReadMemory *memory;
    ReadEntry **entries;size_t count;int error;
} ReadShard;
static const void *batch_key(const ReadEntry *e,const Schema *sc){return sc->auto_key==AK_NONE?(const void*)e->wire:(const void*)e->binary;}
static void batch_parts_free(ReadEntry *e){
    while(e->head){ReadPart *p=e->head;e->head=p->next;free(p->data);free(p);}
    e->tail=NULL;e->retained=0;
}
static uint32_t batch_u32(const uint8_t *p){return (uint32_t)p[0]<<24|(uint32_t)p[1]<<16|(uint32_t)p[2]<<8|p[3];}
/* No allocation: reserve output plus actual numeric/display workspace before
 * any validator/renderer allocates. This scan is never a validity substitute. */
static int batch_render_cost(const ReadCommand *c,const uint8_t *value,size_t len,size_t *cost){
    const TypedSchema *ts=c->fs->ts;size_t text=64,workspace=0;
    if(ts->packed.nfields){
        if(pkr_schema_validate(&ts->packed)!=DEC_OK||ts->packed.nfields!=(uint32_t)ts->nfields)return -1;
        if(!value||len<(size_t)ts->packed.prefix+ts->packed.directory)return -1;
        for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];const PkrField *pf=&ts->packed.fields[i];
            size_t part=512;
            if(pf->kind!=PKR_FIXED){
                const uint8_t *d=value+ts->packed.prefix+pf->offset;
                size_t off=batch_u32(d),n=batch_u32(d+4);
                if(n>pf->payload_max||off>len-ts->packed.prefix||n>len-ts->packed.prefix-off)return -1;
                if(pf->kind==PKR_VARCHAR){if(n>(SIZE_MAX-4)/6)return -1;part=6*n+4;}
                else if(n){
                    if(n<16)return -1;
                    const uint8_t *p=value+ts->packed.prefix+off;
                    int64_t nd=batch_u32(p+4),m=(int32_t)batch_u32(p+8),scale=batch_u32(p+12);
                    if(nd>DEC_MAX_COEFF_DIGITS||scale>DEC_MAX_DSCALE||nd+m>DEC_MAX_INT_DIGITS)return -1;
                    int64_t integer=nd+m;if(integer<1)integer=1;
                    part=(size_t)integer+(size_t)scale+4;
                    if(n>(SIZE_MAX-128)/8)return -1;
                    size_t needed=8*n+128;if(needed>workspace)workspace=needed;
                }else part=4;
            }else if(f->type==FT_ENUM){
                size_t longest=0;
                for(int j=0;f->enum_values&&j<f->n_enum_values;j++)if(f->enum_values[j]){
                    size_t n=strlen(f->enum_values[j]);if(n>longest)longest=n;
                }
                if(longest>(SIZE_MAX-4)/6)return -1;
                part=6*longest+4;
            }
            size_t name=strnlen(f->name,sizeof(f->name));
            if(part>SIZE_MAX-text||6*name+16>SIZE_MAX-text-part)return -1;
            text+=part+6*name+16;
        }
    }else{
        if(len>(SIZE_MAX-64)/6)return -1;
        text+=6*len;
        for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];size_t part=512;
            if(f->type==FT_ENUM)for(int j=0;f->enum_values&&j<f->n_enum_values;j++)if(f->enum_values[j]){
                size_t n=strlen(f->enum_values[j]);if(n>(SIZE_MAX-4)/6)return -1;
                if(6*n+4>part)part=6*n+4;
            }
            size_t name=strnlen(f->name,sizeof(f->name));
            if(part>SIZE_MAX-text||6*name+16>SIZE_MAX-text-part)return -1;
            text+=part+6*name+16;
        }
        workspace=text;
    }
    size_t extra=4096+(size_t)ts->nfields*96+c->wirelen*6;
    if(c->options->fields){size_t n=strlen(c->options->fields);if(n>SIZE_MAX-extra-1)return -1;extra+=n+1;}
    if(text>(SIZE_MAX-workspace)/2||extra>SIZE_MAX-workspace-2*text)return -1;
    *cost=2*text+workspace+extra;return 0;
}
static int batch_fetch(const uint8_t hash[16],const void *key,size_t klen,const void *value,size_t len,uint8_t flags,void *ptr){
    ReadShard *w=ptr;
    for(size_t i=0;i<w->count&&!w->error;i++){
        ReadEntry *e=w->entries[i];
        if(e->found||memcmp(e->hash,hash,16)||e->klen!=klen||memcmp(batch_key(e,w->parent->schema),key,klen))continue;
        TypedReadOptions options=*w->parent->options;
        ReadCommand row={.fs=w->parent->fs,.schema=w->parent->schema,.options=&options,
            .began=w->parent->began,.key=key,.keylen=klen,.wire=e->wire,.wirelen=e->wirelen,.csv=w->parent->csv};
        size_t reserve=0;
        if(batch_render_cost(&row,value,len,&reserve)){w->error=EILSEQ;break;}
        if(batch_take(w->memory,reserve)){w->error=errno;break;}
        options.budget=reserve;
        if(read_selection(&row)||read_deadline(&row)){}
        else read_lookup(hash,key,klen,value,len,flags,&row);
        read_legacy_close(&row);
        size_t selection=row.selection?strlen(options.fields)+1:0;
        free(row.selection);row.selection=NULL;row.retained-=selection;
        if(row.error){w->error=row.error;read_free(&row);batch_release(w->memory,reserve);break;}
        e->found=row.found;e->head=row.head;e->tail=row.tail;e->retained=row.retained;
        batch_release(w->memory,reserve-row.retained);
    }
    return w->error?1:0;
}
static void *batch_shard_worker(void *ptr){
    ReadShard *w=ptr;if(!w->count)return NULL;
    if(w->parent->options->kind==TYPED_READ_GET_MULTI){
        uint8_t (*hashes)[16]=malloc(w->count*sizeof(*hashes));
        if(!hashes){w->error=ENOMEM;return NULL;}
        for(size_t i=0;i<w->count;i++)memcpy(hashes[i],w->entries[i]->hash,16);
        if(slotcask_bulk_resolve_and_fetch_checked(w->db,hashes,w->count,w,batch_fetch)<0&&!w->error)w->error=errno?errno:EIO;
        free(hashes);
    }else{
        SlotcaskBulkRec *records=calloc(w->count,sizeof(*records));
        if(!records){w->error=ENOMEM;return NULL;}
        for(size_t i=0;i<w->count;i++){records[i].key=batch_key(w->entries[i],w->parent->schema);records[i].klen=w->entries[i]->klen;}
        if(slotcask_bulk_lookup_in_kfshard_checked(w->db,w->entries[0]->shard,records,w->count)<0)w->error=errno?errno:EIO;
        else for(size_t i=0;i<w->count;i++){
            if(records[i].status==-1){w->error=EIO;break;}
            w->entries[i]->found=records[i].status==0;
        }
        free(records);
    }
    return NULL;
}
static int read_keys_json(ReadCommand *c,SlotcaskDb *db){
    const Schema *sc=c->schema;const char *json=c->options->keys;
    if(!json||typed_json_validate(json)||sc->max_key<1||sc->max_key>MAX_KEY_CEILING||sc->splits<1||sc->splits!=db->num_shards||sc->splits>MAX_SPLITS){c->error=EINVAL;return -1;}
    if(c->csv&&c->options->kind==TYPED_READ_GET_MULTI&&c->options->fields){c->error=EINVAL;return -1;}
    ReadMemory memory={.limit=c->options->budget};atomic_init(&memory.used,c->retained);
    ReadEntry *entries=NULL;size_t count=0,capacity=0;ReadEntry **ordered=NULL;
    size_t *counts=NULL,*starts=NULL,*fill=NULL;ReadShard *workers=NULL;size_t nworkers=0;
    const char *p=json_skip(json);if(*p++!='['){c->error=EINVAL;return -1;}p=json_skip(p);
    while(*p!=']'&&!c->error){
        if(read_deadline(c))break;
        if(*p!='"'){c->error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t raw=(size_t)(end-start);
        size_t maxwire=sc->auto_key==AK_NONE?(size_t)sc->max_key:MAX_KEY_CEILING;
        if(*end!='"'||raw>6*maxwire){c->error=E2BIG;break;}
        if(count==capacity){
            size_t next=capacity?capacity*2:16;
            if(next<capacity||next>SIZE_MAX/sizeof(*entries)||batch_take(&memory,next*sizeof(*entries))){c->error=ENOBUFS;break;}
            ReadEntry *grown=calloc(next,sizeof(*entries));if(!grown){c->error=ENOMEM;break;}
            if(count)memcpy(grown,entries,count*sizeof(*entries));
            free(entries);batch_release(&memory,capacity*sizeof(*entries));entries=grown;capacity=next;
        }
        ReadEntry *e=&entries[count];
        if(batch_take(&memory,raw+1)){c->error=errno;break;}
        if(json_unescape_string(start,raw,&e->wire,&e->wirelen)){c->error=errno?errno:EINVAL;break;}
        e->wirecharge=raw+1;count++;
        if(!e->wirelen||e->wirelen>maxwire){c->error=EINVAL;break;}
        e->klen=e->wirelen;
        if(sc->auto_key==AK_UUID){if(strlen(e->wire)!=e->wirelen||parse_uuid_string(e->wire,e->binary))c->error=EINVAL;e->klen=16;}
        else if(sc->auto_key==AK_SEQ){
            int64_t n;if(strlen(e->wire)!=e->wirelen||parse_seq_key(e->wire,&n))c->error=EINVAL;
            else for(int j=7;j>=0;j--){e->binary[j]=(uint8_t)(uint64_t)n;n=(int64_t)((uint64_t)n>>8);}
            e->klen=8;
        }else if(sc->auto_key!=AK_NONE)c->error=EINVAL;
        if(e->klen>(size_t)sc->max_key)c->error=E2BIG;
        if(c->error)break;
        compute_hash_raw(batch_key(e,sc),e->klen,e->hash);e->shard=compute_record_shard(e->hash,sc->splits);
        p=json_skip(end+1);if(*p==',')p=json_skip(p+1);else if(*p!=']')c->error=EINVAL;
    }
    if(c->error||!count)goto done;
    size_t arrays=(size_t)sc->splits*3*sizeof(size_t)+count*sizeof(*ordered);
    if(batch_take(&memory,arrays)){c->error=errno;goto done;}
    counts=calloc((size_t)sc->splits,sizeof(*counts));starts=calloc((size_t)sc->splits,sizeof(*starts));fill=calloc((size_t)sc->splits,sizeof(*fill));ordered=malloc(count*sizeof(*ordered));
    if(!counts||!starts||!fill||!ordered){c->error=ENOMEM;goto done;}
    for(size_t i=0;i<count;i++)counts[entries[i].shard]++;
    size_t total=0;
    for(int i=0;i<sc->splits;i++){starts[i]=total;total+=counts[i];if(counts[i])nworkers++;}
    /* Admission covers the existing bulk readers' per-shard metadata while
     * every worker runs with its original dispatch/handle lifetime. */
    size_t per=(size_t)sc->splits*5*sizeof(size_t)+512;
    if(nworkers>SIZE_MAX/per||count>SIZE_MAX/(3*sizeof(SlotcaskResolvedRec)+sizeof(SlotcaskBulkRec)+32)){c->error=ENOBUFS;goto done;}
    size_t core=nworkers*per+count*(3*sizeof(SlotcaskResolvedRec)+sizeof(SlotcaskBulkRec)+32);
    if(batch_take(&memory,core)||batch_take(&memory,nworkers*sizeof(*workers))){c->error=errno;goto done;}
    workers=calloc(nworkers,sizeof(*workers));if(!workers){c->error=ENOMEM;goto done;}
    for(size_t i=0;i<count;i++)ordered[starts[entries[i].shard]+fill[entries[i].shard]++]=&entries[i];
    size_t wi=0;
    for(int i=0;i<sc->splits;i++)if(counts[i])workers[wi++]=(ReadShard){db,c,&memory,ordered+starts[i],counts[i],0};
    parallel_for_io(batch_shard_worker,workers,(int)nworkers,sizeof(*workers));
    for(size_t i=0;i<nworkers;i++)if(workers[i].error&&!c->error)c->error=workers[i].error;
    free(workers);workers=NULL;free(ordered);ordered=NULL;free(counts);counts=NULL;free(starts);starts=NULL;free(fill);fill=NULL;
    if(c->error)goto done;
    size_t metadata=capacity*sizeof(*entries),results=0;
    for(size_t i=0;i<count;i++){metadata+=entries[i].wirecharge;results+=entries[i].retained;}
    c->retained+=metadata+results;
    for(size_t i=0;i<count&&!c->error;i++){
        ReadEntry *e=&entries[i];
        if(c->options->kind!=TYPED_READ_NOT_EXISTS){
            if(c->count&&!c->csv&&read_literal(c,","))break;
            if(c->csv&&c->options->kind==TYPED_READ_GET_MULTI&&e->found){}
            else if(c->csv){
                if(read_csv_key(c,e->wire,e->wirelen))break;
                char separator[2]={c->options->delimiter,0};
                if((c->options->kind==TYPED_READ_EXISTS_MULTI||c->nfields)&&read_literal(c,separator))break;
            }else if(read_quote(c,e->wire,e->wirelen)||read_literal(c,":"))break;
        }
        if(c->options->kind==TYPED_READ_GET_MULTI){
            if(e->found){
                if(c->tail)c->tail->next=e->head;else c->head=e->head;
                if(e->tail)c->tail=e->tail;
                e->head=e->tail=NULL;
            }else if(c->csv){
                char separator[2]={c->options->delimiter,0};
                /* The first separator was emitted with the missing key. */
                for(size_t j=1;j<c->nfields&&!c->error;j++)read_literal(c,separator);
                read_literal(c,"\n");
            }else read_literal(c,"null");
            c->count++;
        }else if(c->options->kind==TYPED_READ_EXISTS_MULTI){read_literal(c,e->found?"true":"false");if(c->csv)read_literal(c,"\n");c->count++;}
        else if(!e->found){if(c->count&&!c->csv)read_literal(c,",");if(c->csv){read_csv_key(c,e->wire,e->wirelen);read_literal(c,"\n");}else read_quote(c,e->wire,e->wirelen);c->count++;}
    }
    c->retained-=metadata;
done:
    for(size_t i=0;i<count;i++){free(entries[i].wire);batch_parts_free(&entries[i]);}
    free(entries);free(ordered);free(counts);free(starts);free(fill);free(workers);
    return c->error?-1:0;
}
int typed_read_command(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const TypedReadOptions *o,FILE *out){
    if(o&&o->format&&!strcmp(o->format,"csv")&&fs&&fs->ts&&fs->ts->packed.nfields){
        PackedCsvOptions csv={.fields=o->fields,.delimiter=o->delimiter,.buffer_budget=o->budget,.timeout_ms=o->timeout_ms};
        if(o->kind==TYPED_READ_FETCH)return packed_csv_fetch(db,fs,o->offset,o->limit,o->cursor,o->want_total,&csv,out);

    }
    ReadCommand c;if(read_open(&c,fs,schema,o,out))return read_finish(&c,out);
    if(!db){c.error=EIO;return read_finish(&c,out);}
    if(o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_KEYS){
        if(o->limit<=0){c.error=EINVAL;}
        else{
            if(o->kind==TYPED_READ_KEYS)c.skip=o->offset>0?o->offset:0;
            int rc=slotcask_walk_live_checked(db,read_walk,&c);
            if(rc<0&&!c.error)c.error=EIO;
        }
    }else read_keys_json(&c,db);
    if(!c.error)read_deadline(&c);
    if(!c.error){
        if(o->kind==TYPED_READ_FETCH){
            if(c.csv){}
            else if(c.rows)read_literal(&c,"]}\n");
            else{
                read_literal(&c,c.dict?"}":"]");
                if(o->want_total)read_literal(&c,",\"total\":null}\n");
                else if(c.count==(size_t)o->limit){
                    if(c.start>INT64_MAX-(int64_t)c.count)c.error=EOVERFLOW;
                    else{char tail[96];snprintf(tail,sizeof(tail),",\"cursor\":\"%lld\"}\n",(long long)(c.start+(int64_t)c.count));read_literal(&c,tail);}
                }else read_literal(&c,",\"cursor\":null}\n");
            }
        }else if(!c.csv)read_literal(&c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"]\n":"}\n");
    }
    return read_finish(&c,out);
}
int typed_read_single(SlotcaskDb *db,const FieldSchema *fs,const void *key,
    size_t keylen,const char *fields,size_t budget,FILE *out){
    TypedReadOptions options={.fields=fields,.budget=budget,.kind=TYPED_READ_GET_MULTI};
    ReadCommand c;memset(&c,0,sizeof(c));c.fs=fs;c.options=&options;
    if(!db||!fs||!fs->ts||!fs->ts->typed||!key||!keylen||keylen>MAX_KEY_CEILING||!out)
        return read_error(out,EINVAL);
    if(read_selection(&c))return read_finish(&c,out);
    c.key=key;c.keylen=keylen;
    uint8_t hash[16];compute_hash_raw(key,keylen,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0&&!c.error)c.error=EIO;
    if(!c.found&&!c.error)c.error=ENOENT;
    if(!c.error)read_literal(&c,"\n");
    return read_finish(&c,out);
}

int typed_read_route(const char *root,const char *object,const TypedReadOptions *o){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_command(db,&fs,&sc,o,g_out?g_out:stdout);
}
int typed_read_single_route(const char *root,const char *object,const void *key,
    size_t len,const char *fields){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_single(db,&fs,key,len,fields,g_query_buffer_max_bytes,g_out?g_out:stdout)!=0;
}
int typed_read_exists(const char *root,const char *object,const void *key,size_t len){
    FILE *out=g_out?g_out:stdout;
    if(!key||!len||len>MAX_KEY_CEILING)return read_error(out,EINVAL);
    Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    if(!db)return read_error(out,EIO);
    TypedReadOptions options={.kind=TYPED_READ_EXISTS_MULTI};
    ReadCommand c={.key=key,.keylen=len,.options=&options};
    uint8_t hash[16];compute_hash_raw(key,len,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0)return read_error(out,EIO);
    const char *text=c.found?"true\n":"false\n";size_t n=strlen(text);
    if(fwrite(text,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    return c.found?0:1;
}
````````

<!-- R1-BLOCK s2e85-old -->
````````text
        return row_error(err,cap,EINVAL,"invalid prepared schema");
    return 0;
}
/* Legacy scalar helpers report status/text but do not promise errno for
   validation failures. Establish the new adapter boundary's error contract. */
static int row_encode_fixed(const TypedField *field,const char *text,size_t len,
                              int quoted,uint8_t *out,char *err,size_t cap){
    errno=0;
    int rc=encode_json_field_checked(field,text,len,quoted,out,err,cap);
    if(rc)errno=errno==ENOMEM?ENOMEM:EINVAL;
    return rc;
}
static int replace_cell(TypedPrepared *p,size_t i,uint8_t *owner,
                          const uint8_t *data,size_t n,int is_null,
                          char *err,size_t cap){
    const PkrField *f=&p->schema->packed.fields[i];
    size_t prior=p->cells[i].is_null||f->kind==PKR_FIXED?0:p->cells[i].len;
    size_t added=is_null||f->kind==PKR_FIXED?0:n;
    if(p->bytes<prior||added>SLOTCASK_SEG_MAX_BYTES-24||
       p->bytes-prior>SLOTCASK_SEG_MAX_BYTES-24-added){
        free(owner);return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");}
    free(p->owned[i]);p->owned[i]=owner;p->cells[i]=(PkrCell){data,n,(uint8_t)is_null};
    p->bytes=p->bytes-prior+added;return 0;
}
int typed_prepared_set(TypedPrepared *p,size_t i,const TypedInput *in,
                        char *err,size_t cap){
    if(prepared_check(p,err,cap)||!in||i>=(size_t)p->schema->nfields)
        return row_error(err,cap,EINVAL,"invalid field input");
    if(!in->supplied)return 0;
    const TypedField *f=&p->schema->fields[i];const PkrField *pf=&p->schema->packed.fields[i];
    if(f->removed)return 0;
    if(in->is_null){
        if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;return -1;}
        int rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);
        if(!rc)p->seen[i]=1;
        return rc;
    }
    if(!in->data&&in->len)return row_error(err,cap,EINVAL,"invalid value span");
    const char *text=in->data?in->data:"";size_t n=in->len;char *decoded=NULL;
    int string_type=f->type==FT_VARCHAR||f->type==FT_NUMERIC||
        f->type==FT_ENUM||f->type==FT_DATE||f->type==FT_DATETIME||
        f->type==FT_DATETIMEMS||f->type==FT_TIME||f->type==FT_UUID||
        f->type==FT_IPV4||f->type==FT_IPV6;
    int was_quoted=in->quoted;
    if(was_quoted&&string_type){
        errno=0;
        int rc=f->type==FT_VARCHAR?json_unescape_string(text,n,&decoded,&n):
            json_unescape_cstring(text,n,&decoded,&n);
        if(rc)return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"malformed JSON string");
        text=decoded;was_quoted=0;
    }
````````

<!-- R1-BLOCK s2e85-new -->
````````c
        return row_error(err,cap,EINVAL,"invalid prepared schema");
    return 0;
}
/* Legacy scalar helpers report status/text but do not promise errno for
   validation failures. Establish the new adapter boundary's error contract. */
static int row_encode_fixed(const TypedField *field,const char *text,size_t len,
                              int quoted,uint8_t *out,char *err,size_t cap){
    errno=0;
    int rc=encode_json_field_checked(field,text,len,quoted,out,err,cap);
    if(rc)errno=errno==ENOMEM?ENOMEM:EINVAL;
    return rc;
}
static size_t prepared_heap_used(const TypedPrepared *p){
    size_t used=0;
    for(int i=0;i<p->schema->nfields;i++)if(p->owned[i]){
        size_t n=p->cells[i].len?p->cells[i].len:1;
        if(n>SIZE_MAX-used)return SIZE_MAX;
        used+=n;
    }
    return used;
}
static int replace_cell(TypedPrepared *p,size_t i,uint8_t *owner,
                          const uint8_t *data,size_t n,int is_null,
                          char *err,size_t cap){
    const PkrField *f=&p->schema->packed.fields[i];
    size_t prior=p->cells[i].is_null||f->kind==PKR_FIXED?0:p->cells[i].len;
    size_t added=is_null||f->kind==PKR_FIXED?0:n;
    if(p->bytes<prior||added>SLOTCASK_SEG_MAX_BYTES-24||
       p->bytes-prior>SLOTCASK_SEG_MAX_BYTES-24-added){
        free(owner);return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");}
    size_t heap=prepared_heap_used(p),allocation=owner?(n?n:1):0;
    if(heap>g_query_buffer_max_bytes||allocation>g_query_buffer_max_bytes-heap){
        free(owner);return row_error(err,cap,ENOMEM,"query memory buffer exceeded");}
    free(p->owned[i]);p->owned[i]=owner;p->cells[i]=(PkrCell){data,n,(uint8_t)is_null};
    p->bytes=p->bytes-prior+added;return 0;
}
int typed_prepared_set(TypedPrepared *p,size_t i,const TypedInput *in,
                        char *err,size_t cap){
    if(prepared_check(p,err,cap)||!in||i>=(size_t)p->schema->nfields)
        return row_error(err,cap,EINVAL,"invalid field input");
    if(!in->supplied)return 0;
    const TypedField *f=&p->schema->fields[i];const PkrField *pf=&p->schema->packed.fields[i];
    if(f->removed)return 0;
    if(in->is_null){
        if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;return -1;}
        int rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);
        if(!rc)p->seen[i]=1;
        return rc;
    }
    if(!in->data&&in->len)return row_error(err,cap,EINVAL,"invalid value span");
    const char *text=in->data?in->data:"";size_t n=in->len;char *decoded=NULL;
    size_t heap=prepared_heap_used(p),remaining=heap<=g_query_buffer_max_bytes?g_query_buffer_max_bytes-heap:0;
    size_t factor=pf->kind==PKR_NUMERIC?12u:2u;
    if(remaining<1024||n>(remaining-1024)/factor)
        return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    int string_type=f->type==FT_VARCHAR||f->type==FT_NUMERIC||
        f->type==FT_ENUM||f->type==FT_DATE||f->type==FT_DATETIME||
        f->type==FT_DATETIMEMS||f->type==FT_TIME||f->type==FT_UUID||
        f->type==FT_IPV4||f->type==FT_IPV6;
    int was_quoted=in->quoted;
    if(was_quoted&&string_type){
        errno=0;
        int rc=f->type==FT_VARCHAR?json_unescape_string(text,n,&decoded,&n):
            json_unescape_cstring(text,n,&decoded,&n);
        if(rc)return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"malformed JSON string");
        text=decoded;was_quoted=0;
    }
````````

<!-- R1-BLOCK s2e86-old -->
````````text
            if(replace_cell(p,(size_t)i,z,z,pf->width,0,err,cap))goto fail;
        }else{
            TypedInput in={pf->kind==PKR_NUMERIC?"0":"",pf->kind==PKR_NUMERIC?1:0,1,0,0};
            if(typed_prepared_set(p,(size_t)i,&in,err,cap))goto fail;
        }
    }
    memset(p->seen,0,sizeof(p->seen));return 0;
fail:typed_prepared_free(p);return -1;
}
int typed_prepared_old(const TypedSchema *ts,const SlotcaskOldRecord *old,
                        TypedPrepared *p,char *err,size_t cap){
    if(!p||!old||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid OLD");
    TypedRowView v;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&v))
        return row_error(err,cap,errno,"invalid OLD record");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++){
        TypedRowSpan sp;if(typed_row_span(&v,(size_t)i,&sp))goto fail;
        int null=sp.is_null||ts->fields[i].removed;
        p->cells[i]=(PkrCell){null?NULL:sp.data,null?0:sp.len,(uint8_t)null};
        if(!null&&ts->packed.fields[i].kind!=PKR_FIXED)p->bytes+=sp.len;
    }
    if(p->bytes>SLOTCASK_SEG_MAX_BYTES-24){
        typed_prepared_free(p);
        return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");
    }
    return 0;
fail:typed_prepared_free(p);return row_error(err,cap,EILSEQ,"invalid OLD field");
}
static int prepared_defaults_mode(TypedPrepared *p,const char *root,const char *object,
                             int defer_ac,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];if(f->removed||p->seen[i]||(defer_ac&&f->default_kind==DK_AUTO_CREATE))continue;
        TypedInput in={NULL,0,1,1,0};char generated[256];
        if(f->default_kind!=DK_NONE){
            in.data=generate_default(f,generated,sizeof(generated),root,object);
            if(!in.data)return row_error(err,cap,EIO,"default generator failed");
            in.len=strlen(in.data);in.is_null=0;
        }
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    return prepared_defaults_mode(p,root,object,0,err,cap);
}
int typed_prepared_stamp(TypedPrepared *p,enum DefaultKind kind,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    if(kind!=DK_AUTO_CREATE&&kind!=DK_AUTO_UPDATE)
        return row_error(err,cap,EINVAL,"invalid timestamp kind");
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||f->default_kind!=kind)continue;
````````

<!-- R1-BLOCK s2e86-new -->
````````c
            if(replace_cell(p,(size_t)i,z,z,pf->width,0,err,cap))goto fail;
        }else{
            TypedInput in={pf->kind==PKR_NUMERIC?"0":"",pf->kind==PKR_NUMERIC?1:0,1,0,0};
            if(typed_prepared_set(p,(size_t)i,&in,err,cap))goto fail;
        }
    }
    memset(p->seen,0,sizeof(p->seen));return 0;
fail:typed_prepared_free(p);return -1;
}
int typed_prepared_old(const TypedSchema *ts,const SlotcaskOldRecord *old,
                        TypedPrepared *p,char *err,size_t cap){
    if(!p||!old||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid OLD");
    size_t retained=p->schema?prepared_heap_used(p):0;
    size_t available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
    if(!typed_packed_render_allowance(ts,old->value,old->vlen,old->hdr2,NULL,available))
        return row_error(err,cap,errno,"OLD validation exceeds query memory or has invalid structure");
    TypedRowView v;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&v))
        return row_error(err,cap,errno,"invalid OLD record");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++){
        TypedRowSpan sp;if(typed_row_span(&v,(size_t)i,&sp))goto fail;
        int null=sp.is_null||ts->fields[i].removed;
        p->cells[i]=(PkrCell){null?NULL:sp.data,null?0:sp.len,(uint8_t)null};
        if(!null&&ts->packed.fields[i].kind!=PKR_FIXED)p->bytes+=sp.len;
    }
    if(p->bytes>SLOTCASK_SEG_MAX_BYTES-24){
        typed_prepared_free(p);
        return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");
    }
    return 0;
fail:typed_prepared_free(p);return row_error(err,cap,EILSEQ,"invalid OLD field");
}
static int prepared_defaults_mode(TypedPrepared *p,const char *root,const char *object,
    int defer_ac,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||p->seen[i]||(defer_ac&&f->default_kind==DK_AUTO_CREATE))continue;
        TypedInput in={NULL,0,1,1,0};char *owned=NULL;
        if(f->default_kind!=DK_NONE){
            size_t heap=prepared_heap_used(p),available=heap<g_query_buffer_max_bytes?g_query_buffer_max_bytes-heap:0;
            /* Generator text overlaps its checked payload copy. Account the
             * already retained owners before requesting either allocation. */
            size_t generator_budget=available>2048?(available-2048)/2:0;
            if(!generator_budget)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
            owned=generate_default_owned(f,root,object,generator_budget);
            if(!owned)return row_error(err,cap,errno,errno==ENOMEM||errno==ENOBUFS?"query memory buffer exceeded":"default generator failed");
            in.data=owned;in.len=strlen(owned);in.is_null=0;
        }
        int rc=typed_prepared_set(p,(size_t)i,&in,err,cap);int saved=errno;
        free(owned);if(rc){errno=saved;return -1;}
    }
    return 0;
}
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    return prepared_defaults_mode(p,root,object,0,err,cap);
}
int typed_prepared_stamp(TypedPrepared *p,enum DefaultKind kind,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    if(kind!=DK_AUTO_CREATE&&kind!=DK_AUTO_UPDATE)
        return row_error(err,cap,EINVAL,"invalid timestamp kind");
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||f->default_kind!=kind)continue;
````````

<!-- R1-BLOCK s2e87-old -->
````````text
    if(used)*used=0;
    if(prepared_valid(p,err,errsz))return -1;
    dec_status st=pkr_compose(&p->schema->packed,p->cells,(size_t)p->schema->nfields,out,cap,used);
    if(st!=DEC_OK)return row_error(err,errsz,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record composition failed");
    return 0;
}
int typed_prepared_alloc(const TypedPrepared *p,uint8_t **out,size_t *len,
                          char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(prepared_valid(p,err,cap))return -1;
    size_t need=0;dec_status st=pkr_measure(&p->schema->packed,p->cells,(size_t)p->schema->nfields,&need);
    if(st!=DEC_OK)return row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record sizing failed");
    uint8_t *buf=malloc(need);if(!buf)return row_error(err,cap,ENOMEM,"oom");
    size_t used=0;if(typed_prepared_compose(p,buf,need,&used,err,cap)){free(buf);return -1;}
    *out=buf;*len=used;return 0;
}

/* Validate syntax before span collection. json_skip_value locates spans but
   deliberately is not a validator. Depth is bounded without global state. */
static const char *row_json_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int row_json_string(const char **cursor){
````````

<!-- R1-BLOCK s2e87-new -->
````````c
    if(used)*used=0;
    if(prepared_valid(p,err,errsz))return -1;
    dec_status st=pkr_compose(&p->schema->packed,p->cells,(size_t)p->schema->nfields,out,cap,used);
    if(st!=DEC_OK)return row_error(err,errsz,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record composition failed");
    return 0;
}
int typed_prepared_alloc(const TypedPrepared *p,uint8_t **out,size_t *len,
                          char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(prepared_valid(p,err,cap))return -1;
    size_t need=0;dec_status st=pkr_measure(&p->schema->packed,p->cells,(size_t)p->schema->nfields,&need);
    if(st!=DEC_OK)return row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record sizing failed");
    if(prepared_heap_used(p)>g_query_buffer_max_bytes||need>g_query_buffer_max_bytes-prepared_heap_used(p))return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    uint8_t *buf=malloc(need);if(!buf)return row_error(err,cap,ENOMEM,"oom");
    size_t used=0;if(typed_prepared_compose(p,buf,need,&used,err,cap)){free(buf);return -1;}
    *out=buf;*len=used;return 0;
}

/* Validate syntax before span collection. json_skip_value locates spans but
   deliberately is not a validator. Depth is bounded without global state. */
static const char *row_json_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int row_json_string(const char **cursor){
````````

<!-- R1-BLOCK s2e88-old -->
````````text
        if(*p=='.'){
            p++;if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='e'||*p=='E'){
            p++;if(*p=='+'||*p=='-')p++;
            if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
    }
    *cursor=p;return 0;
}
static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,const SlotcaskOldRecord *old,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(old?typed_prepared_old(ts,old,out,err,cap):typed_prepared_zero(ts,out,err,cap))return -1;
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    int first=1;
    while(1){
        p=row_json_space(p);if(*p=='}'){p++;break;}
        if(!first){if(*p++!=',')goto bad;p=row_json_space(p);}
        if(*p!='"')goto bad;
        const char *key=p+1,*after=p;
        if(row_json_string(&after))goto bad;
        size_t kn=(size_t)(after-key-1);p=row_json_space(after);
        if(*p++!=':')goto bad;
        p=row_json_space(p);
        const char *v=p;
````````

<!-- R1-BLOCK s2e88-new -->
````````c
        if(*p=='.'){
            p++;if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='e'||*p=='E'){
            p++;if(*p=='+'||*p=='-')p++;
            if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
    }
    *cursor=p;return 0;
}
#include <math.h>
/* Computed operands are borrowed JSON spans; results own their payloads. */
typedef struct {int op,source;const char *literal;size_t len;int quoted;} RowCompute;
static int row_compute_parse(const TypedSchema *ts,size_t index,const char *json,
    size_t n,RowCompute *c,char *err,size_t cap){
    memset(c,0,sizeof(*c));
    const TypedField *f=&ts->fields[index];
    if(f->type!=FT_SHORT&&f->type!=FT_INT&&f->type!=FT_LONG&&
       f->type!=FT_NUMERIC&&f->type!=FT_FLOAT&&f->type!=FT_DOUBLE)
        return row_error(err,cap,EINVAL,"unsupported computed target");
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    p=row_json_space(p);if(*p!='"')goto bad;
    const char *key=p+1,*end=p;if(row_json_string(&end))goto bad;
    char *name=NULL;size_t length=0;
    if(json_unescape_cstring(key,(size_t)(end-key-1),&name,&length))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid computed operator");
    c->op=length==5&&!memcmp(name,"$incr",5)?1:
          length==5&&!memcmp(name,"$copy",5)?2:0;
    free(name);if(!c->op)goto bad;
    p=row_json_space(end);if(*p++!=':')goto bad;p=row_json_space(p);
    const char *v=p;if(row_json_value(&p,0))goto bad;
    size_t vn=(size_t)(p-v);p=row_json_space(p);
    if(*p++!='}'||p!=json+n)goto bad;
    c->quoted=vn>=2&&v[0]=='"'&&v[vn-1]=='"';
    if(c->op==1){
        if(!vn||(!c->quoted&&(v[0]=='{'||v[0]=='['||v[0]=='n'||v[0]=='t'||v[0]=='f')))goto bad;
        c->literal=c->quoted?v+1:v;c->len=c->quoted?vn-2:vn;return 0;
    }
    if(!c->quoted)goto bad;
    char *source=NULL;size_t sn=0;
    if(json_unescape_cstring(v+1,vn-2,&source,&sn))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid copy source");
    c->source=-1;
    for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed&&
       sn==(size_t)ts->fields[i].name_len&&!memcmp(source,ts->fields[i].name,sn)){c->source=i;break;}
    free(source);if(c->source<0)goto bad;
    const TypedField *sf=&ts->fields[c->source];
    const PkrField *a=&ts->packed.fields[index],*b=&ts->packed.fields[c->source];
    if(sf->type!=f->type||a->kind!=b->kind||a->width!=b->width||
       (a->kind==PKR_NUMERIC&&(a->bare!=b->bare||a->precision!=b->precision||a->scale!=b->scale)))goto bad;
    return 0;
bad:return row_error(err,cap,EINVAL,"malformed or incompatible computed value");
}
static int row_compute_apply(TypedPrepared *p,const SlotcaskOldRecord *old,
    const RowCompute *computes,char *err,size_t cap){
    const TypedSchema *ts=p->schema;TypedRowView view;
    size_t retained=prepared_heap_used(p),available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
    if(!typed_packed_render_allowance(ts,old->value,old->vlen,old->hdr2,NULL,available))
        return row_error(err,cap,errno,"OLD validation exceeds query memory or has invalid structure");
    if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))return -1;
    for(int i=0;i<ts->nfields;i++){
        const RowCompute *c=&computes[i];if(!c->op)continue;
        const TypedField *f=&ts->fields[i];const PkrField *pf=&ts->packed.fields[i];
        TypedRowSpan sp;
        if(typed_row_span(&view,(size_t)i,&sp))return -1;
        if(sp.is_null)return row_error(err,cap,EINVAL,"NULL computed target");
        if(c->op==2){
            if(computes[c->source].op)return row_error(err,cap,EINVAL,"copy source is computed");
            if(typed_row_span(&view,(size_t)c->source,&sp))return -1;
            if(sp.is_null)return row_error(err,cap,EINVAL,"NULL copy source");
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            if(f->type==FT_DOUBLE){double v;memcpy(&v,sp.data,8);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            retained=prepared_heap_used(p);available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
            if(sp.len>available)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
            uint8_t *bytes=malloc(sp.len?sp.len:1);
            if(!bytes)return row_error(err,cap,ENOMEM,"oom");
            if(sp.len)memcpy(bytes,sp.data,sp.len);
            if(replace_cell(p,(size_t)i,bytes,bytes,sp.len,0,err,cap))return -1;
            p->seen[i]=1;continue;
        }
        retained=prepared_heap_used(p);available=retained<g_query_buffer_max_bytes?g_query_buffer_max_bytes-retained:0;
        if(available<1024||sp.len>(available-1024)/8||c->len>(available-1024-8*sp.len)/12)
            return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
        char *decoded=NULL;const char *literal=c->literal;size_t length=c->len;
        if(c->quoted){
            if(json_unescape_cstring(literal,length,&decoded,&length))
                return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid increment");
            literal=decoded;
        }
        int rc=-1;char text[128];
        if(f->type==FT_FLOAT||f->type==FT_DOUBLE){
            char *operand=malloc(length+1);
            if(!operand){free(decoded);return row_error(err,cap,ENOMEM,"oom");}
            memcpy(operand,literal,length);operand[length]=0;
            char *end=NULL;errno=0;double delta=strtod(operand,&end);
            int valid=end==operand+length&&end!=operand&&errno!=ERANGE&&isfinite(delta);
            double current;
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);current=v;}
            else memcpy(&current,sp.data,8);
            free(operand);
            double result=current+delta;
            if(valid&&isfinite(current)&&isfinite(result)){
                int written;
                if(f->type==FT_FLOAT){float v=(float)result;written=isfinite(v)?snprintf(text,sizeof(text),"%.9g",(double)v):-1;}
                else written=snprintf(text,sizeof(text),"%.17g",result);
                if(written>0&&(size_t)written<sizeof(text)){
                    TypedInput in={text,(size_t)written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);
                }
            }
            if(rc)row_error(err,cap,EINVAL,"invalid or overflowing floating increment");
        }else{
            dec_t current,delta,sum;dec_init(&current);dec_init(&delta);dec_init(&sum);
            dec_status st;
            if(pf->kind==PKR_NUMERIC)st=dec_unpack(sp.data,sp.len,&current);
            else{
                uint64_t bits=0;for(size_t j=0;j<sp.len;j++)bits=(bits<<8)|sp.data[j];
                if(sp.len<8&&(sp.data[0]&128))bits|=UINT64_MAX<<(sp.len*8);
                int64_t value;memcpy(&value,&bits,8);
                int written=snprintf(text,sizeof(text),"%lld",(long long)value);
                st=dec_parse(text,(size_t)written,&current,err,cap);
            }
            if(st==DEC_OK&&pf->kind!=PKR_NUMERIC){
                char *integer=malloc(length+1);
                if(!integer)st=DEC_ERR_NOMEM;
                else{
                    memcpy(integer,literal,length);integer[length]=0;char *end=NULL;errno=0;
                    (void)strtoll(integer,&end,10);
                    int valid=end!=integer&&errno!=ERANGE;
                    while(*end==' '||*end=='\t'||*end=='\r'||*end=='\n'||*end=='\f'||*end=='\v')end++;
                    if(*end||!valid)st=DEC_ERR_SYNTAX;
                    free(integer);
                }
            }
            if(st==DEC_OK)st=dec_parse(literal,length,&delta,err,cap);
            if(st==DEC_OK){
                int64_t high=current.ndigits?current.weight:delta.weight;
                int64_t low=current.ndigits?(int64_t)current.weight-current.ndigits+1:(int64_t)delta.weight-delta.ndigits+1;
                if(delta.ndigits){if(delta.weight>high)high=delta.weight;int64_t dlow=(int64_t)delta.weight-delta.ndigits+1;if(dlow<low)low=dlow;}
                uint64_t span=high>=low?(uint64_t)(high-low+2)*4:4;
                int64_t integer=(high+1)*4;if(integer<1)integer=1;
                int64_t scale=current.dscale>delta.dscale?current.dscale:delta.dscale;
                uint64_t rendered=(uint64_t)integer+(uint64_t)scale+4;
                uint64_t needed=1024+16*span+16*rendered+8*(uint64_t)sp.len+12*(uint64_t)c->len;
                if(needed>available)st=DEC_ERR_NOMEM;
            }
            if(st==DEC_OK)st=dec_add(&current,&delta,&sum);
            if(st==DEC_OK){
                size_t n=dec_text_len(&sum);char *exact=n?malloc(n):NULL;
                if(!exact)st=DEC_ERR_NOMEM;
                else{
                    size_t written=0;st=dec_to_text(&sum,exact,n,&written);
                    if(st==DEC_OK){TypedInput in={exact,written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);}
                    free(exact);
                }
            }
            dec_free(&sum);dec_free(&delta);dec_free(&current);
            if(st!=DEC_OK)rc=row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"invalid or overflowing exact increment");
        }
        free(decoded);if(rc)return -1;
    }
    return 0;
}

static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,const SlotcaskOldRecord *old,int computed,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    size_t request_length=strnlen(json,g_query_buffer_max_bytes);
    if(request_length>=g_query_buffer_max_bytes/8)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(old?typed_prepared_old(ts,old,out,err,cap):typed_prepared_zero(ts,out,err,cap))return -1;
    RowCompute computes[MAX_FIELDS]={0};
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    int first=1;
    while(1){
        p=row_json_space(p);if(*p=='}'){p++;break;}
        if(!first){if(*p++!=',')goto bad;p=row_json_space(p);}
        if(*p!='"')goto bad;
        const char *key=p+1,*after=p;
        if(row_json_string(&after))goto bad;
        size_t kn=(size_t)(after-key-1);p=row_json_space(after);
        if(*p++!=':')goto bad;
        p=row_json_space(p);
        const char *v=p;
````````

<!-- R1-BLOCK s2e89-old -->
````````text
        size_t n=(size_t)(p-v);char *name=NULL;size_t name_len=0;
        /* Longer encoded keys cannot equal a <=255-byte schema name. */
        if(kn<=6*(sizeof(ts->fields[0].name)-1)){
            errno=0;
            if(json_unescape_string(key,kn,&name,&name_len)){
                row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid field name");goto fail;
            }
        }
        for(int i=0;name&&i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];
            if(f->removed||name_len!=(size_t)f->name_len||memcmp(f->name,name,name_len))continue;
            int quoted=n>=2&&v[0]=='"'&&v[n-1]=='"';
            if(!quoted&&(v[0]=='{'||v[0]=='[')){free(name);goto bad;}
            TypedInput in={quoted?v+1:v,quoted?n-2:n,1,
                (uint8_t)(!quoted&&json_span_is_null(v,n)),(uint8_t)quoted};
            if(typed_prepared_set(out,(size_t)i,&in,err,cap)){free(name);goto fail;}
            break;
        }
        free(name);first=0;
    }
    if(*row_json_space(p))goto bad;
    if(defaults&&typed_prepared_defaults(out,root,object,err,cap))goto fail;
    return 0;
bad:row_error(err,cap,EINVAL,"invalid record JSON");
fail:typed_prepared_free(out);return -1;
}
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
int typed_encode_alloc(const TypedSchema *ts,const char *json,int defaults,
                         const char *root,const char *object,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!ts->typed||ts->total_size<1)
        return row_error(err,cap,EINVAL,"invalid typed schema");
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    if(!ts->packed.nfields){
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        errno=0;
        int rc=defaults?typed_encode_defaults(ts,json,b,ts->total_size,root,object,err,cap):
            typed_encode(ts,json,b,ts->total_size,err,cap);
        if(rc<0){int cause=errno==ENOMEM?ENOMEM:EINVAL;free(b);errno=cause;return rc;}
        *out=b;*len=(size_t)rc;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
/* Validate a complete JSON value before bulk record collection. */
int typed_json_validate(const char *json){
    if(!json)return row_error(NULL,0,EINVAL,"invalid JSON");
    const char *p=json;
    if(row_json_value(&p,0)||*row_json_space(p))
        return row_error(NULL,0,EINVAL,"invalid JSON");
    return 0;
}
static int prepared_insert_finish(TypedPrepared *p,const SlotcaskOldRecord *old,
````````

<!-- R1-BLOCK s2e89-new -->
````````c
        size_t n=(size_t)(p-v);char *name=NULL;size_t name_len=0;
        /* Longer encoded keys cannot equal a <=255-byte schema name. */
        if(kn<=6*(sizeof(ts->fields[0].name)-1)){
            errno=0;
            if(json_unescape_string(key,kn,&name,&name_len)){
                row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid field name");goto fail;
            }
        }
        for(int i=0;name&&i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];
            if(f->removed||name_len!=(size_t)f->name_len||memcmp(f->name,name,name_len))continue;
            int quoted=n>=2&&v[0]=='"'&&v[n-1]=='"';
            if(!quoted&&(v[0]=='{'||v[0]=='[')){
                if(!computed||!old||v[0]!='{'){free(name);goto bad;}
                if(row_compute_parse(ts,(size_t)i,v,n,&computes[i],err,cap)){free(name);goto fail;}
                RowCompute one[MAX_FIELDS]={0};one[i]=computes[i];
                if(row_compute_apply(out,old,one,err,cap)){free(name);goto fail;}
                break;
            }
            computes[i]=(RowCompute){0};
            TypedInput in={quoted?v+1:v,quoted?n-2:n,1,
                (uint8_t)(!quoted&&json_span_is_null(v,n)),(uint8_t)quoted};
            if(typed_prepared_set(out,(size_t)i,&in,err,cap)){free(name);goto fail;}
            break;
        }
        free(name);first=0;
    }
    if(*row_json_space(p))goto bad;
    if(computed)for(int i=0;i<ts->nfields;i++)if(computes[i].op==2&&computes[computes[i].source].op){
        row_error(err,cap,EINVAL,"copy source is computed");goto fail;
    }
    if(defaults&&typed_prepared_defaults(out,root,object,err,cap))goto fail;
    return 0;
bad:row_error(err,cap,EINVAL,"invalid record JSON");
fail:typed_prepared_free(out);return -1;
}
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
int typed_encode_alloc(const TypedSchema *ts,const char *json,int defaults,
                         const char *root,const char *object,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!ts->typed||ts->total_size<1)
        return row_error(err,cap,EINVAL,"invalid typed schema");
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    if(!ts->packed.nfields){
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        errno=0;
        int rc=defaults?typed_encode_defaults(ts,json,b,ts->total_size,root,object,err,cap):
            typed_encode(ts,json,b,ts->total_size,err,cap);
        if(rc<0){int cause=errno==ENOMEM?ENOMEM:EINVAL;free(b);errno=cause;return rc;}
        *out=b;*len=(size_t)rc;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
/* Validate a complete JSON value before bulk record collection. */
int typed_json_validate(const char *json){
    if(!json)return row_error(NULL,0,EINVAL,"invalid JSON");
    const char *p=json;
    if(row_json_value(&p,0)||*row_json_space(p))
        return row_error(NULL,0,EINVAL,"invalid JSON");
    return 0;
}
static int prepared_insert_finish(TypedPrepared *p,const SlotcaskOldRecord *old,
````````

<!-- R1-BLOCK s2e90-old -->
````````text
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
````````

<!-- R1-BLOCK s2e90-new -->
````````c
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
    int rc=prepare_json(ts,json,0,root,object,&p,NULL,0,err,cap);
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
````````

<!-- R1-BLOCK s2e91-old -->
````````text
    typed_prepared_free(&p);return rc;
}

/* Packed scalar JSON patch. Known object/array values, including computed
   expressions, fail before composition; legacy computed semantics stay in
   storage.c until Phase 4 supplies their exact-decimal replacement. */
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                         const TypedInput *inputs,int stamp,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!old)return row_error(err,cap,EINVAL,"invalid update");
    if(!ts->packed.nfields){
        if(ts->total_size<1)return row_error(err,cap,EINVAL,"invalid schema");
````````

<!-- R1-BLOCK s2e91-new -->
````````c
    typed_prepared_free(&p);return rc;
}

/* Packed scalar JSON patch. Known object/array values, including computed
   expressions, fail before composition; legacy computed semantics stay in
   storage.c until Phase 4 supplies their exact-decimal replacement. */
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,0,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_computed_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,1,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                         const TypedInput *inputs,int stamp,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!old)return row_error(err,cap,EINVAL,"invalid update");
    if(!ts->packed.nfields){
        if(ts->total_size<1)return row_error(err,cap,EINVAL,"invalid schema");
````````

<!-- R1-BLOCK s2e92-old -->
````````text
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

int typed_update_computed_alloc(const TypedSchema*t,const SlotcaskOldRecord*o,const char*j,uint8_t**b,size_t*n,char*e,size_t c){(void)t;(void)o;(void)j;(void)e;(void)c;*b=NULL;*n=0;errno=ENOTSUP;return -1;}
````````

<!-- R1-BLOCK s2e92-new -->
````````c
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

/* Conservative peak heap allowance: dec_unpack digits + grown limb storage,
 * projection selector copy, and at most one composite join beside output.
 * Record/cache/input bytes and bounded automatic frames remain borrowed. */
size_t typed_packed_render_allowance(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t flags,const char *fields,size_t available){
    if(!packed_schema_ok(ts)||!value||flags!=PKR_FLAGS||
       len<(size_t)ts->packed.prefix+ts->packed.directory||
       len>ts->packed.maximum||value[0]!=ts->packed.prefix-1){errno=EILSEQ;return 0;}
    size_t largest=0;
    for(int i=0;i<ts->nfields;i++)if(ts->packed.fields[i].kind==PKR_NUMERIC){
        const PkrField *f=&ts->packed.fields[i];const uint8_t *entry=value+ts->packed.prefix+f->offset;
        uint32_t bytes=0;for(int j=0;j<4;j++)bytes=(bytes<<8)|entry[4+j];
        if(bytes>f->payload_max||bytes>len-ts->packed.prefix){errno=EILSEQ;return 0;}
        if(bytes>largest)largest=bytes;
    }
    size_t selector=fields?strnlen(fields,MAX_LINE):0;
    if(selector==MAX_LINE){errno=E2BIG;return 0;}
    if(largest>(SIZE_MAX-selector-129)/8){errno=EOVERFLOW;return 0;}
    size_t workspace=largest?8*largest+128:0;
    if(fields)workspace+=selector+1;
    if(available<=workspace+1){errno=ENOBUFS;return 0;}
    /* Admission precedes pkr_view/decimal allocation; full view validation is
     * still mandatory, never replaced by the inexpensive length scan. */
    size_t allowance=(available-workspace)/2;
    if(!allowance)errno=ENOBUFS;
    return allowance;
}

static int row_selected_open(const TypedRowView *view,const CsvSelection *selection,
    size_t index,size_t budget,NamedText *text){
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    if(selection->indices[index]>=0)
        return projection_text_open(view,selection->indices[index],&text->value);
    return named_text_open(view,selection->names[index],budget,text);
}
char *typed_packed_row_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t flags,const void *key,size_t key_len,const char *fields,size_t budget){
    if(!packed_schema_ok(ts)||(!key&&key_len)){errno=EINVAL;return NULL;}
    if(budget<4){errno=ENOMEM;return NULL;}
    CsvSelection selection;
    if(csv_selection_open(ts,fields,&selection)){free(selection.copy);return NULL;}
    TypedRowView view;char *out=NULL;
    if(typed_row_open(ts,value,len,flags,&view))goto done;
    size_t key_bytes=0,need=3;
    if(quoted_length(key,key_len,&key_bytes))goto done;
    if(key_bytes>SIZE_MAX-need){errno=EOVERFLOW;goto done;}
    need+=key_bytes;if(need>budget){errno=ENOMEM;goto done;}
    for(size_t i=0;i<selection.count;i++){
        NamedText text;size_t part=0;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_length(&text.value,1,&part);
        named_text_free(&text);if(rc)goto done;
        if(part==SIZE_MAX||need>SIZE_MAX-part-1){errno=EOVERFLOW;goto done;}
        need+=part+1;if(need>budget){errno=ENOMEM;goto done;}
    }
    out=malloc(need);if(!out){errno=ENOMEM;goto done;}
    size_t pos=0,used=0;out[pos++]='[';
    if(quote_into(key,key_len,out+pos,need-pos,&used))goto fail;
    pos+=used;
    for(size_t i=0;i<selection.count;i++){
        out[pos++]=',';NamedText text;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_write(&text.value,1,out+pos,need-pos,&used);
        named_text_free(&text);if(rc)goto fail;pos+=used;
    }
    out[pos++]=']';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
````````

<!-- R1-BLOCK s2e93-old -->
````````text
   malloc storage on success. No schema activation. */
/* Validates one complete JSON value, including unknown nested fields. */
int typed_json_validate(const char *json);
/* Positional full replacement: no JSON defaults; preserve/stamp AUTO_CREATE
   under the same locked OLD contract as typed_insert_alloc. inputs has nfields
   entries. Unprovided cells are present zero/empty (removed cells stay NULL). */
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,
    const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap);
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
````````

<!-- R1-BLOCK s2e93-new -->
````````c
   malloc storage on success. No schema activation. */
/* Validates one complete JSON value, including unknown nested fields. */
int typed_json_validate(const char *json);
/* Positional full replacement: no JSON defaults; preserve/stamp AUTO_CREATE
   under the same locked OLD contract as typed_insert_alloc. inputs has nfields
   entries. Unprovided cells are present zero/empty (removed cells stay NULL). */
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,
    const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap);
/* Single-update computed operations read immutable locked OLD. Bulk retains
 * its literal-only contract through typed_update_json_alloc. */
int typed_update_computed_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
````````

<!-- R1-BLOCK s2e94-old -->
````````text
char *typed_packed_csv_row(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
#endif

int typed_update_computed_alloc(const TypedSchema*,const SlotcaskOldRecord*,const char*,uint8_t**,size_t*,char*,size_t);
````````

<!-- R1-BLOCK s2e94-new -->
````````c
char *typed_packed_csv_row(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
/* Remaining heap budget shared by renderer output + transient decimal/join
 * workspace. Zero is failure with errno; no allocation or output is performed. */
size_t typed_packed_render_allowance(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t flags,const char *fields,size_t available);
/* Complete rows-format array with explicit key bytes and one validated view.
 * Use render_allowance first for a command-wide output/workspace budget. */
char *typed_packed_row_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t flags,const void *key,size_t key_len,const char *fields,size_t budget);
#endif
````````

<!-- R1-BLOCK s2e95-old -->
````````text
                           its bytes stay reserved until vacuum compacts them out */
    enum DefaultKind default_kind;
    char default_val[256]; /* literal value, seq name, or random byte count */

    /* not_null=1 rejects every write that would store NULL for this
       field. not_null_set marks that the parsed spec stated nullability
       EXPLICITLY (:not_null or :nullable) — edit-field uses it to tell
       "edit omits nullability → carry the old state" apart from
       ":nullable → remove". Zeroed by the memset at every construction
       site, so both default to unconstrained. */
    uint8_t not_null;
    uint8_t not_null_set;
    uint8_t default_invalid;

    /* FT_ENUM only — populated by load_typed_schema, freed by
       free_typed_schema. Empty/zeroed for every other type.
       `enum_values` is a heap-allocated array of strdup'd value strings;
       a record's encoded byte index `i` decodes to `enum_values[i]`. */
    char **enum_values;
    int    n_enum_values;
    int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */
} TypedField;

typedef struct TypedSchema {
    TypedField fields[MAX_FIELDS];
````````

<!-- R1-BLOCK s2e95-new -->
````````c
                           its bytes stay reserved until vacuum compacts them out */
    enum DefaultKind default_kind;
    char default_val[256]; /* literal value, seq name, or random byte count */

    /* not_null=1 rejects every write that would store NULL for this
       field. not_null_set marks that the parsed spec stated nullability
       EXPLICITLY (:not_null or :nullable) — edit-field uses it to tell
       "edit omits nullability → carry the old state" apart from
       ":nullable → remove". Zeroed by the memset at every construction
       site, so both default to unconstrained. */
    uint8_t not_null;
    uint8_t not_null_set;
    uint8_t default_invalid; /* legacy declaration readable; unsafe default generation refused */

    /* FT_ENUM only — populated by load_typed_schema, freed by
       free_typed_schema. Empty/zeroed for every other type.
       `enum_values` is a heap-allocated array of strdup'd value strings;
       a record's encoded byte index `i` decodes to `enum_values[i]`. */
    char **enum_values;
    int    n_enum_values;
    int    enum_width;  /* 1 if n_enum_values <= 256, else 2 (BE) */
} TypedField;

typedef struct TypedSchema {
    TypedField fields[MAX_FIELDS];
````````

<!-- R1-BLOCK s2e96-old -->
````````text
   on schema invalidation / re-parse. */
int  enum_value_index(const TypedField *f, const char *val, size_t vlen);
void free_enum_values(TypedField *f);

/* Read one '\n'-terminated line from fp into a malloc'd buffer.
   Returns the line without its trailing '\n' (caller frees), or NULL:
   clean EOF sets *err = 0, a read error sets *err = errno, and a line
   whose content reaches max_len before its newline sets *err = EOVERFLOW
   (the rest of the offending line is consumed). Never returns a line of
   max_len bytes or longer. Deliberately not getline(): no feature-macro
   regime questions on macOS. */
char *conf_read_line(FILE *fp, size_t max_len, int *err);

/* True when the spec's ":default=" tail exceeds TypedField.default_val's
   255-byte capacity. parse_default_modifier would silently truncate such
   a literal (and the applied default with it), so every wire gate calls
   this and rejects the spec instead. */
int field_default_too_long(const char *field_spec);
void load_dirs(void);
int is_valid_dir(const char *dir);
int dir_name_ok(const char *dir);
void build_effective_root(char *out, size_t outlen, const char *dir);

/* Typed field system */
````````

<!-- R1-BLOCK s2e96-new -->
````````c
   on schema invalidation / re-parse. */
int  enum_value_index(const TypedField *f, const char *val, size_t vlen);
void free_enum_values(TypedField *f);

/* Read one '\n'-terminated line from fp into a malloc'd buffer.
   Returns the line without its trailing '\n' (caller frees), or NULL:
   clean EOF sets *err = 0, a read error sets *err = errno, and a line
   whose content reaches max_len before its newline sets *err = EOVERFLOW
   (the rest of the offending line is consumed). Never returns a line of
   max_len bytes or longer. Deliberately not getline(): no feature-macro
   regime questions on macOS. */
char *conf_read_line(FILE *fp, size_t max_len, int *err);
char *typed_decode_bounded_size(const TypedSchema *ts,const uint8_t *data,int len,
    uint8_t hdr2,size_t budget,size_t *allocated);
char *typed_decode_bounded(const TypedSchema *ts,const uint8_t *data,int len,
    uint8_t hdr2,size_t budget);
int parse_default_modifier(char *type_spec_inout,TypedField *tf);
int parse_default_modifier_checked(char *type_spec_inout,TypedField *tf);
char *generate_default_owned(const TypedField *tf,const char *root,const char *object,size_t budget);

/* True when the spec's ":default=" tail exceeds TypedField.default_val's
   255-byte capacity. parse_default_modifier would silently truncate such
   a literal (and the applied default with it), so every wire gate calls
   this and rejects the spec instead. */
int field_default_too_long(const char *field_spec);
void load_dirs(void);
int is_valid_dir(const char *dir);
int dir_name_ok(const char *dir);
void build_effective_root(char *out, size_t outlen, const char *dir);

/* Typed field system */
````````

<!-- R1-BLOCK s2e97-old -->
````````text
   is all-zero bytes; canonical wire form is the 36-char dashed lowercase
   hex. Used by decode_field_to_buf, typed_get_field_str,
   typed_field_to_buf_raw, and decode_idx_to_buf. */
int  uuid_is_zero(const uint8_t b[16]);
int  uuid_format_canonical(char *buf, size_t buflen, const uint8_t b[16]);

/* query.c — estimate-index. Sample N records, project on-disk size for
   a hypothetical trigram index on the given field. Spec format
   "<field>:trigram". See [[trigram-impl-map]] phase 5. */
int cmd_estimate_index(const char *db_root, const char *object,
                       const char *spec);
#endif

int parse_default_modifier_checked(char*,TypedField*);
char *generate_default_owned(const TypedField*,const char*,const char*,size_t);
````````

<!-- R1-BLOCK s2e97-new -->
````````c
   is all-zero bytes; canonical wire form is the 36-char dashed lowercase
   hex. Used by decode_field_to_buf, typed_get_field_str,
   typed_field_to_buf_raw, and decode_idx_to_buf. */
int  uuid_is_zero(const uint8_t b[16]);
int  uuid_format_canonical(char *buf, size_t buflen, const uint8_t b[16]);

/* query.c — estimate-index. Sample N records, project on-disk size for
   a hypothetical trigram index on the given field. Spec format
   "<field>:trigram". See [[trigram-impl-map]] phase 5. */
int cmd_estimate_index(const char *db_root, const char *object,
                       const char *spec);
#endif
````````

<!-- R1-BLOCK s2e98-old -->
````````text
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":"
        "{\"balance\":{\"$incr\":1,\"$copy\":\"mirror\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "two ops rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1,\"junk\":9}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown member rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"0.005\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "sub-scale literal rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"price\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "cross-type copy rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"nope\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown copy source rejected");
    free(resp); resp = NULL;

    /* 7. Condition + computed: if fails → nothing; if passes → applied. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
````````

<!-- R1-BLOCK s2e98-new -->
````````c
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":"
        "{\"balance\":{\"$incr\":1,\"$copy\":\"mirror\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "two ops rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1,\"junk\":9}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown member rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"0.005\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "exact increment rounded once on assignment");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"price\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "cross-type copy rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"nope\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown copy source rejected");
    free(resp); resp = NULL;

    /* 7. Condition + computed: if fails → nothing; if passes → applied. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
````````

<!-- R1-BLOCK s2e99-old -->
````````text
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default+token accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=zz:not_null"),
                "canonical line written");
    ASSERT_TRUE(fc && !strstr(fc, "not_null:default="),
                "no second default appended");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"default\":\"zz\"", "default literal clean");
    ASSERT_TRUE(SAFE_STRSTR(resp, "zz:not_null") == NULL,
                "token not swallowed into the literal");
    free(resp); resp = NULL;

    /* swapping the default keeps the constraint, replaces the literal */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\","
                   "\"fields\":[\"m:varchar:40:default=yy:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default swap accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=yy:not_null"),
````````

<!-- R1-BLOCK s2e99-new -->
````````c
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default+token accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=zz:not_null"),
                "canonical line written");
    ASSERT_TRUE(fc && !strstr(fc, "not_null:default="),
                "no second default appended");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"default\":\"zz\"", "default literal clean");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"default\":\"zz:not_null\"") == NULL,
                "token not swallowed into the literal");
    free(resp); resp = NULL;

    /* swapping the default keeps the constraint, replaces the literal */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\","
                   "\"fields\":[\"m:varchar:40:default=yy:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default swap accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=yy:not_null"),
````````

<!-- R1-BLOCK s2e100-old -->
````````text
    ASSERT_TRUE(r && strstr(r, "\"errors\":1") != NULL,
                "bulk error tallied");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bok\"}");
    ASSERT_TRUE(r && strncmp(r, "true", 4) == 0, "valid bulk row stored");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bbad\"}");
    ASSERT_TRUE(r && strncmp(r, "false", 5) == 0, "bad bulk row skipped");
    free(r);

    /* Overflowing literal default now fails the insert (was silent
       wrap). */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"dflt\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:int:default=99999999999\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "dflt object created "
                "(default validation is Phase 4)");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"dflt\","
        "\"key\":\"a\",\"value\":{}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL &&
                strstr(r, "x") != NULL,
                "overflowing literal default refuses the insert");
    free(r);

    /* Float→double widening preserves the exact stored value. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"w\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen create"); free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\",\"value\":{\"x\":\"3.25\"}}"); free(r);
    r = req(tc, "{\"mode\":\"edit-field\",\"dir\":\"conv\",\"object\":\"w\","
        "\"fields\":[\"x:double\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen edit"); free(r);
````````

<!-- R1-BLOCK s2e100-new -->
````````c
    ASSERT_TRUE(r && strstr(r, "\"errors\":1") != NULL,
                "bulk error tallied");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bok\"}");
    ASSERT_TRUE(r && strncmp(r, "true", 4) == 0, "valid bulk row stored");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bbad\"}");
    ASSERT_TRUE(r && strncmp(r, "false", 5) == 0, "bad bulk row skipped");
    free(r);

    /* R1 validates literal defaults before publishing any schema. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"dflt\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:int:default=99999999999\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL,
                "overflowing literal default refuses schema publication");
    free(r);
    r = req(tc, "{\"mode\":\"describe-object\",\"dir\":\"conv\",\"object\":\"dflt\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL && strstr(r,"not found") != NULL,
                "rejected default leaves no object");
    free(r);

    /* Float→double widening preserves the exact stored value. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"w\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen create"); free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\",\"value\":{\"x\":\"3.25\"}}"); free(r);
    r = req(tc, "{\"mode\":\"edit-field\",\"dir\":\"conv\",\"object\":\"w\","
        "\"fields\":[\"x:double\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen edit"); free(r);
````````

<!-- R1-BLOCK s2e101-old -->
````````text
    int observed=0;slotcask_test_set_after_old_hook(w_observe,&observed);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f2\":-0.0000000000000000000000000001,\"f1\":\"longer content\",\"f3\":8}",NULL,0,1,&out),0,"partial update grows actual bytes");
    slotcask_test_set_after_old_hook(NULL,NULL);
    ASSERT_EQ_INT(observed,1,"update constructor under write lock");
    if(out){ASSERT_CONTAINS(out,"\"f0\":2.0000000000","omitted value preserved");
        ASSERT_CONTAINS(out,"-0.0000000000000000000000000001","bare precision survives");ASSERT_CONTAINS(out,ac_copy,"partial preserves AC");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":null,\"f3\":9,\"f3\":10}",NULL,0,1,&out),0,"NULL and duplicate patch");
    if(out){ASSERT_CONTAINS(out,"\"f1\":null","explicit null");ASSERT_CONTAINS(out,"\"f3\":10","last valid duplicate");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":\"a\\u0000b\"}",NULL,0,1,&out),0,"embedded NUL decoded once");
    if(out){ASSERT_CONTAINS(out,"a\\u0000b","embedded NUL byte span");free(out);}
    w_assert_reject(&f,0,"{\"f0\":\"\"}",NULL);
    w_assert_reject(&f,1,"{\"f0\":\"\"}",NULL);
    w_assert_reject(&f,1,"{\"f3\":{\"$incr\":1}}",NULL);
    w_assert_reject(&f,1,"{\"f3\":32768000000}",NULL);
    w_assert_reject(&f,1,"{\"f1\":\"\\uZZZZ\"}",NULL);
    w_assert_reject(&f,1,"{\"f3\":1}","[]");
    f.ts->fields[1].not_null=1;w_assert_reject(&f,1,"{\"f1\":null}",NULL);f.ts->fields[1].not_null=0;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,1,0,&out),1,"strict insert duplicate rejected");
    if(out){ASSERT_CONTAINS(out,"condition_not_met","duplicate response");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"absent","{}",NULL,0,0,&out),1,"update requires OLD");free(out);
    uint64_t pos=w_pos(&f);ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,1,0,&out),0,"dry run existing");free(out);
    ASSERT_TRUE(w_pos(&f)==pos,"dry run no append");
    char *final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"final physical row");
    if(final){ASSERT_CONTAINS(final,"\"f3\":10","last successful fixed field");free(final);}
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,f.root,1,1,slot),0,"reopen actual rows");f.opened=1;
    final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"reopened physical row");free(final);free(before);w_close(&f);
}
static void w_mismatch(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};
    const uint8_t raw[]={1,0,0};ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,raw,sizeof(raw),&o,NULL),0,"legacy raw seed");
    uint64_t p=w_pos(&f);char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,0,1,&out),1,"packed update rejects legacy OLD");free(out);
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),1,"packed upsert rejects legacy OLD");free(out);
    ASSERT_TRUE(w_pos(&f)==p,"generation failures before append");w_close(&f);
}
````````

<!-- R1-BLOCK s2e101-new -->
````````c
    int observed=0;slotcask_test_set_after_old_hook(w_observe,&observed);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f2\":-0.0000000000000000000000000001,\"f1\":\"longer content\",\"f3\":8}",NULL,0,1,&out),0,"partial update grows actual bytes");
    slotcask_test_set_after_old_hook(NULL,NULL);
    ASSERT_EQ_INT(observed,1,"update constructor under write lock");
    if(out){ASSERT_CONTAINS(out,"\"f0\":2.0000000000","omitted value preserved");
        ASSERT_CONTAINS(out,"-0.0000000000000000000000000001","bare precision survives");ASSERT_CONTAINS(out,ac_copy,"partial preserves AC");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":null,\"f3\":9,\"f3\":10}",NULL,0,1,&out),0,"NULL and duplicate patch");
    if(out){ASSERT_CONTAINS(out,"\"f1\":null","explicit null");ASSERT_CONTAINS(out,"\"f3\":10","last valid duplicate");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f1\":\"a\\u0000b\"}",NULL,0,1,&out),0,"embedded NUL decoded once");
    if(out){ASSERT_CONTAINS(out,"a\\u0000b","embedded NUL byte span");free(out);}
    w_assert_reject(&f,0,"{\"f0\":\"\"}",NULL);
    w_assert_reject(&f,1,"{\"f0\":\"\"}",NULL);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f3\":{\"$incr\":1}}",NULL,0,1,&out),0,"packed computed update supported");
    if(out){ASSERT_CONTAINS(out,"\"f3\":11","computed result reaches command response");free(out);}
    w_assert_reject(&f,1,"{\"f3\":{\"$incr\":1,\"$copy\":\"f3\"}}",NULL);
    w_assert_reject(&f,1,"{\"f3\":32768000000}",NULL);
    w_assert_reject(&f,1,"{\"f1\":\"\\uZZZZ\"}",NULL);
    w_assert_reject(&f,1,"{\"f3\":1}","[]");
    f.ts->fields[1].not_null=1;w_assert_reject(&f,1,"{\"f1\":null}",NULL);f.ts->fields[1].not_null=0;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,1,0,&out),1,"strict insert duplicate rejected");
    if(out){ASSERT_CONTAINS(out,"condition_not_met","duplicate response");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"absent","{}",NULL,0,0,&out),1,"update requires OLD");free(out);
    uint64_t pos=w_pos(&f);ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,1,0,&out),0,"dry run existing");free(out);
    ASSERT_TRUE(w_pos(&f)==pos,"dry run no append");
    char *final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"final physical row");
    if(final){ASSERT_CONTAINS(final,"\"f3\":11","last successful fixed field");free(final);}
    int slot=f.db.slot_size;slotcask_close(&f.db);f.opened=0;
    ASSERT_EQ_INT(slotcask_open(&f.db,f.root,1,1,slot),0,"reopen actual rows");f.opened=1;
    final=w_get(&f,"a",&n);ASSERT_NOT_NULL(final,"reopened physical row");free(final);free(before);w_close(&f);
}
static void w_mismatch(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}SlotcaskUpsertOpts o={.record_hdr2=SREC_FLAG_NULLMAP};
    const uint8_t raw[]={1,0,0};ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"a",1,raw,sizeof(raw),&o,NULL),0,"legacy raw seed");
    uint64_t p=w_pos(&f);char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,0,1,&out),1,"packed update rejects legacy OLD");free(out);
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),1,"packed upsert rejects legacy OLD");free(out);
    ASSERT_TRUE(w_pos(&f)==p,"generation failures before append");w_close(&f);
}
````````

<!-- R1-BLOCK s3e102-old -->
````````text
````````

<!-- R1-BLOCK s3e102-new -->
````````text
# Ordinary typed writes, reads and schema transfer

Packed storage remains inactive in public loaders/create-object. These internal
adapters prepare the ordinary routes for activation after criteria/indexes,
aggregates, other type grammars and durable migration are complete.

Ordinary readers retain the existing per-shard KF/segment read-handle lifetime.
Multi-key reads keep shard buckets, one I/O-pool job per touched shard and the
existing batched lookup/resolve-fetch pattern. They add no writer gate, writer
reservation, lock, lock-order change or object
snapshot guarantee. A fetch or multi-get can observe different committed times
across rows, exactly as ordinary readers already can. Responses are collected
before client I/O; cache handles are released before publication. An invalid live
reference fails the command instead of being silently omitted or called absent.
The private packed CSV route no longer reserves every shard's writer gate.

GET, fetch (default/rows/dict/CSV), projected values, multi-get, keys and existence
routes preserve explicit key lengths, JSON escaping and decimal number text.
Fetch keeps its existing envelope and numeric skip cursor. Multi-get keeps
request order and duplicate members; missing records render null. Bounded owned
response chunks share the request budget with selector copies and temporary
rendering allocations. Packed rendering reserves conservative decimal/join
workspace before validation; its length admission scan does not replace record
validation. Legacy decoder allocations are charged by capacity, not strlen.
Borrowed input/cache mappings and fixed automatic frames are not response heap.
Failed output streams return I/O failure without attempting another JSON envelope
following bytes already accepted by the stream.

Single computed updates evaluate every operand against immutable OLD, then
compose complete NEW. Numeric addition uses the decimal core and applies the
column's rounding once on assignment; `$copy` retains type/declaration compatibility.
Legacy scaled-int64 numeric uses exact addition and its real width/scale limit;
its historically informational P is not retroactively enforced. Every duplicate
computed occurrence is validated, including one later overwritten by a literal.
Memory admission covers retained payload owners, OLD numeric validation,
generated text plus its copied payload, and aligned decimal/result workspace
before allocation. Allocation failures preserve their resource-error identity.  Migration audits
that metadata separately. Bulk updates retain their existing literal-only contract;
criteria/per-row guards remain owned by R2. Packed bulk input rejects repeated
normalized storage keys before request submission, preserving its approved private
policy; the older public legacy bulk duplicate policy is unchanged. Auto-key bulk
inserts return generated keys in input order. Generator/sequence gaps after a
failed command are allowed; consumed sequence numbers are never rolled back.
Bulk accounting uses conservative request admission and thread-safe byte counters
inside existing worker callbacks; it changes no worker scheduling or lock protocol.

New create/add/edit declarations consume their complete JSON strings and type
parameters. Defaults have one modifier; literal defaults retain the 255-byte limit.
Malformed generators, overlength declarations, duplicate fields and currently
invalid scalar literal defaults fail before schema publication. Existing legacy
metadata retains its historical read interpretation. Malformed legacy default
syntax is marked unsafe for generation rather than making old stored records
unreadable. Calendar/bool/UUID/IP/string grammar expansion remains R4; these checks
reuse current checked encoders and do not claim those later contracts complete.

Describe includes the exact original `spec`, streams, auto-key identity and
`declaration_layout`. Logical varchar capacity is independent of descriptor width.
Argv/TUI schema export copies the exact spec; it never guesses numeric P=18.
Exports stage their complete manifest and preserve an existing destination on
failure. A packed manifest cannot be imported as legacy: create-object refuses
its layout marker until authoritative packed publication/migration arrives in R5.
Tagged schema manifests with a differing imported stream count are refused
explicitly until R5 can honor geometry; ordinary untagged create requests keep
their historical handling. No migration, schema binding or activation is
introduced here.

Ordinary CSV uses the approved reversible byte-escape layer after CSV decoding:
backslash doubles; control bytes use `\u00xx`. JSON remains the typed interchange
format because CSV still conflates NULL and an empty cell. Existing positional
bulk input is not an inverse CSV export parser. Npm exposes raw response text;
finance applications must preserve number tokens rather than passing arbitrary
precision decimals through JavaScript Number/JSON.parse. The optional pure-JS
cell decoder accepts bytes from a byte-preserving transport. The native query
string route remains UTF-8 text; it is not a new binary transport API.
````````

<!-- R1-BLOCK s3e103-old -->
````````text
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
````````

<!-- R1-BLOCK s3e103-new -->
````````text
# Packed CSV fetch and multi-get commands (public format inactive)

`packed_csv_fetch` and `packed_csv_get_multi` are concrete, packed-only read
backends. Existing fetch and multi-key GET select them only for CSV and an
explicitly bound packed schema. Loaders/create-object still never bind packed;
R1 routes ordinary legacy/non-CSV reads through the shared checked collector.
No new query mode, format flag or startup migration is introduced.

The caller holds the shared object lock and pins the matching registry instance
through the call. Tests may own a stable directly opened physical fixture.
Schema/options/input storage stays immutable for the duration. The backend
uses the existing per-shard KF/segment read handles while collecting values.
It adds no writer gate, reservation, lock or lock-order change. Read handles are
released before FILE output. Callbacks do no index work, network I/O or database
reentry. Different rows can reflect different committed times; no object-wide
snapshot or cross-request cursor snapshot is promised.

Checked traversal returns unreadable/invalid live references as errors instead
of silently dropping them. Skipped rows still require backing-reference checks;
limits stop further scanning once enough rows are collected. No global integrity
proof or throughput improvement is claimed.

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
their NULs and list nodes, selector copies, decoded-key allocation capacity,
and a conservative rendering allowance for decimal/composite workspaces.
Production uses QUERY_BUFFER_MB. Failure before first output is explicit;
there is no truncated fallback or unbounded response streaming. Borrowed request
input, storage cache mappings and fixed automatic frames are excluded. The
allowance is a conservative admission bound, not a schema-ceiling allocation.

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
existing cache read acquisition and stdio write are blocking operations;
no cancellation guarantee while waiting for those or client I/O is claimed.
Error paths release owned lines, cache handles and the caller's scoped registry
reference. Construction errors are returned as -1 with errno and a JSON error;
I/O failure also returns -1, with EIO.

Visible NUL encoding remains `\u0000` with doubled literal backslashes. Ordinary
CSV parsing precedes byte unescaping. Public import/SDK decoding is still an
activation gate, not delivered by these output backends. Find/joins, aggregate
CSV and other predicate/delete/index/maintenance consumers remain in
the Phase 3B inventory; exact numeric criteria/indexes and aggregate operations
retain their later roadmap owners. This child does not close Phase 3B.

Current ordinary read/write and declaration integration contracts are in
[ordinary type integration](ordinary-type-integration.md).
````````

<!-- R1-BLOCK s3e104-old -->
````````text
actual-size owning updates. [Bulk insert integration](packed-bulk-insert.md)
adds JSON and positional insert/upsert preparation under explicit schemas.
[Keyed bulk updates](packed-bulk-update.md) use the existing actual-size partial
constructors and preserve omitted OLD fields. Criteria/CAS updates, bulk-delete
and remaining read/index consumers stay future work; packed schemas remain
inactive in public loaders and creates.

[Checked packed CSV rendering](packed-csv-rendering.md) supplies owned exact
cells, headers and rows with reversible visible byte escapes. The private
[CSV fetch/multi-get backends](packed-csv-commands.md) connect those helpers to
their command entry points with checked failure handling. Legacy CSV writers,
find/join/aggregate/keys consumers and public packed activation remain separate.
````````

<!-- R1-BLOCK s3e104-new -->
````````text
actual-size owning updates. [Bulk insert integration](packed-bulk-insert.md)
adds JSON and positional insert/upsert preparation under explicit schemas.
[Keyed bulk updates](packed-bulk-update.md) use the existing actual-size partial
constructors and preserve omitted OLD fields. Criteria/CAS updates, bulk-delete
and remaining read/index consumers stay future work; packed schemas remain
inactive in public loaders and creates.

[Checked packed CSV rendering](packed-csv-rendering.md) supplies owned exact
cells, headers and rows with reversible visible byte escapes. The private
[CSV fetch/multi-get backends](packed-csv-commands.md) connect those helpers to
their command entry points with checked failure handling. Legacy CSV writers,
find/join/aggregate/keys consumers and public packed activation remain separate.

Current ordinary read/write and declaration integration contracts are in
[ordinary type integration](ordinary-type-integration.md).
````````

<!-- R1-BLOCK s3e105-old -->
````````text
| Platform | Architecture | Prebuild |
|---|---|---|
| Linux | x64 | ✓ |
| Linux | arm64 | ✓ |
| macOS | arm64 (Apple Silicon) | ✓ |
| Windows | — | ✗ not supported |

## Links

- [GitHub](https://github.com/sayyiditow/shard-db)
- [Full documentation](https://github.com/sayyiditow/shard-db/tree/main/docs)
- [Changelog](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md)
````````

<!-- R1-BLOCK s3e105-new -->
````````text
| Platform | Architecture | Prebuild |
|---|---|---|
| Linux | x64 | ✓ |
| Linux | arm64 | ✓ |
| macOS | arm64 (Apple Silicon) | ✓ |
| Windows | — | ✗ not supported |

## Links

- [GitHub](https://github.com/sayyiditow/shard-db)
- [Full documentation](https://github.com/sayyiditow/shard-db/tree/main/docs)
- [Changelog](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md)

### Exact decimal and CSV values

`query()` preserves response text. For finance fields, send numeric literals as
strings (for example `"1234567890123456.1234567890"`) and use a lossless JSON
parser that preserves number tokens when reading. JavaScript `Number` and
`JSON.parse` cannot preserve arbitrary decimal precision. Existing `JSON.parse`
examples above use bounded integers and are not a finance-decimal recipe.

Ordinary fetch/multi-get/keys/existence CSV cells escape literal backslash as `\\` and control bytes as `\u00xx`.
After normal CSV quote/delimiter decoding, `ShardDb.decodeCsvCellBytes(cell)`
returns the original bytes. A literal six-character `\u0000` and an actual NUL
remain distinct. Pass a `Uint8Array` from a byte-preserving transport for
non-UTF-8 content; the native `query()` string route is UTF-8 text. CSV still
conflates SQL NULL with an empty cell, so use JSON for typed record interchange.
The escape layer is not an instruction to feed rendered CSV directly into the
existing positional bulk-import grammar.
````````

<!-- R1-BLOCK s3e106-old -->
````````text
````````

<!-- R1-BLOCK s3e106-new -->
````````text
'use strict'

// Input is one cell AFTER ordinary CSV delimiter/quote decoding.
function decodeCsvCellBytes(cell) {
  const source = typeof cell === 'string' ? Buffer.from(cell, 'utf8') : cell
  if (!(source instanceof Uint8Array)) throw new TypeError('cell must be a string or Uint8Array')
  function scan(output) {
    let count = 0
    for (let i = 0; i < source.length; i++) {
      let byte = source[i]
      if (byte === 92) {
        if (++i >= source.length) throw new SyntaxError('incomplete CSV byte escape')
        if (source[i] === 92) byte = 92
        else {
          if (source[i] !== 117 || i + 4 >= source.length || source[i + 1] !== 48 || source[i + 2] !== 48)
            throw new SyntaxError('invalid CSV byte escape')
          const hex = n => n >= 48 && n <= 57 ? n - 48 : n >= 65 && n <= 70 ? n - 55 : n >= 97 && n <= 102 ? n - 87 : -1
          const high = hex(source[i + 3]), low = hex(source[i + 4])
          if (high < 0 || low < 0) throw new SyntaxError('invalid CSV byte escape')
          byte = high * 16 + low; i += 4
        }
      }
      if (output) output[count] = byte
      count++
    }
    return count
  }
  const output = Buffer.alloc(scan(null)); scan(output); return output
}
module.exports = decodeCsvCellBytes
````````

<!-- R1-BLOCK s3e107-old -->
````````text
export = ShardDb

declare class ShardDb {
  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a query asynchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns a Promise that resolves to the JSON response string. Parse with JSON.parse().
   * Multiple concurrent queries are safe — shard-db's worker pool handles parallelism.
   */
  query(body: ShardDb.QueryBody): Promise<string>
  query(json: string): Promise<string>

  /**
   * Register a callback to receive log events (errors, warnings, slow queries, etc.).
   * The callback fires on the JS thread after each query Promise resolves.
   * Multiple queries may be in flight concurrently; the callback is called once
   * per completed query, in completion order.
   * Pass null to unregister.
   *
````````

<!-- R1-BLOCK s3e107-new -->
````````text
export = ShardDb

declare class ShardDb {
  /** Decode one CSV-unquoted byte-escaped cell. NULL and empty remain indistinguishable. */
  static decodeCsvCellBytes(cell: string | Uint8Array): Uint8Array

  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a query asynchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns the raw response string (CSV when requested). JSON.parse converts
   * unquoted decimals to binary64 and can lose precision; retain number lexemes
   * with an application-selected lossless JSON parser for finance values.
   * Multiple concurrent queries are safe — shard-db's worker pool handles parallelism.
   */
  query(body: ShardDb.QueryBody): Promise<string>
  query(json: string): Promise<string>

  /**
   * Register a callback to receive log events (errors, warnings, slow queries, etc.).
   * The callback fires on the JS thread after each query Promise resolves.
   * Multiple queries may be in flight concurrently; the callback is called once
   * per completed query, in completion order.
   * Pass null to unregister.
   *
````````

<!-- R1-BLOCK s3e108-old -->
````````text
    object: string
    local: string
    /** "key" for a primary-key lookup, or any indexed field on the remote object. */
    remote: string
    /** Column prefix in the output. Defaults to the remote object name. */
    as?: string
    type?: 'inner' | 'left'
    fields?: string[]
  }

  /** One record in bulk-insert/bulk-update array form. */
  interface BulkRecord {
    key: string
    value: Record<string, unknown>
    /** Per-record CAS guard (bulk-update array form only). */
    if?: Criteria
  }

  type QueryBody =
    // ── CRUD ──────────────────────────────────────────────────────────────
    | { mode: 'get'
        dir: string; object: string
        /** Single-key form. */
        key?: string
        /** Multi-key form — response is {key: value | null}. */
        keys?: string[]
        fields?: Fields }

    | { mode: 'exists'
````````

<!-- R1-BLOCK s3e108-new -->
````````text
    object: string
    local: string
    /** "key" for a primary-key lookup, or any indexed field on the remote object. */
    remote: string
    /** Column prefix in the output. Defaults to the remote object name. */
    as?: string
    type?: 'inner' | 'left'
    fields?: string[]
  }

  /** One record in bulk-insert/bulk-update array form. */
  interface BulkRecord {
    /** Omit only for bulk inserts into an auto-key object. */
    key?: string
    value: Record<string, unknown>
    /** Per-record CAS guard (bulk-update array form only). */
    if?: Criteria
  }

  interface BulkUpdateRecord extends BulkRecord { key: string }

  type QueryBody =
    // ── CRUD ──────────────────────────────────────────────────────────────
    | { mode: 'get'
        dir: string; object: string
        /** Single-key form. */
        key?: string
        /** Multi-key form — response is {key: value | null}. */
        keys?: string[]
        fields?: Fields }

    | { mode: 'exists'
````````

<!-- R1-BLOCK s3e109-old -->
````````text
        /** By criteria — mutually exclusive with `keys`. */
        criteria?: Criteria
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update'
        dir: string; object: string
        /** Criteria-driven mass update. */
        criteria?: Criteria
        value?: Record<string, unknown>
        /** Per-key partial update — dict or array form. Mutually exclusive with criteria/value. */
        records?: Record<string, Record<string, unknown>> | BulkRecord[]
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update-delimited'
        dir: string; object: string
        file?: string
        data?: string
        delimiter?: string }

    // ── Query ─────────────────────────────────────────────────────────────
    | { mode: 'find'
````````

<!-- R1-BLOCK s3e109-new -->
````````text
        /** By criteria — mutually exclusive with `keys`. */
        criteria?: Criteria
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update'
        dir: string; object: string
        /** Criteria-driven mass update. */
        criteria?: Criteria
        value?: Record<string, unknown>
        /** Per-key partial update — dict or array form. Mutually exclusive with criteria/value. */
        records?: Record<string, Record<string, unknown>> | BulkUpdateRecord[]
        file?: string
        limit?: number
        dry_run?: boolean }

    | { mode: 'bulk-update-delimited'
        dir: string; object: string
        file?: string
        data?: string
        delimiter?: string }

    // ── Query ─────────────────────────────────────────────────────────────
    | { mode: 'find'
````````

<!-- R1-BLOCK s3e110-old -->
````````text
        cursor?: Record<string, unknown> | null
        /** Return full match count alongside the page. */
        total?: boolean
        /** Response format — 'dict' returns a {key: value} object instead of an array. Ignored (forced tabular) when `join` is present. */
        format?: 'rows' | 'dict'
        join?: Join[]
        explain?: boolean }

    | { mode: 'fetch'
        dir: string; object: string
        offset?: number; limit?: number
        fields?: Fields
        format?: 'dict' }

    | { mode: 'count'
        dir: string; object: string
        criteria?: Criteria }

    | { mode: 'aggregate'
        dir: string; object: string
        aggregates: Aggregate[]
        group_by?: string[]
        criteria?: Criteria
        /** Same shape as criteria, but fields are aggregate aliases (or group-by fields). */
        having?: Criteria
````````

<!-- R1-BLOCK s3e110-new -->
````````text
        cursor?: Record<string, unknown> | null
        /** Return full match count alongside the page. */
        total?: boolean
        /** Response format — 'dict' returns a {key: value} object instead of an array. Ignored (forced tabular) when `join` is present. */
        format?: 'rows' | 'dict'
        join?: Join[]
        explain?: boolean }

    | { mode: 'fetch'
        dir: string; object: string
        offset?: number; limit?: number
        fields?: Fields
        format?: 'rows' | 'dict' | 'csv'
        delimiter?: string
        cursor?: string | null
        total?: boolean }

    | { mode: 'count'
        dir: string; object: string
        criteria?: Criteria }

    | { mode: 'aggregate'
        dir: string; object: string
        aggregates: Aggregate[]
        group_by?: string[]
        criteria?: Criteria
        /** Same shape as criteria, but fields are aggregate aliases (or group-by fields). */
        having?: Criteria
````````

<!-- R1-BLOCK s3e111-old -->
````````text
    return binding.query(this._handle, json)
  }

  close() {
    binding.close(this._handle)
  }

  setLogHandler(fn) {
    binding.setLogHandler(this._handle, fn ?? null)
  }
}

ShardDb.LOG_TYPES = LOG_TYPES
module.exports = ShardDb
````````

<!-- R1-BLOCK s3e111-new -->
````````text
    return binding.query(this._handle, json)
  }

  close() {
    binding.close(this._handle)
  }

  setLogHandler(fn) {
    binding.setLogHandler(this._handle, fn ?? null)
  }
}

ShardDb.decodeCsvCellBytes = require('./csv-bytes')
ShardDb.LOG_TYPES = LOG_TYPES
module.exports = ShardDb
````````

<!-- R1-BLOCK s3e112-old -->
````````text
````````

<!-- R1-BLOCK s3e112-new -->
````````text
'use strict'
const assert = require('node:assert/strict')
const decode = require('../csv-bytes')
function encode(bytes) {
  return Buffer.concat(Array.from(bytes, byte => byte === 92 ? Buffer.from('\\\\') :
    byte < 32 || byte === 127 ? Buffer.from('\\u00' + byte.toString(16).padStart(2, '0')) : Buffer.from([byte])))
}
let state = 20261027
for (let caseNo = 0; caseNo < 2000; caseNo++) {
  const bytes = Buffer.alloc(caseNo % 513)
  for (let i = 0; i < bytes.length; i++) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    bytes[i] = state >>> 24
  }
  assert.deepEqual(decode(encode(bytes)), bytes)
}
const everyByte = Buffer.from(Array.from({ length: 256 }, (_, i) => i))
assert.deepEqual(decode(encode(everyByte)), everyByte)
assert.deepEqual(decode('\\u0000'), Buffer.from([0]))
assert.deepEqual(decode('\\\\u0000'), Buffer.from('\\u0000'))
assert.deepEqual(decode('é'), Buffer.from('é'))
for (const bad of ['\\', '\\z', '\\u00gg', '\\u1234', '\\u000']) assert.throws(() => decode(bad), SyntaxError)
assert.throws(() => decode(1), TypeError)
console.log('CSV byte decoder: 2000 seeded round trips, all 256 bytes, NUL/literal distinction and invalid escapes pass')
````````

<!-- R1-BLOCK driver -->
````````python
#!/usr/bin/env python3
import argparse,hashlib,json,re,subprocess,sys
from pathlib import Path
FENCE='`'*8
BLOCK=re.compile(r'<!-- R1-BLOCK ([A-Za-z0-9_-]+) -->\n'+FENCE+r'[^\n]*\n(.*?)^'+FENCE+r'\n',re.S|re.M)
def digest(data):return hashlib.sha256(data).hexdigest()
def load(plan):
 items=BLOCK.findall(plan.read_text());blocks=dict(items)
 assert len(items)==len(blocks),'duplicate frozen block IDs'
 return blocks,json.loads(blocks['manifest'])
def run():
 p=argparse.ArgumentParser();p.add_argument('--plan',required=True);p.add_argument('--stage',type=int,choices=[1,2,3],required=True);p.add_argument('--check-only',action='store_true');p.add_argument('--verify',action='store_true');p.add_argument('--scratch',action='store_true');a=p.parse_args()
 root=Path.cwd();blocks,m=load(Path(a.plan))
 if not a.scratch:
  head=subprocess.check_output(['rtk','proxy','git','rev-parse','HEAD'],text=True).strip()
  branch=subprocess.check_output(['rtk','proxy','git','branch','--show-current'],text=True).strip()
  assert head==m['base'] and branch==m['branch'],'wrong checkout; do not branch/reset'
  protected=root/m['protected']['path'];assert digest(protected.read_bytes())==m['protected']['sha'],'protected checksum mismatch'
 else:
  assert root.as_posix().startswith('/tmp/'),'scratch mode is limited to /tmp'
 for path,want in m['boundaries'].items():assert digest((root/path).read_bytes())==want,'boundary changed: '+path
 checkpoint=a.stage if a.verify else a.stage-1
 for path,want in m['checkpoints'][str(checkpoint)].items():
  target=root/path;got=digest(target.read_bytes()) if target.exists() else None
  assert got==want,'checkpoint mismatch: '+path
 if a.verify:print('checkpoint',checkpoint,'verified');return
 changes={}
 for e in m['edits']:
  if e['stage']!=a.stage:continue
  target=root/e['path'];text=changes.get(e['path'],target.read_text() if target.exists() else None)
  old=blocks[e['old']];new=blocks[e['new']]
  if e['create']:assert text is None,'new file exists: '+e['path'];text=new
  else:
   assert text is not None and text.count(old)==1,'anchor must match exactly once: '+e['old']
   text=text.replace(old,new,1)
  changes[e['path']]=text
 for path,text in changes.items():assert digest(text.encode())==m['checkpoints'][str(a.stage)][path],'postimage mismatch: '+path
 if a.check_only:print('stage',a.stage,'preflight:',len(changes),'files, no writes');return
 for path,text in changes.items():
  target=root/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(text)
 print('stage',a.stage,'applied:',len(changes),'files; exact postimages verified')
if __name__=='__main__':
 try:run()
 except Exception as error:
  print('HALT:',error,file=sys.stderr);sys.exit(1)
````````

<!-- R1-BLOCK checks -->
````````python
#!/usr/bin/env python3
import argparse,hashlib,json,re,subprocess,tempfile,resource
from pathlib import Path
FENCE='`'*8
BLOCK=re.compile(r'<!-- R1-BLOCK ([A-Za-z0-9_-]+) -->\n'+FENCE+r'[^\n]*\n(.*?)^'+FENCE+r'\n',re.S|re.M)
def run(args,**kw):return subprocess.run(['rtk','proxy',*args],check=True,**kw)
def main():
 resource.setrlimit(resource.RLIMIT_CORE,(0,0))
 p=argparse.ArgumentParser();p.add_argument('--plan',required=True);p.add_argument('--output',required=True);a=p.parse_args()
 blocks=dict(BLOCK.findall(Path(a.plan).read_text()));m=json.loads(blocks['manifest']);out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
 assert out.resolve().as_posix().startswith('/tmp/'),'probe artifacts belong in /tmp'
 source=Path('src/db/slotcask.c').read_bytes();n=m['concurrency_prefix']['length']
 assert hashlib.sha256(source[:n]).hexdigest()==m['concurrency_prefix']['sha']
 added=source[n:].decode()
 assert not re.search(r'writer_gate|write_reservation|pthread_(?:mutex|rwlock|cond)',added)
 assert 'write_reservation' not in Path('src/db/packed_csv_command.c').read_text()
 references=sum(p.read_text().count('typed_schema_bind_packed(') for p in Path('src/db').glob('*.[ch]'))
 assert references==2,'packed binder has a new production caller'
 print('concurrency prefix unchanged; checked readers add no locks/gates; CSV has no reservation; public binder inactive',flush=True)
 probe=out/'alloc_probe.c';probe.write_text(blocks['alloc-probe']);objects=[]
 for name in ['typed_read_command','typed_row','schema_wire','config','packed_csv_command','packed_record','decnum','util','slotcask']:
  obj=out/(name+'.o');objects.append(str(obj))
  run(['cc','-std=gnu11','-O1','-g','-fno-lto','-DTEST_BUILD',*(['-D_GNU_SOURCE'] if name!='slotcask' else []),'-D_XOPEN_SOURCE=700',
   '-Dmalloc=r1_malloc','-Dcalloc=r1_calloc','-Drealloc=r1_realloc','-Dfree=r1_free',
   '-Isrc/db','-c','src/db/'+name+'.c','-o',str(obj)])
 executable=out/'alloc_probe'
 run(['cc','-std=gnu11','-O1','-Isrc/db',str(probe),*objects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(executable)])
 run([str(executable)])

 for key in ['review-probe','old-quota-probe']:
  file=out/(key+'.c');file.write_text(blocks[key]);exe=out/key
  run(['cc','-std=gnu11','-O1','-Isrc/db',str(file),*objects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(exe)])
  run([str(exe)])
 reader=out/'reader.o';run(['cc','-std=gnu11','-O1','-fno-lto','-Isrc/db','-c','src/db/typed_read_command.c','-o',str(reader)])
 schedule=out/'schedule.c';schedule.write_text(blocks['schedule-probe'])
 wrapping=['-Wl,--wrap=parallel_for_io','-Wl,--wrap=slotcask_bulk_resolve_and_fetch_checked','-Wl,--wrap=slotcask_bulk_lookup_in_kfshard_checked']
 run(['cc','-std=gnu11','-O1','-Isrc/db',str(schedule),str(reader),'build/bin/libshard-db.a',*wrapping,'-lpthread','-lssl','-lcrypto','-o',str(out/'schedule')])
 run([str(out/'schedule')])
 for key in ['collect-probe','collect-red-probe']:
  file=out/(key+'.c');file.write_text(blocks[key]);run(['cc','-std=gnu11','-O1',str(file),'-o',str(out/key)])
 run([str(out/'collect-probe')])
 # Review defects reproduce in scratch baseline modules only. These files
 # are never applied to the checkout or copied into production sources.
 baseline=out/'old-row.c';baseline.write_text(blocks['review-baseline-row']);oldobj=out/'old-row.o'
 run(['cc','-std=gnu11','-O1','-fno-lto','-DTEST_BUILD','-D_GNU_SOURCE','-D_XOPEN_SOURCE=700','-Dmalloc=r1_malloc','-Dcalloc=r1_calloc','-Drealloc=r1_realloc','-Dfree=r1_free','-Isrc/db','-c',str(baseline),'-o',str(oldobj)])
 oldobjects=[str(oldobj) if Path(o).name=='typed_row.o' else o for o in objects]
 for key in ['review-probe','old-quota-probe']:
  exe=out/(key+'-red');run(['cc','-std=gnu11','-O1','-Isrc/db',str(out/(key+'.c')),*oldobjects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(exe)])
  result=subprocess.run(['rtk','proxy',str(exe)]);assert result.returncode==1,'review defect did not reproduce: '+key
 exe=out/'computed-errno-red'
 run(['cc','-std=gnu11','-O1','-Isrc/db',str(probe),*oldobjects,'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(exe)])
 assert subprocess.run(['rtk','proxy',str(exe)]).returncode==1,'computed OOM error-identity defect did not reproduce'
 baseline=out/'old-reader.c';baseline.write_text(blocks['review-baseline-reader']);oldobj=out/'old-reader.o'
 run(['cc','-std=gnu11','-O1','-fno-lto','-Isrc/db','-c',str(baseline),'-o',str(oldobj)])
 run(['cc','-std=gnu11','-O1','-Isrc/db',str(schedule),str(oldobj),'build/bin/libshard-db.a',*wrapping,'-lpthread','-lssl','-lcrypto','-o',str(out/'schedule-red')])
 assert subprocess.run(['rtk','proxy',str(out/'schedule-red')]).returncode==1,'sequential dispatch defect did not reproduce'
 assert subprocess.run(['rtk','proxy',str(out/'collect-red-probe')]).returncode in (-11,139),'unterminated response defect did not reproduce'
 print('all review baselines reproduced their defects; fixed-source probes passed',flush=True)
 # The frozen transport probe is extracted verbatim from the final CLI helper
 # bodies. Driver checkpoint verification precedes this check.
 tui=out/'tui_probe.c';tui.write_text(blocks['tui-probe'])
 run(['cc','-std=gnu11','-O1','-Wall','-Wextra','-Wno-misleading-indentation',str(tui),'-o',str(out/'tui_probe')])
 run([str(out/'tui_probe')]);run(['node','npm/test/csv-bytes.js'])
 corruption=out/'corrupt_flag.c';corruption.write_text(blocks['corrupt-flag-probe'])
 fixed=out/'corrupt_flag';run(['cc','-std=gnu11','-O1','-Isrc/db',str(corruption),'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(fixed)])
 run([str(fixed)])
 baseline=out/'old_checked_slotcask.c';baseline.write_bytes(source[:n]+blocks['corrupt-flag-baseline'].encode())
 oldobj=out/'old_checked_slotcask.o'
 run(['cc','-std=gnu11','-O1','-fno-lto','-Isrc/db','-c',str(baseline),'-o',str(oldobj)])
 red=out/'corrupt_flag_red';run(['cc','-std=gnu11','-O1','-Isrc/db',str(corruption),str(oldobj),'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(red)])
 result=subprocess.run(['rtk','proxy',str(red)],capture_output=True,text=True)
 print(result.stdout,flush=True)
 assert result.returncode==1 and 'failures=3' in result.stdout,'bulk corruption defect did not reproduce'
 assert all(name+' corrupt: rc=-1 errno=' in result.stdout for name in ['single']),'baseline fixture invalid'
 assert all(name+' tombstone: rc=0 errno=0' in result.stdout for name in ['exists','revalidation']),'baseline tombstone behavior changed'
 print('checked bulk corrupt flags: baseline RED reproduced; final GREEN and tombstone controls passed',flush=True)
 export=out/'export_probe.py';export.write_text(blocks['export-probe'])
 run(['python3',str(export),str(Path('shard-db').resolve())])
 baseline=out/'old_export_server.c';server=Path('src/db/server.c').read_text()
 assert server.count(blocks['export-tenant-final'])==1
 baseline.write_text(server.replace(blocks['export-tenant-final'],blocks['export-tenant-baseline'],1))
 oldobj=out/'old_export_server.o'
 run(['cc','-std=gnu11','-O1','-Isrc/db','-c',str(baseline),'-o',str(oldobj)])
 oldbin=out/'old_export_daemon'
 run(['cc','-std=gnu11','-O1','-Isrc/db','src/db/main.c',str(oldobj),'build/bin/libshard-db.a','-lpthread','-lssl','-lcrypto','-o',str(oldbin)])
 red=subprocess.run(['rtk','proxy','python3',str(export),str(oldbin)],capture_output=True,text=True)
 assert red.returncode==1 and red.stdout.count('FAIL ')==6 and 'fake server failure' not in red.stdout,'export defect did not reproduce honestly: '+red.stdout+red.stderr
 assert all('PASS empty tenant success / '+mode in red.stdout for mode in ['named','stdout']),'baseline success control failed'
 print('export regression: six baseline RED scenarios reproduced; all 18 final GREEN scenarios passed',flush=True)

 print('R1 checks complete',flush=True)
if __name__=='__main__':main()
````````

<!-- R1-BLOCK alloc-probe -->
````````c
#define _GNU_SOURCE
#include "typed_read_command.h"
#include "schema_wire.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
static int active,ordinal,fail_at,live;
int index_test_abort_fail_after,btree_test_publish_fail_stage,btri_stamp_test_inject;
static void *pointers[4096];
static void remember(void *p){if(active&&p){for(int i=0;i<4096;i++)if(!pointers[i]){pointers[i]=p;live++;return;}abort();}}
static void forget(void *p){if(!p)return;for(int i=0;i<4096;i++)if(pointers[i]==p){pointers[i]=NULL;live--;return;}}
void *r1_malloc(size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=malloc(n);remember(p);return p;}
void *r1_calloc(size_t n,size_t z){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=calloc(n,z);remember(p);return p;}
void *r1_realloc(void *p,size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *q=realloc(p,n);if(q){forget(p);remember(q);}return q;}
void r1_free(void *p){forget(p);free(p);}
int main(void){
 shard_db_offline_init("/tmp/r1-probe-offline");
 g_query_buffer_max_bytes=256u*1024u*1024u;
 TypedSchema *ts=calloc(1,sizeof(*ts));if(!ts){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 ts->typed=1;ts->nfields=2;ts->fields[0].type=FT_NUMERIC;ts->fields[1].type=FT_VARCHAR;
 strcpy(ts->fields[0].name,"cash");ts->fields[0].name_len=4;
 strcpy(ts->fields[1].name,"note");ts->fields[1].name_len=4;
 const char *types[]={"numeric:26,10","varchar"};
 if(typed_schema_bind_packed(ts,types,2,NULL,0)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 uint8_t *bytes=NULL;size_t len=0;
 if(typed_encode_alloc(ts,"{\"cash\":\"1234567890123456.1234567890\",\"note\":\"hello\"}",0,NULL,NULL,&bytes,&len,NULL,0)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 size_t bound=0;if(pkr_slot_bound(&ts->packed,32,&bound)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 char root[]="/tmp/r1-alloc-XXXXXX";if(!mkdtemp(root)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 SlotcaskDb db;slotcask_init(64,64);if(slotcask_open(&db,root,1,1,(int)bound)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};if(slotcask_upsert_with_hooks(&db,-1,"a",1,bytes,len,&opts,NULL)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
 Schema sc={.splits=1,.streams=1,.max_key=32,.slot_size=(int)bound};FieldSchema fs={.ts=ts};
 int checked=0;
 for(int route=0;route<6;route++){
  int count=0;
  for(int nth=0;nth<=count;nth++){
   char *text=NULL;size_t used=0;FILE *out=open_memstream(&text,&used);if(!out){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
   ordinal=0;fail_at=nth;active=1;int rc=0;
   if(route<2||route==4){TypedReadOptions o={.kind=route==1?TYPED_READ_GET_MULTI:TYPED_READ_FETCH,.format=route==4?"csv":NULL,.delimiter=',',.keys="[\"a\",\"missing\"]",.fields="note,cash",.limit=10,.budget=1048576};rc=typed_read_command(&db,&fs,&sc,&o,out);}
   else if(route==5){rc=typed_read_single(&db,&fs,"a",1,NULL,1048576,out);}
   else if(route==2){
    SlotcaskOldRecord old={.value=bytes,.vlen=len,.hdr2=PKR_FLAGS};uint8_t *updated=NULL;size_t size=0;
    rc=typed_update_computed_alloc(ts,&old,"{\"cash\":{\"$incr\":\"0.00000000005\"}}",&updated,&size,NULL,0);
    r1_free(updated);
   }else{
    char *specs[MAX_FIELDS];size_t n=0;rc=typed_field_specs_parse("[\"cash:numeric:26,10\",\"note:varchar:10\"]",specs,&n);
    for(size_t i=0;i<n;i++)r1_free(specs[i]);
   }
   int cause=errno;
   if(route==2&&nth&&rc&&cause!=ENOMEM){fprintf(stderr,"computed allocation failure lost errno at ordinal %d: %d\n",nth,cause);return 1;}
   active=0;if(fclose(out)){fprintf(stderr,"setup error line %d errno %d\n",__LINE__,errno);return 2;}
   if(nth==0){if(rc)return fprintf(stderr,"baseline route=%d rc=%d errno=%d\n",route,rc,errno),1;count=ordinal;}
   else{
    if(rc==0||live){fprintf(stderr,"failure route=%d ordinal=%d allocations=%d rc=%d live=%d\n",route,nth,ordinal,rc,live);return 1;}
    if((route<2||route>=4)&&(!text||strncmp(text,"{\"error\":",9))){fprintf(stderr,"partial response route=%d ordinal=%d\n",route,nth);return 1;}
    checked++;
   }
   if(live){fprintf(stderr,"leak route=%d nth=%d live=%d\n",route,nth,live);return 1;}free(text);
  }
  printf("route %d selected allocations=%d\n",route,count);
 }

 sc.auto_key=AK_SEQ;sc.max_key=8;sc.max_value=ts->total_size;strcpy(sc.auto_key_seq_name,"r1probe");
 const char *batch="[{\"value\":{\"cash\":\"1.0000000000\"}},{\"value\":{\"cash\":\"2.0000000000\"}}]";
 char *response=NULL;size_t response_len=0;FILE *sink=open_memstream(&response,&response_len);if(!sink)return 2;
 FILE *saved=g_out;g_out=sink;
 int bulk=typed_bulk_insert_command(root,"obj",&sc,ts,&db,batch,strlen(batch),0,',',0);
 g_out=saved;if(fclose(sink))return 2;
 if(bulk||!response||!strstr(response,"\"keys\":[\"1\",\"2\"]")){fprintf(stderr,"sequence auto-key probe failed: %s\n",response?response:"NULL");return 1;}
 free(response);puts("sequence bulk auto-key: keys 1,2 in input order");
 slotcask_close(&db);slotcask_shutdown();free(bytes);free(ts);
 printf("allocation failure checks=%d, live=0\n",checked);return 0;
}
````````

<!-- R1-BLOCK tui-probe -->
````````c
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
const char *cli_json_skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}
const char *cli_json_skip_value(const char **p) {
    *p = cli_json_skip_ws(*p);
    const char *start = *p;
    if (**p == '"') {
        (*p)++;
        while (**p && **p != '"') {
            if (**p == '\\' && *(*p + 1)) (*p)++;
            (*p)++;
        }
        if (**p == '"') (*p)++;
    } else if (**p == '{' || **p == '[') {
        char open = **p, close = (open == '{' ? '}' : ']');
        int depth = 0;
        do {
            if (**p == '"') {
                (*p)++;
                while (**p && **p != '"') {
                    if (**p == '\\' && *(*p + 1)) (*p)++;
                    (*p)++;
                }
                if (**p == '"') (*p)++;
                continue;
            }
            if (**p == open) depth++;
            else if (**p == close) depth--;
            if (**p) (*p)++;
        } while (depth > 0 && **p);
    } else {
        while (**p && **p != ',' && **p != '}' && **p != ']' &&
               **p != ' ' && **p != '\n' && **p != '\t' && **p != '\r')
            (*p)++;
    }
    return start;
}
const char *cli_json_find_key(const char *json, const char *key, size_t *value_len) {
    size_t klen = strlen(key);
    const char *p = cli_json_skip_ws(json);
    if (*p != '{') return NULL;
    p++;
    while (*p) {
        p = cli_json_skip_ws(p);
        if (*p == '}') return NULL;
        if (*p != '"') return NULL;
        p++;
        const char *kstart = p;
        while (*p && *p != '"') p++;
        size_t this_klen = p - kstart;
        if (*p == '"') p++;
        p = cli_json_skip_ws(p);
        if (*p != ':') return NULL;
        p++;
        p = cli_json_skip_ws(p);
        const char *vstart = p;
        cli_json_skip_value(&p);
        if (this_klen == klen && memcmp(kstart, key, klen) == 0) {
            *value_len = (size_t)(p - vstart);
            return vstart;
        }
        p = cli_json_skip_ws(p);
        if (*p == ',') p++;
    }
    return NULL;
}
static int transfer_hex(unsigned char c){
    if(c>='0'&&c<='9')return c-'0';
    if(c>='a'&&c<='f')return c-'a'+10;
    if(c>='A'&&c<='F')return c-'A'+10;
    return -1;
}
static int transfer_code(const char **text,unsigned *code){
    unsigned value=0;
    for(int i=0;i<4;i++){
        int digit=transfer_hex((unsigned char)**text);if(digit<0)return -1;
        (*text)++;value=value*16+(unsigned)digit;
    }
    *code=value;return 0;
}
static char *transfer_string(const char **text){
    const char *start=*text;if(*start!='"')return NULL;
    const char *end=start;cli_json_skip_value(&end);
    size_t raw=(size_t)(end-start);
    if(raw<2||end[-1]!='"')return NULL;
    char *out=malloc(raw);if(!out)return NULL;
    size_t n=0;const char *p=start+1;
    while(p<end-1){
        unsigned code=(unsigned char)*p++;
        if(code<32)goto fail;
        if(code=='\\'){
            unsigned char e=(unsigned char)*p++;
            switch(e){
            case '"':code='"';break;case '\\':code='\\';break;case '/':code='/';break;
            case 'b':code=8;break;case 'f':code=12;break;case 'n':code=10;break;
            case 'r':code=13;break;case 't':code=9;break;
            case 'u':
                if(end-1-p<4||transfer_code(&p,&code))goto fail;
                if(code>=0xd800&&code<=0xdbff){
                    unsigned low=0;
                    if(end-1-p<6||p[0]!='\\'||p[1]!='u')goto fail;
                    p+=2;if(transfer_code(&p,&low)||low<0xdc00||low>0xdfff)goto fail;
                    code=0x10000+((code-0xd800)<<10)+(low-0xdc00);
                }else if(code>=0xdc00&&code<=0xdfff)goto fail;
                if(!code)goto fail;
                if(code>=0x10000){out[n++]=(char)(0xf0|(code>>18));out[n++]=(char)(0x80|((code>>12)&63));}
                else if(code>=0x800)out[n++]=(char)(0xe0|(code>>12));
                if(code>=0x80)out[n++]=(char)(code>=0x800?0x80|((code>>6)&63):0xc0|(code>>6));
                code=code<0x80?code:0x80|(code&63);break;
            default:goto fail;
            }
        }
        out[n++]=(char)code;
    }
    out[n]=0;*text=end;return out;
fail:free(out);return NULL;
}
static int collect_string_array(const char *resp,const char *key,char ***out_arr,int *out_n){
    if(!out_arr||!out_n)return -1;
    *out_arr=NULL;*out_n=0;
    size_t length=0;const char *array=key?cli_json_find_key(resp,key,&length):resp;
    const char *p=array?cli_json_skip_ws(array):NULL;
    if(!p||*p++!='[')return -1;
    char **rows=NULL;int count=0;size_t capacity=0;
    p=cli_json_skip_ws(p);
    while(*p!=']'){
        char *value=transfer_string(&p);if(!value)goto fail;
        if((size_t)count==capacity){
            size_t next=capacity?capacity*2:16;
            if(next>SIZE_MAX/sizeof(*rows)||next>INT_MAX){free(value);goto fail;}
            char **grown=realloc(rows,next*sizeof(*rows));if(!grown){free(value);goto fail;}
            rows=grown;capacity=next;
        }
        rows[count++]=value;p=cli_json_skip_ws(p);
        if(*p==']')break;
        if(*p++!=',')goto fail;
        p=cli_json_skip_ws(p);if(*p==']')goto fail;
    }
    if(!key&&*cli_json_skip_ws(p+1))goto fail;
    *out_arr=rows;*out_n=count;return 0;
fail:
    for(int i=0;i<count;i++)free(rows[i]);
    free(rows);return -1;
}

int main(void){
 const char *j="[\"a\\\\b\",\"caf\\u00e9\",\"\\ud83d\\ude00\"]";
 char **out=NULL;int n=0;
 if(collect_string_array(j,NULL,&out,&n)||n!=3||strcmp(out[0],"a\\b")||strcmp(out[1],"caf\xc3\xa9")||strcmp(out[2],"\xf0\x9f\x98\x80"))return 1;
 for(int i=0;i<n;i++)free(out[i]);free(out);
 const char *bad[]={"[\"a\",]","[\"\\u0000\"]","[\"\\ud800\"]","[\"\\udc00\"]","[7]","[]tail"};
 for(size_t i=0;i<sizeof(bad)/sizeof(*bad);i++)if(!collect_string_array(bad[i],NULL,&out,&n))return 2;
 puts("TUI declaration transport: byte escaping, UTF-8, surrogate pair, six malformed arrays pass");return 0;
}
````````

<!-- R1-BLOCK review-probe -->
````````c
#define _GNU_SOURCE
#include "typed_read_command.h"
#include "schema_wire.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
static int active,ordinal,fail_at,live;
int index_test_abort_fail_after,btree_test_publish_fail_stage,btri_stamp_test_inject;
static void *pointers[4096];
static size_t sizes[4096],bytes_live,peak;
static void remember(void *p,size_t size){if(active&&p){for(int i=0;i<4096;i++)if(!pointers[i]){pointers[i]=p;sizes[i]=size;bytes_live+=size;if(bytes_live>peak)peak=bytes_live;live++;return;}abort();}}
static void forget(void *p){if(!p)return;for(int i=0;i<4096;i++)if(pointers[i]==p){pointers[i]=NULL;bytes_live-=sizes[i];sizes[i]=0;live--;return;}}
void *r1_malloc(size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=malloc(n);remember(p,n);return p;}
void *r1_calloc(size_t n,size_t z){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=calloc(n,z);remember(p,n*z);return p;}
void *r1_realloc(void *p,size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *q=realloc(p,n);if(q){forget(p);remember(q,n);}return q;}
void r1_free(void *p){forget(p);free(p);}

int main(void){
 shard_db_offline_init("/tmp/r1-review-probe");g_query_buffer_max_bytes=256u*1024u*1024u;
 int failures=0;TypedSchema *ts=calloc(1,sizeof(*ts));ts->typed=1;ts->nfields=1;
 strcpy(ts->fields[0].name,"cash");ts->fields[0].name_len=4;ts->fields[0].type=FT_NUMERIC;
 const char *decl[]={"numeric:26,10"};if(typed_schema_bind_packed(ts,decl,1,NULL,0))return 2;
 uint8_t *old=NULL,*updated=NULL;size_t len=0,size=0;
 if(typed_encode_alloc(ts,"{\"cash\":1}",0,NULL,NULL,&old,&len,NULL,0))return 2;
 SlotcaskOldRecord record={.value=old,.vlen=len,.hdr2=PKR_FLAGS};
 int rc=typed_update_computed_alloc(ts,&record,"{\"cash\":{\"$incr\":\"junk\"},\"cash\":1}",&updated,&size,NULL,0);
 printf("computed duplicate rc=%d (must reject)\n",rc);if(!rc)failures++;
 r1_free(updated);free(old);free(ts);
 ts=calloc(1,sizeof(*ts));ts->typed=1;ts->nfields=2;const char *decl2[]={"varchar:65535","varchar:65535"};
 for(int i=0;i<2;i++){ts->fields[i].type=FT_VARCHAR;snprintf(ts->fields[i].name,256,"f%d",i);ts->fields[i].name_len=2;ts->fields[i].default_kind=DK_RANDOM;strcpy(ts->fields[i].default_val,"20000");}
 if(typed_schema_bind_packed(ts,decl2,2,NULL,0))return 2;
 g_query_buffer_max_bytes=65536;active=1;ordinal=0;fail_at=0;peak=0;
 updated=NULL;size=0;rc=typed_encode_alloc(ts,"{}",1,NULL,NULL,&updated,&size,NULL,0);r1_free(updated);active=0;
 printf("defaults rc=%d peak=%zu budget=65536 live=%d\n",rc,peak,live);
 if(!rc||peak>65536||live)failures++;
 free(ts);printf("review gates failed=%d\n",failures);return failures?1:0;
}
````````

<!-- R1-BLOCK old-quota-probe -->
````````c
#define _GNU_SOURCE
#include "typed_read_command.h"
#include "schema_wire.h"
#include "typed_bulk_insert.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
static int active,ordinal,fail_at,live;
int index_test_abort_fail_after,btree_test_publish_fail_stage,btri_stamp_test_inject;
static void *pointers[4096];
static size_t sizes[4096],bytes_live,peak;
static void remember(void *p,size_t size){if(active&&p){for(int i=0;i<4096;i++)if(!pointers[i]){pointers[i]=p;sizes[i]=size;bytes_live+=size;if(bytes_live>peak)peak=bytes_live;live++;return;}abort();}}
static void forget(void *p){if(!p)return;for(int i=0;i<4096;i++)if(pointers[i]==p){pointers[i]=NULL;bytes_live-=sizes[i];sizes[i]=0;live--;return;}}
void *r1_malloc(size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=malloc(n);remember(p,n);return p;}
void *r1_calloc(size_t n,size_t z){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *p=calloc(n,z);remember(p,n*z);return p;}
void *r1_realloc(void *p,size_t n){if(active&&++ordinal==fail_at){errno=ENOMEM;return NULL;}void *q=realloc(p,n);if(q){forget(p);remember(q,n);}return q;}
void r1_free(void *p){forget(p);free(p);}

int main(void){
 shard_db_offline_init("/tmp/r1-correctness-probe-root");g_query_buffer_max_bytes=256u*1024u*1024u;
 TypedSchema *ts=calloc(1,sizeof(*ts));ts->typed=1;ts->nfields=1;
 strcpy(ts->fields[0].name,"cash");ts->fields[0].name_len=4;ts->fields[0].type=FT_NUMERIC;
 const char *decl[]={"numeric"};if(typed_schema_bind_packed(ts,decl,1,NULL,0))return 2;
 char *json=malloc(60020);memcpy(json,"{\"cash\":",8);memset(json+8,'7',60000);strcpy(json+60008,"}");
 uint8_t *old=NULL,*updated=NULL;size_t len=0,size=0;
 if(typed_encode_alloc(ts,json,0,NULL,NULL,&old,&len,NULL,0))return 3;
 SlotcaskOldRecord record={.value=old,.vlen=len,.hdr2=PKR_FLAGS};
 g_query_buffer_max_bytes=4096;active=1;ordinal=0;fail_at=0;peak=0;
 int rc=typed_update_computed_alloc(ts,&record,"{\"cash\":{\"$incr\":1}}",&updated,&size,NULL,0);
 r1_free(updated);active=0;
 printf("computed rc=%d peak=%zu budget=4096 live=%d OLD-size=%zu\n",rc,peak,live,len);
 free(old);free(json);free(ts);return rc==0||peak>4096||live?1:0;
}
````````

<!-- R1-BLOCK schedule-probe -->
````````c
#define _GNU_SOURCE
#include "typed_read_command.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
static _Atomic int active,jobs,get_calls,exists_calls;
void __real_parallel_for_io(void *(*)(void*),void*,int,size_t);
void __wrap_parallel_for_io(void *(*fn)(void*),void *args,int n,size_t size){
 if(atomic_load(&active)&&n>atomic_load(&jobs))atomic_store(&jobs,n);
 __real_parallel_for_io(fn,args,n,size);
}
int __real_slotcask_bulk_resolve_and_fetch_checked(SlotcaskDb*,const uint8_t (*)[16],size_t,void*,SlotcaskScanCb);
int __wrap_slotcask_bulk_resolve_and_fetch_checked(SlotcaskDb*d,const uint8_t(*h)[16],size_t n,void*c,SlotcaskScanCb cb){
 if(atomic_load(&active))atomic_fetch_add(&get_calls,1);
 return __real_slotcask_bulk_resolve_and_fetch_checked(d,h,n,c,cb);
}
int __real_slotcask_bulk_lookup_in_kfshard_checked(SlotcaskDb*,int,SlotcaskBulkRec*,size_t);
int __wrap_slotcask_bulk_lookup_in_kfshard_checked(SlotcaskDb*d,int k,SlotcaskBulkRec*r,size_t n){
 if(atomic_load(&active))atomic_fetch_add(&exists_calls,1);
 return __real_slotcask_bulk_lookup_in_kfshard_checked(d,k,r,n);
}
int main(void){
 shard_db_offline_init("/tmp/r1-schedule");g_query_buffer_max_bytes=256u*1024u*1024u;
 TypedSchema *ts=calloc(1,sizeof(*ts));ts->typed=1;ts->nfields=1;strcpy(ts->fields[0].name,"cash");ts->fields[0].name_len=4;ts->fields[0].type=FT_NUMERIC;
 const char *decl[]={"numeric:26,10"};if(typed_schema_bind_packed(ts,decl,1,NULL,0))return 2;
 uint8_t *value=NULL;size_t len=0,bound=0;
 if(typed_encode_alloc(ts,"{\"cash\":\"1234567890123456.1234567890\"}",0,NULL,NULL,&value,&len,NULL,0)||pkr_slot_bound(&ts->packed,32,&bound))return 2;
 char root[]="/tmp/r1-shards-XXXXXX";if(!mkdtemp(root))return 2;
 SlotcaskDb db;slotcask_init(64,64);if(slotcask_open(&db,root,2,1,(int)bound))return 2;
 char keys[2][32]={{0}};int present[2]={0};
 for(int i=0;i<1000&&(!present[0]||!present[1]);i++){
  char candidate[32];snprintf(candidate,sizeof(candidate),"k%d",i);uint8_t hash[16];compute_hash_raw(candidate,strlen(candidate),hash);
  int s=compute_record_shard(hash,2);if(!present[s]){strcpy(keys[s],candidate);present[s]=1;}
 }
 SlotcaskUpsertOpts options={.record_hdr2=PKR_FLAGS};
 for(int i=0;i<2;i++)if(slotcask_upsert_with_hooks(&db,-1,keys[i],strlen(keys[i]),value,len,&options,NULL))return 2;
 char list[256];snprintf(list,sizeof(list),"[\"%s\",\"%s\",\"%s\",\"missing\"]",keys[0],keys[1],keys[0]);
 Schema sc={.splits=2,.streams=1,.max_key=32,.slot_size=(int)bound,.max_value=ts->total_size};FieldSchema fs={.ts=ts};
 for(int mode=0;mode<3;mode++){
  char *text=NULL;size_t n=0;FILE *out=open_memstream(&text,&n);if(!out)return 2;
  TypedReadOptions o={.kind=mode==0?TYPED_READ_GET_MULTI:mode==1?TYPED_READ_EXISTS_MULTI:TYPED_READ_NOT_EXISTS,.keys=list,.budget=1048576};
  atomic_store(&jobs,0);atomic_store(&get_calls,0);atomic_store(&exists_calls,0);atomic_store(&active,1);
  int rc=typed_read_command(&db,&fs,&sc,&o,out);atomic_store(&active,0);if(fclose(out))return 2;
  int count=mode?atomic_load(&exists_calls):atomic_load(&get_calls);
  printf("multi route %d: rc=%d shard jobs=%d bulk calls=%d\n",mode,rc,atomic_load(&jobs),count);
  if(rc||atomic_load(&jobs)!=2||count!=2){fprintf(stderr,"response=%s\n",text?text:"NULL");return 1;}
  if(!mode&&(!text||!strstr(text,"1234567890123456.1234567890")))return 1;
  if(mode==2&&strcmp(text,"[\"missing\"]\n"))return 1;
  free(text);
 }

 uint32_t saved=ts->packed.fields[0].offset;ts->packed.fields[0].offset=UINT32_MAX;
 char *badtext=NULL;size_t badlen=0;FILE *badout=open_memstream(&badtext,&badlen);if(!badout)return 2;
 TypedReadOptions bad={.kind=TYPED_READ_GET_MULTI,.keys=list,.budget=1048576};
 int badrc=typed_read_command(&db,&fs,&sc,&bad,badout);if(fclose(badout))return 2;
 ts->packed.fields[0].offset=saved;
 if(badrc!=-1||!badtext||strncmp(badtext,"{\"error\":",9))return 1;
 free(badtext);puts("invalid packed metadata rejected before batch sizing reads");

 ts->fields[0].removed=1;
 char *emptycols=NULL;size_t colslen=0;FILE *colsout=open_memstream(&emptycols,&colslen);if(!colsout)return 2;
 TypedReadOptions cols={.kind=TYPED_READ_GET_MULTI,.format="csv",.delimiter=',',.keys="[\"missing\"]",.budget=65536};
 int colsrc=typed_read_command(&db,&fs,&sc,&cols,colsout);if(fclose(colsout))return 2;
 if(colsrc||!emptycols||strcmp(emptycols,"key\nmissing\n"))return 1;
 free(emptycols);puts("zero-value-column CSV preserves key-only missing row");
 slotcask_close(&db);slotcask_shutdown();free(value);free(ts);puts("multi-key scheduling preserved: all 3 modes use two shard jobs and batched engine APIs");return 0;
}
````````

<!-- R1-BLOCK collect-probe -->
````````c
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <sys/mman.h>
#include <unistd.h>
typedef struct {int unused;} ClientConn;
static int phase,eof_mode;static void *mapping;static size_t mapping_size,allocation_size;static char *allocation;
static int client_connect(int port,ClientConn *c){(void)port;(void)c;return 0;}
static int client_send_all(ClientConn*c,const char*s,size_t n){(void)c;(void)s;(void)n;return 0;}
static void client_close(ClientConn*c){(void)c;}
static ssize_t client_recv(ClientConn*c,char*out,size_t n){
 (void)c;if(!phase++){if(n<8192)return -1;memset(out,' ',8192);out[0]='[';out[1]=']';return 8192;}
 if(!eof_mode&&phase==2){out[0]=0;return 1;}return 0;
}
static void *guard_malloc(size_t n){
 size_t page=(size_t)sysconf(_SC_PAGESIZE),rounded=(n+page-1)/page*page;
 mapping_size=rounded+page;mapping=mmap(NULL,mapping_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
 if(mapping==MAP_FAILED)return NULL;mprotect((char*)mapping+rounded,page,PROT_NONE);
 allocation=(char*)mapping+rounded-n;allocation_size=n;memset(allocation,127,n);return allocation;
}
static void guard_free(void*p){(void)p;if(mapping&&mapping!=MAP_FAILED)munmap(mapping,mapping_size);mapping=NULL;}
static void *guard_realloc(void*p,size_t n){(void)p;(void)n;abort();}
#define malloc guard_malloc
#define realloc guard_realloc
#define free guard_free
#define LOG_WARN(...) ((void)0)
#define LOG_ERROR(...) ((void)0)
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    if(!out||!out_len)return -1;
    *out=NULL;*out_len=0;
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        LOG_WARN(LOG_SUB_SERVER, "query_collect: client_send_all failed for port=%d json_len=%zu", port, json_len);
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap+1);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SERVER, "query_collect: malloc(%zu) failed", cap);
        client_close(&cc); return -1;
    }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap+1);
                    if (!nb) {
                        LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while framing response (len=%zu)", cap, len);
                        free(buf); client_close(&cc); return -1;
                    }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                buf[len]=0;
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap+1);
            if (!nb) {
                LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while accumulating response (len=%zu)", cap, len);
                free(buf); client_close(&cc); return -1;
            }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    buf[len]=0;
    *out = buf; *out_len = len;
    return 0;
}
#undef malloc
#undef realloc
#undef free
static const char *json_skip(const char*s){while(*s==' '||*s=='\t'||*s=='\r'||*s=='\n')s++;return s;}
static const char *json_raw_string_end(const char*s){while(*s&&*s!='"')s++;return s;}
static int json_unescape_cstring(const char*s,size_t n,char**o,size_t*l){*o=strndup(s,n);*l=n;return !*o;}
static int walk_string_array(const char *json,
    void (*callback)(const char*,size_t,void*),void *ctx){
    const char *p=json_skip(json);if(*p++!='[')return -1;p=json_skip(p);
    if(*p==']')return *json_skip(p+1)?-1:0;
    while(*p){
        if(*p!='"')return -1;
        const char *start=p+1,*end=json_raw_string_end(start);
        if(*end!='"')return -1;
        char *decoded=NULL;size_t length=0;
        if(json_unescape_cstring(start,(size_t)(end-start),&decoded,&length))return -1;
        callback(decoded,length,ctx);free(decoded);p=json_skip(end+1);
        if(*p==']')return *json_skip(p+1)?-1:0;
        if(*p++!=',')return -1;
        p=json_skip(p);
    }
    return -1;
}
static void visit(const char*s,size_t n,void*p){(void)s;(void)n;(void)p;}
int main(void){for(eof_mode=0;eof_mode<2;eof_mode++){
 phase=0;char *out=NULL;size_t n=0;
 if(query_collect(0,"{}",2,&out,&n)||n!=8192||out[n]!=0||walk_string_array(out,visit,NULL))return 1;
 guard_free(out);
}puts("query_collect framing/EOF: exact-capacity payload has bounded terminator and complete JSON parse");return 0;}
````````

<!-- R1-BLOCK collect-red-probe -->
````````c
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <sys/mman.h>
#include <unistd.h>
typedef struct {int unused;} ClientConn;
static int phase,eof_mode;static void *mapping;static size_t mapping_size,allocation_size;static char *allocation;
static int client_connect(int port,ClientConn *c){(void)port;(void)c;return 0;}
static int client_send_all(ClientConn*c,const char*s,size_t n){(void)c;(void)s;(void)n;return 0;}
static void client_close(ClientConn*c){(void)c;}
static ssize_t client_recv(ClientConn*c,char*out,size_t n){
 (void)c;if(!phase++){if(n<8192)return -1;memset(out,' ',8192);out[0]='[';out[1]=']';return 8192;}
 if(!eof_mode&&phase==2){out[0]=0;return 1;}return 0;
}
static void *guard_malloc(size_t n){
 size_t page=(size_t)sysconf(_SC_PAGESIZE),rounded=(n+page-1)/page*page;
 mapping_size=rounded+page;mapping=mmap(NULL,mapping_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
 if(mapping==MAP_FAILED)return NULL;mprotect((char*)mapping+rounded,page,PROT_NONE);
 allocation=(char*)mapping+rounded-n;allocation_size=n;memset(allocation,127,n);return allocation;
}
static void guard_free(void*p){(void)p;if(mapping&&mapping!=MAP_FAILED)munmap(mapping,mapping_size);mapping=NULL;}
static void *guard_realloc(void*p,size_t n){(void)p;(void)n;abort();}
#define malloc guard_malloc
#define realloc guard_realloc
#define free guard_free
#define LOG_WARN(...) ((void)0)
#define LOG_ERROR(...) ((void)0)
static int query_collect(int port, const char *json, size_t json_len, char **out, size_t *out_len) {
    ClientConn cc;
    if (client_connect(port, &cc) != 0) return -1;

    if (client_send_all(&cc, json, json_len) != 0 ||
        client_send_all(&cc, "\n", 1) != 0) {
        LOG_WARN(LOG_SUB_SERVER, "query_collect: client_send_all failed for port=%d json_len=%zu", port, json_len);
        client_close(&cc); return -1;
    }

    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SERVER, "query_collect: malloc(%zu) failed", cap);
        client_close(&cc); return -1;
    }

    char rbuf[8192];
    ssize_t n;
    while ((n = client_recv(&cc, rbuf, sizeof(rbuf))) > 0) {
        for (ssize_t j = 0; j < n; j++) {
            if (rbuf[j] == '\0') {
                if (len + j > cap) {
                    while (cap < len + j) cap *= 2;
                    char *nb = realloc(buf, cap);
                    if (!nb) {
                        LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while framing response (len=%zu)", cap, len);
                        free(buf); client_close(&cc); return -1;
                    }
                    buf = nb;
                }
                memcpy(buf + len, rbuf, j);
                len += j;
                client_close(&cc);
                *out = buf; *out_len = len;
                return 0;
            }
        }
        if (len + (size_t)n > cap) {
            while (cap < len + (size_t)n) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) {
                LOG_ERROR(LOG_SUB_SERVER, "query_collect: realloc(%zu) failed while accumulating response (len=%zu)", cap, len);
                free(buf); client_close(&cc); return -1;
            }
            buf = nb;
        }
        memcpy(buf + len, rbuf, n);
        len += (size_t)n;
    }
    client_close(&cc);
    /* EOF with no \0 sentinel — still return what we got. */
    *out = buf; *out_len = len;
    return 0;
}
#undef malloc
#undef realloc
#undef free
static const char *json_skip(const char*s){while(*s==' '||*s=='\t'||*s=='\r'||*s=='\n')s++;return s;}
static const char *json_raw_string_end(const char*s){while(*s&&*s!='"')s++;return s;}
static int json_unescape_cstring(const char*s,size_t n,char**o,size_t*l){*o=strndup(s,n);*l=n;return !*o;}
static int walk_string_array(const char *json,
    void (*callback)(const char*,size_t,void*),void *ctx){
    const char *p=json_skip(json);if(*p++!='[')return -1;p=json_skip(p);
    if(*p==']')return *json_skip(p+1)?-1:0;
    while(*p){
        if(*p!='"')return -1;
        const char *start=p+1,*end=json_raw_string_end(start);
        if(*end!='"')return -1;
        char *decoded=NULL;size_t length=0;
        if(json_unescape_cstring(start,(size_t)(end-start),&decoded,&length))return -1;
        callback(decoded,length,ctx);free(decoded);p=json_skip(end+1);
        if(*p==']')return *json_skip(p+1)?-1:0;
        if(*p++!=',')return -1;
        p=json_skip(p);
    }
    return -1;
}
static void visit(const char*s,size_t n,void*p){(void)s;(void)n;(void)p;}
int main(void){for(eof_mode=0;eof_mode<2;eof_mode++){
 phase=0;char *out=NULL;size_t n=0;
 if(query_collect(0,"{}",2,&out,&n)||n!=8192||out[n]!=0||walk_string_array(out,visit,NULL))return 1;
 guard_free(out);
}puts("query_collect framing/EOF: exact-capacity payload has bounded terminator and complete JSON parse");return 0;}
````````

<!-- R1-BLOCK review-baseline-row -->
````````c
#include "typed_row.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int row_error(char *err, size_t cap, int code, const char *message) {
    if (err && cap) snprintf(err, cap, "%s", message);
    errno = code; return -1;
}
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
int typed_schema_bind_packed(TypedSchema *ts,const char *const *types,
                              size_t n,char *err,size_t errsz) {
    if (!ts||!types||!ts->typed||n<1||n>MAX_FIELDS||n!=(size_t)ts->nfields)
        return row_error(err,errsz,EINVAL,"invalid packed schema");
    PkrField fields[MAX_FIELDS];enum FieldType kinds[MAX_FIELDS];
    for(size_t i=0;i<n;i++){
        const TypedField *field=&ts->fields[i];
        if(field->name_len<1||field->name_len>=(int)sizeof(field->name)||
           strnlen(field->name,sizeof(field->name))!=(size_t)field->name_len)
            return row_error(err,errsz,EINVAL,"invalid field name");
        if(!types[i])return row_error(err,errsz,EINVAL,"missing type token");
        if(!strncmp(types[i],"varchar",7)||!strncmp(types[i],"numeric",7)||
           !strcmp(types[i],"currency")){
            if(pkr_field_parse(types[i],&fields[i])!=DEC_OK)
                return row_error(err,errsz,EINVAL,"invalid packed declaration");
            kinds[i]=fields[i].kind==PKR_VARCHAR?FT_VARCHAR:FT_NUMERIC;
            if(ts->fields[i].type!=FT_NONE&&ts->fields[i].type!=kinds[i])
                return row_error(err,errsz,EINVAL,"packed logical type mismatch");
        }else{
            if(!strncmp(types[i],"enum(",5)){
                const char *close=strrchr(types[i],')');
                if(!close||close[1])
                    return row_error(err,errsz,EINVAL,"enum token contains trailing declaration text");
            }
            TypedField check={0};errno=0;parse_field_type(types[i],&check);
            int parse_errno=errno;
            int good=check.type!=FT_NONE&&check.type==ts->fields[i].type&&
                check.size==ts->fields[i].size&&check.size>=1&&check.size<=16;
            if(check.type==FT_ENUM){
                good=good&&check.n_enum_values==ts->fields[i].n_enum_values;
                for(int k=0;good&&k<check.n_enum_values;k++)
                    good=!strcmp(check.enum_values[k],ts->fields[i].enum_values[k]);
            }
            free_enum_values(&check);
            if(!good||pkr_field_fixed((uint32_t)ts->fields[i].size,&fields[i])!=DEC_OK)
                return row_error(err,errsz,parse_errno==ENOMEM?ENOMEM:EINVAL,"invalid fixed declaration");
            kinds[i]=ts->fields[i].type;
        }
    }
    PkrSchema next;
    if(pkr_schema_build(fields,n,&next)!=DEC_OK)
        return row_error(err,errsz,ERANGE,"packed schema exceeds bounds");
    for(size_t i=0;i<n;i++){
        ts->fields[i].type=kinds[i];ts->fields[i].offset=(int)next.fields[i].offset;
        ts->fields[i].size=(int)next.fields[i].width;
        if(kinds[i]==FT_NUMERIC){
            ts->fields[i].numeric_scale=next.fields[i].scale;
            ts->fields[i].numeric_scale_mult=0;
        }
    }
    ts->packed=next;ts->nullmap_prefix=(int)next.prefix;
    ts->total_size=(int)next.maximum;return 0;
}
uint8_t typed_schema_hdr2(const TypedSchema *ts) {
    return ts&&ts->packed.nfields?PKR_FLAGS:SREC_FLAG_NULLMAP;
}
int typed_row_open(const TypedSchema *ts,const uint8_t *value,size_t len,
                    uint8_t hdr2,TypedRowView *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!ts||!ts->typed||ts->nfields<1||ts->nfields>MAX_FIELDS||!value){
        errno=EINVAL;return -1;}
    TypedRowView v={0};v.schema=ts;v.value=value;v.len=len;v.hdr2=hdr2;
    if(ts->packed.nfields){
        if(!packed_schema_ok(ts)){errno=EILSEQ;return -1;}
        dec_status st=pkr_view(&ts->packed,value,len,hdr2,&v.packed);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        v.base=value+ts->packed.prefix;v.region=len-ts->packed.prefix;
        v.bitmap=value+1;v.bitmap_bytes=(int)ts->packed.prefix-1;
    }else{
        if(typed_record_view(value,len,hdr2,&v.base,&v.bitmap,
                              &v.bitmap_bytes,&v.region)!=0){
            errno=EILSEQ;return -1;}
    }
    *out=v;return 0;
}
int typed_row_span(const TypedRowView *v,size_t index,TypedRowSpan *out) {
    if(!out){errno=EINVAL;return -1;}memset(out,0,sizeof(*out));
    if(!v||!v->schema||index>=(size_t)v->schema->nfields){errno=EINVAL;return -1;}
    if(v->schema->packed.nfields){
        PkrSpan p;if(pkr_span(&v->packed,index,&p)!=DEC_OK){errno=EILSEQ;return -1;}
        out->data=p.data;out->len=p.len;out->is_null=p.state==PKR_NULL;
        out->complete=1;return 0;
    }
    const TypedField *f=&v->schema->fields[index];
    if(f->offset<0||f->size<1||f->size>65537){errno=EILSEQ;return -1;}
    out->is_null=typed_field_null(v->bitmap,v->bitmap_bytes,(int)index);
    size_t off=(size_t)f->offset,cap=(size_t)f->size;
    if(off<v->region){
        out->len=v->region-off;if(out->len>cap)out->len=cap;
        out->data=v->base+off;
    }
    out->complete=out->len==cap;return 0;
}
int typed_span_read(const TypedSchema *ts,size_t index,const TypedRowSpan *s,
                      const uint8_t **out) {
    static const uint8_t zero[65537];
    if(!out){errno=EINVAL;return -1;}*out=NULL;
    if(!ts||!s||index>=(size_t)ts->nfields||s->is_null){errno=EINVAL;return -1;}
    if(ts->packed.nfields){if(!s->complete){errno=EILSEQ;return -1;}*out=s->data;}
    else *out=s->complete?s->data:zero;
    return 0;
}
int typed_span_copy_legacy(const TypedSchema *ts,size_t index,
                             const TypedRowSpan *s,uint8_t *out,size_t cap){
    if(!ts||ts->packed.nfields||index>=(size_t)ts->nfields||!s||!out||
       ts->fields[index].size<1||cap<(size_t)ts->fields[index].size){
        errno=EINVAL;return -1;}
    size_t width=(size_t)ts->fields[index].size;
    if(s->len>width||(!s->data&&s->len)){errno=EILSEQ;return -1;}
    memset(out,0,width);if(s->len)memcpy(out,s->data,s->len);return 0;
}
void typed_prepared_init(TypedPrepared *p){if(p)memset(p,0,sizeof(*p));}
void typed_prepared_free(TypedPrepared *p){
    if(!p)return;
    for(size_t i=0;i<MAX_FIELDS;i++)free(p->owned[i]);
    memset(p,0,sizeof(*p));
}
static int prepared_check(const TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(p->schema))
        return row_error(err,cap,EINVAL,"invalid prepared schema");
    return 0;
}
/* Legacy scalar helpers report status/text but do not promise errno for
   validation failures. Establish the new adapter boundary's error contract. */
static int row_encode_fixed(const TypedField *field,const char *text,size_t len,
                              int quoted,uint8_t *out,char *err,size_t cap){
    errno=0;
    int rc=encode_json_field_checked(field,text,len,quoted,out,err,cap);
    if(rc)errno=errno==ENOMEM?ENOMEM:EINVAL;
    return rc;
}
static int replace_cell(TypedPrepared *p,size_t i,uint8_t *owner,
                          const uint8_t *data,size_t n,int is_null,
                          char *err,size_t cap){
    const PkrField *f=&p->schema->packed.fields[i];
    size_t prior=p->cells[i].is_null||f->kind==PKR_FIXED?0:p->cells[i].len;
    size_t added=is_null||f->kind==PKR_FIXED?0:n;
    if(p->bytes<prior||added>SLOTCASK_SEG_MAX_BYTES-24||
       p->bytes-prior>SLOTCASK_SEG_MAX_BYTES-24-added){
        free(owner);return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");}
    free(p->owned[i]);p->owned[i]=owner;p->cells[i]=(PkrCell){data,n,(uint8_t)is_null};
    p->bytes=p->bytes-prior+added;return 0;
}
int typed_prepared_set(TypedPrepared *p,size_t i,const TypedInput *in,
                        char *err,size_t cap){
    if(prepared_check(p,err,cap)||!in||i>=(size_t)p->schema->nfields)
        return row_error(err,cap,EINVAL,"invalid field input");
    if(!in->supplied)return 0;
    const TypedField *f=&p->schema->fields[i];const PkrField *pf=&p->schema->packed.fields[i];
    if(f->removed)return 0;
    if(in->is_null){
        if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;return -1;}
        int rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);
        if(!rc)p->seen[i]=1;
        return rc;
    }
    if(!in->data&&in->len)return row_error(err,cap,EINVAL,"invalid value span");
    const char *text=in->data?in->data:"";size_t n=in->len;char *decoded=NULL;
    int string_type=f->type==FT_VARCHAR||f->type==FT_NUMERIC||
        f->type==FT_ENUM||f->type==FT_DATE||f->type==FT_DATETIME||
        f->type==FT_DATETIMEMS||f->type==FT_TIME||f->type==FT_UUID||
        f->type==FT_IPV4||f->type==FT_IPV6;
    int was_quoted=in->quoted;
    if(was_quoted&&string_type){
        errno=0;
        int rc=f->type==FT_VARCHAR?json_unescape_string(text,n,&decoded,&n):
            json_unescape_cstring(text,n,&decoded,&n);
        if(rc)return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"malformed JSON string");
        text=decoded;was_quoted=0;
    }
    uint8_t *bytes=NULL;size_t used=0;int rc=-1;
    if(pf->kind==PKR_VARCHAR){
        if(n>pf->payload_max){row_error(err,cap,ERANGE,"varchar content exceeds declaration");goto done;}
        bytes=malloc(n?n:1);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(n)memcpy(bytes,text,n);
        used=n;
    }else if(pf->kind==PKR_NUMERIC){
        dec_t a,b;dec_init(&a);dec_init(&b);
        dec_status st=dec_parse(text,n,&a,err,cap);const dec_t *value=&a;
        if(st==DEC_OK&&!pf->bare){
            st=dec_apply_precision(&a,pf->precision,pf->scale,&b,err,cap);value=&b;}
        if(st==DEC_OK){
            used=dec_payload_len(value);
            if(used>pf->payload_max)st=DEC_ERR_RANGE;
            else{bytes=malloc(used);if(!bytes)st=DEC_ERR_NOMEM;
                else st=dec_pack(value,bytes,used,&used);}
        }
        dec_free(&b);dec_free(&a);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EINVAL;goto done;}
    }else{
        if(f->type==FT_ENUM&&n&&enum_value_index(f,text,n)<0){
            if(err&&cap)snprintf(err,cap,"unknown enum value for field '%s'",f->name);
            errno=EINVAL;goto done;
        }
        if(!sentinel_span_parses(f->type,text,n)){
            if(f->not_null){fmt_not_null_err(f,err,cap);errno=EINVAL;goto done;}
            rc=replace_cell(p,i,NULL,NULL,0,1,err,cap);goto done;
        }
        bytes=calloc(1,pf->width);if(!bytes){row_error(err,cap,ENOMEM,"oom");goto done;}
        if(row_encode_fixed(f,text,n,was_quoted,bytes,err,cap)!=0)goto done;
        used=pf->width;
    }
    rc=replace_cell(p,i,bytes,bytes,used,0,err,cap);bytes=NULL;
done:if(!rc)p->seen[i]=1;
    int saved_errno=errno;
    free(bytes);free(decoded);
    if(rc)errno=saved_errno;
    return rc;
}
int typed_prepared_zero(const TypedSchema *ts,TypedPrepared *p,char *err,size_t cap){
    if(!p||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++)p->cells[i]=(PkrCell){NULL,0,1};
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        const PkrField *pf=&ts->packed.fields[i];
        if(pf->kind==PKR_FIXED){
            uint8_t *z=calloc(1,pf->width);if(!z){typed_prepared_free(p);return row_error(err,cap,ENOMEM,"oom");}
            if(replace_cell(p,(size_t)i,z,z,pf->width,0,err,cap))goto fail;
        }else{
            TypedInput in={pf->kind==PKR_NUMERIC?"0":"",pf->kind==PKR_NUMERIC?1:0,1,0,0};
            if(typed_prepared_set(p,(size_t)i,&in,err,cap))goto fail;
        }
    }
    memset(p->seen,0,sizeof(p->seen));return 0;
fail:typed_prepared_free(p);return -1;
}
int typed_prepared_old(const TypedSchema *ts,const SlotcaskOldRecord *old,
                        TypedPrepared *p,char *err,size_t cap){
    if(!p||!old||!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid OLD");
    TypedRowView v;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&v))
        return row_error(err,cap,errno,"invalid OLD record");
    typed_prepared_free(p);p->schema=ts;p->bytes=ts->packed.prefix+ts->packed.directory;
    for(int i=0;i<ts->nfields;i++){
        TypedRowSpan sp;if(typed_row_span(&v,(size_t)i,&sp))goto fail;
        int null=sp.is_null||ts->fields[i].removed;
        p->cells[i]=(PkrCell){null?NULL:sp.data,null?0:sp.len,(uint8_t)null};
        if(!null&&ts->packed.fields[i].kind!=PKR_FIXED)p->bytes+=sp.len;
    }
    if(p->bytes>SLOTCASK_SEG_MAX_BYTES-24){
        typed_prepared_free(p);
        return row_error(err,cap,EOVERFLOW,"record exceeds segment capacity");
    }
    return 0;
fail:typed_prepared_free(p);return row_error(err,cap,EILSEQ,"invalid OLD field");
}
static int prepared_defaults_mode(TypedPrepared *p,const char *root,const char *object,
    int defer_ac,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||p->seen[i]||(defer_ac&&f->default_kind==DK_AUTO_CREATE))continue;
        TypedInput in={NULL,0,1,1,0};char *owned=NULL;
        if(f->default_kind!=DK_NONE){
            owned=generate_default_owned(f,root,object,g_query_buffer_max_bytes);
            if(!owned)return row_error(err,cap,errno,"default generator failed");
            in.data=owned;in.len=strlen(owned);in.is_null=0;
        }
        int rc=typed_prepared_set(p,(size_t)i,&in,err,cap);int saved=errno;
        free(owned);if(rc){errno=saved;return -1;}
    }
    return 0;
}
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    return prepared_defaults_mode(p,root,object,0,err,cap);
}
int typed_prepared_stamp(TypedPrepared *p,enum DefaultKind kind,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    if(kind!=DK_AUTO_CREATE&&kind!=DK_AUTO_UPDATE)
        return row_error(err,cap,EINVAL,"invalid timestamp kind");
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];
        if(f->removed||f->default_kind!=kind)continue;
        char b[256];const char *v=generate_default(f,b,sizeof(b),NULL,NULL);
        if(!v)return row_error(err,cap,EIO,"automatic timestamp failed");
        TypedInput in={v,strlen(v),1,0,0};
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
static int prepared_valid(const TypedPrepared *p,char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        if(!p->schema->fields[i].removed&&p->schema->fields[i].not_null&&p->cells[i].is_null){
            fmt_not_null_err(&p->schema->fields[i],err,cap);errno=EINVAL;return -1;}
    }return 0;
}
int typed_prepared_compose(const TypedPrepared *p,uint8_t *out,size_t cap,
                            size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    if(prepared_valid(p,err,errsz))return -1;
    dec_status st=pkr_compose(&p->schema->packed,p->cells,(size_t)p->schema->nfields,out,cap,used);
    if(st!=DEC_OK)return row_error(err,errsz,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record composition failed");
    return 0;
}
int typed_prepared_alloc(const TypedPrepared *p,uint8_t **out,size_t *len,
                          char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(prepared_valid(p,err,cap))return -1;
    size_t need=0;dec_status st=pkr_measure(&p->schema->packed,p->cells,(size_t)p->schema->nfields,&need);
    if(st!=DEC_OK)return row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"record sizing failed");
    if(need>g_query_buffer_max_bytes/2)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    uint8_t *buf=malloc(need);if(!buf)return row_error(err,cap,ENOMEM,"oom");
    size_t used=0;if(typed_prepared_compose(p,buf,need,&used,err,cap)){free(buf);return -1;}
    *out=buf;*len=used;return 0;
}

/* Validate syntax before span collection. json_skip_value locates spans but
   deliberately is not a validator. Depth is bounded without global state. */
static const char *row_json_space(const char *p){
    while(*p==' '||*p=='\t'||*p=='\r'||*p=='\n')p++;
    return p;
}
static int row_json_string(const char **cursor){
    const unsigned char *p=(const unsigned char*)*cursor;
    if(*p++!='"')return -1;
    while(*p&&*p!='"'){
        if(*p<0x20)return -1;
        if(*p++!='\\')continue;
        unsigned char c=*p++;
        if(!c)return -1;
        if(c=='u'){
            for(int i=0;i<4;i++){
                unsigned char h=*p++;
                if(!((h>='0'&&h<='9')||(h>='a'&&h<='f')||(h>='A'&&h<='F')))return -1;
            }
        }else if(c!='"'&&c!='\\'&&c!='/'&&c!='b'&&c!='f'&&c!='n'&&c!='r'&&c!='t')return -1;
    }
    if(*p!='"')return -1;
    *cursor=(const char*)p+1;return 0;
}
static int row_json_value(const char **cursor,unsigned depth){
    const char *p=row_json_space(*cursor);
    if(*p=='"'){
        if(row_json_string(&p))return -1;
    }else if(*p=='{'||*p=='['){
        if(depth>=64)return -1;
        int object=*p=='{';char end=object?'}':']';p=row_json_space(p+1);
        if(*p!=end)for(;;){
            if(object){
                if(row_json_string(&p))return -1;
                p=row_json_space(p);if(*p++!=':')return -1;
            }
            if(row_json_value(&p,depth+1))return -1;
            p=row_json_space(p);if(*p==end)break;
            if(*p++!=',')return -1;
            p=row_json_space(p);
        }
        p++;
    }else if(!strncmp(p,"null",4))p+=4;
    else if(!strncmp(p,"true",4))p+=4;
    else if(!strncmp(p,"false",5))p+=5;
    else{
        if(*p=='-')p++;
        if(*p=='0')p++;
        else{
            if(*p<'1'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='.'){
            p++;if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
        if(*p=='e'||*p=='E'){
            p++;if(*p=='+'||*p=='-')p++;
            if(*p<'0'||*p>'9')return -1;
            do{p++;}while(*p>='0'&&*p<='9');
        }
    }
    *cursor=p;return 0;
}
#include <math.h>
/* Computed operands are borrowed JSON spans; results own their payloads. */
typedef struct {int op,source;const char *literal;size_t len;int quoted;} RowCompute;
static int row_compute_parse(const TypedSchema *ts,size_t index,const char *json,
    size_t n,RowCompute *c,char *err,size_t cap){
    memset(c,0,sizeof(*c));
    const TypedField *f=&ts->fields[index];
    if(f->type!=FT_SHORT&&f->type!=FT_INT&&f->type!=FT_LONG&&
       f->type!=FT_NUMERIC&&f->type!=FT_FLOAT&&f->type!=FT_DOUBLE)
        return row_error(err,cap,EINVAL,"unsupported computed target");
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    p=row_json_space(p);if(*p!='"')goto bad;
    const char *key=p+1,*end=p;if(row_json_string(&end))goto bad;
    char *name=NULL;size_t length=0;
    if(json_unescape_cstring(key,(size_t)(end-key-1),&name,&length))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid computed operator");
    c->op=length==5&&!memcmp(name,"$incr",5)?1:
          length==5&&!memcmp(name,"$copy",5)?2:0;
    free(name);if(!c->op)goto bad;
    p=row_json_space(end);if(*p++!=':')goto bad;p=row_json_space(p);
    const char *v=p;if(row_json_value(&p,0))goto bad;
    size_t vn=(size_t)(p-v);p=row_json_space(p);
    if(*p++!='}'||p!=json+n)goto bad;
    c->quoted=vn>=2&&v[0]=='"'&&v[vn-1]=='"';
    if(c->op==1){
        if(!vn||(!c->quoted&&(v[0]=='{'||v[0]=='['||v[0]=='n'||v[0]=='t'||v[0]=='f')))goto bad;
        c->literal=c->quoted?v+1:v;c->len=c->quoted?vn-2:vn;return 0;
    }
    if(!c->quoted)goto bad;
    char *source=NULL;size_t sn=0;
    if(json_unescape_cstring(v+1,vn-2,&source,&sn))
        return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid copy source");
    c->source=-1;
    for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed&&
       sn==(size_t)ts->fields[i].name_len&&!memcmp(source,ts->fields[i].name,sn)){c->source=i;break;}
    free(source);if(c->source<0)goto bad;
    const TypedField *sf=&ts->fields[c->source];
    const PkrField *a=&ts->packed.fields[index],*b=&ts->packed.fields[c->source];
    if(sf->type!=f->type||a->kind!=b->kind||a->width!=b->width||
       (a->kind==PKR_NUMERIC&&(a->bare!=b->bare||a->precision!=b->precision||a->scale!=b->scale)))goto bad;
    return 0;
bad:return row_error(err,cap,EINVAL,"malformed or incompatible computed value");
}
static int row_compute_apply(TypedPrepared *p,const SlotcaskOldRecord *old,
    const RowCompute *computes,char *err,size_t cap){
    const TypedSchema *ts=p->schema;TypedRowView view;
    if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))return -1;
    for(int i=0;i<ts->nfields;i++){
        const RowCompute *c=&computes[i];if(!c->op)continue;
        const TypedField *f=&ts->fields[i];const PkrField *pf=&ts->packed.fields[i];
        TypedRowSpan sp;
        if(typed_row_span(&view,(size_t)i,&sp))return -1;
        if(sp.is_null)return row_error(err,cap,EINVAL,"NULL computed target");
        if(c->op==2){
            if(computes[c->source].op)return row_error(err,cap,EINVAL,"copy source is computed");
            if(typed_row_span(&view,(size_t)c->source,&sp))return -1;
            if(sp.is_null)return row_error(err,cap,EINVAL,"NULL copy source");
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            if(f->type==FT_DOUBLE){double v;memcpy(&v,sp.data,8);if(!isfinite(v))return row_error(err,cap,EINVAL,"nonfinite copy source");}
            uint8_t *bytes=malloc(sp.len?sp.len:1);
            if(!bytes)return row_error(err,cap,ENOMEM,"oom");
            if(sp.len)memcpy(bytes,sp.data,sp.len);
            if(replace_cell(p,(size_t)i,bytes,bytes,sp.len,0,err,cap))return -1;
            p->seen[i]=1;continue;
        }
        char *decoded=NULL;const char *literal=c->literal;size_t length=c->len;
        if(c->quoted){
            if(json_unescape_cstring(literal,length,&decoded,&length))
                return row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid increment");
            literal=decoded;
        }
        int rc=-1;char text[128];
        if(f->type==FT_FLOAT||f->type==FT_DOUBLE){
            char *operand=malloc(length+1);
            if(!operand){free(decoded);return row_error(err,cap,ENOMEM,"oom");}
            memcpy(operand,literal,length);operand[length]=0;
            char *end=NULL;errno=0;double delta=strtod(operand,&end);
            int valid=end==operand+length&&end!=operand&&errno!=ERANGE&&isfinite(delta);
            double current;
            if(f->type==FT_FLOAT){float v;memcpy(&v,sp.data,4);current=v;}
            else memcpy(&current,sp.data,8);
            free(operand);
            double result=current+delta;
            if(valid&&isfinite(current)&&isfinite(result)){
                int written;
                if(f->type==FT_FLOAT){float v=(float)result;written=isfinite(v)?snprintf(text,sizeof(text),"%.9g",(double)v):-1;}
                else written=snprintf(text,sizeof(text),"%.17g",result);
                if(written>0&&(size_t)written<sizeof(text)){
                    TypedInput in={text,(size_t)written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);
                }
            }
            if(rc)row_error(err,cap,EINVAL,"invalid or overflowing floating increment");
        }else{
            dec_t current,delta,sum;dec_init(&current);dec_init(&delta);dec_init(&sum);
            dec_status st;
            if(pf->kind==PKR_NUMERIC)st=dec_unpack(sp.data,sp.len,&current);
            else{
                uint64_t bits=0;for(size_t j=0;j<sp.len;j++)bits=(bits<<8)|sp.data[j];
                if(sp.len<8&&(sp.data[0]&128))bits|=UINT64_MAX<<(sp.len*8);
                int64_t value;memcpy(&value,&bits,8);
                int written=snprintf(text,sizeof(text),"%lld",(long long)value);
                st=dec_parse(text,(size_t)written,&current,err,cap);
            }
            if(st==DEC_OK&&pf->kind!=PKR_NUMERIC){
                char *integer=malloc(length+1);
                if(!integer)st=DEC_ERR_NOMEM;
                else{
                    memcpy(integer,literal,length);integer[length]=0;char *end=NULL;errno=0;
                    (void)strtoll(integer,&end,10);
                    int valid=end!=integer&&errno!=ERANGE;
                    while(*end==' '||*end=='\t'||*end=='\r'||*end=='\n'||*end=='\f'||*end=='\v')end++;
                    if(*end||!valid)st=DEC_ERR_SYNTAX;
                    free(integer);
                }
            }
            if(st==DEC_OK)st=dec_parse(literal,length,&delta,err,cap);
            if(st==DEC_OK)st=dec_add(&current,&delta,&sum);
            if(st==DEC_OK){
                size_t n=dec_text_len(&sum);char *exact=n?malloc(n):NULL;
                if(!exact)st=DEC_ERR_NOMEM;
                else{
                    size_t written=0;st=dec_to_text(&sum,exact,n,&written);
                    if(st==DEC_OK){TypedInput in={exact,written,1,0,0};rc=typed_prepared_set(p,(size_t)i,&in,err,cap);}
                    free(exact);
                }
            }
            dec_free(&sum);dec_free(&delta);dec_free(&current);
            if(st!=DEC_OK)rc=row_error(err,cap,st==DEC_ERR_NOMEM?ENOMEM:EINVAL,"invalid or overflowing exact increment");
        }
        free(decoded);if(rc)return -1;
    }
    return 0;
}

static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,const SlotcaskOldRecord *old,int computed,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    size_t request_length=strnlen(json,g_query_buffer_max_bytes);
    if(request_length>=g_query_buffer_max_bytes/8)return row_error(err,cap,ENOMEM,"query memory buffer exceeded");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(old?typed_prepared_old(ts,old,out,err,cap):typed_prepared_zero(ts,out,err,cap))return -1;
    RowCompute computes[MAX_FIELDS]={0};
    const char *p=row_json_space(json);if(*p++!='{')goto bad;
    int first=1;
    while(1){
        p=row_json_space(p);if(*p=='}'){p++;break;}
        if(!first){if(*p++!=',')goto bad;p=row_json_space(p);}
        if(*p!='"')goto bad;
        const char *key=p+1,*after=p;
        if(row_json_string(&after))goto bad;
        size_t kn=(size_t)(after-key-1);p=row_json_space(after);
        if(*p++!=':')goto bad;
        p=row_json_space(p);
        const char *v=p;
        if(row_json_value(&p,0))goto bad;
        size_t n=(size_t)(p-v);char *name=NULL;size_t name_len=0;
        /* Longer encoded keys cannot equal a <=255-byte schema name. */
        if(kn<=6*(sizeof(ts->fields[0].name)-1)){
            errno=0;
            if(json_unescape_string(key,kn,&name,&name_len)){
                row_error(err,cap,errno==ENOMEM?ENOMEM:EINVAL,"invalid field name");goto fail;
            }
        }
        for(int i=0;name&&i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];
            if(f->removed||name_len!=(size_t)f->name_len||memcmp(f->name,name,name_len))continue;
            int quoted=n>=2&&v[0]=='"'&&v[n-1]=='"';
            if(!quoted&&(v[0]=='{'||v[0]=='[')){
                if(!computed||!old||v[0]!='{'||row_compute_parse(ts,(size_t)i,v,n,&computes[i],err,cap)){
                    free(name);goto bad;}
                break;
            }
            computes[i]=(RowCompute){0};
            TypedInput in={quoted?v+1:v,quoted?n-2:n,1,
                (uint8_t)(!quoted&&json_span_is_null(v,n)),(uint8_t)quoted};
            if(typed_prepared_set(out,(size_t)i,&in,err,cap)){free(name);goto fail;}
            break;
        }
        free(name);first=0;
    }
    if(*row_json_space(p))goto bad;
    if(computed&&row_compute_apply(out,old,computes,err,cap))goto fail;
    if(defaults&&typed_prepared_defaults(out,root,object,err,cap))goto fail;
    return 0;
bad:row_error(err,cap,EINVAL,"invalid record JSON");
fail:typed_prepared_free(out);return -1;
}
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
int typed_encode_alloc(const TypedSchema *ts,const char *json,int defaults,
                         const char *root,const char *object,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!ts->typed||ts->total_size<1)
        return row_error(err,cap,EINVAL,"invalid typed schema");
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    if(!ts->packed.nfields){
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        errno=0;
        int rc=defaults?typed_encode_defaults(ts,json,b,ts->total_size,root,object,err,cap):
            typed_encode(ts,json,b,ts->total_size,err,cap);
        if(rc<0){int cause=errno==ENOMEM?ENOMEM:EINVAL;free(b);errno=cause;return rc;}
        *out=b;*len=(size_t)rc;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,0,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
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
    int rc=prepare_json(ts,json,0,root,object,&p,NULL,0,err,cap);
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
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,0,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_computed_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts)||!old)return row_error(err,cap,EINVAL,"invalid update");
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,0,NULL,NULL,&p,old,1,err,cap);
    if(!rc)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

int typed_update_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                         const TypedInput *inputs,int stamp,uint8_t **out,
                         size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;if(!ts||!old)return row_error(err,cap,EINVAL,"invalid update");
    if(!ts->packed.nfields){
        if(ts->total_size<1)return row_error(err,cap,EINVAL,"invalid schema");
        uint8_t *b=malloc((size_t)ts->total_size);if(!b)return row_error(err,cap,ENOMEM,"oom");
        if(update_compose_new_from_old(ts,old,b,NULL,NULL,err,cap)){
            free(b);return row_error(err,cap,EILSEQ,"invalid OLD record");
        }
        if(inputs)for(int i=0;i<ts->nfields;i++){
            const TypedField *f=&ts->fields[i];const TypedInput *in=&inputs[i];
            if(f->removed||!in->supplied)continue;
            uint8_t *dst=b+ts->nullmap_prefix+f->offset;
            if(in->is_null){
                if(f->not_null){fmt_not_null_err(f,err,cap);free(b);errno=EINVAL;return -1;}
                memset(dst,0,(size_t)f->size);typed_set_null(b+1,ts->nullmap_prefix-1,i);
            }else{
                if(in->len&&!in->data){free(b);return row_error(err,cap,EINVAL,"invalid value span");}
                if(row_encode_fixed(f,in->data?in->data:"",in->len,in->quoted,dst,err,cap)){
                    int cause=errno;free(b);errno=cause;return -1;
                }
                typed_clear_null(b+1,ts->nullmap_prefix-1,i);
            }
        }
        if(stamp)update_stamp_auto_update_fields(ts,b);
        *out=b;*len=(size_t)ts->total_size;return 0;
    }
    TypedPrepared p;typed_prepared_init(&p);
    int rc=typed_prepared_old(ts,old,&p,err,cap);
    if(!rc&&inputs)for(int i=0;i<ts->nfields;i++){
        if(typed_prepared_set(&p,(size_t)i,&inputs[i],err,cap)){rc=-1;break;}}
    if(!rc&&stamp)rc=typed_prepared_stamp(&p,DK_AUTO_UPDATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
int typed_transfer_ac_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
                             const uint8_t *value,size_t vlen,uint8_t **out,
                             size_t *len,char *err,size_t cap){
    if(!out||!len)return row_error(err,cap,EINVAL,"invalid output");
    *out=NULL;*len=0;
    if(!packed_schema_ok(ts))return row_error(err,cap,EINVAL,"invalid packed schema");
    SlotcaskOldRecord current={value,vlen,PKR_FLAGS};TypedPrepared p;typed_prepared_init(&p);
    if(typed_prepared_old(ts,&current,&p,err,cap))return -1;
    int rc=0;
    if(old&&old->value){
        TypedRowView view;if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view))rc=-1;
        for(int i=0;!rc&&i<ts->nfields;i++){
            if(ts->fields[i].removed||ts->fields[i].default_kind!=DK_AUTO_CREATE)continue;
            TypedRowSpan span;if(typed_row_span(&view,(size_t)i,&span)){rc=-1;break;}
            rc=replace_cell(&p,(size_t)i,NULL,span.data,span.len,span.is_null,err,cap);
        }
    }else rc=typed_prepared_stamp(&p,DK_AUTO_CREATE,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}

/* One bounded field description at a time; no varchar-sized scratch buffer.
   Packed-view validation and decimal workspace are separate from output budget. */
typedef struct {
    const char *data;
    size_t len;
    int quoted, numeric;
    char fixed[512];
    dec_t decimal;
} RowText;
static void row_text_free(RowText *text){dec_free(&text->decimal);}
static int row_text_open(const TypedRowView *view,size_t index,RowText *text){
    memset(text,0,sizeof(*text));dec_init(&text->decimal);
    TypedRowSpan span;if(typed_row_span(view,index,&span))return -1;
    const TypedField *f=&view->schema->fields[index];
    const PkrField *pf=&view->schema->packed.fields[index];
    if(span.is_null||f->removed){text->data="null";text->len=4;return 0;}
    if(pf->kind==PKR_VARCHAR){
        text->data=(const char*)span.data;text->len=span.len;text->quoted=1;return 0;
    }
    if(pf->kind==PKR_NUMERIC){
        dec_status st=dec_unpack(span.data,span.len,&text->decimal);
        if(st!=DEC_OK){errno=st==DEC_ERR_NOMEM?ENOMEM:EILSEQ;return -1;}
        size_t cap=dec_text_len(&text->decimal);
        if(!cap){errno=EILSEQ;return -1;}
        text->numeric=1;text->len=cap-1;return 0;
    }
    if(f->type==FT_ENUM){
        int i=f->enum_width==2?(int)(((uint16_t)span.data[0]<<8)|span.data[1]):span.data[0];
        /* Keep the legacy renderer's corrupt-enum empty-string policy. */
        text->data=i>=0&&i<f->n_enum_values&&f->enum_values&&f->enum_values[i]?
            f->enum_values[i]:"";
        text->len=strlen(text->data);text->quoted=1;return 0;
    }
    int n=decode_field_to_buf(f,span.data,text->fixed,sizeof(text->fixed),1);
    if(n<1||(size_t)n>=sizeof(text->fixed)){errno=EILSEQ;return -1;}
    text->data=text->fixed;text->len=(size_t)n;
    /* These fixed string renderings contain only their canonical ASCII. */
    if(n>=2&&text->fixed[0]=='"'&&text->fixed[n-1]=='"'){
        text->data++;text->len-=2;text->quoted=1;
    }
    return 0;
}
static int quoted_length(const char *data,size_t n,size_t *out){
    size_t len=2;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)data[i];
        size_t add=(c=='"'||c=='\\'||c=='\b'||c=='\f'||c=='\n'||c=='\r'||c=='\t')?2:
            c<0x20?6:1;
        if(len>SIZE_MAX-add){errno=EOVERFLOW;return -1;}len+=add;
    }
    *out=len;return 0;
}
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
static int row_text_length(const RowText *text,int json,size_t *out){
    if(json&&text->quoted)return quoted_length(text->data,text->len,out);
    *out=text->len;return 0;
}
static int row_text_write(const RowText *text,int json,char *out,size_t cap,size_t *used){
    *used=0;
    if(text->numeric){
        dec_status st=dec_to_text(&text->decimal,out,cap,used);
        if(st!=DEC_OK){errno=EOVERFLOW;return -1;}return 0;
    }
    if(json&&text->quoted)return quote_into(text->data,text->len,out,cap,used);
    if(text->len>cap){errno=EOVERFLOW;return -1;}
    if(text->len)memcpy(out,text->data,text->len);
    *used=text->len;return 0;
}
char *typed_packed_field_text(const TypedSchema *ts,const uint8_t *value,size_t len,
                               uint8_t hdr2,size_t index,size_t *text_len){
    if(text_len)*text_len=0;
    if(!text_len||!packed_schema_ok(ts)||index>=(size_t)ts->nfields){errno=EINVAL;return NULL;}
    TypedRowView view;TypedRowSpan span;
    if(typed_row_open(ts,value,len,hdr2,&view)||typed_row_span(&view,index,&span))return NULL;
    if(span.is_null||ts->fields[index].removed){errno=0;return NULL;}
    RowText text;char *out=NULL;
    if(row_text_open(&view,index,&text)){row_text_free(&text);return NULL;}
    if(text.len==SIZE_MAX){errno=EOVERFLOW;goto done;}
    out=malloc(text.len+1);if(!out){errno=ENOMEM;goto done;}
    size_t used=0;
    if(row_text_write(&text,0,out,text.len+1,&used)){free(out);out=NULL;goto done;}
    out[used]=0;*text_len=used;
done:row_text_free(&text);return out;
}
char *typed_packed_decode(const TypedSchema *ts,const uint8_t *value,size_t len,
                           uint8_t hdr2,size_t budget){
    /* Budget is exact JSON output bytes including the terminating NUL.
       Fail before any output-sized allocation, including for empty objects. */
    if(budget<3){errno=ENOMEM;return NULL;}
    if(!packed_schema_ok(ts)){errno=EINVAL;return NULL;}
    TypedRowView view;if(typed_row_open(ts,value,len,hdr2,&view))return NULL;
    size_t need=3;int first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        size_t name=0,part=0;RowText text;
        if(quoted_length(f->name,(size_t)f->name_len,&name))return NULL;
        int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_length(&text,1,&part);
        row_text_free(&text);if(rc)return NULL;
        size_t sep=first?1:2;
        if(name>SIZE_MAX-sep||part>SIZE_MAX-name-sep||need>SIZE_MAX-name-part-sep){
            errno=EOVERFLOW;return NULL;
        }
        need+=name+part+sep;
        if(need>budget){errno=ENOMEM;return NULL;}first=0;
    }
    char *out=malloc(need);if(!out){errno=ENOMEM;return NULL;}
    size_t pos=0;out[pos++]='{';first=1;
    for(int i=0;i<ts->nfields;i++){
        const TypedField *f=&ts->fields[i];if(f->removed)continue;
        if(!first)out[pos++]=',';
        first=0;size_t used=0;
        if(quote_into(f->name,(size_t)f->name_len,out+pos,need-pos,&used))goto fail;
        pos+=used;out[pos++]=':';
        RowText text;int rc=row_text_open(&view,(size_t)i,&text);
        if(!rc)rc=row_text_write(&text,1,out+pos,need-pos,&used);
        row_text_free(&text);if(rc)goto fail;
        pos+=used;
    }
    out[pos++]='}';out[pos]=0;
    if(pos+1!=need){errno=EILSEQ;goto fail;}return out;
fail:free(out);return NULL;
}
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

/* Conservative peak heap allowance: dec_unpack digits + grown limb storage,
 * projection selector copy, and at most one composite join beside output.
 * Record/cache/input bytes and bounded automatic frames remain borrowed. */
size_t typed_packed_render_allowance(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t flags,const char *fields,size_t available){
    if(!packed_schema_ok(ts)||!value||flags!=PKR_FLAGS||
       len<(size_t)ts->packed.prefix+ts->packed.directory||
       len>ts->packed.maximum||value[0]!=ts->packed.prefix-1){errno=EILSEQ;return 0;}
    size_t largest=0;
    for(int i=0;i<ts->nfields;i++)if(ts->packed.fields[i].kind==PKR_NUMERIC){
        const PkrField *f=&ts->packed.fields[i];const uint8_t *entry=value+ts->packed.prefix+f->offset;
        uint32_t bytes=0;for(int j=0;j<4;j++)bytes=(bytes<<8)|entry[4+j];
        if(bytes>f->payload_max||bytes>len-ts->packed.prefix){errno=EILSEQ;return 0;}
        if(bytes>largest)largest=bytes;
    }
    size_t selector=fields?strnlen(fields,MAX_LINE):0;
    if(selector==MAX_LINE){errno=E2BIG;return 0;}
    if(largest>(SIZE_MAX-selector-129)/8){errno=EOVERFLOW;return 0;}
    size_t workspace=largest?8*largest+128:0;
    if(fields)workspace+=selector+1;
    if(available<=workspace+1){errno=ENOBUFS;return 0;}
    /* Admission precedes pkr_view/decimal allocation; full view validation is
     * still mandatory, never replaced by the inexpensive length scan. */
    size_t allowance=(available-workspace)/2;
    if(!allowance)errno=ENOBUFS;
    return allowance;
}

static int row_selected_open(const TypedRowView *view,const CsvSelection *selection,
    size_t index,size_t budget,NamedText *text){
    memset(text,0,sizeof(*text));dec_init(&text->value.decimal);
    if(selection->indices[index]>=0)
        return projection_text_open(view,selection->indices[index],&text->value);
    return named_text_open(view,selection->names[index],budget,text);
}
char *typed_packed_row_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t flags,const void *key,size_t key_len,const char *fields,size_t budget){
    if(!packed_schema_ok(ts)||(!key&&key_len)){errno=EINVAL;return NULL;}
    if(budget<4){errno=ENOMEM;return NULL;}
    CsvSelection selection;
    if(csv_selection_open(ts,fields,&selection)){free(selection.copy);return NULL;}
    TypedRowView view;char *out=NULL;
    if(typed_row_open(ts,value,len,flags,&view))goto done;
    size_t key_bytes=0,need=3;
    if(quoted_length(key,key_len,&key_bytes))goto done;
    if(key_bytes>SIZE_MAX-need){errno=EOVERFLOW;goto done;}
    need+=key_bytes;if(need>budget){errno=ENOMEM;goto done;}
    for(size_t i=0;i<selection.count;i++){
        NamedText text;size_t part=0;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_length(&text.value,1,&part);
        named_text_free(&text);if(rc)goto done;
        if(part==SIZE_MAX||need>SIZE_MAX-part-1){errno=EOVERFLOW;goto done;}
        need+=part+1;if(need>budget){errno=ENOMEM;goto done;}
    }
    out=malloc(need);if(!out){errno=ENOMEM;goto done;}
    size_t pos=0,used=0;out[pos++]='[';
    if(quote_into(key,key_len,out+pos,need-pos,&used))goto fail;
    pos+=used;
    for(size_t i=0;i<selection.count;i++){
        out[pos++]=',';NamedText text;int rc=row_selected_open(&view,&selection,i,budget,&text);
        if(!rc)rc=row_text_write(&text.value,1,out+pos,need-pos,&used);
        named_text_free(&text);if(rc)goto fail;pos+=used;
    }
    out[pos++]=']';out[pos]=0;if(pos+1!=need){errno=EILSEQ;goto fail;}
    goto done;
fail:free(out);out=NULL;
done:free(selection.copy);return out;
}
````````

<!-- R1-BLOCK review-baseline-reader -->
````````c
#include "typed_read_command.h"
#include "packed_csv_command.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
typedef struct ReadPart {struct ReadPart *next;char *data;size_t len;} ReadPart;
typedef struct {
    const FieldSchema *fs;const Schema *schema;const TypedReadOptions *options;
    ReadPart *head,*tail;size_t retained,count;int error,rows,dict,csv,found;
    int64_t skip,start;uint64_t began;const void *key;size_t keylen;
    const char *wire;size_t wirelen;
    char *selection,*legacy;size_t legacy_charge;
    const char *names[MAX_FIELDS];int indices[MAX_FIELDS];size_t nfields;
    const char *legacy_values[MAX_FIELDS];size_t legacy_lengths[MAX_FIELDS];
} ReadCommand;
static int read_error(FILE *out,int code){
    if(!out){errno=code;return -1;}
    const char *message=code==ENOMEM||code==ENOBUFS?
        "{\"error\":\"query memory buffer exceeded or allocation failed\"}\n":
        code==ETIMEDOUT?"{\"error\":\"query timed out\"}\n":
        code==ENOENT?"{\"error\":\"Not found\"}\n":
        code==EIO||code==EILSEQ?"{\"error\":\"corrupt record or storage read failed\"}\n":
        "{\"error\":\"invalid read request\"}\n";
    size_t n=strlen(message);
    if(fwrite(message,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    errno=code;return -1;
}
static void read_free(ReadCommand *c){
    while(c->head){ReadPart *p=c->head;c->head=p->next;free(p->data);free(p);}
    free(c->legacy);c->legacy=NULL;c->legacy_charge=0;
    free(c->selection);c->selection=NULL;c->tail=NULL;c->retained=0;
}
static size_t read_available(ReadCommand *c){
    size_t max=c->options->budget;
    if(c->retained>=max||max-c->retained<=sizeof(ReadPart)){c->error=ENOBUFS;return 0;}
    return max-c->retained-sizeof(ReadPart);
}
static int read_owned_size(ReadCommand *c,char *data,size_t n,size_t allocation){
    if(!data){c->error=errno?errno:EIO;return -1;}
    if(n==SIZE_MAX||n+1>allocation||allocation>read_available(c)){free(data);c->error=ENOBUFS;return -1;}
    ReadPart *p=malloc(sizeof(*p));if(!p){free(data);c->error=ENOMEM;return -1;}
    *p=(ReadPart){.data=data,.len=n};if(c->tail)c->tail->next=p;else c->head=p;
    c->tail=p;c->retained+=sizeof(*p)+allocation;return 0;
}
static int read_owned(ReadCommand *c,char *data,size_t n){
    if(n==SIZE_MAX){free(data);c->error=EOVERFLOW;return -1;}
    return read_owned_size(c,data,n,n+1);
}
static int read_literal(ReadCommand *c,const char *text){
    size_t n=strlen(text);if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(n+1);if(!p){c->error=ENOMEM;return -1;}
    memcpy(p,text,n+1);return read_owned(c,p,n);
}
static int read_quote(ReadCommand *c,const char *s,size_t n){
    size_t need=2;
    for(size_t i=0;i<n;i++){
        unsigned char v=(unsigned char)s[i];size_t k=v=='"'||v=='\\'||v=='\b'||v=='\f'||v=='\n'||v=='\r'||v=='\t'?2:v<32?6:1;
        if(need>SIZE_MAX-k){c->error=EOVERFLOW;return -1;}need+=k;
    }
    if(need==SIZE_MAX||need+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *p=malloc(need+1);if(!p){c->error=ENOMEM;return -1;}
    p[0]='"';int written=json_escape_into(p+1,need-1,s,n);
    if(written<0||(size_t)written!=need-2){free(p);c->error=EILSEQ;return -1;}
    p[need-1]='"';p[need]=0;return read_owned(c,p,need);
}
static int read_deadline(ReadCommand *c){
    if(c->options->timeout_ms&&now_ms_coarse()-c->began>=c->options->timeout_ms){c->error=ETIMEDOUT;return -1;}return 0;
}
static int read_finish(ReadCommand *c,FILE *out){
    if(c->error){int code=c->error;read_free(c);return read_error(out,code);}
    for(ReadPart *p=c->head;p;p=p->next)if(fwrite(p->data,1,p->len,out)!=p->len||ferror(out)){
        read_free(c);errno=EIO;return -1;}
    read_free(c);if(fflush(out)||ferror(out)){errno=EIO;return -1;}return 0;
}
static int read_selection(ReadCommand *c){
    const char *fields=c->options->fields;
    if(!fields||!*fields){
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed)
            {c->names[c->nfields]=c->fs->ts->fields[i].name;c->indices[c->nfields++]=i;}
        return 0;
    }
    size_t n=strnlen(fields,MAX_LINE);
    if(n==MAX_LINE){c->error=E2BIG;return -1;}
    if(n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    c->selection=malloc(n+1);if(!c->selection){c->error=ENOMEM;return -1;}
    memcpy(c->selection,fields,n+1);c->retained+=n+1;
    char *save=NULL;
    for(char *p=strtok_r(c->selection,",",&save);p;p=strtok_r(NULL,",",&save)){
        if(c->nfields==MAX_FIELDS){c->error=E2BIG;return -1;}c->names[c->nfields]=p;c->indices[c->nfields++]=-1;
    }
    return 0;
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n);
static void read_legacy_close(ReadCommand *c){
    if(c->legacy){free(c->legacy);c->legacy=NULL;c->retained-=c->legacy_charge;c->legacy_charge=0;
        memset(c->legacy_values,0,sizeof(c->legacy_values));memset(c->legacy_lengths,0,sizeof(c->legacy_lengths));}
}
static int read_legacy_open(ReadCommand *c,const void *v,size_t n,uint8_t flags){
    if(c->legacy)return 0;
    size_t available=read_available(c);if(n>INT_MAX||available<8){c->error=ENOBUFS;return -1;}
    size_t reserve=available/2;
    c->legacy=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,reserve,&c->legacy_charge);
    if(!c->legacy){c->error=errno?errno:EILSEQ;return -1;}
    c->retained+=c->legacy_charge;
    const char *p=json_skip(c->legacy);if(*p++!='{'){c->error=EILSEQ;return -1;}
    p=json_skip(p);
    while(*p&&*p!='}'){
        if(*p++!='"'){c->error=EILSEQ;return -1;}
        const char *key=p,*end=json_raw_string_end(key);
        if(*end!='"'||(size_t)(end-key)+1>read_available(c)){c->error=ENOBUFS;return -1;}
        char *name=NULL;size_t name_len=0;
        if(json_unescape_cstring(key,(size_t)(end-key),&name,&name_len)){c->error=errno?errno:EINVAL;return -1;}
        p=json_skip(end+1);if(*p++!=':'){free(name);c->error=EILSEQ;return -1;}p=json_skip(p);
        const char *value=p,*after=json_skip_value(p);
        for(int i=0;i<c->fs->ts->nfields;i++)if(!c->fs->ts->fields[i].removed&&
            name_len==(size_t)c->fs->ts->fields[i].name_len&&!memcmp(name,c->fs->ts->fields[i].name,name_len)){
            c->legacy_values[i]=value;c->legacy_lengths[i]=(size_t)(after-value);
        }
        free(name);p=json_skip(after);if(*p==',')p=json_skip(p+1);else if(*p!='}'){c->error=EILSEQ;return -1;}
    }
    return 0;
}
static void read_legacy_span(ReadCommand *c,const char *name,size_t name_len,
    const char **value,size_t *length){
    *value="null";*length=4;
    for(int i=0;i<c->fs->ts->nfields;i++)if(name_len==(size_t)c->fs->ts->fields[i].name_len&&
       !memcmp(name,c->fs->ts->fields[i].name,name_len)){
        if(c->legacy_values[i]){*value=c->legacy_values[i];*length=c->legacy_lengths[i];}
        return;
    }
}

static char *read_legacy_component(ReadCommand *c,const char *name,size_t name_len,size_t *len,int *is_null){
    const char *raw=NULL;size_t n=0;*len=0;*is_null=0;read_legacy_span(c,name,name_len,&raw,&n);
    if(n==4&&!memcmp(raw,"null",4)){*is_null=1;return NULL;}
    size_t available=read_available(c);
    if(n==SIZE_MAX||n+1>available/4){c->error=ENOBUFS;return NULL;}
    char *text=NULL;
    if(n>=2&&raw[0]=='"'&&raw[n-1]=='"'){
        if(json_unescape_string(raw+1,n-2,&text,len)){c->error=errno?errno:EINVAL;return NULL;}
    }else{
        text=malloc(n+1);if(!text){c->error=ENOMEM;return NULL;}memcpy(text,raw,n);text[n]=0;*len=n;
    }
    return text;
}
static char *read_legacy_text(ReadCommand *c,const char *name,int index,size_t *len,int *is_null){
    *len=0;*is_null=0;
    if(index>=0||!strchr(name,'+'))return read_legacy_component(c,name,strlen(name),len,is_null);
    size_t total=0,count=0;const char *p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        if(++count>MAX_FIELDS){c->error=E2BIG;return NULL;}
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        free(part);if(c->error)return NULL;
        if(n>SIZE_MAX-total){c->error=EOVERFLOW;return NULL;}total+=n;p=end;
    }
    if(!total){*is_null=1;return NULL;}
    if(total==SIZE_MAX||total+1>read_available(c)/4){c->error=ENOBUFS;return NULL;}
    char *text=malloc(total+1);if(!text){c->error=ENOMEM;return NULL;}
    c->retained+=total+1;size_t at=0;p=name;
    while(*p){
        if(*p=='+'){p++;continue;}
        const char *end=strchr(p,'+');if(!end)end=p+strlen(p);
        size_t n=0;int null=0;char *part=read_legacy_component(c,p,(size_t)(end-p),&n,&null);
        if(c->error){free(part);free(text);c->retained-=total+1;return NULL;}
        if(n)memcpy(text+at,part,n);
        at+=n;free(part);p=end;
    }
    c->retained-=total+1;text[at]=0;*len=at;return text;
}
static int read_legacy_cell(ReadCommand *c,const char *name,int index){
    if(index<0&&strchr(name,'+')){
        size_t n=0;int null=0;char *text=read_legacy_text(c,name,index,&n,&null);
        if(c->error){free(text);return -1;}if(null)return read_literal(c,"null");
        c->retained+=n+1;int rc=read_quote(c,text,n);c->retained-=n+1;free(text);return rc;
    }
    const char *raw=NULL;size_t n=0;read_legacy_span(c,name,strlen(name),&raw,&n);
    int i=index>=0?index:typed_field_index(c->fs->ts,name);
    if(i>=0&&c->fs->ts->fields[i].type==FT_BOOL&&!(n==4&&!memcmp(raw,"null",4)))return read_quote(c,raw,n);
    if(n==SIZE_MAX||n+1>read_available(c)){c->error=ENOBUFS;return -1;}
    char *copy=malloc(n+1);if(!copy){c->error=ENOMEM;return -1;}memcpy(copy,raw,n);copy[n]=0;
    return read_owned(c,copy,n);
}
static int read_csv_header(ReadCommand *c){
    if(read_literal(c,"key"))return -1;
    if(c->options->kind==TYPED_READ_EXISTS_MULTI){char tail[16];snprintf(tail,sizeof(tail),"%cexists\n",c->options->delimiter);return read_literal(c,tail);}
    if(c->options->kind==TYPED_READ_KEYS||c->options->kind==TYPED_READ_NOT_EXISTS)return read_literal(c,"\n");
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t n=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(c->names[i],strlen(c->names[i]),c->options->delimiter,available,&n):NULL;
        if(read_owned(c,cell,n))return -1;
    }
    return read_literal(c,"\n");
}
static int read_legacy_csv_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(read_legacy_open(c,v,n,flags)||read_csv_key(c,wire,wirelen))return -1;
    char separator[2]={c->options->delimiter,0};
    for(size_t i=0;i<c->nfields;i++){
        if(read_literal(c,separator))return -1;
        size_t length=0;int null=0;char *text=read_legacy_text(c,c->names[i],c->indices[i],&length,&null);
        if(c->error){free(text);return -1;}if(null)continue;
        c->retained+=length+1;size_t used=0,available=read_available(c);
        char *cell=available?typed_csv_cell_text(text,length,c->options->delimiter,available,&used):NULL;
        c->retained-=length+1;free(text);if(read_owned(c,cell,used))return -1;
    }
    return read_literal(c,"\n");
}

static int read_value(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *name,int index){
    size_t available=read_available(c);if(!available)return -1;
    if(!c->fs->ts->packed.nfields){
        if(name||(c->options->fields&&*c->options->fields)){
            if(read_legacy_open(c,v,n,flags))return -1;
            if(name)return read_legacy_cell(c,name,index);
            if(read_literal(c,"{"))return -1;
            for(size_t i=0;i<c->nfields;i++){
                if((i&&read_literal(c,","))||read_quote(c,c->names[i],strlen(c->names[i]))||
                    read_literal(c,":")||read_legacy_cell(c,c->names[i],c->indices[i]))return -1;
            }
            return read_literal(c,"}");
        }
        if(n>INT_MAX){c->error=EOVERFLOW;return -1;}
        size_t allocated=0;char *text=typed_decode_bounded_size(c->fs->ts,v,(int)n,flags,available,&allocated);
        return read_owned_size(c,text,text?strlen(text):0,allocated);
    }
    size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,available);
    if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
    char *text=name?(index>=0?
        typed_packed_projected_cell(c->fs->ts,v,n,flags,(size_t)index,allowance):
        typed_packed_named_json(c->fs->ts,v,n,flags,name,allowance)):
        c->options->fields&&*c->options->fields?
        typed_packed_project_json(c->fs->ts,v,n,flags,c->options->fields,allowance):
        typed_packed_decode(c->fs->ts,v,n,flags,allowance);
    return read_owned(c,text,text?strlen(text):0);
}
static int read_csv_key(ReadCommand *c,const char *wire,size_t n){
    size_t available=read_available(c),used=0;
    char *cell=available?typed_csv_cell_text(wire,n,c->options->delimiter,available,&used):NULL;
    return read_owned(c,cell,used);
}
static int read_row(ReadCommand *c,const void *v,size_t n,uint8_t flags,const char *wire,size_t wirelen){
    if(c->csv&&c->options->kind!=TYPED_READ_KEYS)return read_legacy_csv_row(c,v,n,flags,wire,wirelen);
    if(c->count&&!c->csv&&read_literal(c,","))return -1;
    if(c->options->kind==TYPED_READ_KEYS){
        if(c->csv)return read_csv_key(c,wire,wirelen)||read_literal(c,"\n");
        return read_quote(c,wire,wirelen);
    }
    if(c->rows){
        if(c->fs->ts->packed.nfields){
            size_t allowance=typed_packed_render_allowance(c->fs->ts,v,n,flags,c->options->fields,read_available(c));
            if(!allowance){c->error=errno?errno:ENOBUFS;return -1;}
            char *row=typed_packed_row_json(c->fs->ts,v,n,flags,wire,wirelen,c->options->fields,allowance);
            return read_owned(c,row,row?strlen(row):0);
        }
        if(read_legacy_open(c,v,n,flags))return -1;
        if(read_literal(c,"[")||read_quote(c,wire,wirelen))return -1;
        for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_value(c,v,n,flags,c->names[i],c->indices[i]))return -1;
        return read_literal(c,"]");
    }
    if(!c->dict&&read_literal(c,"{\"key\":"))return -1;
    if(read_quote(c,wire,wirelen)||read_literal(c,c->dict?":":",\"value\":"))return -1;
    if(read_value(c,v,n,flags,NULL,-1))return -1;
    return c->dict?0:read_literal(c,"}");
}
static int read_walk(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;if(read_deadline(c))return 1;
    if(c->skip){c->skip--;return 0;}
    char wire[MAX_KEY_CEILING+1];
    if(!klen||klen>MAX_KEY_CEILING){c->error=EILSEQ;return 1;}
    int n=format_wire_key(c->schema,key,klen,wire,sizeof(wire));
    if(n<0){c->error=EILSEQ;return 1;}
    int rc=read_row(c,value,vlen,flags,wire,(size_t)n);read_legacy_close(c);
    if(rc)return 1;
    c->count++;return c->count>=(size_t)c->options->limit;
}
static int read_lookup(const uint8_t hash[16],const void *key,size_t klen,
    const void *value,size_t vlen,uint8_t flags,void *ctx){
    (void)hash;ReadCommand *c=ctx;
    if(klen!=c->keylen||memcmp(key,c->key,klen))return 0;
    c->found=1;
    if(c->options->kind==TYPED_READ_GET_MULTI){
        if(c->csv)read_row(c,value,vlen,flags,c->wire,c->wirelen);
        else read_value(c,value,vlen,flags,NULL,-1);
        read_legacy_close(c);
    }
    return 1;
}
static int read_offset(const TypedReadOptions *o,int64_t *skip){
    uint64_t n=0;
    for(const unsigned char *p=(const unsigned char*)(o->cursor?o->cursor:"");*p;p++){
        if(*p<'0'||*p>'9'||n>((uint64_t)INT64_MAX-(*p-'0'))/10){errno=EINVAL;return -1;}
        n=n*10+(*p-'0');
    }
    uint64_t add=o->offset>0?(uint64_t)o->offset:0;
    if(n>(uint64_t)INT64_MAX-add){errno=EINVAL;return -1;}
    *skip=(int64_t)(n+add);return 0;
}
static int read_open(ReadCommand *c,const FieldSchema *fs,const Schema *sc,const TypedReadOptions *o,FILE *out){
    memset(c,0,sizeof(*c));c->fs=fs;c->schema=sc;c->options=o;
    if(!fs||!sc||!o||!out||((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&(!fs->ts||!fs->ts->typed))){c->error=EINVAL;return -1;}
    if(o->kind<TYPED_READ_FETCH||o->kind>TYPED_READ_NOT_EXISTS||
       ((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&
        (fs->ts->nfields<1||fs->ts->nfields>MAX_FIELDS))){c->error=EINVAL;return -1;}
    c->began=now_ms_coarse();c->dict=o->format&&!strcmp(o->format,"dict");
    c->rows=o->format&&!strcmp(o->format,"rows");
    c->csv=o->format&&!strcmp(o->format,"csv");
    unsigned char separator=(unsigned char)o->delimiter;
    if(c->csv&&separator!='\t'&&(separator<32||separator>126||separator=='"'||separator=='\\')){
        c->error=EINVAL;return -1;
    }
    if((o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_GET_MULTI)&&read_selection(c))return -1;
    if(o->kind==TYPED_READ_FETCH){
        if(o->limit<=0||read_offset(o,&c->skip)){c->error=EINVAL;return -1;}c->start=c->skip;
        if(c->csv&&o->want_total){c->error=ENOTSUP;return -1;}
    }
    if(c->csv)return read_csv_header(c);
    if(o->kind==TYPED_READ_FETCH){
        if(c->rows){
            if(read_literal(c,"{\"columns\":[\"key\""))return -1;
            for(size_t i=0;i<c->nfields;i++)if(read_literal(c,",")||read_quote(c,c->names[i],strlen(c->names[i])))return -1;
            return read_literal(c,"],\"rows\":[");
        }
        return read_literal(c,o->want_total?(c->dict?"{\"rows\":{":"{\"rows\":["):
            (c->dict?"{\"results\":{":"{\"results\":["));
    }
    if(c->csv){
        if(o->kind!=TYPED_READ_EXISTS_MULTI)return read_literal(c,"key\n");
        char header[16];snprintf(header,sizeof(header),"key%cexists\n",o->delimiter);
        return read_literal(c,header);
    }
    return read_literal(c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"[":"{");
}
static int read_keys_json(ReadCommand *c,SlotcaskDb *db){
    const char *json=c->options->keys;
    if(!json||typed_json_validate(json)||c->schema->max_key<1||c->schema->max_key>MAX_KEY_CEILING){c->error=EINVAL;return -1;}
    const char *p=json_skip(json);if(*p++!='['){c->error=EINVAL;return -1;}p=json_skip(p);
    while(*p!=']'&&!c->error){
        if(read_deadline(c))break;
        if(*p!='"'){c->error=EINVAL;break;}
        const char *start=++p,*end=json_raw_string_end(start);size_t n=(size_t)(end-start);
        size_t wiremax=c->schema->auto_key==AK_NONE?(size_t)c->schema->max_key:MAX_KEY_CEILING;
        if(*end!='"'||n>6*wiremax){c->error=E2BIG;break;}
        if(n==SIZE_MAX||read_available(c)<n+1){c->error=ENOBUFS;break;}
        char *wire=NULL;size_t len=0;
        if(json_unescape_string(start,n,&wire,&len)){c->error=errno==ENOMEM?ENOMEM:EINVAL;break;}
        uint8_t storage[MAX_KEY_CEILING];size_t klen=len;
        if(len>wiremax||!len){free(wire);c->error=EINVAL;break;}
        if(c->schema->auto_key==AK_UUID){
            if(strlen(wire)!=len||parse_uuid_string(wire,storage))c->error=EINVAL;
            klen=16;
        }else if(c->schema->auto_key==AK_SEQ){
            int64_t number;
            if(strlen(wire)!=len||parse_seq_key(wire,&number))c->error=EINVAL;
            else for(int i=7;i>=0;i--){storage[i]=(uint8_t)(uint64_t)number;number=(int64_t)((uint64_t)number>>8);}
            klen=8;
        }else if(c->schema->auto_key==AK_NONE)memcpy(storage,wire,len);
        else c->error=EINVAL;
        if(klen>(size_t)c->schema->max_key)c->error=EINVAL;
        /* Keep wire on the stack while renderers allocate so it is not an
         * uncharged heap allocation concurrent with retained output. */
        char displayed[MAX_KEY_CEILING+1];memcpy(displayed,wire,len);displayed[len]=0;free(wire);
        c->key=storage;c->keylen=klen;c->found=0;c->wire=displayed;c->wirelen=len;
        if(!c->error&&c->options->kind!=TYPED_READ_NOT_EXISTS&&!(c->csv&&c->options->kind==TYPED_READ_GET_MULTI)){
            if(c->count&&!c->csv&&read_literal(c,","))break;
            if(c->csv){
                char sep[2]={c->options->delimiter,0};
                if(read_csv_key(c,displayed,len)||read_literal(c,sep))break;
            }else if(read_quote(c,displayed,len)||read_literal(c,":"))break;
        }
        if(!c->error){
            uint8_t hash[16];compute_hash_raw((const char*)storage,klen,hash);
            if(slotcask_lookup_by_hash_checked(db,hash,read_lookup,c)<0&&!c->error)c->error=EIO;
        }
        if(!c->error){
            if(c->options->kind==TYPED_READ_GET_MULTI){
                if(!c->found){
                    if(c->csv){
                        read_csv_key(c,displayed,len);char separator[2]={c->options->delimiter,0};
                        for(size_t i=0;i<c->nfields&&!c->error;i++)read_literal(c,separator);
                        read_literal(c,"\n");
                    }else read_literal(c,"null");
                }c->count++;
            }
            else if(c->options->kind==TYPED_READ_EXISTS_MULTI){
                read_literal(c,c->found?"true":"false");if(c->csv)read_literal(c,"\n");c->count++;
            }else if(!c->found){
                if(c->csv){read_csv_key(c,displayed,len);read_literal(c,"\n");}
                else{if(c->count)read_literal(c,",");read_quote(c,displayed,len);}c->count++;
            }
        }
        p=json_skip(end+1);if(*p==',')p=json_skip(p+1);else if(*p!=']')c->error=EINVAL;
    }
    return c->error?-1:0;
}
int typed_read_command(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const TypedReadOptions *o,FILE *out){
    if(o&&o->format&&!strcmp(o->format,"csv")&&fs&&fs->ts&&fs->ts->packed.nfields){
        PackedCsvOptions csv={.fields=o->fields,.delimiter=o->delimiter,.buffer_budget=o->budget,.timeout_ms=o->timeout_ms};
        if(o->kind==TYPED_READ_FETCH)return packed_csv_fetch(db,fs,o->offset,o->limit,o->cursor,o->want_total,&csv,out);
        if(o->kind==TYPED_READ_GET_MULTI)return packed_csv_get_multi(db,fs,schema,o->keys,&csv,out);
    }
    ReadCommand c;if(read_open(&c,fs,schema,o,out))return read_finish(&c,out);
    if(!db){c.error=EIO;return read_finish(&c,out);}
    if(o->kind==TYPED_READ_FETCH||o->kind==TYPED_READ_KEYS){
        if(o->limit<=0){c.error=EINVAL;}
        else{
            if(o->kind==TYPED_READ_KEYS)c.skip=o->offset>0?o->offset:0;
            int rc=slotcask_walk_live_checked(db,read_walk,&c);
            if(rc<0&&!c.error)c.error=EIO;
        }
    }else read_keys_json(&c,db);
    if(!c.error)read_deadline(&c);
    if(!c.error){
        if(o->kind==TYPED_READ_FETCH){
            if(c.csv){}
            else if(c.rows)read_literal(&c,"]}\n");
            else{
                read_literal(&c,c.dict?"}":"]");
                if(o->want_total)read_literal(&c,",\"total\":null}\n");
                else if(c.count==(size_t)o->limit){
                    if(c.start>INT64_MAX-(int64_t)c.count)c.error=EOVERFLOW;
                    else{char tail[96];snprintf(tail,sizeof(tail),",\"cursor\":\"%lld\"}\n",(long long)(c.start+(int64_t)c.count));read_literal(&c,tail);}
                }else read_literal(&c,",\"cursor\":null}\n");
            }
        }else if(!c.csv)read_literal(&c,o->kind==TYPED_READ_KEYS||o->kind==TYPED_READ_NOT_EXISTS?"]\n":"}\n");
    }
    return read_finish(&c,out);
}
int typed_read_single(SlotcaskDb *db,const FieldSchema *fs,const void *key,
    size_t keylen,const char *fields,size_t budget,FILE *out){
    TypedReadOptions options={.fields=fields,.budget=budget,.kind=TYPED_READ_GET_MULTI};
    ReadCommand c;memset(&c,0,sizeof(c));c.fs=fs;c.options=&options;
    if(!db||!fs||!fs->ts||!fs->ts->typed||!key||!keylen||keylen>MAX_KEY_CEILING||!out)
        return read_error(out,EINVAL);
    if(read_selection(&c))return read_finish(&c,out);
    c.key=key;c.keylen=keylen;
    uint8_t hash[16];compute_hash_raw(key,keylen,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0&&!c.error)c.error=EIO;
    if(!c.found&&!c.error)c.error=ENOENT;
    if(!c.error)read_literal(&c,"\n");
    return read_finish(&c,out);
}

int typed_read_route(const char *root,const char *object,const TypedReadOptions *o){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_command(db,&fs,&sc,o,g_out?g_out:stdout);
}
int typed_read_single_route(const char *root,const char *object,const void *key,
    size_t len,const char *fields){
    FieldSchema fs;init_field_schema(&fs,root,object);Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    return typed_read_single(db,&fs,key,len,fields,g_query_buffer_max_bytes,g_out?g_out:stdout)!=0;
}
int typed_read_exists(const char *root,const char *object,const void *key,size_t len){
    FILE *out=g_out?g_out:stdout;
    if(!key||!len||len>MAX_KEY_CEILING)return read_error(out,EINVAL);
    Schema sc=load_schema(root,object);
    SlotcaskSchemaInfo info={.splits=sc.splits,.slot_size=sc.slot_size,.streams=sc.streams};
    SlotcaskDb *db SDB_REG_REF = slotcask_registry_get(root,object,&info);
    if(!db)return read_error(out,EIO);
    TypedReadOptions options={.kind=TYPED_READ_EXISTS_MULTI};
    ReadCommand c={.key=key,.keylen=len,.options=&options};
    uint8_t hash[16];compute_hash_raw(key,len,hash);
    int rc=slotcask_lookup_by_hash_checked(db,hash,read_lookup,&c);
    if(rc<0)return read_error(out,EIO);
    const char *text=c.found?"true\n":"false\n";size_t n=strlen(text);
    if(fwrite(text,1,n,out)!=n||fflush(out)||ferror(out)){errno=EIO;return -1;}
    return c.found?0:1;
}
````````

<!-- R1-BLOCK manifest -->
````````json
{
  "base": "0ef1a0ba3b33ec94c947e85091f866b39a1ee583",
  "branch": "fix/phase1-safe-scalar-conversions",
  "protected": {
    "path": "docs/plans/2026-10-19-durability-certification-rig.md",
    "sha": "fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135"
  },
  "checkpoints": {
    "0": {
      "build.sh": "f6355ec998a85821e70da3897af243903627caa90071c101d6c194f43c3a9623",
      "docs/concepts/ordinary-type-integration.md": null,
      "docs/concepts/packed-csv-commands.md": "10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4",
      "docs/concepts/typed-row-adapter.md": "2ab98d158cc7408d85e48ddf2cf5485098dd43eefe072cba272d96aeaf0ca24f",
      "npm/README.md": "eb10e4e92de66d965ca271f20d7427f6e4a893b6f879a5f6d71b21886ba763a7",
      "npm/csv-bytes.js": null,
      "npm/index.d.ts": "169874670b8f2bff631a6cfcc6dbdd37d078b73c1cb3f82e6b9d2da800efb5cd",
      "npm/index.js": "d57ce3a9c99b6e0a8d0bd63cde42a1888ffe0b4c6d16ee8514e1673677e0c6a5",
      "npm/test/csv-bytes.js": null,
      "src/cli/cli.h": "8bbf8c8cd894a91dae7bd3a124a9639af476e055f0b323cc68ee2e034ab5d9cf",
      "src/cli/main.c": "c370a00048bea957ffb47d0683ecc03f5f8feb86512f73af77e0c89f73f0b00d",
      "src/cli/views.c": "c04924e9d3310b9d3f2126a0ab915a0e02703c710f11e5065760a0c567d288a8",
      "src/db/config.c": "6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a",
      "src/db/packed_csv_command.c": "b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb",
      "src/db/query_bulk.c": "c619ab1d8439f9468b80e7bd12fbc6f3d578b29436cef45d47a568db91f07908",
      "src/db/query_find.c": "912588f74c20223c288aacffed15d8995e117d3db0a7398699073ac235436159",
      "src/db/query_schema.c": "ea3530375358664e300821703a2e718c9da21deb850e9ea15f58cd384cdeea37",
      "src/db/schema_wire.c": null,
      "src/db/schema_wire.h": null,
      "src/db/server.c": "a831a4800fd0d3b0537d46657f82dbf1ace8e79793d6e3b7ddb82a404a5761f2",
      "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
      "src/db/slotcask.h": "59a76eb30281627e2dd50b889f3327905c7b00462e5f747105bd8f8f5b59d24c",
      "src/db/storage.c": "dd0e0a9e72c9e4bcd38eda3ba1b9240ec6165d07a5c949f16af2f824087ec6cd",
      "src/db/typed_bulk_insert.c": "90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b",
      "src/db/typed_bulk_insert.h": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
      "src/db/typed_read_command.c": null,
      "src/db/typed_read_command.h": null,
      "src/db/typed_row.c": "dad6b85428457d5abb9004b77610cc8b2f5670bcea62524af737783666ff5998",
      "src/db/typed_row.h": "13cacbc07ece575b17ac0a22788f5a6ff86e2e9aa547e1e2f368138f5c50fdea",
      "src/db/types.h": "3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361",
      "src/test/cases/test_computed_update.c": "7cd376695861985b6706efda12d1df9d298227e7029720d070cc30aeb6ea76bd",
      "src/test/cases/test_not_null_edit_modifiers.c": "e46eee863d592a2a66293509336910d9bcab013841a1ac93febf0012ef7a6d7b",
      "src/test/cases/test_r1_routes.c": null,
      "src/test/cases/test_scalar_conversion_safety.c": "6aa1bee28234f30b4918d91cbff18a1215190683a4b13810589a594d5193052d",
      "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
    },
    "1": {
      "build.sh": "79a751a8fc570c12d17473b53d965867389595fbda7ceccbbcd9a4f334a6f38e",
      "docs/concepts/ordinary-type-integration.md": null,
      "docs/concepts/packed-csv-commands.md": "10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4",
      "docs/concepts/typed-row-adapter.md": "2ab98d158cc7408d85e48ddf2cf5485098dd43eefe072cba272d96aeaf0ca24f",
      "npm/README.md": "eb10e4e92de66d965ca271f20d7427f6e4a893b6f879a5f6d71b21886ba763a7",
      "npm/csv-bytes.js": null,
      "npm/index.d.ts": "169874670b8f2bff631a6cfcc6dbdd37d078b73c1cb3f82e6b9d2da800efb5cd",
      "npm/index.js": "d57ce3a9c99b6e0a8d0bd63cde42a1888ffe0b4c6d16ee8514e1673677e0c6a5",
      "npm/test/csv-bytes.js": null,
      "src/cli/cli.h": "8bbf8c8cd894a91dae7bd3a124a9639af476e055f0b323cc68ee2e034ab5d9cf",
      "src/cli/main.c": "c370a00048bea957ffb47d0683ecc03f5f8feb86512f73af77e0c89f73f0b00d",
      "src/cli/views.c": "c04924e9d3310b9d3f2126a0ab915a0e02703c710f11e5065760a0c567d288a8",
      "src/db/config.c": "b31c47d55944ff0b9283196064399f36654c7cc0b7b7010a41a55f881e66001a",
      "src/db/packed_csv_command.c": "b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb",
      "src/db/query_bulk.c": "c619ab1d8439f9468b80e7bd12fbc6f3d578b29436cef45d47a568db91f07908",
      "src/db/query_find.c": "912588f74c20223c288aacffed15d8995e117d3db0a7398699073ac235436159",
      "src/db/query_schema.c": "ea3530375358664e300821703a2e718c9da21deb850e9ea15f58cd384cdeea37",
      "src/db/schema_wire.c": "accc7dd0fa1a80ec90c1f529277d3af269d4b2e5c5f68810e48eabf898874eee",
      "src/db/schema_wire.h": "915ed13cf2243b5f284ea0d83161a9904f94ae8578568f244f55c6ae9ca8f65e",
      "src/db/server.c": "a831a4800fd0d3b0537d46657f82dbf1ace8e79793d6e3b7ddb82a404a5761f2",
      "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
      "src/db/slotcask.h": "59a76eb30281627e2dd50b889f3327905c7b00462e5f747105bd8f8f5b59d24c",
      "src/db/storage.c": "dd0e0a9e72c9e4bcd38eda3ba1b9240ec6165d07a5c949f16af2f824087ec6cd",
      "src/db/typed_bulk_insert.c": "90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b",
      "src/db/typed_bulk_insert.h": "865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d",
      "src/db/typed_read_command.c": "52662a97c3213ba0a6a7b505b63109c62d61d8197daff2602888bb3b3c78156a",
      "src/db/typed_read_command.h": "f745ff56038ed01d3e55b980ed37a575353e9503374712d95245b8d683f85b4a",
      "src/db/typed_row.c": "cc9356d34ccf60961dcdd10d57cbb42b180e8bf591d9f4ce55fcb74c44940d78",
      "src/db/typed_row.h": "2012c657278cbbc7369e7a4436f3863fe60ea08949fc1c76b12a81c4b7bb25bf",
      "src/db/types.h": "7e92fe24c6587fcad276be2e8d71f1b0f8bc4a080f358af20ddbab3fc8189bac",
      "src/test/cases/test_computed_update.c": "7cd376695861985b6706efda12d1df9d298227e7029720d070cc30aeb6ea76bd",
      "src/test/cases/test_not_null_edit_modifiers.c": "e46eee863d592a2a66293509336910d9bcab013841a1ac93febf0012ef7a6d7b",
      "src/test/cases/test_r1_routes.c": "29601075cd610cdb4d1bf24710bcc1f96f3b5c4f3bd5747a99b956d3e1e1efc4",
      "src/test/cases/test_scalar_conversion_safety.c": "6aa1bee28234f30b4918d91cbff18a1215190683a4b13810589a594d5193052d",
      "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
    },
    "2": {
      "build.sh": "79a751a8fc570c12d17473b53d965867389595fbda7ceccbbcd9a4f334a6f38e",
      "docs/concepts/ordinary-type-integration.md": null,
      "docs/concepts/packed-csv-commands.md": "10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4",
      "docs/concepts/typed-row-adapter.md": "2ab98d158cc7408d85e48ddf2cf5485098dd43eefe072cba272d96aeaf0ca24f",
      "npm/README.md": "eb10e4e92de66d965ca271f20d7427f6e4a893b6f879a5f6d71b21886ba763a7",
      "npm/csv-bytes.js": null,
      "npm/index.d.ts": "169874670b8f2bff631a6cfcc6dbdd37d078b73c1cb3f82e6b9d2da800efb5cd",
      "npm/index.js": "d57ce3a9c99b6e0a8d0bd63cde42a1888ffe0b4c6d16ee8514e1673677e0c6a5",
      "npm/test/csv-bytes.js": null,
      "src/cli/cli.h": "0b0bc71a3487f3a05471700ac6005b874498b83c7783d2a740dc6d2084be6c6b",
      "src/cli/main.c": "163a7b03b80d52615231329124ba25e8aa80a2a170897d888e09ee13b8a88324",
      "src/cli/views.c": "9564030c06761eb8830f0ba000c102ab1ec6ceecb5dcc4436cd68f8378d51ed0",
      "src/db/config.c": "f9257bb95be7455ffad4be357043c57d53a003101e73473674f57e182fe6d7bc",
      "src/db/packed_csv_command.c": "a32042e04880817919873aa0cb6f831eafbd15d6cfc440e878ab400d6df6fda6",
      "src/db/query_bulk.c": "0d838f759f8db712acf888b8f72fa627f9ee61ed325bac910d01f5cfc45f7dc6",
      "src/db/query_find.c": "ca2d6625e72d0d6a7afc2ce64b39cae7be18620103bdc4f47abf0b17e8a1453c",
      "src/db/query_schema.c": "788cfbf028a99ee9d0158283eff37903d930261d8139160b7c71beb8ae3262e9",
      "src/db/schema_wire.c": "b2c99b2f2de4417705160987cdd2ef04b70bc55fbcc336225d8f9b9999622ba5",
      "src/db/schema_wire.h": "5a79ce7557176fabf3d8f8d0b16f9cd9fb51533e5136abfc55ff031266d86e0c",
      "src/db/server.c": "0a65ea89fb9f8a733cdddc0c30873496cb8191b313b81293d0ff8ad4ce8d6aea",
      "src/db/slotcask.c": "bc2ea04ac2c495599ff4cf88a94aac95fd58c1a419c5046f13fa4c3cc337bb6c",
      "src/db/slotcask.h": "ac3d0f8bfe3381c8482a1e5294243a123a8db026c08591ec0e8c845369212b9c",
      "src/db/storage.c": "4c565fe0f4909a0338a7fddd83c64ad14d5dd70477d22be4e763fb141b025cf2",
      "src/db/typed_bulk_insert.c": "4249d336b059e68f646b78c543c9b5979d1ff10170610db9341e7eff72e988db",
      "src/db/typed_bulk_insert.h": "9ddf03030cb50de41ac2fce55dfb0409204ba48de15e26bfe05b21b393173dec",
      "src/db/typed_read_command.c": "f7c59a9692aa21e21bbddce6a00910561e18b1329934c890b2c3f4c828db2246",
      "src/db/typed_read_command.h": "f745ff56038ed01d3e55b980ed37a575353e9503374712d95245b8d683f85b4a",
      "src/db/typed_row.c": "8afb663ea16aca04f3268a78792c4dc9f5a7b739a95df5e9eff18050853cb505",
      "src/db/typed_row.h": "5b27409a4bd4939e59e3ea794800527586d5d6a77297d08107954be84ede0686",
      "src/db/types.h": "f2526f08c72eb744e10e2564702191fc2312b80000f1aef982313e76264024bc",
      "src/test/cases/test_computed_update.c": "e190e1e3606959b9a674d6b20ea2aa39c48121ca8a76f86248e68c264cbdf71e",
      "src/test/cases/test_not_null_edit_modifiers.c": "a245aa15eae29b71a562a5f533ec4c72648c22366e0c11fade978b86c2fadc36",
      "src/test/cases/test_r1_routes.c": "29601075cd610cdb4d1bf24710bcc1f96f3b5c4f3bd5747a99b956d3e1e1efc4",
      "src/test/cases/test_scalar_conversion_safety.c": "1d236b26bffacd2ec0fc7f49a0908df8972571347c633e03c210c296740b6bc9",
      "src/test/cases/test_single_write_integration.c": "9c0ebd6ec454c2deff36a172cd0858ac409c9d5060473f8eb1b7e693805854e0"
    },
    "3": {
      "build.sh": "79a751a8fc570c12d17473b53d965867389595fbda7ceccbbcd9a4f334a6f38e",
      "docs/concepts/ordinary-type-integration.md": "6ad61ee22863ec2f6c7164dad67a3dfa6742c8d359f5d4455277ffacc2356c9a",
      "docs/concepts/packed-csv-commands.md": "098678bcb4a921f3aab86f47daf6214e8f64f7746ebad93c200993001cccbe0e",
      "docs/concepts/typed-row-adapter.md": "ee2926435a6f0bc28b91b6b8c067d2d017a997fd2a9d063254707399021d5ef0",
      "npm/README.md": "a0e09faef927d8344623d373f6016878429a6f30da9ad7aaf457540da90f4288",
      "npm/csv-bytes.js": "1f4f09d9cb62361274491550aaeba90023af44930731746b950843db70641b03",
      "npm/index.d.ts": "81161812c3e15db922d3140d1d4e7ef64681812a96d017c536fe6a5796fc303a",
      "npm/index.js": "460cc98b9f9ef23fb96b2431db942088ad6443fca240acd8c9531b11361aef5a",
      "npm/test/csv-bytes.js": "a2c62b1011239bb1c9581aa1355a8d9bf97ce75c59003d8b17189a1e379836f0",
      "src/cli/cli.h": "0b0bc71a3487f3a05471700ac6005b874498b83c7783d2a740dc6d2084be6c6b",
      "src/cli/main.c": "163a7b03b80d52615231329124ba25e8aa80a2a170897d888e09ee13b8a88324",
      "src/cli/views.c": "9564030c06761eb8830f0ba000c102ab1ec6ceecb5dcc4436cd68f8378d51ed0",
      "src/db/config.c": "f9257bb95be7455ffad4be357043c57d53a003101e73473674f57e182fe6d7bc",
      "src/db/packed_csv_command.c": "a32042e04880817919873aa0cb6f831eafbd15d6cfc440e878ab400d6df6fda6",
      "src/db/query_bulk.c": "0d838f759f8db712acf888b8f72fa627f9ee61ed325bac910d01f5cfc45f7dc6",
      "src/db/query_find.c": "ca2d6625e72d0d6a7afc2ce64b39cae7be18620103bdc4f47abf0b17e8a1453c",
      "src/db/query_schema.c": "788cfbf028a99ee9d0158283eff37903d930261d8139160b7c71beb8ae3262e9",
      "src/db/schema_wire.c": "b2c99b2f2de4417705160987cdd2ef04b70bc55fbcc336225d8f9b9999622ba5",
      "src/db/schema_wire.h": "5a79ce7557176fabf3d8f8d0b16f9cd9fb51533e5136abfc55ff031266d86e0c",
      "src/db/server.c": "0a65ea89fb9f8a733cdddc0c30873496cb8191b313b81293d0ff8ad4ce8d6aea",
      "src/db/slotcask.c": "bc2ea04ac2c495599ff4cf88a94aac95fd58c1a419c5046f13fa4c3cc337bb6c",
      "src/db/slotcask.h": "ac3d0f8bfe3381c8482a1e5294243a123a8db026c08591ec0e8c845369212b9c",
      "src/db/storage.c": "4c565fe0f4909a0338a7fddd83c64ad14d5dd70477d22be4e763fb141b025cf2",
      "src/db/typed_bulk_insert.c": "4249d336b059e68f646b78c543c9b5979d1ff10170610db9341e7eff72e988db",
      "src/db/typed_bulk_insert.h": "9ddf03030cb50de41ac2fce55dfb0409204ba48de15e26bfe05b21b393173dec",
      "src/db/typed_read_command.c": "f7c59a9692aa21e21bbddce6a00910561e18b1329934c890b2c3f4c828db2246",
      "src/db/typed_read_command.h": "f745ff56038ed01d3e55b980ed37a575353e9503374712d95245b8d683f85b4a",
      "src/db/typed_row.c": "8afb663ea16aca04f3268a78792c4dc9f5a7b739a95df5e9eff18050853cb505",
      "src/db/typed_row.h": "5b27409a4bd4939e59e3ea794800527586d5d6a77297d08107954be84ede0686",
      "src/db/types.h": "f2526f08c72eb744e10e2564702191fc2312b80000f1aef982313e76264024bc",
      "src/test/cases/test_computed_update.c": "e190e1e3606959b9a674d6b20ea2aa39c48121ca8a76f86248e68c264cbdf71e",
      "src/test/cases/test_not_null_edit_modifiers.c": "a245aa15eae29b71a562a5f533ec4c72648c22366e0c11fade978b86c2fadc36",
      "src/test/cases/test_r1_routes.c": "29601075cd610cdb4d1bf24710bcc1f96f3b5c4f3bd5747a99b956d3e1e1efc4",
      "src/test/cases/test_scalar_conversion_safety.c": "1d236b26bffacd2ec0fc7f49a0908df8972571347c633e03c210c296740b6bc9",
      "src/test/cases/test_single_write_integration.c": "9c0ebd6ec454c2deff36a172cd0858ac409c9d5060473f8eb1b7e693805854e0"
    }
  },
  "edits": [
    {
      "stage": 1,
      "path": "build.sh",
      "old": "s1e1-old",
      "new": "s1e1-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "build.sh",
      "old": "s1e2-old",
      "new": "s1e2-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "build.sh",
      "old": "s1e3-old",
      "new": "s1e3-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "build.sh",
      "old": "s1e4-old",
      "new": "s1e4-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/db/config.c",
      "old": "s1e5-old",
      "new": "s1e5-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/db/schema_wire.c",
      "old": "s1e6-old",
      "new": "s1e6-new",
      "create": true
    },
    {
      "stage": 1,
      "path": "src/db/schema_wire.h",
      "old": "s1e7-old",
      "new": "s1e7-new",
      "create": true
    },
    {
      "stage": 1,
      "path": "src/db/typed_read_command.c",
      "old": "s1e8-old",
      "new": "s1e8-new",
      "create": true
    },
    {
      "stage": 1,
      "path": "src/db/typed_read_command.h",
      "old": "s1e9-old",
      "new": "s1e9-new",
      "create": true
    },
    {
      "stage": 1,
      "path": "src/db/typed_row.c",
      "old": "s1e10-old",
      "new": "s1e10-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/db/typed_row.h",
      "old": "s1e11-old",
      "new": "s1e11-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/db/types.h",
      "old": "s1e12-old",
      "new": "s1e12-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/db/types.h",
      "old": "s1e13-old",
      "new": "s1e13-new",
      "create": false
    },
    {
      "stage": 1,
      "path": "src/test/cases/test_r1_routes.c",
      "old": "s1e14-old",
      "new": "s1e14-new",
      "create": true
    },
    {
      "stage": 2,
      "path": "src/cli/cli.h",
      "old": "s2e15-old",
      "new": "s2e15-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/main.c",
      "old": "s2e16-old",
      "new": "s2e16-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/main.c",
      "old": "s2e17-old",
      "new": "s2e17-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/main.c",
      "old": "s2e18-old",
      "new": "s2e18-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/main.c",
      "old": "s2e19-old",
      "new": "s2e19-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e20-old",
      "new": "s2e20-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e21-old",
      "new": "s2e21-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e22-old",
      "new": "s2e22-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e23-old",
      "new": "s2e23-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e24-old",
      "new": "s2e24-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e25-old",
      "new": "s2e25-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e26-old",
      "new": "s2e26-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e27-old",
      "new": "s2e27-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e28-old",
      "new": "s2e28-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e29-old",
      "new": "s2e29-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e30-old",
      "new": "s2e30-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/cli/views.c",
      "old": "s2e31-old",
      "new": "s2e31-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e32-old",
      "new": "s2e32-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e33-old",
      "new": "s2e33-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e34-old",
      "new": "s2e34-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e35-old",
      "new": "s2e35-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e36-old",
      "new": "s2e36-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e37-old",
      "new": "s2e37-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e38-old",
      "new": "s2e38-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e39-old",
      "new": "s2e39-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e40-old",
      "new": "s2e40-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e41-old",
      "new": "s2e41-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e42-old",
      "new": "s2e42-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e43-old",
      "new": "s2e43-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/config.c",
      "old": "s2e44-old",
      "new": "s2e44-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/packed_csv_command.c",
      "old": "s2e45-old",
      "new": "s2e45-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/packed_csv_command.c",
      "old": "s2e46-old",
      "new": "s2e46-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/packed_csv_command.c",
      "old": "s2e47-old",
      "new": "s2e47-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_bulk.c",
      "old": "s2e48-old",
      "new": "s2e48-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_bulk.c",
      "old": "s2e49-old",
      "new": "s2e49-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_find.c",
      "old": "s2e50-old",
      "new": "s2e50-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_find.c",
      "old": "s2e51-old",
      "new": "s2e51-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_find.c",
      "old": "s2e52-old",
      "new": "s2e52-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_find.c",
      "old": "s2e53-old",
      "new": "s2e53-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e54-old",
      "new": "s2e54-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e55-old",
      "new": "s2e55-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e56-old",
      "new": "s2e56-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e57-old",
      "new": "s2e57-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e58-old",
      "new": "s2e58-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e59-old",
      "new": "s2e59-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/query_schema.c",
      "old": "s2e60-old",
      "new": "s2e60-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/schema_wire.c",
      "old": "s2e61-old",
      "new": "s2e61-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/schema_wire.h",
      "old": "s2e62-old",
      "new": "s2e62-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e63-old",
      "new": "s2e63-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e64-old",
      "new": "s2e64-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e65-old",
      "new": "s2e65-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e66-old",
      "new": "s2e66-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e67-old",
      "new": "s2e67-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/server.c",
      "old": "s2e68-old",
      "new": "s2e68-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/slotcask.c",
      "old": "s2e69-old",
      "new": "s2e69-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/slotcask.h",
      "old": "s2e70-old",
      "new": "s2e70-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/slotcask.h",
      "old": "s2e71-old",
      "new": "s2e71-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e72-old",
      "new": "s2e72-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e73-old",
      "new": "s2e73-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e74-old",
      "new": "s2e74-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e75-old",
      "new": "s2e75-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e76-old",
      "new": "s2e76-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e77-old",
      "new": "s2e77-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e78-old",
      "new": "s2e78-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e79-old",
      "new": "s2e79-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/storage.c",
      "old": "s2e80-old",
      "new": "s2e80-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_bulk_insert.c",
      "old": "s2e81-old",
      "new": "s2e81-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_bulk_insert.c",
      "old": "s2e82-old",
      "new": "s2e82-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_bulk_insert.h",
      "old": "s2e83-old",
      "new": "s2e83-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_read_command.c",
      "old": "s2e84-old",
      "new": "s2e84-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e85-old",
      "new": "s2e85-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e86-old",
      "new": "s2e86-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e87-old",
      "new": "s2e87-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e88-old",
      "new": "s2e88-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e89-old",
      "new": "s2e89-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e90-old",
      "new": "s2e90-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e91-old",
      "new": "s2e91-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.c",
      "old": "s2e92-old",
      "new": "s2e92-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.h",
      "old": "s2e93-old",
      "new": "s2e93-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/typed_row.h",
      "old": "s2e94-old",
      "new": "s2e94-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/types.h",
      "old": "s2e95-old",
      "new": "s2e95-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/types.h",
      "old": "s2e96-old",
      "new": "s2e96-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/db/types.h",
      "old": "s2e97-old",
      "new": "s2e97-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/test/cases/test_computed_update.c",
      "old": "s2e98-old",
      "new": "s2e98-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/test/cases/test_not_null_edit_modifiers.c",
      "old": "s2e99-old",
      "new": "s2e99-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/test/cases/test_scalar_conversion_safety.c",
      "old": "s2e100-old",
      "new": "s2e100-new",
      "create": false
    },
    {
      "stage": 2,
      "path": "src/test/cases/test_single_write_integration.c",
      "old": "s2e101-old",
      "new": "s2e101-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "docs/concepts/ordinary-type-integration.md",
      "old": "s3e102-old",
      "new": "s3e102-new",
      "create": true
    },
    {
      "stage": 3,
      "path": "docs/concepts/packed-csv-commands.md",
      "old": "s3e103-old",
      "new": "s3e103-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "docs/concepts/typed-row-adapter.md",
      "old": "s3e104-old",
      "new": "s3e104-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/README.md",
      "old": "s3e105-old",
      "new": "s3e105-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/csv-bytes.js",
      "old": "s3e106-old",
      "new": "s3e106-new",
      "create": true
    },
    {
      "stage": 3,
      "path": "npm/index.d.ts",
      "old": "s3e107-old",
      "new": "s3e107-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/index.d.ts",
      "old": "s3e108-old",
      "new": "s3e108-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/index.d.ts",
      "old": "s3e109-old",
      "new": "s3e109-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/index.d.ts",
      "old": "s3e110-old",
      "new": "s3e110-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/index.js",
      "old": "s3e111-old",
      "new": "s3e111-new",
      "create": false
    },
    {
      "stage": 3,
      "path": "npm/test/csv-bytes.js",
      "old": "s3e112-old",
      "new": "s3e112-new",
      "create": true
    }
  ],
  "boundaries": {
    "src/db/decnum.c": "e2da87e467034b49a9cfecb8c8ba7d0430b61f82545d10b7c85bff876dd5e5ed",
    "src/db/decnum.h": "2148631ae519f28649ad3cf386511982ed295850f3176215cbab6c65b265e3a9",
    "src/db/packed_record.c": "17cc0dd151f0a747abaff98291f6f7364d39ecd7e1cdf7c7798dbf4b73c7664c",
    "src/db/packed_record.h": "56682286bd12a932d69fc8bdd46961db2a594357134cf35d90d61a402925ddab",
    "src/db/query.c": "3c97197a529ea68c4a077581c46a53733fbac183108925d9230f0b9b00402e9e",
    "src/db/parallel.c": "242c0051d09674d9873d8a09d50ea7a9ebc7159487a5c75e819196fb0bb3fe10",
    "src/db/query_plan.c": "57cf9a45fd755fc4c4a38b7e13edd212ddba192ab2b2f46ca9aeac697237c5dd",
    "src/db/query_aggregate.c": "6fc3b133351e59fc0a7bd708e94df4bf6fefa0b3e5b56dcbb02507dbeea09c5f",
    "src/db/query_join.c": "df03bf908c1251d99b4fbdf1f18a84eafb401272faee71540333955d0c73cc05",
    "src/db/index.c": "2951fdc581f44c0024b8dde5d495fff7b64248d3ff5ecb4ea9bfc59cf25ae710",
    "src/db/btree.c": "e07c2b05ed71288ec352a316635d3a9a98113671889d3cd3dbd21518b4042825",
    "src/db/bitmap.c": "bed0b32fbbce597e24f58f4172d3b642ce41c77cffbead6b8359e3cfea70442f",
    "src/db/objlock.c": "0da1ea43c5a4541a184a70cbd8b565cffc1be9a15c3ed782b0a745a2951a9f9c",
    "src/db/seg_scan_varlen.h": "84e45894a415e37f1ce730be5f47e577c915822899f048e53066ee423d354903",
    "src/db/shard_db_internal.h": "1ed4c2043df0f6f164e41b0a40796a7f975c5ba759ef4c9c5488e443529991a4",
    "src/db/embedded.c": "6311c45af87446db88e0c9d387296c77c705af3928f5096e9f585e2445591cf6",
    "npm/src/binding.c": "ed4fa79dcb68316f5af18aa92875ce482e66b39ead3f1dab33579b561d3867a2"
  },
  "concurrency_prefix": {
    "length": 475195,
    "sha": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5"
  }
}
````````

<!-- R1-BLOCK export-probe -->
````````python
#!/usr/bin/env python3
"""Usage: python3 blind-export-regression.py /absolute/path/to/shard-db"""
import json, os, pathlib, socket, subprocess, sys, tempfile, threading

binary = str(pathlib.Path(sys.argv[1]).resolve())
failures = []

def run_case(label, response, named, succeeds=False):
    with tempfile.TemporaryDirectory(prefix="shard-export-regression-") as scratch:
        root = pathlib.Path(scratch)
        logs = root / "logs"
        logs.mkdir()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(2)
        port = listener.getsockname()[1]
        (root / "db.env").write_text(f"DB_ROOT={root}\nLOG_DIR={logs}\nTLS_ENABLE=0\n")
        (logs / "shard-db.pid").write_text(f"{os.getpid()}\n{port}\n")
        requests, errors = [], []
        def serve():
            try:
                for reply in (b'["tenant"]', response):
                    conn, _ = listener.accept()
                    with conn:
                        conn.settimeout(2)
                        data = b""
                        while b"\n" not in data:
                            part = conn.recv(65536)
                            if not part:
                                break
                            data += part
                        requests.append(data)
                        conn.sendall(reply + b"\x00\n")
            except Exception as error:
                errors.append(repr(error))
            finally:
                listener.close()
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        destination = root / "manifest.json"
        sentinel = b"prior manifest must survive\n"
        destination.write_bytes(sentinel)
        result = subprocess.run([binary, "export-schema", str(destination) if named else "-"], cwd=root,
                                capture_output=True, timeout=10)
        worker.join(3)
        issues = []
        if worker.is_alive() or errors or len(requests) != 2:
            issues.append(f"fake server failure: {errors!r}, requests={requests!r}")
        if succeeds:
            if result.returncode:
                issues.append(f"expected success, got {result.returncode}: {result.stderr!r}")
            payload = destination.read_bytes() if named else result.stdout
            try:
                if json.loads(payload) != {"dirs":["tenant"], "objects":[]}:
                    issues.append(f"wrong successful manifest {payload!r}")
            except (ValueError, UnicodeError) as error:
                issues.append(f"invalid success JSON: {error}")
        else:
            if result.returncode == 0:
                issues.append("returned success for failed list-objects")
            if destination.read_bytes() != sentinel:
                issues.append(f"overwrote prior manifest with {destination.read_bytes()!r}")
            if result.stdout:
                issues.append(f"published stdout manifest {result.stdout!r}")
        description = f"{label} / {'named' if named else 'stdout'}"
        print(("FAIL " if issues else "PASS ") + description)
        for issue in issues:
            print("  " + issue)
        failures.extend((description, issue) for issue in issues)

cases = [
    ("capacity error", b'{"error":"server at capacity"}'),
    ("error plus empty objects", b'{"error":"server at capacity","objects":[]}'),
    ("missing objects", b'{"unexpected":[]}'),
    ("empty response object", b'{}'),
    ("wrong objects type", b'{"objects":null}'),
    ("unterminated objects array", b'{"objects":['),
    ("non-string objects", b'{"objects":[1]}'),
    ("trailing comma", b'{"objects":["x",]}'),
]
for name, response in cases:
    for named in (True, False):
        run_case(name, response, named)
for named in (True, False):
    run_case("empty tenant success", b'{"objects":[]}', named, succeeds=True)
sys.exit(bool(failures))
````````

<!-- R1-BLOCK corrupt-flag-baseline -->
````````c


/* Ordinary checked reads use exactly the existing keyfile -> segment cache
 * read-handle order. The caller already holds the matching object read lock;
 * no writer gate/reservation is acquired and no global snapshot is promised. */
static int slotcask_emit_checked(SlotcaskDb *db,const SlotcaskKfEntry *entry,
    SlotcaskScanCb callback,void *ctx){
    if(entry->stream_id>=db->num_streams){errno=EUCLEAN;return -1;}
    char path[PATH_MAX];seg_path_for(path,db->data_dir,entry->stream_id,entry->file_id);
    SlotcaskSegHandle segment={.slot=-1,.fd=-1};
    if(segcache_acquire(&segment,path,0,0,0))return -1;
    int rc=0;
    if(entry->offset>segment.map_size||segment.map_size-entry->offset<24){errno=EUCLEAN;rc=-1;}
    else{
        const uint8_t *record=segment.map+entry->offset;
        uint16_t keylen=seg_rec_klen(record);uint32_t value_len=seg_rec_vlen(record);
        size_t encoded=24u+(size_t)keylen+(size_t)value_len;
        size_t padded=slotcask_record_size_varlen(keylen,value_len);
        if(encoded>(size_t)db->slot_size||padded>(size_t)db->slot_size||
           padded>segment.map_size-entry->offset||!seg_rec_live_with_hash(record,entry->hash)){
            errno=EUCLEAN;rc=-1;
        }else if(callback(entry->hash,record+24,keylen,record+24+keylen,value_len,record[19],ctx))rc=1;
    }
    int saved=errno;segcache_release(&segment);errno=saved;return rc;
}
int slotcask_walk_live_checked(SlotcaskDb *db,SlotcaskScanCb callback,void *ctx){
    if(!db||!callback||db->num_shards<1){errno=EINVAL;return -1;}
    for(int shard=0;shard<db->num_shards;shard++){
        char path[PATH_MAX];kf_path_for(path,db->data_dir,shard);
        SlotcaskKfHandle keyfile;
        if(kfcache_acquire(&keyfile,path,db->slots_per_shard,0))return -1;
        int rc=0;
        for(size_t slot=0;slot<keyfile.capacity;slot++){
            const SlotcaskKfEntry *entry=&keyfile.map[slot];
            uint8_t flag=__atomic_load_n(&entry->flag,__ATOMIC_ACQUIRE);
            if(!flag||flag==2)continue;
            if(flag!=1){errno=EUCLEAN;rc=-1;break;}
            rc=slotcask_emit_checked(db,entry,callback,ctx);if(rc)break;
        }
        int saved=errno;kfcache_release(&keyfile);errno=saved;if(rc)return rc;
    }
    return 0;
}
int slotcask_lookup_by_hash_checked(SlotcaskDb *db,const uint8_t hash[16],
    SlotcaskScanCb callback,void *ctx){
    if(!db||!hash||!callback||db->num_shards<1){errno=EINVAL;return -1;}
    int shard=shard_for_hash(hash,db->num_shards);char path[PATH_MAX];
    kf_path_for(path,db->data_dir,shard);SlotcaskKfHandle keyfile;
    SlotRef *ref=db->kf_slot_refs?&db->kf_slot_refs[shard]:NULL;
    if(kfcache_acquire_direct(&keyfile,ref,path,db->slots_per_shard,db,shard))return -1;
    int rc=0;size_t start=kf_slot_for(hash,keyfile.capacity);
    for(size_t i=0;i<keyfile.capacity;i++){
        const SlotcaskKfEntry *entry=&keyfile.map[(start+i)%keyfile.capacity];
        uint8_t flag=__atomic_load_n(&entry->flag,__ATOMIC_ACQUIRE);
        if(!flag)break;
        if(flag==2)continue;
        if(flag!=1){errno=EUCLEAN;rc=-1;break;}
        if(memcmp(entry->hash,hash,16))continue;
        rc=slotcask_emit_checked(db,entry,callback,ctx);if(rc)break;
    }
    int saved=errno;kfcache_release(&keyfile);errno=saved;return rc;
}

/* Checked bulk readers preserve the existing bucket/resolve/revalidation and
 * dispatch patterns. Error status and bounds are explicit; no lock/gate or
 * successful read-handle lifetime changes. Existing public implementations
 * above remain byte-identical for consumers owned by later integrations. */
int slotcask_bulk_lookup_in_kfshard_checked(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n) {
    if(n==0)return 0;
    if(!db||!recs||n>INT_MAX||kf_shard_id<0||kf_shard_id>=db->num_shards){errno=EINVAL;return -1;}
    int failed=0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[kf_shard_id] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, kf_shard_id) != 0) return -1;

    SlotcaskBulkLookupState *st = calloc(n, sizeof(SlotcaskBulkLookupState));
    if (!st) { kfcache_release(&kh); return -1; }

    /* Phase 1: probe-only lookup under one rdlock. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX) { r->status = -1; continue; }
        compute_hash(r->key, r->klen, st[i].hash);
        uint8_t flag = 0;
        size_t slot;
        int rc = kf_lookup_no_verify(&kh, st[i].hash, &flag,
                                      &st[i].sid, &st[i].fid, &st[i].off, &slot);
        if (rc < 0) { r->status = -2; continue; }
        st[i].kf_found = 1;
    }
    /* The kf reader stays held through phase 2: the window contract
       ("Kf read handle stays live until the segment record has been
       checked against its hash/key and copied into caller-owned
       memory") requires it. Released here, a window could tombstone
       (T) and re-emit (P) this slot while phase 2 reads it under an
       independent segcache rdlock — plain-vs-plain, zero common lock.
       Same discipline as slotcask_get and kf_reval_fetch_one. */
#ifdef TEST_BUILD
    /* Regression hook (docs/plans/2026-08-28-eliminate-tsan-supp.md
       Task B1): parks the caller after the probe phase so a test can
       run a full window's worth of slot churn in the gap. Post-fix the
       kf reader is still held here, so any window T step in the gap
       blocks — which is exactly the assertion the test makes. */
    if (atomic_load(&g_shard_test_bulk_lookup_gap) &&
        atomic_fetch_add(&g_shard_test_bulk_lookup_gap_hit, 1) == 0) {
        while (!atomic_load(&g_shard_test_bulk_lookup_gap_release))
            nanosleep(&(struct timespec){0, 1000000L}, NULL);
    }
#endif

    /* Phase 2: batched verify_stored_key — sort kf-hits by (sid, fid),
       take segcache rdlock once per unique file, verify each record
       under the held handle. Records that fail verify (hash collision
       on a different stored key) get status=-2. */
    int *vidx = malloc(n * sizeof(int));
    if (vidx) {
        int vcount = 0;
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status == 0 && st[i].kf_found) vidx[vcount++] = (int)i;
        }
        for (int a = 1; a < vcount; a++) {
            int tmp = vidx[a];
            uint8_t  ta_sid = st[tmp].sid; uint16_t ta_fid = st[tmp].fid;
            int b = a - 1;
            while (b >= 0) {
                int bi = vidx[b];
                if (st[bi].sid < ta_sid ||
                    (st[bi].sid == ta_sid && st[bi].fid <= ta_fid)) break;
                vidx[b + 1] = vidx[b];
                b--;
            }
            vidx[b + 1] = tmp;
        }
        int k = 0;
        while (k < vcount) {
            int run_end = k + 1;
            uint8_t  sid = st[vidx[k]].sid;
            uint16_t fid = st[vidx[k]].fid;
            while (run_end < vcount &&
                   st[vidx[run_end]].sid == sid &&
                   st[vidx[run_end]].fid == fid)
                run_end++;
            char path[PATH_MAX];
            if(sid>=db->num_streams){failed=EILSEQ;for(int j=k;j<run_end;j++)recs[vidx[j]].status=-1;k=run_end;continue;}
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            SlotRef *seg_ref = seg_ref_for(db, (int)sid, (uint32_t)fid);
            if (segcache_acquire_direct(&h, seg_ref, path) != 0) {
                failed=errno?errno:EIO;
                for (int j = k; j < run_end; j++) recs[vidx[j]].status = -1;
                k = run_end;
                continue;
            }
            for (int j = k; j < run_end; j++) {
                int i = vidx[j];
                SlotcaskBulkRec *r = &recs[i];
                if(st[i].sid>=db->num_streams||st[i].off>h.map_size||h.map_size-st[i].off<24){r->status=-1;failed=EILSEQ;continue;}
                const uint8_t *rec = h.map + st[i].off;
                size_t physical=slotcask_record_size_varlen(seg_rec_klen(rec),seg_rec_vlen(rec));
                if(physical>(size_t)db->slot_size||physical>h.map_size-st[i].off||!seg_rec_live_with_hash(rec,st[i].hash)){
                    r->status=-1;failed=EILSEQ;continue;
                }
                if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -2; continue; }
                uint16_t k_stored = seg_rec_klen(rec);
                if (k_stored != r->klen) { r->status = -2; continue; }
                if (memcmp(rec + 24, r->key, r->klen) != 0) { r->status = -2; continue; }
                st[i].verified = 1;
            }
            segcache_release(&h);
            k = run_end;
        }
        free(vidx);
    } else {
        failed=ENOMEM;
    }

    kfcache_release(&kh);
    free(st);
    if(failed){errno=failed;return -1;}return 0;
}
static SlotcaskResolvedRec *slotcask_bulk_resolve_hashes_checked(SlotcaskDb *db,
                                                    const uint8_t (*hashes)[16],
                                                    size_t n,
                                                    size_t *out_n) {
    errno=0;
    if (n == 0 || !out_n) { if (out_n) *out_n = 0; return NULL; }

    SlotcaskResolvedRec *resolved = calloc(n, sizeof(SlotcaskResolvedRec));
    if (!resolved) { *out_n = 0; return NULL; }

    size_t resolved_n = 0;int failure=0;
    int nshards = db->num_shards;

    /* Pass 1: count hashes per KF shard */
    size_t *per_shard_count = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_count) { free(resolved); *out_n = 0; return NULL; }

    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_count[sid]++;
    }

    /* Pass 2: allocate per-shard index arrays */
    size_t **per_shard_idxs = calloc((size_t)nshards, sizeof(size_t *));
    size_t *per_shard_pos   = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_idxs || !per_shard_pos) {
        free(per_shard_count);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    int alloc_ok = 1;
    for (int s = 0; s < nshards; s++) {
        if (per_shard_count[s] > 0) {
            per_shard_idxs[s] = malloc(per_shard_count[s] * sizeof(size_t));
            if (!per_shard_idxs[s]) { alloc_ok = 0; break; }
        }
    }
    if (!alloc_ok) {
        for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(per_shard_count);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    /* Pass 3: fill per-shard index arrays */
    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_idxs[sid][per_shard_pos[sid]++] = i;
    }

    /* Pass 4: for each non-empty shard, acquire KF, probe all its hashes */
    char kf_path[PATH_MAX];
    for (int s = 0; s < nshards; s++) {
        size_t cnt = per_shard_count[s];
        if (cnt == 0) continue;

        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0){failure=errno?errno:EIO;break;}

        for (size_t j = 0; j < cnt; j++) {
            size_t hash_idx = per_shard_idxs[s][j];
            uint8_t flag = 0;
            uint8_t sid;
            uint16_t fid;
            uint32_t off;
            size_t slot;
            int rc = kf_lookup_no_verify(&kh, hashes[hash_idx], &flag,
                                          &sid, &fid, &off, &slot);
            if (rc == 0) {
                SlotcaskResolvedRec *rr = &resolved[resolved_n++];
                memcpy(rr->hash, hashes[hash_idx], 16);
                rr->sid = sid;
                rr->fid = fid;
                rr->off = off;
            }
        }
        kfcache_release(&kh);
    }

    /* Cleanup per-shard structures */
    for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
    free(per_shard_idxs);
    free(per_shard_pos);
    free(per_shard_count);

    if(failure){free(resolved);*out_n=0;errno=failure;return NULL;}
    *out_n = resolved_n;
    if (resolved_n == 0) { free(resolved); return NULL; }
    return resolved;
}
typedef struct {
    SlotcaskDb *db;SlotcaskResolvedRec *recs;size_t start,count;int kf_shard;
    SlotcaskScanCb cb;void *ctx;int error;
} CheckedRevalFetchArg;
static void checked_reval_fetch_one(CheckedRevalFetchArg *fa) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        fa->error=errno?errno:EIO;
        /* Whole partition unreadable — retire every record in it. */
        for (size_t i = 0; i < fa->count; i++)
            fa->recs[fa->start + i].sid = 0xFF;
        return;
    }

    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        uint8_t flag = 0, sid = 0;
        uint16_t fid = 0;
        uint32_t off = 0;
        size_t slot = 0;
        if (kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid, &off,
                                &slot) != 0 ||
            flag != 1 || sid != r->sid || fid != r->fid || off != r->off)
            r->sid = 0xFF;  /* repointed or gone since resolve */
    }

    /* Compact survivors within the slice (disjoint per partition). */
    size_t live_n = 0;
    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        if (r->sid != 0xFF) fa->recs[fa->start + live_n++] = *r;
    }

    if (live_n > 0) {
        qsort(&fa->recs[fa->start], live_n, sizeof(SlotcaskResolvedRec),
              compare_sid_fid_off);
        /* Copy every survivor's bytes under the STILL-HELD reader. */
        size_t run_start = 0;
        for (size_t i = 0; i < live_n; i++) {
            int last = (i == live_n - 1);
            if (!last &&
                fa->recs[fa->start + i].sid ==
                    fa->recs[fa->start + i + 1].sid &&
                fa->recs[fa->start + i].fid ==
                    fa->recs[fa->start + i + 1].fid)
                continue;
            char seg_path[PATH_MAX];
            SlotcaskSegHandle h;
            if(fa->recs[fa->start+run_start].sid>=fa->db->num_streams){fa->error=EILSEQ;break;}
            seg_path_for(seg_path, fa->db->data_dir,
                         fa->recs[fa->start + run_start].sid,
                         fa->recs[fa->start + run_start].fid);
            if (segcache_acquire(&h, seg_path, 0, 0, 0) == 0) {
                for (size_t j = run_start; j <= i; j++) {
                    const SlotcaskResolvedRec *r =
                        &fa->recs[fa->start + j];
                    if(r->sid>=fa->db->num_streams||r->off>h.map_size||h.map_size-r->off<24){fa->error=EILSEQ;break;}
                    const uint8_t *rec = h.map + r->off;
                    size_t physical=slotcask_record_size_varlen(seg_rec_klen(rec),seg_rec_vlen(rec));
                    if(physical>(size_t)fa->db->slot_size||physical>h.map_size-r->off||!seg_rec_live_with_hash(rec,r->hash)){fa->error=EILSEQ;break;}
                    uint16_t klen = seg_rec_klen(rec);
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, rec[19], fa->ctx) != 0)
                        break;
                }
                segcache_release(&h);
            }else fa->error=errno?errno:EIO;
            if(fa->error)break;
            run_start = i + 1;
        }
    }

    kfcache_release(&kh);
}
static void *checked_reval_fetch_worker(void *arg){checked_reval_fetch_one(arg);return NULL;}
int slotcask_bulk_fetch_resolved_checked(SlotcaskDb *db,
                                  SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  SlotcaskScanCb cb) {
    if (n == 0 || !cb) return 0;

    /* Partition in place by KF shard: single pass with an index-keyed
       bucket layout (avoids a comparator that needs db->num_shards). */
    int nshards = db->num_shards;
    size_t *part_start = calloc((size_t)nshards, sizeof(size_t));
    size_t *part_count = calloc((size_t)nshards, sizeof(size_t));
    if (!part_start || !part_count) {
        free(part_start); free(part_count); return -1;
    }
    for (size_t i = 0; i < n; i++)
        part_count[shard_for_hash(recs[i].hash, nshards)]++;
    size_t acc = 0;
    for (int s = 0; s < nshards; s++) {
        part_start[s] = acc;
        acc += part_count[s];
    }
    SlotcaskResolvedRec *sorted = malloc(n * sizeof(SlotcaskResolvedRec));
    size_t *fill = calloc((size_t)nshards, sizeof(size_t));
    if (!sorted || !fill) {
        free(part_start); free(part_count); free(fill); free(sorted);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        int s = shard_for_hash(recs[i].hash, nshards);
        sorted[part_start[s] + fill[s]++] = recs[i];
    }
    free(fill);

    /* Dispatch one reval+fetch pipeline per non-empty partition. */
    int nparts = 0;
    for (int s = 0; s < nshards; s++)
        if (part_count[s] > 0) nparts++;
    if (nparts > 0) {
        CheckedRevalFetchArg *fargs = calloc((size_t)nparts, sizeof(CheckedRevalFetchArg));
        if (!fargs) {
            free(part_start); free(part_count); free(sorted);
            return -1;
        }
        int fi = 0;
        for (int s = 0; s < nshards; s++) {
            if (part_count[s] == 0) continue;
            fargs[fi].db = db;
            fargs[fi].recs = sorted;
            fargs[fi].start = part_start[s];
            fargs[fi].count = part_count[s];
            fargs[fi].kf_shard = s;
            fargs[fi].cb = cb;
            fargs[fi].ctx = ctx;
            fi++;
        }
        if (nparts <= 3) {
            for (int i = 0; i < nparts; i++) checked_reval_fetch_one(&fargs[i]);
        } else {
            parallel_for_io(checked_reval_fetch_worker, fargs, nparts,
                            sizeof(CheckedRevalFetchArg));
        }
        int failure=0;for(int i=0;i<nparts;i++)if(fargs[i].error){failure=fargs[i].error;break;}
        free(fargs);
        if(failure){free(part_start);free(part_count);free(sorted);errno=failure;return -1;}
    }
    free(part_start);
    free(part_count);
    free(sorted);
    return 0;
}
int slotcask_bulk_resolve_and_fetch_checked(SlotcaskDb *db,const uint8_t (*hashes)[16],size_t n,void *ctx,SlotcaskScanCb cb){
    if(!db||(!hashes&&n)||!cb){errno=EINVAL;return -1;}
    size_t count=0;errno=0;
    SlotcaskResolvedRec *resolved=slotcask_bulk_resolve_hashes_checked(db,hashes,n,&count);
    if(!resolved){int error=errno;return error?-1:0;}
    int rc=slotcask_bulk_fetch_resolved_checked(db,resolved,count,ctx,cb);int error=errno;
    free(resolved);errno=error;return rc;
}
````````

<!-- R1-BLOCK corrupt-flag-probe -->
````````c
#define _GNU_SOURCE
#define _XOPEN_SOURCE 700
#include "types.h"
#include "slotcask.h"
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Internal checked fetch entry point: exercise corruption after resolution. */
extern int slotcask_bulk_fetch_resolved_checked(SlotcaskDb *,
    SlotcaskResolvedRec *, size_t, void *, SlotcaskScanCb);

static int count_record(const uint8_t hash[16], const void *key, size_t klen,
    const void *value, size_t vlen, uint8_t flags, void *ctx) {
    (void)hash; (void)key; (void)klen;
    (void)value; (void)vlen; (void)flags;
    (*(int *)ctx)++;
    return 0;
}

static int remove_entry(const char *path, const struct stat *st,
    int kind, struct FTW *walk) {
    (void)st; (void)walk;
    return kind == FTW_DP ? rmdir(path) : unlink(path);
}

static int check_result(const char *name, int rc, int error, int detail,
    int expected_rc, int expected_error, int expected_detail) {
    int failed = rc != expected_rc || error != expected_error ||
                 detail != expected_detail;
    printf("%s: rc=%d errno=%d detail=%d %s\n", name, rc, error,
           detail, failed ? "FAIL" : "PASS");
    return failed;
}

static int write_flag(int fd, off_t entry_offset, uint8_t flag) {
    off_t offset = entry_offset + (off_t)offsetof(SlotcaskKfEntry, flag);
    if (pwrite(fd, &flag, sizeof(flag), offset) != sizeof(flag)) {
        perror("pwrite keyfile flag");
        return -1;
    }
    return 0;
}

static int find_live_entry(int fd, SlotcaskKfEntry *entry, off_t *offset) {
    struct stat st;
    if (fstat(fd, &st)) {
        perror("fstat keyfile");
        return -1;
    }
    if (st.st_size < SLOTCASK_KF_HDR_SIZE ||
        (st.st_size - SLOTCASK_KF_HDR_SIZE) % sizeof(*entry)) {
        fprintf(stderr, "invalid keyfile size\n");
        return -1;
    }
    for (off_t at = SLOTCASK_KF_HDR_SIZE;
         at <= st.st_size - (off_t)sizeof(*entry); at += sizeof(*entry)) {
        if (pread(fd, entry, sizeof(*entry), at) != sizeof(*entry)) {
            perror("pread keyfile entry");
            return -1;
        }
        if (entry->flag == 1) {
            *offset = at;
            return 0;
        }
    }
    fprintf(stderr, "seeded live entry not found\n");
    return -1;
}

int main(void) {
    char root[] = "/tmp/r1-corrupt-flag-XXXXXX";
    SlotcaskDb db = {0};
    int initialized = 0, opened = 0, fd = -1, failures = 0;
    if (!mkdtemp(root)) {
        perror("mkdtemp");
        return 2;
    }
    shard_db_offline_init(root);
    slotcask_init(64, 64);
    initialized = 1;
    if (slotcask_open(&db, root, 1, 1, 128)) {
        perror("slotcask_open");
        failures++;
        goto cleanup;
    }
    opened = 1;
    SlotcaskUpsertOpts options = {0};
    if (slotcask_upsert_with_hooks(&db, -1, "a", 1, "hi", 2,
                                  &options, NULL)) {
        perror("slotcask_upsert_with_hooks");
        failures++;
        goto cleanup;
    }
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s/data/kf/000.kf", root);
    if (length < 0 || (size_t)length >= sizeof(path)) {
        fprintf(stderr, "keyfile path overflow\n");
        failures++;
        goto cleanup;
    }
    fd = open(path, O_RDWR);
    if (fd < 0) {
        perror("open keyfile");
        failures++;
        goto cleanup;
    }
    SlotcaskKfEntry entry;
    off_t entry_offset;
    if (find_live_entry(fd, &entry, &entry_offset)) {
        failures++;
        goto cleanup;
    }
    SlotcaskResolvedRec resolved = {
        .sid = entry.stream_id, .fid = entry.file_id, .off = entry.offset
    };
    memcpy(resolved.hash, entry.hash, sizeof(resolved.hash));
    uint8_t hashes[1][16];
    compute_hash_raw("a", 1, hashes[0]);
    if (memcmp(hashes[0], entry.hash, sizeof(entry.hash))) {
        fprintf(stderr, "live entry does not match seeded key\n");
        failures++;
        goto cleanup;
    }
    if (write_flag(fd, entry_offset, 3)) {
        failures++;
        goto cleanup;
    }

    int calls = 0;
    errno = 0;
    int rc = slotcask_lookup_by_hash_checked(&db, hashes[0],
                                            count_record, &calls);
    failures += check_result("single corrupt", rc, errno, calls,
                             -1, EUCLEAN, 0);
    SlotcaskBulkRec record = {.key = "a", .klen = 1};
    errno = 0;
    rc = slotcask_bulk_lookup_in_kfshard_checked(&db, 0, &record, 1);
    failures += check_result("exists corrupt", rc, errno, record.status,
                             -1, EUCLEAN, -1);
    calls = 0;
    errno = 0;
    rc = slotcask_bulk_resolve_and_fetch_checked(&db, hashes, 1,
                                                 &calls, count_record);
    failures += check_result("get corrupt", rc, errno, calls,
                             -1, EUCLEAN, 0);
    calls = 0;
    errno = 0;
    rc = slotcask_bulk_fetch_resolved_checked(&db, &resolved, 1,
                                              &calls, count_record);
    failures += check_result("revalidation corrupt", rc, errno, calls,
                             -1, EUCLEAN, 0);

    if (write_flag(fd, entry_offset, 2)) {
        failures++;
        goto cleanup;
    }
    record = (SlotcaskBulkRec){.key = "a", .klen = 1};
    errno = 0;
    rc = slotcask_bulk_lookup_in_kfshard_checked(&db, 0, &record, 1);
    failures += check_result("exists tombstone", rc, errno, record.status,
                             0, 0, -2);
    calls = 0;
    errno = 0;
    rc = slotcask_bulk_fetch_resolved_checked(&db, &resolved, 1,
                                              &calls, count_record);
    failures += check_result("revalidation tombstone", rc, errno, calls,
                             0, 0, 0);

cleanup:
    if (fd >= 0 && close(fd)) {
        perror("close keyfile");
        failures++;
    }
    if (opened) slotcask_close(&db);
    if (initialized) slotcask_shutdown();
    if (nftw(root, remove_entry, 16, FTW_DEPTH | FTW_PHYS)) {
        perror("remove temporary fixture");
        failures++;
    }
    printf("failures=%d\n", failures);
    return failures ? 1 : 0;
}
````````

<!-- R1-BLOCK export-tenant-final -->
````````c
static void describe_one_tenant(const char *str, size_t len, void *vctx) {
    TenantCtx *tc = (TenantCtx *)vctx;
    if(tc->exp->failed)return;
    if(len>=256){tc->exp->failed=1;return;}
    DescribeCtx dc;
    dc.port = tc->port;
    dc.exp  = tc->exp;
    memcpy(dc.dir, str, len); dc.dir[len] = '\0';

    char req[512];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dc.dir);
    if(rl<=0||rl>=(int)sizeof(req)){tc->exp->failed=1;return;}

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(tc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: list-objects %s failed; skipping tenant\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: list-objects %s returned malformed JSON\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    const char *ov; size_t ol;
    const char *error_value=NULL;size_t error_length=0;
    if(json_obj_get(&o,"error",&error_value,&error_length)){
        tc->exp->failed=1;free(resp);return;
    }
    if (json_obj_get(&o, "objects", &ov, &ol)) {
        char *objs_buf = malloc(ol + 1);
        if(!objs_buf){tc->exp->failed=1;free(resp);return;}
        memcpy(objs_buf, ov, ol); objs_buf[ol] = '\0';
        if(walk_string_array(objs_buf,describe_one_object,&dc))tc->exp->failed=1;
        free(objs_buf);
    }else tc->exp->failed=1;
    free(resp);
}
````````

<!-- R1-BLOCK export-tenant-baseline -->
````````c
static void describe_one_tenant(const char *str, size_t len, void *vctx) {
    TenantCtx *tc = (TenantCtx *)vctx;
    if(tc->exp->failed)return;
    if(len>=256){tc->exp->failed=1;return;}
    DescribeCtx dc;
    dc.port = tc->port;
    dc.exp  = tc->exp;
    memcpy(dc.dir, str, len); dc.dir[len] = '\0';

    char req[512];
    int rl = snprintf(req, sizeof(req),
        "{\"mode\":\"list-objects\",\"dir\":\"%s\"}", dc.dir);
    if(rl<=0||rl>=(int)sizeof(req)){tc->exp->failed=1;return;}

    char *resp = NULL; size_t resp_len = 0;
    if (query_collect(tc->port, req, (size_t)rl, &resp, &resp_len) != 0 || !resp) {
        fprintf(stderr, "warn: list-objects %s failed; skipping tenant\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    JsonObj o;
    if (json_parse_object(resp, resp_len, &o) <= 0) {
        fprintf(stderr, "warn: list-objects %s returned malformed JSON\n", dc.dir);
        free(resp);
        tc->exp->failed=1;return;
    }
    const char *ov; size_t ol;
    if (json_obj_get(&o, "objects", &ov, &ol)) {
        char *objs_buf = malloc(ol + 1);
        if(!objs_buf){tc->exp->failed=1;free(resp);return;}
        memcpy(objs_buf, ov, ol); objs_buf[ol] = '\0';
        if(walk_string_array(objs_buf,describe_one_object,&dc))tc->exp->failed=1;
        free(objs_buf);
    }
    free(resp);
}
````````
