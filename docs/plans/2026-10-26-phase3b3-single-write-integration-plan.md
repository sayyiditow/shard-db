# Phase 3B.3 — single-command owning write integration

## Primary goal

Make single insert/upsert and partial update carry exact decimal and large-varchar
values through the real command implementations using actual record sizes and
locked OLD state, while existing applications continue working. Packed objects
remain publicly inactive; this child closes single-write integration, not the
finance application blocker or all of Phase 3B.

**Status:** proposed for review and approval; production is not implemented.
**Base:** `82e5d4053431b0b163a621712772d20727d4ff28`, branch
`fix/phase1-safe-scalar-conversions` (reviewed, committed Phase 3B.2).
**Planner:** GPT-6.1. Executor identity must be confirmed for this child at handoff.

**Revision 2 — blind review (2026-10-26):** repaired B1, the shared buffered
legacy decoder's unchecked outer allocation, varchar fallback/omission and
undersized enum rendering. This is required for the proposed precommit response
guarantee. Complete config.c replacement, long-enum regression, explicit decoder
failure RED and config-inclusive legacy/packed allocation probes are included.
Production source remains unchanged.

## Scope, mechanisms and decisions proposed for approval

The 3B.1 row adapter and 3B.2 owning callback are implemented but have no production
command consumers at this checkpoint. Single insert mallocs total_size, always
writes generation 1 and transfers auto_create using a get snapshot before the
write lock. Single update uses the old callback, whose transport allocates
slot_size minus overhead before composition. Both response paths assume fixed
sizes and may silently omit a requested record on allocation failure. A packed
schema can have a roughly 1 GiB theoretical slot bound with a roughly 2 KiB row;
those assumptions cannot be activated safely.

Proposed changes, all included below as complete code and unique anchored edits:

- Use owning callbacks for both single insert/upsert and update, including the
  existing legacy routes. Keep all legacy numeric, computed, CAS and index
  semantics; producers trim their own final legacy bytes.
- Add a zero-default internal opt-in for invoking an owning callback on a fresh
  key with NULL OLD. Default owning and old-buffer contracts remain unchanged;
  the new flag without an owning callback is EINVAL. Built-in conditions run
  before the callback. This is a source-level internal struct extension, not an
  SDK/wire/persisted-layout change.
- Prepare packed insert values against locked OLD. Ordinary omitted defaults
  run once; defer auto_create until copying OLD bytes/nullness or stamping a
  fresh key, then validate final NOT NULL. This also avoids validating a NULL
  placeholder for a NOT NULL variable-width auto_create field.
- Add a packed scalar-JSON patch collector that borrows OLD and preserves
  omitted fields. Validate complete JSON and every known duplicate; last valid
  duplicate wins. Decode escapes once, including embedded NUL byte spans.
  Stamp auto_update after a partial patch. Insert auto_update keeps the existing
  adapter omitted-default policy; any broader change belongs to Phase 4.
- Reject packed indexed/conditional paths before generators or writes, and
  reject known object/array patches, including computed expressions, before
  composition. These are private inactive-branch guard rails, not new refusals
  for existing public legacy applications. Phases 4–5 own their completion.
- Validate OLD generation/schema compatibility in both directions before field
  access. Dry-run validates generation and performs no mutation.
- Check aligned actual physical size against slot and segment bounds. Preserve
  the 128 MiB production segment ceiling and 4 MiB bare-varchar content limit.
  No schema-maximum scratch allocation in either packed single writer.
- Repair typed_decode's outer/field allocation handling and rendered-field
  sizing: no NULL dereference, fallback omission or fixed-size enum truncation.
  Negative render and insufficient-output errors free buffers and return NULL.
  Preserve existing legacy NULL/absence and invalid-enum-index behavior.
- Pre-render return_record JSON from final actual NEW before reservation. A
  render/allocation failure rejects rather than committing then omitting the
  requested record. Existing durability-pending responses remain authoritative.

No new compatibility stamp, schema slot, record format, loader binding, migration,
public configuration switch, default-schema activation or version bump is added.
No route changes to bulk, criteria/index codecs, aggregates, readers,
maintenance or calendar/bool/UUID semantics. One shared buffered-decoder repair
is included because single return_record now relies on its failure contract;
existing buffered readers also receive that repair. Stream decoding and public
packed read integration remain separate 3B work. Remaining fixed offsets in legacy-only index and
computed helpers are guarded from packed use, not claimed converted.

## Execution rules

1. Approval of this written child is required before production execution.
   Continue the existing roadmap branch at the exact base; do not branch from
   main, reset, rebase or discard planning artifacts. This is the explicit
   roadmap continuation exception to the generic fresh-branch template.
2. Execute Tasks 0–3 in order. Use Appendix A's driver; copy named blocks
   verbatim, never retype or infer edits. All implementation anchors are quoted
   blocks. Validate all anchors and prior-state hashes before any stage writes.
3. If any anchor/hash/checkpoint fails, create PLAN_NOTES.md with actual evidence
   and halt the entire run. Do not guess or continue unrelated tasks. Human or
   planner must amend the plan before resumption. An uncovered decision also
   requires a halt, not an executor-selected workaround.
4. Capture runtime RED from stage 1 before applying stage 2. Do not weaken,
   skip, delete or loosen tests. A build/link failure is not runtime RED.
5. Prefix every shell command with rtk (rtk proxy for unfiltered logs). Build
   with SKIP_TESTS=1 ./build.sh. Test with the built C runner, not build.sh's
   implicit tests. Preserve raw output in a unique /tmp directory.
6. Full-suite daemon tests need a socket-capable environment. Request the normal
   local socket execution permission up front if the sandbox blocks it; do not
   count EPERM failures or turn off daemon cases. The baseline suite is 520 cases.
7. No sanitizer or benchmark runs here. Sanitizers remain the end-of-branch,
   immediately-before-push gate in AGENTS.md. Never run user benchmarks.
8. Leave implementation unstaged/uncommitted for plan-aware and independent
   blind raw-diff review. No add/commit/push/PR/merge. Source stubs disappear in
   stage 2; no logs, probes, scratch scripts, binaries or debug/cache files in Git.
9. The separate protected certification plan is hash-only: do not read, edit,
   move, stage, incorporate or execute its contents. Expected SHA256:
   fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
10. Preserve all pre-existing planning documents. Only stage-owned source/docs
    and the new concise execution evidence ledger are execution deliverables.
    Amendments to this plan require planner review; execution never self-resumes.

## Task 0 — documentation, contracts and checkpoint

**How this serves the primary goal:** keeps the real application paths and
already-reviewed storage ownership contract authoritative.

Read AGENTS.md, CORE-PROCESS.md and these sources. Paths/locations are discovery
references; executable edits use the complete quoted anchors in Appendix A.

| Source | Allowed API/pattern and constraint |
|---|---|
| docs/concepts/typed-row-adapter.md; src/db/typed_row.h/.c | Explicit immutable packed schema, TypedPrepared ownership, actual-sized composition, borrowed OLD spans, bounded byte decoding |
| docs/concepts/owning-write-hooks.md; slotcask.h | Callback returns free-compatible owned storage, engine frees all returned allocations on every path; no aliasing OLD/input |
| slotcask.c: upsert_adapter_value_compute, both *_with_hooks wrappers | Conditions before construction, final bytes alive through transaction, hard error vs CAS distinction, physical bounds |
| storage.c: cmd_insert_v2 / cmd_update_v2 and their contexts/hooks | Existing index staging, marker/recovery sequence, pending outcome, wire error escaping and public wrappers |
| config.c: typed_decode, typed_encode_defaults, generate_default, schema_trim_fn | Legacy defaults and fixed-tail trimming; never apply trim to packed descriptors |
| typed_row.c: prepare_json, prepared_valid, typed_prepared_old | Complete JSON validation, duplicate/null/default semantics, validate final automatic values before NOT NULL |
| docs/query-protocol/cas.md | Existing single update, computed, condition, return_record and dry-run behavior |
| src/test/cases/test_owning_write_hooks.c; test_typed_row_adapter.c | Private schema + Slotcask fixtures, exact return bytes, isolated mkdtemp lifetimes |
| src/test/cases/test_composite_in_multishard_duplicate.c | Raw worker threads must bind parent __thread g_db before database access |
| types.h: g_out; existing open_memstream capture tests | Thread-local response capture; no process-wide stdout redirection |
| embedded.c: shard_db_open_internal; shard_db_close | Scratch probe's instance lifecycle; external signature matches source definition |

### Consumer inventory before changing signatures/structures

- SlotcaskUpsertOpts is internal and zero/designated initialized across single,
  bulk transport adapters, maintenance and tests. New option defaults to zero.
  Only single insert and its focused transport test set it. Both single-wrapper
  validators and the shared value_compute dispatcher change. Existing 3B.2
  missing-OLD tests remain unchanged. No external SDK exposes this options type.
- typed_decode retains its signature and owned-string/NULL return contract.
  Existing direct consumers: storage.c cmd_insert_v2, cmd_update_v2,
  cmd_delete_v2, multi_get_fetch_cb; query_find.c decode_value and v2_fetch_cb;
  types.h retains its declaration. New single_write_decode is the single-write
  caller. Existing tests and the new legacy regression call it directly.
  This child repairs buffered allocation/render failure and enum sizing; it
  does not convert or claim to repair typed_decode_stream.
- Existing slotcask_new_from_old_alloc_fn signature is unchanged. New-mode NULL
  OLD is explicit opt-in, not a silent callback-contract widening.
- Private prepare_json gains an OLD argument. Its two existing callers
  typed_encode_packed_into and typed_encode_alloc pass NULL; new insertion and
  scalar-patch collectors account for the other two calls. No external caller.
- typed_prepared_defaults signature stays unchanged; a private mode helper adds
  insertion-only auto_create deferral. Existing users retain their semantics.
- New typed_insert_alloc is packed-only and called only by v2_insert_owned.
  New typed_update_json_alloc is packed-only and called only by v2_update_owned.
  Their declarations are in typed_row.h, not the embedded public SDK header.
- cmd_insert_v2 and cmd_update_v2 retain their existing public-wrapper callers
  (cmd_insert/cmd_update). Their new private *_impl receives an explicit typed
  schema and optional borrowed SlotcaskDb. The public wrapper loads normally;
  TEST_BUILD wrappers borrow private fixtures. Registry cleanup applies only to
  an acquired registry reference, never the borrowed test DB.
- v2_update_new_from_old becomes a legacy-only bounded producer called by the
  new owning constructor; its previous options assignment is removed. Existing
  three bulk update_compose consumers and typed_update_alloc remain unchanged.
- V2InsertCtx adds incoming-buffer/response ownership, actual key length and DB.
  V2UpdateCtx replaces ret_buf with ret_json plus actual key length/DB. Every
  condition, hard-error, pending and success exit releases the right ownership.
- Index prepare/apply and bitmap staging retain their legacy assumptions and
  ordering. Packed index presence is rejected before entering them. Legacy
  record_hdr2 remains 1; packed single writers supply typed_schema_hdr2(ts)=3.
- TEST_BUILD-only single_write_test.h has two borrowed-dependency entry points.
  Their only callers are the new test. No loader/cache mutation, environment
  flag, SDK entry point or daemon mode exposes private schemas.
- Scratch probe includes actual storage.c unchanged to call the private impl;
  separately compiles typed_row.c and config.c without LTO so allocator
  substitutions cannot disappear. It introduces no committed allocation-failure seam.

Confirm read-only identity and state, record actual output:

~~~sh
rtk proxy git rev-parse HEAD
rtk proxy git branch --show-current
rtk git status --short
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
rtk proxy sha256sum docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md
~~~

Expected pre-existing planning changes are this proposed plan and the Phase 3
index link, plus the untracked protected rig. Halt on other source changes.
There is no new investigation task for the executor: the mechanisms and actual
APIs above were inspected while preparing this plan.

## Task 1 — stage the regression and capture RED

**How this serves the primary goal:** proves the new command routes and fresh
owning construction are absent before the implementation is applied.

Appendix A's stage 1 adds the new options member/header contract, complete private
test-header declarations, TEST_BUILD-only unavailable-route stubs, focused case,
and one registration line. Existing production command code is unchanged. Stubs
are test-first scaffolding, not an implementation or new public route.

Bootstrap the supplied driver and probe into a unique scratch directory. Keep
that directory in P3B3_TMP for the remaining commands; do not write either file
inside the repository.

~~~sh
P3B3_TMP=$(rtk proxy mktemp -d /tmp/shard-p3b3-exec-XXXXXX)
rtk proxy python3 - docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md "$P3B3_TMP" <<'PY'
from pathlib import Path
import re,sys
text=Path(sys.argv[1]).read_text()
blocks=dict(re.findall(r'(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)',text))
out=Path(sys.argv[2]);(out/'apply.py').write_text(blocks['driver'])
(out/'probe.c').write_text(blocks['probe'])
PY
rtk proxy python3 "$P3B3_TMP/apply.py" docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md 1
rtk proxy env SKIP_TESTS=1 ./build.sh > "$P3B3_TMP/red-build.log" 2>&1
rtk proxy ./build/bin/shard-db-test run test-single-write-integration > "$P3B3_TMP/red.log" 2>&1
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -Isrc/db -Dmalloc=probe_malloc -Dcalloc=probe_calloc -Drealloc=probe_realloc -c src/db/config.c -o "$P3B3_TMP/config-red.o"
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -DP3B3_RENDER_ONLY -Isrc/db -c "$P3B3_TMP/probe.c" -o "$P3B3_TMP/render-red.o"
rtk proxy gcc -O1 -g "$P3B3_TMP/render-red.o" "$P3B3_TMP/config-red.o" build/bin/libshard-db.a -Wl,--wrap=free -pthread -lssl -lcrypto -lm -o "$P3B3_TMP/render-red"
rtk proxy "$P3B3_TMP/render-red" > "$P3B3_TMP/render-red.log" 2>&1
~~~

The two RED invocations deliberately return nonzero: record both and continue
only if their named failures match the expected defects. The decoder probe
forks before injecting each allocation failure, with core dumps disabled. Its
parent reports the outer-allocation crash and incomplete inner-buffer response
as test failures; it must not itself crash or abort compilation. Context is
initialized before fork so logger/cache TLS is valid. The direct long-enum
assertion also fails on unchanged decoder code, independently of route stubs.

Build must succeed with zero new warnings. The focused runtime must fail for
missing fresh owning support and unavailable explicit-schema single routes.
Appendix C records measured RED. Inspect named failures, not only exit status:
transport fresh callback/invalid-option checks and successful insertion/update
assertions must be RED. Rejection-only checks may pass incidentally while the
route is unavailable; do not claim they prove rejection preservation yet.
Paste actual totals and relevant failure lines into the evidence ledger. Stop
if RED is green or fails to compile rather than failing the intended assertions.

## Task 2 — integrate the owning single-command paths

**How this serves the primary goal:** carries complete exact-decimal and varchar
rows through the real write/response transaction without maximum-sized scratch.

Apply stage 2's complete code blocks only after Task 1 RED is recorded. The
stage replaces both stubs with calls into the same production implementations.
Its internal order is transport → JSON collectors → command ownership →
checked buffered legacy decoding. No
transitional build is claimed between dependent replacements within this stage;
the driver validates the full prior checkpoint and all anchors before writing.

Ownership and failure invariants:

1. Command owns unconsumed legacy insertion input; callback steals it or returns
   a separate packed buffer. Engine owns every returned buffer, including partial
   failure, and frees it once after synchronous transaction hooks.
2. Only command owns response JSON. Render it before reservation, discard on any
   rejection/pending outcome, emit it only after confirmed successful commit.
3. OLD is borrowed only during synchronous callback/hooks. No saved pointer is
   dereferenced after that lifetime. Private schemas/DBs survive calls/joins.
4. Validate OLD generation before offsets or descriptors. Reject over-slot or
   over-segment actual physical size before mutation. New flag alone is EINVAL.
5. Preserve existing legacy escaping-failure seam and fallback message. Insert
   errors escape raw client-derived text once; never emit unescaped enc_err.
6. The buffered legacy decoder checks outer and temporary allocations and
   returns NULL on negative/truncated rendering. Capacities account for actual
   varchar bytes, selected enum string and worst-case JSON escaping. Never
   substitute the old 512-byte fallback on heap failure. Legacy zero skips and
   corrupt enum indexes retain their established rendering semantics.
7. Keep the existing after_old pause hook specific to updates. Do not fire it
   from insert: existing lock-order fixtures install it before inserting data.
8. Every worker binds its inherited ShardDb via thread-local g_db, captures g_out
   separately, and joins before fixture teardown. No sleeps or timing assertions.

~~~sh
rtk proxy python3 "$P3B3_TMP/apply.py" docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md 2
rtk proxy env SKIP_TESTS=1 ./build.sh > "$P3B3_TMP/green-build.log" 2>&1
rtk proxy ./build/bin/shard-db-test run test-single-write-integration > "$P3B3_TMP/green.log" 2>&1
rtk proxy ./build/bin/shard-db-test run test-bt-kf-inversion-stream-find > "$P3B3_TMP/hook-regression.log" 2>&1
~~~

Build has zero warnings; focused case and existing hook regression must pass.
Coverage includes the exact reported numeric value, bare exponent/deep fractional
values, defaults, duplicate/NULL/escape behavior, full 4 MiB content, tiny actual
rows at the 256-varchar ceiling, shrink/grow, generation mismatches both ways,
computed/CAS/index guard rails, require-existing/duplicate/dry-run behavior,
reopen, and concurrent disjoint patches. Rejections compare OLD, counters and
segment position. Do not infer public packed end-to-end acceptance from these
explicit-schema fixtures; it remains an activation gate.

## Task 3 — full verification, allocator probe, documentation and hand-back

**How this serves the primary goal:** proves existing applications still pass and
actual-size/failure ownership survives the integrated paths.

~~~sh
rtk proxy ./build/bin/shard-db-test run-all > "$P3B3_TMP/full.log" 2>&1
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -Isrc/db -Dmalloc=probe_malloc -Dcalloc=probe_calloc -Drealloc=probe_realloc -c src/db/typed_row.c -o "$P3B3_TMP/row-probe.o"
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -Isrc/db -c "$P3B3_TMP/probe.c" -o "$P3B3_TMP/storage-probe.o"
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Wno-unused-parameter -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result -Isrc/db -Dmalloc=probe_malloc -Dcalloc=probe_calloc -Drealloc=probe_realloc -c src/db/config.c -o "$P3B3_TMP/config-probe.o"
rtk proxy gcc -O1 -g "$P3B3_TMP/storage-probe.o" "$P3B3_TMP/row-probe.o" "$P3B3_TMP/config-probe.o" build/bin/libshard-db.a -Wl,--wrap=free -pthread -lssl -lcrypto -lm -o "$P3B3_TMP/probe"
rtk proxy "$P3B3_TMP/probe" > "$P3B3_TMP/probe.log" 2>&1
rtk proxy python3 "$P3B3_TMP/apply.py" docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md 3
rtk git diff --check
rtk git status --short
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
~~~

Important: non-LTO applies to the three probe compilation steps; do not pass
-fno-lto to the final link, because the normal static library contains LTO
objects. The probe instruments allocation sites in actual storage/row/config source,
including both outer and field response allocations, and tracks corresponding
frees. It tests fresh insert, replacement and partial update for both legacy
and packed layouts, plus maximum-schema packed records. It is not
a universal process allocator oracle: allocations in unchanged dependencies,
libc and the storage coordinator are outside the failure-injection count.
Existing 3A/3B.1/3B.2 probes cover those layers' owned contracts; do not relabel
this probe as complete database OOM coverage.

Required probe results: all selected allocation failures preserve exact OLD or
key absence, live/deleted counts, segment position and zero tracked live buffers;
maximum-schema insertion, replacement and update request no selected allocation
over 8 MiB. Inject once at every selected allocation ordinal, not a hand-picked
one. Run ordinary builds only; no sanitizer/benchmark substitute.

Create docs/plans/2026-10-26-phase3b3-single-write-evidence.md with base/branch,
plan and protected hashes, exact command/exit/transcript summaries for RED,
GREEN, existing hook case, full suite, probe, inventory and any deviations.
Raw logs/probes remain under P3B3_TMP. Evidence is a concise durable ledger,
not copied debug dumps or a new debugging framework.

Confirm census: public loader/create-object has no new binder call; only the new
insert constructor and focused transport case set fresh-owning opt-in; single
update no longer assigns the old buffer callback; index/CAS packed guard rails
precede defaults/writes. Exact final source hashes must match checkpoint 3.
No source code changes after the full suite/probe without appropriate reruns.

Then stop uncommitted/unstaged for raw-diff and blind review. Do not begin the
next bulk/read child or claim Phase 3B closed.

## Acceptance and remaining owners

| Requirement | Evidence in this child | Still required later |
|---|---|---|
| Exact decimals and large varchar in single writes | Real command impl, private schema, physical NEW/response/reopen | Public loader/SDK/daemon acceptance at activation |
| Locked OLD and ownership | Callback ordering, existing lock regression, concurrent patches, allocator probe | Bulk routes and wider branch sanitizer gate |
| Defaults/null/auto fields | Scalar collection, NOT NULL AC, rejection preservation | Full Phase 4 policy and exact computed completion |
| Small records at maximum schema | 256-varchar actual size plus allocation ceiling | Bulk arenas, other readers, segment-policy public request tests |
| Conditions/indexes | Legacy full-suite parity; packed private fail-closed guards | Packed criteria/index semantics Phase 5 |
| Durable writes | Existing coordinator/hooks/marker order retained; ordinary reopen | Forced pending marker replay/publication acceptance in 3C |
| Scope isolation | No metadata binding, migration or activation | Remaining 3B consumers, 3C, Phases 4–9 |

## Appendix C — planning verification

Revision 2 was validated using ordinary compilation and runtime tests in scratch
archives only. Production source remains unchanged. No sanitizers or benchmarks.

| Gate | Actual Revision 2 result |
|---|---|
| Stage-1 normal build | exit 0, zero warnings |
| Focused runtime RED | 87 passed / 32 failed, exit 32 |
| Dedicated decoder RED | 0 passed / 2 failed, parent exit 1 |
| Stage-2 normal build | exit 0, zero warnings |
| Focused GREEN | 240 passed / 0 failed, exit 0 |
| Final normal full suite | 19465 passed / 0 failed across 521 cases, exit 0 |
| Decoder OOM GREEN | 2 passed / 0 failed; NULL/ENOMEM, tracked live=0 |
| Legacy write OOM injections | 3 fresh + 3 replacement + 3 partial-update failures pass |
| Packed write OOM injections | 10 fresh + 9 replacement + 5 partial-update failures pass |
| Maximum-schema probe | all three modes: largest selected allocation 2968 bytes, tracked live=0 |
| Exact document replay | stages 1, 2, 3 pass every block/anchor/file hash |

The 33 write allocation failures preserve exact OLD or absence, counts, segment
position and zero tracked live allocations. Legacy requests use a 1000-byte
varchar so field-buffer allocation failure is exercised through each real
single-command path as well as the direct decoder checks. Allocations in
unchanged dependencies are not part of this selected-source injection count.

### Blind finding B1 (P1), resolved

The proposed precommit response guarantee relied on unchanged typed_decode.
Its outer malloc was unchecked; failed varchar allocation fell back to512 and
could omit the field; enum temporary/output estimates could omit a valid long
enum even without OOM. This was missed by Revision 1's packed-only allocation
matrix and is a real gap, not an acceptable legacy behavior to preserve.

RED against unchanged config.c confirmed:
- Dedicated outer decoder allocation failure: child signal11, reported as test
  failure by its parent (not a passing decoder result).
- Field allocation failure: non-NULL incomplete response, child exit2; probe
  parent exits1 with both checks failing.
- The committed long-enum regression fails direct complete-output assertions
  against the unchanged decoder, independently of unavailable route stubs.

The complete replacement sizes varchar/enum output from actual values, checks
outer/field allocations and all negative/truncated rendering, and frees owned
buffers before returning an error. Existing NULL/unflagged-zero/corrupt-index
semantics remain. The extended probe instruments config.c as well as storage.c
and typed_row.c. Both independent reviewers rechecked the fix: no additional
must-fix finding remains. Post-execution raw-diff/blind review is still required.

### Replay and artifact identity

A fresh archive replay of every named edit is at
/tmp/p3b3-r2-audit-rlq0e06b. All executable files match byte for byte the source
built and tested at /tmp/p3b3-r2-x5x53r2f. The latter captured stage-1 RED before
stage-2 application; the audit archive separately verified the literal document
driver and stage-3 documentation. This does not claim redundant full-suite runs
in every archive. Temporary paths are evidence references, never execution
prerequisites; an executor creates fresh scratch logs/probes from the document.

Actual raw evidence, outside version control:
- /tmp/p3b3-r2-red-build.log and /tmp/p3b3-r2-red.log.
- /tmp/p3b3-render-only-red.log (the final context-initialized RED probe).
- /tmp/p3b3-r2-green-build.log and /tmp/p3b3-r2-green.log.
- /tmp/p3b3-r2-probe.log and /tmp/p3b3-r2-full.log.

Revision 1's 225 focused /19450 full-suite results are superseded by this table.
Its earlier planning corrections remain in the candidate: insert must not consume
the existing update-only pause hook, and raw test workers must bind the parent's
thread-local g_db. The unchanged existing lock-order case passed20/0 after the
hook correction and remains covered by the passing final full suite.

Protected certification hash matched at start and finish, contents never read.
Production inventory is only this plan and its five-line Phase 3 index link,
plus the pre-existing protected untracked document. No production code edits,
production builds, staging, commits, pushes or other Git writes.


## Appendix A — extraction driver and manifest

The code blocks below are the complete executable material. The driver reads
only named blocks, validates SHA256 and all old/new file checkpoints, and applies
each stage atomically with respect to validation (not filesystem crash atomicity).
A filesystem write error still halts; do not retry an uncertain partial stage.

<!-- block:edit-01-old -->
```text
    void                         *new_from_old_alloc_ctx;
```

<!-- block:edit-01-new -->
```text
    void                         *new_from_old_alloc_ctx;
    /* Allow this owning callback on a fresh key with old == NULL. Built-in
       if_not_exists/require_existing/check gates still run first. Invalid
       without new_from_old_alloc. Zero preserves the OLD-required contract. */
    int                           new_from_old_alloc_on_insert;
```

<!-- block:edit-02-old -->
```text
   Missing OLD and condition rejections do not invoke the constructor.
```

<!-- block:edit-02-new -->
```text
   Missing OLD does not invoke the constructor unless the explicit
   new_from_old_alloc_on_insert option is set. Conditions always run first.
```

<!-- block:edit-03-new -->
```text
#ifndef SHARD_SINGLE_WRITE_TEST_H
#define SHARD_SINGLE_WRITE_TEST_H
#ifdef TEST_BUILD
#include "types.h"
#include "slotcask.h"
/* Caller owns immutable schema/db until this synchronous call returns.
   No loader, registry publication, schema-cache mutation or activation. */
int shard_test_single_insert(const char *root, const char *object,
    const char *key, size_t klen, const char *json, const char *condition,
    int only_new, int return_record, const Schema *sc, TypedSchema *ts,
    SlotcaskDb *db);
int shard_test_single_update(const char *root, const char *object,
    const char *key, size_t klen, const char *json, const char *condition,
    int dry_run, int return_record, const Schema *sc, TypedSchema *ts,
    SlotcaskDb *db);
#endif
#endif

```

<!-- block:edit-04-old -->
```text
ypes
       (extra `fs` FieldSchema field, different result type) so it can't
       share the multi_bucket_dispatch helper used by exists/not_exists;
       clean up workers explicitly. */
    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

```

<!-- block:edit-04-new -->
```text
ypes
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
    (void)root;(void)object;(void)key;(void)klen;(void)json;(void)condition;
    (void)only_new;(void)return_record;(void)sc;(void)ts;(void)db;
    OUT("{\"error\":\"explicit schema route unavailable\"}\n");return 1;
}
int shard_test_single_update(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int dry_run,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
    (void)root;(void)object;(void)key;(void)klen;(void)json;(void)condition;
    (void)dry_run;(void)return_record;(void)sc;(void)ts;(void)db;
    OUT("{\"error\":\"explicit schema route unavailable\"}\n");return 1;
}
#endif

```

<!-- block:edit-05-new -->
```text
#define _XOPEN_SOURCE 700
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include "single_write_test.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <pthread.h>

typedef struct {TypedSchema *ts;Schema sc;SlotcaskDb db;char root[80];int opened;} WFixture;
static int w_remove(const char *p,const struct stat *s,int k,struct FTW *f){
    (void)s;(void)k;(void)f;return remove(p);
}
static void w_close(WFixture *f){
    if(f->opened)slotcask_close(&f->db);
    invalidate_idx_cache(f->root,"obj");
    if(f->ts){for(int i=0;i<f->ts->nfields;i++)free_enum_values(&f->ts->fields[i]);free(f->ts);}
    slotcask_shutdown();
    if(f->root[0])ASSERT_EQ_INT(nftw(f->root,w_remove,16,FTW_DEPTH|FTW_PHYS),0,"fixture removed");
}
static int w_open(WFixture *f,int maximum){
    memset(f,0,sizeof(*f));f->ts=calloc(1,sizeof(*f->ts));
    ASSERT_NOT_NULL(f->ts,"private schema");if(!f->ts)return -1;
    TypedSchema *ts=f->ts;ts->typed=1;ts->nfields=maximum?MAX_FIELDS:6;
    const char *tokens[MAX_FIELDS];
    for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":i==2?"numeric":i==3?"int":"timestamp";
        if(!strncmp(tokens[i],"varchar",7))ts->fields[i].type=FT_VARCHAR;
        else if(!strncmp(tokens[i],"numeric",7))ts->fields[i].type=FT_NUMERIC;
        else parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);
        ts->fields[i].name_len=(int)strlen(ts->fields[i].name);
    }
    int rc=typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0);
    ASSERT_EQ_INT(rc,0,"bind packed only in fixture");if(rc)return -1;
    if(!maximum){ts->fields[3].default_kind=DK_LITERAL;strcpy(ts->fields[3].default_val,"7");
        ts->fields[4].default_kind=DK_AUTO_CREATE;ts->fields[5].default_kind=DK_AUTO_UPDATE;}
    size_t slot=0;ASSERT_EQ_INT(pkr_slot_bound(&ts->packed,32,&slot),DEC_OK,"schema slot bound");
    f->sc=(Schema){.splits=1,.streams=1,.slot_size=(int)slot,.max_key=32,.max_value=ts->total_size};
    strcpy(f->root,"/tmp/shard-single-write-XXXXXX");
    if(!mkdtemp(f->root)){f->root[0]=0;return -1;}
    slotcask_init(64,64);rc=slotcask_open(&f->db,f->root,1,1,(int)slot);
    f->opened=rc==0;ASSERT_EQ_INT(rc,0,"open private physical object");return rc;
}
static int w_call(WFixture *f,int update,const char *key,const char *json,
    const char *condition,int flag,int ret,char **text){
    size_t n=0;*text=NULL;FILE *mem=open_memstream(text,&n);if(!mem)return -1;
    FILE *save=g_out;g_out=mem;
    int rc=update?shard_test_single_update(f->root,"obj",key,strlen(key),json,condition,flag,ret,&f->sc,f->ts,&f->db):
        shard_test_single_insert(f->root,"obj",key,strlen(key),json,condition,flag,ret,&f->sc,f->ts,&f->db);
    g_out=save;if(fclose(mem))return -1;return rc;
}
static char *w_get(WFixture *f,const char *key,size_t *n){
    void *b=NULL;uint8_t h=0;*n=0;
    if(slotcask_get_ex(&f->db,key,strlen(key),&b,n,&h))return NULL;
    ASSERT_EQ_INT(h,PKR_FLAGS,"persisted packed generation");
    char *s=typed_packed_decode(f->ts,b,*n,h,16*1024*1024);free(b);return s;
}
static uint64_t w_pos(WFixture *f){
    SlotcaskStream *s=&f->db.streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t n=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return n;
}
static void w_assert_reject(WFixture *f,int update,const char *json,const char *condition){
    size_t n=0,after_n=0;char *old=w_get(f,"a",&n),*out=NULL;
    uint64_t pos=w_pos(f),total=0,deleted=0,t2=0,d2=0;
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&total,&deleted),0,"before reject counts");
    ASSERT_EQ_INT(w_call(f,update,"a",json,condition,0,1,&out),1,"invalid write rejected");
    ASSERT_NOT_NULL(out,"error response");if(out){ASSERT_CONTAINS(out,"\"error\"","error JSON");free(out);}
    char *now=w_get(f,"a",&after_n);
    ASSERT_EQ_INT(n,after_n,"actual OLD length preserved");
    if(old&&now)ASSERT_EQ_STR(now,old,"OLD fields preserved");
    ASSERT_TRUE(w_pos(f)==pos,"no segment reservation on rejection");
    ASSERT_EQ_INT(slotcask_sum_kf_totals(&f->db,&t2,&d2),0,"after reject counts");
    ASSERT_TRUE(total==t2&&deleted==d2,"counts unchanged");free(old);free(now);
}
static void w_observe(int locked,void *p){*(int *)p=locked?1:-1;}
static void w_routes(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f1\":\"old\",\"f0\":1234567890123456.1234567890,\"f2\":1.0e2,\"f4\":1}",NULL,0,1,&out),0,"single actual-size insert");
    if(out){ASSERT_CONTAINS(out,"1234567890123456.1234567890","response exact numeric");
        ASSERT_CONTAINS(out,"\"f2\":100","bare exponent normalized");ASSERT_CONTAINS(out,"\"f3\":7","literal default");free(out);}
    size_t n=0;char *before=w_get(&f,"a",&n);ASSERT_NOT_NULL(before,"physical insert");
    if(!before){w_close(&f);return;}
    ASSERT_TRUE(n<256,"tiny actual value under multi-MiB schema");
    ASSERT_TRUE(!strstr(before,"\"f4\":1,"),"client cannot set auto_create");
    char *ac=strstr(before,"\"f4\":");char ac_copy[80]={0};
    if(ac){size_t k=strcspn(ac,",}");memcpy(ac_copy,ac,k);}
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f0\":2,\"f1\":\"replaced\",\"f4\":2}",NULL,0,1,&out),0,"upsert existing constructor");
    if(out){ASSERT_CONTAINS(out,ac_copy,"auto_create carried from locked OLD");free(out);}
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
static void w_maximum(void){
    WFixture f;if(w_open(&f,1)){w_close(&f);return;}
    ASSERT_TRUE(f.sc.slot_size>1000000000,"maximum schema bound over one GiB");char *out=NULL;size_t n=0;
    ASSERT_EQ_INT(w_call(&f,0,"a","{\"f127\":\"tiny\"}",NULL,0,1,&out),0,"max schema tiny single insert");free(out);
    ASSERT_EQ_INT(w_call(&f,1,"a","{\"f128\":\"small\"}",NULL,0,1,&out),0,"max schema tiny update");free(out);
    char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"max schema row");if(s){ASSERT_CONTAINS(s,"tiny","omitted field survives");ASSERT_CONTAINS(s,"small","changed field");free(s);}
    ASSERT_TRUE(n<4096,"physical size independent of schema maximum");w_close(&f);
}
static int w_fresh(const SlotcaskOldRecord *old,uint8_t **b,size_t *n,void *p){
    int *calls=p;(*calls)++;if(old)return -1;*b=malloc(1);if(!*b)return -1;**b=42;*n=1;return 0;
}
static void w_transport(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}int calls=0;SlotcaskUpsertOpts o={0};
    o.new_from_old_alloc=w_fresh;o.new_from_old_alloc_ctx=&calls;o.new_from_old_alloc_on_insert=1;
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&f.db,-1,"fresh",5,NULL,0,&o,NULL),0,"explicit fresh owning callback");
    ASSERT_EQ_INT(calls,1,"fresh constructor once");
    o.require_existing=1;ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"none",4,NULL,0,&o,NULL),-2,"require-existing precedes constructor");
    ASSERT_EQ_INT(calls,1,"missing target no constructor");
    o.require_existing=0;o.new_from_old_alloc=NULL;errno=0;
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&f.db,-1,"bad",3,NULL,0,&o,NULL),-1,"flag alone rejects");ASSERT_EQ_INT(errno,EINVAL,"invalid option errno");
    w_close(&f);
}
static void w_defaults_guards(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    /* Variable-width auto_create must be completed before NOT NULL validation. */
    f.ts->fields[1].default_kind=DK_AUTO_CREATE;f.ts->fields[1].not_null=1;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),0,"NOT NULL varchar auto_create");free(out);
    size_t n=0;char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"variable auto_create stored");free(s);
    f.ts->fields[0].default_kind=DK_LITERAL;strcpy(f.ts->fields[0].default_val,"");
    w_assert_reject(&f,0,"{}",NULL);f.ts->fields[0].default_kind=DK_NONE;
    char path[256];snprintf(path,sizeof(path),"%s/obj",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private metadata directory");
    snprintf(path,sizeof(path),"%s/obj/indexes",f.root);ASSERT_EQ_INT(mkdir(path,0700),0,"private index directory");
    snprintf(path,sizeof(path),"%s/obj/indexes/index.conf",f.root);FILE *fp=fopen(path,"w");ASSERT_NOT_NULL(fp,"private index manifest");
    if(fp){ASSERT_TRUE(fputs("f0\n",fp)>=0,"index manifest write");ASSERT_EQ_INT(fclose(fp),0,"index manifest close");}
    invalidate_idx_cache(f.root,"obj");w_assert_reject(&f,0,"{}",NULL);w_assert_reject(&f,1,"{}",NULL);
    ASSERT_EQ_INT(remove(path),0,"remove private index declaration");invalidate_idx_cache(f.root,"obj");
    void *bytes=NULL;uint8_t hdr=0;
    ASSERT_EQ_INT(slotcask_get_ex(&f.db,"a",1,&bytes,&n,&hdr),0,"packed seed for reverse mismatch");
    /* A separate immutable copy represents a legacy caller; no cache alteration. */
    TypedSchema *legacy=malloc(sizeof(*legacy));ASSERT_NOT_NULL(legacy,"private legacy schema copy");
    if(legacy){memset(legacy,0,sizeof(*legacy));legacy->typed=1;legacy->nfields=1;
        legacy->nullmap_prefix=2;legacy->total_size=6;
        legacy->fields[0].type=FT_INT;legacy->fields[0].size=4;
        strcpy(legacy->fields[0].name,"f0");legacy->fields[0].name_len=2;
        TypedSchema *saved=f.ts;f.ts=legacy;uint64_t pos=w_pos(&f);
        ASSERT_EQ_INT(w_call(&f,1,"a","{}",NULL,0,1,&out),1,"legacy schema rejects packed OLD");free(out);
        ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,1,&out),1,"legacy upsert rejects packed OLD");free(out);
        ASSERT_TRUE(w_pos(&f)==pos,"reverse mismatch no append");f.ts=saved;free(legacy);
        /* The legacy copy was never published; remove its borrowed trim binding. */
        atomic_store_explicit(&f.db.trim_fn,NULL,memory_order_release);f.db.trim_ctx=NULL;
    }
    free(bytes);w_close(&f);
}
static void w_large(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}
    const size_t length=4u*1024*1024;char *json=malloc(length+32),*out=NULL;
    ASSERT_NOT_NULL(json,"maximum varchar request");if(!json){w_close(&f);return;}
    memcpy(json,"{\"f1\":\"",7);memset(json+7,'x',length);memcpy(json+7+length,"\"}",3);
    ASSERT_EQ_INT(w_call(&f,0,"large",json,NULL,0,0,&out),0,"full 4 MiB single insert");free(out);
    size_t n=0;char *text=w_get(&f,"large",&n);ASSERT_NOT_NULL(text,"full large physical row");
    ASSERT_TRUE(n>length&&n<length+256,"4 MiB actual bytes plus bounded overhead");free(text);
    ASSERT_EQ_INT(w_call(&f,1,"large","{\"f1\":\"\"}",NULL,0,1,&out),0,"large-to-empty update");
    if(out){ASSERT_CONTAINS(out,"\"f1\":\"\"","returned empty distinct from NULL");free(out);}
    text=w_get(&f,"large",&n);ASSERT_NOT_NULL(text,"shrunk row physical");ASSERT_TRUE(n<256,"actual shrinking tail");free(text);
    free(json);w_close(&f);
}
typedef struct {pthread_mutex_t lock;pthread_cond_t cond;int ready,go;} WBarrier;
static void w_wait(WBarrier *b){pthread_mutex_lock(&b->lock);b->ready++;
    pthread_cond_broadcast(&b->cond);while(!b->go)pthread_cond_wait(&b->cond,&b->lock);
    pthread_mutex_unlock(&b->lock);}
typedef struct {WFixture *f;WBarrier *barrier;const char *json;char *out;int rc;ShardDb *owner;} WThread;
static void *w_thread(void *p){WThread *t=p;g_db=t->owner;w_wait(t->barrier);
    t->rc=w_call(t->f,1,"a",t->json,NULL,0,1,&t->out);g_db=NULL;return NULL;}
static void w_concurrent(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"a","{}",NULL,0,0,&out),0,"concurrent seed");free(out);
    WBarrier barrier={PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0,0};
    WThread a={&f,&barrier,"{\"f1\":\"writer-a\"}",NULL,-1,g_db},b={&f,&barrier,"{\"f3\":123}",NULL,-1,g_db};pthread_t ta,tb;
    int ra=pthread_create(&ta,NULL,w_thread,&a),rb=pthread_create(&tb,NULL,w_thread,&b);
    /* Thread creation failure is fatal: a missing participant cannot be joined safely. */
    if(ra||rb){fprintf(stderr,"cannot start private writer pair\n");abort();}
    pthread_mutex_lock(&barrier.lock);while(barrier.ready<2)pthread_cond_wait(&barrier.cond,&barrier.lock);
    barrier.go=1;pthread_cond_broadcast(&barrier.cond);pthread_mutex_unlock(&barrier.lock);
    pthread_join(ta,NULL);pthread_join(tb,NULL);pthread_cond_destroy(&barrier.cond);pthread_mutex_destroy(&barrier.lock);
    ASSERT_EQ_INT(a.rc,0,"first concurrent partial update");ASSERT_EQ_INT(b.rc,0,"second concurrent partial update");free(a.out);free(b.out);
    size_t n=0;char *s=w_get(&f,"a",&n);ASSERT_NOT_NULL(s,"concurrent final row");
    if(s){ASSERT_CONTAINS(s,"writer-a","disjoint varchar patch retained");ASSERT_CONTAINS(s,"\"f3\":123","disjoint fixed patch retained");free(s);}w_close(&f);
}

static void w_legacy_render(void){
    WFixture f;if(w_open(&f,0)){w_close(&f);return;}
    free(f.ts);f.ts=calloc(1,sizeof(*f.ts));ASSERT_NOT_NULL(f.ts,"private legacy render schema");
    if(!f.ts){w_close(&f);return;}
    TypedSchema *ts=f.ts;ts->typed=1;ts->nfields=2;ts->nullmap_prefix=2;ts->total_size=1029;
    TypedField *v=&ts->fields[0],*e=&ts->fields[1];
    v->type=FT_VARCHAR;v->size=1026;v->offset=0;strcpy(v->name,"f0");v->name_len=2;
    e->type=FT_ENUM;e->size=1;e->offset=1026;e->enum_width=1;e->n_enum_values=1;
    strcpy(e->name,"f1");e->name_len=2;e->enum_values=calloc(1,sizeof(char*));
    ASSERT_NOT_NULL(e->enum_values,"legacy enum catalog");if(!e->enum_values){w_close(&f);return;}
    e->enum_values[0]=malloc(1001);ASSERT_NOT_NULL(e->enum_values[0],"long enum value");
    if(!e->enum_values[0]){w_close(&f);return;}
    memset(e->enum_values[0],'z',1000);e->enum_values[0][1000]=0;
    uint8_t bytes[1029]={1,0};bytes[2]=3;bytes[3]=232;memset(bytes+4,'v',1000);
    char *decoded=typed_decode(ts,bytes,sizeof(bytes),SREC_FLAG_NULLMAP);
    ASSERT_NOT_NULL(decoded,"legacy complete decode");
    if(decoded){ASSERT_CONTAINS(decoded,e->enum_values[0],"long enum is not silently omitted");
        ASSERT_TRUE(strlen(decoded)>2010&&decoded[strlen(decoded)-1]=='}',"complete legacy JSON");free(decoded);}
    char json[2048];snprintf(json,sizeof(json),"{\"f0\":\"ok\",\"f1\":\"%s\"}",e->enum_values[0]);
    f.sc.max_value=ts->total_size;char *out=NULL;
    ASSERT_EQ_INT(w_call(&f,0,"legacy",json,NULL,0,1,&out),0,"legacy insert with long enum return_record");
    if(out){ASSERT_CONTAINS(out,e->enum_values[0],"insert returns full enum");free(out);}
    ASSERT_EQ_INT(w_call(&f,1,"legacy","{\"f0\":\"changed\"}",NULL,0,1,&out),0,"legacy update preserves long enum");
    if(out){ASSERT_CONTAINS(out,e->enum_values[0],"update returns full enum");free(out);}
    w_close(&f);
}

static int test_single_write_integration_run(void){
    w_legacy_render();w_transport();w_routes();w_mismatch();w_maximum();w_defaults_guards();w_large();w_concurrent();return t_ctx->failed?1:0;
}
TEST_REGISTER("test-single-write-integration",test_single_write_integration_run)

```

<!-- block:edit-06-old -->
```text
    src/test/cases/test_owning_write_hooks.c \
```

<!-- block:edit-06-new -->
```text
    src/test/cases/test_owning_write_hooks.c \
    src/test/cases/test_single_write_integration.c \
```

<!-- block:edit-07-old -->
```text
static int upsert_adapter_value_compute(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;

    if (uo->check && uo->check(old, uo->check_ctx) == 0) return -1;
    if (!uo->new_from_old && !uo->new_from_old_alloc) return 0;
    if (!old) return -1;               /* both constructors require OLD */

    if (uo->new_from_old_alloc) {
        uint8_t *buf = NULL;
        size_t out_vlen = 0;
        errno = 0;
        int rc = uo->new_from_old_alloc(old, &buf, &out_vlen,
                                         uo->new_from_old_alloc_ctx);
        int cause = 0;
        if (rc != 0) cause = errno ? errno : EINVAL;
        else if (!buf) cause = EINVAL;
        else if (out_vlen > UINT32_MAX ||
                 out_vlen > SIZE_MAX - 24 - rec->klen - 7) cause = EFBIG;
        else {
            size_t physical = slotcask_record_size_varlen(rec->klen, out_vlen);
            if (actx->db->slot_size < 0 ||
                physical > (size_t)actx->db->slot_size ||
                physical > slotcask_seg_max_bytes()) cause = EFBIG;
        }
        if (cause) {
            free(buf);
            actx->hard_error = 1;
            actx->owned_errno = cause;
            errno = cause;
            return -1;
        }
        actx->callback_buf = buf;
        rec->value = buf;
        rec->vlen = out_vlen;
        return 0;
    }

    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
    if (!buf) return -1;
    size_t out_vlen = 0;
    if (uo->new_from_old(old, buf, out_capacity, &out_vlen,
                         uo->new_from_old_ctx) != 0 ||
        out_vlen > out_capacity) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&actx->db->trim_fn,
                                                  memory_order_acquire);
    if (trim_fn) out_vlen = trim_fn(buf, out_vlen, actx->db->trim_ctx);
    if ((size_t)24 + rec->klen + out_vlen > (size_t)actx->db->slot_size) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    actx->callback_buf = buf;
    rec->value = buf;
    rec->vlen = out_vlen;
    return 0;
}
```

<!-- block:edit-07-new -->
```text
static int upsert_adapter_value_compute(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;

    if (uo->check && uo->check(old, uo->check_ctx) == 0) return -1;
    if (!uo->new_from_old && !uo->new_from_old_alloc) return 0;
    if (!old && !(uo->new_from_old_alloc && uo->new_from_old_alloc_on_insert))
        return -1;

    if (uo->new_from_old_alloc) {
        uint8_t *buf = NULL;
        size_t out_vlen = 0;
        errno = 0;
        int rc = uo->new_from_old_alloc(old, &buf, &out_vlen,
                                         uo->new_from_old_alloc_ctx);
        int cause = 0;
        if (rc != 0) cause = errno ? errno : EINVAL;
        else if (!buf) cause = EINVAL;
        else if (out_vlen > UINT32_MAX ||
                 out_vlen > SIZE_MAX - 24 - rec->klen - 7) cause = EFBIG;
        else {
            size_t physical = slotcask_record_size_varlen(rec->klen, out_vlen);
            if (actx->db->slot_size < 0 ||
                physical > (size_t)actx->db->slot_size ||
                physical > slotcask_seg_max_bytes()) cause = EFBIG;
        }
        if (cause) {
            free(buf);
            actx->hard_error = 1;
            actx->owned_errno = cause;
            errno = cause;
            return -1;
        }
        actx->callback_buf = buf;
        rec->value = buf;
        rec->vlen = out_vlen;
        return 0;
    }

    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
    if (!buf) return -1;
    size_t out_vlen = 0;
    if (uo->new_from_old(old, buf, out_capacity, &out_vlen,
                         uo->new_from_old_ctx) != 0 ||
        out_vlen > out_capacity) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&actx->db->trim_fn,
                                                  memory_order_acquire);
    if (trim_fn) out_vlen = trim_fn(buf, out_vlen, actx->db->trim_ctx);
    if ((size_t)24 + rec->klen + out_vlen > (size_t)actx->db->slot_size) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    actx->callback_buf = buf;
    rec->value = buf;
    rec->vlen = out_vlen;
    return 0;
}
```

<!-- block:edit-08-old -->
```text
int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if (opts->new_from_old && opts->new_from_old_alloc) {
        errno = EINVAL;
        return -1;
    }

    /* Fail loud on a missed two-phase migration: an indexed object with a
       legacy pre_commit alongside only one half of the prepare/apply pair
       (or with prepare/apply set but the other missing) would silently run
       with an incomplete hook set. This applies to require_existing=1
       (update-only) callers too — an update-resolved record's durable
       index write is just as pre-marker if it lives in a bare pre_commit
       instead of being staged there and applied post-marker by
       prepare/apply. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* kf_shard is a pure hash-routing constant — known before the
       transaction starts, unlike kf_slot which segment reservation only
       resolves mid-transaction. Publish it now so any caller hook that
       reads it via a ctx struct (e.g. pre_commit, which fires before slot
       resolution) sees the real value instead of a stale/zero default. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    bopts.if_not_exists = opts->if_not_exists;
    bopts.require_existing = opts->require_existing;
    bopts.pre_commit = upsert_adapter_pre_commit;
    /* Single-record call: always fetch OLD when the key is found, so
       check/pre_commit/new_from_old (any of which may dereference it) always
       see it — the batch-amortised skip this flag otherwise enables doesn't
       apply to a window_cap=1 transaction. */
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    /* prepare_window/apply_window are only meaningful together (two-phase);
       the coordinator only runs the window-hook path when both are
       non-NULL. Applies to both fresh-insert and update-resolved records —
       see the doc comment on upsert_adapter_prepare_window/apply_window. */
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

<!-- block:edit-08-new -->
```text
int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if ((opts->new_from_old && opts->new_from_old_alloc) ||
        (opts->new_from_old_alloc_on_insert && !opts->new_from_old_alloc)) {
        errno = EINVAL;
        return -1;
    }

    /* Fail loud on a missed two-phase migration: an indexed object with a
       legacy pre_commit alongside only one half of the prepare/apply pair
       (or with prepare/apply set but the other missing) would silently run
       with an incomplete hook set. This applies to require_existing=1
       (update-only) callers too — an update-resolved record's durable
       index write is just as pre-marker if it lives in a bare pre_commit
       instead of being staged there and applied post-marker by
       prepare/apply. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* kf_shard is a pure hash-routing constant — known before the
       transaction starts, unlike kf_slot which segment reservation only
       resolves mid-transaction. Publish it now so any caller hook that
       reads it via a ctx struct (e.g. pre_commit, which fires before slot
       resolution) sees the real value instead of a stale/zero default. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    bopts.if_not_exists = opts->if_not_exists;
    bopts.require_existing = opts->require_existing;
    bopts.pre_commit = upsert_adapter_pre_commit;
    /* Single-record call: always fetch OLD when the key is found, so
       check/pre_commit/new_from_old (any of which may dereference it) always
       see it — the batch-amortised skip this flag otherwise enables doesn't
       apply to a window_cap=1 transaction. */
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    /* prepare_window/apply_window are only meaningful together (two-phase);
       the coordinator only runs the window-hook path when both are
       non-NULL. Applies to both fresh-insert and update-resolved records —
       see the doc comment on upsert_adapter_prepare_window/apply_window. */
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

<!-- block:edit-09-old -->
```text
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if (opts->new_from_old && opts->new_from_old_alloc) {
        errno = EINVAL;
        return -1;
    }

    /* require_existing is incompatible with INSERT-only semantics; the
       caller should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       pre_commit) that fire before slot resolution. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    /* Insert-only: always reject an existing key regardless of what the
       caller passed — the one contract difference from the upsert path. */
    bopts.if_not_exists = 1;
    bopts.pre_commit = upsert_adapter_pre_commit;
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

<!-- block:edit-09-new -->
```text
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
        result->current_hdr2 = 0;
        result->current_hdr2 = 0;
    }
    if (opts && opts->new_from_old_alloc) {
        value = NULL;
        vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn && !(opts && opts->new_from_old_alloc))
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if ((opts->new_from_old && opts->new_from_old_alloc) ||
        (opts->new_from_old_alloc_on_insert && !opts->new_from_old_alloc)) {
        errno = EINVAL;
        return -1;
    }

    /* require_existing is incompatible with INSERT-only semantics; the
       caller should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       pre_commit) that fire before slot resolution. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    /* Insert-only: always reject an existing key regardless of what the
       caller passed — the one contract difference from the upsert path. */
    bopts.if_not_exists = 1;
    bopts.pre_commit = upsert_adapter_pre_commit;
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old || opts->new_from_old_alloc)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL ||
                                   opts->new_from_old_alloc != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.hdr2 = opts ? opts->record_hdr2 : 0;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            uint8_t cv_hdr2 = 0;
            if (slotcask_get_ex(db, key, klen, &cv, &cvl, &cv_hdr2) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
                result->current_hdr2 = cv_hdr2;
            }
        }
        return -2;
    }
    if (rec.status != 0) {
        if (actx.owned_errno) errno = actx.owned_errno;
        return -1;
    }

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}
```

<!-- block:edit-10-old -->
```text
int typed_prepared_defaults(TypedPrepared *p,const char *root,const char *object,
                             char *err,size_t cap){
    if(prepared_check(p,err,cap))return -1;
    for(int i=0;i<p->schema->nfields;i++){
        const TypedField *f=&p->schema->fields[i];if(f->removed||p->seen[i])continue;
        TypedInput in={NULL,0,1,1,0};char generated[256];
        if(f->default_kind!=DK_NONE){
            in.data=generate_default(f,generated,sizeof(generated),root,object);
            if(!in.data)return row_error(err,cap,EIO,"default generator failed");
            in.len=strlen(in.data);in.is_null=0;
        }
        if(typed_prepared_set(p,(size_t)i,&in,err,cap))return -1;
    }return 0;
}
```

<!-- block:edit-10-new -->
```text
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
```

<!-- block:edit-11-old -->
```text
static int prepare_json(const TypedSchema *ts,const char *json,int defaults,
                          const char *root,const char *object,TypedPrepared *out,
                          char *err,size_t cap){
    if(!json)return row_error(err,cap,EINVAL,"invalid record JSON");
    const char *valid=json;
    if(row_json_value(&valid,0)||*row_json_space(valid))
        return row_error(err,cap,EINVAL,"invalid record JSON");
    if(typed_prepared_zero(ts,out,err,cap))return -1;
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
```

<!-- block:edit-11-new -->
```text
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
```

<!-- block:edit-12-old -->
```text
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
```

<!-- block:edit-12-new -->
```text
int typed_encode_packed_into(const TypedSchema *ts,const char *json,int defaults,
                              const char *root,const char *object,uint8_t *out,
                              size_t cap,size_t *used,char *err,size_t errsz){
    if(used)*used=0;
    TypedPrepared p;typed_prepared_init(&p);
    int rc=prepare_json(ts,json,defaults,root,object,&p,NULL,err,errsz);
    if(!rc)rc=typed_prepared_compose(&p,out,cap,used,err,errsz);
    typed_prepared_free(&p);return rc;
}
```

<!-- block:edit-13-old -->
```text
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
    int rc=prepare_json(ts,json,defaults,root,object,&p,err,cap);
    if(!rc)rc=typed_prepared_alloc(&p,out,len,err,cap);
    typed_prepared_free(&p);return rc;
}
```

<!-- block:edit-13-new -->
```text
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

```

<!-- block:edit-14-old -->
```text
int typed_encode_packed_into(
```

<!-- block:edit-14-new -->
```text
/* Internal single-write collectors; output is NULL/0 on failure, owned
   malloc storage on success. No schema activation. */
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,
    const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap);
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_encode_packed_into(
```

<!-- block:edit-15-old -->
```text
#include "slotcask.h"
```

<!-- block:edit-15-new -->
```text
#include "slotcask.h"
#include "typed_row.h"
```

<!-- block:edit-16-old -->
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
```

<!-- block:edit-16-new -->
```text

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
    }
    return 0;
}
static void single_write_error(const char *message,const char *fallback){
    char esc[512*6+1];
    int n=message?json_escape_into(esc,sizeof(esc),message,strlen(message)):-1;
    if(n>=0){esc[n]='\0';OUT("{\"error\":\"%s\"}\n",esc);}
    else OUT("{\"error\":\"%s\"}\n",fallback);
}

typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
```

<!-- block:edit-17-old -->
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
    int               nfields;
    enum IndexType   *idx_types;       /* parallel to fields[], loaded once */
    const char       *value_json;
    TypedSchema      *idx_ts;
    CriteriaNode     *cas_tree;
    FieldSchema      *cas_fs;
    /* Populated by slotcask BEFORE pre_commit so bitmap update_idx_fn can
       address the just-written record by (shard, slot). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit when a bitmap-index field's per-file cap
       is exceeded. cmd_insert_v2 surfaces this as the wire-level error
       so the operator gets an actionable message + the field name. */
    char              err_buf[256];
    /* Fresh-insert path only: staged bitmap writer handles from
       v2_insert_prepare_commit (pre-marker, cap-checked, no durable
       mutation), applied by v2_insert_apply_commit (post-marker). The
       BitmapPrepareSet entries borrow new_key pointers rather than
       copying, so the underlying allocations are tracked separately
       here and freed once apply/abort has read them. */
    BitmapPrepareSet  bm_prep;
    uint8_t          *bm_owned_keys[MAX_FIELDS];
    int               n_bm_owned;
    /* Update-resolved path only: set by v2_insert_pre_commit from the
       hook's is_update argument. The per-field index diff is computed (in
       memory only) in pre_commit and staged here; v2_insert_apply_commit
       applies it durably after the commit-intent marker is published, and
       v2_insert_abort_commit frees it unapplied if the marker write fails. */
    int               is_update;
    UpdateIdxArg      upd_args[MAX_FIELDS];
    int               n_upd_args;
} V2InsertCtx;
```

<!-- block:edit-17-new -->
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
    int               nfields;
    enum IndexType   *idx_types;       /* parallel to fields[], loaded once */
    const char       *value_json;
    TypedSchema      *idx_ts;
    CriteriaNode     *cas_tree;
    FieldSchema      *cas_fs;
    /* Populated by slotcask BEFORE pre_commit so bitmap update_idx_fn can
       address the just-written record by (shard, slot). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit when a bitmap-index field's per-file cap
       is exceeded. cmd_insert_v2 surfaces this as the wire-level error
       so the operator gets an actionable message + the field name. */
    char              err_buf[256];
    /* Fresh-insert path only: staged bitmap writer handles from
       v2_insert_prepare_commit (pre-marker, cap-checked, no durable
       mutation), applied by v2_insert_apply_commit (post-marker). The
       BitmapPrepareSet entries borrow new_key pointers rather than
       copying, so the underlying allocations are tracked separately
       here and freed once apply/abort has read them. */
    BitmapPrepareSet  bm_prep;
    uint8_t          *bm_owned_keys[MAX_FIELDS];
    int               n_bm_owned;
    /* Update-resolved path only: set by v2_insert_pre_commit from the
       hook's is_update argument. The per-field index diff is computed (in
       memory only) in pre_commit and staged here; v2_insert_apply_commit
       applies it durably after the commit-intent marker is published, and
       v2_insert_abort_commit frees it unapplied if the marker write fails. */
    int               is_update;
    UpdateIdxArg      upd_args[MAX_FIELDS];
    int               n_upd_args;
    uint8_t          *incoming; /* command-owned until constructor steals it */
    size_t            incoming_len, key_len;
    SlotcaskDb       *db;
    int               return_record;
    char             *ret_json; /* rendered before commit, command-owned */
} V2InsertCtx;
```

<!-- block:edit-18-old -->
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    enum IndexType   *idx_types;        /* parallel to idx_fields[] */
    TypedSchema      *idx_ts;
    /* Raw partial-update field JSON, parsed inside the lock-protected
       new_from_old callback (v2_update_new_from_old). */
    const char       *partial_json;
    /* CAS criteria — verified inside check_fn under the kf-shard wrlock
       so the check + commit are atomic against concurrent writers. NULL
       when the caller didn't pass `if`. */
    CriteriaNode     *cas_tree;
    FieldSchema      *cas_fs;
    /* Populated by slotcask BEFORE pre_commit (bitmap addresses records
       by physical slot, not by hash). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit on bitmap-index cap overflow, and by
       v2_update_new_from_old for malformed-escape / varchar-overflow
       rejections. */
    char              err_buf[256];
    /* Stashed during v2_update_new_from_old so apply_commit can
       compute the forward index diff (old→new) without re-reading. */
    const uint8_t    *saved_old_value;
    size_t            saved_old_vlen;
    /* OLD record generation (segment header byte 19) — captured by the
       check_fn alongside saved_old_value. */
    uint8_t           old_hdr2;
    /* F4 return_record: set from the request flag; ret_buf holds a copy
       of the final NEW record (filled at the end of
       v2_update_new_from_old) for the success response. Zeroed by the
       ctx initializer. */
    int               return_record;
    uint8_t          *ret_buf;
} V2UpdateCtx;
```

<!-- block:edit-18-new -->
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    enum IndexType   *idx_types;        /* parallel to idx_fields[] */
    TypedSchema      *idx_ts;
    /* Raw partial-update field JSON, parsed inside the lock-protected
       new_from_old callback (v2_update_new_from_old). */
    const char       *partial_json;
    /* CAS criteria — verified inside check_fn under the kf-shard wrlock
       so the check + commit are atomic against concurrent writers. NULL
       when the caller didn't pass `if`. */
    CriteriaNode     *cas_tree;
    FieldSchema      *cas_fs;
    /* Populated by slotcask BEFORE pre_commit (bitmap addresses records
       by physical slot, not by hash). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit on bitmap-index cap overflow, and by
       v2_update_new_from_old for malformed-escape / varchar-overflow
       rejections. */
    char              err_buf[256];
    /* Stashed during v2_update_new_from_old so apply_commit can
       compute the forward index diff (old→new) without re-reading. */
    const uint8_t    *saved_old_value;
    size_t            saved_old_vlen;
    /* OLD record generation (segment header byte 19) — captured by the
       check_fn alongside saved_old_value. */
    uint8_t           old_hdr2;
    int               return_record;
    char             *ret_json; /* rendered before reservation; command-owned */
    SlotcaskDb       *db;
    size_t            key_len;
} V2UpdateCtx;
```

<!-- block:edit-19-old -->
```text
static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = ctx_ptr;
    if (!c->cas_tree) return 1;
    if (!old) return 0;
    return cas_check_tree(c->cas_fs, old->value, (int)old->vlen,
                           old->hdr2, c->cas_tree);
}
```

<!-- block:edit-19-new -->
```text
static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = ctx_ptr;
    if (!c->cas_tree) return 1;
    if (!old) return 0;
    return cas_check_tree(c->cas_fs, old->value, (int)old->vlen,
                           old->hdr2, c->cas_tree);
}
static int v2_insert_owned(const SlotcaskOldRecord *old,uint8_t **out,
    size_t *n,void *ptr){
    V2InsertCtx *c=ptr;TypedSchema *ts=c->idx_ts;
    if(old){TypedRowView view;
        if(typed_row_open(ts,old->value,old->vlen,old->hdr2,&view)){
            snprintf(c->err_buf,sizeof(c->err_buf),"invalid OLD generation or value");errno=EILSEQ;return -1;}}
    int has_ac=0;
    for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed&&
        ts->fields[i].default_kind==DK_AUTO_CREATE){has_ac=1;break;}
    if(ts->packed.nfields){
        if(typed_insert_alloc(ts,old,c->value_json,c->db_root,c->object,
            out,n,c->err_buf,sizeof(c->err_buf)))return -1;
    }else{
        *out=c->incoming;*n=c->incoming_len;c->incoming=NULL;
        if(has_ac){
            const uint8_t *base=NULL,*bm=NULL;int bmn=0;size_t region=0;
            int ok=old&&typed_record_view(old->value,old->vlen,old->hdr2,
                &base,&bm,&bmn,&region)==0;
            for(int i=0;i<ts->nfields;i++)if(!ts->fields[i].removed&&
                ts->fields[i].default_kind==DK_AUTO_CREATE)
                auto_create_field_transfer(ts,*out,i,old!=NULL,ok,base,bm,bmn,
                    region,old?old->hdr2:0);
        }
    }
    return single_write_finish(ts,c->db,c->key_len,*out,n,c->return_record,
        &c->ret_json,c->err_buf,sizeof(c->err_buf));
}

```

<!-- block:edit-20-old -->
```text
static int cmd_insert_v2(const char *db_root, const char *object,
                         const char *key, size_t klen, const char *value,
                         const char *if_json, int if_not_exists,
                         int return_record, const Schema *sc) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }

    if ((int)klen > sc->max_key) {
        fprintf(stderr, "Error: Key too large (%zu > %d)\n", klen, sc->max_key);
        return 1;
    }

    uint8_t *typed_buf = malloc(ts->total_size);
    if (!typed_buf) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    char enc_err[512] = {0};
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object, enc_err, sizeof(enc_err));
    if (enc == -2) {
        /* Strict enum (or future typed) validation rejection — actionable
           error already in enc_err. Caller never sees a successfully-
           encoded but semantically-corrupt record. */
        free(typed_buf);
        OUT("{\"error\":\"%s\"}\n", enc_err);
        return 1;
    }
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    size_t vlen = ts->total_size;
    if ((int)vlen > sc->max_value) {
        free(typed_buf);
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc->max_value);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        free(typed_buf);
        OUT("{\"error\":\"Cannot open shard\"}\n");
        return 1;
    }

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* auto_create: stamp now() on first insert only; preserve the stored value
       on any update / re-insert. This path is an upsert, so a re-insert of an
       existing key would otherwise zero the create-time (typed_encode_defaults
       leaves DK_AUTO_CREATE fields blank). We consult the prior record — but
       only when the schema actually declares an auto_create field, so ordinary
       objects pay nothing. */
    {
        int has_ac = 0;
        for (int i = 0; i < ts->nfields; i++)
            if (!ts->fields[i].removed &&
                ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
        if (has_ac) {
            void *ac_old = NULL; size_t ac_old_vlen = 0;
            uint8_t ac_old_hdr2 = 0;
            int existed = (slotcask_get_ex(sdb, key, klen, &ac_old,
                                           &ac_old_vlen, &ac_old_hdr2) == 0);
            /* OLD generation view (validated once, shared by the loop). */
            const uint8_t *o_base = NULL, *o_bm = NULL;
            int o_bm_bytes = 0; size_t o_region = 0;
            int o_view_ok = existed && ac_old &&
                typed_record_view((const uint8_t *)ac_old, ac_old_vlen,
                                  ac_old_hdr2, &o_base, &o_bm,
                                  &o_bm_bytes, &o_region) == 0;
            for (int i = 0; i < ts->nfields; i++) {
                if (ts->fields[i].removed ||
                    ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
                auto_create_field_transfer(ts, typed_buf, i,
                                           existed && ac_old ? 1 : 0, o_view_ok,
                                           o_base, o_bm, o_bm_bytes, o_region,
                                           ac_old_hdr2);
            }
            free(ac_old);
        }
    }

    /* Index fields + criteria (only parsed if if_json is present). */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                    cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            LOG_WARN(LOG_SUB_SLOTCASK, "insert if rejected: %s", cas_err);
            free(typed_buf);
            OUT("{\"error\":\"invalid if condition\"}\n");
            return 1;
        }
        /* cas_tree NULL with empty cas_err = vacuous pass ([]). */
    }

    V2InsertCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .fields = fields, .nfields = nfields,
        .idx_types = idx_types,
        .value_json = value,
        .idx_ts = ts,
        .cas_tree = cas_tree, .cas_fs = &cas_fs,
    };
    compute_hash_raw(key, klen, ctx.hash);
    int durability_pending = 0;

    SlotcaskUpsertOpts opts = {
        .if_not_exists  = if_not_exists,
        .check          = v2_insert_check_fn,
        .check_ctx      = &ctx,
        .pre_commit     = v2_insert_pre_commit,
        .pre_commit_ctx = &ctx,
        /* Fresh-key inserts route through the two-phase hooks instead of
           pre_commit (v2_insert_pre_commit's fresh-insert branch is a
           no-op — see its doc comment); pre_commit stays wired for the
           update-resolved path only. */
        .prepare_commit = v2_insert_prepare_commit,
        .apply_commit   = v2_insert_apply_commit,
        .abort_commit   = v2_insert_abort_commit,
        /* Bitmap index needs (shard, slot) — slotcask writes them here
           before invoking pre_commit. update_idx_fn reads them via
           V2InsertCtx (same struct, no second indirection). */
        .out_kf_shard   = &ctx.kf_shard,
        .out_kf_slot    = &ctx.kf_slot,
        .has_indexed_fields = nfields > 0,
        .out_durability_degraded = &durability_pending,
        .record_hdr2   = SREC_FLAG_NULLMAP,  /* nullmap encoders */
    };
    SlotcaskUpsertResult result = {0};
    int rc;
    /* Fast path: pure INSERT semantics (if_not_exists=true) without a CAS
       criterion lets us skip the kf_lookup-with-verify pass that the upsert
       path always pays. The duplicate detection still happens implicitly
       inside kf_put_new — caller sees -2 with was_update=1 if a duplicate
       is found. v2_insert_check_fn / v2_insert_pre_commit handle old=NULL
       correctly (they only diff against old when is_update=1). */
    uint64_t _commit_t0 = now_us();
    if (if_not_exists && !if_json) {
        rc = slotcask_insert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    } else {
        rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    }
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        char *cur = result.current_value
            ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen, result.current_hdr2)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur);
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(typed_buf);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(typed_buf);
        if (ctx.err_buf[0]) {
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        } else {
            OUT("{\"error\":\"upsert failed\"}\n");
        }
        return 1;
    }

    if (durability_pending) {
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(typed_buf);
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }

    if (!result.was_update) update_count(db_root, object, 1);
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "%s %s.%s (slotcask)",
            result.was_update ? "UPDATE" : "INSERT", object, wire_key);

    free(result.current_value);
    free_criteria_tree(cas_tree);
    if (return_record) {
        /* typed_buf was mutated in place by the auto_create transfer
           before the upsert — it IS the committed record. */
        char *rec = typed_decode(ts, typed_buf, (uint32_t)vlen,
                                 SREC_FLAG_NULLMAP);
        free(typed_buf);
        if (rec)
            OUT("{\"status\":\"%s\",\"key\":\"%s\",\"record\":%s}\n",
                result.was_update ? "updated" : "inserted", wire_key, rec);
        else
            OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
                result.was_update ? "updated" : "inserted", wire_key);
        free(rec);
        return 0;
    }
    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update ? "updated" : "inserted", wire_key);
    return 0;
}
```

<!-- block:edit-20-new -->
```text
static int cmd_insert_v2_impl(const char *db_root, const char *object,
                         const char *key, size_t klen, const char *value,
                         const char *if_json, int if_not_exists,
                         int return_record, const Schema *sc, TypedSchema *ts, SlotcaskDb *borrowed) {
    if (!ts) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }

    if ((int)klen > sc->max_key) {
        fprintf(stderr, "Error: Key too large (%zu > %d)\n", klen, sc->max_key);
        return 1;
    }

    /* Unsupported private packed integrations fail before defaults or writes. */
    char guard_fields[MAX_FIELDS][256];
    if(ts->packed.nfields&&(if_json||load_index_fields(db_root,object,guard_fields,MAX_FIELDS)>0)){
        OUT("{\"error\":\"packed indexes and conditions await integration\"}\n");return 1;
    }
    uint8_t *typed_buf=NULL;size_t vlen=0;char enc_err[512]={0};
    if(!ts->packed.nfields&&typed_encode_alloc(ts,value,1,db_root,object,&typed_buf,&vlen,enc_err,sizeof(enc_err))){
        single_write_error(enc_err[0]?enc_err:"Typed encode failed","Typed encode failed");return 1;
    }
    if(vlen>(size_t)sc->max_value){free(typed_buf);
        OUT("{\"error\":\"value exceeds schema bound\"}\n");return 1;}

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *owned SDB_REG_REF = borrowed?NULL:slotcask_registry_get(db_root, object, &info);
    SlotcaskDb *sdb=borrowed?borrowed:owned;
    if (!sdb) {
        free(typed_buf);
        OUT("{\"error\":\"Cannot open shard\"}\n");
        return 1;
    }

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!ts->packed.nfields && !atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* Index fields + criteria (only parsed if if_json is present). */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    FieldSchema cas_fs;
    memset(&cas_fs,0,sizeof(cas_fs));cas_fs.ts=ts;
    if(!ts->packed.nfields)init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                    cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            LOG_WARN(LOG_SUB_SLOTCASK, "insert if rejected: %s", cas_err);
            free(typed_buf);
            OUT("{\"error\":\"invalid if condition\"}\n");
            return 1;
        }
        /* cas_tree NULL with empty cas_err = vacuous pass ([]). */
    }

    V2InsertCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .fields = fields, .nfields = nfields,
        .idx_types = idx_types,
        .value_json = value,
        .idx_ts = ts,
        .incoming=typed_buf,.incoming_len=vlen,.key_len=klen,.db=sdb,
        .return_record=return_record,
        .cas_tree = cas_tree, .cas_fs = &cas_fs,
    };
    compute_hash_raw(key, klen, ctx.hash);
    int durability_pending = 0;

    SlotcaskUpsertOpts opts = {
        .if_not_exists  = if_not_exists,
        .check          = v2_insert_check_fn,
        .check_ctx      = &ctx,
        .pre_commit     = v2_insert_pre_commit,
        .pre_commit_ctx = &ctx,
        /* Fresh-key inserts route through the two-phase hooks instead of
           pre_commit (v2_insert_pre_commit's fresh-insert branch is a
           no-op — see its doc comment); pre_commit stays wired for the
           update-resolved path only. */
        .prepare_commit = v2_insert_prepare_commit,
        .apply_commit   = v2_insert_apply_commit,
        .abort_commit   = v2_insert_abort_commit,
        /* Bitmap index needs (shard, slot) — slotcask writes them here
           before invoking pre_commit. update_idx_fn reads them via
           V2InsertCtx (same struct, no second indirection). */
        .out_kf_shard   = &ctx.kf_shard,
        .out_kf_slot    = &ctx.kf_slot,
        .has_indexed_fields = nfields > 0,
        .out_durability_degraded = &durability_pending,
        .record_hdr2   = typed_schema_hdr2(ts),
        .new_from_old_alloc=v2_insert_owned,.new_from_old_alloc_ctx=&ctx,
        .new_from_old_alloc_on_insert=1,
    };
    SlotcaskUpsertResult result = {0};
    int rc;
    /* Fast path: pure INSERT semantics (if_not_exists=true) without a CAS
       criterion lets us skip the kf_lookup-with-verify pass that the upsert
       path always pays. The duplicate detection still happens implicitly
       inside kf_put_new — caller sees -2 with was_update=1 if a duplicate
       is found. v2_insert_check_fn / v2_insert_pre_commit handle old=NULL
       correctly (they only diff against old when is_update=1). */
    uint64_t _commit_t0 = now_us();
    if (if_not_exists && !if_json) {
        rc = slotcask_insert_with_hooks(sdb, -1, key, klen,
                                        NULL, 0, &opts, &result);
    } else {
        rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        NULL, 0, &opts, &result);
    }
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        char *cur = result.current_value
            ? single_write_decode(ts, result.current_value, result.current_vlen, result.current_hdr2)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur);
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(ctx.incoming);free(ctx.ret_json);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(ctx.incoming);free(ctx.ret_json);
        if (ctx.err_buf[0]) {
            single_write_error(ctx.err_buf,"upsert failed");
        } else {
            OUT("{\"error\":\"upsert failed\"}\n");
        }
        return 1;
    }

    if (durability_pending) {
        free(result.current_value);
        free_criteria_tree(cas_tree);
        free(ctx.incoming);free(ctx.ret_json);
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }

    if (!result.was_update) update_count(db_root, object, 1);
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "%s %s.%s (slotcask)",
            result.was_update ? "UPDATE" : "INSERT", object, wire_key);

    free(result.current_value);
    free_criteria_tree(cas_tree);
    free(ctx.incoming);
    if(return_record){
        OUT("{\"status\":\"%s\",\"key\":\"%s\",\"record\":%s}\n",
            result.was_update?"updated":"inserted",wire_key,ctx.ret_json);
        free(ctx.ret_json);return 0;
    }
    free(ctx.ret_json);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update?"updated":"inserted",wire_key);return 0;
}
static int cmd_insert_v2(const char *root,const char *object,const char *key,
    size_t klen,const char *json,const char *condition,int only_new,
    int return_record,const Schema *sc){
    return cmd_insert_v2_impl(root,object,key,klen,json,condition,only_new,
        return_record,sc,load_typed_schema(root,object),NULL);
}
```

<!-- block:edit-21-old -->
```text
static int cmd_update_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *partial_json,
                         const char *if_json, int dry_run,
                         int return_record, const Schema *sc) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) { OUT("{\"error\":\"Object not found\"}\n"); return 1; }

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* dry_run validates criteria but doesn't write — race-tolerant. It is
       the only remaining user of the up-front slotcask_get snapshot: a
       normal update builds NEW from the lock-protected OLD inside
       v2_update_new_from_old instead, so no stale outside-lock snapshot
       exists for the race to be lost against. */
    if (dry_run) {
        void *old_val = NULL; size_t old_vlen = 0;
        uint8_t old_hdr2 = 0;
        if (slotcask_get_ex(sdb, key, klen, &old_val, &old_vlen, &old_hdr2) != 0) {
            OUT("{\"error\":\"Not found\"}\n");
            return 1;
        }
#ifdef TEST_BUILD
        /* TEST_BUILD seam retained for dry_run only: fires on dry-run
           updates in the fixed code (normal updates pause at the top of
           v2_update_new_from_old, under_kf_wrlock == 1). */
        slotcask_test_after_old(0);
#endif
        if (if_json) {
            FieldSchema cas_fs;
            init_field_schema(&cas_fs, db_root, object);
            char cas_err[256];
            CriteriaNode *cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                                      cas_err, sizeof(cas_err));
            if (!cas_tree && cas_err[0]) {
                LOG_WARN(LOG_SUB_SLOTCASK, "update if rejected: %s", cas_err);
                free(old_val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check_tree(&cas_fs, old_val, (int)old_vlen,
                                      old_hdr2, cas_tree);
            free_criteria_tree(cas_tree);
            if (!pass) {
                char *cur = typed_decode(ts, old_val, (uint32_t)old_vlen, old_hdr2);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(old_val);
                return 1;
            }
        }
        free(old_val);
        char wire_key[1100];
        format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    /* Parse `if` once before the lock-protected callback; check_fn runs
       cas_check_tree under the kf-shard wrlock so the verify + commit are
       atomic against concurrent writers. Note the intentional precedence
       shift: when both the partial field JSON and the `if` criteria are
       invalid, this now reports "invalid if condition" first — the field
       parsing happens inside the callback under the lock. */
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                    cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            LOG_WARN(LOG_SUB_SLOTCASK, "update if rejected: %s", cas_err);
            OUT("{\"error\":\"invalid if condition\"}\n");
            return 1;
        }
        /* cas_tree NULL with empty cas_err = vacuous pass ([]). */
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    V2UpdateCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
        .idx_ts = ts,
        .partial_json = partial_json,
        .cas_tree = cas_tree, .cas_fs = &cas_fs,
        .return_record = return_record,
    };
    compute_hash_raw(key, klen, ctx.hash);
    int durability_pending = 0;

    SlotcaskUpsertOpts opts = {
        .require_existing = 1,
        .check            = v2_update_check_fn,
        .check_ctx        = &ctx,
        .new_from_old     = v2_update_new_from_old,
        .new_from_old_ctx = &ctx,
        .prepare_commit   = v2_update_prepare_commit,
        .apply_commit     = v2_update_apply_commit,
        .abort_commit     = v2_update_abort_commit,
        .pre_commit_ctx   = &ctx,
        .out_kf_shard     = &ctx.kf_shard,
        .out_kf_slot      = &ctx.kf_slot,
        .has_indexed_fields = nidx > 0,
        .out_durability_degraded = &durability_pending,
        .record_hdr2   = SREC_FLAG_NULLMAP,  /* nullmap encoders */
    };
    SlotcaskUpsertResult result = {0};
    uint64_t _commit_t0 = now_us();
    int rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        NULL, 0, &opts, &result);
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        /* Either require_existing fired (record vanished) or check_fn
           rejected (criteria didn't match). Disambiguate by whether the
           result has an old value attached. */
        if (result.was_update && result.condition_not_met) {
            char *cur = result.current_value
                ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen, result.current_hdr2)
                : NULL;
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                cur ? cur : "null");
            free(cur);
        } else {
            OUT("{\"error\":\"Not found\"}\n");
        }
        free(result.current_value);
        free(ctx.ret_buf); ctx.ret_buf = NULL;
        free_criteria_tree(cas_tree);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free(ctx.ret_buf); ctx.ret_buf = NULL;
        free_criteria_tree(cas_tree);
        if (ctx.err_buf[0]) {
            /* Escape once, after any truncation: producers format raw
               client text into err_buf. The stack buffer holds the
               mathematical worst case (6 escaped bytes per input byte +
               NUL), so no allocation and no overflow is possible — on
               any escape failure emit a static message, because raw
               client-controlled text must never reach the wire. */
            char esc[sizeof(ctx.err_buf) * 6 + 1];
            int n = json_escape_into(esc, sizeof(esc),
                                     ctx.err_buf, strlen(ctx.err_buf));
            if (n >= 0) esc[n] = '\0';  /* helper writes no terminator */
#ifdef TEST_BUILD
            if (atomic_load(&g_shard_test_json_escape_fail)) n = -1;
#endif
            if (n >= 0) {
                OUT("{\"error\":\"%s\"}\n", esc);
            } else {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "update error escape failed; emitting static fallback");
                OUT("{\"error\":\"update failed\"}\n");
            }
        } else {
            OUT("{\"error\":\"update failed\"}\n");
        }
        return 1;
    }
    if (durability_pending) {
        free(result.current_value);
        free(ctx.ret_buf); ctx.ret_buf = NULL;
        free_criteria_tree(cas_tree);
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }
    free_criteria_tree(cas_tree);

    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    if (return_record && ctx.ret_buf) {
        char *rec = typed_decode(ts, ctx.ret_buf, (uint32_t)ts->total_size,
                                 SREC_FLAG_NULLMAP);
        free(ctx.ret_buf);
        ctx.ret_buf = NULL;
        if (rec) {
            OUT("{\"status\":\"updated\",\"key\":\"%s\",\"record\":%s}\n",
                wire_key, rec);
            free(rec);
            return 0;
        }
    }
    free(ctx.ret_buf);
    ctx.ret_buf = NULL;
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
    return 0;
}
```

<!-- block:edit-21-new -->
```text
static int cmd_update_v2_impl(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *partial_json,
                         const char *if_json, int dry_run,
                         int return_record, const Schema *sc, TypedSchema *ts, SlotcaskDb *borrowed) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *owned SDB_REG_REF = borrowed?NULL:slotcask_registry_get(db_root, object, &info);
    SlotcaskDb *sdb=borrowed?borrowed:owned;
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    if (!ts) { OUT("{\"error\":\"Object not found\"}\n"); return 1; }

    char guard_fields[MAX_FIELDS][256];
    if(ts->packed.nfields&&(if_json||load_index_fields(db_root,object,guard_fields,MAX_FIELDS)>0)){
        OUT("{\"error\":\"packed indexes and conditions await integration\"}\n");return 1;
    }
    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!ts->packed.nfields && !atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* dry_run validates criteria but doesn't write — race-tolerant. It is
       the only remaining user of the up-front slotcask_get snapshot: a
       normal update builds NEW from the lock-protected OLD inside
       v2_update_new_from_old instead, so no stale outside-lock snapshot
       exists for the race to be lost against. */
    if (dry_run) {
        void *old_val = NULL; size_t old_vlen = 0;
        uint8_t old_hdr2 = 0;
        if (slotcask_get_ex(sdb, key, klen, &old_val, &old_vlen, &old_hdr2) != 0) {
            OUT("{\"error\":\"Not found\"}\n");
            return 1;
        }
        TypedRowView dry_view;
        if(typed_row_open(ts,old_val,old_vlen,old_hdr2,&dry_view)){
            free(old_val);OUT("{\"error\":\"invalid OLD generation or value\"}\n");return 1;
        }
#ifdef TEST_BUILD
        /* TEST_BUILD seam retained for dry_run only: fires on dry-run
           updates in the fixed code (normal updates pause at the top of
           v2_update_new_from_old, under_kf_wrlock == 1). */
        slotcask_test_after_old(0);
#endif
        if (if_json) {
            FieldSchema cas_fs;
            memset(&cas_fs,0,sizeof(cas_fs));cas_fs.ts=ts;
        if(!ts->packed.nfields)init_field_schema(&cas_fs, db_root, object);
            char cas_err[256];
            CriteriaNode *cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                                      cas_err, sizeof(cas_err));
            if (!cas_tree && cas_err[0]) {
                LOG_WARN(LOG_SUB_SLOTCASK, "update if rejected: %s", cas_err);
                free(old_val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check_tree(&cas_fs, old_val, (int)old_vlen,
                                      old_hdr2, cas_tree);
            free_criteria_tree(cas_tree);
            if (!pass) {
                char *cur = typed_decode(ts, old_val, (uint32_t)old_vlen, old_hdr2);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(old_val);
                return 1;
            }
        }
        free(old_val);
        char wire_key[1100];
        format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    /* Parse `if` once before the lock-protected callback; check_fn runs
       cas_check_tree under the kf-shard wrlock so the verify + commit are
       atomic against concurrent writers. Note the intentional precedence
       shift: when both the partial field JSON and the `if` criteria are
       invalid, this now reports "invalid if condition" first — the field
       parsing happens inside the callback under the lock. */
    FieldSchema cas_fs;
    memset(&cas_fs,0,sizeof(cas_fs));cas_fs.ts=ts;
        if(!ts->packed.nfields)init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                    cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            LOG_WARN(LOG_SUB_SLOTCASK, "update if rejected: %s", cas_err);
            OUT("{\"error\":\"invalid if condition\"}\n");
            return 1;
        }
        /* cas_tree NULL with empty cas_err = vacuous pass ([]). */
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    V2UpdateCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
        .idx_ts = ts,.db=sdb,.key_len=klen,
        .partial_json = partial_json,
        .cas_tree = cas_tree, .cas_fs = &cas_fs,
        .return_record = return_record,
    };
    compute_hash_raw(key, klen, ctx.hash);
    int durability_pending = 0;

    SlotcaskUpsertOpts opts = {
        .require_existing = 1,
        .check            = v2_update_check_fn,
        .check_ctx        = &ctx,
        .new_from_old_alloc = v2_update_owned,
        .new_from_old_alloc_ctx = &ctx,
        .prepare_commit   = v2_update_prepare_commit,
        .apply_commit     = v2_update_apply_commit,
        .abort_commit     = v2_update_abort_commit,
        .pre_commit_ctx   = &ctx,
        .out_kf_shard     = &ctx.kf_shard,
        .out_kf_slot      = &ctx.kf_slot,
        .has_indexed_fields = nidx > 0,
        .out_durability_degraded = &durability_pending,
        .record_hdr2   = typed_schema_hdr2(ts),
    };
    SlotcaskUpsertResult result = {0};
    uint64_t _commit_t0 = now_us();
    int rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        NULL, 0, &opts, &result);
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        /* Either require_existing fired (record vanished) or check_fn
           rejected (criteria didn't match). Disambiguate by whether the
           result has an old value attached. */
        if (result.was_update && result.condition_not_met) {
            char *cur = result.current_value
                ? single_write_decode(ts, result.current_value, result.current_vlen, result.current_hdr2)
                : NULL;
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                cur ? cur : "null");
            free(cur);
        } else {
            OUT("{\"error\":\"Not found\"}\n");
        }
        free(result.current_value);
        free(ctx.ret_json); ctx.ret_json = NULL;
        free_criteria_tree(cas_tree);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free(ctx.ret_json); ctx.ret_json = NULL;
        free_criteria_tree(cas_tree);
        if (ctx.err_buf[0]) {
            /* Escape once, after any truncation: producers format raw
               client text into err_buf. The stack buffer holds the
               mathematical worst case (6 escaped bytes per input byte +
               NUL), so no allocation and no overflow is possible — on
               any escape failure emit a static message, because raw
               client-controlled text must never reach the wire. */
            char esc[sizeof(ctx.err_buf) * 6 + 1];
            int n = json_escape_into(esc, sizeof(esc),
                                     ctx.err_buf, strlen(ctx.err_buf));
            if (n >= 0) esc[n] = '\0';  /* helper writes no terminator */
#ifdef TEST_BUILD
            if (atomic_load(&g_shard_test_json_escape_fail)) n = -1;
#endif
            if (n >= 0) {
                OUT("{\"error\":\"%s\"}\n", esc);
            } else {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "update error escape failed; emitting static fallback");
                OUT("{\"error\":\"update failed\"}\n");
            }
        } else {
            OUT("{\"error\":\"update failed\"}\n");
        }
        return 1;
    }
    if (durability_pending) {
        free(result.current_value);
        free(ctx.ret_json); ctx.ret_json = NULL;
        free_criteria_tree(cas_tree);
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }
    free_criteria_tree(cas_tree);

    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    if(return_record){
        OUT("{\"status\":\"updated\",\"key\":\"%s\",\"record\":%s}\n",wire_key,ctx.ret_json);
        free(ctx.ret_json);return 0;
    }
    free(ctx.ret_json);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n",wire_key);return 0;
}
static int cmd_update_v2(const char *root,const char *object,const char *key,
    size_t klen,const char *json,const char *condition,int dry_run,
    int return_record,const Schema *sc){
    return cmd_update_v2_impl(root,object,key,klen,json,condition,dry_run,
        return_record,sc,load_typed_schema(root,object),NULL);
}
```

<!-- block:edit-22-old -->
```text
static int v2_update_new_from_old(const SlotcaskOldRecord *old,
                                  uint8_t *out_value, size_t out_capacity,
                                  size_t *out_vlen, void *ctx_ptr) {
    V2UpdateCtx *c = ctx_ptr;
    if (!old || !out_value || !out_vlen || old->vlen > out_capacity ||
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
                                     span, is_computed, &comp[i],
                                     c->err_buf, sizeof(c->err_buf));
        free(span);
        if (rc != 0) return -1;
    }
    const char *names[MAX_FIELDS];
    enum FieldType types[MAX_FIELDS];
    char *values[MAX_FIELDS] = {0}; uint8_t nulls[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        names[i] = c->idx_ts->fields[i].name;
        types[i] = c->idx_ts->fields[i].type;
    }
    int parse_rc = json_get_fields_null_aware(c->partial_json, names,
                         c->idx_ts->nfields, types, values, nulls);
    if (parse_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "malformed JSON escape in field values");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const TypedField *f = &c->idx_ts->fields[i];
        if (f->removed) continue;
        if (comp[i].op) { free(values[i]); values[i] = NULL; continue; }
        if (f->type == FT_VARCHAR && values[i] && strlen(values[i]) > (size_t)f->size - 2) {
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                     f->name, strlen(values[i]), f->size - 2);
            goto reject;
        }
        if (nulls[i] && f->not_null) {
            fmt_not_null_err(f, c->err_buf, sizeof(c->err_buf)); goto reject;
        }
    }
    int compose_rc = update_compose_new_from_old(c->idx_ts, old, out_value,
                                                 values, nulls,
                                                 c->err_buf, sizeof(c->err_buf));
    if (compose_rc == -2)
        goto reject;                     /* validation message in err_buf */
    if (compose_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "corrupt record header on update target");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) { free(values[i]); values[i] = NULL; }
    if (apply_computed_fields(c->idx_ts, old, out_value, comp,
                              c->err_buf, sizeof(c->err_buf)) != 0) return -1;
    update_stamp_auto_update_fields(c->idx_ts, out_value);
    *out_vlen = (size_t)c->idx_ts->total_size;
    if (c->return_record) {
        c->ret_buf = malloc(*out_vlen);
        if (c->ret_buf) memcpy(c->ret_buf, out_value, *out_vlen);
    }
    return 0;
reject:
    for (int i = 0; i < c->idx_ts->nfields; i++) free(values[i]);
    return -1;
}
```

<!-- block:edit-22-new -->
```text
static int v2_update_new_from_old(const SlotcaskOldRecord *old,
                                  uint8_t *out_value, size_t out_capacity,
                                  size_t *out_vlen, void *ctx_ptr) {
    V2UpdateCtx *c = ctx_ptr;
    if (!old || !out_value || !out_vlen ||
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
                                     span, is_computed, &comp[i],
                                     c->err_buf, sizeof(c->err_buf));
        free(span);
        if (rc != 0) return -1;
    }
    const char *names[MAX_FIELDS];
    enum FieldType types[MAX_FIELDS];
    char *values[MAX_FIELDS] = {0}; uint8_t nulls[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        names[i] = c->idx_ts->fields[i].name;
        types[i] = c->idx_ts->fields[i].type;
    }
    int parse_rc = json_get_fields_null_aware(c->partial_json, names,
                         c->idx_ts->nfields, types, values, nulls);
    if (parse_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "malformed JSON escape in field values");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const TypedField *f = &c->idx_ts->fields[i];
        if (f->removed) continue;
        if (comp[i].op) { free(values[i]); values[i] = NULL; continue; }
        if (f->type == FT_VARCHAR && values[i] && strlen(values[i]) > (size_t)f->size - 2) {
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                     f->name, strlen(values[i]), f->size - 2);
            goto reject;
        }
        if (nulls[i] && f->not_null) {
            fmt_not_null_err(f, c->err_buf, sizeof(c->err_buf)); goto reject;
        }
    }
    int compose_rc = update_compose_new_from_old(c->idx_ts, old, out_value,
                                                 values, nulls,
                                                 c->err_buf, sizeof(c->err_buf));
    if (compose_rc == -2)
        goto reject;                     /* validation message in err_buf */
    if (compose_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "corrupt record header on update target");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) { free(values[i]); values[i] = NULL; }
    if (apply_computed_fields(c->idx_ts, old, out_value, comp,
                              c->err_buf, sizeof(c->err_buf)) != 0) return -1;
    update_stamp_auto_update_fields(c->idx_ts, out_value);
    *out_vlen = (size_t)c->idx_ts->total_size;
    return 0;
reject:
    for (int i = 0; i < c->idx_ts->nfields; i++) free(values[i]);
    return -1;
}
static int v2_update_owned(const SlotcaskOldRecord *old,uint8_t **out,
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

```

<!-- block:edit-23-old -->
```text

#ifdef TEST_BUILD
#include "single_write_test.h"
int shard_test_single_insert(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int only_new,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
    (void)root;(void)object;(void)key;(void)klen;(void)json;(void)condition;
    (void)only_new;(void)return_record;(void)sc;(void)ts;(void)db;
    OUT("{\"error\":\"explicit schema route unavailable\"}\n");return 1;
}
int shard_test_single_update(const char *root,const char *object,
    const char *key,size_t klen,const char *json,const char *condition,
    int dry_run,int return_record,const Schema *sc,TypedSchema *ts,SlotcaskDb *db) {
    (void)root;(void)object;(void)key;(void)klen;(void)json;(void)condition;
    (void)dry_run;(void)return_record;(void)sc;(void)ts;(void)db;
    OUT("{\"error\":\"explicit schema route unavailable\"}\n");return 1;
}
#endif

```

<!-- block:edit-23-new -->
```text

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
    return cmd_update_v2_impl(root,object,key,klen,json,condition,dry_run,return_record,sc,ts,db);
}
#endif

```

<!-- block:edit-24-old -->
```text
char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len,
                   uint8_t hdr2) {
    if (!ts || !ts->typed) return NULL;
    const uint8_t *fbase = NULL; const uint8_t *bm = NULL;
    int bm_bytes = 0; size_t field_region_len = 0;
    int render_zeros = 0;
    if (data_len < 0 ||
        typed_record_view(data, (size_t)data_len, hdr2, &fbase, &bm,
                          &bm_bytes, &field_region_len) != 0) {
        errno = EILSEQ;   /* corrupt flagged record — error, not JSON null */
        return NULL;
    }
    render_zeros = (hdr2 & SREC_FLAG_NULLMAP) ? 1 : 0;
    /* Size the outer record-JSON buffer based on actual field types.
       Pre-2026.05.6 this was a flat `nfields * 300` heuristic, which
       silently truncated mid-field on records carrying a large
       varchar (e.g. HN comment text up to 8 KB). For each field:
         "name":<value>, =>  name_len + 4 punctuation bytes
       Plus the value:
         FT_VARCHAR: worst-case 6 * (size - 2) + 2 quotes (escapes can
                     6x-expand control chars per RFC 8259)
         everything else: a generous 64 bytes (long literals, dates,
                     numerics — all fit comfortably) */
    size_t est = 4;  /* { ... } + NUL */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        est += (size_t)ts->fields[i].name_len + 4;
        if (ts->fields[i].type == FT_VARCHAR) {
            int cm = ts->fields[i].size > 2 ? ts->fields[i].size - 2 : 0;
            est += (size_t)cm * 6 + 2;
        } else {
            est += 64;
        }
    }
    char *buf = malloc(est);
    size_t pos = 0;
    if (est > 0) buf[pos++] = '{';
    int first = 1;

    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;  /* tombstoned — not visible to consumers */

        /* For trim-encoded records, fields past the field region are
           zero/default. Use a static zero buffer so decode_field_to_buf
           always has valid input. */
        static const uint8_t zero_field[65537]; /* BSS — always zero-initialised */
        const uint8_t *field_src = ((size_t)f->offset + (size_t)f->size <= field_region_len)
            ? fbase + f->offset
            : zero_field;

        /* NULL bit ⇒ emit JSON null (key present), canonical-NULL bytes
           are an encoder bug (defensive LOG at debug noise level omitted —
           render null regardless). */
        if (typed_field_null(bm, bm_bytes, i)) {
            if (!first && pos + 1 < est) buf[pos++] = ',';
            SB_APPEND(buf, pos, est, "\"%s\":null", f->name);
            first = 0;
            continue;
        }

        /* Stack buffer covers non-varchar fields (numbers / bool / dates
           fit comfortably). Varchar may need 6 * content_max + 2 bytes
           worst-case after JSON-escape expansion (\u00XX per byte); fall
           back to a heap allocation for those. */
        char vbuf_stack[512];
        char *vbuf = vbuf_stack;
        int vbufsz = (int)sizeof(vbuf_stack);
        if (f->type == FT_VARCHAR) {
            int need = 6 * (f->size > 2 ? f->size - 2 : 0) + 3;
            if (need > vbufsz) {
                vbuf = malloc((size_t)need);
                if (!vbuf) { vbuf = vbuf_stack; }   /* falls back; may truncate */
                else        { vbufsz = need; }
            }
        }
        int vlen = decode_field_to_buf(f, field_src, vbuf, vbufsz, render_zeros);
        if (vlen <= 0) { if (vbuf != vbuf_stack) free(vbuf); continue; }

        if (!first && pos + 1 < est) buf[pos++] = ',';
        SB_APPEND(buf, pos, est, "\"%s\":%s", f->name, vbuf);
        first = 0;
        if (vbuf != vbuf_stack) free(vbuf);
    }
    if (pos + 1 < est) buf[pos++] = '}';
    buf[pos] = '\0';
    return buf;
}
```

<!-- block:edit-24-new -->
```text
char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len,
                   uint8_t hdr2) {
    if (!ts || !ts->typed || ts->nfields < 1 || ts->nfields > MAX_FIELDS)
        return NULL;
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
            if (f->size < 2) { errno = EILSEQ; return NULL; }
            size_t n = ((size_t)src[0] << 8) | src[1];
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
        if (typed_field_null(bm, bm_bytes, i)) {
            memcpy(stack, "null", 5);
            len = 4;
        } else {
            int cap = capacities[i];
            if (cap > (int)sizeof(stack)) {
                value = malloc((size_t)cap);
                if (!value) { free(buf); errno = ENOMEM; return NULL; }
            }
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
```

<!-- block:edit-25-new -->
```text
# Single-command typed write integration

Single insert/upsert and partial update construct final NEW through the owning
Slotcask callback. No registry/loader/create-object path binds packed layouts;
only explicit immutable test schemas exercise the packed branch at this stage.
This is preparation, not public numeric/varchar activation or migration.

The callback runs under the kf shard write lock after built-in conditions. An
explicit new_from_old_alloc_on_insert option permits a fresh key with NULL OLD;
its zero default preserves the previous OLD-required owning-hook behavior.

Legacy single writes keep existing encoding, computed operations, CAS and index
semantics. Their owning producers allocate total_size, trim their own final
legacy bytes, and never allocate slot_size scratch. Insert auto_create transfer
now uses the locked OLD rather than a separate pre-lock snapshot.

Packed insert collection uses complete scalar JSON validation and positional
fields. Every known duplicate is validated, the last valid value wins, and
unknown values must still be valid JSON. Ordinary omitted defaults run once;
auto_create is copied from OLD with nullness, or stamped once for a fresh key,
before final NOT NULL validation. Insert auto_update retains the adapter's
existing omitted-default policy; Phase 4 owns any broader policy change.
Packed partial update borrows OLD, overwrites supplied scalar fields, preserves
omissions, applies no insertion defaults, and stamps auto_update after the patch.
JSON escapes are decoded once, including embedded NUL in varchar byte spans.

Packed indexed/conditional writes fail before defaults or writes, and known
computed object/array patches fail before composition. These are private guard
rails until Phases 4–5 integrate them; no public legacy operation is disabled.
The callback validates OLD generation in both schema mismatch directions.
Dry-run validates generation and performs no mutation.

NEW generation comes from the immutable schema. Actual physical size includes
24 header bytes, actual key bytes and 8-byte alignment, and must fit both the
object slot bound and the current segment ceiling. The theoretical schema
maximum may exceed the segment ceiling: small actual records still work.
Single packed insert/update allocates no schema-maximum scratch buffer.

When return_record is requested, its JSON is prepared from final actual NEW
before segment reservation. Allocation/render failure rejects the write;
a successful response cannot silently omit the requested record. The shared
buffered legacy decoder now checks all allocations and rendering results and
sizes varchar/enum buffers from actual values. This also repairs its existing
buffered readers; stream-reader integration remains later work. Slotcask owns
NEW after callback return; command context owns only response JSON and any
unconsumed legacy insertion buffer. Rejections and durability-pending outcomes
free those allocations and use existing transaction/recovery ordering.

Future required integrations remain bulk formats, read/query/index/maintenance
consumers, exact computed operations, public metadata/publication and activation.
A physical private-schema fixture is not a public daemon/SDK acceptance test.

```

<!-- block:edit-26-old -->
```text
gates, once before reservation/pre-commit. Missing OLD and rejected conditions
do not invoke it. It is not a fresh-insert default generator. It must not
re-enter the database or perform network I/O.
```

<!-- block:edit-26-new -->
```text
gates, once before reservation/pre-commit. Rejected conditions never invoke
it. Missing OLD invokes it only with new_from_old_alloc_on_insert=1; otherwise
the previous OLD-required behavior remains. The flag without an owning callback
is EINVAL. A fresh-capable callback must handle NULL OLD explicitly. It must not
re-enter Slotcask or perform network I/O.
```

<!-- block:edit-27-old -->
```text
EM on temporary enum-parser allocation failure.

The [owning write hook](owning-write-hooks.md) supplies a transport-level
actual-size NEW buffer lifetime for future integrations. It is optional and
has no production caller yet. Its presence does not convert public commands,
bulk row ownership, return_record, indexes or criteria to packed storage.

```

<!-- block:edit-27-new -->
```text
EM on temporary enum-parser allocation failure.

The [owning write hook](owning-write-hooks.md) supplies a transport-level
actual-size NEW buffer lifetime for future integrations. It is optional and
has no production caller yet. Its presence does not convert public commands,
bulk row ownership, return_record, indexes or criteria to packed storage.

[Single-command integration](single-write-integration.md) describes the first
production consumers of these helpers, locked insertion/default handling and
actual-size owning updates. Bulk/read/index consumers remain future work;
packed schemas remain inactive in public loaders and creates.

```

<!-- block:edits -->
```json
[
  {
    "stage": 1,
    "path": "src/db/slotcask.h",
    "old": "edit-01-old",
    "new": "edit-01-new"
  },
  {
    "stage": 1,
    "path": "src/db/slotcask.h",
    "old": "edit-02-old",
    "new": "edit-02-new"
  },
  {
    "stage": 1,
    "path": "src/db/single_write_test.h",
    "old": null,
    "new": "edit-03-new"
  },
  {
    "stage": 1,
    "path": "src/db/storage.c",
    "old": "edit-04-old",
    "new": "edit-04-new"
  },
  {
    "stage": 1,
    "path": "src/test/cases/test_single_write_integration.c",
    "old": null,
    "new": "edit-05-new"
  },
  {
    "stage": 1,
    "path": "build.sh",
    "old": "edit-06-old",
    "new": "edit-06-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "edit-07-old",
    "new": "edit-07-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "edit-08-old",
    "new": "edit-08-new"
  },
  {
    "stage": 2,
    "path": "src/db/slotcask.c",
    "old": "edit-09-old",
    "new": "edit-09-new"
  },
  {
    "stage": 2,
    "path": "src/db/typed_row.c",
    "old": "edit-10-old",
    "new": "edit-10-new"
  },
  {
    "stage": 2,
    "path": "src/db/typed_row.c",
    "old": "edit-11-old",
    "new": "edit-11-new"
  },
  {
    "stage": 2,
    "path": "src/db/typed_row.c",
    "old": "edit-12-old",
    "new": "edit-12-new"
  },
  {
    "stage": 2,
    "path": "src/db/typed_row.c",
    "old": "edit-13-old",
    "new": "edit-13-new"
  },
  {
    "stage": 2,
    "path": "src/db/typed_row.h",
    "old": "edit-14-old",
    "new": "edit-14-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-15-old",
    "new": "edit-15-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-16-old",
    "new": "edit-16-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-17-old",
    "new": "edit-17-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-18-old",
    "new": "edit-18-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-19-old",
    "new": "edit-19-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-20-old",
    "new": "edit-20-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-21-old",
    "new": "edit-21-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-22-old",
    "new": "edit-22-new"
  },
  {
    "stage": 2,
    "path": "src/db/storage.c",
    "old": "edit-23-old",
    "new": "edit-23-new"
  },
  {
    "stage": 2,
    "path": "src/db/config.c",
    "old": "edit-24-old",
    "new": "edit-24-new"
  },
  {
    "stage": 3,
    "path": "docs/concepts/single-write-integration.md",
    "old": null,
    "new": "edit-25-new"
  },
  {
    "stage": 3,
    "path": "docs/concepts/owning-write-hooks.md",
    "old": "edit-26-old",
    "new": "edit-26-new"
  },
  {
    "stage": 3,
    "path": "docs/concepts/typed-row-adapter.md",
    "old": "edit-27-old",
    "new": "edit-27-new"
  }
]
```

<!-- block:checkpoints -->
```json
{
  "0": {
    "build.sh": "28681cc82cb5c9e858b333708fd69637f4b5fc5198e25c5eff76af6972df95b3",
    "docs/concepts/owning-write-hooks.md": "d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4",
    "docs/concepts/single-write-integration.md": null,
    "docs/concepts/typed-row-adapter.md": "fb551e31a3b59cda085db556556e4da7fc6cdc32d72c8527e07e3ab04a15cd64",
    "src/db/config.c": "e461acd64dc987a5c0bf6d610d44c7694f3cdfa87eb58a28edc32833cc3ab8fe",
    "src/db/single_write_test.h": null,
    "src/db/slotcask.c": "6836a15715f4ac46808d4acb718f30b14dadde8943242825dbb18270a236fbd0",
    "src/db/slotcask.h": "2cf265d1fa9629a89e08670af2d6401bd3206cfc2b22fb91fb03f43683f128c6",
    "src/db/storage.c": "9ddb16baf992c52798a3d8e51dafb71eea2e28a3870a16af190067e59aad3dce",
    "src/db/typed_row.c": "bb8ff7b923cd3db45c431e3ac37c9ce38f999fc70229cf61cd33ac870c7a37ac",
    "src/db/typed_row.h": "4d19ffa97c167f4953fbf20f6acbb6a471fab80bcc1db641718fb8bd8b9744b0",
    "src/test/cases/test_single_write_integration.c": null
  },
  "1": {
    "build.sh": "a340db74277ce62346a34fbbc260c531b2c25f305aeb8746b26cab50194bdcf4",
    "docs/concepts/owning-write-hooks.md": "d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4",
    "docs/concepts/single-write-integration.md": null,
    "docs/concepts/typed-row-adapter.md": "fb551e31a3b59cda085db556556e4da7fc6cdc32d72c8527e07e3ab04a15cd64",
    "src/db/config.c": "e461acd64dc987a5c0bf6d610d44c7694f3cdfa87eb58a28edc32833cc3ab8fe",
    "src/db/single_write_test.h": "9788c60b2e4336f90fe28c52656470c8c1e8f21a1b5ed9a4d8ad67aae2fa895c",
    "src/db/slotcask.c": "6836a15715f4ac46808d4acb718f30b14dadde8943242825dbb18270a236fbd0",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
    "src/db/storage.c": "ac42a82db455733d71c093cbb3c15da2127ae5d33811728fa92933ea764a385c",
    "src/db/typed_row.c": "bb8ff7b923cd3db45c431e3ac37c9ce38f999fc70229cf61cd33ac870c7a37ac",
    "src/db/typed_row.h": "4d19ffa97c167f4953fbf20f6acbb6a471fab80bcc1db641718fb8bd8b9744b0",
    "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
  },
  "2": {
    "build.sh": "a340db74277ce62346a34fbbc260c531b2c25f305aeb8746b26cab50194bdcf4",
    "docs/concepts/owning-write-hooks.md": "d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4",
    "docs/concepts/single-write-integration.md": null,
    "docs/concepts/typed-row-adapter.md": "fb551e31a3b59cda085db556556e4da7fc6cdc32d72c8527e07e3ab04a15cd64",
    "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
    "src/db/single_write_test.h": "9788c60b2e4336f90fe28c52656470c8c1e8f21a1b5ed9a4d8ad67aae2fa895c",
    "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
    "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
    "src/db/typed_row.c": "fd9c7d70c2bff55a99176af45081ab926a29219e40cb86877604fa0adbc710da",
    "src/db/typed_row.h": "ffe36c203d85461750c8a20523d2acfbf245c1583673b6373474a72f145ba39a",
    "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
  },
  "3": {
    "build.sh": "a340db74277ce62346a34fbbc260c531b2c25f305aeb8746b26cab50194bdcf4",
    "docs/concepts/owning-write-hooks.md": "3f3a54555d926d5b9ac5355efba522d601e5e204b0ffd2d1e6e0d65d0d9c3816",
    "docs/concepts/single-write-integration.md": "e6414f104f81ada5b4c2ea2e527621654de267225fa7e030ac29a61c3609f7b3",
    "docs/concepts/typed-row-adapter.md": "3bd6c2f685a616ff09b31cc7c9b4d7f4c9da2c446aec4aaf2bb498ee22f8f9c9",
    "src/db/config.c": "722958322d182c09305391de43f2b4cf66fb615254c62ecce90504ec81287680",
    "src/db/single_write_test.h": "9788c60b2e4336f90fe28c52656470c8c1e8f21a1b5ed9a4d8ad67aae2fa895c",
    "src/db/slotcask.c": "8fef2c466c99dc26e5ca7eb11655a7c4990a71e5fbaebc3e1cc228a9816d71c5",
    "src/db/slotcask.h": "b25cd4d79e7029330e547b3cf78c2862d61fa05f82bc11d7bc9bb57fd5cc007f",
    "src/db/storage.c": "9434377f8eebbba0dda0f4143f4a767972aecc79ef318f1422a67c1edd864e89",
    "src/db/typed_row.c": "fd9c7d70c2bff55a99176af45081ab926a29219e40cb86877604fa0adbc710da",
    "src/db/typed_row.h": "ffe36c203d85461750c8a20523d2acfbf245c1583673b6373474a72f145ba39a",
    "src/test/cases/test_single_write_integration.c": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67"
  }
}
```

<!-- block:probe -->
```c
#define _XOPEN_SOURCE 700
#include "types.h"
#include "slotcask.h"
#include "typed_row.h"
#include "shard_db.h"
#include <errno.h>
#include <ftw.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/resource.h>
void *probe_malloc(size_t);
void *probe_calloc(size_t,size_t);
void *probe_realloc(void *,size_t);
/* Include the real source verbatim to call its private implementation with
   borrowed fixture dependencies, without a production/testing API switch. */
#ifndef P3B3_RENDER_ONLY
#define malloc probe_malloc
#define calloc probe_calloc
#define realloc probe_realloc
#include "storage.c"
#undef malloc
#undef calloc
#undef realloc
#endif
ShardDb *shard_db_open_internal(const char *);
void __real_free(void *);
static int active,ordinal,fail_at,live,oversized;
static size_t largest;
static void *held[4096];
#define MUST(x) do{if(!(x)){fprintf(stderr,"probe line %d: %s errno=%d live=%d\n",__LINE__,#x,errno,live);exit(1);}}while(0)
static int tracked(void *p){for(int i=0;i<live;i++)if(held[i]==p)return i;return -1;}
static void forget(void *p){int i=tracked(p);if(i>=0)held[i]=held[--live];}
static void remember(void *p){if(active&&p){MUST(live<4096);held[live++]=p;}}
static int allow(size_t n){if(!active)return 1;ordinal++;if(n>largest)largest=n;
    if(n>8u*1024*1024){oversized++;errno=ENOMEM;return 0;}
    if(ordinal==fail_at){errno=ENOMEM;return 0;}return 1;}
void *probe_malloc(size_t n){if(!allow(n))return NULL;void *p=malloc(n);remember(p);return p;}
void *probe_calloc(size_t a,size_t b){if(a&&b>SIZE_MAX/a){errno=ENOMEM;return NULL;}
    if(!allow(a*b))return NULL;
    void *p=calloc(a,b);remember(p);return p;}
void *probe_realloc(void *old,size_t n){if(!allow(n))return NULL;int i=tracked(old);
    void *p=realloc(old,n);if(p){if(i>=0)held[i]=held[--live];remember(p);}return p;}
void __wrap_free(void *p){if(p)forget(p);__real_free(p);}
static int rm(const char *p,const struct stat *s,int k,struct FTW *f){(void)s;(void)k;(void)f;return remove(p);}
#ifndef P3B3_RENDER_ONLY
static uint64_t position(SlotcaskDb *db){SlotcaskStream *s=&db->streams[0];pthread_mutex_lock(&s->rotation_lock);
    uint64_t n=((uint64_t)s->active_file_id<<32)|s->reserve_off;pthread_mutex_unlock(&s->rotation_lock);return n;}
static int invoke(int update,SlotcaskDb *db,TypedSchema *ts,Schema *sc,const char *root,const char *key,int fail){
    FILE *save=g_out,*sink=tmpfile();MUST(sink);g_out=sink;MUST(!live);ordinal=0;largest=0;oversized=0;fail_at=fail;active=1;
    char legacy_json[1100];memcpy(legacy_json,"{\"f0\":2,\"f1\":\"",14);
    memset(legacy_json+14,'x',1000);memcpy(legacy_json+1014,"\"}",3);
    const char *json=ts->packed.nfields?"{\"f0\":2,\"f1\":\"changed\"}":legacy_json;
    int rc=update?cmd_update_v2_impl(root,"obj",key,strlen(key),json,NULL,0,1,sc,ts,db):
        cmd_insert_v2_impl(root,"obj",key,strlen(key),json,NULL,0,1,sc,ts,db);
    active=0;g_out=save;fclose(sink);MUST(!live);MUST(!oversized);return rc;
}
static TypedSchema *schema(int maximum){TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=maximum?256:4;
    const char *tokens[256];for(int i=0;i<ts->nfields;i++){
        tokens[i]=maximum?"varchar":i==0?"numeric:26,10":i==1?"varchar":"timestamp";
        ts->fields[i].type=maximum||i==1?FT_VARCHAR:FT_NUMERIC;
        if(!maximum&&i>=2)parse_field_type(tokens[i],&ts->fields[i]);
        snprintf(ts->fields[i].name,sizeof(ts->fields[i].name),"f%d",i);ts->fields[i].name_len=(int)strlen(ts->fields[i].name);}
    MUST(!typed_schema_bind_packed(ts,tokens,(size_t)ts->nfields,NULL,0));
    if(!maximum){ts->fields[2].default_kind=DK_AUTO_CREATE;ts->fields[3].default_kind=DK_AUTO_UPDATE;}return ts;}
#endif
static TypedSchema *legacy_schema(void){
    TypedSchema *ts=calloc(1,sizeof(*ts));MUST(ts);ts->typed=1;ts->nfields=2;
    ts->nullmap_prefix=2;ts->total_size=1032;
    ts->fields[0].type=FT_INT;ts->fields[0].size=4;
    strcpy(ts->fields[0].name,"f0");ts->fields[0].name_len=2;
    ts->fields[1].type=FT_VARCHAR;ts->fields[1].size=1026;ts->fields[1].offset=4;
    strcpy(ts->fields[1].name,"f1");ts->fields[1].name_len=2;return ts;
}
static int legacy_render_failures(void){
    int failures=0;
    for(int which=1;which<=2;which++){
        fflush(NULL);pid_t pid=fork();MUST(pid>=0);
        if(!pid){
            struct rlimit core={0,0};setrlimit(RLIMIT_CORE,&core);
            TypedSchema *ts=legacy_schema();uint8_t bytes[1032]={1,0};
            bytes[6]=3;bytes[7]=232;memset(bytes+8,'x',1000);
            ordinal=0;fail_at=which;live=0;oversized=0;active=1;
            char *s=typed_decode(ts,bytes,sizeof(bytes),SREC_FLAG_NULLMAP);
            active=0;
            if(s){free(s);free(ts);_exit(2);}
            if(live||errno!=ENOMEM){free(ts);_exit(3);}
            free(ts);_exit(0);
        }
        int status=0;MUST(waitpid(pid,&status,0)==pid);
        if(WIFEXITED(status)&&WEXITSTATUS(status)==0)
            printf("legacy decoder allocation %d: NULL/ENOMEM, tracked live=0\n",which);
        else{
            failures++;
            if(WIFSIGNALED(status))printf("legacy decoder allocation %d: unexpected signal %d\n",which,WTERMSIG(status));
            else printf("legacy decoder allocation %d: incomplete/error result, exit %d\n",which,WEXITSTATUS(status));
        }
    }
    printf("legacy decoder failure checks: %d passed, %d failed\n",2-failures,failures);
    return failures?1:0;
}

#ifdef P3B3_RENDER_ONLY
int main(void){
    char root[]="/tmp/shard-render-red-XXXXXX";MUST(mkdtemp(root));
    ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    int rc=legacy_render_failures();shard_db_close(owner);
    MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return rc;
}
#else
int main(int argc,char **argv){
    char root[]="/tmp/shard-single-probe-XXXXXX";MUST(mkdtemp(root));ShardDb *owner=shard_db_open_internal(root);MUST(owner);
    int decoder_rc=legacy_render_failures();
    if(argc==2&&!strcmp(argv[1],"--legacy-render-only")){
        shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return decoder_rc;}
    MUST(!decoder_rc);
    for(int maximum=-1;maximum<=1;maximum++){
        TypedSchema *ts=maximum<0?legacy_schema():schema(maximum);size_t bound=0;
        if(maximum<0)bound=(24+32+(size_t)ts->total_size+7)&~(size_t)7;
        else MUST(pkr_slot_bound(&ts->packed,32,&bound)==DEC_OK);
        Schema sc={.splits=1,.streams=1,.slot_size=(int)bound,.max_key=32,.max_value=ts->total_size};
        char objroot[256];snprintf(objroot,sizeof(objroot),"%s/p%d",root,maximum);MUST(!mkdir(objroot,0700));
        SlotcaskDb db;MUST(!slotcask_open(&db,objroot,1,1,(int)bound));
        uint8_t *seed=NULL;size_t n=0;char err[256];MUST(!typed_encode_alloc(ts,"{\"f0\":1,\"f1\":\"old\"}",1,NULL,NULL,&seed,&n,err,sizeof(err)));
        SlotcaskUpsertOpts o={.record_hdr2=typed_schema_hdr2(ts)};
        char names[MAX_FIELDS][256];enum IndexType types[MAX_FIELDS];load_index_fields(root,"obj",names,MAX_FIELDS);load_index_types(root,"obj",types,MAX_FIELDS);
        for(int mode=0;mode<3;mode++){
            const char *key=mode?"a":"new";
            if(mode)MUST(!slotcask_upsert_with_hooks(&db,-1,key,strlen(key),seed,n,&o,NULL));
            MUST(invoke(mode==2,&db,ts,&sc,root,key,0)==0);int count=ordinal;
            if(maximum>0){printf("max schema mode %d: largest selected allocation %zu, live=0\n",mode,largest);continue;}
            MUST(slotcask_delete_with_hooks(&db,key,strlen(key),NULL,NULL)==0);
            for(int i=1;i<=count;i++){
                if(mode)MUST(!slotcask_upsert_with_hooks(&db,-1,key,strlen(key),seed,n,&o,NULL));
                uint64_t pos=position(&db),total=0,deleted=0,t2=0,d2=0;MUST(!slotcask_sum_kf_totals(&db,&total,&deleted));
                MUST(invoke(mode==2,&db,ts,&sc,root,key,i)==1);MUST(position(&db)==pos);
                MUST(!slotcask_sum_kf_totals(&db,&t2,&d2));MUST(total==t2&&deleted==d2);
                void *b=NULL;size_t bn=0;uint8_t h=0;int rc=slotcask_get_ex(&db,key,strlen(key),&b,&bn,&h);
                if(mode){MUST(rc==0&&bn==n&&h==typed_schema_hdr2(ts)&&!memcmp(b,seed,n));free(b);}else MUST(rc!=0);
            }
            printf("layout %d mode %d: %d allocation failures preserve OLD/absence, counts, segment, live=0\n",maximum,mode,count);
        }
        slotcask_close(&db);free(seed);free(ts);
    }
    invalidate_idx_cache(root,"obj");shard_db_close(owner);MUST(!nftw(root,rm,16,FTW_DEPTH|FTW_PHYS));return 0;
}

#endif

```

<!-- block:driver -->
```python
from pathlib import Path
import sys,json,re,hashlib

def main():
    plan=Path(sys.argv[1]).read_text();stage=int(sys.argv[2]);root=Path.cwd().resolve()
    if stage not in (1,2,3):raise ValueError("invalid stage")
    pairs=re.findall(r"(?ms)^<!-- block:([a-z0-9-]+) -->\n\x60{3}[^\n]*\n(.*?)\n\x60{3}(?=\n|$)",plan)
    blocks=dict(pairs)
    if len(pairs)!=len(blocks):raise ValueError("duplicate block")
    digest=lambda b:hashlib.sha256(b).hexdigest()
    for name,wanted in json.loads(blocks["checksums"]).items():
        if digest(blocks[name].encode())!=wanted:raise ValueError("block checksum: "+name)
    checkpoints=json.loads(blocks["checkpoints"])
    for rel,wanted in checkpoints[str(stage-1)].items():
        path=root/rel
        if path.is_symlink():raise ValueError("symlink: "+rel)
        got=digest(path.read_bytes()) if path.exists() else None
        if got!=wanted:raise ValueError("checkpoint: "+rel)
    pending={}
    for edit in json.loads(blocks["edits"]):
        if edit["stage"]!=stage:continue
        rel=edit["path"];path=root/rel
        if not path.resolve().is_relative_to(root):raise ValueError("outside root")
        text=pending.get(rel,path.read_text() if path.exists() else None)
        if edit["old"] is None:
            if text is not None:raise ValueError("new file exists: "+rel)
            pending[rel]=blocks[edit["new"]]
        else:
            old=blocks[edit["old"]]
            if text is None or text.count(old)!=1:raise ValueError("anchor: "+rel+" "+edit["old"])
            pending[rel]=text.replace(old,blocks[edit["new"]])
    for rel,text in pending.items():
        if digest(text.encode())!=checkpoints[str(stage)][rel]:raise ValueError("result: "+rel)
    for rel,text in pending.items():(root/rel).write_text(text)
    print("stage",stage,"applied",len(pending),"files via unique anchors")
try:main()
except Exception as exc:
    note=Path("PLAN_NOTES.md")
    if not note.exists():note.write_text("Phase 3B.3 halted\n\n"+str(exc)+"\n")
    print("HALT:",exc,file=sys.stderr);sys.exit(1)

```

<!-- block:checksums -->
```json
{
  "edit-01-old": "bea6f6b30f846814e5732cc389b54ddd0f538e7f2160b58d004c7779c9769439",
  "edit-01-new": "8031d1b7987fed127a30bed8f148c586a08b2fd459439090fb8ec788e1e1243b",
  "edit-02-old": "728b25d876d38d869ed66c9759f867b35c23ac308edd8307b4a9d36e423b6a12",
  "edit-02-new": "6dc80ae4470542b515c17628a7c9cbc4f58212416fb80ed61830a5f3e0b0d70c",
  "edit-03-new": "9788c60b2e4336f90fe28c52656470c8c1e8f21a1b5ed9a4d8ad67aae2fa895c",
  "edit-04-old": "4f7c79b0b61fdf548311b2fcbffab74b3f84dc4f8836d88e46c5512a238a7a88",
  "edit-04-new": "0add6d8cf53cc9727238312d4e6261570717f30629e2932af0e1d5f118a11135",
  "edit-05-new": "b703f809fddb5080bf093784c7b5f457f45c9c26f681e1e722beff90b4267d67",
  "edit-06-old": "d3f6eed5e383c04f8ff2738ece715518886556ed44527f0b77a0f9d26aa7c7cd",
  "edit-06-new": "7e868f348b324d32cc0fb59d952643b7d8a918eaa1376ca49f9cfbbe6a85a651",
  "edit-07-old": "565ed8602a11448ea5e4379f8a5c63b6c4c7976f87d68c9bb852e93f1d2cb08b",
  "edit-07-new": "d2890ea5522e96cc8cd0a3b068d27ac46acafadf058f345ecc29aa06945c151b",
  "edit-08-old": "f271c48e42c2de0b511b875ea2783ad6be80ad11ade5fa22cafe8aabde58c1c7",
  "edit-08-new": "9ce40b40d5664043e26ecea181b3e02c02f47aa5a3a98a1c4a7d70dd16e3e4fc",
  "edit-09-old": "a37ea9668bc5b8091190f3e8aca879c29f3478027e11f2f5910a2dd58f566b41",
  "edit-09-new": "3d909c815eff3643cd104af3fd7a3abb2ada89e7b8d9214b132744a4a4c97c07",
  "edit-10-old": "6087401ffb33c8ee44ecd34fb1186ab186df140d6545a73cb3c095123b4e28ee",
  "edit-10-new": "12a5544eccb1f9b59d7fe8c97b30a808184e52ca2a76cd5c9982affb144618ae",
  "edit-11-old": "c6a6ffb698c4c8925096690e05dfd2d99429efe8f7280ed1adecff7c8d8b8d3b",
  "edit-11-new": "c01ed594178d1033624c6027c03292bb4af6a7f4fb0cd65db085acf7acf0181f",
  "edit-12-old": "eea6a2658eeae9c11bbff4627d50fb3b78c965be71c69188ced9a3b9fc13b6af",
  "edit-12-new": "5e53339efdecb8576305b3875c047dafb5c88dfc4a1c3c60e0ccbfddcc99ceb9",
  "edit-13-old": "031a8a8ef770d09cca710d288ffeeb6b6a3e4359bcbab2a8c48ef917c62a00dc",
  "edit-13-new": "4e817262571166eff9772766bf40f4ae46c5aadeaf81dd9ca3b1e883479e5081",
  "edit-14-old": "406c6ae89ac75db2d6e5a26e007ef0f708bac77eb46a1b4e8d9eac5ef5fc52d1",
  "edit-14-new": "41d5e709340d0fb4c3f9b99bdf5048e7db78699930faefd539bdd4c6b03db9b8",
  "edit-15-old": "a89d242c7b1698aa43ba170aa51206a681602368cc2d5ad0a105d98f0edf077b",
  "edit-15-new": "e3f0f9167f7edb8eb47355f74797047e9c5ed5cfedf252c9d37703e60cfb2ffd",
  "edit-16-old": "42a4c162a1962ce4ef6d11125c89e47c12b38e876baea2399bc05a1bdcc11867",
  "edit-16-new": "8a27a6a79255f5488569fafecffef98cb41320e74ef0a49dd74e8df9d3caea28",
  "edit-17-old": "93de4f767c74e6e239291ae2fee117bd42fcac8cb58f029117232fb271646d9c",
  "edit-17-new": "9bfe62ddee7fd9fe9a85a4415d44e85dbbdbbd973f8bc623494c35bcdbafadd4",
  "edit-18-old": "561903704c49cfb0c91e06dfb8f5ce4f382ea7c0d394988b52a9b162cce16876",
  "edit-18-new": "ddb69376517a32fb2e490c68fa72dd8992b5464c3c76490f76c0ff8df481c0d0",
  "edit-19-old": "5b16c1cad8fde4e57b54668946a0be15026f6a3422259b6ce285caa5f34269fe",
  "edit-19-new": "8d890231b079e9799bf55927d2f1ce46a881322574428b2203a005220b68a487",
  "edit-20-old": "174506d224fb2f19e544aebe4062a735d29a8635803fefdebff259103bae871a",
  "edit-20-new": "9e38b9e9db3be4360783930fa06566f59be98a09bb09c935486e05242cde1ad1",
  "edit-21-old": "1dfaa29551b3c4d512b136f3a7b2d7c871b2df3cbec4f07cebeb0324bf61fec4",
  "edit-21-new": "21ebbfbe0b0738b521e77f9efe9c9f61fb8cf5dee021bf3cf263db51aea438ec",
  "edit-22-old": "f3f1b48b08140e27c21766cef019e9a46eda6dfa87027e73b705867adf7ece08",
  "edit-22-new": "a868e9f2a689001f54352b2578edd238e04f8c2ee52fae3198007c862b93ac59",
  "edit-23-old": "bb3a44dd155317f3465ed037f7f98e4968c28e5c4fc053307393ddaff6cb6fa8",
  "edit-23-new": "87533b103936877e117c27770aa6ab7e5ebb72cedcce458201cf2431f2380bf1",
  "edit-24-old": "1d6996db79d607ee47e574ce4314fa9ef1fe6aa8bb5bd0e540e6b3282bc5c680",
  "edit-24-new": "967eb47d3fda946b6f0f9324183616a9be3159b361f2324cc9f74b9fb6a79d0e",
  "edit-25-new": "e6414f104f81ada5b4c2ea2e527621654de267225fa7e030ac29a61c3609f7b3",
  "edit-26-old": "0f3cc67be8278f7c74b7936659ee29a9a2194f44ee7bbcd86b60017933fc3553",
  "edit-26-new": "75a1d0647936b40e3362b07c485ea3aac947a67e37fad298093b818b0e790923",
  "edit-27-old": "351d2ddca96da656692a68dfdfae77c9632d2ab1a81c12c6939ef555a47603bd",
  "edit-27-new": "06e9dd4fe5b6f02475a3b1d12a3961ca8bcde499acc0d948683c33795ec51cb2",
  "edits": "ed79afe8fd773fb66d391763dc34c66cd1629925d5ba470aed4dfa4737504e8e",
  "checkpoints": "1418d94758571abf08031e594408379f64c5b8ac2e8264c8137c9010eeb82206",
  "probe": "a4dc2c38fdd2ae17afd346c523a9e320aa20aca741d575f5ebb36172a20926e6",
  "driver": "60f4777221a5bd2f5b4b5e583304a32f0ade43429b11f9b247f36d64e548d975"
}
```
