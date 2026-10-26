# Phase 3 — packed-record work index

## Primary goal

Store exact finance decimals and variable-length varchar compactly while keeping
existing applications working throughout the roadmap.

## Status and approved decisions

2026-10-25. The original one-shot child plan was rejected. Its executable hunks
are withdrawn, not instructions for an executor to follow.
The user approved two repair decisions:
1. Keep packed storage inactive until the required integrations are ready.
2. Split Phase 3 into three separately reviewed slices:3A codec/physical tests,
   3B consumer integration,3C metadata/publication safety.

This index is a design/issue ledger, not an implementation plan. Approval of
the split does not approve the new code, all old D1–D9 decisions, or activation.

- [Approved 3A child](2026-10-25-phase3a-packed-codec-plan.md): implemented and
  reviewed, with packed storage still inactive.
  [Execution evidence](2026-10-25-phase3a-packed-codec-evidence.md) and
  [raw-diff review](2026-10-25-phase3a-raw-diff-review.md) record completion.
- 3B: consumer integration remains open. The [3B.1 shared row adapter](2026-10-25-phase3b1-typed-row-adapter-plan.md)
  is implemented and reviewed against committed 3A (`b9cea93`), including the
  post-execution enum fix. [Execution evidence](2026-10-25-phase3b1-typed-row-evidence.md)
  and [raw-diff review](2026-10-25-phase3b1-raw-diff-review.md) record completion:
  256 focused checks, 19072 full-suite assertions across 519 cases, and all 56
  allocation injections pass. It converts no production consumers; write,
  read/index and rebuild children remain required before 3B closure.
  The [3B.2 owning write callback](2026-10-25-phase3b2-owning-write-hook-plan.md)
  is implemented and raw-diff reviewed: 153 focused checks, 19225 full-suite
  assertions across 520 cases, and all eight ownership/OOM/limit cases pass.
  [Execution evidence](2026-10-25-phase3b2-owning-hook-evidence.md) and
  [executed raw-diff review](2026-10-26-phase3b2-raw-diff-review.md) record completion.
  Production command/bulk callers remain unchanged; those conversions remain
  required 3B children. Packed storage remains inactive.
  The [3B.3 single-command write integration](2026-10-26-phase3b3-single-write-integration-plan.md)
  is implemented and raw-diff reviewed: 240 focused checks, 19465 full-suite
  assertions across 521 cases, decoder OOM and 33 write-failure injections pass.
  [Execution evidence](2026-10-26-phase3b3-single-write-evidence.md) and
  [raw-diff review](2026-10-26-phase3b3-raw-diff-review.md) record completion.
  Committed as `a97f9f2`; packed storage remains inactive.
  The [3B.4 packed bulk insert/upsert](2026-10-26-phase3b4-packed-bulk-insert-plan.md)
  is implemented and raw-diff reviewed: 178 focused checks, 19643 full-suite
  assertions across 522 cases, all 90 allocation-failure injections pass.
  [Execution evidence](2026-10-26-phase3b4-packed-bulk-insert-evidence.md) and
  [raw-diff review](2026-10-26-phase3b4-raw-diff-review.md) record completion.
  Review corrected one evidence-directory reference; implementation matches
  all approved checkpoints. Committed as `03617a8`.
  The [3B.5 keyed bulk partial updates](2026-10-26-phase3b5-packed-bulk-update-plan.md)
  are implemented and raw-diff reviewed: 235 focused checks, 19878 full-suite
  assertions across 523 cases, all 112 allocation injections and 19 outcome
  gates pass. [Execution evidence](2026-10-26-phase3b5-packed-bulk-update-evidence.md)
  and [executed raw-diff review](2026-10-26-phase3b5-raw-diff-review.md) record
  completion. All eight owned files match the approved checkpoints. Explicit
  packed criteria/CAS/index guards and four legacy crash repairs are included.
  Committed as `35e49ab`.
  The [3B.6 whole-record JSON readers](2026-10-26-phase3b6-whole-record-readers-plan.md)
  are implemented and raw-diff reviewed: 167 focused checks, 20045 full-suite
  assertions across 524 cases, all 22 allocation injections and 106 probe gates
  pass. [Execution evidence](2026-10-26-phase3b6-whole-reader-evidence.md) and
  [executed raw-diff review](2026-10-26-phase3b6-raw-diff-review.md) record
  completion. All nine owned files match the approved checkpoints; the two
  evidence wording corrections do not change implementation. The legacy GET
  response reserve and temporary field workspace are explicitly documented.
  Field/projection/CSV, read/index/delete consumers, metadata, migration and
  activation remain open; packed schemas stay publicly inactive.
  No Phase 3B closure is implied.
- 3C: child plan must be prepared against the reviewed 3B result.
- [Original review](2026-10-23-phase3-packed-records-plan-review.md): unchanged.
- [Rejected draft archive](archive/2026-10-23-phase3-packed-records-rejected.md):
  byte-identical historical copy, SHA256
  24006910c6b7445fbc08d5c355412be99a13d1f776bed8c59fe29e92fe7b59c1.
  **Never execute it.** It contains known corruption/overflow and missing hunks.
- Parent: [recovery plan](2026-10-19-type-correctness-recovery-plan.md).

Protected certification plan remains separate later work, hash-only access:
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
Do not read/edit/move/stage/incorporate/execute it.

## 3A — codec and physical-record tests

**How this serves the primary goal:** prepares exact compact bytes without
routing applications through partially supported storage.

The child supplies checked declarations, schema-order cells, actual sizing,
bounded composition, validated packed views/spans and exact decimal assignment.
Fixed bytes stay in the directory; packed values use8-byte descriptors.
Bare varchar cap 4 MiB; bare numeric retains 131072/16383 digit limits.
The physical test uses real Slotcask APIs, all 4 MiB content, actual segment vlen,
compact offsets, strict walk/reopen and free-slot reuse/splitting at the maximum
schema bound. The 128 MiB segment cap stays unchanged; actual-row fit is distinct
from theoretical schema maximum. Invalid generation flags are rejected before
incoming upserts stage any rows; computed NEW flags are rechecked before fallback emission.

No TypedField/TypedSchema, loader/create-object, default, query, index, aggregate
or public write integration occurs. No migration or public activation. This
does not yet unblock the finance application.

## 3B — consumer integration, still inactive

**How this serves the primary goal:** makes every reader/writer understand
validated spans and actual sizes before any public object uses packed storage.

Each approved 3B child must provide complete code and test hunks, not prose
for an executor to fill in. The completed first child supplies private shared
adapters, explicit schema/view ownership, actual-size row preparation and bounded
rendering. Production consumers stay legacy and unconverted. Later children
must close the full inventory below; 3C cannot treat 3B.1 alone as completion.
The full 3B inventory includes:
- typed_record_view/typed field access/decode APIs and all31 current view calls:
  config,index,query,aggregate,bulk,find,join,plan,schema,storage translation units;
  repeat the search at the actual new HEAD, not these historical counts alone.
- Raw offsets/field-size buffers/zero-field substitutions, legacy short-tail
  materialization and removed-field slots, null bitmap semantics, enum ownership.
  Legacy zero substitution never backs a73744byte numeric payload.
- Index builder inputs, joins, criteria, sort/group extraction and maintenance
  readers: consume validated spans or fail safely in private unsupported
  integrations, while existing public legacy behavior remains unchanged.
  No descriptor may be interpreted as an eight-byte legacy numeric.
- One actual-size/bounded composition adapter for insert/update/default/transfer,
  all four update_compose callers, adapter NEW allocation, all bulk formats,
  row arenas/slabs and rebuild. No schema-max temporary allocations anywhere.
  Sizing and writing share semantics; capacities are actual destination bounds.
- Separate dec_t precision assignment through the 3A core. Input collection
  resolves schema order before emission; explicit duplicate-key policy, sparse
  defaults, reversed JSON keys and empty numeric rejection need route tests.
- Both schema/record mismatch directions fail closed before field access.
  Legacy trimming/partial-tail/null behavior is preserved, not replaced by
  arbitrary whole-field zeros.
- Ownership, allocation failure and error propagation through every callback,
  including per-row bulk outcomes and record/index/count preservation.
  Fixed-width field validation remains existing semantics until its owning
  roadmap phase changes it.

Testing must be achievable while metadata activation is absent: complete real
adapter/helper fixtures may construct explicit schemas; any proposed testing
seam needs complete code, scoped lifecycle and review. Do not add a production
global switch, silently activate creates, weaken existing tests or pretend a
standalone codec test proves JSON/public route integration. The child must
state precisely which end-to-end tests await 3C/activation and retain them as
required gates, not mark them done.

Exact criteria/index numeric semantics remain Phase5, aggregates Phase6,
write/default completion Phase 4. 3B must define safe integration seams without
claiming those later semantics are delivered. No mid-roadmap public refusals
replace existing working numeric operations.

## 3C — authoritative metadata and publication safety, still inactive

**How this serves the primary goal:** prevents a crash, reload or older binary
from treating packed bytes as legacy values.

A separate approved child must fully resolve:
- Authoritative exact tenant/object identity at the GLOBAL schema manifest;
  bind layout before parsing fields. No substring search, wrong effective-root
  lookup, generation fallback on errors, or silently skipped packed declaration.
  Check cache reload/negative-cache/error behavior and duplicate object names
  across two tenants.
- Persisted metadata/version and record-generation compatibility, normal loader,
  strict startup parser, describe/export/import, old-binary downgrade barrier,
  crash-before-root-stamp window, and release-version decision.
- Checked publication ordering: declarations and required object files durable
  before advertised schema, checked open/write/close/sync/rename/directory-sync,
  cache visibility only after success, recoverable/closed partial publication.
  EIO/ENOSPC/access failure must not return success. Fault/crash tests prove
  every new multistep state; adding unchecked fsync is insufficient.
- Raw transport propagation through rebuild/vacuum and KFM2 recovery; force
  pending durable marker at an existing hook, assert marker before kill/reopen,
  verify actual replay and typed/index/count consistency. A successful write
  followed by restart is not replay evidence.
- Maximum record policy:256 bare-varchar maximum is1073743905 value bytes and
 1073744960 slot bound, above 128 MiB. Propose explicit over-segment rejection
  before mutation, including request-cap configuration and real valid large
  requests in tests; obtain approval for any alternative. No cap raised here.
- Full explicit varchar:N range1..4194304 in public validation/loader;
  bare/bounded/currency/P/S identity survives describe/export/reload.
  No legacy N+2 convenience may erase declaration metadata.
- Eligibility/publication evidence stays inactive. Do not make new objects
  packed until a later explicit activation plan has complete Phase 4–6 support,
  application acceptance, compatibility notes and approval.

D1–D9 from the rejected draft are not blanket-approved. In particular the
all-new-packed policy is superseded by the approved inactive policy. Interim
numeric/index/aggregate/computed-update refusals must not break public legacy
behavior. Metadata slot3, declaration-edit restrictions, fsync strategy,
release bump and describe/export details must be re-derived/reviewed in 3C.

## Original review disposition

The 3A fixes below are implemented and reviewed. They prepare internal packed
storage; they do not activate public packed objects or complete the later slices.
Remaining portions have explicit owners and remain approval blockers for those
slices and activation.

|Finding|3A disposition|Still required|
|---|---|---|
|STD-01 missing implementations|Complete 3A source/stub/tests/unique replacements|Complete 3B/3C implementations; old placeholders withdrawn|
|STD-02 invented test/API shapes|Real TEST_REGISTER/t_ctx/Slotcask signatures|Real daemon/adapter fixtures in owning slice|
|STD-03 early activation|Removed; legacy loaders/create unchanged|Explicit later activation dependency gate|
|STD-04 breaking fresh-object tests|Removed all-new-packed switch; existing tests preserved|Full unchanged suite every completed slice|
|STD-05 fake oracles/max tests|Exact bytes/vlen/capacity/4 MiB/max-schema/reuse assertions|Valid bulk records JSON, reindex success oracle, request-cap/over-segment tests 3B/3C|
|STD-06 replay not forced|Not claimed|Forced pending durable marker+kill/reopen 3C|
|SPEC-01 wrong root/identity|No metadata resolver introduced|Exact authoritative root+tenant+object 3C|
|SPEC-02 parse before binding|No legacy loader/parser changed|Layout bound before parsing, no skipped fields 3C|
|SPEC-03 decimal alias|Separate parsed/rounded decimals; constrained/currency tests|All future adapters reuse safe core 3B/4|
|SPEC-04 fixed directory corruption|Fixed entry copies separate from packed tails|Adapter regressions 3B|
|SPEC-05 JSON-order payloads|Positional schema-order codec|JSON collection/default/duplicate policy 3B|
|SPEC-06 actual-capacity overwrite|Actual-cap codec/canary/failure contract|Update adapter actual allocation/capacity 3B|
|SPEC-07 max allocations|Measured actual records; max arithmetic without huge buffer|Every existing consumer/arena/slab/rebuild 3B|
|SPEC-08 generation mismatch|Packed view requires3; raw writers/scanners allow 0/1/3|Schema-aware legacy/packed mismatch both directions 3B/3C|
|SPEC-09 unchecked publication|No publication change|Checked durable state machine/fault/crash tests 3C|
|SPEC-10 explicit varchar1m cap|Caller-specific checked 4 MiB parser/tests|Public grammar/loader 3C|
|SPEC-11 empty numeric|Codec rejects empty; zero realpayload|Single/bulk/default/NOT NULL routes 3B/4|
|Arithmetic note|One-field and256 field bitmap/directory separated|Keep recalculating actual vs bound/key widths|

## Hand-off order

1. Review/approve 3A candidate, then executor captures RED/implements/GREEN and
   stops uncommitted for raw-diff and blind review.
2. After review cleanup, commit only on separate explicit instruction; no
   probes/logs/debug/cache files. Prepare 3B plan against the resulting checkpoint.
3. Repeat plan→approve→execute→raw-diff review for 3B, then 3C.
4. Finish required later roadmap integrations and explicit activation acceptance.
   Branch-series sanitizers run only after all fixes, immediately before push,
   per AGENTS.md. No automatic push/PR/merge is authorized.

No 3B/3C code, metadata state machine, or public activation is implied by this
index. Keeping open work visible is intentional; it avoids another oversized
plan claiming tests or semantics it cannot execute.
