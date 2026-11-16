# Phase 3 packed-records child plan — review

Date: 2026-10-23. Baseline: `8c0bdf6259d2dc33e540e28e82b9f8010750047d`.
Reviewed plan SHA256: 24006910c6b7445fbc08d5c355412be99a13d1f776bed8c59fe29e92fe7b59c1.

**Disposition: do not approve for execution.** This needs a complete revision,
not just sign-off on D1–D9. The proposed implementation has reproducible value
corruption, invalid decimal calls, a buffer-overwrite path and missing runnable
hunks/tests. No Phase 3 production work was performed during this review.

Independent standards/executability, spec and on-disk/crash-safety reviews were
run separately. Findings are grouped below by axis; overlapping observations
are consolidated within each axis, not ranked against the other axis.

## Standards / executability

### STD-01 [P1] Replace missing implementations and placeholders with complete hunks

Plan lines 1060–1065, 1227–1230, 3102–3108, 3297–3357, 3443–3513,
3794–3837 and 3909–3957. CORE-PROCESS requires complete function/hunk blocks,
all signature consumers and unique quoted anchors. Instead the plan leaves
`typed_schema_cache_put`, `type_of_spec`, sizing-only update behavior,
`criteria_packed_numeric_leaf`, `typed_compose_from_cells`,
`typed_record_composed_len` and multiple reader/caller bodies to prose.
Some code blocks contain literal ellipses and
`return <that command's failure return>`. The rebuild block also declares
`buf` twice in one scope (3916,3926). These cannot be applied literally or
compile. Supply complete before/after blocks, definitions, declarations,
actual callback contexts and all call sites, including slab strides.

### STD-02 [P1] Reconcile new tests with the actual APIs and harness

Plan lines 527–647, 1277–1278, 2598–2673, 3692 onward and 4055.
The unit tests pass `const char **specs` to a function declared/implemented as
`typed_layout_v3_compute(..., const TypedField *fields, ...)`. Type parsing
passes strings such as `n:numeric` although the proposed parser expects the
type token `numeric`; bare numeric also expects size DEC_PAYLOAD_MAX while
the implementation sets size 8. Daemon tests include nonexistent
`test_fixtures.h` and initialize nonexistent `TestClientCfg.db_root`; the
repository uses `fixtures.h` and a port-based client configuration. Several
full-file blocks use `main` with an undefined registered `*_run` symbol.
Provide literal final test files and compile them against the real headers.

### STD-03 [P1] Move activation after support exists

Plan lines 1162–1195 activate slot 3 for every create-object in Task 1, but
`load_schema` still rejects it until Task 5. Task 1.6 promises a green full
suite (1243–1253), including existing cases that create an object then use it.
That state cannot be green. Stage inactive layout APIs first, or move the
complete activation dependency group together; publish achievable per-task
RED/GREEN expectations.

### STD-04 [P1] Preserve legacy coverage while testing interim packed refusals

D2 makes every newly created object packed; D3–D5 reject its numeric queries,
aggregates and computed updates. Existing tests create fresh objects and assert
these operations succeed: `test_all_field_types.c:106–112`,
`test_computed_update.c:37,70–84`, and
`test_aggregate_type_matrix.c:83,219`. The final mandatory green suite therefore
cannot pass unchanged. Supply explicit slot-2 fixtures preserving the existing
assertions and separate slot-3 refusal coverage, or change activation policy.
Do not weaken assertions or interpret fresh-object tests as legacy coverage.

### STD-05 [P1] Repair failing test oracles and complete maximum-record tests

- Reindex uses `expect_resp(..., NULL, ...)` at 3554–3556, but the helper at
  2612–2613 requires `want && strcmp(...)==0`; it always fails.
- Bulk JSON at 3718 uses `rows`; actual dispatch at `server.c:1938` consumes
  `records`.
- The oversized insert at 4125–4135 appends only field openings, not payloads
  or valid JSON. It cannot test the segment-ceiling error. Request-limit
  fixture changes are left to executor discovery at 4155–4160;
  `test_env_start_ex` presently has no MAX_REQUEST_SIZE override.
- The max-value get checks only for a response, and compactness/actual segment
  vlen are described rather than asserted. Implement byte/content/length and
  on-disk vlen assertions plus a complete test fixture and request builder.

### STD-06 [P2] Force durable pending work in the replay test

Plan 4197–4217 kills only after a successful insert. That does not prove a
pending KFM2 marker existed or that startup replay processed it. Pause at an
existing durability hook, assert the durable marker, kill, reopen and assert
replay plus record/index consistency. Do not describe an ordinary restart as
replay coverage.

## Spec / correctness

### SPEC-01 [P1] Resolve the exact global-root/tenant/object layout identity

Plan 991–1018 reads `<db_root>/schema.conf`, but `load_typed_schema` receives
the effective tenant root (`load_schema`, `config.c:997`); the actual schema
file is at `g_db_root/schema.conf` (`config.c:910–914`). The normal call returns
layout 2 for a slot-3 object. `strstr(line, ":object:")` also ignores tenant
identity. Resolve from the authoritative object metadata with exact tenant
and object matching; do not fall back to another generation on read errors.
Test duplicate object names across two tenants and restart/cache reload.

### SPEC-02 [P1] Bind layout before parsing fields, not only at the loader tail

T1.4 (959–984) never replaces the loader's existing
`parse_field_type(type_spec, tf)` at `config.c:1768`, followed by skipping
unknown fields at 1770. Bare fields disappear and constrained fields keep
legacy widths/semantics before packed layout computation starts. New
`dir_size` also needs correct initialization for legacy fields used by the
span API. Resolve layout first and use the appropriate parser for every field;
reject invalid packed declarations instead of silently dropping them. Verify
field count, offsets, P/S, bare mode and legacy trimmed-field bounds after
loading real files, not only hand-built schemas.

### SPEC-03 [P1] Use separate decimals for constrained assignment

Plan 2094–2095 and 2360–2361 call
`dec_apply_precision(&d, P, S, &d, ...)`. The committed API explicitly forbids
input/output aliasing (`decnum.c:618`, public header); this always returns
`DEC_ERR_INVALID`. Every constrained numeric and currency assignment fails.
Use a separately initialized output, pack that result and free both values.
Test successful rounded constrained assignments and currency.

Real-core proof: `parse=0 alias=8 expected_invalid=8 separate=0`.

### SPEC-04 [P1] Write fixed fields into their directory entries

Plan 2223–2241 always passes the tail cursor to the field encoder and writes
an 8-byte descriptor when `used>0`. The fixed branch at 2316–2324 returns
`used=f->size`. Thus fixed values go into the tail and their actual directory
bytes become descriptors; 1/2/4-byte fields also get 8-byte writes across
neighboring bytes. A one-int schema inserting 7 stores directory int **4**.
Branch composition on field storage kind: fixed bytes stay in their fixed
entry, and only packed fields append payloads/write descriptors.

### SPEC-05 [P1] Measure logical values, then emit in schema order

The purported two-pass composer at 2165–2248 writes immediately in JSON-key
order, then appends defaults at 2250–2289. The validator requires increasing
payload offsets in schema order. Schema `[a,b]` with input `{b:"B",a:"A"}`
encodes successfully but its own validator rejects it. Earlier defaults and
duplicate keys can cause the same violation or unreachable payload bytes.
Collect validated values with an explicit duplicate-key policy, then size and
emit once in declaration order. Test reversed keys, sparse defaults, duplicates
and mixed fixed/packed fields.

### SPEC-06 [P1] Bound update writes by the actual output capacity

T6.5 allocates the measured NEW size (3133–3139), but T6.4 clears
`ts->total_size` bytes (2972) and passes schema-maximum capacities to encoding
(2994–3000). A tiny update in a 4-MiB varchar schema therefore clears roughly
4 MiB through a roughly 11-byte logical allocation. This is a heap overwrite.
Make sizing-only composition real, pass output capacity to the composer, and
bound every clear/copy/encode by that capacity. The current signature has no
capacity argument and cannot enforce this contract.

### SPEC-07 [P1] Remove maximum-sized buffers from all packed paths

The parent/handoff prohibit schema-maximum allocations. The plan still requires
`out_cap >= ts->total_size` (2149–2152), inserts allocate that maximum (2806),
bulk arenas do so per row (3770), scratch slabs use schema/segment caps
(3824–3833), and rebuild explicitly claims maximum-size calloc is acceptable
(3918–3932). An almost-empty row in the accepted maximum varchar schema can
therefore request **1073743905 bytes**. Supply actual sizing/bounded composition
for insert, defaults, every bulk format/update and rebuild. Do not silently
reinterpret the parent's ban as applying only to stack/persistent buffers.
Measure allocation size as well as stored vlen; a small stored record does not
prove a small working buffer.

### SPEC-08 [P1] Require record generation compatible with its schema

At 1392 the validator accepts a record without PACKED before checking a slot-3
schema. The span resolver then treats descriptor bytes as fixed field content
(1426–1434), while decoding selects packed behavior from field metadata.
Clearing hdr2 from 3 to 1 on a valid one-varchar packed record returns an
8-byte descriptor span instead of the 1-byte value. Fail the schema/record
pair closed before span resolution; test both mismatch directions and packed
without nullmap. Keep intentionally supported legacy generations explicit.

### SPEC-09 [P1] Check publication durability failures and metadata ordering

T5.4 (2553–2554) ignores both fsync return values, while existing append
open/write/close failures remain unchecked (`query_schema.c:1590–1610`).
Actual helpers in `durability.c:156–178` return errors. `fields.conf` also
remains unsynced before the advertised schema entry. EIO/ENOSPC or denied append
can lead to success without the promised durable metadata barrier, or durable
schema metadata without durable declarations. Define checked publication order,
fail-closed cleanup/rollback and cache visibility; test write/sync/close failure
and crash windows. Adding unchecked fsync calls is not a durability proof.

### SPEC-10 [P2] Accept the full explicit varchar domain

`parse_decl_int` rejects values over 1000000 (828); `varchar:N` uses it (868).
Consequently valid `varchar:4194304` is rejected although bare varchar has that
capacity and the agreed explicit range is 1..4194304. Use a caller-specific
checked bound. Test 1000000,1000001,4194304,4194305.

### SPEC-11 [P2] Reject explicit empty numeric text

Plan 109–110 endorses empty numeric text as zero. The composer at 2209 skips
encoding when quoted text is empty, leaving a zero-length numeric descriptor
with a clear NULL bit. This violates the parent's frozen lexical contract
(empty scalar numeric strings invalid) and is not a canonical 16-byte zero
payload. Distinguish omission/NULL from explicit empty text and reject it on
single, bulk and default routes, including NOT NULL.

## Additional arithmetic correction (nonblocking by itself)

The single-max-varchar row in table 1.3 (146) mixes a 33-byte bitmap prefix
(256 fields) with one 8-byte directory entry. A genuinely one-field record at
max_key=1024 is `24+1024+2+8+4194304 = 4195362` bytes. A 256-field all-varchar
schema with only one max payload is `24+1024+33+2048+4194304 = 4197433` bytes.
Keep the verified all-256 bounds, but label and recompute this row accurately.

## Primary scratch proof and limitations

`/tmp/p3-plan-review-ynrwb3de/composer_probe.c` extracts the proposed declaration
parser, composer, per-field encoder and view/span code verbatim. A minimal
standalone harness supplies relevant schema fields and simple JSON/fixed-int
helpers for the exact valid inputs exercised; it is not a whole-daemon replay.
It links the real committed decimal core. Ordinary compilation/execution gave:

```
explicit_4MiB_param_rc=-1
reverse_keys_encode_len=20 a_off=17 b_off=16 view_rc=-1
actual_buffer_20_schema_34_encode_rc=-1
fixed_int_encode_len=10 directory_int=4 expected=7
cleared_PACKED_view=0 span=0 span_len=8 expected_payload_len=1
numeric_parse=0 aliased_precision=8 separate_precision=0
```

Other findings use direct proposed/current-source inspection. No full plan
replay or production build is claimed: the missing/invalid hunks prevent one.
No sanitizers, benchmarks, git writes or production edits occurred. Protected
plan was checked by hash only and remains
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

## Required next revision

Keep the proposed storage architecture, but first resolve publication/layout
identity and a single measured composition contract shared by all routes.
Then supply complete executable hunks and real fixtures, with activation staged
so each required intermediate state can build and be tested honestly. Preserve
legacy coverage without assertion weakening; add packed-policy refusal tests.
After corrections, validate the literal complete patch in an isolated checkout
with real build lists and focused/full-suite checks, clearly labeling it as
planning-time evidence. Keep scratch code/logs out of version control. Review
the revised complete plan before approving any execution or D1–D9 tradeoffs.

Standards/executability: **6 findings**, worst P1. Spec/correctness: **11
findings**, worst P1. These are approval blockers; the arithmetic note is an
additional nonblocking correction.
