# Typed value correctness and variable-length numeric/varchar recovery plan

## Primary goal

Finance applications can save, read, filter, index and calculate exact decimal
values without silent overflow or rounding through binary floating point.
`numeric:26,10`, PostgreSQL-sized bare `numeric`, and bare `varchar` work, and
small variable-length values occupy space proportional to their actual size.

All other confirmed type conversion, output and schema-conversion defects are
fixed as part of the same workstream, with route-by-route regression evidence.

## Status, scope and execution boundary

Written 2026-10-19 after source inspection, diagnostic probes and documentation
discovery. **This is the parent design and staged implementation plan.** The
first executable task is the investigation/contract-freeze task below. It is
not permission for an executor to invent the remaining code while implementing.

Revised 2026-10-20 after an independent review pass against this checkout; all
quoted anchors were re-verified in the same checkout. Review outcomes now
baked in: the on-disk display-scale carrier is non-negative with negative
declared scales rendered per PostgreSQL; the average/result-scale contract is
pinned from PostgreSQL 18's published implementation and validated with Python
decimal (no live-PostgreSQL gate); the fixed-width directory descriptors are
accepted, with their steady-state overhead pinned in Task 0.3; Task 0.3 gains
free-pool and segment-maximum stress assertions; Phase 5 must publish the
numeric key-budget feasibility table; UUID braces are specified for either
accepted form. Second revision the same day, during the Phase 0 review: the
bare varchar ceiling is raised from 65,535 to **4 MiB (4,194,304 content
bytes)** — the packed layout's uint32 descriptor removes the legacy
uint16-prefix constraint, so the ceiling is enforcement policy rather than
format, and raising it again later would not be a format change.

CORE-PROCESS.md requires complete production-code hunks, quoted anchors and a
consumer inventory before implementation. The committed decimal core, packed
codec and reviewed consumer integrations are tracked by the Phase 3 work index;
public packed storage remains inactive. The original phase specifications below
remain acceptance requirements, not standalone implementation authorization.
An executable delivery plan may now combine work from several original phases
under the consolidation amendment below. It still requires a complete anchored
patch, human approval, test-first execution and raw-diff review.

The parent plan covers every issue in the preceding audit; it does not leave
the executor to decide semantics, silently reduce numeric limits, or broaden
storage work into an unrelated replacement engine. Individual execution
plans should be self-contained and executable in separate chat contexts.

### Execution consolidation — 2026-10-27

The user requested fewer plans and a target of finishing the roadmap by
2026-10-28. The previous estimate of 8–13 more Phase 3 children is superseded
as an execution structure. After the already-reviewed 3B.10 plan, target **six
remaining delivery plans for the whole roadmap**, grouping by complete behavior
rather than helper or translation unit. This is a planning target, not a claim
that completion by the deadline is already demonstrated.

| Delivery | Complete capability and original ownership | Required exit evidence |
|---|---|---|
| R1 | Remaining ordinary write/read command integration, defaults, computed updates, schema-facing declaration round trips and import/client handling (remaining 3B + Phase 4). Predicate/index and aggregate consumers belong to R2/R3; durable metadata publication belongs to R5. | One route matrix for direct/bulk/default/computed/return paths, exact plain/projection/CSV output, failure preservation and actual-size memory accounting. No separate plan per renderer/helper. |
| R2 | Exact criteria, guards, sorting, ordered indexes, joins, cursors and their remaining packed consumers (remaining 3B + Phase 5). | Indexed/scan equivalence; invalid-input rejection; published exact key-size formula/boundaries; oversized live-key failures; order/cursor/join and output/error tests. |
| R3 | Exact aggregates, grouping/having, parallel merge, aggregate emitters and remaining analytical consumers (remaining 3B + Phase 6). | Independent exact oracle, long/decimal precision, all execution/output paths and allocation failures; complete remaining 3B consumer census after R1–R3 with no unowned unsafe seam. |
| R4 | All remaining calendar/bool/UUID/IP/enum/string validation (Phase 7). | Shared length-aware grammar and write/default/query route matrix; valid zero/NULL/empty distinctions; explicit compatibility notes. |
| R5 | Authoritative metadata/loading, version gating, durable publication, rebuild/schema conversion, upgrade/migration and recovery (3C + Phase 8). | Exact identity/cache/reload tests; all layout mutations; old/new whole-object crash states; forced pending-marker replay; downgrade protection and maximum-record/request bounds. |
| R6 | Explicit activation, finance application acceptance, documentation/clients, final blind review and branch-final safety gates (Phase 9 and remaining activation requirements). | Both numeric forms plus 4 MiB varchar through the application route matrix, migration acceptance, all tracked issues closed, fresh normal suite, required ASan+UBSan and TSan runs before an authorized push/PR. |

The numeric labels Phase 3–9 continue to identify requirements; they no longer
imply one separate plan per phase or per helper. Already-reviewed 3B.10 is not
rewritten while the executor is using it. Later plans are authored against the
reviewed predecessor so anchors stay valid. One delivery plan may contain
multiple test-first tasks/checkpoints and reviewable changes; those do not
automatically become new child plans or additional human approval stops.

Keep fixes with their owning integration instead of first shipping a temporary
private refusal and then scheduling another plan to replace it. Where an
intermediate private seam must remain guarded until its owner lands, identify
that owner and test the guard. Public legacy behavior and inactive packed
storage stay intact until explicit activation approval.

Do not silently expand the six-plan target into another helper-plan chain.
If concrete source/test evidence shows a delivery is too large or has an
independent unresolved architecture decision, explain that specific boundary
before proposing a split. Splitting changes grouping, never scope or safety
requirements. All original contracts, RED/GREEN regressions, full-suite gates,
raw-diff/blind reviews and protected-plan restrictions still apply. This
amendment approves the planning structure only, not the future production edits,
Git writes, activation or a guarantee of deadline completion.

### Protected later work

Do not edit, move, rename, execute, incorporate or delete
`docs/plans/2026-10-19-durability-certification-rig.md`. The user explicitly
wants that work **after this work lands**. Its SHA-256 at planning start is:

```text
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135
```

Ordinary regression/crash tests for the changes in this plan still belong
here; building the separate certification rig does not. Normal full-suite
and sanitizer gates required by AGENTS.md still apply to this work.

### User requirements established in this conversation

- Both exact `numeric:26,10` and bare numeric are blockers for another project.
- Bare numeric must support PostgreSQL's finite numeric range: **131,072
  digits before the point and 16,383 after**. A 1,000-significant-digit cap
  for bare numeric was explicitly rejected.
- Maximum permitted value size must not mean maximum disk space consumed by
  a small value, including a value followed by other fields.
- Add bare varchar as well. Proposed default is the existing engine ceiling,
  **65,535 content bytes**; bounded `varchar:N` retains N as a byte limit.
  This is not a proposal for PostgreSQL's roughly 1-GB text limit.
  *(Revised 2026-10-20: the ceiling is now 4 MiB — see Task 0.2. The 1-GB
  point stands: inline-only records have no TOAST-style out-of-line
  storage.)*
- Leave implementation uncommitted for human and independent raw-diff review.

## Evidence and corrections to preserve

These facts matter because several superficially small changes would be
unsafe or would solve only one of the observed paths.

1. The *typed field layout* currently uses fixed offsets/widths. The segment
   engine already stores **variable-length records** with a uint32 value
   length. We do not need a separate blob service or a new storage engine.
2. Increasing only `TypedField.size` leaves gaps before following fields.
   Current `typed_encode_trim_len()` only trims whole trailing fields; it
   does not pack every string or decimal. Variable segments alone therefore
   do not establish compact typed values.
3. In this checkout, both `parse_field_type()` and
   `validate_field_type_clean()` reject bare varchar and varchar:65536.
   A direct validator probe returned 0, 65537 and 0 for bare varchar,
   varchar:65535 and varchar:65536 respectively. The remembered automatic
   default is not present in these inspected paths. Verify public requests
   and any client normalization before stating a broader historical claim.
4. Numeric precision P is not retained or enforced. The write and index
   encoders and compiled criterion parser convert through double.
5. Computed updates already contain stronger checked integer, scaled-decimal
   and float parsing. Those checks are inline, not a complete reusable
   arbitrary-precision decimal library. Preserve them while consolidating.
6. Existing aggregate type-correctness work is partly implemented. Do not
   execute the old September plan wholesale or reinstate its pre-NULL rules.
7. `transform_field_value()` has a separate float-to-double byte-order bug.
   A direct probe using the actual encoder/transformer produced:

   ```text
   float 3.25 -> double 5.3478801856841961e-318
   ```

   The float encoder stores native float bytes using memcpy; the transform
   interprets those bytes as big-endian and emits big-endian double bytes,
   while the decoder reads native double bytes. Add this to schema regression
   coverage rather than treating widening as automatically safe.

## Issue register and phase ownership

| ID | Mechanism / symptom | Required outcome | Owner phase |
| --- | --- | --- | --- |
| T01 | short/int narrowing wraps; atoi accepts junk | full-input parsing and range error | 1 |
| T02 | long/timestamp ignore strtoll errors | preserve full signed range; reject overflow | 1 |
| T03 | byte narrowing wraps modulo 256 | reject outside 0..255 | 1 |
| T04 | numeric/currency parse through double then unchecked int64 cast | exact decimals end to end | 1, 2, 4 |
| T05 | numeric P ignored; bare numeric absent | enforce declarations; full-range bare numeric | 2, 3, 4 |
| T06 | 10^scale can overflow schema multiplier | validated parameters; remove new-format int64 assumption | 1, 2 |
| T07 | float/double use %g; aggregates %.6f | round-trip float output; exact decimal output | 1, 6 |
| T08 | float overflow/nonfinite input silently accepted | explicit finite-input and underflow policy | 1 |
| T09 | integer/decimal aggregates and having use double | exact typed states, comparisons and worker merges | 6 |
| T10 | bool determined from first byte | documented whole-token boolean grammar | 7 |
| T11 | calendar parsers strip digits/pad, accept invalid dates | real calendar and clock validation | 7 |
| T12 | invalid time becomes midnight | invalid input is an error; midnight remains valid | 7 |
| T13 | malformed UUID/IP becomes NULL or zero; UUID ignores suffix | whole-input validation; preserve explicit NULL | 7 |
| T14 | enum/varchar checks bypassable in lower-level routes | consistent route validation and errors | 4, 7 |
| T15 | numeric scale edit multiplies unchecked; divides by truncation | checked, rounded, transactional conversion | 4, 8 |
| T16 | float widening uses incompatible byte order | exact stored binary32 value widened to binary64 | 1, 8 |
| T17 | fixed typed layout cannot compact every variable field | actual-size packed variable values | 3 |
| T18 | bare varchar absent | default 4 MiB ceiling; no silent truncation | 3, 4 |
| T19 | schema export recreates numeric as precision 18 | preserve bare/constrained declaration, P/S and encoding | 4, 8 |
| T20 | legacy records would be misread if widths change in place | versioned decoding and transactional upgrade | 3, 8 |
| T21 | numeric index encoding assumes eight bytes / f->size | exact canonical keys and bounded-key error handling | 5 |
| T22 | huge value/buffer arithmetic and helper zero buffers | checked sizes and enforced resource limits | all |

## Phase 0 — Documentation discovery, reproductions and frozen contracts

**How this serves the primary goal:** establishes a verifiable baseline and
prevents implementing a format change on assumptions about fixed-size fields.

### Sources and allowed existing mechanisms

Read the current sources, not just historical plans:

- `AGENTS.md`, `CORE-PROCESS.md` and the preceding roadmap.
- `docs/concepts/typed-records.md`, `storage-model.md`, `indexes.md` and
  `concurrency.md`.
- Protocol docs `bulk.md`, `cas.md`, `find.md`, `aggregate.md`,
  `schema-mutations.md`, plus schema export/import behavior in server.c.
- `src/test/fixtures.h`, `test_client.h`, `test_assert.h`, existing
  `test_config_encode.c`, `test_varchar_overflow.c`,
  `test_bool_literal_update_roundtrip.c`, `test_computed_update.c`,
  `test_aggregate_type_matrix.c`, `test_edit_field.c` and NULL/bulk tests.
- `slotcask_record_size_varlen()`, `append_reserve_single_varlen()`,
  `seg_scan_varlen_struct_ok()` and `seg_scan_o_direct()` for actual record
  sizes and maximum record validation.
- `RebuildTxn` and the current `rebuild_object_v2()` publication path for
  staged schema/data/index replacement.

Allowed fixture APIs, verified in headers:

```c
int test_env_start(TestEnv *env);
int test_env_start_ex(TestEnv *env, const char *qbuf_mb_override);
int test_env_start_at(TestEnv *env, const char *db_root, int port);
void test_env_stop(TestEnv *env);
void test_env_stop_keep(TestEnv *env);
void test_env_kill(TestEnv *env);
TestClient *tc_connect(const TestClientCfg *cfg);
int tc_request(TestClient *tc, const char *json, char **out_response);
void tc_close(TestClient *tc);
```

These are existing declarations, not proposed API additions. `tc_request`
returns 0 and allocates the response; the caller frees it. `test_env_stop`
removes the fixture root; use stop_keep/start_at for reopen tests. Do not use
shared environment mutation or fixed ports/paths to configure concurrent tests.

### Task 0.1 — Freeze baseline and mechanically inventory consumers

Test first: reproduce the original numeric error, integer boundary failures,
floating output loss, schema widening, and bare type parsing before changes.
The companion baseline plan supplies full diagnostic sources and commands.
Capture command, exit status, output and source revision. Distinguish helper
evidence from public-route evidence.

Use these exact discovery commands (all shell commands use rtk):

```sh
rtk git status --short
rtk git rev-parse HEAD
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
rtk rg -n 'encode_field_len\(|encode_field\(|encode_field_for_index\(|typed_encode\(' src
rtk rg -n 'FT_NUMERIC|numeric_scale_mult|numeric_scale|FT_VARCHAR|f->size - 2|g_zero_field_65537' src
rtk rg -n 'atof\(|atoi\(|atoll\(|strtod\(|strtof\(|strtoll\(|%.6f|%g' src/db src/cli npm
rtk rg -n 'typed_record_view|typed_field_to_index_key|build_index_key_from_record_into|transform_field_value' src
rtk rg -n 'total_size|max_value|slot_size|SREC_FLAG_NULLMAP' src/db
```

Save a purpose-specific inventory under the type-correctness planning files,
not in the certification plan. Every production phase must refresh its own
call-site list and include headers, tests, CLI, embedded API, schema
export/import and npm consumers where applicable. Separate config-number
parsing from data-value parsing; do not refactor unrelated atoi calls.

### Task 0.2 — Freeze lexical and numerical contracts

The following are proposed contracts for review, not implicit executor choices:

| Area | Proposed contract |
| --- | --- |
| Integer fields | optional sign, decimal digits, entire token consumed; signed min/max inclusive; no fractional or exponent notation |
| Byte | same integer grammar, range 0..255 |
| Numeric input | optional sign, decimal point, decimal exponent; consume all input; normalize mathematical value exactly; preserve display scale |
| JSON strings | unescape once before typed parsing, including escaped digits/signs; embedded NUL rejected |
| Quoted surrounding spaces | permit ASCII surrounding whitespace consistently in scalar parsers; reject embedded whitespace |
| Nonfinite numeric/float | reject NaN and infinities for this finance-focused finite-value contract; never emit invalid JSON nan/inf tokens |
| Float underflow | reject nonzero values that become zero; accept representable finite subnormals; directly parse binary32 with strtof to avoid double rounding |
| Float output | enough digits for exact binary round trip (9 for float, 17 for double); no six-digit/fractional-place clipping |
| Numeric(P,S) | P 1..1000 and S -1000..1000; round to declared S, ties away from zero, then enforce precision; S may exceed P. Declared S is schema metadata: the on-disk display-scale carrier holds the value's own canonical scale and is never negative; a negative declared S rounds to that scale and renders at scale 0, matching PostgreSQL |
| Bare numeric | no forced column scale; finite integer/fraction digit limits 131072 / 16383; preserve meaningful input scale including trailing zeros; exponent notation normalizes mathematically and the stored display scale is the value's fractional scale after applying the exponent, never negative (1.0e2 renders 100) |
| Numeric(P) in new schema | scale 0, matching PostgreSQL; legacy numeric:2 retains its old interpretation until explicit migration |
| Currency | exact numeric(19,4) under the new format |
| Bare varchar | default ceiling 4 MiB (4,194,304 content bytes; revised from 65,535 on 2026-10-20 — the uint32 descriptor removes the uint16-prefix constraint); length lives in the descriptor, not a payload prefix |
| Bounded varchar | N=1..4194304 bytes; overlength rejected, never silently truncated |
| Boolean | case-insensitive documented boolean spellings and their unambiguous PostgreSQL-style prefixes; reject ambiguous o and arbitrary words |
| Calendar | valid Gregorian date, year 1..9999, strict supported compact/ISO forms; valid 00:00:00..23:59:59; no timezone interpretation or silent fractional truncation |
| NULL | explicit JSON null is NULL; omission follows insert/default/update rules; malformed non-NULL text is an error, not NULL or zero |
| Empty strings | valid varchar; invalid scalar numeric/calendar/UUID/IP/enum unless explicitly an enum member; not a substitute for NULL |
| Bulk failure | preserve each mode's existing per-record skip/error or request-level rejection contract; no invented atomicity |

Concrete proposed spellings (outer scalar whitespace stripped first):

- Boolean true: `1`, `t`, `tr`, `tru`, `true`, `y`, `ye`, `yes`, `on`.
  Boolean false: `0`, `f`, `fa`, `fal`, `fals`, `false`, `n`, `no`, `of`,
  `off`. Case-insensitive; `o`, `10`, `trash` and other text are errors.
- Date: exactly `YYYYMMDD` or `YYYY-MM-DD`.
- Datetime: exactly `YYYYMMDDHHMMSS`, `YYYY-MM-DD HH:MM:SS` or
  `YYYY-MM-DDTHH:MM:SS`.
- Datetimems: exactly `YYYYMMDDHHMMSSfff`,
  `YYYY-MM-DD HH:MM:SS.fff` or `YYYY-MM-DDTHH:MM:SS.fff`, with exactly
  three millisecond digits. Time: exactly `HH:MM:SS`.
- UUID field values: exactly 32 hexadecimal digits, or the canonical
  36-character dashed form; one matching brace pair may enclose either
  accepted form.
  Hex digits may be upper/lowercase. No other dash positions or trailing text.
  Existing auto-key normalization remains separately specified and tested.

These grammar changes need compatibility tests for prior documented formats;
an actually supported required format discovered in Phase 0 must be added
explicitly before the child code plan is approved, not silently accepted by
digit extraction in the executor.

PostgreSQL-sized **finite values** do not imply copying every PostgreSQL
extension or special value. If NaN/infinities or PostgreSQL's full calendar
range are required, that is an explicit contract revision, not an accidental
side effect of strtod or date digit extraction.

Average is a separate arithmetic policy. Adopt PostgreSQL 18's exact sum/count
finalization and division scale selection, rather than conflating the stored
16383-digit fractional limit with calculation output scale. Its selection uses
normalized base-10000 weights and leading groups to estimate quotient weight,
targets 16 significant digits, considers both operands' display scales, then
clamps the selected fractional scale to **0..1000**. That final clamp takes
precedence when an operand's display scale exceeds 1000. Round ties away from
zero. Do not replace this algorithm with a generic decimal-digit estimate.

The pinned implementation references are PostgreSQL tag `REL_18_0`,
[`numeric.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/numeric.c)
(`numeric_avg`, `numeric_div_opt_error`, `select_div_scale`) and
[`numeric.h`](https://github.com/postgres/postgres/blob/REL_18_0/src/include/utils/numeric.h)
(calculation-scale constants). Before implementation, Phase 0 must derive and
record the complete algorithm and golden cases, then independently validate
them with Python decimal using explicit context. Include input scales above
1000 and result-weight boundaries. A live PostgreSQL capture is optional, not
a gate. Storage/round-trip support still allows 16383 fractional digits;
average can round an extremely small result to zero under the adopted
PostgreSQL calculation policy. Do not silently choose six decimals or claim
every rational average is exact.

### Task 0.3 — Validate the proposed record layout in an isolated sizing spike

Test first: a small bare numeric followed by another field must not allocate
or persist 74 KB of padding. Test two variable fields, empty strings, NULL,
maximum numeric value and schema maximum fields before choosing layout code.

Proposed approach: retain a fixed field directory for fast access, store each
new-format numeric/varchar value in an inline variable tail, and use uint32
offset+length descriptors. All bytes stay in the existing segment record and
therefore within its current commit-intent durability unit. No sidecar files,
separate GC, or additional publication protocol are proposed.

This layout is a proposal requiring the spike and raw-diff review. Its wire
independent serialized fields are described here rather than a packed C struct:

```text
value = existing nullmap prefix || fixed field directory || variable tail

fixed-width scalar directory entry:
    existing type's fixed binary bytes

numeric/varchar directory entry:
    uint32 big-endian offset from value-region base after nullmap
    uint32 big-endian payload length

varchar payload:
    exactly length content bytes (no reservation for declared maximum)

decimal payload candidate:
    uint8 encoding version
    uint8 sign (zero, positive, negative)
    uint16 reserved = 0
    uint32 big-endian significant digit count
    int32 big-endian decimal scale/exponent carrier
    uint32 big-endian display scale
    ceil(significant digits / 2) bytes of packed decimal digits
```

Use canonical coefficients without leading or trailing insignificant zeros;
keep display scale separately so 1.00 and 1 compare/hash equally but can render
their respective scales. Zero has canonical sign/coefficient, distinct from
NULL. Validate every digit nibble and unused nibble, reserved byte, sign,
offset, length, exponent and display scale on decode. No direct unaligned
struct casts or host-endian serialization.

The full finite numeric range has at most 147455 coefficient digits; the
candidate payload ceiling is 16+ceil(147455/2)=73744 bytes. This is a
**validation/allocation ceiling**, not bytes reserved in every record.
Declared precision P reduces constrained numeric's payload ceiling.

Separate fixed directory width, schema maximum value capacity and actual
encoded value length. Both disk writes and intermediate allocations must use
actual lengths where possible; callbacks that currently malloc the schema
maximum require explicit bounded allocation accounting. Checked arithmetic
must precede every size sum/multiply/alignment/cast.

Accepted overhead decision (2026-10-20 review): the fixed-width directory
descriptors stay. Pin and assert the steady-state arithmetic — an 8-byte
offset+length descriptor per variable field, a 16-byte decimal payload
header, ceil(significant digits / 2) packed digit bytes — so a typical
26-significant-digit numeric:26,10 occupies `8 + 16 + ceil(26/2) = 37`
bytes where the legacy field used 8. The original reported value normalizes
to 25 coefficient digits and still occupies 37 bytes. A value with fewer
coefficient digits uses `24 + ceil(digits/2)` bytes; the declaration alone
does not determine its actual length. A varchar uses `8 + content_bytes`,
six more metadata bytes than its legacy 2-byte prefix, rather than retaining
the legacy field's full reserved capacity. These figures exclude the shared
record/nullmap/key headers and outer 8-byte record alignment. This is proportional
to actual size, satisfies the no-maximum-padding requirement, and the
payload's encoding-version byte preserves a later short-form option without
a format break. The spike and acceptance fixture assert these targets so
the cost is explicit and bounded, not emergent.

Stress the ceilings the new format creates: with bare numeric contributing a
73,744-byte payload ceiling and bare varchar 4 MiB (4,194,304 content
bytes), add the
8-byte descriptor for **each** variable field. Do not carry the legacy
varchar length prefix into the new raw payload. At MAX_FIELDS=256 the
nullmap prefix is 33 bytes. The candidate layout therefore yields:

| Homogeneous maximum schema | Maximum value bytes | Maximum slot bytes at max_key=1024, including 24-byte record header and 8-byte alignment |
| --- | --- | --- |
| 256 bare numerics | `33 + 256 * (8 + 73744) = 18880545` | `18881600` |
| 256 bare varchars | `33 + 256 * (8 + 4194304) = 1073743905` | `1073744960` |

The varchar bound exceeds the 128 MiB segment maximum by design: slot_size
is a validation ceiling, not a per-record promise. Every actual record is
individually checked against the segment maximum
(`append_reserve_single_varlen`). At max_key=1024 a one-field maxed-varchar
record has 4,194,314 value bytes and an aligned physical size of 4,195,368
bytes. A 256-varchar schema with one max payload and all other fields NULL
has 4,196,385 value bytes and an aligned physical size of 4,197,440 bytes.
Both fit; roughly 31 maxed varchar fields fit in one record before the
existing segment-capacity write error applies. Both schema maximum
bounds stay inside the uint32 vlen/offset range and the signed-int
`max_value` derivation.

Verify these with checked sizing code; include mixed-schema cases too. Shared
headers are counted once, and actual records use actual key/value lengths,
not these schema ceilings. Verify (a) small records still
pack tightly at that ceiling — the size-bucketed free pool with leftover
splitting must serve small records without stranding capacity — and (b) the
actual-row segment check is separate from the worst-case schema slot_size.
The schema bound may exceed the segment cap; do not allocate that bound or
claim that all maximum field values fit together. Check actual aligned
header+key+value size before mutation, preserve the segment ceiling, and test
the explicit over-capacity rejection in the owning integration plan. A row
within the permitted actual-record bound must not fail because of its larger
schema maximum.

Phase 0 spike result (2026-10-20): every value in this table verified with
checked sizing arithmetic and a working codec prototype — 59/59 checks at
the original 65,535 ceiling and 59/59 again after the 4 MiB revision
(including mixed layouts, NULL/empty/zero distinctness, descriptor
negatives, and the 147455-digit round trip) — see the Phase 0 evidence
ledger. Free-pool bucket 0 spans [0,256) regardless of schema slot_size,
so small records stay tight at these ceilings.

Index feasibility is established before the format freezes: BTRI keys are
built per component as `[tier byte][payload]` — every composite component
carries its own tier byte, NULL fixed-width components zero-pad to the
component width, NULL varchars collapse to the tier byte alone — so the
key budget is `Σ(1 + payloadᵢ) ≤ 544`. A sign+exponent+packed-digit
decimal key then fits single-field for every constrained P ≤ 1000 and in
composites up to the per-component sum (numeric(1000)+numeric(26,10)
fits; numeric(1000)+numeric(1000) does not), while bare numerics beyond
1080 canonical digits and varchars beyond 543 content bytes are
per-value non-indexable (scans remain fully usable; columns are never
rejected for their theoretical maximum). Phase 5 publishes the final
codec's exact formula and the full P=1..1000 feasibility table.

Reject descriptors outside the actual record, pointing into the directory,
overlapping illegally or exceeding their field's logical capacity. Absent,
NULL and present-empty descriptors must be unambiguous. Deterministic payload
order is field order. Recompose the tail on updates/rebuilds rather than
copying stale offsets. Never trim away a referenced payload.

### Phase 0 planning checkpoint

Freeze and review the cross-cutting contracts first: scalar grammar, exact
decimal/result-scale semantics, serialized layout and version identification,
actual-versus-maximum sizing, index-key feasibility constraints, migration and
durability ordering. Record source revision, evidence, decisions and remaining
unknowns. An unproved contract is not frozen merely because it appears here.

Then produce the complete anchored executable child plan for **Phase 1 only**.
Include every changed/new function/struct/hunk as complete code, every consumer,
fixture sources, red/green commands and expected failure reasons. Review and
obtain human approval for that child plan before its production edits.

Author each later child plan just before its phase, against the reviewed
working tree produced by its predecessors. Future child plans need not all
exist before Phase 1 starts. Each consolidated delivery still requires a complete code
plan, human approval, test-first execution and raw-diff review; parent-plan
approval is not blanket permission to implement subsequent phases. This rule
also covers any implementation/test changes introduced in final acceptance.

Keep a phase ledger with input revision plus relevant uncommitted-diff identity,
approved child plan/version, evidence, changes, test results and review status.
Do not rebase later work on a summary that omits earlier uncommitted fixes.
Sanitizers remain the final all-fixes gate, not a per-phase requirement.

If the spike or later source evidence disproves a frozen contract, stop and
have the planning model revise this parent and affected child plans for human
review. The executor cannot substitute a format, change a limit or improvise
a migration/index policy. A stale quoted anchor follows the stricter
PLAN_NOTES.md halt rule below.

## Phase 1 — Safe existing scalar conversions and floating output

**How this serves the primary goal:** stops new silent damage in existing
schemas while the exact decimal format is built and verified.

Root causes: void encoders cannot report errors, C conversions ignore suffixes
and errno, narrowing casts wrap, numeric crosses double, and output formatting
throws digits away. Float schema widening mixes native and big-endian bytes.

Test first: short/int/long/byte/timestamp min/max, one past either bound,
INT64_MIN, malformed suffixes, empty input, exponent/fraction rejection for
integers, nonfinite float and true underflow, subnormals and precision output.
Add binary32-to-binary64 widening and rejected update preservation tests.

Introduce a single length-aware checked conversion boundary with explicit
syntax/range/allocation status and field-specific error text. Reuse the
overflow-checked patterns in apply_computed_fields; keep the wire boundary's
existing -2+err_buf behavior. Decode JSON escapes once before this boundary.
All textual storage and textual index encoders must consume the same validated
value, rather than independently reparsing it.

Until new decimal storage is active, legacy numeric/currency parsing must be
exact and int64-bounded. It must reject the user's overflowing value clearly,
never pretend it can hold it. Avoid rejecting existing roots solely because
their legacy precision declarations were informational. Legacy scale outside
a safe representable multiplier range needs an explicit compatibility error,
not unchecked multiplication or silently dropped fields.

Copy existing error propagation patterns from typed_encode_defaults and
v2_rebuild_walk_cb, not process-global error buffers. Use memcpy-compatible
float widening and preserve the old stored binary32 value exactly.

Verification: focused new boundary cases, existing config/computed-update/
varchar/NULL cases, indexed-versus-unindexed literal parity, full suite.
Do not bulk-replace every atoi/strtod in config or CLI settings.

## Phase 2 — Exact decimal core and independent numerical oracle

**How this serves the primary goal:** gives storage, comparison, arithmetic
and formatting a common exact interpretation of a finance value.

Root cause: scaled int64 plus double cannot represent declared wide decimals.
Widening only the integer to 128 bits would not support the required bare range.

Build a self-contained bounded-memory decimal core in C, with no new external
runtime dependency. Required operations are parse, normalize, compare, hash,
precision/scale validation and rounding, add/subtract, divide by row-count for
average, and exact text/packed serialization. No generic financial formula
engine, transcendental functions or arbitrary API expansion is required.

Use signed coefficient+scale semantics and a separate display scale. Arithmetic
can use base-10000/base-10 limbs internally; the full API and complete bodies
must be in this phase's code plan. Do not share mutable scratch buffers across
workers. Signed-minimum handling must not negate INT64_MIN in signed arithmetic.
Division by a uint64 count can use checked wider integer intermediates; this
does not authorize double-based division of exact decimals.

Test first against literal expected values and an independent oracle, including:

- Original positive/negative numeric(26,10) value, adjacent last-place values,
  zero, signed zero, trailing zeros, exponents, carry propagation and ties.
- numeric(3,1), numeric(3,5), negative scales, rounding that overflows precision,
  numeric(P) scale 0, and currency boundaries.
- 131072 integer digits plus 16383 fractional digits, one beyond each limit,
  enormous exponent text rejected without loops/allocations proportional to
  the exponent, malformed syntax and embedded NUL.
- Equal mathematical values with different scales hash equally; compare and
  sort positive/negative values consistently.
- Sums crossing int64 and 2^53, cancellation and worker partitioning; averages
  with terminating/repeating results and values below 0.000001.

The frozen average/result-scale contract is pinned from PostgreSQL 18's
documented and published implementation (Task 0.2) and validated with Python
decimal over generated cases with explicit precision/rounding/exponent
context; that validation is mandatory. A disposable PostgreSQL database, if
later supplied, may capture an executable oracle table to upgrade the
evidence; it is not a gate. Never connect to or modify a user's finance
database. Do not use the new parser as its own oracle.
No dependency is added silently; any proposed library addition requires a plan
revision naming its license, build and maintenance implications.

## Phase 3 — Versioned packed numeric/varchar records

**How this serves the primary goal:** stores small values compactly and supports
the maximum values without changing the segment engine's commit unit.

**Execution revision, 2026-10-25 (user approved):** keep packed storage
inactive until required integrations are ready, and split this phase into
three separately reviewed slices. [Phase 3 index](2026-10-23-phase3-packed-records-plan.md)
tracks the rejected draft's findings and outstanding gates:

- **3A:** codec, bounded composition/validated spans and raw physical-record
  tests; no production typed-schema adapter or public activation.
  [Approved child](2026-10-25-phase3a-packed-codec-plan.md) is implemented and
  [reviewed](2026-10-25-phase3a-raw-diff-review.md); packed storage remains inactive.
- **3B:** every consumer and actual-size allocation/composition adapter, with
  legacy behavior preserved; separate complete child plan required.
- **3C:** authoritative layout binding, checked metadata publication,
  downgrade/crash/replay safety; separate complete child plan required.

Public activation additionally requires the necessary Phase 4–6 integrations
and explicit approval. This split changes execution order, not the acceptance
requirements below. No migration or public packed capability is delivered by
3A alone. The protected later certification plan remains untouched.

Implement the Phase 0-reviewed layout using the exact core. Bare varchar maps
to capacity 4 MiB (4,194,304 content bytes); explicit varchar:N retains N; numeric declarations retain
precision, scale, bare/constrained mode and encoding independently of physical
directory size. type_desc's current hardcoded numeric width cannot describe
new-format values or derive their logical limits.

New object metadata must explicitly identify the layout; a known record flag
must identify records that use it. A comment in fields.conf alone is not a
downgrade barrier. Existing headerless schemas/records continue to mean the
old physical layout. Version gating must prevent older binaries opening new
data, including a crash between schema/data publication and root stamping.
Select the release version through the repo's normal release convention.

Refactor actual record access behind validated schema-aware value spans before
updating callers. Include typed_record_view, typed_get_field_str, decode paths,
all field pointers, legacy-absence logic, null bitmap handling and zero buffers.
The current global 65537-byte zero field must never be used as a 73744-byte
decimal backing store. No giant bare-numeric stack arrays.

Replace fixed-output assumptions in encoders and their callers with sizing /
bounded composition. The existing segment vlen is uint32 and can represent
these payloads; preserve record header layout unless evidence requires change.
schema max_value/slot_size remain maximum validity bounds, actual value length
is separate, and segment scans/recovery use the correct maximum. Query and
request caps remain effective, not silently increased.

Test first: small bare numeric and bare varchar at first/middle/last positions,
multiple variable fields, NULL versus empty/zero, large values, truncated/
overlapping descriptors, mixed old/new objects, and maximum schema sizes.
Assert encoded lengths and segment vlen directly; `du` alone is unsuitable
because segment allocation and kf growth add unrelated overhead.

## Phase 4 — All writes, defaults, schema surfaces and error propagation

**How this serves the primary goal:** no alternate input route bypasses the
new exact representation or strict scalar checks.

Integrate single insert/upsert/update, literal/generated defaults, auto fields,
JSON/delimited bulk insert/update, computed updates, record return, guards and
schema backfills. Updates compose a complete new value from validated old spans
and client changes; unchanged variable fields retain exact value/display scale,
but their payload offsets are regenerated. All failures occur before the
operation's irreversible publication point.

Preserve $copy's type compatibility checks and null semantics. Decimal $incr
uses the exact core and column rounding/precision rules; do not reintroduce
double via literal parsing. Explicitly reconcile its older extra-fractional-
digit rejection with assignment rounding in the contract/code plan.

Validate field declarations and defaults at create/add/edit/import; validation
must consume the complete declaration, never accept atoi prefixes such as
varchar:12garbage, and never skip an invalid on-disk field shifting later offsets.
Long variable defaults remain subject to the existing documented 255-byte
literal default limit unless that separate limit is deliberately revised.
Empty varchar and explicit NULL continue to suppress defaults appropriately.

Update describe/export/import and CLI/client schema reconstruction. The current
field_obj_to_spec hardcodes numeric precision 18; export/import must preserve
bare numeric, exact P/S, currency semantics, bare varchar versus bounded
varchar, nullability and defaults. Physical descriptor size must not leak as
logical varchar capacity. Validate emitted schema JSON escaping and name bounds.

Test first: cross-product of each newly checked value class with every input
route, especially middle-field updates followed by another variable field,
bulk mixed valid/invalid rows, literal/default generators, computed operations,
failed updates preserving old records/indexes/counts, and export/import round
trip. Do not change the existing bulk partial-success contract incidentally.

## Phase 5 — Exact criteria, indexes, ordered reads, joins and cursors

**How this serves the primary goal:** a correctly saved number is found and
ordered correctly, with identical indexed and scan results.

Replace independent unchecked compiled criterion conversions and textual index
conversions with the shared parsed value. Cover both bounds, every in/not_in
member, eq_field comparisons, CAS/object guards, joins, full scans, KeySet,
intersection and all index fast paths. Invalid literals are errors; they must
not become zero or a different comparison bound.

Use a canonical order-preserving decimal key excluding display scale. It must
sort by mathematical value across sign, exponent and mixed scale; 1, 1.0 and
1.00 produce the same value key. Preserve the NULL tier. New decimal keys must
have explicit format compatibility evidence; never interpret old eight-byte
keys as the new codec. Reindex under the staged schema before publication.

The B+ tree has BT_MAX_VAL_LEN=544. Full finite numeric storage is not the same
as unlimited B+ tree key width (PostgreSQL also has index tuple limits). Proposed
policy: encode **actual** values, support keys that fit, and return an explicit
error before mutation/publication if a value/composite key exceeds the limit.
Unindexed values of the full numeric range must remain fully usable via scans.
Do not reject a bare-numeric column merely because its theoretical maximum
would exceed the key budget, and never truncate or substitute a hash as an
ordered key. An index build encountering an oversized live value fails cleanly
without replacing the old index or silently omitting that row.

The Phase 5 code plan must publish the chosen decimal key codec's exact
key-size formula and a derived feasibility table as a required deliverable:
for each P in 1..1000, whether a single-field numeric(P,S) key fits including
tier bytes, the largest numeric contribution any composite key can carry, and
the same accounting for varchar widths and NULL/composite overhead. The
documented per-value rejection, scan-only usability and the ban on rejecting
a bare-numeric column outright are tested against that published boundary,
not against ad-hoc observations.

Decouple index payload width from physical field-directory width. Variable
keys in non-final composite positions need a proved self-delimiting codec and
updated bound/resume construction; a fixed-width padding assumption is invalid.
The Phase 5 code plan must either implement that codec fully or explicitly
retain/document current composite restrictions and reject unsupported schema/
cursor requests. It cannot claim complete composite support while guessing.

Test first: negative/positive ordering, equal values at different scales,
adjacent 26-digit values, NULL, ASC/DESC, limit/offset and cursor resume,
composites, joins, exact-key boundaries, live oversized key failures, rebuild
failure preservation, and complete indexed-versus-scan result equivalence.

## Phase 6 — Exact aggregates, group keys, having and all output routes

**How this serves the primary goal:** finance totals and comparisons retain
the value the user actually saved, including across parallel execution paths.

Replace double storage for exact aggregate inputs/results with typed exact
states. long/timestamp sums and averages must not cross double; decimal sum,
min/max, division, worker merge, group hash, having, top-N and order_by use
the exact core and the frozen result-scale policy. Overflow/resource failures
must propagate; do not silently drop a group or return partial totals as full
success. Floating aggregates remain floating and reject nonfinite results.

Audit every indexed/leaf-only/record-fetch/parallel/streaming fast path. In
particular typed_field_to_double, decode_index_key_to_double, AggAccum,
AggResult, top-N states, worker arrays and final emitters need type-aware
alternatives. Disabling a fast path until it uses the exact path is acceptable
only if correctness and documented supported behavior remain intact; no bench
claims without the user's benchmarks.

Unify numeric rendering across get, find/fetch projections, rows/dict/CSV,
joins, group labels and index-only reads. Output decimal text directly, not via
atof. Preserve the current JSON-number wire shape, but document that consumers
must retain number lexemes or use a decimal-aware parser. JS JSON.parse can
lose digits even if the server output is exact; inspect npm and provide a
lossless/raw-result route if it currently offers no way to retain exact values.
Do not globally convert every numeric response to strings without an approved
protocol change. Bounded client examples must not teach double for finance.

Test first: large exact sums, 2^53+1 min/max, fraction tails beyond six places,
multiple groups/scales, NULL and zero, cancellation, repeated worker layouts,
having on adjacent large values, sorted top-N, index versus scan, and each
output format. Verify long-only aggregate correctness independently of new
decimal fields.

## Phase 7 — Calendar, boolean, UUID/IP, enum and string validation

**How this serves the primary goal:** malformed values cannot silently turn
into unrelated valid values through a less-used input or comparison route.

Implement the frozen grammar in one length-aware validation layer. Reuse strict
UUID parsing where its accepted forms match the contract; do not stop at 32
hex characters and ignore a suffix. Check IP input length before copying into
inet_pton buffers. Correct bool's first-byte shortcut. Calendar parsers must
check separators, digit counts, actual leap-year/day limits and clock ranges.
Accept only the explicitly listed compact/ISO input forms; reject suffixes,
timezone or fractional forms that would otherwise be silently discarded.

Keep timestamp as signed Unix milliseconds; this work does not rename it to
PostgreSQL timestamp semantics. Nil UUID, zero IP, midnight, numeric zero and
empty varchar remain real non-NULL values. Nullable invalid sentinels become
errors, with a compatibility note for the historic NULL/zero conversion.
Generation-aware legacy absence conversion must not be replaced with a global
all-zero-is-corrupt rule.

Audit enum membership, string escapes, byte bounds and non-NULL constraints
on every direct bulk/default/schema route, not just the JSON insert wrapper.
Retain existing byte-oriented varchar semantics. Changing all bounded string
lengths from bytes to Unicode characters is not included.

Test first: bool true/false spellings and ambiguous/invalid prefixes; leap-day
centuries, month/day/hour/minute/second/ms limits; UUID trailing input and
valid nil; overlong/malformed IPv4/IPv6 and valid zeros; enum bounds and unknown
member; string NUL/escapes/limits; nullable versus not_null; query as well as
write rejection. Full suite after changes.

## Phase 8 — Transactional schema conversion, compatibility and migration

**How this serves the primary goal:** existing databases can adopt the fixes
without interpreting their old bytes using a new field layout.

Use RebuildTxn and its existing staged schema/data/index publication path.
Make transform_field_value fallible; checked conversion errors abort the
staged rebuild and leave the old object usable. Numeric scale-down uses the
approved exact rounding rule; update the docs' incorrect "truncates ... matches
Postgres" statement and its regression expectations deliberately. Integer
narrows and varchar shrinks retain validation-before-publication.

All layout-changing operations must recompose new descriptors/payloads: add,
remove, edit, vacuum compact, restore/rebuild and any schema default backfill.
Splits-only byte copying is legal only when the encoding/layout is unchanged.
The old size+scale-equal shortcut is insufficient: include encoding, logical
type, precision and relevant capacity metadata in equivalence decisions.

Migration policy to freeze in the Phase 0 implementation patch:

- Headerless legacy numeric remains an eight-byte scaled-int64 record until
  transactionally converted. Never derive old width from declared P.
- Legacy numeric:2 used scale 2, unlike new numeric(P). Preserve its actual
  stored interpretation and emit an explicit full declaration on conversion.
- Audit legacy values against proposed new precision. If old informational P
  would reject stored values, refuse the migration with actionable field/key
  diagnostics; do not round, discard or silently widen a declared constraint.
  A human can approve a wider P or bare numeric in the edit request.
- Existing old-format roots remain readable. The code plan must choose and
  document an explicit upgrade path through the existing schema edit/rebuild
  interface, including same-declaration numeric upgrades, before enabling
  larger writes to those objects. No hidden upgrade while holding a normal
  request's read lock, and no mandatory global migration without approval.
- Drain/replay old pending commit-intent markers using their original schema
  before publishing a new layout. Check replay order, backup, restore, cache
  invalidation, startup/rebuild recovery and downgrade protection explicitly.
- A legacy corrupted number is indistinguishable from a legitimate occurrence
  of that stored value in general. Migration preserves stored meaning; original
  input must be restored from application evidence/backups, never guessed.

Test first: representative old schemas and records with fixtures generated by
the base encoder, noncanonical/oversized old P, scale edits, each layout mutation,
reopen, backup/restore and interrupted rebuilds. Verify either old or new whole
object state after interruption, never mixed schema/data/index interpretation.
Test relevant crash points using existing test hooks. This is local regression
coverage for the new behavior, not execution of the protected certification rig.

## Phase 9 — Application acceptance, documentation and final review

**How this serves the primary goal:** provides a concrete point where the
dependent application can resume with verified financial values.

Both numeric forms and bare varchar must pass the companion acceptance fixture
plus the full route matrix. Include maximum-size values and a small value
followed by another variable field. Measure encoded/physical record length,
not just process allocation or compressed response length. Test maximum field
counts under memory/request limits; never silently relax those limits.

Documentation updates belong with their implementation phases:

- typed-records: exact decimal contract, bare types, physical versus logical
  size, NULL/empty/malformed distinction and compact variable payloads.
- storage-model (and the AGENTS.md storage summary): replace the stale
  "padded to slot_size" segment-record wording with the actual variable-length
  packing — actual per-record size, size-bucketed free pool with leftover
  splitting — so future planning does not disprove the varlen premise from a
  stale summary.
- schema-mutations: new declaration semantics, exact rounding, safe upgrade.
- bulk/CAS: validation and error accounting; exact computed updates.
- find/indexes/joins: actual index-key limits and composite/cursor policy.
- aggregate: typed result accuracy, scale/rounding, consistent NULL rules.
- diagnostics/clients: precision/capacity metadata and lossless consumption.
- limits, compatibility/release notes, reference changelog and overview docs.

For each bug, retain the regression tests and temporarily reverse **only its
production fix** in an isolated review checkout, run and capture the expected
red result, then reapply and capture green. Do not change tests to manufacture
a failure. Dependency-heavy new-format tests must be partitioned so reverting
the targeted semantic fix does not merely cause an unrelated build error.

Build/run commands (use rtk and save real output):

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run-all
```

Normal full suite is mandatory after each production change round. Fix flakes
and unexplained failures rather than rerun until green. No benches.

After ALL fixes, before the push/PR gate and hand-back for review, run both
sanitizers once per completed branch series as required by AGENTS.md, each
full suite three consecutive fresh invocations:

```sh
rtk proxy env BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy ./build/bin/shard-db-test run-all
rtk proxy env BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
rtk proxy env TSAN_OPTIONS=second_deadlock_stack=1:print_stacktrace=1 ./build/bin/shard-db-test run-all --jobs 2
rtk proxy env TSAN_OPTIONS=second_deadlock_stack=1:print_stacktrace=1 ./build/bin/shard-db-test run-all --jobs 2
rtk proxy env TSAN_OPTIONS=second_deadlock_stack=1:print_stacktrace=1 ./build/bin/shard-db-test run-all --jobs 2
```

No halt_on_error=0 or blanket suppressions. A new fix after sanitizer runs
invalidates that gate; rerun the final gate after the new fixes, not between
individual rounds. Preserve outputs and distinguish current gates from the
older aggregate plan's stale TSan deferral.

Have an independent reviewer inspect raw diff/repo without seeing the plan
for on-disk/wire/concurrency changes, then a standards/spec review. Resolve
findings and re-review. Check no unintended dependency, debug print, TODO or
unrelated change remains. Verify the protected plan's checksum. Leave all work
uncommitted; do not add, commit, push, open a PR or merge without the exact
operation being authorized by the human.

## Consumer inventory and exact source anchors

These anchors were read in this checkout. Child code plans must quote the
precise insertion/replacement text, refresh references and include full changed
blocks rather than use the line numbers as editing instructions.

| Area | Existing quoted anchor / symbols and consumers |
| --- | --- |
| Schema representation | `int64_t numeric_scale_mult;`, `struct TypedSchema`, `g_type_desc` numeric width 8; types.h/type_desc.c |
| Schema parsing | `void parse_field_type(const char *spec, TypedField *f)`, `TypedSchema *load_typed_schema(`, `static int validate_field_type_clean(char *buf)` |
| Record encoder | `void encode_field_len(`, `void encode_field(`; typed_encode, typed_encode_defaults |
| Index text encoder | `void encode_field_for_index(`; query.c criterion/order keys, query_join.c lookup, query_plan.c composites, index.c textual builders |
| Binary index builder | `int build_index_key_from_record_into(`, `typed_field_to_index_key`; decouple f->size and index payload |
| Record readers | `static int decode_field_to_buf(`, `int typed_record_view(`, `size_t typed_encode_trim_len(`, typed_get_field_str |
| Single writes | cmd_insert_v2, auto_create_field_transfer, update_compose_new_from_old, update_stamp_auto_update_fields, apply_computed_fields in storage.c |
| Direct bulk encoders | bulk_ins_delim_run, v2_bulk_upd_delim_value_compute, v2_bulk_upd_json_value_compute in query_bulk.c |
| Rebuild | `void transform_field_value(`, v2_rebuild_walk_cb, rebuild_object_v2 in query_find.c; field_value_fits_new in query_schema.c |
| Predicate parsing | `static int64_t parse_numeric_i64(` and compile_criteria/match_typed families in query_plan.c; query.c planning and execution |
| Output/aggregates | typed_field_to_buf_raw, decode_idx_to_buf, typed_field_to_double, decode_index_key_to_double, fmt_double, AggAccum/AggResult and all worker/final paths in query_aggregate.c; joins/query consumers |
| Schema round trip | `static void field_obj_to_spec(`, cmd_describe_object, CLI describe caches and npm schema/value handling |
| Physical maximum | `static inline size_t slotcask_record_size_varlen(`, `append_reserve_single_varlen`, `seg_scan_varlen_struct_ok`, schema max_value/slot_size and callback capacities |
| Test registration | exact build.sh source-list anchor `src/test/cases/test_config_encode.c` followed by its continuation backslash; TEST_REGISTER alone does not compile a case |

Known external consumers include the dependent finance application and clients
that use raw JSON versus JSON.parse; their source is not in this workspace.
Document protocol effects and supply lossless examples rather than claim those
external clients have been tested.

## Execution rules embedded in every child plan

1. Work from the default branch on a fresh feature branch, preserving all
   pre-existing uncommitted files. Follow CORE-PROCESS branch conventions and
   request exact git write authorization where required. Do not commit work.
2. Do tasks in order, following their approved complete code patches. Every
   task starts with its failing regression or diagnostic reproduction. Prepare
   each consolidated delivery plan just before its work; obtain human approval
   before its production edits, and review its raw diff before advancing.
   Multiple original phase requirements may share one approved delivery plan
   under the 2026-10-27 consolidation; tasks/checkpoints do not need separate
   child plans merely because they touch different consumers.
   Do not require all future child plans upfront or treat the parent as blanket
   implementation approval. Maintain the phase ledger and preserve all prior
   uncommitted work.
3. If a quoted anchor is not found exactly, write PLAN_NOTES.md describing the
   mismatch and halt the **entire** execution run immediately. Do not guess,
   reinterpret or continue even an unrelated task. The human or re-engaged
   planning model must read it, decide stale anchor versus wrong assumption,
   and supply a patched/fresh approved plan. Execution never resumes itself.
4. If a decision is not covered, stop and ask; do not improvise.
5. Build/test/full-suite and final sanitizer commands are those above and in
   current AGENTS.md. Copy actual red/green outputs into the execution report.
6. No weakening assertions, suppressing unexpected findings, hidden retries,
   unchecked casts, malformed-to-NULL fallbacks or truncated index keys.
7. Keep the certification plan untouched and scheduled after this work lands.

## Milestone definition

The finance project is unblocked only after exact numeric core, packed record
layout, all write/read/query/index paths, exact calculations, migration path
and application acceptance are green together. Scalar safety alone is useful
but is not that milestone. Complete all remaining type validation and final
gates before declaring this whole workstream complete.

## Companion execution material

- [Executing-agent instructions](2026-10-20-type-correctness-executor-handoff.md):
  copy-ready Phase 0 assignment and the stop boundary before Phase 1 approval.
- [Baseline investigation](2026-10-19-type-correctness-baseline-plan.md):
  complete source probes, exact compiler commands and captured failure output.
- [Acceptance fixture and route matrix](2026-10-19-type-correctness-acceptance-plan.md):
  complete proposed C fixture, checked against the current test headers, plus
  mandatory path-specific cases. The fixture passed a planning-time syntax
  check with `-std=gnu11 -Wall -Wextra -Werror -fsyntax-only`; it has not been
  executed against a daemon or represented as a passing feature test.

## Primary references

- PostgreSQL 18 numeric input, precision/scale, finite limits and floating output:
  https://www.postgresql.org/docs/18/datatype-numeric.html
- PostgreSQL 18 aggregate result types:
  https://www.postgresql.org/docs/18/functions-aggregate.html
- PostgreSQL 18 boolean grammar:
  https://www.postgresql.org/docs/18/datatype-boolean.html
- PostgreSQL 18 calendar types:
  https://www.postgresql.org/docs/18/datatype-datetime.html
- PostgreSQL 18 character type differences:
  https://www.postgresql.org/docs/18/datatype-character.html

Compatibility targets are explicitly scoped above. Read these sources in the
code-planning pass; their existence does not prove our implementation follows
them. No performance claim or full PostgreSQL-equivalence claim is made.
