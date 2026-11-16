# Type correctness recovery roadmap

> Updated scope and staged implementation design:
> [Type correctness recovery plan](2026-10-19-type-correctness-recovery-plan.md).
> That plan supersedes this preliminary roadmap wherever they differ. It
> includes the user's subsequent full PostgreSQL-sized finite numeric range,
> compact variable payloads, and bare varchar defaulting to 65535 content
> bytes. The separate durability-certification plan remains untouched and is
> scheduled only after this work lands.

## Primary goal

Applications can write, read, query, and calculate typed values without silent
overflow, precision loss beyond the declared type, or malformed input turning
into an unrelated valid value. Exact `numeric:26,10` and bare `numeric` support
unblock the dependent application without requiring complete PostgreSQL parity.

## Status and boundaries

Proposed roadmap, 2026-10-19. This is a scope and sequencing document, **not an
executable implementation plan or permission to modify production code**. Each
implementation slice needs its own CORE-PROCESS-compliant plan: exact quoted
anchors, complete changed code blocks, enumerated consumers, test-first steps,
and approval. No storage representation or migration design is approved here.

The user confirmed both constrained numeric:26,10 and bare numeric with varying
decimal places are blockers; both are required for the application milestone.
No implementation, build, full-suite run, or sanitizer run has occurred here.

## Phase 0 — Establish the contract and public-path evidence

**How this serves the primary goal:** makes the work finite and ensures fixes
address real application paths rather than isolated helper behavior.

Deliver a matrix of type × operation × route. Include schema creation/edit,
literal/generated defaults, insert/upsert/update, CAS and object guards,
computed updates, JSON/delimited bulk input, get/find/fetch/projections, CSV,
criteria and joins, sorting/cursors, aggregates/grouping/having, index
construction and rebuild, vacuum, backup/reopen, and recovery replay.

For every finding, record: exact input, expected contract, observed response,
raw stored value where appropriate, source mechanism, route coverage, and
whether existing docs intentionally promise the behavior. A low-level probe
alone is not proof that every public route accepts that input.

Current evidence from scratch C harnesses including the actual config.c:

| Type | Input | Observed helper result |
| --- | --- | --- |
| numeric:26,10 | 1234567890123456.1234567890 | stored INT64_MIN; scaled display -922337203.6854775808 |
| bare numeric | numeric type spec | FT_NONE |
| short | 32768 | -32768 |
| int | 2147483648 | -2147483648 |
| long | 9223372036854775808 | 9223372036854775807 |
| int | 123abc / oops | 123 / 0 |
| byte | 256 / -1 | 0 / 255 |
| double | 1.2345678901234567 | stored value retained; normal decoder's %g renders 1.23457 |
| float | 1.2345678 | expected binary32 approximation, then %g renders 1.23457 |
| double | 1e999 | infinity |
| bool | yes / trash / 10 | false / true / true |
| date | 2026-02-30 | 20260230 |
| time | 25:00:00 | zero seconds |
| uuid validator | canonical UUID + TRAILING | accepted |

Source inspection additionally found unchecked digit extraction/padding in
calendar encoders, sentinel-to-NULL behavior in JSON writes, and double-based
numeric/integer aggregate accumulation with six-fractional-digit formatting.
Reproduce these through public routes before finalizing affected-route claims.

Contract decisions required in the child plans:

- Invalid non-NULL input must produce a validation error; explicit NULL retains
  its documented semantics. Existing bulk skip/error-count behavior must be
  respected or explicitly redesigned, not silently made transactional.
- Specify accepted lexical forms, whitespace, exponents, integer fractions,
  empty strings, booleans, calendar bounds, UUID forms, and non-finite floats.
- Use PostgreSQL as a correctness reference, not a mandate to copy every type.
  Keep epoch-millisecond timestamp and byte-sized unsigned values unless a
  separate change is approved; varchar byte limits are not a corruption bug.
- Decide decimal precision/scale limits, rounding and division rules, exact
  output representation, resource bounds, and handling of old numeric schemas
  whose declared precision exceeds their actual int64 storage capacity.

Verification: deterministic public-protocol reproductions, independent expected
values (not the encoder under test), and a complete route inventory. Preserve
the unrelated durability-certification plan and all pre-existing changes.

## Phase 1 — Prevent further silent conversion and output damage

**How this serves the primary goal:** existing supported types become safe to
use while the decimal format is designed.

Plan A: checked full-input parsing for short/int/long/byte/timestamp, finite
float range/underflow policy, round-trip-safe floating output, and exact decimal
parsing with bounds checks for the existing scaled-int64 numeric/currency
format. Validate numeric schema parameters without overflowing 10^scale.

Enumerate every encode_field/encode_field_len consumer before introducing an
error-returning API. Apply validation consistently to criteria and index-bound
encoding as well as writes; a bad criterion must not silently target another
value. Preserve record/index state when validation fails, within documented
per-record bulk semantics. Test defaults and schema conversion too.

Reuse the stronger computed-update parsing already documented in
docs/query-protocol/cas.md and implemented around parse_computed_span and its
consumers in storage.c: exact decimal scaling, integer/result overflow checks,
extra fractional digit rejection and finite float checks already exist there.
Audit and consolidate those mechanisms; do not claim every arithmetic route
has the plain encoder's defects. Pin deliberate differences between assignment
rounding and computed-update validation before sharing an API.

This phase must not pretend int64 can support numeric:26,10. Oversized values
must fail clearly until Phase 2 is ready. Existing roots with oversized P
declarations must not be made unreadable by a blanket schema rejection.

Tests first: exact min/max, one step outside each bound, malformed suffixes,
large negative values, empty input, scientific notation policy, subnormals,
overflow/underflow, signed zero, NULL, and precision-preserving output through
each read format. Prove failed writes leave old records and indexes intact.

## Phase 2 — Exact decimals, including the application's blocker

**How this serves the primary goal:** the reported 26-digit value and varying
decimal places become usable without hidden precision loss elsewhere.

Plan B: design and implement an exact decimal module and representation for
constrained and bare numeric. Include currency compatibility. A wider fixed
integer alone does not provide unconstrained numeric semantics. Explicitly
document finite implementation limits and enforce query/request memory caps.

Consumers to inventory include config.c, type_desc.c, types.h, query_plan.c,
query.c, query_find.c, query_aggregate.c, query_schema.c, query_bulk.c,
storage.c, index.c, joins, CLI/NQL, embedded APIs, and client packages under
npm/. This is an initial discovery list, not a completed call-site inventory.

The design must cover exact parsing/rendering, comparison, sortable index keys,
composite indexes, grouping hashes, cursor tokens, CAS, computed updates,
sum/min/max and defined avg rounding, parallel aggregate merging, and having.
Include exact integer accumulation so valid int64 values do not become
inexact through double. Floating aggregates retain floating semantics.

Storage, index, protocol, and upgrade decisions belong in a dedicated design
checkpoint before writing the executable plan. Migration must be restartable,
durability-checked, and compatible with pending commit-intent replay. Never
reinterpret old bytes using the new decimal layout. A lossy old numeric value
cannot be reconstructed from its corrupted bytes; recovery requires original
application inputs or a suitable backup.

Tests first: the user's exact value and scale, negative counterpart, adjacent
large decimals, significant/trailing zeros, mixed scales, rounding boundaries,
limits, NULL/zero distinctions, indexed-versus-scan equivalence, pagination,
computed arithmetic, aggregates, restart, old roots, and migration interruption.
If staged internally, decimal storage/query support and decimal calculations
are separate implementation slices but one application acceptance milestone.

## Phase 3 — Strict validation for remaining types and routes

**How this serves the primary goal:** dates, identifiers, and booleans cannot
silently become a different value or NULL because input was malformed.

Plan C: validate real calendar dates, leap years and time bounds; define
fractional-second acceptance explicitly; parse booleans from a documented
set; validate entire UUID/IP inputs; distinguish invalid text from NULL and
valid all-zero values across every write path. Audit enum membership,
varchar bounds, escapes and UTF-8 policy, defaults, and schema rewrites for
bypasses rather than assuming their current JSON validation covers all routes.

Existing malformed-time and malformed-sentinel behavior is documented, so
strict rejection is a compatibility change needing release notes and explicit
approval in the executable plan. Do not silently rewrite old invalid dates or
declare all zero-valued records corrupt. Do not promise to detect past integer
wraparound or decimal rounding from otherwise valid stored bytes.

Tests first: invalid/valid leap days, month/day/clock limits, missing/trailing
characters, UUID trailing junk, overlong IP input, zero UUID/IP/midnight,
empty strings, NULL/not_null, enum first/last/unknown members, varchar byte
limits and malformed escapes, plus route-equivalence checks.

## Phase 4 — Application acceptance, upgrade verification, and hand-back

**How this serves the primary goal:** provides evidence the dependent project
can proceed and existing databases remain safe to open and operate.

Maintain a small acceptance fixture modeled on the dependent application's
actual schema and operations. It must round-trip the exact reported number,
use bare numeric with differing scales, and exercise whichever predicates,
indexes, arithmetic, exports and aggregates the application actually uses.
Use textual/decimal-aware assertions so client-side double parsing cannot
hide a server error or create an apparent one. Verify full lifecycle/reopen.

For every implementation slice: prove the regression is red with the fix
temporarily reverted, then green after reapplying; capture both outputs.
Build with `SKIP_TESTS=1 ./build.sh` and run
`./build/bin/shard-db-test run-all` after every change round. Prefix shell
commands with rtk (use rtk proxy env for environment-prefixed commands).
Never run benchmarks; those belong to the user.

Follow the current AGENTS.md sanitizer gate after all fixes, not between fix
rounds: BUILD_MODE=asan build plus three fresh full suites; BUILD_MODE=tsan
build plus three fresh full suites with
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" and --jobs 2.
No relaxed sanitizer options or blanket suppressions. Treat the current gate
as authoritative over any stale instructions in older plans.

Update user docs and release/migration notes in the same implementation diff.
Include independent raw-diff review for storage/wire/concurrency changes.
Leave implementation uncommitted; no push or PR without the user's exact
authorization. The executable child plans must include the repository's full
stale-anchor PLAN_NOTES.md halt rule and stop-on-uncovered-decision rule.

## Milestones and recommended sequencing

1. Approve the scope and contracts, then author Plan A in executable detail.
2. Complete Plan A; it prevents silent damage but does not unblock wide numeric.
3. Author/approve Plan B's representation and migration design; implement its
   slices with exact calculations included in the decimal acceptance milestone.
4. Complete the dependent-application fixture: this is the earliest candidate
   unblock point if it does not depend on still-unfixed Phase 3 behavior.
5. Complete Plan C and integrated verification to close every issue in scope.

Do not publish an unsafe interim release as fully type-correct. Separate
reviewable slices reduce risk; they do not justify dropping known issues.
No time estimate is promised before the representation and migration work is
understood. Complete PostgreSQL parity, new SQL types, timezone features,
unlimited text, collation redesign, and performance benchmarking are outside
this recovery roadmap.

## Documentation discovery and existing patterns

- AGENTS.md and CORE-PROCESS.md: approval, tests, sanitizer gates, review rules.
- docs/concepts/typed-records.md: field widths, decimal claims, sentinels, NULL,
  defaults, and documented compatibility changes.
- docs/query-protocol/aggregate.md: numeric result contract, grouping/having,
  parallel routes and NULL behavior; its older unset-marker wording conflicts
  with its newer NULL section and must be reconciled against current code.
- docs/plans/2026-09-22-aggregate-type-correctness.md: historical context only;
  portions are implemented and NULL rules have since changed. Do not execute
  it or treat its old status/sanitizer wording as current.
- src/test/cases/test_config_encode.c: focused codec test pattern.
- src/test/cases/test_aggregate_type_matrix.c: cross-type aggregate matrix.
- docs/query-protocol/cas.md and src/test/cases/test_computed_update.c:
  existing checked arithmetic and regression patterns; retain their guarantees.
- docs/query-protocol/bulk.md: per-record partial success/error accounting.
- Further end-to-end families: test_all_field_types, test_null_bulk,
  test_not_null_updates, test_varchar_overflow, and
  test_criteria_field_value_validation. Existing test_config_encode assertions
  pin some unsafe low-level coercions; replace their contract explicitly in
  approved child plans instead of silently retaining those behaviors.
- PostgreSQL official references: datatype-numeric.html, datatype-boolean.html,
  datatype-datetime.html, datatype-character.html, functions-aggregate.html.
  These were consulted during diagnosis; child plans should pin the target
  PostgreSQL version and explicitly list intentional differences.
