# Phase 1 child-plan review — changes required before approval

Reviewed 2026-10-20 against source HEAD
`1dd1888fda49ce295717590d1f65415963445270`.

Documents reviewed: the Phase 1 child plan, Phase 0 evidence ledger and relevant
parent contracts. Production source remains unchanged. This review does not
approve implementation. The evidence ledger's green baseline is a baseline,
not evidence that the proposed code compiles or preserves values.

## Findings

### R1 — P1: New exact writes disagree with deferred criterion/index parsers

Child plan line 55 claims `encode_field_for_index` is exact and can remain
unchanged. The evidence ledger repeats that claim at line 66. This confuses
`typed_field_to_index_key` (binary stored-value conversion) with the textual
encoder: actual `encode_field_for_index`, config.c's FT_NUMERIC arm, still
uses atof and the unchecked scaled-int64 conversion. query_plan.c's
parse_numeric_i64 does the same; its FT_FLOAT parsing also needs reconciliation
with direct strtof writes.

Probe results using the proposed numerical parser and unchanged textual index
encoder on identical literals:

```text
numeric:18,2 input=90071992547409.91
  checked stored_scaled=9007199254740991
  textual query_scaled=9007199254740992
numeric:26,10 input=922337203.6854775807
  checked stored_scaled=9223372036854775807
  textual query_scaled=-9223372036854775808
float input=1.0000000596046448
  direct-strtof stored=1.00000012
  old textual index query=1
```

Valid newly written values therefore cannot reliably be searched by the same
literal, and guard/range behavior can use different values. Sharing the parsed
value with the existing key/compiled-criterion encoders is required in the
same reviewed slice; it does not require changing the on-disk index format.
Revise the phase boundary instead of deferring this inconsistency to Phase 5.
Test identical literals through writes, scans, indexes and guards, including
the examples above. Fix the corresponding ledger statement.

### R2 — P1: Very small decimals are rounded up at the wrong digit position

Child plan lines 359–385 compute q_len=0 both when all significant digits are
dropped and when the rounding position lies beyond them, then inspect the
first input digit in both cases. The latter case must not round up.

```text
numeric:18,2 input=6e-100  -> rc=0, stored_scaled=1
numeric:18,2 input=5e-4    -> rc=0, stored_scaled=1
```

Both should store zero at scale 2, rather than 0.01. Correct the position
calculation and test the first retained/drop boundary, multiple extra dropped
places, negative counterparts, exponent forms and leading zeros against an
independent decimal oracle. This is new silent corruption, not just rejection.

### R3 — P1: Unconditional trimming changes varchar and enum values

Child plan lines 406–410 strip whitespace before dispatch on the type. All
converted callers therefore trim varchar and enum text as well as scalar
number text, despite those types' significant whitespace.

```text
varchar:20 input=[  finance  ] -> stored=[finance], len=7
enum(a ,a) input=[a ]         -> stored member index=1 (a), not index=0 (a )
```

The enum can pass the wrapper's membership validation before the new helper
changes it into another member. Restrict scalar whitespace normalization to
types whose grammar permits it, preserve string/enum bytes, and add these
regressions across assignment/default/bulk paths. Do not move their semantics
under a shared numeric-text trimming rule.

### R4 — P1: Zero-multiplier legacy fallback leaves a crashing decoder

Task 3 sets numeric_scale_mult=0 for unsupported legacy scales but guards only
decode_field_to_buf and typed_field_to_buf_raw. Unchanged decode_idx_to_buf
still computes integer v/scale and v%scale for nonzero values. Passing its
planned sentinel with a nonzero numeric key produced:

```text
exit 136: process terminated by signal 8 (SIGFPE)
```

Other compiled/aggregate consumers also use the multiplier. The new sentinel
must be propagated safely to every consumer or the affected operations must
fail explicitly before decoding. Returning the raw integer from selected
paths also changes numeric meaning across routes; do not present that as a
safe exact decimal decode. Add reopen/indexed-read/aggregate tests for an
existing unsupported-scale schema and define a consistent error policy.

### R5 — P1: Literal proposed core and regression source do not compile

Extracting the exact Task 1 and Task 6 C blocks and syntax-checking them against
current headers returns nonzero:

```text
core: implicit declaration of function 'isfinite'
core: too many arguments to function 'fmt_conv_err'; expected 4, have 5
test: macro 'ASSERT_NOT_NULL' requires 2 arguments, but only 1 given
```

Task 1 needs the actual math header change, and fmt_conv_err must have a
consistent formatting contract at every call (lines 81, 324–386). Task 6
line 856 omits the assertion description. The f->name conditional also tests
an array address that is always non-NULL, introducing a warning with -Waddress.

After the test compiles, its float expectation at line 929 is also wrong:
strtof("1.2345678") rendered with %.9g is `1.23456776` on this target,
not the proposed `1.23456777`. Validate the intended binary round trip and
expected lexeme independently, rather than weaken the assertion.

Run syntax checks on the literal final blocks, then compile the complete
planned patch in an isolated scratch checkout before resubmission. Do not
tell the executor to repair these while implementing an approved plan.

### R6 — P2: Valid legacy numeric bounds and scale zero are rejected

Child plan line 269 rejects numeric_scale <= 0 even though Task 3 and the
documentation expressly allow scale 0. Both magnitude accumulation paths use
INT64_MAX even for negative values (lines 335/365), excluding valid INT64_MIN.

```text
numeric:18,0 input=1                    -> rc=-1
numeric:26,10 input=-922337203.6854775808 -> rc=-1
```

Support S=0 and use a sign-dependent magnitude bound including 2^63 for
negative values, with overflow checking after rounding as well. Cover exact
positive/negative endpoints, one beyond, S=0 and all-zero/leading-zero forms.

### R7 — P2: Integration hunks and regression coverage are not execution-ready

Task 2a supplies conflicting replacement snippets, including a checked call
without its two error arguments (lines 510–511), then asks the executor to
wrap/reconstruct the sites. It refers to a nonexistent seen array before
acknowledging its absence. Several call-site/signature changes remain prose
(lines 531–533, 619–620), and Task 3 says to adapt parameter names instead of
giving the complete second hunk. That does not meet the repo's complete-code
and exact-anchor execution gate.

Replace these with complete, unambiguous final hunks/functions and include
all cleanup/error paths. Add meaningful regressions for the changed bulk,
default/backfill and schema-scale routes: the delivered test exercises single
inserts/updates and one easy indexed count, but does not cover those caller
changes, the failures above, or actual indexed-versus-scan equivalence. Run
the failing regression before applying the fix as well as the required
post-fix reverse/reapply proof; the execution order currently starts with
production Tasks 1–5 before adding/running the new case.

### R8 — P2: Phase 0 index-feasibility evidence contains false conclusions

Ledger lines 197–201 assert one NULL tier per key, not per field, and that
numeric(1000)+anything exceeds 544. Actual composite construction in
build_index_key_from_record_into emits a tier for every component. Even using
the ledger's candidate formula, a 503-byte decimal payload plus a two-byte
short payload plus two tier bytes is **507**, which fits 544. Thus both the
accounting model and that blanket composite conclusion need correction.

The candidate codec also needs explicit exponent units and proof of
negative-prefix ordering; a bare sign/int16-exponent/packed-digits formula
alone is not a frozen sortable codec. The original 131072-digit integer
limit cannot fit a plain signed-16-bit decimal exponent. Preserve the parent
requirement to derive and test the actual codec and budget; distinguish
prototype assumptions from established feasibility facts. Also use an
immutable PostgreSQL source tag/commit instead of REL_18_STABLE when claiming
the source pin is exact.

## Verification method and limitations

- No production or submitted plan/ledger file was edited during review.
- Exact core/test code blocks were extracted into
  `/tmp/shard-phase1-plan-review-ko5281ek/`. Literal syntax checks failed as
  recorded above; logs are core.c.compile.log and test.c.compile.log there.
- To exercise numerical behavior despite those build errors, diagnostic-only
  copies added math.h, permitted fmt_conv_err's extra arguments and supplied
  a type-name stub. Parsing, rounding, trimming and byte encoding were left
  as written. The results are isolated-helper evidence, not a claim that the
  literal submitted patch builds or has been applied to a daemon.
- Key probes called the current real encode_field_for_index on the same
  literals as the proposed checked parser. Source inspection verified the
  independent query_plan parser retains the double conversion.
- The zero-multiplier probe linked the actual unchanged aggregate decoder and
  utilities. Core dumps were disabled for the deliberate SIGFPE experiment.
- The reported 17254-pass baseline suite was not rerun: production source did
  not change, and baseline green cannot resolve proposed-patch failures.
- Protected durability plan checksum remains
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

## Resubmission boundary

Revise the Phase 1 plan and incorrect ledger statements, compile the exact
proposed hunks/tests in isolation, include the numerical/route regressions,
and submit for another review. Do not execute production changes or regard
the current child plan as approved. No commit/push/branch operation is
authorized by this review.
