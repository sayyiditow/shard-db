# Phase 1 child plan — third revision review

Reviewed 2026-10-20 against HEAD `1dd1888fda49ce295717590d1f65415963445270`.
Planning review only: no production edits, branch, commits, or certification work.

The revised exact numeric parser and unsigned-magnitude renderer bodies address the previously identified decimal rounding and INT64_MIN issues. The bulk request field, tally assertion and binary32 output expectation are corrected. Approval is still blocked by the findings below.

## R3-01 — P1: writable integer/float/double literals still produce different indexed lookup keys

Plan locations: Task 1 integer/floating parsers; Task 3b, especially lines 935–949; consumer inventory line 123.

The write parsers now consume up to 20 integer characters and 63 floating characters, but textual index encoders retain the old truncating buffers. Replacing `(float)atof` with `strtof` does not remove that truncation. This also invalidates the claim that the FT_DOUBLE path has no parity gap: the parsing function is equivalent, but the input span is not.

Compiled the current document's exact conversion-core block against real config.c and types.h in scratch, applying Task 3b's numeric and float index replacements to a scratch copy of config.c. Executed results:

```text
int length=20 rc=0 stored=1 lookup=0
long length=22 rc=-1 error=value for field '' is not a valid scalar: expected a whole decimal number
float length=53 rc=0 stored=1e+10 lookup=1
double length=53 rc=0 stored=10000000000 lookup=1
```

Inputs:

- int: `00000000000000000001`
- long: `0000000000000000000001` (R3-02)
- both floating types: `1.000000000000000000000000000000000000000000000000e10`

The int index encoder truncates at 15 characters, losing the final 1. Both floating index encoders truncate at 47 characters, losing the exponent. Scan criteria parse the complete text; indexed lookups therefore miss writable values. This is a new write/query disagreement introduced by widening the write parser's accepted span, even though the buffers themselves predate Phase 1.

Required correction: make all in-scope scalar textual index encoders parse the same complete trimmed span accepted by writes, with identical successful conversion results. Keep any intentionally deferred invalid-criterion policy explicit. Add public-route parity tests before/after add-index for these integer and floating examples, plus the already identified binary32 double-rounding literal `1.0000000596046448` and numeric whitespace/rounding examples. Do not fix this by rejecting ordinary valid leading-zero literals.

## R3-02 — P2: integer leading-zero regression remains unresolved

Plan line 195, `parse_full_i64`: `if (vlen == 0 || vlen > 20) return -1;`.

The revision fixes significant-digit counting only in conv_numeric_text. The shared integer parser still rejects the valid 22-character long literal `0000000000000000000001`, demonstrated above. The original long encoder accepts this literal as 1; the proposed patch changes it into a syntax error. The previously reported leading-zero issue included the integer parser, so the claim that all of it is resolved is premature.

Required correction: validate the full span and use checked magnitude accumulation without the raw 20-character cap. Leading zeros do not overflow. Test positive/negative long, int and byte forms with more than 20 leading zeros, signed boundaries, and actual overflow. Coordinate with R3-01 so longer accepted spellings also remain findable through indexes.

## R3-03 — P2: contradictory integration instructions still prevent an unambiguous execution

- Task 1 correctly places typed_type_name in type_desc.c at lines 581 onward, but lines 613–615 explicitly say it is in config.c and no type_desc.c edit is needed. Following the latter instruction omits the definition.
- Task 4 names the aggregate functions backwards: the two-line division at query_aggregate.c:1311 belongs to decode_index_key_to_double; the one-line division at :2060 belongs to typed_field_to_double. Both replacement bodies are usable, but neither named-function/quoted-anchor pair matches. The plan's mandatory anchor-mismatch halt would stop execution.
- The scope paragraph and consumer inventory still defer encode_field_for_index unchanged to Phase 5, contradicting Task 3b's necessary Phase 1 edits.

Required correction: remove the stale alternative instructions, correct both function names and their inventory entries, and explicitly include Phase 1 successful-literal lookup parity in the scope. Check the complete integrated patch in scratch against the actual translation units; compiling isolated helpers cannot catch omitted definitions or signature/caller mismatches.

## R3-04 — P2: the execution procedure still manufactures red after production edits

Plan lines 1556–1559 instruct the executor to apply Tasks 1–6, add the test, then temporarily revert the widening hunk and checked dispatch to obtain red. This is the previously flagged production-first sequence. It neither captures the regression against the actual baseline nor proves that the numeric/index/renderer changes are exercised: those changes remain applied during the manufactured red run.

Required correction: add/register Task 7 and the new parity cases first, build and capture the targeted failure against unchanged production code, then apply Tasks 1–6 and capture targeted green plus the normal full suite. Remove the temporary rollback procedure. Preserve the documented end-of-series sanitizer timing.

## Verification and boundary

Scratch artifacts: `/tmp/shard-phase1-rev3-review-rma6vo7_/` (core.c, scratch config.c, executable, behavior.log). The harness uses real config.c and util.o, with diagnostic type-name/global stubs; it is focused behavioral evidence, not a full integrated build or public-route suite.

Protected certification plan SHA256 remains:
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

No implementation approval is implied. After these corrections, review the integrated scratch patch and public-route regression evidence before authorizing production execution.
