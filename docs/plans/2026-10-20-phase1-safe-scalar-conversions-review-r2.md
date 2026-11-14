# Phase 1 plan review, revision 2 — changes still required

Date: 2026-10-20. Reviewed the revised Phase 1 child plan against unchanged
source HEAD `1dd1888fda49ce295717590d1f65415963445270` and the real test headers.
No production source or submitted plan was edited. This is not approval.

## Confirmed improvements

The extracted conversion core and regression case now pass syntax checks.
The core includes the necessary headers and formatting declarations. Direct
probes confirm that 6e-100 at scale 2 now produces zero, 0.145 produces 0.15,
varchar whitespace is preserved, and scale-zero/negative-limit parsing is
accepted. These close real parts of the prior review, but not the end-to-end
behavior or compilation of the integrated patch.

## R2-01 — P1: Lookup parity is still incomplete

Plan line 90 still defers encode_field_for_index and falsely calls it the
exact binary encoder. That function's textual FT_NUMERIC conversion remains
atof-based, unlike typed_field_to_index_key's stored-byte conversion. Task 3
fixes only compiled numeric comparisons; it does not fix the textual bounds
and lookup keys used by index/ordered/composite/join paths. The statement at
lines 854–857 that FT_FLOAT writable values compare identically is also false
when writes use strtof but index literals still use `(float)atof`.

Using the literal proposed parser and the actual unchanged textual index
encoder on the same inputs gives:

```text
numeric:12,2 literal 0.145
  stored scaled value: 15
  textual index/query scaled value: 14
numeric:18,2 literal 90071992547409.91
  stored scaled value: 9007199254740991
  textual index/query scaled value: 9007199254740992
float literal 1.0000000596046448
  stored binary32 rendered at 9 digits: 1.00000012
  textual index/query binary32: 1
```

There is a second parity gap: encode_field_checked trims outer whitespace,
but exported conv_numeric_text does not. Task 3 passes criterion text directly
to the latter and converts failure into zero:

```text
numeric:18,2 literal [ 0.145 ]
  write accepts it and stores 0.15
  conv_numeric_text returns -1; proposed criterion wrapper returns 0
```

Use the same normalization and numerical interpretation for writes, compiled
criteria and textual index-bound/key construction in this phase. This is a
parser correction without an on-disk key-format change. Verify actual indexed
and unindexed equality/range and relevant guard/join routes, not just one easy
unindexed rounding example. Defer unrelated planner changes, not the parity
required by newly accepted exact writes.

## R2-02 — P1: Accepted scale-zero INT64_MIN still renders invalid JSON

The parser now admits the minimum signed scaled value at S=0, but Task 4's
decoder changes only guard nonpositive multipliers. With S=0, the multiplier
is 1 and the existing whole-part formatting negates INT64_MIN in signed
arithmetic. The submitted Task 7 test expressly expects this round trip.

Direct probe with the proposed checked encoder and unchanged numeric decoder:

```text
numeric:18,0 input=-9223372036854775808
  write rc=0
  decoded token=--9223372036854775808
```

The double minus is invalid JSON; the intermediate signed negation is also
undefined behavior. Task 4's zero-multiplier guard does not affect this path.
Fix magnitude rendering without signed-minimum negation in every numeric text
decoder, including typed_field_to_buf_raw and decode_idx_to_buf. Test negative
limit output across record/index-only/grouping/projection formats rather than
assuming corrected parsing implies correct rendering.

## R2-03 — P1: Integrated hunks still do not compile or identify exact sites

Standalone core/test syntax checks miss these integration defects:

- Task 4, lines 940–948, says typed_field_to_buf_raw has no local scale and
  inserts a guard plus `int64_t scale = ...` immediately after the zero check.
  The actual next line already declares scale. Applying that exact insertion
  to the actual function yields `error: redefinition of 'scale'`.
- Task 2c, lines 681–688, replaces a function-definition prefix ending in `{`
  with a declaration ending in `;`, leaving its existing body outside a
  function. Give separate valid definition and header-declaration hunks.
- Lines 566–568 contradict the accessor code at 533–546: they say it is
  defined in config.c and no type_desc.c change is needed, while the code
  correctly places it in type_desc.c. Remove the obsolete instruction.
- Task 4 swaps the names/anchor shapes of typed_field_to_double and
  decode_index_key_to_double. At current source line 1311 the containing
  function is decode_index_key_to_double; line 2060 is typed_field_to_double.
  The specified two-line versus one-line anchor shapes are consequently
  assigned to the wrong named functions. Under the mandatory exact-anchor
  rule this should halt, not invite executor adaptation.

Supply complete unambiguous final hunks and validate their application and
the resulting translation units in an isolated temporary source copy. Do not
claim that helper/test syntax checks establish that the whole patch compiles.
No changes to the user's production checkout are needed for that proof.

## R2-04 — P2: Regression protocol and expected float output remain wrong

Task 7 still uses `rows` for bulk-insert at line 1218; the documented request
member is `records` in docs/query-protocol/bulk.md. With one valid and one
rejected row, the documented response contains an errors count **and**
`"error":"some_records_dropped"`; the test incorrectly asserts the absence
of an error member. Searching the response for any `1` does not verify the
error tally. Freeze the correct request and exact counters now rather than
instruct the executor to discover/adjust them before green.

The float assertion at line 1275 is still `1.23456777`. Direct strtof of
1.2345678 formatted with the proposed %.9g produces `1.23456776` on this
target, as recorded in the previous review. Verify the binary round trip
and its actual expected lexeme independently.

The execution order still applies production Tasks 1–6 before adding the
regression case. Start with the red regression against baseline, then fix;
retain the additional required reverse/reapply proof. Avoid claiming that
base-branch INT64_MIN long/SHORT_MIN cases fail when they already pass, or
that the old 6e-100 bug was the same as the first draft's newly introduced
rounding defect. Label the expected red causes accurately.

## R2-05 — P2: Raw token-length heuristics reject representable zero/small values

The exact-decimal positive-shift path at line 401 rejects total digits >19 or
shift >18 before removing leading zeros or recognizing a zero coefficient.
parse_full_i64 similarly rejects all spans longer than 20 at line 162.

Observed results:

```text
numeric:18,2 input=0e19                   -> rejected (mathematical zero)
numeric:18,2 input=0000000000000000000001 -> rejected (mathematical one)
long input=0000000000000000000001        -> rejected (mathematical one)
```

The frozen grammar permits decimal digits and does not prohibit these leading
zeros. Normalize/significance-check without allocating or looping proportional
to an exponent, then decide range from actual magnitude. Add zero, leading-zero,
sign and rounding-boundary cases. If a separate lexical token limit is wanted,
it must be explicitly reviewed/documented rather than emerge from a magnitude
shortcut. Existing request-size limits still apply.

## Checks performed

Scratch artifacts: `/tmp/shard-phase1-rev2-review-npwiiqan/`.

- Extracted the literal Task 1 core and Task 7 test; both passed syntax checks
  with actual source/test headers. Supplied the proposed type-name dependency
  as a diagnostic stub and included the stated math/stdarg headers.
- Linked the core against the actual config decoder and utility functions in
  an isolated helper harness. No conversion arithmetic was modified. The TLS
  database pointer is a diagnostic null stub; no daemon/root is involved.
- Compared checked record bytes with the actual existing textual index codec.
- Inserted Task 4's literal guard into a copy of the actual raw-decoder body;
  its translation unit fails with duplicate scale declarations. The original
  function/file was not edited.
- Used the current bulk protocol document to resolve the two explicitly
  deferred test-shape questions.
- No full proposed patch, daemon test suite or sanitizer success is claimed.
  Production remains unchanged; the green baseline is not disputed.
- Protected certification plan SHA-256 is unchanged:
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

## Next step

Resolve R2-01 through R2-05 in the planning documents, validate a complete
scratch application of the patch as well as the focused helpers, and resubmit.
Keep production implementation at the approval boundary. Do not commit,
push, change branch or execute the protected certification plan.
