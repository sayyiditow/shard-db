# Phase 1 implementation review — 2026-10-22

Baseline: `1dd1888`, branch `fix/phase1-safe-scalar-conversions`, actual
uncommitted diff plus the untracked scalar-conversion regression case.
Reviewed the raw implementation, not only the execution summary. Used
independent standards and specification reviewers via the code-review skill.
No production source edits, commits, benchmarks or sanitizer runs.

The implementation closely follows the fifth-revision child plan. The
remaining findings below are inherited plan/coverage gaps, not claims that
the executor departed from the approved instructions. The original financial
blocker still requires subsequent exact-decimal phases; Phase 1 only makes
legacy scalar conversion safe.

## Standards / correctness

### I-01 — P1: allocation failure becomes a successful zero index key

Location: src/db/config.c:1855–1860, scalar index cases including :2541–2543.

span_to_cstr now allocates on every scalar textual lookup conversion. All
six scalar consumers substitute numeric zero when it returns NULL, and
still emit a valid key of the normal length. This reports neither an error
nor an invalid key. The comment that the scan side cannot allocate either
is incorrect: it can already have the criterion string and parse it without
this new allocation.

Executed a focused fault injection against the actual config.c, failing
only the new three-byte allocation for the input "42":

```text
allocation_failed=0 out_len=4 indexed_value=42
allocation_failed=1 out_len=4 indexed_value=0
```

A failed allocation can therefore make an indexed criterion select zero
instead of 42. Text-to-key mutation helpers also use this converter:
index.c build_index_key_from_json (:1682) accepts the resulting normal-length
key and returns success; index_parallel likewise checks only nonzero length.
Thus callers that persist the result cannot distinguish a valid encoding
from an allocation failure. Do not claim every rebuild path uses this
converter: stored-byte rebuild paths already encode directly.

Required fix: avoid the allocation where possible, or make allocation
failure explicit and propagate it through the complete caller inventory.
Never encode a legitimate zero, silently omit a key, or publish a partial
index on OOM. Add targeted allocation-failure tests for lookup and relevant
mutation consumers, including old-record/index preservation. Reconcile the
child plan's zero-on-OOM instruction with the parent's explicit allocation
status and invariant requirements before updating the implementation.

### I-02 — P2: CSV update loses bytes before full-input validation

Location: src/db/query_bulk.c:4242–4243.

The callback knows cell_len but passes strlen(cellbuf) to the checked
converter. The file path scans mmap bytes by length and does not reject
embedded NULs. The integer cell bytes `12<NUL>junk` therefore become the
valid span "12". Bulk insert supplies its full span and rejects the same
malformed input.

Executed an isolated public daemon with a seeded {"i":7} record and a
raw CSV file containing `keep|12<NUL>junk`:

```text
csv_update {"matched":1,"updated":1,"skipped":0}
after_update {"i":12}
```

Required fix: pass the existing cell_len (or the original cell span)
through validation and preserve its semantics for varchar too. Add a file-
based delimited-update regression showing scalar rejection, skipped/error
tally as documented, and the unchanged old record/indexes. This also closes
a specification gap; the planned Task 2e hunk contains the same strlen bug.

### I-03 — P2: legacy non-finite $copy source coverage was removed

Location: src/test/cases/test_computed_update.c:252–288.

The new boundary rejection checks and finite-copy checks are valid, but
replace both assertions that $copy rejects an already-stored NaN/Infinity
source. Existing roots can contain values accepted by the previous binary;
this phase neither migrates nor rejects those roots. Rejecting new writes
therefore does not make the source guard irrelevant for supported upgrades.
The storage.c:1740 guard remains, but the new tests would not catch its
removal or regression.

Required fix: retain the new write-boundary assertions and restore the
non-finite-source regression using a legacy/raw typed-record fixture. Check
both rejection and unchanged destination. Correct the child plan's claim
that this condition is unreachable: it is unreachable through new supported
writes, not through existing data. This is a missing regression scenario,
not a demonstrated failure of the current guard. The test rewrite was
explicitly approved, so this review does not attribute a process violation
to the executor.

## Specification

### I-04 — P2: scalar JSON strings are not unescaped before conversion

Location: src/db/config.c:3003–3023 and :3583–3605.

Parent Phase 1 requires: "Decode JSON escapes once before this boundary."
Both record encoders only unescape FT_VARCHAR; numeric scalar string spans
are sent directly to encode_field_checked. The child plan reproduces this
omission. A valid JSON representation of integer 1 is rejected:

```json
{"mode":"insert","dir":"rev","object":"n","key":"esc","value":{"i":"\u0031"}}
```

Actual response:

```json
{"error":"value for field 'i' is not a valid int: expected a whole decimal number"}
```

Confirmed independently that the request JSON decodes its i value to the
single character '1'; this is not a literal backslash-u string. Required
fix: decode JSON string values exactly once, preserve the resulting span
length (including NULs), and feed that value to the checked conversion
boundary and corresponding index consumers. Keep non-JSON CSV/default
literals on their own literal rules. Cover single/bulk insert and update
parity, escaped digits/sign/exponent, escaped NUL plus suffix rejection, and
index lookup consistency. Coordinate this with I-02 so unescaping cannot
reintroduce strlen truncation.

The specification reviewer also independently reported I-02 against Task 1's
"Length-aware, full-input parsers ... shared by every value-carrying caller."
No additional executor drift or scope creep was found.

## Evidence and scope checks

- Existing logs verify targeted green 131/0, helper green 10/0 and normal
  suite 17389/0 across 512 cases. Did not rerun an unchanged normal suite;
  the new focused reproductions exercise paths those tests do not cover.
- The new regression is registered once in build.sh. The prior scratch
  integrated-code.patch contained a duplicate registration; the executor's
  correction is right and explains the 513 versus 512 case count.
- Eleven of fourteen compiled-code files hash-match the reviewed snapshot;
  the other three are build.sh (duplicate removal), config.c (include order
  and comment) and query_schema.c (local variable spelling). These are not
  review blockers. Task 8 documentation is present.
- `git diff --check` passes. Ignored graphify cache content is outside the
  implementation; its presence exposes a manifest-tool limitation, not
  production scope creep.
- Protected certification SHA256 still equals
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
- Focused review artifacts: `/tmp/p1-impl-review-x6zudlsw/` (oom.c, oom.log,
  public.py, public.log). The public reproducer uses a fresh temporary root,
  starts the built daemon, and stops its process group in finally. Existing
  database roots were not accessed. OOM probe stubs only the diagnostic
  type-name accessor/global and substitutes malloc for the selected call;
  the actual index encoder is compiled from the current config.c.

## Handoff

Update the child plan for I-01 through I-04, then apply the fixes on this
same uncommitted branch. Keep the existing passing coverage and add focused
red/green evidence for these cases. Run the normal full suite after the
complete fix round. Do not begin Phase 2, commit/push, or advance sanitizer
execution timing. Return the raw diff for review again.

Standards/correctness: 3 findings, worst P1 (OOM changes index key).
Specification: 2 findings including shared I-02, worst P2 (JSON decoding and
full-span validation). Four distinct findings total. Approval pending fixes.
