# Phase 3 planner handoff — packed numeric/varchar records

Phase 2 is complete, reviewed and ready to serve as the next local commit
checkpoint on `fix/phase1-safe-scalar-conversions`. Pin the actual HEAD before
planning; work on top of the Phase 2 commit, preserving Phase 1.

**Planning only.** Deliver a complete child plan for Phase 3 of the parent
recovery plan; do not implement, stage, commit, push or start later phases.
Read AGENTS.md, CORE-PROCESS.md, the parent recovery/acceptance/baseline plans,
and the completed decimal API/tests. Historical Phase 2 halt/resume instructions
are not current execution instructions. Hash-check the protected certification
plan only; never read, edit, stage or include it.

## Required planning work

1. Inventory every schema/layout consumer and field-access path before
   proposing a format or signature change. Include typed record views, null
   bitmap and absent fields, field pointers, decoding, transforms, zero buffers,
   build lists, scans, recovery and old/new object interoperability.
2. Specify versioned object metadata and a known record flag for the packed
   layout, with crash-safe downgrade protection. Headerless old schemas/records
   must retain their legacy interpretation. Use the existing decimal payload
   and validated offset/length descriptors; store small values compactly.
3. Preserve bare numeric limits (131072 integer / 16383 fractional digits),
   declared numeric precision/scale semantics and bare varchar capacity
   **4194304 bytes**. Explicit varchar:N retains its requested capacity. Audit
   integer widths so four-MiB capacities and maximum records cannot truncate.
4. Recompute worst-case max_value/slot_size for MAX_FIELDS=256 and test packing
   separately from schema maxima. For an all-variable-varchar schema, the
   frozen descriptor/null-bitmap arithmetic gives:
   `33 + 256 * (4194304 + 8) = 1073743905` value bytes. With a 24-byte record
   header and max_key=1024, align-8 gives slot_size `1073744960` bytes.
   These numbers fit uint32 but exceed the current 128-MiB segment ceiling.
   Verify assumptions against source, then explicitly resolve the row/segment
   policy for review. Do not reuse old 65535-varchar ceiling calculations or
   silently lower the agreed field limit/increase request or query caps.
5. Replace fixed-output assumptions with sizing/bounded composition and
   validated schema-aware spans. No giant stack arrays or undersized global
   zero-field backing stores. Preserve record headers unless evidence demands
   a change; actual vlen and maximum validity bounds remain distinct.
6. Specify RED-first coverage for small/mixed variable fields in every position,
   multiple variable fields, NULL versus empty/zero, large and boundary values,
   malformed/truncated/overlapping descriptors, mixed old/new objects and
   maximum schema sizes. Assert encoded lengths and segment vlen directly.
7. Provide complete code blocks, unique quoted anchors, consumer inventory,
   intermediate build/test expectations and ordinary verification commands.
   Planning-time integration checks must use complete real build lists, not
   object compilation alone. No sanitizers or benchmarks in this phase;
   end-of-branch sanitizer gates remain unchanged.

Deliver the child plan for human review/approval and stop. Public write/schema,
criteria/index, aggregate and migration behavior remain governed by their later
approved phases. Any prerequisites crossing those boundaries must be surfaced
as a decision, not silently implemented.
