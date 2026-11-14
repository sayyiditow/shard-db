# R2 — exact filtering, guards, indexes, ordering, joins and cursors

## Status

Executed and independently reviewed on 2026-10-29 against R1 commit
`bbb62486e72f14db69f4533be812fbf19fb410ab`. This is the retained scope,
decision and acceptance record. Production source and regression tests are
authoritative. The original anchored execution proposal, codebook, manifests,
probe drivers, prototype snapshots and raw logs are preserved locally and
intentionally excluded from version control.

Final normal build: warning-free. Fresh full suite: **21,719 passed, 0 failed,
across 537 cases**. Raw-diff review and follow-up repair review are complete.
No sanitizer pass is claimed here; the human owns those runs before the final
authorized push/PR. Public packed binding remains inactive.

See the [parent recovery plan](2026-10-19-type-correctness-recovery-plan.md) and
[Phase 3 work index](2026-10-23-phase3-packed-records-plan.md).

## Goal and confirmed contracts

Saved financial values must be found, compared, joined and ordered exactly,
with equivalent indexed and scan answers. Invalid predicates and oversized
indexed values fail explicitly before publishing partial results.

- Compare query literals by mathematical value in legacy and packed layouts.
  Assignment rounding stays unchanged. A scale-2 assignment of `0.145` stores
  `0.15`; `eq 0.145` does not match it, while `eq 0.15` does.
- Validate actual NEW ordered keys before segment staging in both layouts.
  The ordered-key budget is 544 bytes including tier/composite overhead.
  Bitmap/trigram payloads use their own actual-size allocation path.
  Rejected fresh rows publish no live row/index/count; rejected updates preserve
  OLD bytes and indexes.
- Preserve per-row/per-window bulk outcomes. Do not invent whole-request
  atomicity, classify invalid input as a condition skip, or count errors as
  success. Preserve durable commit intent and forward replay after intent.
- Preserve locks, lock order, per-shard scheduling and reader blocking.
  No writer reservation, stronger snapshot, B+ tree magic roll or engine redesign.
- Keep legacy interpretation and public activation unchanged. R5 owns migration
  of historically incomplete indexes and durable eligibility evidence; R6 owns
  explicit activation and application acceptance.

## Executed delivery sequence

1. **Predicates/guards:** full operand bytes, checked compilation, exact decimal
   comparison and runtime-error propagation distinct from false matches.
   Shared aggregate filter callers are updated; arithmetic remains R3.
2. **Ordered keys/producers:** decimal ordering and schema-aware typed index
   builders for records, literals, composites and OLD keys. Single/bulk NEW
   values undergo actual-key admission before staging. Rebuild, bitmap and
   ordered-walk consumers use generation-correct encodings.
3. **Ordering/cursors/joins:** exact typed sort keys, ordered bounds, cursor
   resume and join resolution; checked full-length row rendering and existing
   memory accounting rather than truncation or success-shaped error output.
4. **Failure integration:** allocating bitmap/trigram insert preparation fails
   the window on errors; parallel intersect workers carry errno through join.
5. **Candidate verification:** legacy display decoders cannot drop packed zero,
   empty varchar or zero UUID/IP candidates. Bitmap survivors undergo full
   record verification before counting. Buffered find applies pagination after
   criteria, exclusions and joins, not after a fixed candidate count.

Implementation: `src/db/decimal_order.*`, `typed_index.*`, `query*.c`,
`query_internal.h`, `index.c`, `storage.c`, `typed_bulk_insert.*`,
`typed_read_command.c`, `typed_row.c`, `types.h` and `build.sh`.
Relevant index, ordinary-integration and bulk protocol docs are updated.

## Acceptance and review repairs

Tests were added before implementation and checked against reverted engine
changes. Predicate, admission, rebuild, ordering, join, output and error probes
were reversed/restored. The independent Python Decimal oracle checked 4,010
values and 100,000 comparison pairs.

- Allocation/worker propagation: fixes reverted with hooks/tests retained,
  **39/11**; restored **50/0** in `test-alloc-error-propagation`.
- Zero/empty/NULL candidates: physical packed and request-layer regressions
  require indexed/scan parity. The packed fixture restores its process root
  on all returns.
- Final count/pagination repair: identical tests on old engine **200/4**;
  repaired engine **204/0 across three cases**. Old mixed-index count returned
  2 versus full scan 0; pages returned 0/1 instead of 1/2 rows. UUID display
  patterns retain current typed matcher semantics; no new operator support is
  introduced. Pagination uses supported packed numeric `exists` with an
  unindexed integer postfilter.
- Request-flush overlap test: boundedly wait for the unpaused sibling's
  visibility before preserving the exactly-one assertion. Caller-serial
  dispatch remains RED; normal parallel dispatch is **273/0**. Request B's
  completion never synchronized A's sibling.

Final normal commands:

```text
SKIP_TESTS=1 ./build.sh
# exit 0, zero warnings
./build/bin/shard-db-test run-all
# total: 21719 passed, 0 failed across 537 cases
```

Focused bitmap/intersection, nested keyfile-lock, cursor/total, join, predicate
and allocation regressions pass. Diff whitespace checks pass. No benchmark or
sanitizer result is inferred from normal-suite acceptance.

## R3 handoff and remaining ownership

R3 owns exact aggregates, grouping/having, parallel merges, analytical fast
paths and all aggregate output, plus the remaining-consumer closure census
after R1–R3. Preserve the parent's result-scale, overflow, NULL, memory-budget
and failure contracts.

Confirmed private packed numeric reproducers (indexed values 0, 1, 2 and NULL):

- `decode_index_key_to_double` in `query_aggregate.c` interprets new ordered
  decimal payloads as legacy int64. Indexed min/max returned values around
  -904322805175995 instead of 0 and 2.
- Indexed group-by label decoding through `decode_idx_to_buf` collapsed the
  object into one unlabeled count bucket instead of value/NULL groups.
- Audit related same-field min/max, no-criteria aggregate fast paths, top-N
  numeric emission and indexed-group secondary-map labels. Fix the consumer
  class, not only the two observed symptoms.

R4 retains calendar/bool/UUID/IP/enum/string grammars; R5 retains authoritative
metadata/loading, durable publication, schema conversion and migration/recovery;
R6 retains activation, finance acceptance, clients/docs and final safety gates.
The separate durability certification rig remains protected later work: do not
read, edit, move, stage or execute it.

## Deferred buffered-fetch optimization

After type correctness, plan incremental fetching for unordered buffered indexed
find. Today it gathers candidate hashes, fetches all candidate records in
batches of 1,024 and retains them before filtering/emission. Fetch/filter/release
bounded batches instead, requesting more only until `offset + limit` matches
survive criteria, exclusions and joins, or candidates run out. Preserve memory,
deadline, error and output semantics; a sort needing unseen rows cannot stop
early. Candidate-hash streaming and adaptive sizing are separate choices to
justify with measurements. This performance follow-up must not broaden R3.
