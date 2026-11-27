# fix/audit-igb-bitmap-vlen-cap — AUD-QRY-6

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (post-triad
queue). Finding: `AUD-QRY-6` in `docs/plans/2026-11-19-audit-index.md`
(major: memory-safety + silent wrong groups — stack OOB read on bitmap
group values longer than 1024 bytes). Branch name:
`fix/audit-igb-bitmap-vlen-cap`. Branch base: after
`fix/audit-igb-bitmap-256-cap` (AUD-QRY-5) — same callback, and this
plan's fix extends QRY-5's overflow latch; base = harness working tree
stacked accordingly, or `main` if landed. Sequence: QRY-5 first.

## Brief (approval surface)

### Problem and root cause

`bm_collect_uniq_cb` (query_aggregate.c:4721-4733) copies
`min(vlen, 1024)` bytes into the 1024-byte collection rows but records
the **full** `vlen` in `vlens[]`. For any bitmap dict value longer than
1024 bytes (bitmap dict values may be up to 0xffff bytes — bitmap.c:1121
and 1152 refuse only `vlen > 0xffff`; >1024 B values DO reach the dict:
CRUD builds raw payloads via `build_index_payload_from_record` with a
SIZE_MAX budget, index.c:1535-1538 / storage.c:609-611 → `bitmap_update`
→ `bm_set` → `bm_dict_add`), two defects follow:

1. **OOB read inside the callback itself**: the dedup comparison
   `memcmp(c->vals[i], value, vlen)` (query_aggregate.c:4724 — guarded
   only by `vlens[i] == vlen`, so a second occurrence of a >1024 B
   value triggers it) reads `vlen` bytes from a 1024-byte stored row —
   up to 64511 bytes past the row.
2. **OOB reads downstream + silent wrong groups**: the recorded full
   `vlen` feeds `bm_popcount_one_value` (6062-6064 → workers →
   `bm_count` → `bm_dict_lookup`'s `memcmp(p+2, value, vlen)`,
   bitmap.c:612), `bm_walk` (6155-6156; Phase 1b via
   `bitmap_emit_for_shard` 6281-6284 → `bm_walk` query.c:3915) — all
   reading `vlen` bytes from the 1024-byte row. The truncated copy
   matches its dict value only if the next collected row happens to
   begin with the stored value's tail `vlen − 1024` bytes — and even a
   crafted match ends in E2BIG at emission (6073/6165), never a wrong
   count; the real outcome is count 0 → the group is
   silently dropped at emission (`if (total == 0) continue;` 6069;
   `counts[i]==0` skip 6164) — a 200-OK with missing groups, while the
   identical query on a no-index twin returns the **documented E2BIG
   error** (group labels >511 bytes carry an explicit error contract:
   docs/query-protocol/aggregate.md:149-151, produced by
   `agg_report_error(E2BIG)` query_aggregate.c:2346 via `agg_row_label`'s
   `n >= cap` check at 1398 over `char gbuf[MAX_FIELDS][512]` at 2845).

Note on the display path: `agg_index_label` itself is NOT an OOB
consumer — `plen >= cap` short-circuits at query_aggregate.c:729 before
any read (those sites error E2BIG at 6073/6168). The real OOB sites are
the callback dedup and the dict-lookup paths above.

ASan-detectable (stack-buffer-overflow READ). Found by the AUD-QRY-1
round-3 blind review.

### Logical flow

1. The collection callback refuses to collect values longer than the
   1024-byte row: latch the same overflow signal AUD-QRY-5 introduced
   (`c->overflow = 1`) and return.
2. The post-collection routing must consult the latch BEFORE the
   `bm_n == 0` early-outs: a dict holding ONLY long values collects
   nothing (`bm_n == 0`) AND latches — that combination must route to
   the record-scan executor (which returns the documented E2BIG error
   for the over-512 B labels), not to the `igb_done = 1` empty-success
   path (query_aggregate.c:6057 / ~6231).
3. Post-fix end state (honest): for objects with any >1024 B bitmap
   group value, `group_by` returns the SAME documented E2BIG error as
   the no-index twin — error parity, not "exact groups" (exact groups
   for >511 B labels is not a servable outcome under the wire contract).
   Objects with all-short values keep the fast path unchanged.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Refuse collection for `vlen > 1024` → latch → scan executor → documented E2BIG parity | (b) variable-length storage honoring vlen up to 0xffff (keeps a fast path that would still E2BIG at emission); (c) record the truncated length as `vlens[n] = cp` | (a/b) (this option) is the audit-minimal fix and lands on the wire contract's own error; the scan executor's E2BIG is the documented, oracle-consistent outcome; (b) moves collection to the heap for values that can never render as groups anyway; (c) REJECTED — consistent truncation still never matches → silent wrong groups survive | DEFAULT |
| One shared latch (QRY-5's), two trigger conditions; latch checked before both `bm_n == 0` early-outs | separate flags; leave the early-out order alone | one latch = one fallback; the ordering fix is REQUIRED for the long-values-only class (else silent empty success survives — round-1 major) | DEFAULT |
| Fix snippet bounds the refusal with `sizeof(*c->vals)` (the actual row width), not a bare 1024 | hardcoded literal | survives any future row-width change; same value today | DEFAULT |

### Blast radius

- Consumers: `bm_collect_uniq_cb` + the post-collection guards in both
  bitmap IGB blocks. Queries affected: group_by over a bitmap field
  holding any >1024 B value — today silently wrong + OOB, after the fix
  the documented E2BIG error (parity with the no-index twin).
- Memory safety: the OOB reads disappear (ASan gate proves it).
- No wire/protocol/on-disk change; inserts unaffected.

### Test strategy

- New product regression case `test-igb-bitmap-long-value` (Task 1):
  bitmap-indexed varchar with values >1024 B (≥2 occurrences each —
  the dedup memcmp is where the first OOB fires); asserts ERROR PARITY
  between the bitmap twin and the no-index oracle (both E2BIG) and
  silence-vs-error mismatch on base (RED); a long-values-only object
  pins the guard-ordering fix; short-values control stays on the fast
  path.
- The **ASan gate run of this case is the memory-safety proof** (base
  run under ASan must report the stack-buffer-overflow — captured as
  the Task 0 artifact before the fix).
- Differential spot-check: the diff's bitmap twin uses short values —
  no delta expected; record DIVSEC.
- Full suite + sanitizer gate once each (no locks/lifetimes touched).

### Out of scope

- Variable-length collection storage (perf follow-up, own plan).
- AUD-QRY-5's cap logic beyond the shared latch (its branch, ahead in
  the queue; note: if the whole stack is uncommitted, a
  `git stash push -- src/db/query_aggregate.c` revert lifts BOTH fixes —
  the revert-prove red still proves this defect; record the expected
  state in the prove notes).
- Raising the 511-byte label cap (wire contract, docs/query-protocol/
  aggregate.md:149-151 — product decision, not an audit fix).

### Review log

- Round 1 (blind reviewer, 2026-11-20): **1 blocker, 1 major, 7 minors,
  all folded in.** BLOCKER: the plan's post-fix claim ("exact groups
  via the scan executor") contradicted the documented 511-byte label
  contract (docs/query-protocol/aggregate.md:149-151,
  `agg_report_error(E2BIG)` query_aggregate.c:2346, `agg_row_label`
  1398/2845) — the oracle twin itself errors E2BIG on 2000-byte values,
  so the promised GREEN was unsatisfiable; fixed: post-fix end state
  = documented E2BIG PARITY, test restructured around
  error-classification equality. MAJOR: guard-ordering gap — a dict
  holding ONLY >1024 B values latches with `bm_n == 0`, and QRY-5's
  guard sits after the `bm_n == 0 { igb_done = 1; }` early-outs →
  silent empty success would survive the fix; fixed: latch checked
  before both early-outs + a long-values-only Task 1 case. Minors:
  `agg_index_label` removed from the OOB-sites list (E2BIG
  short-circuit at 729 — not an OOB consumer); "practically never"
  wording; Task 0/1 ordering made explicit; `sizeof(*c->vals)` bound;
  fixture rows-identical + unique-key guidance; out-of-scope wording
  (E2BIG is an error, not display truncation); stash-state note.
  Reviewer independently verified: all OOB sites (callback dedup 4724,
  dict-lookup memcmp bitmap.c:612/1255, `bitmap_emit_for_shard` →
  `bm_walk` query.c:3915), the 0xffff dict caps (1121/1152), insert
  reachability of 2000-byte values (index.c:1535-1538,
  storage.c:609-611), eligibility/fixture syntax, ASan soundness, and
  gate compliance.
- Round 2 (blind reviewer, 2026-11-20): **0 blockers, 1 major,
  2 minors, all folded in.** MAJOR: Task 0's ASan red was
  unproducible from lv_bm/lv_only as specified — every OOB read starts
  at `&bm_vals[i]` with `i < bm_n ≤ 4` inside the 262,144-byte
  `uint8_t bm_vals[256][1024]` stack array, and ASan redzones fire
  only on reads crossing the ARRAY end (fixture max read end ~6 kB);
  both stated widening knobs were mechanically incapable (vlen dict
  cap 0xffff → max end 69,631; dedup keeps distinct values only, so
  occurrences never add rows). Fixed: separate ASan-only `lv_wide`
  fixture (bitmap(300), 255 distinct short + 2 distinct long values —
  read end 263,120 > 262,144), detectability boundary written into
  Task 0, explicit warning not to widen lv_bm/lv_only (QRY-5's latch
  would flip base to ERR and kill assertion 1's red). Minor: Brief's
  "practically never matches" replaced with the precise
  truncated-tail-match argument (crafted match still ends E2BIG at
  emission — never a wrong count). Minor: Task 2.2 now names the
  Phase 1b latch variable (`bm_over_h`). Reviewer re-verified fresh:
  all OOB sites, the 0xffff dict caps, insert reachability, both
  `bm_n == 0` early-outs and their consumers, the E2BIG chain,
  lv_plain/lv_only base traces, and that round-1 fixes were genuinely
  folded (E2BIG-parity contract, classification-equality structure,
  lv_only ordering pin, sizeof bound, Task 0/1 ordering, stash note).

---

## Plan (executor-facing)

### Ground rules

Branch base per the header; the base MUST contain QRY-5's latch (if
AUD-QRY-5 has not landed, halt per PLAN_NOTES — this plan builds on
it). Base must contain `src/test/audit/` for the Task 4 differential
spot-check. Work uncommitted; path-limited stashes only; build/test per
AGENTS; `PLAN_NOTES.md` + halt on mismatch; one heavy job at a time;
`/tmp/shard-db-*` removed after every run.

### Task 1 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_igb_bitmap_long_value.c`, registered
`TEST_REGISTER("test-igb-bitmap-long-value", ...)`, wired into `build.sh`
next to the aggregate cases (anchor: the
`src/test/cases/test_aggregate_type_matrix.c \` line at build.sh:233).
Fixture (both objects carry IDENTICAL rows — same keys, same values;
keys unique):

- `lv_plain` — no indexes (oracle; fields `v:varchar:4096`);
- `lv_bm` — `indexes: ["v:bitmap(8)"]` (bitmap(8) syntax per
  test_bitmap_index.c:883/920), same fields;
- `lv_only` — `indexes: ["v:bitmap(4)"]`, rows with ONLY long values
  (the guard-ordering case).
- `lv_wide` — ASan-only fixture (NO functional assertions; exists
  solely to make the Task 0 memory-safety proof producible — see Task
  0's detectability boundary): `indexes: ["v:bitmap(300)"]`, same
  fields; ~255 DISTINCT SHORT values (1 row each, so the collection
  rows fill to index 254) + 2 DISTINCT LONG values (2000 B, repeated
  a few times each so the dedup memcmp fires). ERR on base and
  post-fix alike (base: 255-distinct QRY-5 cap latch → scan → E2BIG;
  post-fix: same) — no classification-change assertion on this object.
  Do NOT widen lv_bm/lv_only instead: a >256-distinct lv_bm would be
  ERR on base too (QRY-5's cap latch, present in this branch's base)
  and assertion 1's ERR-vs-OK mismatch red would vanish.

Rows per object: 50 on lv_plain/lv_bm — 3 distinct long values
(`L0`/`L1`/`L2`, distinct 2000-byte strings, 10 rows each) + 2 short
values (`S0`/`S1`, 10 rows each); lv_only: 30 rows over 3 distinct
long values. Requests: no-criteria `group_by=v` with a count spec.

Classify each response as `ERR` (contains `"error"`) or `OK-with-labels`
(extract the label set + per-label counts). Assertions:

1. On base: `lv_plain` → `ERR` (documented E2BIG for >511 B labels) and
   `lv_bm` → `OK` with ONLY the short labels (silent under-report) —
   the MISMATCH is the defect. RED assertion: the two classifications
   differ.
2. Post-fix (and this is the GREEN contract): `lv_plain` and `lv_bm`
   classify IDENTICALLY — both `ERR`, same error text (the documented
   E2BIG response; pin the exact substring from the actual response at
   execution, not from this plan).
3. `lv_only` post-fix → `ERR` (not a silent empty group list). On base:
   record what it returns (expected `OK` with zero groups — the
   round-1-major state); the assertion pins the post-fix contract.
4. Short-values control: a third object pair with only short values →
   `OK` on both, label sets + counts equal (fast path stays correct).

Test-first proof: assertion 1 RED on the normal build (mismatch), 2-3
FAIL as "not yet true", 4 GREEN. Paste. (The ASan red is Task 0.)

### Task 0 — capture the ASan red artifact (base build; runs the Task 1 case)

Detectability boundary (round-2 major — read before running): the dedup
memcmp starts at `&c->vals[i]` with `i < bm_n` inside the
`uint8_t vals[256][1024]` stack array (262,144 B total); ASan fires only
when a read crosses the ARRAY end, i.e. read end `i*1024 + vlen >
262,144`. The lv_bm/lv_only fixtures (≤4 collected rows, 2000 B values)
peak at ~6 kB — ASan stays silent there, and the fallback knobs
"longer value" (dict caps vlen at 0xffff → max end 69,631) and "more
occurrences" (dedup keeps DISTINCT values only) cannot close the gap.
The firing shape is: ≥256 distinct values so the LAST collected row
(index 255) holds a long value, plus one more long-valued row — 255·1024
+ 2000 = 263,120 > 262,144 (for 65535 B values: row ≥ 203). That is
what `lv_wide` (bitmap(300)) is for.

Build `BUILD_MODE=asan`, run the Task 1 case on base → ASan must report
the stack-buffer-overflow READ in `bm_collect_uniq_cb`'s dedup memcmp
(originating from the lv_wide object's group_by). Capture the report
into `docs/plans/audit-artifacts/AUD-QRY-6-asan-base.log`. If ASan does
NOT fire on lv_wide, verify the collected row count actually reached
255 with a long value at index ≥255 (PLAN_NOTES the observed `bm_n`)
before any fixture adjustment — never loosen assertions; if it still
cannot be produced, PLAN_NOTES + halt. Then rebuild normal for the
functional runs.

### Task 2 — the fix

In `src/db/query_aggregate.c`:

1. `bm_collect_uniq_cb`: FIRST thing in the callback (before the dedup
   memcmp — that memcmp is itself the OOB site):
   ```c
   if (vlen > sizeof(*c->vals)) {
       /* AUD-QRY-6: the collection rows are sizeof(*c->vals) bytes; a
          longer dict value cannot be stored or compared without
          reading past the row (the old code copied
          min(vlen,sizeof) but recorded the full vlen, so the dedup
          memcmp and every dict-lookup consumer read vlen bytes from a
          narrower row). Route the query to the record-scan executor,
          which returns the documented E2BIG error for over-cap
          labels — parity with a no-index object. */
       c->overflow = 1;
       return 0;
   }
   ```
2. Guard ordering (REQUIRED — round-1 major): in BOTH bitmap IGB
   blocks, the latch must be consulted BEFORE the `bm_n == 0` early-out
   (`if (bm_n == 0) { igb_done = 1; goto igb_skip; }` at 6057; the
   Phase 1b equivalent at ~6231). A dict holding only long values
   collects nothing AND latches; `igb_done = 1` there would emit a
   silent empty success. Correct shape (latch variable per block:
   `bm_over` in the count-only/can_slot block, `bm_over_h` in the
   Phase 1b block — QRY-5's in-loop OR-accumulated latches):
   ```c
   if (bm_over) { goto igb_skip; }              /* AUD-QRY-5/6 */
   if (bm_n == 0) { igb_done = 1; goto igb_skip; }
   ```
3. Update the callback's header comment to state the row-width bound,
   the OOB it prevents, and the E2BIG-parity outcome.

The scan executor's E2BIG emission needs no change (existing
`agg_row_label` contract).

### Task 3 — prove and gate

1. Revert-prove-paste: `git stash push -- src/db/query_aggregate.c`
   (never a bare stash; NOTE in the prove output if this lifted QRY-5's
   hunks too — expected when the stack is uncommitted) → rebuild →
   Task 1 assertion-1 red + assertions 2-3 unmet → `git stash pop` →
   rebuild → all green. Paste both.
2. Targeted case + full suite `run-all --jobs 8` fresh.
3. ASan proof of the fix: `BUILD_MODE=asan` → the new case runs clean
   (no OOB report) → then the full `run-all --jobs 6` gate;
   `BUILD_MODE=tsan` → `run-all --jobs 3`. Rebuild normal.
4. Differential spot-check — no delta; record DIVSEC.
5. Changelog convention check + entry if per-fix.
6. Index: AUD-QRY-6 → `fix-branch` with this branch name + the ASan
   artifact reference; correct the row's consumer list
   (agg_index_label is E2BIG-short-circuited, not an OOB consumer).

### Embedded execution rules

Tasks in order (Task 0 uses the Task 1 case; write the case first);
both revert outputs pasted; no unrelated changes; `PLAN_NOTES.md` +
halt on mismatch.
