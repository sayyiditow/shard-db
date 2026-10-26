# Phase 3B.4 Revision 2 — independent plan review

2026-10-26. Parent Phase 3B.3 committed on human instruction as
`a97f9f21caa0afcd29ae54ec665abf79466783ec` on
`fix/phase1-safe-scalar-conversions`. All 12 reviewed parent checkpoint hashes
matched before commit; only the explicit 16-file reviewed code/document allowlist
was staged. Protected file, next plan, raw logs, probes and binaries excluded.

## Independent blind source review

The reviewer inspected proposed final source and actual repo interfaces without
reading the plan narrative or earlier review/evidence. One actionable finding:

**P2 — delimiter-free lines could overwrite OLD with empty values.** The proposed
`bulk_delimited` collector admitted nonempty lines without a separator as a key
and empty value span. On an all-varchar or int/varchar packed object, key-only
input could replace an existing record, unlike the legacy runner's skip rule.

Fixed in the plan's full module block by skipping `key_end == end` before
allocation/write preparation. New named regressions cover LF, CRLF and EOF,
OLD content, absent-key preservation, segment position and counts, plus the
positive guard that an explicit empty column still writes. Test-first evidence
against the unfixed real module: **173 passed / 5 failed**, rc5; after fix:
**178 passed / 0 failed**, rc0. The independent reviewer re-read the fix and
regressions and found no remaining actionable source issue.

Review also checked locked OLD validity, callback/context lifetime, per-row hard
error versus condition skip, actual record fit, allocation cleanup, per-shard
ordering and coordinator ownership. Public packed routing, concurrent same-key
stress, expanded write-failure/deadline integration and later consumers are not
claimed certified by these private fixtures; their owning roadmap gates remain.

## Independent execution-readiness review

A separate reviewer confirmed block hashes, checkpoint 0, unique ordered anchors,
all stage output hashes, five build lists and registration, RED/GREEN sequencing
and halt/review rules. No additional findings. Root revalidated the final amended
42 blocks and literal stage assembly against committed parent sources and the
scratch final code. Driver pins the exact parent HEAD and supports no-write
`--check-only`; stage-1 preflight ran on this real checkout with no source writes.

## Final verification

Ordinary scratch builds only; no production 3B.4 implementation or Git writes:
- Revised stub-linked isolated runner, with private ShardDb owner: RED102/33,
  rc33. Initial revision's normal stub build was warning-free; executor must
  still run the complete normal stage-1 build and capture its RED directly.
- Full normal final build: zero warnings. Focused **178/0**, rc0.
- Fresh normal full suite: **19643/0 across 522 cases**, rc0.
- Final checks runner extracted from the plan: collector requested2/rc0;
  all **90** allocation failures preserve OLD/absence/counts/position/live0.
- Maximum schema: actual2088 bytes; largest selected allocation3072 (<8 MiB).
- Final source bytes match the plan; all block/checkpoint hashes validated.

Raw logs remain under /tmp (p3b4-r2-issue-red.log, p3b4-r2-stub-red.log,
p3b4-r2-green-build.log, p3b4-r2-green.log, p3b4-r2-full.log). Final extracted
checks ran in /tmp/p3b4-check-4bbpjdh8. They are not repository artifacts.

## Disposition

Ready for the human to hand the reviewed plan to the executor. Plan SHA256:
`c06e2b748316ea25b45bdf5c969b5f9bc26a18ef64d53dcfaed1786d841acb6b`.
No Phase 3B.4 production changes, staging, commit, push, PR, activation,
benchmarks or sanitizers. Required executed raw-diff reviews remain mandatory
before the next cleanup/commit. The protected certification plan was accessed
by hash only and remains
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
