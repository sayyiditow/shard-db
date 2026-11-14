# Phase 2 post-fix raw-diff re-review

Date: 2026-10-23. Baseline HEAD: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`.
Disposition: **RAW-01 and RAW-02 closed; no new actionable findings.**
This supersedes the blocking disposition in the earlier raw-diff review;
that historical report remains unchanged.

## Standards and independent blind review

No actionable findings in the changed paths. The blind reviewer inspected the
updated raw source/test code without the plan or execution evidence.

- The average now enforces its integer-digit limit after rounding and
  normalization, returns `DEC_ERR_RANGE`, and clears rejected/reused output.
  Positive and negative overflow and valid boundary probes passed.
- Parse clears an initialized/reused output before rejecting NULL text,
  while still rejecting NULL output without dereference.
- The new regression seed has nonzero weight and display scale, exercising
  the cleanup promises rather than passing those assertions vacuously.
- 15000 randomized ordinary averages matched Python Decimal. No project
  edits, sanitizer runs, benchmarks, or full-suite execution by that reviewer.

## Spec review

No actionable findings. Both production fixes and both test insertions match
the Revision 4 post-review amendment. Existing assertions were preserved;
other test-file differences were blank lines.

The spec reviewer checked the saved final RED (897 passed / 12 failed), all
intended failure assertions, warning-free build logs and the full-suite log
(18425 passed / 0 failed across 516 cases). The interim seed correction from
42 to 42000.50 strengthens coverage and does not weaken assertions.
The old initial Task 8 transcript in the evidence document remains historical;
the post-review section records the current final counts.

## Verification and limits

- Actual current focused test independently rerun: **909 passed, 0 failed**.
- Spec reviewer freshly compiled the independent oracle probe against the
  current module: **426 passed, 0 failed**.
- Primary agent freshly compiled an ordinary shared-library scratch probe:
  six signed maximum-boundary cases passed, including overflow rejection,
  reused-output cleanup, the 1000-fractional-digit valid neighbor, and division
  by 2. Every successful result packed/unpacked successfully.
- Primary NULL-input probe confirmed sign, weight, scale, limb count, capacity
  and storage reset; NULL output remains rejected.
- Primary scratch artifacts: `/tmp/p2-rereview-h7dd7r87/`.
- Full suite: **18425/0 across 516**, verified from executor logs, not rerun
  during this re-review. No production build was performed by the primary.
- Protected certification plan verified by hash only:
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
- No source/test/build edits, git writes, sanitizers, benchmarks, or Phase 3
  implementation during review. The branch-series sanitizer gate remains
  scheduled after all fixes, immediately before push/PR.

Reviewed identities:

- `build.sh`: `4aa3bf7f6d871ed576dc7e47113dc2064fec479dff020f91043fe556cb2f0eb1`
- `src/db/decnum.h`: `42a0d82d2b413c1475ebada1902815eeb21dc2f7b298fab8df40bd61e01474ae`
- `src/db/decnum.c`: `0862d1bd26aef401552f6ce2d68b94798c9b6b5d5bf0490c1e34c271d6ec01ff`
- `src/test/cases/test_decnum_core.c`: `db6ed4fca00976e93232ee61aaa0bfe59282e1bf042ceda588318a1e6d439951`
- `docs/plans/2026-10-22-phase2-decimal-core-plan.md`: `3dfb686cf4ea8ccc1941ed1b751d4ab4687621b1957e5b43f119f3289a1c65ce`

Standards/blind axis: **0 findings**. Spec axis: **0 findings**. Phase 2 is
ready for cleanup and a separately authorized local commit. Keep raw logs,
scratch probes and debugging artifacts outside that commit. Phase 3 requires
its own child plan and approval before implementation.
