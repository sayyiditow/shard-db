# R5b plan review — 2026-11-03-r5b-packed-recompose-plan.md

Reviewer: fresh blind agent (no conversation context; plan file + repo at b1cc3b7 only, read-only). Verified 37 code anchors and 9 doc anchors
(all exactly-once, byte-exact), all referenced APIs/signatures, the crash-state table against objlock.c, response shapes against the server, and the
claim that committed R5 docs contain no refusal text. No compile or logic BLOCKER found. Findings and dispositions:

| # | Sev | Finding | Disposition |
|---|---|---|---|
| 1 | MAJOR | `lm_expect_error` would be unused after the flip (-Wunused-function) | Fixed: §4.3 now deletes it and quotes the nine replaced lines (also resolves #6) |
| 2 | MAJOR | Task 3 revert-proof #2 unsatisfiable: `pr_refused` only matched `"error"`, rollback also yields an error | Fixed: refusals assert `Pre-flight failed on field [..]` incl. `dry_run`; revert-proof text corrected |
| 3 | MAJOR | Bitmap (bool/enum) indexes are slot-addressed; rebuild renumbers slots; no test covered it | Added `test-packed-bitmap-after-rebuild` (add/compact/edit after deletes). Failure there = pre-existing engine bug to report, not paper over |
| 4 | MINOR | D-4 reinterpreted as packed twins | Kept; flagged in §0.5 for human confirmation |
| 5 | MINOR | A-4 only partly fixed (`strstr(":removed")` remains in remove/add/edit) | Documented in §0.5 as intentional |
| 6 | MINOR | §4.3 lacked quoted old text | Fixed (see #1) |
| 7 | MINOR | Crash-test object names not distinct; RED is a 20 s timeout | Text corrected |
| 8 | MINOR | `PkrField pf;` maybe-uninitialized | Fixed: `= {0}` |
| 9 | MINOR | Inconsistent JSON-envelope sanitising | Documented in §0.5 as inherited flaw; new messages sanitised |
| 10 | MINOR | Bare `rename()` durability, not power-loss safe | Documented in §0.5; human decision |
| 11 | MINOR | Old row zero vs later insert NULL | Documented (§0.5, docs hunk) |
| 12 | MINOR | `limits.md` not updated | Added §6.4b |
| 13 | MINOR | Missing tests: tombstone + add/edit, numeric NULL, multi-edit, :not_null add, allow_rename | Added `test-packed-edit-field-tombstone-multi`. Composite index remains untested (noted) |
| 14 | NIT | Staged data path wrong; PLAN_NOTES.md exists; baseline count unverifiable; walk-sequential comment | Fixed path; halt file renamed `PLAN_NOTES-r5b.md`; baseline caveat added; comment noted in §0.5 |

Residual: composite-index edit coverage and the segment-limit row refusal are not tested (stated in the plan).

## Post-review human decisions (2026-11-04)

Applied after the blind review, not re-reviewed: A-1 changed to SQL semantics (declared default, else NULL, else refuse for `:not_null` without default; tests updated in `test-packed-add-field*`), D-6 durability (`durability_publish_replace`, §3.9 H15-H17), D-4 twin functions confirmed (to be removed with legacy support). A short re-review of these hunks is advisable before execution.

## Second blind review (post-decision), applied 2026-11-04

1. H16's anchor did not exist at Task 2 (it needs H13's `merged` buffer) -> durable swap folded into H13; H16 is now a pointer; Task 2 text corrected.
2. Crash fixture passed `streams:1` on a packed create (refused unless it equals the host count) -> removed from `create_packed_fixture`.
3. Nits: Task 0 git-status expectation, §3.9 renumbered §3.8.
