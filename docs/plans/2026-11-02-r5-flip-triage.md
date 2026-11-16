# R5 Task 11 — default-flip triage (2026-11-02)

One-line production change (11a: `create_packed` default 0 → 1). Full suite, fresh, disk-backed: **2,402,132 passed / 347 failed across 568 cases; 54 cases with ≥1 failure.** Per 11c: triage only — NO test edit, NO code fix, NO assertion change until the human has read this file. Totals before the flip (Task 10): 2,402,509 passed / 1 failed (the then-known r1-routes item).

Read this with D9 in mind: add-field / edit-field / vacuum compact on packed objects are fail-closed refused BY DESIGN in R5 (R5b restores them before R6). Cases failing for that reason are classification 1: the pre-authorised edit is adding `"declaration_layout":"legacy"` to that case's create-object request(s).

| Case | First failing assertion | Classification |
|---|---|---|
| test-secure-random-keys | bulk: status OK | 3 — defect: DEFECT-suspect: bulk status/count fail on packed (needs root-cause) |
| test-runner-parallel | sequential run-all exits 0 | 1 — fixture: meta-test spawns run-all; follows the rest of the triage (re-greens after fixture edits) |
| test-topn-stream-bitmap-postfilter-bounded | 45k-row bitmap fixture bulk insert succeeds | 3 — defect: DEFECT-suspect: 45k-row bulk insert into packed fixture errors (error text not in TAP; needs case-log root-cause) |
| test-heavy-vacuum-transaction | heavy vacuum uses transactional rebuild | 1 — fixture: vacuum compact refused on packed (D9; R5b restores) |
| test-r1-routes | legacy layout identity explicit | 2 — difference: asserts legacy identity + old F5 rejection; describe now says packed |
| test-layout-startup-agreement | bare varchar + no token: cross-check refuses (current behaviour) | 1 — fixture: helper creates packed by default now; case needs legacy fixtures (lm_build seam) |
| test-composite-varchar-bound | lower bound length = 4 | 2 — difference: numeric index-key bytes differ under exact packed encoding (btree bound expectations) |
| test-loader-packed-edge-cases | start | 1 — fixture: my helper fixtures: lm_build creates packed by default; helpers need the legacy seam / no double token-append |
| test-layout-token-with-auto-key-malformed-refuses | malformed 8th token refuses startup (not silently truncated) | 3 — defect: DEFECT: line already 8 parts (auto_key + layout from packed create); a 9th token is SILENTLY DROPPED by the 8-cap in startup_schema_line (hunk 1f traded silent-drop-at-7 for silent-drop-at-8). Needs: refuse when more tokens exist beyond the cap |
| test-loader-same-object-two-tenants | restart | 1 — fixture: same helper issue (create now packed; token appended again) |
| test-loader-packed-with-legacy-fields-refused | daemon starts | 1 — fixture: same helper issue: token now written by create, case appends a second ⇒ strict duplicate-token refusal ⇒ daemon refuses; duplicate-token refusal itself is BY DESIGN |
| test-loader-binds-packed | start | 1 — fixture: same helper issue (double token-append on packed-created object) |
| test-layout-token-packed-loads | start with token | 1 — fixture: same helper issue |
| test-scalar-grammar-routes | legacy varchar rejects decoded NUL (D7 unchanged) | 1 — fixture: legacy-grammar fixtures |
| test-scalar-conversion-safety | insert rejected | 1 — fixture: legacy numeric/grammar renderings |
| test-edit-long-default | long default accepted | 1 — fixture: edit-field refused on packed (D9) |
| test-long-field-def | C: add-field long enum succeeds | 1 — fixture: add-field/enum-append refused on packed (D9) |
| test-not-null-edit-gate | clean add succeeds | 1 — fixture: add-field refused on packed (D9) |
| test-not-null-updates | criteria bulk refused | 1 — fixture: bulk-update recompose paths refused on packed (D9) |
| test-not-null-edit-modifiers | resize accepted | 1 — fixture: edit-field refused on packed (D9) |
| test-computed-update-legacy-nonfinite | legacy source guard names non-finite input | 1 — fixture: legacy computed-update semantics |
| test-computed-update | numeric applied exactly | 1 — fixture: update recompose paths on packed (D9) |
| test-cas-tree-if | bulk ok | 1 — fixture: bulk CAS update recompose refused on packed (D9) |
| test-not-null-schema | constrained field added | 1 — fixture: add-field refused on packed (D9) |
| test-null-legacy-rebuild-trims | legacy s renders | 1 — fixture: legacy rebuild trim semantics |
| test-enum | append yellow: ok | 1 — fixture: enum append = edit-field refused on packed (D9) |
| test-bitmap-kfcache-lock-order-rebuild | post-rebuild bitmap count preserves five alpha rows | 1 — fixture: rebuild with schema change refused on packed (D9) |
| test-slotcask-v2-schema | add-field rebuilt v2 | 1 — fixture: add-field rebuilt v2 refused on packed (D9) |
| test-slotcask-v2-bulk | bulk-update criteria: 2 updated (o2, o4) | 1 — fixture: bulk-update CAS recompose refused on packed (D9) |
| test-slotcask-v2-object | schema line has 5 colons (extended form) | 2 — difference: schema line now has 6 colons (layout token) vs asserted 5 |
| test-get-fields | find projection: numeric is a JSON number | 1 — fixture: fixture create now packed; rendering/field expectations differ |
| test-rebuild-txn-edit-field-after-metadata | edit rebuild reaches metadata pause | 1 — fixture: edit-field refused on packed (D9) |
| test-vacuum-streams-mismatch | all 50 records readable under mismatched schema | 1 — fixture: provisional fixture: case mixes compact/streams on packed; verify which op |
| test-vacuum-addfield | compact vacuum status | 1 — fixture: compact/add-field refused on packed (D9) |
| test-bulk-cas | matched=9 (all pending) | 1 — fixture: bulk-update CAS recompose refused on packed (D9) |
| test-token-perms | tenant-rwx add-field on own dir | 1 — fixture: add-field refused on packed (D9) |
| test-schema-export | mig_users name varchar size=42 (on-disk) | 2 — difference: on-disk sizes/layout identity in export differ |
| test-request-timeout | count tight timeout trips | 2 — difference: tight timeouts no longer trip: packed count/aggregate paths are faster |
| test-agg-varchar-groupby-sum | case1: sum=10000 present | 1 — fixture: legacy-grammar/legacy-behaviour fixtures |
| test-field-vs-field | k3 name updated to OVERSPEND | 1 — fixture: bulk-update recompose refused on packed (D9) |
| test-all-field-types | numeric eq | 1 — fixture: legacy numeric/grammar renderings |
| test-index-oversized-varchar | oversize failure not skipped | 1 — fixture: fixture/limit expectations on packed varchar |
| test-length-ops | k3 bio after bulk-update with len | 1 — fixture: bulk-update len recompose refused on packed (D9) |
| test-alloc-error-propagation | wrapper alloc failure is not a successful insert | 2 — difference: allocation-count sensitive OOM injection differs on packed |
| test-bulk-update-json-oom | clean OOM error response | 1 — fixture: bulk-update recompose refused on packed (D9) |
| test-bulk-update-json | matching per-key CAS updates | 1 — fixture: bulk-update recompose refused on packed (D9) |
| test-regex | bulk-update via regex hit k3 | 1 — fixture: bulk-update recompose refused on packed (D9) |
| test-varchar-overflow | over-length insert rejected | 1 — fixture: fixture limit expectations on packed varchar |
| test-agg-exact-limits | fault loop observed both an error and the exact result | 2 — difference: fault-injection allocation counts differ on packed paths |
| test-auto-key | bulk-insert auto-seq status | 3 — defect: DEFECT-suspect: bulk-insert with auto-key on packed errors "invalid packed bulk input" |
| test-add-field-computed-defaults | add-field int :default=7 accepted | 1 — fixture: add-field + computed defaults refused on packed (D9) |
| test-edit-field-polish | edit-field age:long status | 1 — fixture: edit-field refused on packed (D9) |
| test-edit-field | varchar grow status | 1 — fixture: edit-field refused on packed (D9) |
| test-or-logic | bulk-update matched=3 | 1 — fixture: bulk-update recompose refused on packed (D9) |

## Summary counts
- Classification 1 (fixture; pre-authorised edit = add `"declaration_layout":"legacy"`): 43
- Classification 2 (difference; human decides test vs product): 7
- Classification 3 (defect / defect-suspect; fixed in follow-up task after review): 4

## Notes for the human
1. The three my-plan test cases (loader-*, layout-token-*) fail because their helpers build objects via the now-packed default and then append a second token or legacy declarations — the duplicate-token refusal they now hit is BY DESIGN; the helper seam (D10 `declaration_layout:"legacy"` in lm_build/lm_write_fields usage) is the pre-authorised fixture edit.
2. test-layout-token-with-auto-key-malformed-refuses exposes a real gap in hunk 1f: a schema.conf line already carrying auto_key + layout (8 parts, now written by packed create) silently drops any 9th token. Recommendation: refuse when a line has MORE tokens than the cap (detect via one extra strtok), not just cap-parse 8.
3. auto-key / topn-stream-bitmap-postfilter-bounded / secure-random-keys bulk failures need root-cause before R5b; they may share one cause (packed bulk-insert path with auto-key / large volumes).
4. runner-parallel is a suite-meta-test; it re-greens once the triage edits land.

## Resolution (post-review, 2026-11-03)

Human approved fixing the triage. Outcomes:

**Code defects fixed (2):**
1. `startup_schema_line` (embedded.c) silently dropped a 9th token after the 8-token cap. The loop consumed the extra token, so the check now keeps it and refuses (RED→GREEN: `test-layout-token-with-auto-key-malformed-refuses`).
2. Packed bulk-insert upserted over existing records for server-generated auto-keys (silent overwrite of a manually inserted seq key). Legacy used per-record `if_not_exists` for generated keys; the packed path now sets `SlotcaskBulkRec.if_not_exists` for them (typed_bulk_insert.c). Caught by `test-auto-key` ("manual record at 3 NOT overwritten").

**Reclassified 3 → 2 (packed behaves differently, not broken):** `test-auto-key`, `test-secure-random-keys`, `test-topn-stream-bitmap-postfilter-bounded`. Packed bulk-insert returns `{"inserted":N,...}` (documented in bulk.md), a rejected bulk batch reports a generic `invalid packed bulk input` (no per-record name), a rejected bad-key batch consumes one sequence value, and packed bulk charges the whole request body against `QUERY_BUFFER_MB` (fixture now inserts in 1000-row chunks).

**Fixture/test edits:** `declaration_layout:"legacy"` added to legacy-grammar fixtures (TEST_BUILD only); `test-token-perms` runs the production binary so it stays packed and its add-field assertion now pins authorization only; schema-line colon count 5→6; `test-r1-routes` packed create now succeeds (stream-count guard test uses the packed marker); fault-injection sweeps (`test-agg-exact-limits`) widened for the packed path's extra fault points; `test-alloc-error-propagation` pins the legacy bulk hooks.

**Test infrastructure:** `wait_daemon_ready` now stops waiting when the daemon has already exited (a startup refusal used to cost 30 s, so six refusals tripped the 180 s watchdog). TEST_BUILD legacy fixtures may choose their own `streams` (the host stream-count guard still applies to packed imports).
