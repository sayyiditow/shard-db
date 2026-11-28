# Phase 3 slice 1: single-record writes + CAS (insert / update / delete)

Status: WALKTHROUGH COMPLETE (chunks 1-6). AUD-P3-1/2/3/7/9/10 FIXED on branch
`fix/single-write-audit-slice1` (plan `2026-11-28-audit-slice1-single-write-fixes.md`, uncommitted
until reviewed). AUD-P3-4 deferred to slices 8/10; AUD-P3-8 needs a fault-injection repro and
its own plan.
Plan for fixes is written at the end of the slice, after approval.

## Walked so far

- Chunk 1: `dispatch_json_query` gate, guard parsing/evaluation, locking, the
  insert/update/delete handlers (`server.c`).
- Chunk 2: `cmd_insert` / `cmd_insert_v2_impl` up to the commit and reply
  (`storage.c`).
- Chunk 3: `v2_insert_owned` -> `typed_insert_alloc` (`typed_row.c`): JSON prepass,
  per-field set, defaults, auto_create, not-null. Probe:
  `audit-artifacts/AUD-P3-chunk3-probe.sh` (+ `.out`). - Chunk 4: commit path of `cmd_insert_v2_impl` and the two-phase hooks
  (`v2_insert_check_fn`, `_pre_commit`, `_prepare_commit`, `_apply_commit`,
  `_abort_commit` in `storage.c`; slotcask `bulk_commit_one_kf_window`
  replay/`EINPROGRESS` routing and `slotcask_upsert_with_hooks`). Probe:
  `audit-artifacts/AUD-P3-chunk4-probe.sh` (+ `.out`). - Chunk 5: `cmd_update_v2_impl`, `v2_update_owned` -> `typed_update_computed_alloc`,
  `v2_update_check_fn`, `v2_update_apply_commit`, dry_run branch. Probe:
  `audit-artifacts/AUD-P3-chunk5-probe.sh` (+ `.out`). 
- Chunk 6: `cmd_delete_v2`, `v2_delete_check_fn`, `v2_delete_apply_commit`
  (`storage.c`). Probe: `audit-artifacts/AUD-P3-chunk6-probe.sh` (+ `.out`).
  Slice 1 walkthrough is complete.

## Findings (live-confirmed unless stated)

| ID | Sev | Finding |
|---|---|---|
| AUD-P3-1 (FIXED) | minor | A guarded write (`if_match`/`if_no_match`) with an invalid auto-key prints the `auto_key_normalize` error and then `invalid guard criteria`: two JSON replies for one request (server.c guard branch, grc = -2). |
| AUD-P3-2 (FIXED) | major (availability, not DL) | Empty key (`"key":""`) is accepted by insert on a plain object, but `get`/`exists` reject it (`invalid read request`) and `keys`/`fetch` fail for the whole object (`corrupt record or storage read failed`) while the record exists. Any write token can trigger it; `delete` with `key:""` clears it. Fix direction: reject empty keys at write time (and decide on read-side behaviour). |
| AUD-P3-3 (FIXED) | minor | `klen > max_key` in `cmd_insert_v2_impl` prints to stderr and returns with no reply (client gets an empty response). `update` answers `Not found`, `delete` answers `not_found` for the same key. Fix direction: a proper error object. |
| AUD-P3-4 | minor (read path; confirm in slice 8/10) | `find` escapes keys containing `"`, `\` or a newline twice (`a"b` returned as `a\\\"b`); insert and get replies are correct. |
| AUD-P3-7 (FIXED) | minor (confirm docs) | A field name not in the schema is silently ignored on insert (`{"nmae":"x","req":"r"}` -> `inserted`, `name` stays NULL): a typo drops data with no error (`prepare_json`, name loop falls through). Duplicate keys in one JSON object: last wins (no error). Fix direction: decide strict (reject unknown) vs documented lenient. |
| AUD-P3-8 | major (code-read; needs fault injection to confirm live) | In `cmd_insert_v2_impl` (and the update twin) `durability_pending` is checked only after `if (rc != 0)`. But a post-marker failure that replay cannot finish makes slotcask set `out_durability_degraded=1` AND return -1 (`slotcask_upsert_with_hooks`: `rc != 0 || shard.rc != 0` -> -1), so the client gets `upsert failed` / the index error text, never `durability outcome pending; do not retry blindly`, although the marker is retained and startup will forward-replay the write. A client that retries on a plain error double-applies a `$incr` update. `delete` has the same ordering. No test pins the cmd-level shape (slotcask tests assert `EINPROGRESS` only). Live probe with an invalid marker file shows the gate-failure case (nothing written) correctly returns a plain error, so the fix must separate "gate refused, nothing staged" from "post-marker, outcome pending" (e.g. only post-marker paths set the flag), not just reorder the checks. |
| AUD-P3-9 (FIXED) | minor (docs-vs-behavior) | Single `update`/`delete` accept `dry_run:true` (server.c passes `dry`), but `cas.md` says only bulk-update/bulk-delete support it and "single-record CAS writes always execute". Worse, the single-update dry run only checks existence and `if`: it returns `would_update` for an invalid int, a NULL into a not-null field and a non-JSON `value` (live), all of which the real update rejects. Fix direction: either remove dry_run from the single paths, or run the full constructor against OLD without committing. |
| AUD-P3-7 (update side) | minor | Unknown field in a partial update (`{"nn":9}`) returns `updated` and still stamps `auto_update`, changing nothing else. |

Chunk 5 confirmed OK (live): missing key `Not found`; bad int and NULL into not-null rejected with typed errors; `if` mismatch returns `condition_not_met` with `current`; `$incr` (int, numeric), `$copy` work; int overflow, NULL `$incr` target, NULL `$copy` source, `$incr` on varchar, self-copy and `$incr`+`$copy` on one field all reject with errors and leave the record unchanged; `return_record` renders the committed record; index follows the update.
| AUD-P3-10 (FIXED) | minor | `cmd_delete_v2`: (a) the error branch prints `ctx.err_buf` with a raw `%s` (insert/update escape it; today only schema-derived names and `strerror` reach it, so no live injection, but it is one unescaped producer away); (b) if the `return_record` copy (`malloc` in `v2_delete_check_fn`) fails, the delete still commits and replies plain `deleted` without the requested record; (c) missing key answers `{"status":"not_found"}` (success exit) where update answers the error `Not found`; the docs should state this asymmetry. |

Chunk 6 confirmed OK (live): `if` mismatch returns `condition_not_met` + `current`; bad `if` field rejected; `dry_run` leaves the record and count unchanged; `return_record` returns the removed record; second delete is `not_found`; btree, bitmap and trigram index entries are removed (find returns nothing for the deleted key) and the count falls; reinsert after delete works; `orphaned` tracks tombstones.

Chunk 3 confirmed OK (live): not-null enforced for missing and explicit null; oversize varchar, bad int, nested object and non-object values give typed errors; explicit `auto_create` input is overridden by the stamp; an upsert over an existing key keeps the old `created` and resets omitted fields to their defaults/NULL.

Open question for slice 1: `dry_run` is parsed for single update/delete, but
cas.md says single writes always execute; check in `cmd_update`/`cmd_delete`.

Probe artifact: throwaway daemon, object `t`, `max_key` 8 (inputs in the
chunk-2 conversation; will be saved to `audit-artifacts/` with the repros).
