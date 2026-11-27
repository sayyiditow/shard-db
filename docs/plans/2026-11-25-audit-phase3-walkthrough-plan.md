# Phase 3 plan: per-mode audit as a guided walkthrough (2026-11-25)

Format requested by the human: every query mode is reviewed together, code
line by line, with a plain-language explanation of what the code is trying to
do, so that no database feature is missed. Order is data-loss first.

## How each mode runs (same steps every time)

1. **Feature list first.** Before reading code, I write the mode's complete
   feature checklist (every parameter, flag, format, CAS option, edge
   behavior documented in `docs/query-protocol/` and `CLAUDE.md`). You can add
   features I missed. Nothing is "covered" until its box is ticked.
2. **Walkthrough in chunks.** I take the mode's code path in order
   (dispatch -> validation -> auth -> locking -> planning -> execution ->
   response), one function at a time. For each chunk: the code reference,
   what it is trying to do in plain words, what must be true afterwards, and
   anything suspicious. You stop me whenever something is unclear or wrong.
3. **Checklist pass.** The 14-point slice checklist from the campaign
   (data-loss pass first, bounds, overflow, auth, response shape, lifetimes,
   crash safety, locks, caps, hot-loop allocation, error side effects, typed
   edges, platform, docs-vs-behavior).
4. **Live probes.** Where a claim can be tested, a short wire probe or a test
   case, red/green style, recorded in `docs/plans/audit-artifacts/`.
5. **Findings doc** `docs/plans/<date>-audit-<mode>.md`: feature checklist
   (ticked), walkthrough notes, findings with severity, "not covered".

## Order and feature inventory

Slices run in this order (write/destroy first). Each lists its modes; the
feature checklist is built from the docs at the start of the slice.

| # | Slice | Modes |
|---|---|---|
| 1 | single-record writes + CAS | insert, update, delete; `if`, `if_not_exists`, `if_match`, `if_no_match`, `$incr`, `$copy`, `return_record`, auto-key (uuid, seq), defaults, typed fields, NULL |
| 2 | bulk insert | bulk-insert, bulk-insert-delimited; stages, growth gates, KFM2 markers, `if_not_exists`, upsert behavior, indexed batch ingest |
| 3 | bulk delete / update | bulk-delete (keys, criteria, file), bulk-update (criteria+value, records, file), bulk-update-delimited, dry_run, limit, `if` |
| 4 | maintenance | vacuum (light, compact, splits), backup, restore, reindex, recount, truncate, size, orphaned, vacuum-check, auto-vacuum, auto-reshard |
| 5 | schema | create-object, drop-object, add/edit/remove/rename-field, add/remove-index, auto-key, composite and bitmap indexes, enums, numeric |
| 6 | files | put-file, get-file, get-file-path, delete-file, list-files (match modes) |
| 7 | remaining write/destroy modes | add-dir, remove-dir, sequence, restore from the wire, tokens/IP management |
| 8 | find + count | criteria (38 operators), order_by, formats (rows/csv/dict), offset/limit, fields, explain, NQL find/count |
| 9 | cursor pagination | keyset cursors, composite order_by, resume points |
| 10 | fetch / keys / exists / get | single and multi-key, CSV |
| 11 | aggregate | count/sum/avg/min/max, group_by, having, order_by, top-N, NQL aggregate |
| 12 | join | inner/left, by primary key or indexed field |
| 13 | diagnostics | stats, stats-prom, shard-stats, db-dirs, list-objects, describe-object, list-tokens/ips, estimate-index |

The Phase 2 fixes above land first or alongside; slices re-check them where
they touch a mode.

## Working agreement

- I do not skip a function; if a chunk is large I split it, not summarize it.
- You can reorder slices, ask for any code to be re-explained, or add
  features to a checklist at any point.
- Fixes found in a slice follow the same rule: DL and security first, plan
  and approval before source edits, red repro before fix.
