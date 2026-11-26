# Phase 3 slice 1: single-record writes + CAS (insert / update / delete)

Status: IN PROGRESS (walkthrough with the human; chunks 1-2 done).
Plan for fixes is written at the end of the slice, after approval.

## Walked so far

- Chunk 1: `dispatch_json_query` gate, guard parsing/evaluation, locking, the
  insert/update/delete handlers (`server.c`).
- Chunk 2: `cmd_insert` / `cmd_insert_v2_impl` up to the commit and reply
  (`storage.c`). Next: chunk 3 (`v2_insert_owned`: defaults, auto_create, NULL,
  typed errors).

## Findings (live-confirmed unless stated)

| ID | Sev | Finding |
|---|---|---|
| AUD-P3-1 | minor | A guarded write (`if_match`/`if_no_match`) with an invalid auto-key prints the `auto_key_normalize` error and then `invalid guard criteria`: two JSON replies for one request (server.c guard branch, grc = -2). |
| AUD-P3-2 | major (availability, not DL) | Empty key (`"key":""`) is accepted by insert on a plain object, but `get`/`exists` reject it (`invalid read request`) and `keys`/`fetch` fail for the whole object (`corrupt record or storage read failed`) while the record exists. Any write token can trigger it; `delete` with `key:""` clears it. Fix direction: reject empty keys at write time (and decide on read-side behaviour). |
| AUD-P3-3 | minor | `klen > max_key` in `cmd_insert_v2_impl` prints to stderr and returns with no reply (client gets an empty response). `update` answers `Not found`, `delete` answers `not_found` for the same key. Fix direction: a proper error object. |
| AUD-P3-4 | minor (read path; confirm in slice 8/10) | `find` escapes keys containing `"`, `\` or a newline twice (`a"b` returned as `a\\\"b`); insert and get replies are correct. |

Open question for slice 1: `dry_run` is parsed for single update/delete, but
cas.md says single writes always execute; check in `cmd_update`/`cmd_delete`.

Probe artifact: throwaway daemon, object `t`, `max_key` 8 (inputs in the
chunk-2 conversation; will be saved to `audit-artifacts/` with the repros).
