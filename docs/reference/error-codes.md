# Error codes

shard-db errors are JSON objects with a machine-readable `error` field. Most include contextual fields (`current`, `filename`, etc.) to help clients recover.

## Error response shape

```json
{"error":"<code>", "...context fields..."}
```

Clients should match on the `error` string, not on the full message.

## Common errors

### Infrastructure

| `error` | Cause | Recovery |
|---|---|---|
| `Cannot connect to port N` | Server not running or wrong port. | Check `./shard-db status`, verify `PORT` in `db.env`. |
| `Server shutting down` | Sent during graceful stop while draining. | Retry against the new instance or wait for restart. |
| `Request too large (max N bytes)` | JSON request exceeded `MAX_REQUEST_SIZE`. | Split into smaller requests, or raise `MAX_REQUEST_SIZE`. |
| `query_timeout` | Query exceeded `TIMEOUT` (db.env) or per-request `timeout_ms`. | Add filters, add an index, raise the timeout. |
| `query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor` | Query intermediate buffers crossed `QUERY_BUFFER_MB`. | Narrow `criteria`, add `limit`, or stream via `fetch` + cursor. |
| `durability outcome pending; do not retry blindly` | The server durably published an indexed mutation marker but could not synchronously converge it. | Do not retry the mutation. The retained marker blocks later writes on that shard until synchronous replay or startup recovery completes. |
| `object writes disabled: bitmap repair failed at startup; run reindex` | The object's bitmap repair failed at startup and the escape flag (`indexes/.bmfix-disabled`) is set: reads fall back to btree or scan, but every record mutation and every schema/maintenance mode (insert/update/delete/bulk-*, add-field, edit-field, rename-field, remove-field, add-index, remove-index, vacuum, truncate) is refused up front. | Run a full (non-composites-only) `reindex` — it repairs, clears the flag and re-enables writes. |

### Layout and startup compatibility

These refuse start (the daemon exits; an embedded `shard_db_open` returns NULL)
or the request that triggers them. 2026.11.3 removed the legacy layout and both
startup migrations, so a root must already be packed and carry 2026.11.2
clean-open evidence.

| `error` | Cause | Recovery |
|---|---|---|
| `object <dir>/<obj> is not in the packed layout; install shard-db 2026.11.2, start it once to completion, then upgrade` | The `schema.conf` line lacks `layout=packed` and the object exists (rule 1, no `.layout-migrated` stamp). | Install 2026.11.2, start it once to completion (it converts the object), stop, then install this version. |
| `object <dir>/<obj> is not in the packed layout. To convert it: remove stale schema.conf lines (lines with no object directory) and <db_root>/.layout-migrated, start shard-db 2026.11.2 once to completion, then upgrade` | Same, on a root carrying a stale `.layout-migrated` stamp (an interrupted conversion). | Delete the stale lines and the stamp, then run the 2026.11.2 conversion start. |
| `stale schema.conf line for <dir>/<obj> lacks layout=packed; stop shard-db, delete the line from schema.conf, then start it` | A token-less line whose object directory does not exist. | Delete the line (D10: the gate refuses every token-less line). |
| `invalid rebuild metadata under <dir>/<obj>; preserving it` | An active rebuild transaction whose `meta` is unusable or whose `old_layout` is absent/0 (a pre-2026.11.1 artifact). | Preserve the directory for inspection; restore the object from backup, or run 2026.11.2's recovery against the root. |
| `schema.conf is unreadable or malformed; fix or restore it` | `schema.conf` cannot be read or contains a malformed line/token. | Fix or restore the file. |
| `database was last opened by shard-db <v>; install shard-db 2026.11.2, start it once to completion against this DB_ROOT, then install this version.` | Version floor: the root's evidence is older than 2026.11.2. | Run 2026.11.2 once to completion, then upgrade. |
| `backup is not a packed v2 object; restore with shard-db 2026.11.2 first` | `restore` of a backup whose `object.json` lacks `"layout":"packed"` or whose `storage_version` is not 2. | Take the backup with 2026.11.2+ (it writes a packed v2 manifest). |
| `legacy layout objects can no longer be created` | `create-object` with `declaration_layout:"legacy"` (every build). | Omit it; objects are always packed. |
| `unknown declaration_layout; expected "packed"` | Any other `declaration_layout` value. | Use `"packed"` or omit the field. |

### Validation

| `error` | Cause | Recovery |
|---|---|---|
| `Unknown dir: <name>` | `dir` not in `dirs.conf`. | Register the dir or fix the typo. |
| `Object [<name>] not found. Use create-object first.` | Querying an object that doesn't exist. | Create it via `create-object`. |
| `Missing criteria` | `find` without a `criteria` field. | Pass `"criteria":[]` for all-records. |
| `fields is required` | `create-object` without `fields`. | Provide at least one field spec. |
| `fields array is empty` | Empty `fields` array in `create-object` or batch ops. | Provide at least one element. |
| `invalid field type: "<spec>"` | Unknown type or malformed size. | Check spelling; see [Typed records](../concepts/typed-records.md). |
| `invalid field name (no :, +, /, spaces)` | Reserved character in field name. | Use `[a-zA-Z0-9_-]`. |
| `default for field '<f>' cannot be stored: <reason>` | Packed `add-field`: a literal / `seq` / `uuid()` / `random(N)` default is not representable in the field's declared type. | Fix the default or the field type. |
| `random(N) requires 1 <= N <= 256` | `random(N)` outside 1..256 on a packed `add-field`. | Use `1 ≤ N ≤ 256`. |
| `Sequence values do not fit field: <reason>` | The reserved `seq` range's last value overflows the field. The range stays consumed. | Use a wider type. |
| `Pre-flight failed on field [<f>]: key '<k>': <reason>` | Packed `edit-field`: an existing row does not fit the new declaration (nothing changed). | Widen the declaration or fix the row. |
| `compact refused: every field is tombstoned; …` | `vacuum compact` would leave no field. | Keep or re-add a field first. |
| `Rebuild walk failed: field '<f>' key '<k>': <reason>` | Packed rebuild aborted mid-walk; original data restored. | Read the reason. |
| `Invalid field name: <name>` | Same, from `remove-field`. | Fix the name. |
| `max_key N exceeds ceiling 1024` | `create-object` with a too-large `max_key`. | Use ≤ 1024 (UUIDs fit in 36B). |
| `too many fields (max 256)` | Schema exceeded `MAX_FIELDS`. | Consolidate or drop unused fields. |

### NOT NULL constraint (2026.10.2)

| `error` | Cause | Recovery |
|---|---|---|
| `field '<name>' is not nullable` | A write would store NULL on a `:not_null` field: explicit JSON `null` or an omitted insert field with no default. Whole-request error on single insert/update and criteria-form bulk update; per-record JSON bulk paths skip and count the record instead (the field name goes to the log). | Supply a real value; a `:default=` covers omission only, never an explicit null. |
| `value for field '<name>' is not a valid <type>: <detail>` | R4 strict scalar grammar: the input text is not in the type's accepted forms (bool spellings, calendar date, datetime/datetimems shape, `HH:MM:SS`, UUID, IPv4/IPv6, enum member). Fires on every write route and on query comparison literals (criteria carry the generic `invalid criteria value` wire error instead). Malformed `uuid`/`ipv4`/`ipv6` no longer downgrade to NULL/zero, and an empty string is an error for these types (not member 0 / midnight / nil). | Send one of the accepted forms (see [typed records](../concepts/typed-records.md)); send explicit `null` for NULL. |
| `cannot add :not_null to field '<name>': N record(s) have NULL` | `edit-field` adding `:not_null` found N live records with that field NULL (legacy-generation records count when their next rewrite would make them NULL). | Backfill the NULLs (e.g. `bulk-update` the field), then re-run the edit. |
| `cannot add :not_null to field '<name>': record scan failed (unreadable record?) — run vacuum-check/reindex first` | The gate's strict live-record scan failed (unreadable record, invalid live reference, or unopenable object) — fail-closed. | Run `vacuum-check` / `reindex`, repair the object, retry. |

### File storage

| `error` | Cause | Recovery |
|---|---|---|
| `invalid filename` | Filename contained `/`, `\`, `..`, control chars, or was empty / too long. | Use a plain basename ≤ 255 bytes. |
| `missing data` | `put-file` without `data` or `path`. | Provide one. |
| `filename is required when uploading via data` | base64 put-file with no `filename`. | Add it. |
| `filename is required` | `get-file` without filename. | Add it. |
| `file not found` | `get-file` on non-existent file. | Check filename (case-sensitive). |
| `file exists` | `put-file` with `if_not_exists:true` hit an existing file. | Pick a new name or omit `if_not_exists`. |
| `invalid base64` | Malformed `data` payload. | Validate encoder; whitespace is tolerated but invalid chars aren't. |
| `cannot create <path>` | Server-side I/O error writing tmp file. | Check disk space, permissions. |
| `write failed` | Short write to file descriptor. | Usually disk-full. |
| `rename failed` | Atomic rename step failed. | Check filesystem consistency. |
| `read failed` | Short read from stored file. | Indicates corruption — restore from backup. |

### Conditional writes / CAS

| `error` | Cause | Recovery |
|---|---|---|
| `condition_not_met` | `if_not_exists` or `if:[...]` check failed. Response includes `"current"` with the actual record. Guards (below) add `"guard"` and `"matched"` members. | Inspect `current`, reconcile, retry. |
| `key already exists` | `insert` without `if_not_exists` when the key was just written by a concurrent request (rare race). | Use `if_not_exists:true` for idempotent inserts. |
| `invalid if condition` | `if` criteria failed parsing, field validation (unknown field), depth cap, or regex compilation. | Fix the criteria (see [cas.md](../query-protocol/cas.md#if-grammar-202610--full-filter-trees)). |
| `invalid guard criteria` | `if_match`/`if_no_match` guard is empty, unknown-field, malformed, or both guards were supplied non-null. | Fix or drop the guard (see [cas.md](../query-protocol/cas.md#object-scope-guards-if_match--if_no_match)). |
| `guard check failed` | The guard search could not prove a verdict (index unreadable, corrupt record, scan failure). Always fail-closed — nothing was written. | Retry; investigate the object if persistent. |
| `guards are not supported on bulk operations` | A `bulk-*` mode carried `if_match`/`if_no_match`. | Move the guard to the single-write path. |
| `computed updates are only valid on update` | A `{"$incr":…}`/`{"$copy":…}` span appeared on `insert` or a criteria-form `bulk-update` value. | Compute after an initial insert, or use single `update`. |
| `field '<name>' is NULL; cannot apply computed update` | `$incr` target, `$copy` target, or `$copy` source is NULL on the current record — copying never silently clears NULL. | Set the field first, or use `if` to gate on non-NULL. |
| `field '<name>' is not finite; cannot apply computed update` | `$copy` source float/double holds NaN/Infinity — non-finite bytes never move between fields. | Repair the source value first. |
| `field '<name>': computed update overflows <type>` | `$incr` literal or result exceeds the field's range/scale. | Smaller literal. |
| `field '<name>': unsupported type for computed update` | `$incr`/`$copy` on a non-numeric field (only short/int/long/numeric/currency/float/double). | — |
| `field '<name>': $copy type mismatch with '<name>'` | `$copy` source type (or numeric scale) differs from the target. | — |
| `field '<name>': malformed computed value object` | Computed span is not a one-member `{"$incr":…}`/`{"$copy":…}` object. | Fix the span grammar. |
| `field '<name>': bad computed literal` | `$incr` literal unparseable, empty, oversized, or has excess fractional digits. | Fix the literal. |

### Index management

| `error` | Cause | Recovery |
|---|---|---|
| `field is required` | `remove-index` with neither `field` nor `fields`. | Add one. |
| `fields must be a JSON array` | `fields` wasn't an array. | Pass `["a","b"]`. |

The `remove-index` response `{"status":"not_indexed","field":"x"}` is **not an error** — it's the idempotent-no-op response when the field wasn't indexed.

### Bulk operations

| `error` | Cause | Recovery |
|---|---|---|
| `some_records_dropped` | `bulk-insert` rejected some records (type mismatch, missing required). Response includes `errors` count. | Check logs for detail; re-submit rejected rows. |
| `Missing criteria or value` | `bulk-update` without both. | Provide both. |

### Restore (2026.11.3)

Restore is stage-then-swap with a durable commit point; see
[backup.md](../operations/backup.md#interrupted-restores) for the on-disk
staging layout and manual recovery.

| `error` | Cause | Recovery |
|---|---|---|
| `from is required` | `restore` without `from`. | Pass the backup directory name. |
| `invalid from (no slashes or '..' allowed)` | `from` contained `/` or `..`. | Pass a plain timestamp directory name. |
| `object not found` | The object directory does not exist. | Create the object first. |
| `backup not found: <from>` | No such directory under `<obj>/backup/`. | List `<obj>/backup/`. |
| `objlock_wrlock failed for restore` | The object lock could not be taken. | Retry. |
| `restore refused: an earlier interrupted restore of this object could not be rolled back; see the error log` | A leftover staging directory cannot be rolled back (e.g. a non-directory `rb/`). Nothing was changed by this call. | Recover manually (backup.md → Interrupted restores), then retry. |
| `<sub>/ is not empty (use force=true to overwrite)` | The live subtree is non-empty and `force` was not set. | Pass `force:true`. |
| `files/ is not empty (use force=true to overwrite)` | The backup governs `files/` and live `files/` is non-empty. | Pass `force:true`. |
| `backup has no layout metadata (object.json); restore refused` | The backup predates layout capture. | Restore it with the release that wrote it. |
| `backup layout (<a>) does not match the live object's layout (<b>); restore refused — …` | A pre-migration backup into a migrated object. | Restore into a scratch object, then migrate. |
| `schema mismatch: live splits=… max_key=…, backup splits=… max_key=… (use force=true to overwrite)` | Layout differs and `force` was not set. | Pass `force:true` (the live sizing is kept). |
| `live fields.conf is not a regular file; restore refused` | `fields.conf` exists but is not a regular file. | Repair the object directory. |
| `fields.conf differs between live and backup (use force=true to overwrite)` | Field sets differ and `force` was not set. | Pass `force:true`. |
| `cannot read schema.conf: <errno>; restore refused` | `schema.conf` is unreadable. | Check permissions/disk. |
| `backup manifest (object.json) lacks a valid storage_version/streams; cannot recreate the schema.conf line; restore refused` | The line is missing and the manifest cannot recreate it. | Repair `schema.conf` by hand or restore a newer backup. |
| `restore failed at <step>: <errno>; the live object was not changed` | A pre-commit step failed (staging directory/copy/sync, directory preparation, swap marker, fields.conf). | Fix the cause (disk space, permissions) and retry. |
| `restore failed at <step>: <errno>; rolled back, the live object was not changed` | A post-SWAP step failed (swap of `<name>`, directory sync, swap-marker removal/sync); the rollback completed. | Fix the cause and retry. |
| `restore committed but <file> could not be installed: <errno>; the backup data is live; re-run restore with force=true` | The data is committed but a live-absent `fields.conf` or the `schema.conf` line could not be written. | Fix the cause and re-run the same restore (idempotent). |

A daemon `abort()` during an unrecoverable rollback is logged as
`RESTORE …: <step> failed (…) and the rollback could not be completed (…);
aborting, startup recovery finishes the rollback`, and startup refuses the
object if it cannot finish it (`RESTORE-RECOVER …: startup refused`).

### Auth

| `error` | Cause | Recovery |
|---|---|---|
| `auth failed` | No matching token, IP not allowlisted, or token's scope/perm doesn't cover the request. | Check token scope (`global`/tenant/object), permission (`r`/`rw`/`rwx`), and the request's `dir`/`object`. |
| `object scope requires dir` | `add-token` with `object` but no `dir`. | Provide both. |
| `invalid perm: must be r, rw, or rwx` | `add-token` with an unrecognized perm. | Use `r`, `rw`, or `rwx`. |
| `object not found: <dir>/<obj>` | `add-token` referencing a non-existent object. | Create the object first. |
| `invalid dir name (no /,\\,..,control chars; max 64 bytes)` | `add-dir` with an unsafe name. | Use `[a-zA-Z0-9_-]`, ≤ 64 bytes. |
| `dir is not empty — drop objects first or pass check_empty:false` | `remove-dir` while objects still live under the tenant. | Truncate/remove objects, or pass `check_empty:false`. |

## Non-error status values

Some responses look error-ish but are normal:

- `{"status":"not_found","key":"..."}` — `delete` on a missing key. Not an error; your delete is a no-op.
- `{"status":"not_indexed","field":"..."}` — `remove-index` on a non-existent index. Idempotent no-op.
- `{"status":"exists","field":"..."}` — `add-index` without `force:true` on an already-indexed field.
- `{"count":0}` — a count/find with no matches. Normal.

## Debugging

1. **Check the error string verbatim** — they're stable across versions.
2. **Check `error-YYYY-MM-DD.log`** in `$LOG_DIR` for the server-side context.
3. **For mysterious hangs** — `./shard-db stats` shows in-flight writes and active threads.
4. **For slow queries** — `slow-YYYY-MM-DD.log` + the in-memory ring via `stats`.
5. **For suspected corruption** — stop the server, run `vacuum-check`, back up before any repair attempts.
