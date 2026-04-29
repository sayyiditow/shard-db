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
| `Invalid field name: <name>` | Same, from `remove-field`. | Fix the name. |
| `max_key N exceeds ceiling 1024` | `create-object` with a too-large `max_key`. | Use ≤ 1024 (UUIDs fit in 36B). |
| `too many fields (max 256)` | Schema exceeded `MAX_FIELDS`. | Consolidate or drop unused fields. |

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
| `condition_not_met` | `if_not_exists` or `if:[...]` check failed. Response includes `"current"` with the actual record. | Inspect `current`, reconcile, retry. |
| `key already exists` | `insert` without `if_not_exists` when the key was just written by a concurrent request (rare race). | Use `if_not_exists:true` for idempotent inserts. |

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
