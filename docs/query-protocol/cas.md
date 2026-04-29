# Compare-and-swap (CAS)

Optimistic concurrency control without app-side locks. `if_not_exists` and `if:[...]` conditions check the current state atomically with the write.

## `if_not_exists` on `insert`

Idempotent insert — fails if the key already exists.

```json
{
  "mode": "insert",
  "dir": "default",
  "object": "users",
  "key": "u1",
  "value": {"name":"Alice","email":"a@x.com"},
  "if_not_exists": true
}
```

- On success: `{"status":"inserted","key":"u1"}`.
- On conflict: `{"error":"condition_not_met","current":{...}}`.

Use for **idempotent create** — safe to retry on network failure.

## `if` on `update`

Update only when the current record matches a criteria list.

```json
{
  "mode": "update",
  "dir": "acme",
  "object": "orders",
  "key": "o42",
  "value": {"status":"shipped","shipped_at":"20260418153012"},
  "if": [
    {"field":"status","op":"eq","value":"paid"}
  ]
}
```

- On success: `{"status":"updated","key":"o42"}`.
- On condition failure: `{"error":"condition_not_met","current":{...}}`.

`if` uses the same [operators](find.md#operators) as `find` / `count`. Multiple criteria are AND-combined.

## `if` on `delete`

Delete only when the current record matches.

```json
{
  "mode": "delete",
  "dir": "default",
  "object": "sessions",
  "key": "s1",
  "if": [{"field":"expires_at","op":"lt","value":"20260418000000"}]
}
```

Useful for expiring stale data without TOCTOU races — the check and delete are atomic under the per-object lock.

## Pattern: version-based CAS

Combine `auto_update` with a `version:long:default=seq(version_counter)` field to track revisions without the client keeping the state:

```json
// Object schema
{
  "fields": [
    "status:varchar:20",
    "version:long:default=seq(version_counter)",
    "updated:datetime:auto_update"
  ]
}

// Optimistic update: only if version hasn't changed
{
  "mode": "update",
  "dir": "acme",
  "object": "orders",
  "key": "o42",
  "value": {"status":"shipped"},
  "if": [{"field":"version","op":"eq","value":"17"}]
}
```

On success the server bumps `version` via the sequence, and `updated` via `auto_update`. On conflict, the client reads the current record, reconciles, and retries with the new version.

## Pattern: claim-once

```json
{
  "mode": "update",
  "dir": "default",
  "object": "jobs",
  "key": "job-1234",
  "value": {"owner":"worker-7","claimed_at":"20260418153012"},
  "if": [{"field":"owner","op":"nexists"}]
}
```

Multiple workers racing to claim: exactly one wins.

## CAS on bulk operations

`bulk-insert`, `bulk-update`, and `bulk-delete` all carry CAS semantics in addition to the per-record forms:

- `bulk-insert` with `if_not_exists:true` — existing keys are skipped (response gets `"skipped":N`), not overwritten. Pure-idempotent bulk load.
- `bulk-update` with `criteria` + `if` per record — see [Bulk → bulk-update](bulk.md#bulk-update). Combine with `dry_run:true` to preview.
- `bulk-delete` with `if` per record — see [Bulk → bulk-delete](bulk.md#bulk-delete).

## Dry-run

Only `bulk-update` and `bulk-delete` (criteria form) support `dry_run:true`. Single-record CAS writes always execute — the check is the atomic part, not a preview.

## `condition_not_met` response

Whenever a CAS check fails, the server returns the **current** record state so the client can reconcile without another round trip:

```json
{
  "error": "condition_not_met",
  "current": {"status":"cancelled","version":18,"updated":"20260418151023"}
}
```

Your client code should:
1. Catch `condition_not_met`.
2. Inspect `current`.
3. Decide: retry with new state? surface error? give up?

## What CAS is **not**

- **Not cross-record.** All conditions are against the same key you're writing. If you need multi-record atomicity, stage the intent in a third record.
- **Not a queue.** For durable work queues, use a dedicated mechanism. CAS is good for per-key claim patterns but lacks the ordering guarantees of a queue.
- **Not a lock.** No "hold this record". The check is instantaneous; no wait, no timeout, no deadlock.

## Where CAS shines

- Idempotent HTTP handlers (`if_not_exists` on insert).
- State-machine transitions (`status: pending → paid` only when pending).
- Claim-once / lease-once patterns.
- Optimistic UI — update and let the server arbitrate conflicts.
- Expiry without a reaper — `delete if expires_at < now`.
