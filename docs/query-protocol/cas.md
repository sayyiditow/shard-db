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

### `if` grammar (2026.10 — full filter trees)

`if` accepts either shape the read path accepts:

- **Flat array** (historical): `[{"field":"status","op":"eq","value":"paid"}, …]` — AND root, unchanged semantics.
- **Nested tree**: `{"and":[…]}` / `{"or":[…]}` objects, nesting up to `MAX_CRITERIA_DEPTH` (16) — the same grammar `find`'s `criteria` accepts:

```json
{"mode":"update","dir":"default","object":"orders","key":"o42",
 "value":{"status":"shipped"},
 "if":{"or":[
   {"and":[{"field":"status","op":"eq","value":"paid"},
            {"field":"amount","op":"lt","value":10}]},
   {"field":"flagged","op":"eq","value":true}]}}
```

Semantics notes (behavior changes from the historical string comparison, intentional and regression-tested):

- **Typed comparison.** Criteria are evaluated against the typed record values exactly as `find` does — not against rendered strings. A zero-padded integer literal like `"07"` now matches an `int` field holding 7.
- **Unknown fields are validation errors.** `{"field":"nosuch",...}` in `if` returns `{"error":"invalid if condition"}` instead of silently never matching.
- **Invalid regex patterns reject.** A malformed `regex`/`not_regex` pattern rejects with `invalid if condition` instead of acting as never-match/always-match; a regex *evaluation* error fails the condition closed (never authorizes).
- **`if: []` remains a vacuous pass** (historical behavior).
- The `eq_field` family works per-record in write conditions (RHS names a sibling field of the same record).
- `MAX_CRITERIA_DEPTH` (16) is inherited from the read path.

Conditions always evaluate the **pre-image**: `check_fn` runs before the new record is built, so `if` plus computed updates (below) commit atomically or not at all.

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

On success `updated` is bumped via `auto_update`, but `version` is **not** — `default=seq(...)` only evaluates on **insert**, so an update never implicitly re-runs it. Update CAS revisions must be explicitly supplied or incremented by the caller, as shown above (`{"value":{"version":18}}`), or by a separately implemented update policy (e.g. an `auto_update` counter field). On conflict, the client reads the current record, reconciles, and retries with the new version.

With computed updates (below) the client can have the server bump the version atomically with the state change — `{"value":{"status":"shipped","version":{"$incr":1}}}`. The versioning *policy* itself stays app-side; the engine has no `:version` modifier.

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

## Object-scope guards (`if_match` / `if_no_match`)

Key-scoped `if` checks the record you're writing. **Guards** check *other* records in the same object, atomically with the write:

```json
// Uniqueness: insert only while NO other live record has domain == "d.io"
{"mode":"insert","dir":"default","object":"domains","key":"g1",
 "value":{"domain":"d.io","owner":"a"},
 "if_no_match":[{"field":"domain","op":"eq","value":"d.io"}]}

// Existence: insert only while at least one OTHER record has p == "p1"
{"mode":"insert","dir":"default","object":"children","key":"c-y",
 "value":{"p":"p1"},
 "if_match":[{"field":"p","op":"eq","value":"p1"}]}
```

Semantics:

- Guards accept the same grammar as `if` — flat array or nested `{and}/{or}` tree. The full tree is always evaluated per record; an indexed equality leaf under AND only speeds candidate *finding*.
- The record being written is **excluded** (by exact stored key bytes), so updating a record under a guard it itself matches passes ("self excluded").
- `if_match` passes on the first verified OTHER match; `if_no_match` passes only after a complete strict search proves zero OTHER matches.
- `"if_match":null` / `"if_no_match":null` means the guard is **absent** (both null is fine); supplying **both non-null** guards rejects with `{"error":"invalid guard criteria"}` before any lock is taken.
- Empty (`[]`) or unknown-field guards reject with `invalid guard criteria` — a guard matching nothing is meaningless, unlike key-scoped `if`.
- Failure: `{"error":"condition_not_met","guard":"if_no_match","matched":0|1}` — `matched` is a **witness indicator** (0 = no other record matched, 1 = at least one did), not an exact count.
- Guards are single-write only: any `bulk-*` mode carrying a guard rejects with `{"error":"guards are not supported on bulk operations"}` before any write. Guards on non-write modes (find/count/get/…) are ignored.
- Every guard error is **fail-closed**: timeout, corrupt record, unreadable index, or an unprovable search rejects the write with `guard check failed` / `query_timeout` — nothing falls through to a write on an error.

**Cost model.** A guarded write holds the object's shared lock, reserves every existing per-shard writer gate for the object, replays any retained commit markers, runs the guard search, and performs the write without releasing the reservation. Readers do **not** take writer gates and continue during the guard check (subject to the existing per-shard kf/index/cache locks); all other writers on the object wait. An indexed equality guard descends the index per shard plus per-candidate verification; other predicates (ranges, OR/NOT shapes, non-indexed fields, unsupported encodings such as ±0.0 floats) fall back to a strict serial scan of the object's live records while the gates are reserved — unsuccessful existence checks and successful absence checks may inspect the whole object. An index on a field alone does not guarantee a fast guard. Plan guarded writes accordingly; unguarded writes are unaffected.

## Computed updates (`$incr` / `$copy`)

An `update` value field may hold a computed operation evaluated **from the current record under the same lock hold as the CAS check** — no read-modify-write race:

```json
{"mode":"update","dir":"default","object":"accounts","key":"a1",
 "value":{"balance":{"$incr":-50}},
 "if":[{"field":"balance","op":"gte","value":50}]}
```

- `{"$incr": <literal>}` — add a signed literal to the field's current value.
- `{"$copy": "<sibling>"}` — copy a sibling field's current value.
- The computed object must hold **exactly one** of `$incr`/`$copy`; any other member (or both/ neither) rejects with a per-field error.
- Supported types: `short`, `int`, `long`, `numeric:P,S` / `currency`, `float`, `double`.
- Integer/numeric literals are scaled exactly: integer part plus fractional digits padded to the field scale; digits beyond the scale reject (`"0.005"` on `numeric:12,2` → error). All arithmetic is overflow-checked; an overflowing literal or result rejects the whole update.
- Float/double use IEEE addition; `ERANGE` and non-finite results reject.
- Current value NULL — `$incr` target, `$copy` target — or a NULL `$copy` source rejects: `field '<name>' is NULL; cannot apply computed update`. Copying never silently clears NULL.
- `$copy` requires the same type (numeric also same scale); a `$copy` source may not itself be a computed field, and a non-finite float/double source (NaN/Infinity) rejects — non-finite values never move between fields.
- Every rejection is a **whole-request** error — nothing is written. Computed fields only on single `update`: a computed span on `insert` rejects with `computed updates are only valid on update`; bulk forms reject or skip the offending record per their existing error accounting. In `bulk-update` (records form) the D8 detection fails closed: a data span that cannot be parsed as an object at all (e.g. more members than the parser records) skips the record rather than risk storing a computed op as text.
- Rejection messages may quote client-provided text (a `$copy` source name, for instance). The raw message is formatted first — truncating at the internal buffer boundary if needed — and JSON-escaped once at emission, so every `{"error":...}` response is well-formed JSON no matter what the request contained.
- Conditions see the pre-image: `if balance >= 50` + `{"balance":{"$incr":-50}}` commits atomically or not at all.
- `auto_update` stamping still runs after the computed pass.

## Returning records (`return_record`)

Opt-in on `insert` / `update` / `delete`:

```json
{"mode":"update","dir":"default","object":"accounts","key":"a1",
 "value":{"balance":{"$incr":1}},"return_record":true}
```

- Success responses gain a `"record":{...}` member: the **committed** record (insert/update — defaults, auto_create/auto_update and computed values already applied) or the **removed** record (delete).
- Without the flag, responses are byte-identical to the historical shapes. `dry_run` and `condition_not_met` responses never carry `record`.
- On an internal allocation failure the write still commits; the response just omits `record`.

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

- **Not cross-object.** Key-scoped `if` is per-record, and the object-scope guards above cover *one* object: same-object sibling guards prevent concurrent records from invalidating an existence decision, but **cross-object** atomicity does not exist. If you need multi-object atomicity, stage the intent in a third record.
- **Not a queue.** For durable work queues, use a dedicated mechanism. CAS is good for per-key claim patterns but lacks the ordering guarantees of a queue.
- **Not a lock.** No "hold this record". The check is instantaneous; no wait, no timeout, no deadlock. (Guards do serialize concurrent writers on the object for the duration of the check — see the cost model above — but clients never hold anything.)

## Where CAS shines

- Idempotent HTTP handlers (`if_not_exists` on insert).
- State-machine transitions (`status: pending → paid` only when pending).
- Claim-once / lease-once patterns.
- Optimistic UI — update and let the server arbitrate conflicts.
- Expiry without a reaper — `delete if expires_at < now`.
