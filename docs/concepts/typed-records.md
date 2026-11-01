# Typed records

Every object has a **typed binary schema**, declared once at `create-object` time and stored in `<object>/fields.conf`. Records on disk are a fixed-width packed layout driven by this schema — not JSON.

On the v2 slotcask engine these typed records are the **value payload** of each segment record (`<obj>/data/streams/<stream>/<file>.dat`), prefixed by a 24-byte segment record header (hash + klen + flag + vlen) and followed by `klen` bytes of raw key.

## Why typed records

- **Zero-malloc matching** — `match_typed()` compares criterion values directly against byte ranges; no JSON parsing per record.
- **Compact** — an `int` is 4 bytes, not 4–11 ASCII digits. Payloads pack densely.
- **Correct numerics** — `numeric:P,S` stores fixed-point decimals without IEEE 754 drift.

## Declaring fields

Fields are declared in the `create-object` request as an array of strings, each `name:type[:param]`. The full line grammar (as stored in `fields.conf`) is:

```
name:type[:size|P,S][:default=...][:auto_create|:auto_update][:not_null|:nullable][:removed]
```

`[:not_null|:nullable]` is a **trailing** token pair — at most one, and only after any default modifier. A token anywhere else (duplicates included, e.g. `x:int:not_null:default=5`) is rejected as an invalid field line, on every type:

```json
{
  "mode": "create-object",
  "dir": "acme",
  "object": "invoices",
  "splits": 16,
  "max_key": 64,
  "fields": [
    "number:varchar:20",
    "customer:varchar:100",
    "total:numeric:12,2",
    "status:varchar:20",
    "created:datetime:auto_create",
    "paid:bool"
  ],
  "indexes": ["customer", "status"]
}
```

Order matters — it determines the on-disk layout. Once set, fields can be [added](../query-protocol/schema-mutations.md), [renamed](../query-protocol/schema-mutations.md), or [removed](../query-protocol/schema-mutations.md), but their position within the payload is fixed for the object's life (removal is a tombstone until you vacuum).

## Types

| Type | Spec | On-disk bytes | Notes |
|---|---|---|---|
| `varchar` | `name:varchar:N` (N = 1..65535) | `N + 2` | `[uint16 BE length][content]` — content occupies exactly `length` bytes, unused slack padded. |
| `int` | `age:int` | 4 | Signed 32-bit, big-endian. Range ±2.1 B. |
| `long` | `id:long` | 8 | Signed 64-bit, big-endian. |
| `short` | `flags:short` | 2 | Signed 16-bit, big-endian. Range ±32 k. |
| `double` | `score:double` | 8 | IEEE 754. |
| `float` | `weight:float` | 4 | IEEE 754 single-precision. |
| `bool` | `active:bool` | 1 | `0` = false, `1` = true. Wire accepts exactly: `1 t tr tru true y ye yes on` (true) and `0 f fa fal fals false n no of off` (false), in any letter case; `o` alone is ambiguous and rejected. |
| `byte` | `level:byte` | 1 | Unsigned 8-bit. |
| `date` | `dob:date` | 4 | `yyyyMMdd` as int32 BE (e.g., `20260418`). Input: `YYYYMMDD` or `YYYY-MM-DD`, years 1..9999, real Gregorian calendar check. |
| `datetime` | `created:datetime` | 7 | `yyyyMMdd` (int32 BE) + seconds-of-day (uint24 BE, 0..86399). Widened from 6 bytes in 2026.09.2; startup migration rewrites old objects. Input: `YYYYMMDDHHMMSS`, `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DDTHH:MM:SS` (HH 0..23, MM/SS 0..59 — no leap second). |
| `datetimems` | `created_at:datetimems` | 8 | `yyyyMMdd` (int32 BE) + `ms-of-day` (uint32 BE, `0..86399999`). Wire format is the 17-digit string `"yyyyMMddHHmmssfff"`. Input: `YYYYMMDDHHMMSSfff` or `YYYY-MM-DD HH:MM:SS.fff` — exactly 3 fraction digits. |
| `time` | `t:time` | 3 | Seconds-of-day packed as 3 big-endian bytes. Parsed from `HH:MM:SS` (exactly — `00:00:00`..`23:59:59`, no leap second). Anything else is rejected with a field-named error (R4). |
| `timestamp` | `created_at:timestamp` | 8 | Unix epoch **milliseconds** as int64 BE. Storage layout is identical to `long`; the type carries the additional semantic that `:auto_create` / `:auto_update` emit `clock_gettime(CLOCK_REALTIME)` in ms. Suitable for API timelines, event ordering, telemetry — anything where `Date.now()`-style values are the lingua franca. Distinct from `datetime` (which is calendar-packed and can't represent pre-1970 / post-9999). Available since 2026.05.6. |
| `uuid` | `id:uuid` | 16 | Raw 128-bit UUID. Parsed from 32 hex digits or the 36-char dashed form, optionally wrapped in one `{…}` pair; case-insensitive. Anything else is rejected with a field-named error (R4). |
| `ipv4` | `addr:ipv4` | 4 | Raw 4-byte IPv4 address, network byte order. Parsed from a dotted-quad string (e.g. `192.168.1.1`); the whole input must parse. Anything else is rejected with a field-named error (R4). Byte-lexicographic order matches numeric IPv4 order. |
| `ipv6` | `addr:ipv6` | 16 | Raw 16-byte IPv6 address, network byte order. Parsed from a canonical IPv6 string (e.g. `2001:db8::1`); the whole input must parse. Anything else is rejected with a field-named error (R4). Byte-lexicographic order matches numeric IPv6 order. |
| `numeric` | `price:numeric:P,S` | 8 | Scaled int64 BE: stored value = value × 10^S, parsed exactly (no float), rounded half-away-from-zero to the declared scale — identically on writes and in query literals. P is informational until the packed exact format; S is validated to 0..18. Values whose scaled magnitude exceeds int64 are rejected with an explicit error. |
| `currency` | `amount:currency` | 8 | Alias for `numeric:19,4`. |
| `enum` | `color:enum(red,green,blue)` | 1 or 2 | Declared closed value list. Stored as the value's 0-based byte index; **1 byte** for ≤256 values, **2 bytes BE** for 257–65535. Wire format is the display string (`"red"`); the engine validates against the list on insert. Commas inside enum values aren't supported in v1. Bitmap index is opt-in (declare `color:bitmap`, or the bare field name which promotes to bitmap). Available since 2026.05.7. Value lists share the 65,535-byte field-definition cap (see [limits](../reference/limits.md)) — ≈13k four-char values. |


### Strict scalar grammars (R4)

Typed scalars share one strict, length-aware grammar on **every** write route
(insert, update, bulk JSON, bulk delimited cells, `:default=` literals) and on
**query comparison literals** (`eq neq lt gt lte gte between in not_in`). A
value that is not in the grammar is rejected with
`value for field '<field>' is not a valid <type>: <detail>` — malformed input
never silently becomes a different value, zero, or NULL.

| Type | Accepted forms (outer ASCII space/tab/newline/CR stripped first) |
|---|---|
| `bool` | `1 t tr tru true y ye yes on` / `0 f fa fal fals false n no of off`, any letter case; `o` is ambiguous → error |
| `date` | `YYYYMMDD` or `YYYY-MM-DD`; year 1..9999; real Gregorian day limits |
| `datetime` | `YYYYMMDDHHMMSS`, `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS` |
| `datetimems` | `YYYYMMDDHHMMSSfff` or `YYYY-MM-DD HH:MM:SS.fff` (exactly 3 fraction digits) |
| `time` | exactly `HH:MM:SS`, `00:00:00`..`23:59:59` |
| `uuid` | 32 hex digits or 36-char 8-4-4-4-12 dashed; one matching `{…}` pair allowed; case-insensitive |
| `ipv4` / `ipv6` | whole input accepted by the address parser (v4 ≤ 15, v6 ≤ 45 chars); no truncation |

Explicit JSON `null` still stores NULL. The zero values — nil UUID, `0.0.0.0`,
`::`, `00:00:00`, midnight, empty varchar — remain real, non-NULL values.
Pattern/substring operators (`starts ends contains like`, i-variants, regex,
`len_*`) and `*_field` comparisons operate on rendered text and are **not**
grammar-validated. Enum query literals are validated like writes (unknown
member → error). Legacy rows written before R4 are read as-is; only new input
is validated. Delimited (CSV) empty cells keep their historical zero-fill
semantics. See the [changelog](../reference/changelog.md) for the breaking
changes introduced by R4.

### varchar sizing

`varchar:N` reserves `N + 2` bytes per record. Content longer than N is rejected on insert. Pick N carefully:

- Too small → insert errors later. Migrating to a larger N requires `add-field` + manual repopulation.
- Too large → record payload bloats. Every record pays the full reserved size, even for short strings — both inside the segment file and in the slot-size accounting used to estimate disk budget.

Typical patterns:
- `email:varchar:200`
- `name:varchar:100`
- `sku:varchar:32`
- Notes/freeform: `varchar:1000` up to the 65535 byte ceiling.

For larger blobs — images, PDFs — store them via [put-file](../query-protocol/files.md) and reference them by filename in a varchar.

### numeric vs double

- **`double`** — fast, native IEEE 754. Use for rates, scores, physical measurements. Don't use for money.
- **`numeric:P,S`** — stored as `int64 × 10^S`. Use for money, quantities with exact decimals, anything where `0.1 + 0.2` must equal `0.3`. Accepted and returned as strings: `"1500.75"`.
- **`currency`** — shortcut for `numeric:19,4` (enough for any real-world currency value).

### Scalar conversion contract (2026.10 Phase 1)

Writes, updates, defaults, bulk rows and numeric query literals validate
the entire value through one shared exact converter: whole decimal tokens
for `short`/`int`/`long`/`timestamp`/`byte` (optional sign, no fraction
or exponent, full range enforced — no wrapping or saturation, `INT64_MIN`
included), finite `float`/`double` text via `strtof`/`strtod` (non-finite
spellings, overflow, and nonzero underflow-to-zero rejected; subnormals
accepted), and exact scaled-decimal `numeric`/`currency` as above. Outer
ASCII whitespace is stripped once — for the scalar numeric types only;
quoted JSON strings for those numeric types and `varchar` are JSON-decoded
exactly once before validation, with escaped NUL rejected. Unquoted JSON
literals and non-JSON CSV/default literals keep their literal meaning.
`varchar` and `enum` values are byte-exact after their applicable JSON
decoding; raw CSV varchar cells preserve their full byte span, including
embedded NULs. Embedded whitespace, trailing
junk and hex floats are errors. A numeric literal in a query is converted
by the same rule as the write, so `0.145` at scale 2 finds the row stored
from `0.145` (both mean 0.15). Invalid values fail the write with a
field-specific error — the historic wrap/saturation/zero behaviors are
gone. `float` output uses 9 and `double` 17 significant digits (exact
binary round trip), so `3.14` renders as `3.1400000000000001`. Malformed
calendar/boolean/UUID/IP input keeps its pre-2026.10 behavior until the
Phase 7 validation layer.

## Field defaults

Append default modifiers after the type spec. They trigger server-side when the field is absent from the request; a client-provided value always wins.

| Modifier | Description | Example |
|---|---|---|
| `default=<value>` | Constant default on **INSERT** | `status:varchar:20:default=pending` |
| `default=seq(name)` | Next value from named [sequence](../query-protocol/overview.md) on **INSERT** | `id:long:default=seq(invoice_id)` |
| `default=uuid()` | UUID v4 on **INSERT** (36 chars) | `token:varchar:36:default=uuid()` |
| `default=random(N)` | N random bytes, hex-encoded (2N chars) on **INSERT** | `salt:varchar:16:default=random(8)` |
| `auto_create` | Server timestamp on **INSERT** only | `created:datetime:auto_create` (calendar-packed) or `created_at:timestamp:auto_create` (epoch ms, 2026.05.6+) |
| `auto_update` | Server timestamp on **INSERT and every UPDATE** | `modified:datetime:auto_update` or `updated_at:timestamp:auto_update` |

A single field can carry at most one of: `default=...`, `auto_create`, `auto_update`. A `:default=<literal>` is limited to 255 bytes (see [limits](../reference/limits.md)); longer literals are rejected at `create-object` / `add-field` / `edit-field` rather than silently truncated.

### Pattern: versioned records with CAS

```json
"fields": [
  "status:varchar:20",
  "version:long:default=seq(version_counter)",
  "updated:datetime:auto_update"
]
```

Combined with `{"mode":"update", "if":{"version":42}}` gives you optimistic concurrency control without app-side locking. The `default=seq(version_counter)` runs on **insert only** — an update never re-runs the insert default, so the CAS revision must be supplied or incremented explicitly by the caller (e.g. by `{"value":{"version":43}}`), or by a separately implemented update policy.

## NULL values (2026.10.2)

Typed fields are **nullable**. A record carries a per-record null bitmap
(`1 + ceil(nfields/8)` bytes, directly after the segment value header)
with one bit per field: bit set = the field is NULL. On-disk the value
region is `[bm_nbytes][bitmap bytes][field bytes]`; the bitmap prefix is
transparent to readers — field offsets are field-region-relative, and a
NULL field's stored bytes are canonical zeros that never decode.

### Writing NULLs

- **Insert / bulk-insert:** an explicit JSON `null` for a field stores
  NULL; a field **omitted** from the value (with no default) also stores
  NULL.
- **Update / bulk-update:** an explicit `null` clears the field to NULL;
  a field omitted from the update value **carries the old value forward**
  (including a previous NULL). The same §2.7 conversion drives CAS,
  replay, and rebuilds.
- Raw (unquoted) JSON `null` is rejected where a value is required
  (`eq`/`between` bounds, `in` members); the quoted string `"null"` is a
  legitimate varchar value. NULL checks use the `exists` / `not_exists`
  operators.

### Reading NULLs

- `get`/`find` render a NULL field as JSON `null`.
- `eq`, `in`, ranges, `like`, etc. never match a NULL field; the negative
  predicates (`neq`, `not_in`, `not_like`, …) don't match it either — a
  NULL belongs to neither a positive value predicate nor its negation.
- `exists` is true when the field holds any non-NULL value; `not_exists`
  matches NULL. On legacy (pre-2026.10.2) records the query-existence
  predicate is generation-correct: zero-value markers (`""` varchar,
  midnight, zero date) read as absent for text-style checks exactly as
  they always have, so indexed and unindexed results agree on
  mixed-generation data.

### NULLs and indexes (BTRI)

An indexed field stores **one key per record, including NULLs**: value
keys are `[0x00][sortable bytes]`, NULL keys are the single byte
`[0x01]`. This is the `'BTRH'` → `'BTRI'` format bump (composite keys
interleave one tier byte per component). Practical consequences:

- `not_exists` on an indexed field is a full scan (correct, not
  index-driven); `exists` drives the tier-0 walk with per-record
  verification.
- Ranges stop below the NULL tier — `gt 0` never returns NULL rows.
- Ordered finds place NULL rows **last ASC / first DESC**, and keyset
  cursor tokens carry a per-field tier byte (the `"tiers"` array) so
  pagination crosses the NULL boundary without repeating or skipping.
- Bitmap-indexed fields store no entry for NULL (a bitmap cannot
  enumerate "no bit set"): `exists`/`not_exists` and NULL groups on
  bitmap-indexed fields fall back to full scans. Documented, rare.
- An object whose indexes are still `'BTRH'` is upgraded automatically:
  the **2026.10.2 startup migration** reindexes every indexed object
  (strictly, fail-closed) before serving and stamps completion; a
  `'BTRH'` index file also refuses per-request as defense-in-depth.

### NULLs in aggregates

- `sum` / `avg` / `min` / `max` skip NULLs (SQL semantics); `avg` of an
  all-NULL group renders `null`.
- `group_by` gives NULL its **own group**, rendered as bare JSON `null` —
  `NULL`, `""`, and the literal `"null"` are three distinct buckets.
- `count(field)` counts non-NULL values; `count(*)` is a row count.

### Legacy conversion

Rewriting a legacy record into the nullmap format (bulk-update merge,
vacuum recompose, schema-mutation rebuilds) follows one rule: an explicit
null beats the old value; an omitted field carries the old value forward;
a legacy **absent** marker (field bytes beyond the record region) converts
to NULL. New fields added by add-field are never NULL — existing records
read zeros/backfill, never NULL.

### NOT NULL constraint (`:not_null`, 2026.10.2)

Appending `:not_null` to a field spec (or `edit-field <name>:<type>:not_null`)
makes every NULL-producing write branch a request error for that field —
the SQL `NOT NULL` invariant over the same per-record bitmap. The flag is
per field (a composite index `a+n` has no modifier of its own; declare
`:not_null` on each component you want mandatory). `describe-object`
renders `"not_null":true` for constrained fields.

Rejections — every branch that would store NULL on a constrained field:

- **Explicit JSON `null`** on insert / bulk-insert (JSON) / update /
  bulk-update (both JSON forms): error `field '<name>' is not nullable`.
  A `:default=` does **not** rescue an explicit null — the default
  applies to omission only (the SQL rule).
- **Omitted insert field with no default**: same error. Declaring
  `:not_null` without a default is legal; it makes omission an error.
- **Malformed `uuid`/`ipv4`/`ipv6` values**: rejected. Under the null-bitmap
  sentinel convention these would store NULL, which the constraint
  forbids. (`bulk-update`-delimited and single update malformed sentinels
  write a bit-clear zero, not NULL — a known non-NULL edge, unchanged.)
- Bulk insert / bulk update per-record JSON: a violating record is
  skipped and counted (`errors`/`skipped`) while peers commit — no
  per-record error channel exists on those paths; the field name is
  logged. The criteria-form bulk update refuses the whole request.
- Criteria and aggregates get **no shortcut** from the constraint:
  `not_exists` on a `:not_null` field still scans (it currently finds 0
  rows; that constant-folding is deliberately not implemented).

Zeros are values, not NULL: `0.0.0.0`, `::`, the nil UUID, `0`, `0.0`,
`""`, midnight — all legal on a `:not_null` field when well-formed.

Grammar restrictions: an enum value name containing the substring
`:not_null` or `:nullable` (e.g. `enum(a,b:not_null)`) is rejected — the
structural rule cannot distinguish it from a modifier. For upgrade
safety, a root whose stored evidence predates 2026.10.2 refuses to open
if any fields.conf line carries either substring (the raw audit runs
before recovery and before the version stamp; it never edits anything).
Resolve the ambiguity with the previous release's supported schema edits
(change a default literal or rename the enum value with explicit
`allow_rename`, which preserves stored value positions); do not delete
columns or hand-stamp the root. Roots already stamped 2026.10.2 use the
new grammar directly.

Enabling the constraint on existing data: `edit-field` adding `:not_null`
first runs a read-only, fail-closed NULL scan over the object's live
records (legacy-generation records count as NULL exactly when the next
rewrite would convert them to NULL). Nonzero NULLs refuse the edit with
a count; an unreadable record or invalid live reference refuses it too.
See [schema-mutations](../query-protocol/schema-mutations.md) for the
edit-field rules.

## On-disk encoding

Record payloads are encoded by `typed_encode()` (in `config.c`), which packs each field in declaration order. A reader uses `typed_decode()` — also cached per object via `g_typed_cache` — to unpack without re-reading the schema.

The fixed-field description below is the currently public legacy format.
The private [whole-record JSON reader bridge](whole-record-readers.md) also
recognizes explicitly bound packed schemas; it does not activate their loaders,
projection/query consumers or migration. GET validates and builds its actual
JSON response before output, with checked allocation/write errors.

No field-separator bytes, no length tags (varchar has its own uint16 length prefix, but nothing separates fields from each other). Position is everything: field `i` starts at the sum of sizes of fields `0..i-1`. This makes `typed_get_field_str(ts, raw, field_index)` a direct memcpy of a known byte range.

## Schema mutations

Fields can be added/renamed/removed at runtime. See [Query protocol → Schema mutations](../query-protocol/schema-mutations.md).

Order of operations when evolving a schema:

1. **Add new field** — `add-field`. Triggers an object rebuild that re-encodes every record with the new (default-filled) column.
2. **Rename field** — `rename-field`. Metadata-only; no data rewrite. Composite indexes referring to the renamed field are also updated.
3. **Remove field** — `remove-field`. Tombstones the field in `fields.conf`; bytes stay reserved in every record's payload until `vacuum --compact`.
4. **Drop tombstones** — `vacuum --compact`. Full rebuild, shrinks per-record payload size.

## Auditing the layout

- `./shard-db query '{"mode":"create-object",...}'` response includes `value_size` — the total payload bytes per record.
- `cat $DB_ROOT/<dir>/<obj>/fields.conf` — authoritative field order and types (including tombstoned fields, marked `:removed`).
- `./shard-db shard-stats <dir> <obj>` — per-kf-shard load and per-stream segment counts.
- `./shard-db query '{"mode":"describe-object","dir":"<d>","object":"<o>"}'` — full schema dump including `splits`, `max_key`, `value_size`, `streams`, `storage_version`, and the field list.
