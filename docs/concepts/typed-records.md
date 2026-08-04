# Typed records

Every object has a **typed binary schema**, declared once at `create-object` time and stored in `<object>/fields.conf`. Records on disk are a fixed-width packed layout driven by this schema — not JSON.

On the v2 slotcask engine these typed records are the **value payload** of each segment record (`<obj>/data/streams/<stream>/<file>.dat`), prefixed by a 24-byte segment record header (hash + klen + flag + vlen) and followed by `klen` bytes of raw key.

## Why typed records

- **Zero-malloc matching** — `match_typed()` compares criterion values directly against byte ranges; no JSON parsing per record.
- **Compact** — an `int` is 4 bytes, not 4–11 ASCII digits. Payloads pack densely.
- **Correct numerics** — `numeric:P,S` stores fixed-point decimals without IEEE 754 drift.

## Declaring fields

Fields are declared in the `create-object` request as an array of strings, each `name:type[:param]`:

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
| `bool` | `active:bool` | 1 | `0` = false, `1` = true. |
| `byte` | `level:byte` | 1 | Unsigned 8-bit. |
| `date` | `dob:date` | 4 | `yyyyMMdd` as int32 BE (e.g., `20260418`). |
| `datetime` | `created:datetime` | 6 | `yyyyMMdd` (int32 BE) + `HHmmss` (uint16 BE packed). |
| `datetimems` | `created_at:datetimems` | 8 | `yyyyMMdd` (int32 BE) + `ms-of-day` (uint32 BE, `0..86399999`). Wire format is the 17-digit string `"yyyyMMddHHmmssfff"`. |
| `time` | `t:time` | 3 | Seconds-of-day packed as 3 big-endian bytes. Parsed from `HH:MM:SS`. Malformed input encodes 0. |
| `timestamp` | `created_at:timestamp` | 8 | Unix epoch **milliseconds** as int64 BE. Storage layout is identical to `long`; the type carries the additional semantic that `:auto_create` / `:auto_update` emit `clock_gettime(CLOCK_REALTIME)` in ms. Suitable for API timelines, event ordering, telemetry — anything where `Date.now()`-style values are the lingua franca. Distinct from `datetime` (which is calendar-packed and can't represent pre-1970 / post-9999). Available since 2026.05.6. |
| `uuid` | `id:uuid` | 16 | Raw 128-bit UUID. Parsed from `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (36 chars). Malformed input encodes 0. |
| `ipv4` | `addr:ipv4` | 4 | Raw 4-byte IPv4 address, network byte order. Parsed from dotted-quad string (e.g. `192.168.1.1`). Malformed input encodes 0. Byte-lexicographic order matches numeric IPv4 order. |
| `ipv6` | `addr:ipv6` | 16 | Raw 16-byte IPv6 address, network byte order. Parsed from canonical IPv6 string (e.g. `2001:db8::1`). Malformed input encodes 0. Byte-lexicographic order matches numeric IPv6 order. |
| `numeric` | `price:numeric:P,S` | 8 | Scaled int64 BE: stored value = value × 10^S. P is total digits (informational), S is scale. |
| `currency` | `amount:currency` | 8 | Alias for `numeric:19,4`. |
| `enum` | `color:enum(red,green,blue)` | 1 or 2 | Declared closed value list. Stored as the value's 0-based byte index; **1 byte** for ≤256 values, **2 bytes BE** for 257–65535. Wire format is the display string (`"red"`); the engine validates against the list on insert. Commas inside enum values aren't supported in v1. Auto-defaults to a [bitmap index](indexes.md). Available since 2026.05.7. |

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

A single field can carry at most one of: `default=...`, `auto_create`, `auto_update`.

### Pattern: versioned records with CAS

```json
"fields": [
  "status:varchar:20",
  "version:long:default=seq(version_counter)",
  "updated:datetime:auto_update"
]
```

Combined with `{"mode":"update", "if":{"version":42}}` gives you optimistic concurrency control without app-side locking. The `default=seq(version_counter)` runs on **insert only** — an update never re-runs the insert default, so the CAS revision must be supplied or incremented explicitly by the caller (e.g. by `{"value":{"version":43}}`), or by a separately implemented update policy.

## On-disk encoding

Record payloads are encoded by `typed_encode()` (in `config.c`), which packs each field in declaration order. A reader uses `typed_decode()` — also cached per object via `g_typed_cache` — to unpack without re-reading the schema.

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
