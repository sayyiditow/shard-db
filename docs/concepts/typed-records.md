# Typed records

Every object has a **typed binary schema**, declared once at `create-object` time and stored in `<object>/fields.conf`. Records on disk are a fixed-width packed layout driven by this schema — not JSON.

## Why typed records

- **Zero-malloc matching** — `match_typed()` compares criterion values directly against byte ranges; no JSON parsing per record.
- **Compact** — an `int` is 4 bytes, not 4–11 ASCII digits. Zone B payloads pack densely.
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
| `bool` | `active:bool` | 1 | `0` = false, `1` = true. |
| `byte` | `level:byte` | 1 | Unsigned 8-bit. |
| `date` | `dob:date` | 4 | `yyyyMMdd` as int32 BE (e.g., `20260418`). |
| `datetime` | `created:datetime` | 6 | `yyyyMMdd` (int32 BE) + `HHmmss` (uint16 BE packed). |
| `numeric` | `price:numeric:P,S` | 8 | Scaled int64 BE: stored value = value × 10^S. P is total digits (informational), S is scale. |
| `currency` | `amount:currency` | 8 | Alias for `numeric:19,4`. |

### varchar sizing

`varchar:N` reserves `N + 2` bytes per record. Content longer than N is rejected on insert. Pick N carefully:

- Too small → insert errors later. Migrating to a larger N requires `add-field` + manual repopulation.
- Too large → Zone B slot size bloats. Every record pays the full reserved size, even for short strings.

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
| `auto_create` | Server datetime on **INSERT** only | `created:datetime:auto_create` |
| `auto_update` | Server datetime on **INSERT and every UPDATE** | `modified:datetime:auto_update` |

A single field can carry at most one of: `default=...`, `auto_create`, `auto_update`.

### Pattern: versioned records with CAS

```json
"fields": [
  "status:varchar:20",
  "version:long:default=seq(version_counter)",
  "updated:datetime:auto_update"
]
```

Combined with `{"mode":"update", "if":{"version":42}}` gives you optimistic concurrency control without app-side locking.

## On-disk encoding

Records in Zone B are encoded as `typed_encode()` (in `config.c`) packs each field in declaration order. A reader uses `typed_decode()` — also cached per object via `g_typed_cache` — to unpack without re-reading the schema.

No field-separator bytes, no length tags (varchar has its own uint16 length prefix, but nothing separates fields from each other). Position is everything: field `i` starts at the sum of sizes of fields `0..i-1`. This makes `typed_get_field_str(ts, raw, field_index)` a direct memcpy of a known byte range.

## Schema mutations

Fields can be added/renamed/removed at runtime. See [Query protocol → Schema mutations](../query-protocol/schema-mutations.md).

Order of operations when evolving a schema:

1. **Add new field** — `add-field`. Triggers an object rebuild that re-encodes every record with the new (default-filled) column.
2. **Rename field** — `rename-field`. Metadata-only; no data rewrite. Composite indexes referring to the renamed field are also updated.
3. **Remove field** — `remove-field`. Tombstones the field in `fields.conf`; bytes stay reserved in Zone B until `vacuum --compact`.
4. **Drop tombstones** — `vacuum --compact`. Full rebuild, shrinks `slot_size`.

## Auditing the layout

- `./shard-db query '{"mode":"create-object",...}'` response includes `value_size` — the total Zone B bytes per record.
- `cat $DB_ROOT/<dir>/<obj>/fields.conf` — authoritative field order and types (including tombstoned fields, marked `:removed`).
- `./shard-db shard-stats <dir> <obj>` — per-shard load and slot size.
