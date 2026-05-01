# find

The workhorse query mode. Filters records against a criteria array, with optional projection, sorting, pagination, and joins.

## Shape

```json
{
  "mode": "find",
  "dir": "<dir>",
  "object": "<obj>",
  "criteria": [ ... ],

  "offset": 0,
  "limit": 50,
  "fields": "name,email",
  "excludedKeys": "bot1,bot2",
  "order_by": "<field>",
  "order": "asc",
  "format": "rows",
  "join": [ ... ]
}
```

Required: `mode`, `dir`, `object`, `criteria`.

## Parameters

| Field | Type | Default | Meaning |
|---|---|---|---|
| `criteria` | array | required | AND-combined criterion objects (see [Operators](#operators)). Can be empty `[]` — returns all records. |
| `offset` | int | 0 | Skip the first N matches. |
| `limit` | int | `GLOBAL_LIMIT` | Max records to return. |
| `fields` | string OR array | all | Projection. `"name,email"` or `["name","email"]`. |
| `excludedKeys` | string OR array | none | Skip these keys from results. Comma-separated or array. |
| `order_by` | string | — | Sort by this field; matches are buffered and sorted before pagination. |
| `order` | `"asc"` or `"desc"` | `"asc"` | Sort direction when `order_by` is set. |
| `format` | `"rows"` / `"csv"` / `"dict"` | JSON array | See response shapes below. Ignored when `join` is present (always tabular). |
| `join` | array | none | See [joins](joins.md). |

## Response

**Default (JSON array of records):**

```json
[
  {"key":"u1","value":{"name":"Alice","email":"a@x.com","age":30}},
  {"key":"u2","value":{"name":"Bob","email":"b@x.com","age":22}}
]
```

**With `"format":"rows"`:**

```json
{
  "columns": ["key","name","email","age"],
  "rows": [
    ["u1","Alice","a@x.com",30],
    ["u2","Bob","b@x.com",22]
  ]
}
```

Rows form is ~30% smaller on the wire and drops directly into spreadsheets / charting libraries.

**With `"format":"dict"` (new in 2026.05.1):**

```json
{
  "u1":{"name":"Alice","email":"a@x.com","age":30},
  "u2":{"name":"Bob","email":"b@x.com","age":22}
}
```

Dict form gives O(1) lookup by primary key on the client side and round-trips with `bulk-insert`'s dict shape. Combine with `cursor` and the envelope becomes `{"results":{...},"cursor":...}`. Rejected with `join` (joins force tabular). With `order_by`, dict iteration order is parser-dependent — use the default array or `format:"rows"` if you need strict client-side iteration order.

**With `"format":"csv"`:** raw CSV text (no JSON envelope). See [overview](overview.md#csv-output).

**With `"format":"csv"` (raw CSV, not JSON):**

```
key,name,email,age
u1,Alice,a@x.com,30
u2,Bob,b@x.com,22
```

Plain text body — no JSON wrapping. Optional `"delimiter":"|"` picks a single-char separator (`\t` literal for tab; defaults to `,`). Values containing the delimiter or `"` are RFC-4180-quoted with internal `"` doubled; newlines inside values are replaced with a space so one physical line equals one logical row. Errors still come as JSON `{"error":"..."}`. Combines with `join` (2026.05.1+) — emits the same column-prefixed tabular table as the JSON shape, just CSV-encoded. Same shape works on [`fetch`](#fetch) and [`aggregate`](aggregate.md).

## Criteria shape

Each element of `criteria` is a JSON object:

```json
{"field":"<field-name>","op":"<operator>","value":"<value>"}
```

For `between`:

```json
{"field":"age","op":"between","value":"18","value2":"65"}
```

Multiple criteria at the top level are **AND**-combined. Compose **OR** with a `{"or":[...]}` sub-node:

```json
"criteria":[
  {"field":"status","op":"eq","value":"paid"},
  {"or":[
    {"field":"region","op":"eq","value":"us"},
    {"field":"total","op":"gte","value":"1000"}
  ]}
]
```

Reads as: `status = 'paid' AND (region = 'us' OR total >= 1000)`.

- `{"or":[...]}` — branch matches if **any** child matches.
- `{"and":[...]}` — explicit AND sub-branch, useful when nesting.
- Flat arrays remain implicit AND (zero change to existing queries).
- Max nesting depth is **16**. Empty `or:[]` / `and:[]` returns `{"error":"empty or/and"}`.

### Execution paths

The planner selects automatically:

| Shape | Example | Strategy |
|---|---|---|
| Single indexed leaf | `[A]` where A is indexed | Primary-indexed-leaf scan. |
| **Pure AND, 2+ indexed leaves on rangeable ops** | `[{a=x}, {b>10}]` | **PRIMARY_INTERSECT** — walk each leaf's btree into a `KeySet`, intersect candidate hash sets, skip per-record fetch for `count`. Walks most-selective-first. Eligible ops: eq, lt, gt, lte, gte, between, in, starts. Caps at `MAX_INTERSECT_LEAVES=8`. |
| Pure AND, mixed | `[{a=x}, {bio contains x}]` | Primary-indexed-leaf for the indexed sibling; the rest post-filter via `criteria_match_tree`. |
| AND + OR, AND sibling indexed | `[{a=x}, {or:[{b},{c}]}]` | AND leaf drives candidates; OR sub-tree evaluated per record. OR children don't need indexes. |
| Pure OR, every child indexed | `[{or:[{a=x},{b=y}]}]` | Per-child B+ tree lookups unioned into a lock-free `KeySet`. Sublinear — no shard scan. Pure-OR `count` returns `\|KeySet\|` directly. |
| OR with any non-indexed child | `[{or:[{a=x},{b=y}]}]` (b not indexed) | Full parallel shard scan + tree match. |

Hybrid (non-indexed AND + fully-indexed OR sub-tree) uses the KeySet as primary-candidate source and applies the AND siblings as a post-filter.

Per-shard btree layout: each indexed field is sharded into `splits/4` btree files at `<obj>/indexes/<field>/<NNN>.idx`. Reads fan out across all shards via the parallel-for pool; writes route by record hash to one shard.

## Operators

**38 operators.** Every operator uses an index when the field is indexed (with the exceptions noted below for full-scan ops). Composite indexes (`field1+field2`) only assist `eq`, `starts`, and `between`-via-prefix; substring/range ops on composites fall back to leaf scan.

### Equality, range, set membership

| Operator | Description | Applies to | Example |
|---|---|---|---|
| `eq` / `equal` | Exact match | all types | `{"field":"status","op":"eq","value":"paid"}` |
| `neq` / `not_equal` | Not equal | all | `{"field":"status","op":"neq","value":"void"}` |
| `lt` / `less` | Strictly less than | numeric, date, datetime | `{"field":"age","op":"lt","value":"65"}` |
| `gt` / `greater` | Strictly greater than | numeric, date, datetime | `{"field":"age","op":"gt","value":"18"}` |
| `lte` / `less_eq` | `<=` | numeric, date, datetime | `{"field":"score","op":"lte","value":"999"}` |
| `gte` / `greater_eq` | `>=` | numeric, date, datetime | `{"field":"score","op":"gte","value":"100"}` |
| `between` | Inclusive range | numeric, date, datetime, varchar (lexicographic) | `{"field":"age","op":"between","value":"18","value2":"65"}` |
| `in` | Value in CSV set | all | `{"field":"status","op":"in","value":"active,pending"}` |
| `nin` / `not_in` | Value not in set | all | `{"field":"role","op":"nin","value":"bot,test"}` |
| `exists` | Field present / non-empty | all | `{"field":"phone","op":"exists"}` |
| `nexists` / `not_exists` | Field missing / empty | all | `{"field":"deleted_at","op":"nexists"}` (forces full scan — missing fields aren't in the index) |

### Varchar matching — case-sensitive (default)

| Operator | Description | Notes |
|---|---|---|
| `like` | Wildcard — `%` or `*` | Indexed shortcut: `"foo"` (no `%`) → point lookup; `"foo%"` → prefix range; `"%foo"` / `"%foo%"` → leaf scan |
| `nlike` / `not_like` | Wildcard negated | Leaf scan |
| `contains` | Substring match | Leaf scan with per-entry filter |
| `ncontains` / `not_contains` | Not substring | Leaf scan |
| `starts` / `starts_with` | Prefix match | Indexed prefix range scan |
| `ends` / `ends_with` | Suffix match | Leaf scan |

### Varchar matching — case-insensitive (ASCII tolower)

`ilike`, `not_ilike`, `icontains`, `not_icontains`, `istarts`, `iends` — same semantics as their case-sensitive counterparts but `tolower` per byte before compare. Always leaf scan (no prefix shortcut).

### Length filters (varchar only — answered from btree leaf entry's vlen, no record fetch)

| Operator | Example |
|---|---|
| `len_eq` | `{"field":"name","op":"len_eq","value":"5"}` |
| `len_neq` | `{"field":"name","op":"len_neq","value":"5"}` |
| `len_lt`, `len_gt`, `len_lte`, `len_gte` | `{"field":"bio","op":"len_lt","value":"10"}` |
| `len_between` | `{"field":"name","op":"len_between","value":"3","value2":"8"}` |

### Field-vs-field (compare two fields on the same record — full scan only)

| Operator | Example |
|---|---|
| `eq_field`, `neq_field` | `{"field":"createdAt","op":"eq_field","value":"updatedAt"}` |
| `lt_field`, `gt_field`, `lte_field`, `gte_field` | `{"field":"used","op":"gt_field","value":"limit"}` |

The RHS is per-record so no btree shortcut is possible — these always full-scan.

### Regex (POSIX extended regex on varchar — full scan only)

| Operator | Example |
|---|---|
| `regex` | `{"field":"sku","op":"regex","value":"^[A-Z]{2}-[0-9]{4}$"}` |
| `not_regex` | `{"field":"phone","op":"not_regex","value":"^\\+1"}` |

Compiled once at criteria-parse time; matched per record with `REG_STARTEND` on the hot path. Indexed fields could in theory walk leaves only, but the per-entry `regexec` cost dominates the saving from skipping record fetch — kept on the full-scan path.

### Value formatting

- **varchar** — raw string.
- **int / long / short** — numeric string (`"30"`, `"1000"`).
- **double** — numeric string with optional decimal (`"3.14"`).
- **bool** — `"true"` / `"false"`.
- **date** — `"yyyyMMdd"` (e.g., `"20260418"`).
- **datetime** — `"yyyyMMddHHmmss"` (e.g., `"20260418153012"`).
- **numeric** — decimal string matching the declared scale (`"1500.75"` for `numeric:12,2`).

Types are enforced: passing a non-numeric string to `gt` on an `int` field returns an error.

## Sorting

`order_by` buffers every match into memory, sorts, then slices by `offset` / `limit`. Cost is `O(matches * log matches)` — fine for thousands of matches, not for millions.

- Numeric types (`int`, `long`, `short`, `double`, `numeric`, `date`, `datetime`, `bool`, `byte`) sort numerically.
- `varchar` sorts lexicographically.
- `order: "asc"` is the default; `"desc"` flips.
- Not compatible with `join` — tabular join output doesn't sort.

For streaming through large results in an arbitrary order, use `fetch` with keyset `cursor` pagination instead.

## Cursor pagination (keyset)

For deep pagination on large result sets, offset-based paging pays `O(matches)` per page (the buffer-sort path above). A cursor driven off an indexed `order_by` field is **`O(limit)` regardless of page depth**.

```json
// Page 1 — signal cursor pagination with cursor:null (or cursor:{})
{"mode":"find","dir":"t","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "order_by":"amount","order":"asc","limit":100,"cursor":null}

// Response wraps rows and emits the next-page cursor
{"rows":[...], "cursor":{"amount":"500.00","key":"ord_4912"}}

// Page N+1 — hand back the previous page's cursor verbatim
{"mode":"find", ..., "order_by":"amount","limit":100,
 "cursor":{"amount":"500.00","key":"ord_4912"}}

// Last page returns "cursor":null
```

Rules:
- `order_by` field **must be indexed** — cursor queries reject otherwise with a clear error.
- Cursor tie-breaks on `hash16(primary_key)` when multiple rows share the same `order_by` value, so pagination is stable across ties.
- `cursor:null` or `cursor:{}` in the request opts into cursor mode (page 1, walks from start/end).
- Omitting `cursor` entirely keeps backward-compat behaviour (unwrapped array, buffer-sort for `order_by`).
- `format:"csv"` and `join` are not supported with cursor — use the non-cursor `find` path for those.
- ASC + DESC both supported; the server-side k-way merge across per-shard btree iterators reconstructs global order.

## Projection

`fields` narrows the returned columns. Supports either CSV or JSON array:

```json
"fields": "name,email"
"fields": ["name","email"]
```

`key` is always included in the default form; in `rows` format, `key` is the first column.

## Excluding keys

`excludedKeys` drops matching records by key. Useful for "everything except these":

```json
{"mode":"find","dir":"default","object":"users",
 "criteria":[{"field":"active","op":"eq","value":"true"}],
 "excludedKeys":"bot1,bot2,system"}
```

## Indexed vs full-scan

The planner picks an index automatically. Rules of thumb:

- **Indexed path** — used when any criterion's field has a matching index (single-field or the leading component of a composite). B+ tree scan + record re-filter. 1–3 ms on 1 M records.
- **Full scan** — used when no criterion is indexed. Parallel per-shard walk over Zone A + Zone B for candidates. 2–3 ms on 1 M records because Zone A stays in page cache.

See [Concepts → Indexes](../concepts/indexes.md) for the selection logic.

## Recipes

### Pagination with filter and projection

```json
{"mode":"find","dir":"acme","object":"invoices",
 "criteria":[
   {"field":"status","op":"eq","value":"paid"},
   {"field":"paid_at","op":"gte","value":"20260318000000"}
 ],
 "fields":["id","customer","total","paid_at"],
 "order_by":"paid_at","order":"desc",
 "offset":0,
 "limit":50}
```

### "Not in" a set

```json
{"mode":"find","dir":"default","object":"users",
 "criteria":[{"field":"role","op":"nin","value":"bot,spam,deleted"}]}
```

### Field presence

```json
{"mode":"find","dir":"default","object":"users",
 "criteria":[
   {"field":"phone","op":"exists"},
   {"field":"deleted_at","op":"nexists"}
 ]}
```

### Count only (no records returned)

Use [`count`](count.md) for this — same criteria shape, zero record materialization.

## Joins

Any `find` query can enrich results by joining other objects. See [joins](joins.md).
