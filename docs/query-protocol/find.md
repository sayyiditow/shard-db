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
| `format` | `"rows"` | JSON objects | Return tabular `{"columns":[...],"rows":[...]}`. Ignored when `join` is present (always tabular). |
| `join` | array | none | See [joins](joins.md). |

## Response

**Default (JSON records):**

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

## Criteria shape

Each element of `criteria` is a JSON object:

```json
{"field":"<field-name>","op":"<operator>","value":"<value>"}
```

For `between`:

```json
{"field":"age","op":"between","value":"18","value2":"65"}
```

Multiple criteria are **AND**-combined. No `OR` yet — emulate with multiple queries + dedupe, or use `in` / `not_in` for value sets.

## Operators

Seventeen operators. Every one uses an index when the field is indexed.

| Operator | Description | Applies to | Example |
|---|---|---|---|
| `eq` / `equal` | Exact match | all types | `{"field":"status","op":"eq","value":"paid"}` |
| `neq` / `not_equal` | Not equal | all | `{"field":"status","op":"neq","value":"void"}` |
| `lt` / `less` | Strictly less than | numeric, date, datetime | `{"field":"age","op":"lt","value":"65"}` |
| `gt` / `greater` | Strictly greater than | numeric, date, datetime | `{"field":"age","op":"gt","value":"18"}` |
| `lte` / `less_eq` | `<=` | numeric, date, datetime | `{"field":"score","op":"lte","value":"999"}` |
| `gte` / `greater_eq` | `>=` | numeric, date, datetime | `{"field":"score","op":"gte","value":"100"}` |
| `between` | Inclusive range | numeric, date, datetime | `{"field":"age","op":"between","value":"18","value2":"65"}` |
| `in` | Value in CSV set | all | `{"field":"status","op":"in","value":"active,pending"}` |
| `nin` / `not_in` | Value not in set | all | `{"field":"role","op":"nin","value":"bot,test"}` |
| `like` | Wildcard — `%` or `*` | varchar | `{"field":"name","op":"like","value":"Ali%"}` |
| `nlike` / `not_like` | Wildcard negated | varchar | `{"field":"email","op":"nlike","value":"*@test.com"}` |
| `contains` | Substring match | varchar | `{"field":"bio","op":"contains","value":"engineer"}` |
| `ncontains` / `not_contains` | Not substring | varchar | `{"field":"bio","op":"ncontains","value":"spam"}` |
| `starts` / `starts_with` | Prefix match | varchar | `{"field":"email","op":"starts","value":"admin"}` |
| `ends` / `ends_with` | Suffix match | varchar | `{"field":"email","op":"ends","value":".org"}` |
| `exists` | Field present / non-empty | all | `{"field":"phone","op":"exists"}` |
| `nexists` / `not_exists` | Field missing / empty | all | `{"field":"deleted_at","op":"nexists"}` |

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
