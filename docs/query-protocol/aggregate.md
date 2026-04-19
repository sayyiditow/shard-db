# aggregate

Group-by aggregations with `count`, `sum`, `avg`, `min`, `max`. Supports `having` (post-aggregation filter), `order_by`, and `limit`. Numbers are accumulated directly in typed form — no string round-trip.

## Shape

```json
{
  "mode": "aggregate",
  "dir": "<dir>",
  "object": "<obj>",

  "criteria": [ ... ],
  "group_by": ["status", "currency"],
  "aggregates": [
    {"fn":"count", "alias":"total"},
    {"fn":"sum",   "field":"amount", "alias":"revenue"},
    {"fn":"avg",   "field":"amount", "alias":"avg_ticket"},
    {"fn":"min",   "field":"amount", "alias":"smallest"},
    {"fn":"max",   "field":"amount", "alias":"largest"}
  ],

  "having":   [{"field":"total","op":"gt","value":"100"}],
  "order_by": "revenue",
  "order":    "desc",
  "limit":    20
}
```

Required: `mode`, `dir`, `object`, `aggregates`.

## Parameters

| Field | Type | Default | Meaning |
|---|---|---|---|
| `criteria` | array | `[]` | WHERE — filter records before aggregating. Same shape as [`find`](find.md). |
| `group_by` | array of field names | none | Grouping keys. Omit for whole-table aggregation. |
| `aggregates` | array | required | List of aggregate specs (see below). Max 32. |
| `having` | array | none | Filter groups after aggregation. Same shape as `criteria`, but fields are aggregate **aliases** (or group-by fields). |
| `order_by` | string | none | Sort groups by this alias (or group-by field). |
| `order` | `"asc"` / `"desc"` | `"asc"` | Sort direction. |
| `limit` | int | `GLOBAL_LIMIT` | Max groups returned. |

## Aggregate spec

```json
{"fn":"<function>","field":"<field-name>","alias":"<output-name>"}
```

| Function | Needs `field` | Notes |
|---|---|---|
| `count` | optional | Without `field`, counts all records in the group. With `field`, counts non-null/non-empty. |
| `sum` | yes | Numeric types only (`int`, `long`, `short`, `double`, `numeric`). Result is a `double` for speed. Cast back in the app if needed. |
| `avg` | yes | `sum / count`, returned as double. |
| `min` | yes | Numeric or varchar. |
| `max` | yes | Numeric or varchar. |

`alias` is **required** — it names the output column. Without it the result would be ambiguous across multiple aggregates on the same field.

## Response

### Whole-table (no `group_by`)

```json
// Request
{"mode":"aggregate","dir":"acme","object":"orders",
 "aggregates":[
   {"fn":"count","alias":"total"},
   {"fn":"sum","field":"amount","alias":"revenue"}
 ]}

// Response
{"total": 5000, "revenue": 1250000}
```

### With `group_by`

```json
// Request
{"mode":"aggregate","dir":"acme","object":"orders",
 "group_by":["status"],
 "aggregates":[
   {"fn":"count","alias":"n"},
   {"fn":"sum","field":"amount","alias":"total"}
 ]}

// Response
[
  {"status":"paid","n":3000,"total":800000},
  {"status":"pending","n":2000,"total":450000}
]
```

## `having`

Post-aggregation filter. Fields referenced must be aggregate aliases (or group-by fields):

```json
"having": [
  {"field":"n","op":"gte","value":"100"},
  {"field":"total","op":"gt","value":"10000"}
]
```

Multiple `having` clauses are AND-combined.

## Cost

- **Whole-table, no criteria** — O(N) scan of every record. Parallelized across shards.
- **With criteria** — same as [`find`](find.md): indexed candidate scan + per-record typed comparison.
- **With `group_by`** — accumulates into a hash table keyed by group values; final sort + limit.

Typical latency on 1 M records: 1–3 ms for indexed, 2–10 ms for full scans with aggregation.

## Recipes

### Top-10 products by revenue, min 100 units sold

```json
{"mode":"aggregate","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"fulfilled"}],
 "group_by":["product_sku"],
 "aggregates":[
   {"fn":"count","alias":"units_sold"},
   {"fn":"sum","field":"line_total","alias":"revenue"},
   {"fn":"avg","field":"line_total","alias":"avg_ticket"}
 ],
 "having":[{"field":"units_sold","op":"gte","value":"100"}],
 "order_by":"revenue","order":"desc",
 "limit":10}
```

### Daily signup counts

```json
{"mode":"aggregate","dir":"default","object":"users",
 "group_by":["signup_date"],
 "aggregates":[{"fn":"count","alias":"new_users"}],
 "order_by":"signup_date","order":"desc"}
```

### Min/max balance by customer tier

```json
{"mode":"aggregate","dir":"default","object":"accounts",
 "group_by":["tier"],
 "aggregates":[
   {"fn":"min","field":"balance","alias":"smallest"},
   {"fn":"max","field":"balance","alias":"largest"},
   {"fn":"avg","field":"balance","alias":"avg_balance"}
 ]}
```

## OR in `criteria` and `having`

Both `criteria` and `having` accept the full AND/OR tree (see [find → OR criteria](find.md)):

```json
{"mode":"aggregate","dir":"acme","object":"orders",
 "criteria":[
   {"or":[
     {"field":"status","op":"eq","value":"paid"},
     {"field":"status","op":"eq","value":"refunded"}
   ]}
 ],
 "group_by":["region"],
 "aggregates":[{"fn":"count","alias":"n"},{"fn":"sum","field":"amount","alias":"total"}],
 "having":[{"or":[{"field":"n","op":"gte","value":"100"},{"field":"total","op":"gte","value":"10000"}]}]}
```

The planner paths from [find](find.md#execution-paths) apply here too — when the OR is fully indexed the aggregate source records come from a KeySet rather than a shard scan.

## CLI shortcut

```bash
./shard-db aggregate <dir> <obj> '<aggregates_json>' [group_by_csv] [criteria_json] [having_json]
```

`group_by` is a plain comma-separated field list (whitespace tolerated). Later positional slots can be skipped with an empty `''` argument — e.g. to pass `having` without `criteria`:

```bash
./shard-db aggregate acme orders \
  '[{"fn":"count","alias":"n"}]' \
  'status,region' \
  '' \
  '[{"field":"n","op":"gte","value":"100"}]'
```

## Limitations

- Max **32 aggregates per query** (`MAX_AGG_SPECS`).
- Max `limit` capped by `GLOBAL_LIMIT` (default 100,000).
- No `DISTINCT` — emulate with `group_by` + `count`.
- No window functions — aggregate against a query, keep state in the app.
- No nested aggregates.
