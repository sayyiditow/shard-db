# Joins

Read-only joins on [`find`](find.md). Inner and left types, chainable, by the remote object's primary key or any indexed field. Output is always tabular.

## Shape

```json
{
  "mode": "find",
  "dir": "acme",
  "object": "orders",
  "criteria": [{"field":"status","op":"eq","value":"paid"}],

  "join": [
    {
      "object": "users",
      "local":  "user_id",
      "remote": "key",
      "as":     "user",
      "type":   "inner",
      "fields": ["email","name"]
    },
    {
      "object": "products",
      "local":  "product_sku",
      "remote": "sku",
      "as":     "product",
      "type":   "left",
      "fields": ["title","price"]
    }
  ],

  "limit": 50
}
```

## Per-join options

| Field | Required | Meaning |
|---|---|---|
| `object` | yes | Remote object to join. Must live in the same `dir` as the driver. |
| `local` | yes | Driver-side field whose value is looked up in the remote object. Supports composite (`"country+zip"`) when the remote has a matching composite index. |
| `remote` | yes | Either `"key"` (primary-key lookup, O(1) hash) or any **indexed** field on the remote object (O(log n) B+ tree). Unindexed fields are rejected at parse time. |
| `as` | yes | Column prefix in the output. Must be unique — can't collide with the driver object name or other `as` values. |
| `type` | no (default `"inner"`) | `"inner"` drops driver rows without a remote match; `"left"` emits nulls instead. |
| `fields` | no (default all) | Remote fields to include in the output. Tombstoned fields are skipped. |

## Output shape

Always tabular:

```json
{
  "columns": [
    "orders.key", "orders.amount", "orders.status", "orders.user_id",
    "user.email", "user.name",
    "product.title", "product.price"
  ],
  "rows": [
    ["o1", 99.50, "paid", "u42", "a@b.c", "Ana", "Widget",  9.99],
    ["o2", 39.00, "paid", "u51", null,    null,  "Gadget", 19.50]
  ]
}
```

- Driver columns are prefixed with the driver object name (`orders.*`).
- Each join's columns are prefixed with `as` (`user.*`, `product.*`).
- Left-join no-match emits `null` for all the join's columns.
- `key` is always included for the driver; joined objects only emit the fields you list (or all if `fields` is omitted).

Joins always emit a tabular shape. By default that's the JSON `{"columns":[...],"rows":[...]}` envelope. `format:"csv"` (new in 2026.05.1) emits the same table as raw CSV — header row first, then one row per match, RFC 4180-style quoting via `csv_emit_cell`. Custom delimiter via `delimiter:"|"` etc.

```
j_orders.key,j_orders.amount,cust.name,cust.city
ord_1,1500,Alice,Berlin
ord_2,250,Bob,
```

Left-join no-match → empty cell in CSV (parallels the `null` you'd get in the JSON shape). Driver columns prefix with `<driver_object>.<field>`; each joined object's columns prefix with `<as>.<field>`.

`format:"dict"` is **rejected** with `format=dict is not supported with join` — a join produces wide rows that can't sensibly key on the driver alone. `cursor` pagination is also not supported with `join`; use `offset`/`limit`.

## Chaining

Joins apply in declaration order. Each subsequent join uses the same driver record; there's no "join a join" nesting. If you need two hops, chain them as two joins — both off the driver — not as nested joins.

```
driver ── join 1 → remote A
       ── join 2 → remote B
       ── join 3 → remote C
```

## Limit behavior

- **Inner-only (or no) joins** — `limit` is applied after the join. Records that inner-drop don't count toward the limit. You get up to `limit` rows that fully matched.
- **Left joins** — `limit` is applied during collection. Every match (including left-null fallbacks) counts.

## Sorting

Not supported with `join`. Sort in the app after fetching, or pre-filter the driver with sufficient selectivity that the result set fits a reasonable limit.

## Cost

- `remote: "key"` — O(1) hash lookup per driver row. As fast as `get` per record.
- `remote: "<indexed_field>"` — O(log n) B+ tree lookup per driver row, with a typed-equality filter.
- Composite `local` (e.g., `"country+zip"`) requires the remote object to have a matching composite index.

For high-fanout joins (every driver row matches many remote rows), use pagination + projection aggressively — there's no streaming response, everything buffers before flush.

## Recipes

### Enrich orders with customer + product

```json
{"mode":"find","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"customers","local":"customer_id","remote":"key",
    "as":"cust","fields":["name","email","tier"]},
   {"object":"products","local":"product_sku","remote":"sku",
    "as":"prod","fields":["title","price"]}
 ],
 "limit":100}
```

### Left join to detect missing data

```json
{"mode":"find","dir":"default","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"users","local":"user_id","remote":"key",
    "as":"user","type":"left","fields":["email"]}
 ]}
```

Rows where `user.email` is `null` are orders referencing a user that's been deleted.

### Composite join

Driver has `country_code` and `zip_code`; remote has a composite index on `"country+zip"`:

```json
{"mode":"find","dir":"acme","object":"deliveries",
 "join":[
   {"object":"regions","local":"country_code+zip_code","remote":"country+zip",
    "as":"region","fields":["name","timezone"]}
 ]}
```

## Limitations

- Read-only. No joined writes or updates.
- Same-`dir` only. Cross-tenant joins are not supported.
- No nested/subquery joins — join targets are base objects, not query results.
- No right or full outer joins. Use left and reorder.
- `format` forced to tabular — JSON-record / dict shapes are not produced when `join` is present. CSV is supported (2026.05.1+).
