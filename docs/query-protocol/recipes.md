# Query recipes

Real-world patterns stitched from the primitives in [find](find.md), [aggregate](aggregate.md), [joins](joins.md), [bulk](bulk.md), and [CAS](cas.md). Send any of these as the payload to `./shard-db query '<json>'` or over TCP.

## 1. Paginated filter with projection, sorted newest first

Return paid invoices from the last 30 days, newest first, 50 per page, only the fields a dashboard needs:

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

For the next page, bump `offset` by `limit`. `find` buffers all matches then sorts, so deep pagination over huge result sets pays `O(matches)`. For raw full-scan pagination over an entire object (no filter), prefer `fetch` with keyset `cursor`. For sorted pagination on an indexed `order_by` field, use the [find cursor](find.md#cursor-pagination) — O(limit) per page, scales to deep offsets.

## 2. Group-by aggregate with HAVING (revenue by product)

Top 10 products by revenue, excluding any product that sold fewer than 100 units:

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

Output is tabular (`{"columns":[...], "rows":[[...]]}`) — drop-in for spreadsheets or charting libraries.

## 3. Multi-join: enrich orders with user email and product title

A left-join on products (emit nulls if the SKU is missing), inner-join on users (drop orders without a known user):

```json
{"mode":"find","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"users","local":"user_id","remote":"key",
    "as":"user","type":"inner","fields":["email","name"]},
   {"object":"products","local":"product_sku","remote":"sku",
    "as":"product","type":"left","fields":["title","price"]}
 ],
 "limit":100}
```

`remote` is either `"key"` (primary-key lookup) or any **indexed** field on the joined object. Output columns: `orders.key`, `orders.{field}`, `user.{field}`, `product.{field}`.

## 4. Safe bulk update with dry-run first

Mark all `pending` orders older than 7 days as `expired`. Dry-run first to see the blast radius, then run for real with a `limit` guard:

```json
// Dry run — returns the would-be count without writing
{"mode":"bulk-update","dir":"acme","object":"orders",
 "criteria":[
   {"field":"status","op":"eq","value":"pending"},
   {"field":"created","op":"lt","value":"20260410000000"}
 ],
 "value":{"status":"expired"},
 "limit":10000,
 "dry_run":true}
```

```json
// Execute (drop dry_run, same criteria)
{"mode":"bulk-update","dir":"acme","object":"orders",
 "criteria":[
   {"field":"status","op":"eq","value":"pending"},
   {"field":"created","op":"lt","value":"20260410000000"}
 ],
 "value":{"status":"expired"},
 "limit":10000}
```

Use CAS (`"if":{"version":42}`) on single-record updates for lock-free concurrency control. Combine with `auto_update` on a `version:int` field to track revisions.
