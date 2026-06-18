# explain

Return the query plan without executing. Supported on `find`, `count`, and `aggregate` modes.

## Usage

Add `"explain":true` to any find/count/aggregate request:

```json
{"mode":"find","dir":"default","object":"users","criteria":[{"field":"score","op":"gt","value":"50"}],"explain":true}
```

## Response shape

```json
{
  "plan": "leaf",
  "order": "sort",
  "total_cheap": false,
  "table_rows": 5700000,
  "source": [
    {"field":"score","op":"gt","index":"btree","role":"seed","estimated_rows":48200}
  ],
  "postfilter": [
    {"field":"title","op":"contains","index":null,"role":"postfilter","estimated_rows":null}
  ],
  "hints": [
    {"type":"add_index","field":"title","reason":"unindexed field in postfilter; index avoids full record scan"},
    {"type":"composite_index","field":"score+created_at","reason":"filter on score + order_by created_at; composite index eliminates in-memory sort"}
  ]
}
```

## Fields

| Field | Type | Meaning |
|---|---|---|
| `plan` | string | Strategy: `leaf` (single indexed leaf), `scan` (full shard scan), `bitmap` (bitmap index), `intersect` (AND of indexed leaves), `union` (OR of indexed leaves) |
| `order` | string | Ordering strategy: `none`, `composite`, `composite_exact`, `sort` (in-memory sort), `index_walk` |
| `total_cheap` | bool | True if total count is O(1) from KeySet |
| `table_rows` | int | Live record count |
| `source` | array | Indexed criteria driving the plan (seed leaves) |
| `postfilter` | array | Criteria that require per-record filtering |
| `hints` | array | Optimization suggestions |

### Source entries

| Field | Meaning |
|---|---|
| `field` | Field name |
| `op` | Operator (`eq`, `lt`, `gt`, `lte`, `gte`, `like`, `contains`, `starts`, `between`, `in`, `exists`) |
| `index` | Index type (`btree`, `bitmap`, or `none`) |
| `role` | Always `seed` for source entries |
| `estimated_rows` | Estimated match count from index statistics |

### Postfilter entries

| Field | Meaning |
|---|---|
| `field` | Field name |
| `op` | Operator |
| `index` | Index type or `null` if unindexed |
| `role` | Always `postfilter` |
| `estimated_rows` | Always `null` (not estimable for postfilters) |

### Hints

Emitted when `table_rows >= 100000` (except `add_trigram_index` which is always emitted):

| Type | Meaning |
|---|---|
| `add_index` | Unindexed field in postfilter; btree index would avoid full record scan |
| `add_trigram_index` | Varchar text-search op on unindexed/non-trigram field; trigram index enables substring matching |
| `composite_index` | Filter + order_by on different fields; composite index avoids in-memory sort |

## CLI

```bash
shard-db explain find  <dir> <obj> '<criteria>' [order_by]
shard-db explain count <dir> <obj> '<criteria>'
shard-db explain aggregate <dir> <obj> '<criteria>' [order_by]
```

Examples:

```bash
shard-db explain find default users '[{"field":"score","op":"gt","value":"50"}]'
shard-db explain count default users '[{"field":"active","op":"eq","value":"true"}]'
shard-db explain aggregate default users '[{"field":"status","op":"eq","value":"active"}]' created_at
```
