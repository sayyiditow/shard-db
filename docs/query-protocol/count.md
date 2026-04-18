# count

Returns the number of records matching the criteria, without materializing any record values. Uses the same criteria shape as [`find`](find.md).

## Shape

```json
{"mode":"count","dir":"<dir>","object":"<obj>","criteria":[...]}
```

## Response

```json
{"count": 1234}
```

## Cost

- **Single-criterion indexed path** — uses `idx_count_cb`, the inline index-walk counter. Zero record fetches — O(1) per B+ tree hit. Fastest path.
- **Multi-criterion indexed** — primary index picks candidates, other criteria filter with `match_typed()`. Still fast because Zone B is read only for candidates that survive the primary index.
- **Full scan** — parallel per-shard Zone A walk. 2–3 ms on 1 M records.

`count` is always cheaper than `find` + ignoring results because it skips payload materialization and serialization.

## Empty criteria

```json
{"mode":"count","dir":"default","object":"users","criteria":[]}
```

Returns the total active record count (same as `size` without the orphan field). Uses the cached `metadata/counts` value — O(1).

## Examples

### Count by status

```json
// Request
{"mode":"count","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}]}

// Response
{"count": 8432}
```

### Range count

```json
{"mode":"count","dir":"default","object":"events",
 "criteria":[
   {"field":"severity","op":"gte","value":"3"},
   {"field":"created","op":"gte","value":"20260418000000"}
 ]}
```

### When to prefer `aggregate`

If you want multiple counts (by status, by region, etc.) in one trip, use [`aggregate`](aggregate.md) with `group_by` instead of N round trips.

```json
{"mode":"aggregate","dir":"acme","object":"orders",
 "group_by":["status"],
 "aggregates":[{"fn":"count","alias":"n"}]}
```
