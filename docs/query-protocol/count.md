# count

Returns the number of records matching the criteria, without materializing any record values. Uses the same criteria shape as [`find`](find.md).

## Shape

```json
{"mode":"count","dir":"<dir>","object":"<obj>","criteria":[...]}
```

## Response

A bare integer (no JSON wrapper):

```
1234
```

Errors still come back as `{"error":"..."}`.

## Cost

- **Single-criterion indexed path** — uses `idx_count_cb`, the inline index-walk counter. Zero record fetches — O(1) per B+ tree hit. Fastest path.
- **`neq` algebraic shortcut** — `count(neq X)` rewrites to `count(*) - count(eq X)`: two cheap counts instead of a near-everything scan. Same trick applies inside `aggregate`.
- **AND of indexed leaves** — `PRIMARY_INTERSECT` planner branch (2026.05+). Each leaf's btree walks into a `KeySet` (xxh128 hashes, lock-free CAS inserts), sets intersect, and the count is just `|result|` — **no per-record fetch**. Big win when the intersection is small. See [find → AND index intersection](find.md#and-index-intersection).
- **Mixed AND (indexed + non-indexed)** — primary index picks candidates, other criteria filter via the criteria tree (`criteria_match_tree`). Still fast because Zone B is read only for candidates that survive the primary index.
- **Pure-OR (all children indexed)** — B+ tree lookups unioned into a `KeySet`; count = `|KeySet|`. **No record fetch, no per-record match.**
- **Full scan** — parallel per-shard Zone A walk. 2–3 ms on 1 M records.

See [find → OR criteria](find.md) for the full planner table.

`count` is always cheaper than `find` + ignoring results because it skips payload materialization and serialization.

## Empty criteria

```json
{"mode":"count","dir":"default","object":"users","criteria":[]}
```

Returns the total active record count (same as `size`). Uses the cached `metadata/counts` value — O(1). For the deleted-but-not-vacuumed slot count, use the `orphaned` mode.

## Examples

### Count by status

```json
// Request
{"mode":"count","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}]}

// Response (bare integer)
8432
```

### Range count

```json
{"mode":"count","dir":"default","object":"events",
 "criteria":[
   {"field":"severity","op":"gte","value":"3"},
   {"field":"created","op":"gte","value":"20260418000000"}
 ]}
```

### CLI shortcut

```bash
./shard-db count <dir> <obj> [criteria_json]
```

Omit the criteria argument to get the O(1) metadata count. Criteria must be a JSON array in a single shell-quoted argument:

```bash
./shard-db count acme orders '[{"field":"status","op":"eq","value":"paid"}]'
```

### When to prefer `aggregate`

If you want multiple counts (by status, by region, etc.) in one trip, use [`aggregate`](aggregate.md) with `group_by` instead of N round trips.

```json
{"mode":"aggregate","dir":"acme","object":"orders",
 "group_by":["status"],
 "aggregates":[{"fn":"count","alias":"n"}]}
```
