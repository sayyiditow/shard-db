# CLI reference

Every `./shard-db <cmd>` subcommand in one place. Shortcuts for the common operations — for full expressiveness see the [Query protocol](../query-protocol/overview.md).

All data-plane commands (anything besides `start`/`stop`/`status`/`server`) talk to the running server over TCP at `$PORT`. Start the server first.

> **Looking for a TUI?** [`shard-cli`](shard-cli.md) is a separate ncurses binary built alongside `shard-db` — full menu-driven access to every JSON mode (browse / query / schema / maintenance / auth / stats), no JSON typing required.

## Lifecycle

| Command | Args | Description |
|---|---|---|
| `start` | — | Start the server in the background. Reads `PORT`, `DB_ROOT`, etc. from `db.env`. Writes a pidfile. |
| `server` | — | Start in the foreground (no fork). Useful for systemd `Type=simple` or debugging. |
| `stop` | — | Graceful shutdown. Refuses new connections, waits up to 30 s for in-flight writes to drain. |
| `status` | — | Print `running (pid=..., port=...)` or `stopped`. Exit code reflects status. |

## CRUD

| Command | Args | Description |
|---|---|---|
| `insert` | `<dir> <obj> <key> '<json_value>'` | Insert (or overwrite) a record. Set `if_not_exists` via JSON mode. |
| `get` | `<dir> <obj> <key>` | Retrieve a single record. |
| `delete` | `<dir> <obj> <key>` | Delete a record by key. |
| `exists` | `<dir> <obj> <key>` | Returns bare `true` / `false`. (Multi-key form keeps the `{key:bool}` dict shape.) |
| `size` | `<dir> <obj>` | Bare integer — live record count, O(1) metadata read. Use `orphaned` for the tombstoned count. |

## Query

| Command | Args | Description |
|---|---|---|
| `find` | `<dir> <obj> '<criteria_json>' [offset] [limit] [fields]` | Search records. For `join`, `order_by`, `format`, etc. use [JSON mode](../query-protocol/find.md). |
| `count` | `<dir> <obj> [criteria_json]` | Count matching records. Omit criteria for the O(1) metadata count. |
| `aggregate` | `<dir> <obj> '<aggregates_json>' [group_by_csv] [criteria_json] [having_json]` | Group + aggregate. `group_by` is a comma-separated field list; leave intermediate slots empty (`''`) to skip them. |
| `keys` | `<dir> <obj> [offset] [limit]` | List keys, paginated. |
| `fetch` | `<dir> <obj> [offset] [limit] [fields]` | Paginated full scan with optional field projection. Use JSON mode for keyset `cursor` pagination. |

## Bulk

| Command | Args | Description |
|---|---|---|
| `bulk-insert` | `<dir> <obj> [file]` | JSON array of `{"id":"<key>","data":{...}}` objects. File path or stdin. |
| `bulk-delete` | `<dir> <obj> [file]` | JSON array of keys (e.g. `["k1","k2"]`). For criteria-based deletes, use JSON mode. |

## File storage

| Command | Args | Description |
|---|---|---|
| `put-file` | `<dir> <obj> <local-path> [--if-not-exists]` | Upload local file to server (base64 over TCP). `--if-not-exists` refuses overwrite. |
| `get-file` | `<dir> <obj> <filename> [<out-path>]` | Download by filename (base64 over TCP). Writes to `<out-path>` or stdout. |
| `delete-file` | `<dir> <obj> <filename>` | Remove a stored file. Returns `{"status":"deleted",...}` or `{"error":"file not found",...}`. |
| `list-files` | `<dir> <obj> [pattern] [offset] [limit] [--match=<mode>]` | Alphabetical paginated listing. `--match=prefix` (default), `suffix`, `contains`, or `glob` (`fnmatch(3)`: `*`, `?`, `[abc]`). Empty pattern matches all. `limit` defaults to `GLOBAL_LIMIT` when 0/omitted. Returns `{"files":[...],"total":N,"offset":N,"limit":N}`. |

Size bounded by `MAX_REQUEST_SIZE` (default 32 MB ⇒ ~24 MB effective file). For same-host admin tasks, [JSON mode `put-file` with `path`](../query-protocol/files.md) and `get-file-path` skip the base64 roundtrip.

## Index management

| Command | Args | Description |
|---|---|---|
| `add-index` | `<dir> <obj> <field> [-f]` | Build a B+ tree index on a single field. `-f` forces rebuild. Composite via `field1+field2`. Batch builds via [JSON mode](../query-protocol/index-management.md). |
| `remove-index` | `<dir> <obj> <field>` | Drop the index (exact name match, including composites). Safe on non-existent index. |

## Maintenance

| Command | Args | Description |
|---|---|---|
| `vacuum` | `<dir> <obj>` | Fast in-place tombstone reclaim (no schema changes). For compact/reshard, use [JSON mode](../query-protocol/schema-mutations.md). |
| `recount` | `<dir> <obj>` | Rescans shards and rewrites the cached `counts` file. |
| `truncate` | `<dir> <obj>` | Delete all records. Schema + indexes survive. |
| `backup` | `<dir> <obj>` | Copy the object's data + metadata + indexes to a timestamped backup directory. |

## Diagnostics

| Command | Args | Description |
|---|---|---|
| `stats` | — | Global snapshot: uptime, active connections, in-flight writes, cache hit rates, recent slow queries. Prints as table. |
| `stats-prom` | — | Same counters as `stats`, rendered as Prometheus text-format exposition. Wire to your scrape pipeline. |
| `shard-stats` | `[dir] [obj]` | Per-shard load table. Without args, shows all objects; with one arg, all objects in that dir; with both, just that object. |
| `db-dirs` | — | List registered tenant directories (from `dirs.conf`). |
| `vacuum-check` | — | List objects where tombstoned ≥ 10 % AND live ≥ 1000. Suggests candidates for `vacuum`. |
| `reindex` | `[dir] [obj]` | Rebuild indexes — wipes per-field idx directories and rebuilds at the current `splits/4` shard count. No args = all tenants. |
| `orphaned` | `<dir> <obj>` | Bare integer count of tombstoned-but-not-vacuumed slots. O(1) metadata read. New in 2026.05.1. |
| `(./migrate)` | — | Per-release one-shot upgrade runner — separate binary at `build/bin/migrate`. Stops the daemon, runs every required migration for the release, restarts the daemon, exits. For 2026.05.1: `migrate-files` (lift pre-2026.05.2 `XX/XX/<filename>` to flat) + `reindex` (per-shard btree rebuild). |

## JSON query mode

For anything not covered by the shortcut commands (multi-get, aggregates, joins, CAS, schema mutations, bulk-update, file-path variants, etc.):

```bash
./shard-db query '<json>'
```

See [Query protocol](../query-protocol/overview.md) for every `mode` and its parameters.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Error — usage problem, connection failure, or server returned `{"error":...}`. |

Error responses are printed verbatim to stdout as JSON, so you can pipe into `jq` or parse in scripts.

## Examples

```bash
# Multi-get via JSON mode
./shard-db query '{"mode":"get","dir":"default","object":"users","keys":["u1","u2","u3"]}'

# Aggregate (count by status)
./shard-db query '{
  "mode":"aggregate","dir":"default","object":"orders",
  "group_by":["status"],
  "aggregates":[{"fn":"count","alias":"n"}]
}'

# CAS update
./shard-db query '{
  "mode":"update","dir":"default","object":"orders","key":"o1",
  "value":{"status":"paid"},
  "if":[{"field":"status","op":"eq","value":"pending"}]
}'

# Dry-run bulk delete
./shard-db query '{
  "mode":"bulk-delete","dir":"default","object":"orders",
  "criteria":[{"field":"status","op":"eq","value":"cancelled"}],
  "dry_run":true
}'
```
