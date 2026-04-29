# shard-cli — ncurses TUI

A separate binary (`./shard-cli`) that connects to a running `shard-db` daemon over TCP+TLS. Composes existing JSON modes — the daemon is untouched. Built alongside `shard-db` by `./build.sh`, copied to `build/bin/shard-cli`.

## Connecting

`shard-cli` reads the same `db.env` as the daemon to find host/port/TLS settings. Source `db.env` before launching, or run from the same directory:

```bash
source db.env
./shard-cli
```

Honoured environment variables:

| Variable | Default | Used for |
|---|---|---|
| `HOST` | `127.0.0.1` | Daemon host |
| `PORT` | `9199` | Daemon port |
| `TOKEN` | (empty) | Auth token (any scope) |
| `TLS_ENABLE` | `0` | `1` to require TLS 1.3 |
| `TLS_CA` | (empty) | CA bundle for server-cert verify (defaults to OS trust store) |
| `TLS_SKIP_VERIFY` | `0` | `1` to skip server cert verify (dev only) |
| `TLS_SERVER_NAME` | `localhost` | SNI / verify-name override |

## Top-level menus

| Menu | Purpose |
|---|---|
| **Server** | Start / stop / status the daemon. Uses local `./shard-db` for lifecycle ops. |
| **Browse** | Pick tenant → object → describe schema. Surfaces fields, indexes, splits, max_key. |
| **Query** | Insert / get / find / count / exists with a 6-row × 3-column criteria builder. Field/op pickers; multi-value splitter for `IN`, `BETWEEN`, `LEN_BETWEEN`. |
| **Bulk** | Bulk-insert from JSON/CSV file path; bulk-update via JSON records or criteria; bulk-delete. |
| **Schema** | create-object (form), drop-object, add-field, remove-field (form), rename-field, add-index, remove-index. |
| **Maintenance** | vacuum (with `compact` toggle + new-`splits` field), recount, truncate (confirmation), backup. |
| **Files** | put-file / get-file / delete-file / list-files. Base64 round-trip; up to `MAX_REQUEST_SIZE / 4×3 ≈ 24 MB` per file at default settings. |
| **Auth** | list-tokens (fingerprint + scope + perm), add-token (with `dir`/`object`/`perm`), remove-token, list-ips, add-ip, remove-ip. |
| **Stats** | 5-second live refresh of the daemon `stats` output. Connections, in-flight writes, per-cache hit rate, slow-query log tail. |

## Table view

Every JSON response renders as a 2-column or N-column table depending on shape:

- **Object scalar** (`{"count":42}`) → 2-col `metric / value`.
- **Object with nested array/object value** (`{"shard_list":[...]}`) → 2-col, value cell shows `[N items, → drill]`. **Press Enter** to recurse into the nested JSON as a sub-table.
- **Array of objects** (`[{...},{...}]`) → auto-detects columns from each object's keys. Special-cases `"value"` to flatten the nested record into the row.
- **Array of strings** (`["k1","k2"]`) → single-column `value` table.
- **Tabular response** (`{"columns":[...],"rows":[[...]]}` from `find format=rows`) → uses the explicit columns.

## Keys

| Key | Action |
|---|---|
| `↑↓` / `jk` | Move cursor / scroll rows |
| `←→` / `hl` | Horizontal scroll for wide tables |
| `Enter` | Drill into a JSON-array/object cell (Shape 6 only) |
| `g` / `Home` | Top |
| `G` / `End` | Bottom |
| `Page Up` / `b` | Page up |
| `Page Down` / `Space` | Page down |
| `e` | Export current view as CSV (re-issues the underlying request with `format:"csv"` + writes to file) |
| `q` / `Esc` | Close current view / back |

## Caveats

- The daemon must already be running. `shard-cli` doesn't start it — use the **Server → start** menu or run `./shard-db start` directly.
- Heavy queries (full-scan aggregates, deep cursor pages) execute server-side and may take seconds; the UI blocks during that time.
- The TUI doesn't expose every JSON mode — for joins, CAS predicates, custom `format`, `having` clauses, etc., drop to `./shard-db query '<json>'` from a regular shell. The full surface area is in [Query protocol](../query-protocol/overview.md).
