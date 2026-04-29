# shard-cli — ncurses TUI

A separate binary (`./shard-cli`) that connects to a running `shard-db` daemon over TCP+TLS. Composes existing JSON modes — the daemon is untouched. Built alongside `shard-db` by `./build.sh`, copied to `build/bin/shard-cli`.

## Why it exists

`./shard-db query '<json>'` is fine for one-shot ad-hoc work, but it's painful for:

- Browsing a tenant's catalog when you don't remember object names.
- Building criteria with the right operators for the right field types.
- Running a `count` against five different filters back-to-back.
- Watching `stats` refresh in real time during a load.
- Issuing tokens with the right scope+perm combination without retyping JSON.

`shard-cli` covers the same surface area as the JSON protocol but with menu-driven pickers, form-based input, and live tables. For surfaces it doesn't expose (joins, CAS predicates, `having` clauses), drop to `./shard-db query` from a regular shell.

## Launching

`shard-cli` reads the same `db.env` as the daemon to find host/port/TLS settings. Two equivalent launch styles:

```bash
# 1. Source db.env first, then run from anywhere
source build/bin/db.env
./shard-cli

# 2. Or run from the same directory as db.env (build/bin/)
cd build/bin
./shard-cli
```

You can also override anything from the command line:

```bash
shard-cli --host db.internal --port 9199 --token "$ADMIN_TOKEN"
shard-cli -t "$READ_ONLY_TOKEN"
shard-cli --help
```

| Flag | Equivalent env | Default |
|---|---|---|
| `--host H` | `HOST` | `127.0.0.1` |
| `--port P` | `PORT` | `9199` |
| `--token T` / `-t T` | `TOKEN` | (empty — relies on trusted-IP localhost) |
| `--help` / `-h` | — | Prints usage and exits |

### Honoured environment variables

| Variable | Default | Used for |
|---|---|---|
| `HOST` | `127.0.0.1` | Daemon host |
| `PORT` | `9199` | Daemon port |
| `TOKEN` | (empty) | Auth token (any scope) |
| `TLS_ENABLE` | `0` | `1` to require TLS 1.3 |
| `TLS_CA` | (empty) | CA bundle for server-cert verify (defaults to OS trust store) |
| `TLS_SKIP_VERIFY` | `0` | `1` to skip server cert verify (dev only) |
| `TLS_SERVER_NAME` | `localhost` | SNI / verify-name override |

The status bar shows the live connection details (`connected to host:port  tls=on/off`).

## Top-level menus

13 menus on the main screen (cursor with `↑↓` or `jk`, Enter to open, `q` / `Esc` to back out):

| Menu | Purpose |
|---|---|
| **Server** | start / stop / status. Wraps `./shard-db` lifecycle commands. |
| **Browse** | Pick tenant → object → describe. Surfaces fields, indexes, splits, max_key, live count. |
| **Query** | insert / get / update / delete / find / count / aggregate / keys / fetch / exists. Includes the criteria builder (see below). |
| **Bulk** | bulk-insert (JSON or CSV file path) / bulk-update (per-key JSON or criteria) / bulk-delete (key list or criteria). |
| **Schema** | create-object (form) / drop-object / add-field / remove-field / rename-field / add-index / remove-index / reindex. |
| **Maintenance** | vacuum (with `compact` toggle + new-`splits` field) / recount / truncate (confirmation gate) / backup. |
| **Tenants** | list / add / remove tenant directories (`add-dir` / `remove-dir`). |
| **Auth** | list-tokens (fingerprint + scope + perm) / add-token (with `dir` / `object` / `perm`) / remove-token (by token or fingerprint) / list-ips / add-ip / remove-ip. |
| **Files** | put-file / get-file / delete-file / list-files. Base64 round-trip; up to `MAX_REQUEST_SIZE / 1.33` ≈ 24 MB per file at default settings. |
| **Migrate** | export-schema / import-schema — bootstrap another DB with the same schemas (no data). |
| **Diagnostics** | shard-stats / vacuum-check / size. |
| **Stats** | 5-second live refresh of `stats`. Connections, in-flight writes, per-cache hit rate, slow-query log tail. |
| **Quit** | Close the TUI. |

## Criteria builder

The query menu's `find` / `count` / `aggregate` paths open a 6-row × 3-column form:

| Column | Picker / input |
|---|---|
| Field | drop-down sourced from `describe-object` — already-typed schema |
| Operator | drop-down filtered by field type (e.g. `lt`/`gt`/`between` only show on numeric, regex on varchar, `eq`/`neq` everywhere) |
| Value | free-form text; multi-value splitter for `in`, `between`, `len_between` (comma-separated) |

Up to 6 rows, AND-combined. For OR criteria or deeper trees (depth up to 16), use `./shard-db query '<json>'` directly — the TUI's grid form caps at 6 rows of AND.

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
| `Enter` | Drill into a JSON-array/object cell |
| `g` / `Home` | Top |
| `G` / `End` | Bottom |
| `Page Up` / `b` | Page up |
| `Page Down` / `Space` | Page down |
| `e` | Export current view as CSV (re-issues the underlying request with `format:"csv"` + writes to file) |
| `q` / `Esc` | Close current view / back |

## A first session — end-to-end

The fastest way to get oriented. Assumes the daemon is running (`./shard-db start` or **Server → start** from inside the TUI).

1. **Browse → default → (no objects yet)** — confirms you're connected and the `default` tenant exists.
2. **Schema → create-object** — fill in the form:
   - Object: `users`
   - Splits: `16` (default; min)
   - Max key: `64`
   - Fields (one per row): `name:varchar:100`, `email:varchar:200`, `age:int`
   - Indexes: `email`, `age`
3. **Browse → default → users** — describe-object now shows the schema you just built.
4. **Query → insert** — pick `users`, type key `u1`, fill the `value` form (pulls field names from describe-object).
5. **Query → find** — pick `users`, open the criteria builder, add `age gt 18`, run. Results render as a table; press `e` to export to CSV.
6. **Stats** — open and watch `ucache.hits` tick up as you re-query.
7. **Auth → add-token** — issue a tenant-scoped read-only token (`dir=default`, `perm=r`) for a downstream service.
8. **Maintenance → backup** — snapshot the object before further work.

## Caveats

- The daemon must already be running. `shard-cli` doesn't start it automatically — use **Server → start** or run `./shard-db start` directly.
- Heavy queries (full-scan aggregates, deep cursor pages) execute server-side and may take seconds; the UI blocks during that time.
- The TUI doesn't expose every JSON mode. Specifically: `join`, CAS predicates (`if:[...]`), `having`, custom `format`, `cursor` pagination, regex operators. For those, drop to `./shard-db query '<json>'` from a regular shell. The full surface area is in [Query protocol](../query-protocol/overview.md).
- Very wide tables truncate visually but you can scroll horizontally with `←→`. CSV export (`e`) preserves all columns regardless of terminal width.

## Related

- [CLI reference](index.md) — the JSON-mode `shard-db` binary, every subcommand listed.
- [Query protocol overview](../query-protocol/overview.md) — the JSON modes the TUI is composed from.
- [Configuration](../getting-started/configuration.md) — `db.env` reference, the same file `shard-cli` sources for connection settings.
