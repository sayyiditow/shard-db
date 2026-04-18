# Configuration

All runtime configuration lives in **`db.env`** (the sourced shell-env format), with tokens, allowed IPs, and tenant directories stored as plain-text files under `$DB_ROOT`.

## db.env

Placed in the working directory where you run shard-db (usually `build/bin/db.env`). Loaded once at startup.

| Variable | Default | Description |
|---|---|---|
| `DB_ROOT` | `./db` | Root directory for all data, indexes, metadata, and per-tenant subdirectories. |
| `PORT` | `9199` | TCP listen port. |
| `TIMEOUT` | `0` | Query timeout in **seconds**. `0` = disabled. Enforced via cooperative cancellation in scan loops. |
| `LOG_DIR` | `./logs` | Directory for async logs (`info-YYYY-MM-DD.log`, `error-YYYY-MM-DD.log`, `slow-YYYY-MM-DD.log`). |
| `LOG_LEVEL` | `3` | `0`=off · `1`=errors · `2`=warnings · `3`=info · `4`=debug. |
| `LOG_RETAIN_DAYS` | `7` | Auto-prune logs older than N days. `0` = keep forever. |
| `INDEX_PAGE_SIZE` | `4096` | B+ tree page size in bytes (power of 2, 1024–65536). |
| `THREADS` | `0` (auto) | Parallel scan thread count. `0` = number of online CPUs. |
| `WORKERS` | `0` (auto) | Server worker thread pool. `0` = auto (CPU count, minimum 4). |
| `GLOBAL_LIMIT` | `100000` | Soft cap on result rows per query — use per-query `limit` for tighter bounds. |
| `MAX_REQUEST_SIZE` | `33554432` (32 MB) | Maximum JSON request size per line. Oversized requests get `{"error":"Request too large (max N bytes)"}`. Every connection allocates a read buffer of this size. |
| `FCACHE_MAX` | `4096` | Unified shard-mmap cache capacity (entries). See [Tuning](../operations/tuning.md). |
| `BT_CACHE_MAX` | `256` | B+ tree mmap cache capacity (entries). |
| `SLOW_QUERY_MS` | `500` | Log queries slower than N ms to `slow-*.log` and the in-memory ring (`stats` endpoint). `0` = disable. Minimum 100 ms. |

Example:

```bash
# db.env
export DB_ROOT="../db"
export PORT=9199
export TIMEOUT=30
export LOG_DIR="../logs"
export LOG_LEVEL=3
export LOG_RETAIN_DAYS=14
export INDEX_PAGE_SIZE=4096
export THREADS=0
export WORKERS=0
export GLOBAL_LIMIT=100000
export MAX_REQUEST_SIZE=33554432
export FCACHE_MAX=4096
export BT_CACHE_MAX=256
export SLOW_QUERY_MS=500
```

Every variable is optional; defaults apply when the file or a specific export is missing. Changes require a server restart.

## Tenant directories — `dirs.conf`

Every data query must include a `dir` parameter (e.g., `"dir":"acme"`). That directory must be listed in `$DB_ROOT/dirs.conf` — one tenant name per line.

```text
# $DB_ROOT/dirs.conf
default
acme
engineering
```

The `default` dir is auto-registered on first use. Add tenants with a plain edit + server restart, or at runtime via `create-object` (which will create and register the tenant path automatically if it's new).

Queries for unregistered dirs return `{"error":"Unknown dir: <name>"}`.

See [Concepts → Multi-tenancy](../concepts/multi-tenancy.md) for the isolation model.

## API tokens — `tokens.conf`

`$DB_ROOT/tokens.conf` — one token per line. Any request with `"auth":"<token>"` that matches a line is accepted from any IP.

```text
# $DB_ROOT/tokens.conf
ajb9dsuf87sa8df7asdfasdf
another-service-account-token
```

Tokens are loaded at startup and refreshed when `add-token` / `remove-token` JSON modes are used (see [Operations → Deployment](../operations/deployment.md)).

## IP allowlist — `allowed_ips.conf`

`$DB_ROOT/allowed_ips.conf` — one IP per line. Requests from these IPs bypass the token check.

```text
# $DB_ROOT/allowed_ips.conf
127.0.0.1
::1
10.0.1.17
```

Localhost (`127.0.0.1` and `::1`) is trusted by default — you don't need to add it. Use the allowlist for sidecar processes or trusted services on other hosts.

## Precedence

A request is authenticated if **either**:

1. The client IP is on `allowed_ips.conf`, **or**
2. The request carries `"auth":"<token>"` and the token is in `tokens.conf`.

Both lists live under `$DB_ROOT` so they travel with the data root.

## Schema — `schema.conf` and per-object `fields.conf`

- `$DB_ROOT/schema.conf` — one line per object: `dir:object:splits:max_key:max_value:prealloc_mb`. Auto-managed by `create-object` — don't edit by hand.
- `$DB_ROOT/<dir>/<object>/fields.conf` — typed field definitions, one per line: `name:type[:size|P,S][:default=...]`. Also auto-managed (via `create-object`, `add-field`, `remove-field`, `rename-field`).

See [Concepts → Typed records](../concepts/typed-records.md) for the on-disk layout and all type definitions.

## Storage layout

```
$DB_ROOT/
  tokens.conf                      # API tokens
  allowed_ips.conf                 # Trusted IPs
  dirs.conf                        # Allowed tenant directories
  schema.conf                      # Object catalog
  <dir>/                           # Per-tenant directory
    <object>/
      fields.conf                  # Typed field schema
      metadata/
        counts                     # "<live> <deleted>\n"
        sequences/                 # Per-named-sequence counter files
      data/
        NNN.bin                    # Shard files (3 hex digits, max 0fff)
      indexes/
        index.conf                 # List of indexed fields
        <field>.idx                # B+ tree index file
        <a>+<b>.idx                # Composite index
      files/                       # Stored files (put-file)
        XX/XX/<filename>           # Hash-bucketed by filename
  logs/
    info-YYYY-MM-DD.log
    error-YYYY-MM-DD.log
    slow-YYYY-MM-DD.log
```

## Next

- [Operations → Deployment](../operations/deployment.md) — systemd unit, reverse proxy for TLS, log rotation.
- [Operations → Tuning](../operations/tuning.md) — when to raise `THREADS`, `FCACHE_MAX`, etc.
