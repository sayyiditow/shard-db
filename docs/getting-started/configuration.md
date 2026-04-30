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
| `THREADS` | `0` (auto) | Parallel-for worker pool — drives every parallel hot path (shard scans, indexed find/count/aggregate fan-out, parallel index builds, bulk-insert phase 2). `0` = `4 × nproc`, minimum 4. |
| `WORKERS` | `0` (auto) | Server-worker pool that accepts connections + dispatches request handlers. `0` = auto (CPU count, minimum 4). |
| `POOL_CHUNK` | `0` (auto) | parallel_for submission chunk size. `0` = `nproc`. Tasks are enqueued in chunks of this many; larger chunks reduce queue-lock contention but serialise concurrent submitters. Rarely needs tuning. |
| `GLOBAL_LIMIT` | `100000` | Default `limit` applied when a query omits one. Per-query `limit` is **not clamped** — pass any value to override. |
| `MAX_REQUEST_SIZE` | `33554432` (32 MB) | Maximum JSON request size per line. Oversized requests get `{"error":"Request too large (max N bytes)"}`. Every connection allocates a read buffer of this size, so total per-conn memory = N × `MAX_REQUEST_SIZE`. |
| `FCACHE_MAX` | `4096` | Unified shard-mmap cache capacity (entries). **Strict allow-list:** `{4096, 8192, 12288, 16384}`. Invalid values fall back to default with a warning. See [Tuning](../operations/tuning.md). |
| `BT_CACHE_MAX` | derived | **Not configurable as of 2026.05.1.** Derived as `FCACHE_MAX / 4` (so `{1024, 2048, 3072, 4096}`). Setting it in db.env emits a stderr warning and is ignored. |
| `QUERY_BUFFER_MB` | `500` | Per-query intermediate buffer cap. Protects the daemon from one bad query monopolising RAM. The collect-hash buffer is a single mmap MAP_NORESERVE reservation up to this cap; pages lazy-commit on write. |
| `DISABLE_LOCALHOST_TRUST` | `0` | Default: 127.0.0.1/::1 bypasses auth (assumes a trusted loopback proxy). Set to `1` for strict mode (tokens required even same-host). |
| `TOKEN_CAP` | `1024` | Open-addressed bucket count for the token store. Bump to 4096+ if you run thousands of tokens across scopes. |
| `SLOW_QUERY_MS` | `500` | Log queries slower than N ms to `slow-*.log` and the in-memory ring (`stats` endpoint). `0` = disable. Minimum 100 ms. |
| `TLS_ENABLE` | `0` | `1` = require TLS 1.3 on `PORT`; plaintext clients rejected at handshake. See [Operations → Deployment → Native TLS](../operations/deployment.md). |
| `TLS_CERT` / `TLS_KEY` | (empty) | Server cert + private key paths (PEM). Required when `TLS_ENABLE=1`. |
| `TLS_CA` | (empty) | Client-side CA bundle for verifying the server (defaults to OS trust store). |
| `TLS_SKIP_VERIFY` | `0` | Client-side: `1` skips server cert verify (dev only — emits stderr warning). |

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
# BT_CACHE_MAX is no longer configurable — derived as FCACHE_MAX / 4
export QUERY_BUFFER_MB=500
export TOKEN_CAP=1024
export DISABLE_LOCALHOST_TRUST=0
export SLOW_QUERY_MS=500

# Native TLS — leave TLS_ENABLE=0 unless terminating TLS in-process
export TLS_ENABLE=0
export TLS_CERT=""
export TLS_KEY=""
export TLS_CA=""
export TLS_SKIP_VERIFY=0
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
        index.conf                          # List of indexed fields
        <field>/                            # Per-field directory (per-shard btree layout)
          NNN.idx                           #   Sharded B+ tree files, splits/4 of them
        <a>+<b>/                            # Composite index — '+' joined name
          NNN.idx
      files/                       # Stored files (put-file)
        XX/XX/<filename>           # Hash-bucketed by filename
  logs/
    info-YYYY-MM-DD.log
    error-YYYY-MM-DD.log
    slow-YYYY-MM-DD.log
```

Each indexed field is split into `splits/4` btree files (e.g., `splits=64` → 16 idx-shards under `indexes/<field>/000.idx`..`00f.idx`). Writes route by record hash to a single shard; reads fan out across all shards in parallel.

## Next

- [Operations → Deployment](../operations/deployment.md) — systemd unit, reverse proxy for TLS, log rotation.
- [Operations → Tuning](../operations/tuning.md) — when to raise `THREADS`, `FCACHE_MAX`, etc.
