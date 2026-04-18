# Diagnostics

Observability endpoints — inspect server health, cache behavior, shard load, slow queries, tenant allowlist.

## stats

Global server snapshot.

```json
{"mode":"stats"}
{"mode":"stats","format":"table"}
```

### Response (JSON)

```json
{
  "uptime_ms": 3612450,
  "connections": {"active": 4, "total": 18231},
  "in_flight_writes": 0,
  "ucache":    {"used": 128, "total": 4096, "bytes": 1073741824, "hits": 1820391, "misses": 4102},
  "bt_cache":  {"used": 48,  "total": 256,  "bytes": 2097152,    "hits": 923104,  "misses": 847},
  "slow_queries": [
    {"mode":"find","object":"orders","duration_ms":1347,"at":"20260418153012"},
    ...
  ]
}
```

### Response (table)

Human-readable block with the same numbers. Use `format:"table"` from the CLI; JSON from code.

### What to watch

- **`ucache` hit rate** — below 90% on a read-heavy workload = `FCACHE_MAX` too low.
- **`bt_cache` hit rate** — below 90% for indexed queries = `BT_CACHE_MAX` too low.
- **`in_flight_writes`** — should drain quickly. Sustained high = bottleneck (disk, lock contention).
- **`slow_queries` ring** — the last 64 queries exceeding `SLOW_QUERY_MS`. See also `slow-*.log` for history.

CLI:

```bash
./shard-db stats
```

Prints table format by default.

## shard-stats

Per-shard load factor and slot count.

```json
{"mode":"shard-stats","dir":"<dir>","object":"<obj>"}
{"mode":"shard-stats","dir":"<dir>","object":"<obj>","format":"table"}
{"mode":"shard-stats","dir":"<dir>"}              // all objects in dir
{"mode":"shard-stats"}                            // all objects everywhere
```

### Response

```json
{
  "dir":"default","object":"users","splits":16,
  "shards": [
    {"id":"000","slots":256,"active":110,"tombstoned":3,"load_pct":43},
    {"id":"001","slots":256,"active":134,"tombstoned":2,"load_pct":52},
    ...
  ]
}
```

### What to watch

- **Load factor over 50%** — about to grow (next write doubles `slots_per_shard`).
- **Large skew across shards** — hash isn't evenly distributing. Usually a sign of an adversarial key distribution; `vacuum --splits N` can rebalance by resharding.
- **High tombstoned %** — run `vacuum` to reclaim.

CLI:

```bash
./shard-db shard-stats default users
./shard-db shard-stats default
./shard-db shard-stats
```

## vacuum-check

Scan every object and flag ones that need `vacuum` — where tombstoned ≥10% AND total ≥1000.

```json
{"mode":"vacuum-check"}
```

### Response

```json
[
  {"dir":"acme","object":"orders","count":15203,"orphaned":2108,"vacuum":true},
  {"dir":"default","object":"users","count":1042,"orphaned":87,"vacuum":false}
]
```

- `vacuum: true` → candidate for `vacuum`.
- `vacuum: false` → below the threshold; no action needed.

CLI:

```bash
./shard-db vacuum-check
```

Use to build a nightly cron that vacuums objects flagged as needing it.

## db-dirs

List allowlisted tenant directories.

```json
{"mode":"db-dirs"}
```

Response: `["default","acme","engineering"]`.

CLI:

```bash
./shard-db db-dirs
```

## size

Active record count for a single object.

```json
{"mode":"size","dir":"<dir>","object":"<obj>"}
```

Response:

```json
{"count": 10000}
```

When there are tombstoned slots yet to be vacuumed, the response also includes `orphaned`:

```json
{"count": 10000, "orphaned": 342}
```

`count` uses the cached `metadata/counts` file (O(1)). `recount` does a full scan and rewrites it.

## Auth administration

Management operations. Callable only from trusted IPs (localhost or entries in `allowed_ips.conf`), or with a valid existing token.

### Tokens

```json
{"mode":"add-token","token":"<token-value>"}
{"mode":"remove-token","token":"<token-value>"}
{"mode":"list-tokens"}
```

- `list-tokens` returns tokens **masked** for safety: `["****...a8b9","****...c4d7"]`.
- Tokens persist to `$DB_ROOT/tokens.conf`.

### IP allowlist

```json
{"mode":"add-ip","ip":"192.168.1.10"}
{"mode":"remove-ip","ip":"192.168.1.10"}
{"mode":"list-ips"}
```

- `list-ips` returns the full list.
- IPs persist to `$DB_ROOT/allowed_ips.conf`.

### Security note

These admin endpoints inherit auth: anyone with a valid token (or from a trusted IP) can rotate tokens or add IPs. Gate sensitive environments by:

1. Limiting `allowed_ips.conf` to your management host only.
2. Using a TLS proxy + mTLS for remote admin.
3. Rotating the bootstrap token immediately after first use.

## Slow query log

Queries exceeding `SLOW_QUERY_MS` (default 500 ms, floor 100 ms) are logged to:

- **In-memory ring** — last 64, visible via `stats`.
- **On-disk** — `$LOG_DIR/slow-YYYY-MM-DD.log`, one line per slow query with timestamp, mode, object, duration, abridged criteria.

Disable with `SLOW_QUERY_MS=0`. Raise the floor if your baseline queries routinely cross 100 ms — otherwise the log drowns in noise.

## Log files

- `info-YYYY-MM-DD.log` — structured info events (server start/stop, schema mutations, vacuum runs).
- `error-YYYY-MM-DD.log` — errors (auth failures, malformed requests, write errors, crashes).
- `slow-YYYY-MM-DD.log` — slow query ring, persisted.

`LOG_RETAIN_DAYS` auto-prunes older files. `0` = keep forever.

## What's not here

- Historical metrics (time-series) — `stats` is a snapshot. Scrape it periodically and store yourself (Prometheus textfile exporter, InfluxDB, etc.).
- Alerting — no built-in. Pair with your existing stack.
- Per-query tracing — `slow-*.log` is the closest thing today.
