# Diagnostics

Observability endpoints — inspect server health, cache behavior, shard load, slow queries, tenant allowlist, schema catalog, file inventory.

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
  "bt_cache":  {"used": 48,  "total": 1024, "bytes": 2097152,    "hits": 923104,  "misses": 847},
  "slow_queries": [
    {"mode":"find","object":"orders","duration_ms":1347,"at":"20260418153012"}
  ]
}
```

`bt_cache.total` is **derived** as `FCACHE_MAX / 4` since 2026.05.1 — it is no longer configurable. Setting `BT_CACHE_MAX` in db.env emits a stderr warning and is ignored.

### Response (table)

Human-readable block with the same numbers. Use `format:"table"` from the CLI; JSON from code.

### What to watch

- **`ucache` hit rate** — below 90% on a read-heavy workload = `FCACHE_MAX` too low.
- **`bt_cache` hit rate** — below 90% for indexed queries = `FCACHE_MAX` too low (raises bt_cache too, since it's derived).
- **`in_flight_writes`** — should drain quickly. Sustained high = bottleneck (disk, lock contention).
- **`slow_queries` ring** — the last 64 queries exceeding `SLOW_QUERY_MS`. See also `slow-*.log` for history.

CLI:

```bash
./shard-db stats
```

Prints table format by default.

## stats-prom

Same counters as `stats`, but rendered as **Prometheus text-format exposition**. Drop straight into your scrape pipeline — no shim, no jq.

```json
{"mode":"stats-prom"}
```

### Response

```
# HELP shard_db_uptime_seconds Time since server start.
# TYPE shard_db_uptime_seconds gauge
shard_db_uptime_seconds 3612.450
# HELP shard_db_active_threads Worker threads currently servicing requests.
# TYPE shard_db_active_threads gauge
shard_db_active_threads 3
# HELP shard_db_in_flight_writes Write/schema requests currently executing.
# TYPE shard_db_in_flight_writes gauge
shard_db_in_flight_writes 0
shard_db_ucache_used 128
shard_db_ucache_capacity 4096
shard_db_ucache_bytes 1073741824
shard_db_ucache_hits_total 1820391
shard_db_ucache_misses_total 4102
shard_db_bt_cache_used 48
shard_db_bt_cache_capacity 1024
shard_db_bt_cache_bytes 2097152
shard_db_bt_cache_hits_total 923104
shard_db_bt_cache_misses_total 847
shard_db_slow_query_threshold_milliseconds 500
shard_db_slow_query_total 17
```

CLI:

```bash
./shard-db stats-prom
```

Wire it into Prometheus with a small textfile-exporter cron, or expose `/metrics` from a thin sidecar that runs `stats-prom` on each scrape. See [Operations → Monitoring](../operations/monitoring.md).

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
    {"id":"001","slots":256,"active":134,"tombstoned":2,"load_pct":52}
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

## list-objects

Enumerate every object inside a tenant directory.

```json
{"mode":"list-objects","dir":"acme"}
```

Response:

```json
["customers","invoices","products"]
```

The result is alphabetical and reflects on-disk schema (any directory under `<dir>/` containing a `fields.conf`). Used by `shard-cli` to populate the object-picker; reach for it any time you need a programmatic catalog walk.

## describe-object

Read-only schema + index + count snapshot for a single object. No locks taken.

```json
{"mode":"describe-object","dir":"acme","object":"invoices"}
```

Response:

```json
{
  "dir": "acme",
  "object": "invoices",
  "splits": 64,
  "max_key": 128,
  "value_size": 412,
  "fields": [
    {"name":"customer","type":"varchar","size":80},
    {"name":"amount","type":"numeric","precision":12,"scale":2},
    {"name":"status","type":"varchar","size":16},
    {"name":"created","type":"datetime"}
  ],
  "indexes": ["customer","status","status+created"],
  "counts": {"live": 152031, "tombstoned": 412}
}
```

Tombstoned fields are reported with a `removed:true` flag so the caller can distinguish them from live columns.

## list-files

Paginated, alphabetical inventory of stored files for one object. Optional `prefix` filter.

```json
{"mode":"list-files","dir":"acme","object":"invoices","prefix":"2026-","offset":0,"limit":100}
```

Response:

```json
{
  "files": ["2026-01-summary.pdf","2026-02-summary.pdf", ...],
  "total": 245,
  "offset": 0,
  "limit": 100
}
```

`total` is the unpaginated match count (after `prefix` filtering). Walking the `XX/XX` bucket tree is O(file count) — fine up to ~1M files; beyond that, use a separate index.

## size

Active record count for a single object.

```json
{"mode":"size","dir":"<dir>","object":"<obj>"}
```

Response:

```json
{"count": 10000}
```

When there are tombstoned slots yet to be vacuumed:

```json
{"count": 10000, "orphaned": 342}
```

`count` uses the cached `metadata/counts` file (O(1)). `recount` does a full scan and rewrites it.

## Auth administration

Token + IP management. Always **server-admin scope** — callable only from a trusted IP or with a global `rwx` token, regardless of which scope the token being managed has.

### Tokens

```json
{"mode":"add-token","token":"<value>"}                                    // global rw (default)
{"mode":"add-token","token":"<value>","perm":"r"}                         // global read-only
{"mode":"add-token","token":"<value>","dir":"acme","perm":"rwx"}          // tenant admin
{"mode":"add-token","token":"<value>","dir":"acme","object":"invoices"}   // per-object rw
{"mode":"remove-token","token":"<value>"}
{"mode":"remove-token","fingerprint":"abcd...wxyz"}                       // remove by fingerprint
{"mode":"list-tokens"}
```

Default `perm` is `rw` (least-privilege — admins opt into `rwx`). Valid values: `r`, `rw`, `rwx`. `object` scope requires `dir`.

`list-tokens` returns one entry per token:

```json
[
  {"token":"abcd...wxyz","scope":"global","perm":"rwx"},
  {"token":"e123...4567","scope":"acme","perm":"rw"},
  {"token":"f456...8901","scope":"acme/invoices","perm":"r"}
]
```

Full tokens are never echoed — just a 4-char-prefix-and-suffix fingerprint. Pass that fingerprint back as `fingerprint` in `remove-token` if you don't have the original token value.

See [Concepts → Multi-tenancy](../concepts/multi-tenancy.md) for the scope + permission model.

### Tenant directories

```json
{"mode":"add-dir","dir":"newtenant"}
{"mode":"remove-dir","dir":"oldtenant","check_empty":true}
```

`add-dir` creates the directory under `$DB_ROOT` and appends to `dirs.conf`. `remove-dir` defaults to refusing if the directory still has objects; pass `"check_empty":false` to force.

### IP allowlist

```json
{"mode":"add-ip","ip":"192.168.1.10"}
{"mode":"remove-ip","ip":"192.168.1.10"}
{"mode":"list-ips"}
```

IPs persist to `$DB_ROOT/allowed_ips.conf`. Localhost (`127.0.0.1`, `::1`) is trusted by default — set `DISABLE_LOCALHOST_TRUST=1` in db.env for strict mode.

### Security note

Token-management endpoints inherit auth: anyone with a global `rwx` token (or from a trusted IP) can rotate tokens or add IPs. Gate sensitive environments by:

1. Limiting `allowed_ips.conf` to your management host only.
2. Enabling native TLS (`TLS_ENABLE=1`) or fronting with a TLS proxy.
3. Issuing operator credentials as global `rwx` and downstream credentials as tenant- or object-scoped `r`/`rw`.

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

- Historical metrics (time-series) — `stats` / `stats-prom` are snapshots. Scrape periodically into Prometheus, InfluxDB, etc.
- Alerting — no built-in. Pair with your existing stack.
- Per-query tracing — `slow-*.log` is the closest thing today.
