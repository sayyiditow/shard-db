# Monitoring

What to watch and how to hook it into your existing stack.

## Built-in signals

shard-db exposes three layers of observability. All are query-driven — no dedicated metrics port.

### 1. `stats` — global snapshot

```bash
./shard-db stats
```

Returns (JSON):

```json
{
  "uptime_ms": 3612450,
  "connections": {"active": 4, "total": 18231},
  "in_flight_writes": 0,
  "ucache":    {"used": 128, "total": 4096, "hits": 1820391, "misses": 4102},
  "bt_cache":  {"used": 48,  "total": 256,  "hits": 923104,  "misses": 847},
  "slow_queries": [
    {"mode":"find","object":"orders","duration_ms":1347,"at":"20260418153012"}
  ]
}
```

Key metrics to scrape:

| Metric | Alert when |
|---|---|
| `uptime_ms` | Drops unexpectedly (process restart). |
| `connections.active` | Near `WORKERS` cap for extended periods. |
| `in_flight_writes` | Stays elevated (> 0) when traffic is idle — indicates stuck writes. |
| `ucache.hits / (hits + misses)` | Drops below 90% on a read-heavy workload — raise `FCACHE_MAX`. |
| `bt_cache.hits / (hits + misses)` | Drops below 90% on indexed queries — raise `BT_CACHE_MAX`. |
| `slow_queries[].duration_ms` | Any cross their SLO. |

### 2. Slow query log

Queries exceeding `SLOW_QUERY_MS` (default 500 ms, floor 100 ms):

- Last 64 kept in memory (visible in `stats`).
- Persisted to `$LOG_DIR/slow-YYYY-MM-DD.log` — one JSON object per line.

```bash
tail -f /opt/shard-db/logs/slow-$(date +%Y-%m-%d).log
```

Use a log-based alert (`jq` + your monitoring stack, or a log-shipper like Vector / Fluent Bit) to alert on duration or frequency spikes.

### 3. `shard-stats` — per-shard detail

```bash
./shard-db shard-stats <dir> <obj>
```

Per shard: slot count, active records, tombstoned, load factor. Useful for:

- Diagnosing skew (one shard with 2× the load of others).
- Verifying `vacuum --splits N` rebalanced correctly.
- Confirming a shard is about to grow (load > 50%).

## Log files

All logs live under `$LOG_DIR` (default `./logs/`).

| File | Content | Rotation |
|---|---|---|
| `info-YYYY-MM-DD.log` | Server start/stop, schema mutations, vacuum runs, connection accept/close at debug level. | Daily. Pruned after `LOG_RETAIN_DAYS` (default 7). |
| `error-YYYY-MM-DD.log` | Auth failures, malformed requests, write errors, crashes. | Daily. Same retention. |
| `slow-YYYY-MM-DD.log` | Queries exceeding `SLOW_QUERY_MS`. | Daily. Same retention. |

Set `LOG_RETAIN_DAYS=0` to disable auto-prune and use logrotate instead (see [Deployment → Log rotation](deployment.md#log-rotation)).

## Scraping into Prometheus

shard-db doesn't speak Prometheus natively. Two options:

### A. Textfile exporter

Scrape `stats` periodically and write it as a Prometheus textfile:

```bash
# /usr/local/bin/shard-db-metrics.sh
#!/bin/bash
STATS=$(echo '{"mode":"stats"}' | nc -q1 localhost 9199)
cat <<EOF > /var/lib/node_exporter/textfile/shard_db.prom.$$
shard_db_uptime_ms $(echo "$STATS" | jq .uptime_ms)
shard_db_connections_active $(echo "$STATS" | jq .connections.active)
shard_db_in_flight_writes $(echo "$STATS" | jq .in_flight_writes)
shard_db_ucache_hits $(echo "$STATS" | jq .ucache.hits)
shard_db_ucache_misses $(echo "$STATS" | jq .ucache.misses)
shard_db_bt_cache_hits $(echo "$STATS" | jq .bt_cache.hits)
shard_db_bt_cache_misses $(echo "$STATS" | jq .bt_cache.misses)
EOF
mv /var/lib/node_exporter/textfile/shard_db.prom.$$ /var/lib/node_exporter/textfile/shard_db.prom
```

Run via cron or systemd timer (every 30 s is ample). Node exporter with `--collector.textfile` picks it up.

### B. Dedicated exporter

Write a small Python / Go daemon that opens a long-lived connection, polls `stats`, and exposes `/metrics`. Slightly more work, cleaner.

## Alerting rules (starter set)

Adapt to your stack; these are the ones worth setting up first.

| Rule | Trigger | Severity |
|---|---|---|
| Server down | No `stats` response for 60 s | Critical |
| High write backlog | `in_flight_writes > 50` for 5 min | High |
| ucache miss rate | `ucache_miss_rate > 15%` for 10 min | Medium |
| btcache miss rate | `bt_cache_miss_rate > 15%` for 10 min | Medium |
| Slow query rate | More than 10 slow queries per minute | Medium |
| Disk full | `$DB_ROOT` filesystem > 85% | High |
| Vacuum needed | `vacuum-check` returns any object for N days | Low |

## Health check

For load balancers:

```bash
echo '{"mode":"db-dirs"}' | nc -w1 localhost 9199
```

Exits non-zero on connect failure. Any JSON back = server alive.

For a deeper liveness probe, parse `stats` and fail if `in_flight_writes > 100` for several consecutive checks.

## Tracing

No built-in distributed tracing. The slow-query log is the nearest thing. If you need per-request tracing:

- Wrap client calls to record start/end times and correlation IDs.
- Emit to your tracer (OpenTelemetry, Jaeger, whatever).
- Cross-reference with `slow-*.log` when investigating.

## Key questions to monitoring should answer

1. **Is the server alive?** — `db-dirs` probe + process monitoring.
2. **Are reads fast?** — cache hit rates, slow query log.
3. **Are writes flowing?** — `in_flight_writes`, write latency from client metrics.
4. **Is anyone hammering auth?** — count `error-*.log` lines with `auth failed`.
5. **Is disk filling?** — per-object `size` + filesystem usage.
6. **Are any objects overdue for maintenance?** — daily `vacuum-check`.
