<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-lockup-dark.svg">
    <img src="docs/assets/logo-lockup.svg" alt="shard-db" width="360">
  </picture>
</p>

[![CI](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml)
[![Docs](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/sayyiditow/shard-db/badge)](https://scorecard.dev/viewer/?uri=github.com/sayyiditow/shard-db)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12704/badge)](https://www.bestpractices.dev/projects/12704)
[![Coverity Scan](https://scan.coverity.com/projects/33072/badge.svg?v=2026-05-05)](https://scan.coverity.com/projects/sayyiditow-shard-db)
[![codecov](https://codecov.io/gh/sayyiditow/shard-db/branch/main/graph/badge.svg)](https://codecov.io/gh/sayyiditow/shard-db)

A high-performance database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, and a multi-threaded TCP server with optional native TLS 1.3.

**Platform:** Linux x86_64 / ARM64 + macOS (Apple Silicon). License: **MIT**.

## In production

**<a href="https://hn.shard-db.dev" target="_blank">HN Explorer</a>** — 30M+ Hacker News stories, comments, and users served from a single shard-db instance on an 8-core Netcup VPS. Browse by category, search with full-text, paginate deep into the archive at constant latency. [Source](https://github.com/sayyiditow/shard-db-hn-explorer) · [Starter template](https://github.com/sayyiditow/shard-db-svelte-starter) (SvelteKit).

## Highlights

| Area | Highlights |
|---|---|
| **Throughput** | ~5M / ~8.4M K/V ops/sec single-thread / 5-conn parallel bulk insert (CSV, 10M records). 5.7M / 1.9M / 1.4M / 822k op/s bulk EXISTS / DELETE / GET / UPDATE (10K keys per TCP request). Sub-5ms indexed find / count / aggregate at 1M rows. |
| **Indexes** | B+ tree (eq, range, prefix, all 38 operators), bitmap (auto-promote for bool + enum, popcount fast paths), trigram (substring search on varchar; planner auto-picks btree-leaf for short patterns, trigram for long). |
| **Operators (38)** | eq / neq / range / between, like / contains / starts / ends, in / not_in, regex, exists, len_*, ilike / icontains, eq_field — all use the index when one is available. |
| **Planner** | AND-intersection across 2+ indexed leaves without per-record fetch for `count`. Lock-free OR-union via KeySet. Rarest-first selection for trigram intersect. |
| **Query features** | Inner + left joins, count/sum/avg/min/max aggregations with `group_by` + `having`, cursor pagination, CAS (`if` / `if_not_exists`, dry-run bulk ops). |
| **Storage** | Per-shard btree layout (2026.05.1+) — writes route by hash, reads fan out across `splits/4` shards in parallel; k-way streaming merge for ordered queries. |
| **Multi-tenancy** | `dir` parameter + tokens scoped global / per-tenant / per-object × `r` / `rw` / `rwx` permissions. |
| **Transport** | Native TLS 1.3 (single binary, single port, OpenSSL-backed) or reverse-proxy termination — both first-class. |
| **Reliability** | Crash-safe (atomic flag-flip writes, msync on shutdown, recovery sweep at startup). External-merge-sort index build — bounded memory at any scale. |
| **Tools** | `shard-cli` ncurses TUI over the same TCP+TLS wire (separate binary, no daemon source linked). |

Detailed feature reference: [docs/index.md](docs/index.md).

## What it is not

- **Not distributed.** Single-node, single-process. No built-in replication across hosts. Block-level replication (DRBD / BSR) is the recommended HA path and works transparently at the storage layer; application-level replication is on the roadmap.
- **Not SQL.** The wire protocol is JSON-over-TCP; queries are JSON documents. This is intentional — the typed-record model gives the planner a fixed schema to optimise against rather than a general-purpose SQL parser.
- **Not Windows.** Linux x86_64 / ARM64 and macOS Apple Silicon only.

## Quick start

```bash
./build.sh                      # builds shard-db (daemon) + shard-cli (TUI) + migrate (one-shot upgrades)
./shard-db start

./shard-db query '{
  "mode": "create-object", "dir": "default", "object": "users",
  "splits": 16, "max_key": 128,
  "fields": ["name:varchar:100", "email:varchar:200", "age:int"],
  "indexes": ["email", "age"]
}'

./shard-db insert default users u1 '{"name":"Alice","email":"a@x.com","age":30}'
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'
./shard-db stop
```

More: [Install](docs/getting-started/install.md) · [Quick start](docs/getting-started/quickstart.md) · [Client examples (Python / Java / Node.js)](docs/getting-started/clients.md)

### Upgrading from a prior release

```bash
./shard-db stop
# replace build/bin/ contents with the new release (shard-db, shard-cli, migrate)
./migrate                       # runs every required migration for the new release; idempotent
./shard-db start
```

For the 2026.05.1 reissue specifically: `./migrate` lifts pre-2026.05.2 `<obj>/files/<XX>/<XX>/<filename>` hash buckets to flat layout, then rebuilds every B+ tree under the per-shard layout shipped in 2026.05.1. See [the 2026.05.1 changelog entry](docs/reference/changelog.md#202605132026-05-02-reissued) for the full list of breaking changes (read response shapes are bare values now: `get`, `exists`, `count`, `size` no longer wrap in JSON; `get-multi` returns a dict; `find`/`fetch` gain `format:"dict"`).

## Performance snapshot

10M K/V records · 16-byte key, varchar(100) value · AMD Ryzen 7 7840U · NVMe ext4 (TCP-end-to-end measurements):

| Workload | Result |
|---|---|
| Bulk insert (CSV, 10M, 1 conn) | **4.60 M/sec** |
| Bulk insert (CSV, 10M, 5 conns × 2M) | **8.97 M/sec** |
| Bulk insert (CSV, 1M invoice schema, 5 conns × 200k, no idx) | **2.48 M/sec** |
| Bulk insert (CSV, 1M invoice schema, 5 conns × 200k, 14 idx) | **435 k/sec** |
| Bulk EXISTS (10K keys per request) | **4.10 M/sec** |
| Bulk DELETE (10K keys per request) | **1.76 M/sec** |
| Bulk GET (10K keys per request) | **1.49 M/sec** |
| Bulk UPDATE (10K keys per request) | **989 k/sec** |
| Indexed `find` (1M users, limit 10) | **<1 ms** |
| Indexed `count` / `aggregate` (warm cache) | **<1–296 ms** |
| Single-conn GET ×10k (req-resp, 1 conn) | **33 k ops/sec** (28µs/op) |
| Disk footprint (10M K/V records) | 2.2 GB |

### 25M cold-bench highlights (2026.05.4)

Same hardware, post `sync && drop_caches` between runs. Query patterns that exercise the new fast paths landed this release:

| Cold query (25M users) | Result |
|---|---|
| `sum X` single-spec on indexed int/long/short/numeric (each) | **~200 ms** (~10× vs 2026.05.3) |
| `group by username, count limit 10` (high-card varchar idx) | **3.6 ms** (~1570× vs 2026.05.3) |
| `group by email, sum(balance) limit 10` (varchar + indexed numeric agg) | **4.1 ms** (~1800× vs 2026.05.3) |
| First-cold full-scan `count starts bio 'Software'` (non-idx varchar) | ~800 ms (~1.6× vs 2026.05.3) |
| `agg WHERE active=false (count+avg)` | 1.1 s (~2.5× vs 2026.05.3) |

The 1500-1800× wins on the group_by limit shape come from the new streaming k-way merge — earlier paths built a 25M-entry hash table just to truncate to `limit=10`. The 10× wins on single-spec sum come from a tight leaf walker that bypasses `BtRangeIter`'s per-entry overhead plus `MADV_SEQUENTIAL` on the btree mmap during the walk (the per-btree default `MADV_RANDOM` is right for point lookups but suppresses readahead on sequential scans).

Reads are measured strict request-response (no pipelining); pipelining client-side will push throughput higher. The single-conn ceiling is dominated by TCP+JSON framing (~30 µs/op) — bulk paths bypass that and are the right tool for high-throughput multi-key workloads. Bench harness: [`src/bench/`](src/bench/) (C-level timing).

Full breakdown across 5 workloads + tuning notes: [docs/operations/benchmarks.md](docs/operations/benchmarks.md).

## Documentation

- [**Getting started**](docs/getting-started/install.md) — install, quick start, configuration (db.env), client examples
- [**Concepts**](docs/concepts/storage-model.md) — storage model, typed records, indexes, concurrency, multi-tenancy
- [**Query protocol**](docs/query-protocol/overview.md) — wire format + per-mode reference (find, count, aggregate, joins, bulk, CAS, files, schema mutations) + [recipes](docs/query-protocol/recipes.md)
- [**CLI reference**](docs/cli/index.md) — every `shard-db` and `shard-cli` command
- [**Operations**](docs/operations/deployment.md) — deployment, tuning, backup, monitoring, [benchmarks](docs/operations/benchmarks.md)
- [**Reference**](docs/reference/limits.md) — limits, error codes, changelog

## Contributing

Patches welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the build, test, code-style, and PR flow. New issues and feature requests via [GitHub Issues](https://github.com/sayyiditow/shard-db/issues).

## Security

Vulnerability disclosures via [SECURITY.md](SECURITY.md). All releases (2026.05.1+) ship with cosign keyless signatures + SLSA L3 provenance.

## License

[MIT](LICENSE)
