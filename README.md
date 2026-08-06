<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-lockup-dark.svg">
    <img src="docs/assets/logo-lockup.svg" alt="shard-db" width="360">
  </picture>
</p>

[![CI](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml)
[![Docs](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml)
[![npm](https://img.shields.io/npm/v/shard-db)](https://www.npmjs.com/package/shard-db)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/sayyiditow/shard-db/badge)](https://scorecard.dev/viewer/?uri=github.com/sayyiditow/shard-db)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12704/badge)](https://www.bestpractices.dev/projects/12704)
[![Coverity Scan](https://scan.coverity.com/projects/33072/badge.svg?v=2026-05-05)](https://scan.coverity.com/projects/sayyiditow-shard-db)
[![codecov](https://codecov.io/gh/sayyiditow/shard-db/branch/main/graph/badge.svg)](https://codecov.io/gh/sayyiditow/shard-db)

A high-performance database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, and a multi-threaded TCP server with optional native TLS 1.3.

**Platform:** Linux x86_64 / ARM64 + macOS (Apple Silicon). License: **MIT**.

## In production

**[HN Explorer](https://hn.shard-db.dev)** — 30M+ Hacker News stories, comments, and users served from a single shard-db instance on an 8-core Netcup VPS. Browse by category, search with full-text, paginate deep into the archive at constant latency. Runs in **embedded mode** via the npm package — no separate daemon. [Source](https://github.com/sayyiditow/shard-db-hn-explorer) · Starters: [TCP](https://github.com/sayyiditow/shard-db-svelte-starter) · [Embedded](https://github.com/sayyiditow/shard-db-embedded-sveltekit-starter) (SvelteKit + Bun).

## Why I built it

Most projects reach for PostgreSQL by default. That's usually the right call — PostgreSQL is battle-hardened and general-purpose. shard-db is for a narrower problem:

> Schema is fixed and known at design time. Queries are point lookups, range scans, or keyword searches. You need predictable latency and simple deployment. One machine is enough.

In that envelope, a general-purpose SQL engine carries overhead you don't need: a query parser, a WAL, a lock manager, a planner that must handle arbitrary schema. shard-db trades that generality for a simpler execution model — typed binary records, a planner that knows your schema at startup, and cursor pagination that makes page 10,000 as cheap as page 1.

## How it compares

| Feature | shard-db | PostgreSQL | Redis / Valkey | SQLite |
|---|---|---|---|---|
| Single static binary | **Yes** | No | No | Yes (library) |
| Zero-dependency deploy | **Yes** | No | No | Yes (embedded) |
| Server process (multi-client TCP) | **Yes** | Yes | Yes | No |
| SQL | No | **Yes** | No | **Yes** |
| Fixed typed schema | **Yes** | Yes | No | Yes |
| JSON + NQL / TCP wire protocol | **Yes** | No | RESP / TCP | No |
| Built-in TLS (single port) | **Yes** | Via libssl | Via libssl | No |
| B+ tree range queries | **Yes** | Yes | Sorted sets | Yes |
| Bitmap index (bool / enum) | **Yes** | No | Bitfields | No |
| Trigram full-text search | **Yes** | Extension | No | Extension |
| Joins | **Yes** | Yes | No | Yes |
| Aggregates + group by | **Yes** | Yes | Limited | Yes |
| Cursor pagination (O(1) deep pages) | **Yes** | Requires keyset | No | Requires keyset |
| ACID transactions | Per-record | **Yes** | Partial | **Yes** |
| Distributed | No | Extensions | **Cluster** | No |
| Primary use case | Server DB, typed records, fast text search | General-purpose RDBMS | Cache, message broker | Embedded app storage |

## Highlights

| Area | Highlights |
|---|---|
| **Throughput** | ~5M / ~8.4M K/V ops/sec single-thread / 5-conn parallel bulk insert (CSV, 10M records). 5.7M / 1.9M / 1.4M / 822k op/s bulk EXISTS / DELETE / GET / UPDATE (10K keys per TCP request). Sub-5ms indexed find / count / aggregate at 1M rows. |
| **Indexes** | B+ tree (eq, range, prefix, all 38 operators), bitmap (auto-promote for bool + enum, popcount fast paths), trigram (substring search on varchar; planner auto-picks btree-leaf for short patterns, trigram for long). |
| **Operators (38)** | eq / neq / range / between, like / contains / starts / ends, in / not_in, regex, exists, len_*, ilike / icontains, eq_field — all use the index when one is available. |
| **Planner** | AND-intersection across 2+ indexed leaves without per-record fetch for `count`. Lock-free OR-union via KeySet. Rarest-first selection for trigram intersect. |
| **Query features** | Inner + left joins, count/sum/avg/min/max aggregations with `group_by` + `having`, cursor pagination, CAS (`if` / `if_not_exists`, dry-run bulk ops). |
| **Storage** | Per-shard btree layout, VARIABLE-format segments (2026.06.4+) — writes route by hash, reads fan out across `splits/4` shards in parallel; k-way streaming merge for ordered queries. Trailing-zero field trimming shrinks varchar records by 50–90% on typical workloads; `compact` command rewrites existing segments. |
| **Multi-tenancy** | `dir` parameter + tokens scoped global / per-tenant / per-object × `r` / `rw` / `rwx` permissions. |
| **Transport** | Native TLS 1.3 (single binary, single port, OpenSSL-backed) or reverse-proxy termination — both first-class. |
| **Reliability** | Crash-safe (atomic flag-flip writes, msync on shutdown, recovery sweep at startup). External-merge-sort index build — bounded memory at any scale. |
| **Tools** | `shard-cli` ncurses TUI over the same TCP+TLS wire (separate binary, no daemon source linked). |

Detailed feature reference: [docs/index.md](docs/index.md).

## What it is not

- **Not distributed.** Single-node, single-process. No built-in replication across hosts. Block-level replication (DRBD / BSR) is the recommended HA path and works transparently at the storage layer; application-level replication is on the roadmap.
- **Not SQL.** The primary wire protocol is JSON-over-TCP; complex queries are JSON documents. For everyday reads, the [Natural Query Language (NQL)](docs/query-protocol/nql.md) lets you write `find default users 'age > 25'` directly — the server auto-detects NQL vs JSON on the same port. This is intentional — the typed-record model gives the planner a fixed schema to optimise against rather than a general-purpose SQL parser.
- **Not Windows.** Linux x86_64 / ARM64 and macOS Apple Silicon only.

## Quick start

```bash
./build.sh                      # builds shard-db (daemon) + shard-cli (TUI)
./shard-db start

./shard-db query '{
  "mode": "create-object", "dir": "default", "object": "users",
  "splits": 16, "max_key": 128,
  "fields": ["name:varchar:100", "email:varchar:200", "age:int"],
  "indexes": ["email", "age"]
}'

./shard-db insert default users u1 '{"name":"Alice","email":"a@x.com","age":30}'
./shard-db find default users 'age > 25'   # NQL — human-readable filter syntax
./shard-db stop
```

More: [Install](docs/getting-started/install.md) · [Quick start](docs/getting-started/quickstart.md) · [Client examples (Python / Java / Node.js)](docs/getting-started/clients.md)

### Node.js / Bun (embedded — no daemon)

Use shard-db in-process from Node.js or Bun. No TCP, no separate process.

```bash
npm install shard-db   # or: bun add shard-db
```

```js
const ShardDb = require('shard-db')
const db = new ShardDb('/path/to/data')

await db.query({ mode: 'create-object', dir: 'myapp', object: 'users',
  splits: 16, max_key: 128,
  fields: ['name:varchar:100', 'email:varchar:200', 'age:int'],
  indexes: ['email', 'age'] })

await db.query({ mode: 'insert', dir: 'myapp', object: 'users', key: 'u1',
  value: { name: 'Alice', email: 'a@x.com', age: 30 } })

const results = JSON.parse(await db.query({ mode: 'find', dir: 'myapp', object: 'users',
  criteria: { age: { gt: 25 } }, limit: 20 }))

db.close()
```

Prebuilt binaries for Linux x64/arm64 and macOS Apple Silicon (Node ≥ 18, Bun ≥ 1.0). Full API and TypeScript types: [npm package](https://www.npmjs.com/package/shard-db) · [SvelteKit starter](https://github.com/sayyiditow/shard-db-embedded-sveltekit-starter).

### Upgrading from a prior release

```bash
./shard-db stop
# replace build/bin/ contents with the new release (shard-db, shard-cli)
./shard-db start                # startup reindex runs automatically when upgrading
```

The daemon compares `$DB_ROOT/.version` against its compiled-in version and runs this release's full index rebuild in-process on upgrade. The standalone `./migrate` binary is removed as of 2026.08.1. The minimum supported source release is 2026.07.3; that floor is recorded for operators but is informational and not enforced in this release because earlier releases did not write `.version`. `./shard-db reindex` remains available for on-demand use. For the 2026.05.1 reissue specifically, see [the 2026.05.1 changelog entry](docs/reference/changelog.md#202605132026-05-02-reissued) for the full list of breaking changes (read response shapes are bare values now: `get`, `exists`, `count`, `size` no longer wrap in JSON; `get-multi` returns a dict; `find`/`fetch` gain `format:"dict"`).

Single-key writes are atomic, durable, and isolated by the per-object lock, but shard-db has no multi-statement or cross-object transaction scope; its ACID properties are therefore per-record/per-object.

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
