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
[![Coverity Scan](https://scan.coverity.com/projects/33072/badge.svg)](https://scan.coverity.com/projects/sayyiditow-shard-db)
[![codecov](https://codecov.io/gh/sayyiditow/shard-db/branch/main/graph/badge.svg)](https://codecov.io/gh/sayyiditow/shard-db)

A high-performance file-based database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, and a multi-threaded TCP server with optional native TLS 1.3.

**Platform:** Linux x86_64 / ARM64. macOS port planned for 2026.05.2. License: **MIT**.

## Highlights

- **~2.7M K/V ops/sec** single-thread bulk insert; sub-5ms indexed find / count / aggregate at 1M rows
- **38 search operators** (eq/neq/range, like/contains/starts/ends, in/not_in, regex, exists, len_*, ilike/icontains, eq_field…) — every one indexed when an index is available
- **AND-intersection planner** + **lock-free OR-union KeySet** — 2+ indexed criteria intersect candidate sets without per-record fetch for `count`
- **Joins** (inner, left), **aggregations** (count/sum/avg/min/max with group_by + having), **cursor pagination**, **CAS** (if/if_not_exists, dry-run bulk ops)
- **Per-shard btree layout** (2026.05.1+): writes route by hash, reads fan out across `splits/4` shards in parallel; k-way streaming merge for ordered queries
- **Multi-tenancy**: `dir` parameter + tokens scoped global/per-tenant/per-object × `r`/`rw`/`rwx` permissions
- **Native TLS 1.3** (single binary, single port, OpenSSL-backed) or reverse-proxy termination — both first-class
- **Crash-safe**: atomic flag-flip writes, msync on shutdown, recovery sweep at startup
- **shard-cli TUI** — separate ncurses client over the same TCP+TLS wire

Detailed feature reference: [docs/index.md](docs/index.md).

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
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'
./shard-db stop
```

More: [Install](docs/getting-started/install.md) · [Quick start](docs/getting-started/quickstart.md) · [Client examples (Python / Java / Node.js)](docs/getting-started/clients.md)

## Performance snapshot

10M K/V records · 16-byte key, varchar(100) value · AMD Ryzen 7 7840U · NVMe ext4 (TCP-end-to-end measurements):

| Workload | Result |
|---|---|
| Bulk insert (CSV, 10M, 1 conn) | **2.39 M/sec** |
| Bulk insert (CSV, 10M, 10 conns) | **2.72 M/sec** |
| Indexed `find` (1M users, limit 10) | **2–4 ms** |
| Indexed `count` / `aggregate` | **3–48 ms** |
| GET ×10k (pipelined, 1 conn) | **22 k ops/sec** |
| Disk footprint (10M records) | 2.3 GB |

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
