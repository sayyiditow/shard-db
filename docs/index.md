# shard-db

A file-based sharded database in C. Started as a key/value store; now a small-scale database with typed binary records, B+ tree indexes, joins, aggregates, and CAS, served by a multi-threaded TCP server. Single static binary, no external dependencies.

## What it is

- **Typed records** — varchar, int/long/short, double, bool/byte, date, datetime, numeric (fixed-point).
- **B+ tree indexes** — single and composite (`field1+field2`), prefix-compressed leaves.
- **17 search operators** — `eq`, `neq`, `lt`, `gt`, `lte`, `gte`, `between`, `in`, `not_in`, `like`, `not_like`, `contains`, `not_contains`, `starts`, `ends`, `exists`, `not_exists`. All use indexes when available.
- **Aggregations** — `count`, `sum`, `avg`, `min`, `max` with `group_by`, `having`, `order_by`.
- **Joins** — inner and left, by primary key or any indexed field; composite locals supported.
- **CAS (conditional writes)** — `if_not_exists`, `if:{...}` on insert/update/delete; dry-run bulk ops.
- **File storage** — put/get arbitrary files keyed by filename, with a base64-over-TCP path for remote clients and a zero-copy server-local fast path.
- **Schema mutations** — add/rename/remove fields; vacuum compacts or reshards online.
- **Multi-tenancy** — the `dir` parameter isolates tenants, validated against an allowlist.
- **Authentication** — API tokens + IP allowlist.
- **TLS** — not native; terminate at a reverse proxy (HAProxy / nginx / stunnel).

**Platform:** Linux x86_64 / ARM64 only (uses `epoll`, `mmap`, POSIX pthreads). License: **MIT**.

## 60-second tour

```bash
./build.sh
./shard-db start

./shard-db query '{
  "mode": "create-object",
  "dir": "default",
  "object": "users",
  "splits": 16,
  "max_key": 128,
  "fields": ["name:varchar:100", "email:varchar:200", "age:int"],
  "indexes": ["email", "age"]
}'

./shard-db insert default users u1 '{"name":"Alice","email":"a@x.com","age":30}'
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'
./shard-db stop
```

Dive deeper in the [Quick start](getting-started/quickstart.md).

## Where to go next

| If you want to... | Read |
|---|---|
| Install and run shard-db | [Install](getting-started/install.md) · [Quick start](getting-started/quickstart.md) |
| Understand the model | [Concepts](concepts/storage-model.md) |
| Look up a CLI command | [CLI reference](cli/index.md) |
| Look up a JSON query | [Query protocol](query-protocol/overview.md) |
| Deploy to production | [Operations → Deployment](operations/deployment.md) · [Tuning](operations/tuning.md) |
| Find a limit or error | [Reference → Limits](reference/limits.md) · [Error codes](reference/error-codes.md) |
| See what's coming | [Roadmap (v2)](v2/roadmap.md) |
