# shard-db

A high-performance database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, and a multi-threaded TCP server with optional native TLS 1.3.

## What it is

- **Typed records** — varchar, int/long/short, double/float, bool/byte, date, datetime, time, uuid, numeric (fixed-point), currency.
- **Slotcask storage engine (v2)** — bitcask-style: per-object keyfile shards (`data/kf/NNN.kf`) hold 24-byte slot headers; values live in append-only segment files (`data/streams/NNN/NNNNNN.dat`) split across nproc-derived streams. Commits are a single atomic 8-byte store into the keyfile after the value lands on disk. Tombstoned slots feed a per-stream free pool so inline overwrites reuse holes (snake-game). Keyfile shards auto-resplit in-place at 75 % load; no global rehash, no per-shard ceiling.
- **B+ tree indexes** — single and composite (`field1+field2`), prefix-compressed leaves, per-shard layout (`indexes/<field>/NNN.idx`). Each field's btree is split into `index_splits_for(splits)` files (non-linear curve, capped at 128 for `splits=4096`) for read parallelism. Reads fan out across all shards via the worker pool; writes route by record hash to one shard.
- **38 search operators** — eq/neq/lt/gt/lte/gte/between/in/not_in, like/not_like, contains/not_contains, starts/ends, exists/not_exists, len_eq/len_neq/len_lt/len_gt/len_lte/len_gte/len_between (varchar-length filters answered from btree leaf metadata, no record fetch), ilike/not_ilike/icontains/not_icontains/istarts/iends (case-insensitive variants), eq_field/neq_field/lt_field/gt_field/lte_field/gte_field (field-vs-field on same record), regex/not_regex (POSIX extended regex on varchar). Indexes used when available.
- **Aggregations** — `count`, `sum`, `avg`, `min`, `max` with `group_by`, `having`, `order_by`. NEQ shortcut algebraically rewrites `count(*) - count(eq)` for indexed fields.
- **Joins** — inner and left, by primary key or any indexed field; composite locals supported. Tabular output.
- **CAS (conditional writes)** — `if_not_exists`, `if:{...}` on insert/update/delete; dry-run bulk ops.
- **Cursor pagination** — keyset cursor on `find` over an indexed `order_by` field. O(limit) per page, ASC + DESC; tie-breaks on hash16. Preferred over `offset` for deep pages.
- **AND-intersection** — `find/count/aggregate` with 2+ indexed leaves on rangeable ops automatically intersect candidate hash sets via a lock-free `KeySet`, skipping per-record fetch for `count`.
- **File storage** — put/get arbitrary files keyed by filename, base64-over-TCP for remote clients, zero-copy server-local fast path. `list-files` lists with prefix + paginated.
- **Schema mutations** — add/rename/remove fields; vacuum runs Direction-C segment compaction (pair-merges sparse non-active segments to reclaim disk) or rebuilds (`vacuum --compact` drops tombstoned bytes; `vacuum --splits=N` rehashes and reindexes).
- **Multi-tenancy** — the `dir` parameter isolates tenants. Per-tenant and per-object tokens with scoped permissions (`r` / `rw` / `rwx`) on top of global tokens + IP allowlist.
- **Native TLS 1.3** — single binary, single port, OpenSSL-backed. Toggle via `TLS_ENABLE=1` in db.env. Reverse-proxy termination (nginx/HAProxy/stunnel) remains supported.
- **Per-request statement timeout** — any query can carry `"timeout_ms":N`; thread-local override of the global `TIMEOUT`.
- **shard-cli TUI** — separate ncurses client that connects over the same TCP+TLS wire; menus for browse / query / schema / maintenance / auth / live stats. See [CLI reference → shard-cli](cli/shard-cli.md).

**Platform:** Linux x86_64 / ARM64 and macOS Apple Silicon (2026.05.4+). Uses `mmap`, POSIX pthreads, and `poll(2)` for the accept loop on both platforms (the earlier Linux-only `epoll` path was retired in 2026.05.4 with the macOS port — single listen fd had nothing to gain from `epoll`'s selectivity). License: **MIT**.

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
| See what shipped | [Changelog](reference/changelog.md) |
