# Multi-tenancy

shard-db is a single-process server that partitions data by **tenant directory** (`dir`). Every data query carries a `dir` parameter, which the server validates against an allowlist before touching any disk.

## The model

- Each tenant gets a subdirectory under `$DB_ROOT/<dir>/`.
- Every object belongs to exactly one `dir`.
- Queries without a valid `dir` are rejected with `{"error":"Unknown dir: <name>"}`.
- Tenants are **isolated at the filesystem and validation layer** — not at the auth layer. Any authenticated client can query any registered dir.

This is **directory-level multi-tenancy**, not per-tenant authentication. If tenants need isolated credentials, put a gateway in front that signs token-per-tenant requests or terminates one-tenant-per-process.

## dirs.conf

Allowlist of valid tenant names — one per line:

```text
# $DB_ROOT/dirs.conf
default
acme
beta
engineering
```

The `default` dir is auto-registered. New dirs:

- Auto-registered on first `create-object` against them (the directory is created and appended to `dirs.conf`).
- Can be added manually (edit the file + restart, or the server reloads it opportunistically).

List the current allowlist:

```bash
./shard-db db-dirs
```

Returns `["default","acme","beta","engineering"]`.

## On-disk layout per tenant

```
$DB_ROOT/
  dirs.conf
  default/
    users/ ...
    products/ ...
  acme/
    invoices/ ...
    customers/ ...
```

Objects are identified by the **pair** `(dir, object)`. Two tenants can each have an `invoices` object with completely different schemas — they're isolated directories.

## Validation

Every query that takes a `dir` parameter (all data CRUD + queries + schema mutations) gets its `dir` run through `is_valid_dir()`. This is an O(1) hash lookup against `g_dirs_cache`. Rejection happens **before any object lookup or disk I/O**, so attempting to probe an unregistered dir has effectively zero cost and zero information leakage.

## What crosses tenant boundaries

Nothing automatic. **Joins** (`find` with `join`) are same-dir only — a driver object can only join to objects in the same `dir`. If you need cross-tenant data, do two queries and merge in the app.

## What doesn't isolate

- **CPU / memory** — all tenants share the same worker pool and caches. A tenant with a heavy scan affects others. The `TIMEOUT` statement-timeout and the `SLOW_QUERY_MS` log are the main levers.
- **Disk** — all tenants share `$DB_ROOT`'s filesystem. Set up per-tenant quotas at the OS level if that matters.
- **Authentication** — tokens in `tokens.conf` grant access to the server, not to specific tenants. A token that works against tenant A works against tenant B.

## Moving tenants

A tenant's directory is self-contained — you can move it between shard-db instances by copying `<dir>/` to a new root and registering the dir in the destination's `dirs.conf`. Schemas, indexes, and stored files all travel together.

## Deleting a tenant

There's no built-in "drop tenant" command. To retire a tenant:

1. Stop any client traffic to that dir.
2. `truncate` every object within it (or just remove the directory tree).
3. Remove the line from `dirs.conf`.
4. Restart the server (picks up the updated allowlist).

## Naming

Tenant names must be valid filesystem directory names — no `/`, no `..`, no control chars. Short alphanumerics + hyphens + underscores are recommended. `is_valid_dir()` enforces basic safety but doesn't canonicalize beyond that.
