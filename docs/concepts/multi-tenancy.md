# Multi-tenancy

shard-db is a single-process server that partitions data by **tenant directory** (`dir`). Every data query carries a `dir` parameter, which the server validates against an allowlist before touching any disk. As of 2026.05, tokens layer on top of that with **scoped permissions** — a token can be limited to one tenant or even one object, and granted only the rights it needs.

## Two independent dimensions

| Dimension | What it controls | Where it lives |
|---|---|---|
| **Scope** | Which `(dir, object)` pairs a token can touch | The `tokens.conf` file the token sits in |
| **Permission** | Which actions are allowed (read / write / admin) | Suffix on the token line: `:r`, `:rw`, `:rwx` |

Both apply on every request. A `r`-scope-`acme` token can read anything in `acme` and nothing else.

### Scope

Determined by which file the token lives in:

| Scope | File | Covers |
|---|---|---|
| global | `$DB_ROOT/tokens.conf` | any `dir`, any object |
| tenant | `$DB_ROOT/<dir>/tokens.conf` | any object within `<dir>` |
| object | `$DB_ROOT/<dir>/<object>/tokens.conf` | only `<dir>/<object>` |

### Permission

| Perm | Reads | Writes | Admin commands |
|---|---|---|---|
| `r` | ✓ | ✗ | ✗ |
| `rw` | ✓ | ✓ | ✗ |
| `rwx` | ✓ | ✓ | scope-dependent (see below) |
| _(no suffix)_ | same as `rwx` | backward-compat for pre-2026.05 tokens.conf | |

### Admin command scope

Admin commands themselves have a scope. A token needs `rwx` AND scope at least as broad as the command's admin scope:

| Command | Admin scope | Who can run it |
|---|---|---|
| `stats`, `stats-prom`, `db-dirs`, `vacuum-check`, `shard-stats`, `add-ip`/`remove-ip`/`list-ips`, `add-token`/`remove-token`/`list-tokens`, `add-dir`/`remove-dir` | server | global `rwx` or trusted IP only |
| `create-object` | tenant | global `rwx` or tenant `rwx` on that dir |
| `truncate`, `vacuum`, `backup`, `recount`, `add-field`, `remove-field`, `rename-field`, `add-index`, `remove-index` | object | any `rwx` whose scope covers that object |

**Token management is always server-admin** — tenant admins cannot issue new tokens. The platform operator owns all credential issuance.

## Precedence on each request

1. Trusted IP (global `allowed_ips.conf`) → bypass all token checks.
2. Token match with sufficient scope + perm → allow.
3. Otherwise → `{"error":"auth failed"}`.

Localhost (`127.0.0.1`, `::1`) is trusted by default. Set `DISABLE_LOCALHOST_TRUST=1` in db.env to require tokens from same-host callers too.

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

- Auto-registered on first `create-object` against them.
- Added at runtime via `{"mode":"add-dir","dir":"newtenant"}` (server-admin scope).
- Or edited directly + server reload.

List the current allowlist:

```bash
./shard-db db-dirs
```

Remove a dir:

```bash
./shard-db query '{"mode":"remove-dir","dir":"oldtenant","check_empty":true}'
```

`check_empty:true` (default) refuses if the dir still contains objects.

## On-disk layout per tenant

```
$DB_ROOT/
  tokens.conf                          # global tokens
  dirs.conf                            # allowlist
  default/
    tokens.conf                        # tenant tokens (optional)
    users/
      tokens.conf                      # per-object tokens (optional)
      fields.conf
      data/ ...
    products/ ...
  acme/
    tokens.conf
    invoices/
      tokens.conf
      ...
```

Objects are identified by the **pair** `(dir, object)`. Two tenants can each have an `invoices` object with completely different schemas — they're isolated directories.

## Validation

Every query that takes a `dir` parameter (all data CRUD + queries + schema mutations) gets its `dir` run through `is_valid_dir()`. This is an O(1) hash lookup against `g_dirs_cache`. Rejection happens **before any object lookup or disk I/O**, so probing an unregistered dir has zero cost and zero information leakage.

## Cross-tenant boundaries

Nothing automatic. **Joins** (`find` with `join`) are same-dir only — a driver object can only join to objects in the same `dir`. Cross-tenant data needs two queries and an app-side merge.

## What still doesn't isolate

- **CPU / memory** — all tenants share the same worker pool and caches. A tenant with a heavy scan affects others. The `TIMEOUT` / per-request `timeout_ms` and `QUERY_BUFFER_MB` are the main levers; `SLOW_QUERY_MS` log surfaces offenders.
- **Disk** — all tenants share `$DB_ROOT`'s filesystem. Set up per-tenant quotas at the OS level if that matters.

## Moving tenants

A tenant's directory is self-contained — copy `<dir>/` to a new root and register the dir in the destination's `dirs.conf`. Schemas, indexes, stored files, and per-tenant tokens all travel together.

## Deleting a tenant

To retire a tenant cleanly:

1. Stop client traffic to that dir.
2. `truncate` (or rm -rf) every object within it.
3. `{"mode":"remove-dir","dir":"<name>"}` — drops the entry from `dirs.conf`.

`remove-dir` with `check_empty:false` will also blow away non-empty trees if you really mean it.

## Naming

Tenant names must be valid filesystem directory names — no `/`, no `..`, no control chars, ≤64 bytes. Short alphanumerics + hyphens + underscores recommended. `add-dir` enforces these rules and rejects invalid names with `{"error":"invalid dir name (no /,\\,..,control chars; max 64 bytes)"}`.
