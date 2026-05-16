# Install

shard-db is a single static C binary. You can build from source in seconds, or grab a pre-built release tarball.

## Platform requirements

- **Linux** x86_64 or ARM64.
- **macOS** Apple Silicon (2026.05.4+).
- **gcc** 9+ or **clang** 10+. OpenSSL 1.1+ (for TLS) and ncurses (for the TUI client).
- No other runtime dependencies — xxhash is bundled header-only.

Not portable to Windows without source changes; Docker/WSL2 cover that case.

## Option 1 — build from source

```bash
git clone https://github.com/sayyiditow/shard-db.git
cd shard-db
./build.sh
```

Output lives under `build/`:

```
build/
  bin/
    shard-db        # daemon binary
    shard-cli       # ncurses TUI client
    migrate         # one-shot upgrade runner (per-release migrations)
    db.env.example  # template config — copy to db.env on first deploy
```

`build/bin/` is the only artefact. `$DB_ROOT` (from your `db.env`) is created lazily on first `./shard-db start`; an existing data directory is never touched, so dropping a fresh `build/` tree onto an upgraded host is non-destructive.

### Portable vs native builds

Default release builds use `-O2 -flto=auto` — portable across x86-64 + ARM64. For self-built deployments where you don't need to ship the binary off the build host:

```bash
./build.sh                          # portable (default)
BUILD_MARCH=native ./build.sh       # self-built, full local codegen
BUILD_MARCH=x86-64-v3 ./build.sh    # portable-but-modern (BMI2 / AVX2)
```

### Skipping the test suite

`./build.sh` runs the full 77-case C test suite at the end. To skip it during iterative dev:

```bash
SKIP_TESTS=1 ./build.sh
```

## Option 2 — pre-built release

Download the tarball for your platform from the [GitHub releases](https://github.com/sayyiditow/shard-db/releases) page. Each release ships three platforms:

- `shard-db-<version>-linux-x86_64.tar.gz`
- `shard-db-<version>-linux-arm64.tar.gz`
- `shard-db-<version>-darwin-arm64.tar.gz`

```bash
tar xzf shard-db-2026.05.4-linux-x86_64.tar.gz
cd shard-db-2026.05.4
./shard-db start
```

Each archive contains the stripped daemon (`shard-db`), the TUI client (`shard-cli`), the per-release upgrade runner (`migrate`), and a default `db.env.example`. All artifacts are cosign-signed; verify with:

```bash
cosign verify-blob \
  --certificate-identity-regexp 'github\.com/sayyiditow/shard-db' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --bundle shard-db-2026.05.4-linux-x86_64.tar.gz.bundle \
  shard-db-2026.05.4-linux-x86_64.tar.gz
```

## Upgrading from a prior release

shard-db ships per-release migrations as the `./migrate` binary, which runs every required migration for the version you're upgrading to, then exits. The general flow:

```bash
./shard-db stop
# replace build/bin/ contents with the new release artifacts
./migrate                        # idempotent; safe to re-run
./shard-db start
```

For 2026.05.1+, `./migrate` runs three phases:

1. **migrate-files** — lifts pre-2026.05.2 `<obj>/files/<XX>/<XX>/<filename>` hash buckets to the flat `<obj>/files/<filename>` layout. Filesystem-only, no daemon required.
2. **reindex** — rebuilds every B+ tree under the per-shard layout shipped in 2026.05.1 (also picks up the v3 btree format introduced in 2026.05.3, which adds `prev_leaf` for O(1)-step DESC iteration).
3. **migrate-storage-version** — for any object still on storage_version=1 (the legacy probe-into-slot engine), rebuilds it under the v2 slotcask engine (keyfile shards + append-only segments). Idempotent — already-v2 objects are skipped.

The full migration sequence runs once on the v2 upgrade; subsequent releases that don't add new migrations make `./migrate` a no-op.

See the [changelog](../reference/changelog.md) for the migrations each release runs.

## First-run sanity check

```bash
cd build/bin
./shard-db start         # starts the daemon on the port in db.env (default 9199)
./shard-db status        # confirms it's running
./shard-db stop          # graceful shutdown
```

If `status` reports the server as running but connections hang, check:

- `db.env` is present in the current working directory (see [Configuration](configuration.md)).
- The port in `db.env` isn't already in use (`ss -tlnp | grep 9199` on Linux, `lsof -i :9199` on macOS).
- Logs under `./logs/` (or wherever `LOG_DIR` points) — errors are in `error-YYYY-MM-DD.log`.

## Next

- [Quick start](quickstart.md) — insert and query your first record in five minutes.
- [Configuration](configuration.md) — db.env, tokens, allowed IPs, tenant directories.
