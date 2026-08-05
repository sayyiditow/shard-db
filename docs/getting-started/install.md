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

`./build.sh` runs the full 232-case C test suite at the end. To skip it during iterative dev:

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

Point releases of shard-db on the slotcask engine (2026.05.1 and
later) upgrade with a binary swap:

```bash
./shard-db stop
# replace build/bin/ contents with the new release artifacts
./shard-db start
```

The `./migrate` binary was reinstated in 2026.07.1 and ships with
2026.07.1+. It runs two phases: (1) varlen segment migration, then
(2) offline compact. It is idempotent — safe to re-run if interrupted.

Operators who upgraded from a pre-2026.07.1 build affected by kf
corruption must run the 2026.07.1 release's `rebuild-kf` against a
backup before upgrading past this release. This release removes
`rebuild-kf` and makes full rebuilds abort on invalid live kf
references.

Operators on a pre-2026.05.5 install with legacy v1 (probe-into-slot)
objects on disk must first install 2026.05.4 and run that release's
`./migrate` to convert objects to slotcask, then upgrade to 2026.05.5+.

See the [changelog](../reference/changelog.md) for what changed in each release.

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
