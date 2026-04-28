# Install

shard-db is a single static C binary. You can build from source in seconds, or grab a pre-built release tarball.

## Platform requirements

- **Linux** x86_64 or ARM64. Uses `epoll`, `mmap`, POSIX pthreads.
- **gcc** 9+ (or clang 10+).
- No other runtime dependencies — xxhash is bundled header-only; only `libc` and `libpthread` are linked.

Not portable to macOS or Windows without source changes.

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
    db.env.example  # template config — copy to db.env on first deploy
```

`build/bin/` is the only artefact. `$DB_ROOT` (from your `db.env`) is created lazily on first `./shard-db start`; an existing data directory is never touched, so dropping a fresh build/ tree onto an upgraded host is non-destructive.

### Manual compile

```bash
gcc -O2 -o shard-db src/*.c -Isrc -lpthread
```

Useful when you want a custom install prefix or a non-default optimization level. Flags like `-DNDEBUG`, `-march=native`, or `-flto` are safe to add.

## Option 2 — pre-built release

Download the tarball for your platform from the [GitHub releases](https://github.com/sayyiditow/shard-db/releases) page:

```bash
tar xzf shard-db-2026.04.3-linux-x86_64.tar.gz
cd shard-db-2026.04.3
./shard-db start
```

Each release archive contains the stripped binary, default `db.env`, and the helper scripts.

## First run sanity check

```bash
cd build/bin
./shard-db start         # starts the daemon on the port in db.env (default 9199)
./shard-db status        # confirms it's running
./shard-db stop          # graceful shutdown
```

If `status` reports the server as running but connections hang, check:

- `db.env` is present in the current working directory (see [Configuration](configuration.md)).
- The port in `db.env` isn't already in use (`ss -tlnp | grep 9199`).
- Logs under `./logs/` (or wherever `LOG_DIR` points) — errors are in `error-YYYY-MM-DD.log`.

## Next

- [Quick start](quickstart.md) — insert and query your first record in five minutes.
- [Configuration](configuration.md) — db.env, tokens, allowed IPs, tenant directories.
